// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Op synthesis: resolve bind points in op templates from GK variates.
//!
//! This is the bridge between the workload spec (ParsedOp) and the
//! adapter (ResolvedFields). For each cycle, bind points in op template
//! fields are resolved from the GK kernel's output variates.

use std::sync::Arc;

use nb_variates::kernel::{GkKernel, GkProgram, GkState};
use nb_variates::node::Value;
use nb_workload::model::ParsedOp;
use nb_workload::bindpoints::{self, BindPoint, BindQualifier};

/// Resolves bind points in a string template using the GK kernel.
///
/// Resolution order for unqualified `{name}`:
/// 1. GK binding outputs
/// 2. Coordinate inputs (as raw u64)
///
/// Qualified bind points:
/// - `{bind:name}` → GK output only
/// - `{input:name}` → coordinate value only
///
/// Capture-qualified bind points (`{capture:name}`) resolve from
/// GK volatile/sticky ports once capture extraction is implemented.
/// Until then, they resolve as unresolved placeholders.
pub fn substitute_bind_points(
    template: &str,
    kernel: &mut GkKernel,
) -> String {
    let bind_points = bindpoints::extract_bind_points(template);
    if bind_points.is_empty() {
        return template.to_string();
    }

    let mut result = template.to_string();
    for bp in &bind_points {
        match bp {
            BindPoint::Reference { name, qualifier } => {
                let value_str = resolve_bind_point(name, qualifier, kernel);
                let placeholder = match qualifier {
                    BindQualifier::None => format!("{{{name}}}"),
                    BindQualifier::Input => format!("{{input:{name}}}"),
                    BindQualifier::Bind => format!("{{bind:{name}}}"),
                    BindQualifier::Capture => format!("{{capture:{name}}}"),
                };
                result = result.replace(&placeholder, &value_str);
            }
            BindPoint::InlineDefinition(_) => {
                // Inline definitions are handled at compile time, not here
            }
        }
    }
    result
}

/// Resolve a single bind point to its string value.
fn resolve_bind_point(
    name: &str,
    qualifier: &BindQualifier,
    kernel: &mut GkKernel,
) -> String {
    match qualifier {
        BindQualifier::Bind => {
            match kernel.program().resolve_output(name) {
                Some(_) => value_to_string(kernel.pull(name)),
                None => format!("{{bind:{name}}}"),
            }
        }
        BindQualifier::Capture => {
            // Captures will resolve from GK volatile/sticky ports
            // once capture extraction is implemented.
            format!("{{capture:{name}}}")
        }
        BindQualifier::Input => {
            match kernel.get_input(name) {
                Some(v) => v.to_display_string(),
                None => {
                    match kernel.program().resolve_output(name) {
                        Some(_) => value_to_string(kernel.pull(name)),
                        None => format!("{{coord:{name}}}"),
                    }
                }
            }
        }
        BindQualifier::None => {
            if kernel.program().resolve_output(name).is_some() {
                return value_to_string(kernel.pull(name));
            }
            format!("{{{name}}}")
        }
    }
}

/// Convert a GK Value to its string representation for op assembly.
fn value_to_string(value: &Value) -> String {
    value.to_display_string()
}


/// Substitute bind points using program + state (no GkKernel wrapper).
fn substitute_bind_points_with_state(
    template: &str,
    program: &GkProgram,
    state: &mut GkState,
) -> String {
    let bind_points = bindpoints::extract_bind_points(template);
    if bind_points.is_empty() { return template.to_string(); }

    let mut result = template.to_string();
    for bp in &bind_points {
        match bp {
            BindPoint::Reference { name, qualifier } => {
                let value_str = resolve_bind_point_with_state(name, qualifier, program, state);
                let placeholder = match qualifier {
                    BindQualifier::None => format!("{{{name}}}"),
                    BindQualifier::Input => format!("{{input:{name}}}"),
                    BindQualifier::Bind => format!("{{bind:{name}}}"),
                    BindQualifier::Capture => format!("{{capture:{name}}}"),
                };
                result = result.replace(&placeholder, &value_str);
            }
            BindPoint::InlineDefinition(_) => {}
        }
    }
    result
}

fn resolve_bind_point_with_state(
    name: &str,
    qualifier: &BindQualifier,
    program: &GkProgram,
    state: &mut GkState,
) -> String {
    match qualifier {
        BindQualifier::Bind => {
            if program.resolve_output(name).is_some() {
                state.pull(program, name).to_display_string()
            } else {
                format!("{{bind:{name}}}")
            }
        }
        BindQualifier::Capture => {
            // Captures will resolve from GK volatile/sticky ports
            // once capture extraction is implemented.
            format!("{{capture:{name}}}")
        }
        BindQualifier::Input => {
            program.input_names().iter()
                .position(|n| n == name)
                .map(|idx| state.get_input(idx).to_display_string())
                .or_else(|| {
                    if program.resolve_output(name).is_some() {
                        Some(state.pull(program, name).to_display_string())
                    } else { None }
                })
                .unwrap_or_else(|| format!("{{coord:{name}}}"))
        }
        BindQualifier::None => {
            // Resolution order: GK output → input → unresolved
            if program.resolve_output(name).is_some() {
                return state.pull(program, name).to_display_string();
            }
            if program.input_names().contains(&name.to_string()) {
                if let Some(idx) = program.input_names().iter().position(|n| n == name) {
                    return state.get_input(idx).to_display_string();
                }
            }
            format!("{{{name}}}")
        }
    }
}

/// Shared op builder that distributes per-fiber builders.
///
/// Holds the `Arc<GkProgram>` (immutable, shared). Each executor
/// fiber calls `create_fiber_builder()` to get its own `FiberBuilder`
/// with private `GkState` — no locks, no contention on the hot path.
pub struct OpBuilder {
    program: Arc<GkProgram>,
    /// Values to inject into every new FiberBuilder's state at creation.
    /// Used for scope composition: outer scope constants are set as
    /// initial values for inner scope extern inputs.
    scope_values: Vec<(usize, Value)>, // (input_idx, value)
}

impl OpBuilder {
    /// Create an OpBuilder from a kernel.
    /// If the kernel has scope values (set via `bind_outer_scope`),
    /// they are automatically propagated to every fiber's state.
    pub fn new(kernel: GkKernel) -> Self {
        let scope_values = kernel.scope_values();
        let program = kernel.into_program();
        Self { program, scope_values }
    }

    /// Access the shared GK program.
    pub fn program(&self) -> Arc<GkProgram> {
        self.program.clone()
    }

    /// Create a per-fiber builder. No locks, no sharing — the fiber
    /// owns its state exclusively. Scope values (from outer scope
    /// constants) are injected into the state's extern inputs.
    pub fn create_fiber_builder(&self) -> FiberBuilder {
        let mut fb = FiberBuilder::new(self.program.clone());
        for (idx, value) in &self.scope_values {
            fb.state.set_input(*idx, value.clone());
        }
        fb
    }
}

/// Per-fiber op builder. Owns its own GkState.
/// No locks, no synchronization, no contention.
///
/// Created via `OpBuilder::create_fiber_builder()` at fiber startup.
///
/// When capture extraction is implemented, captured values will
/// write directly to GK volatile/sticky ports on the state,
/// bypassing any intermediate storage.
pub struct FiberBuilder {
    program: Arc<GkProgram>,
    state: GkState,
}

/// Validate that all bind points in op templates can be resolved.
///
/// Called at init time. Warns for each unresolvable `{name}` reference.
/// A bind point is resolvable if it matches a GK output, input name,
/// or a known capture declaration from another op. Workload params are
/// injected into the GK source as constant bindings before compilation,
/// so they resolve as GK outputs.
/// Validate that all bind points in op templates can be resolved.
///
/// Returns `Err` with a descriptive message if any bind point is
/// unresolvable. Callers should treat this as a fatal error —
/// unresolved bind points produce broken ops at runtime.
pub fn validate_bind_points(
    templates: &[ParsedOp],
    program: &GkProgram,
) -> Result<(), String> {
    // Collect all capture declarations across templates
    let mut capture_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for template in templates {
        for value in template.op.values() {
            if let serde_json::Value::String(s) = value {
                let result = bindpoints::parse_capture_points(s);
                for cp in result.captures {
                    capture_names.insert(cp.as_name);
                }
            }
        }
    }

    let mut errors: Vec<String> = Vec::new();

    for template in templates {
        for (field_name, value) in &template.op {
            if let serde_json::Value::String(s) = value {
                let bps = bindpoints::extract_bind_points(s);
                for bp in &bps {
                    if let BindPoint::Reference { name, qualifier } = bp {
                        let resolvable = match qualifier {
                            BindQualifier::Bind => program.resolve_output(name).is_some(),
                            BindQualifier::Capture => capture_names.contains(name),
                            BindQualifier::Input => program.input_names().contains(&name.to_string()),
                            BindQualifier::None => {
                                program.resolve_output(name).is_some()
                                    || capture_names.contains(name)
                                    || program.input_names().contains(&name.to_string())
                            }
                        };
                        if !resolvable {
                            errors.push(format!(
                                "unresolved bind point '{{{name}}}' in op '{}' field '{field_name}'. \
                                 Not found in GK bindings, captures, or inputs.",
                                template.name
                            ));
                        }
                    }
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        for e in &errors {
            crate::observer::log(crate::observer::LogLevel::Error, &format!("error: {e}"));
        }
        Err(format!("{} unresolved bind point(s)", errors.len()))
    }
}

/// Memoized bind point plan for a prepared statement.
///
/// Built once per op template at activity init time. Maps each bind
/// point name to a GK output index so per-row evaluation uses
/// `pull_by_index` (direct indexed access) instead of name lookup.
///
/// The `positions` vector is ordered to match the `?` marker order
/// in the prepared statement. Each entry is `(bind_name, gk_output_index)`.
#[derive(Clone, Debug)]
pub struct BindPlan {
    positions: Vec<(String, usize)>,
}

impl BindPlan {
    /// Build a bind plan from the bind point names (in `?` position order)
    /// and the GK program that will provide the values.
    ///
    /// Returns `None` if any bind point name is not a GK output.
    pub fn new(bind_names: &[String], program: &GkProgram) -> Option<Self> {
        let mut positions = Vec::with_capacity(bind_names.len());
        for name in bind_names {
            let idx = program.output_index(name)?;
            positions.push((name.clone(), idx));
        }
        Some(Self { positions })
    }

    /// Number of bind positions.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// The bind point names in position order.
    pub fn names(&self) -> Vec<String> {
        self.positions.iter().map(|(n, _)| n.clone()).collect()
    }

    /// Pull all bind point values from the GK state using memoized
    /// indices. Returns values in `?` position order. No string
    /// lookups — each pull is a direct indexed buffer access.
    pub fn pull_values(&self, state: &mut nb_variates::kernel::GkState, program: &GkProgram) -> Vec<nb_variates::node::Value> {
        self.positions.iter()
            .map(|(_, idx)| state.pull_by_index(program, *idx).clone())
            .collect()
    }

    /// Pull N tuples for consecutive cursor offsets `[base..base+count)`.
    /// Restores the base input after the pull completes.
    pub fn pull_range(
        &self,
        state: &mut nb_variates::kernel::GkState,
        program: &GkProgram,
        base: u64,
        count: usize,
    ) -> Vec<Vec<Value>> {
        let mut rows = Vec::with_capacity(count);
        for i in 0..count {
            state.set_inputs(&[base + i as u64]);
            rows.push(self.pull_values(state, program));
        }
        state.set_inputs(&[base]);
        rows
    }

    /// Pull tuples until the byte budget is reached, using the first
    /// row's measured size as the predictor for remaining rows.
    ///
    /// Because all rows in a batch come from the same op template with
    /// sequential cursor offsets, row sizes are consistent — no peek
    /// or double-buffer needed. The first row is always included, even
    /// if it exceeds the budget alone.
    ///
    /// Capped at `max_rows`. Returns at least one row whenever
    /// `max_rows >= 1`.
    pub fn pull_to_budget(
        &self,
        state: &mut nb_variates::kernel::GkState,
        program: &GkProgram,
        base: u64,
        budget_bytes: usize,
        max_rows: usize,
    ) -> Vec<Vec<Value>> {
        if max_rows == 0 { return Vec::new(); }

        state.set_inputs(&[base]);
        let first = self.pull_values(state, program);
        let first_size = tuple_wire_size(&first);

        let rows_to_fill = if first_size == 0 {
            max_rows
        } else {
            (budget_bytes / first_size).max(1)
        };
        let total = rows_to_fill.min(max_rows);

        let mut rows = Vec::with_capacity(total);
        rows.push(first);
        for i in 1..total {
            state.set_inputs(&[base + i as u64]);
            rows.push(self.pull_values(state, program));
        }
        state.set_inputs(&[base]);
        rows
    }
}

/// Default cap on rows per batch when only `batch_budget` is set.
/// Prevents pathological oversize batches if rows are tiny.
const DEFAULT_BATCH_MAX_ROWS: usize = 1000;

/// Batching configuration parsed once per op template at activity setup.
///
/// - `batch: N` → fixed row count (existing behavior)
/// - `max_batch_size: 64KB` → fill to byte target, capped at max_rows
/// - Both → fill to byte target but cap at N rows
///
/// If `size == 0` and `target_bytes == None`, batching is disabled for
/// this template.
#[derive(Clone, Debug, Default)]
pub struct BatchConfig {
    /// Fixed row count from `batch: N`. Zero means no fixed count.
    pub size: usize,
    /// Byte target from `max_batch_size: 64KB`. None means not set.
    pub target_bytes: Option<usize>,
    /// Maximum rows per batch. Derived from `batch:` if set, else a default.
    pub max_rows: usize,
}

impl BatchConfig {
    /// Parse from an op template's params map.
    pub fn from_params(params: &std::collections::HashMap<String, serde_json::Value>) -> Self {
        let size = params.get("batch")
            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0) as usize;
        let target_bytes = params.get("max_batch_size")
            .and_then(|v| v.as_str())
            .and_then(parse_byte_size);
        let max_rows = if size > 0 { size } else { DEFAULT_BATCH_MAX_ROWS };
        Self { size, target_bytes, max_rows }
    }

    /// Whether batching is active for this template.
    pub fn is_active(&self) -> bool {
        self.size > 0 || self.target_bytes.is_some()
    }
}

/// Approximate byte cost of a pulled tuple. Generic heuristic used by
/// budget-based batching. In-memory value sizes correspond closely to
/// CQL wire cost for common types (blob, text, int, float, vector as
/// bytes). Per-row frame overhead is constant across rows so it doesn't
/// affect the fill ratio and isn't included.
fn tuple_wire_size(values: &[Value]) -> usize {
    values.iter().map(value_wire_size).sum()
}

fn value_wire_size(v: &Value) -> usize {
    match v {
        Value::U64(_) | Value::F64(_) => 8,
        Value::Bool(_) => 1,
        Value::Str(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Json(v) => v.to_string().len(),
        Value::Ext(v) => v.display().len(),
        Value::None => 0,
    }
}

/// Parse a byte-size string like "64KB", "1MB", or plain "32768".
/// Accepts K/KB, M/MB, B suffixes (case insensitive). Returns None if
/// the input doesn't parse as a valid size.
pub(crate) fn parse_byte_size(s: &str) -> Option<usize> {
    let s = s.trim();
    if s.is_empty() { return None; }
    let lower = s.to_ascii_lowercase();
    let (num_part, mult): (&str, usize) = if let Some(n) = lower.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix('m') {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix('k') {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix('b') {
        (n, 1)
    } else {
        (lower.as_str(), 1)
    };
    num_part.trim().parse::<usize>().ok().map(|n| n * mult)
}

impl FiberBuilder {
    /// Create a new fiber builder from a shared GK program.
    pub fn new(program: Arc<GkProgram>) -> Self {
        let state = program.create_state();
        Self {
            program,
            state,
        }
    }

    /// Set coordinates and begin a new evaluation scope.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        self.state.set_inputs(coords);
    }

    /// Feed a source item into the GK state.
    ///
    /// Sets the ordinal as the coordinate input and injects field
    /// projections into the appropriate state slots (e.g., `base__ordinal`,
    /// `base__vector`).
    pub fn set_source_item(&mut self, item: &nb_variates::source::SourceItem) {
        // Set ordinal as the first coordinate input
        self.state.set_inputs(&[item.ordinal]);
        // Inject field projections into named GK inputs
        for (name, value) in &item.fields {
            if let Some(idx) = self.program.find_input(name) {
                self.state.set_input(idx, value.clone());
            }
        }
    }

    /// Reset capture inputs to defaults. Called at stanza boundaries
    /// to prevent capture leakage across stanzas. Coordinates are
    /// not reset.
    pub fn reset_captures(&mut self) {
        self.state.reset_inputs_from(self.program.coord_count());
    }

    /// Invalidate all state: reset all inputs and mark all nodes dirty.
    /// Provides "clean slate" semantics.
    pub fn invalidate_all(&mut self) {
        self.state.invalidate_all();
    }

    /// Store a captured value directly into GK state.
    ///
    /// Writes to the named port in GkState. Returns `true` if the
    /// port was found and the value stored, `false` if no port with
    /// this name exists in the program (value is dropped).
    pub fn capture(&mut self, name: &str, value: Value) -> bool {
        if let Some(idx) = self.program.find_input(name) {
            self.state.set_input(idx, value);
            true
        } else {
            false
        }
    }

    /// Resolve all fields in a template to typed values and strings.
    /// Returns ResolvedFields for consumption by an OpDispenser.
    ///
    /// Convenience wrapper that builds the bind plan and batch config on
    /// demand. Use `resolve_with_extras_cached` in hot paths — the activity
    /// caches both artifacts per op template at setup time.
    pub fn resolve_with_extras(
        &mut self,
        template: &ParsedOp,
        extra_bindings: &[String],
    ) -> crate::adapter::ResolvedFields {
        let stmt_field = template.op.get("stmt")
            .or_else(|| template.op.get("prepared"))
            .or_else(|| template.op.get("raw"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let bind_names = nb_workload::bindpoints::referenced_bindings(stmt_field);
        let plan = BindPlan::new(&bind_names, &self.program);
        let batch_config = BatchConfig::from_params(&template.params);
        self.resolve_with_extras_cached(template, extra_bindings, plan.as_ref(), &batch_config)
    }

    /// Resolve fields using pre-built bind plan and batch config.
    ///
    /// The plan and batch config are built once per op template at activity
    /// setup — this avoids per-cycle reconstruction of BindPlan from bind
    /// point names.
    pub fn resolve_with_extras_cached(
        &mut self,
        template: &ParsedOp,
        extra_bindings: &[String],
        bind_plan: Option<&BindPlan>,
        batch_config: &BatchConfig,
    ) -> crate::adapter::ResolvedFields {
        let mut names = Vec::new();
        let mut values = Vec::new();

        for (key, value) in &template.op {
            names.push(key.clone());
            if let serde_json::Value::String(s) = value {
                let resolved = substitute_bind_points_with_state(
                    s, &self.program, &mut self.state,
                );

                // Preserve typed value for pure bind point references
                let trimmed = s.trim();
                if trimmed.starts_with('{') && trimmed.ends_with('}') && !trimmed.starts_with("{{") {
                    let name = &trimmed[1..trimmed.len()-1];
                    let bare = if let Some((_, n)) = name.split_once(':') { n } else { name };
                    if self.program.resolve_output(bare).is_some() {
                        values.push(self.state.pull(&self.program, bare).clone());
                        continue;
                    }
                }
                values.push(Value::Str(resolved));
            } else {
                values.push(Value::Str(value.to_string()));
            }
        }

        // Pull extra GK bindings (e.g., ground truth for validation)
        for binding in extra_bindings {
            if !names.contains(binding) {
                if self.program.resolve_output(binding).is_some() {
                    names.push(binding.clone());
                    values.push(self.state.pull(&self.program, binding).clone());
                }
            }
        }

        let mut batch_fields = Vec::new();
        if batch_config.is_active() {
            // Batch ops require a bind plan — all bind points must resolve to GK
            // outputs. Validation catches unresolvable bind points at setup; if
            // we get here without a plan, it's a bug upstream (e.g., trying to
            // batch with `{capture:...}` or `{input:...}` references).
            if let Some(plan) = bind_plan {
                let base = self.state.get_input(0).as_u64();
                let rows = if let Some(target) = batch_config.target_bytes {
                    plan.pull_to_budget(
                        &mut self.state, &self.program, base,
                        target, batch_config.max_rows,
                    )
                } else {
                    plan.pull_range(
                        &mut self.state, &self.program, base, batch_config.size,
                    )
                };
                let row_names = plan.names();
                for row_values in rows {
                    batch_fields.push(crate::adapter::ResolvedFieldSet {
                        names: row_names.clone(),
                        values: row_values,
                    });
                }
            }
        }

        let mut result = crate::adapter::ResolvedFields::new(names, values);
        result.batch_fields = batch_fields;
        result
    }

    /// Resolve op fields only (no extras). Convenience for simple cases.
    pub fn resolve(&mut self, template: &ParsedOp) -> crate::adapter::ResolvedFields {
        self.resolve_with_extras(template, &[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nb_variates::assembly::{GkAssembler, WireRef};
    use nb_variates::nodes::hash::Hash64;
    use nb_variates::nodes::arithmetic::ModU64;

    fn make_kernel() -> GkKernel {
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm.add_node("user_id", Box::new(ModU64::new(1_000_000)), vec![WireRef::node("hashed")]);
        asm.add_output("user_id", WireRef::node("user_id"));
        asm.add_output("hashed", WireRef::node("hashed"));
        asm.compile().unwrap()
    }

    #[test]
    fn substitute_no_bind_points() {
        let mut kernel = make_kernel();
        kernel.set_inputs(&[0]);
        let result = substitute_bind_points("plain text", &mut kernel);
        assert_eq!(result, "plain text");
    }

    #[test]
    fn substitute_single_bind_point() {
        let mut kernel = make_kernel();
        kernel.set_inputs(&[42]);
        let result = substitute_bind_points("user:{user_id}", &mut kernel);
        assert!(result.starts_with("user:"));
        let id: u64 = result.strip_prefix("user:").unwrap().parse().unwrap();
        assert!(id < 1_000_000);
    }

    #[test]
    fn substitute_multiple_bind_points() {
        let mut kernel = make_kernel();
        kernel.set_inputs(&[42]);
        let result = substitute_bind_points("id={user_id} hash={hashed}", &mut kernel);
        assert!(result.contains("id="));
        assert!(result.contains("hash="));
    }

    #[test]
    fn substitute_deterministic() {
        let mut kernel = make_kernel();
        kernel.set_inputs(&[42]);
        let r1 = substitute_bind_points("{user_id}", &mut kernel);
        kernel.set_inputs(&[42]);
        let r2 = substitute_bind_points("{user_id}", &mut kernel);
        assert_eq!(r1, r2);
    }

    #[test]
    fn fiber_builder_resolves_fields() {
        let kernel = make_kernel();
        let builder = OpBuilder::new(kernel);
        let mut fiber = builder.create_fiber_builder();
        let template = ParsedOp::simple("test", "SELECT * FROM t WHERE id={user_id}");

        fiber.set_inputs(&[42]);
        let fields = fiber.resolve(&template);

        assert_eq!(fields.names, vec!["stmt"]);
        let stmt = &fields.strings()[0];
        assert!(stmt.starts_with("SELECT * FROM t WHERE id="));
        assert!(!stmt.contains("{user_id}"));
    }

    #[test]
    fn fiber_builder_deterministic() {
        let kernel = make_kernel();
        let builder = OpBuilder::new(kernel);
        let mut fiber = builder.create_fiber_builder();
        let template = ParsedOp::simple("op1", "id={user_id}");

        fiber.set_inputs(&[42]);
        let a = fiber.resolve(&template);
        fiber.set_inputs(&[42]);
        let b = fiber.resolve(&template);
        assert_eq!(a.strings()[0], b.strings()[0]);
    }

    #[test]
    fn fiber_builder_different_cycles() {
        let kernel = make_kernel();
        let builder = OpBuilder::new(kernel);
        let mut fiber = builder.create_fiber_builder();
        let template = ParsedOp::simple("op1", "id={user_id}");

        fiber.set_inputs(&[42]);
        let a = fiber.resolve(&template);
        fiber.set_inputs(&[43]);
        let b = fiber.resolve(&template);
        assert_ne!(a.strings()[0], b.strings()[0]);
    }

    // =====================================================================
    // Byte-size parser
    // =====================================================================

    #[test]
    fn parse_byte_size_kb() {
        assert_eq!(parse_byte_size("64KB"), Some(64 * 1024));
        assert_eq!(parse_byte_size("64kb"), Some(64 * 1024));
        assert_eq!(parse_byte_size("64K"), Some(64 * 1024));
        assert_eq!(parse_byte_size("64k"), Some(64 * 1024));
    }

    #[test]
    fn parse_byte_size_mb() {
        assert_eq!(parse_byte_size("1MB"), Some(1024 * 1024));
        assert_eq!(parse_byte_size("1mb"), Some(1024 * 1024));
        assert_eq!(parse_byte_size("1M"), Some(1024 * 1024));
        assert_eq!(parse_byte_size("2M"), Some(2 * 1024 * 1024));
    }

    #[test]
    fn parse_byte_size_plain_integer() {
        assert_eq!(parse_byte_size("32768"), Some(32768));
        assert_eq!(parse_byte_size("0"), Some(0));
    }

    #[test]
    fn parse_byte_size_bytes_suffix() {
        assert_eq!(parse_byte_size("500B"), Some(500));
        assert_eq!(parse_byte_size("500b"), Some(500));
    }

    #[test]
    fn parse_byte_size_whitespace() {
        assert_eq!(parse_byte_size("  64KB  "), Some(64 * 1024));
        assert_eq!(parse_byte_size(" 1 KB "), Some(1024));
    }

    #[test]
    fn parse_byte_size_invalid() {
        assert_eq!(parse_byte_size(""), None);
        assert_eq!(parse_byte_size("abc"), None);
        assert_eq!(parse_byte_size("64GB"), None);  // GB not supported
        assert_eq!(parse_byte_size("1.5KB"), None); // decimals not supported
    }

    // =====================================================================
    // Tuple wire-size heuristic
    // =====================================================================

    #[test]
    fn value_wire_size_primitives() {
        assert_eq!(value_wire_size(&Value::U64(42)), 8);
        assert_eq!(value_wire_size(&Value::F64(3.14)), 8);
        assert_eq!(value_wire_size(&Value::Bool(true)), 1);
        assert_eq!(value_wire_size(&Value::None), 0);
    }

    #[test]
    fn value_wire_size_heap_types() {
        assert_eq!(value_wire_size(&Value::Str("hello".into())), 5);
        assert_eq!(value_wire_size(&Value::Bytes(vec![0u8; 400])), 400);
        // Json stringified: {"a":1}
        let json = serde_json::json!({"a": 1});
        assert_eq!(value_wire_size(&Value::Json(json)), 7);
    }

    #[test]
    fn tuple_wire_size_sum() {
        let tuple = vec![
            Value::U64(42),          // 8
            Value::Str("hi".into()), // 2
            Value::Bytes(vec![0u8; 100]), // 100
        ];
        assert_eq!(tuple_wire_size(&tuple), 110);
    }

    #[test]
    fn tuple_wire_size_empty() {
        assert_eq!(tuple_wire_size(&[]), 0);
    }

    // =====================================================================
    // BatchConfig parsing
    // =====================================================================

    fn params(entries: &[(&str, serde_json::Value)]) -> std::collections::HashMap<String, serde_json::Value> {
        entries.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn batch_config_empty() {
        let p = params(&[]);
        let c = BatchConfig::from_params(&p);
        assert_eq!(c.size, 0);
        assert_eq!(c.target_bytes, None);
        assert!(!c.is_active());
    }

    #[test]
    fn batch_config_fixed_size_only() {
        let p = params(&[("batch", serde_json::json!(8))]);
        let c = BatchConfig::from_params(&p);
        assert_eq!(c.size, 8);
        assert_eq!(c.target_bytes, None);
        assert_eq!(c.max_rows, 8);
        assert!(c.is_active());
    }

    #[test]
    fn batch_config_target_only() {
        let p = params(&[("max_batch_size", serde_json::json!("64KB"))]);
        let c = BatchConfig::from_params(&p);
        assert_eq!(c.size, 0);
        assert_eq!(c.target_bytes, Some(64 * 1024));
        assert_eq!(c.max_rows, DEFAULT_BATCH_MAX_ROWS);
        assert!(c.is_active());
    }

    #[test]
    fn batch_config_both() {
        let p = params(&[
            ("batch", serde_json::json!(100)),
            ("max_batch_size", serde_json::json!("64KB")),
        ]);
        let c = BatchConfig::from_params(&p);
        assert_eq!(c.size, 100);
        assert_eq!(c.target_bytes, Some(64 * 1024));
        // max_rows caps at the explicit batch size
        assert_eq!(c.max_rows, 100);
    }

    #[test]
    fn batch_config_batch_as_string() {
        let p = params(&[("batch", serde_json::json!("8"))]);
        let c = BatchConfig::from_params(&p);
        assert_eq!(c.size, 8);
    }

    // =====================================================================
    // BindPlan batch pulls
    // =====================================================================

    fn bind_plan_fixture() -> (BindPlan, Arc<GkProgram>, GkState) {
        let kernel = make_kernel();
        let program = kernel.into_program();
        let state = program.create_state();
        let plan = BindPlan::new(&["user_id".to_string()], &program).unwrap();
        (plan, program, state)
    }

    #[test]
    fn bind_plan_pull_range_sequential() {
        let (plan, program, mut state) = bind_plan_fixture();
        let rows = plan.pull_range(&mut state, &program, 0, 5);
        assert_eq!(rows.len(), 5);
        // Each row should be a 1-tuple with a U64 value.
        for row in &rows {
            assert_eq!(row.len(), 1);
            assert!(matches!(row[0], Value::U64(_)));
        }
        // Different cursor offsets should generally produce different hashes.
        let r0 = rows[0][0].as_u64();
        let r1 = rows[1][0].as_u64();
        assert_ne!(r0, r1);
    }

    #[test]
    fn bind_plan_pull_range_restores_base() {
        let (plan, program, mut state) = bind_plan_fixture();
        state.set_inputs(&[7]);
        let _ = plan.pull_range(&mut state, &program, 7, 3);
        assert_eq!(state.get_input(0).as_u64(), 7, "base input should be restored");
    }

    #[test]
    fn bind_plan_pull_to_budget_empty_max_rows() {
        let (plan, program, mut state) = bind_plan_fixture();
        let rows = plan.pull_to_budget(&mut state, &program, 0, 1024, 0);
        assert!(rows.is_empty());
    }

    #[test]
    fn bind_plan_pull_to_budget_first_row_always_committed() {
        let (plan, program, mut state) = bind_plan_fixture();
        // U64 row = 8 bytes. Budget of 4 bytes is less than one row,
        // but the first row must still be returned.
        let rows = plan.pull_to_budget(&mut state, &program, 0, 4, 100);
        assert_eq!(rows.len(), 1, "first row must be committed even if it exceeds budget");
    }

    #[test]
    fn bind_plan_pull_to_budget_fills_to_target() {
        let (plan, program, mut state) = bind_plan_fixture();
        // U64 row = 8 bytes. Budget = 24 bytes → 3 rows fit.
        let rows = plan.pull_to_budget(&mut state, &program, 0, 24, 100);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn bind_plan_pull_to_budget_caps_at_max_rows() {
        let (plan, program, mut state) = bind_plan_fixture();
        // U64 row = 8 bytes. Budget = 1 MB would fit many rows.
        // max_rows = 5 caps the result.
        let rows = plan.pull_to_budget(&mut state, &program, 0, 1024 * 1024, 5);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn bind_plan_pull_to_budget_restores_base() {
        let (plan, program, mut state) = bind_plan_fixture();
        state.set_inputs(&[42]);
        let _ = plan.pull_to_budget(&mut state, &program, 42, 64, 100);
        assert_eq!(state.get_input(0).as_u64(), 42, "base input should be restored");
    }
}
