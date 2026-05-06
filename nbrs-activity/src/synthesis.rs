// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Op synthesis: resolve bind points in op templates from GK variates.
//!
//! This is the bridge between the workload spec (ParsedOp) and the
//! adapter (ResolvedFields). For each cycle, bind points in op template
//! fields are resolved from the GK kernel's output variates.

use std::sync::Arc;

use nbrs_variates::kernel::{GkKernel, GkProgram, GkState};
use nbrs_variates::node::Value;
use nbrs_workload::model::ParsedOp;
use nbrs_workload::bindpoints::{self, BindPoint, BindQualifier};

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
    /// Pre-evaluated init binding values to seed into every new
    /// FiberBuilder's state — `(node_idx, port_idx, value)`. Captured
    /// from the activation kernel after [SRD 11](../../docs/sysref/11_gk_evaluation.md)
    /// §"Init Binding Contract" Plan B has run. With this seeding,
    /// the binding's eval function fires exactly once per scope
    /// activation (on the activation kernel), not once per fiber.
    init_overrides: Vec<(usize, usize, Value)>,
    /// SRD-13d Phase 9 — per-op-template kernel programs keyed
    /// by op name. Populated by [`Self::with_op_template_programs`]
    /// when the runner has materialised op-template kernels in
    /// the scope tree. Wrappers (e.g. `MetricsDispenser`) look up
    /// the program for their template via [`Self::program_for_op`]
    /// and build their `ScopeFixture` against it; flattened
    /// op-templates fall through to the activity-wide `program`.
    op_template_programs: std::collections::HashMap<String, Arc<GkProgram>>,
}

impl OpBuilder {
    /// Create an OpBuilder from a kernel.
    /// If the kernel has scope values (set via `bind_outer_scope`
    /// or directly via `kernel.state().set_input`), they are
    /// captured and propagated into every fiber's state.
    ///
    /// Init binding values that have been pulled on the kernel's
    /// state (typically by the scope-init pass right after
    /// `bind_outer_scope`) are likewise captured as
    /// [`Self::init_overrides`] and propagated to every fiber, so
    /// init eval fires once per activation rather than once per
    /// fiber.
    pub fn new(kernel: GkKernel) -> Self {
        let scope_values = kernel.scope_values();
        let init_overrides = collect_init_overrides(&kernel);
        let program = kernel.into_program();
        Self {
            program,
            scope_values,
            init_overrides,
            op_template_programs: std::collections::HashMap::new(),
        }
    }

    /// Wrap an existing program with no scope values.
    ///
    /// Used by callers that already hold an `Arc<GkProgram>` and
    /// have no per-iteration values to inject (e.g. tests, simple
    /// scripts, the workload-kernel fallback path for phases
    /// without their own bindings or iter parent).
    pub fn from_program(program: Arc<GkProgram>) -> Self {
        Self {
            program,
            scope_values: Vec::new(),
            init_overrides: Vec::new(),
            op_template_programs: std::collections::HashMap::new(),
        }
    }

    /// Install per-op-template kernel programs (SRD-13d Phase 9).
    /// The runner builds these from the scope tree's
    /// `cached_kernel` slots for materialised op-template scopes
    /// and threads them here so wrappers can look up the right
    /// program when constructing their fixtures.
    pub fn with_op_template_programs(
        mut self,
        programs: std::collections::HashMap<String, Arc<GkProgram>>,
    ) -> Self {
        self.op_template_programs = programs;
        self
    }

    /// Look up the kernel program for op `name`. Returns the
    /// per-op-template program if Phase 9 produced one for this
    /// op (i.e. `materialised` and bindings non-empty); otherwise
    /// returns the activity-wide program (the flatten path).
    pub fn program_for_op(&self, name: &str) -> Arc<GkProgram> {
        self.op_template_programs.get(name)
            .cloned()
            .unwrap_or_else(|| self.program.clone())
    }

    /// Access the shared GK program.
    pub fn program(&self) -> Arc<GkProgram> {
        self.program.clone()
    }

    /// Create a per-fiber builder. No locks, no sharing — the fiber
    /// owns its state exclusively. Scope values (per-iteration
    /// inputs from `for_each` / `for_combinations` / outer scope
    /// constants) are injected into the state's extern inputs and
    /// remembered on the builder so `reset_captures` (called at
    /// stanza boundaries) can re-apply them — otherwise the
    /// blanket "reset all non-coord inputs" pass would clobber
    /// the iteration's bound values.
    ///
    /// Init binding values captured by the scope-init pass are
    /// seeded into the fiber's node buffers and the corresponding
    /// nodes are marked clean. This is the runtime mechanism
    /// behind the init-binding contract: each fiber's first pull
    /// of an init binding reads the pre-evaluated value directly
    /// instead of running the binding's eval.
    pub fn create_fiber_builder(&self) -> FiberBuilder {
        let mut fb = FiberBuilder::new(self.program.clone());
        fb.scope_values = self.scope_values.clone();
        for (idx, value) in &self.scope_values {
            fb.state().set_input(*idx, value.clone());
        }
        for (node_idx, port_idx, value) in &self.init_overrides {
            fb.state().seed_node_buffer(*node_idx, *port_idx, value.clone());
        }
        // SRD-13d Phase 9 — instance every per-op-template kernel
        // bound to this fiber's main kernel via `bind_outer_scope`
        // (SRD-13c §"Per-Scope Canonical Kernel Cache" cache-and-
        // rebind primitive). Construction-time bind copies the
        // current values of parent constants into the op-template
        // kernel's extern slots; per-cycle inputs (`cycle`) are
        // set on each kernel directly via `FiberBuilder::set_inputs`.
        for (op_name, program) in &self.op_template_programs {
            let mut op_kernel = GkKernel::from_program(program.clone());
            op_kernel.bind_outer_scope(&fb.main_kernel);
            // Apply scope-bound values to op-template kernel too,
            // so iter-var-style externs the parent owns are also
            // reflected in the op-template kernel's input slots.
            for (idx, value) in &self.scope_values {
                if *idx < op_kernel.program().input_names().len() {
                    op_kernel.state().set_input(*idx, value.clone());
                }
            }
            fb.op_template_kernels.insert(op_name.clone(), op_kernel);
        }
        fb
    }
}

/// Read each init-output's pulled value off the kernel's state,
/// returning `(node_idx, port_idx, value)` triples suitable for
/// seeding fiber states. `Value::None` entries are skipped — the
/// scope-init pass should have errored on those before getting
/// here, but defensively we don't propagate them either way.
fn collect_init_overrides(kernel: &GkKernel) -> Vec<(usize, usize, Value)> {
    let program = kernel.program();
    let init_outputs = program.init_outputs();
    if init_outputs.is_empty() { return Vec::new(); }
    let mut out = Vec::with_capacity(init_outputs.len());
    let state = kernel.state_ref();
    for name in init_outputs {
        let Some(&(node_idx, port_idx)) = program.output_map_lookup(name) else { continue };
        match state.node_buffer(node_idx, port_idx) {
            Some(v) if !matches!(v, Value::None) => {
                out.push((node_idx, port_idx, v.clone()));
            }
            _ => {} // None / out-of-range — Plan B should have caught it
        }
    }
    out
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
    /// The fiber's main kernel — typically the activity-wide
    /// (workload / phase) program. State lives inside the
    /// kernel; access via [`Self::state`] and
    /// [`Self::state_ref`].
    main_kernel: GkKernel,
    /// SRD-13d Phase 9 — per-op-template kernel instances, keyed
    /// by op name. Each instance comes from
    /// [`GkKernel::from_program`] + [`GkKernel::bind_outer_scope`]
    /// (per SRD-13c §"Per-Scope Canonical Kernel Cache" — the
    /// cache-and-rebind primitive). Populated by
    /// [`OpBuilder::create_fiber_builder`] for every materialised
    /// op-template; flattened op-templates produce no entry and
    /// fall back to `main_kernel` in [`Self::resolve_pulls_for_op`].
    op_template_kernels: std::collections::HashMap<String, GkKernel>,
    /// Scope-bound input values (per-iteration extern bindings)
    /// that should persist across stanza-level
    /// `reset_inputs_from` resets. Empty for a builder created
    /// via plain [`FiberBuilder::new`]; populated by
    /// [`OpBuilder::create_fiber_builder`].
    scope_values: Vec<(usize, Value)>,
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
    pub fn pull_values(&self, state: &mut nbrs_variates::kernel::GkState, program: &GkProgram) -> Vec<nbrs_variates::node::Value> {
        self.positions.iter()
            .map(|(_, idx)| state.pull_by_index(program, *idx).clone())
            .collect()
    }

    /// Pull N tuples for consecutive cursor offsets `[base..base+count)`.
    /// Restores the base input after the pull completes.
    pub fn pull_range(
        &self,
        state: &mut nbrs_variates::kernel::GkState,
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
        state: &mut nbrs_variates::kernel::GkState,
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
        Value::Handle(_) => 0,
        Value::VecF32(v) => v.len() * 4,
        Value::VecI32(v) => v.len() * 4,
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
        let main_kernel = GkKernel::from_program(program);
        Self {
            main_kernel,
            op_template_kernels: std::collections::HashMap::new(),
            scope_values: Vec::new(),
        }
    }

    /// Borrow this fiber's main GK program.
    pub fn program(&self) -> &Arc<GkProgram> {
        self.main_kernel.program()
    }

    /// Mutable access to the fiber's main `GkState`. The state
    /// lives inside `main_kernel`; this accessor preserves the
    /// pre-restructure call shape for sites that wrote
    /// `fiber.state.…` directly.
    pub fn state(&mut self) -> &mut GkState {
        self.main_kernel.state()
    }

    /// Borrowed (`&`) accessor for the main state.
    pub fn state_ref(&self) -> &GkState {
        self.main_kernel.state_ref()
    }

    /// Set coordinates and begin a new evaluation scope.
    ///
    /// Bounded by [`GkProgram::coord_count`]: the slice is
    /// truncated to the program's declared coordinate count
    /// before being written. A phase kernel with no
    /// coordinates (e.g. all bindings are invariant within the
    /// stanza, only externs declared) has `coord_count = 0`,
    /// so this becomes a no-op rather than clobbering the
    /// extern slots that follow.
    ///
    /// SRD-13d Phase 9: the same coordinates are also written to
    /// every per-op-template kernel that declares them as
    /// coords. Each kernel binds its own input slot for `cycle`
    /// (cascaded from parent) so per-cycle propagation is a
    /// per-kernel `set_inputs`, not a chain walk.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        let main_n = coords.len().min(self.main_kernel.program().coord_count());
        if main_n > 0 {
            self.main_kernel.state().set_inputs(&coords[..main_n]);
        }
        for kernel in self.op_template_kernels.values_mut() {
            let n = coords.len().min(kernel.program().coord_count());
            if n > 0 {
                kernel.state().set_inputs(&coords[..n]);
            }
        }
    }

    /// Feed a source item into the GK state.
    ///
    /// Sets the ordinal as the coordinate input and injects field
    /// projections into the appropriate state slots (e.g.
    /// `base__ordinal`, `base__vector`).
    ///
    /// The ordinal write is bounded by [`GkProgram::coord_count`]
    /// — phases whose programs declare no coordinates (only
    /// externs and stanza-invariant bindings) skip the write
    /// rather than clobbering an extern slot. Field
    /// projections always write by name, so they're safe
    /// regardless of coord_count.
    ///
    /// SRD-13d Phase 9: ordinal + fields propagate to every
    /// op-template kernel that declares matching slots.
    pub fn set_source_item(&mut self, item: &nbrs_variates::source::SourceItem) {
        if self.main_kernel.program().coord_count() > 0 {
            self.main_kernel.state().set_inputs(&[item.ordinal]);
        }
        for (name, value) in &item.fields {
            if let Some(idx) = self.main_kernel.program().find_input(name) {
                self.main_kernel.state().set_input(idx, value.clone());
            }
        }
        for kernel in self.op_template_kernels.values_mut() {
            if kernel.program().coord_count() > 0 {
                kernel.state().set_inputs(&[item.ordinal]);
            }
            for (name, value) in &item.fields {
                if let Some(idx) = kernel.program().find_input(name) {
                    kernel.state().set_input(idx, value.clone());
                }
            }
        }
    }

    /// Reset capture inputs to defaults. Called at stanza
    /// boundaries to prevent capture leakage across stanzas.
    /// Coordinates are not reset. Scope-bound iter-var inputs
    /// (set by [`OpBuilder::create_fiber_builder`]) are
    /// re-applied after the reset so the iteration's bound
    /// values survive the boundary.
    pub fn reset_captures(&mut self) {
        let coord_count = self.main_kernel.program().coord_count();
        self.main_kernel.state().reset_inputs_from(coord_count);
        for (idx, value) in &self.scope_values {
            self.main_kernel.state().set_input(*idx, value.clone());
        }
        // Op-template kernels: same shape, but each one's coord
        // count comes from its own program.
        for kernel in self.op_template_kernels.values_mut() {
            let n = kernel.program().coord_count();
            kernel.state().reset_inputs_from(n);
        }
    }

    /// Invalidate all state: reset all inputs and mark all nodes dirty.
    /// Provides "clean slate" semantics.
    pub fn invalidate_all(&mut self) {
        self.main_kernel.state().invalidate_all();
        for kernel in self.op_template_kernels.values_mut() {
            kernel.state().invalidate_all();
        }
    }

    /// Store a captured value directly into GK state.
    ///
    /// Writes to the named port in GkState. Returns `true` if the
    /// port was found and the value stored, `false` if no port with
    /// this name exists in the program (value is dropped).
    pub fn capture(&mut self, name: &str, value: Value) -> bool {
        if let Some(idx) = self.main_kernel.program().find_input(name) {
            self.main_kernel.state().set_input(idx, value);
            true
        } else {
            false
        }
    }

    /// Materialize a [`PullPlan`] against this fiber's main GkState.
    /// O(plan_len) on the hot path, no name hashing — the plan
    /// holds pre-resolved indices.
    ///
    /// This is the cycle-time read path used by every wrapper that
    /// holds [`PullHandle`]s registered into the corresponding
    /// [`ScopeFixture`] at init (SRD 31 §"Pull plan vs bind plan",
    /// SRD 32 §"Init-Time Fixture and Consumer Self-Registration").
    ///
    /// [`PullPlan`]: crate::fixture::PullPlan
    /// [`PullHandle`]: crate::fixture::PullHandle
    /// [`ScopeFixture`]: crate::fixture::ScopeFixture
    pub fn resolve_pulls(
        &mut self,
        plan: &crate::fixture::PullPlan,
    ) -> crate::fixture::ResolvedPulls {
        plan.resolve(self.main_kernel.state())
    }

    /// SRD-13d Phase 9 resolve path — picks the right `GkState`
    /// for `op_name` and resolves the plan against it. When a
    /// per-op-template kernel was instanced for `op_name` (the
    /// op materialised), its state is used; otherwise the plan
    /// resolves against the fiber's main kernel state (the
    /// flattened op-template path).
    pub fn resolve_pulls_for_op(
        &mut self,
        op_name: &str,
        plan: &crate::fixture::PullPlan,
    ) -> crate::fixture::ResolvedPulls {
        match self.op_template_kernels.get_mut(op_name) {
            Some(kernel) => plan.resolve(kernel.state()),
            None => plan.resolve(self.main_kernel.state()),
        }
    }

    /// Resolve a template's op fields into a [`ResolvedFields`]
    /// ready for the inner adapter dispenser.
    ///
    /// `field_pull_names` is the set of GK output names referenced
    /// by op-field strings (`{id}`, `{url}`, …) that adapters look
    /// up by name via `ResolvedFields::get_value`/`get_str`. This
    /// is the *adapter*-facing read path; wrapper-side reads
    /// (validation, conditional, throttle) go through the
    /// [`PullPlan`] / [`PullHandle`] path on `ExecCtx::pulls`
    /// instead — see SRD 31 §"Pull plan vs bind plan".
    ///
    /// Convenience wrapper that builds the bind plan and batch
    /// config on demand. Use [`Self::resolve_cached`] in hot paths;
    /// the activity caches both artifacts per op template at setup
    /// time.
    ///
    /// [`PullPlan`]: crate::fixture::PullPlan
    /// [`PullHandle`]: crate::fixture::PullHandle
    pub fn resolve_with_field_pulls(
        &mut self,
        template: &ParsedOp,
        field_pull_names: &[String],
    ) -> crate::adapter::ResolvedFields {
        let stmt_field = template.op.get("stmt")
            .or_else(|| template.op.get("prepared"))
            .or_else(|| template.op.get("raw"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let bind_names = nbrs_workload::bindpoints::referenced_bindings(stmt_field);
        let plan = BindPlan::new(&bind_names, self.main_kernel.program());
        let batch_config = BatchConfig::from_params(&template.params);
        self.resolve_cached(template, field_pull_names, plan.as_ref(), &batch_config)
    }

    /// Resolve fields using pre-built bind plan and batch config.
    ///
    /// The plan and batch config are built once per op template at
    /// activity setup — this avoids per-cycle reconstruction of
    /// BindPlan from bind point names. See [`Self::resolve_with_field_pulls`]
    /// for the `field_pull_names` contract.
    pub fn resolve_cached(
        &mut self,
        template: &ParsedOp,
        field_pull_names: &[String],
        bind_plan: Option<&BindPlan>,
        batch_config: &BatchConfig,
    ) -> crate::adapter::ResolvedFields {
        let mut names = Vec::new();
        let mut values = Vec::new();

        for (key, value) in &template.op {
            names.push(key.clone());
            if let serde_json::Value::String(s) = value {
                let program = self.main_kernel.program().clone();
                let resolved = substitute_bind_points_with_state(
                    s, &program, self.main_kernel.state(),
                );

                // Preserve typed value for pure bind point references
                let trimmed = s.trim();
                if trimmed.starts_with('{') && trimmed.ends_with('}') && !trimmed.starts_with("{{") {
                    let name = &trimmed[1..trimmed.len()-1];
                    let bare = if let Some((_, n)) = name.split_once(':') { n } else { name };
                    if program.resolve_output(bare).is_some() {
                        values.push(self.main_kernel.state().pull(&program, bare).clone());
                        continue;
                    }
                }
                values.push(Value::Str(resolved));
            } else {
                values.push(Value::Str(value.to_string()));
            }
        }

        // Pull op-field bind point names that aren't already in
        // op.keys(). These appear in op fields like
        // `stmt: "INSERT ... VALUES ({id}, {vec})"` — the bind
        // point names (`id`, `vec`) need to be readable by the
        // adapter, separately from any `id`/`vec` keys it might
        // already have in `op`.
        let main_program = self.main_kernel.program().clone();
        for binding in field_pull_names {
            if !names.contains(binding) {
                if main_program.resolve_output(binding).is_some() {
                    names.push(binding.clone());
                    values.push(self.main_kernel.state().pull(&main_program, binding).clone());
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
                let program = self.main_kernel.program().clone();
                let base = self.main_kernel.state().get_input(0).as_u64();
                let rows = if let Some(target) = batch_config.target_bytes {
                    plan.pull_to_budget(
                        self.main_kernel.state(), &program, base,
                        target, batch_config.max_rows,
                    )
                } else {
                    plan.pull_range(
                        self.main_kernel.state(), &program, base, batch_config.size,
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

    /// Resolve op fields only (no field-pulled bind points).
    /// Convenience for simple cases / tests.
    pub fn resolve(&mut self, template: &ParsedOp) -> crate::adapter::ResolvedFields {
        self.resolve_with_field_pulls(template, &[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbrs_variates::assembly::{GkAssembler, WireRef};
    use nbrs_variates::nodes::hash::Hash64;
    use nbrs_variates::nodes::arithmetic::ModU64;

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

    /// SRD 11 §"Init Binding Contract" Plan B verification:
    /// after the activation kernel pulls an init binding, the
    /// pulled value must propagate to every fiber via
    /// `init_overrides`, and per-fiber pulls must read the seeded
    /// buffer rather than re-firing the eval.
    #[test]
    fn init_binding_fires_once_across_many_fibers() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc as StdArc;

        // Counting custom node: bumps a shared counter on every
        // eval call, returns U64(42). Tracks how many times its
        // eval body actually runs across the test.
        struct CountingNode {
            meta: nbrs_variates::node::NodeMeta,
            calls: StdArc<AtomicU64>,
        }
        impl nbrs_variates::node::GkNode for CountingNode {
            fn meta(&self) -> &nbrs_variates::node::NodeMeta { &self.meta }
            fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
                self.calls.fetch_add(1, Ordering::Relaxed);
                outputs[0] = Value::U64(42);
            }
        }

        let calls = StdArc::new(AtomicU64::new(0));
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        // Compile-const seed expression — wires empty.
        asm.add_node("ticks", Box::new(CountingNode {
            meta: nbrs_variates::node::NodeMeta {
                name: "ticks".into(),
                outs: vec![nbrs_variates::node::Port::new("output", nbrs_variates::node::PortType::U64)],
                ins: vec![],
            },
            calls: calls.clone(),
        }), vec![]);
        asm.add_output("ticks", WireRef::node("ticks"));
        asm.mark_init_output("ticks");

        let mut kernel = asm.compile().expect("compile");
        // Plan B normally runs in the executor; for this unit test
        // we simulate it by pulling the init binding once on the
        // activation kernel.
        let v = kernel.pull("ticks").clone();
        assert_eq!(v, Value::U64(42));
        let after_pull = calls.load(Ordering::Relaxed);
        // The fold pass evaluates the node once, then ConstU64
        // replaces it (init binding is pure compile-const here),
        // so subsequent state pulls return the leaf const without
        // re-eval. Assert the call count never grows from here.
        let builder = OpBuilder::new(kernel);

        // Spawn many fibers, each pulls the init binding. None
        // should trigger an eval — the init_overrides path seeds
        // the buffer, and the post-fold leaf-const path returns
        // the constant directly.
        for _ in 0..32 {
            let mut fiber = builder.create_fiber_builder();
            fiber.set_inputs(&[0]);
            let pulled = fiber.state.pull(&builder.program(), "ticks").clone();
            assert_eq!(pulled, Value::U64(42));
        }
        let after_fibers = calls.load(Ordering::Relaxed);
        assert_eq!(after_pull, after_fibers,
            "init binding 'ticks' eval must not re-fire across fibers \
             (eval calls before fibers: {after_pull}, after 32 fibers: {after_fibers})");
        // Independent: confirm the eval ran at most once during
        // compile-time fold + the activation pull.
        assert!(after_fibers <= 1,
            "expected at most one eval across compile fold + activation pull, got {after_fibers}");
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
