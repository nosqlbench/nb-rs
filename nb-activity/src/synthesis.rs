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
                Some(v) => v.to_string(),
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
                .map(|idx| state.get_input(idx).to_string())
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
                    return state.get_input(idx).to_string();
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
}

impl OpBuilder {
    pub fn new(kernel: GkKernel) -> Self {
        let program = kernel.into_program();
        Self { program }
    }

    /// Access the shared GK program.
    pub fn program(&self) -> Arc<GkProgram> {
        self.program.clone()
    }

    /// Create a per-fiber builder. No locks, no sharing — the fiber
    /// owns its state exclusively.
    pub fn create_fiber_builder(&self) -> FiberBuilder {
        FiberBuilder::new(self.program.clone())
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
pub fn validate_bind_points(
    templates: &[ParsedOp],
    program: &GkProgram,
) {
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
                            eprintln!(
                                "warning: unresolved bind point '{{{name}}}' in op '{}' field '{field_name}'. \
                                 Not found in GK bindings, captures, or inputs.",
                                template.name
                            );
                        }
                    }
                }
            }
        }
    }
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

    /// Reset all ports to defaults. Called at stanza boundaries
    /// to prevent capture leakage across stanzas.
    pub fn reset_ports(&mut self) {
        self.state.reset_ports();
    }

    /// Invalidate all state: reset ports and mark all nodes dirty.
    /// Provides "clean slate" semantics — the next evaluation
    /// behaves as if the state were freshly created.
    pub fn invalidate_all(&mut self) {
        self.state.invalidate_all();
    }

    /// Store a captured value directly into GK state.
    ///
    /// Writes to the named port in GkState. If no port with this
    /// name is declared in the program, the value is silently dropped.
    pub fn capture(&mut self, name: &str, value: Value) {
        if let Some(idx) = self.program.find_port(name) {
            self.state.set_port(idx, value);
        }
    }

    /// Resolve all fields in a template to typed values and strings.
    /// Returns ResolvedFields for consumption by an OpDispenser.
    ///
    /// `extra_bindings` are GK output names that should be included in
    /// the resolved fields even though they don't appear in the op map.
    /// Used by the validation layer to pull ground truth bindings.
    pub fn resolve_with_extras(
        &mut self,
        template: &ParsedOp,
        extra_bindings: &[String],
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

        crate::adapter::ResolvedFields::new(names, values)
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
}
