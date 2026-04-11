// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Op synthesis: resolve bind points in op templates from GK variates.
//!
//! This is the bridge between the workload spec (ParsedOp) and the
//! adapter (ResolvedFields). For each cycle, bind points in op template
//! fields are resolved from the GK kernel's output variates.

use std::sync::Arc;

use nb_variates::kernel::{GkKernel, GkProgram, GkState, CaptureContext};
use nb_variates::node::Value;
use nb_workload::model::ParsedOp;
use nb_workload::bindpoints::{self, BindPoint, BindQualifier};

/// Resolves bind points in a string template using the GK kernel
/// and optionally a capture context.
///
/// Resolution order for unqualified `{name}`:
/// 1. GK binding outputs
/// 2. Capture context (if provided)
/// 3. Coordinate inputs (as raw u64)
///
/// Qualified bind points:
/// - `{bind:name}` → GK output only
/// - `{capture:name}` / `{port:name}` → capture context only
/// - `{coord:name}` → coordinate value only
pub fn substitute_bind_points(
    template: &str,
    kernel: &mut GkKernel,
    captures: Option<&CaptureContext>,
) -> String {
    let bind_points = bindpoints::extract_bind_points(template);
    if bind_points.is_empty() {
        return template.to_string();
    }

    let mut result = template.to_string();
    for bp in &bind_points {
        match bp {
            BindPoint::Reference { name, qualifier } => {
                let value_str = resolve_bind_point(name, qualifier, kernel, captures);
                // Replace the full placeholder including qualifier
                let placeholder = match qualifier {
                    BindQualifier::None => format!("{{{name}}}"),
                    BindQualifier::Coord => format!("{{coord:{name}}}"),
                    BindQualifier::Bind => format!("{{bind:{name}}}"),
                    BindQualifier::Capture => format!("{{capture:{name}}}"),
                    BindQualifier::Port => format!("{{port:{name}}}"),
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
    captures: Option<&CaptureContext>,
) -> String {
    match qualifier {
        BindQualifier::Bind => {
            // GK output only
            match kernel.program().resolve_output(name) {
                Some(_) => value_to_string(kernel.pull(name)),
                None => format!("{{bind:{name}}}"), // unresolved
            }
        }
        BindQualifier::Capture | BindQualifier::Port => {
            // Capture context only
            captures
                .and_then(|ctx| ctx.get(name))
                .map(value_to_string)
                .unwrap_or_else(|| format!("{{capture:{name}}}"))
        }
        BindQualifier::Coord => {
            // Coordinate value directly
            match kernel.get_coord(name) {
                Some(v) => v.to_string(),
                None => {
                    // Might be a GK output that shadows a coord name
                    match kernel.program().resolve_output(name) {
                        Some(_) => value_to_string(kernel.pull(name)),
                        None => format!("{{coord:{name}}}"),
                    }
                }
            }
        }
        BindQualifier::None => {
            // Unqualified: try GK output first, then captures, then coordinate
            if kernel.program().resolve_output(name).is_some() {
                return value_to_string(kernel.pull(name));
            }
            if let Some(ctx) = captures
                && let Some(val) = ctx.get(name) {
                    return value_to_string(val);
                }
            // Last resort: might be a coordinate exposed as output
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
    captures: Option<&CaptureContext>,
    params: &std::collections::HashMap<String, String>,
) -> String {
    let bind_points = bindpoints::extract_bind_points(template);
    if bind_points.is_empty() { return template.to_string(); }

    let mut result = template.to_string();
    for bp in &bind_points {
        match bp {
            BindPoint::Reference { name, qualifier } => {
                let value_str = resolve_bind_point_with_state(name, qualifier, program, state, captures, params);
                let placeholder = match qualifier {
                    BindQualifier::None => format!("{{{name}}}"),
                    BindQualifier::Coord => format!("{{coord:{name}}}"),
                    BindQualifier::Bind => format!("{{bind:{name}}}"),
                    BindQualifier::Capture => format!("{{capture:{name}}}"),
                    BindQualifier::Port => format!("{{port:{name}}}"),
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
    captures: Option<&CaptureContext>,
    params: &std::collections::HashMap<String, String>,
) -> String {
    match qualifier {
        BindQualifier::Bind => {
            if program.resolve_output(name).is_some() {
                state.pull(program, name).to_display_string()
            } else {
                format!("{{bind:{name}}}")
            }
        }
        BindQualifier::Capture | BindQualifier::Port => {
            captures
                .and_then(|ctx| ctx.get(name))
                .map(|v| v.to_display_string())
                .unwrap_or_else(|| format!("{{capture:{name}}}"))
        }
        BindQualifier::Coord => {
            program.coord_names().iter()
                .position(|n| n == name)
                .map(|idx| state.get_coord(idx).to_string())
                .or_else(|| {
                    if program.resolve_output(name).is_some() {
                        Some(state.pull(program, name).to_display_string())
                    } else { None }
                })
                .unwrap_or_else(|| format!("{{coord:{name}}}"))
        }
        BindQualifier::None => {
            // Resolution order: GK output → captures → workload params → unresolved
            if program.resolve_output(name).is_some() {
                return state.pull(program, name).to_display_string();
            }
            if let Some(ctx) = captures
                && let Some(val) = ctx.get(name) {
                    return val.to_display_string();
                }
            if let Some(val) = params.get(name) {
                return val.clone();
            }
            format!("{{{name}}}")
        }
    }
}

/// Shared op builder that distributes per-fiber builders.
///
/// Holds the `Arc<GkProgram>` (immutable, shared). Each executor
/// fiber calls `create_fiber_builder()` to get its own `FiberBuilder`
/// with private `GkState` and `CaptureContext` — no locks, no
/// contention on the hot path.
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
        FiberBuilder {
            program: self.program.clone(),
            state: self.program.create_state(),
            captures: CaptureContext::new(),
            params: Arc::new(std::collections::HashMap::new()),
        }
    }
}

/// Per-fiber op builder. Owns its own GkState and CaptureContext.
/// No locks, no synchronization, no contention.
///
/// Created via `OpBuilder::create_fiber_builder()` at fiber startup.
pub struct FiberBuilder {
    program: Arc<GkProgram>,
    state: GkState,
    captures: CaptureContext,
    /// Workload parameters: constant per run, shared across fibers.
    params: Arc<std::collections::HashMap<String, String>>,
}

impl FiberBuilder {
    /// Create a new fiber builder from a shared GK program.
    pub fn new(program: Arc<GkProgram>) -> Self {
        Self::with_params(program, Arc::new(std::collections::HashMap::new()))
    }

    /// Create with workload parameters available for bind-point resolution.
    pub fn with_params(program: Arc<GkProgram>, params: Arc<std::collections::HashMap<String, String>>) -> Self {
        let state = program.create_state();
        Self {
            program,
            state,
            captures: CaptureContext::new(),
            params,
        }
    }

    /// Set coordinates and begin a new isolation scope.
    pub fn set_coordinates(&mut self, coords: &[u64]) {
        self.state.set_coordinates(coords);
    }

    /// Store a captured value.
    pub fn capture(&mut self, name: &str, value: Value) {
        self.captures.set(name, value);
    }

    /// Reset captures for a new stanza.
    pub fn reset_captures(&mut self, cycle: u64) {
        self.captures.reset(cycle);
    }

    /// Apply captures to volatile/sticky ports.
    pub fn apply_captures(&mut self) {
        self.captures.apply_to_state(&self.program, &mut self.state);
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
                    s, &self.program, &mut self.state, Some(&self.captures), &self.params,
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
        asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
        asm.add_node("user_id", Box::new(ModU64::new(1_000_000)), vec![WireRef::node("hashed")]);
        asm.add_output("user_id", WireRef::node("user_id"));
        asm.add_output("hashed", WireRef::node("hashed"));
        asm.compile().unwrap()
    }

    #[test]
    fn substitute_no_bind_points() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[0]);
        let result = substitute_bind_points("plain text", &mut kernel, None);
        assert_eq!(result, "plain text");
    }

    #[test]
    fn substitute_single_bind_point() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);
        let result = substitute_bind_points("user:{user_id}", &mut kernel, None);
        assert!(result.starts_with("user:"));
        let id: u64 = result.strip_prefix("user:").unwrap().parse().unwrap();
        assert!(id < 1_000_000);
    }

    #[test]
    fn substitute_multiple_bind_points() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);
        let result = substitute_bind_points("id={user_id} hash={hashed}", &mut kernel, None);
        assert!(result.contains("id="));
        assert!(result.contains("hash="));
    }

    #[test]
    fn substitute_deterministic() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);
        let r1 = substitute_bind_points("{user_id}", &mut kernel, None);
        kernel.set_coordinates(&[42]);
        let r2 = substitute_bind_points("{user_id}", &mut kernel, None);
        assert_eq!(r1, r2);
    }

    #[test]
    fn fiber_builder_resolves_fields() {
        let kernel = make_kernel();
        let builder = OpBuilder::new(kernel);
        let mut fiber = builder.create_fiber_builder();
        let template = ParsedOp::simple("test", "SELECT * FROM t WHERE id={user_id}");

        fiber.set_coordinates(&[42]);
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

        fiber.set_coordinates(&[42]);
        let a = fiber.resolve(&template);
        fiber.set_coordinates(&[42]);
        let b = fiber.resolve(&template);
        assert_eq!(a.strings()[0], b.strings()[0]);
    }

    #[test]
    fn fiber_builder_different_cycles() {
        let kernel = make_kernel();
        let builder = OpBuilder::new(kernel);
        let mut fiber = builder.create_fiber_builder();
        let template = ParsedOp::simple("op1", "id={user_id}");

        fiber.set_coordinates(&[42]);
        let a = fiber.resolve(&template);
        fiber.set_coordinates(&[43]);
        let b = fiber.resolve(&template);
        assert_ne!(a.strings()[0], b.strings()[0]);
    }
}
