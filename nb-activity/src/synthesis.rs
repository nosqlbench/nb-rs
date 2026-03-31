// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Op synthesis: assemble concrete operations from templates + variates.
//!
//! This is the bridge between the workload spec (ParsedOp) and the
//! adapter (AssembledOp). For each cycle, bind points in op template
//! fields are resolved from the GK kernel's output variates.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nb_variates::kernel::{GkKernel, GkProgram, GkState, CaptureContext};
use nb_variates::node::Value;
use nb_workload::model::ParsedOp;
use nb_workload::bindpoints::{self, BindPoint, BindQualifier};

use crate::adapter::AssembledOp;

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
            if let Some(ctx) = captures {
                if let Some(val) = ctx.get(name) {
                    return value_to_string(val);
                }
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


/// Resolve a single JSON value field, substituting bind points.
fn resolve_field_with_captures(
    value: &serde_json::Value,
    kernel: &mut GkKernel,
    captures: Option<&CaptureContext>,
) -> String {
    match value {
        serde_json::Value::String(s) => substitute_bind_points(s, kernel, captures),
        other => other.to_string(),
    }
}

/// Build an AssembledOp from a ParsedOp template, the GK kernel,
/// and an optional capture context.
///
/// The kernel must already have its coordinate set for this cycle.
/// Build an AssembledOp using the program/state split (no lock needed).
fn assemble_op_with_state(
    template: &ParsedOp,
    program: &GkProgram,
    state: &mut GkState,
    captures: Option<&CaptureContext>,
) -> AssembledOp {
    let mut fields = HashMap::new();
    let mut typed_fields = HashMap::new();
    for (key, value) in &template.op {
        if let serde_json::Value::String(s) = value {
            let resolved = substitute_bind_points_with_state(s, program, state, captures);
            fields.insert(key.clone(), resolved.clone());

            // Try to preserve typed value for pure bind point references
            let trimmed = s.trim();
            if trimmed.starts_with('{') && trimmed.ends_with('}') && !trimmed.starts_with("{{") {
                let name = &trimmed[1..trimmed.len()-1];
                let bare = if let Some((_, n)) = name.split_once(':') { n } else { name };
                if program.resolve_output(bare).is_some() {
                    let val = state.pull(program, bare).clone();
                    typed_fields.insert(key.clone(), val);
                    continue;
                }
            }
            typed_fields.insert(key.clone(), Value::Str(resolved));
        } else {
            let s = value.to_string();
            fields.insert(key.clone(), s.clone());
            typed_fields.insert(key.clone(), Value::Str(s));
        }
    }
    AssembledOp { name: template.name.clone(), typed_fields, fields }
}

/// Substitute bind points using program + state (no GkKernel wrapper).
fn substitute_bind_points_with_state(
    template: &str,
    program: &GkProgram,
    state: &mut GkState,
    captures: Option<&CaptureContext>,
) -> String {
    let bind_points = bindpoints::extract_bind_points(template);
    if bind_points.is_empty() { return template.to_string(); }

    let mut result = template.to_string();
    for bp in &bind_points {
        match bp {
            BindPoint::Reference { name, qualifier } => {
                let value_str = resolve_bind_point_with_state(name, qualifier, program, state, captures);
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
            if program.resolve_output(name).is_some() {
                return state.pull(program, name).to_display_string();
            }
            if let Some(ctx) = captures {
                if let Some(val) = ctx.get(name) {
                    return val.to_display_string();
                }
            }
            format!("{{{name}}}")
        }
    }
}

/// Backward-compatible: build using GkKernel wrapper.
pub fn assemble_op(
    template: &ParsedOp,
    kernel: &mut GkKernel,
    captures: Option<&CaptureContext>,
) -> AssembledOp {
    let mut fields = HashMap::new();
    let mut typed_fields = HashMap::new();
    for (key, value) in &template.op {
        let resolved_str = resolve_field_with_captures(value, kernel, captures);
        fields.insert(key.clone(), resolved_str);
        // For typed fields, pull the raw Value from the kernel if the field
        // is a pure bind point reference (single {name} covering the whole field)
        if let serde_json::Value::String(s) = value {
            let trimmed = s.trim();
            if trimmed.starts_with('{') && trimmed.ends_with('}') && !trimmed.starts_with("{{") {
                let name = &trimmed[1..trimmed.len()-1];
                // Strip qualifier if present
                let bare_name = if let Some((_qual, n)) = name.split_once(':') { n } else { name };
                if kernel.program().resolve_output(bare_name).is_some() {
                    let val = kernel.pull(bare_name).clone();
                    typed_fields.insert(key.clone(), val);
                    continue;
                }
            }
        }
        // Default: string value
        typed_fields.insert(key.clone(), nb_variates::node::Value::Str(fields[key].clone()));
    }
    AssembledOp {
        name: template.name.clone(),
        typed_fields,
        fields,
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
    /// Fallback mutex-based builder for backward compatibility with
    /// the `build(cycle, template)` API. New code should use
    /// `create_fiber_builder()` instead.
    fallback_kernel: Mutex<GkKernel>,
}

impl OpBuilder {
    pub fn new(kernel: GkKernel) -> Self {
        let program = kernel.program().clone();
        Self {
            program,
            fallback_kernel: Mutex::new(kernel),
        }
    }

    /// Create a per-fiber builder. No locks, no sharing — the fiber
    /// owns its state exclusively.
    pub fn create_fiber_builder(&self) -> FiberBuilder {
        FiberBuilder {
            program: self.program.clone(),
            state: self.program.create_state(),
            captures: CaptureContext::new(),
        }
    }

    /// Backward-compatible build (uses mutex). Prefer `create_fiber_builder()`.
    pub fn build(&self, cycle: u64, template: &ParsedOp) -> AssembledOp {
        let mut kernel = self.fallback_kernel.lock().unwrap();
        kernel.set_coordinates(&[cycle]);
        assemble_op_with_kernel(template, &mut kernel, None)
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
}

impl FiberBuilder {
    /// Set coordinates and begin a new isolation scope.
    pub fn set_coordinates(&mut self, coords: &[u64]) {
        self.state.set_coordinates(coords);
    }

    /// Build an assembled op for the current coordinates.
    pub fn build(&mut self, template: &ParsedOp) -> AssembledOp {
        assemble_op_with_state(template, &self.program, &mut self.state, Some(&self.captures))
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
}

/// Assemble using the old GkKernel API (backward compat).
fn assemble_op_with_kernel(
    template: &ParsedOp,
    kernel: &mut GkKernel,
    captures: Option<&CaptureContext>,
) -> AssembledOp {
    let mut fields = HashMap::new();
    let mut typed_fields = HashMap::new();
    for (key, value) in &template.op {
        let resolved_str = resolve_field_with_captures(value, kernel, captures);
        fields.insert(key.clone(), resolved_str.clone());
        typed_fields.insert(key.clone(), Value::Str(resolved_str));
    }
    AssembledOp { name: template.name.clone(), typed_fields, fields }
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
    fn assemble_op_resolves_fields() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);

        let template = ParsedOp::simple("test", "SELECT * FROM t WHERE id={user_id}");
        let assembled = assemble_op(&template, &mut kernel, None);

        assert_eq!(assembled.name, "test");
        let stmt = &assembled.fields["stmt"];
        assert!(stmt.starts_with("SELECT * FROM t WHERE id="));
        assert!(!stmt.contains("{user_id}"));
    }

    #[test]
    fn op_builder_thread_safe() {
        let kernel = make_kernel();
        let builder = OpBuilder::new(kernel);
        let template = ParsedOp::simple("op1", "id={user_id}");

        let op1 = builder.build(42, &template);
        let op2 = builder.build(43, &template);

        assert!(!op1.fields["stmt"].contains("{user_id}"));
        assert!(!op2.fields["stmt"].contains("{user_id}"));
        // Different cycles should produce different ids (probably)
        assert_ne!(op1.fields["stmt"], op2.fields["stmt"]);
    }

    #[test]
    fn op_builder_deterministic() {
        let kernel = make_kernel();
        let builder = OpBuilder::new(kernel);
        let template = ParsedOp::simple("op1", "id={user_id}");

        let a = builder.build(42, &template);
        let b = builder.build(42, &template);
        assert_eq!(a.fields["stmt"], b.fields["stmt"]);
    }
}
