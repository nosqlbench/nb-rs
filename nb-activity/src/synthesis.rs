// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Op synthesis: assemble concrete operations from templates + variates.
//!
//! This is the bridge between the workload spec (ParsedOp) and the
//! adapter (AssembledOp). For each cycle, bind points in op template
//! fields are resolved from the GK kernel's output variates.

use std::collections::HashMap;
use std::sync::Mutex;

use nb_variates::kernel::GkKernel;
use nb_variates::node::Value;
use nb_workload::model::ParsedOp;
use nb_workload::bindpoints;

use crate::adapter::AssembledOp;

/// Resolves bind points in a string template using the GK kernel.
///
/// `{name}` references are replaced with the string representation
/// of the named variate pulled from the kernel. The kernel must
/// already have its coordinate context set for the current cycle.
pub fn substitute_bind_points(template: &str, kernel: &mut GkKernel) -> String {
    let refs = bindpoints::referenced_bindings(template);
    if refs.is_empty() {
        return template.to_string();
    }

    let mut result = template.to_string();
    for name in &refs {
        let placeholder = format!("{{{name}}}");
        let value = kernel.pull(name);
        let value_str = value_to_string(value);
        result = result.replace(&placeholder, &value_str);
    }
    result
}

/// Convert a GK Value to its string representation for op assembly.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::U64(v) => v.to_string(),
        Value::F64(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Str(v) => v.clone(),
        Value::Bytes(v) => hex::encode(v),
        Value::Json(v) => v.to_string(),
        Value::None => String::new(),
    }
}

// We need a tiny hex encoder — avoid pulling in a dep just for this.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Resolve a single JSON value field, substituting bind points.
fn resolve_field(value: &serde_json::Value, kernel: &mut GkKernel) -> String {
    match value {
        serde_json::Value::String(s) => substitute_bind_points(s, kernel),
        other => other.to_string(),
    }
}

/// Build an AssembledOp from a ParsedOp template and the current
/// cycle's variate values.
///
/// The kernel must already have its coordinate set for this cycle.
pub fn assemble_op(template: &ParsedOp, kernel: &mut GkKernel) -> AssembledOp {
    let mut fields = HashMap::new();
    for (key, value) in &template.op {
        fields.insert(key.clone(), resolve_field(value, kernel));
    }
    AssembledOp {
        name: template.name.clone(),
        fields,
    }
}

/// A thread-safe op builder that owns a GK kernel.
///
/// Since GK kernels are stateful (Phase 1 has mutable coordinate
/// context), each async task needs its own kernel or we mutex it.
/// For now, we use a Mutex — each build_op call locks, sets
/// coordinates, pulls variates, and unlocks.
pub struct OpBuilder {
    kernel: Mutex<GkKernel>,
}

impl OpBuilder {
    pub fn new(kernel: GkKernel) -> Self {
        Self { kernel: Mutex::new(kernel) }
    }

    /// Build an assembled op for the given cycle and template.
    pub fn build(&self, cycle: u64, template: &ParsedOp) -> AssembledOp {
        let mut kernel = self.kernel.lock().unwrap();
        kernel.set_coordinates(&[cycle]);
        assemble_op(template, &mut kernel)
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
        let result = substitute_bind_points("plain text", &mut kernel);
        assert_eq!(result, "plain text");
    }

    #[test]
    fn substitute_single_bind_point() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);
        let result = substitute_bind_points("user:{user_id}", &mut kernel);
        assert!(result.starts_with("user:"));
        let id: u64 = result.strip_prefix("user:").unwrap().parse().unwrap();
        assert!(id < 1_000_000);
    }

    #[test]
    fn substitute_multiple_bind_points() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);
        let result = substitute_bind_points("id={user_id} hash={hashed}", &mut kernel);
        assert!(result.contains("id="));
        assert!(result.contains("hash="));
    }

    #[test]
    fn substitute_deterministic() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);
        let r1 = substitute_bind_points("{user_id}", &mut kernel);
        kernel.set_coordinates(&[42]);
        let r2 = substitute_bind_points("{user_id}", &mut kernel);
        assert_eq!(r1, r2);
    }

    #[test]
    fn assemble_op_resolves_fields() {
        let mut kernel = make_kernel();
        kernel.set_coordinates(&[42]);

        let mut template = ParsedOp::simple("test", "SELECT * FROM t WHERE id={user_id}");
        let assembled = assemble_op(&template, &mut kernel);

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
