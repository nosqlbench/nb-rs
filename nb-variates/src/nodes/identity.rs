// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Identity and constant nodes.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Slot, Value};

/// Passthrough: output equals input.
///
/// Signature: `identity(input: u64) -> (u64)`
///
/// Emits the input cycle counter unchanged. Useful as a placeholder
/// during DAG construction, as a debugging tap, or when the raw
/// sequential ordinal is the desired value (e.g., auto-incrementing
/// primary keys). Also serves as the simplest reference node for
/// testing the GkNode trait.
///
/// JIT level: P2 (compiled_u64 is a trivial copy).
pub struct Identity {
    meta: NodeMeta,
}

impl Identity {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "identity".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for Identity {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = inputs[0].clone();
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = inputs[0];
        }))
    }
}

/// Emit a fixed u64 value (no inputs).
///
/// Signature: `const(value: u64) -> (u64)`
///
/// Source node that always produces the same u64 regardless of cycle.
/// Use for injecting literal parameters into a DAG, such as a fixed
/// partition key, an epoch timestamp base, or an addend for `add`.
/// Takes no inputs, so it sits at a DAG root.
///
/// JIT level: P2 (compiled_u64 emits a captured constant).
pub struct ConstU64 {
    meta: NodeMeta,
    value: u64,
}

impl ConstU64 {
    pub fn new(value: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "const".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::const_u64("value", value)],
            },
            value,
        }
    }
}

impl GkNode for ConstU64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.value);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let value = self.value;
        Some(Box::new(move |_inputs, outputs| {
            outputs[0] = value;
        }))
    }
}

/// Emit a fixed string value (no inputs).
///
/// Signature: `const_str(value: String) -> (String)`
///
/// Source node that always produces the same string regardless of cycle.
/// Use for injecting literal string parameters into a DAG, such as a
/// fixed table name, a static label, or a separator for string
/// concatenation pipelines.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct ConstStr {
    meta: NodeMeta,
    value: String,
}

impl ConstStr {
    pub fn new(value: impl Into<String>) -> Self {
        let value: String = value.into();
        Self {
            meta: NodeMeta {
                name: "const_str".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::const_str("value", value.clone())],
            },
            value,
        }
    }
}

impl GkNode for ConstStr {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.value.clone());
    }
    // No compiled_u64 — String output.
}
