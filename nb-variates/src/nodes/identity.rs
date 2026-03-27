// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Identity and constant nodes.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Value};

/// Passthrough: output equals input.
///
/// Signature: `(input: u64) -> (u64)`
pub struct Identity {
    meta: NodeMeta,
}

impl Identity {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "identity".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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
/// Signature: `() -> (u64)`
pub struct ConstU64 {
    meta: NodeMeta,
    value: u64,
}

impl ConstU64 {
    pub fn new(value: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "const".into(),
                inputs: vec![],
                outputs: vec![Port::u64("output")],
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
/// Signature: `() -> (String)`
pub struct ConstStr {
    meta: NodeMeta,
    value: String,
}

impl ConstStr {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            meta: NodeMeta {
                name: "const_str".into(),
                inputs: vec![],
                outputs: vec![Port::str("output")],
            },
            value: value.into(),
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
