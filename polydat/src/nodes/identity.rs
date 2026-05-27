// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Identity and constant nodes.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, PortType, Slot, Value};

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

impl Default for Identity {
    fn default() -> Self {
        Self::new()
    }
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

/// Passthrough for external port values (captures).
///
/// Reads a single input (from a `WireSource::Port`) and copies it
/// unchanged to the output. The port type is declared based on the
/// port's default value type at construction time.
///
/// This node is auto-inserted by the compiler for `extern` port
/// declarations, making captured values available as GK outputs.
pub struct PortPassthrough {
    meta: NodeMeta,
}

impl PortPassthrough {
    /// Create a port passthrough with the given output type.
    pub fn new(name: &str, port_type: crate::node::PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: format!("__port_{name}"),
                outs: vec![Port::new("output", port_type)],
                ins: vec![Slot::Wire(Port::new("input", port_type))],
            },
        }
    }
}

impl GkNode for PortPassthrough {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = inputs[0].clone();
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
    /// `Arc<str>` so per-cycle `eval` emissions share a single
    /// heap allocation across every kernel that uses this node
    /// — `Value::Str` clones become atomic increments, not
    /// heap copies. Matches the grammar's "final" / "init"
    /// shareability intent.
    value: std::sync::Arc<str>,
}

impl ConstStr {
    pub fn new(value: impl Into<std::sync::Arc<str>>) -> Self {
        let value: std::sync::Arc<str> = value.into();
        Self {
            meta: NodeMeta {
                name: "const_str".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::const_str("value", value.to_string())],
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

/// Emit a fixed [`Value::Handle`] (no inputs).
///
/// Signature: `const_handle() -> (Handle)`
///
/// Created by the constant-folding pass to replace an `init`
/// binding whose evaluation produced a `Value::Handle` (e.g.
/// `init prebuffered = dataset_prebuffer(...)`). Without this
/// replacement, the original side-effect-bearing node would
/// stay in the program graph with its eval intact, and every
/// fresh fiber's `GkState` would re-fire the eval at first
/// downstream pull — producing a per-fiber stampede that, in
/// the prebuffer case, exhausts the per-process thread limit
/// when vectordata's HTTP workers spin up concurrently.
///
/// The handle's `Arc` is cloned per `eval()` call (one atomic
/// refcount bump); the underlying resource is shared.
///
/// JIT level: P1 (Handle output; no compiled_u64 path).
pub struct ConstHandle {
    meta: NodeMeta,
    value: std::sync::Arc<dyn std::any::Any + Send + Sync>,
}

impl ConstHandle {
    pub fn new(value: std::sync::Arc<dyn std::any::Any + Send + Sync>) -> Self {
        Self {
            meta: NodeMeta {
                name: "const_handle".into(),
                outs: vec![Port::new("output", PortType::Handle)],
                // No const slot — the handle is type-erased and
                // doesn't fit the const-slot vocabulary; fold-pass
                // synthesises this node directly with no input wires.
                ins: vec![],
            },
            value,
        }
    }
}

impl GkNode for ConstHandle {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Handle(self.value.clone());
    }
}

/// SRD 71 — leaf const for [`Value::Ext`]-typed values
/// (Partition, PartitionSpec, PartitionList, …).
///
/// Mirrors [`ConstHandle`]'s shape for `Handle`-typed values:
/// fold-pass synthesises one of these in place of any
/// node-with-wiring whose evaluated output is an `Ext` value,
/// so the post-fold kernel can read the constant via
/// `get_constant` (no input slots, eval just emits the stored
/// value).
pub struct ConstExt {
    meta: NodeMeta,
    value: Box<dyn crate::node::ReflectedValue>,
}

impl ConstExt {
    pub fn new(value: Box<dyn crate::node::ReflectedValue>) -> Self {
        Self {
            meta: NodeMeta {
                name: "const_ext".into(),
                outs: vec![Port::new("output", PortType::Ext)],
                ins: vec![],
            },
            value,
        }
    }
}

impl GkNode for ConstExt {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Ext(self.value.clone());
    }
}
