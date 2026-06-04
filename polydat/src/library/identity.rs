// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Identity and constant nodes.

use crate::ast::{PolydatNode, NodeMeta, Port, PortType, Slot, Value};

/// Passthrough: output equals input. SRD-80 PR B.8 — polymorphic
/// via PolyWire. The runtime port type is resolved by the
/// assembler from the upstream wire's type and passed to
/// `Identity::new(input_type)`.
///
/// JIT is disabled (Value isn't a u64-buffer carrier); for a
/// u64-only fast path, the assembler can synthesize a typed
/// alternative.
#[crate::polydat_node(category = Diagnostic)]
fn identity(input: Value) -> Value {
    input
}

/// Passthrough for external port values (captures).
///
/// Reads a single input (from a `WireSource::Port`) and copies it
/// unchanged to the output. The port type is declared based on the
/// port's default value type at construction time.
///
/// This node is auto-inserted by the compiler for `extern` port
/// declarations, making captured values available as Polydat outputs.
pub struct PortPassthrough {
    meta: NodeMeta,
}

impl PortPassthrough {
    /// Create a port passthrough with the given output type.
    pub fn new(name: &str, port_type: crate::ast::PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: format!("__port_{name}"),
                outs: vec![Port::new("output", port_type)],
                ins: vec![Slot::Wire(Port::new("input", port_type))],
            },
        }
    }
}

impl PolydatNode for PortPassthrough {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = inputs[0].clone();
    }
}

/// Emit a fixed u64 value (no inputs).
///
/// Signature: `const_u64(value: u64) -> (u64)`
///
/// Source node that always produces the same u64 regardless of cycle.
/// Use for injecting literal parameters into a DAG, such as a fixed
/// partition key, an epoch timestamp base, or an addend for `add`.
/// Takes no inputs, so it sits at a DAG root.
///
/// JIT level: P2 (compiled_u64 emits a captured constant via the
/// `#[polydat_node]`-emitted body capture).
///
/// SRD-80b Phase E migration: `Const<u64>` arg → owned `u64` field;
/// macro auto-emits the matching `compiled_u64()` constant-capture
/// fast path. Operator-facing rename `const` → `const_u64` aligns
/// with the per-type naming scheme already used for `const_f64` /
/// `const_bool` (see `library::fixed`).
#[crate::polydat_node(category = Math)]
fn const_u64(value: crate::derive_support::Const<u64>) -> u64 {
    *value
}

/// Emit a fixed string value (no inputs).
///
/// Signature: `const_str(value: String) -> (Arc<str>)`
///
/// Source node that always produces the same string regardless of cycle.
/// Use for injecting literal string parameters into a DAG, such as a
/// fixed table name, a static label, or a separator for string
/// concatenation pipelines.
///
/// JIT level: P1 (Str output; no compiled_u64 path).
///
/// SRD-80b Phase E migration: `Const<&str>` source captures the
/// owned `String`; `#[poly_const]` derives an `Arc<str>` cache at
/// construction time, so per-cycle eval is a refcount bump on a
/// single heap allocation — matches the previous shared-Arc
/// behaviour (one heap allocation across every kernel using the
/// node). The macro emits `ConstStr::new(value: String)`.
fn const_str_arc(s: &str) -> std::sync::Arc<str> {
    std::sync::Arc::from(s)
}

#[crate::polydat_node(category = Diagnostic)]
fn const_str(
    #[poly_default("")] value: crate::derive_support::Const<&str>,
    #[poly_const(const_str_arc, from = value)] cached: &std::sync::Arc<str>,
) -> std::sync::Arc<str> {
    cached.clone()
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
/// fresh fiber's `PolydatState` would re-fire the eval at first
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

impl PolydatNode for ConstHandle {
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
    value: Box<dyn crate::ast::ReflectedValue>,
}

impl ConstExt {
    pub fn new(value: Box<dyn crate::ast::ReflectedValue>) -> Self {
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

impl PolydatNode for ConstExt {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Ext(self.value.clone());
    }
}
