// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Diagnostic and debugging nodes.
//!
//! These are development aids, not hot-path nodes. They let users
//! inspect types and values flowing through the DAG.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

/// Emit the type name of the input value as a string.
///
/// Signature: `(input: any) -> (String)`
///
/// Returns "u64", "f64", "bool", "String", or "bytes".
pub struct TypeOf {
    meta: NodeMeta,
    input_type: PortType,
}

impl TypeOf {
    pub fn for_u64() -> Self { Self::new(PortType::U64) }
    pub fn for_f64() -> Self { Self::new(PortType::F64) }
    pub fn for_str() -> Self { Self::new(PortType::Str) }
    pub fn for_bool() -> Self { Self::new(PortType::Bool) }

    pub fn new(input_type: PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: "type_of".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", input_type))],
            },
            input_type,
        }
    }
}

impl GkNode for TypeOf {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.input_type.to_string());
    }
}

/// Emit the Rust Debug representation of the input value as a string.
///
/// Signature: `(input: any) -> (String)`
pub struct DebugRepr {
    meta: NodeMeta,
    _input_type: PortType,
}

impl DebugRepr {
    pub fn for_u64() -> Self { Self::new(PortType::U64) }
    pub fn for_f64() -> Self { Self::new(PortType::F64) }
    pub fn for_str() -> Self { Self::new(PortType::Str) }

    pub fn new(input_type: PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: "debug_repr".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", input_type))],
            },
            _input_type: input_type,
        }
    }
}

impl GkNode for DebugRepr {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(format!("{:?}", inputs[0]));
    }
}

/// Passthrough that prints the value to stderr (for development).
///
/// Signature: `(input: u64) -> (u64)` (or any matching type)
///
/// The value passes through unchanged. A side-effect log line is
/// emitted to stderr with the node name, cycle value, and type.
pub struct Inspect {
    meta: NodeMeta,
    label: String,
}

impl Inspect {
    pub fn u64(label: impl Into<String>) -> Self {
        Self::new(label, PortType::U64)
    }

    pub fn f64(label: impl Into<String>) -> Self {
        Self::new(label, PortType::F64)
    }

    pub fn str(label: impl Into<String>) -> Self {
        Self::new(label, PortType::Str)
    }

    pub fn new(label: impl Into<String>, typ: PortType) -> Self {
        let label = label.into();
        Self {
            meta: NodeMeta {
                name: format!("inspect[{label}]"),
                outs: vec![Port::new("output", typ)],
                ins: vec![Slot::Wire(Port::new("input", typ))],
            },
            label,
        }
    }
}

impl GkNode for Inspect {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        eprintln!("[inspect:{}] {:?}", self.label, inputs[0]);
        outputs[0] = inputs[0].clone();
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for diagnostic and introspection nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "type_of", category: C::Diagnostic, outputs: 1,
            description: "emit type name as string",
            help: "Returns the runtime type name of the input value as a String.\nOutputs: \"U64\", \"F64\", \"Str\", \"Bool\", \"Bytes\", \"Json\", etc.\nUseful for debugging type mismatches in complex graphs.\nParameters:\n  input — any wire value",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "debug_repr", category: C::Diagnostic, outputs: 1,
            description: "emit Debug representation as string",
            help: "Returns the Rust Debug representation of the input value as a String.\nShows internal structure: U64(42), Str(\"hello\"), Bytes([0x01, ...]).\nMore detailed than type_of — use for inspecting actual values.\nParameters:\n  input — any wire value",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "inspect", category: C::Diagnostic, outputs: 1,
            description: "passthrough with stderr logging",
            help: "Passes the input value through unchanged while logging it to stderr.\nThe value is printed with its type and Debug repr on every evaluation.\nUse for live debugging during graph development — remove before production.\nParameters:\n  input — any wire value (passed through unmodified)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a diagnostic node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "type_of" => Some(Ok(Box::new(TypeOf::for_u64()))),
        "inspect" => Some(Ok(Box::new(Inspect::u64("inspect")))),
        "debug_repr" => Some(Ok(Box::new(DebugRepr::new(crate::node::PortType::U64)))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_of_u64() {
        let node = TypeOf::for_u64();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "u64");
    }

    #[test]
    fn type_of_f64() {
        let node = TypeOf::for_f64();
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.14)], &mut out);
        assert_eq!(out[0].as_str(), "f64");
    }

    #[test]
    fn type_of_str() {
        let node = TypeOf::for_str();
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert_eq!(out[0].as_str(), "String");
    }

    #[test]
    fn debug_repr_u64() {
        let node = DebugRepr::for_u64();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "U64(42)");
    }

    #[test]
    fn debug_repr_str() {
        let node = DebugRepr::for_str();
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert!(out[0].as_str().contains("hello"));
    }

    #[test]
    fn inspect_passthrough() {
        let node = Inspect::u64("test");
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }
}
