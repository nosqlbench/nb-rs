// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comparison and selection nodes.
//!
//! Two families:
//!
//! - **Comparison** (`u64_eq`, `u64_lt`, `f64_lt`, …): two-input
//!   nodes that produce a u64 truth value (0 or 1). The DSL's
//!   `==`, `!=`, `<`, `>`, `<=`, `>=` infix operators desugar to
//!   these — type-aware dispatch in `compile_binding` picks the
//!   `u64_*` or `f64_*` variant based on operand types.
//!
//! - **Selection** (`select_u64`, `select_f64`): three-input nodes
//!   that pick between two operand values based on a u64 condition
//!   (any nonzero → first arg, zero → second). Used to desugar
//!   `if(cond, a, b)` once the compiler knows the result type.
//!   Both branches always evaluate — no short-circuit. JIT level:
//!   P2 (compiled closure; could become a P3 conditional select
//!   in a future pass).
//!
//! Output of every comparison node is u64 so downstream code can
//! mix them with bitwise operators (`a < b & c < d`) without
//! widening, and pass them as the `cond` input to `select_*`.

use crate::node::{
    CompiledU64Op, GkNode, NodeMeta, Port, Slot, Value,
};

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

/// Two-wire u64 comparison: takes two u64 inputs, returns u64
/// truth value (0 or 1).
macro_rules! cmp_u64_node {
    ($struct_name:ident, $func_name:expr, $doc:expr, $op:expr) => {
        #[doc = $doc]
        pub struct $struct_name {
            meta: NodeMeta,
        }
        impl Default for $struct_name { fn default() -> Self { Self::new() } }
        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        ins: vec![
                            Slot::Wire(Port::u64("a")),
                            Slot::Wire(Port::u64("b")),
                        ],
                        outs: vec![Port::u64("output")],
                    },
                }
            }
        }
        impl GkNode for $struct_name {
            fn meta(&self) -> &NodeMeta { &self.meta }
            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                let a = inputs[0].as_u64();
                let b = inputs[1].as_u64();
                let f: fn(u64, u64) -> u64 = $op;
                outputs[0] = Value::U64(f(a, b));
            }
            fn compiled_u64(&self) -> Option<CompiledU64Op> {
                Some(Box::new(|inputs, outputs| {
                    let f: fn(u64, u64) -> u64 = $op;
                    outputs[0] = f(inputs[0], inputs[1]);
                }))
            }
            fn jit_constants(&self) -> Vec<u64> { vec![] }
        }
    };
}

/// Two-wire f64 comparison: takes two f64 inputs, returns u64
/// truth value (0 or 1). Outputs are u64-typed so downstream code
/// can pass them to `select_*` or bitwise ops without widening.
macro_rules! cmp_f64_node {
    ($struct_name:ident, $func_name:expr, $doc:expr, $op:expr) => {
        #[doc = $doc]
        pub struct $struct_name {
            meta: NodeMeta,
        }
        impl Default for $struct_name { fn default() -> Self { Self::new() } }
        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        ins: vec![
                            Slot::Wire(Port::f64("a")),
                            Slot::Wire(Port::f64("b")),
                        ],
                        outs: vec![Port::u64("output")],
                    },
                }
            }
        }
        impl GkNode for $struct_name {
            fn meta(&self) -> &NodeMeta { &self.meta }
            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                let a = inputs[0].as_f64();
                let b = inputs[1].as_f64();
                let f: fn(f64, f64) -> u64 = $op;
                outputs[0] = Value::U64(f(a, b));
            }
            fn compiled_u64(&self) -> Option<CompiledU64Op> {
                Some(Box::new(|inputs, outputs| {
                    let a = f64::from_bits(inputs[0]);
                    let b = f64::from_bits(inputs[1]);
                    let f: fn(f64, f64) -> u64 = $op;
                    outputs[0] = f(a, b);
                }))
            }
            fn jit_constants(&self) -> Vec<u64> { vec![] }
        }
    };
}

// ---------------------------------------------------------------------------
// Comparison nodes
// ---------------------------------------------------------------------------

cmp_u64_node!(U64Eq, "u64_eq",
    "Equality of two u64 wires; returns 1 if equal else 0.\n\nSignature: `u64_eq(a: u64, b: u64) -> (u64)`",
    |a, b| if a == b { 1 } else { 0 });

cmp_u64_node!(U64Ne, "u64_ne",
    "Inequality of two u64 wires.\n\nSignature: `u64_ne(a: u64, b: u64) -> (u64)`",
    |a, b| if a != b { 1 } else { 0 });

cmp_u64_node!(U64Lt, "u64_lt",
    "Less-than comparison of two u64 wires.\n\nSignature: `u64_lt(a: u64, b: u64) -> (u64)`",
    |a, b| if a <  b { 1 } else { 0 });

cmp_u64_node!(U64Gt, "u64_gt",
    "Greater-than comparison of two u64 wires.\n\nSignature: `u64_gt(a: u64, b: u64) -> (u64)`",
    |a, b| if a >  b { 1 } else { 0 });

cmp_u64_node!(U64Le, "u64_le",
    "Less-than-or-equal comparison of two u64 wires.\n\nSignature: `u64_le(a: u64, b: u64) -> (u64)`",
    |a, b| if a <= b { 1 } else { 0 });

cmp_u64_node!(U64Ge, "u64_ge",
    "Greater-than-or-equal comparison of two u64 wires.\n\nSignature: `u64_ge(a: u64, b: u64) -> (u64)`",
    |a, b| if a >= b { 1 } else { 0 });

// f64 comparisons follow IEEE 754 — NaN compares unequal to
// itself and is neither <, >, <=, nor >=. Tests for NaN should
// use `a != a`.
cmp_f64_node!(F64Eq, "f64_eq",
    "Equality of two f64 wires; returns 1 if equal else 0. NaN-aware via IEEE.\n\nSignature: `f64_eq(a: f64, b: f64) -> (u64)`",
    |a, b| if a == b { 1 } else { 0 });

cmp_f64_node!(F64Ne, "f64_ne",
    "Inequality of two f64 wires.\n\nSignature: `f64_ne(a: f64, b: f64) -> (u64)`",
    |a, b| if a != b { 1 } else { 0 });

cmp_f64_node!(F64Lt, "f64_lt",
    "Less-than comparison of two f64 wires.\n\nSignature: `f64_lt(a: f64, b: f64) -> (u64)`",
    |a, b| if a <  b { 1 } else { 0 });

cmp_f64_node!(F64Gt, "f64_gt",
    "Greater-than comparison of two f64 wires.\n\nSignature: `f64_gt(a: f64, b: f64) -> (u64)`",
    |a, b| if a >  b { 1 } else { 0 });

cmp_f64_node!(F64Le, "f64_le",
    "Less-than-or-equal comparison of two f64 wires.\n\nSignature: `f64_le(a: f64, b: f64) -> (u64)`",
    |a, b| if a <= b { 1 } else { 0 });

cmp_f64_node!(F64Ge, "f64_ge",
    "Greater-than-or-equal comparison of two f64 wires.\n\nSignature: `f64_ge(a: f64, b: f64) -> (u64)`",
    |a, b| if a >= b { 1 } else { 0 });

// ---------------------------------------------------------------------------
// Selection nodes (the desugar target for `if(cond, a, b)`)
// ---------------------------------------------------------------------------

/// Pick between two u64 inputs based on a u64 condition.
/// Any nonzero `cond` → `a`; zero → `b`.
pub struct SelectU64 { meta: NodeMeta }
impl Default for SelectU64 { fn default() -> Self { Self::new() } }
impl SelectU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "select_u64".into(),
                ins: vec![
                    Slot::Wire(Port::u64("cond")),
                    Slot::Wire(Port::u64("a")),
                    Slot::Wire(Port::u64("b")),
                ],
                outs: vec![Port::u64("output")],
            },
        }
    }
}
impl GkNode for SelectU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let cond = inputs[0].as_u64();
        let a = inputs[1].as_u64();
        let b = inputs[2].as_u64();
        outputs[0] = Value::U64(if cond != 0 { a } else { b });
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = if inputs[0] != 0 { inputs[1] } else { inputs[2] };
        }))
    }
    fn jit_constants(&self) -> Vec<u64> { vec![] }
}

// ---------------------------------------------------------------------------
// String comparisons
// ---------------------------------------------------------------------------
//
// Strings live on the heap; the compiled-u64 fast path can't carry
// them in raw u64 buffers, so these are eval-only. The DSL desugar
// in `binding.rs` picks `str_eq` / `str_ne` over the u64 / f64
// variants when either operand has `PortType::Str`.

/// Equality of two String wires. Returns 1 if equal else 0.
///
/// Signature: `str_eq(a: String, b: String) -> (u64)`
pub struct StrEq { meta: NodeMeta }
impl Default for StrEq { fn default() -> Self { Self::new() } }
impl StrEq {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "str_eq".into(),
                ins: vec![
                    Slot::Wire(Port::new("a", crate::node::PortType::Str)),
                    Slot::Wire(Port::new("b", crate::node::PortType::Str)),
                ],
                outs: vec![Port::u64("output")],
            },
        }
    }
}
impl GkNode for StrEq {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let eq = match (&inputs[0], &inputs[1]) {
            (Value::Str(a), Value::Str(b)) => a == b,
            _ => false,
        };
        outputs[0] = Value::U64(if eq { 1 } else { 0 });
    }
}

/// Inequality of two String wires.
///
/// Signature: `str_ne(a: String, b: String) -> (u64)`
pub struct StrNe { meta: NodeMeta }
impl Default for StrNe { fn default() -> Self { Self::new() } }
impl StrNe {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "str_ne".into(),
                ins: vec![
                    Slot::Wire(Port::new("a", crate::node::PortType::Str)),
                    Slot::Wire(Port::new("b", crate::node::PortType::Str)),
                ],
                outs: vec![Port::u64("output")],
            },
        }
    }
}
impl GkNode for StrNe {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let eq = match (&inputs[0], &inputs[1]) {
            (Value::Str(a), Value::Str(b)) => a == b,
            _ => false,
        };
        outputs[0] = Value::U64(if !eq { 1 } else { 0 });
    }
}

/// Pick between two f64 inputs based on a u64 condition.
/// Any nonzero `cond` → `a`; zero → `b`.
pub struct SelectF64 { meta: NodeMeta }
impl Default for SelectF64 { fn default() -> Self { Self::new() } }
impl SelectF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "select_f64".into(),
                ins: vec![
                    Slot::Wire(Port::u64("cond")),
                    Slot::Wire(Port::f64("a")),
                    Slot::Wire(Port::f64("b")),
                ],
                outs: vec![Port::f64("output")],
            },
        }
    }
}
impl GkNode for SelectF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let cond = inputs[0].as_u64();
        let a = inputs[1].as_f64();
        let b = inputs[2].as_f64();
        outputs[0] = Value::F64(if cond != 0 { a } else { b });
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            // f64s travel through u64 buffers as raw bit patterns;
            // pick the chosen pattern unchanged.
            outputs[0] = if inputs[0] != 0 { inputs[1] } else { inputs[2] };
        }))
    }
    fn jit_constants(&self) -> Vec<u64> { vec![] }
}

/// Pick between two String inputs based on a u64 condition.
/// Any nonzero `cond` → `a`; zero → `b`.
pub struct SelectStr { meta: NodeMeta }
impl Default for SelectStr { fn default() -> Self { Self::new() } }
impl SelectStr {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "select_str".into(),
                ins: vec![
                    Slot::Wire(Port::u64("cond")),
                    Slot::Wire(Port::new("a", crate::node::PortType::Str)),
                    Slot::Wire(Port::new("b", crate::node::PortType::Str)),
                ],
                outs: vec![Port::new("output", crate::node::PortType::Str)],
            },
        }
    }
}
impl GkNode for SelectStr {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let cond = inputs[0].as_u64();
        let pick = if cond != 0 { &inputs[1] } else { &inputs[2] };
        outputs[0] = match pick {
            Value::Str(s) => Value::Str(s.clone()),
            other => Value::Str(other.to_display_string()),
        };
    }
}

// ---------------------------------------------------------------------------
// Registry wiring
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

static SELECT_U64_PARAMS: &[ParamSpec] = &[
    ParamSpec { name: "cond", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
    ParamSpec { name: "a",    slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
    ParamSpec { name: "b",    slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
];
static SELECT_F64_PARAMS: &[ParamSpec] = &[
    ParamSpec { name: "cond", slot_type: SlotType::Wire, required: true, example: "cycle",     constraint: None },
    ParamSpec { name: "a",    slot_type: SlotType::Wire, required: true, example: "to_f64(0)", constraint: None },
    ParamSpec { name: "b",    slot_type: SlotType::Wire, required: true, example: "to_f64(0)", constraint: None },
];
static SELECT_STR_PARAMS: &[ParamSpec] = &[
    ParamSpec { name: "cond", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
    ParamSpec { name: "a",    slot_type: SlotType::Wire, required: true, example: "\"yes\"", constraint: None },
    ParamSpec { name: "b",    slot_type: SlotType::Wire, required: true, example: "\"no\"",  constraint: None },
];
static STR_CMP_PARAMS: &[ParamSpec] = &[
    ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "\"foo\"", constraint: None },
    ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "\"bar\"", constraint: None },
];

static SIGS: &[FuncSig] = &[
    // u64 comparisons
    cmp_sig("u64_eq", "equality of two u64 wires"),
    cmp_sig("u64_ne", "inequality of two u64 wires"),
    cmp_sig("u64_lt", "less-than comparison of two u64 wires"),
    cmp_sig("u64_gt", "greater-than comparison of two u64 wires"),
    cmp_sig("u64_le", "less-or-equal comparison of two u64 wires"),
    cmp_sig("u64_ge", "greater-or-equal comparison of two u64 wires"),
    // f64 comparisons
    cmp_f64_sig("f64_eq", "equality of two f64 wires (IEEE 754)"),
    cmp_f64_sig("f64_ne", "inequality of two f64 wires (IEEE 754)"),
    cmp_f64_sig("f64_lt", "less-than comparison of two f64 wires"),
    cmp_f64_sig("f64_gt", "greater-than comparison of two f64 wires"),
    cmp_f64_sig("f64_le", "less-or-equal comparison of two f64 wires"),
    cmp_f64_sig("f64_ge", "greater-or-equal comparison of two f64 wires"),
    // string comparisons
    FuncSig {
        name: "str_eq", category: FuncCategory::Comparison, outputs: 1,
        description: "equality of two String wires",
        help: "Returns 1 if the two String inputs are equal, 0 otherwise.\nUsed when the DSL `==` operator is desugared and either operand is String.",
        identity: None, variadic_ctor: None,
        params: STR_CMP_PARAMS,
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
    },
    FuncSig {
        name: "str_ne", category: FuncCategory::Comparison, outputs: 1,
        description: "inequality of two String wires",
        help: "Returns 1 if the two String inputs differ, 0 otherwise.\nUsed when the DSL `!=` operator is desugared and either operand is String.",
        identity: None, variadic_ctor: None,
        params: STR_CMP_PARAMS,
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
    },
    // selection
    FuncSig {
        name: "select_u64", category: FuncCategory::Comparison, outputs: 1,
        description: "pick u64 a or b based on cond",
        help: "Pick between two u64 inputs based on a u64 condition (any nonzero -> a; zero -> b).\nBoth branches always evaluate (no short-circuit).\nThe DSL `if(cond, a, b)` desugars to this when both branches are u64.\nParameters:\n  cond — u64 wire (0 or nonzero)\n  a    — u64 wire (returned when cond != 0)\n  b    — u64 wire (returned when cond == 0)\nExample: select_u64(u64_lt(x, 10), x, 10)  // clamp x at 10",
        identity: None, variadic_ctor: None,
        params: SELECT_U64_PARAMS,
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
    },
    FuncSig {
        name: "select_f64", category: FuncCategory::Comparison, outputs: 1,
        description: "pick f64 a or b based on cond",
        help: "Pick between two f64 inputs based on a u64 condition.\nThe DSL `if(cond, a, b)` desugars to this when either branch is f64.\nParameters:\n  cond — u64 wire (0 or nonzero)\n  a    — f64 wire (returned when cond != 0)\n  b    — f64 wire (returned when cond == 0)\nExample: select_f64(f64_gt(err_rate, 0.05), 0.5, 1.05)",
        identity: None, variadic_ctor: None,
        params: SELECT_F64_PARAMS,
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
    },
    FuncSig {
        name: "select_str", category: FuncCategory::Comparison, outputs: 1,
        description: "pick String a or b based on cond",
        help: "Pick between two String inputs based on a u64 condition (any nonzero -> a; zero -> b).\nThe DSL `if(cond, a, b)` desugars to this when either branch is String.\nParameters:\n  cond — u64 wire (0 or nonzero)\n  a    — String wire (returned when cond != 0)\n  b    — String wire (returned when cond == 0)",
        identity: None, variadic_ctor: None,
        params: SELECT_STR_PARAMS,
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
    },
];

pub fn signatures() -> &'static [FuncSig] { SIGS }

const fn cmp_sig(name: &'static str, description: &'static str) -> FuncSig {
    FuncSig {
        name, category: FuncCategory::Comparison, outputs: 1,
        description,
        help: "",
        identity: None, variadic_ctor: None,
        params: &[
            ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
        ],
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
    }
}

const fn cmp_f64_sig(name: &'static str, description: &'static str) -> FuncSig {
    FuncSig {
        name, category: FuncCategory::Comparison, outputs: 1,
        description,
        help: "",
        identity: None, variadic_ctor: None,
        params: &[
            ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "to_f64(0)", constraint: None },
            ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "to_f64(0)", constraint: None },
        ],
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
    }
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef], _wire_types: &[crate::node::PortType],
    _consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "u64_eq" => Some(Ok(Box::new(U64Eq::new()))),
        "u64_ne" => Some(Ok(Box::new(U64Ne::new()))),
        "u64_lt" => Some(Ok(Box::new(U64Lt::new()))),
        "u64_gt" => Some(Ok(Box::new(U64Gt::new()))),
        "u64_le" => Some(Ok(Box::new(U64Le::new()))),
        "u64_ge" => Some(Ok(Box::new(U64Ge::new()))),
        "f64_eq" => Some(Ok(Box::new(F64Eq::new()))),
        "f64_ne" => Some(Ok(Box::new(F64Ne::new()))),
        "f64_lt" => Some(Ok(Box::new(F64Lt::new()))),
        "f64_gt" => Some(Ok(Box::new(F64Gt::new()))),
        "f64_le" => Some(Ok(Box::new(F64Le::new()))),
        "f64_ge" => Some(Ok(Box::new(F64Ge::new()))),
        "select_u64" => Some(Ok(Box::new(SelectU64::new()))),
        "select_f64" => Some(Ok(Box::new(SelectF64::new()))),
        "select_str" => Some(Ok(Box::new(SelectStr::new()))),
        "str_eq" => Some(Ok(Box::new(StrEq::new()))),
        "str_ne" => Some(Ok(Box::new(StrNe::new()))),
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Value;

    fn run(node: &dyn GkNode, ins: Vec<Value>) -> Value {
        let mut outs = vec![Value::U64(0)];
        node.eval(&ins, &mut outs);
        outs.into_iter().next().unwrap()
    }

    #[test]
    fn u64_lt_gt_eq_basics() {
        assert_eq!(run(&U64Lt::new(), vec![Value::U64(1), Value::U64(2)]).as_u64(), 1);
        assert_eq!(run(&U64Lt::new(), vec![Value::U64(2), Value::U64(2)]).as_u64(), 0);
        assert_eq!(run(&U64Gt::new(), vec![Value::U64(3), Value::U64(2)]).as_u64(), 1);
        assert_eq!(run(&U64Eq::new(), vec![Value::U64(5), Value::U64(5)]).as_u64(), 1);
        assert_eq!(run(&U64Ne::new(), vec![Value::U64(5), Value::U64(5)]).as_u64(), 0);
        assert_eq!(run(&U64Le::new(), vec![Value::U64(2), Value::U64(2)]).as_u64(), 1);
        assert_eq!(run(&U64Ge::new(), vec![Value::U64(2), Value::U64(2)]).as_u64(), 1);
    }

    #[test]
    fn f64_comparisons_basics() {
        assert_eq!(run(&F64Lt::new(), vec![Value::F64(0.1), Value::F64(0.2)]).as_u64(), 1);
        assert_eq!(run(&F64Gt::new(), vec![Value::F64(0.2), Value::F64(0.1)]).as_u64(), 1);
        assert_eq!(run(&F64Eq::new(), vec![Value::F64(0.1), Value::F64(0.1)]).as_u64(), 1);
        // NaN: f64_eq of NaN with itself is 0 (IEEE 754).
        assert_eq!(run(&F64Eq::new(), vec![Value::F64(f64::NAN), Value::F64(f64::NAN)]).as_u64(), 0);
    }

    #[test]
    fn select_u64_picks_by_cond() {
        let mut outs = vec![Value::U64(0)];
        SelectU64::new().eval(&[Value::U64(1), Value::U64(10), Value::U64(20)], &mut outs);
        assert_eq!(outs[0].as_u64(), 10);
        SelectU64::new().eval(&[Value::U64(0), Value::U64(10), Value::U64(20)], &mut outs);
        assert_eq!(outs[0].as_u64(), 20);
    }

    #[test]
    fn select_f64_picks_by_cond() {
        let mut outs = vec![Value::F64(0.0)];
        SelectF64::new().eval(&[Value::U64(1), Value::F64(0.5), Value::F64(1.05)], &mut outs);
        assert_eq!(outs[0].as_f64(), 0.5);
        SelectF64::new().eval(&[Value::U64(0), Value::F64(0.5), Value::F64(1.05)], &mut outs);
        assert_eq!(outs[0].as_f64(), 1.05);
    }

    #[test]
    fn str_eq_ne_basics() {
        let mut out = vec![Value::U64(0)];
        StrEq::new().eval(&[Value::Str("LATENCY".into()), Value::Str("LATENCY".into())], &mut out);
        assert_eq!(out[0].as_u64(), 1);
        StrEq::new().eval(&[Value::Str("LATENCY".into()), Value::Str("RECALL".into())], &mut out);
        assert_eq!(out[0].as_u64(), 0);
        StrNe::new().eval(&[Value::Str("LATENCY".into()), Value::Str("RECALL".into())], &mut out);
        assert_eq!(out[0].as_u64(), 1);
        StrNe::new().eval(&[Value::Str("a".into()), Value::Str("a".into())], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[test]
    fn select_str_picks_by_cond() {
        let mut out = vec![Value::Str(String::new())];
        SelectStr::new().eval(
            &[Value::U64(1), Value::Str("yes".into()), Value::Str("no".into())],
            &mut out,
        );
        assert_eq!(out[0].as_str(), "yes");
        SelectStr::new().eval(
            &[Value::U64(0), Value::Str("yes".into()), Value::Str("no".into())],
            &mut out,
        );
        assert_eq!(out[0].as_str(), "no");
    }
}
