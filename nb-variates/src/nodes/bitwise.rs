// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Bitwise and two-wire integer arithmetic nodes.
//!
//! These nodes operate on pairs of u64 wire inputs (no constants).
//! They complement the existing const-param arithmetic nodes (`add`,
//! `mul`, `div`, `mod`) which take one wire and one constant.
//!
//! All nodes support P2 (compiled_u64) and P3 (JIT) execution.

use crate::node::{
    CompiledU64Op, GkNode, NodeMeta, Port, Slot, Value,
};

// ---------------------------------------------------------------------------
// Binary u64 node macro
// ---------------------------------------------------------------------------

/// Define a two-wire u64 binary node with eval, compiled_u64, and jit_constants.
macro_rules! binary_u64_node {
    ($struct_name:ident, $func_name:expr, $doc:expr, $op:expr) => {
        #[doc = $doc]
        pub struct $struct_name {
            meta: NodeMeta,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        ins: vec![Slot::Wire(Port::u64("a")), Slot::Wire(Port::u64("b"))],
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

binary_u64_node!(U64Add2, "u64_add",
    "Add two u64 wire inputs (wrapping).\n\nSignature: `u64_add(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`iadd`).",
    |a, b| a.wrapping_add(b));

binary_u64_node!(U64Sub2, "u64_sub",
    "Subtract two u64 wire inputs (wrapping).\n\nSignature: `u64_sub(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`isub`).",
    |a, b| a.wrapping_sub(b));

binary_u64_node!(U64Mul2, "u64_mul",
    "Multiply two u64 wire inputs (wrapping).\n\nSignature: `u64_mul(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`imul`).",
    |a, b| a.wrapping_mul(b));

binary_u64_node!(U64Div2, "u64_div",
    "Divide two u64 wire inputs (returns 0 if divisor is 0).\n\nSignature: `u64_div(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`udiv`).",
    |a, b| if b != 0 { a / b } else { 0 });

// ---------------------------------------------------------------------------
// Checked arithmetic nodes (overflow-safe, opt-in)
// ---------------------------------------------------------------------------

// Checked addition: returns 0 if a + b would overflow u64.
// Unlike `u64_add` (which wraps), this returns 0 when the result would
// exceed u64::MAX. A result of 0 indicates overflow occurred.
// JIT level: P2 (compiled_u64 closure; hardware carry-flag check can be added later).
binary_u64_node!(CheckedAdd, "checked_add",
    "Add two u64 wires; returns 0 on overflow.\n\nSignature: `checked_add(a: u64, b: u64) -> (u64)`\n\nJIT level: P2.",
    |a, b| a.checked_add(b).unwrap_or(0));

// Checked subtraction: returns 0 if a - b would underflow u64.
// Unlike `u64_sub` (which wraps), this returns 0 when b > a.
// A result of 0 indicates underflow occurred. JIT level: P2.
binary_u64_node!(CheckedSub, "checked_sub",
    "Subtract two u64 wires; returns 0 on underflow.\n\nSignature: `checked_sub(a: u64, b: u64) -> (u64)`\n\nJIT level: P2.",
    |a, b| a.checked_sub(b).unwrap_or(0));

// Checked multiplication: returns 0 if a * b would overflow u64.
// Unlike `u64_mul` (which wraps), this returns 0 when the result would
// exceed u64::MAX. A result of 0 indicates overflow occurred. JIT level: P2.
binary_u64_node!(CheckedMul, "checked_mul",
    "Multiply two u64 wires; returns 0 on overflow.\n\nSignature: `checked_mul(a: u64, b: u64) -> (u64)`\n\nJIT level: P2.",
    |a, b| a.checked_mul(b).unwrap_or(0));

binary_u64_node!(U64Mod2, "u64_mod",
    "Modulo of two u64 wire inputs (returns 0 if divisor is 0).\n\nSignature: `u64_mod(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`urem`).",
    |a, b| if b != 0 { a % b } else { 0 });

binary_u64_node!(U64And, "u64_and",
    "Bitwise AND of two u64 wire inputs.\n\nSignature: `u64_and(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`band`).",
    |a, b| a & b);

binary_u64_node!(U64Or, "u64_or",
    "Bitwise OR of two u64 wire inputs.\n\nSignature: `u64_or(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`bor`).",
    |a, b| a | b);

binary_u64_node!(U64Xor, "u64_xor",
    "Bitwise XOR of two u64 wire inputs.\n\nSignature: `u64_xor(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`bxor`).",
    |a, b| a ^ b);

binary_u64_node!(U64Shl, "u64_shl",
    "Shift left a u64 by a u64 amount (wrapping).\n\nSignature: `u64_shl(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`ishl`).",
    |a, b| a.wrapping_shl(b as u32));

binary_u64_node!(U64Shr, "u64_shr",
    "Logical shift right a u64 by a u64 amount (wrapping).\n\nSignature: `u64_shr(a: u64, b: u64) -> (u64)`\n\nJIT level: P3 (`ushr`).",
    |a, b| a.wrapping_shr(b as u32));

// ---------------------------------------------------------------------------
// Unary: u64_not
// ---------------------------------------------------------------------------

/// Bitwise NOT of a u64 wire input.
///
/// Signature: `u64_not(input: u64) -> (u64)`
///
/// JIT level: P3 (`bnot`).
pub struct U64Not {
    meta: NodeMeta,
}

impl U64Not {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "u64_not".into(),
                ins: vec![Slot::Wire(Port::u64("input"))],
                outs: vec![Port::u64("output")],
            },
        }
    }
}

impl GkNode for U64Not {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(!inputs[0].as_u64());
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = !inputs[0];
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![] }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for bitwise, two-wire integer arithmetic, and checked arithmetic nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "checked_add", category: C::Arithmetic, outputs: 1,
            description: "add two u64 wires; returns 0 on overflow",
            help: "Checked addition of two u64 wire inputs. Returns 0 if the result would overflow u64::MAX.\nUse when overflow should be a detectable sentinel rather than a wrapped value.\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: checked_add(counter, increment)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "checked_sub", category: C::Arithmetic, outputs: 1,
            description: "subtract two u64 wires; returns 0 on underflow",
            help: "Checked subtraction of two u64 wire inputs. Returns 0 if b > a (underflow).\nParameters:\n  a — minuend (u64 wire input)\n  b — subtrahend (u64 wire input)\nExample: checked_sub(total, delta)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "checked_mul", category: C::Arithmetic, outputs: 1,
            description: "multiply two u64 wires; returns 0 on overflow",
            help: "Checked multiplication of two u64 wire inputs. Returns 0 if the result would overflow u64::MAX.\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: checked_mul(count, size)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "u64_add", category: C::Arithmetic, outputs: 1,
            description: "add two u64 wires (wrapping)",
            help: "Wrapping addition of two u64 wire inputs.\nDesugared from the `+` infix operator when operating on integers,\nor callable directly as u64_add(a, b).\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: u64_add(cycle, hash(cycle))",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "u64_sub", category: C::Arithmetic, outputs: 1,
            description: "subtract two u64 wires (wrapping)",
            help: "Wrapping subtraction of two u64 wire inputs.\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: u64_sub(a, b)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "u64_mul", category: C::Arithmetic, outputs: 1,
            description: "multiply two u64 wires (wrapping)",
            help: "Wrapping multiplication of two u64 wire inputs.\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: u64_mul(a, b)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "u64_div", category: C::Arithmetic, outputs: 1,
            description: "divide two u64 wires (0 if divisor is 0)",
            help: "Integer division of two u64 wire inputs. Returns 0 if divisor is 0.\nParameters:\n  a — dividend (u64 wire input)\n  b — divisor (u64 wire input)\nExample: u64_div(a, b)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "u64_mod", category: C::Arithmetic, outputs: 1,
            description: "modulo of two u64 wires (0 if divisor is 0)",
            help: "Integer modulo of two u64 wire inputs. Returns 0 if divisor is 0.\nDesugared from the `%` infix operator when both operands are u64,\nor callable directly as u64_mod(a, b).\nParameters:\n  a — dividend (u64 wire input)\n  b — divisor (u64 wire input)\nExample: u64_mod(cycle, 100)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "u64_and", category: C::Arithmetic, outputs: 1,
            description: "bitwise AND of two u64 wires",
            help: "Bitwise AND of two u64 wire inputs.\nDesugared from the `&` infix operator.\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: cycle & 0xFF",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "u64_or", category: C::Arithmetic, outputs: 1,
            description: "bitwise OR of two u64 wires",
            help: "Bitwise OR of two u64 wire inputs.\nDesugared from the `|` infix operator.\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: a | b",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "u64_xor", category: C::Arithmetic, outputs: 1,
            description: "bitwise XOR of two u64 wires",
            help: "Bitwise XOR of two u64 wire inputs.\nDesugared from the `^` infix operator.\nParameters:\n  a — first u64 wire input\n  b — second u64 wire input\nExample: a ^ b",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "u64_shl", category: C::Arithmetic, outputs: 1,
            description: "shift left u64 by u64 amount",
            help: "Shift a u64 wire input left by a u64 amount (wrapping).\nDesugared from the `<<` infix operator.\nParameters:\n  a — value to shift (u64 wire input)\n  b — shift amount (u64 wire input)\nExample: cycle << 8",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "u64_shr", category: C::Arithmetic, outputs: 1,
            description: "logical shift right u64 by u64 amount",
            help: "Logical shift a u64 wire input right by a u64 amount (wrapping).\nDesugared from the `>>` infix operator.\nParameters:\n  a — value to shift (u64 wire input)\n  b — shift amount (u64 wire input)\nExample: hash(cycle) >> 56",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "u64_not", category: C::Arithmetic, outputs: 1,
            description: "bitwise NOT of a u64 wire",
            help: "Bitwise NOT (complement) of a u64 wire input.\nDesugared from the `!` unary operator.\nParameters:\n  input — u64 wire input\nExample: !cycle",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a bitwise/two-wire arithmetic node from a function name.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "checked_add" => Some(Ok(Box::new(CheckedAdd::new()))),
        "checked_sub" => Some(Ok(Box::new(CheckedSub::new()))),
        "checked_mul" => Some(Ok(Box::new(CheckedMul::new()))),
        "u64_add" => Some(Ok(Box::new(U64Add2::new()))),
        "u64_sub" => Some(Ok(Box::new(U64Sub2::new()))),
        "u64_mul" => Some(Ok(Box::new(U64Mul2::new()))),
        "u64_div" => Some(Ok(Box::new(U64Div2::new()))),
        "u64_mod" => Some(Ok(Box::new(U64Mod2::new()))),
        "u64_and" => Some(Ok(Box::new(U64And::new()))),
        "u64_or"  => Some(Ok(Box::new(U64Or::new()))),
        "u64_xor" => Some(Ok(Box::new(U64Xor::new()))),
        "u64_shl" => Some(Ok(Box::new(U64Shl::new()))),
        "u64_shr" => Some(Ok(Box::new(U64Shr::new()))),
        "u64_not" => Some(Ok(Box::new(U64Not::new()))),
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_add_normal() {
        let node = CheckedAdd::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(100), Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 142);
    }

    #[test]
    fn checked_add_overflow_returns_zero() {
        let node = CheckedAdd::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(u64::MAX), Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[test]
    fn checked_sub_normal() {
        let node = CheckedSub::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42), Value::U64(10)], &mut out);
        assert_eq!(out[0].as_u64(), 32);
    }

    #[test]
    fn checked_sub_underflow_returns_zero() {
        let node = CheckedSub::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0), Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[test]
    fn checked_mul_normal() {
        let node = CheckedMul::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(7), Value::U64(6)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn checked_mul_overflow_returns_zero() {
        let node = CheckedMul::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(u64::MAX), Value::U64(2)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[test]
    fn u64_and_basic() {
        let node = U64And::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0xFF00), Value::U64(0x0FF0)], &mut out);
        assert_eq!(out[0].as_u64(), 0x0F00);
    }

    #[test]
    fn u64_or_basic() {
        let node = U64Or::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0xF0), Value::U64(0x0F)], &mut out);
        assert_eq!(out[0].as_u64(), 0xFF);
    }

    #[test]
    fn u64_xor_basic() {
        let node = U64Xor::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0xFF), Value::U64(0x0F)], &mut out);
        assert_eq!(out[0].as_u64(), 0xF0);
    }

    #[test]
    fn u64_shl_basic() {
        let node = U64Shl::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(1), Value::U64(8)], &mut out);
        assert_eq!(out[0].as_u64(), 256);
    }

    #[test]
    fn u64_shr_basic() {
        let node = U64Shr::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(256), Value::U64(4)], &mut out);
        assert_eq!(out[0].as_u64(), 16);
    }

    #[test]
    fn u64_not_basic() {
        let node = U64Not::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_u64(), u64::MAX);
    }

    #[test]
    fn u64_add2_wrapping() {
        let node = U64Add2::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(u64::MAX), Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[test]
    fn u64_div2_zero_divisor() {
        let node = U64Div2::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42), Value::U64(0)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[test]
    fn u64_sub2_wrapping() {
        let node = U64Sub2::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0), Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), u64::MAX);
    }

    #[test]
    fn u64_mul2_basic() {
        let node = U64Mul2::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(7), Value::U64(6)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn u64_mod2_basic() {
        let node = U64Mod2::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42), Value::U64(10)], &mut out);
        assert_eq!(out[0].as_u64(), 2);
    }

    #[test]
    fn u64_mod2_zero_divisor() {
        let node = U64Mod2::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42), Value::U64(0)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }
}
