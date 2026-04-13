// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Trigonometric and mathematical function nodes.
//!
//! Standard math operations on f64 values. Use after `unit_interval`
//! or `scale_range` to transform normalized values into waveforms,
//! angles, or other mathematical shapes.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Slot, Value};

macro_rules! unary_f64_node {
    ($struct_name:ident, $func_name:expr, $op:expr) => {
        pub struct $struct_name {
            meta: NodeMeta,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        ins: vec![Slot::Wire(Port::f64("input"))],
                        outs: vec![Port::f64("output")],
                    },
                }
            }
        }

        impl GkNode for $struct_name {
            fn meta(&self) -> &NodeMeta { &self.meta }

            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                let v = inputs[0].as_f64();
                let f: fn(f64) -> f64 = $op;
                outputs[0] = Value::F64(f(v));
            }

            fn compiled_u64(&self) -> Option<CompiledU64Op> {
                Some(Box::new(|inputs, outputs| {
                    let v = f64::from_bits(inputs[0]);
                    let f: fn(f64) -> f64 = $op;
                    outputs[0] = f(v).to_bits();
                }))
            }
        }
    };
}

// Unary f64 math nodes — JIT level: P2 (compiled_u64 closure).
unary_f64_node!(Sin, "sin", f64::sin);
unary_f64_node!(Cos, "cos", f64::cos);
unary_f64_node!(Tan, "tan", f64::tan);
unary_f64_node!(Asin, "asin", f64::asin);
unary_f64_node!(Acos, "acos", f64::acos);
unary_f64_node!(Atan, "atan", f64::atan);
unary_f64_node!(Sqrt, "sqrt", f64::sqrt);
unary_f64_node!(Abs, "abs_f64", f64::abs);
unary_f64_node!(Ln, "ln", f64::ln);
unary_f64_node!(Exp, "exp", f64::exp);

// --- Binary f64 arithmetic ---

macro_rules! binary_f64_node {
    ($struct_name:ident, $func_name:expr, $desc:expr, $a_name:expr, $b_name:expr, $op:expr) => {
        pub struct $struct_name {
            meta: NodeMeta,
        }

        impl Default for $struct_name {
            fn default() -> Self { Self::new() }
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        ins: vec![
                            Slot::Wire(Port::f64($a_name)),
                            Slot::Wire(Port::f64($b_name)),
                        ],
                        outs: vec![Port::f64("output")],
                    },
                }
            }
        }

        impl GkNode for $struct_name {
            fn meta(&self) -> &NodeMeta { &self.meta }

            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                let a = inputs[0].as_f64();
                let b = inputs[1].as_f64();
                let f: fn(f64, f64) -> f64 = $op;
                outputs[0] = Value::F64(f(a, b));
            }

            fn compiled_u64(&self) -> Option<CompiledU64Op> {
                Some(Box::new(|inputs, outputs| {
                    let a = f64::from_bits(inputs[0]);
                    let b = f64::from_bits(inputs[1]);
                    let f: fn(f64, f64) -> f64 = $op;
                    outputs[0] = f(a, b).to_bits();
                }))
            }

            fn jit_constants(&self) -> Vec<u64> { vec![] }
        }
    };
}

binary_f64_node!(F64Add, "f64_add", "add two f64 values", "a", "b", |a, b| a + b);
binary_f64_node!(F64Sub, "f64_sub", "subtract two f64 values", "a", "b", |a, b| a - b);
binary_f64_node!(F64Mul, "f64_mul", "multiply two f64 values", "a", "b", |a, b| a * b);
binary_f64_node!(F64Div, "f64_div", "divide two f64 values", "a", "b", |a, b| if b != 0.0 { a / b } else { 0.0 });
binary_f64_node!(F64Mod, "f64_mod", "modulo two f64 values", "a", "b", |a, b| if b != 0.0 { a % b } else { 0.0 });

// --- Binary f64 math functions ---

/// Two-argument arc tangent: atan2(y, x).
///
/// Signature: `atan2(y: f64, x: f64) -> (f64)`
///
/// Returns the angle in radians between the positive x-axis and the
/// point (x, y). Output in (-pi, pi]. Use for converting Cartesian
/// coordinates to polar angle.
///
/// JIT level: P2.
pub struct Atan2 {
    meta: NodeMeta,
}

impl Default for Atan2 {
    fn default() -> Self {
        Self::new()
    }
}

impl Atan2 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "atan2".into(),
                ins: vec![
                    Slot::Wire(Port::f64("y")),
                    Slot::Wire(Port::f64("x")),
                ],
                outs: vec![Port::f64("output")],
            },
        }
    }
}

impl GkNode for Atan2 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let y = inputs[0].as_f64();
        let x = inputs[1].as_f64();
        outputs[0] = Value::F64(y.atan2(x));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let y = f64::from_bits(inputs[0]);
            let x = f64::from_bits(inputs[1]);
            outputs[0] = y.atan2(x).to_bits();
        }))
    }
}

/// Power: base^exponent.
///
/// Signature: `pow(base: f64, exponent: f64) -> (f64)`
///
/// JIT level: P2.
pub struct Pow {
    meta: NodeMeta,
}

impl Default for Pow {
    fn default() -> Self {
        Self::new()
    }
}

impl Pow {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "pow".into(),
                ins: vec![
                    Slot::Wire(Port::f64("base")),
                    Slot::Wire(Port::f64("exponent")),
                ],
                outs: vec![Port::f64("output")],
            },
        }
    }
}

impl GkNode for Pow {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let base = inputs[0].as_f64();
        let exp = inputs[1].as_f64();
        outputs[0] = Value::F64(base.powf(exp));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let base = f64::from_bits(inputs[0]);
            let exp = f64::from_bits(inputs[1]);
            outputs[0] = base.powf(exp).to_bits();
        }))
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for mathematical function nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "sin", category: C::Math,
            outputs: 1, description: "sine (radians)",
            help: "Sine of an f64 value in radians.\nOutput oscillates between -1 and 1.\n\nExample: sin(scale_range(hash(cycle), 0.0, 6.2832))",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "cos", category: C::Math,
            outputs: 1, description: "cosine (radians)",
            help: "Cosine of an f64 value in radians.\nOutput oscillates between -1 and 1.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "tan", category: C::Math,
            outputs: 1, description: "tangent (radians)",
            help: "Tangent of an f64 value in radians.\nUnbounded output — has poles at odd multiples of pi/2.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "asin", category: C::Math,
            outputs: 1, description: "arc sine (inverse sin)",
            help: "Arc sine: input in [-1, 1], output in [-pi/2, pi/2] radians.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "acos", category: C::Math,
            outputs: 1, description: "arc cosine (inverse cos)",
            help: "Arc cosine: input in [-1, 1], output in [0, pi] radians.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "atan", category: C::Math,
            outputs: 1, description: "arc tangent",
            help: "Arc tangent: output in (-pi/2, pi/2) radians.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "atan2", category: C::Math,
            outputs: 1, description: "two-argument arc tangent",
            help: "atan2(y, x): angle in radians from positive x-axis to point (x,y).\nOutput in (-pi, pi]. Use for Cartesian-to-polar conversion.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "y", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "x", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "sqrt", category: C::Math,
            outputs: 1, description: "square root",
            help: "Square root of an f64 value.\nReturns NaN for negative inputs.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "abs_f64", category: C::Math,
            outputs: 1, description: "absolute value (f64)",
            help: "Absolute value of an f64. Always non-negative.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "ln", category: C::Math,
            outputs: 1, description: "natural logarithm",
            help: "Natural logarithm (base e).\nReturns -inf for 0, NaN for negative inputs.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "exp", category: C::Math,
            outputs: 1, description: "exponential (e^x)",
            help: "Exponential function: e raised to the power of input.\nexp(0) = 1, exp(1) ≈ 2.718.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "pow", category: C::Math,
            outputs: 1, description: "power (base^exponent)",
            help: "Raise base to the power of exponent.\npow(2, 10) = 1024. Both inputs are f64 wires.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "base", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "exponent", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "f64_add", category: C::Math,
            outputs: 1, description: "add two f64 values",
            help: "Add two f64 wire inputs: a + b.\nUse for composing waveforms, accumulating values, etc.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "f64_sub", category: C::Math,
            outputs: 1, description: "subtract two f64 values",
            help: "Subtract two f64 wire inputs: a - b.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "f64_mul", category: C::Math,
            outputs: 1, description: "multiply two f64 values",
            help: "Multiply two f64 wire inputs: a * b.\nUse for scaling waveforms by amplitude, combining signals, etc.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "f64_div", category: C::Math,
            outputs: 1, description: "divide two f64 values",
            help: "Divide two f64 wire inputs: a / b.\nReturns 0.0 if b is zero.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "f64_mod", category: C::Math,
            outputs: 1, description: "modulo two f64 values",
            help: "Modulo of two f64 wire inputs: a % b.\nReturns 0.0 if b is zero.\nUsed by the `%` infix operator.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a math (trig/elementary) node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "sin" => Some(Ok(Box::new(Sin::new()))),
        "cos" => Some(Ok(Box::new(Cos::new()))),
        "tan" => Some(Ok(Box::new(Tan::new()))),
        "asin" => Some(Ok(Box::new(Asin::new()))),
        "acos" => Some(Ok(Box::new(Acos::new()))),
        "atan" => Some(Ok(Box::new(Atan::new()))),
        "atan2" => Some(Ok(Box::new(Atan2::new()))),
        "sqrt" => Some(Ok(Box::new(Sqrt::new()))),
        "abs_f64" => Some(Ok(Box::new(Abs::new()))),
        "ln" => Some(Ok(Box::new(Ln::new()))),
        "exp" => Some(Ok(Box::new(Exp::new()))),
        "pow" => Some(Ok(Box::new(Pow::new()))),
        "f64_add" => Some(Ok(Box::new(F64Add::new()))),
        "f64_sub" => Some(Ok(Box::new(F64Sub::new()))),
        "f64_mul" => Some(Ok(Box::new(F64Mul::new()))),
        "f64_div" => Some(Ok(Box::new(F64Div::new()))),
        "f64_mod" => Some(Ok(Box::new(F64Mod::new()))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    #[test]
    fn sin_known_values() {
        let node = Sin::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.0)], &mut out);
        assert!((out[0].as_f64() - 0.0).abs() < 1e-10);
        node.eval(&[Value::F64(PI / 2.0)], &mut out);
        assert!((out[0].as_f64() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn cos_known_values() {
        let node = Cos::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.0)], &mut out);
        assert!((out[0].as_f64() - 1.0).abs() < 1e-10);
        node.eval(&[Value::F64(PI)], &mut out);
        assert!((out[0].as_f64() + 1.0).abs() < 1e-10);
    }

    #[test]
    fn sqrt_known() {
        let node = Sqrt::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(4.0)], &mut out);
        assert!((out[0].as_f64() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn atan2_quadrants() {
        let node = Atan2::new();
        let mut out = [Value::None];
        // atan2(1, 0) = pi/2
        node.eval(&[Value::F64(1.0), Value::F64(0.0)], &mut out);
        assert!((out[0].as_f64() - PI / 2.0).abs() < 1e-10);
    }

    #[test]
    fn pow_known() {
        let node = Pow::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(2.0), Value::F64(10.0)], &mut out);
        assert!((out[0].as_f64() - 1024.0).abs() < 1e-10);
    }

    #[test]
    fn ln_exp_roundtrip() {
        let node_ln = Ln::new();
        let node_exp = Exp::new();
        let mut out = [Value::None];
        node_exp.eval(&[Value::F64(3.0)], &mut out);
        let e3 = out[0].as_f64();
        node_ln.eval(&[Value::F64(e3)], &mut out);
        assert!((out[0].as_f64() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn compiled_matches_eval() {
        let node = Sin::new();
        let compiled = node.compiled_u64().unwrap();
        let input = PI / 4.0;
        let mut eval_out = [Value::None];
        node.eval(&[Value::F64(input)], &mut eval_out);
        let mut comp_out = [0u64];
        compiled(&[input.to_bits()], &mut comp_out);
        assert_eq!(eval_out[0].as_f64(), f64::from_bits(comp_out[0]));
    }
}
