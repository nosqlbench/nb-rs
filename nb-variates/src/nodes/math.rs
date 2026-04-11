// Copyright 2024-2026 nosqlbench contributors
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

// --- Binary f64 nodes ---

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
