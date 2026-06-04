// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Trigonometric and mathematical function nodes.
//!
//! Standard math operations on f64 values. Use after `unit_interval`
//! or `scale_range` to transform normalized values into waveforms,
//! angles, or other mathematical shapes.

// SRD-80 PR B.7 — `unary_f64_node!` and `binary_f64_node!`
// declarative macros retired. `#[polydat_node]` auto-emits the
// same Phase 2 closure with f64↔u64 bit-reinterpret from the
// typed signature. Struct names follow snake_case → PascalCase
// (e.g. `f64_add` → `F64Add`, `abs_f64` → `AbsF64`).

#[crate::polydat_node(category = Math)]
fn sin(input: f64) -> f64 { input.sin() }

#[crate::polydat_node(category = Math)]
fn cos(input: f64) -> f64 { input.cos() }

#[crate::polydat_node(category = Math)]
fn tan(input: f64) -> f64 { input.tan() }

#[crate::polydat_node(category = Math)]
fn asin(input: f64) -> f64 { input.asin() }

#[crate::polydat_node(category = Math)]
fn acos(input: f64) -> f64 { input.acos() }

#[crate::polydat_node(category = Math)]
fn atan(input: f64) -> f64 { input.atan() }

#[crate::polydat_node(category = Math)]
fn sqrt(input: f64) -> f64 { input.sqrt() }

#[crate::polydat_node(category = Math)]
fn abs_f64(input: f64) -> f64 { input.abs() }

#[crate::polydat_node(category = Math)]
fn ln(input: f64) -> f64 { input.ln() }

#[crate::polydat_node(category = Math)]
fn exp(input: f64) -> f64 { input.exp() }

#[crate::polydat_node(category = Math)]
fn f64_add(a: f64, b: f64) -> f64 { a + b }

#[crate::polydat_node(category = Math)]
fn f64_sub(a: f64, b: f64) -> f64 { a - b }

#[crate::polydat_node(category = Math)]
fn f64_mul(a: f64, b: f64) -> f64 { a * b }

#[crate::polydat_node(category = Math)]
fn f64_div(a: f64, b: f64) -> f64 { if b != 0.0 { a / b } else { 0.0 } }

#[crate::polydat_node(category = Math)]
fn f64_mod(a: f64, b: f64) -> f64 { if b != 0.0 { a % b } else { 0.0 } }

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
/// Two-argument arc tangent. SRD-80 PR B.7 migration.
#[crate::polydat_node(category = Math)]
fn atan2(y: f64, x: f64) -> f64 { y.atan2(x) }

/// Power: base^exponent. SRD-80 PR B.7 migration.
///
/// Note: the macro-emitted second arg name is `exponent` (from
/// the function signature); workloads that bound that param by
/// position keep working unchanged.
#[crate::polydat_node(category = Math)]
fn pow(base: f64, exponent: f64) -> f64 { base.powf(exponent) }

// ---------------------------------------------------------------------------
// SRD-80 PR B.7 — every node in this module registers
// link-time via the proc-macro-emitted NodeRegistration. The
// hand-maintained signatures()/build_node()/register_nodes!
// plumbing below is retained inside a never-compiled block so
// the migration diff stays readable; remove on next pass.

#[cfg(any())]

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};
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
