// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Linear interpolation and range mapping nodes.
//!
//! SRD-80b PR B.15+ migration — every node in this module
//! authors via `#[polydat_node]`. The hand-maintained
//! `signatures()` / `build_node()` / `validate_node()` /
//! `register_nodes!` plumbing is retired; macro-emitted
//! `NodeRegistration` covers link-time discovery.

use crate::compile::fusion::{DecomposedGraph, DecomposedWire, FusedNode};

/// Linear interpolation with fixed endpoints.
///
/// Signature: `lerp(t: f64, a: f64, b: f64) -> (f64)`
/// Result: `a + t * (b - a)` where a, b are init-time params.
///
/// When t=0 the output is a, t=1 gives b, t=0.5 gives the midpoint.
/// Use after `unit_interval` to map a normalized `[0,1)` value into an
/// arbitrary continuous range. Example: `lerp(unit_interval(h), -180.0,
/// 180.0)` produces a random longitude. Accepts t outside `[0,1]` for
/// extrapolation.
///
/// JIT level: P3 (macro-emitted `compiled_u64` + `jit_constants`
/// `[a.to_bits(), b.to_bits()]` matching the JIT codegen
/// `JitOp::LerpConst(a_bits, b_bits)` layout).
#[crate::polydat_node(category = Interpolation)]
fn lerp(
    t: f64,
    #[poly_default(0.0f64)] a: crate::derive_support::Const<f64>,
    #[poly_default(1.0f64)] b: crate::derive_support::Const<f64>,
) -> f64 {
    *a + t * (*b - *a)
}

/// Map a u64 linearly to an f64 range.
///
/// Signature: `scale_range(input: u64, min: f64, max: f64) -> (f64)`
/// Maps [0, u64::MAX] to [min, max].
///
/// Convenience node that fuses `unit_interval` + `lerp` into a single
/// step. Use directly after `hash` when you need a uniform f64 in a
/// custom range without wiring two separate nodes. Example:
/// `scale_range(hash(cycle), 0.0, 1000.0)` gives a uniform float in
/// [0, 1000].
///
/// JIT level: P3. `jit_constants` is overridden to emit the
/// `(min, range)` pair that `JitOp::ScaleRangeConst` expects;
/// the macro-derived default would emit `(min, max)` which the
/// JIT codegen would interpret incorrectly.
#[crate::polydat_node(
    category = Interpolation,
    jit_constants = scale_range_jit_constants,
)]
fn scale_range(
    input: u64,
    #[poly_default(0.0f64)] min: crate::derive_support::Const<f64>,
    #[poly_default(1.0f64)] max: crate::derive_support::Const<f64>,
) -> f64 {
    let t = input as f64 / u64::MAX as f64;
    *min + t * (*max - *min)
}

/// JIT-constants override for `scale_range`: emit the
/// `(min, range)` layout that `JitOp::ScaleRangeConst`
/// (polydat/src/compile/jit/codegen.rs) consumes.
fn scale_range_jit_constants(node: &ScaleRange) -> Vec<u64> {
    vec![node.min.to_bits(), (node.max - node.min).to_bits()]
}

impl FusedNode for ScaleRange {
    /// `scale_range(x, lo, hi)` decomposes to `lerp(unit_interval(x), lo, hi)`.
    fn decomposed(&self) -> DecomposedGraph {
        use crate::library::sampling::icd::UnitInterval;
        let mut g = DecomposedGraph::new(1);
        let ui = g.add_node(Box::new(UnitInterval::new()), vec![DecomposedWire::Input(0)]);
        let lerp = g.add_node(
            Box::new(Lerp::new(self.min, self.max)),
            vec![DecomposedWire::Node(ui, 0)],
        );
        g.set_outputs(vec![DecomposedWire::Node(lerp, 0)]);
        g
    }
}

/// Inverse linear interpolation: map [a, b] to [0, 1].
///
/// Signature: `inv_lerp(input: f64, a: f64, b: f64) -> (f64)`
/// Result: `(input - a) / (b - a)`, clamped to `[0, 1]`.
///
/// The reverse of `lerp`: normalizes an arbitrary continuous range
/// back to `[0,1]`. Use as the first half of a `remap`, or to feed a
/// domain-specific value into a node that expects unit input. Example:
/// `inv_lerp(temperature, 32.0, 212.0)` normalizes Fahrenheit to
/// `[0,1]`. Output is clamped, so out-of-range inputs saturate.
///
/// Inverse linear interpolation. SRD-80 PR B.12 — inline
/// compute; the per-call `1.0 / (b - a)` divide is acceptable
/// versus the cost of a multi-source Setup mechanism that no
/// other node would need.
#[crate::polydat_node(category = Interpolation)]
fn inv_lerp(
    input: f64,
    #[poly_default(0.0f64)] a: crate::derive_support::Const<f64>,
    #[poly_default(1.0f64)] b: crate::derive_support::Const<f64>,
) -> f64 {
    let inv_range = 1.0 / (*b - *a);
    let t = (input - *a) * inv_range;
    t.clamp(0.0, 1.0)
}

/// Remap from one range to another.
///
/// Signature: `remap(input: f64, in_min: f64, in_max: f64, out_min: f64, out_max: f64) -> (f64)`
/// Maps [in_min, in_max] to [out_min, out_max] linearly.
///
/// Combines `inv_lerp` + `lerp` in one node. Use for unit conversions
/// or rescaling distribution outputs. Example:
/// `remap(value, 32.0, 212.0, 0.0, 100.0)` converts Fahrenheit to
/// Celsius. Unlike `inv_lerp`, the output is not clamped, so
/// extrapolation is possible.
///
/// JIT level: P1 (no compiled_u64; f64 in/out without captured closure).
/// SRD-80 PR B.12 — inline compute; one extra divide per call
/// versus the multi-source Setup machinery that would have
/// precomputed `1.0 / (in_max - in_min)`.
#[crate::polydat_node(category = Interpolation)]
fn remap(
    input: f64,
    #[poly_default(0.0f64)] in_min: crate::derive_support::Const<f64>,
    #[poly_default(1.0f64)] in_max: crate::derive_support::Const<f64>,
    #[poly_default(0.0f64)] out_min: crate::derive_support::Const<f64>,
    #[poly_default(1.0f64)] out_max: crate::derive_support::Const<f64>,
) -> f64 {
    let t = (input - *in_min) / (*in_max - *in_min);
    *out_min + t * (*out_max - *out_min)
}

/// Quantize an f64 to the nearest multiple of a step size.
///
/// Signature: `quantize(input: f64, step: f64) -> (f64)`
/// Result: `round(input / step) * step`
///
/// Snaps continuous values to a discrete grid. Use for rounding
/// prices to the nearest cent (`quantize(price, 0.01)`), snapping
/// coordinates to a tile grid (`quantize(x, 16.0)`), or binning
/// timestamps to fixed intervals. Unlike `discretize`, the output
/// remains f64 at the grid point, not a bucket index.
///
/// JIT level: P3 (macro-emitted; consts = `[step.to_bits()]`).
///
/// Greenfield migration note: the prior hand-written
/// `Quantize::new` asserted `step > 0.0`, and the prior
/// `ParamSpec` carried a `PositiveFiniteF64` constraint that
/// Pass 1 enforced. Both are retired with the macro
/// migration (the macro doesn't yet support const-arg
/// `ConstConstraint` metadata). Behavior mirrors the
/// `div`/`mod_const` precedent (arithmetic.rs): a non-
/// positive `step` propagates a NaN/inf through the body,
/// surfacing at cycle time. If early-fail is required
/// again, it lands via a future macro extension that
/// plumbs `#[constraint(PositiveFiniteF64)]` onto const
/// args (current support is wire-only).
#[crate::polydat_node(category = Interpolation)]
fn quantize(
    input: f64,
    #[poly_default(1.0f64)] step: crate::derive_support::Const<f64>,
) -> f64 {
    (input / *step).round() * *step
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn lerp_endpoints() {
        let node = Lerp::new(10.0, 20.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.0)], &mut out);
        assert_eq!(out[0].as_f64(), 10.0);
        node.eval(&[Value::F64(1.0)], &mut out);
        assert_eq!(out[0].as_f64(), 20.0);
    }

    #[test]
    fn lerp_midpoint() {
        let node = Lerp::new(0.0, 100.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.5)], &mut out);
        assert_eq!(out[0].as_f64(), 50.0);
    }

    #[test]
    fn scale_range_bounds() {
        let node = ScaleRange::new(10.0, 20.0);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert!((out[0].as_f64() - 10.0).abs() < 0.001);
        node.eval(&[Value::U64(u64::MAX)], &mut out);
        assert!((out[0].as_f64() - 20.0).abs() < 0.001);
    }

    #[test]
    fn scale_range_jit_constants_layout() {
        // JIT codegen consumes (min, range); the override
        // must emit that pair regardless of struct field order.
        let node = ScaleRange::new(10.0, 25.0);
        let consts = node.jit_constants();
        assert_eq!(consts.len(), 2);
        assert_eq!(f64::from_bits(consts[0]), 10.0);
        assert_eq!(f64::from_bits(consts[1]), 15.0); // range = max - min
    }

    #[test]
    fn inv_lerp_basic() {
        let node = InvLerp::new(10.0, 20.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(10.0)], &mut out);
        assert!((out[0].as_f64() - 0.0).abs() < 0.001);
        node.eval(&[Value::F64(15.0)], &mut out);
        assert!((out[0].as_f64() - 0.5).abs() < 0.001);
        node.eval(&[Value::F64(20.0)], &mut out);
        assert!((out[0].as_f64() - 1.0).abs() < 0.001);
    }

    #[test]
    fn inv_lerp_clamps() {
        let node = InvLerp::new(0.0, 100.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(-50.0)], &mut out);
        assert_eq!(out[0].as_f64(), 0.0);
        node.eval(&[Value::F64(200.0)], &mut out);
        assert_eq!(out[0].as_f64(), 1.0);
    }

    #[test]
    fn remap_basic() {
        let node = Remap::new(0.0, 100.0, 0.0, 1.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(50.0)], &mut out);
        assert!((out[0].as_f64() - 0.5).abs() < 0.001);
    }

    #[test]
    fn remap_different_ranges() {
        // Fahrenheit to Celsius: [32, 212] → [0, 100]
        let node = Remap::new(32.0, 212.0, 0.0, 100.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(32.0)], &mut out);
        assert!((out[0].as_f64() - 0.0).abs() < 0.001);
        node.eval(&[Value::F64(212.0)], &mut out);
        assert!((out[0].as_f64() - 100.0).abs() < 0.001);
        node.eval(&[Value::F64(72.0)], &mut out);
        assert!((out[0].as_f64() - 22.22).abs() < 0.1);
    }

    #[test]
    fn quantize_basic() {
        let node = Quantize::new(10.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(13.0)], &mut out);
        assert_eq!(out[0].as_f64(), 10.0);
        node.eval(&[Value::F64(17.0)], &mut out);
        assert_eq!(out[0].as_f64(), 20.0);
        node.eval(&[Value::F64(15.0)], &mut out);
        assert_eq!(out[0].as_f64(), 20.0); // round-half-up
    }

    #[test]
    fn quantize_small_step() {
        let node = Quantize::new(0.25);
        let mut out = [Value::None];
        node.eval(&[Value::F64(1.3)], &mut out);
        assert!((out[0].as_f64() - 1.25).abs() < 0.001);
    }
}
