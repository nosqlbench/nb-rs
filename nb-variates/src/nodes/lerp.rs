// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Linear interpolation and range mapping nodes.

use crate::node::{
    CompiledU64Op,
    GkNode, NodeMeta, Port, Slot, Value,
};
use crate::fusion::{DecomposedGraph, DecomposedWire, FusedNode};

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
/// JIT level: P3 (compiled_u64 with jit_constants for a and b).
pub struct LerpConst {
    meta: NodeMeta,
    a: f64,
    b: f64,
}

impl LerpConst {
    pub fn new(a: f64, b: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "lerp".into(),
                outs: vec![Port::f64("output")],
                ins: vec![
                    Slot::Wire(Port::f64("t")),
                    Slot::const_f64("a", a),
                    Slot::const_f64("b", b),
                ],
            },
            a,
            b,
        }
    }
}

impl GkNode for LerpConst {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let t = inputs[0].as_f64();
        outputs[0] = Value::F64(self.a + t * (self.b - self.a));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let a = self.a;
        let b = self.b;
        Some(Box::new(move |inputs, outputs| {
            let t = f64::from_bits(inputs[0]);
            outputs[0] = (a + t * (b - a)).to_bits();
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.a.to_bits(), self.b.to_bits()] }
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
/// JIT level: P3 (compiled_u64 with jit_constants for min and range).
pub struct ScaleRange {
    meta: NodeMeta,
    min: f64,
    range: f64,
}

impl ScaleRange {
    pub fn new(min: f64, max: f64) -> Self {
        let range = max - min;
        Self {
            meta: NodeMeta {
                name: "scale_range".into(),
                outs: vec![Port::f64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_f64("min", min),
                    Slot::const_f64("range", range),
                ],
            },
            min,
            range,
        }
    }
}

impl GkNode for ScaleRange {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let t = inputs[0].as_u64() as f64 / u64::MAX as f64;
        outputs[0] = Value::F64(self.min + t * self.range);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let min = self.min;
        let range = self.range;
        Some(Box::new(move |inputs, outputs| {
            let t = inputs[0] as f64 / u64::MAX as f64;
            outputs[0] = (min + t * range).to_bits();
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.min.to_bits(), self.range.to_bits()] }
}

impl FusedNode for ScaleRange {
    /// `scale_range(x, lo, hi)` decomposes to `lerp(unit_interval(x), lo, hi)`.
    fn decomposed(&self) -> DecomposedGraph {
        use crate::sampling::icd::UnitInterval;
        let hi = self.min + self.range;
        let mut g = DecomposedGraph::new(1);
        let ui = g.add_node(Box::new(UnitInterval::new()), vec![DecomposedWire::Input(0)]);
        let lerp = g.add_node(
            Box::new(LerpConst::new(self.min, hi)),
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
/// JIT level: P1 (no compiled_u64; f64 in/out without captured closure).
pub struct InvLerp {
    meta: NodeMeta,
    a: f64,
    inv_range: f64,
}

impl InvLerp {
    pub fn new(a: f64, b: f64) -> Self {
        assert!((b - a).abs() > f64::EPSILON, "range must be non-zero");
        let inv_range = 1.0 / (b - a);
        Self {
            meta: NodeMeta {
                name: "inv_lerp".into(),
                outs: vec![Port::f64("output")],
                ins: vec![
                    Slot::Wire(Port::f64("input")),
                    Slot::const_f64("a", a),
                    Slot::const_f64("inv_range", inv_range),
                ],
            },
            a,
            inv_range,
        }
    }
}

impl GkNode for InvLerp {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let t = (inputs[0].as_f64() - self.a) * self.inv_range;
        outputs[0] = Value::F64(t.clamp(0.0, 1.0));
    }
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
pub struct Remap {
    meta: NodeMeta,
    in_min: f64,
    in_inv_range: f64,
    out_min: f64,
    out_range: f64,
}

impl Remap {
    pub fn new(in_min: f64, in_max: f64, out_min: f64, out_max: f64) -> Self {
        let in_range = in_max - in_min;
        assert!(in_range.abs() > f64::EPSILON, "input range must be non-zero");
        let in_inv_range = 1.0 / in_range;
        let out_range = out_max - out_min;
        Self {
            meta: NodeMeta {
                name: "remap".into(),
                outs: vec![Port::f64("output")],
                ins: vec![
                    Slot::Wire(Port::f64("input")),
                    Slot::const_f64("in_min", in_min),
                    Slot::const_f64("in_inv_range", in_inv_range),
                    Slot::const_f64("out_min", out_min),
                    Slot::const_f64("out_range", out_range),
                ],
            },
            in_min,
            in_inv_range,
            out_min,
            out_range,
        }
    }
}

impl GkNode for Remap {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let t = (inputs[0].as_f64() - self.in_min) * self.in_inv_range;
        outputs[0] = Value::F64(self.out_min + t * self.out_range);
    }
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
/// JIT level: P3 (compiled_u64 with jit_constants for step).
pub struct Quantize {
    meta: NodeMeta,
    step: f64,
}

impl Quantize {
    pub fn new(step: f64) -> Self {
        assert!(step > 0.0, "step must be positive");
        Self {
            meta: NodeMeta {
                name: "quantize".into(),
                outs: vec![Port::f64("output")],
                ins: vec![
                    Slot::Wire(Port::f64("input")),
                    Slot::const_f64("step", step),
                ],
            },
            step,
        }
    }
}

impl GkNode for Quantize {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_f64();
        outputs[0] = Value::F64((v / self.step).round() * self.step);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let step = self.step;
        Some(Box::new(move |inputs, outputs| {
            let v = f64::from_bits(inputs[0]);
            outputs[0] = ((v / step).round() * step).to_bits();
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.step.to_bits()] }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for interpolation and range-mapping nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "lerp", category: C::Interpolation,
            outputs: 1, description: "linear interpolation with fixed endpoints",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "a", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "b", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Linear interpolation: output = a + t * (b - a).\nInput must be an f64 in [0,1] (the interpolation parameter t).\nParameters:\n  input — f64 wire in [0.0, 1.0] (e.g., from unit_interval)\n  a     — start value (when t=0)\n  b     — end value (when t=1)\nExample: lerp(unit_interval(hash(cycle)), -50.0, 50.0)",
        },
        FuncSig {
            name: "scale_range", category: C::Interpolation,
            outputs: 1, description: "map u64 to f64 range",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "min", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "max", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Maps a u64 directly to an f64 in [min, max). Equivalent to\nlerp(unit_interval(input), min, max) but fused into one node.\nParameters:\n  input — u64 wire input (typically hashed)\n  min   — lower bound of output range (inclusive)\n  max   — upper bound of output range (exclusive)\nExample: scale_range(hash(cycle), 0.0, 100.0)",
        },
        FuncSig {
            name: "quantize", category: C::Interpolation,
            outputs: 1, description: "round to nearest multiple of step",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "step", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Round an f64 to the nearest multiple of a step size.\nOutput remains f64 at the grid point (unlike discretize which returns a bucket index).\nUseful for snapping coordinates to a tile grid or binning to fixed intervals.\nParameters:\n  input — f64 wire input\n  step  — grid spacing (f64, must be > 0)\nExample: quantize(scale_range(hash(cycle), 0.0, 100.0), 5.0)  // 0, 5, 10, ..., 100",
        },
    ]
}

/// Try to build a lerp node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "lerp" => Some(Ok(Box::new(LerpConst::new(
            consts.first().map(|c| c.as_f64()).unwrap_or(0.0),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
        )))),
        "scale_range" => Some(Ok(Box::new(ScaleRange::new(
            consts.first().map(|c| c.as_f64()).unwrap_or(0.0),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
        )))),
        "quantize" => Some(Ok(Box::new(Quantize::new(
            consts.first().map(|c| c.as_f64()).unwrap_or(1.0),
        )))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lerp_endpoints() {
        let node = LerpConst::new(10.0, 20.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.0)], &mut out);
        assert_eq!(out[0].as_f64(), 10.0);
        node.eval(&[Value::F64(1.0)], &mut out);
        assert_eq!(out[0].as_f64(), 20.0);
    }

    #[test]
    fn lerp_midpoint() {
        let node = LerpConst::new(0.0, 100.0);
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
