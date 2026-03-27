// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Linear interpolation and range mapping nodes.

use crate::node::{GkNode, NodeMeta, Port, Value};

/// Linear interpolation with fixed endpoints.
///
/// Signature: `(t: f64) -> (f64)`
/// Result: `a + t * (b - a)` where a, b are init-time params.
///
/// When t=0 → a, t=1 → b, t=0.5 → midpoint.
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
                inputs: vec![Port::f64("t")],
                outputs: vec![Port::f64("output")],
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
}

/// Map a u64 linearly to an f64 range.
///
/// Signature: `(input: u64) -> (f64)`
/// Maps [0, u64::MAX] to [min, max].
///
/// Convenience: combines UnitInterval + LerpConst.
pub struct ScaleRange {
    meta: NodeMeta,
    min: f64,
    range: f64,
}

impl ScaleRange {
    pub fn new(min: f64, max: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "scale_range".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::f64("output")],
            },
            min,
            range: max - min,
        }
    }
}

impl GkNode for ScaleRange {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let t = inputs[0].as_u64() as f64 / u64::MAX as f64;
        outputs[0] = Value::F64(self.min + t * self.range);
    }
}

/// Inverse linear interpolation: map [a, b] → [0, 1].
///
/// Signature: `(input: f64) -> (f64)`
/// Result: `(input - a) / (b - a)`, clamped to [0, 1].
pub struct InvLerp {
    meta: NodeMeta,
    a: f64,
    inv_range: f64,
}

impl InvLerp {
    pub fn new(a: f64, b: f64) -> Self {
        assert!((b - a).abs() > f64::EPSILON, "range must be non-zero");
        Self {
            meta: NodeMeta {
                name: "inv_lerp".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::f64("output")],
            },
            a,
            inv_range: 1.0 / (b - a),
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
/// Signature: `(input: f64) -> (f64)`
/// Maps [in_min, in_max] → [out_min, out_max] linearly.
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
        Self {
            meta: NodeMeta {
                name: "remap".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::f64("output")],
            },
            in_min,
            in_inv_range: 1.0 / in_range,
            out_min,
            out_range: out_max - out_min,
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
/// Signature: `(input: f64) -> (f64)`
/// Result: `round(input / step) * step`
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
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::f64("output")],
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
}

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
