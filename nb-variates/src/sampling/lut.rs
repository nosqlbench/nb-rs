// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! General-purpose interpolating lookup table (LUT).
//!
//! A `LutF64` pre-computes a function `f: [0,1] → f64` at evenly spaced
//! points and provides O(1) linear interpolation at query time. This is
//! the building block that distribution ICD sampling, arbitrary function
//! approximation, and any precomputed f64→f64 mapping can use.
//!
//! The LUT is built at assembly time from any `Fn(f64) -> f64`. At
//! runtime, querying is a single array index + lerp — no branching on
//! distribution type, no function pointer call per sample.

use crate::node::{GkNode, NodeMeta, Port, PortType, Value};

/// A pre-computed interpolating lookup table mapping [0, 1] → f64.
///
/// Built at assembly time. Immutable and thread-safe after construction.
pub struct LutF64 {
    /// Precomputed values at evenly spaced quantiles.
    /// Length = resolution + 1 (includes both endpoints).
    lut: Vec<f64>,
}

impl LutF64 {
    /// Build from an arbitrary function over [0, 1].
    ///
    /// `f(p)` is evaluated at `resolution + 1` evenly spaced points
    /// from 0.0 to 1.0. Non-finite results are replaced with the
    /// nearest finite neighbor.
    pub fn from_fn(f: impl Fn(f64) -> f64, resolution: usize) -> Self {
        assert!(resolution > 0, "resolution must be positive");
        let mut lut = Vec::with_capacity(resolution + 1);
        for i in 0..=resolution {
            let p = i as f64 / resolution as f64;
            lut.push(f(p));
        }
        // Replace non-finite values by scanning forward then backward
        Self::sanitize(&mut lut);
        Self { lut }
    }

    /// Build from a pre-computed slice of values.
    pub fn from_values(values: &[f64]) -> Self {
        assert!(values.len() >= 2, "LUT must have at least 2 entries");
        let mut lut = values.to_vec();
        Self::sanitize(&mut lut);
        Self { lut }
    }

    /// Replace non-finite entries with nearest finite neighbor.
    fn sanitize(lut: &mut [f64]) {
        // Forward pass: replace -inf/nan at start with first finite value
        let mut last_finite = 0.0;
        let mut found_first = false;
        for v in lut.iter_mut() {
            if v.is_finite() {
                last_finite = *v;
                found_first = true;
            } else if found_first {
                *v = last_finite;
            }
        }
        // Backward pass: replace -inf/nan at start with first finite value from end
        let mut last_finite = 0.0;
        for v in lut.iter_mut().rev() {
            if v.is_finite() {
                last_finite = *v;
            } else {
                *v = last_finite;
            }
        }
    }

    /// Query the LUT with linear interpolation.
    ///
    /// `u` should be in [0.0, 1.0]. Values outside are clamped.
    #[inline]
    pub fn sample(&self, u: f64) -> f64 {
        let u = u.clamp(0.0, 1.0);
        let n = (self.lut.len() - 1) as f64;
        let pos = u * n;
        let idx = (pos as usize).min(self.lut.len() - 2);
        let frac = pos - idx as f64;
        self.lut[idx] * (1.0 - frac) + self.lut[idx + 1] * frac
    }

    /// Number of precomputed points (resolution + 1).
    pub fn len(&self) -> usize {
        self.lut.len()
    }

    /// The resolution (number of intervals).
    pub fn resolution(&self) -> usize {
        self.lut.len() - 1
    }
}

// -----------------------------------------------------------------
// GK node: LutSample (f64 → f64)
// -----------------------------------------------------------------

/// GK node that performs interpolating lookup in a precomputed table.
///
/// Signature: `(input: f64) -> (f64)`
///
/// Input is a value in [0, 1]. Output is the interpolated table value.
/// This is a general-purpose node — it doesn't know or care whether the
/// table holds an inverse CDF, a transfer function, or anything else.
pub struct LutSample {
    meta: NodeMeta,
    table: LutF64,
}

impl LutSample {
    /// Create from a pre-built LUT.
    pub fn new(table: LutF64) -> Self {
        Self {
            meta: NodeMeta {
                name: "lut_sample".into(),
                inputs: vec![Port::new("input", PortType::F64)],
                outputs: vec![Port::new("output", PortType::F64)],
            },
            table,
        }
    }
}

impl GkNode for LutSample {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(self.table.sample(inputs[0].as_f64()));
    }
    // No compiled_u64: f64 ports.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lut_identity() {
        let table = LutF64::from_fn(|p| p, 100);
        assert!((table.sample(0.0) - 0.0).abs() < 1e-10);
        assert!((table.sample(0.5) - 0.5).abs() < 0.01);
        assert!((table.sample(1.0) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn lut_quadratic() {
        let table = LutF64::from_fn(|p| p * p, 1000);
        assert!((table.sample(0.5) - 0.25).abs() < 0.001);
        assert!((table.sample(0.0) - 0.0).abs() < 1e-10);
        assert!((table.sample(1.0) - 1.0).abs() < 0.001);
    }

    #[test]
    fn lut_clamps_input() {
        let table = LutF64::from_fn(|p| p * 10.0, 100);
        // Negative input clamps to 0
        assert!((table.sample(-0.5) - 0.0).abs() < 1e-10);
        // Input > 1 clamps to 1
        assert!((table.sample(1.5) - 10.0).abs() < 1e-10);
    }

    #[test]
    fn lut_sanitizes_infinities() {
        let table = LutF64::from_fn(
            |p| {
                if p < 0.01 || p > 0.99 {
                    f64::INFINITY
                } else {
                    p
                }
            },
            100,
        );
        // Edges should be replaced with nearest finite values
        assert!(table.sample(0.0).is_finite());
        assert!(table.sample(1.0).is_finite());
    }

    #[test]
    fn lut_from_values() {
        let table = LutF64::from_values(&[0.0, 5.0, 10.0]);
        assert!((table.sample(0.0) - 0.0).abs() < 1e-10);
        assert!((table.sample(0.5) - 5.0).abs() < 1e-10);
        assert!((table.sample(1.0) - 10.0).abs() < 1e-10);
        // Interpolation at 0.25 should give 2.5
        assert!((table.sample(0.25) - 2.5).abs() < 1e-10);
    }

    #[test]
    fn lut_node_eval() {
        let table = LutF64::from_fn(|p| p * 100.0, 1000);
        let node = LutSample::new(table);
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.5)], &mut out);
        assert!((out[0].as_f64() - 50.0).abs() < 0.1);
    }
}
