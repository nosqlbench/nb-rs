// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Type conversion nodes.
//!
//! Two categories:
//! - **Edge adapters** (prefixed `__`): auto-inserted by the assembly
//!   phase for common lossless coercions. Users rarely reference these.
//! - **Explicit conversions**: user-placed nodes for lossy, formatted,
//!   or parameterized conversions. These require deliberate intent.

use crate::node::{GkNode, NodeMeta, Port, Value};

/// Convert u64 to its decimal string representation.
///
/// Signature: `(input: u64) -> (String)`
pub struct U64ToString {
    meta: NodeMeta,
}

impl U64ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u64_to_string".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::str("output")],
            },
        }
    }
}

impl GkNode for U64ToString {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(inputs[0].as_u64().to_string());
    }
}

/// Convert f64 to its string representation.
///
/// Signature: `(input: f64) -> (String)`
pub struct F64ToString {
    meta: NodeMeta,
}

impl F64ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__f64_to_string".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::str("output")],
            },
        }
    }
}

impl GkNode for F64ToString {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(inputs[0].as_f64().to_string());
    }
}

/// Convert u64 to f64 (lossless for values ≤ 2^53).
///
/// Signature: `(input: u64) -> (f64)`
pub struct U64ToF64 {
    meta: NodeMeta,
}

impl U64ToF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u64_to_f64".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::f64("output")],
            },
        }
    }
}

impl GkNode for U64ToF64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(inputs[0].as_u64() as f64);
    }
}

/// Convert bool to u64 (false=0, true=1).
///
/// Signature: `(input: bool) -> (u64)`
pub struct BoolToU64 {
    meta: NodeMeta,
}

impl BoolToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__bool_to_u64".into(),
                inputs: vec![Port::bool("input")],
                outputs: vec![Port::u64("output")],
            },
        }
    }
}

impl GkNode for BoolToU64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(if inputs[0].as_bool() { 1 } else { 0 });
    }
}

/// Convert u64 to bool (0=false, nonzero=true).
///
/// Signature: `(input: u64) -> (bool)`
pub struct U64ToBool {
    meta: NodeMeta,
}

impl U64ToBool {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u64_to_bool".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::bool("output")],
            },
        }
    }
}

impl GkNode for U64ToBool {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bool(inputs[0].as_u64() != 0);
    }
}

// =================================================================
// Explicit conversions (user-placed, deliberate intent)
// =================================================================

/// Truncate f64 to u64 (floor toward zero). Lossy — requires explicit use.
///
/// Signature: `(input: f64) -> (u64)`
pub struct F64ToU64 {
    meta: NodeMeta,
}

impl F64ToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "f64_to_u64".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::u64("output")],
            },
        }
    }
}

impl GkNode for F64ToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64() as u64);
    }
}

/// Round f64 to nearest u64.
///
/// Signature: `(input: f64) -> (u64)`
pub struct RoundToU64 {
    meta: NodeMeta,
}

impl RoundToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "round_to_u64".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::u64("output")],
            },
        }
    }
}

impl GkNode for RoundToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64().round() as u64);
    }
}

/// Floor f64 to u64 (round toward negative infinity).
///
/// Signature: `(input: f64) -> (u64)`
pub struct FloorToU64 {
    meta: NodeMeta,
}

impl FloorToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "floor_to_u64".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::u64("output")],
            },
        }
    }
}

impl GkNode for FloorToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64().floor() as u64);
    }
}

/// Ceiling f64 to u64 (round toward positive infinity).
///
/// Signature: `(input: f64) -> (u64)`
pub struct CeilToU64 {
    meta: NodeMeta,
}

impl CeilToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "ceil_to_u64".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::u64("output")],
            },
        }
    }
}

impl GkNode for CeilToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64().ceil() as u64);
    }
}

/// Discretize: bin a continuous f64 into N equal-width buckets.
///
/// Maps [0, range) to bucket indices [0, buckets). Values outside
/// the range are clamped.
///
/// Signature: `(input: f64) -> (u64)`
/// Params: `range: f64, buckets: u64`
pub struct Discretize {
    meta: NodeMeta,
    range: f64,
    buckets: u64,
}

impl Discretize {
    pub fn new(range: f64, buckets: u64) -> Self {
        assert!(range > 0.0, "range must be positive");
        assert!(buckets > 0, "buckets must be positive");
        Self {
            meta: NodeMeta {
                name: "discretize".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::u64("output")],
            },
            range,
            buckets,
        }
    }
}

impl GkNode for Discretize {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_f64().clamp(0.0, self.range - f64::EPSILON);
        let bucket = (v / self.range * self.buckets as f64) as u64;
        outputs[0] = Value::U64(bucket.min(self.buckets - 1));
    }
}

/// Format a u64 as a string with a specific radix (2, 8, 10, 16).
///
/// Signature: `(input: u64) -> (String)`
/// Param: `radix: u32`
pub struct FormatU64 {
    meta: NodeMeta,
    radix: u32,
    prefix: &'static str,
}

impl FormatU64 {
    pub fn decimal() -> Self { Self::with_radix(10) }
    pub fn hex() -> Self { Self::with_radix(16) }
    pub fn octal() -> Self { Self::with_radix(8) }
    pub fn binary() -> Self { Self::with_radix(2) }

    pub fn with_radix(radix: u32) -> Self {
        assert!([2, 8, 10, 16].contains(&radix), "radix must be 2, 8, 10, or 16");
        let prefix = match radix {
            2 => "0b",
            8 => "0o",
            16 => "0x",
            _ => "",
        };
        Self {
            meta: NodeMeta {
                name: "format_u64".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::str("output")],
            },
            radix,
            prefix,
        }
    }
}

impl GkNode for FormatU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        let s = match self.radix {
            2 => format!("{}{:b}", self.prefix, v),
            8 => format!("{}{:o}", self.prefix, v),
            16 => format!("{}{:x}", self.prefix, v),
            _ => v.to_string(),
        };
        outputs[0] = Value::Str(s);
    }
}

/// Format an f64 with controlled decimal precision.
///
/// Signature: `(input: f64) -> (String)`
/// Param: `precision: usize`
pub struct FormatF64 {
    meta: NodeMeta,
    precision: usize,
}

impl FormatF64 {
    pub fn new(precision: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "format_f64".into(),
                inputs: vec![Port::f64("input")],
                outputs: vec![Port::str("output")],
            },
            precision,
        }
    }
}

impl GkNode for FormatF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(format!("{:.prec$}", inputs[0].as_f64(), prec = self.precision));
    }
}

/// Zero-pad a u64 to a fixed width string.
///
/// Signature: `(input: u64) -> (String)`
/// Param: `width: usize`
pub struct ZeroPadU64 {
    meta: NodeMeta,
    width: usize,
}

impl ZeroPadU64 {
    pub fn new(width: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "zero_pad_u64".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::str("output")],
            },
            width,
        }
    }
}

impl GkNode for ZeroPadU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(format!("{:0>width$}", inputs[0].as_u64(), width = self.width));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_to_u64_truncates() {
        let node = F64ToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.7)], &mut out);
        assert_eq!(out[0].as_u64(), 3);
        node.eval(&[Value::F64(3.2)], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }

    #[test]
    fn round_to_u64_rounds() {
        let node = RoundToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.7)], &mut out);
        assert_eq!(out[0].as_u64(), 4);
        node.eval(&[Value::F64(3.2)], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }

    #[test]
    fn floor_to_u64_floors() {
        let node = FloorToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.9)], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }

    #[test]
    fn ceil_to_u64_ceils() {
        let node = CeilToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.1)], &mut out);
        assert_eq!(out[0].as_u64(), 4);
    }

    #[test]
    fn discretize_basic() {
        let node = Discretize::new(100.0, 10);
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.0)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
        node.eval(&[Value::F64(55.0)], &mut out);
        assert_eq!(out[0].as_u64(), 5);
        node.eval(&[Value::F64(99.0)], &mut out);
        assert_eq!(out[0].as_u64(), 9);
    }

    #[test]
    fn discretize_clamps() {
        let node = Discretize::new(100.0, 10);
        let mut out = [Value::None];
        node.eval(&[Value::F64(-5.0)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
        node.eval(&[Value::F64(200.0)], &mut out);
        assert_eq!(out[0].as_u64(), 9);
    }

    #[test]
    fn format_u64_hex() {
        let node = FormatU64::hex();
        let mut out = [Value::None];
        node.eval(&[Value::U64(255)], &mut out);
        assert_eq!(out[0].as_str(), "0xff");
    }

    #[test]
    fn format_u64_binary() {
        let node = FormatU64::binary();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "0b101010");
    }

    #[test]
    fn format_u64_decimal() {
        let node = FormatU64::decimal();
        let mut out = [Value::None];
        node.eval(&[Value::U64(12345)], &mut out);
        assert_eq!(out[0].as_str(), "12345");
    }

    #[test]
    fn format_f64_precision() {
        let node = FormatF64::new(2);
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.14159)], &mut out);
        assert_eq!(out[0].as_str(), "3.14");
    }

    #[test]
    fn format_f64_zero_precision() {
        let node = FormatF64::new(0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.7)], &mut out);
        assert_eq!(out[0].as_str(), "4");
    }

    #[test]
    fn zero_pad() {
        let node = ZeroPadU64::new(8);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "00000042");
    }

    #[test]
    fn zero_pad_no_truncation() {
        let node = ZeroPadU64::new(3);
        let mut out = [Value::None];
        node.eval(&[Value::U64(12345)], &mut out);
        assert_eq!(out[0].as_str(), "12345");
    }
}
