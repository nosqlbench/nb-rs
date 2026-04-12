// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Type conversion nodes.
//!
//! Two categories:
//! - **Edge adapters** (prefixed `__`): auto-inserted by the assembly
//!   phase for common lossless coercions. Users rarely reference these.
//! - **Explicit conversions**: user-placed nodes for lossy, formatted,
//!   or parameterized conversions. These require deliberate intent.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, PortType, Slot, Value};

/// Convert u64 to its decimal string representation.
///
/// Signature: `__u64_to_string(input: u64) -> (String)`
///
/// Edge adapter auto-inserted by the assembly phase when a u64 port
/// feeds a String port. Users rarely reference this directly; prefer
/// `format_u64` or `zero_pad_u64` when explicit formatting is wanted.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct U64ToString {
    meta: NodeMeta,
}

impl Default for U64ToString {
    fn default() -> Self {
        Self::new()
    }
}

impl U64ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u64_to_string".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
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
/// Signature: `__f64_to_string(input: f64) -> (String)`
///
/// Edge adapter auto-inserted by the assembly phase when an f64 port
/// feeds a String port. Produces Rust's default f64 Display output.
/// For controlled decimal precision, use `format_f64` instead.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct F64ToString {
    meta: NodeMeta,
}

impl Default for F64ToString {
    fn default() -> Self {
        Self::new()
    }
}

impl F64ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__f64_to_string".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::f64("input"))],
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

/// Convert u64 to f64 (lossless for values <= 2^53).
///
/// Signature: `__u64_to_f64(input: u64) -> (f64)`
///
/// Edge adapter auto-inserted when a u64 port feeds an f64 port.
/// Lossless for values up to 2^53; larger values lose low-order bits.
/// In practice this is safe because hashed u64 values uniformly span
/// the full range, and downstream f64 consumers (lerp, distributions)
/// only need proportional accuracy, not exact integer identity.
///
/// JIT level: P1 (no compiled_u64; output type is f64).
pub struct U64ToF64 {
    meta: NodeMeta,
}

impl Default for U64ToF64 {
    fn default() -> Self {
        Self::new()
    }
}

impl U64ToF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u64_to_f64".into(),
                outs: vec![Port::f64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
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
/// Signature: `__bool_to_u64(input: bool) -> (u64)`
///
/// Edge adapter auto-inserted when a bool port feeds a u64 port.
/// Maps `false` to 0 and `true` to 1. Enables boolean predicate
/// results to flow into arithmetic or indexing nodes.
///
/// JIT level: P1 (no compiled_u64; bool input type).
/// Convert bool to string ("true"/"false").
///
/// Signature: `__bool_to_str(input: bool) -> (str)`
///
/// Edge adapter auto-inserted when a bool port feeds a string port.
pub struct BoolToStr {
    meta: NodeMeta,
}

impl BoolToStr {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__bool_to_str".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::bool("input"))],
            },
        }
    }
}

impl GkNode for BoolToStr {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(if inputs[0].as_bool() { "true" } else { "false" }.into());
    }
}

/// Convert bool to u64 (false=0, true=1).
pub struct BoolToU64 {
    meta: NodeMeta,
}

impl Default for BoolToU64 {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__bool_to_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::bool("input"))],
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
/// Signature: `__u64_to_bool(input: u64) -> (bool)`
///
/// Edge adapter auto-inserted when a u64 port feeds a bool port.
/// Zero maps to `false`, any nonzero value maps to `true`. Useful
/// after modular reduction (e.g., `hash_range(h, 2)`) to produce
/// a boolean flag.
///
/// JIT level: P1 (no compiled_u64; bool output type).
pub struct U64ToBool {
    meta: NodeMeta,
}

impl Default for U64ToBool {
    fn default() -> Self {
        Self::new()
    }
}

impl U64ToBool {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u64_to_bool".into(),
                outs: vec![Port::bool("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
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
// Narrower type widening adapters
// =================================================================

/// Convert U32 to U64 (zero-extend).
///
/// Signature: `__u32_to_u64(input: u32) -> (u64)`
///
/// Edge adapter auto-inserted when a u32 port feeds a u64 port.
/// Masks the low 32 bits to ensure clean zero-extension.
///
/// JIT level: P1 (no compiled_u64 path).
pub struct U32ToU64 {
    meta: NodeMeta,
}

impl U32ToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u32_to_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::U32))],
            },
        }
    }
}

impl GkNode for U32ToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64() & 0xFFFF_FFFF);
    }
}

/// Convert I32 to I64 (sign-extend).
///
/// Signature: `__i32_to_i64(input: i32) -> (i64)`
///
/// Edge adapter auto-inserted when an i32 port feeds an i64 port.
/// Sign-extends the 32-bit value stored in the low bits of u64.
///
/// JIT level: P1 (no compiled_u64 path).
pub struct I32ToI64 {
    meta: NodeMeta,
}

impl I32ToI64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__i32_to_i64".into(),
                outs: vec![Port::new("output", PortType::I64)],
                ins: vec![Slot::Wire(Port::new("input", PortType::I32))],
            },
        }
    }
}

impl GkNode for I32ToI64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let i32_val = inputs[0].as_u64() as i32;
        outputs[0] = Value::U64(i32_val as i64 as u64);
    }
}

/// Convert F32 to F64 (precision widening).
///
/// Signature: `__f32_to_f64(input: f32) -> (f64)`
///
/// Edge adapter auto-inserted when an f32 port feeds an f64 port.
/// Extracts f32 bits from the low 32 bits of u64 and widens to f64.
///
/// JIT level: P1 (no compiled_u64 path).
pub struct F32ToF64 {
    meta: NodeMeta,
}

impl F32ToF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__f32_to_f64".into(),
                outs: vec![Port::f64("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::F32))],
            },
        }
    }
}

impl GkNode for F32ToF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let bits = inputs[0].as_u64() as u32;
        let f32_val = f32::from_bits(bits);
        outputs[0] = Value::F64(f32_val as f64);
    }
}

/// Convert I32 to F64.
///
/// Signature: `__i32_to_f64(input: i32) -> (f64)`
///
/// Edge adapter auto-inserted when an i32 port feeds an f64 port.
/// Sign-extends the 32-bit value and converts to f64 (lossless).
///
/// JIT level: P1 (no compiled_u64 path).
pub struct I32ToF64 {
    meta: NodeMeta,
}

impl I32ToF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__i32_to_f64".into(),
                outs: vec![Port::f64("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::I32))],
            },
        }
    }
}

impl GkNode for I32ToF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let i32_val = inputs[0].as_u64() as i32;
        outputs[0] = Value::F64(i32_val as f64);
    }
}

/// Convert U32 to F64.
///
/// Signature: `__u32_to_f64(input: u32) -> (f64)`
///
/// Edge adapter auto-inserted when a u32 port feeds an f64 port.
/// Masks the low 32 bits and converts to f64 (lossless for all u32 values).
///
/// JIT level: P1 (no compiled_u64 path).
pub struct U32ToF64 {
    meta: NodeMeta,
}

impl U32ToF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u32_to_f64".into(),
                outs: vec![Port::f64("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::U32))],
            },
        }
    }
}

impl GkNode for U32ToF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let u32_val = (inputs[0].as_u64() & 0xFFFF_FFFF) as u32;
        outputs[0] = Value::F64(u32_val as f64);
    }
}

/// Convert I64 to F64.
///
/// Signature: `__i64_to_f64(input: i64) -> (f64)`
///
/// Edge adapter auto-inserted when an i64 port feeds an f64 port.
/// Reinterprets the u64 bits as i64 and converts to f64. Lossless
/// for values with magnitude up to 2^53.
///
/// JIT level: P1 (no compiled_u64 path).
pub struct I64ToF64 {
    meta: NodeMeta,
}

impl I64ToF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__i64_to_f64".into(),
                outs: vec![Port::f64("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::I64))],
            },
        }
    }
}

impl GkNode for I64ToF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let i64_val = inputs[0].as_u64() as i64;
        outputs[0] = Value::F64(i64_val as f64);
    }
}

// -----------------------------------------------------------------
// To-string adapters for narrower types
// -----------------------------------------------------------------

/// Convert I32 to its decimal string representation.
///
/// Signature: `__i32_to_string(input: i32) -> (String)`
///
/// Edge adapter auto-inserted when an i32 port feeds a string port.
/// Sign-extends the 32-bit value and formats as a signed decimal.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct I32ToString {
    meta: NodeMeta,
}

impl I32ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__i32_to_string".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::I32))],
            },
        }
    }
}

impl GkNode for I32ToString {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let i32_val = inputs[0].as_u64() as i32;
        outputs[0] = Value::Str(i32_val.to_string());
    }
}

/// Convert I64 to its decimal string representation.
///
/// Signature: `__i64_to_string(input: i64) -> (String)`
///
/// Edge adapter auto-inserted when an i64 port feeds a string port.
/// Reinterprets the u64 bits as i64 and formats as a signed decimal.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct I64ToString {
    meta: NodeMeta,
}

impl I64ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__i64_to_string".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::I64))],
            },
        }
    }
}

impl GkNode for I64ToString {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let i64_val = inputs[0].as_u64() as i64;
        outputs[0] = Value::Str(i64_val.to_string());
    }
}

/// Convert F32 to its string representation.
///
/// Signature: `__f32_to_string(input: f32) -> (String)`
///
/// Edge adapter auto-inserted when an f32 port feeds a string port.
/// Extracts f32 bits from the low 32 bits and formats via Display.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct F32ToString {
    meta: NodeMeta,
}

impl F32ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__f32_to_string".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::F32))],
            },
        }
    }
}

impl GkNode for F32ToString {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let bits = inputs[0].as_u64() as u32;
        let f32_val = f32::from_bits(bits);
        outputs[0] = Value::Str(f32_val.to_string());
    }
}

/// Convert U32 to its decimal string representation.
///
/// Signature: `__u32_to_string(input: u32) -> (String)`
///
/// Edge adapter auto-inserted when a u32 port feeds a string port.
/// Masks to 32 bits and formats as unsigned decimal.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct U32ToString {
    meta: NodeMeta,
}

impl U32ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u32_to_string".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::U32))],
            },
        }
    }
}

impl GkNode for U32ToString {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let u32_val = (inputs[0].as_u64() & 0xFFFF_FFFF) as u32;
        outputs[0] = Value::Str(u32_val.to_string());
    }
}

// =================================================================
// Explicit conversions (user-placed, deliberate intent)
// =================================================================

/// Truncate f64 to u64 (floor toward zero). Lossy -- requires explicit use.
///
/// Signature: `f64_to_u64(input: f64) -> (u64)`
///
/// Explicit conversion that truncates the fractional part toward zero.
/// Use after distribution sampling or lerp when you need a discrete
/// integer result: `f64_to_u64(lerp(t, 0.0, 1000.0))`. For
/// round-to-nearest, floor, or ceil semantics, use the dedicated
/// `round_to_u64`, `floor_to_u64`, or `ceil_to_u64` nodes instead.
///
/// JIT level: P2 (compiled_u64 via f64::from_bits truncation).
pub struct F64ToU64 {
    meta: NodeMeta,
}

impl Default for F64ToU64 {
    fn default() -> Self {
        Self::new()
    }
}

impl F64ToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "f64_to_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::f64("input"))],
            },
        }
    }
}

impl GkNode for F64ToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64() as u64);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = f64::from_bits(inputs[0]) as u64;
        }))
    }
}

/// Round f64 to nearest u64.
///
/// Signature: `round_to_u64(input: f64) -> (u64)`
///
/// Rounds to the nearest integer (half-up). Use when the distribution
/// or interpolation produces continuous values but downstream nodes
/// need a discrete count or index with minimal rounding bias. Example:
/// `round_to_u64(normal(100.0, 5.0))` yields an integer score centered
/// on 100.
///
/// JIT level: P2 (compiled_u64 via f64::from_bits + round).
pub struct RoundToU64 {
    meta: NodeMeta,
}

impl Default for RoundToU64 {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "round_to_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::f64("input"))],
            },
        }
    }
}

impl GkNode for RoundToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64().round() as u64);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = f64::from_bits(inputs[0]).round() as u64;
        }))
    }
}

/// Floor f64 to u64 (round toward negative infinity).
///
/// Signature: `floor_to_u64(input: f64) -> (u64)`
///
/// Always rounds down. Use when a value must never exceed the
/// continuous input, such as computing a bucket index from a
/// continuous position: `floor_to_u64(scale_range(h, 0.0, 10.0))`
/// yields indices [0, 9].
///
/// JIT level: P2 (compiled_u64 via f64::from_bits + floor).
pub struct FloorToU64 {
    meta: NodeMeta,
}

impl Default for FloorToU64 {
    fn default() -> Self {
        Self::new()
    }
}

impl FloorToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "floor_to_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::f64("input"))],
            },
        }
    }
}

impl GkNode for FloorToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64().floor() as u64);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = f64::from_bits(inputs[0]).floor() as u64;
        }))
    }
}

/// Ceiling f64 to u64 (round toward positive infinity).
///
/// Signature: `ceil_to_u64(input: f64) -> (u64)`
///
/// Always rounds up. Use when the discrete result must be at least as
/// large as the continuous input, for example computing a minimum
/// allocation size or page count from a byte length.
///
/// JIT level: P2 (compiled_u64 via f64::from_bits + ceil).
pub struct CeilToU64 {
    meta: NodeMeta,
}

impl Default for CeilToU64 {
    fn default() -> Self {
        Self::new()
    }
}

impl CeilToU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "ceil_to_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::f64("input"))],
            },
        }
    }
}

impl GkNode for CeilToU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_f64().ceil() as u64);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = f64::from_bits(inputs[0]).ceil() as u64;
        }))
    }
}

/// Discretize: bin a continuous f64 into N equal-width buckets.
///
/// Maps [0, range) to bucket indices [0, buckets). Values outside
/// the range are clamped.
///
/// Signature: `discretize(input: f64, range: f64, buckets: u64) -> (u64)`
///
/// Use after a continuous distribution or interpolation to collapse
/// values into categorical bins. Example: feed a normal distribution
/// through `discretize(100.0, 10)` to get 10 histogram bins across
/// [0, 100). Out-of-range inputs are clamped to the first or last
/// bucket.
///
/// JIT level: P3 (compiled_u64 with jit_constants for range and buckets).
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
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::f64("input")),
                    Slot::const_f64("range", range),
                    Slot::const_u64("buckets", buckets),
                ],
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

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let range = self.range;
        let buckets = self.buckets;
        Some(Box::new(move |inputs, outputs| {
            let v = f64::from_bits(inputs[0]).clamp(0.0, range - f64::EPSILON);
            let bucket = (v / range * buckets as f64) as u64;
            outputs[0] = bucket.min(buckets - 1);
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.range.to_bits(), self.buckets] }
}

/// Format a u64 as a string with a specific radix (2, 8, 10, 16).
///
/// Signature: `format_u64(input: u64, radix: u32) -> (String)`
///
/// Explicit formatting node for producing human-readable or
/// protocol-specific numeric strings. Includes standard prefixes:
/// `0x` for hex, `0b` for binary, `0o` for octal; no prefix for
/// decimal. Use `FormatU64::hex()` for addresses, `::binary()` for
/// bitmask display, or `::decimal()` for plain numeric strings.
///
/// JIT level: P1 (String output; no compiled_u64 path).
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
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
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
/// Signature: `format_f64(input: f64, precision: usize) -> (String)`
///
/// Produces a fixed-precision decimal string. Use when downstream
/// consumers require consistent decimal places, such as monetary
/// values (`FormatF64::new(2)` for cents) or scientific notation
/// alignment. Precision 0 rounds to the nearest integer string.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct FormatF64 {
    meta: NodeMeta,
    precision: usize,
}

impl FormatF64 {
    pub fn new(precision: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "format_f64".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::f64("input"))],
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
/// Signature: `zero_pad_u64(input: u64, width: usize) -> (String)`
///
/// Produces a left-zero-padded decimal string of at least `width`
/// characters. Does not truncate values wider than `width`. Common
/// for generating fixed-width identifiers, partition keys, or file
/// names: `zero_pad_u64(hash_range(h, 10000), 8)` yields
/// `"00004217"`.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct ZeroPadU64 {
    meta: NodeMeta,
    width: usize,
}

impl ZeroPadU64 {
    pub fn new(width: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "zero_pad_u64".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
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

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for type conversion nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "unit_interval", category: C::Conversions, outputs: 1,
            description: "normalize u64 to f64 in [0, 1)",
            help: "Convert a u64 to an f64 in [0.0, 1.0) by dividing by 2^64.\nBridges the integer hash domain into the probability domain.\nFeed the result to lerp, distribution samplers, or coin flips.\nParameters:\n  input — u64 wire input (typically hashed)\nExample: unit_interval(hash(cycle)) -> lerp(0.0, 100.0)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "clamp_f64", category: C::Conversions,
            outputs: 1, description: "clamp f64 to [min, max]",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "min", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "max", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Clamp an f64 value to [min, max].\nUse after distributions with unbounded tails (normal, Cauchy)\nto enforce domain constraints, or to guard against edge values.\nParameters:\n  input — f64 wire input\n  min   — lower bound (inclusive, f64)\n  max   — upper bound (inclusive, f64)\nExample: clamp_f64(icd_normal(hash(cycle), 50.0, 10.0), 0.0, 100.0)",
        },
        FuncSig {
            name: "to_f64", category: C::Conversions, outputs: 1,
            description: "convert u64 integer to f64",
            help: "Numeric conversion: 42u64 becomes 42.0f64.\nNot a bit reinterpret. Values above 2^53 lose precision.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "f64_to_u64", category: C::Conversions, outputs: 1,
            description: "truncate f64 to u64 (lossy)",
            help: "Truncate an f64 to u64 by dropping the fractional part toward zero.\nNegative values and NaN produce 0. Values above u64::MAX saturate.\nUse when you need a raw integer from a float without rounding.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "round_to_u64", category: C::Conversions, outputs: 1,
            description: "round f64 to nearest u64",
            help: "Round an f64 to the nearest u64 (half-to-even / banker's rounding).\nPreferred over truncation when you want the closest integer.\nNegative values and NaN produce 0.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "floor_to_u64", category: C::Conversions, outputs: 1,
            description: "floor f64 to u64",
            help: "Floor an f64 to the next lower u64 (round toward negative infinity).\nFor positive values, equivalent to truncation. Negative values yield 0.\nUse when you want consistent downward rounding.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "ceil_to_u64", category: C::Conversions, outputs: 1,
            description: "ceil f64 to u64",
            help: "Ceiling of an f64 to u64 (round toward positive infinity).\nAlways rounds up: 2.1 becomes 3. Negative values yield 0.\nUse when you need the next integer above a continuous value.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "discretize", category: C::Conversions,
            outputs: 1, description: "bin f64 into N equal-width buckets",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "range", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "buckets", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Bin a continuous f64 into N equal-width integer buckets.\nInput is an f64 in [0, range); output is a u64 bucket index in [0, buckets).\nOut-of-range inputs are clamped to the first or last bucket.\nParameters:\n  input   — f64 wire input\n  range   — upper bound of the input domain (u64, cast to f64)\n  buckets — number of output bins (u64)\nExample: discretize(scale_range(hash(cycle), 0.0, 100.0), 100, 10)",
        },
        FuncSig {
            name: "format_u64", category: C::Conversions,
            outputs: 1, description: "format u64 as string (decimal/hex/octal/binary)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "radix", slot_type: SlotType::ConstU64, required: false },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Format a u64 as a string in the specified radix.\nRadix: 10=decimal (default), 16=hex (0x prefix), 8=octal (0o),\n2=binary (0b). Omit radix for plain decimal.\nParameters:\n  input — u64 wire input\n  radix — optional base (2, 8, 10, or 16; default 10)\nExample: format_u64(hash(cycle), 16)  // \"0x1a2b3c4d\"",
        },
        FuncSig {
            name: "format_f64", category: C::Conversions,
            outputs: 1, description: "format f64 with decimal precision",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "precision", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Format an f64 with a fixed number of decimal places.\nPrecision 0 rounds to the nearest integer string.\nParameters:\n  input     — f64 wire input\n  precision — number of decimal digits (u64)\nExample: format_f64(scale_range(hash(cycle), 0.0, 100.0), 2)  // \"73.41\"",
        },
        FuncSig {
            name: "zero_pad_u64", category: C::Conversions,
            outputs: 1, description: "zero-pad u64 to fixed width string",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "width", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Zero-pad a u64 to a fixed-width decimal string.\nShorter numbers are left-padded with zeros; longer numbers pass through.\nUseful for fixed-width identifiers, partition keys, or filenames.\nParameters:\n  input — u64 wire input\n  width — minimum string width (u64)\nExample: zero_pad_u64(mod(hash(cycle), 10000), 8)  // \"00004217\"",
        },
    ]
}

/// Convert u64 integer value to f64: `x as f64`.
///
/// NOT a bit reinterpret — this is numeric conversion.
/// `42u64` becomes `42.0f64`. Values > 2^53 lose precision.
///
/// Signature: `to_f64(input: u64) -> (f64)`
pub struct ToF64 {
    meta: NodeMeta,
}

impl ToF64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "to_f64".into(),
                ins: vec![Slot::Wire(Port::u64("input"))],
                outs: vec![Port::f64("output")],
            },
        }
    }
}

impl GkNode for ToF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(inputs[0].as_u64() as f64);
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = (inputs[0] as f64).to_bits();
        }))
    }
}

/// Try to build a conversion node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "unit_interval" => Some(Ok(Box::new(crate::sampling::icd::UnitInterval::new()))),
        "clamp_f64" => Some(Ok(Box::new(crate::sampling::icd::ClampF64::new(
            consts.first().map(|c| c.as_f64()).unwrap_or(f64::MIN),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(f64::MAX),
        )))),
        "to_f64" => Some(Ok(Box::new(ToF64::new()))),
        "f64_to_u64" => Some(Ok(Box::new(F64ToU64::new()))),
        "round_to_u64" => Some(Ok(Box::new(RoundToU64::new()))),
        "floor_to_u64" => Some(Ok(Box::new(FloorToU64::new()))),
        "ceil_to_u64" => Some(Ok(Box::new(CeilToU64::new()))),
        "discretize" => Some(Ok(Box::new(Discretize::new(
            consts.first().map(|c| c.as_f64()).unwrap_or(1.0),
            consts.get(1).map(|c| c.as_u64()).unwrap_or(10),
        )))),
        "format_u64" => Some(Ok(Box::new(FormatU64::with_radix(
            consts.first().map(|c| c.as_u64()).unwrap_or(10) as u32,
        )))),
        "format_f64" => Some(Ok(Box::new(FormatF64::new(
            consts.first().map(|c| c.as_u64()).unwrap_or(2) as usize,
        )))),
        "zero_pad_u64" => Some(Ok(Box::new(ZeroPadU64::new(
            consts.first().map(|c| c.as_u64()).unwrap_or(10) as usize,
        )))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
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

    // ---- Narrower type widening adapter tests ----

    #[test]
    fn u32_to_u64_zero_extends() {
        let node = U32ToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
        // High bits are masked off
        node.eval(&[Value::U64(0xFFFF_FFFF_0000_0001)], &mut out);
        assert_eq!(out[0].as_u64(), 1);
    }

    #[test]
    fn i32_to_i64_sign_extends() {
        let node = I32ToI64::new();
        let mut out = [Value::None];
        // Positive value
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
        // Negative i32 (-1 as u32 = 0xFFFFFFFF)
        node.eval(&[Value::U64(0xFFFF_FFFF)], &mut out);
        assert_eq!(out[0].as_u64(), (-1i64) as u64);
    }

    #[test]
    fn f32_to_f64_widens() {
        let node = F32ToF64::new();
        let mut out = [Value::None];
        let f32_bits = 3.14f32.to_bits() as u64;
        node.eval(&[Value::U64(f32_bits)], &mut out);
        // f32 3.14 widened to f64 should be close to 3.14
        let result = out[0].as_f64();
        assert!((result - 3.14).abs() < 0.001, "got {result}");
    }

    #[test]
    fn i32_to_f64_converts() {
        let node = I32ToF64::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_f64(), 42.0);
        // Negative: -10 as u32
        node.eval(&[Value::U64((-10i32) as u32 as u64)], &mut out);
        assert_eq!(out[0].as_f64(), -10.0);
    }

    #[test]
    fn u32_to_f64_converts() {
        let node = U32ToF64::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(1000)], &mut out);
        assert_eq!(out[0].as_f64(), 1000.0);
    }

    #[test]
    fn i64_to_f64_converts() {
        let node = I64ToF64::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_f64(), 42.0);
        // Negative: -1i64 as u64
        node.eval(&[Value::U64((-1i64) as u64)], &mut out);
        assert_eq!(out[0].as_f64(), -1.0);
    }

    // ---- Narrower to-string adapter tests ----

    #[test]
    fn i32_to_string_formats_signed() {
        let node = I32ToString::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "42");
        node.eval(&[Value::U64((-7i32) as u32 as u64)], &mut out);
        assert_eq!(out[0].as_str(), "-7");
    }

    #[test]
    fn i64_to_string_formats_signed() {
        let node = I64ToString::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(100)], &mut out);
        assert_eq!(out[0].as_str(), "100");
        node.eval(&[Value::U64((-42i64) as u64)], &mut out);
        assert_eq!(out[0].as_str(), "-42");
    }

    #[test]
    fn f32_to_string_formats() {
        let node = F32ToString::new();
        let mut out = [Value::None];
        let bits = 2.5f32.to_bits() as u64;
        node.eval(&[Value::U64(bits)], &mut out);
        assert_eq!(out[0].as_str(), "2.5");
    }

    #[test]
    fn u32_to_string_formats() {
        let node = U32ToString::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(12345)], &mut out);
        assert_eq!(out[0].as_str(), "12345");
    }
}
