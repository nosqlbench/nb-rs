// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polyfill edge adapters for the narrow cranelift scalar widths
//! (`u8`/`i8`/`u16`/`i16`/`f16`) added by the full-cranelift type
//! alignment (`polydat/docs/design/type_system_alignment.md` §8.1).
//!
//! Each width mirrors its existing wider sibling's adapter row in
//! `polyfill.rs` exactly — same failure-mode split, same byte
//! order, same panic diagnostics:
//!
//! - `u8`/`u16` mirror `u32` (zero-extend in `Value::U64`).
//! - `i8`/`i16` mirror `i32` (sign-extend in `Value::I64`).
//! - `f16` mirrors `f32` (bit pattern stuffed in `Value::U64`;
//!   widening to f32/f64 is exact, narrowing rounds).
//!
//! Catalog registration lives in
//! `compile/assembly.rs::{auto_adapter, boundary_adapter}` — the
//! lossless widenings are class A (both catalogs), everything
//! that can panic on range/parse/shape is class B (boundary
//! only). Bytes serdes are **little-endian**; `f16 ↔ Bytes` is
//! the 2-byte `to_bits()` pattern.

use std::sync::Arc;

// =================================================================
// 1. Widenings (class A — lossless, always-defined)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u8_to_u64(n: u8) -> u64 { n as u64 }

#[crate::polydat_node(category = Conversions)]
fn __u8_to_u32(n: u8) -> u32 { n as u32 }

#[crate::polydat_node(category = Conversions)]
fn __u8_to_u16(n: u8) -> u16 { n as u16 }

#[crate::polydat_node(category = Conversions)]
fn __u8_to_f64(n: u8) -> f64 { n as f64 }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_u64(n: u16) -> u64 { n as u64 }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_u32(n: u16) -> u32 { n as u32 }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_f64(n: u16) -> f64 { n as f64 }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_i64(n: i8) -> i64 { n as i64 }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_i32(n: i8) -> i32 { n as i32 }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_i16(n: i8) -> i16 { n as i16 }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_f64(n: i8) -> f64 { n as f64 }

#[crate::polydat_node(category = Conversions)]
fn __i16_to_i64(n: i16) -> i64 { n as i64 }

#[crate::polydat_node(category = Conversions)]
fn __i16_to_i32(n: i16) -> i32 { n as i32 }

#[crate::polydat_node(category = Conversions)]
fn __i16_to_f64(n: i16) -> f64 { n as f64 }

// Every f16 is exactly representable in f32 and f64.
#[crate::polydat_node(category = Conversions)]
fn __f16_to_f32(f: half::f16) -> f32 { f.to_f32() }

#[crate::polydat_node(category = Conversions)]
fn __f16_to_f64(f: half::f16) -> f64 { f.to_f64() }

// =================================================================
// 2. Narrowings + non-widening casts (class B — range-checked)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u64_to_u8(n: u64) -> u8 {
    if n > u8::MAX as u64 {
        panic!("__u64_to_u8: value {n} exceeds u8::MAX ({})", u8::MAX);
    }
    n as u8
}

#[crate::polydat_node(category = Conversions)]
fn __u32_to_u8(n: u32) -> u8 {
    if n > u8::MAX as u32 {
        panic!("__u32_to_u8: value {n} exceeds u8::MAX ({})", u8::MAX);
    }
    n as u8
}

#[crate::polydat_node(category = Conversions)]
fn __u16_to_u8(n: u16) -> u8 {
    if n > u8::MAX as u16 {
        panic!("__u16_to_u8: value {n} exceeds u8::MAX ({})", u8::MAX);
    }
    n as u8
}

#[crate::polydat_node(category = Conversions)]
fn __i64_to_u8(n: i64) -> u8 {
    if n < 0 || n > u8::MAX as i64 {
        panic!("__i64_to_u8: value {n} out of u8 range [0, {}]", u8::MAX);
    }
    n as u8
}

#[crate::polydat_node(category = Conversions)]
fn __f64_to_u8(f: f64) -> u8 {
    if !f.is_finite() {
        panic!("__f64_to_u8: non-finite value {f} cannot be represented as u8");
    }
    let n = f.trunc();
    if n < 0.0 || n > u8::MAX as f64 {
        panic!("__f64_to_u8: value {f} out of u8 range [0, {}]", u8::MAX);
    }
    n as u8
}

#[crate::polydat_node(category = Conversions)]
fn __u64_to_u16(n: u64) -> u16 {
    if n > u16::MAX as u64 {
        panic!("__u64_to_u16: value {n} exceeds u16::MAX ({})", u16::MAX);
    }
    n as u16
}

#[crate::polydat_node(category = Conversions)]
fn __u32_to_u16(n: u32) -> u16 {
    if n > u16::MAX as u32 {
        panic!("__u32_to_u16: value {n} exceeds u16::MAX ({})", u16::MAX);
    }
    n as u16
}

#[crate::polydat_node(category = Conversions)]
fn __i64_to_u16(n: i64) -> u16 {
    if n < 0 || n > u16::MAX as i64 {
        panic!("__i64_to_u16: value {n} out of u16 range [0, {}]", u16::MAX);
    }
    n as u16
}

#[crate::polydat_node(category = Conversions)]
fn __f64_to_u16(f: f64) -> u16 {
    if !f.is_finite() {
        panic!("__f64_to_u16: non-finite value {f} cannot be represented as u16");
    }
    let n = f.trunc();
    if n < 0.0 || n > u16::MAX as f64 {
        panic!("__f64_to_u16: value {f} out of u16 range [0, {}]", u16::MAX);
    }
    n as u16
}

#[crate::polydat_node(category = Conversions)]
fn __i64_to_i8(n: i64) -> i8 {
    if n < i8::MIN as i64 || n > i8::MAX as i64 {
        panic!("__i64_to_i8: value {n} out of i8 range [{}, {}]", i8::MIN, i8::MAX);
    }
    n as i8
}

#[crate::polydat_node(category = Conversions)]
fn __i32_to_i8(n: i32) -> i8 {
    if n < i8::MIN as i32 || n > i8::MAX as i32 {
        panic!("__i32_to_i8: value {n} out of i8 range [{}, {}]", i8::MIN, i8::MAX);
    }
    n as i8
}

#[crate::polydat_node(category = Conversions)]
fn __u64_to_i8(n: u64) -> i8 {
    if n > i8::MAX as u64 {
        panic!("__u64_to_i8: value {n} exceeds i8::MAX ({})", i8::MAX);
    }
    n as i8
}

#[crate::polydat_node(category = Conversions)]
fn __f64_to_i8(f: f64) -> i8 {
    if !f.is_finite() {
        panic!("__f64_to_i8: non-finite value {f} cannot be represented as i8");
    }
    let n = f.trunc();
    if n < i8::MIN as f64 || n > i8::MAX as f64 {
        panic!("__f64_to_i8: value {f} out of i8 range [{}, {}]", i8::MIN, i8::MAX);
    }
    n as i8
}

#[crate::polydat_node(category = Conversions)]
fn __i64_to_i16(n: i64) -> i16 {
    if n < i16::MIN as i64 || n > i16::MAX as i64 {
        panic!("__i64_to_i16: value {n} out of i16 range [{}, {}]", i16::MIN, i16::MAX);
    }
    n as i16
}

#[crate::polydat_node(category = Conversions)]
fn __i32_to_i16(n: i32) -> i16 {
    if n < i16::MIN as i32 || n > i16::MAX as i32 {
        panic!("__i32_to_i16: value {n} out of i16 range [{}, {}]", i16::MIN, i16::MAX);
    }
    n as i16
}

#[crate::polydat_node(category = Conversions)]
fn __u64_to_i16(n: u64) -> i16 {
    if n > i16::MAX as u64 {
        panic!("__u64_to_i16: value {n} exceeds i16::MAX ({})", i16::MAX);
    }
    n as i16
}

#[crate::polydat_node(category = Conversions)]
fn __f64_to_i16(f: f64) -> i16 {
    if !f.is_finite() {
        panic!("__f64_to_i16: non-finite value {f} cannot be represented as i16");
    }
    let n = f.trunc();
    if n < i16::MIN as f64 || n > i16::MAX as f64 {
        panic!("__f64_to_i16: value {f} out of i16 range [{}, {}]", i16::MIN, i16::MAX);
    }
    n as i16
}

// f64/f32 → f16 round to nearest representable binary16 (the
// half crate's conversion semantic), saturating to ±INFINITY for
// out-of-range magnitudes — the same lossy-narrowing semantic as
// `__f64_to_f32`.
#[crate::polydat_node(category = Conversions)]
fn __f64_to_f16(f: f64) -> half::f16 { half::f16::from_f64(f) }

#[crate::polydat_node(category = Conversions)]
fn __f32_to_f16(f: f32) -> half::f16 { half::f16::from_f32(f) }

#[crate::polydat_node(category = Conversions)]
fn __u64_to_f16(n: u64) -> half::f16 { half::f16::from_f64(n as f64) }

// =================================================================
// 3. Bool ↔ narrow numerics (class A — 1/0, nonzero test)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __bool_to_u8(b: bool) -> u8 { if b { 1 } else { 0 } }

#[crate::polydat_node(category = Conversions)]
fn __bool_to_u16(b: bool) -> u16 { if b { 1 } else { 0 } }

#[crate::polydat_node(category = Conversions)]
fn __bool_to_i8(b: bool) -> i8 { if b { 1 } else { 0 } }

#[crate::polydat_node(category = Conversions)]
fn __bool_to_i16(b: bool) -> i16 { if b { 1 } else { 0 } }

#[crate::polydat_node(category = Conversions)]
fn __bool_to_f16(b: bool) -> half::f16 {
    if b { half::f16::ONE } else { half::f16::ZERO }
}

#[crate::polydat_node(category = Conversions)]
fn __u8_to_bool(n: u8) -> bool { n != 0 }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_bool(n: u16) -> bool { n != 0 }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_bool(n: i8) -> bool { n != 0 }

#[crate::polydat_node(category = Conversions)]
fn __i16_to_bool(n: i16) -> bool { n != 0 }

#[crate::polydat_node(category = Conversions)]
fn __f16_to_bool(f: half::f16) -> bool {
    f != half::f16::ZERO && f != half::f16::NEG_ZERO && !f.is_nan()
}

// =================================================================
// 4. X → Str (class A — Display render)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u8_to_string(n: u8) -> String { n.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_string(n: u16) -> String { n.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_string(n: i8) -> String { n.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __i16_to_string(n: i16) -> String { n.to_string() }

// Render via the exact f32 widening with the Debug form, matching
// the `Value::VecF16` display rule (whole numbers keep a `.0`).
#[crate::polydat_node(category = Conversions)]
fn __f16_to_string(f: half::f16) -> String { format!("{:?}", f.to_f32()) }

// =================================================================
// 5. Str → narrow numerics (class B — parse-or-panic)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __str_to_u8(input: &str) -> u8 {
    let raw = input.trim();
    raw.parse::<u8>()
        .unwrap_or_else(|e| panic!("__str_to_u8: cannot parse {raw:?} as u8: {e}"))
}

#[crate::polydat_node(category = Conversions)]
fn __str_to_u16(input: &str) -> u16 {
    let raw = input.trim();
    raw.parse::<u16>()
        .unwrap_or_else(|e| panic!("__str_to_u16: cannot parse {raw:?} as u16: {e}"))
}

#[crate::polydat_node(category = Conversions)]
fn __str_to_i8(input: &str) -> i8 {
    let raw = input.trim();
    raw.parse::<i8>()
        .unwrap_or_else(|e| panic!("__str_to_i8: cannot parse {raw:?} as i8: {e}"))
}

#[crate::polydat_node(category = Conversions)]
fn __str_to_i16(input: &str) -> i16 {
    let raw = input.trim();
    raw.parse::<i16>()
        .unwrap_or_else(|e| panic!("__str_to_i16: cannot parse {raw:?} as i16: {e}"))
}

#[crate::polydat_node(category = Conversions)]
fn __str_to_f16(input: &str) -> half::f16 {
    let raw = input.trim();
    let f: f32 = raw
        .parse()
        .unwrap_or_else(|e| panic!("__str_to_f16: cannot parse {raw:?} as f16: {e}"));
    half::f16::from_f32(f)
}

// =================================================================
// 6. Bytes serdes (little-endian; exact-length panic)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u8_to_bytes(n: u8) -> Vec<u8> { vec![n] }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_bytes(n: u16) -> Vec<u8> { n.to_le_bytes().to_vec() }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_bytes(n: i8) -> Vec<u8> { n.to_le_bytes().to_vec() }

#[crate::polydat_node(category = Conversions)]
fn __i16_to_bytes(n: i16) -> Vec<u8> { n.to_le_bytes().to_vec() }

#[crate::polydat_node(category = Conversions)]
fn __f16_to_bytes(f: half::f16) -> Vec<u8> { f.to_bits().to_le_bytes().to_vec() }

#[crate::polydat_node(category = Conversions)]
fn __bytes_to_u8(b: &[u8]) -> u8 {
    if b.len() != 1 {
        panic!("__bytes_to_u8: expected exactly 1 byte for u8, got {}", b.len());
    }
    b[0]
}

#[crate::polydat_node(category = Conversions)]
fn __bytes_to_u16(b: &[u8]) -> u16 {
    if b.len() != 2 {
        panic!("__bytes_to_u16: expected exactly 2 bytes for u16, got {}", b.len());
    }
    u16::from_le_bytes(b.try_into().unwrap())
}

#[crate::polydat_node(category = Conversions)]
fn __bytes_to_i8(b: &[u8]) -> i8 {
    if b.len() != 1 {
        panic!("__bytes_to_i8: expected exactly 1 byte for i8, got {}", b.len());
    }
    b[0] as i8
}

#[crate::polydat_node(category = Conversions)]
fn __bytes_to_i16(b: &[u8]) -> i16 {
    if b.len() != 2 {
        panic!("__bytes_to_i16: expected exactly 2 bytes for i16, got {}", b.len());
    }
    i16::from_le_bytes(b.try_into().unwrap())
}

#[crate::polydat_node(category = Conversions)]
fn __bytes_to_f16(b: &[u8]) -> half::f16 {
    if b.len() != 2 {
        panic!("__bytes_to_f16: expected exactly 2 bytes for f16, got {}", b.len());
    }
    half::f16::from_bits(u16::from_le_bytes(b.try_into().unwrap()))
}

// =================================================================
// 7. Json serdes (integer wraps class A; f16 and extractors
//    class B — non-finite / shape panics)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u8_to_json(n: u8) -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::from(n as u64))
}

#[crate::polydat_node(category = Conversions)]
fn __u16_to_json(n: u16) -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::from(n as u64))
}

#[crate::polydat_node(category = Conversions)]
fn __i8_to_json(n: i8) -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::from(n as i64))
}

#[crate::polydat_node(category = Conversions)]
fn __i16_to_json(n: i16) -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::from(n as i64))
}

#[crate::polydat_node(category = Conversions)]
fn __f16_to_json(f: half::f16) -> Arc<serde_json::Value> {
    let n = serde_json::Number::from_f64(f.to_f64()).unwrap_or_else(|| {
        panic!("__f16_to_json: non-finite f16 {f} not representable as JSON number")
    });
    Arc::new(serde_json::Value::Number(n))
}

#[crate::polydat_node(category = Conversions)]
fn __json_to_u8(j: &serde_json::Value) -> u8 {
    let n = j
        .as_u64()
        .unwrap_or_else(|| panic!("__json_to_u8: JSON value {j} is not a u64"));
    if n > u8::MAX as u64 {
        panic!("__json_to_u8: value {n} exceeds u8::MAX ({})", u8::MAX);
    }
    n as u8
}

#[crate::polydat_node(category = Conversions)]
fn __json_to_u16(j: &serde_json::Value) -> u16 {
    let n = j
        .as_u64()
        .unwrap_or_else(|| panic!("__json_to_u16: JSON value {j} is not a u64"));
    if n > u16::MAX as u64 {
        panic!("__json_to_u16: value {n} exceeds u16::MAX ({})", u16::MAX);
    }
    n as u16
}

#[crate::polydat_node(category = Conversions)]
fn __json_to_i8(j: &serde_json::Value) -> i8 {
    let n = j
        .as_i64()
        .unwrap_or_else(|| panic!("__json_to_i8: JSON value {j} is not an i64"));
    if !(i8::MIN as i64..=i8::MAX as i64).contains(&n) {
        panic!("__json_to_i8: value {n} out of i8 range [{}, {}]", i8::MIN, i8::MAX);
    }
    n as i8
}

#[crate::polydat_node(category = Conversions)]
fn __json_to_i16(j: &serde_json::Value) -> i16 {
    let n = j
        .as_i64()
        .unwrap_or_else(|| panic!("__json_to_i16: JSON value {j} is not an i64"));
    if !(i16::MIN as i64..=i16::MAX as i64).contains(&n) {
        panic!("__json_to_i16: value {n} out of i16 range [{}, {}]", i16::MIN, i16::MAX);
    }
    n as i16
}

#[crate::polydat_node(category = Conversions)]
fn __json_to_f16(j: &serde_json::Value) -> half::f16 {
    let f = j
        .as_f64()
        .unwrap_or_else(|| panic!("__json_to_f16: JSON value {j} is not an f64"));
    half::f16::from_f64(f)
}

// =================================================================
// Tests
// =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    fn check<N: PolydatNode>(node: &N, input: Value, expected: Value) {
        let mut out = [Value::None];
        node.eval(&[input], &mut out);
        assert_eq!(out[0], expected, "{} produced wrong output", node.meta().name);
    }

    fn check_panics<N: PolydatNode>(node: &N, input: Value, msg_substring: &str) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut out = [Value::None];
            node.eval(&[input], &mut out);
        }));
        match result {
            Ok(_) => panic!("{} did not panic as expected", node.meta().name),
            Err(payload) => {
                let s = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                assert!(
                    s.contains(msg_substring),
                    "{} panicked but message didn't contain {msg_substring:?}: {s}",
                    node.meta().name
                );
            }
        }
    }

    // f16 stuffing helper: bit pattern in the low 16 of U64,
    // mirroring the f32 convention.
    fn f16_value(f: f32) -> Value {
        Value::U64(half::f16::from_f32(f).to_bits() as u64)
    }

    #[test]
    fn narrow_widenings() {
        check(&U8ToU64::new(), Value::U64(200), Value::U64(200));
        check(&U8ToU16::new(), Value::U64(200), Value::U64(200));
        check(&U16ToU64::new(), Value::U64(60_000), Value::U64(60_000));
        check(&U8ToF64::new(), Value::U64(7), Value::F64(7.0));
        check(&I8ToI64::new(), Value::I64(-100), Value::I64(-100));
        check(&I8ToI16::new(), Value::I64(-100), Value::I64(-100));
        check(&I16ToI64::new(), Value::I64(-30_000), Value::I64(-30_000));
        check(&I16ToF64::new(), Value::I64(-5), Value::F64(-5.0));
        check(&F16ToF64::new(), f16_value(1.5), Value::F64(1.5));
        check(&F16ToF32::new(), f16_value(1.5), Value::U64(1.5f32.to_bits() as u64));
    }

    #[test]
    fn narrow_narrowings_and_range_panics() {
        check(&U64ToU8::new(), Value::U64(255), Value::U64(255));
        check_panics(&U64ToU8::new(), Value::U64(256), "exceeds u8::MAX");
        check(&U64ToU16::new(), Value::U64(65_535), Value::U64(65_535));
        check_panics(&U64ToU16::new(), Value::U64(65_536), "exceeds u16::MAX");
        check(&I64ToI8::new(), Value::I64(-128), Value::I64(-128));
        check_panics(&I64ToI8::new(), Value::I64(128), "out of i8 range");
        check(&I64ToI16::new(), Value::I64(-32_768), Value::I64(-32_768));
        check_panics(&I64ToI16::new(), Value::I64(32_768), "out of i16 range");
        check_panics(&I64ToU8::new(), Value::I64(-1), "out of u8 range");
        check_panics(&F64ToI8::new(), Value::F64(f64::NAN), "non-finite");
        check(&F64ToI8::new(), Value::F64(-5.9), Value::I64(-5));
        check(&F64ToU16::new(), Value::F64(42.7), Value::U64(42));
        // f64 → f16 rounds to nearest representable binary16.
        check(&F64ToF16::new(), Value::F64(1.5), f16_value(1.5));
    }

    #[test]
    fn narrow_str_parses() {
        check(&StrToU8::new(), Value::Str("200".into()), Value::U64(200));
        check(&StrToU16::new(), Value::Str(" 60000 ".into()), Value::U64(60_000));
        check(&StrToI8::new(), Value::Str("-100".into()), Value::I64(-100));
        check(&StrToI16::new(), Value::Str("-30000".into()), Value::I64(-30_000));
        check(&StrToF16::new(), Value::Str("1.5".into()), f16_value(1.5));
        check_panics(&StrToU8::new(), Value::Str("256".into()), "cannot parse");
        check_panics(&StrToI8::new(), Value::Str("xyz".into()), "cannot parse");
    }

    #[test]
    fn narrow_bytes_round_trip() {
        check(&U8ToBytes::new(), Value::U64(0xAB), Value::Bytes(vec![0xAB].into()));
        check(&BytesToU8::new(), Value::Bytes(vec![0xAB].into()), Value::U64(0xAB));
        check_panics(&BytesToU8::new(), Value::Bytes(vec![1, 2].into()), "expected exactly 1 byte");
        check(&I16ToBytes::new(), Value::I64(-2), Value::Bytes((-2i16).to_le_bytes().to_vec().into()));
        check(&BytesToI16::new(), Value::Bytes((-2i16).to_le_bytes().to_vec().into()), Value::I64(-2));
        let f16_bytes = half::f16::from_f32(1.5).to_bits().to_le_bytes().to_vec();
        check(&F16ToBytes::new(), f16_value(1.5), Value::Bytes(f16_bytes.clone().into()));
        check(&BytesToF16::new(), Value::Bytes(f16_bytes.into()), f16_value(1.5));
    }

    #[test]
    fn narrow_json_and_bool() {
        check(&I8ToJson::new(), Value::I64(-5),
            Value::Json(Arc::new(serde_json::Value::from(-5_i64))));
        check(&JsonToI8::new(),
            Value::Json(Arc::new(serde_json::Value::from(-5_i64))), Value::I64(-5));
        check_panics(&JsonToU8::new(),
            Value::Json(Arc::new(serde_json::Value::from(300_u64))), "exceeds u8::MAX");
        check(&BoolToI8::new(), Value::Bool(true), Value::I64(1));
        check(&BoolToF16::new(), Value::Bool(true), f16_value(1.0));
        check(&I16ToBool::new(), Value::I64(-1), Value::Bool(true));
        check(&F16ToBool::new(), f16_value(0.0), Value::Bool(false));
        check(&F16ToBool::new(), f16_value(2.5), Value::Bool(true));
    }

    #[test]
    fn narrow_string_renders() {
        check(&I8ToString::new(), Value::I64(-7), Value::Str("-7".into()));
        check(&U16ToString::new(), Value::U64(60_000), Value::Str("60000".into()));
        // f16 renders via its exact f32 widening with the Debug
        // form, so whole numbers keep a trailing `.0`.
        check(&F16ToString::new(), f16_value(1.0), Value::Str("1.0".into()));
    }
}
