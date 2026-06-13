// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polyfill edge adapters for the 128-bit integer types
//! (`u128`/`i128`) — cranelift I128 under both signedness
//! interpretations (`polydat/docs/design/type_system_alignment.md`
//! §8.1).
//!
//! Conventions mirror the 64-bit rows:
//!
//! - Widenings from the 64-bit carriers are class A; `u64→i128`
//!   is also A (every u64 fits).
//! - Narrowings and cross-signedness casts are class B with
//!   range-check panics.
//! - `Bytes` serdes are **little-endian, exactly 16 bytes**.
//! - JSON projection is a **decimal string** in both directions
//!   (JSON Number is bounded by u64/i64/f64 leaves); the
//!   extractors also accept an in-range JSON Number for
//!   convenience at result-body boundaries.
//! - `→ f64` mirrors `u64→f64`'s class-A treatment (defined for
//!   every input; precision-lossy above 2^53 by the same rule).

use std::sync::Arc;

// =================================================================
// 1. Widenings (class A)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u64_to_u128(n: u64) -> u128 { n as u128 }

#[crate::polydat_node(category = Conversions)]
fn __u64_to_i128(n: u64) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __i64_to_i128(n: i64) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __u128_to_f64(n: u128) -> f64 { n as f64 }

#[crate::polydat_node(category = Conversions)]
fn __i128_to_f64(n: i128) -> f64 { n as f64 }

// Totality fills (type_system.md §3.3 / adapter_catalog_invariants):
// every integer ≤64 bits widens losslessly into the 128-bit
// carriers (unsigned → either signedness; signed → i128 only), and
// `bool` widens to both. The nonzero test `→ bool` is also total.
// All class A — completes the widening + bool families.
#[crate::polydat_node(category = Conversions)]
fn __u8_to_u128(n: u8) -> u128 { n as u128 }

#[crate::polydat_node(category = Conversions)]
fn __u8_to_i128(n: u8) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_u128(n: u16) -> u128 { n as u128 }

#[crate::polydat_node(category = Conversions)]
fn __u16_to_i128(n: u16) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __u32_to_u128(n: u32) -> u128 { n as u128 }

#[crate::polydat_node(category = Conversions)]
fn __u32_to_i128(n: u32) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __i8_to_i128(n: i8) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __i16_to_i128(n: i16) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __i32_to_i128(n: i32) -> i128 { n as i128 }

#[crate::polydat_node(category = Conversions)]
fn __bool_to_u128(b: bool) -> u128 { b as u128 }

#[crate::polydat_node(category = Conversions)]
fn __bool_to_i128(b: bool) -> i128 { b as i128 }

#[crate::polydat_node(category = Conversions)]
fn __u128_to_bool(n: u128) -> bool { n != 0 }

#[crate::polydat_node(category = Conversions)]
fn __i128_to_bool(n: i128) -> bool { n != 0 }

// =================================================================
// 2. Narrowings + cross-signedness (class B — range-checked)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u128_to_u64(n: u128) -> u64 {
    if n > u64::MAX as u128 {
        panic!("__u128_to_u64: value {n} exceeds u64::MAX ({})", u64::MAX);
    }
    n as u64
}

#[crate::polydat_node(category = Conversions)]
fn __i128_to_i64(n: i128) -> i64 {
    if n < i64::MIN as i128 || n > i64::MAX as i128 {
        panic!("__i128_to_i64: value {n} out of i64 range [{}, {}]", i64::MIN, i64::MAX);
    }
    n as i64
}

#[crate::polydat_node(category = Conversions)]
fn __i64_to_u128(n: i64) -> u128 {
    if n < 0 {
        panic!("__i64_to_u128: negative value {n} cannot be represented as u128");
    }
    n as u128
}

#[crate::polydat_node(category = Conversions)]
fn __u128_to_i128(n: u128) -> i128 {
    if n > i128::MAX as u128 {
        panic!("__u128_to_i128: value {n} exceeds i128::MAX ({})", i128::MAX);
    }
    n as i128
}

#[crate::polydat_node(category = Conversions)]
fn __i128_to_u128(n: i128) -> u128 {
    if n < 0 {
        panic!("__i128_to_u128: negative value {n} cannot be represented as u128");
    }
    n as u128
}

#[crate::polydat_node(category = Conversions)]
fn __f64_to_u128(f: f64) -> u128 {
    if !f.is_finite() {
        panic!("__f64_to_u128: non-finite value {f} cannot be represented as u128");
    }
    let n = f.trunc();
    if n < 0.0 || n > u128::MAX as f64 {
        panic!("__f64_to_u128: value {f} out of u128 range");
    }
    n as u128
}

#[crate::polydat_node(category = Conversions)]
fn __f64_to_i128(f: f64) -> i128 {
    if !f.is_finite() {
        panic!("__f64_to_i128: non-finite value {f} cannot be represented as i128");
    }
    let n = f.trunc();
    if n < i128::MIN as f64 || n > i128::MAX as f64 {
        panic!("__f64_to_i128: value {f} out of i128 range");
    }
    n as i128
}

// =================================================================
// 3. Str / display (parse class B; render class A)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u128_to_string(n: u128) -> String { n.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __i128_to_string(n: i128) -> String { n.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __str_to_u128(input: &str) -> u128 {
    let raw = input.trim();
    raw.parse::<u128>()
        .unwrap_or_else(|e| panic!("__str_to_u128: cannot parse {raw:?} as u128: {e}"))
}

#[crate::polydat_node(category = Conversions)]
fn __str_to_i128(input: &str) -> i128 {
    let raw = input.trim();
    raw.parse::<i128>()
        .unwrap_or_else(|e| panic!("__str_to_i128: cannot parse {raw:?} as i128: {e}"))
}

// =================================================================
// 4. Bytes serdes (little-endian, exactly 16 bytes)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u128_to_bytes(n: u128) -> Vec<u8> { n.to_le_bytes().to_vec() }

#[crate::polydat_node(category = Conversions)]
fn __i128_to_bytes(n: i128) -> Vec<u8> { n.to_le_bytes().to_vec() }

#[crate::polydat_node(category = Conversions)]
fn __bytes_to_u128(b: &[u8]) -> u128 {
    if b.len() != 16 {
        panic!("__bytes_to_u128: expected exactly 16 bytes for u128, got {}", b.len());
    }
    u128::from_le_bytes(b.try_into().unwrap())
}

#[crate::polydat_node(category = Conversions)]
fn __bytes_to_i128(b: &[u8]) -> i128 {
    if b.len() != 16 {
        panic!("__bytes_to_i128: expected exactly 16 bytes for i128, got {}", b.len());
    }
    i128::from_le_bytes(b.try_into().unwrap())
}

// =================================================================
// 5. Json serdes (decimal-string convention; extractors also
//    accept an in-range JSON Number)
// =================================================================

#[crate::polydat_node(category = Conversions)]
fn __u128_to_json(n: u128) -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::String(n.to_string()))
}

#[crate::polydat_node(category = Conversions)]
fn __i128_to_json(n: i128) -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::String(n.to_string()))
}

#[crate::polydat_node(category = Conversions)]
fn __json_to_u128(j: &serde_json::Value) -> u128 {
    match j {
        serde_json::Value::String(s) => s.trim().parse::<u128>().unwrap_or_else(|e| {
            panic!("__json_to_u128: cannot parse {s:?} as u128: {e}")
        }),
        serde_json::Value::Number(n) => n
            .as_u64()
            .map(|u| u as u128)
            .unwrap_or_else(|| panic!("__json_to_u128: JSON number {n} is not a u64")),
        other => panic!("__json_to_u128: JSON value {other} is not a string or number"),
    }
}

#[crate::polydat_node(category = Conversions)]
fn __json_to_i128(j: &serde_json::Value) -> i128 {
    match j {
        serde_json::Value::String(s) => s.trim().parse::<i128>().unwrap_or_else(|e| {
            panic!("__json_to_i128: cannot parse {s:?} as i128: {e}")
        }),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(|i| i as i128)
            .or_else(|| n.as_u64().map(|u| u as i128))
            .unwrap_or_else(|| panic!("__json_to_i128: JSON number {n} is not an integer")),
        other => panic!("__json_to_i128: JSON value {other} is not a string or number"),
    }
}

// =================================================================
// Tests
// =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Bits128, PolydatNode, Value};

    fn u128v(v: u128) -> Value { Value::U128(Bits128::from_u128(v)) }
    fn i128v(v: i128) -> Value { Value::I128(Bits128::from_i128(v)) }

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

    const BIG: u128 = 0xDEAD_BEEF_CAFE_BABE_0123_4567_89AB_CDEF;

    #[test]
    fn limb_round_trip_preserves_all_bits() {
        assert_eq!(Bits128::from_u128(BIG).as_u128(), BIG);
        assert_eq!(Bits128::from_i128(-1).as_i128(), -1);
        assert_eq!(Bits128::from_i128(i128::MIN).as_i128(), i128::MIN);
    }

    #[test]
    fn widenings_and_narrowings() {
        check(&U64ToU128::new(), Value::U64(u64::MAX), u128v(u64::MAX as u128));
        check(&U64ToI128::new(), Value::U64(u64::MAX), i128v(u64::MAX as i128));
        check(&I64ToI128::new(), Value::I64(-5), i128v(-5));
        check(&U128ToU64::new(), u128v(42), Value::U64(42));
        check_panics(&U128ToU64::new(), u128v(u64::MAX as u128 + 1), "exceeds u64::MAX");
        check(&I128ToI64::new(), i128v(-5), Value::I64(-5));
        check_panics(&I128ToI64::new(), i128v(i64::MAX as i128 + 1), "out of i64 range");
        check_panics(&I64ToU128::new(), Value::I64(-1), "negative");
        check_panics(&I128ToU128::new(), i128v(-1), "negative");
        check(&U128ToI128::new(), u128v(42), i128v(42));
    }

    #[test]
    fn string_and_json_round_trip() {
        check(&U128ToString::new(), u128v(BIG), Value::Str(BIG.to_string().into()));
        check(&StrToU128::new(), Value::Str(BIG.to_string().into()), u128v(BIG));
        check(&I128ToString::new(), i128v(-7), Value::Str("-7".into()));
        check(&StrToI128::new(), Value::Str(" -7 ".into()), i128v(-7));
        // JSON: decimal-string convention both ways …
        check(&U128ToJson::new(), u128v(BIG),
            Value::Json(Arc::new(serde_json::Value::String(BIG.to_string()))));
        check(&JsonToU128::new(),
            Value::Json(Arc::new(serde_json::Value::String(BIG.to_string()))), u128v(BIG));
        // … and the extractor accepts in-range JSON Numbers.
        check(&JsonToI128::new(),
            Value::Json(Arc::new(serde_json::Value::from(-5_i64))), i128v(-5));
    }

    #[test]
    fn bytes_round_trip() {
        let bytes = BIG.to_le_bytes().to_vec();
        check(&U128ToBytes::new(), u128v(BIG), Value::Bytes(bytes.clone().into()));
        check(&BytesToU128::new(), Value::Bytes(bytes.into()), u128v(BIG));
        check_panics(&BytesToU128::new(), Value::Bytes(vec![0; 8].into()), "expected exactly 16 bytes");
    }

    #[test]
    fn display_and_json_projection_render_signed() {
        // The Value-level projections (not just adapter nodes)
        // carry the honest signed rendering.
        assert_eq!(i128v(-5).to_display_string(), "-5");
        assert_eq!(
            i128v(-5).to_json_value(),
            serde_json::Value::String("-5".to_string())
        );
        assert_eq!(u128v(BIG).to_display_string(), BIG.to_string());
    }
}
