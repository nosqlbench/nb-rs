// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polyfill edge adapters covering every cell of the type
//! matrix from `polydat/docs/design/type_system.md` §3 that
//! has a sensible conversion.
//!
//! Existing widening + obvious coercions live in
//! `crate::library::convert`. This module fills in the rest:
//!
//! - Numeric narrowings (`U64→U32`, `I64→I32`, `F64→F32`, …)
//!   with range-check panic on out-of-range.
//! - Bool↔every numeric beyond `Bool↔U64`.
//! - String parsers (`Str→{U32, I32, I64, F32, Bytes, Json,
//!   VecF32, VecI32}`) — boundary-only because they can panic
//!   on unparseable input.
//! - Bytes serdes with **little-endian** byte order for
//!   numeric and vector targets, **lowercase hex** for
//!   `Bytes↔Str`.
//! - Json serdes with **try-parse-or-error-wrap** for
//!   `Str→Json` and shape-validating parses elsewhere.
//! - Vec↔{Str, Bytes, Json} serdes; `VecF32↔VecI32` casts.
//!
//! See `polydat/src/compile/assembly.rs::auto_adapter` and
//! `::boundary_adapter` for catalog registration. The split is
//! by failure mode: anything that can panic on input the
//! assembler couldn't verify (range, parse, shape) lives in
//! `boundary_adapter` only; lossless widenings live in both
//! via the `boundary_adapter` superset relation.

use std::sync::Arc;

use crate::ast::{PolydatNode, NodeMeta, Port, PortType, SliceArc, Slot, Value};

// =================================================================
// Adapter macro
// =================================================================

/// Generate an edge-adapter struct + `Default` + `new` +
/// `PolydatNode` impl from (name, node_name, from_port, to_port,
/// eval closure) tuple. Every adapter in this module follows
/// the same shape: one input slot of `from_port` type, one
/// output port of `to_port` type, eval is a single-input
/// `|v: &Value| -> Value` closure.
macro_rules! polyfill_adapter {
    (
        $name:ident,
        $node_name:literal,
        $from:ident,
        $to:ident,
        $eval:expr $(,)?
    ) => {
        pub struct $name {
            meta: NodeMeta,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $node_name.into(),
                        outs: vec![Port::new("output", PortType::$to)],
                        ins: vec![Slot::Wire(Port::new("input", PortType::$from))],
                    },
                }
            }
        }

        impl PolydatNode for $name {
            fn meta(&self) -> &NodeMeta {
                &self.meta
            }

            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                #[allow(clippy::redundant_closure_call)]
                let f: fn(&Value) -> Value = $eval;
                outputs[0] = f(&inputs[0]);
            }
        }
    };
}

// =================================================================
// 1. Numeric → numeric (narrowings + non-widening casts)
// =================================================================
//
// Bit-stuffing convention (`type_system.md` §1):
//   - U64/U32/I64/I32 share `Value::U64` storage. Narrow ints
//     occupy the low bits with sign extension for signed types.
//   - F64/F32 share `Value::F64` storage; F32 carries the
//     materialised float value (not bit-stuffed) per the
//     existing F32ToF64 widening's convention.
//
// Range-check panics keep silent truncation/saturation out of
// the substrate. Authors who want saturating or wrapping
// narrowing use explicit `*_to_*_wrapping` / `_saturating`
// nodes (not provided by polyfill).

polyfill_adapter!(U64ToU32, "__u64_to_u32", U64, U32, |v| {
    let n = v.as_u64();
    if n > u32::MAX as u64 {
        panic!("__u64_to_u32: value {n} exceeds u32::MAX ({})", u32::MAX);
    }
    Value::U64(n)
});

polyfill_adapter!(U64ToI64, "__u64_to_i64", U64, I64, |v| {
    let n = v.as_u64();
    if n > i64::MAX as u64 {
        panic!("__u64_to_i64: value {n} exceeds i64::MAX ({})", i64::MAX);
    }
    Value::U64(n)
});

polyfill_adapter!(U64ToI32, "__u64_to_i32", U64, I32, |v| {
    let n = v.as_u64();
    if n > i32::MAX as u64 {
        panic!("__u64_to_i32: value {n} exceeds i32::MAX ({})", i32::MAX);
    }
    Value::U64(n as i32 as i64 as u64)
});

polyfill_adapter!(U64ToF32, "__u64_to_f32", U64, F32, |v| {
    let n = v.as_u64();
    // u64 → f32 always succeeds (saturates to f32::INFINITY for
    // very large values) but loses precision; that's the
    // expected lossy-narrowing semantic.
    Value::F64(n as f32 as f64)
});

polyfill_adapter!(U32ToI32, "__u32_to_i32", U32, I32, |v| {
    let n = v.as_u64() & 0xFFFF_FFFF;
    if n > i32::MAX as u64 {
        panic!("__u32_to_i32: value {n} exceeds i32::MAX ({})", i32::MAX);
    }
    Value::U64(n as i32 as i64 as u64)
});

polyfill_adapter!(U32ToF32, "__u32_to_f32", U32, F32, |v| {
    let n = (v.as_u64() & 0xFFFF_FFFF) as u32;
    Value::F64(n as f32 as f64)
});

polyfill_adapter!(I64ToU64, "__i64_to_u64", I64, U64, |v| {
    let n = v.as_u64() as i64;
    if n < 0 {
        panic!("__i64_to_u64: negative value {n} cannot be represented as u64");
    }
    Value::U64(n as u64)
});

polyfill_adapter!(I64ToU32, "__i64_to_u32", I64, U32, |v| {
    let n = v.as_u64() as i64;
    if n < 0 || n > u32::MAX as i64 {
        panic!("__i64_to_u32: value {n} out of u32 range [0, {}]", u32::MAX);
    }
    Value::U64(n as u64)
});

polyfill_adapter!(I64ToI32, "__i64_to_i32", I64, I32, |v| {
    let n = v.as_u64() as i64;
    if n < i32::MIN as i64 || n > i32::MAX as i64 {
        panic!(
            "__i64_to_i32: value {n} out of i32 range [{}, {}]",
            i32::MIN,
            i32::MAX
        );
    }
    Value::U64(n as i32 as i64 as u64)
});

polyfill_adapter!(I64ToF32, "__i64_to_f32", I64, F32, |v| {
    let n = v.as_u64() as i64;
    Value::F64(n as f32 as f64)
});

polyfill_adapter!(I32ToU64, "__i32_to_u64", I32, U64, |v| {
    let n = v.as_u64() as i32;
    if n < 0 {
        panic!("__i32_to_u64: negative value {n} cannot be represented as u64");
    }
    Value::U64(n as u64)
});

polyfill_adapter!(I32ToU32, "__i32_to_u32", I32, U32, |v| {
    let n = v.as_u64() as i32;
    if n < 0 {
        panic!("__i32_to_u32: negative value {n} cannot be represented as u32");
    }
    Value::U64(n as u64)
});

polyfill_adapter!(I32ToF32, "__i32_to_f32", I32, F32, |v| {
    let n = v.as_u64() as i32;
    Value::F64(n as f32 as f64)
});

polyfill_adapter!(F64ToU64, "__f64_to_u64_checked", F64, U64, |v| {
    let f = v.as_f64();
    if !f.is_finite() {
        panic!("__f64_to_u64_checked: non-finite value {f} cannot be represented as u64");
    }
    let n = f.trunc();
    if n < 0.0 || n > u64::MAX as f64 {
        panic!("__f64_to_u64_checked: value {f} out of u64 range [0, {}]", u64::MAX);
    }
    Value::U64(n as u64)
});

polyfill_adapter!(F64ToU32, "__f64_to_u32", F64, U32, |v| {
    let f = v.as_f64();
    if !f.is_finite() {
        panic!("__f64_to_u32: non-finite value {f} cannot be represented as u32");
    }
    let n = f.trunc();
    if n < 0.0 || n > u32::MAX as f64 {
        panic!("__f64_to_u32: value {f} out of u32 range [0, {}]", u32::MAX);
    }
    Value::U64(n as u64)
});

polyfill_adapter!(F64ToI64, "__f64_to_i64", F64, I64, |v| {
    let f = v.as_f64();
    if !f.is_finite() {
        panic!("__f64_to_i64: non-finite value {f} cannot be represented as i64");
    }
    let n = f.trunc();
    if n < i64::MIN as f64 || n > i64::MAX as f64 {
        panic!(
            "__f64_to_i64: value {f} out of i64 range [{}, {}]",
            i64::MIN,
            i64::MAX
        );
    }
    Value::U64(n as i64 as u64)
});

polyfill_adapter!(F64ToI32, "__f64_to_i32", F64, I32, |v| {
    let f = v.as_f64();
    if !f.is_finite() {
        panic!("__f64_to_i32: non-finite value {f} cannot be represented as i32");
    }
    let n = f.trunc();
    if n < i32::MIN as f64 || n > i32::MAX as f64 {
        panic!(
            "__f64_to_i32: value {f} out of i32 range [{}, {}]",
            i32::MIN,
            i32::MAX
        );
    }
    Value::U64(n as i32 as i64 as u64)
});

polyfill_adapter!(F64ToF32, "__f64_to_f32", F64, F32, |v| {
    let f = v.as_f64();
    // f64 → f32 always succeeds (saturates to ±INFINITY for
    // very large magnitudes) but loses precision.
    Value::F64(f as f32 as f64)
});

polyfill_adapter!(F32ToU64, "__f32_to_u64", F32, U64, |v| {
    let f = v.as_f64() as f32;
    if !f.is_finite() {
        panic!("__f32_to_u64: non-finite value {f} cannot be represented as u64");
    }
    let n = f.trunc();
    if n < 0.0 || n > u64::MAX as f32 {
        panic!("__f32_to_u64: value {f} out of u64 range [0, {}]", u64::MAX);
    }
    Value::U64(n as u64)
});

polyfill_adapter!(F32ToU32, "__f32_to_u32", F32, U32, |v| {
    let f = v.as_f64() as f32;
    if !f.is_finite() {
        panic!("__f32_to_u32: non-finite value {f} cannot be represented as u32");
    }
    let n = f.trunc();
    if n < 0.0 || n > u32::MAX as f32 {
        panic!("__f32_to_u32: value {f} out of u32 range [0, {}]", u32::MAX);
    }
    Value::U64(n as u64)
});

polyfill_adapter!(F32ToI64, "__f32_to_i64", F32, I64, |v| {
    let f = v.as_f64() as f32;
    if !f.is_finite() {
        panic!("__f32_to_i64: non-finite value {f} cannot be represented as i64");
    }
    let n = f.trunc();
    if n < i64::MIN as f32 || n > i64::MAX as f32 {
        panic!(
            "__f32_to_i64: value {f} out of i64 range [{}, {}]",
            i64::MIN,
            i64::MAX
        );
    }
    Value::U64(n as i64 as u64)
});

polyfill_adapter!(F32ToI32, "__f32_to_i32", F32, I32, |v| {
    let f = v.as_f64() as f32;
    if !f.is_finite() {
        panic!("__f32_to_i32: non-finite value {f} cannot be represented as i32");
    }
    let n = f.trunc();
    if n < i32::MIN as f32 || n > i32::MAX as f32 {
        panic!(
            "__f32_to_i32: value {f} out of i32 range [{}, {}]",
            i32::MIN,
            i32::MAX
        );
    }
    Value::U64(n as i32 as i64 as u64)
});

// =================================================================
// 2. Bool ↔ numeric (beyond Bool↔U64 in convert.rs)
// =================================================================

polyfill_adapter!(BoolToU32, "__bool_to_u32", Bool, U32, |v| {
    Value::U64(if v.as_bool() { 1 } else { 0 })
});

polyfill_adapter!(BoolToI64, "__bool_to_i64", Bool, I64, |v| {
    Value::U64(if v.as_bool() { 1 } else { 0 })
});

polyfill_adapter!(BoolToI32, "__bool_to_i32", Bool, I32, |v| {
    Value::U64(if v.as_bool() { 1 } else { 0 })
});

polyfill_adapter!(BoolToF64, "__bool_to_f64", Bool, F64, |v| {
    Value::F64(if v.as_bool() { 1.0 } else { 0.0 })
});

polyfill_adapter!(BoolToF32, "__bool_to_f32", Bool, F32, |v| {
    Value::F64(if v.as_bool() { 1.0 } else { 0.0 })
});

polyfill_adapter!(U32ToBool, "__u32_to_bool", U32, Bool, |v| {
    Value::Bool((v.as_u64() & 0xFFFF_FFFF) != 0)
});

polyfill_adapter!(I64ToBool, "__i64_to_bool", I64, Bool, |v| {
    Value::Bool((v.as_u64() as i64) != 0)
});

polyfill_adapter!(I32ToBool, "__i32_to_bool", I32, Bool, |v| {
    Value::Bool((v.as_u64() as i32) != 0)
});

polyfill_adapter!(F64ToBool, "__f64_to_bool", F64, Bool, |v| {
    let f = v.as_f64();
    Value::Bool(f != 0.0 && !f.is_nan())
});

polyfill_adapter!(F32ToBool, "__f32_to_bool", F32, Bool, |v| {
    let f = v.as_f64() as f32;
    Value::Bool(f != 0.0 && !f.is_nan())
});

// =================================================================
// 3. Str → narrow numerics + collections (parsers)
// =================================================================
//
// Boundary-only — every parser can panic on unparseable
// input. Trims whitespace before parsing. Out-of-range
// inputs panic with the diagnostic name + offending value.

polyfill_adapter!(StrToU32, "__str_to_u32", Str, U32, |v| {
    let raw = v.as_str().trim();
    let n = raw
        .parse::<u32>()
        .unwrap_or_else(|e| panic!("__str_to_u32: cannot parse {raw:?} as u32: {e}"));
    Value::U64(n as u64)
});

polyfill_adapter!(StrToI64, "__str_to_i64", Str, I64, |v| {
    let raw = v.as_str().trim();
    let n = raw
        .parse::<i64>()
        .unwrap_or_else(|e| panic!("__str_to_i64: cannot parse {raw:?} as i64: {e}"));
    Value::U64(n as u64)
});

polyfill_adapter!(StrToI32, "__str_to_i32", Str, I32, |v| {
    let raw = v.as_str().trim();
    let n = raw
        .parse::<i32>()
        .unwrap_or_else(|e| panic!("__str_to_i32: cannot parse {raw:?} as i32: {e}"));
    Value::U64(n as i64 as u64)
});

polyfill_adapter!(StrToF32, "__str_to_f32", Str, F32, |v| {
    let raw = v.as_str().trim();
    let n = raw
        .parse::<f32>()
        .unwrap_or_else(|e| panic!("__str_to_f32: cannot parse {raw:?} as f32: {e}"));
    Value::F64(n as f64)
});

polyfill_adapter!(StrToBytes, "__str_to_bytes", Str, Bytes, |v| {
    let raw = v.as_str().trim();
    let bytes = data_encoding::HEXLOWER_PERMISSIVE
        .decode(raw.as_bytes())
        .unwrap_or_else(|e| panic!("__str_to_bytes: cannot hex-decode {raw:?}: {e}"));
    Value::Bytes(Arc::from(bytes.as_slice()))
});

polyfill_adapter!(StrToJson, "__str_to_json", Str, Json, |v| {
    let raw = v.as_str();
    // Try-parse-or-error-wrap: well-formed JSON parses through;
    // malformed JSON wraps as a structured error so the substrate
    // never silently loses the original content. Workload authors
    // see the wrapped error when they pull the resulting Json
    // value downstream.
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(parsed) => Value::Json(Arc::new(parsed)),
        Err(e) => {
            let wrapped = serde_json::json!({
                "error": "invalid JSON",
                "message": e.to_string(),
                "raw": raw,
            });
            Value::Json(Arc::new(wrapped))
        }
    }
});

polyfill_adapter!(StrToVecF32, "__str_to_vec_f32", Str, VecF32, |v| {
    let raw = v.as_str().trim();
    let parsed: serde_json::Value = serde_json::from_str(raw).unwrap_or_else(|e| {
        panic!("__str_to_vec_f32: cannot parse {raw:?} as JSON array: {e}")
    });
    let arr = parsed.as_array().unwrap_or_else(|| {
        panic!("__str_to_vec_f32: parsed JSON is not an array: {raw:?}")
    });
    let elems: Vec<f32> = arr
        .iter()
        .map(|j| {
            j.as_f64().unwrap_or_else(|| {
                panic!("__str_to_vec_f32: element {j:?} is not a number in {raw:?}")
            }) as f32
        })
        .collect();
    Value::VecF32(SliceArc::from_vec(elems))
});

polyfill_adapter!(StrToVecI32, "__str_to_vec_i32", Str, VecI32, |v| {
    let raw = v.as_str().trim();
    let parsed: serde_json::Value = serde_json::from_str(raw).unwrap_or_else(|e| {
        panic!("__str_to_vec_i32: cannot parse {raw:?} as JSON array: {e}")
    });
    let arr = parsed.as_array().unwrap_or_else(|| {
        panic!("__str_to_vec_i32: parsed JSON is not an array: {raw:?}")
    });
    let elems: Vec<i32> = arr
        .iter()
        .map(|j| {
            let n = j.as_i64().unwrap_or_else(|| {
                panic!("__str_to_vec_i32: element {j:?} is not an integer in {raw:?}")
            });
            if !(i32::MIN as i64..=i32::MAX as i64).contains(&n) {
                panic!(
                    "__str_to_vec_i32: element {n} out of i32 range [{}, {}]",
                    i32::MIN,
                    i32::MAX
                );
            }
            n as i32
        })
        .collect();
    Value::VecI32(SliceArc::from_vec(elems))
});

// =================================================================
// 4. Bytes ↔ {numerics, Bool, Str, Json, Vec}
// =================================================================
//
// Conventions:
//   - Numeric ↔ Bytes: **little-endian**, exactly sizeof(N)
//     bytes. Wrong length panics. Matches native CPU layout
//     and binary protocols (CQL, Postgres) that this substrate
//     adapts to.
//   - Bool ↔ Bytes: 1 byte (0x00 / 0x01).
//   - Vec ↔ Bytes: little-endian element bytes. Bytes length
//     must be a multiple of sizeof(element).
//   - Bytes ↔ Str: **lowercase hex** (`data_encoding::HEXLOWER`).
//     Roundtrip-lossless, unambiguous, JSON-safe.

polyfill_adapter!(U64ToBytes, "__u64_to_bytes", U64, Bytes, |v| {
    let bytes = v.as_u64().to_le_bytes();
    Value::Bytes(Arc::from(&bytes[..]))
});

polyfill_adapter!(U32ToBytes, "__u32_to_bytes", U32, Bytes, |v| {
    let bytes = ((v.as_u64() & 0xFFFF_FFFF) as u32).to_le_bytes();
    Value::Bytes(Arc::from(&bytes[..]))
});

polyfill_adapter!(I64ToBytes, "__i64_to_bytes", I64, Bytes, |v| {
    let bytes = (v.as_u64() as i64).to_le_bytes();
    Value::Bytes(Arc::from(&bytes[..]))
});

polyfill_adapter!(I32ToBytes, "__i32_to_bytes", I32, Bytes, |v| {
    let bytes = (v.as_u64() as i32).to_le_bytes();
    Value::Bytes(Arc::from(&bytes[..]))
});

polyfill_adapter!(F64ToBytes, "__f64_to_bytes", F64, Bytes, |v| {
    let bytes = v.as_f64().to_le_bytes();
    Value::Bytes(Arc::from(&bytes[..]))
});

polyfill_adapter!(F32ToBytes, "__f32_to_bytes", F32, Bytes, |v| {
    let bytes = (v.as_f64() as f32).to_le_bytes();
    Value::Bytes(Arc::from(&bytes[..]))
});

polyfill_adapter!(BoolToBytes, "__bool_to_bytes", Bool, Bytes, |v| {
    let byte: u8 = if v.as_bool() { 1 } else { 0 };
    Value::Bytes(Arc::from(&[byte][..]))
});

polyfill_adapter!(BytesToU64, "__bytes_to_u64", Bytes, U64, |v| {
    let b = v.as_bytes();
    if b.len() != 8 {
        panic!(
            "__bytes_to_u64: expected exactly 8 bytes for u64, got {}",
            b.len()
        );
    }
    Value::U64(u64::from_le_bytes(b.try_into().unwrap()))
});

polyfill_adapter!(BytesToU32, "__bytes_to_u32", Bytes, U32, |v| {
    let b = v.as_bytes();
    if b.len() != 4 {
        panic!(
            "__bytes_to_u32: expected exactly 4 bytes for u32, got {}",
            b.len()
        );
    }
    Value::U64(u32::from_le_bytes(b.try_into().unwrap()) as u64)
});

polyfill_adapter!(BytesToI64, "__bytes_to_i64", Bytes, I64, |v| {
    let b = v.as_bytes();
    if b.len() != 8 {
        panic!(
            "__bytes_to_i64: expected exactly 8 bytes for i64, got {}",
            b.len()
        );
    }
    Value::U64(i64::from_le_bytes(b.try_into().unwrap()) as u64)
});

polyfill_adapter!(BytesToI32, "__bytes_to_i32", Bytes, I32, |v| {
    let b = v.as_bytes();
    if b.len() != 4 {
        panic!(
            "__bytes_to_i32: expected exactly 4 bytes for i32, got {}",
            b.len()
        );
    }
    Value::U64(i32::from_le_bytes(b.try_into().unwrap()) as i64 as u64)
});

polyfill_adapter!(BytesToF64, "__bytes_to_f64", Bytes, F64, |v| {
    let b = v.as_bytes();
    if b.len() != 8 {
        panic!(
            "__bytes_to_f64: expected exactly 8 bytes for f64, got {}",
            b.len()
        );
    }
    Value::F64(f64::from_le_bytes(b.try_into().unwrap()))
});

polyfill_adapter!(BytesToF32, "__bytes_to_f32", Bytes, F32, |v| {
    let b = v.as_bytes();
    if b.len() != 4 {
        panic!(
            "__bytes_to_f32: expected exactly 4 bytes for f32, got {}",
            b.len()
        );
    }
    Value::F64(f32::from_le_bytes(b.try_into().unwrap()) as f64)
});

polyfill_adapter!(BytesToBool, "__bytes_to_bool", Bytes, Bool, |v| {
    let b = v.as_bytes();
    if b.len() != 1 {
        panic!(
            "__bytes_to_bool: expected exactly 1 byte for bool, got {}",
            b.len()
        );
    }
    Value::Bool(b[0] != 0)
});

polyfill_adapter!(BytesToStr, "__bytes_to_str", Bytes, Str, |v| {
    let hex = data_encoding::HEXLOWER.encode(v.as_bytes());
    Value::Str(Arc::from(hex))
});

polyfill_adapter!(BytesToJson, "__bytes_to_json", Bytes, Json, |v| {
    let hex = data_encoding::HEXLOWER.encode(v.as_bytes());
    Value::Json(Arc::new(serde_json::Value::String(hex)))
});

polyfill_adapter!(BytesToVecF32, "__bytes_to_vec_f32", Bytes, VecF32, |v| {
    let b = v.as_bytes();
    if !b.len().is_multiple_of(4) {
        panic!(
            "__bytes_to_vec_f32: byte length {} is not a multiple of 4 (f32 size)",
            b.len()
        );
    }
    let elems: Vec<f32> = b
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    Value::VecF32(SliceArc::from_vec(elems))
});

polyfill_adapter!(BytesToVecI32, "__bytes_to_vec_i32", Bytes, VecI32, |v| {
    let b = v.as_bytes();
    if !b.len().is_multiple_of(4) {
        panic!(
            "__bytes_to_vec_i32: byte length {} is not a multiple of 4 (i32 size)",
            b.len()
        );
    }
    let elems: Vec<i32> = b
        .chunks_exact(4)
        .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    Value::VecI32(SliceArc::from_vec(elems))
});

// =================================================================
// 5. Json ↔ {numerics, Bool, Bytes, Vec}
// =================================================================
//
// Json→X extracts from the matching Json variant and panics
// otherwise. X→Json wraps in the corresponding Json variant.
// Bytes round-trips through Json::String of hex per §4.

polyfill_adapter!(U64ToJson, "__u64_to_json", U64, Json, |v| {
    Value::Json(Arc::new(serde_json::Value::from(v.as_u64())))
});

polyfill_adapter!(U32ToJson, "__u32_to_json", U32, Json, |v| {
    Value::Json(Arc::new(serde_json::Value::from(
        (v.as_u64() & 0xFFFF_FFFF) as u32,
    )))
});

polyfill_adapter!(I64ToJson, "__i64_to_json", I64, Json, |v| {
    Value::Json(Arc::new(serde_json::Value::from(v.as_u64() as i64)))
});

polyfill_adapter!(I32ToJson, "__i32_to_json", I32, Json, |v| {
    Value::Json(Arc::new(serde_json::Value::from(v.as_u64() as i32)))
});

polyfill_adapter!(F64ToJson, "__f64_to_json", F64, Json, |v| {
    let f = v.as_f64();
    let n = serde_json::Number::from_f64(f).unwrap_or_else(|| {
        panic!("__f64_to_json: non-finite f64 {f} not representable as JSON number")
    });
    Value::Json(Arc::new(serde_json::Value::Number(n)))
});

polyfill_adapter!(F32ToJson, "__f32_to_json", F32, Json, |v| {
    let f = v.as_f64() as f32;
    let n = serde_json::Number::from_f64(f as f64).unwrap_or_else(|| {
        panic!("__f32_to_json: non-finite f32 {f} not representable as JSON number")
    });
    Value::Json(Arc::new(serde_json::Value::Number(n)))
});

polyfill_adapter!(BoolToJson, "__bool_to_json", Bool, Json, |v| {
    Value::Json(Arc::new(serde_json::Value::Bool(v.as_bool())))
});

polyfill_adapter!(JsonToU64, "__json_to_u64", Json, U64, |v| {
    let j = v.as_json();
    let n = j
        .as_u64()
        .unwrap_or_else(|| panic!("__json_to_u64: JSON value {j} is not a u64"));
    Value::U64(n)
});

polyfill_adapter!(JsonToU32, "__json_to_u32", Json, U32, |v| {
    let j = v.as_json();
    let n = j
        .as_u64()
        .unwrap_or_else(|| panic!("__json_to_u32: JSON value {j} is not a u64"));
    if n > u32::MAX as u64 {
        panic!("__json_to_u32: value {n} exceeds u32::MAX ({})", u32::MAX);
    }
    Value::U64(n)
});

polyfill_adapter!(JsonToI64, "__json_to_i64", Json, I64, |v| {
    let j = v.as_json();
    let n = j
        .as_i64()
        .unwrap_or_else(|| panic!("__json_to_i64: JSON value {j} is not an i64"));
    Value::U64(n as u64)
});

polyfill_adapter!(JsonToI32, "__json_to_i32", Json, I32, |v| {
    let j = v.as_json();
    let n = j
        .as_i64()
        .unwrap_or_else(|| panic!("__json_to_i32: JSON value {j} is not an i64"));
    if !(i32::MIN as i64..=i32::MAX as i64).contains(&n) {
        panic!(
            "__json_to_i32: value {n} out of i32 range [{}, {}]",
            i32::MIN,
            i32::MAX
        );
    }
    Value::U64(n as i32 as i64 as u64)
});

polyfill_adapter!(JsonToF64, "__json_to_f64", Json, F64, |v| {
    let j = v.as_json();
    let f = j
        .as_f64()
        .unwrap_or_else(|| panic!("__json_to_f64: JSON value {j} is not an f64"));
    Value::F64(f)
});

polyfill_adapter!(JsonToF32, "__json_to_f32", Json, F32, |v| {
    let j = v.as_json();
    let f = j
        .as_f64()
        .unwrap_or_else(|| panic!("__json_to_f32: JSON value {j} is not an f64"));
    Value::F64(f as f32 as f64)
});

polyfill_adapter!(JsonToBool, "__json_to_bool", Json, Bool, |v| {
    let j = v.as_json();
    let b = j
        .as_bool()
        .unwrap_or_else(|| panic!("__json_to_bool: JSON value {j} is not a bool"));
    Value::Bool(b)
});

polyfill_adapter!(JsonToBytes, "__json_to_bytes", Json, Bytes, |v| {
    let j = v.as_json();
    let s = j
        .as_str()
        .unwrap_or_else(|| panic!("__json_to_bytes: JSON value {j} is not a string (expected hex)"));
    let bytes = data_encoding::HEXLOWER_PERMISSIVE
        .decode(s.as_bytes())
        .unwrap_or_else(|e| panic!("__json_to_bytes: cannot hex-decode {s:?}: {e}"));
    Value::Bytes(Arc::from(bytes.as_slice()))
});

polyfill_adapter!(JsonToVecF32, "__json_to_vec_f32", Json, VecF32, |v| {
    let j = v.as_json();
    let arr = j
        .as_array()
        .unwrap_or_else(|| panic!("__json_to_vec_f32: JSON value {j} is not an array"));
    let elems: Vec<f32> = arr
        .iter()
        .map(|x| {
            x.as_f64()
                .unwrap_or_else(|| panic!("__json_to_vec_f32: element {x} is not a number"))
                as f32
        })
        .collect();
    Value::VecF32(SliceArc::from_vec(elems))
});

polyfill_adapter!(JsonToVecI32, "__json_to_vec_i32", Json, VecI32, |v| {
    let j = v.as_json();
    let arr = j
        .as_array()
        .unwrap_or_else(|| panic!("__json_to_vec_i32: JSON value {j} is not an array"));
    let elems: Vec<i32> = arr
        .iter()
        .map(|x| {
            let n = x
                .as_i64()
                .unwrap_or_else(|| panic!("__json_to_vec_i32: element {x} is not an integer"));
            if !(i32::MIN as i64..=i32::MAX as i64).contains(&n) {
                panic!(
                    "__json_to_vec_i32: element {n} out of i32 range [{}, {}]",
                    i32::MIN,
                    i32::MAX
                );
            }
            n as i32
        })
        .collect();
    Value::VecI32(SliceArc::from_vec(elems))
});

// =================================================================
// 6. Vec → {Str, Bytes, Json}, Vec ↔ Vec
// =================================================================
//
// Vec → scalar (U64/F64/Bool/...) is intentionally NOT in the
// catalog — there's no single natural convention for
// 'collection to scalar' (first? last? length? sum? mean?).
// Authors who need a length use the explicit `vec_len(v)`
// stdlib node; first element uses `vec_first(v)`, etc.
// (See type_system.md §4 for the exclusion rationale.)

polyfill_adapter!(VecF32ToStr, "__vec_f32_to_str", VecF32, Str, |v| {
    let arr: Vec<serde_json::Value> = v
        .as_vec_f32()
        .iter()
        .map(|&f| {
            serde_json::Value::Number(
                serde_json::Number::from_f64(f as f64)
                    .unwrap_or_else(|| panic!("__vec_f32_to_str: non-finite element {f}")),
            )
        })
        .collect();
    Value::Str(Arc::from(serde_json::Value::Array(arr).to_string()))
});

polyfill_adapter!(VecI32ToStr, "__vec_i32_to_str", VecI32, Str, |v| {
    let arr: Vec<serde_json::Value> = v
        .as_vec_i32()
        .iter()
        .map(|&n| serde_json::Value::Number(serde_json::Number::from(n)))
        .collect();
    Value::Str(Arc::from(serde_json::Value::Array(arr).to_string()))
});

polyfill_adapter!(VecF32ToBytes, "__vec_f32_to_bytes", VecF32, Bytes, |v| {
    let elems = v.as_vec_f32();
    let mut buf = Vec::with_capacity(elems.len() * 4);
    for &f in elems {
        buf.extend_from_slice(&f.to_le_bytes());
    }
    Value::Bytes(Arc::from(buf.as_slice()))
});

polyfill_adapter!(VecI32ToBytes, "__vec_i32_to_bytes", VecI32, Bytes, |v| {
    let elems = v.as_vec_i32();
    let mut buf = Vec::with_capacity(elems.len() * 4);
    for &n in elems {
        buf.extend_from_slice(&n.to_le_bytes());
    }
    Value::Bytes(Arc::from(buf.as_slice()))
});

polyfill_adapter!(VecF32ToJson, "__vec_f32_to_json", VecF32, Json, |v| {
    let arr: Vec<serde_json::Value> = v
        .as_vec_f32()
        .iter()
        .map(|&f| {
            serde_json::Value::Number(
                serde_json::Number::from_f64(f as f64)
                    .unwrap_or_else(|| panic!("__vec_f32_to_json: non-finite element {f}")),
            )
        })
        .collect();
    Value::Json(Arc::new(serde_json::Value::Array(arr)))
});

polyfill_adapter!(VecI32ToJson, "__vec_i32_to_json", VecI32, Json, |v| {
    let arr: Vec<serde_json::Value> = v
        .as_vec_i32()
        .iter()
        .map(|&n| serde_json::Value::Number(serde_json::Number::from(n)))
        .collect();
    Value::Json(Arc::new(serde_json::Value::Array(arr)))
});

polyfill_adapter!(VecI32ToVecF32, "__vec_i32_to_vec_f32", VecI32, VecF32, |v| {
    let elems: Vec<f32> = v.as_vec_i32().iter().map(|&n| n as f32).collect();
    Value::VecF32(SliceArc::from_vec(elems))
});

polyfill_adapter!(VecF32ToVecI32, "__vec_f32_to_vec_i32", VecF32, VecI32, |v| {
    let elems: Vec<i32> = v
        .as_vec_f32()
        .iter()
        .map(|&f| {
            if !f.is_finite() {
                panic!("__vec_f32_to_vec_i32: non-finite element {f} cannot be represented as i32");
            }
            let rounded = f.round();
            if rounded < i32::MIN as f32 || rounded > i32::MAX as f32 {
                panic!(
                    "__vec_f32_to_vec_i32: element {f} out of i32 range [{}, {}]",
                    i32::MIN,
                    i32::MAX
                );
            }
            rounded as i32
        })
        .collect();
    Value::VecI32(SliceArc::from_vec(elems))
});

// =================================================================
// Tests
// =================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn check<N: PolydatNode>(node: &N, input: Value, expected: Value) {
        let mut out = [Value::None];
        node.eval(&[input], &mut out);
        assert_eq!(
            out[0], expected,
            "{} produced wrong output",
            node.meta().name
        );
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
                    .or_else(|| {
                        payload.downcast_ref::<&'static str>().map(|s| (*s).into())
                    })
                    .unwrap_or_default();
                assert!(
                    s.contains(msg_substring),
                    "{} panicked but message didn't contain {msg_substring:?}: {s}",
                    node.meta().name
                );
            }
        }
    }

    // -----------------------------------------------------------
    // Numeric → numeric (happy path)
    // -----------------------------------------------------------

    #[test]
    fn numeric_narrowings_happy_path() {
        check(&U64ToU32::new(), Value::U64(42), Value::U64(42));
        check(&U64ToI64::new(), Value::U64(100), Value::U64(100));
        check(&U64ToI32::new(), Value::U64(100), Value::U64(100));
        check(
            &U64ToF32::new(),
            Value::U64(7),
            Value::F64(7.0_f32 as f64),
        );
        check(&U32ToI32::new(), Value::U64(50), Value::U64(50));
        check(
            &U32ToF32::new(),
            Value::U64(3),
            Value::F64(3.0_f32 as f64),
        );
        check(&I64ToU64::new(), Value::U64(42_i64 as u64), Value::U64(42));
        check(&I64ToU32::new(), Value::U64(42_i64 as u64), Value::U64(42));
        check(
            &I64ToI32::new(),
            Value::U64(-100_i64 as u64),
            Value::U64(-100_i32 as i64 as u64),
        );
        check(&I32ToU64::new(), Value::U64(7), Value::U64(7));
        check(&I32ToU32::new(), Value::U64(7), Value::U64(7));
        check(
            &F64ToF32::new(),
            Value::F64(1.5),
            Value::F64(1.5_f32 as f64),
        );
        check(&F64ToU32::new(), Value::F64(42.7), Value::U64(42));
        check(&F64ToI64::new(), Value::F64(-5.9), Value::U64(-5_i64 as u64));
        check(
            &F64ToI32::new(),
            Value::F64(-5.9),
            Value::U64(-5_i32 as i64 as u64),
        );
    }

    #[test]
    fn numeric_narrowing_range_panics() {
        check_panics(
            &U64ToU32::new(),
            Value::U64(u32::MAX as u64 + 1),
            "exceeds u32::MAX",
        );
        check_panics(
            &U64ToI64::new(),
            Value::U64(u64::MAX),
            "exceeds i64::MAX",
        );
        check_panics(
            &I64ToU64::new(),
            Value::U64(-1_i64 as u64),
            "negative",
        );
        check_panics(
            &I64ToI32::new(),
            Value::U64(i64::MAX as u64),
            "out of i32 range",
        );
        check_panics(
            &F64ToU64::new(),
            Value::F64(f64::NAN),
            "non-finite",
        );
        check_panics(
            &F64ToU64::new(),
            Value::F64(-1.0),
            "out of u64 range",
        );
        check_panics(
            &F64ToI32::new(),
            Value::F64(f64::INFINITY),
            "non-finite",
        );
    }

    // -----------------------------------------------------------
    // Bool ↔ numeric
    // -----------------------------------------------------------

    #[test]
    fn bool_to_numerics_round_trip() {
        check(&BoolToU32::new(), Value::Bool(true), Value::U64(1));
        check(&BoolToI64::new(), Value::Bool(false), Value::U64(0));
        check(&BoolToI32::new(), Value::Bool(true), Value::U64(1));
        check(&BoolToF64::new(), Value::Bool(true), Value::F64(1.0));
        check(&BoolToF32::new(), Value::Bool(false), Value::F64(0.0));
        check(&U32ToBool::new(), Value::U64(7), Value::Bool(true));
        check(&U32ToBool::new(), Value::U64(0), Value::Bool(false));
        check(
            &I64ToBool::new(),
            Value::U64(-1_i64 as u64),
            Value::Bool(true),
        );
        check(&I32ToBool::new(), Value::U64(0), Value::Bool(false));
        check(&F64ToBool::new(), Value::F64(0.1), Value::Bool(true));
        check(&F64ToBool::new(), Value::F64(0.0), Value::Bool(false));
        check(&F64ToBool::new(), Value::F64(f64::NAN), Value::Bool(false));
        check(
            &F32ToBool::new(),
            Value::F64(2.5_f32 as f64),
            Value::Bool(true),
        );
    }

    // -----------------------------------------------------------
    // Str → narrow numerics + collections
    // -----------------------------------------------------------

    #[test]
    fn str_to_narrow_numerics() {
        check(&StrToU32::new(), Value::Str("42".into()), Value::U64(42));
        check(
            &StrToI64::new(),
            Value::Str("-100".into()),
            Value::U64(-100_i64 as u64),
        );
        check(
            &StrToI32::new(),
            Value::Str("-7".into()),
            Value::U64(-7_i32 as i64 as u64),
        );
        check(
            &StrToF32::new(),
            Value::Str("1.5".into()),
            Value::F64(1.5_f32 as f64),
        );
        check_panics(
            &StrToU32::new(),
            Value::Str("4294967296".into()),
            "cannot parse",
        );
        check_panics(
            &StrToI32::new(),
            Value::Str("abc".into()),
            "cannot parse",
        );
    }

    #[test]
    fn str_to_bytes_hex_roundtrip() {
        check(
            &StrToBytes::new(),
            Value::Str("0a0b0c".into()),
            Value::Bytes(Arc::from(&[10u8, 11, 12][..])),
        );
        check_panics(
            &StrToBytes::new(),
            Value::Str("not hex!".into()),
            "hex-decode",
        );
    }

    #[test]
    fn str_to_json_parses_or_wraps_error() {
        // Well-formed JSON parses through.
        let node = StrToJson::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("[1, 2, 3]".into())], &mut out);
        match &out[0] {
            Value::Json(j) => {
                assert!(j.is_array());
                assert_eq!(j.as_array().unwrap().len(), 3);
            }
            other => panic!("expected Json::Array, got {other:?}"),
        }
        // Malformed JSON wraps as error structure (does NOT panic).
        let mut out = [Value::None];
        node.eval(&[Value::Str("{bad json".into())], &mut out);
        match &out[0] {
            Value::Json(j) => {
                let obj = j.as_object().expect("error wrap is object");
                assert_eq!(obj.get("error").and_then(|v| v.as_str()), Some("invalid JSON"));
                assert_eq!(obj.get("raw").and_then(|v| v.as_str()), Some("{bad json"));
                assert!(obj.get("message").and_then(|v| v.as_str()).is_some());
            }
            other => panic!("expected Json error wrap, got {other:?}"),
        }
    }

    #[test]
    fn str_to_vec_parses_arrays() {
        let mut out = [Value::None];
        StrToVecF32::new().eval(&[Value::Str("[1.0, 2.5, -3.0]".into())], &mut out);
        match &out[0] {
            Value::VecF32(arr) => assert_eq!(arr.as_ref(), &[1.0_f32, 2.5, -3.0]),
            other => panic!("expected VecF32, got {other:?}"),
        }
        let mut out = [Value::None];
        StrToVecI32::new().eval(&[Value::Str("[1, 2, -3]".into())], &mut out);
        match &out[0] {
            Value::VecI32(arr) => assert_eq!(arr.as_ref(), &[1_i32, 2, -3]),
            other => panic!("expected VecI32, got {other:?}"),
        }
    }

    // -----------------------------------------------------------
    // Bytes ↔ X (little-endian, lowercase hex)
    // -----------------------------------------------------------

    #[test]
    fn numeric_bytes_le_roundtrip() {
        // u64 round trip
        let mut out = [Value::None];
        U64ToBytes::new().eval(&[Value::U64(0x0102030405060708)], &mut out);
        match &out[0] {
            Value::Bytes(b) => assert_eq!(b.as_ref(), &[8u8, 7, 6, 5, 4, 3, 2, 1]),
            other => panic!("expected Bytes, got {other:?}"),
        }
        let mut out = [Value::None];
        BytesToU64::new().eval(&[Value::Bytes(Arc::from(&[8u8, 7, 6, 5, 4, 3, 2, 1][..]))], &mut out);
        assert_eq!(out[0], Value::U64(0x0102030405060708));

        // u32 round trip
        let mut out = [Value::None];
        U32ToBytes::new().eval(&[Value::U64(0x01020304)], &mut out);
        match &out[0] {
            Value::Bytes(b) => assert_eq!(b.as_ref(), &[4u8, 3, 2, 1]),
            other => panic!("expected Bytes, got {other:?}"),
        }

        // f64 round trip
        let mut out_b = [Value::None];
        F64ToBytes::new().eval(&[Value::F64(3.14)], &mut out_b);
        let bytes = match &out_b[0] {
            Value::Bytes(b) => b.clone(),
            _ => panic!(),
        };
        let mut out_f = [Value::None];
        BytesToF64::new().eval(&[Value::Bytes(bytes)], &mut out_f);
        assert_eq!(out_f[0], Value::F64(3.14));
    }

    #[test]
    fn bytes_length_panics() {
        check_panics(
            &BytesToU64::new(),
            Value::Bytes(Arc::from(&[1u8, 2, 3][..])),
            "expected exactly 8 bytes",
        );
        check_panics(
            &BytesToU32::new(),
            Value::Bytes(Arc::from(&[1u8, 2, 3, 4, 5][..])),
            "expected exactly 4 bytes",
        );
        check_panics(
            &BytesToBool::new(),
            Value::Bytes(Arc::from(&[][..])),
            "expected exactly 1 byte",
        );
    }

    #[test]
    fn bytes_str_lowercase_hex() {
        let mut out = [Value::None];
        BytesToStr::new().eval(
            &[Value::Bytes(Arc::from(&[0xDE_u8, 0xAD, 0xBE, 0xEF][..]))],
            &mut out,
        );
        match &out[0] {
            Value::Str(s) => assert_eq!(&**s, "deadbeef"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn bytes_vec_roundtrip() {
        // VecF32 round trip
        let mut out_b = [Value::None];
        VecF32ToBytes::new().eval(
            &[Value::VecF32(SliceArc::from_vec(vec![1.0_f32, 2.0, 3.0]))],
            &mut out_b,
        );
        let bytes = match &out_b[0] {
            Value::Bytes(b) => b.clone(),
            _ => panic!(),
        };
        let mut out_v = [Value::None];
        BytesToVecF32::new().eval(&[Value::Bytes(bytes)], &mut out_v);
        match &out_v[0] {
            Value::VecF32(arr) => assert_eq!(arr.as_ref(), &[1.0_f32, 2.0, 3.0]),
            other => panic!("expected VecF32, got {other:?}"),
        }
        // Bad length panics
        check_panics(
            &BytesToVecF32::new(),
            Value::Bytes(Arc::from(&[1u8, 2, 3][..])),
            "not a multiple of 4",
        );
    }

    // -----------------------------------------------------------
    // Json ↔ X
    // -----------------------------------------------------------

    #[test]
    fn json_scalar_roundtrip() {
        let mut out = [Value::None];
        U64ToJson::new().eval(&[Value::U64(42)], &mut out);
        let j = match &out[0] {
            Value::Json(j) => j.clone(),
            _ => panic!(),
        };
        let mut out_back = [Value::None];
        JsonToU64::new().eval(&[Value::Json(j)], &mut out_back);
        assert_eq!(out_back[0], Value::U64(42));

        let mut out = [Value::None];
        BoolToJson::new().eval(&[Value::Bool(true)], &mut out);
        assert_eq!(
            out[0],
            Value::Json(Arc::new(serde_json::Value::Bool(true)))
        );
    }

    #[test]
    fn json_shape_panics() {
        check_panics(
            &JsonToU64::new(),
            Value::Json(Arc::new(serde_json::Value::String("abc".into()))),
            "is not a u64",
        );
        check_panics(
            &JsonToBool::new(),
            Value::Json(Arc::new(serde_json::Value::from(0))),
            "is not a bool",
        );
        check_panics(
            &JsonToVecF32::new(),
            Value::Json(Arc::new(serde_json::Value::Bool(false))),
            "is not an array",
        );
    }

    #[test]
    fn json_bytes_via_hex() {
        let mut out_j = [Value::None];
        BytesToJson::new().eval(
            &[Value::Bytes(Arc::from(&[0xDE_u8, 0xAD][..]))],
            &mut out_j,
        );
        let j = match &out_j[0] {
            Value::Json(j) => j.clone(),
            _ => panic!(),
        };
        assert_eq!(j.as_str(), Some("dead"));
        let mut out_b = [Value::None];
        JsonToBytes::new().eval(&[Value::Json(j)], &mut out_b);
        match &out_b[0] {
            Value::Bytes(b) => assert_eq!(b.as_ref(), &[0xDE_u8, 0xAD]),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn float_to_json_non_finite_panics() {
        check_panics(
            &F64ToJson::new(),
            Value::F64(f64::NAN),
            "non-finite",
        );
    }

    // -----------------------------------------------------------
    // Vec ↔ Vec / Vec → {Str, Json, Bytes}
    // -----------------------------------------------------------

    #[test]
    fn vec_cast_lossless_and_lossy() {
        // i32 → f32 (lossless)
        let mut out = [Value::None];
        VecI32ToVecF32::new().eval(
            &[Value::VecI32(SliceArc::from_vec(vec![1_i32, -2, 3]))],
            &mut out,
        );
        match &out[0] {
            Value::VecF32(arr) => assert_eq!(arr.as_ref(), &[1.0_f32, -2.0, 3.0]),
            other => panic!("expected VecF32, got {other:?}"),
        }

        // f32 → i32 (round, panic on out of range)
        let mut out = [Value::None];
        VecF32ToVecI32::new().eval(
            &[Value::VecF32(SliceArc::from_vec(vec![1.4_f32, 2.7, -3.5]))],
            &mut out,
        );
        match &out[0] {
            Value::VecI32(arr) => assert_eq!(arr.as_ref(), &[1_i32, 3, -4]),
            other => panic!("expected VecI32, got {other:?}"),
        }
        check_panics(
            &VecF32ToVecI32::new(),
            Value::VecF32(SliceArc::from_vec(vec![f32::INFINITY])),
            "non-finite",
        );
    }

    #[test]
    fn vec_str_json_serialization() {
        let mut out = [Value::None];
        VecI32ToStr::new().eval(
            &[Value::VecI32(SliceArc::from_vec(vec![1_i32, 2, 3]))],
            &mut out,
        );
        match &out[0] {
            Value::Str(s) => assert_eq!(&**s, "[1,2,3]"),
            other => panic!("expected Str, got {other:?}"),
        }

        let mut out = [Value::None];
        VecF32ToJson::new().eval(
            &[Value::VecF32(SliceArc::from_vec(vec![1.5_f32, 2.0]))],
            &mut out,
        );
        match &out[0] {
            Value::Json(j) => {
                assert!(j.is_array());
                let arr = j.as_array().unwrap();
                assert_eq!(arr[0].as_f64().unwrap() as f32, 1.5_f32);
                assert_eq!(arr[1].as_f64().unwrap() as f32, 2.0_f32);
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }
}

