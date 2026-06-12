// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Type conversion nodes.
//!
//! Two categories:
//! - **Edge adapters** (prefixed `__`): auto-inserted by the assembly
//!   phase for common lossless coercions. Users rarely reference these.
//! - **Explicit conversions**: user-placed nodes for lossy, formatted,
//!   or parameterized conversions. These require deliberate intent.

/// Convert u64 to its decimal string representation.
///
/// Signature: `__u64_to_string(input: u64) -> (String)`
///
/// Edge adapter auto-inserted by the assembly phase when a u64 port
/// feeds a String port. Users rarely reference this directly; prefer
/// `format_u64` or `zero_pad_u64` when explicit formatting is wanted.
///
/// JIT level: P1 (String output; no compiled_u64 path).
// SRD-80 PR B.14 — edge adapter family migrated to
// `#[polydat_node]`. Underscore-prefixed names denote
// assembly-phase auto-inserted bridges, not workload-callable
// functions; the macro preserves the leading underscore via
// the function's identifier.

#[crate::polydat_node(category = Conversions)]
fn __u64_to_string(input: u64) -> String { input.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __f64_to_string(input: f64) -> String { input.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __u64_to_f64(input: u64) -> f64 { input as f64 }

#[crate::polydat_node(category = Conversions)]
fn __bool_to_str(input: bool) -> String {
    if input { "true".into() } else { "false".into() }
}

#[crate::polydat_node(category = Conversions)]
fn __bool_to_u64(input: bool) -> u64 {
    if input { 1 } else { 0 }
}

#[crate::polydat_node(category = Conversions)]
fn __u64_to_bool(input: u64) -> bool { input != 0 }

#[crate::polydat_node(category = Conversions)]
fn __u32_to_u64(input: u32) -> u64 { input as u64 }

#[crate::polydat_node(category = Conversions)]
fn __i32_to_i64(input: i32) -> i64 { input as i64 }

#[crate::polydat_node(category = Conversions)]
fn __f32_to_f64(input: f32) -> f64 { input as f64 }

#[crate::polydat_node(category = Conversions)]
fn __i32_to_f64(input: i32) -> f64 { input as f64 }

#[crate::polydat_node(category = Conversions)]
fn __u32_to_f64(input: u32) -> f64 { input as f64 }

#[crate::polydat_node(category = Conversions)]
fn __i64_to_f64(input: i64) -> f64 { input as f64 }

#[crate::polydat_node(category = Conversions)]
fn __i32_to_string(input: i32) -> String { input.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __i64_to_string(input: i64) -> String { input.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __f32_to_string(input: f32) -> String { input.to_string() }

#[crate::polydat_node(category = Conversions)]
fn __u32_to_string(input: u32) -> String { input.to_string() }

// =================================================================
// String parse adapters (Str→X) — workload-param polyfill
// =================================================================
//
// Workload params arrive as strings (YAML string interpolation,
// comma-split iter-values from `for X in {X_values}`, host-
// supplied scope values via `set:`). These edge adapters heal
// the Str → typed-slot boundary writes so the substrate's
// `adapt_boundary_value` boundary check finds a catalog entry
// instead of surfacing `WriteError::TypeMismatch`.
//
// Three adapters cover the workload-param flow (Bool, U64, F64
// targets). Narrow-numeric parses (U32/I32/I64/F32) are
// deferred — see polydat/docs/design/type_system.md §4.
//
// Each parser trims whitespace, then calls the standard library
// `from_str` (or for Bool, recognises "true"/"false" case-
// insensitive and "1"/"0"). Unparseable input panics with the
// adapter name and the offending value; eval_node's
// catch_unwind enrichment surfaces the panic with the node,
// inputs, and source context.

/// Convert string to bool.
///
/// Signature: `__str_to_bool(input: str) -> (bool)`
///
/// Edge adapter auto-inserted when a str port feeds a bool port.
/// Recognises (case-insensitive) `true` / `false` / `1` / `0`
/// after trimming surrounding whitespace. Any other input
/// panics with a diagnostic.
///
/// JIT level: P1 (Str input; no compiled_u64 path).
#[crate::polydat_node(category = Conversions)]
fn __str_to_bool(input: &str) -> bool {
    let raw = input.trim();
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" => true,
        "false" | "0" => false,
        _ => panic!(
            "__str_to_bool: cannot parse {raw:?} as bool \
             (expected case-insensitive 'true'/'false' or '1'/'0')"
        ),
    }
}

#[crate::polydat_node(category = Conversions)]
fn __str_to_u64(input: &str) -> u64 {
    let raw = input.trim();
    raw.parse::<u64>().unwrap_or_else(|e| {
        panic!("__str_to_u64: cannot parse {raw:?} as u64: {e}")
    })
}

#[crate::polydat_node(category = Conversions)]
fn __str_to_f64(input: &str) -> f64 {
    let raw = input.trim();
    raw.parse::<f64>().unwrap_or_else(|e| {
        panic!("__str_to_f64: cannot parse {raw:?} as f64: {e}")
    })
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
#[crate::polydat_node(category = Conversions)]
fn f64_to_u64(input: f64) -> u64 { input as u64 }

#[crate::polydat_node(category = Conversions)]
fn round_to_u64(input: f64) -> u64 { input.round() as u64 }

#[crate::polydat_node(category = Conversions)]
fn floor_to_u64(input: f64) -> u64 { input.floor() as u64 }

/// Ceiling f64 to u64 (round toward positive infinity).
///
/// Signature: `ceil_to_u64(input: f64) -> (u64)`
///
/// Always rounds up. Use when the discrete result must be at least as
/// large as the continuous input, for example computing a minimum
/// allocation size or page count from a byte length.
///
/// JIT level: P2 (compiled_u64 via f64::from_bits + ceil).
#[crate::polydat_node(category = Conversions)]
fn ceil_to_u64(input: f64) -> u64 { input.ceil() as u64 }

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
#[crate::polydat_node(category = Conversions)]
fn discretize(
    input: f64,
    #[poly_default(100.0f64)] range: crate::derive_support::Const<f64>,
    #[poly_default(10u64)] buckets: crate::derive_support::Const<u64>,
) -> u64 {
    let r = *range;
    let b = *buckets;
    let v = input.clamp(0.0, r - f64::EPSILON);
    let bucket = (v / r * b as f64) as u64;
    bucket.min(b.saturating_sub(1))
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
#[crate::polydat_node(category = Conversions)]
fn format_u64(
    input: u64,
    #[poly_default(10u64)] radix: crate::derive_support::Const<u64>,
) -> String {
    match *radix {
        2 => format!("0b{input:b}"),
        8 => format!("0o{input:o}"),
        16 => format!("0x{input:x}"),
        _ => input.to_string(),
    }
}

impl FormatU64 {
    pub fn decimal() -> Self { Self::new(10) }
    pub fn hex() -> Self { Self::new(16) }
    pub fn octal() -> Self { Self::new(8) }
    pub fn binary() -> Self { Self::new(2) }
    pub fn with_radix(radix: u32) -> Self { Self::new(radix as u64) }
}

// SRD-80 PR B.5 — `format_f64` and `zero_pad_u64` migrated to
// the `#[polydat_node]` derive with `Const<u64>` const args.
// Tests below construct via `FormatF64::new(2)` and
// `ZeroPadU64::new(8)` — both work since the macro generates
// `new(precision: u64)` / `new(width: u64)` and integer
// literals coerce to u64. The historic `usize` parameter type
// is now `u64` end-to-end (operator-visible API change in the
// struct's `new()` signature, but the only call sites are this
// module's own tests).

/// Format an f64 with controlled decimal precision.
///
/// Signature: `format_f64(input: f64, precision: u64) -> (String)`
#[crate::polydat_node(category = Conversions)]
fn format_f64(input: f64, #[poly_default(2)] precision: crate::derive_support::Const<u64>) -> String {
    format!("{:.prec$}", input, prec = *precision as usize)
}

/// Zero-pad a u64 to a fixed width string.
///
/// Signature: `zero_pad_u64(input: u64, width: u64) -> (String)`
#[crate::polydat_node(category = Conversions)]
fn zero_pad_u64(input: u64, #[poly_default(10)] width: crate::derive_support::Const<u64>) -> String {
    format!("{:0>width$}", input, width = *width as usize)
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{FuncCategory, FuncSig};

/// Signatures for type conversion nodes.
pub fn signatures() -> &'static [FuncSig] {
    #[allow(unused_imports)]
    use FuncCategory as C;
    &[
        // `unit_interval` / `clamp_f64` migrated to `#[polydat_node]`
        // per SRD-80b Phase E (library/sampling/icd.rs). The macro
        // emits both FuncSig (via inventory) and the matching
        // builder, so this signatures() list no longer carries them
        // and the corresponding `build_node` arms are gone.
        // `to_f64` / `f64_to_u64` / `round_to_u64` / `floor_to_u64` /
        // `ceil_to_u64` / `discretize` / `format_u64` migrated to
        // `#[polydat_node]` per SRD-80 PR B.14.
        // `format_f64` / `zero_pad_u64` migrated to `#[polydat_node]`
        // per SRD-80 PR B.5 — FuncSigs registered via macro-emitted
        // NodeRegistration.
    ]
}

/// Convert u64 integer value to f64. SRD-80 PR B.14 migration.
#[crate::polydat_node(category = Conversions)]
fn to_f64(input: u64) -> f64 { input as f64 }

/// Try to build a conversion node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(_name: &str, _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    // `unit_interval` / `clamp_f64` route via macro-emitted
    // NodeRegistration per SRD-80b Phase E (sampling/icd.rs).
    // `to_f64` / `f64_to_u64` / `round_to_u64` / `floor_to_u64` /
    // `ceil_to_u64` / `discretize` / `format_u64` route via
    // proc-macro NodeRegistration per SRD-80 PR B.14.
    // `format_f64` / `zero_pad_u64` route through proc-macro-emitted
    // NodeRegistration per SRD-80 PR B.5.
    None
}


/// Assembly-time constant validation. See SRD 15 §"Const Constraint Metadata".
///
/// `format_u64.radix` (`AllowedU64{2,8,10,16}`) and
/// `discretize.range` / `.buckets` (`NonZeroU64`) ride on
/// `ParamSpec.constraint`; Pass 1 enforces them and there's
/// nothing relational left for this validator to do.
pub(crate) fn validate_node(
    _name: &str,
    _consts: &[crate::dsl::factory::ConstArg],
) -> Result<(), String> {
    Ok(())
}

crate::register_nodes!(signatures, build_node, validate_node);
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

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
        // Positive value (legacy bit-stuffed input form — the
        // lenient Wire<i32> extract must keep accepting it during
        // the honest-I64 migration).
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0], Value::I64(42));
        // Negative i32, legacy stuffed (-1 as u32 = 0xFFFFFFFF):
        // sign-extension must survive the lenient extract.
        node.eval(&[Value::U64(0xFFFF_FFFF)], &mut out);
        assert_eq!(out[0], Value::I64(-1));
        // Honest signed carrier input round-trips unchanged.
        node.eval(&[Value::I64(-1)], &mut out);
        assert_eq!(out[0], Value::I64(-1));
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

    // -----------------------------------------------------------
    // Str→X parse adapters (type_system.md §4)
    // -----------------------------------------------------------

    #[test]
    fn str_to_bool_canonical_forms() {
        let node = StrToBool::new();
        let mut out = [Value::None];
        for (input, expected) in [
            ("true", true), ("false", false),
            ("True", true), ("False", false),
            ("TRUE", true), ("FALSE", false),
            ("1", true), ("0", false),
        ] {
            node.eval(&[Value::Str(input.into())], &mut out);
            assert_eq!(out[0].as_bool(), expected, "input={input:?}");
        }
    }

    #[test]
    fn str_to_bool_trims_whitespace() {
        let node = StrToBool::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("  true  ".into())], &mut out);
        assert_eq!(out[0].as_bool(), true);
        node.eval(&[Value::Str("\tfalse\n".into())], &mut out);
        assert_eq!(out[0].as_bool(), false);
    }

    #[test]
    #[should_panic(expected = "__str_to_bool")]
    fn str_to_bool_panics_on_unparseable() {
        let node = StrToBool::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("yes".into())], &mut out);
    }

    #[test]
    fn str_to_u64_basic() {
        let node = StrToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("0".into())], &mut out);
        assert_eq!(out[0].as_u64(), 0);
        node.eval(&[Value::Str("42".into())], &mut out);
        assert_eq!(out[0].as_u64(), 42);
        node.eval(&[Value::Str("18446744073709551615".into())], &mut out);
        assert_eq!(out[0].as_u64(), u64::MAX);
    }

    #[test]
    fn str_to_u64_trims_whitespace() {
        let node = StrToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("  42  ".into())], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    #[should_panic(expected = "__str_to_u64")]
    fn str_to_u64_panics_on_negative() {
        let node = StrToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("-1".into())], &mut out);
    }

    #[test]
    #[should_panic(expected = "__str_to_u64")]
    fn str_to_u64_panics_on_garbage() {
        let node = StrToU64::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("abc".into())], &mut out);
    }

    #[test]
    fn str_to_f64_basic() {
        let node = StrToF64::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("0.0".into())], &mut out);
        assert_eq!(out[0].as_f64(), 0.0);
        node.eval(&[Value::Str("3.14".into())], &mut out);
        assert!((out[0].as_f64() - 3.14).abs() < 1e-12);
        node.eval(&[Value::Str("-2.5e3".into())], &mut out);
        assert_eq!(out[0].as_f64(), -2500.0);
        node.eval(&[Value::Str("inf".into())], &mut out);
        assert!(out[0].as_f64().is_infinite());
    }

    #[test]
    fn str_to_f64_trims_whitespace() {
        let node = StrToF64::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("  1.5  ".into())], &mut out);
        assert_eq!(out[0].as_f64(), 1.5);
    }

    #[test]
    #[should_panic(expected = "__str_to_f64")]
    fn str_to_f64_panics_on_garbage() {
        let node = StrToF64::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("not-a-number".into())], &mut out);
    }
}
