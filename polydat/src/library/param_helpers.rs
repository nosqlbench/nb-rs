// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Parameter resolution and validation helpers (SRD 12 §"Parameter
//! resolution and validation").
//!
//! These nodes are pass-throughs with assertions on the value they
//! carry. They let workloads say "this parameter must be defined",
//! "this number must be in range", "this string must match a
//! pattern" — with the assertion fired at the earliest point the
//! value is known. For compile-time-constant inputs, constant
//! folding collapses the assertion into a hard compile error. For
//! init-time-resolved workload params, the assertion fires on the
//! first evaluation (effectively init). For live-read inputs, the
//! assertion fires per cycle.
//!
//! Failure is reported via panic with a descriptive message. Panics
//! inside `eval` surface as workload startup errors for init-time
//! values and as cycle-time aborts for live-read inputs; both are
//! the intended consequence of a violated precondition.

use regex::Regex;

// =========================================================================
// required(input) — assert non-None, pass through
// =========================================================================

/// Assert that an input is defined (i.e. not `Value::None`).
///
/// Signature: `required(input: u64) -> u64`. The `Option<u64>`
/// wire shape opts the node into kernel-Rule-1 bypass (the macro
/// auto-emits `accepts_none_inputs() -> true` when any arg is
/// `Option<_>`); the body's `unwrap_or_else` is what actually
/// fires the diagnostic.
#[crate::polydat_node(category = Arithmetic)]
fn required(
    input: Option<u64>,
    #[poly_default("value")] name: crate::derive_support::Const<&str>,
) -> u64 {
    input.unwrap_or_else(|| panic!("required({}): value was not defined", name.0))
}

// =========================================================================
// this_or(primary, default) — first if defined else second
// =========================================================================

/// Return `primary` if it is defined, otherwise `default`.
///
/// Signature: `this_or(primary: Option<u64>, default: u64) -> u64`.
/// The typed-coalesce equivalent of `default_or` — `None` on
/// `primary` triggers the fallback. `Option<u64>` opts into
/// None-tolerance via the macro's auto-emitted
/// `accepts_none_inputs`.
#[crate::polydat_node(category = Arithmetic)]
fn this_or(primary: Option<u64>, default: u64) -> u64 {
    primary.unwrap_or(default)
}

// =========================================================================
// is_positive(input) — assert > 0, pass through
// =========================================================================

/// Assert that a u64 value is strictly positive (> 0).
///
/// Signature: `is_positive(input: u64) -> u64`
/// Assert that a u64 value is strictly positive (> 0). SRD-80
/// PR B.15 migration.
#[crate::polydat_node(category = Arithmetic)]
fn is_positive(
    input: u64,
    #[poly_default("value")] name: crate::derive_support::Const<&str>,
) -> u64 {
    if input == 0 {
        panic!("is_positive({}): value must be > 0, got 0", name.0);
    }
    input
}

// =========================================================================
// in_range(input, lo, hi) — assert lo ≤ input ≤ hi, pass through
// =========================================================================

/// Assert that a u64 value is in the inclusive range `[lo, hi]`.
///
/// Signature: `in_range(input: u64, lo: u64, hi: u64) -> u64`
/// Assert that a u64 value is in the inclusive range `[lo, hi]`.
/// SRD-80 PR B.15 migration.
#[crate::polydat_node(category = Arithmetic)]
fn in_range(
    input: u64,
    #[poly_default(0u64)] lo: crate::derive_support::Const<u64>,
    #[poly_default(u64::MAX)] hi: crate::derive_support::Const<u64>,
) -> u64 {
    if input < *lo || input > *hi {
        panic!("in_range: value {input} outside [{}, {}]", *lo, *hi);
    }
    input
}

// =========================================================================
// is_one_of(input, ...allowed) — assert input ∈ {allowed}, pass through
// =========================================================================

/// Assert that a u64 value is one of an enumerated allow-list.
/// SRD-80b Phase C migration via `Const<Vec<C>>` combinator.
/// JIT-ineligible by virtue of the variable-length allow-list
/// (the JIT u64 buffer has no slot shape for it); the body
/// runs on the typed-eval path.
#[crate::polydat_node(category = Arithmetic)]
fn is_one_of(input: u64, allowed: crate::derive_support::Const<Vec<u64>>) -> u64 {
    if !allowed.contains(&input) {
        panic!("is_one_of: value {input} not in allowed set {:?}", allowed.0);
    }
    input
}

// =========================================================================
// matches(input, pattern) — assert regex match, pass through
// =========================================================================

/// Build a Regex from a pattern, panicking on invalid input.
fn compile_matches_regex(pattern: &str) -> Regex {
    Regex::new(pattern)
        .unwrap_or_else(|e| panic!("matches: invalid regex {pattern:?}: {e}"))
}

/// Assert that a string value matches a regex pattern.
/// SRD-80 PR B.6 migration.
#[crate::polydat_node(category = Arithmetic)]
fn matches(
    input: &str,
    pattern: crate::derive_support::Const<&str>,
    #[poly_const(compile_matches_regex, from = pattern)]
    re: &Regex,
) -> String {
    if !re.is_match(input) {
        panic!("matches: value {input:?} does not match pattern {:?}", pattern.0);
    }
    input.to_string()
}

// =========================================================================
// Registration
// =========================================================================

use crate::dsl::registry::FuncSig;

pub fn signatures() -> &'static [FuncSig] {
    
    &[
        // `required` / `this_or` migrated to `#[polydat_node]` via the
        // `Option<T>` combinator (SRD-80b Phase C).
        // `is_positive` / `in_range` / `matches` already on the macro.
        // `is_one_of` migrated to `#[polydat_node]` via the
        // `Const<Vec<C>>` combinator (SRD-80b Phase C).
    ]
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType],
    consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    let _ = name;
    let _ = consts;
    // All param-helper nodes route via proc-macro-emitted NodeRegistration.
    None
}

/// Assembly-time constant validation for parameter-helper nodes.
/// See SRD 15 §"Const Constraint Metadata".
pub(crate) fn validate_node(
    name: &str,
    consts: &[crate::dsl::factory::ConstArg],
) -> Result<(), String> {
    match name {
        "in_range" => {
            let lo = consts.first().map(|c| c.as_u64()).unwrap_or(0);
            let hi = consts.get(1).map(|c| c.as_u64()).unwrap_or(u64::MAX);
            if lo > hi {
                Err(format!("lo ({lo}) must be <= hi ({hi})"))
            } else { Ok(()) }
        }
        "is_one_of" => {
            if consts.is_empty() {
                Err("at least one allowed value required".into())
            } else { Ok(()) }
        }
        _ => Ok(()),
    }
}

crate::register_nodes!(signatures, build_node, validate_node);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn required_passes_defined_value() {
        let n = Required::new("x".to_string());
        let mut out = [Value::None];
        n.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    #[should_panic(expected = "required(x): value was not defined")]
    fn required_panics_on_none() {
        let n = Required::new("x".to_string());
        let mut out = [Value::None];
        n.eval(&[Value::None], &mut out);
    }

    #[test]
    fn this_or_prefers_primary_when_defined() {
        let n = ThisOr::new();
        let mut out = [Value::None];
        n.eval(&[Value::U64(7), Value::U64(99)], &mut out);
        assert_eq!(out[0].as_u64(), 7);
    }

    #[test]
    fn this_or_falls_back_to_default_on_none() {
        let n = ThisOr::new();
        let mut out = [Value::None];
        n.eval(&[Value::None, Value::U64(99)], &mut out);
        assert_eq!(out[0].as_u64(), 99);
    }

    #[test]
    fn is_positive_passes_positive() {
        let n = IsPositive::new("rate".to_string());
        let mut out = [Value::None];
        n.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), 1);
    }

    #[test]
    #[should_panic(expected = "is_positive(rate)")]
    fn is_positive_panics_on_zero() {
        let n = IsPositive::new("rate".to_string());
        let mut out = [Value::None];
        n.eval(&[Value::U64(0)], &mut out);
    }

    #[test]
    fn in_range_passes_interior() {
        let n = InRange::new(10, 100);
        let mut out = [Value::None];
        n.eval(&[Value::U64(50)], &mut out);
        assert_eq!(out[0].as_u64(), 50);
        n.eval(&[Value::U64(10)], &mut out);
        assert_eq!(out[0].as_u64(), 10);
        n.eval(&[Value::U64(100)], &mut out);
        assert_eq!(out[0].as_u64(), 100);
    }

    #[test]
    #[should_panic(expected = "outside [10, 100]")]
    fn in_range_panics_below() {
        let n = InRange::new(10, 100);
        let mut out = [Value::None];
        n.eval(&[Value::U64(5)], &mut out);
    }

    #[test]
    #[should_panic(expected = "outside [10, 100]")]
    fn in_range_panics_above() {
        let n = InRange::new(10, 100);
        let mut out = [Value::None];
        n.eval(&[Value::U64(101)], &mut out);
    }

    #[test]
    fn is_one_of_passes_allowed() {
        let n = IsOneOf::new(vec![1, 2, 3, 5, 8]);
        let mut out = [Value::None];
        n.eval(&[Value::U64(5)], &mut out);
        assert_eq!(out[0].as_u64(), 5);
    }

    #[test]
    #[should_panic(expected = "not in allowed set")]
    fn is_one_of_panics_on_disallowed() {
        let n = IsOneOf::new(vec![1, 2, 3]);
        let mut out = [Value::None];
        n.eval(&[Value::U64(4)], &mut out);
    }

    #[test]
    fn matches_passes_matching_string() {
        let n = Matches::new(r"^\w+@\w+\.\w+$".to_string());
        let mut out = [Value::None];
        n.eval(&[Value::Str("jshook@example.com".into())], &mut out);
        assert_eq!(out[0].as_str(), "jshook@example.com");
    }

    #[test]
    #[should_panic(expected = "does not match pattern")]
    fn matches_panics_on_mismatch() {
        let n = Matches::new(r"^\d+$".to_string());
        let mut out = [Value::None];
        n.eval(&[Value::Str("abc".into())], &mut out);
    }
}
