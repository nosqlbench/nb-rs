// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Assembly-time validation of GK node constant arguments.
//!
//! GK's contract (SRD 15 §"Input Validity Model") keeps the hot
//! path branch-free by letting node `::new` trust its constants —
//! no runtime checks. That only holds if the *factory* has already
//! proven each constant satisfies the node's contract, rejecting
//! violations with a structured compile error *before* the node
//! is constructed.
//!
//! This module provides the vocabulary for those checks:
//!
//! * [`ConstConstraint`] describes a single constraint on one
//!   constant argument. Apply it with
//!   [`ConstConstraint::check`].
//! * [`NodeValidator`] is the per-module function the factory
//!   calls before `build`. It gets the function name and the
//!   resolved constant args and returns `Ok(())` or a structured
//!   error string.
//!
//! A module opts in by passing a validator as the third argument
//! to `register_nodes!`. Modules that don't need validation omit
//! it and the factory skips the check.

use crate::dsl::factory::ConstArg;

/// A declarative constraint on one constant argument of a node
/// call.
///
/// Attached to a `ParamSpec` via the optional `constraint` field;
/// the factory walks `FuncSig.params` and enforces every declared
/// constraint before `build_node` constructs the node. All variants
/// are `Copy` so `ParamSpec` (and the static `FuncSig` arrays that
/// embed it) stay `Copy`.
#[derive(Debug, Clone, Copy)]
pub enum ConstConstraint {
    /// Integer must satisfy `min ≤ v ≤ max`.
    RangeU64 { min: u64, max: u64 },
    /// Float must satisfy `min ≤ v ≤ max`.
    RangeF64 { min: f64, max: f64 },
    /// Integer must appear in a closed set (e.g. radix ∈ {2, 8, 10, 16}).
    AllowedU64(&'static [u64]),
    /// Integer must be non-zero (divisors, moduli, ranges).
    NonZeroU64,
    /// String must have non-empty length after trim.
    NonEmptyStr,
    /// Arbitrary string format predicate. Return `Err(msg)` to
    /// reject the constant; the caller prepends parameter context.
    /// Use for structured specs like `"v1:w1;v2:w2"` where a fixed
    /// enum variant can't express the format.
    StrParser(fn(&str) -> Result<(), String>),
    /// Float must be finite and strictly positive. Distinct from
    /// `RangeF64` because the natural upper bound is `+∞` and
    /// `RangeF64` requires a finite max.
    PositiveFiniteF64,
    /// Float must be finite (`!is_nan() && !is_infinite()`).
    /// Endpoint and offset constants where ±∞/NaN would silently
    /// produce nonsense outputs downstream.
    FiniteF64,
}

impl ConstConstraint {
    /// Apply this constraint to `arg`. On violation, the returned
    /// error message is prefixed with `param_name` so the caller
    /// can surface it directly to the user.
    pub fn check(&self, arg: &ConstArg, param_name: &str) -> Result<(), String> {
        match self {
            ConstConstraint::RangeU64 { min, max } => {
                let v = arg.as_u64();
                if v < *min || v > *max {
                    Err(format!("{param_name} must be in [{min}, {max}], got {v}"))
                } else { Ok(()) }
            }
            ConstConstraint::RangeF64 { min, max } => {
                let v = arg.as_f64();
                if !(*min..=*max).contains(&v) {
                    Err(format!("{param_name} must be in [{min}, {max}], got {v}"))
                } else { Ok(()) }
            }
            ConstConstraint::AllowedU64(allowed) => {
                let v = arg.as_u64();
                if !allowed.contains(&v) {
                    Err(format!("{param_name} must be one of {allowed:?}, got {v}"))
                } else { Ok(()) }
            }
            ConstConstraint::NonZeroU64 => {
                let v = arg.as_u64();
                if v == 0 {
                    Err(format!("{param_name} must be non-zero"))
                } else { Ok(()) }
            }
            ConstConstraint::NonEmptyStr => {
                let s = arg.as_str();
                if s.trim().is_empty() {
                    Err(format!("{param_name} must be non-empty"))
                } else { Ok(()) }
            }
            ConstConstraint::StrParser(f) => {
                let s = arg.as_str();
                f(s).map_err(|e| format!("{param_name}: {e}"))
            }
            ConstConstraint::PositiveFiniteF64 => {
                let v = arg.as_f64();
                if !v.is_finite() || v <= 0.0 {
                    Err(format!("{param_name} must be a positive finite f64, got {v}"))
                } else { Ok(()) }
            }
            ConstConstraint::FiniteF64 => {
                let v = arg.as_f64();
                if !v.is_finite() {
                    Err(format!("{param_name} must be a finite f64, got {v}"))
                } else { Ok(()) }
            }
        }
    }
}

/// Per-module validator the factory calls before `build_node`.
///
/// Receives the function name (same key `build_node` dispatches
/// on) and the resolved constant arguments in positional order.
/// Returns `Ok(())` if the constants satisfy every declared
/// constraint, or a structured error on violation.
///
/// The error string is prefixed with `bad constant <func>: ` by
/// the factory, so validators can return terse messages like
/// `"radix must be one of [2,8,10,16], got 42"`.
pub type NodeValidator = fn(name: &str, consts: &[ConstArg]) -> Result<(), String>;

/// Convenience: apply a single constraint to an optional positional
/// argument. Absence is treated as "no value to check" (Ok) — the
/// `required` flag on `ParamSpec` already handles mandatory-ness.
pub fn check_opt(
    constraint: &ConstConstraint,
    consts: &[ConstArg],
    index: usize,
    param_name: &str,
) -> Result<(), String> {
    match consts.get(index) {
        Some(arg) => constraint.check(arg, param_name),
        None => Ok(()),
    }
}
