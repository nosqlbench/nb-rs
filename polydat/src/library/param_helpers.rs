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

use crate::ast::{
    CompiledU64Op, PolydatNode, NodeMeta, Port, Slot, Value,
};

// =========================================================================
// required(input) — assert non-None, pass through
// =========================================================================

/// Assert that an input is defined (i.e. not `Value::None`).
///
/// Signature: `required(input: u64) -> u64`
///
/// Typically applied to a workload parameter read: the compiler
/// resolves the parameter, the value flows into `required`, and
/// the node errors immediately if the parameter was not supplied.
pub struct RequiredU64 {
    meta: NodeMeta,
    /// Name of the value being required, used in the error message
    /// so the operator can see which parameter was undefined.
    name: String,
}

impl RequiredU64 {
    pub fn new(name: impl Into<String>) -> Self {
        let name: String = name.into();
        Self {
            meta: NodeMeta {
                name: "required".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_str("name", name.clone()),
                ],
            },
            name,
        }
    }
}

impl PolydatNode for RequiredU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        if matches!(inputs[0], Value::None) {
            panic!("required({}): value was not defined", self.name);
        }
        outputs[0] = inputs[0].clone();
    }
    /// `required` exists to ASSERT that None is unacceptable —
    /// receiving None is the whole point of its contract. Opt
    /// out of kernel-level Rule 1 propagation so the assertion
    /// fires.
    fn accepts_none_inputs(&self) -> bool { true }
}

// =========================================================================
// this_or(primary, default) — first if defined else second
// =========================================================================

/// Return `primary` if it is defined, otherwise `default`.
///
/// Signature: `this_or(primary: u64, default: u64) -> u64`
///
/// Lets a workload express "use this value if it was supplied,
/// otherwise fall back to that one" without branching logic in
/// the YAML layer. `Value::None` on the primary wire is the
/// "undefined" sentinel.
pub struct ThisOrU64 {
    meta: NodeMeta,
}

impl Default for ThisOrU64 {
    fn default() -> Self { Self::new() }
}

impl ThisOrU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "this_or".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("primary")),
                    Slot::Wire(Port::u64("default")),
                ],
            },
        }
    }
}

impl PolydatNode for ThisOrU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = if matches!(inputs[0], Value::None) {
            inputs[1].clone()
        } else {
            inputs[0].clone()
        };
    }
    /// `this_or` is the typed coalesce equivalent of
    /// `default_or` — consumes None as part of its fallback
    /// semantics. Opt out of Rule 1 propagation.
    fn accepts_none_inputs(&self) -> bool { true }
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
///
/// Signature: `is_one_of(input: u64, allowed...: u64) -> u64`
///
/// Named `is_one_of` rather than `one_of` to avoid collision with
/// the probabilistic `one_of` node (uniform selection from a
/// list) — this is a predicate, not a selector.
pub struct IsOneOfU64 {
    meta: NodeMeta,
    allowed: Vec<u64>,
}

impl IsOneOfU64 {
    pub fn new(allowed: Vec<u64>) -> Self {
        assert!(!allowed.is_empty(), "is_one_of: allowed set must be non-empty");
        let mut ins = vec![Slot::Wire(Port::u64("input"))];
        for (idx, v) in allowed.iter().enumerate() {
            ins.push(Slot::const_u64(format!("allowed_{idx}"), *v));
        }
        Self {
            meta: NodeMeta {
                name: "is_one_of".into(),
                outs: vec![Port::u64("output")],
                ins,
            },
            allowed,
        }
    }
}

impl PolydatNode for IsOneOfU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        if !self.allowed.contains(&v) {
            panic!(
                "is_one_of: value {v} not in allowed set {:?}",
                self.allowed,
            );
        }
        outputs[0] = Value::U64(v);
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let allowed = self.allowed.clone();
        Some(Box::new(move |inputs, outputs| {
            let v = inputs[0];
            if !allowed.contains(&v) {
                panic!("is_one_of: value {v} not in allowed set {allowed:?}");
            }
            outputs[0] = v;
        }))
    }
    fn jit_constants(&self) -> Vec<u64> { self.allowed.clone() }
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

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::ast::SlotType;

pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "required", category: C::Arithmetic, outputs: 1,
            description: "assert a value is defined; pass through",
            help: "Fails at the earliest evaluation if the input resolves to\nValue::None (undefined). Useful on workload parameters that must\nbe supplied at launch — a missing param surfaces as a clear error.\nParameters:\n  input — wire whose value must be defined\n  name  — identifier used in the error message\nExample: required({param:dataset}, \"dataset\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"dataset\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "this_or", category: C::Arithmetic, outputs: 1,
            description: "return primary if defined, else default",
            help: "Returns the primary input if it is defined (i.e. not Value::None),\notherwise returns the default. Use to layer a value explicitly:\n  concurrency := this_or({param:concurrency}, 100)\nParameters:\n  primary — preferred value; may be undefined\n  default — fallback value",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "primary", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "default", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        // `is_positive` / `in_range` migrated to `#[polydat_node]` per SRD-80 PR B.15.
        FuncSig {
            name: "is_one_of", category: C::Arithmetic, outputs: 1,
            description: "assert value in allow-list; pass through",
            help: "Predicate that fails if the input is not one of the allowed\nvalues. Variadic over the allow-list constants.\nParameters:\n  input      — u64 wire\n  allowed... — one or more u64 constants",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "allowed", slot_type: SlotType::ConstU64, required: true, example: "1", constraint: None },
            ],
            arity: Arity::VariadicConsts { min_consts: 1 },
            commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        // `matches` migrated to `#[polydat_node]` per SRD-80 PR B.6.
    ]
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType],
    consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    match name {
        "required" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(RequiredU64::new(n))))
        }
        "this_or" => Some(Ok(Box::new(ThisOrU64::new()))),
        // `is_positive` / `in_range` route via proc-macro NodeRegistration per SRD-80 PR B.15.
        "is_one_of" => {
            let allowed: Vec<u64> = consts.iter().map(|c| c.as_u64()).collect();
            if allowed.is_empty() {
                return Some(Err("is_one_of: at least one allowed value required".into()));
            }
            Some(Ok(Box::new(IsOneOfU64::new(allowed))))
        }
        // `matches` routes via proc-macro-emitted NodeRegistration per SRD-80 PR B.6.
        _ => None,
    }
}

/// Assembly-time constant validation for parameter-helper nodes.
/// See SRD 15 §"Const Constraint Metadata".
///
/// `matches.pattern` rides on `StrParser` in its `ParamSpec`; the
/// remaining rules — `in_range` (relational `lo ≤ hi`) and
/// `is_one_of` (variadic emptiness) — can't be expressed per-param.
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

// `validate_regex_pattern` retired with the `matches` migration
// (SRD-80 PR B.6) — const-constraint registration on macro-emitted
// nodes is forthcoming in a follow-on PR.

crate::register_nodes!(signatures, build_node, validate_node);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_passes_defined_value() {
        let n = RequiredU64::new("x");
        let mut out = [Value::None];
        n.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    #[should_panic(expected = "required(x): value was not defined")]
    fn required_panics_on_none() {
        let n = RequiredU64::new("x");
        let mut out = [Value::None];
        n.eval(&[Value::None], &mut out);
    }

    #[test]
    fn this_or_prefers_primary_when_defined() {
        let n = ThisOrU64::new();
        let mut out = [Value::None];
        n.eval(&[Value::U64(7), Value::U64(99)], &mut out);
        assert_eq!(out[0].as_u64(), 7);
    }

    #[test]
    fn this_or_falls_back_to_default_on_none() {
        let n = ThisOrU64::new();
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
        let n = IsOneOfU64::new(vec![1, 2, 3, 5, 8]);
        let mut out = [Value::None];
        n.eval(&[Value::U64(5)], &mut out);
        assert_eq!(out[0].as_u64(), 5);
    }

    #[test]
    #[should_panic(expected = "not in allowed set")]
    fn is_one_of_panics_on_disallowed() {
        let n = IsOneOfU64::new(vec![1, 2, 3]);
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
