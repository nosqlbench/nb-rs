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

use crate::node::{
    CompiledU64Op, GkNode, NodeMeta, Port, PortType, Slot, Value,
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

impl GkNode for RequiredU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        if matches!(inputs[0], Value::None) {
            panic!("required({}): value was not defined", self.name);
        }
        outputs[0] = inputs[0].clone();
    }
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

impl GkNode for ThisOrU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = if matches!(inputs[0], Value::None) {
            inputs[1].clone()
        } else {
            inputs[0].clone()
        };
    }
}

// =========================================================================
// is_positive(input) — assert > 0, pass through
// =========================================================================

/// Assert that a u64 value is strictly positive (> 0).
///
/// Signature: `is_positive(input: u64) -> u64`
pub struct IsPositiveU64 {
    meta: NodeMeta,
    name: String,
}

impl IsPositiveU64 {
    pub fn new(name: impl Into<String>) -> Self {
        let name: String = name.into();
        Self {
            meta: NodeMeta {
                name: "is_positive".into(),
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

impl GkNode for IsPositiveU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        if v == 0 {
            panic!("is_positive({}): value must be > 0, got {v}", self.name);
        }
        outputs[0] = Value::U64(v);
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let name = self.name.clone();
        Some(Box::new(move |inputs, outputs| {
            let v = inputs[0];
            if v == 0 {
                panic!("is_positive({name}): value must be > 0, got 0");
            }
            outputs[0] = v;
        }))
    }
}

// =========================================================================
// in_range(input, lo, hi) — assert lo ≤ input ≤ hi, pass through
// =========================================================================

/// Assert that a u64 value is in the inclusive range `[lo, hi]`.
///
/// Signature: `in_range(input: u64, lo: u64, hi: u64) -> u64`
pub struct InRangeU64 {
    meta: NodeMeta,
    lo: u64,
    hi: u64,
}

impl InRangeU64 {
    pub fn new(lo: u64, hi: u64) -> Self {
        assert!(lo <= hi, "in_range: lo ({lo}) must be <= hi ({hi})");
        Self {
            meta: NodeMeta {
                name: "in_range".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("lo", lo),
                    Slot::const_u64("hi", hi),
                ],
            },
            lo,
            hi,
        }
    }
}

impl GkNode for InRangeU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        if v < self.lo || v > self.hi {
            panic!(
                "in_range: value {v} outside [{}, {}]",
                self.lo, self.hi,
            );
        }
        outputs[0] = Value::U64(v);
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let lo = self.lo;
        let hi = self.hi;
        Some(Box::new(move |inputs, outputs| {
            let v = inputs[0];
            if v < lo || v > hi {
                panic!("in_range: value {v} outside [{lo}, {hi}]");
            }
            outputs[0] = v;
        }))
    }
    fn jit_constants(&self) -> Vec<u64> { vec![self.lo, self.hi] }
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

impl GkNode for IsOneOfU64 {
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

/// Assert that a string value matches a regex pattern.
///
/// Signature: `matches(input: String, pattern: String) -> String`
pub struct MatchesStr {
    meta: NodeMeta,
    re: Regex,
    pattern: String,
}

impl MatchesStr {
    pub fn new(pattern: &str) -> Self {
        let re = Regex::new(pattern)
            .unwrap_or_else(|e| panic!("matches: invalid regex {pattern:?}: {e}"));
        Self {
            meta: NodeMeta {
                name: "matches".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![
                    Slot::Wire(Port::new("input", PortType::Str)),
                    Slot::const_str("pattern", pattern),
                ],
            },
            re,
            pattern: pattern.to_string(),
        }
    }
}

impl GkNode for MatchesStr {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        if !self.re.is_match(s) {
            panic!(
                "matches: value {s:?} does not match pattern {:?}",
                self.pattern,
            );
        }
        outputs[0] = Value::Str(s.to_string());
    }
}

// =========================================================================
// Registration
// =========================================================================

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "required", category: C::Arithmetic, outputs: 1,
            description: "assert a value is defined; pass through",
            help: "Fails at the earliest evaluation if the input resolves to\nValue::None (undefined). Useful on workload parameters that must\nbe supplied at launch — a missing param surfaces as a clear error.\nParameters:\n  input — wire whose value must be defined\n  name  — identifier used in the error message\nExample: required({param:dataset}, \"dataset\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"dataset\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "this_or", category: C::Arithmetic, outputs: 1,
            description: "return primary if defined, else default",
            help: "Returns the primary input if it is defined (i.e. not Value::None),\notherwise returns the default. Use to layer a value explicitly:\n  concurrency := this_or({param:concurrency}, 100)\nParameters:\n  primary — preferred value; may be undefined\n  default — fallback value",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "primary", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "default", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "is_positive", category: C::Arithmetic, outputs: 1,
            description: "assert value > 0; pass through",
            help: "Predicate that fails if the input is zero. Use on workload\nparams like concurrency or rate where 0 is nonsensical.\nParameters:\n  input — u64 wire\n  name  — identifier used in the error message",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"rate\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "in_range", category: C::Arithmetic, outputs: 1,
            description: "assert value in [lo, hi]; pass through",
            help: "Predicate that fails if the input is outside [lo, hi].\nUse for bounds on tunable parameters (timeouts, concurrency caps).\nParameters:\n  input — u64 wire\n  lo    — lower bound (inclusive)\n  hi    — upper bound (inclusive)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "lo", slot_type: SlotType::ConstU64, required: true, example: "1" },
                ParamSpec { name: "hi", slot_type: SlotType::ConstU64, required: true, example: "100" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "is_one_of", category: C::Arithmetic, outputs: 1,
            description: "assert value in allow-list; pass through",
            help: "Predicate that fails if the input is not one of the allowed\nvalues. Variadic over the allow-list constants.\nParameters:\n  input      — u64 wire\n  allowed... — one or more u64 constants",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "allowed", slot_type: SlotType::ConstU64, required: true, example: "1" },
            ],
            arity: Arity::VariadicConsts { min_consts: 1 },
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "matches", category: C::Arithmetic, outputs: 1,
            description: "assert string matches regex; pass through",
            help: "Predicate that fails if the input string does not match the\nregex pattern. Compiled once at construction; failed compile\n(invalid regex) is a hard error at node construction.\nParameters:\n  input   — string wire\n  pattern — regex pattern (compiled at init)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "pattern", slot_type: SlotType::ConstStr, required: true, example: "\"^[a-z]+$\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef],
    consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "required" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(RequiredU64::new(n))))
        }
        "this_or" => Some(Ok(Box::new(ThisOrU64::new()))),
        "is_positive" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(IsPositiveU64::new(n))))
        }
        "in_range" => {
            let lo = consts.first().map(|c| c.as_u64()).unwrap_or(0);
            let hi = consts.get(1).map(|c| c.as_u64()).unwrap_or(u64::MAX);
            Some(Ok(Box::new(InRangeU64::new(lo, hi))))
        }
        "is_one_of" => {
            let allowed: Vec<u64> = consts.iter().map(|c| c.as_u64()).collect();
            if allowed.is_empty() {
                return Some(Err("is_one_of: at least one allowed value required".into()));
            }
            Some(Ok(Box::new(IsOneOfU64::new(allowed))))
        }
        "matches" => {
            let pat = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(MatchesStr::new(&pat))))
        }
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

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
        let n = IsPositiveU64::new("rate");
        let mut out = [Value::None];
        n.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), 1);
    }

    #[test]
    #[should_panic(expected = "is_positive(rate)")]
    fn is_positive_panics_on_zero() {
        let n = IsPositiveU64::new("rate");
        let mut out = [Value::None];
        n.eval(&[Value::U64(0)], &mut out);
    }

    #[test]
    fn in_range_passes_interior() {
        let n = InRangeU64::new(10, 100);
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
        let n = InRangeU64::new(10, 100);
        let mut out = [Value::None];
        n.eval(&[Value::U64(5)], &mut out);
    }

    #[test]
    #[should_panic(expected = "outside [10, 100]")]
    fn in_range_panics_above() {
        let n = InRangeU64::new(10, 100);
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
        let n = MatchesStr::new(r"^\w+@\w+\.\w+$");
        let mut out = [Value::None];
        n.eval(&[Value::Str("jshook@example.com".into())], &mut out);
        assert_eq!(out[0].as_str(), "jshook@example.com");
    }

    #[test]
    #[should_panic(expected = "does not match pattern")]
    fn matches_panics_on_mismatch() {
        let n = MatchesStr::new(r"^\d+$");
        let mut out = [Value::None];
        n.eval(&[Value::Str("abc".into())], &mut out);
    }
}
