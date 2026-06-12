// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Pattern recognizer catalog — spec §10.9.5.
//!
//! Each recognizer matches one syntactic shape against a
//! predicate string and produces partial [`PredicateInfo`]
//! data. The analyzer ([`super::analyzer::analyze`])
//! composes the recognizers to build the full info.
//!
//! ## Initial catalog (spec §10.9.5)
//!
//! - `{a} OP K` for OP ∈ {==, !=, <, <=, >, >=}
//! - `{a} OP {b}` (cross-axis)
//! - `p1 && p2` (recursive)
//! - `p1 || p2` (recursive)
//! - `!p`
//! - `K1 <= {a} && {a} <= K2` (range fold)
//! - `{a} in [K1, K2, K3]` (discrete-set membership)
//!
//! Patterns NOT in this catalog return `Opaque(UnknownPattern)`.
//! Per spec §10.9.4 property 2 ("Conservatively incomplete"):
//! missing an optimization is acceptable; asserting a false
//! property is not.

use super::coordset::{CoordKind, CoordSet};
use super::info::{
    ConstValue, Determinism, Factorization, Monotonicity, OpaqueReason, PerAxisMap,
    PredicateInfo, RangeConstraint,
};

/// Extract `{name}` interpolation references from a predicate
/// string. Mirrors the helper in `validate.rs`'s V3 check.
pub fn extract_coord_refs(predicate: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = predicate.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{'
            && let Some(close) = predicate[i + 1..].find('}') {
                let name = predicate[i + 1..i + 1 + close].trim();
                if !name.is_empty()
                    && name.chars().all(|c| c.is_alphanumeric() || c == '_')
                    && !out.contains(&name.to_string())
                {
                    out.push(name.to_string());
                }
                i += close + 2;
                continue;
            }
        i += 1;
    }
    out
}

/// Top-level entry — try recognizers in priority order.
/// Returns the most specific match.
pub fn recognize(predicate: &str, coords: &CoordSet) -> PredicateInfo {
    let coord_refs = extract_coord_refs(predicate);

    // Continuous-coord short-circuit. Any reference to a
    // continuous-classified coord makes the whole predicate
    // Opaque(Continuous) per spec §10.9 + F20.
    for r in &coord_refs {
        if matches!(coords.get(r).map(|c| c.kind), Some(CoordKind::Continuous)) {
            return PredicateInfo {
                factorization: Factorization::Opaque(OpaqueReason::Continuous),
                monotonicity: PerAxisMap::new(),
                range_constraint: PerAxisMap::new(),
                determinism: Determinism::Deterministic,
                coords_referenced: coord_refs,
            };
        }
    }

    // Try recognizers in priority order:
    //   1. Range-fold (`K1 OP {a} OP K2`) — most specific.
    //   2. Discrete-set (`{a} in [...]`).
    //   3. Negation (`!p`).
    //   4. Conjunction (`p1 && p2`).
    //   5. Disjunction (`p1 || p2`).
    //   6. Per-axis comparison (`{a} OP K`).
    //   7. Cross-axis comparison (`{a} OP {b}`).
    //   8. Trivially-true / trivially-false.

    let trimmed = predicate.trim();

    // 8. Trivially-true / -false. Folded by R0a; we still
    //    report so callers see the shape.
    if trimmed.eq_ignore_ascii_case("true") {
        return PredicateInfo {
            factorization: Factorization::PerAxis(PerAxisMap::new()),
            monotonicity: PerAxisMap::new(),
            range_constraint: PerAxisMap::new(),
            determinism: Determinism::Deterministic,
            coords_referenced: coord_refs,
        };
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return PredicateInfo {
            factorization: Factorization::Conjunctive(vec!["false".to_string()]),
            monotonicity: PerAxisMap::new(),
            range_constraint: PerAxisMap::new(),
            determinism: Determinism::Deterministic,
            coords_referenced: coord_refs,
        };
    }

    // 4. Conjunction — split on top-level `&&`.
    if let Some(parts) = split_top_level(trimmed, "&&") {
        return recognize_conjunction(&parts, coords, coord_refs);
    }

    // 5. Disjunction — split on top-level `||`.
    if let Some(parts) = split_top_level(trimmed, "||") {
        return recognize_disjunction(&parts, coords, coord_refs);
    }

    // 3. Negation — leading `!`.
    if let Some(inner) = trimmed.strip_prefix('!') {
        let inner_info = recognize(inner.trim(), coords);
        return invert_predicate(&inner_info, coord_refs);
    }

    // 2. Discrete-set — `{a} in [K1, K2, …]`.
    if let Some(info) = recognize_discrete_set(trimmed, coords, &coord_refs) {
        return info;
    }

    // 1. Range-fold — `K1 OP {a} OP K2` (already in conjunction
    //    branch if user wrote `K1 <= {a} && {a} <= K2`).

    // 6. Per-axis comparison `{a} OP K`.
    if let Some(info) = recognize_per_axis_comparison(trimmed, coords, &coord_refs) {
        return info;
    }

    // 7. Cross-axis comparison `{a} OP {b}`.
    if let Some(info) = recognize_cross_axis_comparison(trimmed, coords, &coord_refs) {
        return info;
    }

    // Fallback — unknown pattern.
    PredicateInfo {
        factorization: Factorization::Opaque(OpaqueReason::UnknownPattern),
        monotonicity: PerAxisMap::new(),
        range_constraint: PerAxisMap::new(),
        determinism: classify_determinism(trimmed),
        coords_referenced: coord_refs,
    }
}

/// Detect non-deterministic constructs in the predicate text.
/// Conservative: any reference to a function name we
/// recognize as non-deterministic (PRNG, time, etc.) marks
/// `Determinism::Opaque`.
fn classify_determinism(predicate: &str) -> Determinism {
    const NONDET_FUNCTIONS: &[&str] = &[
        "random", "rand", "pcg(", "pcg_stream(", "now(", "time(",
        "uuid(", "thread_id(", "wall_clock(",
    ];
    let lower = predicate.to_lowercase();
    for fn_name in NONDET_FUNCTIONS {
        if lower.contains(fn_name) {
            return Determinism::Opaque;
        }
    }
    Determinism::Deterministic
}

// ---- per-axis comparison ----

const COMPARISON_OPS: &[(&str, ComparisonKind)] = &[
    ("==", ComparisonKind::Eq),
    ("!=", ComparisonKind::Ne),
    ("<=", ComparisonKind::Le),
    (">=", ComparisonKind::Ge),
    // Strict variants AFTER non-strict so the longest match wins.
    ("<", ComparisonKind::Lt),
    (">", ComparisonKind::Gt),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComparisonKind {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

fn recognize_per_axis_comparison(
    predicate: &str,
    coords: &CoordSet,
    coord_refs: &[String],
) -> Option<PredicateInfo> {
    // Looks for `{name} OP literal` or `literal OP {name}`.
    let trimmed = predicate.trim();
    for (op_str, op_kind) in COMPARISON_OPS {
        if let Some((lhs, rhs)) = split_top_level_op(trimmed, op_str) {
            let lhs = lhs.trim();
            let rhs = rhs.trim();
            // Case A: `{name} OP literal`
            if let Some(name) = strip_curly(lhs)
                && let Some(value) = parse_literal(rhs)
                && coords.contains(&name)
            {
                return Some(per_axis_info(&name, *op_kind, value, coord_refs));
            }
            // Case B: `literal OP {name}` — invert op direction.
            if let Some(name) = strip_curly(rhs)
                && let Some(value) = parse_literal(lhs)
                && coords.contains(&name)
            {
                let inv = invert_op_position(*op_kind);
                return Some(per_axis_info(&name, inv, value, coord_refs));
            }
        }
    }
    None
}

fn per_axis_info(
    axis: &str,
    op: ComparisonKind,
    rhs: ConstValue,
    coord_refs: &[String],
) -> PredicateInfo {
    let mut factor = PerAxisMap::new();
    factor.insert(axis, format!("{{{axis}}} {} {}", op_str(op), const_repr(&rhs)));

    let mut mono = PerAxisMap::new();
    let direction = match op {
        ComparisonKind::Lt | ComparisonKind::Le => Monotonicity::Decreasing,
        ComparisonKind::Gt | ComparisonKind::Ge => Monotonicity::Increasing,
        ComparisonKind::Eq | ComparisonKind::Ne => Monotonicity::None,
    };
    if !matches!(direction, Monotonicity::None) {
        mono.insert(axis, direction);
    }

    let mut range = PerAxisMap::new();
    let constraint = match op {
        ComparisonKind::Eq => RangeConstraint::Discrete(vec![rhs.clone()]),
        ComparisonKind::Ne => RangeConstraint::None,
        ComparisonKind::Lt => RangeConstraint::Bounded {
            lo: None,
            hi: Some(rhs.clone()),
            lo_inclusive: false,
            hi_inclusive: false,
        },
        ComparisonKind::Le => RangeConstraint::Bounded {
            lo: None,
            hi: Some(rhs.clone()),
            lo_inclusive: false,
            hi_inclusive: true,
        },
        ComparisonKind::Gt => RangeConstraint::Bounded {
            lo: Some(rhs.clone()),
            hi: None,
            lo_inclusive: false,
            hi_inclusive: false,
        },
        ComparisonKind::Ge => RangeConstraint::Bounded {
            lo: Some(rhs.clone()),
            hi: None,
            lo_inclusive: true,
            hi_inclusive: false,
        },
    };
    range.insert(axis, constraint);

    PredicateInfo {
        factorization: Factorization::PerAxis(factor),
        monotonicity: mono,
        range_constraint: range,
        determinism: Determinism::Deterministic,
        coords_referenced: coord_refs.to_vec(),
    }
}

fn invert_op_position(op: ComparisonKind) -> ComparisonKind {
    match op {
        ComparisonKind::Lt => ComparisonKind::Gt,
        ComparisonKind::Le => ComparisonKind::Ge,
        ComparisonKind::Gt => ComparisonKind::Lt,
        ComparisonKind::Ge => ComparisonKind::Le,
        ComparisonKind::Eq => ComparisonKind::Eq,
        ComparisonKind::Ne => ComparisonKind::Ne,
    }
}

fn op_str(op: ComparisonKind) -> &'static str {
    match op {
        ComparisonKind::Eq => "==",
        ComparisonKind::Ne => "!=",
        ComparisonKind::Lt => "<",
        ComparisonKind::Le => "<=",
        ComparisonKind::Gt => ">",
        ComparisonKind::Ge => ">=",
    }
}

fn const_repr(v: &ConstValue) -> String {
    match v {
        ConstValue::Int(n) => n.to_string(),
        ConstValue::Float(f) => f.to_string(),
        ConstValue::String(s) => format!("\"{s}\""),
        ConstValue::Bool(b) => b.to_string(),
    }
}

// ---- cross-axis comparison ----

fn recognize_cross_axis_comparison(
    predicate: &str,
    coords: &CoordSet,
    coord_refs: &[String],
) -> Option<PredicateInfo> {
    for (op_str, _) in COMPARISON_OPS {
        if let Some((lhs, rhs)) = split_top_level_op(predicate.trim(), op_str)
            && let (Some(a), Some(b)) = (strip_curly(lhs.trim()), strip_curly(rhs.trim()))
                && coords.contains(&a)
                && coords.contains(&b)
                && a != b
            {
                return Some(PredicateInfo {
                    factorization: Factorization::Conjunctive(vec![predicate.trim().to_string()]),
                    monotonicity: PerAxisMap::new(),
                    range_constraint: PerAxisMap::new(),
                    determinism: Determinism::Deterministic,
                    coords_referenced: coord_refs.to_vec(),
                });
            }
    }
    None
}

// ---- conjunction ----

fn recognize_conjunction(
    parts: &[String],
    coords: &CoordSet,
    coord_refs: Vec<String>,
) -> PredicateInfo {
    let sub_infos: Vec<PredicateInfo> = parts.iter().map(|p| recognize(p, coords)).collect();

    // If every sub-info is PerAxis with disjoint axes, the
    // conjunction is PerAxis.
    let mut merged_factor = PerAxisMap::<String>::new();
    let mut all_per_axis = true;
    for info in &sub_infos {
        match &info.factorization {
            Factorization::PerAxis(m) => {
                for (axis, expr) in m.iter() {
                    if merged_factor.get(axis).is_some() {
                        // Two sub-predicates on the same axis —
                        // fold them with `&&`.
                        let existing = merged_factor.get(axis).cloned().unwrap();
                        merged_factor
                            .insert(axis, format!("({existing}) && ({expr})"));
                    } else {
                        merged_factor.insert(axis, expr.to_string());
                    }
                }
            }
            _ => {
                all_per_axis = false;
                break;
            }
        }
    }

    // Merge per-axis monotonicity + range. For PerAxis merges,
    // intersection rules apply: monotonicity must agree;
    // range_constraint Bounded variants intersect.
    let mut merged_mono = PerAxisMap::<Monotonicity>::new();
    let mut merged_range = PerAxisMap::<RangeConstraint>::new();
    for info in &sub_infos {
        for (axis, dir) in info.monotonicity.iter() {
            match merged_mono.get(axis).copied() {
                None => merged_mono.insert(axis, *dir),
                Some(existing) if existing == *dir => {}
                _ => {
                    // Conflicting directions — drop the entry.
                    // (Cleanest signal: no asserted monotonicity.)
                }
            }
        }
        for (axis, range) in info.range_constraint.iter() {
            match merged_range.get(axis).cloned() {
                None => merged_range.insert(axis, range.clone()),
                Some(existing) => {
                    let intersected = intersect_ranges(&existing, range);
                    merged_range.insert(axis, intersected);
                }
            }
        }
    }

    let determinism = if sub_infos.iter().all(|i| i.determinism == Determinism::Deterministic) {
        Determinism::Deterministic
    } else {
        Determinism::Opaque
    };

    let factorization = if all_per_axis {
        Factorization::PerAxis(merged_factor)
    } else {
        Factorization::Conjunctive(parts.to_vec())
    };

    PredicateInfo {
        factorization,
        monotonicity: merged_mono,
        range_constraint: merged_range,
        determinism,
        coords_referenced: coord_refs,
    }
}

fn intersect_ranges(a: &RangeConstraint, b: &RangeConstraint) -> RangeConstraint {
    match (a, b) {
        (
            RangeConstraint::Bounded {
                lo: lo_a, hi: hi_a, lo_inclusive: li_a, hi_inclusive: hi_inc_a,
            },
            RangeConstraint::Bounded {
                lo: lo_b, hi: hi_b, lo_inclusive: li_b, hi_inclusive: hi_inc_b,
            },
        ) => {
            // Pick the tighter lo / hi.
            let (lo, lo_inclusive) = pick_lo(lo_a.as_ref(), *li_a, lo_b.as_ref(), *li_b);
            let (hi, hi_inclusive) = pick_hi(hi_a.as_ref(), *hi_inc_a, hi_b.as_ref(), *hi_inc_b);
            RangeConstraint::Bounded {
                lo, hi, lo_inclusive, hi_inclusive,
            }
        }
        (RangeConstraint::Discrete(vs), RangeConstraint::Bounded { .. })
        | (RangeConstraint::Bounded { .. }, RangeConstraint::Discrete(vs)) => {
            // Keep the discrete set; its elements are
            // self-contained and the bounded constraint is
            // implied.
            RangeConstraint::Discrete(vs.clone())
        }
        (RangeConstraint::Discrete(vs_a), RangeConstraint::Discrete(vs_b)) => {
            let intersection: Vec<ConstValue> =
                vs_a.iter().filter(|v| vs_b.contains(v)).cloned().collect();
            RangeConstraint::Discrete(intersection)
        }
        (RangeConstraint::None, other) | (other, RangeConstraint::None) => other.clone(),
    }
}

fn pick_lo(
    a: Option<&ConstValue>,
    a_inc: bool,
    b: Option<&ConstValue>,
    b_inc: bool,
) -> (Option<ConstValue>, bool) {
    match (a, b) {
        (None, None) => (None, false),
        (Some(v), None) => (Some(v.clone()), a_inc),
        (None, Some(v)) => (Some(v.clone()), b_inc),
        (Some(av), Some(bv)) => {
            let cmp = compare_const(av, bv);
            if cmp.is_lt() {
                (Some(bv.clone()), b_inc)
            } else if cmp.is_gt() {
                (Some(av.clone()), a_inc)
            } else {
                // Equal — exclusive wins (tighter).
                (Some(av.clone()), a_inc && b_inc)
            }
        }
    }
}

fn pick_hi(
    a: Option<&ConstValue>,
    a_inc: bool,
    b: Option<&ConstValue>,
    b_inc: bool,
) -> (Option<ConstValue>, bool) {
    match (a, b) {
        (None, None) => (None, false),
        (Some(v), None) => (Some(v.clone()), a_inc),
        (None, Some(v)) => (Some(v.clone()), b_inc),
        (Some(av), Some(bv)) => {
            let cmp = compare_const(av, bv);
            if cmp.is_lt() {
                (Some(av.clone()), a_inc)
            } else if cmp.is_gt() {
                (Some(bv.clone()), b_inc)
            } else {
                (Some(av.clone()), a_inc && b_inc)
            }
        }
    }
}

fn compare_const(a: &ConstValue, b: &ConstValue) -> std::cmp::Ordering {
    match (a, b) {
        (ConstValue::Int(a), ConstValue::Int(b)) => a.cmp(b),
        (ConstValue::Float(a), ConstValue::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (ConstValue::Int(a), ConstValue::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (ConstValue::Float(a), ConstValue::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
        _ => std::cmp::Ordering::Equal,
    }
}

// ---- disjunction ----

fn recognize_disjunction(
    parts: &[String],
    coords: &CoordSet,
    coord_refs: Vec<String>,
) -> PredicateInfo {
    let sub_infos: Vec<PredicateInfo> = parts.iter().map(|p| recognize(p, coords)).collect();

    // Per spec §10.9.5's disjunction rule: Disjunctive only if
    // every disjunct is Conjunctive / PerAxis; otherwise
    // Opaque.
    let all_known = sub_infos.iter().all(|i| {
        matches!(
            i.factorization,
            Factorization::PerAxis(_) | Factorization::Conjunctive(_)
        )
    });
    let factorization = if all_known {
        Factorization::Disjunctive(parts.to_vec())
    } else {
        Factorization::Opaque(OpaqueReason::UnknownPattern)
    };

    let determinism = if sub_infos.iter().all(|i| i.determinism == Determinism::Deterministic) {
        Determinism::Deterministic
    } else {
        Determinism::Opaque
    };

    // Disjunction loses per-axis monotonicity claims (the
    // union of strict-monotone slices isn't monotone). Range
    // constraints take the per-axis union.
    let mut merged_range = PerAxisMap::<RangeConstraint>::new();
    for info in &sub_infos {
        for (axis, range) in info.range_constraint.iter() {
            match merged_range.get(axis).cloned() {
                None => merged_range.insert(axis, range.clone()),
                Some(existing) => merged_range.insert(axis, union_ranges(&existing, range)),
            }
        }
    }

    PredicateInfo {
        factorization,
        monotonicity: PerAxisMap::new(),
        range_constraint: merged_range,
        determinism,
        coords_referenced: coord_refs,
    }
}

fn union_ranges(a: &RangeConstraint, b: &RangeConstraint) -> RangeConstraint {
    match (a, b) {
        (RangeConstraint::Discrete(va), RangeConstraint::Discrete(vb)) => {
            let mut merged = va.clone();
            for v in vb {
                if !merged.contains(v) {
                    merged.push(v.clone());
                }
            }
            RangeConstraint::Discrete(merged)
        }
        // Mixed types: drop the constraint (can't union
        // Bounded with Discrete soundly without more work).
        _ => RangeConstraint::None,
    }
}

// ---- negation ----

fn invert_predicate(inner: &PredicateInfo, coord_refs: Vec<String>) -> PredicateInfo {
    // Inverting a PerAxis predicate inverts each per-axis
    // sub-predicate. Inverting Opaque stays Opaque. Inverting
    // Conjunctive becomes Disjunctive of inverted parts; we
    // don't currently auto-De-Morgan beyond that, so the
    // safe fallback is Opaque.
    let factorization = match &inner.factorization {
        Factorization::PerAxis(m) => {
            let mut inverted = PerAxisMap::<String>::new();
            for (axis, expr) in m.iter() {
                inverted.insert(axis, format!("!({expr})"));
            }
            Factorization::PerAxis(inverted)
        }
        Factorization::Opaque(reason) => Factorization::Opaque(reason.clone()),
        _ => Factorization::Opaque(OpaqueReason::UnknownPattern),
    };

    // Invert monotonicity direction.
    let mut inverted_mono = PerAxisMap::<Monotonicity>::new();
    for (axis, dir) in inner.monotonicity.iter() {
        let new = match dir {
            Monotonicity::Increasing => Monotonicity::Decreasing,
            Monotonicity::Decreasing => Monotonicity::Increasing,
            Monotonicity::None => Monotonicity::None,
        };
        inverted_mono.insert(axis, new);
    }

    // Inverting ranges is non-trivial; drop range claims on
    // negated predicates for now.
    let inverted_range = PerAxisMap::<RangeConstraint>::new();

    PredicateInfo {
        factorization,
        monotonicity: inverted_mono,
        range_constraint: inverted_range,
        determinism: inner.determinism,
        coords_referenced: coord_refs,
    }
}

// ---- discrete-set membership ----

fn recognize_discrete_set(
    predicate: &str,
    coords: &CoordSet,
    coord_refs: &[String],
) -> Option<PredicateInfo> {
    // Form: `{name} in [v1, v2, ...]`
    let trimmed = predicate.trim();
    let in_pos = trimmed.find(" in ")?;
    let lhs = trimmed[..in_pos].trim();
    let rhs = trimmed[in_pos + 4..].trim();
    let name = strip_curly(lhs)?;
    if !coords.contains(&name) {
        return None;
    }
    // RHS should be `[…]`
    let inner = rhs.strip_prefix('[')?.strip_suffix(']')?;
    let values: Vec<ConstValue> = inner
        .split(',')
        .map(|s| parse_literal(s.trim()))
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }

    let mut factor = PerAxisMap::new();
    factor.insert(name.clone(), predicate.trim().to_string());
    let mut range = PerAxisMap::new();
    range.insert(name.clone(), RangeConstraint::Discrete(values));

    Some(PredicateInfo {
        factorization: Factorization::PerAxis(factor),
        monotonicity: PerAxisMap::new(),
        range_constraint: range,
        determinism: Determinism::Deterministic,
        coords_referenced: coord_refs.to_vec(),
    })
}

// ---- string parsing helpers ----

/// Strip `{name}` wrapper from a string; return the inner name
/// if matched, else `None`. Only matches a fully-wrapped
/// `{name}` with no surrounding text.
fn strip_curly(s: &str) -> Option<String> {
    let s = s.trim();
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        let trimmed = inner.trim();
        if trimmed.chars().all(|c| c.is_alphanumeric() || c == '_') && !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

/// Parse a literal value (int, float, string, bool). Returns
/// `None` if the input isn't a recognizable literal.
fn parse_literal(s: &str) -> Option<ConstValue> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("true") {
        return Some(ConstValue::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Some(ConstValue::Bool(false));
    }
    // Quoted string?
    if s.len() >= 2
        && ((s.starts_with('"') && s.ends_with('"'))
            || (s.starts_with('\'') && s.ends_with('\'')))
    {
        return Some(ConstValue::String(s[1..s.len() - 1].to_string()));
    }
    // Integer or float?
    if let Ok(n) = s.parse::<i64>() {
        return Some(ConstValue::Int(n));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Some(ConstValue::Float(f));
    }
    None
}

/// Split a string on top-level occurrences of a separator,
/// respecting parens / brackets / braces. Returns `Some(parts)`
/// if the separator was found at the top level (≥2 parts).
fn split_top_level(s: &str, sep: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut depth = 0i64;
    let mut last = 0usize;
    let bytes = s.as_bytes();
    let sep_bytes = sep.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && i + sep_bytes.len() <= bytes.len() && &bytes[i..i + sep_bytes.len()] == sep_bytes {
            parts.push(s[last..i].trim().to_string());
            last = i + sep_bytes.len();
            i = last;
            continue;
        }
        i += 1;
    }
    if parts.is_empty() {
        return None;
    }
    parts.push(s[last..].trim().to_string());
    Some(parts)
}

/// Split exactly once on the first top-level occurrence of a
/// 2-character operator. Used for binary comparison
/// recognition where we want `lhs OP rhs` not `lhs OP rhs OP foo`.
fn split_top_level_op<'a>(s: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut depth = 0i64;
    let bytes = s.as_bytes();
    let op_bytes = op.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && i + op_bytes.len() <= bytes.len() && &bytes[i..i + op_bytes.len()] == op_bytes {
            // For the shorter ops (<, >) ensure we don't catch
            // <=, >= (longer ops are checked first by the
            // caller's loop order).
            if op.len() == 1 {
                let next = bytes.get(i + 1).copied();
                if next == Some(b'=') {
                    i += 1;
                    continue;
                }
            }
            return Some((&s[..i], &s[i + op_bytes.len()..]));
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coords(names: &[&str]) -> CoordSet {
        CoordSet::all_discrete(names.iter().copied())
    }

    #[test]
    fn recognize_per_axis_eq() {
        let info = recognize("{k} == 5", &coords(&["k", "limit"]));
        match info.factorization {
            Factorization::PerAxis(m) => {
                assert!(m.get("k").is_some());
                assert!(m.get("limit").is_none());
            }
            other => panic!("expected PerAxis, got {other:?}"),
        }
        assert_eq!(info.coords_referenced, vec!["k"]);
        let range = info.range_constraint.get("k").unwrap();
        assert!(matches!(range, RangeConstraint::Discrete(vs) if vs.len() == 1));
    }

    #[test]
    fn recognize_per_axis_gt() {
        let info = recognize("{k} > 10", &coords(&["k"]));
        assert!(matches!(info.factorization, Factorization::PerAxis(_)));
        assert_eq!(info.monotonicity.get("k"), Some(&Monotonicity::Increasing));
        let range = info.range_constraint.get("k").unwrap();
        match range {
            RangeConstraint::Bounded { lo: Some(ConstValue::Int(10)), hi: None, .. } => {}
            other => panic!("expected Bounded lo=10, got {other:?}"),
        }
    }

    #[test]
    fn recognize_per_axis_le_reversed() {
        // `5 <= {k}` should canonicalize to `{k} >= 5`.
        let info = recognize("5 <= {k}", &coords(&["k"]));
        assert_eq!(info.monotonicity.get("k"), Some(&Monotonicity::Increasing));
        let range = info.range_constraint.get("k").unwrap();
        match range {
            RangeConstraint::Bounded { lo: Some(ConstValue::Int(5)), lo_inclusive: true, .. } => {}
            other => panic!("expected Bounded lo=5 inclusive, got {other:?}"),
        }
    }

    #[test]
    fn recognize_cross_axis_comparison_is_conjunctive() {
        let info = recognize("{k} == {limit}", &coords(&["k", "limit"]));
        assert!(matches!(info.factorization, Factorization::Conjunctive(_)));
    }

    #[test]
    fn recognize_conjunction_of_per_axis() {
        let info = recognize("{k} > 5 && {limit} < 100", &coords(&["k", "limit"]));
        match &info.factorization {
            Factorization::PerAxis(m) => {
                assert!(m.get("k").is_some());
                assert!(m.get("limit").is_some());
            }
            other => panic!("expected PerAxis, got {other:?}"),
        }
        assert_eq!(info.monotonicity.get("k"), Some(&Monotonicity::Increasing));
        assert_eq!(info.monotonicity.get("limit"), Some(&Monotonicity::Decreasing));
    }

    #[test]
    fn recognize_conjunction_range_fold() {
        let info = recognize("10 <= {k} && {k} <= 100", &coords(&["k"]));
        match &info.factorization {
            Factorization::PerAxis(m) => {
                assert!(m.get("k").is_some());
            }
            other => panic!("expected PerAxis after range fold, got {other:?}"),
        }
        let range = info.range_constraint.get("k").unwrap();
        match range {
            RangeConstraint::Bounded {
                lo: Some(ConstValue::Int(10)),
                hi: Some(ConstValue::Int(100)),
                lo_inclusive: true,
                hi_inclusive: true,
            } => {}
            other => panic!("expected folded [10, 100], got {other:?}"),
        }
    }

    #[test]
    fn recognize_disjunction_per_axis_is_disjunctive() {
        let info = recognize("{k} == 1 || {k} == 100", &coords(&["k"]));
        assert!(matches!(info.factorization, Factorization::Disjunctive(_)));
    }

    #[test]
    fn recognize_negation_per_axis() {
        let info = recognize("!{k} > 0", &coords(&["k"]));
        // The simple recognizer may not parse this depending
        // on whitespace; check what it produces.
        // !pattern where pattern is `{k} > 0` (per-axis) →
        // inverted per-axis with flipped monotonicity.
        match info.factorization {
            Factorization::PerAxis(_) => {
                assert_eq!(info.monotonicity.get("k"), Some(&Monotonicity::Decreasing));
            }
            Factorization::Opaque(_) => {
                // Acceptable conservative fallback.
            }
            other => panic!("unexpected factorization {other:?}"),
        }
    }

    #[test]
    fn recognize_discrete_set() {
        let info = recognize("{k} in [1, 7, 42]", &coords(&["k"]));
        match &info.factorization {
            Factorization::PerAxis(m) => assert!(m.get("k").is_some()),
            other => panic!("expected PerAxis, got {other:?}"),
        }
        let range = info.range_constraint.get("k").unwrap();
        match range {
            RangeConstraint::Discrete(vs) => {
                assert_eq!(vs.len(), 3);
                assert_eq!(vs[0], ConstValue::Int(1));
                assert_eq!(vs[1], ConstValue::Int(7));
                assert_eq!(vs[2], ConstValue::Int(42));
            }
            other => panic!("expected Discrete, got {other:?}"),
        }
    }

    #[test]
    fn unknown_pattern_is_opaque() {
        let info = recognize("complicated_function({k}) > 0", &coords(&["k"]));
        assert!(matches!(
            info.factorization,
            Factorization::Opaque(OpaqueReason::UnknownPattern)
        ));
    }

    #[test]
    fn nondeterministic_function_marks_opaque_determinism() {
        let info = recognize("random() > 0.5", &coords(&[]));
        assert_eq!(info.determinism, Determinism::Opaque);
    }

    #[test]
    fn continuous_coord_short_circuit() {
        use crate::iteration::comprehension::predicate::coordset::{CoordInfo, CoordKind};
        let mut coords = CoordSet::new();
        coords.push(CoordInfo {
            name: "theta".to_string(),
            kind: CoordKind::Continuous,
        });
        let info = recognize("{theta} > 1.5", &coords);
        assert!(matches!(
            info.factorization,
            Factorization::Opaque(OpaqueReason::Continuous)
        ));
    }

    #[test]
    fn extract_coord_refs_simple() {
        assert_eq!(extract_coord_refs("{k} > 0"), vec!["k"]);
        assert_eq!(
            extract_coord_refs("{k} * {limit} <= 1000"),
            vec!["k", "limit"]
        );
    }

    #[test]
    fn split_top_level_respects_parens() {
        let s = "f(a && b) && c";
        let parts = split_top_level(s, "&&").unwrap();
        assert_eq!(parts, vec!["f(a && b)", "c"]);
    }
}
