// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cursor partition specs — SRD 71.
//!
//! Two value types and a small spec language:
//!
//! - [`PartitionSpec`] — the parsed-but-unresolved form of a
//!   `cursor=...` argument. Captures the operator's spec
//!   literal (Form 1 single sub-range, Form 2 delta list, or
//!   Form 3 pre-baked recipe) as a list of typed [`Bound`]s.
//! - [`Partition`] — a single resolved partition with concrete
//!   absolute ordinals, computed by [`resolve`] from a
//!   `PartitionSpec` against a known base extent.
//!
//! See [`docs/sysref/71_cursor_partitions.md`] for the design
//! memo this implements. The parser and resolution math live
//! here; the DSL `over` clause and cursor source factory
//! integration live in their own modules. The GK `Value`
//! integration rides on the existing [`Value::Ext`] /
//! [`ReflectedValue`] mechanism — see the impls below.

use std::fmt;
use std::sync::Arc;

use crate::node::{ReflectedValue, Value};

/// One numeric boundary inside a partition spec. The three forms
/// are distinguished syntactically at parse time:
/// - Trailing `%` → [`Bound::Pct`]
/// - Decimal in `[0.0, 1.0]` → [`Bound::Frac`]
/// - Bare integer → [`Bound::Ord`]
/// - Literal `*` (or `*%`) → [`Bound::Star`]
///
/// [`Bound::Pct`] and [`Bound::Frac`] are equivalent at resolve
/// time (the latter is just the former divided by 100); both
/// require a base extent. [`Bound::Ord`] is already absolute.
/// [`Bound::Star`] is the remainder token, valid only inside a
/// Form 2 delta list at most once.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Bound {
    /// Percentage of the cursor's base extent, `[0.0, 100.0]`.
    Pct(f64),
    /// Fraction of the cursor's base extent, `[0.0, 1.0]`.
    /// Equivalent to `Pct(value * 100)`.
    Frac(f64),
    /// Absolute cursor ordinal (already in ordinal space).
    Ord(u64),
    /// Remainder marker — absorbs whatever's needed for the
    /// containing delta list to span the cursor's full extent.
    Star,
}

impl Bound {
    /// Resolve this bound to an absolute ordinal in
    /// `[base_start, base_end]` against a known extent. Returns
    /// `None` for [`Bound::Star`] — the caller is responsible
    /// for star-resolution (which depends on the sum of the
    /// non-star deltas).
    pub fn resolve_against(&self, base_start: u64, base_end: u64) -> Option<u64> {
        let extent = base_end.saturating_sub(base_start);
        match self {
            Bound::Pct(p) => Some(base_start + ((p / 100.0) * extent as f64).round() as u64),
            Bound::Frac(f) => Some(base_start + (f * extent as f64).round() as u64),
            Bound::Ord(o) => Some(base_start.saturating_add(*o).min(base_end)),
            Bound::Star => None,
        }
    }
}

impl fmt::Display for Bound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Bound::Pct(p) => write!(f, "{p}%"),
            Bound::Frac(v) => write!(f, "{v}"),
            Bound::Ord(o) => write!(f, "{o}"),
            Bound::Star => write!(f, "*"),
        }
    }
}

/// Parsed `cursor=...` argument. Two shapes:
///
/// - [`PartitionSpec::SingleRange`] — Form 1: an explicit
///   `start..end` interval. Always exactly one partition at
///   resolve time.
/// - [`PartitionSpec::DeltaList`] — Forms 2 and 3: an ordered
///   list of per-partition delta sizes, walked left-to-right
///   from `base_start`. Pre-baked recipes (`bin:5`, `fib:7`,
///   etc.) parse into normalised percentage deltas.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionSpec {
    /// `start..end` form. Single partition spanning the named
    /// boundary, regardless of either endpoint's `Bound` kind.
    SingleRange {
        start: Bound,
        end: Bound,
    },
    /// Comma-separated delta list. Each entry is the delta
    /// from the running start; a single [`Bound::Star`] entry
    /// is allowed and resolves to whatever's left after the
    /// other deltas are applied. Deltas summing to less than
    /// the cursor's extent (without `Star`) drop the trailing
    /// gap; summing to more is a resolve-time error.
    DeltaList {
        deltas: Vec<Bound>,
    },
}

/// A single resolved partition: an absolute ordinal range with
/// derived percentage and index metadata.
///
/// `cardinality()` returns `end_ord - start_ord` — the number
/// of ordinals the partition covers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Partition {
    /// 0-based position in the resolved partition list.
    pub idx: u64,
    /// Absolute ordinal at partition start (inclusive).
    pub start_ord: u64,
    /// Absolute ordinal at partition end (exclusive).
    pub end_ord: u64,
    /// Start as a percentage of the base extent, `[0.0, 100.0)`.
    pub start_pct: f64,
    /// End as a percentage of the base extent, `(0.0, 100.0]`.
    pub end_pct: f64,
    /// The base extent the partition was resolved against.
    /// Stored so consumers can recompute pcts or compare
    /// partitions resolved against different extents.
    pub base_extent: u64,
}

impl Partition {
    /// Number of ordinals in the partition: `end_ord - start_ord`.
    #[inline]
    pub fn cardinality(&self) -> u64 {
        self.end_ord - self.start_ord
    }
}

// =========================================================================
// Parser
// =========================================================================

/// Parse a `cursor=...` spec string into a [`PartitionSpec`].
///
/// Accepts all three forms documented in SRD 71:
///
/// - Form 1 — single sub-range: `0..53%`, `[0..53%)`, `100..1000`,
///   `0.05..0.5`, `100..50%`. Bracket placement and closure
///   markers (`[ ] ( )`) are tolerated but advisory; the closure
///   is always `[start, end)`.
/// - Form 2 — delta list: `2%,10%,*%`, `0.02,0.10,*`,
///   `1000,5000,*`, `1000,10%,*`, `20%,30%`.
/// - Form 3 — pre-baked recipe: `linear:N`, `ratios:a,b,c`,
///   `mul:R`, `mul:S,R`, `bin:N`, `fib:N`, `ln:N`.
///
/// Whitespace is ignored throughout. Bracket characters (`[`,
/// `]`, `(`, `)`) are stripped unconditionally — they're
/// advisory closure markers in the grammar (everything's always
/// `[start, end)` at resolve time), so any placement parses the
/// same way.
pub fn parse(input: &str) -> Result<PartitionSpec, String> {
    let cleaned: String = input
        .chars()
        .filter(|c| !matches!(c, '[' | ']' | '(' | ')') && !c.is_whitespace())
        .collect();
    if cleaned.is_empty() {
        return Err(format!("empty spec: `{input}`"));
    }
    // Form 3: pre-baked recipe — contains `:`.
    if let Some((name, args)) = split_recipe(&cleaned) {
        return expand_recipe(name, args);
    }
    // Form 1: single sub-range — contains `..` at top level.
    if let Some((lhs, rhs)) = split_range(&cleaned) {
        let start = parse_bound(lhs)?;
        let end = parse_bound(rhs)?;
        // Form 1 doesn't allow `*` — that's a delta-list-only token.
        if matches!(start, Bound::Star) || matches!(end, Bound::Star) {
            return Err(format!(
                "`*` is only valid inside a comma-separated delta list, not a `..` range; got `{input}`"
            ));
        }
        return Ok(PartitionSpec::SingleRange { start, end });
    }
    // Form 2: delta list — split on commas.
    let entries: Vec<&str> = cleaned.split(',').collect();
    if entries.iter().any(|e| e.is_empty()) {
        return Err(format!("empty entry in delta list: `{input}`"));
    }
    let deltas: Vec<Bound> = entries
        .iter()
        .map(|e| parse_bound(e))
        .collect::<Result<_, _>>()?;
    let star_count = deltas.iter().filter(|b| matches!(b, Bound::Star)).count();
    if star_count > 1 {
        return Err(format!(
            "at most one `*` remainder token is allowed in a delta list; got {star_count} in `{input}`"
        ));
    }
    Ok(PartitionSpec::DeltaList { deltas })
}

/// If `s` matches `<name>:<args>`, return (name, args). Recipe
/// names are alphabetic-only (plus `_`) to avoid colliding with
/// any number form.
fn split_recipe(s: &str) -> Option<(&str, &str)> {
    let colon = s.find(':')?;
    let name = &s[..colon];
    if name.is_empty() {
        return None;
    }
    if !name.chars().all(|c| c.is_ascii_alphabetic() || c == '_') {
        return None;
    }
    Some((name, &s[colon + 1..]))
}

/// Find a top-level `..` separator (the Form 1 range marker).
/// Returns `None` if the input is a single value (no `..`).
fn split_range(s: &str) -> Option<(&str, &str)> {
    s.find("..").map(|idx| (&s[..idx], &s[idx + 2..]))
}

/// Parse a single numeric bound. The form is unambiguous from
/// the literal's shape; see [`Bound`] for the form-to-variant
/// mapping.
fn parse_bound(raw: &str) -> Result<Bound, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("empty bound".into());
    }
    // Remainder token: `*` or `*%` (the `%` is decorative).
    if s == "*" || s == "*%" {
        return Ok(Bound::Star);
    }
    // Percentage form: trailing `%`.
    if let Some(num) = s.strip_suffix('%') {
        let value: f64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid percentage `{raw}`: expected a number before `%`"))?;
        if !(0.0..=100.0).contains(&value) {
            return Err(format!(
                "percentage `{raw}` out of range — must be in [0%, 100%]"
            ));
        }
        return Ok(Bound::Pct(value));
    }
    // Decimal-with-dot: fraction form.
    if s.contains('.') {
        let value: f64 = s
            .parse()
            .map_err(|_| format!("invalid decimal `{raw}`"))?;
        if !(0.0..=1.0).contains(&value) {
            return Err(format!(
                "decimal `{raw}` is ambiguous — fractions must be in [0.0, 1.0]; \
                 did you mean `{}%` (percentage), `0.0{}` (fraction), or `{}` (literal ordinal)?",
                value, raw.replace('.', ""), raw.replace('.', ""),
            ));
        }
        return Ok(Bound::Frac(value));
    }
    // Bare integer: literal ordinal.
    let value: u64 = s
        .parse()
        .map_err(|_| format!("invalid number `{raw}`: expected an integer ordinal, decimal fraction (0.x), or `N%` percentage"))?;
    Ok(Bound::Ord(value))
}

// =========================================================================
// Pre-baked recipes
// =========================================================================

/// Dispatch a recipe name + arg string to its weight list,
/// then normalise to a percentage delta list summing to 100%.
fn expand_recipe(name: &str, args: &str) -> Result<PartitionSpec, String> {
    let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
    let weights = match name {
        "linear" => recipe_linear(&parts)?,
        "ratios" => recipe_ratios(&parts)?,
        "mul" => recipe_mul(&parts)?,
        "bin" => recipe_bin(&parts)?,
        "fib" => recipe_fib(&parts)?,
        "ln" => recipe_ln(&parts)?,
        "geom" => recipe_geom(&parts)?,
        "zipf" => recipe_zipf(&parts)?,
        "pareto" => recipe_pareto(&parts)?,
        "front_heavy" => recipe_front_heavy(&parts)?,
        "back_heavy" => recipe_back_heavy(&parts)?,
        _ => {
            return Err(format!(
                "unknown recipe `{name}` — supported: linear, ratios, mul, bin, fib, ln, \
                 geom, zipf, pareto, front_heavy, back_heavy"
            ));
        }
    };
    let deltas = normalise_to_pct(&weights)?;
    Ok(PartitionSpec::DeltaList { deltas })
}

fn parse_u64_arg(arg: &str, ctx: &str) -> Result<u64, String> {
    arg.parse()
        .map_err(|_| format!("invalid integer arg `{arg}` for {ctx}"))
}

fn parse_f64_arg(arg: &str, ctx: &str) -> Result<f64, String> {
    arg.parse()
        .map_err(|_| format!("invalid number arg `{arg}` for {ctx}"))
}

fn recipe_linear(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 1 {
        return Err(format!(
            "linear:N expects exactly 1 argument (the partition count); got {}",
            args.len()
        ));
    }
    let n = parse_u64_arg(args[0], "linear")?;
    if n == 0 {
        return Err("linear:N requires N >= 1".into());
    }
    Ok(vec![1.0; n as usize])
}

fn recipe_ratios(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.is_empty() {
        return Err("ratios:a,b,c,... requires at least one weight".into());
    }
    args.iter()
        .map(|a| parse_f64_arg(a, "ratios"))
        .collect()
}

fn recipe_mul(args: &[&str]) -> Result<Vec<f64>, String> {
    let (start, ratio) = match args.len() {
        1 => (1.0, parse_f64_arg(args[0], "mul")?),
        2 => (parse_f64_arg(args[0], "mul")?, parse_f64_arg(args[1], "mul")?),
        n => return Err(format!("mul:R or mul:S,R expects 1 or 2 arguments; got {n}")),
    };
    if start <= 0.0 {
        return Err(format!("mul:S,R requires S > 0; got {start}"));
    }
    if ratio <= 0.0 {
        return Err(format!("mul:R requires R > 0; got {ratio}"));
    }
    // Two termination rules, whichever fires first:
    //  - decay case (R < 1): stop when current < start * 0.001 — the
    //    new term contributes less than 0.1% of the leading partition.
    //  - growth case (R >= 1): hard term cap. Without an explicit
    //    count the natural choice is the term where the geometric
    //    growth has covered ~3 orders of magnitude; that's about
    //    log_R(1000) terms. Use `geom:N,R` instead when you want a
    //    specific term count.
    const HARD_CAP: usize = 64;
    let mut weights = Vec::with_capacity(HARD_CAP);
    let mut current = start;
    for _ in 0..HARD_CAP {
        if !current.is_finite() || current <= 0.0 {
            break;
        }
        weights.push(current);
        if ratio < 1.0 && current < start * 0.001 {
            break;
        }
        current *= ratio;
        if ratio >= 1.0 && current >= start * 1000.0 {
            // Growth-case stop: include the next term so the
            // last partition is the dominant one.
            if current.is_finite() {
                weights.push(current);
            }
            break;
        }
    }
    if weights.is_empty() {
        return Err(format!("mul:{start},{ratio} produced no terms — pick a larger start"));
    }
    Ok(weights)
}

fn recipe_bin(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 1 {
        return Err(format!(
            "bin:N expects exactly 1 argument (the term count); got {}",
            args.len()
        ));
    }
    let n = parse_u64_arg(args[0], "bin")?;
    if n == 0 {
        return Err("bin:N requires N >= 1".into());
    }
    // Coefficients of (1+x)^(N-1): C(N-1, k) for k = 0..N-1.
    let degree = n - 1;
    let mut coeffs = vec![1.0f64; n as usize];
    for k in 1..=degree {
        coeffs[k as usize] = coeffs[(k - 1) as usize] * ((degree - k + 1) as f64) / (k as f64);
    }
    Ok(coeffs)
}

fn recipe_fib(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 1 {
        return Err(format!(
            "fib:N expects exactly 1 argument (the term count); got {}",
            args.len()
        ));
    }
    let n = parse_u64_arg(args[0], "fib")?;
    if n == 0 {
        return Err("fib:N requires N >= 1".into());
    }
    // Skip the redundant leading `1, 1` — use the distinct
    // Fibonacci values starting at 1: 1, 2, 3, 5, 8, 13, ...
    let mut weights = Vec::with_capacity(n as usize);
    let (mut a, mut b) = (1u64, 2u64);
    for _ in 0..n {
        weights.push(a as f64);
        let next = a.saturating_add(b);
        a = b;
        b = next;
    }
    Ok(weights)
}

fn recipe_ln(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 1 {
        return Err(format!(
            "ln:N expects exactly 1 argument (the term count); got {}",
            args.len()
        ));
    }
    let n = parse_u64_arg(args[0], "ln")?;
    if n == 0 {
        return Err("ln:N requires N >= 1".into());
    }
    Ok((1..=n).map(|i| (1.0 + i as f64).ln()).collect())
}

fn recipe_geom(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 2 {
        return Err(format!(
            "geom:N,R expects exactly 2 arguments; got {}",
            args.len()
        ));
    }
    let n = parse_u64_arg(args[0], "geom")?;
    let r = parse_f64_arg(args[1], "geom")?;
    if n == 0 {
        return Err("geom:N,R requires N >= 1".into());
    }
    if r <= 0.0 {
        return Err(format!("geom:N,R requires R > 0; got {r}"));
    }
    let mut weights = Vec::with_capacity(n as usize);
    let mut current = 1.0;
    for _ in 0..n {
        weights.push(current);
        current *= r;
    }
    Ok(weights)
}

fn recipe_zipf(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 2 {
        return Err(format!(
            "zipf:s,N expects exactly 2 arguments; got {}",
            args.len()
        ));
    }
    let s = parse_f64_arg(args[0], "zipf")?;
    let n = parse_u64_arg(args[1], "zipf")?;
    if s <= 0.0 {
        return Err(format!("zipf:s,N requires s > 0; got {s}"));
    }
    if n == 0 {
        return Err("zipf:s,N requires N >= 1".into());
    }
    Ok((1..=n).map(|i| 1.0 / (i as f64).powf(s)).collect())
}

fn recipe_pareto(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 2 {
        return Err(format!(
            "pareto:alpha,N expects exactly 2 arguments; got {}",
            args.len()
        ));
    }
    let alpha = parse_f64_arg(args[0], "pareto")?;
    let n = parse_u64_arg(args[1], "pareto")?;
    if alpha <= 0.0 {
        return Err(format!("pareto:alpha,N requires alpha > 0; got {alpha}"));
    }
    if n == 0 {
        return Err("pareto:alpha,N requires N >= 1".into());
    }
    Ok((1..=n).map(|i| (1.0 / i as f64).powf(alpha)).collect())
}

fn recipe_front_heavy(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 1 {
        return Err(format!(
            "front_heavy:N expects exactly 1 argument; got {}",
            args.len()
        ));
    }
    let n = parse_u64_arg(args[0], "front_heavy")?;
    if n == 0 {
        return Err("front_heavy:N requires N >= 1".into());
    }
    Ok((1..=n).rev().map(|i| i as f64).collect())
}

fn recipe_back_heavy(args: &[&str]) -> Result<Vec<f64>, String> {
    if args.len() != 1 {
        return Err(format!(
            "back_heavy:N expects exactly 1 argument; got {}",
            args.len()
        ));
    }
    let n = parse_u64_arg(args[0], "back_heavy")?;
    if n == 0 {
        return Err("back_heavy:N requires N >= 1".into());
    }
    Ok((1..=n).map(|i| i as f64).collect())
}

/// Normalise raw recipe weights to percentage deltas summing
/// to 100%. Weights must be non-negative and have a positive
/// sum.
fn normalise_to_pct(weights: &[f64]) -> Result<Vec<Bound>, String> {
    if weights.iter().any(|w| !w.is_finite() || *w < 0.0) {
        return Err("recipe produced non-finite or negative weights".into());
    }
    let sum: f64 = weights.iter().sum();
    if sum <= 0.0 {
        return Err("recipe produced zero total weight".into());
    }
    Ok(weights.iter().map(|w| Bound::Pct(w / sum * 100.0)).collect())
}

// =========================================================================
// Resolution
// =========================================================================

/// Resolve a [`PartitionSpec`] against a cursor's base extent
/// `[base_start, base_end)`, producing a list of concrete
/// [`Partition`]s with absolute ordinals.
///
/// For [`PartitionSpec::SingleRange`] the result is always a
/// 1-element vector.
///
/// For [`PartitionSpec::DeltaList`] the deltas are walked
/// left-to-right; a `Bound::Star` entry absorbs whatever's
/// needed to reach `base_end`. A sum exceeding the extent
/// (without `Star`) is a hard error.
pub fn resolve(
    spec: &PartitionSpec,
    base_start: u64,
    base_end: u64,
) -> Result<Vec<Partition>, String> {
    if base_end < base_start {
        return Err(format!(
            "resolve: base_end ({base_end}) < base_start ({base_start})"
        ));
    }
    let extent = base_end - base_start;
    match spec {
        PartitionSpec::SingleRange { start, end } => {
            let start_ord = start
                .resolve_against(base_start, base_end)
                .expect("Star not allowed in SingleRange (checked at parse time)");
            let end_ord = end
                .resolve_against(base_start, base_end)
                .expect("Star not allowed in SingleRange (checked at parse time)");
            if end_ord < start_ord {
                return Err(format!(
                    "resolved range is empty or reversed: start={start_ord}, end={end_ord} \
                     (spec start={start}, end={end}, base=[{base_start}..{base_end}))"
                ));
            }
            let start_pct = pct_of(start_ord, base_start, extent);
            let end_pct = pct_of(end_ord, base_start, extent);
            Ok(vec![Partition {
                idx: 0,
                start_ord,
                end_ord,
                start_pct,
                end_pct,
                base_extent: extent,
            }])
        }
        PartitionSpec::DeltaList { deltas } => resolve_delta_list(deltas, base_start, base_end, extent),
    }
}

fn resolve_delta_list(
    deltas: &[Bound],
    base_start: u64,
    base_end: u64,
    extent: u64,
) -> Result<Vec<Partition>, String> {
    // Pre-compute the size of each non-Star delta in ordinal
    // space. Star's size is computed after the others.
    let non_star_total: u64 = deltas
        .iter()
        .filter_map(|b| match b {
            Bound::Star => None,
            other => Some(delta_to_ordinals(other, extent)),
        })
        .sum();
    if non_star_total > extent {
        return Err(format!(
            "delta list sums to {non_star_total} ordinals, exceeding the cursor's extent {extent}; \
             trim the list or use a `*` remainder to absorb the overflow"
        ));
    }
    let star_size = extent - non_star_total;
    let mut partitions = Vec::with_capacity(deltas.len());
    let mut cursor = base_start;
    for (i, delta) in deltas.iter().enumerate() {
        let size = match delta {
            Bound::Star => star_size,
            other => delta_to_ordinals(other, extent),
        };
        let next = cursor + size;
        let start_pct = pct_of(cursor, base_start, extent);
        let end_pct = pct_of(next, base_start, extent);
        partitions.push(Partition {
            idx: i as u64,
            start_ord: cursor,
            end_ord: next,
            start_pct,
            end_pct,
            base_extent: extent,
        });
        cursor = next;
    }
    // Trailing-gap policy: deltas summing to less than the
    // extent (without Star) drop the gap. `cursor` may end
    // short of `base_end` — that's intentional, not an error.
    debug_assert!(cursor <= base_end);
    Ok(partitions)
}

/// Convert a delta `Bound` to an absolute number of ordinals
/// against an extent. `Star` is the caller's responsibility
/// (computed from extent minus non-star total).
fn delta_to_ordinals(b: &Bound, extent: u64) -> u64 {
    match b {
        Bound::Pct(p) => ((p / 100.0) * extent as f64).round() as u64,
        Bound::Frac(f) => (f * extent as f64).round() as u64,
        Bound::Ord(o) => *o,
        Bound::Star => unreachable!("Star handled separately"),
    }
}

#[inline]
fn pct_of(ordinal: u64, base_start: u64, extent: u64) -> f64 {
    if extent == 0 {
        0.0
    } else {
        (ordinal - base_start) as f64 * 100.0 / extent as f64
    }
}

// =========================================================================
// GK Value integration
// =========================================================================
//
// Partition and PartitionSpec carry through GK wires as
// `Value::Ext(Box<dyn ReflectedValue>)` rather than dedicated
// enum variants. This avoids sweeping every Value-match site in
// the codebase. Stdlib node functions that consume partitions
// downcast via [`Value::as_partition`] / [`Value::as_partition_spec`]
// at their entry points.

impl ReflectedValue for Partition {
    fn type_name(&self) -> &str { "Partition" }

    fn display(&self) -> String {
        format!(
            "Partition({}/{} [{}..{}) [{:.2}%..{:.2}%))",
            self.idx,
            // partition_count isn't stored on the partition itself;
            // displays the idx + range without it.
            "?",
            self.start_ord, self.end_ord, self.start_pct, self.end_pct,
        )
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "idx":         self.idx,
            "start_ord":   self.start_ord,
            "end_ord":     self.end_ord,
            "start_pct":   self.start_pct,
            "end_pct":     self.end_pct,
            "base_extent": self.base_extent,
            "cardinality": self.cardinality(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any { self }

    fn clone_reflected(&self) -> Box<dyn ReflectedValue> {
        Box::new(*self)
    }
}

impl ReflectedValue for PartitionSpec {
    fn type_name(&self) -> &str { "PartitionSpec" }

    fn display(&self) -> String {
        match self {
            PartitionSpec::SingleRange { start, end } => {
                format!("PartitionSpec({start}..{end})")
            }
            PartitionSpec::DeltaList { deltas } => {
                let parts: Vec<String> = deltas.iter().map(|b| b.to_string()).collect();
                format!("PartitionSpec({})", parts.join(","))
            }
        }
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::String(self.display())
    }

    fn as_any(&self) -> &dyn std::any::Any { self }

    fn clone_reflected(&self) -> Box<dyn ReflectedValue> {
        Box::new(self.clone())
    }
}

/// A list of resolved partitions carried as a single GK value.
///
/// Needed because [`Value::Ext`] holds one [`ReflectedValue`] —
/// to flow a `Vec<Partition>` on a single wire we wrap it once
/// here. Backed by `Arc` so cloning is one atomic increment.
#[derive(Debug, Clone)]
pub struct PartitionList(pub Arc<Vec<Partition>>);

impl PartitionList {
    pub fn new(partitions: Vec<Partition>) -> Self {
        Self(Arc::new(partitions))
    }

    /// Number of partitions in the list.
    pub fn len(&self) -> usize { self.0.len() }

    /// True if the list is empty.
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    /// Borrow the underlying slice for iteration.
    pub fn as_slice(&self) -> &[Partition] { &self.0 }
}

impl ReflectedValue for PartitionList {
    fn type_name(&self) -> &str { "PartitionList" }

    fn display(&self) -> String {
        let parts: Vec<String> = self.0.iter().map(|p| {
            format!("[{}..{})", p.start_ord, p.end_ord)
        }).collect();
        format!("PartitionList[{}]={}", self.0.len(), parts.join(","))
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::Array(self.0.iter().map(|p| p.to_json_value()).collect())
    }

    fn as_any(&self) -> &dyn std::any::Any { self }

    fn clone_reflected(&self) -> Box<dyn ReflectedValue> {
        Box::new(self.clone())
    }
}

/// Convenience constructors and downcasters on [`Value`] for
/// partition-typed wires. Use these at node entry / exit to
/// avoid `Value::Ext(Box::new(...))` boilerplate.
impl Value {
    /// Wrap a [`Partition`] as a GK `Value::Ext`.
    pub fn from_partition(p: Partition) -> Self {
        Value::Ext(Box::new(p))
    }

    /// Wrap a [`PartitionSpec`] as a GK `Value::Ext`.
    pub fn from_partition_spec(s: PartitionSpec) -> Self {
        Value::Ext(Box::new(s))
    }

    /// Wrap a `Vec<Partition>` as a GK `Value::Ext` via
    /// [`PartitionList`]. Use this when a wire needs to carry
    /// the whole resolved list (e.g. the `<param>.partitions`
    /// projection).
    pub fn from_partition_list(parts: Vec<Partition>) -> Self {
        Value::Ext(Box::new(PartitionList::new(parts)))
    }

    /// Downcast to a [`Partition`] reference. Returns `None` if
    /// the value isn't a partition.
    pub fn as_partition(&self) -> Option<&Partition> {
        match self {
            Value::Ext(b) => b.as_any().downcast_ref::<Partition>(),
            _ => None,
        }
    }

    /// Downcast to a [`PartitionSpec`] reference. Returns `None`
    /// if the value isn't a spec.
    pub fn as_partition_spec(&self) -> Option<&PartitionSpec> {
        match self {
            Value::Ext(b) => b.as_any().downcast_ref::<PartitionSpec>(),
            _ => None,
        }
    }

    /// Downcast to a [`PartitionList`] reference. Returns `None`
    /// if the value isn't a partition list.
    pub fn as_partition_list(&self) -> Option<&PartitionList> {
        match self {
            Value::Ext(b) => b.as_any().downcast_ref::<PartitionList>(),
            _ => None,
        }
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── Number-form parsing ────────────────────────────────

    #[test]
    fn parse_bound_percentage() {
        assert_eq!(parse_bound("53%").unwrap(), Bound::Pct(53.0));
        assert_eq!(parse_bound("0%").unwrap(), Bound::Pct(0.0));
        assert_eq!(parse_bound("100%").unwrap(), Bound::Pct(100.0));
        assert_eq!(parse_bound("0.5%").unwrap(), Bound::Pct(0.5));
    }

    #[test]
    fn parse_bound_percentage_out_of_range_rejected() {
        assert!(parse_bound("101%").is_err());
        assert!(parse_bound("-1%").is_err());
    }

    #[test]
    fn parse_bound_fraction() {
        assert_eq!(parse_bound("0.5").unwrap(), Bound::Frac(0.5));
        assert_eq!(parse_bound("0.0").unwrap(), Bound::Frac(0.0));
        assert_eq!(parse_bound("1.0").unwrap(), Bound::Frac(1.0));
        assert_eq!(parse_bound("0.123").unwrap(), Bound::Frac(0.123));
    }

    #[test]
    fn parse_bound_fraction_out_of_range_rejected() {
        let err = parse_bound("1.5").unwrap_err();
        assert!(err.contains("ambiguous"), "diagnostic should explain: {err}");
    }

    #[test]
    fn parse_bound_literal_ordinal() {
        assert_eq!(parse_bound("0").unwrap(), Bound::Ord(0));
        assert_eq!(parse_bound("100").unwrap(), Bound::Ord(100));
        assert_eq!(parse_bound("999999").unwrap(), Bound::Ord(999_999));
    }

    #[test]
    fn parse_bound_star_token() {
        assert_eq!(parse_bound("*").unwrap(), Bound::Star);
        assert_eq!(parse_bound("*%").unwrap(), Bound::Star);
    }

    // ── Form 1: single sub-range ───────────────────────────

    #[test]
    fn parse_form1_simple_pct() {
        let spec = parse("0..53%").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::SingleRange {
                start: Bound::Ord(0),
                end: Bound::Pct(53.0),
            }
        );
    }

    #[test]
    fn parse_form1_brackets_tolerated() {
        let canonical = PartitionSpec::SingleRange {
            start: Bound::Ord(0),
            end: Bound::Pct(53.0),
        };
        assert_eq!(parse("[0..53%]").unwrap(), canonical);
        assert_eq!(parse("[0..53%)").unwrap(), canonical);
        assert_eq!(parse("(0..53%]").unwrap(), canonical);
    }

    #[test]
    fn parse_form1_fraction_form() {
        let spec = parse("0..0.53").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::SingleRange {
                start: Bound::Ord(0),
                end: Bound::Frac(0.53),
            }
        );
    }

    #[test]
    fn parse_form1_literal_ordinals() {
        let spec = parse("100..1000").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::SingleRange {
                start: Bound::Ord(100),
                end: Bound::Ord(1000),
            }
        );
    }

    #[test]
    fn parse_form1_mixed_literal_and_pct() {
        let spec = parse("100..50%").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::SingleRange {
                start: Bound::Ord(100),
                end: Bound::Pct(50.0),
            }
        );
    }

    #[test]
    fn parse_form1_mixed_frac_and_literal() {
        let spec = parse("0.10..10000").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::SingleRange {
                start: Bound::Frac(0.10),
                end: Bound::Ord(10000),
            }
        );
    }

    #[test]
    fn parse_form1_rejects_star() {
        assert!(parse("0..*").is_err());
        assert!(parse("*..50%").is_err());
    }

    // ── Form 2: delta list ─────────────────────────────────

    #[test]
    fn parse_form2_with_star() {
        let spec = parse("2%,10%,*%").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::DeltaList {
                deltas: vec![Bound::Pct(2.0), Bound::Pct(10.0), Bound::Star],
            }
        );
    }

    #[test]
    fn parse_form2_fraction_equivalent() {
        let spec = parse("0.02,0.10,*").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::DeltaList {
                deltas: vec![Bound::Frac(0.02), Bound::Frac(0.10), Bound::Star],
            }
        );
    }

    #[test]
    fn parse_form2_literal_deltas() {
        let spec = parse("1000,5000,*").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::DeltaList {
                deltas: vec![Bound::Ord(1000), Bound::Ord(5000), Bound::Star],
            }
        );
    }

    #[test]
    fn parse_form2_mixed_entries() {
        let spec = parse("1000,10%,*").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::DeltaList {
                deltas: vec![Bound::Ord(1000), Bound::Pct(10.0), Bound::Star],
            }
        );
    }

    #[test]
    fn parse_form2_short_list_no_star() {
        let spec = parse("20%,30%").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::DeltaList {
                deltas: vec![Bound::Pct(20.0), Bound::Pct(30.0)],
            }
        );
    }

    #[test]
    fn parse_form2_rejects_multiple_stars() {
        let err = parse("*,*").unwrap_err();
        assert!(err.contains("at most one"), "diagnostic: {err}");
    }

    // ── Form 3: pre-baked recipes ──────────────────────────

    fn deltas_only(spec: PartitionSpec) -> Vec<Bound> {
        match spec {
            PartitionSpec::DeltaList { deltas } => deltas,
            other => panic!("expected DeltaList, got {other:?}"),
        }
    }

    fn pcts_of(spec: PartitionSpec) -> Vec<f64> {
        deltas_only(spec)
            .into_iter()
            .map(|b| match b {
                Bound::Pct(p) => p,
                other => panic!("expected Pct, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn recipe_linear_uniform_split() {
        let pcts = pcts_of(parse("linear:4").unwrap());
        assert_eq!(pcts.len(), 4);
        for p in &pcts {
            assert!((p - 25.0).abs() < 1e-9, "expected 25%, got {p}");
        }
    }

    #[test]
    fn recipe_ratios_normalises_weights() {
        let pcts = pcts_of(parse("ratios:1,1,2").unwrap());
        assert_eq!(pcts.len(), 3);
        assert!((pcts[0] - 25.0).abs() < 1e-9);
        assert!((pcts[1] - 25.0).abs() < 1e-9);
        assert!((pcts[2] - 50.0).abs() < 1e-9);
    }

    #[test]
    fn recipe_bin_5_is_five_terms_of_binomial_expansion() {
        // C(4, k) for k = 0..4 → [1, 4, 6, 4, 1], sum 16.
        let pcts = pcts_of(parse("bin:5").unwrap());
        assert_eq!(pcts.len(), 5);
        let expected = [1.0 / 16.0, 4.0 / 16.0, 6.0 / 16.0, 4.0 / 16.0, 1.0 / 16.0];
        for (i, e) in expected.iter().enumerate() {
            assert!((pcts[i] - e * 100.0).abs() < 1e-9, "term {i}: {} vs {}", pcts[i], e * 100.0);
        }
    }

    #[test]
    fn recipe_fib_7_uses_distinct_fibonacci() {
        // 1, 2, 3, 5, 8, 13, 21 — sum 53.
        let pcts = pcts_of(parse("fib:7").unwrap());
        assert_eq!(pcts.len(), 7);
        let expected_weights = [1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0];
        let sum: f64 = expected_weights.iter().sum();
        for (i, w) in expected_weights.iter().enumerate() {
            assert!((pcts[i] - w / sum * 100.0).abs() < 1e-9);
        }
    }

    #[test]
    fn recipe_ln_5_log_spaced() {
        let pcts = pcts_of(parse("ln:5").unwrap());
        assert_eq!(pcts.len(), 5);
        // Monotonically increasing weights.
        for i in 1..pcts.len() {
            assert!(pcts[i] > pcts[i - 1], "ln:N should be monotonic");
        }
        // Sum to 100.
        let total: f64 = pcts.iter().sum();
        assert!((total - 100.0).abs() < 1e-9, "total: {total}");
    }

    #[test]
    fn recipe_mul_decay_tail_off() {
        // Decay case: R < 1, terms shrink. Stop when current
        // term is < 0.1% of the starting weight.
        let pcts = pcts_of(parse("mul:0.5").unwrap());
        assert!(!pcts.is_empty());
        let total: f64 = pcts.iter().sum();
        assert!((total - 100.0).abs() < 1e-9, "total: {total}");
        // 1, 0.5, 0.25, ... — first partition should be the dominant one.
        assert!(pcts[0] > pcts[1]);
    }

    #[test]
    fn recipe_mul_growth_caps_at_3_orders_of_magnitude() {
        // Growth case: R > 1, terms grow. Stop when terms span
        // ~3 orders of magnitude. Sum normalises cleanly.
        let pcts = pcts_of(parse("mul:2").unwrap());
        assert!(!pcts.is_empty());
        assert!(pcts.len() < 64, "should terminate well before hard cap");
        let total: f64 = pcts.iter().sum();
        assert!((total - 100.0).abs() < 1e-9, "total: {total}");
    }

    #[test]
    fn recipe_mul_with_start_and_ratio() {
        // mul:S,R — start at S, compound by R.
        let pcts = pcts_of(parse("mul:5,0.5").unwrap());
        let total: f64 = pcts.iter().sum();
        assert!((total - 100.0).abs() < 1e-9, "total: {total}");
    }

    #[test]
    fn recipe_geom_fixed_term_count() {
        let pcts = pcts_of(parse("geom:5,2").unwrap());
        assert_eq!(pcts.len(), 5);
        // Weights are 1, 2, 4, 8, 16 — sum 31.
        let expected_total: f64 = 31.0;
        let expected = [1.0, 2.0, 4.0, 8.0, 16.0];
        for (i, e) in expected.iter().enumerate() {
            assert!((pcts[i] - e / expected_total * 100.0).abs() < 1e-9);
        }
    }

    #[test]
    fn recipe_front_heavy_declining() {
        let pcts = pcts_of(parse("front_heavy:4").unwrap());
        assert_eq!(pcts.len(), 4);
        for i in 1..pcts.len() {
            assert!(pcts[i] < pcts[i - 1], "front_heavy should be monotonic-declining");
        }
    }

    #[test]
    fn recipe_back_heavy_growing() {
        let pcts = pcts_of(parse("back_heavy:4").unwrap());
        assert_eq!(pcts.len(), 4);
        for i in 1..pcts.len() {
            assert!(pcts[i] > pcts[i - 1], "back_heavy should be monotonic-growing");
        }
    }

    #[test]
    fn recipe_unknown_name_rejected() {
        let err = parse("blorp:3").unwrap_err();
        assert!(err.contains("unknown recipe"), "diagnostic: {err}");
        assert!(err.contains("linear"), "should list supported recipes: {err}");
    }

    // ── Resolution ──────────────────────────────────────────

    #[test]
    fn resolve_form1_percentage_against_extent() {
        let spec = parse("0..50%").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].start_ord, 0);
        assert_eq!(parts[0].end_ord, 500);
        assert_eq!(parts[0].cardinality(), 500);
    }

    #[test]
    fn resolve_form1_literal_ordinals() {
        let spec = parse("100..1000").unwrap();
        let parts = resolve(&spec, 0, 10000).unwrap();
        assert_eq!(parts[0].start_ord, 100);
        assert_eq!(parts[0].end_ord, 1000);
        assert_eq!(parts[0].cardinality(), 900);
    }

    #[test]
    fn resolve_form1_mixed_literal_and_pct() {
        let spec = parse("100..50%").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        assert_eq!(parts[0].start_ord, 100);
        assert_eq!(parts[0].end_ord, 500);
    }

    #[test]
    fn resolve_form2_three_partition_pct_list() {
        let spec = parse("2%,10%,*%").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].start_ord, 0);
        assert_eq!(parts[0].end_ord, 20);
        assert_eq!(parts[1].start_ord, 20);
        assert_eq!(parts[1].end_ord, 120);
        assert_eq!(parts[2].start_ord, 120);
        assert_eq!(parts[2].end_ord, 1000);
        assert_eq!(parts[2].cardinality(), 880);
    }

    #[test]
    fn resolve_form2_literal_deltas() {
        let spec = parse("1000,5000,*").unwrap();
        let parts = resolve(&spec, 0, 10000).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].start_ord, 0);
        assert_eq!(parts[0].end_ord, 1000);
        assert_eq!(parts[1].start_ord, 1000);
        assert_eq!(parts[1].end_ord, 6000);
        assert_eq!(parts[2].start_ord, 6000);
        assert_eq!(parts[2].end_ord, 10000);
    }

    #[test]
    fn resolve_form2_mixed_literal_and_pct_with_star() {
        let spec = parse("1000,10%,*").unwrap();
        let parts = resolve(&spec, 0, 10000).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].cardinality(), 1000);
        assert_eq!(parts[1].cardinality(), 1000); // 10% of 10000
        assert_eq!(parts[2].cardinality(), 8000); // remainder
    }

    #[test]
    fn resolve_form2_short_list_drops_trailing_gap() {
        let spec = parse("20%,30%").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].end_ord, 200);
        assert_eq!(parts[1].end_ord, 500); // 50% boundary; trailing 50% gap dropped
    }

    #[test]
    fn resolve_rejects_over_extent_sum() {
        let spec = parse("60%,60%").unwrap();
        let err = resolve(&spec, 0, 1000).unwrap_err();
        assert!(err.contains("exceeding"), "diagnostic: {err}");
    }

    #[test]
    fn resolve_recipe_against_extent() {
        let spec = parse("linear:4").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        assert_eq!(parts.len(), 4);
        for p in &parts {
            assert_eq!(p.cardinality(), 250);
        }
    }

    #[test]
    fn resolve_partition_indices_assigned() {
        let spec = parse("linear:5").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        for (i, p) in parts.iter().enumerate() {
            assert_eq!(p.idx, i as u64);
        }
    }

    #[test]
    fn resolve_partition_pcts_populated() {
        let spec = parse("linear:4").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        assert!((parts[0].start_pct - 0.0).abs() < 1e-9);
        assert!((parts[0].end_pct - 25.0).abs() < 1e-9);
        assert!((parts[3].end_pct - 100.0).abs() < 1e-9);
    }

    // ── Whitespace tolerance ───────────────────────────────

    #[test]
    fn parse_tolerates_whitespace_in_lists() {
        let spec = parse(" 2% , 10% , *% ").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::DeltaList {
                deltas: vec![Bound::Pct(2.0), Bound::Pct(10.0), Bound::Star],
            }
        );
    }

    // ── GK Value round-trip ────────────────────────────────

    #[test]
    fn partition_roundtrips_through_value_ext() {
        let p = Partition {
            idx: 2,
            start_ord: 100,
            end_ord: 500,
            start_pct: 10.0,
            end_pct: 50.0,
            base_extent: 1000,
        };
        let v = Value::from_partition(p);
        let recovered = v.as_partition().expect("downcast");
        assert_eq!(recovered.idx, 2);
        assert_eq!(recovered.start_ord, 100);
        assert_eq!(recovered.end_ord, 500);
        assert_eq!(recovered.cardinality(), 400);
    }

    #[test]
    fn partition_spec_roundtrips_through_value_ext() {
        let spec = parse("fib:5").unwrap();
        let v = Value::from_partition_spec(spec);
        let recovered = v.as_partition_spec().expect("downcast");
        // Just sanity-check it's a DeltaList with 5 entries
        match recovered {
            PartitionSpec::DeltaList { deltas } => assert_eq!(deltas.len(), 5),
            other => panic!("expected DeltaList, got {other:?}"),
        }
    }

    #[test]
    fn partition_list_roundtrips_through_value_ext() {
        let spec = parse("linear:4").unwrap();
        let parts = resolve(&spec, 0, 1000).unwrap();
        let v = Value::from_partition_list(parts);
        let recovered = v.as_partition_list().expect("downcast");
        assert_eq!(recovered.len(), 4);
        assert_eq!(recovered.as_slice()[0].start_ord, 0);
        assert_eq!(recovered.as_slice()[3].end_ord, 1000);
    }

    #[test]
    fn non_partition_value_downcast_returns_none() {
        let v = Value::U64(42);
        assert!(v.as_partition().is_none());
        assert!(v.as_partition_spec().is_none());
        assert!(v.as_partition_list().is_none());
    }

    #[test]
    fn parse_tolerates_whitespace_in_range() {
        let spec = parse(" 0 .. 53 % ").unwrap();
        assert!(matches!(
            spec,
            PartitionSpec::SingleRange {
                start: Bound::Ord(0),
                end: Bound::Pct(p),
            } if (p - 53.0).abs() < 1e-9
        ));
    }
}
