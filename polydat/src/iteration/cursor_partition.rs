// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cursor partition specs — SRD 71.
//!
//! Value types and a small spec language following the token
//! grammar `chunking [in window] [order]`:
//!
//! - [`PartitionSpec`] — the parsed-but-unresolved form of a
//!   `cursor=...` argument: a [`Chunking`] (Form 1 single
//!   sub-range, Form 2 delta list, or Form 3 pre-baked recipe,
//!   as typed [`Bound`]s), an optional `in start..end` window,
//!   and a [`PartitionOrder`].
//! - [`Partition`] — a single resolved partition with concrete
//!   absolute ordinals, computed by [`resolve`] from a
//!   `PartitionSpec` against a known base extent.
//!
//! See [`docs/sysref/71_cursor_partitions.md`] for the design
//! memo this implements. The parser and resolution math live
//! here; the DSL `over` clause and cursor source factory
//! integration live in their own modules. The Polydat `Value`
//! integration rides on the existing [`Value::Ext`] /
//! [`ReflectedValue`] mechanism — see the impls below.

use std::fmt;
use std::sync::Arc;

use crate::ast::{ReflectedValue, Value};

/// One numeric boundary or list token inside a partition spec.
/// The forms are distinguished syntactically at parse time:
/// - Trailing `%` → [`Bound::Pct`]
/// - Decimal in `[0.0, 1.0]` → [`Bound::Frac`]
/// - Bare integer → [`Bound::Ord`]
/// - Literal `*` (or `*%`) → [`Bound::Star`]
/// - Literal `...` → [`Bound::Fill`]
/// - `*/N` (bare integer N) → [`Bound::StarSplit`]
///
/// [`Bound::Pct`] and [`Bound::Frac`] are equivalent at resolve
/// time (the latter is just the former divided by 100); both
/// require a base extent. [`Bound::Ord`] is already absolute.
///
/// The tail tokens (`Star`, `Fill`, `StarSplit`, `StarShaped`)
/// are valid only inside a Form 2 delta list and at most one per
/// list. `Star` absorbs the remainder as a single partition;
/// `Fill` repeats the preceding delta until the extent is used
/// up; `StarSplit(n)` divides the remainder into `n` equal
/// partitions; `StarShaped(weights)` divides it by recipe
/// weights. `Fill`, `StarSplit`, and `StarShaped` must be the
/// final entry; `Star` may sit anywhere in the list. `Gap`
/// wraps a sized bound and consumes extent without emitting a
/// partition.
#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    /// Percentage of the cursor's base extent, `[0.0, 100.0]`.
    Pct(f64),
    /// Fraction of the cursor's base extent, `[0.0, 1.0]`.
    /// Equivalent to `Pct(value * 100)`.
    Frac(f64),
    /// Absolute cursor ordinal (already in ordinal space).
    Ord(u64),
    /// Remainder marker — absorbs whatever's needed for the
    /// containing delta list to span the cursor's full extent,
    /// as one partition.
    Star,
    /// Fill marker (`...`) — repeats the preceding delta until
    /// the extent is used up. A final chunk smaller than the
    /// repeated delta is emitted truncated, never dropped.
    Fill,
    /// Remainder split (`*/N`) — divides whatever's left after
    /// the other deltas into `N` partitions whose sizes differ
    /// by at most one ordinal.
    StarSplit(u64),
    /// Recipe-shaped remainder (`*/fib:5`, `*/ratios:1,3`, …) —
    /// divides whatever's left by the recipe's normalised
    /// weights (stored summing to 100).
    StarShaped(Vec<f64>),
    /// Gap (`~10%`, `~1000`) — consumes the wrapped sized
    /// bound's extent without emitting a partition. The walk
    /// stays contiguous; the *emitted* partition set skips this
    /// range. Parse guarantees the inner bound is sized
    /// (`Pct` / `Frac` / `Ord`).
    Gap(Box<Bound>),
}

impl Bound {
    /// Resolve this bound to an absolute ordinal in
    /// `[base_start, base_end]` against a known extent. Returns
    /// `None` for the tail tokens and gaps — their resolution
    /// depends on list context and is the caller's
    /// responsibility.
    pub fn resolve_against(&self, base_start: u64, base_end: u64) -> Option<u64> {
        let extent = base_end.saturating_sub(base_start);
        match self {
            Bound::Pct(p) => Some(base_start + ((p / 100.0) * extent as f64).round() as u64),
            Bound::Frac(f) => Some(base_start + (f * extent as f64).round() as u64),
            Bound::Ord(o) => Some(base_start.saturating_add(*o).min(base_end)),
            Bound::Star | Bound::Fill | Bound::StarSplit(_) | Bound::StarShaped(_)
            | Bound::Gap(_) => None,
        }
    }

    /// True for the tail tokens that consume the unallocated
    /// remainder of the extent rather than naming a fixed size.
    pub fn is_tail(&self) -> bool {
        matches!(
            self,
            Bound::Star | Bound::Fill | Bound::StarSplit(_) | Bound::StarShaped(_)
        )
    }

    /// True for the sized forms (`Pct` / `Frac` / `Ord`) that
    /// name a fixed amount of extent.
    pub fn is_sized(&self) -> bool {
        matches!(self, Bound::Pct(_) | Bound::Frac(_) | Bound::Ord(_))
    }
}

impl fmt::Display for Bound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Bound::Pct(p) => write!(f, "{p}%"),
            Bound::Frac(v) => write!(f, "{v}"),
            Bound::Ord(o) => write!(f, "{o}"),
            Bound::Star => write!(f, "*"),
            Bound::Fill => write!(f, "..."),
            Bound::StarSplit(n) => write!(f, "*/{n}"),
            Bound::StarShaped(w) => {
                let ws: Vec<String> = w.iter().map(|x| format!("{x:.3}")).collect();
                write!(f, "*/shaped:{}", ws.join(","))
            }
            Bound::Gap(inner) => write!(f, "~{inner}"),
        }
    }
}

/// Iteration order for a resolved partition list — the optional
/// trailing keyword of a spec (`"fib:5 largest_first"`).
///
/// `unchanged` and `random` are a **common-subset vocabulary**
/// shared conceptually with comprehension traversal orders
/// (SRD 18c) — where a word exists in both places it means the
/// same thing, while comprehensions additionally offer
/// algorithm-specific strategies (sobol, halton, lhs, …) that
/// do not apply to partition lists.
///
/// The size sorts are named for their axis: partition lists are
/// positionally contiguous by construction, so an unqualified
/// "ascending" would be ambiguous between ordinal position
/// (always the generation order — an alias of `unchanged`) and
/// size. `smallest_first` / `largest_first` key on
/// **cardinality**, unambiguously (stable — ties keep
/// generation order). The bare words `ascending` / `descending`
/// are rejected at parse time with a diagnostic naming these.
/// `Random` is a deterministic shuffle seeded from the spec
/// text, so the same spec yields the same order on every run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionOrder {
    /// Generation order (left-to-right as resolved). The default.
    #[default]
    Unchanged,
    /// Smallest cardinality first (stable).
    SmallestFirst,
    /// Largest cardinality first (stable).
    LargestFirst,
    /// Deterministic shuffle, seeded from the spec text.
    Random,
}

impl fmt::Display for PartitionOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            PartitionOrder::Unchanged => "unchanged",
            PartitionOrder::SmallestFirst => "smallest_first",
            PartitionOrder::LargestFirst => "largest_first",
            PartitionOrder::Random => "random",
        };
        write!(f, "{s}")
    }
}

/// The chunking part of a spec — the shape that carves a domain
/// into partitions. Two shapes:
///
/// - [`Chunking::SingleRange`] — Form 1: an explicit
///   `start..end` interval. Always exactly one partition at
///   resolve time.
/// - [`Chunking::DeltaList`] — Forms 2 and 3: an ordered list
///   of per-partition delta sizes, walked left-to-right from
///   the domain's start. Pre-baked recipes (`bin:5`, `fib:7`,
///   etc.) parse into normalised percentage deltas.
#[derive(Debug, Clone, PartialEq)]
pub enum Chunking {
    /// `start..end` form. Single partition spanning the named
    /// boundary, regardless of either endpoint's `Bound` kind.
    SingleRange {
        start: Bound,
        end: Bound,
    },
    /// Comma-separated delta list. Each entry is the delta
    /// from the running start; a single tail token is allowed
    /// per list and resolves against whatever's left after the
    /// sized deltas are applied: [`Bound::Star`] takes it as
    /// one partition, [`Bound::Fill`] repeats the preceding
    /// delta until the extent is used up, [`Bound::StarSplit`]
    /// divides it into `n` near-equal partitions,
    /// [`Bound::StarShaped`] divides it by recipe weights.
    /// [`Bound::Gap`] entries consume extent without emitting.
    /// Deltas summing to less than the extent (without a tail
    /// token) drop the trailing gap; summing to more is a
    /// resolve-time error.
    DeltaList {
        deltas: Vec<Bound>,
    },
}

/// Parsed `cursor=...` argument:
///
/// ```text
/// spec   := chunking [ "in" window ] [ order ]
/// window := Form 1 range (e.g. `25%..75%`)
/// order  := unchanged | smallest_first | largest_first | random
/// ```
///
/// The window scopes the chunking: the chunking resolves
/// against the window's ordinal range instead of the full
/// extent, so percentages inside the chunking are relative to
/// the **window**. Without a window, the chunking spans the
/// whole extent. The order keyword reorders the resolved list
/// for iteration; partition `idx` keeps identifying the
/// generation position.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSpec {
    /// The shape that carves the (windowed) domain.
    pub chunking: Chunking,
    /// Optional `in start..end` window the chunking applies to.
    pub window: Option<(Bound, Bound)>,
    /// Iteration order of the resolved list.
    pub order: PartitionOrder,
}

impl PartitionSpec {
    /// A windowless, unordered Form 1 spec.
    pub fn single_range(start: Bound, end: Bound) -> Self {
        Self {
            chunking: Chunking::SingleRange { start, end },
            window: None,
            order: PartitionOrder::Unchanged,
        }
    }

    /// A windowless, unordered Form 2/3 spec.
    pub fn delta_list(deltas: Vec<Bound>) -> Self {
        Self {
            chunking: Chunking::DeltaList { deltas },
            window: None,
            order: PartitionOrder::Unchanged,
        }
    }
}

/// A single resolved partition: an absolute ordinal range with
/// derived percentage and index metadata.
///
/// `cardinality()` returns `end_ord - start_ord` — the number
/// of ordinals the partition covers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Partition {
    /// 0-based position in the resolved partition list
    /// (generation order — stable under spec-level reordering).
    pub idx: u64,
    /// Total number of partitions in the list this one was
    /// resolved as part of. `1` for a single-partition spec.
    /// Carried per-partition so an iter-var or `q.cursor`
    /// projection can answer `partition_count` (and the status
    /// banner can render `i/n`) without the originating list.
    pub count: u64,
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
///   `1000,5000,*`, `1000,10%,*`, `20%,30%`. Tail tokens:
///   `*` (remainder as one partition), `...` (repeat the
///   preceding delta until the extent is used up, e.g.
///   `90%,1%,...`), `*/N` (remainder divided into N equal
///   partitions, e.g. `90%,*/10`), `*/recipe:args` (remainder
///   shaped by recipe weights, e.g. `90%,*/fib:5`). Entry
///   modifiers: `<delta>xN` finite repetition (`1%x5` = five 1%
///   chunks), `~<delta>` gap (`~10%` consumes 10% of the extent
///   without emitting a partition).
/// - Form 3 — pre-baked recipe: `linear:N`, `ratios:a,b,c,…`,
///   `mul:R`, `mul:S,R`, `bin:N`, `fib:N`, `ln:N`, `geom:N,R`,
///   `zipf:s,N`, `pareto:alpha,N`, `front_heavy:N`,
///   `back_heavy:N`.
///
/// The whole spec follows the token grammar
/// `chunking [in window] [order]` — a whitespace-delimited `in`
/// scopes the chunking to a Form 1 window (`linear:5 in
/// 25%..75%`), and a trailing order keyword (`unchanged` /
/// `smallest_first` / `largest_first` / `random`) reorders the
/// resolved list for iteration (`fib:5 largest_first`).
///
/// Whitespace is otherwise ignored. Bracket characters (`[`,
/// `]`, `(`, `)`) are stripped unconditionally — they're
/// advisory closure markers in the grammar (everything's always
/// `[start, end)` at resolve time), so any placement parses the
/// same way.
pub fn parse(input: &str) -> Result<PartitionSpec, String> {
    // Token phase: `chunking [in window] [order]`. Whitespace
    // splits tokens; within the chunking / window parts it
    // carries no meaning and the parts are re-joined.
    let mut tokens: Vec<&str> = input.split_whitespace().collect();
    if tokens.is_empty() {
        return Err(format!("empty spec: `{input}`"));
    }
    // Order suffix: a trailing bare-word token (alphabetic plus
    // `_`; a spec body never ends in a bare word — recipes
    // carry `:`).
    let mut order = PartitionOrder::Unchanged;
    if tokens.len() >= 2 {
        let last = *tokens.last().unwrap();
        if !last.is_empty()
            && last.chars().all(|c| c.is_ascii_alphabetic() || c == '_')
            && last != "in"
        {
            order = match last {
                "unchanged" => PartitionOrder::Unchanged,
                "smallest_first" => PartitionOrder::SmallestFirst,
                "largest_first" => PartitionOrder::LargestFirst,
                "random" => PartitionOrder::Random,
                // Size sorts are named for their axis: a bare
                // direction word is ambiguous between ordinal
                // position (always the generation order) and
                // size, so teach the unambiguous spelling.
                "ascending" => {
                    return Err(
                        "`ascending`: partition order sorts key on partition SIZE, \
                         not ordinal position (position order is always the \
                         generation order — that's `unchanged`). Spell it \
                         `smallest_first`"
                            .into(),
                    );
                }
                "descending" => {
                    return Err(
                        "`descending`: partition order sorts key on partition SIZE, \
                         not ordinal position (position order is always the \
                         generation order — that's `unchanged`). Spell it \
                         `largest_first`"
                            .into(),
                    );
                }
                other => {
                    return Err(format!(
                        "unknown order `{other}` — supported: unchanged, \
                         smallest_first, largest_first, random"
                    ));
                }
            };
            tokens.pop();
        }
    }
    // Window clause: a standalone `in` token splits chunking
    // from window.
    let in_positions: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter_map(|(i, t)| (*t == "in").then_some(i))
        .collect();
    let (chunk_tokens, window_tokens): (&[&str], Option<&[&str]>) = match in_positions.as_slice() {
        [] => (&tokens[..], None),
        [i] => {
            if *i == 0 {
                return Err(format!("`in` without a chunking spec before it: `{input}`"));
            }
            if *i == tokens.len() - 1 {
                return Err(format!("`in` without a window range after it: `{input}`"));
            }
            (&tokens[..*i], Some(&tokens[*i + 1..]))
        }
        _ => {
            return Err(format!(
                "at most one `in <window>` clause is allowed: `{input}`"
            ));
        }
    };
    let window = match window_tokens {
        None => None,
        Some(wt) => Some(parse_window(&clean_part(wt), input)?),
    };
    let chunking = parse_chunking(&clean_part(chunk_tokens), input)?;
    Ok(PartitionSpec { chunking, window, order })
}

/// Re-join part tokens and strip the advisory bracket markers.
fn clean_part(tokens: &[&str]) -> String {
    tokens
        .concat()
        .chars()
        .filter(|c| !matches!(c, '[' | ']' | '(' | ')'))
        .collect()
}

/// Parse the window of an `in` clause: a Form 1 range with
/// sized endpoints.
fn parse_window(cleaned: &str, input: &str) -> Result<(Bound, Bound), String> {
    let Some((lhs, rhs)) = split_range(cleaned) else {
        return Err(format!(
            "the window after `in` must be a `start..end` range; got `{cleaned}` in `{input}`"
        ));
    };
    let start = parse_bound(lhs)?;
    let end = parse_bound(rhs)?;
    if !start.is_sized() || !end.is_sized() {
        return Err(format!(
            "window endpoints must be sized values (percentage, fraction, or \
             ordinal); got `{cleaned}` in `{input}`"
        ));
    }
    Ok((start, end))
}

/// Parse the chunking part of a spec (everything except the
/// `in` window and order suffix).
fn parse_chunking(cleaned: &str, input: &str) -> Result<Chunking, String> {
    if cleaned.is_empty() {
        return Err(format!("empty spec: `{input}`"));
    }
    // Form 3: pre-baked recipe — `name:args` with an alphabetic
    // name.
    if let Some((name, args)) = split_recipe(cleaned) {
        let deltas = normalise_to_pct(&expand_recipe_weights(name, args)?)?;
        return Ok(Chunking::DeltaList { deltas });
    }
    // A lone fill token has nothing to repeat. Caught before the
    // Form 1 check because `...` contains the `..` range marker.
    if cleaned == "..." {
        return Err(
            "the fill token `...` repeats the preceding delta until the extent \
             is used up; it needs at least one delta before it (e.g. `1%,...`)"
                .into(),
        );
    }
    // The star tail (`*/...`) may carry a recipe whose args
    // contain commas, so it is split off before the comma walk.
    // Tail tokens must be last, which is what makes this split
    // unambiguous.
    if cleaned.starts_with("*/") || cleaned.contains(",*/") {
        return parse_delta_list(cleaned, input);
    }
    // Form 2: delta list — any comma makes it a list. Checked
    // before Form 1 because a `...` fill entry contains the `..`
    // range marker.
    if cleaned.contains(',') {
        return parse_delta_list(cleaned, input);
    }
    // Form 1: single sub-range — contains `..`.
    if let Some((lhs, rhs)) = split_range(cleaned) {
        let start = parse_bound(lhs)?;
        let end = parse_bound(rhs)?;
        // Form 1 doesn't allow the list tail tokens.
        if !start.is_sized() || !end.is_sized() {
            return Err(format!(
                "`*`, `...`, `~`, and `*/N` are only valid inside a comma-separated \
                 delta list, not a `..` range; got `{input}`"
            ));
        }
        return Ok(Chunking::SingleRange { start, end });
    }
    // Single-entry delta list (`50%`, `*`, `1%x5`, ...).
    parse_delta_list(cleaned, input)
}

/// Parse a comma-separated Form 2 delta list and validate its
/// grammar: at most one tail token (`*` / `...` / `*/N` /
/// `*/recipe`) per list; `...`, `*/N`, and `*/recipe` must be
/// the final entry (`*` may sit anywhere); `...` needs a
/// preceding sized, non-gap delta; at least one entry must emit
/// a partition.
fn parse_delta_list(cleaned: &str, input: &str) -> Result<Chunking, String> {
    // Split off a star tail first — its recipe args may contain
    // commas (`*/ratios:1,3`).
    let (head, star_tail) = if let Some(rest) = cleaned.strip_prefix("*/") {
        ("", Some(rest))
    } else if let Some(pos) = cleaned.find(",*/") {
        (&cleaned[..pos], Some(&cleaned[pos + 3..]))
    } else {
        (cleaned, None)
    };
    let mut deltas: Vec<Bound> = Vec::new();
    if !head.is_empty() {
        for entry in head.split(',') {
            if entry.is_empty() {
                return Err(format!("empty entry in delta list: `{input}`"));
            }
            deltas.extend(parse_delta_entry(entry)?);
        }
    } else if star_tail.is_none() {
        return Err(format!("empty spec: `{input}`"));
    }
    if let Some(tail) = star_tail {
        deltas.push(parse_star_tail(tail)?);
    }
    let tail_count = deltas.iter().filter(|b| b.is_tail()).count();
    if tail_count > 1 {
        return Err(format!(
            "at most one remainder token (`*`, `...`, `*/N`, or `*/recipe`) is \
             allowed in a delta list; got {tail_count} in `{input}`"
        ));
    }
    if let Some(pos) = deltas
        .iter()
        .position(|b| matches!(b, Bound::Fill | Bound::StarSplit(_) | Bound::StarShaped(_)))
    {
        if pos != deltas.len() - 1 {
            return Err(format!(
                "`{}` consumes the rest of the extent and must be the last entry \
                 in the delta list; got `{input}`",
                deltas[pos]
            ));
        }
        if matches!(deltas[pos], Bound::Fill) {
            if pos == 0 {
                return Err(
                    "the fill token `...` repeats the preceding delta until the extent \
                     is used up; it needs at least one delta before it (e.g. `1%,...`)"
                        .into(),
                );
            }
            if matches!(deltas[pos - 1], Bound::Gap(_)) {
                return Err(format!(
                    "`...` after a gap would emit nothing — the fill token repeats \
                     the immediately preceding delta. Put a sized delta before `...`, \
                     in `{input}`"
                ));
            }
        }
    }
    // A spec that never emits is a mistake, not an empty sweep.
    if !deltas.iter().any(|b| b.is_sized() || b.is_tail()) {
        return Err(format!(
            "spec emits no partitions — every entry is a gap: `{input}`"
        ));
    }
    Ok(Chunking::DeltaList { deltas })
}

/// Parse one head entry of a delta list: a sized bound, `*`,
/// `...`, a gap (`~<sized>`), or a finite repetition
/// (`<sized>xN`).
fn parse_delta_entry(raw: &str) -> Result<Vec<Bound>, String> {
    // Gap prefix: `~<sized>`.
    if let Some(rest) = raw.strip_prefix('~') {
        if let Some((_, rep)) = rest.split_once('x') {
            if !rep.is_empty() && rep.chars().all(|c| c.is_ascii_digit()) {
                return Err(format!(
                    "`~{rest}`: repetition does not apply to gaps — size the gap \
                     directly (adjacent gaps are one gap)"
                ));
            }
        }
        let inner = parse_bound(rest)?;
        if !inner.is_sized() {
            return Err(format!(
                "`~{rest}`: a gap requires a sized value (percentage, fraction, or \
                 ordinal). To ignore the trailing remainder, just end the list \
                 without a tail token — under-summing lists drop the gap"
            ));
        }
        return Ok(vec![Bound::Gap(Box::new(inner))]);
    }
    // Finite repetition: `<sized>xN`.
    if let Some((lhs, rhs)) = raw.split_once('x') {
        if !lhs.is_empty() && !rhs.is_empty() && rhs.chars().all(|c| c.is_ascii_digit()) {
            let n: u64 = rhs
                .parse()
                .map_err(|_| format!("invalid repetition count in `{raw}`"))?;
            if n == 0 {
                return Err(format!(
                    "`{raw}`: the repetition count must be >= 1"
                ));
            }
            let b = parse_bound(lhs)?;
            if !b.is_sized() {
                return Err(format!(
                    "`{raw}`: repetition applies to sized deltas (percentage, \
                     fraction, or ordinal) only"
                ));
            }
            return Ok(vec![b; n as usize]);
        }
    }
    Ok(vec![parse_bound(raw)?])
}

/// Parse the divisor of a star tail (the text after `*/`):
/// a bare integer count (`*/10`) or a recipe (`*/fib:5`).
fn parse_star_tail(divisor: &str) -> Result<Bound, String> {
    // A comma in a non-recipe divisor means entries follow the
    // star tail — recipes own their comma-separated args, a bare
    // count doesn't.
    if !divisor.contains(':') && divisor.contains(',') {
        let count = divisor.split(',').next().unwrap_or(divisor);
        return Err(format!(
            "`*/{count}` consumes the rest of the extent and must be the last \
             entry in the delta list; got trailing entries after it"
        ));
    }
    if let Some((name, args)) = split_recipe(divisor) {
        if name == "linear" {
            return Err(format!(
                "`*/linear:{args}`: spell an equal-count remainder split as \
                 `*/{args}` — `*/N` is the canonical form"
            ));
        }
        let weights = normalise_weights(&expand_recipe_weights(name, args)?)?;
        return Ok(Bound::StarShaped(weights));
    }
    if divisor.contains('%') || divisor.contains('.') {
        return Err(format!(
            "`*/{divisor}`: the divisor after `*/` is a chunk count and must be a \
             bare integer (e.g. `*/10` = remainder in 10 equal chunks). For \
             fixed-size chunks repeated until the extent is used up, spell the \
             size as a delta followed by the fill token: `{divisor},...`"
        ));
    }
    let n: u64 = divisor.parse().map_err(|_| {
        format!("invalid remainder split `*/{divisor}`: expected `*/N` with integer N >= 1, or `*/recipe:args`")
    })?;
    if n == 0 {
        return Err("`*/0`: the remainder split count must be >= 1".into());
    }
    Ok(Bound::StarSplit(n))
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
    // Fill token: `...` — repeat the preceding delta.
    if s == "..." {
        return Ok(Bound::Fill);
    }
    // Star tails (`*/N`, `*/recipe`) are parsed by
    // [`parse_star_tail`] — the delta-list walk splits them off
    // before reaching here, so a `*/` reaching this point is a
    // misplacement (e.g. a Form 1 endpoint) and falls through to
    // the number-parse error below.
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

/// Dispatch a recipe name + arg string to its raw weight list.
/// Callers normalise: [`normalise_to_pct`] for a whole-spec
/// recipe (Form 3), [`normalise_weights`] for a star tail
/// (`*/recipe`).
fn expand_recipe_weights(name: &str, args: &str) -> Result<Vec<f64>, String> {
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
    Ok(weights)
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

/// Normalise raw recipe weights so they sum to 100. Weights
/// must be non-negative and have a positive sum.
fn normalise_weights(weights: &[f64]) -> Result<Vec<f64>, String> {
    if weights.iter().any(|w| !w.is_finite() || *w < 0.0) {
        return Err("recipe produced non-finite or negative weights".into());
    }
    let sum: f64 = weights.iter().sum();
    if sum <= 0.0 {
        return Err("recipe produced zero total weight".into());
    }
    Ok(weights.iter().map(|w| w / sum * 100.0).collect())
}

/// Normalise raw recipe weights to percentage deltas summing
/// to 100%.
fn normalise_to_pct(weights: &[f64]) -> Result<Vec<Bound>, String> {
    Ok(normalise_weights(weights)?.into_iter().map(Bound::Pct).collect())
}

// =========================================================================
// Resolution
// =========================================================================

/// Resolve a [`PartitionSpec`] against a cursor's base extent
/// `[base_start, base_end)`, producing a list of concrete
/// [`Partition`]s with absolute ordinals.
///
/// An `in` window narrows the domain first: the window's range
/// resolves against the full extent, then the chunking resolves
/// against the window (percentages inside the chunking are
/// window-relative, and the resulting partitions' `base_extent`
/// / pct fields are window-based).
///
/// For [`Chunking::SingleRange`] the result is always a
/// 1-element vector.
///
/// For [`Chunking::DeltaList`] the deltas are walked
/// left-to-right. Gap entries consume extent without emitting.
/// Tail tokens consume the unallocated remainder: `Bound::Star`
/// absorbs it as one partition, `Bound::Fill` repeats the
/// preceding delta until the domain end (final chunk truncated,
/// never dropped), `Bound::StarSplit(n)` divides it into `n`
/// near-equal partitions, and `Bound::StarShaped(weights)`
/// divides it by recipe weights. A sized-delta sum exceeding
/// the extent is a hard error.
///
/// Finally, the spec's [`PartitionOrder`] reorders the list for
/// iteration; `idx` keeps identifying the generation position.
///
/// **Frames:** the window affects *sizing and placement* only —
/// percentages inside the chunking are window-relative when
/// computing boundaries. The resulting partitions' `start_pct` /
/// `end_pct` / `base_extent` are always labelled against the
/// **full base frame**, so a windowed partition re-projected
/// onto another extent (the `over` clause's cross-extent
/// contract) keeps its position in the whole domain instead of
/// collapsing the window offset.
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
    let base_extent = base_end - base_start;
    // Window: narrow the domain the chunking applies to.
    let (dom_start, dom_end) = match &spec.window {
        None => (base_start, base_end),
        Some((ws, we)) => {
            let s = ws
                .resolve_against(base_start, base_end)
                .expect("window bounds are sized (checked at parse time)");
            let e = we
                .resolve_against(base_start, base_end)
                .expect("window bounds are sized (checked at parse time)");
            if e < s {
                return Err(format!(
                    "window `in {ws}..{we}` is empty or reversed against \
                     base=[{base_start}..{base_end}): start={s}, end={e}"
                ));
            }
            (s, e)
        }
    };
    let dom_extent = dom_end - dom_start;
    // Labelling frame: pct fields and base_extent always
    // describe the full base, regardless of the window.
    let frame = Frame { base_start, base_extent };
    let mut partitions = match &spec.chunking {
        Chunking::SingleRange { start, end } => {
            let start_ord = start
                .resolve_against(dom_start, dom_end)
                .expect("tail tokens not allowed in SingleRange (checked at parse time)");
            let end_ord = end
                .resolve_against(dom_start, dom_end)
                .expect("tail tokens not allowed in SingleRange (checked at parse time)");
            if end_ord < start_ord {
                return Err(format!(
                    "resolved range is empty or reversed: start={start_ord}, end={end_ord} \
                     (spec start={start}, end={end}, base=[{dom_start}..{dom_end}))"
                ));
            }
            // A Form 1 range is an operator-explicit slice; one
            // that rounds to zero ordinals would silently run
            // nothing — the "why did this do nothing" trap.
            // (Delta lists are NOT held to this: auto-
            // terminating recipes like `mul:0.5` legitimately
            // produce sub-ordinal tail weights on small extents,
            // and their zero-width entries iterate zero cycles
            // by correct arithmetic. The tail tokens carry their
            // own non-empty guards.)
            if start_ord == end_ord {
                return Err(format!(
                    "range `{start}..{end}` resolves to zero ordinals \
                     ([{start_ord}..{end_ord}) against base=[{dom_start}..{dom_end})) — \
                     the slice rounds to nothing at this extent; widen the range \
                     or use a larger extent"
                ));
            }
            vec![frame.partition(0, start_ord, end_ord)]
        }
        Chunking::DeltaList { deltas } => {
            resolve_delta_list(deltas, dom_start, dom_end, dom_extent, frame)?
        }
    };
    // Patch the sibling count now that the list is complete.
    let count = partitions.len() as u64;
    for p in &mut partitions {
        p.count = count;
    }
    apply_order(&mut partitions, spec);
    Ok(partitions)
}

/// The labelling frame for resolved partitions: pct fields and
/// `base_extent` always describe the cursor's full base, even
/// when a window narrows where the chunking lands.
#[derive(Clone, Copy)]
struct Frame {
    base_start: u64,
    base_extent: u64,
}

impl Frame {
    fn partition(&self, idx: u64, start_ord: u64, end_ord: u64) -> Partition {
        // `count` is patched in one post-pass once the full list
        // is built (see `resolve`).
        Partition {
            count: 0,
            idx,
            start_ord,
            end_ord,
            start_pct: pct_of(start_ord, self.base_start, self.base_extent),
            end_pct: pct_of(end_ord, self.base_start, self.base_extent),
            base_extent: self.base_extent,
        }
    }
}

/// Reorder a resolved partition list per the spec's order
/// keyword. `SmallestFirst` / `LargestFirst` sort by
/// **cardinality** (stable — equal-sized partitions keep their
/// generation order); `Random` is a deterministic Fisher–Yates
/// shuffle seeded from the spec text, so the same spec yields
/// the same order on every run. `idx` values are not
/// reassigned — they keep identifying the generation position.
fn apply_order(partitions: &mut [Partition], spec: &PartitionSpec) {
    match spec.order {
        PartitionOrder::Unchanged => {}
        PartitionOrder::SmallestFirst => {
            partitions.sort_by_key(|p| p.cardinality());
        }
        PartitionOrder::LargestFirst => {
            partitions.sort_by_key(|p| std::cmp::Reverse(p.cardinality()));
        }
        PartitionOrder::Random => {
            let mut state = xxhash_rust::xxh3::xxh3_64(format!("{spec:?}").as_bytes());
            for i in (1..partitions.len()).rev() {
                let j = (splitmix64(&mut state) % (i as u64 + 1)) as usize;
                partitions.swap(i, j);
            }
        }
    }
}

/// SplitMix64 step — the deterministic stream behind
/// [`PartitionOrder::Random`].
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn resolve_delta_list(
    deltas: &[Bound],
    dom_start: u64,
    dom_end: u64,
    extent: u64,
    frame: Frame,
) -> Result<Vec<Partition>, String> {
    // One boundary rule everywhere: every partition boundary is
    // the *exact* cumulative position, rounded once. Sizes are
    // boundary differences. This keeps rounding slack from
    // accumulating across entries (`linear:3` over 1000 yields
    // 333/334/333 covering the extent exactly, not 333/333/333
    // with a silently dropped ordinal) and matches Form 1's
    // position-rounding and `split_evenly`'s boundary math.
    //
    // `extent` here is the (possibly windowed) domain the deltas
    // size against; `frame` is the full-base labelling frame.
    let non_tail_exact: f64 = deltas
        .iter()
        .filter(|b| !b.is_tail())
        .map(|b| delta_exact_ordinals(b, extent))
        .sum();
    // Float-noise tolerance: a list like `90%,1%,…(×10)` may sum
    // to 100% plus epsilon; only reject genuine overshoot.
    let tolerance = 1e-6 * (extent as f64).max(1.0);
    if non_tail_exact > extent as f64 + tolerance {
        return Err(format!(
            "delta list sums to {} ordinals, exceeding the cursor's extent {extent}; \
             trim the list or use a `*` remainder to absorb the overflow",
            non_tail_exact.round() as u64
        ));
    }
    let mut partitions: Vec<Partition> = Vec::with_capacity(deltas.len());
    let mut cursor = dom_start;
    // Exact running position, in ordinals relative to dom_start.
    let mut exact_pos = 0.0f64;
    let push = |partitions: &mut Vec<Partition>, start: u64, end: u64| {
        let idx = partitions.len() as u64;
        partitions.push(frame.partition(idx, start, end));
    };
    let boundary = |exact_pos: f64| -> u64 {
        (dom_start + exact_pos.round() as u64).min(dom_end)
    };
    for (i, delta) in deltas.iter().enumerate() {
        match delta {
            Bound::Star => {
                // Absorb exactly what the sized deltas leave.
                exact_pos += extent as f64 - non_tail_exact;
                let next = boundary(exact_pos);
                push(&mut partitions, cursor, next);
                cursor = next;
            }
            Bound::Fill => {
                // Parse guarantees Fill is last with a sized
                // delta before it; repeat that delta's exact
                // size until the extent is used up. The final
                // chunk truncates at `dom_end` — emitted
                // short, never dropped.
                let chunk = delta_exact_ordinals(&deltas[i - 1], extent);
                if chunk < 1.0 {
                    return Err(format!(
                        "fill token `...` would repeat a delta of less than one \
                         ordinal (`{}` resolves to {chunk:.3} ordinals against \
                         extent {extent})",
                        deltas[i - 1]
                    ));
                }
                while cursor < dom_end {
                    exact_pos += chunk;
                    let next = boundary(exact_pos);
                    push(&mut partitions, cursor, next);
                    cursor = next;
                }
            }
            Bound::StarSplit(n) => {
                // Parse guarantees StarSplit is last; what's
                // left of the extent is split into n near-equal
                // partitions.
                let remainder = dom_end - cursor;
                if remainder == 0 {
                    return Err(format!(
                        "`*/{n}` has no remainder to divide — the preceding deltas \
                         already cover the extent {extent}"
                    ));
                }
                if *n > remainder {
                    return Err(format!(
                        "`*/{n}` cannot divide a remainder of {remainder} ordinals \
                         into {n} non-empty partitions"
                    ));
                }
                for (s, e) in split_evenly(cursor, dom_end, *n) {
                    push(&mut partitions, s, e);
                }
                cursor = dom_end;
            }
            Bound::StarShaped(weights) => {
                // Parse guarantees StarShaped is last; what's
                // left of the extent is divided by the recipe's
                // normalised weights (cumulative-position
                // rounding, same rule as everything else).
                let remainder = dom_end - cursor;
                if remainder == 0 {
                    return Err(format!(
                        "`*/<recipe>` has no remainder to divide — the preceding \
                         deltas already cover the extent {extent}"
                    ));
                }
                let start = cursor;
                let mut cum = 0.0f64;
                for w in weights {
                    cum += w;
                    let next = (start + ((cum / 100.0) * remainder as f64).round() as u64)
                        .min(dom_end);
                    if next == cursor {
                        return Err(format!(
                            "`*/<recipe>` produces an empty partition — weight \
                             {w:.3}% of a {remainder}-ordinal remainder rounds to \
                             zero ordinals; use fewer/coarser weights or a larger \
                             remainder"
                        ));
                    }
                    push(&mut partitions, cursor, next);
                    cursor = next;
                }
                exact_pos += remainder as f64;
            }
            Bound::Gap(inner) => {
                // Consume the gap's extent without emitting a
                // partition. The walk stays contiguous; the
                // emitted set skips this range.
                exact_pos += delta_exact_ordinals(inner, extent);
                cursor = boundary(exact_pos);
            }
            other => {
                exact_pos += delta_exact_ordinals(other, extent);
                let next = boundary(exact_pos);
                push(&mut partitions, cursor, next);
                cursor = next;
            }
        }
    }
    // Trailing-gap policy: deltas summing to less than the
    // extent (without a tail token) drop the gap. `cursor` may
    // end short of `dom_end` — that's intentional, not an error.
    debug_assert!(cursor <= dom_end);
    Ok(partitions)
}

/// Convert a delta `Bound` to its *exact* size in ordinals
/// against an extent (unrounded — boundaries round once, at the
/// cumulative position). A gap's size is its wrapped bound's.
/// Tail tokens are the caller's responsibility (their sizes
/// depend on the sized deltas).
fn delta_exact_ordinals(b: &Bound, extent: u64) -> f64 {
    match b {
        Bound::Pct(p) => (p / 100.0) * extent as f64,
        Bound::Frac(f) => f * extent as f64,
        Bound::Ord(o) => *o as f64,
        Bound::Gap(inner) => delta_exact_ordinals(inner, extent),
        Bound::Star | Bound::Fill | Bound::StarSplit(_) | Bound::StarShaped(_) => {
            unreachable!("tail tokens handled separately")
        }
    }
}

/// Split a resolved [`Partition`] into `n` contiguous
/// sub-partitions whose sizes differ by at most one ordinal —
/// the value-level form of the `*/N` spec token (identical
/// boundary math via [`split_evenly`]). Indices restart at 0,
/// `count` is `n`, `base_extent` propagates, and the pct fields
/// interpolate the parent's span.
///
/// Errors when `n` is 0 or exceeds the partition's cardinality
/// (every sub-partition must be non-empty). This is the shared
/// engine behind the `subdivide(p, n)` stdlib node (which
/// panics per node convention) and the comprehension-source
/// form (`for: "inner in subdivide(outer, n)"`, which surfaces
/// the error as a clause diagnostic).
pub fn subdivide_partition(p: &Partition, n: u64) -> Result<Vec<Partition>, String> {
    let card = p.cardinality();
    if n == 0 {
        return Err("subdivide(p, 0): the sub-partition count must be >= 1".into());
    }
    if n > card {
        return Err(format!(
            "subdivide(p, {n}): cannot divide partition #{} of {card} ordinals \
             into {n} non-empty sub-partitions",
            p.idx
        ));
    }
    let pct_at = |ord: u64| -> f64 {
        p.start_pct
            + (ord - p.start_ord) as f64 / card as f64 * (p.end_pct - p.start_pct)
    };
    Ok(split_evenly(p.start_ord, p.end_ord, n)
        .into_iter()
        .enumerate()
        .map(|(i, (start_ord, end_ord))| Partition {
            idx: i as u64,
            count: n,
            start_ord,
            end_ord,
            start_pct: pct_at(start_ord),
            end_pct: pct_at(end_ord),
            base_extent: p.base_extent,
        })
        .collect())
}

/// Split `[start_ord, end_ord)` into `n` contiguous half-open
/// chunks whose sizes differ by at most one ordinal. Boundary
/// `i` sits at `start_ord + round(i * span / n)`, so the
/// rounding slack is distributed across the chunks rather than
/// accumulating in the last one. `n` must be >= 1.
///
/// Shared between the `*/N` spec tail token and the
/// `subdivide(p, n)` stdlib node so both produce identical
/// boundaries.
pub fn split_evenly(start_ord: u64, end_ord: u64, n: u64) -> Vec<(u64, u64)> {
    debug_assert!(n >= 1, "split_evenly requires n >= 1");
    debug_assert!(end_ord >= start_ord);
    let span = (end_ord - start_ord) as u128;
    let n_wide = n as u128;
    let boundary = |i: u64| -> u64 {
        start_ord + ((i as u128 * span + n_wide / 2) / n_wide) as u64
    };
    (0..n).map(|i| (boundary(i), boundary(i + 1))).collect()
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
// Polydat Value integration
// =========================================================================
//
// Partition and PartitionSpec carry through Polydat wires as
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
            self.idx, self.count,
            self.start_ord, self.end_ord, self.start_pct, self.end_pct,
        )
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "idx":         self.idx,
            "count":       self.count,
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
        let chunking = match &self.chunking {
            Chunking::SingleRange { start, end } => format!("{start}..{end}"),
            Chunking::DeltaList { deltas } => {
                let parts: Vec<String> = deltas.iter().map(|b| b.to_string()).collect();
                parts.join(",")
            }
        };
        let window = match &self.window {
            Some((s, e)) => format!(" in {s}..{e}"),
            None => String::new(),
        };
        let order = match self.order {
            PartitionOrder::Unchanged => String::new(),
            o => format!(" {o}"),
        };
        format!("PartitionSpec({chunking}{window}{order})")
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::String(self.display())
    }

    fn as_any(&self) -> &dyn std::any::Any { self }

    fn clone_reflected(&self) -> Box<dyn ReflectedValue> {
        Box::new(self.clone())
    }
}

/// A list of resolved partitions carried as a single Polydat value.
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
    /// Wrap a [`Partition`] as a Polydat `Value::Ext`.
    pub fn from_partition(p: Partition) -> Self {
        Value::Ext(Box::new(p))
    }

    /// Wrap a [`PartitionSpec`] as a Polydat `Value::Ext`.
    pub fn from_partition_spec(s: PartitionSpec) -> Self {
        Value::Ext(Box::new(s))
    }

    /// Wrap a `Vec<Partition>` as a Polydat `Value::Ext` via
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
            PartitionSpec::single_range(Bound::Ord(0), Bound::Pct(53.0))
        );
    }

    #[test]
    fn parse_form1_brackets_tolerated() {
        let canonical = PartitionSpec::single_range(Bound::Ord(0), Bound::Pct(53.0));
        assert_eq!(parse("[0..53%]").unwrap(), canonical);
        assert_eq!(parse("[0..53%)").unwrap(), canonical);
        assert_eq!(parse("(0..53%]").unwrap(), canonical);
    }

    #[test]
    fn parse_form1_fraction_form() {
        let spec = parse("0..0.53").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::single_range(Bound::Ord(0), Bound::Frac(0.53))
        );
    }

    #[test]
    fn parse_form1_literal_ordinals() {
        let spec = parse("100..1000").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::single_range(Bound::Ord(100), Bound::Ord(1000))
        );
    }

    #[test]
    fn parse_form1_mixed_literal_and_pct() {
        let spec = parse("100..50%").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::single_range(Bound::Ord(100), Bound::Pct(50.0))
        );
    }

    #[test]
    fn parse_form1_mixed_frac_and_literal() {
        let spec = parse("0.10..10000").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::single_range(Bound::Frac(0.10), Bound::Ord(10000))
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
            PartitionSpec::delta_list(vec![Bound::Pct(2.0), Bound::Pct(10.0), Bound::Star])
        );
    }

    #[test]
    fn parse_form2_fraction_equivalent() {
        let spec = parse("0.02,0.10,*").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Frac(0.02), Bound::Frac(0.10), Bound::Star])
        );
    }

    #[test]
    fn parse_form2_literal_deltas() {
        let spec = parse("1000,5000,*").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Ord(1000), Bound::Ord(5000), Bound::Star])
        );
    }

    #[test]
    fn parse_form2_mixed_entries() {
        let spec = parse("1000,10%,*").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Ord(1000), Bound::Pct(10.0), Bound::Star])
        );
    }

    #[test]
    fn parse_form2_short_list_no_star() {
        let spec = parse("20%,30%").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Pct(20.0), Bound::Pct(30.0)])
        );
    }

    #[test]
    fn parse_form2_rejects_multiple_stars() {
        let err = parse("*,*").unwrap_err();
        assert!(err.contains("at most one"), "diagnostic: {err}");
    }

    // ── Form 2 tail tokens: `...` fill and `*/N` split ─────

    #[test]
    fn parse_form2_fill_token() {
        let spec = parse("90%,1%,...").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Pct(90.0), Bound::Pct(1.0), Bound::Fill])
        );
    }

    #[test]
    fn parse_form2_star_split_token() {
        let spec = parse("90%,*/10").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Pct(90.0), Bound::StarSplit(10)])
        );
    }

    #[test]
    fn parse_star_split_alone_is_whole_extent_split() {
        // Degenerate no-head case: the remainder is everything.
        let spec = parse("*/16").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::StarSplit(16)])
        );
    }

    #[test]
    fn parse_fill_alone_rejected_with_hint() {
        let err = parse("...").unwrap_err();
        assert!(err.contains("preceding delta") || err.contains("before it"), "diagnostic: {err}");
    }

    #[test]
    fn parse_fill_first_in_list_rejected() {
        let err = parse("...,10%").unwrap_err();
        assert!(err.contains("before it") || err.contains("last entry"), "diagnostic: {err}");
    }

    #[test]
    fn parse_fill_not_last_rejected() {
        let err = parse("1%,...,10%").unwrap_err();
        assert!(err.contains("last entry"), "diagnostic: {err}");
    }

    #[test]
    fn parse_star_split_not_last_rejected() {
        let err = parse("*/4,10%").unwrap_err();
        assert!(err.contains("last entry"), "diagnostic: {err}");
    }

    #[test]
    fn parse_rejects_mixed_tail_tokens() {
        let err = parse("1%,*,...").unwrap_err();
        assert!(err.contains("at most one"), "diagnostic: {err}");
        let err = parse("1%,*,*/4").unwrap_err();
        assert!(err.contains("at most one"), "diagnostic: {err}");
    }

    #[test]
    fn parse_star_split_pct_divisor_rejected_with_teaching_hint() {
        // `*/1%` is the chunk-SIZE reading; one canonical
        // spelling for that exists (`1%,...`), and the
        // diagnostic must point at it.
        let err = parse("90%,*/1%").unwrap_err();
        assert!(err.contains("chunk count"), "diagnostic: {err}");
        assert!(err.contains("1%,..."), "diagnostic should teach the fill form: {err}");
        let err = parse("90%,*/0.01").unwrap_err();
        assert!(err.contains("chunk count"), "diagnostic: {err}");
    }

    #[test]
    fn parse_star_split_zero_rejected() {
        let err = parse("90%,*/0").unwrap_err();
        assert!(err.contains(">= 1"), "diagnostic: {err}");
    }

    #[test]
    fn parse_form1_rejects_tail_tokens() {
        assert!(parse("0..*/4").is_err());
        // `0....` reads as `0..` + `..` noise — any tail token in
        // a range position must fail to parse, one way or another.
        assert!(parse("0....").is_err());
    }

    // ── Form 3: pre-baked recipes ──────────────────────────

    fn deltas_only(spec: PartitionSpec) -> Vec<Bound> {
        match spec.chunking {
            Chunking::DeltaList { deltas } => deltas,
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

    // ── Resolution: tail tokens ─────────────────────────────

    /// The three spellings of "first 90%, then the rest in ten
    /// 1%-of-the-whole chunks" that coincide at head=90%:
    /// explicit enumeration, fill, and remainder split.
    #[test]
    fn resolve_fill_and_star_split_coincide_at_90_10() {
        let explicit = resolve(
            &parse("90%,1%,1%,1%,1%,1%,1%,1%,1%,1%,1%").unwrap(), 0, 1000).unwrap();
        let filled = resolve(&parse("90%,1%,...").unwrap(), 0, 1000).unwrap();
        let split = resolve(&parse("90%,*/10").unwrap(), 0, 1000).unwrap();
        assert_eq!(explicit.len(), 11);
        assert_eq!(filled, explicit);
        assert_eq!(split, explicit);
        assert_eq!(filled[0].cardinality(), 900);
        for p in &filled[1..] {
            assert_eq!(p.cardinality(), 10);
        }
        assert_eq!(filled[10].end_ord, 1000);
    }

    #[test]
    fn resolve_fill_truncates_final_chunk() {
        // 3 + 2 + 2 + 2 + 1(truncated) over extent 10.
        let parts = resolve(&parse("3,2,...").unwrap(), 0, 10).unwrap();
        let bounds: Vec<(u64, u64)> =
            parts.iter().map(|p| (p.start_ord, p.end_ord)).collect();
        assert_eq!(bounds, vec![(0, 3), (3, 5), (5, 7), (7, 9), (9, 10)]);
    }

    #[test]
    fn resolve_fill_with_nothing_left_adds_no_chunks() {
        // 90% + 10% covers the extent; `...` repeats 10% zero times.
        let parts = resolve(&parse("90%,10%,...").unwrap(), 0, 1000).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1].end_ord, 1000);
    }

    #[test]
    fn resolve_fill_subordinal_chunk_rejected() {
        // 0.01% of 100 is 0.01 ordinals — chunks under one
        // ordinal could never all be non-empty.
        let err = resolve(&parse("50%,0.01%,...").unwrap(), 0, 100).unwrap_err();
        assert!(err.contains("less than one ordinal"), "diagnostic: {err}");
    }

    #[test]
    fn resolve_pct_boundaries_round_at_cumulative_position() {
        // linear:3 over 1000 — per-entry rounding would give
        // 333/333/333 and silently drop ordinal 999; the
        // boundary rule distributes the slack: 333/334/333,
        // covering the extent exactly.
        let parts = resolve(&parse("linear:3").unwrap(), 0, 1000).unwrap();
        let bounds: Vec<(u64, u64)> =
            parts.iter().map(|p| (p.start_ord, p.end_ord)).collect();
        assert_eq!(bounds, vec![(0, 333), (333, 667), (667, 1000)]);
    }

    #[test]
    fn resolve_star_split_distributes_rounding_slack() {
        // Remainder of 100 into 3: sizes 33/34/33 (rounded
        // boundaries), contiguous, exactly covering the extent.
        let parts = resolve(&parse("*/3").unwrap(), 0, 100).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].start_ord, 0);
        assert_eq!(parts[2].end_ord, 100);
        for w in parts.windows(2) {
            assert_eq!(w[0].end_ord, w[1].start_ord, "contiguous");
        }
        let sizes: Vec<u64> = parts.iter().map(|p| p.cardinality()).collect();
        assert!(sizes.iter().all(|s| *s == 33 || *s == 34), "sizes: {sizes:?}");
        assert_eq!(sizes.iter().sum::<u64>(), 100);
    }

    #[test]
    fn resolve_star_split_alone_equals_linear_recipe() {
        let split = resolve(&parse("*/16").unwrap(), 0, 1600).unwrap();
        let linear = resolve(&parse("linear:16").unwrap(), 0, 1600).unwrap();
        assert_eq!(split, linear);
    }

    #[test]
    fn resolve_star_split_no_remainder_rejected() {
        let err = resolve(&parse("100%,*/4").unwrap(), 0, 1000).unwrap_err();
        assert!(err.contains("no remainder"), "diagnostic: {err}");
    }

    #[test]
    fn resolve_star_split_finer_than_remainder_rejected() {
        let err = resolve(&parse("90%,*/200").unwrap(), 0, 1000).unwrap_err();
        assert!(err.contains("non-empty"), "diagnostic: {err}");
    }

    #[test]
    fn resolve_tail_indices_continue_from_head() {
        let parts = resolve(&parse("50%,*/5").unwrap(), 0, 1000).unwrap();
        assert_eq!(parts.len(), 6);
        for (i, p) in parts.iter().enumerate() {
            assert_eq!(p.idx, i as u64);
        }
    }

    #[test]
    fn split_evenly_boundaries_monotone_and_exact() {
        for (start, end, n) in [(0u64, 100u64, 7u64), (5, 5, 1), (0, 3, 3), (1000, 10007, 13)] {
            let chunks = split_evenly(start, end, n);
            assert_eq!(chunks.len(), n as usize);
            assert_eq!(chunks[0].0, start);
            assert_eq!(chunks[n as usize - 1].1, end);
            for w in chunks.windows(2) {
                assert_eq!(w[0].1, w[1].0);
            }
            let total: u64 = chunks.iter().map(|(s, e)| e - s).sum();
            assert_eq!(total, end - start);
        }
    }

    // ── Whitespace tolerance ───────────────────────────────

    #[test]
    fn parse_tolerates_whitespace_in_lists() {
        let spec = parse(" 2% , 10% , *% ").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Pct(2.0), Bound::Pct(10.0), Bound::Star])
        );
    }

    // ── Polydat Value round-trip ────────────────────────────────

    #[test]
    fn partition_roundtrips_through_value_ext() {
        let p = Partition {
            idx: 2,
            count: 4,
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
        match &recovered.chunking {
            Chunking::DeltaList { deltas } => assert_eq!(deltas.len(), 5),
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
        assert_eq!(
            spec,
            PartitionSpec::single_range(Bound::Ord(0), Bound::Pct(53.0))
        );
    }

    // ── Windowed chunking (`in <window>`) ──────────────────

    #[test]
    fn parse_window_clause() {
        let spec = parse("linear:4 in 25%..75%").unwrap();
        assert_eq!(spec.window, Some((Bound::Pct(25.0), Bound::Pct(75.0))));
        assert_eq!(spec.order, PartitionOrder::Unchanged);
        match &spec.chunking {
            Chunking::DeltaList { deltas } => assert_eq!(deltas.len(), 4),
            other => panic!("expected DeltaList, got {other:?}"),
        }
    }

    #[test]
    fn parse_window_requires_range() {
        let err = parse("linear:4 in 50%").unwrap_err();
        assert!(err.contains("start..end"), "diagnostic: {err}");
    }

    #[test]
    fn parse_window_requires_sized_bounds() {
        let err = parse("linear:4 in 0..*").unwrap_err();
        assert!(err.contains("sized"), "diagnostic: {err}");
    }

    #[test]
    fn parse_window_clause_position_errors() {
        assert!(parse("in 0..50%").unwrap_err().contains("chunking spec"));
        assert!(parse("linear:4 in").unwrap_err().contains("window range"));
        assert!(parse("linear:2 in 0..50% in 0..10%").unwrap_err().contains("at most one"));
    }

    #[test]
    fn resolve_windowed_chunking_is_window_relative() {
        // The chunking resolves against the window's range:
        // linear:4 over [20%, 100%) of 1000 → four 200-ordinal
        // partitions starting at 200.
        let parts = resolve(&parse("linear:4 in 20%..100%").unwrap(), 0, 1000).unwrap();
        let bounds: Vec<(u64, u64)> =
            parts.iter().map(|p| (p.start_ord, p.end_ord)).collect();
        assert_eq!(bounds, vec![(200, 400), (400, 600), (600, 800), (800, 1000)]);
    }

    #[test]
    fn resolve_windowed_form1_composes() {
        // `0..50%` of the window [500, 1000) → [500, 750).
        let parts = resolve(&parse("0..50% in 50%..100%").unwrap(), 0, 1000).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!((parts[0].start_ord, parts[0].end_ord), (500, 750));
    }

    #[test]
    fn resolve_windowed_tail_tokens() {
        // `90%,*/10` inside a window: percentages are relative
        // to the window (here [0, 500)), so head = 450 and the
        // ten chunks split the remaining 50.
        let parts = resolve(&parse("90%,*/10 in 0..50%").unwrap(), 0, 1000).unwrap();
        assert_eq!(parts.len(), 11);
        assert_eq!((parts[0].start_ord, parts[0].end_ord), (0, 450));
        assert_eq!(parts[10].end_ord, 500);
        assert_eq!(parts[1].cardinality(), 5);
    }

    // ── Finite repetition (`<delta>xN`) ────────────────────

    #[test]
    fn parse_finite_repetition_expands() {
        let spec = parse("1%x3").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![Bound::Pct(1.0); 3])
        );
    }

    #[test]
    fn parse_repetition_zero_rejected() {
        let err = parse("1%x0").unwrap_err();
        assert!(err.contains(">= 1"), "diagnostic: {err}");
    }

    #[test]
    fn parse_repetition_on_tail_rejected() {
        assert!(parse("*x3").is_err());
        assert!(parse("...x3").is_err());
    }

    #[test]
    fn resolve_repetition_equals_fill_and_split_at_90_10() {
        // The FOURTH spelling of "first 90%, then ten 1% chunks":
        // size and count both declared.
        let explicit = resolve(&parse("90%,1%,...").unwrap(), 0, 1000).unwrap();
        let repeated = resolve(&parse("90%,1%x10").unwrap(), 0, 1000).unwrap();
        assert_eq!(repeated, explicit);
    }

    // ── Gaps (`~<delta>`) ──────────────────────────────────

    #[test]
    fn parse_gap_entry() {
        let spec = parse("10%,~80%,10%").unwrap();
        assert_eq!(
            spec,
            PartitionSpec::delta_list(vec![
                Bound::Pct(10.0),
                Bound::Gap(Box::new(Bound::Pct(80.0))),
                Bound::Pct(10.0),
            ])
        );
    }

    #[test]
    fn parse_gap_requires_sized_bound() {
        let err = parse("10%,~*").unwrap_err();
        assert!(err.contains("sized"), "diagnostic: {err}");
    }

    #[test]
    fn parse_gap_repetition_rejected() {
        let err = parse("10%,~10%x3").unwrap_err();
        assert!(err.contains("size the gap"), "diagnostic: {err}");
    }

    #[test]
    fn parse_all_gaps_rejected() {
        let err = parse("~10%,~20%").unwrap_err();
        assert!(err.contains("emits no partitions"), "diagnostic: {err}");
    }

    #[test]
    fn parse_fill_after_gap_rejected() {
        let err = parse("5%,~5%,...").unwrap_err();
        assert!(err.contains("emit nothing"), "diagnostic: {err}");
    }

    #[test]
    fn resolve_gap_consumes_without_emitting() {
        let parts = resolve(&parse("10%,~80%,10%").unwrap(), 0, 1000).unwrap();
        let bounds: Vec<(u64, u64, u64)> =
            parts.iter().map(|p| (p.idx, p.start_ord, p.end_ord)).collect();
        // Emitted partitions only; idx counts emitted entries.
        assert_eq!(bounds, vec![(0, 0, 100), (1, 900, 1000)]);
    }

    #[test]
    fn resolve_gap_counts_toward_star_remainder() {
        // 10% head + 40% gap leaves 50% for the star.
        let parts = resolve(&parse("10%,~40%,*").unwrap(), 0, 1000).unwrap();
        let bounds: Vec<(u64, u64)> =
            parts.iter().map(|p| (p.start_ord, p.end_ord)).collect();
        assert_eq!(bounds, vec![(0, 100), (500, 1000)]);
    }

    // ── Recipe-shaped remainder (`*/recipe`) ───────────────

    #[test]
    fn parse_star_shaped_recipe() {
        let spec = parse("50%,*/ratios:1,3").unwrap();
        match &spec.chunking {
            Chunking::DeltaList { deltas } => {
                assert_eq!(deltas.len(), 2);
                match &deltas[1] {
                    Bound::StarShaped(w) => {
                        assert_eq!(w.len(), 2);
                        assert!((w[0] - 25.0).abs() < 1e-9);
                        assert!((w[1] - 75.0).abs() < 1e-9);
                    }
                    other => panic!("expected StarShaped, got {other:?}"),
                }
            }
            other => panic!("expected DeltaList, got {other:?}"),
        }
    }

    #[test]
    fn parse_star_linear_rejected_with_canonical_hint() {
        let err = parse("90%,*/linear:4").unwrap_err();
        assert!(err.contains("*/4"), "diagnostic should point at `*/N`: {err}");
    }

    #[test]
    fn resolve_star_shaped_divides_remainder_by_weights() {
        // Remainder 500, weights 25/75 → [500, 625), [625, 1000).
        let parts = resolve(&parse("50%,*/ratios:1,3").unwrap(), 0, 1000).unwrap();
        let bounds: Vec<(u64, u64)> =
            parts.iter().map(|p| (p.start_ord, p.end_ord)).collect();
        assert_eq!(bounds, vec![(0, 500), (500, 625), (625, 1000)]);
    }

    #[test]
    fn resolve_star_shaped_alone_covers_extent() {
        let parts = resolve(&parse("*/fib:3").unwrap(), 0, 600).unwrap();
        // fib:3 weights [1, 2, 3] → 100/200/300.
        let bounds: Vec<(u64, u64)> =
            parts.iter().map(|p| (p.start_ord, p.end_ord)).collect();
        assert_eq!(bounds, vec![(0, 100), (100, 300), (300, 600)]);
    }

    #[test]
    fn resolve_star_shaped_empty_chunk_rejected() {
        // A 0.1%-ish weight of a tiny remainder rounds to zero.
        let err = resolve(&parse("90%,*/ratios:1,1000").unwrap(), 0, 100).unwrap_err();
        assert!(err.contains("empty partition"), "diagnostic: {err}");
    }

    // ── Ordering suffix ────────────────────────────────────

    #[test]
    fn parse_order_suffix() {
        assert_eq!(parse("fib:5 largest_first").unwrap().order, PartitionOrder::LargestFirst);
        assert_eq!(parse("fib:5 smallest_first").unwrap().order, PartitionOrder::SmallestFirst);
        assert_eq!(parse("fib:5 random").unwrap().order, PartitionOrder::Random);
        assert_eq!(parse("fib:5 unchanged").unwrap().order, PartitionOrder::Unchanged);
        assert_eq!(parse("fib:5").unwrap().order, PartitionOrder::Unchanged);
    }

    #[test]
    fn parse_unknown_order_rejected() {
        let err = parse("fib:5 descend").unwrap_err();
        assert!(err.contains("unknown order"), "diagnostic: {err}");
        assert!(err.contains("largest_first"), "diagnostic should list options: {err}");
    }

    #[test]
    fn parse_bare_direction_words_rejected_with_axis_hint() {
        // `ascending` / `descending` are ambiguous between
        // ordinal position and size; the diagnostics teach the
        // axis-named spellings.
        let err = parse("fib:5 ascending").unwrap_err();
        assert!(err.contains("smallest_first"), "diagnostic: {err}");
        assert!(err.contains("SIZE"), "diagnostic should name the axis: {err}");
        let err = parse("fib:5 descending").unwrap_err();
        assert!(err.contains("largest_first"), "diagnostic: {err}");
    }

    #[test]
    fn resolve_largest_first_sorts_by_cardinality_keeping_idx() {
        let parts = resolve(&parse("fib:5 largest_first").unwrap(), 0, 1000).unwrap();
        for w in parts.windows(2) {
            assert!(w[0].cardinality() >= w[1].cardinality(), "largest first");
        }
        // fib weights grow, so the largest partition was
        // generated last: iteration starts at idx 4.
        assert_eq!(parts[0].idx, 4);
        assert_eq!(parts[4].idx, 0);
    }

    #[test]
    fn resolve_smallest_first_is_stable_for_equal_sizes() {
        // Equal-sized partitions keep generation order.
        let parts = resolve(&parse("linear:3 smallest_first").unwrap(), 0, 999).unwrap();
        let idxs: Vec<u64> = parts.iter().map(|p| p.idx).collect();
        assert_eq!(idxs, vec![0, 1, 2]);
    }

    #[test]
    fn resolve_random_is_deterministic_permutation() {
        let a = resolve(&parse("linear:8 random").unwrap(), 0, 800).unwrap();
        let b = resolve(&parse("linear:8 random").unwrap(), 0, 800).unwrap();
        assert_eq!(a, b, "same spec must shuffle identically");
        let mut by_idx = a.clone();
        by_idx.sort_by_key(|p| p.idx);
        let unchanged = resolve(&parse("linear:8").unwrap(), 0, 800).unwrap();
        assert_eq!(by_idx, unchanged, "shuffle is a permutation of the same partitions");
        assert_ne!(a, unchanged, "8 elements should not shuffle to identity here");
    }

    #[test]
    fn display_round_trips_window_and_order() {
        let spec = parse("linear:2 in 0..50% largest_first").unwrap();
        let shown = ReflectedValue::display(&spec);
        assert!(shown.contains("in 0..50%"), "display: {shown}");
        assert!(shown.contains("largest_first"), "display: {shown}");
    }

    // ── Labelling frames and degenerate extents ────────────

    #[test]
    fn windowed_partitions_label_against_full_base_frame() {
        // The window affects sizing/placement; pct fields and
        // base_extent describe the FULL base. This is what lets
        // the `over` clause's cross-extent reprojection keep a
        // windowed partition's position in the whole domain
        // (window-relative pcts would collapse the offset:
        // [200, 400) would reproject as if it started at 0).
        let parts = resolve(&parse("linear:4 in 20%..100%").unwrap(), 0, 1000).unwrap();
        let p = &parts[0];
        assert_eq!((p.start_ord, p.end_ord), (200, 400));
        assert!((p.start_pct - 20.0).abs() < 1e-9, "start_pct: {}", p.start_pct);
        assert!((p.end_pct - 40.0).abs() < 1e-9, "end_pct: {}", p.end_pct);
        assert_eq!(p.base_extent, 1000, "base_extent is the full base, not the window");
    }

    #[test]
    fn form1_zero_width_slice_rejected() {
        // `0..1%` of a 10-ordinal extent rounds to nothing — an
        // operator-explicit slice that would silently run zero
        // cycles is an error, not a no-op.
        let err = resolve(&parse("0..1%").unwrap(), 0, 10).unwrap_err();
        assert!(err.contains("zero ordinals"), "diagnostic: {err}");
    }

    #[test]
    fn delta_list_subordinal_recipe_tails_tolerated() {
        // Auto-terminating recipes legitimately produce
        // sub-ordinal tail weights on small extents; their
        // zero-width entries are correct arithmetic, not an
        // error (contrast with the Form 1 check above).
        let parts = resolve(&parse("mul:0.5").unwrap(), 0, 100).unwrap();
        assert_eq!(parts.len(), 11, "term count is weight-driven, not extent-driven");
        assert_eq!(parts.last().unwrap().end_ord, 100);
    }
}
