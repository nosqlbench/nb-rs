// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! MetricsQL evaluator. AST → query plan → result, against a
//! pluggable [`DataSource`].
//!
//! This is the first cut. The shape will fill out incremen-
//! tally as evaluator paths land:
//!
//!   1. Selector evaluation → `DataSource::fetch`
//!   2. Range vectors (`[5m]` rollups, offset, `@`)
//!   3. Aggregations (`sum/avg/min/max/count` by/without)
//!   4. Binary ops with vector matching
//!   5. Range queries (stepped multi-instant evaluation)
//!   6. Rollup-consumer functions (`rate`, `*_over_time`)
//!   7. [`DataSource`] boundary formalised                  ← *here*
//!
//! The boundary review at step (7) was informed by the four
//! call shapes the previous passes exposed against `fetch`:
//! instant `(T, T)`, windowed `(T-w, T)`, range-query loop of
//! either, and rollup-function-over-window. All four route
//! through a single `(matchers, start, end)` signature, and
//! none of them has yet surfaced a need for streaming or
//! batched-prefetch — so the trait stays one method wide.
//! The notable shape change at this step is admitting
//! backend failures through a typed [`DataSourceError`]
//! instead of pretending storage can't fail.
//!
//! After (2)–(3) light up, the [`DataSource`] trait gets a
//! second pass to harden the boundary against real call-site
//! pressure (range queries, label-set materialization,
//! streaming vs. materialized aggregate inputs). Until then
//! the shape here is provisional — see
//! `project_metricsql_eval_boundary.md`.

use crate::ast::{
    AggrModifier, AggrModifierOp, BinaryOp, BinaryOpExpr, DurationExpr, Expr,
    FuncExpr, LabelFilter, LabelFilterOp, MetricExpr, NumberExpr, RollupExpr,
};

/// One observation: time + value, with the labels that
/// identify the producing series. Aligns with VM's
/// `Timeseries` shape but keeps the type name domain-neutral.
#[derive(Debug, Clone)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// One time series — its identifying label set plus the
/// observed samples within the query range.
#[derive(Debug, Clone)]
pub struct Series {
    pub labels: Vec<(String, String)>,
    pub samples: Vec<Sample>,
}

/// Pluggable data backend. Implementations adapt their
/// underlying storage (sqlite, in-memory, remote) to the
/// engine's selector contract.
///
/// # Contract
///
/// `fetch(matchers, start_ms, end_ms)` returns every series
/// whose label set satisfies **every** [`Matcher`], with
/// samples lying in the closed interval `[start_ms, end_ms]`.
/// Implementations MUST honour these invariants:
///
/// - **`__name__` in labels.** Each returned [`Series`]
///   carries an `__name__` label whose value is the metric
///   name. Selectors with a metric-name matcher rely on it,
///   and aggregate-modifier semantics (`without` drops it,
///   `by` may keep it) reach for it explicitly.
/// - **Samples sorted ascending.** [`Series::samples`] is
///   sorted by `timestamp_ms`. The rollup reducers and
///   sample-alignment paths assume monotonic order; an
///   unsorted series would produce wrong `first_over_time` /
///   `last_over_time` / `rate` results without explicit
///   detection cost.
/// - **Window inclusive.** Samples MUST satisfy
///   `start_ms <= ts <= end_ms`. Out-of-window samples will
///   fold into windowed reducers (`sum_over_time`, …) and
///   produce incorrect totals.
/// - **No empty-series promise.** A series with zero matching
///   samples in the window MAY be omitted from the result, or
///   returned with an empty [`Series::samples`] list — the
///   evaluator handles both. Reducers operating on an empty
///   sample list yield `NaN` per upstream semantics.
///
/// # Errors
///
/// Backends that can fail (I/O, parse, transient remote)
/// surface failure via [`DataSourceError`]. Distinct from
/// [`EvalError`] so caller code can distinguish "the storage
/// layer broke" from "the query is unsupported".
///
/// # Non-goals (deferred)
///
/// The trait is intentionally one method wide. Future shape
/// pressure that may pull more methods in (revisit when a
/// real backend lights it up):
///
/// - **Prefetch hint** for stepped range queries (today the
///   evaluator re-fetches overlapping windows on every step;
///   sqlite-backed nb-rs metrics.db absorbs this fine, remote
///   stores will not).
/// - **Streaming sample iterators** for queries that scan
///   wide windows where in-memory materialisation hurts.
/// - **Pushdown of `or`-group disjunctions** so the backend
///   answers a multi-group selector in one round trip.
pub trait DataSource {
    /// Fetch all series whose labels satisfy every matcher,
    /// containing samples in `[start_ms, end_ms]`. See trait
    /// docs for contract invariants.
    fn fetch(
        &self,
        matchers: &[Matcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Series>, DataSourceError>;
}

/// Surface errors from a [`DataSource`] backend. The payload
/// is a free-form string message — backends own their error
/// taxonomy; the evaluator treats them all as opaque from a
/// flow-control perspective.
#[derive(Debug, Clone)]
pub struct DataSourceError {
    pub message: String,
}

impl DataSourceError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { message: msg.into() }
    }
}

impl std::fmt::Display for DataSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data source: {}", self.message)
    }
}

impl std::error::Error for DataSourceError {}

/// One label-matcher in a selector. Mirrors
/// [`crate::ast::LabelFilter`] but flattened for evaluator
/// consumers (no template-ref / quoted-form metadata).
#[derive(Debug, Clone)]
pub struct Matcher {
    pub label: String,
    pub op: MatcherOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherOp {
    Eq, Ne, EqRegex, NeRegex,
}

/// Evaluation context: the data source plus the time range
/// the query operates over. Step size matters for range
/// queries; instant queries use `start_ms == end_ms`.
pub struct EvalContext<'a> {
    pub data: &'a dyn DataSource,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
}

#[derive(Debug, Clone)]
pub enum EvalError {
    NotYetImplemented(&'static str),
    /// A node carries shape that the evaluator can't honour
    /// (e.g. a template-ref filter that should have been
    /// expanded by `parse`). Indicates a missed canonicalisation
    /// step rather than a user-facing parse error.
    InvalidShape(String),
    /// User-facing input the evaluator couldn't interpret —
    /// malformed duration text, unsupported `@` expression,
    /// etc.
    BadValue(String),
    /// Failure raised by the [`DataSource`] backend. Wraps
    /// the underlying message verbatim so callers can surface
    /// it.
    DataSource(DataSourceError),
}

impl From<DataSourceError> for EvalError {
    fn from(e: DataSourceError) -> Self {
        EvalError::DataSource(e)
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::NotYetImplemented(what) => {
                write!(f, "evaluator: {what} is not yet implemented")
            }
            EvalError::InvalidShape(msg) => {
                write!(f, "evaluator: invalid AST shape: {msg}")
            }
            EvalError::BadValue(msg) => {
                write!(f, "evaluator: {msg}")
            }
            EvalError::DataSource(e) => {
                write!(f, "evaluator: {e}")
            }
        }
    }
}

impl std::error::Error for EvalError {}

/// Evaluate `expr` as a **range query**: re-run it at every
/// step from `ctx.start_ms` to `ctx.end_ms` (inclusive), and
/// merge each step's output series by label identity. Each
/// per-step evaluation observes a degenerate `[T, T]` instant
/// context with the original step retained for `i`-relative
/// durations.
///
/// `step_ms` must be `> 0`; instant queries
/// (`start_ms == end_ms`) work too — they produce a single
/// step at `start_ms`.
///
/// The naïve loop fetches the underlying data once per step
/// even when the same window would suffice for many steps in
/// a row. That's the first real call-site shape pushing on
/// the storage trait — the optimisation lands when the
/// boundary is formalised (see
/// `project_metricsql_eval_boundary.md`); behaviour stays
/// correct in the meantime.
pub fn evaluate_range(ctx: &EvalContext<'_>, expr: &Expr) -> Result<Vec<Series>, EvalError> {
    if ctx.step_ms <= 0 {
        return Err(EvalError::BadValue(format!(
            "evaluate_range requires step_ms > 0, got {}", ctx.step_ms)));
    }
    if ctx.end_ms < ctx.start_ms {
        return Err(EvalError::BadValue(format!(
            "evaluate_range requires end_ms >= start_ms, got start={} end={}",
            ctx.start_ms, ctx.end_ms)));
    }
    let mut merged: Vec<Series> = Vec::new();
    let mut t = ctx.start_ms;
    loop {
        let step_ctx = EvalContext {
            data: ctx.data,
            start_ms: t,
            end_ms: t,
            step_ms: ctx.step_ms,
        };
        let step_result = evaluate(&step_ctx, expr)?;
        merge_step_into(&mut merged, step_result);
        if t == ctx.end_ms { break; }
        t = (t.saturating_add(ctx.step_ms)).min(ctx.end_ms);
    }
    Ok(merged)
}

/// Append every series in `step` to `merged`, joining samples
/// onto an existing entry when its label set already appears.
/// Preserves first-seen order.
fn merge_step_into(merged: &mut Vec<Series>, step: Vec<Series>) {
    for s in step {
        match merged.iter_mut().find(|m| label_sets_equal(&m.labels, &s.labels)) {
            Some(existing) => existing.samples.extend(s.samples),
            None => merged.push(s),
        }
    }
}

/// Evaluate a parsed MetricsQL expression against the
/// context's data source. Currently dispatches the subset
/// covered by the first evaluator path; everything else
/// returns [`EvalError::NotYetImplemented`].
pub fn evaluate(ctx: &EvalContext<'_>, expr: &Expr) -> Result<Vec<Series>, EvalError> {
    match expr {
        Expr::Metric(me) => evaluate_metric_expr(ctx, me),
        Expr::Number(n) => Ok(scalar_series(ctx, n.value)),
        Expr::String(_) => Err(EvalError::NotYetImplemented("string literals")),
        Expr::Duration(_) => Err(EvalError::NotYetImplemented("duration literals")),
        Expr::Func(f) => evaluate_func(ctx, f),
        Expr::Binary(b) => evaluate_binary(ctx, b),
        Expr::Rollup(re) => evaluate_rollup(ctx, re),
        Expr::Paren(_) => Err(EvalError::NotYetImplemented("parens groups")),
        Expr::With(_) => Err(EvalError::InvalidShape(
            "WithExpr survived into evaluation — caller should use `parse` (which expands) not `parse_for_prettify`".into())),
    }
}

/// Lift a numeric literal into the series shape the rest of
/// the evaluator speaks. Mirrors upstream's "scalar is a
/// single label-less series" convention. For instant queries
/// the sample lands at `end_ms`; range queries (when they
/// arrive) will populate one sample per step.
fn scalar_series(ctx: &EvalContext<'_>, value: f64) -> Vec<Series> {
    vec![Series {
        labels: Vec::new(),
        samples: vec![Sample { timestamp_ms: ctx.end_ms, value }],
    }]
}

/// Evaluate a single instant-vector selector. Each `or` group
/// in `label_filterss` becomes one independent fetch; the
/// union of returned series (de-duplicated by label set) is
/// the result.
///
/// Label filters with `is_template_ref` or non-empty
/// `value_expr` indicate the AST wasn't fully canonicalised
/// and surface as [`EvalError::InvalidShape`] — `parse` should
/// always resolve them before reaching here.
fn evaluate_metric_expr(
    ctx: &EvalContext<'_>,
    me: &MetricExpr,
) -> Result<Vec<Series>, EvalError> {
    if me.label_filterss.is_empty() {
        return Ok(Vec::new());
    }
    let mut out: Vec<Series> = Vec::new();
    let mut seen: Vec<Vec<(String, String)>> = Vec::new();
    for group in &me.label_filterss {
        let matchers = filters_to_matchers(group)?;
        let fetched = ctx.data.fetch(&matchers, ctx.start_ms, ctx.end_ms)?;
        for s in fetched {
            if !seen.iter().any(|prev| label_sets_equal(prev, &s.labels)) {
                seen.push(s.labels.clone());
                out.push(s);
            }
        }
    }
    Ok(out)
}

/// Evaluate a [`BinaryOpExpr`]. Three shapes:
///   - **scalar/scalar** — both sides label-less, one sample
///     each → one synthetic series with the op applied.
///   - **scalar/vector** (or vice versa) — broadcast the
///     scalar value across every sample of the vector.
///   - **vector/vector** — match left series to right series
///     by label set (excluding `__name__`) and apply the op
///     per timestamp.
///
/// Comparison operators with the `bool` modifier produce 0/1
/// per upstream; without `bool` they act as filters
/// (deferred — see `comparisons_filter_mode_unsupported`).
/// Vector-matching modifiers (`on`/`ignoring`/`group_left`/
/// `group_right`) and the set ops (`and`/`or`/`unless`) are
/// also deferred.
fn evaluate_binary(ctx: &EvalContext<'_>, b: &BinaryOpExpr) -> Result<Vec<Series>, EvalError> {
    if b.group_modifier.is_some() || b.join_modifier.is_some() {
        return Err(EvalError::NotYetImplemented(
            "vector-matching modifiers (on / ignoring / group_left / group_right)"));
    }
    if matches!(b.op, BinaryOp::And | BinaryOp::Or | BinaryOp::Unless
        | BinaryOp::If | BinaryOp::IfNot | BinaryOp::Default) {
        return Err(EvalError::NotYetImplemented("set / filter binary operators"));
    }
    if is_cmp_op(b.op) && !b.bool_modifier {
        return Err(EvalError::NotYetImplemented(
            "filter-mode comparisons (use `<op> bool` until this lands)"));
    }
    let left = evaluate(ctx, &b.left)?;
    let right = evaluate(ctx, &b.right)?;
    let bool_mod = b.bool_modifier;
    let combine_value = move |l: f64, r: f64| eval_binary_value(b.op, l, r, bool_mod);

    let left_is_scalar = is_scalar_series(&left);
    let right_is_scalar = is_scalar_series(&right);

    if left_is_scalar && right_is_scalar {
        let l = left[0].samples.first().map(|s| s.value).unwrap_or(f64::NAN);
        let r = right[0].samples.first().map(|s| s.value).unwrap_or(f64::NAN);
        return Ok(scalar_series(ctx, combine_value(l, r)));
    }
    if left_is_scalar {
        let l = left[0].samples.first().map(|s| s.value).unwrap_or(f64::NAN);
        return Ok(broadcast_scalar(&right, l, true, &combine_value));
    }
    if right_is_scalar {
        let r = right[0].samples.first().map(|s| s.value).unwrap_or(f64::NAN);
        return Ok(broadcast_scalar(&left, r, false, &combine_value));
    }
    Ok(combine_vectors(&left, &right, &combine_value))
}

fn is_cmp_op(op: BinaryOp) -> bool {
    matches!(op, BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt
        | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge)
}

/// True when a series list looks like a constant — exactly
/// one entry with no labels. Number literals produce this
/// shape directly; intermediate aggregate results keep their
/// `by`/`without` labels and don't qualify.
fn is_scalar_series(s: &[Series]) -> bool {
    s.len() == 1 && s[0].labels.is_empty()
}

/// Per-sample binary op. Comparison ops with `bool_mod`
/// return 0/1; arithmetic ops use the same machinery the
/// parser's constant-folder uses, so result semantics agree
/// (NaN propagation, division-by-zero → ±inf, etc.).
fn eval_binary_value(op: BinaryOp, l: f64, r: f64, bool_mod: bool) -> f64 {
    use BinaryOp::*;
    match op {
        Add => l + r,
        Sub => l - r,
        Mul => l * r,
        Div => l / r,
        Mod => l % r,
        Pow => if l.is_nan() { f64::NAN } else { l.powf(r) },
        Atan2 => l.atan2(r),
        Eq | Ne | Lt | Le | Gt | Ge => {
            let cmp = match op {
                Eq => bin_eq(l, r),
                Ne => bin_neq(l, r),
                Gt => l > r,
                Lt => l < r,
                Ge => l >= r,
                Le => l <= r,
                _ => unreachable!(),
            };
            if bool_mod {
                if cmp { 1.0 } else { 0.0 }
            } else if cmp {
                l
            } else {
                f64::NAN
            }
        }
        // Set / filter ops are dispatched away before reaching
        // here — guard with NaN to keep the path total.
        _ => f64::NAN,
    }
}

fn bin_eq(l: f64, r: f64) -> bool {
    if l.is_nan() { return r.is_nan(); }
    l == r
}

fn bin_neq(l: f64, r: f64) -> bool {
    if l.is_nan() { return !r.is_nan(); }
    if r.is_nan() { return true; }
    l != r
}

/// Apply a scalar value to every sample of every series in
/// `vector`. `scalar_on_left` controls operand order so non-
/// commutative ops (`-`, `/`, `^`, `%`, `atan2`) match the
/// source `<scalar> <op> <vector>` vs. `<vector> <op> <scalar>`
/// orientation.
fn broadcast_scalar(
    vector: &[Series],
    scalar: f64,
    scalar_on_left: bool,
    combine: &impl Fn(f64, f64) -> f64,
) -> Vec<Series> {
    vector.iter().map(|s| {
        let labels = labels_after_op(&s.labels);
        let samples = s.samples.iter().map(|sm| {
            let value = if scalar_on_left {
                combine(scalar, sm.value)
            } else {
                combine(sm.value, scalar)
            };
            Sample { timestamp_ms: sm.timestamp_ms, value }
        }).collect();
        Series { labels, samples }
    }).collect()
}

/// Vector-vector match. Default semantics: pair series by
/// label set excluding `__name__`. The `__name__` label is
/// dropped from the result series — a binary op produces a
/// new series identity, not a continuation of either operand.
fn combine_vectors(
    left: &[Series],
    right: &[Series],
    combine: &impl Fn(f64, f64) -> f64,
) -> Vec<Series> {
    let mut out: Vec<Series> = Vec::new();
    for ls in left {
        let l_match = match_labels(&ls.labels);
        for rs in right {
            if !label_sets_equal(&l_match, &match_labels(&rs.labels)) {
                continue;
            }
            let labels = labels_after_op(&ls.labels);
            let samples = align_and_combine(&ls.samples, &rs.samples, combine);
            out.push(Series { labels, samples });
            // Default 1:1 matching — once we've paired this
            // left series, move on. `group_left`/`group_right`
            // (when implemented) will lift this constraint.
            break;
        }
    }
    out
}

/// The matching key for vector-vector ops: every label
/// except `__name__`.
fn match_labels(labels: &[(String, String)]) -> Vec<(String, String)> {
    labels.iter()
        .filter(|(k, _)| k != "__name__")
        .cloned()
        .collect()
}

/// Result-series labels: drop `__name__` (per upstream:
/// arithmetic between vectors loses metric identity).
fn labels_after_op(labels: &[(String, String)]) -> Vec<(String, String)> {
    labels.iter()
        .filter(|(k, _)| k != "__name__")
        .cloned()
        .collect()
}

/// Inner-join two sample streams on timestamp and apply
/// `combine` to each matched pair. Timestamps unique to one
/// side don't contribute — there's no value to operate
/// against.
fn align_and_combine(
    left: &[Sample],
    right: &[Sample],
    combine: &impl Fn(f64, f64) -> f64,
) -> Vec<Sample> {
    let mut out: Vec<Sample> = Vec::new();
    for l in left {
        if let Some(r) = right.iter().find(|r| r.timestamp_ms == l.timestamp_ms) {
            out.push(Sample {
                timestamp_ms: l.timestamp_ms,
                value: combine(l.value, r.value),
            });
        }
    }
    out
}

/// Dispatch [`FuncExpr`] by name. Aggregate functions
/// (sum/avg/min/max/count and friends) handle their `by` /
/// `without` modifiers here; transform / rollup functions
/// will land in subsequent passes.
fn evaluate_func(ctx: &EvalContext<'_>, f: &FuncExpr) -> Result<Vec<Series>, EvalError> {
    if f.name.is_empty() {
        return Err(EvalError::NotYetImplemented(
            "anonymous union() function (multi-element parens group)"));
    }
    if let Some(agg) = AggregateOp::from_name(&f.name) {
        return evaluate_aggregate(ctx, f, agg);
    }
    if let Some(op) = RollupFn::from_name(&f.name) {
        return evaluate_rollup_fn(ctx, f, op);
    }
    Err(EvalError::NotYetImplemented("non-aggregate / non-rollup function calls"))
}

/// Rollup-function reducers: take a range-vector argument
/// (`metric[w]`), reduce each series's window down to a
/// single value at the query anchor. Mirrors the subset of
/// upstream's `rollupFuncs` that nb-rs's reporting actually
/// reaches for first; the long tail (quantile, holt-winters,
/// stddev_over_time, …) lands as queries demand it.
#[derive(Debug, Clone, Copy)]
enum RollupFn {
    Rate, Increase, Delta,
    SumOverTime, AvgOverTime, MinOverTime, MaxOverTime,
    CountOverTime, LastOverTime, FirstOverTime,
}

impl RollupFn {
    fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "rate"             => Some(Self::Rate),
            "increase"         => Some(Self::Increase),
            "delta"            => Some(Self::Delta),
            "sum_over_time"    => Some(Self::SumOverTime),
            "avg_over_time"    => Some(Self::AvgOverTime),
            "min_over_time"    => Some(Self::MinOverTime),
            "max_over_time"    => Some(Self::MaxOverTime),
            "count_over_time"  => Some(Self::CountOverTime),
            "last_over_time"   => Some(Self::LastOverTime),
            "first_over_time"  => Some(Self::FirstOverTime),
            _ => None,
        }
    }

    /// Whether the reducer needs the window length in seconds
    /// (to compute a per-second rate). Other reducers ignore
    /// it.
    fn needs_window(self) -> bool {
        matches!(self, Self::Rate)
    }
}

/// Evaluate a rollup function over a range-vector argument.
/// The evaluator only accepts an explicit `[w]` window today;
/// PromQL's "infer window from query step" shorthand
/// (`rate(cpu)` without brackets) lands when subqueries do
/// — that's where step-relative durations get their proper
/// hookup.
fn evaluate_rollup_fn(
    ctx: &EvalContext<'_>,
    f: &FuncExpr,
    op: RollupFn,
) -> Result<Vec<Series>, EvalError> {
    if f.args.len() != 1 {
        return Err(EvalError::BadValue(format!(
            "rollup function {:?} expects 1 range-vector argument, got {}",
            f.name, f.args.len())));
    }
    let arg = &f.args[0];
    let window_ms: Option<i64> = window_of_arg(arg, ctx.step_ms)?;
    if op.needs_window() && window_ms.is_none() {
        return Err(EvalError::BadValue(format!(
            "rollup function {:?} needs an explicit `[window]` argument", f.name)));
    }
    let input = evaluate(ctx, arg)?;
    let mut out: Vec<Series> = Vec::with_capacity(input.len());
    for s in input {
        let value = reduce_rollup(op, &s.samples, window_ms.unwrap_or(0));
        let labels = labels_after_op(&s.labels);
        out.push(Series {
            labels,
            samples: vec![Sample { timestamp_ms: ctx.end_ms, value }],
        });
    }
    Ok(out)
}

/// Pick the window duration off a rollup argument's outer
/// `RollupExpr`. `None` when no window is present (the
/// argument was a bare selector — only valid for reducers
/// that don't need the window length).
fn window_of_arg(arg: &Expr, step_ms: i64) -> Result<Option<i64>, EvalError> {
    if let Expr::Rollup(re) = arg
        && let Some(w) = &re.window {
        return Ok(Some(parse_duration_ms(&w.value, step_ms)?));
    }
    Ok(None)
}

/// Apply the rollup reducer to one series's windowed samples.
/// NaN samples are dropped before reducing, per upstream;
/// empty inputs produce NaN.
fn reduce_rollup(op: RollupFn, samples: &[Sample], window_ms: i64) -> f64 {
    let xs: Vec<&Sample> = samples.iter()
        .filter(|s| !s.value.is_nan())
        .collect();
    if xs.is_empty() { return f64::NAN; }
    match op {
        RollupFn::Rate => {
            // Simplified: (last - first) / window_seconds. No
            // counter-reset adjustment, no partial-window
            // extrapolation — those land alongside subqueries
            // where the rate semantics get formalised.
            if window_ms == 0 { return f64::NAN; }
            let first = xs.first().unwrap().value;
            let last = xs.last().unwrap().value;
            (last - first) / (window_ms as f64 / 1000.0)
        }
        RollupFn::Increase | RollupFn::Delta => {
            let first = xs.first().unwrap().value;
            let last = xs.last().unwrap().value;
            last - first
        }
        RollupFn::SumOverTime => xs.iter().map(|s| s.value).sum(),
        RollupFn::AvgOverTime => {
            xs.iter().map(|s| s.value).sum::<f64>() / xs.len() as f64
        }
        RollupFn::MinOverTime => xs.iter()
            .map(|s| s.value)
            .fold(f64::INFINITY, f64::min),
        RollupFn::MaxOverTime => xs.iter()
            .map(|s| s.value)
            .fold(f64::NEG_INFINITY, f64::max),
        RollupFn::CountOverTime => xs.len() as f64,
        RollupFn::LastOverTime => xs.last().unwrap().value,
        RollupFn::FirstOverTime => xs.first().unwrap().value,
    }
}

/// Aggregate function kinds the evaluator implements today.
/// Mirrors the subset of upstream `aggrFuncs` that take a
/// single vector argument and reduce per-group; the
/// percentile / histogram aggregates land later.
#[derive(Debug, Clone, Copy)]
enum AggregateOp { Sum, Avg, Min, Max, Count, Group }

impl AggregateOp {
    fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "sum"   => Some(Self::Sum),
            "avg"   => Some(Self::Avg),
            "min"   => Some(Self::Min),
            "max"   => Some(Self::Max),
            "count" => Some(Self::Count),
            "group" => Some(Self::Group),
            _ => None,
        }
    }
}

fn evaluate_aggregate(
    ctx: &EvalContext<'_>,
    f: &FuncExpr,
    op: AggregateOp,
) -> Result<Vec<Series>, EvalError> {
    if f.args.len() != 1 {
        return Err(EvalError::BadValue(format!(
            "aggregate {:?} expects 1 vector argument, got {}",
            f.name, f.args.len())));
    }
    let input = evaluate(ctx, &f.args[0])?;
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let groups = group_series(&input, f.modifier.as_ref());
    let mut out: Vec<Series> = Vec::with_capacity(groups.len());
    for (group_labels, members) in groups {
        out.push(reduce_group(op, group_labels, &members));
    }
    Ok(out)
}

/// Bucket the input series by their (modifier-filtered)
/// label sets. Returns `(group_labels, members)` pairs in
/// first-encountered order; with no modifier, all series
/// collapse into a single empty-labelled group (per upstream
/// aggregate semantics).
fn group_series(
    input: &[Series],
    modifier: Option<&AggrModifier>,
) -> Vec<(Vec<(String, String)>, Vec<Series>)> {
    let mut out: Vec<(Vec<(String, String)>, Vec<Series>)> = Vec::new();
    for s in input {
        let key = group_key(&s.labels, modifier);
        match out.iter_mut().find(|(k, _)| label_sets_equal(k, &key)) {
            Some((_, members)) => members.push(s.clone()),
            None => out.push((key, vec![s.clone()])),
        }
    }
    out
}

/// Project a series's label set down to the labels the
/// aggregate's modifier preserves. Mirrors upstream:
///   - `by(l1, l2)`     → keep only l1, l2.
///   - `without(l1, l2)`→ keep everything except l1, l2 and
///                        `__name__` (always dropped).
///   - no modifier      → empty key (all series share one
///                        bucket).
fn group_key(
    labels: &[(String, String)],
    modifier: Option<&AggrModifier>,
) -> Vec<(String, String)> {
    let Some(m) = modifier else { return Vec::new(); };
    let mut keep: Vec<(String, String)> = match m.op {
        AggrModifierOp::By => labels.iter()
            .filter(|(k, _)| m.args.iter().any(|w| w == k))
            .cloned()
            .collect(),
        AggrModifierOp::Without => labels.iter()
            .filter(|(k, _)| k != "__name__" && !m.args.iter().any(|w| w == k))
            .cloned()
            .collect(),
    };
    keep.sort_by(|a, b| a.0.cmp(&b.0));
    keep
}

/// Apply the aggregate operator across the timestamp-aligned
/// samples in `members` and emit a single [`Series`] tagged
/// with `group_labels`. Samples with NaN values are skipped
/// per Prometheus' aggregate semantics.
fn reduce_group(
    op: AggregateOp,
    group_labels: Vec<(String, String)>,
    members: &[Series],
) -> Series {
    // Bucket samples by timestamp first, then reduce each
    // bucket. We don't assume members share a timestamp set —
    // missing samples just don't contribute to that bucket.
    let mut buckets: Vec<(i64, Vec<f64>)> = Vec::new();
    for s in members {
        for sample in &s.samples {
            if sample.value.is_nan() { continue; }
            match buckets.iter_mut().find(|(t, _)| *t == sample.timestamp_ms) {
                Some((_, vals)) => vals.push(sample.value),
                None => buckets.push((sample.timestamp_ms, vec![sample.value])),
            }
        }
    }
    buckets.sort_by_key(|(t, _)| *t);
    let samples: Vec<Sample> = buckets.into_iter().map(|(t, vals)| {
        let value = match op {
            AggregateOp::Sum   => vals.iter().sum::<f64>(),
            AggregateOp::Avg   => vals.iter().sum::<f64>() / vals.len() as f64,
            AggregateOp::Min   => vals.iter().cloned().fold(f64::INFINITY, f64::min),
            AggregateOp::Max   => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            AggregateOp::Count => vals.len() as f64,
            AggregateOp::Group => 1.0,
        };
        Sample { timestamp_ms: t, value }
    }).collect();
    Series { labels: group_labels, samples }
}

/// Evaluate a [`RollupExpr`]: shift / window the time range
/// the inner expression observes.
///
/// For an instant query at time `T`:
///   - `m[5m]`             → fetch series with samples in
///                           `[T-5m, T]`.
///   - `m offset 1h`       → instant value at `T-1h` (range
///                           `[T-1h, T-1h]`).
///   - `m[5m] offset 1h`   → window `[T-1h-5m, T-1h]`.
///   - `m @ 1500000000`    → anchor at the literal seconds
///                           timestamp instead of `T`.
///
/// `step` (sub-query step) and non-literal `@` expressions
/// (`start()`, `end()`) are deferred — they only matter once
/// range queries land.
fn evaluate_rollup(ctx: &EvalContext<'_>, re: &RollupExpr) -> Result<Vec<Series>, EvalError> {
    if re.step.is_some() || re.inherit_step {
        return Err(EvalError::NotYetImplemented("subquery step `[w:s]`"));
    }
    let mut anchor_end_ms = ctx.end_ms;
    if let Some(at) = &re.at {
        anchor_end_ms = evaluate_at_modifier(at)?;
    }
    if let Some(off) = &re.offset {
        anchor_end_ms -= duration_to_ms(off, ctx.step_ms)?;
    }
    let anchor_start_ms = if let Some(w) = &re.window {
        anchor_end_ms - duration_to_ms(w, ctx.step_ms)?
    } else {
        // No window — instant lookup at the (offset-adjusted)
        // anchor. Inner expression sees a degenerate range.
        anchor_end_ms
    };
    let inner_ctx = EvalContext {
        data: ctx.data,
        start_ms: anchor_start_ms,
        end_ms: anchor_end_ms,
        step_ms: ctx.step_ms,
    };
    evaluate(&inner_ctx, &re.expr)
}

/// Resolve the `@ <expr>` modifier to a millisecond timestamp.
/// Currently accepts only a numeric literal (Unix seconds, per
/// upstream's `@` semantics); `start()` / `end()` lookups land
/// when range queries do.
fn evaluate_at_modifier(at: &Expr) -> Result<i64, EvalError> {
    match at {
        Expr::Number(NumberExpr { value, .. }) => Ok((*value * 1000.0) as i64),
        Expr::Func(_) => Err(EvalError::NotYetImplemented(
            "`@` with function expressions (start(), end())")),
        _ => Err(EvalError::BadValue(
            "`@` modifier must be a numeric timestamp".into())),
    }
}

/// Parse a [`DurationExpr`] to milliseconds. Mirrors upstream
/// `metricsql.PositiveDuration` for the unit set the
/// evaluator currently exercises (s/m/h/d/w/y, ms, plus
/// step-relative `i`). Multi-unit durations like `1h30m` and
/// the leading `-` sign for offsets are both supported.
fn duration_to_ms(d: &DurationExpr, step_ms: i64) -> Result<i64, EvalError> {
    parse_duration_ms(&d.value, step_ms)
}

fn parse_duration_ms(s: &str, step_ms: i64) -> Result<i64, EvalError> {
    let (sign, rest) = if let Some(stripped) = s.strip_prefix('-') {
        (-1i64, stripped)
    } else {
        (1i64, s)
    };
    if rest.is_empty() {
        return Err(EvalError::BadValue(format!("empty duration {s:?}")));
    }
    // Bare numeric → seconds (matches upstream's
    // `PositiveDurationValue` fallback).
    if let Ok(n) = rest.parse::<f64>() {
        return Ok(sign * (n * 1000.0) as i64);
    }
    let bytes = rest.as_bytes();
    let mut i = 0;
    let mut total_ms: i64 = 0;
    while i < bytes.len() {
        // Mantissa: optional digits + optional `.digits`.
        let start = i;
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
            i += 1;
        }
        if i == start {
            return Err(EvalError::BadValue(format!(
                "duration {s:?}: expected number at byte {start}")));
        }
        let n: f64 = rest[start..i].parse().map_err(|_| EvalError::BadValue(
            format!("duration {s:?}: invalid number {:?}", &rest[start..i])))?;
        // Unit: 1- or 2-letter suffix.
        let unit_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
            i += 1;
        }
        if i == unit_start {
            return Err(EvalError::BadValue(format!(
                "duration {s:?}: missing unit after {n}")));
        }
        let unit = &rest[unit_start..i];
        let unit_ms = match unit {
            "ms" => 1.0,
            "s"  => 1_000.0,
            "m"  => 60.0 * 1_000.0,
            "h"  => 60.0 * 60.0 * 1_000.0,
            "d"  => 24.0 * 60.0 * 60.0 * 1_000.0,
            "w"  => 7.0 * 24.0 * 60.0 * 60.0 * 1_000.0,
            "y"  => 365.0 * 24.0 * 60.0 * 60.0 * 1_000.0,
            "i"  => step_ms as f64,
            other => return Err(EvalError::BadValue(format!(
                "duration {s:?}: unknown unit {other:?}"))),
        };
        total_ms = total_ms.saturating_add((n * unit_ms) as i64);
    }
    Ok(sign * total_ms)
}

fn filters_to_matchers(group: &[LabelFilter]) -> Result<Vec<Matcher>, EvalError> {
    let mut out = Vec::with_capacity(group.len());
    for lf in group {
        if lf.is_template_ref {
            return Err(EvalError::InvalidShape(format!(
                "unexpanded template-ref filter {:?} in selector — use `parse` not `parse_for_prettify`",
                lf.label)));
        }
        if lf.value_expr.is_some() {
            return Err(EvalError::InvalidShape(format!(
                "unexpanded value expression on filter {:?} in selector",
                lf.label)));
        }
        out.push(Matcher {
            label: lf.label.clone(),
            op: match lf.op {
                LabelFilterOp::Eq => MatcherOp::Eq,
                LabelFilterOp::Ne => MatcherOp::Ne,
                LabelFilterOp::EqRegex => MatcherOp::EqRegex,
                LabelFilterOp::NeRegex => MatcherOp::NeRegex,
            },
            value: lf.value.clone(),
        });
    }
    Ok(out)
}

fn label_sets_equal(a: &[(String, String)], b: &[(String, String)]) -> bool {
    if a.len() != b.len() { return false; }
    for (k, v) in a {
        if !b.iter().any(|(k2, v2)| k2 == k && v2 == v) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    /// In-memory backend that exact-matches `Eq` matchers and
    /// supports `Ne`. Regex matchers fall back to a literal
    /// equality check (the evaluator path under test doesn't
    /// exercise them yet).
    struct MemoryDataSource {
        series: Vec<Series>,
    }

    impl DataSource for MemoryDataSource {
        fn fetch(&self, matchers: &[Matcher], _start: i64, _end: i64)
            -> Result<Vec<Series>, DataSourceError>
        {
            Ok(self.series.iter()
                .filter(|s| matchers.iter().all(|m| matches_series(m, s)))
                .cloned()
                .collect())
        }
    }

    fn matches_series(m: &Matcher, s: &Series) -> bool {
        let v = s.labels.iter()
            .find(|(k, _)| k == &m.label)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        match m.op {
            MatcherOp::Eq => v == m.value,
            MatcherOp::Ne => v != m.value,
            MatcherOp::EqRegex | MatcherOp::NeRegex => v == m.value,
        }
    }

    fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
        Series {
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            samples: samples.iter().map(|(t, v)| Sample { timestamp_ms: *t, value: *v }).collect(),
        }
    }

    fn ctx_for(ds: &MemoryDataSource) -> EvalContext<'_> {
        EvalContext { data: ds, start_ms: 0, end_ms: 100, step_ms: 1 }
    }

    #[test]
    fn bare_metric_selector_returns_all_with_that_name() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 2.0)]),
                series(&[("__name__", "mem"), ("host", "a")], &[(0, 3.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("cpu").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 2);
        assert!(got.iter().any(|s| s.labels.iter().any(|(k, v)| k == "host" && v == "a")));
        assert!(got.iter().any(|s| s.labels.iter().any(|(k, v)| k == "host" && v == "b")));
    }

    #[test]
    fn label_filter_narrows_selection() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 2.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse(r#"cpu{host="b"}"#).expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].samples[0].value, 2.0);
    }

    #[test]
    fn or_groups_union_results() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 2.0)]),
                series(&[("__name__", "cpu"), ("host", "c")], &[(0, 3.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse(r#"cpu{host="a" or host="b"}"#).expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn or_groups_dedup_overlapping_series() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a"), ("zone", "z1")], &[(0, 1.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        // Both sides match the same series — should appear once.
        let ast = parse(r#"cpu{host="a" or zone="z1"}"#).expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn empty_selector_returns_nothing() {
        let ds = MemoryDataSource {
            series: vec![series(&[("__name__", "cpu")], &[(0, 1.0)])],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("{}").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 0);
    }

    #[test]
    fn unimplemented_node_types_surface_cleanly() {
        let ds = MemoryDataSource { series: vec![] };
        let ctx = ctx_for(&ds);
        // String literals aren't wired yet — should not panic,
        // just report the gap.
        let ast = parse("\"hello\"").expect("parse");
        let err = evaluate(&ctx, &ast).expect_err("eval");
        assert!(matches!(err, EvalError::NotYetImplemented(_)));
    }

    /// In-memory source that records the time range it was
    /// last invoked with — lets tests assert that the rollup
    /// evaluator passed the right window.
    struct RecordingDataSource {
        series: Vec<Series>,
        last_range: std::cell::Cell<(i64, i64)>,
    }

    impl DataSource for RecordingDataSource {
        fn fetch(&self, matchers: &[Matcher], start: i64, end: i64)
            -> Result<Vec<Series>, DataSourceError>
        {
            self.last_range.set((start, end));
            Ok(self.series.iter()
                .filter(|s| matchers.iter().all(|m| matches_series(m, s)))
                .cloned()
                .collect())
        }
    }

    #[test]
    fn rollup_window_shifts_start() {
        let ds = RecordingDataSource {
            series: vec![series(&[("__name__", "cpu")], &[(0, 1.0)])],
            last_range: std::cell::Cell::new((-1, -1)),
        };
        let ctx = EvalContext { data: &ds, start_ms: 1_000_000, end_ms: 1_000_000, step_ms: 1 };
        let ast = parse("cpu[5m]").expect("parse");
        evaluate(&ctx, &ast).expect("eval");
        let (start, end) = ds.last_range.get();
        // 5m = 300_000 ms.
        assert_eq!(end, 1_000_000);
        assert_eq!(start, 1_000_000 - 300_000);
    }

    #[test]
    fn offset_shifts_anchor_back() {
        let ds = RecordingDataSource {
            series: vec![series(&[("__name__", "cpu")], &[(0, 1.0)])],
            last_range: std::cell::Cell::new((-1, -1)),
        };
        let ctx = EvalContext { data: &ds, start_ms: 1_000_000, end_ms: 1_000_000, step_ms: 1 };
        let ast = parse("cpu offset 1h").expect("parse");
        evaluate(&ctx, &ast).expect("eval");
        let (start, end) = ds.last_range.get();
        // 1h = 3_600_000 ms; no window so start == end.
        assert_eq!(end, 1_000_000 - 3_600_000);
        assert_eq!(start, 1_000_000 - 3_600_000);
    }

    #[test]
    fn window_and_offset_compose() {
        let ds = RecordingDataSource {
            series: vec![series(&[("__name__", "cpu")], &[(0, 1.0)])],
            last_range: std::cell::Cell::new((-1, -1)),
        };
        let ctx = EvalContext { data: &ds, start_ms: 1_000_000, end_ms: 1_000_000, step_ms: 1 };
        let ast = parse("cpu[5m] offset 1h").expect("parse");
        evaluate(&ctx, &ast).expect("eval");
        let (start, end) = ds.last_range.get();
        assert_eq!(end, 1_000_000 - 3_600_000);
        assert_eq!(start, 1_000_000 - 3_600_000 - 300_000);
    }

    #[test]
    fn negative_offset_shifts_anchor_forward() {
        let ds = RecordingDataSource {
            series: vec![series(&[("__name__", "cpu")], &[(0, 1.0)])],
            last_range: std::cell::Cell::new((-1, -1)),
        };
        let ctx = EvalContext { data: &ds, start_ms: 1_000_000, end_ms: 1_000_000, step_ms: 1 };
        let ast = parse("cpu offset -1h").expect("parse");
        evaluate(&ctx, &ast).expect("eval");
        let (_, end) = ds.last_range.get();
        assert_eq!(end, 1_000_000 + 3_600_000);
    }

    #[test]
    fn at_modifier_overrides_anchor() {
        let ds = RecordingDataSource {
            series: vec![series(&[("__name__", "cpu")], &[(0, 1.0)])],
            last_range: std::cell::Cell::new((-1, -1)),
        };
        // anchor at 12345 seconds → 12_345_000 ms.
        let ctx = EvalContext { data: &ds, start_ms: 1_000_000, end_ms: 1_000_000, step_ms: 1 };
        let ast = parse("cpu @ 12345").expect("parse");
        evaluate(&ctx, &ast).expect("eval");
        let (_, end) = ds.last_range.get();
        assert_eq!(end, 12_345_000);
    }

    #[test]
    fn duration_units_compose() {
        // 1h30m45s = 5445 s = 5_445_000 ms.
        assert_eq!(parse_duration_ms("1h30m45s", 1).unwrap(), 5_445_000);
        // 0.5h = 30 min = 1_800_000 ms.
        assert_eq!(parse_duration_ms("0.5h", 1).unwrap(), 1_800_000);
        // Bare number → seconds.
        assert_eq!(parse_duration_ms("30", 1).unwrap(), 30_000);
        // Step-relative.
        assert_eq!(parse_duration_ms("1i", 250).unwrap(), 250);
    }

    #[test]
    fn subquery_step_reports_not_yet_implemented() {
        let ds = MemoryDataSource { series: vec![] };
        let ctx = ctx_for(&ds);
        let ast = parse("cpu[5m:30s]").expect("parse");
        let err = evaluate(&ctx, &ast).expect_err("eval");
        assert!(matches!(err, EvalError::NotYetImplemented(_)));
    }

    fn lookup_label<'a>(s: &'a Series, key: &str) -> Option<&'a str> {
        s.labels.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    #[test]
    fn sum_collapses_all_into_one_series() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 2.0)]),
                series(&[("__name__", "cpu"), ("host", "c")], &[(0, 4.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("sum(cpu)").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 1);
        assert!(got[0].labels.is_empty());
        assert_eq!(got[0].samples.len(), 1);
        assert_eq!(got[0].samples[0].value, 7.0);
    }

    #[test]
    fn sum_by_groups_on_named_labels() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "b")], &[(0, 2.0)]),
                series(&[("__name__", "cpu"), ("zone", "z2"), ("host", "c")], &[(0, 4.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("sum(cpu) by (zone)").expect("parse");
        let mut got = evaluate(&ctx, &ast).expect("eval");
        got.sort_by(|a, b| lookup_label(a, "zone").unwrap_or("").cmp(lookup_label(b, "zone").unwrap_or("")));
        assert_eq!(got.len(), 2);
        assert_eq!(lookup_label(&got[0], "zone"), Some("z1"));
        assert_eq!(got[0].samples[0].value, 3.0);
        assert_eq!(lookup_label(&got[1], "zone"), Some("z2"));
        assert_eq!(got[1].samples[0].value, 4.0);
    }

    #[test]
    fn sum_without_drops_listed_labels_and_metric_name() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "b")], &[(0, 2.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        // `without (host)` keeps `zone`; `__name__` is dropped
        // automatically per upstream aggregate semantics.
        let ast = parse("sum(cpu) without (host)").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 1);
        assert_eq!(lookup_label(&got[0], "zone"), Some("z1"));
        assert_eq!(lookup_label(&got[0], "__name__"), None);
        assert_eq!(lookup_label(&got[0], "host"), None);
        assert_eq!(got[0].samples[0].value, 3.0);
    }

    #[test]
    fn avg_min_max_count_group_share_grouping_logic() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 5.0)]),
                series(&[("__name__", "cpu"), ("host", "c")], &[(0, 9.0)]),
            ],
        };
        let ctx = ctx_for(&ds);

        let go = |q: &str| -> f64 {
            let ast = parse(q).expect("parse");
            let got = evaluate(&ctx, &ast).expect("eval");
            assert_eq!(got.len(), 1);
            got[0].samples[0].value
        };
        assert_eq!(go("avg(cpu)"),   5.0);
        assert_eq!(go("min(cpu)"),   1.0);
        assert_eq!(go("max(cpu)"),   9.0);
        assert_eq!(go("count(cpu)"), 3.0);
        assert_eq!(go("group(cpu)"), 1.0);
    }

    #[test]
    fn aggregate_skips_nan_inputs() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 2.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, f64::NAN)]),
                series(&[("__name__", "cpu"), ("host", "c")], &[(0, 4.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("count(cpu)").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        // NaN sample doesn't contribute to count.
        assert_eq!(got[0].samples[0].value, 2.0);
    }

    #[test]
    fn aggregate_aligns_samples_per_timestamp() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(10, 1.0), (20, 2.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(10, 3.0), (20, 4.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("sum(cpu)").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got[0].samples.len(), 2);
        assert_eq!(got[0].samples[0].timestamp_ms, 10);
        assert_eq!(got[0].samples[0].value, 4.0);
        assert_eq!(got[0].samples[1].timestamp_ms, 20);
        assert_eq!(got[0].samples[1].value, 6.0);
    }

    #[test]
    fn unknown_function_reports_not_yet_implemented() {
        let ds = MemoryDataSource { series: vec![] };
        let ctx = ctx_for(&ds);
        // `clamp_min` isn't a rollup or aggregate — should
        // surface NotYetImplemented, not panic.
        let ast = parse("clamp_min(cpu, 0)").expect("parse");
        let err = evaluate(&ctx, &ast).expect_err("eval");
        assert!(matches!(err, EvalError::NotYetImplemented(_)));
    }

    #[test]
    fn rate_computes_per_second_delta_over_window() {
        let ds = WindowedDataSource {
            series: vec![series(
                &[("__name__", "counter"), ("host", "a")],
                // Counter rises 100 over 60s → rate is 100/60.
                &[(0, 0.0), (30_000, 50.0), (60_000, 100.0)],
            )],
        };
        let ctx = EvalContext { data: &ds, start_ms: 60_000, end_ms: 60_000, step_ms: 1 };
        let got = evaluate(&ctx, &parse("rate(counter[60s])").expect("parse")).expect("eval");
        assert_eq!(got.len(), 1);
        assert_eq!(lookup_label(&got[0], "__name__"), None);
        assert_eq!(lookup_label(&got[0], "host"), Some("a"));
        assert!((got[0].samples[0].value - (100.0 / 60.0)).abs() < 1e-9);
    }

    #[test]
    fn increase_returns_last_minus_first() {
        let ds = WindowedDataSource {
            series: vec![series(&[("__name__", "c")], &[(0, 5.0), (1000, 8.0), (2000, 12.0)])],
        };
        let ctx = EvalContext { data: &ds, start_ms: 2000, end_ms: 2000, step_ms: 1 };
        let got = evaluate(&ctx, &parse("increase(c[2s])").expect("parse")).expect("eval");
        assert_eq!(got[0].samples[0].value, 7.0);
    }

    #[test]
    fn delta_works_for_negative_change() {
        let ds = WindowedDataSource {
            series: vec![series(&[("__name__", "g")], &[(0, 100.0), (1000, 80.0)])],
        };
        let ctx = EvalContext { data: &ds, start_ms: 1000, end_ms: 1000, step_ms: 1 };
        let got = evaluate(&ctx, &parse("delta(g[1s])").expect("parse")).expect("eval");
        assert_eq!(got[0].samples[0].value, -20.0);
    }

    #[test]
    fn sum_over_time_adds_all_samples() {
        let ds = WindowedDataSource {
            series: vec![series(&[("__name__", "v")], &[(0, 1.0), (10, 2.0), (20, 3.0)])],
        };
        let ctx = EvalContext { data: &ds, start_ms: 20, end_ms: 20, step_ms: 1 };
        let got = evaluate(&ctx, &parse("sum_over_time(v[20ms])").expect("parse")).expect("eval");
        assert_eq!(got[0].samples[0].value, 6.0);
    }

    #[test]
    fn over_time_family_share_window() {
        let ds = WindowedDataSource {
            series: vec![series(&[("__name__", "v")], &[(0, 1.0), (10, 7.0), (20, 4.0)])],
        };
        let ctx = EvalContext { data: &ds, start_ms: 20, end_ms: 20, step_ms: 1 };
        let go = |q: &str| -> f64 {
            let got = evaluate(&ctx, &parse(q).expect("parse")).expect("eval");
            got[0].samples[0].value
        };
        assert_eq!(go("avg_over_time(v[20ms])"), 4.0);
        assert_eq!(go("min_over_time(v[20ms])"), 1.0);
        assert_eq!(go("max_over_time(v[20ms])"), 7.0);
        assert_eq!(go("count_over_time(v[20ms])"), 3.0);
        assert_eq!(go("first_over_time(v[20ms])"), 1.0);
        assert_eq!(go("last_over_time(v[20ms])"), 4.0);
    }

    #[test]
    fn rollup_fn_emits_at_query_anchor_not_sample_times() {
        let ds = WindowedDataSource {
            series: vec![series(&[("__name__", "v")], &[(100, 5.0), (200, 9.0)])],
        };
        let ctx = EvalContext { data: &ds, start_ms: 200, end_ms: 200, step_ms: 1 };
        let got = evaluate(&ctx, &parse("max_over_time(v[200ms])").expect("parse")).expect("eval");
        assert_eq!(got[0].samples.len(), 1);
        assert_eq!(got[0].samples[0].timestamp_ms, 200);
    }

    #[test]
    fn rate_without_window_is_rejected() {
        let ds = WindowedDataSource {
            series: vec![series(&[("__name__", "v")], &[(0, 1.0)])],
        };
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 0, step_ms: 1 };
        // No `[w]` window — current code requires it.
        let err = evaluate(&ctx, &parse("rate(v)").expect("parse")).expect_err("eval");
        assert!(matches!(err, EvalError::BadValue(_)));
    }

    /// Backend that always fails. Verifies the evaluator
    /// surfaces storage errors via `EvalError::DataSource`
    /// instead of swallowing or remapping them.
    struct FailingDataSource {
        message: &'static str,
    }

    impl DataSource for FailingDataSource {
        fn fetch(&self, _: &[Matcher], _: i64, _: i64)
            -> Result<Vec<Series>, DataSourceError>
        {
            Err(DataSourceError::new(self.message))
        }
    }

    #[test]
    fn data_source_error_propagates_as_evalerror_datasource() {
        let ds = FailingDataSource { message: "sqlite died" };
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 0, step_ms: 1 };
        let err = evaluate(&ctx, &parse("cpu").expect("parse")).expect_err("eval");
        match err {
            EvalError::DataSource(e) => assert_eq!(e.message, "sqlite died"),
            other => panic!("expected DataSource error, got {other:?}"),
        }
    }

    #[test]
    fn rate_over_range_query_emits_per_step() {
        let ds = WindowedDataSource {
            series: vec![series(
                &[("__name__", "c"), ("host", "h1")],
                &[(0, 0.0), (1000, 1.0), (2000, 2.0), (3000, 3.0)],
            )],
        };
        let ctx = EvalContext { data: &ds, start_ms: 1000, end_ms: 3000, step_ms: 1000 };
        let got = evaluate_range(&ctx, &parse("rate(c[1s])").expect("parse")).expect("eval");
        assert_eq!(got.len(), 1);
        // Per-second rate is 1.0 across the steady-rate counter.
        for sm in &got[0].samples {
            assert!((sm.value - 1.0).abs() < 1e-9);
        }
        let timestamps: Vec<i64> = got[0].samples.iter().map(|s| s.timestamp_ms).collect();
        assert_eq!(timestamps, vec![1000, 2000, 3000]);
    }

    #[test]
    fn scalar_op_scalar_produces_label_less_series() {
        let ds = MemoryDataSource { series: vec![] };
        let ctx = ctx_for(&ds);
        // Constant fold collapses `2 + 3` at parse time, so
        // wrap one side in a non-foldable construct.
        let ast = parse("2 + 3").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        // Folded already, but the eval path still has to
        // accept the resulting Number.
        assert_eq!(got.len(), 1);
        assert!(got[0].labels.is_empty());
        assert_eq!(got[0].samples[0].value, 5.0);
    }

    #[test]
    fn vector_plus_scalar_broadcasts() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0), (10, 2.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 3.0), (10, 4.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("cpu + 10").expect("parse");
        let mut got = evaluate(&ctx, &ast).expect("eval");
        got.sort_by(|a, b| lookup_label(a, "host").unwrap_or("").cmp(lookup_label(b, "host").unwrap_or("")));
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].samples[0].value, 11.0);
        assert_eq!(got[0].samples[1].value, 12.0);
        // Result drops `__name__` per upstream binary-op rules.
        assert_eq!(lookup_label(&got[0], "__name__"), None);
        assert_eq!(lookup_label(&got[0], "host"), Some("a"));
    }

    #[test]
    fn scalar_minus_vector_keeps_operand_order() {
        let ds = MemoryDataSource {
            series: vec![series(&[("__name__", "cpu"), ("host", "a")], &[(0, 4.0)])],
        };
        let ctx = ctx_for(&ds);
        // 10 - 4 = 6, NOT 4 - 10 = -6.
        let ast = parse("10 - cpu").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got[0].samples[0].value, 6.0);
    }

    #[test]
    fn vector_times_vector_matches_on_labels_drops_metric_name() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "a"), ("host", "h1")], &[(0, 2.0)]),
                series(&[("__name__", "a"), ("host", "h2")], &[(0, 5.0)]),
                series(&[("__name__", "b"), ("host", "h1")], &[(0, 3.0)]),
                series(&[("__name__", "b"), ("host", "h2")], &[(0, 7.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("a * b").expect("parse");
        let mut got = evaluate(&ctx, &ast).expect("eval");
        got.sort_by(|x, y| lookup_label(x, "host").unwrap_or("").cmp(lookup_label(y, "host").unwrap_or("")));
        assert_eq!(got.len(), 2);
        assert_eq!(lookup_label(&got[0], "__name__"), None);
        assert_eq!(lookup_label(&got[0], "host"), Some("h1"));
        assert_eq!(got[0].samples[0].value, 6.0);
        assert_eq!(lookup_label(&got[1], "host"), Some("h2"));
        assert_eq!(got[1].samples[0].value, 35.0);
    }

    #[test]
    fn vector_op_vector_unmatched_labels_drop_out() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "a"), ("host", "h1")], &[(0, 2.0)]),
                series(&[("__name__", "b"), ("host", "h2")], &[(0, 5.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("a + b").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        // No label-set overlap between the two series → empty.
        assert!(got.is_empty());
    }

    #[test]
    fn timestamp_alignment_inner_joins_samples() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "a"), ("host", "h1")], &[(10, 1.0), (20, 2.0), (30, 3.0)]),
                series(&[("__name__", "b"), ("host", "h1")], &[(20, 4.0), (30, 5.0), (40, 6.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("a + b").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 1);
        // Only timestamps 20 and 30 appear in both sides.
        assert_eq!(got[0].samples.len(), 2);
        assert_eq!(got[0].samples[0].timestamp_ms, 20);
        assert_eq!(got[0].samples[0].value, 6.0);
        assert_eq!(got[0].samples[1].timestamp_ms, 30);
        assert_eq!(got[0].samples[1].value, 8.0);
    }

    #[test]
    fn comparison_with_bool_produces_zero_or_one() {
        let ds = MemoryDataSource {
            series: vec![series(&[("__name__", "cpu"), ("host", "a")], &[(0, 4.0)])],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("cpu >bool 3").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got[0].samples[0].value, 1.0);

        let ast = parse("cpu >bool 5").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got[0].samples[0].value, 0.0);
    }

    #[test]
    fn comparisons_filter_mode_unsupported() {
        let ds = MemoryDataSource {
            series: vec![series(&[("__name__", "cpu"), ("host", "a")], &[(0, 4.0)])],
        };
        let ctx = ctx_for(&ds);
        // Without `bool`, comparison acts as a filter — defer.
        let ast = parse("cpu > 3").expect("parse");
        let err = evaluate(&ctx, &ast).expect_err("eval");
        assert!(matches!(err, EvalError::NotYetImplemented(_)));
    }

    #[test]
    fn vector_matching_modifiers_unsupported() {
        let ds = MemoryDataSource { series: vec![] };
        let ctx = ctx_for(&ds);
        let ast = parse("a + on(host) b").expect("parse");
        let err = evaluate(&ctx, &ast).expect_err("eval");
        assert!(matches!(err, EvalError::NotYetImplemented(_)));
    }

    /// Time-aware in-memory source: returns each series's
    /// samples filtered to the requested `[start, end]`. Lets
    /// range-query tests see what "the value at time T" means
    /// when T sweeps across the query window.
    struct WindowedDataSource {
        series: Vec<Series>,
    }

    impl DataSource for WindowedDataSource {
        fn fetch(&self, matchers: &[Matcher], start: i64, end: i64)
            -> Result<Vec<Series>, DataSourceError>
        {
            Ok(self.series.iter()
                .filter(|s| matchers.iter().all(|m| matches_series(m, s)))
                .map(|s| Series {
                    labels: s.labels.clone(),
                    samples: s.samples.iter()
                        .filter(|sm| sm.timestamp_ms >= start && sm.timestamp_ms <= end)
                        .cloned()
                        .collect(),
                })
                .collect())
        }
    }

    #[test]
    fn range_query_steps_through_window_and_merges_per_series() {
        let ds = WindowedDataSource {
            series: vec![series(
                &[("__name__", "cpu"), ("host", "a")],
                &[(0, 10.0), (10, 11.0), (20, 12.0), (30, 13.0)],
            )],
        };
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 30, step_ms: 10 };
        // Each step returns the [T, T] sample for the host=a series.
        let got = evaluate_range(&ctx, &parse("cpu").expect("parse")).expect("eval");
        assert_eq!(got.len(), 1);
        let ts: Vec<_> = got[0].samples.iter().map(|s| (s.timestamp_ms, s.value)).collect();
        assert_eq!(ts, vec![(0, 10.0), (10, 11.0), (20, 12.0), (30, 13.0)]);
    }

    #[test]
    fn range_query_aggregate_evaluates_per_step() {
        let ds = WindowedDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0), (10, 2.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 3.0), (10, 4.0)]),
            ],
        };
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 10, step_ms: 10 };
        let got = evaluate_range(&ctx, &parse("sum(cpu)").expect("parse")).expect("eval");
        assert_eq!(got.len(), 1);
        let ts: Vec<_> = got[0].samples.iter().map(|s| (s.timestamp_ms, s.value)).collect();
        // sum at T=0: 1+3=4. sum at T=10: 2+4=6.
        assert_eq!(ts, vec![(0, 4.0), (10, 6.0)]);
    }

    #[test]
    fn range_query_step_zero_is_rejected() {
        let ds = WindowedDataSource { series: vec![] };
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 10, step_ms: 0 };
        let err = evaluate_range(&ctx, &parse("cpu").expect("parse")).expect_err("eval");
        assert!(matches!(err, EvalError::BadValue(_)));
    }

    #[test]
    fn range_query_clamps_final_step_to_end() {
        // Step of 7 over [0, 20] should hit T=0, 7, 14, 20 —
        // the last step lands on the boundary even though
        // it's a partial step from the previous one.
        let ds = WindowedDataSource {
            series: vec![series(
                &[("__name__", "cpu")],
                &[(0, 1.0), (7, 2.0), (14, 3.0), (20, 4.0)],
            )],
        };
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 20, step_ms: 7 };
        let got = evaluate_range(&ctx, &parse("cpu").expect("parse")).expect("eval");
        let timestamps: Vec<i64> = got[0].samples.iter().map(|s| s.timestamp_ms).collect();
        assert_eq!(timestamps, vec![0, 7, 14, 20]);
    }

    #[test]
    fn range_query_instant_window_yields_one_step() {
        let ds = WindowedDataSource {
            series: vec![series(&[("__name__", "cpu")], &[(5, 42.0)])],
        };
        let ctx = EvalContext { data: &ds, start_ms: 5, end_ms: 5, step_ms: 1 };
        let got = evaluate_range(&ctx, &parse("cpu").expect("parse")).expect("eval");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].samples.len(), 1);
        assert_eq!(got[0].samples[0].value, 42.0);
    }

    #[test]
    fn aggregate_then_arithmetic_composes() {
        let ds = MemoryDataSource {
            series: vec![
                series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0)]),
                series(&[("__name__", "cpu"), ("host", "b")], &[(0, 2.0)]),
                series(&[("__name__", "cpu"), ("host", "c")], &[(0, 3.0)]),
            ],
        };
        let ctx = ctx_for(&ds);
        let ast = parse("sum(cpu) * 2").expect("parse");
        let got = evaluate(&ctx, &ast).expect("eval");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].samples[0].value, 12.0);
    }
}
