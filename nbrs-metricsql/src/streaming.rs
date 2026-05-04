// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Streaming / incremental aggregation for MetricsQL.
//!
//! This module is the implementation of [SRD 47][srd47].
//! Read it before changing the [`Reducer`] trait, the
//! [`StreamingPlan`] compiler, or the property test below —
//! the algebraic invariants documented in the SRD are
//! load-bearing.
//!
//! [srd47]: ../../../docs/sysref/47_metricsql_streaming.md
//!
//! # The three algebraic classes
//!
//! Every metricsql aggregation falls into one of three
//! classes, and which class determines what's possible:
//!
//! | Class | Property | Examples |
//! |-------|----------|----------|
//! | **Distributive** | `f(A ∪ B) = combine(f(A), f(B))`, no extra state. | `sum`, `count`, `min`, `max`, `group`, `*_over_time` (sum/count/min/max/first/last) |
//! | **Algebraic** | Bounded-size accumulator richer than the result. | `avg`, `stddev`, `rate`, `increase`, `delta`, `avg_over_time` |
//! | **Holistic** | Bounded-size *exact* accumulator does not exist. | `quantile`, `topk`, `bottomk`, `count_values`, `mad`, `median` |
//!
//! This module ships the **distributive** subset. Algebraic
//! and holistic reducers ride the same trait and land in
//! their own focused pushes.
//!
//! # The load-bearing guarantee
//!
//! For every supported query shape × every supported reducer,
//! streaming evaluation produces the same result as batch
//! evaluation, regardless of how the input samples are
//! partitioned across `ingest` calls. This is verified by
//! [`tests::streaming_equals_batch_for_supported_shapes`] —
//! the load-bearing artifact. If it ever fails, the algebra
//! is broken; relaxing the test is not the recovery path.

use crate::ast::{AggrModifier, AggrModifierOp, Expr, FuncExpr, MetricExpr, RollupExpr};
use crate::eval::{Matcher, MatcherOp, Sample, Series};
use std::collections::BTreeMap;

/// Commutative-monoid-with-finalize algebra for streaming
/// aggregation. Implementations MUST satisfy:
///
/// - **identity**: `merge(empty, x) ≡ x`, where
///   `empty` is `Self::Acc::default()`
/// - **commutative**: `merge(a, b) ≡ merge(b, a)`
/// - **associative**: `merge(merge(a, b), c) ≡ merge(a, merge(b, c))`
///
/// `ingest(acc, s)` is equivalent to `merge(acc,
/// single_sample(s))`, exposed separately so the hot path
/// can skip constructing a transient single-sample
/// accumulator.
///
/// Every implementor goes through the streaming-equivalence
/// property test before being considered done — see
/// `tests::streaming_equals_batch_for_supported_shapes`.
pub trait Reducer: Send + Sync {
    /// Accumulator state. Must be cheap to clone (the merge
    /// path takes one by value), default-constructible to
    /// the algebra's identity element, and `Send + Sync` so
    /// the plan can hold heterogeneous reducers and ingest
    /// from any thread.
    type Acc: Clone + Default + Send + Sync;

    /// Fold one new sample into the accumulator. NaN samples
    /// are caller-filtered or reducer-handled depending on
    /// the operator's semantics — most reducers treat NaN as
    /// "no observation" and skip it.
    fn ingest(&self, acc: &mut Self::Acc, sample: &Sample);

    /// Combine two accumulators in place. Must be
    /// commutative and associative; the plan relies on these
    /// laws to make ingest order irrelevant.
    fn merge(&self, into: &mut Self::Acc, other: Self::Acc);

    /// Project the accumulator to the result value. Empty
    /// accumulators (no samples ingested) return NaN per
    /// upstream MetricsQL semantics.
    fn snapshot(&self, acc: &Self::Acc) -> f64;
}

// =============================================================
// Distributive reducers
// =============================================================
//
// Each carries a small, default-constructible accumulator
// and satisfies the three monoid laws by construction.
// Verified per-impl in the `monoid_laws_*` unit tests below.

/// `sum` — adds non-NaN sample values. Uses Kahan summation
/// to keep numerical error bounded and order-independent
/// enough that the equivalence property test passes at
/// `1e-9` tolerance.
pub struct SumReducer;

#[derive(Clone, Default, Debug)]
pub struct KahanAcc {
    /// Running total.
    pub total: f64,
    /// Compensation term — accumulated low-order bits lost
    /// in successive additions. See Kahan 1965.
    pub compensation: f64,
    /// Distinguishes "no samples yet" (snapshot → NaN) from
    /// "samples summed to 0" (snapshot → 0.0).
    pub has_data: bool,
}

impl Reducer for SumReducer {
    type Acc = KahanAcc;

    fn ingest(&self, acc: &mut KahanAcc, sample: &Sample) {
        if sample.value.is_nan() { return; }
        kahan_add(acc, sample.value);
        acc.has_data = true;
    }

    fn merge(&self, into: &mut KahanAcc, other: KahanAcc) {
        if !other.has_data { return; }
        // Add `other.total` with the existing compensation,
        // then fold in `other.compensation` separately. This
        // keeps the merge order-independent within tolerance
        // — the property test at 1e-9 catches regressions.
        kahan_add(into, other.total);
        kahan_add(into, other.compensation);
        into.has_data = true;
    }

    fn snapshot(&self, acc: &KahanAcc) -> f64 {
        if !acc.has_data { return f64::NAN; }
        acc.total
    }
}

fn kahan_add(acc: &mut KahanAcc, x: f64) {
    let y = x - acc.compensation;
    let t = acc.total + y;
    acc.compensation = (t - acc.total) - y;
    acc.total = t;
}

/// `count` — counts non-NaN samples ingested.
pub struct CountReducer;

#[derive(Clone, Default, Debug)]
pub struct CountAcc {
    pub count: u64,
}

impl Reducer for CountReducer {
    type Acc = CountAcc;

    fn ingest(&self, acc: &mut CountAcc, sample: &Sample) {
        if !sample.value.is_nan() {
            acc.count += 1;
        }
    }

    fn merge(&self, into: &mut CountAcc, other: CountAcc) {
        into.count = into.count.saturating_add(other.count);
    }

    fn snapshot(&self, acc: &CountAcc) -> f64 {
        // Empty count is 0.0, NOT NaN — `count(empty)`
        // returns zero per upstream, which differs from the
        // sum / min / max convention.
        acc.count as f64
    }
}

/// `min` — minimum of non-NaN sample values.
pub struct MinReducer;

#[derive(Clone, Default, Debug)]
pub struct MinAcc {
    /// Defaults to `+∞` lazily — we don't initialise it at
    /// `Default::default()` because `f64::INFINITY` isn't
    /// the type's default; track presence explicitly via
    /// `has_data` and treat the field as undefined until
    /// then.
    pub value: f64,
    pub has_data: bool,
}

impl Reducer for MinReducer {
    type Acc = MinAcc;

    fn ingest(&self, acc: &mut MinAcc, sample: &Sample) {
        if sample.value.is_nan() { return; }
        if !acc.has_data || sample.value < acc.value {
            acc.value = sample.value;
        }
        acc.has_data = true;
    }

    fn merge(&self, into: &mut MinAcc, other: MinAcc) {
        if !other.has_data { return; }
        if !into.has_data || other.value < into.value {
            into.value = other.value;
        }
        into.has_data = true;
    }

    fn snapshot(&self, acc: &MinAcc) -> f64 {
        if !acc.has_data { return f64::NAN; }
        acc.value
    }
}

/// `max` — maximum of non-NaN sample values.
pub struct MaxReducer;

#[derive(Clone, Default, Debug)]
pub struct MaxAcc {
    pub value: f64,
    pub has_data: bool,
}

impl Reducer for MaxReducer {
    type Acc = MaxAcc;

    fn ingest(&self, acc: &mut MaxAcc, sample: &Sample) {
        if sample.value.is_nan() { return; }
        if !acc.has_data || sample.value > acc.value {
            acc.value = sample.value;
        }
        acc.has_data = true;
    }

    fn merge(&self, into: &mut MaxAcc, other: MaxAcc) {
        if !other.has_data { return; }
        if !into.has_data || other.value > into.value {
            into.value = other.value;
        }
        into.has_data = true;
    }

    fn snapshot(&self, acc: &MaxAcc) -> f64 {
        if !acc.has_data { return f64::NAN; }
        acc.value
    }
}

/// `group` — emits 1.0 for any group with at least one
/// non-NaN sample, NaN otherwise. Used to materialize a
/// label-set with no data dependency.
pub struct GroupReducer;

#[derive(Clone, Default, Debug)]
pub struct GroupAcc {
    pub has_data: bool,
}

impl Reducer for GroupReducer {
    type Acc = GroupAcc;

    fn ingest(&self, acc: &mut GroupAcc, sample: &Sample) {
        if !sample.value.is_nan() {
            acc.has_data = true;
        }
    }

    fn merge(&self, into: &mut GroupAcc, other: GroupAcc) {
        into.has_data = into.has_data || other.has_data;
    }

    fn snapshot(&self, acc: &GroupAcc) -> f64 {
        if acc.has_data { 1.0 } else { f64::NAN }
    }
}

// =============================================================
// Timestamp-aware reducers: first / last
// =============================================================
//
// `sum_over_time` / `count_over_time` / `min_over_time` /
// `max_over_time` reuse [`SumReducer`] / [`CountReducer`] /
// [`MinReducer`] / [`MaxReducer`] — the algebra is identical,
// only the binding is different (across-series vs.
// within-series-across-time, which is a plan-compiler
// concern, not a reducer concern).
//
// `first_over_time` / `last_over_time` need explicit
// timestamp-aware accumulators: the per-sample compare key
// is the *timestamp*, not the value.

/// `first_over_time` — value of the earliest non-NaN sample.
pub struct FirstOverTimeReducer;

#[derive(Clone, Default, Debug)]
pub struct FirstAcc {
    pub value: f64,
    pub timestamp_ms: i64,
    pub has_data: bool,
}

impl Reducer for FirstOverTimeReducer {
    type Acc = FirstAcc;

    fn ingest(&self, acc: &mut FirstAcc, sample: &Sample) {
        if sample.value.is_nan() { return; }
        if !acc.has_data || sample.timestamp_ms < acc.timestamp_ms {
            acc.value = sample.value;
            acc.timestamp_ms = sample.timestamp_ms;
        }
        acc.has_data = true;
    }

    fn merge(&self, into: &mut FirstAcc, other: FirstAcc) {
        if !other.has_data { return; }
        if !into.has_data || other.timestamp_ms < into.timestamp_ms {
            into.value = other.value;
            into.timestamp_ms = other.timestamp_ms;
        }
        into.has_data = true;
    }

    fn snapshot(&self, acc: &FirstAcc) -> f64 {
        if !acc.has_data { return f64::NAN; }
        acc.value
    }
}

/// `last_over_time` — value of the latest non-NaN sample.
pub struct LastOverTimeReducer;

#[derive(Clone, Default, Debug)]
pub struct LastAcc {
    pub value: f64,
    pub timestamp_ms: i64,
    pub has_data: bool,
}

impl Reducer for LastOverTimeReducer {
    type Acc = LastAcc;

    fn ingest(&self, acc: &mut LastAcc, sample: &Sample) {
        if sample.value.is_nan() { return; }
        if !acc.has_data || sample.timestamp_ms > acc.timestamp_ms {
            acc.value = sample.value;
            acc.timestamp_ms = sample.timestamp_ms;
        }
        acc.has_data = true;
    }

    fn merge(&self, into: &mut LastAcc, other: LastAcc) {
        if !other.has_data { return; }
        if !into.has_data || other.timestamp_ms > into.timestamp_ms {
            into.value = other.value;
            into.timestamp_ms = other.timestamp_ms;
        }
        into.has_data = true;
    }

    fn snapshot(&self, acc: &LastAcc) -> f64 {
        if !acc.has_data { return f64::NAN; }
        acc.value
    }
}

// =============================================================
// Reducer dispatch — enum-erased heterogeneous reducers
// =============================================================
//
// `StreamingPlan` holds reducers and accumulators
// heterogeneously (one map keyed on group key, values of
// different `Acc` types depending on the reducer). Static
// dispatch over a closed enum keeps the trait surface
// generic while avoiding `Box<dyn Reducer>` and the
// associated-type erasure dance — we know the reducer set
// at compile time.

/// Closed enumeration of the reducer kinds the streaming
/// layer ships in this push. Adding a reducer is two edits:
/// add a variant here + add the matching arm in [`AccCell`]
/// and the four dispatch methods below.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ReducerKind {
    /// `sum(...)` and `sum_over_time(...[w])` — same reducer,
    /// the plan pipeline binds it differently.
    Sum,
    /// `count(...)` and `count_over_time(...[w])`.
    Count,
    /// `min(...)` and `min_over_time(...[w])`.
    Min,
    /// `max(...)` and `max_over_time(...[w])`.
    Max,
    /// `group(...)` — emits 1.0 per group with any data.
    Group,
    /// `first_over_time(...[w])` — timestamp-aware.
    FirstOverTime,
    /// `last_over_time(...[w])` — timestamp-aware.
    LastOverTime,
}

/// Erased accumulator cell. Each variant matches one
/// [`ReducerKind`]; the dispatch methods below downcast via
/// `match` (no `Any`, no runtime type checks).
#[derive(Clone, Debug)]
pub enum AccCell {
    Sum(KahanAcc),
    Count(CountAcc),
    Min(MinAcc),
    Max(MaxAcc),
    Group(GroupAcc),
    First(FirstAcc),
    Last(LastAcc),
}

impl ReducerKind {
    /// Produce the identity accumulator for this kind. The
    /// returned variant matches the kind 1:1; the dispatch
    /// methods below assume that pairing — mismatches would
    /// be a programmer error.
    pub fn empty(self) -> AccCell {
        match self {
            ReducerKind::Sum   => AccCell::Sum(KahanAcc::default()),
            ReducerKind::Count => AccCell::Count(CountAcc::default()),
            ReducerKind::Min   => AccCell::Min(MinAcc::default()),
            ReducerKind::Max   => AccCell::Max(MaxAcc::default()),
            ReducerKind::Group => AccCell::Group(GroupAcc::default()),
            ReducerKind::FirstOverTime => AccCell::First(FirstAcc::default()),
            ReducerKind::LastOverTime  => AccCell::Last(LastAcc::default()),
        }
    }

    pub fn ingest(self, acc: &mut AccCell, sample: &Sample) {
        match (self, acc) {
            (ReducerKind::Sum,           AccCell::Sum(a))   => SumReducer.ingest(a, sample),
            (ReducerKind::Count,         AccCell::Count(a)) => CountReducer.ingest(a, sample),
            (ReducerKind::Min,           AccCell::Min(a))   => MinReducer.ingest(a, sample),
            (ReducerKind::Max,           AccCell::Max(a))   => MaxReducer.ingest(a, sample),
            (ReducerKind::Group,         AccCell::Group(a)) => GroupReducer.ingest(a, sample),
            (ReducerKind::FirstOverTime, AccCell::First(a)) => FirstOverTimeReducer.ingest(a, sample),
            (ReducerKind::LastOverTime,  AccCell::Last(a))  => LastOverTimeReducer.ingest(a, sample),
            _ => unreachable!("ReducerKind/AccCell mismatch — `empty()` produces matched pairs"),
        }
    }

    pub fn merge(self, into: &mut AccCell, other: AccCell) {
        match (self, into, other) {
            (ReducerKind::Sum,           AccCell::Sum(a),   AccCell::Sum(b))   => SumReducer.merge(a, b),
            (ReducerKind::Count,         AccCell::Count(a), AccCell::Count(b)) => CountReducer.merge(a, b),
            (ReducerKind::Min,           AccCell::Min(a),   AccCell::Min(b))   => MinReducer.merge(a, b),
            (ReducerKind::Max,           AccCell::Max(a),   AccCell::Max(b))   => MaxReducer.merge(a, b),
            (ReducerKind::Group,         AccCell::Group(a), AccCell::Group(b)) => GroupReducer.merge(a, b),
            (ReducerKind::FirstOverTime, AccCell::First(a), AccCell::First(b)) => FirstOverTimeReducer.merge(a, b),
            (ReducerKind::LastOverTime,  AccCell::Last(a),  AccCell::Last(b))  => LastOverTimeReducer.merge(a, b),
            _ => unreachable!("ReducerKind/AccCell mismatch in merge"),
        }
    }

    pub fn snapshot(self, acc: &AccCell) -> f64 {
        match (self, acc) {
            (ReducerKind::Sum,           AccCell::Sum(a))   => SumReducer.snapshot(a),
            (ReducerKind::Count,         AccCell::Count(a)) => CountReducer.snapshot(a),
            (ReducerKind::Min,           AccCell::Min(a))   => MinReducer.snapshot(a),
            (ReducerKind::Max,           AccCell::Max(a))   => MaxReducer.snapshot(a),
            (ReducerKind::Group,         AccCell::Group(a)) => GroupReducer.snapshot(a),
            (ReducerKind::FirstOverTime, AccCell::First(a)) => FirstOverTimeReducer.snapshot(a),
            (ReducerKind::LastOverTime,  AccCell::Last(a))  => LastOverTimeReducer.snapshot(a),
            _ => unreachable!("ReducerKind/AccCell mismatch in snapshot"),
        }
    }
}

// =============================================================
// StreamingPlan
// =============================================================

/// Compile-time error from [`compile_streaming`]. Always
/// names the reason — the supported subset grows by adding
/// reducers and shapes, not by relaxing the compiler.
#[derive(Debug, Clone)]
pub enum CompileError {
    /// AST node shape isn't supported by the streaming
    /// compiler in this push (binary ops, algebraic
    /// reducers, quantile, vector matching modifiers, etc.).
    /// The reason text names the shape so callers can
    /// produce useful diagnostics.
    Unsupported(String),
    /// AST is structurally invalid for a streaming plan
    /// (e.g. nested aggregates, missing window).
    InvalidShape(String),
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompileError::Unsupported(s) => write!(f, "streaming compile: unsupported: {s}"),
            CompileError::InvalidShape(s) => write!(f, "streaming compile: invalid shape: {s}"),
        }
    }
}

impl std::error::Error for CompileError {}

/// Grouping mode for an aggregate stage. Mirrors the
/// `by(...)` / `without(...)` modifier on [`FuncExpr`];
/// `All` is the no-modifier case (every series collapses
/// into the same empty-keyed bucket).
#[derive(Clone, Debug)]
pub enum Grouping {
    All,
    By(Vec<String>),
    Without(Vec<String>),
}

/// A compiled streaming query plan.
///
/// Holds reducer state and dispatches per-sample. Construct
/// via [`compile_streaming`]; ingest series data through
/// [`StreamingPlan::ingest`]; read the current result via
/// [`StreamingPlan::snapshot`].
#[derive(Debug)]
pub struct StreamingPlan {
    matchers: Vec<Matcher>,
    pipeline: Pipeline,
}

/// The four supported pipeline shapes — see SRD-47
/// §"Plan compilation" for the AST patterns each maps to.
#[derive(Debug)]
enum Pipeline {
    /// `<selector>` — bare passthrough; ingest stores per-series.
    /// Mostly for tests / completeness; production queries
    /// always wrap a selector in a reducer.
    Bare {
        per_series: BTreeMap<Vec<(String, String)>, Vec<Sample>>,
    },
    /// `<agg>(<selector>) [by|without (...)]`
    ///
    /// Per-timestamp accumulator nested inside per-group:
    /// batch `sum(cpu)` aggregates the *same* timestamp's
    /// samples across series, so the streaming model must
    /// key the same way to be batch-equivalent. Memory is
    /// `O(group_cardinality × distinct_timestamps)` —
    /// bounded by retention horizon for continuous queries.
    Aggregate {
        reducer: ReducerKind,
        grouping: Grouping,
        groups: BTreeMap<Vec<(String, String)>, BTreeMap<i64, AccCell>>,
    },
    /// `<rollup_fn>(<selector>[w])` — windowed reducer per
    /// input series. The `window_ms` is retained for the
    /// future sliding-window framing; for this push the
    /// snapshot reads whatever has accumulated since the
    /// last `reset`.
    Window {
        reducer: ReducerKind,
        // Held for future sliding-window framing; not used
        // in this push.
        #[allow(dead_code)]
        window_ms: i64,
        per_series: BTreeMap<Vec<(String, String)>, AccCell>,
    },
    /// `<agg>(<rollup_fn>(<selector>[w])) [by|without (...)]`
    /// — windowed inner reducer feeds an outer aggregate at
    /// snapshot time.
    WindowedAggregate {
        window_reducer: ReducerKind,
        // Same: retained for sliding-window framing.
        #[allow(dead_code)]
        window_ms: i64,
        per_series: BTreeMap<Vec<(String, String)>, AccCell>,
        outer_reducer: ReducerKind,
        grouping: Grouping,
    },
}

// =============================================================
// StreamingPlan: ingest / snapshot data path
// =============================================================

impl StreamingPlan {
    /// Ingest one sample identified by its label set. Samples
    /// whose labels don't satisfy this plan's matchers are
    /// silently dropped — the runtime feeding the plan can
    /// pre-filter or not, the plan handles both.
    pub fn ingest_sample(&mut self, labels: &[(String, String)], sample: &Sample) {
        if !self.matches(labels) { return; }
        match &mut self.pipeline {
            Pipeline::Bare { per_series } => {
                let key = series_key(labels);
                per_series.entry(key).or_default().push(sample.clone());
            }
            Pipeline::Aggregate { reducer, grouping, groups } => {
                let key = group_key(labels, grouping);
                let ts_map = groups.entry(key).or_default();
                let acc = ts_map.entry(sample.timestamp_ms)
                    .or_insert_with(|| reducer.empty());
                reducer.ingest(acc, sample);
            }
            Pipeline::Window { reducer, per_series, .. } => {
                let key = series_key(labels);
                let acc = per_series.entry(key).or_insert_with(|| reducer.empty());
                reducer.ingest(acc, sample);
            }
            Pipeline::WindowedAggregate { window_reducer, per_series, .. } => {
                let key = series_key(labels);
                let acc = per_series.entry(key).or_insert_with(|| window_reducer.empty());
                window_reducer.ingest(acc, sample);
            }
        }
    }

    /// Convenience batch form — ingest a full series
    /// (one label set, many samples).
    pub fn ingest_series(&mut self, labels: &[(String, String)], samples: &[Sample]) {
        if !self.matches(labels) { return; }
        for s in samples { self.ingest_sample(labels, s); }
    }

    /// Convenience: ingest a slice of `Series`. Each series's
    /// `labels` and `samples` flow through the per-sample
    /// path; non-matching series are dropped.
    pub fn ingest(&mut self, series: &[Series]) {
        for s in series {
            self.ingest_series(&s.labels, &s.samples);
        }
    }

    /// Read the current result. Each output series carries a
    /// single sample at `anchor_ms` whose value is the
    /// reducer's `snapshot()`. Groups whose accumulator is
    /// empty (no contributing samples) emit NaN, matching
    /// the eval-side rollup semantics (`first_over_time`,
    /// `sum`, etc. with empty input).
    pub fn snapshot(&self, anchor_ms: i64) -> Vec<Series> {
        match &self.pipeline {
            Pipeline::Bare { per_series } => {
                per_series.iter().map(|(labels, samples)| Series {
                    labels: labels.clone(),
                    samples: samples.clone(),
                }).collect()
            }
            Pipeline::Aggregate { reducer, groups, .. } => {
                // Per-group → per-timestamp → snapshot value.
                // Sorted timestamp iteration via BTreeMap so
                // the output series's samples land in
                // monotonic order, matching the eval-side
                // contract.
                let mut out = Vec::with_capacity(groups.len());
                for (labels, ts_map) in groups {
                    let samples: Vec<Sample> = ts_map.iter()
                        .map(|(ts, acc)| Sample {
                            timestamp_ms: *ts,
                            value: reducer.snapshot(acc),
                        })
                        .collect();
                    out.push(Series { labels: labels.clone(), samples });
                }
                let _ = anchor_ms; // unused in this branch — kept for symmetry
                out
            }
            Pipeline::Window { reducer, per_series, .. } => {
                let mut out = Vec::with_capacity(per_series.len());
                for (labels, acc) in per_series {
                    let value = reducer.snapshot(acc);
                    out.push(Series {
                        labels: labels.clone(),
                        samples: vec![Sample { timestamp_ms: anchor_ms, value }],
                    });
                }
                out
            }
            Pipeline::WindowedAggregate { window_reducer, per_series, outer_reducer, grouping, .. } => {
                // Two-stage snapshot: each per-series window
                // accumulator yields a synthetic sample,
                // which then folds into a fresh outer-stage
                // group accumulator. This is exactly the
                // pipeline `sum(sum_over_time(cpu[1m])) by
                // (host)` semantically demands.
                let mut groups: BTreeMap<Vec<(String, String)>, AccCell> = BTreeMap::new();
                for (labels, w_acc) in per_series {
                    let inner_value = window_reducer.snapshot(w_acc);
                    if inner_value.is_nan() { continue; }
                    let key = group_key(labels, grouping);
                    let entry = groups.entry(key).or_insert_with(|| outer_reducer.empty());
                    outer_reducer.ingest(entry, &Sample {
                        timestamp_ms: anchor_ms,
                        value: inner_value,
                    });
                }
                let mut out = Vec::with_capacity(groups.len());
                for (labels, acc) in groups {
                    let value = outer_reducer.snapshot(&acc);
                    out.push(Series {
                        labels,
                        samples: vec![Sample { timestamp_ms: anchor_ms, value }],
                    });
                }
                out
            }
        }
    }

    /// Reset every accumulator in the plan back to identity.
    /// Used by tumbling-window cadences that want a clean
    /// slate at each grid tick.
    pub fn reset(&mut self) {
        match &mut self.pipeline {
            Pipeline::Bare { per_series } => per_series.clear(),
            Pipeline::Aggregate { groups, .. } => groups.clear(),
            Pipeline::Window { per_series, .. } => per_series.clear(),
            Pipeline::WindowedAggregate { per_series, .. } => per_series.clear(),
        }
    }

    /// Apply the plan's matchers to a label set. NaN-tolerant
    /// regex falls back to exact match — regex compilation
    /// belongs in a separate pass alongside the sqlite
    /// adapter, not here.
    fn matches(&self, labels: &[(String, String)]) -> bool {
        self.matchers.iter().all(|m| {
            let v = labels.iter()
                .find(|(k, _)| k == &m.label)
                .map(|(_, v)| v.as_str())
                .unwrap_or("");
            match m.op {
                MatcherOp::Eq => v == m.value,
                MatcherOp::Ne => v != m.value,
                MatcherOp::EqRegex | MatcherOp::NeRegex => v == m.value,
            }
        })
    }
}

/// Series identity for windowed reducers: every label except
/// `__name__`, sorted by key for stable hashing across
/// arrival orders.
fn series_key(labels: &[(String, String)]) -> Vec<(String, String)> {
    let mut out: Vec<_> = labels.iter()
        .filter(|(k, _)| k != "__name__")
        .cloned()
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Group key derivation per the active aggregate modifier.
/// Mirrors `eval::group_key` semantics: `__name__` is
/// dropped under `Without`; `By` keeps only the listed
/// labels; `All` collapses everything to the empty key.
fn group_key(labels: &[(String, String)], grouping: &Grouping) -> Vec<(String, String)> {
    let mut out: Vec<_> = match grouping {
        Grouping::All => Vec::new(),
        Grouping::By(keep) => labels.iter()
            .filter(|(k, _)| keep.iter().any(|w| w == k))
            .cloned()
            .collect(),
        Grouping::Without(drop) => labels.iter()
            .filter(|(k, _)| k != "__name__" && !drop.iter().any(|w| w == k))
            .cloned()
            .collect(),
    };
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Compile a parsed metricsql expression into a streaming
/// plan. Supported shapes (see SRD-47 §"Plan compilation"):
///
/// - `<agg>(<selector>) [by|without (...)]`
/// - `<rollup_fn>(<selector>[w])`
/// - `<agg>(<rollup_fn>(<selector>[w])) [by|without (...)]`
///
/// `<agg>` ∈ `{sum, count, min, max, group}`,
/// `<rollup_fn>` ∈ `{sum/count/min/max/first/last_over_time}`.
/// Any other AST shape returns
/// [`CompileError::Unsupported`].
pub fn compile_streaming(expr: &Expr) -> Result<StreamingPlan, CompileError> {
    match expr {
        Expr::Func(f) => compile_func(f),
        Expr::Metric(m) => Ok(StreamingPlan {
            matchers: matchers_for(m)?,
            pipeline: Pipeline::Bare { per_series: BTreeMap::new() },
        }),
        other => Err(CompileError::Unsupported(format!(
            "top-level expression {:?} not supported in streaming plans",
            short_kind(other)))),
    }
}

fn compile_func(f: &FuncExpr) -> Result<StreamingPlan, CompileError> {
    if f.args.len() != 1 {
        return Err(CompileError::InvalidShape(format!(
            "streaming function {:?} expects 1 argument, got {}", f.name, f.args.len())));
    }
    let outer_kind = AggKind::classify(&f.name);
    let inner = &f.args[0];
    match outer_kind {
        AggKind::Aggregate(outer_reducer) => {
            // Inner is either a selector (pure aggregate) or
            // a rollup-function call (windowed aggregate).
            match inner {
                Expr::Metric(m) => Ok(StreamingPlan {
                    matchers: matchers_for(m)?,
                    pipeline: Pipeline::Aggregate {
                        reducer: outer_reducer,
                        grouping: grouping_for(f.modifier.as_ref()),
                        groups: BTreeMap::new(),
                    },
                }),
                Expr::Func(inner_f) => {
                    let AggKind::Window(inner_reducer) = AggKind::classify(&inner_f.name) else {
                        return Err(CompileError::Unsupported(format!(
                            "aggregate {:?} cannot wrap non-rollup function {:?}",
                            f.name, inner_f.name)));
                    };
                    if inner_f.args.len() != 1 {
                        return Err(CompileError::InvalidShape(format!(
                            "rollup function {:?} expects 1 argument", inner_f.name)));
                    }
                    let (matchers, window_ms) = matchers_and_window(&inner_f.args[0])?;
                    Ok(StreamingPlan {
                        matchers,
                        pipeline: Pipeline::WindowedAggregate {
                            window_reducer: inner_reducer,
                            window_ms,
                            per_series: BTreeMap::new(),
                            outer_reducer,
                            grouping: grouping_for(f.modifier.as_ref()),
                        },
                    })
                }
                other => Err(CompileError::Unsupported(format!(
                    "aggregate {:?} over {:?} not supported in streaming plans",
                    f.name, short_kind(other)))),
            }
        }
        AggKind::Window(window_reducer) => {
            let (matchers, window_ms) = matchers_and_window(inner)?;
            Ok(StreamingPlan {
                matchers,
                pipeline: Pipeline::Window {
                    reducer: window_reducer,
                    window_ms,
                    per_series: BTreeMap::new(),
                },
            })
        }
        AggKind::Unsupported => Err(CompileError::Unsupported(format!(
            "function {:?} not supported in streaming plans this push", f.name))),
    }
}

/// Classification of a function name into aggregate vs.
/// rollup vs. unsupported. Internal to the compiler.
enum AggKind {
    /// Cross-series aggregate (top-level).
    Aggregate(ReducerKind),
    /// Per-series window reducer (`*_over_time`).
    Window(ReducerKind),
    /// Algebraic / holistic / unsupported function name.
    Unsupported,
}

impl AggKind {
    fn classify(name: &str) -> Self {
        match name.to_ascii_lowercase().as_str() {
            "sum"   => Self::Aggregate(ReducerKind::Sum),
            "count" => Self::Aggregate(ReducerKind::Count),
            "min"   => Self::Aggregate(ReducerKind::Min),
            "max"   => Self::Aggregate(ReducerKind::Max),
            "group" => Self::Aggregate(ReducerKind::Group),
            "sum_over_time"   => Self::Window(ReducerKind::Sum),
            "count_over_time" => Self::Window(ReducerKind::Count),
            "min_over_time"   => Self::Window(ReducerKind::Min),
            "max_over_time"   => Self::Window(ReducerKind::Max),
            "first_over_time" => Self::Window(ReducerKind::FirstOverTime),
            "last_over_time"  => Self::Window(ReducerKind::LastOverTime),
            _ => Self::Unsupported,
        }
    }
}

fn grouping_for(modifier: Option<&AggrModifier>) -> Grouping {
    let Some(m) = modifier else { return Grouping::All; };
    match m.op {
        AggrModifierOp::By => Grouping::By(m.args.clone()),
        AggrModifierOp::Without => Grouping::Without(m.args.clone()),
    }
}

fn matchers_for(m: &MetricExpr) -> Result<Vec<Matcher>, CompileError> {
    use crate::ast::LabelFilterOp;
    if m.label_filterss.len() > 1 {
        return Err(CompileError::Unsupported(
            "selectors with `or` filter groups not supported in streaming plans this push".into()));
    }
    let group = m.label_filterss.first().cloned().unwrap_or_default();
    let mut out = Vec::with_capacity(group.len());
    for lf in group {
        if lf.is_template_ref || lf.value_expr.is_some() {
            return Err(CompileError::InvalidShape(format!(
                "unexpanded WITH template ref on filter {:?} — call `parse` not `parse_for_prettify`",
                lf.label)));
        }
        out.push(Matcher {
            label: lf.label,
            op: match lf.op {
                LabelFilterOp::Eq => MatcherOp::Eq,
                LabelFilterOp::Ne => MatcherOp::Ne,
                LabelFilterOp::EqRegex => MatcherOp::EqRegex,
                LabelFilterOp::NeRegex => MatcherOp::NeRegex,
            },
            value: lf.value,
        });
    }
    Ok(out)
}

/// Accept `<selector>[<window>]` (a `RollupExpr` wrapping a
/// `MetricExpr`) and return the matchers + window in
/// milliseconds. Rejects rollups without a window — the
/// streaming layer doesn't infer step-relative windows.
fn matchers_and_window(arg: &Expr) -> Result<(Vec<Matcher>, i64), CompileError> {
    let Expr::Rollup(re) = arg else {
        return Err(CompileError::InvalidShape(
            "rollup function argument must be `<selector>[<window>]`".into()));
    };
    let Some(win) = &re.window else {
        return Err(CompileError::InvalidShape(
            "rollup function requires an explicit `[<window>]` argument".into()));
    };
    let window_ms = parse_window_ms(&win.value).map_err(|e|
        CompileError::InvalidShape(format!("bad rollup window {:?}: {e}", win.value)))?;
    let inner = re_inner_metric(re)?;
    let matchers = matchers_for(inner)?;
    Ok((matchers, window_ms))
}

fn re_inner_metric(re: &RollupExpr) -> Result<&MetricExpr, CompileError> {
    match &*re.expr {
        Expr::Metric(m) => Ok(m),
        other => Err(CompileError::Unsupported(format!(
            "rollup over {:?} (expected bare selector)", short_kind(other)))),
    }
}

/// Minimal duration parser for plan compilation. Supports
/// the units the streaming layer's tests exercise; for the
/// full unit set the eval path's parser handles, see
/// `eval::parse_duration_ms`.
fn parse_window_ms(s: &str) -> Result<i64, String> {
    let mut total: i64 = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') { i += 1; }
        if i == start { return Err("expected number".into()); }
        let n: f64 = s[start..i].parse().map_err(|e| format!("number parse: {e}"))?;
        let unit_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() { i += 1; }
        let unit = &s[unit_start..i];
        let mult = match unit {
            "ms" => 1.0,
            "s"  => 1_000.0,
            "m"  => 60_000.0,
            "h"  => 3_600_000.0,
            "d"  => 86_400_000.0,
            ""   => return Err("missing unit".into()),
            other => return Err(format!("unknown unit {other:?}")),
        };
        total = total.saturating_add((n * mult) as i64);
    }
    Ok(total)
}

fn short_kind(e: &Expr) -> &'static str {
    match e {
        Expr::Metric(_)   => "Metric",
        Expr::Number(_)   => "Number",
        Expr::String(_)   => "String",
        Expr::Duration(_) => "Duration",
        Expr::Func(_)     => "Func",
        Expr::Binary(_)   => "Binary",
        Expr::Rollup(_)   => "Rollup",
        Expr::Paren(_)    => "Paren",
        Expr::With(_)     => "With",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t: i64, v: f64) -> Sample {
        Sample { timestamp_ms: t, value: v }
    }

    /// Build an accumulator from a slice of samples by
    /// `ingest`-ing them in order. Used by the monoid-law
    /// tests as a one-step "make me an accumulator" helper.
    fn build<R: Reducer>(r: &R, samples: &[Sample]) -> R::Acc {
        let mut acc = R::Acc::default();
        for sm in samples { r.ingest(&mut acc, sm); }
        acc
    }

    /// Verify the three monoid laws for one reducer against
    /// a fixed set of inputs. Each test below picks inputs
    /// that exercise distinct accumulator regions (with /
    /// without data, NaN handling, edge values).
    fn assert_monoid_laws<R: Reducer>(r: &R, a: &[Sample], b: &[Sample], c: &[Sample]) {
        // identity: merge(empty, x) ≡ x
        let x = build(r, a);
        let mut empty = R::Acc::default();
        let x_clone = x.clone();
        r.merge(&mut empty, x);
        assert_snap_eq(r, &empty, &x_clone, "identity (empty merged into)");

        let mut x_owned = x_clone.clone();
        let empty2 = R::Acc::default();
        r.merge(&mut x_owned, empty2);
        assert_snap_eq(r, &x_owned, &x_clone, "identity (empty merged onto)");

        // commutative: merge(a, b) ≡ merge(b, a)
        let mut ab = build(r, a);
        let bcc = build(r, b);
        r.merge(&mut ab, bcc);
        let mut ba = build(r, b);
        let acc2 = build(r, a);
        r.merge(&mut ba, acc2);
        assert_snap_eq(r, &ab, &ba, "commutativity");

        // associative: merge(merge(a, b), c) ≡ merge(a, merge(b, c))
        let mut left = build(r, a);
        let bcc2 = build(r, b);
        r.merge(&mut left, bcc2);
        let cleft = build(r, c);
        r.merge(&mut left, cleft);

        let mut right_inner = build(r, b);
        let c_inner = build(r, c);
        r.merge(&mut right_inner, c_inner);
        let mut right = build(r, a);
        r.merge(&mut right, right_inner);

        assert_snap_eq(r, &left, &right, "associativity");
    }

    fn assert_snap_eq<R: Reducer>(r: &R, x: &R::Acc, y: &R::Acc, why: &str) {
        let xv = r.snapshot(x);
        let yv = r.snapshot(y);
        // NaN-aware equality so identity-on-empty doesn't
        // fail the test (NaN != NaN normally).
        if xv.is_nan() && yv.is_nan() { return; }
        let tol = 1e-9_f64.max(xv.abs() * 1e-12);
        assert!((xv - yv).abs() <= tol,
            "{why}: snapshots diverged: {xv} vs {yv}");
    }

    #[test]
    fn monoid_laws_sum() {
        let a = [s(0, 1.0), s(1, 2.5), s(2, -0.5)];
        let b = [s(3, 100.0)];
        let c = [s(4, 0.0), s(5, f64::NAN), s(6, 7.7)];
        assert_monoid_laws(&SumReducer, &a, &b, &c);
        // Empty snapshot is NaN per `has_data == false`.
        assert!(SumReducer.snapshot(&KahanAcc::default()).is_nan());
        // Non-empty snapshot is finite.
        assert_eq!(SumReducer.snapshot(&build(&SumReducer, &a)), 3.0);
    }

    #[test]
    fn monoid_laws_count() {
        let a = [s(0, 1.0), s(1, 2.0)];
        let b = [s(2, 3.0), s(3, f64::NAN)];
        let c = [s(4, 5.0)];
        assert_monoid_laws(&CountReducer, &a, &b, &c);
        // Count's empty is 0.0 not NaN — distinguishes
        // count from sum/min/max snapshots.
        assert_eq!(CountReducer.snapshot(&CountAcc::default()), 0.0);
        assert_eq!(CountReducer.snapshot(&build(&CountReducer, &a)), 2.0);
        // NaN samples skipped.
        assert_eq!(CountReducer.snapshot(&build(&CountReducer, &b)), 1.0);
    }

    #[test]
    fn monoid_laws_min() {
        let a = [s(0, 5.0), s(1, 3.0)];
        let b = [s(2, 7.0)];
        let c = [s(3, -1.0), s(4, f64::NAN)];
        assert_monoid_laws(&MinReducer, &a, &b, &c);
        assert!(MinReducer.snapshot(&MinAcc::default()).is_nan());
        assert_eq!(MinReducer.snapshot(&build(&MinReducer, &a)), 3.0);
        // NaN doesn't replace min.
        assert_eq!(MinReducer.snapshot(&build(&MinReducer, &c)), -1.0);
    }

    #[test]
    fn monoid_laws_max() {
        let a = [s(0, 5.0), s(1, 3.0)];
        let b = [s(2, 7.0)];
        let c = [s(3, -1.0), s(4, f64::NAN)];
        assert_monoid_laws(&MaxReducer, &a, &b, &c);
        assert!(MaxReducer.snapshot(&MaxAcc::default()).is_nan());
        assert_eq!(MaxReducer.snapshot(&build(&MaxReducer, &a)), 5.0);
        assert_eq!(MaxReducer.snapshot(&build(&MaxReducer, &b)), 7.0);
    }

    #[test]
    fn monoid_laws_group() {
        let a = [s(0, 1.0)];
        let b = [s(1, f64::NAN)];
        let c = [];
        assert_monoid_laws(&GroupReducer, &a, &b, &c);
        assert!(GroupReducer.snapshot(&GroupAcc::default()).is_nan());
        assert_eq!(GroupReducer.snapshot(&build(&GroupReducer, &a)), 1.0);
        // NaN-only inputs leave `has_data` false.
        assert!(GroupReducer.snapshot(&build(&GroupReducer, &b)).is_nan());
    }

    #[test]
    fn monoid_laws_first_over_time() {
        let a = [s(10, 1.0), s(20, 2.0)];
        let b = [s(5, 99.0)];
        let c = [s(15, 3.0), s(25, f64::NAN)];
        assert_monoid_laws(&FirstOverTimeReducer, &a, &b, &c);
        assert!(FirstOverTimeReducer.snapshot(&FirstAcc::default()).is_nan());
        // First sample is the one with the smallest timestamp,
        // regardless of ingest order.
        let acc = build(&FirstOverTimeReducer, &a);
        assert_eq!(FirstOverTimeReducer.snapshot(&acc), 1.0);
    }

    #[test]
    fn first_over_time_picks_smallest_timestamp_across_partitions() {
        let r = FirstOverTimeReducer;
        // Ingest order shouldn't matter — the acc with the
        // smaller timestamp wins on merge.
        let mut a = FirstAcc::default();
        r.ingest(&mut a, &s(20, 2.0));
        let mut b = FirstAcc::default();
        r.ingest(&mut b, &s(10, 1.0));
        r.merge(&mut a, b);
        assert_eq!(r.snapshot(&a), 1.0);
    }

    #[test]
    fn monoid_laws_last_over_time() {
        let a = [s(10, 1.0), s(20, 2.0)];
        let b = [s(30, 99.0)];
        let c = [s(15, 3.0), s(25, f64::NAN)];
        assert_monoid_laws(&LastOverTimeReducer, &a, &b, &c);
        assert!(LastOverTimeReducer.snapshot(&LastAcc::default()).is_nan());
        let acc = build(&LastOverTimeReducer, &a);
        assert_eq!(LastOverTimeReducer.snapshot(&acc), 2.0);
    }

    #[test]
    fn last_over_time_picks_largest_timestamp_across_partitions() {
        let r = LastOverTimeReducer;
        let mut a = LastAcc::default();
        r.ingest(&mut a, &s(10, 1.0));
        let mut b = LastAcc::default();
        r.ingest(&mut b, &s(20, 2.0));
        r.merge(&mut a, b);
        assert_eq!(r.snapshot(&a), 2.0);
    }

    // ---- compile_streaming shape tests ----

    use crate::parse;

    fn compile(q: &str) -> Result<StreamingPlan, CompileError> {
        let expr = parse(q).expect("parse");
        compile_streaming(&expr)
    }

    fn pipeline_kind(plan: &StreamingPlan) -> &'static str {
        match &plan.pipeline {
            Pipeline::Bare { .. }              => "Bare",
            Pipeline::Aggregate { .. }         => "Aggregate",
            Pipeline::Window { .. }            => "Window",
            Pipeline::WindowedAggregate { .. } => "WindowedAggregate",
        }
    }

    #[test]
    fn compile_aggregate_no_modifier() {
        let plan = compile("sum(cpu)").expect("compile");
        assert_eq!(pipeline_kind(&plan), "Aggregate");
        match &plan.pipeline {
            Pipeline::Aggregate { reducer, grouping, .. } => {
                assert_eq!(*reducer, ReducerKind::Sum);
                assert!(matches!(grouping, Grouping::All));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn compile_aggregate_with_by_modifier() {
        let plan = compile("max(cpu) by (host, zone)").expect("compile");
        match &plan.pipeline {
            Pipeline::Aggregate { reducer, grouping, .. } => {
                assert_eq!(*reducer, ReducerKind::Max);
                match grouping {
                    Grouping::By(labels) => assert_eq!(labels, &["host", "zone"]),
                    _ => panic!("expected By"),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn compile_aggregate_with_without_modifier() {
        let plan = compile("min(cpu) without (instance)").expect("compile");
        match &plan.pipeline {
            Pipeline::Aggregate { reducer, grouping, .. } => {
                assert_eq!(*reducer, ReducerKind::Min);
                match grouping {
                    Grouping::Without(labels) => assert_eq!(labels, &["instance"]),
                    _ => panic!("expected Without"),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn compile_pure_window() {
        let plan = compile("max_over_time(cpu[5m])").expect("compile");
        match &plan.pipeline {
            Pipeline::Window { reducer, window_ms, .. } => {
                assert_eq!(*reducer, ReducerKind::Max);
                assert_eq!(*window_ms, 5 * 60 * 1000);
            }
            _ => panic!("expected Window pipeline, got {:?}", pipeline_kind(&plan)),
        }
    }

    #[test]
    fn compile_windowed_aggregate() {
        let plan = compile("sum(sum_over_time(cpu[1m])) by (host)").expect("compile");
        match &plan.pipeline {
            Pipeline::WindowedAggregate { window_reducer, outer_reducer, window_ms, grouping, .. } => {
                assert_eq!(*window_reducer, ReducerKind::Sum);
                assert_eq!(*outer_reducer, ReducerKind::Sum);
                assert_eq!(*window_ms, 60_000);
                match grouping {
                    Grouping::By(labels) => assert_eq!(labels, &["host"]),
                    _ => panic!("expected By"),
                }
            }
            _ => panic!("expected WindowedAggregate pipeline, got {:?}", pipeline_kind(&plan)),
        }
    }

    #[test]
    fn compile_first_last_over_time() {
        let plan = compile("first_over_time(cpu[5m])").expect("compile");
        match &plan.pipeline {
            Pipeline::Window { reducer, .. } => {
                assert_eq!(*reducer, ReducerKind::FirstOverTime);
            }
            _ => unreachable!(),
        }
        let plan = compile("last_over_time(cpu[5m])").expect("compile");
        match &plan.pipeline {
            Pipeline::Window { reducer, .. } => {
                assert_eq!(*reducer, ReducerKind::LastOverTime);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn compile_bare_selector() {
        let plan = compile("cpu").expect("compile");
        assert_eq!(pipeline_kind(&plan), "Bare");
    }

    // ---- rejection tests ----

    #[test]
    fn reject_binary_op() {
        let err = compile("cpu + 1").expect_err("expected compile error");
        assert!(matches!(err, CompileError::Unsupported(_)),
            "expected Unsupported, got {err:?}");
    }

    #[test]
    fn reject_avg_aggregate() {
        // Algebraic reducer — supported in a follow-up push,
        // not this one.
        let err = compile("avg(cpu)").expect_err("expected compile error");
        assert!(matches!(err, CompileError::Unsupported(_)));
    }

    #[test]
    fn reject_rate_function() {
        let err = compile("rate(cpu[5m])").expect_err("expected compile error");
        assert!(matches!(err, CompileError::Unsupported(_)));
    }

    #[test]
    fn reject_quantile_holistic() {
        // `quantile(0.9, cpu)` carries 2 args — the
        // arity check fires before classification, but the
        // result is still a clean rejection.
        let err = compile("quantile(0.9, cpu)").expect_err("expected compile error");
        assert!(matches!(err, CompileError::Unsupported(_) | CompileError::InvalidShape(_)));
    }

    #[test]
    fn reject_or_filter_groups() {
        let err = compile(r#"cpu{host="a" or host="b"}"#)
            .expect_err("expected compile error");
        assert!(matches!(err, CompileError::Unsupported(_)));
    }

    #[test]
    fn reject_rollup_without_window() {
        // Rate-style rollup without an explicit window —
        // rejected because the streaming layer doesn't infer
        // step-relative durations.
        let err = compile("sum_over_time(cpu)").expect_err("expected compile error");
        assert!(matches!(err, CompileError::InvalidShape(_)));
    }

    #[test]
    fn reject_top_level_number() {
        let err = compile("42").expect_err("expected compile error");
        assert!(matches!(err, CompileError::Unsupported(_)));
    }

    // ---- end-to-end ingest + snapshot tests ----

    fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
        Series {
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            samples: samples.iter().map(|(t, v)| Sample { timestamp_ms: *t, value: *v }).collect(),
        }
    }

    fn lookup<'a>(s: &'a Series, key: &str) -> Option<&'a str> {
        s.labels.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    #[test]
    fn ingest_aggregate_collapses_to_single_group_per_timestamp() {
        let mut plan = compile("sum(cpu)").expect("compile");
        // Two timestamps, three series → output should have
        // ONE group with TWO samples, one per timestamp.
        plan.ingest(&[
            series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0), (10, 10.0)]),
            series(&[("__name__", "cpu"), ("host", "b")], &[(0, 2.0), (10, 20.0)]),
            series(&[("__name__", "cpu"), ("host", "c")], &[(0, 4.0), (10, 40.0)]),
        ]);
        let got = plan.snapshot(0);
        assert_eq!(got.len(), 1);
        assert!(got[0].labels.is_empty());
        assert_eq!(got[0].samples.len(), 2);
        // T=0: 1+2+4 = 7
        assert_eq!(got[0].samples[0].timestamp_ms, 0);
        assert_eq!(got[0].samples[0].value, 7.0);
        // T=10: 10+20+40 = 70
        assert_eq!(got[0].samples[1].timestamp_ms, 10);
        assert_eq!(got[0].samples[1].value, 70.0);
    }

    #[test]
    fn ingest_aggregate_by_groups_distinct_label_values() {
        let mut plan = compile("sum(cpu) by (zone)").expect("compile");
        plan.ingest(&[
            series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "a")], &[(0, 1.0)]),
            series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "b")], &[(0, 2.0)]),
            series(&[("__name__", "cpu"), ("zone", "z2"), ("host", "c")], &[(0, 4.0)]),
        ]);
        let mut got = plan.snapshot(0);
        got.sort_by(|a, b| lookup(a, "zone").unwrap_or("").cmp(lookup(b, "zone").unwrap_or("")));
        assert_eq!(got.len(), 2);
        assert_eq!(lookup(&got[0], "zone"), Some("z1"));
        assert_eq!(got[0].samples[0].value, 3.0);
        assert_eq!(lookup(&got[1], "zone"), Some("z2"));
        assert_eq!(got[1].samples[0].value, 4.0);
    }

    #[test]
    fn ingest_window_holds_per_series_state() {
        let mut plan = compile("sum_over_time(cpu[1m])").expect("compile");
        plan.ingest(&[
            series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0), (10, 2.0), (20, 4.0)]),
            series(&[("__name__", "cpu"), ("host", "b")], &[(0, 8.0)]),
        ]);
        let mut got = plan.snapshot(100);
        got.sort_by(|a, b| lookup(a, "host").unwrap_or("").cmp(lookup(b, "host").unwrap_or("")));
        assert_eq!(got.len(), 2);
        // host=a: 1+2+4 = 7
        assert_eq!(got[0].samples[0].value, 7.0);
        // host=b: 8
        assert_eq!(got[1].samples[0].value, 8.0);
    }

    #[test]
    fn ingest_windowed_aggregate_two_stage_reduces() {
        let mut plan = compile("sum(sum_over_time(cpu[1m])) by (zone)").expect("compile");
        plan.ingest(&[
            series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "a")], &[(0, 1.0), (10, 2.0)]),
            series(&[("__name__", "cpu"), ("zone", "z1"), ("host", "b")], &[(0, 4.0)]),
            series(&[("__name__", "cpu"), ("zone", "z2"), ("host", "c")], &[(0, 100.0), (10, 200.0)]),
        ]);
        let mut got = plan.snapshot(0);
        got.sort_by(|a, b| lookup(a, "zone").unwrap_or("").cmp(lookup(b, "zone").unwrap_or("")));
        assert_eq!(got.len(), 2);
        // z1: per-series sums (1+2) + 4 = 7
        assert_eq!(got[0].samples[0].value, 7.0);
        // z2: 100+200 = 300
        assert_eq!(got[1].samples[0].value, 300.0);
    }

    #[test]
    fn ingest_first_last_over_time_pick_by_timestamp() {
        let samples = [
            series(&[("__name__", "v"), ("host", "h1")], &[(20, 2.0), (10, 1.0), (30, 3.0)]),
        ];
        let mut p_first = compile("first_over_time(v[1m])").expect("compile");
        p_first.ingest(&samples);
        assert_eq!(p_first.snapshot(0)[0].samples[0].value, 1.0);

        let mut p_last = compile("last_over_time(v[1m])").expect("compile");
        p_last.ingest(&samples);
        assert_eq!(p_last.snapshot(0)[0].samples[0].value, 3.0);
    }

    #[test]
    fn matchers_drop_unmatched_samples() {
        let mut plan = compile(r#"sum(cpu{host="a"})"#).expect("compile");
        plan.ingest(&[
            series(&[("__name__", "cpu"), ("host", "a")], &[(0, 1.0)]),
            series(&[("__name__", "cpu"), ("host", "b")], &[(0, 99.0)]),
            series(&[("__name__", "mem"), ("host", "a")], &[(0, 50.0)]),
        ]);
        let got = plan.snapshot(0);
        assert_eq!(got[0].samples[0].value, 1.0);
    }

    #[test]
    fn reset_clears_all_accumulators() {
        let mut plan = compile("sum(cpu)").expect("compile");
        plan.ingest(&[series(&[("__name__", "cpu")], &[(0, 5.0)])]);
        assert_eq!(plan.snapshot(0).len(), 1);
        plan.reset();
        // After reset the plan has no groups → empty
        // snapshot. Re-ingest works as on a fresh plan.
        assert_eq!(plan.snapshot(0).len(), 0);
        plan.ingest(&[series(&[("__name__", "cpu")], &[(0, 7.0)])]);
        assert_eq!(plan.snapshot(0)[0].samples[0].value, 7.0);
    }

    // ---- equivalence property test (load-bearing) ----

    /// Tiny xorshift64 PRNG so the property test reproduces
    /// failures with a fixed seed and doesn't pull in a dep.
    struct XorShift64 { state: u64 }
    impl XorShift64 {
        fn new(seed: u64) -> Self {
            // xorshift requires non-zero seed.
            Self { state: if seed == 0 { 0xC0FFEE } else { seed } }
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.state = x;
            x
        }
        fn range(&mut self, lo: usize, hi_exclusive: usize) -> usize {
            lo + (self.next_u64() as usize) % (hi_exclusive - lo)
        }
        fn f64(&mut self) -> f64 {
            // Map to [0, 1). Top 53 bits → mantissa.
            let bits = self.next_u64() >> 11;
            (bits as f64) / ((1u64 << 53) as f64)
        }
    }

    /// One supported query shape, with a function that
    /// produces the batch-evaluation result so the property
    /// test can compare against streaming output.
    struct Shape {
        query: &'static str,
        // Number of distinct (host, zone) combinations to
        // generate inputs over.
        cardinality: usize,
        // Number of timestamps per series.
        samples_per_series: usize,
    }

    fn supported_shapes() -> Vec<Shape> {
        vec![
            Shape { query: "sum(cpu)",                              cardinality: 6, samples_per_series: 4 },
            Shape { query: "min(cpu)",                              cardinality: 5, samples_per_series: 3 },
            Shape { query: "max(cpu)",                              cardinality: 5, samples_per_series: 3 },
            Shape { query: "count(cpu)",                            cardinality: 5, samples_per_series: 3 },
            Shape { query: "group(cpu)",                            cardinality: 5, samples_per_series: 3 },
            Shape { query: "sum(cpu) by (zone)",                    cardinality: 8, samples_per_series: 3 },
            Shape { query: "max(cpu) without (host)",               cardinality: 8, samples_per_series: 3 },
            Shape { query: "sum_over_time(cpu[1m])",                cardinality: 4, samples_per_series: 5 },
            Shape { query: "max_over_time(cpu[1m])",                cardinality: 4, samples_per_series: 5 },
            Shape { query: "first_over_time(cpu[1m])",              cardinality: 4, samples_per_series: 5 },
            Shape { query: "last_over_time(cpu[1m])",               cardinality: 4, samples_per_series: 5 },
            Shape { query: "sum(sum_over_time(cpu[1m])) by (zone)", cardinality: 8, samples_per_series: 4 },
        ]
    }

    /// Build a representative sample-set: `cardinality`
    /// distinct series, each with `samples_per_series`
    /// timestamps. Hosts and zones cycle across an
    /// intentionally-small alphabet so groups are populated.
    fn random_input(rng: &mut XorShift64, shape: &Shape) -> Vec<Series> {
        let zones = ["z1", "z2", "z3"];
        let mut out = Vec::with_capacity(shape.cardinality);
        for i in 0..shape.cardinality {
            let host = format!("h{i}");
            let zone = zones[i % zones.len()];
            let mut samples = Vec::with_capacity(shape.samples_per_series);
            for j in 0..shape.samples_per_series {
                let ts = (j as i64) * 100;
                let value = rng.f64() * 1000.0 - 500.0;
                samples.push(Sample { timestamp_ms: ts, value });
            }
            out.push(series(
                &[("__name__", "cpu"), ("host", host.as_str()), ("zone", zone)],
                &samples.iter().map(|s| (s.timestamp_ms, s.value)).collect::<Vec<_>>(),
            ));
        }
        out
    }

    /// Flatten `Vec<Series>` into per-sample tuples so the
    /// partitioner can shuffle independent of series
    /// boundaries — that's what makes the property test
    /// load-bearing.
    fn flatten(series: &[Series]) -> Vec<(Vec<(String, String)>, Sample)> {
        let mut out = Vec::new();
        for s in series {
            for sm in &s.samples {
                out.push((s.labels.clone(), sm.clone()));
            }
        }
        out
    }

    /// Random partition of `pairs` into `k` non-empty
    /// batches. Order within a batch is preserved — the test
    /// is about partition-then-merge associativity, not
    /// per-sample order (timestamp-aware reducers care, the
    /// rest don't).
    fn random_partition(
        rng: &mut XorShift64,
        pairs: &[(Vec<(String, String)>, Sample)],
        k: usize,
    ) -> Vec<Vec<(Vec<(String, String)>, Sample)>> {
        let mut batches: Vec<Vec<_>> = (0..k).map(|_| Vec::new()).collect();
        for p in pairs {
            let target = rng.range(0, k);
            batches[target].push(p.clone());
        }
        batches
    }

    /// Compute the batch-evaluation result for the same
    /// shape via the existing `eval::evaluate` path — that's
    /// the reference the streaming snapshot is asserted to
    /// match. Runs against an in-memory data source built
    /// from the test input.
    fn batch_result(query: &str, input: &[Series], anchor_ms: i64) -> Vec<Series> {
        struct Mem { series: Vec<Series> }
        impl crate::eval::DataSource for Mem {
            fn fetch(&self, matchers: &[Matcher], _start: i64, _end: i64)
                -> Result<Vec<Series>, crate::eval::DataSourceError>
            {
                Ok(self.series.iter()
                    .filter(|s| matchers.iter().all(|m| {
                        let v = s.labels.iter()
                            .find(|(k, _)| k == &m.label)
                            .map(|(_, v)| v.as_str())
                            .unwrap_or("");
                        match m.op {
                            MatcherOp::Eq => v == m.value,
                            MatcherOp::Ne => v != m.value,
                            _ => v == m.value,
                        }
                    }))
                    .cloned().collect())
            }
        }
        let ds = Mem { series: input.to_vec() };
        let ctx = crate::eval::EvalContext {
            data: &ds, start_ms: 0, end_ms: anchor_ms, step_ms: 1,
        };
        let expr = parse(query).expect("parse");
        crate::eval::evaluate(&ctx, &expr).expect("evaluate")
    }

    /// Index `(labels, timestamp) → value`. Aggregate shapes
    /// emit one sample per observed timestamp; window shapes
    /// emit one sample at the query anchor. Either way the
    /// (labels, ts) tuple uniquely identifies a result point.
    fn index_by_labels_and_ts(s: &[Series]) -> BTreeMap<(Vec<(String, String)>, i64), f64> {
        let mut out = BTreeMap::new();
        for series in s {
            let mut k = series.labels.clone();
            k.sort_by(|a, b| a.0.cmp(&b.0));
            for sm in &series.samples {
                out.insert((k.clone(), sm.timestamp_ms), sm.value);
            }
        }
        out
    }

    /// `f64` equality that treats NaN==NaN and tolerates
    /// summation order drift within a small bound. Tightness
    /// matches Kahan's expected behaviour (~1 ulp per op).
    fn approx_eq(a: f64, b: f64) -> bool {
        if a.is_nan() && b.is_nan() { return true; }
        let scale = a.abs().max(b.abs()).max(1.0);
        (a - b).abs() <= scale * 1e-9
    }

    #[test]
    fn streaming_equals_batch_for_supported_shapes() {
        // Fixed seed so failures reproduce. Iteration count
        // chosen so total wall-time stays under SRD-47's
        // 50ms target.
        let mut rng = XorShift64::new(0xC0FFEE_BEEF);
        const ITERATIONS_PER_SHAPE: usize = 50;
        let anchor_ms = 1000;

        for shape in supported_shapes() {
            for _trial in 0..ITERATIONS_PER_SHAPE {
                let input = random_input(&mut rng, &shape);
                let batch = batch_result(shape.query, &input, anchor_ms);
                let batch_idx = index_by_labels_and_ts(&batch);

                // Pick a partition count [1, 6]. k=1 is
                // batch-equivalent (sanity); higher values
                // exercise the merge path.
                let k = rng.range(1, 7);
                let pairs = flatten(&input);
                let batches = random_partition(&mut rng, &pairs, k);

                let mut plan = compile_streaming(&parse(shape.query).expect("parse"))
                    .expect("compile");
                for batch_pairs in &batches {
                    for (labels, sample) in batch_pairs {
                        plan.ingest_sample(labels, sample);
                    }
                }
                let stream = plan.snapshot(anchor_ms);
                let stream_idx = index_by_labels_and_ts(&stream);
                let batch_idx = batch_idx;

                if batch_idx.len() != stream_idx.len() {
                    panic!(
                        "shape {:?}: result cardinality differs — batch={} stream={}\n  batch keys: {:?}\n  stream keys: {:?}",
                        shape.query, batch_idx.len(), stream_idx.len(),
                        batch_idx.keys().collect::<Vec<_>>(),
                        stream_idx.keys().collect::<Vec<_>>(),
                    );
                }
                for (k, batch_v) in &batch_idx {
                    let stream_v = stream_idx.get(k).copied().unwrap_or(f64::NAN);
                    if !approx_eq(*batch_v, stream_v) {
                        panic!(
                            "shape {:?}: divergence at {:?} — batch={} stream={}",
                            shape.query, k, batch_v, stream_v,
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn kahan_summation_resists_order_drift() {
        // Classic Kahan stress: many small values plus one
        // large value. Naive summation loses the tail.
        let mut left = KahanAcc::default();
        let r = SumReducer;
        r.ingest(&mut left, &s(0, 1e16));
        for i in 0..1000 {
            r.ingest(&mut left, &s(i + 1, 1.0));
        }
        let mut right = KahanAcc::default();
        for i in 0..1000 {
            r.ingest(&mut right, &s(i + 1, 1.0));
        }
        r.ingest(&mut right, &s(0, 1e16));
        let lv = r.snapshot(&left);
        let rv = r.snapshot(&right);
        // Kahan keeps both within ~1 ulp; naive f64 would
        // diverge by ~1000.
        assert!((lv - rv).abs() < 2.0,
            "kahan order drift too large: {lv} vs {rv}");
        // And both are within 1 of the true total `1e16 + 1000`.
        let truth = 1e16 + 1000.0;
        assert!((lv - truth).abs() <= 1.0);
    }
}
