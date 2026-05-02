// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! OpenMetrics-aligned snapshot data model (SRD-42 §"Snapshot data
//! model"). Mirrors the OpenMetrics specification 1:1 so external
//! consumers (Prometheus scrape, OTel translation, third-party
//! dashboards) get a near-trivial projection.
//!
//! ## Container hierarchy (spec terms verbatim)
//!
//! | Layer | OpenMetrics §  | Type |
//! |---|---|---|
//! | top-level | §4.1 `MetricSet`     | [`MetricSet`] |
//! | family    | §4.4 `MetricFamily`  | [`MetricFamily`] |
//! | series    | §4.5 `Metric`        | [`Metric`] |
//! | point     | §4.6 `MetricPoint`   | [`MetricPoint`] |
//!
//! A time series is identified by `(MetricFamily.name, LabelSet)`
//! per spec §4.5.1 — the same identity used by cascade-time combine
//! (matching identity → matching reservoir / counter / gauge → combine
//! permitted).
//!
//! ## Histograms
//!
//! Internally we keep the HDR reservoir as the source of truth on
//! [`HistogramValue`] — that's what combines correctly across
//! cascade folds and ephemeral merges. The OpenMetrics-shaped
//! cumulative `Bucket` list is **derived on demand at exposition
//! time** against the consumer-requested bucket layout. `sum` /
//! `count` are also derivable but maintained alongside the reservoir
//! for O(1) access.
//!
//! ## Naming convention
//!
//! Suffix rules from spec §4.4.1 / §5.x (`_total`, `_count`, `_sum`,
//! `_bucket`, `_created`, `_info`) are **exposition-time concerns,
//! not stored**. A counter is named `cycles` in memory; the
//! exposition layer appends `_total` per spec.
//!
//! ## Initial coverage
//!
//! `Counter`, `Gauge`, `Histogram` are implemented. `Summary`,
//! `Info`, `StateSet`, `Unknown`, `GaugeHistogram` are listed in
//! [`MetricType`] but their value variants are added when a real
//! consumer needs them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram as HdrHistogram;

use crate::labels::Labels;

// =========================================================================
// MetricSet — top-level snapshot (OpenMetrics §4.1)
// =========================================================================

/// Top-level snapshot container per OpenMetrics §4.1. Holds zero or
/// more [`MetricFamily`] entries, names unique within the set.
///
/// Snapshots are immutable once published. Producers build a new
/// `MetricSet` per cadence-window close; consumers read the
/// `Arc<MetricSet>` published into the cadence reporter's store.
///
/// `MetricSet` carries two pieces of nb-rs internal metadata that
/// don't appear in the OpenMetrics spec but are needed by the
/// scheduler's coalesce path:
///
/// - `captured_at` — wall-clock instant the snapshot was sealed.
/// - `interval` — duration the snapshot represents (cadence window
///   length for cadence-window snapshots; `Duration::ZERO` for
///   instantaneous `now` reads).
///
/// These are intentionally not in any `MetricPoint`; consumers that
/// project to OpenMetrics on-wire format should read the per-point
/// timestamps and `_created` fields instead.
#[derive(Clone, Debug)]
pub struct MetricSet {
    captured_at: Instant,
    interval: Duration,
    families: Vec<MetricFamily>,
}

impl Default for MetricSet {
    fn default() -> Self {
        Self {
            captured_at: Instant::now(),
            interval: Duration::ZERO,
            families: Vec::new(),
        }
    }
}

impl MetricSet {
    /// Construct an empty snapshot stamped with the current instant
    /// and the given window interval.
    pub fn new(interval: Duration) -> Self {
        Self {
            captured_at: Instant::now(),
            interval,
            families: Vec::new(),
        }
    }

    /// Construct an empty snapshot stamped with an explicit
    /// `captured_at` (e.g., for tests reproducing a known instant).
    pub fn at(captured_at: Instant, interval: Duration) -> Self {
        Self { captured_at, interval, families: Vec::new() }
    }

    pub fn captured_at(&self) -> Instant { self.captured_at }
    pub fn interval(&self) -> Duration { self.interval }

    /// Set the represented interval. Used when a coalesce path
    /// promotes a snapshot to a coarser cadence's window.
    pub fn set_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    /// Iterator over all families in this set.
    pub fn families(&self) -> impl Iterator<Item = &MetricFamily> {
        self.families.iter()
    }

    /// Lookup a family by name. `None` if no family has that name.
    pub fn family(&self, name: &str) -> Option<&MetricFamily> {
        self.families.iter().find(|f| f.name() == name)
    }

    /// Number of families in this set.
    pub fn len(&self) -> usize { self.families.len() }
    pub fn is_empty(&self) -> bool { self.families.is_empty() }

    /// Insert a family. Panics if a family of the same name already
    /// exists — spec §4.1 requires unique names within a `MetricSet`.
    pub fn insert(&mut self, family: MetricFamily) {
        assert!(
            !self.families.iter().any(|f| f.name() == family.name()),
            "MetricSet already contains family '{}' — names must be unique (OpenMetrics §4.1)",
            family.name(),
        );
        self.families.push(family);
    }

    /// Coalesce multiple snapshots into one. Used by the scheduler
    /// to fold smaller-cadence snapshots into a larger-cadence
    /// window per SRD-42 §"Streaming coalesce semantics".
    ///
    /// Combine rules per SRD-42 §"Combine semantics — algebraic
    /// uniformity":
    ///
    /// - Counter `total` sums; `created` keeps the earliest;
    ///   exemplar most-recent-wins.
    /// - Gauge values weighted-average by `interval`.
    /// - Histogram reservoirs add (`HdrHistogram::add`); `count`/`sum`
    ///   re-derive from the merged reservoir; bucket exemplars
    ///   most-recent-wins per index.
    /// - Identity: `(family.name, LabelSet)` — matching identity
    ///   combines, others append. Type mismatch on matching identity
    ///   is a hard error (panic).
    ///
    /// `captured_at` of the result is the latest contributing
    /// snapshot's `captured_at`; `interval` is the sum of contributing
    /// intervals.
    pub fn coalesce(snapshots: &[MetricSet]) -> MetricSet {
        if snapshots.is_empty() {
            return MetricSet::default();
        }
        if snapshots.len() == 1 {
            return snapshots[0].clone();
        }

        let captured_at = snapshots.iter().map(|s| s.captured_at).max().unwrap();
        let interval: Duration = snapshots.iter().map(|s| s.interval).sum();

        let mut out = MetricSet { captured_at, interval, families: Vec::new() };

        // Family identity is `name`. For each unique family name
        // across inputs, fold its metrics in identity order.
        let mut seen_family: Vec<String> = Vec::new();
        for s in snapshots {
            for f in &s.families {
                if !seen_family.contains(&f.name) {
                    seen_family.push(f.name.clone());
                }
            }
        }

        for fname in seen_family {
            let mut acc: Option<MetricFamily> = None;
            // Total interval used as denominator for weighted gauges.
            let total_seconds: f64 = snapshots.iter()
                .filter(|s| s.families.iter().any(|f| f.name == fname))
                .map(|s| s.interval.as_secs_f64())
                .sum();
            // Per-LabelSet weighted gauge accumulator.
            let mut gauge_acc: Vec<(Labels, f64, f64)> = Vec::new();

            for s in snapshots {
                let Some(src_family) = s.families.iter().find(|f| f.name == fname) else { continue };
                if acc.is_none() {
                    let mut seed = MetricFamily {
                        name: src_family.name.clone(),
                        r#type: src_family.r#type,
                        unit: src_family.unit.clone(),
                        help: src_family.help.clone(),
                        metrics: Vec::new(),
                    };
                    if seed.r#type != MetricType::Gauge {
                        seed.metrics = src_family.metrics.clone();
                    }
                    acc = Some(seed);
                    if src_family.r#type == MetricType::Gauge {
                        // Seed gauge_acc from this snapshot's gauge points.
                        for m in &src_family.metrics {
                            if let Some(point) = m.points.first() {
                                if let MetricValue::Gauge(g) = &point.value {
                                    let weight = s.interval.as_secs_f64();
                                    gauge_acc.push((m.labels.clone(), g.value * weight, weight));
                                }
                            }
                        }
                    }
                    continue;
                }
                let dst = acc.as_mut().unwrap();
                if dst.r#type == MetricType::Gauge {
                    for m in &src_family.metrics {
                        if let Some(point) = m.points.first() {
                            if let MetricValue::Gauge(g) = &point.value {
                                let weight = s.interval.as_secs_f64();
                                if let Some(entry) = gauge_acc.iter_mut().find(|(l, _, _)| l == &m.labels) {
                                    entry.1 += g.value * weight;
                                    entry.2 += weight;
                                } else {
                                    gauge_acc.push((m.labels.clone(), g.value * weight, weight));
                                }
                            }
                        }
                    }
                } else {
                    for m in &src_family.metrics {
                        let dst_metric = dst.metrics.iter_mut().find(|d| d.labels == m.labels);
                        match dst_metric {
                            Some(dm) => {
                                let (Some(dp), Some(sp)) = (dm.points.first_mut(), m.points.first()) else {
                                    continue;
                                };
                                combine_into(dp, sp).expect("matching identity must combine");
                            }
                            None => {
                                dst.metrics.push(m.clone());
                            }
                        }
                    }
                }
            }

            if let Some(mut family) = acc {
                if family.r#type == MetricType::Gauge {
                    for (labels, weighted_sum, weight) in gauge_acc {
                        // Weighted-average when at least one snapshot
                        // contributed positive interval. If every input
                        // snapshot had `Duration::ZERO` (e.g. a point-
                        // in-time lifecycle flush like a validation
                        // summary frame), fall back to last-write-wins
                        // using the newest non-zero reading — otherwise
                        // the gauge would silently collapse to 0 and
                        // we'd lose what the metric actually reported.
                        let value = if weight > 0.0 {
                            weighted_sum / weight
                        } else {
                            snapshots.iter().rev()
                                .filter_map(|s| {
                                    s.families.iter()
                                        .find(|f| f.name == family.name)?
                                        .metrics.iter()
                                        .find(|m| m.labels == labels)?
                                        .points.first()
                                        .and_then(|p| match &p.value {
                                            MetricValue::Gauge(g) => Some(g.value),
                                            _ => None,
                                        })
                                })
                                .next()
                                .unwrap_or(0.0)
                        };
                        let last_ts = snapshots.iter()
                            .rev()
                            .filter_map(|s| {
                                s.families.iter()
                                    .find(|f| f.name == family.name)?
                                    .metrics.iter()
                                    .find(|m| m.labels == labels)?
                                    .points.first()
                                    .and_then(|p| p.timestamp)
                            })
                            .next()
                            .unwrap_or(captured_at);
                        family.insert(Metric::single(
                            labels,
                            MetricPoint::new(MetricValue::Gauge(GaugeValue::new(value)), last_ts),
                        ));
                    }
                    let _ = total_seconds; // silence unused
                }
                out.families.push(family);
            }
        }

        out
    }
}

// =========================================================================
// MetricFamily (OpenMetrics §4.4)
// =========================================================================

/// One metric family — a set of [`Metric`] series sharing a name,
/// type, optional unit, and optional help text. OpenMetrics §4.4.
#[derive(Clone, Debug)]
pub struct MetricFamily {
    name: String,
    r#type: MetricType,
    unit: Option<String>,
    help: Option<String>,
    metrics: Vec<Metric>,
}

impl MetricFamily {
    /// Construct an empty family.
    pub fn new(name: impl Into<String>, r#type: MetricType) -> Self {
        Self {
            name: name.into(),
            r#type,
            unit: None,
            help: None,
            metrics: Vec::new(),
        }
    }

    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    pub fn name(&self) -> &str { &self.name }
    pub fn r#type(&self) -> MetricType { self.r#type }
    pub fn unit(&self) -> Option<&str> { self.unit.as_deref() }
    pub fn help(&self) -> Option<&str> { self.help.as_deref() }

    /// Iterator over the family's series.
    pub fn metrics(&self) -> impl Iterator<Item = &Metric> {
        self.metrics.iter()
    }

    pub fn len(&self) -> usize { self.metrics.len() }
    pub fn is_empty(&self) -> bool { self.metrics.is_empty() }

    /// Look up the series with the given LabelSet, if any. Identity
    /// per spec §4.5.1: `(family.name, label_set)`.
    pub fn metric_with_labels(&self, labels: &Labels) -> Option<&Metric> {
        self.metrics.iter().find(|m| m.labels() == labels)
    }

    /// Insert a series. Panics if a series with the same LabelSet
    /// already exists — spec §4.5 requires unique LabelSets within
    /// a family.
    pub fn insert(&mut self, metric: Metric) {
        assert!(
            !self.metrics.iter().any(|m| m.labels() == metric.labels()),
            "MetricFamily '{}' already contains a Metric with labels {:?} — LabelSets must be unique (OpenMetrics §4.5)",
            self.name, metric.labels(),
        );
        self.metrics.push(metric);
    }
}

/// OpenMetrics metric type per spec §4.4. Stored on every
/// [`MetricFamily`] and projected verbatim to exposition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    GaugeHistogram,
    Summary,
    Info,
    StateSet,
    Unknown,
}

impl MetricType {
    /// The exposition-format token for this type (spec §4.4).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Counter        => "counter",
            Self::Gauge          => "gauge",
            Self::Histogram      => "histogram",
            Self::GaugeHistogram => "gaugehistogram",
            Self::Summary        => "summary",
            Self::Info           => "info",
            Self::StateSet       => "stateset",
            Self::Unknown        => "unknown",
        }
    }
}

// =========================================================================
// Metric (OpenMetrics §4.5)
// =========================================================================

/// One labeled time series within a [`MetricFamily`]. Identity is
/// `(family.name, labels)` per spec §4.5.1.
///
/// Carries an ordered list of [`MetricPoint`]s — typically one in a
/// snapshot, but the spec permits multiple when several observations
/// belong to the same series.
#[derive(Clone, Debug)]
pub struct Metric {
    labels: Labels,
    points: Vec<MetricPoint>,
}

impl Metric {
    pub fn new(labels: Labels, points: Vec<MetricPoint>) -> Self {
        Self { labels, points }
    }

    /// Convenience for the common single-point case.
    pub fn single(labels: Labels, point: MetricPoint) -> Self {
        Self { labels, points: vec![point] }
    }

    pub fn labels(&self) -> &Labels { &self.labels }

    /// Iterator over the series' points.
    pub fn points(&self) -> impl Iterator<Item = &MetricPoint> {
        self.points.iter()
    }

    /// First point — convenience for the typical single-point case.
    /// Returns `None` if the series is empty (which violates the
    /// spec but is permitted at construction time so consumers
    /// don't have to handle Result).
    pub fn point(&self) -> Option<&MetricPoint> { self.points.first() }
}

// =========================================================================
// MetricPoint (OpenMetrics §4.6)
// =========================================================================

/// One observation for a [`Metric`], plus an optional timestamp.
/// OpenMetrics §4.6.
///
/// `timestamp` is **always populated** in nb-rs snapshots (the
/// cadence-window-close instant for cadence-window points, the live
/// read instant for `now` points, the merge instant for ephemeral
/// `recent_window` / `session_lifetime` points) — even though the
/// spec marks it optional.
#[derive(Clone, Debug)]
pub struct MetricPoint {
    value: MetricValue,
    timestamp: Option<Instant>,
}

impl MetricPoint {
    pub fn new(value: MetricValue, timestamp: Instant) -> Self {
        Self { value, timestamp: Some(timestamp) }
    }

    /// Construct without a timestamp — for cases (typically tests)
    /// where the timestamp is unknown or irrelevant. Production
    /// snapshots always set one.
    pub fn untimed(value: MetricValue) -> Self {
        Self { value, timestamp: None }
    }

    pub fn value(&self) -> &MetricValue { &self.value }
    pub fn timestamp(&self) -> Option<Instant> { self.timestamp }
}

/// The typed value carried by a [`MetricPoint`]. Variants mirror
/// OpenMetrics §5.x — initial implementation covers the three
/// commonly-used types.
#[derive(Clone, Debug)]
pub enum MetricValue {
    /// OpenMetrics §5.1.1.
    Counter(CounterValue),
    /// OpenMetrics §5.2.1.
    Gauge(GaugeValue),
    /// OpenMetrics §5.3.1.
    Histogram(HistogramValue),
}

// ---- CounterValue (§5.1.1) ---------------------------------------------

/// OpenMetrics §5.1.1 counter point. Sample name carries the
/// spec-required `_total` suffix on exposition (not stored here).
#[derive(Clone, Debug)]
pub struct CounterValue {
    pub total: u64,
    /// Series start time per spec §5.1; lets external consumers
    /// detect counter resets. Optional in spec.
    pub created: Option<Instant>,
    /// Optional exemplar per spec §4.6.1. At most one per
    /// `CounterValue`.
    pub exemplar: Option<Exemplar>,
}

impl CounterValue {
    pub fn new(total: u64) -> Self {
        Self { total, created: None, exemplar: None }
    }

    pub fn with_created(mut self, t: Instant) -> Self {
        self.created = Some(t);
        self
    }

    pub fn with_exemplar(mut self, e: Exemplar) -> Self {
        self.exemplar = Some(e);
        self
    }
}

// ---- GaugeValue (§5.2.1) -----------------------------------------------

/// OpenMetrics §5.2.1 gauge point.
#[derive(Clone, Debug)]
pub struct GaugeValue {
    pub value: f64,
}

impl GaugeValue {
    pub fn new(value: f64) -> Self { Self { value } }
}

// ---- HistogramValue (§5.3.1) -------------------------------------------

/// OpenMetrics §5.3.1 histogram point. Internally carries the HDR
/// reservoir as the source of truth — OpenMetrics-shaped cumulative
/// buckets are derived on demand at exposition time.
///
/// `sum` / `count` are derivable from the reservoir but cached for
/// O(1) access. `created` is the series start time (component start)
/// per spec.
///
/// Per-bucket exemplars are NOT stored on the reservoir — they live
/// in [`HistogramValue::bucket_exemplars`] keyed by bucket index of
/// the consumer's eventual bucket layout. (Combine semantics:
/// most-recent-wins by `MetricPoint.timestamp`, per SRD-42
/// §"Exemplars → Combine semantics".)
#[derive(Clone, Debug)]
pub struct HistogramValue {
    /// HDR reservoir — the lossless source of truth for combining
    /// across cascade folds and ephemeral merges.
    pub reservoir: Arc<HdrHistogram<u64>>,
    /// Cached observation count. Equal to `reservoir.len()`.
    pub count: u64,
    /// Cached observation sum (nanoseconds for latency timers).
    pub sum: f64,
    /// Series start time per spec §5.3; optional.
    pub created: Option<Instant>,
    /// Sampled exemplars, one per bucket of an eventual exposition
    /// layout. Sparse: an empty slot means "no exemplar for that
    /// bucket". See SRD-42 §"Exemplars".
    pub bucket_exemplars: Vec<Option<Exemplar>>,
}

impl HistogramValue {
    pub fn from_hdr(reservoir: HdrHistogram<u64>) -> Self {
        let count = reservoir.len();
        let sum = hdr_sum(&reservoir);
        Self {
            reservoir: Arc::new(reservoir),
            count,
            sum,
            created: None,
            bucket_exemplars: Vec::new(),
        }
    }

    pub fn with_created(mut self, t: Instant) -> Self {
        self.created = Some(t);
        self
    }

    pub fn with_bucket_exemplars(mut self, exemplars: Vec<Option<Exemplar>>) -> Self {
        self.bucket_exemplars = exemplars;
        self
    }

    /// Project to OpenMetrics-shaped cumulative buckets at the given
    /// upper bounds. Per spec §5.3, the final bucket MUST have
    /// `upper_bound = +Inf`; this helper appends it automatically
    /// if not present.
    ///
    /// `bounds` should be sorted ascending. Returns `(upper_bound,
    /// cumulative_count)` pairs.
    pub fn project_buckets(&self, bounds: &[u64]) -> Vec<Bucket> {
        let mut out = Vec::with_capacity(bounds.len() + 1);
        for &le in bounds {
            let cumulative = self.reservoir.count_between(0, le);
            out.push(Bucket {
                upper_bound: BucketBound::Finite(le),
                cumulative_count: cumulative,
                exemplar: None,
            });
        }
        out.push(Bucket {
            upper_bound: BucketBound::PositiveInfinity,
            cumulative_count: self.count,
            exemplar: None,
        });
        out
    }
}

/// One cumulative bucket projected from a [`HistogramValue`] for
/// exposition. Per spec §5.3, the final bucket MUST have
/// `upper_bound = +Inf`.
#[derive(Clone, Debug)]
pub struct Bucket {
    pub upper_bound: BucketBound,
    pub cumulative_count: u64,
    pub exemplar: Option<Exemplar>,
}

/// Bucket upper bound — `+Inf` is the spec-required final bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketBound {
    Finite(u64),
    PositiveInfinity,
}

// =========================================================================
// Exemplar (OpenMetrics §4.6.1, §4.7)
// =========================================================================

/// OpenMetrics §4.6.1 exemplar: a labeled link from a metric
/// observation to an external context (typically a trace/span ID,
/// workload cycle number, or sample identifier).
///
/// Per spec §4.7 the serialized LabelSet MUST be ≤ 128 UTF-8
/// characters. Validation lives at exposition (a stored exemplar
/// that exceeds the limit is dropped from the wire; the recording
/// path is allowed to be permissive).
#[derive(Clone, Debug)]
pub struct Exemplar {
    pub labels: Labels,
    pub value: f64,
    pub timestamp: Option<Instant>,
}

impl Exemplar {
    pub fn new(labels: Labels, value: f64) -> Self {
        Self { labels, value, timestamp: None }
    }

    pub fn with_timestamp(mut self, t: Instant) -> Self {
        self.timestamp = Some(t);
        self
    }
}

// =========================================================================
// Combine — algebraic uniformity (SRD-42 §"Combine semantics")
// =========================================================================

/// In-place combine of `other` into `self`. Both must have the same
/// identity `(family.name, labels)` and the same value variant —
/// otherwise this is a hard error (panic) per the SRD's "matching
/// identity → matching combine" rule.
///
/// Combine rules per SRD-42 §"Combine semantics":
/// - Counter `total` sums; `created` keeps the earliest;
///   exemplar most-recent-wins by `MetricPoint.timestamp`.
/// - Gauge values weighted-average — but values alone don't carry
///   a weight, so `combine_into` here just keeps the most recent
///   (newer timestamp wins). Use [`combine_gauge_weighted`] for the
///   interval-weighted form.
/// - Histogram reservoirs add via `HdrHistogram::add`; sum/count
///   re-derive; bucket exemplars most-recent-wins per index.
pub fn combine_into(
    dst: &mut MetricPoint,
    src: &MetricPoint,
) -> Result<(), CombineError> {
    match (&mut dst.value, &src.value) {
        (MetricValue::Counter(a), MetricValue::Counter(b)) => {
            a.total = a.total.saturating_add(b.total);
            a.created = match (a.created, b.created) {
                (Some(x), Some(y)) => Some(x.min(y)),
                (Some(x), None) | (None, Some(x)) => Some(x),
                (None, None) => None,
            };
            a.exemplar = pick_more_recent_exemplar(
                a.exemplar.take(), b.exemplar.clone(),
                dst.timestamp, src.timestamp,
            );
        }
        (MetricValue::Gauge(a), MetricValue::Gauge(b)) => {
            // Most-recent-wins by timestamp; same-or-missing → src.
            if dst.timestamp.is_none()
                || src.timestamp.map(|s| Some(s) >= dst.timestamp).unwrap_or(false)
            {
                a.value = b.value;
            }
        }
        (MetricValue::Histogram(a), MetricValue::Histogram(b)) => {
            let merged = combine_hdr(&a.reservoir, &b.reservoir)?;
            a.count = merged.len();
            a.sum = hdr_sum(&merged);
            a.reservoir = Arc::new(merged);
            a.created = match (a.created, b.created) {
                (Some(x), Some(y)) => Some(x.min(y)),
                (Some(x), None) | (None, Some(x)) => Some(x),
                (None, None) => None,
            };
            combine_bucket_exemplars(
                &mut a.bucket_exemplars, &b.bucket_exemplars,
                dst.timestamp, src.timestamp,
            );
        }
        _ => return Err(CombineError::TypeMismatch),
    }
    if let Some(src_ts) = src.timestamp {
        dst.timestamp = Some(match dst.timestamp {
            Some(d) if d >= src_ts => d,
            _ => src_ts,
        });
    }
    Ok(())
}

/// Combine two `HistogramValue` reservoirs into a new owned HDR
/// histogram. Used both by `combine_into` and by ephemeral
/// `recent_window` / `session_lifetime` queries that fold many
/// reservoirs without mutating any of them.
pub fn combine_hdr(
    a: &HdrHistogram<u64>,
    b: &HdrHistogram<u64>,
) -> Result<HdrHistogram<u64>, CombineError> {
    let mut out = a.clone();
    out.add(b).map_err(|_| CombineError::HdrAddFailed)?;
    Ok(out)
}

fn pick_more_recent_exemplar(
    a: Option<Exemplar>,
    b: Option<Exemplar>,
    a_ts: Option<Instant>,
    b_ts: Option<Instant>,
) -> Option<Exemplar> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(ax), Some(bx)) => {
            if b_ts.map(|s| Some(s) >= a_ts).unwrap_or(false) { Some(bx) } else { Some(ax) }
        }
    }
}

fn combine_bucket_exemplars(
    dst: &mut Vec<Option<Exemplar>>,
    src: &[Option<Exemplar>],
    dst_ts: Option<Instant>,
    src_ts: Option<Instant>,
) {
    if dst.len() < src.len() {
        dst.resize(src.len(), None);
    }
    for (i, src_ex) in src.iter().enumerate() {
        let dst_slot = dst[i].take();
        dst[i] = pick_more_recent_exemplar(dst_slot, src_ex.clone(), dst_ts, src_ts);
    }
}

/// Errors from [`combine_into`] / [`combine_hdr`].
#[derive(Debug, PartialEq, Eq)]
pub enum CombineError {
    /// The two `MetricPoint`s have different value variants
    /// (e.g., Counter vs Gauge). Indicates a programming error —
    /// only matching identity should ever combine.
    TypeMismatch,
    /// HDR `add` failed (typically because reservoirs have
    /// incompatible bounds).
    HdrAddFailed,
}

impl std::fmt::Display for CombineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TypeMismatch => write!(f, "MetricPoint value type mismatch"),
            Self::HdrAddFailed => write!(f, "HDR histogram add failed"),
        }
    }
}

impl std::error::Error for CombineError {}

// =========================================================================
// Quantiles
// =========================================================================

/// Standard quantiles reported for histogram samples by reporters
/// that emit a fixed quantile set (Prometheus summary-shape, CSV
/// percentile columns, etc.).
pub const QUANTILES: &[f64] = &[0.5, 0.75, 0.90, 0.95, 0.98, 0.99, 0.999];

// =========================================================================
// Migration helpers — extract `name` label, build single-point families
// =========================================================================

/// Split a [`Labels`] value into `(family_name, residual_labels)` by
/// extracting the `name` label. Producers that historically embedded
/// the metric name in `Labels` (the pre-snapshot pattern) use this
/// to feed [`MetricSet::insert_metric`].
///
/// Panics if `name` is missing — every metric MUST have a family
/// name per OpenMetrics §4.4.
pub fn split_name_label(labels: &Labels) -> (String, Labels) {
    let name = labels.get("name")
        .map(|s| s.to_string())
        .expect("every metric must have a 'name' label");
    let mut residual = Labels::default();
    for (k, v) in labels.iter() {
        if k != "name" {
            residual = residual.with(k, v);
        }
    }
    (name, residual)
}

impl MetricSet {
    /// Insert a single observation, looking up or creating its
    /// `MetricFamily` by name and appending one [`Metric`]/[`MetricPoint`]
    /// pair. Convenience for producers that build snapshots one
    /// observation at a time.
    ///
    /// Panics if the family already exists with a different
    /// [`MetricType`] (per identity rules) or if the LabelSet
    /// already exists within the family (spec §4.5).
    pub fn insert_metric(
        &mut self,
        family_name: impl Into<String>,
        family_type: MetricType,
        labels: Labels,
        value: MetricValue,
        timestamp: Instant,
    ) {
        let name = family_name.into();
        let point = MetricPoint::new(value, timestamp);
        if let Some(fam) = self.families.iter_mut().find(|f| f.name == name) {
            assert_eq!(
                fam.r#type, family_type,
                "family '{}' already exists as {:?}; cannot insert as {:?}",
                name, fam.r#type, family_type,
            );
            fam.insert(Metric::single(labels, point));
        } else {
            let mut fam = MetricFamily::new(name, family_type);
            fam.insert(Metric::single(labels, point));
            self.families.push(fam);
        }
    }

    /// Insert a counter observation. Convenience over
    /// [`insert_metric`] that builds the [`CounterValue`] for you.
    pub fn insert_counter(
        &mut self,
        family_name: impl Into<String>,
        labels: Labels,
        total: u64,
        timestamp: Instant,
    ) {
        self.insert_metric(
            family_name, MetricType::Counter, labels,
            MetricValue::Counter(CounterValue::new(total)),
            timestamp,
        );
    }

    /// Insert a gauge observation. Convenience over [`insert_metric`].
    pub fn insert_gauge(
        &mut self,
        family_name: impl Into<String>,
        labels: Labels,
        value: f64,
        timestamp: Instant,
    ) {
        self.insert_metric(
            family_name, MetricType::Gauge, labels,
            MetricValue::Gauge(GaugeValue::new(value)),
            timestamp,
        );
    }

    /// Insert a histogram observation. Convenience over
    /// [`insert_metric`] that wraps the HDR reservoir into a
    /// [`HistogramValue`] and computes `count`/`sum` from it.
    pub fn insert_histogram(
        &mut self,
        family_name: impl Into<String>,
        labels: Labels,
        reservoir: HdrHistogram<u64>,
        timestamp: Instant,
    ) {
        self.insert_metric(
            family_name, MetricType::Histogram, labels,
            MetricValue::Histogram(HistogramValue::from_hdr(reservoir)),
            timestamp,
        );
    }
}

// =========================================================================
// Helpers
// =========================================================================

/// Approximate observation sum from an HDR histogram. HDR doesn't
/// store a true sum — we estimate by `mean × count`. Sufficient for
/// OpenMetrics `_sum` exposition; consumers who need an exact sum
/// have to record it independently.
fn hdr_sum(h: &HdrHistogram<u64>) -> f64 {
    h.mean() * h.len() as f64
}

/// Convenience: build a single-point Counter family.
pub fn counter_family(
    name: impl Into<String>,
    labels: Labels,
    total: u64,
    timestamp: Instant,
) -> MetricFamily {
    let mut f = MetricFamily::new(name, MetricType::Counter);
    f.insert(Metric::single(
        labels,
        MetricPoint::new(MetricValue::Counter(CounterValue::new(total)), timestamp),
    ));
    f
}

/// Convenience: build a single-point Gauge family.
pub fn gauge_family(
    name: impl Into<String>,
    labels: Labels,
    value: f64,
    timestamp: Instant,
) -> MetricFamily {
    let mut f = MetricFamily::new(name, MetricType::Gauge);
    f.insert(Metric::single(
        labels,
        MetricPoint::new(MetricValue::Gauge(GaugeValue::new(value)), timestamp),
    ));
    f
}

/// Convenience: build a single-point Histogram family from an HDR
/// reservoir.
pub fn histogram_family(
    name: impl Into<String>,
    labels: Labels,
    reservoir: HdrHistogram<u64>,
    timestamp: Instant,
) -> MetricFamily {
    let mut f = MetricFamily::new(name, MetricType::Histogram);
    f.insert(Metric::single(
        labels,
        MetricPoint::new(MetricValue::Histogram(HistogramValue::from_hdr(reservoir)), timestamp),
    ));
    f
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> Instant { Instant::now() }
    fn empty_set() -> MetricSet { MetricSet::new(Duration::from_secs(1)) }

    #[test]
    fn metric_set_is_empty_by_default() {
        let m = empty_set();
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
        assert!(m.family("anything").is_none());
        assert_eq!(m.interval(), Duration::from_secs(1));
    }

    #[test]
    fn metric_set_inserts_and_looks_up_by_name() {
        let mut m = empty_set();
        m.insert(counter_family("cycles", Labels::of("phase", "load"), 100, ts()));
        m.insert(gauge_family("temp", Labels::of("phase", "load"), 42.0, ts()));

        assert_eq!(m.len(), 2);
        assert!(m.family("cycles").is_some());
        assert!(m.family("temp").is_some());
        assert!(m.family("missing").is_none());
    }

    #[test]
    #[should_panic(expected = "names must be unique")]
    fn metric_set_rejects_duplicate_family_names() {
        let mut m = empty_set();
        m.insert(counter_family("cycles", Labels::of("a", "1"), 1, ts()));
        m.insert(counter_family("cycles", Labels::of("a", "2"), 2, ts()));
    }

    fn make_counter_set(interval: Duration, value: u64) -> MetricSet {
        let mut s = MetricSet::new(interval);
        s.insert(counter_family("cycles", Labels::of("name", "ops"), value, Instant::now()));
        s
    }

    fn make_histogram_set(interval: Duration, values: &[u64]) -> MetricSet {
        let mut h = HdrHistogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        for v in values { h.record(*v).unwrap(); }
        let mut s = MetricSet::new(interval);
        s.insert(histogram_family("latency", Labels::of("name", "rt"), h, Instant::now()));
        s
    }

    fn make_gauge_set(interval: Duration, value: f64) -> MetricSet {
        let mut s = MetricSet::new(interval);
        s.insert(gauge_family("temp", Labels::of("name", "x"), value, Instant::now()));
        s
    }

    #[test]
    fn coalesce_empty_returns_empty() {
        let merged = MetricSet::coalesce(&[]);
        assert!(merged.is_empty());
    }

    #[test]
    fn coalesce_single_clones() {
        let s = make_counter_set(Duration::from_secs(1), 10);
        let m = MetricSet::coalesce(std::slice::from_ref(&s));
        assert_eq!(m.interval(), Duration::from_secs(1));
        let f = m.family("cycles").unwrap();
        let c = match f.metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Counter(c) => c.total,
            _ => panic!("wrong type"),
        };
        assert_eq!(c, 10);
    }

    #[test]
    fn coalesce_counters_sum_total_and_intervals() {
        let merged = MetricSet::coalesce(&[
            make_counter_set(Duration::from_secs(1), 10),
            make_counter_set(Duration::from_secs(1), 25),
            make_counter_set(Duration::from_secs(1), 7),
        ]);
        assert_eq!(merged.interval(), Duration::from_secs(3));
        let total = match merged.family("cycles").unwrap()
            .metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Counter(c) => c.total,
            _ => panic!("wrong type"),
        };
        assert_eq!(total, 42);
    }

    #[test]
    fn coalesce_histograms_merge_reservoirs() {
        let merged = MetricSet::coalesce(&[
            make_histogram_set(Duration::from_secs(1), &[1_000, 2_000, 3_000]),
            make_histogram_set(Duration::from_secs(1), &[4_000, 5_000]),
        ]);
        let hv = match merged.family("latency").unwrap()
            .metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Histogram(h) => h.clone(),
            _ => panic!("wrong type"),
        };
        assert_eq!(hv.count, 5);
        assert!(hv.reservoir.max() >= 4_900);
    }

    #[test]
    fn coalesce_gauges_weighted_average_by_interval() {
        // Two snapshots: (1s @ 10.0) and (2s @ 20.0). Weighted avg
        // = (10*1 + 20*2) / 3 = 50/3 ≈ 16.67.
        let merged = MetricSet::coalesce(&[
            make_gauge_set(Duration::from_secs(1), 10.0),
            make_gauge_set(Duration::from_secs(2), 20.0),
        ]);
        let v = match merged.family("temp").unwrap()
            .metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Gauge(g) => g.value,
            _ => panic!("wrong type"),
        };
        assert!((v - 50.0/3.0).abs() < 0.01, "weighted gauge avg = {v}");
    }

    #[test]
    fn coalesce_disjoint_label_sets_appended() {
        // Same family name, different LabelSets — should NOT combine.
        let mut a = MetricSet::new(Duration::from_secs(1));
        a.insert(counter_family("cycles", Labels::of("phase", "load"), 100, ts()));
        let mut b = MetricSet::new(Duration::from_secs(1));
        b.insert(counter_family("cycles", Labels::of("phase", "verify"), 50, ts()));

        let merged = MetricSet::coalesce(&[a, b]);
        let f = merged.family("cycles").unwrap();
        assert_eq!(f.len(), 2);
        let load = f.metric_with_labels(&Labels::of("phase", "load")).unwrap();
        let verify = f.metric_with_labels(&Labels::of("phase", "verify")).unwrap();
        match load.point().unwrap().value() {
            MetricValue::Counter(c) => assert_eq!(c.total, 100),
            _ => panic!(),
        }
        match verify.point().unwrap().value() {
            MetricValue::Counter(c) => assert_eq!(c.total, 50),
            _ => panic!(),
        }
    }

    #[test]
    fn metric_family_records_type_and_optional_metadata() {
        let f = MetricFamily::new("latency", MetricType::Histogram)
            .with_unit("nanoseconds")
            .with_help("End-to-end op latency");
        assert_eq!(f.name(), "latency");
        assert_eq!(f.r#type(), MetricType::Histogram);
        assert_eq!(f.unit(), Some("nanoseconds"));
        assert_eq!(f.help(), Some("End-to-end op latency"));
    }

    #[test]
    #[should_panic(expected = "LabelSets must be unique")]
    fn metric_family_rejects_duplicate_labelsets() {
        let mut f = MetricFamily::new("cycles", MetricType::Counter);
        f.insert(Metric::single(
            Labels::of("phase", "load"),
            MetricPoint::untimed(MetricValue::Counter(CounterValue::new(1))),
        ));
        f.insert(Metric::single(
            Labels::of("phase", "load"),
            MetricPoint::untimed(MetricValue::Counter(CounterValue::new(2))),
        ));
    }

    #[test]
    fn metric_lookup_by_labels_matches_identity() {
        let mut f = MetricFamily::new("cycles", MetricType::Counter);
        f.insert(Metric::single(
            Labels::of("phase", "load"),
            MetricPoint::untimed(MetricValue::Counter(CounterValue::new(10))),
        ));
        f.insert(Metric::single(
            Labels::of("phase", "verify"),
            MetricPoint::untimed(MetricValue::Counter(CounterValue::new(20))),
        ));

        let load = f.metric_with_labels(&Labels::of("phase", "load")).unwrap();
        match load.point().unwrap().value() {
            MetricValue::Counter(c) => assert_eq!(c.total, 10),
            _ => panic!("wrong type"),
        }
        assert!(f.metric_with_labels(&Labels::of("phase", "missing")).is_none());
    }

    #[test]
    fn metric_type_strings_match_open_metrics_spec() {
        assert_eq!(MetricType::Counter.as_str(),        "counter");
        assert_eq!(MetricType::Gauge.as_str(),          "gauge");
        assert_eq!(MetricType::Histogram.as_str(),      "histogram");
        assert_eq!(MetricType::GaugeHistogram.as_str(), "gaugehistogram");
        assert_eq!(MetricType::Summary.as_str(),        "summary");
        assert_eq!(MetricType::Info.as_str(),           "info");
        assert_eq!(MetricType::StateSet.as_str(),       "stateset");
        assert_eq!(MetricType::Unknown.as_str(),        "unknown");
    }

    #[test]
    fn counter_combine_sums_total_keeps_earlier_created() {
        let t1 = Instant::now();
        let t0 = t1 - Duration::from_secs(60);

        let mut a = MetricPoint::new(
            MetricValue::Counter(CounterValue::new(10).with_created(t1)),
            t1,
        );
        let b = MetricPoint::new(
            MetricValue::Counter(CounterValue::new(25).with_created(t0)),
            t1,
        );
        combine_into(&mut a, &b).unwrap();
        match a.value() {
            MetricValue::Counter(c) => {
                assert_eq!(c.total, 35);
                assert_eq!(c.created, Some(t0), "earliest created wins");
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn histogram_combine_adds_reservoirs_re_derives_count_and_sum() {
        let mut h1 = HdrHistogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        h1.record(1_000_000).unwrap();
        h1.record(2_000_000).unwrap();
        let mut h2 = HdrHistogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        h2.record(3_000_000).unwrap();

        let mut a = MetricPoint::new(
            MetricValue::Histogram(HistogramValue::from_hdr(h1)),
            Instant::now(),
        );
        let b = MetricPoint::new(
            MetricValue::Histogram(HistogramValue::from_hdr(h2)),
            Instant::now(),
        );
        combine_into(&mut a, &b).unwrap();
        match a.value() {
            MetricValue::Histogram(h) => {
                assert_eq!(h.count, 3);
                assert!(h.sum > 0.0);
                assert!(h.reservoir.max() >= 3_000_000);
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn gauge_combine_keeps_most_recent_value() {
        let t1 = Instant::now();
        let t2 = t1 + Duration::from_secs(1);
        let mut a = MetricPoint::new(MetricValue::Gauge(GaugeValue::new(5.0)), t1);
        let b = MetricPoint::new(MetricValue::Gauge(GaugeValue::new(9.0)), t2);
        combine_into(&mut a, &b).unwrap();
        match a.value() {
            MetricValue::Gauge(g) => assert_eq!(g.value, 9.0),
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn combine_type_mismatch_is_hard_error() {
        let mut a = MetricPoint::untimed(MetricValue::Counter(CounterValue::new(1)));
        let b = MetricPoint::untimed(MetricValue::Gauge(GaugeValue::new(1.0)));
        let err = combine_into(&mut a, &b).unwrap_err();
        assert_eq!(err, CombineError::TypeMismatch);
    }

    #[test]
    fn exemplar_most_recent_wins_on_combine() {
        let t1 = Instant::now();
        let t2 = t1 + Duration::from_secs(1);
        let ex_old = Exemplar::new(Labels::of("trace_id", "old"), 1.0).with_timestamp(t1);
        let ex_new = Exemplar::new(Labels::of("trace_id", "new"), 2.0).with_timestamp(t2);

        let mut a = MetricPoint::new(
            MetricValue::Counter(CounterValue::new(5).with_exemplar(ex_old)),
            t1,
        );
        let b = MetricPoint::new(
            MetricValue::Counter(CounterValue::new(5).with_exemplar(ex_new.clone())),
            t2,
        );
        combine_into(&mut a, &b).unwrap();
        match a.value() {
            MetricValue::Counter(c) => {
                let e = c.exemplar.as_ref().expect("exemplar should survive");
                assert_eq!(e.labels.get("trace_id"), Some("new"));
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn histogram_projects_to_open_metrics_buckets() {
        let mut h = HdrHistogram::<u64>::new_with_bounds(1, 1_000_000, 3).unwrap();
        for v in [10u64, 50, 100, 500, 1000, 5000, 50_000].iter() {
            h.record(*v).unwrap();
        }
        let hv = HistogramValue::from_hdr(h);

        let buckets = hv.project_buckets(&[100, 1000, 10_000]);
        // 3 finite + 1 +Inf = 4 total
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0].upper_bound, BucketBound::Finite(100));
        assert_eq!(buckets[3].upper_bound, BucketBound::PositiveInfinity);

        // Cumulative: ≤100 should include 10, 50, 100 — count ≥ 3
        assert!(buckets[0].cumulative_count >= 3);
        // Final +Inf bucket equals total count
        assert_eq!(buckets[3].cumulative_count, hv.count);
        // Cumulative is monotonically non-decreasing
        for w in buckets.windows(2) {
            assert!(w[0].cumulative_count <= w[1].cumulative_count);
        }
    }

    #[test]
    fn metric_point_timestamp_propagates_on_construction() {
        let now = Instant::now();
        let p = MetricPoint::new(MetricValue::Gauge(GaugeValue::new(1.0)), now);
        assert_eq!(p.timestamp(), Some(now));

        let untimed = MetricPoint::untimed(MetricValue::Gauge(GaugeValue::new(1.0)));
        assert_eq!(untimed.timestamp(), None);
    }
}
