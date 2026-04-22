// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Unified metrics read API (SRD-42 §"MetricsQuery").
//!
//! Every consumer (TUI, summary report, SQLite emitter, GK
//! `metric()`/`metric_window()` nodes, programmatic callers) reads
//! through this single interface. There is no per-consumer access
//! layer — the query speaks the metrics system's native types
//! ([`MetricSet`] / [`MetricFamily`] / [`Metric`] / [`MetricPoint`]),
//! and exposes four uniform query modes:
//!
//! - [`MetricsQuery::now`] — read-through to the live instrument(s).
//! - [`MetricsQuery::cadence_window`] — last full closed window for a
//!   declared cadence.
//! - [`MetricsQuery::recent_window`] — approximation of "the last
//!   `span` of time": closed cadence windows tiled to span the
//!   request, plus `now` for the trailing fragment.
//! - [`MetricsQuery::session_lifetime`] — full canonical span of the
//!   session, walking the cascade down at read time so no in-flight
//!   data is missed.
//!
//! ## Selection
//!
//! Every query takes a [`Selection`] — a label-based filter applied
//! to each `(component_labels, metric_labels)` pair as the query
//! walks the store and the live tree. Identity for combine /
//! deduplication is `(family.name, label_set)` per OpenMetrics
//! §4.5.1.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::cadence_reporter::CadenceReporter;
use crate::component::Component;
use crate::labels::Labels;
use crate::snapshot::{MetricSet, MetricFamily, Metric};

/// A label-based filter for selecting which metrics a query operates
/// on. Composes by AND — every constraint must match.
#[derive(Clone, Debug, Default)]
pub struct Selection {
    /// Required metric family name. `None` matches any family.
    family: Option<String>,
    /// Required label `(key, value)` pairs on the metric's `LabelSet`.
    /// All pairs must match.
    label_eq: Vec<(String, String)>,
    /// Required label `(key, value_substring)` pairs. The value
    /// must contain the substring.
    label_contains: Vec<(String, String)>,
}

impl Selection {
    pub fn all() -> Self { Self::default() }

    /// Match any series in the named family.
    pub fn family(name: impl Into<String>) -> Self {
        Self { family: Some(name.into()), ..Default::default() }
    }

    /// Restrict to series whose `LabelSet` contains `key=value`.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.label_eq.push((key.into(), value.into()));
        self
    }

    /// Restrict to series whose label value at `key` contains
    /// `substring` (operator-friendly for path-style labels).
    pub fn with_label_containing(mut self, key: impl Into<String>, substring: impl Into<String>) -> Self {
        self.label_contains.push((key.into(), substring.into()));
        self
    }

    /// True when the selection's family constraint matches the
    /// candidate, or there is no family constraint.
    pub fn matches_family(&self, family_name: &str) -> bool {
        self.family.as_deref().map(|f| f == family_name).unwrap_or(true)
    }

    /// True when every label constraint is satisfied by `labels`.
    pub fn matches_labels(&self, labels: &Labels) -> bool {
        for (k, v) in &self.label_eq {
            if labels.get(k) != Some(v.as_str()) { return false; }
        }
        for (k, sub) in &self.label_contains {
            match labels.get(k) {
                Some(value) if value.contains(sub.as_str()) => {}
                _ => return false,
            }
        }
        true
    }
}

/// Errors returned by selection-required queries.
#[derive(Debug, PartialEq, Eq)]
pub enum SelectError {
    /// The selection matched no metric instance.
    NoMatch,
    /// The selection matched more than one instance — caller
    /// requested exactly one.
    MultipleMatches(usize),
}

impl std::fmt::Display for SelectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoMatch => write!(f, "selection matched no metric instance"),
            Self::MultipleMatches(n) =>
                write!(f, "selection matched {n} instances, expected exactly one"),
        }
    }
}

impl std::error::Error for SelectError {}

/// The unified metrics read interface. Constructed once at session
/// start with references to the cadence reporter (for closed
/// windows + cascade peeks) and the component tree root (for the
/// `now` mode's live instrument walk).
pub struct MetricsQuery {
    reporter: Arc<CadenceReporter>,
    /// Session-scope component root. Held for component-tree
    /// structural queries (e.g. "how many phases are running?")
    /// performed by display code like the TUI's Focus-LOD
    /// placeholder logic.
    component_root: Arc<RwLock<Component>>,
}

impl MetricsQuery {
    pub fn new(
        reporter: Arc<CadenceReporter>,
        component_root: Arc<RwLock<Component>>,
    ) -> Self {
        Self { reporter, component_root }
    }

    /// Reference to the cadence reporter — exposed so consumers that
    /// need to enumerate declared cadences (e.g., per-cadence
    /// columns) can ask it directly.
    pub fn reporter(&self) -> &Arc<CadenceReporter> { &self.reporter }

    /// Count of phases currently in `Running` state anywhere in the
    /// session's component tree. A pure structural query — no
    /// metric data involved. Used by display code that needs to
    /// decide "live vs waiting vs done" without re-implementing
    /// that logic over its own state mirror.
    pub fn running_phase_count(&self) -> usize {
        self.component_root.read()
            .map(|c| c.running_descendant_count())
            .unwrap_or(0)
    }

    // ---- now: live instrument peek -------------------------------------

    /// Recent snapshot at the smallest declared cadence, filtered
    /// by `selection`.
    ///
    /// Reads [`Self::cadence_window`] at the smallest declared
    /// cadence (1 s for default configurations) — the last fully-
    /// closed window of that cadence. Does NOT pass through to
    /// the live instruments: counter values, gauge values, and
    /// histogram reservoirs all come from the cadence-reporter
    /// store, which the scheduler populates via its per-tick
    /// coalesce.
    ///
    /// Why not a live-instrument peek? Counters are absolute
    /// atomics (peek is free), but histogram peeks return "samples
    /// accumulated since the scheduler's last drain" — a partial,
    /// drifting sub-interval window. The 1 s cadence window is a
    /// stable, sample-weighted view that matches what every other
    /// reader (summary, SQLite, cadence subscribers) sees for the
    /// same time slice.
    ///
    /// Returns an empty `MetricSet` before the first window of the
    /// smallest declared cadence closes (i.e., during the first
    /// `1 s` of a run). Callers that need true sub-second live data
    /// for a specific Timer should use
    /// [`crate::instruments::timer::Timer::peek_live_window`].
    pub fn now(&self, selection: &Selection) -> MetricSet {
        let smallest = self.reporter.declared_cadences().smallest();
        if smallest.is_zero() {
            return MetricSet::at(Instant::now(), Duration::ZERO);
        }
        self.cadence_window(smallest, selection)
    }

    /// Build a [`MetricHandle`] that caches the `(selection,
    /// cadence)` pair for repeated cheap reads. The handle reads
    /// the smallest declared cadence's last closed window on every
    /// `read_now` — no component-tree walk, no live-instrument
    /// access.
    ///
    /// Callers that want a specific cadence (not the smallest) can
    /// use [`Self::resolve_at`].
    pub fn resolve(&self, selection: Selection) -> MetricHandle {
        let cadence = self.reporter.declared_cadences().smallest();
        MetricHandle {
            reporter: self.reporter.clone(),
            selection,
            cadence,
        }
    }

    /// Same as [`Self::resolve`] but pins the handle to a specific
    /// cadence — use for per-cadence columns in summary reports
    /// or for explicit longer-horizon readers.
    pub fn resolve_at(&self, selection: Selection, cadence: Duration) -> MetricHandle {
        MetricHandle {
            reporter: self.reporter.clone(),
            selection,
            cadence,
        }
    }

    // ---- cadence_window: last closed snapshot --------------------------

    /// Latest fully-closed snapshot for the named cadence, filtered
    /// by `selection`. Returns an empty snapshot when no closed
    /// window has been published yet (early in a run).
    ///
    /// Walks every component tracked by the cadence reporter,
    /// merging matching metrics into one result. Identity follows
    /// OpenMetrics §4.5.1 — same `(family.name, label_set)` combines.
    pub fn cadence_window(&self, cadence: Duration, selection: &Selection) -> MetricSet {
        let mut out = MetricSet::at(Instant::now(), cadence);
        for component in self.reporter.component_labels() {
            let Some(snap) = self.reporter.latest(&component, cadence) else { continue };
            for family in snap.families() {
                if !selection.matches_family(family.name()) { continue; }
                for metric in family.metrics() {
                    if !selection.matches_labels(metric.labels()) { continue; }
                    insert_metric_into(&mut out, family, metric);
                }
            }
        }
        out
    }

    // ---- recent_window: cadence windows tiling span + now -------------

    /// Approximate "the last `span` of wall-clock time", filtered by
    /// `selection`.
    ///
    /// Implementation walks the smallest cadence whose ring covers
    /// `span`, merges the most recent `ceil(span / cadence)` closed
    /// windows of that cadence, then tops up with a `now` peek for
    /// the trailing fragment. Per SRD-42 §"Cost rule for recent_window",
    /// only matched metric instances combine — never the whole frame.
    pub fn recent_window(&self, span: Duration, selection: &Selection) -> MetricSet {
        let mut out = MetricSet::at(Instant::now(), span);

        // Pick the smallest declared cadence whose ring may cover span.
        let mut chosen: Option<Duration> = None;
        for layer in self.reporter.layers() {
            if layer.hidden { continue; }
            chosen = Some(layer.interval);
            if layer.interval >= span { break; }
        }
        let Some(cadence) = chosen else { return out };

        // Per-component, take the last `needed` windows from the ring.
        let needed = ((span.as_nanos().max(1)) / (cadence.as_nanos().max(1))) as usize + 1;
        for component in self.reporter.component_labels() {
            let ring = self.reporter.ring(&component, cadence);
            let take = ring.len().min(needed);
            let start = ring.len().saturating_sub(take);
            for snap in &ring[start..] {
                for family in snap.families() {
                    if !selection.matches_family(family.name()) { continue; }
                    for metric in family.metrics() {
                        if !selection.matches_labels(metric.labels()) { continue; }
                        insert_metric_into(&mut out, family, metric);
                    }
                }
            }
        }

        // `recent_window` / `session_lifetime` used to top up with
        // a live-instrument peek here. Since `now` now returns the
        // 1 s cadence window (SRD-42 decision), the recent fragment
        // is already visible through the smallest cadence's
        // prebuffer walked above. No additional top-up.

        out
    }

    // ---- session_lifetime: full canonical span ------------------------

    /// Full canonical session span as of *now*, filtered by
    /// `selection`. Walks the cascade *down* at read time:
    ///
    /// 1. Read-clones every cadence's prebuffer (in-flight partials).
    /// 2. Folds the latest closed snapshot per cadence too (as long
    ///    as it's still in the ring — defensive).
    /// 3. Tops up with a live `now` peek for samples since the last
    ///    smallest-cadence tick.
    ///
    /// Per SRD-42 §"Cost rule for recent_window", only matched
    /// metric instances combine — same shape as `recent_window`.
    pub fn session_lifetime(&self, selection: &Selection) -> MetricSet {
        let session_age = self.reporter.started_at().elapsed();
        let mut out = MetricSet::at(Instant::now(), session_age);

        for component in self.reporter.component_labels() {
            for layer in self.reporter.layers() {
                // Prebuffer (in-flight partial).
                if let Some(pre) = self.reporter.prebuffer(&component, layer.interval) {
                    for family in pre.families() {
                        if !selection.matches_family(family.name()) { continue; }
                        for metric in family.metrics() {
                            if !selection.matches_labels(metric.labels()) { continue; }
                            insert_metric_into(&mut out, family, metric);
                        }
                    }
                }
                // Plus the largest-cadence latest closed snapshot
                // — represents data that already promoted out of
                // smaller layers.
                if layer.interval == self.reporter.layers().last().map(|l| l.interval).unwrap_or_default() {
                    if let Some(latest) = self.reporter.latest(&component, layer.interval) {
                        for family in latest.families() {
                            if !selection.matches_family(family.name()) { continue; }
                            for metric in family.metrics() {
                                if !selection.matches_labels(metric.labels()) { continue; }
                                insert_metric_into(&mut out, family, metric);
                            }
                        }
                    }
                }
            }
        }

        // Top up with live now.
        let live = self.now(selection);
        for family in live.families() {
            for metric in family.metrics() {
                insert_metric_into(&mut out, family, metric);
            }
        }

        out
    }

    // ---- expect-exactly-one helpers ------------------------------------

    /// Run a query mode and assert exactly one matching `Metric` per
    /// the SRD's "specific metric" semantics. Returns `Err` if 0 or
    /// >1 matches.
    pub fn select_one<'a, F>(&self, mode: F) -> Result<MetricSet, SelectError>
    where
        F: FnOnce(&Self) -> MetricSet,
    {
        let snap = mode(self);
        let total: usize = snap.families().map(|f| f.len()).sum();
        match total {
            0 => Err(SelectError::NoMatch),
            1 => Ok(snap),
            n => Err(SelectError::MultipleMatches(n)),
        }
    }
}

/// Memoized pull handle — resolved once via [`MetricsQuery::resolve`],
/// then reused for cheap per-draw / per-frame reads.
///
/// The handle caches the `(selection, cadence)` pair the caller
/// asked for. Each `read_now` issues a `cadence_window` query
/// against the reporter — O(components) per call, no component-tree
/// walk, no instrument access.
///
/// Per SRD-42 (revised), "recent info" always routes through the
/// cadence-reporter store, never through live instruments. The
/// handle's `read_now` reads the smallest declared cadence's last
/// closed window; callers who need true sub-second live data for a
/// specific Timer should use
/// [`crate::instruments::timer::Timer::peek_live_window`] directly.
pub struct MetricHandle {
    reporter: Arc<CadenceReporter>,
    selection: Selection,
    cadence: Duration,
}

impl MetricHandle {
    /// Read the last closed window at this handle's cadence,
    /// filtered by the handle's selection. Non-mutating, safe to
    /// call arbitrarily often.
    pub fn read_now(&self) -> MetricSet {
        let mut out = MetricSet::at(Instant::now(), self.cadence);
        for component in self.reporter.component_labels() {
            let Some(snap) = self.reporter.latest(&component, self.cadence) else { continue };
            for family in snap.families() {
                if !self.selection.matches_family(family.name()) { continue; }
                for metric in family.metrics() {
                    if !self.selection.matches_labels(metric.labels()) { continue; }
                    insert_metric_into(&mut out, family, metric);
                }
            }
        }
        out
    }

    /// No-op retained for API compatibility with callers that
    /// expect to "refresh" after phase transitions. The new handle
    /// reads through the cadence reporter's store, which already
    /// reflects the current set of tracked components — no resync
    /// needed.
    pub fn refresh(&mut self) {}

    /// The selection this handle was resolved against.
    pub fn selection(&self) -> &Selection { &self.selection }

    /// Cadence this handle reads from (the smallest declared
    /// cadence at resolve time).
    pub fn cadence(&self) -> Duration { self.cadence }

    /// Number of components currently tracked by the reporter —
    /// informational. Not cached; queried fresh each call.
    pub fn source_count(&self) -> usize {
        self.reporter.component_labels().len()
    }
}

/// Insert one `(family, metric)` pair into `out`, merging with an
/// existing same-identity entry per OpenMetrics §4.5.1.
fn insert_metric_into(out: &mut MetricSet, family: &MetricFamily, metric: &Metric) {
    let Some(point) = metric.point() else { return };
    let existing = out.family(family.name())
        .and_then(|f| f.metric_with_labels(metric.labels()))
        .is_some();
    if existing {
        // Combine into existing — requires owned access via a
        // rebuild. Simpler pattern: drop the existing family and
        // rebuild with a coalesced replacement. For Phase 7 v1 we
        // accept the cost and use `MetricSet::coalesce` over a
        // two-element slice.
        let mut tmp = MetricSet::at(out.captured_at(), out.interval());
        tmp.insert_metric(
            family.name().to_string(),
            family.r#type(),
            metric.labels().clone(),
            point.value().clone(),
            point.timestamp().unwrap_or(out.captured_at()),
        );
        let merged = MetricSet::coalesce(std::slice::from_ref(out)
            .iter().chain(std::slice::from_ref(&tmp).iter())
            .cloned().collect::<Vec<_>>().as_slice());
        *out = merged;
    } else {
        out.insert_metric(
            family.name().to_string(),
            family.r#type(),
            metric.labels().clone(),
            point.value().clone(),
            point.timestamp().unwrap_or(out.captured_at()),
        );
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadence::{Cadences, CadenceTree};
    use crate::component::{Component, ComponentState, InstrumentSet, attach};
    use crate::snapshot::MetricValue;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct StubInstruments {
        n: AtomicU64,
    }
    impl InstrumentSet for StubInstruments {
        fn capture_delta(&self, interval: Duration) -> MetricSet {
            let v = self.n.load(Ordering::Relaxed);
            let mut s = MetricSet::new(interval);
            s.insert_counter("ops", Labels::default(), v, Instant::now());
            s
        }
        fn capture_current(&self) -> MetricSet {
            self.capture_delta(Duration::ZERO)
        }
    }

    fn build_one_component_query() -> (Arc<RwLock<Component>>, Arc<CadenceReporter>, MetricsQuery) {
        let root = Component::root(Labels::of("session", "s1"), HashMap::new());
        let phase = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "load"), HashMap::new()),
        ));
        attach(&root, &phase);
        {
            let mut p = phase.write().unwrap();
            p.set_state(ComponentState::Running);
            p.set_instruments(Arc::new(StubInstruments { n: AtomicU64::new(7) }));
        }

        let cadences = Cadences::new(&[Duration::from_millis(100)]).unwrap();
        let tree = CadenceTree::plan_default(cadences);
        let reporter = Arc::new(CadenceReporter::new(tree));
        let query = MetricsQuery::new(reporter.clone(), root.clone());
        (root, reporter, query)
    }

    #[test]
    fn now_reads_smallest_cadence_window() {
        // `now` no longer walks the live tree — it reads
        // `cadence_window(smallest_declared)`. So we must ingest a
        // closed window first; before any close, `now` is empty.
        let (_root, reporter, query) = build_one_component_query();
        assert!(query.now(&Selection::family("ops")).is_empty(),
            "pre-close now should be empty");

        let labels = Labels::of("session", "s1").extend(&Labels::of("phase", "load"));
        let mut s = MetricSet::new(Duration::from_millis(100));
        s.insert_counter("ops", Labels::default(), 42, Instant::now());
        reporter.ingest(&labels, s);

        let snap = query.now(&Selection::family("ops"));
        let total = match snap.family("ops").unwrap()
            .metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Counter(c) => c.total,
            _ => panic!("not a counter"),
        };
        assert_eq!(total, 42);
    }

    #[test]
    fn cadence_window_returns_latest_closed_snapshot() {
        let (_root, reporter, query) = build_one_component_query();
        let labels = Labels::of("session", "s1").extend(&Labels::of("phase", "load"));
        // Inject a closed window via the reporter.
        let mut s = MetricSet::new(Duration::from_millis(100));
        s.insert_counter("ops", Labels::default(), 99, Instant::now());
        reporter.ingest(&labels, s);

        let snap = query.cadence_window(Duration::from_millis(100), &Selection::family("ops"));
        let f = snap.family("ops").expect("ops family in cadence_window result");
        match f.metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Counter(c) => assert_eq!(c.total, 99),
            _ => panic!("not a counter"),
        }
    }

    #[test]
    fn selection_filter_excludes_non_matching_labels() {
        let (_root, reporter, query) = build_one_component_query();
        let labels = Labels::of("session", "s1").extend(&Labels::of("phase", "load"));
        let mut s = MetricSet::new(Duration::from_millis(100));
        s.insert_counter("ops", Labels::of("kind", "a"), 5, Instant::now());
        s.insert_counter("ops", Labels::of("kind", "b"), 9, Instant::now());
        reporter.ingest(&labels, s);

        let snap = query.cadence_window(
            Duration::from_millis(100),
            &Selection::family("ops").with_label("kind", "b"),
        );
        let f = snap.family("ops").expect("ops family");
        assert_eq!(f.len(), 1);
        match f.metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Counter(c) => assert_eq!(c.total, 9),
            _ => panic!("not a counter"),
        }
    }

    #[test]
    fn select_one_errors_on_zero_matches() {
        let (_root, _reporter, query) = build_one_component_query();
        let result = query.select_one(|q| q.cadence_window(
            Duration::from_millis(100),
            &Selection::family("nonexistent"),
        ));
        assert_eq!(result.unwrap_err(), SelectError::NoMatch);
    }

    #[test]
    fn select_one_succeeds_on_exact_match() {
        let (_root, reporter, query) = build_one_component_query();
        let labels = Labels::of("session", "s1").extend(&Labels::of("phase", "load"));
        let mut s = MetricSet::new(Duration::from_millis(100));
        s.insert_counter("ops", Labels::default(), 1, Instant::now());
        reporter.ingest(&labels, s);

        let result = query.select_one(|q| q.cadence_window(
            Duration::from_millis(100),
            &Selection::family("ops"),
        ));
        assert!(result.is_ok());
    }
}
