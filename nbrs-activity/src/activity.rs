// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Activity: the unit of concurrent execution.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};


use nbrs_errorhandler::ErrorRouter;
use nbrs_metrics::instruments::counter::Counter;
use nbrs_metrics::instruments::histogram::Histogram;
use nbrs_metrics::instruments::timer::Timer;
use nbrs_metrics::labels::Labels;
use nbrs_metrics::snapshot::MetricSet;
use nbrs_rate::RateLimiter;

use crate::adapter::{DriverAdapter, OpDispenser};
// CycleSource removed — all iteration goes through DataSourceFactory
use crate::opseq::{OpSequence, SequencerType};
use crate::validation;

/// Configuration for an activity.
pub struct ActivityConfig {
    pub name: String,
    pub cycles: u64,
    /// Number of fibers (tokio tasks) executing stanzas concurrently.
    pub concurrency: usize,
    /// Target ops/sec for the single activity-level rate
    /// limiter. `None` disables rate limiting. There is one
    /// rate limiter per activity — no separate stanza-rate
    /// mechanism.
    pub rate: Option<f64>,
    pub sequencer: SequencerType,
    pub error_spec: String,
    pub max_retries: u32,
    /// Maximum number of ops within a stanza that execute concurrently.
    pub stanza_concurrency: usize,
    /// Source factory for data-driven phases. When present, fibers pull
    /// from this source instead of the cycle counter. Each fiber creates
    /// its own reader via `create_reader()`.
    pub source_factory: Option<Arc<dyn nbrs_variates::source::DataSourceFactory>>,
    /// Suppress the inline stderr progress line (TUI handles
    /// display). Wrapped in `Arc<AtomicBool>` so the runner can
    /// flip it at runtime — when the user dismisses the TUI
    /// mid-run (`q` keypress), this flag drops to `false` and
    /// the status thread resumes emission, making the
    /// experience feel like tui=off was set from the start.
    /// A bare `bool` would have baked the TUI-mode value in at
    /// activity construction, so post-dismissal there'd be no
    /// progress display at all.
    pub suppress_status_line: Arc<std::sync::atomic::AtomicBool>,
    /// Names of relevancy / live aggregate metrics to surface on
    /// the inline progress line and the per-phase ✓ DONE summary.
    /// Empty → no extra metrics are shown (status line carries
    /// only the universal counters). Set per-phase via the YAML
    /// `status_metrics: [name]` field; workload-level phases that
    /// compute relevancy must opt in explicitly — nothing is
    /// presumed to be present.
    pub status_metrics: Vec<String>,
    /// Full root-first coordinate label (e.g.
    /// `(profile=label_00), (bucket=1, kind=READ)`) for this
    /// phase's iteration. Used by the ✓ DONE summary line to
    /// show the same identity the per-phase header would carry,
    /// so the completed-status line stands alone — no separate
    /// phase-starting row needed.
    pub phase_labels: String,
    /// Pre-map sequence number `[N/total]` for this phase. Same
    /// numbering the TUI tree row and post-run summary use.
    /// `None` ⇒ inline-CLI form / pre-map didn't produce a seq.
    pub phase_seq: Option<(usize, usize)>,
    /// Resolved `readouts:` slot bindings from the workload
    /// (SRD-63 §5). Empty → all slots fall through to the
    /// hard-coded built-in defaults (`phase_done` at
    /// `on_phase_end`, `phase_status` at `on_update`).
    pub readouts: nbrs_workload::model::ReadoutsBindings,
    /// CLI `--readout=<body>` override (SRD-63 §8).
    /// Applies to the `on_update` slot only; replaces
    /// (or with `+` prefix, appends to) whatever the
    /// workload + default path resolved.
    pub cli_readout_override: Option<String>,
    /// Per-session SQLite writer. Used by Push 6's snapshot
    /// store — every binder.fire captures its rendered
    /// output via `upsert_readout_snapshot` so replay /
    /// scrollback can reproduce the line later. `None`
    /// means snapshot capture is skipped (no session db
    /// — short test fixtures, in-memory sessions).
    pub snapshot_writer: Option<
        Arc<std::sync::Mutex<Option<nbrs_metrics::reporters::sqlite::SqliteReporter>>>,
    >,
}

impl Default for ActivityConfig {
    fn default() -> Self {
        Self {
            name: "default".into(),
            cycles: 1,
            concurrency: 1,
            rate: None,
            sequencer: SequencerType::Bucket,
            error_spec: ".*:warn,stop".into(),
            max_retries: 3,
            stanza_concurrency: 1,
            source_factory: None,
            suppress_status_line: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            status_metrics: Vec::new(),
            phase_labels: String::new(),
            phase_seq: None,
            readouts: nbrs_workload::model::ReadoutsBindings::default(),
            cli_readout_override: None,
            snapshot_writer: None,
        }
    }
}

/// Standard metrics for an activity. Shared via Arc so the metrics
/// scheduler can capture snapshots while executor tasks record.
///
/// Fields are `Arc<Counter>` / `Arc<Timer>` / `Arc<Histogram>` so
/// the same instrument is held both here (for per-cycle record
/// access) and in the activity's `Component` instrument registry
/// (for the cadence reporter's per-tick capture). Per-cycle code
/// continues calling `metrics.cycles_total.inc()` etc. through
/// `Arc`'s `Deref`.
///
/// Static instruments (the fields below) register on the component
/// from [`ActivityMetrics::register_on`] called by
/// [`Activity::attach_component`]. Dynamic per-error-type counters
/// and adapter-specific metrics flow through the
/// [`nbrs_metrics::component::DynamicCapture`] hook implemented
/// for [`ActivityMetricsDynamic`].
pub struct ActivityMetrics {
    pub service_time: Arc<Timer>,
    pub wait_time: Arc<Timer>,
    pub response_time: Arc<Timer>,
    /// Service time for successful ops only. Allows isolating
    /// success latency from error/retry latency.
    pub result_success_time: Arc<Timer>,
    /// Number of tries per op (1 = succeeded first try, 2+ = retried).
    /// Distribution shape reveals incremental saturation.
    pub tries_histogram: Arc<Histogram>,
    pub cycles_total: Arc<Counter>,
    pub successes_total: Arc<Counter>,
    pub skips_total: Arc<Counter>,
    pub errors_total: Arc<Counter>,
    pub stanzas_total: Arc<Counter>,
    /// Number of ops dispatched to adapters (monotonic).
    pub ops_started: std::sync::atomic::AtomicU64,
    /// Number of ops returned from adapters (monotonic).
    pub ops_finished: std::sync::atomic::AtomicU64,
    pub result_elements: Arc<Counter>,
    pub result_bytes: Arc<Counter>,
    /// Per-error-type counters, keyed by error_name.
    /// Created on demand when a new error type is first seen.
    /// Captured via the [`DynamicCapture`] hook — the registry on
    /// `Component` only holds instruments known at init.
    error_type_counts: std::sync::Mutex<std::collections::HashMap<String, Arc<Counter>>>,
    labels: Labels,
    /// Dispensers for adapter-specific metrics capture. Set after dispenser creation.
    dispensers: std::sync::Mutex<Option<Arc<Vec<Arc<dyn crate::adapter::OpDispenser>>>>>,
    /// Shared handles to the per-template validation metrics. Populated
    /// after executor setup so the progress thread can read live
    /// relevancy aggregates (recall-over-last-N, all-time mean) without
    /// draining the precision accumulators.
    validation_metrics: std::sync::Mutex<Option<Arc<Vec<Arc<crate::validation::ValidationMetrics>>>>>,
}

impl ActivityMetrics {
    pub fn new(labels: &Labels) -> Self {
        Self::with_sigdigs(labels, nbrs_metrics::instruments::histogram::DEFAULT_HDR_SIGDIGS)
    }

    /// Construct activity metrics using an explicit HDR
    /// significant-digits precision for every histogram and
    /// timer below. The runner resolves `hdr.sigdigs` from the
    /// session root via
    /// [`nbrs_metrics::instruments::histogram::resolve_hdr_sigdigs`]
    /// once per activity and threads it here (SRD 40 §"HDR
    /// significant digits — subtree-scoped setting").
    pub fn with_sigdigs(labels: &Labels, sigdigs: u8) -> Self {
        Self {
            service_time: Arc::new(Timer::with_sigdigs(labels.with("name", "cycles_servicetime"), sigdigs)),
            wait_time: Arc::new(Timer::with_sigdigs(labels.with("name", "cycles_waittime"), sigdigs)),
            response_time: Arc::new(Timer::with_sigdigs(labels.with("name", "cycles_responsetime"), sigdigs)),
            result_success_time: Arc::new(Timer::with_sigdigs(labels.with("name", "result_success"), sigdigs)),
            tries_histogram: Arc::new(nbrs_metrics::instruments::histogram::Histogram::with_sigdigs(labels.with("name", "tries"), sigdigs)),
            cycles_total: Arc::new(Counter::new(labels.with("name", "cycles_total"))),
            successes_total: Arc::new(Counter::new(labels.with("name", "successes_total"))),
            skips_total: Arc::new(Counter::new(labels.with("name", "skips_total"))),
            errors_total: Arc::new(Counter::new(labels.with("name", "errors_total"))),
            stanzas_total: Arc::new(Counter::new(labels.with("name", "stanzas_total"))),
            ops_started: std::sync::atomic::AtomicU64::new(0),
            ops_finished: std::sync::atomic::AtomicU64::new(0),
            result_elements: Arc::new(Counter::new(labels.with("name", "result_elements"))),
            result_bytes: Arc::new(Counter::new(labels.with("name", "result_bytes"))),
            error_type_counts: std::sync::Mutex::new(std::collections::HashMap::new()),
            labels: labels.clone(),
            dispensers: std::sync::Mutex::new(None),
            validation_metrics: std::sync::Mutex::new(None),
        }
    }

    /// Register every static instrument on `component` and install
    /// a [`DynamicCapture`] hook for the dynamic surface (per-error-type
    /// counters and adapter-specific metrics from registered dispensers).
    ///
    /// Called once from [`Activity::attach_component`]. After this
    /// point:
    /// - The cadence reporter's tree walk picks up every static
    ///   instrument here through `component.capture_delta`.
    /// - Per-cycle code continues recording through this struct's
    ///   typed `Arc` fields — same `Arc` that the registry holds.
    pub fn register_on(
        self: &Arc<Self>,
        component: &mut nbrs_metrics::component::Component,
    ) -> Result<(), String> {
        use nbrs_metrics::component::InstrumentRef;
        // Order mirrors the historical capture_delta emission so
        // metric_family ordering stays stable for downstream
        // consumers. `successes_total` was omitted historically
        // even though the field exists; preserve that omission to
        // avoid a behavioural change.
        component.register_instrument(
            "cycles_servicetime",
            InstrumentRef::Timer(self.service_time.clone()),
        )?;
        component.register_instrument(
            "cycles_waittime",
            InstrumentRef::Timer(self.wait_time.clone()),
        )?;
        component.register_instrument(
            "cycles_responsetime",
            InstrumentRef::Timer(self.response_time.clone()),
        )?;
        component.register_instrument(
            "result_success",
            InstrumentRef::Timer(self.result_success_time.clone()),
        )?;
        component.register_instrument(
            "cycles_total",
            InstrumentRef::Counter(self.cycles_total.clone()),
        )?;
        component.register_instrument(
            "skips_total",
            InstrumentRef::Counter(self.skips_total.clone()),
        )?;
        component.register_instrument(
            "errors_total",
            InstrumentRef::Counter(self.errors_total.clone()),
        )?;
        component.register_instrument(
            "stanzas_total",
            InstrumentRef::Counter(self.stanzas_total.clone()),
        )?;
        component.register_instrument(
            "result_elements",
            InstrumentRef::Counter(self.result_elements.clone()),
        )?;
        component.register_instrument(
            "result_bytes",
            InstrumentRef::Counter(self.result_bytes.clone()),
        )?;
        component.register_instrument(
            "tries",
            InstrumentRef::Histogram(self.tries_histogram.clone()),
        )?;

        component.set_dynamic_capture(Arc::new(
            ActivityMetricsDynamic {
                metrics: self.clone(),
                prev_counters: std::sync::Mutex::new(
                    std::collections::HashMap::new(),
                ),
            },
        ));
        Ok(())
    }

    /// Return the number of cycles completed so far.
    ///
    /// Reads from the `cycles_total` counter atomically. Used by the
    /// progress reporter thread to display live throughput.
    pub fn cycles_completed(&self) -> u64 {
        self.cycles_total.get()
    }

    /// Increment counter for a specific error type. Creates the
    /// counter on first occurrence of each error name. The new
    /// counter is read by the [`DynamicCapture`] hook on every
    /// capture tick — registration on `Component` is implicit
    /// through the hook, not a per-name `register_instrument` call.
    pub fn count_error_type(&self, error_name: &str) {
        let mut map = self.error_type_counts.lock()
            .unwrap_or_else(|e| e.into_inner());
        let counter = map.entry(error_name.to_string())
            .or_insert_with(|| {
                Arc::new(Counter::new(
                    self.labels.with("name", &format!("errors.{error_name}")),
                ))
            });
        counter.inc();
    }

    /// Capture an absolute snapshot (counters at their current value,
    /// timer histograms drained as deltas).
    ///
    /// Used by the legacy per-activity capture thread. For the component
    /// tree scheduler, use [`capture_delta`] instead.
    pub fn capture(&self, interval: std::time::Duration) -> MetricSet {
        use nbrs_metrics::snapshot::split_name_label;
        let service_snap = self.service_time.snapshot();
        let wait_snap = self.wait_time.snapshot();
        let response_snap = self.response_time.snapshot();
        let success_snap = self.result_success_time.snapshot();
        let tries_snap = self.tries_histogram.snapshot();
        let now = Instant::now();
        let mut snap = MetricSet::at(now, interval);

        let (n, lbl) = split_name_label(self.service_time.labels());
        snap.insert_histogram(n, lbl, service_snap.histogram, now);
        let (n, lbl) = split_name_label(self.wait_time.labels());
        snap.insert_histogram(n, lbl, wait_snap.histogram, now);
        let (n, lbl) = split_name_label(self.response_time.labels());
        snap.insert_histogram(n, lbl, response_snap.histogram, now);
        let (n, lbl) = split_name_label(self.result_success_time.labels());
        snap.insert_histogram(n, lbl, success_snap.histogram, now);

        let (n, lbl) = split_name_label(self.cycles_total.labels());
        snap.insert_counter(n, lbl, self.cycles_total.get(), now);
        let (n, lbl) = split_name_label(self.skips_total.labels());
        snap.insert_counter(n, lbl, self.skips_total.get(), now);
        let (n, lbl) = split_name_label(self.errors_total.labels());
        snap.insert_counter(n, lbl, self.errors_total.get(), now);
        let (n, lbl) = split_name_label(self.stanzas_total.labels());
        snap.insert_counter(n, lbl, self.stanzas_total.get(), now);
        let (n, lbl) = split_name_label(self.result_elements.labels());
        snap.insert_counter(n, lbl, self.result_elements.get(), now);
        let (n, lbl) = split_name_label(self.result_bytes.labels());
        snap.insert_counter(n, lbl, self.result_bytes.get(), now);
        let (n, lbl) = split_name_label(self.tries_histogram.labels());
        snap.insert_histogram(n, lbl, tries_snap, now);

        let error_counts = self.error_type_counts.lock()
            .unwrap_or_else(|e| e.into_inner());
        for counter in error_counts.values() {
            let (n, lbl) = split_name_label(counter.labels());
            snap.insert_counter(n, lbl, counter.get(), now);
        }

        snap
    }

    /// Register dispensers for adapter-specific metrics capture.
    pub fn set_dispensers(&self, dispensers: Arc<Vec<Arc<dyn crate::adapter::OpDispenser>>>) {
        *self.dispensers.lock().unwrap_or_else(|e| e.into_inner()) = Some(dispensers);
    }

    /// Register the per-template validation metrics so the progress
    /// thread can read live relevancy aggregates.
    pub fn set_validation_metrics(
        &self,
        vms: Arc<Vec<Arc<crate::validation::ValidationMetrics>>>,
    ) {
        *self.validation_metrics.lock().unwrap_or_else(|e| e.into_inner()) = Some(vms);
    }

    /// Snapshot live relevancy aggregates from every registered
    /// validation-metrics instance (one per op template that declared
    /// `relevancy:`). Non-destructive — safe to call every frame.
    pub fn collect_relevancy_live(&self) -> Vec<crate::validation::RelevancyLive> {
        let mut out = Vec::new();
        if let Ok(guard) = self.validation_metrics.lock() {
            if let Some(ref vms) = *guard {
                for vm in vms.iter() {
                    out.extend(vm.live_snapshot());
                }
            }
        }
        out
    }

    /// Collect every status-line value whose name matches one of
    /// `patterns`. Patterns are glob-style (`*` for any run of
    /// characters, `?` for a single character; literal otherwise),
    /// matched against the canonical names below. Returns formatted
    /// ` name:value` strings ready to concatenate into the inline
    /// progress / DONE summary line, in pattern declaration order
    /// with duplicates suppressed.
    ///
    /// Supported metric families:
    /// - **Relevancy aggregates** — one entry per registered
    ///   `relevancy.functions:` (e.g. `recall`, `precision`,
    ///   `f1`). The relevancy cutoff rides on the metric's
    ///   `k` / `r` labels rather than the family name.
    ///   Value: `total_mean × 100` as a percent.
    /// - **Latency** — `latency_p50`, `latency_p99`, `latency_max`,
    ///   `latency_mean`, sourced from `service_time` (the per-op
    ///   timer, exclusive of wait time). Value: auto-scaled
    ///   duration via [`nbrs_metrics::reporters::summary::format_duration`].
    pub fn collect_status_values(&self, patterns: &[String]) -> Vec<String> {
        if patterns.is_empty() {
            return Vec::new();
        }
        // Build the candidate list once. Order is stable so
        // pattern ordering, not iteration order, drives the
        // output sequence.
        let mut candidates: Vec<(String, String)> = Vec::new();
        for live in self.collect_relevancy_live() {
            candidates.push((
                live.name,
                format!("{:.2}%", live.total_mean * 100.0),
            ));
        }
        let snap = self.service_time.peek_snapshot();
        let h = &snap.histogram;
        if h.len() > 0 {
            let fmt = nbrs_metrics::reporters::summary::format_duration;
            candidates.push(("latency_p50".to_string(),  fmt(h.value_at_quantile(0.50) as f64)));
            candidates.push(("latency_p99".to_string(),  fmt(h.value_at_quantile(0.99) as f64)));
            candidates.push(("latency_max".to_string(),  fmt(h.max() as f64)));
            candidates.push(("latency_mean".to_string(), fmt(h.mean())));
        }
        let mut out: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for pat in patterns {
            for (name, val) in &candidates {
                if !seen.contains(name.as_str()) && glob_match(pat, name) {
                    seen.insert(name.clone());
                    out.push(format!(" {name}:{val}"));
                }
            }
        }
        out
    }

    /// Collect status counters from all registered dispensers.
    pub fn collect_status_counters(&self) -> Vec<(String, u64)> {
        let mut counters = Vec::new();
        if let Ok(guard) = self.dispensers.lock() {
            if let Some(ref disps) = *guard {
                for disp in disps.iter() {
                    for (name, total) in disp.status_counters() {
                        counters.push((name.to_string(), total));
                    }
                }
            }
        }
        counters
    }

}

/// [`DynamicCapture`] adapter for [`ActivityMetrics`]. Captures the
/// dynamic surface — per-error-type counters and adapter-specific
/// metrics from registered dispensers — that isn't known at
/// `register_on` time and therefore can't live in the static
/// component instrument registry.
struct ActivityMetricsDynamic {
    metrics: Arc<ActivityMetrics>,
    /// Per-counter previous-value baseline for delta emission on
    /// the `drain=true` path. Keyed by `counter.labels().identity_hash()`.
    /// Mirrors the per-component baseline that `Component` keeps for
    /// registered counters; per-error-type counters live outside
    /// the registry so the baseline travels with the hook.
    ///
    /// Why deltas: `MetricSet::combine_into` for Counter is
    /// `total = a.total.saturating_add(b.total)` — the cascade
    /// coalesce path treats Counter.total as the per-interval
    /// delta and SUMS across intervals. Emitting absolutes here
    /// would inflate as the cascade coalesces.
    prev_counters: std::sync::Mutex<std::collections::HashMap<u64, u64>>,
}

impl nbrs_metrics::component::DynamicCapture for ActivityMetricsDynamic {
    fn capture_into(
        &self,
        out: &mut MetricSet,
        now: Instant,
        drain: bool,
    ) {
        use nbrs_metrics::snapshot::{MetricType, MetricValue, split_name_label};

        // Per-error-type counters.
        // - drain=true (cadence path): emit deltas vs. the stored
        //   baseline so cascade coalesce sums across intervals
        //   without inflation.
        // - drain=false (peek path): emit absolute totals.
        let error_counts = self.metrics.error_type_counts.lock()
            .unwrap_or_else(|e| e.into_inner());
        if drain {
            let mut prev = self.prev_counters.lock()
                .unwrap_or_else(|e| e.into_inner());
            for counter in error_counts.values() {
                let (name, lbl) = split_name_label(counter.labels());
                let current = counter.get();
                let key = counter.labels().identity_hash();
                let previous = prev.insert(key, current).unwrap_or(0);
                out.insert_counter(
                    name, lbl, current.saturating_sub(previous), now,
                );
            }
        } else {
            for counter in error_counts.values() {
                let (name, lbl) = split_name_label(counter.labels());
                out.insert_counter(name, lbl, counter.get(), now);
            }
        }

        // Adapter-specific metrics from each registered dispenser.
        // Passthrough — the adapter decides delta vs. absolute
        // semantics for its own metrics.
        if let Some(ref disps) = *self.metrics.dispensers.lock()
            .unwrap_or_else(|e| e.into_inner())
        {
            for dispenser in disps.iter() {
                for (family, metric_labels, value) in dispenser.adapter_metrics() {
                    let mtype = match &value {
                        MetricValue::Counter(_) => MetricType::Counter,
                        MetricValue::Gauge(_) => MetricType::Gauge,
                        MetricValue::Histogram(_) => MetricType::Summary,
                        MetricValue::BucketedHistogram(_) => MetricType::Histogram,
                        MetricValue::Info(_) => MetricType::Info,
                        MetricValue::StateSet(_) => MetricType::StateSet,
                    };
                    out.insert_metric(family, mtype, metric_labels, value, now);
                }
            }
        }
    }
}

/// A running activity.
pub struct Activity {
    pub config: ActivityConfig,
    pub labels: Labels,
    pub metrics: Arc<ActivityMetrics>,
    pub op_sequence: OpSequence,
    pub error_router: ErrorRouter,
    /// Source factory — creates per-fiber readers. All phases go through
    /// sources. `cycles: N` desugars to `range(0, N)`.
    source_factory: Arc<dyn nbrs_variates::source::DataSourceFactory>,
    /// Resolved workload parameters (constant per run).
    pub workload_params: Arc<std::collections::HashMap<String, String>>,
    /// Shared flag: set to true when a `stop` error handler fires.
    /// All fibers check this and exit their loop when set.
    pub stop_flag: Arc<std::sync::atomic::AtomicBool>,
    /// First error message that triggered `stop_flag` — captured
    /// once (the first stopping error wins, subsequent fibers'
    /// errors don't overwrite). Surfaced in the phase-level
    /// error so the user doesn't have to grep the per-cycle
    /// log to learn what actually stopped the run.
    pub stop_reason: Arc<std::sync::Mutex<Option<String>>>,
    /// Final validation metrics frame, populated after all cycles complete.
    /// Read by the metrics capture thread after the activity finishes.
    pub validation_frame: Arc<std::sync::Mutex<Option<MetricSet>>>,
    /// Optional handle to this activity's component in the session tree.
    /// Set by the runner via [`Self::attach_component`] before
    /// execution; when present, the executor declares the
    /// `concurrency` control on it (SRD 23) and wires the
    /// [`crate::fiber_pool::ConcurrencyApplier`] so runtime writes
    /// resize the fiber pool.
    pub component: Option<Arc<std::sync::RwLock<nbrs_metrics::component::Component>>>,
    /// SRD-32a Push 3 — workload-root wrapper-composition
    /// override. When populated (from the workload's
    /// `wrappers: { order: [...] }` block), every op
    /// template that doesn't carry its own
    /// per-template override uses this innermost-to-outermost
    /// list as its composition order. Validated against the
    /// per-op triggered set at cascade time; mismatch is a
    /// hard error per SRD-32a §"Workload-level override".
    pub wrappers_override: Option<Vec<String>>,
    /// SRD-32a Push 3 — CLI `--wrap-default-order` override.
    /// Replaces the resolver's built-in `DEFAULT_ORDER`
    /// tiebreaker for this activity. `None` ⇒ resolver uses
    /// the built-in order. Distinct from
    /// `wrappers_override`: that pins the per-op stack;
    /// this changes the tiebreaker used when constraints
    /// leave order ambiguous.
    pub wrap_default_order: Option<Vec<String>>,
}

/// Invoke [`DriverAdapter::declare_controls`] for each unique adapter
/// instance against the given parent component, deduping by
/// `Arc`-pointer identity. The same adapter `Arc` may be entered
/// into the map under multiple alias keys; this guarantees each
/// physical instance gets exactly one declaration call per
/// invocation of this helper.
///
/// Called from two sites:
///
/// 1. The phase executor at component-attach time, so
///    `dryrun=controls` walks a populated tree before any
///    cycles run.
/// 2. [`Activity::run_with_adapters`] at run start, so adapters
///    that only ever materialize at run time still get declared.
///
/// Adapter implementations are expected to be idempotent — calling
/// this helper twice against the same parent must not produce
/// duplicate subcomponents or duplicate-name control declarations.
pub fn declare_adapter_controls(
    adapters: &std::collections::HashMap<String, Arc<dyn DriverAdapter>>,
    component: &Arc<std::sync::RwLock<nbrs_metrics::component::Component>>,
) {
    let mut seen: Vec<*const dyn DriverAdapter> = Vec::new();
    for adapter in adapters.values() {
        let ptr = Arc::as_ptr(adapter);
        if seen.contains(&ptr) { continue; }
        seen.push(ptr);
        adapter.declare_controls(component);
    }
}

impl Activity {
    pub fn new(
        config: ActivityConfig,
        parent_labels: &Labels,
        op_sequence: OpSequence,
    ) -> Self {
        Self::with_params(config, parent_labels, op_sequence, std::collections::HashMap::new())
    }

    pub fn with_params(
        config: ActivityConfig,
        parent_labels: &Labels,
        op_sequence: OpSequence,
        params: std::collections::HashMap<String, String>,
    ) -> Self {
        Self::with_params_and_sigdigs(
            config, parent_labels, op_sequence, params,
            nbrs_metrics::instruments::histogram::DEFAULT_HDR_SIGDIGS,
        )
    }

    /// Build an activity with explicit HDR significant-digits
    /// precision. Used by the runner after it resolves
    /// `hdr.sigdigs` from the session root (SRD 40); every
    /// histogram the activity owns is constructed at this
    /// precision. Callers that don't resolve from a tree can
    /// use [`Self::with_params`] which defaults to
    /// [`nbrs_metrics::instruments::histogram::DEFAULT_HDR_SIGDIGS`].
    pub fn with_params_and_sigdigs(
        config: ActivityConfig,
        parent_labels: &Labels,
        op_sequence: OpSequence,
        params: std::collections::HashMap<String, String>,
        sigdigs: u8,
    ) -> Self {
        let labels = parent_labels.clone();
        let metrics = Arc::new(ActivityMetrics::with_sigdigs(&labels, sigdigs));
        let error_router = ErrorRouter::parse(&config.error_spec)
            .unwrap_or_else(|e| {
                crate::diag!(crate::observer::LogLevel::Warn, "warning: invalid error spec '{}': {e}; using default (warn,stop)", config.error_spec);
                ErrorRouter::default_stop()
            });
        // All phases go through sources. cycles: N desugars to range(0, N).
        // Named cursors in GK provide their own factory via config.source_factory.
        let source_factory: Arc<dyn nbrs_variates::source::DataSourceFactory> = config.source_factory
            .clone()
            .unwrap_or_else(|| Arc::new(
                nbrs_variates::source::RangeSourceFactory::named("cycles", 0, config.cycles)
            ));

        Self {
            config,
            labels,
            metrics,
            op_sequence,
            error_router,
            source_factory,
            workload_params: Arc::new(params),
            stop_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stop_reason: Arc::new(std::sync::Mutex::new(None)),
            validation_frame: Arc::new(std::sync::Mutex::new(None)),
            component: None,
            wrappers_override: None,
            wrap_default_order: None,
        }
    }

    /// SRD-32a Push 3 — set the workload-root wrapper-
    /// composition override on this activity. Pass `None` to
    /// clear; pass `Some(order)` to install. The order list
    /// is innermost-to-outermost; per-op `wrappers:` blocks
    /// shadow this entry entirely.
    pub fn set_wrappers_override(&mut self, order: Option<Vec<String>>) {
        self.wrappers_override = order;
    }

    /// SRD-32a Push 3 — set the resolver's default-order
    /// tiebreaker for this activity (CLI
    /// `--wrap-default-order`). `None` ⇒ the resolver uses
    /// its built-in `DEFAULT_ORDER` list.
    pub fn set_wrap_default_order(&mut self, order: Option<Vec<String>>) {
        self.wrap_default_order = order;
    }

    /// Attach this activity to its component in the session tree.
    /// The runner creates the component and installs it here so
    /// `run_with_*` can register appliers on the activity's
    /// declared controls.
    ///
    /// Structural control declarations happen here — not at run
    /// time — so `dryrun=controls` (and every other pre-execution
    /// discovery path) sees the activity's controls without
    /// needing to start any cycles. Appliers that depend on
    /// run-time state (the fiber pool, the rate limiter) are
    /// registered later in `run_with_adapters`.
    pub fn attach_component(
        &mut self,
        component: Arc<std::sync::RwLock<nbrs_metrics::component::Component>>,
    ) {
        use nbrs_metrics::controls::{BranchScope, ControlBuilder};
        let initial = self.config.concurrency as u32;
        let concurrency_control: nbrs_metrics::controls::Control<u32> =
            ControlBuilder::new("concurrency", initial)
                .reify_as_gauge(|v| Some(*v as f64))
                .from_f64(|v| {
                    if v < 1.0 || v > 100_000.0 {
                        Err(format!("concurrency out of range: {v}"))
                    } else {
                        Ok(v as u32)
                    }
                })
                .branch_scope(BranchScope::Local)
                .build();
        component.read().unwrap_or_else(|e| e.into_inner())
            .controls().declare(concurrency_control);

        // Declare a `rate` control whenever the activity config
        // has a rate set. The control's reified gauge projects
        // ops/sec so metric sinks and the f64-writable surface
        // (TUI `e` prompt, web POST, GK `control_set`) all read
        // and write in the same unit. The [`RateLimiterApplier`]
        // gets registered at run time once the limiter exists
        // (see `run_with_adapters`).
        if let Some(rate) = self.config.rate {
            let rate_control: nbrs_metrics::controls::Control<nbrs_rate::RateSpec> =
                ControlBuilder::new("rate", nbrs_rate::RateSpec::new(rate))
                    .reify_as_gauge(|spec: &nbrs_rate::RateSpec| Some(spec.ops_per_sec))
                    .from_f64(|v| {
                        if v <= 0.0 {
                            Err(format!("rate must be > 0, got {v}"))
                        } else {
                            Ok(nbrs_rate::RateSpec::new(v))
                        }
                    })
                    .branch_scope(BranchScope::Local)
                    .build();
            component.read().unwrap_or_else(|e| e.into_inner())
                .controls().declare(rate_control);
        }
        // Register every static instrument owned by ActivityMetrics
        // on this component so the cadence reporter's tree walk
        // sees them. Failures here are programming errors
        // (duplicate family on the activity's own component) —
        // panic so the issue surfaces during init.
        {
            let mut guard = component.write().unwrap_or_else(|e| e.into_inner());
            self.metrics.register_on(&mut guard)
                .expect("ActivityMetrics::register_on failed on a fresh activity component");
        }
        self.component = Some(component);
    }

    /// Get a shared reference to the metrics for external capture.
    pub fn shared_metrics(&self) -> Arc<ActivityMetrics> {
        self.metrics.clone()
    }

    /// Run the activity with a single adapter for all ops.
    pub async fn run_with_driver(
        self,
        adapter: Arc<dyn DriverAdapter>,
        op_builder: Arc<crate::synthesis::OpBuilder>,
    ) -> bool {
        let mut adapters = std::collections::HashMap::new();
        let name = adapter.name().to_string();
        adapters.insert(name.clone(), adapter);
        self.run_with_adapters(adapters, &name, op_builder).await
    }

    /// Run the activity with multiple adapters (SRD 38/40).
    ///
    /// Each op template's `adapter` param selects which adapter to use.
    /// Templates without an explicit adapter use `default_adapter`.
    /// At init time: maps each template to a dispenser from the
    /// appropriate adapter. Per fiber: creates a FiberBuilder. Per
    /// cycle: resolves fields via GK, executes via dispenser.
    /// Returns true if the activity was stopped by an error handler.
    pub async fn run_with_adapters(
        self,
        adapters: std::collections::HashMap<String, Arc<dyn DriverAdapter>>,
        default_adapter: &str,
        op_builder: Arc<crate::synthesis::OpBuilder>,
    ) -> bool {
        let activity = Arc::new(self);
        let program = op_builder.program();

        // Init time: map each template to a dispenser from its adapter,
        // then wrap with result traverser for consumption/capture
        let templates = activity.op_sequence.templates();

        // Validate all bind points are resolvable before execution
        if let Err(e) = crate::synthesis::validate_bind_points(templates, &program) {
            crate::diag!(crate::observer::LogLevel::Error, "error: {e}");
            return true;
        }

        // Adapter-level dynamic controls (SRD 23). The phase
        // executor already declared adapter controls at attach
        // time so `dryrun=controls` saw them; calling again here
        // is the safety net for adapters that materialize only
        // at run time. Adapter `declare_controls` impls are
        // contractually idempotent — see `declare_adapter_controls`.
        if let Some(component) = activity.component.as_ref() {
            declare_adapter_controls(&adapters, component);
        }

        let traversal_stats = Arc::new(crate::wrappers::TraversalStats {
            metrics: activity.metrics.clone(),
        });

        // SRD-32a — wrapper registry + resolver. The
        // registry is fixed at link time (every `inventory::
        // submit!` block in the binary contributes one
        // entry); the resolver carries the validated
        // default-order tiebreaker. Both are built once
        // here and reused for every op template in this
        // activity.
        let wrapper_registry =
            crate::wrapper_registry::WrapperRegistry::from_inventory();
        // SRD-32a Push 3 — CLI `--wrap-default-order` replaces
        // the resolver's built-in tiebreaker. When unset, the
        // resolver builds with its DEFAULT_ORDER. The CLI list
        // is validated against the constraint graph at
        // construction; an inconsistent list aborts the run.
        let wrapper_resolver = match &activity.wrap_default_order {
            Some(order) => {
                let names: Vec<&str> = order.iter().map(|s| s.as_str()).collect();
                crate::wrapper_resolver::WrapperResolver::from_names(
                    &names, &wrapper_registry,
                )
            }
            None => crate::wrapper_resolver::WrapperResolver
                ::with_default_order(&wrapper_registry),
        };
        let wrapper_resolver = match wrapper_resolver {
            Ok(r) => r,
            Err(e) => {
                crate::diag!(crate::observer::LogLevel::Error,
                    "error: wrapper default-order is inconsistent with the \
                     registered wrapper graph: {e}. CLI `--wrap-default-order` \
                     and the built-in default both must satisfy every \
                     registered constraint.");
                return true;
            }
        };

        let mut dispensers: Vec<Arc<dyn OpDispenser>> = Vec::new();
        let mut validation_metrics: Vec<Arc<validation::ValidationMetrics>> = Vec::new();
        // SRD-40b §6/§7 — one `Component` per **op dispenser**
        // (= per op template), not per op execution. Op
        // dispensers are the durable CNS layer of the nbrs
        // runtime; per-cycle op invocations are stack-ephemeral
        // and inherit the dispenser's component implicitly via
        // the wrapper-stack closure capture. Each component
        // carries `op=<template.name>` labels (child of the
        // activity component) so SRD-40b §7.2's duplicate-
        // family check (`Component::register_instrument`)
        // sees one dimensional cell per dispenser, surviving
        // for the run's duration. Held here to keep the Arc
        // alive.
        let mut dispenser_components: Vec<std::sync::Arc<std::sync::RwLock<
            nbrs_metrics::component::Component>>> = Vec::new();
        // Per-template wrapper pull plan. Wrapper-side reads
        // (validation, conditional, throttle) go through this
        // `PullPlan` against the firing fiber's state — see
        // SRD 31 §"Pull plan vs bind plan". Adapter-side reads
        // moved to the generic `crate::wires::WireSource` surface
        // at SRD-68 Push 5; the legacy `field_pulls` /
        // `bind_plans` / `batch_configs` lists are retired.
        let mut pull_plans_per_template: Vec<crate::fixture::PullPlan> = Vec::new();
        for template in templates {
            // Resolve adapter: per-template override or default
            let adapter_name = template.params.get("adapter")
                .and_then(|v| v.as_str())
                .or_else(|| template.params.get("driver").and_then(|v| v.as_str()))
                .unwrap_or(default_adapter);
            let adapter = match adapters.get(adapter_name) {
                Some(a) => a,
                None => {
                    let available = adapters.keys().cloned().collect::<Vec<_>>().join(", ");
                    crate::diag!(crate::observer::LogLevel::Error, "error: unknown adapter '{adapter_name}' for op '{}' (available: {available})", template.name);
                    return true; // signal stop — cannot proceed without the adapter
                }
            };

            if template.params.contains_key("batch") {
                crate::diag!(crate::observer::LogLevel::Debug, "[activity] op '{}' has batch param: {:?}", template.name, template.params.get("batch"));
            }
            // SRD 30 §"Core-first field processing": if the adapter
            // declares its known op fields, every key in
            // `template.op` must be one of them. Core has already
            // stripped its own fields during parse (activity_params
            // in nbrs-workload), so anything left is an adapter
            // concern. Unknown fields are a typo or a misplaced
            // core directive — fail loudly rather than silently
            // dropping the field.
            if let Some(known) = adapter.known_op_fields() {
                let unknown: Vec<&String> = template.op.keys()
                    .filter(|k| !known.contains(&k.as_str()))
                    .collect();
                if !unknown.is_empty() {
                    let list = unknown.iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    crate::diag!(crate::observer::LogLevel::Error,
                        "error: adapter '{}' does not recognize op fields [{list}] on op '{}'; known fields: [{}]",
                        adapter.name(),
                        template.name,
                        known.join(", "),
                    );
                    return true; // stop — misconfiguration
                }
            }

            // Same idea for `template.params`: validate against
            // a closed vocabulary so silent-ignore traps like
            // `evaluations: { relevancy: ... }` (wrapper keys
            // the runtime never reads) cannot hide a
            // misconfigured op. Allowed keys are the union of:
            //   1. core op-level params consumed by the runtime
            //      (validation, batching, polling, weighting,
            //      adapter selection) — `CORE_OP_PARAMS`.
            //   2. workload/CLI-level params that the parser
            //      blast-merges into every op's params at parse
            //      time — `runner::KNOWN_PARAMS`.
            //   3. user-declared workload params from the
            //      workload's top-level `params:` block (e.g.
            //      `table`, `keyspace`, `num_items`). The parser
            //      threads these into every op's params during
            //      doc → block → op merge, where they're meant
            //      for `{name}` interpolation in op templates.
            //      Visible here as `activity.workload_params`.
            //   4. adapter-specific params declared via
            //      `DriverAdapter::known_op_params()`.
            // Anything else is a typo / misplaced wrapper / dead
            // YAML and is rejected.
            {
                let allowed_extras = adapter.known_op_params();
                let workload_keys = &activity.workload_params;
                let unknown_params: Vec<&String> = template.params.keys()
                    .filter(|k| {
                        !crate::validation::CORE_OP_PARAMS.contains(&k.as_str())
                            && !crate::runner::KNOWN_PARAMS.contains(&k.as_str())
                            && !allowed_extras.contains(&k.as_str())
                            && !workload_keys.contains_key(k.as_str())
                    })
                    .collect();
                if !unknown_params.is_empty() {
                    let list = unknown_params.iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    crate::diag!(crate::observer::LogLevel::Error,
                        "error: op '{}' has unknown params keys [{list}] — \
                         not in the core vocabulary, not declared by \
                         adapter '{}', and not declared as a workload-level \
                         param. Known core op params: [{}]. Adapter \
                         extras: [{}]. Did you mean to put this under a \
                         declared param, or did you misspell `relevancy:` \
                         / `verify:` / nest it under a wrapper key the \
                         runtime doesn't read?",
                        template.name,
                        adapter.name(),
                        crate::validation::CORE_OP_PARAMS.join(", "),
                        allowed_extras.join(", "),
                    );
                    return true; // stop — misconfiguration
                }
            }

            // SRD-32a Push 2 — field ownership and misplaced-
            // field guard. The wrapper registry knows which
            // `params:` keys each wrapper consumes
            // (`owned_fields`) and what makes the wrapper
            // trigger. A field that's owned by a wrapper that
            // ISN'T triggered is misplaced — silently ignoring
            // it would mask a typo or a half-applied
            // configuration. Example: `poll_interval_ms: 5000`
            // on an op without `poll:` is a misconfiguration —
            // the operator probably meant to enable polling
            // but forgot the trigger.
            //
            // The closed-vocabulary check above catches "I
            // don't recognise this key at all"; THIS check
            // catches "I recognise the key but it has no effect
            // here." Both surface as hard errors.
            //
            // Note: we cross-check against `template.params`
            // for keys. The registry's owned_fields includes
            // a few names that live elsewhere on `ParsedOp`
            // (`if` → `template.condition`, `delay` →
            // `template.delay`); those happen to BE their
            // wrapper's trigger field too, so when set the
            // outer `(reg.triggers)(template)` short-circuits
            // and we never reach the params-key check for
            // them. The set is small enough that we don't
            // need a separate "where does this field live"
            // helper.
            {
                let violations = wrapper_registry.misplaced_fields(
                    template,
                    |field| template.params.contains_key(field),
                );
                if !violations.is_empty() {
                    for (wrapper, field) in &violations {
                        crate::diag!(crate::observer::LogLevel::Error,
                            "error: op '{}': field `{field}` is owned by wrapper \
                             `{wrapper}`, but the trigger condition for `{wrapper}` \
                             is not satisfied (no trigger field set on this op). \
                             Either remove `{field}` or add the wrapper's trigger \
                             field. SRD-32a §\"Field ownership and parse-time \
                             validation\".",
                            template.name);
                    }
                    return true; // stop — misconfiguration
                }
            }

            match adapter.map_op(template, op_builder.canonical_kernel_for_op(&template.name)) {
                Ok(d) => {
                    let raw = Arc::from(d);

                    // Open the per-template scope fixture (SRD 32
                    // §"Init-Time Fixture and Consumer Self-
                    // Registration"). Each wrapper below registers
                    // its own GK name dependencies; the fixture is
                    // sealed after the wrapper chain is complete and
                    // the resulting PullPlan drives cycle-time reads.
                    //
                    // SRD-13d Phase 9 — when this op-template
                    // materialised its own kernel, the fixture
                    // builds its plan against THAT program so
                    // pulls resolve in the op-template scope.
                    // Flattened op-templates fall back to the
                    // activity-wide program (same scope as before
                    // Phase 9 landed).
                    let template_program = op_builder.program_for_op(&template.name);
                    let mut fx = crate::fixture::ScopeFixture::new(template_program.clone());

                    // SRD-32a — resolve which wrappers fire and
                    // in what order. The plan is innermost-first;
                    // `traverse` is always inner. The cascade
                    // below dispatches to the existing per-
                    // wrapper `wrap()` factory based on the plan
                    // entries' names. Plan order matches the
                    // built-in default order, which mirrors the
                    // pre-SRD-32a hand-rolled cascade — existing
                    // tests exercise the same composition.
                    //
                    // SRD-32a Push 3 — override precedence:
                    //   1. Per-op `template.wrappers.order` shadows everything else.
                    //   2. Else workload-root `activity.wrappers_override`.
                    //   3. Else the resolver's default-order tiebreaker.
                    // Per-op shadows root entirely (no merge).
                    let per_op_override = template.wrappers.as_ref()
                        .filter(|c| !c.order.is_empty())
                        .map(|c| c.order.clone());
                    let effective_override = per_op_override
                        .or_else(|| activity.wrappers_override.clone());
                    let plan = match effective_override {
                        Some(order) => {
                            let order_strs: Vec<&str> = order.iter()
                                .map(|s| s.as_str()).collect();
                            wrapper_resolver.resolve_with_order(
                                template, &wrapper_registry, &order_strs)
                        }
                        None => wrapper_resolver.resolve(template, &wrapper_registry),
                    };
                    let plan = match plan {
                        Ok(p) => p,
                        Err(e) => {
                            crate::diag!(crate::observer::LogLevel::Error,
                                "error: op '{}': wrapper resolution failed: {e}",
                                template.name);
                            return true;
                        }
                    };

                    // SRD-32a §"Composition telemetry" — emit one
                    // Info-level line per assigned wrapper so
                    // operators can see, at session start, exactly
                    // which wrappers shape each op and how. Trivial
                    // wrappers (e.g. always-on `traverse`) return
                    // `None` from `describe_assignment` and are
                    // dropped from this list.
                    let assignments: Vec<(crate::wrapper_registry::WrapperName, String)> = plan
                        .iter_innermost_first()
                        .filter_map(|reg| {
                            (reg.describe_assignment)(template).map(|s| (reg.name, s))
                        })
                        .collect();
                    if !assignments.is_empty() {
                        crate::diag!(crate::observer::LogLevel::Info,
                            "op '{}' wrappers (innermost → outermost):", template.name);
                        for (i, (_, line)) in assignments.iter().enumerate() {
                            crate::diag!(crate::observer::LogLevel::Info,
                                "  {}. {}", i + 1, line);
                        }
                    }

                    // Wrap with traversal (innermost). Traversal
                    // does not read GK values; no fixture
                    // registration needed. Always present per
                    // the registry's always-true trigger.
                    let mut current: Arc<dyn OpDispenser> = crate::wrappers::TraversingDispenser::wrap(
                        raw, template, traversal_stats.clone(),
                    );

                    // Apply each remaining wrapper in plan order.
                    // Skip `traverse`; it's already constructed.
                    for reg in plan.iter_innermost_first() {
                        if reg.name == crate::wrapper_registrations::TRAVERSE { continue; }
                        let stop = match reg.name {
                            crate::wrapper_registrations::THROTTLE => {
                                let raw_name = template.delay.as_deref()
                                    .expect("throttle triggered → delay set");
                                let name = raw_name.trim()
                                    .strip_prefix('{')
                                    .and_then(|s| s.strip_suffix('}'))
                                    .unwrap_or(raw_name.trim());
                                match crate::wrappers::ThrottleDispenser::wrap(current.clone(), name, &mut fx) {
                                    Ok(d) => { current = d; false }
                                    Err(e) => {
                                        crate::diag!(crate::observer::LogLevel::Error,
                                            "error: op '{}': {e}", template.name);
                                        true
                                    }
                                }
                            }
                            crate::wrapper_registrations::VALIDATE => {
                                match crate::validation::ValidatingDispenser::wrap(
                                    current.clone(), template, &activity.labels, Some(&program), &mut fx,
                                ) {
                                    Ok((d, vm)) => {
                                        if let Some(vm) = vm { validation_metrics.push(vm); }
                                        current = d;
                                        false
                                    }
                                    Err(e) => {
                                        crate::diag!(crate::observer::LogLevel::Error,
                                            "error: op '{}': {e}", template.name);
                                        true
                                    }
                                }
                            }
                            crate::wrapper_registrations::POLL => {
                                let interval = template.params.get("poll_interval_ms")
                                    .and_then(|v| v.as_str().and_then(|s| s.parse().ok())
                                        .or_else(|| v.as_u64()))
                                    .unwrap_or(1000);
                                let timeout = template.params.get("timeout_ms")
                                    .and_then(|v| v.as_str().and_then(|s| s.parse().ok())
                                        .or_else(|| v.as_u64()))
                                    .unwrap_or(300_000);
                                // SRD-03 §"Status-Determination
                                // Invariant — Retries Within": bounded
                                // retry budget for retryable inner
                                // errors. Default 0 (strict). Operators
                                // raise this when long fixture readiness
                                // checks tolerate transient blips.
                                let max_error_retries = template.params.get("poll_max_error_retries")
                                    .and_then(|v| v.as_str().and_then(|s| s.parse::<u32>().ok())
                                        .or_else(|| v.as_u64().map(|n| n as u32)))
                                    .unwrap_or(0);
                                let metric_name = template.params.get("poll_metric_name")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                let (d, _pm) = crate::wrappers::PollingDispenser::wrap(
                                    current.clone(), interval, timeout, max_error_retries, metric_name,
                                );
                                crate::diag!(crate::observer::LogLevel::Debug,
                                    "  op '{}': polling enabled (interval={}ms, timeout={}ms, max_error_retries={})",
                                    template.name, interval, timeout, max_error_retries);
                                current = d;
                                false
                            }
                            crate::wrapper_registrations::IF_COND => {
                                // `if:` short-circuits before the
                                // inner cascade — load-bearing for
                                // the recent fix that pulls polling
                                // inside `if`. Resolver order
                                // mirrors that.
                                let cond = template.condition.as_deref()
                                    .expect("if triggered → condition set");
                                let cond_name = cond.trim()
                                    .strip_prefix('{')
                                    .and_then(|s| s.strip_suffix('}'))
                                    .unwrap_or(cond.trim());
                                match crate::wrappers::ConditionalDispenser::wrap(
                                    current.clone(), cond_name, activity.metrics.clone(), &mut fx,
                                ) {
                                    Ok(d) => { current = d; false }
                                    Err(e) => {
                                        crate::diag!(crate::observer::LogLevel::Error,
                                            "error: op '{}': {e}", template.name);
                                        true
                                    }
                                }
                            }
                            crate::wrapper_registrations::EMIT => {
                                current = crate::wrappers::EmitDispenser::wrap(current.clone(), &template.name);
                                false
                            }
                            crate::wrapper_registrations::RESULT => {
                                // SRD-40b §5: result-as-GK adapter —
                                // exposes captured result fields to
                                // the op's GK scope via
                                // `OpResult.captures` so metric
                                // expressions (and any later wrappers)
                                // can reference them by name. No-op
                                // when the op declares no `result:`
                                // wires.
                                current = crate::wrappers::ResultDispenser::wrap(current.clone(), template.result.as_ref());
                                false
                            }
                            crate::wrapper_registrations::METRICS => {
                                // SRD-40b §6/§7 — one `Component`
                                // per dispenser carrying
                                // `op=<template.name>` so the
                                // duplicate-family check sees one
                                // dimensional cell per dispenser
                                // and child ops collide cleanly on
                                // their `op=` label.
                                let labels = nbrs_metrics::labels::Labels::of("op", &template.name);
                                let dispenser_component = std::sync::Arc::new(std::sync::RwLock::new(
                                    nbrs_metrics::component::Component::new(
                                        labels, std::collections::HashMap::new(),
                                    )
                                ));
                                if let Some(parent) = activity.component.as_ref() {
                                    nbrs_metrics::component::attach(parent, &dispenser_component);
                                }
                                let wrap_result = {
                                    let mut guard = dispenser_component.write()
                                        .unwrap_or_else(|e| e.into_inner());
                                    crate::wrappers::MetricsDispenser::wrap(
                                        current.clone(), &template.metrics, &mut guard, &mut fx,
                                    )
                                };
                                match wrap_result {
                                    Ok(d) => {
                                        // Mark the dispenser
                                        // component Running so the
                                        // cadence reporter's
                                        // `capture_tree` walk visits
                                        // it on every tick.
                                        dispenser_component
                                            .write()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .set_state(nbrs_metrics::component::ComponentState::Running);
                                        dispenser_components.push(dispenser_component);
                                        current = d;
                                        false
                                    }
                                    Err(e) => {
                                        crate::diag!(crate::observer::LogLevel::Error,
                                            "error: op '{}': {e}", template.name);
                                        true
                                    }
                                }
                            }
                            other => {
                                crate::diag!(crate::observer::LogLevel::Error,
                                    "error: op '{}': resolver returned wrapper `{}` \
                                     with no dispatch handler in the cascade",
                                    template.name, other);
                                true
                            }
                        };
                        if stop { return true; }
                    }

                    dispensers.push(current);

                    // Seal the per-template fixture. The PullPlan
                    // drives cycle-time reads for every wrapper that
                    // registered (validation ground truth, conditional
                    // `if`, throttle `delay`). See SRD 31 §"Pull plan
                    // vs bind plan".
                    pull_plans_per_template.push(fx.seal());
                }
                Err(e) => {
                    crate::diag!(crate::observer::LogLevel::Error, "error: adapter.map_op failed for '{}': {e}", template.name);
                    return true;
                }
            }
        }
        let dispensers = Arc::new(dispensers);
        // Register dispensers for adapter-specific metrics capture
        activity.metrics.set_dispensers(dispensers.clone());
        let pull_plans_per_template = Arc::new(pull_plans_per_template);
        let validation_metrics = Arc::new(validation_metrics);
        // Share the validation-metrics handle with ActivityMetrics so
        // the progress thread (below) can read live relevancy aggregates.
        activity.metrics.set_validation_metrics(validation_metrics.clone());

        // Single activity-level rate limiter. One ops-per-sec
        // ceiling gates every fiber; there is no separate
        // stanza-rate mechanism. Activities with no `rate`
        // configured skip construction cleanly.
        let rate_limiter = activity.config.rate.map(|r| {
            Arc::new(RateLimiter::start(nbrs_rate::RateSpec::new(r)))
        });

        // Register the [`RateLimiterApplier`] against the
        // already-declared `rate` control if both the control
        // and the limiter exist. The declaration happens in
        // [`Self::attach_component`] — this step only wires the
        // applier so a runtime write actually reconfigures the
        // running limiter.
        if let (Some(ref ac), Some(ref rl)) = (
            activity.component.as_ref(), rate_limiter.as_ref(),
        ) {
            let existing: Option<nbrs_metrics::controls::Control<nbrs_rate::RateSpec>> =
                ac.read().unwrap_or_else(|e| e.into_inner())
                    .controls().get("rate");
            if let Some(ctl) = existing {
                ctl.register_applier(
                    nbrs_rate::RateLimiterApplier::new(Arc::clone(rl)),
                );
            }
        }

        // Spawn a progress reporter thread that prints cycle count to stderr
        // every 500 ms when stderr is a TTY and cycle count is large enough
        // to be worth reporting. The flag is cleared after all executor
        // fibers finish so the thread terminates and clears its line.
        let progress_flag = Arc::new(AtomicBool::new(true));
        let activity_name = activity.config.name.clone();
        let is_stderr_tty = std::io::IsTerminal::is_terminal(&std::io::stderr());
        let suppress_progress = adapters.values()
            .any(|a| a.name() == "plotter");
        let start_time = Instant::now();
        // Use source extent for progress (data-driven), not cycles
        let source_for_progress = activity.source_factory.clone();
        let total_extent = source_for_progress.global_extent().unwrap_or(activity.config.cycles);
        let cursor_name = {
            let name = &source_for_progress.schema().name;
            format!(" cursor={name}")
        };
        if is_stderr_tty && total_extent > 1000 && !suppress_progress {
            // Spawn the inline progress thread unconditionally
            // (subject to the TTY / extent / adapter guards).
            // The thread gates each emission tick on the live
            // `suppress_status_line` flag, so a TUI dismissal
            // mid-run (`q` keypress) flips emission back on
            // automatically — same UX as if tui=off had been
            // set from the start.
            let flag = progress_flag.clone();
            let suppress_flag = activity.config.suppress_status_line.clone();
            let progress_metrics = activity.metrics.clone();
            let start_time = start_time;
            let activity_name_progress = activity_name.clone();
            let cursor_name_progress = cursor_name.clone();
            let activity_concurrency = activity.config.concurrency;
            let status_metrics = activity.config.status_metrics.clone();
            // Build the on_update binder once at spawn time:
            // workload's `on_update:` overrides if any, else
            // the default `phase_status` body. This is the
            // SRD-63 §7 binding layer landing — every refresh
            // tick fires through the binder rather than
            // calling the readout directly.
            let phase_status_default = {
                let readout = crate::readouts::Registry::lookup("phase_status")
                    .expect("phase_status registered");
                crate::readouts::BakedBody::from_single(
                    readout, crate::readouts::Lod::Labeled,
                )
            };
            let update_binder = match crate::readouts::binder::build_event_binder_with_cli(
                &activity.config.readouts,
                crate::readouts::Event::Update,
                phase_status_default,
                activity.config.cli_readout_override.as_deref(),
            ) {
                Ok(b) => b,
                Err(e) => {
                    crate::diag!(crate::observer::LogLevel::Error,
                        "readouts: failed to bind on_update — {e}");
                    return false;
                }
            };
            let snapshot_writer_for_thread = activity.config.snapshot_writer.clone();
            std::thread::spawn(move || {
                let activity_name = activity_name_progress;
                let cursor_name = cursor_name_progress;
                let snapshot_writer = snapshot_writer_for_thread;
                let mut tick: u64 = 0;
                let mut binder = update_binder;
                while flag.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(500));
                    if !flag.load(Ordering::Relaxed) { break; }
                    tick = tick.wrapping_add(1);
                    if suppress_flag.load(Ordering::Relaxed) {
                        // TUI is currently displaying — don't
                        // overwrite its alternate-screen
                        // rendering with a stderr status line.
                        // Re-check next tick.
                        continue;
                    }
                    // Snapshot the per-tick context, hand it
                    // to the binder; the binder walks its
                    // resolved bodies in declaration order
                    // and writes into the StringSink.
                    let ctx = crate::readout_context::build_inline_refresh_context(
                        &progress_metrics,
                        &activity_name,
                        activity_concurrency,
                        total_extent,
                        start_time.elapsed().as_secs_f64(),
                        tick,
                        &status_metrics,
                    );
                    use crate::readouts::ReadoutBinder;
                    let mut sink = crate::readouts::StringSink::with_capacity(192);
                    binder.fire(crate::readouts::Event::Update, &ctx, &mut sink);
                    let rendered = sink.take();
                    // Push 6: capture the latest on_update
                    // render to the snapshot store. PK collapses
                    // ticks to the most recent — the inline
                    // thread fires often, but only the last
                    // render survives in `readout_snapshots`.
                    use crate::readouts::ReadoutContext;
                    crate::readouts::snapshot::capture(
                        snapshot_writer.as_ref(),
                        crate::readouts::Event::Update.slot_name(),
                        crate::readouts::Event::Update.subject_kind().as_str(),
                        &ctx.subject_id(),
                        "binder",
                        crate::readouts::snapshot::lod_str(crate::readouts::Lod::Labeled),
                        &rendered,
                    );
                    let cols = terminal_cols().unwrap_or(200);
                    let truncated = truncate_to_width(&rendered, cols.saturating_sub(1));
                    eprint!("\r\x1b[K{truncated}");
                    let _ = cursor_name; // retained for log-file detail; status line stays compact
                }
            });
        }

        // One Arc<str> shared by every fiber in this phase. The
        // GK runtime-context `phase()` node clones this per read
        // instead of per fiber, keeping the per-cycle cost O(1).
        let phase_name_arc: Arc<str> = Arc::from(activity_name.as_str());

        // SRD 23 §"Fiber executor": fiber lifecycle goes through
        // a [`FiberPool`] that the `ConcurrencyApplier` can
        // resize via the activity's `concurrency` control. Each
        // fiber receives its own stop-flag and exits
        // cooperatively at the next cycle boundary when flagged.
        let pool_spawner: crate::fiber_pool::FiberSpawner = {
            let activity = activity.clone();
            let dispensers_outer = dispensers.clone();
            let pull_plans_outer = pull_plans_per_template.clone();
            let op_builder_outer = op_builder.clone();
            let rate_limiter_outer = rate_limiter.clone();
            let phase_arc_outer = phase_name_arc.clone();
            Box::new(move |stop: crate::fiber_pool::StopFlag| {
                let activity = activity.clone();
                let dispensers = dispensers_outer.clone();
                let pull_plans = pull_plans_outer.clone();
                let op_builder = op_builder_outer.clone();
                let rate_limiter = rate_limiter_outer.clone();
                let phase_arc = phase_arc_outer.clone();
                tokio::spawn(async move {
                    // Catch panics inside the fiber so they surface
                    // in diagnostics rather than silently terminating
                    // the task. Without this, a panic in any cycle's
                    // accessor / binder code would leave the fiber
                    // gone and the run still "active" from the
                    // perspective of the executor, hanging the TUI
                    // with no visible cause. The session log line
                    // captures location + message; the runtime's
                    // own panic reporting (if any) is unchanged.
                    use futures::FutureExt as _;
                    let activity_for_panic = activity.clone();
                    let activity_name_for_log = activity.config.name.clone();
                    let body = nbrs_variates::nodes::runtime_context::with_fiber_context(
                        phase_arc,
                        async move {
                            executor_task(
                                activity, dispensers, pull_plans,
                                op_builder, rate_limiter, stop,
                            ).await;
                        },
                    );
                    let result = std::panic::AssertUnwindSafe(body).catch_unwind().await;
                    match result {
                        Ok(()) => {
                            // Normal fiber exit is silent — the
                            // session log used to record one
                            // line per fiber here at Debug, but
                            // with concurrency=N, that's N lines
                            // per phase boundary in session.log
                            // for no diagnostic value (the phase
                            // completion + duration already
                            // tells the user the fibers
                            // completed). Panic exits below
                            // remain Error-level.
                            let _ = activity_name_for_log;
                        }
                        Err(panic_payload) => {
                        let msg = panic_payload
                            .downcast_ref::<&'static str>().map(|s| (*s).to_string())
                            .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "<non-string panic payload>".into());
                        crate::diag!(crate::observer::LogLevel::Error,
                            "fiber panic in activity '{}': {}",
                            activity_name_for_log, msg);
                            // Mark stop_flag so other fibers and the
                            // executor's main loop see that something
                            // went wrong; the run will terminate at
                            // the next coordination point rather
                            // than continuing in a half-broken state.
                            activity_for_panic.stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                })
            })
        };
        let fiber_pool = Arc::new(crate::fiber_pool::FiberPool::new(pool_spawner));

        // Register the pool's applier against the already-declared
        // `concurrency` control (see [`Self::attach_component`]).
        // At run time the applier is what turns a control write
        // into an actual fiber-pool resize. Without a component
        // attached (library-level tests that call `Activity::new`
        // directly) we skip registration — the pool still
        // operates, just without the runtime control surface.
        if let Some(ac) = activity.component.as_ref() {
            let existing: Option<nbrs_metrics::controls::Control<u32>> =
                ac.read().unwrap_or_else(|e| e.into_inner())
                    .controls().get("concurrency");
            if let Some(ctl) = existing {
                ctl.register_applier(
                    crate::fiber_pool::ConcurrencyApplier::new(fiber_pool.clone()),
                );
            }
        }

        fiber_pool.spawn_initial(activity.config.concurrency);
        // Wait for fibers to exit by natural exhaustion (source
        // drained) or `stop_flag` set by the error router.
        // Runtime resize-down flags some of them earlier; those
        // exit at the next cycle boundary and the remainder
        // drain when the source is done.
        let mut last_seen_count = activity.config.concurrency;
        let mut last_seen_cycles = activity.metrics.cycles_completed();
        let mut stuck_since = std::time::Instant::now();
        let mut last_logged_count = activity.config.concurrency;
        loop {
            fiber_pool.reap_finished();
            let n = fiber_pool.tracked_count();
            if n == 0 { break; }
            // Periodic stall detection. A real stall means
            // *neither* signal of progress has moved:
            //   - `tracked_count` only changes when a fiber
            //     exits. During steady-state rampup every fiber
            //     is alive and busy, so this stays constant
            //     even when work is flying.
            //   - `cycles_completed` increments per finished op,
            //     so it reflects actual throughput regardless of
            //     whether any fiber has exited yet.
            // Either signal moving resets the stuck timer; only
            // when both are flat for the full 30 s do we warn.
            let cycles = activity.metrics.cycles_completed();
            let count_changed = n != last_seen_count;
            let cycles_changed = cycles != last_seen_cycles;
            if count_changed || cycles_changed {
                last_seen_count = n;
                last_seen_cycles = cycles;
                stuck_since = std::time::Instant::now();
                // Log progressing-but-slow drain: every time the
                // count changes we re-emit at debug so a stuck
                // run's session.log shows the slope (or lack of it)
                // without flooding when drain is fast.
                if count_changed && (last_logged_count.saturating_sub(n) >= 10
                    || (n < 10 && n != last_logged_count))
                {
                    crate::diag!(crate::observer::LogLevel::Debug,
                        "activity '{}': fiber drain at {n} (from {last_logged_count})",
                        activity.config.name);
                    last_logged_count = n;
                }
            } else if stuck_since.elapsed() > std::time::Duration::from_secs(30) {
                crate::diag!(crate::observer::LogLevel::Warn,
                    "activity '{}': {} fibers running, {} cycles completed, \
                     no progress for 30s — likely blocked on adapter response, \
                     lock, or IO",
                    activity.config.name, n, cycles);
                stuck_since = std::time::Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        crate::diag!(crate::observer::LogLevel::Debug,
            "activity '{}': all fibers drained", activity.config.name);

        // Final completion line — always emitted (one per phase),
        // not gated on TTY/extent. Replaces the old executor-side
        // `phase 'X' complete (Ns)` line. Honors the live
        // `suppress_status_line` flag (TUI takes over rendering)
        // and the global `suppress_progress` (e.g. CI / `--quiet`).
        if !suppress_progress
            && !activity.config.suppress_status_line.load(Ordering::Relaxed)
        {
            // Counter snapshots — the readout recomputes
            // pct / rate / ok_pct from these primitives, so
            // we don't pre-format them here. Retries are
            // derived per the existing convention (errors
            // minus the skips-adjusted failed-op count).
            let consumed = activity.source_factory.global_consumed();
            let ops_completed = activity.metrics.cycles_completed();
            let successes = activity.metrics.successes_total.get();
            let errors = activity.metrics.errors_total.get();
            let elapsed = start_time.elapsed().as_secs_f64();
            let failed_ops = ops_completed.saturating_sub(successes).saturating_sub(
                activity.metrics.skips_total.get());
            let retries = errors.saturating_sub(failed_ops);
            // Concurrency (fiber count) — the `c:N` tail mirrors
            // the live progress line so a completed phase reads
            // with the same shape as a running one.
            let concurrency = activity.config.concurrency;
            // Workload-emphasized metrics — same resolver as the
            // inline progress line, glob-matched against the
            // declared `status_metrics: [...]`. Empty list ⇒ no
            // metrics tail; nothing is presumed to be present.
            let relevancy_str: String = activity.metrics
                .collect_status_values(&activity.config.status_metrics)
                .concat();
            // Clear the in-place progress line ONLY when the
            // inline progress thread was actually rendering — the
            // spawn site at line ~980 gates on
            // `is_stderr_tty && total_extent > 1000`. Without
            // those conditions there's nothing to clear and the
            // `\r\x1b[K` would just spurt control codes in
            // pipelined output.
            let inline_was_rendering = is_stderr_tty && total_extent > 1000 && !suppress_progress;
            if inline_was_rendering {
                eprint!("\r\x1b[K");
            }
            // Render the ✓ DONE line via the readout engine.
            // SRD-63 / Push 1: the previous inline `format!()`
            // is now `phase_done.render()` driven by an
            // `ActivityReadoutContext` snapshot of the values
            // gathered above. Output is byte-equivalent.
            let phase_name_bare = activity.config.name.split_once(" (")
                .map(|(n, _)| n.to_string())
                .unwrap_or_else(|| activity.config.name.clone());
            let ctx = crate::readout_context::ActivityReadoutContext {
                phase_name: phase_name_bare,
                phase_seq: activity.config.phase_seq,
                phase_labels: activity.config.phase_labels.clone(),
                cycles_completed: ops_completed,
                cycles_total: total_extent,
                ops_ok: successes,
                errors,
                retries,
                concurrency,
                elapsed_secs: elapsed,
                consumed,
                status_metric_chips: relevancy_str,
                depth_indent: crate::scene_tree::running_phase_indent(),
                use_color: crate::observer::use_color(),
            };
            // SRD-63 §6.2 / Push 9c: synthesise one final
            // `on_update` tick before the DONE summary. The
            // inline thread (if running) fires every 500 ms
            // and may have missed the last 100-499 ms of
            // counter changes — and for short phases under
            // the TTY/extent threshold it never spawned at
            // all. This guarantees the snapshot store sees
            // the phase's end-of-life on_update render
            // matching what the user would have seen if the
            // refresh tick had aligned exactly with phase
            // termination.
            //
            // Renders silently (no eprint) — the DONE line
            // immediately following carries the visible
            // ✓ summary; we just want the snapshot row to
            // reflect end-state.
            {
                let final_ctx = crate::readout_context::build_inline_refresh_context(
                    &activity.metrics,
                    &activity.config.name,
                    activity.config.concurrency,
                    total_extent,
                    elapsed,
                    u64::MAX,  // sentinel: spinner frame doesn't matter at end-of-phase
                    &activity.config.status_metrics,
                );
                let phase_status_default = {
                    let readout = crate::readouts::Registry::lookup("phase_status")
                        .expect("phase_status registered");
                    crate::readouts::BakedBody::from_single(
                        readout, crate::readouts::Lod::Labeled,
                    )
                };
                if let Ok(mut binder) = crate::readouts::binder::build_event_binder_with_cli(
                    &activity.config.readouts,
                    crate::readouts::Event::Update,
                    phase_status_default,
                    activity.config.cli_readout_override.as_deref(),
                ) {
                    use crate::readouts::ReadoutBinder;
                    use crate::readouts::ReadoutContext;
                    let mut sink = crate::readouts::StringSink::with_capacity(192);
                    binder.fire(crate::readouts::Event::Update, &final_ctx, &mut sink);
                    let rendered_final = sink.take();
                    crate::readouts::snapshot::capture(
                        activity.config.snapshot_writer.as_ref(),
                        crate::readouts::Event::Update.slot_name(),
                        crate::readouts::Event::Update.subject_kind().as_str(),
                        &final_ctx.subject_id(),
                        "binder",
                        crate::readouts::snapshot::lod_str(crate::readouts::Lod::Labeled),
                        &rendered_final,
                    );
                }
            }

            // Build a one-shot binder for `on_phase_end`:
            // workload's `on_phase_end:` overrides if any,
            // else the default `phase_done` body. Same
            // SRD-63 §7 binding-layer pattern as the inline
            // status thread above.
            let phase_done_default = {
                let readout = crate::readouts::Registry::lookup("phase_done")
                    .expect("phase_done registered");
                crate::readouts::BakedBody::from_single(
                    readout, crate::readouts::Lod::Labeled,
                )
            };
            let rendered = match crate::readouts::build_event_binder(
                &activity.config.readouts,
                crate::readouts::Event::PhaseEnd,
                phase_done_default,
            ) {
                Ok(mut binder) => {
                    use crate::readouts::ReadoutBinder;
                    let mut sink = crate::readouts::StringSink::with_capacity(160);
                    binder.fire(crate::readouts::Event::PhaseEnd, &ctx, &mut sink);
                    sink.take()
                }
                Err(e) => {
                    crate::diag!(crate::observer::LogLevel::Error,
                        "readouts: failed to bind on_phase_end — {e}");
                    String::new()
                }
            };
            // Push 6: capture the on_phase_end render to the
            // snapshot store. The DONE line is the canonical
            // "what the operator saw at completion" — replay
            // returns it byte-for-byte.
            if !rendered.is_empty() {
                use crate::readouts::ReadoutContext;
                crate::readouts::snapshot::capture(
                    activity.config.snapshot_writer.as_ref(),
                    crate::readouts::Event::PhaseEnd.slot_name(),
                    crate::readouts::Event::PhaseEnd.subject_kind().as_str(),
                    &ctx.subject_id(),
                    "binder",
                    crate::readouts::snapshot::lod_str(crate::readouts::Lod::Labeled),
                    &rendered,
                );
            }
            if !rendered.is_empty() {
                crate::diag!(crate::observer::LogLevel::Info, "{}", rendered);
            }
        }

        // Signal the progress thread to stop.
        progress_flag.store(false, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(10));

        // Print validation summary AND capture to the metrics
        // store in one pass. `snapshot()` drains the histogram
        // (delta semantics), so we must use the same snapshot
        // for both printing and SQLite capture.
        if !validation_metrics.is_empty() {
            let mut total_passed = 0u64;
            let mut total_failed = 0u64;
            let now = Instant::now();
            let mut final_snapshot = MetricSet::at(now, Duration::ZERO);
            let activity_labels = activity.labels.clone();

            for vm in validation_metrics.iter() {
                total_passed += vm.passed();
                total_failed += vm.failed();

                for (name, stats) in &vm.relevancy_stats {
                    let snap = stats.snapshot();
                    if !snap.is_empty() {
                        let mean = snap.mean();
                        let p50 = snap.p50();
                        let p99 = snap.p99();
                        let min = snap.min();
                        let max = snap.max();
                        let n = snap.len();
                        // Relevancy stats (recall@k, precision@k, F1@k)
                        // are fractions in [0, 1]. Render as percent
                        // — the unit operators read these in.
                        // Underlying gauges below stay as fractions so
                        // downstream consumers (recall_summary,
                        // metrics scrapes) keep their existing scale.
                        // Indent matches the phase / DONE / complete
                        // lines so the relevancy summary nests under
                        // the phase row in tui=terminal output.
                        let depth_indent = crate::scene_tree::running_phase_indent();
                        let color = crate::observer::use_color();
                        let dim = color.then(|| "\x1b[2m").unwrap_or("");
                        let bold = color.then(|| "\x1b[1m").unwrap_or("");
                        let reset = color.then(|| "\x1b[0m").unwrap_or("");
                        crate::diag!(crate::observer::LogLevel::Info,
                            "{depth_indent}{bold}{name}{reset}: mean={:.2}% {dim}p50={:.2}% p99={:.2}% min={:.2}% max={:.2}% (n={n}){reset}",
                            mean * 100.0, p50 * 100.0, p99 * 100.0, min * 100.0, max * 100.0,
                        );
                        // Pick up `k`/`r` from the F64Stats's
                        // labels so per-phase summary gauges
                        // remain unique under OpenMetrics §4.5
                        // when multiple relevancy configs share
                        // a phase but differ in cutoff.
                        let stats_labels = stats.labels();
                        let k_label = stats_labels.get("k").map(str::to_string);
                        let r_label = stats_labels.get("r").map(str::to_string);
                        for (stat, val) in [("mean", mean), ("p50", p50), ("p99", p99), ("min", min), ("max", max)] {
                            let mut gauge_labels = activity_labels.with("n", &n.to_string());
                            if let Some(k) = &k_label {
                                gauge_labels = gauge_labels.with("k", k);
                            }
                            if let Some(r) = &r_label {
                                gauge_labels = gauge_labels.with("r", r);
                            }
                            final_snapshot.insert_gauge(
                                format!("{name}_{stat}"),
                                gauge_labels,
                                val,
                                now,
                            );
                        }
                    }
                }
            }

            // Phase-level aggregate counters. One pair per phase
            // — `total_passed` / `total_failed` sum across every
            // op's `vm` so the metric instance is unique under
            // OpenMetrics §4.5 (LabelSets must be unique). The
            // earlier per-`vm` insertion path inserted N copies
            // with identical labels, which the snapshot
            // assembler now rejects as a duplicate. Per-op
            // breakdown isn't carried by the validation counters
            // anyway — the labels are activity-scope, not
            // op-scope.
            if total_passed > 0 || total_failed > 0 {
                final_snapshot.insert_counter(
                    "validations_passed",
                    activity_labels.clone(),
                    total_passed,
                    now,
                );
                final_snapshot.insert_counter(
                    "validations_failed",
                    activity_labels.clone(),
                    total_failed,
                    now,
                );
            }

            // Validation summary line: only emit when there are
            // failures. On clean runs the relevancy summary's
            // `n=N` already conveys "N validations passed", and
            // the `validation: N passed, 0 failed` line was just
            // duplicate text on every phase. On failure runs the
            // line is signal — promote it to Warn so it stands
            // out and route only when failed > 0.
            if total_failed > 0 {
                let depth_indent = crate::scene_tree::running_phase_indent();
                crate::diag!(crate::observer::LogLevel::Warn,
                    "{depth_indent}validation: {} passed, {} FAILED",
                    total_passed, total_failed
                );
            }

            if !final_snapshot.is_empty() {
                activity.validation_frame.lock().unwrap_or_else(|e| e.into_inner())
                    .replace(final_snapshot);
            }
        }

        activity.stop_flag.load(Ordering::Relaxed)
            || crate::session_signals::stop_requested()
    }
}

/// Executor task for the tiered DriverAdapter interface.
///
/// Each fiber has its own FiberBuilder (lock-free GK state).
/// Ops within a stanza are processed in dependency groups:
/// - Groups execute sequentially (captures flow between groups)
/// - Ops within a group execute concurrently (join_all)
///
/// Groups are determined at init time by analyzing capture
/// declarations and references across templates.
// `pull_plans`: per-template wrapper-side `PullPlan`s, sealed at init.
// Drives cycle-time reads for validation / conditional / throttle
// wrappers via memoized `PullHandle`s. See SRD 31 §"Pull plan vs bind
// plan".
async fn executor_task(
    activity: Arc<Activity>,
    dispensers: Arc<Vec<Arc<dyn OpDispenser>>>,
    pull_plans: Arc<Vec<crate::fixture::PullPlan>>,
    op_builder: Arc<crate::synthesis::OpBuilder>,
    // Optional activity-level rate limiter. `acquire` fires
    // once per cycle before adapter dispatch. There is no
    // separate stanza-rate limiter.
    rate_limiter: Option<Arc<RateLimiter>>,
    // Per-fiber cooperative-exit flag owned by the activity's
    // [`crate::fiber_pool::FiberPool`]. Set to `true` by
    // `ConcurrencyApplier` when the pool scales down.
    fiber_stop: crate::fiber_pool::StopFlag,
) {
    let stanza_len = activity.op_sequence.stanza_length() as u64;
    let max_retries = activity.config.max_retries;
    // Per-fiber `FiberBuilder` carries scope values (per-iteration
    // extern inputs) populated by the OpBuilder, so iter-var
    // references like `{table}` in op templates resolve to the
    // current iteration's value.
    let mut fiber = op_builder.create_fiber_builder();

    // SRD-68 Push 3 — materialise per-fiber subscope kernels from
    // each dispenser's canonical kernel. The fiber holds them as
    // `Vec<Option<GkKernel>>` indexed parallel to the dispenser
    // registry; cycle dispatch reads `fiber.per_op_kernel(template_idx)`
    // to populate `ExecCtx::wires` for the firing dispenser.
    // Dispensers that return `None` from `canonical_kernel()` get
    // a `None` slot and the cycle falls back to `NullWireSource`.
    fiber.attach_dispenser_kernels(&dispensers);

    // Create per-fiber source reader (used for all phases).
    // Source-declared phases will eventually use the advancer model,
    // but for now all phases go through the source reader.
    let mut source = activity.source_factory.create_reader();

    loop {
        if activity.stop_flag.load(std::sync::atomic::Ordering::Relaxed) { break; }
        if crate::session_signals::stop_requested() { break; }
        if fiber_stop.load(std::sync::atomic::Ordering::Acquire) { break; }

        // Phase 1: RESERVE — CAS on shared cursor, instantaneous.
        // Acquires one stanza's worth of ordinals. This is the only
        // shared-state interaction per stanza.
        let range = match source.reserve(stanza_len as usize) {
            Some(r) => r,
            None => break, // source exhausted
        };

        activity.metrics.stanzas_total.inc();
        fiber.reset_captures();

        // Phase 2: RENDER + EXECUTE — fiber-local, no contention.
        // Each op resolves via this fiber's GK instance, then
        // dispatches to the adapter. Sequential in declaration order.
        for ordinal in range.clone() {
            if activity.stop_flag.load(Ordering::Relaxed) { break; }
            if crate::session_signals::stop_requested() { break; }

            // Mark op as active from render through result join.
            // "Active" means this fiber is working on an op — resolving
            // fields, waiting for the adapter, or recording results.
            activity.metrics.ops_started.fetch_add(1, Ordering::Relaxed);

            // Render the source item (fiber-local, no shared state)
            let item = source.render_item(ordinal);
            let cycle = ordinal;
            // Publish the cycle to the enclosing fiber-context
            // scope so any GK node reading `cycle()` or implicitly
            // `cycle` inside the DAG sees the same ordinal as
            // adapter execution. No-op outside a fiber scope.
            nbrs_variates::nodes::runtime_context::set_task_cycle(cycle);

            let wait_start = Instant::now();
            if let Some(ref rl) = rate_limiter {
                rl.acquire().await;
            }
            let wait_nanos = wait_start.elapsed().as_nanos() as u64;

            let (template_idx, template) = activity.op_sequence.get_with_index(cycle);
            fiber.set_source_item(&item);
            // SRD-68 Push 5: `ctx.fields` is no longer the
            // resolution surface for adapters or wrappers — they
            // read everything through `ctx.wires` (the bound GK
            // context). The empty `ResolvedFields` satisfies the
            // `ExecCtx` struct-shape contract until the field is
            // removed from the trait surface entirely.
            let fields = crate::adapter::ResolvedFields::new(Vec::new(), Vec::new());

            // Resolve the wrapper-side pull plan against this
            // fiber's GkState (one indexed pull per registered
            // name, no name hashing). The resulting `pulls` is
            // disjoint from `fields`: adapters see only `fields`,
            // wrappers see only `pulls`.
            //
            // SRD-13d Phase 9 — when this op template materialised
            // its own kernel, the plan was sealed against the
            // op-template program; resolve_pulls_for_op picks
            // that kernel's state. Flattened op-templates fall
            // through to the main kernel (the workload program)
            // — same call site, the lookup is idempotent.
            let pulls = fiber.resolve_pulls_for_idx(
                template_idx,
                &pull_plans[template_idx],
            );
            let dispenser = &dispensers[template_idx];
            // SRD-68 invariant I-2: cycle-time reads against the
            // firing dispenser's per-fiber kernel slot, exposed
            // through the narrow `WireSource` trait. `CycleWires`
            // wraps the per-fiber kernel handle for the cycle's
            // duration so `WireSource::get` can drive output pulls
            // (`pull(&mut state, …)` through interior mutability)
            // alongside input/constant lookups. Dispensers with
            // no canonical kernel (legacy adapters, wrapper
            // delegates) fall through to the `NullWireSource`
            // baseline `ExecCtx::new` provides.
            // SRD-13f / SRD-68: cycle-time wire reads go through a
            // single kernel handle — the dispenser's per-fiber
            // op-template kernel. Every visible cross-scope wire
            // was wired into that kernel at construction (cells
            // for shared, folded constants for workload params,
            // construction-time slot setup + per-cycle refresh in
            // `set_inputs` for other parent outputs). The local
            // read API resolves every name; the wires layer never
            // composes chains externally.
            let cycle_wires = match fiber.per_op_kernel_mut(template_idx) {
                Some(p) => crate::wires::CycleWires::new(p),
                None => crate::wires::CycleWires::new(fiber.main_kernel_mut()),
            };
            let exec_ctx = crate::fixture::ExecCtx::with_wires(&fields, &pulls, &cycle_wires);
            let service_start = Instant::now();
            let mut tries = 1u32;
            let (success, captures, skipped) = loop {
                match dispenser.execute(cycle, &exec_ctx).await {
                    Ok(result) => {
                        break (true, result.captures, result.skipped);
                    }
                    Err(e) => {
                        let duration_nanos = service_start.elapsed().as_nanos() as u64;
                        let inner = e.error();
                        let detail = activity.error_router.handle_error(
                            &inner.error_name, &inner.message, cycle, duration_nanos,
                        );
                        activity.metrics.errors_total.inc();
                        activity.metrics.count_error_type(&inner.error_name);

                        if detail.should_stop {
                            activity.stop_flag.store(true, Ordering::Relaxed);
                            // Capture the first stopping error so the
                            // phase-level error surfaces a real
                            // diagnostic instead of a bare "stopped
                            // by error handler". Lock-and-set-once;
                            // later fibers' errors don't overwrite.
                            //
                            // Include op-template name + cycle so the
                            // operator can pinpoint exactly which op
                            // template inside the phase fired the
                            // error. Plus the dispenser's `describe()`
                            // (when it has one) so the operator can
                            // see the actual statement / request the
                            // op was firing — a bare op-template name
                            // doesn't tell you whether it's the
                            // wrong dialect's branch, a malformed
                            // bindpoint, or a broken filter clause.
                            if let Ok(mut slot) = activity.stop_reason.lock()
                                && slot.is_none()
                            {
                                let op_shape = dispenser.describe()
                                    .map(|d| format!("\n    op-template: {d}"))
                                    .unwrap_or_default();
                                let op_resolved = dispenser
                                    .describe_resolved(exec_ctx.wires)
                                    .map(|d| format!("\n    op-resolved: {d}"))
                                    .unwrap_or_default();
                                *slot = Some(format!(
                                    "[{}] op '{}' at cycle {}: {}{op_shape}{op_resolved}",
                                    inner.error_name,
                                    template.name,
                                    cycle,
                                    inner.message,
                                ));
                            }
                        }

                        if !e.is_adapter_level() && detail.is_retryable() && tries <= max_retries {
                            tries += 1;
                            continue;
                        }

                        break (false, std::collections::HashMap::new(), false);
                    }
                }
            };
            let service_nanos = service_start.elapsed().as_nanos() as u64;

            // Record metrics
            activity.metrics.cycles_total.inc();
            if !skipped {
                activity.metrics.service_time.record(service_nanos);
                activity.metrics.wait_time.record(wait_nanos);
                activity.metrics.response_time.record(service_nanos + wait_nanos);
                activity.metrics.tries_histogram.record(tries as u64);
                if success {
                    activity.metrics.successes_total.inc();
                    activity.metrics.result_success_time.record(service_nanos);
                    // SRD-67 Phase 5 — captures route to BOTH
                    // the fiber's main kernel (legacy path) AND
                    // the per-op-template kernel when one was
                    // materialised. The op-template kernel is the
                    // one that carries result-binding extern
                    // slots for `body` / `count` / `ok` plus any
                    // user captures the result-bindings reference,
                    // and Rule 2 write-throughs feed values up
                    // through parent `shared` cells. Slots that
                    // don't exist on either kernel are silently
                    // dropped (closure-binding economy).
                    let debug_nodes = nbrs_variates::nodes::debug_nodes_enabled();
                    if debug_nodes {
                        crate::observer::log(
                            crate::observer::LogLevel::Debug,
                            &format!(
                                "activity.cycle: op '{}' captures={:?}",
                                template.name,
                                captures.keys().collect::<Vec<_>>()
                            ),
                        );
                    }
                    for (name, value) in captures {
                        fiber.capture(&name, value.clone());
                        let wrote = fiber.write_op_template_input_for_idx(template_idx, &name, value);
                        if debug_nodes {
                            crate::observer::log(
                                crate::observer::LogLevel::Debug,
                                &format!(
                                    "activity.cycle: op '{}' write_op_template_input \
                                     name='{}' wrote={}",
                                    template.name, name, wrote
                                ),
                            );
                        }
                    }
                    // Fire the Rule 2 write-through commit on the
                    // op-template kernel — pulls every
                    // `__write_<X>` and stores its value through
                    // the cell-bound input slot for `<X>`. No-op
                    // when the kernel carries no write-throughs.
                    fiber.commit_op_template_write_throughs_for_idx(template_idx);
                }
            }

            // Op fully processed — render, execute, and metrics all done.
            activity.metrics.ops_finished.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Best-effort terminal column count read off stderr (fd 2) via
/// the `TIOCGWINSZ` ioctl. Returns `None` when stderr isn't a
/// TTY or the call fails. The inline status line uses this to
/// truncate to a single visual row, since the `\r\x1b[K`
/// in-place rewrite only erases from the cursor to the end of
/// the *current* visual line — anything that wraps once stays
/// on screen and previous-tick text peeks through underneath.
fn terminal_cols() -> Option<usize> {
    use std::os::raw::c_int;
    #[repr(C)]
    struct WinSize {
        ws_row: u16,
        ws_col: u16,
        ws_xpixel: u16,
        ws_ypixel: u16,
    }
    let mut ws = WinSize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    // SAFETY: `libc::ioctl` is FFI; `TIOCGWINSZ` writes into the
    // out-parameter which we own (pinned on the stack for the
    // duration of the call). Failure is signalled by negative
    // return — we ignore the actual errno.
    let rc: c_int = unsafe {
        libc::ioctl(2, libc::TIOCGWINSZ, &mut ws as *mut _)
    };
    if rc < 0 || ws.ws_col == 0 {
        return None;
    }
    Some(ws.ws_col as usize)
}

/// Glob-style match: `*` matches zero or more characters, `?`
/// matches exactly one character, every other byte must match
/// literally. Recursive — adequate for the short patterns
/// `status_metrics:` accepts (`recall*`, `latency_p99`, etc.).
/// Trades worst-case quadratic time for simplicity; the
/// candidate set is also tiny (low single-digit count of metric
/// names per phase).
fn glob_match(pattern: &str, candidate: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), candidate.as_bytes())
}

fn glob_match_bytes(pat: &[u8], s: &[u8]) -> bool {
    match (pat.first(), s.first()) {
        (None, None) => true,
        (Some(b'*'), _) => {
            // zero-or-more: try consuming nothing OR consume one
            // char of input and re-attempt.
            glob_match_bytes(&pat[1..], s)
                || (!s.is_empty() && glob_match_bytes(pat, &s[1..]))
        }
        (Some(b'?'), Some(_)) => glob_match_bytes(&pat[1..], &s[1..]),
        (Some(p), Some(c)) if p == c => glob_match_bytes(&pat[1..], &s[1..]),
        _ => false,
    }
}

// `spinner_frame`, `braille_bar`, `format_eta` moved to
// `crate::readouts::format` in Push 2 — the readouts that
// consume them now own the helpers. `truncate_to_width`
// stays here (it's a surface-level width-clamp concern,
// not a readout concern).

/// Truncate `s` to at most `max_cols` *visible* columns,
/// appending an ellipsis when truncation actually elides
/// content. Skips ANSI SGR escape sequences (`\x1b[...m`) when
/// counting visible width — they consume characters in the
/// string but no terminal columns. The truncation point is
/// always at a character boundary that's NOT inside an escape
/// sequence, so we never emit a half-broken `\x1b[3` to the
/// terminal.
fn truncate_to_width(s: &str, max_cols: usize) -> String {
    if max_cols == 0 { return String::new(); }
    let bytes = s.as_bytes();
    let mut visible = 0usize;
    let mut byte_pos = 0usize; // last clean truncation point
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        if c == '\x1b' && bytes.get(i + 1) == Some(&b'[') {
            // SGR escape: walk until the final byte (`m`,
            // `K`, `J`, etc.) so we don't truncate mid-escape.
            for (_, ch) in chars.by_ref() {
                if ch.is_ascii_alphabetic() { break; }
            }
            // byte_pos doesn't advance — escape costs no
            // visible columns, and the next plain char's
            // position is what we'd truncate to.
            continue;
        }
        if visible + 1 > max_cols.saturating_sub(1) {
            return format!("{}…", &s[..byte_pos]);
        }
        visible += 1;
        byte_pos = i + c.len_utf8();
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{OpResult, AdapterError, ExecutionError};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A counting DriverAdapter + OpDispenser for testing.
    struct CountingDriverAdapter {
        count: Arc<AtomicU64>,
    }

    impl CountingDriverAdapter {
        fn new() -> (Self, Arc<AtomicU64>) {
            let count = Arc::new(AtomicU64::new(0));
            (Self { count: count.clone() }, count)
        }
    }

    impl DriverAdapter for CountingDriverAdapter {
        fn name(&self) -> &str { "counting" }
        fn map_op(
            &self,
            _template: &nbrs_workload::model::ParsedOp,
            _parent: std::sync::Arc<nbrs_variates::kernel::GkKernel>,
        ) -> Result<Box<dyn OpDispenser>, String> {
            Ok(Box::new(CountingDispenser { count: self.count.clone() }))
        }
    }

    struct CountingDispenser {
        count: Arc<AtomicU64>,
    }

    impl OpDispenser for CountingDispenser {
        fn execute<'a>(&'a self, _cycle: u64, _ctx: &'a crate::fixture::ExecCtx<'a>)
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(OpResult { body: None, captures: HashMap::new(), skipped: false }) })
        }
    }

    /// A fail-then-succeed DriverAdapter for retry testing.
    struct FailThenSucceedDriverAdapter {
        fails_remaining: Arc<AtomicU64>,
        total_calls: Arc<AtomicU64>,
    }

    impl FailThenSucceedDriverAdapter {
        fn new(fail_count: u64) -> (Self, Arc<AtomicU64>) {
            let total = Arc::new(AtomicU64::new(0));
            (Self {
                fails_remaining: Arc::new(AtomicU64::new(fail_count)),
                total_calls: total.clone(),
            }, total)
        }
    }

    impl DriverAdapter for FailThenSucceedDriverAdapter {
        fn name(&self) -> &str { "fail-then-succeed" }
        fn map_op(
            &self,
            _template: &nbrs_workload::model::ParsedOp,
            _parent: std::sync::Arc<nbrs_variates::kernel::GkKernel>,
        ) -> Result<Box<dyn OpDispenser>, String> {
            Ok(Box::new(FailThenSucceedDispenser {
                fails_remaining: self.fails_remaining.clone(),
                total_calls: self.total_calls.clone(),
            }))
        }
    }

    struct FailThenSucceedDispenser {
        fails_remaining: Arc<AtomicU64>,
        total_calls: Arc<AtomicU64>,
    }

    impl OpDispenser for FailThenSucceedDispenser {
        fn execute<'a>(&'a self, _cycle: u64, _ctx: &'a crate::fixture::ExecCtx<'a>)
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            self.total_calls.fetch_add(1, Ordering::Relaxed);
            let remaining = self.fails_remaining.fetch_sub(1, Ordering::Relaxed);
            Box::pin(async move {
                if remaining > 0 {
                    Err(ExecutionError::Op(AdapterError {
                        error_name: "TransientError".into(),
                        message: "temporary failure".into(),
                        retryable: true,
                    }))
                } else {
                    Ok(OpResult { body: None, captures: HashMap::new(), skipped: false })
                }
            })
        }
    }

    /// Build a minimal GK root kernel (single identity node) for tests.
    fn test_kernel() -> nbrs_variates::kernel::GkKernel {
        use nbrs_variates::assembly::{GkAssembler, WireRef};
        use nbrs_variates::nodes::identity::Identity;
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("id", Box::new(Identity::new()), vec![WireRef::input("cycle")]);
        asm.add_output("id", WireRef::node("id"));
        asm.compile().unwrap()
    }

    #[tokio::test]
    async fn activity_runs_all_cycles() {
        let config = ActivityConfig {
            name: "test".into(),
            cycles: 100,
            concurrency: 4,
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "test"), seq);

        let (adapter, count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        assert_eq!(count.load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn activity_retries_on_error() {
        let config = ActivityConfig {
            name: "retrytest".into(),
            cycles: 1,
            concurrency: 1,
            error_spec: "TransientError:retry,warn;.*:stop".into(),
            max_retries: 5,
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let (adapter, total_calls) = FailThenSucceedDriverAdapter::new(2);
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        assert_eq!(total_calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn shared_metrics_accessible() {
        let config = ActivityConfig {
            name: "metricstest".into(),
            cycles: 50,
            concurrency: 2,
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let shared_metrics = activity.shared_metrics();

        let (adapter, _count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        assert_eq!(shared_metrics.cycles_total.get(), 50);
        let frame = shared_metrics.capture(std::time::Duration::from_secs(1));
        assert!(!frame.is_empty());
    }

    #[tokio::test]
    async fn per_error_type_counters_emit_deltas_through_dynamic_capture() {
        // SRD-40 / cascade coalesce: `MetricSet::combine_into` for
        // Counter sums `total` across intervals — so per-cycle
        // emissions must be DELTAS, not absolutes. Per-error-type
        // counters live on `ActivityMetricsDynamic` (outside the
        // static registry), so they need their own delta tracking.
        // This test exercises that path directly.
        use nbrs_metrics::component::Component;
        use nbrs_metrics::snapshot::MetricValue;

        let metrics = Arc::new(ActivityMetrics::new(&Labels::of("session", "s1")));
        let component = Arc::new(std::sync::RwLock::new(
            Component::new(Labels::of("activity", "t"), HashMap::new()),
        ));
        {
            let mut g = component.write().unwrap();
            g.set_state(nbrs_metrics::component::ComponentState::Running);
            metrics.register_on(&mut g).unwrap();
        }

        // Seed two error-type counters with different totals.
        for _ in 0..3 { metrics.count_error_type("net"); }
        for _ in 0..7 { metrics.count_error_type("timeout"); }

        // First capture_delta — totals=3 and 7 are the deltas.
        let snap1 = component.read().unwrap()
            .capture_delta(std::time::Duration::from_secs(1));
        let net1 = read_counter(&snap1, "errors.net");
        let to1  = read_counter(&snap1, "errors.timeout");
        assert_eq!(net1, 3, "first delta for net should be 3, got {net1}");
        assert_eq!(to1, 7,  "first delta for timeout should be 7, got {to1}");

        // Drive the per-error-type counters further.
        for _ in 0..2 { metrics.count_error_type("net"); }
        for _ in 0..1 { metrics.count_error_type("timeout"); }

        // Second capture_delta — should report only the new deltas
        // (2 and 1), NOT the absolute totals (5 and 8).
        let snap2 = component.read().unwrap()
            .capture_delta(std::time::Duration::from_secs(1));
        let net2 = read_counter(&snap2, "errors.net");
        let to2  = read_counter(&snap2, "errors.timeout");
        assert_eq!(net2, 2, "second delta for net should be 2 (new only), got {net2}");
        assert_eq!(to2, 1,  "second delta for timeout should be 1 (new only), got {to2}");

        // capture_current (drain=false) should still report absolutes.
        let cur = component.read().unwrap().capture_current();
        let net_abs = read_counter(&cur, "errors.net");
        let to_abs  = read_counter(&cur, "errors.timeout");
        assert_eq!(net_abs, 5, "current should be absolute total 5, got {net_abs}");
        assert_eq!(to_abs, 8,  "current should be absolute total 8, got {to_abs}");

        fn read_counter(snap: &nbrs_metrics::snapshot::MetricSet, family: &str) -> u64 {
            let f = snap.family(family).unwrap_or_else(||
                panic!("family {family:?} missing from snapshot"));
            let m = f.metrics().next().expect("at least one metric");
            match m.point().unwrap().value() {
                MetricValue::Counter(c) => c.total,
                v => panic!("not a counter: {v:?}"),
            }
        }
    }

    #[tokio::test]
    async fn activity_with_rate() {
        let config = ActivityConfig {
            name: "ratetest".into(),
            cycles: 10,
            concurrency: 2,
            rate: Some(10000.0),
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let (adapter, count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        assert_eq!(count.load(Ordering::Relaxed), 10);
    }

    #[tokio::test]
    async fn activity_with_weighted_ops() {
        let config = ActivityConfig {
            name: "weighted".into(),
            cycles: 12,
            concurrency: 1,
            ..Default::default()
        };
        let ops = vec![
            nbrs_workload::model::ParsedOp::simple("read", "SELECT"),
            nbrs_workload::model::ParsedOp::simple("write", "INSERT"),
        ];
        let seq = OpSequence::build(ops, &[4, 2], SequencerType::Bucket);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let (adapter, count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        assert_eq!(count.load(Ordering::Relaxed), 12);
    }

    #[tokio::test]
    async fn rate_control_is_declared_when_rate_configured() {
        use nbrs_metrics::component::Component;
        use nbrs_metrics::labels::Labels as L;
        use std::sync::RwLock;

        let config = ActivityConfig {
            name: "rate_decl".into(),
            cycles: 5,
            concurrency: 1,
            rate: Some(2500.0),
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let mut activity = Activity::new(
            config, &L::of("session", "s_rate"), seq,
        );
        let component = Arc::new(RwLock::new(Component::new(
            L::of("session", "s_rate"), std::collections::HashMap::new(),
        )));
        activity.attach_component(component.clone());

        let (adapter, _count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        // After the activity runs, the rate control is on the
        // component and reports the configured target via its
        // reified gauge.
        let guard = component.read().unwrap();
        let erased = guard.controls().get_erased("rate")
            .expect("rate control should be declared when rate is set");
        assert!(erased.accepts_f64_writes());
        assert_eq!(erased.gauge_f64(), Some(2500.0));
    }

    #[tokio::test]
    async fn rate_control_is_absent_when_no_rate() {
        use nbrs_metrics::component::Component;
        use nbrs_metrics::labels::Labels as L;
        use std::sync::RwLock;

        let config = ActivityConfig {
            name: "no_rate".into(),
            cycles: 3,
            concurrency: 1,
            rate: None,
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let mut activity = Activity::new(
            config, &L::of("session", "s_nr"), seq,
        );
        let component = Arc::new(RwLock::new(Component::new(
            L::of("session", "s_nr"), std::collections::HashMap::new(),
        )));
        activity.attach_component(component.clone());

        let (adapter, _count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        let guard = component.read().unwrap();
        assert!(
            guard.controls().get_erased("rate").is_none(),
            "no rate control should exist without rate configured",
        );
    }

    #[tokio::test]
    async fn rate_control_write_retargets_the_running_limiter() {
        use nbrs_metrics::component::Component;
        use nbrs_metrics::controls::ControlOrigin;
        use nbrs_metrics::labels::Labels as L;
        use std::sync::RwLock;

        // 200 cycles with a low rate + a concurrent writer that
        // bumps the rate mid-flight. The committed value on the
        // control reflects the write; the limiter carries the
        // same target after reconfigure.
        let config = ActivityConfig {
            name: "rate_live".into(),
            cycles: 200,
            concurrency: 2,
            rate: Some(50.0),
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let mut activity = Activity::new(
            config, &L::of("session", "s_live"), seq,
        );
        let component = Arc::new(RwLock::new(Component::new(
            L::of("session", "s_live"), std::collections::HashMap::new(),
        )));
        activity.attach_component(component.clone());

        // Spawn the activity, wait for the applier to be wired,
        // issue a typed write, assert the control value advanced.
        let component_for_writer = component.clone();
        let writer = tokio::spawn(async move {
            for _ in 0..50 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                let ctl: Option<nbrs_metrics::controls::Control<nbrs_rate::RateSpec>> =
                    component_for_writer.read().unwrap()
                        .controls().get("rate");
                if let Some(c) = ctl {
                    // Only attempt once the applier is registered.
                    if c.applier_count() > 0 {
                        c.set(nbrs_rate::RateSpec::new(10_000.0),
                              ControlOrigin::Test).await.ok();
                        return;
                    }
                }
            }
        });

        let (adapter, _count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;
        let _ = writer.await;

        let guard = component.read().unwrap();
        let ctl: nbrs_metrics::controls::Control<nbrs_rate::RateSpec> =
            guard.controls().get("rate").unwrap();
        assert_eq!(ctl.value().ops_per_sec, 10_000.0);
    }

    #[tokio::test]
    async fn concurrency_control_is_declared_on_attached_component() {
        // SRD 23 integration: the activity declares its
        // `concurrency` control on the attached component during
        // startup; the control's reified gauge reads the
        // configured value.
        use nbrs_metrics::component::Component;
        use nbrs_metrics::labels::Labels as L;
        use std::sync::RwLock;

        let config = ActivityConfig {
            name: "ctrl_decl".into(),
            cycles: 10,
            concurrency: 3,
            ..Default::default()
        };
        let ops = vec![nbrs_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let mut activity = Activity::new(
            config, &L::of("session", "s_decl"), seq,
        );
        let component = Arc::new(RwLock::new(Component::new(
            L::of("session", "s_decl"), std::collections::HashMap::new(),
        )));
        activity.attach_component(component.clone());

        let (adapter, _count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::new(test_kernel()))).await;

        // After run completes the control is still on the
        // component (structural declaration survives execution).
        let guard = component.read().unwrap();
        let erased = guard.controls().get_erased("concurrency")
            .expect("concurrency control should be declared on attached component");
        assert_eq!(erased.value_string(), "3");
        assert!(erased.accepts_f64_writes());
        // Gauge projection reads as f64.
        assert_eq!(erased.gauge_f64(), Some(3.0));
    }
}
