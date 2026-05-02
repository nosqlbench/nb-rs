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
        }
    }
}

/// Standard metrics for an activity. Shared via Arc so the metrics
/// scheduler can capture snapshots while executor tasks record.
pub struct ActivityMetrics {
    pub service_time: Timer,
    pub wait_time: Timer,
    pub response_time: Timer,
    /// Service time for successful ops only. Allows isolating
    /// success latency from error/retry latency.
    pub result_success_time: Timer,
    /// Number of tries per op (1 = succeeded first try, 2+ = retried).
    /// Distribution shape reveals incremental saturation.
    pub tries_histogram: Histogram,
    pub cycles_total: Counter,
    pub successes_total: Counter,
    pub skips_total: Counter,
    pub errors_total: Counter,
    pub stanzas_total: Counter,
    /// Number of ops dispatched to adapters (monotonic).
    pub ops_started: std::sync::atomic::AtomicU64,
    /// Number of ops returned from adapters (monotonic).
    pub ops_finished: std::sync::atomic::AtomicU64,
    pub result_elements: Counter,
    pub result_bytes: Counter,
    /// Per-error-type counters, keyed by error_name.
    /// Created on demand when a new error type is first seen.
    error_type_counts: std::sync::Mutex<std::collections::HashMap<String, Counter>>,
    labels: Labels,
    /// Previous counter values for delta computation. Keyed by label identity hash.
    /// Updated on each `capture_delta()` call.
    prev_counters: std::sync::Mutex<std::collections::HashMap<u64, u64>>,
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
            service_time: Timer::with_sigdigs(labels.with("name", "cycles_servicetime"), sigdigs),
            wait_time: Timer::with_sigdigs(labels.with("name", "cycles_waittime"), sigdigs),
            response_time: Timer::with_sigdigs(labels.with("name", "cycles_responsetime"), sigdigs),
            result_success_time: Timer::with_sigdigs(labels.with("name", "result_success"), sigdigs),
            tries_histogram: nbrs_metrics::instruments::histogram::Histogram::with_sigdigs(labels.with("name", "tries"), sigdigs),
            cycles_total: Counter::new(labels.with("name", "cycles_total")),
            successes_total: Counter::new(labels.with("name", "successes_total")),
            skips_total: Counter::new(labels.with("name", "skips_total")),
            errors_total: Counter::new(labels.with("name", "errors_total")),
            stanzas_total: Counter::new(labels.with("name", "stanzas_total")),
            ops_started: std::sync::atomic::AtomicU64::new(0),
            ops_finished: std::sync::atomic::AtomicU64::new(0),
            result_elements: Counter::new(labels.with("name", "result_elements")),
            result_bytes: Counter::new(labels.with("name", "result_bytes")),
            error_type_counts: std::sync::Mutex::new(std::collections::HashMap::new()),
            labels: labels.clone(),
            prev_counters: std::sync::Mutex::new(std::collections::HashMap::new()),
            dispensers: std::sync::Mutex::new(None),
            validation_metrics: std::sync::Mutex::new(None),
        }
    }

    /// Return the number of cycles completed so far.
    ///
    /// Reads from the `cycles_total` counter atomically. Used by the
    /// progress reporter thread to display live throughput.
    pub fn cycles_completed(&self) -> u64 {
        self.cycles_total.get()
    }

    /// Increment counter for a specific error type. Creates the
    /// counter on first occurrence of each error name.
    pub fn count_error_type(&self, error_name: &str) {
        let mut map = self.error_type_counts.lock()
            .unwrap_or_else(|e| e.into_inner());
        let counter = map.entry(error_name.to_string())
            .or_insert_with(|| {
                Counter::new(self.labels.with("name", &format!("errors.{error_name}")))
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

    /// Compute the counter delta: current absolute value minus previous.
    /// Updates the stored previous value for next call.
    fn counter_delta(&self, counter: &Counter) -> u64 {
        let current = counter.get();
        let hash = counter.labels().identity_hash();
        let mut prev = self.prev_counters.lock().unwrap_or_else(|e| e.into_inner());
        let previous = prev.insert(hash, current).unwrap_or(0);
        current.saturating_sub(previous)
    }
}

impl nbrs_metrics::component::InstrumentSet for ActivityMetrics {
    /// Capture a delta snapshot suitable for the component tree scheduler.
    ///
    /// Timer histograms are inherently delta (reset on snapshot).
    /// Counters emit the change since the last `capture_delta()` call.
    fn capture_delta(&self, interval: Duration) -> MetricSet {
        let service_snap = self.service_time.snapshot();
        let wait_snap = self.wait_time.snapshot();
        let response_snap = self.response_time.snapshot();
        let success_snap = self.result_success_time.snapshot();
        let tries_snap = self.tries_histogram.snapshot();
        let now = Instant::now();

        let mut snap = MetricSet::at(now, interval);

        // Helper: take an instrument's `Labels` (which currently embeds
        // the metric name as a `name=...` pair) and route it into the
        // snapshot's family-keyed shape.
        fn split(l: &Labels) -> (String, Labels) {
            nbrs_metrics::snapshot::split_name_label(l)
        }

        // Timers (histograms in OpenMetrics terms).
        let (n, lbl) = split(self.service_time.labels());
        snap.insert_histogram(n, lbl, service_snap.histogram, now);
        let (n, lbl) = split(self.wait_time.labels());
        snap.insert_histogram(n, lbl, wait_snap.histogram, now);
        let (n, lbl) = split(self.response_time.labels());
        snap.insert_histogram(n, lbl, response_snap.histogram, now);
        let (n, lbl) = split(self.result_success_time.labels());
        snap.insert_histogram(n, lbl, success_snap.histogram, now);

        // Counters.
        let (n, lbl) = split(self.cycles_total.labels());
        snap.insert_counter(n, lbl, self.counter_delta(&self.cycles_total), now);
        let (n, lbl) = split(self.skips_total.labels());
        snap.insert_counter(n, lbl, self.counter_delta(&self.skips_total), now);
        let (n, lbl) = split(self.errors_total.labels());
        snap.insert_counter(n, lbl, self.counter_delta(&self.errors_total), now);
        let (n, lbl) = split(self.stanzas_total.labels());
        snap.insert_counter(n, lbl, self.counter_delta(&self.stanzas_total), now);
        let (n, lbl) = split(self.result_elements.labels());
        snap.insert_counter(n, lbl, self.counter_delta(&self.result_elements), now);
        let (n, lbl) = split(self.result_bytes.labels());
        snap.insert_counter(n, lbl, self.counter_delta(&self.result_bytes), now);

        // Tries histogram.
        let (n, lbl) = split(self.tries_histogram.labels());
        snap.insert_histogram(n, lbl, tries_snap, now);

        // Per-error-type counter deltas.
        let error_counts = self.error_type_counts.lock()
            .unwrap_or_else(|e| e.into_inner());
        for counter in error_counts.values() {
            let (n, lbl) = split(counter.labels());
            snap.insert_counter(n, lbl, self.counter_delta(counter), now);
        }

        // Add adapter-specific metrics (e.g., rows_inserted timer from CQL batch).
        if let Some(ref disps) = *self.dispensers.lock().unwrap_or_else(|e| e.into_inner()) {
            for dispenser in disps.iter() {
                for (family, metric_labels, value) in dispenser.adapter_metrics() {
                    use nbrs_metrics::snapshot::{MetricType, MetricValue};
                    let mtype = match &value {
                        MetricValue::Counter(_) => MetricType::Counter,
                        MetricValue::Gauge(_) => MetricType::Gauge,
                        MetricValue::Histogram(_) => MetricType::Histogram,
                    };
                    snap.insert_metric(family, mtype, metric_labels, value, now);
                }
            }
        }

        snap
    }

    fn capture_current(&self) -> MetricSet {
        use nbrs_metrics::snapshot::split_name_label as split;
        let now = Instant::now();
        let mut snap = MetricSet::at(now, Duration::ZERO);

        // Timers / histograms: non-draining peeks so the pull path
        // never disturbs the scheduler's cascade delta reservoir.
        let (n, lbl) = split(self.service_time.labels());
        snap.insert_histogram(n, lbl, self.service_time.peek_snapshot().histogram, now);
        let (n, lbl) = split(self.wait_time.labels());
        snap.insert_histogram(n, lbl, self.wait_time.peek_snapshot().histogram, now);
        let (n, lbl) = split(self.response_time.labels());
        snap.insert_histogram(n, lbl, self.response_time.peek_snapshot().histogram, now);
        let (n, lbl) = split(self.result_success_time.labels());
        snap.insert_histogram(n, lbl, self.result_success_time.peek_snapshot().histogram, now);
        let (n, lbl) = split(self.tries_histogram.labels());
        snap.insert_histogram(n, lbl, self.tries_histogram.peek_snapshot(), now);

        // Counters: absolute atomic reads — no baseline advance,
        // readable arbitrarily often without perturbing deltas.
        let (n, lbl) = split(self.cycles_total.labels());
        snap.insert_counter(n, lbl, self.cycles_total.get(), now);
        let (n, lbl) = split(self.skips_total.labels());
        snap.insert_counter(n, lbl, self.skips_total.get(), now);
        let (n, lbl) = split(self.errors_total.labels());
        snap.insert_counter(n, lbl, self.errors_total.get(), now);
        let (n, lbl) = split(self.stanzas_total.labels());
        snap.insert_counter(n, lbl, self.stanzas_total.get(), now);
        let (n, lbl) = split(self.result_elements.labels());
        snap.insert_counter(n, lbl, self.result_elements.get(), now);
        let (n, lbl) = split(self.result_bytes.labels());
        snap.insert_counter(n, lbl, self.result_bytes.get(), now);

        let error_counts = self.error_type_counts.lock()
            .unwrap_or_else(|e| e.into_inner());
        for counter in error_counts.values() {
            let (n, lbl) = split(counter.labels());
            snap.insert_counter(n, lbl, counter.get(), now);
        }

        // Adapter-specific metrics are already non-mutating — the
        // delta path just pulls current values and hands them up.
        // Same call works for capture_current.
        if let Some(ref disps) = *self.dispensers.lock().unwrap_or_else(|e| e.into_inner()) {
            for dispenser in disps.iter() {
                for (family, metric_labels, value) in dispenser.adapter_metrics() {
                    use nbrs_metrics::snapshot::{MetricType, MetricValue};
                    let mtype = match &value {
                        MetricValue::Counter(_) => MetricType::Counter,
                        MetricValue::Gauge(_) => MetricType::Gauge,
                        MetricValue::Histogram(_) => MetricType::Histogram,
                    };
                    snap.insert_metric(family, mtype, metric_labels, value, now);
                }
            }
        }

        snap
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
        }
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
        let mut dispensers: Vec<Arc<dyn OpDispenser>> = Vec::new();
        let mut validation_metrics: Vec<Arc<validation::ValidationMetrics>> = Vec::new();
        // Per-template list of GK output names that must appear in
        // `ResolvedFields` for the inner adapter (op-field bind
        // points only). Wrapper-side reads (validation, conditional,
        // throttle) go through the per-template `pull_plans_per_template`
        // PullPlan instead — see SRD 31 §"Pull plan vs bind plan".
        let mut field_pulls_per_template: Vec<Vec<String>> = Vec::new();
        let mut pull_plans_per_template: Vec<crate::fixture::PullPlan> = Vec::new();
        let mut bind_plans_per_template: Vec<Option<crate::synthesis::BindPlan>> = Vec::new();
        let mut batch_configs_per_template: Vec<crate::synthesis::BatchConfig> = Vec::new();
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
            match adapter.map_op(template) {
                Ok(d) => {
                    let raw = Arc::from(d);
                    // Wrap with traversal (innermost). Traversal
                    // does not read GK values; no fixture
                    // registration needed.
                    let traversed = crate::wrappers::TraversingDispenser::wrap(
                        raw, template, traversal_stats.clone(),
                    );

                    // Open the per-template scope fixture (SRD 32
                    // §"Init-Time Fixture and Consumer Self-
                    // Registration"). Each consumer below registers
                    // its own GK name dependencies; the fixture is
                    // sealed after the wrapper chain is complete and
                    // the resulting PullPlan drives cycle-time reads.
                    let mut fx = crate::fixture::ScopeFixture::new(program.clone());

                    // Wrap with delay — only if template has `delay:`
                    let throttled = if let Some(ref delay_name) = template.delay {
                        let name = delay_name.trim()
                            .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                            .unwrap_or(delay_name.trim());
                        match crate::wrappers::ThrottleDispenser::wrap(traversed, name, &mut fx) {
                            Ok(d) => d,
                            Err(e) => {
                                crate::diag!(crate::observer::LogLevel::Error,
                                    "error: op '{}': {e}", template.name);
                                return true;
                            }
                        }
                    } else {
                        traversed
                    };
                    // Wrap with validation — only if template declares it
                    let (validated, vm) = match crate::validation::ValidatingDispenser::wrap(
                        throttled, template, &activity.labels, Some(&program), &mut fx,
                    ) {
                        Ok(pair) => pair,
                        Err(e) => {
                            crate::diag!(crate::observer::LogLevel::Error,
                                "error: op '{}': {e}", template.name);
                            return true; // stop — misconfiguration
                        }
                    };
                    if let Some(vm) = vm {
                        validation_metrics.push(vm);
                    }
                    // Wrap with condition check — only if template has `if:`
                    let conditional = if let Some(ref cond) = template.condition {
                        let cond_name = cond.trim()
                            .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                            .unwrap_or(cond.trim());
                        match crate::wrappers::ConditionalDispenser::wrap(
                            validated, cond_name, activity.metrics.clone(), &mut fx,
                        ) {
                            Ok(d) => d,
                            Err(e) => {
                                crate::diag!(crate::observer::LogLevel::Error,
                                    "error: op '{}': {e}", template.name);
                                return true;
                            }
                        }
                    } else {
                        validated
                    };
                    // Wrap with polling (outermost) — only if template has `poll: await_empty`
                    let final_dispenser = if template.params.get("poll")
                        .and_then(|v| v.as_str()).is_some()
                    {
                        let interval = template.params.get("poll_interval_ms")
                            .and_then(|v| v.as_str().and_then(|s| s.parse().ok())
                                .or_else(|| v.as_u64()))
                            .unwrap_or(1000);
                        let timeout = template.params.get("timeout_ms")
                            .and_then(|v| v.as_str().and_then(|s| s.parse().ok())
                                .or_else(|| v.as_u64()))
                            .unwrap_or(300_000);
                        // SRD-03 §"Status-Determination Invariant
                        // — Retries Within": bounded retry budget
                        // for retryable inner errors. Default 0
                        // (strict — first error fails the poll).
                        // Operators set `poll_max_error_retries:
                        // N` on the op when transient blips
                        // during a long fixture readiness check
                        // are expected.
                        let max_error_retries = template.params.get("poll_max_error_retries")
                            .and_then(|v| v.as_str().and_then(|s| s.parse::<u32>().ok())
                                .or_else(|| v.as_u64().map(|n| n as u32)))
                            .unwrap_or(0);
                        let metric_name = template.params.get("poll_metric_name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let (dispenser, poll_metrics) =
                            crate::wrappers::PollingDispenser::wrap(
                                conditional, interval, timeout, max_error_retries, metric_name,
                            );
                        crate::diag!(crate::observer::LogLevel::Debug,
                            "  op '{}': polling enabled (interval={}ms, timeout={}ms, max_error_retries={})",
                            template.name, interval, timeout, max_error_retries);
                        let _ = poll_metrics; // metrics accessible via Arc if needed
                        dispenser
                    } else {
                        conditional
                    };
                    // Wrap with emit (outermost) — prints result JSON
                    let emitted = if template.params.get("emit")
                        .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
                        .unwrap_or(false)
                    {
                        crate::wrappers::EmitDispenser::wrap(final_dispenser, &template.name)
                    } else {
                        final_dispenser
                    };
                    dispensers.push(emitted);

                    // Seal the per-template fixture. The PullPlan
                    // drives cycle-time reads for every wrapper that
                    // registered (validation ground truth, conditional
                    // `if`, throttle `delay`). See SRD 31 §"Pull plan
                    // vs bind plan".
                    pull_plans_per_template.push(fx.seal());

                    // Collect names that must appear in
                    // ResolvedFields for the inner adapter's op-field
                    // substitution. Wrapper-only names (validation,
                    // condition, delay) are NOT in this list — they
                    // ride the PullPlan.
                    let mut field_pulls = Vec::new();
                    for value in template.op.values() {
                        if let Some(s) = value.as_str() {
                            for name in nbrs_workload::bindpoints::referenced_bindings(s) {
                                if !field_pulls.contains(&name) {
                                    field_pulls.push(name);
                                }
                            }
                        }
                    }
                    field_pulls_per_template.push(field_pulls);

                    // Pre-build the bind plan and batch config once per template.
                    // These were previously built per-cycle inside the resolver.
                    let stmt_field = template.op.get("stmt")
                        .or_else(|| template.op.get("prepared"))
                        .or_else(|| template.op.get("raw"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let bind_names = nbrs_workload::bindpoints::referenced_bindings(stmt_field);
                    let bind_plan = crate::synthesis::BindPlan::new(&bind_names, &program);
                    bind_plans_per_template.push(bind_plan);

                    let batch_config = crate::synthesis::BatchConfig::from_params(&template.params);
                    batch_configs_per_template.push(batch_config);
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
        let field_pulls_per_template = Arc::new(field_pulls_per_template);
        let pull_plans_per_template = Arc::new(pull_plans_per_template);
        let bind_plans_per_template = Arc::new(bind_plans_per_template);
        let batch_configs_per_template = Arc::new(batch_configs_per_template);
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
            std::thread::spawn(move || {
                let activity_name = activity_name_progress;
                let cursor_name = cursor_name_progress;
                let mut tick: u64 = 0;
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
                    // Progress counters — all derived from ops_started/ops_finished
                    // so pending + active + complete = total_extent exactly.
                    let started = progress_metrics.ops_started.load(Ordering::Relaxed);
                    let finished = progress_metrics.ops_finished.load(Ordering::Relaxed);
                    let active = started.saturating_sub(finished);
                    let completed = finished;
                    let pending = total_extent.saturating_sub(started);
                    let pct = if total_extent > 0 {
                        started as f64 * 100.0 / total_extent as f64
                    } else {
                        0.0
                    };
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rate = if elapsed > 0.0 { finished as f64 / elapsed } else { 0.0 };
                    let rate_str = if rate >= 1_000_000.0 {
                        format!("{:.1}M/s", rate / 1_000_000.0)
                    } else if rate >= 1_000.0 {
                        format!("{:.1}K/s", rate / 1_000.0)
                    } else {
                        format!("{:.0}/s", rate)
                    };
                    // ok% and errors use op counts (cycles_total), not source items
                    let ops_completed = progress_metrics.cycles_completed();
                    let successes = progress_metrics.successes_total.get();
                    let ok_pct = if ops_completed > 0 {
                        successes as f64 * 100.0 / ops_completed as f64
                    } else {
                        100.0
                    };
                    let errors = progress_metrics.errors_total.get();
                    let failed_ops = ops_completed.saturating_sub(successes).saturating_sub(
                        progress_metrics.skips_total.get());
                    let retries = errors.saturating_sub(failed_ops);
                    // Collect adapter-specific status counters (e.g., rows/s).
                    // Uses status_counters() which reads cumulative atomics
                    // without draining the delta timer pipeline.
                    let mut adapter_status = String::new();
                    if let Some(ref disps) = *progress_metrics.dispensers.lock().unwrap_or_else(|e| e.into_inner()) {
                        for disp in disps.iter() {
                            for (name, total) in disp.status_counters() {
                                let item_rate = if elapsed > 0.0 {
                                    total as f64 / elapsed
                                } else { 0.0 };
                                let rate_str = if item_rate >= 1_000_000.0 {
                                    format!("{:.1}M", item_rate / 1_000_000.0)
                                } else if item_rate >= 1_000.0 {
                                    format!("{:.1}K", item_rate / 1_000.0)
                                } else {
                                    format!("{:.0}", item_rate)
                                };
                                adapter_status.push_str(&format!(" {name}:{rate_str}/s"));
                            }
                        }
                    }
                    // Compute avg rows/batch from adapter counters
                    let stanzas = progress_metrics.stanzas_total.get();
                    let mut batch_info = String::new();
                    if stanzas > 0 {
                        if let Some(ref disps) = *progress_metrics.dispensers.lock().unwrap_or_else(|e| e.into_inner()) {
                            for disp in disps.iter() {
                                for (name, total) in disp.status_counters() {
                                    if name == "rows_inserted" && total > stanzas {
                                        let avg = total as f64 / stanzas as f64;
                                        batch_info = format!(" rows/batch:{avg:.1}");
                                    }
                                }
                            }
                        }
                    }
                    let concurrency = activity_concurrency;
                    // Inline recall aggregates (recall@10 etc.) as a
                    // key metric when the workload declares relevancy.
                    // Rendered as percent — relevancy values are
                    // fractions in [0, 1] and operators read these
                    // visually, not numerically.
                    let mut relevancy_str = String::new();
                    for live in progress_metrics.collect_relevancy_live() {
                        relevancy_str.push_str(&format!(
                            " {}:{:.2}%",
                            live.name,
                            live.total_mean * 100.0,
                        ));
                    }
                    // Pre-map sequence prefix `[N/total]` and
                    // scope-tree depth — both looked up in the
                    // same single tree-walk so the status line
                    // carries the same numbering the TUI tree
                    // row and post-run summary use, plus an
                    // indent matching the phase's nesting depth
                    // (SRD 18b §"Iteration variables as scope
                    // outputs"). The indent is the visual cue
                    // for "where in the scope hierarchy this
                    // running phase sits" without re-emitting
                    // the ancestor scope chain on every tick.
                    let (seq_prefix, depth_indent) = crate::scene_tree::current()
                        .and_then(|t| {
                            // Activity name has the leaf coord
                            // appended; the scene-tree phase
                            // node was registered by phase_name
                            // alone — match the prefix before
                            // the first `(`.
                            let bare_name = activity_name
                                .split_once(" (")
                                .map(|(n, _)| n)
                                .unwrap_or(&activity_name);
                            let node = t.dfs_phases()
                                .find(|n| n.name == bare_name
                                    && matches!(n.status,
                                        crate::scene_tree::PhaseStatus::Running))?
                                .clone();
                            let seq = node.seq?;
                            // SceneNode depth counts the synthetic
                            // root as 1; subtract so top-level
                            // entries land at depth 0 (matches
                            // the TUI / post-run summary indent
                            // basis).
                            let depth = node.depth.saturating_sub(1);
                            Some((
                                format!("[{seq}/{}] ", t.total_phases()),
                                "  ".repeat(depth),
                            ))
                        })
                        .unwrap_or_default();
                    let _ = (pending, active, completed);
                    // Visual progress affordances:
                    // - Braille spinner glyph (cycles every tick)
                    //   that flips to a green ✓ on terminal-state
                    //   completion (the activity-end DONE summary
                    //   carries the check, not this in-place line).
                    // - 10-char braille completion bar driven by
                    //   `pct` — 80 sub-levels via per-char dot
                    //   patterns, so the bar fills smoothly on
                    //   long phases without flicker.
                    // - ETA computed from rate × remaining work,
                    //   formatted as `Ns` / `NmMMs` / `NhMMm`.
                    //   Shown only when total_extent > 0 AND rate
                    //   > 0; otherwise omitted (better blank than
                    //   misleadingly-precise on a stalled
                    //   activity).
                    let color = crate::observer::use_color();
                    let cyan = color.then(|| "\x1b[36m").unwrap_or("");
                    let dim = color.then(|| "\x1b[2m").unwrap_or("");
                    let reset = color.then(|| "\x1b[0m").unwrap_or("");
                    let spinner = spinner_frame(tick);
                    let bar = if total_extent > 0 {
                        format!(" {dim}{}{reset}", braille_bar(pct, 10))
                    } else {
                        String::new()
                    };
                    let eta = if total_extent > 0 && rate > 0.0 {
                        let remaining = total_extent.saturating_sub(finished) as f64;
                        format!(" {dim}ETA {}{reset}", format_eta(remaining / rate))
                    } else {
                        String::new()
                    };
                    let line = format!(
                        "{depth_indent}{cyan}{spinner}{reset}{bar} {seq_prefix}{activity_name} {pct:.0}% {rate_str} ok:{ok_pct:.0}% e:{errors} r:{retries} c:{concurrency}{adapter_status}{batch_info}{relevancy_str}{eta}"
                    );
                    let cols = terminal_cols().unwrap_or(200);
                    let truncated = truncate_to_width(&line, cols.saturating_sub(1));
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
            let field_pulls_outer = field_pulls_per_template.clone();
            let pull_plans_outer = pull_plans_per_template.clone();
            let bind_plans_outer = bind_plans_per_template.clone();
            let batch_configs_outer = batch_configs_per_template.clone();
            let op_builder_outer = op_builder.clone();
            let rate_limiter_outer = rate_limiter.clone();
            let phase_arc_outer = phase_name_arc.clone();
            Box::new(move |stop: crate::fiber_pool::StopFlag| {
                let activity = activity.clone();
                let dispensers = dispensers_outer.clone();
                let field_pulls = field_pulls_outer.clone();
                let pull_plans = pull_plans_outer.clone();
                let bind_plans = bind_plans_outer.clone();
                let batch_configs = batch_configs_outer.clone();
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
                                activity, dispensers, field_pulls, pull_plans,
                                bind_plans, batch_configs, op_builder,
                                rate_limiter, stop,
                            ).await;
                        },
                    );
                    let result = std::panic::AssertUnwindSafe(body).catch_unwind().await;
                    match result {
                        Ok(()) => {
                            // Per-fiber exit logging stays at
                            // Debug — useful for drain-loop
                            // diagnostics, filtered out of the
                            // TUI log panel by default so it
                            // doesn't drown out the signal.
                            crate::diag!(crate::observer::LogLevel::Debug,
                                "fiber exit (normal) in activity '{}'",
                                activity_name_for_log);
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

        // Print final completion line. Honors the live
        // suppression flag — if the TUI is still displaying we
        // skip; if it dismissed mid-run we emit. The other
        // guards remain static (TTY presence, source extent,
        // adapter type) since those don't change during a run.
        if is_stderr_tty && total_extent > 1000 && !suppress_progress
            && !activity.config.suppress_status_line.load(Ordering::Relaxed)
        {
            let consumed = activity.source_factory.global_consumed();
            let ops_completed = activity.metrics.cycles_completed();
            let successes = activity.metrics.successes_total.get();
            let errors = activity.metrics.errors_total.get();
            let ok_pct = if ops_completed > 0 { successes as f64 * 100.0 / ops_completed as f64 } else { 100.0 };
            let elapsed = start_time.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 { consumed as f64 / elapsed } else { 0.0 };
            let rate_str = if rate >= 1_000_000.0 {
                format!("{:.1}M/s", rate / 1_000_000.0)
            } else if rate >= 1_000.0 {
                format!("{:.1}K/s", rate / 1_000.0)
            } else {
                format!("{:.0}/s", rate)
            };
            let failed_ops = ops_completed.saturating_sub(successes).saturating_sub(
                activity.metrics.skips_total.get());
            let retries = errors.saturating_sub(failed_ops);
            // Terminal: clear the in-place progress line and
            // print the final DONE summary on its own line. The
            // `\r\x1b[K` is load-bearing — the progress thread
            // has been overwriting a single line via `\r`, and
            // a plain `eprintln!` here would leave the partial
            // progress as visual cruft above the DONE line.
            eprint!("\r\x1b[K");
            // Logging plane: same content through `observer::log`
            // so the session.log file captures the DONE summary
            // alongside everything else.
            //
            // Indented by scope depth so the line nests under the
            // phase startup row (which the LogOnlyObserver
            // already emits at the same depth). The cursor name
            // (`cursor=q`) is dropped — it's a workload-internal
            // detail, not user-facing — and the activity_name's
            // leaf coord is sufficient to disambiguate concurrent
            // phases sharing a name.
            let depth_indent = crate::scene_tree::running_phase_indent();
            let color = crate::observer::use_color();
            let dim = color.then(|| "\x1b[2m").unwrap_or("");
            let yellow = color.then(|| "\x1b[33m").unwrap_or("");
            let green = color.then(|| "\x1b[32m").unwrap_or("");
            let reset = color.then(|| "\x1b[0m").unwrap_or("");
            // Throughput / ok-rate / error counters, with the
            // dim items-count tail. Yellow on error / retry
            // counts when non-zero; otherwise dim. Leading ✓
            // replaces the in-place spinner glyph from the
            // progress thread above — same line position, same
            // visual rhythm, but signals "activity reached its
            // terminal cycle" at a glance.
            let err_color = if errors > 0 || retries > 0 { yellow } else { dim };
            crate::diag!(crate::observer::LogLevel::Info,
                "{depth_indent}{green}✓{reset} {activity_name} {rate_str} ok:{ok_pct:.1}% {err_color}e:{errors} r:{retries}{reset} {dim}({consumed} items){reset}");
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

                final_snapshot.insert_counter(
                    "validations_passed",
                    activity_labels.clone(),
                    vm.passed(),
                    now,
                );
                final_snapshot.insert_counter(
                    "validations_failed",
                    activity_labels.clone(),
                    vm.failed(),
                    now,
                );

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
                        for (stat, val) in [("mean", mean), ("p50", p50), ("p99", p99), ("min", min), ("max", max)] {
                            final_snapshot.insert_gauge(
                                format!("{name}.{stat}"),
                                activity_labels.with("n", &n.to_string()),
                                val,
                                now,
                            );
                        }
                    }
                }
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
// `field_pulls`: per-template names that must populate
// `ResolvedFields` for the inner adapter (op-field bind points only).
// `pull_plans`: per-template wrapper-side `PullPlan`s, sealed at init.
// Drives cycle-time reads for validation / conditional / throttle
// wrappers via memoized `PullHandle`s. See SRD 31 §"Pull plan vs bind
// plan".
async fn executor_task(
    activity: Arc<Activity>,
    dispensers: Arc<Vec<Arc<dyn OpDispenser>>>,
    field_pulls: Arc<Vec<Vec<String>>>,
    pull_plans: Arc<Vec<crate::fixture::PullPlan>>,
    bind_plans: Arc<Vec<Option<crate::synthesis::BindPlan>>>,
    batch_configs: Arc<Vec<crate::synthesis::BatchConfig>>,
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
            // Wrap synchronous GK eval in catch_unwind so a node-
            // level panic at cycle time becomes a phase-stopping
            // error with full context instead of crashing the
            // runtime. The wrapper cannot recover — fiber state
            // may be in a partially-mutated state — so we set the
            // stop flag and break the fiber's loop.
            let fields = match std::panic::catch_unwind(
                std::panic::AssertUnwindSafe(|| {
                    fiber.resolve_cached(
                        template,
                        &field_pulls[template_idx],
                        bind_plans[template_idx].as_ref(),
                        &batch_configs[template_idx],
                    )
                })
            ) {
                Ok(f) => f,
                Err(payload) => {
                    let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "<non-string panic payload>".to_string()
                    };
                    let full = format!(
                        "GK eval panic at cycle {cycle}: {msg}"
                    );
                    activity.metrics.errors_total.inc();
                    activity.metrics.count_error_type("gk_eval_panic");
                    activity.stop_flag.store(true, Ordering::Relaxed);
                    if let Ok(mut slot) = activity.stop_reason.lock()
                        && slot.is_none()
                    {
                        *slot = Some(format!("[gk_eval_panic] {full}"));
                    }
                    break;
                }
            };

            // Resolve the wrapper-side pull plan against this
            // fiber's GkState (one indexed pull per registered
            // name, no name hashing). The resulting `pulls` is
            // disjoint from `fields`: adapters see only `fields`,
            // wrappers see only `pulls`.
            let pulls = fiber.resolve_pulls(&pull_plans[template_idx]);
            let dispenser = &dispensers[template_idx];
            let exec_ctx = crate::fixture::ExecCtx::new(&fields, &pulls);
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
                            // phase-level error can surface a real
                            // diagnostic instead of a bare "stopped
                            // by error handler". Lock-and-set-once;
                            // later fibers' errors don't overwrite.
                            if let Ok(mut slot) = activity.stop_reason.lock()
                                && slot.is_none()
                            {
                                *slot = Some(format!(
                                    "[{}] {}",
                                    inner.error_name, inner.message,
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
                    for (name, value) in captures {
                        fiber.capture(&name, value);
                    }
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

/// Inline-status spinner glyph — the standard 10-frame braille
/// spinner cycle. Picks a frame deterministically from `tick %
/// 10` so the in-place rewrite at `\r\x1b[K{spinner} …` looks
/// like a smooth animation as long as the progress thread fires
/// at a steady cadence.
fn spinner_frame(tick: u64) -> char {
    static FRAMES: [char; 10] = [
        '⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏',
    ];
    FRAMES[(tick as usize) % FRAMES.len()]
}

/// 10-character braille completion bar. `pct` is clamped to
/// [0, 100]; each of the 10 chars represents 10 percentage
/// points, with 8 within-char sub-levels via the standard
/// bottom-up braille fill pattern (so the bar fills smoothly
/// at ~1.25-percent resolution).
fn braille_bar(pct: f64, width: usize) -> String {
    static FILL: [char; 9] = [
        '\u{2800}', // ⠀  empty
        '\u{2840}', // ⡀  +dot 7
        '\u{28C0}', // ⣀  +dot 8
        '\u{28C4}', // ⣄  +dot 3
        '\u{28E4}', // ⣤  +dot 6
        '\u{28E6}', // ⣦  +dot 2
        '\u{28F6}', // ⣶  +dot 5
        '\u{28F7}', // ⣷  +dot 1
        '\u{28FF}', // ⣿  full (+dot 4)
    ];
    if width == 0 { return String::new(); }
    let bounded = pct.clamp(0.0, 100.0);
    let total = (bounded / 100.0 * (width as f64) * 8.0).round() as usize;
    let total = total.min(width * 8);
    let full = total / 8;
    let part = total % 8;
    let mut s = String::with_capacity(width * 3);
    for _ in 0..full { s.push(FILL[8]); }
    if full < width {
        s.push(FILL[part]);
        for _ in (full + 1)..width { s.push(FILL[0]); }
    }
    s
}

/// Format a remaining-time ETA. Compact ladder: under a minute
/// → `Ns`; under an hour → `NmMMs`; otherwise → `NhMMm`. Returns
/// `—` for non-finite or negative inputs (rate stalled, etc.) so
/// the in-place status line never lies about timing.
fn format_eta(remaining_secs: f64) -> String {
    if !remaining_secs.is_finite() || remaining_secs < 0.0 {
        return "—".to_string();
    }
    let secs = remaining_secs.round() as u64;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    }
}

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
        fn map_op(&self, _template: &nbrs_workload::model::ParsedOp)
            -> Result<Box<dyn OpDispenser>, String> {
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
        fn map_op(&self, _template: &nbrs_workload::model::ParsedOp)
            -> Result<Box<dyn OpDispenser>, String> {
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

    /// Build a minimal GK program (single identity node) for tests.
    fn test_program() -> Arc<nbrs_variates::kernel::GkProgram> {
        use nbrs_variates::assembly::{GkAssembler, WireRef};
        use nbrs_variates::nodes::identity::Identity;
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("id", Box::new(Identity::new()), vec![WireRef::input("cycle")]);
        asm.add_output("id", WireRef::node("id"));
        asm.compile().unwrap().into_program()
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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

        assert_eq!(shared_metrics.cycles_total.get(), 50);
        let frame = shared_metrics.capture(std::time::Duration::from_secs(1));
        assert!(!frame.is_empty());
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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;
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
        activity.run_with_driver(Arc::new(adapter), Arc::new(crate::synthesis::OpBuilder::from_program(test_program()))).await;

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
