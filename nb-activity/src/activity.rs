// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Activity: the unit of concurrent execution.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use nb_errorhandler::ErrorRouter;
use nb_metrics::instruments::counter::Counter;
use nb_metrics::instruments::histogram::Histogram;
use nb_metrics::instruments::timer::Timer;
use nb_metrics::labels::Labels;
use nb_metrics::snapshot::MetricSet;
use nb_rate::RateLimiter;

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
    pub cycle_rate: Option<f64>,
    pub stanza_rate: Option<f64>,
    pub sequencer: SequencerType,
    pub error_spec: String,
    pub max_retries: u32,
    /// Maximum number of ops within a stanza that execute concurrently.
    pub stanza_concurrency: usize,
    /// Source factory for data-driven phases. When present, fibers pull
    /// from this source instead of the cycle counter. Each fiber creates
    /// its own reader via `create_reader()`.
    pub source_factory: Option<Arc<dyn nb_variates::source::DataSourceFactory>>,
    /// Suppress the inline stderr progress line (TUI handles display).
    pub suppress_status_line: bool,
}

impl Default for ActivityConfig {
    fn default() -> Self {
        Self {
            name: "default".into(),
            cycles: 1,
            concurrency: 1,
            cycle_rate: None,
            stanza_rate: None,
            sequencer: SequencerType::Bucket,
            error_spec: ".*:warn,stop".into(),
            max_retries: 3,
            stanza_concurrency: 1,
            source_factory: None,
            suppress_status_line: false,
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
        Self {
            service_time: Timer::new(labels.with("name", "cycles_servicetime")),
            wait_time: Timer::new(labels.with("name", "cycles_waittime")),
            response_time: Timer::new(labels.with("name", "cycles_responsetime")),
            result_success_time: Timer::new(labels.with("name", "result_success")),
            tries_histogram: Histogram::new(labels.with("name", "tries")),
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
        use nb_metrics::snapshot::split_name_label;
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

impl nb_metrics::component::InstrumentSet for ActivityMetrics {
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
            nb_metrics::snapshot::split_name_label(l)
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
                    use nb_metrics::snapshot::{MetricType, MetricValue};
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
        use nb_metrics::snapshot::split_name_label as split;
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
                    use nb_metrics::snapshot::{MetricType, MetricValue};
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
    source_factory: Arc<dyn nb_variates::source::DataSourceFactory>,
    /// Resolved workload parameters (constant per run).
    pub workload_params: Arc<std::collections::HashMap<String, String>>,
    /// Shared flag: set to true when a `stop` error handler fires.
    /// All fibers check this and exit their loop when set.
    pub stop_flag: Arc<std::sync::atomic::AtomicBool>,
    /// Final validation metrics frame, populated after all cycles complete.
    /// Read by the metrics capture thread after the activity finishes.
    pub validation_frame: Arc<std::sync::Mutex<Option<MetricSet>>>,
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
        // Labels come from the component tree (parent_labels).
        // The activity name is for display only, not a metric dimension.
        let labels = parent_labels.clone();
        let metrics = Arc::new(ActivityMetrics::new(&labels));
        let error_router = ErrorRouter::parse(&config.error_spec)
            .unwrap_or_else(|e| {
                crate::diag!(crate::observer::LogLevel::Warn, "warning: invalid error spec '{}': {e}; using default (warn,stop)", config.error_spec);
                ErrorRouter::default_stop()
            });
        // All phases go through sources. cycles: N desugars to range(0, N).
        // Named cursors in GK provide their own factory via config.source_factory.
        let source_factory: Arc<dyn nb_variates::source::DataSourceFactory> = config.source_factory
            .clone()
            .unwrap_or_else(|| Arc::new(
                nb_variates::source::RangeSourceFactory::named("cycles", 0, config.cycles)
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
            validation_frame: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Get a shared reference to the metrics for external capture.
    pub fn shared_metrics(&self) -> Arc<ActivityMetrics> {
        self.metrics.clone()
    }

    /// Run the activity with a single adapter for all ops.
    pub async fn run_with_driver(
        self,
        adapter: Arc<dyn DriverAdapter>,
        program: Arc<nb_variates::kernel::GkProgram>,
    ) -> bool {
        let mut adapters = std::collections::HashMap::new();
        let name = adapter.name().to_string();
        adapters.insert(name.clone(), adapter);
        self.run_with_adapters(adapters, &name, program).await
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
        program: Arc<nb_variates::kernel::GkProgram>,
    ) -> bool {
        let activity = Arc::new(self);

        // Init time: map each template to a dispenser from its adapter,
        // then wrap with result traverser for consumption/capture
        let templates = activity.op_sequence.templates();

        // Validate all bind points are resolvable before execution
        if let Err(e) = crate::synthesis::validate_bind_points(templates, &program) {
            crate::diag!(crate::observer::LogLevel::Error, "error: {e}");
            return true;
        }

        let traversal_stats = Arc::new(crate::wrappers::TraversalStats {
            metrics: activity.metrics.clone(),
        });
        let mut dispensers: Vec<Arc<dyn OpDispenser>> = Vec::new();
        let mut validation_metrics: Vec<Arc<validation::ValidationMetrics>> = Vec::new();
        let mut extra_bindings_per_template: Vec<Vec<String>> = Vec::new();
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
            match adapter.map_op(template) {
                Ok(d) => {
                    let raw = Arc::from(d);
                    // Wrap with traversal (innermost)
                    let traversed = crate::wrappers::TraversingDispenser::wrap(
                        raw, template, traversal_stats.clone(),
                    );
                    // Wrap with delay — only if template has `delay:`
                    let throttled = if let Some(ref delay_name) = template.delay {
                        let name = delay_name.trim()
                            .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                            .unwrap_or(delay_name.trim());
                        crate::wrappers::ThrottleDispenser::wrap(traversed, name)
                    } else {
                        traversed
                    };
                    // Wrap with validation — only if template declares it
                    let (validated, vm) = crate::validation::ValidatingDispenser::wrap(
                        throttled, template, &activity.labels, Some(&program),
                    );
                    if let Some(vm) = vm {
                        validation_metrics.push(vm);
                    }
                    // Wrap with condition check — only if template has `if:`
                    let conditional = if let Some(ref cond) = template.condition {
                        let cond_name = cond.trim()
                            .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                            .unwrap_or(cond.trim());
                        crate::wrappers::ConditionalDispenser::wrap(
                            validated, cond_name, activity.metrics.clone(),
                        )
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
                        let metric_name = template.params.get("poll_metric_name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let (dispenser, poll_metrics) =
                            crate::wrappers::PollingDispenser::wrap(conditional, interval, timeout, metric_name);
                        crate::diag!(crate::observer::LogLevel::Debug,
                            "  op '{}': polling enabled (interval={}ms, timeout={}ms)",
                            template.name, interval, timeout);
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

                    // Collect extra bindings: statement bind points + validation + condition + delay.
                    // Bind points must be in extras so resolve_with_extras pulls their
                    // typed GK values into the ResolvedFields for prepared binding.
                    let mut extras = Vec::new();
                    for value in template.op.values() {
                        if let Some(s) = value.as_str() {
                            for name in nb_workload::bindpoints::referenced_bindings(s) {
                                if !extras.contains(&name) {
                                    extras.push(name);
                                }
                            }
                        }
                    }
                    extras.extend(validation::extra_bindings(template));
                    for opt_field in [&template.condition, &template.delay] {
                        if let Some(field) = opt_field {
                            let name = field.trim()
                                .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                                .unwrap_or(field.trim());
                            if !extras.contains(&name.to_string()) {
                                extras.push(name.to_string());
                            }
                        }
                    }
                    extra_bindings_per_template.push(extras);

                    // Pre-build the bind plan and batch config once per template.
                    // These were previously built per-cycle inside resolve_with_extras.
                    let stmt_field = template.op.get("stmt")
                        .or_else(|| template.op.get("prepared"))
                        .or_else(|| template.op.get("raw"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let bind_names = nb_workload::bindpoints::referenced_bindings(stmt_field);
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
        let extra_bindings_per_template = Arc::new(extra_bindings_per_template);
        let bind_plans_per_template = Arc::new(bind_plans_per_template);
        let batch_configs_per_template = Arc::new(batch_configs_per_template);
        let validation_metrics = Arc::new(validation_metrics);
        // Share the validation-metrics handle with ActivityMetrics so
        // the progress thread (below) can read live relevancy aggregates.
        activity.metrics.set_validation_metrics(validation_metrics.clone());

        let cycle_rl = activity.config.cycle_rate.map(|r| {
            Arc::new(RateLimiter::start(nb_rate::RateSpec::new(r)))
        });
        let stanza_rl = activity.config.stanza_rate.map(|r| {
            Arc::new(RateLimiter::start(nb_rate::RateSpec::new(r)))
        });

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
        if is_stderr_tty && total_extent > 1000 && !suppress_progress && !activity.config.suppress_status_line {
            let flag = progress_flag.clone();
            let progress_metrics = activity.metrics.clone();
            let start_time = start_time;
            let activity_name_progress = activity_name.clone();
            let cursor_name_progress = cursor_name.clone();
            let activity_concurrency = activity.config.concurrency;
            std::thread::spawn(move || {
                let activity_name = activity_name_progress;
                let cursor_name = cursor_name_progress;
                while flag.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(500));
                    if !flag.load(Ordering::Relaxed) { break; }
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
                    let mut relevancy_str = String::new();
                    for live in progress_metrics.collect_relevancy_live() {
                        relevancy_str.push_str(&format!(
                            " {}:{:.3}(last{}:{:.3})",
                            live.name, live.total_mean, live.window_len, live.window_mean,
                        ));
                    }
                    eprint!("\r\x1b[K{activity_name}{cursor_name} pending:{pending} active:{active} complete:{completed} of {total_extent} {pct:.2}% {rate_str} ok:{ok_pct:.1}% errors:{errors} retries:{retries} concurrency:{concurrency}{adapter_status}{batch_info}{relevancy_str}");
                }
            });
        }

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for _task_id in 0..activity.config.concurrency {
            let activity = activity.clone();
            let dispensers = dispensers.clone();
            let extra_bindings = extra_bindings_per_template.clone();
            let bind_plans = bind_plans_per_template.clone();
            let batch_configs = batch_configs_per_template.clone();
            let program = program.clone();
            let cycle_rl = cycle_rl.clone();
            let stanza_rl = stanza_rl.clone();

            handles.push(tokio::spawn(async move {
                executor_task(
                    activity, dispensers, extra_bindings, bind_plans, batch_configs,
                    program, cycle_rl, stanza_rl,
                ).await;
            }));
        }

        for handle in handles {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    crate::diag!(crate::observer::LogLevel::Error, "error: executor fiber panicked: {e}");
                } else {
                    crate::diag!(crate::observer::LogLevel::Error, "error: executor fiber failed: {e}");
                }
                activity.metrics.errors_total.inc();
                activity.stop_flag.store(true, Ordering::Relaxed);
            }
        }

        // Print final completion line
        if is_stderr_tty && total_extent > 1000 && !suppress_progress && !activity.config.suppress_status_line {
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
            eprintln!("\r\x1b[K{activity_name}{cursor_name} DONE ({consumed} items) {rate_str} ok:{ok_pct:.1}% errors:{errors} retries:{retries}");
        }

        // Signal the progress thread to stop.
        progress_flag.store(false, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(10));

        // Print validation summary AND capture to MetricsFrame in one pass.
        // snapshot() drains the histogram (delta semantics), so we must
        // use the same snapshot for both printing and SQLite capture.
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
                        crate::diag!(crate::observer::LogLevel::Info,
                            "  {name}: mean={mean:.4} p50={p50:.4} p99={p99:.4} min={min:.4} max={max:.4} (n={n})"
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

            crate::diag!(crate::observer::LogLevel::Info,
                "validation: {} passed, {} failed",
                total_passed, total_failed
            );

            if !final_snapshot.is_empty() {
                activity.validation_frame.lock().unwrap_or_else(|e| e.into_inner())
                    .replace(final_snapshot);
            }
        }

        activity.stop_flag.load(Ordering::Relaxed)
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
async fn executor_task(
    activity: Arc<Activity>,
    dispensers: Arc<Vec<Arc<dyn OpDispenser>>>,
    extra_bindings: Arc<Vec<Vec<String>>>,
    bind_plans: Arc<Vec<Option<crate::synthesis::BindPlan>>>,
    batch_configs: Arc<Vec<crate::synthesis::BatchConfig>>,
    program: Arc<nb_variates::kernel::GkProgram>,
    cycle_rl: Option<Arc<RateLimiter>>,
    stanza_rl: Option<Arc<RateLimiter>>,
) {
    use crate::synthesis::FiberBuilder;

    let stanza_len = activity.op_sequence.stanza_length() as u64;
    let max_retries = activity.config.max_retries;
    let mut fiber = FiberBuilder::new(program.clone());

    // Create per-fiber source reader (used for all phases).
    // Source-declared phases will eventually use the advancer model,
    // but for now all phases go through the source reader.
    let mut source = activity.source_factory.create_reader();

    loop {
        if activity.stop_flag.load(std::sync::atomic::Ordering::Relaxed) { break; }

        // Phase 1: RESERVE — CAS on shared cursor, instantaneous.
        // Acquires one stanza's worth of ordinals. This is the only
        // shared-state interaction per stanza.
        let range = match source.reserve(stanza_len as usize) {
            Some(r) => r,
            None => break, // source exhausted
        };

        if let Some(srl) = &stanza_rl {
            srl.acquire().await;
        }
        activity.metrics.stanzas_total.inc();
        fiber.reset_captures();

        // Phase 2: RENDER + EXECUTE — fiber-local, no contention.
        // Each op resolves via this fiber's GK instance, then
        // dispatches to the adapter. Sequential in declaration order.
        for ordinal in range.clone() {
            if activity.stop_flag.load(Ordering::Relaxed) { break; }

            // Mark op as active from render through result join.
            // "Active" means this fiber is working on an op — resolving
            // fields, waiting for the adapter, or recording results.
            activity.metrics.ops_started.fetch_add(1, Ordering::Relaxed);

            // Render the source item (fiber-local, no shared state)
            let item = source.render_item(ordinal);
            let cycle = ordinal;

            let wait_start = Instant::now();
            if let Some(crl) = &cycle_rl {
                crl.acquire().await;
            }
            let wait_nanos = wait_start.elapsed().as_nanos() as u64;

            let (template_idx, template) = activity.op_sequence.get_with_index(cycle);
            fiber.set_source_item(&item);
            let fields = fiber.resolve_with_extras_cached(
                template,
                &extra_bindings[template_idx],
                bind_plans[template_idx].as_ref(),
                &batch_configs[template_idx],
            );

            // Execute — dispatch to adapter and await result
            let dispenser = &dispensers[template_idx];
            let service_start = Instant::now();
            let mut tries = 1u32;
            let (success, captures, skipped) = loop {
                match dispenser.execute(cycle, &fields).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{OpResult, AdapterError, ExecutionError, ResolvedFields};
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
        fn map_op(&self, _template: &nb_workload::model::ParsedOp)
            -> Result<Box<dyn OpDispenser>, String> {
            Ok(Box::new(CountingDispenser { count: self.count.clone() }))
        }
    }

    struct CountingDispenser {
        count: Arc<AtomicU64>,
    }

    impl OpDispenser for CountingDispenser {
        fn execute<'a>(&'a self, _cycle: u64, _fields: &'a ResolvedFields)
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
        fn map_op(&self, _template: &nb_workload::model::ParsedOp)
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
        fn execute<'a>(&'a self, _cycle: u64, _fields: &'a ResolvedFields)
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
    fn test_program() -> Arc<nb_variates::kernel::GkProgram> {
        use nb_variates::assembly::{GkAssembler, WireRef};
        use nb_variates::nodes::identity::Identity;
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
        let ops = vec![nb_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "test"), seq);

        let (adapter, count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), test_program()).await;

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
        let ops = vec![nb_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let (adapter, total_calls) = FailThenSucceedDriverAdapter::new(2);
        activity.run_with_driver(Arc::new(adapter), test_program()).await;

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
        let ops = vec![nb_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let shared_metrics = activity.shared_metrics();

        let (adapter, _count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), test_program()).await;

        assert_eq!(shared_metrics.cycles_total.get(), 50);
        let frame = shared_metrics.capture(std::time::Duration::from_secs(1));
        assert!(!frame.is_empty());
    }

    #[tokio::test]
    async fn activity_with_cycle_rate() {
        let config = ActivityConfig {
            name: "ratetest".into(),
            cycles: 10,
            concurrency: 2,
            cycle_rate: Some(10000.0),
            ..Default::default()
        };
        let ops = vec![nb_workload::model::ParsedOp::simple("op1", "test")];
        let seq = OpSequence::uniform(ops);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let (adapter, count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), test_program()).await;

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
            nb_workload::model::ParsedOp::simple("read", "SELECT"),
            nb_workload::model::ParsedOp::simple("write", "INSERT"),
        ];
        let seq = OpSequence::build(ops, &[4, 2], SequencerType::Bucket);
        let activity = Activity::new(config, &Labels::of("session", "s1"), seq);

        let (adapter, count) = CountingDriverAdapter::new();
        activity.run_with_driver(Arc::new(adapter), test_program()).await;

        assert_eq!(count.load(Ordering::Relaxed), 12);
    }
}
