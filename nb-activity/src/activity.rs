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
use nb_metrics::frame::{MetricsFrame, Sample};
use nb_metrics::labels::Labels;
use nb_rate::RateLimiter;

use crate::adapter::{DriverAdapter, OpDispenser};
use crate::cycle::CycleSource;
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
    /// Default 1 (sequential): ops execute one at a time with capture
    /// flow between them. Values > 1 allow concurrent op execution
    /// within a stanza window — captures only flow between windows.
    pub stanza_concurrency: usize,
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
            error_spec: ".*:warn,counter".into(),
            max_retries: 3,
            stanza_concurrency: 1,
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
    pub errors_total: Counter,
    pub stanzas_total: Counter,
    pub result_elements: Counter,
    pub result_bytes: Counter,
    /// Per-error-type counters, keyed by error_name.
    /// Created on demand when a new error type is first seen.
    error_type_counts: std::sync::Mutex<std::collections::HashMap<String, Counter>>,
    labels: Labels,
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
            errors_total: Counter::new(labels.with("name", "errors_total")),
            stanzas_total: Counter::new(labels.with("name", "stanzas_total")),
            result_elements: Counter::new(labels.with("name", "result_elements")),
            result_bytes: Counter::new(labels.with("name", "result_bytes")),
            error_type_counts: std::sync::Mutex::new(std::collections::HashMap::new()),
            labels: labels.clone(),
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

    /// Capture a metrics frame from the current instrument state.
    ///
    /// This snapshots (delta) all timers and reads all counters.
    pub fn capture(&self, interval: std::time::Duration) -> MetricsFrame {
        let service_snap = self.service_time.snapshot();
        let wait_snap = self.wait_time.snapshot();
        let response_snap = self.response_time.snapshot();
        let success_snap = self.result_success_time.snapshot();
        let tries_snap = self.tries_histogram.snapshot();

        let mut frame = MetricsFrame {
            captured_at: Instant::now(),
            interval,
            samples: vec![
                Sample::Timer {
                    labels: self.service_time.labels().clone(),
                    count: service_snap.count,
                    histogram: service_snap.histogram,
                },
                Sample::Timer {
                    labels: self.wait_time.labels().clone(),
                    count: wait_snap.count,
                    histogram: wait_snap.histogram,
                },
                Sample::Timer {
                    labels: self.response_time.labels().clone(),
                    count: response_snap.count,
                    histogram: response_snap.histogram,
                },
                Sample::Timer {
                    labels: self.result_success_time.labels().clone(),
                    count: success_snap.count,
                    histogram: success_snap.histogram,
                },
                Sample::Counter {
                    labels: self.cycles_total.labels().clone(),
                    value: self.cycles_total.get(),
                },
                Sample::Counter {
                    labels: self.errors_total.labels().clone(),
                    value: self.errors_total.get(),
                },
                Sample::Counter {
                    labels: self.stanzas_total.labels().clone(),
                    value: self.stanzas_total.get(),
                },
                Sample::Counter {
                    labels: self.result_elements.labels().clone(),
                    value: self.result_elements.get(),
                },
                Sample::Counter {
                    labels: self.result_bytes.labels().clone(),
                    value: self.result_bytes.get(),
                },
                Sample::Timer {
                    labels: self.tries_histogram.labels().clone(),
                    count: tries_snap.len(),
                    histogram: tries_snap,
                },
            ],
        };

        // Add per-error-type counters
        let error_counts = self.error_type_counts.lock()
            .unwrap_or_else(|e| e.into_inner());
        for counter in error_counts.values() {
            frame.samples.push(Sample::Counter {
                labels: counter.labels().clone(),
                value: counter.get(),
            });
        }

        frame
    }
}

/// A running activity.
pub struct Activity {
    pub config: ActivityConfig,
    pub labels: Labels,
    pub metrics: Arc<ActivityMetrics>,
    pub op_sequence: OpSequence,
    pub error_router: ErrorRouter,
    cycle_source: CycleSource,
    /// Resolved workload parameters (constant per run).
    pub workload_params: Arc<std::collections::HashMap<String, String>>,
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
        let labels = parent_labels.with("activity", &config.name);
        let metrics = Arc::new(ActivityMetrics::new(&labels));
        let error_router = ErrorRouter::parse(&config.error_spec)
            .unwrap_or_else(|e| {
                eprintln!("warning: invalid error spec '{}': {e}; using default (warn,count)", config.error_spec);
                ErrorRouter::default_warn_count()
            });
        let cycle_source = CycleSource::new(0, config.cycles);

        Self {
            config,
            labels,
            metrics,
            op_sequence,
            error_router,
            cycle_source,
            workload_params: Arc::new(params),
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
    ) {
        let mut adapters = std::collections::HashMap::new();
        let name = adapter.name().to_string();
        adapters.insert(name.clone(), adapter);
        self.run_with_adapters(adapters, &name, program).await;
    }

    /// Run the activity with multiple adapters (SRD 38/40).
    ///
    /// Each op template's `adapter` param selects which adapter to use.
    /// Templates without an explicit adapter use `default_adapter`.
    /// At init time: maps each template to a dispenser from the
    /// appropriate adapter. Per fiber: creates a FiberBuilder. Per
    /// cycle: resolves fields via GK, executes via dispenser.
    pub async fn run_with_adapters(
        self,
        adapters: std::collections::HashMap<String, Arc<dyn DriverAdapter>>,
        default_adapter: &str,
        program: Arc<nb_variates::kernel::GkProgram>,
    ) {
        let activity = Arc::new(self);

        // Init time: map each template to a dispenser from its adapter,
        // then wrap with result traverser for consumption/capture
        let templates = activity.op_sequence.templates();

        // Validate all bind points are resolvable before execution
        crate::synthesis::validate_bind_points(
            templates, &program,
        );

        let traversal_stats = Arc::new(crate::wrappers::TraversalStats {
            metrics: activity.metrics.clone(),
        });
        let mut dispensers: Vec<Arc<dyn OpDispenser>> = Vec::new();
        let mut validation_metrics: Vec<Arc<validation::ValidationMetrics>> = Vec::new();
        let mut extra_bindings_per_template: Vec<Vec<String>> = Vec::new();
        for template in templates {
            // Resolve adapter: per-template override or default
            let adapter_name = template.params.get("adapter")
                .and_then(|v| v.as_str())
                .or_else(|| template.params.get("driver").and_then(|v| v.as_str()))
                .unwrap_or(default_adapter);
            let adapter = match adapters.get(adapter_name) {
                Some(a) => a,
                None => {
                    eprintln!("error: unknown adapter '{adapter_name}' for op '{}'", template.name);
                    eprintln!("  available: {}", adapters.keys().cloned().collect::<Vec<_>>().join(", "));
                    return;
                }
            };

            match adapter.map_op(template) {
                Ok(d) => {
                    let raw = Arc::from(d);
                    // Wrap with traversal (innermost)
                    let traversed = crate::wrappers::TraversingDispenser::wrap(
                        raw, template, traversal_stats.clone(),
                    );
                    // Wrap with validation (outermost) — only if template declares it
                    let (final_dispenser, vm) = crate::validation::ValidatingDispenser::wrap(
                        traversed, template, &activity.labels,
                    );
                    if let Some(vm) = vm {
                        validation_metrics.push(vm);
                    }
                    dispensers.push(final_dispenser);
                    extra_bindings_per_template.push(validation::extra_bindings(template));
                }
                Err(e) => {
                    eprintln!("error: adapter.map_op failed for '{}': {e}", template.name);
                    return;
                }
            }
        }
        let dispensers = Arc::new(dispensers);
        let extra_bindings_per_template = Arc::new(extra_bindings_per_template);
        let validation_metrics = Arc::new(validation_metrics);

        // Analyze capture dependencies across the stanza sequence to
        // determine which ops can execute concurrently vs must be
        // sequenced. Uses the expanded stanza (via LUT), not just
        // the unique templates, because weighted ops repeat.
        let stanza_templates: Vec<&nb_workload::model::ParsedOp> =
            (0..activity.op_sequence.stanza_length())
            .map(|offset| activity.op_sequence.get(offset as u64))
            .collect();
        let stanza_owned: Vec<nb_workload::model::ParsedOp> =
            stanza_templates.iter().map(|t| (*t).clone()).collect();
        let dep_groups = Arc::new(crate::linearize::analyze_dependencies(&stanza_owned));
        if dep_groups.len() > 1 {
            eprintln!("linearization: {} dependency group(s) in stanza of {} ops",
                dep_groups.len(), stanza_owned.len());
        }

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
        let total_cycles = activity.config.cycles;
        let is_stderr_tty = std::io::IsTerminal::is_terminal(&std::io::stderr());
        if is_stderr_tty && total_cycles > 1000 {
            let flag = progress_flag.clone();
            let progress_metrics = activity.metrics.clone();
            std::thread::spawn(move || {
                while flag.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(500));
                    if !flag.load(Ordering::Relaxed) { break; }
                    let completed = progress_metrics.cycles_completed();
                    let pct = if total_cycles > 0 {
                        completed * 100 / total_cycles
                    } else {
                        0
                    };
                    eprint!("\r  progress: {completed}/{total_cycles} ({pct}%)    ");
                }
                // Clear the progress line when done
                eprint!("\r                                                        \r");
            });
        }

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for _task_id in 0..activity.config.concurrency {
            let activity = activity.clone();
            let dispensers = dispensers.clone();
            let extra_bindings = extra_bindings_per_template.clone();
            let dep_groups = dep_groups.clone();
            let program = program.clone();
            let cycle_rl = cycle_rl.clone();
            let stanza_rl = stanza_rl.clone();

            handles.push(tokio::spawn(async move {
                executor_task(
                    activity, dispensers, extra_bindings, dep_groups,
                    program, cycle_rl, stanza_rl,
                ).await;
            }));
        }

        for handle in handles {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    eprintln!("error: executor fiber panicked: {e}");
                } else {
                    eprintln!("error: executor fiber failed: {e}");
                }
                activity.metrics.errors_total.inc();
            }
        }

        // Signal the progress thread to stop and allow it to clear its line.
        progress_flag.store(false, Ordering::Relaxed);
        // Brief yield so the progress thread can print the clear sequence
        // before any subsequent output appears on stderr.
        std::thread::sleep(Duration::from_millis(10));

        // Print validation summary if any validation was active
        if !validation_metrics.is_empty() {
            let mut total_passed = 0u64;
            let mut total_failed = 0u64;
            for vm in validation_metrics.iter() {
                total_passed += vm.passed();
                total_failed += vm.failed();
                for (name, histo) in &vm.relevancy_histograms {
                    let snap = histo.snapshot();
                    if snap.len() > 0 {
                        let mean = snap.mean() / 10_000.0;
                        let p50 = snap.value_at_quantile(0.5) as f64 / 10_000.0;
                        let p99 = snap.value_at_quantile(0.99) as f64 / 10_000.0;
                        let min = snap.min() as f64 / 10_000.0;
                        let max = snap.max() as f64 / 10_000.0;
                        eprintln!(
                            "  {name}: mean={mean:.4} p50={p50:.4} p99={p99:.4} min={min:.4} max={max:.4} (n={})",
                            snap.len()
                        );
                    }
                }
            }
            eprintln!(
                "validation: {} passed, {} failed",
                total_passed, total_failed
            );
        }
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
    dep_groups: Arc<Vec<crate::linearize::DepGroup>>,
    program: Arc<nb_variates::kernel::GkProgram>,
    cycle_rl: Option<Arc<RateLimiter>>,
    stanza_rl: Option<Arc<RateLimiter>>,
) {
    use crate::synthesis::FiberBuilder;
    use crate::adapter::ResolvedFields;

    let stanza_len = activity.op_sequence.stanza_length() as u64;
    let max_retries = activity.config.max_retries;
    let mut fiber = FiberBuilder::new(program);

    loop {
        let Some(base_cycle) = activity.cycle_source.next_n(stanza_len) else { break };

        if let Some(srl) = &stanza_rl {
            srl.acquire().await;
        }
        activity.metrics.stanzas_total.inc();
        fiber.reset_captures(base_cycle);

        // Process stanza ops in dependency groups.
        // Track which captures have been produced so far. If a group
        // requires captures that were not produced (due to upstream
        // failure), the entire group is skipped.
        let mut available_captures: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for (group_idx, group) in dep_groups.iter().enumerate() {
            // Apply captures from previous group before resolving
            if group_idx > 0 {
                fiber.apply_captures();
            }

            // Check if this group's required captures are all available
            if !group.required_captures.is_empty() {
                let missing: Vec<&String> = group.required_captures.iter()
                    .filter(|c| !available_captures.contains(*c))
                    .collect();
                if !missing.is_empty() {
                    // Skip this group — upstream captures were not produced
                    let _skip_count = group.op_indices.len();
                    for &stanza_offset in &group.op_indices {
                        let cycle = base_cycle + stanza_offset as u64;
                        activity.metrics.errors_total.inc();
                        activity.metrics.count_error_type("upstream_capture_missing");
                        activity.error_router.handle_error(
                            "upstream_capture_missing",
                            &format!("skipped: required capture(s) {} not produced by upstream ops",
                                missing.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(", ")),
                            cycle,
                            0,
                        );
                    }
                    continue; // skip to next group
                }
            }

            // Phase 1: Resolve all ops in the group (sequential, uses GK state)
            let mut window: Vec<(u64, usize, ResolvedFields, u64)> =
                Vec::with_capacity(group.op_indices.len());
            for &stanza_offset in &group.op_indices {
                let cycle = base_cycle + stanza_offset as u64;

                let wait_start = Instant::now();
                if let Some(crl) = &cycle_rl {
                    crl.acquire().await;
                }
                let wait_nanos = wait_start.elapsed().as_nanos() as u64;

                let (template_idx, template) = activity.op_sequence.get_with_index(cycle);
                fiber.set_inputs(&[cycle]);
                let fields = fiber.resolve_with_extras(template, &extra_bindings[template_idx]);
                window.push((cycle, template_idx, fields, wait_nanos));
            }

            // Phase 2: Execute all ops in the group concurrently
            let futures: Vec<_> = window.iter().map(|(cycle, template_idx, fields, _)| {
                let dispenser = dispensers[*template_idx].clone();
                let cycle = *cycle;
                let max_retries = max_retries;
                let activity = activity.clone();
                async move {
                    let service_start = Instant::now();
                    let mut tries = 1u32;
                    loop {
                        match dispenser.execute(cycle, fields).await {
                            Ok(result) => {
                                let service_nanos = service_start.elapsed().as_nanos() as u64;
                                return (true, service_nanos, tries, result.captures);
                            }
                            Err(e) => {
                                let duration_nanos = service_start.elapsed().as_nanos() as u64;
                                let inner = e.error();
                                let detail = activity.error_router.handle_error(
                                    &inner.error_name, &inner.message, cycle, duration_nanos,
                                );
                                activity.metrics.errors_total.inc();
                                activity.metrics.count_error_type(&inner.error_name);

                                if !e.is_adapter_level() && detail.is_retryable() && tries <= max_retries {
                                    tries += 1;
                                    continue;
                                }

                                let service_nanos = service_start.elapsed().as_nanos() as u64;
                                return (false, service_nanos, tries, std::collections::HashMap::new());
                            }
                        }
                    }
                }
            }).collect();

            let results = futures::future::join_all(futures).await;

            // Phase 3: Record metrics, store captures, track produced captures
            let mut _group_all_success = true;
            for (i, (success, service_nanos, tries, captures)) in results.into_iter().enumerate() {
                let wait_nanos = window[i].3;
                activity.metrics.service_time.record(service_nanos);
                activity.metrics.wait_time.record(wait_nanos);
                activity.metrics.response_time.record(service_nanos + wait_nanos);
                activity.metrics.tries_histogram.record(tries as u64);
                if success {
                    activity.metrics.cycles_total.inc();
                    activity.metrics.result_success_time.record(service_nanos);
                    for (name, value) in captures {
                        available_captures.insert(name.clone());
                        fiber.capture(&name, value);
                    }
                } else {
                    _group_all_success = false;
                    // Failed op's captures are NOT added to available_captures.
                    // Downstream groups that need them will be skipped.
                }
            }
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
            Box::pin(async { Ok(OpResult { body: None, captures: HashMap::new() }) })
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
                    Ok(OpResult { body: None, captures: HashMap::new() })
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
        assert!(!frame.samples.is_empty());
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
