// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Activity: the unit of concurrent execution.

use std::sync::Arc;
use std::time::Instant;

use tokio::task::JoinHandle;

use nb_errorhandler::ErrorRouter;
use nb_metrics::instruments::counter::Counter;
use nb_metrics::instruments::timer::Timer;
use nb_metrics::frame::{MetricsFrame, Sample};
use nb_metrics::labels::Labels;
use nb_rate::RateLimiter;

use crate::adapter::Adapter;
use crate::cycle::CycleSource;
use crate::opseq::{OpSequence, SequencerType};
use crate::adapter::AssembledOp;

/// Configuration for an activity.
pub struct ActivityConfig {
    pub name: String,
    pub cycles: u64,
    pub concurrency: usize,
    pub cycle_rate: Option<f64>,
    pub stanza_rate: Option<f64>,
    pub sequencer: SequencerType,
    pub error_spec: String,
    pub max_retries: u32,
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
        }
    }
}

/// Standard metrics for an activity. Shared via Arc so the metrics
/// scheduler can capture snapshots while executor tasks record.
pub struct ActivityMetrics {
    pub service_time: Timer,
    pub wait_time: Timer,
    pub response_time: Timer,
    pub cycles_total: Counter,
    pub errors_total: Counter,
    pub stanzas_total: Counter,
}

impl ActivityMetrics {
    pub fn new(labels: &Labels) -> Self {
        Self {
            service_time: Timer::new(labels.with("name", "cycles_servicetime")),
            wait_time: Timer::new(labels.with("name", "cycles_waittime")),
            response_time: Timer::new(labels.with("name", "cycles_responsetime")),
            cycles_total: Counter::new(labels.with("name", "cycles_total")),
            errors_total: Counter::new(labels.with("name", "errors_total")),
            stanzas_total: Counter::new(labels.with("name", "stanzas_total")),
        }
    }

    /// Capture a metrics frame from the current instrument state.
    ///
    /// This snapshots (delta) all timers and reads all counters.
    pub fn capture(&self, interval: std::time::Duration) -> MetricsFrame {
        let service_snap = self.service_time.snapshot();
        let wait_snap = self.wait_time.snapshot();
        let response_snap = self.response_time.snapshot();

        MetricsFrame {
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
            ],
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
    cycle_source: CycleSource,
}

impl Activity {
    pub fn new(
        config: ActivityConfig,
        parent_labels: &Labels,
        op_sequence: OpSequence,
    ) -> Self {
        let labels = parent_labels.with("activity", &config.name);
        let metrics = Arc::new(ActivityMetrics::new(&labels));
        let error_router = ErrorRouter::parse(&config.error_spec)
            .unwrap_or_else(|_| ErrorRouter::default_warn_count());
        let cycle_source = CycleSource::new(0, config.cycles);

        Self {
            config,
            labels,
            metrics,
            op_sequence,
            error_router,
            cycle_source,
        }
    }

    /// Get a shared reference to the metrics for external capture.
    pub fn shared_metrics(&self) -> Arc<ActivityMetrics> {
        self.metrics.clone()
    }

    /// Run the activity to completion.
    pub async fn run<A: Adapter + 'static>(
        self,
        adapter: Arc<A>,
        build_op: Arc<dyn Fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp + Send + Sync>,
    ) {
        let activity = Arc::new(self);

        let cycle_rl = activity.config.cycle_rate.map(|r| {
            Arc::new(RateLimiter::start(nb_rate::RateSpec::new(r)))
        });
        let stanza_rl = activity.config.stanza_rate.map(|r| {
            Arc::new(RateLimiter::start(nb_rate::RateSpec::new(r)))
        });

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for _task_id in 0..activity.config.concurrency {
            let activity = activity.clone();
            let adapter = adapter.clone();
            let cycle_rl = cycle_rl.clone();
            let stanza_rl = stanza_rl.clone();
            let build_op = build_op.clone();

            handles.push(tokio::spawn(async move {
                executor_task(activity, adapter, cycle_rl, stanza_rl, build_op).await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }
    }
}

/// Execute cycles as stanza-level units.
///
/// Each iteration claims a full stanza of consecutive cycles and
/// processes them in sequence. This ensures captures from op N are
/// available to op N+1 within the same stanza. Each stanza is an
/// isolation scope — the capture context resets at stanza start.
async fn executor_task<A: Adapter>(
    activity: Arc<Activity>,
    adapter: Arc<A>,
    cycle_rl: Option<Arc<RateLimiter>>,
    stanza_rl: Option<Arc<RateLimiter>>,
    build_op: Arc<dyn Fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp + Send + Sync>,
) {
    let stanza_len = activity.op_sequence.stanza_length() as u64;
    let max_retries = activity.config.max_retries;

    if stanza_len <= 1 {
        // Single-op stanza — no capture flow needed, use fast path
        executor_task_simple(&activity, &adapter, &cycle_rl, &stanza_rl, &build_op, max_retries).await;
        return;
    }

    // Stanza-level execution: claim full stanzas atomically
    loop {
        let Some(base_cycle) = activity.cycle_source.next_n(stanza_len) else { break };

        // Rate limit at stanza boundary
        if let Some(srl) = &stanza_rl {
            srl.acquire().await;
        }
        activity.metrics.stanzas_total.inc();

        // Process each op in the stanza sequentially
        for offset in 0..stanza_len {
            let cycle = base_cycle + offset;

            let wait_start = Instant::now();
            if let Some(crl) = &cycle_rl {
                crl.acquire().await;
            }
            let wait_nanos = wait_start.elapsed().as_nanos() as u64;

            let op_template = activity.op_sequence.get(cycle);
            let op = build_op(cycle, op_template);

            let service_start = Instant::now();
            let mut retries = 0u32;

            loop {
                match adapter.execute(&op).await {
                    Ok(_result) => {
                        // Future: extract captures from result and apply
                        // to the OpBuilder's capture context here.
                        let service_nanos = service_start.elapsed().as_nanos() as u64;
                        activity.metrics.service_time.record(service_nanos);
                        activity.metrics.wait_time.record(wait_nanos);
                        activity.metrics.response_time.record(service_nanos + wait_nanos);
                        activity.metrics.cycles_total.inc();
                        break;
                    }
                    Err(e) => {
                        let duration_nanos = service_start.elapsed().as_nanos() as u64;
                        let detail = activity.error_router.handle_error(
                            &e.error_name,
                            &e.message,
                            cycle,
                            duration_nanos,
                        );
                        activity.metrics.errors_total.inc();

                        if detail.is_retryable() && retries < max_retries {
                            retries += 1;
                            continue;
                        }

                        let service_nanos = service_start.elapsed().as_nanos() as u64;
                        activity.metrics.service_time.record(service_nanos);
                        activity.metrics.wait_time.record(wait_nanos);
                        activity.metrics.response_time.record(service_nanos + wait_nanos);
                        activity.metrics.cycles_total.inc();
                        break;
                    }
                }
            }
        }
    }
}

/// Fast path for single-op stanzas (no capture flow needed).
async fn executor_task_simple<A: Adapter>(
    activity: &Arc<Activity>,
    adapter: &Arc<A>,
    cycle_rl: &Option<Arc<RateLimiter>>,
    stanza_rl: &Option<Arc<RateLimiter>>,
    build_op: &Arc<dyn Fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp + Send + Sync>,
    max_retries: u32,
) {
    loop {
        let Some(cycle) = activity.cycle_source.next() else { break };

        let stanza_len = activity.op_sequence.stanza_length() as u64;
        if let Some(srl) = stanza_rl {
            if stanza_len > 0 && cycle % stanza_len == 0 {
                srl.acquire().await;
                activity.metrics.stanzas_total.inc();
            }
        } else if stanza_len > 0 && cycle % stanza_len == 0 {
            activity.metrics.stanzas_total.inc();
        }

        let wait_start = Instant::now();
        if let Some(crl) = cycle_rl {
            crl.acquire().await;
        }
        let wait_nanos = wait_start.elapsed().as_nanos() as u64;

        let op_template = activity.op_sequence.get(cycle);
        let op = build_op(cycle, op_template);

        let service_start = Instant::now();
        let mut retries = 0u32;

        loop {
            match adapter.execute(&op).await {
                Ok(_result) => {
                    let service_nanos = service_start.elapsed().as_nanos() as u64;
                    activity.metrics.service_time.record(service_nanos);
                    activity.metrics.wait_time.record(wait_nanos);
                    activity.metrics.response_time.record(service_nanos + wait_nanos);
                    activity.metrics.cycles_total.inc();
                    break;
                }
                Err(e) => {
                    let duration_nanos = service_start.elapsed().as_nanos() as u64;
                    let detail = activity.error_router.handle_error(
                        &e.error_name,
                        &e.message,
                        cycle,
                        duration_nanos,
                    );
                    activity.metrics.errors_total.inc();

                    if detail.is_retryable() && retries < max_retries {
                        retries += 1;
                        continue;
                    }

                    let service_nanos = service_start.elapsed().as_nanos() as u64;
                    activity.metrics.service_time.record(service_nanos);
                    activity.metrics.wait_time.record(wait_nanos);
                    activity.metrics.response_time.record(service_nanos + wait_nanos);
                    activity.metrics.cycles_total.inc();
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{OpResult, AdapterError};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct CountingAdapter {
        count: AtomicU64,
    }

    impl CountingAdapter {
        fn new() -> Self { Self { count: AtomicU64::new(0) } }
        fn count(&self) -> u64 { self.count.load(Ordering::Relaxed) }
    }

    impl Adapter for CountingAdapter {
        fn execute(&self, _op: &AssembledOp) -> impl std::future::Future<Output = Result<OpResult, AdapterError>> + Send {
            self.count.fetch_add(1, Ordering::Relaxed);
            async { Ok(OpResult { success: true, status: 200, body: None }) }
        }
    }

    struct FailThenSucceed {
        fails_remaining: AtomicU64,
        total_calls: AtomicU64,
    }

    impl FailThenSucceed {
        fn new(fail_count: u64) -> Self {
            Self {
                fails_remaining: AtomicU64::new(fail_count),
                total_calls: AtomicU64::new(0),
            }
        }
    }

    impl Adapter for FailThenSucceed {
        fn execute(&self, _op: &AssembledOp) -> impl std::future::Future<Output = Result<OpResult, AdapterError>> + Send {
            self.total_calls.fetch_add(1, Ordering::Relaxed);
            let remaining = self.fails_remaining.fetch_sub(1, Ordering::Relaxed);
            async move {
                if remaining > 0 {
                    Err(AdapterError {
                        error_name: "TransientError".into(),
                        message: "temporary failure".into(),
                    })
                } else {
                    Ok(OpResult { success: true, status: 200, body: None })
                }
            }
        }
    }

    fn simple_build_op(_cycle: u64, _template: &nb_workload::model::ParsedOp) -> AssembledOp {
        AssembledOp { name: "test".into(), fields: HashMap::new() }
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

        let adapter = Arc::new(CountingAdapter::new());
        let build_op = Arc::new(simple_build_op as fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp);

        let adapter_ref = adapter.clone();
        activity.run(adapter_ref, build_op).await;

        assert_eq!(adapter.count(), 100);
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

        let adapter = Arc::new(FailThenSucceed::new(2));
        let build_op = Arc::new(simple_build_op as fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp);

        let adapter_ref = adapter.clone();
        activity.run(adapter_ref, build_op).await;

        assert_eq!(adapter.total_calls.load(Ordering::Relaxed), 3);
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

        // Get shared metrics BEFORE run consumes the activity
        let shared_metrics = activity.shared_metrics();

        let adapter = Arc::new(CountingAdapter::new());
        let build_op = Arc::new(simple_build_op as fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp);
        activity.run(adapter, build_op).await;

        // Metrics should reflect the completed run
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

        let adapter = Arc::new(CountingAdapter::new());
        let build_op = Arc::new(simple_build_op as fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp);
        let adapter_ref = adapter.clone();
        activity.run(adapter_ref, build_op).await;

        assert_eq!(adapter.count(), 10);
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
        let adapter = Arc::new(CountingAdapter::new());
        let build_op = Arc::new(simple_build_op as fn(u64, &nb_workload::model::ParsedOp) -> AssembledOp);
        activity.run(adapter.clone(), build_op).await;
        assert_eq!(adapter.count(), 12);
    }
}
