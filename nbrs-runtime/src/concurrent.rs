// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-88 Push 1 — the **headless concurrent-execution harness**.
//!
//! Runs N executions concurrently in one process, each inside its own
//! [`ExecutionContext`](crate::execution_context) — distinct `exec_id`,
//! isolated stop flag, and its own observer — so their lifecycle/log routing
//! does not collide. "Headless" per SRD-88 §4: no live display surface; each
//! execution folds its own outcome through a [`HeadlessObserver`] and the
//! harness returns the outcomes.
//!
//! Push 1 proves the per-execution routing concurrently with inline tasks. The
//! full workload executor under this harness — propagating the context into
//! the spawned per-cycle fibers, plus de-globalizing the scene tree and
//! session-root the executor reads — is the next increment (SRD-88 Push 1
//! cont. / Push 2).

use std::sync::{Arc, Mutex};

use crate::execution_context::{self, ExecutionContext};
use crate::observer::{LogLevel, PhaseProgressUpdate, RunObserver};

/// One phase's terminal record captured by a [`HeadlessObserver`].
#[derive(Clone, Debug, PartialEq)]
pub enum PhaseRecord {
    Completed { name: String, labels: String, duration_secs: f64 },
    Failed { name: String, labels: String, error: String },
}

/// What one execution produced, returned by [`run_executions_concurrent`].
#[derive(Clone, Debug)]
pub struct ExecutionOutcome {
    /// The execution's process-unique id (SRD-77 / §A2).
    pub exec_id: u64,
    /// Terminal phase records, in completion order.
    pub phases: Vec<PhaseRecord>,
    /// Diagnostic log lines this execution emitted (`level >= Info` kept;
    /// captured headless rather than displayed).
    pub logs: Vec<String>,
}

/// A headless [`RunObserver`]: captures lifecycle + log into an outcome, draws
/// nothing. Each concurrent execution gets its own, so events route here via
/// the task-local context (`observer::global_observer()` resolves to it) and
/// never to a shared global.
pub struct HeadlessObserver {
    phases: Mutex<Vec<PhaseRecord>>,
    logs: Mutex<Vec<String>>,
}

impl HeadlessObserver {
    pub fn new() -> Self {
        Self { phases: Mutex::new(Vec::new()), logs: Mutex::new(Vec::new()) }
    }

    fn take(&self) -> (Vec<PhaseRecord>, Vec<String>) {
        let p = std::mem::take(&mut *self.phases.lock().unwrap_or_else(|e| e.into_inner()));
        let l = std::mem::take(&mut *self.logs.lock().unwrap_or_else(|e| e.into_inner()));
        (p, l)
    }
}

impl Default for HeadlessObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl RunObserver for HeadlessObserver {
    fn phase_starting(&self, _name: &str, _labels: &str, _ops: usize, _cycles: u64, _conc: usize) {}

    fn phase_completed(&self, name: &str, labels: &str, duration_secs: f64) {
        self.phases.lock().unwrap_or_else(|e| e.into_inner()).push(PhaseRecord::Completed {
            name: name.to_string(),
            labels: labels.to_string(),
            duration_secs,
        });
    }

    fn phase_failed(&self, name: &str, labels: &str, error: &str) {
        self.phases.lock().unwrap_or_else(|e| e.into_inner()).push(PhaseRecord::Failed {
            name: name.to_string(),
            labels: labels.to_string(),
            error: error.to_string(),
        });
    }

    fn phase_progress(&self, _update: &PhaseProgressUpdate) {}

    fn run_finished(&self) {}

    fn log(&self, _level: LogLevel, message: &str) {
        self.logs.lock().unwrap_or_else(|e| e.into_inner()).push(message.to_string());
    }
}

/// One unit of concurrent work — a boxed future. Boxed because the tasks are
/// heterogeneous (each is a distinct `async` block) and a `Vec` needs one
/// element type.
pub type ExecutionTask = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Run ONE workload headless inside its own [`ExecutionContext`]: route the
/// run's lifecycle + log through a [`HeadlessObserver`] (no display surface),
/// then return the captured [`ExecutionOutcome`] alongside the run result.
///
/// This is the building block that proves the de-globalized run path works
/// **inside a scoped context** — the run's observer/scene-tree/stop resolve to
/// the execution's own (task-local), and the propagated per-cycle fibers
/// (SRD-88 fiber propagation) carry the context end-to-end.
///
/// Single-execution: it lets `run_with_observer` create its own session.
/// Running N of these CONCURRENTLY against ONE shared session needs the
/// run-path factoring (shared session setup vs. per-execution run) — the next
/// SRD-88 increment; the process-global `log_sink` / session dir are shared
/// once there, not created per execution.
pub async fn run_workload_headless(args: &[String]) -> (ExecutionOutcome, Result<(), String>) {
    let obs = Arc::new(HeadlessObserver::new());
    let ctx = ExecutionContext::with_observer(obs.clone() as Arc<dyn RunObserver>);
    let exec_id = ctx.exec_id;
    let result = execution_context::scope(
        ctx,
        crate::runner::run_with_observer(args, obs.clone() as Arc<dyn RunObserver>),
    )
    .await;
    let (phases, logs) = obs.take();
    (ExecutionOutcome { exec_id, phases, logs }, result)
}

/// Run each task as its own execution, concurrently, in one process — **at most
/// `max_concurrent` running at a time**. Each task runs inside its own
/// [`ExecutionContext`] (distinct `exec_id`, isolated stop flag, own
/// [`HeadlessObserver`]), so its lifecycle/log routing is isolated from its
/// siblings. Returns each execution's [`ExecutionOutcome`] in input order.
///
/// The cap (a `Semaphore`) bounds in-flight executions — e.g. a workspace test
/// running many workload examples within one session at ≤10 at a time —
/// without bounding total count. `max_concurrent == 0` is treated as `1`.
///
/// SRD-88 §4 headless: no live display. The tasks share whatever durable
/// session store the caller has set up (one per session, `exec_id`-tagged —
/// Push 2); this harness owns only the per-execution routing.
pub async fn run_executions_concurrent(
    tasks: Vec<ExecutionTask>,
    max_concurrent: usize,
) -> Vec<ExecutionOutcome> {
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1)));
    let futs = tasks.into_iter().map(|task| {
        let sem = sem.clone();
        let obs = Arc::new(HeadlessObserver::new());
        let ctx = ExecutionContext::with_observer(obs.clone() as Arc<dyn RunObserver>);
        let exec_id = ctx.exec_id;
        async move {
            // Bound in-flight executions; the permit is held for this
            // execution's whole run and released on completion.
            let _permit = sem.acquire().await.expect("semaphore not closed");
            execution_context::scope(ctx, task).await;
            let (phases, logs) = obs.take();
            ExecutionOutcome { exec_id, phases, logs }
        }
    });
    // `join_all` returns results in input order regardless of completion order.
    futures::future::join_all(futs).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_executions_route_logs_to_their_own_observer() {
        // Two executions logging concurrently. Each line must land ONLY in the
        // execution that emitted it — proving observer routing is per-execution
        // (task-local), not a shared process-global.
        let outcomes = run_executions_concurrent(vec![
            Box::pin(async {
                crate::observer::log(LogLevel::Info, "from-exec-A");
            }) as ExecutionTask,
            Box::pin(async {
                crate::observer::log(LogLevel::Info, "from-exec-B");
            }),
        ], 10)
        .await;

        assert_eq!(outcomes.len(), 2);
        assert_ne!(outcomes[0].exec_id, outcomes[1].exec_id, "distinct exec_ids");

        assert!(outcomes[0].logs.iter().any(|l| l.contains("from-exec-A")));
        assert!(!outcomes[0].logs.iter().any(|l| l.contains("from-exec-B")),
            "execution A must NOT capture B's log");
        assert!(outcomes[1].logs.iter().any(|l| l.contains("from-exec-B")));
        assert!(!outcomes[1].logs.iter().any(|l| l.contains("from-exec-A")),
            "execution B must NOT capture A's log");
    }

    #[tokio::test]
    async fn concurrent_executions_have_isolated_stop_flags() {
        // Run A and B; midway, A stops itself. B must keep observing not-stopped.
        let a_saw = Arc::new(Mutex::new(false));
        let b_saw = Arc::new(Mutex::new(true));
        let a_saw2 = a_saw.clone();
        let b_saw2 = b_saw.clone();
        let _ = run_executions_concurrent(vec![
            Box::pin(async move {
                // Stop THIS execution, then observe.
                if let Some(stop) = execution_context::current_stop() {
                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                *a_saw2.lock().unwrap() = crate::session_signals::stop_requested();
            }) as ExecutionTask,
            Box::pin(async move {
                *b_saw2.lock().unwrap() = crate::session_signals::stop_requested();
            }),
        ], 10)
        .await;
        assert!(*a_saw.lock().unwrap(), "A observes its own stop");
        assert!(!*b_saw.lock().unwrap(), "B is unaffected by A's stop");
    }

    #[tokio::test]
    async fn caps_in_flight_executions_at_max_concurrent() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // 8 executions, cap 3: no more than 3 may be in flight at once, but
        // concurrency must actually happen (not serialized).
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<ExecutionTask> = (0..8)
            .map(|_| {
                let inf = in_flight.clone();
                let pk = peak.clone();
                Box::pin(async move {
                    let now = inf.fetch_add(1, Ordering::SeqCst) + 1;
                    pk.fetch_max(now, Ordering::SeqCst);
                    // Yield so siblings interleave and the cap is exercised.
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    inf.fetch_sub(1, Ordering::SeqCst);
                }) as ExecutionTask
            })
            .collect();
        let outcomes = run_executions_concurrent(tasks, 3).await;
        assert_eq!(outcomes.len(), 8, "every execution runs to completion");
        let p = peak.load(Ordering::SeqCst);
        assert!(p <= 3, "cap of 3 must bound in-flight executions; peaked at {p}");
        assert!(p >= 2, "with 8 tasks and cap 3, executions must overlap; peaked at {p}");
    }
}
