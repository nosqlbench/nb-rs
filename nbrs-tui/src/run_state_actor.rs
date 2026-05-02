// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Display-side actor that owns [`RunState`] and publishes
//! immutable snapshots.
//!
//! See `docs/sysref/02_concurrency_model.md` §"Display and
//! Diagnostic Decoupling" for the architectural rationale. In
//! short:
//!
//! - **Downstream (core → UI):** the actor owns a private
//!   mutable `RunState`. After every applied command, it
//!   publishes a fresh `Arc<RunState>` into a shared
//!   [`arc_swap::ArcSwap`]. UI / web / OOB readers do
//!   `snapshot.load()` — a single atomic op, never a wait.
//! - **Upstream (UI → core):** every mutation is a typed
//!   [`RunStateCmd`] variant sent over an [`mpsc::Sender`]. The
//!   actor's `match` is exhaustive, so a new mutation cannot be
//!   added without the actor handling it.
//!
//! There is no shared `RwLock<RunState>` — the principle is that
//! the renderer can never wait on the writer and the writer can
//! never wait on the renderer.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use nbrs_activity::observer::PhaseProgressUpdate;
use nbrs_activity::scene_tree::SceneTree;
use nbrs_metrics::summaries::binomial_summary::BinomialSummary;
use nbrs_metrics::summaries::ewma::Ewma;
use nbrs_metrics::summaries::peak_tracker::PeakTracker;

use crate::state::{ActivePhase, LogSeverity, PhaseSummary, RunState};

/// One mutation of [`RunState`]. Every observer-driven and
/// UI-driven write is one of these variants. The actor's
/// `match` is exhaustive — adding a new mutation requires
/// adding a variant and a handler arm, by design.
#[derive(Debug)]
pub enum RunStateCmd {
    /// Replace the scenario tree wholesale (called once after
    /// pre-mapping).
    InstallTree(SceneTree),
    /// A phase has begun. Carries everything needed to seed an
    /// [`ActivePhase`] entry.
    PhaseStarting {
        name: String,
        labels: String,
        /// Count of distinct op definitions in the phase (the
        /// "shape" of the stanza; typically 1 for query workloads).
        op_templates: usize,
        /// Number of times the stanza will iterate (the "amount"
        /// of work).
        total_cycles: u64,
        concurrency: usize,
    },
    /// A phase finished cleanly. The actor builds the
    /// [`PhaseSummary`] from its own active-phase entry before
    /// removing it.
    PhaseCompleted {
        name: String,
        labels: String,
        duration_secs: f64,
    },
    /// A phase failed. The actor removes the active entry and
    /// flips the tree node's status.
    PhaseFailed {
        name: String,
        labels: String,
        error: String,
    },
    /// Live update from the executor's progress thread.
    PhaseProgress(PhaseProgressUpdate),
    /// Run is complete; flips `state.finished` so the renderer
    /// drops out of its event loop.
    RunFinished,
    /// Append a log line to the ring buffer.
    Log {
        severity: LogSeverity,
        message: String,
    },
    /// Latency frame ingest from the metrics scheduler. Updates
    /// the live percentile fields, the rolling histories, and
    /// each active phase's peak trackers.
    LatencyFrame {
        min: u64,
        p50: u64,
        p90: u64,
        p99: u64,
        p999: u64,
        max: u64,
    },
    /// Sparkline samples for the throughput / secondary-counter
    /// rows. Driven from the same metrics-frame drain as
    /// `LatencyFrame`.
    SparklineSamples {
        ops: Option<f64>,
        rows: Option<f64>,
        rows_label: Option<String>,
    },
    /// Bookkeeping fields the runner sets before the run starts
    /// (profiler / limit strings shown in the header strip).
    SetMeta {
        profiler: Option<String>,
        limit: Option<String>,
    },
    /// Synchronous render checkpoint. Processed in command order
    /// so its position in the queue marks "all preceding mutations
    /// have been applied". The actor:
    ///
    /// 1. registers `tx` in the shared `pending_acks` queue,
    /// 2. sets the `force_redraw` flag,
    /// 3. publishes a fresh snapshot,
    ///
    /// after which the TUI thread observes `force_redraw`, drops
    /// its poll timeout to zero, draws one frame against the
    /// just-published state, and drains `pending_acks` — at which
    /// point each registered `tx` is signalled and the original
    /// caller's blocking `rx.recv()` returns.
    ///
    /// Used by [`crate::observer::TuiObserver::phase_completed`] /
    /// [`crate::observer::TuiObserver::phase_failed`] to guarantee
    /// at least one fully rendered frame of the just-completed
    /// phase before the executor moves on. The `tick_rate`
    /// throttle is bypassed for this single redraw.
    FrameAck(mpsc::Sender<()>),
}

/// Synchronisation surface shared between the actor (which
/// receives [`RunStateCmd::FrameAck`]) and the TUI app (which
/// honours it). Cloned cheaply; held by both sides.
///
/// - `force_redraw` — when set, the TUI app drops its poll
///   timeout to zero so the next iteration draws immediately,
///   bypassing `tick_rate`. Cleared by the app after the draw.
/// - `pending_acks` — `tx` channels handed in via `FrameAck`.
///   Drained and signalled by the app after each `terminal.draw()`.
#[derive(Clone, Default)]
pub struct FrameSync {
    pub force_redraw: Arc<AtomicBool>,
    pub pending_acks: Arc<Mutex<Vec<mpsc::Sender<()>>>>,
}

impl FrameSync {
    /// Drain every queued ack and signal it. Called by the TUI
    /// app immediately after a successful `terminal.draw()`. The
    /// `force_redraw` flag is cleared in the same call so the
    /// app doesn't busy-spin until the next FrameAck arrives.
    pub fn signal_post_draw(&self) {
        self.force_redraw.store(false, Ordering::Release);
        let mut q = self.pending_acks.lock().unwrap_or_else(|e| e.into_inner());
        for tx in q.drain(..) {
            // Receiver may have given up (recv_timeout); drop is
            // fine.
            let _ = tx.send(());
        }
    }
}

/// Handle the rest of the system uses to talk to the actor.
///
/// Cheap to clone — every field is itself `Arc`-style. The
/// snapshot side is read with a single atomic load; the inbox
/// side is fire-and-forget (`send` returns immediately, dropped
/// silently if the actor has exited).
#[derive(Clone)]
pub struct RunStateHandle {
    snapshot: Arc<ArcSwap<RunState>>,
    inbox: mpsc::Sender<RunStateCmd>,
    frame_sync: FrameSync,
}

impl RunStateHandle {
    /// Load the current snapshot. Always returns the most
    /// recently published `Arc<RunState>` — the only way for
    /// this to return a stale snapshot is if the actor hasn't
    /// processed a sent command yet, which is the intended
    /// decoupling.
    pub fn load(&self) -> Arc<RunState> {
        self.snapshot.load_full()
    }

    /// Send a command into the actor inbox. Fire-and-forget: if
    /// the actor has exited, the send is dropped silently. The
    /// actor never falls behind in a way that pressures the
    /// caller — the caller doesn't wait for it.
    pub fn send(&self, cmd: RunStateCmd) {
        let _ = self.inbox.send(cmd);
    }

    /// Block until the TUI has rendered at least one frame that
    /// reflects every command sent on this handle prior to the
    /// call. Bypasses the TUI's `tick_rate` throttle so the
    /// caller doesn't pay up to a full tick of latency.
    ///
    /// Returns `true` if the round trip completed inside `timeout`
    /// and `false` if the TUI didn't respond (e.g. the app has
    /// already exited or is wedged). Callers should treat a
    /// `false` as best-effort: the run continues either way.
    pub fn flush_frame(&self, timeout: Duration) -> bool {
        let (tx, rx) = mpsc::channel::<()>();
        if self.inbox.send(RunStateCmd::FrameAck(tx)).is_err() {
            return false;
        }
        rx.recv_timeout(timeout).is_ok()
    }

    /// Access the shared frame-sync surface. The TUI app holds a
    /// clone and consumes it from its draw loop.
    pub fn frame_sync(&self) -> FrameSync {
        self.frame_sync.clone()
    }
}

/// Spawn the RunState actor on its own OS thread. Returns the
/// handle (used by the observer, the TUI app, the web API, and
/// any OOB introspection surface) and the thread `JoinHandle`
/// (so the runner can join cleanly at shutdown).
///
/// The actor exits when every clone of the inbox sender is
/// dropped — `Receiver::recv` returns `Err`, the loop falls
/// through, and the final state is published one more time so
/// post-shutdown readers see `finished = true`.
pub fn spawn_run_state_actor(
    initial: RunState,
) -> (RunStateHandle, JoinHandle<()>) {
    let snapshot = Arc::new(ArcSwap::new(Arc::new(initial.clone())));
    let (tx, rx) = mpsc::channel::<RunStateCmd>();
    let snapshot_for_thread = snapshot.clone();
    let frame_sync = FrameSync::default();
    let frame_sync_for_thread = frame_sync.clone();

    let handle = std::thread::Builder::new()
        .name("run-state-actor".into())
        .spawn(move || {
            let mut state = initial;
            // recv() blocks the actor thread when the inbox is
            // empty — fine, this is a dedicated OS thread, not a
            // tokio worker. SRD-02 §"No Blocking Primitives in
            // Async Contexts" only forbids blocking *inside*
            // tokio.
            while let Ok(first) = rx.recv() {
                handle_cmd(&mut state, &frame_sync_for_thread, first);
                // Coalesce: drain any further-pending commands
                // before publishing. Cuts publish cost when the
                // executor bursts updates; readers always see
                // the latest published state anyway.
                while let Ok(more) = rx.try_recv() {
                    handle_cmd(&mut state, &frame_sync_for_thread, more);
                }
                snapshot_for_thread.store(Arc::new(state.clone()));
            }
            // Final publish on shutdown — straggler readers see
            // post-shutdown state (finished = true, last logs).
            snapshot_for_thread.store(Arc::new(state));
            // Drop any acks still queued so blocked observers
            // unblock instead of waiting out their full timeout.
            frame_sync_for_thread.signal_post_draw();
        })
        .expect("spawn run-state-actor thread");

    (RunStateHandle { snapshot, inbox: tx, frame_sync }, handle)
}

/// Top-level command dispatch. Most commands route to `apply`,
/// which mutates the [`RunState`] in place. `FrameAck` is handled
/// here because its target is the [`FrameSync`] surface, not the
/// state.
fn handle_cmd(state: &mut RunState, frame_sync: &FrameSync, cmd: RunStateCmd) {
    match cmd {
        RunStateCmd::FrameAck(tx) => {
            // Register the tx so the TUI app can signal it after
            // the next draw. Setting `force_redraw` bypasses the
            // app's tick_rate so that draw fires within ~1 ms
            // rather than waiting up to a full tick.
            frame_sync.pending_acks.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(tx);
            frame_sync.force_redraw.store(true, Ordering::Release);
        }
        other => apply(state, other),
    }
}

fn apply(state: &mut RunState, cmd: RunStateCmd) {
    match cmd {
        RunStateCmd::InstallTree(tree) => {
            state.install_tree(tree);
        }
        RunStateCmd::PhaseStarting { name, labels, op_templates, total_cycles: _, concurrency } => {
            state.set_phase_running(&name, &labels, op_templates);
            let key = (name.clone(), labels.clone());
            // Sparkline capacity = bar width used by
            // latency_detail_lines so the throughput row aligns
            // with the latency rows.
            let throughput_summary = Arc::new(BinomialSummary::new(60));
            // 1 s half-life: short enough to track real
            // throughput changes, long enough to stop the raw
            // value from flickering between frames.
            let rate_ewma = Arc::new(Ewma::new(Duration::from_secs(1)));
            let latency_peak_5s = Arc::new(PeakTracker::max(Duration::from_secs(5)));
            let latency_peak_10s = Arc::new(PeakTracker::max(Duration::from_secs(10)));
            state.active_phases.insert(key, ActivePhase {
                name,
                labels,
                cursor_name: "?".into(),
                cursor_extent: 0,
                fibers: concurrency,
                started_at: Instant::now(),
                ops_started: 0,
                ops_finished: 0,
                ops_ok: 0,
                errors: 0,
                retries: 0,
                ops_per_sec: 0.0,
                adapter_counters: Vec::new(),
                rows_per_batch: 0.0,
                relevancy: Vec::new(),
                throughput_summary,
                rate_ewma,
                latency_peak_5s,
                latency_peak_10s,
            });
            // Sparklines reset on every phase boundary so a
            // short ann_query phase doesn't show several seconds
            // of rampup throughput instead of its own.
            state.ops_history.clear();
            state.rows_history.clear();
            state.rows_sparkline_label = None;
        }
        RunStateCmd::PhaseCompleted { name, labels, duration_secs } => {
            let key = (name.clone(), labels.clone());
            let min_ns = state.min_nanos;
            let p50_ns = state.p50_nanos;
            let p99_ns = state.p99_nanos;
            let max_ns = state.max_nanos;
            let summary = state.active_phases.get(&key).map(|a| PhaseSummary {
                ops_finished: a.ops_finished,
                ops_ok: a.ops_ok,
                ops_started: a.ops_started,
                errors: a.errors,
                retries: a.retries,
                fibers: a.fibers,
                ops_per_sec: a.ops_per_sec,
                min_nanos: min_ns,
                p50_nanos: p50_ns,
                p99_nanos: p99_ns,
                max_nanos: max_ns,
                cursor_name: a.cursor_name.clone(),
                cursor_extent: a.cursor_extent,
                adapter_counters: a.adapter_counters.clone(),
                rows_per_batch: a.rows_per_batch,
                cursors: std::iter::once((a.cursor_name.clone(), a.ops_finished))
                    .chain(a.adapter_counters.iter().map(|(n, t, _)| (n.clone(), *t)))
                    .collect(),
                relevancy: a.relevancy.clone(),
                // Freeze the sparkline as a durable artifact —
                // the live Arc<BinomialSummary> is dropped with
                // the ActivePhase below.
                throughput_samples: a.throughput_summary.snapshot(),
            }).unwrap_or_default();
            state.set_phase_completed(&name, &labels, duration_secs, summary);
            state.active_phases.remove(&key);
        }
        RunStateCmd::PhaseFailed { name, labels, error } => {
            state.set_phase_failed(&name, &labels, &error);
            state.active_phases.remove(&(name, labels));
        }
        RunStateCmd::PhaseProgress(update) => {
            if let Some(active) = state.active_phase_mut(&update.name, &update.labels) {
                active.cursor_name = update.cursor_name.clone();
                active.cursor_extent = update.cursor_extent;
                active.fibers = update.fibers;
                active.ops_started = update.ops_started;
                active.ops_finished = update.ops_finished;
                active.ops_ok = update.ops_ok;
                active.errors = update.errors;
                active.retries = update.retries;
                active.ops_per_sec = update.ops_per_sec;
                active.adapter_counters = update.adapter_counters.iter()
                    .map(|(n, t, r)| (n.clone(), *t, *r))
                    .collect();
                active.rows_per_batch = update.rows_per_batch;
                active.relevancy = update.relevancy.iter()
                    .map(|r| (r.name.clone(), r.window_mean, r.total_mean,
                              r.total_count, r.window_len))
                    .collect();
                active.throughput_summary.record(update.ops_per_sec);
                active.rate_ewma.record_now(update.ops_per_sec);
            }
        }
        RunStateCmd::RunFinished => {
            state.finished = true;
        }
        RunStateCmd::Log { severity, message } => {
            state.push_log(severity, message);
        }
        RunStateCmd::LatencyFrame { min, p50, p90, p99, p999, max } => {
            state.min_nanos  = min;
            state.p50_nanos  = p50;
            state.p90_nanos  = p90;
            state.p99_nanos  = p99;
            state.p999_nanos = p999;
            state.max_nanos  = max;

            const HISTORY_CAP: usize = 300; // 5 min at 1 Hz
            state.min_history.push(min);
            state.p50_history.push(p50);
            state.p90_history.push(p90);
            state.p99_history.push(p99);
            state.p999_history.push(p999);
            state.max_history.push(max);
            let trim = |h: &mut Vec<u64>| {
                if h.len() > HISTORY_CAP { h.remove(0); }
            };
            trim(&mut state.min_history);
            trim(&mut state.p50_history);
            trim(&mut state.p90_history);
            trim(&mut state.p99_history);
            trim(&mut state.p999_history);
            trim(&mut state.max_history);

            // Each active phase's peak trackers see this frame's
            // max latency. Frames are session-labeled today, so
            // every active phase observes the same max — fine
            // for single-phase scenarios; multi-phase will need
            // per-phase frame demux.
            let now = Instant::now();
            for active in state.active_phases.values() {
                active.latency_peak_5s.record(max, now);
                active.latency_peak_10s.record(max, now);
            }
        }
        RunStateCmd::SparklineSamples { ops, rows, rows_label } => {
            if let Some(o) = ops { state.push_ops_sample(o); }
            if let Some(r) = rows { state.push_rows_sample(r); }
            state.rows_sparkline_label = rows_label;
        }
        RunStateCmd::SetMeta { profiler, limit } => {
            if let Some(p) = profiler { state.profiler = p; }
            if let Some(l) = limit    { state.limit    = l; }
        }
        RunStateCmd::FrameAck(_) => {
            // Routed to `handle_cmd` before reaching this match;
            // included here only to keep the match exhaustive.
            unreachable!("FrameAck is handled in handle_cmd before apply")
        }
    }
}
