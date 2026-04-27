// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Shared state between the executor and the TUI display.
//!
//! The executor writes to `RunState` via `Arc<RwLock<RunState>>`.
//! The TUI reads it at its render cadence (4 Hz). Writes are
//! infrequent (phase transitions, not per-op) so lock contention
//! is negligible.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use nb_metrics::summaries::binomial_summary::BinomialSummary;
use nb_metrics::summaries::ewma::Ewma;
use nb_metrics::summaries::peak_tracker::PeakTracker;

/// Composite key for the active-phase map: (name, labels).
/// Matches the tuple observer callbacks already use to address a
/// specific phase iteration.
pub type PhaseKey = (String, String);

/// Log severity level for display coloring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogSeverity {
    Debug,
    Info,
    Warn,
    Error,
}

/// A log message with severity for display coloring.
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub severity: LogSeverity,
    pub message: String,
}

// `EntryKind` and `PhaseStatus` now live on the canonical
// [`nb_activity::scene_tree`] types — re-exported here so existing
// `crate::state::EntryKind::Phase` references keep working without
// touching every call site.
pub use nb_activity::scene_tree::NodeKind as EntryKind;
pub use nb_activity::scene_tree::PhaseStatus;
pub use nb_activity::scene_tree::{SceneNode, SceneNodeId, SceneTree};

/// End-of-phase metrics snapshot attached to a completed phase.
/// Mirrors the live progress bar so an expanded tree entry shows the
/// same fields a non-TUI run would print on stderr.
#[derive(Clone, Debug, Default)]
pub struct PhaseSummary {
    /// Total ops finished during the phase.
    pub ops_finished: u64,
    /// Ops that succeeded (no error after retry).
    pub ops_ok: u64,
    /// Ops started — used with `ops_finished` to compute active/pending.
    pub ops_started: u64,
    /// Errors observed (includes retries).
    pub errors: u64,
    /// Retries attempted.
    pub retries: u64,
    /// Fibers the phase was run with (concurrency).
    pub fibers: usize,
    /// Average ops/s over the phase duration.
    pub ops_per_sec: f64,
    /// Service-time percentiles in nanoseconds (latest sample).
    pub min_nanos: u64,
    pub p50_nanos: u64,
    pub p99_nanos: u64,
    pub max_nanos: u64,
    /// Primary cursor: name and total extent at phase end.
    pub cursor_name: String,
    pub cursor_extent: u64,
    /// Adapter-specific status counters: (name, total, rate) at phase end.
    pub adapter_counters: Vec<(String, u64, f64)>,
    /// Average rows per batch (if batching — else 0).
    pub rows_per_batch: f64,
    /// Count of cycles consumed from each input cursor, in the order
    /// the source dispatch produced them.
    pub cursors: Vec<(String, u64)>,
    /// Final relevancy aggregates per metric. Same shape as
    /// `ActivePhase::relevancy` but captured at phase_completed time.
    pub relevancy: Vec<(String, f64, f64, u64, usize)>,
    /// Frozen snapshot of the phase's throughput sparkline at
    /// completion — a clone of the `BinomialSummary`'s sample
    /// buffer. The detail block renders this instead of the
    /// (now-discarded) live `Arc<BinomialSummary>` so a scrolled-
    /// back completed phase still shows the shape of its
    /// throughput curve. Empty when the phase produced no
    /// samples (no `phase_progress` updates).
    pub throughput_samples: Vec<f64>,
}

/// Live metrics for the currently running phase.
#[derive(Clone, Debug)]
pub struct ActivePhase {
    pub name: String,
    pub labels: String,
    pub cursor_name: String,
    pub cursor_extent: u64,
    pub fibers: usize,
    pub started_at: Instant,

    // Snapshot counters (updated by progress thread)
    pub ops_started: u64,
    pub ops_finished: u64,
    pub ops_ok: u64,
    pub errors: u64,
    pub retries: u64,
    pub ops_per_sec: f64,

    // Adapter-specific
    pub adapter_counters: Vec<(String, u64, f64)>, // (name, total, rate)
    pub rows_per_batch: f64,

    /// Live relevancy aggregates — one entry per metric (e.g. `recall@10`).
    /// `(name, window_mean, total_mean, total_count, window_len)`
    pub relevancy: Vec<(String, f64, f64, u64, usize)>,

    /// Phase-scoped throughput sparkline storage. One sample per
    /// `phase_progress` tick; capacity caps at the sparkline's
    /// horizontal cell count so the ring never outgrows the
    /// render width. Wrapped in `Arc` so cloning `ActivePhase`
    /// (for the pause snapshot) shares the summary instead of
    /// duplicating its buffer. See SRD 62 §"Design notes →
    /// Per-phase sparkline".
    pub throughput_summary: Arc<BinomialSummary>,
    /// Smoothed cursor-advance rate. The raw `ops_per_sec` from
    /// the progress thread bounces frame-to-frame; the EWMA
    /// gives the detail-block readout a stable number that
    /// matches what a human would call "the current rate".
    pub rate_ewma: Arc<Ewma>,
    /// Rolling max latency over the last 5 seconds — drives the
    /// `╪` 5s-peak cross-bar marker on the latency range row.
    pub latency_peak_5s: Arc<PeakTracker>,
    /// Rolling max latency over the last 10 seconds — drives the
    /// `╫` 10s-peak cross-bar marker.
    pub latency_peak_10s: Arc<PeakTracker>,
}

/// A single entry rendered in the TUI's phase list — a flat
/// projection over the scene tree's DFS order. Carries the scene-
/// node id so callers can cross-reference parent / children, plus
/// the heavy `PhaseSummary` (only meaningful for completed
/// phases). Held as a `Cow`-equivalent: produced by walking the
/// tree at iter time, not stored.
#[derive(Clone, Debug)]
pub struct PhaseEntry {
    pub node_id: SceneNodeId,
    pub name: String,
    pub labels: String,
    pub status: PhaseStatus,
    pub kind: EntryKind,
    pub op_count: usize,
    pub duration_secs: Option<f64>,
    pub depth: usize,
    pub summary: Option<PhaseSummary>,
}

/// Top-level run state shared between executor and TUI.
#[derive(Clone)]
pub struct RunState {
    pub workload_file: String,
    pub scenario_name: String,
    pub adapter: String,
    pub started_at: Instant,
    pub profiler: String,
    pub limit: String,

    /// Canonical scene tree: every concrete phase + scope header
    /// wired by parent / children pointers. The pre-mapped
    /// scenario shape lands here once via `scenario_pre_mapped`;
    /// lifecycle callbacks (`set_phase_running` / `_completed` /
    /// `_failed`) mutate node statuses in place.
    pub tree: SceneTree,
    /// Heavy per-phase metrics produced at completion. Keyed by
    /// `SceneNodeId` so scope-level renderers can also look up
    /// child summaries cheaply. Side-map (rather than baked into
    /// `SceneNode`) so the tree itself stays small / serializable.
    pub summaries: HashMap<SceneNodeId, PhaseSummary>,
    /// Denormalized DFS view of `tree` in display order, kept in
    /// sync by `rebuild_phases()` after every tree mutation. Read
    /// by the renderer hot paths via `state.phases` indexing —
    /// rebuilding once per mutation beats walking the tree per
    /// frame, and the cost is negligible (mutations are a handful
    /// per phase lifecycle, not per-op).
    pub phases: Vec<PhaseEntry>,

    /// Every phase currently in flight, keyed by (name, labels).
    /// Empty between phases. Multi-phase scenarios (stanza-level
    /// parallelism, multi-activity sessions) populate more than
    /// one entry. Most read sites today still assume at most one
    /// running phase — those use [`Self::first_active`] as a
    /// compatibility shim over the map.
    pub active_phases: HashMap<PhaseKey, ActivePhase>,

    /// Latency percentiles from last capture (nanoseconds).
    pub min_nanos: u64,
    pub p50_nanos: u64,
    pub p90_nanos: u64,
    pub p99_nanos: u64,
    pub p999_nanos: u64,
    pub max_nanos: u64,

    /// Log ring buffer (last 200 entries). Displayed in TUI log panel.
    pub log_messages: Vec<LogEntry>,

    /// Rolling ops/s history for sparkline (last 120 samples).
    pub ops_history: Vec<f64>,
    /// Rolling secondary-counter history for sparkline. The counter
    /// sampled is whichever adapter counter is first in
    /// `active.adapter_counters` — only populated when an adapter
    /// actually reports one, so it's never a hardcoded "rows".
    pub rows_history: Vec<f64>,
    /// Display label for the secondary sparkline (e.g. "rows/s" or
    /// "inserted/s"). `None` when no adapter counter is being tracked.
    pub rows_sparkline_label: Option<String>,
    /// Rolling max-latency history (nanoseconds). Sampled every drain
    /// tick (~250ms). Used by the latency panel to mark windowed peaks
    /// (e.g., "last 5s", "last 10s") with cross-bar glyphs.
    pub max_history: Vec<u64>,
    /// Rolling per-percentile histories, one push per frame delivered
    /// by the metrics scheduler (≈1 Hz). Fed to the time-series latency
    /// view and the short-window (5s / 15s max) variants on the
    /// barchart. Bounded at HISTORY_CAP so memory doesn't grow with
    /// the run — for true lifetime statistics use the `*_lifetime`
    /// aggregates below.
    pub min_history: Vec<u64>,
    pub p50_history: Vec<u64>,
    pub p90_history: Vec<u64>,
    pub p99_history: Vec<u64>,
    pub p999_history: Vec<u64>,

    /// Set to true when the run is complete.
    pub finished: bool,
}

impl RunState {
    pub fn new(
        workload_file: &str,
        scenario_name: &str,
        adapter: &str,
    ) -> Self {
        Self {
            workload_file: workload_file.to_string(),
            scenario_name: scenario_name.to_string(),
            adapter: adapter.to_string(),
            started_at: Instant::now(),
            profiler: "off".to_string(),
            limit: "none".to_string(),
            tree: SceneTree::new(),
            summaries: HashMap::new(),
            phases: Vec::new(),
            active_phases: HashMap::new(),
            log_messages: Vec::new(),
            min_nanos: 0,
            p50_nanos: 0,
            p90_nanos: 0,
            p99_nanos: 0,
            p999_nanos: 0,
            max_nanos: 0,
            ops_history: Vec::new(),
            rows_history: Vec::new(),
            rows_sparkline_label: None,
            max_history: Vec::new(),
            min_history: Vec::new(),
            p50_history: Vec::new(),
            p90_history: Vec::new(),
            p99_history: Vec::new(),
            p999_history: Vec::new(),
            finished: false,
        }
    }

    /// Borrow any one currently-running phase, if any exist.
    /// Compatibility shim for call sites that still assume a single
    /// running phase (ETA display, header labels, …). Multi-phase
    /// call sites should iterate [`Self::active_phases`] directly.
    pub fn first_active(&self) -> Option<&ActivePhase> {
        self.active_phases.values().next()
    }

    /// Borrow the active-phase entry matching a specific (name,
    /// labels) pair — used when the caller already knows which
    /// phase row it's rendering detail for.
    pub fn active_phase(&self, name: &str, labels: &str) -> Option<&ActivePhase> {
        self.active_phases.get(&(name.to_string(), labels.to_string()))
    }

    /// Mutable borrow of the active-phase entry for a specific
    /// (name, labels) pair. Used by the observer's progress callback
    /// to update in place.
    pub fn active_phase_mut(&mut self, name: &str, labels: &str) -> Option<&mut ActivePhase> {
        self.active_phases.get_mut(&(name.to_string(), labels.to_string()))
    }

    /// Push a log entry to the ring buffer (capped at 200).
    pub fn push_log(&mut self, severity: LogSeverity, message: String) {
        self.log_messages.push(LogEntry { severity, message });
        if self.log_messages.len() > 200 {
            self.log_messages.remove(0);
        }
    }

    /// Push an ops/s sample to the sparkline history (capped at 120).
    pub fn push_ops_sample(&mut self, ops_per_sec: f64) {
        self.ops_history.push(ops_per_sec);
        if self.ops_history.len() > 120 {
            self.ops_history.remove(0);
        }
    }

    /// Push a rows/s sample to the sparkline history.
    pub fn push_rows_sample(&mut self, rows_per_sec: f64) {
        self.rows_history.push(rows_per_sec);
        if self.rows_history.len() > 120 {
            self.rows_history.remove(0);
        }
    }

    /// Replace the scene tree wholesale — called once from the
    /// observer's `scenario_pre_mapped` hook with the fully
    /// resolved pre-map. Existing summaries are dropped (they
    /// never apply across pre-maps).
    pub fn install_tree(&mut self, tree: SceneTree) {
        self.tree = tree;
        self.summaries.clear();
        self.rebuild_phases();
    }

    /// Add a pending phase to the tree at the synthetic root —
    /// fallback path for sources that don't pre-map.
    pub fn add_phase(&mut self, name: &str, labels: &str, _depth: usize) {
        let root = self.tree.root();
        self.tree.push(root, EntryKind::Phase, name, labels);
        self.rebuild_phases();
    }

    /// Add a visual grouping header at the synthetic root.
    pub fn add_scope(&mut self, label: &str, _depth: usize) {
        let root = self.tree.root();
        self.tree.push(root, EntryKind::Scope, label, "");
        self.rebuild_phases();
    }

    /// Mark a phase as running. The first pending phase matching
    /// (name, labels) wins, so repeat iterations transition in
    /// encounter order.
    pub fn set_phase_running(&mut self, name: &str, labels: &str, op_count: usize) {
        if self.tree.find_phase(name, labels, Some(&PhaseStatus::Pending)).is_some() {
            self.tree.set_phase_running(name, labels, op_count);
        } else {
            // Not found — add dynamically and mark running. Phases
            // that weren't pre-mapped (e.g. unresolvable for_each
            // at pre-map time) still need a tree slot.
            let root = self.tree.root();
            let id = self.tree.push(root, EntryKind::Phase, name, labels);
            let n = &mut self.tree.nodes[id];
            n.status = PhaseStatus::Running;
            n.op_count = op_count;
        }
        self.rebuild_phases();
    }

    /// Mark a phase as completed and attach a metrics summary.
    pub fn set_phase_completed(
        &mut self,
        name: &str,
        labels: &str,
        duration_secs: f64,
        summary: PhaseSummary,
    ) {
        if let Some(id) = self.tree.find_phase(name, labels, Some(&PhaseStatus::Running)) {
            self.tree.set_phase_completed(name, labels, duration_secs);
            self.summaries.insert(id, summary);
            self.rebuild_phases();
        }
    }

    /// Mark a phase as failed.
    pub fn set_phase_failed(&mut self, name: &str, labels: &str, error: &str) {
        self.tree.set_phase_failed(name, labels, error);
        self.rebuild_phases();
    }

    /// Elapsed time since run started.
    pub fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    /// Rebuild the denormalized `phases` view from the current
    /// tree. Called after every mutation that affects the DFS
    /// order or any node's display fields.
    fn rebuild_phases(&mut self) {
        self.phases = self.tree
            .dfs()
            .filter(|n| n.kind != EntryKind::Root)
            .map(|n| PhaseEntry {
                node_id: n.id,
                name: n.name.clone(),
                labels: n.labels.clone(),
                status: n.status.clone(),
                kind: n.kind,
                op_count: n.op_count,
                duration_secs: n.duration_secs,
                // SceneNode depths count the synthetic root as 1 —
                // subtract so top-level scenario entries land at
                // depth 0 (matching pre-tree behavior).
                depth: n.depth.saturating_sub(1),
                summary: self.summaries.get(&n.id).cloned(),
            })
            .collect();
    }
}
