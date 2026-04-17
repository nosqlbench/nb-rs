// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Run observer: callback trait for phase lifecycle events.
//!
//! The executor notifies observers when phases start, complete,
//! or fail. The TUI implements this to update its display state.
//! The default stderr observer prints phase progress lines.

/// Lifecycle events from the executor.
pub trait RunObserver: Send + Sync {
    /// A phase is about to start executing.
    fn phase_starting(&self, name: &str, labels: &str, op_count: usize, concurrency: usize);

    /// A phase completed successfully.
    fn phase_completed(&self, name: &str, labels: &str, duration_secs: f64);

    /// A phase failed.
    fn phase_failed(&self, name: &str, labels: &str, error: &str);

    /// Update live metrics for the active phase (called at progress tick rate).
    fn phase_progress(&self, update: &PhaseProgressUpdate);

    /// The entire run is complete.
    fn run_finished(&self);

    /// Whether to suppress the inline stderr progress line
    /// (because the TUI is handling display).
    fn suppresses_stderr(&self) -> bool { false }

    /// Optional reporter to register on the metrics scheduler.
    /// The runner calls this once during setup. Return None for
    /// observers that don't need metrics frames (like StderrObserver).
    fn reporter(&self) -> Option<Box<dyn nb_metrics::scheduler::Reporter>> { None }
}

/// Live metrics snapshot for progress updates.
pub struct PhaseProgressUpdate {
    pub cursor_name: String,
    pub cursor_extent: u64,
    pub fibers: usize,
    pub ops_started: u64,
    pub ops_finished: u64,
    pub ops_ok: u64,
    pub errors: u64,
    pub retries: u64,
    pub ops_per_sec: f64,
    pub adapter_counters: Vec<(String, u64, f64)>,
    pub rows_per_batch: f64,
}

/// Default observer: prints to stderr (current behavior).
pub struct StderrObserver;

impl RunObserver for StderrObserver {
    fn phase_starting(&self, name: &str, _labels: &str, op_count: usize, concurrency: usize) {
        eprintln!("phase '{name}': {op_count} ops, concurrency={concurrency}");
    }

    fn phase_completed(&self, name: &str, _labels: &str, _duration_secs: f64) {
        eprintln!("phase '{name}' complete");
    }

    fn phase_failed(&self, name: &str, _labels: &str, error: &str) {
        eprintln!("error: {error}");
        eprintln!("phase '{name}' failed");
    }

    fn phase_progress(&self, _update: &PhaseProgressUpdate) {
        // The inline status line in activity.rs handles this
    }

    fn run_finished(&self) {
        eprintln!("all phases complete");
    }
}
