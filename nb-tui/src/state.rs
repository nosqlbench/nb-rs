// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Shared state between the executor and the TUI display.
//!
//! The executor writes to `RunState` via `Arc<RwLock<RunState>>`.
//! The TUI reads it at its render cadence (4 Hz). Writes are
//! infrequent (phase transitions, not per-op) so lock contention
//! is negligible.

use std::time::Instant;

/// Status of a single phase in the scenario tree.
#[derive(Clone, Debug)]
pub struct PhaseEntry {
    pub name: String,
    /// Dimensional labels from for_each (e.g., "k=10, table=fknn_default")
    pub labels: String,
    pub status: PhaseStatus,
    /// Number of ops in the phase.
    pub op_count: usize,
    /// Duration if completed.
    pub duration_secs: Option<f64>,
    /// Nesting depth for tree display.
    pub depth: usize,
}

/// Phase lifecycle state.
#[derive(Clone, Debug, PartialEq)]
pub enum PhaseStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
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
}

/// Top-level run state shared between executor and TUI.
pub struct RunState {
    pub workload_file: String,
    pub scenario_name: String,
    pub adapter: String,
    pub started_at: Instant,
    pub profiler: String,
    pub limit: String,

    /// Scenario tree entries in display order.
    pub phases: Vec<PhaseEntry>,

    /// Currently active phase metrics (None between phases).
    pub active: Option<ActivePhase>,

    /// Latency percentiles from last capture (nanoseconds).
    pub p50_nanos: u64,
    pub p90_nanos: u64,
    pub p99_nanos: u64,
    pub p999_nanos: u64,
    pub max_nanos: u64,

    /// Log ring buffer (last 200 messages). Displayed in TUI log panel.
    pub log_messages: Vec<String>,

    /// Rolling ops/s history for sparkline (last 120 samples).
    pub ops_history: Vec<f64>,
    /// Rolling rows/s history for sparkline.
    pub rows_history: Vec<f64>,

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
            phases: Vec::new(),
            active: None,
            log_messages: Vec::new(),
            p50_nanos: 0,
            p90_nanos: 0,
            p99_nanos: 0,
            p999_nanos: 0,
            max_nanos: 0,
            ops_history: Vec::new(),
            rows_history: Vec::new(),
            finished: false,
        }
    }

    /// Push a log message to the ring buffer (capped at 200).
    pub fn push_log(&mut self, message: String) {
        self.log_messages.push(message);
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

    /// Add a pending phase to the tree.
    pub fn add_phase(&mut self, name: &str, labels: &str, depth: usize) {
        self.phases.push(PhaseEntry {
            name: name.to_string(),
            labels: labels.to_string(),
            status: PhaseStatus::Pending,
            op_count: 0,
            duration_secs: None,
            depth,
        });
    }

    /// Mark a phase as running.
    pub fn set_phase_running(&mut self, name: &str, labels: &str, op_count: usize) {
        for phase in &mut self.phases {
            if phase.name == name && phase.labels == labels && phase.status == PhaseStatus::Pending {
                phase.status = PhaseStatus::Running;
                phase.op_count = op_count;
                return;
            }
        }
        // Not found — add dynamically (for_each phases not known ahead of time)
        self.phases.push(PhaseEntry {
            name: name.to_string(),
            labels: labels.to_string(),
            status: PhaseStatus::Running,
            op_count,
            duration_secs: None,
            depth: 0,
        });
    }

    /// Mark a phase as completed.
    pub fn set_phase_completed(&mut self, name: &str, labels: &str, duration_secs: f64) {
        for phase in &mut self.phases {
            if phase.name == name && phase.labels == labels && phase.status == PhaseStatus::Running {
                phase.status = PhaseStatus::Completed;
                phase.duration_secs = Some(duration_secs);
                return;
            }
        }
    }

    /// Mark a phase as failed.
    pub fn set_phase_failed(&mut self, name: &str, labels: &str, error: &str) {
        for phase in &mut self.phases {
            if phase.name == name && phase.labels == labels {
                phase.status = PhaseStatus::Failed(error.to_string());
                return;
            }
        }
    }

    /// Elapsed time since run started.
    pub fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }
}
