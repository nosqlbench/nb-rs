// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Run observer: callback trait for phase lifecycle events.
//!
//! The executor notifies observers when phases start, complete,
//! or fail. The TUI implements this to update its display state.
//! The default stderr observer prints phase progress lines.

/// Log level for diagnostic messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// Detailed diagnostics (parse notes, connection info)
    Debug,
    /// Normal operational messages (phase info, metrics paths)
    Info,
    /// Warnings (CQL driver warnings, recoverable errors)
    Warn,
    /// Errors (phase failures, binding errors)
    Error,
}

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

    /// Diagnostic log message. Routed to stderr in CLI mode,
    /// to a ring buffer in TUI mode. All `eprintln!` in the
    /// runtime should go through this instead.
    fn log(&self, level: LogLevel, message: &str);

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

/// Global observer for code that can't thread the observer through.
/// Set once at run start, remains for the process lifetime.
static GLOBAL_OBSERVER: std::sync::OnceLock<Arc<dyn RunObserver>> = std::sync::OnceLock::new();

/// Set the global observer. Called once by the runner at startup.
pub fn set_global_observer(observer: Arc<dyn RunObserver>) {
    let _ = GLOBAL_OBSERVER.set(observer);
}

/// Log a diagnostic message through the global observer.
/// Safe to call from anywhere — falls back to stderr if no
/// observer is set.
pub fn log(level: LogLevel, message: &str) {
    if let Some(obs) = GLOBAL_OBSERVER.get() {
        obs.log(level, message);
    } else {
        eprintln!("{message}");
    }
}

/// Convenience macros for logging through the global observer.
#[macro_export]
macro_rules! diag {
    ($level:expr, $($arg:tt)*) => {
        $crate::observer::log($level, &format!($($arg)*))
    };
}

use std::sync::Arc;

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

    fn log(&self, _level: LogLevel, message: &str) {
        eprintln!("{message}");
    }
}
