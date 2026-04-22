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

/// Kind of pre-mapped scenario entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreMapKind {
    /// An executable phase (Pending → Running → Completed).
    Phase,
    /// A scope header (for_each / for_combinations / do_while / do_until).
    /// Rendered as a visual group in the scenario tree.
    Scope,
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
    ///
    /// Kept for back-compat; for observers that want multiple reporters
    /// at different cadences, override [`reporters`] instead — the
    /// default impl forwards this single reporter as the base cadence.
    fn reporter(&self) -> Option<Box<dyn nb_metrics::scheduler::Reporter>> { None }

    /// Multiple reporters with explicit cadences. The runner calls
    /// this once during setup. Each `(interval, reporter)` entry is
    /// registered with the scheduler at that interval. The default
    /// implementation returns whatever [`reporter`] produced at the
    /// base 1s cadence, so existing observers work unchanged.
    fn reporters(&self) -> Vec<(std::time::Duration, Box<dyn nb_metrics::scheduler::Reporter>)> {
        match self.reporter() {
            Some(r) => vec![(std::time::Duration::from_secs(1), r)],
            None => vec![],
        }
    }

    /// User-declared cadences for this observer's consumers (SRD-42).
    ///
    /// When present, the runner uses these to plan the cadence tree
    /// passed to the scheduler's [`nb_metrics::cadence_reporter::CadenceReporter`].
    /// The reporter writes all windowed snapshots into a single store
    /// that every consumer reads through [`nb_metrics::metrics_query::MetricsQuery`].
    ///
    /// Observers that don't need windowed views (e.g. StderrObserver)
    /// return `None` and the runner falls back to
    /// `Cadences::defaults()`.
    fn cadences(&self) -> Option<nb_metrics::cadence::Cadences> { None }

    /// Callback invoked once the runner has built the shared
    /// [`nb_metrics::metrics_query::MetricsQuery`]. Observers that
    /// render metrics (TUI, CLI status) capture this handle to read
    /// cadence windows, `now` values, and session-lifetime aggregates.
    fn on_metrics_query(&self, _query: std::sync::Arc<nb_metrics::metrics_query::MetricsQuery>) {}

    /// Pre-populated scenario tree entries.
    ///
    /// Called once before execution begins. Each entry is
    /// `(kind, name_or_label, labels, depth)`:
    /// - for [`PreMapKind::Phase`]: `name_or_label` is the phase name
    ///   and `labels` is the binding context.
    /// - for [`PreMapKind::Scope`]: `name_or_label` is the iterator
    ///   description (e.g., `"profile in [label_00, label_01, ...]"`)
    ///   and `labels` is empty.
    ///
    /// The TUI uses this to show all phases as Pending and every
    /// for_each as a group header in the scenario tree from the start.
    fn scenario_pre_mapped(&self, _entries: &[(PreMapKind, String, String, usize)]) {}
}

/// Live metrics snapshot for progress updates.
pub struct PhaseProgressUpdate {
    /// Phase name this update belongs to — matches the `name`
    /// passed to [`RunObserver::phase_starting`]. Present so
    /// observers that track multiple concurrent phases can route
    /// the update to the correct per-phase slot.
    pub name: String,
    /// Phase dimensional labels (e.g. `profile=label_00, k=10`) —
    /// together with `name` this uniquely identifies one phase
    /// iteration.
    pub labels: String,
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
    /// Live relevancy aggregates — one entry per relevancy metric (e.g.
    /// `recall@10`). Each has a moving-window mean over the last N
    /// recall calculations and a whole-activity running mean.
    pub relevancy: Vec<crate::validation::RelevancyLive>,
}

/// Global observer for code that can't thread the observer through.
/// Set once at run start, remains for the process lifetime.
static GLOBAL_OBSERVER: std::sync::OnceLock<Arc<dyn RunObserver>> = std::sync::OnceLock::new();

/// Optional durable log sink — every [`log`] call appends here too, so
/// messages survive the TUI teardown and can be inspected post-run.
/// Initialized by [`set_log_file`] once the session directory exists.
static GLOBAL_LOG_FILE: std::sync::OnceLock<Arc<std::sync::Mutex<std::fs::File>>>
    = std::sync::OnceLock::new();

/// Set the global observer. Called once by the runner at startup.
pub fn set_global_observer(observer: Arc<dyn RunObserver>) {
    let _ = GLOBAL_OBSERVER.set(observer);
}

/// Direct the log sink to a file. Opens for append-writes. Called once
/// by the runner after the session directory is created. Silently
/// no-ops on a second call — the first session wins, which is the
/// intended behavior (one run per process).
pub fn set_log_file(path: &std::path::Path) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new()
        .create(true).append(true).open(path)?;
    let _ = GLOBAL_LOG_FILE.set(Arc::new(std::sync::Mutex::new(file)));
    Ok(())
}

/// Log a diagnostic message through the global observer and append to
/// the session log file (if set). Safe to call from anywhere — falls
/// back to stderr if no observer is set.
pub fn log(level: LogLevel, message: &str) {
    if let Some(file) = GLOBAL_LOG_FILE.get() {
        if let Ok(mut f) = file.lock() {
            use std::io::Write;
            let tag = match level {
                LogLevel::Debug => "DBG",
                LogLevel::Info  => "INF",
                LogLevel::Warn  => "WRN",
                LogLevel::Error => "ERR",
            };
            // Human-readable wall-clock timestamp from the session
            // formatter — matches the session id's date/time style so
            // log lines correlate visually with the session directory.
            let ts = crate::session::now_log_timestamp();
            let _ = writeln!(f, "{ts} {tag} {message}");
        }
    }
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
