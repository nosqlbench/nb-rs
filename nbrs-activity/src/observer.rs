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

/// Kind of pre-mapped scenario entry. Re-export of
/// [`crate::scene_tree::NodeKind`] for callers that already
/// imported it via the observer module.
pub use crate::scene_tree::NodeKind as PreMapKind;

/// Lifecycle events from the executor.
pub trait RunObserver: Send + Sync {
    /// A phase is about to start executing.
    ///
    /// `op_templates` is the count of op definitions in the phase
    /// (typically 1 for query workloads). `total_cycles` is the
    /// number of times the stanza will iterate. Both are reported
    /// because they answer different questions: the first describes
    /// the *shape* of the phase, the second describes the *amount*
    /// of work it represents.
    fn phase_starting(&self, name: &str, labels: &str, op_templates: usize, total_cycles: u64, concurrency: usize);

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

    /// Optional shared flag mirroring [`Self::suppresses_stderr`]
    /// that the runner threads into long-lived components
    /// (e.g. the activity's inline status thread) so they can
    /// react to dismissal mid-run rather than honoring a
    /// snapshot taken at construction. When `None`, the
    /// activity uses a fresh `AtomicBool(false)` (never
    /// suppress). Implementations that go through a TUI
    /// (and only those) typically expose their internal
    /// "tui_active" flag here.
    fn live_suppress_flag(&self) -> Option<std::sync::Arc<std::sync::atomic::AtomicBool>> {
        None
    }

    /// Optional reporter to register on the metrics scheduler.
    /// The runner calls this once during setup. Return None for
    /// observers that don't need metrics frames (like StderrObserver).
    ///
    /// Kept for back-compat; for observers that want multiple reporters
    /// at different cadences, override [`reporters`] instead — the
    /// default impl forwards this single reporter as the base cadence.
    fn reporter(&self) -> Option<Box<dyn nbrs_metrics::scheduler::Reporter>> { None }

    /// Multiple reporters with explicit cadences. The runner calls
    /// this once during setup. Each `(interval, reporter)` entry is
    /// registered with the scheduler at that interval. The default
    /// implementation returns whatever [`reporter`] produced at the
    /// base 1s cadence, so existing observers work unchanged.
    fn reporters(&self) -> Vec<(std::time::Duration, Box<dyn nbrs_metrics::scheduler::Reporter>)> {
        match self.reporter() {
            Some(r) => vec![(std::time::Duration::from_secs(1), r)],
            None => vec![],
        }
    }

    /// User-declared cadences for this observer's consumers (SRD-42).
    ///
    /// When present, the runner uses these to plan the cadence tree
    /// passed to the scheduler's [`nbrs_metrics::cadence_reporter::CadenceReporter`].
    /// The reporter writes all windowed snapshots into a single store
    /// that every consumer reads through [`nbrs_metrics::metrics_query::MetricsQuery`].
    ///
    /// Observers that don't need windowed views (e.g. StderrObserver)
    /// return `None` and the runner falls back to
    /// `Cadences::defaults()`.
    fn cadences(&self) -> Option<nbrs_metrics::cadence::Cadences> { None }

    /// Callback invoked once the runner has built the shared
    /// [`nbrs_metrics::metrics_query::MetricsQuery`]. Observers that
    /// render metrics (TUI, CLI status) capture this handle to read
    /// cadence windows, `now` values, and session-lifetime aggregates.
    fn on_metrics_query(&self, _query: std::sync::Arc<nbrs_metrics::metrics_query::MetricsQuery>) {}

    /// Pre-populated scenario tree.
    ///
    /// Called once before execution begins with the full
    /// [`crate::scene_tree::SceneTree`] — synthetic root, every
    /// concrete phase, and every scope header (`for_each`,
    /// `for_combinations`, `do_while`, `do_until`) wired up by
    /// parent / children pointers.
    ///
    /// The TUI uses this to show all phases as Pending from the
    /// start; renderers that want hierarchical features (collapse,
    /// scope-level aggregate status) walk the tree directly. The
    /// callee may store the tree (e.g. behind an `RwLock`) and
    /// mutate node statuses in place via the lifecycle callbacks.
    fn scenario_pre_mapped(&self, _tree: &crate::scene_tree::SceneTree) {}
}

/// Live metrics snapshot for progress updates.
#[derive(Clone, Debug)]
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

/// Set the global observer. Called once by the runner at startup.
pub fn set_global_observer(observer: Arc<dyn RunObserver>) {
    let _ = GLOBAL_OBSERVER.set(observer);
}

/// Direct the log sink to a file. Opens for append-writes,
/// installs the [`crate::log_sink`] async writer thread.
/// Producers thereafter `try_send` and never block — see SRD-02
/// §"Display and Diagnostic Decoupling". Silently no-ops on a
/// second call — the first session wins (one run per process).
pub fn set_log_file(path: &std::path::Path) -> std::io::Result<()> {
    crate::log_sink::init(path)
}

/// Log a diagnostic message through the global observer and
/// append to the async log sink (if initialized). Safe to call
/// from anywhere — falls back to stderr if no observer is set.
///
/// The file write is non-blocking: the line is enqueued onto a
/// bounded channel consumed by the dedicated `log-sink` thread.
/// On overflow the line is dropped and the sink's `dropped_count`
/// is bumped — never blocks the caller, even on a stalled disk.
/// Whether ANSI color escapes are appropriate for stderr.
/// `true` only when stderr is a TTY and the operator hasn't
/// disabled color via the conventional `NO_COLOR` env var
/// (https://no-color.org). Pipelined / CI contexts return
/// `false` so log archives stay readable. Cached on first
/// call; the answer doesn't change over a process's lifetime.
pub fn use_color() -> bool {
    use std::io::IsTerminal;
    use std::sync::OnceLock;
    static CACHE: OnceLock<bool> = OnceLock::new();
    *CACHE.get_or_init(|| {
        if std::env::var_os("NO_COLOR").is_some() { return false; }
        std::io::stderr().is_terminal()
    })
}

/// Minimum severity that reaches the file sink
/// (`session.log`). Default `Debug` — the file gets every
/// non-trivial entry. The display threshold (per-observer
/// `min_level`) is the SEPARATE knob that controls what
/// reaches stderr; it defaults to `Info`. The two are
/// configured independently via:
///
/// - `loglevel-retain=` / `--log-retain-level` /
///   `NBRS_LOG_RETAIN_LEVEL` — what the file gets.
/// - `loglevel=` / `--log-display-level` /
///   `NBRS_LOG_DISPLAY_LEVEL` — what stderr gets.
///
/// The runner installs the user-supplied retain level via
/// [`set_retain_level`] at startup; readers consult
/// [`retain_level`] before sending to the sink.
static RETAIN_LEVEL: std::sync::OnceLock<LogLevel> = std::sync::OnceLock::new();

/// Install the file-sink retention threshold. Called once
/// by the runner; subsequent calls are silent no-ops
/// (matching the "first wins" pattern of the rest of the
/// global observer surface).
pub fn set_retain_level(level: LogLevel) {
    let _ = RETAIN_LEVEL.set(level);
}

/// Effective retention threshold. Defaults to `Debug` so
/// pre-runner-init log calls (very early startup) still
/// reach the file sink.
pub fn retain_level() -> LogLevel {
    *RETAIN_LEVEL.get().unwrap_or(&LogLevel::Debug)
}

/// Companion to [`RETAIN_LEVEL`]: the *display* threshold
/// that gates console emission. Each observer carries its
/// own `min_level` for the live log path; this global
/// captures the effective value so secondary surfaces
/// (the failure-dump path in `nbrs-tui::observer`) can
/// honour the same threshold without plumbing the observer
/// reference everywhere.
static DISPLAY_LEVEL: std::sync::OnceLock<LogLevel> = std::sync::OnceLock::new();

/// Install the console display threshold. Called by the
/// runner alongside [`set_retain_level`] at startup.
pub fn set_display_level(level: LogLevel) {
    let _ = DISPLAY_LEVEL.set(level);
}

/// Effective console display threshold. Defaults to
/// `Info` — same default the live observers use.
pub fn display_level() -> LogLevel {
    *DISPLAY_LEVEL.get().unwrap_or(&LogLevel::Info)
}

pub fn log(level: LogLevel, message: &str) {
    if level >= retain_level() {
        if let Some(sink) = crate::log_sink::global() {
            let tag = match level {
                LogLevel::Debug => "DBG",
                LogLevel::Info  => "INF",
                LogLevel::Warn  => "WRN",
                LogLevel::Error => "ERR",
            };
            // Human-readable wall-clock timestamp from the session
            // formatter — matches the session id's date/time style
            // so log lines correlate visually with the session
            // directory.
            let ts = crate::session::now_log_timestamp();
            let line = format!("{ts} {tag} {message}\n").into_bytes();
            let _ = sink.try_send(line);
        }
    }
    if let Some(obs) = GLOBAL_OBSERVER.get() {
        obs.log(level, message);
    } else {
        eprintln!("{}", colorize_log_line(level, message));
    }
}

/// ANSI-colorize a log line by severity for console
/// output. Always applied at the producer side — every
/// console emission of a log entry runs through this so
/// `DBG`/`INF`/`WRN`/`ERR` are visually distinct without
/// the operator having to squint at message bodies.
/// Falls through to the bare message when stderr isn't a
/// TTY or `NO_COLOR` is set (per [`use_color`]); pipeline
/// captures stay readable.
pub fn colorize_log_line(level: LogLevel, message: &str) -> String {
    if !use_color() { return message.to_string(); }
    let (color, reset) = match level {
        // Dim grey for debug — present but de-emphasized.
        LogLevel::Debug => ("\x1b[2m",      "\x1b[0m"),
        // Default-color for info — the baseline; no
        // override so user-themed terminals show their
        // preferred default.
        LogLevel::Info  => ("",             ""),
        // Yellow for warn.
        LogLevel::Warn  => ("\x1b[33m",     "\x1b[0m"),
        // Bold red for error.
        LogLevel::Error => ("\x1b[1;31m",   "\x1b[0m"),
    };
    if color.is_empty() { message.to_string() }
    else { format!("{color}{message}{reset}") }
}

/// Convenience macros for logging through the global observer.
#[macro_export]
macro_rules! diag {
    ($level:expr, $($arg:tt)*) => {
        $crate::observer::log($level, &format!($($arg)*))
    };
}

use std::sync::Arc;

/// Default observer: prints to stderr.
///
/// `min_level` controls the minimum severity that reaches
/// stderr — Info by default, matching the TUI log panel's
/// default filter (so high-cadence Debug instrumentation
/// doesn't drown the signal in either mode). Override via
/// `loglevel=debug|info|warn|error` on the workload command
/// line. The async log sink (session.log) still receives
/// every level regardless of this filter.
pub struct StderrObserver {
    pub min_level: LogLevel,
}

impl Default for StderrObserver {
    fn default() -> Self {
        Self { min_level: LogLevel::Info }
    }
}

impl StderrObserver {
    /// Build a stderr observer with the given min severity.
    pub fn with_min_level(min_level: LogLevel) -> Self {
        Self { min_level }
    }
}

impl RunObserver for StderrObserver {
    fn phase_starting(&self, name: &str, _labels: &str, op_templates: usize, total_cycles: u64, concurrency: usize) {
        // Route through the canonical event channel so the line
        // lands in `session.log` AND on stderr (via the recursive
        // call back into `StderrObserver::log` below).
        let template_word = if op_templates == 1 { "op template" } else { "op templates" };
        let cycle_word = if total_cycles == 1 { "cycle" } else { "cycles" };
        crate::observer::log(LogLevel::Info,
            &format!("phase '{name}': {op_templates} {template_word}, {total_cycles} {cycle_word}, concurrency={concurrency}"));
    }

    fn phase_completed(&self, _name: &str, _labels: &str, _duration_secs: f64) {
        // No-op — the executor's own diag emits a fully-formatted
        // "phase 'X' complete (Ns)" line via the log path. Doing
        // it here too produced a duplicate (and a less
        // informative one — no duration). The structured
        // callback stays for non-stderr consumers.
    }

    fn phase_failed(&self, _name: &str, _labels: &str, _error: &str) {
        // Same reasoning as phase_completed — the executor diags
        // already emit "phase 'X' stopped by error handler (Ns)"
        // (or other failure messages) right before calling this.
        // Re-emitting here was a duplicate.
    }

    fn phase_progress(&self, _update: &PhaseProgressUpdate) {
        // The inline status line in activity.rs handles this
    }

    fn run_finished(&self) {
        // Same routing as `phase_starting` — through `observer::log`
        // so session.log captures the run-end marker.
        crate::observer::log(LogLevel::Info, "all phases complete");
    }

    fn log(&self, level: LogLevel, message: &str) {
        // Severity filter: only entries `>= min_level` reach
        // stderr. The session log file still gets every level
        // via the async log sink — this filter only affects
        // what the operator sees on screen.
        if level >= self.min_level {
            // Cosmetic: when the runtime announces a Ctrl-C-
            // initiated graceful shutdown, the terminal has just
            // echoed `^C` on the current line. A leading blank
            // line makes the announcement visually clear that
            // marker without leaving a stray newline in the
            // structured session.log (which never sees this
            // path). Same idea for force-exit on second Ctrl-C.
            if message.starts_with("session: graceful shutdown requested")
                || message.starts_with("session: force-exit on second")
            {
                eprintln!();
            }
            eprintln!("{}", colorize_log_line(level, message));
        }
    }
}
