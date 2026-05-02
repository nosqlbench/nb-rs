// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! TUI-driving [`RunObserver`] implementation.
//!
//! [`TuiObserver`] is the glue layer between
//! [`nbrs_activity::observer::RunObserver`] (lifecycle callbacks
//! emitted by the runner) and the rest of this crate
//! ([`crate::run_state_actor`], [`crate::reporter::TuiReporter`],
//! [`crate::app::App`]). The runner only knows about the trait;
//! this module owns the translation of trait callbacks into
//! [`crate::run_state_actor::RunStateCmd`] messages sent to the
//! display-side actor.
//!
//! Lifecycle:
//!
//! 1. Persona spawns the RunState actor (via
//!    [`crate::run_state_actor::spawn_run_state_actor`]),
//!    constructs a [`TuiObserver`] holding the resulting
//!    [`crate::run_state_actor::RunStateHandle`], and hands it
//!    to [`nbrs_activity::runner::run_with_observer`].
//! 2. The runner calls `phase_starting` / `phase_progress` /
//!    `phase_completed` / `log` / etc. as the workload runs;
//!    each call sends one or more `RunStateCmd`s into the
//!    actor's inbox — no write lock anywhere.
//! 3. The first `phase_starting` triggers
//!    [`TuiObserver::ensure_tui_started`] which spawns the
//!    [`crate::app::App`] thread (also a snapshot reader and
//!    command sender); pre-phase work runs with plain stderr
//!    output so a startup failure leaves the terminal clean.
//! 4. After the run completes, the persona calls
//!    [`TuiObserver::shutdown`] to wait on the TUI thread, then
//!    [`print_post_run_summary`] for the post-teardown summary
//!    line + tree, then exits with an appropriate status from
//!    [`unreached_phase_exit_code`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use parking_lot::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

use nbrs_metrics::cadence::Cadences;
use nbrs_metrics::metrics_query::MetricsQuery;
use nbrs_metrics::snapshot::MetricSet;

use crate::reporter::TuiReporter;
use crate::run_state_actor::{RunStateCmd, RunStateHandle};
use crate::state::{EntryKind, LogEntry, LogSeverity, PhaseEntry, PhaseStatus, RunState};

/// Convert a TUI-side [`LogSeverity`] back to the activity-side
/// [`nbrs_activity::observer::LogLevel`] for comparison against
/// [`TuiObserver::min_level`]. The two enums carry the same four
/// rungs in the same order; this is a flat one-to-one mapping
/// kept private to the observer.
fn log_severity_to_level(s: LogSeverity) -> nbrs_activity::observer::LogLevel {
    match s {
        LogSeverity::Debug => nbrs_activity::observer::LogLevel::Debug,
        LogSeverity::Info  => nbrs_activity::observer::LogLevel::Info,
        LogSeverity::Warn  => nbrs_activity::observer::LogLevel::Warn,
        LogSeverity::Error => nbrs_activity::observer::LogLevel::Error,
    }
}

/// `RunObserver` impl that drives [`crate::app::App`] from
/// runner lifecycle events.
///
/// The TUI terminal takeover (raw mode + alternate screen) is
/// deferred until the first phase actually starts. Pre-phase
/// work runs with plain stderr output. This keeps the terminal
/// clean if the run fails before any activity begins.
pub struct TuiObserver {
    state: RunStateHandle,
    /// Base-cadence frame reporter, taken by the runner via
    /// `reporters()`. Forwards every scheduler tick's delta
    /// frame into the TUI so history rings + sparklines stay
    /// live.
    reporter: Mutex<Option<TuiReporter>>,
    /// Receiver end of the base-frame channel. Taken when the
    /// TUI thread is spawned on the first phase start.
    frame_rx: Mutex<Option<mpsc::Receiver<MetricSet>>>,
    /// User-declared cadences for the TUI's barchart view.
    /// Surfaced to the runner through `RunObserver::cadences()`
    /// so the cadence tree is planned with these values.
    cadences: Cadences,
    /// Shared `MetricsQuery` handle — populated by the runner
    /// via `on_metrics_query` once the cadence reporter is
    /// built.
    metrics_query: Mutex<Option<Arc<MetricsQuery>>>,
    /// Join handle for the TUI thread, once spawned.
    tui_handle: Mutex<Option<JoinHandle<()>>>,
    /// True once the TUI thread has been spawned and owns the
    /// terminal. Shared with the thread so it can flip it
    /// false on exit — which routes `log()` and phase
    /// lifecycle events back to stderr if the user pressed `q`
    /// before the run finished.
    tui_active: Arc<AtomicBool>,
    /// Minimum severity that the stderr fallback paths emit.
    /// Same role as
    /// [`nbrs_activity::observer::StderrObserver::min_level`] —
    /// without this filter, dropping out of the TUI at runtime
    /// (`q` pressed) drowned the operator in `Debug`-level
    /// fiber-lifecycle traces. Default `Info`, override via
    /// the workload's `loglevel=` param.
    min_level: nbrs_activity::observer::LogLevel,
}

impl TuiObserver {
    /// Build a fresh observer over a [`RunStateHandle`].
    /// `cadences` is forwarded to the runner via
    /// `RunObserver::cadences()`. Stderr fallback severity
    /// defaults to `Info`; use [`Self::with_min_level`] to
    /// override.
    pub fn new(state: RunStateHandle, cadences: Cadences) -> Self {
        let (reporter, frame_rx) = TuiReporter::channel();
        Self {
            state,
            reporter: Mutex::new(Some(reporter)),
            frame_rx: Mutex::new(Some(frame_rx)),
            tui_handle: Mutex::new(None),
            tui_active: Arc::new(AtomicBool::new(false)),
            cadences,
            metrics_query: Mutex::new(None),
            min_level: nbrs_activity::observer::LogLevel::Info,
        }
    }

    /// Override the minimum severity that the pre-TUI stderr
    /// mirror and the post-TUI stderr fallback emit. Has no
    /// effect on what the TUI's in-app log panel shows — that
    /// is filtered by the panel's own LOD knobs.
    pub fn with_min_level(
        mut self,
        min_level: nbrs_activity::observer::LogLevel,
    ) -> Self {
        self.min_level = min_level;
        self
    }

    /// Spawn the TUI thread on first use. Subsequent calls are
    /// no-ops.
    fn ensure_tui_started(&self) {
        if self.tui_active.load(Ordering::Acquire) {
            return;
        }
        let rx = match self.frame_rx.lock().take() {
            Some(rx) => rx,
            None => return, // already claimed
        };
        let query = match self.metrics_query.lock().clone() {
            Some(q) => q,
            None => return, // runner hasn't wired the query yet
        };
        let state = self.state.clone();
        let tui_active = self.tui_active.clone();
        let min_level = self.min_level;
        let handle = std::thread::spawn(move || {
            let mut app = crate::app::App::new(rx, state.clone(), query);
            if let Err(e) = app.run() {
                eprintln!("TUI error: {e}");
            }
            // TUI thread has exited and restored the terminal.
            // If the run is still going (user hit `q` mid-run),
            // flip the active flag FIRST so subsequent log() /
            // phase_starting() calls route to stderr. Then
            // replay the captured log ring buffer so the console
            // looks as if tui=off were in effect from the
            // start, plus a notice indicating the fallback.
            let was_active = tui_active.swap(false, Ordering::AcqRel);
            let snap = state.load();
            if was_active && !snap.finished {
                // Replay obeys the same severity filter the
                // live stderr fallback below uses — otherwise
                // dropping out of the TUI dumps the entire
                // in-memory Debug-level ring buffer to the
                // console, which is exactly the noise the
                // user wanted suppressed.
                for entry in &snap.log_messages {
                    if log_severity_to_level(entry.severity) >= min_level {
                        eprintln!("{}", entry.message);
                    }
                }
                eprintln!("--- tui disabled (q pressed); falling back to tui=off mode ---");
            }
        });
        *self.tui_handle.lock() = Some(handle);
        self.tui_active.store(true, Ordering::Release);
    }

    /// Signal the TUI (if running) to exit and wait for the
    /// terminal to be restored. Safe to call when the TUI never
    /// started.
    pub fn shutdown(&self) {
        self.state.send(RunStateCmd::RunFinished);
        let handle = self.tui_handle.lock().take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl nbrs_activity::observer::RunObserver for TuiObserver {
    fn phase_starting(&self, name: &str, labels: &str, op_templates: usize, total_cycles: u64, concurrency: usize) {
        self.ensure_tui_started();
        self.state.send(RunStateCmd::PhaseStarting {
            name: name.to_string(),
            labels: labels.to_string(),
            op_templates,
            total_cycles,
            concurrency,
        });
        // Route the human-readable line through the canonical
        // event channel so it lands in `session.log` even when
        // the TUI is suppressing stderr.
        let template_word = if op_templates == 1 { "op template" } else { "op templates" };
        let cycle_word = if total_cycles == 1 { "cycle" } else { "cycles" };
        nbrs_activity::observer::log(
            nbrs_activity::observer::LogLevel::Info,
            &format!("phase '{name}': {op_templates} {template_word}, {total_cycles} {cycle_word}, concurrency={concurrency}"));
    }

    fn phase_completed(&self, name: &str, labels: &str, duration_secs: f64) {
        self.state.send(RunStateCmd::PhaseCompleted {
            name: name.to_string(),
            labels: labels.to_string(),
            duration_secs,
        });
        // Block until the TUI has rendered at least one frame
        // reflecting the just-completed phase before letting the
        // executor proceed to the next one. Bypasses tick_rate;
        // bounded so a wedged TUI can never hang the run.
        // Skip when the TUI hasn't taken the screen yet — pre-
        // TUI phase completions land on stderr / session.log and
        // there's nothing to render.
        if self.tui_active.load(Ordering::Acquire) {
            self.state.flush_frame(std::time::Duration::from_secs(2));
        }
    }

    fn phase_failed(&self, name: &str, labels: &str, error: &str) {
        self.state.send(RunStateCmd::PhaseFailed {
            name: name.to_string(),
            labels: labels.to_string(),
            error: error.to_string(),
        });
        // Same end-of-phase rendering guarantee as phase_completed.
        if self.tui_active.load(Ordering::Acquire) {
            self.state.flush_frame(std::time::Duration::from_secs(2));
        }
    }

    fn phase_progress(&self, update: &nbrs_activity::observer::PhaseProgressUpdate) {
        self.state.send(RunStateCmd::PhaseProgress(update.clone()));
    }

    fn run_finished(&self) {
        self.state.send(RunStateCmd::RunFinished);
    }

    fn log(&self, level: nbrs_activity::observer::LogLevel, message: &str) {
        let severity = match level {
            nbrs_activity::observer::LogLevel::Debug => LogSeverity::Debug,
            nbrs_activity::observer::LogLevel::Info => LogSeverity::Info,
            nbrs_activity::observer::LogLevel::Warn => LogSeverity::Warn,
            nbrs_activity::observer::LogLevel::Error => LogSeverity::Error,
        };
        self.state.send(RunStateCmd::Log {
            severity,
            message: message.to_string(),
        });
        // Stderr fallback fires both *before* the TUI claims
        // the terminal (pre-phase diagnostics like session id,
        // metrics path, scenario tree) and *after* the TUI
        // tears down mid-run (`q` pressed). Apply the same
        // `min_level` filter `StderrObserver` uses, otherwise
        // the post-`q` console gets Debug-level fiber-exit
        // chatter that the operator doesn't want.
        if !self.tui_active.load(Ordering::Acquire)
            && level >= self.min_level
        {
            eprintln!("{message}");
        }
    }

    fn suppresses_stderr(&self) -> bool {
        self.tui_active.load(Ordering::Acquire)
    }

    fn live_suppress_flag(&self) -> Option<Arc<AtomicBool>> {
        // Hand the runner the same flag the TUI thread flips on
        // teardown — when the user dismisses the TUI mid-run,
        // this AtomicBool drops to `false` and any consumer
        // polling it (notably the activity's inline status
        // thread) resumes its tui=off behavior automatically.
        Some(self.tui_active.clone())
    }

    fn scenario_pre_mapped(&self, tree: &nbrs_activity::scene_tree::SceneTree) {
        self.state.send(RunStateCmd::InstallTree(tree.clone()));
    }

    fn reporters(&self) -> Vec<(Duration, Box<dyn nbrs_metrics::scheduler::Reporter>)> {
        let mut guard = self.reporter.lock();
        let Some(r) = guard.take() else { return Vec::new(); };
        vec![(Duration::from_secs(1), Box::new(r) as Box<dyn nbrs_metrics::scheduler::Reporter>)]
    }

    fn cadences(&self) -> Option<Cadences> {
        Some(self.cadences.clone())
    }

    fn on_metrics_query(&self, query: Arc<MetricsQuery>) {
        *self.metrics_query.lock() = Some(query);
    }
}

// ============================================================
// Post-run summary
// ============================================================

/// Print a summary of the run after the TUI has torn down.
///
/// Without this, successful runs leave the terminal with no
/// indication anything happened. Fails-soft if the state lock
/// is poisoned.
pub fn print_post_run_summary(
    run_state: &RunStateHandle,
    run_result: &Result<(), String>,
) {
    let s = run_state.load();
    let s: &RunState = &s;

    eprintln!();
    eprintln!("session: {} ({})", s.scenario_name, s.workload_file);
    eprintln!("logs:    logs/latest/");

    // Count phases only (scope headers are visual, not
    // executable).
    let phases_only: Vec<&PhaseEntry> = s.phases.iter()
        .filter(|p| p.kind == EntryKind::Phase)
        .collect();
    if phases_only.is_empty() {
        eprintln!("phases:  none executed");
    } else {
        let completed = phases_only.iter().filter(|p| {
            matches!(p.status, PhaseStatus::Completed)
        }).count();
        let failed = phases_only.iter().filter(|p| {
            matches!(p.status, PhaseStatus::Failed(_))
        }).count();
        let pending = phases_only.iter().filter(|p| {
            matches!(p.status, PhaseStatus::Pending)
        }).count();
        eprintln!("phases:  {} completed, {} failed, {} not run (of {} total)",
            completed, failed, pending, phases_only.len());

        // When there's a failure, printing every pending phase
        // after it gives screens of noise. Trim the tail: show
        // at most a small window of phases after the last
        // failure, then summarize the rest as "(... and N more
        // phases)" with a pointer at `dryrun=phase` for the
        // full plan.
        let last_failed: Option<usize> = s.phases.iter().enumerate()
            .rev()
            .find(|(_, p)| matches!(p.status, PhaseStatus::Failed(_)))
            .map(|(i, _)| i);
        let pending_tail_limit: usize = 6; // phases after last failure
        let mut printed_after_failure: usize = 0;
        let mut truncated_phases: usize = 0;

        for (i, phase) in s.phases.iter().enumerate() {
            let indent = "  ".repeat(phase.depth);

            // Truncation guard: once we're past the last
            // failure and past the small tail window, count the
            // remainder for the summary line and skip the
            // output.
            if let Some(fi) = last_failed {
                if i > fi && printed_after_failure >= pending_tail_limit {
                    if phase.kind == EntryKind::Phase {
                        truncated_phases += 1;
                    }
                    continue;
                }
            }

            if phase.kind == EntryKind::Scope {
                // Group header — no status glyph. Scope nodes
                // store their descriptor (e.g. `for_each k=10`,
                // `for_combinations [k, limit]`) in `name`; the
                // `labels` field is reserved for phase identity.
                eprintln!("  {indent}· {}", phase.name);
                continue;
            }
            let (marker, status_str) = match &phase.status {
                PhaseStatus::Completed => ("[ok]", String::new()),
                PhaseStatus::Running => ("[..]", " (still running)".into()),
                PhaseStatus::Pending => ("[  ]", " (not run)".into()),
                PhaseStatus::Failed(err) => ("[!!]", format!(" ({err})")),
            };
            // Phase rows omit the structural-identity `labels`
            // string here: every coord on the path is already
            // visible in the indented chain of scope headers
            // immediately above this row, so repeating the full
            // striated tuple on every phase produces the
            // "cartesian wall" the SceneTree was built to
            // replace.
            //
            // Zero-duration completions (the dryrun=phase
            // sentinel — see `executor::run_phase`'s early-
            // return) render without the " 0.00s" suffix; the
            // dry-run plan view stays clean rather than
            // peppered with `0.00s` after every line.
            let dur = phase.duration_secs
                .filter(|d| *d > 0.0)
                .map(|d| format!(" {d:.2}s"))
                .unwrap_or_default();
            // Pre-map `[N/total]` prefix mirrors the live TUI's
            // header counter so a post-run reader can scan the
            // summary and a screenshot of the running TUI side-
            // by-side without reconciling two different
            // numbering schemes.
            let total = phases_only.len();
            let seq_prefix = phase.seq
                .map(|s| format!("[{s}/{total}] "))
                .unwrap_or_default();
            eprintln!("  {indent}{marker} {seq_prefix}{}{dur}{status_str}",
                phase.name);

            if let Some(fi) = last_failed {
                if i > fi {
                    printed_after_failure += 1;
                }
            }
        }

        if truncated_phases > 0 {
            eprintln!("  (... and {truncated_phases} more phase{} not listed)",
                if truncated_phases == 1 { "" } else { "s" });
            eprintln!("  tip: run with dryrun=phase to see the full plan");
        }
    }

    // Focused error inset: for each failed phase, print the
    // chain of for_each / for_combinations / do_while scopes
    // that enclose it, then the failed phase itself. The
    // reader gets the exact binding context that led to the
    // failure without having to scan the full phase tree above.
    let failed: Vec<(usize, &PhaseEntry)> = s.phases.iter().enumerate()
        .filter(|(_, p)| p.kind == EntryKind::Phase
            && matches!(p.status, PhaseStatus::Failed(_)))
        .collect();
    if !failed.is_empty() {
        eprintln!();
        eprintln!("failures:");
        for (idx, phase) in &failed {
            for scope_idx in scope_ancestors(&s.phases, *idx) {
                let scope = &s.phases[scope_idx];
                let indent = "  ".repeat(scope.depth);
                // Scope descriptors live in `name` (e.g.
                // `for_each k=10`); `labels` carries the
                // structural-identity coord-path string and is
                // always empty for Scope entries.
                eprintln!("  {indent}· {}", scope.name);
            }
            let indent = "  ".repeat(phase.depth);
            // For the failure context, surface the leaf coord
            // path on the phase row (the structural-identity
            // string carries the iteration tuple) so a single
            // failed-phase block in `failures:` is self-
            // contained — the reader doesn't have to scroll up
            // to recover the iteration that failed.
            let labels = if phase.labels.is_empty() {
                String::new()
            } else {
                format!(" ({})", phase.labels)
            };
            let err_text = match &phase.status {
                PhaseStatus::Failed(err) => format!(" ({err})"),
                _ => String::new(),
            };
            eprintln!("  {indent}[!!] {}{labels}{err_text}", phase.name);
        }
    }

    // Dump recent log messages on failure for error context.
    if run_result.is_err() && !s.log_messages.is_empty() {
        eprintln!();
        eprintln!("--- recent log messages ---");
        let recent: Vec<&LogEntry> = s.log_messages.iter().rev().take(20).collect();
        for entry in recent.into_iter().rev() {
            eprintln!("  {}", entry.message);
        }
        eprintln!("---");
    }
}

/// Return the indices of scope entries that enclose
/// `target_idx`, ordered outermost-first. Walks backward from
/// the target collecting the nearest scope at each
/// strictly-shallower depth.
fn scope_ancestors(phases: &[PhaseEntry], target_idx: usize) -> Vec<usize> {
    if target_idx >= phases.len() { return Vec::new(); }
    let mut needed_depth = phases[target_idx].depth;
    let mut ancestors: Vec<usize> = Vec::new();
    for i in (0..target_idx).rev() {
        let p = &phases[i];
        if p.kind == EntryKind::Scope && p.depth < needed_depth {
            ancestors.push(i);
            needed_depth = p.depth;
            if needed_depth == 0 { break; }
        }
    }
    ancestors.reverse();
    ancestors
}

/// Returns `Some(2)` if any phases were pre-mapped (reachable
/// via scenario traversal) but never reached `Running` status —
/// the caller should print a warning and exit with this code.
/// Returns `None` if every reachable phase was visited.
///
/// A zero-cycle phase is fine when the data source legitimately
/// has no data — that phase still transitions Pending → Running
/// → Completed. What this catches is phases the executor should
/// have visited but didn't.
pub fn unreached_phase_exit_code(
    run_state: &RunStateHandle,
) -> Option<i32> {
    let s = run_state.load();
    let s: &RunState = &s;
    let unreached: Vec<&PhaseEntry> = s.phases.iter()
        .filter(|p| p.kind == EntryKind::Phase
            && matches!(p.status, PhaseStatus::Pending))
        .collect();
    if unreached.is_empty() { return None; }
    eprintln!();
    eprintln!("warning: {} pre-mapped phase(s) were not executed:",
        unreached.len());
    for p in &unreached {
        let labels = if p.labels.is_empty() {
            String::new()
        } else {
            format!(" ({})", p.labels)
        };
        eprintln!("  - {}{labels}", p.name);
    }
    Some(2)
}
