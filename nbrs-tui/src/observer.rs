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

use nbrs_activity::readouts as ro;

/// Per-row `ReadoutContext` for the post-run summary's
/// `[ok] [N/total] name 0.02s` lines. Routes through the
/// SRD-63 `phase_summary` readout so the marker / sequence
/// / duration formatting lives in one place. The call
/// site (the post-run summary tree walk in
/// [`emit_run_summary`]) supplies the depth-based indent
/// — the readout doesn't know about the surrounding
/// chrome.
struct SummaryRowContext {
    name: String,
    labels: String,
    seq: Option<(usize, usize)>,
    duration_secs: f64,
    state: ro::LifecycleState,
}

/// Per-row `ReadoutContext` for scope-header rows in the
/// post-run summary tree walk and the focused-error
/// inset's scope ancestors. Routes through the SRD-63
/// `scope_header` readout so the bullet + italic
/// formatter lives alongside the rest of the engine
/// (Push 8c).
struct ScopeRowContext {
    name: String,
    use_color: bool,
}

impl ScopeRowContext {
    fn new(name: &str, use_color: bool) -> Self {
        Self { name: name.to_string(), use_color }
    }
}

impl ro::ReadoutContext for ScopeRowContext {
    fn subject_name(&self) -> &str { &self.name }
    fn subject_seq(&self) -> Option<(usize, usize)> { None }
    fn subject_labels(&self) -> &str { "" }
    fn cycles_completed(&self) -> u64 { 0 }
    fn cycles_total(&self) -> u64 { 0 }
    fn ops_ok(&self) -> u64 { 0 }
    fn errors(&self) -> u64 { 0 }
    fn retries(&self) -> u64 { 0 }
    fn concurrency(&self) -> usize { 0 }
    fn elapsed_secs(&self) -> f64 { 0.0 }
    fn consumed(&self) -> u64 { 0 }
    fn status_metric_chips(&self) -> String { String::new() }
    fn depth_indent(&self) -> &str { "" }
    fn use_color(&self) -> bool { self.use_color }
    fn event(&self) -> ro::Event { ro::Event::EachStart }
}

/// `ReadoutContext` for the post-run summary's session-scope
/// readouts (`session_banner` for the opening line,
/// `session_summary` for the `phases: …` rollup). Carries the
/// scenario / workload identity and the per-status phase
/// counts; everything else falls through to the trait
/// defaults.
struct SessionSummaryContext {
    scenario_name: String,
    workload_file: String,
    completed: usize,
    failed: usize,
    pending: usize,
    total: usize,
    /// SRD-63 Push 9h: truncated-phase count for the
    /// `truncated_phases` readout. Set after the per-phase
    /// loop counts how many entries the post-failure tail
    /// trim dropped; zero when no truncation occurred.
    truncated: std::cell::Cell<usize>,
}

impl ro::ReadoutContext for SessionSummaryContext {
    fn subject_name(&self) -> &str { "session" }
    fn subject_id(&self) -> String { "session".to_string() }
    fn event(&self) -> ro::Event { ro::Event::SessionEnd }
    fn session_scenario_name(&self) -> &str { &self.scenario_name }
    fn session_workload_file(&self) -> &str { &self.workload_file }
    fn session_phases_completed(&self) -> usize { self.completed }
    fn session_phases_failed(&self) -> usize { self.failed }
    fn session_phases_pending(&self) -> usize { self.pending }
    fn session_phases_total(&self) -> usize { self.total }
    fn session_phases_truncated(&self) -> usize { self.truncated.get() }
}

impl ro::ReadoutContext for SummaryRowContext {
    fn subject_name(&self) -> &str { &self.name }
    fn subject_seq(&self) -> Option<(usize, usize)> { self.seq }
    fn subject_labels(&self) -> &str { &self.labels }
    fn elapsed_secs(&self) -> f64 { self.duration_secs }
    fn event(&self) -> ro::Event { ro::Event::PhaseEnd }
    fn subject_state(&self) -> ro::LifecycleState { self.state.clone() }
}

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
        // the TUI is suppressing stderr. Same `[name] (root-first
        // coords): …` shape the terminal-mode observer emits, so
        // the archived log reads identically to stderr.
        let template_word = if op_templates == 1 { "op template" } else { "op templates" };
        let cycle_word = if total_cycles == 1 { "cycle" } else { "cycles" };
        let coords_part = if labels.is_empty() {
            String::new()
        } else {
            format!(" {}", crate::widgets::coords_root_first(labels))
        };
        nbrs_activity::observer::log(
            nbrs_activity::observer::LogLevel::Info,
            &format!("[{name}]{coords_part}: {op_templates} {template_word}, {total_cycles} {cycle_word}, concurrency={concurrency}"));
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

    fn set_status_line(&self, rendered: Option<String>) {
        // Same actor channel as the log-only observer: route the
        // pre-rendered status string into RunState so whichever
        // sink is currently active (LogOnlySink or TuiSink)
        // can pick it up. The TUI app today still draws from
        // ActivePhase directly, but exposing the binder output
        // through the actor means a future "render the same
        // status string the terminal would" panel works without
        // re-running the binder.
        self.state.send(RunStateCmd::SetStatusLine(rendered));
    }

    fn run_finished(&self) {
        self.state.send(RunStateCmd::RunFinished);
    }

    fn log(&self, level: nbrs_activity::observer::LogLevel, message: &str) {
        let severity = match level {
            nbrs_activity::observer::LogLevel::Trace => LogSeverity::Debug,
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

    // Count phases only (scope headers are visual, not
    // executable).
    let phases_only: Vec<&PhaseEntry> = s.phases.iter()
        .filter(|p| p.kind == EntryKind::Phase)
        .collect();
    let completed = phases_only.iter().filter(|p| {
        matches!(p.status, PhaseStatus::Completed)
    }).count();
    let failed = phases_only.iter().filter(|p| {
        matches!(p.status, PhaseStatus::Failed(_))
    }).count();
    let pending = phases_only.iter().filter(|p| {
        matches!(p.status, PhaseStatus::Pending)
    }).count();

    // SRD-63 Push 9d: route the opening banner + the
    // `phases:  X completed, Y failed, …` rollup through
    // the readout engine. Both share one
    // `SessionSummaryContext` so the totals and identity
    // accessors agree.
    let session_ctx = SessionSummaryContext {
        scenario_name: s.scenario_name.clone(),
        workload_file: s.workload_file.clone(),
        completed,
        failed,
        pending,
        total: phases_only.len(),
        // Filled in by the per-phase loop below as it
        // counts entries dropped from the post-failure
        // tail trim. The `truncated_phases` readout reads
        // this after the loop completes.
        truncated: std::cell::Cell::new(0),
    };
    {
        let mut s_buf = String::with_capacity(96);
        let mut buf = ro::buf::StringBuf::new(&mut s_buf);
        use ro::Readout;
        ro::builtins::session_banner::SessionBanner.render(
            &session_ctx,
            ro::Lod::Labeled,
            ro::ContentMode::Value,
            &ro::ReadoutOptions::new(),
            &mut buf,
        );
        if !s_buf.is_empty() {
            eprintln!("{s_buf}");
        }
    }
    eprintln!("logs:    logs/latest/");

    if phases_only.is_empty() {
        eprintln!("phases:  none executed");
    } else {
        let mut s_buf = String::with_capacity(96);
        {
            let mut buf = ro::buf::StringBuf::new(&mut s_buf);
            use ro::Readout;
            ro::builtins::session_summary::SessionSummary.render(
                &session_ctx,
                ro::Lod::Labeled,
                ro::ContentMode::Value,
                &ro::ReadoutOptions::new(),
                &mut buf,
            );
        }
        eprintln!("{s_buf}");

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
                // Push 8c routes through the `scope_header`
                // readout so the formatter (cyan bullet +
                // italic name) lives in one place, shared
                // with the live mid-run scope walker in
                // `log_only_observer`.
                let mut s_buf = String::with_capacity(64);
                {
                    let mut buf = ro::buf::StringBuf::new(&mut s_buf);
                    use ro::Readout;
                    ro::builtins::scope_header::ScopeHeader.render(
                        &ScopeRowContext::new(&phase.name, false),
                        ro::Lod::Labeled,
                        ro::ContentMode::Value,
                        &ro::ReadoutOptions::new(),
                        &mut buf,
                    );
                }
                eprintln!("  {indent}{s_buf}");
                continue;
            }
            // SRD-63 Push 8b: route the per-phase summary
            // line through the `phase_summary` readout
            // instead of hand-rolling the marker / seq /
            // duration formatting here. Same byte output —
            // the readout's branches on `subject_state`
            // produce identical text — but the indent
            // chrome (`"  {indent}"` prefix) stays the
            // surface's job.
            //
            // Zero-duration completions (the `dryrun=phase`
            // sentinel — see executor::run_phase's
            // early-return) render without the " 0.00s"
            // suffix; the dry-run plan view stays clean.
            let total = phases_only.len();
            let ctx = SummaryRowContext {
                name: phase.name.clone(),
                labels: phase.labels.clone(),
                seq: phase.seq.map(|s| (s, total)),
                duration_secs: phase.duration_secs.unwrap_or(0.0),
                state: match &phase.status {
                    PhaseStatus::Completed   => ro::LifecycleState::Completed,
                    PhaseStatus::Running     => ro::LifecycleState::Running,
                    PhaseStatus::Pending     => ro::LifecycleState::Pending,
                    PhaseStatus::Failed(err) => ro::LifecycleState::Failed(err.clone()),
                },
            };
            let mut s_buf = String::with_capacity(64);
            {
                let mut buf = ro::buf::StringBuf::new(&mut s_buf);
                use ro::Readout;
                ro::builtins::phase_summary::PhaseSummary.render(
                    &ctx,
                    ro::Lod::Labeled,
                    ro::ContentMode::Value,
                    &ro::ReadoutOptions::new(),
                    &mut buf,
                );
            }
            eprintln!("  {indent}{s_buf}");

            if let Some(fi) = last_failed {
                if i > fi {
                    printed_after_failure += 1;
                }
            }
        }

        // SRD-63 Push 9h: route the post-failure tail-trim
        // rollup through the `truncated_phases` readout.
        // Stamps the count onto the session ctx and fires;
        // the readout returns zero bytes when count == 0,
        // so no `if` guard is needed at the call site.
        session_ctx.truncated.set(truncated_phases);
        let mut s_buf = String::with_capacity(128);
        {
            let mut buf = ro::buf::StringBuf::new(&mut s_buf);
            use ro::Readout;
            ro::builtins::truncated_phases::TruncatedPhases.render(
                &session_ctx,
                ro::Lod::Labeled,
                ro::ContentMode::Value,
                &ro::ReadoutOptions::new(),
                &mut buf,
            );
        }
        if !s_buf.is_empty() {
            // Match the prior emission's two-space indent
            // for alignment with the per-phase rows above.
            for line in s_buf.lines() {
                eprintln!("  {line}");
            }
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
                // Push 8c: route through `scope_header`
                // readout — same formatter as the post-run
                // summary above.
                let mut s_buf = String::with_capacity(64);
                {
                    let mut buf = ro::buf::StringBuf::new(&mut s_buf);
                    use ro::Readout;
                    ro::builtins::scope_header::ScopeHeader.render(
                        &ScopeRowContext::new(&scope.name, false),
                        ro::Lod::Labeled,
                        ro::ContentMode::Value,
                        &ro::ReadoutOptions::new(),
                        &mut buf,
                    );
                }
                eprintln!("  {indent}{s_buf}");
            }
            let indent = "  ".repeat(phase.depth);
            // SRD-63 Push 9e: route the failed-phase inset
            // through `phase_summary` with `show_labels=true`
            // so the leaf-coord path lands on the failure
            // line itself. The reader doesn't have to scroll
            // up to recover which iteration failed; the
            // failure block is self-contained. Output
            // matches the prior direct eprintln byte-for-
            // byte: `[!!] {name} ({labels}) ({err})` with
            // labels omitted when empty.
            let err = match &phase.status {
                PhaseStatus::Failed(e) => e.clone(),
                _ => String::new(),
            };
            let ctx = SummaryRowContext {
                name: phase.name.clone(),
                labels: phase.labels.clone(),
                seq: None,
                duration_secs: 0.0,
                state: ro::LifecycleState::Failed(err),
            };
            let mut s_buf = String::with_capacity(96);
            {
                let mut buf = ro::buf::StringBuf::new(&mut s_buf);
                let mut opts = ro::ReadoutOptions::new();
                opts.set("show_labels", ro::OptionValue::Bool(true));
                use ro::Readout;
                ro::builtins::phase_summary::PhaseSummary.render(
                    &ctx,
                    ro::Lod::Labeled,
                    ro::ContentMode::Value,
                    &opts,
                    &mut buf,
                );
            }
            eprintln!("  {indent}{s_buf}");
        }
    }

    // Dump recent log messages on failure for error
    // context. Each line carries elapsed-since-session-
    // start (in fractional seconds) plus severity. Header
    // line names the UTC moment that "0.00000" corresponds
    // to, so the operator can map the relative timestamps
    // back to wall-clock if they need to correlate with
    // other systems.
    //
    // The relative-seconds shape is more readable than full
    // UTC for short-failure dumps: events 0.005 seconds
    // apart show up as `1.34020` / `1.34520` rather than
    // `19:14:32.040` / `19:14:32.045` where the operator
    // has to do mental arithmetic.
    if run_result.is_err() && !s.log_messages.is_empty() {
        // The failure dump honours the same display
        // threshold the live observer used (default Info)
        // so DBG noise stays in the file but doesn't
        // re-spam the console at the end of a failed run.
        // Operators who want the dump fuller pass
        // `loglevel=debug` (or set the env var) — same
        // knob as the live path.
        let display_min = nbrs_activity::observer::display_level();
        let recent: Vec<&LogEntry> = s.log_messages.iter()
            .rev()
            .filter(|e| log_severity_to_level(e.severity) >= display_min)
            .take(20)
            .collect();
        if recent.is_empty() {
            // Everything in the ring was below the
            // display threshold; nothing useful to show.
            return;
        }
        eprintln!();
        eprintln!("--- recent log messages ---");
        eprintln!("  (elapsed-seconds since session start at {} UTC)",
            nbrs_activity::session::format_log_timestamp(s.started_at_utc));
        for entry in recent.into_iter().rev() {
            let elapsed = entry.at.duration_since(s.started_at_utc)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            let ts = format_elapsed_seconds(elapsed);
            let level = log_severity_to_level(entry.severity);
            let lvl_tag = match entry.severity {
                crate::state::LogSeverity::Debug => "DBG",
                crate::state::LogSeverity::Info  => "INF",
                crate::state::LogSeverity::Warn  => "WRN",
                crate::state::LogSeverity::Error => "ERR",
            };
            // Color the entire line by severity so the
            // dump matches the live console look.
            let line = format!("  {ts} {lvl_tag} {}", entry.message);
            eprintln!("{}",
                nbrs_activity::observer::colorize_log_line(level, &line));
        }
        eprintln!("---");
    }
}

/// Render `elapsed_secs` so the integer-seconds part takes
/// just enough columns and the fractional part fills the
/// remaining width up to a 7-character target. Once the
/// session's been running long enough that the integer part
/// needs ≥4 columns, the fractional part stabilises at 3
/// digits (millisecond precision) and the seconds column
/// keeps growing.
///
/// Examples:
/// -   `0.00012` → `0.00012` (5 fractional digits — early-
///     session events, dense)
/// -   `9.91234` → `9.91234`
/// -  `12.5`     → `12.5000` (4 fractional digits)
/// - `123.4`     → `123.400` (3 fractional digits)
/// - `1234.5678` → `1234.568` (3 fractional digits, integer widens)
fn format_elapsed_seconds(elapsed_secs: f64) -> String {
    let s = elapsed_secs.max(0.0);
    if s < 10.0 {
        format!("{s:.5}")          // 0.00000 → 9.99999  (7 chars)
    } else if s < 100.0 {
        format!("{s:.4}")          // 10.0000 → 99.9999 (7 chars)
    } else if s < 1000.0 {
        format!("{s:.3}")          // 100.000 → 999.999 (7 chars)
    } else {
        format!("{s:.3}")          // 1000.000 and beyond — 3 millis fixed,
                                   // integer part widens (8+ chars)
    }
}

#[cfg(test)]
mod elapsed_format_tests {
    use super::format_elapsed_seconds;

    #[test]
    fn buckets_widen_seconds_keep_seven_until_thousand() {
        // [0, 10): one integer digit + 5 fractional
        assert_eq!(format_elapsed_seconds(0.0),       "0.00000");
        assert_eq!(format_elapsed_seconds(0.000123),  "0.00012");
        assert_eq!(format_elapsed_seconds(9.99999),   "9.99999");
        // [10, 100): two integer digits + 4 fractional
        assert_eq!(format_elapsed_seconds(10.0),      "10.0000");
        assert_eq!(format_elapsed_seconds(99.9999),   "99.9999");
        // [100, 1000): three integer digits + 3 fractional
        assert_eq!(format_elapsed_seconds(100.0),     "100.000");
        assert_eq!(format_elapsed_seconds(999.999),   "999.999");
        // [1000, ∞): integer widens, millis stays at 3
        assert_eq!(format_elapsed_seconds(1234.5678), "1234.568");
        assert_eq!(format_elapsed_seconds(12345.6),   "12345.600");
    }

    #[test]
    fn negative_or_clock_skew_clamps_to_zero() {
        // SystemTime can run backward across a clock
        // adjustment. Don't surface a negative-elapsed
        // string; just clamp.
        assert_eq!(format_elapsed_seconds(-0.5), "0.00000");
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
