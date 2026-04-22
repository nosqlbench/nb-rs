// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! cassnbrs — Cassandra/CQL persona for nb-rs.
//!
//! A thin shell that links the CQL adapter crate (which registers
//! itself via inventory) and delegates to the shared runner.

// Link the CQL adapter + GK nodes (inventory registration happens at link time).
extern crate cassnbrs_adapter_cql;

// Link the standard adapters.
extern crate nb_adapter_stdout;
extern crate nb_adapter_http;
extern crate nb_adapter_testkit;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use nb_metrics::snapshot::MetricSet;
use nb_metrics::cadence::Cadences;
use nb_metrics::metrics_query::MetricsQuery;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args.first().map(|s| s.as_str()) == Some("--help") {
        print_usage();
        return;
    }

    // Handle `completions` / hidden `__complete` before doing any
    // other work. These subcommands must never touch the CQL driver,
    // open connections, or write to stderr — a stray line would land
    // in the user's shell mid-tab.
    let completion_spec = nb_activity::completions::CompletionSpec {
        binary_name: "cassnbrs",
        subcommands: &["run", "completions"],
        run_params: nb_activity::runner::KNOWN_PARAMS,
    };
    if nb_activity::completions::handle_if_match(&completion_spec, &args) {
        return;
    }

    // Detect TUI mode: auto-enable when stderr is a TTY, disable with tui=off
    // Skip subcommand ("run") for param parsing
    let param_args: Vec<String> = args.iter()
        .filter(|a| a.contains('=') || a.ends_with(".yaml") || a.ends_with(".yml"))
        .cloned()
        .collect();
    let params = nb_activity::runner::parse_params(&param_args);
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stderr());
    let has_dryrun = params.contains_key("dryrun");

    // Check adapter display preferences — if any adapter needs raw terminal, TUI must not activate.
    let adapter_name = params.get("adapter").or(params.get("driver"))
        .map(|s| s.as_str()).unwrap_or("cql");
    let adapter_pref = nb_activity::adapter::adapter_display_preference(adapter_name);

    let tui_mode = params.get("tui").map(|s| s.as_str()).unwrap_or(
        if adapter_pref == nb_activity::adapter::DisplayPreference::Off {
            "off"
        } else if is_tty && !has_dryrun {
            "on"
        } else {
            "off"
        }
    );

    let rt = tokio::runtime::Runtime::new().unwrap();

    if tui_mode == "on" {
        // Suppress CQL driver warnings when TUI owns the screen
        cassandra_cpp::set_level(cassandra_cpp::LogLevel::ERROR);

        // Create shared state and TUI observer. The TUI thread is NOT spawned
        // here — it starts only when the first phase actually begins, so
        // startup and any pre-phase failures leave the terminal untouched.
        let run_state = Arc::new(RwLock::new(nb_tui::state::RunState::new(
            params.get("workload").map(|s| s.as_str()).unwrap_or("?"),
            params.get("scenario").map(|s| s.as_str()).unwrap_or("default"),
            params.get("adapter").or(params.get("driver")).map(|s| s.as_str()).unwrap_or("cql"),
        )));

        // Set profiler/limit info
        if let Ok(mut s) = run_state.write() {
            s.profiler = params.get("profiler").cloned().unwrap_or_else(|| "off".into());
            s.limit = params.get("limit").cloned().unwrap_or_else(|| "none".into());
        }

        // Parse user-declared latency cadences. Defaults if omitted.
        // Unknown / bad values fall back to defaults with a warning.
        let cadences = params.get("latency-cadences")
            .or_else(|| params.get("latency_cadences"))
            .and_then(|s| match Cadences::parse(s) {
                Ok(c) => Some(c),
                Err(e) => {
                    eprintln!("warning: latency-cadences='{s}': {e} — using defaults");
                    None
                }
            })
            .unwrap_or_else(Cadences::defaults);

        let observer = Arc::new(TuiObserver::new(run_state.clone(), cadences));

        // Run the workload. TuiObserver spawns the TUI thread lazily on the
        // first phase_starting event.
        let run_result = rt.block_on(async {
            nb_activity::runner::run_with_observer(&args, observer.clone()).await
        });

        // Signal the TUI (if it ever started) to exit and wait for terminal
        // restoration before any further stderr writes.
        observer.shutdown();

        // Always print a post-run summary so the user sees what happened
        // after the TUI alternate screen is torn down. Otherwise successful
        // runs leave the terminal with no indication anything occurred.
        print_post_run_summary(&run_state, &run_result);

        if let Err(ref e) = run_result {
            eprintln!("error: {e}");
            std::process::exit(1);
        }

        // Detect phases that were pre-mapped (i.e. reachable by scenario
        // traversal) but never reached Running status. A zero-cycle phase
        // is fine when the data source legitimately has no data — that
        // phase still transitions Pending → Running → Completed. What this
        // catches is phases the executor should have visited but didn't.
        if let Ok(s) = run_state.read() {
            let unreached: Vec<&nb_tui::state::PhaseEntry> = s.phases.iter()
                .filter(|p| p.kind == nb_tui::state::EntryKind::Phase
                    && matches!(p.status, nb_tui::state::PhaseStatus::Pending))
                .collect();
            if !unreached.is_empty() {
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
                std::process::exit(2);
            }
        }

    } else {
        // No TUI — standard stderr output
        rt.block_on(async {
            if let Err(e) = nb_activity::runner::run(&args).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        });
    }
}

/// TUI observer: updates RunState from executor lifecycle events.
///
/// The TUI terminal takeover (raw mode + alternate screen) is deferred
/// until the first phase actually starts. Pre-phase work — workload
/// parsing, GK compilation, session setup — runs with plain stderr
/// output. This keeps the terminal clean if the run fails before any
/// activity begins.
struct TuiObserver {
    state: Arc<RwLock<nb_tui::state::RunState>>,
    /// Base-cadence frame reporter, taken by the runner via
    /// `reporters()`. Forwards every scheduler tick's delta frame
    /// into the TUI so history rings + sparklines stay live.
    reporter: Mutex<Option<nb_tui::reporter::TuiReporter>>,
    /// Receiver end of the base-frame channel. Taken when the TUI
    /// thread is spawned on the first phase start.
    frame_rx: Mutex<Option<mpsc::Receiver<MetricSet>>>,
    /// User-declared cadences for the TUI's barchart view.
    /// Surfaced to the runner through `RunObserver::cadences()` so
    /// the cadence tree is planned with these values.
    cadences: Cadences,
    /// Shared `MetricsQuery` handle — populated by the runner via
    /// `on_metrics_query` once the cadence reporter is built.
    metrics_query: Mutex<Option<Arc<MetricsQuery>>>,
    /// Join handle for the TUI thread, once spawned.
    tui_handle: Mutex<Option<JoinHandle<()>>>,
    /// True once the TUI thread has been spawned and owns the terminal.
    /// Shared with the thread so it can flip it false on exit — which
    /// routes `log()` and phase lifecycle events back to stderr so the
    /// run keeps producing visible output if the user pressed `q`.
    tui_active: Arc<AtomicBool>,
}

impl TuiObserver {
    fn new(
        state: Arc<RwLock<nb_tui::state::RunState>>,
        cadences: Cadences,
    ) -> Self {
        let (reporter, frame_rx) = nb_tui::reporter::TuiReporter::channel();
        Self {
            state,
            reporter: Mutex::new(Some(reporter)),
            frame_rx: Mutex::new(Some(frame_rx)),
            tui_handle: Mutex::new(None),
            tui_active: Arc::new(AtomicBool::new(false)),
            cadences,
            metrics_query: Mutex::new(None),
        }
    }

    /// Spawn the TUI thread on first use. Subsequent calls are no-ops.
    fn ensure_tui_started(&self) {
        if self.tui_active.load(Ordering::Acquire) {
            return;
        }
        let rx = match self.frame_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => return, // already claimed
        };
        let query = match self.metrics_query.lock().unwrap().clone() {
            Some(q) => q,
            None => return, // runner hasn't wired the query yet
        };
        let state = self.state.clone();
        let tui_active = self.tui_active.clone();
        let handle = std::thread::spawn(move || {
            let mut app = nb_tui::app::App::new(rx, state.clone(), query);
            if let Err(e) = app.run() {
                eprintln!("TUI error: {e}");
            }
            // TUI thread has exited and restored the terminal. If the
            // run is still going (user hit `q` mid-run), flip the
            // active flag FIRST so subsequent log() / phase_starting()
            // calls route to stderr. Then replay the captured log ring
            // buffer so the console looks as if tui=off were in effect
            // from the start, plus a notice indicating the fallback.
            let was_active = tui_active.swap(false, Ordering::AcqRel);
            let run_finished = state.read().map(|s| s.finished).unwrap_or(true);
            if was_active && !run_finished {
                if let Ok(s) = state.read() {
                    for entry in &s.log_messages {
                        eprintln!("{}", entry.message);
                    }
                }
                eprintln!("--- tui disabled (q pressed); falling back to tui=off mode ---");
            }
        });
        *self.tui_handle.lock().unwrap() = Some(handle);
        self.tui_active.store(true, Ordering::Release);
    }

    /// Signal the TUI (if running) to exit and wait for the terminal to
    /// be restored. Safe to call when the TUI never started.
    fn shutdown(&self) {
        if let Ok(mut s) = self.state.write() {
            s.finished = true;
        }
        let handle = self.tui_handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl nb_activity::observer::RunObserver for TuiObserver {
    fn phase_starting(&self, name: &str, labels: &str, op_count: usize, concurrency: usize) {
        self.ensure_tui_started();
        if let Ok(mut s) = self.state.write() {
            s.set_phase_running(name, labels, op_count);
            let key = (name.to_string(), labels.to_string());
            // Sparkline capacity = bar width used by latency_detail_lines
            // so the throughput row aligns with the latency rows.
            let summary = std::sync::Arc::new(
                nb_metrics::summaries::binomial_summary::BinomialSummary::new(60),
            );
            // Smoothed rate readout: 1 s half-life is short enough
            // to track real throughput changes but long enough to
            // stop the raw value from flickering between frames.
            let rate_ewma = std::sync::Arc::new(
                nb_metrics::summaries::ewma::Ewma::new(
                    std::time::Duration::from_secs(1),
                ),
            );
            // Rolling latency peaks for the ╪ / ╫ cross-bar markers
            // on the latency range row. Each phase owns its own
            // trackers so the markers are phase-scoped, not
            // session-scoped.
            let latency_peak_5s = std::sync::Arc::new(
                nb_metrics::summaries::peak_tracker::PeakTracker::max(
                    std::time::Duration::from_secs(5),
                ),
            );
            let latency_peak_10s = std::sync::Arc::new(
                nb_metrics::summaries::peak_tracker::PeakTracker::max(
                    std::time::Duration::from_secs(10),
                ),
            );
            s.active_phases.insert(key, nb_tui::state::ActivePhase {
                name: name.to_string(),
                labels: labels.to_string(),
                cursor_name: "?".into(),
                cursor_extent: 0,
                fibers: concurrency,
                started_at: std::time::Instant::now(),
                ops_started: 0,
                ops_finished: 0,
                ops_ok: 0,
                errors: 0,
                retries: 0,
                ops_per_sec: 0.0,
                adapter_counters: Vec::new(),
                rows_per_batch: 0.0,
                relevancy: Vec::new(),
                throughput_summary: summary,
                rate_ewma,
                latency_peak_5s,
                latency_peak_10s,
            });
            // Sparklines are scoped to the currently-running phase,
            // so the history rings reset on every phase boundary.
            // Otherwise a short ann_query phase would show several
            // seconds of rampup throughput instead of its own.
            // (Will be replaced by per-phase binomial-summary
            // instruments — see task #64 / SRD 62.)
            s.ops_history.clear();
            s.rows_history.clear();
            s.rows_sparkline_label = None;
        }
        if !self.tui_active.load(Ordering::Acquire) {
            eprintln!("phase '{name}': {op_count} ops, concurrency={concurrency}");
        }
    }

    fn phase_completed(&self, name: &str, labels: &str, duration_secs: f64) {
        if let Ok(mut s) = self.state.write() {
            // Snapshot the active phase's end-of-run metrics before
            // removing it, so the tree entry keeps a record after the
            // phase ends. Captures the same fields the non-TUI
            // progress bar renders, so a later expansion reads
            // equivalently.
            let key = (name.to_string(), labels.to_string());
            let min_ns = s.min_nanos;
            let p50_ns = s.p50_nanos;
            let p99_ns = s.p99_nanos;
            let max_ns = s.max_nanos;
            let summary = s.active_phases.get(&key).map(|a| nb_tui::state::PhaseSummary {
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
                // Freeze the throughput sparkline as a durable
                // artifact — SRD 62 §"Design notes → Per-phase
                // sparkline" calls this out as the expected
                // behavior. The live Arc<BinomialSummary> is
                // dropped with the ActivePhase below; this
                // Vec<f64> keeps the rendered curve available.
                throughput_samples: a.throughput_summary.snapshot(),
            }).unwrap_or_default();
            s.set_phase_completed(name, labels, duration_secs, summary);
            s.active_phases.remove(&key);
        }
        if !self.tui_active.load(Ordering::Acquire) {
            eprintln!("phase '{name}' complete ({duration_secs:.2}s)");
        }
    }

    fn phase_failed(&self, name: &str, labels: &str, error: &str) {
        if let Ok(mut s) = self.state.write() {
            s.set_phase_failed(name, labels, error);
            s.active_phases.remove(&(name.to_string(), labels.to_string()));
        }
        if !self.tui_active.load(Ordering::Acquire) {
            eprintln!("phase '{name}' FAILED: {error}");
        }
    }

    fn phase_progress(&self, update: &nb_activity::observer::PhaseProgressUpdate) {
        if let Ok(mut s) = self.state.write() {
            if let Some(active) = s.active_phase_mut(&update.name, &update.labels) {
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
                    .map(|r| (r.name.clone(), r.window_mean, r.total_mean, r.total_count, r.window_len))
                    .collect();
                // Feed the phase's binomial sparkline summary.
                // `record` is cheap and auto-reduces when capacity
                // is reached, so we don't need to sample-gate here.
                active.throughput_summary.record(update.ops_per_sec);
                // Smoothed rate readout for the detail-block text.
                active.rate_ewma.record_now(update.ops_per_sec);
            }
        }
    }

    fn run_finished(&self) {
        if let Ok(mut s) = self.state.write() {
            s.finished = true;
        }
    }

    fn log(&self, level: nb_activity::observer::LogLevel, message: &str) {
        if let Ok(mut s) = self.state.write() {
            let severity = match level {
                nb_activity::observer::LogLevel::Debug => nb_tui::state::LogSeverity::Debug,
                nb_activity::observer::LogLevel::Info => nb_tui::state::LogSeverity::Info,
                nb_activity::observer::LogLevel::Warn => nb_tui::state::LogSeverity::Warn,
                nb_activity::observer::LogLevel::Error => nb_tui::state::LogSeverity::Error,
            };
            s.push_log(severity, message.to_string());
        }
        // Before the TUI claims the terminal, mirror log messages to
        // stderr so pre-phase diagnostics (session id, metrics path,
        // scenario tree, warnings) are visible to the user.
        if !self.tui_active.load(Ordering::Acquire) {
            eprintln!("{message}");
        }
    }

    fn suppresses_stderr(&self) -> bool {
        self.tui_active.load(Ordering::Acquire)
    }

    fn scenario_pre_mapped(&self, entries: &[(nb_activity::observer::PreMapKind, String, String, usize)]) {
        if let Ok(mut s) = self.state.write() {
            for (kind, name, labels, depth) in entries {
                match kind {
                    nb_activity::observer::PreMapKind::Phase => {
                        s.add_phase(name, labels, *depth);
                    }
                    nb_activity::observer::PreMapKind::Scope => {
                        s.add_scope(name, *depth);
                    }
                }
            }
        }
    }

    fn reporters(&self) -> Vec<(Duration, Box<dyn nb_metrics::scheduler::Reporter>)> {
        let mut guard = self.reporter.lock().unwrap();
        let Some(r) = guard.take() else { return Vec::new(); };
        vec![(Duration::from_secs(1), Box::new(r) as Box<dyn nb_metrics::scheduler::Reporter>)]
    }

    fn cadences(&self) -> Option<Cadences> {
        Some(self.cadences.clone())
    }

    fn on_metrics_query(&self, query: Arc<MetricsQuery>) {
        *self.metrics_query.lock().unwrap() = Some(query);
    }
}

/// Print a summary of the run after the TUI has torn down. Without this,
/// successful runs leave the terminal with no indication anything happened.
fn print_post_run_summary(
    run_state: &Arc<RwLock<nb_tui::state::RunState>>,
    run_result: &Result<(), String>,
) {
    let s = match run_state.read() {
        Ok(s) => s,
        Err(_) => return,
    };

    eprintln!();
    eprintln!("session: {} ({})", s.scenario_name, s.workload_file);
    eprintln!("logs:    logs/latest/");

    // Count phases only (scope headers are visual, not executable).
    let phases_only: Vec<&nb_tui::state::PhaseEntry> = s.phases.iter()
        .filter(|p| p.kind == nb_tui::state::EntryKind::Phase)
        .collect();
    if phases_only.is_empty() {
        eprintln!("phases:  none executed");
    } else {
        let completed = phases_only.iter().filter(|p| {
            matches!(p.status, nb_tui::state::PhaseStatus::Completed)
        }).count();
        let failed = phases_only.iter().filter(|p| {
            matches!(p.status, nb_tui::state::PhaseStatus::Failed(_))
        }).count();
        let pending = phases_only.iter().filter(|p| {
            matches!(p.status, nb_tui::state::PhaseStatus::Pending)
        }).count();
        eprintln!("phases:  {} completed, {} failed, {} not run (of {} total)",
            completed, failed, pending, phases_only.len());

        // When there's a failure, printing every pending phase after
        // it gives screens of noise. Trim the tail: show at most a
        // small window of phases after the last failure, then summarize
        // the rest as "(... and N more phases)" with a pointer at
        // `dryrun=phase` for the full plan.
        let last_failed: Option<usize> = s.phases.iter().enumerate()
            .rev()
            .find(|(_, p)| matches!(p.status, nb_tui::state::PhaseStatus::Failed(_)))
            .map(|(i, _)| i);
        let pending_tail_limit: usize = 6; // phases (not scope lines) after last failure
        let mut printed_after_failure: usize = 0;
        let mut truncated_phases: usize = 0;

        for (i, phase) in s.phases.iter().enumerate() {
            let indent = "  ".repeat(phase.depth);

            // Truncation guard: once we're past the last failure and
            // past the small tail window, count the remainder for the
            // summary line and skip the output.
            if let Some(fi) = last_failed {
                if i > fi && printed_after_failure >= pending_tail_limit {
                    if phase.kind == nb_tui::state::EntryKind::Phase {
                        truncated_phases += 1;
                    }
                    continue;
                }
            }

            if phase.kind == nb_tui::state::EntryKind::Scope {
                // Group header — no status glyph. The header text lives
                // in `labels` (set by `add_scope`).
                eprintln!("  {indent}· {}", phase.labels);
                continue;
            }
            let (marker, status_str) = match &phase.status {
                nb_tui::state::PhaseStatus::Completed => ("[ok]", String::new()),
                nb_tui::state::PhaseStatus::Running => ("[..]", " (still running)".into()),
                nb_tui::state::PhaseStatus::Pending => ("[  ]", " (not run)".into()),
                nb_tui::state::PhaseStatus::Failed(err) => ("[!!]", format!(" ({err})")),
            };
            let labels = if phase.labels.is_empty() {
                String::new()
            } else {
                format!(" ({})", phase.labels)
            };
            let dur = phase.duration_secs
                .map(|d| format!(" {d:.2}s"))
                .unwrap_or_default();
            eprintln!("  {indent}{marker} {}{labels}{dur}{status_str}",
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

    // Focused error inset: for each failed phase, print the chain of
    // for_each / for_combinations / do_while scopes that enclose it,
    // then the failed phase itself. The reader gets the exact binding
    // context that led to the failure without having to scan the full
    // phase tree above.
    let failed: Vec<(usize, &nb_tui::state::PhaseEntry)> = s.phases.iter().enumerate()
        .filter(|(_, p)| p.kind == nb_tui::state::EntryKind::Phase
            && matches!(p.status, nb_tui::state::PhaseStatus::Failed(_)))
        .collect();
    if !failed.is_empty() {
        eprintln!();
        eprintln!("failures:");
        for (idx, phase) in &failed {
            for scope_idx in scope_ancestors(&s.phases, *idx) {
                let scope = &s.phases[scope_idx];
                let indent = "  ".repeat(scope.depth);
                eprintln!("  {indent}· {}", scope.labels);
            }
            let indent = "  ".repeat(phase.depth);
            let labels = if phase.labels.is_empty() {
                String::new()
            } else {
                format!(" ({})", phase.labels)
            };
            let err_text = match &phase.status {
                nb_tui::state::PhaseStatus::Failed(err) => format!(" ({err})"),
                _ => String::new(),
            };
            eprintln!("  {indent}[!!] {}{labels}{err_text}", phase.name);
        }
    }

    // Dump recent log messages on failure for error context.
    if run_result.is_err() && !s.log_messages.is_empty() {
        eprintln!();
        eprintln!("--- recent log messages ---");
        let recent: Vec<&nb_tui::state::LogEntry> = s.log_messages.iter().rev().take(20).collect();
        for entry in recent.into_iter().rev() {
            eprintln!("  {}", entry.message);
        }
        eprintln!("---");
    }
}

/// Return the indices of scope entries that enclose `target_idx`,
/// ordered outermost-first. Walks backward from the target collecting
/// the nearest scope at each strictly-shallower depth.
fn scope_ancestors(phases: &[nb_tui::state::PhaseEntry], target_idx: usize) -> Vec<usize> {
    if target_idx >= phases.len() { return Vec::new(); }
    let mut needed_depth = phases[target_idx].depth;
    let mut ancestors: Vec<usize> = Vec::new();
    for i in (0..target_idx).rev() {
        let p = &phases[i];
        if p.kind == nb_tui::state::EntryKind::Scope && p.depth < needed_depth {
            ancestors.push(i);
            needed_depth = p.depth;
            if needed_depth == 0 { break; }
        }
    }
    ancestors.reverse();
    ancestors
}

fn print_usage() {
    eprintln!("cassnbrs — Cassandra/CQL workload testing with nb-rs");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  cassnbrs run adapter=cql hosts=<hosts> workload=<file.yaml>");
    eprintln!("  cassnbrs run adapter=stdout workload=file.yaml");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  tui=on|off       Enable/disable TUI (auto-detected from TTY)");
    eprintln!("  profiler=flamegraph|perf   Built-in CPU profiling");
    eprintln!("  limit=N          Cap cursor extent for smoke testing");
    eprintln!();
    eprintln!("Adapters: {}", nb_activity::adapter::registered_driver_names().join(", "));
}
