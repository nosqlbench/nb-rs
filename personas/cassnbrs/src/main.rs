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

use std::sync::{Arc, RwLock};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args.first().map(|s| s.as_str()) == Some("--help") {
        print_usage();
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
    let tui_mode = params.get("tui").map(|s| s.as_str()).unwrap_or(
        if is_tty { "on" } else { "off" }
    );

    let rt = tokio::runtime::Runtime::new().unwrap();

    if tui_mode == "on" {
        // Suppress CQL driver warnings when TUI owns the screen
        cassandra_cpp::set_level(cassandra_cpp::LogLevel::ERROR);

        // Create shared state and TUI observer
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

        let observer = Arc::new(TuiObserver::new(run_state.clone()));

        // Start TUI on a dedicated thread
        let tui_state = run_state.clone();
        let (tui_reporter, tui_rx) = nb_tui::reporter::TuiReporter::channel();
        let tui_handle = std::thread::spawn(move || {
            let mut app = nb_tui::app::App::new(tui_rx, tui_state);
            if let Err(e) = app.run() {
                eprintln!("TUI error: {e}");
            }
        });

        // Store the reporter on the observer so it can be returned
        observer.set_reporter(tui_reporter);

        // Run the workload
        let run_result = rt.block_on(async {
            nb_activity::runner::run_with_observer(&args, observer).await
        });

        // Signal TUI to exit and wait for it
        if let Ok(mut s) = run_state.write() {
            s.finished = true;
        }
        let _ = tui_handle.join();

        if let Err(e) = run_result {
            eprintln!("error: {e}");
            std::process::exit(1);
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
struct TuiObserver {
    state: Arc<RwLock<nb_tui::state::RunState>>,
    reporter: std::sync::Mutex<Option<nb_tui::reporter::TuiReporter>>,
}

impl TuiObserver {
    fn new(state: Arc<RwLock<nb_tui::state::RunState>>) -> Self {
        Self {
            state,
            reporter: std::sync::Mutex::new(None),
        }
    }

    fn set_reporter(&self, reporter: nb_tui::reporter::TuiReporter) {
        *self.reporter.lock().unwrap() = Some(reporter);
    }
}

impl nb_activity::observer::RunObserver for TuiObserver {
    fn phase_starting(&self, name: &str, labels: &str, op_count: usize, concurrency: usize) {
        if let Ok(mut s) = self.state.write() {
            s.set_phase_running(name, labels, op_count);
            s.active = Some(nb_tui::state::ActivePhase {
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
            });
        }
    }

    fn phase_completed(&self, name: &str, labels: &str, duration_secs: f64) {
        if let Ok(mut s) = self.state.write() {
            s.set_phase_completed(name, labels, duration_secs);
            s.active = None;
        }
    }

    fn phase_failed(&self, name: &str, labels: &str, error: &str) {
        if let Ok(mut s) = self.state.write() {
            s.set_phase_failed(name, labels, error);
            s.active = None;
        }
    }

    fn phase_progress(&self, update: &nb_activity::observer::PhaseProgressUpdate) {
        if let Ok(mut s) = self.state.write() {
            if let Some(ref mut active) = s.active {
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
            }
        }
    }

    fn run_finished(&self) {
        if let Ok(mut s) = self.state.write() {
            s.finished = true;
        }
    }

    fn suppresses_stderr(&self) -> bool { true }

    fn reporter(&self) -> Option<Box<dyn nb_metrics::scheduler::Reporter>> {
        let mut guard = self.reporter.lock().unwrap();
        guard.take().map(|r| Box::new(r) as Box<dyn nb_metrics::scheduler::Reporter>)
    }
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
