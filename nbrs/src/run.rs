// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The `run` subcommand: workload execution with optional TUI.
//!
//! nbrs registers its adapters (stdout, http, testkit, plotter,
//! cql/scylla, optionally cassandra-cpp) via inventory at link
//! time. This module decides between two execution paths:
//!
//! - **TUI mode** — stderr is a TTY, no `dryrun=`, the adapter
//!   doesn't claim raw terminal. Builds an
//!   [`nbrs_tui::observer::TuiObserver`], runs via
//!   [`nbrs_activity::runner::run_with_observer`], prints a
//!   post-teardown summary, and exits 2 if any pre-mapped
//!   phases were skipped.
//! - **Plain mode** — stderr is not a TTY, or `tui=off`, or a
//!   raw-terminal adapter (e.g. `plotter`). Falls through to
//!   `runner::run` with the default stderr observer.
//!
//! `tui=on|off` overrides auto-detection.

// Link adapter crates for inventory registration.
extern crate nbrs_adapter_stdout;
extern crate nbrs_adapter_http;
extern crate nbrs_adapter_testkit;
extern crate nbrs_adapter_plotter;
// CQL adapter — `default-features = false` in Cargo.toml; nbrs's
// own engine-* features forward into it. The always-on `common`
// module registers `adapter=cql`; the engine modules contribute
// `DriverImpl`s selected at runtime via `cqldriver=`.
extern crate nbrs_adapter_cql;

use std::sync::Arc;

use nbrs_metrics::cadence::Cadences;
use nbrs_tui::observer::{print_post_run_summary, unreached_phase_exit_code, TuiObserver};
use nbrs_tui::run_state_actor::{spawn_run_state_actor, RunStateCmd};
use nbrs_tui::state::RunState;

pub async fn run_command(args: &[String]) {
    // Parse only `key=value` and workload-file args for mode
    // detection. Skip the `run` subcommand token itself.
    let param_args: Vec<String> = args.iter()
        .filter(|a| a.contains('=') || a.ends_with(".yaml") || a.ends_with(".yml"))
        .cloned()
        .collect();
    let params = nbrs_activity::runner::parse_params(&param_args);
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stderr());
    let has_dryrun = params.contains_key("dryrun");

    // Adapters that need raw terminal output (e.g. plotter)
    // override TUI detection — checked at startup before any
    // adapter is constructed.
    let adapter_name = params.get("adapter").or(params.get("driver"))
        .map(|s| s.as_str()).unwrap_or("stdout");
    let adapter_pref = nbrs_activity::adapter::adapter_display_preference(adapter_name);

    // Three-mode lattice. Default is `terminal` for interactive
    // sessions: line-mode rendering driven by the snapshot stream
    // (see `nbrs_tui::log_only_sink`). `on` is explicit opt-in
    // for the full raw-mode TUI. `off` strips the sink entirely
    // — used when an adapter needs unfettered terminal access
    // (plotter, anything writing cursor controls of its own) or
    // when the operator wants bare stderr output for piping/CI.
    //
    // Adapter override: if the adapter declares
    // `DisplayPreference::Off`, the mode collapses to `off`
    // regardless of what the user asked for, with a log line
    // explaining the override.
    let user_tui = params.get("tui").map(|s| s.as_str());
    let tui_mode: &str = if adapter_pref == nbrs_activity::adapter::DisplayPreference::Off {
        if let Some(req) = user_tui
            && req != "off"
        {
            eprintln!(
                "display: adapter '{adapter_name}' requires exclusive terminal — \
                 forcing tui=off (overriding tui={req})"
            );
        }
        "off"
    } else if let Some(req) = user_tui {
        req
    } else if is_tty && !has_dryrun {
        "terminal"
    } else {
        "off"
    };

    // Spawn the RunState actor + inspector socket *before* the
    // tui-mode branch. The actor was originally TUI-only and the
    // inspector got tied to it by accident — but the high-value
    // inspector commands (`controls`/`set`/`metrics`/`metric`/
    // `:pin`) walk the live component tree via
    // `runtime_context::session_root_handle()` and don't need a
    // populated RunState. Lifting the spawn above the branch lets
    // `nbrs attach` reach a tui=off run for those commands too.
    // Legacy commands (`meta`/`phases`/`active`/`latency`/`tree`/
    // `log`) read the RunState; in tui=off mode the actor is
    // unpopulated and they return empty/defaults. The TUI thread
    // is still NOT spawned here — the observer starts it lazily
    // on the first `phase_starting` event so pre-phase failures
    // leave the terminal untouched.
    let (run_state, run_state_join) = spawn_run_state_actor(RunState::new(
        params.get("workload").map(|s| s.as_str()).unwrap_or("?"),
        params.get("scenario").map(|s| s.as_str()).unwrap_or("default"),
        params.get("adapter").or(params.get("driver"))
            .map(|s| s.as_str()).unwrap_or(adapter_name),
    ));
    run_state.send(RunStateCmd::SetMeta {
        profiler: Some(params.get("profiler").cloned().unwrap_or_else(|| "off".into())),
        limit:    Some(params.get("limit").cloned().unwrap_or_else(|| "none".into())),
    });

    // Capture the current tokio runtime handle so the inspector
    // server thread (a sync OS thread, not a tokio worker) can
    // dispatch async control writes via `handle.block_on(...)`
    // when an inspector client issues `set <name> <value>`. The
    // block_on runs on the per-connection thread, never on a
    // runtime worker, so no executor starvation. Bind failures
    // (read-only fs, socket name collision) don't abort the run;
    // the inspector just stays disabled with a warning.
    let runtime_handle = tokio::runtime::Handle::try_current().ok();
    let _inspector_join = match nbrs_tui::inspector_server::spawn(
        run_state.clone(), runtime_handle,
    ) {
        Ok((path, join)) => {
            eprintln!("inspector socket: {}", path.display());
            Some(join)
        }
        Err(e) => {
            eprintln!("warning: inspector endpoint disabled: {e}");
            None
        }
    };

    if tui_mode != "on" {
        // Two non-`on` modes: `terminal` runs a `LogOnlySink`
        // against the snapshot stream; `off` skips the sink
        // entirely (no rendering layer between the observer's
        // direct stderr writes and the user's terminal — used
        // by adapters that own the terminal themselves, like
        // plotter, and for piped/CI output).
        //
        // Both share the `LogOnlyObserver` since the observer's
        // job is just "send commands to the actor"; whether
        // anything renders from those commands is the sink's
        // call. The `sink_active` flag coordinates the handoff:
        // if no sink is up, the observer writes stderr
        // synchronously (legacy behaviour); when the sink
        // claims rendering, the observer suppresses its writes.
        let stripped: &[String] = match args.first().map(|s| s.as_str()) {
            Some("run") => &args[1..],
            _ => args,
        };
        let cli_params = nbrs_activity::runner::parse_params(stripped);
        // dryrun=phase walks the scenario tree purely to dump the
        // plan; the per-phase construction trace ("=== phase: X ===",
        // "phase 'X' (...): N op templates …", "phase 'X' complete")
        // is signal during a real run but pure noise when the user
        // just wants the post-run plan view. Default loglevel up to
        // Warn so the construction Info chatter falls below the
        // stderr threshold; explicit `loglevel=info` still wins.
        let dryrun_phase_default = cli_params.get("dryrun")
            .map(|s| s.split(',').any(|f| f.trim() == "phase" || f.trim() == "controls"))
            .unwrap_or(false);
        let default_min_level = if dryrun_phase_default {
            nbrs_activity::observer::LogLevel::Warn
        } else {
            nbrs_activity::observer::LogLevel::Info
        };
        let stderr_min_level = cli_params.get("loglevel")
            .and_then(|s| nbrs_activity::runner::parse_log_level(s))
            .unwrap_or(default_min_level);
        // Same cadence parsing the `tui=on` path uses, so the
        // metrics scheduler plans the same windows whether the
        // observer eventually drives a LogOnlySink or a TuiSink.
        let cadences = cli_params.get("latency-cadences")
            .or_else(|| cli_params.get("latency_cadences"))
            .and_then(|s| match nbrs_metrics::cadence::Cadences::parse(s) {
                Ok(c) => Some(c),
                Err(e) => {
                    eprintln!("warning: latency-cadences='{s}': {e} — using defaults");
                    None
                }
            })
            .unwrap_or_else(nbrs_metrics::cadence::Cadences::defaults);
        let observer_concrete = nbrs_tui::log_only_observer::LogOnlyObserver::new(
            run_state.clone(), cadences,
        ).with_min_level(stderr_min_level);
        let observer_arc = std::sync::Arc::new(observer_concrete);
        let observer: std::sync::Arc<dyn nbrs_activity::observer::RunObserver> =
            observer_arc.clone();

        // `tui=terminal`: hand off to the SinkSupervisor. The
        // supervisor owns the active sink (`LogOnlySink`
        // initially) plus the `KeyWatcher`, and swaps to
        // `TuiSink` on Ctrl-T (and back on Ctrl-T or `q`
        // inside the TUI). Tears everything down cleanly when
        // the runner future completes via the supervisor's
        // own shutdown handle.
        //
        // `tui=off`: no supervisor, no sink, no keystroke
        // watcher. The observer's `sink_active` stays false;
        // every log line goes straight to stderr through the
        // synchronous `eprintln!` path. Adapters needing
        // exclusive terminal access (plotter) end up here via
        // the adapter-override above.
        let supervisor = if tui_mode == "terminal" {
            Some(nbrs_tui::sink_supervisor::SinkSupervisor::spawn(
                observer_arc.clone(),
                run_state.clone(),
            ))
        } else {
            None
        };

        let run_result = nbrs_activity::runner::run_with_observer(args, observer).await;

        if let Some(s) = supervisor {
            // Two-step teardown so the terminal is **fully
            // restored** before any post-run output fires:
            //
            //   1. Brief grace period (150 ms) so the active
            //      sink can drain the final log lines —
            //      `run_finished` enqueues `all phases
            //      complete` via `observer::log`, which lands
            //      in the actor; the LogOnlySink's 50 ms
            //      poller picks it up.
            //   2. `supervisor.shutdown()` joins the active
            //      sink and the KeyWatcher; the watcher's
            //      drop disables raw mode and the active
            //      TuiSink (if up) leaves the alt-screen.
            //
            // After step 2 returns, the terminal is in its
            // pre-run discipline (cooked mode, no alt-screen,
            // mouse capture off). Anything that writes
            // directly to stderr/stdout before step 2 is a
            // bug — observer-routed `crate::diag!()` calls
            // are the only legal in-run output channel.
            std::thread::sleep(std::time::Duration::from_millis(150));
            s.shutdown();
        }

        // From here down the terminal is back in cooked mode
        // (or we never claimed it — `tui=off` path). Post-run
        // reports / errors are safe to print.
        print_post_run_reports(&run_state, &run_result);

        if let Err(e) = run_result {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
        if let Some(code) = unreached_phase_exit_code(&run_state) {
            std::process::exit(code);
        }
        // Keep the actor join + run_state alive until the run
        // returns so the inspector socket stays serviceable for
        // the duration. Drop on return.
        let _ = run_state_join;
        let _ = run_state;
        return;
    }

    // Suppress C++ CQL driver chatter when the TUI owns the
    // screen. Only relevant when the cassandra-cpp engine is
    // built in; the scylla engine uses `tracing` so its log
    // levels are controlled via env (RUST_LOG / SCYLLA_LOG).
    #[cfg(feature = "engine-cassandra-cpp")]
    cassandra_cpp::set_level(cassandra_cpp::LogLevel::ERROR);

    // Parse user-declared latency cadences. Defaults if
    // omitted; bad values fall back to defaults with a warning.
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

    // Match what runner.rs::run does for tui=off: parse
    // `loglevel=` and apply it as the stderr-fallback severity
    // filter. The TUI's in-app log panel filters separately
    // (own LOD knobs); this only controls what reaches stderr
    // before the TUI claims the terminal and after it tears
    // down (`q` mid-run).
    let stderr_min_level = params.get("loglevel")
        .and_then(|s| nbrs_activity::runner::parse_log_level(s))
        .unwrap_or(nbrs_activity::observer::LogLevel::Info);
    let observer = Arc::new(
        TuiObserver::new(run_state.clone(), cadences)
            .with_min_level(stderr_min_level),
    );

    // Run with the TUI observer. The TUI thread is spawned
    // lazily on the first phase_starting event.
    let run_result = nbrs_activity::runner::run_with_observer(args, observer.clone()).await;

    // Wait for the TUI to tear down the alternate screen before
    // any further stderr / stdout writes.
    observer.shutdown();

    // From here down the terminal is back in cooked mode.
    // Shared with the `tui=terminal` / `tui=off` path above.
    print_post_run_reports(&run_state, &run_result);

    if let Err(ref e) = run_result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }

    // Catch pre-mapped phases that were never visited.
    if let Some(code) = unreached_phase_exit_code(&run_state) {
        std::process::exit(code);
    }

    // The RunState actor thread is detached; the global
    // observer (set via `set_global_observer` inside the
    // runner) keeps a sender alive for the process lifetime,
    // which is fine — the actor exits with the process. We
    // still hold the JoinHandle so a future sandboxed test
    // build could opt to join it; runtime nbrs just lets it
    // ride.
    drop(run_state_join);
}

/// Print summary files + the post-run summary line. Called
/// after the active display sink (LogOnlySink / TuiSink) has
/// been torn down — the contract is that the terminal is back
/// in cooked mode by the time this runs, so direct stdout /
/// stderr writes don't compete with raw-mode output. Shared
/// between `tui=terminal` (`tui=off` adapter override goes
/// through here too) and `tui=on`.
///
/// Markdown summaries are echoed verbatim; non-Markdown formats
/// are listed by path so the user knows where to find them.
/// `_summary.*` files in `logs/latest` are scanned; the runner
/// has deferred their stdout output until now in TUI mode (the
/// alternate screen would have buffered and discarded any
/// inline writes).
fn print_post_run_reports(
    run_state: &nbrs_tui::run_state_actor::RunStateHandle,
    run_result: &Result<(), String>,
) {
    // SRD-46 auto-render: when the workload completed without
    // being aborted by the error handler, render every plot
    // item the runner persisted into the session db. Tables
    // were already rendered inline by the runner. Plots have
    // to land here because plot_metrics lives in this crate
    // (cross-crate from nbrs-activity); same fault-gate as
    // tables (run_result.is_ok() ⇒ render, else skip).
    if run_result.is_ok() {
        auto_render_plots();
        auto_inject_details();
    }

    if let Ok(entries) = std::fs::read_dir("logs/latest") {
        let mut summary_paths: Vec<std::path::PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.contains("_summary."))
                    .unwrap_or(false)
            })
            .collect();
        summary_paths.sort();
        for path in &summary_paths {
            let ext = path.extension()
                .and_then(|e| e.to_str())
                .unwrap_or("");
            if ext == "md" {
                if let Ok(rendered) = std::fs::read_to_string(path) {
                    if !rendered.is_empty() {
                        print!("{rendered}");
                    }
                }
            } else {
                eprintln!("summary ({ext}): {}", path.display());
            }
        }
    }
    print_post_run_summary(run_state, run_result);
}

/// Render every persisted plot item from `logs/latest/metrics.db`
/// post-run (SRD-46). Each plot becomes a PNG in the session
/// directory and a heading in `summary.md`. Failures are logged
/// and don't abort other plots — auto-rendering is best-effort.
fn auto_render_plots() {
    let db_path = std::path::PathBuf::from("logs/latest/metrics.db");
    if !db_path.exists() { return; }
    let conn = match rusqlite::Connection::open(&db_path) {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut stmt = match conn.prepare(
        "SELECT key, value FROM session_metadata \
         WHERE key LIKE 'report.%' ORDER BY rowid"
    ) {
        Ok(s) => s,
        Err(_) => return,
    };
    let rows = match stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    }) {
        Ok(it) => it,
        Err(_) => return,
    };
    let mut idx: usize = 0;
    let mut total: usize = 0;
    let entries: Vec<(String, String)> = rows.flatten().collect();
    for (_key, value) in &entries {
        idx += 1;
        let mut lines = value.lines();
        let head = match lines.next() { Some(h) => h, None => continue };
        let (kind, name) = if let Some(rest) = head.strip_prefix("plot ") {
            ("plot", rest.trim().to_string())
        } else if head.starts_with("table ") {
            // Tables already rendered inline by the runner.
            continue;
        } else {
            continue;
        };
        let mut label: Option<String> = None;
        let mut body_lines: Vec<&str> = Vec::new();
        for line in lines {
            if let Some(rest) = line.strip_prefix("label ") {
                let s = rest.trim();
                let s = s.strip_prefix('"').and_then(|x| x.strip_suffix('"'))
                    .or_else(|| s.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')))
                    .unwrap_or(s);
                label = Some(s.to_string());
            } else {
                body_lines.push(line);
            }
        }
        let _ = kind;
        let body = body_lines.join("\n");
        // Forward to plot_metrics_command exactly the way
        // `nbrs report plot <name>` would.
        let mut args: Vec<String> = vec![
            format!("--name={name}"),
            "--figure-num".into(), idx.to_string(),
        ];
        if let Some(l) = label.as_deref() {
            args.push("--label".into());
            args.push(l.to_string());
        }
        args.push(body);
        crate::plot_metrics::plot_metrics_command(&args);
        total += 1;
    }
    if total > 0 {
        eprintln!("auto-render: {total} plot{} rendered (SRD-46)",
            if total == 1 { "" } else { "s" });
    }
}

/// SRD-46 Details auto-injection: walk every output markdown
/// file in the session directory (default `summary.md` plus
/// every named file referenced by `report.<name>` items'
/// `target` line) and prepend a session-context section.
///
/// Source data: `session_metadata` rows the runner persists at
/// end-of-run (`session`, `start_time`, `end_time`,
/// `phase_count`, `scenario_count`, `workload_file`,
/// `adapter`).
fn auto_inject_details() {
    let session_dir = std::path::PathBuf::from("logs/latest");
    let db_path = session_dir.join("metrics.db");
    if !db_path.exists() { return; }
    let conn = match rusqlite::Connection::open(&db_path) {
        Ok(c) => c,
        Err(_) => return,
    };

    let read_meta = |key: &str| -> Option<String> {
        conn.query_row(
            "SELECT value FROM session_metadata WHERE key = ?1",
            [key],
            |r| r.get::<_, String>(0),
        ).ok()
    };

    let session_id = read_meta("session").unwrap_or_else(|| "?".into());
    let workload = read_meta("workload_file")
        .or_else(|| read_meta("workload"))
        .unwrap_or_else(|| "(inline)".into());
    let scenario = read_meta("scenario").unwrap_or_else(|| "?".into());
    let adapter = read_meta("adapter").unwrap_or_else(|| "?".into());
    let phase_count = read_meta("phase_count").unwrap_or_else(|| "?".into());
    let scenario_count = read_meta("scenario_count").unwrap_or_else(|| "?".into());
    let start_time = read_meta("start_time")
        .and_then(|s| s.parse::<u64>().ok());
    let end_time = read_meta("end_time")
        .and_then(|s| s.parse::<u64>().ok());
    let duration = match (start_time, end_time) {
        (Some(s), Some(e)) if e >= s => format_duration(e - s),
        _ => "?".to_string(),
    };
    let started = start_time
        .map(format_unix_seconds)
        .unwrap_or_else(|| "?".to_string());
    let ended = end_time
        .map(format_unix_seconds)
        .unwrap_or_else(|| "?".to_string());

    let body = format!(
        "| Field | Value |\n\
         | --- | --- |\n\
         | Session | `{session_id}` |\n\
         | Workload | `{workload}` |\n\
         | Scenario | `{scenario}` |\n\
         | Adapter | `{adapter}` |\n\
         | Started | {started} |\n\
         | Ended | {ended} |\n\
         | Duration | {duration} |\n\
         | Phases | {phase_count} |\n\
         | Scenarios | {scenario_count} |\n",
    );

    // Collect every distinct target file referenced by any
    // persisted report item, plus the default summary.md.
    let mut files: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    files.insert("summary.md".into());
    if let Ok(mut stmt) = conn.prepare(
        "SELECT value FROM session_metadata WHERE key LIKE 'report.%'"
    ) {
        if let Ok(iter) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for value in iter.flatten() {
                for line in value.lines() {
                    if let Some(rest) = line.strip_prefix("target ") {
                        files.insert(rest.trim().to_string());
                    }
                }
            }
        }
    }

    for f in &files {
        let path = session_dir.join(f);
        if let Err(e) = crate::report::write_named_section_first(
            &path, "run_details", "Run Details", &body,
        ) {
            eprintln!("warning: details auto-inject failed on '{}': {e}",
                path.display());
        }
    }
}

/// `123` → `2m 3s` / `7261` → `2h 1m 1s`.
fn format_duration(seconds: u64) -> String {
    let h = seconds / 3600;
    let m = (seconds % 3600) / 60;
    let s = seconds % 60;
    if h > 0 { format!("{h}h {m}m {s}s") }
    else if m > 0 { format!("{m}m {s}s") }
    else { format!("{s}s") }
}

/// UNIX seconds → ISO-ish `YYYY-MM-DD HH:MM:SS UTC`.
fn format_unix_seconds(secs: u64) -> String {
    // Cheap formatter — avoids pulling in chrono just for this.
    // Days since 1970-01-01 → calendar (proleptic Gregorian).
    let days = secs / 86400;
    let rem = secs % 86400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let (year, month, day) = days_to_ymd(days as i64);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02} UTC")
}

fn days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    // Howard Hinnant's algorithm.
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (year, m as u32, d as u32)
}
