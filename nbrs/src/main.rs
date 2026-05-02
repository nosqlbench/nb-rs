// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! nbrs — the nb-rs command-line tool.
//!
//! Usage:
//!   nbrs run adapter=stdout workload=file.yaml cycles=100 threads=4
//!   nbrs run workload=file.yaml tags=block:main rate=1000
//!   nbrs file.yaml scenario_name [param=value ...]

mod bench;
mod cli;
mod completion;
mod daemon;
mod describe;
mod inspector;
mod db_merge;
mod plot;
mod plot_metrics;
mod report;
mod run;
mod summary;
#[allow(dead_code)]
mod web_push;

#[cfg(feature = "openapi")]
mod openapi;

fn main() {
    // Shell-completion callback. Reads `_NBRS_COMPLETE=bash`,
    // emits candidates, exits. Must run BEFORE any
    // arg-consuming logic so tab presses never touch adapters,
    // files, or stderr. See `nbrs/src/completion.rs` for the
    // stratified tap progression: tab 1 → workload commands;
    // tab 2 → global flags; tab 3 → everything.
    let tree = completion::build_tree();
    if completion::handle_complete_env(&tree) {
        return;
    }

    let args: Vec<String> = std::env::args().skip(1).collect();

    // `nbrs attach` connects to a running nbrs's OOB
    // introspection socket (SRD-02 §"Display and Diagnostic
    // Decoupling"). Probes the socket on entry — fails fast
    // with a clear message if no live server is reachable
    // rather than dropping the user into a REPL with broken
    // queries. Renamed from the legacy `--inspector` flag.
    if args.first().map(|s| s.as_str()) == Some("attach") {
        inspector::inspector_command(&args[1..]);
        return;
    }

    // `nbrs summary` renders a summary report from a
    // previously-written `metrics.db`. Same
    // SqliteReporter::format_summary code path the runner uses
    // at end-of-run, so identical input → identical output.
    // It's a proper subcommand (not a workload) because it
    // operates on a finished session — no workload file to
    // parse, no scenarios to execute.
    if args.first().map(|s| s.as_str()) == Some("summary") {
        summary::summary_command(&args[1..]);
        return;
    }

    // `nbrs completions` emits a `source <(...)` preamble
    // (recommended: `eval "$(nbrs completions)"`).
    // `nbrs completions --shell bash` emits the raw shim that
    // the preamble sources. Same UX as `veks completions`.
    if args.first().map(|s| s.as_str()) == Some("completions") {
        completion::print_completions(&args[1..]);
        return;
    }

    if args.is_empty() {
        cli::print_usage();
        return;
    }

    let cmd = args[0].as_str();

    match cmd {
        "describe" => {
            describe::describe_command(&args[1..]);
        }
        "bench" => {
            bench::bench_command(&args[1..]);
        }
        "plot" => {
            // `nbrs plot gk <expr>` keeps the legacy terminal-
            // braille GK-expression plotter. Everything else goes
            // to the metrics-DB plotter (`nbrs plot --metric ...`).
            if args.get(1).map(|s| s.as_str()) == Some("gk") {
                plot::plot_command(&args[1..]);
            } else {
                plot_metrics::plot_metrics_command(&args[1..]);
            }
        }
        "web" => {
            daemon::web_command(&args);
        }
        "run" => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(run::run_command(&args));
        }
        #[cfg(feature = "openapi")]
        "describe-openapi" => {
            openapi::describe_command(&args[1..]);
        }
        #[cfg(feature = "openapi")]
        "run-openapi" => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(openapi::run_command(&args[1..]));
        }
        _ => {
            // Bare workload file: nbrs myworkload.yaml [params...]
            if !cmd.starts_with('-') {
                if let Some(path) = cli::resolve_workload_path(cmd) {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let mut run_args = vec![format!("workload={path}")];
                    // First bare arg (no = or --) after the workload file
                    // is treated as the scenario name.
                    let mut scenario_set = false;
                    for extra in &args[1..] {
                        if !scenario_set && !extra.contains('=') && !extra.starts_with('-') {
                            run_args.push(format!("scenario={extra}"));
                            scenario_set = true;
                        } else {
                            run_args.push(extra.clone());
                        }
                    }
                    rt.block_on(run::run_command(&run_args));
                    return;
                }
            }
            eprintln!("error: unknown command '{cmd}'");
            cli::print_usage();
            std::process::exit(1);
        }
    }
}

