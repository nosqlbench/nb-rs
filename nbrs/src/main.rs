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
mod metrics_cache;
mod metrics_cmd;
mod palette;
mod plot;
mod plot_metrics;
mod report;
mod report_cmd;
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

    // SRD-45 startup hook: honors `--session`, `--session-path`,
    // `--session-name`, …, plus the `NBRS_SESSION*` env vars.
    // Updates `logs/latest` to point at the resolved session
    // directory so subsequent subcommands (`run`, `report`,
    // `plot`, `summary`, `tui`, …) see consistent session
    // wiring. Must run before any subcommand dispatch — read-
    // side commands (plot, report) resolve their default
    // `logs/latest/metrics.db` paths immediately on entry.
    nbrs_activity::session::apply_session_directory_at_startup(&args);

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
        "metrics" => {
            metrics_cmd::metrics_command(&args[1..]);
        }
        "report" => {
            // `nbrs report ...` — primary surface for SRD-46
            // report items. Sub-dispatch (list, all, glob,
            // figure N, plot/table) lives in `report_cmd`.
            report_cmd::report_command(&args[1..], report_cmd::KindFilter::Any);
        }
        "plot" => {
            // Unadvertised alias for `nbrs report plot ...`.
            report_cmd::report_command(&args[1..], report_cmd::KindFilter::Plot);
        }
        "table" => {
            // Unadvertised alias for `nbrs report table ...`.
            report_cmd::report_command(&args[1..], report_cmd::KindFilter::Table);
        }
        "gk" => {
            // `nbrs gk visualize <expr|file.gk>` — GK-expression
            // terminal plotter (formerly `nbrs plot gk`). Sibling
            // of `gk functions` / `gk dag` (still under
            // `describe gk` until a broader gk-subcommand
            // refactor; this entry is the new home for the
            // visualizer specifically).
            match args.get(1).map(|s| s.as_str()) {
                Some("visualize") => plot::plot_command(&args[1..]),
                Some(other) => {
                    eprintln!("nbrs gk: unknown subcommand '{other}' \
                        (try `visualize`)");
                    std::process::exit(2);
                }
                None => {
                    eprintln!("nbrs gk <subcommand>: expected `visualize`");
                    std::process::exit(2);
                }
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

