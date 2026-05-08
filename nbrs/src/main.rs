// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! nbrs — the nb-rs command-line tool.
//!
//! Usage:
//!   nbrs run adapter=stdout workload=file.yaml cycles=100 threads=4
//!   nbrs run workload=file.yaml tags=block:main rate=1000
//!   nbrs file.yaml scenario_name [param=value ...]

mod bench;
mod checkpoint_cmd;
mod cli;
mod cli_spec;
mod completion;
mod daemon;
mod describe;
mod inspector;
mod db_merge;
mod metrics_cache;
mod metrics_cmd;
mod metricsql_cmd;
mod palette;
mod plot;
mod plot_metrics;
mod replay;
mod report;
mod report_build;
mod report_cmd;
mod report_scratch;
mod run;
mod summary;
#[allow(dead_code)]
mod web_push;

#[cfg(feature = "openapi")]
mod openapi;

fn main() {
    // Build the canonical CLI spec once. `cli_spec::root` pulls
    // every subcommand's `spec()` so this single value drives
    // both completion and dispatch — there is no second list
    // of names to keep in sync.
    let root = cli_spec::root::root();

    // Shell-completion callback. Reads `_NBRS_COMPLETE=bash`,
    // emits candidates, exits. Must run BEFORE any
    // arg-consuming logic so tab presses never touch adapters,
    // files, or stderr.
    let comp_tree = cli_spec::completion::build_command_tree(&root);
    let comp_tree = completion::attach_global_value_providers(comp_tree);
    if completion::handle_complete_env(&comp_tree) {
        return;
    }

    let args: Vec<String> = std::env::args().skip(1).collect();

    // SRD-45 startup hook: honors `--session`, `--session-path`,
    // `--session-name`, …, plus the `NBRS_SESSION*` env vars.
    // Updates `logs/latest` to point at the resolved session
    // directory so subsequent subcommands (`run`, `report`,
    // `plot`, `summary`, `tui`, …) see consistent session
    // wiring. Must run before subcommand dispatch — read-side
    // commands (plot, report) resolve their default
    // `logs/latest/metrics.db` paths immediately on entry.
    nbrs_activity::session::apply_session_directory_at_startup(&args);

    if args.is_empty() {
        cli_spec::help::render_usage(&root, &[]);
        return;
    }

    // Bare-workload-file shortcut (`nbrs myworkload.yaml …`).
    // Predates the spec model and isn't a Command — handle it
    // before parsing so the walker doesn't see "myworkload.yaml"
    // as an unknown command.
    let cmd = args[0].as_str();
    if !cmd.starts_with('-')
        && root.subcommands.iter().all(|s| s.name != cmd)
        && let Some(path) = cli::resolve_workload_path(cmd)
    {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let run_args = build_bare_workload_args(&path, &args[1..]);
        rt.block_on(run::run_command(&run_args));
        return;
    }

    // Walker-driven dispatch: parse argv against the spec, look
    // up the matched leaf's handler, run it. Async handlers spin
    // up tokio lazily — sync handlers never touch the runtime.
    let parsed = match cli_spec::walker::parse(&root, &args) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("nbrs: {e}");
            cli_spec::help::render_usage(&root, &[]);
            std::process::exit(2);
        }
    };

    // `--help` / `-h` short-circuit: render usage for the matched
    // command path and exit 0 without invoking the handler. Walker
    // already stopped at the deepest subcommand seen *before* the
    // help flag, so `nbrs --help`, `nbrs metrics --help`, and
    // `nbrs metrics list --help` each render the right slice.
    if parsed.help_requested {
        let sub_path: Vec<&str> = parsed.path[1..]
            .iter().map(String::as_str).collect();
        cli_spec::help::render_usage(&root, &sub_path);
        return;
    }

    // Walk the matched path back through the spec to find the
    // handler attached to the deepest matched command.
    let handler = lookup_handler(&root, &parsed.path[1..]);
    let result: Result<(), String> = match handler {
        Some(cli_spec::Handler::Sync(f)) => f(parsed),
        Some(cli_spec::Handler::Async(f)) => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(f(parsed))
        }
        None => {
            eprintln!("nbrs: command at `{}` has no handler", parsed.path.join(" "));
            cli_spec::help::render_usage(&root, &parsed.path[1..]
                .iter().map(String::as_str).collect::<Vec<_>>());
            std::process::exit(2);
        }
    };

    if let Err(e) = result {
        eprintln!("nbrs: {e}");
        std::process::exit(2);
    }
}

/// Walk the matched command path inside the root spec to find
/// the handler. `path` is the matched-command segments after
/// the binary name — e.g. `["metrics", "list"]` for
/// `nbrs metrics list …`.
fn lookup_handler<'a>(root: &'a cli_spec::Command, path: &[String]) -> Option<cli_spec::Handler> {
    let mut current = root;
    for seg in path {
        current = current.subcommands.iter().find(|s| s.name == seg.as_str())?;
    }
    current.handler
}

/// Translate `nbrs <workload.yaml> [scenario] [params...]` into
/// the `run`-shaped arg list. Same logic as the legacy main —
/// preserved verbatim because the walker doesn't model this
/// shape (it would otherwise trip on the bare-yaml positional).
fn build_bare_workload_args(path: &str, tail: &[String]) -> Vec<String> {
    const VALUE_FLAGS: &[&str] = &[
        "--session", "--session-name", "--session-path",
        "--session-reuse", "--session-keep",
        "--session-shelflife", "--readout",
    ];
    let mut run_args = vec![format!("workload={path}")];
    let mut scenario_set = false;
    let mut iter = tail.iter().peekable();
    while let Some(extra) = iter.next() {
        if VALUE_FLAGS.iter().any(|f| *f == extra.as_str()) {
            run_args.push(extra.clone());
            if let Some(val) = iter.next() {
                run_args.push(val.clone());
            }
            continue;
        }
        if !scenario_set && !extra.contains('=') && !extra.starts_with('-') {
            run_args.push(format!("scenario={extra}"));
            scenario_set = true;
        } else {
            run_args.push(extra.clone());
        }
    }
    run_args
}

