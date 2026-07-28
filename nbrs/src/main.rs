// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! nbrs — the nb-rs command-line tool.
//!
//! Usage:
//!   nbrs run adapter=stdout workload=file.yaml cycles=100 threads=4
//!   nbrs run workload=file.yaml tags=block:main rate=1000
//!   nbrs file.yaml scenario_name [param=value ...]

mod bench;
mod bundled;
mod check_cmd;
mod copy_cmd;
mod diag_cmd;
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
mod refine;
mod run;
mod session_cmd;
mod summary;
mod watch_trigger;
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

    // SRD-85: assemble the bundled-workload catalog first —
    // workload resolution, `describe workloads`, `copy`, AND
    // the completion callback (catalog names complete under
    // `workload=`) all read it. Cheap: a Vec of &'static refs.
    bundled::install_catalog();

    // Install the run-style param vocabulary into the runner so its
    // workload/CLI param validation references the CLI command-spec
    // (`completion::RUN_KV_PARAMS`) directly — one source of truth, no
    // hand-synced copy. Done before any run dispatch.
    nbrs_runtime::runner::install_known_params(completion::known_param_keys());

    // Shell-completion callback. Reads `_NBRS_COMPLETE=bash`,
    // emits candidates, exits. Must run BEFORE any
    // arg-consuming logic so tab presses never touch adapters,
    // files, or stderr.
    let comp_tree = cli_spec::completion::build_command_tree(&root);
    if completion::handle_complete_env(&comp_tree) {
        return;
    }

    let mut args: Vec<String> = std::env::args().skip(1).collect();

    // SRD-102: strip the process-wide `--threads.*` flags out of the arg
    // vector BEFORE session handling and command dispatch. They are global,
    // not per-subcommand, so the walker/runner param guards must never see
    // them; their values are applied to the thread-pool config below.
    let thread_overrides = extract_thread_overrides(&mut args);

    // SRD-45 startup hook: session-lifecycle cleanup, honouring
    // `--session-keep` / `--session-shelflife` (and the
    // `NBRS_SESSION*` env vars) to purge aged-out session dirs.
    //
    // It no longer repoints `sessions/latest` at whatever `--session` names. That
    // made `--session` work by mutating shared state — a read-only
    // `nbrs table … --session=sessions/foo` left `latest` on `foo`, so a later
    // bare `nbrs report` or `--resume-latest` silently used `foo` instead of the
    // newest real run — and it only worked for sessions under `sessions/`. Each
    // command that owns `latest` claims it itself, and read-side commands resolve
    // `--session` locally.
    // Only a command that CREATES a session may retire old ones: the cleanup
    // deletes directories, and a read must never delete data. Unrecognised
    // subcommands count as non-creating, which fails safe (cleanup is skipped and
    // the next writing command does it).
    let creates_session = matches!(
        args.first().map(String::as_str),
        Some("run" | "check" | "refine" | "session" | "bench" | "daemon"));
    nbrs_runtime::session::purge_stale_sessions_at_startup(&args, creates_session);

    // SRD-102: resolve the physical thread-pool config — CLI `--threads.*`
    // flags over NBRS_THREADS_* env over core-count-derived defaults — and
    // install the process-wide registry before any tokio runtime or cadence
    // scheduler starts. A malformed value (CLI or env) is a hard startup
    // error — never silently ignored.
    match nbrs_metrics::thread_pools::ThreadPoolConfig::resolve_with_cli(&thread_overrides) {
        Ok(cfg) => nbrs_metrics::thread_pools::init(cfg),
        Err(e) => {
            eprintln!("nbrs: {e}");
            std::process::exit(2);
        }
    }

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
        let rt = build_workers_runtime();
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

    // `--version` / `-V` short-circuit: print the version
    // string + the set of compile-time engine/feature flags
    // and exit. Side-channel surface like `--help`; no
    // subcommand path interaction.
    if parsed.version_requested {
        print_version();
        return;
    }

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
            let rt = build_workers_runtime();
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

/// Pull the global `--threads.*` flags (SRD-102 §4) out of `args`, in place,
/// returning their raw values. Equals-form only (`--threads.timing=2`); parsing
/// and not-silent validation happen in [`nbrs_metrics::thread_pools::ThreadPoolConfig::resolve_with_cli`]
/// so a malformed value is the same hard error as the env path. An unknown
/// `--threads.<x>` subkey is left in `args` for the normal unknown-flag guard.
fn extract_thread_overrides(
    args: &mut Vec<String>,
) -> nbrs_metrics::thread_pools::CliThreadOverrides {
    let mut ov = nbrs_metrics::thread_pools::CliThreadOverrides::default();
    args.retain(|a| {
        let Some(rest) = a.strip_prefix("--threads.") else { return true; };
        let Some((key, val)) = rest.split_once('=') else { return true; };
        let slot = match key {
            "timing" => &mut ov.timing,
            "io" => &mut ov.io,
            "workers" => &mut ov.workers,
            "timing.sched" => &mut ov.timing_sched,
            "timing.pin" => &mut ov.timing_pin,
            // Unknown --threads.* subkey: leave it so the dispatch guard
            // reports it, rather than silently swallowing a typo.
            _ => return true,
        };
        *slot = Some(val.to_string());
        false // consume this token
    });
    ov
}

/// The `workers` tokio runtime (SRD-102): the async worker pool that runs
/// workload fibers, sized to `ThreadPoolConfig::workers` (cores − reserved)
/// so the dedicated `timing`/`io` pools always have a core and the cadence
/// scheduler is never queued behind a fiber. Named `workers` for `top`/perf
/// legibility. Falls back to the tokio default if the sized build fails.
fn build_workers_runtime() -> tokio::runtime::Runtime {
    let workers = nbrs_metrics::thread_pools::global().config().workers;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.max(1))
        .thread_name("workers")
        .enable_all()
        .build()
        .unwrap_or_else(|_| tokio::runtime::Runtime::new().unwrap())
}

/// Walk the matched command path inside the root spec to find
/// the handler. `path` is the matched-command segments after
/// the binary name — e.g. `["metrics", "list"]` for
/// `nbrs metrics list …`.
fn lookup_handler(root: &cli_spec::Command, path: &[String]) -> Option<cli_spec::Handler> {
    let mut current = root;
    for seg in path {
        current = current.subcommands.iter().find(|s| s.name == seg.as_str())?;
    }
    current.handler
}

/// `--version` / `-V` output. Single bare line: `nbrs <ver>`.
/// Engine availability is surfaced via `describe adapter cql`.
fn print_version() {
    println!("nbrs {}", env!("CARGO_PKG_VERSION"));
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
        if VALUE_FLAGS.contains(&extra.as_str()) {
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

