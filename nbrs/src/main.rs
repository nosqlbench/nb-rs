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
mod daemon;
mod describe;
mod plot;
mod run;
#[allow(dead_code)]
mod web_push;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // Shell completion: share the same harness cassnbrs uses so
    // `workload=<TAB>`, `scenario=<TAB>`, `adapter=<TAB>`, and
    // param-name completion behave identically across personas.
    // Handles `completions` (preamble / --shell bash) and the hidden
    // `__complete` callback. Must run before any other work so tab
    // presses never touch adapters, files, or stderr.
    let completion_spec = nb_activity::completions::CompletionSpec {
        binary_name: "nbrs",
        subcommands: &["run", "describe", "bench", "plot", "web", "completions"],
        run_params: nb_activity::runner::KNOWN_PARAMS,
    };
    if nb_activity::completions::handle_if_match(&completion_spec, &args) {
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
            plot::plot_command(&args[1..]);
        }
        "web" => {
            daemon::web_command(&args);
        }
        "run" => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(run::run_command(&args));
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

