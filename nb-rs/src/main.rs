// Copyright 2024-2026 nosqlbench contributors
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
mod run;
mod web_push;

fn main() {
    // Handle shell completion callbacks before anything else.
    let tree = cli::cli_tree();
    if veks_completion::handle_complete_env("nbrs", &tree) {
        std::process::exit(0);
    }

    let args: Vec<String> = std::env::args().skip(1).collect();

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
        "completions" => {
            completions_command(&args);
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
                    run_args.extend(args[1..].iter().cloned());
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

fn completions_command(args: &[String]) {
    let shell = args.iter().find_map(|a| a.strip_prefix("--shell="))
        .or_else(|| args.iter().skip_while(|a| *a != "--shell").nth(1).map(|s| s.as_str()))
        .or_else(|| args.get(1).map(|s| s.as_str()))
        .unwrap_or("bash");
    match shell {
        "bash" => veks_completion::print_bash_script("nbrs"),
        other => {
            eprintln!("Shell '{other}' is not yet supported.");
            eprintln!("For bash: eval \"$(nbrs completions)\"");
        }
    }
}
