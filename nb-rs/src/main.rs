// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! nbrs — the nb-rs command-line tool.
//!
//! Usage:
//!   nbrs run adapter=stdout workload=file.yaml cycles=100 threads=4
//!   nbrs run workload=file.yaml tags=block:main rate=1000
//!   nbrs file.yaml scenario_name [param=value ...]

use std::collections::HashMap;
use std::sync::Arc;

use nb_activity::activity::{Activity, ActivityConfig};
use nb_adapter_stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
use nb_activity::bindings::compile_bindings_with_libs_excluding;
use nb_activity::opseq::{OpSequence, SequencerType};
use nb_activity::synthesis::OpBuilder;
use nb_metrics::labels::Labels;
use nb_tui::app::App;
use nb_tui::reporter::TuiReporter;
use nb_variates::dsl::registry;
use nb_workload::parse::parse_workload;
use nb_workload::tags::TagFilter;

mod web_push;
mod daemon;

/// Discover workload-declared parameters for dynamic completion.
///
/// When `workload=somefile.yaml` is on the command line, parse the
/// file's `params:` section and return param names as `key=` completions.
fn discover_workload_params(_partial: &str, context: &[&str]) -> Vec<String> {
    for word in context {
        let path = if let Some(p) = word.strip_prefix("workload=") {
            p
        } else if word.ends_with(".yaml") || word.ends_with(".yml") {
            word
        } else {
            continue;
        };
        // Try to read the YAML and extract top-level params
        if let Ok(source) = std::fs::read_to_string(path)
            && let Ok(doc) = serde_yaml::from_str::<serde_json::Value>(&source)
                && let Some(params) = doc.get("params").and_then(|v| v.as_object()) {
                    return params.keys().map(|k| format!("{k}=")).collect();
                }
    }
    Vec::new()
}

/// Build the definitive CLI command tree. This is the single source of
/// truth for all subcommands, options, and flags. Shell completions,
/// help text, and option validation all derive from this definition.
fn cli_tree() -> veks_completion::CommandTree {
    use veks_completion::Node;

    veks_completion::CommandTree::new("nbrs")
        .command("run", Node::leaf_with_flags(
            &[
                "adapter=", "driver=", "workload=", "op=", "cycles=", "threads=",
                "rate=", "stanzarate=", "errors=", "seq=", "tags=", "format=",
                "filename=", "stanza_concurrency=", "sc=",
                // CQL adapter params
                "hosts=", "host=", "port=", "keyspace=", "consistency=",
                "username=", "password=", "request_timeout_ms=",
                // HTTP adapter params
                "base_url=", "timeout=",
            ],
            &["--strict", "--dry-run", "--tui", "--diagnose",
              "--dry-run=emit", "--dry-run=json"],
        ).with_dynamic_options(discover_workload_params))
        .command("describe", Node::group(vec![
            ("gk", Node::group(vec![
                ("functions", Node::leaf(&[])),
                ("stdlib", Node::leaf(&[])),
                ("dag", Node::leaf(&[])),
                ("modules", Node::leaf(&[])),
            ])),
        ]))
        .command("bench", Node::group(vec![
            ("gk", Node::leaf_with_flags(
                &["cycles=", "concurrency=", "--cycles", "--concurrency", "-c"],
                &["--explain"],
            )),
        ]))
        .command("web", Node::leaf_with_flags(
            &["bind=", "port="],
            &["--daemon", "--stop", "--restart"],
        ))
        .command("completions", Node::leaf(&["bash", "zsh", "fish"]))
}

fn main() {
    // Handle shell completion callbacks before anything else.
    let tree = cli_tree();
    if veks_completion::handle_complete_env("nbrs", &tree) {
        std::process::exit(0);
    }

    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        print_usage();
        return;
    }

    // Detect bare workload file as first argument:
    //   nbrs myworkload.yaml [params...]
    //   nbrs myworkload [params...]  (auto-appends .yaml/.yml)
    if let Some(first) = args.first() {
        if first != "run" && first != "describe" && first != "bench"
            && first != "web" && first != "completions"
            && !first.starts_with('-')
        {
            let workload_path = resolve_workload_path(first);
            if let Some(path) = workload_path {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let mut run_args = vec![format!("workload={path}")];
                run_args.extend(args[1..].iter().cloned());
                rt.block_on(async {
                    run_command(&run_args).await;
                });
                return;
            }
        }
    }

    // Handle describe command synchronously (no tokio needed)
    if args.first().map(|s| s.as_str()) == Some("describe") {
        describe_command(&args[1..]);
        return;
    }

    // Handle bench command synchronously
    if args.first().map(|s| s.as_str()) == Some("bench") {
        bench_command(&args[1..]);
        return;
    }

    // Handle completions command: output shell completion script.
    // Usage:
    //   eval "$(nbrs completions)"
    //   source <(nbrs completions --shell bash)
    //   echo 'eval "$(nbrs completions)"' >> ~/.bashrc
    if args.first().map(|s| s.as_str()) == Some("completions") {
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
        return;
    }

    // Handle web command
    if args.first().map(|s| s.as_str()) == Some("web") {
        // Handle --stop: kill a running daemon
        if args.iter().any(|a| a == "--stop") {
            match daemon::stop_daemon() {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
            return;
        }

        // Handle --restart: stop the old daemon, re-launch with its saved args.
        // Falls back to process-table scan if no anchor file exists.
        // If nothing is found at all, starts fresh.
        if args.iter().any(|a| a == "--restart") {
            if let Some(anchor) = daemon::read_anchor() {
                // Anchor file exists — use it for a clean restart.
                let _ = daemon::stop_daemon();
                if !anchor.args.is_empty() {
                    let exe = std::env::current_exe().unwrap_or_else(|_| "nbrs".into());
                    eprintln!("nbrs web: restarting with: {} {}", exe.display(),
                        anchor.args.join(" "));
                    let status = std::process::Command::new(&exe)
                        .args(&anchor.args)
                        .status()
                        .unwrap_or_else(|e| {
                            eprintln!("error: failed to restart: {e}");
                            std::process::exit(1);
                        });
                    std::process::exit(status.code().unwrap_or(1));
                }
                eprintln!("nbrs web: anchor has no saved args, starting with defaults");
            } else {
                // No anchor — scan the process table for orphaned nbrs web processes.
                let procs = daemon::find_nbrs_web_processes();
                if procs.is_empty() {
                    eprintln!("nbrs web: no running instance found, starting fresh");
                } else {
                    eprintln!("nbrs web: no anchor file, but found {} running nbrs web process(es):",
                        procs.len());
                    for p in &procs {
                        eprintln!("  pid {} — {}", p.pid, p.cmdline);
                    }
                    if daemon::confirm_prompt("Kill these and start fresh?") {
                        for p in &procs {
                            match daemon::kill_pid(p.pid) {
                                Ok(()) => eprintln!("  stopped pid {}", p.pid),
                                Err(e) => eprintln!("  warning: {e}"),
                            }
                        }
                        // Clean up any leftover PID/anchor files.
                        let _ = std::fs::remove_file(daemon::pid_file_path());
                        daemon::remove_anchor();
                    } else {
                        eprintln!("nbrs web: aborted");
                        return;
                    }
                }
            }
        }

        // Warn about unrecognized --flags.
        let known_flags = ["--daemon", "--stop", "--restart"];
        for a in args.iter().filter(|a| a.starts_with("--")) {
            let key = a.split('=').next().unwrap_or(a);
            if !known_flags.contains(&key) && key != "--bind" && key != "--port" {
                eprintln!("warning: unrecognized option '{a}' (known: --daemon, --stop, --restart, --bind=, --port=)");
            }
        }

        let bind_raw = args.iter()
            .find_map(|a| a.strip_prefix("bind=").or_else(|| a.strip_prefix("--bind=")))
            .unwrap_or("0.0.0.0");
        let port_raw = args.iter()
            .find_map(|a| a.strip_prefix("port=").or_else(|| a.strip_prefix("--port=")));

        // Parse bind flexibly: accept bare IP, host:port, or full URL
        let (bind, port) = parse_bind_address(bind_raw, port_raw);
        let addr: std::net::SocketAddr = format!("{bind}:{port}").parse()
            .unwrap_or_else(|e| { eprintln!("error: invalid bind address '{bind}:{port}': {e}"); std::process::exit(1); });

        // Clean up stale anchor if the recorded PID is dead.
        daemon::cleanup_stale_anchor();

        // Check if the port is already in use before attempting to bind.
        if let Err(msg) = daemon::check_port_available(&addr) {
            eprintln!("error: {msg}");
            std::process::exit(1);
        }

        // Handle --daemon: fork to background
        if args.iter().any(|a| a == "--daemon") {
            eprintln!("nbrs web: daemonizing on {addr}...");
            daemon::daemonize().unwrap_or_else(|e| {
                eprintln!("error: failed to daemonize: {e}");
                std::process::exit(1);
            });
            // After daemonize(), stdout/stderr are /dev/null.
            // The PID file has been written.
        }

        // Write anchor file so `nbrs run` in this directory auto-discovers us.
        // Save the full "web ..." args (excluding --restart) for --restart.
        let saved_args: Vec<String> = std::env::args().skip(1)
            .filter(|a| a != "--restart")
            .collect();
        daemon::write_anchor(&addr, &saved_args);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let broadcast = nb_web::ws::MetricsBroadcast::new(16);
            if let Err(e) = nb_web::server::serve_with(addr, broadcast).await {
                eprintln!("error: web server failed: {e}");
            }
        });

        // Clean up on exit.
        let _ = std::fs::remove_file(daemon::pid_file_path());
        daemon::remove_anchor();
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        run_command(&args).await;
    });
}

/// Resolve a potential workload path, trying extensions if needed.
///
/// Returns `Some(path)` if a workload file exists, `None` otherwise.
fn resolve_workload_path(name: &str) -> Option<String> {
    // Already has extension
    if name.ends_with(".yaml") || name.ends_with(".yml") {
        if std::path::Path::new(name).exists() {
            return Some(name.to_string());
        }
        return None;
    }

    // Try appending extensions
    for ext in &[".yaml", ".yml"] {
        let path = format!("{name}{ext}");
        if std::path::Path::new(&path).exists() {
            return Some(path);
        }
    }

    // Try in workloads/ subdirectory
    for ext in &["", ".yaml", ".yml"] {
        let path = format!("workloads/{name}{ext}");
        if std::path::Path::new(&path).exists() {
            return Some(path);
        }
    }

    None
}

fn print_usage() {
    eprintln!("nbrs — nosqlbench for Rust");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  nbrs run adapter=stdout workload=file.yaml cycles=100 threads=4");
    eprintln!("  nbrs run workload=file.yaml tags=block:main rate=1000 format=json");
    eprintln!("  nbrs run op='hello {{{{cycle}}}}' cycles=10");
    eprintln!("  nbrs run op='id={{{{mod(hash(cycle), 1000)}}}}' cycles=100 format=json");
    eprintln!("  nbrs describe gk functions    List all GK node functions");
    eprintln!("  nbrs describe gk stdlib       List standard library modules");
    eprintln!("  nbrs describe gk dag <file>   Render a .gk file as DOT/Mermaid/SVG");
    eprintln!("  nbrs bench gk <expr>    Benchmark a GK expression at all compilation levels");
    eprintln!("  nbrs web [bind=0.0.0.0] [port=8080]  Start the web dashboard");
    eprintln!("  nbrs web --daemon             Start web dashboard in the background");
    eprintln!("  nbrs web --stop               Stop a running background web dashboard");
    eprintln!("  nbrs web --restart            Restart with the same arguments");
    eprintln!();
    eprintln!("Parameters:");
    eprintln!("  workload=<file.yaml>   Workload definition file");
    eprintln!("  adapter=<name>         Adapter type (default: stdout)");
    eprintln!("  cycles=<n>             Number of cycles to execute");
    eprintln!("  threads=<n>            Concurrency level (default: 1)");
    eprintln!("  rate=<n>               Per-cycle rate limit (ops/sec)");
    eprintln!("  stanzarate=<n>         Per-stanza rate limit (stanzas/sec)");
    eprintln!("  tags=<filter>          Tag filter for op selection");
    eprintln!("  seq=<type>             Sequencer: bucket|interval|concat");
    eprintln!("  format=<type>          Output format: assignments|json|csv|stmt");
    eprintln!("  errors=<spec>          Error handler spec");
    eprintln!("  filename=<path>        Output file (default: stdout)");
    eprintln!("  --report-openmetrics-to=<url>  Push metrics in OpenMetrics format");
    eprintln!("                         e.g. http://localhost:8080/api/v1/import/prometheus");
}

fn describe_command(args: &[String]) {
    let topic = args.first().map(|s| s.as_str()).unwrap_or("");
    let subtopic = args.get(1).map(|s| s.as_str()).unwrap_or("");

    match (topic, subtopic) {
        ("gk", "functions") => describe_gk_functions(),
        ("gk", "stdlib") => describe_gk_stdlib(),
        ("gk", "dag") => {
            // Remaining args after "describe gk dag" are the GK source or file
            let rest: Vec<String> = args.iter().skip(2).cloned().collect();
            describe_gk_dag(&rest);
        }
        ("gk", "modules") => {
            let rest: Vec<String> = args.iter().skip(2).cloned().collect();
            describe_gk_modules(&rest);
        }
        ("gk", _) => {
            eprintln!("nbrs describe gk <subtopic>");
            eprintln!("  functions    List all GK node functions");
            eprintln!("  stdlib       List embedded standard library modules");
            eprintln!("  dag          Render a GK source as DOT, Mermaid, or SVG");
            eprintln!("  modules      List modules from a directory");
        }
        _ => {
            eprintln!("nbrs describe <topic>");
            eprintln!("  gk           Generation kernel topics");
        }
    }
}

fn describe_gk_functions() {
    use nb_activity::bindings::probe_compile_level;

    let grouped = registry::by_category();
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    // ANSI color codes
    let (bold, dim, reset, green, cyan, magenta) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[35m")
    } else {
        ("", "", "", "", "", "")
    };

    println!();
    println!("{bold}GK Node Functions{reset}");
    println!("{bold}═════════════════{reset}");
    println!();

    for (cat, funcs) in &grouped {
        let cat_name = cat.display_name();
        println!("  {bold}{cyan}── {cat_name} ──{reset}");
        println!();

        for sig in funcs {
            let level = probe_compile_level(sig.name);
            let (p1, p2, p3) = match level {
                registry::CompileLevel::Phase3 => (
                    format!("{green}\u{2713}{reset}"),
                    format!("{green}\u{2713}{reset}"),
                    format!("{green}\u{2713}{reset}"),
                ),
                registry::CompileLevel::Phase2 => (
                    format!("{green}\u{2713}{reset}"),
                    format!("{green}\u{2713}{reset}"),
                    format!("{dim}\u{2717}{reset}"),
                ),
                registry::CompileLevel::Phase1 => (
                    format!("{green}\u{2713}{reset}"),
                    format!("{dim}\u{2717}{reset}"),
                    format!("{dim}\u{2717}{reset}"),
                ),
            };
            let level_col = format!("{bold}P{reset}{p1}{p2}{p3}");

            let const_info = sig.const_param_info();
            let params_desc = if const_info.is_empty() {
                String::new()
            } else {
                let p: Vec<String> = const_info.iter()
                    .map(|(name, required)| {
                        if *required { name.to_string() } else { format!("[{name}]") }
                    })
                    .collect();
                format!("({})", p.join(", "))
            };

            let arity = if sig.outputs == 0 {
                format!("{}→N", sig.wire_input_count())
            } else {
                format!("{}→{}", sig.wire_input_count(), sig.outputs)
            };

            let name_padded = format!("{:<24}", sig.name);
            let params_padded = format!("{:<24}", params_desc);
            let arity_padded = format!("{:<5}", arity);

            print!("  {bold}{magenta}{name_padded}{reset}");
            print!(" {dim}{params_padded}{reset}");
            print!(" {arity_padded}");
            print!("  {level_col}");
            println!("  {dim}{}{reset}", sig.description);
        }
        println!();
    }

    println!("  {bold}Legend:{reset}  {bold}P{reset}{green}\u{2713}{reset}{green}\u{2713}{reset}{green}\u{2713}{reset} = supported levels  {green}\u{2713}{reset} = yes  {dim}\u{2717}{reset} = no");
    println!("    {bold}P{reset}3  Cranelift native code       {dim}(~0.2ns/node){reset}");
    println!("    {bold}P{reset}2  Compiled u64 closure        {dim}(~4.5ns/node){reset}");
    println!("    {bold}P{reset}1  Runtime Value interpreter   {dim}(~70ns/node){reset}");
    println!();
    println!("  {dim}Levels probed from live node instances.{reset}");
    println!("  {dim}Nodes with constant params (mod, div, etc.) reach P3 when{reset}");
    println!("  {dim}constants are known at assembly time, P2 otherwise.{reset}");
    println!();
}

/// Display embedded stdlib modules with their typed signatures.
///
/// Parses each `.gk` source from the compiled-in standard library,
/// extracts `ModuleDef` statements, and prints them grouped by
/// category (source filename) with ANSI coloring.
fn describe_gk_stdlib() {
    use nb_variates::dsl::lexer::lex;
    use nb_variates::dsl::parser::parse;
    use nb_variates::dsl::ast::Statement;

    let sources = nb_variates::dsl::stdlib_sources();
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    let (bold, dim, reset, green, cyan, magenta) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[35m")
    } else {
        ("", "", "", "", "", "")
    };

    println!();
    println!("{bold}GK Standard Library{reset}");
    println!("{bold}═══════════════════{reset}");
    println!();

    for (filename, source) in sources {
        // Category name: filename without .gk extension, title-cased
        let category = filename
            .strip_suffix(".gk")
            .unwrap_or(filename);
        let category_title = category
            .chars()
            .enumerate()
            .map(|(i, c)| if i == 0 { c.to_ascii_uppercase() } else { c })
            .collect::<String>();

        let tokens = match lex(source) {
            Ok(t) => t,
            Err(e) => { eprintln!("warning: failed to lex stdlib file: {e}"); continue; }
        };
        let ast = match parse(tokens) {
            Ok(a) => a,
            Err(e) => { eprintln!("warning: failed to parse stdlib file: {e}"); continue; }
        };

        // Collect module defs from this file
        let mut modules = Vec::new();
        for stmt in &ast.statements {
            if let Statement::ModuleDef(mdef) = stmt {
                modules.push(mdef);
            }
        }

        if modules.is_empty() {
            continue;
        }

        println!("  {bold}{cyan}── {category_title} ──{reset}");
        println!();

        for mdef in &modules {
            // Build typed params string: (name: type, name: type, ...)
            let params_str = mdef.params.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            // Build typed outputs string: (name: type, ...)
            let outputs_str = mdef.outputs.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            let signature = format!("({params_str}) -> ({outputs_str})");

            // Extract the first comment line immediately before this module def
            // by scanning the source text for the comment block above the def
            let description = extract_first_comment(source, &mdef.name);

            // Name column: bold magenta, padded to 24 chars
            let name_padded = format!("{:<24}", mdef.name);
            print!("  {bold}{magenta}{name_padded}{reset}");

            // Signature in green
            println!(" {green}{signature}{reset}");

            // Description on the next line, indented and dim
            if let Some(desc) = description {
                println!("  {:<24} {dim}{desc}{reset}", "");
            }

            println!();
        }
    }
}

/// Display GK modules found in a directory.
///
/// Scans a directory for `.gk` files, parses each one, extracts
/// `ModuleDef` statements, and displays them with their typed
/// signatures — same format as `describe gk stdlib`.
///
/// Usage:
///   nbrs describe gk modules [--dir=path]
fn describe_gk_modules(args: &[String]) {
    use nb_variates::dsl::lexer::lex;
    use nb_variates::dsl::parser::parse;
    use nb_variates::dsl::ast::Statement;

    let dir = args.iter()
        .find_map(|a| a.strip_prefix("--dir="))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")));

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    let (bold, dim, reset, green, cyan, magenta) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[35m")
    } else {
        ("", "", "", "", "", "")
    };

    println!();
    println!("{bold}GK Modules in {}{reset}", dir.display());
    println!("{bold}{}{reset}", "═".repeat(15 + dir.display().to_string().len()));
    println!();

    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("error: cannot read directory '{}': {e}", dir.display());
            return;
        }
    };

    let mut gk_files: Vec<std::path::PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("gk"))
        .collect();
    gk_files.sort();

    if gk_files.is_empty() {
        println!("  {dim}(no .gk files found){reset}");
        println!();
        return;
    }

    for path in &gk_files {
        let source = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => { eprintln!("warning: failed to read {}: {e}", path.display()); continue; }
        };

        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        let category = filename
            .strip_suffix(".gk")
            .unwrap_or(filename);
        let category_title = category
            .chars()
            .enumerate()
            .map(|(i, c)| if i == 0 { c.to_ascii_uppercase() } else { c })
            .collect::<String>();

        let tokens = match lex(&source) {
            Ok(t) => t,
            Err(e) => { eprintln!("warning: failed to lex {filename}: {e}"); continue; }
        };
        let ast = match parse(tokens) {
            Ok(a) => a,
            Err(e) => { eprintln!("warning: failed to parse {filename}: {e}"); continue; }
        };

        let mut modules = Vec::new();
        for stmt in &ast.statements {
            if let Statement::ModuleDef(mdef) = stmt {
                modules.push(mdef);
            }
        }

        if modules.is_empty() {
            continue;
        }

        println!("  {bold}{cyan}-- {category_title} ({filename}) --{reset}");
        println!();

        for mdef in &modules {
            let params_str = mdef.params.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            let outputs_str = mdef.outputs.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            let signature = format!("({params_str}) -> ({outputs_str})");

            let description = extract_first_comment(&source, &mdef.name);

            let name_padded = format!("{:<24}", mdef.name);
            print!("  {bold}{magenta}{name_padded}{reset}");
            println!(" {green}{signature}{reset}");

            if let Some(desc) = description {
                println!("  {:<24} {dim}{desc}{reset}", "");
            }

            println!();
        }
    }
}

/// Extract the first comment line above a module definition.
///
/// Scans for `// <text>` lines in the comment block immediately
/// preceding the line that starts with `<name>(`. Only the nearest
/// contiguous comment block is considered — a blank line ends the
/// block. Returns the first non-empty line from that block.
fn extract_first_comment(source: &str, name: &str) -> Option<String> {
    let lines: Vec<&str> = source.lines().collect();
    // Find the line where the module def starts
    let def_prefix = format!("{name}(");
    let def_idx = lines.iter().position(|l| l.trim_start().starts_with(&def_prefix))?;

    // Walk backwards from the def line, collecting the nearest comment block.
    // Stop at the first blank line or non-comment line.
    let mut comment_lines = Vec::new();
    let mut i = def_idx;
    let mut seen_comment = false;
    while i > 0 {
        i -= 1;
        let trimmed = lines[i].trim();
        if trimmed.starts_with("//") {
            let text = trimmed.strip_prefix("//").unwrap().trim();
            comment_lines.push(text);
            seen_comment = true;
        } else if trimmed.is_empty() {
            if seen_comment {
                // Blank line after we already found comments — end of block
                break;
            }
            // Blank line directly above def (before any comment) — skip
            continue;
        } else {
            break;
        }
    }

    // comment_lines is in reverse order; flip to get first-to-last
    comment_lines.reverse();
    // Return the first non-empty line
    for line in &comment_lines {
        if !line.is_empty() {
            return Some(line.to_string());
        }
    }
    None
}

/// Render a GK source file as DOT, Mermaid, or SVG.
///
/// Usage:
///   nbrs describe gk dag <file.gk> [--format=dot|mermaid|svg] [--output=file]
fn describe_gk_dag(args: &[String]) {
    use nb_variates::viz;

    let file = args.iter().find(|a| !a.starts_with("--"));
    let format = args.iter()
        .find_map(|a| a.strip_prefix("--format="))
        .unwrap_or("dot");
    let output = args.iter()
        .find_map(|a| a.strip_prefix("--output="));

    let source = match file {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: failed to read '{}': {e}", path);
                return;
            }
        },
        None => {
            eprintln!("nbrs describe gk dag <file.gk> [--format=dot|mermaid|svg] [--output=file]");
            eprintln!();
            eprintln!("Renders a GK source file as a DAG diagram.");
            eprintln!("  --format=dot       DOT digraph (default)");
            eprintln!("  --format=mermaid   Mermaid flowchart");
            eprintln!("  --format=svg       Self-contained SVG (pure Rust, no external tools)");
            eprintln!("  --output=file      Write to file instead of stdout");
            return;
        }
    };

    let result = match format {
        "dot" => viz::gk_to_dot(&source),
        "mermaid" => viz::gk_to_mermaid(&source),
        "svg" => viz::gk_to_svg(&source),
        other => {
            eprintln!("error: unknown format '{other}' (use dot, mermaid, or svg)");
            return;
        }
    };

    match result {
        Ok(content) => {
            if let Some(path) = output {
                match std::fs::write(path, &content) {
                    Ok(()) => eprintln!("wrote {} bytes to {path}", content.len()),
                    Err(e) => eprintln!("error: failed to write '{path}': {e}"),
                }
            } else {
                println!("{content}");
            }
        }
        Err(e) => eprintln!("error: {e}"),
    }
}

// =================================================================
// Bench command: A/B timing across compilation levels and threads
// =================================================================

/// Parse a range spec: `N`, `start:end:step`, or `start:end*factor`.
///
/// - `4` → [4]
/// - `1:8:2` → [1, 3, 5, 7]
/// - `1:64*2` → [1, 2, 4, 8, 16, 32, 64]
fn parse_range(s: &str) -> Vec<u64> {
    // Single value
    if !s.contains(':') {
        return vec![s.parse().unwrap_or(1)];
    }

    let parts: Vec<&str> = s.splitn(2, ':').collect();
    let start: f64 = parts[0].parse().unwrap_or(1.0);
    let rest = parts[1];

    // Geometric: start:end*factor
    if let Some(pos) = rest.find('*') {
        let end: f64 = rest[..pos].parse().unwrap_or(start);
        let factor: f64 = rest[pos + 1..].parse().unwrap_or(2.0);
        let mut result = Vec::new();
        let mut v = start;
        while v <= end + 0.001 {
            let rounded = v.round() as u64;
            if rounded >= 1 && (result.is_empty() || *result.last().unwrap() != rounded) {
                result.push(rounded);
            }
            v *= factor;
        }
        return result;
    }

    // Linear: start:end:step
    if let Some(pos) = rest.find(':') {
        let end: f64 = rest[..pos].parse().unwrap_or(start);
        let step: f64 = rest[pos + 1..].parse().unwrap_or(1.0);
        let mut result = Vec::new();
        let mut v = start;
        while v <= end + 0.001 {
            result.push(v.round() as u64);
            v += step;
        }
        return result;
    }

    // start:end (implicit step=1)
    let end: f64 = rest.parse().unwrap_or(start);
    (start as u64..=end as u64).collect()
}

/// Benchmark a GK expression at all compilation levels, optionally
/// across multiple thread counts.
///
/// Usage: `nbrs bench gk <expr> [cycles=N] [threads=RANGE]`
///
/// Range syntax:
///   threads=4          single value
///   threads=1:8:2      linear: 1, 3, 5, 7
///   threads=1:64*2     geometric: 1, 2, 4, 8, 16, 32, 64
fn bench_command(args: &[String]) {
    let topic = args.first().map(|s| s.as_str()).unwrap_or("");
    if topic != "gk" {
        eprintln!("Usage: nbrs bench gk <expr> [cycles=N] [threads=RANGE]");
        eprintln!("  Example: nbrs bench gk \"hash_range(hash(cycle), 1000)\"");
        eprintln!("  Example: nbrs bench gk \"weighted_pick(hash(cycle), 0.5, 10, 0.3, 20)\" threads=1:8*2");
        eprintln!();
        eprintln!("Range syntax: N | start:end:step | start:end*factor");
        return;
    }

    let rest = &args[1..];
    let mut expr = String::new();
    let mut cycles: u64 = 100_000;
    let mut thread_counts: Vec<u64> = vec![1];
    let mut explain = false;

    let mut i = 0;
    while i < rest.len() {
        let arg = &rest[i];
        if let Some(val) = arg.strip_prefix("cycles=") {
            cycles = val.parse().unwrap_or(100_000);
        } else if let Some(val) = arg.strip_prefix("threads=") {
            thread_counts = parse_range(val);
        } else if arg == "--cycles" || arg == "-c" {
            i += 1;
            if i < rest.len() {
                cycles = rest[i].parse().unwrap_or(100_000);
            }
        } else if arg == "--threads" || arg == "-t" {
            i += 1;
            if i < rest.len() {
                thread_counts = parse_range(&rest[i]);
            }
        } else if arg.starts_with("--cycles=") {
            cycles = arg["--cycles=".len()..].parse().unwrap_or(100_000);
        } else if arg.starts_with("--threads=") {
            thread_counts = parse_range(&arg["--threads=".len()..]);
        } else if arg == "--explain" {
            explain = true;
        } else if arg.starts_with('-') {
            eprintln!("error: unrecognized option '{arg}'");
            eprintln!("  Valid options: cycles=N, threads=RANGE, --cycles N, --threads RANGE, --explain");
            eprintln!("  The expression can also be a .gk file path.");
            return;
        } else if expr.is_empty() {
            expr = arg.clone();
        } else {
            eprintln!("error: unexpected argument '{arg}'");
            eprintln!("  The GK expression must be quoted if it contains spaces.");
            return;
        }
        i += 1;
    }

    if expr.is_empty() {
        eprintln!("Usage: nbrs bench gk <expr|file.gk> [cycles=N] [threads=RANGE] [--explain]");
        eprintln!("  Example: nbrs bench gk \"hash_range(hash(cycle), 1000)\"");
        eprintln!("  Example: nbrs bench gk tests/examples/gk/constant_folding.gk --explain");
        return;
    }

    // If the expression is a .gk file path, read it as a complete program.
    // Otherwise, treat as an inline expression to auto-wrap.
    let source = if expr.ends_with(".gk") {
        match std::fs::read_to_string(&expr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: failed to read '{expr}': {e}");
                return;
            }
        }
    } else {
        // Allow semicolons as statement separators
        let expr = expr.replace(';', "\n");

        if expr.contains(":=") {
            let lines: Vec<&str> = expr.lines().map(|l| l.trim())
                .filter(|l| !l.is_empty() && !l.starts_with("//"))
                .collect();
            let mut out_lines = vec!["coordinates := (cycle)".to_string()];
            for (i, line) in lines.iter().enumerate() {
                if line.contains(":=") {
                    out_lines.push(line.to_string());
                } else if i == lines.len() - 1 {
                    out_lines.push(format!("out := {line}"));
                } else {
                    out_lines.push(format!("__expr_{i} := {line}"));
                }
            }
            out_lines.join("\n")
        } else {
            format!("coordinates := (cycle)\nout := {expr}")
        }
    };
    let warmup = 1000u64;

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let (bold, dim, reset, green) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m")
    } else {
        ("", "", "", "")
    };

    // --explain: detailed compilation walkthrough
    if explain {
        use nb_variates::dsl::events::{CompileEvent, CompileEventLog};
        let mut log = CompileEventLog::new();

        println!("{bold}GK Compilation Explain{reset}");
        println!();

        // 1. Show the normalized source
        println!("{bold}Source:{reset}");
        for line in source.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("//") {
                println!("  {dim}{trimmed}{reset}");
            }
        }
        println!();

        // 2. Parse
        let tokens = match nb_variates::dsl::lexer::lex(&source) {
            Ok(t) => t,
            Err(e) => { eprintln!("error: lex failed: {e}"); return; }
        };
        let ast = match nb_variates::dsl::parser::parse(tokens) {
            Ok(a) => a,
            Err(e) => { eprintln!("error: parse failed: {e}"); return; }
        };
        log.push(CompileEvent::Parsed { statements: ast.statements.len() });

        // 3. Compile with event logging
        match compile_gk_to_assembler(&source) {
            Err(e) => { eprintln!("error: compile failed: {e}"); return; }
            Ok(asm) => {
                for name in asm.output_names() {
                    log.push(CompileEvent::OutputDeclared { name: name.to_string() });
                }

                match asm.compile_with_log(Some(&mut log)) {
                    Ok(kernel) => {
                        let program = kernel.program();

                        // 4. Show optimization events
                        let events = log.events();
                        let has_optimizations = events.iter().any(|e| matches!(e,
                            CompileEvent::ConstantFolded { .. } |
                            CompileEvent::TypeAdapterInserted { .. } |
                            CompileEvent::FusionApplied { .. }
                        ));
                        if has_optimizations {
                            println!("{bold}Optimizations:{reset}");
                            for event in events {
                                match event {
                                    CompileEvent::ConstantFolded { node, value } =>
                                        println!("  {green}constant folded:{reset} {node} → {value}"),
                                    CompileEvent::TypeAdapterInserted { from_node, to_node, adapter } =>
                                        println!("  type adapter: {from_node} → {to_node} ({adapter})"),
                                    CompileEvent::FusionApplied { pattern, nodes_replaced } =>
                                        println!("  {green}fusion:{reset} {pattern} ({nodes_replaced} nodes merged)"),
                                    _ => {}
                                }
                            }
                            println!();
                        }

                        // 5. Show per-output compile levels
                        println!("{bold}Outputs:{reset}");
                        for name in program.output_names() {
                            if let Some((node_idx, _)) = program.resolve_output(name) {
                                let level = program.node_compile_level(node_idx);
                                let level_str = format!("{level:?}");
                                let color = if level_str.contains("3") { green } else { "" };
                                println!("  {name}: {color}{level_str}{reset}");
                            }
                        }
                        println!();

                        // 6. Summary
                        println!("{bold}Summary:{reset} {} nodes, {} outputs, {} constant(s) folded",
                            program.node_count(),
                            program.output_names().len(),
                            kernel.constants_folded);
                        println!();
                    }
                    Err(e) => {
                        eprintln!("error: assembly failed: {e}");
                        return;
                    }
                }
            }
        }
    }

    use nb_variates::dsl::compile::compile_gk_to_assembler;

    // Show input/output type summary
    if let Ok(asm) = compile_gk_to_assembler(&source) {
        if let Ok(kernel) = asm.compile() {
            let program = kernel.program();

            // Inputs (coordinates)
            let coords = program.coord_names();
            if !coords.is_empty() {
                let coord_list: Vec<String> = coords.iter()
                    .map(|c| format!("{c}: u64"))
                    .collect();
                println!("{bold}Inputs:{reset}  {}", coord_list.join(", "));
            }

            // Outputs with types
            let mut out_list: Vec<String> = Vec::new();
            for name in program.output_names() {
                if name.starts_with("__") { continue; }
                if let Some((node_idx, port_idx)) = program.resolve_output(name) {
                    let port_type = &program.node_meta(node_idx).outs[port_idx].typ;
                    out_list.push(format!("{name}: {port_type:?}"));
                }
            }
            if !out_list.is_empty() {
                out_list.sort();
                println!("{bold}Outputs:{reset} {}", out_list.join(", "));
            }
            println!();
        }
    }

    println!("{bold}Benchmarking:{reset} {}", if expr.len() > 60 { &expr[..57] } else { &expr }.replace('\n', "; ").trim());
    println!("{dim}  {cycles} cycles per thread, {warmup} warmup{reset}");

    for &nthreads in &thread_counts {
        let nthreads = nthreads.max(1) as usize;

        if thread_counts.len() > 1 {
            println!();
            println!("  {bold}--- {nthreads} thread{} ---{reset}",
                if nthreads == 1 { "" } else { "s" });
        }
        println!();
        println!("  {bold}{:<10} {:>12} {:>12} {:>8} {:>14}  {reset}",
            "Level", "Total", "Per-cycle", "Speedup", "Throughput");
        println!("  {}", "-".repeat(62));

        let total_ops = (cycles * nthreads as u64) as f64;
        let mut p1_ns: Option<f64> = None;

        // Phase 1: uses Arc<GkProgram> + per-thread GkState (true multi-thread)
        match compile_gk_to_assembler(&source) {
            Err(e) => {
                eprintln!("  {bold}compile error:{reset} {e}");
                return;
            }
            Ok(asm) => if let Ok(kernel) = asm.compile() {
                let program = kernel.program().clone();
                let output_name = program.output_names().first()
                    .map(|s| s.to_string()).unwrap_or_else(|| "out".to_string());
                // Warmup
                {
                    let mut state = program.create_state();
                    for c in 0..warmup { state.set_coordinates(&[c]); state.pull(&program, &output_name); }
                }
                let per_thread = cycles;
                let barrier = Arc::new(std::sync::Barrier::new(nthreads));
                let elapsed = Arc::new(std::sync::Mutex::new(std::time::Duration::ZERO));

                std::thread::scope(|s| {
                    for tid in 0..nthreads {
                        let program = program.clone();
                        let barrier = barrier.clone();
                        let elapsed = elapsed.clone();
                        let out = output_name.clone();
                        s.spawn(move || {
                            let mut state = program.create_state();
                            let base = tid as u64 * per_thread;
                            barrier.wait();
                            let start = std::time::Instant::now();
                            for c in base..base + per_thread {
                                state.set_coordinates(&[c]);
                                state.pull(&program, &out);
                            }
                            let e = start.elapsed();
                            // Take the max elapsed across threads (wall-clock)
                            let mut guard = elapsed.lock().unwrap();
                            if e > *guard { *guard = e; }
                        });
                    }
                });

                let wall = *elapsed.lock().unwrap();
                let per_cycle = wall.as_nanos() as f64 / total_ops;
                let throughput = total_ops / wall.as_secs_f64();
                p1_ns = Some(per_cycle);
                println!("  {:<10} {:>12} {:>10.1} ns {:>7.1}x {:>12.0} /s  {dim}runtime interpreter{reset}",
                    "P1", format_duration(wall), per_cycle, 1.0, throughput);
            }
        }

        // Find the last binding name from the source for output resolution.
        // P2/Hybrid/P3 kernels need an output slot name to read from.
        let last_binding: String = source.lines().rev()
            .filter_map(|line| {
                let line = line.trim();
                if let Some(pos) = line.find(":=") {
                    let target = line[..pos].trim().trim_matches(|c| c == '(' || c == ')');
                    // Take the last target in a multi-target binding
                    target.split(',').next_back().map(|s| s.trim().to_string())
                } else {
                    None
                }
            })
            .next()
            .unwrap_or_else(|| "out".to_string());

        // P2, Hybrid, P3: each thread gets its own compiled kernel
        // (the buffer is mutable per-kernel, no sharing needed).
        let levels: Vec<(&str, bool, &str)> = vec![
            ("P2", false, "compiled u64 closures"),
            ("Hybrid", true, "per-node optimal (JIT + closure)"),
            ("P3", true, "Cranelift JIT native code"),
        ];

        for (level_name, highlight, desc) in &levels {
            let kernels: Vec<_> = (0..nthreads).filter_map(|_| {
                let asm = compile_gk_to_assembler(&source).ok()?;
                match *level_name {
                    "P2" => asm.try_compile().ok().and_then(|mut k| {
                        let out = k.resolve_output(&last_binding)?;
                        Some(Box::new(move |c: u64| { k.eval(&[c]); let _ = k.get_slot(out); })
                            as Box<dyn FnMut(u64) + Send>)
                    }),
                    "Hybrid" => asm.compile_hybrid().ok().and_then(|mut k| {
                        let out = k.resolve_output(&last_binding)?;
                        Some(Box::new(move |c: u64| { k.eval(&[c]); let _ = k.get_slot(out); })
                            as Box<dyn FnMut(u64) + Send>)
                    }),
                    "P3" => asm.try_compile_jit().ok().and_then(|mut k| {
                        let out = k.resolve_output(&last_binding)?;
                        Some(Box::new(move |c: u64| { k.eval(&[c]); let _ = k.get_slot(out); })
                            as Box<dyn FnMut(u64) + Send>)
                    }),
                    _ => None,
                }
            }).collect();

            if kernels.len() != nthreads {
                if kernels.is_empty() {
                    println!("  {dim}{:<10} not available{reset}", level_name);
                }
                continue;
            }

            // Warmup on first kernel
            // (already warmed by construction + first eval)

            let per_thread = cycles;
            let barrier = Arc::new(std::sync::Barrier::new(nthreads));
            let elapsed = Arc::new(std::sync::Mutex::new(std::time::Duration::ZERO));

            let kernel_cells: Vec<_> = kernels.into_iter()
                .map(std::sync::Mutex::new)
                .collect();

            std::thread::scope(|s| {
                for (tid, cell) in kernel_cells.iter().enumerate() {
                    let barrier = barrier.clone();
                    let elapsed = elapsed.clone();
                    s.spawn(move || {
                        let mut kernel = cell.lock().unwrap();
                        let base = tid as u64 * per_thread;
                        // Warmup
                        for c in base..base + warmup { (kernel)(c); }
                        barrier.wait();
                        let start = std::time::Instant::now();
                        for c in base..base + per_thread { (kernel)(c); }
                        let e = start.elapsed();
                        let mut guard = elapsed.lock().unwrap();
                        if e > *guard { *guard = e; }
                    });
                }
            });

            let wall = *elapsed.lock().unwrap();
            let per_cycle = wall.as_nanos() as f64 / total_ops;
            let speedup = p1_ns.map(|p| p / per_cycle).unwrap_or(0.0);
            let throughput = total_ops / wall.as_secs_f64();
            if *highlight {
                println!("  {green}{:<10}{reset} {:>12} {:>10.1} ns {:>7.1}x {:>12.0} /s  {dim}{desc}{reset}",
                    level_name, format_duration(wall), per_cycle, speedup, throughput);
            } else {
                println!("  {:<10} {:>12} {:>10.1} ns {:>7.1}x {:>12.0} /s  {dim}{desc}{reset}",
                    level_name, format_duration(wall), per_cycle, speedup, throughput);
            }
        }
    }

    println!();
}

// Shell completions are handled by veks-completion (see cli_tree()).
// The cli_tree() function is the single source of truth.

fn _deleted_bash_completions() {
    // Get all GK function names for completing expressions
    let funcs: Vec<&str> = nb_variates::dsl::registry::registry()
        .iter()
        .map(|s| s.name)
        .collect();
    let func_list = funcs.join(" ");

    print!(r#"_nbrs() {{
    local cur prev words cword
    _init_completion || return

    local subcommands="run describe bench web completions"
    local describe_topics="gk"
    local describe_gk="functions stdlib dag modules"
    local bench_topics="gk"
    local web_flags="--daemon --stop --restart"
    local run_params="workload= adapter= cycles= threads= rate= stanzarate= tags= seq= format= errors= filename= op= gk-lib="
    local bench_params="cycles= threads= --cycles --threads -c -t --explain --gk-explain"
    local gk_functions="{func_list}"

    case "${{cword}}" in
        1)
            COMPREPLY=( $(compgen -W "$subcommands" -- "$cur") )
            return
            ;;
    esac

    case "${{words[1]}}" in
        describe)
            case "${{cword}}" in
                2) COMPREPLY=( $(compgen -W "$describe_topics" -- "$cur") ) ;;
                3)
                    if [[ "${{words[2]}}" == "gk" ]]; then
                        COMPREPLY=( $(compgen -W "$describe_gk" -- "$cur") )
                    fi
                    ;;
            esac
            ;;
        bench)
            case "${{cword}}" in
                2) COMPREPLY=( $(compgen -W "$bench_topics" -- "$cur") ) ;;
                *)
                    COMPREPLY=( $(compgen -W "$bench_params" -- "$cur") )
                    ;;
            esac
            ;;
        web)
            COMPREPLY=( $(compgen -W "$web_flags bind= port=" -- "$cur") )
            ;;
        run)
            COMPREPLY=( $(compgen -W "$run_params" -- "$cur") )
            # Also complete YAML files
            _filedir yaml
            _filedir yml
            ;;
        completions)
            COMPREPLY=( $(compgen -W "bash zsh fish" -- "$cur") )
            ;;
    esac
}}
complete -o nospace -F _nbrs nbrs
"#);
}

fn _deleted_zsh_completions() {
    print!(r#"#compdef nbrs

_nbrs() {{
    local -a subcommands
    subcommands=(
        'run:Execute a workload'
        'describe:Describe GK functions, stdlib, or DAG'
        'bench:Benchmark a GK expression at all compilation levels'
        'web:Start the web dashboard'
        'completions:Output shell completion script'
    )

    if (( CURRENT == 2 )); then
        _describe 'subcommand' subcommands
        return
    fi

    case "$words[2]" in
        describe)
            if (( CURRENT == 3 )); then
                _values 'topic' gk
            elif (( CURRENT == 4 )) && [[ "$words[3]" == "gk" ]]; then
                _values 'subtopic' functions stdlib dag modules
            fi
            ;;
        bench)
            if (( CURRENT == 3 )); then
                _values 'topic' gk
            else
                _values 'option' --explain --gk-explain
                _message 'expr or option (cycles=N threads=RANGE --explain)'
            fi
            ;;
        web)
            _values 'option' --daemon --stop --restart 'bind=' 'port='
            ;;
        run)
            _files -g '*.y(a|)ml'
            _message 'param=value'
            ;;
        completions)
            _values 'shell' bash zsh fish
            ;;
    esac
}}

_nbrs "$@"
"#);
}

fn _deleted_fish_completions() {
    println!("# nbrs fish completions");
    println!("complete -c nbrs -n '__fish_use_subcommand' -a run -d 'Execute a workload'");
    println!("complete -c nbrs -n '__fish_use_subcommand' -a describe -d 'Describe GK functions'");
    println!("complete -c nbrs -n '__fish_use_subcommand' -a bench -d 'Benchmark GK expression'");
    println!("complete -c nbrs -n '__fish_use_subcommand' -a web -d 'Start web dashboard'");
    println!("complete -c nbrs -n '__fish_use_subcommand' -a completions -d 'Output shell completions'");
    println!("complete -c nbrs -n '__fish_seen_subcommand_from describe' -a 'gk'");
    println!("complete -c nbrs -n '__fish_seen_subcommand_from bench' -a 'gk'");
    println!("complete -c nbrs -n '__fish_seen_subcommand_from bench' -l explain -d 'Dump GK compilation event stream'");
    println!("complete -c nbrs -n '__fish_seen_subcommand_from web' -l daemon -d 'Run in background'");
    println!("complete -c nbrs -n '__fish_seen_subcommand_from web' -l stop -d 'Stop daemon'");
    println!("complete -c nbrs -n '__fish_seen_subcommand_from web' -l restart -d 'Restart daemon'");
    println!("complete -c nbrs -n '__fish_seen_subcommand_from completions' -a 'bash zsh fish'");
}

/// Parse a bind address flexibly. Accepts any of:
///   `0.0.0.0`                     → (0.0.0.0, default_port)
///   `0.0.0.0:8085`                → (0.0.0.0, 8085)
///   `http://0.0.0.0:8085/`        → (0.0.0.0, 8085)
///   `http://0.0.0.0/`             → (0.0.0.0, default_port)
///   `https://localhost:9090/path`  → (localhost, 9090)
///
/// The `port_override` from a separate `port=` arg takes precedence
/// over any port embedded in the bind string.
fn parse_bind_address(raw: &str, port_override: Option<&str>) -> (String, u16) {
    let default_port = 8080u16;

    // Strip scheme (http://, https://)
    let without_scheme = raw
        .strip_prefix("http://").or_else(|| raw.strip_prefix("https://"))
        .unwrap_or(raw);

    // Strip trailing path (everything after first /)
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);

    // Split host and port
    let (host, embedded_port) = if let Some(colon_pos) = host_port.rfind(':') {
        let maybe_port = &host_port[colon_pos + 1..];
        if let Ok(p) = maybe_port.parse::<u16>() {
            (host_port[..colon_pos].to_string(), Some(p))
        } else {
            (host_port.to_string(), None)
        }
    } else {
        (host_port.to_string(), None)
    };

    // port= arg overrides embedded port, which overrides default
    let port = port_override
        .and_then(|s| s.parse::<u16>().ok())
        .or(embedded_port)
        .unwrap_or(default_port);

    let host = if host.is_empty() { "0.0.0.0".to_string() } else { host };
    (host, port)
}

fn format_duration(d: std::time::Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", ns as f64 / 1_000_000_000.0)
    }
}

/// Parse `key=value` pairs from command line args.
fn parse_params(args: &[String]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for arg in args {
        if let Some(eq_pos) = arg.find('=') {
            let key = arg[..eq_pos].to_string();
            let value = arg[eq_pos + 1..].to_string();
            params.insert(key, value);
        }
    }
    params
}

async fn run_command(args: &[String]) {
    // Skip "run" if present
    let args = if args.first().map(|s| s.as_str()) == Some("run") {
        &args[1..]
    } else {
        args
    };

    let params = parse_params(args);

    // Load workload — from inline op= or YAML file.
    let mut workload_file: Option<String> = None;
    let workload = if let Some(op_str) = params.get("op") {
        if params.contains_key("workload") {
            eprintln!("nbrs: warning: op= overrides workload=");
        }
        match nb_workload::inline::synthesize_inline_workload(op_str) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("error: failed to synthesize inline workload: {e}");
                std::process::exit(1);
            }
        }
    } else {
        let workload_path = params.get("workload")
            .or_else(|| {
                // Look for a .yaml file in the args
                args.iter().find(|a| a.ends_with(".yaml") || a.ends_with(".yml"))
            });

        let workload_path = match workload_path {
            Some(p) => p.clone(),
            None => {
                eprintln!("error: no workload specified");
                eprintln!("  use: nbrs run workload=file.yaml ...");
                eprintln!("   or: nbrs run op='hello {{{{cycle}}}}' cycles=10");
                std::process::exit(1);
            }
        };

        workload_file = Some(workload_path.clone());

        let yaml_source = match std::fs::read_to_string(&workload_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: failed to read workload file '{}': {}", workload_path, e);
                std::process::exit(1);
            }
        };

        match parse_workload(&yaml_source, &params) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("error: failed to parse workload: {e}");
                std::process::exit(1);
            }
        }
    };

    // Extract activity parameters
    let driver = params.get("adapter")
        .map(|s| s.as_str())
        .unwrap_or("stdout");
    let explicit_cycles: Option<u64> = params.get("cycles").and_then(|s| parse_count(s));
    let threads: usize = params.get("threads").and_then(|s| s.parse().ok()).unwrap_or(1);
    let cycle_rate: Option<f64> = params.get("rate").and_then(|s| s.parse().ok());
    let stanza_rate: Option<f64> = params.get("stanzarate").and_then(|s| s.parse().ok());
    let tag_filter = params.get("tags").map(|s| s.as_str());
    let seq_type = params.get("seq")
        .map(|s| SequencerType::parse(s).unwrap_or(SequencerType::Bucket))
        .unwrap_or(SequencerType::Bucket);
    let error_spec = params.get("errors")
        .cloned()
        .unwrap_or_else(|| ".*:warn,counter".to_string());
    let format = params.get("format")
        .map(|s| StdoutFormat::parse(s).unwrap_or(StdoutFormat::Assignments))
        .unwrap_or(StdoutFormat::Statement);
    let filename = params.get("filename")
        .cloned()
        .unwrap_or_else(|| "stdout".to_string());

    // Extract workload params before consuming ops
    let workload_params = workload.params;

    // Filter ops by tags
    let mut ops = workload.ops;
    if let Some(filter) = tag_filter {
        ops = match TagFilter::filter_ops(&ops, filter) {
            Ok(filtered) => filtered,
            Err(e) => {
                eprintln!("error: invalid tag filter: {e}");
                std::process::exit(1);
            }
        };
    }

    if ops.is_empty() {
        eprintln!("error: no ops selected (tag filter may have excluded all ops)");
        std::process::exit(1);
    }

    // Warn about ops without explicit adapter selection, auto-assign to default
    let has_explicit_adapter = params.contains_key("adapter") || params.contains_key("driver");
    for op in &ops {
        let op_adapter = op.params.get("adapter")
            .and_then(|v| v.as_str())
            .or_else(|| op.params.get("driver").and_then(|v| v.as_str()));
        if op_adapter.is_none() && !has_explicit_adapter {
            eprintln!("warning: op '{}' has no adapter selection — using '{driver}'", op.name);
        }
    }

    eprintln!("nbrs: {} ops selected, {} cycles, {} threads, adapter={}",
        ops.len(), explicit_cycles.map(|c| c.to_string()).unwrap_or("auto".into()), threads, driver);

    // Check for --strict and --dry-run flags
    let strict = args.iter().any(|a| a == "--strict");
    let dry_run = args.iter().find_map(|a| {
        if a == "--dry-run" { Some("silent") }
        else if let Some(mode) = a.strip_prefix("--dry-run=") { Some(mode) }
        else { None }
    });

    // Collect --gk-lib=path flags
    let gk_lib_paths: Vec<std::path::PathBuf> = args.iter()
        .filter_map(|a| a.strip_prefix("--gk-lib="))
        .map(std::path::PathBuf::from)
        .collect();

    // Expand workload params in GK binding source before compilation.
    if !workload_params.is_empty() {
        for op in &mut ops {
            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                for (key, value) in &workload_params {
                    let placeholder = format!("{{{key}}}");
                    if src.contains(&placeholder) {
                        *src = src.replace(&placeholder, value);
                    }
                }
            }
        }
    }

    // Compile bindings into GK kernel, with module resolution from the workload directory
    let workload_dir: Option<&std::path::Path> = workload_file.as_ref()
        .and_then(|p| std::path::Path::new(p).parent())
        .or_else(|| Some(std::path::Path::new(".")));
    // Workload params are excluded from bind-point validation —
    // they resolve at cycle time via the synthesis pipeline.
    let wp_names: Vec<String> = workload_params.keys().cloned().collect();
    let kernel = match compile_bindings_with_libs_excluding(&ops, workload_dir, gk_lib_paths, strict, &wp_names) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("error: failed to compile bindings: {e}");
            std::process::exit(1);
        }
    };

    // Build op sequence
    let op_sequence = OpSequence::from_ops(ops, seq_type);

    // Default cycles to one stanza if not specified
    let cycles = explicit_cycles.unwrap_or(op_sequence.stanza_length() as u64);
    eprintln!("nbrs: stanza length={}, sequencer={:?}", op_sequence.stanza_length(), seq_type);

    // Create and run activity
    let stanza_concurrency: usize = params.get("stanza_concurrency")
        .or_else(|| params.get("sc"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let config = ActivityConfig {
        name: "main".into(),
        cycles,
        concurrency: threads,
        cycle_rate,
        stanza_rate,
        sequencer: seq_type,
        error_spec,
        max_retries: 3,
        stanza_concurrency,
    };

    let builder = Arc::new(OpBuilder::new(kernel));
    let activity = Activity::with_params(config, &Labels::of("session", "cli"), op_sequence, workload_params);

    // Check for --tui flag
    let use_tui = args.iter().any(|a| a == "--tui");

    // Get shared metrics before activity is consumed by run()
    let shared_metrics = activity.shared_metrics();

    // If TUI mode, spawn metrics capture thread + TUI thread
    let tui_handle = if use_tui {
        let (tui_reporter, tui_rx) = TuiReporter::channel();

        // Start a metrics capture thread that periodically snapshots
        // the activity's instruments and sends frames to the TUI.
        let capture_metrics = shared_metrics.clone();
        let capture_interval = std::time::Duration::from_millis(500);
        let capture_running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let capture_flag = capture_running.clone();

        let mut reporter = tui_reporter;
        std::thread::spawn(move || {
            use nb_metrics::scheduler::Reporter;
            while capture_flag.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(capture_interval);
                let frame = capture_metrics.capture(capture_interval);
                reporter.report(&frame);
            }
        });

        // Start TUI on its own thread
        let mut app = App::with_metrics(tui_rx);
        app.metrics.activity_name = "main".to_string();
        app.metrics.driver_name = driver.to_string();
        app.metrics.threads = threads;
        app.metrics.total_target = cycles;
        app.metrics.rate_config = cycle_rate.map(|r| format!("{r}/s")).unwrap_or("unlimited".into());

        let tui_thread = std::thread::spawn(move || {
            if let Err(e) = app.run() {
                eprintln!("error: TUI failed: {e}");
            }
        });

        Some((tui_thread, capture_running))
    } else {
        None
    };

    // Determine the openmetrics push URL: explicit flag, or auto-discover
    // from a running `nbrs web` instance in this directory.
    let explicit_url: Option<String> = args.iter()
        .find_map(|a| a.strip_prefix("--report-openmetrics-to=")
            .or_else(|| a.strip_prefix("report-openmetrics-to=")))
        .map(|s| s.to_string());
    let push_url = explicit_url.or_else(|| {
        let url = daemon::discover_web_instance()?;
        eprintln!("nbrs: discovered local web instance, auto-pushing metrics");
        Some(url)
    });

    // If we have a push URL, spawn a metrics push thread.
    let openmetrics_push_flag = push_url.map(|url| {
        let mut reporter = web_push::OpenMetricsPushReporter::new(&url);
        let capture_metrics = shared_metrics.clone();
        let capture_interval = std::time::Duration::from_secs(1);
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = running.clone();
        eprintln!("nbrs: pushing openmetrics to {url}");
        std::thread::spawn(move || {
            use nb_metrics::scheduler::Reporter;
            while flag.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(capture_interval);
                let frame = capture_metrics.capture(capture_interval);
                reporter.report(&frame);
            }
        });
        running
    });

    let program = builder.program();

    // Handle --dry-run: override adapter with a no-op or printing adapter
    if let Some(dry_mode) = dry_run {
        match dry_mode {
            "emit" => {
                let adapter: Arc<dyn nb_activity::adapter::DriverAdapter> =
                    Arc::new(StdoutAdapter::with_config(StdoutConfig {
                        filename: "stdout".into(),
                        newline: true,
                        format: StdoutFormat::Statement,
                fields_filter: Vec::new(),
                    }));
                activity.run_with_driver(adapter, program).await;
            }
            "json" => {
                let adapter: Arc<dyn nb_activity::adapter::DriverAdapter> =
                    Arc::new(StdoutAdapter::with_config(StdoutConfig {
                        filename: "stdout".into(),
                        newline: true,
                        format: StdoutFormat::Json,
                fields_filter: Vec::new(),
                    }));
                activity.run_with_driver(adapter, program).await;
            }
            _ => {
                // Silent: assemble but don't execute
                use nb_activity::adapter::{DriverAdapter, OpDispenser, OpResult, ExecutionError, ResolvedFields};
                struct NoopDriverAdapter;
                impl DriverAdapter for NoopDriverAdapter {
                    fn name(&self) -> &str { "noop" }
                    fn map_op(&self, _template: &nb_workload::model::ParsedOp)
                        -> Result<Box<dyn OpDispenser>, String> {
                        struct NoopDispenser;
                        impl OpDispenser for NoopDispenser {
                            fn execute<'a>(&'a self, _cycle: u64, _fields: &'a ResolvedFields)
                                -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
                                Box::pin(async { Ok(OpResult { body: None, captures: std::collections::HashMap::new() }) })
                            }
                        }
                        Ok(Box::new(NoopDispenser))
                    }
                }
                activity.run_with_driver(Arc::new(NoopDriverAdapter), program).await;
            }
        }
        eprintln!("nbrs: dry-run complete");
    } else {

    match driver {
        "stdout" => {
            let adapter: Arc<dyn nb_activity::adapter::DriverAdapter> =
                Arc::new(StdoutAdapter::with_config(StdoutConfig {
                    filename,
                    newline: true,
                    format,
                    fields_filter: Vec::new(),
                }));
            activity.run_with_driver(adapter, program).await;
        }
        "model" => {
            use nb_adapter_model::{ModelAdapter, ModelConfig};
            let diagnose = args.iter().any(|a| a == "--diagnose");
            let adapter: Arc<dyn nb_activity::adapter::DriverAdapter> =
                Arc::new(ModelAdapter::with_config(ModelConfig {
                    stdout: StdoutConfig {
                        filename,
                        newline: true,
                        format,
                        fields_filter: Vec::new(),
                    },
                    diagnose,
                }));
            activity.run_with_driver(adapter, program).await;
        }
        "http" => {
            use nb_adapter_http::{HttpAdapter, HttpConfig};
            let base_url = params.get("base_url").or_else(|| params.get("host")).cloned();
            let timeout = params.get("timeout")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(30_000);
            let adapter: Arc<dyn nb_activity::adapter::DriverAdapter> =
                Arc::new(HttpAdapter::with_config(HttpConfig {
                    base_url,
                    timeout_ms: timeout,
                    follow_redirects: true,
                }));
            activity.run_with_driver(adapter, program).await;
        }
        other => {
            eprintln!("error: unknown driver '{other}' (supported: stdout, model, http)");
            std::process::exit(1);
        }
    };

    } // end of else block for dry-run check

    // Stop the openmetrics push thread if running.
    if let Some(running) = openmetrics_push_flag {
        running.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    if let Some((tui_thread, capture_running)) = tui_handle {
        // Stop the capture thread
        capture_running.store(false, std::sync::atomic::Ordering::Relaxed);
        // TUI will exit when user presses q
        eprintln!("nbrs: activity complete. Press q in TUI to exit.");
        let _ = tui_thread.join();
    } else {
        eprintln!("nbrs: done");
    }
}

/// Parse a cycle count that may have suffixes: K, M, B.
fn parse_count(s: &str) -> Option<u64> {
    let s = s.trim().to_uppercase();
    if let Some(n) = s.strip_suffix('K') {
        n.trim().parse::<u64>().ok().map(|v| v * 1_000)
    } else if let Some(n) = s.strip_suffix('M') {
        n.trim().parse::<u64>().ok().map(|v| v * 1_000_000)
    } else if let Some(n) = s.strip_suffix('B') {
        n.trim().parse::<u64>().ok().map(|v| v * 1_000_000_000)
    } else {
        s.parse().ok()
    }
}
