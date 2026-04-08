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
use nb_activity::bindings::compile_bindings_with_libs;
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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        print_usage();
        return;
    }

    // Handle describe command synchronously (no tokio needed)
    if args.first().map(|s| s.as_str()) == Some("describe") {
        describe_command(&args[1..]);
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
        // If no daemon is running, fall through and start fresh.
        if args.iter().any(|a| a == "--restart") {
            if let Some(anchor) = daemon::read_anchor() {
                if !anchor.args.is_empty() {
                    // Stop the old daemon (ignore errors — it may already be dead).
                    let _ = daemon::stop_daemon();
                    // Re-exec ourselves with the saved args.
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
                eprintln!("nbrs web: no running daemon found, starting fresh");
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

        let bind = args.iter()
            .find_map(|a| a.strip_prefix("bind=").or_else(|| a.strip_prefix("--bind=")))
            .unwrap_or("0.0.0.0");
        let port = args.iter()
            .find_map(|a| a.strip_prefix("port=").or_else(|| a.strip_prefix("--port=")))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(8080);
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

            let params_desc = if sig.const_params.is_empty() {
                String::new()
            } else {
                let p: Vec<String> = sig.const_params.iter()
                    .map(|(name, required)| {
                        if *required { name.to_string() } else { format!("[{name}]") }
                    })
                    .collect();
                format!("({})", p.join(", "))
            };

            let arity = if sig.outputs == 0 {
                format!("{}→N", sig.wire_inputs)
            } else {
                format!("{}→{}", sig.wire_inputs, sig.outputs)
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
            Err(_) => continue,
        };
        let ast = match parse(tokens) {
            Ok(a) => a,
            Err(_) => continue,
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
            Err(_) => continue,
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
            Err(_) => continue,
        };
        let ast = match parse(tokens) {
            Ok(a) => a,
            Err(_) => continue,
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
    let cycles: u64 = params.get("cycles").and_then(|s| parse_count(s)).unwrap_or(1);
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

    eprintln!("nbrs: {} ops selected, {} cycles, {} threads, adapter={}",
        ops.len(), cycles, threads, driver);

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

    // Compile bindings into GK kernel, with module resolution from the workload directory
    let workload_dir: Option<&std::path::Path> = workload_file.as_ref()
        .and_then(|p| std::path::Path::new(p).parent())
        .or_else(|| Some(std::path::Path::new(".")));
    let kernel = match compile_bindings_with_libs(&ops, workload_dir, gk_lib_paths, strict) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("error: failed to compile bindings: {e}");
            std::process::exit(1);
        }
    };

    // Build op sequence
    let op_sequence = OpSequence::from_ops(ops, seq_type);
    eprintln!("nbrs: stanza length={}, sequencer={:?}", op_sequence.stanza_length(), seq_type);

    // Create and run activity
    let config = ActivityConfig {
        name: "main".into(),
        cycles,
        concurrency: threads,
        cycle_rate,
        stanza_rate,
        sequencer: seq_type,
        error_spec,
        max_retries: 3,
    };

    let builder = Arc::new(OpBuilder::new(kernel));
    let activity = Activity::new(config, &Labels::of("session", "cli"), op_sequence);

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
            let _ = app.run();
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

    // Handle --dry-run: override adapter with a no-op or printing adapter
    if let Some(dry_mode) = dry_run {
        let b = builder.clone();
        match dry_mode {
            "emit" => {
                // Print each assembled op (like stdout adapter)
                let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
                    filename: "stdout".into(),
                    newline: true,
                    format: StdoutFormat::Statement,
                }));
                activity.run(
                    adapter,
                    Arc::new(move |cycle, template| b.build(cycle, template)),
                ).await;
            }
            "json" => {
                let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
                    filename: "stdout".into(),
                    newline: true,
                    format: StdoutFormat::Json,
                }));
                activity.run(
                    adapter,
                    Arc::new(move |cycle, template| b.build(cycle, template)),
                ).await;
            }
            _ => {
                // Silent: assemble but don't print
                use nb_activity::adapter::{Adapter, OpResult, AdapterError};
                struct NoopAdapter;
                impl Adapter for NoopAdapter {
                    fn execute(&self, _op: &nb_activity::adapter::AssembledOp)
                        -> impl std::future::Future<Output = Result<OpResult, AdapterError>> + Send {
                        async { Ok(OpResult { success: true, status: 0, body: None }) }
                    }
                }
                activity.run(
                    Arc::new(NoopAdapter),
                    Arc::new(move |cycle, template| b.build(cycle, template)),
                ).await;
            }
        }
        eprintln!("nbrs: dry-run complete");
    } else {

    match driver {
        "stdout" => {
            let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
                filename,
                newline: true,
                format,
            }));
            let b = builder.clone();
            activity.run(
                adapter,
                Arc::new(move |cycle, template| b.build(cycle, template)),
            ).await;
        }
        "model" => {
            use nb_adapter_model::{ModelAdapter, ModelConfig};
            let diagnose = args.iter().any(|a| a == "--diagnose");
            let adapter = Arc::new(ModelAdapter::with_config(ModelConfig {
                stdout: StdoutConfig {
                    filename,
                    newline: true,
                    format,
                },
                diagnose,
            }));
            let b = builder.clone();
            activity.run(
                adapter,
                Arc::new(move |cycle, template| b.build(cycle, template)),
            ).await;
        }
        "http" => {
            use nb_adapter_http::{HttpAdapter, HttpConfig};
            let base_url = params.get("base_url").or_else(|| params.get("host")).cloned();
            let timeout = params.get("timeout")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(30_000);
            let adapter = Arc::new(HttpAdapter::with_config(HttpConfig {
                base_url,
                timeout_ms: timeout,
                follow_redirects: true,
            }));
            let b = builder.clone();
            activity.run(
                adapter,
                Arc::new(move |cycle, template| b.build(cycle, template)),
            ).await;
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
