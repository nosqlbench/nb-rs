// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! nbrs — the nb-rs command-line tool.
//!
//! Usage:
//!   nbrs run driver=stdout workload=file.yaml cycles=100 threads=4
//!   nbrs run workload=file.yaml tags=block:main rate=1000
//!   nbrs file.yaml scenario_name [param=value ...]

use std::collections::HashMap;
use std::sync::Arc;

use nb_activity::activity::{Activity, ActivityConfig};
use nb_activity::adapters::stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
use nb_activity::bindings::compile_bindings_with_path;
use nb_activity::opseq::{OpSequence, SequencerType};
use nb_activity::synthesis::OpBuilder;
use nb_metrics::labels::Labels;
use nb_tui::app::App;
use nb_tui::reporter::TuiReporter;
use nb_variates::dsl::registry;
use nb_workload::parse::parse_workload;
use nb_workload::tags::TagFilter;

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

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        run_command(&args).await;
    });
}

fn print_usage() {
    eprintln!("nbrs — nosqlbench for Rust");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  nbrs run driver=stdout workload=file.yaml cycles=100 threads=4");
    eprintln!("  nbrs run workload=file.yaml tags=block:main rate=1000 format=json");
    eprintln!("  nbrs describe gk functions    List all GK node functions");
    eprintln!();
    eprintln!("Parameters:");
    eprintln!("  workload=<file.yaml>   Workload definition file");
    eprintln!("  driver=<name>          Adapter type (default: stdout)");
    eprintln!("  cycles=<n>             Number of cycles to execute");
    eprintln!("  threads=<n>            Concurrency level (default: 1)");
    eprintln!("  rate=<n>               Per-cycle rate limit (ops/sec)");
    eprintln!("  stanzarate=<n>         Per-stanza rate limit (stanzas/sec)");
    eprintln!("  tags=<filter>          Tag filter for op selection");
    eprintln!("  seq=<type>             Sequencer: bucket|interval|concat");
    eprintln!("  format=<type>          Output format: assignments|json|csv|stmt");
    eprintln!("  errors=<spec>          Error handler spec");
    eprintln!("  filename=<path>        Output file (default: stdout)");
}

fn describe_command(args: &[String]) {
    let topic = args.first().map(|s| s.as_str()).unwrap_or("");
    let subtopic = args.get(1).map(|s| s.as_str()).unwrap_or("");

    match (topic, subtopic) {
        ("gk", "functions") => describe_gk_functions(),
        ("gk", _) => {
            eprintln!("nbrs describe gk <subtopic>");
            eprintln!("  functions    List all GK node functions");
        }
        _ => {
            eprintln!("nbrs describe <topic>");
            eprintln!("  gk           Generation kernel topics");
        }
    }
}

fn describe_gk_functions() {
    use nb_activity::bindings::probe_compile_level;

    let funcs = registry::registry();
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    // ANSI color codes
    let (bold, dim, reset, green, cyan, magenta) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[35m")
    } else {
        ("", "", "", "", "", "")
    };

    // Group functions by category
    let categories = [
        ("Hashing", &["hash"][..]),
        ("Arithmetic", &["add", "mul", "div", "mod", "clamp", "interleave", "mixed_radix", "identity"]),
        ("Conversions", &["unit_interval", "clamp_f64", "f64_to_u64", "round_to_u64", "floor_to_u64",
            "ceil_to_u64", "discretize", "format_u64", "format_f64", "zero_pad_u64"]),
        ("Distributions", &["dist_normal", "dist_exponential", "dist_uniform", "dist_pareto",
            "dist_zipf", "lut_sample", "icd_normal", "icd_exponential"]),
        ("Datetime", &["epoch_scale", "epoch_offset", "to_timestamp", "date_components"]),
        ("Encoding", &["html_encode", "html_decode", "url_encode", "url_decode",
            "to_hex", "from_hex", "to_base64", "from_base64", "escape_json"]),
        ("Interpolation", &["lerp", "scale_range", "quantize"]),
        ("Weighted", &["weighted_strings", "weighted_u64"]),
        ("String", &["combinations", "number_to_words"]),
        ("JSON", &["to_json", "json_to_str", "json_merge"]),
        ("Byte Buffers", &["u64_to_bytes", "bytes_from_hash"]),
        ("Digest", &["sha256", "md5"]),
        ("Noise", &["perlin_1d", "perlin_2d", "simplex_2d"]),
        ("Regex", &["regex_replace", "regex_match"]),
        ("Shuffle", &["shuffle"]),
        ("Real Data", &["first_names", "full_names", "state_codes", "country_names"]),
        ("Context", &["current_epoch_millis", "counter", "session_start_millis"]),
        ("Diagnostic", &["type_of", "debug_repr", "inspect"]),
    ];

    println!();
    println!("{bold}GK Node Functions{reset}");
    println!("{bold}═════════════════{reset}");
    println!();

    for (cat_name, cat_funcs) in &categories {
        println!("  {bold}{cyan}── {cat_name} ──{reset}");
        println!();

        for &func_name in *cat_funcs {
            if let Some(sig) = funcs.iter().find(|s| s.name == func_name) {
                let level = probe_compile_level(sig.name);
                // Build "P✓✗✗" column: P always bright, checkmark or x per level
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

                // Pad to visual width, then apply color
                let name_padded = format!("{:<24}", sig.name);
                let params_padded = format!("{:<24}", params_desc);
                let arity_padded = format!("{:<5}", arity);

                print!("  {bold}{magenta}{name_padded}{reset}");
                print!(" {dim}{params_padded}{reset}");
                print!(" {arity_padded}");
                print!("  {level_col}");
                println!("  {dim}{}{reset}", sig.description);
            }
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

    // Load workload
    let workload_path = params.get("workload")
        .or_else(|| {
            // Look for a .yaml file in the args
            args.iter().find(|a| a.ends_with(".yaml") || a.ends_with(".yml"))
        });

    let workload_path = match workload_path {
        Some(p) => p.clone(),
        None => {
            eprintln!("error: no workload file specified");
            eprintln!("  use: nbrs run workload=file.yaml ...");
            std::process::exit(1);
        }
    };

    let yaml_source = match std::fs::read_to_string(&workload_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to read workload file '{}': {}", workload_path, e);
            std::process::exit(1);
        }
    };

    // Parse workload with user-provided template params
    let workload = match parse_workload(&yaml_source, &params) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("error: failed to parse workload: {e}");
            std::process::exit(1);
        }
    };

    // Extract activity parameters
    let driver = params.get("driver").map(|s| s.as_str()).unwrap_or("stdout");
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

    eprintln!("nbrs: {} ops selected, {} cycles, {} threads, driver={}",
        ops.len(), cycles, threads, driver);

    // Compile bindings into GK kernel, with module resolution from the workload directory
    let workload_dir = std::path::Path::new(&workload_path).parent();
    let kernel = match compile_bindings_with_path(&ops, workload_dir) {
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
        other => {
            eprintln!("error: unknown driver '{other}' (only 'stdout' supported currently)");
            std::process::exit(1);
        }
    };

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
