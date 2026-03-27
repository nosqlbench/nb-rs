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
use nb_activity::bindings::compile_bindings;
use nb_activity::opseq::{OpSequence, SequencerType};
use nb_activity::synthesis::OpBuilder;
use nb_metrics::labels::Labels;
use nb_tui::app::App;
use nb_tui::reporter::TuiReporter;
use nb_workload::parse::parse_workload;
use nb_workload::tags::TagFilter;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        print_usage();
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
    eprintln!("Usage:");
    eprintln!("  nbrs run driver=stdout workload=file.yaml cycles=100 threads=4");
    eprintln!("  nbrs run workload=file.yaml tags=block:main rate=1000 format=json");
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

    // Compile bindings into GK kernel
    let kernel = match compile_bindings(&ops) {
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

    // If TUI mode, spawn TUI on a separate thread
    let tui_handle = if use_tui {
        let (_tui_reporter, tui_rx) = TuiReporter::channel();

        // Set up the metrics scheduler with TUI as a consumer
        
        

        // We'll manually feed frames to the TUI reporter from the
        // activity metrics. For now, start the TUI thread and feed
        // it activity info.
        let mut app = App::with_metrics(tui_rx);
        app.metrics.activity_name = "main".to_string();
        app.metrics.driver_name = driver.to_string();
        app.metrics.threads = threads;
        app.metrics.total_target = cycles;
        app.metrics.rate_config = cycle_rate.map(|r| format!("{r}/s")).unwrap_or("unlimited".into());

        let tui_thread = std::thread::spawn(move || {
            let _ = app.run();
        });

        // TODO: wire tui_reporter into the metrics scheduler so it
        // receives live frames. For now the TUI shows static info
        // until the scheduler integration is complete.

        Some(tui_thread)
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

    if let Some(handle) = tui_handle {
        // TUI will exit when user presses q — don't wait forever
        eprintln!("nbrs: activity complete. Press q in TUI to exit.");
        let _ = handle.join();
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
