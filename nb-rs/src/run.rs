// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! The `run` subcommand: execute a workload against an adapter.

use std::collections::HashMap;
use std::sync::Arc;

use nb_activity::activity::{Activity, ActivityConfig};
use nb_activity::bindings::compile_bindings_with_libs_excluding;
use nb_activity::opseq::{OpSequence, SequencerType};
use nb_activity::synthesis::OpBuilder;
use nb_adapter_stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
use nb_metrics::labels::Labels;
use nb_tui::app::App;
use nb_tui::reporter::TuiReporter;
use nb_workload::parse::parse_workload;
use nb_workload::tags::TagFilter;

use crate::daemon;
use crate::web_push;

/// Parse `key=value` pairs from command line args.
pub fn parse_params(args: &[String]) -> HashMap<String, String> {
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

/// Parse a cycle count that may have suffixes: K, M, B.
pub fn parse_count(s: &str) -> Option<u64> {
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

pub async fn run_command(args: &[String]) {
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
