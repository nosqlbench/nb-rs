// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! opennbrs — OpenAPI persona for nb-rs.
//!
//! Generates workloads from OpenAPI/Swagger specifications. Reads an
//! OpenAPI 3.x spec, discovers endpoints, generates appropriate request
//! bodies using the GK variate system, and drives traffic against the
//! target API.
//!
//! Includes all core adapters (stdout, http, model) plus OpenAPI-aware
//! workload generation that understands schemas, path parameters,
//! request bodies, and response validation.
//!
//! Usage:
//!   opennbrs run spec=petstore.yaml base_url=http://localhost:8080 cycles=1000
//!   opennbrs describe spec=petstore.yaml
//!   opennbrs run spec=petstore.yaml adapter=stdout   # dry-run to console

mod spec;
mod workload;

use std::collections::HashMap;
use std::sync::Arc;

use nb_activity::activity::{Activity, ActivityConfig};
use nb_activity::adapter::DriverAdapter;
use nb_activity::bindings::compile_bindings;
use nb_activity::opseq::{OpSequence, SequencerType};
use nb_activity::synthesis::OpBuilder;
use nb_metrics::labels::Labels;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args.first().map(|s| s.as_str()) == Some("--help") {
        print_usage();
        return;
    }

    let command = args[0].as_str();
    let rest: Vec<String> = args[1..].to_vec();

    match command {
        "describe" => describe_command(&rest),
        "run" => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(run_command(&rest));
        }
        other => {
            // If the first arg looks like a param (key=value), treat as implicit "run"
            if other.contains('=') {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(run_command(&args));
            } else {
                eprintln!("error: unknown command '{other}'");
                eprintln!("  valid commands: run, describe");
                std::process::exit(1);
            }
        }
    }
}

fn parse_params(args: &[String]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for arg in args {
        if arg.starts_with("--") || arg.starts_with('-') { continue; }
        if let Some(eq_pos) = arg.find('=') {
            let key = arg[..eq_pos].to_string();
            let val = arg[eq_pos + 1..].to_string();
            params.insert(key, val);
        }
    }
    params
}

fn load_spec(params: &HashMap<String, String>) -> Result<(openapiv3::OpenAPI, Vec<spec::ApiOperation>), String> {
    let spec_path = params.get("spec")
        .ok_or("missing required parameter: spec=<file.yaml>")?;
    let source = std::fs::read_to_string(spec_path)
        .map_err(|e| format!("failed to read spec '{spec_path}': {e}"))?;
    spec::parse_spec(&source)
}

fn describe_command(args: &[String]) {
    let params = parse_params(args);
    let (api, ops) = match load_spec(&params) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    let title = api.info.title.as_str();
    let version = &api.info.version;
    println!("OpenAPI: {title} v{version}");
    println!("Operations ({}):", ops.len());
    spec::describe_operations(&ops);

    // If operations have tags, show tag summary
    let mut tag_counts: HashMap<&str, usize> = HashMap::new();
    for op in &ops {
        for tag in &op.tags {
            *tag_counts.entry(tag.as_str()).or_insert(0) += 1;
        }
    }
    if !tag_counts.is_empty() {
        println!("\nTags:");
        for (tag, count) in &tag_counts {
            println!("  {tag}: {count} operations");
        }
    }
}

async fn run_command(args: &[String]) {
    let params = parse_params(args);

    let (_api, api_ops) = match load_spec(&params) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    if api_ops.is_empty() {
        eprintln!("error: no operations found in spec");
        std::process::exit(1);
    }

    // Filter operations if requested
    let ops_to_run: Vec<spec::ApiOperation> = if let Some(filter) = params.get("operations") {
        let ids: Vec<&str> = filter.split(',').map(|s| s.trim()).collect();
        api_ops.into_iter()
            .filter(|op| ids.contains(&op.operation_id.as_str()))
            .collect()
    } else {
        api_ops
    };

    if ops_to_run.is_empty() {
        eprintln!("error: no operations match the filter");
        std::process::exit(1);
    }

    let base_url = params.get("base_url")
        .or_else(|| params.get("host"))
        .cloned()
        .unwrap_or_else(|| "http://localhost:8080".into());

    // Generate ParsedOps and GK bindings from API operations
    let (parsed_ops, bindings_source) = workload::generate_ops(&ops_to_run, &base_url);

    eprintln!("opennbrs: {} operations, base_url={}", parsed_ops.len(), base_url);
    for op in &parsed_ops {
        eprintln!("  {} {} {}", op.op.get("method").and_then(|v| v.as_str()).unwrap_or("?"),
            op.op.get("uri").and_then(|v| v.as_str()).unwrap_or("?"), op.name);
    }

    if !bindings_source.is_empty() {
        eprintln!("opennbrs: GK bindings:\n{bindings_source}");
    }

    // Compile GK bindings
    let kernel = match compile_bindings(&parsed_ops) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("error: failed to compile bindings: {e}");
            std::process::exit(1);
        }
    };

    // Build op sequence and activity
    let explicit_cycles: Option<u64> = params.get("cycles").and_then(|s| s.parse().ok());
    let threads: usize = params.get("threads").and_then(|s| s.parse().ok()).unwrap_or(1);
    let seq_type = params.get("seq")
        .map(|s| SequencerType::parse(s).unwrap_or(SequencerType::Bucket))
        .unwrap_or(SequencerType::Bucket);

    let op_sequence = OpSequence::from_ops(parsed_ops, seq_type);
    let cycles = explicit_cycles.unwrap_or(op_sequence.stanza_length() as u64);

    let config = ActivityConfig {
        name: "openapi".into(),
        cycles,
        concurrency: threads,
        cycle_rate: params.get("rate").and_then(|s| s.parse().ok()),
        stanza_rate: None,
        sequencer: seq_type,
        error_spec: params.get("errors").cloned().unwrap_or_default(),
        max_retries: 3,
        stanza_concurrency: params.get("stanza_concurrency")
            .and_then(|s| s.parse().ok()).unwrap_or(1),
    };

    let builder = Arc::new(OpBuilder::new(kernel));
    let program = builder.program();
    let activity = Activity::new(config, &Labels::of("session", "opennbrs"), op_sequence);

    eprintln!("opennbrs: {cycles} cycles, {threads} threads");

    // Dispatch adapter
    let driver = params.get("adapter").or(params.get("driver"))
        .map(|s| s.as_str())
        .unwrap_or("http");

    let adapter: Arc<dyn DriverAdapter> = match driver {
        "http" => {
            use nb_adapter_http::{HttpAdapter, HttpConfig};
            Arc::new(HttpAdapter::with_config(HttpConfig {
                base_url: None, // base_url is already in the URI templates
                timeout_ms: params.get("timeout")
                    .and_then(|s| s.parse().ok()).unwrap_or(30_000),
                follow_redirects: true,
            }))
        }
        "stdout" => {
            use nb_adapter_stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
            let format = match params.get("format").map(|s| s.as_str()) {
                Some("json") => StdoutFormat::Json,
                Some("csv") => StdoutFormat::Csv,
                _ => StdoutFormat::Assignments,
            };
            Arc::new(StdoutAdapter::with_config(StdoutConfig {
                filename: params.get("filename").cloned().unwrap_or("stdout".into()),
                format,
                ..Default::default()
            }))
        }
        "model" => {
            use nb_adapter_model::{ModelAdapter, ModelConfig};
            use nb_adapter_stdout::{StdoutConfig, StdoutFormat};
            Arc::new(ModelAdapter::with_config(ModelConfig {
                stdout: StdoutConfig {
                    format: StdoutFormat::Assignments,
                    ..Default::default()
                },
                diagnose: args.iter().any(|a| a == "--diagnose"),
            }))
        }
        other => {
            eprintln!("error: unknown adapter '{other}' (available: http, stdout, model)");
            std::process::exit(1);
        }
    };

    activity.run_with_driver(adapter, program).await;
    eprintln!("opennbrs: done.");
}

fn print_usage() {
    eprintln!("opennbrs — API workload testing from OpenAPI specifications");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  opennbrs run spec=<openapi.yaml> base_url=<url> cycles=<n>");
    eprintln!("  opennbrs run spec=<openapi.yaml> adapter=stdout   # dry-run");
    eprintln!("  opennbrs describe spec=<openapi.yaml>");
    eprintln!();
    eprintln!("Parameters:");
    eprintln!("  spec=<file>          OpenAPI 3.x specification (YAML or JSON)");
    eprintln!("  base_url=<url>       Target API base URL (default: http://localhost:8080)");
    eprintln!("  adapter=<name>       http (default), stdout, model");
    eprintln!("  operations=<list>    Comma-separated operation IDs to test (default: all)");
    eprintln!("  cycles=<n>           Number of cycles (default: one stanza)");
    eprintln!("  threads=<n>          Concurrency level (default: 1)");
    eprintln!("  rate=<n>             Target cycle rate per second");
    eprintln!("  format=<type>        Output format for stdout adapter: assignments|json|csv");
}
