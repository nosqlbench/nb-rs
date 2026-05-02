// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! OpenAPI subcommand glue (`describe` / `run` from spec).
//!
//! Compiled only when the `openapi` Cargo feature is enabled,
//! which pulls in [`nbrs_adapter_openapi`] for spec parsing and
//! workload synthesis. This module owns:
//!
//! - `nbrs describe-openapi spec=<file>` — spec inspection
//!   (operations, tag summary).
//! - `nbrs run-openapi spec=<file> [base_url=...] [adapter=...]
//!   [...]` — synthesizes ops + GK bindings from the spec and
//!   runs them against the chosen adapter (default: `http`).
//!
//! Pairs with [`nbrs_adapter_openapi`], which carries the spec
//! parser + workload synthesis. nbrs is the single user-facing
//! CLI; opting in via `--features openapi` adds these
//! subcommands without affecting the default build.

use std::collections::HashMap;
use std::sync::Arc;

use nbrs_activity::activity::{Activity, ActivityConfig};
use nbrs_activity::adapter::DriverAdapter;
use nbrs_activity::bindings::compile_bindings;
use nbrs_activity::opseq::{OpSequence, SequencerType};
use nbrs_activity::synthesis::OpBuilder;
use nbrs_adapter_openapi::{describe_operations, generate_ops, parse_spec, ApiOperation};
use nbrs_metrics::labels::Labels;

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

fn load_spec(
    params: &HashMap<String, String>,
) -> Result<(openapiv3::OpenAPI, Vec<ApiOperation>), String> {
    let spec_path = params.get("spec")
        .ok_or("missing required parameter: spec=<file.yaml>")?;
    let source = std::fs::read_to_string(spec_path)
        .map_err(|e| format!("failed to read spec '{spec_path}': {e}"))?;
    parse_spec(&source)
}

/// `nbrs describe-openapi spec=<file>` — inspect operations and
/// tag summary without running anything.
pub fn describe_command(args: &[String]) {
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
    describe_operations(&ops);

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

/// `nbrs run-openapi spec=<file> ...` — synthesize ops from the
/// spec and run them through the chosen adapter.
pub async fn run_command(args: &[String]) {
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

    let ops_to_run: Vec<ApiOperation> = if let Some(filter) = params.get("operations") {
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

    let (parsed_ops, bindings_source) = generate_ops(&ops_to_run, &base_url);

    eprintln!("openapi: {} operations, base_url={}", parsed_ops.len(), base_url);
    for op in &parsed_ops {
        eprintln!("  {} {} {}",
            op.op.get("method").and_then(|v| v.as_str()).unwrap_or("?"),
            op.op.get("uri").and_then(|v| v.as_str()).unwrap_or("?"),
            op.name);
    }

    if !bindings_source.is_empty() {
        eprintln!("openapi: GK bindings:\n{bindings_source}");
    }

    let kernel = match compile_bindings(&parsed_ops) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("error: failed to compile bindings: {e}");
            std::process::exit(1);
        }
    };

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
        rate: params.get("rate").and_then(|s| s.parse().ok()),
        sequencer: seq_type,
        error_spec: params.get("errors").cloned().unwrap_or_default(),
        max_retries: 3,
        stanza_concurrency: params.get("stanza_concurrency")
            .and_then(|s| s.parse().ok()).unwrap_or(1),
        source_factory: None,
        suppress_status_line: false,
    };

    let builder = Arc::new(OpBuilder::new(kernel));
    let program = builder.program();
    let activity = Activity::new(config, &Labels::of("session", "openapi"), op_sequence);

    eprintln!("openapi: {cycles} cycles, {threads} threads");

    let driver = params.get("adapter").or(params.get("driver"))
        .map(|s| s.as_str())
        .unwrap_or("http");

    let adapter: Arc<dyn DriverAdapter> = match driver {
        "http" => {
            use nbrs_adapter_http::{HttpAdapter, HttpConfig};
            Arc::new(HttpAdapter::with_config(HttpConfig {
                base_url: None, // base_url already in URI templates
                timeout_ms: params.get("timeout")
                    .and_then(|s| s.parse().ok()).unwrap_or(30_000),
                follow_redirects: true,
            }))
        }
        "stdout" => {
            use nbrs_adapter_stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
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
        "testkit" => {
            use nbrs_adapter_testkit::{ModelAdapter, ModelConfig};
            use nbrs_adapter_stdout::{StdoutConfig, StdoutFormat};
            Arc::new(ModelAdapter::with_config(ModelConfig {
                stdout: StdoutConfig {
                    format: StdoutFormat::Assignments,
                    ..Default::default()
                },
                diagnose: args.iter().any(|a| a == "--diagnose"),
            }))
        }
        other => {
            eprintln!("error: unknown adapter '{other}' (available: http, stdout, testkit)");
            std::process::exit(1);
        }
    };

    activity.run_with_driver(adapter, program).await;
    eprintln!("openapi: done.");
}
