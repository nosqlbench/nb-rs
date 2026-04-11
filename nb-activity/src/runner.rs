// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared run pipeline for persona binaries.
//!
//! Encapsulates workload parsing → GK compilation → activity
//! construction. Each persona binary provides a DriverAdapter and
//! calls `run_with_driver(adapter)`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::activity::{Activity, ActivityConfig};
use crate::bindings::compile_bindings_excluding;
use crate::opseq::{OpSequence, SequencerType};
use crate::synthesis::OpBuilder;
use nb_metrics::labels::Labels;

/// A prepared activity ready to be run with a DriverAdapter.
/// The persona binary matches on the driver name and calls `run_with_driver()`.
pub struct PreparedRun {
    pub driver: String,
    pub params: HashMap<String, String>,
    pub activity: Activity,
    pub builder: Arc<OpBuilder>,
}

impl PreparedRun {
    /// Run with the tiered DriverAdapter interface (SRD 38).
    ///
    /// The adapter maps templates to dispensers at init time.
    /// Per-fiber GK state is created from the shared program.
    pub async fn run_with_driver(self, adapter: Arc<dyn crate::adapter::DriverAdapter>) {
        let program = self.builder.program();
        self.activity.run_with_driver(adapter, program).await;
    }

    /// Access the shared GK program.
    pub fn program(&self) -> Arc<nb_variates::kernel::GkProgram> {
        self.builder.program()
    }
}

/// Parse workload, compile bindings, and prepare an activity for execution.
/// Returns a `PreparedRun` that the persona binary dispatches to the
/// appropriate adapter.
pub fn prepare(args: &[String]) -> Result<PreparedRun, String> {
    let params = parse_params(args);

    // Load workload
    let workload = if let Some(op_str) = params.get("op") {
        nb_workload::inline::synthesize_inline_workload(op_str)
            .map_err(|e| format!("inline workload: {e}"))?
    } else {
        let workload_path = params.get("workload")
            .cloned()
            .or_else(|| args.iter()
                .find(|a| a.ends_with(".yaml") || a.ends_with(".yml"))
                .cloned()
            )
            .ok_or("no workload specified. Use workload=file.yaml or op=\"...\"")?;

        {
            let yaml_source = std::fs::read_to_string(&workload_path)
                .map_err(|e| format!("read workload '{workload_path}': {e}"))?;
            nb_workload::parse::parse_workload(&yaml_source, &params)
                .map_err(|e| format!("parse workload: {e}"))?
        }
    };

    // Extract params
    let driver = params.get("adapter").or(params.get("driver"))
        .cloned().unwrap_or_else(|| "stdout".into());
    let explicit_cycles: Option<u64> = match params.get("cycles") {
        Some(s) => Some(s.parse().map_err(|e| format!("invalid cycles='{s}': {e}"))?),
        None => None,
    };
    let seq_type = match params.get("seq") {
        Some(s) => SequencerType::parse(s)
            .map_err(|e| format!("invalid seq='{s}': {e}"))?,
        None => SequencerType::Bucket,
    };
    let error_spec = params.get("errors").cloned().unwrap_or_default();

    // Extract workload params before consuming ops
    let workload_params = workload.params;

    // Filter ops by tags
    let mut ops: Vec<_> = if let Some(filter) = params.get("tags") {
        let tf = nb_workload::tags::TagFilter::parse(filter)
            .map_err(|e| format!("bad tag filter: {e}"))?;
        workload.ops.into_iter().filter(|op| tf.matches(&op.tags)).collect()
    } else {
        workload.ops
    };

    if ops.is_empty() {
        return Err("no ops selected (tag filter may have excluded all ops)".into());
    }

    let num_ops = ops.len();

    // Expand workload params in GK binding source strings before compilation.
    // This resolves {param} references in const arguments like dataset names
    // that node constructors need at compile time (not cycle time).
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

    // Compile GK bindings.
    // Workload params are excluded from the "undeclared bind point"
    // check — they resolve at cycle time via the synthesis pipeline.
    let param_names: Vec<String> = workload_params.keys().cloned().collect();
    let kernel = compile_bindings_excluding(&ops, &param_names)
        .map_err(|e| format!("compile bindings: {e}"))?;

    // Build op sequence
    let op_sequence = OpSequence::from_ops(ops, seq_type);

    // Resolve cycles: CLI > workload param > one stanza
    let cycles = if let Some(c) = explicit_cycles {
        c
    } else {
        resolve_param(&params, &workload_params, "cycles")
            .and_then(|s| s.parse().ok())
            .unwrap_or(op_sequence.stanza_length() as u64)
    };

    // Resolve activity settings: CLI params override workload params
    let concurrency: usize = resolve_param(&params, &workload_params, "concurrency")
        .map(|s| s.parse().map_err(|e| format!("invalid concurrency='{s}': {e}")))
        .transpose()?
        .unwrap_or(1);
    let stanza_concurrency: usize = resolve_param(&params, &workload_params, "stanza_concurrency")
        .or_else(|| resolve_param(&params, &workload_params, "sc"))
        .map(|s| s.parse().map_err(|e| format!("invalid stanza_concurrency='{s}': {e}")))
        .transpose()?
        .unwrap_or(1);
    let cycle_rate: Option<f64> = resolve_param(&params, &workload_params, "rate")
        .map(|s| s.parse().map_err(|e| format!("invalid rate='{s}': {e}")))
        .transpose()?;
    let stanza_rate: Option<f64> = resolve_param(&params, &workload_params, "stanzarate")
        .map(|s| s.parse().map_err(|e| format!("invalid stanzarate='{s}': {e}")))
        .transpose()?;

    eprintln!("{num_ops} ops, {cycles} cycles, concurrency={concurrency}, adapter={driver}");

    let config = ActivityConfig {
        name: "main".into(),
        cycles,
        concurrency,
        cycle_rate,
        stanza_rate,
        sequencer: seq_type,
        error_spec,
        max_retries: 3,
        stanza_concurrency,
    };

    // Validate: warn on unrecognized parameters
    let known_params = [
        "adapter", "driver", "workload", "op", "cycles", "concurrency",
        "rate", "stanzarate", "errors", "seq", "tags", "format",
        "filename", "stanza_concurrency", "sc",
        // CQL adapter params
        "hosts", "host", "port", "keyspace", "consistency",
        "username", "password", "request_timeout_ms",
        // HTTP adapter params
        "base_url", "timeout",
        // OpenAPI params
        "spec", "operations",
    ];
    let unknown: Vec<&String> = params.keys()
        .filter(|key| !known_params.contains(&key.as_str()) && !workload_params.contains_key(*key))
        .collect();
    if !unknown.is_empty() {
        return Err(format!("unrecognized parameter(s): {}. Check for typos.",
            unknown.iter().map(|k| format!("'{k}'")).collect::<Vec<_>>().join(", ")));
    }

    let builder = Arc::new(OpBuilder::new(kernel));
    let activity = Activity::with_params(config, &Labels::of("session", "cli"), op_sequence, workload_params.clone());

    Ok(PreparedRun { driver, params, activity, builder })
}

/// Resolve a parameter: CLI params override workload params.
fn resolve_param<'a>(
    cli: &'a HashMap<String, String>,
    workload: &'a HashMap<String, String>,
    key: &str,
) -> Option<String> {
    cli.get(key).or_else(|| workload.get(key)).cloned()
}

/// Parse `key=value` pairs from command line args.
pub fn parse_params(args: &[String]) -> HashMap<String, String> {
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
