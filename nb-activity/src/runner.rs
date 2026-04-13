// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Shared run pipeline for persona binaries.
//!
//! Encapsulates workload parsing → GK compilation → activity
//! construction → execution (single or phased).
//!
//! Each persona binary links its adapter crates (which register
//! themselves via `inventory::submit!`) and calls [`run()`].
//! The persona adds nothing but adapters and node functions —
//! all orchestration logic lives here.

use std::collections::HashMap;
use std::sync::Arc;

use crate::activity::{Activity, ActivityConfig};
use crate::adapter::{find_adapter_registration, registered_adapter_params, registered_driver_names};
use crate::bindings::compile_bindings_with_libs_excluding;
use crate::opseq::{OpSequence, SequencerType};
use crate::synthesis::OpBuilder;
use nb_metrics::labels::Labels;
use nb_workload::tags::TagFilter;

/// Known `key=value` params accepted by the shared runner.
/// Adapter-specific params are discovered from inventory registrations.
const KNOWN_PARAMS: &[&str] = &[
    // Activity-level
    "adapter", "driver", "workload", "op", "cycles", "threads",
    "rate", "stanzarate", "errors", "seq", "tags", "format",
    "filename", "separator", "header", "color",
    "stanza_concurrency", "sc", "scenario",
];

/// Run a workload. Adapters are discovered from link-time inventory
/// registrations — the calling binary just needs to link the adapter
/// crates it wants available.
pub async fn run(args: &[String]) -> Result<(), String> {
    // Skip "run" if present
    let args: &[String] = if args.first().map(|s| s.as_str()) == Some("run") {
        &args[1..]
    } else {
        args
    };

    // Detect scenario shorthand: `workload.yaml <scenario_name>` → `scenario=<name>`
    let args = normalize_args(args);
    let params = parse_params(&args);

    // Load workload — from inline op= or YAML file.
    let mut workload_file: Option<String> = None;
    let workload = if let Some(op_str) = params.get("op") {
        if params.contains_key("workload") {
            eprintln!("warning: op= overrides workload=");
        }
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

        workload_file = Some(workload_path.clone());

        let yaml_source = std::fs::read_to_string(&workload_path)
            .map_err(|e| format!("read workload '{workload_path}': {e}"))?;
        nb_workload::parse::parse_workload(&yaml_source, &params)
            .map_err(|e| format!("parse workload: {e}"))?
    };

    // Merge CLI params over workload params. CLI takes priority.
    let mut merged_params = workload.params.clone();
    for (k, v) in &params {
        merged_params.insert(k.clone(), v.clone());
    }

    // Extract core config
    let driver = merged_params.get("adapter")
        .or_else(|| merged_params.get("driver"))
        .cloned()
        .unwrap_or_else(|| "stdout".into());
    let explicit_cycles: Option<u64> = merged_params.get("cycles").and_then(|s| parse_count(s));
    let threads: usize = merged_params.get("threads").and_then(|s| s.parse().ok()).unwrap_or(1);
    let cycle_rate: Option<f64> = merged_params.get("rate").and_then(|s| s.parse().ok());
    let stanza_rate: Option<f64> = merged_params.get("stanzarate").and_then(|s| s.parse().ok());
    let tag_filter = merged_params.get("tags").cloned();
    let seq_type = merged_params.get("seq")
        .map(|s| SequencerType::parse(s).unwrap_or(SequencerType::Bucket))
        .unwrap_or(SequencerType::Bucket);
    let error_spec = merged_params.get("errors")
        .cloned()
        .unwrap_or_else(|| ".*:warn,counter".to_string());

    // Validate CLI parameters (runner-known + adapter-registered + workload-declared)
    {
        let adapter_params = registered_adapter_params();
        let all_known: Vec<&str> = KNOWN_PARAMS.iter().copied()
            .chain(adapter_params.iter().copied())
            .chain(workload.declared_params.iter().map(|s| s.as_str()))
            .collect();
        for key in params.keys() {
            if !all_known.contains(&key.as_str()) && !workload.params.contains_key(key) {
                let suggestion = closest_match(key, &all_known);
                if let Some(closest) = suggestion {
                    return Err(format!("unrecognized parameter '{key}='. Did you mean '{closest}='?"));
                } else {
                    return Err(format!("unrecognized parameter '{key}='"));
                }
            }
        }
    }

    // Extract workload structure before consuming
    let workload_params = workload.params;
    let phases = workload.phases;
    let phase_order = workload.phase_order;
    let scenarios = workload.scenarios;

    // Collect ALL ops: top-level ops + all phase inline ops.
    let mut ops = workload.ops;

    // Filter top-level ops by tags (CLI-level tag filter)
    if let Some(ref filter) = tag_filter {
        ops = TagFilter::filter_ops(&ops, filter)
            .map_err(|e| format!("invalid tag filter: {e}"))?;
    }

    // Separate phase ops: non-for_each go into outer kernel, for_each saved raw
    let mut phase_ops_for_compile: Vec<nb_workload::model::ParsedOp> = Vec::new();
    let mut for_each_raw_ops: HashMap<String, Vec<nb_workload::model::ParsedOp>> = HashMap::new();
    for (name, phase) in &phases {
        if phase.for_each.is_some() {
            for_each_raw_ops.insert(name.clone(), phase.ops.clone());
        } else {
            phase_ops_for_compile.extend(phase.ops.iter().cloned());
        }
    }

    // For non-phased workloads, require at least some ops
    if ops.is_empty() && phases.is_empty() {
        return Err("no ops selected (tag filter may have excluded all ops)".into());
    }

    if phases.is_empty() {
        eprintln!("{} ops, {} cycles, {} threads, adapter={}",
            ops.len(),
            explicit_cycles.map(|c| c.to_string()).unwrap_or("auto".into()),
            threads,
            driver);
    } else {
        eprintln!("{} phases, {} top-level ops, adapter={}",
            phases.len(), ops.len(), driver);
    }

    // Collect --gk-lib=path flags
    let gk_lib_paths: Vec<std::path::PathBuf> = args.iter()
        .filter_map(|a| a.strip_prefix("--gk-lib="))
        .map(std::path::PathBuf::from)
        .collect();
    let strict = args.iter().any(|a| a == "--strict");

    // Dry-run mode: override adapter with stdout or noop
    let dry_run = args.iter().find_map(|a| {
        if a == "--dry-run" { Some("silent") }
        else if let Some(mode) = a.strip_prefix("--dry-run=") { Some(mode) }
        else { None }
    });

    // OpenMetrics push URL
    let openmetrics_url: Option<String> = args.iter()
        .find_map(|a| a.strip_prefix("--report-openmetrics-to=")
            .or_else(|| a.strip_prefix("report-openmetrics-to=")))
        .map(|s| s.to_string());

    // Merge all ops for param expansion and GK compilation.
    let num_top_level_ops = ops.len();
    let mut all_ops_for_compile: Vec<nb_workload::model::ParsedOp> = ops;
    all_ops_for_compile.extend(phase_ops_for_compile);

    // === GK Expansion Pipeline (outer kernel ops only) ===
    expand_gk_bindings(&mut all_ops_for_compile, &workload_params, &phases);

    // === GK Compilation ===

    let workload_dir: Option<&std::path::Path> = workload_file.as_ref()
        .and_then(|p| std::path::Path::new(p).parent())
        .or_else(|| Some(std::path::Path::new(".")));

    let mut config_refs: Vec<String> = params.values()
        .filter(|v| v.starts_with('{') && v.ends_with('}'))
        .map(|v| {
            let mut inner = v[1..v.len()-1].to_string();
            // Expand workload params in config expressions
            for (key, value) in &workload_params {
                let placeholder = format!("{{{key}}}");
                if inner.contains(&placeholder) {
                    inner = inner.replace(&placeholder, value);
                }
            }
            inner
        })
        .collect();
    for (name, phase) in &phases {
        if phase.for_each.is_some() {
            continue; // for_each phase cycles resolved per-iteration
        }
        if let Some(ref c) = phase.cycles {
            if c.starts_with('{') && c.ends_with('}') {
                let mut inner = c[1..c.len()-1].to_string();
                for (key, value) in &workload_params {
                    let placeholder = format!("{{{key}}}");
                    if inner.contains(&placeholder) {
                        inner = inner.replace(&placeholder, value);
                    }
                }
                config_refs.push(inner);
            }
        }
        let _ = name; // suppress unused warning
    }

    let kernel = compile_bindings_with_libs_excluding(
        &all_ops_for_compile, workload_dir, gk_lib_paths.clone(), strict, &[], &config_refs,
    ).map_err(|e| format!("compile bindings: {e}"))?;

    // === GK Config Resolution (all done before kernel is consumed) ===

    let resolved_cli_cycles: Option<u64> = explicit_cycles.or_else(||
        params.get("cycles").and_then(|s| resolve_gk_config(s, &kernel))
    );
    let resolved_cli_threads: Option<u64> = params.get("threads")
        .and_then(|s| resolve_gk_config(s, &kernel));

    // Pre-resolve phase cycles (with workload param expansion)
    let mut resolved_phase_cycles: HashMap<String, Option<u64>> = HashMap::new();
    for (name, phase) in &phases {
        let resolved = phase.cycles.as_ref().and_then(|s| {
            let expanded = expand_workload_params(s, &workload_params);
            resolve_gk_config(&expanded, &kernel)
        });
        resolved_phase_cycles.insert(name.clone(), resolved);
    }

    // Pre-resolve for_each iterations from the kernel
    let mut phase_iterations: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
    for (name, phase) in &phases {
        let iterations = resolve_for_each(phase, &workload_params, &kernel)?;
        phase_iterations.insert(name.clone(), iterations);
    }

    // Strip workload-level adapter/driver from op params
    for op in &mut all_ops_for_compile {
        op.params.remove("adapter");
        op.params.remove("driver");
    }

    let builder = Arc::new(OpBuilder::new(kernel));
    let program = builder.program();

    // === Execution ===

    if !phases.is_empty() {
        // --- Phased execution ---
        let scenario_name = params.get("scenario").map(|s| s.as_str()).unwrap_or("default");
        let phase_names = resolve_scenario(&scenarios, &phase_order, scenario_name)?;

        eprintln!("scenario '{scenario_name}' with {} phases: [{}]",
            phase_names.len(), phase_names.join(", "));

        for phase_name in &phase_names {
            let phase = phases.get(phase_name)
                .ok_or_else(|| format!(
                    "phase '{phase_name}' referenced in scenario '{scenario_name}' not found in phases section"
                ))?;

            eprintln!("=== phase: {phase_name} ===");

            // Get pre-resolved for_each iterations
            let iterations = phase_iterations.get(phase_name)
                .cloned()
                .unwrap_or_else(|| vec![HashMap::new()]);

            let is_for_each = phase.for_each.is_some();

            for (iter_idx, iter_bindings) in iterations.iter().enumerate() {
                if !iter_bindings.is_empty() {
                    let label = iter_bindings.values().next().unwrap_or(&String::new()).clone();
                    eprintln!("  iteration {iter_idx}: {label}");
                }

                // For for_each phases: clone raw ops, substitute iteration var,
                // run full expansion pipeline, compile inner kernel.
                // For non-for_each phases: use pre-compiled outer kernel ops.
                let (phase_ops, iter_program) = if is_for_each {
                    // Clone raw (pre-expansion) ops for this iteration
                    let mut iter_ops = for_each_raw_ops.get(phase_name)
                        .cloned()
                        .unwrap_or_default();

                    // Substitute iteration variable BEFORE expansion pipeline
                    for op in &mut iter_ops {
                        for (var, val) in iter_bindings {
                            let placeholder = format!("{{{var}}}");
                            // Substitute in op templates
                            for value in op.op.values_mut() {
                                if let Some(s) = value.as_str() {
                                    if s.contains(&placeholder) {
                                        *value = serde_json::Value::String(
                                            s.replace(&placeholder, val)
                                        );
                                    }
                                }
                            }
                            // Substitute in GK source
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                *src = src.replace(&placeholder, val);
                            }
                        }
                    }

                    // Strip adapter/driver from op params
                    for op in &mut iter_ops {
                        op.params.remove("adapter");
                        op.params.remove("driver");
                    }

                    // Run full GK expansion pipeline on substituted ops
                    expand_gk_bindings(&mut iter_ops, &workload_params, &phases);

                    // Compile inner kernel for this iteration
                    let inner_kernel = compile_bindings_with_libs_excluding(
                        &iter_ops, workload_dir, gk_lib_paths.clone(), strict, &[], &[],
                    ).map_err(|e| format!("compile bindings for {phase_name} iteration {iter_idx}: {e}"))?;

                    let inner_builder = Arc::new(OpBuilder::new(inner_kernel));
                    let inner_program = inner_builder.program();
                    (iter_ops, inner_program)
                } else {
                    // Non-for_each: use ops from outer compilation
                    let ops = if !phase.ops.is_empty() {
                        let phase_op_names: std::collections::HashSet<String> =
                            phase.ops.iter().map(|o| o.name.clone()).collect();
                        all_ops_for_compile.iter()
                            .filter(|o| {
                                phase_op_names.contains(&o.name)
                                    && o.tags.get("phase").map(|p| p == phase_name).unwrap_or(false)
                            })
                            .cloned()
                            .collect::<Vec<_>>()
                    } else if let Some(ref tag_spec) = phase.tags {
                        TagFilter::filter_ops(&all_ops_for_compile, tag_spec)
                            .map_err(|e| format!("invalid tag filter in phase '{phase_name}': {e}"))?
                    } else {
                        all_ops_for_compile[..num_top_level_ops].to_vec()
                    };
                    (ops, program.clone())
                };

                if phase_ops.is_empty() {
                    eprintln!("warning: phase '{phase_name}' has no ops, skipping");
                    continue;
                }

                let op_sequence = OpSequence::from_ops(phase_ops, seq_type);
                let stanza_len = op_sequence.stanza_length() as u64;

                let phase_stanzas = if is_for_each {
                    // Already resolved above via inner kernel (encoded in iter_program)
                    // But we need the cycle count. Re-resolve from phase config.
                    phase.cycles.as_ref().and_then(|c| {
                        let mut expanded = c.clone();
                        for (var, val) in iter_bindings {
                            expanded = expanded.replace(&format!("{{{var}}}"), val);
                        }
                        let expanded = expand_workload_params(&expanded, &workload_params);
                        // Use eval_const_expr since the inner kernel is consumed
                        if expanded.starts_with('{') && expanded.ends_with('}') {
                            let inner = &expanded[1..expanded.len()-1];
                            nb_variates::dsl::compile::eval_const_expr(inner)
                                .ok()
                                .map(|v| value_to_u64(&v))
                        } else {
                            parse_count(&expanded)
                        }
                    }).unwrap_or(1)
                } else {
                    resolved_phase_cycles.get(phase_name)
                        .copied()
                        .flatten()
                        .unwrap_or(1)
                };
                let phase_cycles = phase_stanzas * stanza_len;
                let phase_concurrency = phase.concurrency.unwrap_or(1);
                let phase_rate = phase.rate.or(cycle_rate);
                let phase_error_spec = phase.errors.clone().unwrap_or_else(|| error_spec.clone());

                eprintln!("phase '{phase_name}': {} ops, cycles={phase_cycles}, concurrency={phase_concurrency}",
                    op_sequence.stanza_length());

                let config = ActivityConfig {
                    name: phase_name.clone(),
                    cycles: phase_cycles,
                    concurrency: phase_concurrency,
                    cycle_rate: phase_rate,
                    stanza_rate: None,
                    sequencer: seq_type,
                    error_spec: phase_error_spec,
                    max_retries: 3,
                    stanza_concurrency: 1,
                };

                let phase_driver = phase.adapter.as_deref().unwrap_or(&driver);
                let adapter = create_adapter(phase_driver, &merged_params, dry_run).await?;
                let activity = Activity::with_params(
                    config, &Labels::of("session", "cli"), op_sequence, workload_params.clone(),
                );
                run_activity(activity, adapter, iter_program, openmetrics_url.as_deref()).await;
            }

            eprintln!("phase '{phase_name}' complete");
        }

        eprintln!("all phases complete");
    } else {
        // --- Single-activity execution ---
        let ops = all_ops_for_compile;
        let op_sequence = OpSequence::from_ops(ops, seq_type);

        let cycles = if let Some(c) = resolved_cli_cycles {
            if explicit_cycles.is_none() && params.contains_key("cycles") {
                eprintln!("cycles={c} (from GK constant)");
            }
            c
        } else {
            op_sequence.stanza_length() as u64
        };

        let threads = if let Some(c) = resolved_cli_threads {
            c as usize
        } else {
            threads
        };

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

        let adapter = create_adapter(&driver, &merged_params, dry_run).await?;
        let activity = Activity::with_params(
            config, &Labels::of("session", "cli"), op_sequence, workload_params,
        );
        run_activity(activity, adapter, program, openmetrics_url.as_deref()).await;
    }

    if dry_run.is_some() {
        eprintln!("dry-run complete.");
    } else {
        eprintln!("done.");
    }
    Ok(())
}

/// Create an adapter from inventory registrations.
/// Create an adapter, respecting dry-run mode.
async fn create_adapter(
    driver: &str,
    params: &HashMap<String, String>,
    dry_run: Option<&str>,
) -> Result<Arc<dyn crate::adapter::DriverAdapter>, String> {
    if let Some(mode) = dry_run {
        return Ok(Arc::new(DryRunAdapter { mode: mode.to_string() }));
    }
    let reg = find_adapter_registration(driver)
        .ok_or_else(|| {
            let available = registered_driver_names();
            format!("unknown adapter '{driver}' (available: {})", available.join(", "))
        })?;
    (reg.create)(params.clone()).await
}

/// Run an activity with optional OpenMetrics push reporting.
async fn run_activity(
    activity: Activity,
    adapter: Arc<dyn crate::adapter::DriverAdapter>,
    program: Arc<nb_variates::kernel::GkProgram>,
    openmetrics_url: Option<&str>,
) {
    if let Some(url) = openmetrics_url {
        let shared_metrics = activity.shared_metrics();
        let mut reporter = nb_metrics::reporters::victoriametrics::VictoriaMetricsReporter::new(url);
        let capture_interval = std::time::Duration::from_secs(1);
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = running.clone();
        eprintln!("pushing openmetrics to {url}");
        std::thread::spawn(move || {
            use nb_metrics::scheduler::Reporter;
            while flag.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(capture_interval);
                let frame = shared_metrics.capture(capture_interval);
                reporter.report(&frame);
            }
        });
        activity.run_with_driver(adapter, program).await;
        running.store(false, std::sync::atomic::Ordering::Relaxed);
    } else {
        activity.run_with_driver(adapter, program).await;
    }
}

/// Dry-run adapter: emit ops to stdout or silently discard.
struct DryRunAdapter {
    mode: String,
}

impl crate::adapter::DriverAdapter for DryRunAdapter {
    fn name(&self) -> &str { "dry-run" }

    fn map_op(&self, _template: &nb_workload::model::ParsedOp)
        -> Result<Box<dyn crate::adapter::OpDispenser>, String>
    {
        let mode = self.mode.clone();
        Ok(Box::new(DryRunDispenser { mode }))
    }
}

struct DryRunDispenser {
    mode: String,
}

impl crate::adapter::OpDispenser for DryRunDispenser {
    fn execute<'a>(&'a self, _cycle: u64, fields: &'a crate::adapter::ResolvedFields)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::adapter::OpResult, crate::adapter::ExecutionError>> + Send + 'a>>
    {
        let mode = &self.mode;
        Box::pin(async move {
            match mode.as_str() {
                "emit" => {
                    if let Some(stmt) = fields.get_str("stmt")
                        .or_else(|| fields.get_str("raw"))
                        .or_else(|| fields.get_str("prepared"))
                    {
                        println!("{stmt}");
                    } else {
                        println!("{}", fields.strings().join("\n"));
                    }
                }
                "json" => {
                    println!("{}", fields.to_json());
                }
                _ => {} // silent
            }
            Ok(crate::adapter::OpResult {
                body: None,
                captures: std::collections::HashMap::new(),
                skipped: false,
            })
        })
    }
}

// =========================================================================
// Helpers
// =========================================================================

/// GK expansion pipeline (Phases 1-3): workload param substitution,
/// param binding injection, and inline expression extraction.
///
/// Called once for the outer kernel's ops and once per for_each iteration.
fn expand_gk_bindings(
    ops: &mut [nb_workload::model::ParsedOp],
    workload_params: &HashMap<String, String>,
    phases: &HashMap<String, nb_workload::model::WorkloadPhase>,
) {
    // Phase 1: string substitution in GK source AND op template strings
    if !workload_params.is_empty() {
        for op in ops.iter_mut() {
            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                for (key, value) in workload_params {
                    let placeholder = format!("{{{key}}}");
                    if src.contains(&placeholder) {
                        *src = src.replace(&placeholder, value);
                    }
                }
            }
            for value in op.op.values_mut() {
                if let Some(s) = value.as_str() {
                    let mut rewritten = s.to_string();
                    let mut changed = false;
                    for (key, param_value) in workload_params {
                        let placeholder = format!("{{{key}}}");
                        if rewritten.contains(&placeholder) {
                            rewritten = rewritten.replace(&placeholder, param_value);
                            changed = true;
                        }
                    }
                    if changed {
                        *value = serde_json::Value::String(rewritten);
                    }
                }
            }
        }

        // Phase 2: inject param bindings into GK source
        let mut op_refs: std::collections::HashSet<String> = std::collections::HashSet::new();
        for op in ops.iter() {
            for value in op.op.values() {
                if let Some(s) = value.as_str() {
                    for name in nb_workload::bindpoints::referenced_bindings(s) {
                        if workload_params.contains_key(&name) {
                            op_refs.insert(name);
                        }
                    }
                }
            }
        }
        for phase in phases.values() {
            for config_val in [&phase.cycles].into_iter().flatten() {
                if config_val.starts_with('{') && config_val.ends_with('}') {
                    let name = &config_val[1..config_val.len()-1];
                    if workload_params.contains_key(name) {
                        op_refs.insert(name.to_string());
                    }
                }
            }
        }
        if !op_refs.is_empty() {
            for op in ops.iter_mut() {
                if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                    for name in &op_refs {
                        let def_pattern = format!("{name} :=");
                        if src.contains(&def_pattern) {
                            continue;
                        }
                        let value = &workload_params[name];
                        let binding = if value.parse::<u64>().is_ok() || value.parse::<f64>().is_ok() {
                            format!("\n{name} := {value}")
                        } else {
                            format!("\n{name} := \"{value}\"")
                        };
                        src.push_str(&binding);
                    }
                }
            }
        }
    }

    // Phase 3: rewrite inline expressions in op templates to GK bindings
    {
        let mut inline_idx = 0usize;
        let mut expr_to_name: std::collections::HashMap<String, String> = std::collections::HashMap::new();

        for op in ops.iter() {
            for value in op.op.values() {
                if let Some(s) = value.as_str() {
                    for bp in nb_workload::bindpoints::extract_bind_points(s) {
                        if let nb_workload::bindpoints::BindPoint::InlineDefinition(ref expr) = bp {
                            if !expr_to_name.contains_key(expr) {
                                let name = format!("__expr_{inline_idx}");
                                inline_idx += 1;
                                expr_to_name.insert(expr.clone(), name);
                            }
                        }
                    }
                }
            }
        }

        if !expr_to_name.is_empty() {
            for op in ops.iter_mut() {
                if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                    for (expr, name) in &expr_to_name {
                        let def_pattern = format!("{name} :=");
                        if !src.contains(&def_pattern) {
                            src.push_str(&format!("\n{name} := {expr}"));
                        }
                    }
                }
            }

            for op in ops.iter_mut() {
                for value in op.op.values_mut() {
                    if let Some(s) = value.as_str() {
                        let mut rewritten = s.to_string();
                        for (expr, name) in &expr_to_name {
                            rewritten = rewritten.replace(
                                &format!("{{{{{expr}}}}}"),
                                &format!("{{{name}}}"),
                            );
                            rewritten = rewritten.replace(
                                &format!("{{:={expr}:=}}"),
                                &format!("{{{name}}}"),
                            );
                            rewritten = rewritten.replace(
                                &format!("{{:={expr}}}"),
                                &format!("{{{name}}}"),
                            );
                            rewritten = rewritten.replace(
                                &format!("{{{expr}}}"),
                                &format!("{{{name}}}"),
                            );
                        }
                        *value = serde_json::Value::String(rewritten);
                    }
                }
            }
        }
    }
}

/// Resolve a phase's `for_each` directive into iteration bindings.
///
/// Parses `"varname in expr(...)"`, evaluates the expression as a GK
/// init-time constant (via the compiled kernel), and splits the
/// comma-separated result into individual values.
///
/// Returns a Vec of binding maps — one per iteration. If no `for_each`
/// is present, returns a single empty map (one iteration, no bindings).
fn resolve_for_each(
    phase: &nb_workload::model::WorkloadPhase,
    workload_params: &HashMap<String, String>,
    kernel: &nb_variates::kernel::GkKernel,
) -> Result<Vec<HashMap<String, String>>, String> {
    let Some(ref spec) = phase.for_each else {
        return Ok(vec![HashMap::new()]);
    };

    // Parse "varname in expression"
    let parts: Vec<&str> = spec.splitn(2, " in ").collect();
    if parts.len() != 2 {
        return Err(format!("invalid for_each syntax: '{spec}'. Expected 'varname in expression'"));
    }
    let var_name = parts[0].trim();
    let mut expr = parts[1].trim().to_string();

    // Expand workload params in the expression
    for (key, value) in workload_params {
        let placeholder = format!("{{{key}}}");
        expr = expr.replace(&placeholder, value);
    }

    // Try to read the value from the GK kernel as a folded constant.
    // The expression might be a binding name or an inline const expression.
    let value_str = if let Some(val) = kernel.get_constant(&expr) {
        match val {
            nb_variates::node::Value::Str(s) => s.clone(),
            other => other.to_display_string(),
        }
    } else {
        // Try evaluating as inline const expression
        match nb_variates::dsl::compile::eval_const_expr(&expr) {
            Ok(val) => match val {
                nb_variates::node::Value::Str(s) => s,
                other => other.to_display_string(),
            },
            Err(e) => return Err(format!("for_each expression failed: '{expr}': {e}")),
        }
    };

    // Split comma-separated values into individual iterations
    let values: Vec<String> = value_str.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if values.is_empty() {
        eprintln!("warning: for_each '{spec}' produced no values, skipping phase");
        return Ok(vec![]);
    }

    eprintln!("for_each: {var_name} in [{values}] ({} iterations)",
        values.len(),
        values = values.join(", "));

    Ok(values.into_iter()
        .map(|v| {
            let mut bindings = HashMap::new();
            bindings.insert(var_name.to_string(), v);
            bindings
        })
        .collect())
}

/// Expand `{key}` workload param placeholders in a string.
fn expand_workload_params(s: &str, params: &HashMap<String, String>) -> String {
    let mut result = s.to_string();
    for (key, value) in params {
        let placeholder = format!("{{{key}}}");
        if result.contains(&placeholder) {
            result = result.replace(&placeholder, value);
        }
    }
    result
}

/// Resolve a config value to u64 via GK constants or numeric parsing.
fn resolve_gk_config(value: &str, kernel: &nb_variates::kernel::GkKernel) -> Option<u64> {
    if value.starts_with('{') && value.ends_with('}') {
        let inner = &value[1..value.len() - 1];
        if let Some(v) = kernel.get_constant(inner) {
            return Some(value_to_u64(v));
        }
        match nb_variates::dsl::compile::eval_const_expr(inner) {
            Ok(v) => Some(value_to_u64(&v)),
            Err(e) => {
                eprintln!("error: const expression failed: '{{{inner}}}'");
                eprintln!("  {e}");
                None
            }
        }
    } else {
        parse_count(value)
    }
}

/// Convert a GK Value to u64, handling f64→u64 truncation.
fn value_to_u64(v: &nb_variates::node::Value) -> u64 {
    match v {
        nb_variates::node::Value::U64(n) => *n,
        nb_variates::node::Value::F64(f) => *f as u64,
        nb_variates::node::Value::Bool(b) => if *b { 1 } else { 0 },
        _ => 0,
    }
}

/// Resolve a scenario name to a list of phase names.
fn resolve_scenario(
    scenarios: &HashMap<String, Vec<nb_workload::model::ScenarioStep>>,
    phase_order: &[String],
    name: &str,
) -> Result<Vec<String>, String> {
    if let Some(steps) = scenarios.get(name) {
        return Ok(steps.iter().map(|s| s.name.clone()).collect());
    }
    if name == "default" && !phase_order.is_empty() {
        return Ok(phase_order.to_vec());
    }
    Err(format!("scenario '{name}' not found"))
}

/// Normalize args: detect scenario shorthand where a bare word after
/// the workload file becomes `scenario=<name>`.
fn normalize_args(args: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    let mut workload_seen = false;
    let mut scenario_set = false;
    for arg in args {
        if !workload_seen
            && (arg.ends_with(".yaml") || arg.ends_with(".yml") || arg.contains("workload="))
        {
            workload_seen = true;
            result.push(arg.clone());
        } else if workload_seen && !scenario_set && !arg.contains('=') && !arg.starts_with('-') {
            result.push(format!("scenario={arg}"));
            scenario_set = true;
        } else {
            result.push(arg.clone());
        }
    }
    result
}

/// Parse `key=value` pairs from command line args.
pub fn parse_params(args: &[String]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for arg in args {
        if arg.starts_with('-') { continue; }
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

/// Find the closest match using Levenshtein distance.
fn closest_match<'a>(input: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let mut best: Option<(&str, usize)> = None;
    for &candidate in candidates {
        let d = levenshtein(input, candidate);
        if best.is_none() || d < best.unwrap().1 {
            best = Some((candidate, d));
        }
    }
    best.filter(|(_, d)| *d <= (input.len() / 2).max(2))
        .map(|(s, _)| s)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());
    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];
    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}
