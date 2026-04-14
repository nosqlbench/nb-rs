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
    "adapter", "driver", "workload", "op", "cycles", "concurrency",
    "rate", "stanzarate", "errors", "seq", "tags", "format",
    "filename", "separator", "header", "color",
    "stanza_concurrency", "sc", "scenario", "dryrun",
];

/// Run a workload. Adapters are discovered from link-time inventory
/// registrations — the calling binary just needs to link the adapter
/// crates it wants available.
/// Execution depth: how far through the pipeline to go.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ExecDepth {
    /// Compile scopes, stop before cycles. No adapters created.
    Phase,
    /// Run cycles with dry-run adapter.
    Cycle,
    /// Normal execution.
    Full,
}

/// Diagnostic configuration parsed from `dryrun=` parameter.
#[derive(Clone)]
pub struct DiagnosticConfig {
    /// How far to execute.
    pub depth: ExecDepth,
    /// Emit GK provenance and data flow analysis.
    pub explain_gk: bool,
}

impl DiagnosticConfig {
    /// Normal execution, no diagnostics.
    pub fn normal() -> Self {
        Self { depth: ExecDepth::Full, explain_gk: false }
    }

    /// Parse from `dryrun=` value (e.g., "phase,gk" or "cycle").
    pub fn parse(spec: &str) -> Self {
        let mut config = Self::normal();
        for flag in spec.split(',') {
            match flag.trim() {
                "phase" => config.depth = ExecDepth::Phase,
                "cycle" => config.depth = ExecDepth::Cycle,
                "full" => config.depth = ExecDepth::Full,
                "gk" => config.explain_gk = true,
                _ => eprintln!("warning: unknown dryrun flag '{flag}'"),
            }
        }
        config
    }
}


pub async fn run(args: &[String]) -> Result<(), String> {
    let args: &[String] = match args.first().map(|s| s.as_str()) {
        Some("run") => &args[1..],
        // Reject unknown subcommands — don't silently fall through to execution
        Some(cmd) if !cmd.contains('=') && !cmd.ends_with(".yaml") && !cmd.ends_with(".yml") => {
            return Err(format!("unknown command '{cmd}'. Use 'run' or pass a workload file."));
        }
        _ => args,
    };
    run_impl(args).await
}

/// Core runner. Diagnostic mode is controlled by `dryrun=` param.
async fn run_impl(args: &[String]) -> Result<(), String> {
    let mut diag = DiagnosticConfig::normal();

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
    let concurrency: usize = match merged_params.get("concurrency") {
        Some(s) => s.parse().map_err(|_| format!("concurrency value '{s}' is not a valid integer"))?,
        None => 1,
    };
    let cycle_rate: Option<f64> = match merged_params.get("rate") {
        Some(s) => Some(s.parse().map_err(|_| format!("rate value '{s}' is not a valid number"))?),
        None => None,
    };
    let stanza_rate: Option<f64> = match merged_params.get("stanzarate") {
        Some(s) => Some(s.parse().map_err(|_| format!("stanzarate value '{s}' is not a valid number"))?),
        None => None,
    };
    let tag_filter = merged_params.get("tags").cloned();
    let seq_type = merged_params.get("seq")
        .map(|s| SequencerType::parse(s).unwrap_or(SequencerType::Bucket))
        .unwrap_or(SequencerType::Bucket);
    let error_spec = merged_params.get("errors")
        .cloned()
        .unwrap_or_else(|| ".*:warn,stop".to_string());

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

    // Validate workload-declared params are actually referenced.
    // Unreferenced params can shadow runner params (e.g., a workload
    // declaring `concurrency` as a param masks the CLI parameter
    // validation, but if nothing in the workload uses `{concurrency}`
    // the value is silently ignored).
    {
        let referenced = collect_param_references(&workload);
        for name in &workload.declared_params {
            // Skip params that are also known runner/adapter params — these
            // are referenced by the runner itself.
            if KNOWN_PARAMS.contains(&name.as_str()) {
                continue;
            }
            if !referenced.contains(name) {
                return Err(format!(
                    "workload declares param '{name}' but it is never referenced as '{{{}}}' \
                     in any op, phase, or binding. Remove it or use it.",
                    name
                ));
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

    // Classify phase ops for compilation:
    // - Phases with own bindings or for_each: saved raw, compiled per-phase
    // - Phases without own bindings: included in outer (workload) kernel
    let mut phase_ops_for_compile: Vec<nb_workload::model::ParsedOp> = Vec::new();
    let mut phase_raw_ops: HashMap<String, Vec<nb_workload::model::ParsedOp>> = HashMap::new();
    let mut phases_needing_own_kernel: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (name, phase) in &phases {
        let has_own_bindings = phase.ops.iter().any(|op| !op.bindings.is_empty());
        if phase.for_each.is_some() || has_own_bindings {
            phase_raw_ops.insert(name.clone(), phase.ops.clone());
            phases_needing_own_kernel.insert(name.clone());
        } else {
            phase_ops_for_compile.extend(phase.ops.iter().cloned());
        }
    }


    // For non-phased workloads, require at least some ops
    if ops.is_empty() && phases.is_empty() {
        return Err("no ops selected (tag filter may have excluded all ops)".into());
    }

    if phases.is_empty() {
        eprintln!("{} ops, {} cycles, concurrency={}, adapter={}",
            ops.len(),
            explicit_cycles.map(|c| c.to_string()).unwrap_or("auto".into()),
            concurrency,
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

    // Parse dryrun= param into diagnostic config
    if let Some(spec) = params.get("dryrun") {
        diag = DiagnosticConfig::parse(spec);
    }

    // Dry-run mode: dryrun=cycle uses "silent" adapter
    let dry_run: Option<&str> = if diag.depth == ExecDepth::Cycle {
        Some("silent")
    } else {
        params.get("dryrun").and_then(|s| match s.as_str() {
            "emit" => Some("emit"),
            "silent" => Some("silent"),
            _ => None,
        })
    };

    // OpenMetrics push URL
    let openmetrics_url: Option<String> = args.iter()
        .find_map(|a| a.strip_prefix("--report-openmetrics-to=")
            .or_else(|| a.strip_prefix("report-openmetrics-to=")))
        .map(|s| s.to_string());

    // SQLite metrics capture: create per-session database.
    // Standard for every session — stores all dimensional metrics.
    let sqlite_path = format!("nb-metrics-{}.db",
        chrono_session_id());
    let sqlite_reporter = nb_metrics::reporters::sqlite::SqliteReporter::new(&sqlite_path)
        .map(|mut r| {
            // Record session metadata
            r.set_metadata("workload", workload_file.as_deref().unwrap_or("inline"));
            r.set_metadata("start_time", &format!("{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()));
            for (k, v) in &merged_params {
                r.set_metadata(&format!("param.{k}"), v);
            }
            eprintln!("metrics: {sqlite_path}");
            r
        })
        .map_err(|e| eprintln!("warning: SQLite metrics disabled: {e}"))
        .ok();
    let sqlite_reporter = std::sync::Arc::new(std::sync::Mutex::new(sqlite_reporter));

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

    // Extract output manifest and folded constant values from outer kernel
    // for scope composition (sysref 16). Must be done before kernel is
    // consumed by OpBuilder.
    let outer_manifest = extract_manifest(kernel.program());
    let mut outer_scope_values: Vec<(String, nb_variates::node::Value)> = outer_manifest.iter()
        .filter_map(|entry| {
            kernel.get_constant(&entry.name)
                .map(|v| (entry.name.clone(), v.clone()))
        })
        .collect();
    // Original snapshot for loop_scope: clean (unaffected by shared write-back)
    let original_scope_values = outer_scope_values.clone();

    // === GK Config Resolution (all done before kernel is consumed) ===

    let resolved_cli_cycles: Option<u64> = explicit_cycles.or_else(||
        params.get("cycles").and_then(|s| resolve_gk_config(s, &kernel))
    );
    let resolved_cli_concurrency: Option<u64> = params.get("concurrency")
        .and_then(|s| resolve_gk_config(s, &kernel));

    // Collect phases that are inside scenario for_each groups — these have
    // iteration variables resolved at runtime, not pre-resolution time.
    fn collect_grouped_phases(nodes: &[nb_workload::model::ScenarioNode], in_group: bool, out: &mut std::collections::HashSet<String>) {
        for node in nodes {
            match node {
                nb_workload::model::ScenarioNode::Phase(name) => {
                    if in_group { out.insert(name.clone()); }
                }
                nb_workload::model::ScenarioNode::ForEach { children, .. } => {
                    collect_grouped_phases(children, true, out);
                }
            }
        }
    }
    let mut grouped_phases = std::collections::HashSet::new();
    for nodes in scenarios.values() {
        collect_grouped_phases(nodes, false, &mut grouped_phases);
    }

    // Pre-resolve phase cycles (skip phases with for_each or in scenario groups)
    let mut resolved_phase_cycles: HashMap<String, Option<u64>> = HashMap::new();
    for (name, phase) in &phases {
        if phase.for_each.is_some() || grouped_phases.contains(name) {
            continue;
        }
        let resolved = phase.cycles.as_ref().and_then(|s| {
            let expanded = expand_workload_params(s, &workload_params);
            resolve_gk_config(&expanded, &kernel)
        });
        resolved_phase_cycles.insert(name.clone(), resolved);
    }

    // Pre-resolve for_each iterations (only for standalone phases, not grouped ones)
    let mut phase_iterations: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
    for (name, phase) in &phases {
        if phase.for_each.is_some() && !grouped_phases.contains(name) {
            let iterations = resolve_for_each(phase, &workload_params, &kernel)?;
            phase_iterations.insert(name.clone(), iterations);
        }
    }

    // Strip workload-level adapter/driver from op params
    // (adapter is resolved per-phase/per-op, not from workload params)
    for op in &mut all_ops_for_compile {
        op.params.remove("adapter");
        op.params.remove("driver");
    }
    for ops in phase_raw_ops.values_mut() {
        for op in ops.iter_mut() {
            op.params.remove("adapter");
            op.params.remove("driver");
        }
    }

    let builder = Arc::new(OpBuilder::new(kernel));
    let program = builder.program();

    // === Execution ===

    if !phases.is_empty() {
        // --- Phased execution ---
        let scenario_name = params.get("scenario").map(|s| s.as_str()).unwrap_or("default");
        let scenario_nodes = resolve_scenario(&scenarios, &phase_order, scenario_name)?;

        eprintln!("scenario '{scenario_name}': {}", format_scenario_tree(&scenario_nodes));

        // Build flat execution plan from recursive scenario tree.
        // Each entry is (phase_name, bindings) where bindings accumulate
        // from all enclosing for_each levels.
        let execution_plan = flatten_scenario_tree(
            &scenario_nodes, &phases, &workload_params,
            &outer_scope_values, &phase_iterations,
            &HashMap::new(), // no outer bindings at top level
        )?;

        for (phase_name, iterations) in &execution_plan {
            let phase = phases.get(phase_name)
                .ok_or_else(|| format!(
                    "phase '{phase_name}' referenced in scenario '{scenario_name}' not found in phases section"
                ))?;

            eprintln!("=== phase: {phase_name} ===");

            // iterations come from the execution plan (pre-resolved for grouped steps,
            // or from phase.for_each for non-grouped steps)
            let is_for_each = iterations.len() > 1 || iterations.iter().any(|m| !m.is_empty());

            // Detect if for_each variable is structural (in bindings) or
            // parametric (only in op fields). Parametric means the GK
            // kernel can be compiled once and reused across iterations.
            let for_each_var = phase.for_each.as_ref()
                .and_then(|spec| spec.splitn(2, " in ").next().map(|s| s.trim().to_string()))
                .or_else(|| iterations.first()
                    .and_then(|m| m.keys().next().cloned()));
            let has_own_bindings = phase_raw_ops.get(phase_name)
                .map(|ops| ops.iter().any(|op| !op.bindings.is_empty()))
                .unwrap_or(false);
            let is_parametric = if let Some(ref var) = for_each_var {
                // Parametric requires: (1) variable not in bindings, AND
                // (2) the phase has no own bindings that need a separate kernel.
                // If the phase has bindings (dim, id, train_vector, etc.),
                // it needs its own compiled kernel even if the for_each var
                // is only in op fields.
                if has_own_bindings {
                    false
                } else {
                    let placeholder = format!("{{{var}}}");
                    let raw = phase_raw_ops.get(phase_name).cloned().unwrap_or_default();
                    !raw.iter().any(|op| {
                        if let nb_workload::model::BindingsDef::GkSource(ref src) = op.bindings {
                            src.contains(&placeholder)
                        } else {
                            false
                        }
                    })
                }
            } else {
                false
            };

            if is_for_each && is_parametric {
                eprintln!("  for_each variable '{}' is parametric (no recompilation needed)",
                    for_each_var.as_deref().unwrap_or("?"));
            }

            // Parse scope modes (only meaningful for for_each phases)
            // Default: loop starts clean from outer, iterations inherit
            // from each other (shared loop-level state).
            let loop_scope = phase.loop_scope.as_deref().unwrap_or("clean");
            let iter_scope = phase.iter_scope.as_deref()
                .unwrap_or(if is_for_each { "inherit" } else { "clean" });
            if is_for_each {
                if !matches!(loop_scope, "clean" | "inherit") {
                    return Err(format!(
                        "phase '{phase_name}': invalid loop_scope '{loop_scope}'. \
                         Expected 'clean' or 'inherit'."
                    ));
                }
                if !matches!(iter_scope, "clean" | "inherit") {
                    return Err(format!(
                        "phase '{phase_name}': invalid iter_scope '{iter_scope}'. \
                         Expected 'clean' or 'inherit'."
                    ));
                }
            }

            // Loop scope: determines which outer values the loop sees.
            // - clean: original workload snapshot (ignores shared write-back from prior phases)
            // - inherit: current outer state (includes shared mutations from prior phases)
            let loop_scope_values: Vec<(String, nb_variates::node::Value)> = match loop_scope {
                "inherit" => outer_scope_values.clone(),
                _ => original_scope_values.clone(), // clean: original snapshot
            };

            // For iter_scope: inherit, track mutations across iterations
            // (only effective when `shared` variables exist in the kernel)
            let mut iter_carried_scope: Vec<(String, nb_variates::node::Value)> =
                loop_scope_values.clone();

            for (iter_idx, iter_bindings) in iterations.iter().enumerate() {
                if !iter_bindings.is_empty() {
                    let label = iter_bindings.values().next().unwrap_or(&String::new()).clone();
                    eprintln!("  iteration {iter_idx}: {label}");
                }

                // Select scope values based on iter_scope mode
                let scope_values = match iter_scope {
                    "inherit" => &iter_carried_scope,
                    _ => &loop_scope_values, // clean: each iter gets loop snapshot
                };

                // For for_each phases: clone raw ops, substitute iteration var,
                // run full expansion pipeline, compile inner kernel.
                // For non-for_each phases: use pre-compiled outer kernel ops.
                let (phase_ops, iter_program, iter_stanzas) = if is_for_each && !is_parametric {
                    // STRUCTURAL: iteration variable appears in bindings,
                    // must recompile the GK kernel per iteration.
                    let mut iter_ops = phase_raw_ops.get(phase_name)
                        .cloned()
                        .unwrap_or_default();

                    // Substitute iteration variables in GK bindings source only
                    // (needed for node construction literals like "sift1m:{profile}").
                    // Op field resolution pulls values from GK outputs — no text
                    // substitution in op fields needed.
                    for op in &mut iter_ops {
                        for (var, val) in iter_bindings {
                            let placeholder = format!("{{{var}}}");
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                *src = src.replace(&placeholder, val);
                            }
                        }
                    }

                    // Inject iteration variables as init-time GK constants.
                    // This makes them available as GK outputs so op field resolution,
                    // relevancy config, cycle expressions, etc. can pull them through
                    // the normal GK path — no side-channel text substitution needed.
                    for (var, val) in iter_bindings {
                        for op in &mut iter_ops {
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                // Prepend init binding (after any inputs declaration)
                                let init_line = format!("init {var} = \"{val}\"\n");
                                *src = format!("{init_line}{src}");
                                break; // only inject once (all ops share the same source)
                            }
                        }
                    }

                    // Strip adapter/driver from op params
                    for op in &mut iter_ops {
                        op.params.remove("adapter");
                        op.params.remove("driver");
                    }

                    // Generate auto-externs from outer manifest (after substitution)
                    let auto_externs = generate_auto_externs(&iter_ops, &outer_manifest)?;
                    if !auto_externs.is_empty() {
                        for op in &mut iter_ops {
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                *src = format!("{auto_externs}{src}");
                            }
                        }
                    }

                    // Run full GK expansion pipeline on substituted ops
                    expand_gk_bindings(&mut iter_ops, &workload_params, &phases);

                    // Compile inner kernel for this iteration
                    // Include the cycles expression as a config_ref so it survives DCE
                    let mut iter_config_refs: Vec<String> = Vec::new();
                    if let Some(ref c) = phase.cycles {
                        if c.starts_with('{') && c.ends_with('}') {
                            let mut inner = c[1..c.len()-1].to_string();
                            for (var, val) in iter_bindings {
                                inner = inner.replace(&format!("{{{var}}}"), val);
                            }
                            inner = expand_workload_params(&inner, &workload_params);
                            iter_config_refs.push(inner);
                        }
                    }
                    let mut inner_kernel = compile_bindings_with_libs_excluding(
                        &iter_ops, workload_dir, gk_lib_paths.clone(), strict, &[], &iter_config_refs,
                    ).map_err(|e| format!("compile bindings for {phase_name} iteration {iter_idx}: {e}"))?;

                    // Resolve cycles from inner kernel BEFORE consuming it
                    let stanzas = phase.cycles.as_ref().and_then(|c| {
                        let mut expanded = c.clone();
                        for (var, val) in iter_bindings {
                            expanded = expanded.replace(&format!("{{{var}}}"), val);
                        }
                        let expanded = expand_workload_params(&expanded, &workload_params);
                        resolve_gk_config(&expanded, &inner_kernel)
                    });

                    // Wire scope values into inner kernel's extern inputs
                    for (name, value) in scope_values {
                        if let Some(idx) = inner_kernel.program().find_input(name) {
                            inner_kernel.state().set_input(idx, value.clone());
                        }
                    }

                    // For iter_scope: inherit, extract inner kernel's constants
                    // to carry into the next iteration's scope.
                    // (Effective when `shared` variables exist — the runtime
                    // propagates their values across iteration boundaries.)
                    if iter_scope == "inherit" {
                        for entry in &outer_manifest {
                            if let Some(val) = inner_kernel.get_constant(&entry.name) {
                                if let Some(existing) = iter_carried_scope.iter_mut()
                                    .find(|(n, _)| n == &entry.name)
                                {
                                    existing.1 = val.clone();
                                } else {
                                    iter_carried_scope.push((entry.name.clone(), val.clone()));
                                }
                            }
                        }
                    }

                    let inner_builder = Arc::new(OpBuilder::new(inner_kernel));
                    let inner_program = inner_builder.program();
                    (iter_ops, inner_program, stanzas)
                } else if is_for_each && is_parametric {
                    // PARAMETRIC: iteration variable only in op fields.
                    // Compile a kernel with iteration vars as init constants
                    // so all values are resolved through GK (no side-channel).
                    let mut iter_ops = phase_raw_ops.get(phase_name)
                        .cloned()
                        .unwrap_or_default();

                    // Substitute iteration variables in GK bindings source
                    for op in &mut iter_ops {
                        for (var, val) in iter_bindings {
                            let placeholder = format!("{{{var}}}");
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                *src = src.replace(&placeholder, val);
                            }
                        }
                    }

                    // Inject iteration variables as GK init constants
                    for (var, val) in iter_bindings {
                        for op in &mut iter_ops {
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                let init_line = format!("init {var} = \"{val}\"\n");
                                *src = format!("{init_line}{src}");
                                break;
                            }
                        }
                    }

                    // Strip adapter/driver from op params
                    for op in &mut iter_ops {
                        op.params.remove("adapter");
                        op.params.remove("driver");
                    }

                    // Generate auto-externs + expand
                    let auto_externs = generate_auto_externs(&iter_ops, &outer_manifest)?;
                    if !auto_externs.is_empty() {
                        for op in &mut iter_ops {
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                *src = format!("{auto_externs}{src}");
                            }
                        }
                    }
                    expand_gk_bindings(&mut iter_ops, &workload_params, &phases);

                    // Compile kernel with iteration constants
                    let mut iter_config_refs: Vec<String> = Vec::new();
                    if let Some(ref c) = phase.cycles {
                        if c.starts_with('{') && c.ends_with('}') {
                            let mut inner = c[1..c.len()-1].to_string();
                            for (var, val) in iter_bindings {
                                inner = inner.replace(&format!("{{{var}}}"), val);
                            }
                            inner = expand_workload_params(&inner, &workload_params);
                            iter_config_refs.push(inner);
                        }
                    }
                    let mut inner_kernel = compile_bindings_with_libs_excluding(
                        &iter_ops, workload_dir, gk_lib_paths.clone(), strict, &[], &iter_config_refs,
                    ).map_err(|e| format!("compile bindings for {phase_name} iteration {iter_idx}: {e}"))?;

                    let stanzas = phase.cycles.as_ref().and_then(|c| {
                        let mut expanded = c.clone();
                        for (var, val) in iter_bindings {
                            expanded = expanded.replace(&format!("{{{var}}}"), val);
                        }
                        let expanded = expand_workload_params(&expanded, &workload_params);
                        resolve_gk_config(&expanded, &inner_kernel)
                    });

                    // Wire scope values
                    for (name, value) in scope_values {
                        if let Some(idx) = inner_kernel.program().find_input(name) {
                            inner_kernel.state().set_input(idx, value.clone());
                        }
                    }

                    let inner_builder = Arc::new(OpBuilder::new(inner_kernel));
                    let inner_program = inner_builder.program();
                    (iter_ops, inner_program, stanzas)
                } else if phases_needing_own_kernel.contains(phase_name) {
                    // Phase has own bindings: compose with outer scope
                    // via auto-extern declarations (sysref 16).
                    let mut phase_ops = phase_raw_ops.get(phase_name)
                        .cloned()
                        .unwrap_or_default();

                    // Generate extern declarations for outer-scope names
                    // referenced in inner ops but not defined in inner bindings
                    let auto_externs = generate_auto_externs(&phase_ops, &outer_manifest)?;
                    if !auto_externs.is_empty() {
                        for op in &mut phase_ops {
                            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                                *src = format!("{auto_externs}{src}");
                            }
                        }
                    }

                    // Run expansion pipeline
                    expand_gk_bindings(&mut phase_ops, &workload_params, &phases);

                    // Compile phase kernel (contains only inner nodes + extern inputs)
                    let mut phase_config_refs: Vec<String> = Vec::new();
                    if let Some(ref c) = phase.cycles {
                        if c.starts_with('{') && c.ends_with('}') {
                            let inner = expand_workload_params(&c[1..c.len()-1], &workload_params);
                            phase_config_refs.push(inner);
                        }
                    }
                    let mut phase_kernel = compile_bindings_with_libs_excluding(
                        &phase_ops, workload_dir, gk_lib_paths.clone(), strict, &[], &phase_config_refs,
                    ).map_err(|e| format!("compile bindings for phase '{phase_name}': {e}"))?;

                    let stanzas = phase.cycles.as_ref().and_then(|c| {
                        let expanded = expand_workload_params(c, &workload_params);
                        resolve_gk_config(&expanded, &phase_kernel)
                    });

                    // Wire outer scope constants into phase kernel's extern inputs
                    for (name, value) in &outer_scope_values {
                        if let Some(idx) = phase_kernel.program().find_input(name) {
                            phase_kernel.state().set_input(idx, value.clone());
                        }
                    }
                    let phase_builder = Arc::new(OpBuilder::new(phase_kernel));
                    let phase_program = phase_builder.program();
                    (phase_ops, phase_program, stanzas)
                } else {
                    // Phase uses workload kernel (no own bindings)
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
                    let stanzas = resolved_phase_cycles.get(phase_name).copied().flatten();
                    (ops, program.clone(), stanzas)
                };

                if phase_ops.is_empty() {
                    eprintln!("warning: phase '{phase_name}' has no ops, skipping");
                    continue;
                }

                let op_sequence = OpSequence::from_ops(phase_ops, seq_type);
                let stanza_len = op_sequence.stanza_length() as u64;

                // Resolve cycle count: ==auto (verbose), ===auto (silent), or explicit
                let cycles_spec = phase.cycles.as_deref().unwrap_or("");
                let phase_cycles = if cycles_spec == "==auto" {
                    eprintln!("  cycles: auto ({stanza_len} ops = {stanza_len} cycles)");
                    stanza_len
                } else if cycles_spec == "===auto" || cycles_spec.is_empty() {
                    stanza_len
                } else {
                    iter_stanzas.unwrap_or(1) * stanza_len
                };
                let phase_concurrency = match phase.concurrency.as_ref() {
                    Some(s) => {
                        let expanded = expand_workload_params(s, &workload_params);
                        match expanded.parse::<usize>() {
                            Ok(v) => v,
                            Err(_) => return Err(format!(
                                "phase '{phase_name}': concurrency value '{expanded}' is not a valid integer"
                            )),
                        }
                    }
                    None => concurrency,
                };
                let phase_rate = phase.rate.or(cycle_rate);
                let phase_error_spec = phase.errors.clone().unwrap_or_else(|| error_spec.clone());

                // Diagnostic output: GK explain
                if diag.explain_gk {
                    let iter_label = iter_bindings.values().next().cloned().unwrap_or_default();
                    let iter_note = if !iter_label.is_empty() {
                        format!(" (iteration: {iter_label})")
                    } else { String::new() };
                    crate::describe::print_kernel_analysis(
                        phase_name, &iter_note, &iter_program,
                    );
                }
                // Execution depth gate: skip cycles if depth is Phase
                if diag.depth == ExecDepth::Phase {
                    continue;
                }

                eprintln!("phase '{phase_name}': {} ops, cycles={phase_cycles}, concurrency={phase_concurrency}",
                    op_sequence.stanza_length());

                let iter_label = iter_bindings.values().next().cloned().unwrap_or_default();
                let iter_name = if !iter_label.is_empty() {
                    let var = iter_bindings.keys().next().unwrap();
                    format!("{phase_name} ({var}={iter_label})")
                } else {
                    phase_name.clone()
                };
                let config = ActivityConfig {
                    name: iter_name,
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

                // Build adapter map: phase default + any per-op overrides.
                let mut adapter_names: std::collections::HashSet<String> = std::collections::HashSet::new();
                adapter_names.insert(phase_driver.to_string());
                for t in op_sequence.templates() {
                    if let Some(a) = t.params.get("adapter").and_then(|v| v.as_str()) {
                        if a != phase_driver {
                            adapter_names.insert(a.to_string());
                        }
                    }
                }
                let mut adapter_map: std::collections::HashMap<String, Arc<dyn crate::adapter::DriverAdapter>> =
                    std::collections::HashMap::new();
                for aname in &adapter_names {
                    let a = create_adapter(aname, &merged_params, dry_run).await?;
                    adapter_map.insert(a.name().to_string(), a);
                }

                let activity = Activity::with_params(
                    config, &Labels::of("session", "cli"), op_sequence, workload_params.clone(),
                );
                let stopped = run_activity_with_adapters(
                    activity, adapter_map, phase_driver, iter_program,
                    openmetrics_url.as_deref(), sqlite_reporter.clone(),
                ).await;
                if stopped {
                    return Err(format!("phase '{phase_name}' stopped by error handler"));
                }
            }

            // Shared write-back: after all iterations, propagate `shared`
            // outputs from the last iteration back to outer_scope_values
            // so subsequent phases see the updated values.
            if is_for_each {
                for entry in &outer_manifest {
                    if entry.modifier == nb_variates::dsl::ast::BindingModifier::Shared {
                        if let Some(carried) = iter_carried_scope.iter()
                            .find(|(n, _)| n == &entry.name)
                        {
                            if let Some(existing) = outer_scope_values.iter_mut()
                                .find(|(n, _)| n == &entry.name)
                            {
                                existing.1 = carried.1.clone();
                            } else {
                                outer_scope_values.push((entry.name.clone(), carried.1.clone()));
                            }
                        }
                    }
                }
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

        let resolved_concurrency = if let Some(c) = resolved_cli_concurrency {
            c as usize
        } else {
            concurrency
        };

        let stanza_concurrency: usize = match params.get("stanza_concurrency").or_else(|| params.get("sc")) {
            Some(s) => s.parse().map_err(|_| format!("stanza_concurrency value '{s}' is not a valid integer"))?,
            None => 1,
        };

        let config = ActivityConfig {
            name: "main".into(),
            cycles,
            concurrency: resolved_concurrency,
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
        let stopped = run_activity(activity, adapter, program, openmetrics_url.as_deref(), sqlite_reporter.clone()).await;
        if stopped {
            return Err("activity stopped by error handler".into());
        }
    }

    if dry_run.is_some() {
        eprintln!("dry-run complete.");
    } else {
        eprintln!("done.");
    }

    // Print summary report from SQLite metrics
    if let Ok(mut guard) = sqlite_reporter.lock() {
        if let Some(ref mut reporter) = *guard {
            reporter.print_summary();
        }
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

/// Run an activity with metrics capture (SQLite + optional OpenMetrics push).
/// Returns true if the activity was stopped by an error handler.
async fn run_activity(
    activity: Activity,
    adapter: Arc<dyn crate::adapter::DriverAdapter>,
    program: Arc<nb_variates::kernel::GkProgram>,
    openmetrics_url: Option<&str>,
    sqlite: Arc<std::sync::Mutex<Option<nb_metrics::reporters::sqlite::SqliteReporter>>>,
) -> bool {
    let shared_metrics = activity.shared_metrics();
    let capture_interval = std::time::Duration::from_secs(1);
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let flag = running.clone();
    let url = openmetrics_url.map(|s| s.to_string());
    let sqlite_clone = sqlite.clone();
    std::thread::spawn(move || {
        use nb_metrics::scheduler::Reporter;
        let mut vm_reporter = url.map(|u| {
            nb_metrics::reporters::victoriametrics::VictoriaMetricsReporter::new(&u)
        });
        while flag.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(capture_interval);
            let frame = shared_metrics.capture(capture_interval);
            if let Some(ref mut vm) = vm_reporter {
                vm.report(&frame);
            }
            if let Ok(mut guard) = sqlite_clone.lock() {
                if let Some(ref mut sq) = *guard {
                    sq.report(&frame);
                }
            }
        }
    });
    let validation_frame = activity.validation_frame.clone();
    let stopped = activity.run_with_driver(adapter, program).await;
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(10));
    // Capture final validation metrics (recall, precision) to SQLite
    if let Some(frame) = validation_frame.lock().unwrap_or_else(|e| e.into_inner()).take() {
        use nb_metrics::scheduler::Reporter;
        if let Ok(mut guard) = sqlite.lock() {
            if let Some(ref mut sq) = *guard {
                sq.report(&frame);
            }
        }
    }
    stopped
}

/// Run an activity with multiple adapters and metrics capture.
/// Returns true if the activity was stopped by an error handler.
async fn run_activity_with_adapters(
    activity: Activity,
    adapters: std::collections::HashMap<String, Arc<dyn crate::adapter::DriverAdapter>>,
    default_adapter: &str,
    program: Arc<nb_variates::kernel::GkProgram>,
    openmetrics_url: Option<&str>,
    sqlite: Arc<std::sync::Mutex<Option<nb_metrics::reporters::sqlite::SqliteReporter>>>,
) -> bool {
    let shared_metrics = activity.shared_metrics();
    let capture_interval = std::time::Duration::from_secs(1);
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let flag = running.clone();
    let url = openmetrics_url.map(|s| s.to_string());
    let sqlite_clone = sqlite.clone();
    std::thread::spawn(move || {
        use nb_metrics::scheduler::Reporter;
        let mut vm_reporter = url.map(|u| {
            nb_metrics::reporters::victoriametrics::VictoriaMetricsReporter::new(&u)
        });
        while flag.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(capture_interval);
            let frame = shared_metrics.capture(capture_interval);
            if let Some(ref mut vm) = vm_reporter {
                vm.report(&frame);
            }
            if let Ok(mut guard) = sqlite_clone.lock() {
                if let Some(ref mut sq) = *guard {
                    sq.report(&frame);
                }
            }
        }
    });
    let validation_frame = activity.validation_frame.clone();
    let stopped = activity.run_with_adapters(adapters, default_adapter, program).await;
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(10));
    // Capture final validation metrics (recall, precision) to SQLite
    if let Some(frame) = validation_frame.lock().unwrap_or_else(|e| e.into_inner()).take() {
        use nb_metrics::scheduler::Reporter;
        if let Ok(mut guard) = sqlite.lock() {
            if let Some(ref mut sq) = *guard {
                sq.report(&frame);
            }
        }
    }
    stopped
}

/// Generate a session ID from the current timestamp.
fn chrono_session_id() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{secs}")
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
pub fn expand_gk_bindings(
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
    let mut values: Vec<String> = value_str.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    values.sort();

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

/// Collect all `{name}` param references from a workload's ops, phases,
/// and bindings. Returns the set of referenced param names.
fn collect_param_references(workload: &nb_workload::model::Workload) -> std::collections::HashSet<String> {
    let mut refs = std::collections::HashSet::new();

    let mut scan = |s: &str| {
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'{' {
                if let Some(end) = s[i + 1..].find('}') {
                    let name = &s[i + 1..i + 1 + end];
                    if !name.is_empty()
                        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                        && !name.bytes().next().unwrap().is_ascii_digit()
                    {
                        refs.insert(name.to_string());
                    }
                    i += end + 2;
                } else {
                    break;
                }
            } else {
                i += 1;
            }
        }
    };

    // Scan ops
    for op in &workload.ops {
        for value in op.op.values() {
            if let serde_json::Value::String(s) = value {
                scan(s);
            }
        }
        match &op.bindings {
            nb_workload::model::BindingsDef::GkSource(s) => scan(s),
            nb_workload::model::BindingsDef::Map(m) => {
                for v in m.values() { scan(v); }
            }
        }
    }

    // Scan phases
    for phase in workload.phases.values() {
        if let Some(s) = &phase.cycles { scan(s); }
        if let Some(s) = &phase.concurrency { scan(s); }
        if let Some(s) = &phase.for_each { scan(s); }
        for op in &phase.ops {
            for value in op.op.values() {
                if let serde_json::Value::String(s) = value {
                    scan(s);
                }
            }
            match &op.bindings {
                nb_workload::model::BindingsDef::GkSource(s) => scan(s),
                nb_workload::model::BindingsDef::Map(m) => {
                    for v in m.values() { scan(v); }
                }
            }
        }
    }

    // Scan scenario nodes for for_each specs
    fn scan_scenario_nodes(nodes: &[nb_workload::model::ScenarioNode], refs: &mut std::collections::HashSet<String>) {
        for node in nodes {
            match node {
                nb_workload::model::ScenarioNode::Phase(_) => {}
                nb_workload::model::ScenarioNode::ForEach { spec, children } => {
                    // Scan the for_each expression for param references
                    let bytes = spec.as_bytes();
                    let mut i = 0;
                    while i < bytes.len() {
                        if bytes[i] == b'{' {
                            if let Some(end) = spec[i + 1..].find('}') {
                                let name = &spec[i + 1..i + 1 + end];
                                if !name.is_empty()
                                    && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                                    && !name.bytes().next().unwrap().is_ascii_digit()
                                {
                                    refs.insert(name.to_string());
                                }
                                i += end + 2;
                            } else { break; }
                        } else { i += 1; }
                    }
                    scan_scenario_nodes(children, refs);
                }
            }
        }
    }
    for nodes in workload.scenarios.values() {
        scan_scenario_nodes(nodes, &mut refs);
    }

    refs
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
    scenarios: &HashMap<String, Vec<nb_workload::model::ScenarioNode>>,
    phase_order: &[String],
    name: &str,
) -> Result<Vec<nb_workload::model::ScenarioNode>, String> {
    if let Some(nodes) = scenarios.get(name) {
        return Ok(nodes.clone());
    }
    if name == "default" && !phase_order.is_empty() {
        return Ok(phase_order.iter()
            .map(|n| nb_workload::model::ScenarioNode::Phase(n.clone()))
            .collect());
    }
    Err(format!("scenario '{name}' not found"))
}

/// Format a scenario tree for display.
fn format_scenario_tree(nodes: &[nb_workload::model::ScenarioNode]) -> String {
    let parts: Vec<String> = nodes.iter().map(|n| match n {
        nb_workload::model::ScenarioNode::Phase(name) => name.clone(),
        nb_workload::model::ScenarioNode::ForEach { spec, children } => {
            let inner = format_scenario_tree(children);
            let var = spec.splitn(2, " in ").next().unwrap_or("?");
            format!("for_each {var}: [{inner}]")
        }
    }).collect();
    parts.join(", ")
}

/// Resolve a for_each expression with workload params and accumulated outer bindings.
fn resolve_for_each_expr(
    spec: &str,
    workload_params: &HashMap<String, String>,
    outer_bindings: &HashMap<String, String>,
    outer_scope_values: &[(String, nb_variates::node::Value)],
) -> Result<(String, Vec<String>), String> {
    let parts: Vec<&str> = spec.splitn(2, " in ").collect();
    if parts.len() != 2 {
        return Err(format!("invalid for_each syntax: '{spec}'. Expected 'var in expr'"));
    }
    let var_name = parts[0].trim().to_string();
    let mut expr = parts[1].trim().to_string();

    // Substitute workload params
    for (k, v) in workload_params {
        expr = expr.replace(&format!("{{{k}}}"), v);
    }
    // Substitute accumulated outer iteration bindings
    for (k, v) in outer_bindings {
        expr = expr.replace(&format!("{{{k}}}"), v);
    }

    // Resolve: try GK const expr, then kernel constants, then treat as literal
    let value_str = if let Some(val) = outer_scope_values.iter().find(|(n, _)| n == &expr) {
        val.1.to_display_string()
    } else {
        match nb_variates::dsl::compile::eval_const_expr(&expr) {
            Ok(val) => val.to_display_string(),
            Err(_) => expr.clone(), // literal comma-separated fallback
        }
    };

    let mut values: Vec<String> = value_str.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    values.sort();

    Ok((var_name, values))
}

/// Recursively flatten a scenario tree into a linear execution plan.
///
/// Each leaf (Phase) produces one entry with the accumulated bindings
/// from all enclosing ForEach levels. Phase-level for_each is lifted
/// into the tree as an additional ForEach wrapping that phase.
fn flatten_scenario_tree(
    nodes: &[nb_workload::model::ScenarioNode],
    phases: &HashMap<String, nb_workload::model::WorkloadPhase>,
    workload_params: &HashMap<String, String>,
    outer_scope_values: &[(String, nb_variates::node::Value)],
    phase_iterations: &HashMap<String, Vec<HashMap<String, String>>>,
    outer_bindings: &HashMap<String, String>,
) -> Result<Vec<(String, Vec<HashMap<String, String>>)>, String> {
    let mut plan = Vec::new();

    for node in nodes {
        match node {
            nb_workload::model::ScenarioNode::Phase(name) => {
                // Check if this phase has its own for_each — lift it
                let phase = phases.get(name);
                let phase_for_each = phase.and_then(|p| p.for_each.as_ref());

                if let Some(spec) = phase_for_each {
                    // Phase has its own for_each: resolve with outer bindings
                    let (var, values) = resolve_for_each_expr(
                        spec, workload_params, outer_bindings, outer_scope_values,
                    )?;
                    eprintln!("  for_each {var} in [{}] ({} iterations)",
                        values.join(", "), values.len());
                    for value in &values {
                        let mut bindings = outer_bindings.clone();
                        bindings.insert(var.clone(), value.clone());
                        plan.push((name.clone(), vec![bindings]));
                    }
                } else if !outer_bindings.is_empty() {
                    // Inside a for_each group: use accumulated bindings
                    plan.push((name.clone(), vec![outer_bindings.clone()]));
                } else {
                    // Standalone phase: use pre-resolved iterations
                    let iterations = phase_iterations.get(name)
                        .cloned()
                        .unwrap_or_else(|| vec![HashMap::new()]);
                    plan.push((name.clone(), iterations));
                }
            }
            nb_workload::model::ScenarioNode::ForEach { spec, children } => {
                let (var, values) = resolve_for_each_expr(
                    spec, workload_params, outer_bindings, outer_scope_values,
                )?;
                eprintln!("for_each {var} in [{}] ({} iterations × {} children)",
                    values.join(", "), values.len(), children.len());

                for value in &values {
                    let mut inner_bindings = outer_bindings.clone();
                    inner_bindings.insert(var.clone(), value.clone());
                    // Recurse into children with accumulated bindings
                    let child_plan = flatten_scenario_tree(
                        children, phases, workload_params,
                        outer_scope_values, phase_iterations,
                        &inner_bindings,
                    )?;
                    plan.extend(child_plan);
                }
            }
        }
    }

    Ok(plan)
}

/// Normalize args: detect scenario shorthand where a bare word after
/// the workload file becomes `scenario=<name>`.
pub fn normalize_args(args: &[String]) -> Vec<String> {
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
        // Strip leading dashes: --dryrun=phase,gk → dryrun=phase,gk
        let stripped = arg.trim_start_matches('-');
        if let Some(eq_pos) = stripped.find('=') {
            let key = stripped[..eq_pos].to_string();
            let value = stripped[eq_pos + 1..].to_string();
            params.insert(key, value);
        } else if arg.ends_with(".yaml") || arg.ends_with(".yml") {
            // Workload file path — handled elsewhere
        } else {
            eprintln!("error: unrecognized argument '{arg}'. Expected key=value format.");
            std::process::exit(1);
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

// =========================================================================
// GK Scope Composition (sysref 16)
// =========================================================================

/// Output manifest entry: name + type + modifier for a scope's outputs.
#[derive(Debug, Clone)]
struct ManifestEntry {
    name: String,
    port_type: nb_variates::node::PortType,
    modifier: nb_variates::dsl::ast::BindingModifier,
}

/// Extract the output manifest from a compiled GK program.
/// Returns entries for every output in declaration order.
fn extract_manifest(program: &nb_variates::kernel::GkProgram) -> Vec<ManifestEntry> {
    (0..program.output_count())
        .map(|i| {
            let name = program.output_name(i).to_string();
            let (ni, pi) = program.resolve_output_by_index(i);
            let port_type = program.node_meta(ni).outs[pi].typ;
            let modifier = program.output_modifier(&name);
            ManifestEntry { name, port_type, modifier }
        })
        .collect()
}

/// Generate `extern` declarations for names in the outer manifest
/// that are referenced in the inner ops but not defined in the inner
/// bindings. Returns the extern source text to prepend.
fn generate_auto_externs(
    inner_ops: &[nb_workload::model::ParsedOp],
    outer_manifest: &[ManifestEntry],
) -> Result<String, String> {
    // Collect names defined in the inner bindings
    let mut inner_defined: std::collections::HashSet<String> = std::collections::HashSet::new();
    for op in inner_ops {
        if let nb_workload::model::BindingsDef::GkSource(ref src) = op.bindings {
            for line in src.lines() {
                let trimmed = line.trim();
                // Skip comments and blank lines
                if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("/*") {
                    continue;
                }
                // Extract binding name from "name := ..." or "inputs := ..."
                if let Some(pos) = trimmed.find(":=") {
                    let lhs = trimmed[..pos].trim();
                    // Handle destructuring: (a, b) := ...
                    if lhs.starts_with('(') && lhs.ends_with(')') {
                        let inner = &lhs[1..lhs.len()-1];
                        for part in inner.split(',') {
                            inner_defined.insert(part.trim().to_string());
                        }
                    } else if lhs == "inputs" || lhs == "coordinates" {
                        // inputs declaration — extract coordinate names
                        let rhs = trimmed[pos+2..].trim();
                        if rhs.starts_with('(') {
                            let names = rhs.trim_start_matches('(').trim_end_matches(')');
                            for name in names.split(',') {
                                inner_defined.insert(name.trim().to_string());
                            }
                        }
                    } else if lhs.starts_with("extern") {
                        // extern declarations — not inner-defined bindings
                    } else if lhs.starts_with("shared ") || lhs.starts_with("final ") {
                        // shared/final prefix: extract the actual name
                        let name = lhs.split_whitespace().nth(1).unwrap_or("");
                        if !name.is_empty() {
                            inner_defined.insert(name.to_string());
                        }
                    } else if lhs == "init" || lhs.starts_with("init ") {
                        let name = lhs.split_whitespace().nth(1).unwrap_or("");
                        if !name.is_empty() {
                            inner_defined.insert(name.to_string());
                        }
                    } else {
                        inner_defined.insert(lhs.to_string());
                    }
                }
            }
        }
    }

    // Collect names referenced in op templates
    let mut referenced: std::collections::HashSet<String> = std::collections::HashSet::new();
    for op in inner_ops {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                for name in nb_workload::bindpoints::referenced_bindings(s) {
                    referenced.insert(name);
                }
            }
        }
        // Also check condition and delay fields
        if let Some(ref cond) = op.condition {
            let bare = cond.trim().strip_prefix('{').and_then(|s| s.strip_suffix('}')).unwrap_or(cond.trim());
            referenced.insert(bare.to_string());
        }
    }

    // Check for final shadowing violations: inner scope must not
    // redefine names that are `final` in the outer scope
    for entry in outer_manifest {
        if entry.modifier == nb_variates::dsl::ast::BindingModifier::Final
            && inner_defined.contains(&entry.name)
        {
            return Err(format!(
                "cannot shadow 'final' binding '{}' from outer scope",
                entry.name
            ));
        }
    }

    // Generate extern declarations for outer names that are
    // referenced but not defined in the inner scope
    let mut externs = String::new();
    for entry in outer_manifest {
        if referenced.contains(&entry.name) && !inner_defined.contains(&entry.name) {
            let type_name = match entry.port_type {
                nb_variates::node::PortType::U64 => "u64",
                nb_variates::node::PortType::F64 => "f64",
                nb_variates::node::PortType::Str => "String",
                nb_variates::node::PortType::Bool => "bool",
                _ => "String",
            };
            externs.push_str(&format!("extern {}: {}\n", entry.name, type_name));
        }
    }
    Ok(externs)
}


