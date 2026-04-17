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
use nb_metrics::scheduler::Reporter;
use nb_workload::tags::TagFilter;

/// Known `key=value` params accepted by the shared runner.
/// Adapter-specific params are discovered from inventory registrations.
const KNOWN_PARAMS: &[&str] = &[
    // Activity-level
    "adapter", "driver", "workload", "op", "cycles", "concurrency",
    "rate", "stanzarate", "errors", "seq", "tags", "format",
    "filename", "separator", "header", "color",
    "stanza_concurrency", "sc", "scenario", "dryrun", "summary", "metrics", "limit", "profiler", "tui",
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
    /// Emit dimensional labels for all phases.
    pub show_labels: bool,
}

impl DiagnosticConfig {
    /// Normal execution, no diagnostics.
    pub fn normal() -> Self {
        Self { depth: ExecDepth::Full, explain_gk: false, show_labels: false }
    }

    /// Parse from `dryrun=` value (e.g., "phase,gk" or "cycle").
    /// If no depth flag (phase/cycle/full) is given, defaults to `Phase`.
    pub fn parse(spec: &str) -> Self {
        let mut config = Self::normal();
        let mut depth_set = false;
        for flag in spec.split(',') {
            match flag.trim() {
                "phase" => { config.depth = ExecDepth::Phase; depth_set = true; }
                "cycle" => { config.depth = ExecDepth::Cycle; depth_set = true; }
                "full" => { config.depth = ExecDepth::Full; depth_set = true; }
                "gk" => config.explain_gk = true,
                "labels" => config.show_labels = true,
                _ => eprintln!("warning: unknown dryrun flag '{flag}'"),
            }
        }
        // Default to phase depth if no explicit depth was given
        if !depth_set {
            config.depth = ExecDepth::Phase;
        }
        config
    }
}


pub async fn run(args: &[String]) -> Result<(), String> {
    run_with_observer(args, Arc::new(crate::observer::StderrObserver)).await
}

/// Run with a custom observer for phase lifecycle events.
/// The TUI persona uses this to inject a TuiObserver that updates
/// the display state instead of printing to stderr.
pub async fn run_with_observer(
    args: &[String],
    observer: Arc<dyn crate::observer::RunObserver>,
) -> Result<(), String> {
    let args: &[String] = match args.first().map(|s| s.as_str()) {
        Some("run") => &args[1..],
        // Reject unknown subcommands — don't silently fall through to execution
        Some(cmd) if !cmd.contains('=') && !cmd.ends_with(".yaml") && !cmd.ends_with(".yml") => {
            return Err(format!("unknown command '{cmd}'. Use 'run' or pass a workload file."));
        }
        _ => args,
    };
    run_impl(args, observer).await
}

/// Core runner. Diagnostic mode is controlled by `dryrun=` param.
async fn run_impl(args: &[String], observer: Arc<dyn crate::observer::RunObserver>) -> Result<(), String> {
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
        let workload_raw = params.get("workload")
            .cloned()
            .or_else(|| args.iter()
                .find(|a| a.ends_with(".yaml") || a.ends_with(".yml"))
                .cloned()
            )
            .ok_or("no workload specified. Use workload=file.yaml or op=\"...\"")?;

        // Resolve bare names: try as-is, then with .yaml/.yml, then under workloads/
        let workload_path = resolve_workload_file(&workload_raw)
            .ok_or_else(|| format!("workload not found: '{workload_raw}'"))?;

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
    let workload_summary = workload.summary.or_else(|| {
        // Sidecar: if the workload has no summary: field, look for
        // summary.yaml in the same directory as the workload file.
        let wf = workload_file.as_deref()?;
        let dir = std::path::Path::new(wf).parent()?;
        let sidecar = dir.join("summary.yaml");
        let content = std::fs::read_to_string(&sidecar).ok()?;
        let trimmed = content.trim();
        if trimmed.is_empty() { return None; }
        Some(nb_workload::model::SummaryConfig::parse(trimmed))
    });

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
        if !observer.suppresses_stderr() {
            eprintln!("{} phases, {} top-level ops, adapter={}",
                phases.len(), ops.len(), driver);
        }
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

    // SQLite metrics capture — always a unique session-coded filename.
    // A symlink `metrics.db` always points to the latest session.
    let session_id = chrono_session_id();
    let sqlite_path = format!("nb-metrics-{session_id}.db");
    // Create/update symlink to latest
    let _ = std::fs::remove_file("metrics.db");
    let _ = std::os::unix::fs::symlink(&sqlite_path, "metrics.db");
    let sqlite_reporter = nb_metrics::reporters::sqlite::SqliteReporter::new(&sqlite_path)
        .map(|mut r| {
            r.set_metadata("workload", workload_file.as_deref().unwrap_or("inline"));
            r.set_metadata("start_time", &format!("{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()));
            for (k, v) in &merged_params {
                r.set_metadata(&format!("param.{k}"), v);
            }
            if !observer.suppresses_stderr() {
                eprintln!("metrics: {sqlite_path}");
            }
            r
        })
        .map_err(|e| eprintln!("warning: SQLite metrics disabled: {e}"))
        .ok();
    let sqlite_reporter = std::sync::Arc::new(std::sync::Mutex::new(sqlite_reporter));

    // Merge all ops for param expansion and GK compilation.
    let _num_top_level_ops = ops.len();
    let mut all_ops_for_compile: Vec<nb_workload::model::ParsedOp> = ops;
    all_ops_for_compile.extend(phase_ops_for_compile);

    // === Text Substitution (before scope ingestion) ===
    // Replace {param} placeholders in GK source and op templates.
    // Param injection and inline expr extraction are handled by
    // BindingScope inside compile_bindings_with_libs_excluding.
    crate::scope::substitute_workload_params(&mut all_ops_for_compile, &workload_params);
    crate::scope::rewrite_inline_exprs(&mut all_ops_for_compile);

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

    // Parse limit param for cursor clamping
    let cursor_limit: Option<u64> = merged_params.get("limit")
        .and_then(|s| s.parse().ok());

    let kernel = compile_bindings_with_libs_excluding(
        &all_ops_for_compile, workload_dir, gk_lib_paths.clone(), strict, &[], &config_refs,
        "outer workload bindings", cursor_limit,
    ).map_err(|e| format!("outer workload bindings: {e}"))?;

    // Extract output manifest and folded constant values from outer kernel
    // for scope composition (sysref 16). Must be done before kernel is
    // consumed by OpBuilder.
    let outer_manifest = extract_manifest(kernel.program());
    let outer_scope_values: Vec<(String, nb_variates::node::Value)> = outer_manifest.iter()
        .filter_map(|entry| {
            kernel.get_constant(&entry.name)
                .map(|v| (entry.name.clone(), v.clone()))
        })
        .collect();
    // Original snapshot for loop_scope: clean (unaffected by shared write-back)
    let _original_scope_values = outer_scope_values.clone();

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
                nb_workload::model::ScenarioNode::ForEach { children, .. }
                | nb_workload::model::ScenarioNode::DoWhile { children, .. }
                | nb_workload::model::ScenarioNode::DoUntil { children, .. }
                | nb_workload::model::ScenarioNode::ForCombinations { children, .. } => {
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

        if !observer.suppresses_stderr() {
            eprintln!("scenario '{scenario_name}': {}", format_scenario_tree(&scenario_nodes));
        }

        // Build the centralized metrics scheduler with component tree.
        let store = std::sync::Arc::new(std::sync::RwLock::new(
            nb_metrics::store::InProcessMetricsStore::new()
        ));
        // Make store accessible to GK metric() / metric_window() nodes.
        nb_variates::nodes::metrics::set_global_store(store.clone());
        let session_component = nb_metrics::component::Component::root(
            nb_metrics::labels::Labels::of("session", &session_id),
            std::collections::HashMap::new(),
        );

        let session_for_capture = session_component.clone();
        let mut sched_builder = nb_metrics::scheduler::SchedulerBuilder::new()
            .base_interval(std::time::Duration::from_secs(1))
            .with_store(store.clone());

        // Register SQLite reporter at 10s cadence
        if let Ok(guard) = sqlite_reporter.lock() {
            if guard.is_some() {
                drop(guard);
                let sqlite_for_sched = sqlite_reporter.clone();
                sched_builder = sched_builder.add_reporter(
                    std::time::Duration::from_secs(10),
                    MutexReporter(sqlite_for_sched),
                );
            }
        }

        // Register observer's reporter (e.g., TUI metrics feed)
        if let Some(obs_reporter) = observer.reporter() {
            sched_builder = sched_builder.add_reporter(
                std::time::Duration::from_secs(1),
                BoxedReporter(obs_reporter),
            );
        }

        let scheduler = sched_builder.build(Box::new(move || {
            nb_metrics::component::capture_tree(
                &session_for_capture,
                std::time::Duration::from_secs(1),
            )
        }));
        let stop_handle = Arc::new(scheduler.start());

        // Start profiler if requested (profiler=flamegraph or profiler=perf)
        let _profiler = crate::profiler::ProfileGuard::maybe_start(&merged_params);

        // Observer is passed from the caller (default: StderrObserver).

        // Execute the scenario tree recursively via the executor module.
        {
            let dry_run_static: Option<&'static str> = match dry_run {
                Some("silent") => Some("silent"),
                Some("emit") => Some("emit"),
                _ => None,
            };
            let mut exec_ctx = crate::executor::ExecCtx {
                phases: phases.clone(),
                workload_params: workload_params.clone(),
                outer_scope_values: outer_scope_values.clone(),
                outer_manifest: outer_manifest.clone(),
                program: program.clone(),
                gk_lib_paths: gk_lib_paths.clone(),
                workload_dir: workload_dir.map(|p| p.to_path_buf()),
                strict,
                driver: driver.clone(),
                merged_params: merged_params.clone(),
                dry_run: dry_run_static,
                diag: diag.clone(),
                openmetrics_url: openmetrics_url.clone(),
                seq_type,
                concurrency,
                cycle_rate,
                error_spec: error_spec.clone(),
                session_id: session_id.clone(),
                label_stack: Vec::new(),
                session_component: session_component.clone(),
                store: store.clone(),
                stop_handle: stop_handle.clone(),
                observer: observer.clone(),
            };
            crate::executor::execute_tree(
                &mut exec_ctx,
                &scenario_nodes,
                &HashMap::new(),
            ).await?;
        }

        // Stop profiler and write flamegraph before scheduler flush
        if let Some(profiler) = _profiler {
            profiler.finish();
        }

        // Stop the scheduler (flushes all reporters).
        // ExecCtx is dropped, so this is the last Arc reference.
        if let Ok(mut sh) = Arc::try_unwrap(stop_handle) {
            sh.stop();
        }
        observer.run_finished();

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
            source_factory: None,
            suppress_status_line: observer.suppresses_stderr(),
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
        if !observer.suppresses_stderr() { eprintln!("dry-run complete."); }
    } else {
        if !observer.suppresses_stderr() { eprintln!("done."); }
    }

    // Print summary report.
    // Summary only prints when a config is present (workload-level or CLI override).
    let summary_config = if let Some(cli_summary) = merged_params.get("summary") {
        Some(nb_workload::model::SummaryConfig::parse(cli_summary))
    } else {
        workload_summary
    };
    if let Some(ref config) = summary_config {
        let report_config = nb_metrics::reporters::sqlite::ReportConfig {
            columns: config.columns.clone(),
            row_filters: config.row_filters.clone(),
            aggregates: config.aggregates.iter().map(|a| {
                nb_metrics::reporters::sqlite::ReportAggregate {
                    function: a.function.to_string(),
                    column_pattern: a.column_pattern.clone(),
                    label_key: a.label_key.clone(),
                    label_pattern: a.label_pattern.clone(),
                }
            }).collect(),
            show_details: config.show_details,
        };
        // Summary report always comes from SQLite — the durable record.
        // The in-memory store exists for GK access and reactive control,
        // not for reporting.
        if let Ok(mut guard) = sqlite_reporter.lock() {
            if let Some(ref mut reporter) = *guard {
                reporter.print_summary(&report_config);
            }
        }
    }

    Ok(())
}

/// Create an adapter from inventory registrations.
/// Create an adapter, respecting dry-run mode.
pub async fn create_adapter(
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
    let final_metrics = activity.shared_metrics();
    let activity_start = std::time::Instant::now();
    let stopped = activity.run_with_driver(adapter, program).await;
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(10));
    // Final capture: ensures short activities get at least one sample
    {
        use nb_metrics::scheduler::Reporter;
        let frame = final_metrics.capture(activity_start.elapsed());
        if let Ok(mut guard) = sqlite.lock() {
            if let Some(ref mut sq) = *guard {
                sq.report(&frame);
            }
        }
    }
    // Capture validation metrics (recall, precision) to SQLite
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

/// Run an activity without its own capture thread.
///
/// The centralized scheduler handles all metrics capture. This function
/// just runs the activity to completion. Lifecycle flush (final delta +
/// validation metrics) is handled by the caller (executor).
pub async fn run_activity_simple(
    activity: Activity,
    adapters: std::collections::HashMap<String, Arc<dyn crate::adapter::DriverAdapter>>,
    default_adapter: &str,
    program: Arc<nb_variates::kernel::GkProgram>,
) -> bool {
    activity.run_with_adapters(adapters, default_adapter, program).await
}

/// Run an activity with multiple adapters and metrics capture.
/// Returns true if the activity was stopped by an error handler.
pub async fn run_activity_with_adapters(
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
    let final_metrics = activity.shared_metrics();
    let activity_start = std::time::Instant::now();
    let stopped = activity.run_with_adapters(adapters, default_adapter, program).await;
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(10));
    // Final capture: ensures short activities get at least one sample
    {
        use nb_metrics::scheduler::Reporter;
        let frame = final_metrics.capture(activity_start.elapsed());
        if let Ok(mut guard) = sqlite.lock() {
            if let Some(ref mut sq) = *guard {
                sq.report(&frame);
            }
        }
    }
    // Capture validation metrics (recall, precision) to SQLite
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

/// Adapter that delegates to an `Arc<Mutex<Option<SqliteReporter>>>`.
///
/// Allows the SQLite reporter to be registered on the scheduler while
/// also being accessible for summary queries after the scheduler stops.
struct MutexReporter(std::sync::Arc<std::sync::Mutex<Option<nb_metrics::reporters::sqlite::SqliteReporter>>>);

impl Reporter for MutexReporter {
    fn report(&mut self, frame: &nb_metrics::frame::MetricsFrame) {
        if let Ok(mut guard) = self.0.lock() {
            if let Some(ref mut r) = *guard {
                Reporter::report(r, frame);
            }
        }
    }

    fn flush(&mut self) {
        if let Ok(mut guard) = self.0.lock() {
            if let Some(ref mut r) = *guard {
                Reporter::flush(r);
            }
        }
    }
}

/// Wrapper to make `Box<dyn Reporter>` usable with `add_reporter(impl Reporter)`.
struct BoxedReporter(Box<dyn Reporter>);
impl Reporter for BoxedReporter {
    fn report(&mut self, frame: &nb_metrics::frame::MetricsFrame) {
        self.0.report(frame);
    }
    fn flush(&mut self) {
        self.0.flush();
    }
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
            Err(_) => expr.clone(), // literal comma-separated fallback
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
pub fn expand_workload_params(s: &str, params: &HashMap<String, String>) -> String {
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
                nb_workload::model::ScenarioNode::DoWhile { children, .. }
                | nb_workload::model::ScenarioNode::DoUntil { children, .. } => {
                    scan_scenario_nodes(children, refs);
                }
                nb_workload::model::ScenarioNode::ForCombinations { specs, children } => {
                    // Scan each combination expression for param references
                    for (_, expr) in specs {
                        let bytes = expr.as_bytes();
                        let mut i = 0;
                        while i < bytes.len() {
                            if bytes[i] == b'{' {
                                if let Some(end) = expr[i + 1..].find('}') {
                                    let name = &expr[i + 1..i + 1 + end];
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
pub fn resolve_gk_config(value: &str, kernel: &nb_variates::kernel::GkKernel) -> Option<u64> {
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
        nb_workload::model::ScenarioNode::DoWhile { condition, counter, children } => {
            let inner = format_scenario_tree(children);
            let ctr = counter.as_deref().map(|c| format!(" ({c})")).unwrap_or_default();
            format!("do_while '{condition}'{ctr}: [{inner}]")
        }
        nb_workload::model::ScenarioNode::DoUntil { condition, counter, children } => {
            let inner = format_scenario_tree(children);
            let ctr = counter.as_deref().map(|c| format!(" ({c})")).unwrap_or_default();
            format!("do_until '{condition}'{ctr}: [{inner}]")
        }
        nb_workload::model::ScenarioNode::ForCombinations { specs, children } => {
            let inner = format_scenario_tree(children);
            let vars: Vec<&str> = specs.iter().map(|(v, _)| v.as_str()).collect();
            format!("for_combinations [{}]: [{inner}]", vars.join(", "))
        }
    }).collect();
    parts.join(", ")
}


/// Resolve a workload file path from a bare name.
/// Tries: as-is, with .yaml/.yml extension, then under workloads/.
fn resolve_workload_file(name: &str) -> Option<String> {
    let p = std::path::Path::new(name);
    if p.exists() { return Some(name.to_string()); }

    // Already has yaml extension — no further search
    if name.ends_with(".yaml") || name.ends_with(".yml") {
        // Try under workloads/
        let under = format!("workloads/{name}");
        if std::path::Path::new(&under).exists() { return Some(under); }
        return None;
    }

    // Try adding extensions
    for ext in [".yaml", ".yml"] {
        let with_ext = format!("{name}{ext}");
        if std::path::Path::new(&with_ext).exists() { return Some(with_ext); }
    }

    // Try under workloads/
    for ext in ["", ".yaml", ".yml"] {
        let under = format!("workloads/{name}{ext}");
        if std::path::Path::new(&under).exists() { return Some(under); }
    }

    None
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
pub struct ManifestEntry {
    pub name: String,
    pub port_type: nb_variates::node::PortType,
    pub modifier: nb_variates::dsl::ast::BindingModifier,
}

/// Extract the output manifest from a compiled GK program.
/// Returns entries for every output in declaration order.
pub fn extract_manifest(program: &nb_variates::kernel::GkProgram) -> Vec<ManifestEntry> {
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

/// Build the gauge column filter from phase-level `summary:` configs.
///
/// Returns `None` if any phase uses `summary: true` (show all gauges),
/// or `Some(patterns)` with the union of all declared gauge name patterns.
/// An empty Vec means no gauge columns should appear.
pub fn build_gauge_filter(
    phases: &HashMap<String, nb_workload::model::WorkloadPhase>,
) -> Option<Vec<String>> {
    let mut patterns: Vec<String> = Vec::new();
    let mut any_open = false;
    for phase in phases.values() {
        match &phase.summary {
            None => {} // excluded from summary
            Some(v) if v.is_boolean() => {
                // summary: true — show all gauge columns
                any_open = true;
            }
            Some(v) if v.is_array() => {
                // summary: ["recall", "precision"] — show only matching
                if let Some(arr) = v.as_array() {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            if !patterns.contains(&s.to_string()) {
                                patterns.push(s.to_string());
                            }
                        }
                    }
                }
            }
            Some(v) if v.is_string() => {
                // summary: "recall" — single pattern
                if let Some(s) = v.as_str() {
                    if !patterns.contains(&s.to_string()) {
                        patterns.push(s.to_string());
                    }
                }
            }
            _ => { any_open = true; }
        }
    }
    if any_open { None } else { Some(patterns) }
}

