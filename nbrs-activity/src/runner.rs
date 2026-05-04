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
use nbrs_metrics::labels::Labels;
use nbrs_metrics::scheduler::Reporter;
use nbrs_workload::tags::TagFilter;

/// Known `key=value` params accepted by the shared runner.
/// Adapter-specific params are discovered from inventory registrations.
pub const KNOWN_PARAMS: &[&str] = &[
    // Activity-level
    "adapter", "driver", "workload", "op", "cycles", "concurrency",
    "rate", "errors", "seq", "tags", "format",
    "filename", "separator", "header", "color",
    "stanza_concurrency", "sc", "scenario", "dryrun", "summary", "metrics", "limit",
    "profiler", "profiler_callgraph", "tui",
    "latency-cadences", "latency_cadences",
    "jobname", "instance", "prompush_apikeyfile",
    "resume", "resume_latest", "force_retry_failed",
];

/// Convert the workload-model `SummaryConfig` (parsed from the
/// `summary:` workload field or the `--summary` CLI flag) into
/// the SQLite reporter's `ReportConfig`. Used by both the
/// in-run summary path (workload finished, render to
/// `summary.md`) and the standalone `nbrs --summary` command,
/// so both produce identical output for the same spec.
pub fn report_config_from_summary(
    config: &nbrs_workload::model::SummaryConfig,
) -> nbrs_metrics::reporters::sqlite::ReportConfig {
    nbrs_metrics::reporters::sqlite::ReportConfig {
        columns: config.columns.clone(),
        row_filters: config.row_filters.clone(),
        aggregates: config.aggregates.iter().map(|a| {
            nbrs_metrics::reporters::sqlite::ReportAggregate {
                function: a.function.to_string(),
                column_pattern: a.column_pattern.clone(),
                label_key: a.label_key.clone(),
                label_pattern: a.label_pattern.clone(),
                group_by: a.group_by.clone(),
            }
        }).collect(),
        show_details: config.show_details,
    }
}

/// Try to resolve a workload name (bare or with extension) to an
/// actual file path, searching the current directory and
/// `./workloads/`. Returns `None` if nothing matches.
///
/// Exposed for shell-completion tooling. Application code should
/// just use [`run_with_observer`] which calls this internally.
pub fn resolve_workload_file_public(name: &str) -> Option<String> {
    resolve_workload_file(name)
}

/// List the scenario names declared at the top level of a workload
/// YAML file. Used by shell-completion tooling to offer
/// `scenario=<tab>` suggestions. Returns an empty vector on any
/// parse error — completion is best-effort, not a hard check.
pub fn scenarios_in_workload_file(path: &str) -> Vec<String> {
    let Ok(src) = std::fs::read_to_string(path) else { return Vec::new() };
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&src) else { return Vec::new() };
    let Some(scenarios) = doc.get("scenarios") else { return Vec::new() };
    let Some(map) = scenarios.as_mapping() else { return Vec::new() };
    map.keys()
        .filter_map(|k| k.as_str().map(String::from))
        .collect()
}

/// Run a workload. Adapters are discovered from link-time inventory
/// registrations — the calling binary just needs to link the adapter
/// crates it wants available.
/// Execution depth: how far through the pipeline to go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    /// Walk the post-construction component tree, render every
    /// declared dynamic control, and exit. SRD 23 §"Enumeration:
    /// controls are structural".
    pub list_controls: bool,
}

impl DiagnosticConfig {
    /// Normal execution, no diagnostics.
    pub fn normal() -> Self {
        Self {
            depth: ExecDepth::Full,
            explain_gk: false,
            show_labels: false,
            list_controls: false,
        }
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
                "controls" => {
                    // Implies an early exit before any phase
                    // runs — `controls` is a discovery dump, not
                    // an execution mode.
                    config.list_controls = true;
                    config.depth = ExecDepth::Phase;
                    depth_set = true;
                }
                _ => crate::diag!(crate::observer::LogLevel::Warn, "warning: unknown dryrun flag '{flag}'"),
            }
        }
        // Default to phase depth if no explicit depth was given
        if !depth_set {
            config.depth = ExecDepth::Phase;
        }
        config
    }
}

/// Walk a component subtree and print every declared control
/// (name, type, current value, scope, final flag, applier
/// count) in a stable order. Used by `dryrun=controls` and any
/// other discovery-style call site.
pub fn render_controls_tree(
    root: &std::sync::Arc<std::sync::RwLock<nbrs_metrics::component::Component>>,
    out: &mut dyn std::io::Write,
) -> std::io::Result<()> {
    use nbrs_metrics::component::find;
    use nbrs_metrics::selector::Selector;

    writeln!(out, "Declared dynamic controls (SRD 23):")?;
    let all = find(root, &Selector::new());
    let mut entries: Vec<(String, String, String, String, String, String)> = Vec::new();
    for comp in all {
        let guard = match comp.read() {
            Ok(g) => g,
            Err(_) => continue,
        };
        let path = guard.effective_labels()
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        for ctl in guard.controls().list() {
            let scope = match ctl.branch_scope() {
                nbrs_metrics::controls::BranchScope::Local => "local",
                nbrs_metrics::controls::BranchScope::Subtree => "subtree",
            };
            let final_marker = match ctl.final_scope() {
                Some(s) => format!("final@{s}"),
                None => "-".to_string(),
            };
            entries.push((
                if path.is_empty() { "<root>".into() } else { path.clone() },
                ctl.name().to_string(),
                ctl.value_type_name().to_string(),
                ctl.value_string(),
                format!("scope={scope}, {final_marker}, appliers={}", ctl.applier_count()),
                if ctl.accepts_f64_writes() { "f64-writable".into() } else { "no-f64".into() },
            ));
        }
    }
    if entries.is_empty() {
        writeln!(out, "  (no controls declared)")?;
        return Ok(());
    }
    entries.sort();
    for (path, name, ty, value, meta, write) in entries {
        writeln!(
            out,
            "  {path}\n    {name}: {value}  [{ty}]  {meta}  {write}",
        )?;
    }
    Ok(())
}


pub async fn run(args: &[String]) -> Result<(), String> {
    // Default tui=off observer — stderr with the same Info-level
    // filter the TUI's log panel applies by default. `loglevel=`
    // CLI param overrides; absent means Info.
    //
    // We need to peek at one CLI param before kicking off the
    // full runner pipeline. Strip a leading `run` subcommand the
    // same way `run_with_observer` does at its own param-parse
    // step, so this peek doesn't reject perfectly valid CLI
    // shapes (`nbrs run loglevel=debug …`).
    let stripped: &[String] = match args.first().map(|s| s.as_str()) {
        Some("run") => &args[1..],
        _ => args,
    };
    let cli_params = parse_params(stripped);
    let min_level = cli_params.get("loglevel")
        .and_then(|s| parse_log_level(s))
        .unwrap_or(crate::observer::LogLevel::Info);
    run_with_observer(args,
        Arc::new(crate::observer::StderrObserver::with_min_level(min_level))).await
}

/// Parse a CLI/workload `loglevel=` value. Case-insensitive,
/// accepts the standard names plus the abbreviations the log
/// sink emits (`DBG` / `INF` / `WRN` / `ERR`).
pub fn parse_log_level(s: &str) -> Option<crate::observer::LogLevel> {
    use crate::observer::LogLevel;
    match s.trim().to_ascii_lowercase().as_str() {
        "debug" | "dbg" | "trace"        => Some(LogLevel::Debug),
        "info"  | "inf"                  => Some(LogLevel::Info),
        "warn"  | "wrn" | "warning"      => Some(LogLevel::Warn),
        "error" | "err"                  => Some(LogLevel::Error),
        _ => None,
    }
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
    // Set global observer so all code can log through it
    crate::observer::set_global_observer(observer.clone());

    // Wire error handler logging through the observer.
    // Per-cycle error lines fire from inside an executing
    // phase, so prefix with the running phase's scope-depth
    // indent — the same alignment the polling-op messages,
    // phase startup/complete lines, and DONE summary use.
    // The errorhandler crate stays scope-agnostic; the
    // bridging closure here is what makes the output
    // hierarchic in tui=terminal mode.
    nbrs_errorhandler::handlers::set_log_fn(|msg| {
        let indent = crate::scene_tree::running_phase_indent();
        crate::observer::log(crate::observer::LogLevel::Warn, &format!("{indent}{msg}"));
    });

    // Route nbrs-metrics diagnostic warnings through the observer so
    // reporter write failures, histogram-record errors, etc. don't
    // slip past the TUI as raw stderr prints. Indent matches the
    // running phase the same way the errorhandler bridge above does
    // — these emits fire mid-phase from the metrics pipeline.
    nbrs_metrics::diag::set_warn_fn(|msg| {
        let indent = crate::scene_tree::running_phase_indent();
        crate::observer::log(crate::observer::LogLevel::Warn, &format!("{indent}{msg}"));
    });
    nbrs_metrics::diag::set_info_fn(|msg| {
        let indent = crate::scene_tree::running_phase_indent();
        crate::observer::log(crate::observer::LogLevel::Info, &format!("{indent}{msg}"));
    });

    // Route nbrs-variates' data-source audit lines (prebuffer
    // coverage + reader opens, plus general nbrs-variates
    // diagnostics) through the observer log so every level
    // lands in session.log alongside the rest of the run trace.
    // See `nbrs_variates::audit` for the events emitted.
    nbrs_variates::audit::set_log_fn(|level, msg| {
        let mapped = match level {
            nbrs_variates::audit::LogLevel::Debug => crate::observer::LogLevel::Debug,
            nbrs_variates::audit::LogLevel::Info  => crate::observer::LogLevel::Info,
            nbrs_variates::audit::LogLevel::Warn  => crate::observer::LogLevel::Warn,
            nbrs_variates::audit::LogLevel::Error => crate::observer::LogLevel::Error,
        };
        // Audit lines fire from inside data-source nodes during
        // phase execution; align with the running phase like
        // the errorhandler / metrics bridges above.
        let indent = crate::scene_tree::running_phase_indent();
        crate::observer::log(mapped, &format!("{indent}{msg}"));
    });

    let mut diag = DiagnosticConfig::normal();

    // Detect scenario shorthand: `workload.yaml <scenario_name>` → `scenario=<name>`
    let args = normalize_args(args);
    let params = parse_params(&args);

    // Load workload — from inline op= or YAML file.
    let mut workload_file: Option<String> = None;
    let mut workload_source_text: Option<String> = None;
    let workload = if let Some(op_str) = params.get("op") {
        if params.contains_key("workload") {
            crate::diag!(crate::observer::LogLevel::Warn, "warning: op= overrides workload=");
        }
        nbrs_workload::inline::synthesize_inline_workload(op_str)
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
        let workload = nbrs_workload::parse::parse_workload(&yaml_source, &params)
            .map_err(|e| format!("parse workload: {e}"))?;
        // Stash the raw YAML for runtime error diagnostics — the
        // dispatch layer formats `<path>:<line>:<col>: …` when a
        // for_each spec or do_while condition fails interpolation,
        // so the user can jump to the exact source location.
        workload_source_text = Some(yaml_source);
        workload
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
    let rate: Option<f64> = match merged_params.get("rate") {
        Some(s) => Some(s.parse().map_err(|_| format!("rate value '{s}' is not a valid number"))?),
        None => None,
    };
    let tag_filter = merged_params.get("tags").cloned();
    let seq_type = merged_params.get("seq")
        .map(|s| SequencerType::parse(s).unwrap_or(SequencerType::Bucket))
        .unwrap_or(SequencerType::Bucket);
    let mut error_spec = merged_params.get("errors")
        .cloned()
        .unwrap_or_else(|| ".*:warn,stop".to_string());
    // SRD-44 §"--force-retry-failed": when set on a resume
    // invocation, prepend a `.*:retry,warn` rule to the errors
    // cascade so any failure surfaces a retry rather than the
    // workload's normal stop / fail behaviour. Idempotent: when
    // set on a fresh run, it still applies (doesn't gate on
    // is_resume) — operators who want the override on a fresh
    // run get it; operators who pass it accidentally without
    // resume= get the same generous-retry policy they'd see on
    // resume.
    let force_retry_failed = params.get("force_retry_failed")
        .map(|s| s != "false" && s != "0")
        .unwrap_or(false)
        || args.iter().any(|a| a == "--force-retry-failed");
    if force_retry_failed {
        error_spec = format!(".*:retry,warn;{error_spec}");
        crate::diag!(crate::observer::LogLevel::Info,
            "--force-retry-failed: errors cascade prefixed with '.*:retry,warn'");
    }

    // Validate CLI parameters (runner-known + adapter-registered + workload-declared).
    //
    // Allow-list = `KNOWN_PARAMS` ∪ adapter-registered ∪
    // `workload.declared_params` (the original YAML keys from
    // the workload's `params:` block). We do **not** consult
    // `workload.params` here — `parse.rs` merges every CLI arg
    // into that map regardless of whether the workload declared
    // it, so checking against it would let any CLI param through
    // and silently drop typos like `profile=perf` (vs.
    // `profiler=perf`). `declared_params` preserves the user's
    // declared surface independent of CLI overlays, which is
    // what the closed-vocabulary check needs.
    {
        let adapter_params = registered_adapter_params();
        let all_known: Vec<&str> = KNOWN_PARAMS.iter().copied()
            .chain(adapter_params.iter().copied())
            .chain(workload.declared_params.iter().map(|s| s.as_str()))
            .collect();
        for key in params.keys() {
            if !all_known.contains(&key.as_str()) {
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

    // Extract workload structure before consuming. M3.6:
    // `workload_params` is the set of *workload-declared* params
    // (the YAML `params:` block, with CLI overrides applied) —
    // these are what get injected as `final` bindings on the
    // workload kernel. The full `workload.params` map also
    // contains ad-hoc CLI params like `cycles=`, `workload=`,
    // `tags=`, etc., which are not declared bindings and must
    // not become identifiers in the GK source. Filter by
    // `declared_params` to keep only the YAML-declared subset.
    let declared: std::collections::HashSet<&String> = workload.declared_params.iter().collect();
    let workload_params: HashMap<String, String> = workload.params.iter()
        .filter(|(k, _)| declared.contains(*k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    drop(declared);
    let phases = workload.phases;
    let phase_order = workload.phase_order;
    let scenarios = workload.scenarios;
    // Unified report block (SRD-46). Tables auto-render at
    // end-of-run; plot specs persist into the session db so
    // post-hoc `nbrs report ...` can replay them. Empty
    // `report:` block ⇒ no auto-render and no persisted specs.
    let workload_report = workload.report.clone();
    let workload_summaries: HashMap<String, nbrs_workload::model::SummaryConfig> =
        workload_report.items()
            .filter(|i| matches!(i.kind, nbrs_workload::report::Kind::Table))
            .map(|i| (i.name.clone(),
                nbrs_workload::model::SummaryConfig::parse(&i.body)))
            .collect();

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
    let mut phase_ops_for_compile: Vec<nbrs_workload::model::ParsedOp> = Vec::new();
    let mut phase_raw_ops: HashMap<String, Vec<nbrs_workload::model::ParsedOp>> = HashMap::new();
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
        crate::diag!(crate::observer::LogLevel::Info, "{} ops, {} cycles, concurrency={}, adapter={}",
            ops.len(),
            explicit_cycles.map(|c| c.to_string()).unwrap_or("auto".into()),
            concurrency,
            driver);
    } else {
        // Always log to session.log; stderr suppression is the
        // observer's job (TuiObserver gates eprintln internally).
        crate::diag!(crate::observer::LogLevel::Info, "{} phases, {} top-level ops, adapter={}",
            phases.len(), ops.len(), driver);
    }

    // Collect --gk-lib=path flags
    let gk_lib_paths: Vec<std::path::PathBuf> = args.iter()
        .filter_map(|a| a.strip_prefix("--gk-lib="))
        .map(std::path::PathBuf::from)
        .collect();
    let strict = args.iter().any(|a| a == "--strict")
        || matches!(params.get("strict").map(String::as_str), Some("true") | Some("1"));

    // Parse dryrun= param into diagnostic config
    if let Some(spec) = params.get("dryrun") {
        diag = DiagnosticConfig::parse(spec);
    }

    // SRD-46 + SRD-15: surface report-block warnings collected
    // by the parser. Strict mode promotes to a hard error so a
    // workload with `defaults`-collisions or empty groups can't
    // silently pass; otherwise we log them and continue.
    if !workload.report_warnings.is_empty() {
        if strict {
            return Err(format!(
                "report-block warnings (strict mode promotes to errors):\n  - {}",
                workload.report_warnings.join("\n  - "),
            ));
        }
        for w in &workload.report_warnings {
            eprintln!("warning: report: {w}");
        }
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

    // Resolve the resume source BEFORE creating the new session
    // — `Session::new` eagerly remaps `logs/latest` at the new
    // session id, so any path resolution that depends on the old
    // `latest` target has to happen first. Stored as
    // `resume_target` and consulted later when constructing the
    // checkpoint writer + plan. SRD-44 §"Resume CLI surface".
    let resume_target: Option<std::path::PathBuf> = {
        let explicit = params.get("resume")
            .filter(|s| !s.is_empty())
            .map(|s| {
                let p = std::path::PathBuf::from(s);
                if p.is_file() { p }
                else if p.is_dir() { p.join("checkpoint.json") }
                else { std::path::PathBuf::from("logs").join(s).join("checkpoint.json") }
            });
        let resume_latest = params.get("resume_latest")
            .map(|s| s != "false" && s != "0")
            .unwrap_or(false)
            || args.iter().any(|a| a == "--resume-latest");
        if resume_latest {
            // Resolve the symlink to a concrete session dir
            // *now* — once `Session::new` runs the symlink will
            // be repointed at the new session.
            let latest = std::path::PathBuf::from("logs/latest");
            let resolved = std::fs::read_link(&latest).ok()
                .map(|target| {
                    if target.is_absolute() { target }
                    else { std::path::PathBuf::from("logs").join(target) }
                })
                .map(|d| d.join("checkpoint.json"));
            explicit.or(resolved)
        } else {
            explicit
        }
    };

    // Session: root context for this run. Creates logs/{scenario}_{timestamp}/
    // for fresh runs; reuses the prior session dir when resuming
    // so the metrics.db is appended-to in-place per SRD-44
    // §"Wholesale metrics-purge".
    let scenario_for_session = params.get("scenario").map(|s| s.as_str()).unwrap_or("default");
    let session = match resume_target.as_ref() {
        Some(p) if p.exists() => {
            let prior_dir = p.parent()
                .map(|d| d.to_path_buf())
                .unwrap_or_else(|| std::path::PathBuf::from("logs/latest"));
            crate::session::Session::resume(
                prior_dir,
                workload_file.as_deref().unwrap_or("inline"),
                scenario_for_session,
            )
        }
        _ => crate::session::Session::new_with_args(
            workload_file.as_deref().unwrap_or("inline"),
            scenario_for_session,
            &args,
        ),
    };
    let session_id = session.id.clone();

    // dryrun=controls: defer the tree walk until after phase
    // construction. `list_controls` implies depth=Phase, which
    // means every phase compiles and attaches its component —
    // that's when activity-scoped controls get declared — but
    // no cycles run. Walking here would only see session-root
    // controls. The renderer fires at the very end of the run,
    // just before the session returns.

    // Direct the diagnostic log sink at <session_dir>/session.log so every
    // observer::log() call is captured durably, even under the TUI.
    let session_log_path = session.output_dir.join("session.log");
    if let Err(e) = crate::observer::set_log_file(&session_log_path) {
        crate::diag!(crate::observer::LogLevel::Warn,
            "warning: failed to open session log {}: {e}",
            session_log_path.display());
    }

    crate::diag!(crate::observer::LogLevel::Info, "session: {} ({})",
        session.id, session.output_dir.display());

    // SQLite metrics in session directory
    let sqlite_path = session.metrics_path();
    let sqlite_reporter = nbrs_metrics::reporters::sqlite::SqliteReporter::new(&sqlite_path)
        .map(|mut r| {
            r.set_metadata("session", &session.id);
            r.set_metadata("workload", &session.workload);
            r.set_metadata("scenario", &session.scenario);
            r.set_metadata("start_time", &format!("{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()));
            for (k, v) in &merged_params {
                r.set_metadata(&format!("param.{k}"), v);
            }
            crate::diag!(crate::observer::LogLevel::Info, "metrics: {}",
                sqlite_path.display());
            r
        })
        .map_err(|e| crate::diag!(crate::observer::LogLevel::Warn,
            "warning: SQLite metrics disabled: {e}"))
        .ok();
    let sqlite_reporter = std::sync::Arc::new(std::sync::Mutex::new(sqlite_reporter));

    // Merge all ops for param expansion and GK compilation.
    let _num_top_level_ops = ops.len();
    let mut all_ops_for_compile: Vec<nbrs_workload::model::ParsedOp> = ops;
    all_ops_for_compile.extend(phase_ops_for_compile);

    // === Pre-compile rewrites ===
    //
    // Stage 2 (post-M3.6): the workload-params kernel
    // (`crate::params::build_workload_params_kernel`) installs
    // every workload param as a `final <name> := <literal>`
    // binding on the workload-root kernel. Descendant scopes
    // see those bindings via `bind_outer_scope` + standard GK
    // scope-chain lookup. The legacy
    // `rewrite_workload_param_idents_in_bindings` text pass
    // (which substituted `{name}` → literal value before
    // compilation) was redundant once that path landed and
    // produced broken output for in-string placeholders
    // (`"{dataset}:{profile}"` → `""sift1m":"default""`),
    // so it has been retired.
    //
    // What's left here: rewrite inline `{{expr}}` constructs to
    // named bindings so the GK compiler can hoist them as
    // `final __expr_N := …` entries. That pass is a bind-point
    // shape transform, not a value substitution — it operates
    // independently of workload params.
    crate::scope::rewrite_inline_exprs(&mut all_ops_for_compile);

    // The workload-level `bindings:` (top-level YAML block) is
    // a first-class workload-scope input — separate from any
    // op's bindings. We pass it through to the compiler as a
    // distinct source so cursor declarations and other
    // workload-scoped GK statements land on the workload kernel
    // alongside the workload params, *without* going through
    // the op-binding param-ident rewrite (which would text-
    // substitute `{name}` placeholders inside string literals).
    // GK's standard string-interpolation handles `{name}` at
    // compile time against the `final <name> := <literal>`
    // bindings that workload-params injection installs (M3.6 path).
    let workload_level_gk: Option<String> = match &workload.bindings {
        nbrs_workload::model::BindingsDef::GkSource(s) if !s.trim().is_empty()
            => Some(s.clone()),
        _ => None,
    };

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

    // Build the workload-params root kernel first. This is the
    // canonical home for every declared workload parameter —
    // one `final <name> := <literal>` per param, compiled into
    // a stand-alone kernel whose outputs every descendant
    // `bind_outer_scope`s through. Replaces the prior approach
    // of patching params into multiple places (per-op binding
    // text substitution, per-kernel `final` injection in
    // `build_scope`). See `nbrs-activity::params`.
    let params_kernel = crate::params::build_workload_params_kernel(&workload_params)
        .map_err(|e| format!("workload params kernel: {e}"))?;

    let mut kernel = compile_bindings_with_libs_excluding(
        &all_ops_for_compile, workload_dir, gk_lib_paths.clone(), strict, &[], &config_refs,
        "outer workload bindings", cursor_limit, &workload_params,
        workload_level_gk.as_deref(),
    ).map_err(|e| format!("outer workload bindings: {e}"))?;

    // Chain the workload kernel through the params kernel:
    // names declared on params flow in via `bind_outer_scope`,
    // visible to every descendant kernel that
    // `bind_outer_scope`s through this workload kernel in
    // turn (per SRD-16 §"Single Name-Resolution Surface").
    kernel.bind_outer_scope(&params_kernel);

    // Canonical workload-scope kernel for the scope tree (SRD
    // 18b §"Iteration variables as scope outputs"). Built from
    // the same compiled program; lives behind `Arc` so the scope
    // tree can answer `lookup_name(...)` queries via the
    // GK-native `get_constant` path with no sidecar cache.
    // Installed below once the scope tree is constructed.
    let workload_canonical_kernel: std::sync::Arc<nbrs_variates::kernel::GkKernel> =
        std::sync::Arc::new(
            nbrs_variates::kernel::GkKernel::from_program(kernel.program().clone())
        );


    // Extract output manifest and folded constant values from outer kernel
    // === GK Config Resolution (all done before kernel is consumed) ===

    let resolved_cli_cycles: Option<u64> = explicit_cycles.or_else(||
        params.get("cycles").and_then(|s| resolve_gk_config(s, &kernel))
    );
    let resolved_cli_concurrency: Option<u64> = params.get("concurrency")
        .and_then(|s| resolve_gk_config(s, &kernel));

    // Collect phases that are inside scenario for_each groups — these have
    // iteration variables resolved at runtime, not pre-resolution time.
    fn collect_grouped_phases(nodes: &[nbrs_workload::model::ScenarioNode], in_group: bool, out: &mut std::collections::HashSet<String>) {
        for node in nodes {
            match node {
                nbrs_workload::model::ScenarioNode::Phase(name) => {
                    if in_group { out.insert(name.clone()); }
                }
                nbrs_workload::model::ScenarioNode::Comprehension { children, .. }
                | nbrs_workload::model::ScenarioNode::DoWhile { children, .. }
                | nbrs_workload::model::ScenarioNode::DoUntil { children, .. } => {
                    collect_grouped_phases(children, true, out);
                }
                nbrs_workload::model::ScenarioNode::IncludedScenario { children, .. } => {
                    // Inclusion is transparent — children inherit
                    // whatever grouping context wrapped the
                    // include site. We pass `in_group` through
                    // so a `scenario:` reference at top level of
                    // a scenario doesn't artificially mark its
                    // phases as grouped.
                    collect_grouped_phases(children, in_group, out);
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
    //
    // Metrics infrastructure is shared across both the phased and
    // single-activity paths — both route through the session-level
    // `CadenceReporter` + `MetricsQuery`. The legacy per-activity
    // capture thread with inline VM/SQLite reporter calls is gone.

    // Plan the cadence tree from the observer's cadence preferences
    // (or defaults), validated against the scheduler base interval,
    // build the CadenceReporter, and wire it through the session +
    // GK metric nodes as the single query source.
    let base_interval = std::time::Duration::from_secs(1);
    let cadences = observer.cadences()
        .unwrap_or_else(nbrs_metrics::cadence::Cadences::defaults);
    let cadence_tree = nbrs_metrics::cadence::CadenceTree::plan_validated(
        cadences,
        nbrs_metrics::cadence::DEFAULT_MAX_FAN_IN,
        base_interval,
    ).map_err(|e| format!("cadence tree: {e}"))?;
    let cadence_reporter = Arc::new(
        nbrs_metrics::cadence_reporter::CadenceReporter::new(cadence_tree.clone()),
    );
    let metrics_query = Arc::new(nbrs_metrics::metrics_query::MetricsQuery::new(
        cadence_reporter.clone(),
        session.component.clone(),
    ));
    session.set_metrics_query(metrics_query.clone());
    nbrs_variates::nodes::metrics::set_global_query(metrics_query.clone());
    observer.on_metrics_query(metrics_query.clone());

    let session_for_capture = session.component.clone();
    let mut sched_builder = nbrs_metrics::scheduler::SchedulerBuilder::new()
        .base_interval(std::time::Duration::from_secs(1))
        .with_cadence_reporter(cadence_reporter.clone())
        .with_cadence_tree(cadence_tree.clone());

    // SRD-42 §"SQLite — near-time persistence": subscribe the
    // SQLite reporter via the CadenceReporter push path so slow
    // disk can't stall the cascade. The subscription runs on its
    // own dispatch thread with a per-subscription timeout.
    //
    // Preferred write cadence is 30 s — coarse enough to keep
    // write volume low for long runs, fine enough for post-run
    // analysis. Aligns to the nearest declared cadence ≥ 30 s
    // (default declared set includes 30 s so this resolves exactly).
    // Journal mode is WAL (set in SqliteReporter::new via
    // `PRAGMA journal_mode=WAL`), so readers never block writers.
    //
    // Always-on: this subscription fires whenever the SQLite
    // reporter was constructed successfully. Operators don't need
    // to opt in with any extra param — every run produces a
    // `metrics.db` in its session directory by default.
    let sqlite_cadence = cadence_tree.align_to_declared(
        std::time::Duration::from_secs(30),
    );
    if let (Some(cadence), Ok(guard)) = (sqlite_cadence, sqlite_reporter.lock()) {
        if guard.is_some() {
            drop(guard);
            let sqlite_for_sub = sqlite_reporter.clone();
            match cadence_reporter.subscribe(
                cadence,
                Box::new(MutexReporter(sqlite_for_sub)),
                nbrs_metrics::cadence_reporter::SubscriptionOpts::default(),
            ) {
                Ok(_) => {
                    crate::diag!(crate::observer::LogLevel::Info,
                        "metrics: SQLite writes every {:?} (WAL mode)", cadence);
                }
                Err(e) => {
                    crate::diag!(crate::observer::LogLevel::Warn,
                        "metrics: SQLite subscription failed: {e}");
                }
            }
        }
    }

    // Same routing for the VictoriaMetrics / Prometheus push reporter
    // when `--report-to` (or equivalent param) was provided.
    // `jobname` / `instance` params match the nosqlbench-java
    // `PromPushReporterComponent` convention; they're substituted
    // into any `JOBNAME` / `INSTANCE` placeholders in the URL.
    if let Some(url) = openmetrics_url.as_ref() {
        if let Some(cadence) = cadence_tree.align_to_declared(
            std::time::Duration::from_secs(10),
        ) {
            let jobname = merged_params.get("jobname").cloned()
                .unwrap_or_else(|| "default".to_string());
            let instance = merged_params.get("instance").cloned()
                .unwrap_or_else(|| "default".to_string());
            let mut vm = match nbrs_metrics::reporters::victoriametrics
                ::VictoriaMetricsReporter::from_spec(url)
            {
                Ok(r) => r,
                Err(_) => nbrs_metrics::reporters::victoriametrics
                    ::VictoriaMetricsReporter::new(url),
            };
            vm = vm.with_jobname(jobname).with_instance(instance);
            if let Some(token_path) = merged_params.get("prompush_apikeyfile") {
                match vm.with_bearer_token_file(token_path) {
                    Ok(r) => vm = r,
                    Err(e) => {
                        crate::diag!(crate::observer::LogLevel::Warn,
                            "prompush_apikeyfile '{token_path}': {e}");
                        vm = nbrs_metrics::reporters::victoriametrics
                            ::VictoriaMetricsReporter::from_spec(url)
                            .unwrap_or_else(|_| nbrs_metrics::reporters::victoriametrics
                                ::VictoriaMetricsReporter::new(url))
                            .with_jobname(
                                merged_params.get("jobname").cloned()
                                    .unwrap_or_else(|| "default".to_string()),
                            )
                            .with_instance(
                                merged_params.get("instance").cloned()
                                    .unwrap_or_else(|| "default".to_string()),
                            );
                    }
                }
            }
            let _ = cadence_reporter.subscribe(
                cadence,
                Box::new(vm),
                nbrs_metrics::cadence_reporter::SubscriptionOpts::default(),
            );
        }
    }

    // Register the observer's reporters at their requested cadences
    // on the scheduler tree (base-interval live-frame forwarding for
    // sparklines / live histogram).
    for (interval, reporter) in observer.reporters() {
        sched_builder = sched_builder.add_reporter(
            interval,
            BoxedReporter(reporter),
        );
    }

    let scheduler = sched_builder.build(Box::new(move || {
        nbrs_metrics::component::capture_tree(
            &session_for_capture,
            std::time::Duration::from_secs(1),
        )
    }));
    let stop_handle = Arc::new(scheduler.start());

    // Install the session-wide Ctrl-C handler. First SIGINT
    // requests cooperative shutdown (fibers exit at cycle
    // boundary, profiler + cadence reporter flush in normal
    // teardown order); second SIGINT force-exits. Idempotent —
    // safe to call again on retry / reentry paths.
    crate::session_signals::install_signal_handler();

    // Start profiler if requested (profiler=flamegraph or profiler=perf).
    // Shared across both phased and single-activity paths. The
    // guard's Drop impl flushes the flamegraph SVG on early
    // returns (panic, ?-propagation, SIGINT-driven shutdown), so
    // the explicit `finish()` below is the happy-path fast lane,
    // not the only flush site.
    let mut _profiler = crate::profiler::ProfileGuard::maybe_start(
        &merged_params, Some(&session.output_dir));

    if !phases.is_empty() {
        // --- Phased execution ---
        let scenario_name = params.get("scenario").map(|s| s.as_str()).unwrap_or("default");
        let scenario_nodes = resolve_scenario(&scenarios, &phase_order, scenario_name)?;

        // Build the canonical scope tree (SRD 18b §"Canonical
        // traversal"). Mirrors the scenario tree 1:1 with parent
        // pointers, depth, and pragma slots. Today consumed by
        // observer pre-mapping and diagnostic display; future
        // steps drive execution from this tree directly.
        let scope_tree = {
            let mut t = crate::scope_tree::ScopeTree::build(scenario_name, &scenario_nodes);
            // Populate phase-leaf pragmas from each phase's GK
            // source and chain each scope's `PragmaSet` to its
            // parent's. SRD 18b §"Pragma chain along the scope
            // tree"; SRD 15 §"Pragma Scope".
            let conflicts = t.populate_pragmas(&phases);
            for c in &conflicts {
                let path = t.ancestors(c.scope_idx)
                    .map(|(_, n)| n.kind.label())
                    .collect::<Vec<_>>()
                    .join(" ← ");
                let msg = format!(
                    "pragma '{}' conflict at {path}: outer (line {}) overrides inner (line {})",
                    c.name, c.outer_line, c.inner_line,
                );
                if strict {
                    return Err(msg);
                } else {
                    crate::diag!(crate::observer::LogLevel::Warn, "{msg}");
                }
            }
            // Validate iter-var name uniqueness against workload
            // params and enclosing iter vars. Aliasing creates an
            // unambiguous spec-evaluation case the runtime can't
            // disambiguate; reject up-front.
            let wp_names: std::collections::HashSet<String> =
                workload_params.keys().cloned().collect();
            t.validate_iter_var_uniqueness(&wp_names)?;
            std::sync::Arc::new(t)
        };

        // Install the canonical workload kernel (SRD 18b §"Iter
        // vars as scope outputs"). After this, intermediate
        // scopes (for_each, for_combinations, …) install their
        // own kernels in DFS pre-order below — each one's
        // synthesis reads its parent's manifest via the standard
        // GK API on the parent's installed kernel.
        scope_tree.install_kernel(scope_tree.root, workload_canonical_kernel);

        // M3.2: install per-scope kernels for for_each /
        // for_combinations nodes. Each kernel re-exports its
        // iteration variables and any referenced inherited
        // values as outputs (`final x := x` passthrough), so
        // children's standard `bind_outer_scope(parent)`
        // chains inheritance through arbitrary nesting depth
        // — no caller-side scope-tree walking for name
        // resolution at runtime.
        let workload_dir_owned: Option<std::path::PathBuf> =
            workload_dir.map(|p| p.to_path_buf());
        // M3.4b: scope kinds get categorized for synthesis.
        // For-comprehensions (ForEach, ForCombinations,
        // ForEachUnion) carry tuple iteration vars; do-loops
        // (DoWhile, DoUntil) carry an optional counter +
        // condition expression. Both produce installed kernels
        // that the unified dispatch_comprehension reads from.
        enum InstallSpec {
            ForComprehension {
                idx: crate::scope_tree::ScopeNodeIdx,
                iter_vars: Vec<String>,
                spec_exprs: Vec<String>,
            },
            DoLoop {
                idx: crate::scope_tree::ScopeNodeIdx,
                counter: Option<String>,
                condition: String,
            },
        }
        let install_specs: Vec<InstallSpec> = scope_tree.iter_dfs()
            .filter_map(|(idx, node)| match &node.kind {
                crate::scope_tree::ScopeKind::Comprehension { comprehension } => {
                    // Representative iter_vars + spec_exprs for
                    // synthesis: dedup'd by var name (Union mode
                    // can repeat names across sub-spaces; we
                    // declare each extern once with the first
                    // occurrence's spec for type detection).
                    let mut vars = Vec::new();
                    let mut specs = Vec::new();
                    let mut seen = std::collections::HashSet::new();
                    for clause in comprehension.flat_clauses() {
                        if seen.insert(clause.var.clone()) {
                            vars.push(clause.var.clone());
                            specs.push(clause.expr.clone());
                        }
                    }
                    Some(InstallSpec::ForComprehension {
                        idx,
                        iter_vars: vars,
                        spec_exprs: specs,
                    })
                }
                crate::scope_tree::ScopeKind::DoWhile { condition, counter } => {
                    Some(InstallSpec::DoLoop {
                        idx,
                        counter: counter.clone(),
                        condition: condition.clone(),
                    })
                }
                crate::scope_tree::ScopeKind::DoUntil { condition, counter } => {
                    Some(InstallSpec::DoLoop {
                        idx,
                        counter: counter.clone(),
                        condition: condition.clone(),
                    })
                }
                crate::scope_tree::ScopeKind::Phase { name } => {
                    // Phase-level for_each: if this phase
                    // declares `for_each: "var in expr"`, treat
                    // it as a single-clause tuple comprehension.
                    // The Phase scope-tree node hosts the
                    // synthesized kernel; the executor's Phase
                    // handler routes through dispatch_comprehension
                    // with TerminalAction::Phase.
                    phases.get(name.as_str())
                        .and_then(|p| p.for_each.as_ref())
                        .map(|spec| {
                            // Parse "var in expr"; if malformed,
                            // synthesis produces an empty kernel
                            // and the runtime reports the proper
                            // error.
                            let (var, expr) = if let Some(pos) = spec.find(" in ") {
                                let (lhs, rhs) = spec.split_at(pos);
                                (lhs.trim().to_string(), rhs[" in ".len()..].trim().to_string())
                            } else {
                                (String::new(), spec.clone())
                            };
                            InstallSpec::ForComprehension {
                                idx,
                                iter_vars: vec![var],
                                spec_exprs: vec![expr],
                            }
                        })
                }
                _ => None,
            })
            .collect();

        for install_spec in install_specs {
            let idx = match &install_spec {
                InstallSpec::ForComprehension { idx, .. } => *idx,
                InstallSpec::DoLoop { idx, .. } => *idx,
            };
            // Nearest installed ancestor — skips Scenario /
            // IncludedScenario nodes that don't install kernels
            // (those are pass-through structural).
            let parent_kernel = {
                let mut cursor = scope_tree.nodes[idx].parent;
                let mut found: Option<std::sync::Arc<nbrs_variates::kernel::GkKernel>> = None;
                while let Some(p) = cursor {
                    if let Some(k) = scope_tree.nodes[p].cached_kernel.get() {
                        found = Some(k.clone());
                        break;
                    }
                    cursor = scope_tree.nodes[p].parent;
                }
                found.expect("workload root always has an installed kernel")
            };
            let parent_manifest = extract_manifest(parent_kernel.program());
            let context = format!(
                "scope idx {idx} ({})",
                scope_tree.nodes[idx].kind.label(),
            );

            let result = match install_spec {
                InstallSpec::ForComprehension { iter_vars, spec_exprs, .. } => {
                    nbrs_variates::comprehension::synthesize_for_each_scope(
                        &iter_vars,
                        &spec_exprs,
                        &parent_manifest,
                        &parent_kernel,
                        &workload_params,
                        gk_lib_paths.clone(),
                        workload_dir_owned.as_deref(),
                        strict,
                        &context,
                    )
                }
                InstallSpec::DoLoop { counter, condition, .. } => {
                    crate::scope::build_do_loop_scope_kernel(
                        counter.as_deref(),
                        &condition,
                        &parent_manifest,
                        &parent_kernel,
                        &workload_params,
                        gk_lib_paths.clone(),
                        workload_dir_owned.as_deref(),
                        strict,
                        &context,
                    )
                }
            };

            match result {
                Ok(kernel) => {
                    let _ = scope_tree.install_kernel(
                        idx,
                        std::sync::Arc::new(kernel),
                    );
                }
                Err(e) => {
                    if strict {
                        return Err(e);
                    }
                    crate::diag!(
                        crate::observer::LogLevel::Warn,
                        "scope kernel synthesis failed: {e}"
                    );
                }
            }
        }

        crate::diag!(crate::observer::LogLevel::Info,
            "scenario '{scenario_name}': {}",
            format_scenario_tree(&scenario_nodes));

        // Observer is passed from the caller (default: StderrObserver).

        // Pre-map the scenario tree for observer (TUI phase tree
        // population) and publish to the global accessor so
        // out-of-band consumers (web API, post-run scripting)
        // can snapshot the structure. In strict mode, empty
        // iteration sources fail the run here — before any
        // phase executes — so a CI run can't silently skip
        // a sub-space (SRD-15 §"Empty Iteration Sources").
        let pre_mapped_tree = match crate::executor::pre_map_tree(
            &scenario_nodes, &phases, &scope_tree, strict,
            // Seed the path with the top-level scenario name —
            // pre_map_recursive extends it through every nested
            // construct as it walks. See SRD-44 §"Phase
            // identity" for the path-segment vocabulary.
            vec![crate::checkpoint::PathSegment::Scenario(
                scenario_name.to_string(),
            )],
        ) {
            Ok(scene_tree) => {
                observer.scenario_pre_mapped(&scene_tree);
                Some(scene_tree)
            }
            Err(e) if strict => {
                return Err(e);
            }
            Err(e) => {
                crate::diag!(crate::observer::LogLevel::Warn,
                    "pre-map walker failed (scope hierarchy will be flat in summaries / TUI): {e}");
                // Non-strict: the pre-map failure is purely a
                // diagnostic affordance. The TUI will populate
                // its tree lazily as phases run.
                None
            }
        };

        // --- Checkpoint writer + resume plan (SRD-44) ---
        //
        // The writer file lives at `<session-dir>/checkpoint.json`
        // and is rewritten atomically on every flush. Resume from
        // an explicit prior session is wired through the
        // `--resume <session>` / `--resume-latest` CLI surface
        // (see runner CLI parsing); for a fresh session the writer
        // starts empty and the plan reruns everything.
        let checkpoint_path = session.output_dir.join("checkpoint.json");
        // `resume_target` was resolved at the top of run(),
        // before Session::new repointed `logs/latest` at the new
        // session id. SRD-44 §"Resume CLI surface".
        let saved_doc = match resume_target.as_ref() {
            Some(p) => match crate::checkpoint::storage::read(p) {
                Ok(Some(doc)) => Some(doc),
                Ok(None) => {
                    crate::diag!(crate::observer::LogLevel::Warn,
                        "resume: no checkpoint found at {} — fresh session",
                        p.display());
                    None
                }
                Err(e) => {
                    return Err(format!("resume: {e}"));
                }
            },
            None => None,
        };
        let invocation = saved_doc.as_ref().map(|d| d.invocation + 1).unwrap_or(1);
        let started_at = saved_doc.as_ref()
            .map(|d| d.started_at.clone())
            .unwrap_or_else(|| crate::checkpoint::storage::now_rfc3339());
        let checkpoint_writer = std::sync::Arc::new(match saved_doc.as_ref() {
            Some(_doc) => {
                // Restore from saved.
                let doc = saved_doc.clone().unwrap();
                crate::checkpoint::CheckpointWriter::from_existing(
                    checkpoint_path.clone(), doc,
                    crate::checkpoint::storage::now_rfc3339(), invocation,
                )
            }
            None => crate::checkpoint::CheckpointWriter::new(
                checkpoint_path.clone(),
                session.id.clone(),
                started_at,
                invocation,
            ),
        });

        // End-of-run notices: drops on success OR error path.
        //
        //  - Resume hint: when checkpoint state shows
        //    incomplete idempotent phases (SRD-44), advise the
        //    operator how to resume.
        //  - Keep-purge forecast: when the next new session
        //    would auto-purge sessions under the keep cap
        //    (SRD-45), let the operator know how many and how
        //    to disable.
        let parent_for_keep_check = if let Some(p) = session.output_dir.parent() {
            p.to_path_buf()
        } else {
            std::path::PathBuf::from("logs")
        };
        let session_keep = crate::session::resolve_session_dir(&args).session_keep;
        struct EndOfRunNoticeGuard {
            writer: std::sync::Arc<crate::checkpoint::CheckpointWriter>,
            parent: std::path::PathBuf,
            keep_cap: usize,
        }
        impl Drop for EndOfRunNoticeGuard {
            fn drop(&mut self) {
                if let Some(hint) = self.writer.resume_hint() {
                    eprintln!("\n{hint}");
                }
                let n = crate::session::forecast_keep_purge(&self.parent, self.keep_cap);
                if n > 0 {
                    crate::diag!(
                        crate::observer::LogLevel::Info,
                        "the next new nbrs session will auto-purge {n} prior session \
                         director{plural} under {} due to --session-keep={cap}. \
                         To disable: --session-keep=0 (or NBRS_SESSION_KEEP=0). \
                         To raise the cap: --session-keep=<bigger>.",
                        self.parent.display(),
                        plural = if n == 1 { "y" } else { "ies" },
                        cap = self.keep_cap,
                    );
                }
            }
        }
        let _eor_notice_guard = EndOfRunNoticeGuard {
            writer: checkpoint_writer.clone(),
            parent: parent_for_keep_check,
            keep_cap: session_keep,
        };
        let resume_plan = if let (Some(saved), Some(tree)) =
            (saved_doc.as_ref(), pre_mapped_tree.as_ref())
        {
            let candidates = crate::checkpoint::scene_tree_resume_candidates(
                tree, &scope_tree, &phases);
            std::sync::Arc::new(crate::checkpoint::ResumePlan::from_checkpoint(
                saved, &candidates,
            ))
        } else {
            std::sync::Arc::new(crate::checkpoint::ResumePlan::fresh())
        };

        // Declare every pre-mapped phase into the writer so a
        // future resume can tell "didn't run yet" from "wasn't
        // planned". Idempotent — re-declaring an entry the
        // writer already restored from disk is a no-op.
        if let Some(tree) = pre_mapped_tree.as_ref() {
            crate::checkpoint::declare_scene_tree_phases(
                &checkpoint_writer, tree, &phases,
            );
        }

        if resume_plan.is_resume {
            let skip = resume_plan.skip_count();
            let mismatch = resume_plan.mismatch_count();
            let cursor = resume_plan.cursor_resume_count();
            crate::diag!(crate::observer::LogLevel::Info,
                "resume: invocation #{invocation} — \
                 {skip} skip, {mismatch} mismatched, {cursor} cursor-resume");
        }

        if let Some(scene_tree) = pre_mapped_tree {
            crate::scene_tree::install_global(scene_tree);
        }

        // Execute the scenario tree recursively via the executor module.
        {
            let dry_run_static: Option<&'static str> = match dry_run {
                Some("silent") => Some("silent"),
                Some("emit") => Some("emit"),
                _ => None,
            };
            // Pluggable scheduler (SRD 18b §"Scheduler
            // abstraction"). The `schedule=` workload param
            // controls per-level concurrency: `1` (default,
            // serial), `*` (unlimited), `N`, or a slash-list
            // like `1/4/*` per depth. Non-serial specs dispatch
            // `ConcurrentScheduler`, which forks per-task ExecCtx
            // clones under a Semaphore for bounded levels.
            let schedule_spec = std::sync::Arc::new(match params.get("schedule") {
                Some(s) => crate::scheduler::ScheduleSpec::parse(s)
                    .map_err(|e| format!("schedule= param: {e}"))?,
                None => crate::scheduler::ScheduleSpec::default_serial(),
            });
            let mut exec_ctx = crate::executor::ExecCtx {
                phases: phases.clone(),
                workload_params: workload_params.clone(),
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
                rate,
                error_spec: error_spec.clone(),
                session_id: session_id.clone(),
                label_stack: Vec::new(),
                session_component: session.component.clone(),
                cadence_reporter: cadence_reporter.clone(),
                stop_handle: stop_handle.clone(),
                observer: observer.clone(),
                scope_tree: scope_tree.clone(),
                schedule_spec: schedule_spec.clone(),
                // M3.4b: workload kernel as the default parent
                // for any phase that runs without an enclosing
                // for_each scope's per-branch kernel set on top.
                // The dispatcher overrides this within for_each
                // scopes (saving/restoring at each recursion
                // boundary). Leaf phases at the workload level
                // therefore also flow through the standard GK
                // chain (`bind_outer_scope` from workload kernel)
                // rather than the legacy flat
                // `outer_manifest` / `outer_scope_values` data
                // path.
                current_parent_kernel: scope_tree.nodes[scope_tree.root]
                    .cached_kernel.get().cloned(),
                workload_source: workload_file.as_ref().and_then(|path| {
                    workload_source_text.as_ref().map(|text| {
                        std::sync::Arc::new(crate::executor::WorkloadSource {
                            path: path.clone(),
                            text: text.clone(),
                        })
                    })
                }),
                checkpoint_writer: Some(checkpoint_writer.clone()),
                resume_plan: resume_plan.clone(),
                sqlite_reporter: sqlite_reporter.clone(),
            };
            let scheduler = crate::scheduler::build(&schedule_spec);
            scheduler.run(
                &mut exec_ctx,
                &scenario_nodes,
            ).await?;
        }

        // Workload-end lifecycle boundary: every phase in the
        // scenario has completed. Individual phase paths already
        // closed themselves at phase-end, but any workload-level
        // ingests (e.g. aggregate metrics the tree code emits at
        // scope scope rather than phase scope) still need a flush.
        // In phased mode the workload's label set is the session
        // root — there's no intermediate `activity=...` label —
        // so we close at the session root.
        cadence_reporter.close_path(&Labels::of("session", &session.id));

    } else {
        // --- Single-activity execution ---
        let ops = all_ops_for_compile;
        let op_sequence = OpSequence::from_ops(ops, seq_type);

        let cycles = if let Some(c) = resolved_cli_cycles {
            if explicit_cycles.is_none() && params.contains_key("cycles") {
                crate::diag!(crate::observer::LogLevel::Debug, "cycles={c} (from GK constant)");
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
            rate,
            sequencer: seq_type,
            error_spec,
            max_retries: 3,
            stanza_concurrency,
            source_factory: None,
            // Live shared flag — the TuiObserver hands its
            // `tui_active` AtomicBool here so a `q` keypress
            // mid-run drops the flag and the activity's inline
            // status thread resumes emission. tui=off
            // observers return None and we synthesize a
            // never-suppressed flag, matching pre-change
            // semantics.
            suppress_status_line: observer.live_suppress_flag()
                .unwrap_or_else(|| {
                    Arc::new(std::sync::atomic::AtomicBool::new(
                        observer.suppresses_stderr()))
                }),
        };

        let adapter = create_adapter(&driver, &merged_params, dry_run).await?;
        let labels = Labels::of("session", &session.id).with("activity", "main");
        // SRD 40: resolve hdr.sigdigs from the session root
        // (walk-up) so this activity's timers/histograms use the
        // configured precision. Falls back to the project default
        // if no ancestor declares the property.
        let sigdigs = nbrs_metrics::instruments::histogram::resolve_hdr_sigdigs(
            &session.component.read().unwrap_or_else(|e| e.into_inner()),
        );
        let mut activity = Activity::with_params_and_sigdigs(
            config, &labels, op_sequence, workload_params, sigdigs,
        );
        let shared_metrics = activity.shared_metrics();

        // Attach the single activity as a component of the session
        // tree so the session-level scheduler captures its metrics
        // via the same `CadenceReporter` → `MetricsQuery` path used
        // by the phased execution. No separate per-activity capture
        // thread needed; no direct Reporter calls.
        let activity_component = Arc::new(std::sync::RwLock::new(
            nbrs_metrics::component::Component::new(labels.clone(), HashMap::new()),
        ));
        nbrs_metrics::component::attach(&session.component, &activity_component);
        {
            let mut ac = activity_component.write().unwrap_or_else(|e| e.into_inner());
            ac.set_instruments(shared_metrics.clone());
            ac.set_state(nbrs_metrics::component::ComponentState::Running);
        }

        // Wire the activity component back onto the Activity so
        // `run_with_adapters` can declare the `concurrency`
        // control on it (SRD 23 §"Fiber executor").
        activity.attach_component(activity_component.clone());

        let stopped = activity.run_with_driver(
            adapter,
            Arc::new(crate::synthesis::OpBuilder::from_program(program)),
        ).await;

        // Workload-end lifecycle boundary for the single-activity
        // path. Capture the final delta, ingest, then close the
        // activity's path so the trailing partial is published
        // immediately rather than idling until session shutdown.
        {
            use nbrs_metrics::component::InstrumentSet;
            let final_delta = shared_metrics.capture_delta(
                std::time::Duration::from_secs(1),
            );
            cadence_reporter.ingest(&labels, final_delta);
            cadence_reporter.close_path(&labels);
        }
        {
            let mut ac = activity_component.write().unwrap_or_else(|e| e.into_inner());
            ac.set_state(nbrs_metrics::component::ComponentState::Stopped);
        }

        if stopped {
            return Err("activity stopped by error handler".into());
        }
    }

    // Session-end lifecycle boundary. Close the session root path
    // for any aggregate windows that were ingested at session level
    // (rare today, but the boundary must be explicit — otherwise
    // session-level aggregates would only flush during
    // `shutdown_flush` at the very end, after all the per-subscriber
    // teardown logic had already started).
    cadence_reporter.close_path(&Labels::of("session", &session.id));

    // Stop profiler and scheduler now that execution (phased or
    // single-activity) is done. `finish()` is idempotent — Drop
    // will also call it on early returns / SIGINT-driven
    // shutdowns, so the flamegraph SVG lands regardless.
    if let Some(ref mut profiler) = _profiler {
        profiler.finish();
    }
    if let Ok(mut sh) = Arc::try_unwrap(stop_handle) {
        sh.stop();
    }

    // Shutdown the cadence reporter: flush trailing partials through
    // every cascade layer AND drain every subscriber's channel. This
    // MUST happen before reading any sink for the summary — otherwise
    // short phases (e.g. ann_query under a 30s cadence) contribute no
    // rows because their data is still sitting in an unclosed window.
    cadence_reporter.shutdown();

    observer.run_finished();

    if dry_run.is_some() {
        crate::diag!(crate::observer::LogLevel::Info, "dry-run complete.");
    } else {
        crate::diag!(crate::observer::LogLevel::Info, "done.");
    }

    // Build the active set of named summaries.
    //
    // Precedence:
    //   - CLI `summary=<spec>` wins outright — produces a single
    //     ad-hoc summary under the synthetic name `default`,
    //     overriding any workload-declared map. Matches prior
    //     CLI behavior.
    //   - Otherwise the workload's `summary:` map (and the
    //     `summary.yaml` sidecar fallback already merged into
    //     `workload_summaries` above) is used as-is.
    //
    // An empty map means "no summary at end of run" — same as
    // the legacy "no `summary:` field" case.
    let active_summaries: HashMap<String, nbrs_workload::model::SummaryConfig> =
        if let Some(cli_summary) = merged_params.get("summary") {
            let mut m = HashMap::new();
            m.insert("default".into(),
                nbrs_workload::model::SummaryConfig::parse(cli_summary));
            m
        } else {
            workload_summaries.clone()
        };

    // SRD-46 Details auto-injection: persist run-wide context
    // (end time, phase + scenario counts, adapter, …) into
    // session_metadata regardless of whether the workload
    // declared a `report:` block. Post-run hooks read this to
    // build the auto-injected Details section that lands at
    // the top of every output markdown file.
    if let Ok(mut guard) = sqlite_reporter.lock()
        && let Some(ref mut reporter) = *guard {
        let end_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs())
            .unwrap_or(0);
        reporter.set_metadata("end_time", &end_time.to_string());
        reporter.set_metadata("phase_count", &phases.len().to_string());
        reporter.set_metadata("scenario_count", &scenarios.len().to_string());
        if let Some(wf) = workload_file.as_deref() {
            reporter.set_metadata("workload_file", wf);
        }
        reporter.set_metadata("adapter", &driver);
    }

    if !active_summaries.is_empty() {
        // Summary report always comes from SQLite — the
        // durable record. The in-memory store exists for GK
        // access and reactive control, not for reporting.
        if let Ok(mut guard) = sqlite_reporter.lock() {
            if let Some(ref mut reporter) = *guard {
                // Persist every report item (SRD-46) under
                // `report.<name>` keys. Value carries the kind
                // keyword (`plot ...` / `table ...`) followed
                // by an optional `label "..."` line and then
                // the spec body — same shape the report parser
                // ingests, so the db-fallback path in
                // `nbrs report` round-trips through the same
                // parser the workload uses.
                for item in workload_report.items() {
                    let mut value = String::new();
                    value.push_str(item.kind.as_str());
                    value.push(' ');
                    value.push_str(&item.name);
                    value.push('\n');
                    if let Some(label) = item.label.as_deref() {
                        value.push_str("label ");
                        value.push('"');
                        value.push_str(label);
                        value.push_str("\"\n");
                    }
                    if let Some(tf) = item.target_file.as_deref() {
                        value.push_str("target ");
                        value.push_str(tf);
                        value.push('\n');
                    }
                    value.push_str(&item.body);
                    reporter.set_metadata(
                        &format!("report.{}", item.name), &value);
                }

                // Stable ordering for consistent output across
                // runs (HashMap iteration is non-deterministic).
                let mut names: Vec<&String> = active_summaries.keys().collect();
                names.sort();
                for name in names {
                    let cfg = &active_summaries[name];
                    let (basename, format) =
                        nbrs_metrics::reporters::sqlite::derive_name_and_format(name);
                    let report_config = report_config_from_summary(cfg);
                    let rendered = reporter.format_summary_with_format(
                        &report_config, &format);
                    if rendered.is_empty() { continue; }
                    let filename = format!("{basename}_summary.{format}");
                    let summary_path = session.output_dir.join(&filename);
                    if let Err(e) = std::fs::write(&summary_path, &rendered) {
                        crate::diag!(crate::observer::LogLevel::Warn,
                            "warning: failed to write summary to {}: {e}",
                            summary_path.display());
                    } else {
                        crate::diag!(crate::observer::LogLevel::Info,
                            "summary: {}", summary_path.display());
                    }
                    // Inline print only when the observer is
                    // not suppressing stderr — i.e. we're in
                    // tui=off mode and the user can see stdout
                    // right now. In TUI mode the alternate
                    // screen is up, so `print!()` here would
                    // get buffered behind the TUI rendering and
                    // discarded on teardown. The persona reads
                    // the *_summary.* files and prints them
                    // post-shutdown (see `nbrs/src/run.rs`).
                    if !observer.suppresses_stderr() {
                        print!("{rendered}");
                    }
                }
            }
        }
    }

    // Refresh convenience symlinks at the logs/ root so
    //   logs/metrics.db, logs/summary.md, logs/session.log
    // always resolve to this session's artifacts. `logs/latest` (a
    // symlink to the whole session dir) is created by Session::new;
    // these are per-file counterparts for direct tool access like
    // `sqlite3 logs/metrics.db` or `tail -f logs/session.log`.
    refresh_latest_file_links(&session);

    // dryrun=controls: every phase has now been constructed (at
    // depth=Phase the executor stops before cycles but still
    // attaches components and declares controls). Walk the
    // session tree and dump the catalog.
    if diag.list_controls {
        let mut out = std::io::stdout();
        if let Err(e) = render_controls_tree(&session.component, &mut out) {
            crate::diag!(crate::observer::LogLevel::Warn,
                "warning: rendering controls: {e}");
        }
    }

    Ok(())
}

/// Point per-file symlinks under `logs/` at the latest session's
/// artifacts. Silently skips files that don't exist (e.g. summary.md
/// when no `summary:` was declared). Replaces any existing symlink.
fn refresh_latest_file_links(session: &crate::session::Session) {
    let logs_dir = std::path::Path::new("logs");
    for file in ["metrics.db", "summary.md", "session.log"] {
        let target = session.output_dir.join(file);
        if !target.exists() { continue; }
        let link = logs_dir.join(file);
        // Remove any existing entry (symlink or regular file) so we can
        // recreate the link. If this fails because nothing's there,
        // that's fine.
        let _ = std::fs::remove_file(&link);
        // Link targets are relative to logs/ so the symlink survives
        // directory moves — `{session_id}/{file}` under logs/.
        let rel_target = std::path::Path::new(&session.id).join(file);
        if let Err(e) = std::os::unix::fs::symlink(&rel_target, &link) {
            crate::diag!(crate::observer::LogLevel::Warn,
                "warning: failed to link {} → {}: {e}",
                link.display(), rel_target.display());
        }
    }
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

/// Run an activity without its own capture thread.
///
/// All metrics flow through the session-level scheduler →
/// `CadenceReporter` → `MetricsQuery`. This function just runs the
/// activity to completion; lifecycle flush (final delta +
/// validation metrics) is handled by the caller (executor).
pub async fn run_activity_simple(
    activity: Activity,
    adapters: std::collections::HashMap<String, Arc<dyn crate::adapter::DriverAdapter>>,
    default_adapter: &str,
    op_builder: Arc<crate::synthesis::OpBuilder>,
) -> bool {
    activity.run_with_adapters(adapters, default_adapter, op_builder).await
}

/// Adapter that delegates to an `Arc<Mutex<Option<SqliteReporter>>>`.
///
/// Allows the SQLite reporter to be registered on the scheduler while
/// also being accessible for summary queries after the scheduler stops.
struct MutexReporter(std::sync::Arc<std::sync::Mutex<Option<nbrs_metrics::reporters::sqlite::SqliteReporter>>>);

impl Reporter for MutexReporter {
    fn report(&mut self, snapshot: &nbrs_metrics::snapshot::MetricSet) {
        if let Ok(mut guard) = self.0.lock() {
            if let Some(ref mut r) = *guard {
                Reporter::report(r, snapshot);
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
    fn report(&mut self, snapshot: &nbrs_metrics::snapshot::MetricSet) {
        self.0.report(snapshot);
    }
    fn flush(&mut self) {
        self.0.flush();
    }
}


/// Dry-run adapter: emit ops to stdout or silently discard.
struct DryRunAdapter {
    mode: String,
}

impl crate::adapter::DriverAdapter for DryRunAdapter {
    fn name(&self) -> &str { "dry-run" }

    fn map_op(&self, _template: &nbrs_workload::model::ParsedOp)
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
    fn execute<'a>(&'a self, _cycle: u64, ctx: &'a crate::fixture::ExecCtx<'a>)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::adapter::OpResult, crate::adapter::ExecutionError>> + Send + 'a>>
    {
        let mode = &self.mode;
        let fields = ctx.fields;
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

/// Collected param references from a workload, separating direct
/// `{name}` references from composite-name templates like
/// `{k_{k}_limits}` whose ground form depends on a runtime
/// substitution.
///
/// Used by the workload param validator to recognize that a
/// declared param like `k_1_limits` is genuinely referenced
/// when the workload uses `{k_{k}_limits}` and `k` ranges over
/// values including `1`.
#[derive(Default)]
struct ParamRefs {
    /// Names that appeared directly as `{name}` placeholders.
    direct: std::collections::HashSet<String>,
    /// Composite templates: the literal body of a `{...}` whose
    /// inner content contained nested `{...}`. Stored verbatim
    /// (e.g. `"k_{k}_limits"`); validation checks each declared
    /// param name against these templates by replacing each
    /// inner `{NAME}` with a word-character wildcard.
    templates: Vec<String>,
}

impl ParamRefs {
    /// Does `param` appear either directly or via composition?
    fn contains(&self, param: &str) -> bool {
        if self.direct.contains(param) { return true; }
        self.templates.iter().any(|tpl| template_matches(tpl, param))
    }
}

/// Match a declared param name against a composite-name template.
///
/// `template` is the body of a `{...}` reference whose content
/// included nested `{...}` substitutions — e.g. `k_{k}_limits`.
/// Each inner `{NAME}` matches one or more word characters
/// (`[A-Za-z0-9_]+`); the surrounding literal chars must match
/// exactly. Returns `true` iff `param` exactly matches the
/// template's ground form for some substitution of the inner
/// names.
fn template_matches(template: &str, param: &str) -> bool {
    let t = template.as_bytes();
    let p = param.as_bytes();
    let mut ti = 0;
    let mut pi = 0;
    while ti < t.len() {
        if t[ti] == b'{' {
            // Skip past the inner {...}. The template body
            // doesn't nest deeper than one level in practice
            // (composed names like `{k_{k}_limits}` don't
            // contain `{a_{b}_c}` recursively); a simple
            // first-`}` lookup suffices.
            let close = match template[ti + 1..].find('}') {
                Some(n) => ti + 1 + n,
                None => return false, // malformed template
            };
            // Determine where the template's literal context
            // resumes after the inner placeholder.
            let next_lit = close + 1;
            // The next literal char (or end-of-template) bounds
            // how far the wildcard can consume.
            if next_lit >= t.len() {
                // Wildcard must consume the rest of `param`,
                // and that suffix must be at least one word char.
                if pi >= p.len() { return false; }
                return p[pi..].iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_');
            }
            let stop = t[next_lit];
            // Greedy-match word chars in param up to the next
            // literal in template.
            let mut consumed = 0;
            while pi + consumed < p.len() && p[pi + consumed] != stop {
                let b = p[pi + consumed];
                if !(b.is_ascii_alphanumeric() || b == b'_') {
                    return false;
                }
                consumed += 1;
            }
            if consumed == 0 { return false; } // wildcard requires ≥1 char
            pi += consumed;
            ti = next_lit;
        } else {
            // Literal char: must match.
            if pi >= p.len() || p[pi] != t[ti] { return false; }
            ti += 1;
            pi += 1;
        }
    }
    pi == p.len()
}

/// Scan a string for `{name}` references and `{composite_{x}_name}`
/// templates, accumulating into `refs`.
///
/// Plain `{name}` placeholders (where the body is a single
/// identifier — alphanumerics + underscore, leading non-digit)
/// are recorded as direct references. A `{...}` whose body
/// contains nested `{...}` is recorded as a template; the inner
/// leaf names are also recorded as direct references because
/// they're the substitution inputs (e.g. `{k_{k}_limits}`
/// records the template `k_{k}_limits` AND the direct ref `k`).
/// Walk a `serde_json::Value` and call [`scan_param_refs`] on
/// every string leaf. Used by [`collect_param_references`] to
/// reach `{name}` references nested inside structured
/// `params:` blocks (e.g. `relevancy: { expected: "{ground_truth}" }`).
fn scan_json_for_refs(v: &serde_json::Value, refs: &mut ParamRefs) {
    match v {
        serde_json::Value::String(s) => scan_param_refs(s, refs),
        serde_json::Value::Array(a) => {
            for item in a { scan_json_for_refs(item, refs); }
        }
        serde_json::Value::Object(m) => {
            for item in m.values() { scan_json_for_refs(item, refs); }
        }
        _ => {} // numbers, booleans, null — no string content
    }
}

fn scan_param_refs(text: &str, refs: &mut ParamRefs) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'{' {
            i += 1;
            continue;
        }
        // Find the matching `}`, balancing nested `{`s.
        let body_start = i + 1;
        let mut depth = 1;
        let mut j = body_start;
        while j < bytes.len() && depth > 0 {
            match bytes[j] {
                b'{' => depth += 1,
                b'}' => depth -= 1,
                _ => {}
            }
            if depth == 0 { break; }
            j += 1;
        }
        if depth != 0 {
            // Unmatched `{` — bail, treat as literal.
            break;
        }
        let body = &text[body_start..j];
        if body.contains('{') {
            // Composite template (e.g. `k_{k}_limits`).
            refs.templates.push(body.to_string());
            // Recurse into the body to pick up the inner leaf
            // names as direct references.
            scan_param_refs(body, refs);
        } else if !body.is_empty()
            && body.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            && !body.bytes().next().unwrap().is_ascii_digit()
        {
            // Plain `{name}` — direct reference.
            refs.direct.insert(body.to_string());
        } else {
            // Inline GK expression body, e.g.
            // `{is_one_of(cassandra_dialect, "cndb")}` or
            // `{:=mod(hash(cycle), 100):=}`. Walk the body,
            // collect identifier-shaped tokens that aren't
            // inside string literals — those are GK name
            // references which may resolve to workload params.
            // Over-collecting (function names, GK stdlib
            // identifiers) is harmless — the validator's
            // membership test below uses the workload's own
            // declared params as the universe of interest.
            scan_expression_idents(body, &mut refs.direct);
        }
        i = j + 1;
    }
}

/// Walk a GK-expression body (no surrounding `{}`) and add any
/// identifier-shaped tokens to `out`. Skips identifiers nested
/// inside `"..."` or `'...'` string literals — those are CQL /
/// regex / display strings, not name references. Recognises
/// backslash escapes inside string literals.
///
/// This is best-effort: it doesn't honor GK lexer subtleties
/// (numeric suffixes, raw strings, etc.). For the unused-param
/// check in `runner.rs::collect_param_references` the goal is
/// "does the param name appear anywhere we'd evaluate it" — a
/// loose match is correct because false positives only mean
/// "param is considered used when it might not have been",
/// which is the safer failure mode.
fn scan_expression_idents(body: &str, out: &mut std::collections::HashSet<String>) {
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        // Skip string literals.
        if b == b'"' || b == b'\'' {
            let quote = b;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Identifier start: ASCII letter or underscore.
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            let ident = &body[start..i];
            // Skip the few literals the GK lexer also recognises
            // — they're definitely not param names.
            if ident != "true" && ident != "false" {
                out.insert(ident.to_string());
            }
            continue;
        }
        i += 1;
    }
}

/// Collect all `{name}` param references from a workload's ops,
/// phases, bindings, and scenario tree. Returns both direct
/// refs and composite templates so the validator can recognize
/// dynamic-name-composition references like `{k_{k}_limits}`.
fn collect_param_references(workload: &nbrs_workload::model::Workload) -> ParamRefs {
    let mut refs = ParamRefs::default();

    // Local helper so every op-bearing scope (top-level + per-
    // phase) hits the same set of fields. Critically this
    // includes the *core* fields the parser hoists out of the
    // op map — `if:` (condition), `delay:`, and the
    // serde_json values inside `params:`. Missing any of those
    // produced false positives on the unused-param check
    // (e.g. `if: '{is_one_of(cassandra_dialect, "cndb")}'`
    // landed in `condition` rather than `op.op`, so the
    // workload param `cassandra_dialect` looked unreferenced).
    fn scan_op(op: &nbrs_workload::model::ParsedOp, refs: &mut ParamRefs) {
        for value in op.op.values() {
            if let serde_json::Value::String(s) = value {
                scan_param_refs(s, refs);
            }
        }
        if let Some(s) = &op.condition { scan_param_refs(s, refs); }
        if let Some(s) = &op.delay { scan_param_refs(s, refs); }
        // `params:` values can be strings, numbers, nested
        // maps (e.g. `relevancy: { actual: key, expected: …}`).
        // Walk the JSON recursively so anything stringy gets
        // scanned regardless of nesting depth.
        for v in op.params.values() {
            scan_json_for_refs(v, refs);
        }
        match &op.bindings {
            nbrs_workload::model::BindingsDef::GkSource(s) => scan_param_refs(s, refs),
            nbrs_workload::model::BindingsDef::Map(m) => {
                for v in m.values() { scan_param_refs(v, refs); }
            }
        }
    }

    // Scan top-level ops
    for op in &workload.ops {
        scan_op(op, &mut refs);
    }

    // Scan phases
    for phase in workload.phases.values() {
        if let Some(s) = &phase.cycles { scan_param_refs(s, &mut refs); }
        if let Some(s) = &phase.concurrency { scan_param_refs(s, &mut refs); }
        if let Some(s) = &phase.for_each { scan_param_refs(s, &mut refs); }
        for op in &phase.ops {
            scan_op(op, &mut refs);
        }
    }

    // Scan scenario tree — every node kind contributes its
    // `{...}`-bearing fields. DoWhile/DoUntil contribute their
    // condition text; ForEach/ForCombinations/ForEachUnion
    // contribute their iteration specs.
    fn scan_scenario_nodes(
        nodes: &[nbrs_workload::model::ScenarioNode],
        refs: &mut ParamRefs,
    ) {
        for node in nodes {
            match node {
                nbrs_workload::model::ScenarioNode::Phase(_) => {}
                nbrs_workload::model::ScenarioNode::Comprehension { comprehension, children } => {
                    for clause in comprehension.flat_clauses() {
                        scan_param_refs(&clause.expr, refs);
                    }
                    scan_scenario_nodes(children, refs);
                }
                nbrs_workload::model::ScenarioNode::DoWhile { condition, children, .. }
                | nbrs_workload::model::ScenarioNode::DoUntil { condition, children, .. } => {
                    scan_param_refs(condition, refs);
                    scan_scenario_nodes(children, refs);
                }
                nbrs_workload::model::ScenarioNode::IncludedScenario { children, .. } => {
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

/// Resolve a config value to u64 via GK scope lookup or numeric parsing.
pub fn resolve_gk_config(value: &str, kernel: &nbrs_variates::kernel::GkKernel) -> Option<u64> {
    if value.starts_with('{') && value.ends_with('}') {
        let inner = &value[1..value.len() - 1];
        // SRD-16 §"Visibility Rules: Shadowing": `lookup`
        // walks own folded outputs first then the cell-aware
        // input slot, so a config reference like `{cycles}`
        // resolves whether `cycles` is a folded constant or
        // an extern bound from an outer scope. The previous
        // `get_constant` shape only saw the folded tier, so
        // configs referencing iter-vars or workload params
        // silently fell through to `eval_const_expr`.
        if let Some(v) = kernel.lookup(inner) {
            return Some(value_to_u64(&v));
        }
        match nbrs_variates::dsl::compile::eval_const_expr(inner) {
            Ok(v) => Some(value_to_u64(&v)),
            Err(e) => {
                crate::diag!(crate::observer::LogLevel::Error, "error: const expression failed: '{{{inner}}}'");
                crate::diag!(crate::observer::LogLevel::Error, "  {e}");
                None
            }
        }
    } else {
        parse_count(value)
    }
}

/// Convert a GK Value to u64, handling f64→u64 truncation.
fn value_to_u64(v: &nbrs_variates::node::Value) -> u64 {
    match v {
        nbrs_variates::node::Value::U64(n) => *n,
        nbrs_variates::node::Value::F64(f) => *f as u64,
        nbrs_variates::node::Value::Bool(b) => if *b { 1 } else { 0 },
        _ => 0,
    }
}

/// Resolve a scenario name to a list of phase names.
fn resolve_scenario(
    scenarios: &HashMap<String, Vec<nbrs_workload::model::ScenarioNode>>,
    phase_order: &[String],
    name: &str,
) -> Result<Vec<nbrs_workload::model::ScenarioNode>, String> {
    if let Some(nodes) = scenarios.get(name) {
        return Ok(nodes.clone());
    }
    if name == "default" && !phase_order.is_empty() {
        return Ok(phase_order.iter()
            .map(|n| nbrs_workload::model::ScenarioNode::Phase(n.clone()))
            .collect());
    }
    Err(format!("scenario '{name}' not found"))
}

/// Format a scenario tree for display.
fn format_scenario_tree(nodes: &[nbrs_workload::model::ScenarioNode]) -> String {
    use nbrs_variates::comprehension::ComprehensionMode;
    let parts: Vec<String> = nodes.iter().map(|n| match n {
        nbrs_workload::model::ScenarioNode::Phase(name) => name.clone(),
        nbrs_workload::model::ScenarioNode::Comprehension { comprehension, children } => {
            let inner = format_scenario_tree(children);
            match &comprehension.mode {
                ComprehensionMode::Cartesian(clauses) if clauses.len() == 1 => {
                    format!("for_each {}: [{inner}]", clauses[0].var)
                }
                ComprehensionMode::Cartesian(clauses) => {
                    let vars: Vec<&str> = clauses.iter().map(|c| c.var.as_str()).collect();
                    format!("for_combinations [{}]: [{inner}]", vars.join(", "))
                }
                ComprehensionMode::Union(subspaces) => {
                    let names = comprehension.coordinate_names().join(", ");
                    format!("for_each_union [{}] ({} sub-spaces): [{inner}]",
                        names, subspaces.len())
                }
            }
        }
        nbrs_workload::model::ScenarioNode::DoWhile { condition, counter, children } => {
            let inner = format_scenario_tree(children);
            let ctr = counter.as_deref().map(|c| format!(" ({c})")).unwrap_or_default();
            format!("do_while '{condition}'{ctr}: [{inner}]")
        }
        nbrs_workload::model::ScenarioNode::DoUntil { condition, counter, children } => {
            let inner = format_scenario_tree(children);
            let ctr = counter.as_deref().map(|c| format!(" ({c})")).unwrap_or_default();
            format!("do_until '{condition}'{ctr}: [{inner}]")
        }
        nbrs_workload::model::ScenarioNode::IncludedScenario { name, children } => {
            let inner = format_scenario_tree(children);
            format!("scenario '{name}': [{inner}]")
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

/// Bare flags accepted by the runner — these don't follow the
/// `key=value` shape but are otherwise recognized. Centralized
/// here so [`parse_params`] doesn't reject them and any consumer
/// can re-check the raw `args` for them.
const RECOGNIZED_BARE_FLAGS: &[&str] = &[
    "--strict",              // SRD-15 strict-mode toggle.
    "--resume-latest",       // SRD-44: resume from logs/latest.
    "--force-retry-failed",  // SRD-44: prepend retry,warn to errors.
];

/// Parse `key=value` pairs from command line args.
pub fn parse_params(args: &[String]) -> HashMap<String, String> {
    // Flags consumed by `crate::session::resolve_session_dir`
    // at startup. They appear in raw `args` but shouldn't reach
    // the per-key params map. Both equals-form
    // (`--session-dir=/path`) and space-form
    // (`--session-dir /path`) are recognised; the space-form
    // value is silently absorbed.
    const SESSION_DIR_FLAGS: &[&str] = &[
        // Umbrella flag (kv-list).
        "--session",
        // Per-key long-form flags.
        "--session-name", "--session-path", "--session-reuse",
        "--session-keep", "--session-shelflife",
    ];
    let mut params = HashMap::new();
    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        // Session-dir flags (consumed by the startup hook,
        // not stored in params).
        if SESSION_DIR_FLAGS.iter().any(|p| {
            arg == p || arg.starts_with(&format!("{p}="))
        }) {
            if !arg.contains('=') {
                let _consumed = iter.next();
            }
            continue;
        }

        // Strip leading dashes: --dryrun=phase,gk → dryrun=phase,gk
        let stripped = arg.trim_start_matches('-');
        if let Some(eq_pos) = stripped.find('=') {
            let key = stripped[..eq_pos].to_string();
            let value = stripped[eq_pos + 1..].to_string();
            params.insert(key, value);
        } else if arg.ends_with(".yaml") || arg.ends_with(".yml") {
            // Workload file path — handled elsewhere
        } else if RECOGNIZED_BARE_FLAGS.contains(&arg.as_str())
            || arg.starts_with("--gk-lib=")
        {
            // Bare runner flag — consumed elsewhere via `args`
            // scan (e.g. `--strict`, `--gk-lib=path`).
        } else {
            crate::diag!(crate::observer::LogLevel::Error, "error: unrecognized argument '{arg}'. Expected key=value format.");
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

// `ManifestEntry` and `extract_manifest` now live in
// `nbrs_variates::kernel`. Re-exported here so existing
// `crate::runner::extract_manifest` / `ManifestEntry` callers
// keep working — pure compatibility shim.
pub use nbrs_variates::kernel::{extract_manifest, ManifestEntry};



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dryrun_controls_sets_list_flag() {
        let cfg = DiagnosticConfig::parse("controls");
        assert!(cfg.list_controls);
        // Implies phase depth so the runner exits before any
        // cycle-time work.
        assert_eq!(cfg.depth, ExecDepth::Phase);
    }

    #[test]
    fn parse_dryrun_controls_combines_with_other_flags() {
        let cfg = DiagnosticConfig::parse("controls,labels");
        assert!(cfg.list_controls);
        assert!(cfg.show_labels);
    }

    #[test]
    fn parse_dryrun_unknown_flag_does_not_set_controls() {
        let cfg = DiagnosticConfig::parse("phase,bogus");
        assert!(!cfg.list_controls);
    }

    #[test]
    fn render_controls_tree_empty_session_writes_placeholder() {
        let root = nbrs_metrics::component::Component::root(
            nbrs_metrics::labels::Labels::of("session", "t"),
            std::collections::HashMap::new(),
        );
        let mut buf: Vec<u8> = Vec::new();
        render_controls_tree(&root, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("no controls declared"), "got: {s}");
    }

    #[test]
    fn render_controls_tree_lists_session_root_controls() {
        let root = nbrs_metrics::component::Component::root(
            nbrs_metrics::labels::Labels::of("session", "t"),
            std::collections::HashMap::new(),
        );
        root.read().unwrap().controls().declare(
            nbrs_metrics::controls::ControlBuilder::new("log_level", 1u32)
                .reify_as_gauge(|v| Some(*v as f64))
                .branch_scope(nbrs_metrics::controls::BranchScope::Subtree)
                .from_f64(|v| Ok(v as u32))
                .final_at_scope("session_root")
                .build(),
        );

        let mut buf: Vec<u8> = Vec::new();
        render_controls_tree(&root, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("log_level"), "missing name: {s}");
        assert!(s.contains("scope=subtree"), "missing scope: {s}");
        assert!(s.contains("final@session_root"), "missing final marker: {s}");
        assert!(s.contains("f64-writable"), "missing write surface: {s}");
    }
}

