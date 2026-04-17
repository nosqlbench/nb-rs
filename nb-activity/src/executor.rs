// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Recursive scenario tree executor.
//!
//! Walks `ScenarioNode` trees dynamically at runtime. All control
//! flow constructs (`for_each`, `do_while`, `do_until`) are evaluated
//! uniformly — no pre-flattening. GK scope composition handles
//! variable scoping at every nesting level.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::path::PathBuf;

use crate::activity::{Activity, ActivityConfig};
use crate::adapter::DriverAdapter;
use crate::opseq::{OpSequence, SequencerType};
use crate::synthesis::OpBuilder;
use nb_metrics::component::{self, Component, ComponentState};
use nb_metrics::labels::Labels;
use nb_metrics::store::InProcessMetricsStore;
use nb_workload::model::{ScenarioNode, WorkloadPhase};

/// Shared context for the recursive executor.
pub struct ExecCtx {
    pub phases: HashMap<String, WorkloadPhase>,
    pub workload_params: HashMap<String, String>,
    pub outer_scope_values: Vec<(String, nb_variates::node::Value)>,
    pub outer_manifest: Vec<crate::runner::ManifestEntry>,
    pub program: Arc<nb_variates::kernel::GkProgram>,
    pub gk_lib_paths: Vec<PathBuf>,
    pub workload_dir: Option<PathBuf>,
    pub strict: bool,
    pub driver: String,
    pub merged_params: HashMap<String, String>,
    pub dry_run: Option<&'static str>,
    pub diag: crate::runner::DiagnosticConfig,
    pub openmetrics_url: Option<String>,
    pub seq_type: SequencerType,
    pub concurrency: usize,
    pub cycle_rate: Option<f64>,
    pub error_spec: String,
    /// Session identifier for metric labeling.
    pub session_id: String,
    /// Label stack: accumulated dimensional labels from the component tree.
    /// for_each pushes (var, value), phase pushes ("phase", name).
    /// do_while/do_until are transparent — they don't contribute labels.
    pub label_stack: Vec<(String, String)>,
    /// Session root component (owns the component tree).
    pub session_component: Arc<RwLock<Component>>,
    /// In-process metrics store for lifecycle flush.
    pub store: Arc<RwLock<InProcessMetricsStore>>,
    /// Scheduler stop handle for delivering frames to reporters.
    pub stop_handle: Arc<nb_metrics::scheduler::StopHandle>,
    /// Run observer for phase lifecycle events (TUI or stderr).
    pub observer: Arc<dyn crate::observer::RunObserver>,
}

impl ExecCtx {
    /// Build Labels from the current label stack.
    pub fn labels(&self) -> Labels {
        let mut labels = Labels::of("session", &self.session_id);
        for (k, v) in &self.label_stack {
            labels = labels.with(k, v);
        }
        labels
    }

    /// Push a label onto the stack.
    pub fn push_label(&mut self, key: &str, value: &str) {
        self.label_stack.push((key.to_string(), value.to_string()));
    }

    /// Pop the top label from the stack.
    pub fn pop_label(&mut self) {
        self.label_stack.pop();
    }

    /// Whether stderr diagnostic output is suppressed (TUI handles display).
    pub fn quiet(&self) -> bool {
        self.observer.suppresses_stderr()
    }
}

/// Execute a scenario tree recursively.
pub fn execute_tree<'a>(
    ctx: &'a mut ExecCtx,
    nodes: &'a [ScenarioNode],
    bindings: &'a HashMap<String, String>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
    Box::pin(async move {
    for node in nodes {
        match node {
            ScenarioNode::Phase(name) => {
                // Lift phase-level for_each
                let phase_fe = ctx.phases.get(name.as_str())
                    .and_then(|p| p.for_each.clone());
                if let Some(spec) = phase_fe {
                    let (var, values) = resolve_expr(
                        &spec, &ctx.workload_params, bindings, &ctx.outer_scope_values)?;
                    if !ctx.quiet() {
                        crate::diag!(crate::observer::LogLevel::Debug, "  for_each {var} in [{}] ({} iterations)",
                            values.join(", "), values.len());
                    }
                    for value in &values {
                        let mut inner = bindings.clone();
                        inner.insert(var.clone(), value.clone());
                        // for_each pushes label
                        ctx.push_label(&var, &value);
                        run_phase(ctx, name, &inner).await?;
                        ctx.pop_label();
                    }
                } else {
                    run_phase(ctx, name, bindings).await?;
                }
            }
            ScenarioNode::ForEach { spec, children } => {
                let (var, values) = resolve_expr(
                    spec, &ctx.workload_params, bindings, &ctx.outer_scope_values)?;
                if !ctx.quiet() {
                    crate::diag!(crate::observer::LogLevel::Debug, "for_each {var} in [{}] ({} iterations × {} children)",
                        values.join(", "), values.len(), children.len());
                }
                for value in &values {
                    let mut inner = bindings.clone();
                    inner.insert(var.clone(), value.clone());
                    // for_each pushes label: var=value
                    ctx.push_label(&var, &value);
                    execute_tree(ctx, children, &inner).await?;
                    ctx.pop_label();
                }
            }
            ScenarioNode::ForCombinations { specs, children } => {
                // Resolve dimensions lazily: later dimensions may reference
                // earlier variables (e.g., "table in vec_{profile}").
                // Each dimension is resolved inside the Cartesian recursion
                // after prior variables are bound.
                fn cartesian_recurse<'a>(
                    ctx: &'a mut ExecCtx,
                    specs: &'a [(String, String)],
                    dim_idx: usize,
                    bindings: &'a HashMap<String, String>,
                    children: &'a [ScenarioNode],
                    first: bool,
                ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
                    Box::pin(async move {
                        if dim_idx >= specs.len() {
                            execute_tree(ctx, children, bindings).await?;
                            return Ok(());
                        }
                        let (var, expr) = &specs[dim_idx];
                        // Substitute already-bound variables into the expression
                        let mut resolved_expr = expr.clone();
                        for (k, v) in bindings {
                            resolved_expr = resolved_expr.replace(&format!("{{{k}}}"), v);
                        }
                        let (_, values) = resolve_expr(
                            &format!("{var} in {resolved_expr}"),
                            &ctx.workload_params, bindings, &ctx.outer_scope_values)?;

                        if first {
                            // Build dimension summary for display
                            let mut dims: Vec<String> = Vec::new();
                            dims.push(format!("{var}({} values)", values.len()));
                            // Peek at remaining specs for display
                            for (v, _) in &specs[dim_idx + 1..] {
                                dims.push(v.clone());
                            }
                            let total_hint = values.len();
                            if !ctx.quiet() {
                                crate::diag!(crate::observer::LogLevel::Debug, "for_combinations [{}] ({total_hint}+ combinations × {} children)",
                                    dims.join(" × "), children.len());
                            }
                        }

                        for value in &values {
                            let mut inner = bindings.clone();
                            inner.insert(var.clone(), value.clone());
                            ctx.push_label(var, value);
                            cartesian_recurse(ctx, specs, dim_idx + 1, &inner, children, false).await?;
                            ctx.pop_label();
                        }
                        Ok(())
                    })
                }
                cartesian_recurse(ctx, specs, 0, bindings, children, true).await?;
            }
            ScenarioNode::DoWhile { condition, counter, children } => {
                // do_while is transparent to labels — no label push
                crate::diag!(crate::observer::LogLevel::Debug, "=== do_while: {condition} ===");
                let mut i = 0u64;
                loop {
                    let mut inner = bindings.clone();
                    if let Some(c) = counter { inner.insert(c.clone(), i.to_string()); }
                    execute_tree(ctx, children, &inner).await?;
                    i += 1;
                    // TODO: evaluate condition from last op result / GK expr
                    crate::diag!(crate::observer::LogLevel::Debug, "  (do_while: executed {i} iteration(s), condition eval pending)");
                    break; // stub: one iteration
                }
            }
            ScenarioNode::DoUntil { condition, counter, children } => {
                crate::diag!(crate::observer::LogLevel::Debug, "=== do_until: {condition} ===");
                let mut i = 0u64;
                loop {
                    let mut inner = bindings.clone();
                    if let Some(c) = counter { inner.insert(c.clone(), i.to_string()); }
                    execute_tree(ctx, children, &inner).await?;
                    i += 1;
                    // TODO: evaluate condition from last op result / GK expr
                    crate::diag!(crate::observer::LogLevel::Debug, "  (do_until: executed {i} iteration(s), condition eval pending)");
                    break; // stub: one iteration
                }
            }
        }
    }
    Ok(())
    }) // Box::pin(async move { ... })
}

/// Execute one phase with the given bindings.
async fn run_phase(
    ctx: &mut ExecCtx,
    phase_name: &str,
    bindings: &HashMap<String, String>,
) -> Result<(), String> {
    let phase_start = std::time::Instant::now();
    let phase = ctx.phases.get(phase_name)
        .ok_or_else(|| format!("phase '{phase_name}' not found"))?
        .clone();
    let has_bindings = phase.ops.iter().any(|op| !op.bindings.is_empty());
    let is_iter = !bindings.is_empty();

    if !ctx.observer.suppresses_stderr() {
        crate::diag!(crate::observer::LogLevel::Info, "=== phase: {phase_name} ===");
        if is_iter {
            for (var, val) in bindings {
                if !val.is_empty() { crate::diag!(crate::observer::LogLevel::Debug, "  {var}={val}"); }
            }
        }
    }

    // --- Compile inner kernel via BindingScope ---
    let (iter_program, iter_ops) = if is_iter || has_bindings {
        let mut ops = phase.ops.clone();

        // Text substitution: replace {var} placeholders in GK source
        // and op templates. This must happen before scope ingestion
        // because GK expressions may embed placeholders like
        // `vector_at(row, "{spec}")`.
        crate::scope::substitute_iteration_vars(&mut ops, bindings);
        crate::scope::substitute_workload_params(&mut ops, &ctx.workload_params);

        // Rewrite inline expressions ({{expr}} → {__expr_N}) in op templates.
        // This modifies op template strings and returns the expr→name map
        // so the scope can register the corresponding bindings.
        crate::scope::rewrite_inline_exprs(&mut ops);

        // Strip adapter/driver (resolved per-phase, not from params)
        for op in &mut ops { op.params.remove("adapter"); op.params.remove("driver"); }

        // Build typed scope from structured inputs
        let scope = crate::scope::build_scope(
            &ops,
            bindings,
            &ctx.outer_manifest,
            &ctx.workload_params,
            &ctx.phases,
            phase.cycles.as_deref(),
            &[], // exclude
        )?;

        // Validate scope rules (shadow detection, final checks)
        let gk_context = if bindings.is_empty() {
            format!("phase '{phase_name}'")
        } else {
            let vars: Vec<String> = bindings.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            format!("phase '{phase_name}' ({})", vars.join(", "))
        };
        scope.validate().map_err(|e| format!("{gk_context}: {e}"))?;

        // Compile from the validated scope
        let cursor_limit: Option<u64> = ctx.merged_params.get("limit")
            .and_then(|s| s.parse().ok());
        let mut kernel = crate::bindings::compile_from_scope(
            &scope,
            ctx.workload_dir.as_deref(),
            ctx.gk_lib_paths.clone(),
            ctx.strict,
            &gk_context,
            cursor_limit,
        ).map_err(|e| format!("{gk_context}: {e}"))?;

        // Wire outer scope values into the inner kernel's inputs
        for (name, value) in &ctx.outer_scope_values {
            if let Some(idx) = kernel.program().find_input(name) {
                kernel.state().set_input(idx, value.clone());
            }
        }
        let prog = Arc::new(OpBuilder::new(kernel)).program();
        (prog, ops)
    } else {
        (ctx.program.clone(), phase.ops.clone())
    };

    let op_sequence = OpSequence::from_ops(iter_ops, ctx.seq_type);
    if op_sequence.stanza_length() == 0 {
        crate::diag!(crate::observer::LogLevel::Warn, "warning: phase '{phase_name}' has no ops, skipping");
        return Ok(());
    }

    // Resolve cycles
    let stanza_len = op_sequence.stanza_length() as u64;
    let spec = phase.cycles.as_deref().unwrap_or("");
    let phase_cycles = if spec == "==auto" {
        crate::diag!(crate::observer::LogLevel::Debug, "  cycles: auto ({stanza_len} ops = {stanza_len} cycles)");
        stanza_len
    } else if spec == "===auto" || spec.is_empty() {
        stanza_len
    } else {
        // Try resolving from kernel
        let mut expanded = spec.to_string();
        for (v, val) in bindings { expanded = expanded.replace(&format!("{{{v}}}"), val); }
        expanded = crate::runner::expand_workload_params(&expanded, &ctx.workload_params);
        let stanzas = crate::runner::parse_count(&expanded)
            .or_else(|| {
                if expanded.starts_with('{') && expanded.ends_with('}') {
                    let inner = &expanded[1..expanded.len()-1];
                    nb_variates::dsl::compile::eval_const_expr(inner).ok()
                        .map(|v| v.as_u64())
                } else {
                    None
                }
            })
            .unwrap_or(1);
        stanzas * stanza_len
    };

    // Diagnostic output
    if ctx.diag.explain_gk {
        let note = if is_iter {
            let pairs: Vec<String> = bindings.iter().map(|(k, v)| format!("{k}={v}")).collect();
            format!(" ({})", pairs.join(", "))
        } else { String::new() };
        crate::describe::print_kernel_analysis(phase_name, &note, &iter_program);
    }
    if ctx.diag.depth == crate::runner::ExecDepth::Phase {
        crate::diag!(crate::observer::LogLevel::Info, "phase '{phase_name}' complete");
        return Ok(());
    }

    // Resolve concurrency
    let phase_concurrency = match phase.concurrency.as_ref() {
        Some(s) => {
            let mut exp = crate::runner::expand_workload_params(s, &ctx.workload_params);
            for (v, val) in bindings { exp = exp.replace(&format!("{{{v}}}"), val); }
            exp.parse::<usize>().map_err(|_| format!(
                "phase '{phase_name}': concurrency '{exp}' not a valid integer"))?
        }
        None => ctx.concurrency,
    };

    let phase_labels = bindings.iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.observer.phase_starting(phase_name, &phase_labels,
        op_sequence.stanza_length(), phase_concurrency);

    let iter_label = bindings.values().next().cloned().unwrap_or_default();
    let activity_name = if !iter_label.is_empty() {
        let var = bindings.keys().next().unwrap();
        format!("{phase_name} ({var}={iter_label})")
    } else {
        phase_name.to_string()
    };

    // If the compiled kernel declares cursors, create a source factory
    // from the first cursor's schema (name + extent). Otherwise the
    // Activity falls back to a range source named "cycles".
    let source_factory: Option<Arc<dyn nb_variates::source::DataSourceFactory>> = {
        let schemas = iter_program.cursor_schemas();
        if let Some(schema) = schemas.first() {
            let extent = schema.extent.unwrap_or(phase_cycles);
            Some(Arc::new(
                nb_variates::source::RangeSourceFactory::named(&schema.name, 0, extent)
            ))
        } else {
            None
        }
    };

    // Capture progress info before source_factory is moved into config
    let progress_extent = source_factory.as_ref()
        .and_then(|f| f.global_extent())
        .unwrap_or(phase_cycles);
    let progress_cursor_name = source_factory.as_ref()
        .map(|f| f.schema().name.clone())
        .unwrap_or_else(|| "cycles".into());
    let progress_fibers = phase_concurrency;

    let config = ActivityConfig {
        name: activity_name,
        cycles: phase_cycles,
        concurrency: phase_concurrency,
        cycle_rate: phase.rate.or(ctx.cycle_rate),
        stanza_rate: None,
        sequencer: ctx.seq_type,
        error_spec: phase.errors.clone().unwrap_or_else(|| ctx.error_spec.clone()),
        max_retries: 3,
        stanza_concurrency: 1,
        source_factory,
        suppress_status_line: ctx.observer.suppresses_stderr(),
    };

    let phase_driver_owned = phase.adapter.clone().unwrap_or_else(|| ctx.driver.clone());
    let phase_driver = phase_driver_owned.as_str();
    let mut adapter_names = std::collections::HashSet::new();
    adapter_names.insert(phase_driver.to_string());
    for t in op_sequence.templates() {
        if let Some(a) = t.params.get("adapter").and_then(|v| v.as_str()) {
            if a != phase_driver { adapter_names.insert(a.to_string()); }
        }
    }
    let mut adapters: HashMap<String, Arc<dyn DriverAdapter>> = HashMap::new();
    for aname in &adapter_names {
        let a = crate::runner::create_adapter(aname, &ctx.merged_params, ctx.dry_run).await?;
        adapters.insert(a.name().to_string(), a);
    }

    // Build labels from component tree: session + for_each levels + phase
    ctx.push_label("phase", phase_name);
    let mut labels = ctx.labels();
    ctx.pop_label();
    // Phases without a `summary:` field are excluded from the report
    if phase.summary.is_none() {
        labels = labels.with("nosummary", "true");
    }

    // Create phase component and attach to session
    let phase_component = Arc::new(RwLock::new(
        Component::new(labels.clone(), HashMap::new()),
    ));
    component::attach(&ctx.session_component, &phase_component);

    let activity = Activity::with_params(
        config, &labels, op_sequence, ctx.workload_params.clone(),
    );

    // Register instruments on the component and set Running
    {
        let mut pc = phase_component.write().unwrap_or_else(|e| e.into_inner());
        pc.set_instruments(activity.shared_metrics());
        pc.set_state(ComponentState::Running);
    }

    let validation_frame = activity.validation_frame.clone();
    let final_metrics = activity.shared_metrics();

    // Feed the observer with live metrics at 500ms cadence.
    // This populates the TUI's ActivePhase panel.
    let observer_for_progress = ctx.observer.clone();
    let progress_metrics = activity.shared_metrics();
    let progress_start = std::time::Instant::now();
    let progress_running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let progress_flag = progress_running.clone();

    // Send initial progress to set cursor info on the observer
    if observer_for_progress.suppresses_stderr() {
        observer_for_progress.phase_progress(&crate::observer::PhaseProgressUpdate {
            cursor_name: progress_cursor_name.clone(),
            cursor_extent: progress_extent,
            fibers: progress_fibers,
            ops_started: 0,
            ops_finished: 0,
            ops_ok: 0,
            errors: 0,
            retries: 0,
            ops_per_sec: 0.0,
            adapter_counters: Vec::new(),
            rows_per_batch: 0.0,
        });
    }

    let _progress_thread = if observer_for_progress.suppresses_stderr() {
        let obs = observer_for_progress.clone();
        let cursor_name_for_thread = progress_cursor_name.clone();
        let fibers_for_thread = progress_fibers;
        Some(std::thread::spawn(move || {
            let progress_cursor_name = cursor_name_for_thread;
            let progress_fibers = fibers_for_thread;
            while progress_flag.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(500));
                if !progress_flag.load(std::sync::atomic::Ordering::Relaxed) { break; }

                let started = progress_metrics.ops_started.load(std::sync::atomic::Ordering::Relaxed);
                let finished = progress_metrics.ops_finished.load(std::sync::atomic::Ordering::Relaxed);
                let successes = progress_metrics.successes_total.get();
                let errors = progress_metrics.errors_total.get();
                let elapsed = progress_start.elapsed().as_secs_f64();
                let ops_per_sec = if elapsed > 0.0 { finished as f64 / elapsed } else { 0.0 };

                let adapter_counters: Vec<(String, u64, f64)> = progress_metrics
                    .collect_status_counters()
                    .into_iter()
                    .map(|(name, total)| {
                        let rate = if elapsed > 0.0 { total as f64 / elapsed } else { 0.0 };
                        (name, total, rate)
                    })
                    .collect();

                let stanzas = progress_metrics.stanzas_total.get();
                let rows_total: u64 = adapter_counters.iter()
                    .find(|(n, _, _)| n == "rows_inserted")
                    .map(|(_, t, _)| *t)
                    .unwrap_or(0);
                let rows_per_batch = if stanzas > 0 && rows_total > stanzas {
                    rows_total as f64 / stanzas as f64
                } else { 0.0 };

                obs.phase_progress(&crate::observer::PhaseProgressUpdate {
                    cursor_name: progress_cursor_name.clone(),
                    cursor_extent: progress_extent,
                    fibers: progress_fibers,
                    ops_started: started,
                    ops_finished: finished,
                    ops_ok: successes,
                    errors,
                    retries: errors.saturating_sub(finished.saturating_sub(successes)),
                    ops_per_sec,
                    adapter_counters,
                    rows_per_batch,
                });
            }
        }))
    } else {
        None
    };

    let stopped = crate::runner::run_activity_simple(
        activity, adapters, phase_driver, iter_program,
    ).await;

    // Stop progress thread
    progress_running.store(false, std::sync::atomic::Ordering::Relaxed);

    // Lifecycle flush: capture final delta, merge into store, deliver to reporters
    {
        use nb_metrics::component::InstrumentSet;
        let final_delta = final_metrics.capture_delta(std::time::Duration::from_secs(1));
        if let Ok(mut s) = ctx.store.write() {
            s.flush_component(&labels, final_delta.clone());
        }
        ctx.stop_handle.report_frame(&final_delta);

        // Flush validation metrics (recall, precision) as gauges
        if let Some(vframe) = validation_frame.lock().unwrap_or_else(|e| e.into_inner()).take() {
            if let Ok(mut s) = ctx.store.write() {
                s.flush_component(&labels, vframe.clone());
            }
            ctx.stop_handle.report_frame(&vframe);
        }
    }

    // Transition to Stopped
    {
        let mut pc = phase_component.write().unwrap_or_else(|e| e.into_inner());
        pc.set_state(ComponentState::Stopped);
    }

    let phase_duration = phase_start.elapsed().as_secs_f64();
    if stopped {
        ctx.observer.phase_failed(phase_name, &phase_labels, "stopped by error handler");
        return Err(format!("phase '{phase_name}' stopped by error handler"));
    }

    ctx.observer.phase_completed(phase_name, &phase_labels, phase_duration);
    Ok(())
}

/// Resolve a for_each expression.
fn resolve_expr(
    spec: &str,
    workload_params: &HashMap<String, String>,
    bindings: &HashMap<String, String>,
    scope_values: &[(String, nb_variates::node::Value)],
) -> Result<(String, Vec<String>), String> {
    let parts: Vec<&str> = spec.splitn(2, " in ").collect();
    if parts.len() != 2 {
        return Err(format!("invalid for_each: '{spec}'. Expected 'var in expr'"));
    }
    let var = parts[0].trim().to_string();
    let mut expr = parts[1].trim().to_string();
    for (k, v) in workload_params { expr = expr.replace(&format!("{{{k}}}"), v); }
    for (k, v) in bindings { expr = expr.replace(&format!("{{{k}}}"), v); }

    let value_str = if let Some(val) = scope_values.iter().find(|(n, _)| n == &expr) {
        val.1.to_display_string()
    } else {
        match nb_variates::dsl::compile::eval_const_expr(&expr) {
            Ok(val) => val.to_display_string(),
            Err(_) => expr.clone(),
        }
    };

    let mut values: Vec<String> = value_str.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    values.sort();
    Ok((var, values))
}
