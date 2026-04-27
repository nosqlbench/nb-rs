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
use nb_metrics::cadence_reporter::CadenceReporter;
use nb_metrics::component::{self, Component, ComponentState};
use nb_metrics::labels::Labels;
use nb_workload::model::{ScenarioNode, WorkloadPhase};

/// Shared context for the recursive executor.
///
/// `Clone` is derived so the concurrent scheduler can fork per-task
/// copies: every Arc field aliases cheaply, while the mutable
/// `label_stack` forks so each concurrent sibling carries its own
/// label path.
#[derive(Clone)]
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
    pub rate: Option<f64>,
    pub error_spec: String,
    /// Session identifier for metric labeling.
    pub session_id: String,
    /// Label stack: accumulated dimensional labels from the component tree.
    /// for_each pushes (var, value), phase pushes ("phase", name).
    /// do_while/do_until are transparent — they don't contribute labels.
    pub label_stack: Vec<(String, String)>,
    /// Session root component (owns the component tree).
    pub session_component: Arc<RwLock<Component>>,
    /// Cadence reporter for lifecycle flush — same reporter the
    /// scheduler is feeding; end-of-phase final deltas route here.
    pub cadence_reporter: Arc<CadenceReporter>,
    /// Scheduler stop handle for delivering frames to reporters.
    pub stop_handle: Arc<nb_metrics::scheduler::StopHandle>,
    /// Run observer for phase lifecycle events (TUI or stderr).
    pub observer: Arc<dyn crate::observer::RunObserver>,
    /// Canonical scope tree for the current scenario (SRD 18b).
    /// Built once per session from the resolved scenario nodes;
    /// mirrors the scenario structure 1:1 with parent / child
    /// pointers, depth tags, and pragma slots. Today consumed by
    /// observer pre-mapping and diagnostic display; future steps
    /// (extern-binding migration, scheduler) drive execution off
    /// this tree directly.
    pub scope_tree: Arc<crate::scope_tree::ScopeTree>,
    /// Per-level concurrency policy (SRD 18b §"Scheduler
    /// abstraction"). Consulted by the tree walker at each depth
    /// to decide whether sibling scopes / for_each iterations run
    /// serially or concurrently. Shared across forked clones —
    /// the spec is immutable after session construction.
    pub schedule_spec: Arc<crate::scheduler::ScheduleSpec>,
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
///
/// Entry point — siblings are at scope-tree depth 0. The per-depth
/// concurrency policy lives on `ctx.schedule_spec`; when the policy
/// at some depth allows >1 concurrency, siblings (and ForEach /
/// phase-level-for_each iterations at the next depth) fork via
/// cloned per-task `ExecCtx`.
pub fn execute_tree<'a>(
    ctx: &'a mut ExecCtx,
    nodes: &'a [ScenarioNode],
    bindings: &'a HashMap<String, String>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    execute_tree_at(ctx, nodes, bindings, 0)
}

/// Depth-tagged tree walk. Each recursive descent (into ForEach
/// children, ForCombinations children, DoWhile / DoUntil bodies,
/// or phase-level iterations) bumps `depth`. The `schedule_spec`
/// on `ctx` is consulted at `depth` to decide sibling strategy.
fn execute_tree_at<'a>(
    ctx: &'a mut ExecCtx,
    nodes: &'a [ScenarioNode],
    bindings: &'a HashMap<String, String>,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        let limit = ctx.schedule_spec.limit_at(depth);
        if matches!(limit, crate::scheduler::ConcurrencyLimit::Serial) || nodes.len() <= 1 {
            for node in nodes {
                execute_node(ctx, node, bindings, depth).await?;
            }
            Ok(())
        } else {
            run_siblings_concurrently(ctx, nodes, bindings, depth, limit).await
        }
    })
}

/// Spawn each sibling in its own task with a cloned `ExecCtx` and
/// cloned bindings. Bounded limits gate tasks through a
/// `Semaphore`; Unlimited launches all at once. Joins via
/// `JoinSet`; the first task error aborts the rest (remaining
/// tasks still finish their own permits).
async fn run_siblings_concurrently(
    ctx: &mut ExecCtx,
    nodes: &[ScenarioNode],
    bindings: &HashMap<String, String>,
    depth: usize,
    limit: crate::scheduler::ConcurrencyLimit,
) -> Result<(), String> {
    use crate::scheduler::ConcurrencyLimit;
    let sem: Option<Arc<tokio::sync::Semaphore>> = match limit {
        ConcurrencyLimit::Bounded(n) => Some(Arc::new(tokio::sync::Semaphore::new(n as usize))),
        ConcurrencyLimit::Unlimited => None,
        ConcurrencyLimit::Serial => unreachable!("serial handled by caller"),
    };
    let mut set = tokio::task::JoinSet::new();
    for node in nodes {
        let node = node.clone();
        let mut task_ctx = ctx.clone();
        let task_bindings = bindings.clone();
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = match sem {
                Some(s) => Some(s.acquire_owned().await.map_err(|e| e.to_string())?),
                None => None,
            };
            execute_node(&mut task_ctx, &node, &task_bindings, depth).await
        });
    }
    let mut first_err: Option<String> = None;
    while let Some(res) = set.join_next().await {
        match res {
            Err(join_err) => {
                if first_err.is_none() {
                    first_err = Some(format!("concurrent task panicked: {join_err}"));
                }
            }
            Ok(Err(e)) => {
                if first_err.is_none() { first_err = Some(e); }
            }
            Ok(Ok(())) => {}
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Execute a single scenario node. Descent into children happens
/// at `depth + 1`; iteration loops (ForEach, ForCombinations,
/// phase-level for_each, DoWhile, DoUntil) also treat their
/// iteration instances as siblings at `depth + 1` and honor the
/// concurrency limit at that depth.
fn execute_node<'a>(
    ctx: &'a mut ExecCtx,
    node: &'a ScenarioNode,
    bindings: &'a HashMap<String, String>,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        match node {
            ScenarioNode::Phase(name) => {
                let phase_fe = ctx.phases.get(name.as_str())
                    .and_then(|p| p.for_each.clone());
                if let Some(spec) = phase_fe {
                    let (var, values) = resolve_expr(
                        &spec, &ctx.workload_params, bindings, &ctx.outer_scope_values)?;
                    if !ctx.quiet() {
                        crate::diag!(crate::observer::LogLevel::Debug, "  for_each {var} in [{}] ({} iterations)",
                            values.join(", "), values.len());
                    }
                    run_iterations(ctx, &var, &values, bindings, depth + 1, IterKind::Phase { name }).await?;
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
                run_iterations(ctx, &var, &values, bindings, depth + 1, IterKind::Children { children }).await?;
            }
            ScenarioNode::ForCombinations { specs, children } => {
                cartesian_recurse(ctx, specs, 0, bindings, children, depth + 1, true).await?;
            }
            ScenarioNode::DoWhile { condition, counter, children } => {
                crate::diag!(crate::observer::LogLevel::Debug, "=== do_while: {condition} ===");
                let mut i = 0u64;
                loop {
                    let mut inner = bindings.clone();
                    if let Some(c) = counter { inner.insert(c.clone(), i.to_string()); }
                    execute_tree_at(ctx, children, &inner, depth + 1).await?;
                    i += 1;
                    crate::diag!(crate::observer::LogLevel::Debug, "  (do_while: executed {i} iteration(s), condition eval pending)");
                    break;
                }
            }
            ScenarioNode::DoUntil { condition, counter, children } => {
                crate::diag!(crate::observer::LogLevel::Debug, "=== do_until: {condition} ===");
                let mut i = 0u64;
                loop {
                    let mut inner = bindings.clone();
                    if let Some(c) = counter { inner.insert(c.clone(), i.to_string()); }
                    execute_tree_at(ctx, children, &inner, depth + 1).await?;
                    i += 1;
                    crate::diag!(crate::observer::LogLevel::Debug, "  (do_until: executed {i} iteration(s), condition eval pending)");
                    break;
                }
            }
        }
        Ok(())
    })
}

/// Distinguishes the two iteration shapes that share a body of
/// "push label / bind var / descend": a bare phase (per-iter we
/// call `run_phase`) vs. a scope with children (per-iter we
/// descend via `execute_tree_at`). Keeps `run_iterations` generic
/// over both without duplicating the concurrency logic.
enum IterKind<'a> {
    Phase { name: &'a str },
    Children { children: &'a [ScenarioNode] },
}

/// Run N iterations under one variable, either serially or
/// concurrently based on `limit_at(depth)`. Iterations share the
/// same variable name but each owns its bound value; each concurrent
/// iteration forks a full `ExecCtx` clone so label pushes don't
/// race.
async fn run_iterations(
    ctx: &mut ExecCtx,
    var: &str,
    values: &[String],
    bindings: &HashMap<String, String>,
    depth: usize,
    kind: IterKind<'_>,
) -> Result<(), String> {
    use crate::scheduler::ConcurrencyLimit;
    let limit = ctx.schedule_spec.limit_at(depth);
    if matches!(limit, ConcurrencyLimit::Serial) || values.len() <= 1 {
        for value in values {
            let mut inner = bindings.clone();
            inner.insert(var.to_string(), value.clone());
            ctx.push_label(var, value);
            match &kind {
                IterKind::Phase { name } => run_phase(ctx, name, &inner).await?,
                IterKind::Children { children } => execute_tree_at(ctx, children, &inner, depth).await?,
            }
            ctx.pop_label();
        }
        return Ok(());
    }
    // Concurrent iterations: fork per-task ctx clones.
    let sem: Option<Arc<tokio::sync::Semaphore>> = match limit {
        ConcurrencyLimit::Bounded(n) => Some(Arc::new(tokio::sync::Semaphore::new(n as usize))),
        ConcurrencyLimit::Unlimited => None,
        ConcurrencyLimit::Serial => unreachable!(),
    };
    let mut set = tokio::task::JoinSet::new();
    let var_owned = var.to_string();
    // Materialize the iteration body kind into owned form.
    enum OwnedKind {
        Phase(String),
        Children(Vec<ScenarioNode>),
    }
    let owned_kind = Arc::new(match &kind {
        IterKind::Phase { name } => OwnedKind::Phase(name.to_string()),
        IterKind::Children { children } => OwnedKind::Children(children.to_vec()),
    });
    for value in values {
        let value = value.clone();
        let var = var_owned.clone();
        let mut task_ctx = ctx.clone();
        let mut inner = bindings.clone();
        inner.insert(var.clone(), value.clone());
        let sem = sem.clone();
        let owned_kind = owned_kind.clone();
        set.spawn(async move {
            let _permit = match sem {
                Some(s) => Some(s.acquire_owned().await.map_err(|e| e.to_string())?),
                None => None,
            };
            task_ctx.push_label(&var, &value);
            let res = match &*owned_kind {
                OwnedKind::Phase(name) => run_phase(&mut task_ctx, name, &inner).await,
                OwnedKind::Children(children) => execute_tree_at(&mut task_ctx, children, &inner, depth).await,
            };
            task_ctx.pop_label();
            res
        });
    }
    let mut first_err: Option<String> = None;
    while let Some(res) = set.join_next().await {
        match res {
            Err(join_err) => {
                if first_err.is_none() {
                    first_err = Some(format!("concurrent iteration panicked: {join_err}"));
                }
            }
            Ok(Err(e)) => {
                if first_err.is_none() { first_err = Some(e); }
            }
            Ok(Ok(())) => {}
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// ForCombinations walks dimensions lazily — later dimensions
/// may reference earlier-bound variables. At the innermost
/// dimension we descend into children via `execute_tree_at`.
/// Each dimension's value loop is a sibling-set at `depth + dim`,
/// but today's impl runs dimensions serially; cross-dimension
/// concurrency is deferred. The children subtree honors the
/// per-depth spec normally.
fn cartesian_recurse<'a>(
    ctx: &'a mut ExecCtx,
    specs: &'a [(String, String)],
    dim_idx: usize,
    bindings: &'a HashMap<String, String>,
    children: &'a [ScenarioNode],
    depth: usize,
    first: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        if dim_idx >= specs.len() {
            execute_tree_at(ctx, children, bindings, depth).await?;
            return Ok(());
        }
        let (var, expr) = &specs[dim_idx];
        let mut resolved_expr = expr.clone();
        for (k, v) in bindings {
            resolved_expr = resolved_expr.replace(&format!("{{{k}}}"), v);
        }
        let (_, values) = resolve_expr(
            &format!("{var} in {resolved_expr}"),
            &ctx.workload_params, bindings, &ctx.outer_scope_values)?;

        if first {
            let mut dims: Vec<String> = Vec::new();
            dims.push(format!("{var}({} values)", values.len()));
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
            cartesian_recurse(ctx, specs, dim_idx + 1, &inner, children, depth, false).await?;
            ctx.pop_label();
        }
        Ok(())
    })
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

        // Iteration variables are no longer text-substituted
        // (SRD 18b §"Iteration variables as scope outputs").
        // `add_iteration_var` declares each as a typed extern;
        // the runtime populates extern values per iteration just
        // below, after the kernel is compiled. References to
        // `{var}` in GK source resolve through the extern's
        // passthrough output; op-field bind-point references
        // (`stmt: "INSERT id={var}"`) render through the same
        // mechanism at execution time.
        //
        // Workload params are still substituted because they're
        // static for the session — making them externs would
        // be the same shape but without the per-iter rebinding
        // benefit.
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

        // Compile-and-cache or rebind path (SRD 18b §"Cache-and-
        // rebind contract").
        //
        // The phase scope's `Arc<GkProgram>` lives in a
        // `OnceLock` on its scope-tree node. First call compiles
        // (using the chain-walked pragmas), inserts; subsequent
        // calls retrieve and build a fresh `GkKernel` from the
        // cached program with a freshly-created `GkState`. Each
        // call ends up with the same shape — a populated
        // `GkKernel` ready for outer-scope and iteration-variable
        // extern injection — but only the first call pays the
        // compile cost.
        let cursor_limit: Option<u64> = ctx.merged_params.get("limit")
            .and_then(|s| s.parse().ok());
        let phase_idx = ctx.scope_tree.phase_node_by_name(phase_name);
        let phase_pragmas = phase_idx
            .map(|idx| ctx.scope_tree.nodes[idx].pragmas.clone())
            .unwrap_or_default();

        let mut kernel = if let Some(idx) = phase_idx {
            let node = &ctx.scope_tree.nodes[idx];
            if let Some(prog) = node.cached_program.get() {
                // Cache hit: rebind path. Build a fresh kernel
                // from the cached program; the freshly-allocated
                // state is empty and will be populated below.
                nb_variates::kernel::GkKernel::from_program(prog.clone())
            } else {
                // First call for this phase — compile, then
                // populate the cache. Other callers racing with
                // us all see the first-winner via OnceLock.
                let compiled = crate::bindings::compile_from_scope(
                    &scope,
                    ctx.workload_dir.as_deref(),
                    ctx.gk_lib_paths.clone(),
                    ctx.strict,
                    &gk_context,
                    cursor_limit,
                    &phase_pragmas,
                ).map_err(|e| format!("{gk_context}: {e}"))?;
                let prog = compiled.program().clone();
                let _ = node.cached_program.set(prog);
                compiled
            }
        } else {
            // Phase not in the scope tree (shouldn't happen for
            // any executor-driven invocation; defensive). Fall
            // back to the un-cached compile path.
            crate::bindings::compile_from_scope(
                &scope,
                ctx.workload_dir.as_deref(),
                ctx.gk_lib_paths.clone(),
                ctx.strict,
                &gk_context,
                cursor_limit,
                &phase_pragmas,
            ).map_err(|e| format!("{gk_context}: {e}"))?
        };

        // Wire outer scope values into the inner kernel's inputs
        for (name, value) in &ctx.outer_scope_values {
            if let Some(idx) = kernel.program().find_input(name) {
                kernel.state().set_input(idx, value.clone());
            }
        }
        // Populate iteration-variable externs (SRD 18b
        // §"Iteration variables as scope outputs"). Each iter
        // var was declared as a typed extern by
        // `add_iteration_var`; here we set the current
        // iteration's value with type matching the declaration.
        // String → numeric coercion mirrors the inference in
        // `add_iteration_var`.
        for (name, value) in bindings {
            if let Some(idx) = kernel.program().find_input(name) {
                let v = if let Ok(n) = value.parse::<u64>() {
                    nb_variates::node::Value::U64(n)
                } else if let Ok(n) = value.parse::<f64>() {
                    nb_variates::node::Value::F64(n)
                } else {
                    nb_variates::node::Value::Str(value.clone())
                };
                kernel.state().set_input(idx, v);
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

    let phase_labels = format_labels(bindings);
    let stanza_len = op_sequence.stanza_length();
    ctx.observer.phase_starting(phase_name, &phase_labels,
        stanza_len, phase_concurrency);
    crate::scene_tree::with_global_mut(|t| {
        t.set_phase_running(phase_name, &phase_labels, stanza_len);
    });

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
        rate: phase.rate.or(ctx.rate),
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

    // SRD 40: resolve hdr.sigdigs via walk-up from the phase's
    // ancestor chain before constructing the activity so the
    // histograms start at the configured precision.
    let sigdigs = nb_metrics::instruments::histogram::resolve_hdr_sigdigs(
        &phase_component.read().unwrap_or_else(|e| e.into_inner()),
    );
    let mut activity = Activity::with_params_and_sigdigs(
        config, &labels, op_sequence, ctx.workload_params.clone(), sigdigs,
    );
    // Wire the phase component back onto the activity so the
    // fiber pool can declare its `concurrency` control here
    // (SRD 23 §"Fiber executor").
    activity.attach_component(phase_component.clone());

    // Register instruments on the component and set Running
    {
        let mut pc = phase_component.write().unwrap_or_else(|e| e.into_inner());
        pc.set_instruments(activity.shared_metrics());
        pc.set_state(ComponentState::Running);
    }

    // SRD-42 §"now" reads come through `MetricsQuery::now()` which
    // walks the live component tree on demand — no per-phase hook
    // registration needed. The legacy `set_live_source` path is
    // gone as of Phase 7b.

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
            name: phase_name.to_string(),
            labels: phase_labels.clone(),
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
            relevancy: Vec::new(),
        });
    }

    let _progress_thread = if observer_for_progress.suppresses_stderr() {
        let obs = observer_for_progress.clone();
        let cursor_name_for_thread = progress_cursor_name.clone();
        let fibers_for_thread = progress_fibers;
        let name_for_thread = phase_name.to_string();
        let labels_for_thread = phase_labels.clone();
        // Clone so the post-phase final-progress emission below still
        // has access — the thread needs to own its own Arc handle.
        let progress_metrics = progress_metrics.clone();
        Some(std::thread::spawn(move || {
            let progress_cursor_name = cursor_name_for_thread;
            let progress_fibers = fibers_for_thread;
            let phase_name = name_for_thread;
            let phase_labels = labels_for_thread;
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

                let relevancy = progress_metrics.collect_relevancy_live();

                obs.phase_progress(&crate::observer::PhaseProgressUpdate {
                    name: phase_name.clone(),
                    labels: phase_labels.clone(),
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
                    relevancy,
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

    // Emit one final phase_progress with fresh numbers before
    // `phase_completed`. Short phases (e.g. 100ms ann_query) can
    // finish between progress-thread ticks (every 500ms), so
    // relevancy / counter snapshots that were being updated live
    // would otherwise arrive empty at the observer's summary-
    // capture step. This guarantees the final frame is never stale.
    if ctx.observer.suppresses_stderr() {
        let started_total = progress_metrics.ops_started.load(std::sync::atomic::Ordering::Relaxed);
        let finished_total = progress_metrics.ops_finished.load(std::sync::atomic::Ordering::Relaxed);
        let successes = progress_metrics.successes_total.get();
        let errors = progress_metrics.errors_total.get();
        let elapsed = progress_start.elapsed().as_secs_f64();
        let ops_per_sec = if elapsed > 0.0 { finished_total as f64 / elapsed } else { 0.0 };
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
        let relevancy = progress_metrics.collect_relevancy_live();
        ctx.observer.phase_progress(&crate::observer::PhaseProgressUpdate {
            name: phase_name.to_string(),
            labels: phase_labels.clone(),
            cursor_name: progress_cursor_name.clone(),
            cursor_extent: progress_extent,
            fibers: progress_fibers,
            ops_started: started_total,
            ops_finished: finished_total,
            ops_ok: successes,
            errors,
            retries: errors.saturating_sub(finished_total.saturating_sub(successes)),
            ops_per_sec,
            adapter_counters,
            rows_per_batch,
            relevancy,
        });
    }

    // Lifecycle flush: capture final delta, route through the
    // cadence reporter (single writer of windowed snapshots), and
    // deliver to the scheduler-tree reporters for external sinks.
    //
    // The call to `close_path` at the end is the primary lifecycle
    // boundary for this phase's metrics. By the time the ingests
    // above return, no more data for this phase's label set will
    // ever arrive — the next for_each iteration (or the next phase)
    // uses a different label combination, so it lands in a different
    // path. Closing here publishes the phase's windows now instead
    // of leaving them to idle until the next cadence tick (which
    // could be 30s away) or session shutdown (which produced a
    // thundering herd of stale windows).
    {
        use nb_metrics::component::InstrumentSet;
        let final_delta = final_metrics.capture_delta(std::time::Duration::from_secs(1));
        ctx.cadence_reporter.ingest(&labels, final_delta.clone());
        ctx.stop_handle.report_frame(&final_delta);

        // Flush validation metrics (recall, precision) as gauges
        if let Some(vframe) = validation_frame.lock().unwrap_or_else(|e| e.into_inner()).take() {
            ctx.cadence_reporter.ingest(&labels, vframe.clone());
            ctx.stop_handle.report_frame(&vframe);
        }

        ctx.cadence_reporter.close_path(&labels);
    }

    // Transition to Stopped
    {
        let mut pc = phase_component.write().unwrap_or_else(|e| e.into_inner());
        pc.set_state(ComponentState::Stopped);
    }

    let phase_duration = phase_start.elapsed().as_secs_f64();
    if stopped {
        ctx.observer.phase_failed(phase_name, &phase_labels, "stopped by error handler");
        crate::scene_tree::with_global_mut(|t| {
            t.set_phase_failed(phase_name, &phase_labels, "stopped by error handler");
        });
        return Err(format!("phase '{phase_name}' stopped by error handler"));
    }

    ctx.observer.phase_completed(phase_name, &phase_labels, phase_duration);
    crate::scene_tree::with_global_mut(|t| {
        t.set_phase_completed(phase_name, &phase_labels, phase_duration);
    });
    Ok(())
}

/// Format bindings as a sorted labels string for stable matching.
///
/// HashMap iteration order is non-deterministic. Sorting ensures that
/// pre-map entries match the labels produced at execution time.
fn format_labels(bindings: &HashMap<String, String>) -> String {
    let mut pairs: Vec<String> = bindings.iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    pairs.sort();
    pairs.join(", ")
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

// =========================================================================
// Scenario tree pre-mapping (walk without executing)
// =========================================================================

use crate::scene_tree::{NodeKind, SceneNodeId, SceneTree};

/// Walk the scenario tree without executing and build a
/// [`SceneTree`] of every concrete phase and scope header.
///
/// `for_each` / `for_combinations` specs are resolved here so each
/// iteration appears as its own concrete phase under a per-iteration
/// scope header. `do_while` / `do_until` are shown once (iteration
/// count is unknown at pre-map time).
pub fn pre_map_tree(
    nodes: &[ScenarioNode],
    phases: &HashMap<String, WorkloadPhase>,
    workload_params: &HashMap<String, String>,
    outer_scope_values: &[(String, nb_variates::node::Value)],
) -> Result<SceneTree, String> {
    let mut tree = SceneTree::new();
    let bindings = HashMap::new();
    let root = tree.root();
    pre_map_recursive(nodes, phases, workload_params, outer_scope_values,
                      &bindings, root, &mut tree)?;
    Ok(tree)
}

fn pre_map_recursive(
    nodes: &[ScenarioNode],
    phases: &HashMap<String, WorkloadPhase>,
    workload_params: &HashMap<String, String>,
    outer_scope_values: &[(String, nb_variates::node::Value)],
    bindings: &HashMap<String, String>,
    parent: SceneNodeId,
    tree: &mut SceneTree,
) -> Result<(), String> {
    for node in nodes {
        match node {
            ScenarioNode::Phase(name) => {
                let phase_fe = phases.get(name.as_str())
                    .and_then(|p| p.for_each.clone());
                if let Some(spec) = phase_fe {
                    match resolve_expr(&spec, workload_params, bindings, outer_scope_values) {
                        Ok((var, values)) => {
                            let scope = tree.push(
                                parent,
                                NodeKind::Scope,
                                format!("phase.for_each {var} in [{}]", values.join(", ")),
                                "",
                            );
                            for value in &values {
                                let mut inner = bindings.clone();
                                inner.insert(var.clone(), value.clone());
                                tree.push(scope, NodeKind::Phase, name.clone(), format_labels(&inner));
                            }
                        }
                        Err(_) => {
                            tree.push(parent, NodeKind::Phase, name.clone(), format_labels(bindings));
                        }
                    }
                } else {
                    tree.push(parent, NodeKind::Phase, name.clone(), format_labels(bindings));
                }
            }
            ScenarioNode::ForEach { spec, children } => {
                match resolve_expr(spec, workload_params, bindings, outer_scope_values) {
                    Ok((var, values)) => {
                        // One scope header per iteration so each
                        // iteration's phases cluster under their own
                        // concrete binding.
                        for value in &values {
                            let scope = tree.push(
                                parent,
                                NodeKind::Scope,
                                format!("for_each {var}={value}"),
                                "",
                            );
                            let mut inner = bindings.clone();
                            inner.insert(var.clone(), value.clone());
                            pre_map_recursive(children, phases, workload_params,
                                outer_scope_values, &inner, scope, tree)?;
                        }
                    }
                    Err(_) => {
                        let scope = tree.push(parent, NodeKind::Scope, format!("for_each {spec}"), "");
                        pre_map_recursive(children, phases, workload_params,
                            outer_scope_values, bindings, scope, tree)?;
                    }
                }
            }
            ScenarioNode::ForCombinations { specs, children } => {
                let summary = specs.iter().map(|(v, _)| v.as_str()).collect::<Vec<_>>().join(", ");
                let scope = tree.push(
                    parent,
                    NodeKind::Scope,
                    format!("for_combinations [{summary}]"),
                    "",
                );
                pre_map_combinations(specs, 0, phases, workload_params,
                    outer_scope_values, bindings, scope, children, tree)?;
            }
            ScenarioNode::DoWhile { condition, children, .. } => {
                let scope = tree.push(parent, NodeKind::Scope, format!("do_while {condition}"), "");
                pre_map_recursive(children, phases, workload_params,
                    outer_scope_values, bindings, scope, tree)?;
            }
            ScenarioNode::DoUntil { condition, children, .. } => {
                let scope = tree.push(parent, NodeKind::Scope, format!("do_until {condition}"), "");
                pre_map_recursive(children, phases, workload_params,
                    outer_scope_values, bindings, scope, tree)?;
            }
        }
    }
    Ok(())
}

fn pre_map_combinations(
    specs: &[(String, String)],
    dim_idx: usize,
    phases: &HashMap<String, WorkloadPhase>,
    workload_params: &HashMap<String, String>,
    outer_scope_values: &[(String, nb_variates::node::Value)],
    bindings: &HashMap<String, String>,
    parent: SceneNodeId,
    children: &[ScenarioNode],
    tree: &mut SceneTree,
) -> Result<(), String> {
    if dim_idx >= specs.len() {
        return pre_map_recursive(children, phases, workload_params,
            outer_scope_values, bindings, parent, tree);
    }
    let (var, expr) = &specs[dim_idx];
    let mut resolved_expr = expr.clone();
    for (k, v) in bindings {
        resolved_expr = resolved_expr.replace(&format!("{{{k}}}"), v);
    }
    match resolve_expr(
        &format!("{var} in {resolved_expr}"),
        workload_params, bindings, outer_scope_values,
    ) {
        Ok((_, values)) => {
            for value in &values {
                let mut inner = bindings.clone();
                inner.insert(var.clone(), value.clone());
                pre_map_combinations(specs, dim_idx + 1, phases, workload_params,
                    outer_scope_values, &inner, parent, children, tree)?;
            }
        }
        Err(_) => {
            pre_map_combinations(specs, dim_idx + 1, phases, workload_params,
                outer_scope_values, bindings, parent, children, tree)?;
        }
    }
    Ok(())
}
