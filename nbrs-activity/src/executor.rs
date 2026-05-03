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

use indexmap::IndexMap;

use crate::activity::{Activity, ActivityConfig};
use crate::adapter::DriverAdapter;
use crate::opseq::{OpSequence, SequencerType};
use crate::synthesis::OpBuilder;
use nbrs_metrics::cadence_reporter::CadenceReporter;
use nbrs_metrics::component::{self, Component, ComponentState};
use nbrs_metrics::labels::Labels;
use nbrs_variates::kernel::{format_scope_coordinate_path, GkKernel, ScopeCoord};
use nbrs_workload::model::{ScenarioNode, WorkloadPhase};

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
    pub program: Arc<nbrs_variates::kernel::GkProgram>,
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
    pub stop_handle: Arc<nbrs_metrics::scheduler::StopHandle>,
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
    /// M3.4 — current immediate-parent scope kernel for the
    /// leaf phase compile. Set by the dependent-tuple
    /// dispatcher to the per-branch `GkKernel` it owns; cleared
    /// (or restored) when the dispatcher unwinds. When `Some`,
    /// the leaf-phase compile path uses this kernel's manifest
    /// for auto-extern wiring and calls `bind_outer_scope`
    /// against it directly — iteration vars and inherited
    /// values both flow through the standard GK chain. When
    /// `None`, the leaf phase falls back to the workload-level
    /// `outer_manifest` / `outer_scope_values` (the legacy flat
    /// data flow that M3.4 retires for kernel-routed scopes).
    pub current_parent_kernel:
        Option<Arc<nbrs_variates::kernel::GkKernel>>,
    /// Workload source text + path, kept for error diagnostics.
    /// Errors at the dispatch layer (for_each / do_while spec
    /// evaluation, interpolation failures) include the YAML
    /// line / column where the failing spec was authored, so
    /// the user can jump straight to the source. `None` for
    /// inline workloads (`op=`) where there's no file.
    pub workload_source: Option<Arc<WorkloadSource>>,
}

/// Workload YAML source kept alongside the parsed model so
/// runtime errors can report YAML line/column locations.
pub struct WorkloadSource {
    pub path: String,
    pub text: String,
}

impl WorkloadSource {
    /// Find the first occurrence of `needle` in the source
    /// text and return its 1-indexed (line, column). Returns
    /// `None` if `needle` doesn't appear (e.g. it was
    /// dynamically constructed and isn't a substring of the
    /// authored YAML).
    pub fn locate(&self, needle: &str) -> Option<(usize, usize)> {
        let idx = self.text.find(needle)?;
        let prefix = &self.text[..idx];
        let line = prefix.bytes().filter(|b| *b == b'\n').count() + 1;
        let col = prefix.rfind('\n').map(|nl| idx - nl).unwrap_or(idx + 1);
        Some((line, col))
    }
}

/// Format an error with a YAML-source location prefix when one
/// can be found. Used at dispatch sites (for_each, do_while)
/// to enrich downstream error messages with `<path>:<line>:<col>`.
///
/// Idempotent under nesting: if the error already starts with
/// this workload's location prefix (because an inner dispatcher
/// already enriched it), the outer wrapper leaves it alone — no
/// double-prefix.
pub(crate) fn enrich_with_yaml_location(
    ctx: &ExecCtx,
    needle: &str,
    err: String,
) -> String {
    let Some(src) = ctx.workload_source.as_ref() else { return err; };
    if err.starts_with(&format!("{}:", src.path)) {
        return err;
    }
    let Some((line, col)) = src.locate(needle) else { return err; };
    format!("{}:{line}:{col}: {err}", src.path)
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
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    execute_tree_at(ctx, nodes, 0)
}

/// Depth-tagged tree walk. Each recursive descent (into ForEach
/// children, ForCombinations children, DoWhile / DoUntil bodies,
/// or phase-level iterations) bumps `depth`. The `schedule_spec`
/// on `ctx` is consulted at `depth` to decide sibling strategy.
fn execute_tree_at<'a>(
    ctx: &'a mut ExecCtx,
    nodes: &'a [ScenarioNode],
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        let limit = ctx.schedule_spec.limit_at(depth);
        if matches!(limit, crate::scheduler::ConcurrencyLimit::Serial) || nodes.len() <= 1 {
            for node in nodes {
                execute_node(ctx, node, depth).await?;
            }
            Ok(())
        } else {
            run_siblings_concurrently(ctx, nodes, depth, limit).await
        }
    })
}

/// Spawn each sibling in its own task with a cloned `ExecCtx`.
/// Iter-var values flow exclusively via `ctx.current_parent_kernel`
/// per the M3.4b unified-comprehension contract — no separate
/// HashMap to clone. Bounded limits gate tasks through a
/// `Semaphore`; Unlimited launches all at once. Joins via
/// `JoinSet`; the first task error aborts the rest (remaining
/// tasks still finish their own permits).
async fn run_siblings_concurrently(
    ctx: &mut ExecCtx,
    nodes: &[ScenarioNode],
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
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = match sem {
                Some(s) => Some(s.acquire_owned().await.map_err(|e| e.to_string())?),
                None => None,
            };
            execute_node(&mut task_ctx, &node, depth).await
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

/// Resolve the parent kernel for a scope dispatched at runtime.
///
/// `ctx.current_parent_kernel` carries the *live execution* kernel
/// of the immediately-enclosing scope — set by `run_one_iteration`
/// (or the do-loop dispatcher) to its per-iteration kernel before
/// descending into children, restored on the way out. That's the
/// kernel that has the outer iter-var values *set to the current
/// iteration's values*, so spec evaluation in nested scopes sees
/// `{outer_var}` resolve correctly.
///
/// The scope tree's canonical ancestor kernel (via
/// `nearest_installed_ancestor_kernel`) only carries the structural
/// constants; iter vars are present as outputs but their values are
/// the kernel's defaults, not the current iteration's values.
///
/// Prefer the live execution kernel; fall back to the structural
/// ancestor when no dispatcher is currently active above us.
fn effective_parent_kernel(
    ctx: &ExecCtx,
    scope_idx: usize,
) -> Option<std::sync::Arc<nbrs_variates::kernel::GkKernel>> {
    ctx.current_parent_kernel.clone()
        .or_else(|| ctx.scope_tree.nearest_installed_ancestor_kernel(scope_idx))
}

/// Execute a single scenario node. Descent into children happens
/// at `depth + 1`; iteration loops (ForEach, ForCombinations,
/// phase-level for_each, DoWhile, DoUntil) also treat their
/// iteration instances as siblings at `depth + 1` and honor the
/// concurrency limit at that depth.
fn execute_node<'a>(
    ctx: &'a mut ExecCtx,
    node: &'a ScenarioNode,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        match node {
            ScenarioNode::Phase(name) => {
                let phase_fe = ctx.phases.get(name.as_str())
                    .and_then(|p| p.for_each.clone());
                if let Some(spec) = phase_fe {
                    // Phase-level for_each routes through the
                    // unified dispatcher with terminal action
                    // `TerminalAction::Phase(name)` — the scope
                    // kernel exposes the iter var as a scope
                    // output, the dispatcher's per-branch kernel
                    // is set as the parent for the leaf phase
                    // compile, the phase runs once per iter
                    // value.
                    let (var_parsed, expr_parsed) = parse_var_in_expr(&spec);
                    let scope_idx = ctx.scope_tree.phase_node_by_name(name)
                        .ok_or_else(|| format!(
                            "phase '{name}' for_each '{spec}': no matching scope-tree entry."
                        ))?;
                    let canonical = ctx.scope_tree.nodes[scope_idx].cached_kernel.get()
                        .cloned()
                        .ok_or_else(|| format!(
                            "phase '{name}' for_each '{spec}': scope at index {scope_idx} \
                             has no installed phase-for_each kernel."
                        ))?;
                    let parent = effective_parent_kernel(ctx, scope_idx)
                        .ok_or_else(|| format!(
                            "phase '{name}' for_each '{spec}': no installed ancestor kernel."
                        ))?;
                    let clauses = vec![(var_parsed, expr_parsed)];
                    let needle = spec.clone();
                    let parent_coords = ctx.current_parent_kernel.as_ref()
                        .map(|k| k.scope_coordinates().iter().rev().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();
                    let steps = runtime_iterate(
                        ctx, &canonical, &parent, &parent_coords, &clauses, None, None, None,
                    ).map_err(|e| enrich_with_yaml_location(ctx, &needle, e))?;
                    return dispatch_comprehension(
                        ctx, steps,
                        TerminalAction::Phase(name), depth + 1, false,
                    ).await
                        .map_err(|e| enrich_with_yaml_location(ctx, &needle, e));
                } else {
                    run_phase(ctx, name).await?;
                }
            }
            ScenarioNode::Comprehension { comprehension, children } => {
                use nbrs_variates::comprehension::ComprehensionMode;
                let label = crate::scope_tree::ScopeKind::Comprehension {
                    comprehension: comprehension.clone(),
                }.label();
                let scope_idx = ctx.scope_tree.find_comprehension_scope(comprehension)
                    .ok_or_else(|| format!(
                        "{label}: no matching scope-tree entry — scenario/scope-tree drift bug.",
                    ))?;
                let canonical = ctx.scope_tree.nodes[scope_idx].cached_kernel.get()
                    .cloned()
                    .ok_or_else(|| format!(
                        "{label}: scope at index {scope_idx} has no installed kernel.",
                    ))?;
                let parent = effective_parent_kernel(ctx, scope_idx)
                    .ok_or_else(|| format!(
                        "{label}: no installed ancestor kernel.",
                    ))?;
                let filter = comprehension.filter.as_deref();
                let order = comprehension.order.as_ref();
                match &comprehension.mode {
                    ComprehensionMode::Cartesian(clauses) => {
                        let pairs: Vec<(String, String)> = clauses.iter()
                            .map(|c| (c.var.clone(), c.expr.clone())).collect();
                        let needle = pairs.first()
                            .map(|(v, e)| format!("{v} in {e}"))
                            .unwrap_or_default();
                        let parent_coords = ctx.current_parent_kernel.as_ref()
                            .map(|k| k.scope_coordinates().iter().rev().cloned().collect::<Vec<_>>())
                            .unwrap_or_default();
                        let steps = runtime_iterate(
                            ctx, &canonical, &parent, &parent_coords, &pairs, filter, order, None,
                        ).map_err(|e| enrich_with_yaml_location(ctx, &needle, e))?;
                        return dispatch_comprehension(
                            ctx, steps,
                            TerminalAction::Children(children), depth + 1, false,
                        ).await
                            .map_err(|e| enrich_with_yaml_location(ctx, &needle, e));
                    }
                    ComprehensionMode::Union(subspaces) => {
                        if !ctx.quiet() {
                            crate::diag!(crate::observer::LogLevel::Debug,
                                "for_each_union ({} sub-spaces) × {} children",
                                subspaces.len(), children.len());
                        }
                        let total = subspaces.len();
                        for (i, sub) in subspaces.iter().enumerate() {
                            let pairs: Vec<(String, String)> = sub.iter()
                                .map(|c| (c.var.clone(), c.expr.clone())).collect();
                            if !ctx.quiet() {
                                crate::diag!(crate::observer::LogLevel::Debug,
                                    "  sub-space {}/{}: [{}]",
                                    i + 1, total,
                                    pairs.iter().map(|(v, e)| format!("{v} in {e}"))
                                        .collect::<Vec<_>>().join(", "));
                            }
                            let needle = pairs.first()
                                .map(|(v, e)| format!("{v} in {e}"))
                                .unwrap_or_default();
                            let parent_coords = ctx.current_parent_kernel.as_ref()
                                .map(|k| k.scope_coordinates().iter().rev().cloned().collect::<Vec<_>>())
                                .unwrap_or_default();
                            let steps = runtime_iterate(
                                ctx, &canonical, &parent, &parent_coords, &pairs, filter, order,
                                Some((i, total)),
                            ).map_err(|e| enrich_with_yaml_location(ctx, &needle, e))?;
                            dispatch_comprehension(
                                ctx, steps,
                                TerminalAction::Children(children), depth + 1, false,
                            ).await
                                .map_err(|e| enrich_with_yaml_location(ctx, &needle, e))?;
                        }
                        return Ok(());
                    }
                }
            }
            ScenarioNode::IncludedScenario { name, children } => {
                // Transparent at runtime — the wrapper exists
                // only so the scope/scene trees can show the
                // include hierarchy. Walk straight through.
                if !ctx.quiet() {
                    crate::diag!(crate::observer::LogLevel::Debug,
                        "include scenario '{name}' ({} children)",
                        children.len());
                }
                execute_tree_at(ctx, children, depth + 1).await?;
            }
            ScenarioNode::DoWhile { condition, counter, children } => {
                crate::diag!(crate::observer::LogLevel::Debug, "=== do_while: {condition} ===");
                run_do_loop(ctx, condition, counter.as_deref(), false,
                    children, depth + 1).await?;
            }
            ScenarioNode::DoUntil { condition, counter, children } => {
                crate::diag!(crate::observer::LogLevel::Debug, "=== do_until: {condition} ===");
                run_do_loop(ctx, condition, counter.as_deref(), true,
                    children, depth + 1).await?;
            }
        }
        Ok(())
    })
}

// =====================================================================
// Unified comprehension dispatcher — SRD 18b §"M3 — per-scope
// kernel composition". One control-loop harness for every
// iteration kind: for_each / for_combinations / for_each_union
// (tuple comprehensions), do_while / do_until (counter-driven
// loops), and phase-level for_each. Each iteration kind plugs in
// via the [`Comprehension`] strategy trait, which produces
// successive iteration bindings; the dispatcher's single
// per-branch loop applies those bindings to a fresh per-branch
// kernel (`GkKernel::from_program` from the scope's installed
// canonical) and runs the children under it. No duplicated
// recursion logic per iteration kind.
// =====================================================================

/// Strategy plugin: produce the next iteration's typed bindings.
/// `Ok(Some(_))` = run children with these bindings; `Ok(None)` =
/// halt; `Err(_)` = propagate up.
///
/// Activity-side adapter over the GK
/// [`nbrs_variates::comprehension::iterate_scope`] driver:
/// applies strict-vs-warn empty-clause policy with diag emission
/// honoring `ExecCtx::quiet()`. The runtime executor and the
/// pre-map walker both go through `iterate_scope`; `runtime_iterate`
/// just adds the activity-layer concerns (warn-level logging) to
/// each iteration request.
///
/// Returns the materialised step list — runtime needs to know the
/// count up front to decide serial vs. concurrent dispatch
/// (`schedule=` limits, single-iter fast path).
#[allow(clippy::too_many_arguments)]
fn runtime_iterate(
    ctx: &ExecCtx,
    canonical: &std::sync::Arc<nbrs_variates::kernel::GkKernel>,
    parent: &std::sync::Arc<nbrs_variates::kernel::GkKernel>,
    parent_coords: &[ScopeCoord],
    clauses: &[(String, String)],
    filter: Option<&str>,
    order: Option<&nbrs_variates::comprehension::TraversalOrder>,
    union_context: Option<(usize, usize)>,
) -> Result<Vec<nbrs_variates::comprehension::IterationStep>, String> {
    let strict = ctx.strict;
    let quiet = ctx.quiet();
    let on_empty = |var: &str, spec_text: &str| -> Result<(), String> {
        let context_label = match union_context {
            Some((i, n)) => format!(
                "for_each_union sub-space {}/{} clause '{var}'", i + 1, n,
            ),
            None => format!("for_each clause '{var}'"),
        };
        let msg = format!(
            "{context_label}: spec '{spec_text}' produced no values"
        );
        if strict { return Err(format!("strict: {msg}")); }
        if !quiet {
            crate::diag!(crate::observer::LogLevel::Warn, "warning: {msg}");
        }
        Ok(())
    };
    let iter = nbrs_variates::comprehension::iterate_scope(
        canonical, parent, parent_coords, clauses, filter, order, &[], on_empty,
    )?;
    Ok(iter.collect())
}

// `Comprehension` trait + `TupleComprehension` retired: the
// dependent-tuple walk + per-iteration kernel binding is now
// owned by `nbrs_variates::comprehension::iterate_scope` and the
// types it returns. Both runtime (`runtime_iterate`) and pre-map
// (`premap_iterate`) call into the same GK primitive.
//
// Do-loops (`do_while` / `do_until`) bypass this path — they
// need a persistent kernel across iterations (counter
// `set_input`, condition eval, interleaved with child
// execution) which doesn't fit the eager-enumeration shape.
// See [`run_do_loop`] for the streaming dispatcher.

/// What runs at the leaf of each comprehension iteration. The
/// dispatcher switches on this for the per-iteration terminal
/// step — scenario-level for/do scopes descend into children;
/// phase-level for_each runs the phase itself once per
/// iteration value.
#[derive(Clone)]
pub enum TerminalAction<'a> {
    Children(&'a [ScenarioNode]),
    Phase(&'a str),
}

/// Owned form of [`TerminalAction`] for moving into spawned
/// concurrent tasks. `borrow()` reconstructs a borrowed
/// `TerminalAction` for the dispatcher's per-iteration call.
#[derive(Clone)]
enum OwnedTerminal {
    Children(std::sync::Arc<Vec<ScenarioNode>>),
    Phase(String),
}

impl OwnedTerminal {
    fn borrow(&self) -> TerminalAction<'_> {
        match self {
            OwnedTerminal::Children(arc) => TerminalAction::Children(arc.as_slice()),
            OwnedTerminal::Phase(name) => TerminalAction::Phase(name.as_str()),
        }
    }
}

/// Unified comprehension dispatcher. Drains the strategy into a
/// flat tuple list, then walks it serially or concurrently per
/// the level's `schedule=` policy. Each iteration:
///
/// 1. Builds a fresh per-branch `GkKernel` via `from_program`.
/// 2. `bind_outer_scope(parent_kernel)` for inheritance.
/// 3. `set_input` for each iteration-variable value.
/// 4. Pushes itself as `ctx.current_parent_kernel` so leaf
///    phases (and any nested comprehensions) inherit through
///    standard GK chain.
/// 5. Pushes labels for the iteration's variables.
/// 6. Runs the [`TerminalAction`] — either descend into
///    children via `execute_tree_at` or `run_phase` for
///    phase-level `for_each`.
/// 7. Pops labels, restores `current_parent_kernel`.
///
/// `sequential_only` forces serial dispatch regardless of
/// `schedule=` — used for do-loops where iteration N depends on
/// iteration N-1's effects (would need to be revisited for
/// `shared`-state propagation; SRD-16 §"Shared Mutable").
fn dispatch_comprehension<'a>(
    ctx: &'a mut ExecCtx,
    steps: Vec<nbrs_variates::comprehension::IterationStep>,
    terminal: TerminalAction<'a>,
    depth: usize,
    sequential_only: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
    use crate::scheduler::ConcurrencyLimit;
    Box::pin(async move {
        if steps.is_empty() {
            // GK-side `iterate_scope` already routed any clause-
            // level diagnostic through the strict-vs-warn callback;
            // an empty step list here is the success-with-zero-
            // iterations case (filter eliminated everything, or a
            // do-loop condition false from the start).
            return Ok(());
        }

        let limit = ctx.schedule_spec.limit_at(depth);
        let serial = sequential_only
            || matches!(limit, ConcurrencyLimit::Serial)
            || steps.len() <= 1;

        if serial {
            for step in &steps {
                run_one_iteration(ctx, step, &terminal, depth).await?;
            }
            return Ok(());
        }

        let sem: Option<std::sync::Arc<tokio::sync::Semaphore>> = match limit {
            ConcurrencyLimit::Bounded(n) => {
                Some(std::sync::Arc::new(tokio::sync::Semaphore::new(n as usize)))
            }
            ConcurrencyLimit::Unlimited => None,
            ConcurrencyLimit::Serial => unreachable!("handled by serial branch"),
        };
        let mut set = tokio::task::JoinSet::new();
        // The TerminalAction borrows from the caller's slice;
        // for spawning into 'static futures, materialize into an
        // owned form. Children slice → owned Vec; Phase name →
        // owned String.
        let owned_terminal = match &terminal {
            TerminalAction::Children(c) => OwnedTerminal::Children(std::sync::Arc::new(c.to_vec())),
            TerminalAction::Phase(name) => OwnedTerminal::Phase(name.to_string()),
        };

        for step in steps {
            let mut task_ctx = ctx.clone();
            let owned_terminal = owned_terminal.clone();
            let sem = sem.clone();
            set.spawn(async move {
                let _permit = match sem {
                    Some(s) => Some(s.acquire_owned().await.map_err(|e| e.to_string())?),
                    None => None,
                };
                let terminal = owned_terminal.borrow();
                run_one_iteration(&mut task_ctx, &step, &terminal, depth).await
            });
        }

        let mut first_err: Option<String> = None;
        while let Some(res) = set.join_next().await {
            match res {
                Err(join_err) => {
                    if first_err.is_none() {
                        first_err = Some(format!("concurrent comprehension iteration panicked: {join_err}"));
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
    })
}

/// Route a `do_while` / `do_until` scenario node through the
/// streaming dispatcher (SRD-18b §"Deferred follow-on:
/// Streaming dispatch for state-driven comprehensions").
///
/// One persistent kernel for the whole loop's lifetime — forked
/// from the do-loop scope's canonical program at entry, bound to
/// the parent scope once. Each iteration:
///
/// 1. `set_input` the counter on the persistent kernel.
/// 2. Evaluate the condition against the persistent kernel via
///    the standard `interpolate_via_kernel` + `eval_const_expr`
///    path. Halt if the predicate flips.
/// 3. Wrap the persistent kernel in `Arc` and install it as
///    `ctx.current_parent_kernel`. Run children — they read the
///    loop kernel through the standard scope chain.
/// 4. After children return, reclaim the kernel via
///    `Arc::try_unwrap`. Iterations are sequential by design
///    (state-dependent), so the Arc is uniquely owned by the
///    time we get here. Concurrent siblings within an iteration
///    body each cloned the Arc but their tasks have all
///    awaited.
/// 5. Increment counter, repeat.
///
/// Shared writes from children's leaf kernels back to the
/// persistent loop kernel are not yet wired (Stage 2 of the
/// follow-on); the existing per-iteration fork model in
/// `run_one_iteration` doesn't surface a propagate hook to the
/// streaming dispatcher. Conditions that depend purely on the
/// counter (`do_while: counter < 100`) work today; conditions
/// that depend on `shared`-modifier state mutated by children
/// are still pending.
async fn run_do_loop(
    ctx: &mut ExecCtx,
    condition: &str,
    counter: Option<&str>,
    invert: bool,
    children: &[ScenarioNode],
    depth: usize,
) -> Result<(), String> {
    // Find the matching scope-tree node by structural content.
    // Both DoWhile and DoUntil get installed kernels per the
    // runner.rs install loop, but they're stored under their
    // own ScopeKind variants so the lookup needs to check both.
    let scope_idx = ctx.scope_tree.iter_dfs().find_map(|(idx, node)| {
        match &node.kind {
            crate::scope_tree::ScopeKind::DoWhile { condition: c, counter: ct } => {
                if c == condition && ct.as_deref() == counter && !invert {
                    Some(idx)
                } else { None }
            }
            crate::scope_tree::ScopeKind::DoUntil { condition: c, counter: ct } => {
                if c == condition && ct.as_deref() == counter && invert {
                    Some(idx)
                } else { None }
            }
            _ => None,
        }
    }).ok_or_else(|| format!(
        "do-loop '{condition}': no matching scope-tree entry — \
         scenario/scope-tree drift bug."
    ))?;
    let canonical = ctx.scope_tree.nodes[scope_idx].cached_kernel.get()
        .cloned()
        .ok_or_else(|| format!(
            "do-loop '{condition}': scope at index {scope_idx} has no installed kernel."
        ))?;
    let parent = effective_parent_kernel(ctx, scope_idx)
        .ok_or_else(|| format!("do-loop '{condition}': no installed ancestor kernel."))?;

    // Persistent loop kernel: one fork from the do-loop scope's
    // canonical program, bound once from the parent. Lives for
    // the loop's whole duration.
    let mut loop_kernel = nbrs_variates::kernel::GkKernel::from_program(
        canonical.program().clone(),
    );
    loop_kernel.bind_outer_scope(&parent);

    let mut counter_value: u64 = 0;
    loop {
        // Set the counter on the persistent kernel.
        if let Some(c) = counter
            && let Some(idx) = loop_kernel.program().find_input(c)
        {
            loop_kernel.state().set_input(
                idx,
                nbrs_variates::node::Value::U64(counter_value),
            );
        }

        // Evaluate the condition against the persistent kernel.
        let interpolated = nbrs_variates::comprehension::interpolate_via_kernel(
            condition, &loop_kernel,
        ).map_err(|e| format!("do-loop condition '{condition}': {e}"))?;
        let cond_value = nbrs_variates::dsl::compile::eval_const_expr(&interpolated)
            .map_err(|e| format!("do-loop condition '{condition}': {e}"))?;
        let cond_true = match cond_value {
            nbrs_variates::node::Value::Bool(b) => b,
            nbrs_variates::node::Value::U64(n) => n != 0,
            nbrs_variates::node::Value::F64(n) => n != 0.0,
            other => return Err(format!(
                "do-loop condition '{condition}': expected bool/u64/f64, got {other:?}",
            )),
        };
        let should_continue = if invert { !cond_true } else { cond_true };
        if !should_continue { break; }

        // Install the persistent kernel as ctx.current_parent_kernel
        // for children to read via the standard scope chain. We
        // move it into the Arc temporarily, then reclaim it after
        // children return.
        let prior_parent = ctx.current_parent_kernel.take();
        if let Some(c) = counter {
            ctx.push_label(c, &counter_value.to_string());
        }
        let arc_loop = std::sync::Arc::new(std::mem::replace(
            &mut loop_kernel,
            // Placeholder — overwritten on reclaim. Cheap dummy.
            nbrs_variates::kernel::GkKernel::from_program(canonical.program().clone()),
        ));
        ctx.current_parent_kernel = Some(arc_loop.clone());

        let res = execute_tree_at(ctx, children, depth).await;

        ctx.current_parent_kernel = prior_parent;
        if counter.is_some() { ctx.pop_label(); }

        // Reclaim the persistent kernel. Iterations are sequential
        // and concurrent children within the iteration body have
        // all awaited by this point, so the Arc is uniquely owned.
        loop_kernel = std::sync::Arc::try_unwrap(arc_loop).map_err(|_| format!(
            "do-loop '{condition}' iteration {counter_value}: persistent kernel \
             still referenced after children completed — concurrency bug."
        ))?;

        res?;
        counter_value = counter_value.saturating_add(1);
    }

    Ok(())
}

/// Run one comprehension iteration: build branch kernel, set
/// inputs, push as `current_parent_kernel`, run the terminal
/// action (children tree-walk or single phase), restore.
/// Shared by serial and concurrent dispatch paths.
/// Drive one iteration of a comprehension dispatch: install the
/// pre-built bound kernel as the live parent, push iteration
/// labels for diagnostics, then descend through the terminal
/// action.
///
/// The bound kernel comes from the GK-side
/// [`nbrs_variates::comprehension::IterationStep`] — same
/// kernel both pre-map and runtime see for the same iteration
/// position. No `from_program`/`bind_outer_scope`/`set_input`
/// dance here; that recipe is owned by `GkKernel::for_iteration`
/// and reached via `iterate_scope`.
async fn run_one_iteration(
    ctx: &mut ExecCtx,
    step: &nbrs_variates::comprehension::IterationStep,
    terminal: &TerminalAction<'_>,
    depth: usize,
) -> Result<(), String> {
    let prior_parent = ctx.current_parent_kernel.take();
    ctx.current_parent_kernel = Some(step.bound_kernel.clone());
    for (var, value) in &step.bindings {
        ctx.push_label(var, &value.to_display_string());
    }

    // Children downstream consume iter-var values via
    // `ctx.current_parent_kernel` (set above), no separate
    // HashMap parameter.
    let res = match terminal {
        TerminalAction::Children(children) => {
            execute_tree_at(ctx, children, depth).await
        }
        TerminalAction::Phase(name) => {
            run_phase(ctx, name).await
        }
    };

    for _ in &step.bindings { ctx.pop_label(); }
    ctx.current_parent_kernel = prior_parent;
    res
}

/// Empty-bindings sentinel used by `run_phase` when the
/// kernel-routed M3.4 path is active. Iteration vars come via
/// the parent kernel's manifest in that case, so `build_scope`
/// gets an empty iteration_vars map and skips its
/// `add_iteration_var` injection.
static EMPTY_BINDINGS: std::sync::LazyLock<HashMap<String, String>> =
    std::sync::LazyLock::new(HashMap::new);

/// Execute one phase. Iteration-variable values come from the
/// `current_parent_kernel` manifest — every name visible at this
/// phase's enclosing scope is reachable via the standard GK API
/// on that one kernel. The legacy `bindings: HashMap` parameter
/// is gone (M3.4b).
async fn run_phase(
    ctx: &mut ExecCtx,
    phase_name: &str,
) -> Result<(), String> {
    let phase_start = std::time::Instant::now();
    let phase = ctx.phases.get(phase_name)
        .ok_or_else(|| format!("phase '{phase_name}' not found"))?
        .clone();
    let has_bindings = phase.ops.iter().any(|op| !op.bindings.is_empty());

    // Derive iteration-variable values from the current parent
    // kernel's namespace. Names visible at the parent scope —
    // own outputs (folded constants from `final` bindings) plus
    // inherited extern inputs (populated by `bind_outer_scope`
    // chain or per-iteration `set_input` from the dispatcher)
    // — flow through a single `GkKernel::lookup` call per name,
    // which is the canonical scope-aware reader (SRD-16
    // §"Visibility Rules: Shadowing"): folded outputs first,
    // then cell-aware input read, then `None`. Switching off
    // the older `get_constant` + `get_input` two-pass shape
    // closes the last call site that conflated the two read
    // tiers — Stage 2 of the params-kernel rework.
    //
    // iter_var_values must hold the FULL set of names visible
    // at the parent scope (own outputs + cascade-inherited
    // workload params + extern input slots) because it drives
    // both op-template `{name}` text substitution AND config
    // expression resolution downstream — dropping inherited
    // names here would leave `{keyspace}` etc. unresolved in
    // op templates. Display layers (phase_labels, scene tree)
    // filter to own names separately via
    // `program.own_output_names()` / `is_inherited`.
    //
    // IndexMap (insertion-ordered) so `phase_labels`,
    // `gk_context`, and `activity_name` reflect scenario-tree
    // declaration order — `output_names()` / `input_names()`
    // already return Vecs in that order. A plain HashMap here
    // randomises iteration per process and produced visibly
    // jumbled multi-clause for_each labels.
    let iter_var_values: IndexMap<String, String> = {
        let mut out = IndexMap::new();
        if let Some(parent) = ctx.current_parent_kernel.as_ref() {
            for name in parent.program().output_names() {
                if let Some(v) = parent.lookup(name) {
                    out.insert(name.to_string(), v.to_display_string());
                }
            }
            for name in parent.program().input_names() {
                if out.contains_key(&name) { continue; }
                if let Some(v) = parent.lookup(&name) {
                    out.insert(name.clone(), v.to_display_string());
                }
            }
        }
        out
    };
    let is_iter = !iter_var_values.is_empty();

    // The `=== phase: NAME ===` decoration is informational
    // chrome — the per-phase startup line below already names
    // the phase in a structured way that the inline status
    // thread, the post-run summary, and the TUI all consume.
    // Demoted to Debug so default Info-level stderr stays
    // hierarchically-clean; `loglevel=debug` brings it back
    // for callers that want the visual section break.
    crate::diag!(crate::observer::LogLevel::Debug, "=== phase: {phase_name} ===");
    if is_iter
        && let Some(parent) = ctx.current_parent_kernel.as_ref()
    {
        let prog = parent.program();
        for (var, val) in &iter_var_values {
            if !val.is_empty() && !prog.is_inherited(var) {
                crate::diag!(crate::observer::LogLevel::Debug, "  {var}={val}");
            }
        }
    }

    // --- Compile inner kernel via BindingScope ---
    let (iter_op_builder, iter_ops, runtime_cursor_extents) = if is_iter || has_bindings {
        let mut ops = phase.ops.clone();

        // SRD-16 single read path: every `{name}` placeholder
        // in op fields and op-level params resolves through
        // the populated parent kernel's `lookup` interface —
        // the same surface that answers iter vars, cascaded
        // workload params, and any other in-scope name. There
        // is no parallel HashMap or fresh-state pull. Per-cycle
        // bindings declared in this phase's `bindings:` block
        // pass through (the dispenser resolves them at execute
        // time); anything else that doesn't resolve is a hard
        // error with the field path and the in-scope name list
        // surfaced to the operator.
        let parent_kernel = ctx.current_parent_kernel.as_ref().ok_or_else(|| format!(
            "phase '{phase_name}': no current_parent_kernel — \
             single-resolution-path requires the populated parent kernel",
        ))?;
        crate::scope::resolve_placeholders_via_kernel(&mut ops, parent_kernel)
            .map_err(|e| format!("phase '{phase_name}': {e}"))?;

        // Rewrite inline expressions ({{expr}} → {__expr_N}) in op templates.
        // This modifies op template strings and returns the expr→name map
        // so the scope can register the corresponding bindings.
        crate::scope::rewrite_inline_exprs(&mut ops);

        // Strip adapter/driver (resolved per-phase, not from params)
        for op in &mut ops { op.params.remove("adapter"); op.params.remove("driver"); }

        // M3.4a: when the dependent-tuple dispatcher (or any
        // future kernel-routed enclosing scope) has installed a
        // per-branch parent kernel via
        // `ctx.current_parent_kernel`, use *that kernel's*
        // manifest as the auto-extern source. Iteration vars
        // are scope outputs of the parent (per M3.2's extern
        // auto-passthrough synthesis) and inherited workload
        // values flow through the same chain — one source of
        // resolvable values per SRD-16. The empty
        // `iteration_vars` map below means `build_scope`
        // doesn't separately call `add_iteration_var`; the
        // names auto-extern from the parent manifest with
        // their already-detected native types. When no parent
        // kernel is set (legacy non-dispatcher path), fall
        // M3.4b: every leaf phase compiles against its
        // immediate parent scope's manifest (via
        // `current_parent_kernel`). Iter vars are scope outputs
        // there; the empty `iteration_vars` map below skips
        // `add_iteration_var`'s typed-extern injection — the
        // names auto-extern from the parent's manifest with
        // their already-detected native types. Workload root is
        // always installed (M3.4b), so this branch always
        // fires; the legacy `outer_manifest` fallback is gone.
        let parent_kernel = ctx.current_parent_kernel.as_ref()
            .ok_or_else(|| format!(
                "phase '{phase_name}': no current_parent_kernel — \
                 workload root install missed at session start (internal bug)."
            ))?;
        let effective_manifest = crate::runner::extract_manifest(parent_kernel.program());

        // Build typed scope from structured inputs. M3.6:
        // phase-level scope passes empty workload_params —
        // those are now scope outputs of the workload kernel
        // (declared as `final` bindings at compile) and reach
        // this phase via the parent-kernel manifest's
        // auto-extern. Local injection here would just create
        // duplicate locals.
        let scope = crate::scope::build_scope(
            &ops,
            &EMPTY_BINDINGS,
            &effective_manifest,
            &EMPTY_BINDINGS,
            &ctx.phases,
            phase.cycles.as_deref(),
            &[], // exclude
        )?;

        // Validate scope rules (shadow detection, final checks)
        let gk_context = if iter_var_values.is_empty() {
            format!("phase '{phase_name}'")
        } else {
            let vars: Vec<String> = iter_var_values.iter()
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
            if let Some(canonical) = node.cached_kernel.get() {
                // Cache hit: rebind path. Build a fresh kernel
                // from the canonical's program; the freshly-
                // allocated state is empty and will be populated
                // below.
                nbrs_variates::kernel::GkKernel::from_program(canonical.program().clone())
            } else {
                // First call for this phase — compile, install
                // the just-compiled kernel as this scope's
                // canonical instance (so `lookup_name` can read
                // its folded constants), then build a fresh
                // execution kernel from its program for this
                // iteration. Subsequent iterations all hit the
                // OnceLock cache hit branch above. The program
                // is iter-invariant (iter vars flow as wires;
                // dataset specs interpolate at eval), so the
                // same program serves every iteration.
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
                let _ = node.cached_kernel.set(std::sync::Arc::new(compiled));
                nbrs_variates::kernel::GkKernel::from_program(prog)
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

        // Wire inherited values + iter-var values from the
        // parent scope's per-branch kernel via standard GK
        // chain composition. Single call, single source of
        // values — SRD-16 §"Visibility Rules".
        kernel.bind_outer_scope(parent_kernel);

        // ─── Plan B: Init-Binding Contract (scope-activation) ─────
        //
        // SRD 11 §"Init Binding Contract" Plan B: every binding
        // declared `init` must materialize as a single concrete
        // value once iteration externs are populated. We pull each
        // one on the activation kernel's state — that runs the
        // eval exactly once per scope activation — and verify the
        // result is non-None. The pulled values are captured into
        // OpBuilder.init_overrides below so per-fiber states inherit
        // them without re-evaluating.
        //
        // Plan A (in fold_init_constants_impl) caught structural
        // violations at compile time; Plan B catches runtime
        // failures (eval panic via catch_unwind, fatal Value::None
        // returns, missing output_map entry).
        let init_outputs: Vec<String> = kernel.program().init_outputs()
            .iter().cloned().collect();
        for init_name in &init_outputs {
            // catch_unwind so a panicking eval becomes a clean error,
            // not a fiber-pool poisoning panic. Nodes that do blocking
            // I/O are responsible for parking the worker themselves
            // (see `nbrs-variates`'s `run_blocking_io`); the activation
            // boundary stays a plain eval.
            let pull_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                kernel.pull(init_name).clone()
            }));
            match pull_result {
                Ok(v) if !matches!(v, nbrs_variates::node::Value::None) => {}
                Ok(_) => {
                    return Err(format!(
                        "{gk_context}: init binding '{init_name}' violates the init contract: \
                         scope-init eval returned Value::None (per SRD 11 §\"Init Binding Contract\" \
                         Plan B). The eval function signaled failure or returned no value."
                    ));
                }
                Err(payload) => {
                    let msg = panic_message(&payload);
                    return Err(format!(
                        "{gk_context}: init binding '{init_name}' violates the init contract: \
                         scope-init eval panicked: {msg} (per SRD 11 §\"Init Binding Contract\" \
                         Plan B)."
                    ));
                }
            }
        }
        // ───────────────────────────────────────────────────────────

        // Resolve cursor extents whose `range(...)` bounds depend on
        // wire-bound externs (e.g., `vector_count("{dataset}:{profile}")`
        // where `dataset` and `profile` are iter-var externs). The
        // compiler couldn't const-fold these, so it stashed the aux
        // output names on the schema's `extent_outputs`. Now that the
        // externs are populated on `kernel.state`, pull each pair and
        // record the resolved extent — keyed by cursor name — for the
        // source-factory construction below.
        let mut runtime_extents: HashMap<String, u64> = HashMap::new();
        let cursor_specs: Vec<(String, Option<(String, String)>, Option<u64>)> = kernel
            .program()
            .cursor_schemas()
            .iter()
            .map(|s| (s.name.clone(), s.extent_outputs.clone(), s.extent_limit))
            .collect();
        for (name, outputs, limit) in cursor_specs {
            if let Some((start_out, end_out)) = outputs {
                let start = kernel.pull(&start_out).as_u64();
                let end = kernel.pull(&end_out).as_u64();
                let extent = end.saturating_sub(start);
                let final_extent = limit.map(|l| extent.min(l)).unwrap_or(extent);
                runtime_extents.insert(name, final_extent);
            }
        }
        // Wrap in an `Arc<OpBuilder>` so the per-iteration extern
        // values just bound on `kernel.state` ride along to every
        // fiber via `OpBuilder::create_fiber_builder`. Without
        // this the Arc<GkProgram> alone would be iter-invariant
        // and `{table}` / `{profile}` references in op templates
        // would render with default (empty) values.
        let op_builder = Arc::new(OpBuilder::new(kernel));
        (op_builder, ops, runtime_extents)
    } else {
        // Workload-kernel fallback: no per-iteration values to
        // inject, so a no-scope-values wrapper is sufficient.
        (Arc::new(OpBuilder::from_program(ctx.program.clone())), phase.ops.clone(), HashMap::new())
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
        for (v, val) in &iter_var_values { expanded = expanded.replace(&format!("{{{v}}}"), val); }
        expanded = crate::runner::expand_workload_params(&expanded, &ctx.workload_params);
        let stanzas = crate::runner::parse_count(&expanded)
            .or_else(|| {
                if expanded.starts_with('{') && expanded.ends_with('}') {
                    let inner = &expanded[1..expanded.len()-1];
                    nbrs_variates::dsl::compile::eval_const_expr(inner).ok()
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
            let pairs: Vec<String> = iter_var_values.iter()
                .map(|(k, v)| format!("{k}={v}")).collect();
            format!(" ({})", pairs.join(", "))
        } else { String::new() };
        crate::describe::print_kernel_analysis(phase_name, &note, &iter_op_builder.program());
    }
    // NOTE: the depth==Phase early-return used to live here, but
    // it short-circuited *before* component attach + control
    // declarations, leaving `dryrun=controls` walking an empty
    // tree. The guard now fires below — after phase_component
    // attach, `Activity::attach_component` (declares concurrency
    // / rate), and the per-phase adapter `declare_controls`
    // pass — so the discovery path sees the full control surface
    // without spinning up the fiber pool / progress thread / or
    // running any cycles.

    // Resolve concurrency
    let phase_concurrency = match phase.concurrency.as_ref() {
        Some(s) => {
            let mut exp = crate::runner::expand_workload_params(s, &ctx.workload_params);
            for (v, val) in &iter_var_values { exp = exp.replace(&format!("{{{v}}}"), val); }
            exp.parse::<usize>().map_err(|_| format!(
                "phase '{phase_name}': concurrency '{exp}' not a valid integer"))?
        }
        None => ctx.concurrency,
    };

    // Phase labels read straight off the parent kernel's
    // formal scope-coordinate path (SRD 18b §"Scope
    // coordinates" / `nbrs_variates::kernel::scope_coords`).
    // The path is leaf-first: each entry is one scope's own
    // coordinates (the LHS of its `var in expr` clauses).
    // We render as striated parens so the operator can read
    // off the active iteration at each enclosing level
    // independently:
    //
    //     ann_query (k=10, limit=20), (table=…, optimize_for=…)
    //
    // Empty strata (scenario lists, no-coord phases) are
    // skipped. With no parent kernel (root-scope phase) the
    // label is empty.
    let phase_labels = match ctx.current_parent_kernel.as_ref() {
        Some(parent) => format_scope_coordinate_path(parent.scope_coordinates()),
        None => String::new(),
    };
    let stanza_len = op_sequence.stanza_length();
    // `phase_starting` moved below the source-factory + progress-
    // extent computation so the cycle count we report is the same
    // value that bounds the loop. See immediately after
    // `progress_extent` is resolved.

    // Activity name carries only the **leaf** scope's iter
    // coords — the innermost stratum from
    // `parent_kernel.scope_coordinates()[0]`. The full path
    // (`(k=10), (table=…), (profile=…)`) is what
    // `phase_labels` carries for diagnostic / identity uses;
    // surfacing the leaf only on the inline status line keeps
    // the line short enough to fit in a typical terminal
    // width and shows the operator the iter that's actively
    // changing across executions of this same phase. Outer
    // strata are stable across an entire scope iteration and
    // are visible via the TUI's tree / pre-map plan.
    let activity_name = {
        let leaf_label = ctx.current_parent_kernel.as_ref()
            .and_then(|k| k.scope_coordinates().first())
            .filter(|c| !c.is_empty())
            .map(|c| c.vars.iter()
                .map(|(k, v)| format!("{k}={}", v.to_display_string()))
                .collect::<Vec<_>>()
                .join(", "))
            .unwrap_or_default();
        if leaf_label.is_empty() {
            phase_name.to_string()
        } else {
            format!("{phase_name} ({leaf_label})")
        }
    };

    // If the compiled kernel declares cursors, create a source factory
    // from the first cursor's schema (name + extent). Otherwise the
    // Activity falls back to a range source named "cycles".
    let source_factory: Option<Arc<dyn nbrs_variates::source::DataSourceFactory>> = {
        let program = iter_op_builder.program();
        let schemas = program.cursor_schemas();
        if let Some(schema) = schemas.first() {
            // Prefer the runtime-resolved extent (computed above after
            // iter-var externs were bound) over the schema's
            // compile-time extent — the latter is None when the
            // cursor's `range(...)` bounds depend on wire-bound
            // externs like `vector_count("{dataset}:{profile}")`.
            let extent = runtime_cursor_extents.get(&schema.name).copied()
                .or(schema.extent)
                .unwrap_or(phase_cycles);
            Some(Arc::new(
                nbrs_variates::source::RangeSourceFactory::named(&schema.name, 0, extent)
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

    // Now that the source factory and its global extent are
    // settled, fire phase_starting with the actual loop bound —
    // same value that flows into the activity's progress tracker
    // (`cursor_extent: progress_extent` below). Reporting
    // `phase_cycles` here would print `1` for any cursor-driven
    // phase whose `cycles:` field is omitted (the workload's
    // common case), even though the activity goes on to run
    // `vector_count(...)` cycles.
    // Transition the global scene tree first so the
    // observer's startup log line (`LogOnlyObserver`'s
    // `phase_starting`) can look up `[N/total]` and the
    // depth-indent against a Running entry. Reversing this
    // order leaves the per-phase log line un-prefixed and
    // un-indented because the lookup fires before
    // `set_phase_running`.
    crate::scene_tree::with_global_mut(|t| {
        t.set_phase_running(phase_name, &phase_labels, stanza_len);
    });
    ctx.observer.phase_starting(phase_name, &phase_labels,
        stanza_len, progress_extent, phase_concurrency);

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
        // Same plumbing as the workload-level activity build —
        // see runner.rs. Per-phase activity gets the live
        // suppression flag so a TUI dismissal mid-run resumes
        // status-line emission.
        suppress_status_line: ctx.observer.live_suppress_flag()
            .unwrap_or_else(|| {
                Arc::new(std::sync::atomic::AtomicBool::new(
                    ctx.observer.suppresses_stderr()))
            }),
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
    let sigdigs = nbrs_metrics::instruments::histogram::resolve_hdr_sigdigs(
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

    // Adapter-level dynamic controls (SRD 23). Declared here at
    // phase-attach time so `dryrun=controls` walks a populated
    // component tree before any cycles run. `Activity::run_with_adapters`
    // also calls this — the adapter trait contract requires
    // `declare_controls` to be idempotent so the second call is
    // a no-op.
    crate::activity::declare_adapter_controls(&adapters, &phase_component);

    // dryrun=Phase: every phase has now been constructed —
    // component attached, concurrency / rate / adapter controls
    // declared — so dump-the-tree paths (`dryrun=controls` in
    // particular) see the full surface. Stop before fiber pool
    // spawn / progress thread / cycle execution.
    //
    // Fire phase_completed (with a sentinel zero duration) so
    // scene-tree state transitions Running → Completed and the
    // post-run summary renders `[ok] phase` instead of
    // `[..] phase (still running)`. The renderer suppresses the
    // " 0.00s" duration suffix for zero-duration completions
    // (see `nbrs_tui::observer::print_post_run_summary`).
    if ctx.diag.depth == crate::runner::ExecDepth::Phase {
        ctx.observer.phase_completed(phase_name, &phase_labels, 0.0);
        crate::scene_tree::with_global_mut(|t| {
            t.set_phase_completed(phase_name, &phase_labels, 0.0);
        });
        return Ok(());
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

    crate::diag!(crate::observer::LogLevel::Debug,
        "phase '{phase_name}': activity starting (concurrency={phase_concurrency})");
    // Clone the stop-reason handle BEFORE consuming the activity;
    // populated by the per-cycle stop trigger inside the run if a
    // `stop` error handler fires. Read after the run to surface
    // the actual triggering error (instead of a bare "stopped by
    // error handler") in the phase-level error.
    let stop_reason = activity.stop_reason.clone();
    let stopped = crate::runner::run_activity_simple(
        activity, adapters, phase_driver, iter_op_builder,
    ).await;
    crate::diag!(crate::observer::LogLevel::Debug,
        "phase '{phase_name}': activity returned (stopped={stopped})");

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
        use nbrs_metrics::component::InstrumentSet;
        crate::diag!(crate::observer::LogLevel::Debug,
            "phase '{phase_name}': capture_delta start");
        let final_delta = final_metrics.capture_delta(std::time::Duration::from_secs(1));
        crate::diag!(crate::observer::LogLevel::Debug,
            "phase '{phase_name}': cadence ingest(final_delta) start");
        ctx.cadence_reporter.ingest(&labels, final_delta.clone());
        crate::diag!(crate::observer::LogLevel::Debug,
            "phase '{phase_name}': cadence ingest(final_delta) returned");
        ctx.stop_handle.report_frame(&final_delta);
        crate::diag!(crate::observer::LogLevel::Debug,
            "phase '{phase_name}': stop_handle.report_frame(final_delta) returned");

        // Flush validation metrics (recall, precision) as gauges
        if let Some(vframe) = validation_frame.lock().unwrap_or_else(|e| e.into_inner()).take() {
            crate::diag!(crate::observer::LogLevel::Debug,
                "phase '{phase_name}': cadence ingest(vframe) start");
            ctx.cadence_reporter.ingest(&labels, vframe.clone());
            crate::diag!(crate::observer::LogLevel::Debug,
                "phase '{phase_name}': cadence ingest(vframe) returned");
            ctx.stop_handle.report_frame(&vframe);
            crate::diag!(crate::observer::LogLevel::Debug,
                "phase '{phase_name}': stop_handle.report_frame(vframe) returned");
        }

        crate::diag!(crate::observer::LogLevel::Debug,
            "phase '{phase_name}': cadence close_path start");
        ctx.cadence_reporter.close_path(&labels);
        crate::diag!(crate::observer::LogLevel::Debug,
            "phase '{phase_name}': cadence close_path returned");
    }

    // Transition to Stopped
    {
        let mut pc = phase_component.write().unwrap_or_else(|e| e.into_inner());
        pc.set_state(ComponentState::Stopped);
    }
    crate::diag!(crate::observer::LogLevel::Debug,
        "phase '{phase_name}': phase_component set Stopped");

    let phase_duration = phase_start.elapsed().as_secs_f64();
    if stopped {
        // Pull the first triggering error captured by the
        // activity's stop_flag setter (activity.rs per-cycle
        // dispatch). Fall back to a bare reason when the stop
        // came from an early-init path that doesn't populate it
        // (validate_bind_points, missing adapter, etc. — those
        // paths log directly to stderr).
        let reason = stop_reason.lock().ok()
            .and_then(|g| g.clone())
            .unwrap_or_else(|| "stopped by error handler".to_string());
        let detail_msg = format!("stopped by error handler: {reason}");
        // `phase_labels` already carries its own striated
        // Failure line. Drops the striated coord suffix and
        // indents to phase scope depth — same hierarchic
        // pattern as the completion line below. Red on the
        // phase name + the failure-reason summary so a failure
        // is visually distinct from a normal completion in
        // tui=terminal output.
        let depth_indent = crate::scene_tree::running_phase_indent();
        let color = crate::observer::use_color();
        let bold = color.then(|| "\x1b[1m").unwrap_or("");
        let red = color.then(|| "\x1b[31m").unwrap_or("");
        let dim = color.then(|| "\x1b[2m").unwrap_or("");
        let reset = color.then(|| "\x1b[0m").unwrap_or("");
        crate::diag!(crate::observer::LogLevel::Error,
            "{depth_indent}phase '{bold}{phase_name}{reset}' {red}{detail_msg}{reset} {dim}({phase_duration:.2}s){reset}");
        ctx.observer.phase_failed(phase_name, &phase_labels, &detail_msg);
        crate::scene_tree::with_global_mut(|t| {
            t.set_phase_failed(phase_name, &phase_labels, &detail_msg);
        });
        return Err(format!("phase '{phase_name}' {detail_msg}"));
    }

    // Indent the completion line by scope depth so
    // tui=terminal logs read as a hierarchic walk: scope
    // headers + phase startup + activity end-of-run lines
    // + this completion line all share the same indent
    // basis. The striated `(coord)` suffix is dropped
    // because the scope headers above the phase row already
    // carry the coordinate context — repeating it on every
    // start/complete line is the "duplicitous" output the
    // tui=terminal cleanup explicitly removed.
    let depth_indent = crate::scene_tree::current()
        .and_then(|t| t.find_phase(phase_name, &phase_labels,
            Some(&crate::scene_tree::PhaseStatus::Running))
            .and_then(|id| t.nodes.get(id).map(|n| n.depth.saturating_sub(1))))
        .map(|d| "  ".repeat(d))
        .unwrap_or_default();
    let color = crate::observer::use_color();
    let bold = color.then(|| "\x1b[1m").unwrap_or("");
    let green = color.then(|| "\x1b[32m").unwrap_or("");
    let dim = color.then(|| "\x1b[2m").unwrap_or("");
    let reset = color.then(|| "\x1b[0m").unwrap_or("");
    crate::diag!(crate::observer::LogLevel::Info,
        "{depth_indent}phase '{bold}{phase_name}{reset}' {green}complete{reset} {dim}({phase_duration:.2}s){reset}");
    ctx.observer.phase_completed(phase_name, &phase_labels, phase_duration);
    crate::scene_tree::with_global_mut(|t| {
        t.set_phase_completed(phase_name, &phase_labels, phase_duration);
    });
    Ok(())
}

/// Format bindings as a sorted labels string for stable matching.
///
// `format_scope_coordinate_path` lives on the GK side — see
// `nbrs_variates::kernel::format_scope_coordinate_path`. Re-exporting
// the path here would just be alias chrome; consumers in this crate
// import it directly from `nbrs_variates::kernel`.

/// Resolve a for_each expression.
/// Split a `for_each` spec string of the form `"<var> in <expr>"`
/// into its two halves. Mirrors `scope_tree::parse_for_each_spec`
/// (private to that module); we duplicate the logic locally
/// rather than expose it because the splits are trivial and
/// the routing code is the only consumer that needs both halves.
fn parse_var_in_expr(spec: &str) -> (String, String) {
    if let Some(idx) = spec.find(" in ") {
        let (lhs, rhs) = spec.split_at(idx);
        (lhs.trim().to_string(), rhs[" in ".len()..].trim().to_string())
    } else {
        (String::new(), spec.to_string())
    }
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
    scope_tree: &crate::scope_tree::ScopeTree,
    strict: bool,
    // `initial_path`: caller seeds this with the scenario's
    // location (`vec![PathSegment::Scenario(name)]`); pre-map
    // extends it through every nested for_each /
    // for_combinations / do-loop / sub-scenario as it walks,
    // so each phase node carries its full identity per
    // SRD-44 §"Phase identity".
    initial_path: Vec<crate::checkpoint::PathSegment>,
) -> Result<SceneTree, String> {
    let mut tree = SceneTree::new();
    let root = tree.root();
    pre_map_recursive(nodes, phases, scope_tree, root, &[], None, &initial_path, &mut tree, strict)?;
    Ok(tree)
}

/// Pre-map walker — uses the same scope-tree-installed kernels
/// the runtime dispatcher uses, so the SceneTree displayed
/// at session start is an exact preview of the runtime's
/// iteration shape. No separate session-start logic; the plan
/// view IS what the session does.
///
/// `parent_coords` is the root-first scope-coordinate chain
/// accumulated through enclosing comprehension iterations. Each
/// concrete phase emits its labels via
/// [`format_scope_coordinate_path`] applied to the leaf-first
/// reversal of this chain — same formatter the runtime uses on
/// `parent_kernel.scope_coordinates()`, so pre-mapped phase
/// nodes share their structural identity with the runtime
/// transitions that activate them.
///
/// `effective_parent_kernel` is the **bound** kernel of the
/// immediately-enclosing iteration. Inner comprehensions and
/// phase-level for_each clauses interpolate placeholders like
/// `vec_{profile}` against this kernel, so a nested for_each
/// whose spec depends on an outer iter-var resolves correctly
/// at pre-map time. `None` at the top-level call; populated
/// per-iteration as we descend through outer comprehensions.
/// Mirrors the runtime's `ExecCtx::current_parent_kernel`
/// (`effective_parent_kernel` in [`execute_node`]).
fn pre_map_recursive(
    nodes: &[ScenarioNode],
    phases: &HashMap<String, WorkloadPhase>,
    scope_tree: &crate::scope_tree::ScopeTree,
    parent: SceneNodeId,
    parent_coords: &[ScopeCoord],
    effective_parent_kernel: Option<&std::sync::Arc<nbrs_variates::kernel::GkKernel>>,
    // `parent_path`: structural YAML path accumulated from the
    // workload root down to the current parent scope. Each
    // call appends one PathSegment per scenario / for_each /
    // for_combinations / do-loop level, populating each
    // created scene-tree node with its own complete path. See
    // SRD-44 §"Phase identity".
    parent_path: &[crate::checkpoint::PathSegment],
    tree: &mut SceneTree,
    strict: bool,
) -> Result<(), String> {
    use crate::checkpoint::PathSegment;
    for node in nodes {
        match node {
            ScenarioNode::Phase(name) => {
                let op_names: Vec<String> = phases.get(name.as_str())
                    .map(|p| p.ops.iter().map(|op| op.name.clone()).collect())
                    .unwrap_or_default();
                let phase_fe = phases.get(name.as_str())
                    .and_then(|p| p.for_each.clone());
                if let Some(spec) = phase_fe {
                    let (var, expr) = parse_var_in_expr(&spec);
                    let scope_idx = scope_tree.phase_node_by_name(name)
                        .ok_or_else(|| format!(
                            "phase '{name}' for_each '{spec}': no scope-tree entry"
                        ))?;
                    let canonical = scope_tree.nodes[scope_idx].cached_kernel.get();
                    let parent_kernel = effective_parent_kernel.cloned()
                        .or_else(|| scope_tree.nearest_installed_ancestor_kernel(scope_idx));
                    let steps = match (canonical, parent_kernel.as_ref()) {
                        (Some(c), Some(p)) => premap_iterate(
                            c, p, parent_coords,
                            &[(var.clone(), expr.clone())],
                            None, None, None, strict,
                        )?,
                        _ => Vec::new(),
                    };
                    // Phase-level for_each: the wrapping scope
                    // node represents the for_each construct
                    // (one path level), and each per-iteration
                    // phase node sits under it with the phase
                    // name as the terminal segment.
                    let mut scope_path = parent_path.to_vec();
                    scope_path.push(PathSegment::ForEach { var: var.clone() });
                    let scope = tree.push(
                        parent,
                        NodeKind::Scope,
                        format!("phase.for_each {var} in [{}]",
                            steps.iter()
                                .filter_map(|s| s.bindings.first().map(|(_, v)| v.to_display_string()))
                                .collect::<Vec<_>>()
                                .join(", ")),
                        "",
                    );
                    tree.set_yaml_path(scope, scope_path.clone());
                    let mut phase_path = scope_path;
                    phase_path.push(PathSegment::Phase(name.clone()));
                    for step in &steps {
                        let id = tree.push(
                            scope,
                            NodeKind::Phase,
                            name.clone(),
                            canonical_phase_label(&step.coord_path),
                        );
                        tree.set_phase_op_names(id, op_names.clone());
                        tree.set_yaml_path(id, phase_path.clone());
                    }
                } else {
                    let mut phase_path = parent_path.to_vec();
                    phase_path.push(PathSegment::Phase(name.clone()));
                    let id = tree.push(
                        parent,
                        NodeKind::Phase,
                        name.clone(),
                        canonical_phase_label(parent_coords),
                    );
                    tree.set_phase_op_names(id, op_names);
                    tree.set_yaml_path(id, phase_path);
                }
            }
            ScenarioNode::Comprehension { comprehension, children } => {
                use nbrs_variates::comprehension::ComprehensionMode;
                let scope_idx = scope_tree.find_comprehension_scope(comprehension)
                    .ok_or_else(|| "comprehension: no scope-tree entry".to_string())?;
                let canonical = scope_tree.nodes[scope_idx].cached_kernel.get();
                let parent_kernel = effective_parent_kernel.cloned()
                    .or_else(|| scope_tree.nearest_installed_ancestor_kernel(scope_idx));
                let own_names: Vec<String> = canonical
                    .map(|k| k.program().own_output_names()
                        .into_iter().map(String::from).collect())
                    .unwrap_or_default();
                let filter = comprehension.filter.as_deref();

                match &comprehension.mode {
                    ComprehensionMode::Cartesian(clauses) if clauses.len() == 1 => {
                        let pairs: Vec<(String, String)> = clauses.iter()
                            .map(|c| (c.var.clone(), c.expr.clone())).collect();
                        let steps = match (canonical, parent_kernel.as_ref()) {
                            (Some(c), Some(p)) => premap_iterate(
                                c, p, parent_coords, &pairs, filter, None, None, strict,
                            )?,
                            _ => Vec::new(),
                        };
                        // Single-clause for_each: one path
                        // segment for the construct. All
                        // iteration scopes share this path.
                        let mut scope_path = parent_path.to_vec();
                        scope_path.push(PathSegment::ForEach { var: clauses[0].var.clone() });
                        let header = format!("for_each {} in {}",
                            clauses[0].var, clauses[0].expr);
                        if steps.is_empty() {
                            let scope = tree.push(parent, NodeKind::Scope, header, "");
                            tree.set_own_names(scope, own_names.clone());
                            tree.set_yaml_path(scope, scope_path.clone());
                            pre_map_recursive(
                                children, phases, scope_tree, scope, parent_coords,
                                effective_parent_kernel, &scope_path, tree, strict,
                            )?;
                        } else {
                            for step in &steps {
                                let scope = tree.push(parent, NodeKind::Scope,
                                    format!("for_each {}", iter_label(&step.bindings)), "");
                                tree.set_own_names(scope, own_names.clone());
                                tree.set_yaml_path(scope, scope_path.clone());
                                pre_map_recursive(
                                    children, phases, scope_tree, scope, &step.coord_path,
                                    Some(&step.bound_kernel), &scope_path, tree, strict,
                                )?;
                            }
                        }
                    }
                    ComprehensionMode::Cartesian(clauses) => {
                        let pairs: Vec<(String, String)> = clauses.iter()
                            .map(|c| (c.var.clone(), c.expr.clone())).collect();
                        let summary = clauses.iter().map(|c| c.var.as_str())
                            .collect::<Vec<_>>().join(", ");
                        let mut scope_path = parent_path.to_vec();
                        scope_path.push(PathSegment::ForCombinations {
                            vars: clauses.iter().map(|c| c.var.clone()).collect(),
                        });
                        let scope = tree.push(parent, NodeKind::Scope,
                            format!("for_combinations [{summary}]"), "");
                        tree.set_own_names(scope, own_names.clone());
                        tree.set_yaml_path(scope, scope_path.clone());
                        let steps = match (canonical, parent_kernel.as_ref()) {
                            (Some(c), Some(p)) => premap_iterate(
                                c, p, parent_coords, &pairs, filter, None, None, strict,
                            )?,
                            _ => Vec::new(),
                        };
                        for step in &steps {
                            let inner_scope = tree.push(scope, NodeKind::Scope,
                                iter_label(&step.bindings), "");
                            tree.set_own_names(inner_scope, own_names.clone());
                            tree.set_yaml_path(inner_scope, scope_path.clone());
                            pre_map_recursive(
                                children, phases, scope_tree, inner_scope, &step.coord_path,
                                Some(&step.bound_kernel), &scope_path, tree, strict,
                            )?;
                        }
                    }
                    ComprehensionMode::Union(subspaces) => {
                        let names = comprehension.coordinate_names().join(", ");
                        // Union: treat as ForCombinations with
                        // the merged variable list. Sub-spaces
                        // don't create distinct path levels —
                        // they're alternative materialisations
                        // of the same conceptual iteration.
                        let mut scope_path = parent_path.to_vec();
                        scope_path.push(PathSegment::ForCombinations {
                            vars: comprehension.coordinate_names()
                                .into_iter().map(String::from).collect(),
                        });
                        let scope = tree.push(parent, NodeKind::Scope,
                            format!("for_each_union [{names}] ({} sub-spaces)", subspaces.len()), "");
                        tree.set_own_names(scope, own_names.clone());
                        tree.set_yaml_path(scope, scope_path.clone());
                        let total = subspaces.len();
                        for (i, sub) in subspaces.iter().enumerate() {
                            let pairs: Vec<(String, String)> = sub.iter()
                                .map(|c| (c.var.clone(), c.expr.clone())).collect();
                            let steps = match (canonical, parent_kernel.as_ref()) {
                                (Some(c), Some(p)) => premap_iterate(
                                    c, p, parent_coords, &pairs, filter, None,
                                    Some((i, total)), strict,
                                )?,
                                _ => Vec::new(),
                            };
                            for step in &steps {
                                let inner_scope = tree.push(scope, NodeKind::Scope,
                                    iter_label(&step.bindings), "");
                                tree.set_own_names(inner_scope, own_names.clone());
                                tree.set_yaml_path(inner_scope, scope_path.clone());
                                pre_map_recursive(
                                    children, phases, scope_tree, inner_scope, &step.coord_path,
                                    Some(&step.bound_kernel), &scope_path, tree, strict,
                                )?;
                            }
                        }
                    }
                }
            }
            ScenarioNode::IncludedScenario { name, children } => {
                let mut scope_path = parent_path.to_vec();
                scope_path.push(PathSegment::ScenarioInclude(name.clone()));
                let scope = tree.push(parent, NodeKind::Scope, format!("scenario '{name}'"), "");
                tree.set_yaml_path(scope, scope_path.clone());
                pre_map_recursive(
                    children, phases, scope_tree, scope, parent_coords,
                    effective_parent_kernel, &scope_path, tree, strict,
                )?;
            }
            ScenarioNode::DoWhile { condition, counter, children } => {
                // Iteration count is unknown a priori (condition-
                // driven); show one scope header without
                // enumerating iterations.
                let mut scope_path = parent_path.to_vec();
                scope_path.push(PathSegment::DoWhile { counter: counter.clone() });
                let scope = tree.push(parent, NodeKind::Scope, format!("do_while {condition}"), "");
                tree.set_yaml_path(scope, scope_path.clone());
                if let Some(idx) = scope_tree.iter_dfs().find_map(|(i, n)| match &n.kind {
                    crate::scope_tree::ScopeKind::DoWhile { condition: c, counter: ct }
                        if c == condition && ct == counter => Some(i),
                    _ => None,
                }) {
                    if let Some(k) = scope_tree.nodes[idx].cached_kernel.get() {
                        let own: Vec<String> = k.program().own_output_names()
                            .into_iter().map(String::from).collect();
                        tree.set_own_names(scope, own);
                    }
                }
                pre_map_recursive(
                    children, phases, scope_tree, scope, parent_coords,
                    effective_parent_kernel, &scope_path, tree, strict,
                )?;
            }
            ScenarioNode::DoUntil { condition, counter, children } => {
                let mut scope_path = parent_path.to_vec();
                scope_path.push(PathSegment::DoUntil { counter: counter.clone() });
                let scope = tree.push(parent, NodeKind::Scope, format!("do_until {condition}"), "");
                tree.set_yaml_path(scope, scope_path.clone());
                if let Some(idx) = scope_tree.iter_dfs().find_map(|(i, n)| match &n.kind {
                    crate::scope_tree::ScopeKind::DoUntil { condition: c, counter: ct }
                        if c == condition && ct == counter => Some(i),
                    _ => None,
                }) {
                    if let Some(k) = scope_tree.nodes[idx].cached_kernel.get() {
                        let own: Vec<String> = k.program().own_output_names()
                            .into_iter().map(String::from).collect();
                        tree.set_own_names(scope, own);
                    }
                }
                pre_map_recursive(
                    children, phases, scope_tree, scope, parent_coords,
                    effective_parent_kernel, &scope_path, tree, strict,
                )?;
            }
        }
    }
    Ok(())
}

/// Build the canonical phase label from a root-first scope-coordinate
/// chain. Reverses to leaf-first and runs [`format_scope_coordinate_path`],
/// the GK-side formatter the executor uses on
/// `parent_kernel.scope_coordinates()` at runtime — so pre-map and
/// runtime produce identical strings for the same iteration position.
fn canonical_phase_label(parent_coords: &[ScopeCoord]) -> String {
    let leaf_first: Vec<_> = parent_coords.iter().rev().cloned().collect();
    format_scope_coordinate_path(&leaf_first)
}

/// Format a typed binding list as the `k=v, k=v` text that
/// scope-header rows show under the parent comprehension. Uses
/// `Value::to_display_string` so the rendering matches what every
/// other scope-coordinate consumer produces.
fn iter_label(bindings: &[(String, nbrs_variates::node::Value)]) -> String {
    bindings.iter()
        .map(|(k, v)| format!("{k}={}", v.to_display_string()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Pre-map's adapter over the GK-side
/// [`nbrs_variates::comprehension::iterate_scope`]: applies the
/// activity's strict-vs-warn empty-clause policy with diagnostic
/// emission suppressed (pre-map walks every workload at session
/// start, so a warn here would double-fire alongside the runtime's
/// own emission). Returns the materialised iteration list — pre-map
/// is a tree-build step, no concurrency or streaming needed.
#[allow(clippy::too_many_arguments)]
fn premap_iterate(
    canonical: &std::sync::Arc<GkKernel>,
    parent: &std::sync::Arc<GkKernel>,
    parent_coords: &[ScopeCoord],
    clauses: &[(String, String)],
    filter: Option<&str>,
    order: Option<&nbrs_variates::comprehension::TraversalOrder>,
    union_context: Option<(usize, usize)>,
    strict: bool,
) -> Result<Vec<nbrs_variates::comprehension::IterationStep>, String> {
    let on_empty = |var: &str, spec_text: &str| -> Result<(), String> {
        let context_label = match union_context {
            Some((i, n)) => format!(
                "for_each_union sub-space {}/{} clause '{var}'", i + 1, n,
            ),
            None => format!("for_each clause '{var}'"),
        };
        let msg = format!(
            "{context_label}: spec '{spec_text}' produced no values"
        );
        if strict { return Err(format!("strict: {msg}")); }
        Ok(())
    };
    let iter = nbrs_variates::comprehension::iterate_scope(
        canonical, parent, parent_coords, clauses, filter, order, &[], on_empty,
    )?;
    Ok(iter.collect())
}

// Per-iteration kernel construction lives on the GK side as
// `GkKernel::for_iteration`. Tuple drain + per-iteration binding
// is now `nbrs_variates::comprehension::iterate_scope`. Call sites
// here used to inline both, with two parallel implementations
// (one in pre-map, one in runtime); both now consume the same GK
// primitive.

/// Extract a human-readable message from a `catch_unwind` payload.
/// Used by the init-binding scope-activation check (SRD 11
/// §"Init Binding Contract" Plan B) to surface eval panics as
/// clean error messages rather than re-panicking the executor.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
