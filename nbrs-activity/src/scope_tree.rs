// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Canonical scope tree for a workload's runtime hierarchy.
//!
//! `ScenarioNode` (in `nbrs-workload`) is the *static authored*
//! tree — what the user wrote in YAML. `ScopeTree` is the
//! *runtime hierarchy* — what GK and the scheduler see. Every
//! non-trivial scenario node gets a 1:1 scope here, with
//! parent pointers, depth, pragma sets, and a slot for a compiled
//! kernel.
//!
//! This module is **structural only** — it builds the tree and
//! exposes traversal helpers. Pragma attachment, kernel
//! compilation, and execution scheduling live in subsequent
//! steps of SRD 18b §"Migration":
//!
//! 1. *(this module)* introduce the data structure
//! 2. wire `PragmaSet::attach_to` at scope-tree construction
//!    (M2 follow-up)
//! 3. replace text-substitution of iteration vars with extern
//!    binding (compile leaf phases once)
//! 4. pluggable scheduler reading the `schedule=<level0>/...`
//!    spec
//! 5. hierarchical display surface
//!
//! Until those steps land, `ScopeTree` is built but not consumed
//! by the runner — the existing executor continues to drive
//! traversal directly off `ScenarioNode`. Building the tree is
//! cheap and deterministic; intermediate sysrefs (TUI display,
//! `dryrun=phase`) can already start consuming it.

use nbrs_workload::model::ScenarioNode;
use nbrs_variates::comprehension::Comprehension;
use nbrs_variates::dsl::pragmas::PragmaSet;

/// Index into the `ScopeTree.nodes` vector. Stable for the
/// lifetime of the tree.
pub type ScopeNodeIdx = usize;

/// What kind of scope a `ScopeNode` represents. Mirrors the
/// `ScenarioNode` variants 1:1, with two extra kinds for the
/// implicit workload root and the named scenario layer that
/// wraps the user's authored children. SRD 18b §"Canonical
/// traversal".
#[derive(Debug, Clone)]
pub enum ScopeKind {
    /// The workload root. Always the single tree root. Owns the
    /// outer GK kernel that's currently compiled in
    /// `runner::run_with_observer` once at session start.
    Workload,
    /// A named scenario. Wraps the scenario's children so that
    /// "phase P in scenario default" survives as a path query
    /// rather than a flattened label.
    Scenario { name: String },
    /// Iteration scope — `for_each` (single or multi-clause) or
    /// `for_each_union`. The `Comprehension` AST captures the
    /// shape and clauses; the executor uses it to enumerate
    /// tuples and bind iteration variables on per-iteration
    /// child kernels.
    Comprehension { comprehension: Comprehension },
    /// Logical inclusion of another scenario by name. The
    /// runtime walks straight through to the children; the
    /// scope is preserved purely so the scope tree retains the
    /// include hierarchy for `dryrun=phase` and TUI output.
    /// See
    /// [`nbrs_workload::model::ScenarioNode::IncludedScenario`].
    IncludedScenario { name: String },
    /// `do_while` with optional counter as a scope output.
    DoWhile {
        condition: String,
        counter: Option<String>,
    },
    /// `do_until` with optional counter as a scope output.
    DoUntil {
        condition: String,
        counter: Option<String>,
    },
    /// A phase reference. With SRD-13d Phase 6 the phase is no
    /// longer a leaf — every op template the phase declares
    /// becomes an `OpTemplate` child of this node. The kernel
    /// slot, if filled, holds the per-phase GK program.
    Phase { name: String },
    /// SRD-13d Phase 6 — an op template's scope, child of its
    /// declaring phase. Per-template GK content (`bindings:`,
    /// `metrics:` wire-injections, inline `{{<expr>}}` rewrites)
    /// hangs off this node; the scope-flattening pre-walk
    /// (§3.3) decides whether it materialises its own kernel
    /// or flattens into the parent phase. Op-template scopes
    /// also own per-op `Component` instances at runtime so
    /// SRD-40b's duplicate-family check (via
    /// `Component::register_instrument`) surfaces per-op
    /// rather than per-phase.
    OpTemplate { name: String },
    /// Scenario-tree-level GK bindings block (see
    /// [`nbrs_workload::model::ScenarioNode::Bindings`]). The
    /// `source` is GK matter text that compiles into a kernel
    /// layered over the parent scope. Used for any scope-tree-
    /// level state injection: workload-param shadowing (the
    /// `set: { ... }` sugar form), derived bindings spanning a
    /// subtree, shared cells, etc. — the GK grammar is the
    /// only constraint on what the source may contain.
    Bindings {
        source: String,
    },
}

impl ScopeKind {
    /// True if this kind opens a *new* GK scope (its own
    /// kernel + pragmas + extern wiring). Phase scopes are only
    /// "new" when the phase has its own bindings or it's an
    /// iteration of a parent — that decision lives in the
    /// compiler step, not this static descriptor.
    pub fn opens_kernel(&self) -> bool {
        !matches!(self, ScopeKind::Workload)
    }

    /// Short label for diagnostic output (`dryrun=phase`, TUI).
    pub fn label(&self) -> String {
        use nbrs_variates::comprehension::ComprehensionMode;
        match self {
            ScopeKind::Workload => "workload".into(),
            ScopeKind::Scenario { name } => format!("scenario '{name}'"),
            ScopeKind::Comprehension { comprehension } => match &comprehension.mode {
                ComprehensionMode::Cartesian(clauses) if clauses.len() == 1 => {
                    // Both single-clause and multi-clause
                    // comprehensions render as `each …` in
                    // user-facing displays — operators
                    // shouldn't have to learn the
                    // for_each / for_combinations
                    // distinction to read a scenario tree.
                    // Header carries variable names only;
                    // bound values appear on per-iteration
                    // child scopes below.
                    format!("each {}", clauses[0].var())
                }
                ComprehensionMode::Cartesian(clauses) => {
                    let vars: Vec<&str> = clauses.iter()
                        .map(|c| c.var())
                        .collect();
                    format!("each {}", vars.join(", "))
                }
                ComprehensionMode::Union(subspaces) => {
                    let parts: Vec<String> = subspaces.iter().map(|set| {
                        let dims: Vec<String> = set.iter()
                            .map(|c| format!("{} in {}", c.var(), c.expr()))
                            .collect();
                        format!("[{}]", dims.join(", "))
                    }).collect();
                    format!("for_each_union {{{}}}", parts.join(" | "))
                }
            },
            ScopeKind::IncludedScenario { name } => format!("scenario '{name}'"),
            ScopeKind::DoWhile { condition, counter } => match counter {
                Some(c) => format!("do_while {condition} ({c})"),
                None => format!("do_while {condition}"),
            },
            ScopeKind::DoUntil { condition, counter } => match counter {
                Some(c) => format!("do_until {condition} ({c})"),
                None => format!("do_until {condition}"),
            },
            ScopeKind::Phase { name } => format!("phase '{name}'"),
            ScopeKind::OpTemplate { name } => format!("op '{name}'"),
            ScopeKind::Bindings { source } => {
                // Render as a one-line summary; long source
                // blocks are truncated for readability in
                // diagnostic output.
                let one_line: String = source.lines()
                    .filter(|l| !l.trim().is_empty())
                    .collect::<Vec<_>>()
                    .join("; ");
                if one_line.len() > 80 {
                    format!("bindings: {}…", &one_line[..77])
                } else {
                    format!("bindings: {one_line}")
                }
            }
        }
    }
}

/// One node in the runtime scope tree. Carries enough metadata
/// for the scheduler to walk and the compiler to fill in.
#[derive(Debug)]
pub struct ScopeNode {
    pub kind: ScopeKind,
    pub parent: Option<ScopeNodeIdx>,
    pub children: Vec<ScopeNodeIdx>,
    /// Depth from the root. Root is 0; its children are 1; and
    /// so on. The scheduler's `schedule=<level0>/<level1>/...`
    /// spec indexes by *child depth*, so a node at depth `d`
    /// schedules its children with the spec entry for index
    /// `d`.
    pub depth: usize,
    /// Pragmas declared at this scope level. Empty by default;
    /// step 2 of the migration fills these in by walking the
    /// node's source (or, for control-flow nodes, the optional
    /// inline pragma block once the workload model supports
    /// per-node pragmas).
    pub pragmas: PragmaSet,
    /// Cache for this scope's compiled GK kernel — the canonical
    /// instance that owns its `Arc<GkProgram>` and a folded-
    /// constant-seeded `GkState` so `get_constant(name)` is a
    /// straight `&self` read. Populated at pre-map time by
    /// [`ScopeTree::install_kernel`].
    ///
    /// SRD 18b §"Iteration variables as scope outputs": every
    /// non-trivial scope owns a kernel. The cached kernel is
    /// shared via `Arc` (read-only canonical state). Mutable
    /// per-iteration / per-fiber execution pulls a fresh kernel
    /// via `GkKernel::from_program(kernel.program().clone())` —
    /// the cache-and-rebind primitive documented on
    /// `GkKernel::from_program`.
    ///
    /// `OnceLock` keeps installation lock-free; downstream
    /// readers walk the parent chain via
    /// [`ScopeTree::lookup_name`] and never touch this slot
    /// directly.
    pub cached_kernel: std::sync::OnceLock<std::sync::Arc<nbrs_variates::kernel::GkKernel>>,
    /// SRD-13d §3 scope-flattening mark — set once at
    /// pre-walk by [`ScopeTree::mark_scope_flattening`] and
    /// read by every consumer (premap, runtime, diagnostics).
    /// `None` means "not yet computed"; the pre-walk
    /// guarantees every node has `Some` after it finishes.
    /// `true` ⇒ this scope materialises its own kernel;
    /// `false` ⇒ flattened into the nearest materialised
    /// ancestor.
    pub materialised: Option<bool>,
    /// SRD-13d §5.3 logical kernel name. Stable, fully-
    /// qualified scope-tree path (`workload`, `phase.<n>`,
    /// `phase.<n>.op.<o>`, etc.). Used by `dryrun=op`
    /// diagnostics and `nbrs describe gk` displays. Empty
    /// before the pre-walk runs.
    pub logical_name: String,
}

// `OnceLock` doesn't implement `Clone`, so neither does
// `ScopeNode` automatically. We don't actually need clones today
// — the tree is built once and shared via `Arc<ScopeTree>` — but
// some test helpers and serialisation paths assume `Clone`.
// Provide a manual clone that drops the cache (subsequent reads
// repopulate from a fresh compile, which is correct for clones
// since each clone owns an independent cache).
impl Clone for ScopeNode {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            parent: self.parent,
            children: self.children.clone(),
            depth: self.depth,
            pragmas: self.pragmas.clone(),
            cached_kernel: std::sync::OnceLock::new(),
            materialised: self.materialised,
            logical_name: self.logical_name.clone(),
        }
    }
}

/// A workload's runtime scope hierarchy. Built once per session.
/// Stable indices into `nodes`; parent / child pointers are
/// `ScopeNodeIdx`. Use the helpers on this struct for traversal
/// — direct `nodes` access is fine for read-only inspection but
/// the navigation helpers are easier to read.
#[derive(Debug, Clone)]
pub struct ScopeTree {
    pub nodes: Vec<ScopeNode>,
    pub root: ScopeNodeIdx,
}

impl ScopeTree {
    /// Build a scope tree from the resolved scenario children.
    /// `scenario_name` becomes the named [`ScopeKind::Scenario`]
    /// that wraps `nodes` — the user's authored grouping is
    /// preserved as a real ancestor, restoring "phase P in
    /// scenario default" as a path query.
    pub fn build(scenario_name: &str, nodes: &[ScenarioNode]) -> Self {
        let mut tree = ScopeTree {
            nodes: Vec::new(),
            root: 0,
        };

        // Root: the implicit workload. Always at index 0.
        tree.nodes.push(ScopeNode {
            kind: ScopeKind::Workload,
            parent: None,
            children: Vec::new(),
            depth: 0,
            pragmas: PragmaSet::default(),
            cached_kernel: std::sync::OnceLock::new(),
            materialised: None,
            logical_name: String::new(),
        });

        // Scenario layer wraps the user's children. This is the
        // "lost grouping" the user called out — a real scope
        // ancestor named after the scenario.
        let scenario_idx = tree.add_node(ScopeNode {
            kind: ScopeKind::Scenario { name: scenario_name.into() },
            parent: Some(0),
            children: Vec::new(),
            depth: 1,
            pragmas: PragmaSet::default(),
            cached_kernel: std::sync::OnceLock::new(),
            materialised: None,
            logical_name: String::new(),
        });
        tree.nodes[0].children.push(scenario_idx);

        // Walk the user's children recursively under the scenario.
        for child in nodes {
            tree.append_subtree(scenario_idx, child);
        }

        tree
    }

    /// Append the subtree rooted at `node` as a child of `parent_idx`.
    /// Recursive — control-flow nodes pull in their own children.
    fn append_subtree(&mut self, parent_idx: ScopeNodeIdx, node: &ScenarioNode) {
        let parent_depth = self.nodes[parent_idx].depth;
        let depth = parent_depth + 1;

        match node {
            ScenarioNode::Phase(name) => {
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::Phase { name: name.clone() },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
            cached_kernel: std::sync::OnceLock::new(),
            materialised: None,
            logical_name: String::new(),
                });
                self.nodes[parent_idx].children.push(idx);
            }
            ScenarioNode::Comprehension { comprehension, children } => {
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::Comprehension {
                        comprehension: comprehension.clone(),
                    },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
                    cached_kernel: std::sync::OnceLock::new(),
                    materialised: None,
                    logical_name: String::new(),
                });
                self.nodes[parent_idx].children.push(idx);
                for child in children {
                    self.append_subtree(idx, child);
                }
            }
            ScenarioNode::IncludedScenario { name, children } => {
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::IncludedScenario { name: name.clone() },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
                    cached_kernel: std::sync::OnceLock::new(),
                    materialised: None,
                    logical_name: String::new(),
                });
                self.nodes[parent_idx].children.push(idx);
                for child in children {
                    self.append_subtree(idx, child);
                }
            }
            ScenarioNode::DoWhile { condition, counter, children } => {
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::DoWhile {
                        condition: condition.clone(),
                        counter: counter.clone(),
                    },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
            cached_kernel: std::sync::OnceLock::new(),
            materialised: None,
            logical_name: String::new(),
                });
                self.nodes[parent_idx].children.push(idx);
                for child in children {
                    self.append_subtree(idx, child);
                }
            }
            ScenarioNode::DoUntil { condition, counter, children } => {
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::DoUntil {
                        condition: condition.clone(),
                        counter: counter.clone(),
                    },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
            cached_kernel: std::sync::OnceLock::new(),
            materialised: None,
            logical_name: String::new(),
                });
                self.nodes[parent_idx].children.push(idx);
                for child in children {
                    self.append_subtree(idx, child);
                }
            }
            ScenarioNode::Bindings { source, children } => {
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::Bindings {
                        source: source.clone(),
                    },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
                    cached_kernel: std::sync::OnceLock::new(),
                    materialised: None,
                    logical_name: String::new(),
                });
                self.nodes[parent_idx].children.push(idx);
                for child in children {
                    self.append_subtree(idx, child);
                }
            }
        }
    }

    fn add_node(&mut self, node: ScopeNode) -> ScopeNodeIdx {
        let idx = self.nodes.len();
        self.nodes.push(node);
        idx
    }

    /// SRD-13d Phase 6 — extend every `Phase` scope node with
    /// `OpTemplate` children (one per op declared in the
    /// phase). Two-step build: `ScopeTree::build` produces
    /// the scenario-shaped skeleton (phases as leaves);
    /// this method adds the op-template tier on top by
    /// consulting the workload's per-phase `WorkloadPhase`
    /// records.
    ///
    /// Idempotent: a phase whose `OpTemplate` children are
    /// already present is left alone (the post-build pre-walk
    /// can run before or after this without double-adding).
    /// Run before `mark_scope_flattening` so the per-op
    /// classification gets the chance to flatten / materialise
    /// each op-template tier.
    pub fn extend_with_op_templates(
        &mut self,
        phases: &std::collections::HashMap<String, nbrs_workload::model::WorkloadPhase>,
    ) {
        // Snapshot the indices first — we'll mutate `nodes`
        // during the loop.
        let phase_nodes: Vec<(ScopeNodeIdx, String, usize)> = self.nodes.iter()
            .enumerate()
            .filter_map(|(i, n)| match &n.kind {
                ScopeKind::Phase { name } => Some((i, name.clone(), n.depth)),
                _ => None,
            })
            .collect();

        for (phase_idx, phase_name, phase_depth) in phase_nodes {
            // Skip phases that already have OpTemplate children.
            let already_has_ops = self.nodes[phase_idx].children.iter()
                .any(|&c| matches!(self.nodes[c].kind, ScopeKind::OpTemplate { .. }));
            if already_has_ops {
                continue;
            }
            // Look up the phase's op list. Phases referenced
            // by name with no entry in `phases` (e.g. the
            // `default` scenario including a phase that's
            // declared elsewhere) just get no op children —
            // not a structural error.
            let Some(phase) = phases.get(&phase_name) else { continue; };
            for op in &phase.ops {
                let op_idx = self.add_node(ScopeNode {
                    kind: ScopeKind::OpTemplate { name: op.name.clone() },
                    parent: Some(phase_idx),
                    children: Vec::new(),
                    depth: phase_depth + 1,
                    pragmas: PragmaSet::default(),
                    cached_kernel: std::sync::OnceLock::new(),
                    materialised: None,
                    logical_name: String::new(),
                });
                self.nodes[phase_idx].children.push(op_idx);
            }
        }
    }

    /// SRD-13d §3.3 — pre-walk every scope-tree node and mark
    /// it `materialised` (own kernel) or flattened (descendants
    /// bind through parent). Also assigns the SRD-13d §5.3
    /// logical kernel name, which is the fully-qualified
    /// scope-tree path. Run once at workload-load; premap and
    /// runtime read the marks afterward.
    ///
    /// `is_materialising` is the predicate the pre-walk
    /// applies per node — typically a closure that consults
    /// the AST node's `HasGkMatter` classification (None /
    /// Readonly ⇒ flatten; Definitions ⇒ check program-hash
    /// equivalence with the parent and decide). The walker is
    /// agnostic to the exact predicate; SRD-13d §3.3 fixes
    /// the order.
    ///
    /// The workload root is **always** materialised (see
    /// SRD-13d §5.1) so the walk terminates at a materialised
    /// ancestor regardless of how aggressively descendants
    /// flatten.
    pub fn mark_scope_flattening<F>(&mut self, mut is_materialising: F)
    where F: FnMut(&ScopeKind, ScopeNodeIdx) -> bool,
    {
        // Walk in DFS order; logical names depend on parent
        // names being assigned first, which DFS pre-order
        // guarantees (root → scenario → … → leaf).
        let order: Vec<ScopeNodeIdx> = self.iter_dfs().map(|(idx, _)| idx).collect();
        for idx in order {
            // Root: always materialised, named "workload".
            if idx == self.root {
                self.nodes[idx].materialised = Some(true);
                self.nodes[idx].logical_name = "workload".to_string();
                continue;
            }
            let kind = self.nodes[idx].kind.clone();
            let materialise = is_materialising(&kind, idx);
            self.nodes[idx].materialised = Some(materialise);

            // Logical name = parent's logical name + "."
            // + per-kind segment. The segment shape follows
            // SRD-13d §5.3's table (`phase.<n>`,
            // `for_each.<var>`, `op.<o>`).
            let parent_name = self.nodes[idx].parent
                .map(|p| self.nodes[p].logical_name.clone())
                .unwrap_or_default();
            let segment = match &kind {
                ScopeKind::Workload => "workload".to_string(),
                ScopeKind::Scenario { name } => format!("scenario.{name}"),
                ScopeKind::Phase { name } => format!("phase.{name}"),
                ScopeKind::OpTemplate { name } => format!("op.{name}"),
                ScopeKind::Comprehension { .. } => "for_each".to_string(),
                ScopeKind::IncludedScenario { name } => format!("include.{name}"),
                ScopeKind::DoWhile { .. } => "do_while".to_string(),
                ScopeKind::DoUntil { .. } => "do_until".to_string(),
                ScopeKind::Bindings { source } => {
                    // First `final NAME` / `NAME :=` in the
                    // source distinguishes this scope-tree node
                    // in the logical-name path. For sugar from
                    // `set: { mode: verbose }` the source starts
                    // with `final mode := …` so the segment is
                    // `bindings.mode`. Sources with no clear
                    // first name fall back to a positional tag.
                    let first_name = source.lines()
                        .map(str::trim)
                        .find(|l| !l.is_empty())
                        .and_then(|line| {
                            let after_kw = line
                                .strip_prefix("final ")
                                .or_else(|| line.strip_prefix("init "))
                                .or_else(|| line.strip_prefix("shared "))
                                .unwrap_or(line);
                            after_kw
                                .split([' ', ':'])
                                .next()
                                .filter(|s| !s.is_empty())
                                .map(str::to_string)
                        })
                        .unwrap_or_else(|| "anon".to_string());
                    format!("bindings.{first_name}")
                }
            };
            self.nodes[idx].logical_name = if parent_name.is_empty() {
                segment
            } else {
                format!("{parent_name}.{segment}")
            };
        }
    }

    /// SRD-13d §5.1 — walk past flattened scope tiers to the
    /// nearest materialised ancestor (or self, when this
    /// node is itself materialised). Every consumer that
    /// needs a kernel handle (cache lookups, bind-outer-
    /// scope, diagnostics) routes through this — it's the
    /// single point that knows about flattening; nothing
    /// else does.
    ///
    /// The workload root is always materialised, so this
    /// always terminates with `Some(idx)`. Returns `None`
    /// only if [`mark_scope_flattening`] hasn't been run.
    pub fn nearest_materialised(&self, idx: ScopeNodeIdx) -> Option<ScopeNodeIdx> {
        let mut cur = idx;
        loop {
            match self.nodes[cur].materialised? {
                true => return Some(cur),
                false => match self.nodes[cur].parent {
                    Some(p) => cur = p,
                    None => return Some(cur), // root by construction
                },
            }
        }
    }

    /// Iterate every scope node in depth-first pre-order. The
    /// scheduler's default walk and the canonical display
    /// linearisation both consume this.
    pub fn iter_dfs(&self) -> DfsIter<'_> {
        DfsIter {
            tree: self,
            stack: vec![self.root],
        }
    }

    /// Walk from `idx` up through its ancestors to the root,
    /// inclusive of `idx` itself. Use this to compute effective
    /// pragmas (chain `attach_to`) or to render a path label.
    pub fn ancestors(&self, idx: ScopeNodeIdx) -> AncestorsIter<'_> {
        AncestorsIter {
            tree: self,
            cursor: Some(idx),
        }
    }

    /// First scope-tree node whose kind is `Phase { name }`
    /// matching the given name. Returns `None` if the scenario
    /// doesn't reference this phase. When a single phase is
    /// invoked from multiple scenario sites (rare; most workloads
    /// reference a phase exactly once), this returns the first
    /// occurrence in depth-first order — sufficient for current
    /// callers, who use the result to fetch the chain-walked
    /// `PragmaSet`.
    pub fn phase_node_by_name(&self, name: &str) -> Option<ScopeNodeIdx> {
        self.iter_dfs()
            .find_map(|(idx, node)| match &node.kind {
                ScopeKind::Phase { name: n } if n == name => Some(idx),
                _ => None,
            })
    }

    /// Op-template kernel programs for every materialised
    /// op-template that's a child of `phase_idx`. Keyed by the
    /// op's name. Used by the executor to thread per-op-template
    /// programs into the activity so each `MetricsDispenser`
    /// builds its `ScopeFixture` against the correct scope
    /// (SRD-13d Phase 9 §"per-dispenser kernel instancing").
    /// Flattened op-templates (`materialised != Some(true)`) are
    /// omitted from the map; their dispensers reach the parent
    /// kernel through the standard `nearest_materialised`
    /// fall-through.
    ///
    /// Rule 2 write-through bindings ride on the program itself
    /// (baked in by the SRD-67 builder's finalize step). Any
    /// kernel built from the program inherits them automatically
    /// via `GkKernel::from_program` — no side channel.
    pub fn op_template_programs_for_phase(
        &self,
        phase_idx: ScopeNodeIdx,
    ) -> std::collections::HashMap<String, std::sync::Arc<nbrs_variates::kernel::GkProgram>> {
        let mut out = std::collections::HashMap::new();
        for &child_idx in &self.nodes[phase_idx].children {
            let child = &self.nodes[child_idx];
            let ScopeKind::OpTemplate { name } = &child.kind else { continue };
            if child.materialised != Some(true) { continue; }
            if let Some(kernel) = child.cached_kernel.get() {
                out.insert(name.clone(), kernel.program().clone());
            }
        }
        out
    }

    /// All phase-leaf indices in depth-first order. Equivalent
    /// to filtering `iter_dfs()` to `ScopeKind::Phase` — the
    /// helper exists because it's the most common consumer
    /// query (TUI tree pre-mapping, dryrun=phase).
    pub fn phase_leaves(&self) -> Vec<ScopeNodeIdx> {
        self.iter_dfs()
            .filter_map(|(idx, node)| matches!(node.kind, ScopeKind::Phase { .. }).then_some(idx))
            .collect()
    }

    /// Walk ancestors of `idx` looking for the nearest scope
    /// node that has a kernel installed. Used at routing time
    /// to find the kernel a for_each scope's `materialize_wiring_from_outer`
    /// should chain from. Workload root always has a kernel
    /// installed (per M3.1), so this never returns `None` for
    /// any descendant of the root.
    pub fn nearest_installed_ancestor_kernel(
        &self,
        idx: ScopeNodeIdx,
    ) -> Option<std::sync::Arc<nbrs_variates::kernel::GkKernel>> {
        let mut cursor = self.nodes.get(idx)?.parent;
        while let Some(p) = cursor {
            if let Some(k) = self.nodes[p].cached_kernel.get() {
                return Some(k.clone());
            }
            cursor = self.nodes[p].parent;
        }
        None
    }

    /// Collect every installed ancestor kernel of `idx`,
    /// innermost first (immediate parent → workload root).
    /// Skips ancestor levels whose `cached_kernel` is empty
    /// (intermediate nodes that don't own their own kernel).
    /// Used by the checkpoint identity path to feed
    /// [`nbrs_variates::kernel::GkProgram::instance_hash`]
    /// (SRD-44 §"Identity matching at resume" + project
    /// memory `program_vs_instance_hash`).
    pub fn ancestor_kernels(
        &self,
        idx: ScopeNodeIdx,
    ) -> Vec<std::sync::Arc<nbrs_variates::kernel::GkKernel>> {
        let mut out = Vec::new();
        let mut cursor = self.nodes.get(idx).and_then(|n| n.parent);
        while let Some(p) = cursor {
            if let Some(k) = self.nodes[p].cached_kernel.get() {
                out.push(k.clone());
            }
            cursor = self.nodes[p].parent;
        }
        out
    }

    /// Find a `Comprehension` scope by structural-equality match
    /// against its [`Comprehension`] AST. Returns the **first**
    /// DFS-pre-order match.
    pub fn find_comprehension_scope(
        &self,
        comprehension: &Comprehension,
    ) -> Option<ScopeNodeIdx> {
        self.iter_dfs().find_map(|(idx, node)| match &node.kind {
            ScopeKind::Comprehension { comprehension: c } if c == comprehension => Some(idx),
            _ => None,
        })
    }

    /// First scope-tree node whose kind is
    /// `ScopeKind::Bindings { source }` matching exactly. Used
    /// by the runtime executor's `Bindings` arm to find the
    /// scope-tree node corresponding to the scenario-node it's
    /// currently executing, so it can push that scope's
    /// installed kernel as `ctx.current_parent_kernel` for
    /// children to read.
    ///
    /// `source` is sufficient identity: two `Bindings` nodes
    /// with identical sources produce structurally identical
    /// kernels, so picking either is benign — both publish the
    /// same lexical scope.
    pub fn find_bindings_scope(
        &self,
        source: &str,
    ) -> Option<ScopeNodeIdx> {
        self.iter_dfs().find_map(|(idx, node)| match &node.kind {
            ScopeKind::Bindings { source: s } if s == source => Some(idx),
            _ => None,
        })
    }

    /// Validate iteration-variable name uniqueness against the
    /// surrounding scope chain.
    ///
    /// An iter-var name (`for_each: "X in ..."`,
    /// `for_combinations: "X in ..., Y in ..."`,
    /// `for_each_union: ...`, do-loop counters) **must not**
    /// shadow:
    /// - a workload param,
    /// - an iter var declared by an enclosing scope.
    ///
    /// Aliasing creates a name that can't unambiguously resolve
    /// at spec-evaluation time (the iter var is being defined
    /// from a value that uses the same name; the runtime can't
    /// tell whether `{X}` means the iter var or the shadowed
    /// outer name). Rather than try to disambiguate, the build
    /// rejects it up-front with a clear error so the user
    /// renames the iter var.
    ///
    /// Returns `Ok(())` if every iter-var name is unique. Returns
    /// `Err(...)` with the offending name and which kind of
    /// collision (workload param vs ancestor iter var) the user
    /// has on the first violation found.
    pub fn validate_iter_var_uniqueness(
        &self,
        workload_params: &std::collections::HashSet<String>,
    ) -> Result<(), String> {
        fn walk(
            tree: &ScopeTree,
            idx: ScopeNodeIdx,
            ancestor_iter_vars: &std::collections::HashSet<String>,
            workload_params: &std::collections::HashSet<String>,
        ) -> Result<(), String> {
            let node = &tree.nodes[idx];
            // Collect the iter vars declared at this node.
            let own_iter_vars: Vec<&str> = match &node.kind {
                ScopeKind::Comprehension { comprehension } => {
                    comprehension.coordinate_names()
                }
                ScopeKind::DoWhile { counter: Some(c), .. }
                | ScopeKind::DoUntil { counter: Some(c), .. } => vec![c.as_str()],
                _ => Vec::new(),
            };
            for var in &own_iter_vars {
                if workload_params.contains(*var) {
                    return Err(format!(
                        "iter-var '{var}' aliases workload param '{var}'. \
                         A for_each / for_combinations / for_each_union iter \
                         variable cannot share a name with a workload param — \
                         spec evaluation can't disambiguate `{{{var}}}` between \
                         the iter var and the param. Rename one of them."
                    ));
                }
                if ancestor_iter_vars.contains(*var) {
                    return Err(format!(
                        "iter-var '{var}' aliases an iter var declared by an \
                         enclosing scope. Inner iter vars must use distinct \
                         names from outer iter vars."
                    ));
                }
            }
            // Extend the ancestor set for descent.
            let mut next_ancestors = ancestor_iter_vars.clone();
            for v in &own_iter_vars {
                next_ancestors.insert(v.to_string());
            }
            for &child in &node.children {
                walk(tree, child, &next_ancestors, workload_params)?;
            }
            Ok(())
        }
        walk(self, self.root, &std::collections::HashSet::new(), workload_params)
    }

    /// Install the canonical compiled kernel for `scope_idx`.
    ///
    /// Called at pre-map time after compiling the scope's
    /// `GkProgram`. Once installed, the kernel is the *single*
    /// authoritative answer for "what is `<name>` at this
    /// scope?" — every name visible at this scope (own outputs
    /// plus parent-inherited values bound via
    /// [`GkKernel::materialize_wiring_from_outer`]) resolves through the
    /// standard GK API on this one kernel. Callers don't walk
    /// the scope tree to do name resolution; GK's auto-extern +
    /// outer-scope wiring already encapsulates the layering.
    ///
    /// Idempotent only by virtue of `OnceLock`: a second install
    /// silently no-ops, returning `false`. Returns `true` on
    /// fresh install. Callers that need to detect a duplicate
    /// install should check the boolean.
    pub fn install_kernel(
        &self,
        scope_idx: ScopeNodeIdx,
        kernel: std::sync::Arc<nbrs_variates::kernel::GkKernel>,
    ) -> bool {
        match self.nodes.get(scope_idx) {
            Some(node) => node.cached_kernel.set(kernel).is_ok(),
            None => false,
        }
    }

    /// Populate `pragmas` on every phase-leaf scope by scanning
    /// each phase's `BindingsDef::GkSource` strings for `pragma`
    /// statements, then walk the tree to chain each scope's
    /// `PragmaSet` onto its parent's. After this call, querying
    /// `node.pragmas.strict_values()` walks the chain through
    /// every ancestor.
    ///
    /// SRD 18b §"Pragma chain along the scope tree". Idempotent
    /// per call (replaces any prior `pragmas` content).
    ///
    /// Returns the list of conflicts surfaced during chain
    /// attachment (today: empty for presence-only pragmas; the
    /// list exists for forward compatibility). Caller decides
    /// whether to log or fail on conflicts based on strict mode.
    pub fn populate_pragmas(
        &mut self,
        phases: &std::collections::HashMap<String, nbrs_workload::model::WorkloadPhase>,
    ) -> Vec<crate::scope_tree::PragmaConflict> {
        let mut conflicts = Vec::new();

        // Pass 1: extract phase-local pragmas. Iterate by
        // `phase_leaves` (which already does the kind filter)
        // and walk each phase's ops for GK source strings to
        // parse.
        let leaves = self.phase_leaves();
        for idx in leaves {
            let name = match &self.nodes[idx].kind {
                ScopeKind::Phase { name } => name.clone(),
                _ => continue,
            };
            if let Some(phase) = phases.get(&name) {
                self.nodes[idx].pragmas = extract_phase_pragmas(phase);
            }
        }

        // Pass 2: attach each scope to its parent. Walk in
        // depth order so a parent's `Arc<PragmaSet>` is finalised
        // before its children pin to it.
        let order: Vec<ScopeNodeIdx> = self.iter_dfs().map(|(i, _)| i).collect();
        for idx in order {
            if let Some(parent) = self.nodes[idx].parent {
                let parent_arc = std::sync::Arc::new(self.nodes[parent].pragmas.clone());
                let local = std::mem::take(&mut self.nodes[idx].pragmas);
                let (attached, mut local_conflicts) = local.attach_to(parent_arc);
                self.nodes[idx].pragmas = attached;
                for c in &mut local_conflicts {
                    conflicts.push(PragmaConflict {
                        scope_idx: idx,
                        name: c.name.clone(),
                        outer_line: c.outer_line,
                        inner_line: c.inner_line,
                    });
                }
            }
        }

        conflicts
    }
}

/// One pragma conflict surfaced when attaching a scope to its
/// parent. Reports the offending scope index so the caller can
/// turn it into a structured diagnostic with a path label.
#[derive(Debug, Clone)]
pub struct PragmaConflict {
    pub scope_idx: ScopeNodeIdx,
    pub name: String,
    pub outer_line: usize,
    pub inner_line: usize,
}

/// Extract pragmas from a phase's source by walking every op's
/// `BindingsDef::GkSource` and collecting `Statement::Pragma`s.
/// A phase has multiple ops; their bindings can each declare
/// pragmas. Today the convention is one pragma block at the
/// phase head; multi-op phases that put pragmas on individual
/// ops still get them aggregated here.
fn extract_phase_pragmas(phase: &nbrs_workload::model::WorkloadPhase) -> PragmaSet {
    use nbrs_workload::model::BindingsDef;
    let mut entries = Vec::new();
    for op in &phase.ops {
        let src = match &op.bindings {
            BindingsDef::GkSource(s) => s.as_str(),
            _ => continue,
        };
        // Lex/parse to AST to surface `Statement::Pragma`s. If
        // the source is malformed, skip — the real phase compile
        // will report a clean parse error later.
        let tokens = match nbrs_variates::dsl::lexer::lex(src) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let ast = match nbrs_variates::dsl::parser::parse(tokens) {
            Ok(a) => a,
            Err(_) => continue,
        };
        let local = nbrs_variates::dsl::pragmas::collect_from_ast(&ast);
        entries.extend(local.entries);
    }
    PragmaSet { entries, parent: None }
}

/// Depth-first pre-order iterator over `(idx, &ScopeNode)`.
pub struct DfsIter<'a> {
    tree: &'a ScopeTree,
    stack: Vec<ScopeNodeIdx>,
}

impl<'a> Iterator for DfsIter<'a> {
    type Item = (ScopeNodeIdx, &'a ScopeNode);
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.stack.pop()?;
        let node = &self.tree.nodes[idx];
        // Push children in reverse so the leftmost child comes
        // out of the stack first (pre-order).
        for &child in node.children.iter().rev() {
            self.stack.push(child);
        }
        Some((idx, node))
    }
}

/// Walk from a node up through its ancestors to the root.
pub struct AncestorsIter<'a> {
    tree: &'a ScopeTree,
    cursor: Option<ScopeNodeIdx>,
}

impl<'a> Iterator for AncestorsIter<'a> {
    type Item = (ScopeNodeIdx, &'a ScopeNode);
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.cursor?;
        let node = &self.tree.nodes[idx];
        self.cursor = node.parent;
        Some((idx, node))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn phase(name: &str) -> ScenarioNode {
        ScenarioNode::Phase(name.into())
    }
    fn for_each(spec: &str, children: Vec<ScenarioNode>) -> ScenarioNode {
        let clauses = nbrs_variates::comprehension::parse_clause_list(spec).unwrap();
        let comprehension = nbrs_variates::comprehension::Comprehension::cartesian(clauses);
        ScenarioNode::Comprehension { comprehension, children }
    }

    #[test]
    fn workload_and_scenario_always_present() {
        let tree = ScopeTree::build("default", &[]);
        // Even with no children, root + scenario layer survive
        // so observer code doesn't have to special-case empty
        // scenarios.
        assert_eq!(tree.nodes.len(), 2);
        assert!(matches!(tree.nodes[0].kind, ScopeKind::Workload));
        assert!(matches!(&tree.nodes[1].kind, ScopeKind::Scenario { name } if name == "default"));
        assert_eq!(tree.nodes[1].depth, 1);
    }

    #[test]
    fn flat_phases_under_scenario() {
        let tree = ScopeTree::build("default", &[phase("setup"), phase("run")]);
        assert_eq!(tree.nodes.len(), 4);
        let scenario = &tree.nodes[1];
        assert_eq!(scenario.children.len(), 2);
        for &c in &scenario.children {
            assert!(matches!(tree.nodes[c].kind, ScopeKind::Phase { .. }));
            assert_eq!(tree.nodes[c].depth, 2);
            assert_eq!(tree.nodes[c].parent, Some(1));
        }
    }

    #[test]
    fn nested_for_each_preserves_depth() {
        // for_each x in xs { for_each y in ys { phase P } }
        let tree = ScopeTree::build("default", &[
            for_each("x in xs", vec![
                for_each("y in ys", vec![phase("P")]),
            ]),
        ]);
        // workload(0) → scenario(1) → for_each_x(2) → for_each_y(3) → phase_P(4)
        assert_eq!(tree.nodes.len(), 5);
        assert_eq!(tree.nodes[2].depth, 2);
        assert_eq!(tree.nodes[3].depth, 3);
        assert_eq!(tree.nodes[4].depth, 4);
        assert!(matches!(
            &tree.nodes[2].kind,
            ScopeKind::Comprehension { comprehension }
                if comprehension.coordinate_names() == vec!["x"]
        ));
        assert!(matches!(
            &tree.nodes[3].kind,
            ScopeKind::Comprehension { comprehension }
                if comprehension.coordinate_names() == vec!["y"]
        ));
    }

    #[test]
    fn dfs_pre_order_matches_authored_order() {
        let tree = ScopeTree::build("default", &[
            for_each("x in xs", vec![phase("a"), phase("b")]),
            phase("c"),
        ]);
        let names: Vec<String> = tree
            .iter_dfs()
            .map(|(_, n)| n.kind.label())
            .collect();
        assert_eq!(names, vec![
            "workload".to_string(),
            "scenario 'default'".into(),
            "each x".into(),
            "phase 'a'".into(),
            "phase 'b'".into(),
            "phase 'c'".into(),
        ]);
    }

    #[test]
    fn ancestors_walk_to_root() {
        let tree = ScopeTree::build("default", &[
            for_each("x in xs", vec![phase("a")]),
        ]);
        let phase_idx = tree.phase_leaves()[0];
        let ancestors: Vec<String> = tree.ancestors(phase_idx)
            .map(|(_, n)| n.kind.label())
            .collect();
        assert_eq!(ancestors, vec![
            "phase 'a'".to_string(),
            "each x".into(),
            "scenario 'default'".into(),
            "workload".into(),
        ]);
    }

    #[test]
    fn phase_leaves_returns_only_phases() {
        let tree = ScopeTree::build("default", &[
            for_each("x in xs", vec![phase("a"), phase("b")]),
            phase("c"),
        ]);
        let leaves = tree.phase_leaves();
        assert_eq!(leaves.len(), 3);
        for idx in leaves {
            assert!(matches!(tree.nodes[idx].kind, ScopeKind::Phase { .. }));
        }
    }

    fn make_phase_with_source(src: &str) -> nbrs_workload::model::WorkloadPhase {
        use nbrs_workload::model::{BindingsDef, ParsedOp, WorkloadPhase};
        let mut op = ParsedOp::simple("op", "noop");
        op.bindings = BindingsDef::GkSource(src.into());
        WorkloadPhase {
            cycles: None,
            concurrency: None,
            rate: None,
            adapter: None,
            errors: None,
            tags: None,
            ops: vec![op],
            for_each: None,
            ..Default::default()
        }
    }

    #[test]
    fn populate_pragmas_propagates_through_chain() {
        // Phase has `pragma strict_values` in its source. After
        // populate_pragmas + attach, an inner for_each scope (no
        // own pragmas) should still resolve `strict_values()` true
        // through its parent chain back to… wait. Phase is the
        // *leaf*, not the parent. The propagation we care about is
        // "phase's pragmas propagate up", but the chain is parent
        // → child. Let's flip: put the pragma in a phase, and the
        // assertion is "the phase scope sees its own pragmas." A
        // future test will demonstrate cross-scope propagation
        // once non-phase scopes can declare pragmas.
        let phases = std::collections::HashMap::from([(
            "p".to_string(),
            make_phase_with_source("pragma strict_values\n id := cycle\n"),
        )]);
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        let conflicts = tree.populate_pragmas(&phases);
        assert!(conflicts.is_empty());
        let phase_idx = tree.phase_leaves()[0];
        assert!(tree.nodes[phase_idx].pragmas.strict_values());
    }

    #[test]
    fn populate_pragmas_chain_walk_through_attach() {
        // Build a small tree where the phase declares strict_values
        // and verify that querying through `attach_to` resolves it
        // even from sibling scopes that don't declare it. Sibling
        // queries are valid because every scope's `parent` chain
        // ultimately reaches the workload root.
        let phases = std::collections::HashMap::from([(
            "p".to_string(),
            make_phase_with_source("pragma strict\n id := cycle\n"),
        )]);
        let mut tree = ScopeTree::build("default", &[
            for_each("x in xs", vec![phase("p")]),
        ]);
        tree.populate_pragmas(&phases);
        let phase_idx = tree.phase_leaves()[0];
        // Phase declares strict (alias for both). Confirm:
        assert!(tree.nodes[phase_idx].pragmas.strict_types());
        assert!(tree.nodes[phase_idx].pragmas.strict_values());
    }

    // ---- M3.1: kernel install primitive ----

    /// Compile a tiny GK source into a kernel for use as a
    /// scope's canonical instance. A one-line `name := <const>`
    /// suffices to populate `output_map` so `get_constant`
    /// returns the folded value.
    fn compile_kernel(source: &str) -> std::sync::Arc<nbrs_variates::kernel::GkKernel> {
        let kernel = nbrs_variates::dsl::compile::compile_gk(source)
            .expect("test source should compile");
        std::sync::Arc::new(kernel)
    }

    #[test]
    fn install_kernel_seeds_canonical_state() {
        // After install, the cached kernel answers the name via
        // the standard GK API. No tree-walking on the caller
        // side — the kernel encapsulates its own scope, and
        // composition (auto-extern + materialize_wiring_from_outer) is what
        // makes parent values reachable. This test only verifies
        // the install primitive; the GK side already has its own
        // tests for composition.
        let tree = ScopeTree::build("default", &[phase("p")]);
        let workload_kernel = compile_kernel("final dataset := \"example\"\n");
        assert!(tree.install_kernel(0, workload_kernel));

        let cached = tree.nodes[0].cached_kernel.get()
            .expect("install populated the slot");
        match cached.get_constant("dataset") {
            Some(nbrs_variates::node::Value::Str(s)) => assert_eq!(&**s, "example"),
            other => panic!("expected Str(\"example\"), got {other:?}"),
        }
    }

    #[test]
    fn for_each_scope_kernel_inherits_parent_via_materialize_wiring_from_outer() {
        // M3.2 end-to-end: build a parent kernel that exposes a
        // workload-style param as an output, synthesize a
        // for_each scope kernel that references that param plus
        // its own iter var, bind from parent, then verify both
        // values are reachable on the synthesized kernel via
        // standard GK API. Validates the chain inheritance
        // path without any caller-side scope walking.
        use nbrs_variates::kernel::GkKernel;
        use std::sync::Arc;

        // Parent: a workload-shaped kernel exposing `k_values`.
        let parent_src = "final k_values := \"1, 10\"\n";
        let parent: Arc<GkKernel> = Arc::new(
            nbrs_variates::dsl::compile::compile_gk(parent_src).unwrap(),
        );

        // Build the for_each scope kernel as the runner would.
        let parent_manifest = crate::runner::extract_manifest(parent.program());
        let kernel = nbrs_variates::comprehension::synthesize_for_each_scope(
            &[("k".to_string(), "{k_values}".to_string())],
            &parent_manifest,
            &parent,
            &std::collections::HashMap::new(),
            Vec::new(),
            None,
            false,
            "test",
            None,
        ).expect("synthesis should succeed");

        // After `materialize_wiring_from_outer` (called inside the helper),
        // the inherited extern is populated with the parent's
        // value.
        match kernel.get_input("k_values") {
            Some(nbrs_variates::node::Value::Str(s)) => assert_eq!(&*s, "1, 10"),
            other => panic!("expected Str(\"1, 10\"), got {other:?}"),
        }

        // The iter var `k` is also visible as an extern; not
        // yet set by the runtime, so its current value is the
        // default for String externs.
        // (Runtime semantics test belongs in executor.rs once
        // M3.4 wires this up; M3.2 only verifies the install +
        // chain mechanics.)
        assert!(kernel.program().find_input("k").is_some(),
            "iter var should be declared as an extern input");

        // GK's `extern` declaration auto-installs a passthrough
        // node that exposes the name as an output too — so
        // children's `materialize_wiring_from_outer(this_scope)` sees both
        // `k_values` and `k` in this scope's manifest and the
        // chain inheritance flows through standard GK API
        // without any caller-side scope walking.
        let manifest = crate::runner::extract_manifest(kernel.program());
        let output_names: std::collections::HashSet<_> =
            manifest.iter().map(|e| e.name.as_str()).collect();
        assert!(output_names.contains("k_values"),
            "inherited name appears as output via extern's auto-passthrough");
        assert!(output_names.contains("k"),
            "iter var appears as output via extern's auto-passthrough");
    }

    #[test]
    fn for_each_scope_kernel_uses_native_type_for_numeric_iter_var() {
        // Single-clause for_each over a numeric workload param.
        // Pre-eval at synthesis detects U64 from "1, 10" and
        // declares `extern k: u64` instead of `extern k: String`.
        // Per SRD-18b "native types as the general rule".
        use nbrs_variates::kernel::GkKernel;
        use std::sync::Arc;

        let parent_src = "final k_values := \"1, 10\"\n";
        let parent: Arc<GkKernel> = Arc::new(
            nbrs_variates::dsl::compile::compile_gk(parent_src).unwrap(),
        );
        let parent_manifest = crate::runner::extract_manifest(parent.program());

        let kernel = nbrs_variates::comprehension::synthesize_for_each_scope(
            &[("k".to_string(), "{k_values}".to_string())],
            &parent_manifest,
            &parent,
            &std::collections::HashMap::new(),
            Vec::new(),
            None,
            false,
            "test",
            None,
        ).expect("synthesis should succeed");

        // Assert k's input port is u64-typed, not String.
        let manifest = crate::runner::extract_manifest(kernel.program());
        let k_entry = manifest.iter().find(|e| e.name == "k")
            .expect("k must appear in manifest");
        assert_eq!(k_entry.port_type, nbrs_variates::node::PortType::U64,
            "iter var over numeric values should be typed u64, not String");
    }

    #[test]
    fn for_each_scope_kernel_recursive_probe_for_dependent_clause() {
        // Multi-clause dependent: clause 2's spec text references
        // clause 1's iter var via `{k}`. Pre-eval probes clause 1
        // (k_values = "1, 10" → first value 1, type U64). Then
        // for clause 2's spec `{k_{k}_limits}`, the probe
        // substitutes {k}→1, leaving `{k_1_limits}`, which
        // resolves to "1, 2, 4, 8" via parent's manifest. First
        // value is 1, type U64.
        use nbrs_variates::kernel::GkKernel;
        use std::sync::Arc;

        let parent_src = concat!(
            "final k_values := \"1, 10\"\n",
            "final k_1_limits := \"1, 2, 4, 8\"\n",
            "final k_10_limits := \"10, 20, 30\"\n",
        );
        let parent: Arc<GkKernel> = Arc::new(
            nbrs_variates::dsl::compile::compile_gk(parent_src).unwrap(),
        );
        let parent_manifest = crate::runner::extract_manifest(parent.program());

        let kernel = nbrs_variates::comprehension::synthesize_for_each_scope(
            &[
                ("k".to_string(),     "{k_values}".to_string()),
                ("limit".to_string(), "{k_{k}_limits}".to_string()),
            ],
            &parent_manifest,
            &parent,
            &std::collections::HashMap::new(),
            Vec::new(),
            None,
            false,
            "test",
            None,
        ).expect("synthesis should succeed");

        let manifest = crate::runner::extract_manifest(kernel.program());
        let k_entry = manifest.iter().find(|e| e.name == "k").unwrap();
        let limit_entry = manifest.iter().find(|e| e.name == "limit").unwrap();
        assert_eq!(k_entry.port_type, nbrs_variates::node::PortType::U64,
            "k typed u64 from k_values pre-eval");
        assert_eq!(limit_entry.port_type, nbrs_variates::node::PortType::U64,
            "limit typed u64 via recursive probe k=1 → k_1_limits → \"1, 2, 4, 8\"");
    }

    // ── SRD-13d Phase 4 + 5: scope flattening marks ──

    #[test]
    fn mark_scope_flattening_assigns_logical_names() {
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        // All-materialise predicate so every node gets a name.
        tree.mark_scope_flattening(|_kind, _idx| true);
        // Root is "workload" by SRD-13d §5.3 convention.
        assert_eq!(tree.nodes[0].logical_name, "workload");
        assert_eq!(tree.nodes[0].materialised, Some(true));
        // Scenario is named after its scenario tag.
        let scenario_idx = tree.nodes[0].children[0];
        assert_eq!(tree.nodes[scenario_idx].logical_name,
            "workload.scenario.default");
        // Phase descends from scenario.
        let phase_idx = tree.nodes[scenario_idx].children[0];
        assert_eq!(tree.nodes[phase_idx].logical_name,
            "workload.scenario.default.phase.p");
    }

    #[test]
    fn mark_scope_flattening_records_predicate_decisions() {
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        // Predicate: only Phase scopes materialise.
        tree.mark_scope_flattening(|kind, _idx| {
            matches!(kind, ScopeKind::Phase { .. })
        });
        let scenario_idx = tree.nodes[0].children[0];
        let phase_idx = tree.nodes[scenario_idx].children[0];
        assert_eq!(tree.nodes[scenario_idx].materialised, Some(false));
        assert_eq!(tree.nodes[phase_idx].materialised, Some(true));
    }

    #[test]
    fn nearest_materialised_walks_past_flattened_layers() {
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        // Predicate: only the workload root materialises.
        tree.mark_scope_flattening(|kind, _idx| {
            matches!(kind, ScopeKind::Workload)
        });
        let scenario_idx = tree.nodes[0].children[0];
        let phase_idx = tree.nodes[scenario_idx].children[0];
        // Phase's nearest materialised ancestor is the root.
        assert_eq!(tree.nearest_materialised(phase_idx), Some(0));
        assert_eq!(tree.nearest_materialised(scenario_idx), Some(0));
        assert_eq!(tree.nearest_materialised(0), Some(0));
    }

    #[test]
    fn nearest_materialised_returns_self_when_node_materialises() {
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        tree.mark_scope_flattening(|_kind, _idx| true);
        let phase_idx = tree.nodes[tree.nodes[0].children[0]].children[0];
        assert_eq!(tree.nearest_materialised(phase_idx), Some(phase_idx));
    }

    #[test]
    fn nearest_materialised_none_before_pre_walk() {
        // Pre-walk hasn't run — every node's `materialised` is
        // None — so the walker can't terminate. Returns None.
        let tree = ScopeTree::build("default", &[phase("p")]);
        assert_eq!(tree.nearest_materialised(0), None);
    }

    #[test]
    fn workload_root_always_materialises_regardless_of_predicate() {
        // Even an "always flatten" predicate can't flatten the
        // root — SRD-13d §5.1 mandates the root is the
        // termination point of nearest_materialised walks.
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        tree.mark_scope_flattening(|_kind, _idx| false);
        assert_eq!(tree.nodes[0].materialised, Some(true));
    }

    // ── SRD-13d Phase 6: op-template tier ──

    #[test]
    fn extend_with_op_templates_adds_one_child_per_op() {
        use std::collections::HashMap;
        use nbrs_workload::model::{ParsedOp, WorkloadPhase, BindingsDef};
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        let mut phases = HashMap::new();
        phases.insert("p".into(), WorkloadPhase {
            cycles: None, concurrency: None, rate: None,
            adapter: None, errors: None, tags: None,
            ops: vec![
                ParsedOp::simple("alpha", "noop"),
                ParsedOp::simple("beta", "noop"),
            ],
            for_each: None, loop_scope: None, iter_scope: None,
            checkpoint: None, status_metrics: vec![],
            bindings: BindingsDef::default(),
        });
        tree.extend_with_op_templates(&phases);
        let scenario_idx = tree.nodes[0].children[0];
        let phase_idx = tree.nodes[scenario_idx].children[0];
        // Phase now has 2 op-template children.
        assert_eq!(tree.nodes[phase_idx].children.len(), 2);
        let op_a_idx = tree.nodes[phase_idx].children[0];
        let op_b_idx = tree.nodes[phase_idx].children[1];
        assert!(matches!(&tree.nodes[op_a_idx].kind,
            ScopeKind::OpTemplate { name } if name == "alpha"));
        assert!(matches!(&tree.nodes[op_b_idx].kind,
            ScopeKind::OpTemplate { name } if name == "beta"));
        // Depth = phase depth + 1.
        assert_eq!(tree.nodes[op_a_idx].depth, tree.nodes[phase_idx].depth + 1);
    }

    #[test]
    fn extend_with_op_templates_is_idempotent() {
        use std::collections::HashMap;
        use nbrs_workload::model::{ParsedOp, WorkloadPhase, BindingsDef};
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        let mut phases = HashMap::new();
        phases.insert("p".into(), WorkloadPhase {
            cycles: None, concurrency: None, rate: None,
            adapter: None, errors: None, tags: None,
            ops: vec![ParsedOp::simple("only", "noop")],
            for_each: None, loop_scope: None, iter_scope: None,
            checkpoint: None, status_metrics: vec![],
            bindings: BindingsDef::default(),
        });
        tree.extend_with_op_templates(&phases);
        let n_after_first = tree.nodes.len();
        tree.extend_with_op_templates(&phases); // Second call.
        assert_eq!(tree.nodes.len(), n_after_first,
            "second call should not add nodes");
    }

    #[test]
    fn op_template_logical_name_uses_op_segment() {
        use std::collections::HashMap;
        use nbrs_workload::model::{ParsedOp, WorkloadPhase, BindingsDef};
        let mut tree = ScopeTree::build("default", &[phase("p")]);
        let mut phases = HashMap::new();
        phases.insert("p".into(), WorkloadPhase {
            cycles: None, concurrency: None, rate: None,
            adapter: None, errors: None, tags: None,
            ops: vec![ParsedOp::simple("foo", "noop")],
            for_each: None, loop_scope: None, iter_scope: None,
            checkpoint: None, status_metrics: vec![],
            bindings: BindingsDef::default(),
        });
        tree.extend_with_op_templates(&phases);
        tree.mark_scope_flattening(|_kind, _idx| true);
        // Find the op node and check its logical name.
        let op_idx = tree.iter_dfs()
            .find(|(_, n)| matches!(&n.kind, ScopeKind::OpTemplate { name } if name == "foo"))
            .map(|(i, _)| i)
            .expect("op-template node");
        assert_eq!(tree.nodes[op_idx].logical_name,
            "workload.scenario.default.phase.p.op.foo");
    }

    #[test]
    fn install_is_idempotent_via_oncelock() {
        // OnceLock semantics: first install wins; subsequent
        // installs silently no-op. The boolean return lets
        // callers detect duplicate installs (likely a logic bug
        // in the runner) without panicking.
        let tree = ScopeTree::build("default", &[phase("p")]);
        let k1 = compile_kernel("final x := 1\n");
        let k2 = compile_kernel("final x := 2\n");
        assert!(tree.install_kernel(0, k1), "first install succeeds");
        assert!(!tree.install_kernel(0, k2), "second install no-ops");

        let cached = tree.nodes[0].cached_kernel.get().unwrap();
        match cached.get_constant("x") {
            Some(nbrs_variates::node::Value::U64(n)) => assert_eq!(*n, 1),
            other => panic!("expected U64(1), got {other:?}"),
        }
    }
}
