// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Scene tree — the runtime hierarchy as it's surfaced to renderers.
//!
//! Distinct from [`crate::scope_tree::ScopeTree`]: the scope tree
//! mirrors the static scenario AST 1:1 (one node per `ScenarioNode`),
//! while the *scene* tree is what's actually shown to the user —
//! `for_each` iterations are unrolled into per-iteration phase
//! children under a single `for_each` scope header, and any phases
//! that aren't reachable until runtime resolution still appear under
//! a fallback parent.
//!
//! Renderers (TUI, web API, post-run summary) walk this tree by
//! parent / children pointers rather than by depth tags, so:
//!
//! - Per-scope status aggregation (`for_each` is "running" if any
//!   child phase is running) becomes a tree walk.
//! - Web `GET /api/scope-tree` can serialize the structure directly.
//! - TUI features that want collapse / expand / scope-level summary
//!   have the structural information they need.
//!
//! Status carried here is the small lifecycle enum (`PhaseStatus`).
//! Renderers that want richer per-phase metrics (the TUI's
//! `PhaseSummary` with sparkline buffer, percentiles, etc.) keep a
//! parallel side-map keyed by [`SceneNodeId`] — the scene tree
//! stays cheap to clone and serialize.

use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock, RwLock};

/// Process-wide handle to the running session's scene tree.
///
/// Published once by the runner after `pre_map_tree` builds the
/// initial pending shape; lifecycle hooks (phase start / complete
/// / fail) mutate the same tree in place. Out-of-band consumers
/// (web API, post-run summary, future scripting hooks) read a
/// snapshot via [`current`] without depending on the observer
/// surface.
static GLOBAL_TREE: OnceLock<Arc<RwLock<SceneTree>>> = OnceLock::new();

/// Install the session's scene tree. The first installer wins —
/// matches the rest of the singleton-per-session pattern in this
/// crate (observer, log file, etc.).
pub fn install_global(tree: SceneTree) -> Arc<RwLock<SceneTree>> {
    GLOBAL_TREE
        .get_or_init(|| Arc::new(RwLock::new(tree)))
        .clone()
}

/// Snapshot the current global scene tree, if installed. Returns
/// `None` outside an active session — e.g. standalone `nbrs web`.
pub fn current() -> Option<SceneTree> {
    GLOBAL_TREE.get().and_then(|t| t.read().ok().map(|g| g.clone()))
}

/// Apply a mutation to the global tree, if installed. No-op when
/// no session has published one. Used by the runner's lifecycle
/// emit sites so the global tree mirrors the observer's view.
pub fn with_global_mut<F: FnOnce(&mut SceneTree)>(f: F) {
    if let Some(arc) = GLOBAL_TREE.get()
        && let Ok(mut g) = arc.write() {
            f(&mut g);
        }
}

/// Stable index into [`SceneTree::nodes`]. Indices never change for
/// a given tree instance; renderers can hold onto them across
/// status updates.
pub type SceneNodeId = usize;

/// What kind of node this scene entry represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    /// Synthetic root above all top-level scenario entries.
    /// Has no display analogue — its children are rendered as
    /// the scenario's top-level nodes.
    Root,
    /// An executable phase with a Pending → Running → Completed
    /// lifecycle.
    Phase,
    /// A grouping header (`for_each`, `for_combinations`,
    /// `do_while`, `do_until`, or a phase-level `for_each` lift).
    /// No own lifecycle — its aggregate status is computed from
    /// its descendants by [`SceneTree::aggregate_status`].
    Scope,
}

/// Phase lifecycle state. Only carries meaning on `Phase` nodes;
/// `Scope` nodes always start (and stay) `Pending`, with their
/// effective status derived from descendants.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PhaseStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
}

/// One node in the scene tree.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SceneNode {
    pub id: SceneNodeId,
    pub parent: Option<SceneNodeId>,
    pub children: Vec<SceneNodeId>,
    pub depth: usize,
    pub kind: NodeKind,
    /// For `Phase`: the phase name. For `Scope`: a description
    /// like `"for_each color=red"` or `"do_while empty"`.
    pub name: String,
    /// For `Phase`: dimensional labels (e.g. `"k=10, table=fknn"`).
    /// For `Scope`: empty (the description is in `name`).
    pub labels: String,
    pub status: PhaseStatus,
    pub op_count: usize,
    pub duration_secs: Option<f64>,
    /// For `Phase`: the ordered list of op template names in this
    /// phase's stanza (one entry per `ParsedOp`). Empty for
    /// `Scope` and `Root`. Populated at pre-map time so the TUI's
    /// scenario view can drill into a phase and show its ops
    /// without having to reach back into the workload model.
    #[serde(default)]
    pub op_names: Vec<String>,
    /// Names *defined* at this scope: own bindings, iter vars,
    /// and externs that the scope's specs / op templates
    /// reference. Excludes inherited cascade-propagation names
    /// (workload params auto-injected at intermediate scopes
    /// solely so descendants see them). Populated from the
    /// scope's installed kernel via
    /// `program.own_output_names()`. Empty for `Root`. Used by
    /// the TUI / dryrun renderer to show "what's defined here"
    /// without listing every name that's merely visible.
    #[serde(default)]
    pub own_names: Vec<String>,
    /// 1-based sequence number assigned to **Phase** nodes at
    /// pre-map time, in DFS order. `None` for `Scope` and
    /// `Root` entries.
    ///
    /// The TUI shows this as `[N/total]` next to the phase name
    /// and as `phase X/Y` in the header counter, so the
    /// operator can at any moment see which step of the planned
    /// scenario is in flight relative to the whole. The
    /// numbering is stable for the lifetime of one session
    /// (assigned once during pre-map), so a UI that displays
    /// "phase 47" on screen N and "phase 48" on screen N+1 is
    /// always referring to the same two phases — not a fresh
    /// re-numbering per draw.
    #[serde(default)]
    pub seq: Option<usize>,
    /// Fully-qualified structural location of this node in the
    /// workload YAML — outer-first chain of scenarios,
    /// for_each/for_combinations clauses, do-loops, and
    /// (terminal) the phase name itself. Populated for
    /// `Phase` nodes; ancestor `Scope` nodes carry partial
    /// paths (everything down to but not including the phase
    /// name).
    ///
    /// Used by the checkpoint resume planner — `yaml_path`
    /// plus the leaf-first coord-path string is the
    /// per-phase identity tuple that decides whether a saved
    /// checkpoint entry applies to a freshly-pre-mapped
    /// phase. See SRD-44 §"Phase identity".
    #[serde(default)]
    pub yaml_path: Vec<crate::checkpoint::PathSegment>,
}

/// The scene tree itself. `nodes[0]` is always the synthetic root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SceneTree {
    pub nodes: Vec<SceneNode>,
}

impl Default for SceneTree {
    fn default() -> Self { Self::new() }
}

impl SceneTree {
    /// Build an empty tree containing just the synthetic root.
    pub fn new() -> Self {
        let mut t = Self { nodes: Vec::new() };
        t.nodes.push(SceneNode {
            id: 0,
            parent: None,
            children: Vec::new(),
            depth: 0,
            kind: NodeKind::Root,
            name: String::new(),
            labels: String::new(),
            status: PhaseStatus::Pending,
            op_count: 0,
            duration_secs: None,
            op_names: Vec::new(),
            own_names: Vec::new(),
            seq: None,
            yaml_path: Vec::new(),
        });
        t
    }

    /// Index of the synthetic root.
    pub fn root(&self) -> SceneNodeId { 0 }

    /// Append a node under `parent` and return its id. `Phase`
    /// nodes are auto-assigned a 1-based sequence number in
    /// insertion order (see [`SceneNode::seq`]); since the
    /// pre-map walker pushes phases in DFS-of-the-scenario-tree
    /// order, the resulting numbers match the order in which the
    /// runtime will execute them.
    pub fn push(
        &mut self,
        parent: SceneNodeId,
        kind: NodeKind,
        name: impl Into<String>,
        labels: impl Into<String>,
    ) -> SceneNodeId {
        let id = self.nodes.len();
        let depth = self.nodes[parent].depth + 1;
        let seq = match kind {
            NodeKind::Phase => {
                let count = self.nodes.iter()
                    .filter(|n| n.kind == NodeKind::Phase)
                    .count();
                Some(count + 1)
            }
            _ => None,
        };
        self.nodes.push(SceneNode {
            id,
            parent: Some(parent),
            children: Vec::new(),
            depth,
            kind,
            name: name.into(),
            labels: labels.into(),
            status: PhaseStatus::Pending,
            op_count: 0,
            duration_secs: None,
            op_names: Vec::new(),
            own_names: Vec::new(),
            seq,
            yaml_path: Vec::new(),
        });
        self.nodes[parent].children.push(id);
        id
    }

    /// Set the structural YAML path for a node. Called by the
    /// pre-map walker as it descends through scenarios /
    /// for_each / for_combinations / do-loops, so each Scope
    /// and Phase node carries the full chain from the workload
    /// root down to its declaration site. Used by the
    /// checkpoint resume planner to identify phases across
    /// runs (per SRD-44 §"Phase identity").
    pub fn set_yaml_path(
        &mut self,
        id: SceneNodeId,
        path: Vec<crate::checkpoint::PathSegment>,
    ) {
        if id < self.nodes.len() {
            self.nodes[id].yaml_path = path;
        }
    }

    /// Total number of `Phase` entries in the tree. Equal to the
    /// largest assigned `seq` value once the tree is fully built.
    pub fn total_phases(&self) -> usize {
        self.nodes.iter().filter(|n| n.kind == NodeKind::Phase).count()
    }

    /// Set the op-template names for a phase node. Called at
    /// pre-map time once the workload model has been resolved so
    /// the TUI can drill into a phase and show its stanza
    /// elements.
    pub fn set_phase_op_names(&mut self, id: SceneNodeId, names: Vec<String>) {
        if id < self.nodes.len() {
            self.nodes[id].op_names = names;
        }
    }

    /// Set the scope-local "own names" — names defined at this
    /// scope vs. inherited via cascade. See
    /// [`SceneNode::own_names`]. Called at pre-map time from
    /// the scope kernel's `program.own_output_names()`.
    pub fn set_own_names(&mut self, id: SceneNodeId, names: Vec<String>) {
        if id < self.nodes.len() {
            self.nodes[id].own_names = names;
        }
    }

    /// DFS walk from the root, yielding every node in display
    /// order. The synthetic root itself is included as the first
    /// item; renderers filter on `kind == Root` to skip it.
    pub fn dfs(&self) -> DfsIter<'_> {
        DfsIter { tree: self, stack: vec![0] }
    }

    /// DFS yielding only `Phase`-kind nodes, in the same order
    /// the flat pre-map vector used to produce.
    pub fn dfs_phases(&self) -> impl Iterator<Item = &SceneNode> {
        self.dfs().filter(|n| n.kind == NodeKind::Phase)
    }

    /// First phase node matching `(name, status)`. Used by
    /// observer callbacks to bind a `phase_starting` event to the
    /// next pending phase, then `phase_completed` to its running
    /// counterpart.
    ///
    /// Matching is **structural-order**, not label-based: pre-map
    /// (`executor::pre_map_recursive`) and runtime
    /// (`executor::execute_node` → `dispatch_comprehension`) walk
    /// the scenario tree in the same DFS order, so the *i*-th
    /// runtime invocation of phase `name` always corresponds to
    /// the *i*-th pre-mapped phase node by `name`. That lets us
    /// avoid forcing pre-map's coordinate-path label string to
    /// match runtime's `format_scope_coordinate_path` output
    /// byte-for-byte — historically a fragile coupling that
    /// silently degraded to the "push under root" fallback when
    /// any workload-param vs. iter-var distinction shifted (e.g.
    /// `optimize_for_values` vs. `optimize_for`).
    ///
    /// `labels` was the legacy match key; preserved on the
    /// signature so callers don't have to change, but only used
    /// now if the order-based lookup misses (which shouldn't
    /// happen — surface as a warning if it does).
    pub fn find_phase(
        &self,
        name: &str,
        _labels: &str,
        want: Option<&PhaseStatus>,
    ) -> Option<SceneNodeId> {
        self.dfs_phases()
            .find(|n| {
                n.name == name
                    && want.is_none_or(|w| &n.status == w)
            })
            .map(|n| n.id)
    }

    /// Mark a phase as running. Looks up the first pending phase
    /// matching `(name, labels)` and transitions it.
    pub fn set_phase_running(&mut self, name: &str, labels: &str, op_count: usize) {
        if let Some(id) = self.find_phase(name, labels, Some(&PhaseStatus::Pending)) {
            let n = &mut self.nodes[id];
            n.status = PhaseStatus::Running;
            n.op_count = op_count;
        }
    }

    /// Mark a phase as completed. Matches the running phase with
    /// the given (name, labels).
    pub fn set_phase_completed(&mut self, name: &str, labels: &str, duration_secs: f64) {
        if let Some(id) = self.find_phase(name, labels, Some(&PhaseStatus::Running)) {
            let n = &mut self.nodes[id];
            n.status = PhaseStatus::Completed;
            n.duration_secs = Some(duration_secs);
        }
    }

    /// Mark a phase as failed. Matches the first phase with the
    /// given (name, labels) regardless of status — failure can
    /// arrive while the phase is still pending in the rare case
    /// of pre-flight resolution errors.
    pub fn set_phase_failed(&mut self, name: &str, labels: &str, error: &str) {
        if let Some(id) = self.find_phase(name, labels, None) {
            self.nodes[id].status = PhaseStatus::Failed(error.to_string());
        }
    }

    /// Effective status for a `Scope` (or `Root`) node, computed
    /// by walking descendants:
    /// - any descendant `Failed` → Failed
    /// - any descendant `Running` → Running
    /// - all descendant phases `Completed` → Completed
    /// - else → Pending
    pub fn aggregate_status(&self, id: SceneNodeId) -> PhaseStatus {
        let n = &self.nodes[id];
        if n.kind == NodeKind::Phase {
            return n.status.clone();
        }
        let mut seen_phase = false;
        let mut all_completed = true;
        let mut any_running = false;
        let mut first_failure: Option<String> = None;
        for &child in &n.children {
            let cs = self.aggregate_status(child);
            match cs {
                PhaseStatus::Failed(e) => {
                    if first_failure.is_none() { first_failure = Some(e); }
                    all_completed = false;
                }
                PhaseStatus::Running => {
                    any_running = true;
                    all_completed = false;
                }
                PhaseStatus::Pending => {
                    all_completed = false;
                }
                PhaseStatus::Completed => {}
            }
            if self.nodes[child].kind == NodeKind::Phase {
                seen_phase = true;
            } else if self.descendants_contain_phase(child) {
                seen_phase = true;
            }
        }
        if let Some(e) = first_failure { return PhaseStatus::Failed(e); }
        if any_running { return PhaseStatus::Running; }
        if seen_phase && all_completed { return PhaseStatus::Completed; }
        PhaseStatus::Pending
    }

    fn descendants_contain_phase(&self, id: SceneNodeId) -> bool {
        let n = &self.nodes[id];
        if n.kind == NodeKind::Phase { return true; }
        n.children.iter().any(|&c| self.descendants_contain_phase(c))
    }

    /// Total count of `Phase`-kind nodes in the tree.
    pub fn phase_count(&self) -> usize {
        self.dfs_phases().count()
    }
}

/// Indent prefix (`"  "` repeats) for log lines whose visual
/// nesting should match the currently-running phase's scope
/// depth. Looks up the global scene tree, finds the first
/// `Running` phase in DFS order, returns `"  ".repeat(depth)`
/// for it. Empty string when no scene tree is installed or no
/// phase is currently running.
///
/// Used by emit sites that fire from inside a phase's
/// execution (polling-op progress, activity-end DONE summary,
/// relevancy stats) so they nest under the phase's startup line
/// in tui=terminal output.
///
/// Note: with concurrent phases the "first Running in DFS
/// order" picks one; for poll / verify ops within a single
/// phase's stanza this is unambiguous, but a workload running
/// peer phases concurrently may see a poll-progress line
/// indented to the wrong sibling. Acceptable approximation
/// for v1 — a richer "carry phase context through ExecCtx"
/// design would let each emit know exactly which phase it
/// belongs to.
pub fn running_phase_indent() -> String {
    let Some(tree) = current() else { return String::new(); };
    tree.dfs_phases()
        .find(|n| matches!(n.status, PhaseStatus::Running))
        .map(|n| "  ".repeat(n.depth.saturating_sub(1)))
        .unwrap_or_default()
}

/// Depth-first iterator over a [`SceneTree`].
pub struct DfsIter<'a> {
    tree: &'a SceneTree,
    stack: Vec<SceneNodeId>,
}

impl<'a> Iterator for DfsIter<'a> {
    type Item = &'a SceneNode;

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.stack.pop()?;
        let node = &self.tree.nodes[id];
        for &c in node.children.iter().rev() {
            self.stack.push(c);
        }
        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_simple() -> SceneTree {
        let mut t = SceneTree::new();
        let s = t.push(t.root(), NodeKind::Scope, "for_each x=1", "");
        let _ = t.push(s, NodeKind::Phase, "p", "x=1");
        let _ = t.push(s, NodeKind::Phase, "q", "x=1");
        let s2 = t.push(t.root(), NodeKind::Scope, "for_each x=2", "");
        let _ = t.push(s2, NodeKind::Phase, "p", "x=2");
        let _ = t.push(s2, NodeKind::Phase, "q", "x=2");
        t
    }

    #[test]
    fn dfs_yields_all_in_display_order() {
        let t = build_simple();
        let names: Vec<&str> = t.dfs().map(|n| n.name.as_str()).collect();
        assert_eq!(names, vec!["", "for_each x=1", "p", "q", "for_each x=2", "p", "q"]);
    }

    #[test]
    fn dfs_phases_skips_root_and_scopes() {
        let t = build_simple();
        let names: Vec<&str> = t.dfs_phases().map(|n| n.name.as_str()).collect();
        assert_eq!(names, vec!["p", "q", "p", "q"]);
    }

    #[test]
    fn find_pending_then_running_progresses_through_iterations() {
        let mut t = build_simple();
        // First (p, x=1) Pending → Running → Completed.
        t.set_phase_running("p", "x=1", 3);
        let n = t.find_phase("p", "x=1", Some(&PhaseStatus::Running)).unwrap();
        assert_eq!(t.nodes[n].op_count, 3);
        t.set_phase_completed("p", "x=1", 0.5);
        // The next pending (p, x=2) is now matchable.
        t.set_phase_running("p", "x=2", 5);
        let n2 = t.find_phase("p", "x=2", Some(&PhaseStatus::Running)).unwrap();
        assert_ne!(n, n2);
        assert_eq!(t.nodes[n2].op_count, 5);
    }

    #[test]
    fn aggregate_status_walks_descendants() {
        let mut t = build_simple();
        // No phases moved yet — aggregate is Pending.
        assert_eq!(t.aggregate_status(t.root()), PhaseStatus::Pending);
        // Mark every phase Completed → root aggregates to Completed.
        for (name, labels) in [("p", "x=1"), ("q", "x=1"), ("p", "x=2"), ("q", "x=2")] {
            t.set_phase_running(name, labels, 1);
            t.set_phase_completed(name, labels, 0.1);
        }
        assert_eq!(t.aggregate_status(t.root()), PhaseStatus::Completed);
    }

    #[test]
    fn aggregate_propagates_failure() {
        let mut t = build_simple();
        t.set_phase_running("p", "x=1", 1);
        t.set_phase_failed("p", "x=1", "boom");
        let s = t.aggregate_status(t.root());
        assert!(matches!(s, PhaseStatus::Failed(ref e) if e == "boom"), "got {s:?}");
    }

    #[test]
    fn aggregate_running_when_any_running() {
        let mut t = build_simple();
        t.set_phase_running("p", "x=1", 1);
        assert_eq!(t.aggregate_status(t.root()), PhaseStatus::Running);
    }
}
