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
        });
        t
    }

    /// Index of the synthetic root.
    pub fn root(&self) -> SceneNodeId { 0 }

    /// Append a node under `parent` and return its id.
    pub fn push(
        &mut self,
        parent: SceneNodeId,
        kind: NodeKind,
        name: impl Into<String>,
        labels: impl Into<String>,
    ) -> SceneNodeId {
        let id = self.nodes.len();
        let depth = self.nodes[parent].depth + 1;
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
        });
        self.nodes[parent].children.push(id);
        id
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

    /// First phase node matching `(name, labels, status)`. Used by
    /// observer callbacks to bind a `phase_starting` event to the
    /// next pending phase, then `phase_completed` to its running
    /// counterpart, exactly as the previous flat-vec scan did.
    pub fn find_phase(
        &self,
        name: &str,
        labels: &str,
        want: Option<&PhaseStatus>,
    ) -> Option<SceneNodeId> {
        self.dfs_phases()
            .find(|n| {
                n.name == name
                    && n.labels == labels
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
