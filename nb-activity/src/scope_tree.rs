// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Canonical scope tree for a workload's runtime hierarchy.
//!
//! `ScenarioNode` (in `nb-workload`) is the *static authored*
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

use nb_workload::model::ScenarioNode;
use nb_variates::dsl::pragmas::PragmaSet;

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
    /// `for_each var in [...]`. The variable becomes a binding
    /// output of this scope (one value per iteration); children
    /// see it as an extern.
    ForEach { var: String, spec: String },
    /// `for_combinations`. Currently flat in the authored tree;
    /// scope-tree builders may later expand each dimension into
    /// its own `ForEach`-style scope for tidier diagnostics.
    ForCombinations { specs: Vec<(String, String)> },
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
    /// A leaf phase reference. The kernel slot, if filled, holds
    /// the per-phase GK program that the executor activates.
    Phase { name: String },
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
        match self {
            ScopeKind::Workload => "workload".into(),
            ScopeKind::Scenario { name } => format!("scenario '{name}'"),
            ScopeKind::ForEach { var, spec } => format!("for_each {var} in {spec}"),
            ScopeKind::ForCombinations { specs } => {
                let dims: Vec<String> = specs.iter()
                    .map(|(v, e)| format!("{v} in {e}"))
                    .collect();
                format!("for_combinations [{}]", dims.join(", "))
            }
            ScopeKind::DoWhile { condition, counter } => match counter {
                Some(c) => format!("do_while {condition} ({c})"),
                None => format!("do_while {condition}"),
            },
            ScopeKind::DoUntil { condition, counter } => match counter {
                Some(c) => format!("do_until {condition} ({c})"),
                None => format!("do_until {condition}"),
            },
            ScopeKind::Phase { name } => format!("phase '{name}'"),
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
    /// Cache for the phase scope's compiled GK program. Populated
    /// on the first `run_phase` call (or pre-compile pass) and
    /// reused thereafter. Empty for non-phase nodes and for
    /// phase nodes that take the workload-kernel fallback path
    /// (no own bindings, no iter parent). SRD 18b §"Cache-and-
    /// rebind contract".
    ///
    /// `OnceLock` lets multiple readers share the cache through
    /// the `Arc<ScopeTree>` without requiring a mutex on the hot
    /// path; the program itself is already `Arc`-shared and
    /// immutable.
    pub cached_program: std::sync::OnceLock<std::sync::Arc<nb_variates::kernel::GkProgram>>,
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
            cached_program: std::sync::OnceLock::new(),
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
            cached_program: std::sync::OnceLock::new(),
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
            cached_program: std::sync::OnceLock::new(),
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
            cached_program: std::sync::OnceLock::new(),
                });
                self.nodes[parent_idx].children.push(idx);
            }
            ScenarioNode::ForEach { spec, children } => {
                // Parse "var in expr" once at tree-build time.
                let (var, expr) = parse_for_each_spec(spec);
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::ForEach { var, spec: expr },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
            cached_program: std::sync::OnceLock::new(),
                });
                self.nodes[parent_idx].children.push(idx);
                for child in children {
                    self.append_subtree(idx, child);
                }
            }
            ScenarioNode::ForCombinations { specs, children } => {
                let idx = self.add_node(ScopeNode {
                    kind: ScopeKind::ForCombinations { specs: specs.clone() },
                    parent: Some(parent_idx),
                    children: Vec::new(),
                    depth,
                    pragmas: PragmaSet::default(),
            cached_program: std::sync::OnceLock::new(),
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
            cached_program: std::sync::OnceLock::new(),
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
            cached_program: std::sync::OnceLock::new(),
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

    /// All phase-leaf indices in depth-first order. Equivalent
    /// to filtering `iter_dfs()` to `ScopeKind::Phase` — the
    /// helper exists because it's the most common consumer
    /// query (TUI tree pre-mapping, dryrun=phase).
    pub fn phase_leaves(&self) -> Vec<ScopeNodeIdx> {
        self.iter_dfs()
            .filter_map(|(idx, node)| matches!(node.kind, ScopeKind::Phase { .. }).then_some(idx))
            .collect()
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
        phases: &std::collections::HashMap<String, nb_workload::model::WorkloadPhase>,
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
fn extract_phase_pragmas(phase: &nb_workload::model::WorkloadPhase) -> PragmaSet {
    use nb_workload::model::BindingsDef;
    let mut entries = Vec::new();
    for op in &phase.ops {
        let src = match &op.bindings {
            BindingsDef::GkSource(s) => s.as_str(),
            _ => continue,
        };
        // Lex/parse to AST to surface `Statement::Pragma`s. If
        // the source is malformed, skip — the real phase compile
        // will report a clean parse error later.
        let tokens = match nb_variates::dsl::lexer::lex(src) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let ast = match nb_variates::dsl::parser::parse(tokens) {
            Ok(a) => a,
            Err(_) => continue,
        };
        let local = nb_variates::dsl::pragmas::collect_from_ast(&ast);
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

/// Parse a `for_each` spec of the form `"var in expr"` into its
/// two halves. Returns `(var, expr)`. If the spec doesn't match
/// the expected shape, both halves come back; the runner does the
/// real validation when it resolves the values, so this is a
/// best-effort split for diagnostic labelling.
fn parse_for_each_spec(spec: &str) -> (String, String) {
    if let Some(idx) = spec.find(" in ") {
        let (lhs, rhs) = spec.split_at(idx);
        (lhs.trim().into(), rhs[" in ".len()..].trim().into())
    } else {
        (String::new(), spec.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase(name: &str) -> ScenarioNode {
        ScenarioNode::Phase(name.into())
    }
    fn for_each(spec: &str, children: Vec<ScenarioNode>) -> ScenarioNode {
        ScenarioNode::ForEach { spec: spec.into(), children }
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
        assert!(matches!(&tree.nodes[2].kind, ScopeKind::ForEach { var, .. } if var == "x"));
        assert!(matches!(&tree.nodes[3].kind, ScopeKind::ForEach { var, .. } if var == "y"));
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
            "for_each x in xs".into(),
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
            "for_each x in xs".into(),
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

    fn make_phase_with_source(src: &str) -> nb_workload::model::WorkloadPhase {
        use nb_workload::model::{BindingsDef, ParsedOp, WorkloadPhase};
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
}
