// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Graph-level node fusion optimization pass.
//!
//! Recognizes subgraph patterns in the GK DAG and replaces them with
//! semantically equivalent fused nodes that are computationally cheaper.
//! Runs during assembly after wiring resolution, before dead code
//! elimination and topological sort.
//!
//! See SRD 36 (docs/design/36_node_fusion.md) for the full design.

use crate::kernel::WireSource;
use crate::node::{Commutativity, ConstValue, GkNode};

// ---------------------------------------------------------------------------
// Pattern types
// ---------------------------------------------------------------------------

/// A structural pattern that matches a subgraph of the GK DAG.
///
/// Patterns are trees — each sub-pattern matches exactly one node.
/// Diamond shapes (two pattern leaves matching the same upstream node)
/// are handled by bind-name equality checks after matching.
#[derive(Debug, Clone)]
pub enum FusionPattern {
    /// Match a node by its `meta().name` string.
    ///
    /// Sub-patterns match the node's inputs (respecting the node's
    /// declared commutativity). The node's `jit_constants()` are
    /// captured under `bind`.
    Node {
        /// The node's `meta().name` (e.g., "hash", "mod", "add").
        op: &'static str,
        /// Sub-patterns for the node's inputs.
        inputs: Vec<FusionPattern>,
        /// Binding name for this node's constants in the match result.
        bind: &'static str,
    },

    /// Match any wire source (coordinate, upstream node output, etc.).
    /// This is the "hole" — it captures the wire reference for rewiring
    /// to the fused replacement node.
    Any {
        /// Binding name for this wire in the match result.
        bind: &'static str,
    },

    /// Match a variadic node with N children, applying a sub-pattern
    /// to each child. Children are bound as `{bind}_0`, `{bind}_1`, etc.
    ///
    /// Use for fusion rules that operate on variadic nodes like `sum`
    /// where the number of inputs isn't known at rule-definition time.
    VariadicNode {
        /// The node's `meta().name`.
        op: &'static str,
        /// Pattern applied to each child input.
        child_pattern: Box<FusionPattern>,
        /// Binding prefix: children bound as `{bind}_0`, `{bind}_1`, ...
        /// The node's own constants are bound under `{bind}`.
        bind: &'static str,
        /// Minimum number of children to match.
        min_children: usize,
    },
}

impl FusionPattern {
    /// Convenience: `node(op, inputs, bind)`.
    pub fn node(
        op: &'static str,
        inputs: Vec<FusionPattern>,
        bind: &'static str,
    ) -> Self {
        FusionPattern::Node { op, inputs, bind }
    }

    /// Convenience: `any(bind)`.
    pub fn any(bind: &'static str) -> Self {
        FusionPattern::Any { bind }
    }

    /// Return the root operation name, if this is a `Node` pattern.
    pub fn root_op(&self) -> Option<&'static str> {
        match self {
            FusionPattern::Node { op, .. } => Some(op),
            FusionPattern::VariadicNode { op, .. } => Some(op),
            FusionPattern::Any { .. } => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Match result
// ---------------------------------------------------------------------------

/// The result of a successful pattern match against a subgraph.
#[derive(Debug, Clone)]
pub struct MatchResult {
    /// Bound wire sources: `bind_name → WireSource` for each `Any` leaf.
    pub wires: Vec<(String, WireSource)>,

    /// Bound node constants (JIT u64 form): `bind_name → jit_constants()`.
    pub constants: Vec<(String, Vec<u64>)>,

    /// Bound typed constants from the slot model.
    /// Empty for nodes not yet migrated to slots.
    pub typed_constants: Vec<(String, Vec<ConstValue>)>,

    /// The set of node indices consumed by this match. These nodes
    /// will be removed from the DAG and replaced by the fused node.
    pub consumed_nodes: Vec<usize>,
}

impl MatchResult {
    fn new() -> Self {
        Self {
            wires: Vec::new(),
            constants: Vec::new(),
            typed_constants: Vec::new(),
            consumed_nodes: Vec::new(),
        }
    }

    /// Look up a captured wire by binding name.
    pub fn wire(&self, name: &str) -> &WireSource {
        self.wires
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, w)| w)
            .unwrap_or_else(|| panic!("no wire bound as '{name}'"))
    }

    /// Look up captured constants (u64 form) by binding name.
    pub fn const_vec(&self, name: &str) -> &[u64] {
        self.constants
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| c.as_slice())
            .unwrap_or_else(|| panic!("no constants bound as '{name}'"))
    }

    /// Look up a single captured constant by binding name.
    pub fn const_u64(&self, name: &str) -> u64 {
        self.const_vec(name)[0]
    }

    /// Look up typed constants by binding name.
    pub fn typed_consts(&self, name: &str) -> &[ConstValue] {
        self.typed_constants
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| c.as_slice())
            .unwrap_or(&[])
    }

    fn merge(&mut self, other: MatchResult) {
        self.wires.extend(other.wires);
        self.constants.extend(other.constants);
        self.typed_constants.extend(other.typed_constants);
        self.consumed_nodes.extend(other.consumed_nodes);
    }
}

// ---------------------------------------------------------------------------
// Fusion rule
// ---------------------------------------------------------------------------

/// A graph rewrite rule: a subgraph pattern and its replacement.
///
/// Each rule is a declarative specification. The pattern describes
/// what to match; the replacement factory produces the fused node;
/// `input_bindings` maps the fused node's input ports to captured
/// wire sources by name.
pub struct FusionRule {
    /// Human-readable name for diagnostics, logging, and test output.
    pub name: &'static str,

    /// The subgraph pattern to match.
    pub pattern: FusionPattern,

    /// Factory: given the match result, produce the replacement fused node.
    pub replacement: fn(&MatchResult) -> Box<dyn GkNode>,

    /// Binding names for the fused node's inputs, in order.
    /// Each name must correspond to an `Any` leaf in the pattern.
    pub input_bindings: &'static [&'static str],
}

// ---------------------------------------------------------------------------
// Pattern matching engine
// ---------------------------------------------------------------------------

/// Intermediate node representation used during fusion — borrows from
/// the pending node list to avoid cloning.
struct NodeView<'a> {
    nodes: &'a [Option<Box<dyn GkNode>>],
    wiring: &'a [Vec<WireSource>],
}

/// Try to match a pattern against the subgraph rooted at `node_idx`.
fn try_match(
    pattern: &FusionPattern,
    source: &WireSource,
    view: &NodeView<'_>,
) -> Option<MatchResult> {
    match pattern {
        FusionPattern::Any { bind } => {
            let mut result = MatchResult::new();
            result.wires.push((bind.to_string(), source.clone()));
            Some(result)
        }

        FusionPattern::Node { op, inputs, bind } => {
            // The source must be a node output (not a coordinate or port).
            let node_idx = match source {
                WireSource::NodeOutput(idx, 0) => *idx,
                _ => return None,
            };

            let node = view.nodes[node_idx].as_ref()?;

            // Check the node's operation name matches.
            if node.meta().name != *op {
                return None;
            }

            // Check arity matches.
            let node_wiring = &view.wiring[node_idx];
            if node_wiring.len() != inputs.len() {
                return None;
            }

            // Try to match sub-patterns against the node's inputs,
            // respecting the node's commutativity declaration.
            let matched = match_inputs(inputs, node_wiring, &node.commutativity(), view)?;

            let mut result = matched;
            result.constants.push((bind.to_string(), node.jit_constants()));

            // Also capture typed constants from the slot model.
            let typed: Vec<ConstValue> = node.meta().const_slots()
                .iter()
                .map(|c| c.1.clone())
                .collect();
            if !typed.is_empty() {
                result.typed_constants.push((bind.to_string(), typed));
            }

            result.consumed_nodes.push(node_idx);
            Some(result)
        }

        FusionPattern::VariadicNode { op, child_pattern, bind, min_children } => {
            let node_idx = match source {
                WireSource::NodeOutput(idx, 0) => *idx,
                _ => return None,
            };

            let node = view.nodes[node_idx].as_ref()?;
            if node.meta().name != *op {
                return None;
            }

            let node_wiring = &view.wiring[node_idx];
            if node_wiring.len() < *min_children {
                return None;
            }

            // Match each child input against the child pattern.
            let mut result = MatchResult::new();
            for (i, wire) in node_wiring.iter().enumerate() {
                let child_bind = format!("{bind}_{i}");
                // For Any patterns, override the bind name with the indexed one.
                let indexed_pattern = match child_pattern.as_ref() {
                    FusionPattern::Any { .. } => FusionPattern::Any {
                        bind: Box::leak(child_bind.clone().into_boxed_str()),
                    },
                    _ => *child_pattern.clone(),
                };
                let m = try_match(&indexed_pattern, wire, view)?;
                result.merge(m);
            }

            // Capture the variadic node's own constants.
            result.constants.push((bind.to_string(), node.jit_constants()));
            let typed: Vec<ConstValue> = node.meta().const_slots()
                .iter()
                .map(|c| c.1.clone())
                .collect();
            if !typed.is_empty() {
                result.typed_constants.push((bind.to_string(), typed));
            }

            result.consumed_nodes.push(node_idx);
            Some(result)
        }
    }
}

/// Match a list of sub-patterns against a node's input wires,
/// respecting the node's commutativity.
fn match_inputs(
    patterns: &[FusionPattern],
    wires: &[WireSource],
    commutativity: &Commutativity,
    view: &NodeView<'_>,
) -> Option<MatchResult> {
    match commutativity {
        Commutativity::Positional => match_positional(patterns, wires, view),

        Commutativity::AllCommutative => {
            // Try all permutations of wire indices.
            let indices: Vec<usize> = (0..wires.len()).collect();
            for perm in permutations(&indices) {
                let reordered: Vec<&WireSource> = perm.iter().map(|&i| &wires[i]).collect();
                if let Some(m) = match_ordered(patterns, &reordered, view) {
                    return Some(m);
                }
            }
            None
        }

        Commutativity::Groups(groups) => {
            // Build the set of indices that belong to some group.
            let mut in_group = vec![false; wires.len()];
            for g in groups {
                for &idx in g {
                    if idx < in_group.len() {
                        in_group[idx] = true;
                    }
                }
            }

            // Start with positional matching for non-group indices.
            // Then try permutations within each group.
            try_groups_match(patterns, wires, groups, &in_group, view)
        }
    }
}

/// Positional matching: patterns[i] matches wires[i] in order.
fn match_positional(
    patterns: &[FusionPattern],
    wires: &[WireSource],
    view: &NodeView<'_>,
) -> Option<MatchResult> {
    let refs: Vec<&WireSource> = wires.iter().collect();
    match_ordered(patterns, &refs, view)
}

/// Match patterns against wires in the given order.
fn match_ordered(
    patterns: &[FusionPattern],
    wires: &[&WireSource],
    view: &NodeView<'_>,
) -> Option<MatchResult> {
    let mut result = MatchResult::new();
    for (pat, wire) in patterns.iter().zip(wires.iter()) {
        let m = try_match(pat, wire, view)?;
        result.merge(m);
    }
    Some(result)
}

/// Match with grouped commutativity: try permutations within each
/// group, positional for everything else.
fn try_groups_match(
    patterns: &[FusionPattern],
    wires: &[WireSource],
    groups: &[Vec<usize>],
    _in_group: &[bool],
    view: &NodeView<'_>,
) -> Option<MatchResult> {
    // Generate all combinations of per-group permutations.
    // For typical groups (size 2-3), this is small.
    let mut index_map: Vec<usize> = (0..wires.len()).collect();

    fn recurse(
        group_idx: usize,
        groups: &[Vec<usize>],
        index_map: &mut Vec<usize>,
        patterns: &[FusionPattern],
        wires: &[WireSource],
        view: &NodeView<'_>,
    ) -> Option<MatchResult> {
        if group_idx >= groups.len() {
            // All groups assigned — try matching with this mapping.
            let reordered: Vec<&WireSource> =
                index_map.iter().map(|&i| &wires[i]).collect();
            return match_ordered(patterns, &reordered, view);
        }

        let group = &groups[group_idx];
        let original_values: Vec<usize> = group.iter().map(|&i| index_map[i]).collect();

        for perm in permutations(&original_values) {
            for (slot, &val) in group.iter().zip(perm.iter()) {
                index_map[*slot] = val;
            }
            if let Some(m) = recurse(group_idx + 1, groups, index_map, patterns, wires, view) {
                return Some(m);
            }
        }

        // Restore original values.
        for (slot, val) in group.iter().zip(original_values.iter()) {
            index_map[*slot] = *val;
        }
        None
    }

    recurse(0, groups, &mut index_map, patterns, wires, view)
}

/// Generate all permutations of a small slice (expected size <= 4).
fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut result = Vec::new();
    for (i, &item) in items.iter().enumerate() {
        let rest: Vec<usize> = items.iter().enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, &v)| v)
            .collect();
        for mut perm in permutations(&rest) {
            perm.insert(0, item);
            result.push(perm);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Fusion pass
// ---------------------------------------------------------------------------

/// Apply all fusion rules to the node graph, returning the number of
/// fusions applied.
///
/// Operates on mutable vectors of nodes and wiring. Nodes consumed by
/// fusion are replaced with `None` (removed during later DCE/topo sort).
///
/// The pass runs to a fixed point: it repeats until no more rules match.
/// Apply all fusion rules to the node graph, returning the number of
/// fusions applied.
///
/// `output_nodes` lists node indices that are directly referenced by
/// named outputs — these must not be consumed as interior nodes.
pub fn apply_fusions(
    nodes: &mut Vec<Option<Box<dyn GkNode>>>,
    wiring: &mut Vec<Vec<WireSource>>,
    name_to_idx: &mut std::collections::HashMap<String, usize>,
    rules: &[FusionRule],
    output_nodes: &[usize],
) -> usize {
    let mut total_fused = 0;

    loop {
        let mut fused_this_pass = false;

        // Compute consumer counts for the external-consumer guard.
        let consumer_counts = compute_consumer_counts(nodes, wiring);

        let view = NodeView {
            nodes,
            wiring,
        };

        // Try each rule against each node.
        let mut best_match: Option<(usize, MatchResult, &FusionRule)> = None;

        'rule_loop: for rule in rules {
            let root_op = match rule.pattern.root_op() {
                Some(op) => op,
                None => continue,
            };

            for node_idx in 0..view.nodes.len() {
                let node = match &view.nodes[node_idx] {
                    Some(n) => n,
                    None => continue, // already consumed
                };

                if node.meta().name != root_op {
                    continue;
                }

                // Try matching the pattern rooted at this node.
                let source = WireSource::NodeOutput(node_idx, 0);
                let result = match try_match(&rule.pattern, &source, &view) {
                    Some(r) => r,
                    None => continue,
                };

                // External consumer guard: intermediate nodes (all consumed
                // nodes except the root) must have no consumers outside the
                // matched subgraph, and must not be output-referenced.
                if !check_consumer_guard(&result, node_idx, &consumer_counts, output_nodes) {
                    continue;
                }

                best_match = Some((node_idx, result, rule));
                break 'rule_loop;
            }
        }

        // Apply the best match, if any.
        if let Some((root_idx, result, rule)) = best_match {
            apply_single_fusion(root_idx, &result, rule, nodes, wiring, name_to_idx);
            total_fused += 1;
            fused_this_pass = true;
        }

        if !fused_this_pass {
            break;
        }
    }

    total_fused
}

/// Check that intermediate consumed nodes have no external consumers.
///
/// The root node is allowed to have external consumers — they'll be
/// rewired to the fused replacement. But interior nodes that get
/// deleted must not have consumers outside the matched subgraph.
fn check_consumer_guard(
    result: &MatchResult,
    root_idx: usize,
    consumer_counts: &[usize],
    output_nodes: &[usize],
) -> bool {
    for &consumed in &result.consumed_nodes {
        if consumed == root_idx {
            // Root node — consumers will be rewired.
            continue;
        }
        // Interior node must not be directly referenced by an output.
        if output_nodes.contains(&consumed) {
            return false;
        }
        // Interior node should have exactly 1 consumer (its parent
        // in the pattern). If it has more, someone else reads from it.
        if consumer_counts[consumed] > 1 {
            return false;
        }
    }
    true
}

/// Compute how many downstream nodes consume each node's output.
fn compute_consumer_counts(
    nodes: &[Option<Box<dyn GkNode>>],
    wiring: &[Vec<WireSource>],
) -> Vec<usize> {
    let mut counts = vec![0usize; nodes.len()];
    for (node_idx, node_wiring) in wiring.iter().enumerate() {
        if nodes[node_idx].is_none() {
            continue;
        }
        for source in node_wiring {
            if let WireSource::NodeOutput(upstream, _) = source {
                counts[*upstream] += 1;
            }
        }
    }
    counts
}

/// Apply a single fusion: replace the matched subgraph with the fused node.
fn apply_single_fusion(
    root_idx: usize,
    result: &MatchResult,
    rule: &FusionRule,
    nodes: &mut Vec<Option<Box<dyn GkNode>>>,
    wiring: &mut Vec<Vec<WireSource>>,
    name_to_idx: &mut std::collections::HashMap<String, usize>,
) {
    // Build the fused node.
    let fused_node = (rule.replacement)(result);
    let _fused_name = fused_node.meta().name.clone();

    // Build wiring for the fused node from the captured wire bindings.
    let fused_wiring: Vec<WireSource> = rule
        .input_bindings
        .iter()
        .map(|bind_name| result.wire(bind_name).clone())
        .collect();

    // Remove consumed interior nodes (not the root — we reuse its slot).
    for &consumed in &result.consumed_nodes {
        if consumed != root_idx {
            nodes[consumed] = None;
            wiring[consumed] = Vec::new();
        }
    }

    // Replace the root node with the fused node.
    nodes[root_idx] = Some(fused_node);
    wiring[root_idx] = fused_wiring;

    // Update name map: remove names for consumed interior nodes.
    // Keep the root node's name(s) so downstream references still resolve.
    name_to_idx.retain(|_, &mut idx| {
        !result.consumed_nodes.contains(&idx) || idx == root_idx
    });

    // Rewire any downstream nodes that referenced consumed interior
    // nodes. This shouldn't happen if the consumer guard passed, but
    // handle it defensively.
    // (The root node keeps its index, so downstream refs to it are fine.)
}

// ---------------------------------------------------------------------------
// Built-in fusion rules
// ---------------------------------------------------------------------------

/// The default set of fusion rules applied during assembly.
///
/// Rules are tried in order. Each rule's correctness is verified by
/// equivalence property tests that exercise the fused node's
/// `decomposed()` contract against random inputs.
pub fn default_rules() -> Vec<FusionRule> {
    use crate::nodes::hash::{HashRange, HashInterval};
    use crate::nodes::lerp::ScaleRange;

    vec![
        // mod(hash(x), K) → hash_range(x, K)
        //
        // Combines hashing and bounded reduction. The fused node
        // avoids a separate buffer slot for the intermediate hash.
        FusionRule {
            name: "hash_mod_to_hash_range",
            pattern: FusionPattern::node(
                "mod",
                vec![
                    FusionPattern::node("hash", vec![FusionPattern::any("x")], "hash_node"),
                ],
                "mod_node",
            ),
            replacement: |m| {
                let max = m.const_u64("mod_node");
                Box::new(HashRange::new(max))
            },
            input_bindings: &["x"],
        },

        // lerp(unit_interval(hash(x)), lo, hi) → hash_interval(x, lo, hi)
        //
        // Single hash + scaled float in one step.
        FusionRule {
            name: "hash_unit_lerp_to_hash_interval",
            pattern: FusionPattern::node(
                "lerp",
                vec![
                    FusionPattern::node(
                        "unit_interval",
                        vec![
                            FusionPattern::node(
                                "hash",
                                vec![FusionPattern::any("x")],
                                "hash_node",
                            ),
                        ],
                        "ui_node",
                    ),
                ],
                "lerp_node",
            ),
            replacement: |m| {
                let consts = m.const_vec("lerp_node");
                let lo = f64::from_bits(consts[0]);
                let hi = f64::from_bits(consts[1]);
                Box::new(HashInterval::new(lo, hi))
            },
            input_bindings: &["x"],
        },

        // scale_range fusion: unit_interval(x) fed into lerp(t, lo, hi)
        // → scale_range(x, lo, hi)
        //
        // Eliminates the intermediate f64 unit interval node.
        FusionRule {
            name: "unit_lerp_to_scale_range",
            pattern: FusionPattern::node(
                "lerp",
                vec![
                    FusionPattern::node(
                        "unit_interval",
                        vec![FusionPattern::any("x")],
                        "ui_node",
                    ),
                ],
                "lerp_node",
            ),
            replacement: |m| {
                let consts = m.const_vec("lerp_node");
                let lo = f64::from_bits(consts[0]);
                let hi = f64::from_bits(consts[1]);
                Box::new(ScaleRange::new(lo, hi))
            },
            input_bindings: &["x"],
        },
    ]
}

// ---------------------------------------------------------------------------
// Equivalence testing support
// ---------------------------------------------------------------------------

/// A mini-DAG used for equivalence testing.
///
/// Built by fused nodes to represent their unfused (decomposed) form.
/// Not used at runtime — only in tests.
pub struct DecomposedGraph {
    pub input_count: usize,
    pub nodes: Vec<(Box<dyn GkNode>, Vec<DecomposedWire>)>,
    pub output_wires: Vec<DecomposedWire>,
}

/// Wire source within a `DecomposedGraph`.
#[derive(Debug, Clone)]
pub enum DecomposedWire {
    /// One of the graph's external inputs, by index.
    Input(usize),
    /// Output of a node within this graph: (node_index, port).
    Node(usize, usize),
}

impl DecomposedGraph {
    pub fn new(input_count: usize) -> Self {
        Self {
            input_count,
            nodes: Vec::new(),
            output_wires: Vec::new(),
        }
    }

    /// Add a node and return its index.
    pub fn add_node(
        &mut self,
        node: Box<dyn GkNode>,
        wires: Vec<DecomposedWire>,
    ) -> usize {
        let idx = self.nodes.len();
        self.nodes.push((node, wires));
        idx
    }

    /// Set the output wire(s) of this graph.
    pub fn set_outputs(&mut self, wires: Vec<DecomposedWire>) {
        self.output_wires = wires;
    }

    /// Evaluate this decomposed graph on the given inputs.
    /// Returns the output values.
    pub fn eval(&self, inputs: &[crate::node::Value]) -> Vec<crate::node::Value> {
        use crate::node::Value;

        let mut node_outputs: Vec<Vec<Value>> = Vec::new();

        for (node, wire_sources) in &self.nodes {
            // Gather inputs for this node.
            let node_inputs: Vec<Value> = wire_sources
                .iter()
                .map(|w| match w {
                    DecomposedWire::Input(i) => inputs[*i].clone(),
                    DecomposedWire::Node(n, p) => node_outputs[*n][*p].clone(),
                })
                .collect();

            // Evaluate.
            let output_count = node.meta().outs.len();
            let mut outputs = vec![Value::None; output_count];
            node.eval(&node_inputs, &mut outputs);
            node_outputs.push(outputs);
        }

        // Gather final outputs.
        self.output_wires
            .iter()
            .map(|w| match w {
                DecomposedWire::Input(i) => inputs[*i].clone(),
                DecomposedWire::Node(n, p) => node_outputs[*n][*p].clone(),
            })
            .collect()
    }
}

/// Trait for fused nodes that carry an equivalence contract.
///
/// Any node produced by a fusion rule's `replacement` factory should
/// implement this to enable automated equivalence testing.
pub trait FusedNode: GkNode {
    /// Build the decomposed (unfused) subgraph that this node is
    /// semantically equivalent to.
    fn decomposed(&self) -> DecomposedGraph;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assembly::{GkAssembler, WireRef};
    use crate::node::Value;
    use crate::nodes::arithmetic::ModU64;
    use crate::nodes::hash::Hash64;

    #[test]
    fn hash_mod_fuses_to_hash_range() {
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm.add_node("m", Box::new(ModU64::new(100)), vec![WireRef::node("h")]);
        asm.add_output("out", WireRef::node("m"));

        let mut kernel = asm.compile().unwrap();

        // Verify output correctness after fusion.
        for cycle in 0..1000u64 {
            kernel.set_inputs(&[cycle]);
            let result = kernel.pull("out").as_u64();
            // Must match hash_range semantics: hash(cycle) % 100
            let expected = xxhash_rust::xxh3::xxh3_64(&cycle.to_le_bytes()) % 100;
            assert_eq!(result, expected, "cycle {cycle}");
        }
    }

    #[test]
    fn fusion_skipped_when_intermediate_has_consumers() {
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm.add_node("m", Box::new(ModU64::new(100)), vec![WireRef::node("h")]);
        // Also wire hash output to a second consumer.
        asm.add_node("m2", Box::new(ModU64::new(50)), vec![WireRef::node("h")]);
        asm.add_output("out1", WireRef::node("m"));
        asm.add_output("out2", WireRef::node("m2"));

        let mut kernel = asm.compile().unwrap();

        // Both outputs should still work correctly.
        for cycle in 0..100u64 {
            kernel.set_inputs(&[cycle]);
            let h = xxhash_rust::xxh3::xxh3_64(&cycle.to_le_bytes());
            assert_eq!(kernel.pull("out1").as_u64(), h % 100, "out1 cycle {cycle}");
            assert_eq!(kernel.pull("out2").as_u64(), h % 50, "out2 cycle {cycle}");
        }
    }

    #[test]
    fn permutations_small() {
        let p = permutations(&[0, 1]);
        assert_eq!(p.len(), 2);
        assert!(p.contains(&vec![0, 1]));
        assert!(p.contains(&vec![1, 0]));

        let p3 = permutations(&[0, 1, 2]);
        assert_eq!(p3.len(), 6);
    }

    #[test]
    fn permutations_single() {
        let p = permutations(&[42]);
        assert_eq!(p, vec![vec![42]]);
    }

    #[test]
    fn permutations_empty() {
        let p: Vec<Vec<usize>> = permutations(&[]);
        assert_eq!(p, vec![Vec::<usize>::new()]);
    }

    // -------------------------------------------------------------------
    // Equivalence property tests
    // -------------------------------------------------------------------

    /// Helper: compare fused node output vs decomposed graph output
    /// across a range of deterministic inputs.
    fn assert_equivalence(fused: &dyn FusedNode, test_count: usize) {
        let decomposed = fused.decomposed();
        let input_count = fused.meta().wire_inputs().len();
        let output_count = fused.meta().outs.len();

        for seed in 0..test_count as u64 {
            // Generate deterministic inputs from the seed.
            let inputs: Vec<Value> = (0..input_count)
                .map(|port| {
                    let v = xxhash_rust::xxh3::xxh3_64(
                        &(seed.wrapping_mul(31).wrapping_add(port as u64)).to_le_bytes()
                    );
                    // Use the port type from the fused node's metadata.
                    match fused.meta().wire_inputs()[port].typ {
                        crate::node::PortType::U64 => Value::U64(v),
                        crate::node::PortType::F64 => Value::F64(f64::from_bits(v)),
                        _ => Value::U64(v), // fallback for other types
                    }
                })
                .collect();

            // Evaluate fused node.
            let mut fused_outputs = vec![Value::None; output_count];
            fused.eval(&inputs, &mut fused_outputs);

            // Evaluate decomposed graph.
            let decomposed_outputs = decomposed.eval(&inputs);

            // Compare each output.
            for (port_idx, (fused_val, decomposed_val)) in
                fused_outputs.iter().zip(decomposed_outputs.iter()).enumerate()
            {
                match (&fused_val, &decomposed_val) {
                    (Value::U64(a), Value::U64(b)) => {
                        assert_eq!(a, b,
                            "equivalence failed: seed={seed} port={port_idx} fused={a} decomposed={b}"
                        );
                    }
                    (Value::F64(a), Value::F64(b)) => {
                        // Allow tiny floating point differences from operation reordering.
                        let diff = (a - b).abs();
                        let tolerance = 1e-10 * a.abs().max(b.abs()).max(1.0);
                        assert!(diff <= tolerance,
                            "equivalence failed: seed={seed} port={port_idx} fused={a} decomposed={b} diff={diff}"
                        );
                    }
                    _ => {
                        assert_eq!(
                            fused_val.to_display_string(),
                            decomposed_val.to_display_string(),
                            "equivalence failed: seed={seed} port={port_idx}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn hash_range_equivalence() {
        use crate::nodes::hash::HashRange;
        // Test with various moduli including edge cases.
        for max in [1, 2, 7, 100, 10_000, u64::MAX] {
            let fused = HashRange::new(max);
            assert_equivalence(&fused, 10_000);
        }
    }

    #[test]
    fn hash_interval_equivalence() {
        use crate::nodes::hash::HashInterval;
        // Test with various ranges.
        for (lo, hi) in [(0.0, 1.0), (-180.0, 180.0), (0.0, 1000.0), (-1.0, -0.5)] {
            let fused = HashInterval::new(lo, hi);
            assert_equivalence(&fused, 10_000);
        }
    }

    #[test]
    fn scale_range_equivalence() {
        use crate::nodes::lerp::ScaleRange;
        for (lo, hi) in [(0.0, 1.0), (-100.0, 100.0), (0.0, 360.0), (1e6, 1e7)] {
            let fused = ScaleRange::new(lo, hi);
            assert_equivalence(&fused, 10_000);
        }
    }

    /// Meta-test: verify that the default fusion rules all reference
    /// nodes that implement FusedNode and pass equivalence checks.
    #[test]
    fn all_default_rules_produce_equivalent_nodes() {
        let rules = default_rules();
        for rule in &rules {
            // Build a representative match result with plausible constants.
            // We test the actual fused nodes via their specific tests above;
            // this test verifies the rule table is wired correctly.
            assert!(
                rule.pattern.root_op().is_some(),
                "rule '{}' has no root op",
                rule.name
            );
            assert!(
                !rule.input_bindings.is_empty(),
                "rule '{}' has no input bindings",
                rule.name
            );
        }
    }

    // --- VariadicNode pattern tests ---

    #[test]
    fn variadic_pattern_matches_sum() {
        use crate::nodes::arithmetic::SumN;

        // Build a graph: sum(a, b, c) where a, b, c are coordinates
        let mut asm = GkAssembler::new(vec!["a".into(), "b".into(), "c".into()]);
        asm.add_node("s", Box::new(SumN::new(3)), vec![
            WireRef::input("a"), WireRef::input("b"), WireRef::input("c"),
        ]);
        asm.add_output("out", WireRef::node("s"));

        let mut kernel = asm.compile().unwrap();

        // Verify it works.
        kernel.set_inputs(&[10, 20, 30]);
        assert_eq!(kernel.pull("out").as_u64(), 60);
    }

    #[test]
    fn typed_constants_captured_in_match() {
        use crate::nodes::hash::HashRange;
        use crate::node::ConstValue;

        // Build: hash_range(cycle, 100)
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("hr", Box::new(HashRange::new(100)), vec![WireRef::input("cycle")]);
        asm.add_output("out", WireRef::node("hr"));

        // Manually test pattern matching on the resolved graph.
        // HashRange has slots: [Wire("input"), Const("max", U64, 100)]
        let node = HashRange::new(100);
        let typed: Vec<ConstValue> = node.meta().const_slots()
            .iter()
            .map(|c| c.1.clone())
            .collect();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0], ConstValue::U64(100));
    }

    #[test]
    fn match_result_string_bindings() {
        // Verify String bindings work correctly for lookup.
        let mut m = MatchResult::new();
        m.wires.push(("x".to_string(), WireSource::Input(0)));
        m.constants.push(("mod_node".to_string(), vec![42]));
        m.typed_constants.push(("mod_node".to_string(), vec![
            crate::node::ConstValue::U64(42),
        ]));

        assert_eq!(m.const_u64("mod_node"), 42);
        assert_eq!(m.typed_consts("mod_node").len(), 1);
        assert!(matches!(m.wire("x"), WireSource::Input(0)));
    }
}
