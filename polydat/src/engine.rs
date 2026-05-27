// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Engine selection: analyze a compiled DAG and choose the optimal
//! monomorphic kernel variant.
//!
//! The heuristic is applied at construction time by the assembler.
//! The resulting kernel has the selected optimizations baked in with
//! no runtime strategy branching.

use std::collections::HashMap;
use crate::compiled::{CompiledKernelRaw, CompiledKernelPull, CompiledKernelPushPull};
use crate::kernel::{GkProgram, WireSource};
use crate::node::GkNode;

/// Which provenance optimization the compiler selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvMode {
    /// No provenance — eval runs all nodes unconditionally.
    Raw,
    /// Pull-side cone guard only. `set_inputs` tracks `changed_mask`,
    /// `eval_for_slot` skips eval when the output cone is clean.
    /// Zero overhead on all-dirty graphs.
    Pull,
    /// Push + pull. Per-node dirty tracking in `set_inputs` +
    /// cone guard. Only selected when output cones are large AND
    /// partially stable.
    PushPull,
}

/// Graph analysis results used by the engine selection heuristic.
#[derive(Debug, Clone)]
pub struct GraphAnalysis {
    pub total_nodes: usize,
    pub num_inputs: usize,
    pub num_outputs: usize,
    /// Per-output cone size (number of nodes in transitive dependency).
    pub output_cone_sizes: Vec<(String, usize)>,
    /// max(cone_size) / total_nodes
    pub max_cone_ratio: f64,
    /// Average cone_size / total_nodes
    pub avg_cone_ratio: f64,
    /// Per-node provenance bitmask (which inputs the node depends on).
    pub node_provenance: Vec<u64>,
}

/// Analyze a resolved DAG to compute structural metrics.
pub fn analyze_graph(
    nodes: &[Box<dyn GkNode>],
    wiring: &[Vec<WireSource>],
    output_map: &HashMap<String, (usize, usize)>,
) -> GraphAnalysis {
    let total_nodes = nodes.len();
    let node_provenance = GkProgram::compute_provenance(nodes, wiring);

    // Compute per-output cone size: count nodes reachable from each output.
    // A node is in the cone if its provenance overlaps with the output's.
    // Actually, we need the transitive upstream set, which is the set of
    // nodes that can reach this output. We compute this by walking backward
    // from the output node.
    let mut output_cone_sizes = Vec::new();
    for (name, &(node_idx, _port)) in output_map {
        let cone_size = compute_cone_size(node_idx, wiring);
        output_cone_sizes.push((name.clone(), cone_size));
    }

    let max_cone = output_cone_sizes.iter().map(|(_, s)| *s).max().unwrap_or(0);
    let avg_cone: f64 = if output_cone_sizes.is_empty() { 0.0 }
        else { output_cone_sizes.iter().map(|(_, s)| *s as f64).sum::<f64>() / output_cone_sizes.len() as f64 };

    let max_cone_ratio = if total_nodes > 0 { max_cone as f64 / total_nodes as f64 } else { 1.0 };
    let avg_cone_ratio = if total_nodes > 0 { avg_cone / total_nodes as f64 } else { 1.0 };

    // Count distinct inputs
    let mut max_input = 0usize;
    for sources in wiring {
        for s in sources {
            if let WireSource::Input(idx) = s {
                max_input = max_input.max(*idx + 1);
            }
        }
    }

    GraphAnalysis {
        total_nodes,
        num_inputs: max_input,
        num_outputs: output_map.len(),
        output_cone_sizes,
        max_cone_ratio,
        avg_cone_ratio,
        node_provenance,
    }
}

/// Count the number of nodes in the transitive upstream cone of a node.
fn compute_cone_size(node_idx: usize, wiring: &[Vec<WireSource>]) -> usize {
    let mut visited = vec![false; wiring.len()];
    let mut stack = vec![node_idx];
    let mut count = 0;
    while let Some(idx) = stack.pop() {
        if idx >= visited.len() || visited[idx] { continue; }
        visited[idx] = true;
        count += 1;
        for source in &wiring[idx] {
            if let WireSource::NodeOutput(upstream, _) = source {
                if !visited[*upstream] {
                    stack.push(*upstream);
                }
            }
        }
    }
    count
}

/// Select the optimal provenance mode based on graph analysis.
///
/// Heuristic (from benchmark findings in memo 09):
/// - Pull has zero overhead on all-dirty graphs (cone check ~2ns)
/// - Pull is the safe default for selective output access
/// - PushPull when multiple inputs exist (push skip helps within
///   dirty cones when some subgraphs are stable)
/// - Raw only for tiny single-input graphs
pub fn select_prov_mode(analysis: &GraphAnalysis) -> ProvMode {
    // Tiny single-input graphs: skip provenance data entirely.
    // The overhead of tracking changed_mask isn't worth it.
    if analysis.total_nodes < 15 && analysis.num_inputs <= 1 {
        return ProvMode::Raw;
    }

    // Multiple inputs: some may be stable at runtime, enabling both
    // push-side skip (within dirty cones) and pull-side skip (clean cones).
    // PushPull is the right choice because:
    // - If an output's cone is clean: pull guard skips eval entirely
    // - If an output's cone is dirty: push skip avoids stable nodes
    // The push overhead (~10ns in set_inputs for dependent marking)
    // is justified by the potential to skip 30-80% of nodes within
    // dirty cones.
    if analysis.num_inputs >= 2 {
        return ProvMode::PushPull;
    }

    // Single input: every node's cone includes the one input, so the
    // pull guard will always see "dirty" and fall through. Pull still
    // has zero overhead (the AND + branch costs ~2ns), so prefer it
    // over Raw as free insurance for future multi-output scenarios
    // where not all outputs are pulled every cycle.
    ProvMode::Pull
}

// ── Compiled engine wrappers ───────────────────────────────────

/// Auto-selected P2 compiled kernel. The variant is chosen at
/// construction time based on graph analysis. One outer branch
/// per eval call (perfectly predicted), zero branches inside.
pub enum P2Engine {
    Raw(CompiledKernelRaw),
    Pull(CompiledKernelPull),
    PushPull(CompiledKernelPushPull),
}

impl P2Engine {
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        match self {
            P2Engine::Raw(k) => k.eval_for_slot(coords, slot),
            P2Engine::Pull(k) => k.eval_for_slot(coords, slot),
            P2Engine::PushPull(k) => k.eval_for_slot(coords, slot),
        }
    }

    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        match self {
            P2Engine::Raw(k) => k.eval(coords),
            P2Engine::Pull(k) => k.eval(coords),
            P2Engine::PushPull(k) => k.eval(coords),
        }
    }

    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        match self {
            P2Engine::Raw(k) => k.get_slot(slot),
            P2Engine::Pull(k) => k.get_slot(slot),
            P2Engine::PushPull(k) => k.get_slot(slot),
        }
    }

    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        match self {
            P2Engine::Raw(k) => k.resolve_output(name),
            P2Engine::Pull(k) => k.resolve_output(name),
            P2Engine::PushPull(k) => k.resolve_output(name),
        }
    }

    pub fn coord_count(&self) -> usize {
        match self {
            P2Engine::Raw(k) => k.coord_count(),
            P2Engine::Pull(k) => k.coord_count(),
            P2Engine::PushPull(k) => k.coord_count(),
        }
    }

    pub fn prov_mode(&self) -> ProvMode {
        match self {
            P2Engine::Raw(_) => ProvMode::Raw,
            P2Engine::Pull(_) => ProvMode::Pull,
            P2Engine::PushPull(_) => ProvMode::PushPull,
        }
    }
}

/// Auto-selected P3 JIT kernel.
#[cfg(feature = "jit")]
pub enum P3Engine {
    Raw(crate::jit::JitKernelRaw),
    Pull(crate::jit::JitKernelPull),
    PushPull(crate::jit::JitKernelPushPull),
}

#[cfg(feature = "jit")]
impl P3Engine {
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        match self {
            P3Engine::Raw(k) => k.eval_for_slot(coords, slot),
            P3Engine::Pull(k) => k.eval_for_slot(coords, slot),
            P3Engine::PushPull(k) => k.eval_for_slot(coords, slot),
        }
    }

    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        match self {
            P3Engine::Raw(k) => k.eval(coords),
            P3Engine::Pull(k) => k.eval(coords),
            P3Engine::PushPull(k) => k.eval(coords),
        }
    }

    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        match self {
            P3Engine::Raw(k) => k.get_slot(slot),
            P3Engine::Pull(k) => k.get_slot(slot),
            P3Engine::PushPull(k) => k.get_slot(slot),
        }
    }

    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        match self {
            P3Engine::Raw(k) => k.resolve_output(name),
            P3Engine::Pull(k) => k.resolve_output(name),
            P3Engine::PushPull(k) => k.resolve_output(name),
        }
    }

    pub fn coord_count(&self) -> usize {
        match self {
            P3Engine::Raw(k) => k.coord_count(),
            P3Engine::Pull(k) => k.coord_count(),
            P3Engine::PushPull(k) => k.coord_count(),
        }
    }

    pub fn prov_mode(&self) -> ProvMode {
        match self {
            P3Engine::Raw(_) => ProvMode::Raw,
            P3Engine::Pull(_) => ProvMode::Pull,
            P3Engine::PushPull(_) => ProvMode::PushPull,
        }
    }
}
