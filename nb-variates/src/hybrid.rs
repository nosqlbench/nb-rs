// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Hybrid kernel: per-node optimal compilation level.
//!
//! Splits the DAG into segments based on each node's compilation
//! capability. JIT-able nodes are batched into native code segments.
//! Non-JIT-able nodes run as Phase 2 closures. All segments share
//! the same flat u64 buffer.
//!
//! This is the "best of all worlds" kernel — no node pays more
//! overhead than it needs to.

use std::collections::HashMap;

use crate::node::{CompiledU64Op, GkNode};
use crate::kernel::WireSource;

#[cfg(feature = "jit")]
use crate::jit::{self, JitOp};

/// A step in the hybrid kernel: either JIT native code or a Phase 2 closure.
enum HybridStep {
    /// A batch of nodes compiled to native code via Cranelift.
    /// The function reads/writes directly to the shared buffer.
    #[cfg(feature = "jit")]
    Jit(JitSegment),
    /// A single node executed via its Phase 2 closure.
    Closure(ClosureStep),
}

#[cfg(feature = "jit")]
struct JitSegment {
    code_fn: unsafe fn(*const u64, *mut u64),
    /// Keep the JIT module alive so the generated code isn't freed.
    _module: Box<dyn std::any::Any + Send>,
}

struct ClosureStep {
    op: CompiledU64Op,
    input_slots: Vec<usize>,
    output_slots: Vec<usize>,
}

/// A hybrid kernel where each node runs at its optimal compilation level.
pub struct HybridKernel {
    buffer: Vec<u64>,
    coord_count: usize,
    steps: Vec<HybridStep>,
    output_map: HashMap<String, usize>,
    gather_buf: Vec<u64>,
    scatter_buf: Vec<u64>,
    /// Keep source nodes alive so JIT-baked pointers remain valid.
    _nodes: Vec<Box<dyn crate::node::GkNode>>,
    /// Provenance caching state.
    provenance: Option<HybridProvenanceState>,
}

struct HybridProvenanceState {
    /// Per-step clean flag.
    step_clean: Vec<bool>,
    /// Per-input: list of step indices that depend on this input.
    input_dependents: Vec<Vec<usize>>,
}

impl HybridKernel {
    /// Evaluate the DAG. With provenance, skips cached steps.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        if let Some(ref mut prov) = self.provenance {
            for i in 0..coords.len().min(self.coord_count) {
                if self.buffer[i] != coords[i] {
                    self.buffer[i] = coords[i];
                    if i < prov.input_dependents.len() {
                        for &step_idx in &prov.input_dependents[i] {
                            prov.step_clean[step_idx] = false;
                        }
                    }
                }
            }
            for (step_idx, step) in self.steps.iter().enumerate() {
                if prov.step_clean[step_idx] { continue; }
                match step {
                    #[cfg(feature = "jit")]
                    HybridStep::Jit(seg) => unsafe {
                        (seg.code_fn)(self.buffer.as_ptr(), self.buffer.as_mut_ptr());
                    },
                    HybridStep::Closure(cs) => {
                        for (i, &slot) in cs.input_slots.iter().enumerate() {
                            self.gather_buf[i] = self.buffer[slot];
                        }
                        (cs.op)(
                            &self.gather_buf[..cs.input_slots.len()],
                            &mut self.scatter_buf[..cs.output_slots.len()],
                        );
                        for (i, &slot) in cs.output_slots.iter().enumerate() {
                            self.buffer[slot] = self.scatter_buf[i];
                        }
                    }
                }
                prov.step_clean[step_idx] = true;
            }
        } else {
            self.buffer[..self.coord_count].copy_from_slice(coords);
            for step in &self.steps {
                match step {
                    #[cfg(feature = "jit")]
                    HybridStep::Jit(seg) => unsafe {
                        (seg.code_fn)(self.buffer.as_ptr(), self.buffer.as_mut_ptr());
                    },
                    HybridStep::Closure(cs) => {
                        for (i, &slot) in cs.input_slots.iter().enumerate() {
                            self.gather_buf[i] = self.buffer[slot];
                        }
                        (cs.op)(
                            &self.gather_buf[..cs.input_slots.len()],
                            &mut self.scatter_buf[..cs.output_slots.len()],
                        );
                        for (i, &slot) in cs.output_slots.iter().enumerate() {
                            self.buffer[slot] = self.scatter_buf[i];
                        }
                    }
                }
            }
        }
    }

    /// Read a named output after eval().
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        self.buffer[self.output_map[name]]
    }

    /// Read by slot index.
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.buffer[slot]
    }

    pub fn coord_count(&self) -> usize { self.coord_count }

    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.output_map.get(name).copied()
    }

    /// Store owned nodes to keep JIT-baked pointers valid.
    pub fn retain_nodes(&mut self, nodes: Vec<Box<dyn crate::node::GkNode>>) {
        self._nodes = nodes;
    }
}

/// Build a hybrid kernel from resolved DAG data.
///
/// Each node is classified: if it can be JIT-compiled, it goes into
/// a JIT segment. If not, it becomes a closure step. Adjacent JIT-able
/// nodes are batched into a single JIT segment for efficiency.
#[cfg(feature = "jit")]
pub fn build_hybrid(
    nodes: &[Box<dyn GkNode>],
    wiring: &[Vec<WireSource>],
    coord_count: usize,
    total_slots: usize,
    slot_bases: &[usize],
    output_map: HashMap<String, usize>,
) -> Result<HybridKernel, String> {
    let mut steps: Vec<HybridStep> = Vec::new();
    let mut max_inputs = 0usize;
    let mut max_outputs = 0usize;

    // Classify each node
    let classifications: Vec<(JitOp, Vec<usize>, Vec<usize>)> = nodes.iter()
        .enumerate()
        .map(|(node_idx, node)| {
            let jit_op = jit::classify_node(node.as_ref());

            let input_slots: Vec<usize> = wiring[node_idx]
                .iter()
                .map(|source| match source {
                    WireSource::Input(c) => *c,
                    WireSource::NodeOutput(upstream, port) => slot_bases[*upstream] + port,
                    WireSource::VolatilePort(_) | WireSource::StickyPort(_) => todo!("port slots in hybrid kernel"),
                })
                .collect();

            let output_count = node.meta().outs.len();
            let output_slots: Vec<usize> = (0..output_count)
                .map(|p| slot_bases[node_idx] + p)
                .collect();

            max_inputs = max_inputs.max(input_slots.len());
            max_outputs = max_outputs.max(output_slots.len());

            (jit_op, input_slots, output_slots)
        })
        .collect();

    // Batch adjacent JIT-able nodes into segments
    let mut i = 0;
    while i < classifications.len() {
        if matches!(classifications[i].0, JitOp::Fallback) {
            // This node needs a closure
            let node = &nodes[i];
            let (_, ref input_slots, ref output_slots) = classifications[i];
            if let Some(op) = node.compiled_u64() {
                steps.push(HybridStep::Closure(ClosureStep {
                    op,
                    input_slots: input_slots.clone(),
                    output_slots: output_slots.clone(),
                }));
            } else {
                return Err(format!("node '{}' has no compiled_u64 and can't be JIT-compiled", node.meta().name));
            }
            i += 1;
        } else {
            // Batch consecutive JIT-able nodes
            let batch_start = i;
            while i < classifications.len() && !matches!(classifications[i].0, JitOp::Fallback) {
                i += 1;
            }
            let batch: Vec<(JitOp, Vec<usize>, Vec<usize>)> = classifications[batch_start..i].to_vec();

            // Compile the batch to native code
            let empty_map = HashMap::new();
            let _jit_kernel = jit::compile_jit(coord_count, total_slots, batch, empty_map, Vec::new())?;
            // Extract the function pointer and module from the JitKernel
            // We need to reach into it... let's add a method.
            // Actually, for the hybrid, we need the raw fn pointer and module.
            // Let me restructure to expose these.

            // For now, just compile each JIT-able node as its own JIT segment.
            // Batching multiple nodes into one segment is a future optimization.
            for j in batch_start..i {
                let (ref jit_op, ref input_slots, ref output_slots) = classifications[j];
                let single_batch = vec![(jit_op.clone(), input_slots.clone(), output_slots.clone())];
                let jit_kernel = jit::compile_jit(coord_count, total_slots, single_batch, HashMap::new(), Vec::new())?;

                // Extract fn and module
                let (code_fn, module) = jit_kernel.into_parts();
                steps.push(HybridStep::Jit(JitSegment {
                    code_fn,
                    _module: Box::new(module),
                }));
            }
        }
    }

    // Compute per-step provenance: union of all nodes in each step
    let node_provenance = crate::kernel::GkProgram::compute_provenance(nodes, wiring);
    let step_count = steps.len();

    // Map node provenance to step provenance. Each step covers
    // one or more consecutive nodes. Build step→provenance bitmask.
    // For single-node steps this is direct. For batched JIT segments
    // it's the union of all nodes in the batch.
    // Currently each step is one node, so step_provenance == node_provenance.
    // (Batched segments would union the provenance of all nodes in the batch.)
    let step_provenance: Vec<u64> = {
        // Reconstruct which nodes each step covers by walking the classification
        let mut sp = Vec::with_capacity(step_count);
        // Since we build one step per node (JIT segments are per-node for now),
        // step index == node index
        for (i, _) in steps.iter().enumerate() {
            if i < node_provenance.len() {
                sp.push(node_provenance[i]);
            } else {
                sp.push(u64::MAX); // conservative: always dirty
            }
        }
        sp
    };

    let step_dependents = crate::kernel::GkProgram::compute_dependents(&step_provenance, coord_count);

    Ok(HybridKernel {
        buffer: vec![0u64; total_slots],
        coord_count,
        steps,
        output_map,
        gather_buf: vec![0u64; max_inputs],
        scatter_buf: vec![0u64; max_outputs],
        _nodes: Vec::new(),
        provenance: Some(HybridProvenanceState {
            step_clean: vec![false; step_count],
            input_dependents: step_dependents,
        }),
    })
}

/// Build a hybrid kernel without JIT (all closures).
#[cfg(not(feature = "jit"))]
pub fn build_hybrid(
    nodes: &[Box<dyn GkNode>],
    wiring: &[Vec<WireSource>],
    coord_count: usize,
    total_slots: usize,
    slot_bases: &[usize],
    output_map: HashMap<String, usize>,
) -> Result<HybridKernel, String> {
    let mut steps: Vec<HybridStep> = Vec::new();
    let mut max_inputs = 0usize;
    let mut max_outputs = 0usize;

    for (node_idx, node) in nodes.iter().enumerate() {
        let input_slots: Vec<usize> = wiring[node_idx]
            .iter()
            .map(|source| match source {
                WireSource::Input(c) => *c,
                WireSource::NodeOutput(upstream, port) => slot_bases[*upstream] + port,
                    WireSource::VolatilePort(_) | WireSource::StickyPort(_) => todo!("port slots in hybrid kernel"),
            })
            .collect();

        let output_count = node.meta().outs.len();
        let output_slots: Vec<usize> = (0..output_count)
            .map(|p| slot_bases[node_idx] + p)
            .collect();

        max_inputs = max_inputs.max(input_slots.len());
        max_outputs = max_outputs.max(output_slots.len());

        if let Some(op) = node.compiled_u64() {
            steps.push(HybridStep::Closure(ClosureStep {
                op,
                input_slots,
                output_slots,
            }));
        } else {
            return Err(format!("node '{}' has no compiled_u64", node.meta().name));
        }
    }

    let node_provenance = crate::kernel::GkProgram::compute_provenance(nodes, wiring);
    let step_count = steps.len();
    let step_dependents = crate::kernel::GkProgram::compute_dependents(&node_provenance, coord_count);

    Ok(HybridKernel {
        buffer: vec![0u64; total_slots],
        coord_count,
        steps,
        output_map,
        gather_buf: vec![0u64; max_inputs],
        scatter_buf: vec![0u64; max_outputs],
        _nodes: Vec::new(),
        provenance: Some(HybridProvenanceState {
            step_clean: vec![false; step_count],
            input_dependents: step_dependents,
        }),
    })
}
