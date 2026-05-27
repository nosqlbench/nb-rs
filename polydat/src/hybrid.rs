// Copyright 2024-2026 Jonathan Shook
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
//!
//! Three monomorphic kernel types, each with no runtime branching:
//!
//! | Type | Push (per-step skip) | Pull (cone guard) |
//! |------|---------------------|-------------------|
//! | `HybridKernelRaw` | — | — |
//! | `HybridKernelPull` | — | yes |
//! | `HybridKernelPushPull` | yes | yes |

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

/// Common fields shared by all hybrid kernel variants.
struct HybridCore {
    buffer: Vec<u64>,
    coord_count: usize,
    steps: Vec<HybridStep>,
    output_map: HashMap<String, usize>,
    gather_buf: Vec<u64>,
    scatter_buf: Vec<u64>,
    /// Keep source nodes alive so JIT-baked pointers remain valid.
    _nodes: Vec<Box<dyn GkNode>>,
}

/// Run all hybrid steps unconditionally (no clean checks).
#[inline]
fn eval_all_hybrid_steps(core: &mut HybridCore) {
    for step in &core.steps {
        match step {
            #[cfg(feature = "jit")]
            HybridStep::Jit(seg) => {
                // Funnel through the setjmp wrapper so JIT
                // predicate violations surface as catchable
                // panics instead of aborting. Matches the path
                // every stand-alone JIT kernel variant uses.
                let code_fn = seg.code_fn;
                let buf_const = core.buffer.as_ptr();
                let buf_mut = core.buffer.as_mut_ptr();
                crate::jit::invoke_with_catch(move || {
                    unsafe { (code_fn)(buf_const, buf_mut); }
                });
            }
            HybridStep::Closure(cs) => {
                for (i, &slot) in cs.input_slots.iter().enumerate() {
                    core.gather_buf[i] = core.buffer[slot];
                }
                (cs.op)(
                    &core.gather_buf[..cs.input_slots.len()],
                    &mut core.scatter_buf[..cs.output_slots.len()],
                );
                for (i, &slot) in cs.output_slots.iter().enumerate() {
                    core.buffer[slot] = core.scatter_buf[i];
                }
            }
        }
    }
}

/// Compute per-slot provenance bitmasks for the hybrid kernel.
///
/// Returns `slot_provenance[slot]` = bitmask of which inputs affect
/// that buffer slot. Used by pull-side cone guard.
fn compute_hybrid_slot_provenance(
    coord_count: usize,
    total_slots: usize,
    step_dependents: &[Vec<usize>],
    steps: &[HybridStep],
) -> Vec<u64> {
    let step_count = steps.len();
    let mut step_prov = vec![0u64; step_count];
    for (input_idx, deps) in step_dependents.iter().enumerate() {
        for &step_idx in deps {
            if step_idx < step_count {
                step_prov[step_idx] |= 1u64 << input_idx;
            }
        }
    }
    let mut slot_provenance = vec![0u64; total_slots];
    for i in 0..coord_count.min(64) {
        slot_provenance[i] = 1u64 << i;
    }
    for (step_idx, step) in steps.iter().enumerate() {
        // Only closure steps carry explicit output_slots; JIT steps use the
        // same buffer region but slot assignment is managed by the JIT code.
        if let HybridStep::Closure(cs) = step {
            for &slot in &cs.output_slots {
                if slot < slot_provenance.len() {
                    slot_provenance[slot] = step_prov[step_idx];
                }
            }
        }
    }
    slot_provenance
}

// ═══════════════════════════════════════════════════════════════
// Raw: no provenance, no cone guard. Eval runs all steps.
// ═══════════════════════════════════════════════════════════════

/// Hybrid kernel with no provenance tracking.
///
/// Every `eval()` call runs all steps unconditionally. Useful as a
/// baseline and for graphs where inputs change on every cycle.
pub struct HybridKernelRaw {
    core: HybridCore,
}

impl HybridKernelRaw {
    /// Evaluate all hybrid steps unconditionally.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.core.buffer[..self.core.coord_count.min(coords.len())]
            .copy_from_slice(&coords[..self.core.coord_count.min(coords.len())]);
        eval_all_hybrid_steps(&mut self.core);
    }

    /// Eval all steps and return the value at `slot`.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.eval(coords);
        self.core.buffer[slot]
    }

    /// Read a named output after `eval()`.
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        self.core.buffer[self.core.output_map[name]]
    }

    /// Read by slot index.
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.core.buffer[slot]
    }

    /// Number of coordinate inputs.
    pub fn coord_count(&self) -> usize { self.core.coord_count }

    /// Resolve an output name to its buffer slot.
    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.core.output_map.get(name).copied()
    }

    /// Store owned nodes to keep JIT-baked pointers valid.
    pub fn retain_nodes(&mut self, nodes: Vec<Box<dyn GkNode>>) {
        self.core._nodes = nodes;
    }
}

// ═══════════════════════════════════════════════════════════════
// Pull: cone guard only, no per-step skip.
// set_inputs tracks changed_mask. eval_for_slot checks the cone
// then runs ALL steps if dirty.
// ═══════════════════════════════════════════════════════════════

/// Hybrid kernel with pull-side cone guard.
///
/// `eval_for_slot()` checks whether the output's transitive input
/// cone changed before running steps. If nothing in the cone changed,
/// the cached value is returned without re-evaluation.
pub struct HybridKernelPull {
    core: HybridCore,
    slot_provenance: Vec<u64>,
    changed_mask: u64,
}

impl HybridKernelPull {
    /// Track which inputs changed. Does not mark individual steps dirty.
    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        self.changed_mask = 0;
        for i in 0..coords.len().min(self.core.coord_count) {
            if self.core.buffer[i] != coords[i] {
                self.core.buffer[i] = coords[i];
                self.changed_mask |= 1u64 << i;
            }
        }
    }

    /// Evaluate all steps (no cone guard).
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        eval_all_hybrid_steps(&mut self.core);
    }

    /// Cone guard: if the output's cone is clean, skip eval entirely.
    /// Otherwise run ALL steps (no per-step skip).
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.set_inputs(coords);
        if slot < self.slot_provenance.len() {
            if self.slot_provenance[slot] & self.changed_mask == 0 {
                return self.core.buffer[slot];
            }
        }
        eval_all_hybrid_steps(&mut self.core);
        self.core.buffer[slot]
    }

    /// Read a named output after `eval()`.
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        self.core.buffer[self.core.output_map[name]]
    }

    /// Read by slot index.
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.core.buffer[slot]
    }

    /// Number of coordinate inputs.
    pub fn coord_count(&self) -> usize { self.core.coord_count }

    /// Resolve an output name to its buffer slot.
    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.core.output_map.get(name).copied()
    }

    /// Store owned nodes to keep JIT-baked pointers valid.
    pub fn retain_nodes(&mut self, nodes: Vec<Box<dyn GkNode>>) {
        self.core._nodes = nodes;
    }
}

// ═══════════════════════════════════════════════════════════════
// PushPull: push-side per-step skip + pull-side cone guard.
// Full optimization — the production default.
// ═══════════════════════════════════════════════════════════════

/// Hybrid kernel with both push-side per-step skip and pull-side cone guard.
///
/// Push side: `set_inputs()` marks only steps that depend on changed inputs
/// as dirty; clean steps are skipped during `eval()`.
///
/// Pull side: `eval_for_slot()` first checks whether the output's cone of
/// influence changed at all. If not, the cached value is returned without
/// entering the eval loop.
pub struct HybridKernelPushPull {
    core: HybridCore,
    step_clean: Vec<bool>,
    input_dependents: Vec<Vec<usize>>,
    slot_provenance: Vec<u64>,
    changed_mask: u64,
}

impl HybridKernelPushPull {
    /// Track which inputs changed and dirty affected steps.
    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        self.changed_mask = 0;
        for i in 0..coords.len().min(self.core.coord_count) {
            if self.core.buffer[i] != coords[i] {
                self.core.buffer[i] = coords[i];
                self.changed_mask |= 1u64 << i;
                if i < self.input_dependents.len() {
                    for &step_idx in &self.input_dependents[i] {
                        self.step_clean[step_idx] = false;
                    }
                }
            }
        }
    }

    /// Evaluate with push-side step skip (no cone guard).
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        for (step_idx, step) in self.core.steps.iter().enumerate() {
            if self.step_clean[step_idx] { continue; }
            match step {
                #[cfg(feature = "jit")]
                HybridStep::Jit(seg) => {
                    let code_fn = seg.code_fn;
                    let buf_const = self.core.buffer.as_ptr();
                    let buf_mut = self.core.buffer.as_mut_ptr();
                    crate::jit::invoke_with_catch(move || {
                        unsafe { (code_fn)(buf_const, buf_mut); }
                    });
                }
                HybridStep::Closure(cs) => {
                    for (i, &slot) in cs.input_slots.iter().enumerate() {
                        self.core.gather_buf[i] = self.core.buffer[slot];
                    }
                    (cs.op)(
                        &self.core.gather_buf[..cs.input_slots.len()],
                        &mut self.core.scatter_buf[..cs.output_slots.len()],
                    );
                    for (i, &slot) in cs.output_slots.iter().enumerate() {
                        self.core.buffer[slot] = self.core.scatter_buf[i];
                    }
                }
            }
            self.step_clean[step_idx] = true;
        }
    }

    /// Cone guard + push-side skip: the full optimization.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.set_inputs(coords);
        if slot < self.slot_provenance.len() {
            if self.slot_provenance[slot] & self.changed_mask == 0 {
                return self.core.buffer[slot];
            }
        }
        for (step_idx, step) in self.core.steps.iter().enumerate() {
            if self.step_clean[step_idx] { continue; }
            match step {
                #[cfg(feature = "jit")]
                HybridStep::Jit(seg) => {
                    let code_fn = seg.code_fn;
                    let buf_const = self.core.buffer.as_ptr();
                    let buf_mut = self.core.buffer.as_mut_ptr();
                    crate::jit::invoke_with_catch(move || {
                        unsafe { (code_fn)(buf_const, buf_mut); }
                    });
                }
                HybridStep::Closure(cs) => {
                    for (i, &slot) in cs.input_slots.iter().enumerate() {
                        self.core.gather_buf[i] = self.core.buffer[slot];
                    }
                    (cs.op)(
                        &self.core.gather_buf[..cs.input_slots.len()],
                        &mut self.core.scatter_buf[..cs.output_slots.len()],
                    );
                    for (i, &slot) in cs.output_slots.iter().enumerate() {
                        self.core.buffer[slot] = self.core.scatter_buf[i];
                    }
                }
            }
            self.step_clean[step_idx] = true;
        }
        self.core.buffer[slot]
    }

    /// Read a named output after `eval()`.
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        self.core.buffer[self.core.output_map[name]]
    }

    /// Read by slot index.
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.core.buffer[slot]
    }

    /// Number of coordinate inputs.
    pub fn coord_count(&self) -> usize { self.core.coord_count }

    /// Resolve an output name to its buffer slot.
    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.core.output_map.get(name).copied()
    }

    /// Store owned nodes to keep JIT-baked pointers valid.
    pub fn retain_nodes(&mut self, nodes: Vec<Box<dyn GkNode>>) {
        self.core._nodes = nodes;
    }
}

/// Type alias for the default hybrid kernel (PushPull — full optimization).
///
/// Assembler and bench code that references `HybridKernel` uses the full
/// push+pull variant. Rename uses to the concrete type if different
/// optimization trade-offs are needed.
pub type HybridKernel = HybridKernelPushPull;

/// Build a hybrid kernel from resolved DAG data.
///
/// Each node is classified: if it can be JIT-compiled, it goes into
/// a JIT segment. If not, it becomes a closure step. Adjacent JIT-able
/// nodes are batched into a single JIT segment for efficiency.
///
/// Returns a `HybridKernelPushPull` (the production default).
#[cfg(feature = "jit")]
pub fn build_hybrid(
    nodes: &[Box<dyn GkNode>],
    wiring: &[Vec<WireSource>],
    coord_count: usize,
    total_slots: usize,
    slot_bases: &[usize],
    output_map: HashMap<String, usize>,
) -> Result<HybridKernelPushPull, String> {
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
            let _jit_kernel = jit::compile_jit_raw(coord_count, total_slots, batch, empty_map, Vec::new())?;

            // For now, compile each JIT-able node as its own JIT segment.
            // Batching multiple nodes into one segment is a future optimization.
            for j in batch_start..i {
                let (ref jit_op, ref input_slots, ref output_slots) = classifications[j];
                let single_batch = vec![(jit_op.clone(), input_slots.clone(), output_slots.clone())];
                let jit_kernel = jit::compile_jit_raw(coord_count, total_slots, single_batch, HashMap::new(), Vec::new())?;

                // Extract fn and module
                let (code_fn, module) = jit_kernel.into_parts();
                steps.push(HybridStep::Jit(JitSegment {
                    code_fn,
                    _module: Box::new(module),
                }));
            }
        }
    }

    build_pushpull_from_steps(
        steps, wiring, nodes, coord_count, total_slots,
        output_map, max_inputs, max_outputs,
    )
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
) -> Result<HybridKernelPushPull, String> {
    let mut steps: Vec<HybridStep> = Vec::new();
    let mut max_inputs = 0usize;
    let mut max_outputs = 0usize;

    for (node_idx, node) in nodes.iter().enumerate() {
        let input_slots: Vec<usize> = wiring[node_idx]
            .iter()
            .map(|source| match source {
                WireSource::Input(c) => *c,
                WireSource::NodeOutput(upstream, port) => slot_bases[*upstream] + port,
                WireSource::Port(_) => todo!("port slots in hybrid kernel"),
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

    build_pushpull_from_steps(
        steps, wiring, nodes, coord_count, total_slots,
        output_map, max_inputs, max_outputs,
    )
}

/// Shared construction of `HybridKernelPushPull` from assembled steps.
///
/// Computes provenance bitmasks from the DAG wiring and builds the
/// step_dependents list for push-side invalidation and the slot_provenance
/// table for pull-side cone guard.
fn build_pushpull_from_steps(
    steps: Vec<HybridStep>,
    wiring: &[Vec<WireSource>],
    nodes: &[Box<dyn GkNode>],
    coord_count: usize,
    total_slots: usize,
    output_map: HashMap<String, usize>,
    max_inputs: usize,
    max_outputs: usize,
) -> Result<HybridKernelPushPull, String> {
    let step_count = steps.len();

    // Compute per-node provenance and invert into per-input step dependents.
    // Since each step currently maps to one node, step index == node index.
    let node_provenance = crate::kernel::GkProgram::compute_provenance(nodes, wiring);
    let step_dependents = crate::kernel::GkProgram::compute_dependents(&node_provenance, coord_count);

    let slot_provenance = compute_hybrid_slot_provenance(
        coord_count, total_slots, &step_dependents, &steps,
    );

    Ok(HybridKernelPushPull {
        core: HybridCore {
            buffer: vec![0u64; total_slots],
            coord_count,
            steps,
            output_map,
            gather_buf: vec![0u64; max_inputs.max(1)],
            scatter_buf: vec![0u64; max_outputs.max(1)],
            _nodes: Vec::new(),
        },
        step_clean: vec![false; step_count],
        input_dependents: step_dependents,
        slot_provenance,
        changed_mask: u64::MAX, // all dirty on first eval
    })
}
