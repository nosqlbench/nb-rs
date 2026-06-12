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

use crate::ast::{CompiledU64Op, PolydatNode};
use crate::kernel::WireSource;

#[cfg(feature = "jit")]
use crate::compile::jit::{self, JitOp};

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

/// A closure step's op: pure-scalar u64 closure, or a slot op
/// with kernel-owned scratch for typed-slice ports (§8.4 L3).
enum ClosureOp {
    U64(CompiledU64Op),
    Slot(crate::ast::CompiledSlotOp),
}

struct ClosureStep {
    op: ClosureOp,
    input_slots: Vec<usize>,
    output_slots: Vec<usize>,
    /// `[start, end)` into the kernel's scratch arena.
    scratch_range: (usize, usize),
}

/// Common fields shared by all hybrid kernel variants.
struct HybridCore {
    buffer: Vec<u64>,
    coord_count: usize,
    steps: Vec<HybridStep>,
    output_map: HashMap<String, usize>,
    gather_buf: Vec<u64>,
    scatter_buf: Vec<u64>,
    /// Kernel-owned vector storage; vector-producing ports'
    /// (ptr, len) slots view entries here (§8.4 layer 3).
    scratch: Vec<crate::ast::ScratchBuf>,
    /// Axiom S2: per-slot Ref2 mask — raw readers panic on these.
    ref_slots: Vec<bool>,
    /// Axiom S9(a): (first slot of a Ref pair → scratch index).
    ref_scratch: Vec<(usize, usize)>,
    /// Keep source nodes alive so JIT-baked pointers remain valid.
    _nodes: Vec<Box<dyn PolydatNode>>,
}

impl HybridCore {
    /// Axiom S9(a) — deterministic Ref validation (see
    /// `jit_boundary.md` §"Slot-state axioms"). Gated to
    /// `debug_assertions` to match its call sites, which compile
    /// out in release.
    #[cfg(debug_assertions)]
    fn validate_refs(&self) {
        for &(slot, idx) in &self.ref_scratch {
            let (p, l) = self.scratch[idx].ptr_len();
            assert!(
                self.buffer[slot] == p && self.buffer[slot + 1] == l,
                "S9 ref-validator: slot pair ({slot}, {}) = ({:#x}, {}) \
                 does not match scratch[{idx}] = ({p:#x}, {l})",
                slot + 1,
                self.buffer[slot],
                self.buffer[slot + 1],
            );
        }
    }

    /// Axiom S2 guard for raw u64 readers.
    #[inline]
    fn guard_ref_slot(&self, slot: usize) {
        if self.ref_slots.get(slot).copied().unwrap_or(false) {
            panic!(
                "S2 pointer containment: slot {slot} is Ref2-colored; raw \
                 u64 readers would leak an interior address. Use the typed \
                 borrow-checked accessor (read_vec_*) or copy out."
            );
        }
    }

    /// Axiom S2 typed accessor core (borrow ties to &self).
    fn ref_entry(&self, slot: usize) -> &crate::ast::ScratchBuf {
        match self.ref_scratch.iter().find(|(s, _)| *s == slot) {
            Some(&(_, idx)) => &self.scratch[idx],
            None if self.ref_slots.get(slot).copied().unwrap_or(false) => panic!(
                "slot {slot} is a Ref pair owned by the CALLER (a kernel \
                 input) — read it on the caller side"
            ),
            None => panic!("slot {slot} is not a Ref2-colored slot"),
        }
    }
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
                crate::compile::jit::invoke_with_catch(move || {
                    unsafe { (code_fn)(buf_const, buf_mut); }
                });
            }
            HybridStep::Closure(cs) => {
                for (i, &slot) in cs.input_slots.iter().enumerate() {
                    core.gather_buf[i] = core.buffer[slot];
                }
                match &cs.op {
                    ClosureOp::U64(op) => op(
                        &core.gather_buf[..cs.input_slots.len()],
                        &mut core.scatter_buf[..cs.output_slots.len()],
                    ),
                    ClosureOp::Slot(op) => op(
                        &core.gather_buf[..cs.input_slots.len()],
                        &mut core.scatter_buf[..cs.output_slots.len()],
                        &mut core.scratch[cs.scratch_range.0..cs.scratch_range.1],
                    ),
                }
                for (i, &slot) in cs.output_slots.iter().enumerate() {
                    core.buffer[slot] = core.scatter_buf[i];
                }
            }
        }
    }
    #[cfg(debug_assertions)]
    core.validate_refs();
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
    for (i, slot) in slot_provenance.iter_mut().enumerate().take(coord_count.min(64)) {
        *slot = 1u64 << i;
    }
    for (step_idx, step) in steps.iter().enumerate() {
        // Only closure steps carry explicit output_slots; JIT steps use the
        // same buffer region but slot assignment is managed by the JIT code.
        // (Under `not(feature = "jit")` Closure is the only variant, making
        // this pattern irrefutable — that's fine, not a bug.)
        #[allow(irrefutable_let_patterns)]
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
        self.core.guard_ref_slot(slot);
        self.eval(coords);
        self.core.buffer[slot]
    }

    /// Read a named output after `eval()`. Panics on Ref2 slots
    /// (axiom S2) — use `read_vec_*`.
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        let slot = self.core.output_map[name];
        self.core.guard_ref_slot(slot);
        self.core.buffer[slot]
    }

    /// Read by slot index. Panics on Ref2 slots (axiom S2) —
    /// use `read_vec_*`.
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.core.guard_ref_slot(slot);
        self.core.buffer[slot]
    }

    crate::compile::ref_readers!();

    /// Number of coordinate inputs.
    pub fn coord_count(&self) -> usize { self.core.coord_count }

    /// Resolve an output name to its buffer slot.
    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.core.output_map.get(name).copied()
    }

    /// Store owned nodes to keep JIT-baked pointers valid.
    pub fn retain_nodes(&mut self, nodes: Vec<Box<dyn PolydatNode>>) {
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
        for (i, &c) in coords.iter().enumerate().take(self.core.coord_count) {
            if self.core.buffer[i] != c {
                self.core.buffer[i] = c;
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
        self.core.guard_ref_slot(slot);
        self.set_inputs(coords);
        if slot < self.slot_provenance.len()
            && self.slot_provenance[slot] & self.changed_mask == 0 {
                return self.core.buffer[slot];
            }
        eval_all_hybrid_steps(&mut self.core);
        self.core.buffer[slot]
    }

    /// Read a named output after `eval()`. Panics on Ref2 slots
    /// (axiom S2) — use `read_vec_*`.
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        let slot = self.core.output_map[name];
        self.core.guard_ref_slot(slot);
        self.core.buffer[slot]
    }

    /// Read by slot index. Panics on Ref2 slots (axiom S2) —
    /// use `read_vec_*`.
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.core.guard_ref_slot(slot);
        self.core.buffer[slot]
    }

    crate::compile::ref_readers!();

    /// Number of coordinate inputs.
    pub fn coord_count(&self) -> usize { self.core.coord_count }

    /// Resolve an output name to its buffer slot.
    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.core.output_map.get(name).copied()
    }

    /// Store owned nodes to keep JIT-baked pointers valid.
    pub fn retain_nodes(&mut self, nodes: Vec<Box<dyn PolydatNode>>) {
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
        for (i, &c) in coords.iter().enumerate().take(self.core.coord_count) {
            if self.core.buffer[i] != c {
                self.core.buffer[i] = c;
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
                    crate::compile::jit::invoke_with_catch(move || {
                        unsafe { (code_fn)(buf_const, buf_mut); }
                    });
                }
                HybridStep::Closure(cs) => {
                    for (i, &slot) in cs.input_slots.iter().enumerate() {
                        self.core.gather_buf[i] = self.core.buffer[slot];
                    }
                    match &cs.op {
                        ClosureOp::U64(op) => op(
                            &self.core.gather_buf[..cs.input_slots.len()],
                            &mut self.core.scatter_buf[..cs.output_slots.len()],
                        ),
                        ClosureOp::Slot(op) => op(
                            &self.core.gather_buf[..cs.input_slots.len()],
                            &mut self.core.scatter_buf[..cs.output_slots.len()],
                            &mut self.core.scratch[cs.scratch_range.0..cs.scratch_range.1],
                        ),
                    }
                    for (i, &slot) in cs.output_slots.iter().enumerate() {
                        self.core.buffer[slot] = self.core.scatter_buf[i];
                    }
                }
            }
            self.step_clean[step_idx] = true;
        }
        #[cfg(debug_assertions)]
        self.core.validate_refs();
    }

    /// Cone guard + push-side skip: the full optimization.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.core.guard_ref_slot(slot);
        self.set_inputs(coords);
        if slot < self.slot_provenance.len()
            && self.slot_provenance[slot] & self.changed_mask == 0 {
                return self.core.buffer[slot];
            }
        for (step_idx, step) in self.core.steps.iter().enumerate() {
            if self.step_clean[step_idx] { continue; }
            match step {
                #[cfg(feature = "jit")]
                HybridStep::Jit(seg) => {
                    let code_fn = seg.code_fn;
                    let buf_const = self.core.buffer.as_ptr();
                    let buf_mut = self.core.buffer.as_mut_ptr();
                    crate::compile::jit::invoke_with_catch(move || {
                        unsafe { (code_fn)(buf_const, buf_mut); }
                    });
                }
                HybridStep::Closure(cs) => {
                    for (i, &slot) in cs.input_slots.iter().enumerate() {
                        self.core.gather_buf[i] = self.core.buffer[slot];
                    }
                    match &cs.op {
                        ClosureOp::U64(op) => op(
                            &self.core.gather_buf[..cs.input_slots.len()],
                            &mut self.core.scatter_buf[..cs.output_slots.len()],
                        ),
                        ClosureOp::Slot(op) => op(
                            &self.core.gather_buf[..cs.input_slots.len()],
                            &mut self.core.scatter_buf[..cs.output_slots.len()],
                            &mut self.core.scratch[cs.scratch_range.0..cs.scratch_range.1],
                        ),
                    }
                    for (i, &slot) in cs.output_slots.iter().enumerate() {
                        self.core.buffer[slot] = self.core.scatter_buf[i];
                    }
                }
            }
            self.step_clean[step_idx] = true;
        }
        #[cfg(debug_assertions)]
        self.core.validate_refs();
        self.core.buffer[slot]
    }

    /// Read a named output after `eval()`. Panics on Ref2 slots
    /// (axiom S2) — use `read_vec_*`.
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        let slot = self.core.output_map[name];
        self.core.guard_ref_slot(slot);
        self.core.buffer[slot]
    }

    /// Read by slot index. Panics on Ref2 slots (axiom S2) —
    /// use `read_vec_*`.
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.core.guard_ref_slot(slot);
        self.core.buffer[slot]
    }

    crate::compile::ref_readers!();

    /// Number of coordinate inputs.
    pub fn coord_count(&self) -> usize { self.core.coord_count }

    /// Resolve an output name to its buffer slot.
    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.core.output_map.get(name).copied()
    }

    /// Store owned nodes to keep JIT-baked pointers valid.
    pub fn retain_nodes(&mut self, nodes: Vec<Box<dyn PolydatNode>>) {
        self.core._nodes = nodes;
    }
}

/// Type alias for the default hybrid kernel (PushPull — full optimization).
///
/// Assembler and bench code that references `HybridKernel` uses the full
/// push+pull variant. Rename uses to the concrete type if different
/// optimization trade-offs are needed.
pub type HybridKernel = HybridKernelPushPull;

/// Flattened slot list for one node's wire inputs under per-port
/// widths (type_system_alignment.md §8.4 layer 1): every source
/// contributes `slot_width` consecutive slots.
fn flatten_input_slots(
    wiring: &[Vec<WireSource>],
    nodes: &[Box<dyn PolydatNode>],
    node_idx: usize,
    port_offsets: &[Vec<usize>],
    input_starts: &[usize],
    input_widths: &[usize],
) -> Vec<usize> {
    let mut slots = Vec::new();
    for source in &wiring[node_idx] {
        let (start, w) = match source {
            WireSource::Input(c) => (
                input_starts.get(*c).copied().unwrap_or(*c),
                input_widths.get(*c).copied().unwrap_or(1),
            ),
            WireSource::NodeOutput(u, p) => (
                port_offsets[*u][*p],
                nodes[*u].meta().outs[*p].typ.slot_width(),
            ),
        };
        slots.extend(start..start + w);
    }
    slots
}

/// First slot of each Ref2-colored output port of one node, in
/// port order (axiom S3 pairing with CompiledSlotKit scratch).
fn flatten_ref_output_starts(
    nodes: &[Box<dyn PolydatNode>],
    node_idx: usize,
    port_offsets: &[Vec<usize>],
) -> Vec<usize> {
    nodes[node_idx]
        .meta()
        .outs
        .iter()
        .enumerate()
        .filter(|(_, out)| out.typ.slot_color() == crate::ast::SlotColor::Ref2)
        .map(|(p, _)| port_offsets[node_idx][p])
        .collect()
}

/// Flattened slot list for one node's outputs.
fn flatten_output_slots(
    nodes: &[Box<dyn PolydatNode>],
    node_idx: usize,
    port_offsets: &[Vec<usize>],
) -> Vec<usize> {
    let mut slots = Vec::new();
    for (p, out) in nodes[node_idx].meta().outs.iter().enumerate() {
        let start = port_offsets[node_idx][p];
        slots.extend(start..start + out.typ.slot_width());
    }
    slots
}

/// Build a hybrid kernel from resolved DAG data.
///
/// Each node is classified: if it can be JIT-compiled, it goes into
/// a JIT segment. If not, it becomes a closure step. Adjacent JIT-able
/// nodes are batched into a single JIT segment for efficiency.
///
/// Returns a `HybridKernelPushPull` (the production default).
#[cfg(feature = "jit")]
#[allow(clippy::too_many_arguments)]
pub fn build_hybrid(
    nodes: &[Box<dyn PolydatNode>],
    wiring: &[Vec<WireSource>],
    coord_count: usize,
    total_slots: usize,
    port_offsets: &[Vec<usize>],
    input_starts: &[usize],
    input_widths: &[usize],
    output_map: HashMap<String, usize>,
    ref_slots: Vec<bool>,
) -> Result<HybridKernelPushPull, String> {
    let mut steps: Vec<HybridStep> = Vec::new();
    let mut scratch: Vec<crate::ast::ScratchBuf> = Vec::new();
    let mut ref_scratch: Vec<(usize, usize)> = Vec::new();
    let mut max_inputs = 0usize;
    let mut max_outputs = 0usize;

    // Classify each node
    let classifications: Vec<(JitOp, Vec<usize>, Vec<usize>)> = nodes.iter()
        .enumerate()
        .map(|(node_idx, node)| {
            let jit_op = jit::classify_node(node.as_ref());

            let input_slots = flatten_input_slots(
                wiring, nodes, node_idx, port_offsets, input_starts, input_widths,
            );
            let output_slots = flatten_output_slots(nodes, node_idx, port_offsets);

            max_inputs = max_inputs.max(input_slots.len());
            max_outputs = max_outputs.max(output_slots.len());

            (jit_op, input_slots, output_slots)
        })
        .collect();

    // Batch adjacent JIT-able nodes into segments
    let mut i = 0;
    while i < classifications.len() {
        if matches!(classifications[i].0, JitOp::Fallback) {
            // This node needs a closure — scalar u64 op preferred,
            // slot op for slice-bearing nodes (§8.4 layer 3).
            let node = &nodes[i];
            let (_, ref input_slots, ref output_slots) = classifications[i];
            let scratch_start = scratch.len();
            let op = if let Some(op) = node.compiled_u64() {
                ClosureOp::U64(op)
            } else if let Some(kit) = node.compiled_slot() {
                scratch.extend(kit.scratch.iter().map(|e| crate::ast::ScratchBuf::new(*e)));
                // Axiom S9(a): map this step's Ref output pairs to
                // its scratch entries (port order — S3 contract).
                let starts = flatten_ref_output_starts(nodes, i, port_offsets);
                assert_eq!(starts.len(), kit.scratch.len(),
                    "slot-op scratch/Ref-output mismatch on '{}'",
                    node.meta().name);
                for (k, &slot) in starts.iter().enumerate() {
                    ref_scratch.push((slot, scratch_start + k));
                }
                ClosureOp::Slot(kit.op)
            } else {
                return Err(format!(
                    "node '{}' has no compiled form and can't be JIT-compiled",
                    node.meta().name
                ));
            };
            steps.push(HybridStep::Closure(ClosureStep {
                op,
                input_slots: input_slots.clone(),
                output_slots: output_slots.clone(),
                scratch_range: (scratch_start, scratch.len()),
            }));
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
            for (jit_op, input_slots, output_slots) in &classifications[batch_start..i] {
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
        steps, scratch, ref_scratch, ref_slots, wiring, nodes, coord_count,
        total_slots, output_map, max_inputs, max_outputs, input_starts,
        input_widths,
    )
}

/// Build a hybrid kernel without JIT (all closures).
#[cfg(not(feature = "jit"))]
#[allow(clippy::too_many_arguments)]
pub fn build_hybrid(
    nodes: &[Box<dyn PolydatNode>],
    wiring: &[Vec<WireSource>],
    coord_count: usize,
    total_slots: usize,
    port_offsets: &[Vec<usize>],
    input_starts: &[usize],
    input_widths: &[usize],
    output_map: HashMap<String, usize>,
    ref_slots: Vec<bool>,
) -> Result<HybridKernelPushPull, String> {
    let mut steps: Vec<HybridStep> = Vec::new();
    let mut scratch: Vec<crate::ast::ScratchBuf> = Vec::new();
    let mut ref_scratch: Vec<(usize, usize)> = Vec::new();
    let mut max_inputs = 0usize;
    let mut max_outputs = 0usize;

    for (node_idx, node) in nodes.iter().enumerate() {
        let input_slots = flatten_input_slots(
            wiring, nodes, node_idx, port_offsets, input_starts, input_widths,
        );
        let output_slots = flatten_output_slots(nodes, node_idx, port_offsets);

        max_inputs = max_inputs.max(input_slots.len());
        max_outputs = max_outputs.max(output_slots.len());

        let scratch_start = scratch.len();
        let op = if let Some(op) = node.compiled_u64() {
            ClosureOp::U64(op)
        } else if let Some(kit) = node.compiled_slot() {
            scratch.extend(kit.scratch.iter().map(|e| crate::ast::ScratchBuf::new(*e)));
            // Axiom S9(a): map this step's Ref output pairs to
            // its scratch entries (port order — S3 contract).
            let starts = flatten_ref_output_starts(nodes, node_idx, port_offsets);
            assert_eq!(starts.len(), kit.scratch.len(),
                "slot-op scratch/Ref-output mismatch on '{}'",
                node.meta().name);
            for (k, &slot) in starts.iter().enumerate() {
                ref_scratch.push((slot, scratch_start + k));
            }
            ClosureOp::Slot(kit.op)
        } else {
            return Err(format!("node '{}' has no compiled form", node.meta().name));
        };
        steps.push(HybridStep::Closure(ClosureStep {
            op,
            input_slots,
            output_slots,
            scratch_range: (scratch_start, scratch.len()),
        }));
    }

    build_pushpull_from_steps(
        steps, scratch, ref_scratch, ref_slots, wiring, nodes, coord_count,
        total_slots, output_map, max_inputs, max_outputs, input_starts,
        input_widths,
    )
}

/// Shared construction of `HybridKernelPushPull` from assembled steps.
///
/// Computes provenance bitmasks from the DAG wiring and builds the
/// step_dependents list for push-side invalidation and the slot_provenance
/// table for pull-side cone guard.
#[allow(clippy::too_many_arguments)]
fn build_pushpull_from_steps(
    steps: Vec<HybridStep>,
    scratch: Vec<crate::ast::ScratchBuf>,
    ref_scratch: Vec<(usize, usize)>,
    ref_slots: Vec<bool>,
    wiring: &[Vec<WireSource>],
    nodes: &[Box<dyn PolydatNode>],
    coord_count: usize,
    total_slots: usize,
    output_map: HashMap<String, usize>,
    max_inputs: usize,
    max_outputs: usize,
    _input_starts: &[usize],
    input_widths: &[usize],
) -> Result<HybridKernelPushPull, String> {
    let step_count = steps.len();

    // Compute per-node provenance and invert into per-input step dependents.
    // Since each step currently maps to one node, step index == node index.
    // Dependents come back per-INPUT; expand to per-SLOT so the kernels'
    // slot-indexed dirty tracking / changed-mask bits stay coherent under
    // multi-slot inputs (§8.4 layer 1). Identity for all-scalar inputs.
    let node_provenance = crate::kernel::PolydatProgram::compute_provenance(nodes, wiring);
    let input_dependents = crate::kernel::PolydatProgram::compute_dependents(
        &node_provenance, input_widths.len());
    let step_dependents: Vec<Vec<usize>> = input_widths
        .iter()
        .enumerate()
        .flat_map(|(i, w)| {
            std::iter::repeat_n(input_dependents.get(i).cloned().unwrap_or_default(), *w)
        })
        .collect();

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
            scratch,
            ref_slots,
            ref_scratch,
            _nodes: Vec::new(),
        },
        step_clean: vec![false; step_count],
        input_dependents: step_dependents,
        slot_provenance,
        changed_mask: u64::MAX, // all dirty on first eval
    })
}
