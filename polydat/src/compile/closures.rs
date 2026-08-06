// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Phase 2: compiled u64-only kernels with flat buffer evaluation.
//!
//! Four monomorphic kernel types, each produced by a distinct compiler
//! path. No runtime branching for optimization strategy — the eval
//! loop is baked in at construction time.
//!
//! | Type | Push (per-node skip) | Pull (cone guard) |
//! |------|---------------------|-------------------|
//! | `CompiledKernelRaw` | — | — |
//! | `CompiledKernelPush` | yes | — |
//! | `CompiledKernelPull` | — | yes |
//! | `CompiledKernelPushPull` | yes | yes |

use std::collections::HashMap;

use crate::ast::{CompiledSlotOp, CompiledU64Op, ScratchBuf, ScratchElem};

/// A single evaluation step in the compiled kernel.
/// A compiled step's op: pure-scalar u64 closure, or a slot op
/// with kernel-owned scratch for typed-slice ports (§8.4 L3).
pub(crate) enum StepOp {
    U64(CompiledU64Op),
    Slot(CompiledSlotOp),
}

/// One compiled step plus its slice of the scratch arena.
pub(crate) struct P2Step {
    pub(crate) op: StepOp,
    pub(crate) input_slots: Vec<usize>,
    pub(crate) output_slots: Vec<usize>,
    /// Scratch element declarations (consumed by build_core).
    pub(crate) scratch: Vec<ScratchElem>,
    /// First slot of each Ref2-colored output port, in port
    /// order — zipped with the scratch entries to build the
    /// slot→arena map for axiom S9(a)'s validator and the S2
    /// accessors.
    pub(crate) ref_output_starts: Vec<usize>,
}

struct CompiledStep {
    op: StepOp,
    input_slots: Vec<usize>,
    output_slots: Vec<usize>,
    scratch_range: (usize, usize),
}

/// Common fields shared by all kernel variants.
struct KernelCore {
    buffer: Vec<u64>,
    coord_count: usize,
    steps: Vec<CompiledStep>,
    output_map: HashMap<String, usize>,
    gather_buf: Vec<u64>,
    scatter_buf: Vec<u64>,
    /// Kernel-owned vector storage; vector-producing ports'
    /// (ptr, len) slots view entries here (§8.4 layer 3).
    scratch: Vec<ScratchBuf>,
    /// Axiom S2: per-slot Ref2 mask — the raw readers panic on
    /// these instead of leaking addresses.
    ref_slots: Vec<bool>,
    /// Axiom S9(a): (first slot of a Ref pair → scratch arena
    /// index) for every scratch-backed Ref output.
    ref_scratch: Vec<(usize, usize)>,
}

impl KernelCore {
    /// Axiom S9(a) — deterministic Ref validation: every
    /// scratch-backed Ref pair in the buffer must equal its
    /// owning entry's current `(as_ptr(), len())`. Run after
    /// every eval in debug/test builds; a violation names the
    /// slot instead of dangling. Gated to `debug_assertions` to
    /// match its call sites, which compile out in release.
    #[cfg(debug_assertions)]
    fn validate_refs(&self) {
        for &(slot, idx) in &self.ref_scratch {
            let (p, l) = self.scratch[idx].ptr_len();
            assert!(
                self.buffer[slot] == p && self.buffer[slot + 1] == l,
                "S9 ref-validator: slot pair ({slot}, {}) = ({:#x}, {}) \
                 does not match scratch[{idx}] = ({p:#x}, {l}) — a slot \
                 op failed to republish or wrote the wrong slots",
                slot + 1,
                self.buffer[slot],
                self.buffer[slot + 1],
            );
        }
    }

    /// Axiom S2 guard for the raw u64 readers.
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

    /// Axiom S2 typed accessor core: resolve a Ref pair's first
    /// slot to its kernel-owned scratch entry. The returned
    /// borrow ties to `&self`, so holding it across the next
    /// `eval(&mut self)` is a compile error — stale reads are
    /// statically impossible.
    fn ref_entry(&self, slot: usize) -> &ScratchBuf {
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

/// Build kernel core from raw step data.
fn build_core(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<P2Step>,
    output_map: HashMap<String, usize>,
    ref_slots: Vec<bool>,
) -> KernelCore {
    let max_inputs = steps.iter().map(|s| s.input_slots.len()).max().unwrap_or(0);
    let max_outputs = steps.iter().map(|s| s.output_slots.len()).max().unwrap_or(0);
    let mut scratch: Vec<ScratchBuf> = Vec::new();
    let mut ref_scratch: Vec<(usize, usize)> = Vec::new();
    let compiled_steps: Vec<CompiledStep> = steps.into_iter()
        .map(|step| {
            let start = scratch.len();
            scratch.extend(step.scratch.iter().map(|e| ScratchBuf::new(*e)));
            // Axiom S3: one scratch entry per Ref output, in port
            // order — the CompiledSlotKit contract. A mismatch is
            // a macro/builder bug, caught at construction.
            assert_eq!(
                step.ref_output_starts.len(),
                step.scratch.len(),
                "slot-op step declares {} scratch entries for {} Ref \
                 output ports",
                step.scratch.len(),
                step.ref_output_starts.len(),
            );
            for (k, &slot) in step.ref_output_starts.iter().enumerate() {
                ref_scratch.push((slot, start + k));
            }
            CompiledStep {
                op: step.op,
                input_slots: step.input_slots,
                output_slots: step.output_slots,
                scratch_range: (start, scratch.len()),
            }
        })
        .collect();
    KernelCore {
        buffer: vec![0u64; total_slots],
        coord_count,
        steps: compiled_steps,
        output_map,
        gather_buf: vec![0u64; max_inputs],
        scatter_buf: vec![0u64; max_outputs],
        scratch,
        ref_slots,
        ref_scratch,
    }
}

/// Compute per-slot provenance bitmasks from input_dependents.
///
/// Returns `slot_provenance[slot]` = exact multi-word mask of which inputs affect
/// that buffer slot. Used by pull-side cone guard.
fn compute_slot_provenance(
    coord_count: usize,
    total_slots: usize,
    input_dependents: &[Vec<usize>],
    steps: &[CompiledStep],
) -> Vec<crate::kernel::ProvMask> {
    let step_count = steps.len();
    let mut step_prov: Vec<crate::kernel::ProvMask> =
        (0..step_count).map(|_| crate::kernel::ProvMask::empty()).collect();
    for (input_idx, deps) in input_dependents.iter().enumerate() {
        for &step_idx in deps {
            if step_idx < step_count {
                step_prov[step_idx].set(input_idx);
            }
        }
    }
    let mut slot_provenance: Vec<crate::kernel::ProvMask> =
        (0..total_slots).map(|_| crate::kernel::ProvMask::empty()).collect();
    for (i, slot) in slot_provenance.iter_mut().enumerate().take(coord_count) {
        slot.set(i);
    }
    for (step_idx, step) in steps.iter().enumerate() {
        for &slot in &step.output_slots {
            if slot < slot_provenance.len() {
                slot_provenance[slot] = step_prov[step_idx].clone();
            }
        }
    }
    slot_provenance
}

// ── Shared accessor methods ────────────────────────────────────

macro_rules! kernel_accessors {
    () => {
        pub fn coord_count(&self) -> usize { self.core.coord_count }

        pub fn resolve_output(&self, name: &str) -> Option<usize> {
            self.core.output_map.get(name).copied()
        }

        pub fn output_names(&self) -> Vec<&str> {
            self.core.output_map.keys().map(|s| s.as_str()).collect()
        }

        /// Read an output by pre-resolved slot index. Panics on
        /// Ref2-colored slots (axiom S2) — use `read_vec_*`.
        #[inline]
        pub fn get_slot(&self, slot: usize) -> u64 {
            self.core.guard_ref_slot(slot);
            self.core.buffer[slot]
        }

        /// Read a named output variate after `eval()`. Panics on
        /// Ref2-colored outputs (axiom S2) — use `read_vec_*`.
        #[inline]
        pub fn get(&self, name: &str) -> u64 {
            let slot = self.core.output_map[name];
            self.core.guard_ref_slot(slot);
            self.core.buffer[slot]
        }

        crate::compile::ref_readers!();
    };
}

/// Run all steps unconditionally (no clean checks).
#[inline]
fn eval_all_steps(core: &mut KernelCore) {
    for step in &core.steps {
        for (i, &s) in step.input_slots.iter().enumerate() {
            core.gather_buf[i] = core.buffer[s];
        }
        match &step.op {
            StepOp::U64(op) => op(
                &core.gather_buf[..step.input_slots.len()],
                &mut core.scatter_buf[..step.output_slots.len()],
            ),
            StepOp::Slot(op) => op(
                &core.gather_buf[..step.input_slots.len()],
                &mut core.scatter_buf[..step.output_slots.len()],
                &mut core.scratch[step.scratch_range.0..step.scratch_range.1],
            ),
        }
        for (i, &s) in step.output_slots.iter().enumerate() {
            core.buffer[s] = core.scatter_buf[i];
        }
    }
    #[cfg(debug_assertions)]
    core.validate_refs();
}

// ═══════════════════════════════════════════════════════════════
// Raw: no provenance, no cone guard. Eval runs all steps.
// ═══════════════════════════════════════════════════════════════

pub struct CompiledKernelRaw {
    core: KernelCore,
}

impl CompiledKernelRaw {
    pub(crate) fn new(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<P2Step>,
        output_map: HashMap<String, usize>,
        ref_slots: Vec<bool>,
    ) -> Self {
        Self { core: build_core(coord_count, total_slots, steps, output_map, ref_slots) }
    }

    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.core.buffer[..self.core.coord_count.min(coords.len())]
            .copy_from_slice(&coords[..self.core.coord_count.min(coords.len())]);
        eval_all_steps(&mut self.core);
    }

    /// Eval + return a specific slot. No cone guard — always evaluates.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.core.guard_ref_slot(slot);
        self.eval(coords);
        self.core.buffer[slot]
    }

    kernel_accessors!();
}

// ═══════════════════════════════════════════════════════════════
// Push: per-node dirty skip, no cone guard.
// set_inputs marks dependents dirty. eval skips clean steps.
// ═══════════════════════════════════════════════════════════════

pub struct CompiledKernelPush {
    core: KernelCore,
    node_clean: Vec<bool>,
    input_dependents: Vec<Vec<usize>>,
}

impl CompiledKernelPush {
    pub(crate) fn new(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<P2Step>,
        output_map: HashMap<String, usize>,
        input_dependents: Vec<Vec<usize>>,
        ref_slots: Vec<bool>,
    ) -> Self {
        let step_count = steps.len();
        Self {
            core: build_core(coord_count, total_slots, steps, output_map, ref_slots),
            node_clean: vec![false; step_count],
            input_dependents,
        }
    }

    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        for (i, &c) in coords.iter().enumerate().take(self.core.coord_count) {
            if self.core.buffer[i] != c {
                self.core.buffer[i] = c;
                if i < self.input_dependents.len() {
                    for &step_idx in &self.input_dependents[i] {
                        self.node_clean[step_idx] = false;
                    }
                }
            }
        }
    }

    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        for (step_idx, step) in self.core.steps.iter().enumerate() {
            if self.node_clean[step_idx] { continue; }
            for (i, &s) in step.input_slots.iter().enumerate() {
                self.core.gather_buf[i] = self.core.buffer[s];
            }
            match &step.op {
                StepOp::U64(op) => op(
                    &self.core.gather_buf[..step.input_slots.len()],
                    &mut self.core.scatter_buf[..step.output_slots.len()],
                ),
                StepOp::Slot(op) => op(
                    &self.core.gather_buf[..step.input_slots.len()],
                    &mut self.core.scatter_buf[..step.output_slots.len()],
                    &mut self.core.scratch[step.scratch_range.0..step.scratch_range.1],
                ),
            }
            for (i, &s) in step.output_slots.iter().enumerate() {
                self.core.buffer[s] = self.core.scatter_buf[i];
            }
            self.node_clean[step_idx] = true;
        }
        #[cfg(debug_assertions)]
        self.core.validate_refs();
    }

    /// Eval + return a specific slot. No cone guard — always enters eval loop.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.core.guard_ref_slot(slot);
        self.eval(coords);
        self.core.buffer[slot]
    }

    kernel_accessors!();
}

// ═══════════════════════════════════════════════════════════════
// Pull: cone guard only, no per-node skip.
// set_inputs tracks changed_mask. eval_for_slot checks cone
// then runs ALL steps if dirty.
// ═══════════════════════════════════════════════════════════════

pub struct CompiledKernelPull {
    core: KernelCore,
    slot_provenance: Vec<crate::kernel::ProvMask>,
    changed_mask: crate::kernel::ProvMask,
}

impl CompiledKernelPull {
    pub(crate) fn new(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<P2Step>,
        output_map: HashMap<String, usize>,
        input_dependents: &[Vec<usize>],
        ref_slots: Vec<bool>,
    ) -> Self {
        let core = build_core(coord_count, total_slots, steps, output_map, ref_slots);
        let slot_provenance = compute_slot_provenance(
            coord_count, total_slots, input_dependents, &core.steps);
        Self {
            core,
            slot_provenance,
            changed_mask: crate::kernel::ProvMask::all_below(coord_count), // all dirty initially
        }
    }

    /// Track which inputs changed (for cone guard). Does NOT mark
    /// individual nodes dirty — there is no per-node clean state.
    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        self.changed_mask.clear();
        for (i, &c) in coords.iter().enumerate().take(self.core.coord_count) {
            if self.core.buffer[i] != c {
                self.core.buffer[i] = c;
                self.changed_mask.set(i);
            }
        }
    }

    /// Evaluate eagerly (no cone guard). Runs all steps.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        eval_all_steps(&mut self.core);
    }

    /// Cone guard: if the output's cone is clean, skip eval entirely.
    /// Otherwise run ALL steps (no per-node skip).
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.core.guard_ref_slot(slot);
        self.set_inputs(coords);
        if slot < self.slot_provenance.len()
            && !self.slot_provenance[slot].intersects(&self.changed_mask) {
                return self.core.buffer[slot];
            }
        eval_all_steps(&mut self.core);
        self.core.buffer[slot]
    }

    kernel_accessors!();
}

// ═══════════════════════════════════════════════════════════════
// PushPull: push-side per-node skip + pull-side cone guard.
// Full optimization.
// ═══════════════════════════════════════════════════════════════

pub struct CompiledKernelPushPull {
    core: KernelCore,
    node_clean: Vec<bool>,
    input_dependents: Vec<Vec<usize>>,
    slot_provenance: Vec<crate::kernel::ProvMask>,
    changed_mask: crate::kernel::ProvMask,
}

impl CompiledKernelPushPull {
    pub(crate) fn new(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<P2Step>,
        output_map: HashMap<String, usize>,
        input_dependents: Vec<Vec<usize>>,
        ref_slots: Vec<bool>,
    ) -> Self {
        let step_count = steps.len();
        let core = build_core(coord_count, total_slots, steps, output_map, ref_slots);
        let slot_provenance = compute_slot_provenance(
            coord_count, total_slots, &input_dependents, &core.steps);
        Self {
            core,
            node_clean: vec![false; step_count],
            input_dependents,
            slot_provenance,
            changed_mask: crate::kernel::ProvMask::all_below(coord_count),
        }
    }

    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        self.changed_mask.clear();
        for (i, &c) in coords.iter().enumerate().take(self.core.coord_count) {
            if self.core.buffer[i] != c {
                self.core.buffer[i] = c;
                self.changed_mask.set(i);
                if i < self.input_dependents.len() {
                    for &step_idx in &self.input_dependents[i] {
                        self.node_clean[step_idx] = false;
                    }
                }
            }
        }
    }

    /// Eval with push-side skip (no cone guard).
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        for (step_idx, step) in self.core.steps.iter().enumerate() {
            if self.node_clean[step_idx] { continue; }
            for (i, &s) in step.input_slots.iter().enumerate() {
                self.core.gather_buf[i] = self.core.buffer[s];
            }
            match &step.op {
                StepOp::U64(op) => op(
                    &self.core.gather_buf[..step.input_slots.len()],
                    &mut self.core.scatter_buf[..step.output_slots.len()],
                ),
                StepOp::Slot(op) => op(
                    &self.core.gather_buf[..step.input_slots.len()],
                    &mut self.core.scatter_buf[..step.output_slots.len()],
                    &mut self.core.scratch[step.scratch_range.0..step.scratch_range.1],
                ),
            }
            for (i, &s) in step.output_slots.iter().enumerate() {
                self.core.buffer[s] = self.core.scatter_buf[i];
            }
            self.node_clean[step_idx] = true;
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
            && !self.slot_provenance[slot].intersects(&self.changed_mask) {
                return self.core.buffer[slot];
            }
        for (step_idx, step) in self.core.steps.iter().enumerate() {
            if self.node_clean[step_idx] { continue; }
            for (i, &s) in step.input_slots.iter().enumerate() {
                self.core.gather_buf[i] = self.core.buffer[s];
            }
            match &step.op {
                StepOp::U64(op) => op(
                    &self.core.gather_buf[..step.input_slots.len()],
                    &mut self.core.scatter_buf[..step.output_slots.len()],
                ),
                StepOp::Slot(op) => op(
                    &self.core.gather_buf[..step.input_slots.len()],
                    &mut self.core.scatter_buf[..step.output_slots.len()],
                    &mut self.core.scratch[step.scratch_range.0..step.scratch_range.1],
                ),
            }
            for (i, &s) in step.output_slots.iter().enumerate() {
                self.core.buffer[s] = self.core.scatter_buf[i];
            }
            self.node_clean[step_idx] = true;
        }
        #[cfg(debug_assertions)]
        self.core.validate_refs();
        self.core.buffer[slot]
    }

    kernel_accessors!();
}

