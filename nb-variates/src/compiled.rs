// Copyright 2024-2026 nosqlbench contributors
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

use crate::node::CompiledU64Op;

/// A single evaluation step in the compiled kernel.
struct CompiledStep {
    op: CompiledU64Op,
    input_slots: Vec<usize>,
    output_slots: Vec<usize>,
}

/// Common fields shared by all kernel variants.
struct KernelCore {
    buffer: Vec<u64>,
    coord_count: usize,
    steps: Vec<CompiledStep>,
    output_map: HashMap<String, usize>,
    gather_buf: Vec<u64>,
    scatter_buf: Vec<u64>,
}

/// Build kernel core from raw step data.
fn build_core(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(CompiledU64Op, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
) -> KernelCore {
    let max_inputs = steps.iter().map(|s| s.1.len()).max().unwrap_or(0);
    let max_outputs = steps.iter().map(|s| s.2.len()).max().unwrap_or(0);
    let compiled_steps: Vec<CompiledStep> = steps.into_iter()
        .map(|(op, input_slots, output_slots)| CompiledStep { op, input_slots, output_slots })
        .collect();
    KernelCore {
        buffer: vec![0u64; total_slots],
        coord_count,
        steps: compiled_steps,
        output_map,
        gather_buf: vec![0u64; max_inputs],
        scatter_buf: vec![0u64; max_outputs],
    }
}

/// Compute per-slot provenance bitmasks from input_dependents.
///
/// Returns `slot_provenance[slot]` = bitmask of which inputs affect
/// that buffer slot. Used by pull-side cone guard.
fn compute_slot_provenance(
    coord_count: usize,
    total_slots: usize,
    input_dependents: &[Vec<usize>],
    steps: &[CompiledStep],
) -> Vec<u64> {
    let step_count = steps.len();
    let mut step_prov = vec![0u64; step_count];
    for (input_idx, deps) in input_dependents.iter().enumerate() {
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
        for &slot in &step.output_slots {
            if slot < slot_provenance.len() {
                slot_provenance[slot] = step_prov[step_idx];
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

        /// Read an output by pre-resolved slot index.
        #[inline]
        pub fn get_slot(&self, slot: usize) -> u64 {
            self.core.buffer[slot]
        }

        /// Read a named output variate after `eval()`.
        #[inline]
        pub fn get(&self, name: &str) -> u64 {
            self.core.buffer[self.core.output_map[name]]
        }
    };
}

/// Run all steps unconditionally (no clean checks).
#[inline]
fn eval_all_steps(core: &mut KernelCore) {
    for step in &core.steps {
        for (i, &s) in step.input_slots.iter().enumerate() {
            core.gather_buf[i] = core.buffer[s];
        }
        (step.op)(
            &core.gather_buf[..step.input_slots.len()],
            &mut core.scatter_buf[..step.output_slots.len()],
        );
        for (i, &s) in step.output_slots.iter().enumerate() {
            core.buffer[s] = core.scatter_buf[i];
        }
    }
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
        steps: Vec<(CompiledU64Op, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
    ) -> Self {
        Self { core: build_core(coord_count, total_slots, steps, output_map) }
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
        steps: Vec<(CompiledU64Op, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
        input_dependents: Vec<Vec<usize>>,
    ) -> Self {
        let step_count = steps.len();
        Self {
            core: build_core(coord_count, total_slots, steps, output_map),
            node_clean: vec![false; step_count],
            input_dependents,
        }
    }

    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        for i in 0..coords.len().min(self.core.coord_count) {
            if self.core.buffer[i] != coords[i] {
                self.core.buffer[i] = coords[i];
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
            (step.op)(
                &self.core.gather_buf[..step.input_slots.len()],
                &mut self.core.scatter_buf[..step.output_slots.len()],
            );
            for (i, &s) in step.output_slots.iter().enumerate() {
                self.core.buffer[s] = self.core.scatter_buf[i];
            }
            self.node_clean[step_idx] = true;
        }
    }

    /// Eval + return a specific slot. No cone guard — always enters eval loop.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
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
    slot_provenance: Vec<u64>,
    changed_mask: u64,
}

impl CompiledKernelPull {
    pub(crate) fn new(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<(CompiledU64Op, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
        input_dependents: &[Vec<usize>],
    ) -> Self {
        let core = build_core(coord_count, total_slots, steps, output_map);
        let slot_provenance = compute_slot_provenance(
            coord_count, total_slots, input_dependents, &core.steps);
        Self {
            core,
            slot_provenance,
            changed_mask: u64::MAX, // all dirty initially
        }
    }

    /// Track which inputs changed (for cone guard). Does NOT mark
    /// individual nodes dirty — there is no per-node clean state.
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
        self.set_inputs(coords);
        if slot < self.slot_provenance.len() {
            if self.slot_provenance[slot] & self.changed_mask == 0 {
                return self.core.buffer[slot];
            }
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
    slot_provenance: Vec<u64>,
    changed_mask: u64,
}

impl CompiledKernelPushPull {
    pub(crate) fn new(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<(CompiledU64Op, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
        input_dependents: Vec<Vec<usize>>,
    ) -> Self {
        let step_count = steps.len();
        let core = build_core(coord_count, total_slots, steps, output_map);
        let slot_provenance = compute_slot_provenance(
            coord_count, total_slots, &input_dependents, &core.steps);
        Self {
            core,
            node_clean: vec![false; step_count],
            input_dependents,
            slot_provenance,
            changed_mask: u64::MAX,
        }
    }

    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        self.changed_mask = 0;
        for i in 0..coords.len().min(self.core.coord_count) {
            if self.core.buffer[i] != coords[i] {
                self.core.buffer[i] = coords[i];
                self.changed_mask |= 1u64 << i;
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
            (step.op)(
                &self.core.gather_buf[..step.input_slots.len()],
                &mut self.core.scatter_buf[..step.output_slots.len()],
            );
            for (i, &s) in step.output_slots.iter().enumerate() {
                self.core.buffer[s] = self.core.scatter_buf[i];
            }
            self.node_clean[step_idx] = true;
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
            if self.node_clean[step_idx] { continue; }
            for (i, &s) in step.input_slots.iter().enumerate() {
                self.core.gather_buf[i] = self.core.buffer[s];
            }
            (step.op)(
                &self.core.gather_buf[..step.input_slots.len()],
                &mut self.core.scatter_buf[..step.output_slots.len()],
            );
            for (i, &s) in step.output_slots.iter().enumerate() {
                self.core.buffer[s] = self.core.scatter_buf[i];
            }
            self.node_clean[step_idx] = true;
        }
        self.core.buffer[slot]
    }

    kernel_accessors!();
}

