// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Phase 2: compiled u64-only kernel with flat buffer evaluation.
//!
//! When all nodes in a DAG are u64-only and provide `compiled_u64()`
//! implementations, the assembly phase compiles the DAG into this form.
//!
//! The compiled kernel evaluates eagerly in topological order using a
//! flat `Vec<u64>` buffer. Each step is a closure that reads from and
//! writes to fixed slots in the buffer. There is no `Value` enum, no
//! virtual dispatch, and no per-node memoization overhead.

use std::collections::HashMap;

use crate::node::CompiledU64Op;

/// A single evaluation step in the compiled kernel.
struct CompiledStep {
    /// The compiled operation closure.
    op: CompiledU64Op,
    /// Buffer slot indices to gather as inputs.
    input_slots: Vec<usize>,
    /// Buffer slot indices to scatter outputs to.
    output_slots: Vec<usize>,
}

/// A compiled generation kernel for u64-only DAGs.
///
/// All wires are u64. Evaluation is eager: calling `eval` runs every
/// step in topological order and populates the entire buffer. Outputs
/// are then read by slot index.
pub struct CompiledKernel {
    /// Flat u64 buffer. Layout:
    ///   [0..coord_count)           — coordinate input slots
    ///   [coord_count..total_slots) — node output slots
    buffer: Vec<u64>,

    /// Number of coordinate slots at the front of the buffer.
    coord_count: usize,

    /// Steps to execute in topological order.
    steps: Vec<CompiledStep>,

    /// Map from output variate name to buffer slot index.
    output_map: HashMap<String, usize>,

    /// Scratch space for gathering inputs per step.
    gather_buf: Vec<u64>,

    /// Scratch space for scattering outputs per step.
    scatter_buf: Vec<u64>,

    /// Provenance caching state. Only present when built with
    /// `new_with_provenance`. When absent, eval is straight-through.
    provenance: Option<ProvenanceState>,
}

struct ProvenanceState {
    node_clean: Vec<bool>,
    input_dependents: Vec<Vec<usize>>,
    /// Per-slot provenance: which inputs affect each buffer slot.
    /// Used for pull-side guard: if the pulled slot's provenance
    /// doesn't intersect changed inputs, skip eval entirely.
    slot_provenance: Vec<u64>,
    /// Bitmask of inputs that changed since last eval.
    changed_mask: u64,
}

impl CompiledKernel {
    /// Create a compiled kernel without provenance (unconditional eval).
    #[allow(dead_code)]
    pub(crate) fn new(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<(CompiledU64Op, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
    ) -> Self {
        // Find the max input and output width across all steps
        let max_inputs = steps.iter().map(|s| s.1.len()).max().unwrap_or(0);
        let max_outputs = steps.iter().map(|s| s.2.len()).max().unwrap_or(0);

        let compiled_steps: Vec<CompiledStep> = steps
            .into_iter()
            .map(|(op, input_slots, output_slots)| CompiledStep {
                op,
                input_slots,
                output_slots,
            })
            .collect();

        Self {
            buffer: vec![0u64; total_slots],
            coord_count,
            steps: compiled_steps,
            output_map,
            gather_buf: vec![0u64; max_inputs],
            scatter_buf: vec![0u64; max_outputs],
            provenance: None,
        }
    }

    /// Create a compiled kernel with provenance-aware caching.
    /// The generated eval path includes per-step clean checks.
    pub(crate) fn new_with_provenance(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<(CompiledU64Op, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
        input_dependents: Vec<Vec<usize>>,
    ) -> Self {
        let max_inputs = steps.iter().map(|s| s.1.len()).max().unwrap_or(0);
        let max_outputs = steps.iter().map(|s| s.2.len()).max().unwrap_or(0);
        let step_count = steps.len();

        // Compute per-step provenance from input_dependents (inverted).
        // step_prov[i] = bitmask of which inputs step i depends on.
        let mut step_prov = vec![0u64; step_count];
        for (input_idx, deps) in input_dependents.iter().enumerate() {
            for &step_idx in deps {
                if step_idx < step_count {
                    step_prov[step_idx] |= 1u64 << input_idx;
                }
            }
        }

        // Map output slots to their step's provenance.
        // slot_provenance[slot] = bitmask of inputs affecting that slot.
        let mut slot_provenance = vec![0u64; total_slots];
        // Coordinate slots depend on their own input
        for i in 0..coord_count.min(64) {
            slot_provenance[i] = 1u64 << i;
        }
        // Node output slots inherit the step's provenance
        let compiled_steps: Vec<CompiledStep> = steps
            .into_iter()
            .enumerate()
            .map(|(step_idx, (op, input_slots, output_slots))| {
                for &slot in &output_slots {
                    if slot < slot_provenance.len() {
                        slot_provenance[slot] = step_prov[step_idx];
                    }
                }
                CompiledStep { op, input_slots, output_slots }
            })
            .collect();

        Self {
            buffer: vec![0u64; total_slots],
            coord_count,
            steps: compiled_steps,
            output_map,
            gather_buf: vec![0u64; max_inputs],
            scatter_buf: vec![0u64; max_outputs],
            provenance: Some(ProvenanceState {
                node_clean: vec![false; step_count],
                input_dependents,
                slot_provenance,
                changed_mask: u64::MAX, // all dirty initially
            }),
        }
    }

    /// Set inputs with change detection. Tracks which inputs changed
    /// for pull-side guard in `get_slot`.
    #[inline]
    pub fn set_inputs(&mut self, coords: &[u64]) {
        if let Some(ref mut prov) = self.provenance {
            prov.changed_mask = 0;
            for i in 0..coords.len().min(self.coord_count) {
                if self.buffer[i] != coords[i] {
                    self.buffer[i] = coords[i];
                    prov.changed_mask |= 1u64 << i;
                    if i < prov.input_dependents.len() {
                        for &step_idx in &prov.input_dependents[i] {
                            prov.node_clean[step_idx] = false;
                        }
                    }
                }
            }
        } else {
            self.buffer[..self.coord_count.min(coords.len())]
                .copy_from_slice(&coords[..self.coord_count.min(coords.len())]);
        }
    }

    /// Evaluate the DAG eagerly. All dirty steps run.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        if let Some(ref mut prov) = self.provenance {
            for (step_idx, step) in self.steps.iter().enumerate() {
                if prov.node_clean[step_idx] { continue; }
                for (i, &slot) in step.input_slots.iter().enumerate() {
                    self.gather_buf[i] = self.buffer[slot];
                }
                (step.op)(
                    &self.gather_buf[..step.input_slots.len()],
                    &mut self.scatter_buf[..step.output_slots.len()],
                );
                for (i, &slot) in step.output_slots.iter().enumerate() {
                    self.buffer[slot] = self.scatter_buf[i];
                }
                prov.node_clean[step_idx] = true;
            }
        } else {
            self.buffer[..self.coord_count].copy_from_slice(coords);
            for step in &self.steps {
                for (i, &slot) in step.input_slots.iter().enumerate() {
                    self.gather_buf[i] = self.buffer[slot];
                }
                (step.op)(
                    &self.gather_buf[..step.input_slots.len()],
                    &mut self.scatter_buf[..step.output_slots.len()],
                );
                for (i, &slot) in step.output_slots.iter().enumerate() {
                    self.buffer[slot] = self.scatter_buf[i];
                }
            }
        }
    }

    /// Read a named output variate after `eval()`.
    #[inline]
    pub fn get(&self, name: &str) -> u64 {
        let slot = self.output_map[name];
        self.buffer[slot]
    }

    /// Read an output by pre-resolved slot index (avoids HashMap lookup).
    #[inline]
    pub fn get_slot(&self, slot: usize) -> u64 {
        self.buffer[slot]
    }

    /// Set inputs and evaluate ONLY if the requested output's cone
    /// was affected by the input changes. Pull-side guard: if the
    /// output's provenance doesn't intersect the changed inputs,
    /// skip eval entirely and return the cached value.
    ///
    /// This is the optimal call pattern for provenance-enabled
    /// kernels with selective output access.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.set_inputs(coords);
        if let Some(ref mut prov) = self.provenance {
            // Pull-side guard: check if this output's cone was affected
            if slot < prov.slot_provenance.len() {
                let output_prov = prov.slot_provenance[slot];
                if output_prov & prov.changed_mask == 0 {
                    // Output's cone is clean — skip eval entirely
                    return self.buffer[slot];
                }
            }
            // Cone was affected — run dirty steps
            for (step_idx, step) in self.steps.iter().enumerate() {
                if prov.node_clean[step_idx] { continue; }
                for (i, &s) in step.input_slots.iter().enumerate() {
                    self.gather_buf[i] = self.buffer[s];
                }
                (step.op)(
                    &self.gather_buf[..step.input_slots.len()],
                    &mut self.scatter_buf[..step.output_slots.len()],
                );
                for (i, &s) in step.output_slots.iter().enumerate() {
                    self.buffer[s] = self.scatter_buf[i];
                }
                prov.node_clean[step_idx] = true;
            }
        } else {
            self.buffer[..self.coord_count.min(coords.len())]
                .copy_from_slice(&coords[..self.coord_count.min(coords.len())]);
            for step in &self.steps {
                for (i, &s) in step.input_slots.iter().enumerate() {
                    self.gather_buf[i] = self.buffer[s];
                }
                (step.op)(
                    &self.gather_buf[..step.input_slots.len()],
                    &mut self.scatter_buf[..step.output_slots.len()],
                );
                for (i, &s) in step.output_slots.iter().enumerate() {
                    self.buffer[s] = self.scatter_buf[i];
                }
            }
        }
        self.buffer[slot]
    }

    pub fn coord_count(&self) -> usize { self.coord_count }

    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.output_map.get(name).copied()
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_map.keys().map(|s| s.as_str()).collect()
    }
}
