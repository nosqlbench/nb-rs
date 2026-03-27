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
    /// Reused across evaluations to avoid allocation.
    gather_buf: Vec<u64>,

    /// Scratch space for scattering outputs per step.
    scatter_buf: Vec<u64>,
}

impl CompiledKernel {
    /// Create a compiled kernel. Called by the assembly phase.
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
        }
    }

    /// Set coordinates and evaluate the entire DAG eagerly.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        // Write coordinates into the front of the buffer
        self.buffer[..self.coord_count].copy_from_slice(coords);

        // Execute every step in topological order
        for step in &self.steps {
            // Gather inputs from buffer
            let input_count = step.input_slots.len();
            for (i, &slot) in step.input_slots.iter().enumerate() {
                self.gather_buf[i] = self.buffer[slot];
            }

            // Execute
            let output_count = step.output_slots.len();
            (step.op)(
                &self.gather_buf[..input_count],
                &mut self.scatter_buf[..output_count],
            );

            // Scatter outputs to buffer
            for (i, &slot) in step.output_slots.iter().enumerate() {
                self.buffer[slot] = self.scatter_buf[i];
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

    /// Resolve an output name to a slot index for repeated fast access.
    pub fn resolve_output(&self, name: &str) -> Option<usize> {
        self.output_map.get(name).copied()
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_map.keys().map(|s| s.as_str()).collect()
    }
}
