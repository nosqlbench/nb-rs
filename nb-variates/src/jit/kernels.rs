// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! JIT kernel types: structs and impls for all four kernel variants.
//!
//! `JitCore` holds the shared buffer, slot map, and module handle.
//! The four kernel structs (`JitKernelRaw`, `JitKernelPush`,
//! `JitKernelPull`, `JitKernelPushPull`) wrap a `JitCore` and a
//! compiled function pointer, providing `eval` and accessor methods.

use std::collections::HashMap;

use cranelift_jit::JITModule;

use crate::node::GkNode;

/// Shared fields for all JIT kernel variants.
pub(super) struct JitCore {
    pub(super) buffer: Vec<u64>,
    pub(super) coord_count: usize,
    pub(super) output_map: HashMap<String, usize>,
    pub(super) _module: JITModule,
    pub(super) _nodes: Vec<Box<dyn GkNode>>,
}

/// Compute slot provenance from input_dependents.
pub(super) fn compute_jit_slot_provenance(
    coord_count: usize,
    buffer_len: usize,
    step_count: usize,
    input_dependents: &[Vec<usize>],
) -> Vec<u64> {
    let mut step_prov = vec![0u64; step_count];
    for (input_idx, deps) in input_dependents.iter().enumerate() {
        for &step_idx in deps {
            if step_idx < step_count {
                step_prov[step_idx] |= 1u64 << input_idx;
            }
        }
    }
    let mut slot_prov = vec![0u64; buffer_len];
    for i in 0..coord_count.min(64) {
        slot_prov[i] = 1u64 << i;
    }
    for i in 0..step_count {
        let slot = coord_count + i;
        if slot < slot_prov.len() {
            slot_prov[slot] = step_prov[i];
        }
    }
    slot_prov
}

macro_rules! jit_accessors {
    () => {
        /// Returns the number of coordinate inputs this kernel accepts.
        pub fn coord_count(&self) -> usize { self.core.coord_count }

        /// Returns the buffer slot index for the named output, if present.
        pub fn resolve_output(&self, name: &str) -> Option<usize> {
            self.core.output_map.get(name).copied()
        }

        /// Returns the raw u64 value stored in the named output slot.
        #[inline]
        pub fn get(&self, name: &str) -> u64 {
            self.core.buffer[self.core.output_map[name]]
        }

        /// Returns the raw u64 value stored at the given buffer slot index.
        #[inline]
        pub fn get_slot(&self, slot: usize) -> u64 {
            self.core.buffer[slot]
        }
    };
}

// ── JitKernelRaw ───────────────────────────────────────────

/// Raw JIT kernel: no provenance, all nodes evaluate unconditionally.
pub struct JitKernelRaw {
    pub(super) core: JitCore,
    pub(super) code_fn: unsafe fn(*const u64, *mut u64),
}

impl JitKernelRaw {
    /// Evaluate the kernel with the given coordinate values.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.core.buffer[..self.core.coord_count.min(coords.len())]
            .copy_from_slice(&coords[..self.core.coord_count.min(coords.len())]);
        unsafe { (self.code_fn)(self.core.buffer.as_ptr(), self.core.buffer.as_mut_ptr()); }
    }

    /// Evaluate and return the value at the given buffer slot index.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.eval(coords);
        self.core.buffer[slot]
    }

    /// Decompose into raw parts for hybrid kernel integration.
    pub fn into_parts(self) -> (unsafe fn(*const u64, *mut u64), JITModule) {
        (self.code_fn, self.core._module)
    }

    jit_accessors!();
}

// ── JitKernelPush ──────────────────────────────────────────

/// Push-only JIT kernel: per-node dirty tracking, no cone guard.
pub struct JitKernelPush {
    pub(super) core: JitCore,
    pub(super) code_fn_prov: unsafe fn(*const u64, *mut u64, *mut u8),
    pub(super) node_clean: Vec<u8>,
    pub(super) input_dependents: Vec<Vec<usize>>,
}

impl JitKernelPush {
    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        for i in 0..coords.len().min(self.core.coord_count) {
            if self.core.buffer[i] != coords[i] {
                self.core.buffer[i] = coords[i];
                if i < self.input_dependents.len() {
                    for &step_idx in &self.input_dependents[i] {
                        self.node_clean[step_idx] = 0;
                    }
                }
            }
        }
    }

    /// Evaluate the kernel with the given coordinate values.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        unsafe {
            (self.code_fn_prov)(
                self.core.buffer.as_ptr(),
                self.core.buffer.as_mut_ptr(),
                self.node_clean.as_mut_ptr(),
            );
        }
    }

    /// Evaluate and return the value at the given buffer slot index.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.eval(coords);
        self.core.buffer[slot]
    }

    jit_accessors!();
}

// ── JitKernelPull ──────────────────────────────────────────

/// Pull-only JIT kernel: cone guard, but all nodes run when cone is dirty.
/// Uses the raw (non-provenance) JIT function — no per-node clean checks.
pub struct JitKernelPull {
    pub(super) core: JitCore,
    pub(super) code_fn: unsafe fn(*const u64, *mut u64),
    pub(super) slot_provenance: Vec<u64>,
    pub(super) changed_mask: u64,
}

impl JitKernelPull {
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

    /// Evaluate the kernel with the given coordinate values.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        unsafe { (self.code_fn)(self.core.buffer.as_ptr(), self.core.buffer.as_mut_ptr()); }
    }

    /// Evaluate and return the value at the given buffer slot index,
    /// skipping evaluation if the slot's provenance cone is unaffected.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.set_inputs(coords);
        if slot < self.slot_provenance.len()
            && self.slot_provenance[slot] & self.changed_mask == 0 {
            return self.core.buffer[slot];
        }
        unsafe { (self.code_fn)(self.core.buffer.as_ptr(), self.core.buffer.as_mut_ptr()); }
        self.core.buffer[slot]
    }

    jit_accessors!();
}

// ── JitKernelPushPull ──────────────────────────────────────

/// Full optimization: push-side dirty tracking + pull-side cone guard.
pub struct JitKernelPushPull {
    pub(super) core: JitCore,
    pub(super) code_fn_prov: unsafe fn(*const u64, *mut u64, *mut u8),
    pub(super) node_clean: Vec<u8>,
    pub(super) input_dependents: Vec<Vec<usize>>,
    pub(super) slot_provenance: Vec<u64>,
    pub(super) changed_mask: u64,
}

impl JitKernelPushPull {
    #[inline]
    fn set_inputs(&mut self, coords: &[u64]) {
        self.changed_mask = 0;
        for i in 0..coords.len().min(self.core.coord_count) {
            if self.core.buffer[i] != coords[i] {
                self.core.buffer[i] = coords[i];
                self.changed_mask |= 1u64 << i;
                if i < self.input_dependents.len() {
                    for &step_idx in &self.input_dependents[i] {
                        self.node_clean[step_idx] = 0;
                    }
                }
            }
        }
    }

    /// Evaluate the kernel with the given coordinate values.
    #[inline]
    pub fn eval(&mut self, coords: &[u64]) {
        self.set_inputs(coords);
        unsafe {
            (self.code_fn_prov)(
                self.core.buffer.as_ptr(),
                self.core.buffer.as_mut_ptr(),
                self.node_clean.as_mut_ptr(),
            );
        }
    }

    /// Evaluate and return the value at the given buffer slot index,
    /// applying both push and pull optimizations.
    #[inline]
    pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
        self.set_inputs(coords);
        if slot < self.slot_provenance.len()
            && self.slot_provenance[slot] & self.changed_mask == 0 {
            return self.core.buffer[slot];
        }
        unsafe {
            (self.code_fn_prov)(
                self.core.buffer.as_ptr(),
                self.core.buffer.as_mut_ptr(),
                self.node_clean.as_mut_ptr(),
            );
        }
        self.core.buffer[slot]
    }

    jit_accessors!();
}
