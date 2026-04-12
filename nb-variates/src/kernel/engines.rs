// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK evaluation engines: EngineCore (shared eval loop) and the three
//! P1 engine types — GkState (dependent-list), RawState (no provenance),
//! and ProvScanState (provenance-scan).

use crate::node::Value;
use super::WireSource;
use super::program::GkProgram;

/// Shared evaluation state for all GK engines. Contains the node
/// output buffers, input values, port values, and the eval loop.
/// Engine types wrap this and provide their own `set_inputs()`.
pub struct EngineCore {
    /// Per-node output value buffers, reused across evaluations.
    pub(crate) buffers: Vec<Vec<Value>>,
    /// Per-node: true = cached output is valid, false = needs eval.
    pub(crate) node_clean: Vec<bool>,
    /// Current input values.
    pub(crate) inputs: Vec<u64>,
    /// Current volatile port values (reset on set_inputs).
    pub(crate) volatile_values: Vec<Value>,
    /// Default values for volatile ports (copied on reset).
    pub(crate) volatile_defaults: Vec<Value>,
    /// Current sticky port values (persist across set_inputs).
    pub(crate) sticky_values: Vec<Value>,
    /// Pre-allocated scratch buffer for node input gathering.
    pub(crate) input_scratch: Vec<Value>,
}

impl EngineCore {
    /// Evaluate a node by index. Shared by all engines.
    /// Checks the clean flag, recursively evaluates upstream, gathers
    /// inputs, calls node.eval(), marks clean.
    pub fn eval_node(&mut self, program: &GkProgram, node_idx: usize) {
        if self.node_clean[node_idx] {
            return;
        }

        let wiring = &program.wiring[node_idx];
        for source in wiring.iter() {
            if let WireSource::NodeOutput(upstream_idx, _) = source {
                self.eval_node(program, *upstream_idx);
            }
        }

        for (i, source) in wiring.iter().enumerate() {
            self.input_scratch[i] = match source {
                WireSource::Input(idx) => Value::U64(self.inputs[*idx]),
                WireSource::NodeOutput(upstream_idx, port_idx) => {
                    self.buffers[*upstream_idx][*port_idx].clone()
                }
                WireSource::VolatilePort(idx) => self.volatile_values[*idx].clone(),
                WireSource::StickyPort(idx) => self.sticky_values[*idx].clone(),
            };
        }

        let input_count = wiring.len();
        program.nodes[node_idx].eval(
            &self.input_scratch[..input_count],
            &mut self.buffers[node_idx],
        );
        self.node_clean[node_idx] = true;
    }

    /// Pull a named output.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        let (node_idx, port_idx) = *program.output_map
            .get(output_name)
            .unwrap_or_else(|| panic!("unknown output variate: {output_name}"));
        self.eval_node(program, node_idx);
        &self.buffers[node_idx][port_idx]
    }
}

// =================================================================
// GkState: dependent-list engine (default, O(affected) invalidation)
// =================================================================

/// GK evaluation engine using precomputed per-input dependent lists.
///
/// On `set_inputs()`, only nodes that depend on actually-changed
/// inputs are dirtied. O(affected_nodes) per input change.
/// This is the default engine for production use.
pub struct GkState {
    /// Shared evaluation core (buffers, clean flags, inputs, ports).
    pub core: EngineCore,
    /// Per-input dependent node lists for O(affected) invalidation.
    input_dependents: Vec<Vec<usize>>,
    /// Indices of non-deterministic nodes (zero-provenance, no declared inputs).
    ///
    /// These nodes produce a different value on every evaluation (e.g.,
    /// `counter()`, `current_epoch_millis()`). They are unconditionally
    /// marked dirty on every `set_inputs()` call so they are never cached.
    nondeterministic_nodes: Vec<usize>,
}

impl GkState {
    /// Construct a GkState from its component parts.
    pub(crate) fn from_parts(
        core: EngineCore,
        input_dependents: Vec<Vec<usize>>,
        nondeterministic_nodes: Vec<usize>,
    ) -> Self {
        Self { core, input_dependents, nondeterministic_nodes }
    }

    /// Set new input values with change detection.
    ///
    /// Compares each input against its current value. Only inputs that
    /// actually changed are flagged in `changed_mask`. Nodes that don't
    /// depend on changed inputs remain cached (provenance check).
    ///
    /// Non-deterministic nodes (counter, current_epoch_millis, etc.) are
    /// always marked dirty regardless of input changes, because their output
    /// depends on internal state rather than graph inputs.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        for i in 0..coords.len().min(self.core.inputs.len()) {
            if self.core.inputs[i] != coords[i] {
                self.core.inputs[i] = coords[i];
                for &node_idx in &self.input_dependents[i] {
                    self.core.node_clean[node_idx] = false;
                }
            }
        }
        if !self.core.volatile_defaults.is_empty() {
            self.core.volatile_values.clone_from_slice(&self.core.volatile_defaults);
            let port_slot = self.input_dependents.len() - 1;
            for &node_idx in &self.input_dependents[port_slot] {
                self.core.node_clean[node_idx] = false;
            }
        }
        // Non-deterministic nodes must always re-evaluate — never use cached values.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Set the value of a volatile port by index, dirtying dependent nodes.
    pub fn set_volatile(&mut self, idx: usize, value: Value) {
        self.core.volatile_values[idx] = value;
        let port_slot = self.input_dependents.len() - 1;
        for &node_idx in &self.input_dependents[port_slot] {
            self.core.node_clean[node_idx] = false;
        }
    }

    /// Set the value of a sticky port by index, dirtying dependent nodes.
    pub fn set_sticky(&mut self, idx: usize, value: Value) {
        self.core.sticky_values[idx] = value;
        let port_slot = self.input_dependents.len() - 1;
        for &node_idx in &self.input_dependents[port_slot] {
            self.core.node_clean[node_idx] = false;
        }
    }

    /// Read the current value of a volatile port by index.
    pub fn get_volatile(&self, idx: usize) -> &Value {
        &self.core.volatile_values[idx]
    }

    /// Read the current value of a sticky port by index.
    pub fn get_sticky(&self, idx: usize) -> &Value {
        &self.core.sticky_values[idx]
    }

    /// Read a coordinate input by index.
    pub fn get_input(&self, idx: usize) -> u64 {
        self.core.inputs[idx]
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }

    /// Evaluate a node by index (exposed for constant folding in GkProgram).
    pub(crate) fn eval_node_public(&mut self, program: &GkProgram, node_idx: usize) {
        self.core.eval_node(program, node_idx);
    }
}

// =================================================================
// RawState: no provenance engine (all nodes dirty every eval)
// =================================================================

/// GK evaluation engine with no provenance. Every `set_inputs()`
/// marks all nodes dirty. Baseline for benchmarking provenance overhead.
pub struct RawState {
    /// Shared evaluation core.
    pub core: EngineCore,
}

impl RawState {
    /// Set new input values and mark all nodes dirty (no provenance check).
    pub fn set_inputs(&mut self, coords: &[u64]) {
        for i in 0..coords.len().min(self.core.inputs.len()) {
            self.core.inputs[i] = coords[i];
        }
        // All nodes dirty — no provenance check
        for clean in &mut self.core.node_clean {
            *clean = false;
        }
        if !self.core.volatile_defaults.is_empty() {
            self.core.volatile_values.clone_from_slice(&self.core.volatile_defaults);
        }
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }
}

// =================================================================
// ProvScanState: provenance-scan engine (O(all) invalidation)
// =================================================================

/// GK evaluation engine using provenance bitmask scanning.
///
/// On `set_inputs()`, scans ALL nodes and checks each node's
/// provenance bitmask against the changed-inputs mask.
/// O(all_nodes) per input change regardless of how many changed.
pub struct ProvScanState {
    /// Shared evaluation core.
    pub core: EngineCore,
    input_provenance: Vec<u64>,
    /// Indices of non-deterministic nodes (zero-provenance, no declared inputs).
    ///
    /// Marked dirty unconditionally on every `set_inputs()` call since
    /// their output depends on internal state rather than graph inputs.
    nondeterministic_nodes: Vec<usize>,
}

impl ProvScanState {
    /// Construct a ProvScanState from its component parts.
    pub(crate) fn from_parts(
        core: EngineCore,
        input_provenance: Vec<u64>,
        nondeterministic_nodes: Vec<usize>,
    ) -> Self {
        Self { core, input_provenance, nondeterministic_nodes }
    }

    /// Set new input values and invalidate affected nodes.
    ///
    /// Scans all nodes and checks each node's provenance bitmask
    /// against the changed-inputs mask. O(all_nodes) per call.
    ///
    /// Non-deterministic nodes (zero provenance, no declared inputs) are
    /// always marked dirty regardless of what inputs changed.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        let mut mask = 0u64;
        for i in 0..coords.len().min(self.core.inputs.len()) {
            if self.core.inputs[i] != coords[i] {
                self.core.inputs[i] = coords[i];
                mask |= 1u64 << i;
            }
        }
        if !self.core.volatile_defaults.is_empty() {
            self.core.volatile_values.clone_from_slice(&self.core.volatile_defaults);
            mask |= 1u64 << 63;
        }
        if mask != 0 {
            for (i, clean) in self.core.node_clean.iter_mut().enumerate() {
                if *clean && (self.input_provenance[i] & mask) != 0 {
                    *clean = false;
                }
            }
        }
        // Non-deterministic nodes must always re-evaluate — never use cached values.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }
}
