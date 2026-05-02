// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK evaluation engines: EngineCore (shared eval loop) and the three
//! P1 engine types — GkState (dependent-list), RawState (no provenance),
//! and ProvScanState (provenance-scan).

use std::sync::{Arc, Mutex};

use crate::node::Value;
use super::WireSource;
use super::program::GkProgram;

/// A cross-kernel mutable cell for a `shared`-modifier wire.
///
/// When a `shared` output in an outer scope is bound into an
/// inner kernel via `bind_outer_scope`, both kernels' input
/// slots reference the same `SharedCell`. Writes from inner via
/// `set_input` flow through to the cell; reads on either side
/// pick up the latest value (with `refresh_shared` re-reading
/// the cell into the local snapshot).
///
/// Concurrent writers serialize at the Mutex; the current
/// semantic is **last-write-wins** (lock-acquisition order).
/// Future templated patterns (atomic-fetch-add, sum-reduction,
/// merge, etc.) — see SRD-16 §"Open: concurrent shared
/// mutation" — will introduce alternative cell types selected
/// per binding declaration.
pub type SharedCell = Arc<Mutex<Value>>;

/// Shared evaluation state for all GK engines. Contains the node
/// output buffers, input values, and the eval loop.
/// Engine types wrap this and provide their own invalidation strategy.
pub struct EngineCore {
    /// Per-node output value buffers, reused across evaluations.
    pub(crate) buffers: Vec<Vec<Value>>,
    /// Per-node: true = cached output is valid, false = needs eval.
    pub(crate) node_clean: Vec<bool>,
    /// Current input values (coordinates + captures, all unified).
    /// For `shared`-bound slots, this holds a local snapshot of
    /// the cell value — `refresh_shared` re-syncs it from the
    /// cell, and `set_input` writes through to both the cell
    /// and the snapshot.
    pub(crate) inputs: Vec<Value>,
    /// Default values for each input (used by reset_inputs).
    pub(crate) input_defaults: Vec<Value>,
    /// Optional cross-kernel shared cell per input slot. `None`
    /// = local-only input (the common case). `Some(cell)` =
    /// the slot is bound to a shared cell; writes propagate
    /// through the cell to whatever other kernels share it.
    pub(crate) shared_cells: Vec<Option<SharedCell>>,
    /// Pre-allocated scratch buffer for node input gathering.
    pub(crate) input_scratch: Vec<Value>,
}

impl EngineCore {
    /// Read an input slot's current value, transparent to whether
    /// it's a plain slot or backed by a `SharedCell`. The
    /// canonical read path used by both `eval_node` and
    /// `GkState::get_input` — there's no separate "refresh" step
    /// the caller must remember; the cell is queried on every
    /// read.
    ///
    /// Cost: one Mutex lock per read on shared slots; a clone of
    /// `inputs[idx]` on plain slots (Value's clone is cheap —
    /// Arc-based for vectors, primitive copy otherwise).
    #[inline]
    pub(crate) fn read_input(&self, idx: usize) -> Value {
        if let Some(cell) = self.shared_cells.get(idx).and_then(|c| c.as_ref()) {
            return cell.lock().unwrap().clone();
        }
        self.inputs[idx].clone()
    }

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
                // `read_input` transparently reads the cell for
                // `shared`-bound slots, so per-cycle eval picks
                // up cross-kernel writes without any explicit
                // refresh.
                WireSource::Input(idx) => self.read_input(*idx),
                WireSource::NodeOutput(upstream_idx, port_idx) => {
                    self.buffers[*upstream_idx][*port_idx].clone()
                }
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
/// On `set_input()`, only nodes that depend on the changed input
/// are dirtied. O(affected_nodes) per input change.
/// This is the default engine for production use.
pub struct GkState {
    /// Shared evaluation core (buffers, clean flags, inputs).
    pub core: EngineCore,
    /// Per-input dependent node lists for O(affected) invalidation.
    input_dependents: Vec<Vec<usize>>,
    /// Indices of non-deterministic nodes (zero-provenance, no declared inputs).
    ///
    /// These nodes produce a different value on every evaluation (e.g.,
    /// `counter()`, `current_epoch_millis()`). They are unconditionally
    /// marked dirty on every `set_input()` call so they are never cached.
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

    /// Set all coordinate inputs at once (convenience for the common
    /// single-cycle case). Wraps each u64 as `Value::U64` and sets
    /// them at indices 0..N with per-input change detection.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        for i in 0..coords.len().min(self.core.inputs.len()) {
            let new_val = Value::U64(coords[i]);
            if self.core.inputs[i] != new_val {
                self.core.inputs[i] = new_val;
                if i < self.input_dependents.len() {
                    for &node_idx in &self.input_dependents[i] {
                        self.core.node_clean[node_idx] = false;
                    }
                }
            }
        }
        // Non-deterministic nodes must always re-evaluate.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Set a single input by index, dirtying only dependent nodes.
    ///
    /// If the slot has a `SharedCell` attached (from
    /// `bind_outer_scope` wiring `shared`-modifier outputs from
    /// an outer kernel), the new value is also written through
    /// the cell so other kernels sharing it see the update on
    /// their next `refresh_shared`.
    pub fn set_input(&mut self, idx: usize, value: Value) {
        if self.core.inputs[idx] != value {
            self.core.inputs[idx] = value.clone();
            if idx < self.input_dependents.len() {
                for &node_idx in &self.input_dependents[idx] {
                    self.core.node_clean[node_idx] = false;
                }
            }
        }
        // Write-through to the shared cell, if any. The Mutex
        // serializes concurrent writers; semantic is last-write-
        // wins (SRD-16 §"Mutability Rules: Shared Mutable").
        if let Some(cell) = self.core.shared_cells.get(idx).and_then(|c| c.as_ref()) {
            *cell.lock().unwrap() = value;
        }
        // Non-deterministic nodes must always re-evaluate.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Read the snapshot value of an input by index.
    ///
    /// Lower-level accessor returning a borrow into the kernel's
    /// local input array — for non-shared slots this is the
    /// canonical value; for `shared`-bound slots it's a snapshot
    /// updated when *this* kernel calls `set_input`, but
    /// potentially stale relative to writes by other kernels
    /// sharing the same cell. Callers wanting a guaranteed-fresh
    /// cross-kernel read should use [`crate::kernel::GkKernel::lookup`]
    /// or `pull` — both go through the cell transparently.
    pub fn get_input(&self, idx: usize) -> &Value {
        &self.core.inputs[idx]
    }

    /// Cell-aware read of an input by index. Goes through the
    /// `SharedCell` Mutex when the slot is shared, returning the
    /// canonical cross-kernel value.
    pub fn read_input_value(&self, idx: usize) -> Value {
        self.core.read_input(idx)
    }

    /// Attach a `SharedCell` to an input slot.
    ///
    /// After this call, `set_input` on the slot writes through
    /// to the cell (in addition to the local snapshot), and
    /// `refresh_shared` reads cell updates back into the local
    /// snapshot. This is the wiring done by
    /// [`crate::kernel::GkKernel::bind_outer_scope`] for
    /// `shared`-modifier outputs in the outer kernel.
    ///
    /// Initializes the slot's snapshot from the cell's current
    /// value, so subsequent reads see the right value without
    /// an explicit `refresh_shared`.
    pub fn attach_shared_cell(&mut self, idx: usize, cell: SharedCell) {
        let cell_value = cell.lock().unwrap().clone();
        if idx >= self.core.shared_cells.len() {
            self.core.shared_cells.resize(idx + 1, None);
        }
        self.core.shared_cells[idx] = Some(cell);
        if self.core.inputs[idx] != cell_value {
            self.core.inputs[idx] = cell_value;
            if idx < self.input_dependents.len() {
                for &node_idx in &self.input_dependents[idx] {
                    self.core.node_clean[node_idx] = false;
                }
            }
        }
    }

    /// Returns the `SharedCell` attached to an input slot, if any.
    /// Used by `bind_outer_scope` to share an existing cell with
    /// inner kernels.
    pub fn shared_cell(&self, idx: usize) -> Option<SharedCell> {
        self.core.shared_cells.get(idx).and_then(|c| c.clone())
    }

    /// Reset a range of inputs to their defaults. Used at stanza
    /// boundaries to prevent capture leakage across stanzas.
    /// `from_idx` is typically `coord_count` (skip coordinates,
    /// reset only capture inputs).
    pub fn reset_inputs_from(&mut self, from_idx: usize) {
        for i in from_idx..self.core.inputs.len() {
            if self.core.inputs[i] != self.core.input_defaults[i] {
                self.core.inputs[i] = self.core.input_defaults[i].clone();
                if i < self.input_dependents.len() {
                    for &node_idx in &self.input_dependents[i] {
                        self.core.node_clean[node_idx] = false;
                    }
                }
            }
        }
    }

    /// Invalidate all state: reset all inputs to defaults and mark
    /// every node dirty. Provides "clean slate" semantics.
    pub fn invalidate_all(&mut self) {
        self.core.inputs.clone_from_slice(&self.core.input_defaults);
        for clean in &mut self.core.node_clean {
            *clean = false;
        }
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }

    /// Pre-populate a node's output buffer slot and mark it clean,
    /// suppressing on-demand evaluation. Used by the scope-init
    /// pass (SRD 11 §"Init Binding Contract" Plan B) to seed
    /// per-fiber states with init binding values that the
    /// activation kernel already evaluated, so each fiber doesn't
    /// re-fire the eval at first pull.
    pub fn seed_node_buffer(&mut self, node_idx: usize, port_idx: usize, value: Value) {
        if node_idx >= self.core.buffers.len() { return; }
        if port_idx >= self.core.buffers[node_idx].len() { return; }
        self.core.buffers[node_idx][port_idx] = value;
        self.core.node_clean[node_idx] = true;
    }

    /// Read a node's output buffer slot. Used by the scope-init
    /// pass to extract a pre-pulled init binding value from one
    /// state and seed it into another.
    pub fn node_buffer(&self, node_idx: usize, port_idx: usize) -> Option<&Value> {
        self.core.buffers.get(node_idx)
            .and_then(|ports| ports.get(port_idx))
    }

    /// Pull an output by index (declaration order). Only evaluates
    /// the computation cone for this specific output.
    pub fn pull_by_index(&mut self, program: &GkProgram, output_idx: usize) -> &Value {
        let (node_idx, port_idx) = program.resolve_output_by_index(output_idx);
        self.core.eval_node(program, node_idx);
        &self.core.buffers[node_idx][port_idx]
    }

    /// Pull all outputs in declaration order.
    pub fn pull_all<'a>(&'a mut self, program: &GkProgram) -> Vec<&'a Value> {
        for i in 0..program.output_count() {
            let (node_idx, _) = program.resolve_output_by_index(i);
            self.core.eval_node(program, node_idx);
        }
        (0..program.output_count())
            .map(|i| {
                let (ni, pi) = program.resolve_output_by_index(i);
                &self.core.buffers[ni][pi]
            })
            .collect()
    }

    /// Create a memoized accessor for a named subset of outputs.
    /// Resolves names to indices once; subsequent access uses indices only.
    pub fn accessor(program: &GkProgram, names: &[&str]) -> OutputAccessor {
        let indices: Vec<usize> = names.iter()
            .filter_map(|n| program.output_index(n))
            .collect();
        OutputAccessor { indices }
    }

    /// Evaluate a node by index (exposed for constant folding in GkProgram).
    pub(crate) fn eval_node_public(&mut self, program: &GkProgram, node_idx: usize) {
        self.core.eval_node(program, node_idx);
    }
}

/// Memoized output accessor for a named subset of outputs.
///
/// Created once from output names via `GkState::accessor()`.
/// Subsequent pulls use pre-resolved indices — no name lookups.
pub struct OutputAccessor {
    indices: Vec<usize>,
}

impl OutputAccessor {
    /// Pull all outputs in this accessor from the given state.
    pub fn pull_all<'a>(&self, state: &'a mut GkState, program: &GkProgram) -> Vec<&'a Value> {
        for &idx in &self.indices {
            let (node_idx, _) = program.resolve_output_by_index(idx);
            state.core.eval_node(program, node_idx);
        }
        self.indices.iter()
            .map(|&idx| {
                let (ni, pi) = program.resolve_output_by_index(idx);
                &state.core.buffers[ni][pi]
            })
            .collect()
    }

    /// Number of outputs in this accessor.
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Whether this accessor has no outputs.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
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
            self.core.inputs[i] = Value::U64(coords[i]);
        }
        for clean in &mut self.core.node_clean {
            *clean = false;
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
    /// Indices of non-deterministic nodes.
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
    pub fn set_inputs(&mut self, coords: &[u64]) {
        let mut mask = 0u64;
        for i in 0..coords.len().min(self.core.inputs.len()) {
            let new_val = Value::U64(coords[i]);
            if self.core.inputs[i] != new_val {
                self.core.inputs[i] = new_val;
                mask |= 1u64 << i;
            }
        }
        if mask != 0 {
            for (i, clean) in self.core.node_clean.iter_mut().enumerate() {
                if *clean && (self.input_provenance[i] & mask) != 0 {
                    *clean = false;
                }
            }
        }
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }
}
