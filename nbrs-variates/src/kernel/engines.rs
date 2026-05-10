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

/// One named shared cell propagated through the parent → child
/// scope chain. Carried on `GkKernel` (and surfaced through
/// `ScopeKernel::shared_cells_in_scope`) so a descendant whose
/// program declares a matching input slot can attach the cell —
/// even when intermediate scopes' bodies never name it and so
/// have no input slot for it themselves.
///
/// Without this carrier, an ancestral `shared X := …` cell
/// becomes invisible past the first intermediate scope under
/// the closure-binding economy. With it, every spawn step
/// computes "every cell visible at this scope" and threads the
/// full set forward — the cascade is transitive by
/// construction.
#[derive(Clone, Debug)]
pub struct SharedCellEntry {
    pub name: String,
    pub port_type: crate::node::PortType,
    pub cell: SharedCell,
}

/// Build the rich diagnostic message for a node-level eval panic.
/// Includes the node's function name, every output it feeds, the
/// input values it was called with, and the program's diagnostic
/// context (typically the source path / scope label). This is
/// what the user sees instead of the bare panic payload.
fn enrich_eval_panic(
    payload: Box<dyn std::any::Any + Send>,
    program: &GkProgram,
    node_idx: usize,
    inputs: &[Value],
) -> String {
    let original = payload
        .downcast_ref::<&'static str>().map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".into());
    let node_name = program.nodes.get(node_idx)
        .map(|n| n.meta().name.to_string())
        .unwrap_or_else(|| format!("<unknown node #{node_idx}>"));
    let mut output_names: Vec<&str> = program.output_map_iter()
        .filter_map(|(name, (n_idx, _))| {
            if *n_idx == node_idx { Some(name.as_str()) } else { None }
        })
        .collect();
    output_names.sort();
    let outputs_label = if output_names.is_empty() {
        "no declared output".to_string()
    } else {
        format!("output{} {}",
            if output_names.len() == 1 { "" } else { "s" },
            output_names.join(", "))
    };
    let mut input_label = String::new();
    for (i, v) in inputs.iter().enumerate() {
        if i > 0 { input_label.push_str(", "); }
        input_label.push_str(&format!("[{i}]={}", format_value_for_diag(v)));
    }
    format!(
        "{original}\n  ↳ in node `{node_name}` ({outputs_label}) \
         while evaluating {context}\n  \
         ↳ inputs: [{input_label}]",
        context = program.context(),
    )
}

/// Format a `Value` into a short diagnostic string. Strings are
/// quoted + truncated; vectors print their length not contents.
fn format_value_for_diag(v: &Value) -> String {
    match v {
        Value::U64(n) => format!("U64({n})"),
        Value::F64(n) => format!("F64({n})"),
        Value::Bool(b) => format!("Bool({b})"),
        Value::Str(s) => {
            let trimmed: String = s.chars().take(40).collect();
            if s.chars().count() > 40 {
                format!("Str({trimmed:?}…)")
            } else {
                format!("Str({trimmed:?})")
            }
        }
        Value::None => "None".to_string(),
        other => format!("{:?}", other.port_type()),
    }
}

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
        // Wrap the node's eval in catch_unwind so a node-level
        // panic (e.g. `Value::as_u64` on a Str) can be re-raised
        // with the diagnostic context the user actually needs:
        // which node panicked, which output(s) it feeds, what
        // the input values were, and where in the source the
        // node came from. Without this, the fiber-level catcher
        // sees only the bare message — "expected U64, got Str"
        // — and the user has no way to find the offending
        // binding short of bisecting the workload.
        //
        // Cost: one catch_unwind frame per slow-path node eval.
        // The JIT path doesn't go through here. On the success
        // path the frame is a few stack words; on the panic
        // path it's strictly an improvement over what the
        // user sees today.
        let payload = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| {
                program.nodes[node_idx].eval(
                    &self.input_scratch[..input_count],
                    &mut self.buffers[node_idx],
                );
            })
        );
        if let Err(e) = payload {
            let enriched = enrich_eval_panic(
                e, program, node_idx,
                &self.input_scratch[..input_count],
            );
            std::panic::resume_unwind(Box::new(enriched));
        }
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
    /// Single-register semantics: a cell-bound slot's only
    /// register IS the cell — `set_input` writes through the
    /// cell. A non-cell slot's register is the local
    /// `inputs[idx]` array. There's no second snapshot kept in
    /// lockstep with the cell; reads always go to whichever is
    /// the slot's register.
    ///
    /// Dependents-marking is the dependent-list invalidation
    /// strategy carried by `GkState`; it's the write-side
    /// half of the engine's dirty-tracking. Other engines
    /// (`RawState`, `ProvScanState`) implement different
    /// strategies — see their own `set_inputs` impls.
    pub fn set_input(&mut self, idx: usize, value: Value) {
        let changed = if let Some(cell) = self.core.shared_cells.get(idx).and_then(|c| c.as_ref()) {
            // Cell-bound slot: the cell is the register. We do
            // NOT mirror the value into `inputs[idx]`; that
            // array slot is unused for cell-bound inputs.
            let mut guard = cell.lock().unwrap();
            let changed = *guard != value;
            *guard = value;
            changed
        } else {
            let changed = self.core.inputs[idx] != value;
            if changed {
                self.core.inputs[idx] = value;
            }
            changed
        };
        if changed && idx < self.input_dependents.len() {
            for &node_idx in &self.input_dependents[idx] {
                self.core.node_clean[node_idx] = false;
            }
        }
        // Non-deterministic nodes must always re-evaluate.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Read the value of an input by index.
    ///
    /// Single-register read: cell-bound slots return the cell's
    /// current value; non-cell slots return the local register.
    /// One canonical value per slot, no stale snapshot.
    pub fn get_input(&self, idx: usize) -> Value {
        self.core.read_input(idx)
    }

    /// Alias for [`Self::get_input`]; kept for legacy callers
    /// that picked the more explicit name. Both read the cell
    /// when one is attached.
    pub fn read_input_value(&self, idx: usize) -> Value {
        self.core.read_input(idx)
    }

    /// Attach a `SharedCell` to an input slot.
    ///
    /// After this call the cell becomes the slot's sole
    /// register: reads via `read_input` go through the cell,
    /// `set_input` writes through the cell. The local
    /// `inputs[idx]` array entry for this slot is unused for
    /// cell-bound slots — there is no second register kept in
    /// lockstep.
    ///
    /// Dependents are dirtied because the slot's effective
    /// value just changed from the local default to whatever
    /// the cell currently holds.
    pub fn attach_shared_cell(&mut self, idx: usize, cell: SharedCell) {
        if idx >= self.core.shared_cells.len() {
            self.core.shared_cells.resize(idx + 1, None);
        }
        self.core.shared_cells[idx] = Some(cell);
        if idx < self.input_dependents.len() {
            for &node_idx in &self.input_dependents[idx] {
                self.core.node_clean[node_idx] = false;
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
    ///
    /// Cell-bound slots are skipped: the cell is cross-kernel
    /// shared state with its own lifecycle (managed by the
    /// owning ancestor scope), and a stanza-local reset must
    /// not clobber other kernels' views.
    pub fn reset_inputs_from(&mut self, from_idx: usize) {
        for i in from_idx..self.core.inputs.len() {
            // Cell-bound slots: the cell is the register, owned
            // by the ancestor that declared `shared X := init`.
            // Don't touch.
            if self.core.shared_cells.get(i).is_some_and(|c| c.is_some()) {
                continue;
            }
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

#[cfg(test)]
mod panic_enrichment_tests {
    //! Verify the eval-panic enricher produces a usable diagnostic
    //! when a node panics on a type mismatch (the "expected U64,
    //! got Str" surface that operators see in the wild).

    use crate::dsl::compile::compile_gk_with_libs;

    #[test]
    fn type_mismatch_panic_carries_node_and_output_context() {
        // Declare the input as u64, then write a Str into the
        // slot — `mul`'s u64 path will panic on `as_u64()`.
        // The enricher must wrap the message with the node
        // name + output name + program context.
        let mut k = compile_gk_with_libs(
            "extern x: u64\n\
             doubled := mul(x, 2)\n",
            None, vec![], &[], false, "test_workload",
        ).expect("compile");
        let idx = k.program().find_input("x").unwrap();
        k.state().set_input(idx, crate::node::Value::Str("oops".into()));
        let result = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| { k.pull("doubled"); })
        );
        let err = result.expect_err("pull should panic on type mismatch");
        let msg = err.downcast_ref::<String>().cloned()
            .or_else(|| err.downcast_ref::<&'static str>().map(|s| (*s).to_string()))
            .expect("panic payload should be a String");
        assert!(msg.contains("expected U64"),
            "missing original panic body in: {msg}");
        assert!(msg.contains("`mul`"),
            "missing node name in enriched message: {msg}");
        assert!(msg.contains("doubled"),
            "missing output binding in enriched message: {msg}");
        assert!(msg.contains("test_workload"),
            "missing program context in enriched message: {msg}");
        assert!(msg.contains("\"oops\""),
            "missing input snapshot in enriched message: {msg}");
        // Surface the full enriched message in `cargo test --
        // --nocapture` runs so the format is easy to eyeball.
        eprintln!("== enriched message ==\n{msg}\n======================");
    }
}
