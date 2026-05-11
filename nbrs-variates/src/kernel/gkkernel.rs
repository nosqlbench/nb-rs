// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GkKernel: a compiled GK kernel pairing an Arc<GkProgram> with a GkState.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::node::{GkNode, Value};
use super::{WireSource, InputDef};
use super::program::GkProgram;
use super::engines::{GkState, SharedCellEntry};

/// Auto-create `SharedCell`s for `shared`-modifier outputs that
/// have a backing input slot on this kernel. Call once at
/// construction so subsequent `materialize_wiring_from_outer` from inner
/// kernels can pick the cells up via `outer.shared_cell(idx)`
/// without mutating outer.
///
/// A `shared` output without a backing input slot (the legacy
/// shape — `shared X := <node-binding>` compiles to a
/// computation node, not an input slot) is silently skipped;
/// without a slot there's nothing to share.
fn seed_shared_cells(state: &mut GkState, program: &GkProgram) {
    for name in program.shared_outputs() {
        let Some(idx) = program.find_input(name) else { continue };
        if state.shared_cell(idx).is_some() { continue; } // already seeded
        let init_value = state.get_input(idx);
        state.attach_shared_cell(idx, Arc::new(Mutex::new(init_value)));
    }
}

/// A compiled GK kernel: an `Arc<GkProgram>` plus one `GkState`.
///
/// ## Invariants
///
/// - **Scope coordinates are always populated.** After construction
///   `scope_coords` reflects this kernel's place in the comprehension
///   chain: leaf-first list of [`super::ScopeCoord`] from the kernel's
///   own scope up through every enclosing comprehension. Root-scope
///   kernels (no parent) start with their own coords (or empty).
///   [`Self::materialize_wiring_from_outer`] re-computes the path so post-bind it
///   includes the outer's chain. Consumers (presentation layer,
///   inspector, scope-aware diagnostics) call
///   [`Self::scope_coordinates`] without needing to walk the scope
///   tree themselves. See [`super::scope_coords`].
pub struct GkKernel {
    program: Arc<GkProgram>,
    state: GkState,
    /// Number of init-time constants folded during compilation.
    pub constants_folded: usize,
    /// Leaf-first scope-coordinate path. Maintained as an
    /// invariant — see struct docs.
    scope_coords: Vec<super::ScopeCoord>,
    /// SRD-67 Phase 5 — Rule 2 write-through bindings carried
    /// alongside the kernel for per-cycle commit. Each entry pairs
    /// an export name (which the kernel exposes as a cell-bound
    /// input slot) with the synthetic `__write_<name>` source
    /// output the rewrite emitted. Empty for the vast majority
    /// of kernels; populated by the SRD-67 builder when result-
    /// bindings or `shared` collisions trigger Rule 2.
    write_throughs: Vec<KernelWriteThrough>,
    /// Shared cells visible at this kernel's scope but with no
    /// matching input slot on this kernel's program (closure-
    /// binding economy elided the slot). Carried as a transit
    /// channel so a descendant whose program DOES declare the
    /// slot can attach the same cell handle.
    ///
    /// `materialize_wiring_from_outer` is the single writer: when binding
    /// child to parent, it attaches every parent-visible cell
    /// to whatever child input slot exists, and stores the
    /// remaining unattached cells here for further propagation.
    /// The activity layer never sees this directly — the typed
    /// `ScopeKernel::shared_cells_in_scope` returns the merged
    /// view.
    transit_cells: Vec<SharedCellEntry>,
}

/// SRD-67 Phase 5 — local data shape of a write-through binding
/// the kernel carries. Mirrors `subcontext::WriteThroughBinding`
/// but lives at this layer so [`GkKernel`] avoids a cyclic
/// dependency on the subcontext module (which already depends on
/// kernel types).
#[derive(Debug, Clone)]
pub(crate) struct KernelWriteThrough {
    pub export_name: String,
    pub source_output: String,
}

impl std::fmt::Debug for GkKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GkKernel")
            .field("program", &self.program)
            .finish()
    }
}

impl GkKernel {
    /// Create from pre-validated components (all inputs are coordinates).
    pub(crate) fn new(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        source: &str,
        context: &str,
    ) -> Self {
        let coord_count = input_names.len();
        let input_defs: Vec<InputDef> = input_names.into_iter()
            .map(|name| InputDef {
                name,
                default: Value::U64(0),
                port_type: crate::node::PortType::U64,
                kind: crate::kernel::InputKind::Coordinate,
            })
            .collect();
        let order: Vec<String> = output_map.keys().cloned().collect();
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, order,
                       std::collections::HashSet::new(),
                       HashMap::new(),
                       source, context, None, false).unwrap()
    }

    /// Create with explicit input definitions.
    ///
    /// Returns `Err` for init-binding contract violations (SRD 11
    /// §"Init Binding Contract" Plan A); these are always fatal
    /// regardless of strict mode.
    pub(crate) fn new_with_inputs(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        init_outputs: std::collections::HashSet<String>,
        output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Result<Self, String> {
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, output_order, init_outputs, output_modifiers, source, context, log, false)
    }

    /// Construct with strict mode.
    pub(crate) fn new_strict_with_inputs(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        init_outputs: std::collections::HashSet<String>,
        output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Result<Self, String> {
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, output_order, init_outputs, output_modifiers, source, context, log, true)
    }

    fn new_impl(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        init_outputs: std::collections::HashSet<String>,
        output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
        strict: bool,
    ) -> Result<Self, String> {
        let mut program = GkProgram::with_inputs(
            nodes, wiring, input_defs, coord_count, output_map, output_order,
            source, context,
        );
        // Mark init bindings BEFORE fold runs so the compile-time
        // check (Plan A) can validate each one's upstream chain.
        for name in &init_outputs {
            program.mark_init_output(name);
        }
        // SRD-13f Push D: install output modifiers BEFORE fold so
        // the lifecycle classifier sees `volatile`. Without this,
        // a `volatile` binding's producing node defaults to
        // CompileConst, fold replaces it with a literal, and the
        // workload's `volatile` declaration loses its "exclude
        // from program identity" guarantee.
        for (name, modifier) in &output_modifiers {
            program.set_output_modifier(name, *modifier);
        }
        let constants_folded = if strict {
            program.fold_init_constants_strict(log, true)?
        } else {
            program.fold_init_constants_with_log(log)?
        };
        let program = Arc::new(program);
        let mut state = program.create_state();
        // Populate buffers for folded constants so get_constant() works.
        let dummy = vec![0u64; program.coord_count()];
        state.set_inputs(&dummy);
        for name in program.output_names() {
            if let Some(&(node_idx, _)) = program.output_map.get(name) {
                if program.wiring[node_idx].is_empty() {
                    state.pull(&program, name);
                }
            }
        }
        seed_shared_cells(&mut state, &program);
        state.core.seed_output_cells(&program);
        let mut k = Self {
            program,
            state,
            constants_folded,
            scope_coords: Vec::new(),
            write_throughs: Vec::new(),
            transit_cells: Vec::new(),
        };
        k.refresh_scope_coordinates();
        Ok(k)
    }

    /// Mark a set of output names as inherited (cascade-only)
    /// on the program. Must be called immediately after
    /// construction, before the `Arc<GkProgram>` is shared.
    /// Panics if the Arc has other references.
    pub fn mark_inherited_outputs<I>(&mut self, names: I)
    where I: IntoIterator<Item = String>
    {
        let program = Arc::get_mut(&mut self.program)
            .expect("mark_inherited_outputs called after program was shared");
        for name in names {
            program.mark_inherited(&name);
        }
    }

    /// Bake Rule 2 write-through bindings onto the underlying
    /// program. Must be called immediately after construction,
    /// before the `Arc<GkProgram>` is shared. Panics if the Arc
    /// has other references. Also updates this kernel's own
    /// `write_throughs` field so the just-built kernel matches
    /// what later `from_program` callers will see.
    ///
    /// The single legitimate caller is the SRD-67 builder's
    /// finalize step. The bake-into-program approach replaces
    /// the prior side-channel where the activity layer carried
    /// write-throughs alongside the program; now any kernel
    /// built from the program inherits the bindings via
    /// `from_program`'s automatic seeding.
    pub(crate) fn bake_write_throughs(&mut self, write_throughs: Vec<KernelWriteThrough>) {
        let program = Arc::get_mut(&mut self.program)
            .expect("bake_write_throughs called after program was shared");
        program.set_write_throughs(write_throughs.clone());
        self.write_throughs = write_throughs;
    }

    /// Construct a fresh kernel from a previously-compiled
    /// `Arc<GkProgram>`. The state is freshly created and seeded
    /// the same way the standard new-kernel path does, so callers
    /// can immediately `set_input(...)` for externs and execute.
    ///
    /// Used by the cache-and-rebind path in nbrs-activity (SRD 18b
    /// §"Cache-and-rebind contract"): a phase scope compiles once,
    /// caches its program, and instantiates a fresh kernel per
    /// `run_phase` call against the cached program.
    pub(crate) fn from_program(program: Arc<GkProgram>) -> Self {
        let mut state = program.create_state();
        // Populate buffers for folded constants so get_constant()
        // works on the new kernel — mirrors the seeding done in
        // `new_impl` after fold.
        let dummy = vec![0u64; program.coord_count()];
        state.set_inputs(&dummy);
        for name in program.output_names() {
            if let Some(&(node_idx, _)) = program.output_map.get(name)
                && program.wiring[node_idx].is_empty()
            {
                state.pull(&program, name);
            }
        }
        seed_shared_cells(&mut state, &program);
        state.core.seed_output_cells(&program);
        // Auto-seed the kernel's Rule 2 write-through bindings
        // from the program. The program is the single source of
        // truth; any kernel built from it inherits the same
        // bindings — eliminating the side-channel that the
        // activity-layer fiber-rebuild path used to need.
        let write_throughs = program.write_throughs().to_vec();
        let mut k = Self {
            program,
            state,
            constants_folded: 0, // already folded; see program contents
            scope_coords: Vec::new(),
            write_throughs,
            transit_cells: Vec::new(),
        };
        k.refresh_scope_coordinates();
        k
    }

    /// The shared immutable program.
    pub fn program(&self) -> &Arc<GkProgram> {
        &self.program
    }

    /// SRD-67 Phase 5 — attach Rule 2 write-through bindings to
    /// this kernel. Per-cycle eval calls
    /// [`Self::commit_write_throughs`] after the inputs flowing
    /// into the result-binding expressions are written; the
    /// commit walks each binding, pulls its synthetic source
    /// output, and stores the value back through the cell-bound
    /// input slot for `export_name`. Because the slot was
    /// attached to the parent's `SharedCell` at
    /// `materialize_wiring_from_outer` time, the write fans through.
    ///
    /// The bridge (`build_kernel_under_parent_full`) sets these
    /// in one shot at construction; per-cycle code never mutates
    /// them.
    // Used only by the SRD-67 subcontext tests today — the
    // production path auto-seeds write-throughs in
    // `from_program`, never needing a post-construction setter.
    // Kept for the test surface; dead-code-lint silenced.
    #[allow(dead_code)]
    pub(crate) fn set_write_throughs(&mut self, write_throughs: Vec<KernelWriteThrough>) {
        self.write_throughs = write_throughs;
    }

    /// The Rule 2 write-through bindings carried by this kernel.
    /// Empty for kernels without result-bindings or `shared`
    /// collisions.
    #[allow(dead_code)]
    pub(crate) fn write_throughs(&self) -> &[KernelWriteThrough] {
        &self.write_throughs
    }

    /// SRD-67 Phase 5 — per-cycle commit. Pulls each write-
    /// through's synthetic source output and stores its value
    /// through the corresponding cell-bound input slot for the
    /// declared export name. Reads of that name in the parent or
    /// in sibling kernels share the same cell and observe the
    /// write on the next read.
    ///
    /// No-op when the kernel carries no write-throughs.
    pub fn commit_write_throughs(&mut self) {
        let debug = crate::nodes::debug_nodes_enabled();
        if self.write_throughs.is_empty() {
            if debug {
                crate::audit::debug("commit_write_throughs: kernel has zero bindings — no-op");
            }
            return;
        }
        // Two-pass: pull each value first (each pull mutates the
        // state), collect, then write to the slot. Avoids
        // overlapping borrows on `self.state` / `self.program`.
        // For cell-bound slots `set_input` writes through the
        // cell (single-register: cell IS the slot's register);
        // for non-cell slots it updates the local register.
        let mut pending: Vec<(usize, Value)> = Vec::with_capacity(self.write_throughs.len());
        let bindings = self.write_throughs.clone();
        if debug {
            crate::audit::debug(&format!(
                "commit_write_throughs: {} binding(s)",
                bindings.len()
            ));
        }
        for wt in &bindings {
            let Some(idx) = self.program.find_input(&wt.export_name) else {
                if debug {
                    crate::audit::debug(&format!(
                        "commit_write_throughs: skip {} — no input slot",
                        wt.export_name
                    ));
                }
                continue;
            };
            let value = self.state.pull(&self.program, &wt.source_output).clone();
            if debug {
                crate::audit::debug(&format!(
                    "commit_write_throughs: {} → {}",
                    wt.export_name,
                    value.to_display_string()
                ));
            }
            pending.push((idx, value));
        }
        for (idx, value) in pending {
            self.state.set_input(idx, value);
        }
    }

    /// Set source schemas on the program (called by the compiler).
    pub fn set_cursor_schemas(&mut self, schemas: Vec<crate::source::SourceSchema>) {
        Arc::get_mut(&mut self.program)
            .expect("set_cursor_schemas must be called before program is shared")
            .set_cursor_schemas(schemas);
    }

    /// Attach the parsed AST as live program metadata. Called by
    /// every DSL compile entry point immediately after the
    /// assembler produces the kernel, while the program Arc is
    /// still uniquely owned. The subscope synthesizer
    /// (SRD-13f §"Wire-reference classification") queries this
    /// to integrate parent bindings' matter into child scopes.
    pub fn set_ast(&mut self, ast: Arc<crate::dsl::ast::GkFile>) {
        Arc::get_mut(&mut self.program)
            .expect("set_ast must be called before program is shared")
            .set_ast(ast);
    }

    /// The per-fiber mutable evaluation state.
    pub fn state(&mut self) -> &mut GkState {
        &mut self.state
    }

    /// Read-only access to the kernel's evaluation state. Used by
    /// callers (e.g. the scope-init pass) that need to inspect
    /// pulled values without consuming the kernel.
    pub fn state_ref(&self) -> &GkState {
        &self.state
    }

    /// Convenience: set coordinate inputs on the owned state.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        self.state.set_inputs(coords);
    }

    /// Read an input value by name. Cell-aware: cell-bound
    /// slots return the cell's current value.
    pub fn get_input(&self, name: &str) -> Option<Value> {
        self.program.find_input(name)
            .map(|idx| self.state.get_input(idx))
    }

    /// Convenience: pull from the owned state.
    pub fn pull(&mut self, output_name: &str) -> &Value {
        self.state.pull(&self.program, output_name)
    }

    /// Return the names of the inputs.
    pub fn input_names(&self) -> Vec<String> {
        self.program.input_names()
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.program.output_names()
    }

    /// Read the value of a named output that was folded to a constant.
    ///
    /// Underlying primitive — prefer [`Self::lookup`] for
    /// scope-aware name resolution. This method only succeeds for
    /// constant-folded outputs whose buffer is populated; it
    /// returns `None` for auto-passthrough outputs (where the
    /// value lives in the input slot) and for cycle-dependent
    /// outputs that haven't been pulled.
    pub fn get_constant(&self, name: &str) -> Option<&Value> {
        let (node_idx, port_idx) = self.program.output_map.get(name)?;
        let val = &self.state.core.buffers[*node_idx][*port_idx];
        if matches!(val, Value::None) { None } else { Some(val) }
    }

    /// Look up a name in this kernel's scope.
    ///
    /// The canonical scope-aware read documented by SRD-16
    /// §"Visibility Rules: Shadowing": own-scope folded outputs
    /// shadow inherited extern values, with auto-passthrough
    /// outputs falling through to the input slot transparently.
    ///
    /// Resolution order:
    /// 1. Folded output buffer (compile-time constants).
    /// 2. Cell-aware input read (covers extern values bound via
    ///    `materialize_wiring_from_outer`, auto-passthrough outputs from
    ///    `inputs := (...)` / `extern`, and `shared`-cell-backed
    ///    slots — the cell is queried on every read so reads
    ///    pick up writes from sibling kernels intrinsically).
    ///
    /// Returns `None` when the name doesn't resolve in either
    /// tier or when the resolved value is `Value::None` (unset).
    ///
    /// Returns `Value` (owned, not borrowed) because shared-cell
    /// reads acquire a Mutex and clone out — there's no
    /// long-lived borrow into the cell. For non-shared slots
    /// the clone is cheap (Value's Clone is Arc-based for
    /// vectors, primitive copy otherwise).
    ///
    /// This is the single read API for scope-aware name lookup
    /// and is cell-aware by default — callers don't need to
    /// know whether a name is shared or not.
    pub fn lookup(&self, name: &str) -> Option<Value> {
        if let Some(v) = self.get_constant(name)
            && !matches!(v, Value::None)
        {
            return Some(v.clone());
        }
        let idx = self.program.find_input(name)?;
        let v = self.state.read_input_value(idx);
        if matches!(v, Value::None) { None } else { Some(v) }
    }

    /// Bind this kernel's extern inputs from an outer scope kernel.
    ///
    /// For each output in the outer kernel that matches an input
    /// name in this kernel:
    /// - If outer has a `SharedCell` attached to its
    ///   matching input slot (set up at outer's construction
    ///   for `shared`-modifier outputs that have a backing
    ///   input slot — see SRD-16 §"Mutability Rules: Shared
    ///   Mutable"), share that cell with this kernel's slot.
    ///   Both sides' `set_input` calls write through the cell;
    ///   `refresh_shared` syncs reads from it.
    /// - Otherwise, copy outer's current value into this
    ///   kernel's input slot via [`Self::lookup`] (one-way at
    ///   bind time, no live link).
    ///
    /// Outer is `&self` — cells are created at outer's
    /// construction time, so no mutation of outer is needed at
    /// bind time. Many concurrent inners can share the same
    /// outer-owned cell.
    ///
    /// Call this after construction, before moving the kernel
    /// into an `OpBuilder`.
    /// Materialize a sub-scope kernel under this kernel as
    /// parent. THE single primitive for parent → child kernel
    /// construction with cell propagation.
    ///
    /// Per SRD-67's "parent supervises sub-context construction":
    /// only the parent has the right to materialize a sub-scope
    /// kernel. The parent owns the cell cascade, the value-copy
    /// path for outputs, the scope-coordinate plumbing, and any
    /// pre-bind iter-var injection. Every other code path that
    /// needs a parent-bound child kernel routes through here —
    /// the underlying `materialize_wiring_from_outer` step is private to
    /// this impl and not callable from anywhere else in the
    /// crate.
    ///
    /// `iter_bindings` lets callers inject iter-var values
    /// before binding, matching `for_iteration`'s contract:
    /// values must be installed BEFORE
    /// `refresh_scope_coordinates` runs so the own-coord
    /// snapshot sees them.
    ///
    /// # Side-channel lock
    ///
    /// `materialize_wiring_from_outer` is private to this impl block. The
    /// following must NOT compile (anyone trying to bypass the
    /// typed primitive should be caught at the compiler):
    ///
    /// ```compile_fail,E0624
    /// use nbrs_variates::kernel::GkKernel;
    /// use nbrs_variates::dsl::compile::compile_gk;
    /// let parent = compile_gk("inputs := (cycle)\n").unwrap();
    /// let mut child = compile_gk("inputs := (cycle)\n").unwrap();
    /// child.materialize_wiring_from_outer(&parent); // ← private; refuses to compile
    /// ```
    pub(crate) fn materialize_subscope(
        &self,
        program: Arc<GkProgram>,
        iter_bindings: &[(String, Value)],
    ) -> GkKernel {
        let mut child = GkKernel::from_program(program);
        for (var, value) in iter_bindings {
            if let Some(idx) = child.program.find_input(var) {
                child.state.set_input(idx, value.clone());
            }
        }
        child.materialize_wiring_from_outer(self);
        child
    }

    /// Produce a fresh kernel that mirrors this one's program
    /// AND its full shared-cell view (own input-slot cells +
    /// transit cells). The cell handles are Arc-shared; the
    /// returned kernel reads/writes the same cells as `self`.
    ///
    /// Used by the typed-builder bridge
    /// (`build_kernel_under_parent_full`) when it needs an
    /// `Arc<ScopeKernel<RootMarker>>` standing in for a borrowed
    /// `&GkKernel` — the wrapping must reflect the LIVE parent's
    /// cell view, not just its program shape, otherwise Rule 2
    /// in the builder's finalize sees no cells and produces no
    /// write-throughs.
    pub(crate) fn snapshot_with_cells(&self) -> GkKernel {
        let mut snapshot = GkKernel::from_program(self.program.clone());
        snapshot.transit_cells = self.transit_cells.clone();
        // Re-attach every cell from `self`'s input slots onto
        // the matching input slot of `snapshot`. Slot indices
        // and names are isomorphic since the program is the
        // same Arc.
        for name in self.program.input_names() {
            let Some(idx) = self.program.find_input(&name) else { continue };
            let Some(cell) = self.state.shared_cell(idx) else { continue };
            snapshot.state.attach_shared_cell(idx, cell);
        }
        snapshot
    }

    /// SRD-13f §"The cross-scope wiring operation is matter-AST-
    /// driven at construction": materialize this kernel's input-
    /// slot wiring against `outer`'s exports. Reads `self.program`'s
    /// matter (its extern / shared / coord declarations) to decide
    /// each slot's materialization gradient — cell-attach for
    /// shared and computed outputs, value-copy for passthrough,
    /// transit-forward for cells with no matching local slot.
    ///
    /// Private; the only sanctioned construction path is
    /// `build_subscope` (which calls `materialize_subscope` /
    /// `adopt_subscope` internally). External callers don't see
    /// this operation directly.
    fn materialize_wiring_from_outer(&mut self, outer: &GkKernel) {
        // Step 1 — typed shared-cell cascade. Compute every
        // cell visible at the outer scope: cells on outer's
        // own input slots (its `shared X := …` declarations
        // and any cells inherited from its own ancestors that
        // landed on slots) PLUS outer's transit cells (cells
        // outer carried forward as a transit because outer's
        // program had no matching slot). Together these are
        // every cell a descendant could legitimately bind to.
        //
        // Attach each cell to whichever child input slot
        // exists; drop cells whose name the child has already
        // attached itself to (idempotent reattach with the
        // same handle is a no-op, but a name collision with
        // a DIFFERENT cell would be a contract violation —
        // not observed in practice). Cells with no matching
        // child slot are stored on the child as transit so
        // a deeper descendant can pick them up.
        let outer_cells = outer.shared_cells_in_scope();
        let mut transit_forward: Vec<SharedCellEntry> = Vec::new();
        let mut attached_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for entry in outer_cells {
            if let Some(idx) = self.program.find_input(&entry.name) {
                self.state.attach_shared_cell(idx, entry.cell.clone());
                attached_names.insert(entry.name);
            } else {
                transit_forward.push(entry);
            }
        }
        self.transit_cells = transit_forward;

        // Step 2 — SRD-13f read invariant. For each output on
        // outer that matches an input slot on inner:
        //
        // - If the name also exists as an *input slot* on
        //   outer (i.e. it's a passthrough output backed by
        //   an input slot — `extern X: T`, `shared X :=
        //   <lit>`, coord inputs like `cycle`), the canonical
        //   storage is the input slot. Step 1 already
        //   attached the cell for shared / iter-var slots;
        //   for plain passthrough we value-copy the current
        //   slot value. Cycle-derived coord propagation goes
        //   through the explicit set_inputs path on the
        //   inner kernel, not through this bind step.
        //
        // - Otherwise the name is a truly-computed output
        //   (node-backed, no input slot on outer). Attach
        //   outer's output broadcast cell to inner's input
        //   slot. Outer's `pull` writes the freshly computed
        //   value through the cell; inner reads through
        //   `read_input` transparently. The read invariant
        //   from SRD-13f §"The read invariant" holds because
        //   the chain restructure in `nbrs-activity` ensures
        //   inner and outer are per-fiber kernels in the
        //   same lineage — no shared-kernel race on the
        //   cell.
        for name in outer.program.output_names() {
            if attached_names.contains(name) { continue; }
            let Some(inner_idx) = self.program.find_input(name) else { continue };
            let outer_has_slot = outer.program.find_input(name).is_some();
            if outer_has_slot {
                if let Some(value) = outer.lookup(name) {
                    self.state.set_input(inner_idx, value);
                }
            } else if let Some(cell) = outer.state.core.output_cell(&outer.program, name) {
                self.state.attach_shared_cell(inner_idx, cell);
                attached_names.insert(name.to_string());
            } else if let Some(value) = outer.lookup(name) {
                self.state.set_input(inner_idx, value);
            }
        }

        // Step 3 — scope-coordinates plumbing. Path is now
        // `[own] ++ outer.scope_coordinates()`. Refresh own
        // (extern values may have just been populated above),
        // then prepend outer's frozen path.
        self.refresh_scope_coordinates();
        let outer_path = outer.scope_coordinates().to_vec();
        self.scope_coords.extend(outer_path);
    }

    /// SRD-13f Push B.2 — advance this kernel's broadcast
    /// state: pull every output that has an attached
    /// broadcast cell, forcing the eval cone to recompute
    /// against current inputs and writing the fresh value
    /// through the cell. Descendant kernels with input slots
    /// cell-attached to these outputs then observe the
    /// current value on their next `read_input` without any
    /// per-fiber-write coordination.
    ///
    /// Intended to run once per cycle on each per-fiber outer
    /// kernel whose outputs are visible to inner scopes. The
    /// alternative — validity-bit + auto-pull-on-stale-read
    /// — would put the trigger fully inside the GK engine
    /// (so inner reads transparently fetch fresh values),
    /// but requires the engine to track upstream dependencies
    /// across the cell boundary. This eager-broadcast form
    /// is simpler and lives entirely within the kernel's own
    /// surface: callers ask the kernel to advance its
    /// broadcasts; the kernel does the pulls; cells receive
    /// the values.
    pub fn advance_broadcasts(&mut self) {
        let program = self.program.clone();
        let n_outputs = program.output_names().len();
        for i in 0..n_outputs {
            if self.state.core.output_cells.get(i)
                .and_then(|c| c.as_ref()).is_some()
            {
                let name = program.output_names()[i].to_string();
                // SRD-13f Push D: some workload-level bindings
                // intentionally panic at specific cycles
                // (`throw_at(cycle, threshold, ...)` for the
                // resume-test fixture). Those panics belong to
                // the per-op evaluation path — the op's wire
                // resolution pulls the same wire and the
                // cascade catches the panic as a per-op error.
                // Here in the eager-broadcast pre-step we
                // suppress panics so the descendant pull path
                // remains the canonical error-handling site.
                let state = &mut self.state;
                let prog = &program;
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.pull(prog, &name);
                }));
            }
        }
    }

    /// Every shared cell visible at this kernel's scope —
    /// own input slots' attached cells unioned with the
    /// transit cells inherited from ancestors. The typed
    /// `ScopeKernel::shared_cells_in_scope` delegates here.
    ///
    /// Used by `materialize_wiring_from_outer` to compute the parent's
    /// full visible cell set and propagate it to the child.
    /// Public for the typed surface; semantics are the same
    /// as the typed accessor.
    pub fn shared_cells_in_scope(&self) -> Vec<SharedCellEntry> {
        let mut by_name: std::collections::HashMap<String, SharedCellEntry> =
            std::collections::HashMap::new();
        for entry in &self.transit_cells {
            by_name.insert(entry.name.clone(), entry.clone());
        }
        for name in self.program.input_names() {
            let Some(idx) = self.program.find_input(&name) else { continue };
            let Some(cell) = self.state.shared_cell(idx) else { continue };
            let port_type = self
                .program
                .input_port_type(&name)
                .unwrap_or(crate::node::PortType::Str);
            by_name.insert(name.clone(), SharedCellEntry { name, port_type, cell });
        }
        by_name.into_values().collect()
    }

    /// Construct a per-iteration kernel: clone `canonical`'s
    /// program, bind it to `parent`'s scope, and pre-load every
    /// `(var, value)` binding into the corresponding input slot.
    ///
    /// This is the canonical recipe for materialising the scope
    /// kernel at one specific iteration position — the runtime
    /// dispatcher uses it before descending into a comprehension
    /// iteration's children, and the pre-map walker uses it for
    /// the same purpose so nested for_each clauses with
    /// outer-iter-var interpolation (`vec_{profile}`) resolve
    /// at pre-map time.
    ///
    /// Owning the recipe here ensures both consumers produce
    /// identical kernels for identical inputs; previously each
    /// site reimplemented the three-step `from_program` →
    /// `materialize_wiring_from_outer` → `set_input` dance and could (and
    /// did) drift.
    pub fn for_iteration(
        canonical: &Arc<GkKernel>,
        parent: &Arc<GkKernel>,
        bindings: &[(String, Value)],
    ) -> Arc<GkKernel> {
        // Routes through the parent's typed materialization
        // primitive so cell propagation is uniform with every
        // other parent → child path.
        Arc::new(parent.materialize_subscope(canonical.program().clone(), bindings))
    }

    /// Recompute this kernel's *own* scope coordinates from
    /// the current state and overwrite [`Self::scope_coords`]
    /// with `[own]`. Used at construction time and at the start
    /// of [`Self::materialize_wiring_from_outer`] before extending with the
    /// outer chain. Internal — callers want
    /// [`Self::scope_coordinates`].
    fn refresh_scope_coordinates(&mut self) {
        let own = self.compute_own_coordinates();
        self.scope_coords.clear();
        if !own.is_empty() {
            self.scope_coords.push(own);
        }
    }

    /// Compute the iteration coordinates this scope owns —
    /// every input slot tagged `IterationExtern` whose name
    /// isn't marked inherited in the program. Values come
    /// from the live state. Empty for non-comprehension
    /// scopes (workload root, scenario lists, individual
    /// phases).
    fn compute_own_coordinates(&self) -> super::ScopeCoord {
        use crate::kernel::InputKind;
        let mut vars = indexmap::IndexMap::new();
        for (idx, name) in self.program.input_names().into_iter().enumerate() {
            let kind = self.program.input_kind(idx);
            if kind != Some(InputKind::IterationExtern) { continue; }
            if self.program.is_inherited(&name) { continue; }
            let value = self.state.get_input(idx);
            if matches!(value, Value::None) { continue; }
            vars.insert(name, value.clone());
        }
        super::ScopeCoord { vars }
    }

    /// The leaf-first scope coordinate path — see the
    /// [`super::scope_coords`] module doc for the formal
    /// definition. Always reflects the current binding state:
    /// after [`Self::materialize_wiring_from_outer`] the path includes the
    /// outer kernel's full chain; for root scopes the path is
    /// just this kernel's own coords (or empty).
    pub fn scope_coordinates(&self) -> &[super::ScopeCoord] {
        &self.scope_coords
    }

    // `propagate_shared_to` retired in favor of SharedCell-backed
    // input slots — writes from inner kernels flow through the
    // cell's Mutex automatically, no scope-exit copy needed. See
    // SRD-16 §"Mutability Rules: Shared Mutable".

    /// Extract the scope values that were set via `materialize_wiring_from_outer`.
    /// Returns `[(name, value)]` for inputs that are not at their
    /// default. Used by `OpBuilder` to inject the same values into
    /// every fiber's state, including per-op-template kernels
    /// whose input layout differs from this kernel's. The name-
    /// keyed shape is the cross-kernel-safe contract: an index
    /// captured against this kernel's layout is meaningless when
    /// applied to a kernel synthesised from a different source
    /// (different extern declaration order, lazy-cascade omissions,
    /// etc.). Naming the binding makes the cross-scope write
    /// unambiguous — a missing name on the target program is a
    /// no-op rather than a silently mis-routed write.
    pub fn scope_values(&self) -> Vec<(String, Value)> {
        let mut values = Vec::new();
        for (i, name) in self.program.input_names().into_iter().enumerate() {
            let val = self.state.get_input(i);
            if !matches!(val, Value::None) {
                values.push((name, val.clone()));
            }
        }
        values
    }

    /// Extract the program for concurrent use.
    pub fn into_program(self) -> Arc<GkProgram> {
        self.program
    }
}
