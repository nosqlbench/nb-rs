// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GkKernel: a compiled GK kernel pairing an Arc<GkProgram> with a GkState.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::node::{GkNode, Value};
use super::{WireSource, InputDef};
use super::program::GkProgram;
use super::engines::GkState;

/// Auto-create `SharedCell`s for `shared`-modifier outputs that
/// have a backing input slot on this kernel. Call once at
/// construction so subsequent `bind_outer_scope` from inner
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
        let init_value = state.get_input(idx).clone();
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
///   [`Self::bind_outer_scope`] re-computes the path so post-bind it
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
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Result<Self, String> {
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, output_order, init_outputs, source, context, log, false)
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
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Result<Self, String> {
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, output_order, init_outputs, source, context, log, true)
    }

    fn new_impl(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        init_outputs: std::collections::HashSet<String>,
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
        let mut k = Self {
            program,
            state,
            constants_folded,
            scope_coords: Vec::new(),
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

    /// Apply output binding modifiers to the program.
    ///
    /// Must be called immediately after construction, before the
    /// `Arc<GkProgram>` is shared. Panics if the Arc has other
    /// references.
    ///
    /// After applying modifiers, re-seeds shared cells: the
    /// state was constructed before any modifier was set on the
    /// program, so its initial seeding pass found no shared
    /// outputs. Now that the modifiers are in place, any
    /// `shared`-modifier output that has a backing input slot
    /// gets a `SharedCell` attached.
    pub(crate) fn set_output_modifiers(&mut self, modifiers: &std::collections::HashMap<String, crate::dsl::ast::BindingModifier>) {
        let program = Arc::get_mut(&mut self.program)
            .expect("set_output_modifiers called after program was shared");
        for (name, modifier) in modifiers {
            program.set_output_modifier(name, *modifier);
        }
        seed_shared_cells(&mut self.state, &self.program);
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
    pub fn from_program(program: Arc<GkProgram>) -> Self {
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
        let mut k = Self {
            program,
            state,
            constants_folded: 0, // already folded; see program contents
            scope_coords: Vec::new(),
        };
        k.refresh_scope_coordinates();
        k
    }

    /// The shared immutable program.
    pub fn program(&self) -> &Arc<GkProgram> {
        &self.program
    }

    /// Set source schemas on the program (called by the compiler).
    pub fn set_cursor_schemas(&mut self, schemas: Vec<crate::source::SourceSchema>) {
        Arc::get_mut(&mut self.program)
            .expect("set_cursor_schemas must be called before program is shared")
            .set_cursor_schemas(schemas);
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

    /// Read an input value by name.
    pub fn get_input(&self, name: &str) -> Option<&Value> {
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
    ///    `bind_outer_scope`, auto-passthrough outputs from
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
    pub fn bind_outer_scope(&mut self, outer: &GkKernel) {
        for name in outer.program.output_names() {
            let Some(inner_idx) = self.program.find_input(name) else { continue };
            // If outer has a shared cell for this name, share it.
            // Otherwise, fall through to the value-copy path.
            if let Some(outer_idx) = outer.program.find_input(name)
                && let Some(cell) = outer.state.shared_cell(outer_idx)
            {
                self.state.attach_shared_cell(inner_idx, cell);
                continue;
            }
            if let Some(value) = outer.lookup(name) {
                self.state.set_input(inner_idx, value);
            }
        }
        // Maintain the scope-coordinates invariant: the path
        // is now `[own] ++ outer.scope_coordinates()`. Refresh
        // own (extern values may have just been populated by
        // the loop above), then prepend outer's frozen path.
        self.refresh_scope_coordinates();
        let outer_path = outer.scope_coordinates().to_vec();
        self.scope_coords.extend(outer_path);
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
    /// `bind_outer_scope` → `set_input` dance and could (and
    /// did) drift.
    pub fn for_iteration(
        canonical: &Arc<GkKernel>,
        parent: &Arc<GkKernel>,
        bindings: &[(String, Value)],
    ) -> Arc<GkKernel> {
        let mut k = GkKernel::from_program(canonical.program().clone());
        // Iter-var values must be installed *before*
        // `bind_outer_scope` runs `refresh_scope_coordinates`,
        // otherwise the own-coord snapshot it takes sees the
        // default (None) for every iter-var slot and the
        // resulting `scope_coordinates()` path is missing this
        // scope's own stratum entirely. The iter-var names
        // aren't in `parent`'s output set, so the subsequent
        // `bind_outer_scope` call doesn't overwrite them.
        for (var, value) in bindings {
            if let Some(idx) = k.program().find_input(var) {
                k.state().set_input(idx, value.clone());
            }
        }
        k.bind_outer_scope(parent);
        Arc::new(k)
    }

    /// Recompute this kernel's *own* scope coordinates from
    /// the current state and overwrite [`Self::scope_coords`]
    /// with `[own]`. Used at construction time and at the start
    /// of [`Self::bind_outer_scope`] before extending with the
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
    /// after [`Self::bind_outer_scope`] the path includes the
    /// outer kernel's full chain; for root scopes the path is
    /// just this kernel's own coords (or empty).
    pub fn scope_coordinates(&self) -> &[super::ScopeCoord] {
        &self.scope_coords
    }

    // `propagate_shared_to` retired in favor of SharedCell-backed
    // input slots — writes from inner kernels flow through the
    // cell's Mutex automatically, no scope-exit copy needed. See
    // SRD-16 §"Mutability Rules: Shared Mutable".

    /// Extract the scope values that were set via `bind_outer_scope`.
    /// Returns `[(input_idx, value)]` for inputs that are not at
    /// their default. Used by `OpBuilder` to inject the same values
    /// into every fiber's state.
    pub fn scope_values(&self) -> Vec<(usize, Value)> {
        let mut values = Vec::new();
        let input_count = self.program.input_names().len();
        for i in 0..input_count {
            let val = self.state.get_input(i);
            if !matches!(val, Value::None) {
                // Check if it differs from the default (coordinate inputs
                // default to U64(0), extern inputs default to None)
                values.push((i, val.clone()));
            }
        }
        values
    }

    /// Extract the program for concurrent use.
    pub fn into_program(self) -> Arc<GkProgram> {
        self.program
    }
}
