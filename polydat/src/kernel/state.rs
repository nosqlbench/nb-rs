// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! PolydatKernel: a compiled Polydat Kernel pairing an Arc<PolydatProgram> with a PolydatState.

use std::collections::HashMap;
use std::sync::Arc;

use crate::ast::{PolydatNode, Value};
use super::{WireSource, InputDef};
use super::program::PolydatProgram;
use super::engines::{PolydatState, SharedCellEntry};

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
fn seed_shared_cells(state: &mut PolydatState, program: &PolydatProgram) {
    for name in program.shared_outputs() {
        let Some(idx) = program.find_input(name) else { continue };
        if state.shared_cell(idx).is_some() { continue; } // already seeded
        let init_value = state.get_input(idx);
        // `make_shared_cell` allocates the next bit position
        // from this scope's intent-dirty vector and constructs
        // the cell with the right validity-tracking handles
        // (cross_fiber_invalidation.md §3.1). The cell carries
        // its own intent_dirty Arc + bit so any fiber writing
        // through it publishes dirty intent to this scope's
        // vector — descendant kernels that later attach via
        // `materialize_wiring_from_outer` inherit the same
        // handles automatically.
        let cell = state.core.make_shared_cell(init_value);
        state.attach_shared_cell(idx, cell);
    }
}

/// γ-5 boundary-adapter helper: when the outer-scope binding's
/// runtime value type doesn't match the inner kernel's
/// declared slot type, consult the catalog
/// (`compile::assembly::auto_adapter`) and apply the adapter
/// if one exists. Returns the (possibly adapted) value to set
/// in the slot.
///
/// When no catalog entry exists for the (from, to) type pair,
/// returns the value unchanged with a one-line warning via
/// the audit log — the caller's `set_input` will then proceed
/// with the type-mismatched value, preserving pre-γ-5 behavior
/// for unhealable mismatches.
///
/// Spec: `expression_engine.md` §5.4 (boundary adapter
/// polyfills); `composition_substrate.md` T2 (typed-mismatch
/// healing extended to synthesis sites).
pub(crate) fn adapt_boundary_value(slot_name: &str, slot_type: crate::ast::PortType, value: Value) -> Value {
    let value_type = value.port_type();
    if value_type == slot_type {
        return value;
    }
    // `Value::None` is the "absent" sentinel — pass through
    // without trying to adapt; downstream None-propagation
    // (SRD-74) handles it.
    if matches!(value, Value::None) {
        return value;
    }
    match crate::compile::assembly::boundary_adapter(value_type, slot_type) {
        Some(adapter) => {
            // Adapter::eval reads inputs[0..N], writes outputs[0..M].
            // For the boundary case, every adapter is 1→1.
            let inputs = vec![value];
            let mut outputs = vec![Value::None];
            adapter.eval(&inputs, &mut outputs);
            outputs.remove(0)
        }
        None => {
            // Actionable warning surface — `Ext` as the slot
            // type is by far the most common landing point
            // here (it's the fallback when the auto-extern
            // inferrer couldn't resolve the binding's RHS
            // output type from the assembler or the surface
            // AST). The advice differs based on the slot's
            // declared type because the fix differs too:
            //
            // - Slot is `Ext`: the workload likely meant a
            //   primitive type. The inferrer surfaced its
            //   gap; the right fix is a registry update or
            //   an explicit `extern NAME: <type>` declaration
            //   so the slot's type matches the producer's.
            // - Slot is concrete: there's a real type
            //   mismatch the catalog can't bridge. The
            //   author wrote `extern NAME: <wrong_type>` or
            //   the consumer's declared port type doesn't
            //   match the actual cross-scope contract.
            let hint = if slot_type == crate::ast::PortType::Ext {
                "  - The slot's type defaulted to `Ext` (extension type) — the auto-extern \
                 inferrer couldn't resolve the binding's RHS to a concrete `PortType`. \
                 Options:\n\
                 \x20   * Add an explicit `extern {slot_name}: <type>` declaration in the \
                 receiving scope so the slot's type is pinned at the source.\n\
                 \x20   * If the binding is set from YAML sugar (e.g. `set: {{ {slot_name}: \"{{ outer }}\" }}`), \
                 the desugared `const {slot_name} := \"{{ outer }}\"` evaluates to a Str — \
                 use the bare form `set: {{ {slot_name}: outer }}` to pass the original \
                 type through, or quote-encode if the consumer expects a string.\n\
                 \x20   * File a registry gap if the binding's RHS function isn't recognized \
                 by `infer_auto_extern_type` — the function's output `PortType` should be \
                 surfaced via the DSL registry."
            } else {
                "  - The slot's declared type and the cross-scope provider's type don't match. \
                 Options:\n\
                 \x20   * Change the `extern {slot_name}: <type>` declaration to match the \
                 producer's actual type.\n\
                 \x20   * Convert at the consumer: wrap the read with the matching `as_*` / \
                 `*_from_*` adapter for the slot type."
            };
            let hint = hint
                .replace("{slot_name}", slot_name);
            crate::library::support::audit::warn(&format!(
                "boundary adapter: no catalog entry for {value_type:?} → {slot_type:?} \
                 at slot '{slot_name}'; passing value as-is (will likely produce a wire \
                 error or coerce silently at first read)\n\
                 {hint}"
            ));
            value
        }
    }
}

/// A compiled Polydat Kernel: an `Arc<PolydatProgram>` plus one `PolydatState`.
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
pub struct PolydatKernel {
    program: Arc<PolydatProgram>,
    state: PolydatState,
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
/// but lives at this layer so [`PolydatKernel`] avoids a cyclic
/// dependency on the subcontext module (which already depends on
/// kernel types).
#[derive(Debug, Clone)]
pub(crate) struct KernelWriteThrough {
    pub export_name: String,
    pub source_output: String,
}

impl std::fmt::Debug for PolydatKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolydatKernel")
            .field("program", &self.program)
            .finish()
    }
}

impl PolydatKernel {
    /// Create from pre-validated components (all inputs are coordinates).
    pub(crate) fn new(
        nodes: Vec<Box<dyn PolydatNode>>,
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
                port_type: crate::ast::PortType::U64,
                kind: crate::kernel::InputKind::Coordinate,
            })
            .collect();
        let order: Vec<String> = output_map.keys().cloned().collect();
        Self::new_with_inputs(nodes, wiring, input_defs, coord_count, output_map, order,
                       std::collections::HashSet::new(),
                       HashMap::new(),
                       source, context, None, false).unwrap()
    }

    /// Create with explicit input definitions. `strict` selects
    /// strict-mode const folding (config-wire violations become
    /// errors).
    ///
    /// Returns `Err` for init-binding contract violations (SRD 11
    /// §"Init Binding Contract" Plan A); these are always fatal
    /// regardless of strict mode.
    // Twelve parameters describe one thing — a compiled program
    // definition. A params struct is the right end state, but it
    // belongs to the construction-protocol reshape (SRD-13e
    // scope-as-module territory), not lint cleanup — this fn is
    // the SRD-67 walled-off construction chokepoint.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_inputs(
        nodes: Vec<Box<dyn PolydatNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        const_outputs: std::collections::HashSet<String>,
        output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
        strict: bool,
    ) -> Result<Self, String> {
        let mut program = PolydatProgram::with_inputs(
            nodes, wiring, input_defs, coord_count, output_map, output_order,
            source, context,
        );
        // Mark const bindings BEFORE fold runs so the compile-time
        // check (Plan A) can validate each one's upstream chain.
        for name in &const_outputs {
            program.mark_const_output(name);
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
        // Seed buffers for folded *constant* nullary nodes so
        // `get_constant()` works. Skip `Nondeterministic` nullary nodes
        // (live-metric readers, entropy, clocks): they have no
        // compile-time value, and pulling one here would evaluate it
        // against an empty/absent runtime source — mirrors the same
        // skip the fold pass makes (`fold_init_constants`).
        for name in program.output_names() {
            if let Some(&(node_idx, _)) = program.output_map.get(name)
                && program.wiring[node_idx].is_empty()
                && !matches!(
                    program.nodes[node_idx].purity(),
                    crate::ast::Purity::Nondeterministic { .. }
                )
            {
                state.pull(&program, name);
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
    /// construction, before the `Arc<PolydatProgram>` is shared.
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
    /// before the `Arc<PolydatProgram>` is shared. Panics if the Arc
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
    /// `Arc<PolydatProgram>`. The state is freshly created and seeded
    /// the same way the standard new-kernel path does, so callers
    /// can immediately `set_input(...)` for externs and execute.
    ///
    /// # Cache-and-rehydrate role
    ///
    /// This is the **rehydrate** primitive of the cache-and-
    /// rehydrate pattern documented on [`Self::for_iteration`].
    /// External callers use `for_iteration` (which composes
    /// this with parent-chain wiring); this method itself is
    /// `pub(crate)` because hydrating a kernel without
    /// installing parent-chain wiring would skip the load-
    /// bearing materialization step.
    ///
    /// Used by the cache-and-rebind path the host drives (SRD 18b
    /// §"Cache-and-rebind contract"): a phase scope compiles once,
    /// caches its program, and instantiates a fresh kernel per
    /// `run_phase` call against the cached program.
    pub(crate) fn from_program(program: Arc<PolydatProgram>) -> Self {
        let mut state = program.create_state();
        // Populate buffers for folded constants so get_constant()
        // works on the new kernel — mirrors the seeding done in
        // `new_with_inputs` after fold.
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
    pub fn program(&self) -> &Arc<PolydatProgram> {
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
        let debug = crate::library::debug_nodes_enabled();
        if self.write_throughs.is_empty() {
            if debug {
                crate::library::support::audit::debug("commit_write_throughs: kernel has zero bindings — no-op");
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
            crate::library::support::audit::debug(&format!(
                "commit_write_throughs: {} binding(s)",
                bindings.len()
            ));
        }
        for wt in &bindings {
            let Some(idx) = self.program.find_input(&wt.export_name) else {
                if debug {
                    crate::library::support::audit::debug(&format!(
                        "commit_write_throughs: skip {} — no input slot",
                        wt.export_name
                    ));
                }
                continue;
            };
            let value = self.state.pull(&self.program, &wt.source_output).clone();
            if debug {
                crate::library::support::audit::debug(&format!(
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
    pub fn set_cursor_schemas(&mut self, schemas: Vec<crate::iteration::source::SourceSchema>) {
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
    pub fn set_ast(&mut self, ast: Arc<crate::dsl::ast::PolydatFile>) {
        Arc::get_mut(&mut self.program)
            .expect("set_ast must be called before program is shared")
            .set_ast(ast);
    }

    /// The per-fiber mutable evaluation state.
    pub fn state(&mut self) -> &mut PolydatState {
        &mut self.state
    }

    /// Read-only access to the kernel's evaluation state. Used by
    /// callers (e.g. the scope-init pass) that need to inspect
    /// pulled values without consuming the kernel.
    pub fn state_ref(&self) -> &PolydatState {
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

    /// Pull a program output by its output-list **index**, skipping the
    /// name→index resolution `pull` does. Pair with
    /// [`PolydatProgram::output_index`] resolved ONCE (at bind time) so a
    /// per-cycle reader pays no name hash on the hot path.
    pub fn pull_by_index(&mut self, output_idx: usize) -> &Value {
        self.state.pull_by_index(&self.program, output_idx)
    }

    /// Copy `self`'s currently-set input-slot values into `child`'s
    /// input slots by name.
    ///
    /// Companion to the internal `materialize_wiring_from_outer`
    /// pass that runs as part of `build_subscope`. That pass
    /// walks the parent's outputs; this method walks the parent's
    /// **inputs** — so cascade-extern'd names that the parent
    /// inherited from *its* parent reach `child` too, rather than
    /// stopping at the parent and silently leaving `child`'s
    /// matching slot at its default.
    ///
    /// `Value::None` inputs are skipped (no point overwriting a
    /// child's possibly-set default with absence). Inputs whose
    /// name has no matching slot on `child` are skipped silently
    /// — they're not the child's concern.
    ///
    /// This is the kernel-chain operation that lets cascade-extern
    /// propagate transitively across multi-level scope chains. Each
    /// scope builder calls it after `build_subscope` finishes.
    pub fn propagate_inputs_into(&self, child: &mut PolydatKernel) {
        let names = self.program.input_names();
        for name in names {
            let Some(outer_value) = self.get_input(&name) else { continue };
            if matches!(outer_value, Value::None) { continue; }
            let cloned = outer_value.clone();
            let Some(inner_idx) = child.program.find_input(&name) else { continue };
            child.state.set_input(inner_idx, cloned);
        }
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

    /// Find every `const` output whose Plan B materialisation
    /// left the buffer as `Value::None`. The L2.f sub-axiom in
    /// composition_substrate.md describes this case: an
    /// intermediate-layer `const X := <expr>` whose RHS yields
    /// None falls through silently to the outer scope's X via
    /// the conditional-shadow semantics in none_semantics.md.
    /// This method is the substrate's "did silent fall-through
    /// occur" query — strict-mode callers (per L2.f's
    /// strict-mode hardening note) use it to escalate the
    /// silent fall-through to a hard error.
    ///
    /// Returns the const-output names whose buffers are
    /// `Value::None` after the scope-init pull. Empty `Vec`
    /// means every const materialised to a defined value.
    /// Polydat itself does not implement the strict-mode
    /// policy — it provides this query and the caller decides
    /// whether to surface a diagnostic.
    ///
    /// Call only after `materialize_wiring_from_outer` has run
    /// (i.e., after the kernel is fully constructed and
    /// scope-init pulls have completed). Calling before
    /// scope-init returns a misleading result.
    pub fn find_l2f_violations(&self) -> Vec<String> {
        self.program.const_outputs.iter()
            .filter(|name| {
                self.program.output_map.get(name.as_str())
                    .map(|(node_idx, port_idx)| {
                        matches!(
                            &self.state.core.buffers[*node_idx][*port_idx],
                            Value::None
                        )
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
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
    ///    `input ...: u64` / `extern`, and `shared`-cell-backed
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
        if let Some(idx) = self.program.find_input(name) {
            let v = self.state.read_input_value(idx);
            return if matches!(v, Value::None) { None } else { Some(v) };
        }
        // Dotted names follow the established field-access wire
        // convention (`a.b` lowers to the wire `a__b`), so a
        // text-context reference like `{q.cursor.idx}` resolves
        // through the same flattening the DSL compiler applies.
        if name.contains('.') {
            let flattened = name.replace('.', "__");
            return self.lookup(&flattened);
        }
        None
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
    /// use polydat::kernel::PolydatKernel;
    /// use polydat::dsl::compile::compile_polydat;
    /// let parent = compile_polydat("input cycle: u64\n").unwrap();
    /// let mut child = compile_polydat("input cycle: u64\n").unwrap();
    /// child.materialize_wiring_from_outer(&parent); // ← private; refuses to compile
    /// ```
    pub(crate) fn materialize_subscope(
        &self,
        program: Arc<PolydatProgram>,
        iter_bindings: &[(String, Value)],
    ) -> PolydatKernel {
        let mut child = PolydatKernel::from_program(program);
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
    /// `&PolydatKernel` — the wrapping must reflect the LIVE parent's
    /// cell view, not just its program shape, otherwise Rule 2
    /// in the builder's finalize sees no cells and produces no
    /// write-throughs.
    pub(crate) fn snapshot_with_cells(&self) -> PolydatKernel {
        let mut snapshot = PolydatKernel::from_program(self.program.clone());
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
    fn materialize_wiring_from_outer(&mut self, outer: &PolydatKernel) {
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
        // Names this scope declares as a local authoritative
        // output — `const NAME := …` (const-folded at compile
        // time) or `init NAME := …` (computed once at scope-init
        // after wiring, then fixed for the scope's lifetime).
        // Either form means this scope owns the binding for
        // `NAME` over its subtree, so any transit cell carrying
        // a stale value from a grandparent must be suppressed:
        // without that suppression, step 1's blanket cell-attach
        // would short-circuit step 2's value-copy from
        // `outer.lookup(name)` (already-in-attached_names
        // guard), and descendants would read the transit cell's
        // value instead of the local declaration's.
        //
        // The two forms are uniform from the chain's
        // perspective: both produce a single authoritative
        // value visible to descendants via the standard
        // `extern NAME` lookup. The distinction is internal
        // (when the value is computed) and doesn't affect the
        // shadowing semantics.
        let local_finals: std::collections::HashSet<&str> = self.program
            .output_names()
            .into_iter()
            // `const_outputs()` filters output_modifiers for CONST,
            // so checking the modifier directly is the same query —
            // single source of truth for "this scope authoritatively
            // owns NAME via a const binding."
            .filter(|n| {
                self.program.output_modifier(n)
                    == crate::dsl::ast::BindingModifier::CONST
            })
            .collect();
        for entry in outer_cells {
            // A local final on this scope is the canonical writer
            // for the name; the transit cell from above is stale.
            // Drop it on the floor — don't attach to a slot we
            // own, don't transit-forward to descendants. They'll
            // see this scope's final via the standard step-2
            // value-copy or cell-attach path.
            if local_finals.contains(entry.name.as_str()) {
                continue;
            }
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
        //   the chain restructure in `nbrs-runtime` ensures
        //   inner and outer are per-fiber kernels in the
        //   same lineage — no shared-kernel race on the
        //   cell.
        for name in outer.program.output_names() {
            if attached_names.contains(name) { continue; }
            let Some(inner_idx) = self.program.find_input(name) else { continue };
            let outer_has_slot = outer.program.find_input(name).is_some();
            // SRD-74 P2 transitive composition: when outer's output
            // is a `const` binding, ALWAYS go through outer.lookup
            // (value-copy), never through the broadcast cell. The
            // const's output buffer may be Value::None (Rule 1
            // None-propagation, e.g. set:'s `const X := "{Y}"` when
            // Y is unbound); outer.lookup applies the two-tier read
            // so None falls through to outer's wired-from-grandparent
            // input slot, giving us the canonical chain-walked
            // value. Cell-attaching the None-valued buffer would
            // defeat that fall-through.
            //
            // Const outputs are effectively-const for the scope's
            // lifetime (SRD-11) — value-copy is semantically
            // equivalent to cell-attach and avoids the dynamic-cell
            // overhead.
            let outer_is_const = outer.program.output_modifier(name)
                == crate::dsl::ast::BindingModifier::CONST;
            // Slot's declared port type — needed for γ-5
            // boundary-adapter dispatch. `find_input` returned
            // `Some(inner_idx)` above, so `input_port_type` on
            // the same name is a program-shape invariant; a
            // `None` here means the program is malformed.
            let inner_slot_type = self
                .program
                .input_port_type(name)
                .expect("input index resolved but no declared port type");
            if outer_has_slot || outer_is_const {
                // Both conditions force the chain-walking value-copy
                // path (see the const rationale above; an outer input
                // slot likewise reads through outer.lookup so the
                // grandparent fall-through applies).
                if let Some(value) = outer.lookup(name) {
                    let adapted = adapt_boundary_value(name, inner_slot_type, value);
                    self.state.set_input(inner_idx, adapted);
                }
            } else if let Some(cell) = outer.state.core.output_cell(&outer.program, name) {
                self.state.attach_shared_cell(inner_idx, cell);
                attached_names.insert(name.to_string());
            } else if let Some(value) = outer.lookup(name) {
                let adapted = adapt_boundary_value(name, inner_slot_type, value);
                self.state.set_input(inner_idx, adapted);
            } else if let Some(value) =
                crate::dsl::factories::resolve_extern(name, inner_slot_type)
            {
                // γ-8 virtual-wire resolver: outer chain has no
                // binding; a host-registered resolver provides one.
                let adapted = adapt_boundary_value(name, inner_slot_type, value);
                self.state.set_input(inner_idx, adapted);
            }
        }

        // Step 3 — materialize scope-init const outputs. A `const`
        // binding whose RHS depends on inputs (auto-extern,
        // iteration variable, params-kernel passthrough) can't
        // fold at compile time; its wiring stays node-backed and
        // its buffer is `Value::None` until something pulls it.
        // Now that step 2 has populated the input slots from the
        // outer chain, pull every const output once to capture
        // its effectively-const value for the lifetime of this
        // scope. After this point the buffer is frozen — the
        // const lifecycle promises immutability — so downstream
        // `lookup(name)` reads through `get_constant`'s buffer
        // path and sees the materialised value.
        //
        // Panics during the pull are caught (not swallowed) — a
        // const binding may depend on side-effectful resolution
        // (`dataset_prebuffer`, etc.) that isn't ready until the
        // workload actually runs, AND we want to surface real
        // type / arity / Value::None-coercion errors so they're
        // not hidden by the same catch. The recovery shape
        // (buffer stays None, consumer's eventual read re-
        // triggers the panic in context) is unchanged; the
        // additional behavior is a diagnostic on every caught
        // panic so operators can see the eval failure even when
        // the conditional-shadow fall-through papers over the
        // None buffer at the next lookup.
        let const_outputs: Vec<String> = self.program
            .const_outputs()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let program = self.program.clone();
        for name in const_outputs {
            let state = &mut self.state;
            let prog = &program;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                state.pull(prog, &name);
            }));
            if let Err(payload) = result {
                let msg = if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = payload.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "<non-string panic payload>".to_string()
                };
                // Single-line, diag-routed warning. Operators
                // see this in stderr / session.log immediately;
                // they don't have to wait until the const's
                // downstream consumer re-pulls and the panic
                // re-fires with full context.
                eprintln!(
                    "warning: scope-init const pull failed for '{name}': {msg} \
                     (buffer left at Value::None; downstream lookup will \
                     fall through to wired-in input or surface the error \
                     when the binding is consumed)"
                );
            }
        }

        // Step 4 — scope-coordinates plumbing. Path is now
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
    /// — would put the trigger fully inside the Polydat engine
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
            // `find_input` just returned `Some(idx)`; the program
            // shape guarantees a declared port type for that idx.
            let port_type = self
                .program
                .input_port_type(&name)
                .expect("input index resolved but no declared port type");
            by_name.insert(name.clone(), SharedCellEntry { name, port_type, cell });
        }
        by_name.into_values().collect()
    }

    /// Construct a per-iteration kernel: clone `canonical`'s
    /// program, bind it to `parent`'s scope, and pre-load every
    /// `(var, value)` binding into the corresponding input slot.
    ///
    /// # Cache-and-rehydrate pattern
    ///
    /// `for_iteration` is the public entry point for the
    /// **cache-and-rehydrate pattern** a host builds on:
    /// compile a scope's program **once**, then hydrate many
    /// per-instance kernels from it — one per iteration tuple,
    /// per fiber, per scenario-tree visit. The program is
    /// immutable substance (the `Arc<PolydatProgram>`); each
    /// hydrated kernel carries its own state (the input slot
    /// values for this iteration).
    ///
    /// The pattern's three load-bearing properties:
    ///
    /// 1. **Compile cost amortizes.** Polydat source → typed program
    ///    is paid once per canonical scope, not per iteration
    ///    or per fiber. The compiled `Arc<PolydatProgram>` is shared
    ///    via clone (cheap — refcount bump).
    /// 2. **Each hydrated kernel is independent.** Per-fiber
    ///    state means no synchronization between fibers running
    ///    the same iteration in parallel. Each `for_iteration`
    ///    call produces a fresh kernel with its own input
    ///    slots, output cells, and write-through bindings.
    /// 3. **Parent-chain wiring is uniform.** Every hydrated
    ///    kernel runs through the parent's
    ///    `materialize_subscope` (and downstream
    ///    `materialize_wiring_from_outer`) so cell propagation,
    ///    shared-cell attach, and the SRD-13f read-invariant
    ///    are byte-identical to any other parent → child path.
    ///
    /// # When to use this
    ///
    /// - **Per-iteration kernel construction** in scope
    ///   walkers and pre-map walkers. The runtime dispatcher
    ///   uses it before descending into a comprehension
    ///   iteration's children; the pre-map walker uses it so
    ///   nested `for_each` clauses with outer-iter-var
    ///   interpolation (`vec_{profile}`) resolve at pre-map
    ///   time.
    /// - **Cross-cutover migration paths.** The walker rewrite
    ///   in PR 9c-1b (see
    ///   `polydat/docs/design/comprehension_cutover_contact_surfaces.md`)
    ///   uses this method to hydrate per-iteration kernels
    ///   from the canonical scope kernel that
    ///   `build_for_each_scope_kernel` produced.
    ///
    /// # Why one entry point
    ///
    /// Owning the recipe here ensures both consumers (runtime
    /// dispatcher + pre-map walker) produce identical kernels
    /// for identical inputs. Pre-`for_iteration`, each site
    /// reimplemented the three-step
    /// `from_program` → `materialize_wiring_from_outer` →
    /// `set_input` dance and could — and did — drift.
    ///
    /// # See also
    ///
    /// - [`Self::from_program`] (internal) — the
    ///   build-fresh-state primitive `for_iteration` composes
    ///   with parent-chain wiring.
    /// - [`Self::propagate_inputs_into`] — the kernel-chain
    ///   operation that extends cascade-extern values into a
    ///   subkernel (called once after `for_iteration` from each
    ///   scope walker so multi-level cascades reach the
    ///   grandchild).
    pub fn for_iteration(
        canonical: &Arc<PolydatKernel>,
        parent: &Arc<PolydatKernel>,
        bindings: &[(String, Value)],
    ) -> Arc<PolydatKernel> {
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
            // Use `lookup` (two-tier: const buffer first, input
            // slot second) rather than reading the input slot
            // directly. The conditional-shadow `const NAME :=
            // <expr>` pattern from SRD-74 P2 makes NAME both an
            // input slot (wired with the outer scope's binding —
            // typically a workload-param default) AND a const
            // output (the iter-shadow result). The own-coordinate
            // should report the AUTHORITATIVE value the scope
            // publishes, which is the const buffer when present.
            // Reading the input slot directly would report the
            // wired-in default, masking the per-iter shadow value
            // in activity labels / scope-coord display paths.
            let Some(value) = self.lookup(&name) else { continue; };
            if matches!(value, Value::None) { continue; }
            vars.insert(name, value);
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
    pub fn into_program(self) -> Arc<PolydatProgram> {
        self.program
    }
}
