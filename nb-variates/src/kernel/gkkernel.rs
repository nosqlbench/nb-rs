// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GkKernel: a compiled GK kernel pairing an Arc<GkProgram> with a GkState.

use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{GkNode, Value};
use super::{WireSource, InputDef};
use super::program::GkProgram;
use super::engines::GkState;

/// A compiled GK kernel: an `Arc<GkProgram>` plus one `GkState`.
pub struct GkKernel {
    program: Arc<GkProgram>,
    state: GkState,
    /// Number of init-time constants folded during compilation.
    pub constants_folded: usize,
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
            .map(|name| InputDef { name, default: Value::U64(0), port_type: crate::node::PortType::U64 })
            .collect();
        let order: Vec<String> = output_map.keys().cloned().collect();
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, order, source, context, None, false).unwrap()
    }

    /// Create with explicit input definitions.
    pub(crate) fn new_with_inputs(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Self {
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, output_order, source, context, log, false).unwrap()
    }

    /// Construct with strict mode.
    pub(crate) fn new_strict_with_inputs(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Result<Self, String> {
        Self::new_impl(nodes, wiring, input_defs, coord_count, output_map, output_order, source, context, log, true)
    }

    fn new_impl(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        source: &str,
        context: &str,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
        strict: bool,
    ) -> Result<Self, String> {
        let mut program = GkProgram::with_inputs(
            nodes, wiring, input_defs, coord_count, output_map, output_order,
            source, context,
        );
        let constants_folded = if strict {
            program.fold_init_constants_strict(log, true)?
        } else {
            program.fold_init_constants_with_log(log)
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
        Ok(Self { program, state, constants_folded })
    }

    /// Apply output binding modifiers to the program.
    ///
    /// Must be called immediately after construction, before the
    /// `Arc<GkProgram>` is shared. Panics if the Arc has other
    /// references.
    pub(crate) fn set_output_modifiers(&mut self, modifiers: &std::collections::HashMap<String, crate::dsl::ast::BindingModifier>) {
        let program = Arc::get_mut(&mut self.program)
            .expect("set_output_modifiers called after program was shared");
        for (name, modifier) in modifiers {
            program.set_output_modifier(name, *modifier);
        }
    }

    /// Construct a fresh kernel from a previously-compiled
    /// `Arc<GkProgram>`. The state is freshly created and seeded
    /// the same way the standard new-kernel path does, so callers
    /// can immediately `set_input(...)` for externs and execute.
    ///
    /// Used by the cache-and-rebind path in nb-activity (SRD 18b
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
        Self {
            program,
            state,
            constants_folded: 0, // already folded; see program contents
        }
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
    pub fn get_constant(&self, name: &str) -> Option<&Value> {
        let (node_idx, port_idx) = self.program.output_map.get(name)?;
        let val = &self.state.core.buffers[*node_idx][*port_idx];
        if matches!(val, Value::None) { None } else { Some(val) }
    }

    /// Bind this kernel's extern inputs from an outer scope kernel.
    ///
    /// For each output in the outer kernel that matches an input name
    /// in this kernel, copies the outer value into this kernel's
    /// input state. This is the scope composition wiring described
    /// in sysref 16 — the inner kernel sees outer constants as
    /// pre-populated inputs.
    ///
    /// Call this after construction, before moving the kernel into
    /// an `OpBuilder`.
    pub fn bind_outer_scope(&mut self, outer: &GkKernel) {
        for name in outer.program.output_names() {
            if let Some(inner_idx) = self.program.find_input(name) {
                if let Some(value) = outer.get_constant(name) {
                    self.state.set_input(inner_idx, value.clone());
                }
            }
        }
    }

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
