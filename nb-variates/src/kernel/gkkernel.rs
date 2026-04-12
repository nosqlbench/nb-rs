// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GkKernel: a compiled GK kernel pairing an Arc<GkProgram> with a GkState.

use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{GkNode, Value};
use super::{WireSource};
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
    /// Create from pre-validated components (called by the assembler).
    pub(crate) fn new(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
    ) -> Self {
        Self::new_with_log(nodes, wiring, input_names, output_map, None)
    }

    /// Create from pre-validated components, emitting compile events to the log.
    pub(crate) fn new_with_log(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Self {
        let mut program = GkProgram::new(nodes, wiring, input_names, output_map);
        let constants_folded = program.fold_init_constants_with_log(log);
        let program = Arc::new(program);
        let state = program.create_state();
        Self { program, state, constants_folded }
    }

    /// Construct with strict mode: config wire violations are errors.
    pub(crate) fn new_strict(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Result<Self, String> {
        let mut program = GkProgram::new(nodes, wiring, input_names, output_map);
        let constants_folded = program.fold_init_constants_strict(log, true)?;
        let program = Arc::new(program);
        let state = program.create_state();
        Ok(Self { program, state, constants_folded })
    }

    /// The shared immutable program.
    pub fn program(&self) -> &Arc<GkProgram> {
        &self.program
    }

    /// The per-fiber mutable evaluation state.
    pub fn state(&mut self) -> &mut GkState {
        &mut self.state
    }

    /// Convenience: set coordinates on the owned state.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        self.state.set_inputs(coords);
    }

    /// Read a coordinate value by name.
    pub fn get_input(&self, name: &str) -> Option<u64> {
        self.program.input_names().iter()
            .position(|n| n == name)
            .map(|idx| self.state.get_input(idx))
    }

    /// Convenience: pull from the owned state.
    pub fn pull(&mut self, output_name: &str) -> &Value {
        self.state.pull(&self.program, output_name)
    }

    /// Return the names of the input coordinates.
    pub fn input_names(&self) -> &[String] {
        self.program.input_names()
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.program.output_names()
    }

    /// Read the value of a named output that was folded to a constant
    /// at init time. Returns `None` if the output doesn't exist or
    /// wasn't folded (i.e., it depends on coordinates).
    pub fn get_constant(&self, name: &str) -> Option<&Value> {
        let (node_idx, port_idx) = self.program.output_map.get(name)?;
        let val = &self.state.core.buffers[*node_idx][*port_idx];
        if matches!(val, Value::None) { None } else { Some(val) }
    }

    /// Set global values (resolved workload params) on the program.
    /// Must be called before `into_program()` or `program().clone()`.
    pub fn set_globals(&mut self, globals: HashMap<String, String>) {
        Arc::get_mut(&mut self.program)
            .expect("set_globals must be called before program is shared")
            .set_globals(globals);
    }

    /// Extract the program for concurrent use.
    pub fn into_program(self) -> Arc<GkProgram> {
        self.program
    }
}
