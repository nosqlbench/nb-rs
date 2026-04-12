// Copyright 2024-2026 Jonathan Shook
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
        Self::new_with_ports(nodes, wiring, input_names, output_map, Vec::new(), None)
    }

    /// Create from pre-validated components with external ports.
    pub(crate) fn new_with_ports(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        ports: Vec<super::PortDef>,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Self {
        let mut program = if ports.is_empty() {
            GkProgram::new(nodes, wiring, input_names, output_map)
        } else {
            GkProgram::with_ports(nodes, wiring, input_names, output_map, ports)
        };
        let constants_folded = program.fold_init_constants_with_log(log);
        let program = Arc::new(program);
        let mut state = program.create_state();
        // Populate buffers for folded constants so get_constant() works.
        // After constant folding, init-time nodes are replaced with
        // Const* nodes that have empty wiring. Pull them to populate
        // their output buffers. Skip nodes with any wiring (they
        // depend on inputs that aren't set yet).
        // Populate folded constants so get_constant() works.
        let dummy = vec![0u64; program.input_names().len()];
        state.set_inputs(&dummy);
        for name in program.output_names() {
            if let Some(&(node_idx, _)) = program.output_map.get(name) {
                if program.wiring[node_idx].is_empty() {
                    state.pull(&program, name);
                }
            }
        }
        Self { program, state, constants_folded }
    }

    /// Construct with strict mode: config wire violations are errors.
    pub(crate) fn new_strict_with_ports(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        ports: Vec<super::PortDef>,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Result<Self, String> {
        let mut program = if ports.is_empty() {
            GkProgram::new(nodes, wiring, input_names, output_map)
        } else {
            GkProgram::with_ports(nodes, wiring, input_names, output_map, ports)
        };
        let constants_folded = program.fold_init_constants_strict(log, true)?;
        let program = Arc::new(program);
        let mut state = program.create_state();
        let dummy_inputs = vec![0u64; program.input_names().len()];
        state.set_inputs(&dummy_inputs);
        for name in program.output_names() {
            if let Some(&(node_idx, _)) = program.output_map.get(name) {
                if program.wiring[node_idx].is_empty() {
                    state.pull(&program, name);
                }
            }
        }
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

    /// Extract the program for concurrent use.
    pub fn into_program(self) -> Arc<GkProgram> {
        self.program
    }
}
