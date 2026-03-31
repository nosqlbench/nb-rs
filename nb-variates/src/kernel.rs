// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK runtime kernel: compiled DAG with pull-through evaluation.
//!
//! Split into two parts:
//! - `GkProgram`: the immutable compiled DAG (nodes, wiring, output map).
//!   Shared via `Arc` across all fibers/threads. No mutable state.
//! - `GkState`: per-fiber mutable evaluation state (buffers, generation,
//!   coordinates, volatile/sticky ports). Created cheaply from a program.
//!
//! `GkKernel` is the combined convenience type that owns both for
//! single-threaded use. For concurrent use, share the program and
//! give each fiber its own state.

use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{GkNode, Value};

/// Source of a value for a node input port.
#[derive(Debug, Clone)]
pub enum WireSource {
    /// An input coordinate, by index into the coordinate tuple.
    Coordinate(usize),
    /// Output of another node: `(node_index, output_port_index)`.
    NodeOutput(usize, usize),
}

/// The immutable compiled DAG. Shared across fibers via `Arc`.
///
/// Contains the node instances, wiring, and output map. No mutable
/// state — all evaluation state lives in `GkState`.
///
/// Thread safety: `GkProgram` is `Send + Sync`. The `Box<dyn GkNode>`
/// instances are `Send + Sync` (required by the trait). The wiring
/// and output map are read-only after construction.
pub struct GkProgram {
    /// Node instances in topological order.
    nodes: Vec<Box<dyn GkNode>>,
    /// For each node, the wiring of its input ports.
    wiring: Vec<Vec<WireSource>>,
    /// Input coordinate names, in tuple order.
    coord_names: Vec<String>,
    /// Map from output variate name to `(node_index, output_port_index)`.
    output_map: HashMap<String, (usize, usize)>,
}

// SAFETY: GkNode requires Send + Sync. All other fields are read-only.
unsafe impl Send for GkProgram {}
unsafe impl Sync for GkProgram {}

impl std::fmt::Debug for GkProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GkProgram")
            .field("nodes", &self.nodes.len())
            .field("coords", &self.coord_names)
            .finish()
    }
}

impl GkProgram {
    /// Create a program from pre-validated, topologically-sorted components.
    pub(crate) fn new(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        coord_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
    ) -> Self {
        Self { nodes, wiring, coord_names, output_map }
    }

    /// Create a new evaluation state for this program.
    ///
    /// Each fiber/thread should have its own state. Creating state is
    /// cheap — just allocates the value buffers and generation counters.
    pub fn create_state(&self) -> GkState {
        let buffers: Vec<Vec<Value>> = self.nodes
            .iter()
            .map(|n| vec![Value::None; n.meta().outputs.len()])
            .collect();
        let node_count = self.nodes.len();
        let coord_count = self.coord_names.len();
        GkState {
            buffers,
            generation: 0,
            node_generation: vec![0; node_count],
            coords: vec![0; coord_count],
        }
    }

    /// Return the names of the input coordinates.
    pub fn coord_names(&self) -> &[String] {
        &self.coord_names
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_map.keys().map(|s| s.as_str()).collect()
    }

    /// Resolve an output name to its (node_index, port_index).
    pub fn resolve_output(&self, name: &str) -> Option<(usize, usize)> {
        self.output_map.get(name).copied()
    }

    /// Number of nodes in the program.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Probe the compile level of a node by index.
    pub fn node_compile_level(&self, idx: usize) -> crate::node::CompileLevel {
        crate::node::compile_level_of(self.nodes[idx].as_ref())
    }

    /// Probe the compile level of the last node (typically the output).
    pub fn last_node_compile_level(&self) -> crate::node::CompileLevel {
        if self.nodes.is_empty() {
            return crate::node::CompileLevel::Phase1;
        }
        self.node_compile_level(self.nodes.len() - 1)
    }
}

/// Per-fiber mutable evaluation state.
///
/// Contains the value buffers, generation counter, and current
/// coordinates. Each fiber/thread owns one of these. No sharing,
/// no synchronization, no blocking.
///
/// Setting coordinates (`set_coordinates`) marks the beginning of
/// an isolation scope. All cached node outputs are implicitly
/// invalidated. No other fiber can interact with this state.
pub struct GkState {
    /// Per-node output value buffers, reused across evaluations.
    buffers: Vec<Vec<Value>>,
    /// Current generation (advances on each coordinate change).
    generation: u64,
    /// Per-node: the generation at which this node was last evaluated.
    node_generation: Vec<u64>,
    /// Current coordinate values.
    coords: Vec<u64>,
}

impl GkState {
    /// Set new coordinates and begin a new isolation scope.
    ///
    /// All cached node outputs are implicitly invalidated (the
    /// generation counter advances). Volatile ports would be reset
    /// here (future: SRD 28).
    pub fn set_coordinates(&mut self, coords: &[u64]) {
        self.generation = self.generation.wrapping_add(1);
        self.coords.copy_from_slice(coords);
    }

    /// Pull a named output variate using the given program.
    ///
    /// Triggers lazy evaluation of upstream nodes as needed.
    /// The program is borrowed immutably — only the state mutates.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        let (node_idx, port_idx) = *program.output_map
            .get(output_name)
            .unwrap_or_else(|| panic!("unknown output variate: {output_name}"));
        self.eval_node(program, node_idx);
        &self.buffers[node_idx][port_idx]
    }

    fn eval_node(&mut self, program: &GkProgram, node_idx: usize) {
        if self.node_generation[node_idx] == self.generation {
            return;
        }

        // Gather inputs from coordinates and upstream nodes
        let wiring = &program.wiring[node_idx];
        let mut inputs: Vec<Value> = Vec::with_capacity(wiring.len());
        for source in wiring {
            let val = match source {
                WireSource::Coordinate(coord_idx) => Value::U64(self.coords[*coord_idx]),
                WireSource::NodeOutput(upstream_idx, port_idx) => {
                    self.eval_node(program, *upstream_idx);
                    self.buffers[*upstream_idx][*port_idx].clone()
                }
            };
            inputs.push(val);
        }

        // Evaluate: program is &, state is &mut — no conflict
        program.nodes[node_idx].eval(&inputs, &mut self.buffers[node_idx]);
        self.node_generation[node_idx] = self.generation;
    }
}

/// A compiled GK kernel: an `Arc<GkProgram>` plus one `GkState`.
///
/// The access pattern is identical in single-threaded and concurrent
/// use — you always go through `program()` and `state()`:
///
/// ```ignore
/// let mut kernel = compile_gk("h := hash(cycle)").unwrap();
/// kernel.state().set_coordinates(&[42]);
/// let val = kernel.state().pull(kernel.program(), "h");
/// ```
///
/// For concurrent use, clone the program and create per-fiber states:
///
/// ```ignore
/// let program = kernel.program().clone();  // Arc clone, cheap
/// // In each fiber:
/// let mut state = program.create_state();
/// state.set_coordinates(&[cycle]);
/// let val = state.pull(&program, "h");
/// ```
///
/// The pattern is the same — `state.pull(&program, name)`. The only
/// difference is who owns the state and how the program is shared.
pub struct GkKernel {
    program: Arc<GkProgram>,
    state: GkState,
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
        coord_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
    ) -> Self {
        let program = Arc::new(GkProgram::new(nodes, wiring, coord_names, output_map));
        let state = program.create_state();
        Self { program, state }
    }

    /// The shared immutable program. Borrow or `Arc::clone` for
    /// concurrent use.
    pub fn program(&self) -> &Arc<GkProgram> {
        &self.program
    }

    /// The per-fiber mutable evaluation state.
    pub fn state(&mut self) -> &mut GkState {
        &mut self.state
    }

    /// Convenience: set coordinates on the owned state.
    pub fn set_coordinates(&mut self, coords: &[u64]) {
        self.state.set_coordinates(coords);
    }

    /// Convenience: pull from the owned state using the owned program.
    pub fn pull(&mut self, output_name: &str) -> &Value {
        self.state.pull(&self.program, output_name)
    }

    /// Return the names of the input coordinates.
    pub fn coord_names(&self) -> &[String] {
        self.program.coord_names()
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.program.output_names()
    }

    /// Extract the program for concurrent use, consuming the kernel.
    pub fn into_program(self) -> Arc<GkProgram> {
        self.program
    }
}
