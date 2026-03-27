// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK runtime kernel: compiled DAG with pull-through evaluation.

use std::collections::HashMap;

use crate::node::{GkNode, Value};

/// Source of a value for a node input port.
#[derive(Debug, Clone)]
pub enum WireSource {
    /// An input coordinate, by index into the coordinate tuple.
    Coordinate(usize),
    /// Output of another node: `(node_index, output_port_index)`.
    NodeOutput(usize, usize),
}

/// A compiled generation kernel.
///
/// Nodes are stored in topological order. Evaluation is pull-through:
/// an outer loop sets the coordinate context, then output variates
/// are pulled by name, triggering lazy evaluation of upstream nodes.
impl std::fmt::Debug for GkKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GkKernel")
            .field("nodes", &self.nodes.len())
            .field("coords", &self.coord_names)
            .finish()
    }
}

pub struct GkKernel {
    /// Node instances in topological order.
    nodes: Vec<Box<dyn GkNode>>,

    /// For each node, the wiring of its input ports.
    /// `wiring[node_index][input_port_index]` → `WireSource`.
    wiring: Vec<Vec<WireSource>>,

    /// Input coordinate names, in tuple order.
    coord_names: Vec<String>,

    /// Map from output variate name to `(node_index, output_port_index)`.
    output_map: HashMap<String, (usize, usize)>,

    /// Per-node output value buffers, reused across evaluations.
    buffers: Vec<Vec<Value>>,

    /// Current generation (advances on each new coordinate context).
    generation: u64,

    /// Per-node: the generation at which this node was last evaluated.
    node_generation: Vec<u64>,

    /// Current coordinate values.
    coords: Vec<u64>,
}

impl GkKernel {
    /// Create a kernel from pre-validated, topologically-sorted components.
    ///
    /// This is called by the assembly phase — not directly by users.
    pub(crate) fn new(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        coord_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
    ) -> Self {
        let buffers: Vec<Vec<Value>> = nodes
            .iter()
            .map(|n| vec![Value::None; n.meta().outputs.len()])
            .collect();
        let node_count = nodes.len();
        let coord_count = coord_names.len();
        Self {
            nodes,
            wiring,
            coord_names,
            output_map,
            buffers,
            generation: 0,
            node_generation: vec![0; node_count],
            coords: vec![0; coord_count],
        }
    }

    /// Set a new coordinate context and advance the generation counter.
    ///
    /// All cached node outputs are implicitly invalidated.
    pub fn set_coordinates(&mut self, coords: &[u64]) {
        assert_eq!(
            coords.len(),
            self.coord_names.len(),
            "coordinate tuple length mismatch: expected {}, got {}",
            self.coord_names.len(),
            coords.len(),
        );
        self.generation = self.generation.wrapping_add(1);
        self.coords = coords.to_vec();
    }

    /// Pull a named output variate for the current coordinate context.
    ///
    /// Triggers lazy evaluation of upstream nodes as needed.
    pub fn pull(&mut self, output_name: &str) -> &Value {
        let (node_idx, port_idx) = *self
            .output_map
            .get(output_name)
            .unwrap_or_else(|| panic!("unknown output variate: {output_name}"));
        self.eval_node(node_idx);
        &self.buffers[node_idx][port_idx]
    }

    /// Return the names of the input coordinates.
    pub fn coord_names(&self) -> &[String] {
        &self.coord_names
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_map.keys().map(|s| s.as_str()).collect()
    }

    fn eval_node(&mut self, node_idx: usize) {
        // Skip if already evaluated in this generation
        if self.node_generation[node_idx] == self.generation {
            return;
        }

        // Recursively evaluate upstream nodes and gather inputs
        let wiring = self.wiring[node_idx].clone();
        let mut inputs: Vec<Value> = Vec::with_capacity(wiring.len());
        for source in &wiring {
            let val = match source {
                WireSource::Coordinate(coord_idx) => Value::U64(self.coords[*coord_idx]),
                WireSource::NodeOutput(upstream_idx, port_idx) => {
                    self.eval_node(*upstream_idx);
                    self.buffers[*upstream_idx][*port_idx].clone()
                }
            };
            inputs.push(val);
        }

        // Evaluate this node
        self.nodes[node_idx].eval(&inputs, &mut self.buffers[node_idx]);
        self.node_generation[node_idx] = self.generation;
    }
}

