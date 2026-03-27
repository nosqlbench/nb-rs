// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Programmatic assembly API for building GK kernels.
//!
//! The assembler validates wiring and types, auto-inserts edge adapters,
//! topologically sorts nodes, and produces either a Phase 1 runtime
//! kernel or a Phase 2 compiled kernel.

use std::collections::HashMap;

use crate::compiled::CompiledKernel;
use crate::kernel::{GkKernel, WireSource};
use crate::node::{GkNode, PortType};
use crate::nodes::convert::{F64ToString, U64ToF64, U64ToString};
use crate::nodes::json::JsonToStr;

/// A reference to a value in the assembler: either a coordinate or a
/// node output port.
#[derive(Debug, Clone)]
pub enum WireRef {
    /// An input coordinate, by name.
    Coord(String),
    /// A node output: `(node_name, output_port_index)`.
    Node(String, usize),
}

impl WireRef {
    /// Convenience: reference the first (or only) output of a named node.
    pub fn node(name: impl Into<String>) -> Self {
        WireRef::Node(name.into(), 0)
    }

    /// Reference a specific output port of a named node.
    pub fn node_port(name: impl Into<String>, port: usize) -> Self {
        WireRef::Node(name.into(), port)
    }

    /// Reference a coordinate by name.
    pub fn coord(name: impl Into<String>) -> Self {
        WireRef::Coord(name.into())
    }
}

struct PendingNode {
    name: String,
    node: Box<dyn GkNode>,
    inputs: Vec<WireRef>,
}

/// Errors that can occur during assembly.
#[derive(Debug)]
pub enum AssemblyError {
    UnknownWire(String),
    TypeMismatch {
        from_node: String,
        from_port: usize,
        from_type: PortType,
        to_node: String,
        to_port: usize,
        to_type: PortType,
    },
    DuplicateNode(String),
    CycleDetected,
    ArityMismatch {
        node_name: String,
        expected: usize,
        got: usize,
    },
}

impl std::fmt::Display for AssemblyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssemblyError::UnknownWire(name) => write!(f, "unknown wire: {name}"),
            AssemblyError::TypeMismatch {
                from_node, from_port, from_type, to_node, to_port, to_type,
            } => write!(
                f, "type mismatch: {from_node}[{from_port}] ({from_type}) -> {to_node}[{to_port}] ({to_type})"
            ),
            AssemblyError::DuplicateNode(name) => write!(f, "duplicate node name: {name}"),
            AssemblyError::CycleDetected => write!(f, "cycle detected in DAG"),
            AssemblyError::ArityMismatch { node_name, expected, got } => {
                write!(f, "node {node_name}: expected {expected} inputs, got {got}")
            }
        }
    }
}

impl std::error::Error for AssemblyError {}

/// Validated, topologically sorted intermediate form.
struct ResolvedDag {
    /// Nodes in topological order.
    nodes: Vec<Box<dyn GkNode>>,
    /// Per-node wiring (in topological order).
    wiring: Vec<Vec<WireSource>>,
    /// Coordinate names.
    coord_names: Vec<String>,
    /// Output name → (node_index_in_sorted, output_port_index).
    output_map: HashMap<String, (usize, usize)>,
}

/// Builder for assembling a GK kernel programmatically.
pub struct GkAssembler {
    coord_names: Vec<String>,
    nodes: Vec<PendingNode>,
    outputs: HashMap<String, WireRef>,
}

impl GkAssembler {
    /// Create a new assembler with the given coordinate names.
    pub fn new(coord_names: Vec<String>) -> Self {
        Self {
            coord_names,
            nodes: Vec::new(),
            outputs: HashMap::new(),
        }
    }

    /// Add a node to the assembler with the given name and input wiring.
    pub fn add_node(
        &mut self,
        name: impl Into<String>,
        node: Box<dyn GkNode>,
        inputs: Vec<WireRef>,
    ) -> &mut Self {
        self.nodes.push(PendingNode {
            name: name.into(),
            node,
            inputs,
        });
        self
    }

    /// Designate a wire as a named output variate.
    pub fn add_output(&mut self, name: impl Into<String>, wire: WireRef) -> &mut Self {
        self.outputs.insert(name.into(), wire);
        self
    }

    /// Validate, resolve, and produce a Phase 1 runtime kernel.
    pub fn compile(self) -> Result<GkKernel, AssemblyError> {
        let resolved = self.resolve()?;
        Ok(GkKernel::new(
            resolved.nodes,
            resolved.wiring,
            resolved.coord_names,
            resolved.output_map,
        ))
    }

    /// Validate, resolve, and attempt Phase 2 compilation.
    ///
    /// Returns `Ok(CompiledKernel)` if all nodes are u64-only and provide
    /// `compiled_u64()`. Falls back to `Err(GkKernel)` (a working Phase 1
    /// kernel) if any node cannot be compiled.
    pub fn try_compile(self) -> Result<CompiledKernel, GkKernel> {
        // We need to resolve first, but resolve consumes self.
        // To avoid duplicating work, we resolve once and then try
        // to extract compiled ops from the resolved nodes.
        let resolved = self.resolve().expect("assembly validation failed");

        // Try to extract compiled_u64 from every node
        let mut compiled_ops = Vec::with_capacity(resolved.nodes.len());
        let mut all_compilable = true;
        for node in &resolved.nodes {
            if let Some(op) = node.compiled_u64() {
                compiled_ops.push(Some(op));
            } else {
                all_compilable = false;
                compiled_ops.push(None);
            }
        }

        if !all_compilable {
            // Fall back to Phase 1
            return Err(GkKernel::new(
                resolved.nodes,
                resolved.wiring,
                resolved.coord_names,
                resolved.output_map,
            ));
        }

        // All nodes are compilable — build the flat buffer layout.
        let coord_count = resolved.coord_names.len();

        // Assign buffer slots: coordinates first, then each node's
        // output ports in topological order.
        let mut slot_base: Vec<usize> = Vec::with_capacity(resolved.nodes.len());
        let mut next_slot = coord_count;
        for node in &resolved.nodes {
            slot_base.push(next_slot);
            next_slot += node.meta().outputs.len();
        }
        let total_slots = next_slot;

        // Build compiled steps
        let mut steps = Vec::with_capacity(resolved.nodes.len());
        for (node_idx, op) in compiled_ops.into_iter().enumerate() {
            let op = op.unwrap(); // safe: all_compilable checked above

            // Map wiring to buffer slot indices
            let input_slots: Vec<usize> = resolved.wiring[node_idx]
                .iter()
                .map(|source| match source {
                    WireSource::Coordinate(c) => *c,
                    WireSource::NodeOutput(upstream, port) => slot_base[*upstream] + port,
                })
                .collect();

            let output_count = resolved.nodes[node_idx].meta().outputs.len();
            let output_slots: Vec<usize> = (0..output_count)
                .map(|p| slot_base[node_idx] + p)
                .collect();

            steps.push((op, input_slots, output_slots));
        }

        // Remap output names to buffer slots
        let output_map: HashMap<String, usize> = resolved
            .output_map
            .iter()
            .map(|(name, (node_idx, port))| {
                (name.clone(), slot_base[*node_idx] + port)
            })
            .collect();

        Ok(CompiledKernel::new(coord_count, total_slots, steps, output_map))
    }

    /// Internal: validate, resolve wiring, insert adapters, topological sort.
    fn resolve(self) -> Result<ResolvedDag, AssemblyError> {
        // Build name → index map for nodes
        let mut name_to_idx: HashMap<String, usize> = HashMap::new();
        for (i, pn) in self.nodes.iter().enumerate() {
            if name_to_idx.contains_key(&pn.name) {
                return Err(AssemblyError::DuplicateNode(pn.name.clone()));
            }
            name_to_idx.insert(pn.name.clone(), i);
        }

        // Build coord name → index map
        let coord_to_idx: HashMap<String, usize> = self
            .coord_names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i))
            .collect();

        // Validate arity
        for pn in &self.nodes {
            let expected = pn.node.meta().inputs.len();
            let got = pn.inputs.len();
            if expected != got {
                return Err(AssemblyError::ArityMismatch {
                    node_name: pn.name.clone(),
                    expected,
                    got,
                });
            }
        }

        let mut all_nodes: Vec<PendingNode> = Vec::new();
        let mut all_name_to_idx: HashMap<String, usize> = HashMap::new();
        let mut adapter_count = 0usize;

        for pn in self.nodes {
            let idx = all_nodes.len();
            all_name_to_idx.insert(pn.name.clone(), idx);
            all_nodes.push(pn);
        }

        let mut resolved_wiring: Vec<Vec<WireSource>> = Vec::new();

        for node_idx in 0..all_nodes.len() {
            let mut node_wiring = Vec::new();

            for (port_idx, wire_ref) in all_nodes[node_idx].inputs.clone().iter().enumerate() {
                let expected_type = all_nodes[node_idx].node.meta().inputs[port_idx].typ;

                let (source, source_type) = match wire_ref {
                    WireRef::Coord(name) => {
                        let coord_idx = coord_to_idx
                            .get(name)
                            .ok_or_else(|| AssemblyError::UnknownWire(name.clone()))?;
                        (WireSource::Coordinate(*coord_idx), PortType::U64)
                    }
                    WireRef::Node(name, out_port) => {
                        let src_idx = all_name_to_idx
                            .get(name)
                            .ok_or_else(|| AssemblyError::UnknownWire(name.clone()))?;
                        let src_type = all_nodes[*src_idx].node.meta().outputs[*out_port].typ;
                        (WireSource::NodeOutput(*src_idx, *out_port), src_type)
                    }
                };

                if source_type == expected_type {
                    node_wiring.push(source);
                } else if let Some(adapter) = auto_adapter(source_type, expected_type) {
                    let adapter_name = format!("__adapt_{adapter_count}");
                    adapter_count += 1;
                    let adapter_idx = all_nodes.len();
                    all_name_to_idx.insert(adapter_name.clone(), adapter_idx);

                    let adapter_wiring = vec![source];
                    resolved_wiring.push(adapter_wiring);

                    all_nodes.push(PendingNode {
                        name: adapter_name,
                        node: adapter,
                        inputs: vec![],
                    });

                    node_wiring.push(WireSource::NodeOutput(adapter_idx, 0));
                } else {
                    let from_name = match wire_ref {
                        WireRef::Coord(n) => n.clone(),
                        WireRef::Node(n, _) => n.clone(),
                    };
                    return Err(AssemblyError::TypeMismatch {
                        from_node: from_name,
                        from_port: match wire_ref {
                            WireRef::Coord(_) => 0,
                            WireRef::Node(_, p) => *p,
                        },
                        from_type: source_type,
                        to_node: all_nodes[node_idx].name.clone(),
                        to_port: port_idx,
                        to_type: expected_type,
                    });
                }
            }

            while resolved_wiring.len() <= node_idx {
                resolved_wiring.push(Vec::new());
            }
            resolved_wiring[node_idx] = node_wiring;
        }

        while resolved_wiring.len() < all_nodes.len() {
            resolved_wiring.push(Vec::new());
        }

        // Topological sort (Kahn's algorithm)
        let node_count = all_nodes.len();
        let mut in_degree = vec![0usize; node_count];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); node_count];

        for (node_idx, wiring) in resolved_wiring.iter().enumerate() {
            for source in wiring {
                if let WireSource::NodeOutput(upstream, _) = source {
                    in_degree[node_idx] += 1;
                    dependents[*upstream].push(node_idx);
                }
            }
        }

        let mut queue: Vec<usize> = (0..node_count)
            .filter(|i| in_degree[*i] == 0)
            .collect();
        let mut sorted_order: Vec<usize> = Vec::with_capacity(node_count);

        while let Some(idx) = queue.pop() {
            sorted_order.push(idx);
            for &dep in &dependents[idx] {
                in_degree[dep] -= 1;
                if in_degree[dep] == 0 {
                    queue.push(dep);
                }
            }
        }

        if sorted_order.len() != node_count {
            return Err(AssemblyError::CycleDetected);
        }

        let mut old_to_new = vec![0usize; node_count];
        for (new_idx, &old_idx) in sorted_order.iter().enumerate() {
            old_to_new[old_idx] = new_idx;
        }

        let mut sorted_nodes: Vec<Option<Box<dyn GkNode>>> = all_nodes
            .into_iter()
            .map(|pn| Some(pn.node))
            .collect();

        let final_nodes: Vec<Box<dyn GkNode>> = sorted_order
            .iter()
            .map(|&old_idx| sorted_nodes[old_idx].take().unwrap())
            .collect();

        let final_wiring: Vec<Vec<WireSource>> = sorted_order
            .iter()
            .map(|&old_idx| {
                resolved_wiring[old_idx]
                    .iter()
                    .map(|source| match source {
                        WireSource::Coordinate(c) => WireSource::Coordinate(*c),
                        WireSource::NodeOutput(old_up, port) => {
                            WireSource::NodeOutput(old_to_new[*old_up], *port)
                        }
                    })
                    .collect()
            })
            .collect();

        let mut final_output_map: HashMap<String, (usize, usize)> = HashMap::new();
        for (name, wire_ref) in &self.outputs {
            match wire_ref {
                WireRef::Coord(coord_name) => {
                    return Err(AssemblyError::UnknownWire(format!(
                        "output '{name}' references coordinate '{coord_name}' directly; \
                         wire through a node instead"
                    )));
                }
                WireRef::Node(node_name, port) => {
                    let old_idx = all_name_to_idx
                        .get(node_name)
                        .ok_or_else(|| AssemblyError::UnknownWire(node_name.clone()))?;
                    final_output_map.insert(name.clone(), (old_to_new[*old_idx], *port));
                }
            }
        }

        Ok(ResolvedDag {
            nodes: final_nodes,
            wiring: final_wiring,
            coord_names: self.coord_names,
            output_map: final_output_map,
        })
    }
}

/// Return an auto-insert edge adapter for common coercions, if one exists.
fn auto_adapter(from: PortType, to: PortType) -> Option<Box<dyn GkNode>> {
    match (from, to) {
        (PortType::U64, PortType::Str) => Some(Box::new(U64ToString::new())),
        (PortType::F64, PortType::Str) => Some(Box::new(F64ToString::new())),
        (PortType::U64, PortType::F64) => Some(Box::new(U64ToF64::new())),
        (PortType::Json, PortType::Str) => Some(Box::new(JsonToStr::new())),
        _ => None,
    }
}
