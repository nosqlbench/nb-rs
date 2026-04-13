// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Programmatic assembly API for building GK kernels.
//!
//! The assembler validates wiring and types, auto-inserts edge adapters,
//! topologically sorts nodes, and produces either a Phase 1 runtime
//! kernel or a Phase 2 compiled kernel.

use std::collections::HashMap;

use crate::compiled::{CompiledKernelRaw, CompiledKernelPush, CompiledKernelPull, CompiledKernelPushPull};
use crate::engine::{self, GraphAnalysis, ProvMode, P2Engine};
use crate::kernel::{GkKernel, GkProgram, WireSource};
use crate::node::{GkNode, PortType};
use crate::nodes::convert::{F64ToString, U64ToF64, U64ToString};
use crate::nodes::json::JsonToStr;

/// A reference to a value in the assembler: either a coordinate or a
/// node output port.
#[derive(Debug, Clone)]
pub enum WireRef {
    /// A graph input, by name.
    Input(String),
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

    /// Reference a graph input by name.
    pub fn input(name: impl Into<String>) -> Self {
        WireRef::Input(name.into())
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
    /// Catch-all for errors from downstream phases (e.g., strict mode).
    Other(String),
}

impl std::fmt::Display for AssemblyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssemblyError::UnknownWire(name) => {
                write!(f, "unknown wire: '{name}'\n\n")?;
                writeln!(f, "  No node output or coordinate named '{name}' exists.")?;
                write!(f, "  Check spelling, or add a node that produces this output.")
            }
            AssemblyError::TypeMismatch {
                from_node, from_port, from_type, to_node, to_port, to_type,
            } => {
                writeln!(f, "type mismatch: cannot connect {from_type} output to {to_type} input")?;
                writeln!(f)?;
                writeln!(f, "  {from_node} [{from_port}]  ──({from_type})──▶  {to_node} [{to_port}] expects {to_type}")?;
                writeln!(f)?;
                // Suggest auto-adapters that exist
                let suggestion = match (from_type, to_type) {
                    (PortType::U64, PortType::Str) => Some("This should auto-convert. If you see this, file a bug."),
                    (PortType::F64, PortType::Str) => Some("This should auto-convert. If you see this, file a bug."),
                    (PortType::U64, PortType::F64) => Some("This should auto-convert. If you see this, file a bug."),
                    (PortType::U64, PortType::Bytes) => Some("Add u64_to_bytes() between them to convert."),
                    (PortType::Str, PortType::Bytes) => Some("String cannot be directly used as bytes."),
                    (PortType::U64, PortType::Json) => Some("Add to_json() between them to wrap as JSON."),
                    (PortType::Str, PortType::Json) => Some("Add str_to_json() to parse the string as JSON."),
                    (PortType::Bytes, PortType::Str) => Some("Add to_hex() or to_base64() to convert bytes to string."),
                    (PortType::Bytes, PortType::U64) => Some("Bytes cannot be directly converted to u64."),
                    _ => None,
                };
                if let Some(hint) = suggestion {
                    write!(f, "  Hint: {hint}")?;
                }
                Ok(())
            }
            AssemblyError::DuplicateNode(name) => {
                write!(f, "duplicate node name: '{name}'\n\n")?;
                write!(f, "  Two nodes cannot share the same name.")
            }
            AssemblyError::CycleDetected => {
                write!(f, "cycle detected in DAG\n\n")?;
                writeln!(f, "  The graph contains a loop. GK graphs must be acyclic")?;
                write!(f, "  (data flows in one direction only).")
            }
            AssemblyError::ArityMismatch { node_name, expected, got } => {
                write!(f, "wrong number of inputs for '{node_name}'\n\n")?;
                writeln!(f, "  Expected {expected} input(s), but got {got}.")?;
                if *got < *expected {
                    write!(f, "  Connect more wires to this node's input ports.")
                } else {
                    write!(f, "  Disconnect extra wires from this node.")
                }
            }
            AssemblyError::Other(msg) => write!(f, "{msg}"),
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
    /// All input definitions (coordinates + captures).
    input_defs: Vec<crate::kernel::InputDef>,
    /// Number of coordinate inputs.
    coord_count: usize,
    /// Output name → (node_index_in_sorted, output_port_index).
    output_map: HashMap<String, (usize, usize)>,
    /// Output names in declaration order.
    output_order: Vec<String>,
    /// Source text for diagnostics.
    source: String,
    /// Diagnostic context.
    context: String,
    /// Output binding modifiers.
    output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
}

impl ResolvedDag {
    /// Coordinate input names (for P2/P3 kernels that use positional u64 buffers).
    fn input_names(&self) -> Vec<String> {
        self.input_defs[..self.coord_count].iter()
            .map(|d| d.name.clone()).collect()
    }
}

/// Builder for assembling a GK kernel programmatically.
pub struct GkAssembler {
    /// All input definitions. Coordinates come first (indices 0..coord_count).
    input_defs: Vec<crate::kernel::InputDef>,
    /// How many of the inputs are coordinates.
    coord_count: usize,
    nodes: Vec<PendingNode>,
    /// Output declarations in insertion order.
    output_order: Vec<String>,
    outputs: HashMap<String, WireRef>,
    /// Original source text for diagnostics. Set by the DSL compiler.
    source: String,
    /// Diagnostic context (e.g., "workload.yaml bindings").
    context: String,
    /// Binding modifiers for named outputs.
    output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
}

impl GkAssembler {
    /// Create a new assembler with the given coordinate names.
    pub fn new(input_names: Vec<String>) -> Self {
        let coord_count = input_names.len();
        let input_defs: Vec<crate::kernel::InputDef> = input_names.into_iter()
            .map(|name| crate::kernel::InputDef { name, default: crate::node::Value::U64(0), port_type: crate::node::PortType::U64 })
            .collect();
        Self {
            input_defs,
            coord_count,
            nodes: Vec::new(),
            output_order: Vec::new(),
            outputs: HashMap::new(),
            source: String::new(),
            context: "(assembler)".into(),
            output_modifiers: HashMap::new(),
        }
    }

    /// Set the source text and diagnostic context for this assembler.
    /// Called by the DSL compiler to attach the original GK source.
    pub fn set_context(&mut self, source: &str, context: &str) {
        self.source = source.to_string();
        self.context = context.to_string();
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

    /// Set the binding modifier for a named output.
    pub fn set_output_modifier(&mut self, name: &str, modifier: crate::dsl::ast::BindingModifier) {
        if modifier != crate::dsl::ast::BindingModifier::None {
            self.output_modifiers.insert(name.to_string(), modifier);
        }
    }

    /// Designate a wire as a named output variate.
    pub fn add_output(&mut self, name: impl Into<String>, wire: WireRef) -> &mut Self {
        let name = name.into();
        if !self.outputs.contains_key(&name) {
            self.output_order.push(name.clone());
        }
        self.outputs.insert(name, wire);
        self
    }

    /// Declare an additional named input (e.g., a capture slot).
    ///
    /// Added after coordinate inputs. Nodes wire to it via
    /// `WireRef::input(name)` — same as coordinate inputs.
    pub fn add_input(&mut self, name: impl Into<String>, default: crate::node::Value, port_type: crate::node::PortType) -> &mut Self {
        self.input_defs.push(crate::kernel::InputDef {
            name: name.into(),
            default,
            port_type,
        });
        self
    }

    /// Return the names of all inputs (coordinates + captures).
    pub fn input_names(&self) -> Vec<&str> {
        self.input_defs.iter().map(|d| d.name.as_str()).collect()
    }

    /// Query the output port type of a named node (first output).
    /// Returns U64 if the node is not found.
    pub fn node_output_type(&self, name: &str) -> crate::node::PortType {
        self.nodes.iter()
            .find(|n| n.name == name)
            .and_then(|n| n.node.meta().outs.first())
            .map(|p| p.typ)
            .unwrap_or(crate::node::PortType::U64)
    }

    /// Return the names of declared outputs.
    pub fn output_names(&self) -> Vec<&str> {
        self.outputs.keys().map(|s| s.as_str()).collect()
    }

    /// Look up the output port type of a named node.
    ///
    /// Returns the first output port's `PortType` if the node exists.
    pub fn output_type(&self, name: &str) -> Option<PortType> {
        self.nodes.iter()
            .find(|pn| pn.name == name)
            .and_then(|pn| pn.node.meta().outs.first())
            .map(|port| port.typ)
    }

    /// Validate, resolve, and produce a Phase 1 runtime kernel.
    pub fn compile(self) -> Result<GkKernel, AssemblyError> {
        self.compile_with_log(None)
    }

    /// Compile with diagnostic event logging.
    pub fn compile_with_log(self, mut log: Option<&mut crate::dsl::events::CompileEventLog>) -> Result<GkKernel, AssemblyError> {
        let resolved = self.resolve_with_log(log.as_deref_mut())?;
        let _coord_names = resolved.input_names();
        let modifiers = resolved.output_modifiers.clone();
        let mut kernel = GkKernel::new_with_inputs(
            resolved.nodes,
            resolved.wiring,
            resolved.input_defs,
            resolved.coord_count,
            resolved.output_map,
            resolved.output_order,
            &resolved.source,
            &resolved.context,
            log,
        );
        if !modifiers.is_empty() {
            kernel.set_output_modifiers(&modifiers);
        }
        Ok(kernel)
    }

    /// Compile with strict mode: config wire violations are errors,
    /// implicit type coercions are rejected, unused bindings flagged.
    pub fn compile_strict(self, strict: bool) -> Result<GkKernel, AssemblyError> {
        if !strict {
            return self.compile();
        }
        let resolved = self.resolve()?;
        let _coord_names = resolved.input_names();

        // Strict: reject implicit type coercions (auto-inserted adapter nodes)
        // Adapters have names starting with "__" and containing type conversion hints
        let adapter_count = resolved.nodes.iter()
            .filter(|n| {
                let name = &n.meta().name;
                name.starts_with("__adapt_") || name.starts_with("__u64_to_") || name.starts_with("__f64_to_")
                    || name.starts_with("__bool_to_") || name.starts_with("__str_to_")
            })
            .count();
        if adapter_count > 0 {
            return Err(AssemblyError::Other(format!(
                "strict mode: {adapter_count} implicit type coercion(s) inserted. \
                 Use explicit conversion functions (e.g., u64_to_f64, f64_to_u64)."
            )));
        }

        let modifiers = resolved.output_modifiers.clone();
        let mut kernel = GkKernel::new_strict_with_inputs(
            resolved.nodes,
            resolved.wiring,
            resolved.input_defs,
            resolved.coord_count,
            resolved.output_map,
            resolved.output_order,
            &resolved.source,
            &resolved.context,
            None,
        ).map_err(AssemblyError::Other)?;
        if !modifiers.is_empty() {
            kernel.set_output_modifiers(&modifiers);
        }
        Ok(kernel)
    }

    /// Validate, resolve, and attempt Phase 2 compilation.
    ///
    /// Returns `Ok(CompiledKernelPushPull)` if all nodes are u64-only and provide
    /// `compiled_u64()`. Falls back to `Err(GkKernel)` (a working Phase 1
    /// kernel) if any node cannot be compiled.
    pub fn try_compile(self) -> Result<CompiledKernelPushPull, GkKernel> {
        let resolved = self.resolve().expect("assembly validation failed");
        let _coord_names = resolved.input_names();
        let coord_names = resolved.input_names();
        let coord_count = coord_names.len();

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
                coord_names,
                resolved.output_map,
                &resolved.source,
                &resolved.context,
            ));
        }

        // Assign buffer slots: coordinates first, then each node's
        // output ports in topological order.
        let mut slot_base: Vec<usize> = Vec::with_capacity(resolved.nodes.len());
        let mut next_slot = coord_count;
        for node in &resolved.nodes {
            slot_base.push(next_slot);
            next_slot += node.meta().outs.len();
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
                    WireSource::Input(c) => *c,
                    WireSource::NodeOutput(upstream, port) => slot_base[*upstream] + port,
                })
                .collect();

            let output_count = resolved.nodes[node_idx].meta().outs.len();
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

        Ok(CompiledKernelPushPull::new(
            coord_count, total_slots, steps, output_map,
            GkProgram::compute_dependents(
                &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                coord_count,
            ),
        ))
    }

    /// Phase 2 compilation without provenance caching.
    pub fn try_compile_raw(self) -> Result<CompiledKernelRaw, GkKernel> {
        let resolved = match self.resolve() {
            Ok(r) => r,
            Err(_) => return Err(GkKernel::new(vec![], vec![], vec![], HashMap::new(), "", "(fallback)")),
        };
        let coord_names = resolved.input_names();
        let coord_count = coord_names.len();
        let mut slot_base: Vec<usize> = Vec::with_capacity(resolved.nodes.len());
        let mut next_slot = coord_count;
        for node in &resolved.nodes {
            slot_base.push(next_slot);
            next_slot += node.meta().outs.len();
        }
        let total_slots = next_slot;
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
            return Err(GkKernel::new(
                resolved.nodes, resolved.wiring, coord_names.clone(), resolved.output_map,
                &resolved.source, &resolved.context,
            ));
        }
        let mut steps = Vec::with_capacity(resolved.nodes.len());
        for (node_idx, op) in compiled_ops.into_iter().enumerate() {
            let op = op.unwrap();
            let input_slots: Vec<usize> = resolved.wiring[node_idx].iter()
                .map(|source| match source {
                    WireSource::Input(c) => *c,
                    WireSource::NodeOutput(upstream, port) => slot_base[*upstream] + port,
                }).collect();
            let output_count = resolved.nodes[node_idx].meta().outs.len();
            let output_slots: Vec<usize> = (0..output_count).map(|p| slot_base[node_idx] + p).collect();
            steps.push((op, input_slots, output_slots));
        }
        let output_map = resolved.output_map.iter()
            .map(|(name, (node_idx, port))| (name.clone(), slot_base[*node_idx] + port))
            .collect();
        Ok(CompiledKernelRaw::new(coord_count, total_slots, steps, output_map))
    }

    /// Phase 2 compilation with push-side provenance only (no cone guard).
    pub fn try_compile_push(self) -> Result<CompiledKernelPush, GkKernel> {
        let resolved = match self.resolve() {
            Ok(r) => r,
            Err(_) => return Err(GkKernel::new(vec![], vec![], vec![], HashMap::new(), "", "(fallback)")),
        };
        let coord_names = resolved.input_names();
        let (coord_count, total_slots, steps, output_map) =
            match Self::build_p2_layout(&resolved) {
                Some(r) => r,
                None => return Err(GkKernel::new(
                    resolved.nodes, resolved.wiring, coord_names, resolved.output_map,
                    &resolved.source, &resolved.context)),
            };
        let dependents = GkProgram::compute_dependents(
            &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
            coord_count,
        );
        Ok(CompiledKernelPush::new(coord_count, total_slots, steps, output_map, dependents))
    }

    /// Phase 2 compilation with pull-side cone guard only (no per-node skip).
    pub fn try_compile_pull(self) -> Result<CompiledKernelPull, GkKernel> {
        let resolved = match self.resolve() {
            Ok(r) => r,
            Err(_) => return Err(GkKernel::new(vec![], vec![], vec![], HashMap::new(), "", "(fallback)")),
        };
        let coord_names = resolved.input_names();
        let (coord_count, total_slots, steps, output_map) =
            match Self::build_p2_layout(&resolved) {
                Some(r) => r,
                None => return Err(GkKernel::new(
                    resolved.nodes, resolved.wiring, coord_names, resolved.output_map,
                    &resolved.source, &resolved.context)),
            };
        let dependents = GkProgram::compute_dependents(
            &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
            coord_count,
        );
        Ok(CompiledKernelPull::new(coord_count, total_slots, steps, output_map, &dependents))
    }

    /// Shared: extract P2 compiled steps + slot layout from resolved DAG.
    /// Returns None if any node lacks a compiled_u64 implementation.
    fn build_p2_layout(
        resolved: &ResolvedDag,
    ) -> Option<(usize, usize, Vec<(crate::node::CompiledU64Op, Vec<usize>, Vec<usize>)>, HashMap<String, usize>)> {
        let coord_count = resolved.coord_count;
        let mut slot_base: Vec<usize> = Vec::with_capacity(resolved.nodes.len());
        let mut next_slot = coord_count;
        for node in &resolved.nodes {
            slot_base.push(next_slot);
            next_slot += node.meta().outs.len();
        }
        let total_slots = next_slot;

        let mut compiled_ops = Vec::with_capacity(resolved.nodes.len());
        for node in &resolved.nodes {
            compiled_ops.push(node.compiled_u64()?);
        }

        let mut steps = Vec::with_capacity(resolved.nodes.len());
        for (node_idx, op) in compiled_ops.into_iter().enumerate() {
            let input_slots: Vec<usize> = resolved.wiring[node_idx].iter()
                .map(|source| match source {
                    WireSource::Input(c) => *c,
                    WireSource::NodeOutput(upstream, port) => slot_base[*upstream] + port,
                }).collect();
            let output_count = resolved.nodes[node_idx].meta().outs.len();
            let output_slots: Vec<usize> = (0..output_count).map(|p| slot_base[node_idx] + p).collect();
            steps.push((op, input_slots, output_slots));
        }
        let output_map = resolved.output_map.iter()
            .map(|(name, (node_idx, port))| (name.clone(), slot_base[*node_idx] + port))
            .collect();

        Some((coord_count, total_slots, steps, output_map))
    }

    /// Shared: resolve nodes to JIT steps + slot layout.
    #[cfg(feature = "jit")]
    fn build_jit_layout(resolved: &ResolvedDag)
        -> Result<(usize, usize, Vec<(crate::jit::JitOp, Vec<usize>, Vec<usize>)>, HashMap<String, usize>), String>
    {
        let coord_count = resolved.coord_count;
        let mut slot_base: Vec<usize> = Vec::with_capacity(resolved.nodes.len());
        let mut next_slot = coord_count;
        for node in &resolved.nodes { slot_base.push(next_slot); next_slot += node.meta().outs.len(); }
        let total_slots = next_slot;

        let mut jit_steps = Vec::new();
        for (node_idx, node) in resolved.nodes.iter().enumerate() {
            let jit_op = crate::jit::classify_node(node.as_ref());
            let input_slots: Vec<usize> = resolved.wiring[node_idx].iter()
                .map(|s| match s {
                    WireSource::Input(c) => *c,
                    WireSource::NodeOutput(u, p) => slot_base[*u] + p,
                }).collect();
            let output_slots: Vec<usize> = (0..node.meta().outs.len())
                .map(|p| slot_base[node_idx] + p).collect();
            jit_steps.push((jit_op, input_slots, output_slots));
        }

        if jit_steps.iter().any(|(op, _, _)| matches!(op, crate::jit::JitOp::Fallback)) {
            return Err("some nodes cannot be JIT-compiled".into());
        }

        let output_map = resolved.output_map.iter()
            .map(|(name, (ni, p))| (name.clone(), slot_base[*ni] + p)).collect();
        Ok((coord_count, total_slots, jit_steps, output_map))
    }

    /// Phase 3 JIT: push+pull (full provenance).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit(self) -> Result<crate::jit::JitKernelPushPull, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        let deps = GkProgram::compute_dependents(
            &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring), coord_count);
        crate::jit::compile_jit_push_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, deps)
    }

    /// Phase 3 JIT: raw (no provenance).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit_raw(self) -> Result<crate::jit::JitKernelRaw, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        crate::jit::compile_jit_raw(coord_count, total_slots, jit_steps, output_map, resolved.nodes)
    }

    /// Phase 3 JIT: push-only (per-node dirty tracking, no cone guard).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit_push(self) -> Result<crate::jit::JitKernelPush, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        let deps = GkProgram::compute_dependents(
            &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring), coord_count);
        crate::jit::compile_jit_push(coord_count, total_slots, jit_steps, output_map, resolved.nodes, deps)
    }

    /// Phase 3 JIT: pull-only (cone guard, no per-node dirty tracking).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit_pull(self) -> Result<crate::jit::JitKernelPull, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        let deps = GkProgram::compute_dependents(
            &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring), coord_count);
        crate::jit::compile_jit_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, &deps)
    }

    /// Analyze the graph and auto-select the optimal P2 provenance mode.
    ///
    /// Returns a `P2Engine` enum wrapping the monomorphic kernel variant.
    /// The selection is based on graph structure (cone sizes, input count).
    pub fn auto_compile_p2(self) -> Result<(P2Engine, GraphAnalysis), String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let analysis = engine::analyze_graph(&resolved.nodes, &resolved.wiring, &resolved.output_map);
        let mode = engine::select_prov_mode(&analysis);

        let (coord_count, total_slots, steps, output_map) =
            match Self::build_p2_layout(&resolved) {
                Some(r) => r,
                None => return Err("not all nodes support P2 compilation".into()),
            };

        let engine = match mode {
            ProvMode::Raw => {
                P2Engine::Raw(CompiledKernelRaw::new(coord_count, total_slots, steps, output_map))
            }
            ProvMode::Pull => {
                let deps = GkProgram::compute_dependents(
                    &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring), coord_count);
                P2Engine::Pull(CompiledKernelPull::new(coord_count, total_slots, steps, output_map, &deps))
            }
            ProvMode::PushPull => {
                let deps = GkProgram::compute_dependents(
                    &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring), coord_count);
                P2Engine::PushPull(CompiledKernelPushPull::new(coord_count, total_slots, steps, output_map, deps))
            }
        };
        Ok((engine, analysis))
    }

    /// Analyze the graph and auto-select the optimal P3 JIT provenance mode.
    #[cfg(feature = "jit")]
    pub fn auto_compile_p3(self) -> Result<(engine::P3Engine, GraphAnalysis), String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let analysis = engine::analyze_graph(&resolved.nodes, &resolved.wiring, &resolved.output_map);
        let mode = engine::select_prov_mode(&analysis);

        let (coord_count, total_slots, jit_steps, output_map) =
            Self::build_jit_layout(&resolved)?;

        let engine = match mode {
            ProvMode::Raw => {
                let k = crate::jit::compile_jit_raw(coord_count, total_slots, jit_steps, output_map, resolved.nodes)?;
                engine::P3Engine::Raw(k)
            }
            ProvMode::Pull => {
                let deps = GkProgram::compute_dependents(
                    &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring), coord_count);
                let k = crate::jit::compile_jit_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, &deps)?;
                engine::P3Engine::Pull(k)
            }
            ProvMode::PushPull => {
                let deps = GkProgram::compute_dependents(
                    &GkProgram::compute_provenance(&resolved.nodes, &resolved.wiring), coord_count);
                let k = crate::jit::compile_jit_push_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, deps)?;
                engine::P3Engine::PushPull(k)
            }
        };
        Ok((engine, analysis))
    }

    /// Validate, resolve, and compile a hybrid kernel where each node
    /// runs at its optimal level (JIT native code or Phase 2 closure).
    ///
    /// This always succeeds for u64-only DAGs — no all-or-nothing
    /// fallback. JIT-able nodes get native code, others get closures.
    pub fn compile_hybrid(self) -> Result<crate::hybrid::HybridKernel, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let coord_count = resolved.coord_count;

        let mut slot_bases: Vec<usize> = Vec::with_capacity(resolved.nodes.len());
        let mut next_slot = coord_count;
        for node in &resolved.nodes {
            slot_bases.push(next_slot);
            next_slot += node.meta().outs.len();
        }
        let total_slots = next_slot;

        let output_map: HashMap<String, usize> = resolved
            .output_map
            .iter()
            .map(|(name, (node_idx, port))| {
                (name.clone(), slot_bases[*node_idx] + port)
            })
            .collect();

        let mut kernel = crate::hybrid::build_hybrid(
            &resolved.nodes,
            &resolved.wiring,
            coord_count,
            total_slots,
            &slot_bases,
            output_map,
        )?;
        kernel.retain_nodes(resolved.nodes);
        Ok(kernel)
    }

    /// Internal: validate, resolve wiring, insert adapters, topological sort.
    fn resolve(self) -> Result<ResolvedDag, AssemblyError> {
        self.resolve_with_log(None)
    }

    fn resolve_with_log(self, mut log: Option<&mut crate::dsl::events::CompileEventLog>) -> Result<ResolvedDag, AssemblyError> {
        // Build name → index map for nodes
        let mut name_to_idx: HashMap<String, usize> = HashMap::new();
        for (i, pn) in self.nodes.iter().enumerate() {
            if name_to_idx.contains_key(&pn.name) {
                return Err(AssemblyError::DuplicateNode(pn.name.clone()));
            }
            name_to_idx.insert(pn.name.clone(), i);
        }

        // Build input name → index map (covers both coords and captures)
        let input_to_idx: HashMap<String, usize> = self
            .input_defs
            .iter()
            .enumerate()
            .map(|(i, d)| (d.name.clone(), i))
            .collect();

        // Validate arity
        for pn in &self.nodes {
            let expected = pn.node.meta().wire_inputs().len();
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
                let expected_type = all_nodes[node_idx].node.meta().wire_inputs()[port_idx].typ;

                let (source, source_type) = match wire_ref {
                    WireRef::Input(name) => {
                        let input_idx = input_to_idx
                            .get(name)
                            .ok_or_else(|| AssemblyError::UnknownWire(name.clone()))?;
                        let source_type = self.input_defs[*input_idx].port_type;
                        (WireSource::Input(*input_idx), source_type)
                    }
                    WireRef::Node(name, out_port) => {
                        let src_idx = all_name_to_idx
                            .get(name)
                            .ok_or_else(|| AssemblyError::UnknownWire(name.clone()))?;
                        let src_type = all_nodes[*src_idx].node.meta().outs[*out_port].typ;
                        (WireSource::NodeOutput(*src_idx, *out_port), src_type)
                    }
                };

                // Printf accepts any input type — skip type checking for it
                let skip_type_check = all_nodes[node_idx].node.meta().name == "printf";

                if skip_type_check || source_type == expected_type {
                    node_wiring.push(source);
                } else if let Some(adapter) = auto_adapter(source_type, expected_type) {
                    let adapter_name = format!("__adapt_{adapter_count}");
                    adapter_count += 1;
                    let adapter_idx = all_nodes.len();

                    if let Some(ref mut log) = log {
                        let from_name = match wire_ref {
                            WireRef::Input(n) => n.clone(),
                            WireRef::Node(n, _) => n.clone(),
                        };
                        log.push(crate::dsl::events::CompileEvent::TypeAdapterInserted {
                            from_node: from_name,
                            to_node: all_nodes[node_idx].name.clone(),
                            adapter: format!("{source_type:?}→{expected_type:?}"),
                        });
                    }

                    all_name_to_idx.insert(adapter_name.clone(), adapter_idx);

                    let adapter_wiring = vec![source];
                    while resolved_wiring.len() <= adapter_idx {
                        resolved_wiring.push(Vec::new());
                    }
                    resolved_wiring[adapter_idx] = adapter_wiring;

                    all_nodes.push(PendingNode {
                        name: adapter_name,
                        node: adapter,
                        inputs: vec![],
                    });

                    node_wiring.push(WireSource::NodeOutput(adapter_idx, 0));
                } else {
                    let from_name = match wire_ref {
                        WireRef::Input(n) => n.clone(),
                        WireRef::Node(n, _) => n.clone(),
                    };
                    return Err(AssemblyError::TypeMismatch {
                        from_node: from_name,
                        from_port: match wire_ref {
                            WireRef::Input(_) => 0,
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

        // --- Node fusion optimization ---
        //
        // Recognize fusible subgraph patterns and replace them with
        // semantically equivalent fused nodes. See SRD 36.
        {
            let rules = crate::fusion::default_rules();
            if !rules.is_empty() {
                // Collect node indices that are directly referenced by outputs.
                // These nodes must not be consumed as interior nodes by fusion.
                let mut output_nodes: Vec<usize> = Vec::new();
                for wire_ref in self.outputs.values() {
                    if let WireRef::Node(node_name, _) = wire_ref
                        && let Some(&idx) = all_name_to_idx.get(node_name) {
                            output_nodes.push(idx);
                        }
                }

                // Convert to Option<Box<dyn GkNode>> for the fusion pass.
                let mut opt_nodes: Vec<Option<Box<dyn GkNode>>> = all_nodes
                    .into_iter()
                    .map(|pn| Some(pn.node))
                    .collect();

                let fused_count = crate::fusion::apply_fusions(
                    &mut opt_nodes,
                    &mut resolved_wiring,
                    &mut all_name_to_idx,
                    &rules,
                    &output_nodes,
                );
                if fused_count > 0
                    && let Some(ref mut log) = log {
                        log.push(crate::dsl::events::CompileEvent::FusionApplied {
                            pattern: "subgraph".into(),
                            nodes_replaced: fused_count,
                        });
                    }

                // Convert back, rebuilding PendingNode wrappers.
                // Fused-away nodes (None) get placeholder names.
                all_nodes = opt_nodes
                    .into_iter()
                    .enumerate()
                    .map(|(i, opt)| PendingNode {
                        name: all_name_to_idx
                            .iter()
                            .find(|&(_, &idx)| idx == i)
                            .map(|(n, _)| n.clone())
                            .unwrap_or_else(|| format!("__removed_{i}")),
                        node: opt.unwrap_or_else(|| Box::new(crate::nodes::identity::Identity::new())),
                        inputs: vec![], // wiring is in resolved_wiring
                    })
                    .collect();
            }
        }

        // --- Dead code elimination ---
        //
        // Trace backward from output nodes to find all reachable nodes.
        // Only reachable nodes participate in the topological sort and
        // end up in the final kernel. This prunes unused binding chains
        // when the caller requests a subset of outputs.
        let node_count = all_nodes.len();
        let mut reachable = vec![false; node_count];
        {
            let mut worklist: Vec<usize> = Vec::new();
            // Seed with output nodes
            for wire_ref in self.outputs.values() {
                if let WireRef::Node(node_name, _) = wire_ref
                    && let Some(&idx) = all_name_to_idx.get(node_name) {
                        worklist.push(idx);
                    }
            }
            // Walk backward through wiring
            while let Some(idx) = worklist.pop() {
                if reachable[idx] { continue; }
                reachable[idx] = true;
                for source in &resolved_wiring[idx] {
                    if let WireSource::NodeOutput(upstream, _) = source
                        && !reachable[*upstream] {
                            worklist.push(*upstream);
                        }
                }
            }
        }
        let live_count = reachable.iter().filter(|&&r| r).count();

        // Topological sort (Kahn's algorithm) over reachable nodes only
        let mut in_degree = vec![0usize; node_count];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); node_count];

        for (node_idx, wiring) in resolved_wiring.iter().enumerate() {
            if !reachable[node_idx] { continue; }
            for source in wiring {
                if let WireSource::NodeOutput(upstream, _) = source {
                    in_degree[node_idx] += 1;
                    dependents[*upstream].push(node_idx);
                }
            }
        }

        let mut queue: Vec<usize> = (0..node_count)
            .filter(|i| reachable[*i] && in_degree[*i] == 0)
            .collect();
        let mut sorted_order: Vec<usize> = Vec::with_capacity(live_count);

        while let Some(idx) = queue.pop() {
            sorted_order.push(idx);
            for &dep in &dependents[idx] {
                in_degree[dep] -= 1;
                if in_degree[dep] == 0 {
                    queue.push(dep);
                }
            }
        }

        if sorted_order.len() != live_count {
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
                        WireSource::Input(c) => WireSource::Input(*c),
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
                WireRef::Input(coord_name) => {
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
            input_defs: self.input_defs,
            coord_count: self.coord_count,
            output_map: final_output_map,
            output_order: self.output_order,
            source: self.source,
            context: self.context,
            output_modifiers: self.output_modifiers,
        })
    }
}

/// Return an auto-insert edge adapter for common coercions, if one exists.
fn auto_adapter(from: PortType, to: PortType) -> Option<Box<dyn GkNode>> {
    use crate::nodes::convert::{
        BoolToStr, BoolToU64,
        U32ToU64, U32ToF64, U32ToString,
        I32ToI64, I32ToF64, I32ToString,
        I64ToF64, I64ToString,
        F32ToF64, F32ToString,
    };
    match (from, to) {
        // Widening: safe, no precision loss
        (PortType::U64, PortType::F64) => Some(Box::new(U64ToF64::new())),
        // Widening: unsigned integers
        (PortType::U32, PortType::U64) => Some(Box::new(U32ToU64::new())),
        (PortType::U32, PortType::F64) => Some(Box::new(U32ToF64::new())),
        // Widening: signed integers
        (PortType::I32, PortType::I64) => Some(Box::new(I32ToI64::new())),
        (PortType::I32, PortType::F64) => Some(Box::new(I32ToF64::new())),
        (PortType::I64, PortType::F64) => Some(Box::new(I64ToF64::new())),
        // Widening: floats
        (PortType::F32, PortType::F64) => Some(Box::new(F32ToF64::new())),
        // To-string: all types render as strings
        (PortType::U64, PortType::Str) => Some(Box::new(U64ToString::new())),
        (PortType::F64, PortType::Str) => Some(Box::new(F64ToString::new())),
        (PortType::Bool, PortType::Str) => Some(Box::new(BoolToStr::new())),
        (PortType::Json, PortType::Str) => Some(Box::new(JsonToStr::new())),
        (PortType::U32, PortType::Str) => Some(Box::new(U32ToString::new())),
        (PortType::I32, PortType::Str) => Some(Box::new(I32ToString::new())),
        (PortType::I64, PortType::Str) => Some(Box::new(I64ToString::new())),
        (PortType::F32, PortType::Str) => Some(Box::new(F32ToString::new())),
        // Bool to numeric
        (PortType::Bool, PortType::U64) => Some(Box::new(BoolToU64::new())),
        _ => None,
    }
}
