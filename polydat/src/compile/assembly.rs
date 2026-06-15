// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Programmatic assembly API for building Polydat Kernels.
//!
//! The assembler validates wiring and types, auto-inserts edge adapters,
//! topologically sorts nodes, and produces either a Phase 1 runtime
//! kernel or a Phase 2 compiled kernel.

use std::collections::HashMap;

use crate::compile::closures::{CompiledKernelRaw, CompiledKernelPush, CompiledKernelPull, CompiledKernelPushPull};
use crate::compile::select::{self, GraphAnalysis, ProvMode, P2Engine};
use crate::kernel::{PolydatKernel, PolydatProgram, WireSource};
use crate::ast::{PolydatNode, PortType};
use crate::library::convert::{F64ToString, U64ToF64, U64ToString};
use crate::library::json::JsonToStr;

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
    node: Box<dyn PolydatNode>,
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
                writeln!(f, "  The graph contains a loop. Polydat graphs must be acyclic")?;
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
    nodes: Vec<Box<dyn PolydatNode>>,
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
    /// Names declared with `init` (SRD 11 §"Init Binding Contract").
    const_outputs: std::collections::HashSet<String>,
}

impl ResolvedDag {
    /// Coordinate input names (for P2/P3 kernels that use positional u64 buffers).
    fn input_names(&self) -> Vec<String> {
        self.input_defs[..self.coord_count].iter()
            .map(|d| d.name.clone()).collect()
    }
}

/// Per-port slot layout for compiled kernels
/// (type_system_alignment.md §8.4 layer 1). Each port occupies
/// `PortType::slot_width()` consecutive buffer slots; for the
/// all-scalar kernels that exist today this degenerates exactly
/// to the historical "one slot per port" layout.
struct SlotLayout {
    /// Per kernel input: first slot index.
    input_starts: Vec<usize>,
    /// Total slots occupied by kernel inputs.
    coord_slots: usize,
    /// Per node, per output port: first slot index.
    port_offsets: Vec<Vec<usize>>,
    /// Total buffer length.
    total_slots: usize,
}

fn slot_layout(resolved: &ResolvedDag) -> SlotLayout {
    let mut input_starts = Vec::with_capacity(resolved.coord_count);
    let mut next = 0usize;
    for d in &resolved.input_defs[..resolved.coord_count] {
        input_starts.push(next);
        next += d.port_type.slot_width();
    }
    let coord_slots = next;
    let mut port_offsets: Vec<Vec<usize>> = Vec::with_capacity(resolved.nodes.len());
    for node in &resolved.nodes {
        let mut po = Vec::with_capacity(node.meta().outs.len());
        for out in &node.meta().outs {
            po.push(next);
            next += out.typ.slot_width();
        }
        port_offsets.push(po);
    }
    SlotLayout { input_starts, coord_slots, port_offsets, total_slots: next }
}

/// Compiled-op selection for one node: pure-scalar `compiled_u64`
/// first (cheapest dispatch), then the slot op for slice-bearing
/// nodes (§8.4 layer 3), else `None` → typed-eval fallback.
fn node_step_op(
    node: &dyn crate::ast::PolydatNode,
) -> Option<(crate::compile::closures::StepOp, Vec<crate::ast::ScratchElem>)> {
    if let Some(op) = node.compiled_u64() {
        return Some((crate::compile::closures::StepOp::U64(op), Vec::new()));
    }
    node.compiled_slot().map(|kit| {
        (crate::compile::closures::StepOp::Slot(kit.op), kit.scratch)
    })
}

impl SlotLayout {
    /// Flattened input slot list for one node: every wire source
    /// contributes its full width, in port order.
    fn input_slots(&self, resolved: &ResolvedDag, node_idx: usize) -> Vec<usize> {
        let mut slots = Vec::new();
        for source in &resolved.wiring[node_idx] {
            let (start, w) = match source {
                WireSource::Input(c) => (
                    self.input_starts.get(*c).copied().unwrap_or(*c),
                    resolved
                        .input_defs
                        .get(*c)
                        .map(|d| d.port_type.slot_width())
                        .unwrap_or(1),
                ),
                WireSource::NodeOutput(u, p) => (
                    self.port_offsets[*u][*p],
                    resolved.nodes[*u].meta().outs[*p].typ.slot_width(),
                ),
            };
            slots.extend(start..start + w);
        }
        slots
    }

    /// Flattened output slot list for one node.
    fn output_slots(&self, resolved: &ResolvedDag, node_idx: usize) -> Vec<usize> {
        let mut slots = Vec::new();
        for (p, out) in resolved.nodes[node_idx].meta().outs.iter().enumerate() {
            let start = self.port_offsets[node_idx][p];
            slots.extend(start..start + out.typ.slot_width());
        }
        slots
    }

    /// Output name → first slot of the named port.
    fn named_outputs(&self, resolved: &ResolvedDag) -> HashMap<String, usize> {
        resolved
            .output_map
            .iter()
            .map(|(name, (n, p))| (name.clone(), self.port_offsets[*n][*p]))
            .collect()
    }

    /// Axiom S2: per-slot Ref2 mask over the whole buffer —
    /// kernel inputs and node outputs alike. Both slots of a Ref
    /// pair are masked.
    fn ref_slot_mask(&self, resolved: &ResolvedDag) -> Vec<bool> {
        let mut mask = vec![false; self.total_slots];
        for (i, d) in resolved.input_defs[..resolved.coord_count].iter().enumerate() {
            if d.port_type.slot_color() == crate::ast::SlotColor::Ref2 {
                let start = self.input_starts[i];
                mask[start] = true;
                mask[start + 1] = true;
            }
        }
        for (n, node) in resolved.nodes.iter().enumerate() {
            for (p, out) in node.meta().outs.iter().enumerate() {
                if out.typ.slot_color() == crate::ast::SlotColor::Ref2 {
                    let start = self.port_offsets[n][p];
                    mask[start] = true;
                    mask[start + 1] = true;
                }
            }
        }
        mask
    }

    /// First slot of each Ref2-colored output port of one node,
    /// in port order — pairs with the node's `CompiledSlotKit`
    /// scratch entries (axiom S3).
    fn ref_output_starts(&self, resolved: &ResolvedDag, node_idx: usize) -> Vec<usize> {
        resolved.nodes[node_idx]
            .meta()
            .outs
            .iter()
            .enumerate()
            .filter(|(_, out)| out.typ.slot_color() == crate::ast::SlotColor::Ref2)
            .map(|(p, _)| self.port_offsets[node_idx][p])
            .collect()
    }

    /// Expand per-INPUT dependent-step lists to per-SLOT lists so
    /// the kernels' slot-indexed dirty tracking / changed-mask
    /// bits stay coherent under multi-slot inputs (every slot of
    /// one input shares that input's dependents). Identity for
    /// all-scalar inputs.
    fn expand_dependents(
        &self,
        resolved: &ResolvedDag,
        deps: &[Vec<usize>],
    ) -> Vec<Vec<usize>> {
        let mut out = Vec::with_capacity(self.coord_slots);
        for (i, d) in resolved.input_defs[..resolved.coord_count].iter().enumerate() {
            for _ in 0..d.port_type.slot_width() {
                out.push(deps.get(i).cloned().unwrap_or_default());
            }
        }
        out
    }
}

/// Builder for assembling a Polydat Kernel programmatically.
pub struct PolydatAssembler {
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
    /// Names declared with the `const` keyword. Subject to the
    /// init-binding contract (SRD 11 §"Init Binding Contract").
    const_outputs: std::collections::HashSet<String>,
    /// SRD 15 §"Strict Wire Mode": when true, the resolver
    /// auto-inserts `AssertValue` nodes in front of every wire
    /// input whose declared `Port.constraint` can't be statically
    /// proven satisfied by the source.
    pub(crate) strict_values: bool,
    /// SRD 15: when true, the resolver auto-inserts `AssertType`
    /// nodes in front of wires where the source's runtime variant
    /// can't be statically proven to match the sink's declared
    /// `PortType`. Today this is mainly latent — the type system
    /// already proves variants match for nearly every wire — so
    /// the flag exists for forward compatibility with dynamic
    /// JSON navigation, `Ext` unwraps, and cross-adapter values.
    pub(crate) strict_types: bool,
}

/// `(coord_slots, total_slots, steps, named outputs, ref-slot
/// mask)` — the Phase-2 compiled layout shared by the closure
/// kernel builders.
type P2Layout = (
    usize,
    usize,
    Vec<crate::compile::closures::P2Step>,
    HashMap<String, usize>,
    Vec<bool>,
);

/// `(coord_slots, total_slots, JIT steps, named outputs)` — the
/// JIT compiled layout shared by the native kernel builders.
#[cfg(feature = "jit")]
type JitLayout = (
    usize,
    usize,
    Vec<(crate::compile::jit::JitOp, Vec<usize>, Vec<usize>)>,
    HashMap<String, usize>,
);

impl PolydatAssembler {
    /// Create a new assembler with the given coordinate names.
    pub fn new(input_names: Vec<String>) -> Self {
        let coord_count = input_names.len();
        let input_defs: Vec<crate::kernel::InputDef> = input_names.into_iter()
            .map(|name| crate::kernel::InputDef {
                name,
                default: crate::ast::Value::U64(0),
                port_type: crate::ast::PortType::U64,
                kind: crate::kernel::InputKind::Coordinate,
            })
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
            const_outputs: std::collections::HashSet::new(),
            strict_values: false,
            strict_types: false,
        }
    }

    /// Enable strict-wire-mode auto-insertion of value/type assertion
    /// nodes (SRD 15 §"Strict Wire Mode"). Off by default — the
    /// caller (compiler / DSL pragma extractor) opts in.
    pub fn set_strict_wires(&mut self, strict_types: bool, strict_values: bool) {
        self.strict_types = strict_types;
        self.strict_values = strict_values;
    }

    /// Set the source text and diagnostic context for this assembler.
    /// Called by the DSL compiler to attach the original Polydat source.
    pub fn set_context(&mut self, source: &str, context: &str) {
        self.source = source.to_string();
        self.context = context.to_string();
    }

    /// Add a node to the assembler with the given name and input wiring.
    pub fn add_node(
        &mut self,
        name: impl Into<String>,
        node: Box<dyn PolydatNode>,
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
        if modifier != crate::dsl::ast::BindingModifier::NONE {
            self.output_modifiers.insert(name.to_string(), modifier);
        }
    }

    /// Mark an output as declared with the `const` keyword. Compile-
    /// time and scope-activation checks (SRD 11 §"Init Binding
    /// Contract") read this set to enforce const-like-constraint
    /// semantics on the binding.
    pub fn mark_const_output(&mut self, name: &str) {
        self.const_outputs.insert(name.to_string());
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

    /// Declare an additional named input.
    ///
    /// Added after coordinate inputs. Nodes wire to it via
    /// `WireRef::input(name)` — same as coordinate inputs.
    /// `kind` controls the lifecycle classification used by the
    /// init-binding contract (see [evaluation_model.md](../../docs/design/evaluation_model.md)
    /// §"Effectively-Const Nodes"): `IterationExtern` for slots
    /// populated by `materialize_wiring_from_outer`, `ExternalWrite` for slots
    /// written by capture extraction.
    pub fn add_input(&mut self, name: impl Into<String>, default: crate::ast::Value, port_type: crate::ast::PortType, kind: crate::kernel::InputKind) -> &mut Self {
        self.input_defs.push(crate::kernel::InputDef {
            name: name.into(),
            default,
            port_type,
            kind,
        });
        self
    }

    /// Return the names of all inputs (coordinates + captures).
    pub fn input_names(&self) -> Vec<&str> {
        self.input_defs.iter().map(|d| d.name.as_str()).collect()
    }

    /// Query the output port type of a named node (first output).
    /// Returns `None` if the node is not found or has no output
    /// ports; callers surface the absence as a loud diagnostic
    /// rather than silently substituting a default.
    pub fn node_output_type(&self, name: &str) -> Option<crate::ast::PortType> {
        self.nodes.iter()
            .find(|n| n.name == name)
            .and_then(|n| n.node.meta().outs.first())
            .map(|p| p.typ)
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

    /// Look up the port type of a graph input by name.
    pub fn input_type(&self, name: &str) -> Option<PortType> {
        self.input_defs.iter()
            .find(|d| d.name == name)
            .map(|d| d.port_type)
    }

    /// Look up the produced port type of a `WireRef`. Returns `None`
    /// if the wire's source isn't yet known to the assembler (e.g.
    /// it points to a not-yet-added node — a bug in the binding
    /// compiler if it happens).
    pub fn wire_type(&self, wire: &WireRef) -> Option<PortType> {
        match wire {
            WireRef::Input(name) => self.input_type(name),
            WireRef::Node(name, port_idx) => self.nodes.iter()
                .find(|pn| &pn.name == name)
                .and_then(|pn| pn.node.meta().outs.get(*port_idx))
                .map(|p| p.typ),
        }
    }

    /// Validate, resolve, and produce a Phase 1 runtime kernel.
    pub fn compile(self) -> Result<PolydatKernel, AssemblyError> {
        self.compile_with_log(None)
    }

    /// Compile with diagnostic event logging.
    pub fn compile_with_log(self, mut log: Option<&mut crate::dsl::events::CompileEventLog>) -> Result<PolydatKernel, AssemblyError> {
        let resolved = self.resolve_with_log(log.as_deref_mut())?;
        let _coord_names = resolved.input_names();
        let modifiers = resolved.output_modifiers.clone();
        let kernel = PolydatKernel::new_with_inputs(
            resolved.nodes,
            resolved.wiring,
            resolved.input_defs,
            resolved.coord_count,
            resolved.output_map,
            resolved.output_order,
            resolved.const_outputs,
            modifiers,
            &resolved.source,
            &resolved.context,
            log,
            false,
        ).map_err(AssemblyError::Other)?;
        Ok(kernel)
    }

    /// Compile with strict mode: config wire violations are errors,
    /// implicit type coercions are rejected, unused bindings flagged.
    pub fn compile_strict(self, strict: bool) -> Result<PolydatKernel, AssemblyError> {
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
        let kernel = PolydatKernel::new_with_inputs(
            resolved.nodes,
            resolved.wiring,
            resolved.input_defs,
            resolved.coord_count,
            resolved.output_map,
            resolved.output_order,
            resolved.const_outputs,
            modifiers,
            &resolved.source,
            &resolved.context,
            None,
            true,
        ).map_err(AssemblyError::Other)?;
        Ok(kernel)
    }

    /// Validate, resolve, and attempt Phase 2 compilation.
    ///
    /// Returns `Ok(CompiledKernelPushPull)` if all nodes are u64-only and provide
    /// `compiled_u64()`. Falls back to `Err(Box<PolydatKernel>)` (a working
    /// Phase 1 kernel; boxed so the happy-path `Result` stays small) if any
    /// node cannot be compiled.
    pub fn try_compile(self) -> Result<CompiledKernelPushPull, Box<PolydatKernel>> {
        let resolved = self.resolve().expect("assembly validation failed");
        let coord_names = resolved.input_names();
        let layout = slot_layout(&resolved);

        // Try to extract a compiled op from every node
        let mut compiled_ops = Vec::with_capacity(resolved.nodes.len());
        let mut all_compilable = true;
        for node in &resolved.nodes {
            if let Some(op) = node_step_op(node.as_ref()) {
                compiled_ops.push(Some(op));
            } else {
                all_compilable = false;
                compiled_ops.push(None);
            }
        }

        if !all_compilable {
            // Fall back to Phase 1
            return Err(Box::new(PolydatKernel::new(
                resolved.nodes,
                resolved.wiring,
                coord_names,
                resolved.output_map,
                &resolved.source,
                &resolved.context,
            )));
        }

        // Build compiled steps over the per-port-width layout
        let mut steps = Vec::with_capacity(resolved.nodes.len());
        for (node_idx, op) in compiled_ops.into_iter().enumerate() {
            let (op, scratch) = op.unwrap(); // safe: all_compilable checked above
            steps.push(crate::compile::closures::P2Step {
                op,
                input_slots: layout.input_slots(&resolved, node_idx),
                output_slots: layout.output_slots(&resolved, node_idx),
                ref_output_starts: if scratch.is_empty() {
                    Vec::new()
                } else {
                    layout.ref_output_starts(&resolved, node_idx)
                },
                scratch,
            });
        }

        let output_map = layout.named_outputs(&resolved);

        let dependents = layout.expand_dependents(
            &resolved,
            &PolydatProgram::compute_dependents(
                &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                resolved.coord_count,
            ),
        );
        let ref_slots = layout.ref_slot_mask(&resolved);
        Ok(CompiledKernelPushPull::new(
            layout.coord_slots, layout.total_slots, steps, output_map, dependents, ref_slots,
        ))
    }

    /// Phase 2 compilation without provenance caching.
    pub fn try_compile_raw(self) -> Result<CompiledKernelRaw, Box<PolydatKernel>> {
        let resolved = match self.resolve() {
            Ok(r) => r,
            Err(_) => return Err(Box::new(PolydatKernel::new(vec![], vec![], vec![], HashMap::new(), "", "(fallback)"))),
        };
        let coord_names = resolved.input_names();
        let layout = slot_layout(&resolved);
        let mut compiled_ops = Vec::with_capacity(resolved.nodes.len());
        let mut all_compilable = true;
        for node in &resolved.nodes {
            if let Some(op) = node_step_op(node.as_ref()) {
                compiled_ops.push(Some(op));
            } else {
                all_compilable = false;
                compiled_ops.push(None);
            }
        }
        if !all_compilable {
            return Err(Box::new(PolydatKernel::new(
                resolved.nodes, resolved.wiring, coord_names.clone(), resolved.output_map,
                &resolved.source, &resolved.context,
            )));
        }
        let mut steps = Vec::with_capacity(resolved.nodes.len());
        for (node_idx, op) in compiled_ops.into_iter().enumerate() {
            let (op, scratch) = op.unwrap();
            steps.push(crate::compile::closures::P2Step {
                op,
                input_slots: layout.input_slots(&resolved, node_idx),
                output_slots: layout.output_slots(&resolved, node_idx),
                ref_output_starts: if scratch.is_empty() {
                    Vec::new()
                } else {
                    layout.ref_output_starts(&resolved, node_idx)
                },
                scratch,
            });
        }
        let output_map = layout.named_outputs(&resolved);
        let ref_slots = layout.ref_slot_mask(&resolved);
        Ok(CompiledKernelRaw::new(
            layout.coord_slots, layout.total_slots, steps, output_map, ref_slots,
        ))
    }

    /// Phase 2 compilation with push-side provenance only (no cone guard).
    pub fn try_compile_push(self) -> Result<CompiledKernelPush, Box<PolydatKernel>> {
        let resolved = match self.resolve() {
            Ok(r) => r,
            Err(_) => return Err(Box::new(PolydatKernel::new(vec![], vec![], vec![], HashMap::new(), "", "(fallback)"))),
        };
        let coord_names = resolved.input_names();
        let (coord_count, total_slots, steps, output_map, ref_slots) =
            match Self::build_p2_layout(&resolved) {
                Some(r) => r,
                None => return Err(Box::new(PolydatKernel::new(
                    resolved.nodes, resolved.wiring, coord_names, resolved.output_map,
                    &resolved.source, &resolved.context))),
            };
        let dependents = slot_layout(&resolved).expand_dependents(
            &resolved,
            &PolydatProgram::compute_dependents(
                &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                resolved.coord_count,
            ),
        );
        Ok(CompiledKernelPush::new(coord_count, total_slots, steps, output_map, dependents, ref_slots))
    }

    /// Phase 2 compilation with pull-side cone guard only (no per-node skip).
    pub fn try_compile_pull(self) -> Result<CompiledKernelPull, Box<PolydatKernel>> {
        let resolved = match self.resolve() {
            Ok(r) => r,
            Err(_) => return Err(Box::new(PolydatKernel::new(vec![], vec![], vec![], HashMap::new(), "", "(fallback)"))),
        };
        let coord_names = resolved.input_names();
        let (coord_count, total_slots, steps, output_map, ref_slots) =
            match Self::build_p2_layout(&resolved) {
                Some(r) => r,
                None => return Err(Box::new(PolydatKernel::new(
                    resolved.nodes, resolved.wiring, coord_names, resolved.output_map,
                    &resolved.source, &resolved.context))),
            };
        let dependents = slot_layout(&resolved).expand_dependents(
            &resolved,
            &PolydatProgram::compute_dependents(
                &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                resolved.coord_count,
            ),
        );
        Ok(CompiledKernelPull::new(coord_count, total_slots, steps, output_map, &dependents, ref_slots))
    }

    /// Shared: extract P2 compiled steps + slot layout from resolved DAG.
    /// Returns None if any node lacks a compiled_u64 implementation.
    fn build_p2_layout(
        resolved: &ResolvedDag,
    ) -> Option<P2Layout> {
        let layout = slot_layout(resolved);

        let mut compiled_ops = Vec::with_capacity(resolved.nodes.len());
        for node in &resolved.nodes {
            compiled_ops.push(node_step_op(node.as_ref())?);
        }

        let mut steps = Vec::with_capacity(resolved.nodes.len());
        for (node_idx, (op, scratch)) in compiled_ops.into_iter().enumerate() {
            steps.push(crate::compile::closures::P2Step {
                op,
                input_slots: layout.input_slots(resolved, node_idx),
                output_slots: layout.output_slots(resolved, node_idx),
                ref_output_starts: if scratch.is_empty() {
                    Vec::new()
                } else {
                    layout.ref_output_starts(resolved, node_idx)
                },
                scratch,
            });
        }
        let output_map = layout.named_outputs(resolved);
        let ref_slots = layout.ref_slot_mask(resolved);

        Some((layout.coord_slots, layout.total_slots, steps, output_map, ref_slots))
    }

    /// Shared: resolve nodes to JIT steps + slot layout.
    #[cfg(feature = "jit")]
    fn build_jit_layout(resolved: &ResolvedDag)
        -> Result<JitLayout, String>
    {
        let layout = slot_layout(resolved);

        // P3 corollary (jit_boundary.md slot-state axioms): a
        // pure-P3 kernel must contain no Ref2-colored ports —
        // slice-bearing nodes classify Fallback, but enforce the
        // axiom directly rather than relying on classification.
        for node in &resolved.nodes {
            for out in &node.meta().outs {
                if out.typ.slot_color() == crate::ast::SlotColor::Ref2 {
                    return Err(format!(
                        "node '{}' has a Ref2-colored output ({}); pure-P3 \
                         kernels carry no reference slots",
                        node.meta().name, out.typ
                    ));
                }
            }
        }

        let mut jit_steps = Vec::new();
        for (node_idx, node) in resolved.nodes.iter().enumerate() {
            let jit_op = crate::compile::jit::classify_node(node.as_ref());
            jit_steps.push((
                jit_op,
                layout.input_slots(resolved, node_idx),
                layout.output_slots(resolved, node_idx),
            ));
        }

        if jit_steps.iter().any(|(op, _, _)| matches!(op, crate::compile::jit::JitOp::Fallback)) {
            return Err("some nodes cannot be JIT-compiled".into());
        }

        let output_map = layout.named_outputs(resolved);
        Ok((layout.coord_slots, layout.total_slots, jit_steps, output_map))
    }

    /// Phase 3 JIT: push+pull (full provenance).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit(self) -> Result<crate::compile::jit::JitKernelPushPull, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        let deps = slot_layout(&resolved).expand_dependents(
            &resolved,
            &PolydatProgram::compute_dependents(
                &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                resolved.coord_count,
            ),
        );
        crate::compile::jit::compile_jit_push_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, deps)
    }

    /// Phase 3 JIT: raw (no provenance).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit_raw(self) -> Result<crate::compile::jit::JitKernelRaw, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        crate::compile::jit::compile_jit_raw(coord_count, total_slots, jit_steps, output_map, resolved.nodes)
    }

    /// Phase 3 JIT: push-only (per-node dirty tracking, no cone guard).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit_push(self) -> Result<crate::compile::jit::JitKernelPush, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        let deps = slot_layout(&resolved).expand_dependents(
            &resolved,
            &PolydatProgram::compute_dependents(
                &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                resolved.coord_count,
            ),
        );
        crate::compile::jit::compile_jit_push(coord_count, total_slots, jit_steps, output_map, resolved.nodes, deps)
    }

    /// Phase 3 JIT: pull-only (cone guard, no per-node dirty tracking).
    #[cfg(feature = "jit")]
    pub fn try_compile_jit_pull(self) -> Result<crate::compile::jit::JitKernelPull, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let (coord_count, total_slots, jit_steps, output_map) = Self::build_jit_layout(&resolved)?;
        let deps = slot_layout(&resolved).expand_dependents(
            &resolved,
            &PolydatProgram::compute_dependents(
                &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                resolved.coord_count,
            ),
        );
        crate::compile::jit::compile_jit_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, &deps)
    }

    /// Analyze the graph and auto-select the optimal P2 provenance mode.
    ///
    /// Returns a `P2Engine` enum wrapping the monomorphic kernel variant.
    /// The selection is based on graph structure (cone sizes, input count).
    pub fn auto_compile_p2(self) -> Result<(P2Engine, GraphAnalysis), String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let analysis = select::analyze_graph(&resolved.nodes, &resolved.wiring, &resolved.output_map);
        let mode = select::select_prov_mode(&analysis);

        let (coord_count, total_slots, steps, output_map, ref_slots) =
            match Self::build_p2_layout(&resolved) {
                Some(r) => r,
                None => return Err("not all nodes support P2 compilation".into()),
            };

        let engine = match mode {
            ProvMode::Raw => {
                P2Engine::Raw(CompiledKernelRaw::new(coord_count, total_slots, steps, output_map, ref_slots))
            }
            ProvMode::Pull => {
                let deps = slot_layout(&resolved).expand_dependents(
                    &resolved,
                    &PolydatProgram::compute_dependents(
                        &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                        resolved.coord_count,
                    ),
                );
                P2Engine::Pull(CompiledKernelPull::new(coord_count, total_slots, steps, output_map, &deps, ref_slots))
            }
            ProvMode::PushPull => {
                let deps = slot_layout(&resolved).expand_dependents(
                    &resolved,
                    &PolydatProgram::compute_dependents(
                        &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                        resolved.coord_count,
                    ),
                );
                P2Engine::PushPull(CompiledKernelPushPull::new(coord_count, total_slots, steps, output_map, deps, ref_slots))
            }
        };
        Ok((engine, analysis))
    }

    /// Analyze the graph and auto-select the optimal P3 JIT provenance mode.
    #[cfg(feature = "jit")]
    pub fn auto_compile_p3(self) -> Result<(select::P3Engine, GraphAnalysis), String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let analysis = select::analyze_graph(&resolved.nodes, &resolved.wiring, &resolved.output_map);
        let mode = select::select_prov_mode(&analysis);

        let (coord_count, total_slots, jit_steps, output_map) =
            Self::build_jit_layout(&resolved)?;

        let engine = match mode {
            ProvMode::Raw => {
                let k = crate::compile::jit::compile_jit_raw(coord_count, total_slots, jit_steps, output_map, resolved.nodes)?;
                select::P3Engine::Raw(k)
            }
            ProvMode::Pull => {
                let deps = slot_layout(&resolved).expand_dependents(
                    &resolved,
                    &PolydatProgram::compute_dependents(
                        &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                        resolved.coord_count,
                    ),
                );
                let k = crate::compile::jit::compile_jit_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, &deps)?;
                select::P3Engine::Pull(k)
            }
            ProvMode::PushPull => {
                let deps = slot_layout(&resolved).expand_dependents(
                    &resolved,
                    &PolydatProgram::compute_dependents(
                        &PolydatProgram::compute_provenance(&resolved.nodes, &resolved.wiring),
                        resolved.coord_count,
                    ),
                );
                let k = crate::compile::jit::compile_jit_push_pull(coord_count, total_slots, jit_steps, output_map, resolved.nodes, deps)?;
                select::P3Engine::PushPull(k)
            }
        };
        Ok((engine, analysis))
    }

    /// Validate, resolve, and compile a hybrid kernel where each node
    /// runs at its optimal level (JIT native code or Phase 2 closure).
    ///
    /// This always succeeds for u64-only DAGs — no all-or-nothing
    /// fallback. JIT-able nodes get native code, others get closures.
    pub fn compile_hybrid(self) -> Result<crate::compile::hybrid::HybridKernel, String> {
        let resolved = self.resolve().map_err(|e| format!("{e}"))?;
        let _coord_names = resolved.input_names();
        let layout = slot_layout(&resolved);

        let output_map = layout.named_outputs(&resolved);
        let input_widths: Vec<usize> = resolved.input_defs[..resolved.coord_count]
            .iter()
            .map(|d| d.port_type.slot_width())
            .collect();

        let ref_slots = layout.ref_slot_mask(&resolved);
        let mut kernel = crate::compile::hybrid::build_hybrid(
            &resolved.nodes,
            &resolved.wiring,
            layout.coord_slots,
            layout.total_slots,
            &layout.port_offsets,
            &layout.input_starts,
            &input_widths,
            output_map,
            ref_slots,
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
        let mut assertion_count = 0usize;
        let strict_values = self.strict_values;
        let strict_types = self.strict_types;

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

                // Printf accepts any input type — skip type checking for it.
                // `pick` is also type-flexible: its selector wires must be
                // Bool but its value wires can be any type so long as they
                // share a common type at eval — uniformity is enforced at
                // eval time (SRD-66 §"Surface 3"). The variadic ctor can't
                // know the value-half port type at construction, so we
                // declare placeholder ports and skip the assembler check;
                // the per-eval validator catches mismatches with a clear
                // panic via `enrich_eval_panic`.
                //
                // The `log_*` family is also type-polymorphic by intent:
                // `log_info(regex_match(...))` is the canonical SRD-66
                // probe-phase shape, where the input is Bool. Without
                // skipping the check, the assembler inserts a Bool→Str
                // adapter that converts the value, breaking the
                // result-binding writeback (the cell receives Str("false")
                // instead of Bool(false), and downstream `pick` rejects
                // it as non-bool). The eval is a pass-through, so the
                // actual value flows through unchanged.
                // `exactly_one_value` is similarly type-polymorphic:
                // its eval inspects the actual `Value` variant and
                // walks structural shape (Json / VecF32 / VecI32) or
                // passes through scalars. The declared input port
                // type is a placeholder. Without the skip, an
                // upstream `Json` body (the magic `body` extern's
                // declared type) gets coerced to `Str` via the
                // `JsonToStr` adapter — at which point the SRD-66
                // probe shape `regex_match(exactly_one_value(body), …)`
                // sees JSON-serialised text with `\n` literal
                // escapes, and `^`-anchored regexes never match
                // inside `create_statement` columns.
                let node_name_for_typing = &all_nodes[node_idx].node.meta().name;
                let skip_type_check = node_name_for_typing == "printf"
                    || node_name_for_typing == "pick"
                    || node_name_for_typing == "log_debug"
                    || node_name_for_typing == "log_info"
                    || node_name_for_typing == "log_warn"
                    || node_name_for_typing == "log_error"
                    || node_name_for_typing == "exactly_one_value"
                    || node_name_for_typing == "json_text"
                    || node_name_for_typing == "str_concat";

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

                // === Strict-wire assertion insertion (SRD 15) ===
                //
                // After a wire is resolved (and any type adapter
                // inserted), look at the sink port's declared
                // `constraint`. If strict_values is on, we either
                // prove the source already satisfies it (skip) or
                // splice an `AssertValue` node in front of the
                // sink. The skip cases mirror the four bullets in
                // SRD 15 §"Strict Wire Mode": static type match is
                // already handled by the adapter pass above; here
                // we cover constant sources and upstream-assertion
                // chains for value constraints.
                let sink_port = &all_nodes[node_idx].node.meta().wire_inputs()[port_idx];
                if let Some(constraint) = sink_port.constraint {
                    let last_source = node_wiring.last().expect("wire just pushed").clone();
                    if strict_values && !value_constraint_proven(
                        &all_nodes,
                        &last_source,
                        &constraint,
                    ) {
                        let assert_name = format!("__assert_v_{assertion_count}");
                        assertion_count += 1;
                        let assert_idx = all_nodes.len();

                        if let Some(ref mut log) = log {
                            let from_name = match wire_ref {
                                WireRef::Input(n) => n.clone(),
                                WireRef::Node(n, _) => n.clone(),
                            };
                            log.push(crate::dsl::events::CompileEvent::AssertionInserted {
                                from_node: from_name,
                                to_node: all_nodes[node_idx].name.clone(),
                                kind: format!("{:?} value-assert {:?}",
                                    expected_type, &constraint),
                            });
                        }

                        all_name_to_idx.insert(assert_name.clone(), assert_idx);
                        let assert_wiring = vec![last_source];
                        while resolved_wiring.len() <= assert_idx {
                            resolved_wiring.push(Vec::new());
                        }
                        resolved_wiring[assert_idx] = assert_wiring;

                        all_nodes.push(PendingNode {
                            name: assert_name,
                            node: crate::library::assertions::assert_value_node(
                                expected_type,
                                constraint,
                            ),
                            inputs: vec![],
                        });

                        // Replace the just-pushed source with the
                        // assertion's output.
                        *node_wiring.last_mut().unwrap() =
                            WireSource::NodeOutput(assert_idx, 0);
                    } else if let Some(ref mut log) = log {
                        let from_name = match wire_ref {
                            WireRef::Input(n) => n.clone(),
                            WireRef::Node(n, _) => n.clone(),
                        };
                        log.push(crate::dsl::events::CompileEvent::AssertionSkipped {
                            from_node: from_name,
                            to_node: all_nodes[node_idx].name.clone(),
                            reason: assertion_skip_reason(
                                strict_values,
                                &all_nodes,
                                &last_source,
                                &constraint,
                            ),
                        });
                    }
                } else if strict_types && source_type != expected_type {
                    // Type mismatch was already adapted above; the
                    // post-adapter wire is statically the right
                    // type. No assertion needed. Tracking the skip
                    // here is forward-compatible — once dynamic
                    // type cases (JSON nav, Ext unwraps) appear,
                    // this is where the AssertType insertion would
                    // hook in.
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
            let rules = crate::compile::fusion::default_rules();
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

                // Convert to Option<Box<dyn PolydatNode>> for the fusion pass.
                let mut opt_nodes: Vec<Option<Box<dyn PolydatNode>>> = all_nodes
                    .into_iter()
                    .map(|pn| Some(pn.node))
                    .collect();

                let fused_count = crate::compile::fusion::apply_fusions(
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
                        node: opt.unwrap_or_else(|| Box::new(crate::library::identity::Identity::new(crate::ast::PortType::U64))),
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
            // Side-effecting nodes are pinned alive regardless
            // of reachability from a declared output. `log_info`
            // and friends emit one audit-log line per eval as a
            // deliberate side effect — DCE-pruning them would
            // silently drop diagnostic logging the operator
            // explicitly asked for. The set is closed and
            // matched by node-meta name so the marker survives
            // any wiring shape (passthrough, captured-but-unused,
            // synthesised wrapper, etc.).
            for (idx, pn) in all_nodes.iter().enumerate() {
                if matches!(pn.node.meta().name.as_str(),
                    "log_debug" | "log_info" | "log_warn" | "log_error")
                {
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

        let mut sorted_nodes: Vec<Option<Box<dyn PolydatNode>>> = all_nodes
            .into_iter()
            .map(|pn| Some(pn.node))
            .collect();

        let final_nodes: Vec<Box<dyn PolydatNode>> = sorted_order
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
            const_outputs: self.const_outputs,
        })
    }
}

/// Decide whether the source feeding `wire_source` already
/// guarantees the sink's value `constraint` at compile time.
/// Returns `true` if the assertion can be safely skipped.
///
/// Today we recognise two skip cases (SRD 15 §"Strict Wire Mode"):
///
/// 1. **Constant source.** The source node has no wire inputs and
///    its name matches the convention used by `fixed::ConstU64`
///    et al. Const sources have already been validated against
///    their `ParamSpec.constraint` at the factory layer, so any
///    further runtime check would be redundant.
/// 2. **Upstream assertion.** The source is itself an
///    `AssertValue` node (its name starts with `__assert_v_`),
///    which already enforces the same or stronger contract.
fn value_constraint_proven(
    all_nodes: &[PendingNode],
    src: &WireSource,
    _constraint: &crate::dsl::const_constraints::ConstConstraint,
) -> bool {
    match src {
        WireSource::Input(_) => false,
        WireSource::NodeOutput(idx, _) => {
            let meta = all_nodes[*idx].node.meta();
            // Const-source heuristic: a node with no wire inputs
            // is a constant. Today's `ConstU64` / `ConstF64` /
            // `ConstBool` (in `nodes::fixed`) and the synthesised
            // `ConstNode` from compile-time folding both qualify.
            let no_wire_inputs = meta.wire_inputs().is_empty();
            if no_wire_inputs {
                return true;
            }
            // Upstream assertion: skip stacking the same guard.
            // Conservative — any `__assert_v_*` upstream counts as
            // proof. A fancier analysis would compare constraint
            // shapes; for now, idempotency is good enough.
            if meta.name.starts_with("__assert_v_")
                || meta.name.starts_with("assert_")
            {
                return true;
            }
            false
        }
    }
}

/// Format the reason a strict-wire assertion was skipped, for the
/// `AssertionSkipped` advisory event. Mirrors the bullets in SRD 15
/// §"Strict Wire Mode" so the log is grep-able.
fn assertion_skip_reason(
    strict_values: bool,
    all_nodes: &[PendingNode],
    src: &WireSource,
    _constraint: &crate::dsl::const_constraints::ConstConstraint,
) -> String {
    if !strict_values {
        return "strict_values not enabled".into();
    }
    match src {
        WireSource::Input(_) => "raw input wire".into(),
        WireSource::NodeOutput(idx, _) => {
            let meta = all_nodes[*idx].node.meta();
            if meta.wire_inputs().is_empty() {
                "constant source already validated".into()
            } else if meta.name.starts_with("__assert_v_")
                || meta.name.starts_with("assert_")
            {
                "upstream assertion".into()
            } else {
                "no skip rule matched".into()
            }
        }
    }
}

/// Return an auto-insert edge adapter for common coercions, if one exists.
/// Look up an auto-conversion adapter for type pairs (γ-5 / spec
/// expression_engine.md §5.4). The catalog is intra-graph
/// today plus the boundary-adapter sites that γ-5 + γ-6
/// extend it to. Returns `None` for type pairs the catalog
/// doesn't cover — callers must surface a typed
/// `TypeMismatch` error in that case.
/// Intra-graph wire adapter catalog. Consulted by the assembler
/// during construction to heal mismatched producer/consumer
/// `PortType` pairs. Strict: only adapters whose `eval` is
/// total over the input domain (never panics on any valid
/// runtime value of `from`). Lossy or parseable adapters
/// belong in [`boundary_adapter`] only.
pub fn auto_adapter(from: PortType, to: PortType) -> Option<Box<dyn PolydatNode>> {
    use crate::library::convert::{
        BoolToStr, BoolToU64,
        U32ToU64, U32ToI64, U32ToF64, U32ToString,
        I32ToI64, I32ToF64, I32ToString,
        I64ToF64, I64ToString,
        F32ToF64, F32ToString,
    };
    use crate::library::polyfill as P;
    use crate::library::polyfill_narrow as N;
    use crate::library::polyfill_128 as W;
    use crate::library::polyfill_complete as C;
    match (from, to) {
        // ── Numeric widening (lossless) ─────────────────────────
        (PortType::U64, PortType::F64) => Some(Box::new(U64ToF64::new())),
        (PortType::U32, PortType::U64) => Some(Box::new(U32ToU64::new())),
        (PortType::U32, PortType::I64) => Some(Box::new(U32ToI64::new())),
        (PortType::U32, PortType::F64) => Some(Box::new(U32ToF64::new())),
        (PortType::I32, PortType::I64) => Some(Box::new(I32ToI64::new())),
        (PortType::I32, PortType::F64) => Some(Box::new(I32ToF64::new())),
        (PortType::I64, PortType::F64) => Some(Box::new(I64ToF64::new())),
        (PortType::F32, PortType::F64) => Some(Box::new(F32ToF64::new())),

        // ── X → Str (every type renders as a string) ────────────
        (PortType::U64, PortType::Str)  => Some(Box::new(U64ToString::new())),
        (PortType::F64, PortType::Str)  => Some(Box::new(F64ToString::new())),
        (PortType::Bool, PortType::Str) => Some(Box::new(BoolToStr::new())),
        (PortType::Json, PortType::Str) => Some(Box::new(JsonToStr::new())),
        (PortType::U32, PortType::Str)  => Some(Box::new(U32ToString::new())),
        (PortType::I32, PortType::Str)  => Some(Box::new(I32ToString::new())),
        (PortType::I64, PortType::Str)  => Some(Box::new(I64ToString::new())),
        (PortType::F32, PortType::Str)  => Some(Box::new(F32ToString::new())),

        // ── Bool ↔ numeric (always-defined; 1/0 mapping) ────────
        (PortType::Bool, PortType::U64) => Some(Box::new(BoolToU64::new())),
        (PortType::Bool, PortType::U32) => Some(Box::new(P::BoolToU32::new())),
        (PortType::Bool, PortType::I64) => Some(Box::new(P::BoolToI64::new())),
        (PortType::Bool, PortType::I32) => Some(Box::new(P::BoolToI32::new())),
        (PortType::Bool, PortType::F64) => Some(Box::new(P::BoolToF64::new())),
        (PortType::Bool, PortType::F32) => Some(Box::new(P::BoolToF32::new())),
        (PortType::U64, PortType::Bool) => Some(Box::new(crate::library::convert::U64ToBool::new())),
        (PortType::U32, PortType::Bool) => Some(Box::new(P::U32ToBool::new())),
        (PortType::I64, PortType::Bool) => Some(Box::new(P::I64ToBool::new())),
        (PortType::I32, PortType::Bool) => Some(Box::new(P::I32ToBool::new())),
        (PortType::F64, PortType::Bool) => Some(Box::new(P::F64ToBool::new())),
        (PortType::F32, PortType::Bool) => Some(Box::new(P::F32ToBool::new())),

        // ── X → Bytes (little-endian serialize, always-defined) ─
        (PortType::U64, PortType::Bytes)  => Some(Box::new(P::U64ToBytes::new())),
        (PortType::U32, PortType::Bytes)  => Some(Box::new(P::U32ToBytes::new())),
        (PortType::I64, PortType::Bytes)  => Some(Box::new(P::I64ToBytes::new())),
        (PortType::I32, PortType::Bytes)  => Some(Box::new(P::I32ToBytes::new())),
        (PortType::F64, PortType::Bytes)  => Some(Box::new(P::F64ToBytes::new())),
        (PortType::F32, PortType::Bytes)  => Some(Box::new(P::F32ToBytes::new())),
        (PortType::Bool, PortType::Bytes) => Some(Box::new(P::BoolToBytes::new())),
        (PortType::VecF32, PortType::Bytes) => Some(Box::new(P::VecF32ToBytes::new())),
        (PortType::VecI32, PortType::Bytes) => Some(Box::new(P::VecI32ToBytes::new())),

        // ── X → Json (integer / bool wraps; F* and VecF32 are
        //              boundary-only because non-finite floats
        //              aren't representable in JSON) ────────────
        (PortType::U64, PortType::Json)  => Some(Box::new(P::U64ToJson::new())),
        (PortType::U32, PortType::Json)  => Some(Box::new(P::U32ToJson::new())),
        (PortType::I64, PortType::Json)  => Some(Box::new(P::I64ToJson::new())),
        (PortType::I32, PortType::Json)  => Some(Box::new(P::I32ToJson::new())),
        (PortType::Bool, PortType::Json) => Some(Box::new(P::BoolToJson::new())),
        (PortType::VecI32, PortType::Json) => Some(Box::new(P::VecI32ToJson::new())),

        // ── Vec ↔ Vec (VecI32 → VecF32 is lossless) ─────────────
        (PortType::VecI32, PortType::VecF32) => Some(Box::new(P::VecI32ToVecF32::new())),

        // ── Narrow cranelift widths (u8/i8/u16/i16/f16) ─────────
        // Lossless widenings + Display renders + Bool maps + LE
        // byte / JSON wraps, mirroring the u32/i32/f32 rows.
        // (type_system_alignment.md §8.1)
        (PortType::U8, PortType::U64)  => Some(Box::new(N::U8ToU64::new())),
        (PortType::U8, PortType::U32)  => Some(Box::new(N::U8ToU32::new())),
        (PortType::U8, PortType::U16)  => Some(Box::new(N::U8ToU16::new())),
        (PortType::U8, PortType::F64)  => Some(Box::new(N::U8ToF64::new())),
        (PortType::U16, PortType::U64) => Some(Box::new(N::U16ToU64::new())),
        (PortType::U16, PortType::U32) => Some(Box::new(N::U16ToU32::new())),
        (PortType::U16, PortType::F64) => Some(Box::new(N::U16ToF64::new())),
        (PortType::I8, PortType::I64)  => Some(Box::new(N::I8ToI64::new())),
        (PortType::I8, PortType::I32)  => Some(Box::new(N::I8ToI32::new())),
        (PortType::I8, PortType::I16)  => Some(Box::new(N::I8ToI16::new())),
        (PortType::I8, PortType::F64)  => Some(Box::new(N::I8ToF64::new())),
        (PortType::I16, PortType::I64) => Some(Box::new(N::I16ToI64::new())),
        (PortType::I16, PortType::I32) => Some(Box::new(N::I16ToI32::new())),
        (PortType::I16, PortType::F64) => Some(Box::new(N::I16ToF64::new())),
        (PortType::F16, PortType::F32) => Some(Box::new(N::F16ToF32::new())),
        (PortType::F16, PortType::F64) => Some(Box::new(N::F16ToF64::new())),
        // Totality fills: unsigned → strictly-larger signed, and
        // narrow int → f32 (exact, magnitude ≤ 2^24). All class A.
        (PortType::U8, PortType::I16)  => Some(Box::new(N::U8ToI16::new())),
        (PortType::U8, PortType::I32)  => Some(Box::new(N::U8ToI32::new())),
        (PortType::U8, PortType::I64)  => Some(Box::new(N::U8ToI64::new())),
        (PortType::U8, PortType::F32)  => Some(Box::new(N::U8ToF32::new())),
        (PortType::U16, PortType::I32) => Some(Box::new(N::U16ToI32::new())),
        (PortType::U16, PortType::I64) => Some(Box::new(N::U16ToI64::new())),
        (PortType::U16, PortType::F32) => Some(Box::new(N::U16ToF32::new())),
        (PortType::I8, PortType::F32)  => Some(Box::new(N::I8ToF32::new())),
        (PortType::I16, PortType::F32) => Some(Box::new(N::I16ToF32::new())),
        (PortType::U8, PortType::F16)  => Some(Box::new(N::U8ToF16::new())),
        (PortType::I8, PortType::F16)  => Some(Box::new(N::I8ToF16::new())),
        (PortType::U8, PortType::Str)  => Some(Box::new(N::U8ToString::new())),
        (PortType::U16, PortType::Str) => Some(Box::new(N::U16ToString::new())),
        (PortType::I8, PortType::Str)  => Some(Box::new(N::I8ToString::new())),
        (PortType::I16, PortType::Str) => Some(Box::new(N::I16ToString::new())),
        (PortType::F16, PortType::Str) => Some(Box::new(N::F16ToString::new())),
        (PortType::Bool, PortType::U8)  => Some(Box::new(N::BoolToU8::new())),
        (PortType::Bool, PortType::U16) => Some(Box::new(N::BoolToU16::new())),
        (PortType::Bool, PortType::I8)  => Some(Box::new(N::BoolToI8::new())),
        (PortType::Bool, PortType::I16) => Some(Box::new(N::BoolToI16::new())),
        (PortType::Bool, PortType::F16) => Some(Box::new(N::BoolToF16::new())),
        (PortType::U8, PortType::Bool)  => Some(Box::new(N::U8ToBool::new())),
        (PortType::U16, PortType::Bool) => Some(Box::new(N::U16ToBool::new())),
        (PortType::I8, PortType::Bool)  => Some(Box::new(N::I8ToBool::new())),
        (PortType::I16, PortType::Bool) => Some(Box::new(N::I16ToBool::new())),
        (PortType::F16, PortType::Bool) => Some(Box::new(N::F16ToBool::new())),
        (PortType::U8, PortType::Bytes)  => Some(Box::new(N::U8ToBytes::new())),
        (PortType::U16, PortType::Bytes) => Some(Box::new(N::U16ToBytes::new())),
        (PortType::I8, PortType::Bytes)  => Some(Box::new(N::I8ToBytes::new())),
        (PortType::I16, PortType::Bytes) => Some(Box::new(N::I16ToBytes::new())),
        (PortType::F16, PortType::Bytes) => Some(Box::new(N::F16ToBytes::new())),
        (PortType::U8, PortType::Json)  => Some(Box::new(N::U8ToJson::new())),
        (PortType::U16, PortType::Json) => Some(Box::new(N::U16ToJson::new())),
        (PortType::I8, PortType::Json)  => Some(Box::new(N::I8ToJson::new())),
        (PortType::I16, PortType::Json) => Some(Box::new(N::I16ToJson::new())),

        // ── 128-bit integers (cranelift I128) ───────────────────
        // Widenings from the 64-bit carriers, Display renders,
        // LE byte / decimal-string JSON wraps. → f64 mirrors
        // u64→f64's class-A treatment (defined for every input).
        (PortType::U64, PortType::U128) => Some(Box::new(W::U64ToU128::new())),
        (PortType::U64, PortType::I128) => Some(Box::new(W::U64ToI128::new())),
        (PortType::I64, PortType::I128) => Some(Box::new(W::I64ToI128::new())),
        // Totality fills: every ≤64-bit integer widens losslessly
        // into the 128-bit carriers (unsigned → both signednesses,
        // signed → i128), `bool` widens to both, and the nonzero
        // test `128 → bool` is total. All class A.
        (PortType::U8, PortType::U128)  => Some(Box::new(W::U8ToU128::new())),
        (PortType::U8, PortType::I128)  => Some(Box::new(W::U8ToI128::new())),
        (PortType::U16, PortType::U128) => Some(Box::new(W::U16ToU128::new())),
        (PortType::U16, PortType::I128) => Some(Box::new(W::U16ToI128::new())),
        (PortType::U32, PortType::U128) => Some(Box::new(W::U32ToU128::new())),
        (PortType::U32, PortType::I128) => Some(Box::new(W::U32ToI128::new())),
        (PortType::I8, PortType::I128)  => Some(Box::new(W::I8ToI128::new())),
        (PortType::I16, PortType::I128) => Some(Box::new(W::I16ToI128::new())),
        (PortType::I32, PortType::I128) => Some(Box::new(W::I32ToI128::new())),
        (PortType::Bool, PortType::U128) => Some(Box::new(W::BoolToU128::new())),
        (PortType::Bool, PortType::I128) => Some(Box::new(W::BoolToI128::new())),
        (PortType::U128, PortType::Bool) => Some(Box::new(W::U128ToBool::new())),
        (PortType::I128, PortType::Bool) => Some(Box::new(W::I128ToBool::new())),
        (PortType::U128, PortType::F64) => Some(Box::new(W::U128ToF64::new())),
        (PortType::I128, PortType::F64) => Some(Box::new(W::I128ToF64::new())),
        (PortType::U128, PortType::Str) => Some(Box::new(W::U128ToString::new())),
        (PortType::I128, PortType::Str) => Some(Box::new(W::I128ToString::new())),
        (PortType::U128, PortType::Bytes) => Some(Box::new(W::U128ToBytes::new())),
        (PortType::I128, PortType::Bytes) => Some(Box::new(W::I128ToBytes::new())),
        (PortType::U128, PortType::Json) => Some(Box::new(W::U128ToJson::new())),
        (PortType::I128, PortType::Json) => Some(Box::new(W::I128ToJson::new())),

        // ── Register views (free bitcasts) ──────────────────────
        // Any reg→reg pair heals with a zero-cost retag — the
        // materialized "views are free bitcasts" rule
        // (type_system_alignment.md §8.4 layer 2).
        (from, to)
            if crate::library::register::is_reg_port(from)
                && crate::library::register::is_reg_port(to) =>
        {
            Some(Box::new(crate::library::register::RegView::new(to)))
        }

        // ── Vector lane completion — class A (total) ────────────
        // Lossless inter-lane widenings, `→ Bytes` serialise, and
        // integer-lane `→ Json`/`→ Str`. See library/polyfill_complete.rs.
        (PortType::VecI8, PortType::VecI16) => Some(Box::new(C::VecI8ToVecI16::new())),
        (PortType::VecI8, PortType::VecI32) => Some(Box::new(C::VecI8ToVecI32::new())),
        (PortType::VecI8, PortType::VecI64) => Some(Box::new(C::VecI8ToVecI64::new())),
        (PortType::VecI8, PortType::VecF16) => Some(Box::new(C::VecI8ToVecF16::new())),
        (PortType::VecI8, PortType::VecF32) => Some(Box::new(C::VecI8ToVecF32::new())),
        (PortType::VecI8, PortType::VecF64) => Some(Box::new(C::VecI8ToVecF64::new())),
        (PortType::VecI16, PortType::VecI32) => Some(Box::new(C::VecI16ToVecI32::new())),
        (PortType::VecI16, PortType::VecI64) => Some(Box::new(C::VecI16ToVecI64::new())),
        (PortType::VecI16, PortType::VecF32) => Some(Box::new(C::VecI16ToVecF32::new())),
        (PortType::VecI16, PortType::VecF64) => Some(Box::new(C::VecI16ToVecF64::new())),
        (PortType::VecI32, PortType::VecI64) => Some(Box::new(C::VecI32ToVecI64::new())),
        (PortType::VecI32, PortType::VecF64) => Some(Box::new(C::VecI32ToVecF64::new())),
        (PortType::VecI64, PortType::VecF64) => Some(Box::new(C::VecI64ToVecF64::new())),
        (PortType::VecF16, PortType::VecF32) => Some(Box::new(C::VecF16ToVecF32::new())),
        (PortType::VecF16, PortType::VecF64) => Some(Box::new(C::VecF16ToVecF64::new())),
        (PortType::VecF32, PortType::VecF64) => Some(Box::new(C::VecF32ToVecF64::new())),
        (PortType::VecF64, PortType::Bytes) => Some(Box::new(C::VecF64ToBytes::new())),
        (PortType::VecI64, PortType::Bytes) => Some(Box::new(C::VecI64ToBytes::new())),
        (PortType::VecF16, PortType::Bytes) => Some(Box::new(C::VecF16ToBytes::new())),
        (PortType::VecI16, PortType::Bytes) => Some(Box::new(C::VecI16ToBytes::new())),
        (PortType::VecI8, PortType::Bytes) => Some(Box::new(C::VecI8ToBytes::new())),
        (PortType::VecI64, PortType::Json) => Some(Box::new(C::VecI64ToJson::new())),
        (PortType::VecI16, PortType::Json) => Some(Box::new(C::VecI16ToJson::new())),
        (PortType::VecI8, PortType::Json) => Some(Box::new(C::VecI8ToJson::new())),
        (PortType::VecI32, PortType::Str) => Some(Box::new(C::VecI32ToStr::new())),
        (PortType::VecI64, PortType::Str) => Some(Box::new(C::VecI64ToStr::new())),
        (PortType::VecI16, PortType::Str) => Some(Box::new(C::VecI16ToStr::new())),
        (PortType::VecI8, PortType::Str) => Some(Box::new(C::VecI8ToStr::new())),

        _ => None,
    }
}

/// Boundary adapter catalog. Consulted by
/// `adapt_boundary_value` when a host-injected scope value
/// crosses into a typed slot. Strictly a superset of
/// [`auto_adapter`]: every intra-graph adapter is also a
/// boundary adapter, plus all the lossy / parseable / shape-
/// checking adapters that can panic on input the assembler
/// can't statically verify.
///
/// Boundary-only adapters fall into four classes:
///
/// - **Numeric narrowings** — `U64→{U32, I64, I32, F32}`,
///   `F64→{U64, U32, I64, I32, F32}`, etc. Range-checked,
///   panic on out-of-range.
/// - **Str → X parsers** — workload-param flow (YAML string
///   interpolations, comma-split iter-values). Panic on
///   unparseable input.
/// - **Bytes → X parsers** — wrong-length panics. Numeric
///   reads expect exactly sizeof(N) bytes; Vec reads expect
///   a multiple of sizeof(element).
/// - **Json → X extractors** — shape mismatch panics
///   (`Json::Array` expected for Vec; `Json::Number` for
///   numerics; etc.).
///
/// Plus a small set of "almost-auto" adapters that the
/// assembler can't promote because they panic on non-finite
/// floats: `F64→Json`, `F32→Json`, `VecF32→Json`,
/// `VecF32→Str`.
///
/// See `polydat/docs/design/type_system.md`.
pub fn boundary_adapter(from: PortType, to: PortType) -> Option<Box<dyn PolydatNode>> {
    if let Some(adapter) = auto_adapter(from, to) {
        return Some(adapter);
    }
    use crate::library::convert::{StrToBool, StrToU64, StrToF64};
    use crate::library::polyfill as P;
    use crate::library::polyfill_narrow as N;
    use crate::library::polyfill_128 as W;
    use crate::library::polyfill_complete as C;
    match (from, to) {
        // ── Numeric narrowings + non-widening casts ─────────────
        (PortType::U64, PortType::U32) => Some(Box::new(P::U64ToU32::new())),
        (PortType::U64, PortType::I64) => Some(Box::new(P::U64ToI64::new())),
        (PortType::U64, PortType::I32) => Some(Box::new(P::U64ToI32::new())),
        (PortType::U64, PortType::F32) => Some(Box::new(P::U64ToF32::new())),
        (PortType::U32, PortType::I32) => Some(Box::new(P::U32ToI32::new())),
        (PortType::U32, PortType::F32) => Some(Box::new(P::U32ToF32::new())),
        (PortType::I64, PortType::U64) => Some(Box::new(P::I64ToU64::new())),
        (PortType::I64, PortType::U32) => Some(Box::new(P::I64ToU32::new())),
        (PortType::I64, PortType::I32) => Some(Box::new(P::I64ToI32::new())),
        (PortType::I64, PortType::F32) => Some(Box::new(P::I64ToF32::new())),
        (PortType::I32, PortType::U64) => Some(Box::new(P::I32ToU64::new())),
        (PortType::I32, PortType::U32) => Some(Box::new(P::I32ToU32::new())),
        (PortType::I32, PortType::F32) => Some(Box::new(P::I32ToF32::new())),
        (PortType::F64, PortType::U64) => Some(Box::new(P::F64ToU64Checked::new())),
        (PortType::F64, PortType::U32) => Some(Box::new(P::F64ToU32::new())),
        (PortType::F64, PortType::I64) => Some(Box::new(P::F64ToI64::new())),
        (PortType::F64, PortType::I32) => Some(Box::new(P::F64ToI32::new())),
        (PortType::F64, PortType::F32) => Some(Box::new(P::F64ToF32::new())),
        (PortType::F32, PortType::U64) => Some(Box::new(P::F32ToU64::new())),
        (PortType::F32, PortType::U32) => Some(Box::new(P::F32ToU32::new())),
        (PortType::F32, PortType::I64) => Some(Box::new(P::F32ToI64::new())),
        (PortType::F32, PortType::I32) => Some(Box::new(P::F32ToI32::new())),

        // ── Str → X parsers (boundary-only: panic on unparseable)
        (PortType::Str, PortType::Bool)   => Some(Box::new(StrToBool::new())),
        (PortType::Str, PortType::U64)    => Some(Box::new(StrToU64::new())),
        (PortType::Str, PortType::F64)    => Some(Box::new(StrToF64::new())),
        (PortType::Str, PortType::U32)    => Some(Box::new(P::StrToU32::new())),
        (PortType::Str, PortType::I64)    => Some(Box::new(P::StrToI64::new())),
        (PortType::Str, PortType::I32)    => Some(Box::new(P::StrToI32::new())),
        (PortType::Str, PortType::F32)    => Some(Box::new(P::StrToF32::new())),
        (PortType::Str, PortType::Bytes)  => Some(Box::new(P::StrToBytes::new())),
        (PortType::Str, PortType::Json)   => Some(Box::new(P::StrToJson::new())),
        (PortType::Str, PortType::VecF32) => Some(Box::new(P::StrToVecF32::new())),
        (PortType::Str, PortType::VecI32) => Some(Box::new(P::StrToVecI32::new())),

        // ── Bytes → X (length-checked, little-endian) ───────────
        (PortType::Bytes, PortType::U64)    => Some(Box::new(P::BytesToU64::new())),
        (PortType::Bytes, PortType::U32)    => Some(Box::new(P::BytesToU32::new())),
        (PortType::Bytes, PortType::I64)    => Some(Box::new(P::BytesToI64::new())),
        (PortType::Bytes, PortType::I32)    => Some(Box::new(P::BytesToI32::new())),
        (PortType::Bytes, PortType::F64)    => Some(Box::new(P::BytesToF64::new())),
        (PortType::Bytes, PortType::F32)    => Some(Box::new(P::BytesToF32::new())),
        (PortType::Bytes, PortType::Bool)   => Some(Box::new(P::BytesToBool::new())),
        (PortType::Bytes, PortType::Str)    => Some(Box::new(P::BytesToStr::new())),
        (PortType::Bytes, PortType::Json)   => Some(Box::new(P::BytesToJson::new())),
        (PortType::Bytes, PortType::VecF32) => Some(Box::new(P::BytesToVecF32::new())),
        (PortType::Bytes, PortType::VecI32) => Some(Box::new(P::BytesToVecI32::new())),

        // ── Json → X (shape-checked) ────────────────────────────
        (PortType::Json, PortType::U64)    => Some(Box::new(P::JsonToU64::new())),
        (PortType::Json, PortType::U32)    => Some(Box::new(P::JsonToU32::new())),
        (PortType::Json, PortType::I64)    => Some(Box::new(P::JsonToI64::new())),
        (PortType::Json, PortType::I32)    => Some(Box::new(P::JsonToI32::new())),
        (PortType::Json, PortType::F64)    => Some(Box::new(P::JsonToF64::new())),
        (PortType::Json, PortType::F32)    => Some(Box::new(P::JsonToF32::new())),
        (PortType::Json, PortType::Bool)   => Some(Box::new(P::JsonToBool::new())),
        (PortType::Json, PortType::Bytes)  => Some(Box::new(P::JsonToBytes::new())),
        (PortType::Json, PortType::VecF32) => Some(Box::new(P::JsonToVecF32::new())),
        (PortType::Json, PortType::VecI32) => Some(Box::new(P::JsonToVecI32::new())),

        // ── Almost-auto (panic on non-finite floats) ────────────
        (PortType::F64, PortType::Json) => Some(Box::new(P::F64ToJson::new())),
        (PortType::F32, PortType::Json) => Some(Box::new(P::F32ToJson::new())),
        (PortType::VecF32, PortType::Json) => Some(Box::new(P::VecF32ToJson::new())),
        (PortType::VecF32, PortType::Str)  => Some(Box::new(P::VecF32ToStr::new())),

        // ── Vec ↔ Vec (lossy round) ─────────────────────────────
        (PortType::VecF32, PortType::VecI32) => Some(Box::new(P::VecF32ToVecI32::new())),

        // ── Narrow cranelift widths (u8/i8/u16/i16/f16) ─────────
        // Range-checked narrowings + parsers + shape-checked
        // extractors, mirroring the u32/i32/f32 rows.
        (PortType::U64, PortType::U8)  => Some(Box::new(N::U64ToU8::new())),
        (PortType::U32, PortType::U8)  => Some(Box::new(N::U32ToU8::new())),
        (PortType::U16, PortType::U8)  => Some(Box::new(N::U16ToU8::new())),
        (PortType::I64, PortType::U8)  => Some(Box::new(N::I64ToU8::new())),
        (PortType::F64, PortType::U8)  => Some(Box::new(N::F64ToU8::new())),
        (PortType::U64, PortType::U16) => Some(Box::new(N::U64ToU16::new())),
        (PortType::U32, PortType::U16) => Some(Box::new(N::U32ToU16::new())),
        (PortType::I64, PortType::U16) => Some(Box::new(N::I64ToU16::new())),
        (PortType::F64, PortType::U16) => Some(Box::new(N::F64ToU16::new())),
        (PortType::I64, PortType::I8)  => Some(Box::new(N::I64ToI8::new())),
        (PortType::I32, PortType::I8)  => Some(Box::new(N::I32ToI8::new())),
        (PortType::U64, PortType::I8)  => Some(Box::new(N::U64ToI8::new())),
        (PortType::F64, PortType::I8)  => Some(Box::new(N::F64ToI8::new())),
        (PortType::I64, PortType::I16) => Some(Box::new(N::I64ToI16::new())),
        (PortType::I32, PortType::I16) => Some(Box::new(N::I32ToI16::new())),
        (PortType::U64, PortType::I16) => Some(Box::new(N::U64ToI16::new())),
        (PortType::F64, PortType::I16) => Some(Box::new(N::F64ToI16::new())),
        (PortType::F64, PortType::F16) => Some(Box::new(N::F64ToF16::new())),
        (PortType::F32, PortType::F16) => Some(Box::new(N::F32ToF16::new())),
        (PortType::U64, PortType::F16) => Some(Box::new(N::U64ToF16::new())),
        (PortType::Str, PortType::U8)  => Some(Box::new(N::StrToU8::new())),
        (PortType::Str, PortType::U16) => Some(Box::new(N::StrToU16::new())),
        (PortType::Str, PortType::I8)  => Some(Box::new(N::StrToI8::new())),
        (PortType::Str, PortType::I16) => Some(Box::new(N::StrToI16::new())),
        (PortType::Str, PortType::F16) => Some(Box::new(N::StrToF16::new())),
        (PortType::Bytes, PortType::U8)  => Some(Box::new(N::BytesToU8::new())),
        (PortType::Bytes, PortType::U16) => Some(Box::new(N::BytesToU16::new())),
        (PortType::Bytes, PortType::I8)  => Some(Box::new(N::BytesToI8::new())),
        (PortType::Bytes, PortType::I16) => Some(Box::new(N::BytesToI16::new())),
        (PortType::Bytes, PortType::F16) => Some(Box::new(N::BytesToF16::new())),
        (PortType::Json, PortType::U8)  => Some(Box::new(N::JsonToU8::new())),
        (PortType::Json, PortType::U16) => Some(Box::new(N::JsonToU16::new())),
        (PortType::Json, PortType::I8)  => Some(Box::new(N::JsonToI8::new())),
        (PortType::Json, PortType::I16) => Some(Box::new(N::JsonToI16::new())),
        (PortType::Json, PortType::F16) => Some(Box::new(N::JsonToF16::new())),
        // f16 → Json panics on non-finite (same as f32 → Json).
        (PortType::F16, PortType::Json) => Some(Box::new(N::F16ToJson::new())),

        // ── 128-bit integers (range-checked / parse / shape) ────
        (PortType::U128, PortType::U64) => Some(Box::new(W::U128ToU64::new())),
        (PortType::I128, PortType::I64) => Some(Box::new(W::I128ToI64::new())),
        (PortType::I64, PortType::U128) => Some(Box::new(W::I64ToU128::new())),
        (PortType::U128, PortType::I128) => Some(Box::new(W::U128ToI128::new())),
        (PortType::I128, PortType::U128) => Some(Box::new(W::I128ToU128::new())),
        (PortType::F64, PortType::U128) => Some(Box::new(W::F64ToU128::new())),
        (PortType::F64, PortType::I128) => Some(Box::new(W::F64ToI128::new())),
        (PortType::Str, PortType::U128) => Some(Box::new(W::StrToU128::new())),
        (PortType::Str, PortType::I128) => Some(Box::new(W::StrToI128::new())),
        (PortType::Bytes, PortType::U128) => Some(Box::new(W::BytesToU128::new())),
        (PortType::Bytes, PortType::I128) => Some(Box::new(W::BytesToI128::new())),
        (PortType::Json, PortType::U128) => Some(Box::new(W::JsonToU128::new())),
        (PortType::Json, PortType::I128) => Some(Box::new(W::JsonToI128::new())),

        // ── Scalar matrix completion (library/polyfill_complete.rs) ──
        // Every remaining scalar→scalar narrowing / cross-sign /
        // float→int / int→narrow-float cell, so the 14×14 scalar
        // block has no `·`. All class B (range-checked, can panic).
        (PortType::U8, PortType::I8) => Some(Box::new(C::U8ToI8::new())),
        (PortType::I8, PortType::U8) => Some(Box::new(C::I8ToU8::new())),
        (PortType::I8, PortType::U16) => Some(Box::new(C::I8ToU16::new())),
        (PortType::I8, PortType::U32) => Some(Box::new(C::I8ToU32::new())),
        (PortType::I8, PortType::U64) => Some(Box::new(C::I8ToU64::new())),
        (PortType::I8, PortType::U128) => Some(Box::new(C::I8ToU128::new())),
        (PortType::U16, PortType::I8) => Some(Box::new(C::U16ToI8::new())),
        (PortType::U16, PortType::I16) => Some(Box::new(C::U16ToI16::new())),
        (PortType::U16, PortType::F16) => Some(Box::new(C::U16ToF16::new())),
        (PortType::I16, PortType::U8) => Some(Box::new(C::I16ToU8::new())),
        (PortType::I16, PortType::I8) => Some(Box::new(C::I16ToI8::new())),
        (PortType::I16, PortType::U16) => Some(Box::new(C::I16ToU16::new())),
        (PortType::I16, PortType::F16) => Some(Box::new(C::I16ToF16::new())),
        (PortType::I16, PortType::U32) => Some(Box::new(C::I16ToU32::new())),
        (PortType::I16, PortType::U64) => Some(Box::new(C::I16ToU64::new())),
        (PortType::I16, PortType::U128) => Some(Box::new(C::I16ToU128::new())),
        (PortType::U32, PortType::I8) => Some(Box::new(C::U32ToI8::new())),
        (PortType::U32, PortType::I16) => Some(Box::new(C::U32ToI16::new())),
        (PortType::U32, PortType::F16) => Some(Box::new(C::U32ToF16::new())),
        (PortType::I32, PortType::U8) => Some(Box::new(C::I32ToU8::new())),
        (PortType::I32, PortType::U16) => Some(Box::new(C::I32ToU16::new())),
        (PortType::I32, PortType::F16) => Some(Box::new(C::I32ToF16::new())),
        (PortType::I32, PortType::U128) => Some(Box::new(C::I32ToU128::new())),
        (PortType::F16, PortType::U8) => Some(Box::new(C::F16ToU8::new())),
        (PortType::F16, PortType::I8) => Some(Box::new(C::F16ToI8::new())),
        (PortType::F16, PortType::U16) => Some(Box::new(C::F16ToU16::new())),
        (PortType::F16, PortType::I16) => Some(Box::new(C::F16ToI16::new())),
        (PortType::F16, PortType::U32) => Some(Box::new(C::F16ToU32::new())),
        (PortType::F16, PortType::I32) => Some(Box::new(C::F16ToI32::new())),
        (PortType::F16, PortType::U64) => Some(Box::new(C::F16ToU64::new())),
        (PortType::F16, PortType::I64) => Some(Box::new(C::F16ToI64::new())),
        (PortType::F16, PortType::U128) => Some(Box::new(C::F16ToU128::new())),
        (PortType::F16, PortType::I128) => Some(Box::new(C::F16ToI128::new())),
        (PortType::F32, PortType::U8) => Some(Box::new(C::F32ToU8::new())),
        (PortType::F32, PortType::I8) => Some(Box::new(C::F32ToI8::new())),
        (PortType::F32, PortType::U16) => Some(Box::new(C::F32ToU16::new())),
        (PortType::F32, PortType::I16) => Some(Box::new(C::F32ToI16::new())),
        (PortType::F32, PortType::U128) => Some(Box::new(C::F32ToU128::new())),
        (PortType::F32, PortType::I128) => Some(Box::new(C::F32ToI128::new())),
        (PortType::I64, PortType::F16) => Some(Box::new(C::I64ToF16::new())),
        (PortType::U128, PortType::U8) => Some(Box::new(C::U128ToU8::new())),
        (PortType::U128, PortType::I8) => Some(Box::new(C::U128ToI8::new())),
        (PortType::U128, PortType::U16) => Some(Box::new(C::U128ToU16::new())),
        (PortType::U128, PortType::I16) => Some(Box::new(C::U128ToI16::new())),
        (PortType::U128, PortType::F16) => Some(Box::new(C::U128ToF16::new())),
        (PortType::U128, PortType::U32) => Some(Box::new(C::U128ToU32::new())),
        (PortType::U128, PortType::I32) => Some(Box::new(C::U128ToI32::new())),
        (PortType::U128, PortType::F32) => Some(Box::new(C::U128ToF32::new())),
        (PortType::U128, PortType::I64) => Some(Box::new(C::U128ToI64::new())),
        (PortType::I128, PortType::U8) => Some(Box::new(C::I128ToU8::new())),
        (PortType::I128, PortType::I8) => Some(Box::new(C::I128ToI8::new())),
        (PortType::I128, PortType::U16) => Some(Box::new(C::I128ToU16::new())),
        (PortType::I128, PortType::I16) => Some(Box::new(C::I128ToI16::new())),
        (PortType::I128, PortType::F16) => Some(Box::new(C::I128ToF16::new())),
        (PortType::I128, PortType::U32) => Some(Box::new(C::I128ToU32::new())),
        (PortType::I128, PortType::I32) => Some(Box::new(C::I128ToI32::new())),
        (PortType::I128, PortType::F32) => Some(Box::new(C::I128ToF32::new())),
        (PortType::I128, PortType::U64) => Some(Box::new(C::I128ToU64::new())),

        // ── Vector lane completion — class B (lossy / checked) ──
        // Inter-lane narrowing + float→int, Bytes/Json/Str decode &
        // parse, float-lane → Json/Str (non-finite panics).
        (PortType::VecI16, PortType::VecI8) => Some(Box::new(C::VecI16ToVecI8::new())),
        (PortType::VecI16, PortType::VecF16) => Some(Box::new(C::VecI16ToVecF16::new())),
        (PortType::VecI32, PortType::VecI8) => Some(Box::new(C::VecI32ToVecI8::new())),
        (PortType::VecI32, PortType::VecI16) => Some(Box::new(C::VecI32ToVecI16::new())),
        (PortType::VecI32, PortType::VecF16) => Some(Box::new(C::VecI32ToVecF16::new())),
        (PortType::VecI64, PortType::VecI8) => Some(Box::new(C::VecI64ToVecI8::new())),
        (PortType::VecI64, PortType::VecI16) => Some(Box::new(C::VecI64ToVecI16::new())),
        (PortType::VecI64, PortType::VecI32) => Some(Box::new(C::VecI64ToVecI32::new())),
        (PortType::VecI64, PortType::VecF16) => Some(Box::new(C::VecI64ToVecF16::new())),
        (PortType::VecI64, PortType::VecF32) => Some(Box::new(C::VecI64ToVecF32::new())),
        (PortType::VecF16, PortType::VecI8) => Some(Box::new(C::VecF16ToVecI8::new())),
        (PortType::VecF16, PortType::VecI16) => Some(Box::new(C::VecF16ToVecI16::new())),
        (PortType::VecF16, PortType::VecI32) => Some(Box::new(C::VecF16ToVecI32::new())),
        (PortType::VecF16, PortType::VecI64) => Some(Box::new(C::VecF16ToVecI64::new())),
        (PortType::VecF32, PortType::VecI8) => Some(Box::new(C::VecF32ToVecI8::new())),
        (PortType::VecF32, PortType::VecI16) => Some(Box::new(C::VecF32ToVecI16::new())),
        (PortType::VecF32, PortType::VecI64) => Some(Box::new(C::VecF32ToVecI64::new())),
        (PortType::VecF32, PortType::VecF16) => Some(Box::new(C::VecF32ToVecF16::new())),
        (PortType::VecF64, PortType::VecI8) => Some(Box::new(C::VecF64ToVecI8::new())),
        (PortType::VecF64, PortType::VecI16) => Some(Box::new(C::VecF64ToVecI16::new())),
        (PortType::VecF64, PortType::VecI32) => Some(Box::new(C::VecF64ToVecI32::new())),
        (PortType::VecF64, PortType::VecI64) => Some(Box::new(C::VecF64ToVecI64::new())),
        (PortType::VecF64, PortType::VecF16) => Some(Box::new(C::VecF64ToVecF16::new())),
        (PortType::VecF64, PortType::VecF32) => Some(Box::new(C::VecF64ToVecF32::new())),
        (PortType::Bytes, PortType::VecF64) => Some(Box::new(C::BytesToVecF64::new())),
        (PortType::Bytes, PortType::VecI64) => Some(Box::new(C::BytesToVecI64::new())),
        (PortType::Bytes, PortType::VecF16) => Some(Box::new(C::BytesToVecF16::new())),
        (PortType::Bytes, PortType::VecI16) => Some(Box::new(C::BytesToVecI16::new())),
        (PortType::Bytes, PortType::VecI8) => Some(Box::new(C::BytesToVecI8::new())),
        (PortType::VecF64, PortType::Json) => Some(Box::new(C::VecF64ToJson::new())),
        (PortType::VecF16, PortType::Json) => Some(Box::new(C::VecF16ToJson::new())),
        (PortType::Json, PortType::VecF64) => Some(Box::new(C::JsonToVecF64::new())),
        (PortType::Json, PortType::VecI64) => Some(Box::new(C::JsonToVecI64::new())),
        (PortType::Json, PortType::VecF16) => Some(Box::new(C::JsonToVecF16::new())),
        (PortType::Json, PortType::VecI16) => Some(Box::new(C::JsonToVecI16::new())),
        (PortType::Json, PortType::VecI8) => Some(Box::new(C::JsonToVecI8::new())),
        (PortType::VecF64, PortType::Str) => Some(Box::new(C::VecF64ToStr::new())),
        (PortType::VecF16, PortType::Str) => Some(Box::new(C::VecF16ToStr::new())),
        (PortType::Str, PortType::VecF64) => Some(Box::new(C::StrToVecF64::new())),
        (PortType::Str, PortType::VecI64) => Some(Box::new(C::StrToVecI64::new())),
        (PortType::Str, PortType::VecF16) => Some(Box::new(C::StrToVecF16::new())),
        (PortType::Str, PortType::VecI16) => Some(Box::new(C::StrToVecI16::new())),
        (PortType::Str, PortType::VecI8) => Some(Box::new(C::StrToVecI8::new())),

        _ => None,
    }
}
