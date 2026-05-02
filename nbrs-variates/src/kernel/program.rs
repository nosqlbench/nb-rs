// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GkProgram: the immutable compiled DAG shared across all fibers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{GkNode, Value};
use super::{WireSource, InputDef};
use super::engines::{GkState, RawState, ProvScanState, EngineCore};

/// Evaluation lifecycle classification used by the init-binding
/// contract (see [SRD 11 §"Three Evaluation Lifecycles"](../../../../docs/sysref/11_gk_evaluation.md)).
///
/// The variants are *ordered* — `Dynamic > ScopeInit > CompileConst`
/// — so propagation along wires is a `max()` operation: a node's
/// lifecycle is the most-dynamic of its own seed and every upstream
/// node's lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EvalLifecycle {
    /// Foldable at GK compile time. No dependency on extern slots
    /// or cycle inputs.
    CompileConst,
    /// Foldable at scope activation, after `bind_outer_scope`
    /// populates iteration externs. Effectively-const for the
    /// duration of one activation.
    ScopeInit,
    /// Re-evaluated on each pull at execution time. Reaches a
    /// graph input (cycle / capture port) or a non-deterministic
    /// source.
    Dynamic,
}

/// Build a diagnostic phrase pinpointing the first wire on a
/// dynamic init-binding's upstream chain that broke the
/// effectively-const contract. Walks one step deep into the
/// node's wiring; for transitive cases the message points at the
/// nearest dynamic source. Best-effort — an unresolvable wire
/// returns a generic message.
fn first_dynamic_wire(
    nodes: &[Box<dyn GkNode>],
    wiring: &[Vec<WireSource>],
    lifecycle: &[EvalLifecycle],
    input_defs: &[InputDef],
    node_idx: usize,
) -> String {
    use crate::kernel::InputKind;
    let owner = nodes[node_idx].meta().name.clone();
    for source in &wiring[node_idx] {
        match source {
            WireSource::Input(idx) => {
                let def = match input_defs.get(*idx) {
                    Some(d) => d,
                    None => continue,
                };
                match def.kind {
                    InputKind::Coordinate => {
                        return format!(
                            "wire on node '{owner}' reaches coordinate input '{}' \
                             (dynamic; changes every cycle)",
                            def.name);
                    }
                    InputKind::CapturePort => {
                        return format!(
                            "wire on node '{owner}' reaches capture port '{}' \
                             (dynamic; mutated by op execution)",
                            def.name);
                    }
                    InputKind::IterationExtern => {} // not the offender
                }
            }
            WireSource::NodeOutput(upstream, _) => {
                if lifecycle[*upstream] == EvalLifecycle::Dynamic {
                    let upstream_name = nodes[*upstream].meta().name.clone();
                    // Detect non-deterministic seed nodes.
                    if wiring[*upstream].is_empty()
                        && (upstream_name == "counter"
                            || upstream_name == "current_epoch_millis"
                            || upstream_name == "session_start_millis"
                            || upstream_name == "elapsed_millis"
                            || upstream_name == "thread_id")
                    {
                        return format!(
                            "wire on node '{owner}' reaches non-deterministic \
                             source '{upstream_name}' (dynamic by construction)");
                    }
                    return format!(
                        "wire on node '{owner}' reaches dynamic node \
                         '{upstream_name}' upstream");
                }
            }
        }
    }
    format!("node '{owner}' is dynamic but the offending wire could not be isolated")
}

/// The immutable compiled DAG. Shared across fibers via `Arc`.
pub struct GkProgram {
    /// Node instances in topological order.
    pub(crate) nodes: Vec<Box<dyn GkNode>>,
    /// For each node, the wiring of its input ports.
    pub(crate) wiring: Vec<Vec<WireSource>>,
    /// All input definitions (coordinates first, then captures).
    input_defs: Vec<InputDef>,
    /// Original source text that produced this program. Arc-shared
    /// so multiple references (diagnostics, describe, debugger) don't
    /// duplicate the string. Empty if constructed programmatically.
    source: Arc<String>,
    /// Diagnostic context describing where this program came from
    /// (e.g., "workload.yaml bindings", "phase rampup (pname=label-1)").
    /// Required on all construction paths — no silent empty contexts.
    context: Arc<String>,
    /// How many of the inputs are coordinate inputs (set via set_inputs(&[u64])).
    /// Inputs at indices [0..coord_count) are coordinates.
    /// Inputs at indices [coord_count..) are capture inputs.
    coord_count: usize,
    /// Map from output variate name to `(node_index, output_port_index)`.
    pub(crate) output_map: HashMap<String, (usize, usize)>,
    /// Outputs in declaration order: (name, node_index, port_index).
    /// Stable ordering for positional access.
    output_list: Vec<(String, usize, usize)>,
    /// Per-node input provenance bitmask. Bit i is set if the node
    /// transitively depends on graph input i.
    pub(crate) input_provenance: Vec<u64>,
    /// Per-input dependent node lists. For each input, the list of
    /// node indices that transitively depend on it.
    input_dependents: Vec<Vec<usize>>,
    /// Output binding modifiers: `shared` or `final`.
    /// Only populated for outputs that have a modifier; absent = default.
    output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
    /// Names exposed by this program *only* to pass them through
    /// the scope chain — not because the scope's own bindings or
    /// specs reference them. Set by intermediate-scope synthesis
    /// (for_each / for_combinations / do-loop) when auto-cascading
    /// workload params or other inherited values: an `extern` is
    /// declared so `bind_outer_scope` can wire the value, but the
    /// scope itself doesn't *own* the name. Display layers
    /// (scenario tree pre-map, TUI per-scope listing) use this
    /// to distinguish "names defined here" from "names visible
    /// here through inheritance."
    inherited_outputs: std::collections::HashSet<String>,
    /// Source schemas declared in the GK program. The runtime queries
    /// these to discover data sources and their extents.
    cursor_schemas: Vec<crate::source::SourceSchema>,
    /// Names declared with the `init` keyword in the source. Subject
    /// to the init-binding contract (SRD 11 §"Init Binding Contract"):
    /// every name listed here must reach exactly one effectively-const
    /// value at scope-init time. Plan A (compile-time) and Plan B
    /// (scope-activation) checks both consult this set.
    pub(crate) init_outputs: std::collections::HashSet<String>,
}

unsafe impl Send for GkProgram {}
unsafe impl Sync for GkProgram {}

impl std::fmt::Debug for GkProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GkProgram")
            .field("nodes", &self.nodes.len())
            .field("inputs", &self.input_names())
            .field("coord_count", &self.coord_count)
            .finish()
    }
}

impl GkProgram {
    /// Create a program from pre-validated, topologically-sorted components.
    /// All inputs are treated as coordinates.
    #[allow(dead_code)]
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
        let input_provenance = Self::compute_provenance(&nodes, &wiring);
        let input_dependents = Self::compute_dependents(&input_provenance, input_defs.len());
        // No declaration order available — fall back to sorted by node index
        let fallback_order: Vec<String> = {
            let mut v: Vec<(String, usize, usize)> = output_map.iter()
                .map(|(n, &(ni, pi))| (n.clone(), ni, pi)).collect();
            v.sort_by_key(|(_, ni, pi)| (*ni, *pi));
            v.into_iter().map(|(n, _, _)| n).collect()
        };
        let output_list = Self::build_output_list(&fallback_order, &output_map);
        Self {
            nodes, wiring, input_defs, coord_count, output_map, output_list,
            input_provenance, input_dependents,
            source: Arc::new(source.to_string()),
            context: Arc::new(context.to_string()),
            output_modifiers: HashMap::new(),
            inherited_outputs: std::collections::HashSet::new(),
            cursor_schemas: Vec::new(),
            init_outputs: std::collections::HashSet::new(),
        }
    }

    /// Create a program with explicit input definitions and output ordering.
    #[allow(dead_code)]
    pub(crate) fn with_inputs(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        source: &str,
        context: &str,
    ) -> Self {
        let input_provenance = Self::compute_provenance(&nodes, &wiring);
        let input_dependents = Self::compute_dependents(&input_provenance, input_defs.len());
        let output_list = Self::build_output_list(&output_order, &output_map);
        Self {
            nodes, wiring, input_defs, coord_count, output_map, output_list,
            input_provenance, input_dependents,
            source: Arc::new(source.to_string()),
            context: Arc::new(context.to_string()),
            output_modifiers: HashMap::new(),
            inherited_outputs: std::collections::HashSet::new(),
            cursor_schemas: Vec::new(),
            init_outputs: std::collections::HashSet::new(),
        }
    }

    /// Mark a binding as declared with the `init` keyword. The
    /// init-binding contract (SRD 11) is checked against this set.
    pub(crate) fn mark_init_output(&mut self, name: &str) {
        self.init_outputs.insert(name.to_string());
    }

    /// Read the set of names declared with the `init` keyword.
    pub fn init_outputs(&self) -> &std::collections::HashSet<String> {
        &self.init_outputs
    }

    /// Read the input classification for slot `idx`.
    pub fn input_kind(&self, idx: usize) -> Option<crate::kernel::InputKind> {
        self.input_defs.get(idx).map(|d| d.kind)
    }

    /// Look up `name` in the output map, returning `(node_idx, port_idx)`.
    /// Public surface for the scope-init pass and other consumers
    /// outside the kernel module.
    pub fn output_map_lookup(&self, name: &str) -> Option<&(usize, usize)> {
        self.output_map.get(name)
    }

    /// Set the binding modifier for a named output.
    pub(crate) fn set_output_modifier(&mut self, name: &str, modifier: crate::dsl::ast::BindingModifier) {
        if modifier != crate::dsl::ast::BindingModifier::None {
            self.output_modifiers.insert(name.to_string(), modifier);
        }
    }

    /// Query the binding modifier for a named output.
    pub fn output_modifier(&self, name: &str) -> crate::dsl::ast::BindingModifier {
        self.output_modifiers.get(name).copied()
            .unwrap_or(crate::dsl::ast::BindingModifier::None)
    }

    /// Return all output names that have the `shared` modifier.
    pub fn shared_outputs(&self) -> Vec<&str> {
        self.output_modifiers.iter()
            .filter(|(_, m)| **m == crate::dsl::ast::BindingModifier::Shared)
            .map(|(n, _)| n.as_str())
            .collect()
    }

    /// Mark `name` as an inherited (cascade-propagated) output —
    /// declared on this program only to flow the value through
    /// to descendants via `bind_outer_scope`, not because this
    /// scope's own bindings or specs reference it.
    pub fn mark_inherited(&mut self, name: &str) {
        self.inherited_outputs.insert(name.to_string());
    }

    /// Is `name` an inherited (cascade-propagated) output? See
    /// [`Self::mark_inherited`].
    pub fn is_inherited(&self, name: &str) -> bool {
        self.inherited_outputs.contains(name)
    }

    /// Return only the outputs *owned* by this program — names
    /// the scope's own bindings, externs, or specs declared,
    /// excluding inherited cascade-propagation outputs. Used by
    /// the scenario tree pre-map and TUI to render per-scope
    /// "what's defined here" without listing every inherited
    /// name. Output order matches `output_names`.
    pub fn own_output_names(&self) -> Vec<&str> {
        self.output_names().into_iter()
            .filter(|name| !self.inherited_outputs.contains(*name))
            .collect()
    }

    /// Return all output names that have the `final` modifier.
    pub fn final_outputs(&self) -> Vec<&str> {
        self.output_modifiers.iter()
            .filter(|(_, m)| **m == crate::dsl::ast::BindingModifier::Final)
            .map(|(n, _)| n.as_str())
            .collect()
    }

    /// The original source text that produced this program.
    pub fn source(&self) -> &str { &self.source }

    /// Diagnostic context (e.g., "workload.yaml bindings").
    pub fn context(&self) -> &str { &self.context }

    /// Source schemas declared in this program. The runtime queries
    /// these to discover data sources, their extents, and projections.
    pub fn cursor_schemas(&self) -> &[crate::source::SourceSchema] {
        &self.cursor_schemas
    }

    /// Set source schemas (called by the compiler after processing source declarations).
    pub(crate) fn set_cursor_schemas(&mut self, schemas: Vec<crate::source::SourceSchema>) {
        self.cursor_schemas = schemas;
    }

    /// Build ordered output list from declaration order and the output map.
    fn build_output_list(
        output_order: &[String],
        output_map: &HashMap<String, (usize, usize)>,
    ) -> Vec<(String, usize, usize)> {
        // Use declaration order from the assembler
        let mut list: Vec<(String, usize, usize)> = output_order.iter()
            .filter_map(|name| {
                output_map.get(name).map(|&(ni, pi)| (name.clone(), ni, pi))
            })
            .collect();
        // Add any outputs not in the declaration order (shouldn't happen,
        // but defensive against manual assembler use)
        for (name, &(ni, pi)) in output_map {
            if !output_order.contains(name) {
                list.push((name.clone(), ni, pi));
            }
        }
        list
    }

    /// Invert provenance into per-input dependent node lists.
    pub(crate) fn compute_dependents(provenance: &[u64], num_inputs: usize) -> Vec<Vec<usize>> {
        let mut deps = vec![Vec::new(); num_inputs];
        for (node_idx, &prov) in provenance.iter().enumerate() {
            for input_idx in 0..num_inputs {
                if prov & (1u64 << input_idx) != 0 {
                    deps[input_idx].push(node_idx);
                }
            }
        }
        deps
    }

    /// Compute per-node input provenance bitmask from the DAG wiring.
    pub(crate) fn compute_provenance(
        nodes: &[Box<dyn GkNode>],
        wiring: &[Vec<WireSource>],
    ) -> Vec<u64> {
        let n = nodes.len();
        let mut prov = vec![0u64; n];
        for i in 0..n {
            for source in &wiring[i] {
                match source {
                    WireSource::Input(idx) => {
                        prov[i] |= 1u64 << idx;
                    }
                    WireSource::NodeOutput(upstream, _) => {
                        prov[i] |= prov[*upstream];
                    }
                }
            }
        }
        prov
    }

    /// Build an EngineCore (shared by all state constructors).
    fn build_engine_core(&self) -> EngineCore {
        let buffers: Vec<Vec<Value>> = self.nodes
            .iter().map(|n| vec![Value::None; n.meta().outs.len()]).collect();
        let node_count = self.nodes.len();
        let inputs: Vec<Value> = self.input_defs.iter()
            .map(|d| d.default.clone()).collect();
        let input_defaults = inputs.clone();
        let max_inputs = self.wiring.iter().map(|w| w.len()).max().unwrap_or(0);
        let input_count = inputs.len();
        EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs, input_defaults,
            shared_cells: vec![None; input_count],
            input_scratch: vec![Value::None; max_inputs],
        }
    }

    /// Create a new evaluation state for this program.
    pub fn create_state(&self) -> GkState {
        let buffers: Vec<Vec<Value>> = self.nodes
            .iter()
            .map(|n| vec![Value::None; n.meta().outs.len()])
            .collect();
        let node_count = self.nodes.len();

        let inputs: Vec<Value> = self.input_defs.iter()
            .map(|d| d.default.clone()).collect();
        let input_defaults = inputs.clone();

        let max_inputs = self.wiring.iter()
            .map(|w| w.len())
            .max()
            .unwrap_or(0);

        let nondeterministic_nodes: Vec<usize> = self.nodes.iter().enumerate()
            .filter(|(i, node)| self.wiring[*i].is_empty() && node.meta().ins.is_empty())
            .map(|(i, _)| i)
            .collect();

        let input_count = inputs.len();
        let core = EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs, input_defaults,
            shared_cells: vec![None; input_count],
            input_scratch: vec![Value::None; max_inputs],
        };

        GkState::from_parts(core, self.input_dependents.clone(), nondeterministic_nodes)
    }

    /// Create a raw state (no provenance). For benchmarking.
    pub fn create_raw_state(&self) -> RawState {
        RawState { core: self.build_engine_core() }
    }

    /// Create the provenance-scan engine state (for benchmarking).
    pub fn create_provscan_state(&self) -> ProvScanState {
        let core = self.build_engine_core();
        let nondeterministic_nodes: Vec<usize> = self.nodes.iter().enumerate()
            .filter(|(i, node)| self.wiring[*i].is_empty() && node.meta().ins.is_empty())
            .map(|(i, _)| i)
            .collect();
        ProvScanState::from_parts(core, self.input_provenance.clone(), nondeterministic_nodes)
    }

    /// Return the names of all inputs.
    pub fn input_names(&self) -> Vec<String> {
        self.input_defs.iter().map(|d| d.name.clone()).collect()
    }

    /// Return the number of coordinate inputs.
    pub fn coord_count(&self) -> usize {
        self.coord_count
    }

    /// Find an input by name. Returns its index.
    pub fn find_input(&self, name: &str) -> Option<usize> {
        self.input_defs.iter().position(|d| d.name == name)
    }

    /// Lookup the declared port type of a named input.
    /// Returns `None` if the name isn't an input of this program.
    pub fn input_port_type(&self, name: &str) -> Option<crate::node::PortType> {
        self.input_defs.iter().find(|d| d.name == name).map(|d| d.port_type)
    }

    /// Number of declared outputs.
    pub fn output_count(&self) -> usize {
        self.output_list.len()
    }

    /// Output name at index (declaration order).
    pub fn output_name(&self, idx: usize) -> &str {
        &self.output_list[idx].0
    }

    /// Return all output names in declaration order.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_list.iter().map(|(n, _, _)| n.as_str()).collect()
    }

    /// Resolve an output name to its (node_index, port_index).
    pub fn resolve_output(&self, name: &str) -> Option<(usize, usize)> {
        self.output_map.get(name).copied()
    }

    /// Resolve an output index to its (node_index, port_index).
    pub fn resolve_output_by_index(&self, idx: usize) -> (usize, usize) {
        let (_, ni, pi) = &self.output_list[idx];
        (*ni, *pi)
    }

    /// Find the output index for a name (for building memoized getters).
    pub fn output_index(&self, name: &str) -> Option<usize> {
        self.output_list.iter().position(|(n, _, _)| n == name)
    }

    /// Get the provenance bitmask for a node by index.
    pub fn input_provenance_for(&self, node_idx: usize) -> u64 {
        self.input_provenance.get(node_idx).copied().unwrap_or(0)
    }

    /// Number of nodes in the program.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Total wire count (sum of all node input edges).
    pub fn wire_count(&self) -> usize {
        self.wiring.iter().map(|w| w.len()).sum()
    }

    /// Average in-degree (wires per node).
    pub fn avg_degree(&self) -> f64 {
        let n = self.nodes.len();
        if n == 0 { return 0.0; }
        self.wire_count() as f64 / n as f64
    }

    /// Access a node's metadata by index.
    pub fn node_meta(&self, idx: usize) -> &crate::node::NodeMeta {
        self.nodes[idx].meta()
    }

    /// Access the wiring for a node by index.
    /// Returns the list of `WireSource`s feeding this node's inputs.
    pub fn node_wiring(&self, idx: usize) -> &[super::WireSource] {
        &self.wiring[idx]
    }

    /// Probe the compile level of a node by index.
    pub fn node_compile_level(&self, idx: usize) -> crate::node::CompileLevel {
        crate::node::compile_level_of(self.nodes[idx].as_ref())
    }

    /// Probe the compile level of the last node.
    pub fn last_node_compile_level(&self) -> crate::node::CompileLevel {
        if self.nodes.is_empty() {
            return crate::node::CompileLevel::Phase1;
        }
        self.node_compile_level(self.nodes.len() - 1)
    }

    /// Fold init-time constant nodes.
    ///
    /// Returns `Err` only when the init-binding contract (SRD 11
    /// §"Init Binding Contract" Plan A) is violated; non-fatal
    /// warnings (config-wire / non-determinism / implicit coercion)
    /// continue to be log-emitted and don't surface here.
    pub fn fold_init_constants(&mut self) -> Result<usize, String> {
        self.fold_init_constants_impl(None, false)
    }

    /// Fold init-time constants, emitting diagnostic events to the log.
    /// Returns `Err` for init-binding contract violations (Plan A).
    pub fn fold_init_constants_with_log(&mut self, log: Option<&mut crate::dsl::events::CompileEventLog>) -> Result<usize, String> {
        self.fold_init_constants_impl(log, false)
    }

    /// Fold init-time constants with strict mode.
    pub fn fold_init_constants_strict(
        &mut self,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
        strict: bool,
    ) -> Result<usize, String> {
        self.fold_init_constants_impl(log, strict)
    }

    fn fold_init_constants_impl(
        &mut self,
        mut log: Option<&mut crate::dsl::events::CompileEventLog>,
        strict: bool,
    ) -> Result<usize, String> {
        use crate::nodes::identity::{ConstU64, ConstStr, ConstHandle};
        use crate::nodes::fixed::ConstF64;
        use crate::node::Value;

        let n = self.nodes.len();
        if n == 0 { return Ok(0); }

        // Phase 1: Classify each node by its evaluation lifecycle.
        // Per SRD 11 §"Three Evaluation Lifecycles": every node is
        // CompileConst, ScopeInit, or Dynamic; the three are
        // ordered (Dynamic dominates ScopeInit dominates
        // CompileConst) and `max()`-propagate downstream.
        //
        // CompileConst: foldable now (no extern / cycle dependencies).
        // ScopeInit:    not foldable now, but will be at scope
        //               activation (depends on iteration externs).
        // Dynamic:      depends on cycle inputs, capture ports, or
        //               non-deterministic sources.
        use crate::kernel::InputKind;
        let mut lifecycle: Vec<EvalLifecycle> = vec![EvalLifecycle::CompileConst; n];

        for (i, wiring) in self.wiring.iter().enumerate() {
            for source in wiring {
                match source {
                    WireSource::Input(idx) => {
                        let kind = self.input_defs.get(*idx).map(|d| d.kind)
                            .unwrap_or(InputKind::Coordinate);
                        let lc = match kind {
                            InputKind::IterationExtern => EvalLifecycle::ScopeInit,
                            InputKind::Coordinate | InputKind::CapturePort => EvalLifecycle::Dynamic,
                        };
                        if lc > lifecycle[i] {
                            lifecycle[i] = lc;
                        }
                    }
                    WireSource::NodeOutput(_, _) => {}
                }
            }
            if wiring.is_empty() {
                let name = &self.nodes[i].meta().name;
                if name == "current_epoch_millis" || name == "session_start_millis"
                    || name == "elapsed_millis" || name == "counter"
                    || name == "thread_id"
                {
                    lifecycle[i] = EvalLifecycle::Dynamic;
                }
            }
        }

        // Propagate: a node's lifecycle is the max of its own seed
        // and every upstream NodeOutput's lifecycle. Iterate to
        // fixed point.
        let mut changed = true;
        while changed {
            changed = false;
            for i in 0..n {
                for source in &self.wiring[i] {
                    if let WireSource::NodeOutput(upstream, _) = source
                        && lifecycle[*upstream] > lifecycle[i]
                    {
                        lifecycle[i] = lifecycle[*upstream];
                        changed = true;
                    }
                }
            }
        }

        // is_init is the compile-const subset. Subsequent fold
        // phases below only operate on CompileConst nodes; ScopeInit
        // nodes are deferred to the scope-activation pass.
        let mut is_init: Vec<bool> = lifecycle.iter()
            .map(|lc| *lc == EvalLifecycle::CompileConst)
            .collect();

        // ─── Plan A: Init-Binding Contract (compile-time) ──────────
        //
        // SRD 11 §"Init Binding Contract": every binding declared
        // `init` must reach a single effectively-const value at
        // scope-init time. At compile time, that means: the
        // binding's owning node must classify as CompileConst or
        // ScopeInit — never Dynamic.
        //
        // A Dynamic classification on an init binding is a hard
        // structural error. The diagnostic names the binding and
        // the offending wire. There is no soft fall-through.
        if !self.init_outputs.is_empty() {
            for init_name in &self.init_outputs {
                let Some((node_idx, _)) = self.output_map.get(init_name) else { continue };
                if lifecycle[*node_idx] == EvalLifecycle::Dynamic {
                    let offending = first_dynamic_wire(&self.nodes, &self.wiring, &lifecycle, &self.input_defs, *node_idx);
                    return Err(format!(
                        "init binding '{init_name}' violates the init contract: \
                         {offending} \
                         (init bindings must be effectively-const at scope-init time \
                         per SRD 11 §\"Init Binding Contract\")"
                    ));
                }
            }
        }
        // ─────────────────────────────────────────────────────────────

        // Wire cost check
        for i in 0..n {
            let wire_inputs = self.nodes[i].meta().wire_inputs();
            for (port_idx, wire_source) in self.wiring[i].iter().enumerate() {
                if port_idx >= wire_inputs.len() { break; }
                if wire_inputs[port_idx].wire_cost != crate::node::WireCost::Config {
                    continue;
                }
                let source_is_cycle = match wire_source {
                    WireSource::Input(_) => true,
                    WireSource::NodeOutput(src_idx, _) => !is_init[*src_idx],
                };
                if source_is_cycle {
                    let node_name = &self.nodes[i].meta().name;
                    let port_name = &wire_inputs[port_idx].name;
                    if strict {
                        return Err(format!(
                            "strict mode: config wire '{port_name}' on node '{node_name}' \
                             is connected to a cycle-time source."
                        ));
                    }
                    crate::audit::warn(&format!(
                        "config wire '{port_name}' on node '{node_name}' is connected to a \
                         cycle-time source."
                    ));
                    if let Some(ref mut log) = log {
                        log.push(crate::dsl::events::CompileEvent::ConfigWireCycleWarning {
                            node: node_name.clone(),
                            port: port_name.clone(),
                        });
                    }
                }
            }
        }

        // Non-deterministic node check
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            let is_nondeterministic = self.wiring[i].is_empty() && !is_init[i]
                && !name.starts_with("__");
            if is_nondeterministic {
                let msg = format!(
                    "non-deterministic node '{name}' used without explicit acknowledgment"
                );
                if strict {
                    return Err(format!("strict mode: {msg}. Use a deterministic alternative."));
                }
                crate::audit::warn(&msg);
                if let Some(ref mut log) = log {
                    log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
                }
            }
        }

        // Implicit type coercion check
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            if name.starts_with("__adapt_") {
                let msg = format!(
                    "implicit type coercion via '{name}'. Use explicit conversion function."
                );
                if strict {
                    return Err(format!("strict mode: {msg}"));
                }
                crate::audit::warn(&msg);
                if let Some(ref mut log) = log {
                    log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
                }
            }
        }

        // Unused binding check
        let output_node_indices: std::collections::HashSet<usize> =
            self.output_map.values().map(|(idx, _)| *idx).collect();
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            if name.starts_with("__") { continue; }
            let is_output = output_node_indices.contains(&i);
            let is_consumed = (0..n).any(|j| {
                self.wiring[j].iter().any(|w| matches!(w, WireSource::NodeOutput(src, _) if *src == i))
            });
            if !is_output && !is_consumed {
                let msg = format!("binding '{name}' is never referenced");
                if strict {
                    return Err(format!("strict mode: {msg}. Remove it or mark as output."));
                }
                if !name.contains("__") {
                    crate::audit::warn(&msg);
                    if let Some(ref mut log) = log {
                        log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
                    }
                }
            }
        }

        let init_count = is_init.iter().filter(|&&b| b).count();
        if init_count == 0 { return Ok(0); }

        // Phase 2: Evaluate init-time nodes.
        let mut state = self.create_state();
        let dummy_inputs = vec![0u64; self.coord_count];
        state.set_inputs(&dummy_inputs);

        for i in 0..n {
            if is_init[i] {
                if self.nodes[i].meta().outs.len() != 1 {
                    is_init[i] = false;
                    continue;
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.eval_node_public(self, i);
                }));
                if result.is_err() {
                    let node_name = &self.nodes[i].meta().name;
                    crate::audit::warn(&format!(
                        "constant folding: node '{node_name}' panicked during init-time eval — skipping fold"));
                    is_init[i] = false;
                }
            }
        }

        // Phase 3: Replace init-time nodes with constants.
        let mut folded = 0;
        for i in 0..n {
            if !is_init[i] { continue; }

            let value = state.core.buffers[i][0].clone();
            if matches!(value, Value::None) { continue; }

            let const_node: Box<dyn crate::node::GkNode> = match &value {
                Value::U64(v) => Box::new(ConstU64::new(*v)),
                Value::F64(v) => Box::new(ConstF64::new(*v)),
                Value::Bool(v) => Box::new(ConstU64::new(if *v { 1 } else { 0 })),
                Value::Str(s) => Box::new(ConstStr::new(s.clone())),
                // Handles (e.g. `init prebuffered = dataset_prebuffer(...)`)
                // get a dedicated `ConstHandle` replacement so the original
                // side-effect-bearing node is removed from the program.
                // Without this, every fresh fiber's `GkState` walks the
                // dirty original on first pull and re-fires its eval —
                // producing a per-fiber stampede that exhausts process
                // thread limits when the eval spawns HTTP workers (the
                // exact failure mode that motivates this branch).
                Value::Handle(arc) => {
                    let original_name = self.nodes[i].meta().name.clone();
                    crate::audit::info(&format!(
                        "fold: replacing init node '{original_name}' with ConstHandle \
                         (Arc<dyn Any>) — eval will not re-fire post-fold"));
                    Box::new(ConstHandle::new(arc.clone()))
                }
                _ => continue,
            };

            let node_name = self.nodes[i].meta().name.clone();
            if let Some(ref mut log) = log {
                log.push(crate::dsl::events::CompileEvent::ConstantFolded {
                    node: node_name,
                    value: value.to_display_string(),
                });
            }
            self.nodes[i] = const_node;
            self.wiring[i] = Vec::new();
            folded += 1;
        }

        Ok(folded)
    }
}
