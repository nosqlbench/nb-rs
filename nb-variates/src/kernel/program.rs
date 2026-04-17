// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GkProgram: the immutable compiled DAG shared across all fibers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{GkNode, Value};
use super::{WireSource, InputDef};
use super::engines::{GkState, RawState, ProvScanState, EngineCore};

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
    /// Source schemas declared in the GK program. The runtime queries
    /// these to discover data sources and their extents.
    cursor_schemas: Vec<crate::source::SourceSchema>,
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
            .map(|name| InputDef { name, default: Value::U64(0), port_type: crate::node::PortType::U64 })
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
            cursor_schemas: Vec::new(),
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
            cursor_schemas: Vec::new(),
        }
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
        EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs, input_defaults,
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

        let core = EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs, input_defaults,
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
    pub fn fold_init_constants(&mut self) -> usize {
        self.fold_init_constants_impl(None, false).unwrap()
    }

    /// Fold init-time constants, emitting diagnostic events to the log.
    pub fn fold_init_constants_with_log(&mut self, log: Option<&mut crate::dsl::events::CompileEventLog>) -> usize {
        self.fold_init_constants_impl(log, false).unwrap()
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
        use crate::nodes::identity::{ConstU64, ConstStr};
        use crate::nodes::fixed::ConstF64;
        use crate::node::Value;

        let n = self.nodes.len();
        if n == 0 { return Ok(0); }

        // Phase 1: Determine which nodes are init-time evaluable.
        let mut is_init: Vec<bool> = vec![true; n];

        for (i, wiring) in self.wiring.iter().enumerate() {
            for source in wiring {
                match source {
                    WireSource::Input(_) => {
                        is_init[i] = false;
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
                    is_init[i] = false;
                }
            }
        }

        // Propagate
        let mut changed = true;
        while changed {
            changed = false;
            for i in 0..n {
                if !is_init[i] { continue; }
                for source in &self.wiring[i] {
                    if let WireSource::NodeOutput(upstream, _) = source
                        && !is_init[*upstream] {
                            is_init[i] = false;
                            changed = true;
                            break;
                        }
                }
            }
        }

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
                    eprintln!(
                        "warning: config wire '{port_name}' on node '{node_name}' is connected to a \
                         cycle-time source."
                    );
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
                eprintln!("warning: {msg}");
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
                eprintln!("warning: {msg}");
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
                    eprintln!("warning: {msg}");
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
                    eprintln!("warning: constant folding: node '{node_name}' panicked during init-time eval — skipping fold");
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
