// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GkProgram: the immutable compiled DAG shared across all fibers.

use std::collections::HashMap;

use crate::node::{GkNode, Value};
use super::{WireSource, PortDef};
use super::engines::{GkState, RawState, ProvScanState, EngineCore};

/// The immutable compiled DAG. Shared across fibers via `Arc`.
pub struct GkProgram {
    /// Node instances in topological order.
    pub(crate) nodes: Vec<Box<dyn GkNode>>,
    /// For each node, the wiring of its input ports.
    pub(crate) wiring: Vec<Vec<WireSource>>,
    /// Input coordinate names, in tuple order.
    input_names: Vec<String>,
    /// Map from output variate name to `(node_index, output_port_index)`.
    pub(crate) output_map: HashMap<String, (usize, usize)>,
    /// Volatile port definitions (reset on each set_inputs).
    volatile_ports: Vec<PortDef>,
    /// Sticky port definitions (persist until explicitly overwritten).
    sticky_ports: Vec<PortDef>,
    /// Per-node input provenance bitmask. Bit i is set if the node
    /// transitively depends on graph input i. Computed once from the
    /// DAG wiring. Supports up to 64 distinct inputs.
    pub(crate) input_provenance: Vec<u64>,
    /// Per-input dependent node lists. For each graph input (and
    /// bit 63 for ports), the list of node indices that transitively
    /// depend on it. Precomputed from provenance bitmasks.
    /// Used by set_inputs() for O(affected) invalidation instead of
    /// scanning all nodes.
    input_dependents: Vec<Vec<usize>>,
}

unsafe impl Send for GkProgram {}
unsafe impl Sync for GkProgram {}

impl std::fmt::Debug for GkProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GkProgram")
            .field("nodes", &self.nodes.len())
            .field("coords", &self.input_names)
            .field("volatile_ports", &self.volatile_ports.len())
            .field("sticky_ports", &self.sticky_ports.len())
            .finish()
    }
}

impl GkProgram {
    /// Create a program from pre-validated, topologically-sorted components.
    pub(crate) fn new(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
    ) -> Self {
        let input_provenance = Self::compute_provenance(&nodes, &wiring);
        let input_dependents = Self::compute_dependents(&input_provenance, input_names.len());
        Self {
            nodes, wiring, input_names, output_map,
            volatile_ports: Vec::new(),
            sticky_ports: Vec::new(),
            input_provenance,
            input_dependents,
        }
    }

    /// Create a program with external input ports.
    #[allow(dead_code)]  // used by tests and future assembly integration
    pub(crate) fn with_ports(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        volatile_ports: Vec<PortDef>,
        sticky_ports: Vec<PortDef>,
    ) -> Self {
        let input_provenance = Self::compute_provenance(&nodes, &wiring);
        let input_dependents = Self::compute_dependents(&input_provenance, input_names.len());
        Self {
            nodes, wiring, input_names, output_map,
            volatile_ports, sticky_ports,
            input_provenance,
            input_dependents,
        }
    }

    /// Invert provenance into per-input dependent node lists.
    ///
    /// For each input bit (0..num_inputs and bit 63 for ports),
    /// collects the list of node indices that depend on it.
    pub(crate) fn compute_dependents(provenance: &[u64], num_inputs: usize) -> Vec<Vec<usize>> {
        // Slots 0..num_inputs for graph inputs, slot num_inputs for ports (bit 63)
        let num_slots = num_inputs + 1;
        let mut deps = vec![Vec::new(); num_slots];
        for (node_idx, &prov) in provenance.iter().enumerate() {
            for input_idx in 0..num_inputs {
                if prov & (1u64 << input_idx) != 0 {
                    deps[input_idx].push(node_idx);
                }
            }
            // Port dependents (bit 63)
            if prov & (1u64 << 63) != 0 {
                deps[num_inputs].push(node_idx);
            }
        }
        deps
    }

    /// Compute per-node input provenance bitmask from the DAG wiring.
    ///
    /// Bit i is set if the node transitively depends on input i.
    /// Processed in topological order (nodes are already sorted).
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
                    WireSource::VolatilePort(_) | WireSource::StickyPort(_) => {
                        // Ports are external — treated as always-changing
                        // (all bits set would force re-eval, but that's too
                        // aggressive). Instead, port changes bump a separate
                        // generation counter that forces re-eval for nodes
                        // that depend on ports. Use bit 63 as the "port" bit.
                        prov[i] |= 1u64 << 63;
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
        let coord_count = self.input_names.len();
        let volatile_values: Vec<Value> = self.volatile_ports.iter()
            .map(|p| p.default.clone()).collect();
        let volatile_defaults: Vec<Value> = volatile_values.clone();
        let sticky_values: Vec<Value> = self.sticky_ports.iter()
            .map(|p| p.default.clone()).collect();
        let max_inputs = self.wiring.iter().map(|w| w.len()).max().unwrap_or(0);
        EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs: vec![0; coord_count],
            volatile_values, volatile_defaults, sticky_values,
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
        let coord_count = self.input_names.len();

        // Initialize port values to defaults
        let volatile_values: Vec<Value> = self.volatile_ports.iter()
            .map(|p| p.default.clone())
            .collect();
        let volatile_defaults: Vec<Value> = volatile_values.clone();
        let sticky_values: Vec<Value> = self.sticky_ports.iter()
            .map(|p| p.default.clone())
            .collect();

        // Pre-allocate scratch buffer for the largest node input count
        let max_inputs = self.wiring.iter()
            .map(|w| w.len())
            .max()
            .unwrap_or(0);

        // Identify non-deterministic nodes: zero-provenance AND no declared input slots.
        // These produce different values on every evaluation and must never be cached.
        let nondeterministic_nodes: Vec<usize> = self.nodes.iter().enumerate()
            .filter(|(i, node)| self.wiring[*i].is_empty() && node.meta().ins.is_empty())
            .map(|(i, _)| i)
            .collect();

        let core = EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs: vec![0; coord_count],
            volatile_values,
            volatile_defaults,
            sticky_values,
            input_scratch: vec![Value::None; max_inputs],
        };

        GkState::from_parts(core, self.input_dependents.clone(), nondeterministic_nodes)
    }

    /// Create a raw state with no provenance — all nodes marked dirty
    /// on every set_inputs call. For benchmarking the baseline.
    pub fn create_raw_state(&self) -> RawState {
        RawState { core: self.build_engine_core() }
    }

    /// Create the provenance-scan engine state (for benchmarking).
    pub fn create_provscan_state(&self) -> ProvScanState {
        let buffers: Vec<Vec<Value>> = self.nodes
            .iter()
            .map(|n| vec![Value::None; n.meta().outs.len()])
            .collect();
        let node_count = self.nodes.len();
        let coord_count = self.input_names.len();
        let volatile_values: Vec<Value> = self.volatile_ports.iter()
            .map(|p| p.default.clone()).collect();
        let volatile_defaults: Vec<Value> = volatile_values.clone();
        let sticky_values: Vec<Value> = self.sticky_ports.iter()
            .map(|p| p.default.clone()).collect();
        let max_inputs = self.wiring.iter()
            .map(|w| w.len()).max().unwrap_or(0);

        let nondeterministic_nodes: Vec<usize> = self.nodes.iter().enumerate()
            .filter(|(i, node)| self.wiring[*i].is_empty() && node.meta().ins.is_empty())
            .map(|(i, _)| i)
            .collect();

        let core = EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs: vec![0; coord_count],
            volatile_values,
            volatile_defaults,
            sticky_values,
            input_scratch: vec![Value::None; max_inputs],
        };

        ProvScanState::from_parts(core, self.input_provenance.clone(), nondeterministic_nodes)
    }

    /// Return the names of the graph inputs.
    pub fn input_names(&self) -> &[String] {
        &self.input_names
    }

    /// Return the names of all available output variates.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_map.keys().map(|s| s.as_str()).collect()
    }

    /// Resolve an output name to its (node_index, port_index).
    pub fn resolve_output(&self, name: &str) -> Option<(usize, usize)> {
        self.output_map.get(name).copied()
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

    /// Volatile port definitions.
    pub fn volatile_ports(&self) -> &[PortDef] {
        &self.volatile_ports
    }

    /// Sticky port definitions.
    pub fn sticky_ports(&self) -> &[PortDef] {
        &self.sticky_ports
    }

    /// Find a volatile port by name. Returns its index.
    pub fn find_volatile_port(&self, name: &str) -> Option<usize> {
        self.volatile_ports.iter().position(|p| p.name == name)
    }

    /// Find a sticky port by name. Returns its index.
    pub fn find_sticky_port(&self, name: &str) -> Option<usize> {
        self.sticky_ports.iter().position(|p| p.name == name)
    }

    /// Fold init-time constant nodes (SRD 44).
    ///
    /// Identifies nodes whose transitive dependencies contain no
    /// coordinate inputs, volatile ports, or sticky ports — these are
    /// init-time evaluable. Evaluates them once, then replaces their
    /// outputs with constant nodes in the program.
    ///
    /// Returns the number of nodes folded. The program is modified
    /// in place.
    pub fn fold_init_constants(&mut self) -> usize {
        self.fold_init_constants_impl(None, false).unwrap()
    }

    /// Fold init-time constants, emitting diagnostic events to the log.
    pub fn fold_init_constants_with_log(&mut self, log: Option<&mut crate::dsl::events::CompileEventLog>) -> usize {
        self.fold_init_constants_impl(log, false).unwrap()
    }

    /// Fold init-time constants with strict mode.
    ///
    /// In strict mode, Config wire inputs connected to cycle-time
    /// sources are promoted to errors (not warnings). This enforces
    /// that expensive recomputation cannot happen per-cycle.
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
        // A node is init-time if ALL its wire sources are either:
        //   - NodeOutput from another init-time node
        //   - (nothing else — no Coordinate, Volatile, Sticky)
        // Nodes with zero inputs (constants, context nodes like counter)
        // are init-time IF they have no side effects that matter at cycle time.
        // We conservatively exclude nodes with zero inputs that are
        // non-deterministic (like current_epoch_millis, counter, thread_id).
        let mut is_init: Vec<bool> = vec![true; n];

        // Mark nodes that directly depend on coordinates or external ports
        for (i, wiring) in self.wiring.iter().enumerate() {
            for source in wiring {
                match source {
                    WireSource::Input(_) |
                    WireSource::VolatilePort(_) |
                    WireSource::StickyPort(_) => {
                        is_init[i] = false;
                    }
                    WireSource::NodeOutput(_, _) => {}
                }
            }
            // Exclude non-deterministic zero-input nodes
            if wiring.is_empty() {
                let name = &self.nodes[i].meta().name;
                if name == "current_epoch_millis" || name == "session_start_millis"
                    || name == "elapsed_millis" || name == "counter"
                    || name == "thread_id"
                {
                    is_init[i] = false;
                }
            }
            // Note: auto-inserted type adapter nodes (names starting with
            // `__`) are intentionally NOT excluded here.  Adapters are pure
            // functions and should participate in constant folding.  A chain
            // such as `ConstU64(42) → __u64_to_f64 → sin` correctly folds
            // to a single constant because the propagation pass below marks
            // each node init-time as long as all its inputs are init-time.
        }

        // Propagate: if any input is not init-time, this node isn't either
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

        // Wire cost check: warn when a Config wire connects to a cycle-time source.
        // This catches accidental performance traps (e.g., rebuilding an LUT every cycle).
        for i in 0..n {
            let wire_inputs = self.nodes[i].meta().wire_inputs();
            for (port_idx, wire_source) in self.wiring[i].iter().enumerate() {
                if port_idx >= wire_inputs.len() { break; }
                if wire_inputs[port_idx].wire_cost != crate::node::WireCost::Config {
                    continue;
                }
                // This is a Config wire. Check if the source is cycle-time.
                let source_is_cycle = match wire_source {
                    WireSource::Input(_) => true,
                    WireSource::NodeOutput(src_idx, _) => !is_init[*src_idx],
                    WireSource::VolatilePort(_) => true,
                    WireSource::StickyPort(_) => true,
                };
                if source_is_cycle {
                    let node_name = &self.nodes[i].meta().name;
                    let port_name = &wire_inputs[port_idx].name;
                    if strict {
                        return Err(format!(
                            "strict mode: config wire '{port_name}' on node '{node_name}' \
                             is connected to a cycle-time source. In strict mode, config \
                             wires must be init-time constants."
                        ));
                    }
                    eprintln!(
                        "warning: config wire '{port_name}' on node '{node_name}' is connected to a \
                         cycle-time source. This may cause expensive per-cycle recomputation \
                         (e.g., LUT rebuild). Wire to an init-time constant if this is unintentional."
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

        // Non-deterministic node check: nodes with no inputs that are
        // excluded from init (counter, current_epoch_millis, etc.)
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            let is_nondeterministic = self.wiring[i].is_empty() && !is_init[i]
                && !name.starts_with("__");
            if is_nondeterministic {
                let msg = format!(
                    "non-deterministic node '{name}' used without explicit acknowledgment"
                );
                if strict {
                    return Err(format!("strict mode: {msg}. Mark as volatile or use a deterministic alternative."));
                }
                eprintln!("warning: {msg}");
                if let Some(ref mut log) = log {
                    log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
                }
            }
        }

        // Implicit type coercion check: auto-inserted adapter nodes
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

        // Unused binding check: output nodes with no downstream consumers
        // An output is "used" if it's in the output_map.
        let output_node_indices: std::collections::HashSet<usize> =
            self.output_map.values().map(|(idx, _)| *idx).collect();
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            if name.starts_with("__") { continue; }
            // Check if this node is consumed by any downstream node or is an output
            let is_output = output_node_indices.contains(&i);
            let is_consumed = (0..n).any(|j| {
                self.wiring[j].iter().any(|w| matches!(w, WireSource::NodeOutput(src, _) if *src == i))
            });
            if !is_output && !is_consumed {
                let msg = format!("binding '{name}' is never referenced");
                if strict {
                    return Err(format!("strict mode: {msg}. Remove it or mark as output."));
                }
                // Only warn for user-defined nodes (not auto-generated)
                if !name.contains("__") {
                    eprintln!("warning: {msg}");
                    if let Some(ref mut log) = log {
                        log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
                    }
                }
            }
        }

        // Count how many init-time nodes have downstream cycle-time consumers
        let init_count = is_init.iter().filter(|&&b| b).count();
        if init_count == 0 { return Ok(0); }

        // Phase 2: Evaluate init-time nodes.
        // Use catch_unwind to handle any panics gracefully —
        // if a node panics during init evaluation, skip folding it.
        let mut state = self.create_state();
        let dummy_inputs = vec![0u64; self.input_names.len()];
        state.set_inputs(&dummy_inputs);

        for i in 0..n {
            if is_init[i] {
                // Only fold nodes with exactly 1 output (simple constants)
                if self.nodes[i].meta().outs.len() != 1 {
                    is_init[i] = false;
                    continue;
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.eval_node_public(self, i);
                }));
                if result.is_err() {
                    is_init[i] = false; // evaluation panicked, don't fold
                }
            }
        }

        // Phase 3: Replace init-time nodes with constants.
        let mut folded = 0;
        for i in 0..n {
            if !is_init[i] { continue; }

            let value = state.core.buffers[i][0].clone();
            if matches!(value, Value::None) { continue; } // not evaluated
            let port_type = self.nodes[i].meta().outs[0].typ;

            // Replace the node with a constant
            let const_node: Box<dyn crate::node::GkNode> = match (&value, port_type) {
                (Value::U64(v), _) => Box::new(ConstU64::new(*v)),
                (Value::F64(v), _) => Box::new(ConstF64::new(*v)),
                (Value::Bool(v), _) => Box::new(ConstU64::new(if *v { 1 } else { 0 })),
                (Value::Str(s), _) => Box::new(ConstStr::new(s.clone())),
                _ => continue, // Can't fold this type
            };

            let node_name = self.nodes[i].meta().name.clone();
            if let Some(ref mut log) = log {
                log.push(crate::dsl::events::CompileEvent::ConstantFolded {
                    node: node_name,
                    value: value.to_display_string(),
                });
            }
            self.nodes[i] = const_node;
            self.wiring[i] = Vec::new(); // Constants have no inputs
            folded += 1;
        }

        Ok(folded)
    }
}
