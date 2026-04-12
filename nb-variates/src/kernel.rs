// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK runtime kernel: compiled DAG with pull-through evaluation.
//!
//! ## Architecture
//!
//! ```text
//! GkProgram (Arc, immutable, shared across all fibers)
//! ┌──────────────────────────────────────────────────────────────┐
//! │  nodes[]         — Box<dyn GkNode> in topological order     │
//! │  wiring[]        — per-node input source tables              │
//! │  input_names[]   — graph input dimension names ("cycle")     │
//! │  output_map      — name → (node_idx, port_idx)               │
//! │  globals         — resolved workload params (set once)       │
//! │  volatile_ports  — port definitions (reset per evaluation)   │
//! │  sticky_ports    — port definitions (persist per stanza)     │
//! └──────────────────────────────────────────────────────────────┘
//!
//! GkState (per-fiber, mutable, private — never shared)
//! ┌──────────────────────────────────────────────────────────────┐
//! │  inputs[]            — current input values (e.g., [cycle])  │
//! │  generation          — advances on set_inputs(), used for    │
//! │                        memoization (skip re-evaluation)      │
//! │  node_generation[]   — last-evaluated generation per node    │
//! │  buffers[][]         — per-node output value slots:          │
//! │    ┌───────────┐                                             │
//! │    │ node 0    │ [Value, Value, ...]  (one per output port)  │
//! │    │ node 1    │ [Value]                                     │
//! │    │ node 2    │ [Value, Value]                              │
//! │    │ ...       │                                             │
//! │    └───────────┘                                             │
//! │  volatile_values[]   — external ports, reset on set_inputs() │
//! │  volatile_defaults[] — reset targets for volatile ports      │
//! │  sticky_values[]     — external ports, persist across evals  │
//! │  input_scratch[]     — temp buffer for node input gathering  │
//! └──────────────────────────────────────────────────────────────┘
//!
//! Evaluation:
//!   1. fiber.set_inputs(&[cycle])  → state.inputs = [cycle],
//!                                    generation++, volatiles reset
//!   2. state.pull(program, "name") → walk topologically, skip nodes
//!                                    already evaluated this generation,
//!                                    return &buffers[node][port]
//!
//! Globals:
//!   Resolved workload params stored on GkProgram. Set once after
//!   compilation, read by fibers via program.globals(). Never
//!   re-resolved from external sources. Each fiber reads the same
//!   immutable Arc<GkProgram>.
//! ```
//!
//! Buffer layout in GkState:
//! ```text
//! coords[0..C) | volatile[0..V) | sticky[0..S) | node_buffers[...]
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{GkNode, Value};

/// Source of a value for a node input port.
#[derive(Debug, Clone)]
pub enum WireSource {
    /// An input coordinate, by index into the coordinate tuple.
    Input(usize),
    /// Output of another node: `(node_index, output_port_index)`.
    NodeOutput(usize, usize),
    /// A volatile external input port, by index.
    VolatilePort(usize),
    /// A sticky external input port, by index.
    StickyPort(usize),
}

/// Metadata for an external input port.
#[derive(Debug, Clone)]
pub struct PortDef {
    /// Port name (used for capture wiring and bind point resolution).
    pub name: String,
    /// Default value (used for volatile reset, or initial sticky value).
    pub default: Value,
}

/// The immutable compiled DAG. Shared across fibers via `Arc`.
pub struct GkProgram {
    /// Node instances in topological order.
    nodes: Vec<Box<dyn GkNode>>,
    /// For each node, the wiring of its input ports.
    wiring: Vec<Vec<WireSource>>,
    /// Input coordinate names, in tuple order.
    input_names: Vec<String>,
    /// Map from output variate name to `(node_index, output_port_index)`.
    output_map: HashMap<String, (usize, usize)>,
    /// Volatile port definitions (reset on each set_inputs).
    volatile_ports: Vec<PortDef>,
    /// Sticky port definitions (persist until explicitly overwritten).
    sticky_ports: Vec<PortDef>,
    /// Global values: resolved workload params stored on the program.
    /// Set once after compilation, before any fibers are created.
    /// Fibers read these via `globals()` — no separate params map needed.
    globals: std::collections::HashMap<String, String>,
    /// Per-node input provenance bitmask. Bit i is set if the node
    /// transitively depends on graph input i. Computed once from the
    /// DAG wiring. Supports up to 64 distinct inputs.
    input_provenance: Vec<u64>,
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
            globals: HashMap::new(),
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
            globals: HashMap::new(),
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

        GkState {
            core: EngineCore {
                buffers,
                node_clean: vec![false; node_count],
                inputs: vec![0; coord_count],
                volatile_values,
                volatile_defaults,
                sticky_values,
                input_scratch: vec![Value::None; max_inputs],
            },
            input_dependents: self.input_dependents.clone(),
            nondeterministic_nodes,
        }
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

        ProvScanState {
            core: EngineCore {
                buffers,
                node_clean: vec![false; node_count],
                inputs: vec![0; coord_count],
                volatile_values,
                volatile_defaults,
                sticky_values,
                input_scratch: vec![Value::None; max_inputs],
            },
            input_provenance: self.input_provenance.clone(),
            nondeterministic_nodes,
        }
    }

    /// Set global values (resolved workload params). Called once
    /// after compilation, before any fibers are created.
    pub fn set_globals(&mut self, globals: HashMap<String, String>) {
        self.globals = globals;
    }

    /// Access global values (workload params stored on the program).
    pub fn globals(&self) -> &HashMap<String, String> {
        &self.globals
    }

    /// Get a global value by name.
    pub fn get_global(&self, name: &str) -> Option<&str> {
        self.globals.get(name).map(|s| s.as_str())
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

    /// Number of nodes in the program.
    /// Get the provenance bitmask for a node by index.
    pub fn input_provenance_for(&self, node_idx: usize) -> u64 {
        self.input_provenance.get(node_idx).copied().unwrap_or(0)
    }

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
            // Exclude auto-inserted type adapter nodes (internal wiring)
            let name = &self.nodes[i].meta().name;
            if name.starts_with("__") {
                is_init[i] = false;
            }
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

/// Per-fiber mutable evaluation state.
///
/// Contains the value buffers, generation counter, coordinates, and
/// external input port values. Each fiber/thread owns one of these.
///
/// Setting coordinates (`set_inputs`) begins a new isolation
/// scope: generation advances, volatile ports reset to defaults,
/// sticky ports persist.
// =================================================================
// Engine core: shared evaluation state used by all GK engines.
// Contains buffers, input values, port values, and the eval loop.
// Each engine wraps this core with its own invalidation strategy.
// =================================================================

/// Shared evaluation state for all GK engines. Contains the node
/// output buffers, input values, port values, and the eval loop.
/// Engine types wrap this and provide their own `set_inputs()`.
pub struct EngineCore {
    /// Per-node output value buffers, reused across evaluations.
    pub(crate) buffers: Vec<Vec<Value>>,
    /// Per-node: true = cached output is valid, false = needs eval.
    pub(crate) node_clean: Vec<bool>,
    /// Current input values.
    pub(crate) inputs: Vec<u64>,
    /// Current volatile port values (reset on set_inputs).
    pub(crate) volatile_values: Vec<Value>,
    /// Default values for volatile ports (copied on reset).
    pub(crate) volatile_defaults: Vec<Value>,
    /// Current sticky port values (persist across set_inputs).
    pub(crate) sticky_values: Vec<Value>,
    /// Pre-allocated scratch buffer for node input gathering.
    pub(crate) input_scratch: Vec<Value>,
}

impl EngineCore {
    /// Evaluate a node by index. Shared by all engines.
    /// Checks the clean flag, recursively evaluates upstream, gathers
    /// inputs, calls node.eval(), marks clean.
    pub fn eval_node(&mut self, program: &GkProgram, node_idx: usize) {
        if self.node_clean[node_idx] {
            return;
        }

        let wiring = &program.wiring[node_idx];
        for source in wiring.iter() {
            if let WireSource::NodeOutput(upstream_idx, _) = source {
                self.eval_node(program, *upstream_idx);
            }
        }

        for (i, source) in wiring.iter().enumerate() {
            self.input_scratch[i] = match source {
                WireSource::Input(idx) => Value::U64(self.inputs[*idx]),
                WireSource::NodeOutput(upstream_idx, port_idx) => {
                    self.buffers[*upstream_idx][*port_idx].clone()
                }
                WireSource::VolatilePort(idx) => self.volatile_values[*idx].clone(),
                WireSource::StickyPort(idx) => self.sticky_values[*idx].clone(),
            };
        }

        let input_count = wiring.len();
        program.nodes[node_idx].eval(
            &self.input_scratch[..input_count],
            &mut self.buffers[node_idx],
        );
        self.node_clean[node_idx] = true;
    }

    /// Pull a named output.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        let (node_idx, port_idx) = *program.output_map
            .get(output_name)
            .unwrap_or_else(|| panic!("unknown output variate: {output_name}"));
        self.eval_node(program, node_idx);
        &self.buffers[node_idx][port_idx]
    }
}

// =================================================================
// GkState: dependent-list engine (default, O(affected) invalidation)
// =================================================================

/// GK evaluation engine using precomputed per-input dependent lists.
///
/// On `set_inputs()`, only nodes that depend on actually-changed
/// inputs are dirtied. O(affected_nodes) per input change.
/// This is the default engine for production use.
pub struct GkState {
    /// Shared evaluation core (buffers, clean flags, inputs, ports).
    pub core: EngineCore,
    /// Per-input dependent node lists for O(affected) invalidation.
    input_dependents: Vec<Vec<usize>>,
    /// Indices of non-deterministic nodes (zero-provenance, no declared inputs).
    ///
    /// These nodes produce a different value on every evaluation (e.g.,
    /// `counter()`, `current_epoch_millis()`). They are unconditionally
    /// marked dirty on every `set_inputs()` call so they are never cached.
    nondeterministic_nodes: Vec<usize>,
}

impl GkState {
    /// Set new input values with change detection.
    ///
    /// Compares each input against its current value. Only inputs that
    /// actually changed are flagged in `changed_mask`. Nodes that don't
    /// depend on changed inputs remain cached (provenance check).
    ///
    /// Non-deterministic nodes (counter, current_epoch_millis, etc.) are
    /// always marked dirty regardless of input changes, because their output
    /// depends on internal state rather than graph inputs.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        for i in 0..coords.len().min(self.core.inputs.len()) {
            if self.core.inputs[i] != coords[i] {
                self.core.inputs[i] = coords[i];
                for &node_idx in &self.input_dependents[i] {
                    self.core.node_clean[node_idx] = false;
                }
            }
        }
        if !self.core.volatile_defaults.is_empty() {
            self.core.volatile_values.clone_from_slice(&self.core.volatile_defaults);
            let port_slot = self.input_dependents.len() - 1;
            for &node_idx in &self.input_dependents[port_slot] {
                self.core.node_clean[node_idx] = false;
            }
        }
        // Non-deterministic nodes must always re-evaluate — never use cached values.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    pub fn set_volatile(&mut self, idx: usize, value: Value) {
        self.core.volatile_values[idx] = value;
        let port_slot = self.input_dependents.len() - 1;
        for &node_idx in &self.input_dependents[port_slot] {
            self.core.node_clean[node_idx] = false;
        }
    }

    pub fn set_sticky(&mut self, idx: usize, value: Value) {
        self.core.sticky_values[idx] = value;
        let port_slot = self.input_dependents.len() - 1;
        for &node_idx in &self.input_dependents[port_slot] {
            self.core.node_clean[node_idx] = false;
        }
    }

    pub fn get_volatile(&self, idx: usize) -> &Value {
        &self.core.volatile_values[idx]
    }

    pub fn get_sticky(&self, idx: usize) -> &Value {
        &self.core.sticky_values[idx]
    }

    pub fn get_input(&self, idx: usize) -> u64 {
        self.core.inputs[idx]
    }

    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }

    pub(crate) fn eval_node_public(&mut self, program: &GkProgram, node_idx: usize) {
        self.core.eval_node(program, node_idx);
    }
}

// =================================================================
// RawState: no provenance engine (all nodes dirty every eval)
// =================================================================

/// GK evaluation engine with no provenance. Every `set_inputs()`
/// marks all nodes dirty. Baseline for benchmarking provenance overhead.
pub struct RawState {
    pub core: EngineCore,
}

impl RawState {
    pub fn set_inputs(&mut self, coords: &[u64]) {
        for i in 0..coords.len().min(self.core.inputs.len()) {
            self.core.inputs[i] = coords[i];
        }
        // All nodes dirty — no provenance check
        for clean in &mut self.core.node_clean {
            *clean = false;
        }
        if !self.core.volatile_defaults.is_empty() {
            self.core.volatile_values.clone_from_slice(&self.core.volatile_defaults);
        }
    }

    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }
}

// =================================================================
// ProvScanState: provenance-scan engine (O(all) invalidation)
// =================================================================

/// GK evaluation engine using provenance bitmask scanning.
///
/// On `set_inputs()`, scans ALL nodes and checks each node's
/// provenance bitmask against the changed-inputs mask.
/// O(all_nodes) per input change regardless of how many changed.
pub struct ProvScanState {
    pub core: EngineCore,
    input_provenance: Vec<u64>,
    /// Indices of non-deterministic nodes (zero-provenance, no declared inputs).
    ///
    /// Marked dirty unconditionally on every `set_inputs()` call since
    /// their output depends on internal state rather than graph inputs.
    nondeterministic_nodes: Vec<usize>,
}

impl ProvScanState {
    /// Set new input values and invalidate affected nodes.
    ///
    /// Scans all nodes and checks each node's provenance bitmask
    /// against the changed-inputs mask. O(all_nodes) per call.
    ///
    /// Non-deterministic nodes (zero provenance, no declared inputs) are
    /// always marked dirty regardless of what inputs changed.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        let mut mask = 0u64;
        for i in 0..coords.len().min(self.core.inputs.len()) {
            if self.core.inputs[i] != coords[i] {
                self.core.inputs[i] = coords[i];
                mask |= 1u64 << i;
            }
        }
        if !self.core.volatile_defaults.is_empty() {
            self.core.volatile_values.clone_from_slice(&self.core.volatile_defaults);
            mask |= 1u64 << 63;
        }
        if mask != 0 {
            for (i, clean) in self.core.node_clean.iter_mut().enumerate() {
                if *clean && (self.input_provenance[i] & mask) != 0 {
                    *clean = false;
                }
            }
        }
        // Non-deterministic nodes must always re-evaluate — never use cached values.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }
}

/// Stanza-scoped capture context.
///
/// Stores captured values from operation results within a single stanza.
/// Values are written by the adapter extraction step and read by
/// subsequent operations' GK kernels (via volatile/sticky ports) or
/// directly by bind point resolution.
///
/// Each executor fiber owns one. Reset at stanza start.
pub struct CaptureContext {
    /// Named captured values.
    values: HashMap<String, Value>,
    /// The cycle this context is evaluating.
    cycle: u64,
}

impl CaptureContext {
    /// Create an empty capture context.
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            cycle: 0,
        }
    }

    /// Reset for a new stanza/cycle. Clears all captured values.
    pub fn reset(&mut self, cycle: u64) {
        self.values.clear();
        self.cycle = cycle;
    }

    /// Store a captured value.
    pub fn set(&mut self, name: &str, value: Value) {
        self.values.insert(name.to_string(), value);
    }

    /// Read a captured value. Returns None if not yet captured.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    /// The current cycle.
    pub fn cycle(&self) -> u64 {
        self.cycle
    }

    /// All captured name-value pairs.
    pub fn values(&self) -> &HashMap<String, Value> {
        &self.values
    }

    /// Transfer captured values into a GkState's volatile/sticky ports.
    ///
    /// For each captured name, if the program has a matching volatile
    /// or sticky port, write the value into the state's port buffer.
    pub fn apply_to_state(&self, program: &GkProgram, state: &mut GkState) {
        for (name, value) in &self.values {
            if let Some(idx) = program.find_volatile_port(name) {
                state.set_volatile(idx, value.clone());
            } else if let Some(idx) = program.find_sticky_port(name) {
                state.set_sticky(idx, value.clone());
            }
        }
    }
}

impl Default for CaptureContext {
    fn default() -> Self { Self::new() }
}

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
        self.program.input_names.iter()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volatile_ports_reset_on_set_inputs() {
        // Build a minimal program with one volatile port
        let program = Arc::new(GkProgram::with_ports(
            vec![], vec![], vec!["cycle".into()],
            HashMap::new(),
            vec![PortDef { name: "balance".into(), default: Value::F64(0.0) }],
            vec![],
        ));
        let mut state = program.create_state();

        // Default value
        assert_eq!(state.get_volatile(0), &Value::F64(0.0));

        // Set a value
        state.set_volatile(0, Value::F64(1234.56));
        assert_eq!(state.get_volatile(0), &Value::F64(1234.56));

        // Reset via set_inputs
        state.set_inputs(&[42]);
        assert_eq!(state.get_volatile(0), &Value::F64(0.0)); // back to default
    }

    #[test]
    fn sticky_ports_persist_across_coordinates() {
        let program = Arc::new(GkProgram::with_ports(
            vec![], vec![], vec!["cycle".into()],
            HashMap::new(),
            vec![],
            vec![PortDef { name: "auth_token".into(), default: Value::Str("anonymous".into()) }],
        ));
        let mut state = program.create_state();

        // Default
        assert_eq!(state.get_sticky(0), &Value::Str("anonymous".into()));

        // Set
        state.set_sticky(0, Value::Str("token_abc".into()));
        assert_eq!(state.get_sticky(0), &Value::Str("token_abc".into()));

        // Survives coordinate change
        state.set_inputs(&[99]);
        assert_eq!(state.get_sticky(0), &Value::Str("token_abc".into()));
    }

    #[test]
    fn capture_context_lifecycle() {
        let mut ctx = CaptureContext::new();
        ctx.reset(42);
        assert_eq!(ctx.cycle(), 42);
        assert!(ctx.get("balance").is_none());

        ctx.set("balance", Value::F64(100.0));
        ctx.set("user_id", Value::U64(7));
        assert_eq!(ctx.get("balance"), Some(&Value::F64(100.0)));
        assert_eq!(ctx.get("user_id"), Some(&Value::U64(7)));

        // Reset clears everything
        ctx.reset(43);
        assert!(ctx.get("balance").is_none());
        assert!(ctx.get("user_id").is_none());
    }

    #[test]
    fn capture_context_applies_to_state() {
        let program = Arc::new(GkProgram::with_ports(
            vec![], vec![], vec!["cycle".into()],
            HashMap::new(),
            vec![PortDef { name: "balance".into(), default: Value::F64(0.0) }],
            vec![PortDef { name: "session".into(), default: Value::U64(0) }],
        ));
        let mut state = program.create_state();
        let mut ctx = CaptureContext::new();

        ctx.reset(1);
        ctx.set("balance", Value::F64(999.0));
        ctx.set("session", Value::U64(42));
        ctx.apply_to_state(&program, &mut state);

        assert_eq!(state.get_volatile(0), &Value::F64(999.0));
        assert_eq!(state.get_sticky(0), &Value::U64(42));
    }

    #[test]
    fn fold_init_constants_basic() {
        // base=42, seed=hash(base) should both be folded
        // user_id=hash(cycle) should NOT be folded (depends on coordinate)
        use crate::dsl::compile::compile_gk;
        let mut k = compile_gk("coordinates := (cycle)\nbase := 42\nseed := hash(base)\nuser_id := hash(cycle)").unwrap();

        // seed should be constant across cycles
        k.set_inputs(&[0]);
        let seed_0 = k.pull("seed").clone();
        k.set_inputs(&[1]);
        let seed_1 = k.pull("seed").clone();
        assert_eq!(seed_0.as_u64(), seed_1.as_u64(), "seed should be constant (folded)");

        // user_id should vary
        k.set_inputs(&[0]);
        let uid_0 = k.pull("user_id").clone();
        k.set_inputs(&[1]);
        let uid_1 = k.pull("user_id").clone();
        assert_ne!(uid_0.as_u64(), uid_1.as_u64(), "user_id should vary per cycle");
    }

    #[test]
    fn fold_does_not_touch_cycle_dependent() {
        use crate::dsl::compile::compile_gk;
        let mut k = compile_gk("coordinates := (cycle)\nout := hash(cycle)").unwrap();
        k.set_inputs(&[42]);
        let v1 = k.pull("out").as_u64();
        k.set_inputs(&[43]);
        let v2 = k.pull("out").as_u64();
        assert_ne!(v1, v2, "cycle-dependent node should not be folded");
    }

    // ---------------------------------------------------------------
    // WireCost tests: config wire warnings for various DAG shapes
    // ---------------------------------------------------------------

    /// A test node with one Config wire input and one Data wire input.
    /// Simulates a node with an expensive LUT that's configured by
    /// the first input and driven by the second.
    struct ConfigWireTestNode {
        meta: crate::node::NodeMeta,
    }

    impl ConfigWireTestNode {
        fn new() -> Self {
            use crate::node::{Port, Slot};
            Self {
                meta: crate::node::NodeMeta {
                    name: "config_test".into(),
                    outs: vec![Port::u64("output")],
                    ins: vec![
                        Slot::Wire(Port::u64("config_param").config()),
                        Slot::Wire(Port::u64("data_input")),
                    ],
                },
            }
        }
    }

    impl crate::node::GkNode for ConfigWireTestNode {
        fn meta(&self) -> &crate::node::NodeMeta { &self.meta }
        fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
            let config = inputs[0].as_u64();
            let data = inputs[1].as_u64();
            outputs[0] = Value::U64(config.wrapping_add(data));
        }
    }

    #[test]
    fn wire_cost_no_warning_when_config_is_init_time() {
        // DAG: constant(42) → config_test.config_param
        //      cycle → hash → config_test.data_input
        // Config wire fed by init-time constant → no warning
        use crate::assembly::{GkAssembler, WireRef};
        use crate::nodes::identity::ConstU64;
        use crate::nodes::hash::Hash64;
        use crate::dsl::events::CompileEventLog;

        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("config_val", Box::new(ConstU64::new(42)), vec![]);
        asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm.add_node("test_node", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::node("config_val"),
            WireRef::node("hashed"),
        ]);
        asm.add_output("result", WireRef::node("test_node"));

        let mut log = CompileEventLog::new();
        let k = asm.compile_with_log(Some(&mut log)).unwrap();
        let program = k.into_program();

        // Check: no ConfigWireCycleWarning in events
        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert!(warnings.is_empty(), "no warning expected when config wire is init-time: {warnings:?}");
    }

    #[test]
    fn wire_cost_warning_when_config_is_cycle_time() {
        // DAG: cycle → hash → config_test.config_param  (BAD: config from cycle)
        //      cycle → config_test.data_input
        // Config wire fed by cycle-time node → should warn
        use crate::assembly::{GkAssembler, WireRef};
        use crate::nodes::hash::Hash64;
        use crate::dsl::events::CompileEventLog;

        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm.add_node("test_node", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::node("hashed"),   // config_param ← cycle-time!
            WireRef::input("cycle"),   // data_input ← cycle
        ]);
        asm.add_output("result", WireRef::node("test_node"));

        let mut log = CompileEventLog::new();
        let _k = asm.compile_with_log(Some(&mut log)).unwrap();

        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert_eq!(warnings.len(), 1, "expected exactly one config wire warning: {warnings:?}");
    }

    #[test]
    fn wire_cost_warning_when_config_is_coordinate_direct() {
        // DAG: cycle → config_test.config_param  (BAD: coordinate direct to config)
        //      cycle → config_test.data_input
        use crate::assembly::{GkAssembler, WireRef};
        use crate::dsl::events::CompileEventLog;

        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("test_node", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::input("cycle"),   // config_param ← coordinate!
            WireRef::input("cycle"),   // data_input ← cycle
        ]);
        asm.add_output("result", WireRef::node("test_node"));

        let mut log = CompileEventLog::new();
        let _k = asm.compile_with_log(Some(&mut log)).unwrap();

        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert_eq!(warnings.len(), 1, "config wire from coordinate should warn");
    }

    #[test]
    fn wire_cost_no_warning_data_wire_from_cycle() {
        // DAG: constant(10) → config_test.config_param (init-time, ok)
        //      cycle → config_test.data_input           (cycle-time, ok for Data wire)
        // Only the data wire is cycle-time → no warning
        use crate::assembly::{GkAssembler, WireRef};
        use crate::nodes::identity::ConstU64;
        use crate::dsl::events::CompileEventLog;

        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("config_val", Box::new(ConstU64::new(10)), vec![]);
        asm.add_node("test_node", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::node("config_val"),  // config_param ← constant
            WireRef::input("cycle"),      // data_input ← cycle (Data wire, ok)
        ]);
        asm.add_output("result", WireRef::node("test_node"));

        let mut log = CompileEventLog::new();
        let _k = asm.compile_with_log(Some(&mut log)).unwrap();

        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert!(warnings.is_empty(), "data wire from cycle should not warn");
    }

    #[test]
    fn wire_cost_diamond_config_from_init() {
        // Diamond DAG using two ConfigWireTestNodes:
        //   constant(5) → inner.config_param ─┐
        //   constant(3) → inner.data_input    ─┤→ inner.output → outer.config_param
        //   cycle → hash → outer.data_input
        // inner is fully init-time → its output feeds outer's config wire → no warning
        use crate::assembly::{GkAssembler, WireRef};
        use crate::nodes::identity::ConstU64;
        use crate::nodes::hash::Hash64;
        use crate::dsl::events::CompileEventLog;

        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("a", Box::new(ConstU64::new(5)), vec![]);
        asm.add_node("b", Box::new(ConstU64::new(3)), vec![]);
        asm.add_node("inner", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::node("a"), WireRef::node("b"),
        ]);
        asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm.add_node("outer", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::node("inner"),  // config_param ← init-time (5+3)
            WireRef::node("hashed"), // data_input ← cycle-time
        ]);
        asm.add_output("result", WireRef::node("outer"));

        let mut log = CompileEventLog::new();
        let _k = asm.compile_with_log(Some(&mut log)).unwrap();

        // inner's config wire from constant is fine. outer's config wire
        // from init-time inner output is also fine.
        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert!(warnings.is_empty(), "init-time derived config should not warn: {warnings:?}");
    }

    #[test]
    fn wire_cost_diamond_config_from_mixed() {
        // Mixed init/cycle feeding config:
        //   constant(5) → mixer.config_param ─┐
        //   cycle → mixer.data_input          ─┤→ mixer.output → outer.config_param
        //   cycle → outer.data_input
        // mixer depends on cycle → its output is cycle-time → outer's config wire warns
        use crate::assembly::{GkAssembler, WireRef};
        use crate::nodes::identity::ConstU64;
        use crate::dsl::events::CompileEventLog;

        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("five", Box::new(ConstU64::new(5)), vec![]);
        asm.add_node("mixer", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::node("five"),     // config_param ← init
            WireRef::input("cycle"),   // data_input ← cycle
        ]);
        asm.add_node("outer", Box::new(ConfigWireTestNode::new()), vec![
            WireRef::node("mixer"),    // config_param ← cycle-tainted!
            WireRef::input("cycle"),   // data_input
        ]);
        asm.add_output("result", WireRef::node("outer"));

        let mut log = CompileEventLog::new();
        let _k = asm.compile_with_log(Some(&mut log)).unwrap();

        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        // outer's config from cycle-tainted mixer should warn.
        // mixer's config from constant should NOT warn.
        assert_eq!(warnings.len(), 1, "exactly one warning for outer's config: {warnings:?}");
    }
}
