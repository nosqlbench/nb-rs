// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK runtime kernel: compiled DAG with pull-through evaluation.
//!
//! Split into two parts:
//! - `GkProgram`: immutable compiled DAG. Shared via `Arc`.
//! - `GkState`: per-fiber mutable state (buffers, ports, generation).
//!
//! External input ports (SRD 28):
//! - **Volatile ports**: reset to defaults on `set_coordinates()`.
//!   Used for per-cycle capture results.
//! - **Sticky ports**: persist across coordinate changes until
//!   explicitly overwritten. Used for session-level state.
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
    Coordinate(usize),
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
    coord_names: Vec<String>,
    /// Map from output variate name to `(node_index, output_port_index)`.
    output_map: HashMap<String, (usize, usize)>,
    /// Volatile port definitions (reset on each set_coordinates).
    volatile_ports: Vec<PortDef>,
    /// Sticky port definitions (persist until explicitly overwritten).
    sticky_ports: Vec<PortDef>,
}

unsafe impl Send for GkProgram {}
unsafe impl Sync for GkProgram {}

impl std::fmt::Debug for GkProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GkProgram")
            .field("nodes", &self.nodes.len())
            .field("coords", &self.coord_names)
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
        coord_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
    ) -> Self {
        Self {
            nodes, wiring, coord_names, output_map,
            volatile_ports: Vec::new(),
            sticky_ports: Vec::new(),
        }
    }

    /// Create a program with external input ports.
    #[allow(dead_code)]  // used by tests and future assembly integration
    pub(crate) fn with_ports(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        coord_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        volatile_ports: Vec<PortDef>,
        sticky_ports: Vec<PortDef>,
    ) -> Self {
        Self {
            nodes, wiring, coord_names, output_map,
            volatile_ports, sticky_ports,
        }
    }

    /// Create a new evaluation state for this program.
    pub fn create_state(&self) -> GkState {
        let buffers: Vec<Vec<Value>> = self.nodes
            .iter()
            .map(|n| vec![Value::None; n.meta().outs.len()])
            .collect();
        let node_count = self.nodes.len();
        let coord_count = self.coord_names.len();

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

        GkState {
            buffers,
            generation: 0,
            node_generation: vec![0; node_count],
            coords: vec![0; coord_count],
            volatile_values,
            volatile_defaults,
            sticky_values,
            input_scratch: vec![Value::None; max_inputs],
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
        self.fold_init_constants_with_log(None)
    }

    /// Fold init-time constants, emitting diagnostic events to the log.
    pub fn fold_init_constants_with_log(&mut self, mut log: Option<&mut crate::dsl::events::CompileEventLog>) -> usize {
        use crate::nodes::identity::{ConstU64, ConstStr};
        use crate::nodes::fixed::ConstF64;
        use crate::node::Value;

        let n = self.nodes.len();
        if n == 0 { return 0; }

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
                    WireSource::Coordinate(_) |
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

        // Count how many init-time nodes have downstream cycle-time consumers
        let init_count = is_init.iter().filter(|&&b| b).count();
        if init_count == 0 { return 0; }

        // Phase 2: Evaluate init-time nodes.
        // Use catch_unwind to handle any panics gracefully —
        // if a node panics during init evaluation, skip folding it.
        let mut state = self.create_state();
        let dummy_coords = vec![0u64; self.coord_names.len()];
        state.set_coordinates(&dummy_coords);

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

            let value = state.buffers[i][0].clone();
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

        folded
    }
}

/// Per-fiber mutable evaluation state.
///
/// Contains the value buffers, generation counter, coordinates, and
/// external input port values. Each fiber/thread owns one of these.
///
/// Setting coordinates (`set_coordinates`) begins a new isolation
/// scope: generation advances, volatile ports reset to defaults,
/// sticky ports persist.
pub struct GkState {
    /// Per-node output value buffers, reused across evaluations.
    buffers: Vec<Vec<Value>>,
    /// Current generation (advances on each coordinate change).
    generation: u64,
    /// Per-node: the generation at which this node was last evaluated.
    node_generation: Vec<u64>,
    /// Current coordinate values.
    coords: Vec<u64>,
    /// Current volatile port values (reset on set_coordinates).
    volatile_values: Vec<Value>,
    /// Default values for volatile ports (copied on reset).
    volatile_defaults: Vec<Value>,
    /// Current sticky port values (persist across set_coordinates).
    sticky_values: Vec<Value>,
    /// Pre-allocated scratch buffer for node input gathering.
    /// Sized to the maximum input count across all nodes.
    /// Reused on every eval_node call — zero per-cycle allocation.
    input_scratch: Vec<Value>,
}

impl GkState {
    /// Set new coordinates and begin a new isolation scope.
    ///
    /// - Generation advances (invalidates cached node outputs)
    /// - Volatile ports reset to their defaults
    /// - Sticky ports are untouched
    pub fn set_coordinates(&mut self, coords: &[u64]) {
        self.generation = self.generation.wrapping_add(1);
        self.coords.copy_from_slice(coords);
        // Reset volatile ports to defaults (fast memcpy-equivalent)
        self.volatile_values.clone_from_slice(&self.volatile_defaults);
    }

    /// Set a volatile port value by index.
    ///
    /// Called by the executor after a capture fires. The value is
    /// available to subsequent GK evaluations in the same stanza.
    pub fn set_volatile(&mut self, idx: usize, value: Value) {
        self.volatile_values[idx] = value;
        // Invalidate cached outputs since input data changed
        self.generation = self.generation.wrapping_add(1);
    }

    /// Set a sticky port value by index.
    ///
    /// Called by the executor after a capture fires. The value persists
    /// across coordinate changes until explicitly overwritten.
    pub fn set_sticky(&mut self, idx: usize, value: Value) {
        self.sticky_values[idx] = value;
        self.generation = self.generation.wrapping_add(1);
    }

    /// Read a volatile port value.
    pub fn get_volatile(&self, idx: usize) -> &Value {
        &self.volatile_values[idx]
    }

    /// Read a sticky port value.
    pub fn get_sticky(&self, idx: usize) -> &Value {
        &self.sticky_values[idx]
    }

    /// Read a coordinate value by index.
    pub fn get_coord(&self, idx: usize) -> u64 {
        self.coords[idx]
    }

    /// Pull a named output variate using the given program.
    pub fn pull(&mut self, program: &GkProgram, output_name: &str) -> &Value {
        let (node_idx, port_idx) = *program.output_map
            .get(output_name)
            .unwrap_or_else(|| panic!("unknown output variate: {output_name}"));
        self.eval_node(program, node_idx);
        &self.buffers[node_idx][port_idx]
    }

    /// Evaluate a node by index (package-visible for constant folding).
    pub(crate) fn eval_node_public(&mut self, program: &GkProgram, node_idx: usize) {
        self.eval_node(program, node_idx);
    }

    fn eval_node(&mut self, program: &GkProgram, node_idx: usize) {
        if self.node_generation[node_idx] == self.generation {
            return;
        }

        // First: recursively evaluate all upstream nodes. This must
        // happen BEFORE gathering values into the scratch buffer,
        // because recursive eval_node calls also write to input_scratch.
        let wiring = &program.wiring[node_idx];
        for source in wiring.iter() {
            if let WireSource::NodeOutput(upstream_idx, _) = source {
                self.eval_node(program, *upstream_idx);
            }
        }

        // Now gather inputs into the scratch buffer. All upstream nodes
        // are already evaluated, so no recursive calls will clobber it.
        for (i, source) in wiring.iter().enumerate() {
            self.input_scratch[i] = match source {
                WireSource::Coordinate(coord_idx) => Value::U64(self.coords[*coord_idx]),
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
        self.node_generation[node_idx] = self.generation;
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
        coord_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
    ) -> Self {
        Self::new_with_log(nodes, wiring, coord_names, output_map, None)
    }

    pub(crate) fn new_with_log(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        coord_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
    ) -> Self {
        let mut program = GkProgram::new(nodes, wiring, coord_names, output_map);
        let constants_folded = program.fold_init_constants_with_log(log);
        let program = Arc::new(program);
        let state = program.create_state();
        Self { program, state, constants_folded }
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
    pub fn set_coordinates(&mut self, coords: &[u64]) {
        self.state.set_coordinates(coords);
    }

    /// Read a coordinate value by name.
    pub fn get_coord(&self, name: &str) -> Option<u64> {
        self.program.coord_names.iter()
            .position(|n| n == name)
            .map(|idx| self.state.get_coord(idx))
    }

    /// Convenience: pull from the owned state.
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

    /// Read the value of a named output that was folded to a constant
    /// at init time. Returns `None` if the output doesn't exist or
    /// wasn't folded (i.e., it depends on coordinates).
    pub fn get_constant(&self, name: &str) -> Option<&Value> {
        let (node_idx, port_idx) = self.program.output_map.get(name)?;
        let val = &self.state.buffers[*node_idx][*port_idx];
        if matches!(val, Value::None) { None } else { Some(val) }
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
    fn volatile_ports_reset_on_set_coordinates() {
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

        // Reset via set_coordinates
        state.set_coordinates(&[42]);
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
        state.set_coordinates(&[99]);
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
        k.set_coordinates(&[0]);
        let seed_0 = k.pull("seed").clone();
        k.set_coordinates(&[1]);
        let seed_1 = k.pull("seed").clone();
        assert_eq!(seed_0.as_u64(), seed_1.as_u64(), "seed should be constant (folded)");

        // user_id should vary
        k.set_coordinates(&[0]);
        let uid_0 = k.pull("user_id").clone();
        k.set_coordinates(&[1]);
        let uid_1 = k.pull("user_id").clone();
        assert_ne!(uid_0.as_u64(), uid_1.as_u64(), "user_id should vary per cycle");
    }

    #[test]
    fn fold_does_not_touch_cycle_dependent() {
        use crate::dsl::compile::compile_gk;
        let mut k = compile_gk("coordinates := (cycle)\nout := hash(cycle)").unwrap();
        k.set_coordinates(&[42]);
        let v1 = k.pull("out").as_u64();
        k.set_coordinates(&[43]);
        let v2 = k.pull("out").as_u64();
        assert_ne!(v1, v2, "cycle-dependent node should not be folded");
    }
}
