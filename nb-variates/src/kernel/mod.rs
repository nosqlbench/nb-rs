// Copyright 2024-2026 Jonathan Shook
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

mod program;
mod engines;
mod capture;
mod gkkernel;

pub use program::*;
pub use engines::*;
pub use capture::*;
pub use gkkernel::*;

use crate::node::Value;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

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
        let _program = k.into_program();

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
