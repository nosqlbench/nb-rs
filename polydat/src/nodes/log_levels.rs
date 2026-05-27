// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `log_debug` / `log_info` / `log_warn` / `log_error` — pass-through
//! logging node functions (SRD-66 §"Surface 5").
//!
//! Each takes one wire input, emits a single diag line at the named
//! level containing the value's display form, and returns the input
//! unchanged. The pass-through return value lets workloads insert
//! logging into a binding chain without restructuring:
//!
//! ```yaml
//! result: |
//!   has_sai := log_info(regex_match(body, "..."))
//! ```
//!
//! Probe phases run rarely and gate downstream dispatch — surfacing
//! the detected facts at session start without a custom readout is
//! the load-bearing use case.
//!
//! Diag emission routes through [`crate::audit`] so the activity
//! runner's installed sink forwards every line to
//! `nbrs_activity::observer::log` and into `session.log` alongside
//! the rest of the run trace. With no sink installed (unit tests,
//! dryrun, pre-init) lines fall back to stderr.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

/// One of the four supported log levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn func_name(self) -> &'static str {
        match self {
            LogLevel::Debug => "log_debug",
            LogLevel::Info => "log_info",
            LogLevel::Warn => "log_warn",
            LogLevel::Error => "log_error",
        }
    }
}

/// Pass-through logger node. Construction wires through whatever
/// `PortType` the input declares (defaulting to Str — the most common
/// case for probe-phase result wires). Eval emits one diag line via
/// [`crate::audit`] and returns the value unchanged.
pub struct LogPassthrough {
    meta: NodeMeta,
    level: LogLevel,
}

impl LogPassthrough {
    /// Construct a log passthrough node typed for `port_type`.
    /// Both input and output ports take this type — the eval
    /// clones the input through, so the runtime value retains
    /// its variant. The registry's `OutputType::SameAsInput(0)`
    /// declaration drives the build dispatch to call this with
    /// the resolved input wire's port type, so the wire-level
    /// type contract matches what flows through at runtime.
    pub fn new(level: LogLevel, port_type: PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: level.func_name().into(),
                outs: vec![Port::new("output", port_type)],
                ins: vec![Slot::Wire(Port::new("value", port_type))],
            },
            level,
        }
    }
}

impl GkNode for LogPassthrough {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        // Route through `crate::audit` so the activity runner's
        // installed sink forwards the line to `observer::log` and
        // it lands in `session.log` alongside every other run-level
        // entry. Falls back to stderr when no sink is installed
        // (unit tests, dryrun, pre-init).
        let msg = format!(
            "{func}: {value}",
            func = self.level.func_name(),
            value = inputs[0].to_display_string()
        );
        let audit_level = match self.level {
            LogLevel::Debug => crate::audit::LogLevel::Debug,
            LogLevel::Info  => crate::audit::LogLevel::Info,
            LogLevel::Warn  => crate::audit::LogLevel::Warn,
            LogLevel::Error => crate::audit::LogLevel::Error,
        };
        crate::audit::log(audit_level, &msg);
        outputs[0] = inputs[0].clone();
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

const LOG_PARAMS: &[ParamSpec] = &[ParamSpec {
    name: "value",
    slot_type: SlotType::Wire,
    required: true,
    example: "cycle",
    constraint: None,
}];

pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "log_debug",
            category: C::Diagnostic,
            outputs: 1,
            description: "log value at debug level; pass-through return",
            help: "log_debug(value) -> value\n\
                   \n\
                   Emit one diag line at debug level containing the value's\n\
                   display form, then return the value unchanged. Use to\n\
                   surface a wire's runtime value without restructuring an\n\
                   expression. Logging level filters apply: log_debug lines\n\
                   drop when the runtime threshold is Info or higher.",
            identity: None,
            variadic_ctor: None,
            params: LOG_PARAMS,
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::SameAsInput(0),
        },
        FuncSig {
            name: "log_info",
            category: C::Diagnostic,
            outputs: 1,
            description: "log value at info level; pass-through return",
            help: "log_info(value) -> value\n\
                   \n\
                   Emit one diag line at info level containing the value's\n\
                   display form, then return the value unchanged. Common\n\
                   on probe-phase result wires so detected facts surface\n\
                   at session start without a custom readout.",
            identity: None,
            variadic_ctor: None,
            params: LOG_PARAMS,
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::SameAsInput(0),
        },
        FuncSig {
            name: "log_warn",
            category: C::Diagnostic,
            outputs: 1,
            description: "log value at warn level; pass-through return",
            help: "log_warn(value) -> value\n\
                   \n\
                   Emit one diag line at warn level containing the value's\n\
                   display form, then return the value unchanged.",
            identity: None,
            variadic_ctor: None,
            params: LOG_PARAMS,
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::SameAsInput(0),
        },
        FuncSig {
            name: "log_error",
            category: C::Diagnostic,
            outputs: 1,
            description: "log value at error level; pass-through return",
            help: "log_error(value) -> value\n\
                   \n\
                   Emit one diag line at error level containing the value's\n\
                   display form, then return the value unchanged.",
            identity: None,
            variadic_ctor: None,
            params: LOG_PARAMS,
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::SameAsInput(0),
        },
    ]
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef],
    wire_types: &[crate::node::PortType],
    _consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    let level = match name {
        "log_debug" => LogLevel::Debug,
        "log_info" => LogLevel::Info,
        "log_warn" => LogLevel::Warn,
        "log_error" => LogLevel::Error,
        _ => return None,
    };
    // SRD: per FuncSig.output_type = SameAsInput(0), the
    // output port type tracks the input wire's type. The
    // dispatch path passes the resolved input wire type via
    // `wire_types[0]`. Default to U64 if the wire is unresolved
    // (forward reference, dangling — rare, surfaces upstream as
    // a separate compile error).
    let typ = wire_types.first().copied().unwrap_or(PortType::U64);
    Some(Ok(Box::new(LogPassthrough::new(level, typ))))
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;

    /// Each level passes through the input value verbatim. Stderr
    /// emission is verified at the assembled-graph layer; here we
    /// confirm the eval contract — value in == value out.
    fn run(level: LogLevel, port_type: PortType, input: Value) -> Value {
        let node = LogPassthrough::new(level, port_type);
        let mut out = [Value::None];
        node.eval(&[input], &mut out);
        out.into_iter().next().unwrap()
    }

    #[test]
    fn log_debug_passthrough() {
        let v = run(LogLevel::Debug, PortType::Str, Value::Str("hello".into()));
        assert_eq!(v.as_str(), "hello");
    }

    #[test]
    fn log_info_passthrough() {
        let v = run(LogLevel::Info, PortType::Bool, Value::Bool(true));
        assert!(v.as_bool());
    }

    #[test]
    fn log_warn_passthrough() {
        let v = run(LogLevel::Warn, PortType::U64, Value::U64(42));
        assert_eq!(v.as_u64(), 42);
    }

    #[test]
    fn log_error_passthrough() {
        let v = run(LogLevel::Error, PortType::F64, Value::F64(1.5));
        assert_eq!(v.as_f64(), 1.5);
    }

    #[test]
    fn log_node_meta_has_one_input_one_output() {
        let node = LogPassthrough::new(LogLevel::Info, PortType::Str);
        assert_eq!(node.meta().ins.len(), 1);
        assert_eq!(node.meta().outs.len(), 1);
        assert_eq!(node.meta().name, "log_info");
    }

    #[test]
    fn log_info_meta_tracks_constructor_port_type() {
        // SRD: log_* declare OutputType::SameAsInput(0). The
        // constructor takes a port_type and both input and
        // output ports carry it. Verify the constructor honours
        // the contract — passing Bool produces Bool ports.
        let node = LogPassthrough::new(LogLevel::Info, PortType::Bool);
        assert_eq!(node.meta().outs[0].typ, PortType::Bool);
        match &node.meta().ins[0] {
            Slot::Wire(p) => assert_eq!(p.typ, PortType::Bool),
            other => panic!("expected Slot::Wire, got {other:?}"),
        }
    }

    #[test]
    fn build_node_recognises_all_four_levels() {
        for name in &["log_debug", "log_info", "log_warn", "log_error"] {
            let result = build_node(name, &[], &[], &[]);
            let node = result
                .unwrap_or_else(|| panic!("name {name} not handled"))
                .unwrap_or_else(|e| panic!("build failed for {name}: {e}"));
            assert_eq!(node.meta().name, *name);
        }
    }
}
