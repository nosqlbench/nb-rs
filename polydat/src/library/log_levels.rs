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
//! Diag emission routes through [`crate::library::support::audit`] so the
//! host's installed audit sink forwards every line to its own logger
//! alongside the rest of the run trace. With no sink installed (unit
//! tests, dryrun, pre-init) lines fall back to stderr.

use crate::ast::Value;

// SRD-80 PR B.8 — log_{debug,info,warn,error} migrated to
// `#[polydat_node]` with `Value` PolyWire args. Each node
// derives its own struct (LogDebug, LogInfo, LogWarn, LogError)
// from snake_case → PascalCase; the runtime port type is
// resolved by the assembler and passed to `new(value_type)`.

fn log_at(level: crate::library::support::audit::LogLevel, fn_name: &str, value: &Value) {
    let msg = format!("{fn_name}: {}", value.to_display_string());
    crate::library::support::audit::log(level, &msg);
}

#[crate::polydat_node(category = Diagnostic, purity = SideChannel(LogBuffer))]
fn log_debug(value: Value) -> Value {
    log_at(crate::library::support::audit::LogLevel::Debug, "log_debug", &value);
    value
}

#[crate::polydat_node(category = Diagnostic, purity = SideChannel(LogBuffer))]
fn log_info(value: Value) -> Value {
    log_at(crate::library::support::audit::LogLevel::Info, "log_info", &value);
    value
}

#[crate::polydat_node(category = Diagnostic, purity = SideChannel(LogBuffer))]
fn log_warn(value: Value) -> Value {
    log_at(crate::library::support::audit::LogLevel::Warn, "log_warn", &value);
    value
}

#[crate::polydat_node(category = Diagnostic, purity = SideChannel(LogBuffer))]
fn log_error(value: Value) -> Value {
    log_at(crate::library::support::audit::LogLevel::Error, "log_error", &value);
    value
}

// SRD-80 PR B.8 — every node in this module is registered
// link-time via the proc-macro-emitted NodeRegistration. The
// hand-maintained signatures()/build_node()/register_nodes!
// plumbing is retired.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, PortType, Slot};

    #[test]
    fn log_debug_passthrough() {
        let node = LogDebug::new(PortType::Str);
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert_eq!(out[0].as_str(), "hello");
    }

    #[test]
    fn log_info_passthrough() {
        let node = LogInfo::new(PortType::Bool);
        let mut out = [Value::None];
        node.eval(&[Value::Bool(true)], &mut out);
        assert!(out[0].as_bool());
    }

    #[test]
    fn log_warn_passthrough() {
        let node = LogWarn::new(PortType::U64);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn log_error_passthrough() {
        let node = LogError::new(PortType::F64);
        let mut out = [Value::None];
        node.eval(&[Value::F64(1.5)], &mut out);
        assert_eq!(out[0].as_f64(), 1.5);
    }

    #[test]
    fn log_node_meta_has_one_input_one_output() {
        let node = LogInfo::new(PortType::Str);
        assert_eq!(node.meta().ins.len(), 1);
        assert_eq!(node.meta().outs.len(), 1);
        assert_eq!(node.meta().name, "log_info");
    }

    #[test]
    fn log_info_meta_tracks_constructor_port_type() {
        let node = LogInfo::new(PortType::Bool);
        assert_eq!(node.meta().outs[0].typ, PortType::Bool);
        if let Slot::Wire(p) = &node.meta().ins[0] {
            assert_eq!(p.typ, PortType::Bool);
        } else {
            panic!("expected Slot::Wire");
        }
    }

    #[test]
    fn log_purity_is_side_channel() {
        use crate::ast::{Purity, SideChannelSink};
        let node = LogInfo::new(PortType::U64);
        let p = node.purity();
        assert!(matches!(p, Purity::SideChannel { sink: SideChannelSink::LogBuffer }));
    }
}
