// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! cassandra-cpp-engine per-statement modifier impls for the
//! universal CQL field surface (SRD 73).
//!
//! Each modifier captures the resolved GK Value at dispenser-
//! initializer time and applies it via the engine's per-statement
//! setter at execute time. Target type is `cassandra_cpp::Statement`.
//!
//! Setter naming differs from scylla in two places — this module
//! is the bridge:
//!
//! - `request_timeout_ms` → `set_statement_request_timeout`
//!   (cassandra-cpp uses the `statement_` prefix; scylla just
//!   `request_timeout`).
//! - `page_size` → `set_paging_size` (cassandra-cpp uses "paging";
//!   scylla uses "page").
//!
//! The universal-field names match scylla — the difference is
//! invisible to workload authors.

use std::time::Duration;

use cassandra_cpp as cass;
use nbrs_activity::op_modifier::OpFieldModifier;
use nbrs_variates::node::Value;

use crate::common::op_modifier::{CqlModifierFactory, parse_consistency};
use crate::common::CqlConsistency;

// =========================================================================
// Per-field modifier impls
// =========================================================================

struct ConsistencyMod {
    consistency: cass::Consistency,
    display: &'static str,
}

impl OpFieldModifier<cass::Statement> for ConsistencyMod {
    fn field_name(&self) -> &'static str { "consistency" }
    fn apply(&self, s: &mut cass::Statement) {
        let _ = s.set_consistency(self.consistency);
    }
    fn diagnostic_value(&self) -> serde_json::Value {
        serde_json::Value::String(self.display.to_string())
    }
}

struct SerialConsistencyMod {
    serial: cass::Consistency,
    display: &'static str,
}

impl OpFieldModifier<cass::Statement> for SerialConsistencyMod {
    fn field_name(&self) -> &'static str { "serial_consistency" }
    fn apply(&self, s: &mut cass::Statement) {
        let _ = s.set_serial_consistency(self.serial);
    }
    fn diagnostic_value(&self) -> serde_json::Value {
        serde_json::Value::String(self.display.to_string())
    }
}

struct RequestTimeoutMod {
    timeout: Duration,
}

impl OpFieldModifier<cass::Statement> for RequestTimeoutMod {
    fn field_name(&self) -> &'static str { "request_timeout_ms" }
    fn apply(&self, s: &mut cass::Statement) {
        s.set_statement_request_timeout(Some(self.timeout));
    }
    fn diagnostic_value(&self) -> serde_json::Value {
        serde_json::Value::from(self.timeout.as_millis() as u64)
    }
}

struct PageSizeMod {
    page_size: i32,
}

impl OpFieldModifier<cass::Statement> for PageSizeMod {
    fn field_name(&self) -> &'static str { "page_size" }
    fn apply(&self, s: &mut cass::Statement) {
        let _ = s.set_paging_size(self.page_size);
    }
    fn diagnostic_value(&self) -> serde_json::Value {
        serde_json::Value::from(self.page_size as i64)
    }
}

struct CqlTraceMod {
    tracing: bool,
}

impl OpFieldModifier<cass::Statement> for CqlTraceMod {
    fn field_name(&self) -> &'static str { "cql_trace" }
    fn apply(&self, s: &mut cass::Statement) {
        let _ = s.set_tracing(self.tracing);
    }
    fn diagnostic_value(&self) -> serde_json::Value {
        serde_json::Value::Bool(self.tracing)
    }
}

// =========================================================================
// Factory
// =========================================================================

/// cassandra-cpp engine factory. Targets `cass::Statement` —
/// the engine's single per-statement type used by raw, prepared,
/// and batch op dispatch paths.
pub struct CassModifierFactory;

impl CqlModifierFactory for CassModifierFactory {
    type Statement = cass::Statement;

    fn modifier_for(
        field: &'static str,
        value: Value,
    ) -> Result<Option<Box<dyn OpFieldModifier<cass::Statement>>>, String> {
        match field {
            "consistency" => {
                let s = match &value {
                    Value::Str(s) => s.as_ref(),
                    other => return Err(format!("expected str, got {:?}", other.port_type())),
                };
                let cl = parse_consistency(s)?;
                Ok(Some(Box::new(ConsistencyMod {
                    consistency: cql_to_cass(cl),
                    display: cql_consistency_display(cl),
                })))
            }
            "serial_consistency" => {
                let s = match &value {
                    Value::Str(s) => s.as_ref(),
                    other => return Err(format!("expected str, got {:?}", other.port_type())),
                };
                let (serial, display) = parse_serial_consistency_cass(s)?;
                Ok(Some(Box::new(SerialConsistencyMod { serial, display })))
            }
            "request_timeout_ms" => {
                let ms = match &value {
                    Value::U64(v) => *v,
                    other => return Err(format!("expected u64, got {:?}", other.port_type())),
                };
                Ok(Some(Box::new(RequestTimeoutMod {
                    timeout: Duration::from_millis(ms),
                })))
            }
            "page_size" => {
                let n = match &value {
                    Value::U64(v) => i32::try_from(*v)
                        .map_err(|_| format!("page_size {v} does not fit in i32"))?,
                    other => return Err(format!("expected u64, got {:?}", other.port_type())),
                };
                if n <= 0 {
                    return Err(format!("page_size must be positive, got {n}"));
                }
                Ok(Some(Box::new(PageSizeMod { page_size: n })))
            }
            "cql_trace" => {
                let b = match &value {
                    Value::Bool(v) => *v,
                    other => return Err(format!("expected bool, got {:?}", other.port_type())),
                };
                Ok(Some(Box::new(CqlTraceMod { tracing: b })))
            }
            _ => Err(format!("unknown universal field '{field}'")),
        }
    }
}

// =========================================================================
// Helpers
// =========================================================================

fn cql_to_cass(c: CqlConsistency) -> cass::Consistency {
    // Mirrors `to_cass_consistency` in cassandra_cpp/mod.rs — kept
    // local to this module so the universal-field code path is
    // self-contained.
    match c {
        CqlConsistency::Any         => cass::Consistency::ANY,
        CqlConsistency::One         => cass::Consistency::ONE,
        CqlConsistency::Two         => cass::Consistency::TWO,
        CqlConsistency::Three       => cass::Consistency::THREE,
        CqlConsistency::Quorum      => cass::Consistency::QUORUM,
        CqlConsistency::All         => cass::Consistency::ALL,
        CqlConsistency::LocalQuorum => cass::Consistency::LOCAL_QUORUM,
        CqlConsistency::EachQuorum  => cass::Consistency::EACH_QUORUM,
        CqlConsistency::LocalOne    => cass::Consistency::LOCAL_ONE,
    }
}

fn cql_consistency_display(c: CqlConsistency) -> &'static str {
    match c {
        CqlConsistency::Any         => "ANY",
        CqlConsistency::One         => "ONE",
        CqlConsistency::Two         => "TWO",
        CqlConsistency::Three       => "THREE",
        CqlConsistency::Quorum      => "QUORUM",
        CqlConsistency::All         => "ALL",
        CqlConsistency::LocalQuorum => "LOCAL_QUORUM",
        CqlConsistency::EachQuorum  => "EACH_QUORUM",
        CqlConsistency::LocalOne    => "LOCAL_ONE",
    }
}

/// cassandra-cpp uses a single `Consistency` enum for both normal
/// and serial consistency; `SERIAL` / `LOCAL_SERIAL` are variants
/// of the same enum (unlike scylla's split into `Consistency` +
/// `SerialConsistency`).
fn parse_serial_consistency_cass(s: &str) -> Result<(cass::Consistency, &'static str), String> {
    match s.to_uppercase().as_str() {
        "SERIAL"       => Ok((cass::Consistency::SERIAL, "SERIAL")),
        "LOCAL_SERIAL" => Ok((cass::Consistency::LOCAL_SERIAL, "LOCAL_SERIAL")),
        _ => Err(format!(
            "unrecognized serial_consistency '{s}'. Valid: SERIAL, LOCAL_SERIAL"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_err<T>(r: Result<T, String>) -> String {
        match r {
            Ok(_) => panic!("expected Err, got Ok"),
            Err(e) => e,
        }
    }

    #[test]
    fn request_timeout_modifier_builds() {
        let m = CassModifierFactory::modifier_for(
            "request_timeout_ms",
            Value::U64(300_000),
        ).unwrap().unwrap();
        assert_eq!(m.field_name(), "request_timeout_ms");
        assert_eq!(m.diagnostic_value(), serde_json::json!(300_000u64));
    }

    #[test]
    fn consistency_modifier_builds() {
        let m = CassModifierFactory::modifier_for(
            "consistency",
            Value::Str(std::sync::Arc::from("LOCAL_QUORUM")),
        ).unwrap().unwrap();
        assert_eq!(m.diagnostic_value(), serde_json::json!("LOCAL_QUORUM"));
    }

    #[test]
    fn page_size_rejects_zero() {
        let err = expect_err(CassModifierFactory::modifier_for(
            "page_size", Value::U64(0),
        ));
        assert!(err.contains("positive"), "{err}");
    }

    #[test]
    fn cql_trace_accepts_bool() {
        let m = CassModifierFactory::modifier_for(
            "cql_trace", Value::Bool(false),
        ).unwrap().unwrap();
        assert_eq!(m.diagnostic_value(), serde_json::json!(false));
    }

    #[test]
    fn serial_consistency_parses_levels() {
        let m = CassModifierFactory::modifier_for(
            "serial_consistency",
            Value::Str(std::sync::Arc::from("SERIAL")),
        ).unwrap().unwrap();
        assert_eq!(m.diagnostic_value(), serde_json::json!("SERIAL"));
    }
}
