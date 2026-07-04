// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL universal per-op field surface.
//!
//! Common across both `scylla` and `cassandra-cpp` engines.
//! Defines the universal field name list and the factory trait
//! each engine implements to bridge from a resolved Polydat scope
//! value into an engine-specific
//! [`OpFieldModifier`](nbrs_runtime::op_modifier::OpFieldModifier).
//!
//! See [`SRD 73`](../../../../docs/SRD/73_op_field_modifiers.md)
//! §"CQL universal field superset" for naming rationale.

use nbrs_runtime::op_modifier::{ModifierChain, OpFieldModifier};
use polydat::kernel::PolydatKernel;
use polydat::ast::Value;

/// Universal per-op field names supported by every CQL engine.
///
/// Each name is resolvable from any Polydat scope by calling
/// `kernel.lookup(name)`. The dispenser-initializer walks this
/// list once per op-template at `map_op` time, asking the
/// parent kernel for each name. Names that resolve become
/// modifiers; names that don't are simply absent from the
/// resulting chain.
///
/// Naming conventions documented in SRD 73 §"CQL universal
/// field superset":
///
/// - `timeout` — the canonical per-op request timeout, given as a duration
///   spec-string (`60s`, `500ms`, `1h30m`) OR a bare number meaning
///   fractional seconds (`60`, `60.5`). Normalised to the per-statement
///   request timeout uniformly across engines (see
///   [`build_cql_modifier_chain`]); WINS over `request_timeout_ms` when both
///   are bound.
/// - `request_timeout_ms` — explicit-milliseconds escape hatch; matches the
///   existing session-level workload-param name.
/// - `page_size` — CQL-spec spelling.
/// - `consistency` / `serial_consistency` — full words; no
///   `cl`/`scl` shorthands at this surface.
/// - `cql_trace` — the **CQL query-tracing subsystem** (rows
///   written to `system_traces.*` on the cluster). Orthogonal
///   to the Rust `tracing` crate's log severity (which nb-rs
///   doesn't use anyway) and to nb-rs event-log emissions.
///   See SRD 73 §"Tracing terminology".
pub const CQL_UNIVERSAL_FIELDS: &[&str] = &[
    "consistency",
    "serial_consistency",
    "timeout",
    "request_timeout_ms",
    "page_size",
    "cql_trace",
];

/// Engines implement this to translate a resolved Polydat [`Value`]
/// into a typed
/// [`OpFieldModifier`](nbrs_runtime::op_modifier::OpFieldModifier).
///
/// The factory is called once per universal field that the user
/// bound in the Polydat scope. Returning `Ok(None)` means "this
/// engine doesn't support this field yet" (deferred per
/// SRD 73); the universal field surface skips it silently. A
/// shape mismatch (e.g. wrong `Value` variant) returns `Err`
/// with an actionable message.
pub trait CqlModifierFactory {
    /// The engine's per-statement type. For `scylla` this is
    /// `PreparedStatement` (and the same modifier impls apply
    /// to unprepared statements via the engine's enum); for
    /// `cassandra-cpp` it is the engine's `Statement`.
    type Statement: 'static;

    /// Build a modifier for a single field. The field name is
    /// guaranteed to be one of [`CQL_UNIVERSAL_FIELDS`].
    fn modifier_for(
        field: &'static str,
        value: Value,
    ) -> Result<Option<Box<dyn OpFieldModifier<Self::Statement>>>, String>;
}

/// Walk [`CQL_UNIVERSAL_FIELDS`], ask the Polydat scope for each name
/// via `parent.lookup`, and build a typed `ModifierChain`.
///
/// Two-phase contract per SRD 73:
///
/// - This function is called from the dispenser-initializer
///   (`DriverAdapter::map_op`) — once per op-template per phase.
/// - The returned chain is stored on the dispenser. Per-cycle
///   `execute()` then calls `chain.apply(&mut stmt)` on the
///   critical path. No further Polydat access happens at execute time.
///
/// The session-global trace sink is fetched once via
/// [`nbrs_runtime::op_modifier::session_sink`] and attached to
/// the chain. Sessions with no sink installed produce chains
/// that fall through the no-observer hot path.
pub fn build_cql_modifier_chain<F>(
    parent: &PolydatKernel,
    op_label: impl Into<String>,
) -> Result<ModifierChain<F::Statement>, String>
where
    F: CqlModifierFactory,
{
    let mut active: Vec<Box<dyn OpFieldModifier<F::Statement>>> = Vec::new();

    // `timeout` is the canonical request-timeout knob: a duration spec-string
    // or bare fractional-seconds. It normalises to `request_timeout_ms` in
    // milliseconds RIGHT HERE (parse once, shared) and reuses each engine's
    // existing request-timeout modifier — so cassandra-cpp and scylla behave
    // identically with no per-engine parsing. When set it WINS over an
    // explicit `request_timeout_ms` (the ms escape hatch).
    let timeout_ms: Option<u64> = match parent.lookup("timeout") {
        Some(v) => Some(cql_timeout_value_to_ms(&v)
            .map_err(|e| format!("CQL universal field 'timeout': {e}"))?),
        None => None,
    };

    for &field in CQL_UNIVERSAL_FIELDS {
        // `timeout` is normalised below, not a modifier of its own.
        if field == "timeout" { continue; }
        // A bound `timeout` overrides an explicit `request_timeout_ms`.
        if field == "request_timeout_ms" && timeout_ms.is_some() { continue; }
        let Some(value) = parent.lookup(field) else {
            continue; // user did not bind this field — driver default in force
        };
        if let Some(m) = F::modifier_for(field, value)
            .map_err(|e| format!("CQL universal field '{field}': {e}"))?
        {
            active.push(m);
        }
    }

    // Emit the request-timeout modifier normalised from `timeout` (if any),
    // via the engine's existing `request_timeout_ms` path.
    if let Some(ms) = timeout_ms
        && let Some(m) = F::modifier_for("request_timeout_ms", Value::U64(ms))
            .map_err(|e| format!("CQL universal field 'timeout': {e}"))?
    {
        active.push(m);
    }

    Ok(ModifierChain::new(
        op_label,
        active,
        nbrs_runtime::op_modifier::session_sink(),
    ))
}

/// Convert a bound `timeout` [`Value`] to whole milliseconds.
///
/// A **string** is parsed as a duration spec / bare fractional-seconds
/// (`60s`, `500ms`, `1h30m`, `60.5`) via
/// [`nbrs_runtime::timeval::parse_time_ms`]. A **numeric** value is treated as
/// fractional SECONDS (`60` → 60000 ms, `60.5` → 60500 ms) — the same
/// convention as the bare-number string form.
pub fn cql_timeout_value_to_ms(value: &Value) -> Result<u64, String> {
    match value {
        Value::Str(s) => nbrs_runtime::timeval::parse_time_ms(s),
        Value::U64(n) => Ok(n.saturating_mul(1000)),
        Value::I64(n) if *n >= 0 => Ok((*n as u64).saturating_mul(1000)),
        Value::F64(f) if f.is_finite() && *f >= 0.0 => Ok((f * 1000.0).round() as u64),
        other => Err(format!(
            "expected a duration string (e.g. '60s') or a non-negative number \
             of seconds, got {other:?}")),
    }
}

/// Parse a consistency-level string into the engine-agnostic
/// [`crate::common::CqlConsistency`] enum. Shared between
/// engines so the validation error message is uniform.
pub fn parse_consistency(s: &str) -> Result<crate::common::CqlConsistency, String> {
    crate::common::CqlConsistency::parse(s).ok_or_else(|| {
        format!(
            "unrecognized consistency level '{s}'. Valid: {}",
            crate::common::CqlConsistency::valid_names().join(", ")
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn universal_fields_is_nonempty_and_unique() {
        assert!(!CQL_UNIVERSAL_FIELDS.is_empty());
        let mut sorted: Vec<&str> = CQL_UNIVERSAL_FIELDS.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), CQL_UNIVERSAL_FIELDS.len(), "duplicate field names");
    }

    #[test]
    fn timeout_value_to_ms_accepts_all_forms() {
        // Duration spec-string.
        assert_eq!(cql_timeout_value_to_ms(&Value::Str("60s".into())).unwrap(), 60_000);
        assert_eq!(cql_timeout_value_to_ms(&Value::Str("500ms".into())).unwrap(), 500);
        assert_eq!(cql_timeout_value_to_ms(&Value::Str("1h30m".into())).unwrap(), 5_400_000);
        // Bare number string = fractional seconds.
        assert_eq!(cql_timeout_value_to_ms(&Value::Str("60".into())).unwrap(), 60_000);
        assert_eq!(cql_timeout_value_to_ms(&Value::Str("60.5".into())).unwrap(), 60_500);
        // Numeric values = fractional seconds (same convention).
        assert_eq!(cql_timeout_value_to_ms(&Value::U64(60)).unwrap(), 60_000);
        assert_eq!(cql_timeout_value_to_ms(&Value::F64(60.5)).unwrap(), 60_500);
        assert_eq!(cql_timeout_value_to_ms(&Value::F64(0.25)).unwrap(), 250);
        // Garbage rejected.
        assert!(cql_timeout_value_to_ms(&Value::Str("nope".into())).is_err());
        assert!(cql_timeout_value_to_ms(&Value::Bool(true)).is_err());
    }

    #[test]
    fn timeout_is_a_universal_field() {
        assert!(CQL_UNIVERSAL_FIELDS.contains(&"timeout"));
    }

    #[test]
    fn parse_consistency_accepts_canonical_names() {
        assert!(parse_consistency("LOCAL_ONE").is_ok());
        assert!(parse_consistency("ONE").is_ok());
        assert!(parse_consistency("QUORUM").is_ok());
        assert!(parse_consistency("LOCAL_QUORUM").is_ok());
    }

    #[test]
    fn parse_consistency_rejects_garbage() {
        let err = parse_consistency("NOT_A_LEVEL").unwrap_err();
        assert!(err.contains("unrecognized"));
        assert!(err.contains("NOT_A_LEVEL"));
        assert!(err.contains("LOCAL_ONE"), "error should list valid names: {err}");
    }
}
