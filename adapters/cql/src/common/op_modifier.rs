// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL universal per-op field surface.
//!
//! Common across both `scylla` and `cassandra-cpp` engines.
//! Defines the universal field name list and the factory trait
//! each engine implements to bridge from a resolved GK scope
//! value into an engine-specific
//! [`OpFieldModifier`](nbrs_activity::op_modifier::OpFieldModifier).
//!
//! See [`SRD 73`](../../../../docs/sysref/73_op_field_modifiers.md)
//! §"CQL universal field superset" for naming rationale.

use nbrs_activity::op_modifier::{ModifierChain, OpFieldModifier};
use nbrs_variates::kernel::GkKernel;
use nbrs_variates::node::Value;

/// Universal per-op field names supported by every CQL engine.
///
/// Each name is resolvable from any GK scope by calling
/// `kernel.lookup(name)`. The dispenser-initializer walks this
/// list once per op-template at `map_op` time, asking the
/// parent kernel for each name. Names that resolve become
/// modifiers; names that don't are simply absent from the
/// resulting chain.
///
/// Naming conventions documented in SRD 73 §"CQL universal
/// field superset":
///
/// - `request_timeout_ms` — explicit unit suffix; matches the
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
    "request_timeout_ms",
    "page_size",
    "cql_trace",
];

/// Engines implement this to translate a resolved GK [`Value`]
/// into a typed
/// [`OpFieldModifier`](nbrs_activity::op_modifier::OpFieldModifier).
///
/// The factory is called once per universal field that the user
/// bound in the GK scope. Returning `Ok(None)` means "this
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

/// Walk [`CQL_UNIVERSAL_FIELDS`], ask the GK scope for each name
/// via `parent.lookup`, and build a typed `ModifierChain`.
///
/// Two-phase contract per SRD 73:
///
/// - This function is called from the dispenser-initializer
///   (`DriverAdapter::map_op`) — once per op-template per phase.
/// - The returned chain is stored on the dispenser. Per-cycle
///   `execute()` then calls `chain.apply(&mut stmt)` on the
///   critical path. No further GK access happens at execute time.
///
/// The session-global trace sink is fetched once via
/// [`nbrs_activity::op_modifier::session_sink`] and attached to
/// the chain. Sessions with no sink installed produce chains
/// that fall through the no-observer hot path.
pub fn build_cql_modifier_chain<F>(
    parent: &GkKernel,
    op_label: impl Into<String>,
) -> Result<ModifierChain<F::Statement>, String>
where
    F: CqlModifierFactory,
{
    let mut active: Vec<Box<dyn OpFieldModifier<F::Statement>>> = Vec::new();
    for &field in CQL_UNIVERSAL_FIELDS {
        let Some(value) = parent.lookup(field) else {
            continue; // user did not bind this field — driver default in force
        };
        if let Some(m) = F::modifier_for(field, value)
            .map_err(|e| format!("CQL universal field '{field}': {e}"))?
        {
            active.push(m);
        }
    }
    Ok(ModifierChain::new(
        op_label,
        active,
        nbrs_activity::op_modifier::session_sink(),
    ))
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
