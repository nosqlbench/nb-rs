// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The four `metricsql_*` polydat nodes. Each parses its MetricsQL
//! expression once at construction, then on every eval locates a live
//! metrics-access service ([`nmbrs_metrics::queryapi::live_access`]),
//! evaluates the expression through this crate's engine, and projects
//! the result [`Vector`] into a `Value` by result-type affinity
//! ([`super::project`]).
//!
//! | node | shape | output |
//! |---|---|---|
//! | `metricsql` | general | `Value::Json` (labeled series) |
//! | `metricsql_scalar` | scalar | `Value::F64` |
//! | `metricsql_vector` | instant vector | `Value::VecF64` |
//! | `metricsql_window` | range vector (single series) | `Value::VecF64` |
//!
//! Authored via `#[polydat::polydat_node]` (SRD-80b) — each fn's FuncSig +
//! builder are macro-generated and registered through the macro's own
//! `inventory::submit!`; polydat discovers them at link time. The query is
//! parsed once at construction via `#[poly_const(parse_query, from = query)]`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// Node metadata + registration are emitted by `#[polydat::polydat_node]`
// (fully-qualified `polydat::…` paths, incl. the `Const<…>` marker), so the
// only polydat types named here are the value carriers the bodies build.
use polydat::ast::{SliceArc, Value};

use nmbrs_metrics::queryapi::{Vector, live_access};

use super::project::{Shape, project};
use crate::ast::Expr;
use crate::eval::{EvalContext, evaluate};

/// Stale-tolerance lookback for the final instant evaluation — absorbs
/// cadence skew, matching the `nmbrs metrics query` CLI default. Inner
/// rollups (`rate(m[5m])`) carry their own window.
const INSTANT_LOOKBACK_MS: i64 = 300_000;

/// Parse a MetricsQL query once, at node construction — `#[poly_const]`
/// caches the result and the per-eval body borrows it, so the string is
/// parsed exactly once per node instance, not on every pull. The `Err`
/// is carried (not surfaced at build): a malformed query warns + returns
/// the type-appropriate empty value on first eval, matching the node's
/// long-standing warn-not-poison contract for read failures.
fn parse_query(query: &str) -> Result<Expr, String> {
    crate::parse(query).map_err(|e| format!("parse error: {e}"))
}

/// Type-appropriate empty value for a failed/empty read.
fn empty_value(shape: Shape) -> Value {
    match shape {
        Shape::Scalar => Value::F64(0.0),
        Shape::Vector | Shape::Window => Value::VecF64(SliceArc::from_vec(Vec::new())),
        Shape::General => Value::Json(Arc::new(serde_json::Value::Array(Vec::new()))),
    }
}

/// Evaluate the pre-parsed query against the live metrics service at "now"
/// and project to `shape`. Warns + returns [`empty_value`] on any failure
/// (parse error, no service, shape mismatch) — warned, not poisoned.
fn read_value(parsed: &Result<Expr, String>, label: &str, shape: Shape) -> Value {
    let attempt = || -> Result<Value, String> {
        let expr = parsed.as_ref().map_err(|e| e.clone())?;
        let service = live_access().ok_or("no live metrics service installed")?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let ctx = EvalContext {
            data: &*service,
            start_ms: now,
            end_ms: now,
            step_ms: 60_000,
            lookback_ms: Some(INSTANT_LOOKBACK_MS),
            query_start_ms: None,
            query_end_ms: None,
        };
        let series = evaluate(&ctx, expr).map_err(|e| e.to_string())?;
        if series.is_empty() {
            // SRD-89 — an empty windowed result is NO DATA, not a real 0. The
            // common cause is a `rate(metric[W])` whose lookback window holds
            // fewer than two samples (early in a phase, or under concurrent
            // execution where the shared cadence ring fills unevenly), so the
            // whole query yields zero series. Returning the shape's no-data
            // sentinel (NaN for scalar) lets a consumer distinguish "the window
            // has no samples yet" from "the value settled at 0" (a flat counter,
            // which projects a genuine 0). The optimizer settle holds on NaN
            // instead of settling on a fabricated zero — without this, windowed
            // servo objectives mis-converge under concurrency (the early empty
            // reads look like a stable 0). A non-empty-but-mis-shaped result
            // (e.g. a scalar query that returns a 2-sample series) is a query
            // error, not no-data — it falls through to the `project` error and
            // the `empty_value` default below.
            return Ok(no_data_value(shape));
        }
        project(&Vector::new(series), shape).map_err(|e| e.message)
    };
    attempt().unwrap_or_else(|e| {
        polydat::audit::warn(&format!("{label}: {e}"));
        empty_value(shape)
    })
}

/// Shape-typed **no-data** sentinel — what a syntactically-valid query that
/// matched zero data should read as (distinct from a query/parse error, which
/// uses [`empty_value`]). Scalar is `NaN` so a consumer (the optimizer settle)
/// can tell "no samples yet" from a real 0; vector/window/general reuse the
/// natural empty value (an empty result already reads as "no data" there).
fn no_data_value(shape: Shape) -> Value {
    match shape {
        Shape::Scalar => Value::F64(f64::NAN),
        Shape::Vector | Shape::Window | Shape::General => empty_value(shape),
    }
}

/// Evaluate a MetricsQL query → JSON (full labeled result).
///
/// Signature: `metricsql(query: const str) -> json`. Authored via
/// `#[polydat::polydat_node]` (SRD-80b). Intrinsically `Nondeterministic` —
/// reads the live metrics the cadence pipeline frames, so it is never
/// const-folded and re-evaluates on every pull.
#[polydat::polydat_node(
    category = Context,
    purity = Nondeterministic("reads live metrics framed by the cadence pipeline; value changes over the run"),
)]
fn metricsql(
    query: Const<&str>,
    #[poly_const(parse_query, from = query)] parsed: &Result<Expr, String>,
) -> Arc<serde_json::Value> {
    read_value(parsed, "metricsql", Shape::General)
        .as_json_arc()
        .clone()
}

/// Evaluate a MetricsQL query → f64 (single value). Intrinsically
/// `Nondeterministic` — see [`metricsql`].
#[polydat::polydat_node(
    category = Context,
    purity = Nondeterministic("reads live metrics framed by the cadence pipeline; value changes over the run"),
)]
fn metricsql_scalar(
    query: Const<&str>,
    #[poly_const(parse_query, from = query)] parsed: &Result<Expr, String>,
) -> f64 {
    read_value(parsed, "metricsql_scalar", Shape::Scalar).as_f64()
}

/// Evaluate a MetricsQL query → VecF64 (instant vector). Intrinsically
/// `Nondeterministic` — see [`metricsql`].
#[polydat::polydat_node(
    category = Context,
    purity = Nondeterministic("reads live metrics framed by the cadence pipeline; value changes over the run"),
)]
fn metricsql_vector(
    query: Const<&str>,
    #[poly_const(parse_query, from = query)] parsed: &Result<Expr, String>,
) -> Vec<f64> {
    read_value(parsed, "metricsql_vector", Shape::Vector)
        .as_vec_f64()
        .to_vec()
}

/// Evaluate a MetricsQL query → VecF64 (windowed series). Intrinsically
/// `Nondeterministic` — see [`metricsql`].
#[polydat::polydat_node(
    category = Context,
    purity = Nondeterministic("reads live metrics framed by the cadence pipeline; value changes over the run"),
)]
fn metricsql_window(
    query: Const<&str>,
    #[poly_const(parse_query, from = query)] parsed: &Result<Expr, String>,
) -> Vec<f64> {
    read_value(parsed, "metricsql_window", Shape::Window)
        .as_vec_f64()
        .to_vec()
}

#[cfg(test)]
mod tests {
    use polydat::ast::PortType;
    use polydat::dsl::compile::compile_polydat;

    #[test]
    fn each_name_compiles_to_its_shape() {
        // Each macro-authored name registers with its declared output port,
        // discoverable by the polydat compiler.
        for (decl, port) in [
            ("j := metricsql(\"up\")", PortType::Json),
            ("s := metricsql_scalar(\"up\")", PortType::F64),
            ("v := metricsql_vector(\"up\")", PortType::VecF64),
            ("w := metricsql_window(\"up\")", PortType::VecF64),
        ] {
            let wire = decl.split_once(" :=").unwrap().0;
            let k = compile_polydat(decl).unwrap_or_else(|e| panic!("compile {decl}: {e:?}"));
            assert_eq!(
                k.program().output_port_type(wire),
                Some(port),
                "wrong output port for {decl}",
            );
        }
    }

    #[test]
    fn registered_and_discoverable_through_the_polydat_compiler() {
        // The macro's inventory registration makes the node findable by name
        // when polydat compiles a program that uses it.
        let k = compile_polydat("score := metricsql_scalar(\"up\")");
        assert!(
            k.is_ok(),
            "metricsql_scalar should be a registered node: {k:?}"
        );
    }

    #[test]
    fn malformed_query_warns_and_defaults_at_eval() {
        // The query is parsed once at construction; an `Err` is carried, not
        // surfaced at build, so a malformed query compiles fine and (with no
        // live service either) warns + reads the F64 default on pull.
        let mut k = compile_polydat("score := metricsql_scalar(\"((((\")")
            .expect("malformed query still compiles (parse error carried, not a build error)");
        assert_eq!(k.pull("score").as_f64(), 0.0);
    }

    #[test]
    fn eval_without_a_service_warns_and_defaults() {
        // No live service installed in this unit test → type-appropriate
        // empty value, no panic.
        let mut k = compile_polydat("score := metricsql_scalar(\"up\")").expect("compile");
        assert_eq!(k.pull("score").as_f64(), 0.0);
    }
}
