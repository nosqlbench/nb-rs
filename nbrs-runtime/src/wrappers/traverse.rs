// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Default result-traversal wrapper. Always wraps the inner
//! adapter dispenser: counts result elements + bytes, walks
//! declared capture points, and writes extracted values onto
//! the per-fiber op-template kernel via `ctx.wires.write`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::{ExecutionError, OpDispenser, OpResult};
use nbrs_workload::bindpoints;
use crate::adapter::WrappingDispenser;
use crate::wrapper_registry::{WrapperName, WrapperRegistration};
use nbrs_workload::model::ParsedOp;

/// SRD-32a wrapper name. Innermost layer; always present.
pub const NAME: WrapperName = WrapperName::new("traverse");

/// Trigger: always — every op gets a traversal layer so result
/// bodies are consumed (and per-row metrics record element /
/// byte counts) even if the workload didn't ask for anything.
fn triggers(_: &ParsedOp) -> bool { true }

/// No per-op assignment text — the wrapper has no operator-
/// configurable knobs.
fn describe_assignment(_: &ParsedOp) -> Option<String> { None }

inventory::submit! {
    WrapperRegistration {
        name: NAME,
        owned_fields: &[],
        triggers,
        requires_inner: &[],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment,
    }
}

/// Result traversal statistics, backed by activity metrics counters.
pub struct TraversalStats {
    pub metrics: Arc<crate::activity::ActivityMetrics>,
}

/// Wraps an inner OpDispenser with result traversal and optional
/// capture extraction.
///
/// This is the default wrapper, always applied unless disabled.
/// It ensures that:
/// 1. The result body is fully consumed (element/byte counting)
/// 2. Captures are extracted from the result (if declared)
/// 3. Traversal metrics are recorded
pub struct TraversingDispenser {
    inner: Arc<dyn OpDispenser>,
    stats: Arc<TraversalStats>,
    /// Capture points parsed from the template at init time.
    /// Empty if no captures are declared.
    captures: Vec<bindpoints::CapturePoint>,
}

impl TraversingDispenser {
    /// Wrap an inner dispenser with traversal.
    ///
    /// Reads `template.captures` (the parse-time-extracted capture
    /// specs) directly. The op-template parser has already stripped
    /// `[name]` / `[@name]` brackets from the op text fields, so
    /// adapters see clean SQL/URL/body strings.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        template: &nbrs_workload::model::ParsedOp,
        stats: Arc<TraversalStats>,
    ) -> Arc<dyn OpDispenser> {
        Arc::new(Self {
            inner,
            stats,
            captures: template.captures.clone(),
        })
    }
}

/// Extract captures from a result body's JSON.
///
/// Walks each declared capture spec against the body. Two modes:
///
/// - **Single** (`[name]`): take the first matching value. For an
///   array-of-rows body shape (CQL's standard JSON form), reads
///   row[0].name. For an object body, reads top-level `name`. For
///   wildcard `*`, captures every top-level field.
/// - **Slurp** (`[@name]`): walks every row of an array-of-rows
///   body and collects each row's column into a single
///   `Value::Json(array)`. Object bodies produce a single-element
///   list. This is the convenient shape for downstream consumers
///   that need all per-row values as a list (e.g. recall
///   evaluator's `actual:` reads).
///
/// The body's `.to_json()` form is the source of truth — adapters
/// that produce typed-row data render to a JSON array of row
/// objects.
fn extract_captures_from_json(
    body: &dyn crate::adapter::ResultBody,
    specs: &[bindpoints::CapturePoint],
) -> HashMap<String, polydat::ast::Value> {
    if specs.is_empty() {
        return HashMap::new();
    }
    let json = body.to_json();
    let mut captures = HashMap::new();
    for spec in specs {
        // Declarative `capture:` block form: JSON-Pointer path
        // takes precedence over the bracket-source-name path.
        // Lets a workload address Jolokia bulk-POST responses
        // (`[{value:N}, {value:[...]}, {value:K}]`) by index +
        // nested field without re-shaping the response.
        if let Some(path) = spec.path.as_deref() {
            let sub = json.pointer(path);
            let value = if spec.count {
                polydat::ast::Value::U64(count_of_subtree(sub))
            } else {
                match sub {
                    Some(v) => json_subtree_to_value(v),
                    None => polydat::ast::Value::None,
                }
            };
            captures.insert(spec.as_name.clone(), value);
            continue;
        }
        if spec.slurp {
            // Slurp form: collect across all rows.
            let collected = slurp_column(&json, &spec.source_name);
            captures.insert(spec.as_name.clone(), polydat::ast::Value::Json(
                std::sync::Arc::new(serde_json::Value::Array(collected)),
            ));
            continue;
        }
        // Single form.
        if spec.source_name == "*" {
            // Wildcard: capture every top-level field. Falls
            // through to scalar-form per field.
            let target = match &json {
                serde_json::Value::Array(rows) => rows.first().cloned()
                    .unwrap_or(serde_json::Value::Null),
                other => other.clone(),
            };
            if let serde_json::Value::Object(map) = target {
                for (k, v) in map {
                    captures.insert(k, json_to_value(&v));
                }
            }
            continue;
        }
        if let Some(val) = first_row_field(&json, &spec.source_name) {
            captures.insert(spec.as_name.clone(), json_to_value(&val));
        }
    }
    captures
}

/// Reduce an addressed JSON sub-tree to a u64 count, mirroring
/// the polling wrapper's `count_from_json_pointer` semantics:
/// array → length, object → key count, scalar → 1 (non-empty)
/// or 0 (false / empty string / zero number), null / missing →
/// 0. Use cases: `capture: { active_count: "/value:count" }`
/// reduces a list-of-running-jobs to a numeric gauge in one
/// step.
fn count_of_subtree(v: Option<&serde_json::Value>) -> u64 {
    let Some(v) = v else { return 0 };
    match v {
        serde_json::Value::Array(a) => a.len() as u64,
        serde_json::Value::Object(m) => m.len() as u64,
        serde_json::Value::Number(n) => {
            n.as_u64()
                .or_else(|| n.as_i64().map(|i| i.max(0) as u64))
                .or_else(|| n.as_f64().map(|f| f.max(0.0) as u64))
                .unwrap_or(0)
        }
        serde_json::Value::Bool(b) => if *b { 1 } else { 0 },
        serde_json::Value::String(s) if s.is_empty() => 0,
        serde_json::Value::String(_) => 1,
        serde_json::Value::Null => 0,
    }
}

/// Project a JSON sub-tree into a Polydat [`Value`]. Scalars go
/// through [`json_to_value`]'s typed coercion; structural
/// shapes (array / object) are kept as `Value::Json` so the
/// kernel can carry the original shape without lossy
/// stringification.
fn json_subtree_to_value(v: &serde_json::Value) -> polydat::ast::Value {
    match v {
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            polydat::ast::Value::Json(std::sync::Arc::new(v.clone()))
        }
        scalar => json_to_value(scalar),
    }
}

/// First-row lookup: for an array body, read `rows[0].name`; for
/// an object body, read `obj.name`. Returns `None` when the field
/// isn't present.
fn first_row_field(json: &serde_json::Value, name: &str) -> Option<serde_json::Value> {
    match json {
        serde_json::Value::Array(rows) => rows.first().and_then(|row| row.get(name)).cloned(),
        serde_json::Value::Object(_) => json.get(name).cloned(),
        _ => None,
    }
}

/// Slurp helper: walk an array body and collect each row's `name`
/// field. Object bodies produce a single-element list. Non-object,
/// non-array bodies produce an empty list.
fn slurp_column(json: &serde_json::Value, name: &str) -> Vec<serde_json::Value> {
    match json {
        serde_json::Value::Array(rows) => rows.iter()
            .filter_map(|row| row.get(name).cloned())
            .collect(),
        serde_json::Value::Object(_) => json.get(name).map(|v| vec![v.clone()])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Convert a serde_json::Value to a Polydat Value. JSON `null`
/// maps to [`Value::None`] (per SRD-74 — None propagates
/// rather than coercing to the string `"null"`); arrays and
/// objects stringify only when reached via the row-shape
/// extraction path (the JSON-Pointer extraction route uses
/// [`json_subtree_to_value`] which preserves them as
/// `Value::Json`).
pub(crate) fn json_to_value(v: &serde_json::Value) -> polydat::ast::Value {
    match v {
        serde_json::Value::Null => polydat::ast::Value::None,
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_u64() {
                polydat::ast::Value::U64(i)
            } else if let Some(f) = n.as_f64() {
                polydat::ast::Value::F64(f)
            } else {
                polydat::ast::Value::Str(n.to_string().into())
            }
        }
        serde_json::Value::Bool(b) => polydat::ast::Value::Bool(*b),
        serde_json::Value::String(s) => polydat::ast::Value::Str(s.as_str().into()),
        other => polydat::ast::Value::Str(other.to_string().into()),
    }
}

impl WrappingDispenser for TraversingDispenser {}

impl OpDispenser for TraversingDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Execute the inner dispenser
            let result = self.inner.execute(cycle, ctx).await?;

            // Traverse: count elements and bytes
            if let Some(body) = &result.body {
                self.stats.metrics.result_elements.inc_by(body.element_count());
                if let Some(bytes) = body.byte_count() {
                    self.stats.metrics.result_bytes.inc_by(bytes);
                }
            }

            // Extract captures from result if declared. Values land
            // on the per-fiber kernel's input slot via ctx.wires.write;
            // wrappers above this layer (e.g. MetricsDispenser) see
            // them through wires.get on the same cycle.
            if !self.captures.is_empty()
                && let Some(body) = &result.body {
                    let extracted = extract_captures_from_json(body.as_ref(), &self.captures);
                    for (name, value) in extracted {
                        let _ = ctx.wires.write(&name, value);
                    }
                }

            Ok(result)
        })
    }
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::ResultBody;

    fn cap(source: &str, alias: &str, slurp: bool) -> bindpoints::CapturePoint {
        bindpoints::CapturePoint {
            source_name: source.into(),
            as_name: alias.into(),
            cast_type: None,
            slurp,
            path: None,
            count: false,
        }
    }

    #[test]
    fn parse_captures_from_template() {
        let parsed = bindpoints::parse_capture_points(
            "SELECT [username], [age as user_age] FROM users"
        );
        assert_eq!(parsed.captures.len(), 2);
        assert_eq!(parsed.captures[0].source_name, "username");
        assert_eq!(parsed.captures[0].as_name, "username");
        assert!(!parsed.captures[0].slurp);
        assert_eq!(parsed.captures[1].source_name, "age");
        assert_eq!(parsed.captures[1].as_name, "user_age");
        assert_eq!(parsed.raw_template, "SELECT username, age FROM users");
    }

    #[test]
    fn parse_slurp_capture() {
        let parsed = bindpoints::parse_capture_points("SELECT [@keys] FROM t");
        assert_eq!(parsed.captures.len(), 1);
        assert_eq!(parsed.captures[0].source_name, "keys");
        assert!(parsed.captures[0].slurp);
        assert_eq!(parsed.raw_template, "SELECT keys FROM t");
    }

    #[derive(Debug)]
    struct JsonBody(serde_json::Value);
    impl ResultBody for JsonBody {
        fn to_json(&self) -> serde_json::Value { self.0.clone() }
        fn as_any(&self) -> &dyn std::any::Any { self }
    }

    #[test]
    fn extract_from_json_top_level() {
        let body = JsonBody(serde_json::json!({
            "user_id": 42,
            "name": "alice",
            "balance": 99.5
        }));
        let specs = vec![
            cap("user_id", "uid", false),
            cap("name", "name", false),
        ];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures.len(), 2);
        assert_eq!(captures["uid"].as_u64(), 42);
        match &captures["name"] {
            polydat::ast::Value::Str(s) => assert_eq!(&**s, "alice"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn extract_wildcard() {
        let body = JsonBody(serde_json::json!({"a": 1, "b": 2}));
        let specs = vec![cap("*", "*", false)];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures.len(), 2);
    }

    #[test]
    fn extract_slurp_array_of_rows() {
        let body = JsonBody(serde_json::json!([
            {"key": 4, "value": 0.5},
            {"key": 17, "value": 0.4},
            {"key": 42, "value": 0.3},
        ]));
        let specs = vec![cap("key", "key", true)];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures.len(), 1);
        match &captures["key"] {
            polydat::ast::Value::Json(arc) => {
                let serde_json::Value::Array(items) = arc.as_ref() else {
                    panic!("expected Value::Json(array), got {arc:?}");
                };
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], serde_json::json!(4));
                assert_eq!(items[1], serde_json::json!(17));
                assert_eq!(items[2], serde_json::json!(42));
            }
            other => panic!("expected Value::Json(array), got {other:?}"),
        }
    }

    #[test]
    fn extract_single_first_row_of_array() {
        let body = JsonBody(serde_json::json!([
            {"key": 4}, {"key": 17}, {"key": 42},
        ]));
        let specs = vec![cap("key", "first_key", false)];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures.len(), 1);
        assert_eq!(captures["first_key"].as_u64(), 4);
    }

    fn cap_path(name: &str, path: &str, count: bool) -> bindpoints::CapturePoint {
        bindpoints::CapturePoint {
            source_name: name.into(),
            as_name: name.into(),
            cast_type: None,
            slurp: false,
            path: Some(path.into()),
            count,
        }
    }

    #[test]
    fn extract_json_pointer_scalar_from_bulk_response() {
        let body = JsonBody(serde_json::json!([
            {"value": 7,       "status": 200},
            {"value": [],      "status": 200},
            {"value": 0,       "status": 200},
        ]));
        let specs = vec![
            cap_path("sstables",       "/0/value", false),
            cap_path("pending_for_cf", "/2/value", false),
        ];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures["sstables"].as_u64(), 7);
        assert_eq!(captures["pending_for_cf"].as_u64(), 0);
    }

    #[test]
    fn extract_json_pointer_resolved_null_yields_none_not_string() {
        let body = JsonBody(serde_json::json!([
            {"value": null, "status": 200},
        ]));
        let specs = vec![cap_path("pending_for_cf", "/0/value", false)];
        let captures = extract_captures_from_json(&body, &specs);
        assert!(matches!(captures["pending_for_cf"],
            polydat::ast::Value::None),
            "JSON null at path should yield Value::None, got {:?}",
            captures["pending_for_cf"],
        );
    }

    #[test]
    fn extract_json_pointer_count_on_resolved_null_returns_zero() {
        let body = JsonBody(serde_json::json!([
            {"value": null, "status": 200},
        ]));
        let specs = vec![cap_path("pending_for_cf", "/0/value", true)];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures["pending_for_cf"].as_u64(), 0,
            "`:count` on resolved-null should return 0, got {:?}",
            captures["pending_for_cf"],
        );
    }

    #[test]
    fn extract_json_pointer_count_collapses_array_to_length() {
        let body = JsonBody(serde_json::json!([
            {"value": 7},
            {"value": [
                {"compactionId":"a", "keyspace":"ks", "columnfamily":"cf"},
                {"compactionId":"b", "keyspace":"ks", "columnfamily":"cf"},
            ]},
        ]));
        let specs = vec![cap_path("active_count", "/1/value", true)];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures["active_count"].as_u64(), 2);
    }

    #[test]
    fn extract_json_pointer_missing_path_yields_none() {
        let body = JsonBody(serde_json::json!({"value": 7}));
        let specs = vec![cap_path("not_there", "/missing/path", false)];
        let captures = extract_captures_from_json(&body, &specs);
        assert!(matches!(captures["not_there"], polydat::ast::Value::None),
            "expected Value::None for unresolvable JSON-Pointer, got {:?}",
            captures["not_there"],
        );
    }

    #[test]
    fn extract_json_pointer_count_of_missing_path_is_zero() {
        let body = JsonBody(serde_json::json!({"value": 7}));
        let specs = vec![cap_path("active_count", "/missing/value", true)];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures["active_count"].as_u64(), 0);
    }

    #[test]
    fn extract_json_pointer_structural_sub_tree_captured_as_json() {
        let body = JsonBody(serde_json::json!([
            {"value": {"keyspace": "ks", "table": "cf", "ssTables": 3}},
        ]));
        let specs = vec![cap_path("state", "/0/value", false)];
        let captures = extract_captures_from_json(&body, &specs);
        match &captures["state"] {
            polydat::ast::Value::Json(arc) => {
                assert_eq!(arc.get("keyspace").and_then(|v| v.as_str()),
                    Some("ks"));
                assert_eq!(arc.get("ssTables").and_then(|v| v.as_u64()),
                    Some(3));
            }
            other => panic!("expected Value::Json, got {other:?}"),
        }
        // Silence dead-code warnings if helpers are unused in some subset
        let _ = count_of_subtree;
        let _ = json_subtree_to_value;
    }
}
