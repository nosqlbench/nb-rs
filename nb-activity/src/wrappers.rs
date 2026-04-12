// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Composable op dispenser wrappers.
//!
//! These decorators wrap an inner `OpDispenser` to add cross-cutting
//! behaviors: result traversal, capture extraction, assertions, etc.
//! The op synthesis pipeline selects wrappers at init time based on
//! the template's declarations.

use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::{
    ExecutionError, OpDispenser, OpResult, ResolvedFields,
};
use nb_workload::bindpoints;

/// Result traversal statistics, backed by activity metrics counters.
pub struct TraversalStats {
    pub metrics: Arc<super::activity::ActivityMetrics>,
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
    captures: Vec<CaptureSpec>,
}

/// A single capture point to extract from the result.
struct CaptureSpec {
    /// Field name to look up in the result (JSON path).
    source: String,
    /// Name to store the captured value under.
    alias: String,
}

impl TraversingDispenser {
    /// Wrap an inner dispenser with traversal.
    ///
    /// If the template has capture points (`[name]` syntax in any
    /// string field), they are parsed and the traverser will extract
    /// those fields from the result's JSON representation.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        template: &nb_workload::model::ParsedOp,
        stats: Arc<TraversalStats>,
    ) -> Arc<dyn OpDispenser> {
        let captures = parse_template_captures(template);
        Arc::new(Self { inner, stats, captures })
    }
}

/// Parse capture points from all string fields in a template.
fn parse_template_captures(template: &nb_workload::model::ParsedOp) -> Vec<CaptureSpec> {
    let mut captures = Vec::new();
    for value in template.op.values() {
        if let serde_json::Value::String(s) = value {
            let result = bindpoints::parse_capture_points(s);
            for cp in result.captures {
                captures.push(CaptureSpec {
                    source: cp.source_name,
                    alias: cp.as_name,
                });
            }
        }
    }
    captures
}

/// Extract captures from a result body's JSON using simple field lookup.
///
/// This is the naive fallback: serialize to JSON, look up top-level fields.
/// Adapters that want better performance can implement native extraction.
fn extract_captures_from_json(
    body: &dyn crate::adapter::ResultBody,
    specs: &[CaptureSpec],
) -> HashMap<String, nb_variates::node::Value> {
    if specs.is_empty() {
        return HashMap::new();
    }
    let json = body.to_json();
    let mut captures = HashMap::new();
    for spec in specs {
        // Try top-level field lookup
        if let Some(val) = json.get(&spec.source) {
            let value = json_to_value(val);
            captures.insert(spec.alias.clone(), value);
        } else if spec.source == "*" {
            // Wildcard: capture all top-level fields
            if let serde_json::Value::Object(map) = &json {
                for (k, v) in map {
                    captures.insert(k.clone(), json_to_value(v));
                }
            }
        }
        // TODO: support dotted paths like "rows.0.user_id" via
        // json pointer syntax for nested results
    }
    captures
}

/// Convert a serde_json::Value to a GK Value.
fn json_to_value(v: &serde_json::Value) -> nb_variates::node::Value {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_u64() {
                nb_variates::node::Value::U64(i)
            } else if let Some(f) = n.as_f64() {
                nb_variates::node::Value::F64(f)
            } else {
                nb_variates::node::Value::Str(n.to_string())
            }
        }
        serde_json::Value::Bool(b) => nb_variates::node::Value::Bool(*b),
        serde_json::Value::String(s) => nb_variates::node::Value::Str(s.clone()),
        other => nb_variates::node::Value::Str(other.to_string()),
    }
}

impl OpDispenser for TraversingDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Execute the inner dispenser
            let mut result = self.inner.execute(cycle, fields).await?;

            // Traverse: count elements and bytes
            if let Some(body) = &result.body {
                self.stats.metrics.result_elements.inc_by(body.element_count());
                if let Some(bytes) = body.byte_count() {
                    self.stats.metrics.result_bytes.inc_by(bytes);
                }
            }

            // Extract captures from result if declared
            if !self.captures.is_empty()
                && let Some(body) = &result.body {
                    let extracted = extract_captures_from_json(body.as_ref(), &self.captures);
                    for (name, value) in extracted {
                        result.captures.insert(name, value);
                    }
                }

            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{TextBody, ResultBody};

    #[test]
    fn parse_captures_from_template() {
        let mut template = nb_workload::model::ParsedOp::simple("test", "SELECT [username], [age as user_age] FROM users");
        let captures = parse_template_captures(&template);
        assert_eq!(captures.len(), 2);
        assert_eq!(captures[0].source, "username");
        assert_eq!(captures[0].alias, "username");
        assert_eq!(captures[1].source, "age");
        assert_eq!(captures[1].alias, "user_age");
    }

    #[test]
    fn parse_captures_no_captures() {
        let template = nb_workload::model::ParsedOp::simple("test", "INSERT INTO t VALUES (1)");
        let captures = parse_template_captures(&template);
        assert!(captures.is_empty());
    }

    #[test]
    fn extract_from_json_top_level() {
        #[derive(Debug)]
        struct JsonBody(serde_json::Value);
        impl ResultBody for JsonBody {
            fn to_json(&self) -> serde_json::Value { self.0.clone() }
            fn as_any(&self) -> &dyn std::any::Any { self }
        }

        let body = JsonBody(serde_json::json!({
            "user_id": 42,
            "name": "alice",
            "balance": 99.5
        }));
        let specs = vec![
            CaptureSpec { source: "user_id".into(), alias: "uid".into() },
            CaptureSpec { source: "name".into(), alias: "name".into() },
        ];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures.len(), 2);
        assert_eq!(captures["uid"].as_u64(), 42);
        match &captures["name"] {
            nb_variates::node::Value::Str(s) => assert_eq!(s, "alice"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn extract_wildcard() {
        #[derive(Debug)]
        struct JsonBody(serde_json::Value);
        impl ResultBody for JsonBody {
            fn to_json(&self) -> serde_json::Value { self.0.clone() }
            fn as_any(&self) -> &dyn std::any::Any { self }
        }

        let body = JsonBody(serde_json::json!({"a": 1, "b": 2}));
        let specs = vec![CaptureSpec { source: "*".into(), alias: "*".into() }];
        let captures = extract_captures_from_json(&body, &specs);
        assert_eq!(captures.len(), 2);
    }
}
