// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Result validation and relevancy measurement (SRD 47).
//!
//! Provides `ValidatingDispenser`, a composable op dispenser wrapper
//! that verifies operation results against expected values and computes
//! information retrieval metrics (recall@k, precision@k, etc.).
//!
//! Applied only to templates that declare `verify:` or `relevancy:`
//! blocks — zero overhead for templates without validation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nb_metrics::labels::Labels;

use crate::adapter::{
    ExecutionError, OpDispenser, OpResult, ResolvedFields,
};
use crate::relevancy::{self, RelevancyFn};

// =========================================================================
// Validation configuration (parsed from workload YAML at init time)
// =========================================================================

/// Configuration for relevancy measurement on a single op template.
#[derive(Debug, Clone)]
pub struct RelevancyConfig {
    /// Column/field name to extract actual result indices from.
    pub actual_field: String,
    /// GK binding name that produces ground truth indices.
    pub expected_binding: String,
    /// Maximum k for @k metrics.
    pub k: usize,
    /// Which functions to compute.
    pub functions: Vec<RelevancyFn>,
}

/// A single field assertion from a `verify:` block.
#[derive(Debug, Clone)]
pub struct AssertionSpec {
    /// Field name to check in the result.
    pub field: String,
    /// The predicate to apply.
    pub predicate: AssertionPredicate,
}

/// Predicate for field assertion checks.
#[derive(Debug, Clone)]
pub enum AssertionPredicate {
    /// Field must equal this value (string comparison).
    Eq(String),
    /// Field must not be null/absent.
    NotNull,
    /// Field must be null/absent.
    IsNull,
    /// Numeric: field >= threshold.
    Gte(f64),
    /// Numeric: field <= threshold.
    Lte(f64),
    /// String: field contains substring.
    Contains(String),
}

impl AssertionSpec {
    /// Check this assertion against a result.
    pub fn check(&self, result: &OpResult) -> bool {
        let json = match &result.body {
            Some(body) => body.to_json(),
            None => return matches!(self.predicate, AssertionPredicate::IsNull),
        };

        let field_val = extract_field_from_json(&json, &self.field);

        match &self.predicate {
            AssertionPredicate::NotNull => field_val.is_some(),
            AssertionPredicate::IsNull => field_val.is_none(),
            AssertionPredicate::Eq(expected) => {
                field_val.map_or(false, |v| json_value_as_string(&v) == *expected)
            }
            AssertionPredicate::Gte(threshold) => {
                field_val.and_then(|v| v.as_f64()).map_or(false, |v| v >= *threshold)
            }
            AssertionPredicate::Lte(threshold) => {
                field_val.and_then(|v| v.as_f64()).map_or(false, |v| v <= *threshold)
            }
            AssertionPredicate::Contains(substr) => {
                field_val.map_or(false, |v| json_value_as_string(&v).contains(substr.as_str()))
            }
        }
    }
}

// =========================================================================
// ValidationMetrics
// =========================================================================

/// Metrics for result validation, shared across fibers.
pub struct ValidationMetrics {
    pub validations_passed: AtomicU64,
    pub validations_failed: AtomicU64,
    /// Per-function lossless f64 statistics accumulators for relevancy scores.
    /// Exact precision — no quantization, no bucket rounding.
    pub relevancy_stats: HashMap<String, nb_metrics::instruments::f64stats::F64Stats>,
}

impl ValidationMetrics {
    /// Create metrics for the given relevancy functions and k value.
    pub fn new(labels: &Labels, functions: &[RelevancyFn], k: usize) -> Self {
        let mut stats = HashMap::new();
        for func in functions {
            let metric_name = format!("{}@{}", func.metric_name(), k);
            stats.insert(
                metric_name.clone(),
                nb_metrics::instruments::f64stats::F64Stats::new(labels.with("name", &metric_name)),
            );
        }
        Self {
            validations_passed: AtomicU64::new(0),
            validations_failed: AtomicU64::new(0),
            relevancy_stats: stats,
        }
    }

    /// Create metrics with no relevancy functions (assertions only).
    pub fn assertions_only() -> Self {
        Self {
            validations_passed: AtomicU64::new(0),
            validations_failed: AtomicU64::new(0),
            relevancy_stats: HashMap::new(),
        }
    }

    /// Record a relevancy score for a named function.
    pub fn record_relevancy(&self, metric_name: &str, score: f64) {
        if let Some(stats) = self.relevancy_stats.get(metric_name) {
            stats.record(score);
        }
    }

    /// Get pass count.
    pub fn passed(&self) -> u64 {
        self.validations_passed.load(Ordering::Relaxed)
    }

    /// Get fail count.
    pub fn failed(&self) -> u64 {
        self.validations_failed.load(Ordering::Relaxed)
    }
}

// =========================================================================
// ValidatingDispenser
// =========================================================================

/// Op dispenser wrapper that validates results after execution.
///
/// Applied only to templates that declare `verify:` or `relevancy:`
/// blocks. Zero overhead for templates without validation.
pub struct ValidatingDispenser {
    inner: Arc<dyn OpDispenser>,
    assertions: Vec<AssertionSpec>,
    relevancy: Option<RelevancyConfig>,
    metrics: Arc<ValidationMetrics>,
    /// If true, assertion failures become ExecutionError::Op.
    strict: bool,
}

impl ValidatingDispenser {
    /// Wrap a dispenser with validation, if the template declares it.
    ///
    /// Returns the inner dispenser unchanged if no validation is declared.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        template: &nb_workload::model::ParsedOp,
        labels: &Labels,
        program: Option<&nb_variates::kernel::GkProgram>,
    ) -> (Arc<dyn OpDispenser>, Option<Arc<ValidationMetrics>>) {
        let assertions = parse_assertions(template);
        let relevancy = parse_relevancy(template, program);
        let strict = template.params.get("strict")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);


        if assertions.is_empty() && relevancy.is_none() {
            return (inner, None);
        }

        let metrics = Arc::new(match &relevancy {
            Some(config) => ValidationMetrics::new(labels, &config.functions, config.k),
            None => ValidationMetrics::assertions_only(),
        });

        let wrapper = Arc::new(Self {
            inner,
            assertions,
            relevancy,
            metrics: metrics.clone(),
            strict,
        });
        (wrapper, Some(metrics))
    }
}

impl OpDispenser for ValidatingDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self.inner.execute(cycle, fields).await?;

            // Phase 1: Field assertions
            let mut all_pass = true;
            for assertion in &self.assertions {
                if !assertion.check(&result) {
                    all_pass = false;
                }
            }

            // Phase 2: Relevancy metrics
            if let Some(config) = &self.relevancy {
                let actual_ordered = extract_actual_indices(&result, &config.actual_field);
                let expected_raw = resolve_expected(fields, &config.expected_binding);

                // Hard error if ground truth or actual results are empty
                if expected_raw.is_empty() {
                    let binding_name = config.expected_binding.trim_matches(|c| c == '{' || c == '}');
                    return Err(ExecutionError::Op(crate::adapter::AdapterError {
                        error_name: "relevancy_error".into(),
                        message: format!(
                            "relevancy: no ground truth for '{binding_name}'. \
                             Available fields: {fields:?}. \
                             Ensure the binding exists in the GK program.",
                        ),
                        retryable: false,
                    }));
                }
                if actual_ordered.is_empty() && result.body.is_some() {
                    // Log on first occurrence only
                    if self.metrics.passed() + self.metrics.failed() == 0 {
                        eprintln!("warning: relevancy: no values extracted for field '{}' from result", config.actual_field);
                        if let Some(body) = &result.body {
                            let preview = serde_json::to_string(&body.to_json()).unwrap_or_default();
                            eprintln!("  result preview: {}", &preview[..preview.len().min(300)]);
                        }
                    }
                }

                let expected_sorted = relevancy::truncate_and_sort(&expected_raw, config.k);
                let actual_sorted = relevancy::truncate_and_sort(&actual_ordered, config.k);

                for func in &config.functions {
                    let score = func.compute(
                        &expected_sorted,
                        &actual_sorted,
                        &actual_ordered,
                        config.k,
                    );
                    let metric_name = format!("{}@{}", func.metric_name(), config.k);
                    self.metrics.record_relevancy(&metric_name, score);
                }
            }

            if all_pass {
                self.metrics.validations_passed.fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics.validations_failed.fetch_add(1, Ordering::Relaxed);
                if self.strict {
                    return Err(ExecutionError::Op(crate::adapter::AdapterError {
                        error_name: "validation_failed".into(),
                        message: "result validation failed (strict mode)".into(),
                        retryable: false,
                    }));
                }
            }

            Ok(result)
        })
    }
}

// =========================================================================
// YAML parsing
// =========================================================================

/// Extract extra GK binding names that validation needs but aren't in op fields.
///
/// Called at init time so the resolver knows to include these bindings
/// in `ResolvedFields` for each cycle.
pub fn extra_bindings(template: &nb_workload::model::ParsedOp) -> Vec<String> {
    let mut extras = Vec::new();
    if let Some(config) = parse_relevancy(template, None) {
        let name = config.expected_binding.trim_matches(|c| c == '{' || c == '}').to_string();
        if !template.op.contains_key(&name) {
            extras.push(name);
        }
    }
    extras
}

/// Parse `verify:` block from a ParsedOp's params.
///
/// Expected YAML structure:
/// ```yaml
/// verify:
///   - field: name
///     is: not_null
///   - field: balance
///     gte: 0
///   - field: data
///     eq: "expected_value"
/// ```
fn parse_assertions(template: &nb_workload::model::ParsedOp) -> Vec<AssertionSpec> {
    let Some(verify) = template.params.get("verify") else {
        return Vec::new();
    };

    let Some(items) = verify.as_array() else {
        return Vec::new();
    };

    let mut assertions = Vec::new();
    for item in items {
        let Some(obj) = item.as_object() else { continue };
        let Some(field) = obj.get("field").and_then(|v| v.as_str()) else { continue };

        let predicate = if let Some(v) = obj.get("eq") {
            AssertionPredicate::Eq(json_value_as_string(v))
        } else if let Some(v) = obj.get("gte") {
            AssertionPredicate::Gte(v.as_f64().unwrap_or(0.0))
        } else if let Some(v) = obj.get("lte") {
            AssertionPredicate::Lte(v.as_f64().unwrap_or(0.0))
        } else if let Some(v) = obj.get("contains") {
            AssertionPredicate::Contains(json_value_as_string(v))
        } else if let Some(v) = obj.get("is") {
            match v.as_str().unwrap_or("").to_lowercase().as_str() {
                "not_null" | "notnull" => AssertionPredicate::NotNull,
                "null" => AssertionPredicate::IsNull,
                _ => continue,
            }
        } else {
            continue;
        };

        assertions.push(AssertionSpec {
            field: field.to_string(),
            predicate,
        });
    }
    assertions
}

/// Parse `relevancy:` block from a ParsedOp's params.
///
/// Expected YAML structure:
/// ```yaml
/// relevancy:
///   actual: key
///   expected: "{ground_truth}"
///   k: 10
///   functions:
///     - recall
///     - precision
///     - f1
/// ```
fn parse_relevancy(
    template: &nb_workload::model::ParsedOp,
    program: Option<&nb_variates::kernel::GkProgram>,
) -> Option<RelevancyConfig> {
    let rel = template.params.get("relevancy")?;
    let obj = rel.as_object()?;

    let actual_field = obj.get("actual")?.as_str()?.to_string();
    let expected_binding = obj.get("expected")?.as_str()?.to_string();
    let k = obj.get("k")
        .and_then(|v| {
            if let Some(n) = v.as_u64() { return Some(n); }
            if let Some(s) = v.as_str() {
                if let Ok(n) = s.parse::<u64>() { return Some(n); }
                // {name} reference — resolve from GK program constants
                let bare = s.trim().strip_prefix('{')
                    .and_then(|s| s.strip_suffix('}'));
                if let Some(name) = bare {
                    if let Some(prog) = program {
                        // Pull the value from a temporary state
                        let mut state = prog.create_state();
                        state.set_inputs(&[0]);
                        let val = state.pull(prog, name);
                        return match val {
                            nb_variates::node::Value::U64(n) => Some(*n),
                            nb_variates::node::Value::Str(s) => s.parse().ok(),
                            nb_variates::node::Value::F64(f) => Some(*f as u64),
                            _ => None,
                        };
                    }
                }
            }
            None
        })
        .unwrap_or(10) as usize;

    let functions: Vec<RelevancyFn> = obj.get("functions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().and_then(RelevancyFn::parse))
                .collect()
        })
        .unwrap_or_else(|| vec![RelevancyFn::Recall]);

    Some(RelevancyConfig {
        actual_field,
        expected_binding,
        k,
        functions,
    })
}

// =========================================================================
// Result extraction
// =========================================================================

/// Extract integer indices from a result body for relevancy comparison.
///
/// Tries adapter-native downcast first, then falls back to JSON extraction.
fn extract_actual_indices(result: &OpResult, field: &str) -> Vec<i64> {
    let Some(body) = &result.body else {
        return Vec::new();
    };
    extract_indices_from_json(&body.to_json(), field)
}

/// Extract integer values for a named field from JSON result structure.
fn extract_indices_from_json(json: &serde_json::Value, field: &str) -> Vec<i64> {
    match json {
        serde_json::Value::Array(rows) => {
            rows.iter()
                .filter_map(|row| json_field_as_i64(row.get(field)?))
                .collect()
        }
        serde_json::Value::Object(obj) => {
            if let Some(rows) = obj.get("rows") {
                return extract_indices_from_json(rows, field);
            }
            obj.get(field)
                .and_then(json_field_as_i64)
                .into_iter()
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Coerce a JSON value to i64: native integer, or parse from string.
fn json_field_as_i64(v: &serde_json::Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}

/// Resolve ground truth indices from ResolvedFields.
///
/// The expected binding produces either:
/// - A `Value::Str` containing a JSON array like "[1, 5, 12, ...]"
/// - A `Value::Str` containing comma-separated integers "1,5,12,..."
fn resolve_expected(fields: &ResolvedFields, binding: &str) -> Vec<i64> {
    // Strip surrounding braces from binding name if present: "{ground_truth}" → "ground_truth"
    let name = binding.trim_matches(|c| c == '{' || c == '}');
    let Some(value) = fields.get_value(name) else {
        return Vec::new();
    };
    match value {
        nb_variates::node::Value::Str(s) => parse_int_array(s),
        nb_variates::node::Value::U64(v) => vec![*v as i64],
        _ => {
            // Try display string
            let s = value.to_display_string();
            parse_int_array(&s)
        }
    }
}

/// Parse a string containing integers into a Vec<i64>.
///
/// Handles formats: `[1, 5, 12]`, `1,5,12`, `1 5 12`.
fn parse_int_array(s: &str) -> Vec<i64> {
    let trimmed = s.trim().trim_start_matches('[').trim_end_matches(']');
    trimmed.split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse::<i64>().ok())
        .collect()
}

/// Extract a field from JSON by name, checking both top-level and row arrays.
fn extract_field_from_json<'a>(json: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
    match json {
        serde_json::Value::Object(obj) => {
            obj.get(field).or_else(|| {
                obj.get("rows")
                    .and_then(|r| r.as_array())
                    .and_then(|rows| rows.first())
                    .and_then(|row| row.get(field))
            })
        }
        serde_json::Value::Array(rows) => {
            rows.first().and_then(|row| row.get(field))
        }
        _ => None,
    }
}

/// Convert a JSON value to its string representation for comparison.
fn json_value_as_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::ResultBody;
    use std::any::Any;

    #[derive(Debug)]
    struct JsonBody(serde_json::Value);
    impl ResultBody for JsonBody {
        fn to_json(&self) -> serde_json::Value { self.0.clone() }
        fn as_any(&self) -> &dyn Any { self }
    }

    #[test]
    fn parse_int_array_bracket_format() {
        assert_eq!(parse_int_array("[1, 5, 12, 23]"), vec![1, 5, 12, 23]);
    }

    #[test]
    fn parse_int_array_comma_format() {
        assert_eq!(parse_int_array("1,5,12,23"), vec![1, 5, 12, 23]);
    }

    #[test]
    fn parse_int_array_space_format() {
        assert_eq!(parse_int_array("1 5 12 23"), vec![1, 5, 12, 23]);
    }

    #[test]
    fn parse_int_array_empty() {
        assert_eq!(parse_int_array("[]"), Vec::<i64>::new());
        assert_eq!(parse_int_array(""), Vec::<i64>::new());
    }

    #[test]
    fn extract_indices_from_json_array() {
        let json = serde_json::json!([
            {"key": 5, "distance": 0.1},
            {"key": 12, "distance": 0.2},
            {"key": 3, "distance": 0.3},
        ]);
        assert_eq!(extract_indices_from_json(&json, "key"), vec![5, 12, 3]);
    }

    #[test]
    fn extract_indices_from_json_rows_wrapper() {
        let json = serde_json::json!({
            "rows": [
                {"key": 5},
                {"key": 12},
            ]
        });
        assert_eq!(extract_indices_from_json(&json, "key"), vec![5, 12]);
    }

    #[test]
    fn assertion_not_null() {
        let result = OpResult {
            body: Some(Box::new(JsonBody(serde_json::json!({"name": "alice"})))),
            captures: HashMap::new(),
            skipped: false,
        };
        let spec = AssertionSpec {
            field: "name".into(),
            predicate: AssertionPredicate::NotNull,
        };
        assert!(spec.check(&result));

        let spec_missing = AssertionSpec {
            field: "age".into(),
            predicate: AssertionPredicate::NotNull,
        };
        assert!(!spec_missing.check(&result));
    }

    #[test]
    fn assertion_eq() {
        let result = OpResult {
            body: Some(Box::new(JsonBody(serde_json::json!({"status": "ok"})))),
            captures: HashMap::new(),
            skipped: false,
        };
        let spec = AssertionSpec {
            field: "status".into(),
            predicate: AssertionPredicate::Eq("ok".into()),
        };
        assert!(spec.check(&result));

        let spec_fail = AssertionSpec {
            field: "status".into(),
            predicate: AssertionPredicate::Eq("error".into()),
        };
        assert!(!spec_fail.check(&result));
    }

    #[test]
    fn assertion_gte() {
        let result = OpResult {
            body: Some(Box::new(JsonBody(serde_json::json!({"balance": 42.5})))),
            captures: HashMap::new(),
            skipped: false,
        };
        let spec = AssertionSpec {
            field: "balance".into(),
            predicate: AssertionPredicate::Gte(0.0),
        };
        assert!(spec.check(&result));

        let spec_fail = AssertionSpec {
            field: "balance".into(),
            predicate: AssertionPredicate::Gte(100.0),
        };
        assert!(!spec_fail.check(&result));
    }

    #[test]
    fn assertion_no_body() {
        let result = OpResult {
            body: None,
            captures: HashMap::new(),
            skipped: false,
        };
        let spec = AssertionSpec {
            field: "anything".into(),
            predicate: AssertionPredicate::IsNull,
        };
        assert!(spec.check(&result));

        let spec_not_null = AssertionSpec {
            field: "anything".into(),
            predicate: AssertionPredicate::NotNull,
        };
        assert!(!spec_not_null.check(&result));
    }

    #[test]
    fn validation_metrics_record_relevancy() {
        let labels = Labels::of("activity", "test");
        let metrics = ValidationMetrics::new(
            &labels,
            &[RelevancyFn::Recall, RelevancyFn::Precision],
            10,
        );
        assert!(metrics.relevancy_stats.contains_key("recall@10"));
        assert!(metrics.relevancy_stats.contains_key("precision@10"));
        assert!(!metrics.relevancy_stats.contains_key("f1@10"));

        metrics.record_relevancy("recall@10", 0.85);
        metrics.record_relevancy("recall@10", 0.90);
        let snap = metrics.relevancy_stats["recall@10"].snapshot();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn resolve_expected_binding() {
        let fields = ResolvedFields::new(
            vec!["ground_truth".into()],
            vec![nb_variates::node::Value::Str("[1, 5, 12, 23]".into())],
        );
        let result = resolve_expected(&fields, "{ground_truth}");
        assert_eq!(result, vec![1, 5, 12, 23]);
    }

    #[test]
    fn resolve_expected_bare_name() {
        let fields = ResolvedFields::new(
            vec!["gt".into()],
            vec![nb_variates::node::Value::Str("1,5,12".into())],
        );
        let result = resolve_expected(&fields, "gt");
        assert_eq!(result, vec![1, 5, 12]);
    }

    #[test]
    fn parse_relevancy_from_params() {
        let mut template = nb_workload::model::ParsedOp::simple("test", "SELECT key FROM t");
        template.params.insert("relevancy".into(), serde_json::json!({
            "actual": "key",
            "expected": "{ground_truth}",
            "k": 10,
            "functions": ["recall", "precision", "f1"]
        }));
        let config = parse_relevancy(&template, None).unwrap();
        assert_eq!(config.actual_field, "key");
        assert_eq!(config.expected_binding, "{ground_truth}");
        assert_eq!(config.k, 10);
        assert_eq!(config.functions.len(), 3);
        assert_eq!(config.functions[0], RelevancyFn::Recall);
        assert_eq!(config.functions[1], RelevancyFn::Precision);
        assert_eq!(config.functions[2], RelevancyFn::F1);
    }

    #[test]
    fn parse_relevancy_missing() {
        let template = nb_workload::model::ParsedOp::simple("test", "INSERT");
        assert!(parse_relevancy(&template, None).is_none());
    }

    #[test]
    fn parse_assertions_from_params() {
        let mut template = nb_workload::model::ParsedOp::simple("test", "SELECT");
        template.params.insert("verify".into(), serde_json::json!([
            {"field": "name", "is": "not_null"},
            {"field": "balance", "gte": 0},
            {"field": "status", "eq": "active"},
        ]));
        let assertions = parse_assertions(&template);
        assert_eq!(assertions.len(), 3);
        assert_eq!(assertions[0].field, "name");
        assert!(matches!(assertions[0].predicate, AssertionPredicate::NotNull));
        assert_eq!(assertions[1].field, "balance");
        assert!(matches!(assertions[1].predicate, AssertionPredicate::Gte(v) if v == 0.0));
        assert_eq!(assertions[2].field, "status");
        assert!(matches!(&assertions[2].predicate, AssertionPredicate::Eq(s) if s == "active"));
    }

    #[test]
    fn parse_assertions_missing() {
        let template = nb_workload::model::ParsedOp::simple("test", "INSERT");
        assert!(parse_assertions(&template).is_empty());
    }
}
