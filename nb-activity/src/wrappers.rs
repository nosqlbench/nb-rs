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
    AdapterError, ExecutionError, OpDispenser, OpResult, ResolvedFields,
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
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
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

/// Wraps an inner OpDispenser with a conditional check.
///
/// Evaluates a named field in `ResolvedFields` before executing.
/// If the field is falsy (0, 0.0, false, empty string, None), the
/// op is skipped — no inner execution, no adapter call. Returns
/// `OpResult::skipped()`.
///
/// The condition field must be included in the resolved fields
/// (added via `extra_bindings` during compilation).
pub struct ConditionalDispenser {
    inner: Arc<dyn OpDispenser>,
    /// The name of the resolved field to check.
    condition_field: String,
    /// Metrics reference for counting skips.
    metrics: Arc<super::activity::ActivityMetrics>,
}

impl ConditionalDispenser {
    /// Wrap an inner dispenser with a condition check.
    ///
    /// `condition_field` is the GK output name that will appear in
    /// the resolved fields. If the op template has no `if:` field,
    /// don't wrap — just use the inner dispenser directly.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        condition_field: &str,
        metrics: Arc<super::activity::ActivityMetrics>,
    ) -> Arc<dyn OpDispenser> {
        Arc::new(Self {
            inner,
            condition_field: condition_field.to_string(),
            metrics,
        })
    }
}

/// Test whether a resolved field value is truthy.
fn is_truthy(value: &nb_variates::node::Value) -> bool {
    match value {
        nb_variates::node::Value::None => false,
        nb_variates::node::Value::U64(v) => *v != 0,
        nb_variates::node::Value::F64(v) => *v != 0.0,
        nb_variates::node::Value::Bool(v) => *v,
        nb_variates::node::Value::Str(s) => !s.is_empty(),
        _ => true,
    }
}

impl OpDispenser for ConditionalDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Check condition field in resolved values
            if let Some(value) = fields.get_value(&self.condition_field) {
                if !is_truthy(value) {
                    self.metrics.skips_total.inc();
                    return Ok(OpResult::skipped());
                }
            }
            // Condition truthy (or field missing) — execute normally.
            // Strip the condition field from resolved values so the
            // adapter doesn't see internal fields in its output.
            let stripped = fields.without(&self.condition_field);
            self.inner.execute(cycle, &stripped).await
        })
    }
}

/// Wraps an inner OpDispenser with a per-cycle delay.
///
/// Reads a named field from `ResolvedFields` as a delay in
/// nanoseconds (u64) or milliseconds (f64), sleeps for that
/// duration, then delegates to the inner dispenser. This models
/// GK-driven latency injection — the delay varies per cycle
/// based on the GK binding.
///
/// The delay field is stripped from resolved values before the
/// adapter sees them.
pub struct ThrottleDispenser {
    inner: Arc<dyn OpDispenser>,
    /// The name of the resolved field containing the delay.
    delay_field: String,
}

impl ThrottleDispenser {
    /// Wrap an inner dispenser with a per-cycle delay.
    ///
    /// `delay_field` is a GK output name that produces the delay.
    /// - u64 values are interpreted as nanoseconds
    /// - f64 values are interpreted as milliseconds
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        delay_field: &str,
    ) -> Arc<dyn OpDispenser> {
        Arc::new(Self {
            inner,
            delay_field: delay_field.to_string(),
        })
    }
}

impl OpDispenser for ThrottleDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Read delay from resolved fields
            if let Some(value) = fields.get_value(&self.delay_field) {
                let nanos = match value {
                    nb_variates::node::Value::U64(ns) => *ns,
                    nb_variates::node::Value::F64(ms) => (*ms * 1_000_000.0) as u64,
                    _ => 0,
                };
                if nanos > 0 {
                    tokio::time::sleep(std::time::Duration::from_nanos(nanos)).await;
                }
            }
            let stripped = fields.without(&self.delay_field);
            self.inner.execute(cycle, &stripped).await
        })
    }
}

// =========================================================================
// PollingDispenser: retry until condition met
// =========================================================================

/// Wraps an inner dispenser and re-executes it until the result
/// body is empty (zero rows). Used for awaiting conditions like
/// SAI index compaction completing.
///
/// Configured via op params:
/// - `poll_interval_ms`: delay between polls (default: 1000)
/// - `timeout_ms`: maximum total wait (default: 300000 = 5 min)
/// - `poll_condition`: when to stop: "empty" (default) = stop when 0 rows
pub struct PollingDispenser {
    inner: Arc<dyn OpDispenser>,
    poll_interval: std::time::Duration,
    timeout: std::time::Duration,
    /// Named metric for the poll elapsed time (e.g., "index_build_time").
    metric_name: Option<String>,
    /// Externally visible metrics for the polling operation.
    pub metrics: Arc<PollingMetrics>,
}

/// Metrics surfaced by the polling wrapper.
pub struct PollingMetrics {
    /// Total polls executed across all invocations.
    pub polls_total: std::sync::atomic::AtomicU64,
    /// Total time spent polling (milliseconds).
    pub poll_elapsed_ms: std::sync::atomic::AtomicU64,
    /// Whether the condition has been met (0 = waiting, 1 = done).
    pub condition_met: std::sync::atomic::AtomicU64,
    /// The last observed value from the poll condition (e.g., number of
    /// remaining tasks). This is the metric that determines completion.
    pub poll_metric: std::sync::atomic::AtomicU64,
}

impl PollingMetrics {
    fn new() -> Self {
        Self {
            polls_total: std::sync::atomic::AtomicU64::new(0),
            poll_elapsed_ms: std::sync::atomic::AtomicU64::new(0),
            condition_met: std::sync::atomic::AtomicU64::new(0),
            poll_metric: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl PollingDispenser {
    /// Wrap an inner dispenser with polling behavior.
    /// Returns the wrapped dispenser and a handle to the metrics.
    ///
    /// `metric_name`: if set, the elapsed poll time is captured as a named
    /// gauge (in seconds) for the summary report.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        poll_interval_ms: u64,
        timeout_ms: u64,
        metric_name: Option<String>,
    ) -> (Arc<dyn OpDispenser>, Arc<PollingMetrics>) {
        let metrics = Arc::new(PollingMetrics::new());
        let dispenser = Arc::new(Self {
            inner,
            poll_interval: std::time::Duration::from_millis(poll_interval_ms),
            timeout: std::time::Duration::from_millis(timeout_ms),
            metric_name,
            metrics: metrics.clone(),
        });
        (dispenser, metrics)
    }
}

impl OpDispenser for PollingDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let start = std::time::Instant::now();
            let mut polls = 0u64;

            loop {
                let result = self.inner.execute(cycle, fields).await?;
                polls += 1;

                // Check condition: empty result body = done
                let row_count = result.body.as_ref()
                    .map(|b| b.element_count())
                    .unwrap_or(0);
                let is_empty = row_count == 0;

                self.metrics.poll_metric.store(row_count, std::sync::atomic::Ordering::Relaxed);

                if !is_empty {
                    eprint!("\r\x1b[K  awaiting: {row_count} task(s) remaining ({:.0}s elapsed)",
                        start.elapsed().as_secs_f64());
                }

                if is_empty {
                    let elapsed = start.elapsed();
                    let elapsed_secs = elapsed.as_secs_f64();
                    self.metrics.polls_total.fetch_add(polls, std::sync::atomic::Ordering::Relaxed);
                    self.metrics.poll_elapsed_ms.store(elapsed.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
                    self.metrics.condition_met.store(1, std::sync::atomic::Ordering::Relaxed);
                    crate::observer::log(
                        crate::observer::LogLevel::Info,
                        &format!("  poll complete: {} polls in {:.1}s", polls, elapsed_secs),
                    );
                    let mut captures = std::collections::HashMap::new();
                    captures.insert("poll_count".into(), nb_variates::node::Value::U64(polls));
                    captures.insert("poll_elapsed_ms".into(),
                        nb_variates::node::Value::U64(elapsed.as_millis() as u64));
                    // Emit named metric (e.g., "index_build_time") in seconds
                    if let Some(ref name) = self.metric_name {
                        captures.insert(name.clone(),
                            nb_variates::node::Value::F64(elapsed_secs));
                    }
                    return Ok(OpResult {
                        body: None,
                        captures,
                        skipped: false,
                    });
                }

                // Check timeout
                if start.elapsed() > self.timeout {
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: "poll_timeout".into(),
                        message: format!(
                            "polling timed out after {:.1}s ({} polls). Last result had rows.",
                            start.elapsed().as_secs_f64(), polls
                        ),
                        retryable: false,
                    }));
                }

                // Wait before next poll
                tokio::time::sleep(self.poll_interval).await;
            }
        })
    }
}

// =========================================================================
// EmitDispenser: print result body as JSON after execution
// =========================================================================

/// Wraps any adapter's dispenser and prints the result body to stdout
/// as JSON after each execution. Adapter-agnostic — works with CQL,
/// HTTP, stdout, or any adapter that returns a ResultBody.
///
/// Enabled by wrapping at init time when `dryrun=emit` is active
/// or when the op has `emit: true`.
pub struct EmitDispenser {
    inner: Arc<dyn OpDispenser>,
    op_name: String,
}

impl EmitDispenser {
    pub fn wrap(inner: Arc<dyn OpDispenser>, op_name: &str) -> Arc<dyn OpDispenser> {
        Arc::new(Self {
            inner,
            op_name: op_name.to_string(),
        })
    }
}

impl OpDispenser for EmitDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self.inner.execute(cycle, fields).await?;

            // Print result body as JSON
            if let Some(ref body) = result.body {
                let json = body.to_json();
                println!("[{}@{}] {} rows: {}",
                    self.op_name, cycle,
                    body.element_count(),
                    serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string()));
            } else {
                println!("[{}@{}] (no result body)", self.op_name, cycle);
            }

            // Print captures if any
            if !result.captures.is_empty() {
                for (name, value) in &result.captures {
                    println!("  capture {name} = {}", value.to_display_string());
                }
            }

            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::ResultBody;

    #[test]
    fn parse_captures_from_template() {
        let template = nb_workload::model::ParsedOp::simple("test", "SELECT [username], [age as user_age] FROM users");
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
