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
    AdapterError, ExecutionError, OpDispenser, OpResult,
};
use nbrs_workload::bindpoints;

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
        template: &nbrs_workload::model::ParsedOp,
        stats: Arc<TraversalStats>,
    ) -> Arc<dyn OpDispenser> {
        let captures = parse_template_captures(template);
        Arc::new(Self { inner, stats, captures })
    }
}

/// Parse capture points from all string fields in a template.
fn parse_template_captures(template: &nbrs_workload::model::ParsedOp) -> Vec<CaptureSpec> {
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
) -> HashMap<String, nbrs_variates::node::Value> {
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
fn json_to_value(v: &serde_json::Value) -> nbrs_variates::node::Value {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_u64() {
                nbrs_variates::node::Value::U64(i)
            } else if let Some(f) = n.as_f64() {
                nbrs_variates::node::Value::F64(f)
            } else {
                nbrs_variates::node::Value::Str(n.to_string())
            }
        }
        serde_json::Value::Bool(b) => nbrs_variates::node::Value::Bool(*b),
        serde_json::Value::String(s) => nbrs_variates::node::Value::Str(s.clone()),
        other => nbrs_variates::node::Value::Str(other.to_string()),
    }
}

impl OpDispenser for TraversingDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Execute the inner dispenser
            let mut result = self.inner.execute(cycle, ctx).await?;

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
/// The condition GK name is resolved at init time via
/// `ScopeFixture::register_pull` and read at cycle time through
/// the stored `PullHandle` against `ExecCtx::pulls`.
pub struct ConditionalDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Memoized handle for the condition GK name registered into
    /// the scope fixture at init.
    condition_handle: crate::fixture::PullHandle,
    /// Metrics reference for counting skips.
    metrics: Arc<super::activity::ActivityMetrics>,
}

impl ConditionalDispenser {
    /// Wrap an inner dispenser with a condition check, registering
    /// `condition_field` into the supplied scope fixture so the
    /// per-cycle read goes through the canonical PullPlan path
    /// (SRD 32 §"Init-Time Fixture and Consumer Self-Registration").
    ///
    /// Errors if the kernel doesn't know `condition_field`.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        condition_field: &str,
        metrics: Arc<super::activity::ActivityMetrics>,
        fx: &mut crate::fixture::ScopeFixture,
    ) -> Result<Arc<dyn OpDispenser>, String> {
        let condition_handle = fx.register_pull(condition_field).map_err(|e| {
            format!("conditional `if`: {e}")
        })?;
        Ok(Arc::new(Self { inner, condition_handle, metrics }))
    }
}

/// Test whether a resolved field value is truthy.
fn is_truthy(value: &nbrs_variates::node::Value) -> bool {
    match value {
        nbrs_variates::node::Value::None => false,
        nbrs_variates::node::Value::U64(v) => *v != 0,
        nbrs_variates::node::Value::F64(v) => *v != 0.0,
        nbrs_variates::node::Value::Bool(v) => *v,
        nbrs_variates::node::Value::Str(s) => !s.is_empty(),
        _ => true,
    }
}

impl OpDispenser for ConditionalDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Single read path: condition value via handle from
            // the cycle's ResolvedPulls. Adapter never sees the
            // condition — it's not in fields, so no strip step.
            let value = ctx.pulls.get(self.condition_handle);
            if !is_truthy(value) {
                self.metrics.skips_total.inc();
                return Ok(OpResult::skipped());
            }
            self.inner.execute(cycle, ctx).await
        })
    }
}

/// Wraps an inner OpDispenser with a per-cycle delay.
///
/// Reads a delay value via `PullHandle` from the cycle's
/// `ResolvedPulls`. u64 values are interpreted as nanoseconds;
/// f64 values are interpreted as milliseconds. The delay is
/// invisible to the inner adapter — it's never in `ResolvedFields`.
pub struct ThrottleDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Memoized handle for the delay GK name registered into the
    /// scope fixture at init.
    delay_handle: crate::fixture::PullHandle,
}

impl ThrottleDispenser {
    /// Wrap an inner dispenser with a per-cycle delay, registering
    /// `delay_field` into the supplied scope fixture so the
    /// per-cycle read goes through the canonical PullPlan path
    /// (SRD 32 §"Init-Time Fixture and Consumer Self-Registration").
    ///
    /// Errors if the kernel doesn't know `delay_field`.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        delay_field: &str,
        fx: &mut crate::fixture::ScopeFixture,
    ) -> Result<Arc<dyn OpDispenser>, String> {
        let delay_handle = fx.register_pull(delay_field).map_err(|e| {
            format!("throttle `delay`: {e}")
        })?;
        Ok(Arc::new(Self { inner, delay_handle }))
    }
}

impl OpDispenser for ThrottleDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let value = ctx.pulls.get(self.delay_handle);
            let nanos = match value {
                nbrs_variates::node::Value::U64(ns) => *ns,
                nbrs_variates::node::Value::F64(ms) => (*ms * 1_000_000.0) as u64,
                _ => 0,
            };
            if nanos > 0 {
                tokio::time::sleep(std::time::Duration::from_nanos(nanos)).await;
            }
            self.inner.execute(cycle, ctx).await
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
/// - `poll_max_error_retries`: how many retryable errors to swallow
///   before propagating (default: 0 — strict: any inner error fails
///   the poll immediately, per SRD-03 §"Status-Determination
///   Invariant")
///
/// Per SRD-03 §"Status-Determination Invariant", this wrapper
/// short-circuits on every non-positive case:
///
/// - **Positive case**: inner op returns `OpResult` with an empty
///   body → poll succeeds, this dispenser returns success.
/// - **Any other case**: inner op returns a non-retryable
///   `ExecutionError`, OR a retryable error past the retry limit,
///   OR the timeout fires while non-empty bodies are still coming
///   back → this dispenser returns the error, the activity error
///   router sees it, and (under default `errors:` policy) the
///   phase + the run stop. Errors are never swallowed behind the
///   poll.
pub struct PollingDispenser {
    inner: Arc<dyn OpDispenser>,
    poll_interval: std::time::Duration,
    timeout: std::time::Duration,
    /// Cap on consecutive retryable inner-op errors before the
    /// wrapper propagates upstream. `0` means strict: any
    /// inner-op error fails the poll immediately.
    max_error_retries: u32,
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
    /// `max_error_retries`: cap on consecutive retryable inner errors
    /// (default 0 = strict).
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        poll_interval_ms: u64,
        timeout_ms: u64,
        max_error_retries: u32,
        metric_name: Option<String>,
    ) -> (Arc<dyn OpDispenser>, Arc<PollingMetrics>) {
        let metrics = Arc::new(PollingMetrics::new());
        let dispenser = Arc::new(Self {
            inner,
            poll_interval: std::time::Duration::from_millis(poll_interval_ms),
            timeout: std::time::Duration::from_millis(timeout_ms),
            max_error_retries,
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
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let start = std::time::Instant::now();
            let mut polls = 0u64;
            let mut retryable_errors_consumed: u32 = 0;

            loop {
                // SRD-03 §"Status-Determination Invariant":
                // every inner-op outcome other than the
                // specific positive case (empty body, signalling
                // "no remaining build tasks") short-circuits
                // upstream as an error. Retryable errors get a
                // bounded retry budget (`max_error_retries`);
                // non-retryable errors never get retried — they
                // propagate on the first occurrence.
                let result = match self.inner.execute(cycle, ctx).await {
                    Ok(r) => r,
                    Err(e) => {
                        let retryable = match &e {
                            ExecutionError::Op(ad) => ad.retryable,
                            ExecutionError::Adapter(_) => false,
                        };
                        if !retryable {
                            return Err(e);
                        }
                        if retryable_errors_consumed >= self.max_error_retries {
                            return Err(e);
                        }
                        retryable_errors_consumed += 1;
                        let indent = crate::scene_tree::running_phase_indent();
                        let color = crate::observer::use_color();
                        let yellow = color.then(|| "\x1b[33m").unwrap_or("");
                        let reset = color.then(|| "\x1b[0m").unwrap_or("");
                        crate::diag!(
                            crate::observer::LogLevel::Warn,
                            "{indent}{yellow}poll retry {retryable_errors_consumed}/{}{reset} after retryable error: {}",
                            self.max_error_retries,
                            match &e {
                                ExecutionError::Op(ad) => &ad.message,
                                ExecutionError::Adapter(ad) => &ad.message,
                            },
                        );
                        // Backoff before retry — same as the
                        // between-polls cadence so a flapping
                        // backend doesn't burn the retry budget
                        // in a tight loop.
                        tokio::time::sleep(self.poll_interval).await;
                        continue;
                    }
                };
                polls += 1;

                // Check condition: empty result body = done
                let row_count = result.body.as_ref()
                    .map(|b| b.element_count())
                    .unwrap_or(0);
                let is_empty = row_count == 0;

                self.metrics.poll_metric.store(row_count, std::sync::atomic::Ordering::Relaxed);

                if !is_empty {
                    // Per-poll progress goes to the durable
                    // session log at Debug — direct `eprint!`
                    // here would clobber the TUI's render
                    // surface. The TUI surfaces poll progress
                    // via the `poll_metric` gauge (live row
                    // count) which is already updated above.
                    let indent = crate::scene_tree::running_phase_indent();
                    crate::diag!(
                        crate::observer::LogLevel::Debug,
                        "{indent}awaiting: {row_count} task(s) remaining ({:.0}s elapsed)",
                        start.elapsed().as_secs_f64()
                    );
                }

                if is_empty {
                    let elapsed = start.elapsed();
                    let elapsed_secs = elapsed.as_secs_f64();
                    self.metrics.polls_total.fetch_add(polls, std::sync::atomic::Ordering::Relaxed);
                    self.metrics.poll_elapsed_ms.store(elapsed.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
                    self.metrics.condition_met.store(1, std::sync::atomic::Ordering::Relaxed);
                    let indent = crate::scene_tree::running_phase_indent();
                    let color = crate::observer::use_color();
                    let dim = color.then(|| "\x1b[2m").unwrap_or("");
                    let green = color.then(|| "\x1b[32m").unwrap_or("");
                    let reset = color.then(|| "\x1b[0m").unwrap_or("");
                    crate::observer::log(
                        crate::observer::LogLevel::Info,
                        &format!("{indent}{green}poll complete{reset}: {polls} polls {dim}in {elapsed_secs:.1}s{reset}"),
                    );
                    let mut captures = std::collections::HashMap::new();
                    captures.insert("poll_count".into(), nbrs_variates::node::Value::U64(polls));
                    captures.insert("poll_elapsed_ms".into(),
                        nbrs_variates::node::Value::U64(elapsed.as_millis() as u64));
                    // Emit named metric (e.g., "index_build_time") in seconds
                    if let Some(ref name) = self.metric_name {
                        captures.insert(name.clone(),
                            nbrs_variates::node::Value::F64(elapsed_secs));
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
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self.inner.execute(cycle, ctx).await?;

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
        let template = nbrs_workload::model::ParsedOp::simple("test", "SELECT [username], [age as user_age] FROM users");
        let captures = parse_template_captures(&template);
        assert_eq!(captures.len(), 2);
        assert_eq!(captures[0].source, "username");
        assert_eq!(captures[0].alias, "username");
        assert_eq!(captures[1].source, "age");
        assert_eq!(captures[1].alias, "user_age");
    }

    #[test]
    fn parse_captures_no_captures() {
        let template = nbrs_workload::model::ParsedOp::simple("test", "INSERT INTO t VALUES (1)");
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
            nbrs_variates::node::Value::Str(s) => assert_eq!(s, "alice"),
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
