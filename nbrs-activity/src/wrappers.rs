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
    AdapterError, ExecutionError, OpDispenser, OpResult, WrappingDispenser,
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

impl WrappingDispenser for TraversingDispenser {}

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

impl WrappingDispenser for ConditionalDispenser {}

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

impl WrappingDispenser for ThrottleDispenser {}

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
    /// Threshold for "done": the poll is considered satisfied
    /// when the inner op's row-count is in `[min_rows, max_rows]`.
    /// Default `max_rows=0, min_rows=0` reproduces the historical
    /// `await_empty` semantics (zero rows = done). Use
    /// `min_rows=1, max_rows=1` for "settled to a single row"
    /// cases such as SAI's `sai_sstable_count == 1` after
    /// memtable flush + compaction (without the lower bound the
    /// poll would exit too early at count=0, before the
    /// memtable has flushed).
    min_rows: u64,
    max_rows: u64,
    /// Optional JSON-Pointer path (RFC 6901, e.g. `/value`) that
    /// drills into the result body before computing the count
    /// for the `[min_rows, max_rows]` check. Use this when the
    /// op's body wraps the meaningful payload in an envelope —
    /// notably Jolokia, whose every response is
    /// `{request, value, status, timestamp}` and the actual
    /// answer lives under `.value`. When the addressed sub-tree
    /// is an array, count is its length; a number maps directly
    /// to count; an object or null maps to 1 / 0. Default `None`
    /// uses `body.element_count()` as-is.
    json_path: Option<String>,
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
        min_rows: u64,
        max_rows: u64,
        json_path: Option<String>,
    ) -> (Arc<dyn OpDispenser>, Arc<PollingMetrics>) {
        let metrics = Arc::new(PollingMetrics::new());
        let dispenser = Arc::new(Self {
            inner,
            poll_interval: std::time::Duration::from_millis(poll_interval_ms),
            timeout: std::time::Duration::from_millis(timeout_ms),
            max_error_retries,
            metric_name,
            min_rows,
            max_rows,
            json_path,
            metrics: metrics.clone(),
        });
        (dispenser, metrics)
    }
}

impl WrappingDispenser for PollingDispenser {}

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

                // Check condition: row count in [min_rows, max_rows] = done.
                // Default `min_rows=0, max_rows=0` reproduces the legacy
                // `await_empty` (exactly 0 = done) semantics.
                //
                // When `json_path` is set, the count comes from the
                // addressed sub-tree of the body's JSON projection
                // — array length, raw number, or 1 (object) / 0
                // (null/missing). This is the Jolokia-poll path:
                // `getCompactions` returns `{value: [...], status: 200, ...}`
                // and we want the length of `.value`, not 1 (the
                // envelope object).
                let row_count = match (&result.body, self.json_path.as_deref()) {
                    (Some(body), Some(path)) => {
                        let json = body.to_json();
                        count_from_json_pointer(&json, path)
                    }
                    (Some(body), None) => body.element_count(),
                    (None, _) => 0,
                };
                let is_done = row_count >= self.min_rows && row_count <= self.max_rows;

                self.metrics.poll_metric.store(row_count, std::sync::atomic::Ordering::Relaxed);

                if !is_done {
                    // Per-poll progress goes to the durable
                    // session log at Debug — direct `eprint!`
                    // here would clobber the TUI's render
                    // surface. The TUI surfaces poll progress
                    // via the `poll_metric` gauge (live row
                    // count) which is already updated above.
                    let indent = crate::scene_tree::running_phase_indent();
                    crate::diag!(
                        crate::observer::LogLevel::Debug,
                        "{indent}awaiting: {row_count} row(s), need [{}..={}] ({:.0}s elapsed)",
                        self.min_rows,
                        self.max_rows,
                        start.elapsed().as_secs_f64()
                    );
                }

                if is_done {
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
                    // Emit named metric. The recorded value is the
                    // elapsed wait duration; if `metric_name` carries
                    // a recognized unit suffix (`_ns` / `_us` / `_ms`
                    // / `_s` / `_m` / `_h`), the seconds are
                    // converted so the metric reads in the unit its
                    // name advertises. Names without a recognized
                    // suffix fall through as seconds (legacy
                    // behaviour, used by e.g. `index_build_time`).
                    if let Some(ref name) = self.metric_name {
                        let value = duration_value_for_metric_name(name, elapsed_secs);
                        captures.insert(name.clone(),
                            nbrs_variates::node::Value::F64(value));
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

impl WrappingDispenser for EmitDispenser {}

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

// =========================================================================
// ResultDispenser: SRD-40b §5 result-as-GK adapter
// =========================================================================

/// Wraps an inner OpDispenser to expose declared op-result fields
/// as GK named wires (SRD-40b §5).
///
/// Per cycle, after the inner adapter returns its `OpResult`, this
/// wrapper walks the op template's `result: HashMap<String,
/// ResultWireSpec>` declarations, computes each value from the
/// result body, and inserts it into `OpResult.captures` under the
/// wire name. Downstream synthesis writes those captures into the
/// next cycle's GkState (the same plumbing that
/// `TraversingDispenser` uses for `[name as alias]` capture
/// points), making them visible to every subsequent GK eval —
/// most notably the SRD-40b §5.2 metric-evaluation step.
///
/// Insertion order in the wrapper stack (SRD-40b §5.2): inner
/// adapter → ResultDispenser → MetricsDispenser (Phase E). The
/// metric wrappers see a fully-populated capture map.
///
/// Source grammars (SRD-40b §5.1):
/// - `count` — built-in; `OpResult::body.element_count()`.
/// - `ok` — built-in; `true` iff the inner adapter returned `Ok(_)`
///   (errors short-circuit before this wrapper's body runs, so
///   reaching this code already implies success — `ok` is `true`
///   unless the result was a skip).
/// - `<path-expr>` — JSON-pointer-style lookup into the result
///   body. Supports bare names (`field`), dotted paths
///   (`rows.0.field`), and bracketed indices (`rows[0].field`).
/// - `<gk-call>` — DEFERRED. Recognized as anything containing a
///   `(` token; currently logged once and skipped. Phase E or a
///   follow-up adds the GK-eval-against-result-context path.
pub struct ResultDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Map-shape `count` / `ok` / path-expr declarations that
    /// stay on the dispenser's evaluator (SRD-40b §5.1
    /// backwards compat). SRD-66 string-shape and gk-call
    /// entries are NOT here — they're compiled into the
    /// op-template kernel's body via SRD-67 Phase 5
    /// `add_result_bindings` and evaluated by GK; the dispenser
    /// just feeds them inputs through the
    /// `populate_kernel_inputs` flag.
    specs: Vec<ResultSlot>,
    /// SRD-67 Phase 5 — when the op's `result:` source contains
    /// any string-shape or gk-call entries, the dispenser writes
    /// the magic pre-bound inputs (`body` / `count` / `ok`) into
    /// `OpResult.captures` so the activity loop can feed them
    /// into the op-template kernel before the result-binding
    /// expressions evaluate. The kernel's closure-binding
    /// economy ignores writes for slots it doesn't reference,
    /// so unconditional writes are safe.
    populate_kernel_inputs: bool,
}

/// Parsed form of one `result:` declaration.
struct ResultSlot {
    /// Wire name (the map key in `ParsedOp.result`). Drives the
    /// `OpResult.captures` insertion key — downstream synthesis
    /// projects each capture into the matching GkState slot.
    wire: String,
    /// Decoded source grammar.
    source: ResultSource,
    /// Optional default rendered as a GK Value (string fallback)
    /// when the source resolves to nothing.
    default: Option<nbrs_variates::node::Value>,
}

/// Decoded SRD-40b §5.1 source grammar.
enum ResultSource {
    /// `count` — element count of the result body.
    Count,
    /// `ok` — success boolean.
    Ok,
    /// `<path-expr>` — JSON path into the result body, pre-parsed
    /// into segments.
    Path(Vec<PathSeg>),
    /// `<gk-call>` — deferred. Carries the raw source for the
    /// follow-up implementation.
    #[allow(dead_code)]
    GkCall(String),
}

/// One segment of a parsed path expression.
#[derive(Debug, Clone)]
enum PathSeg {
    Field(String),
    Index(usize),
}

/// Parse `rows[0].field` / `rows.0.field` / `field` into segments.
/// Returns `Err` for empty paths only — anything else parses as a
/// best-effort sequence of identifiers + indices.
fn parse_path_expr(src: &str) -> Result<Vec<PathSeg>, String> {
    let trimmed = src.trim();
    if trimmed.is_empty() {
        return Err("empty path".into());
    }
    let mut segs = Vec::new();
    let mut cur = String::new();
    let mut iter = trimmed.chars().peekable();
    let push_field = |segs: &mut Vec<PathSeg>, cur: &mut String| {
        if !cur.is_empty() {
            // Numeric bareword (after a dot) becomes an index.
            if let Ok(n) = cur.parse::<usize>() {
                segs.push(PathSeg::Index(n));
            } else {
                segs.push(PathSeg::Field(std::mem::take(cur)));
            }
            cur.clear();
        }
    };
    while let Some(&c) = iter.peek() {
        match c {
            '.' => {
                push_field(&mut segs, &mut cur);
                iter.next();
            }
            '[' => {
                push_field(&mut segs, &mut cur);
                iter.next();
                let mut idx = String::new();
                for c2 in iter.by_ref() {
                    if c2 == ']' { break; }
                    idx.push(c2);
                }
                let n: usize = idx.trim().parse().map_err(|_| {
                    format!("path '{src}': invalid index '[{idx}]'")
                })?;
                segs.push(PathSeg::Index(n));
            }
            _ => {
                cur.push(c);
                iter.next();
            }
        }
    }
    push_field(&mut segs, &mut cur);
    if segs.is_empty() {
        return Err(format!("path '{src}': no segments"));
    }
    Ok(segs)
}

/// Walk a parsed path against a JSON value. Returns `None` when
/// any segment misses (object lacks key, array shorter than index,
/// scalar where a container was expected).
fn resolve_path<'a>(
    json: &'a serde_json::Value,
    segs: &[PathSeg],
) -> Option<&'a serde_json::Value> {
    let mut cur = json;
    for seg in segs {
        cur = match (cur, seg) {
            (serde_json::Value::Object(m), PathSeg::Field(k)) => m.get(k)?,
            (serde_json::Value::Array(a),  PathSeg::Index(i)) => a.get(*i)?,
            // Bareword field on an array, or numeric on an object —
            // we don't try to coerce; the path doesn't match.
            _ => return None,
        };
    }
    Some(cur)
}

/// Decode one `(name, source-string)` pair from a SRD-66
/// map-shape `result:` fragment into a `ResultSlot`. Unknown
/// / unparseable sources land as `None` (caller logs and
/// drops them — SRD-40b §5.1 calls for "log a warning and
/// skip" over a hard failure here, since the value mechanism
/// is supposed to be best-effort per cycle).
///
/// String-shape and list-shape `result:` fragments don't go
/// through this function — they compile into the auxiliary
/// kernel under SRD-66 §"Compilation lifecycle" (TBD when
/// the structural-body Value variant lands).
fn decode_slot(
    name: &str,
    raw_source: &str,
) -> Option<ResultSlot> {
    let raw = raw_source.trim();
    let source = if raw == "count" {
        ResultSource::Count
    } else if raw == "ok" {
        ResultSource::Ok
    } else if raw.contains('(') {
        // SRD-66 Surface 1 — GK-expression form. The full
        // kernel-driven path (compile auxiliary kernel,
        // wire body/count/ok externs via the closure-binding
        // rule, evaluate per-cycle) is staged behind this
        // diagnostic until the structural-body Value variant
        // and op-template kernel extension land. Today the
        // form is recognised but evaluates to its default.
        crate::diag!(
            crate::observer::LogLevel::Warn,
            "result wire '{name}': GK-expression source '{raw}' is not yet \
             evaluated end-to-end — slot will resolve to its default. \
             SRD-66 Push 2 follow-up wires the kernel-driven path.",
        );
        ResultSource::GkCall(raw.to_string())
    } else {
        // Path expression. Parse failures degrade to skip.
        match parse_path_expr(raw) {
            Ok(segs) => ResultSource::Path(segs),
            Err(e) => {
                crate::diag!(
                    crate::observer::LogLevel::Warn,
                    "result wire '{name}': source '{raw}' is not parseable as a \
                     path expression ({e}) — slot will be skipped.",
                );
                return None;
            }
        }
    };
    Some(ResultSlot { wire: name.to_string(), source, default: None })
}

impl ResultDispenser {
    /// Wrap an inner dispenser with result-as-GK exposure for the
    /// op template's `result:` declarations (SRD-66). Returns the
    /// inner dispenser unchanged when `result:` is absent or
    /// empty (no overhead for ops that don't declare wires).
    ///
    /// Vari-structured `ResultSpec` shapes (string / list / map)
    /// flatten into `(wire, source)` pairs via
    /// [`ResultSpec::walk_fragments`]. Map-shape entries pass
    /// through `decode_slot`'s short-form dispatch. String-shape
    /// fragments are TODO at the kernel-driven path and emit a
    /// Warn diagnostic per fragment until that path lands.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        result_spec: Option<&nbrs_workload::model::ResultSpec>,
    ) -> Arc<dyn OpDispenser> {
        let Some(spec) = result_spec else { return inner; };
        if spec.is_empty() {
            return inner;
        }

        // SRD-66 / SRD-67 Phase 5 — split the spec into two
        // populations:
        //
        //   * map-shape `count` / `ok` / path-expr entries stay
        //     on the dispenser as before (legacy SRD-40b §5.1
        //     dispatch — keeps existing tests' path-expr capture
        //     semantics until they migrate to the kernel-driven
        //     form).
        //   * map-shape gk-call entries AND every entry under
        //     a string-shape source block are kernel-driven —
        //     `add_result_bindings` already compiled their
        //     bindings into the op-template kernel; the
        //     dispenser only needs to feed `body` / `count` /
        //     `ok` inputs per cycle.
        //
        // The `populate_kernel_inputs` flag fires when ANY
        // kernel-driven entries exist; the activity loop uses
        // it (via the captures map's magic keys) to drive the
        // op-template kernel's `set_input` + `commit_write_throughs`.
        let mut specs: Vec<ResultSlot> = Vec::new();
        let mut populate_kernel_inputs = false;

        spec.walk_fragments(|frag| match frag {
            nbrs_workload::model::ResultFragment::Named { name, source } => {
                let raw = source.trim();
                if raw == "count" || raw == "ok" {
                    if let Some(slot) = decode_slot(name, source) {
                        specs.push(slot);
                    }
                } else if raw.contains('(') {
                    // gk-call form — kernel-driven via SRD-67
                    // Phase 5. No dispenser-side dispatch.
                    populate_kernel_inputs = true;
                } else {
                    // Path expression — keep the legacy JSON-path
                    // path for SRD-40b §5.1 backwards compat.
                    if let Some(slot) = decode_slot(name, source) {
                        specs.push(slot);
                    }
                }
            }
            nbrs_workload::model::ResultFragment::Source(_source) => {
                // String-shape — fully kernel-driven.
                populate_kernel_inputs = true;
            }
        });

        if specs.is_empty() && !populate_kernel_inputs {
            return inner;
        }
        // Stable order so wire-resolution warnings (and the
        // per-cycle insertion order) are reproducible.
        specs.sort_by(|a, b| a.wire.cmp(&b.wire));
        Arc::new(Self {
            inner,
            specs,
            populate_kernel_inputs,
        })
    }

    /// Compute the GK value for one slot from the cycle's result.
    /// Returns `None` when the slot resolves to nothing and has no
    /// default — caller logs at debug and moves on.
    fn evaluate(
        slot: &ResultSlot,
        result: &OpResult,
    ) -> Option<nbrs_variates::node::Value> {
        match &slot.source {
            ResultSource::Count => {
                let n = result.body.as_ref().map(|b| b.element_count()).unwrap_or(0);
                Some(nbrs_variates::node::Value::U64(n))
            }
            ResultSource::Ok => {
                // Reached only on Ok(_) from the inner adapter; a
                // skipped op also counts as "not a failure" — we
                // treat skip as ok=true, matching the SRD-40b §5
                // intent that this is a binary success signal.
                Some(nbrs_variates::node::Value::Bool(true))
            }
            ResultSource::Path(segs) => {
                let body = result.body.as_ref()?;
                let json = body.to_json();
                resolve_path(&json, segs).map(json_to_value)
                    .or_else(|| slot.default.clone())
            }
            ResultSource::GkCall(_) => slot.default.clone(),
        }
    }
}

impl WrappingDispenser for ResultDispenser {}

impl OpDispenser for ResultDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = self.inner.execute(cycle, ctx).await?;
            // Skipped ops carry no body; per SRD-40b §5.2 the
            // metric pipeline doesn't fire on skips either, so
            // the only safe thing to write here is `ok=true` /
            // `count=0`. We write neither and let metric wrappers
            // see a clean capture map — the Phase E wrappers will
            // observe `result.skipped` and bail before evaluating.
            if result.skipped {
                return Ok(result);
            }
            for slot in &self.specs {
                if let Some(v) = Self::evaluate(slot, &result) {
                    result.captures.insert(slot.wire.clone(), v);
                }
                // Per-cycle missing-wire is silent. If a downstream
                // consumer (e.g. MetricsDispenser) references a wire
                // that didn't land in captures, that consumer
                // surfaces the failure as a hard ExecutionError —
                // logging it here would just add per-cycle session.log
                // spam without telling the user anything actionable.
            }

            // SRD-67 Phase 5 — magic-extern population. When the
            // op declares any kernel-driven result-bindings
            // (string-shape OR map-shape gk-call), inject the
            // standard `body` / `count` / `ok` inputs into
            // captures so the activity loop's
            // `write_op_template_input` step can route them to
            // the op-template kernel. The closure-binding economy
            // drops slots the kernel doesn't reference, so this
            // is safe even if the user's source only references
            // a subset.
            if self.populate_kernel_inputs {
                let count = result.body.as_ref().map(|b| b.element_count()).unwrap_or(0);
                // SRD-66 §"Surface 4 §Open: body type" resolved
                // to `Value::Json` — body rides the kernel as a
                // structural value so `exactly_one_value(body)`
                // can walk row × column shape (per
                // `nbrs-variates::nodes::exactly_one`). For ops
                // whose body has no structural projection the
                // adapter's `to_json()` returns a JSON String,
                // which `exactly_one_value` collapses to
                // `Value::Str`.
                let body_json = result
                    .body
                    .as_ref()
                    .map(|b| b.to_json())
                    .unwrap_or(serde_json::Value::Null);
                result
                    .captures
                    .entry("body".to_string())
                    .or_insert(nbrs_variates::node::Value::Json(body_json));
                result
                    .captures
                    .entry("count".to_string())
                    .or_insert(nbrs_variates::node::Value::U64(count));
                result
                    .captures
                    .entry("ok".to_string())
                    .or_insert(nbrs_variates::node::Value::Bool(true));
            }

            let _ = cycle;
            Ok(result)
        })
    }
}

// =========================================================================
// MetricsDispenser: SRD-40b §6 synthetic-metric recorder
// =========================================================================

/// Wraps an inner OpDispenser to publish per-cycle synthetic
/// metrics declared in the op template's `metrics:` map
/// (SRD-40b §6).
///
/// Per-cycle responsibilities (in order, matching SRD-40b §5.2 →
/// §6 pipeline):
///
/// 1. Await the inner dispenser's `execute`. With a
///    [`ResultDispenser`] in the wrapper stack between the inner
///    adapter and this one, declared `result:` wires are already
///    written to `OpResult.captures` by the time we run.
/// 2. For each declared metric, look up the value-producing GK
///    expression in the captures map (bare-binding-name canonical
///    form per SRD-40b §1).
/// 3. Apply the optional [`metric_format::FormatSpec`] sanitiser
///    to round to the configured precision (Phase B).
/// 4. Dispatch to the kind-specific instrument record method
///    (SRD-40b §6.1):
///    - [`MetricKind::Gauge`] → [`ValueGauge::set`] (f64).
///    - [`MetricKind::Histogram`] → [`Histogram::record`]
///      (truncated to u64 after format rounding).
///    - [`MetricKind::Counter`] → [`Counter::inc_by`] (u64);
///      non-positive values warn and skip — counters are
///      monotonic by definition.
///
/// **GkState access (the open question, pragmatically resolved)**
///
/// SRD-40b §6 names the per-cycle GK state as the eval target.
/// The wrapper doesn't get a direct handle on the per-fiber
/// `FiberBuilder` from `ExecCtx`, but it doesn't have to: the
/// canonical case for synthetic-metric workloads is a bare
/// binding name (SRD-40b §1: "bare binding name is the canonical
/// form"). For that case, the value lives in the freshly-written
/// `OpResult.captures` map produced by [`ResultDispenser`] (or by
/// any prior wrapper such as [`TraversingDispenser`]'s capture
/// extractor). We look it up there.
///
/// Non-bare-name expressions (`factor * 2.0`, `if(...)`, …) are
/// **deferred** to SRD-13d Phase 9 (op-dispenser kernel handle).
/// The wrapper carries a `value_expr: String` so the eventual
/// upgrade is a one-line swap from "captures lookup" to "GK
/// kernel eval against per-fiber state."
pub struct MetricsDispenser {
    inner: Arc<dyn OpDispenser>,
    /// One slot per declared metric. Stable ordering by metric
    /// name keeps per-cycle dispatch deterministic for tests and
    /// makes any per-cycle warning sequence reproducible.
    slots: Vec<MetricSlot>,
}

/// One compiled metric slot: instrument storage + sanitiser +
/// pre-bound GK pull handle.
struct MetricSlot {
    /// Family name registered with the [`Component`]. Used in
    /// diagnostic messages (e.g. the counter non-positive warning).
    family: String,
    /// The original `value:` text from the workload, kept for
    /// diagnostics. Per-cycle resolution goes through the
    /// pre-bound `pull_handle` below.
    value_expr: String,
    /// Optional value sanitiser. Applied after the value is
    /// pulled, before the instrument record.
    format: Option<nbrs_workload::metric_format::FormatSpec>,
    /// Resolved instrument storage — exactly one variant is
    /// populated per slot, matching `MetricSpec.kind`.
    instrument: MetricInstrument,
    /// Pre-bound handle into the per-cycle GK state. Every
    /// metric value flows through the GK kernel — if a name
    /// referenced in `value:` isn't in the kernel's wire
    /// vocabulary at wrap time, the wrap call errors before
    /// any cycle runs. Per the project's "GK Is Canonical Scope"
    /// rule (memory:feedback_gk_canonical_scope), there is one
    /// touch-point for value reads: the GK context. No sidecar
    /// captures-map fallback.
    pull_handle: crate::fixture::PullHandle,
}

/// Kind-specialised instrument storage owned by a [`MetricSlot`].
///
/// The same `Arc<...>` is shared with the dispenser's `Component`
/// instrument registry — registered via
/// `Component::register_instrument` at [`MetricsDispenser::wrap`] time.
/// Per-cycle code records through the slot's typed `Arc`; the cadence
/// reporter snapshots through the registry. One source of truth, two
/// access paths.
#[derive(Clone)]
enum MetricInstrument {
    Gauge(Arc<nbrs_metrics::instruments::gauge::ValueGauge>),
    Histogram(Arc<nbrs_metrics::instruments::histogram::Histogram>),
    Counter(Arc<nbrs_metrics::instruments::counter::Counter>),
}

impl MetricInstrument {
    /// Promote the kind-erased slot value into the canonical
    /// [`InstrumentRef`] for registry storage.
    fn as_ref(&self) -> nbrs_metrics::component::InstrumentRef {
        match self {
            MetricInstrument::Gauge(g) =>
                nbrs_metrics::component::InstrumentRef::Gauge(g.clone()),
            MetricInstrument::Histogram(h) =>
                nbrs_metrics::component::InstrumentRef::Histogram(h.clone()),
            MetricInstrument::Counter(c) =>
                nbrs_metrics::component::InstrumentRef::Counter(c.clone()),
        }
    }
}

/// Numeric coercion for capture-map lookups. Returns `None`
/// for non-numeric variants (string, vector, none) so the
/// MetricsDispenser slot path logs + skips rather than panicking
/// through `Value::as_f64`'s strict matcher.
fn value_to_f64(v: &nbrs_variates::node::Value) -> Option<f64> {
    match v {
        nbrs_variates::node::Value::F64(f) => Some(*f),
        nbrs_variates::node::Value::U64(u) => Some(*u as f64),
        nbrs_variates::node::Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

impl MetricsDispenser {
    /// Wrap an inner dispenser with synthetic-metric publication
    /// for the op template's `metrics:` declarations.
    ///
    /// Init steps (SRD-40b §6 init):
    /// 1. Empty declaration → return `inner` unchanged. No
    ///    overhead for ops that don't publish synthetic metrics.
    /// 2. For each `(name, spec)`, allocate the kind-specific
    ///    instrument and register it on the component via
    ///    `Component::register_instrument`. A duplicate-family
    ///    collision (§7.2) errors here, before any cycle runs.
    /// 3. Pre-parse the optional `format:` string into a
    ///    [`FormatSpec`].
    ///
    /// `component` is borrowed mutably so `register_instrument`
    /// can claim the family slot atomically with the instrument
    /// allocation. The same `Arc<...>` is held both on the
    /// component (for cadence-reporter capture) and in the
    /// returned dispenser's slots (for per-cycle record).
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        metrics: &HashMap<String, nbrs_workload::model::MetricSpec>,
        component: &mut nbrs_metrics::component::Component,
        fx: &mut crate::fixture::ScopeFixture,
    ) -> Result<Arc<dyn OpDispenser>, String> {
        if metrics.is_empty() {
            return Ok(inner);
        }
        // Stable ordering on metric names so init-time
        // diagnostics + per-cycle dispatch are reproducible.
        let mut entries: Vec<_> = metrics.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        let component_labels = component.effective_labels().clone();
        let mut slots = Vec::with_capacity(entries.len());
        for (name, spec) in entries {
            let family = spec.family.clone().unwrap_or_else(|| name.clone());

            let format = match &spec.format {
                Some(s) => Some(
                    nbrs_workload::metric_format::parse_format_spec(s)
                        .map_err(|e| format!("metric '{name}' format: {e}"))?,
                ),
                None => None,
            };

            let kind = spec.kind.unwrap_or_default();
            // Instrument labels carry the family name as a label
            // alongside the component's effective labels. The
            // `family` argument to `register_instrument` is the
            // canonical family-name string; the labels are the
            // dimensional cell.
            let instr_labels = component_labels.with("family", family.clone());
            let instrument = match kind {
                nbrs_workload::model::MetricKind::Gauge => {
                    MetricInstrument::Gauge(Arc::new(
                        nbrs_metrics::instruments::gauge::ValueGauge::new(instr_labels),
                    ))
                }
                nbrs_workload::model::MetricKind::Histogram => {
                    MetricInstrument::Histogram(Arc::new(
                        nbrs_metrics::instruments::histogram::Histogram::new(instr_labels),
                    ))
                }
                nbrs_workload::model::MetricKind::Counter => {
                    MetricInstrument::Counter(Arc::new(
                        nbrs_metrics::instruments::counter::Counter::new(instr_labels),
                    ))
                }
            };

            // Resolve `value:` against the GK kernel up front. The
            // GK context is the sole touch-point for reads in the
            // op's scope (project rule "GK Is Canonical Scope"); if
            // the kernel doesn't know the name at wrap time, the
            // workload is referencing a wire that no binding /
            // input / `result:` capture produced — surface as a
            // hard init error before any cycle runs.
            if !is_bare_name(&spec.value) {
                return Err(format!(
                    "metric '{name}' value '{value}' is not a bare \
                     binding name — non-bare expressions are deferred \
                     to SRD-13d Phase 9 (op-dispenser kernel handle). \
                     Either rename the metric's `value:` to a single \
                     binding, or wait for Phase 9.",
                    value = spec.value,
                ));
            }
            let pull_handle = fx.register_pull(spec.value.trim()).map_err(|e| {
                format!(
                    "metric '{name}' value '{value}': {e}",
                    value = spec.value,
                )
            })?;

            // SRD-40b §7.2 — collide-on-duplicate at init. The
            // single registry on `Component` is the canonical
            // store; the slot's `Arc<...>` shares the same
            // instrument for the per-cycle hot path. The
            // optional `unit` rides through to drive the
            // `_<unit>` suffix on `metric_family.name` and the
            // `unit` column at capture time (SRD-40a §4.3).
            component.register_instrument_with_unit(
                family.clone(),
                spec.unit.clone(),
                instrument.as_ref(),
            )?;

            slots.push(MetricSlot {
                family,
                value_expr: spec.value.clone(),
                format,
                instrument,
                pull_handle,
            });
        }

        Ok(Arc::new(Self { inner, slots }))
    }
}

/// Predicate for an SRD-40b §1 "bare binding name": a single
/// ident-shaped token. Whitespace is allowed around the edges
/// but not inside.
/// Convert an elapsed duration in seconds to the unit advertised
/// by a metric name's suffix. Recognised suffixes:
///
/// - `_ns` → nanoseconds
/// - `_us` → microseconds  (ASCII `u`; not `μ`)
/// - `_ms` → milliseconds
/// - `_s`  → seconds (identity)
/// - `_m`  → minutes
/// - `_h`  → hours
///
/// Names without a recognised suffix fall through as seconds
/// (preserves the historical contract — e.g. `index_build_time`
/// used to be emitted raw in seconds and still is).
///
/// Longest suffixes are tested first so `_ms` doesn't tail-bind
/// to a more-permissive `_s` rule by accident.
fn duration_value_for_metric_name(name: &str, elapsed_secs: f64) -> f64 {
    if name.ends_with("_ns") { elapsed_secs * 1e9 }
    else if name.ends_with("_us") { elapsed_secs * 1e6 }
    else if name.ends_with("_ms") { elapsed_secs * 1e3 }
    else if name.ends_with("_s")  { elapsed_secs }
    else if name.ends_with("_m")  { elapsed_secs / 60.0 }
    else if name.ends_with("_h")  { elapsed_secs / 3600.0 }
    else { elapsed_secs }
}

/// Drill into a JSON tree via JSON-Pointer path (RFC 6901, e.g.
/// `/value`, `/value/results/0`) and reduce the addressed
/// sub-tree to a u64 count for the polling threshold:
///
/// - Array → `len()` (use case: "list of running jobs is empty").
/// - Number → the integer value (use case: a numeric counter
///   like `Compaction.PendingTasks.Value` reaches zero).
/// - Object → 1 (the addressed payload exists; for "wait until
///   *something* is present" patterns).
/// - Null / missing path → 0 (treat as "nothing there").
///
/// An empty path string addresses the root, matching
/// `serde_json::Value::pointer("")`'s contract.
fn count_from_json_pointer(json: &serde_json::Value, path: &str) -> u64 {
    let Some(v) = json.pointer(path) else { return 0 };
    match v {
        serde_json::Value::Array(a) => a.len() as u64,
        serde_json::Value::Number(n) => {
            n.as_u64()
                .or_else(|| n.as_i64().map(|i| i.max(0) as u64))
                .or_else(|| n.as_f64().map(|f| f.max(0.0) as u64))
                .unwrap_or(0)
        }
        serde_json::Value::Object(_) => 1,
        serde_json::Value::Bool(b) => if *b { 1 } else { 0 },
        serde_json::Value::String(s) if s.is_empty() => 0,
        serde_json::Value::String(_) => 1,
        serde_json::Value::Null => 0,
    }
}

fn is_bare_name(expr: &str) -> bool {
    let trimmed = expr.trim();
    !trimmed.is_empty()
        && trimmed.chars().all(|c| c.is_alphanumeric() || c == '_')
}

impl WrappingDispenser for MetricsDispenser {}

impl OpDispenser for MetricsDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self.inner.execute(cycle, ctx).await?;
            // Skipped ops produce no measurement — SRD-40b §5.2 /
            // §6 pipeline only fires on a successfully-executed op.
            if result.skipped {
                return Ok(result);
            }
            for slot in &self.slots {
                // Sole resolution path: pre-bound `PullHandle`
                // into the per-cycle GK state. The GK context is
                // the canonical scope (project rule
                // "GK Is Canonical Scope") — there is no second
                // way to fetch a value here.
                let value = ctx.pulls.get(slot.pull_handle);
                let raw = match value_to_f64(value) {
                    Some(v) => v,
                    None => {
                        // The wire resolved but its type can't
                        // coerce to a numeric metric value (Str,
                        // vector, handle, etc.). Surface as a
                        // hard ExecutionError so the activity's
                        // `errors:` policy decides — by default
                        // (errors=stop) the phase + run halt.
                        return Err(ExecutionError::Op(crate::adapter::AdapterError {
                            error_name: "metric_value_non_numeric".into(),
                            message: format!(
                                "metric '{family}' on cycle {cycle}: \
                                 binding '{expr}' is not coercible to f64 \
                                 (got value variant {disc:?}); metric \
                                 values must be numeric (U64 / F64 / Bool)",
                                family = slot.family,
                                expr = slot.value_expr,
                                disc = std::mem::discriminant(value),
                            ),
                            retryable: false,
                        }));
                    }
                };
                let sanitised = slot.format.as_ref().map(|f| f.apply(raw)).unwrap_or(raw);
                match &slot.instrument {
                    MetricInstrument::Gauge(g) => g.set(sanitised),
                    MetricInstrument::Histogram(h) => h.record(sanitised as u64),
                    MetricInstrument::Counter(c) => {
                        if sanitised <= 0.0 {
                            crate::diag!(
                                crate::observer::LogLevel::Warn,
                                "counter '{}' got non-positive value {sanitised}; skipping",
                                slot.family,
                            );
                        } else {
                            c.inc_by(sanitised as u64);
                        }
                    }
                }
            }
            Ok(result)
        })
    }
}

// =========================================================================
// MemoDispenser: operator-visible phase memo
// =========================================================================

/// Op-wrapper whose only side effect is to publish a short
/// human-visible string to the activity's `memo` ArcSwap.
///
/// Two templates are accepted:
///
/// - `before`: rendered + stored *before* the inner op runs.
///   Useful for "now compacting {table}" style state — reads
///   workload params / cycle wires that exist pre-execution.
/// - `after`: rendered + stored *after* the inner op returns
///   Ok. Lets the next-rendered memo reflect the post-op state.
///
/// Either or both may be present. A shorthand string form
/// (`memo: "doing X"`) is parsed as both-templates-the-same.
///
/// The wrapper is a no-op on inner errors — the result is
/// returned unchanged whether or not memo publication happened.
/// Substitution failures are downgraded to a debug log; we
/// don't fail an otherwise-good op because the memo couldn't
/// render.
pub struct MemoDispenser {
    inner: Arc<dyn OpDispenser>,
    before_template: Option<String>,
    after_template: Option<String>,
    /// Shared atomic owned by the activity (see
    /// `Activity::memo`). Cloned into the wrapper at wrap-time
    /// so writes here are visible to the inline-status thread
    /// and end-of-phase readout context without a separate
    /// channel.
    memo_state: Arc<arc_swap::ArcSwap<String>>,
}

impl MemoDispenser {
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        before_template: Option<String>,
        after_template: Option<String>,
        memo_state: Arc<arc_swap::ArcSwap<String>>,
    ) -> Arc<dyn OpDispenser> {
        Arc::new(Self {
            inner,
            before_template,
            after_template,
            memo_state,
        })
    }

    fn publish(&self, template: &str, wires: &dyn crate::wires::WireSource) {
        match crate::wires::substitute_via_wires(template, wires) {
            Ok(rendered) => {
                self.memo_state.store(Arc::new(rendered));
            }
            Err(e) => {
                crate::diag!(crate::observer::LogLevel::Debug,
                    "memo: substitution failed for '{template}': {e}");
            }
        }
    }
}

impl WrappingDispenser for MemoDispenser {}

impl OpDispenser for MemoDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> {
        Some(self.inner.as_ref())
    }

    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(t) = &self.before_template {
                self.publish(t, ctx.wires);
            }
            let result = self.inner.execute(cycle, ctx).await?;
            if let Some(t) = &self.after_template {
                self.publish(t, ctx.wires);
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

    // ---------------- ResultDispenser tests (SRD-40b §5) ----------------

    use crate::adapter::AdapterError;
    use crate::fixture::{ExecCtx, ResolvedPulls};
    #[allow(unused_imports)]
    use nbrs_workload::model::ResultSpec;

    /// Minimal `ResultBody` carrying a JSON value and a configurable
    /// element count, so tests can exercise the `count` built-in.
    #[derive(Debug)]
    struct ResultDispBody {
        value: serde_json::Value,
        count: u64,
    }
    impl ResultBody for ResultDispBody {
        fn to_json(&self) -> serde_json::Value { self.value.clone() }
        fn as_any(&self) -> &dyn std::any::Any { self }
        fn element_count(&self) -> u64 { self.count }
    }

    /// A canned-result inner dispenser. `body` controls the
    /// successful path; `error` short-circuits to an `ExecutionError`.
    struct FakeInner {
        body: Option<ResultDispBody>,
        error: Option<&'static str>,
    }

    impl OpDispenser for FakeInner {
        fn execute<'a>(
            &'a self,
            _cycle: u64,
            _ctx: &'a ExecCtx<'a>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            Box::pin(async move {
                if let Some(msg) = self.error {
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: "test".into(),
                        message: msg.into(),
                        retryable: false,
                    }));
                }
                Ok(OpResult {
                    body: self.body.as_ref().map(|b| Box::new(ResultDispBody {
                        value: b.value.clone(),
                        count: b.count,
                    }) as Box<dyn ResultBody>),
                    captures: HashMap::new(),
                    skipped: false,
                })
            })
        }
    }

    /// Build a default `ExecCtx` for tests that don't read fields/pulls.
    fn empty_ctx() -> (crate::adapter::ResolvedFields, ResolvedPulls) {
        let fields = crate::adapter::ResolvedFields::new(vec![], vec![]);
        let pulls = ResolvedPulls::empty();
        (fields, pulls)
    }

    #[tokio::test]
    async fn memo_wrapper_publishes_before_and_after() {
        // The wrapper renders both templates through wires and
        // stores them in the ArcSwap. With no `{name}` tokens
        // the templates pass through as literals.
        let memo = Arc::new(arc_swap::ArcSwap::from_pointee(String::new()));
        let inner = Arc::new(FakeInner { body: None, error: None });
        let dispenser = MemoDispenser::wrap(
            inner,
            Some("before-state".into()),
            Some("after-state".into()),
            memo.clone(),
        );
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        // Execution writes BOTH templates — by the time
        // execute() returns the after-template has overwritten
        // the before-template.
        let _ = dispenser.execute(0, &ctx).await.expect("inner ok");
        assert_eq!(memo.load().as_str(), "after-state");
    }

    #[tokio::test]
    async fn memo_wrapper_only_before_when_after_unset() {
        // Single-shot: only `before:` configured. The wrapper
        // writes once and leaves the value in place across
        // the inner call.
        let memo = Arc::new(arc_swap::ArcSwap::from_pointee(String::new()));
        let inner = Arc::new(FakeInner { body: None, error: None });
        let dispenser = MemoDispenser::wrap(
            inner,
            Some("ready".into()),
            None,
            memo.clone(),
        );
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let _ = dispenser.execute(0, &ctx).await.expect("inner ok");
        assert_eq!(memo.load().as_str(), "ready");
    }

    #[tokio::test]
    async fn memo_wrapper_does_not_run_after_on_inner_error() {
        // Inner returns Err → wrapper propagates without
        // running the `after` template. Memo holds the
        // before-value (which represents the "we tried to do
        // X" state).
        let memo = Arc::new(arc_swap::ArcSwap::from_pointee(String::new()));
        let inner = Arc::new(FakeInner {
            body: None,
            error: Some("boom"),
        });
        let dispenser = MemoDispenser::wrap(
            inner,
            Some("attempting".into()),
            Some("finished".into()),
            memo.clone(),
        );
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let res = dispenser.execute(0, &ctx).await;
        assert!(res.is_err());
        assert_eq!(memo.load().as_str(), "attempting",
            "after-template must not run on inner error");
    }

    #[test]
    fn parse_path_dotted_and_bracketed_equivalent() {
        let a = parse_path_expr("rows[0].value").unwrap();
        let b = parse_path_expr("rows.0.value").unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 3);
        match (&a[0], &b[0]) {
            (PathSeg::Field(f1), PathSeg::Field(f2)) => assert_eq!(f1, f2),
            _ => panic!("expected leading field"),
        }
        match (&a[1], &b[1]) {
            (PathSeg::Index(0), PathSeg::Index(0)) => {}
            _ => panic!("expected index 0"),
        }
    }

    /// Build a SRD-66 map-shape `ResultSpec` from
    /// `(name, source)` pairs.
    fn map_spec(entries: &[(&str, &str)]) -> nbrs_workload::model::ResultSpec {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in entries {
            m.insert((*k).to_string(), (*v).to_string());
        }
        nbrs_workload::model::ResultSpec::Map(m)
    }

    #[test]
    fn result_dispenser_count_and_path() {
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody {
                value: serde_json::json!({"rows": [{"value": 42}]}),
                count: 1,
            }),
            error: None,
        });
        let decl = map_spec(&[
            ("row_count", "count"),
            ("first_value", "rows[0].value"),
        ]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let result = rt.block_on(wrapped.execute(0, &ctx)).unwrap();

        assert_eq!(result.captures["row_count"].as_u64(), 1);
        assert_eq!(result.captures["first_value"].as_u64(), 42);
    }

    #[test]
    fn result_dispenser_ok_builtin_on_success() {
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody { value: serde_json::json!({}), count: 0 }),
            error: None,
        });
        let decl = map_spec(&[("succeeded", "ok")]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let result = rt.block_on(wrapped.execute(0, &ctx)).unwrap();
        match &result.captures["succeeded"] {
            nbrs_variates::node::Value::Bool(b) => assert!(*b),
            other => panic!("expected Bool(true), got {other:?}"),
        }
    }

    #[test]
    fn result_dispenser_error_propagates_no_capture_write() {
        // On inner error the wrapper short-circuits — `ok` is
        // never written. SRD-40b §5.2 leaves the failure path
        // to the metrics wrapper / error router; the result-as-
        // GK adapter only writes on Ok(_).
        let inner = Arc::new(FakeInner {
            body: None,
            error: Some("boom"),
        });
        let decl = map_spec(&[("succeeded", "ok")]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let err = rt.block_on(wrapped.execute(0, &ctx)).unwrap_err();
        assert!(format!("{err}").contains("boom"));
    }

    #[test]
    fn result_dispenser_unresolved_path_skips_silently() {
        // SRD-66 dropped the legacy `Object { source, default }`
        // form. An unresolvable path now logs a Warn and the
        // wire is absent from the captures map (no default
        // fallback).
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody {
                value: serde_json::json!({"rows": []}),
                count: 0,
            }),
            error: None,
        });
        let decl = map_spec(&[
            ("missing", "rows[0].value"),
        ]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let result = rt.block_on(wrapped.execute(0, &ctx)).unwrap();

        assert!(!result.captures.contains_key("missing"));
    }

    #[test]
    fn result_dispenser_empty_decl_returns_inner_unchanged() {
        let inner: Arc<dyn OpDispenser> = Arc::new(FakeInner {
            body: Some(ResultDispBody { value: serde_json::json!({}), count: 0 }),
            error: None,
        });
        let inner_ptr = Arc::as_ptr(&inner);
        let wrapped = ResultDispenser::wrap(inner.clone(), None);
        // Empty declaration short-circuits — the wrapper returns
        // the inner Arc itself, not a fresh `ResultDispenser`.
        assert_eq!(Arc::as_ptr(&wrapped), inner_ptr);
    }

    #[test]
    fn result_dispenser_skipped_op_writes_no_captures() {
        struct SkipInner;
        impl OpDispenser for SkipInner {
            fn execute<'a>(
                &'a self,
                _cycle: u64,
                _ctx: &'a ExecCtx<'a>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
                Box::pin(async move { Ok(OpResult::skipped()) })
            }
        }
        let decl = map_spec(&[("c", "count")]);
        let wrapped = ResultDispenser::wrap(Arc::new(SkipInner), Some(&decl));
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let result = rt.block_on(wrapped.execute(0, &ctx)).unwrap();
        assert!(result.skipped);
        assert!(result.captures.is_empty());
    }

    // ---------------- MetricsDispenser tests (SRD-40b §6) ----------------

    use nbrs_workload::model::{MetricKind, MetricSpec};

    /// An inner dispenser that returns a pre-baked captures map,
    /// simulating the state Phase D's `ResultDispenser` would
    /// hand to the metrics wrapper.
    struct CapturesInner {
        captures: HashMap<String, nbrs_variates::node::Value>,
    }
    impl OpDispenser for CapturesInner {
        fn execute<'a>(
            &'a self,
            _cycle: u64,
            _ctx: &'a ExecCtx<'a>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            let captures = self.captures.clone();
            Box::pin(async move {
                Ok(OpResult { body: None, captures, skipped: false })
            })
        }
    }

    fn fresh_component() -> nbrs_metrics::component::Component {
        nbrs_metrics::component::Component::new(
            nbrs_metrics::labels::Labels::empty(),
            HashMap::new(),
        )
    }

    /// Build a tiny `ScopeFixture` over a one-input `cycle` program
    /// for tests that just need a fixture argument to satisfy the
    /// `MetricsDispenser::wrap` signature. The slot-level
    /// `pull_handle` stays `None` for any binding name that isn't
    /// in this minimal program — tests exercise the captures-lookup
    /// fallback path instead.
    fn fresh_fixture() -> crate::fixture::ScopeFixture {
        use nbrs_variates::assembly::{GkAssembler, WireRef};
        use nbrs_variates::nodes::identity::Identity;
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("cycle_id", Box::new(Identity::new()), vec![WireRef::input("cycle")]);
        asm.add_output("cycle_id", WireRef::node("cycle_id"));
        let kernel = asm.compile().expect("test fixture asm.compile");
        crate::fixture::ScopeFixture::new(kernel.program().clone())
    }

    fn make_spec(value: &str, kind: MetricKind, format: Option<&str>) -> MetricSpec {
        MetricSpec {
            value: value.to_string(),
            family: None,
            kind: Some(kind),
            unit: None,
            format: format.map(|s| s.to_string()),
        }
    }

    #[test]
    fn metrics_dispenser_empty_returns_inner_unchanged() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner {
            captures: HashMap::new(),
        });
        let inner_ptr = Arc::as_ptr(&inner);
        let mut comp = fresh_component();
        let mut fx = fresh_fixture();
        let wrapped = MetricsDispenser::wrap(
            inner.clone(), &HashMap::new(), &mut comp, &mut fx,
        ).unwrap();
        // Empty declaration short-circuits — wrapper returns the
        // inner Arc itself, not a fresh `MetricsDispenser`.
        assert_eq!(Arc::as_ptr(&wrapped), inner_ptr);
    }

    /// Test-only introspection: peek at allocated instrument
    /// `Arc`s by family name. Tests need this because `wrap`
    /// returns `Arc<dyn OpDispenser>` and we want to assert
    /// against the same `ValueGauge` / `Histogram` / `Counter`
    /// the wrapper writes through.
    impl MetricsDispenser {
        #[cfg(test)]
        fn slot_gauge(&self, family: &str) -> Option<Arc<nbrs_metrics::instruments::gauge::ValueGauge>> {
            self.slots.iter().find(|s| s.family == family).and_then(|s| match &s.instrument {
                MetricInstrument::Gauge(g) => Some(g.clone()),
                _ => None,
            })
        }
        #[cfg(test)]
        fn slot_histogram(&self, family: &str) -> Option<Arc<nbrs_metrics::instruments::histogram::Histogram>> {
            self.slots.iter().find(|s| s.family == family).and_then(|s| match &s.instrument {
                MetricInstrument::Histogram(h) => Some(h.clone()),
                _ => None,
            })
        }
        #[cfg(test)]
        fn slot_counter(&self, family: &str) -> Option<Arc<nbrs_metrics::instruments::counter::Counter>> {
            self.slots.iter().find(|s| s.family == family).and_then(|s| match &s.instrument {
                MetricInstrument::Counter(c) => Some(c.clone()),
                _ => None,
            })
        }
    }

    /// Build a one-input GK kernel whose outputs are the given
    /// `(name, value)` constants. Used by dispenser tests so each
    /// metric `value:` reference resolves to a per-cycle value
    /// through the canonical GK pull path. Returns the kernel +
    /// fixture; caller seals the fixture after wrapping the
    /// dispenser, then resolves pulls against the kernel state.
    fn kernel_with_const_outputs(
        consts: &[(&str, f64)],
    ) -> (
        nbrs_variates::kernel::GkKernel,
        crate::fixture::ScopeFixture,
    ) {
        use nbrs_variates::assembly::{GkAssembler, WireRef};
        use nbrs_variates::nodes::fixed::ConstF64;
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        for (name, val) in consts {
            asm.add_node(*name, Box::new(ConstF64::new(*val)), vec![]);
            asm.add_output(*name, WireRef::node(*name));
        }
        let kernel = asm.compile().expect("test kernel asm.compile");
        let fx = crate::fixture::ScopeFixture::new(kernel.program().clone());
        (kernel, fx)
    }

    /// Mirrors `MetricsDispenser::wrap` for the non-empty-decl
    /// case but returns a typed `Arc<MetricsDispenser>` so tests
    /// can read instrument values via `slot_*` accessors. Also
    /// builds the resolved pulls so the caller can run
    /// `execute()` against a real cycle context.
    fn typed_wrap_with_kernel(
        inner: Arc<dyn OpDispenser>,
        decls: &HashMap<String, MetricSpec>,
        consts: &[(&str, f64)],
    ) -> Result<
        (
            Arc<MetricsDispenser>,
            crate::fixture::ResolvedPulls,
        ),
        String,
    > {
        let (mut kernel, mut fx) = kernel_with_const_outputs(consts);
        let mut comp = fresh_component();

        // Replicate the production wrap body so the returned
        // handle is typed. Same registration order (sorted by
        // metric name) so handle-indexes line up with what the
        // production path produces.
        if decls.is_empty() {
            return Err("typed_wrap_with_kernel requires non-empty decls".into());
        }
        let mut entries: Vec<_> = decls.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let component_labels = comp.effective_labels().clone();
        let mut slots = Vec::with_capacity(entries.len());
        for (name, spec) in entries {
            let family = spec.family.clone().unwrap_or_else(|| name.clone());
            let format = match &spec.format {
                Some(s) => Some(
                    nbrs_workload::metric_format::parse_format_spec(s)
                        .map_err(|e| format!("metric '{name}' format: {e}"))?,
                ),
                None => None,
            };
            let kind = spec.kind.unwrap_or_default();
            let instr_labels = component_labels.with("family", family.clone());
            let instrument = match kind {
                MetricKind::Gauge => MetricInstrument::Gauge(Arc::new(
                    nbrs_metrics::instruments::gauge::ValueGauge::new(instr_labels),
                )),
                MetricKind::Histogram => MetricInstrument::Histogram(Arc::new(
                    nbrs_metrics::instruments::histogram::Histogram::new(instr_labels),
                )),
                MetricKind::Counter => MetricInstrument::Counter(Arc::new(
                    nbrs_metrics::instruments::counter::Counter::new(instr_labels),
                )),
            };
            comp.register_instrument_with_unit(
                family.clone(), spec.unit.clone(), instrument.as_ref(),
            )?;
            if !is_bare_name(&spec.value) {
                return Err(format!(
                    "metric '{name}' value '{}' is not a bare binding name",
                    spec.value,
                ));
            }
            let pull_handle = fx.register_pull(spec.value.trim())?;
            slots.push(MetricSlot {
                family,
                value_expr: spec.value.clone(),
                format,
                instrument,
                pull_handle,
            });
        }
        let typed = Arc::new(MetricsDispenser { inner, slots });

        let plan = fx.seal();
        kernel.set_inputs(&[0]);
        let pulls = plan.resolve_with(&mut kernel);
        Ok((typed, pulls))
    }

    /// Run `dispenser.execute(0, ctx)` to completion against
    /// pulls + empty fields. Returns the result.
    fn run_dispenser(
        dispenser: Arc<dyn OpDispenser>,
        pulls: &crate::fixture::ResolvedPulls,
    ) -> Result<OpResult, ExecutionError> {
        let fields = crate::adapter::ResolvedFields::new(vec![], vec![]);
        let ctx = ExecCtx::new(&fields, pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dispenser.execute(0, &ctx))
    }

    #[test]
    fn metrics_dispenser_gauge_records_f64() {
        // The kernel produces `my_factor = 3.14` as an output;
        // the metric's `value: my_factor` resolves to that wire
        // through the GK pull plan.
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures: HashMap::new() });
        let mut decl = HashMap::new();
        decl.insert("my_factor".into(), make_spec("my_factor", MetricKind::Gauge, None));

        let (typed, pulls) = typed_wrap_with_kernel(
            inner, &decl, &[("my_factor", 3.14)],
        ).unwrap();
        let gauge = typed.slot_gauge("my_factor").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls).unwrap();

        assert!((gauge.get() - 3.14).abs() < 1e-9);
    }

    #[test]
    fn metrics_dispenser_histogram_truncates_to_u64() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures: HashMap::new() });
        let mut decl = HashMap::new();
        decl.insert("latency_ms".into(), make_spec("latency_ms", MetricKind::Histogram, None));

        let (typed, pulls) = typed_wrap_with_kernel(
            inner, &decl, &[("latency_ms", 7.9)],
        ).unwrap();
        let hist = typed.slot_histogram("latency_ms").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls).unwrap();

        // Truncated 7.9 -> 7. Histogram snapshot's max recorded.
        let snap = hist.peek_snapshot();
        assert_eq!(snap.max(), 7);
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn metrics_dispenser_counter_positive_inc_and_skip_non_positive() {
        // Two counters: one positive, one non-positive (zero).
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures: HashMap::new() });
        let mut decl = HashMap::new();
        decl.insert("ok_inc".into(), make_spec("ok_inc", MetricKind::Counter, None));
        decl.insert("skip_inc".into(), make_spec("skip_inc", MetricKind::Counter, None));

        let (typed, pulls) = typed_wrap_with_kernel(
            inner, &decl, &[("ok_inc", 5.0), ("skip_inc", 0.0)],
        ).unwrap();
        let ok_counter = typed.slot_counter("ok_inc").unwrap();
        let skip_counter = typed.slot_counter("skip_inc").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls).unwrap();

        assert_eq!(ok_counter.get(), 5);
        // Non-positive value warns and skips — counter stays at 0.
        assert_eq!(skip_counter.get(), 0);
    }

    #[test]
    fn metrics_dispenser_format_rounds_value() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures: HashMap::new() });
        let mut decl = HashMap::new();
        decl.insert(
            "ratio".into(),
            make_spec("ratio", MetricKind::Gauge, Some("#.##")),
        );

        let (typed, pulls) = typed_wrap_with_kernel(
            inner, &decl, &[("ratio", 1.234)],
        ).unwrap();
        let gauge = typed.slot_gauge("ratio").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls).unwrap();

        assert!((gauge.get() - 1.23).abs() < 1e-9);
    }

    #[test]
    fn metrics_dispenser_duplicate_family_errors() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures: HashMap::new() });
        let mut comp = fresh_component();
        // Pre-claim the family by registering an instrument under
        // it — the wrapper's `register_instrument` call should now
        // collide and surface SRD-40b §7.2 error.
        comp.register_instrument(
            "recall_at_10",
            nbrs_metrics::component::InstrumentRef::Counter(Arc::new(
                nbrs_metrics::instruments::counter::Counter::new(
                    nbrs_metrics::labels::Labels::of("name", "recall_at_10"),
                ),
            )),
        ).unwrap();

        let mut decl = HashMap::new();
        decl.insert("recall_at_10".into(), make_spec("recall_at_10", MetricKind::Gauge, None));

        let (_kernel, mut fx) = kernel_with_const_outputs(&[("recall_at_10", 0.0)]);
        let err = match MetricsDispenser::wrap(inner, &decl, &mut comp, &mut fx) {
            Ok(_) => panic!("expected duplicate-family error, got Ok"),
            Err(e) => e,
        };
        assert!(err.contains("duplicate family name"), "unexpected error: {err}");
    }

    #[test]
    fn metrics_dispenser_skipped_op_records_nothing() {
        struct SkipInner;
        impl OpDispenser for SkipInner {
            fn execute<'a>(
                &'a self,
                _cycle: u64,
                _ctx: &'a ExecCtx<'a>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
                Box::pin(async move { Ok(OpResult::skipped()) })
            }
        }
        let mut decl = HashMap::new();
        decl.insert("g".into(), make_spec("g", MetricKind::Gauge, None));

        let (typed, pulls) = typed_wrap_with_kernel(
            Arc::new(SkipInner), &decl, &[("g", 1.0)],
        ).unwrap();
        let gauge = typed.slot_gauge("g").unwrap();

        let res = run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls).unwrap();
        assert!(res.skipped);
        // Gauge default value untouched.
        assert_eq!(gauge.get(), 0.0);
    }

    #[test]
    fn metrics_dispenser_non_bare_expr_errors_at_init() {
        // Inline expression like "factor * 2.0" is deferred to
        // SRD-13d Phase 9. The GK pull-handle resolution at wrap()
        // time rejects it with a clear init error, so the workload
        // fails fast — no cycles run.
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures: HashMap::new() });
        let mut decl = HashMap::new();
        decl.insert(
            "computed".into(),
            make_spec("factor * 2.0", MetricKind::Gauge, None),
        );

        let (_kernel, mut fx) = kernel_with_const_outputs(&[("factor", 3.0)]);
        let mut comp = fresh_component();
        let err = MetricsDispenser::wrap(inner, &decl, &mut comp, &mut fx)
            .err()
            .expect("non-bare expr should error at init");
        assert!(err.contains("computed"), "msg: {err}");
        assert!(err.contains("not a bare binding name"), "msg: {err}");
    }

    #[test]
    fn metrics_dispenser_missing_wire_errors_at_init() {
        // value: declares a bare name that no binding produces.
        // The fixture's `register_pull` errors with a list of
        // available outputs/inputs — surface that to the workload
        // author at init time.
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures: HashMap::new() });
        let mut decl = HashMap::new();
        decl.insert(
            "missing_metric".into(),
            make_spec("absent_wire", MetricKind::Gauge, None),
        );

        let (_kernel, mut fx) = kernel_with_const_outputs(&[("present", 1.0)]);
        let mut comp = fresh_component();
        let err = MetricsDispenser::wrap(inner, &decl, &mut comp, &mut fx)
            .err()
            .expect("missing-wire metric should error at init");
        assert!(err.contains("absent_wire"), "msg: {err}");
        // The fixture error includes "Available outputs" / "inputs".
        assert!(err.contains("Available"), "msg: {err}");
    }
}
