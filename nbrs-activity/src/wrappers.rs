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
    specs: Vec<ResultSlot>,
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

/// Decode one `(name, ResultWireSpec)` pair into a `ResultSlot`.
/// Unknown / unparseable sources land as `None` (caller logs and
/// drops them — SRD-40b §5.1 calls for "log a warning and skip"
/// over a hard failure here, since the value mechanism is
/// supposed to be best-effort per cycle).
fn decode_slot(
    name: &str,
    spec: &nbrs_workload::model::ResultWireSpec,
) -> Option<ResultSlot> {
    let raw = spec.source().trim();
    let default = match spec {
        nbrs_workload::model::ResultWireSpec::Object { default: Some(d), .. } => {
            Some(nbrs_variates::node::Value::Str(d.clone()))
        }
        _ => None,
    };
    let source = if raw == "count" {
        ResultSource::Count
    } else if raw == "ok" {
        ResultSource::Ok
    } else if raw.contains('(') {
        // GK-call form — DEFERRED. Phase E (or a follow-up) will
        // wire this up to a per-cycle GK eval against an extended
        // scope that exposes the result body. For now we keep the
        // declaration parseable so workloads don't break, and log
        // a single warning at init time when the slot is decoded.
        crate::diag!(
            crate::observer::LogLevel::Warn,
            "result wire '{name}': gk-call source '{raw}' is not yet \
             supported — slot will resolve to its default (or skip).",
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
    Some(ResultSlot { wire: name.to_string(), source, default })
}

impl ResultDispenser {
    /// Wrap an inner dispenser with result-as-GK exposure for the
    /// op template's `result:` declarations. Returns the inner
    /// dispenser unchanged when `result:` is empty (no overhead
    /// for ops that don't declare wires).
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        result_decl: &HashMap<String, nbrs_workload::model::ResultWireSpec>,
    ) -> Arc<dyn OpDispenser> {
        if result_decl.is_empty() {
            return inner;
        }
        let mut specs = Vec::with_capacity(result_decl.len());
        // Stable iteration order so wire-resolution warnings
        // (and the per-cycle insertion order) don't depend on
        // HashMap rehashing.
        let mut entries: Vec<_> = result_decl.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (name, spec) in entries {
            if let Some(slot) = decode_slot(name, spec) {
                specs.push(slot);
            }
        }
        Arc::new(Self { inner, specs })
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
                match Self::evaluate(slot, &result) {
                    Some(v) => {
                        result.captures.insert(slot.wire.clone(), v);
                    }
                    None => {
                        crate::diag!(
                            crate::observer::LogLevel::Debug,
                            "result wire '{}' resolved to nothing on cycle {cycle} \
                             (no default declared); skipping.",
                            slot.wire,
                        );
                    }
                }
            }
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
/// expression to evaluate.
struct MetricSlot {
    /// Family name registered with the [`Component`]. Used in
    /// diagnostic messages (e.g. the counter non-positive warning).
    family: String,
    /// The GK expression to evaluate per cycle. Currently treated
    /// as a bare binding name — looked up in `OpResult.captures`.
    /// SRD-13d Phase 9 will route this through a real GK eval.
    value_expr: String,
    /// Optional value sanitiser. Applied after expression eval,
    /// before the instrument record.
    format: Option<nbrs_workload::metric_format::FormatSpec>,
    /// Resolved instrument storage — exactly one variant is
    /// populated per slot, matching `MetricSpec.kind`.
    instrument: MetricInstrument,
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
            });
        }

        Ok(Arc::new(Self { inner, slots }))
    }

    /// Read the bare-binding-name value out of the captures map.
    /// Returns `None` if the name is missing — caller logs at
    /// debug and skips the slot. Non-bare-name expressions
    /// (anything containing operators / parens / whitespace) get
    /// the same `None` treatment until Phase 9 lands.
    ///
    /// Captures map values can be any `Value` variant; we coerce
    /// numeric variants (U64, F64, Bool) to `f64`. Non-numeric
    /// variants (Str, vectors) yield `None` so the slot logs +
    /// skips rather than panicking through `Value::as_f64`'s
    /// strict matcher.
    fn lookup_bare_name(
        captures: &HashMap<String, nbrs_variates::node::Value>,
        expr: &str,
    ) -> Option<f64> {
        let trimmed = expr.trim();
        // Treat anything that isn't a single ident-shaped token
        // as a deferred GK expression — log once per cycle and
        // skip. This is the SRD-13d Phase 9 path.
        let bare = !trimmed.is_empty()
            && trimmed.chars().all(|c| c.is_alphanumeric() || c == '_');
        if !bare {
            return None;
        }
        captures.get(trimmed).and_then(value_to_f64)
    }
}

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
                let raw = match Self::lookup_bare_name(&result.captures, &slot.value_expr) {
                    Some(v) => v,
                    None => {
                        // Two reasons we could land here:
                        //  * bare name not yet present in captures
                        //    (likely a workload bug — log at debug)
                        //  * expression isn't a bare name (deferred
                        //    to SRD-13d Phase 9 — log at debug too)
                        crate::diag!(
                            crate::observer::LogLevel::Debug,
                            "metric '{}' value expr '{}' did not resolve on cycle {cycle}; \
                             skipping (non-bare-name exprs are deferred to Phase 9)",
                            slot.family, slot.value_expr,
                        );
                        continue;
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
    use nbrs_workload::model::ResultWireSpec;

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

    #[test]
    fn result_dispenser_count_and_path() {
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody {
                value: serde_json::json!({"rows": [{"value": 42}]}),
                count: 1,
            }),
            error: None,
        });
        let mut decl: HashMap<String, ResultWireSpec> = HashMap::new();
        decl.insert("row_count".into(), ResultWireSpec::String("count".into()));
        decl.insert("first_value".into(), ResultWireSpec::String("rows[0].value".into()));

        let wrapped = ResultDispenser::wrap(inner, &decl);
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
        let mut decl = HashMap::new();
        decl.insert("succeeded".into(), ResultWireSpec::String("ok".into()));

        let wrapped = ResultDispenser::wrap(inner, &decl);
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
        let mut decl = HashMap::new();
        decl.insert("succeeded".into(), ResultWireSpec::String("ok".into()));

        let wrapped = ResultDispenser::wrap(inner, &decl);
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let err = rt.block_on(wrapped.execute(0, &ctx)).unwrap_err();
        assert!(format!("{err}").contains("boom"));
    }

    #[test]
    fn result_dispenser_unresolved_path_uses_default() {
        // An unresolvable path that has a `default:` falls back to
        // the default value; one with no default is silently
        // skipped (warning logged, not asserted here).
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody {
                value: serde_json::json!({"rows": []}),
                count: 0,
            }),
            error: None,
        });
        let mut decl = HashMap::new();
        decl.insert(
            "missing_with_default".into(),
            ResultWireSpec::Object {
                source: "rows[0].value".into(),
                default: Some("none".into()),
            },
        );
        decl.insert(
            "missing_no_default".into(),
            ResultWireSpec::String("rows[0].value".into()),
        );

        let wrapped = ResultDispenser::wrap(inner, &decl);
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let result = rt.block_on(wrapped.execute(0, &ctx)).unwrap();

        match &result.captures["missing_with_default"] {
            nbrs_variates::node::Value::Str(s) => assert_eq!(s, "none"),
            other => panic!("expected default Str, got {other:?}"),
        }
        assert!(!result.captures.contains_key("missing_no_default"));
    }

    #[test]
    fn result_dispenser_empty_decl_returns_inner_unchanged() {
        let inner: Arc<dyn OpDispenser> = Arc::new(FakeInner {
            body: Some(ResultDispBody { value: serde_json::json!({}), count: 0 }),
            error: None,
        });
        let inner_ptr = Arc::as_ptr(&inner);
        let wrapped = ResultDispenser::wrap(inner.clone(), &HashMap::new());
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
        let mut decl = HashMap::new();
        decl.insert("c".into(), ResultWireSpec::String("count".into()));
        let wrapped = ResultDispenser::wrap(Arc::new(SkipInner), &decl);
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
        let wrapped = MetricsDispenser::wrap(inner.clone(), &HashMap::new(), &mut comp).unwrap();
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

    /// Test helper: build a typed `Arc<MetricsDispenser>` so
    /// tests can both `execute` (via dyn coercion) and read the
    /// stashed instruments via `slot_*`. Mirrors the public
    /// `MetricsDispenser::wrap` body for the non-empty-decls case.
    fn typed_wrap(
        inner: Arc<dyn OpDispenser>,
        decls: &HashMap<String, MetricSpec>,
    ) -> Result<Arc<MetricsDispenser>, String> {
        let mut comp = fresh_component();
        if decls.is_empty() {
            // The public `wrap` short-circuits to the inner — no
            // typed handle exists in that case. Tests should not
            // call this for the empty case.
            return Err("typed_wrap requires non-empty decls".into());
        }
        // Replicate `MetricsDispenser::wrap` body here so the
        // returned handle is typed. Mirror the public function
        // exactly so the test path validates the same behaviour.
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
                family.clone(),
                spec.unit.clone(),
                instrument.as_ref(),
            )?;
            slots.push(MetricSlot {
                family,
                value_expr: spec.value.clone(),
                format,
                instrument,
            });
        }
        Ok(Arc::new(MetricsDispenser { inner, slots }))
    }

    #[test]
    fn metrics_dispenser_gauge_records_f64() {
        let mut captures = HashMap::new();
        captures.insert("my_factor".into(), nbrs_variates::node::Value::F64(3.14));
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures });

        let mut decl = HashMap::new();
        decl.insert("my_factor".into(), make_spec("my_factor", MetricKind::Gauge, None));

        let typed = typed_wrap(inner, &decl).unwrap();
        let dyn_disp: Arc<dyn OpDispenser> = typed.clone();
        let gauge = typed.slot_gauge("my_factor").unwrap();

        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dyn_disp.execute(0, &ctx)).unwrap();

        assert!((gauge.get() - 3.14).abs() < 1e-9);
    }

    #[test]
    fn metrics_dispenser_histogram_truncates_to_u64() {
        let mut captures = HashMap::new();
        captures.insert("latency_ms".into(), nbrs_variates::node::Value::F64(7.9));
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures });

        let mut decl = HashMap::new();
        decl.insert("latency_ms".into(), make_spec("latency_ms", MetricKind::Histogram, None));

        let typed = typed_wrap(inner, &decl).unwrap();
        let dyn_disp: Arc<dyn OpDispenser> = typed.clone();
        let hist = typed.slot_histogram("latency_ms").unwrap();

        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dyn_disp.execute(0, &ctx)).unwrap();

        // Truncated 7.9 -> 7. Histogram snapshot's max recorded.
        let snap = hist.peek_snapshot();
        assert_eq!(snap.max(), 7);
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn metrics_dispenser_counter_positive_inc_and_skip_non_positive() {
        // Two counters: one positive, one non-positive (zero).
        let mut captures = HashMap::new();
        captures.insert("ok_inc".into(), nbrs_variates::node::Value::U64(5));
        captures.insert("skip_inc".into(), nbrs_variates::node::Value::F64(0.0));
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures });

        let mut decl = HashMap::new();
        decl.insert("ok_inc".into(), make_spec("ok_inc", MetricKind::Counter, None));
        decl.insert("skip_inc".into(), make_spec("skip_inc", MetricKind::Counter, None));

        let typed = typed_wrap(inner, &decl).unwrap();
        let dyn_disp: Arc<dyn OpDispenser> = typed.clone();
        let ok_counter = typed.slot_counter("ok_inc").unwrap();
        let skip_counter = typed.slot_counter("skip_inc").unwrap();

        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dyn_disp.execute(0, &ctx)).unwrap();

        assert_eq!(ok_counter.get(), 5);
        // Non-positive value warns and skips — counter stays at 0.
        assert_eq!(skip_counter.get(), 0);
    }

    #[test]
    fn metrics_dispenser_format_rounds_value() {
        let mut captures = HashMap::new();
        captures.insert("ratio".into(), nbrs_variates::node::Value::F64(1.234));
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures });

        let mut decl = HashMap::new();
        decl.insert(
            "ratio".into(),
            make_spec("ratio", MetricKind::Gauge, Some("#.##")),
        );

        let typed = typed_wrap(inner, &decl).unwrap();
        let dyn_disp: Arc<dyn OpDispenser> = typed.clone();
        let gauge = typed.slot_gauge("ratio").unwrap();

        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dyn_disp.execute(0, &ctx)).unwrap();

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

        let err = match MetricsDispenser::wrap(inner, &decl, &mut comp) {
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

        let typed = typed_wrap(Arc::new(SkipInner), &decl).unwrap();
        let dyn_disp: Arc<dyn OpDispenser> = typed.clone();
        let gauge = typed.slot_gauge("g").unwrap();

        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let res = rt.block_on(dyn_disp.execute(0, &ctx)).unwrap();
        assert!(res.skipped);
        // Gauge default value untouched.
        assert_eq!(gauge.get(), 0.0);
    }

    #[test]
    fn metrics_dispenser_non_bare_expr_skips() {
        // Inline expression like "factor * 2.0" should be deferred
        // to Phase 9 — wrapper logs at debug and continues without
        // recording. The gauge stays untouched.
        let mut captures = HashMap::new();
        captures.insert("factor".into(), nbrs_variates::node::Value::F64(3.0));
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner { captures });

        let mut decl = HashMap::new();
        decl.insert(
            "computed".into(),
            make_spec("factor * 2.0", MetricKind::Gauge, None),
        );

        let typed = typed_wrap(inner, &decl).unwrap();
        let dyn_disp: Arc<dyn OpDispenser> = typed.clone();
        let gauge = typed.slot_gauge("computed").unwrap();

        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dyn_disp.execute(0, &ctx)).unwrap();

        // Expression is non-bare → skipped → gauge stays default.
        assert_eq!(gauge.get(), 0.0);
    }
}
