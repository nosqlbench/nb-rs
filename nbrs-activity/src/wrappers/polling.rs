// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polling / await wrapper. Re-executes the inner op until its
//! row count (optionally projected through a JSON-Pointer path)
//! falls into the configured `[min_rows, max_rows]` window, or
//! the timeout fires. Used for waiting on backend state to
//! settle: SAI index build, compactions, etc.

use std::sync::Arc;

use crate::adapter::{AdapterError, ExecutionError, OpDispenser, OpResult};
use crate::adapter::WrappingDispenser;
use crate::wrapper_registry::{WrapperName, WrapperRegistration};
use nbrs_workload::model::ParsedOp;

/// SRD-32a wrapper name.
pub const NAME: WrapperName = WrapperName::new("poll");

/// Trigger: `poll:` may be a bare string (mode only, defaults
/// for everything else) or a map carrying the full config —
/// either form turns the wrapper on.
fn triggers(template: &ParsedOp) -> bool {
    template
        .params
        .get("poll")
        .map(|v| v.is_string() || v.is_object())
        .unwrap_or(false)
}

fn describe_assignment(template: &ParsedOp) -> Option<String> {
    let poll_val = template.params.get("poll")?;
    let (mode, interval, timeout): (String, u64, u64) = match poll_val {
        v if v.is_string() => (
            v.as_str().unwrap().to_string(),
            1000,
            300_000,
        ),
        v if v.is_object() => {
            let m = v.as_object().unwrap();
            let mode = m
                .get("mode")
                .and_then(|x| x.as_str())
                .unwrap_or("await_empty")
                .to_string();
            let interval = m.get("interval_ms")
                .and_then(crate::wrapper_registrations::json_to_u64).unwrap_or(1000);
            let timeout = m.get("timeout_ms")
                .and_then(crate::wrapper_registrations::json_to_u64).unwrap_or(300_000);
            (mode, interval, timeout)
        }
        _ => return None,
    };
    Some(format!(
        "poll: every {}ms, timeout {}ms, on `{mode}`",
        interval, timeout
    ))
}

inventory::submit! {
    WrapperRegistration {
        name: NAME,
        owned_fields: &[
            // `poll:` is the single discriminant for the poll
            // wrapper; every knob (interval_ms, timeout_ms,
            // min_rows, max_rows, json_path, metric_name,
            // max_error_retries) lives under it as a map. The
            // flat `poll_*`-prefix surface was retired.
            "poll",
        ],
        triggers,
        requires_inner: &[super::traversing::NAME],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment,
    }
}

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
                    // Captures land on the per-fiber kernel directly
                    // via ctx.wires.write — wrappers above this layer
                    // see the values through wires.get on the same
                    // cycle. Slot-absent writes silently no-op
                    // (closure-binding economy).
                    let _ = ctx.wires.write(
                        "poll_count",
                        polydat::ast::Value::U64(polls),
                    );
                    let _ = ctx.wires.write(
                        "poll_elapsed_ms",
                        polydat::ast::Value::U64(elapsed.as_millis() as u64),
                    );
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
                        let _ = ctx.wires.write(
                            name,
                            polydat::ast::Value::F64(value),
                        );
                    }
                    return Ok(OpResult {
                        body: None,
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

/// Map a named metric's elapsed-time value to the numeric value the
/// metric name advertises. The metric name suffix selects the unit
/// (`_ns`, `_us`, `_ms`, `_s`, `_m`, `_h`); the elapsed seconds are
/// scaled accordingly so a metric called `index_build_time_ms`
/// reads in milliseconds.
///
/// Names without a recognised suffix fall through as seconds
/// (preserves the historical contract — e.g. `index_build_time`
/// used to be emitted raw in seconds and still is).
///
/// Longest suffixes are tested first so `_ms` doesn't tail-bind
/// to a more-permissive `_s` rule by accident.
pub(crate) fn duration_value_for_metric_name(name: &str, elapsed_secs: f64) -> f64 {
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
pub(crate) fn count_from_json_pointer(json: &serde_json::Value, path: &str) -> u64 {
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
