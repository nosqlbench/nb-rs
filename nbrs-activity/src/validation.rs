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

use nbrs_metrics::labels::Labels;

use crate::adapter::{
    ExecutionError, OpDispenser, OpResult,
};
use crate::relevancy::{self, RelevancyFn};

// =========================================================================
// Validation configuration (parsed from workload YAML at init time)
// =========================================================================

/// Closed vocabulary of op-template `params:` keys the runtime
/// itself consumes (validation, batching, polling, op weighting,
/// adapter selection). Joined at validation time with
/// [`crate::runner::KNOWN_PARAMS`] (workload/CLI-level keys that
/// get blast-merged into every op's params during parse) and the
/// adapter's
/// [`crate::adapter::DriverAdapter::known_op_params`] declarations
/// to form the full allow-list. Unknown keys are rejected at op
/// setup time so silent-ignore traps like `evaluations: { relevancy: ... }`
/// (a wrapper key the runtime never reads) cannot hide a
/// misconfigured op.
pub const CORE_OP_PARAMS: &[&str] = &[
    // Validation / scoring
    "relevancy", "verify", "strict",
    // Batching
    "batch", "batchtype", "max_batch_size",
    // Polling
    "poll", "poll_interval_ms", "poll_metric_name", "poll_max_error_retries", "timeout_ms",
    // Op weighting / dry-run
    "ratio", "emit",
    // Adapter selection
    "adapter", "driver",
];

/// Configuration for relevancy measurement on a single op template.
#[derive(Debug, Clone)]
pub struct RelevancyConfig {
    /// Column/field name to extract actual result indices from.
    pub actual_field: String,
    /// GK binding name that produces ground truth indices.
    pub expected_binding: String,
    /// Recall window — number of top results used in the @k
    /// metric computation. The first `k` of `actual` are
    /// compared against the first `k` of `expected`.
    pub k: usize,
    /// Optional retrieval window. When set, declares "we
    /// retrieved exactly `r` results, but score using only the
    /// first `k` of them" (the "k-recall@r" semantic). Runtime
    /// asserts `actual.len() == r`. When `None`, no separate
    /// retrieval window is enforced — actual is truncated to
    /// `k` for the comparison and any length is accepted (the
    /// pre-`r` behavior).
    pub r: Option<usize>,
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
    /// Body-level: result must contain at least N rows
    /// (`element_count() >= N`). Use to make a SELECT
    /// failsafe — empty result sets surface as a hard error
    /// instead of silently passing per-row assertions vacuously.
    /// Field name is ignored for this predicate.
    MinRows(u64),
}

impl AssertionSpec {
    /// Check this assertion against a result.
    pub fn check(&self, result: &OpResult) -> bool {
        // Body-level predicates are evaluated against the result's
        // shape rather than a per-row field value. Handled before
        // field extraction.
        if let AssertionPredicate::MinRows(n) = &self.predicate {
            let row_count = result.body.as_ref()
                .map(|b| b.element_count())
                .unwrap_or(0);
            return row_count >= *n;
        }

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
            AssertionPredicate::MinRows(_) => unreachable!(
                "MinRows handled in early-return above"
            ),
        }
    }
}

// =========================================================================
// ValidationMetrics
// =========================================================================

/// Running aggregate per relevancy metric. Holds both the all-time
/// running mean and a bounded sliding window so progress views can
/// show recent trend alongside the cumulative average.
pub struct RunningAgg {
    /// Sum of every score ever recorded (for all-time mean).
    pub total_sum: f64,
    /// Count of scores recorded (for all-time mean).
    pub total_count: u64,
    /// Recent scores, newest at the back. Capped at `window_size`.
    pub window: std::collections::VecDeque<f64>,
    /// Upper bound on `window` length.
    pub window_size: usize,
}

impl RunningAgg {
    pub fn new(window_size: usize) -> Self {
        Self {
            total_sum: 0.0,
            total_count: 0,
            window: std::collections::VecDeque::with_capacity(window_size),
            window_size,
        }
    }

    pub fn record(&mut self, score: f64) {
        self.total_sum += score;
        self.total_count += 1;
        if self.window.len() == self.window_size {
            self.window.pop_front();
        }
        self.window.push_back(score);
    }

    pub fn window_mean(&self) -> f64 {
        if self.window.is_empty() {
            0.0
        } else {
            self.window.iter().sum::<f64>() / self.window.len() as f64
        }
    }

    pub fn total_mean(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            self.total_sum / self.total_count as f64
        }
    }
}

/// Default size of the moving-average window. Chosen to match the
/// common recall@10 "last 10" semantic the user's workloads expect.
pub const DEFAULT_RECALL_WINDOW: usize = 10;

/// Live snapshot of a relevancy metric: window mean, all-time mean,
/// how many scores have been recorded, and the current window size.
#[derive(Debug, Clone)]
pub struct RelevancyLive {
    pub name: String,
    pub window_mean: f64,
    pub total_mean: f64,
    pub total_count: u64,
    pub window_len: usize,
}

/// Metrics for result validation, shared across fibers.
pub struct ValidationMetrics {
    pub validations_passed: AtomicU64,
    pub validations_failed: AtomicU64,
    /// Per-function lossless f64 statistics accumulators for relevancy scores.
    /// Exact precision — no quantization, no bucket rounding.
    pub relevancy_stats: HashMap<String, nbrs_metrics::summaries::f64stats::F64Stats>,
    /// Live aggregates (moving-window + all-time) per relevancy metric.
    /// These read non-destructively, so progress views can sample them
    /// every frame without disturbing the exact-precision stats above.
    pub running_aggregates: HashMap<String, std::sync::Mutex<RunningAgg>>,
}

impl ValidationMetrics {
    /// Create metrics for the given relevancy functions and k value.
    pub fn new(labels: &Labels, functions: &[RelevancyFn], k: usize) -> Self {
        let mut stats = HashMap::new();
        let mut running = HashMap::new();
        for func in functions {
            let metric_name = format!("{}_at_{}", func.metric_name(), k);
            stats.insert(
                metric_name.clone(),
                nbrs_metrics::summaries::f64stats::F64Stats::new(labels.with("name", &metric_name)),
            );
            running.insert(
                metric_name.clone(),
                std::sync::Mutex::new(RunningAgg::new(DEFAULT_RECALL_WINDOW)),
            );
        }
        Self {
            validations_passed: AtomicU64::new(0),
            validations_failed: AtomicU64::new(0),
            relevancy_stats: stats,
            running_aggregates: running,
        }
    }

    /// Create metrics with no relevancy functions (assertions only).
    pub fn assertions_only() -> Self {
        Self {
            validations_passed: AtomicU64::new(0),
            validations_failed: AtomicU64::new(0),
            relevancy_stats: HashMap::new(),
            running_aggregates: HashMap::new(),
        }
    }

    /// Record a relevancy score for a named function. Updates both the
    /// lossless stats accumulator and the live running aggregate.
    pub fn record_relevancy(&self, metric_name: &str, score: f64) {
        if let Some(stats) = self.relevancy_stats.get(metric_name) {
            stats.record(score);
        }
        if let Some(agg) = self.running_aggregates.get(metric_name) {
            let mut a = agg.lock().unwrap_or_else(|e| e.into_inner());
            a.record(score);
        }
    }

    /// Snapshot all live relevancy aggregates without disturbing the
    /// accumulators. Safe to call every frame from the progress thread.
    pub fn live_snapshot(&self) -> Vec<RelevancyLive> {
        let mut out = Vec::with_capacity(self.running_aggregates.len());
        for (name, agg) in &self.running_aggregates {
            let a = agg.lock().unwrap_or_else(|e| e.into_inner());
            out.push(RelevancyLive {
                name: name.clone(),
                window_mean: a.window_mean(),
                total_mean: a.total_mean(),
                total_count: a.total_count,
                window_len: a.window.len(),
            });
        }
        out.sort_by(|x, y| x.name.cmp(&y.name));
        out
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
    /// Memoized handle for the relevancy `expected` binding (the
    /// ground-truth GK name). Registered into the per-template
    /// `ScopeFixture` by [`Self::wrap`]. `None` only when no
    /// `relevancy:` block is declared (assertion-only validation).
    /// SRD 33 §"Ground Truth Flow".
    expected_handle: Option<crate::fixture::PullHandle>,
    metrics: Arc<ValidationMetrics>,
    /// If true, assertion failures become ExecutionError::Op.
    strict: bool,
}

impl ValidatingDispenser {
    /// Wrap a dispenser with validation, registering the relevancy
    /// `expected` binding into the supplied scope fixture.
    ///
    /// Returns the inner dispenser unchanged if neither `verify:`
    /// nor `relevancy:` are declared.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        template: &nbrs_workload::model::ParsedOp,
        labels: &Labels,
        program: Option<&nbrs_variates::kernel::GkProgram>,
        fx: &mut crate::fixture::ScopeFixture,
    ) -> Result<(Arc<dyn OpDispenser>, Option<Arc<ValidationMetrics>>), String> {
        let assertions = parse_assertions(template);
        let relevancy = parse_relevancy(template, program)?;
        let strict = template.params.get("strict")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if assertions.is_empty() && relevancy.is_none() {
            return Ok((inner, None));
        }

        // Register the relevancy ground-truth binding with the
        // fixture so the wrapper can read it via PullHandle at
        // cycle time. Strip the `{...}` braces from the binding
        // reference; the fixture registers bare names.
        let expected_handle = match &relevancy {
            Some(cfg) => {
                let name = cfg.expected_binding
                    .trim_matches(|c| c == '{' || c == '}');
                Some(fx.register_pull(name).map_err(|e| format!(
                    "op '{op}' relevancy.expected: {e}",
                    op = template.name,
                ))?)
            }
            None => None,
        };

        let metrics = Arc::new(match &relevancy {
            Some(config) => ValidationMetrics::new(labels, &config.functions, config.k),
            None => ValidationMetrics::assertions_only(),
        });

        let wrapper = Arc::new(Self {
            inner,
            assertions,
            relevancy,
            expected_handle,
            metrics: metrics.clone(),
            strict,
        });
        Ok((wrapper, Some(metrics)))
    }
}

impl OpDispenser for ValidatingDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let fields = ctx.fields;
            let result = self.inner.execute(cycle, ctx).await?;

            // Phase 1: Field assertions. Track failed predicates
            // so the strict-mode error can say which check failed
            // (especially helpful for body-level checks like
            // `min_rows: 1` where the user gets clear feedback that
            // the result was empty rather than a generic "validation
            // failed" message).
            let mut failed_assertions: Vec<String> = Vec::new();
            for assertion in &self.assertions {
                if !assertion.check(&result) {
                    failed_assertions.push(describe_assertion_failure(assertion, &result));
                }
            }
            let all_pass = failed_assertions.is_empty();

            // Phase 2: Relevancy metrics
            if let Some(config) = &self.relevancy {
                let actual_ordered = extract_actual_indices(&result, &config.actual_field);
                // Single read path: ground truth comes from the
                // PullHandle registered into the ScopeFixture at
                // init (SRD 33 §"Ground Truth Flow"). When
                // relevancy is configured, the handle is
                // *required* — wrap registers it unconditionally,
                // and a missing handle here is a programming error.
                let h = self.expected_handle.expect(
                    "ValidatingDispenser invariant violated: relevancy is \
                     configured but expected_handle was not registered. \
                     Construct via ValidatingDispenser::wrap.",
                );
                let expected_raw = resolve_expected_from_value(ctx.pulls.get(h));

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
                        let indent = crate::scene_tree::running_phase_indent();
                        crate::observer::log(
                            crate::observer::LogLevel::Warn,
                            &format!("{indent}relevancy: no values extracted for field '{}' from result", config.actual_field),
                        );
                        if let Some(body) = &result.body {
                            let preview = serde_json::to_string(&body.to_json()).unwrap_or_default();
                            crate::observer::log(
                                crate::observer::LogLevel::Warn,
                                &format!("{indent}  result preview: {}", &preview[..preview.len().min(300)]),
                            );
                        }
                    }
                }

                // k-recall@r contract: when the workload declares
                // `r:` in the relevancy block, it has asserted
                // that the database query was sized to retrieve
                // exactly that many results. Mismatch is a
                // correctness signal — the recall measurement is
                // only meaningful if the retrieval window matches
                // what the operator asked for. Fail the op rather
                // than silently producing a recall figure for an
                // off-by-one retrieval window.
                if let Some(r) = config.r {
                    if actual_ordered.len() != r {
                        return Err(ExecutionError::Op(crate::adapter::AdapterError {
                            error_name: "relevancy_error".into(),
                            message: format!(
                                "relevancy: expected exactly r={r} results from \
                                 retrieval, got {} (k-recall@r contract). Either \
                                 size the query LIMIT to {r}, or remove the `r:` \
                                 declaration to fall back to first-k semantics.",
                                actual_ordered.len(),
                            ),
                            retryable: false,
                        }));
                    }
                }

                // k-recall@r semantics: the recall metric counts
                // how many of the *top-k* ground-truth items appear
                // anywhere in the *top-r* returned, divided by k.
                // Truncating `actual` to `k` (the pre-`r:` shape)
                // computes top-k ∩ top-k instead, which collapses
                // to ~K/R when the server post-filters and the
                // returned top-K rarely contains true neighbours —
                // that's the "stuck at 0.1" symptom for r=10×k.
                // When `r` is unset, fall back to k (the legacy
                // "first-k" behaviour).
                let r_window = config.r.unwrap_or(config.k);
                let expected_sorted = relevancy::truncate_and_sort(&expected_raw, config.k);
                let actual_sorted = relevancy::truncate_and_sort(&actual_ordered, r_window);


                for func in &config.functions {
                    let score = func.compute(
                        &expected_sorted,
                        &actual_sorted,
                        &actual_ordered,
                        config.k,
                    );
                    let metric_name = format!("{}_at_{}", func.metric_name(), config.k);
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
                        message: format!(
                            "result validation failed (strict mode): {}",
                            failed_assertions.join("; "),
                        ),
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

/// Render a one-line description of an assertion that failed
/// against `result`. Used by the strict-mode error message so
/// the workload author sees exactly which predicate fired.
fn describe_assertion_failure(assertion: &AssertionSpec, result: &OpResult) -> String {
    match &assertion.predicate {
        AssertionPredicate::MinRows(n) => {
            let got = result.body.as_ref()
                .map(|b| b.element_count())
                .unwrap_or(0);
            format!("min_rows: expected ≥{n}, got {got}")
        }
        AssertionPredicate::Eq(expected) =>
            format!("field '{}' eq '{}' failed", assertion.field, expected),
        AssertionPredicate::NotNull =>
            format!("field '{}' must not be null", assertion.field),
        AssertionPredicate::IsNull =>
            format!("field '{}' must be null", assertion.field),
        AssertionPredicate::Gte(t) =>
            format!("field '{}' >= {t} failed", assertion.field),
        AssertionPredicate::Lte(t) =>
            format!("field '{}' <= {t} failed", assertion.field),
        AssertionPredicate::Contains(sub) =>
            format!("field '{}' contains '{}' failed", assertion.field, sub),
    }
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
///   - min_rows: 1     # body-level: result must have ≥1 row
/// ```
///
/// Most predicates target a named field within each result row
/// and require `field:`. The body-level `min_rows:` predicate is
/// the exception — it asserts on the result's row count rather
/// than any specific field, so it has no `field:` key.
fn parse_assertions(template: &nbrs_workload::model::ParsedOp) -> Vec<AssertionSpec> {
    let Some(verify) = template.params.get("verify") else {
        return Vec::new();
    };

    let Some(items) = verify.as_array() else {
        return Vec::new();
    };

    let mut assertions = Vec::new();
    for item in items {
        let Some(obj) = item.as_object() else { continue };

        // Body-level predicates first: `min_rows: N` doesn't take
        // a `field:` because it asserts on the body's row count.
        if let Some(v) = obj.get("min_rows") {
            let n = v.as_u64().unwrap_or(0);
            assertions.push(AssertionSpec {
                field: String::new(),  // ignored for MinRows
                predicate: AssertionPredicate::MinRows(n),
            });
            continue;
        }

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

/// Allowed sub-keys under `relevancy:`. Anything outside this
/// vocabulary is a hard error — silent acceptance of typos
/// (e.g. `relevency:`) is exactly the failure mode SRD-15 §
/// "Strict mode" rules out.
const RELEVANCY_VOCAB: &[&str] = &[
    "actual", "expected", "k", "r", "functions",
];

/// Parse `relevancy:` block from a ParsedOp's params.
///
/// **Strict.** Required fields (`actual`, `expected`) missing →
/// error. Unknown sub-keys → error. Non-numeric `k`/`r` → error.
/// Surviving `{name}` placeholders → error (they should already
/// have been resolved by the SRD-16 single read path before this
/// function runs).
///
/// Expected YAML structure:
/// ```yaml
/// relevancy:
///   actual: key
///   expected: "{ground_truth}"
///   k: 10                       # or "{k}" — resolved beforehand
///   r: 64                       # optional
///   functions: [recall, …]
/// ```
fn parse_relevancy(
    template: &nbrs_workload::model::ParsedOp,
    _program: Option<&nbrs_variates::kernel::GkProgram>,
) -> Result<Option<RelevancyConfig>, String> {
    let Some(rel) = template.params.get("relevancy") else { return Ok(None); };
    let obj = rel.as_object().ok_or_else(|| format!(
        "op '{}': relevancy: expected a mapping, got {kind}",
        template.name,
        kind = match rel {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            _ => "unknown",
        },
    ))?;

    // Closed-vocabulary check.
    for k in obj.keys() {
        if !RELEVANCY_VOCAB.contains(&k.as_str()) {
            return Err(format!(
                "op '{op}' relevancy: unknown key '{k}'. Allowed: [{vocab}]",
                op = template.name,
                vocab = RELEVANCY_VOCAB.join(", "),
            ));
        }
    }

    let actual_field = obj.get("actual")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!(
            "op '{}' relevancy: missing required field 'actual' (string column name)",
            template.name,
        ))?
        .to_string();
    let expected_binding = obj.get("expected")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!(
            "op '{}' relevancy: missing required field 'expected' (binding reference)",
            template.name,
        ))?
        .to_string();

    let k_label = format!("op '{}' relevancy.k", template.name);
    let k = parse_count_param(obj.get("k"), &k_label)?
        .ok_or_else(|| format!(
            "op '{}' relevancy: missing required field 'k' (integer)",
            template.name,
        ))? as usize;

    let r_label = format!("op '{}' relevancy.r", template.name);
    let r: Option<usize> = parse_count_param(obj.get("r"), &r_label)?
        .map(|n| n as usize);

    if let Some(rv) = r {
        if rv < k {
            return Err(format!(
                "op '{op}' relevancy: r={rv} is smaller than k={k}; \
                 the k-recall@r contract requires r >= k",
                op = template.name,
            ));
        }
    }

    let functions: Vec<RelevancyFn> = match obj.get("functions") {
        None => vec![RelevancyFn::Recall],
        Some(serde_json::Value::Array(arr)) => {
            let mut out: Vec<RelevancyFn> = Vec::new();
            for (i, v) in arr.iter().enumerate() {
                let name = v.as_str().ok_or_else(|| format!(
                    "op '{op}' relevancy.functions[{i}]: expected a string, got {kind}",
                    op = template.name,
                    kind = match v {
                        serde_json::Value::Null => "null",
                        serde_json::Value::Bool(_) => "boolean",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                        _ => "unknown",
                    },
                ))?;
                let func = RelevancyFn::parse(name).ok_or_else(|| format!(
                    "op '{op}' relevancy.functions[{i}]: unknown function '{name}'",
                    op = template.name,
                ))?;
                out.push(func);
            }
            if out.is_empty() {
                return Err(format!(
                    "op '{op}' relevancy.functions: empty list — declare at least one \
                     function or remove the field to default to [recall]",
                    op = template.name,
                ));
            }
            out
        }
        Some(_) => return Err(format!(
            "op '{op}' relevancy.functions: expected an array of strings",
            op = template.name,
        )),
    };

    Ok(Some(RelevancyConfig {
        actual_field,
        expected_binding,
        k,
        r,
        functions,
    }))
}

/// Parse a YAML count parameter (`k:` or `r:`) — strict.
/// Accepts only a JSON integer or a literal numeric string.
/// `{name}` placeholders are *not* resolved here: the SRD-16
/// single read path means every placeholder must already have
/// been resolved by [`crate::scope::resolve_placeholders_via_kernel`]
/// before this function runs. Surviving `{…}` shapes are a
/// workload bug and surface as `Err`. The caller decides
/// whether the parameter is required or optional — `None` here
/// means "absent," not "unresolvable."
fn parse_count_param(
    val: Option<&serde_json::Value>,
    field_label: &str,
) -> Result<Option<u64>, String> {
    let Some(v) = val else { return Ok(None); };
    if let Some(n) = v.as_u64() { return Ok(Some(n)); }
    let s = match v.as_str() {
        Some(s) => s,
        None => return Err(format!(
            "{field_label}: expected an integer or numeric string, got {kind}",
            kind = match v {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
                _ => "unsupported value",
            },
        )),
    };
    let trimmed = s.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return Err(format!(
            "{field_label}: '{trimmed}' was not resolved before parameter parsing — \
             this is a placeholder-resolution bug, not a config-time issue. The \
             single-read-path resolver should have substituted it from the kernel."
        ));
    }
    match trimmed.parse::<u64>() {
        Ok(n) => Ok(Some(n)),
        Err(_) => Err(format!(
            "{field_label}: '{trimmed}' is not a valid non-negative integer"
        )),
    }
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

/// Extract integer ground-truth indices from a typed `Value`.
///
/// Single read path: at cycle time, [`ValidatingDispenser::execute`]
/// pulls the value via the `expected_handle` registered with the
/// scope fixture at init (SRD 33 §"Ground Truth Flow"), then
/// hands it here for type-aware extraction.
///
/// Fast path for the typed-array vector data path (SRD 53
/// §"Native vector PortType"): when the binding produces
/// `Value::VecI32` / `Value::VecF32` (the shape
/// `neighbor_indices_at` and friends emit), the slice is read
/// directly with no string round-trip. String fallback covers
/// legacy bindings that emit `Value::Str("[1, 5, 12, ...]")` or
/// `Value::Str("1,5,12,...")`.
fn resolve_expected_from_value(value: &nbrs_variates::node::Value) -> Vec<i64> {
    match value {
        // Typed-slice fast paths — zero-copy read, no parse.
        nbrs_variates::node::Value::VecI32(slice) => {
            slice.as_slice().iter().map(|&x| x as i64).collect()
        }
        nbrs_variates::node::Value::VecF32(slice) => {
            // Vector ground-truth indices are integer-valued
            // by domain. Truncating fractional parts is the
            // correct semantic; anything else would mean the
            // dataset's index column is mis-typed at the source.
            slice.as_slice().iter().map(|&x| x as i64).collect()
        }
        nbrs_variates::node::Value::Str(s) => parse_int_array(s),
        nbrs_variates::node::Value::U64(v) => vec![*v as i64],
        _ => {
            // Display-string fallback for anything else.
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

    /// Body whose `element_count()` reflects the number of items
    /// in a JSON array — exercises body-level predicates like
    /// `MinRows` that read from `element_count` rather than
    /// inspecting fields.
    #[derive(Debug)]
    struct CountedBody {
        rows: Vec<serde_json::Value>,
    }
    impl ResultBody for CountedBody {
        fn to_json(&self) -> serde_json::Value {
            serde_json::Value::Array(self.rows.clone())
        }
        fn as_any(&self) -> &dyn Any { self }
        fn element_count(&self) -> u64 { self.rows.len() as u64 }
    }

    #[test]
    fn assertion_min_rows_passes_when_threshold_met() {
        let result = OpResult {
            body: Some(Box::new(CountedBody {
                rows: vec![
                    serde_json::json!({"index_name": "vec_idx"}),
                    serde_json::json!({"index_name": "meta_idx"}),
                ],
            })),
            captures: HashMap::new(),
            skipped: false,
        };
        let spec = AssertionSpec {
            field: String::new(),
            predicate: AssertionPredicate::MinRows(1),
        };
        assert!(spec.check(&result));

        let spec_two = AssertionSpec {
            field: String::new(),
            predicate: AssertionPredicate::MinRows(2),
        };
        assert!(spec_two.check(&result));
    }

    #[test]
    fn assertion_min_rows_fails_when_below_threshold() {
        // Empty array: element_count = 0, MinRows(1) must fail.
        let result = OpResult {
            body: Some(Box::new(CountedBody { rows: Vec::new() })),
            captures: HashMap::new(),
            skipped: false,
        };
        let spec = AssertionSpec {
            field: String::new(),
            predicate: AssertionPredicate::MinRows(1),
        };
        assert!(!spec.check(&result));

        // No body at all: element_count defaults to 0 → MinRows(1) fails.
        let result_none = OpResult {
            body: None,
            captures: HashMap::new(),
            skipped: false,
        };
        assert!(!spec.check(&result_none));
    }

    #[test]
    fn parse_assertions_min_rows_from_yaml() {
        // SRD-40b-shaped failsafe verify form:
        //   verify:
        //     - min_rows: 1
        let mut template = nbrs_workload::model::ParsedOp::simple("await", "test");
        template.params.insert("verify".into(), serde_json::json!([
            {"min_rows": 1},
        ]));
        let assertions = parse_assertions(&template);
        assert_eq!(assertions.len(), 1);
        match &assertions[0].predicate {
            AssertionPredicate::MinRows(n) => assert_eq!(*n, 1),
            other => panic!("expected MinRows(1), got {other:?}"),
        }
    }

    #[test]
    fn min_rows_failure_describes_actual_vs_expected() {
        // Strict-mode error renderer should name the predicate
        // and the actual count so the user sees "expected ≥1, got 0"
        // rather than a generic "validation failed".
        let result = OpResult {
            body: Some(Box::new(CountedBody { rows: Vec::new() })),
            captures: HashMap::new(),
            skipped: false,
        };
        let spec = AssertionSpec {
            field: String::new(),
            predicate: AssertionPredicate::MinRows(1),
        };
        let msg = describe_assertion_failure(&spec, &result);
        assert!(msg.contains("min_rows"), "got: {msg}");
        assert!(msg.contains("≥1"), "got: {msg}");
        assert!(msg.contains("got 0"), "got: {msg}");
    }

    #[test]
    fn validation_metrics_record_relevancy() {
        let labels = Labels::of("activity", "test");
        let metrics = ValidationMetrics::new(
            &labels,
            &[RelevancyFn::Recall, RelevancyFn::Precision],
            10,
        );
        assert!(metrics.relevancy_stats.contains_key("recall_at_10"));
        assert!(metrics.relevancy_stats.contains_key("precision_at_10"));
        assert!(!metrics.relevancy_stats.contains_key("f1_at_10"));

        metrics.record_relevancy("recall_at_10", 0.85);
        metrics.record_relevancy("recall_at_10", 0.90);
        let snap = metrics.relevancy_stats["recall_at_10"].snapshot();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn resolve_expected_string_array_form() {
        let v = nbrs_variates::node::Value::Str("[1, 5, 12, 23]".into());
        assert_eq!(resolve_expected_from_value(&v), vec![1, 5, 12, 23]);
    }

    #[test]
    fn resolve_expected_string_csv_form() {
        let v = nbrs_variates::node::Value::Str("1,5,12".into());
        assert_eq!(resolve_expected_from_value(&v), vec![1, 5, 12]);
    }

    #[test]
    fn resolve_expected_native_veci32_fast_path() {
        // The fast path the dataset accessors emit
        // (`neighbor_indices_at` etc. → Value::VecI32). No
        // string round-trip happens; the slice is read directly.
        use nbrs_variates::node::{SliceArc, Value};
        let slice = SliceArc::<i32>::from_vec(vec![1, 5, 12, 23, 100]);
        let v = Value::VecI32(slice);
        assert_eq!(resolve_expected_from_value(&v), vec![1, 5, 12, 23, 100]);
    }

    #[test]
    fn resolve_expected_native_vecf32_fast_path() {
        // VecF32 ground truth (rare but legal — some datasets
        // store ranks as floats). Truncates to i64.
        use nbrs_variates::node::{SliceArc, Value};
        let slice = SliceArc::<f32>::from_vec(vec![1.0, 2.0, 3.0]);
        let v = Value::VecF32(slice);
        assert_eq!(resolve_expected_from_value(&v), vec![1, 2, 3]);
    }

    #[test]
    fn parse_relevancy_from_params() {
        let mut template = nbrs_workload::model::ParsedOp::simple("test", "SELECT key FROM t");
        template.params.insert("relevancy".into(), serde_json::json!({
            "actual": "key",
            "expected": "{ground_truth}",
            "k": 10,
            "functions": ["recall", "precision", "f1"]
        }));
        let config = parse_relevancy(&template, None).unwrap().unwrap();
        assert_eq!(config.actual_field, "key");
        assert_eq!(config.expected_binding, "{ground_truth}");
        assert_eq!(config.k, 10);
        assert!(config.r.is_none(), "r should default to None when absent");
        assert_eq!(config.functions.len(), 3);
        assert_eq!(config.functions[0], RelevancyFn::Recall);
        assert_eq!(config.functions[1], RelevancyFn::Precision);
        assert_eq!(config.functions[2], RelevancyFn::F1);
    }

    #[test]
    fn parse_relevancy_with_r_for_k_recall_at_r() {
        // 10-recall@100: retrieve 100, score against the first
        // 10. The `r` field is parsed alongside `k` from the
        // same numeric/`{name}` shapes.
        let mut template = nbrs_workload::model::ParsedOp::simple("test", "SELECT key FROM t");
        template.params.insert("relevancy".into(), serde_json::json!({
            "actual": "key",
            "expected": "{ground_truth}",
            "k": 10,
            "r": 100,
            "functions": ["recall"],
        }));
        let config = parse_relevancy(&template, None).unwrap().unwrap();
        assert_eq!(config.k, 10);
        assert_eq!(config.r, Some(100));
    }

    #[test]
    fn parse_relevancy_r_accepts_string_form() {
        let mut template = nbrs_workload::model::ParsedOp::simple("test", "SELECT key FROM t");
        template.params.insert("relevancy".into(), serde_json::json!({
            "actual": "key",
            "expected": "{ground_truth}",
            "k": "10",
            "r": "100",
        }));
        let config = parse_relevancy(&template, None).unwrap().unwrap();
        assert_eq!(config.k, 10);
        assert_eq!(config.r, Some(100));
    }

    #[test]
    fn parse_relevancy_missing() {
        let template = nbrs_workload::model::ParsedOp::simple("test", "INSERT");
        assert!(parse_relevancy(&template, None).unwrap().is_none());
    }

    #[test]
    fn parse_assertions_from_params() {
        let mut template = nbrs_workload::model::ParsedOp::simple("test", "SELECT");
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
        let template = nbrs_workload::model::ParsedOp::simple("test", "INSERT");
        assert!(parse_assertions(&template).is_empty());
    }
}
