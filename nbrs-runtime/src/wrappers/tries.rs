// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Tries wrapper — the CONDITIONAL innermost op-level wrapper (SRD-82 Part
//! 3b). `tries:` is its sigil: the TOTAL number of attempts an op may make.
//!
//! - **No `tries` in scope** → the wrapper is not constructed; the op runs
//!   single-attempt (the outermost error-handler wrapper records the
//!   single-attempt tallies).
//! - **`tries: 1`** → identical to no wrapper (single attempt); the cascade
//!   skips construction.
//! - **`tries: 0`** → the op FAILS WITHOUT EXECUTING: every cycle yields a
//!   synthesised `tries_zero` op error, routed through the op's error
//!   policy like any terminal failure. The explicit "never run this" knob.
//! - **`tries: N ≥ 2`** → up to `N` total attempts; a retryable attempt
//!   failure re-runs the inner op until the budget is spent.
//!
//! When constructed it owns the whole ATTEMPT→RESULT boundary: it runs the
//! inner op (adapter) one-or-more times, owns the `attempt_*` counters,
//! catches a per-attempt panic, and returns exactly ONE terminal outcome to
//! the layers above. Everything above it — traversal, result-binding,
//! metrics, the outermost error-handler wrapper — sees a single result,
//! never the retries.
//!
//! Retryability is the ADAPTER's signal: an `ExecutionError::Op` whose
//! `retryable` flag is set (CQL timeouts/overloads are). The `errors:`
//! policy is deliberately NOT consulted here — the two surfaces are
//! orthogonal; the policy's `retry` verb participates only by INJECTING a
//! `tries` budget at dispenser build (the SRD-82 Part 3b bridge), never by
//! steering the loop per-cycle.

use std::sync::Arc;
use std::time::Instant;

use nbrs_workload::model::ParsedOp;

use crate::activity::ActivityMetrics;
use crate::adapter::{AdapterError, ExecutionError, OpDispenser, OpResult, WrappingDispenser};
use crate::wrapper_registry::{WrapperName, WrapperRegistration};

pub const NAME: WrapperName = WrapperName::new("tries");

/// Registry trigger: the op's own `tries:` field. The full activation set is
/// wider — an in-scope `tries` wire, the inherited phase/root `tries`, or an
/// `errors:` retry-verb injection — but those resolve against the kernel /
/// config at dispenser build (the cascade), which a `ParsedOp` predicate
/// cannot see. The registry entry drives field validation + telemetry; the
/// hand-placed innermost construction is authoritative.
fn triggers(op: &ParsedOp) -> bool {
    op.params.contains_key("tries")
}

fn describe_assignment(op: &ParsedOp) -> Option<String> {
    op.params.get("tries").map(|v| format!("tries: {v}"))
}

inventory::submit! {
    WrapperRegistration {
        name: NAME,
        owned_fields: &["tries"],
        triggers,
        requires_inner: &[],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment,
        levels: &[crate::wrapper_registry::WrapperLevel::Op],
    }
}

/// Wraps an inner dispenser with a bounded attempt loop over retryable
/// attempt failures, owning the `attempt_*` metrics and the per-attempt
/// panic catch. Constructed only when a `tries` budget of `0` or `≥ 2`
/// resolves for the op (see the module doc; `1` / absent skip construction).
pub struct TriesDispenser {
    inner: Arc<dyn OpDispenser>,
    /// TOTAL attempts the op may make. `0` = fail without executing.
    /// (`1` never reaches construction — the cascade skips the wrapper.)
    tries: u32,
    /// Activity-level metrics — the attempt tallies live here so the whole
    /// phase shares one attempt counter regardless of which op path ran.
    metrics: Arc<ActivityMetrics>,
}

impl TriesDispenser {
    /// Wrap `inner` with a total-attempts budget.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        tries: u32,
        metrics: Arc<ActivityMetrics>,
    ) -> Arc<dyn OpDispenser> {
        Arc::new(Self { inner, tries, metrics })
    }
}

impl WrappingDispenser for TriesDispenser {}

impl OpDispenser for TriesDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // `tries: 0` — the op is configured to fail without executing.
            // Accounted as one failed (zero-length) attempt so the att:%
            // display stays truthful, and routed through the error policy by
            // the outermost error-handler wrapper like any terminal failure.
            if self.tries == 0 {
                self.metrics.attempt_total.inc();
                self.metrics.attempt_failure.observe(0);
                self.metrics.tries_histogram.record(0);
                return Err(ExecutionError::Op(AdapterError {
                    error_name: "tries_zero".into(),
                    message: "tries: 0 — op is configured to fail without executing".into(),
                    retryable: false,
                }));
            }
            // `attempt_no` counts attempts (1-based); total attempts run is
            // at most `tries`. It is also the value recorded into
            // `tries_histogram` per op.
            let mut attempt_no: u32 = 0;
            loop {
                attempt_no += 1;
                self.metrics.attempt_total.inc();
                let attempt_start = Instant::now();

                // Per-attempt panic catch: an adapter that unwinds out of
                // `execute` (an FFI driver on a broken connection) becomes a
                // synthesised `panic` op error rather than killing the fiber.
                // It is NOT adapter-retryable, so it counts as a failed attempt
                // and (absent a retryable classification) terminates without
                // spinning — the fiber survives regardless.
                let outcome: Result<OpResult, ExecutionError> = {
                    use futures::FutureExt as _;
                    match std::panic::AssertUnwindSafe(self.inner.execute(cycle, ctx))
                        .catch_unwind()
                        .await
                    {
                        Ok(r) => r,
                        Err(payload) => {
                            let msg = payload
                                .downcast_ref::<&'static str>()
                                .map(|s| (*s).to_string())
                                .or_else(|| payload.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "<non-string panic payload>".into());
                            Err(ExecutionError::Op(AdapterError {
                                error_name: "panic".into(),
                                message: msg,
                                retryable: false,
                            }))
                        }
                    }
                };
                let dt = attempt_start.elapsed().as_nanos() as u64;

                match outcome {
                    Ok(result) => {
                        self.metrics.attempt_success.observe(dt);
                        self.metrics.tries_histogram.record(attempt_no as u64);
                        return Ok(result);
                    }
                    Err(e) => {
                        self.metrics.attempt_failure.observe(dt);
                        // Retry only an adapter-retryable OP error, within
                        // the total-attempts budget. Adapter-level errors are
                        // never retried here (they are connection-level, not
                        // per-op).
                        let retryable = matches!(&e, ExecutionError::Op(ad) if ad.retryable);
                        if retryable && attempt_no < self.tries {
                            continue;
                        }
                        // Terminal: hand the failure up to the result level.
                        self.metrics.tries_histogram.record(attempt_no as u64);
                        return Err(e);
                    }
                }
            }
        })
    }

    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> {
        Some(self.inner.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::ResultBody;
    use crate::fixture::{ExecCtx, ResolvedPulls};
    use nbrs_metrics::labels::Labels;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Inner stub: fails with a retryable error until `fail_first` attempts
    /// have been consumed, then succeeds. Counts invocations.
    struct FlakyInner {
        fail_first: u32,
        calls: AtomicU32,
    }

    impl OpDispenser for FlakyInner {
        fn execute<'a>(
            &'a self,
            _cycle: u64,
            _ctx: &'a ExecCtx<'a>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            Box::pin(async move {
                let n = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
                if n <= self.fail_first {
                    Err(ExecutionError::Op(AdapterError {
                        error_name: "Timeout".into(),
                        message: "flaky".into(),
                        retryable: true,
                    }))
                } else {
                    Ok(OpResult { body: None::<Box<dyn ResultBody>>, skipped: false })
                }
            })
        }
    }

    fn empty_ctx() -> (crate::adapter::ResolvedFields, ResolvedPulls) {
        let fields = crate::adapter::ResolvedFields::new(vec![], vec![]);
        let pulls = ResolvedPulls::empty();
        (fields, pulls)
    }

    /// `tries: 0` fails WITHOUT invoking the inner op.
    #[tokio::test]
    async fn tries_zero_fails_without_executing() {
        let inner = Arc::new(FlakyInner { fail_first: 0, calls: AtomicU32::new(0) });
        let metrics = Arc::new(ActivityMetrics::new(&Labels::empty()));
        let d = TriesDispenser::wrap(inner.clone(), 0, metrics);
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let err = d.execute(0, &ctx).await.expect_err("tries:0 must fail");
        assert_eq!(err.error().error_name, "tries_zero");
        assert_eq!(inner.calls.load(Ordering::Relaxed), 0,
            "inner must never be invoked at tries:0");
    }

    /// `tries: 3` retries a retryable failure up to 3 TOTAL attempts and
    /// succeeds when the third works.
    #[tokio::test]
    async fn tries_is_a_total_attempt_budget() {
        let inner = Arc::new(FlakyInner { fail_first: 2, calls: AtomicU32::new(0) });
        let metrics = Arc::new(ActivityMetrics::new(&Labels::empty()));
        let d = TriesDispenser::wrap(inner.clone(), 3, metrics.clone());
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        d.execute(0, &ctx).await.expect("third attempt succeeds");
        assert_eq!(inner.calls.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.attempt_total.get(), 3);
    }

    /// The budget is TOTAL attempts: `tries: 2` against an op that needs a
    /// third attempt fails after exactly 2 invocations.
    #[tokio::test]
    async fn budget_exhaustion_is_terminal() {
        let inner = Arc::new(FlakyInner { fail_first: 5, calls: AtomicU32::new(0) });
        let metrics = Arc::new(ActivityMetrics::new(&Labels::empty()));
        let d = TriesDispenser::wrap(inner.clone(), 2, metrics);
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let err = d.execute(0, &ctx).await.expect_err("budget spent");
        assert_eq!(err.error().error_name, "Timeout");
        assert_eq!(inner.calls.load(Ordering::Relaxed), 2,
            "tries:2 = exactly two total attempts");
    }
}
