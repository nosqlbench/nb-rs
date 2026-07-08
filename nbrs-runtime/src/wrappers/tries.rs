// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Retry wrapper — the innermost op-level wrapper. It owns the whole
//! ATTEMPT→RESULT boundary: it captures op identity on entry, runs the inner
//! op (adapter) one-or-more times, owns the `attempt_*` counters, catches a
//! per-attempt panic, and returns exactly ONE terminal outcome to the layers
//! above. Everything above this wrapper — traversal, result-binding, metrics,
//! and the fiber loop's result-level accounting (`result_*`, the error policy,
//! `errors_total`, `should_stop`) — sees a single result, never the retries.
//!
//! This localises what used to be smeared across the fiber loop:
//! - `attempt_total` / `attempt_success` / `attempt_failure` (per attempt);
//! - the retry counter (`retries` additional attempts on a retryable error);
//! - the `catch_unwind` that keeps a panicking adapter from killing the fiber
//!   (a panic becomes a synthesised `panic` op error — a retryable-if-you-like
//!   attempt failure that is NOT adapter-retryable, so it does not spin).
//!
//! Retryability is the ADAPTER's signal: an `ExecutionError::Op` whose
//! `retryable` flag is set (CQL timeouts/overloads are). The `errors:` policy
//! is deliberately NOT consulted here — it governs the *result* level, after
//! the attempt budget is spent. `retries: 0` = a single attempt.

use std::sync::Arc;
use std::time::Instant;

use crate::activity::ActivityMetrics;
use crate::adapter::{AdapterError, ExecutionError, OpDispenser, OpResult, WrappingDispenser};

/// Wraps an inner dispenser with a bounded retry loop over retryable attempt
/// failures, owning the `attempt_*` metrics and the per-attempt panic catch.
pub struct RetryDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Additional attempts beyond the first on a retryable error. Total
    /// attempts = `retries + 1`. `0` = single attempt (no retry).
    retries: u32,
    /// Activity-level metrics — the attempt tallies live here so the whole
    /// phase shares one attempt counter regardless of which op path ran.
    metrics: Arc<ActivityMetrics>,
}

impl RetryDispenser {
    /// Wrap `inner` with retry behaviour. Always constructed (innermost), so
    /// the attempt counters + panic catch live in exactly one place.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        retries: u32,
        metrics: Arc<ActivityMetrics>,
    ) -> Arc<dyn OpDispenser> {
        Arc::new(Self { inner, retries, metrics })
    }
}

impl WrappingDispenser for RetryDispenser {}

impl OpDispenser for RetryDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // `attempt_no` counts attempts (1-based); total attempts run is at
            // most `retries + 1`. It is also the value recorded into
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
                        // budget. Adapter-level errors are never retried here
                        // (they are connection-level, not per-op).
                        let retryable = matches!(&e, ExecutionError::Op(ad) if ad.retryable);
                        if retryable && attempt_no <= self.retries {
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
