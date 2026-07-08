// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! RetryDispenser wrapper (SRD-32a wrapper stack): the innermost wrapper owns
//! the attempt→result boundary. A retryable attempt failure retries up to
//! `retries` times; the retries are counted as ATTEMPTS while the op resolves
//! to a single RESULT. This test drives a fail-then-succeed adapter and asserts
//! that separation via the metrics, plus that `retries: 0` fails terminally.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use nbrs_runtime::activity::{Activity, ActivityConfig};
use nbrs_runtime::adapter::{AdapterError, DriverAdapter, ExecutionError, OpDispenser, OpResult};
use nbrs_runtime::opseq::{OpSequence, SequencerType};
use nbrs_metrics::labels::Labels;
use polydat::compile::assembly::{PolydatAssembler, WireRef};
use polydat::library::identity::Identity;

/// Adapter whose op returns a RETRYABLE error for the first `fail_first`
/// attempts (across the whole run), then succeeds. Shared counter so a single
/// op's attempts are what get retried.
struct FailThenSucceedAdapter {
    fail_first: u32,
    seen: Arc<AtomicU32>,
}

impl DriverAdapter for FailThenSucceedAdapter {
    fn name(&self) -> &str { "flaky" }

    fn map_op<'a>(
        &'a self,
        _template: &'a nbrs_workload::model::ParsedOp,
        _parent: Arc<polydat::kernel::PolydatKernel>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Box<dyn OpDispenser>, String>> + Send + 'a>>
    {
        let fail_first = self.fail_first;
        let seen = self.seen.clone();
        Box::pin(async move { Ok(Box::new(FlakyDispenser { fail_first, seen }) as Box<dyn OpDispenser>) })
    }
}

struct FlakyDispenser {
    fail_first: u32,
    seen: Arc<AtomicU32>,
}

impl OpDispenser for FlakyDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        _ctx: &'a nbrs_runtime::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>>
    {
        let n = self.seen.fetch_add(1, Ordering::SeqCst);
        let fail = n < self.fail_first;
        Box::pin(async move {
            if fail {
                Err(ExecutionError::Op(AdapterError {
                    error_name: "Timeout".into(),
                    message: "synthetic retryable timeout".into(),
                    retryable: true,
                }))
            } else {
                Ok(OpResult { body: None, skipped: false })
            }
        })
    }
}

fn test_kernel() -> polydat::kernel::PolydatKernel {
    let mut asm = PolydatAssembler::new(vec!["cycle".into()]);
    asm.add_node(
        "id",
        Box::new(Identity::new(polydat::ast::PortType::U64)),
        vec![WireRef::input("cycle")],
    );
    asm.add_output("id", WireRef::node("id"));
    asm.compile().unwrap()
}

fn one_op() -> OpSequence {
    let ops = nbrs_workload::parse::parse_ops("ops:\n  step:\n    stmt: \"x\"\n").unwrap();
    OpSequence::from_ops(ops, SequencerType::Bucket)
}

#[tokio::test]
async fn retries_move_attempt_to_result_boundary() {
    // K=2 retryable failures then success, with a budget of 3 → the single op
    // takes 3 attempts (2 retried + 1 success) to produce 1 result.
    let seen = Arc::new(AtomicU32::new(0));
    let adapter: Arc<dyn DriverAdapter> = Arc::new(FailThenSucceedAdapter { fail_first: 2, seen });

    let config = ActivityConfig {
        name: "retry".into(),
        cycles: 1,
        concurrency: 1,
        max_retries: 3,
        // Whatever the policy is, retries are gated by the adapter's
        // `retryable` flag + the budget, not by the policy.
        error_spec: ".*:warn,counter".into(),
        ..Default::default()
    };
    let activity = Activity::new(config, &Labels::of("session", "test"), one_op());
    let metrics = activity.shared_metrics();
    activity
        .run_with_driver(adapter, Arc::new(nbrs_runtime::synthesis::OpBuilder::new(test_kernel())))
        .await;

    // 3 attempts for 1 op; the op resolved successfully (1 result_success).
    assert_eq!(metrics.attempt_total.get(), 3, "2 retries + 1 success = 3 attempts");
    assert_eq!(metrics.result_total.get(), 1, "one op → one result");
    assert_eq!(metrics.result_success.count(), 1, "the op ultimately succeeded");
    assert_eq!(metrics.result_failure.count(), 0, "no terminal failure");
    // attempt_total exceeds result_total by exactly the retried count.
    assert_eq!(
        metrics.attempt_total.get() - metrics.result_total.get(),
        2,
        "attempts exceed results by the number of retries",
    );
}

#[tokio::test]
async fn zero_retries_fails_terminally() {
    // With retries=0, the first retryable failure is terminal — 1 attempt, 1
    // result, a terminal failure.
    let seen = Arc::new(AtomicU32::new(0));
    let adapter: Arc<dyn DriverAdapter> = Arc::new(FailThenSucceedAdapter { fail_first: 5, seen });

    let config = ActivityConfig {
        name: "no_retry".into(),
        cycles: 1,
        concurrency: 1,
        max_retries: 0,
        error_spec: ".*:warn,counter".into(),
        ..Default::default()
    };
    let activity = Activity::new(config, &Labels::of("session", "test"), one_op());
    let metrics = activity.shared_metrics();
    activity
        .run_with_driver(adapter, Arc::new(nbrs_runtime::synthesis::OpBuilder::new(test_kernel())))
        .await;

    assert_eq!(metrics.attempt_total.get(), 1, "retries=0 → single attempt");
    assert_eq!(metrics.result_total.get(), 1, "one op → one result");
    assert_eq!(metrics.result_failure.count(), 1, "the op failed terminally");
    assert_eq!(metrics.result_success.count(), 0, "no success");
}
