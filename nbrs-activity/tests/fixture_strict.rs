// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Regression suite for the `ScopeFixture` / `PullPlan` strict
//! contract (SRD 32 §"Init-Time Fixture and Consumer Self-
//! Registration", SRD 33 §"Ground Truth Flow"). Pins:
//!
//! - Every wrapper that registers a name fails loud at init time
//!   when the program does not provision the name.
//! - Idempotent registration: the same name registered by multiple
//!   consumers collapses to one plan entry / one shared handle.
//! - The error message names both the missing binding and the
//!   set of available names, so workload authors can fix typos
//!   without spelunking.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use nbrs_activity::adapter::{
    ExecCtx, ExecutionError, OpDispenser, OpResult, ResolvedFields, ResultBody,
};
use nbrs_activity::fixture::ScopeFixture;
use nbrs_activity::validation::ValidatingDispenser;
use nbrs_activity::wrappers::{ConditionalDispenser, ThrottleDispenser};
use nbrs_metrics::labels::Labels;
use nbrs_variates::dsl::compile::compile_gk;
use nbrs_variates::kernel::GkProgram;

/// Minimal program with `cycle` + `ground_truth: Str` extern.
fn program_with_gt() -> Arc<GkProgram> {
    compile_gk(
        "inputs := (cycle)\n\
         extern ground_truth: Str = \"\"\n",
    ).expect("compile_gk").into_program()
}

/// Program with no externs — every wrapper that registers any
/// non-`cycle` name will fail.
fn program_minimal() -> Arc<GkProgram> {
    compile_gk("inputs := (cycle)\n").expect("compile_gk").into_program()
}

/// No-op inner dispenser — we never call execute in these tests.
#[derive(Debug)]
struct NoopBody;
impl ResultBody for NoopBody {
    fn to_json(&self) -> serde_json::Value { serde_json::Value::Null }
    fn as_any(&self) -> &dyn Any { self }
}
struct NoopDispenser;
impl OpDispenser for NoopDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        _ctx: &'a ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(OpResult { body: Some(Box::new(NoopBody)), captures: HashMap::new(), skipped: false })
        })
    }
}

fn relevancy_template(expected: &str) -> nbrs_workload::model::ParsedOp {
    let mut t = nbrs_workload::model::ParsedOp::simple("op", "SELECT 1");
    t.params.insert("relevancy".into(), serde_json::json!({
        "actual": "key",
        "expected": expected,
        "k": 10,
        "functions": ["recall"],
    }));
    t
}

#[test]
fn validation_unknown_expected_binding_errors_loud() {
    let program = program_minimal();
    let mut fx = ScopeFixture::new(program.clone());
    let template = relevancy_template("{not_a_thing}");
    let inner: Arc<dyn OpDispenser> = Arc::new(NoopDispenser);
    let labels = Labels::of("session", "test");
    let result = ValidatingDispenser::wrap(
        inner, &template, &labels, Some(program.as_ref()), &mut fx,
    );
    let err = match result {
        Ok(_) => panic!("wrap should have errored on unknown binding"),
        Err(e) => e,
    };
    assert!(err.contains("not_a_thing"),
        "error must name the missing binding: {err}");
    assert!(err.contains("Available outputs") || err.contains("not known to the program"),
        "error must guide toward the fix: {err}");
}

#[test]
fn validation_known_expected_binding_succeeds() {
    let program = program_with_gt();
    let mut fx = ScopeFixture::new(program.clone());
    let template = relevancy_template("{ground_truth}");
    let inner: Arc<dyn OpDispenser> = Arc::new(NoopDispenser);
    let labels = Labels::of("session", "test");
    let (_wrapped, vm) = ValidatingDispenser::wrap(
        inner, &template, &labels, Some(program.as_ref()), &mut fx,
    ).expect("wrap with known binding should succeed");
    assert!(vm.is_some(), "relevancy declared → metrics created");
    let plan = fx.seal();
    assert_eq!(plan.len(), 1);
    assert_eq!(plan.names(), vec!["ground_truth"]);
}

#[test]
fn conditional_unknown_name_errors_loud() {
    let program = program_minimal();
    let mut fx = ScopeFixture::new(program.clone());
    let inner: Arc<dyn OpDispenser> = Arc::new(NoopDispenser);
    let labels = Labels::of("session", "test");
    let metrics = Arc::new(nbrs_activity::activity::ActivityMetrics::new(&labels));
    let result = ConditionalDispenser::wrap(
        inner, "ghost_flag", metrics, &mut fx,
    );
    let err = match result {
        Ok(_) => panic!("wrap should have errored on unknown name"),
        Err(e) => e,
    };
    assert!(err.contains("ghost_flag"),
        "conditional error must name the missing binding: {err}");
    assert!(err.contains("conditional"),
        "error must identify the consumer: {err}");
}

#[test]
fn throttle_unknown_name_errors_loud() {
    let program = program_minimal();
    let mut fx = ScopeFixture::new(program.clone());
    let inner: Arc<dyn OpDispenser> = Arc::new(NoopDispenser);
    let result = ThrottleDispenser::wrap(
        inner, "ghost_delay", &mut fx,
    );
    let err = match result {
        Ok(_) => panic!("wrap should have errored on unknown name"),
        Err(e) => e,
    };
    assert!(err.contains("ghost_delay"),
        "throttle error must name the missing binding: {err}");
    assert!(err.contains("throttle"),
        "error must identify the consumer: {err}");
}

#[test]
fn duplicate_registration_is_idempotent_across_consumers() {
    // Register the same name from two different consumers' wraps;
    // expect the plan to deduplicate to a single entry.
    let program = program_with_gt();
    let mut fx = ScopeFixture::new(program.clone());

    // Validation registers `ground_truth`.
    let template = relevancy_template("{ground_truth}");
    let inner: Arc<dyn OpDispenser> = Arc::new(NoopDispenser);
    let labels = Labels::of("session", "test");
    let (validated, _vm) = ValidatingDispenser::wrap(
        inner, &template, &labels, Some(program.as_ref()), &mut fx,
    ).unwrap();

    // Throttle hijacks the same name — contrived but the
    // idempotency rule must hold regardless of intent.
    let _throttled = ThrottleDispenser::wrap(
        validated, "ground_truth", &mut fx,
    ).unwrap();

    let plan = fx.seal();
    assert_eq!(plan.len(), 1, "same name across consumers → one plan entry");
    assert_eq!(plan.names(), vec!["ground_truth"]);
}

#[test]
fn empty_fixture_seals_to_empty_plan() {
    // No consumer registers anything → sealed plan is empty,
    // resolves to empty pulls, no work at cycle time.
    let program = program_minimal();
    let fx = ScopeFixture::new(program.clone());
    let plan = fx.seal();
    assert!(plan.is_empty());
    let mut state = program.create_state();
    let pulls = plan.resolve(&mut state);
    assert!(pulls.is_empty());
    // Demonstrate that an ExecCtx with empty pulls is still well-formed.
    let fields = ResolvedFields::new(Vec::new(), Vec::new());
    let _ctx = ExecCtx::new(&fields, &pulls);
}
