// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! End-to-end integration of the runtime-context + param-helper
//! node families (SRD 12). Exercises the full pipeline:
//!
//!   1. A session root carries a branch-scoped, f64-writable
//!      `concurrency` control that reifies as a gauge.
//!   2. A simulated fiber runs under
//!      [`with_fiber_context`] so `phase()` / `cycle()` / the
//!      task-local scope resolve.
//!   3. From inside, `control("concurrency")` returns the live
//!      gauge value; `control_set("concurrency", v)` submits a
//!      non-blocking write that later commits to the underlying
//!      `Control<u32>` via the `from_f64` converter.
//!   4. Param helpers (`in_range`, `is_positive`, `required`)
//!      are exercised end-to-end with both pass and fail paths.
//!
//! These tests cross crate boundaries (nb-metrics ↔ nb-variates)
//! and validate the contract the workload runner relies on —
//! the same contract that `personas/cassnbrs` and future web
//! / TUI writers use.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nb_metrics::component::Component;
use nb_metrics::controls::{
    BranchScope, ControlBuilder, ControlOrigin,
};
use nb_metrics::labels::Labels;
use nb_variates::node::{GkNode, Value};
use nb_variates::nodes::param_helpers::{
    InRangeU64, IsPositiveU64, RequiredU64, ThisOrU64,
};
use nb_variates::nodes::runtime_context::{
    set_session_root, set_task_cycle, with_fiber_context,
    ControlSet, ControlStr, ControlU64, ControlValue,
    CycleNow, PhaseName,
};

/// Lock serializing every test that touches the process-global
/// `SESSION_ROOT`. Parallel test execution otherwise interleaves
/// installs.
use std::sync::Mutex;
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn build_session_with_concurrency(initial: u32) -> Arc<std::sync::RwLock<Component>> {
    let root = Component::root(
        Labels::empty()
            .with("type", "session")
            .with("session", "integ"),
        HashMap::new(),
    );
    root.read().unwrap().controls().declare(
        ControlBuilder::new("concurrency", initial)
            .reify_as_gauge(|v| Some(*v as f64))
            .branch_scope(BranchScope::Subtree)
            .from_f64(|v| {
                if v < 0.0 || v > 10_000.0 {
                    Err(format!("concurrency out of range: {v}"))
                } else {
                    Ok(v as u32)
                }
            })
            .build(),
    );
    set_session_root(root.clone());
    root
}

#[tokio::test]
async fn fiber_reads_phase_and_cycle_from_task_context() {
    let _g = TEST_LOCK.lock().unwrap();
    let phase: Arc<str> = Arc::from("rampup");

    with_fiber_context(phase.clone(), async {
        // The fiber body advances the cycle a few times, asserting
        // each read reflects the most-recent update.
        for cycle in [0u64, 1, 17, 999] {
            set_task_cycle(cycle);

            let p = PhaseName::new();
            let mut out = [Value::None];
            p.eval(&[], &mut out);
            assert_eq!(out[0].as_str(), "rampup");

            let c = CycleNow::new();
            let mut cycle_out = [Value::None];
            c.eval(&[], &mut cycle_out);
            assert_eq!(cycle_out[0].as_u64(), cycle);
        }
    }).await;
}

#[tokio::test]
async fn param_helpers_pass_happy_values() {
    let _g = TEST_LOCK.lock().unwrap();
    // required: non-None value passes through.
    let n = RequiredU64::new("cycles");
    let mut out = [Value::None];
    n.eval(&[Value::U64(10_000)], &mut out);
    assert_eq!(out[0].as_u64(), 10_000);

    // is_positive: 1 passes.
    let n = IsPositiveU64::new("rate");
    let mut out = [Value::None];
    n.eval(&[Value::U64(1)], &mut out);
    assert_eq!(out[0].as_u64(), 1);

    // in_range: within bounds passes.
    let n = InRangeU64::new(1, 100);
    let mut out = [Value::None];
    n.eval(&[Value::U64(50)], &mut out);
    assert_eq!(out[0].as_u64(), 50);

    // this_or: primary when present.
    let n = ThisOrU64::new();
    let mut out = [Value::None];
    n.eval(&[Value::U64(7), Value::U64(99)], &mut out);
    assert_eq!(out[0].as_u64(), 7);
    // this_or: default when primary undefined.
    n.eval(&[Value::None, Value::U64(99)], &mut out);
    assert_eq!(out[0].as_u64(), 99);
}

#[tokio::test]
async fn fiber_reads_control_through_context() {
    let _g = TEST_LOCK.lock().unwrap();
    let root = build_session_with_concurrency(8);

    let phase: Arc<str> = Arc::from("rampup");
    with_fiber_context(phase, async {
        set_task_cycle(0);

        let c = ControlValue::new("concurrency");
        let mut out = [Value::None];
        c.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 8.0);

        // The u64 and string projections work through the same
        // walk-up.
        let u = ControlU64::new("concurrency");
        let mut u_out = [Value::None];
        u.eval(&[], &mut u_out);
        assert_eq!(u_out[0].as_u64(), 8);

        let s = ControlStr::new("concurrency");
        let mut s_out = [Value::None];
        s.eval(&[], &mut s_out);
        assert_eq!(s_out[0].as_str(), "8");
    }).await;

    let _ = root;
}

#[tokio::test]
async fn fiber_writes_control_via_control_set_and_reads_back() {
    let _g = TEST_LOCK.lock().unwrap();
    let root = build_session_with_concurrency(8);

    let phase: Arc<str> = Arc::from("rampup");
    with_fiber_context(phase, async {
        // Issue a write from inside the fiber.
        let writer = ControlSet::new("concurrency", "integration_feedback_loop");
        let mut write_out = [Value::None];
        writer.eval(&[Value::F64(42.0)], &mut write_out);
        assert_eq!(write_out[0].as_u64(), 1, "write should report submitted");

        // Write is async — give the spawned task a few cycles
        // to validate → fanout → commit.
        let mut observed = 0.0;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let r = ControlValue::new("concurrency");
            let mut out = [Value::None];
            r.eval(&[], &mut out);
            observed = out[0].as_f64();
            if observed == 42.0 {
                break;
            }
        }
        assert_eq!(observed, 42.0, "control_set's write should commit and be visible via read");
    }).await;

    // The committed Versioned carries the GK origin the writer
    // supplied — critical for attribution in logs and replay.
    let control: nb_metrics::controls::Control<u32> = root.read().unwrap()
        .controls().get("concurrency").unwrap();
    let versioned = control.get();
    assert_eq!(versioned.value, 42);
    assert!(
        matches!(versioned.origin, ControlOrigin::Gk { ref binding } if binding == "integration_feedback_loop"),
        "expected Gk origin tagged with the feedback_loop binding, got {:?}",
        versioned.origin,
    );
}

#[tokio::test]
async fn control_set_out_of_range_leaves_value_unchanged() {
    let _g = TEST_LOCK.lock().unwrap();
    let root = build_session_with_concurrency(16);
    let control: nb_metrics::controls::Control<u32> = root.read().unwrap()
        .controls().get("concurrency").unwrap();

    let phase: Arc<str> = Arc::from("rampup");
    with_fiber_context(phase, async {
        let writer = ControlSet::new("concurrency", "bad_writer");
        let mut write_out = [Value::None];
        // The f64_setter rejects values outside [0, 10_000].
        writer.eval(&[Value::F64(99_999.0)], &mut write_out);
        assert_eq!(write_out[0].as_u64(), 1);
        // Give the write task a chance to fail-and-log.
        tokio::time::sleep(Duration::from_millis(30)).await;
    }).await;

    // Committed value did NOT advance.
    assert_eq!(control.value(), 16);
    assert_eq!(control.get().rev, 0);
}

#[tokio::test]
async fn branch_scoped_control_resolves_from_descendant_fiber() {
    let _g = TEST_LOCK.lock().unwrap();
    // Session declares hdr_sigdigs = 4 with BranchScope::Subtree;
    // the fiber reads it via walk-up without declaring it locally.
    let root = Component::root(
        Labels::empty().with("type", "session").with("session", "integ_bs"),
        HashMap::new(),
    );
    root.read().unwrap().controls().declare(
        ControlBuilder::new("hdr_sigdigs", 4u32)
            .reify_as_gauge(|v| Some(*v as f64))
            .branch_scope(BranchScope::Subtree)
            .build(),
    );
    set_session_root(root.clone());

    let phase: Arc<str> = Arc::from("any_phase");
    with_fiber_context(phase, async {
        let r = ControlValue::new("hdr_sigdigs");
        let mut out = [Value::None];
        r.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 4.0);
    }).await;
}
