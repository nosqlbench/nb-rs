// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-105 Push 1 — cone extraction correctness.
//!
//! The load-bearing invariant is differential: for any program, a
//! `Force`-compiled kernel produces bit-identical outputs to an
//! `Off`-compiled one. These tests pin that invariant on the
//! boundary types this push marshals (U64/F64), the mixed-graph
//! case (fallback node kept on the interpreter), and the panic
//! attribution contract for predicate violations inside native
//! code. Global-mode changes serialize through `MODE_LOCK`; every
//! test restores `Off` before releasing it.

use crate::ast::Value;
use crate::compile::cone::{set_default_jit_mode, JitMode};
use crate::dsl::compile::compile_polydat_with_libs;
use std::sync::Mutex;

static MODE_LOCK: Mutex<()> = Mutex::new(());

/// Run `f` with the process-default JIT mode set to `mode`,
/// restoring `Off` afterwards even if `f` panics.
fn with_mode<T>(mode: JitMode, f: impl FnOnce() -> T) -> T {
    let _guard = MODE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            set_default_jit_mode(JitMode::Off);
        }
    }
    let _reset = Reset;
    set_default_jit_mode(mode);
    f()
}

fn compile(src: &str) -> crate::kernel::PolydatKernel {
    compile_polydat_with_libs(src, None, vec![], &[], false, "cone_test")
        .expect("compile")
}

/// Pull `output` for each x in `xs`, returning the values.
fn sweep(src: &str, output: &str, xs: &[u64]) -> Vec<Value> {
    let mut k = compile(src);
    let idx = k.program().find_input("x").expect("input x");
    xs.iter()
        .map(|&x| {
            k.state().set_input(idx, Value::U64(x));
            k.pull(output).clone()
        })
        .collect()
}

fn node_count(src: &str) -> usize {
    compile(src).program().nodes.len()
}

const U64_CHAIN: &str = "input (x: u64)\n\
                         v := mul(x, 3)\n\
                         w := add(v, 7)\n";

#[test]
fn default_mode_is_off() {
    // SRD-105 Push 3 flips this to Auto behind the differential
    // battery; until then the shipped default is interpreter-only.
    // Serialize with the other tests: parallel with_mode holders
    // legitimately have the global set; under the lock it is
    // always the restored (= shipped) default.
    let _guard = MODE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    assert_eq!(crate::compile::cone::default_jit_mode(), JitMode::Off);
}

#[test]
fn force_fuses_and_matches_interpreter_u64() {
    let xs: Vec<u64> = (0..50).chain([u64::MAX / 3, u64::MAX]).collect();
    let (baseline, base_nodes) =
        with_mode(JitMode::Off, || (sweep(U64_CHAIN, "w", &xs), node_count(U64_CHAIN)));
    let (fused, fused_nodes) =
        with_mode(JitMode::Force, || (sweep(U64_CHAIN, "w", &xs), node_count(U64_CHAIN)));
    assert_eq!(baseline, fused, "cone output must be bit-identical");
    assert!(
        fused_nodes < base_nodes,
        "force must fuse mul+add into one cone: {fused_nodes} vs {base_nodes}"
    );
}

#[test]
fn force_matches_interpreter_f64_boundary() {
    // to_f64 → f64 arithmetic: the F64 bits cross the cone
    // boundary in both directions.
    let src = "input (x: u64)\n\
               f := to_f64(x)\n\
               g := ((f * 1.5) + 0.25)\n";
    let xs: Vec<u64> = (0..40).chain([1 << 52, u64::MAX >> 1]).collect();
    let baseline = with_mode(JitMode::Off, || sweep(src, "g", &xs));
    let fused = with_mode(JitMode::Force, || sweep(src, "g", &xs));
    assert_eq!(baseline, fused, "f64 cone output must be bit-identical");
}

#[test]
fn mixed_graph_keeps_fallback_on_interpreter() {
    // `default_or` consumes None (SRD-74 opt-out) — ineligible by
    // contract, so it must survive extraction as its own node while
    // the u64 chain ahead of it fuses.
    let src = "input (x: u64)\n\
               v := mul(x, 3)\n\
               w := add(v, 7)\n\
               out := default_or(w, 9)\n";
    let xs: Vec<u64> = (0..20).collect();
    let (baseline, base_nodes) =
        with_mode(JitMode::Off, || (sweep(src, "out", &xs), node_count(src)));
    let (fused, fused_nodes) =
        with_mode(JitMode::Force, || (sweep(src, "out", &xs), node_count(src)));
    assert_eq!(baseline, fused);
    assert!(
        fused_nodes < base_nodes,
        "the eligible prefix must still fuse: {fused_nodes} vs {base_nodes}"
    );
}

#[test]
fn auto_requires_two_members() {
    // A single eligible node clears Force's threshold but not
    // Auto's: fusing one node buys no fusion win and pays boundary
    // marshalling.
    let src = "input (x: u64)\n\
               v := mul(x, 3)\n";
    let auto_nodes = with_mode(JitMode::Auto, || node_count(src));
    let force_nodes = with_mode(JitMode::Force, || node_count(src));
    let off_nodes = with_mode(JitMode::Off, || node_count(src));
    assert_eq!(auto_nodes, off_nodes, "auto must not fuse a 1-node cone");
    assert_eq!(force_nodes, off_nodes, "a 1-node cone replaces 1 node with 1 cone");
    let xs: Vec<u64> = (0..10).collect();
    let baseline = with_mode(JitMode::Off, || sweep(src, "v", &xs));
    let forced = with_mode(JitMode::Force, || sweep(src, "v", &xs));
    assert_eq!(baseline, forced);
}

#[test]
fn violation_inside_cone_attributes_members() {
    // A predicate violation in native code must surface through
    // invoke_with_catch → eval_node enrichment, naming both the
    // violated predicate and the cone (whose label lists the fused
    // member functions).
    let src = "input (x: u64)\n\
               checked := is_positive(mul(x, 0))\n";
    let msg = with_mode(JitMode::Force, || {
        let mut k = compile(src);
        let idx = k.program().find_input("x").expect("input x");
        k.state().set_input(idx, Value::U64(5));
        let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            k.pull("checked");
        }))
        .expect_err("violation must panic");
        err.downcast_ref::<String>()
            .cloned()
            .or_else(|| err.downcast_ref::<&'static str>().map(|s| (*s).to_string()))
            .expect("string payload")
    });
    assert!(
        msg.contains("is_positive"),
        "violation names the predicate: {msg}"
    );
    assert!(
        msg.contains("jit_cone["),
        "enrichment names the cone with its members: {msg}"
    );
}

#[test]
fn const_subgraphs_stay_on_the_fold_path() {
    // A compile-time-constant subgraph must NOT fuse: const
    // folding evaluates it once and replaces it with a literal
    // (feeding `get_constant` consumers like eval_const_expr);
    // fusing it would demote it to per-pull native evaluation and
    // break the single-output fold replacement. Lifecycle
    // classification keeps extraction on per-cycle (Dynamic)
    // work only.
    let v = with_mode(JitMode::Force, || {
        crate::dsl::compile::eval_const_expr("mod(hash(42), 100)")
    })
    .expect("const expr must fold under force");
    assert!(v.as_u64() < 100);
    let baseline = with_mode(JitMode::Off, || {
        crate::dsl::compile::eval_const_expr("mod(hash(42), 100)")
    })
    .expect("const expr folds under off");
    assert_eq!(v, baseline, "fold result is mode-independent");
}

#[test]
fn scope_init_chains_stay_on_the_fold_path() {
    // Extern-fed (IterationExtern) subgraphs classify ScopeInit:
    // the scope-activation fold owns them, so extraction must not
    // fuse them — only per-cycle (Coordinate/ExternalWrite-fed)
    // work joins cones.
    let src = "extern x: u64\n\
               v := mul(x, 3)\n\
               w := add(v, 7)\n";
    let off_nodes = with_mode(JitMode::Off, || node_count(src));
    let force_nodes = with_mode(JitMode::Force, || node_count(src));
    assert_eq!(
        off_nodes, force_nodes,
        "scope-init chains must not fuse"
    );
    let xs: Vec<u64> = (0..10).collect();
    let baseline = with_mode(JitMode::Off, || sweep(src, "w", &xs));
    let forced = with_mode(JitMode::Force, || sweep(src, "w", &xs));
    assert_eq!(baseline, forced);
}

/// SRD-105 panic parity: a predicate violation reports the same
/// actionable core — predicate name and violation text — whether it
/// fires on the interpreter or inside a fused cone. The cone adds
/// its member attribution; it never obscures the original message.
#[test]
fn violation_message_parity_between_engines() {
    let src = "input (x: u64)\n\
               checked := is_positive(mul(x, 0))\n";
    let capture = |mode: JitMode| -> String {
        with_mode(mode, || {
            let mut k = compile(src);
            let idx = k.program().find_input("x").expect("input x");
            k.state().set_input(idx, Value::U64(5));
            let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                k.pull("checked");
            }))
            .expect_err("violation must panic");
            err.downcast_ref::<String>()
                .cloned()
                .or_else(|| err.downcast_ref::<&'static str>().map(|s| (*s).to_string()))
                .expect("string payload")
        })
    };
    let off = capture(JitMode::Off);
    let force = capture(JitMode::Force);
    let core = "is_positive(value): value must be > 0, got 0";
    assert!(off.contains(core), "interpreter message carries the core: {off}");
    assert!(force.contains(core), "cone message carries the same core: {force}");
    assert!(off.contains("in node"), "interpreter enriches: {off}");
    assert!(force.contains("in node"), "cone enriches: {force}");
}
