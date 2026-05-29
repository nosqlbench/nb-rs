// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! §9.3 resource-bound checker verification.
//!
//! For each AST shape, verify that:
//!   1. The bound checker's output describes the right kind
//!      of bound (zero barriers for streaming programs;
//!      explicit barrier entries for materializing forms).
//!   2. The reported working-set sizes match the post-R2
//!      optimizer's metadata (when push-down applies).

use polydat::comprehension::ast::Comprehension;
use polydat::comprehension::ir::{check_bounds, compile};
use polydat::comprehension::optimize::optimize;
use polydat::comprehension::source::{LiteralValue, Source};
use polydat::comprehension::strategy::{StrategyName, ZipMode};

fn clause(name: &str, vs: &[i64]) -> Comprehension {
    Comprehension::clause(
        name,
        Source::Literal {
            values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
        },
    )
}

fn bounds_for(ast: Comprehension) -> polydat::comprehension::ir::ResourceBound {
    let opt = optimize(ast);
    let prog = compile(&opt);
    check_bounds(&prog)
}

// ---- Streaming programs have no barriers ----

#[test]
fn cartesian_only_has_no_barriers() {
    let ast = Comprehension::cartesian(vec![
        clause("a", &[1, 2, 3]),
        clause("b", &[10, 20]),
    ]);
    let b = bounds_for(ast);
    assert!(b.barriers.is_empty());
    assert_eq!(b.total_barrier_working_set(), Some(0));
}

#[test]
fn union_only_has_no_barriers() {
    let ast = Comprehension::union(vec![
        clause("k", &[1, 2]),
        clause("k", &[10, 20]),
    ]);
    let b = bounds_for(ast);
    assert!(b.barriers.is_empty());
}

#[test]
fn zip_strict_has_no_barriers() {
    let ast = Comprehension::zip(
        vec![clause("a", &[1, 2, 3]), clause("b", &[10, 20, 30])],
        ZipMode::Strict,
    );
    let b = bounds_for(ast);
    assert!(b.barriers.is_empty());
}

#[test]
fn filter_streams() {
    let ast = Comprehension::filter(clause("k", &[1, 2, 3, 4, 5]), "true");
    let b = bounds_for(ast);
    assert!(b.barriers.is_empty());
}

#[test]
fn order_lex_streams() {
    let ast = Comprehension::order(clause("k", &[1, 2, 3]), StrategyName::Lex, Some(2));
    let b = bounds_for(ast);
    // Lex compiles to OrderStreaming — not a barrier.
    assert!(b.barriers.is_empty());
}

// ---- Materializing programs have barriers ----

#[test]
fn order_halton_has_barrier_sized_by_truncation() {
    let ast = Comprehension::order(
        Comprehension::cartesian(vec![clause("a", &[1, 2, 3]), clause("b", &[10, 20])]),
        StrategyName::Halton,
        Some(4),
    );
    let b = bounds_for(ast);
    assert_eq!(b.barriers.len(), 1);
    assert_eq!(b.barriers[0].working_set_size, Some(4));
    assert!(b.barriers[0].description.to_lowercase().contains("halton"));
}

#[test]
fn order_shuffle_has_barrier_sized_by_truncation() {
    let ast = Comprehension::order(clause("k", &[1, 2, 3, 4, 5]), StrategyName::Shuffle, Some(3));
    let b = bounds_for(ast);
    assert_eq!(b.barriers.len(), 1);
    assert_eq!(b.barriers[0].working_set_size, Some(3));
}

#[test]
fn zip_cycle_has_barrier_with_unknown_ir_layer_size() {
    let ast = Comprehension::zip(
        vec![clause("k", &[1, 2, 3, 4, 5]), clause("color", &[100, 200, 300])],
        ZipMode::Cycle,
    );
    let b = bounds_for(ast);
    assert_eq!(b.barriers.len(), 1);
    // IR layer doesn't know child cardinalities; metadata
    // layer carries the precise size (sum of non-longest
    // children).
    assert!(b.barriers[0].working_set_size.is_none());
}

#[test]
fn two_orders_two_barriers() {
    let ast = Comprehension::order(
        Comprehension::order(clause("k", &[1, 2, 3, 4, 5]), StrategyName::Shuffle, Some(3)),
        StrategyName::Halton,
        Some(2),
    );
    let b = bounds_for(ast);
    // R7 should fold the inner shuffle (no truncation? No —
    // shuffle HAS truncation, so R7 doesn't fire per O2).
    // Wait — inner DOES have truncation, so R7 leaves the
    // two-stage form. Expect 2 barriers.
    assert_eq!(b.barriers.len(), 2);
    // total = 3 (inner) + 2 (outer)
    assert_eq!(b.total_barrier_working_set(), Some(5));
}

#[test]
fn order_chain_folds_when_inner_untruncated_one_barrier() {
    // R7 fires when inner has no truncation → only the outer
    // order survives as a barrier.
    let ast = Comprehension::order(
        Comprehension::order(clause("k", &[1, 2, 3, 4, 5]), StrategyName::Shuffle, None),
        StrategyName::Halton,
        Some(2),
    );
    let b = bounds_for(ast);
    assert_eq!(b.barriers.len(), 1);
    assert_eq!(b.barriers[0].working_set_size, Some(2));
}

// ---- Stack depth tracking ----

#[test]
fn stack_depth_grows_with_combinator_arity() {
    let ast = Comprehension::cartesian(vec![
        clause("a", &[1]),
        clause("b", &[2]),
        clause("c", &[3]),
        clause("d", &[4]),
    ]);
    let b = bounds_for(ast);
    // 4 PushClause ops → max depth 4 right before CARTESIAN(4)
    // pops them.
    assert!(b.stack_depth >= 4);
}

#[test]
fn streaming_op_count_excludes_dispense() {
    let ast = Comprehension::cartesian(vec![clause("a", &[1]), clause("b", &[2])]);
    let b = bounds_for(ast);
    // Ops: PUSH(a), PUSH(b), CARTESIAN, [DISPENSE].
    // streaming_op_count = 3 (dispense excluded).
    assert_eq!(b.streaming_op_count, 3);
}
