// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! End-to-end optimizer tests against spec §11 worked
//! examples. Validates that the full R-rule catalog applied
//! to a fixed point produces the expected canonical form.

use polydat::comprehension::ast::Comprehension;
use polydat::comprehension::optimize::{
    analyze_reducibility, optimize, ComplexityOrdering, RuleId,
};
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

#[test]
fn spec_section_11_5_filter_distribution_shrinking() {
    // From spec §11.5 / §10.5: filter over union with per-axis
    // sub-predicate. R4 distributes the filter; R5 pushes
    // per-axis into each cartesian.
    let union = Comprehension::union(vec![
        Comprehension::cartesian(vec![clause("k", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), clause("x", &[1, 2, 3])]),
        Comprehension::cartesian(vec![clause("k", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), clause("x", &[11, 12, 13])]),
    ]);
    let filtered = Comprehension::filter(union, "{k} > 5");

    let optimized = optimize(filtered);

    // After R4 + R5: union of cartesians each with filtered k.
    match optimized {
        Comprehension::Union { children } => {
            assert_eq!(children.len(), 2);
            for child in &children {
                match child {
                    Comprehension::Cartesian { children: cart_kids } => {
                        // k child should be wrapped in filter.
                        assert!(matches!(&cart_kids[0], Comprehension::Filter { .. }));
                        // x child should be unwrapped.
                        assert!(matches!(&cart_kids[1], Comprehension::Clause { name, .. } if name == "x"));
                    }
                    other => panic!("expected Cartesian in union child, got {other:?}"),
                }
            }
        }
        other => panic!("expected outer Union, got {other:?}"),
    }
}

#[test]
fn optimizer_eliminates_redundant_inner_order() {
    // R7: inner untruncated order is redundant; outer wins.
    // Then R0a eliminates the now-redundant outer Lex/None
    // wrapper if applicable.
    let inner_order = Comprehension::order(clause("k", &[1, 2, 3, 4, 5]), StrategyName::Shuffle, None);
    let outer_order = Comprehension::order(inner_order, StrategyName::Halton, Some(3));

    let optimized = optimize(outer_order);

    match optimized {
        Comprehension::Order { child, strategy: StrategyName::Halton, truncation: Some(3) } => {
            assert!(matches!(&*child, Comprehension::Clause { .. }));
        }
        other => panic!("expected Order(Halton, Some(3)) wrapping a Clause, got {other:?}"),
    }
}

#[test]
fn optimizer_folds_chained_filters() {
    // R6: chained filters fold into one.
    let inner = clause("k", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let f1 = Comprehension::filter(inner, "{k} > 0");
    let f2 = Comprehension::filter(f1, "{k} < 5");

    let optimized = optimize(f2);

    match optimized {
        Comprehension::Filter { predicate, .. } => {
            assert!(predicate.contains("{k} > 0"));
            assert!(predicate.contains("{k} < 5"));
            assert!(predicate.contains("&&"));
        }
        other => panic!("expected single Filter after fold, got {other:?}"),
    }
}

#[test]
fn optimizer_canonicalizes_nested_cartesians_to_flat() {
    // R0b: flatten nested cartesians.
    let inner_cart = Comprehension::cartesian(vec![clause("b", &[1, 2]), clause("c", &[10, 20])]);
    let outer_cart = Comprehension::cartesian(vec![clause("a", &[100]), inner_cart]);

    let optimized = optimize(outer_cart);

    match optimized {
        Comprehension::Cartesian { children } => {
            assert_eq!(children.len(), 3);
            assert!(matches!(&children[0], Comprehension::Clause { name, .. } if name == "a"));
            assert!(matches!(&children[1], Comprehension::Clause { name, .. } if name == "b"));
            assert!(matches!(&children[2], Comprehension::Clause { name, .. } if name == "c"));
        }
        other => panic!("expected flat 3-child Cartesian, got {other:?}"),
    }
}

#[test]
fn optimizer_is_idempotent_on_random_ast() {
    // §10.6.2: optimize(optimize(C)) == optimize(C).
    let messy = Comprehension::filter(
        Comprehension::union(vec![
            Comprehension::cartesian(vec![Comprehension::cartesian(vec![clause("a", &[1])]), clause("b", &[2])]),
            Comprehension::cartesian(vec![clause("a", &[3]), clause("b", &[4])]),
        ]),
        "{a} > 0",
    );

    let once = optimize(messy.clone());
    let twice = optimize(once.clone());
    assert_eq!(once, twice, "optimizer must be idempotent");
}

#[test]
fn empty_finding_for_canonical_input() {
    // After optimization, analyze_reducibility should return
    // the empty finding.
    let canonical = clause("k", &[1, 2, 3]);
    let finding = analyze_reducibility(&canonical);
    assert!(finding.reduction.is_none());
    assert!(finding.rule.is_none());
}

#[test]
fn reducibility_finding_carries_rule_id_on_fire() {
    // A singleton cartesian fires R0a.
    let degenerate = Comprehension::cartesian(vec![clause("k", &[1])]);
    let finding = analyze_reducibility(&degenerate);
    assert_eq!(finding.rule, Some(RuleId::R0a));
    assert!(finding.reduction.is_some());
    // R0a's improvement is `less_compute`.
    assert_eq!(finding.improvement.compute_order, ComplexityOrdering::Less);
    assert_eq!(finding.improvement.memory_order, ComplexityOrdering::Equal);
}

#[test]
fn optimizer_preserves_coordinate_names() {
    // After full optimization, the coordinate set is
    // preserved — no axis disappears.
    let original = Comprehension::filter(
        Comprehension::cartesian(vec![clause("k", &[1, 2, 3, 4, 5]), clause("limit", &[10, 20, 30])]),
        "{k} > 2 && {limit} < 25",
    );
    let original_coords = original.coordinate_names();

    let optimized = optimize(original);
    let optimized_coords = optimized.coordinate_names();

    // Same coordinate set (possibly reordered but same names).
    let mut original_sorted = original_coords.clone();
    original_sorted.sort();
    let mut optimized_sorted = optimized_coords.clone();
    optimized_sorted.sort();
    assert_eq!(original_sorted, optimized_sorted);
}

#[test]
fn zip_does_not_flatten() {
    // R0b doesn't flatten zip (A3 — zip is not associative).
    // A zip whose children include another zip would be a
    // parse error in practice; we just verify the optimizer
    // doesn't try to flatten if it ever sees this shape.
    let inner_zip = Comprehension::zip(vec![clause("a", &[1, 2]), clause("b", &[3, 4])], ZipMode::Strict);
    // Wrapping inner_zip inside another zip is technically
    // V1-rejected (V1: disjoint names is fine, but the
    // result's shape becomes weird) — for this test we just
    // check R0b doesn't flatten across zip boundaries.
    let outer_cart = Comprehension::cartesian(vec![inner_zip, clause("c", &[5, 6])]);
    let optimized = optimize(outer_cart);
    // The Zip child stays intact inside the (flattened-or-not)
    // cartesian.
    fn contains_zip(c: &Comprehension) -> bool {
        match c {
            Comprehension::Zip { .. } => true,
            _ => c.children().any(contains_zip),
        }
    }
    assert!(contains_zip(&optimized));
}
