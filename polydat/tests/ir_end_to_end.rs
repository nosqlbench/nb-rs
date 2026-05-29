// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! End-to-end IR tests: AST → optimize → compile → interpret
//! → tuples. Verifies §9.2 correctness via direct dispense
//! comparison.

use polydat::iteration::comprehension::ast::Comprehension;
use polydat::iteration::comprehension::ir::{compile, interpret};
use polydat::iteration::comprehension::optimize::optimize;
use polydat::iteration::comprehension::source::{LiteralValue, Source};
use polydat::iteration::comprehension::strategies::TupleValue;
use polydat::iteration::comprehension::strategy::{StrategyName, ZipMode};

fn clause(name: &str, vs: &[i64]) -> Comprehension {
    Comprehension::clause(
        name,
        Source::Literal {
            values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
        },
    )
}

fn dispense(ast: Comprehension) -> Vec<Vec<(String, TupleValue)>> {
    let optimized = optimize(ast);
    let prog = compile(&optimized);
    let mut stream = interpret(&prog);
    let mut tuples = Vec::new();
    while let Some(t) = stream.advance() {
        tuples.push(t.bindings);
    }
    tuples
}

fn dispense_naive(ast: Comprehension) -> Vec<Vec<(String, TupleValue)>> {
    // Skip the optimizer: compile + interpret the un-optimized
    // AST directly. Per §9.2 the dispense sequence must match.
    let prog = compile(&ast);
    let mut stream = interpret(&prog);
    let mut tuples = Vec::new();
    while let Some(t) = stream.advance() {
        tuples.push(t.bindings);
    }
    tuples
}

#[test]
fn cartesian_dispense_length() {
    let ast = Comprehension::cartesian(vec![
        clause("a", &[1, 2, 3]),
        clause("b", &[10, 20]),
    ]);
    let tuples = dispense(ast);
    assert_eq!(tuples.len(), 6);
}

#[test]
fn nested_cartesian_after_flattening_dispense_unchanged() {
    let nested = Comprehension::cartesian(vec![
        Comprehension::cartesian(vec![clause("a", &[1, 2]), clause("b", &[10])]),
        clause("c", &[100, 200]),
    ]);
    let flat = Comprehension::cartesian(vec![
        clause("a", &[1, 2]),
        clause("b", &[10]),
        clause("c", &[100, 200]),
    ]);
    assert_eq!(dispense(nested), dispense(flat));
}

#[test]
fn singleton_cartesian_collapses_to_clause_dispense() {
    let wrapped = Comprehension::cartesian(vec![clause("k", &[1, 2, 3])]);
    let bare = clause("k", &[1, 2, 3]);
    assert_eq!(dispense(wrapped), dispense(bare));
}

#[test]
fn order_lex_none_eliminates() {
    let wrapped = Comprehension::order(clause("k", &[1, 2, 3]), StrategyName::Lex, None);
    let bare = clause("k", &[1, 2, 3]);
    assert_eq!(dispense(wrapped), dispense(bare));
}

#[test]
fn filter_then_cartesian_distributes() {
    let pre = Comprehension::filter(
        Comprehension::union(vec![
            Comprehension::cartesian(vec![clause("k", &[1, 2, 3]), clause("limit", &[10, 20])]),
            Comprehension::cartesian(vec![clause("k", &[4, 5, 6]), clause("limit", &[30, 40])]),
        ]),
        "{k} > 2",
    );
    let tuples = dispense(pre);
    // Original: union of (3×2)+(3×2)=12 tuples. After filter:
    // k > 2 keeps k in {3, 4, 5, 6}. So 1*2 + 3*2 = 8 tuples.
    assert_eq!(tuples.len(), 8);
}

#[test]
fn chained_filters_fold_dispense_invariant() {
    let chained = Comprehension::filter(
        Comprehension::filter(clause("k", &[1, 2, 3, 4, 5]), "{k} > 1"),
        "{k} < 5",
    );
    let folded = Comprehension::filter(clause("k", &[1, 2, 3, 4, 5]), "({k} > 1) && ({k} < 5)");
    assert_eq!(dispense(chained), dispense(folded));
}

#[test]
fn zip_strict_dispense() {
    let ast = Comprehension::zip(
        vec![clause("x", &[1, 2, 3]), clause("y", &[10, 20, 30])],
        ZipMode::Strict,
    );
    let tuples = dispense(ast);
    assert_eq!(tuples.len(), 3);
    for (i, t) in tuples.iter().enumerate() {
        let x_expected = (i + 1) as i64;
        let y_expected = ((i + 1) * 10) as i64;
        assert_eq!(t[0].1, TupleValue::I64(x_expected));
        assert_eq!(t[1].1, TupleValue::I64(y_expected));
    }
}

#[test]
fn order_truncate_caps_output() {
    let ast = Comprehension::order(
        Comprehension::cartesian(vec![clause("k", &[1, 2, 3, 4, 5]), clause("l", &[10, 20, 30])]),
        StrategyName::Lex,
        Some(7),
    );
    let tuples = dispense(ast);
    assert_eq!(tuples.len(), 7);
}

#[test]
fn order_materialize_shuffle_emits_all() {
    let ast = Comprehension::order(clause("k", &[1, 2, 3, 4, 5]), StrategyName::Shuffle, None);
    let tuples = dispense(ast);
    assert_eq!(tuples.len(), 5);
    let mut values: Vec<i64> = tuples
        .iter()
        .map(|t| match t[0].1 {
            TupleValue::I64(n) => n,
            _ => panic!(),
        })
        .collect();
    values.sort();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[test]
fn section_92_equivalence_simple_filter() {
    // §9.2 correctness contract: optimized AST and
    // un-optimized AST produce identical dispense sequences.
    let ast = Comprehension::filter(
        Comprehension::cartesian(vec![clause("k", &[1, 2, 3, 4, 5]), clause("l", &[10, 20])]),
        "{k} > 2",
    );
    let naive = dispense_naive(ast.clone());
    let optimized = dispense(ast);
    assert_eq!(naive, optimized);
}

#[test]
fn section_92_equivalence_nested_combinators() {
    let ast = Comprehension::filter(
        Comprehension::union(vec![
            Comprehension::cartesian(vec![
                Comprehension::cartesian(vec![clause("a", &[1, 2])]),
                clause("b", &[10, 20]),
            ]),
            Comprehension::cartesian(vec![clause("a", &[3, 4]), clause("b", &[30, 40])]),
        ]),
        "{a} > 1",
    );
    let naive = dispense_naive(ast.clone());
    let optimized = dispense(ast);
    assert_eq!(naive, optimized);
}
