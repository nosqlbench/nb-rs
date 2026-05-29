// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Parity smoke tests for the [`ComprehensionSpec`] friendly
//! surface — confirms that for representative shapes, the
//! algebra AST built via the new spec surface dispenses the
//! same tuple sequence as the legacy parser + executor path
//! (compiled IR interpreter).
//!
//! The smoke test deliberately doesn't try to enumerate every
//! algebra shape — the per-PR unit tests do that. It exists to
//! catch any global drift between the two paths (e.g., source-
//! string parsing diverging from legacy semantics, structural
//! detection rule mismatch).
//!
//! ## Scope
//!
//! - Single-clause cartesian
//! - Multi-clause cartesian
//! - Union of sub-spaces (name-repetition triggers Union)
//! - With `where` filter
//! - With `order` truncation
//!
//! Each case constructs the algebra AST via both paths
//! (`parse_comprehension_text` + `legacy_to_algebra` vs.
//! `ComprehensionSpec::into_algebra`) and asserts the
//! resulting compiled programs dispense identical tuples.

use polydat::comprehension::spec::{legacy_to_algebra, parse_comprehension_text, parse_text};
use polydat::comprehension::surfaces::{compile, CoordinateStream};

/// Dispense up to `cap` tuples from a coord stream and return
/// them as Debug-rendered strings (cheap structural compare).
fn dispense(stream: &mut CoordinateStream, cap: usize) -> Vec<String> {
    let mut out = Vec::new();
    for _ in 0..cap {
        match stream.next() {
            Some(tuple) => out.push(format!("{tuple:?}")),
            None => break,
        }
    }
    out
}

/// Compile both ASTs and assert their dispense sequences match.
fn assert_dispense_parity(legacy_text: &str, spec_yaml: &str) {
    // Path A: legacy text → legacy AST → algebra AST.
    let legacy_ast = parse_comprehension_text(legacy_text)
        .expect("legacy parse should succeed");
    let algebra_a = legacy_to_algebra(&legacy_ast)
        .expect("legacy → algebra conversion should succeed");

    // Path B: YAML text → ComprehensionSpec → algebra AST.
    let algebra_b = parse_text(spec_yaml).expect("spec parse should succeed");

    // Dispense both, compare tuple-by-tuple.
    let mut stream_a = compile(&algebra_a).coordinate_stream();
    let mut stream_b = compile(&algebra_b).coordinate_stream();
    let tuples_a = dispense(&mut stream_a, 200);
    let tuples_b = dispense(&mut stream_b, 200);
    assert_eq!(
        tuples_a, tuples_b,
        "dispense sequence parity failed:\n  legacy: {legacy_text}\n  spec:   {spec_yaml}\n  legacy_ast: {legacy_ast:?}"
    );
}

#[test]
fn single_clause_cartesian_parity() {
    assert_dispense_parity(
        "k in 1..5",
        r#"
            for: "k in 1..5"
        "#,
    );
}

#[test]
fn multi_clause_cartesian_parity() {
    assert_dispense_parity(
        "k in 1..3, limit in [10, 100]",
        r#"
            for: "k in 1..3, limit in [10, 100]"
        "#,
    );
}

#[test]
fn multi_clause_cartesian_via_list_form_parity() {
    assert_dispense_parity(
        "k in 1..3, limit in [10, 100]",
        r#"
            for:
              - "k in 1..3"
              - "limit in [10, 100]"
        "#,
    );
}

#[test]
fn union_via_repeated_names_parity() {
    // Legacy detection rule: name `k` repeats across sub-spaces
    // ⇒ Union. The spec surface's UnionOfClauseLists form
    // makes this explicit.
    assert_dispense_parity(
        "k in [10, 20], k in [100, 200]",
        r#"
            for:
              - ["k in [10, 20]"]
              - ["k in [100, 200]"]
        "#,
    );
}

#[test]
fn cartesian_with_where_parity() {
    assert_dispense_parity(
        "k in 1..5, limit in 1..5 where {k} + {limit} <= 5",
        r#"
            for: "k in 1..5, limit in 1..5"
            where: "{k} + {limit} <= 5"
        "#,
    );
}

#[test]
fn cartesian_with_order_truncation_parity() {
    assert_dispense_parity(
        "k in 1..3, limit in 1..3 order lex/4",
        r#"
            for: "k in 1..3, limit in 1..3"
            order: "lex/4"
        "#,
    );
}

#[test]
fn cartesian_with_where_and_order_parity() {
    assert_dispense_parity(
        "k in 1..5, limit in 1..5 where {k} * {limit} <= 10 order lex/6",
        r#"
            for: "k in 1..5, limit in 1..5"
            where: "{k} * {limit} <= 10"
            order: "lex/6"
        "#,
    );
}

#[test]
fn json_input_parity_with_yaml() {
    // Same algebra from a JSON-shaped text block.
    assert_dispense_parity(
        "k in 1..4, limit in [1, 2]",
        r#"{
            "for": "k in 1..4, limit in [1, 2]"
        }"#,
    );
}

#[test]
fn scalar_literal_parity() {
    // Single-element literal: `k in 10` dispenses one tuple
    // {k=10}. Confirms parse_source's bare-scalar handling
    // matches legacy semantics.
    assert_dispense_parity(
        "k in 10",
        r#"
            for: "k in 10"
        "#,
    );
}
