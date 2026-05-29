// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Spec §11 worked examples — end-to-end verification.
//!
//! Each test corresponds to one §11.x example from
//! `polydat/docs/design/comprehension_forms.md`. Verifies
//! that the constructed AST produces the documented
//! cardinality / dispense behavior end-to-end.

use polydat::iteration::comprehension::ast::Comprehension;
use polydat::iteration::comprehension::ir::{check_bounds, compile, interpret};
use polydat::iteration::comprehension::metadata::IndexFn;
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

fn dispense(ast: &Comprehension) -> Vec<Vec<(String, TupleValue)>> {
    let opt = optimize(ast.clone());
    let prog = compile(&opt);
    let mut stream = interpret(&prog);
    let mut out = Vec::new();
    while let Some(t) = stream.advance() {
        out.push(t.bindings);
    }
    out
}

// ---- §11.1 Single Cartesian ----

#[test]
fn spec_11_1_single_cartesian_basic_dispense() {
    let ast = Comprehension::cartesian(vec![
        clause("k", &[1, 2, 3]),
        clause("profile", &[10, 20]),
    ]);
    let m = ast.metadata();
    // §11.1: cardinality = Bounded(10 × len(profiles)).
    assert!(matches!(
        m.cardinality,
        polydat::iteration::comprehension::cardinality::CardinalityClass::Bounded(6)
    ));
    // §11.1: index_addressable = Some(Lattice { axis_sizes: [10, len] }).
    assert!(matches!(
        m.index_addressable,
        Some(IndexFn::Lattice { ref axis_sizes }) if axis_sizes == &vec![3u64, 2]
    ));
    // Dispense: 3 × 2 = 6 tuples in Lex.
    let tuples = dispense(&ast);
    assert_eq!(tuples.len(), 6);
}

// ---- §11.2 Cartesian with filter and order (Extrema/5) ----

#[test]
fn spec_11_2_filter_then_extrema_truncated() {
    let cart = Comprehension::cartesian(vec![clause("k", &[1, 5, 10]), clause("limit", &[10, 50, 100])]);
    let filtered = Comprehension::filter(cart, "true");
    let ast = Comprehension::order(filtered, StrategyName::Extrema, Some(5));
    // §11.2: cardinality is BoundedAtMost(5) after the
    // truncation cap. The interpreter's current Extrema
    // naive-form emits up to `truncation` tuples; the
    // indexed-form push-down (which would emit exactly the
    // 2² = 4 lattice corners) is the Phase 7 follow-up
    // documented in the implementation plan.
    let tuples = dispense(&ast);
    assert!(tuples.len() <= 5);
    assert!(!tuples.is_empty());
}

// ---- §11.3 Union of differently-modified sub-spaces ----

#[test]
fn spec_11_3_union_of_sub_spaces() {
    // Two sub-spaces, each producing tuples (k, limit).
    let sub_a = Comprehension::cartesian(vec![clause("k", &[10]), clause("limit", &[10, 50, 100])]);
    let sub_b = Comprehension::cartesian(vec![clause("k", &[100]), clause("limit", &[100, 200, 500])]);
    let ast = Comprehension::union(vec![sub_a, sub_b]);
    let tuples = dispense(&ast);
    // 1×3 + 1×3 = 6 tuples.
    assert_eq!(tuples.len(), 6);
}

// ---- §11.4 Union with outer reordering (Lex/30) ----

#[test]
fn spec_11_4_union_with_outer_lex_truncation() {
    let sub_a = Comprehension::cartesian(vec![clause("k", &[10]), clause("limit", &[10, 20, 30, 40, 50])]);
    let sub_b = Comprehension::cartesian(vec![clause("k", &[100]), clause("limit", &[100, 200, 300, 400, 500])]);
    let outer = Comprehension::order(
        Comprehension::union(vec![sub_a, sub_b]),
        StrategyName::Lex,
        Some(7),
    );
    let tuples = dispense(&outer);
    // 5 + 5 = 10 tuples, truncated to 7.
    assert_eq!(tuples.len(), 7);
}

// ---- §11.5 Halton over a union (combined index space) ----

#[test]
fn spec_11_5_halton_over_union() {
    let sub_a = Comprehension::cartesian(vec![clause("k", &[10]), clause("limit", &[10, 20, 30])]);
    let sub_b = Comprehension::cartesian(vec![clause("k", &[100]), clause("limit", &[100, 200, 300])]);
    let ast = Comprehension::order(
        Comprehension::union(vec![sub_a, sub_b]),
        StrategyName::Halton,
        Some(4),
    );
    let tuples = dispense(&ast);
    // Halton/4 over 6-element combined index → 4 tuples.
    assert_eq!(tuples.len(), 4);
}

// ---- §11.6 Filter-then-order vs order-then-filter ----

#[test]
fn spec_11_6_form_a_order_then_filter() {
    // (cart) order extrema/4 where {k} * {limit} > 50
    let cart = Comprehension::cartesian(vec![clause("k", &[1, 2, 3]), clause("limit", &[1, 2, 3])]);
    let ordered = Comprehension::order(cart, StrategyName::Extrema, Some(4));
    let form_a = Comprehension::filter(ordered, "true"); // simplified: trivially true
    let tuples = dispense(&form_a);
    // Extrema gives 4 corners; filter "true" keeps all 4.
    assert_eq!(tuples.len(), 4);
}

#[test]
fn spec_11_6_form_b_filter_then_order() {
    // (cart where p) order extrema/4
    let cart = Comprehension::cartesian(vec![clause("k", &[1, 2, 3]), clause("limit", &[1, 2, 3])]);
    let filtered = Comprehension::filter(cart, "true");
    let form_b = Comprehension::order(filtered, StrategyName::Extrema, Some(4));
    let tuples = dispense(&form_b);
    assert_eq!(tuples.len(), 4);
}

// ---- §11.7 Bounded zip ----

#[test]
fn spec_11_7_bounded_zip() {
    let ast = Comprehension::zip(
        vec![clause("x", &[1, 2, 3]), clause("y", &[10, 20, 30])],
        ZipMode::Strict,
    );
    let m = ast.metadata();
    assert!(matches!(
        m.cardinality,
        polydat::iteration::comprehension::cardinality::CardinalityClass::Bounded(3)
    ));
    let tuples = dispense(&ast);
    assert_eq!(tuples.len(), 3);
    // Each tuple has 2 bindings (x, y).
    assert_eq!(tuples[0].len(), 2);
}

// ---- §11.8 Cycle zip ----

#[test]
fn spec_11_8_cycle_zip_with_shorter_child() {
    let ast = Comprehension::zip(
        vec![
            clause("k", &[1, 2, 3, 4, 5]),
            clause("color", &[100, 200, 300]),
        ],
        ZipMode::Cycle,
    );
    let tuples = dispense(&ast);
    // Cycle: max(5, 3) = 5 dispense steps.
    assert_eq!(tuples.len(), 5);
    // color cycles: 100, 200, 300, 100, 200.
    assert_eq!(tuples[0][1].1, TupleValue::I64(100));
    assert_eq!(tuples[3][1].1, TupleValue::I64(100)); // cycle restarts
}

// ---- §11.9 Derived streamers from one base ----

#[test]
fn spec_11_9_derived_streamers_independent() {
    use polydat::iteration::comprehension::surfaces::compile as surfaces_compile;
    let base = Comprehension::cartesian(vec![clause("k", &[1, 2, 3]), clause("limit", &[10, 20])]);
    let compiled = surfaces_compile(&base);

    let s1 = compiled.coordinate_stream();
    let s2 = compiled.coordinate_stream();
    let tuples1: Vec<_> = s1.collect();
    let tuples2: Vec<_> = s2.collect();
    assert_eq!(tuples1, tuples2);
    assert_eq!(tuples1.len(), 6);
}

// ---- §11.10 Continuous parameter sweep (smoke) ----

#[test]
fn spec_11_10_continuous_sampling_via_halton() {
    use polydat::iteration::comprehension::cardinality::{Interval, ProductMeasure};
    // Continuous source wrapped in Halton sampling.
    let alpha = Comprehension::clause(
        "alpha",
        Source::ContinuousInterval {
            interval: Interval::closed(0.0, 1.0),
            measure: ProductMeasure::Uniform,
        },
    );
    let beta = Comprehension::clause(
        "beta",
        Source::ContinuousInterval {
            interval: Interval::closed(0.0, 1.0),
            measure: ProductMeasure::Uniform,
        },
    );
    let cart = Comprehension::cartesian(vec![alpha, beta]);
    let ast = Comprehension::order(cart, StrategyName::Halton, Some(50));
    // V8 satisfied — continuous wrapped in sampling order.
    let m = ast.metadata();
    assert!(matches!(
        m.cardinality,
        polydat::iteration::comprehension::cardinality::CardinalityClass::Bounded(50)
    ));
}

// ---- §11.11 Sample-then-zip (lockstep continuous pairing) ----

#[test]
fn spec_11_11_sample_then_zip_continuous_pairing() {
    use polydat::iteration::comprehension::cardinality::{Interval, ProductMeasure};
    let alpha = Comprehension::clause(
        "alpha",
        Source::ContinuousInterval {
            interval: Interval::closed(0.0, 1.0),
            measure: ProductMeasure::Uniform,
        },
    );
    let beta = Comprehension::clause(
        "beta",
        Source::ContinuousInterval {
            interval: Interval::closed(0.0, 1.0),
            measure: ProductMeasure::Uniform,
        },
    );
    // Sample each independently to Bounded, then zip.
    let alpha_samples = Comprehension::order(alpha, StrategyName::Halton, Some(20));
    let beta_samples = Comprehension::order(beta, StrategyName::Halton, Some(20));
    let paired = Comprehension::zip(vec![alpha_samples, beta_samples], ZipMode::Strict);
    let m = paired.metadata();
    // Both inner sampling orders produce Bounded(20), so the
    // zip-Strict result is Bounded(20).
    assert!(matches!(
        m.cardinality,
        polydat::iteration::comprehension::cardinality::CardinalityClass::Bounded(20)
    ));
}

// ---- §11.12 Dependent-source cartesian ----

#[test]
fn spec_11_12_dependent_source_loses_addressability() {
    // clause "replicas" depends on "k" — dependent source.
    let k_clause = clause("k", &[1, 2, 3]);
    let replicas_clause = Comprehension::clause(
        "replicas",
        Source::Generator {
            expr: "range(0, 2 * {k})".into(),
            cardinality_hint: Some(6),
        },
    );
    let ast = Comprehension::cartesian(vec![k_clause, replicas_clause]);
    let m = ast.metadata();
    // §11.12: dependent cartesian → index_addressable = None.
    assert!(m.index_addressable.is_none());
}

// ---- §11.13 Two consumption surfaces from one comprehension ----

#[test]
fn spec_11_13_three_surfaces_from_one_comprehension() {
    use polydat::iteration::comprehension::strategies::Tuple;
    use polydat::iteration::comprehension::surfaces::{compile as surfaces_compile, KernelScope};

    #[derive(Clone)]
    struct K(&'static str);
    impl KernelScope for K {
        type Scoped = (&'static str, Tuple);
        fn scope(&self, coords: &Tuple) -> Self::Scoped {
            (self.0, coords.clone())
        }
    }

    let ast = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("profile", &[10, 20])]);
    let compiled = surfaces_compile(&ast);

    let coord_tuples: Vec<_> = compiled.coordinate_stream().collect();
    let kernel_instances: Vec<_> = compiled.scoped_kernel_stream(K("parent")).collect();
    let one_shot = compiled.scope_once(
        &K("parent"),
        &Tuple::new()
            .with("k", TupleValue::I64(7))
            .with("profile", TupleValue::I64(99)),
    );

    assert_eq!(coord_tuples.len(), 4);
    assert_eq!(kernel_instances.len(), 4);
    assert_eq!(one_shot.scoped.0, "parent");
}

// ---- Resource bounds sanity ----

#[test]
fn dispense_bound_matches_simple_cartesian() {
    let ast = Comprehension::cartesian(vec![clause("a", &[1, 2, 3]), clause("b", &[10, 20])]);
    let prog = compile(&optimize(ast));
    let bound = check_bounds(&prog);
    assert!(bound.barriers.is_empty(), "fully-streaming cartesian has no barriers");
    assert!(bound.stack_depth >= 2);
}

#[test]
fn dispense_bound_matches_materialize() {
    let ast = Comprehension::order(
        Comprehension::cartesian(vec![clause("a", &[1, 2, 3]), clause("b", &[10, 20])]),
        StrategyName::Halton,
        Some(3),
    );
    let prog = compile(&optimize(ast));
    let bound = check_bounds(&prog);
    assert_eq!(bound.barriers.len(), 1);
    assert_eq!(bound.barriers[0].working_set_size, Some(3));
}
