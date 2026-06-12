// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Property-based soundness tests for the predicate analyzer
//! — spec §10.9.4 property 1.
//!
//! Soundness statement: when the analyzer asserts
//! `factorization = PerAxis(p_a({a}), p_b({b}))`, then for
//! every tuple `(a, b)` in the coord space, evaluating the
//! original predicate against `(a, b)` is equivalent to
//! evaluating `p_a(a) && p_b(b)`.
//!
//! These tests use hand-coded predicates plus their Rust-side
//! equivalent closures. The analyzer is run against the
//! predicate string; per-axis claims are then cross-checked
//! against the equivalent Rust expression over random tuples.
//! No external proptest / quickcheck dep — uses polydat's
//! PCG for deterministic tuple generation.

use polydat::iteration::comprehension::predicate::{
    analyze, CoordSet, Determinism, Factorization, OpaqueReason, RangeConstraint,
};

// Borrow the strategy-layer PCG for deterministic tuple
// generation. (The path is module-internal but the test
// proves the property end-to-end with public API only.)
struct TestRng {
    state: u64,
    inc: u64,
    pos: u64,
}
impl TestRng {
    fn new(seed: u64) -> Self {
        Self { state: seed, inc: 1, pos: 0 }
    }
    fn next_u64(&mut self) -> u64 {
        // Tiny inline PCG-equivalent for tuple generation.
        // Doesn't have to match polydat's exact PCG — just
        // needs to be deterministic and well-mixed.
        self.pos = self.pos.wrapping_add(1);
        let mut z = self.state
            .wrapping_add(self.inc.wrapping_mul(self.pos))
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_in(&mut self, lo: i64, hi: i64) -> i64 {
        let range = (hi - lo) as u64;
        lo + (self.next_u64() % range) as i64
    }
}

const SAMPLES: usize = 200;

#[test]
fn soundness_per_axis_eq() {
    // Predicate: `{k} == 42`
    // Analyzer should claim PerAxis({k}: Discrete([42])).
    // Sound iff for every (k, limit): (k == 42) ≡ analyzer's
    // per-axis claim on k, independent of limit.
    let coords = CoordSet::all_discrete(["k", "limit"]);
    let info = analyze("{k} == 42", &coords);
    assert!(matches!(info.factorization, Factorization::PerAxis(_)));
    assert!(info.range_constraint.get("k").is_some());

    // Verify on random tuples: the analyzer's range constraint
    // for k must classify every k value identically to the
    // original predicate.
    let mut rng = TestRng::new(7);
    for _ in 0..SAMPLES {
        let k = rng.next_in(-100, 100);
        let _limit = rng.next_in(-100, 100);
        let original = k == 42;
        let from_range = match info.range_constraint.get("k").unwrap() {
            RangeConstraint::Discrete(vs) => vs.iter().any(|v| matches!(v,
                polydat::iteration::comprehension::predicate::info::ConstValue::Int(n) if *n == k
            )),
            _ => panic!("expected Discrete"),
        };
        assert_eq!(original, from_range, "k = {k}");
    }
}

#[test]
fn soundness_per_axis_gt() {
    let coords = CoordSet::all_discrete(["k"]);
    let info = analyze("{k} > 10", &coords);
    let range = info.range_constraint.get("k").unwrap();

    let mut rng = TestRng::new(11);
    for _ in 0..SAMPLES {
        let k = rng.next_in(-50, 50);
        let original = k > 10;
        let from_range = match range {
            RangeConstraint::Bounded { lo: Some(lo), lo_inclusive, .. } => match lo {
                polydat::iteration::comprehension::predicate::info::ConstValue::Int(lv) => {
                    if *lo_inclusive { k >= *lv } else { k > *lv }
                }
                _ => panic!(),
            },
            other => panic!("expected Bounded, got {other:?}"),
        };
        assert_eq!(original, from_range, "k = {k}");
    }
}

#[test]
fn soundness_range_fold() {
    // `10 <= {k} && {k} <= 100` should fold to PerAxis with
    // a single Bounded(10, 100) inclusive on both ends.
    let coords = CoordSet::all_discrete(["k"]);
    let info = analyze("10 <= {k} && {k} <= 100", &coords);
    let range = info.range_constraint.get("k").unwrap();

    let mut rng = TestRng::new(17);
    for _ in 0..SAMPLES {
        let k = rng.next_in(-50, 200);
        let original = (10..=100).contains(&k);
        let from_range = match range {
            RangeConstraint::Bounded {
                lo: Some(lo),
                hi: Some(hi),
                lo_inclusive,
                hi_inclusive,
            } => {
                let lo_i = match lo {
                    polydat::iteration::comprehension::predicate::info::ConstValue::Int(n) => *n,
                    _ => panic!(),
                };
                let hi_i = match hi {
                    polydat::iteration::comprehension::predicate::info::ConstValue::Int(n) => *n,
                    _ => panic!(),
                };
                let lo_ok = if *lo_inclusive { k >= lo_i } else { k > lo_i };
                let hi_ok = if *hi_inclusive { k <= hi_i } else { k < hi_i };
                lo_ok && hi_ok
            }
            other => panic!("expected Bounded, got {other:?}"),
        };
        assert_eq!(original, from_range, "k = {k}");
    }
}

#[test]
fn soundness_discrete_set() {
    let coords = CoordSet::all_discrete(["k"]);
    let info = analyze("{k} in [1, 7, 42]", &coords);
    let range = info.range_constraint.get("k").unwrap();

    let mut rng = TestRng::new(19);
    for _ in 0..SAMPLES {
        let k = rng.next_in(-100, 100);
        let original = matches!(k, 1 | 7 | 42);
        let from_range = match range {
            RangeConstraint::Discrete(vs) => vs.iter().any(|v| matches!(
                v,
                polydat::iteration::comprehension::predicate::info::ConstValue::Int(n) if *n == k
            )),
            _ => panic!(),
        };
        assert_eq!(original, from_range, "k = {k}");
    }
}

#[test]
fn soundness_per_axis_conjunction_disjoint_axes() {
    // `{k} > 5 && {limit} < 100` — analyzer claims PerAxis with
    // independent constraints on k and limit. Soundness: for
    // every (k, limit), the original is equivalent to the
    // per-axis claims AND'd together.
    let coords = CoordSet::all_discrete(["k", "limit"]);
    let info = analyze("{k} > 5 && {limit} < 100", &coords);
    let k_range = info.range_constraint.get("k").unwrap();
    let l_range = info.range_constraint.get("limit").unwrap();

    let mut rng = TestRng::new(23);
    for _ in 0..SAMPLES {
        let k = rng.next_in(-50, 50);
        let limit = rng.next_in(-200, 200);
        let original = k > 5 && limit < 100;
        let k_ok = match k_range {
            RangeConstraint::Bounded {
                lo: Some(polydat::iteration::comprehension::predicate::info::ConstValue::Int(lv)),
                lo_inclusive, ..
            } => {
                if *lo_inclusive { k >= *lv } else { k > *lv }
            }
            _ => panic!(),
        };
        let l_ok = match l_range {
            RangeConstraint::Bounded {
                hi: Some(polydat::iteration::comprehension::predicate::info::ConstValue::Int(hv)),
                hi_inclusive, ..
            } => {
                if *hi_inclusive { limit <= *hv } else { limit < *hv }
            }
            _ => panic!(),
        };
        assert_eq!(original, k_ok && l_ok, "k = {k}, limit = {limit}");
    }
}

#[test]
fn cross_axis_not_factored_as_per_axis() {
    // `{k} == {limit}` cross-cuts; the analyzer must NOT
    // factor it as PerAxis. (Sound-by-construction: PerAxis
    // would be unsound.)
    let coords = CoordSet::all_discrete(["k", "limit"]);
    let info = analyze("{k} == {limit}", &coords);
    assert!(!matches!(info.factorization, Factorization::PerAxis(_)),
        "cross-axis predicate must NOT be PerAxis, got {:?}",
        info.factorization);
}

#[test]
fn unknown_pattern_marked_opaque_not_factored() {
    // The analyzer should fall back to Opaque rather than
    // making any factorization claim for shapes it can't
    // recognize.
    let coords = CoordSet::all_discrete(["k"]);
    let info = analyze("polynomial_factorization({k}) > 0", &coords);
    assert!(matches!(
        info.factorization,
        Factorization::Opaque(OpaqueReason::UnknownPattern)
    ));
}

#[test]
fn determinism_preserved_across_calls() {
    let coords = CoordSet::all_discrete(["k", "limit"]);
    let info_a = analyze("{k} > 5 && {limit} <= 100", &coords);
    let info_b = analyze("{k} > 5 && {limit} <= 100", &coords);
    assert_eq!(info_a, info_b);
    assert_eq!(info_a.determinism, Determinism::Deterministic);
}
