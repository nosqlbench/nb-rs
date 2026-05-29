// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! R5 — per-axis filter pushdown (spec §7.3 D2's optimizer
//! direction + §10.2 R5).
//!
//! `filter(cartesian(c1, ..., cN), p) →
//!  cartesian(c1, ..., filter(ci, p_i), ..., cN)`
//!
//! when the predicate analyzer reports `factorization =
//! PerAxis(p_i)`. The optimizer wraps each cartesian child
//! whose coordinate appears in the analyzer's per-axis map
//! with the corresponding per-axis sub-predicate.
//!
//! Children not mentioned in the factorization keep their
//! original form. Predicates that the analyzer can't factor
//! per-axis (Conjunctive cross-cutting, Disjunctive,
//! Opaque) leave R5 dormant.
//!
//! Guard:
//! - Outer is `Filter`.
//! - Child is `Cartesian`.
//! - Predicate's `Factorization` is `PerAxis(_)`.

use crate::comprehension::ast::Comprehension;
use crate::comprehension::predicate::{
    CoordSet, Factorization, PredicateInfo,
};

/// R5's predicate-analyzer interface — accepts a closure so
/// the optimizer (which doesn't depend on predicate internals)
/// can pass `analyzer = |p, c| predicate::analyze(p, c)`.
pub fn apply<F>(ast: &Comprehension, analyze: &F) -> Option<Comprehension>
where
    F: Fn(&str, &CoordSet) -> PredicateInfo,
{
    let Comprehension::Filter { child, predicate } = ast else {
        return None;
    };
    let Comprehension::Cartesian { children: cart_children } = child.as_ref() else {
        return None;
    };

    // Build CoordSet from the cartesian's metadata.
    let coord_names = child.coordinate_names();
    let metadata = child.metadata();
    let coords = CoordSet::from_metadata(&coord_names, &metadata);

    let info = analyze(predicate, &coords);
    let per_axis = match info.factorization {
        Factorization::PerAxis(m) => m,
        _ => return None,
    };

    // For each axis with a per-axis sub-predicate, find the
    // cartesian child that binds that axis and wrap it with
    // the sub-filter. Children without a mention stay as-is.
    let mut new_children: Vec<Comprehension> = Vec::with_capacity(cart_children.len());
    let mut any_change = false;
    for child in cart_children {
        let child_coords = child.coordinate_names();
        // Collect all per-axis sub-predicates this child owns.
        let owned_subs: Vec<String> = per_axis
            .iter()
            .filter_map(|(axis, sub_pred)| {
                if child_coords.iter().any(|n| n == axis) {
                    Some(sub_pred.clone())
                } else {
                    None
                }
            })
            .collect();
        if owned_subs.is_empty() {
            new_children.push(child.clone());
        } else {
            any_change = true;
            let combined_pred = if owned_subs.len() == 1 {
                owned_subs.into_iter().next().unwrap()
            } else {
                // Fold multiple per-axis subs that bind into
                // the same child (rare — a cartesian child
                // typically binds one axis).
                owned_subs
                    .iter()
                    .map(|s| format!("({s})"))
                    .collect::<Vec<_>>()
                    .join(" && ")
            };
            new_children.push(Comprehension::filter(child.clone(), combined_pred));
        }
    }

    if !any_change {
        return None;
    }
    Some(Comprehension::Cartesian { children: new_children })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::predicate::analyze;
    use crate::comprehension::source::{LiteralValue, Source};

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn r5_pushes_per_axis_filter_into_one_child() {
        // filter(cartesian(k, limit), "{k} > 5")
        // → cartesian(filter(k, "{k} > 5"), limit)
        let cart = Comprehension::cartesian(vec![clause("k", &[1, 5, 10]), clause("limit", &[100, 200])]);
        let ast = Comprehension::filter(cart, "{k} > 5");
        let result = apply(&ast, &analyze).unwrap();
        match result {
            Comprehension::Cartesian { children } => {
                assert_eq!(children.len(), 2);
                // k-child wrapped in filter
                assert!(matches!(&children[0], Comprehension::Filter { .. }));
                // limit-child unwrapped
                assert!(matches!(&children[1], Comprehension::Clause { name, .. } if name == "limit"));
            }
            other => panic!("expected Cartesian, got {other:?}"),
        }
    }

    #[test]
    fn r5_pushes_per_axis_conjunction_to_both_children() {
        // filter(cartesian(k, limit), "{k} > 5 && {limit} < 200")
        // → cartesian(filter(k, "{k} > 5"), filter(limit, "{limit} < 200"))
        let cart = Comprehension::cartesian(vec![clause("k", &[1, 5, 10]), clause("limit", &[100, 200, 300])]);
        let ast = Comprehension::filter(cart, "{k} > 5 && {limit} < 200");
        let result = apply(&ast, &analyze).unwrap();
        match result {
            Comprehension::Cartesian { children } => {
                assert!(matches!(&children[0], Comprehension::Filter { .. }));
                assert!(matches!(&children[1], Comprehension::Filter { .. }));
            }
            other => panic!("expected Cartesian, got {other:?}"),
        }
    }

    #[test]
    fn r5_does_not_fire_for_cross_axis_predicate() {
        // filter(cartesian(k, limit), "{k} * {limit} > 100")
        // — cross-axis; analyzer marks Conjunctive, R5 doesn't fire.
        let cart = Comprehension::cartesian(vec![clause("k", &[1, 5, 10]), clause("limit", &[10, 100])]);
        let ast = Comprehension::filter(cart, "{k} * {limit} > 100");
        assert_eq!(apply(&ast, &analyze), None);
    }

    #[test]
    fn r5_does_not_fire_without_cartesian_child() {
        let ast = Comprehension::filter(clause("k", &[1, 2, 3]), "{k} > 0");
        // No cartesian to push into; even though the
        // predicate is PerAxis, the cartesian guard fails.
        assert_eq!(apply(&ast, &analyze), None);
    }

    #[test]
    fn r5_does_not_fire_for_opaque_predicate() {
        let cart = Comprehension::cartesian(vec![clause("k", &[1]), clause("l", &[2])]);
        let ast = Comprehension::filter(cart, "polynomial_factorization({k}) > 0");
        assert_eq!(apply(&ast, &analyze), None);
    }
}
