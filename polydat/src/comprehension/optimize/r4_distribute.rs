// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! R4 — filter distributes over union (spec §7.3 D1).
//!
//! `filter(union(a, b, ...), p) → union(filter(a, p), filter(b, p), ...)`
//!
//! Each child becomes its own filtered sub-pipeline; downstream
//! barriers (if any) see smaller inputs.
//!
//! Unconditionally safe because V2 requires identical tuple
//! shape across union children — every name `p` could
//! reference is bound by every child, so there's no
//! "this predicate makes sense against child a but not child
//! b" case.
//!
//! Guard:
//! - Outer is `Filter`.
//! - Child is `Union`.

use crate::comprehension::ast::Comprehension;

pub fn apply(ast: &Comprehension) -> Option<Comprehension> {
    let Comprehension::Filter { child, predicate } = ast else {
        return None;
    };
    let Comprehension::Union { children: union_children } = child.as_ref() else {
        return None;
    };
    let distributed: Vec<Comprehension> = union_children
        .iter()
        .map(|c| Comprehension::Filter {
            child: Box::new(c.clone()),
            predicate: predicate.clone(),
        })
        .collect();
    Some(Comprehension::Union { children: distributed })
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn r4_distributes_filter_into_union_children() {
        let a = Comprehension::cartesian(vec![clause("k", &[10]), clause("limit", &[1, 2])]);
        let b = Comprehension::cartesian(vec![clause("k", &[100]), clause("limit", &[3, 4])]);
        let ast = Comprehension::filter(
            Comprehension::union(vec![a.clone(), b.clone()]),
            "{limit} > 1",
        );
        let result = apply(&ast).unwrap();
        match result {
            Comprehension::Union { children } => {
                assert_eq!(children.len(), 2);
                for child in &children {
                    assert!(matches!(child, Comprehension::Filter { predicate, .. } if predicate == "{limit} > 1"));
                }
            }
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn r4_preserves_child_order() {
        let a = clause("x", &[1]);
        let b = clause("x", &[2]);
        let c = clause("x", &[3]);
        let ast = Comprehension::filter(
            Comprehension::union(vec![a, b, c]),
            "true",
        );
        let result = apply(&ast).unwrap();
        match result {
            Comprehension::Union { children } => {
                assert_eq!(children.len(), 3);
                // Each Filter wraps the original child in order.
                for (i, child) in children.iter().enumerate() {
                    let inner = match child {
                        Comprehension::Filter { child, .. } => child.as_ref(),
                        other => panic!("expected Filter at index {i}, got {other:?}"),
                    };
                    let expected_value = (i + 1) as i64;
                    match inner {
                        Comprehension::Clause { source: Source::Literal { values }, .. } => {
                            assert_eq!(values[0], LiteralValue::Int(expected_value));
                        }
                        other => panic!("unexpected inner {other:?}"),
                    }
                }
            }
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn r4_does_not_fire_without_filter() {
        let ast = Comprehension::union(vec![clause("a", &[1]), clause("a", &[2])]);
        assert_eq!(apply(&ast), None);
    }

    #[test]
    fn r4_does_not_fire_without_union_child() {
        let ast = Comprehension::filter(clause("a", &[1]), "true");
        assert_eq!(apply(&ast), None);
    }
}
