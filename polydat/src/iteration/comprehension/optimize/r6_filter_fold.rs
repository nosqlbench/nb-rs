// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! R6 — chained filter folding (spec §7.2 F1).
//!
//! `filter(filter(c, p), q) → filter(c, p && q)`
//!
//! One predicate evaluation per tuple instead of two. The
//! folded predicate is the conjunction of the original two;
//! parentheses preserve precedence.

use crate::iteration::comprehension::ast::Comprehension;

pub fn apply(ast: &Comprehension) -> Option<Comprehension> {
    let Comprehension::Filter { child: outer_child, predicate: outer_pred } = ast else {
        return None;
    };
    let Comprehension::Filter { child: inner_child, predicate: inner_pred } = outer_child.as_ref() else {
        return None;
    };
    let folded = format!("({}) && ({})", inner_pred, outer_pred);
    Some(Comprehension::Filter {
        child: inner_child.clone(),
        predicate: folded,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::{LiteralValue, Source};

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn r6_folds_chained_filters() {
        let inner = clause("k", &[1, 2, 3, 4, 5]);
        let f1 = Comprehension::filter(inner.clone(), "{k} > 0");
        let f2 = Comprehension::filter(f1, "{k} < 5");
        let result = apply(&f2).unwrap();
        match result {
            Comprehension::Filter { child, predicate } => {
                assert_eq!(&*child, &inner);
                assert!(predicate.contains("{k} > 0"));
                assert!(predicate.contains("{k} < 5"));
                assert!(predicate.contains("&&"));
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[test]
    fn r6_does_not_fire_for_single_filter() {
        let ast = Comprehension::filter(clause("k", &[1]), "{k} > 0");
        assert_eq!(apply(&ast), None);
    }

    #[test]
    fn r6_does_not_fire_for_filter_over_non_filter() {
        let inner = Comprehension::cartesian(vec![clause("k", &[1]), clause("l", &[2])]);
        let ast = Comprehension::filter(inner, "{k} > 0");
        assert_eq!(apply(&ast), None);
    }
}
