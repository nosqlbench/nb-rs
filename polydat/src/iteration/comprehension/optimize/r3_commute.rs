// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! R3 — Lex / filter commute (spec §7.5 N2).
//!
//! `order(filter(c, p), Lex, None) → filter(order(c, Lex, None), p)`
//!
//! When untruncated, permutation and selection commute. The
//! Lex-then-filter form is equivalent and cheaper to emit
//! (Lex is streaming; filter wraps it). The R0a I5 rule will
//! then eliminate the now-redundant `order(c, Lex, None)`.
//!
//! Guard:
//! - Outer is `Order(Lex, None)`.
//! - Child is `Filter`.

use crate::iteration::comprehension::ast::Comprehension;
use crate::iteration::comprehension::strategy::StrategyName;

pub fn apply(ast: &Comprehension) -> Option<Comprehension> {
    let Comprehension::Order { child, strategy: StrategyName::Lex, truncation: None } = ast else {
        return None;
    };
    let Comprehension::Filter { child: filter_child, predicate } = child.as_ref() else {
        return None;
    };
    Some(Comprehension::Filter {
        child: Box::new(Comprehension::Order {
            child: filter_child.clone(),
            strategy: StrategyName::Lex,
            truncation: None,
        }),
        predicate: predicate.clone(),
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
    fn r3_commutes_lex_none_with_filter() {
        let inner = clause("k", &[1, 2, 3]);
        let filtered = Comprehension::filter(inner.clone(), "{k} > 0");
        let ast = Comprehension::order(filtered, StrategyName::Lex, None);
        let result = apply(&ast).unwrap();
        match result {
            Comprehension::Filter { child, predicate } => {
                assert_eq!(predicate, "{k} > 0");
                match child.as_ref() {
                    Comprehension::Order { strategy: StrategyName::Lex, truncation: None, .. } => {}
                    other => panic!("expected order(Lex, None) inside, got {other:?}"),
                }
            }
            other => panic!("expected outer Filter, got {other:?}"),
        }
    }

    #[test]
    fn r3_does_not_fire_with_truncation() {
        let inner = clause("k", &[1, 2, 3]);
        let filtered = Comprehension::filter(inner, "{k} > 0");
        let ast = Comprehension::order(filtered, StrategyName::Lex, Some(2));
        assert_eq!(apply(&ast), None);
    }

    #[test]
    fn r3_does_not_fire_for_non_lex() {
        let inner = clause("k", &[1, 2, 3]);
        let filtered = Comprehension::filter(inner, "{k} > 0");
        let ast = Comprehension::order(filtered, StrategyName::Halton, None);
        assert_eq!(apply(&ast), None);
    }

    #[test]
    fn r3_does_not_fire_without_filter_child() {
        let inner = clause("k", &[1, 2, 3]);
        let ast = Comprehension::order(inner, StrategyName::Lex, None);
        assert_eq!(apply(&ast), None);
    }
}
