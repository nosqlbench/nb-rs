// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! R7 — order chain folding (spec §7.4 O1).
//!
//! `order(order(c, s1, None), s2, t) → order(c, s2, t)`
//!
//! The inner full-permutation is wasted when the outer
//! reorders the whole result. The inner's `truncation` MUST
//! be `None` for the rule to fire — `order(order(c, s1, Some(n)), s2, t)`
//! is meaningful (O2: pick n in s1, then reorder those n in s2)
//! and the optimizer must NOT collapse it.
//!
//! Guard:
//! - Outer is `Order`.
//! - Child is `Order` with `truncation: None`.

use crate::iteration::comprehension::ast::Comprehension;

pub fn apply(ast: &Comprehension) -> Option<Comprehension> {
    let Comprehension::Order { child: outer_child, strategy: outer_strat, truncation: outer_trunc } = ast else {
        return None;
    };
    let Comprehension::Order { child: inner_child, truncation: None, .. } = outer_child.as_ref() else {
        return None;
    };
    Some(Comprehension::Order {
        child: inner_child.clone(),
        strategy: *outer_strat,
        truncation: *outer_trunc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::{LiteralValue, Source};
    use crate::iteration::comprehension::strategy::StrategyName;

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn r7_folds_two_orders_when_inner_untruncated() {
        let inner = clause("k", &[1, 2, 3]);
        let o1 = Comprehension::order(inner.clone(), StrategyName::Shuffle, None);
        let o2 = Comprehension::order(o1, StrategyName::Halton, Some(2));
        let result = apply(&o2).unwrap();
        match result {
            Comprehension::Order { child, strategy: StrategyName::Halton, truncation: Some(2) } => {
                assert_eq!(&*child, &inner);
            }
            other => panic!("expected Order(Halton, Some(2)) → clause, got {other:?}"),
        }
    }

    #[test]
    fn r7_does_not_fire_when_inner_truncated() {
        let inner = clause("k", &[1, 2, 3]);
        let o1 = Comprehension::order(inner, StrategyName::Shuffle, Some(2));
        let o2 = Comprehension::order(o1, StrategyName::Halton, Some(1));
        // O2 — meaningful two-stage composition.
        assert_eq!(apply(&o2), None);
    }

    #[test]
    fn r7_does_not_fire_for_single_order() {
        let ast = Comprehension::order(clause("k", &[1]), StrategyName::Lex, None);
        assert_eq!(apply(&ast), None);
    }
}
