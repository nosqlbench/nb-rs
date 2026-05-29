// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! R0a — identity elimination (spec §4.2 I1–I5).
//!
//! Five identity rewrites:
//!
//! - I1 — singleton union: `union([c]) → c`.
//! - I2 — singleton cartesian: `cartesian([c]) → c`.
//! - I3 — singleton zip: `zip([c], _mode) → c`.
//! - I4 — trivially-true filter: `filter(c, "true") → c`.
//! - I5 — un-truncated Lex order: `order(c, Lex, None) → c`.
//!
//! Each rewrite strictly decreases AST node count without
//! changing the dispense sequence. R0a runs to a fixed point
//! before any non-canonicalization rule fires.

use crate::comprehension::ast::Comprehension;
use crate::comprehension::strategy::StrategyName;

/// Try to apply R0a at this node. Returns `Some(rewritten)`
/// if any of the I1–I5 patterns matches; `None` otherwise.
pub fn apply(ast: &Comprehension) -> Option<Comprehension> {
    match ast {
        // I1 — singleton union
        Comprehension::Union { children } if children.len() == 1 => {
            Some(children[0].clone())
        }
        // I2 — singleton cartesian
        Comprehension::Cartesian { children } if children.len() == 1 => {
            Some(children[0].clone())
        }
        // I3 — singleton zip
        Comprehension::Zip { children, .. } if children.len() == 1 => {
            Some(children[0].clone())
        }
        // I4 — trivially-true filter
        Comprehension::Filter { child, predicate } if is_trivially_true(predicate) => {
            Some((**child).clone())
        }
        // I5 — un-truncated Lex order
        Comprehension::Order {
            child,
            strategy: StrategyName::Lex,
            truncation: None,
        } => Some((**child).clone()),
        _ => None,
    }
}

fn is_trivially_true(predicate: &str) -> bool {
    let trimmed = predicate.trim();
    trimmed.eq_ignore_ascii_case("true")
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
    fn i1_singleton_union() {
        let c = clause("k", &[1, 2]);
        let ast = Comprehension::union(vec![c.clone()]);
        assert_eq!(apply(&ast), Some(c));
    }

    #[test]
    fn i2_singleton_cartesian() {
        let c = clause("k", &[1, 2]);
        let ast = Comprehension::cartesian(vec![c.clone()]);
        assert_eq!(apply(&ast), Some(c));
    }

    #[test]
    fn i3_singleton_zip() {
        use crate::comprehension::strategy::ZipMode;
        let c = clause("k", &[1, 2]);
        let ast = Comprehension::zip(vec![c.clone()], ZipMode::Strict);
        assert_eq!(apply(&ast), Some(c));
    }

    #[test]
    fn i4_trivially_true_filter() {
        let c = clause("k", &[1, 2]);
        let ast = Comprehension::filter(c.clone(), "true");
        assert_eq!(apply(&ast), Some(c));
    }

    #[test]
    fn i4_handles_whitespace_and_case() {
        let c = clause("k", &[1, 2]);
        let ast = Comprehension::filter(c.clone(), "  TRUE  ");
        assert_eq!(apply(&ast), Some(c));
    }

    #[test]
    fn i5_untruncated_lex_order() {
        let c = clause("k", &[1, 2]);
        let ast = Comprehension::order(c.clone(), StrategyName::Lex, None);
        assert_eq!(apply(&ast), Some(c));
    }

    #[test]
    fn i5_does_not_apply_to_truncated_lex() {
        let c = clause("k", &[1, 2]);
        let ast = Comprehension::order(c, StrategyName::Lex, Some(1));
        assert_eq!(apply(&ast), None);
    }

    #[test]
    fn does_not_apply_to_multi_child_combinator() {
        let ast = Comprehension::cartesian(vec![clause("k", &[1]), clause("limit", &[10])]);
        assert_eq!(apply(&ast), None);
    }
}
