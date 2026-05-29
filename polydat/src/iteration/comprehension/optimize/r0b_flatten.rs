// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! R0b — associativity flattening (spec §7.1 A1, A2).
//!
//! - A1: `union(union(a, b), c) → union(a, b, c)` (and the
//!   right-nested form).
//! - A2: `cartesian(cartesian(a, b), c) → cartesian(a, b, c)`.
//!
//! Zip is NOT associative (spec A3) — nested zips are a parse
//! error and never appear in valid input.
//!
//! Each flatten strictly decreases AST node count.

use crate::iteration::comprehension::ast::Comprehension;

pub fn apply(ast: &Comprehension) -> Option<Comprehension> {
    match ast {
        Comprehension::Union { children } => {
            if children.iter().any(|c| matches!(c, Comprehension::Union { .. })) {
                let mut flat = Vec::with_capacity(children.len());
                for child in children {
                    match child {
                        Comprehension::Union { children: inner } => {
                            flat.extend(inner.iter().cloned());
                        }
                        other => flat.push(other.clone()),
                    }
                }
                Some(Comprehension::Union { children: flat })
            } else {
                None
            }
        }
        Comprehension::Cartesian { children } => {
            if children.iter().any(|c| matches!(c, Comprehension::Cartesian { .. })) {
                let mut flat = Vec::with_capacity(children.len());
                for child in children {
                    match child {
                        Comprehension::Cartesian { children: inner } => {
                            flat.extend(inner.iter().cloned());
                        }
                        other => flat.push(other.clone()),
                    }
                }
                Some(Comprehension::Cartesian { children: flat })
            } else {
                None
            }
        }
        _ => None,
    }
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
    fn flatten_left_nested_union() {
        let inner = Comprehension::union(vec![clause("a", &[1]), clause("b", &[2])]);
        let ast = Comprehension::union(vec![inner, clause("c", &[3])]);
        let result = apply(&ast).unwrap();
        match result {
            Comprehension::Union { children } => {
                assert_eq!(children.len(), 3);
                // Each child is a clause; ordering preserved.
                assert!(matches!(&children[0], Comprehension::Clause { name, .. } if name == "a"));
                assert!(matches!(&children[2], Comprehension::Clause { name, .. } if name == "c"));
            }
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn flatten_right_nested_cartesian() {
        let inner = Comprehension::cartesian(vec![clause("b", &[2]), clause("c", &[3])]);
        let ast = Comprehension::cartesian(vec![clause("a", &[1]), inner]);
        let result = apply(&ast).unwrap();
        match result {
            Comprehension::Cartesian { children } => {
                assert_eq!(children.len(), 3);
            }
            other => panic!("expected Cartesian, got {other:?}"),
        }
    }

    #[test]
    fn does_not_flatten_unrelated_combinator() {
        // union containing a cartesian — different operators
        // don't flatten into each other.
        let inner = Comprehension::cartesian(vec![clause("b", &[1]), clause("c", &[2])]);
        let ast = Comprehension::union(vec![inner, clause("a", &[1])]);
        // Need V2 to pass — both children must have same shape.
        // Here `inner` has (b,c) shape and `a` clause has (a)
        // shape, so the union wouldn't validate. But for the
        // structural R0b test we only check that the rewrite
        // doesn't fire.
        assert!(apply(&ast).is_none());
    }

    #[test]
    fn does_not_flatten_already_flat() {
        let ast = Comprehension::cartesian(vec![clause("a", &[1]), clause("b", &[2])]);
        assert!(apply(&ast).is_none());
    }
}
