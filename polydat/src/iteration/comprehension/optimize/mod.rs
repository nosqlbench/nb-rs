// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Post-parse optimizer — spec §10.
//!
//! Required pass upstream of compilation. Takes an AST and
//! produces a canonical, push-down form with these properties
//! (per spec §10.6):
//!
//! 1. **Semantic-preserving.** Output produces the same
//!    dispense sequence (per §9.2).
//! 2. **Idempotent.** `optimize(optimize(C)) == optimize(C)`.
//! 3. **Decidable termination.** Each rewrite strictly
//!    decreases a metadata-derived measure or leaves the AST
//!    unchanged.
//! 4. **Bounds-improving.** Peak memory never grows.
//! 5. **No rejections.** Validity is decided pre-optimizer.
//!
//! ## R-rule catalog
//!
//! Priority order: R0a → R0b → R1 → R2 → R3 → R4 → R5 → R6 →
//! R7 (spec §10.10.5).
//!
//! - **R0a — identity elimination** (I1–I5): singleton
//!   combinators, trivially-true filter, `order(Lex, None)`.
//! - **R0b — associativity flattening** (A1, A2): nested
//!   union / cartesian collapse to n-ary form.
//! - **R1 — `order(Lex)` → `ORDER_STREAMING`**: metadata-
//!   driven. The IR compiler (Phase 7) reads
//!   `metadata.materialization == Streaming` for
//!   `order(Lex, _)` and emits `ORDER_STREAMING`. Not an AST
//!   rewrite; recorded in the reducibility catalog as an
//!   IR-compilation eligibility.
//! - **R2 — `order(c, strategy, Some(n))` → `indexed_order`**:
//!   metadata-driven. Working set already shrunk via
//!   `strategy_working_set` in `metadata.rs`'s propagation
//!   rule. IR compiler emits `ORDER_MATERIALIZE` with the
//!   indexed variant.
//! - **R3 — `order(filter, Lex, None)` → `filter(order, Lex, None)`**:
//!   AST rewrite. Commute when un-truncated.
//! - **R4 — `filter(union(...), p)` → `union(filter(...))`**:
//!   AST rewrite. Distribute filter into each union child.
//! - **R5 — per-axis filter pushdown**: AST rewrite. Consults
//!   the predicate analyzer (§10.9) for factorization; when
//!   `factorization = PerAxis`, splits the filter into
//!   per-axis filters wrapping each cartesian child.
//! - **R6 — chained filter folding** (F1): AST rewrite.
//!   `filter(filter(c, p), q)` → `filter(c, p && q)`.
//! - **R7 — order chain folding** (O1): AST rewrite.
//!   `order(order(c, s1, None), s2, t)` → `order(c, s2, t)`.
//!
//! ## Module layout
//!
//! - [`finding`] — `ReducibilityFinding`, `Reduction`,
//!   `ComplexityDelta`.
//! - [`r0a_identity`] — I1–I5 elimination.
//! - [`r0b_flatten`] — A1, A2 flattening.
//! - [`r3_commute`] — Lex/filter commute.
//! - [`r4_distribute`] — filter over union.
//! - [`r5_factorize`] — per-axis filter pushdown.
//! - [`r6_filter_fold`] — chained filter folding.
//! - [`r7_order_fold`] — order chain folding.

use super::ast::Comprehension;
use super::predicate::CoordSet;
use crate::iteration::comprehension::metadata::Metadata;

pub mod finding;
pub mod r0a_identity;
pub mod r0b_flatten;
pub mod r3_commute;
pub mod r4_distribute;
pub mod r5_factorize;
pub mod r6_filter_fold;
pub mod r7_order_fold;

pub use finding::{ComplexityDelta, Ordering as ComplexityOrdering, Reduction, ReducibilityFinding, RuleId};

/// Top-level optimizer entry. Applies the R-rule catalog to a
/// fixed point and returns the optimized AST.
///
/// Per spec §10.6 the function is total — it never rejects.
/// Validation (V1–V9) must run before this; the optimizer
/// assumes its input is well-formed.
///
/// The optimizer is a thin loop over the reducibility analyzer
/// (§10.10): ask `analyze_reducibility` for a finding; apply
/// its witness if non-empty; repeat. The empty finding ends
/// the loop.
pub fn optimize(ast: Comprehension) -> Comprehension {
    let mut current = ast;
    let mut steps_remaining = max_steps(&current);
    while steps_remaining > 0 {
        match analyze_reducibility(&current) {
            ReducibilityFinding {
                reduction: Some(Reduction::Rewrite { witness, .. }),
                ..
            } => {
                current = witness;
            }
            ReducibilityFinding {
                reduction: Some(Reduction::Replace { with }),
                ..
            } => {
                current = with;
            }
            _ => break,
        }
        steps_remaining -= 1;
    }
    current
}

/// Reducibility analyzer entry — spec §10.10.
///
/// Walks the AST bottom-up trying each R-rule in priority
/// order. Returns the first non-empty finding; returns
/// the empty finding when no rule fires.
pub fn analyze_reducibility(ast: &Comprehension) -> ReducibilityFinding {
    // Bottom-up: try to rewrite each child first.
    // Rewriting a child returns a new parent that wraps the
    // rewritten child; subsequent rule attempts then see the
    // updated subtree on the next outer-loop iteration.
    if let Some(finding) = try_rewrite_child_first(ast) {
        return finding;
    }
    // No rewrite in a child — try rules at this node.
    try_rules_at_node(ast)
}

/// Attempt to rewrite a child; return a finding that wraps
/// the rewritten subtree in this node's variant.
fn try_rewrite_child_first(ast: &Comprehension) -> Option<ReducibilityFinding> {
    let children: Vec<Comprehension> = ast.children().cloned().collect();
    for (i, child) in children.iter().enumerate() {
        let child_finding = analyze_reducibility(child);
        let rewritten = match child_finding.reduction {
            Some(Reduction::Rewrite { witness, .. }) => witness,
            Some(Reduction::Replace { with }) => with,
            None => continue,
        };
        // Re-build this node with the rewritten child at position i.
        let new_ast = replace_child_at(ast, i, rewritten);
        return Some(ReducibilityFinding {
            reduction: Some(Reduction::Rewrite {
                rule: child_finding.rule.unwrap_or(RuleId::R0a),
                witness: new_ast,
            }),
            rule: child_finding.rule,
            improvement: child_finding.improvement,
        });
    }
    None
}

/// Try every R-rule at this node in priority order.
/// First fire wins.
fn try_rules_at_node(ast: &Comprehension) -> ReducibilityFinding {
    // R0a — identity elimination
    if let Some(witness) = r0a_identity::apply(ast) {
        return ReducibilityFinding {
            reduction: Some(Reduction::Rewrite { rule: RuleId::R0a, witness }),
            rule: Some(RuleId::R0a),
            improvement: ComplexityDelta::less_compute(),
        };
    }
    // R0b — associativity flattening
    if let Some(witness) = r0b_flatten::apply(ast) {
        return ReducibilityFinding {
            reduction: Some(Reduction::Rewrite { rule: RuleId::R0b, witness }),
            rule: Some(RuleId::R0b),
            improvement: ComplexityDelta::less_compute(),
        };
    }
    // R3 — Lex/filter commute
    if let Some(witness) = r3_commute::apply(ast) {
        return ReducibilityFinding {
            reduction: Some(Reduction::Rewrite { rule: RuleId::R3, witness }),
            rule: Some(RuleId::R3),
            improvement: ComplexityDelta::less_memory(),
        };
    }
    // R4 — filter distributes over union
    if let Some(witness) = r4_distribute::apply(ast) {
        return ReducibilityFinding {
            reduction: Some(Reduction::Rewrite { rule: RuleId::R4, witness }),
            rule: Some(RuleId::R4),
            improvement: ComplexityDelta::less_memory(),
        };
    }
    // R5 — per-axis filter pushdown
    if let Some(witness) = r5_factorize::apply(ast, &|p, c| {
        super::predicate::analyze(p, c)
    }) {
        return ReducibilityFinding {
            reduction: Some(Reduction::Rewrite { rule: RuleId::R5, witness }),
            rule: Some(RuleId::R5),
            improvement: ComplexityDelta::less_both(),
        };
    }
    // R6 — chained filter folding
    if let Some(witness) = r6_filter_fold::apply(ast) {
        return ReducibilityFinding {
            reduction: Some(Reduction::Rewrite { rule: RuleId::R6, witness }),
            rule: Some(RuleId::R6),
            improvement: ComplexityDelta::less_compute(),
        };
    }
    // R7 — order chain folding
    if let Some(witness) = r7_order_fold::apply(ast) {
        return ReducibilityFinding {
            reduction: Some(Reduction::Rewrite { rule: RuleId::R7, witness }),
            rule: Some(RuleId::R7),
            improvement: ComplexityDelta::less_both(),
        };
    }
    // No rule fires.
    ReducibilityFinding {
        reduction: None,
        rule: None,
        improvement: ComplexityDelta::equal(),
    }
}

/// Replace the i-th child of `ast` with `replacement`. Used by
/// the bottom-up walker to plumb child rewrites back into the
/// parent node.
fn replace_child_at(ast: &Comprehension, i: usize, replacement: Comprehension) -> Comprehension {
    match ast {
        Comprehension::Clause { .. } => unreachable!("clause has no children"),
        Comprehension::Cartesian { children } => {
            let mut new_children = children.clone();
            new_children[i] = replacement;
            Comprehension::Cartesian { children: new_children }
        }
        Comprehension::Zip { children, mode } => {
            let mut new_children = children.clone();
            new_children[i] = replacement;
            Comprehension::Zip { children: new_children, mode: *mode }
        }
        Comprehension::Union { children } => {
            let mut new_children = children.clone();
            new_children[i] = replacement;
            Comprehension::Union { children: new_children }
        }
        Comprehension::Filter { predicate, .. } => Comprehension::Filter {
            child: Box::new(replacement),
            predicate: predicate.clone(),
        },
        Comprehension::Order { strategy, truncation, .. } => Comprehension::Order {
            child: Box::new(replacement),
            strategy: *strategy,
            truncation: *truncation,
        },
    }
}

/// Bound on optimizer iterations. Per spec §10.6.3 the
/// optimizer halts because each rewrite strictly decreases a
/// well-founded measure. We bound iterations defensively as
/// `node_count^2` to guard against any bug in a rule that
/// would otherwise loop.
fn max_steps(ast: &Comprehension) -> usize {
    let n = ast.node_count();
    n.saturating_mul(n).saturating_add(16)
}

/// Convenience: build a `CoordSet` from a comprehension's
/// coordinate names and its computed metadata. R5 uses this
/// when invoking the predicate analyzer.
pub fn coord_set_for(ast: &Comprehension) -> CoordSet {
    let names = ast.coordinate_names();
    let metadata = ast.metadata();
    coord_set_from(&names, &metadata)
}

fn coord_set_from(names: &[String], metadata: &Metadata) -> CoordSet {
    CoordSet::from_metadata(names, metadata)
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
    fn optimize_well_formed_ast_does_not_panic() {
        let ast = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("limit", &[10, 20])]);
        let _ = optimize(ast);
    }

    #[test]
    fn optimize_singleton_cartesian_eliminates() {
        // R0a I2: singleton cartesian → its only child.
        let ast = Comprehension::cartesian(vec![clause("k", &[1, 2, 3])]);
        let optimized = optimize(ast);
        assert!(matches!(optimized, Comprehension::Clause { .. }));
    }

    #[test]
    fn optimize_lex_none_eliminates() {
        // R0a I5: order(c, Lex, None) → c.
        let inner = clause("k", &[1, 2, 3]);
        let ast = Comprehension::order(inner.clone(), StrategyName::Lex, None);
        let optimized = optimize(ast);
        assert_eq!(optimized, inner);
    }

    #[test]
    fn optimize_is_idempotent() {
        let ast = Comprehension::cartesian(vec![
            Comprehension::cartesian(vec![clause("a", &[1])]),
            clause("b", &[2]),
        ]);
        let once = optimize(ast.clone());
        let twice = optimize(once.clone());
        assert_eq!(once, twice);
    }
}
