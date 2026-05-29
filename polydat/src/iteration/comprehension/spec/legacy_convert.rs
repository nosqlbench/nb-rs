// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Legacy → algebra AST converter.
//!
//! Reuses the existing legacy parsers
//! (`polydat::iteration::comprehension::parse::*`) for structural shape
//! recognition, then converts the legacy [`Comprehension`]
//! flat-struct AST to the new algebra-layer operator-tree
//! [`crate::iteration::comprehension::ast::Comprehension`].
//!
//! Source-string typing is handled by
//! [`super::source_parser::parse_source`] — the legacy AST
//! carries source expressions as raw strings; the algebra
//! layer requires typed [`crate::iteration::comprehension::source::Source`]
//! values at AST construction time so the validator and
//! metadata propagator can do their work statically.
//!
//! This converter is the "single bridge" the audit calls for:
//! every legacy AST funnels through here on the way to the
//! algebra layer. nb-workload's parser remains responsible for
//! turning YAML / text into legacy ASTs; polydat owns the
//! conversion onward.

use crate::iteration::comprehension::ast::Comprehension as AlgebraAst;
use crate::iteration::comprehension::strategy::{StrategyName, ZipMode as AlgebraZipMode};
use crate::iteration::comprehension::ast_legacy::{
    Clause as LegacyClause, ClauseSource as LegacyClauseSource,
    Comprehension as LegacyAst, ComprehensionMode as LegacyMode,
    Subspace as LegacySubspace, TraversalOrder as LegacyOrder,
    ZipMode as LegacyZipMode,
};

use super::source_parser::{parse_source, SourceParseError};

/// Errors produced when converting a legacy AST to algebra.
#[derive(Debug, Clone, PartialEq)]
pub enum ConvertError {
    /// A clause's source string didn't parse to a typed `Source`.
    SourceParse {
        clause_var: String,
        source: String,
        cause: SourceParseError,
    },
    /// An empty cartesian or empty union mode.
    EmptyComprehension,
    /// A union sub-space was empty.
    EmptyUnionSubspace,
    /// A parallel clause's vars and exprs had mismatched lengths
    /// (should be caught by the parser, but defensive here).
    ParallelArityMismatch { vars: usize, exprs: usize },
    /// Custom traversal order encountered — removed from the
    /// algebra per spec §3.6.
    CustomOrderingRemoved { function: String },
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertError::SourceParse { clause_var, source, cause } => write!(
                f,
                "clause {clause_var:?} source {source:?} failed to parse: {cause}"
            ),
            ConvertError::EmptyComprehension => f.write_str("comprehension has no clauses"),
            ConvertError::EmptyUnionSubspace => f.write_str("union has empty sub-space"),
            ConvertError::ParallelArityMismatch { vars, exprs } => write!(
                f,
                "parallel clause vars={vars} != exprs={exprs}"
            ),
            ConvertError::CustomOrderingRemoved { function } => write!(
                f,
                "custom ordering {function:?} is no longer supported (spec §3.6)"
            ),
        }
    }
}

impl std::error::Error for ConvertError {}

/// Convert a legacy [`LegacyAst`] to the algebra-layer
/// [`AlgebraAst`].
///
/// Handles:
/// - `mode` → cartesian / union
/// - `filter` → wrapping `Filter` node
/// - `order` → wrapping `Order` node (with `Custom` removed
///   per spec §3.6)
/// - `Clause::Single` source → typed `Source` via
///   [`parse_source`]
/// - `Clause::Parallel` source → algebra `Zip` of single-var
///   clauses (the algebra layer represents parallel iteration
///   as zip; the legacy parallel-clause shape is an inline
///   form of the same thing)
// The algebra → legacy bridge (`algebra_to_legacy_iter_inputs`,
// `algebra_union_subspaces`, `LegacyIterInputs` + their
// private helpers) was retired in 9c-4b phase 2 — the
// executor consumes the algebra runtime evaluator directly,
// and test fixtures walk the algebra AST natively via
// `Comprehension::coordinate_specs` etc. What remains here
// is the forward direction (`legacy_to_algebra`) used by
// `ComprehensionSpec::into_algebra` to convert parser output
// to algebra shape.

pub fn legacy_to_algebra(legacy: &LegacyAst) -> Result<AlgebraAst, ConvertError> {
    let body = match &legacy.mode {
        LegacyMode::Cartesian(clauses) => convert_cartesian(clauses)?,
        LegacyMode::Union(subspaces) => convert_union(subspaces)?,
    };

    let with_filter = if let Some(pred) = &legacy.filter {
        AlgebraAst::filter(body, pred.clone())
    } else {
        body
    };

    let with_order = if let Some(order) = &legacy.order {
        let (strategy, truncation) = convert_order(order)?;
        AlgebraAst::order(with_filter, strategy, truncation)
    } else {
        with_filter
    };

    Ok(with_order)
}

fn convert_cartesian(clauses: &[LegacyClause]) -> Result<AlgebraAst, ConvertError> {
    if clauses.is_empty() {
        return Err(ConvertError::EmptyComprehension);
    }
    let algebra_children: Vec<AlgebraAst> = clauses
        .iter()
        .map(convert_clause)
        .collect::<Result<_, _>>()?;
    if algebra_children.len() == 1 {
        // Single clause = the clause itself (R0a I2 would
        // eliminate the singleton cartesian anyway; produce
        // the canonical form upfront).
        Ok(algebra_children.into_iter().next().unwrap())
    } else {
        Ok(AlgebraAst::cartesian(algebra_children))
    }
}

fn convert_union(subspaces: &[LegacySubspace]) -> Result<AlgebraAst, ConvertError> {
    if subspaces.is_empty() {
        return Err(ConvertError::EmptyComprehension);
    }
    let algebra_children: Vec<AlgebraAst> = subspaces
        .iter()
        .map(|s| {
            if s.is_empty() {
                Err(ConvertError::EmptyUnionSubspace)
            } else {
                convert_cartesian(&s.clauses)
            }
        })
        .collect::<Result<_, _>>()?;
    if algebra_children.len() == 1 {
        Ok(algebra_children.into_iter().next().unwrap())
    } else {
        Ok(AlgebraAst::union(algebra_children))
    }
}

fn convert_clause(clause: &LegacyClause) -> Result<AlgebraAst, ConvertError> {
    match &clause.source {
        LegacyClauseSource::Single(source_str) => {
            let var = clause
                .single_var()
                .unwrap_or_else(|| clause.first_var())
                .to_string();
            let source = parse_source(source_str).map_err(|cause| {
                ConvertError::SourceParse {
                    clause_var: var.clone(),
                    source: source_str.clone(),
                    cause,
                }
            })?;
            Ok(AlgebraAst::clause(var, source))
        }
        LegacyClauseSource::Parallel { mode, exprs } => {
            if clause.vars.len() != exprs.len() {
                return Err(ConvertError::ParallelArityMismatch {
                    vars: clause.vars.len(),
                    exprs: exprs.len(),
                });
            }
            // Parallel iteration in legacy = zip in algebra.
            // Build a single-var clause per (var, expr) pair,
            // wrap in a Zip with the converted mode.
            let mut children = Vec::with_capacity(clause.vars.len());
            for (var, expr) in clause.vars.iter().zip(exprs.iter()) {
                let source = parse_source(expr).map_err(|cause| {
                    ConvertError::SourceParse {
                        clause_var: var.clone(),
                        source: expr.clone(),
                        cause,
                    }
                })?;
                children.push(AlgebraAst::clause(var.clone(), source));
            }
            let zip_mode = convert_zip_mode(*mode);
            Ok(AlgebraAst::zip(children, zip_mode))
        }
    }
}

fn convert_zip_mode(legacy: LegacyZipMode) -> AlgebraZipMode {
    match legacy {
        LegacyZipMode::Strict => AlgebraZipMode::Strict,
        LegacyZipMode::Truncate => AlgebraZipMode::Truncate,
        LegacyZipMode::Cycle => AlgebraZipMode::Cycle,
    }
}

/// Convert a legacy [`LegacyOrder`] into the algebra's
/// `(StrategyName, Option<u64>)` pair.
///
/// The legacy `Custom { function }` form is rejected — per
/// spec §3.6, custom orderings are no longer supported.
fn convert_order(order: &LegacyOrder) -> Result<(StrategyName, Option<u64>), ConvertError> {
    let pair = match order {
        LegacyOrder::Lex { count } => (StrategyName::Lex, count.map(|n| n as u64)),
        LegacyOrder::ReverseLex { count } => (StrategyName::ReverseLex, count.map(|n| n as u64)),
        LegacyOrder::Diagonal { count } => (StrategyName::Diagonal, count.map(|n| n as u64)),
        LegacyOrder::Antidiagonal { count } => (StrategyName::Antidiagonal, count.map(|n| n as u64)),
        LegacyOrder::Extrema { strata } => (StrategyName::Extrema, strata.map(|n| n as u64)),
        LegacyOrder::Shells { depth, .. } => (StrategyName::Shells, depth.map(|n| n as u64)),
        LegacyOrder::Halton { count } => (StrategyName::Halton, count.map(|n| n as u64)),
        LegacyOrder::Sobol { count } => (StrategyName::Sobol, count.map(|n| n as u64)),
        LegacyOrder::Lhs { count, .. } => (StrategyName::Lhs, count.map(|n| n as u64)),
        LegacyOrder::Custom { function } => {
            return Err(ConvertError::CustomOrderingRemoved {
                function: function.clone(),
            });
        }
    };
    Ok(pair)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::{LiteralValue, Source};

    fn legacy_clause(var: &str, source: &str) -> LegacyClause {
        LegacyClause::new(var, source)
    }

    #[test]
    fn cartesian_single_clause_collapses_to_clause() {
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![legacy_clause("k", "1..10")]),
            filter: None,
            order: None,
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        match algebra {
            AlgebraAst::Clause { name, source } => {
                assert_eq!(name, "k");
                assert!(matches!(source, Source::IntRange { lo: 1, hi: 10, step: 1 }));
            }
            other => panic!("expected Clause, got {other:?}"),
        }
    }

    #[test]
    fn multi_clause_cartesian_becomes_algebra_cartesian() {
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![
                legacy_clause("k", "1..10"),
                legacy_clause("limit", "[10, 100, 1000]"),
            ]),
            filter: None,
            order: None,
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        match algebra {
            AlgebraAst::Cartesian { children } => {
                assert_eq!(children.len(), 2);
                // First clause: int range
                match &children[0] {
                    AlgebraAst::Clause { name, source } => {
                        assert_eq!(name, "k");
                        assert!(matches!(
                            source,
                            Source::IntRange { lo: 1, hi: 10, step: 1 }
                        ));
                    }
                    other => panic!("expected Clause, got {other:?}"),
                }
                // Second clause: literal list
                match &children[1] {
                    AlgebraAst::Clause { name, source } => {
                        assert_eq!(name, "limit");
                        match source {
                            Source::Literal { values } => {
                                assert_eq!(values.len(), 3);
                                assert_eq!(values[0], LiteralValue::Int(10));
                            }
                            other => panic!("expected Literal, got {other:?}"),
                        }
                    }
                    other => panic!("expected Clause, got {other:?}"),
                }
            }
            other => panic!("expected Cartesian, got {other:?}"),
        }
    }

    #[test]
    fn filter_wraps_body() {
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![legacy_clause("k", "1..10")]),
            filter: Some("{k} > 5".to_string()),
            order: None,
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        assert!(matches!(algebra, AlgebraAst::Filter { .. }));
    }

    #[test]
    fn order_lex_with_count_round_trips() {
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![legacy_clause("k", "1..10")]),
            filter: None,
            order: Some(LegacyOrder::Lex { count: Some(5) }),
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        match algebra {
            AlgebraAst::Order { strategy: StrategyName::Lex, truncation: Some(5), .. } => {}
            other => panic!("expected Order(Lex, Some(5)), got {other:?}"),
        }
    }

    #[test]
    fn order_halton_with_count() {
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![
                legacy_clause("k", "1..10"),
                legacy_clause("limit", "1..100"),
            ]),
            filter: None,
            order: Some(LegacyOrder::Halton { count: Some(20) }),
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        match algebra {
            AlgebraAst::Order {
                strategy: StrategyName::Halton,
                truncation: Some(20),
                ..
            } => {}
            other => panic!("expected Order(Halton, Some(20)), got {other:?}"),
        }
    }

    #[test]
    fn custom_ordering_rejected() {
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![legacy_clause("k", "1..10")]),
            filter: None,
            order: Some(LegacyOrder::Custom {
                function: "my_fn".to_string(),
            }),
        };
        let err = legacy_to_algebra(&legacy).unwrap_err();
        assert!(matches!(err, ConvertError::CustomOrderingRemoved { .. }));
    }

    #[test]
    fn union_of_subspaces() {
        let legacy = LegacyAst {
            mode: LegacyMode::Union(vec![
                LegacySubspace::new(vec![
                    legacy_clause("k", "10"),
                    legacy_clause("limit", "[1, 2, 3]"),
                ]),
                LegacySubspace::new(vec![
                    legacy_clause("k", "100"),
                    legacy_clause("limit", "[10, 20, 30]"),
                ]),
            ]),
            filter: None,
            order: None,
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        match algebra {
            AlgebraAst::Union { children } => assert_eq!(children.len(), 2),
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn parallel_clause_becomes_zip() {
        let parallel = LegacyClause::parallel(["x", "y"], ["1..3", "10..30"]);
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![parallel]),
            filter: None,
            order: None,
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        // After the singleton-cartesian elide, the Zip
        // surfaces at the top level.
        match algebra {
            AlgebraAst::Zip { children, mode: AlgebraZipMode::Strict } => {
                assert_eq!(children.len(), 2);
            }
            other => panic!("expected Zip, got {other:?}"),
        }
    }

    #[test]
    fn unparseable_source_falls_back_to_generator() {
        // parse_source now treats unrecognized text as a
        // Generator expression (runtime evaluates). So
        // "totally nonsense" round-trips through algebra as
        // a Source::Generator. No conversion error.
        let legacy = LegacyAst {
            mode: LegacyMode::Cartesian(vec![legacy_clause("k", "totally nonsense")]),
            filter: None,
            order: None,
        };
        let algebra = legacy_to_algebra(&legacy).unwrap();
        match algebra {
            AlgebraAst::Clause { source, .. } => match source {
                crate::iteration::comprehension::source::Source::Generator { expr, .. } => {
                    assert_eq!(expr, "totally nonsense");
                }
                other => panic!("expected Generator, got {other:?}"),
            },
            other => panic!("expected Clause, got {other:?}"),
        }
    }

    // (algebra → legacy back-converter tests retired with the
    // bridge in 9c-4b phase 2. The forward direction
    // (`legacy_to_algebra`) tests above remain.)
}
