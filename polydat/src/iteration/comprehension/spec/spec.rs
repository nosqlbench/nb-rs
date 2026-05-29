// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`ComprehensionSpec`] — author-friendly serde-deserializable
//! form, plus its conversion into the algebra-layer AST.
//!
//! The friendly form has a **single `for` verb** (per user
//! direction) whose RHS shape determines the constructor:
//!
//! - `for: "k in 1..10"` — single clause / inline cartesian
//! - `for: ["k in 1..10", "limit in [1, 2, 3]"]` — list of
//!   clauses, cartesian (or union if names repeat)
//! - `for: [["k in 10", "limit in 1..5"], ["k in 100", "limit
//!   in 1..50"]]` — explicit union of cartesian sub-spaces
//!
//! With optional modifiers:
//!
//! - `where: "..."` — filter predicate
//! - `order: "halton/50"` — traversal order spec
//!
//! Conversion delegates to the existing legacy parsers + the
//! [`super::legacy_convert`] bridge. This module owns only the
//! serde surface and the shape-routing logic.

use serde::{Deserialize, Serialize};

use crate::iteration::comprehension::ast::Comprehension as AlgebraAst;
use crate::iteration::comprehension::ast_legacy::{
    Clause as LegacyClause, Comprehension as LegacyAst,
};
use crate::iteration::comprehension::parse::{
    comprehension_from_subspaces, parse_clause_list, parse_order_spec,
};

use super::legacy_convert::{legacy_to_algebra, ConvertError};

/// The friendly, serde-deserializable comprehension surface.
///
/// Field names match the YAML / JSON keys 1:1. The
/// [`r#for`](Self::r#for) field carries the only required input
/// — the clause specification — in any of the three accepted
/// shapes (see [`ForSpec`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComprehensionSpec {
    /// The clause specification.  See [`ForSpec`] for the
    /// accepted shapes.  `r#for` because `for` is a reserved
    /// keyword in Rust — serde renames it for YAML / JSON.
    #[serde(rename = "for")]
    pub r#for: ForSpec,
    /// Optional filter predicate (the `where` clause). String
    /// form, evaluated against bound coordinates at iteration
    /// time.
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub r#where: Option<String>,
    /// Optional traversal-order spec. See
    /// [`crate::iteration::comprehension::parse::parse_order_spec`] for
    /// the accepted syntax (`lex`, `halton/50`,
    /// `shells(origin=center, depth=3)`, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
}

/// The three accepted shapes of the `for` field.
///
/// Routes through the legacy parser:
///
/// - [`ForSpec::Inline`] → one call to `parse_clause_list`,
///   then the structural-detection rule (`comprehension_from_subspaces`).
/// - [`ForSpec::ClauseList`] → one `parse_clause_list` per
///   entry; each entry becomes its own sub-space (so name
///   repetition across entries triggers Union per the rule).
/// - [`ForSpec::UnionOfClauseLists`] → one `parse_clause_list`
///   per inner list; each inner list is one sub-space.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ForSpec {
    /// `for: "k in 1..10, limit in [1, 2, 3]"` — one inline
    /// string, possibly multi-clause.
    Inline(String),
    /// `for: ["k in 1..10", "limit in [1, 2, 3]"]` — each
    /// entry is one clause's text.
    ClauseList(Vec<String>),
    /// `for: [["k in 10", "limit in 1..5"], …]` — each inner
    /// list is one sub-space (cartesian over those clauses).
    UnionOfClauseLists(Vec<Vec<String>>),
}

/// Errors produced when converting a [`ComprehensionSpec`] to
/// the algebra-layer AST.
#[derive(Debug, Clone)]
pub enum SpecConvertError {
    /// `parse_clause_list` failed on one of the input strings.
    ParseClause { input: String, message: String },
    /// `parse_order_spec` failed on the `order` field.
    ParseOrder { input: String, message: String },
    /// Legacy AST failed self-validation.
    LegacyValidate { errors: Vec<String> },
    /// Conversion from legacy AST to algebra AST failed.
    Convert(ConvertError),
}

impl std::fmt::Display for SpecConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecConvertError::ParseClause { input, message } => {
                write!(f, "failed to parse clause(s) {input:?}: {message}")
            }
            SpecConvertError::ParseOrder { input, message } => {
                write!(f, "failed to parse order {input:?}: {message}")
            }
            SpecConvertError::LegacyValidate { errors } => {
                write!(f, "legacy AST validation failed: {}", errors.join("; "))
            }
            SpecConvertError::Convert(e) => {
                write!(f, "algebra conversion failed: {e}")
            }
        }
    }
}

impl std::error::Error for SpecConvertError {}

impl From<ConvertError> for SpecConvertError {
    fn from(e: ConvertError) -> Self {
        SpecConvertError::Convert(e)
    }
}

impl ComprehensionSpec {
    /// Convert this spec into the algebra-layer
    /// [`AlgebraAst`].  Routes the `for` shape through the
    /// legacy parser, builds a legacy AST (applying `where` /
    /// `order` modifiers), and runs the [`legacy_to_algebra`]
    /// bridge.
    pub fn into_algebra(self) -> Result<AlgebraAst, SpecConvertError> {
        let legacy = self.into_legacy()?;
        let algebra = legacy_to_algebra(&legacy)?;
        Ok(algebra)
    }

    /// Build the intermediate legacy AST.  Exposed for tests
    /// and for any consumer that still needs the legacy shape
    /// (e.g., during incremental cutover).
    pub fn into_legacy(self) -> Result<LegacyAst, SpecConvertError> {
        let subspaces = self.r#for.into_subspaces()?;
        let mut legacy = comprehension_from_subspaces(subspaces);
        if let Some(predicate) = self.r#where {
            legacy = legacy.with_filter(predicate);
        }
        if let Some(order_text) = self.order {
            let order =
                parse_order_spec(&order_text).map_err(|msg| SpecConvertError::ParseOrder {
                    input: order_text.clone(),
                    message: msg,
                })?;
            legacy = legacy.with_order(order);
        }
        legacy
            .validate()
            .map_err(|errs| SpecConvertError::LegacyValidate { errors: errs })?;
        Ok(legacy)
    }
}

impl ForSpec {
    /// Lower a `ForSpec` to the `Vec<Vec<Clause>>` shape that
    /// [`comprehension_from_subspaces`] expects.
    fn into_subspaces(self) -> Result<Vec<Vec<LegacyClause>>, SpecConvertError> {
        match self {
            ForSpec::Inline(text) => {
                // One inline string = one sub-space per clause
                // (matches `parse_comprehension_text`'s
                // convention so the union-detection rule sees
                // per-clause boundaries).
                let clauses = parse_clause_list(&text).map_err(|message| {
                    SpecConvertError::ParseClause {
                        input: text.clone(),
                        message,
                    }
                })?;
                Ok(clauses.into_iter().map(|c| vec![c]).collect())
            }
            ForSpec::ClauseList(entries) => {
                // Each entry is one clause's text — one
                // sub-space per entry (same convention).
                let mut subspaces = Vec::with_capacity(entries.len());
                for entry in entries {
                    let clauses = parse_clause_list(&entry).map_err(|message| {
                        SpecConvertError::ParseClause {
                            input: entry.clone(),
                            message,
                        }
                    })?;
                    for c in clauses {
                        subspaces.push(vec![c]);
                    }
                }
                Ok(subspaces)
            }
            ForSpec::UnionOfClauseLists(groups) => {
                // Each inner list is one sub-space (cartesian
                // over those clauses).
                let mut subspaces = Vec::with_capacity(groups.len());
                for group in groups {
                    let mut subspace_clauses = Vec::with_capacity(group.len());
                    for entry in group {
                        let clauses = parse_clause_list(&entry).map_err(|message| {
                            SpecConvertError::ParseClause {
                                input: entry.clone(),
                                message,
                            }
                        })?;
                        subspace_clauses.extend(clauses);
                    }
                    subspaces.push(subspace_clauses);
                }
                Ok(subspaces)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::strategy::StrategyName;

    #[test]
    fn inline_single_clause() {
        let yaml = r#"
            for: "k in 1..10"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().unwrap();
        // Single-clause cartesian collapses to the bare Clause.
        assert!(matches!(algebra, AlgebraAst::Clause { .. }));
    }

    #[test]
    fn inline_multi_clause_cartesian() {
        let yaml = r#"
            for: "k in 1..10, limit in [10, 100, 1000]"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().unwrap();
        match algebra {
            AlgebraAst::Cartesian { children } => assert_eq!(children.len(), 2),
            other => panic!("expected Cartesian, got {other:?}"),
        }
    }

    #[test]
    fn clause_list_form_cartesian() {
        let yaml = r#"
            for:
              - "k in 1..10"
              - "limit in [10, 100, 1000]"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().unwrap();
        match algebra {
            AlgebraAst::Cartesian { children } => assert_eq!(children.len(), 2),
            other => panic!("expected Cartesian, got {other:?}"),
        }
    }

    #[test]
    fn union_of_clause_lists() {
        let yaml = r#"
            for:
              - ["k in 10",  "limit in [1, 2, 3]"]
              - ["k in 100", "limit in [10, 20, 30]"]
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().unwrap();
        match algebra {
            AlgebraAst::Union { children } => assert_eq!(children.len(), 2),
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn where_clause_wraps_with_filter() {
        let yaml = r#"
            for: "k in 1..10"
            where: "{k} > 5"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().unwrap();
        assert!(matches!(algebra, AlgebraAst::Filter { .. }));
    }

    #[test]
    fn order_clause_wraps_with_order() {
        let yaml = r#"
            for: "k in 1..10, limit in 1..100"
            order: "halton/50"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().unwrap();
        match algebra {
            AlgebraAst::Order {
                strategy: StrategyName::Halton,
                truncation: Some(50),
                ..
            } => {}
            other => panic!("expected Order(Halton, Some(50)), got {other:?}"),
        }
    }

    #[test]
    fn where_and_order_compose() {
        let yaml = r#"
            for: "k in 1..10, limit in 1..100"
            where: "{k} * {limit} <= 100"
            order: "lex/20"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().unwrap();
        // Order wraps Filter wraps Cartesian
        match algebra {
            AlgebraAst::Order { child, strategy: StrategyName::Lex, truncation: Some(20), .. } => {
                assert!(matches!(*child, AlgebraAst::Filter { .. }));
            }
            other => panic!("expected Order(Lex, Some(20)) wrapping Filter, got {other:?}"),
        }
    }

    #[test]
    fn json_input_round_trips() {
        let json = r#"
            {
                "for": ["k in 1..10", "limit in [10, 100]"],
                "where": "{k} > 0",
                "order": "halton/20"
            }
        "#;
        let spec: ComprehensionSpec = serde_json::from_str(json).unwrap();
        let algebra = spec.into_algebra().unwrap();
        assert!(matches!(algebra, AlgebraAst::Order { .. }));
    }

    #[test]
    fn malformed_clause_surfaces_error() {
        let yaml = r#"
            for: "this is not a valid clause"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let err = spec.into_algebra().unwrap_err();
        assert!(matches!(err, SpecConvertError::ParseClause { .. }));
    }

    #[test]
    fn malformed_order_surfaces_error() {
        let yaml = r#"
            for: "k in 1..10"
            order: "(((not valid"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let err = spec.into_algebra().unwrap_err();
        assert!(matches!(err, SpecConvertError::ParseOrder { .. }));
    }

    #[test]
    fn unparseable_source_inside_for_falls_back_to_generator() {
        // parse_source now treats unrecognized text as a
        // Source::Generator that the runtime evaluator resolves
        // against the GK kernel chain — matching the legacy
        // grammar's permissive accept-anything behavior.
        let yaml = r#"
            for: "k in something-weird"
        "#;
        let spec: ComprehensionSpec = serde_yaml::from_str(yaml).unwrap();
        let algebra = spec.into_algebra().expect("permissive accept");
        match algebra {
            AlgebraAst::Clause { source, .. } => match source {
                crate::iteration::comprehension::source::Source::Generator { expr, .. } => {
                    assert_eq!(expr, "something-weird");
                }
                other => panic!("expected Generator, got {other:?}"),
            },
            other => panic!("expected Clause, got {other:?}"),
        }
    }
}
