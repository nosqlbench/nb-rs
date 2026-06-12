// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`parse_text`] — entry point for text-block consumers.
//!
//! Accepts a single string holding either YAML or JSON, picks
//! the right parser, deserializes into a
//! [`ComprehensionSpec`], and routes through
//! [`ComprehensionSpec::into_algebra`].
//!
//! Detection rule: a leading `{` (after trimming whitespace)
//! is JSON; anything else is YAML.  YAML is a strict superset
//! of JSON for the shapes we accept, so YAML can read JSON
//! input fine — but choosing the matching parser surfaces
//! sharper error messages for JSON authors.

use super::serde_form::{ComprehensionSpec, SpecConvertError};
use crate::iteration::comprehension::ast::Comprehension as AlgebraAst;

/// Parse a YAML or JSON text block describing a comprehension
/// into the algebra-layer AST.
///
/// Detection is leading-character based: `{...}` ⇒ JSON,
/// otherwise YAML.
pub fn parse_text(text: &str) -> Result<AlgebraAst, TextParseError> {
    let spec = deserialize_spec(text)?;
    let algebra = spec
        .into_algebra()
        .map_err(TextParseError::Convert)?;
    Ok(algebra)
}

fn deserialize_spec(text: &str) -> Result<ComprehensionSpec, TextParseError> {
    let trimmed = text.trim_start();
    if trimmed.starts_with('{') {
        serde_json::from_str(text).map_err(|e| TextParseError::Json(e.to_string()))
    } else {
        serde_yaml::from_str(text).map_err(|e| TextParseError::Yaml(e.to_string()))
    }
}

/// Errors produced by [`parse_text`].
#[derive(Debug, Clone)]
pub enum TextParseError {
    /// YAML deserialization failed.
    Yaml(String),
    /// JSON deserialization failed.
    Json(String),
    /// The deserialized [`ComprehensionSpec`] failed to
    /// convert into the algebra AST.
    Convert(SpecConvertError),
}

impl std::fmt::Display for TextParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TextParseError::Yaml(msg) => write!(f, "YAML parse error: {msg}"),
            TextParseError::Json(msg) => write!(f, "JSON parse error: {msg}"),
            TextParseError::Convert(e) => write!(f, "spec conversion error: {e}"),
        }
    }
}

impl std::error::Error for TextParseError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::strategy::StrategyName;

    #[test]
    fn yaml_text_block() {
        let text = r#"
            for: "k in 1..10, limit in [10, 100]"
            where: "{k} > 0"
            order: "halton/20"
        "#;
        let algebra = parse_text(text).unwrap();
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
    fn json_text_block() {
        let text = r#"
            {
                "for": "k in 1..10",
                "order": "lex/5"
            }
        "#;
        let algebra = parse_text(text).unwrap();
        match algebra {
            AlgebraAst::Order {
                strategy: StrategyName::Lex,
                truncation: Some(5),
                ..
            } => {}
            other => panic!("expected Order(Lex, Some(5)), got {other:?}"),
        }
    }

    #[test]
    fn json_union_form() {
        let text = r#"{
            "for": [
                ["k in 10",  "limit in [1, 2, 3]"],
                ["k in 100", "limit in [10, 20, 30]"]
            ]
        }"#;
        let algebra = parse_text(text).unwrap();
        match algebra {
            AlgebraAst::Union { children } => assert_eq!(children.len(), 2),
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn malformed_yaml_surfaces_error() {
        let text = r#"
            for: [unterminated
        "#;
        let err = parse_text(text).unwrap_err();
        assert!(matches!(err, TextParseError::Yaml(_)));
    }

    #[test]
    fn malformed_json_surfaces_error() {
        let text = r#"{ "for": "k in 1..10", oops }"#;
        let err = parse_text(text).unwrap_err();
        assert!(matches!(err, TextParseError::Json(_)));
    }

    #[test]
    fn convert_error_surfaces() {
        let text = r#"
            for: "k in 1..10"
            order: "garbage(((not valid"
        "#;
        let err = parse_text(text).unwrap_err();
        assert!(matches!(err, TextParseError::Convert(_)));
    }
}
