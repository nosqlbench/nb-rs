// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Normalized workload model: the canonical ParsedOp representation.
//!
//! All YAML shorthand forms normalize to this model. This is what
//! driver adapters consume.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// A complete workload definition after normalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workload {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub scenarios: HashMap<String, Vec<ScenarioStep>>,
    #[serde(default)]
    pub ops: Vec<ParsedOp>,
}

/// A single step in a scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioStep {
    pub name: String,
    pub command: String,
}

/// A normalized op template — the canonical form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedOp {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The operation payload: field name → value.
    pub op: HashMap<String, serde_json::Value>,
    /// Binding recipes: name → expression string.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub bindings: HashMap<String, String>,
    /// Configuration parameters.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, serde_json::Value>,
    /// Tags for filtering and metadata.
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl ParsedOp {
    /// Create a minimal ParsedOp with just a name and stmt.
    pub fn simple(name: &str, stmt: &str) -> Self {
        let mut op = HashMap::new();
        op.insert("stmt".to_string(), serde_json::Value::String(stmt.to_string()));
        Self {
            name: name.to_string(),
            description: None,
            op,
            bindings: HashMap::new(),
            params: HashMap::new(),
            tags: HashMap::new(),
        }
    }
}
