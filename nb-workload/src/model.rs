// Copyright 2024-2026 Jonathan Shook
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
    /// Resolved workload parameters. These are available as bind points
    /// in op templates and as constants in GK bindings.
    /// Populated from: workload `params:` defaults, CLI overrides, env vars.
    #[serde(default)]
    pub params: HashMap<String, String>,
    /// Phase definitions. Each phase has its own config and either
    /// inline ops or tag filters to select from blocks/top-level ops.
    #[serde(default)]
    pub phases: HashMap<String, WorkloadPhase>,
    /// Phase names in YAML definition order. HashMap does not preserve
    /// insertion order, so this Vec tracks the order phases appeared in
    /// the workload YAML for deterministic default scenario execution.
    #[serde(default)]
    pub phase_order: Vec<String>,
    /// Param names declared in the workload YAML `params:` section.
    /// Used to detect unrecognized CLI params. Does not include
    /// ad-hoc CLI params.
    #[serde(default)]
    pub declared_params: Vec<String>,
}

/// A workload phase: runs as a separate Activity with its own
/// cycle count, concurrency, rate limit, and op selection.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkloadPhase {
    /// Number of stanzas for this phase. Each stanza executes all
    /// ops in sequence once. String type to support GK constant
    /// references like `"{train_count}"`. Default 1 (one stanza).
    #[serde(default)]
    pub cycles: Option<String>,
    /// Concurrency (async fibers). Default 1.
    #[serde(default)]
    pub concurrency: Option<usize>,
    /// Rate limit (ops/sec). Default unlimited.
    #[serde(default)]
    pub rate: Option<f64>,
    /// Adapter override for this phase.
    #[serde(default)]
    pub adapter: Option<String>,
    /// Error routing spec override.
    #[serde(default)]
    pub errors: Option<String>,
    /// Tag filter to select ops from blocks (e.g., `"block:schema"`).
    #[serde(default)]
    pub tags: Option<String>,
    /// Inline ops for this phase (parsed into `ParsedOp` list).
    #[serde(default)]
    pub ops: Vec<ParsedOp>,
    /// Phase template iteration: `"var in expr"`.
    /// The phase is instantiated once per element of the GK expression
    /// result (which must be a comma-separated string). Each instance
    /// has `{var}` available as a workload param in its ops and config.
    ///
    /// Example: `for_each: "profile in matching_profiles('{dataset}', '{prefix}')"`
    #[serde(default)]
    pub for_each: Option<String>,
}

/// A single step in a scenario.
///
/// For the new phased format (`scenarios.default: [schema, rampup, main]`),
/// `name` is the phase name and `command` is also set to the phase name.
/// For the legacy command-string format (`scenarios.default.schema: "run ..."`),
/// `name` is the step key and `command` is the full command string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioStep {
    pub name: String,
    pub command: String,
}

/// How bindings are defined for an op.
///
/// Two modes:
/// - **Map**: Legacy nosqlbench-style `name: "FuncA(); FuncB()"` chains.
///   Each binding is independent; inheritance merges at key level.
/// - **GkSource**: Native GK grammar as a multiline string. The entire
///   binding block is a single GK program with coordinates, named outputs,
///   and full DAG wiring. Replaces (not merges with) any inherited bindings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BindingsDef {
    /// Legacy nosqlbench-style: name → expression chain.
    Map(HashMap<String, String>),
    /// Native GK grammar source text.
    GkSource(String),
}

impl Default for BindingsDef {
    fn default() -> Self {
        BindingsDef::Map(HashMap::new())
    }
}

impl BindingsDef {
    /// Returns true if there are no bindings defined.
    pub fn is_empty(&self) -> bool {
        match self {
            BindingsDef::Map(m) => m.is_empty(),
            BindingsDef::GkSource(s) => s.trim().is_empty(),
        }
    }

    /// Get the map view (for legacy code). Returns empty map for GkSource.
    pub fn as_map(&self) -> &HashMap<String, String> {
        static EMPTY: std::sync::LazyLock<HashMap<String, String>> =
            std::sync::LazyLock::new(HashMap::new);
        match self {
            BindingsDef::Map(m) => m,
            BindingsDef::GkSource(_) => &EMPTY,
        }
    }

    /// Insert a key-value pair (legacy map mode). Converts GkSource to Map.
    pub fn insert(&mut self, key: String, value: String) {
        match self {
            BindingsDef::Map(m) => { m.insert(key, value); }
            _ => {
                let mut m = HashMap::new();
                m.insert(key, value);
                *self = BindingsDef::Map(m);
            }
        }
    }
}

/// A normalized op template — the canonical form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedOp {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The operation payload: field name → value.
    /// The statement is always under `"stmt"` after normalization.
    /// The original field name that carried the statement (e.g., `"raw"`,
    /// `"simple"`, `"prepared"`, `"stmt"`) is preserved in `stmt_type`
    /// for adapters that dispatch on execution mode.
    pub op: HashMap<String, serde_json::Value>,
    /// Binding definitions: either a name→expression map (legacy) or
    /// a GK grammar source string (native).
    #[serde(default, skip_serializing_if = "BindingsDef::is_empty")]
    pub bindings: BindingsDef,
    /// Configuration parameters.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, serde_json::Value>,
    /// Tags for filtering and metadata.
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Optional condition expression (from YAML `if:` field).
    /// Evaluated per cycle before the op executes. If the result
    /// is falsy (false, 0, empty string, None), the op is skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// Optional delay binding (from YAML `delay:` field).
    /// GK binding name producing per-cycle delay: u64 = nanoseconds,
    /// f64 = milliseconds. Applied before adapter execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay: Option<String>,
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
            bindings: BindingsDef::default(),
            params: HashMap::new(),
            tags: HashMap::new(),
            condition: None,
            delay: None,
        }
    }
}
