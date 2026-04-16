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
    /// Summary report configuration from the workload `summary:` section.
    /// Absent means no summary is printed. Present enables the summary
    /// and controls columns, row filters, and aggregate expressions.
    #[serde(default)]
    pub summary: Option<SummaryConfig>,
}

/// Parsed summary report configuration.
///
/// Controls which columns, rows, and aggregates appear in the
/// post-run summary table. Parsed from a semicolon-delimited DSL:
///
/// ```text
/// "recall; mean(recall) over profile~label; details=hide"
/// ```
///
/// Directives:
/// - Bare words (no `=` or `(`): gauge column filter patterns, comma-separated.
///   `"all"` shows every discovered gauge.
/// - `filter=<regex>`: row filter on activity labels.
/// - `<func>(<col>) over <key>~<pat>`: aggregate expression.
/// - `details=hide`: suppress individual data rows.
#[derive(Debug, Clone)]
pub struct SummaryConfig {
    /// Gauge column filter patterns (e.g., `["recall", "precision"]`).
    /// Empty means show all discovered gauges.
    pub columns: Vec<String>,
    /// Row filter regex patterns on activity labels.
    pub row_filters: Vec<String>,
    /// Aggregate expressions to compute after the data rows.
    pub aggregates: Vec<AggregateExpr>,
    /// Whether to show individual data rows (default `true`).
    pub show_details: bool,
    /// Raw source string for diagnostics and future GK template detection.
    pub raw: String,
}

/// An aggregate expression: `mean(recall) over profile~label`.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    /// Aggregation function.
    pub function: AggFunction,
    /// Column name pattern — only gauge columns containing this string
    /// are aggregated; others show `-` in the aggregate row.
    pub column_pattern: String,
    /// Label key to filter rows on (e.g., `"profile"`).
    pub label_key: String,
    /// Substring pattern matched against the label value (e.g., `"label"`).
    pub label_pattern: String,
}

/// Supported aggregation functions for summary report expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    Mean,
    Min,
    Max,
}

impl std::fmt::Display for AggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggFunction::Mean => write!(f, "mean"),
            AggFunction::Min => write!(f, "min"),
            AggFunction::Max => write!(f, "max"),
        }
    }
}

impl Serialize for SummaryConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for SummaryConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(SummaryConfig::parse(&raw))
    }
}

impl SummaryConfig {
    /// Parse a short-form summary DSL string.
    ///
    /// Semicolon-separated directives:
    /// - `"recall,precision"` — column filters
    /// - `"filter=search_post"` — row filter
    /// - `"mean(recall) over profile~label"` — aggregate expression
    /// - `"details=hide"` — hide individual data rows
    pub fn parse(raw: &str) -> Self {
        let mut columns = Vec::new();
        let mut row_filters = Vec::new();
        let mut aggregates = Vec::new();
        let mut show_details = true;

        for directive in raw.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            if directive == "details=hide" {
                show_details = false;
            } else if let Some(filter) = directive.strip_prefix("filter=") {
                row_filters.push(filter.trim().to_string());
            } else if let Some(agg) = Self::parse_aggregate(directive) {
                aggregates.push(agg);
            } else {
                // Column filter: comma-separated names
                for col in directive.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    if col != "all" {
                        columns.push(col.to_string());
                    }
                    // "all" = empty columns vec = show everything
                }
            }
        }

        SummaryConfig { columns, row_filters, aggregates, show_details, raw: raw.to_string() }
    }

    /// Try to parse an aggregate directive like `mean(recall) over profile~label`.
    fn parse_aggregate(s: &str) -> Option<AggregateExpr> {
        // Pattern: <func>(<col>) over <key>~<pat>
        let paren_open = s.find('(')?;
        let paren_close = s.find(')')?;
        if paren_close <= paren_open { return None; }

        let func_name = s[..paren_open].trim();
        let function = match func_name {
            "mean" => AggFunction::Mean,
            "min" => AggFunction::Min,
            "max" => AggFunction::Max,
            _ => return None,
        };

        let column_pattern = s[paren_open + 1..paren_close].trim().to_string();

        let after_paren = s[paren_close + 1..].trim();
        let over_rest = after_paren.strip_prefix("over")?.trim();
        let tilde = over_rest.find('~')?;

        let label_key = over_rest[..tilde].trim().to_string();
        let label_pattern = over_rest[tilde + 1..].trim().to_string();

        Some(AggregateExpr { function, column_pattern, label_key, label_pattern })
    }
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
    /// Concurrency (async fibers). String type to support GK constant
    /// or workload param references like `"{concurrency}"`. Default 1.
    #[serde(default)]
    pub concurrency: Option<String>,
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
    /// Loop scope mode for `for_each` phases.
    ///
    /// Controls how the loop context is seeded from the outer scope:
    /// - `clean` (default): snapshot of outer scope at loop entry
    /// - `inherit`: outer scope's live state (includes prior phase mutations)
    #[serde(default)]
    pub loop_scope: Option<String>,
    /// Iteration scope mode for `for_each` phases.
    ///
    /// Controls how each iteration is seeded from the loop scope:
    /// - `inherit` (default for for_each): each iteration starts from the loop
    ///   scope's current state. All loop-level variables are implicitly shared
    ///   with iterations, so iteration N+1 sees what N wrote.
    /// - `clean`: each iteration starts from the loop scope snapshot (isolated)
    #[serde(default)]
    pub iter_scope: Option<String>,
    /// Summary report configuration for this phase.
    /// Absent means the phase does not appear in the summary.
    /// Present means the phase contributes a row. The value
    /// configures what is shown (true = all available columns).
    #[serde(default)]
    pub summary: Option<serde_json::Value>,
}

/// A node in a scenario execution tree.
///
/// Scenarios are trees of phases and control flow constructs.
/// Nesting is supported to arbitrary depth. All nodes are
/// evaluated dynamically at runtime — no pre-flattening.
///
/// `cycle` is immutable — loop constructs declare their own
/// counter variables for iteration indices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScenarioNode {
    /// A single phase to execute.
    Phase(String),
    /// Iterate a pre-resolved list of values.
    ForEach { spec: String, children: Vec<ScenarioNode> },
    /// Execute children while condition is true (test after).
    DoWhile { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
    /// Execute children until condition becomes true (test after).
    DoUntil { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
}

/// Legacy alias.
pub type ScenarioStep = ScenarioNode;

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
