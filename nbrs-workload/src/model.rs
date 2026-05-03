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
    /// Workload-level GK bindings declared via the top-level
    /// `bindings:` block. These compile into the workload-root
    /// kernel directly, separate from per-op bindings — so
    /// declarations like `cursor row = range(0, 50)` are visible
    /// to scenario-level comprehensions (e.g.,
    /// `for xval in all(row)`) without needing to be threaded
    /// through phase-level ops.
    #[serde(default)]
    pub bindings: BindingsDef,
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
    /// Summary report definitions from the workload `summary:`
    /// section. Empty map = no summaries.
    ///
    /// The `summary:` field accepts two forms:
    ///
    /// - **String** (single anonymous summary): legacy form,
    ///   normalized at parse time to a single entry under the
    ///   key `"default"`.
    /// - **Mapping** of `name → spec` pairs: each named entry
    ///   becomes its own summary. The name drives both the
    ///   per-summary `summary.<name>` row written to
    ///   `session_metadata` in `metrics.db` (so the standalone
    ///   `nbrs --summary` command can regenerate every named
    ///   report from the db alone) and the output filename
    ///   (`<name>_summary.<format>`). A name that contains an
    ///   extension (e.g. `recallnmore.csv`) infers the
    ///   format from the suffix; otherwise the format
    ///   defaults to Markdown.
    ///
    /// The spec text inside each entry is the same DSL that
    /// the legacy single-string form accepts —
    /// see [`SummaryConfig::parse`].
    #[serde(default)]
    pub summaries: std::collections::HashMap<String, SummaryConfig>,
    /// Named plot specifications, parallel to `summaries`. Each
    /// value is the raw spec text — `nbrs plot` parses it the
    /// same way it parses `nbrs plot "<spec>"` from the CLI.
    /// Persisted into the metrics db at end-of-run as
    /// `plot.<name>` so post-hoc `nbrs plot --name <name>`
    /// replays it without the workload file.
    #[serde(default)]
    pub plots: std::collections::HashMap<String, String>,
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

/// An aggregate expression: either
/// `mean(recall) over profile~label` (single-key filter form,
/// emits one aggregate row) or
/// `mean(recall) over k,limit,optimize_for` (multi-key grouping
/// form, emits one aggregate row per distinct value-tuple).
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    /// Aggregation function.
    pub function: AggFunction,
    /// Column name pattern — only gauge columns containing this string
    /// are aggregated; others show `-` in the aggregate row.
    pub column_pattern: String,
    /// Label key to filter rows on (e.g., `"profile"`).
    /// Set in the single-key filter form. Empty when
    /// `group_by` is non-empty (multi-key grouping form).
    pub label_key: String,
    /// Substring pattern matched against the label value (e.g., `"label"`).
    /// Set in the single-key filter form. Empty when
    /// `group_by` is non-empty.
    pub label_pattern: String,
    /// Multi-key grouping form: when non-empty, rows are
    /// grouped by every distinct tuple of values across these
    /// label keys, and the aggregate emits one row per group.
    /// `label_key` / `label_pattern` are empty when this is set.
    pub group_by: Vec<String>,
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
                // Column filter: comma-separated names. Two
                // names are recognized as wildcards ("show
                // every gauge column"): the legacy `all`
                // keyword and `*` (the bare-`--summary` user
                // mental model — `nbrs --summary '*'` means
                // "default summary of all metrics").
                for col in directive.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    if col != "all" && col != "*" {
                        columns.push(col.to_string());
                    }
                    // `all` / `*` = empty columns vec = show
                    // every gauge with no filtering.
                }
            }
        }

        SummaryConfig { columns, row_filters, aggregates, show_details, raw: raw.to_string() }
    }

    /// Try to parse an aggregate directive in either form:
    /// - `<func>(<col>) over <key>~<pat>` (single-key filter)
    /// - `<func>(<col>) over <k1>,<k2>,…` (multi-key grouping)
    fn parse_aggregate(s: &str) -> Option<AggregateExpr> {
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

        // Single-key filter form: `<key>~<pat>` (note: `~` may
        // appear inside multi-key form too, e.g. nobody writes
        // `k,a~b` — use presence of `~` as the discriminator;
        // for clean multi-key, no `~` is present).
        if let Some(tilde) = over_rest.find('~') {
            let label_key = over_rest[..tilde].trim().to_string();
            let label_pattern = over_rest[tilde + 1..].trim().to_string();
            return Some(AggregateExpr {
                function, column_pattern,
                label_key, label_pattern,
                group_by: Vec::new(),
            });
        }

        // Multi-key grouping form: comma-separated label keys.
        let group_by: Vec<String> = over_rest.split(',')
            .map(str::trim).filter(|s| !s.is_empty())
            .map(|s| s.to_string()).collect();
        if group_by.is_empty() { return None; }
        Some(AggregateExpr {
            function, column_pattern,
            label_key: String::new(),
            label_pattern: String::new(),
            group_by,
        })
    }
}

#[cfg(test)]
mod summary_config_tests {
    use super::*;

    #[test]
    fn parses_multi_key_grouping() {
        let cfg = SummaryConfig::parse(
            "recall; mean(recall) over k,limit,optimize_for"
        );
        assert_eq!(cfg.aggregates.len(), 1, "got: {:?}", cfg.aggregates);
        let agg = &cfg.aggregates[0];
        assert_eq!(agg.group_by, vec!["k", "limit", "optimize_for"]);
        assert!(agg.label_key.is_empty());
    }

    #[test]
    fn parses_single_key_filter_form_unchanged() {
        let cfg = SummaryConfig::parse("mean(recall) over profile~label");
        assert_eq!(cfg.aggregates.len(), 1);
        let agg = &cfg.aggregates[0];
        assert!(agg.group_by.is_empty());
        assert_eq!(agg.label_key, "profile");
        assert_eq!(agg.label_pattern, "label");
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
    /// Checkpoint declaration: skip-on-resume eligibility plus
    /// optional sub-properties (hashing, verify op). `None` =
    /// no declaration = phase always re-runs on resume. See
    /// SRD-44 §"Eligibility — `checkpoint:` per-phase declaration".
    ///
    /// Parsed via [`Checkpoint`]'s custom deserialize from the
    /// three YAML forms (short string, disabled string/bool,
    /// full mapping).
    #[serde(default)]
    pub checkpoint: Option<Checkpoint>,
}

/// Per-phase checkpoint declaration. Three legal forms in YAML:
///
/// - `checkpoint: idempotent` — short form, equivalent to
///   `Checkpoint { idempotent: true, hashed: true, verify: None }`.
/// - `checkpoint: none` (or `false`, or `no`) — explicitly not
///   skip-eligible. Equivalent to no declaration; the phase
///   always re-runs on resume.
/// - `checkpoint: { idempotent: true, hashed: true, verify: ... }`
///   — full mapping form with sub-properties.
///
/// See SRD-44 §"Forms" and §"Sub-properties" for the full
/// contract. The `Default` is "skip-eligible with hashing on,
/// no verify" — what the short form `idempotent` produces.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Checkpoint {
    /// Marks this phase as skip-eligible on resume. `false`
    /// here is equivalent to `checkpoint: none` and means the
    /// phase always re-runs.
    pub idempotent: bool,
    /// When `true` (the default for any set checkpoint
    /// declaration), the resume planner additionally verifies
    /// that the freshly-pre-mapped phase's compiled program
    /// hash matches the saved one before honouring the saved
    /// status. `false` is the operator opt-out — "trust
    /// structural identity (yaml_path + coords) alone".
    pub hashed: bool,
    /// Optional verify op-template body. When present, the
    /// resume planner runs this op against the live system
    /// before classifying the phase as Skip; verify failure
    /// reclassifies to re-run with wholesale purge.
    /// Currently typed as a generic YAML value — the runtime
    /// re-parses it through the op-template grammar so the
    /// existing pipeline (SRD-32 wrappers, SRD-03 status-
    /// determination invariants) governs the verify
    /// execution.
    pub verify: Option<serde_json::Value>,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            idempotent: true,
            hashed: true,
            verify: None,
        }
    }
}

impl<'de> serde::Deserialize<'de> for Checkpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // The YAML accepts strings, booleans, and mappings —
        // each meaning a different declaration form. Serde's
        // visitor pattern lets us handle each input shape
        // directly without going through a typed-value
        // intermediate, which means this works equally well
        // for the YAML parser path and the JSON-staged path
        // (parse.rs walks `serde_json::Map` for phases).
        struct CheckpointVisitor;
        impl<'de> serde::de::Visitor<'de> for CheckpointVisitor {
            type Value = Checkpoint;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("checkpoint declaration: short string ('idempotent' / 'none' / etc), bool, or mapping with sub-properties")
            }

            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Checkpoint, E> {
                let trimmed = s.trim().to_ascii_lowercase();
                match trimmed.as_str() {
                    "idempotent" => Ok(Checkpoint::default()),
                    "none" | "no" | "false" | "off" | "" => Ok(Checkpoint {
                        idempotent: false,
                        hashed: true,
                        verify: None,
                    }),
                    other => Err(E::custom(format!(
                        "checkpoint: unknown short form '{other}'; \
                         expected 'idempotent', 'none', 'no', 'false', or a mapping"
                    ))),
                }
            }

            fn visit_string<E: serde::de::Error>(self, s: String) -> Result<Checkpoint, E> {
                self.visit_str(&s)
            }

            fn visit_bool<E: serde::de::Error>(self, b: bool) -> Result<Checkpoint, E> {
                if b {
                    Ok(Checkpoint::default())
                } else {
                    Ok(Checkpoint {
                        idempotent: false,
                        hashed: true,
                        verify: None,
                    })
                }
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Checkpoint, E> {
                // Bare `null` ≡ `none`.
                Ok(Checkpoint {
                    idempotent: false,
                    hashed: true,
                    verify: None,
                })
            }

            fn visit_map<M>(self, mut map: M) -> Result<Checkpoint, M::Error>
            where M: serde::de::MapAccess<'de>
            {
                let mut idempotent = true;
                let mut hashed = true;
                let mut verify: Option<serde_json::Value> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "idempotent" => idempotent = map.next_value::<bool>()?,
                        "hashed" => hashed = map.next_value::<bool>()?,
                        "verify" => verify = Some(map.next_value::<serde_json::Value>()?),
                        other => {
                            return Err(serde::de::Error::custom(format!(
                                "checkpoint: unknown key '{other}'; \
                                 expected 'idempotent', 'hashed', or 'verify'"
                            )));
                        }
                    }
                }
                Ok(Checkpoint { idempotent, hashed, verify })
            }
        }
        deserializer.deserialize_any(CheckpointVisitor)
    }
}

/// A node in a scenario execution tree.
///
/// Scenarios are trees of phases and control flow constructs.
/// Nesting is supported to arbitrary depth. All nodes are
/// evaluated dynamically at runtime — no pre-flattening.
///
/// `cycle` is immutable — loop constructs declare their own
/// counter variables for iteration indices.
///
/// All iteration shapes (`for_each` single-clause,
/// `for_combinations`, `for_each_union`) collapse into one
/// `Comprehension` variant carrying the canonical
/// `nbrs_variates::comprehension::Comprehension` AST. The
/// AST's mode (Cartesian vs Union) is the discriminator;
/// clause count distinguishes single-var iteration from
/// cross-product iteration. See SRD-18b §"Iteration as a
/// First-Class Concept".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScenarioNode {
    /// A single phase to execute.
    Phase(String),
    /// Iteration node — single-clause for_each, multi-clause
    /// for_combinations, or for_each_union all map here. The
    /// `Comprehension` AST captures the iteration shape; the
    /// runtime executes the cross-product of clauses for the
    /// Cartesian mode and the concatenation of sub-spaces'
    /// products for Union mode.
    ///
    /// YAML forms (all normalize to this variant):
    /// ```yaml
    /// # Single clause
    /// - for_each: "k in 10,100"
    ///
    /// # Multi-clause cross product
    /// - for_each: "profile in profiles, k in {k_values}"
    ///
    /// # Multi-clause cross product (map form)
    /// - for_combinations:
    ///     profile: "matching_profiles('{dataset}', '{prefix}')"
    ///     k: "{k_values}"
    ///
    /// # Union of sub-spaces
    /// - for_each:
    ///   - "k in 10, limit in 10,20,30"
    ///   - "k in 100, limit in 100,200,300"
    /// ```
    Comprehension {
        comprehension: nbrs_variates::comprehension::Comprehension,
        children: Vec<ScenarioNode>,
    },
    /// Execute children while condition is true (test after).
    DoWhile { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
    /// Execute children until condition becomes true (test after).
    DoUntil { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
    /// Logical inclusion of another scenario by name.
    ///
    /// Wherever this node appears (top-level of a scenario, inside
    /// a `phases:` list under a `for_each` / `for_combinations` /
    /// `for_each_union`, etc.), it expands to the children of the
    /// named scenario at execution time. The wrapper is preserved
    /// (not flattened) so the scope tree retains the include
    /// hierarchy and the renderer can show the operator which
    /// scenario each group of phases came from.
    ///
    /// Resolution happens once after parsing
    /// (see `crate::parse::resolve_scenario_includes`); cycles
    /// (`A` includes `B` includes `A`) are rejected with a clear
    /// error naming the cycle path.
    ///
    /// YAML form:
    /// ```yaml
    /// scenarios:
    ///   smoke:
    ///     - schema
    ///     - rampup
    ///   bench:
    ///     - scenario: smoke
    ///     - for_each: "k in 10,100"
    ///       phases:
    ///         - scenario: smoke
    ///         - search
    /// ```
    IncludedScenario { name: String, children: Vec<ScenarioNode> },
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
