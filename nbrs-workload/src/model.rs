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
    /// Unified report block (SRD-46): plots and tables under one
    /// schema with figure enumeration, palette/style cascade, and
    /// declaration-order rendering. Replaces the separate
    /// `plot:` and `summary:` blocks (gone, no shim).
    #[serde(default)]
    pub report: crate::report::Report,
    /// Non-fatal warnings emitted by the report-block parser
    /// (SRD-46). Empty in normal mode; strict mode (SRD-15)
    /// promotes them to errors. Plumbed up so the runner /
    /// validator decide how to surface them.
    #[serde(default, skip_serializing)]
    pub report_warnings: Vec<String>,
    /// Workload-wide default for the per-phase
    /// [`WorkloadPhase::status_metrics`] field. Phases that don't
    /// declare their own `status_metrics:` inherit this list.
    /// Supports glob-style patterns (`recall*`, `latency*`) so a
    /// single doc-root entry can emphasize a metric family across
    /// every phase that produces it.
    ///
    /// Empty (default) → no metrics tail anywhere; per-phase
    /// declarations are still honoured.
    #[serde(default)]
    pub status_metrics: Vec<String>,
    /// Resolved `readouts:` block bindings (SRD-63 §5).
    /// One entry per event slot the workload bound; the
    /// runtime binder reads this map and dispatches at fire
    /// time. Empty (default) → all slots fall back to the
    /// hard-coded built-ins activity.rs uses today.
    ///
    /// Each value is a list of literal body strings — one
    /// per readout invocation in the slot. The body strings
    /// haven't been parsed against the readout grammar yet;
    /// that happens at activity-init time once the workload
    /// kernel is in place. Push 3 ships the data shape
    /// only; Push 4 wires resolved layered overrides
    /// (CLI / extends).
    #[serde(default)]
    pub readouts: ReadoutsBindings,
    /// SRD-32a Push 3 — workload-root wrapper composition
    /// override. When present, every op template in this
    /// workload uses this innermost-to-outermost order
    /// instead of the runtime's default tiebreaker order.
    /// Per-op `wrappers: { order: ... }` shadows this entry
    /// entirely (no cascading merge).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrappers: Option<WrappersConfig>,
}

/// SRD-32a Push 3 — wrapper-composition override block.
/// Carries an explicit innermost-to-outermost order list
/// that the resolver uses in place of its built-in
/// default-order tiebreaker. The list must be a permutation
/// of the wrappers the op actually triggers (after
/// transitive activation); listing a non-triggered wrapper
/// or omitting a triggered one is a hard error per SRD-32a
/// §"Workload-level override".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WrappersConfig {
    /// Innermost-to-outermost wrapper-name list. Empty list
    /// is treated as "no override" (equivalent to leaving
    /// `wrappers:` off the workload).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order: Vec<String>,
}

/// Per-event-slot list of readout body strings declared in
/// the workload's `readouts:` block. See SRD-63 §5.0 for
/// the three legal forms.
///
/// The lower-case slot keys here mirror the
/// [`Event::slot_name`] return values
/// (`on_phase_end`, `on_update`, …) so workload yaml
/// uses the same vocabulary the design doc uses.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReadoutsBindings {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_session_start: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_session_end: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_phase_start: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_phase_end: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_each_start: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_each_end: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_scope_start: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_scope_end: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_update: Vec<String>,
}

impl ReadoutsBindings {
    /// True when no slot has any binding. Workloads in
    /// this state fall through to the built-in defaults
    /// activity.rs uses today.
    pub fn is_empty(&self) -> bool {
        self.on_session_start.is_empty()
            && self.on_session_end.is_empty()
            && self.on_phase_start.is_empty()
            && self.on_phase_end.is_empty()
            && self.on_each_start.is_empty()
            && self.on_each_end.is_empty()
            && self.on_scope_start.is_empty()
            && self.on_scope_end.is_empty()
            && self.on_update.is_empty()
    }

    /// Look up a slot's body list by its `slot_name` (e.g.
    /// `"on_update"`). Returns an empty slice when the
    /// slot has no bindings.
    pub fn get(&self, slot_name: &str) -> &[String] {
        match slot_name {
            "on_session_start" => &self.on_session_start,
            "on_session_end"   => &self.on_session_end,
            "on_phase_start"   => &self.on_phase_start,
            "on_phase_end"     => &self.on_phase_end,
            "on_each_start"    => &self.on_each_start,
            "on_each_end"      => &self.on_each_end,
            "on_scope_start"   => &self.on_scope_start,
            "on_scope_end"     => &self.on_scope_end,
            "on_update"        => &self.on_update,
            _ => &[],
        }
    }
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
    /// SRD-46 v2: native MetricsQL columns. When non-empty,
    /// `summary_command` routes through the metricsql renderer
    /// instead of the legacy SQL builder. Each entry is
    /// `(column_name, metricsql_expression)`. Anonymous
    /// single-column form (`query: <expr>`) lands as
    /// `("value", expr)`.
    pub metricsql_columns: Vec<(String, String)>,
    /// Label key the metricsql results are grouped on (becomes
    /// the leftmost column of the rendered table). When empty
    /// AND `metricsql_columns` is non-empty, the renderer falls
    /// back to a single un-grouped row showing the average value
    /// across all returned series.
    ///
    /// Multi-key form: `group_by: k, r, optimize_for` produces
    /// one table row per distinct tuple — the same series
    /// breakdown the matching plot draws.
    pub group_by: Vec<String>,
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
        let mut metricsql_columns: Vec<(String, String)> = Vec::new();
        let mut group_by: Vec<String> = Vec::new();

        // Strip `#` line comments before parsing (SRD-46:
        // report/plot/table bodies all support `#` comments).
        let cleaned = strip_hash_line_comments(raw);

        // SRD-46 v2 line-pass: native-form directives
        // (`query: <expr>`, `query <col>: <expr>`, `group_by: <key>`).
        // Pulled out before the legacy `;`-separator pass so a
        // metricsql expression containing `;` (rare but legal)
        // doesn't get sliced apart, and so legacy and native
        // forms can coexist during migration.
        let mut residual_lines: Vec<String> = Vec::new();
        for line in cleaned.lines().map(str::trim).filter(|s| !s.is_empty()) {
            if let Some(rest) = line.strip_prefix("group_by:").map(str::trim)
                .or_else(|| line.strip_prefix("group-by:").map(str::trim))
            {
                group_by = rest.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                continue;
            }
            // Three surface forms for query columns:
            //   query <col>: <expr>     — legacy named (space sep)
            //   query: <col>: <expr>    — canonical named (uniform `name: value`)
            //   query: <expr>           — single anonymous column
            // The canonical form factors out as: after `query:`, if the
            // remainder begins with a bare-identifier followed by `:`,
            // the leading identifier is the column name; otherwise the
            // whole remainder is the anonymous expression. Identifiers
            // here are `[A-Za-z_][A-Za-z0-9_-]*` — anything containing
            // whitespace, parens, braces, or operators forces the
            // anonymous interpretation, which is what we want for
            // metricsql expressions whose label-literal `:` shows up
            // before a function-call `(`.
            if let Some(rest) = line.strip_prefix("query") {
                let rest = rest.trim_start();
                if let Some(after_colon) = rest.strip_prefix(':') {
                    let after_colon = after_colon.trim_start();
                    if let Some((col, expr)) = split_named_query(after_colon) {
                        metricsql_columns.push((col, expr));
                    } else {
                        metricsql_columns.push((
                            "value".to_string(),
                            after_colon.trim().to_string(),
                        ));
                    }
                    continue;
                }
                // Legacy `query <col>: <expr>` form — the next
                // colon terminates the column name.
                if let Some(colon_idx) = rest.find(':') {
                    let col = rest[..colon_idx].trim().to_string();
                    let expr = rest[colon_idx + 1..].trim().to_string();
                    if !col.is_empty() && !expr.is_empty() {
                        metricsql_columns.push((col, expr));
                        continue;
                    }
                }
            }
            residual_lines.push(line.to_string());
        }
        let cleaned: String = residual_lines.join(";");

        for directive in cleaned.split(';').map(str::trim).filter(|s| !s.is_empty()) {
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

        SummaryConfig {
            columns, row_filters, aggregates, show_details,
            raw: raw.to_string(),
            metricsql_columns,
            group_by,
        }
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

/// Strip `#` line comments from a multi-line spec body. A `#`
/// Split a `query:` payload into `(column_name, expression)` when
/// the payload's leading token is a bare identifier followed by
/// `:`. Returns `None` for the anonymous-column form (the whole
/// payload is the expression).
///
/// An identifier here is `[A-Za-z_][A-Za-z0-9_-]*`. The lookup
/// fails as soon as any character outside that class appears
/// before the first `:`, which is what guards a metricsql label
/// expression like `recall_mean{k="10"}` from being mistaken for
/// a column name (the `{` ends the candidate identifier before
/// the eventual `:` inside `k="10"` is reached).
fn split_named_query(text: &str) -> Option<(String, String)> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let is_first = i == 0;
        let ok = if is_first {
            b.is_ascii_alphabetic() || b == b'_'
        } else {
            b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
        };
        if !ok { break; }
        i += 1;
    }
    if i == 0 {
        return None;
    }
    // Optional whitespace, then `:` to separate name from value.
    let mut j = i;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b':' {
        return None;
    }
    let name = text[..i].to_string();
    let expr = text[j + 1..].trim().to_string();
    if name.is_empty() || expr.is_empty() {
        return None;
    }
    Some((name, expr))
}

/// starts a comment only when it's at line-start or preceded by
/// whitespace — so hex colors (`#117733`) and JSON sub-blocks
/// (`{"color": "#fff"}`) survive. Quoted strings are honoured.
fn strip_hash_line_comments(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for line in s.split_inclusive('\n') {
        let mut quote: Option<char> = None;
        let mut prev_ws = true;
        let mut cut: Option<usize> = None;
        for (i, ch) in line.char_indices() {
            match quote {
                Some(q) if ch == q => { quote = None; prev_ws = false; }
                Some(_) => { prev_ws = false; }
                None => match ch {
                    '"' | '\'' => { quote = Some(ch); prev_ws = false; }
                    '#' if prev_ws => { cut = Some(i); break; }
                    c if c.is_whitespace() => { prev_ws = true; }
                    _ => { prev_ws = false; }
                }
            }
        }
        match cut {
            Some(idx) => {
                out.push_str(&line[..idx]);
                if line.ends_with('\n') { out.push('\n'); }
            }
            None => out.push_str(line),
        }
    }
    out
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
    /// Names of metrics to surface on the inline progress line
    /// and the per-phase ✓ DONE summary. Empty (default) → no
    /// extra metrics shown; the status line carries only the
    /// universal counters (pct, throughput, ok-rate, errors,
    /// retries, concurrency, duration).
    ///
    /// Each name is matched against the live relevancy
    /// aggregates (`recall_at_10`, `precision_at_10`, …) by exact
    /// equality. Workloads that compute custom relevancy metrics
    /// list the names they want emphasized; nothing is presumed
    /// to be present.
    ///
    /// Example:
    /// ```yaml
    /// phases:
    ///   ann_query:
    ///     status_metrics: [recall_at_10]
    /// ```
    #[serde(default)]
    pub status_metrics: Vec<String>,
    /// Phase-level GK `bindings:` block (SRD-13c, SRD-13d).
    /// Captured on the phase AST so the scope-tree pre-walk
    /// (SRD-13d §3) can classify phase-level GK content via
    /// [`crate::gk_matter::HasGkMatter`] and so the runtime
    /// can compose a phase kernel layered between the
    /// workload kernel and any op-template kernels.
    ///
    /// Today the parser ALSO merges this block into per-op
    /// bindings (legacy `parse.rs::parse_phases` behaviour) so
    /// the existing runtime keeps working unchanged. Once
    /// SRD-13d phases 3–9 land (per-template kernels with
    /// proper `bind_outer_scope` chaining through the phase
    /// kernel), the per-op merge is removed and ops resolve
    /// phase bindings via the GK scope chain.
    #[serde(default, skip_serializing_if = "BindingsDef::is_empty")]
    pub bindings: BindingsDef,
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
    /// Scenario-tree-level GK bindings block — the canonical way
    /// to introduce a scope-local layer of bound names anywhere
    /// in the scenario tree.
    ///
    /// `source` is GK matter text exactly as a phase-level
    /// `bindings:` block would contain. Anything the GK grammar
    /// accepts is valid: `final NAME := <literal>`, derived
    /// bindings (`scaled := mul(workload_limit, 2)`), shared
    /// cells, init bindings, etc. Workload-param `{name}` and
    /// string-interpolation references resolve through the
    /// scope chain at kernel build time — no separate
    /// preprocessing pass.
    ///
    /// `Bindings` is also the canonical lowered form of `set:`.
    /// The parser recognizes `set: { name: value, ... }` as
    /// syntactic sugar and emits a `Bindings` node whose
    /// `source` is `final <name> := <gk-literal>\n` (one line
    /// per pair, declaration order preserved). So
    ///
    /// ```yaml
    /// - set: { mode: verbose }
    ///   phases:
    ///     - announce
    /// ```
    ///
    /// is semantically identical to
    ///
    /// ```yaml
    /// - bindings: |
    ///     final mode := "verbose"
    ///   phases:
    ///     - announce
    /// ```
    ///
    /// Both produce one `Bindings` node. Authors keep the
    /// short `set:` form for the common override case; the
    /// long form unlocks the full GK grammar (derived
    /// bindings, expressions referencing other in-scope
    /// names, etc.) without any new variant.
    ///
    /// Lexical-shadow semantics are uniform with phase-level
    /// `bindings:`: a `final NAME := <value>` shadows any
    /// upstream binding for `NAME` over this node's `children`
    /// subtree. The shadow is enforced via the local-final
    /// transit-suppression rule in `materialize_wiring_from_outer`
    /// — the same mechanism every other scope uses.
    ///
    /// Composition example (two siblings, each defining its
    /// own value for the same name; the included subtree is
    /// physically cloned per include site so encapsulation is
    /// per-instance):
    ///
    /// ```yaml
    /// scenarios:
    ///   fanout:
    ///     - set: { mode: verbose }
    ///       phases:
    ///         - scenario: load_test
    ///     - set: { mode: quiet }
    ///       phases:
    ///         - scenario: load_test
    /// ```
    Bindings { source: String, children: Vec<ScenarioNode> },
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
    /// SRD-40b synthetic-metric declarations. Each entry
    /// publishes one metric family per cycle, valued by a GK
    /// expression evaluated in the op's bound scope. Empty
    /// when absent. Map key is the metric name and the
    /// **default family name**; `MetricSpec::family` overrides
    /// it when set. See SRD-40b §1 for the schema, §2 for
    /// sugared forms (bare-string / list with wire-expression
    /// entries).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metrics: HashMap<String, MetricSpec>,
    /// SRD-66 result-bindings. Vari-structured: string is
    /// GK source, list is a sequence of fragments, map is
    /// named-key short-forms with a composite-map output.
    /// `None` ⇒ no result wires; the result wrapper is a
    /// no-op for this op.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ResultSpec>,
    /// SRD-32a Push 3 — per-op wrapper-composition override.
    /// When present, this op uses the named order instead of
    /// the workload-root or runtime-default tiebreaker order.
    /// Shadows the workload-root `wrappers:` block entirely
    /// (no merge).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrappers: Option<WrappersConfig>,
    /// Capture-point specs extracted at parse time from any
    /// string-valued entry in `op`. Each spec names a column the
    /// result body carries and the wire it should be written to
    /// via `ctx.wires.write` at cycle time. The `slurp` flag
    /// selects between single-row (`[name]`) and all-rows
    /// (`[@name]`) extraction.
    ///
    /// The parser strips the bracket syntax from the source op
    /// fields after harvesting the spec, so adapters consume
    /// clean text (e.g. `SELECT [key] FROM ...` becomes
    /// `SELECT key FROM ...`). Downstream wrappers read this
    /// list directly — no re-parsing of the op's text fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub captures: Vec<crate::bindpoints::CapturePoint>,
}

/// SRD-40b §1 schema for one synthetic-metric declaration on
/// an op template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSpec {
    /// Required. A GK expression evaluated in the op's bound
    /// scope. A bare binding name is the canonical form when
    /// the formula belongs in a `bindings:` block; any GK
    /// expression that produces a numeric result is also
    /// valid. See SRD-40b §4.
    pub value: String,
    /// Optional override of the family name. Defaults to the
    /// map key on `ParsedOp.metrics`. SRD-40b §1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// Optional metric type. Defaults to `Gauge` per SRD-40b §1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<MetricKind>,
    /// Optional OpenMetrics unit suffix (`ms`, `bytes`, …).
    /// When set, lands in BOTH the family-name suffix and the
    /// `metric_family.unit` column per SRD-40a §4.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// Optional generation-time numeric sanitiser using
    /// Excel-style hash patterns (`#.##`, `0.000`, etc.).
    /// Translated at registration time into a round op that
    /// runs before the value is recorded on the instrument;
    /// storage holds the sanitised number. SRD-40b §1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// Metric type discriminator. SRD-40b §1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    /// Current-state observation; per-cycle `set(value)`.
    /// Default per SRD-40b §1.
    Gauge,
    /// Distribution sample; per-cycle `record(value)`.
    Histogram,
    /// Monotonic running total; per-cycle `inc_by(value)`.
    Counter,
}

impl Default for MetricKind {
    /// SRD-40b §1: gauge is the default — synthetic values are
    /// most often current-state observations.
    fn default() -> Self { MetricKind::Gauge }
}

/// SRD-66 result-bindings declaration. Vari-structured to
/// match the three YAML shapes the user can write:
///
/// - **String**: a multi-line GK source block. Each
///   `<name> := <expr>` assignment declares one result wire.
/// - **List**: a sequence of nested `ResultSpec`s; each
///   element processes in order and contributes its
///   declarations.
/// - **Map**: named-key short-forms (`count`, `ok`,
///   path-expr, or any other string treated as a GK
///   expression). Map shape additionally produces a
///   composite-map wire (deferred — see Push 2 follow-ups).
///
/// SRD-40b §5.1's mapping form is preserved as the map
/// shape with two refinements: any non-built-in non-path
/// string is a GK expression (no `(`-detector magic), and
/// the composite-map output is added.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResultSpec {
    /// GK source block — one or more `<name> := <expr>`
    /// assignments separated by newlines. The pre-bound
    /// wires (`body`, `count`, `ok`, captures) are
    /// available; references resolve via the standard
    /// closure-binding rule (gk module matter detects
    /// linkages).
    String(String),
    /// Sequence of fragments. Each element is itself a
    /// `ResultSpec` (string or map; nested lists are parsed
    /// but unconventional). Fragments concatenate into one
    /// result-bindings scope; key collisions across map-
    /// shape fragments are a hard error.
    List(Vec<ResultSpec>),
    /// Named-key short-forms. Each value is one of:
    /// `"count"`, `"ok"`, a path expression (no parens), or
    /// any other string treated as a GK expression. Map
    /// shape also produces a composite-map wire keyed by
    /// the YAML keys.
    Map(std::collections::BTreeMap<String, String>),
}

/// Legacy alias for backwards compatibility during Push 2.
/// Drops once every consumer migrates to `ResultSpec`.
pub type ResultWireSpec = LegacyResultWireSpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LegacyResultWireSpec {
    String(String),
    Object {
        source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
    },
}

impl LegacyResultWireSpec {
    pub fn source(&self) -> &str {
        match self {
            LegacyResultWireSpec::String(s) => s,
            LegacyResultWireSpec::Object { source, .. } => source,
        }
    }
}

impl ResultSpec {
    /// Walk the spec tree (handling list-shape recursion)
    /// and yield every (wire-name, source-expr) pair the
    /// spec ultimately declares.
    ///
    /// For map-shape entries, the source is whatever the
    /// user wrote (`count` / `ok` / path-expr / GK expr).
    /// For string-shape entries, the source is the entire
    /// GK block — the caller compiles it as a unit and
    /// extracts wire names from the LHS of each `:=`
    /// assignment.
    pub fn walk_fragments<F: FnMut(ResultFragment<'_>)>(&self, mut on: F) {
        self.walk_fragments_inner(&mut on);
    }

    fn walk_fragments_inner<F: FnMut(ResultFragment<'_>)>(&self, on: &mut F) {
        match self {
            ResultSpec::String(s) => on(ResultFragment::Source(s)),
            ResultSpec::List(items) => {
                for item in items {
                    item.walk_fragments_inner(on);
                }
            }
            ResultSpec::Map(entries) => {
                for (name, source) in entries {
                    on(ResultFragment::Named { name, source });
                }
            }
        }
    }

    /// True when the spec declares no wires. Used by the
    /// wrapper-trigger to skip wrapping when `result:` was
    /// explicitly empty.
    pub fn is_empty(&self) -> bool {
        match self {
            ResultSpec::String(s) => s.trim().is_empty(),
            ResultSpec::List(items) => items.iter().all(|i| i.is_empty()),
            ResultSpec::Map(entries) => entries.is_empty(),
        }
    }
}

/// One step of `ResultSpec::walk_fragments`. Either a
/// string-shape source block (compile as a GK module) or a
/// map-shape `(name, source)` pair (compile as a single
/// `name := source` binding).
pub enum ResultFragment<'a> {
    Source(&'a str),
    Named { name: &'a str, source: &'a str },
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
            metrics: HashMap::new(),
            result: None,
            wrappers: None,
            captures: Vec::new(),
        }
    }
}
