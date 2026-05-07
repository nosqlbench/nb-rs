// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Unified `report:` block — plots and tables under one schema.
//!
//! See SRD-46 (`docs/sysref/46_reports.md`) for the authoritative
//! design. This module owns:
//!
//! - The data model: [`Report`] / [`ReportGroup`] / [`ReportItem`]
//!   / [`Kind`] / [`Style`].
//! - The directive-string tokenizer that splits a group's body
//!   into items keyed by leading `plot <name>` / `table <name>`.
//! - The JSON sub-block parser for `series ... {…}` style
//!   overrides (strict JSON only — relies on the existing rule
//!   that GK `{...}` is unambiguously not JSON).
//! - Style cascade helpers: `defaults` at workload root,
//!   `defaults` at group level, then per-item directives.
//!
//! The crate is parser-only. Renderers in `nbrs/src/plot_metrics.rs`
//! and the corresponding table renderer consume `ReportItem.body`
//! plus the resolved [`Style`] — they don't touch the raw YAML
//! again.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub mod vocab;
pub use vocab::{
    Directive, DirectiveTarget, KindMask, ValueProvider, YamlForm,
    ALL_DIRECTIVES, AGG_FNS, AXIS_SCALES, LINE_STYLES, MARKER_SHAPES, PALETTE_NAMES,
    cli_flags_for, directive_by_cli_flag, directive_by_yaml_keyword, directives_for,
};

/// Top-level `report:` block.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Report {
    /// Cross-cutting style + metadata defaults declared via the
    /// reserved `defaults:` mapping. Cascade into every group.
    #[serde(default)]
    pub defaults: Style,
    /// Groups in YAML declaration order. Each group's name becomes
    /// a markdown section heading; items render in body order.
    #[serde(default)]
    pub groups: Vec<ReportGroup>,
}

impl Report {
    /// Iterate every item across every group in declaration order.
    pub fn items(&self) -> impl Iterator<Item = &ReportItem> {
        self.groups.iter().flat_map(|g| g.items.iter())
    }

    /// Find an item by canonical name. Returns the first match in
    /// declaration order. Names should be unique within a `Report`
    /// (collisions are caught at parse time).
    pub fn find(&self, name: &str) -> Option<&ReportItem> {
        self.items().find(|i| i.name == name)
    }

    /// Resolve the effective style for one item, walking the
    /// cascade: report defaults → group defaults → item directives.
    pub fn effective_style(&self, group: &ReportGroup, item: &ReportItem) -> Style {
        let mut s = self.defaults.clone();
        s.merge_from(&group.defaults);
        s.merge_from(&item.style);
        s
    }
}

/// One named container in `report:`. Holds zero or more items
/// plus group-level defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReportGroup {
    /// YAML key the group was declared under (e.g.
    /// `recall_block`). Used as the markdown section heading.
    pub name: String,
    /// Group-level style defaults (from `defaults <directives>`
    /// lines at the start of the group body). Cascade into every
    /// item in this group.
    #[serde(default)]
    pub defaults: Style,
    /// Items declared in this group, in body declaration order.
    #[serde(default)]
    pub items: Vec<ReportItem>,
}

/// One renderable report item — either a plot or a table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReportItem {
    /// `plot` or `table`.
    pub kind: Kind,
    /// Canonical name (the token after `plot` / `table`).
    pub name: String,
    /// Display label (`label "..."`). Falls back to a prettified
    /// form of `name` when absent.
    #[serde(default)]
    pub label: Option<String>,
    /// Output-filename stem (`as <stem>`). When absent, default
    /// is `plot_<name>` / `table_<name>` derived at render time.
    #[serde(default)]
    pub as_stem: Option<String>,
    /// Per-item style overrides parsed from the directive body.
    #[serde(default)]
    pub style: Style,
    /// Raw spec body — the directive lines following the
    /// `<kind> <name>` line, with `as`/`label`/style directives
    /// stripped out (they live on the [`ReportItem`] / [`Style`]
    /// fields). Renderers parse this exactly the way they parse
    /// the legacy CLI spec strings.
    #[serde(default)]
    pub body: String,
    /// Output file this item flows into when the markdown
    /// assembler runs. `None` ⇒ default `summary.md`. Set by a
    /// preceding `file <filename>` directive in the same group.
    /// Resolved at parse time so consumers don't need to walk
    /// the group order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_file: Option<String>,
    /// Plot-only directive: when `true`, the renderer
    /// emits a companion table immediately after the plot
    /// in the same markdown file, sharing the plot's
    /// underlying query data. The companion table reuses
    /// the plot's name with a `_table` suffix for its
    /// anchor and figure-numbering slot, so users can
    /// link to either view independently.
    ///
    /// Set via `with-table: true` in the plot's body.
    /// No-op for `Kind::Table` and other non-plot items.
    /// Default `false` — plots without an explicit toggle
    /// keep their existing single-image behaviour.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub with_table: bool,
}

/// Kind discriminator. Identifies which renderer owns the item.
///
/// `Plot` and `Table` are figures — they carry data and get a
/// figure number. `Text` is markdown prose; `File` is a scope
/// directive that switches subsequent items' output file.
/// `Details` is an auto-injected session-context block (the
/// runner emits one to every target markdown file at end-of-
/// run; users can also declare it explicitly to control its
/// position). Only Plot and Table participate in figure
/// numbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Plot,
    Table,
    Text,
    File,
    Details,
}

impl Default for Kind {
    fn default() -> Self { Kind::Plot }
}

impl Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Plot => "plot",
            Kind::Table => "table",
            Kind::Text => "text",
            Kind::File => "file",
            Kind::Details => "details",
        }
    }

    /// True for kinds that contribute a figure to the report
    /// (plot, table). Used by the figure-numbering pass.
    pub fn is_figure(&self) -> bool {
        matches!(self, Kind::Plot | Kind::Table)
    }
}

/// Style and figure-metadata bag. Every field optional; cascade
/// is "first non-`None` wins outer-to-inner".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Style {
    /// Palette name or numeric index (`"wong"`, `"3"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub palette: Option<String>,
    /// Line-dash style: `solid`, `dashed`, `dotted`, `dashdot`,
    /// `none`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<String>,
    /// Stroke width in pixels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<f32>,
    /// Marker shape: `none`, `circle`, `square`, `triangle`,
    /// `diamond`, `plus`, `cross`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker: Option<String>,
    /// Marker radius in pixels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<f32>,
    /// Hex color override (`#RRGGBB`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// Figure width in pixels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub figure_width: Option<u32>,
    /// Figure height in pixels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub figure_height: Option<u32>,
    /// Per-series JSON sub-block overrides keyed by the
    /// `<key>=<value>` discriminator after the `series` directive.
    /// Values are arbitrary JSON objects; renderers consume the
    /// fields they recognise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub series: Vec<SeriesOverride>,
}

/// One `series <key>=<value> {...}` directive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesOverride {
    /// Discriminator key (e.g. `profile`).
    pub key: String,
    /// Discriminator value (e.g. `hnsw`).
    pub value: String,
    /// Style overrides — strict JSON object verbatim, parsed
    /// into a Style at apply time.
    pub style: Style,
}

impl ReportItem {
    /// Render this item back to its canonical YAML directive
    /// form — the same string shape the workload-YAML parser
    /// in [`parse_group`] consumes. Round-trips:
    /// `parse_group(... item.to_yaml_directive_string() ...) == item`.
    ///
    /// The output is one or more lines:
    ///
    /// ```text
    /// plot <name>
    ///   as <stem>
    ///   label "<label>"
    ///   palette=<v>
    ///   line=<v> width=<n> marker=<v> size=<n> color=<#hex>
    ///   <body lines verbatim>
    ///   series <key>=<val> {<json>}
    /// ```
    ///
    /// Order matches [`vocab::ALL_DIRECTIVES`] so the
    /// round-trip is stable. Style fields that are `None` are
    /// omitted; the body is appended verbatim (it carries the
    /// renderer-consumed `over` / `by` / `where` / `agg` /
    /// `xlabel` / etc. lines).
    pub fn to_yaml_directive_string(&self) -> String {
        let mut out = String::new();
        // Header line: `<kind> <name>`. `Details` items also
        // round-trip; `File` directives use their target stem
        // as the "name" (a `file <stem>` line).
        out.push_str(self.kind.as_str());
        out.push(' ');
        out.push_str(&self.name);
        out.push('\n');

        // `as <stem>` and `label "<text>"` come first when
        // present — the reader expects identity directives
        // before the per-renderer body.
        if let Some(stem) = &self.as_stem {
            out.push_str("  as ");
            out.push_str(stem);
            out.push('\n');
        }
        if let Some(label) = &self.label {
            out.push_str("  label \"");
            out.push_str(&label.replace('\\', "\\\\").replace('"', "\\\""));
            out.push_str("\"\n");
        }

        // Style scalars in declaration order from the vocab
        // registry. Each is `<key>=<value>` on its own line —
        // matches the existing parser's `apply_one_style_kv`
        // contract.
        for line in self.style.scalar_directive_lines() {
            out.push_str("  ");
            out.push_str(&line);
            out.push('\n');
        }

        // Body lines (over / by / where / agg / xlabel / etc.)
        // are passed through verbatim. The body is already in
        // the canonical form the renderer expects.
        if !self.body.trim().is_empty() {
            for line in self.body.split('\n') {
                if line.trim().is_empty() { continue; }
                out.push_str("  ");
                out.push_str(line.trim_start());
                out.push('\n');
            }
        }

        // Per-series sub-blocks. Use the brace-free directive
        // form (`series profile=hnsw line=dashed`) when only
        // simple key=value scalars are involved; fall back to
        // the JSON form for completeness when the series style
        // carries series-style fields that don't survive the
        // simple form (today they all do, but reserved for
        // future extension).
        for s in &self.style.series {
            out.push_str("  style ");
            out.push_str(&s.key);
            out.push('=');
            out.push_str(&s.value);
            for line in s.style.scalar_directive_lines() {
                out.push(' ');
                out.push_str(&line);
            }
            out.push('\n');
        }

        out
    }
}

impl Style {
    /// Render the scalar (non-series) fields of this style
    /// as a list of `key=value` directive lines, in the
    /// canonical [`vocab::ALL_DIRECTIVES`] order. Used by
    /// [`ReportItem::to_yaml_directive_string`] and by
    /// the `series` sub-block renderer.
    ///
    /// `None` fields are skipped; only fields whose vocab
    /// `target` is [`vocab::DirectiveTarget::StyleField`]
    /// are emitted.
    pub fn scalar_directive_lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        for d in vocab::ALL_DIRECTIVES {
            if !matches!(d.target, vocab::DirectiveTarget::StyleField) {
                continue;
            }
            let value: Option<String> = match d.yaml_directive {
                "palette"       => self.palette.clone(),
                "line"          => self.line.clone(),
                "width"         => self.width.map(|v| v.to_string()),
                "marker"        => self.marker.clone(),
                "size"          => self.size.map(|v| v.to_string()),
                "color"         => self.color.clone(),
                "figure_width"  => self.figure_width.map(|v| v.to_string()),
                "figure_height" => self.figure_height.map(|v| v.to_string()),
                _ => None,
            };
            if let Some(v) = value {
                out.push(format!("{}={}", d.yaml_directive, v));
            }
        }
        out
    }

    /// Merge `other` into `self`: every `Some(_)` field in `other`
    /// overrides the corresponding field in `self`. Used to walk
    /// the cascade outer → inner.
    pub fn merge_from(&mut self, other: &Style) {
        if other.palette.is_some() { self.palette = other.palette.clone(); }
        if other.line.is_some() { self.line = other.line.clone(); }
        if other.width.is_some() { self.width = other.width; }
        if other.marker.is_some() { self.marker = other.marker.clone(); }
        if other.size.is_some() { self.size = other.size; }
        if other.color.is_some() { self.color = other.color.clone(); }
        if other.figure_width.is_some() { self.figure_width = other.figure_width; }
        if other.figure_height.is_some() { self.figure_height = other.figure_height; }
        if !other.series.is_empty() {
            for s in &other.series {
                self.series.retain(|t| !(t.key == s.key && t.value == s.value));
                self.series.push(s.clone());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Reserved directive keywords. Used to:
/// (1) reject group child keys that collide with directive names
///     (warn / error in strict mode),
/// (2) detect when a directive line is the start of a new item
///     (`plot`, `table`) vs a continuation directive.
const STYLE_DIRECTIVE_KEYWORDS: &[&str] = &[
    "palette", "line", "width", "marker", "size", "color",
    "figure_width", "figure_height", "style", "label", "as",
];

const ITEM_KIND_KEYWORDS: &[&str] = &["plot", "table", "text", "file"];

const ALL_RESERVED_DIRECTIVES: &[&str] = &[
    "defaults",
    "plot", "table", "text", "file",
    "palette", "line", "width", "marker", "size", "color",
    "figure_width", "figure_height", "style", "label", "as",
];

/// Parse a `report:` value (a YAML mapping) into a [`Report`].
///
/// Errors describe the offending input precisely; warnings (e.g.
/// empty groups, kind-mismatched directives) are collected
/// alongside so callers can decide whether to surface or promote
/// to errors under strict mode.
pub fn parse_report(value: &serde_json::Value) -> Result<ParsedReport, String> {
    let map = value.as_object()
        .ok_or_else(|| "report: must be a mapping".to_string())?;

    let mut report = Report::default();
    let mut warnings: Vec<String> = Vec::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    // Global counter for anonymous text item naming so the
    // round-trip identity (`report.<name>` keys) stays unique
    // across the whole document, not just within one group.
    let mut text_counter: usize = 0;

    for (key, v) in map {
        let key: &str = key.as_str();
        if key == "defaults" {
            report.defaults = parse_style_mapping(v)
                .map_err(|e| format!("report.defaults: {e}"))?;
            continue;
        }
        if STYLE_DIRECTIVE_KEYWORDS.contains(&key) {
            warnings.push(format!(
                "report.{key}: bare directive keyword used as a group name; \
                 nest under `defaults:` to set as a default, or rename the group"
            ));
        }

        let body = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Null => String::new(),
            _ => return Err(format!(
                "report.{key}: must be a string (single-line or block scalar) \
                 of directive lines starting with `plot` / `table`"
            )),
        };

        let group = parse_group(key, &body, &mut warnings, &mut text_counter)?;
        for it in &group.items {
            if !seen_names.insert(it.name.clone()) {
                return Err(format!(
                    "duplicate report item name '{}' (within scope: workload root)",
                    it.name
                ));
            }
        }
        if group.items.is_empty() {
            warnings.push(format!(
                "report.{key}: empty group (no `plot` or `table` items)"
            ));
        }
        report.groups.push(group);
    }

    Ok(ParsedReport { report, warnings })
}

/// Result of parsing a `report:` block. Warnings are non-fatal
/// in normal mode; callers running under SRD-15 strict mode
/// should promote them to errors.
#[derive(Debug, Clone, Default)]
pub struct ParsedReport {
    pub report: Report,
    pub warnings: Vec<String>,
}

fn parse_group(
    name: &str,
    body: &str,
    warnings: &mut Vec<String>,
    text_counter: &mut usize,
) -> Result<ReportGroup, String> {
    let mut group = ReportGroup { name: name.to_string(), ..Default::default() };
    let mut current: Option<PartialItem> = None;
    // Active output file scope (set by a `file <filename>` line).
    // Items declared after a `file` directive inherit this until
    // the next `file` directive in the same group.
    let mut current_file: Option<String> = None;

    let emit = |partial: PartialItem,
                target_file: &Option<String>,
                group: &mut ReportGroup,
                warnings: &mut Vec<String>|
        -> Result<(), String> {
        let mut item = partial.finalize(warnings)?;
        if item.target_file.is_none() {
            item.target_file = target_file.clone();
        }
        group.items.push(item);
        Ok(())
    };

    for (lineno, raw_line) in body.lines().enumerate() {
        // Strip `#` line comments (SRD-46) before parsing —
        // honours quoted strings so `label "see #1 above"`
        // survives.
        let stripped = strip_line_comment(raw_line);
        let line = stripped.trim();
        if line.is_empty() { continue; }
        let line_no = lineno + 1;

        // `defaults <directives>` — group-level defaults. Only
        // valid before any item starts (since otherwise the
        // intent is ambiguous: do they apply to subsequent items
        // only? to all items? we pick "before-first-item" so
        // there's exactly one place defaults can land).
        if let Some(rest) = strip_directive_keyword(line, "defaults") {
            if current.is_some() {
                return Err(format!(
                    "report.{name}:{line_no}: `defaults` must precede the first \
                     item in the group"
                ));
            }
            apply_directives_to_style(rest, &mut group.defaults, name, line_no)?;
            continue;
        }

        // `<kind> <args>` — start of new item.
        if let Some((kind, rest)) = strip_kind_keyword(line) {
            if let Some(prev) = current.take() {
                emit(prev, &current_file, &mut group, warnings)?;
            }
            match kind {
                Kind::Plot | Kind::Table => {
                    let mut tokens = rest.splitn(2, char::is_whitespace);
                    let item_name = tokens.next()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| format!(
                            "report.{name}:{line_no}: `{}` must be followed by a name",
                            kind.as_str()
                        ))?;
                    if ALL_RESERVED_DIRECTIVES.contains(&item_name) {
                        return Err(format!(
                            "report.{name}:{line_no}: item name '{item_name}' \
                             collides with a reserved directive keyword"
                        ));
                    }
                    let trailing = tokens.next().unwrap_or("");
                    let mut p = PartialItem::new(kind, item_name, name.to_string(), line_no);
                    if !trailing.trim().is_empty() {
                        p.directives.push(trailing.to_string());
                    }
                    current = Some(p);
                }
                Kind::Text => {
                    // `text <body>` — anonymous markdown prose.
                    // Auto-name globally so the persistence
                    // round-trip keeps unique `report.<name>`
                    // keys; body is the remainder of this line
                    // plus continuation lines until the next
                    // kind keyword.
                    *text_counter += 1;
                    let auto_name = format!("text_{:03}", *text_counter);
                    let mut p = PartialItem::new(
                        Kind::Text, &auto_name, name.to_string(), line_no);
                    if !rest.trim().is_empty() {
                        p.directives.push(rest.to_string());
                    }
                    current = Some(p);
                }
                Kind::File => {
                    // `file <filename> [as <label>]` — switches
                    // the active output file. The directive is
                    // also persisted as a Kind::File item so the
                    // listing surface can show it; no body
                    // attaches.
                    let mut tokens = rest.splitn(2, char::is_whitespace);
                    let filename = tokens.next()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| format!(
                            "report.{name}:{line_no}: `file` must be followed by a filename"
                        ))?
                        .to_string();
                    let trailing = tokens.next().unwrap_or("");
                    let mut p = PartialItem::new(
                        Kind::File, &filename, name.to_string(), line_no);
                    // Honor optional `as '<label>'` on the same
                    // line (passed through to finalize via the
                    // directive list — the `label` extractor
                    // already understands quoted strings).
                    if !trailing.trim().is_empty() {
                        // Translate `as 'X'` → `label X` so the
                        // existing label extractor handles it.
                        let trailing = trailing.trim();
                        if let Some(rest) = strip_directive_keyword(trailing, "as") {
                            p.directives.push(format!("label {rest}"));
                        } else {
                            p.directives.push(trailing.to_string());
                        }
                    }
                    current_file = Some(filename);
                    current = Some(p);
                }
                Kind::Details => {
                    // Auto-injected at end-of-run; explicit
                    // `details` declarations are accepted so the
                    // author can pin position. Body is whatever
                    // the assembler decides — usually empty
                    // when declared explicitly (the runtime
                    // fills it in).
                    let mut p = PartialItem::new(
                        Kind::Details, "details", name.to_string(), line_no);
                    if !rest.trim().is_empty() {
                        p.directives.push(rest.to_string());
                    }
                    current = Some(p);
                }
            }
            continue;
        }

        // Continuation directive line for the current item.
        match current.as_mut() {
            Some(p) => p.directives.push(line.to_string()),
            None => return Err(format!(
                "report.{name}:{line_no}: directive `{line}` precedes any \
                 kind keyword (plot / table / text / file)"
            )),
        }
    }

    if let Some(p) = current.take() {
        emit(p, &current_file, &mut group, warnings)?;
    }
    Ok(group)
}

struct PartialItem {
    kind: Kind,
    name: String,
    group: String,
    line_no: usize,
    directives: Vec<String>,
}

impl PartialItem {
    fn new(kind: Kind, name: &str, group: String, line_no: usize) -> Self {
        Self { kind, name: name.to_string(), group, line_no, directives: Vec::new() }
    }

    /// Pull `label` / `as` / style directives out of the directive
    /// body. Anything else is left in `body` for the kind-specific
    /// renderer to consume.
    fn finalize(self, warnings: &mut Vec<String>) -> Result<ReportItem, String> {
        let mut item = ReportItem {
            kind: self.kind,
            name: self.name.clone(),
            label: None,
            as_stem: None,
            style: Style::default(),
            body: String::new(),
            target_file: None,
            with_table: false,
        };

        // Text items: body is verbatim markdown. Pull out a
        // leading `label "..."` line if present (so the heading
        // can carry a title), but every other line stays as
        // prose. Style / series / `as` directives don't apply
        // to text — preserved in body if the user wrote them
        // (most likely they meant prose).
        if matches!(self.kind, Kind::Text) {
            let mut body_lines: Vec<String> = Vec::new();
            for line in &self.directives {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    body_lines.push(String::new());
                    continue;
                }
                if let Some(rest) = strip_directive_keyword(trimmed, "label") {
                    item.label = Some(parse_quoted_or_bare(rest));
                } else {
                    body_lines.push(line.clone());
                }
            }
            item.body = body_lines.join("\n");
            return Ok(item);
        }

        let mut residual: Vec<String> = Vec::new();
        for line in &self.directives {
            let line = line.trim();
            if line.is_empty() { continue; }

            if let Some(rest) = strip_directive_keyword(line, "label") {
                item.label = Some(parse_quoted_or_bare(rest));
                continue;
            }
            if let Some(rest) = strip_directive_keyword(line, "as") {
                item.as_stem = Some(rest.trim().to_string());
                continue;
            }
            // `with-table: true|false` — plot-only flag
            // that asks the renderer to emit a companion
            // table immediately after the plot, sharing
            // the plot's underlying query data. SRD-46
            // §"Table-from-plot". Accepted-but-warned for
            // non-plot items.
            if let Some(rest) = strip_directive_keyword(line, "with-table") {
                let v = rest.trim().trim_matches(':').trim();
                let truthy = matches!(
                    v.to_ascii_lowercase().as_str(),
                    "true" | "yes" | "on" | "1" | "",
                );
                let falsy = matches!(
                    v.to_ascii_lowercase().as_str(),
                    "false" | "no" | "off" | "0",
                );
                if !truthy && !falsy {
                    return Err(format!(
                        "report.{}:{} `with-table`: expected true/false, got '{v}'",
                        self.group, self.line_no,
                    ));
                }
                item.with_table = truthy;
                if item.with_table && !matches!(self.kind, Kind::Plot) {
                    warnings.push(format!(
                        "report.{}:{} item '{}' uses `with-table` but is a {}; \
                         only plots can have a companion table.",
                        self.group, self.line_no, item.name, item.kind.as_str(),
                    ));
                    item.with_table = false;
                }
                continue;
            }
            if let Some(rest) = strip_directive_keyword(line, "style") {
                let so = parse_series_override(rest)
                    .map_err(|e| format!(
                        "report.{}:{} `style`: {}",
                        self.group, self.line_no, e,
                    ))?;
                item.style.series.push(so);
                continue;
            }

            if let Some(applied) = try_apply_style_directives(line, &mut item.style)? {
                if !applied {
                    residual.push(line.to_string());
                } else {
                    // Per-item directive that doesn't apply to the
                    // item's kind: warn (SRD-46 strict-mode hook).
                    if !style_directive_applies_to_kind(line, item.kind) {
                        warnings.push(format!(
                            "report.{}:{} item '{}' is a {} but uses \
                             directive `{}`, which has no effect on this kind",
                            self.group, self.line_no, item.name,
                            item.kind.as_str(), line
                        ));
                    }
                }
                continue;
            }

            residual.push(line.to_string());
        }
        item.body = residual.join("\n");
        Ok(item)
    }
}

fn strip_directive_keyword<'a>(line: &'a str, kw: &str) -> Option<&'a str> {
    let line = line.trim_start();
    if let Some(rest) = line.strip_prefix(kw) {
        let next = rest.chars().next();
        if next.is_none() || next == Some(' ') || next == Some('\t') || next == Some('=') {
            return Some(rest.trim_start_matches(|c: char| c == ' ' || c == '\t' || c == '='));
        }
    }
    None
}

fn strip_kind_keyword(line: &str) -> Option<(Kind, &str)> {
    for kw in ITEM_KIND_KEYWORDS {
        if let Some(rest) = line.strip_prefix(kw)
            && let Some(next) = rest.chars().next()
            && next.is_whitespace() {
            let kind = match *kw {
                "plot" => Kind::Plot,
                "table" => Kind::Table,
                "text" => Kind::Text,
                "file" => Kind::File,
                _ => unreachable!(),
            };
            return Some((kind, rest.trim_start()));
        }
    }
    None
}

fn parse_quoted_or_bare(s: &str) -> String {
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix('"').and_then(|x| x.strip_suffix('"')) {
        return stripped.to_string();
    }
    if let Some(stripped) = s.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')) {
        return stripped.to_string();
    }
    s.to_string()
}

/// Apply style directives from a single line (any combination
/// of `key=value` pairs separated by whitespace or commas) to
/// the given Style. Returns `Ok(Some(true))` if every token on
/// the line was a recognized style directive, `Ok(Some(false))`
/// if some tokens belonged to the kind-specific spec body, or
/// `Ok(None)` if the line doesn't look like a style directive
/// line at all (no `=` and no leading style keyword).
fn try_apply_style_directives(line: &str, style: &mut Style) -> Result<Option<bool>, String> {
    if !line.contains('=') && !line_starts_with_any(line, STYLE_DIRECTIVE_KEYWORDS) {
        return Ok(None);
    }
    let mut all_consumed = true;
    for token in tokenize_directive_line(line) {
        if let Some((k, v)) = token.split_once('=') {
            if STYLE_DIRECTIVE_KEYWORDS.contains(&k) {
                apply_one_style_kv(k, v, style)?;
            } else {
                all_consumed = false;
            }
        } else if STYLE_DIRECTIVE_KEYWORDS.contains(&token.as_str()) {
            // Bare keyword without value — not yet meaningful.
            return Err(format!("style directive `{token}` requires a value"));
        } else {
            all_consumed = false;
        }
    }
    Ok(Some(all_consumed))
}

fn apply_one_style_kv(k: &str, v: &str, style: &mut Style) -> Result<(), String> {
    let v = v.trim().trim_matches('"').trim_matches('\'');
    match k {
        "palette" => style.palette = Some(v.to_string()),
        "line" => style.line = Some(v.to_string()),
        "width" => style.width = Some(v.parse()
            .map_err(|_| format!("`width={v}` is not a number"))?),
        "marker" => style.marker = Some(v.to_string()),
        "size" => style.size = Some(v.parse()
            .map_err(|_| format!("`size={v}` is not a number"))?),
        "color" => style.color = Some(v.to_string()),
        "figure_width" => style.figure_width = Some(v.parse()
            .map_err(|_| format!("`figure_width={v}` is not an integer"))?),
        "figure_height" => style.figure_height = Some(v.parse()
            .map_err(|_| format!("`figure_height={v}` is not an integer"))?),
        _ => return Err(format!("unknown style directive `{k}`")),
    }
    Ok(())
}

fn tokenize_directive_line(line: &str) -> Vec<String> {
    // Split on whitespace and commas at depth 0. Quoted values
    // stay together. No JSON braces here — those are only inside
    // `series` sub-blocks and parsed separately.
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for ch in line.chars() {
        match quote {
            Some(q) if ch == q => { quote = None; cur.push(ch); }
            Some(_) => cur.push(ch),
            None => match ch {
                '"' | '\'' => { quote = Some(ch); cur.push(ch); }
                ' ' | '\t' | ',' => {
                    if !cur.is_empty() { out.push(std::mem::take(&mut cur)); }
                }
                _ => cur.push(ch),
            }
        }
    }
    if !cur.is_empty() { out.push(cur); }
    out
}

fn line_starts_with_any(line: &str, kws: &[&str]) -> bool {
    let trimmed = line.trim_start();
    kws.iter().any(|kw| {
        trimmed.starts_with(kw)
            && trimmed.as_bytes().get(kw.len())
                .is_none_or(|c| matches!(*c, b' ' | b'\t' | b'='))
    })
}

fn style_directive_applies_to_kind(line: &str, kind: Kind) -> bool {
    let head = tokenize_directive_line(line).first()
        .map(|s| s.split_once('=').map(|(k, _)| k.to_string())
            .unwrap_or_else(|| s.clone()))
        .unwrap_or_default();
    match (kind, head.as_str()) {
        (Kind::Table, "line" | "width" | "marker" | "size") => false,
        _ => true,
    }
}

fn parse_series_override(s: &str) -> Result<SeriesOverride, String> {
    // Shape: `<key>=<value> {<json>}` or `<key>=<value> <directives>`
    let s = s.trim();
    let (head, rest) = s.split_once(char::is_whitespace).unwrap_or((s, ""));
    let (key, value) = head.split_once('=')
        .ok_or_else(|| format!("series discriminator must be <key>=<value>, got `{head}`"))?;

    let mut style = Style::default();
    let rest = rest.trim();

    if rest.starts_with('{') {
        // JSON sub-block — strict.
        if !rest.ends_with('}') {
            return Err("JSON sub-block must close with `}` on the same line".to_string());
        }
        let json: serde_json::Value = serde_json::from_str(rest)
            .map_err(|e| format!("JSON sub-block parse error: {e}"))?;
        apply_json_to_style(&json, &mut style)?;
    } else if !rest.is_empty() {
        // Brace-free directive form.
        try_apply_style_directives(rest, &mut style)?;
    }

    Ok(SeriesOverride {
        key: key.trim().to_string(),
        value: value.trim().trim_matches('"').trim_matches('\'').to_string(),
        style,
    })
}

fn apply_json_to_style(v: &serde_json::Value, style: &mut Style) -> Result<(), String> {
    let map = v.as_object()
        .ok_or_else(|| "series JSON sub-block must be an object".to_string())?;
    for (k, val) in map {
        let v_str = match val {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => return Err(format!(
                "series JSON sub-block: value for `{k}` must be a string, number, or bool"
            )),
        };
        apply_one_style_kv(k, &v_str, style)?;
    }
    Ok(())
}

fn apply_directives_to_style(
    line: &str,
    style: &mut Style,
    group: &str,
    line_no: usize,
) -> Result<(), String> {
    match try_apply_style_directives(line, style) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("report.{group}:{line_no}: {e}")),
    }
}

/// Strip a `#` line comment from one source line. A `#` starts
/// a comment only when it is at line start or preceded by
/// whitespace — so hex colors (`#117733`) and JSON sub-blocks
/// (`{"color": "#fff"}`) are unaffected. Quoted strings are
/// honoured so `label "see #1 above"` keeps its `#`.
fn strip_line_comment(line: &str) -> &str {
    let mut quote: Option<char> = None;
    let mut prev_ws = true; // start-of-line counts as whitespace boundary
    for (i, ch) in line.char_indices() {
        match quote {
            Some(q) if ch == q => { quote = None; prev_ws = false; }
            Some(_) => { prev_ws = false; }
            None => match ch {
                '"' | '\'' => { quote = Some(ch); prev_ws = false; }
                '#' if prev_ws => return &line[..i],
                c if c.is_whitespace() => { prev_ws = true; }
                _ => { prev_ws = false; }
            }
        }
    }
    line
}

fn parse_style_mapping(v: &serde_json::Value) -> Result<Style, String> {
    let map = v.as_object()
        .ok_or_else(|| "must be a mapping of style directives".to_string())?;
    let mut style = Style::default();
    for (key, val) in map {
        let key = key.as_str();
        let value: String = match val {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => continue,
            _ => return Err(format!(
                "value for `{key}` must be a scalar (string, number, or bool)"
            )),
        };
        apply_one_style_kv(key, &value, &mut style)?;
    }
    Ok(style)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> ParsedReport {
        let v: serde_json::Value = serde_yaml::from_str(yaml).unwrap();
        parse_report(&v).unwrap()
    }

    #[test]
    fn single_plot_minimal() {
        let p = parse(r#"
recall_block: |
  plot recall_at_k10
    over limit by profile
    label "Recall@10 vs k limit"
"#);
        assert_eq!(p.report.groups.len(), 1);
        let g = &p.report.groups[0];
        assert_eq!(g.name, "recall_block");
        assert_eq!(g.items.len(), 1);
        let item = &g.items[0];
        assert_eq!(item.kind, Kind::Plot);
        assert_eq!(item.name, "recall_at_k10");
        assert_eq!(item.label.as_deref(), Some("Recall@10 vs k limit"));
        assert!(item.body.contains("over limit"));
    }

    #[test]
    fn defaults_at_root_and_group() {
        let p = parse(r#"
defaults:
  palette: wong
  width: 1024

recall_block: |
  defaults palette=tol_muted
  plot recall_at_k10 over limit
"#);
        assert_eq!(p.report.defaults.palette.as_deref(), Some("wong"));
        assert_eq!(p.report.defaults.width, Some(1024.0));
        let g = &p.report.groups[0];
        assert_eq!(g.defaults.palette.as_deref(), Some("tol_muted"));
        // Item-level inherits via merge_from at apply time.
        let s = p.report.effective_style(g, &g.items[0]);
        assert_eq!(s.palette.as_deref(), Some("tol_muted"));
        assert_eq!(s.width, Some(1024.0));
    }

    #[test]
    fn plots_and_tables_in_one_group() {
        let p = parse(r#"
combo: |
  plot recall_at_k10 over limit
  table recall_summary metric=recall@.* group_by=profile
"#);
        let items = &p.report.groups[0].items;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].kind, Kind::Plot);
        assert_eq!(items[0].name, "recall_at_k10");
        assert_eq!(items[1].kind, Kind::Table);
        assert_eq!(items[1].name, "recall_summary");
    }

    #[test]
    fn style_json_sub_block() {
        let p = parse(r#"
g: |
  plot p1 over x
    style profile=hnsw {"line": "dashed", "marker": "triangle"}
"#);
        let item = &p.report.groups[0].items[0];
        assert_eq!(item.style.series.len(), 1);
        let so = &item.style.series[0];
        assert_eq!(so.key, "profile");
        assert_eq!(so.value, "hnsw");
        assert_eq!(so.style.line.as_deref(), Some("dashed"));
        assert_eq!(so.style.marker.as_deref(), Some("triangle"));
    }

    #[test]
    fn style_directive_form() {
        let p = parse(r#"
g: |
  plot p1 over x
    style profile=ivf line=dotted color=#117733
"#);
        let so = &p.report.groups[0].items[0].style.series[0];
        assert_eq!(so.value, "ivf");
        assert_eq!(so.style.line.as_deref(), Some("dotted"));
        assert_eq!(so.style.color.as_deref(), Some("#117733"));
    }

    #[test]
    fn duplicate_name_is_error() {
        let v: serde_json::Value = serde_yaml::from_str(r#"
g1: "plot dup over x"
g2: "plot dup over y"
"#).unwrap();
        assert!(parse_report(&v).is_err());
    }

    #[test]
    fn empty_group_warns() {
        let p = parse(r#"
empty_block: ""
"#);
        assert_eq!(p.report.groups.len(), 1);
        assert!(p.warnings.iter().any(|w| w.contains("empty_block")));
    }

    #[test]
    fn unknown_directive_falls_into_body() {
        let p = parse(r#"
g: |
  plot p1
    over limit
    where dataset=glove
    custom_directive=foo
"#);
        let item = &p.report.groups[0].items[0];
        assert!(item.body.contains("over limit"));
        assert!(item.body.contains("custom_directive=foo"));
    }

    #[test]
    fn label_with_quotes() {
        let p = parse(r#"
g: |
  plot p1 over x
    label 'p99 latency'
"#);
        assert_eq!(p.report.groups[0].items[0].label.as_deref(), Some("p99 latency"));
    }

    #[test]
    fn item_name_collides_with_directive_keyword_errors() {
        let v: serde_json::Value = serde_yaml::from_str(r#"
g: "plot palette over x"
"#).unwrap();
        assert!(parse_report(&v).is_err());
    }

    #[test]
    fn declaration_order_preserved_across_groups() {
        let p = parse(r#"
zzz_block: "plot z over x"
aaa_block: "plot a over x"
mmm_block: "plot m over x"
"#);
        let names: Vec<_> = p.report.groups.iter()
            .map(|g| g.name.clone()).collect();
        assert_eq!(names, vec!["zzz_block", "aaa_block", "mmm_block"]);
    }

    #[test]
    fn style_cascade_root_to_item() {
        let p = parse(r#"
defaults:
  palette: wong
  width: 1024

g: |
  defaults palette=tol_muted
  plot p1 over x
    palette=ibm
"#);
        let g = &p.report.groups[0];
        let item = &g.items[0];
        let s = p.report.effective_style(g, item);
        assert_eq!(s.palette.as_deref(), Some("ibm"));
        assert_eq!(s.width, Some(1024.0));
    }

    #[test]
    fn text_kind_keeps_body_verbatim() {
        let p = parse(r#"
intro: |
  text Welcome to the report.
   Multi-line prose continues here
   with arbitrary text.
"#);
        let item = &p.report.groups[0].items[0];
        assert_eq!(item.kind, Kind::Text);
        assert_eq!(item.name, "text_001");
        assert!(item.body.contains("Welcome to the report"));
        assert!(item.body.contains("arbitrary text"));
    }

    #[test]
    fn file_directive_scopes_following_items() {
        let p = parse(r#"
sections: |
  file my_report.md as 'Markdown Report'
    text Intro paragraph
    plot recall_at_k10 over limit
    table summary metric=recall@.* group_by=p
"#);
        let items = &p.report.groups[0].items;
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].kind, Kind::File);
        assert_eq!(items[0].name, "my_report.md");
        assert_eq!(items[0].label.as_deref(), Some("Markdown Report"));
        // Subsequent items inherit the file as target.
        for it in &items[1..] {
            assert_eq!(it.target_file.as_deref(), Some("my_report.md"),
                "item {} should target my_report.md, got {:?}",
                it.name, it.target_file);
        }
    }

    #[test]
    fn file_switch_resets_scope() {
        let p = parse(r#"
two_files: |
  file alpha.md as 'Alpha'
    plot p1 over x
  file beta.md
    plot p2 over y
"#);
        let items = &p.report.groups[0].items;
        // Items 0=file alpha, 1=plot p1 (alpha), 2=file beta, 3=plot p2 (beta)
        assert_eq!(items[0].name, "alpha.md");
        assert_eq!(items[1].target_file.as_deref(), Some("alpha.md"));
        assert_eq!(items[2].name, "beta.md");
        assert_eq!(items[3].target_file.as_deref(), Some("beta.md"));
    }

    #[test]
    fn items_before_any_file_have_no_target() {
        let p = parse(r#"
mixed: |
  plot orphan over x
  file r.md
    plot inside over x
"#);
        let items = &p.report.groups[0].items;
        assert_eq!(items[0].name, "orphan");
        assert_eq!(items[0].target_file, None);
        assert_eq!(items[2].name, "inside");
        assert_eq!(items[2].target_file.as_deref(), Some("r.md"));
    }

    #[test]
    fn hash_line_comments_stripped() {
        let p = parse(r#"
g: |
  # comment line — ignored
  plot p1   # trailing comment
    over limit  # also stripped
    label "Mean recall"  # don't strip inside quotes
"#);
        let item = &p.report.groups[0].items[0];
        assert_eq!(item.kind, Kind::Plot);
        assert_eq!(item.name, "p1");
        assert!(item.body.contains("over limit"), "body kept: {:?}", item.body);
        assert!(!item.body.contains("trailing"), "comment leaked: {:?}", item.body);
        assert_eq!(item.label.as_deref(), Some("Mean recall"));
    }

    #[test]
    fn auto_text_names_unique_across_document() {
        let p = parse(r#"
g1: |
  text First
  text Second
g2: |
  text Third
"#);
        let g1 = &p.report.groups[0];
        assert_eq!(g1.items[0].name, "text_001");
        assert_eq!(g1.items[1].name, "text_002");
        // Counter is global, not per-group, so persistence keys
        // (`report.<name>`) stay unique.
        let g2 = &p.report.groups[1];
        assert_eq!(g2.items[0].name, "text_003");
    }

    #[test]
    fn label_directive_strips_quotes() {
        let p = parse(r#"
g: |
  plot p1 over x
    label "My label"
  plot p2 over x
    label 'Another'
"#);
        let items = &p.report.groups[0].items;
        assert_eq!(items[0].label.as_deref(), Some("My label"));
        assert_eq!(items[1].label.as_deref(), Some("Another"));
    }

    // ------------------------------------------------------------------
    // Round-trip emitter — Phase A SRD-64 contract test
    // ------------------------------------------------------------------
    //
    // For every report-grammar shape we accept, the emitter
    // must produce a string that re-parses to an equal AST.
    // That's the contract that lets `--add` write the same
    // grammar back to YAML faithfully.

    fn round_trip_via_group(item: &ReportItem) -> ReportItem {
        let group_body = item.to_yaml_directive_string();
        let yaml = format!("g: |\n{}",
            group_body.lines().map(|l| format!("  {l}")).collect::<Vec<_>>().join("\n"));
        let parsed = parse(&yaml);
        assert_eq!(parsed.report.groups.len(), 1,
            "round-trip should yield one group, got: {parsed:#?}");
        let items = &parsed.report.groups[0].items;
        assert_eq!(items.len(), 1,
            "round-trip should yield one item, got: {items:#?}");
        items[0].clone()
    }

    fn assert_round_trip_eq(item: ReportItem) {
        let recovered = round_trip_via_group(&item);
        // Compare individual fields for clearer diffs on failure.
        assert_eq!(recovered.kind, item.kind);
        assert_eq!(recovered.name, item.name);
        assert_eq!(recovered.label, item.label);
        assert_eq!(recovered.as_stem, item.as_stem);
        assert_eq!(recovered.style.palette, item.style.palette);
        assert_eq!(recovered.style.line, item.style.line);
        assert_eq!(recovered.style.width, item.style.width);
        assert_eq!(recovered.style.marker, item.style.marker);
        assert_eq!(recovered.style.size, item.style.size);
        assert_eq!(recovered.style.color, item.style.color);
        assert_eq!(recovered.style.figure_width, item.style.figure_width);
        assert_eq!(recovered.style.figure_height, item.style.figure_height);
        assert_eq!(recovered.style.series.len(), item.style.series.len());
        for (a, b) in recovered.style.series.iter().zip(&item.style.series) {
            assert_eq!(a.key, b.key);
            assert_eq!(a.value, b.value);
        }
        // Body comparison is whitespace-tolerant: the emitter
        // re-indents to canonical 2-space, the parser strips
        // leading whitespace anyway.
        let normalize = |s: &str| s.split('\n')
            .map(|l| l.trim()).filter(|l| !l.is_empty())
            .collect::<Vec<_>>().join("\n");
        assert_eq!(normalize(&recovered.body), normalize(&item.body));
    }

    #[test]
    fn round_trip_minimal_plot() {
        let item = ReportItem {
            kind: Kind::Plot,
            name: "demo".to_string(),
            body: "over cycle\nmetric=throughput".to_string(),
            ..Default::default()
        };
        assert_round_trip_eq(item);
    }

    #[test]
    fn round_trip_plot_with_label_and_palette() {
        let mut item = ReportItem {
            kind: Kind::Plot,
            name: "recall".to_string(),
            label: Some("Recall@10".to_string()),
            body: "over limit\nby profile".to_string(),
            ..Default::default()
        };
        item.style.palette = Some("wong".to_string());
        assert_round_trip_eq(item);
    }

    #[test]
    fn round_trip_plot_full_style() {
        let mut item = ReportItem {
            kind: Kind::Plot,
            name: "full".to_string(),
            label: Some("Full".to_string()),
            as_stem: Some("plot_full".to_string()),
            body: "over limit\nby profile\nwhere dataset=glove\nagg=mean"
                .to_string(),
            ..Default::default()
        };
        item.style.palette = Some("tol_muted".to_string());
        item.style.line = Some("dashed".to_string());
        item.style.width = Some(2.0);
        item.style.marker = Some("circle".to_string());
        item.style.size = Some(4.0);
        item.style.color = Some("#117733".to_string());
        item.style.figure_width = Some(800);
        item.style.figure_height = Some(600);
        assert_round_trip_eq(item);
    }

    #[test]
    fn round_trip_table() {
        let mut item = ReportItem {
            kind: Kind::Table,
            name: "summary".to_string(),
            label: Some("Summary".to_string()),
            body: "metric=recall@.*\ngroup_by=profile".to_string(),
            ..Default::default()
        };
        item.style.palette = Some("wong".to_string());
        assert_round_trip_eq(item);
    }

    #[test]
    fn round_trip_with_series_overrides() {
        let mut item = ReportItem {
            kind: Kind::Plot,
            name: "perseries".to_string(),
            body: "over limit\nby profile".to_string(),
            ..Default::default()
        };
        let mut s1 = Style::default();
        s1.line = Some("dashed".to_string());
        s1.marker = Some("triangle".to_string());
        item.style.series.push(SeriesOverride {
            key: "profile".to_string(),
            value: "hnsw".to_string(),
            style: s1,
        });
        let mut s2 = Style::default();
        s2.line = Some("solid".to_string());
        item.style.series.push(SeriesOverride {
            key: "profile".to_string(),
            value: "ivf".to_string(),
            style: s2,
        });
        let recovered = round_trip_via_group(&item);
        assert_eq!(recovered.style.series.len(), 2);
        assert_eq!(recovered.style.series[0].key, "profile");
        assert_eq!(recovered.style.series[0].value, "hnsw");
        assert_eq!(recovered.style.series[0].style.line.as_deref(), Some("dashed"));
        assert_eq!(recovered.style.series[0].style.marker.as_deref(), Some("triangle"));
        assert_eq!(recovered.style.series[1].value, "ivf");
        assert_eq!(recovered.style.series[1].style.line.as_deref(), Some("solid"));
    }

    #[test]
    fn round_trip_label_with_internal_quotes_is_escaped() {
        let item = ReportItem {
            kind: Kind::Plot,
            name: "tricky".to_string(),
            label: Some(r#"He said "hi""#.to_string()),
            body: "over cycle".to_string(),
            ..Default::default()
        };
        // The emitter must escape the inner quotes so the
        // parser sees a single label string. (The parser today
        // strips outer quotes only; if it doesn't honour the
        // escape, the test catches that as a real bug to fix.)
        let group_body = item.to_yaml_directive_string();
        assert!(group_body.contains(r#"label "He said \"hi\"""#),
            "emitter should escape inner quotes; got:\n{group_body}");
    }

    #[test]
    fn emitter_orders_directives_canonically() {
        // `as` comes before `label`; identity before style;
        // style (scalar fields) before body; body before
        // per-series style overrides. This is the canonical
        // order from vocab::ALL_DIRECTIVES so the round-trip
        // stays stable.
        let mut item = ReportItem {
            kind: Kind::Plot,
            name: "ordered".to_string(),
            label: Some("L".to_string()),
            as_stem: Some("S".to_string()),
            body: "over cycle".to_string(),
            ..Default::default()
        };
        item.style.palette = Some("wong".to_string());
        item.style.series.push(SeriesOverride {
            key: "k".to_string(),
            value: "v".to_string(),
            style: Style::default(),
        });
        let s = item.to_yaml_directive_string();
        let pos = |needle: &str| s.find(needle)
            .unwrap_or_else(|| panic!("missing {needle} in:\n{s}"));
        let pos_as     = pos("as S");
        let pos_label  = pos("label \"L\"");
        let pos_style  = pos("palette=wong");
        let pos_body   = pos("over cycle");
        let pos_per_series = pos("style k=v");
        assert!(pos_as < pos_label,    "as before label");
        assert!(pos_label < pos_style, "label before style");
        assert!(pos_style < pos_body,  "style before body");
        assert!(pos_body < pos_per_series,
            "body before per-series style overrides");
    }
}
