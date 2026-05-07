// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

// Module-level allow: the legacy `LIST_FLAGS` / `MATCH_FLAGS` /
// `SESSION_FLAGS` constants and the corresponding `*_all_flags`
// helpers + `metrics_command` are referenced only by
// `completion::metrics_node()`, which itself is in the legacy
// dead path retained for fallback (see `completion.rs` module
// comment). Kept so a future re-activation of the legacy tree
// finds the contracts intact.
#![allow(dead_code)]

//! `nbrs metrics list [<expr>]` and `nbrs metrics show [<expr>]`
//!
//! Reads `metric_instance` rows from the active session's
//! `metrics.db` and renders them as a hierarchical tree keyed
//! on metric family then label dimensions in declaration order.
//! `show` adds a one-line summary of the most recent
//! `sample_value` row per instance.
//!
//! Filter expression accepts:
//!   - bare glob on the family name: `recall*`
//!   - OpenMetrics-style label match: `recall@10.mean{k="10"}`
//!   - substring filter via `~`: `{profile=~label}` or
//!     `recall{profile=~label}`
//!
//! Honors the `--session <path>` umbrella (consumed at startup
//! via `apply_session_directory_at_startup`); the active db
//! defaults to `logs/latest/metrics.db`. `--db <path>` overrides.
//!
//! Output formats (`--format <name>` on `list` / `show`):
//!   - `plain`       hierarchical tree (default)
//!   - `json`        single pretty-printed object envelope
//!   - `jsonl`       one JSON object per instance, no envelope
//!   - `yaml`        single YAML document
//!   - `csv`         table with the union of label keys as columns
//!
//! Sink: `--tofile <path>`. When `--format` is omitted, the
//! file extension chooses (`.json`, `.jsonl`, `.yaml` / `.yml`,
//! `.csv`, `.txt`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Session-resolution flags consumed by the umbrella before
/// the subcommand parser sees them. Listed here so the
/// completion tree can offer them without re-typing the set.
pub const SESSION_FLAGS: &[&str] = &[
    "--session", "--session-name", "--session-path",
    "--session-reuse", "--session-keep", "--session-shelflife",
];

/// Canonical flag set for `nbrs metrics list` and `nbrs
/// metrics show`. Single source of truth — both this file's
/// parser and `completion::metrics_node` read from here, so
/// adding a flag is one edit, not two.
pub const LIST_FLAGS: &[&str] = &[
    "--db", "--format", "--tofile",
];

/// Boolean flags for `nbrs metrics list` / `show`. Same
/// single-source-of-truth contract as [`LIST_FLAGS`], but
/// these don't take a value.
pub const LIST_BOOL_FLAGS: &[&str] = &[
    "--tree",
];

/// Canonical flag set for `nbrs metrics match`.
pub const MATCH_FLAGS: &[&str] = &["--db"];

/// Closed value set for `--format`.
pub const FORMAT_VALUES: &[&str] = &[
    "plain", "json", "jsonl", "yaml", "csv",
    "metricsql", "openmetrics", "promql",
];

/// Concatenation helper: the full advertised flag list for a
/// given subcommand (kind-specific flags + session flags).
pub fn list_all_flags() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = LIST_FLAGS.iter().copied().collect();
    v.extend(SESSION_FLAGS.iter().copied());
    v
}

pub fn match_all_flags() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = MATCH_FLAGS.iter().copied().collect();
    v.extend(SESSION_FLAGS.iter().copied());
    v
}

// ── cli_spec entry ─────────────────────────────────────────
// Single source of truth for `nbrs metrics …` shape: the
// walker-driven parser, completion, and help all read this.
// Adding a flag here automatically surfaces in tab and in
// `nbrs metrics … --help`.

use crate::cli_spec::{
    Arity, Category, Command, Flag, Handler, Level,
    ValueProvider, ParsedCommand,
};

/// Closed-set value provider for `--format`. Returned values
/// match [`Format::parse`].
fn format_completer(partial: &str, _ctx: &[&str]) -> Vec<String> {
    FORMAT_VALUES.iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string())
        .collect()
}

fn list_or_show_flags() -> Vec<Flag> {
    vec![
        Flag {
            long: "--db", short: None, aliases: &[],
            arity: Arity::Value, value: ValueProvider::Path,
            help: "Override metrics db. Default: logs/latest/metrics.db",
            repeatable: false,
        },
        Flag {
            long: "--format", short: None, aliases: &[],
            arity: Arity::Value, value: ValueProvider::Custom(format_completer),
            help: "plain | json | jsonl | yaml | csv. Default: plain.",
            repeatable: false,
        },
        Flag {
            long: "--tofile", short: None, aliases: &["--to-file"],
            arity: Arity::Value, value: ValueProvider::Path,
            help: "Write to <path>. With no --format, extension picks.",
            repeatable: false,
        },
        Flag {
            long: "--tree", short: None, aliases: &[],
            arity: Arity::Bool, value: ValueProvider::None,
            help: "Reshape json/yaml as nested key=value tree.",
            repeatable: false,
        },
    ]
}

fn match_flags() -> Vec<Flag> {
    vec![Flag {
        long: "--db", short: None, aliases: &[],
        arity: Arity::Value, value: ValueProvider::Path,
        help: "Override metrics db. Default: logs/latest/metrics.db",
        repeatable: false,
    }]
}

pub fn spec() -> Command {
    // No-subcommand handler: print usage. The walker lands
    // here when the user types `nbrs metrics` or
    // `nbrs metrics --bogus` — `raw_args: true` lets us absorb
    // any tail and the handler decides how to react.
    fn handle_bare(_p: ParsedCommand) -> Result<(), String> {
        print_metrics_usage();
        Ok(())
    }
    Command {
        name: "metrics",
        help: "Read-side introspection over a session's metrics db.",
        category: Category::Tools,
        level: Level::Secondary,
        flags: Vec::new(),
        positionals: Vec::new(),
        handler: Some(Handler::Sync(handle_bare)),
        raw_args: true,
        completion_override: None,
        subcommands: vec![
            Command {
                name: "list",
                help: "List metric families and label dimensions.",
                category: Category::Tools, level: Level::Secondary,
                flags: list_or_show_flags(),
                positionals: vec![crate::cli_spec::Positional {
                    name: "expr",
                    help: "Optional filter (family-glob or `family{labels}`).",
                    kind: crate::cli_spec::PositionalKind::ZeroOrOne,
                }],
                subcommands: Vec::new(),
                handler: Some(Handler::Sync(handle_list)),
                raw_args: false,
                completion_override: None,
            },
            Command {
                name: "show",
                help: "Same as `list` plus a value summary at each leaf.",
                category: Category::Tools, level: Level::Secondary,
                flags: list_or_show_flags(),
                positionals: vec![crate::cli_spec::Positional {
                    name: "expr",
                    help: "Optional filter (family-glob or `family{labels}`).",
                    kind: crate::cli_spec::PositionalKind::ZeroOrOne,
                }],
                subcommands: Vec::new(),
                handler: Some(Handler::Sync(handle_show)),
                raw_args: false,
                completion_override: None,
            },
            Command {
                name: "match",
                help: "Flat list of `family{labels}` specs matching <expr>.",
                category: Category::Tools, level: Level::Secondary,
                flags: match_flags(),
                positionals: vec![crate::cli_spec::Positional {
                    name: "expr",
                    help: "Filter expression (required).",
                    kind: crate::cli_spec::PositionalKind::One,
                }],
                subcommands: Vec::new(),
                handler: Some(Handler::Sync(handle_match)),
                raw_args: false,
                completion_override: None,
            },
            // metricsql query/watch keep their existing parsers
            // (they have a richer flag surface — duration, --at,
            // streaming options). raw_args=true here so the spec
            // documents the subcommand without the walker
            // attempting to second-guess metricsql_cmd's parser.
            Command {
                name: "query",
                help: "Evaluate a metricsql expression against the db.",
                category: Category::Tools, level: Level::Secondary,
                flags: Vec::new(),
                positionals: Vec::new(),
                subcommands: Vec::new(),
                handler: Some(Handler::Sync(handle_query)),
                raw_args: true,
                completion_override: None,
            },
            Command {
                name: "watch",
                help: "Live-update a metricsql expression on a polling interval.",
                category: Category::Tools, level: Level::Secondary,
                flags: Vec::new(),
                positionals: Vec::new(),
                subcommands: Vec::new(),
                handler: Some(Handler::Sync(handle_watch)),
                raw_args: true,
                completion_override: None,
            },
        ],
    }
}

// ── handlers ──────────────────────────────────────────────

fn handle_list(p: ParsedCommand) -> Result<(), String> {
    list_from_parsed(&p, false);
    Ok(())
}

fn handle_show(p: ParsedCommand) -> Result<(), String> {
    list_from_parsed(&p, true);
    Ok(())
}

fn handle_match(p: ParsedCommand) -> Result<(), String> {
    // Reconstruct an arg list compatible with the existing
    // `match_specs` parser. Cheaper than rewriting the
    // imperative match_specs body to consume ParsedCommand
    // directly — both surfaces share the same flag set, so
    // the walker has already validated unknown flags upstream.
    let mut argv: Vec<String> = Vec::new();
    if let Some(db) = p.flag("--db") {
        argv.push("--db".into());
        argv.push(db.to_string());
    }
    if let Some(expr) = p.positional(0) {
        argv.push(expr.to_string());
    }
    match_specs(&argv);
    Ok(())
}

fn handle_query(p: ParsedCommand) -> Result<(), String> {
    crate::metricsql_cmd::query(&p.raw);
    Ok(())
}

fn handle_watch(p: ParsedCommand) -> Result<(), String> {
    crate::metricsql_cmd::watch(&p.raw);
    Ok(())
}

/// Walker-driven entry into the rendering pipeline. Reads
/// every option from [`ParsedCommand`] — the walker has
/// already accepted the flag set against
/// [`list_or_show_flags`] so this body never touches argv
/// strings.
fn list_from_parsed(p: &ParsedCommand, show_values: bool) {
    let mut argv: Vec<String> = Vec::new();
    if let Some(db) = p.flag("--db") {
        argv.push("--db".into());
        argv.push(db.to_string());
    }
    if let Some(fmt) = p.flag("--format") {
        argv.push("--format".into());
        argv.push(fmt.to_string());
    }
    if let Some(file) = p.flag("--tofile") {
        argv.push("--tofile".into());
        argv.push(file.to_string());
    }
    if p.bool("--tree") {
        argv.push("--tree".into());
    }
    if let Some(expr) = p.positional(0) {
        argv.push(expr.to_string());
    }
    list(&argv, show_values);
}

pub fn metrics_command(args: &[String]) {
    let sub = args.first().map(String::as_str);
    let rest = args.get(1..).unwrap_or(&[]);
    match sub {
        Some("list") => list(rest, false),
        Some("show") => list(rest, true),
        Some("match") => match_specs(rest),
        Some("query") => crate::metricsql_cmd::query(rest),
        Some("watch") => crate::metricsql_cmd::watch(rest),
        Some(other) => {
            eprintln!("nbrs metrics: unknown subcommand '{other}'");
            print_metrics_usage();
            std::process::exit(2);
        }
        None => {
            eprintln!("nbrs metrics: missing subcommand");
            print_metrics_usage();
            std::process::exit(2);
        }
    }
}

fn print_metrics_usage() {
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  nbrs metrics list  [<expr>]  List metric families + dimensions");
    eprintln!("                               as a tree.");
    eprintln!("  nbrs metrics show  [<expr>]  Same as `list` plus a one-line");
    eprintln!("                               value summary at each leaf.");
    eprintln!("  nbrs metrics match  <expr>   Flat list of full");
    eprintln!("                               `family{{labels}}` specs that");
    eprintln!("                               match — copy-paste into other");
    eprintln!("                               commands or sanity-check a");
    eprintln!("                               filter pattern.");
    eprintln!("  nbrs metrics query  <expr>   Evaluate a metricsql");
    eprintln!("                               expression against the db.");
    eprintln!("                               Run `nbrs metrics query` with");
    eprintln!("                               no args for full flag list.");
    eprintln!("  nbrs metrics watch  <expr>   Live-update a metricsql");
    eprintln!("                               expression on a polling");
    eprintln!("                               interval. Uses the streaming");
    eprintln!("                               engine when supported, batch");
    eprintln!("                               eval otherwise.");
    eprintln!();
    eprintln!("Filter expressions:");
    eprintln!("  recall*                      Family-name glob.");
    eprintln!("  recall@10.mean{{k=\"10\"}}      OpenMetrics-style label match.");
    eprintln!("  {{profile=~label}}             Substring filter.");
    eprintln!();
    eprintln!("Source selection:");
    eprintln!("  --db <path>                  Override metrics db.");
    eprintln!("                               Default: logs/latest/metrics.db");
    eprintln!("  --session <path-or-name>     SRD-04 umbrella; redirects");
    eprintln!("                               logs/latest before reading.");
    eprintln!();
    eprintln!("Output (list / show):");
    eprintln!("  --format <name>              plain | json | jsonl | yaml | csv");
    eprintln!("                               | metricsql");
    eprintln!("                               Default: plain. `metricsql`");
    eprintln!("                               (aliases: openmetrics / promql /");
    eprintln!("                               prometheus) emits one OpenMetrics");
    eprintln!("                               line per instance —");
    eprintln!("                               family{{key=\"value\",…}} — with");
    eprintln!("                               labels in natural alphanumeric");
    eprintln!("                               order so every line shares the");
    eprintln!("                               same key sequence (copy-paste");
    eprintln!("                               into match/query/PromQL tools).");
    eprintln!("  --tofile <path>              Write to <path>. With no");
    eprintln!("                               --format, the extension chooses");
    eprintln!("                               (.json, .jsonl, .yaml, .csv,");
    eprintln!("                               .om / .openmetrics / .promql /");
    eprintln!("                               .metricsql → metricsql,");
    eprintln!("                               .txt → plain).");
    eprintln!("  --tree                       Reshape json/yaml output into a");
    eprintln!("                               nested map keyed family →");
    eprintln!("                               constants + dim tree. Roll up by");
    eprintln!("                               family with `jq '.families.<n>'`.");
    eprintln!("                               Implies --format yaml when used");
    eprintln!("                               alone; rejects csv / jsonl /");
    eprintln!("                               metricsql.");
}

/// `nbrs metrics match <expr>` — print a flat list of every
/// `family{labels}` spec that matches the filter, one per line.
/// Unlike `list`/`show` (which group hierarchically by label
/// dimension), `match` preserves the spec verbatim so the
/// output round-trips into other commands that take a fully
/// qualified metric instance reference.
fn match_specs(args: &[String]) {
    let mut db_path: Option<PathBuf> = None;
    let mut filter_expr: Option<String> = None;
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--db" => { db_path = iter.next().map(PathBuf::from); }
            other if other.starts_with("--db=") => {
                db_path = Some(PathBuf::from(&other[5..]));
            }
            "--session" | "--session-name" | "--session-path"
            | "--session-reuse" | "--session-keep" | "--session-shelflife" => {
                let _ = iter.next();
            }
            other if other.starts_with("--session") => {}
            other if !other.starts_with("--") => {
                filter_expr = Some(other.to_string());
            }
            other => {
                eprintln!("nbrs metrics match: unknown flag '{other}'");
                std::process::exit(2);
            }
        }
    }
    let Some(expr) = filter_expr else {
        eprintln!("nbrs metrics match: pattern required");
        eprintln!("  e.g. `nbrs metrics match 'recall*'`");
        eprintln!("       `nbrs metrics match 'recall@10.mean{{k=\"10\"}}'`");
        std::process::exit(2);
    };
    let filter = match parse_filter(&expr) {
        Ok(f) => f,
        Err(e) => { eprintln!("nbrs metrics match: filter: {e}"); std::process::exit(2); }
    };
    let db = db_path.unwrap_or_else(|| PathBuf::from("logs/latest/metrics.db"));
    if !db.exists() {
        eprintln!("nbrs metrics match: db not found at '{}'", db.display());
        std::process::exit(2);
    }
    let conn = match rusqlite::Connection::open(&db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("nbrs metrics match: open '{}': {e}", db.display());
            std::process::exit(2);
        }
    };
    let mut stmt = match conn.prepare(
        "SELECT spec FROM metric_instance ORDER BY spec"
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("nbrs metrics match: query: {e}");
            std::process::exit(2);
        }
    };
    let rows: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(0))
        .map(|it| it.flatten().collect())
        .unwrap_or_default();

    let mut count = 0;
    for spec in &rows {
        let (family, labels) = split_spec(spec);
        if filter.matches(&family, &labels) {
            println!("{spec}");
            count += 1;
        }
    }
    eprintln!("# {} match{} ({} total instances)",
        count,
        if count == 1 { "" } else { "es" },
        rows.len(),
    );
}

/// Chosen serialization for `nbrs metrics list` / `show`.
/// `Plain` is the hierarchical tree view; everything else is
/// machine-readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Plain,
    Json,
    Jsonl,
    Yaml,
    Csv,
    /// MetricsQL / OpenMetrics canonical line form:
    /// `family{key="value",key="value",…}` — one instance per
    /// line, label keys sorted in natural alphanumeric order so
    /// every line shares the same key sequence and pivots /
    /// diffs / line-by-line readers see consistent column
    /// alignment. Output is directly copy-pasteable into
    /// `nbrs metrics match` / `nbrs metrics query` /
    /// downstream PromQL-compatible tooling.
    MetricsQL,
}

impl Format {
    fn parse(name: &str) -> Result<Self, String> {
        match name.to_ascii_lowercase().as_str() {
            "plain" | "txt" | "text" | "tree" => Ok(Format::Plain),
            "json" => Ok(Format::Json),
            "jsonl" | "ndjson" => Ok(Format::Jsonl),
            "yaml" | "yml" => Ok(Format::Yaml),
            "csv" => Ok(Format::Csv),
            "metricsql" | "openmetrics" | "promql" | "prometheus"
                => Ok(Format::MetricsQL),
            other => Err(format!(
                "unknown format '{other}' (expected one of: \
                 plain, json, jsonl, yaml, csv, metricsql)"
            )),
        }
    }

    /// Pick a format from a file extension. `.txt` (and unknown
    /// extensions) map to `Plain`.
    fn from_extension(ext: &str) -> Self {
        match ext.to_ascii_lowercase().as_str() {
            "json"  => Format::Json,
            "jsonl" | "ndjson" => Format::Jsonl,
            "yaml" | "yml" => Format::Yaml,
            "csv"   => Format::Csv,
            "om" | "openmetrics" | "promql" | "metricsql"
                    => Format::MetricsQL,
            _ => Format::Plain,
        }
    }
}

/// Quote a label value for MetricsQL / OpenMetrics output.
/// Escapes per the OpenMetrics spec: `\` → `\\`, `"` → `\"`,
/// newline → `\n`. Bare other-bytes pass through.
fn escape_metricsql_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"'  => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Render flat instance rows as one OpenMetrics-canonical line
/// per instance: `family{key="value",key="value",…}`. Labels
/// within each line are sorted by [`cmp_natural`] (the
/// existing natural-alphanumeric comparator used elsewhere in
/// this module) so all lines share the same key sequence and
/// readers can rely on columnar alignment.
fn render_metricsql(
    w: &mut dyn std::io::Write,
    flat: &[InstanceRow],
) -> std::io::Result<()> {
    for row in flat {
        let mut labels: Vec<&(String, String)> = row.labels.iter().collect();
        labels.sort_by(|a, b| cmp_natural(&a.0, &b.0));
        write!(w, "{}", row.family)?;
        if !labels.is_empty() {
            write!(w, "{{")?;
            let mut first = true;
            for (k, v) in &labels {
                if !first { write!(w, ",")?; }
                first = false;
                write!(w, "{}={}", k, escape_metricsql_value(v))?;
            }
            write!(w, "}}")?;
        }
        writeln!(w)?;
    }
    Ok(())
}

/// One row in the structured output. `values` is populated only
/// when `show_values` is set (i.e., the user asked for `nbrs
/// metrics show`).
#[derive(Debug, Clone)]
struct InstanceRow {
    family: String,
    /// Labels in declaration order from the spec — preserves
    /// the same key sequence the producer used. The structured
    /// formats serialize this as an ordered map.
    labels: Vec<(String, String)>,
    values: Option<ValueSummary>,
}

#[derive(Debug, Clone, Default)]
struct ValueSummary {
    count:  Option<i64>,
    mean:   Option<f64>,
    p50:    Option<f64>,
    p99:    Option<f64>,
    min:    Option<f64>,
    max:    Option<f64>,
    stddev: Option<f64>,
    /// Earliest sample_value.timestamp_ms for this instance.
    /// Combined with [`ts_max_ms`] gives the span the summary
    /// covers.
    ts_min_ms: Option<i64>,
    ts_max_ms: Option<i64>,
}

fn list(args: &[String], show_values_in: bool) {
    let mut db_path: Option<PathBuf> = None;
    let mut filter_expr: Option<String> = None;
    let mut format_arg: Option<String> = None;
    let mut tofile: Option<PathBuf> = None;
    let mut tree_mode: bool = false;
    // `show_values` may be promoted by `--tree`: tree mode's
    // leaves *are* the value summary — null leaves carry no
    // information, so loading the summary is required for the
    // output to be useful. `list --tree` and `show --tree` thus
    // produce the same content.
    let mut show_values: bool = show_values_in;
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--db" => { db_path = iter.next().map(PathBuf::from); }
            other if other.starts_with("--db=") => {
                db_path = Some(PathBuf::from(&other[5..]));
            }
            "--format" => {
                format_arg = iter.next().cloned();
            }
            other if other.starts_with("--format=") => {
                format_arg = Some(other[9..].to_string());
            }
            "--tofile" | "--to-file" => {
                tofile = iter.next().map(PathBuf::from);
            }
            other if other.starts_with("--tofile=") => {
                tofile = Some(PathBuf::from(&other[9..]));
            }
            other if other.starts_with("--to-file=") => {
                tofile = Some(PathBuf::from(&other[10..]));
            }
            "--tree" => { tree_mode = true; show_values = true; }
            // Globals consumed at startup.
            "--session" | "--session-name" | "--session-path"
            | "--session-reuse" | "--session-keep" | "--session-shelflife" => {
                let _ = iter.next();
            }
            other if other.starts_with("--session") => {}
            other if !other.starts_with("--") => {
                filter_expr = Some(other.to_string());
            }
            other => {
                eprintln!("nbrs metrics: unknown flag '{other}'");
                std::process::exit(2);
            }
        }
    }

    // Resolve format. Explicit `--format` wins; otherwise derive
    // from the `--tofile` extension; otherwise default to plain
    // — except `--tree` alone implies yaml (since plain already
    // is a tree view).
    let format = match (format_arg.as_deref(), tofile.as_deref()) {
        (Some(name), _) => match Format::parse(name) {
            Ok(f) => f,
            Err(e) => { eprintln!("nbrs metrics: {e}"); std::process::exit(2); }
        },
        (None, Some(path)) => path.extension()
            .and_then(|s| s.to_str())
            .map(Format::from_extension)
            .unwrap_or(Format::Plain),
        (None, None) if tree_mode => Format::Yaml,
        (None, None) => Format::Plain,
    };

    // `--tree` reshapes structured output into a hierarchical
    // map keyed by family → label-key → label-value → … Only
    // json/yaml carry that structure; csv is by definition
    // tabular, jsonl is line-delimited, plain is already a
    // tree (its own renderer). Reject the combinations that
    // can't carry hierarchy rather than silently falling back.
    if tree_mode && !matches!(format, Format::Json | Format::Yaml) {
        eprintln!(
            "nbrs metrics: --tree requires --format json or yaml \
             (csv/jsonl/metricsql are flat; plain is already a tree)"
        );
        std::process::exit(2);
    }

    let db = db_path.unwrap_or_else(|| PathBuf::from("logs/latest/metrics.db"));
    if !db.exists() {
        eprintln!("nbrs metrics: db not found at '{}'", db.display());
        std::process::exit(2);
    }
    let conn = match rusqlite::Connection::open(&db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("nbrs metrics: open '{}': {e}", db.display());
            std::process::exit(2);
        }
    };
    let filter = filter_expr.as_deref().map(parse_filter).transpose();
    let filter = match filter {
        Ok(f) => f,
        Err(e) => { eprintln!("nbrs metrics: filter: {e}"); std::process::exit(2); }
    };
    let mut stmt = match conn.prepare(
        "SELECT mi.id, mi.spec FROM metric_instance mi ORDER BY mi.spec"
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("nbrs metrics: query: {e}");
            std::process::exit(2);
        }
    };
    let rows: Vec<(i64, String)> = stmt.query_map([], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
    }).map(|it| it.flatten().collect()).unwrap_or_default();

    // Bucket by family then by sorted label tuple. Used by the
    // tree renderer; structured renderers walk the flat list.
    let mut tree: BTreeMap<String, BTreeMap<Vec<(String, String)>, i64>> = BTreeMap::new();
    let mut flat: Vec<InstanceRow> = Vec::new();
    for (id, spec) in &rows {
        let (family, labels) = split_spec(spec);
        if let Some(f) = filter.as_ref() {
            if !f.matches(&family, &labels) { continue; }
        }
        let mut sorted = labels.clone();
        sorted.sort();
        tree.entry(family.clone()).or_default().insert(sorted, *id);
        let values = if show_values {
            Some(load_value_summary(&conn, *id))
        } else {
            None
        };
        flat.push(InstanceRow { family, labels, values });
    }

    // Natural-order sort the flat list: family by natural compare
    // (so `recall@2.mean` < `recall@10.mean`), then by label
    // values so two instances with `k="2"` / `k="10"` come out 2,
    // 10 — not lex 10, 2. The downstream structured renderers
    // (json/jsonl/yaml/csv) walk this Vec in order and inherit
    // the ordering as-is.
    flat.sort_by(|a, b| {
        cmp_natural(&a.family, &b.family)
            .then_with(|| cmp_label_pairs_natural(&a.labels, &b.labels))
    });

    if tree.is_empty() {
        let msg = match filter_expr {
            Some(ref e) => format!("(no metrics matching '{e}')"),
            None => "(no metrics)".to_string(),
        };
        // Empty result still goes through the chosen sink so
        // callers piping into a file get a valid empty document
        // rather than a stderr message they can't capture.
        match (format, tree_mode) {
            (Format::Plain, _) => emit(&tofile, format, |w| {
                writeln!(w, "{msg}")
            }),
            (Format::Json, false) => emit(&tofile, format, |w| {
                writeln!(w, "{}", render_json(&db, &flat, show_values))
            }),
            (Format::Json, true) => emit(&tofile, format, |w| {
                writeln!(w, "{}", render_tree_json(&db, &flat, show_values))
            }),
            (Format::Jsonl, _) => emit(&tofile, format, |_w| Ok(())),
            (Format::Yaml, false) => emit(&tofile, format, |w| {
                writeln!(w, "{}", render_yaml(&db, &flat, show_values))
            }),
            (Format::Yaml, true) => emit(&tofile, format, |w| {
                writeln!(w, "{}", render_tree_yaml(&db, &flat, show_values))
            }),
            (Format::Csv, _) => emit(&tofile, format, |w| {
                writeln!(w, "{}", render_csv_header(&flat, show_values))
            }),
            (Format::MetricsQL, _) => emit(&tofile, format, |_w| Ok(())),
        }
        return;
    }

    match (format, tree_mode) {
        (Format::Plain, _) => emit(&tofile, format, |w| {
            render_plain(w, &db, &tree, &rows, &conn, show_values)
        }),
        (Format::Json, false) => emit(&tofile, format, |w| {
            writeln!(w, "{}", render_json(&db, &flat, show_values))
        }),
        (Format::Json, true) => emit(&tofile, format, |w| {
            writeln!(w, "{}", render_tree_json(&db, &flat, show_values))
        }),
        (Format::Jsonl, _) => emit(&tofile, format, |w| {
            for row in &flat {
                writeln!(w, "{}", row_to_json(row, show_values))?;
            }
            Ok(())
        }),
        (Format::Yaml, false) => emit(&tofile, format, |w| {
            writeln!(w, "{}", render_yaml(&db, &flat, show_values))
        }),
        (Format::Yaml, true) => emit(&tofile, format, |w| {
            writeln!(w, "{}", render_tree_yaml(&db, &flat, show_values))
        }),
        (Format::Csv, _) => emit(&tofile, format, |w| {
            render_csv(w, &flat, show_values)
        }),
        (Format::MetricsQL, _) => emit(&tofile, format, |w| {
            render_metricsql(w, &flat)
        }),
    }
}

/// Run `f` against either the chosen `--tofile` sink or stdout.
/// I/O errors abort with a non-zero exit; the caller's closure
/// can return `io::Error` directly.
fn emit<F>(tofile: &Option<PathBuf>, _format: Format, f: F)
where F: FnOnce(&mut dyn std::io::Write) -> std::io::Result<()> {
    let result = match tofile {
        Some(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    let _ = std::fs::create_dir_all(parent);
                }
            }
            match std::fs::File::create(path) {
                Ok(file) => {
                    let mut bw = std::io::BufWriter::new(file);
                    let r = f(&mut bw);
                    r.and_then(|_| std::io::Write::flush(&mut bw))
                }
                Err(e) => {
                    eprintln!("nbrs metrics: open '{}': {e}", path.display());
                    std::process::exit(2);
                }
            }
        }
        None => {
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            f(&mut lock)
        }
    };
    if let Err(e) = result {
        eprintln!("nbrs metrics: write: {e}");
        std::process::exit(2);
    }
}

fn render_plain(
    w: &mut dyn std::io::Write,
    db: &Path,
    tree: &BTreeMap<String, BTreeMap<Vec<(String, String)>, i64>>,
    rows: &[(i64, String)],
    conn: &rusqlite::Connection,
    show_values: bool,
) -> std::io::Result<()> {
    writeln!(w, "# {} ({} famil{}, {} instance{})",
        db.display(),
        tree.len(),
        if tree.len() == 1 { "y" } else { "ies" },
        rows.len(),
        if rows.len() == 1 { "" } else { "s" },
    )?;
    // Natural-order family iteration — `recall@2.mean` before
    // `recall@10.mean`. Re-sort the BTreeMap key list rather
    // than swapping the bucket type (BTreeMap is doing the
    // grouping work, just not the final ordering).
    let mut family_names: Vec<&String> = tree.keys().collect();
    family_names.sort_by(|a, b| cmp_natural(a, b));
    for family in family_names {
        let instances = tree.get(family).unwrap();
        writeln!(w)?;
        // Collapse dimensions that have a single shared value
        // across every instance under this family — they're
        // not adding information to the tree, just noise.
        // Print them once at the family-level header.
        let label_sets: Vec<Vec<(String, String)>> = instances.keys().cloned().collect();
        let (constant_dims, varying_label_sets) = factor_constant_dims(&label_sets);
        if constant_dims.is_empty() {
            writeln!(w, "{family}")?;
        } else {
            let const_str = constant_dims.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>().join(", ");
            writeln!(w, "{family}  [{const_str}]")?;
        }
        let varying_instances: BTreeMap<Vec<(String, String)>, i64> = instances.iter()
            .zip(varying_label_sets.iter())
            .map(|((_, id), labels)| (labels.clone(), *id))
            .collect();
        let dim_tree = build_dim_tree(varying_label_sets);
        write_dim_tree(w, &dim_tree, "  ", &varying_instances, conn, show_values)?;
    }
    Ok(())
}

fn row_to_json(row: &InstanceRow, show_values: bool) -> String {
    let labels: serde_json::Map<String, serde_json::Value> = row.labels.iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    let mut obj = serde_json::Map::new();
    obj.insert("family".into(), serde_json::Value::String(row.family.clone()));
    obj.insert("labels".into(), serde_json::Value::Object(labels));
    if show_values {
        let v = row.values.clone().unwrap_or_default();
        let mut vobj = serde_json::Map::new();
        if let Some(c) = v.count { vobj.insert("count".into(), serde_json::json!(c)); }
        if let Some(m) = v.mean  { vobj.insert("mean".into(),  serde_json::json!(m)); }
        if let Some(m) = v.p50   { vobj.insert("p50".into(),   serde_json::json!(m)); }
        if let Some(m) = v.p99   { vobj.insert("p99".into(),   serde_json::json!(m)); }
        if let Some(m) = v.min   { vobj.insert("min".into(),   serde_json::json!(m)); }
        if let Some(m) = v.max   { vobj.insert("max".into(),   serde_json::json!(m)); }
        obj.insert("values".into(), serde_json::Value::Object(vobj));
    }
    serde_json::Value::Object(obj).to_string()
}

fn render_json(db: &Path, flat: &[InstanceRow], show_values: bool) -> String {
    let instances: Vec<serde_json::Value> = flat.iter()
        .map(|r| serde_json::from_str(&row_to_json(r, show_values))
            .unwrap_or(serde_json::Value::Null))
        .collect();
    let envelope = serde_json::json!({
        "db": db.display().to_string(),
        "count": flat.len(),
        "instances": instances,
    });
    serde_json::to_string_pretty(&envelope)
        .unwrap_or_else(|_| "{}".to_string())
}

fn render_yaml(db: &Path, flat: &[InstanceRow], show_values: bool) -> String {
    let json = render_json(db, flat, show_values);
    let v: serde_json::Value = serde_json::from_str(&json)
        .unwrap_or(serde_json::Value::Null);
    serde_yaml::to_string(&v).unwrap_or_default()
}

/// Build a tree-shaped json document keyed `family → constants
/// + nested label tree`. Lets the user roll up by family with
/// `jq '.families."recall@10.mean"'` (or the yaml equivalent)
/// and walk the dim tree structurally instead of by-row.
fn render_tree_json(db: &Path, flat: &[InstanceRow], show_values: bool) -> String {
    let value = build_tree_value(db, flat, show_values);
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string())
}

fn render_tree_yaml(db: &Path, flat: &[InstanceRow], show_values: bool) -> String {
    let value = build_tree_value(db, flat, show_values);
    serde_yaml::to_string(&value).unwrap_or_default()
}

fn build_tree_value(db: &Path, flat: &[InstanceRow], show_values: bool) -> serde_json::Value {
    let mut by_family: BTreeMap<String, Vec<&InstanceRow>> = BTreeMap::new();
    for row in flat {
        by_family.entry(row.family.clone()).or_default().push(row);
    }
    // Walk families in natural-sort order so `recall@2.mean`
    // emits before `recall@10.mean`. (BTreeMap is lex-sorted —
    // re-sort the key list instead of switching the bucket
    // type.)
    let mut family_names: Vec<String> = by_family.keys().cloned().collect();
    family_names.sort_by(|a, b| cmp_natural(a, b));
    let mut families = serde_json::Map::new();
    for name in family_names {
        let rows = by_family.remove(&name).unwrap_or_default();
        families.insert(name, build_family_tree(&rows, show_values));
    }
    serde_json::json!({
        "db": db.display().to_string(),
        "families": serde_json::Value::Object(families),
    })
}

/// Build the tree for one family: every label key contributes
/// a level, regardless of how many distinct values it carries
/// in the current result set. Labels with one value just have
/// one branch under that level — uniform traversal beats
/// factoring "constants" out into a separate sibling map.
///
/// Labels are sorted by key per-instance first so siblings at
/// the same depth share the same label key (canonical order),
/// even if the producer declared labels in different orders
/// across instances.
fn build_family_tree(rows: &[&InstanceRow], show_values: bool) -> serde_json::Value {
    let normalized: Vec<(Vec<(String, String)>, Option<ValueSummary>)> = rows.iter()
        .map(|r| {
            let mut s = r.labels.clone();
            s.sort_by(|a, b| a.0.cmp(&b.0));
            (s, r.values.clone())
        })
        .collect();

    nest_label_tree(&normalized, show_values)
}

/// Recursive nester. At each level, groups rows by their next
/// `(label_key, label_value)` pair and emits the branch as a
/// single `"key=value"` map key — one level per dimension
/// instead of alternating key-level / value-level layers.
/// Leaves (no labels left) emit `{count, mean, …}` in `show`
/// mode, `null` in `list` mode.
fn nest_label_tree(
    rows: &[(Vec<(String, String)>, Option<ValueSummary>)],
    show_values: bool,
) -> serde_json::Value {
    if rows.is_empty() {
        return serde_json::Value::Null;
    }
    if rows.iter().all(|(l, _)| l.is_empty()) {
        // Pure leaf — typical for the deepest level. Multiple
        // rows colliding here shouldn't happen unless the
        // metric instance set has duplicate label-tuples; if
        // it does, we take the first row's values (BTreeMap
        // dedup should have removed duplicates upstream anyway).
        if show_values {
            let v = rows[0].1.clone().unwrap_or_default();
            return serde_json::Value::String(tree_leaf_summary(&v));
        }
        return serde_json::Value::Null;
    }
    // Bucket rows by their first `(key, value)` pair. The
    // BTreeMap is just for grouping; we re-sort the key tuples
    // naturally before emitting so `limit=2` precedes
    // `limit=10`.
    let mut by_kv: BTreeMap<(String, String), Vec<(Vec<(String, String)>, Option<ValueSummary>)>>
        = BTreeMap::new();
    for (labels, values) in rows {
        if labels.is_empty() {
            // Mixed-arity case: some siblings have ended their
            // dim list while others continue. Skip — the
            // emitted tree won't carry this row, but it's a
            // pathological label-shape that doesn't appear in
            // OpenMetrics-conforming families. Surface as
            // structural drift later if it ever does.
            continue;
        }
        let (k, v) = labels[0].clone();
        let rest: Vec<(String, String)> = labels[1..].to_vec();
        by_kv.entry((k, v)).or_default()
            .push((rest, values.clone()));
    }
    let mut sorted_kvs: Vec<(String, String)> = by_kv.keys().cloned().collect();
    sorted_kvs.sort_by(|a, b| {
        cmp_natural(&a.0, &b.0).then_with(|| cmp_natural(&a.1, &b.1))
    });
    let mut out = serde_json::Map::new();
    for kv in sorted_kvs {
        let subset = by_kv.remove(&kv).unwrap_or_default();
        let key = format!("{}={}", kv.0, kv.1);
        out.insert(key, nest_label_tree(&subset, show_values));
    }
    serde_json::Value::Object(out)
}

/// One-line text summary used at `--tree` leaves: a compact,
/// scannable encoding of count / time range / canonical
/// statistical moments. Format:
/// `samples[N] timespan[<duration>] (min,mean,max,median,stddev)=(...)`.
/// Unknown fields render as `?` so the format stays positional
/// (a reader can `cut`/`awk` it without a header).
fn tree_leaf_summary(v: &ValueSummary) -> String {
    let n = v.count.map(|c| c.to_string()).unwrap_or_else(|| "?".into());
    let span = match (v.ts_min_ms, v.ts_max_ms) {
        (Some(a), Some(b)) if b >= a => format_duration_ms(b - a),
        _ => "?".into(),
    };
    let f = |x: Option<f64>| x.map(|v| format!("{v}")).unwrap_or_else(|| "?".into());
    format!(
        "samples[{n}] timespan[{span}] (min,mean,max,median,stddev)=({},{},{},{},{})",
        f(v.min), f(v.mean), f(v.max), f(v.p50), f(v.stddev),
    )
}

/// Render a millisecond duration as `HhMmSs` / `MmSs` / `Ss` /
/// `Nms` — the largest unit non-zero down to the next, dropping
/// trailing zero units. Stays compact at the leaf-summary level.
fn format_duration_ms(ms: i64) -> String {
    if ms < 1000 { return format!("{ms}ms"); }
    let total_s = ms / 1000;
    let h = total_s / 3600;
    let m = (total_s % 3600) / 60;
    let s = total_s % 60;
    if h > 0 {
        if m == 0 && s == 0 { format!("{h}h") }
        else if s == 0 { format!("{h}h{m}m") }
        else { format!("{h}h{m}m{s}s") }
    } else if m > 0 {
        if s == 0 { format!("{m}m") } else { format!("{m}m{s}s") }
    } else {
        format!("{s}s")
    }
}

fn render_csv_header(flat: &[InstanceRow], show_values: bool) -> String {
    let keys = union_label_keys(flat);
    let mut header: Vec<String> = vec!["family".into()];
    header.extend(keys.iter().cloned());
    if show_values {
        header.extend(["count", "mean", "p50", "p99", "min", "max"]
            .iter().map(|s| s.to_string()));
    }
    header.iter().map(|s| csv_escape(s)).collect::<Vec<_>>().join(",")
}

fn render_csv(
    w: &mut dyn std::io::Write,
    flat: &[InstanceRow],
    show_values: bool,
) -> std::io::Result<()> {
    let keys = union_label_keys(flat);
    writeln!(w, "{}", render_csv_header(flat, show_values))?;
    for row in flat {
        let label_map: BTreeMap<&str, &str> = row.labels.iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let mut cells: Vec<String> = vec![csv_escape(&row.family)];
        for k in &keys {
            cells.push(csv_escape(label_map.get(k.as_str()).copied().unwrap_or("")));
        }
        if show_values {
            let v = row.values.clone().unwrap_or_default();
            cells.push(v.count.map(|x| x.to_string()).unwrap_or_default());
            cells.push(v.mean .map(fmt_f64).unwrap_or_default());
            cells.push(v.p50  .map(fmt_f64).unwrap_or_default());
            cells.push(v.p99  .map(fmt_f64).unwrap_or_default());
            cells.push(v.min  .map(fmt_f64).unwrap_or_default());
            cells.push(v.max  .map(fmt_f64).unwrap_or_default());
        }
        writeln!(w, "{}", cells.join(","))?;
    }
    Ok(())
}

fn fmt_f64(x: f64) -> String {
    // Compact human form; CSV consumers parse this fine.
    format!("{x}")
}

fn union_label_keys(flat: &[InstanceRow]) -> Vec<String> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for row in flat {
        for (k, _) in &row.labels { keys.insert(k.clone()); }
    }
    keys.into_iter().collect()
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Natural-order compare for label keys/values: numeric runs are
/// compared by integer value (so `"2"` < `"10"` instead of the
/// lexicographic `"10"` < `"2"`), with non-numeric runs falling
/// back to byte-wise compare. Walks both strings in parallel,
/// chunking each into "all digits" / "all non-digits" segments
/// and comparing the corresponding segments numerically when
/// both are digit runs. Handles mixed strings like `"k10"` vs
/// `"k2"` correctly.
fn cmp_natural(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a = a.as_bytes();
    let b = b.as_bytes();
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        let a_digit = a[i].is_ascii_digit();
        let b_digit = b[j].is_ascii_digit();
        if a_digit && b_digit {
            // Find the digit-run boundaries on both sides.
            let ai = i;
            while i < a.len() && a[i].is_ascii_digit() { i += 1; }
            let bj = j;
            while j < b.len() && b[j].is_ascii_digit() { j += 1; }
            // Drop leading zeros so "007" == "7" numerically;
            // tiebreak on the original lengths so "07" < "7"
            // sorts the longer-with-leading-zeros after the
            // bare form (stable for IDs like "0001").
            let a_run = std::str::from_utf8(&a[ai..i]).unwrap_or("");
            let b_run = std::str::from_utf8(&b[bj..j]).unwrap_or("");
            let a_trim = a_run.trim_start_matches('0');
            let b_trim = b_run.trim_start_matches('0');
            // Compare by stripped length first (shorter ⇒ smaller),
            // then char-wise; this is the standard "natural" rule.
            match a_trim.len().cmp(&b_trim.len()) {
                Ordering::Equal => match a_trim.cmp(b_trim) {
                    Ordering::Equal => match a_run.len().cmp(&b_run.len()) {
                        Ordering::Equal => continue,
                        non_eq => return non_eq,
                    },
                    non_eq => return non_eq,
                },
                non_eq => return non_eq,
            }
        } else {
            match a[i].cmp(&b[j]) {
                Ordering::Equal => { i += 1; j += 1; }
                non_eq => return non_eq,
            }
        }
    }
    a.len().cmp(&b.len())
}

/// Natural-order tuple compare: walks `(key, value)` pairs in
/// parallel using [`cmp_natural`] on each field. Used to sort
/// instances within a family so two instances with `k="2"` /
/// `k="10"` come out as 2, 10 instead of 10, 2.
fn cmp_label_pairs_natural(
    a: &[(String, String)],
    b: &[(String, String)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (av, bv) in a.iter().zip(b.iter()) {
        match cmp_natural(&av.0, &bv.0) {
            Ordering::Equal => match cmp_natural(&av.1, &bv.1) {
                Ordering::Equal => continue,
                non_eq => return non_eq,
            },
            non_eq => return non_eq,
        }
    }
    a.len().cmp(&b.len())
}

/// Split a list of label sets into (constant dims, per-set
/// varying dims). A "constant dim" is a `(key, value)` pair
/// shared by every input set; those are factored out so the
/// tree depth reflects only the dimensions that actually vary.
fn factor_constant_dims(
    label_sets: &[Vec<(String, String)>],
) -> (Vec<(String, String)>, Vec<Vec<(String, String)>>) {
    if label_sets.is_empty() { return (Vec::new(), Vec::new()); }
    let first: BTreeMap<String, String> = label_sets[0].iter().cloned().collect();
    let mut shared: BTreeMap<String, String> = first;
    for set in &label_sets[1..] {
        let cur: BTreeMap<String, String> = set.iter().cloned().collect();
        shared.retain(|k, v| cur.get(k) == Some(v));
        if shared.is_empty() { break; }
    }
    let const_keys: std::collections::HashSet<String> = shared.keys().cloned().collect();
    let varying: Vec<Vec<(String, String)>> = label_sets.iter()
        .map(|s| s.iter()
            .filter(|(k, _)| !const_keys.contains(k))
            .cloned()
            .collect())
        .collect();
    let constant: Vec<(String, String)> = shared.into_iter().collect();
    (constant, varying)
}

#[derive(Debug, Clone, PartialEq)]
struct LabelMatcher {
    /// Family glob pattern (`*` allowed); `None` means match-all.
    family: Option<String>,
    /// Per-label match: equals, regex/substring (`~`), or
    /// just-presence.
    labels: Vec<(String, LabelMatch)>,
}

#[derive(Debug, Clone, PartialEq)]
enum LabelMatch {
    Equals(String),
    Substring(String),
}

impl LabelMatcher {
    fn matches(&self, family: &str, labels: &[(String, String)]) -> bool {
        if let Some(g) = self.family.as_deref()
            && !glob_matches(g, family) { return false; }
        for (k, want) in &self.labels {
            let v = labels.iter().find(|(lk, _)| lk == k).map(|(_, v)| v);
            let ok = match (v, want) {
                (Some(v), LabelMatch::Equals(e)) => v == e,
                (Some(v), LabelMatch::Substring(s)) => v.contains(s),
                (None, _) => false,
            };
            if !ok { return false; }
        }
        true
    }
}

/// Parse a metric filter expression. Accepted shapes:
///   - `family_glob`
///   - `family_glob{label="value", label2=~substring}`
///   - `{label="value"}` (label-only filter, any family)
fn parse_filter(expr: &str) -> Result<LabelMatcher, String> {
    let expr = expr.trim();
    let (family_part, labels_part) = match expr.find('{') {
        Some(i) => (expr[..i].trim(), Some(expr[i..].to_string())),
        None => (expr, None),
    };
    let family = if family_part.is_empty() { None } else { Some(family_part.to_string()) };
    let mut labels: Vec<(String, LabelMatch)> = Vec::new();
    if let Some(lp) = labels_part {
        let lp = lp.trim();
        let inner = lp.strip_prefix('{').and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| "label block must be `{...}`".to_string())?;
        for raw in inner.split(',') {
            let raw = raw.trim();
            if raw.is_empty() { continue; }
            let (key, op_val) = if let Some((k, v)) = raw.split_once("=~") {
                (k.trim().to_string(), LabelMatch::Substring(unquote(v.trim()).to_string()))
            } else if let Some((k, v)) = raw.split_once('=') {
                (k.trim().to_string(), LabelMatch::Equals(unquote(v.trim()).to_string()))
            } else if let Some((k, v)) = raw.split_once('~') {
                (k.trim().to_string(), LabelMatch::Substring(unquote(v.trim()).to_string()))
            } else {
                return Err(format!("label clause '{raw}': expected `key=value` or `key=~substring`"));
            };
            labels.push((key, op_val));
        }
    }
    Ok(LabelMatcher { family, labels })
}

fn unquote(s: &str) -> &str {
    s.strip_prefix('"').and_then(|x| x.strip_suffix('"'))
        .or_else(|| s.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')))
        .unwrap_or(s)
}

fn glob_matches(glob: &str, name: &str) -> bool {
    fn rec(g: &[u8], n: &[u8]) -> bool {
        match (g.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => {
                if rec(&g[1..], n) { return true; }
                if !n.is_empty() && rec(g, &n[1..]) { return true; }
                false
            }
            (Some(b'?'), Some(_)) => rec(&g[1..], &n[1..]),
            (Some(gc), Some(nc)) if gc == nc => rec(&g[1..], &n[1..]),
            _ => false,
        }
    }
    rec(glob.as_bytes(), name.as_bytes())
}

pub(crate) fn split_spec(spec: &str) -> (String, Vec<(String, String)>) {
    let (family, labels_text) = match spec.find('{') {
        Some(i) => (spec[..i].to_string(), &spec[i + 1..]),
        None => return (spec.to_string(), Vec::new()),
    };
    let inner = labels_text.strip_suffix('}').unwrap_or(labels_text);
    let mut out = Vec::new();
    let bytes = inner.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b',') { i += 1; }
        if i >= bytes.len() { break; }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' { i += 1; }
        if i >= bytes.len() { break; }
        let key = inner[key_start..i].trim().to_string();
        i += 1;
        if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let vs = i;
            while i < bytes.len() && bytes[i] != b'"' { i += 1; }
            let val = inner[vs..i].to_string();
            if i < bytes.len() { i += 1; }
            out.push((key, val));
        } else {
            let vs = i;
            while i < bytes.len() && !matches!(bytes[i], b',') { i += 1; }
            out.push((key, inner[vs..i].trim().to_string()));
        }
    }
    (family, out)
}

/// One node in the dimensional tree. Ordered by label-key
/// occurrence so deeper levels reflect which dimensions vary.
#[derive(Debug, Default)]
struct DimNode {
    /// Distinct label-tuple paths reaching this node.
    leaves: Vec<Vec<(String, String)>>,
    /// Children keyed by `(label_key, label_value)`.
    children: BTreeMap<(String, String), DimNode>,
}

fn build_dim_tree(label_sets: Vec<Vec<(String, String)>>) -> DimNode {
    let mut root = DimNode::default();
    for ls in label_sets {
        insert_into_dim_tree(&mut root, &ls, 0);
    }
    root
}

fn insert_into_dim_tree(node: &mut DimNode, labels: &[(String, String)], depth: usize) {
    if depth >= labels.len() {
        node.leaves.push(labels.to_vec());
        return;
    }
    let (k, v) = &labels[depth];
    let child = node.children.entry((k.clone(), v.clone())).or_default();
    insert_into_dim_tree(child, labels, depth + 1);
}

fn write_dim_tree(
    w: &mut dyn std::io::Write,
    node: &DimNode,
    indent: &str,
    instances: &BTreeMap<Vec<(String, String)>, i64>,
    conn: &rusqlite::Connection,
    show_values: bool,
) -> std::io::Result<()> {
    // Iterate in natural order: by label key first, then by
    // value (so `k=2` comes before `k=10`). The underlying
    // BTreeMap is keyed (k, v); a lex iteration would put
    // `"10"` before `"2"`. Build the sorted key list once.
    let n_children = node.children.len();
    let mut sorted_keys: Vec<&(String, String)> = node.children.keys().collect();
    sorted_keys.sort_by(|a, b| {
        cmp_natural(&a.0, &b.0).then_with(|| cmp_natural(&a.1, &b.1))
    });
    for (idx, kv) in sorted_keys.iter().enumerate() {
        let (k, v) = (&kv.0, &kv.1);
        let child = node.children.get(*kv).unwrap();
        let is_last = idx + 1 == n_children;
        let connector = if is_last { "└── " } else { "├── " };
        let next_indent = if is_last {
            format!("{indent}    ")
        } else {
            format!("{indent}│   ")
        };
        // Leaf detection: any node with at least one leaf at
        // *this exact level* prints its summary inline.
        let inline_leaf: Option<&Vec<(String, String)>> = child.leaves.iter()
            .find(|ls| ls.last().map(|kv| kv == &(k.clone(), v.clone())).unwrap_or(false));

        if let Some(ls) = inline_leaf
            && child.children.is_empty() {
            let id = instances.get(ls).copied().unwrap_or(-1);
            let summary = if show_values {
                format!("  {}", value_summary_string(conn, id))
            } else {
                String::new()
            };
            writeln!(w, "{indent}{connector}{k}={v}{summary}")?;
        } else {
            writeln!(w, "{indent}{connector}{k}={v}")?;
            write_dim_tree(w, child, &next_indent, instances, conn, show_values)?;
        }
    }
    Ok(())
}

fn load_value_summary(conn: &rusqlite::Connection, instance_id: i64) -> ValueSummary {
    if instance_id < 0 { return ValueSummary::default(); }
    // Pull the highest-count snapshot's stats — sample_value
    // rows are typically rolling-window or final summaries, and
    // the row with peak count is the most representative single
    // observation. Subqueries grab the timespan covered by all
    // rows for this instance so callers can render
    // `timespan[5m23s]` alongside the stats.
    let row: Result<(
        Option<f64>, Option<f64>, Option<f64>, Option<f64>,
        Option<f64>, Option<i64>, Option<f64>,
        Option<i64>, Option<i64>,
    ), _> = conn.query_row(
        "SELECT mean, p50, p99, min, max, count, stddev,
                (SELECT MIN(timestamp_ms) FROM sample_value WHERE instance_id = ?1),
                (SELECT MAX(timestamp_ms) FROM sample_value WHERE instance_id = ?1)
         FROM sample_value
         WHERE instance_id = ?1
         ORDER BY count DESC
         LIMIT 1",
        [instance_id],
        |r| Ok((
            r.get::<_, Option<f64>>(0)?,
            r.get::<_, Option<f64>>(1)?,
            r.get::<_, Option<f64>>(2)?,
            r.get::<_, Option<f64>>(3)?,
            r.get::<_, Option<f64>>(4)?,
            r.get::<_, Option<i64>>(5)?,
            r.get::<_, Option<f64>>(6)?,
            r.get::<_, Option<i64>>(7)?,
            r.get::<_, Option<i64>>(8)?,
        )),
    );
    match row {
        Ok((mean, p50, p99, min, max, count, stddev, ts_min_ms, ts_max_ms)) =>
            ValueSummary {
                count, mean, p50, p99, min, max, stddev,
                ts_min_ms, ts_max_ms,
            },
        Err(_) => ValueSummary::default(),
    }
}

fn value_summary_string(conn: &rusqlite::Connection, instance_id: i64) -> String {
    let v = load_value_summary(conn, instance_id);
    let mut parts: Vec<String> = Vec::new();
    if let Some(c) = v.count { parts.push(format!("n={c}")); }
    if let Some(m) = v.mean { parts.push(format!("mean={m:.4}")); }
    if let Some(p) = v.p50 { parts.push(format!("p50={p:.4}")); }
    if let Some(p) = v.p99 { parts.push(format!("p99={p:.4}")); }
    if let (Some(mn), Some(mx)) = (v.min, v.max) {
        parts.push(format!("[{mn:.4}..{mx:.4}]"));
    }
    if parts.is_empty() { String::new() }
    else { format!("({})", parts.join(", ")) }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cmp::Ordering;

    #[test]
    fn natural_order_pure_numeric() {
        assert_eq!(cmp_natural("2", "10"), Ordering::Less);
        assert_eq!(cmp_natural("10", "2"), Ordering::Greater);
        assert_eq!(cmp_natural("100", "100"), Ordering::Equal);
        assert_eq!(cmp_natural("1000", "100"), Ordering::Greater);
    }

    #[test]
    fn natural_order_with_prefix() {
        // Mixed strings: prefix-equal, numeric tail compares by value.
        assert_eq!(cmp_natural("k2", "k10"), Ordering::Less);
        assert_eq!(cmp_natural("recall@2.mean", "recall@10.mean"), Ordering::Less);
    }

    #[test]
    fn natural_order_leading_zeros() {
        // "007" == "7" numerically; the longer leading-zero form
        // sorts AFTER the bare form so iteration is stable.
        assert_eq!(cmp_natural("7", "007"), Ordering::Less);
        assert_eq!(cmp_natural("007", "7"), Ordering::Greater);
    }

    #[test]
    fn natural_order_non_numeric_falls_back_to_lex() {
        assert_eq!(cmp_natural("alpha", "beta"), Ordering::Less);
        assert_eq!(cmp_natural("foo", "foo"), Ordering::Equal);
    }

    #[test]
    fn natural_sort_ordering_demo() {
        let mut v = vec!["10", "1", "2", "100", "20"];
        v.sort_by(|a, b| cmp_natural(a, b));
        assert_eq!(v, vec!["1", "2", "10", "20", "100"]);
    }

    #[test]
    fn render_metricsql_sorts_labels_naturally() {
        // Producer-order labels (as parsed from metric_instance.spec)
        // should re-emerge in natural-alphanumeric key order on
        // the metricsql line — every line shares the same key
        // sequence so columnar readers / diff tools align.
        let row1 = InstanceRow {
            family: "recall".into(),
            labels: vec![
                ("profile".into(), "label_03".into()),
                ("k".into(), "10".into()),
                ("limit".into(), "50".into()),
                ("k_at_test".into(), "1".into()),
            ],
            values: None,
        };
        let row2 = InstanceRow {
            family: "recall".into(),
            labels: vec![
                ("limit".into(), "100".into()),
                ("k".into(), "1".into()),
                ("profile".into(), "label_00".into()),
                ("k_at_test".into(), "10".into()),
            ],
            values: None,
        };
        let mut buf: Vec<u8> = Vec::new();
        render_metricsql(&mut buf, &[row1, row2]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        // Both lines share the same key sequence:
        // k, k_at_test, limit, profile (natural alpha-num).
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0],
            r#"recall{k="10",k_at_test="1",limit="50",profile="label_03"}"#);
        assert_eq!(lines[1],
            r#"recall{k="1",k_at_test="10",limit="100",profile="label_00"}"#);
    }

    #[test]
    fn render_metricsql_escapes_special_chars() {
        let row = InstanceRow {
            family: "weird".into(),
            labels: vec![
                ("path".into(), r#"a"b\c"#.into()),
                ("note".into(), "line1\nline2".into()),
            ],
            values: None,
        };
        let mut buf: Vec<u8> = Vec::new();
        render_metricsql(&mut buf, &[row]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        // Backslash → \\, double-quote → \", newline → \n.
        // Keys appear in natural-alphanumeric order: note, path.
        assert_eq!(out.trim_end(),
            r#"weird{note="line1\nline2",path="a\"b\\c"}"#);
    }

    #[test]
    fn render_metricsql_empty_labels() {
        // Family with no labels emits just the bare name.
        let row = InstanceRow {
            family: "stanzas_total".into(),
            labels: vec![],
            values: None,
        };
        let mut buf: Vec<u8> = Vec::new();
        render_metricsql(&mut buf, &[row]).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap().trim_end(), "stanzas_total");
    }

    #[test]
    fn format_parse_metricsql_aliases() {
        for alias in &["metricsql", "openmetrics", "promql", "prometheus",
                       "MetricsQL", "OpenMetrics"] {
            assert_eq!(Format::parse(alias).unwrap(), Format::MetricsQL,
                "alias '{alias}' must resolve to Format::MetricsQL");
        }
    }

    #[test]
    fn split_spec_basic() {
        let (f, l) = split_spec(r#"recall@10.mean{profile="label_03",k="10",limit="50"}"#);
        assert_eq!(f, "recall@10.mean");
        assert_eq!(l, vec![
            ("profile".into(), "label_03".into()),
            ("k".into(), "10".into()),
            ("limit".into(), "50".into()),
        ]);
    }

    #[test]
    fn parse_filter_family_only() {
        let m = parse_filter("recall*").unwrap();
        assert_eq!(m.family.as_deref(), Some("recall*"));
        assert!(m.labels.is_empty());
    }

    #[test]
    fn parse_filter_label_eq() {
        let m = parse_filter(r#"recall{k="10"}"#).unwrap();
        assert_eq!(m.family.as_deref(), Some("recall"));
        assert_eq!(m.labels.len(), 1);
        assert!(matches!(m.labels[0].1, LabelMatch::Equals(ref s) if s == "10"));
    }

    #[test]
    fn parse_filter_label_substring_em() {
        let m = parse_filter(r#"{profile=~label}"#).unwrap();
        assert!(m.family.is_none());
        assert!(matches!(m.labels[0].1, LabelMatch::Substring(ref s) if s == "label"));
    }

    #[test]
    fn parse_filter_label_substring_tilde_only() {
        let m = parse_filter("{profile~label}").unwrap();
        assert!(matches!(m.labels[0].1, LabelMatch::Substring(ref s) if s == "label"));
    }

    #[test]
    fn matcher_substring_matches() {
        let m = parse_filter(r#"{profile=~label}"#).unwrap();
        assert!(m.matches("any", &[("profile".into(), "label_03".into())]));
        assert!(!m.matches("any", &[("profile".into(), "default".into())]));
    }
}
