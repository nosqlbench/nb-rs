// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `nbrs summary` — render a summary report from any
//! `metrics.db` produced by a previous run.
//!
//! Internally calls the same
//! [`nbrs_metrics::reporters::sqlite::SqliteReporter::format_summary`]
//! that the workload-end-of-run path uses (via
//! [`nbrs_activity::runner::report_config_from_summary`]). Two
//! call sites, one source of truth for what a summary looks
//! like.
//!
//! Usage:
//!
//! ```text
//!   nbrs summary                                # list stored
//!   nbrs summary all                            # render every stored
//!   nbrs summary --name recall_v1               # render stored by name
//!   nbrs summary "recall; mean(recall) over profile~label"
//!   nbrs summary "*"                            # all-metrics ad-hoc
//!   nbrs summary --name recall_v1 --create "recall; mean(recall)"
//!                                               # persist + render
//!   nbrs summary --db /path/to/metrics.db ...   # override db
//!   nbrs summary --format csv --output out.csv  # ad-hoc with format
//! ```
//!
//! Defaults:
//! - `--db`: `logs/latest/metrics.db` (the symlink the runner
//!   refreshes after each session — same path the in-run
//!   summary picks up).
//! - `--format`: `md` (Markdown table; matches what the runner
//!   produces today). Override per-call with `--format md|csv`.
//! - `--output`: `<db_dir>/<basename>_summary.<format>`. A
//!   bare basename in `--output` gets the format-derived
//!   extension appended; a path with an extension is used
//!   verbatim.
//!
//! Five resolution modes:
//!
//! 1. **Bare** (`nbrs summary`) — list every stored named
//!    summary in the db so the user can pick (or hint at
//!    literal-spec usage when the db has none).
//! 2. **All stored** (`summary all`) — render every named
//!    summary persisted into the db's `session_metadata`
//!    table. Format and output filename derive from each
//!    stored name (e.g. `recallnmore.csv` → CSV).
//! 3. **Stored by name** (`summary --name <NAME>`) —
//!    regenerate that single named report. Errors if no
//!    stored entry matches.
//! 4. **Ad-hoc literal spec** (`summary "<spec text>"` with
//!    no `--name`) — render a one-off report. `*` is just
//!    "all metrics" in the DSL and routes here.
//! 5. **Persist + render** (`summary --name <NAME> --create
//!    "<spec>"`) — saves the spec into `session_metadata`
//!    under `summary.<NAME>` and renders it. Future
//!    `summary --name <NAME>` calls replay it.

use std::path::{Path, PathBuf};

use nbrs_activity::runner::report_config_from_summary;
use nbrs_metrics::reporters::sqlite::{derive_name_and_format, SqliteReporter};
use nbrs_workload::model::SummaryConfig;

/// Best-effort lookup of stored summary names from a metrics
/// db. Returns an empty Vec when the path doesn't exist or the
/// db can't be opened — callers (e.g. shell completion) read
/// this before any user action and shouldn't surface partial
/// failures.
pub fn list_stored_summary_names(db_path: &Path) -> Vec<String> {
    if !db_path.exists() { return Vec::new(); }
    let Ok(reporter) = SqliteReporter::new(db_path) else { return Vec::new(); };
    reporter.read_stored_summaries()
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Best-effort lookup of named summaries declared in a workload
/// YAML's `summary:` block. Same shape as
/// [`list_stored_summary_names`] but sourced from the file —
/// useful for `nbrs summary --name <TAB> workload=…` when no
/// matching session has been recorded yet.
pub fn list_workload_summary_names(workload_path: &Path) -> Vec<String> {
    load_workload_summaries(workload_path)
        .map(|specs| specs.into_iter().map(|(n, _)| n).collect())
        .unwrap_or_default()
}

/// Read a workload YAML's `report:` block and return
/// `(name, spec_text)` pairs for every `table` item, in
/// declaration order (SRD-46).
fn load_workload_summaries(path: &Path) -> Result<Vec<(String, String)>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read: {e}"))?;
    let workload = nbrs_workload::parse::parse_workload(
        &text, &std::collections::HashMap::new(),
    ).map_err(|e| format!("parse: {e}"))?;
    let entries: Vec<(String, String)> = workload.report.items()
        .filter(|i| matches!(i.kind, nbrs_workload::report::Kind::Table))
        .map(|i| (i.name.clone(), i.body.clone()))
        .collect();
    Ok(entries)
}

/// Render a SRD-46 v2 metricsql-driven table. Each entry in
/// `cfg.metricsql_columns` is a `(column_name, expression)`
/// pair; each is evaluated independently against the session
/// db's `SqliteDataSource`, then the results are joined on the
/// `cfg.group_by` label to produce a row per distinct group.
///
/// `format` is `md` (markdown table) or `csv`. Other formats
/// fall back to markdown — same convention as the legacy path.
fn render_metricsql_table(
    db_path: &Path,
    cfg: &SummaryConfig,
    format: &str,
) -> Result<String, String> {
    use nbrs_metricsql::adapters::sqlite::SqliteDataSource;
    use nbrs_metricsql::eval::{EvalContext, evaluate};
    use std::collections::BTreeMap;

    let ds = SqliteDataSource::open(db_path)
        .map_err(|e| format!("open metricsql sqlite adapter: {e}"))?;
    // Anchor the instant query at the latest sample in the db
    // with a wide lookback so cadence-skewed gauge writes still
    // resolve. Same anchor logic as `plot_metrics::rows_via_metricsql`.
    let conn = rusqlite::Connection::open(db_path)
        .map_err(|e| format!("open db: {e}"))?;
    let (min_ts, max_ts): (i64, i64) = conn.query_row(
        "SELECT COALESCE(MIN(timestamp_ms), 0), COALESCE(MAX(timestamp_ms), 0) \
         FROM sample_value",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|e| format!("read time bounds: {e}"))?;
    if max_ts == 0 {
        return Ok(String::new());
    }
    let ctx = EvalContext {
        data: &ds,
        start_ms: min_ts,
        end_ms: max_ts,
        step_ms: 60_000,
        lookback_ms: Some(300_000),
        query_start_ms: Some(min_ts),
        query_end_ms: Some(max_ts),
    };

    // Evaluate each column expression. For each query, build a
    // `group_value -> column_value` map. When `group_by` is
    // empty, every row collapses into a single un-named row;
    // we put it under the empty string for stable iteration.
    let group_key = cfg.group_by.as_str();
    let mut by_group: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
    let n_cols = cfg.metricsql_columns.len();
    for (col_idx, (_col_name, expr)) in cfg.metricsql_columns.iter().enumerate() {
        let parsed = nbrs_metricsql::parse(expr)
            .map_err(|e| format!("parse '{expr}': {e}"))?;
        let series = evaluate(&ctx, &parsed)
            .map_err(|e| format!("evaluate '{expr}': {e}"))?;
        for s in series {
            let group_val: String = if group_key.is_empty() {
                String::new()
            } else {
                s.labels.iter()
                    .find(|(k, _)| k == group_key)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            };
            // Use the latest sample as the cell value.
            let value = s.samples.iter()
                .max_by_key(|s| s.timestamp_ms)
                .map(|s| s.value);
            let row = by_group.entry(group_val).or_insert_with(|| vec![None; n_cols]);
            row[col_idx] = value;
        }
    }

    // Per-column unit / scaling decision. For columns whose
    // query targets a time-domain metric (latency,
    // servicetime, anything ending in `_ns`/`_seconds`/`_us`/
    // `_ms`), pick a uniform display unit from the column's
    // own value range so cells are read-able and the unit
    // is shown in the heading. Without this, the operator
    // sees a column of bare nanosecond integers labelled
    // `latency` and has no way to tell whether
    // `338422551` is microseconds, nanoseconds, or seconds.
    let column_units: Vec<Option<TimeUnit>> = cfg.metricsql_columns.iter()
        .enumerate()
        .map(|(idx, (_name, expr))| {
            if !is_time_domain_query(expr) { return None; }
            // Gather the max-abs cell value across rows.
            let max_abs = by_group.values()
                .filter_map(|row| row[idx])
                .fold(0.0_f64, |m, v| m.max(v.abs()));
            Some(TimeUnit::for_max_nanos(max_abs))
        })
        .collect();

    // Render headers including unit annotations.
    let column_headers: Vec<String> = cfg.metricsql_columns.iter()
        .zip(column_units.iter())
        .map(|((name, _expr), unit)| match unit {
            Some(u) => format!("{name} ({})", u.symbol),
            None    => name.clone(),
        })
        .collect();

    // Render a single cell against the chosen column unit.
    fn render_cell(value: Option<f64>, unit: Option<&TimeUnit>, sep: &str) -> String {
        let _ = sep;
        match (value, unit) {
            (None, _)            => "-".to_string(),
            (Some(v), Some(u))   => format!("{:.3}", v / u.divisor),
            (Some(v), None)      => format!("{v:.4}"),
        }
    }

    // Emit. Markdown table by default; CSV with `--format=csv`.
    if format.eq_ignore_ascii_case("csv") {
        let mut out = String::new();
        let mut header: Vec<&str> = Vec::new();
        if !group_key.is_empty() { header.push(group_key); }
        let header_strs: Vec<&str> = column_headers.iter().map(String::as_str).collect();
        for h in &header_strs { header.push(*h); }
        out.push_str(&header.join(","));
        out.push('\n');
        for (group_val, cells) in &by_group {
            let mut row: Vec<String> = Vec::new();
            if !group_key.is_empty() { row.push(group_val.clone()); }
            for (cell, unit) in cells.iter().zip(column_units.iter()) {
                row.push(render_cell(*cell, unit.as_ref(), ","));
            }
            out.push_str(&row.join(","));
            out.push('\n');
        }
        return Ok(out);
    }

    // Markdown.
    let mut out = String::new();
    let mut header: Vec<&str> = Vec::new();
    if !group_key.is_empty() { header.push(group_key); }
    let header_strs: Vec<&str> = column_headers.iter().map(String::as_str).collect();
    for h in &header_strs { header.push(*h); }
    out.push_str("| ");
    out.push_str(&header.join(" | "));
    out.push_str(" |\n|");
    for _ in &header {
        out.push_str("---|");
    }
    out.push('\n');
    for (group_val, cells) in &by_group {
        out.push_str("| ");
        if !group_key.is_empty() {
            out.push_str(group_val);
            out.push_str(" | ");
        }
        let cell_strs: Vec<String> = cells.iter()
            .zip(column_units.iter())
            .map(|(c, u)| render_cell(*c, u.as_ref(), " | "))
            .collect();
        out.push_str(&cell_strs.join(" | "));
        out.push_str(" |\n");
    }
    Ok(out)
}

/// Display unit for a time-domain column. nb-rs internal
/// metrics are nanoseconds (per project memory
/// "Nanos Standard"); the formatter picks a single
/// human-readable unit per *column* so cells stay aligned
/// and the unit shows up in the heading once.
#[derive(Copy, Clone, Debug)]
struct TimeUnit {
    /// `s` / `ms` / `µs` / `ns`.
    symbol: &'static str,
    /// What to divide nanoseconds by to produce the
    /// displayed value.
    divisor: f64,
}

impl TimeUnit {
    fn for_max_nanos(max_abs: f64) -> Self {
        if max_abs >= 1e9       { Self { symbol: "s",  divisor: 1e9 } }
        else if max_abs >= 1e6  { Self { symbol: "ms", divisor: 1e6 } }
        else if max_abs >= 1e3  { Self { symbol: "µs", divisor: 1e3 } }
        else                    { Self { symbol: "ns", divisor: 1.0 } }
    }
}

/// Heuristic: does this metricsql query target a time-domain
/// metric whose values are stored as nanoseconds?
///
/// We can't reach into the metric registry from the table
/// renderer, but the OpenMetrics naming convention and
/// nb-rs's own metric-name vocabulary make this an easy
/// substring match. Both internal-time names (containing
/// `latency`, `servicetime`, `duration`, `elapsed`) and the
/// suffix conventions (`_ns`, `_seconds`, `_ms`, `_us`,
/// `_µs`) are recognised. False positives (a hypothetical
/// `latency_count` of dimensionless integers) would just
/// re-scale a small column harmlessly.
fn is_time_domain_query(expr: &str) -> bool {
    let lower = expr.to_ascii_lowercase();
    const NAME_HINTS: &[&str] = &[
        "latency", "servicetime", "service_time",
        "duration", "elapsed", "responsetime", "response_time",
    ];
    if NAME_HINTS.iter().any(|h| lower.contains(h)) { return true; }
    // Suffix conventions. Look at every metric-shaped
    // token (alphanumeric + underscores) for the suffix.
    for tok in lower.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        for suffix in ["_ns", "_seconds", "_ms", "_us"] {
            if tok.ends_with(suffix) { return true; }
        }
    }
    false
}

#[cfg(test)]
mod time_unit_tests {
    use super::*;

    #[test]
    fn time_unit_for_max_nanos_picks_natural_scale() {
        assert_eq!(TimeUnit::for_max_nanos(2_500_000_000.0).symbol, "s");
        assert_eq!(TimeUnit::for_max_nanos(338_422_551.0).symbol,    "ms");
        assert_eq!(TimeUnit::for_max_nanos(951_290.0).symbol,        "µs");
        assert_eq!(TimeUnit::for_max_nanos(750.0).symbol,            "ns");
        assert_eq!(TimeUnit::for_max_nanos(0.0).symbol,              "ns");
    }

    #[test]
    fn is_time_domain_query_recognises_the_canonical_names() {
        assert!(is_time_domain_query("avg(cycles_servicetime_mean) by (profile)"));
        assert!(is_time_domain_query("avg(latency_p99) by (profile)"));
        assert!(is_time_domain_query("avg(some_metric_ns)"));
        assert!(is_time_domain_query("rate(http_request_duration_seconds[5m])"));
        assert!(is_time_domain_query("AVG(LATENCY_MEAN)"), "case-insensitive");
    }

    #[test]
    fn is_time_domain_query_rejects_dimensionless() {
        assert!(!is_time_domain_query("avg(recall_mean) by (profile)"));
        assert!(!is_time_domain_query("count(rows_total)"));
        assert!(!is_time_domain_query("max(connection_errors)"));
    }
}

pub fn summary_command(args: &[String]) {
    let opts = parse_args(args);

    // Resolve the effective db path. With one db (or none) the
    // path is used as-is. With multiple dbs the merge step runs
    // first, producing a temp file whose merged rows feed
    // SqliteReporter as if from one logical session.
    let primary_db = opts.db.clone().unwrap_or_else(
        || PathBuf::from("logs/latest/metrics.db"));
    let effective_dbs: Vec<PathBuf> = if opts.dbs.is_empty() {
        vec![primary_db.clone()]
    } else {
        opts.dbs.clone()
    };
    for db in &effective_dbs {
        if !db.exists() {
            eprintln!("nbrs summary: metrics db not found at '{}'.", db.display());
            eprintln!();
            eprintln!("Did a previous run finish? `logs/latest/` should be");
            eprintln!("a symlink to the most recent session directory.");
            eprintln!("Override with --db <path>.");
            std::process::exit(1);
        }
    }
    let db_path: PathBuf = if effective_dbs.len() > 1 {
        match crate::db_merge::merge_dbs(&effective_dbs) {
            Ok(path) => {
                eprintln!("merge: {} dbs → {}",
                    effective_dbs.len(), path.display());
                path
            }
            Err(e) => {
                eprintln!("nbrs summary: failed to merge dbs: {e}");
                std::process::exit(1);
            }
        }
    } else {
        effective_dbs[0].clone()
    };
    // Output paths anchor on the primary (first) db, not the
    // merged temp — keeps artifacts alongside real session data.
    let output_anchor: PathBuf = effective_dbs[0].clone();

    let mut reporter = match SqliteReporter::new(&db_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("nbrs summary: failed to open '{}': {e}",
                db_path.display());
            std::process::exit(1);
        }
    };

    // Six cases, decided by combinations of `--name`,
    // `--create`, and the bare positional spec:
    //
    //   1. Bare `summary` (no flags, no positional)
    //                                          → error, list stored.
    //   2. `summary all` (positional only)     → every stored.
    //   3. `summary --name N` (no `--create`, no positional)
    //                                          → render stored N.
    //   4. `summary <spec>` (positional only)  → ad-hoc literal.
    //   5. `summary --name N --create <spec>`  → persist <spec>
    //      under N, then render the new entry.
    //   6. Combinations the contract forbids   → hard error.
    // Source named summaries: `workload=<path>` overrides the
    // metrics db's `session_metadata` table. Useful before any
    // run has happened, or when the workload's `summary:` block
    // has been edited and the user wants the new spec applied
    // to existing data.
    let stored: Vec<(String, String)> = match opts.workload.as_deref() {
        Some(path) => match load_workload_summaries(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("nbrs summary: workload '{}': {e}", path.display());
                std::process::exit(1);
            }
        },
        None => reporter.read_stored_summaries(),
    };
    let to_render: Vec<(String, SummaryConfig)> = match (
        opts.name.as_deref(),
        opts.create,
        opts.spec.as_deref(),
    ) {
        // Case 6a: --create without --name has no place to
        // store the spec. Reject early with a clear message.
        (None, true, _) => {
            eprintln!("nbrs summary: --create requires --name <NAME>");
            std::process::exit(1);
        }
        // Case 6b: --name with no --create can't take a
        // positional — the positional is only meaningful when
        // creating a new entry. Reject so we don't silently
        // swallow user input.
        (Some(_), false, Some(_)) => {
            eprintln!("nbrs summary: positional spec is only valid with \
                       `--create`; drop it or add `--create` to persist.");
            std::process::exit(1);
        }
        // Case 5: persist + render.
        (Some(name), true, Some(spec_text)) => {
            let cfg = SummaryConfig::parse(spec_text);
            reporter.set_metadata(&format!("summary.{name}"), &cfg.raw);
            eprintln!("created: summary.{name} → {} (in {})",
                cfg.raw.lines().next().unwrap_or("").trim(),
                db_path.display());
            vec![(name.to_string(), cfg)]
        }
        // Case 6c: --create --name N but no spec — nothing to
        // persist.
        (Some(_), true, None) => {
            eprintln!("nbrs summary: --create --name <NAME> needs a positional spec");
            std::process::exit(1);
        }
        // Case 3: render stored by name.
        (Some(name), false, None) => {
            let Some((found, raw)) = stored.iter().find(|(n, _)| n == name) else {
                eprintln!("nbrs summary: no stored summary named '{name}' in '{}'",
                    db_path.display());
                if !stored.is_empty() {
                    eprintln!();
                    eprintln!("Available:");
                    for (n, _) in &stored { eprintln!("  {n}"); }
                }
                std::process::exit(1);
            };
            return_stored_or_literal(found, raw)
        }
        // Case 2: render every stored.
        (None, false, Some("all")) => {
            if stored.is_empty() {
                eprintln!("nbrs summary: '{}' has no stored named \
                           summaries to render. Use `nbrs summary '*'` \
                           for an ad-hoc all-metrics report, or \
                           `--name <N> --create <spec>` to persist \
                           one first.",
                    db_path.display());
                std::process::exit(1);
            }
            stored.into_iter()
                .map(|(name, raw)| (name, SummaryConfig::parse(&raw)))
                .collect()
        }
        // Case 4: ad-hoc literal spec (no `--name`, no
        // `--create`). Includes the `*` wildcard, which is just
        // a literal spec the DSL knows how to parse.
        (None, false, Some(spec_text)) => {
            literal_spec(spec_text, None)
        }
        // Case 1: bare. List stored, or hint at literal-spec
        // mode if nothing is persisted yet.
        (None, false, None) => {
            if stored.is_empty() {
                eprintln!("nbrs summary: '{}' has no stored named \
                           summaries.", db_path.display());
                eprintln!();
                eprintln!("Pass a literal spec to render an ad-hoc report:");
                eprintln!("  nbrs summary '*'                  # all metrics");
                eprintln!("  nbrs summary 'recall; mean(...)'  # custom DSL");
                eprintln!();
                eprintln!("Use `--name <N> --create <spec>` to persist a");
                eprintln!("spec into the db so future runs can replay it.");
            } else {
                eprintln!("nbrs summary: '{}' has stored named summaries —",
                    db_path.display());
                eprintln!("pick one with --name, or use `summary all` for every.");
                eprintln!();
                eprintln!("Available:");
                for (name, raw) in &stored {
                    let preview = raw.lines().next().unwrap_or("").trim();
                    let preview = if preview.len() > 60 {
                        format!("{}…", &preview[..60])
                    } else {
                        preview.to_string()
                    };
                    eprintln!("  {name:<24}  {preview}");
                }
                eprintln!();
                eprintln!("Examples:");
                eprintln!("  nbrs summary all                       # render every stored");
                eprintln!("  nbrs summary --name {}", stored[0].0);
            }
            std::process::exit(1);
        }
    };

    // When a single ad-hoc report is requested AND the user
    // gave `--output`, that path applies to the one report
    // (whether it has an extension or not). When multiple
    // reports are produced, `--output` would be ambiguous —
    // ignored with a warning.
    let multiple = to_render.len() > 1;
    if multiple && opts.output.is_some() {
        eprintln!("warning: --output is ignored when multiple summaries \
                   are rendered; falling back to per-name filenames in \
                   the db's session directory.");
    }

    let cli_format = opts.format.clone();
    let mut any_nonempty = false;
    for (name, cfg) in &to_render {
        // Format precedence: CLI `--format` wins; otherwise
        // derive from the stored name's suffix; default to md.
        let (basename, derived_format) = derive_name_and_format(name);
        let format = cli_format.clone().unwrap_or(derived_format);
        // SRD-46 v2: native metricsql tables route through a
        // dedicated renderer; legacy DSL tables stay on the
        // SqliteReporter path.
        let rendered = if !cfg.metricsql_columns.is_empty() {
            match render_metricsql_table(&db_path, cfg, &format) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("nbrs summary: metricsql table '{name}' failed: {e}");
                    continue;
                }
            }
        } else {
            let report_cfg = report_config_from_summary(cfg);
            reporter.format_summary_with_format(&report_cfg, &format)
        };
        if rendered.is_empty() {
            eprintln!("nbrs summary: '{name}' produced no rows \
                       (db='{}').", db_path.display());
            continue;
        }
        any_nonempty = true;
        let output_path = if !multiple && opts.output.is_some() {
            resolve_output_path(opts.output.as_deref(), &format, &output_anchor)
        } else {
            default_output_path(&basename, &format, &output_anchor)
        };
        if let Some(parent) = output_path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    eprintln!("nbrs summary: failed to create output dir '{}': {e}",
                        parent.display());
                    std::process::exit(1);
                }
            }
        }
        if let Err(e) = std::fs::write(&output_path, &rendered) {
            eprintln!("nbrs summary: failed to write '{}': {e}",
                output_path.display());
            std::process::exit(1);
        }
        eprintln!("summary: {}", output_path.display());

        // Upsert into the framing markdown report (default
        // `<db_dir>/summary.md`). Only Markdown summaries embed
        // inline; CSV/other formats record a link to the file
        // since rendering them inline would be unreadable.
        if !opts.report_disabled {
            let report_path = opts.report.clone().unwrap_or_else(|| {
                let dir = output_anchor.parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| PathBuf::from("."));
                dir.join("summary.md")
            });
            // Don't recursively upsert into the same file we're
            // rendering when --output happens to be summary.md.
            if report_path != output_path {
                let body = if format == "md" {
                    rendered.clone()
                } else {
                    let leaf = output_path.file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| output_path.to_string_lossy().into_owned());
                    format!("[{leaf}]({leaf})\n")
                };
                let label = opts.label.clone()
                    .unwrap_or_else(|| crate::report::prettify_name(&basename));
                let heading_display = match opts.figure_num {
                    Some(n) => format!("{n}. {label} (table)"),
                    None => format!("{label} (table)"),
                };
                let mode = opts.report_mode
                    .unwrap_or(crate::report::WriteMode::Update);
                match crate::report::write_named_section(
                    &report_path, &basename, &heading_display, &body, mode,
                ) {
                    Ok(true) => {}
                    Ok(false) => eprintln!(
                        "report: {} (skipped — section exists, --add-to-markdown mode)",
                        report_path.display()),
                    Err(e) => eprintln!(
                        "warning: failed to update report '{}': {e}",
                        report_path.display()),
                }
            }
        }

        // Echo to stdout for redirection-friendly use. With
        // multiple reports, prefix each with a separator banner
        // so a piped consumer can distinguish them.
        if multiple {
            println!("=== {name} → {} ===", output_path.display());
        }
        print!("{rendered}");
    }
    if !any_nonempty {
        std::process::exit(1);
    }
}

/// Default output path for a single summary: live in the db's
/// session directory, named `<basename>_summary.<format>`.
fn default_output_path(basename: &str, format: &str, db_path: &Path) -> PathBuf {
    let dir = db_path.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    dir.join(format!("{basename}_summary.{format}"))
}

/// Wrap a stored hit into the `to_render` shape.
fn return_stored_or_literal(name: &str, raw: &str)
    -> Vec<(String, SummaryConfig)>
{
    vec![(name.to_string(), SummaryConfig::parse(raw))]
}

/// Wrap an ad-hoc literal spec into the `to_render` shape, using
/// `override_name` if supplied (currently unused — the new CLI
/// reserves names for stored entries) else `"default"`.
fn literal_spec(spec: &str, override_name: Option<&str>)
    -> Vec<(String, SummaryConfig)>
{
    let name = override_name.unwrap_or("default").to_string();
    vec![(name, SummaryConfig::parse(spec))]
}

/// Resolve the output path:
/// - `--output <path>`: as-is if it has any extension, otherwise
///   append `.{format}`.
/// - no `--output`: same directory as the metrics db, basename
///   `summary.{format}`.
fn resolve_output_path(
    user_output: Option<&str>,
    format: &str,
    db_path: &std::path::Path,
) -> PathBuf {
    match user_output {
        Some(path) => {
            let p = PathBuf::from(path);
            if p.extension().is_none() {
                let mut q = p;
                q.set_extension(format);
                q
            } else {
                p
            }
        }
        None => {
            let dir = db_path.parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            dir.join(format!("summary.{format}"))
        }
    }
}

#[derive(Default)]
struct SummaryOpts {
    /// Bare positional. With `--create`, this is the spec to
    /// persist. Without `--name`, this is the ad-hoc literal
    /// spec to render. With `--name` and no `--create`, this
    /// must be empty (the user is referring to a stored name).
    spec: Option<String>,
    /// Reference to a stored named summary
    /// (`--name <NAME>`). Without `--create`, looks up the
    /// stored entry — error if missing. With `--create`,
    /// names the new entry being persisted from `spec`.
    name: Option<String>,
    /// Persist a new named summary (`--create`). Requires
    /// both `--name <NAME>` and a positional `<spec>`.
    create: bool,
    /// First-given db; preserved separately so the existing
    /// "single-db" diagnostic and default-output-path code
    /// works unchanged.
    db: Option<PathBuf>,
    /// Every db given via `--db` (repeatable, or
    /// comma-separated). When more than one is present, the
    /// summary command merges them into a temp db first, then
    /// runs SqliteReporter against the merged file. Sessions
    /// of the same workload are deduplicated by stripping the
    /// `session=` label so summary aggregates flow across all
    /// inputs as if from one logical session.
    dbs: Vec<PathBuf>,
    format: Option<String>,
    output: Option<String>,
    /// Source named summaries from a workload YAML's
    /// `summary:` block instead of the metrics db's
    /// `session_metadata`. Useful before any run has happened,
    /// or to overlay a fresh spec set on existing data.
    workload: Option<PathBuf>,
    /// Path to the framing markdown report. Each rendered
    /// summary is upserted as a `## summary: <name>` section.
    /// Default: `<db_dir>/summary.md`. `--report=skip` to
    /// suppress.
    report: Option<PathBuf>,
    /// Collision policy. `--update-markdown` (default) replaces
    /// existing same-anchor sections in place; `--add-to-markdown`
    /// only appends when no section under the same anchor
    /// exists, leaving existing content untouched.
    report_mode: Option<crate::report::WriteMode>,
    /// True when `--report=skip` / `--no-report` is passed.
    report_disabled: bool,
    /// SRD-46: figure number injected by `nbrs report` for
    /// markdown heading prefix.
    figure_num: Option<usize>,
    /// SRD-46: display label injected by `nbrs report`. Falls
    /// back to a prettified name.
    label: Option<String>,
}

fn parse_args(args: &[String]) -> SummaryOpts {
    let mut opts = SummaryOpts::default();
    // `--session` / `--session-path` / `--session-name` resolve
    // to a session dir uniformly across read-side tools — see
    // `nbrs_activity::session::read_session_dir`. `--db` below
    // overrides this when it's given explicitly.
    if let Some(session_dir) = nbrs_activity::session::read_session_dir(args) {
        opts.db = Some(session_dir.join("metrics.db"));
    }
    let mut iter = args.iter().peekable();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--db" => {
                if let Some(v) = iter.next() {
                    for path in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                        opts.dbs.push(PathBuf::from(path));
                    }
                    if opts.db.is_none() {
                        opts.db = opts.dbs.first().cloned();
                    }
                }
            }
            "--format" => {
                if let Some(v) = iter.next() {
                    opts.format = Some(v.clone());
                }
            }
            "--output" => {
                if let Some(v) = iter.next() {
                    opts.output = Some(v.clone());
                }
            }
            "--name" => {
                if let Some(v) = iter.next() {
                    opts.name = Some(v.clone());
                }
            }
            "--label" => {
                if let Some(v) = iter.next() {
                    opts.label = Some(v.clone());
                }
            }
            "--figure-num" => {
                if let Some(v) = iter.next()
                    && let Ok(n) = v.parse::<usize>() {
                    opts.figure_num = Some(n);
                }
            }
            "--create" => {
                opts.create = true;
            }
            "--report" | "--update-markdown" => {
                if let Some(v) = iter.next() {
                    if v == "skip" || v.is_empty() {
                        opts.report_disabled = true;
                    } else {
                        opts.report = Some(PathBuf::from(v));
                        opts.report_mode = Some(crate::report::WriteMode::Update);
                    }
                }
            }
            "--add-to-markdown" => {
                if let Some(v) = iter.next() {
                    opts.report = Some(PathBuf::from(v));
                    opts.report_mode = Some(crate::report::WriteMode::AddIfMissing);
                }
            }
            "--no-report" => opts.report_disabled = true,
            // Global session flags — already consumed by
            // `read_session_dir` above. Swallow the value so
            // it doesn't drift into `opts.spec` as a stray
            // positional.
            "--session" | "--session-name" | "--session-path"
            | "--session-reuse" | "--session-keep" | "--session-shelflife"
            | "--resume" | "--gk-lib" => { let _ = iter.next(); }
            "--strict" | "--no-prompt" | "--resume-latest"
            | "--force-retry-failed" => {}
            other => {
                if let Some(v) = other.strip_prefix("--db=") {
                    for path in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                        opts.dbs.push(PathBuf::from(path));
                    }
                    if opts.db.is_none() {
                        opts.db = opts.dbs.first().cloned();
                    }
                } else if let Some(v) = other.strip_prefix("--format=") {
                    opts.format = Some(v.to_string());
                } else if let Some(v) = other.strip_prefix("--output=") {
                    opts.output = Some(v.to_string());
                } else if let Some(v) = other.strip_prefix("--name=") {
                    opts.name = Some(v.to_string());
                } else if let Some(v) = other.strip_prefix("workload=") {
                    let resolved = crate::cli::resolve_workload_path(v)
                        .unwrap_or_else(|| v.to_string());
                    opts.workload = Some(PathBuf::from(resolved));
                } else if let Some(v) = other.strip_prefix("--report=")
                    .or_else(|| other.strip_prefix("--update-markdown=")) {
                    if v == "skip" || v.is_empty() {
                        opts.report_disabled = true;
                    } else {
                        opts.report = Some(PathBuf::from(v));
                        opts.report_mode = Some(crate::report::WriteMode::Update);
                    }
                } else if let Some(v) = other.strip_prefix("--add-to-markdown=") {
                    opts.report = Some(PathBuf::from(v));
                    opts.report_mode = Some(crate::report::WriteMode::AddIfMissing);
                } else if !other.starts_with("--") && opts.spec.is_none() {
                    // First bare positional is the spec / stored
                    // name / `*` shortcut. Subsequent positionals
                    // are silently ignored; the previous CLI
                    // shape only ever accepted one.
                    opts.spec = Some(other.to_string());
                }
            }
        }
    }
    opts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> String { v.to_string() }

    #[test]
    fn bare_yields_no_spec() {
        let opts = parse_args(&[]);
        assert!(opts.spec.is_none(), "bare `summary` should leave spec as None (lists stored)");
    }

    #[test]
    fn first_positional_becomes_spec() {
        let opts = parse_args(&[s("recall; mean(recall) over profile~label")]);
        assert_eq!(opts.spec.as_deref(), Some("recall; mean(recall) over profile~label"));
    }

    #[test]
    fn flags_do_not_become_spec() {
        let opts = parse_args(&[s("--db"), s("/tmp/m.db")]);
        assert!(opts.spec.is_none(), "flags must not be parsed as the spec positional");
        assert_eq!(opts.db.as_deref(), Some(std::path::Path::new("/tmp/m.db")));
    }

    #[test]
    fn output_extension_added_when_basename_only() {
        let p = resolve_output_path(Some("report"), "md", std::path::Path::new("/tmp/m.db"));
        assert_eq!(p, PathBuf::from("report.md"));
    }

    #[test]
    fn output_extension_preserved_when_present() {
        let p = resolve_output_path(Some("/tmp/x.csv"), "md", std::path::Path::new("/tmp/m.db"));
        assert_eq!(p, PathBuf::from("/tmp/x.csv"), "explicit extension wins over --format default");
    }

    #[test]
    fn default_output_lives_alongside_db() {
        let p = resolve_output_path(None, "md",
            std::path::Path::new("logs/session_1/metrics.db"));
        assert_eq!(p, PathBuf::from("logs/session_1/summary.md"));
    }

    #[test]
    fn all_options_combined() {
        let opts = parse_args(&[
            s("recall"),
            s("--db"), s("/tmp/m.db"),
            s("--format"), s("md"),
            s("--output"), s("/tmp/out"),
        ]);
        assert_eq!(opts.spec.as_deref(), Some("recall"));
        assert_eq!(opts.db.as_deref(), Some(std::path::Path::new("/tmp/m.db")));
        assert_eq!(opts.format.as_deref(), Some("md"));
        assert_eq!(opts.output.as_deref(), Some("/tmp/out"));
    }

    #[test]
    fn name_alone_targets_stored() {
        let opts = parse_args(&[s("--name"), s("recall_v1")]);
        assert_eq!(opts.name.as_deref(), Some("recall_v1"));
        assert!(!opts.create);
        assert!(opts.spec.is_none());
    }

    #[test]
    fn name_with_create_and_spec() {
        let opts = parse_args(&[
            s("--name"), s("recall_v1"),
            s("--create"),
            s("recall; mean(recall)"),
        ]);
        assert_eq!(opts.name.as_deref(), Some("recall_v1"));
        assert!(opts.create);
        assert_eq!(opts.spec.as_deref(), Some("recall; mean(recall)"));
    }
}
