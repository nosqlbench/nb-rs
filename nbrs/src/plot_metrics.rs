// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `nbrs plot` (metrics-DB form) — render a PNG line plot from
//! the metrics db produced by a previous run.
//!
//! Companion to `nbrs summary`: same source (the session's
//! `metrics.db`), same post-hoc applicability, complementary
//! output (PNG line plot rather than Markdown/CSV table). This
//! is **not** the plotter adapter — that path renders to the
//! terminal in real time. This command operates on a
//! finished session.
//!
//! Spec layout follows the `summary` family:
//!   nbrs plot --metric <pattern>            # required: which metric family
//!             --x <label_key>               # required: label whose values become the X axis
//!             [--series <label_key>]        # optional: one line per distinct value of this label
//!             [--filter <key>=<value> ...]  # restrict rows to matching label values
//!             [--agg mean|min|max|p50|p99]  # how multiple samples per (series, x) collapse
//!             [--db <path>]                 # default logs/latest/metrics.db
//!             [--output <path>]             # default <db_dir>/<metric>_<x>.png
//!             [--title <text>]              # plot title
//!             [--xlabel <text>] [--ylabel <text>]
//!             [--xscale linear|log]
//!             [--yscale linear|log]
//!             [--width <px>] [--height <px>]
//!
//! Worked example — "mean recall@10 vs limit at k=10":
//!
//!   nbrs plot \
//!     --metric recall@10.mean \
//!     --x limit \
//!     --filter k=10 \
//!     --filter phase=ann_query \
//!     --output recall_at_10.png
//!
//! Worked example — one line per profile:
//!
//!   nbrs plot \
//!     --metric recall@10.mean \
//!     --x limit \
//!     --series profile \
//!     --filter k=10 \
//!     --output recall_per_profile.png

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use plotters::prelude::*;

/// Bundled font so PNG output is hermetic — plotters' bitmap
/// backend would otherwise need fontconfig to find a system
/// font, and bare-metal containers / sandboxed builds often
/// don't ship one. DejaVu Sans is permissively licensed (a
/// derivative of Bitstream Vera; redistribution permitted).
const DEJAVU_SANS: &[u8] = include_bytes!("DejaVuSans.ttf");

/// Flags that take a value as the next arg. Used by
/// [`parse_args`]'s positional-spec pre-sweep so a flag's value
/// (e.g. `local/foo` after `--session`) doesn't get
/// misclassified as a positional DSL spec and rewrite `opts`.
const FLAGS_TAKING_VALUE: &[&str] = &[
    // Plot-specific
    "--metric", "--x", "--series", "--filter", "--agg",
    "--db", "--output", "--name", "--label", "--palette",
    "--line", "--line-width", "--marker", "--marker-size",
    "--figure-num", "--title", "--xlabel", "--ylabel",
    "--xscale", "--yscale", "--width", "--height",
    "--x-min", "--x-max", "--y-min", "--y-max", "--legend",
    "--query",
    "--csv-also", "--report", "--update-markdown",
    "--add-to-markdown",
    // Global flags consumed at startup but still appearing in
    // argv when plot's parser walks them.
    "--session", "--session-name", "--session-path",
    "--session-reuse", "--session-keep", "--session-shelflife",
    "--resume", "--gk-lib",
];

/// Register the bundled font with plotters' ab_glyph backend.
/// Idempotent — re-registration is a no-op. Called once at the
/// start of every plot command so PNG and SVG renderers can both
/// resolve "sans-serif".
fn register_bundled_font() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Register under both `sans-serif` (which our chart
        // builders ask for) and the literal family name so users
        // who customize face strings hit the same bundled font.
        let _ = plotters::style::register_font(
            "sans-serif",
            plotters::style::FontStyle::Normal,
            DEJAVU_SANS,
        );
        let _ = plotters::style::register_font(
            "DejaVu Sans",
            plotters::style::FontStyle::Normal,
            DEJAVU_SANS,
        );
    });
}

/// Parse a single-string spec into a [`PlotMetricsOpts`]. The
/// spec is a semicolon-delimited DSL that mirrors `summary`'s
/// shape:
///
/// ```text
/// "<metric_pattern> over <x_label> [by <series>] [where <k>=<v>, ...] [agg=<fn>]"
/// ```
///
/// Each directive can also stand alone, separated by `;`:
///
/// ```text
/// "recall@10.mean; over limit; where k=10; agg=mean"
/// ```
///
/// Filters can be a comma-separated list inside one `where`
/// directive, or repeated `where=<k>=<v>` directives.
fn parse_spec(spec: &str) -> Result<PlotMetricsOpts, String> {
    let mut opts = PlotMetricsOpts {
        agg: "mean".to_string(),
        xscale: "linear".to_string(),
        yscale: "linear".to_string(),
        width: 1024,
        height: 640,
        ..Default::default()
    };
    // Strip `#` line-oriented comments before any other parsing.
    // A `#` outside a quoted string runs the rest of the line out
    // as a comment. Each input line then trims to its non-comment
    // content; empty lines drop. (SRD-46: report/plot/table
    // bodies all support `#` comments.)
    let cleaned = strip_line_comments(spec);
    // Per-line splitting: each non-empty line is its own
    // directive (in addition to `;` separators within a line).
    // Multi-line plot bodies are normalised here so the rest of
    // the parser doesn't need to know about them.
    // Native-form pre-pass owns the pre-line scan now (see
    // below); the legacy `by_lines` formed `;`-joined directives
    // out of every non-empty line. Replaced by `residual_lines`
    // after we strip the native-form lines (`query:`, `x:`,
    // `series:`).
    let parse_over = |rest: &str, opts: &mut PlotMetricsOpts| {
        // `over <items>` accepts a comma-separated list. Each
        // item is one of:
        //   - bare key: contributes to the axis/series set
        //   - `key~pattern`: substring filter on `key` (in-band
        //     `~`-prefixed value in opts.filters)
        // Disposition: the LAST bare key is the x-axis; earlier
        // bare keys become series-discriminator labels.
        let mut bare: Vec<String> = Vec::new();
        for item in rest.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if let Some((k, p)) = item.split_once('~') {
                opts.filters.push((k.trim().to_string(), format!("~{}", p.trim())));
            } else {
                bare.push(item.to_string());
            }
        }
        if let Some(x) = bare.pop() {
            opts.x_label = Some(x);
        }
        for k in bare {
            opts.series_labels.push(k);
        }
    };

    // Native-form directives: `query: <metricsql>`, `x: <label>`,
    // `series: <label>[,<label>...]`. These are the canonical
    // SRD-46-v2 surface — every other directive is the legacy
    // DSL retained for back-compat. Detected with `:` separator
    // (not `=`) to keep them distinct from the bind-point /
    // filter shorthand.
    //
    // Splitting and rewriting these BEFORE the per-line
    // directive walk so a `query:` body containing arbitrary
    // metricsql isn't sliced apart by the `;`-separator pass
    // below. We extract them by line index, then drop those
    // lines before joining.
    let lines_vec: Vec<&str> = cleaned.lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let mut residual_lines: Vec<String> = Vec::new();
    for line in &lines_vec {
        if let Some(rest) = line.strip_prefix("query:").map(str::trim) {
            opts.query = Some(rest.to_string());
        } else if let Some(rest) = line.strip_prefix("x:").map(str::trim) {
            opts.x_label = Some(rest.to_string());
        } else if let Some(rest) = line.strip_prefix("series:").map(str::trim) {
            for k in rest.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                opts.series_labels.push(k.to_string());
            }
        } else {
            residual_lines.push((*line).to_string());
        }
    }
    let by_lines = residual_lines.join(";");

    for directive in by_lines.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        // Two equivalent aggregator-shorthand forms:
        //
        //   mean recall@10 over limit         (prefix form)
        //   mean(recall@10) over limit        (function-call form)
        //
        // Both rewrite to `recall@10.mean over limit` before the
        // directive parser runs.
        let directive_owned;
        let directive: &str = if let Some((agg, metric, after)) = parse_function_agg(directive) {
            directive_owned = if after.is_empty() {
                format!("{metric}.{agg}")
            } else {
                format!("{metric}.{agg} {after}")
            };
            &directive_owned
        } else if let Some((agg_prefix, rest)) = strip_agg_prefix(directive) {
            let (metric, after) = match rest.split_once(char::is_whitespace) {
                Some(p) => p,
                None => (rest, ""),
            };
            directive_owned = if after.is_empty() {
                format!("{metric}.{agg_prefix}")
            } else {
                format!("{metric}.{agg_prefix} {after}")
            };
            &directive_owned
        } else {
            directive
        };
        if let Some(rest) = directive.strip_prefix("over ") {
            parse_over(rest, &mut opts);
        } else if let Some(rest) = directive.strip_prefix("by ") {
            // `by` takes either a comma-separated list of label
            // keys (multi-key tuple → one series per distinct
            // tuple) or `*` (auto-detect every label that has
            // >1 distinct value after filtering, excluding the
            // x-axis label).
            for k in rest.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                opts.series_labels.push(k.to_string());
            }
        } else if let Some(rest) = directive.strip_prefix("where ") {
            for filter in rest.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                let (k, v) = filter.split_once('=')
                    .ok_or_else(|| format!("where filter '{filter}' must be <key>=<value>"))?;
                opts.filters.push((k.trim().to_string(), v.trim().to_string()));
            }
        } else if let Some(v) = directive.strip_prefix("agg=") {
            opts.agg = v.trim().to_string();
        } else if let Some(v) = directive.strip_prefix("title=") {
            opts.title = Some(v.trim().to_string());
        } else if let Some(v) = directive.strip_prefix("xlabel=") {
            opts.xlabel = Some(v.trim().to_string());
        } else if let Some(v) = directive.strip_prefix("ylabel=") {
            opts.ylabel = Some(v.trim().to_string());
        } else if let Some(v) = directive.strip_prefix("xscale=") {
            opts.xscale = v.trim().to_string();
        } else if let Some(v) = directive.strip_prefix("yscale=") {
            opts.yscale = v.trim().to_string();
        } else if !directive.contains(' ') && !directive.contains('=') && opts.metric.is_none() {
            // First bare token is the metric.
            opts.metric = Some(directive.to_string());
        } else {
            // Allow `<metric> over <x>` as a single directive too.
            if let Some((before_over, after_over)) = directive.split_once(" over ") {
                if opts.metric.is_none() {
                    opts.metric = Some(before_over.trim().to_string());
                }
                // The rest may itself contain `by` and `where`.
                if let Some((x_part, by_rest)) = after_over.split_once(" by ") {
                    parse_over(x_part, &mut opts);
                    let (series_text, where_text) = match by_rest.split_once(" where ") {
                        Some((s, w)) => (s, Some(w)),
                        None => (by_rest, None),
                    };
                    for k in series_text.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                        opts.series_labels.push(k.to_string());
                    }
                    if let Some(where_rest) = where_text {
                        for f in where_rest.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                            let (k, v) = f.split_once('=')
                                .ok_or_else(|| format!("where filter '{f}' must be <key>=<value>"))?;
                            opts.filters.push((k.trim().to_string(), v.trim().to_string()));
                        }
                    }
                } else if let Some((x_part, where_rest)) = after_over.split_once(" where ") {
                    parse_over(x_part, &mut opts);
                    for f in where_rest.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                        let (k, v) = f.split_once('=')
                            .ok_or_else(|| format!("where filter '{f}' must be <key>=<value>"))?;
                        opts.filters.push((k.trim().to_string(), v.trim().to_string()));
                    }
                } else {
                    parse_over(after_over, &mut opts);
                }
            } else {
                return Err(format!(
                    "unrecognized plot directive '{directive}' — \
                     expected `<metric>`, `over <label>`, `by <label>`, `where <k>=<v>[,…]`, or `agg=…`"
                ));
            }
        }
    }
    Ok(opts)
}

/// Parsed CLI options for `nbrs plot` (metrics form).
#[derive(Debug)]
struct PlotMetricsOpts {
    metric: Option<String>,
    x_label: Option<String>,
    /// One series per distinct *tuple* of values across these
    /// label keys. Empty → single series. `["*"]` → auto-detect:
    /// every label key that has >1 distinct value after filtering
    /// (excluding the x-axis label) becomes part of the series
    /// tuple. The auto-detected set is reported to stderr so the
    /// user can see what was inferred.
    series_labels: Vec<String>,
    /// `(label_key, label_value)` pairs that must all match for a
    /// row to be included.
    filters: Vec<(String, String)>,
    /// Aggregation for the per-window samples that share a
    /// `(series, x)` cell. Default: `mean`.
    agg: String,
    /// Default-source db (first listed). Equivalent to
    /// `dbs.first().clone()` — kept as a separate field for
    /// the existing single-db invariant in `render_one`'s
    /// header diagnostic. Multi-db merge layers more dbs into
    /// `dbs` and treats the union as one logical session.
    db: Option<PathBuf>,
    /// Every db whose rows participate in the query. Populated
    /// from `--db <path>` (repeatable) or `--db <a>,<b>,…`. The
    /// first entry also lands in `db` for default-output-path
    /// derivation. When more than one db is present, queries
    /// run against each and rows are concatenated as if from
    /// one session.
    dbs: Vec<PathBuf>,
    output: Option<PathBuf>,
    title: Option<String>,
    xlabel: Option<String>,
    ylabel: Option<String>,
    xscale: String,
    yscale: String,
    width: u32,
    height: u32,
    /// Print the aggregated (series, x, n, value) table to
    /// stderr alongside the plot — same data the renderer
    /// drew, in tabular form for inspection.
    verbose: bool,
    /// Also write the aggregated points as CSV at this path —
    /// machine-readable counterpart to the plot. Default: none.
    csv_also: Option<PathBuf>,
    /// Path to the framing markdown report. Each rendered plot
    /// is upserted as a `## plot: <name>` section embedding
    /// the PNG. `None` means default `<db_dir>/summary.md`;
    /// `report_disabled = true` suppresses.
    ///
    /// Two write modes (set via `report_mode`):
    /// - [`crate::report::WriteMode::Update`] (`--update-markdown`):
    ///   replace same-anchor sections in place, preserve order.
    ///   Default — keeps the doc fresh as plots are regenerated.
    /// - [`crate::report::WriteMode::AddIfMissing`] (`--add-to-markdown`):
    ///   append only when no section under the same anchor exists.
    ///   Use to build a doc up over many invocations without
    ///   ever changing earlier figures.
    report: Option<PathBuf>,
    report_mode: crate::report::WriteMode,
    /// True when `--report=skip` / `--no-report` was passed.
    /// Suppresses the framing doc entirely.
    report_disabled: bool,
    /// Optional figure number injected by `nbrs report figure N`
    /// (SRD-46) so the markdown heading reads
    /// `## {N}. {Label} (plot) {{#anchor}}`.
    figure_num: Option<usize>,
    /// Optional display label injected via `--label="..."` from
    /// `report_cmd`. Falls back to a prettified item name.
    label: Option<String>,
    /// SRD-46 colorblind-safe palette name or numeric index.
    /// `None` ⇒ default (`wong`).
    palette: Option<String>,
    /// Line dash style (`solid`, `dashed`, `dotted`, `none`).
    /// `None` ⇒ solid.
    line: Option<String>,
    /// Stroke width in pixels. `None` ⇒ 2.
    line_width: Option<f32>,
    /// Marker shape (`none`, `circle`, `square`, `triangle`,
    /// `diamond`, `plus`, `cross`). `None` ⇒ `circle`.
    marker: Option<String>,
    /// Marker radius in pixels. `None` ⇒ 3.
    marker_size: Option<f32>,
    /// Hard-coded axis bounds. `None` for any side ⇒ derive
    /// from data with a 5% padding band. When set, that side's
    /// bound is used verbatim — useful for cross-plot
    /// comparison (so two charts share scales) and for trimming
    /// outliers without --filter.
    x_min: Option<f64>,
    x_max: Option<f64>,
    y_min: Option<f64>,
    y_max: Option<f64>,
    /// Legend placement. Accepts the long form (`top-left`,
    /// `bottom-right`, `center`, …) or one/two-letter codes
    /// (`tl`, `br`, `c`, `t`, `b`, `l`, `r`). `None` ⇒
    /// upper-right (the existing default). `Some("none")`
    /// suppresses the legend.
    legend: Option<String>,
    /// Native MetricsQL expression. When `Some`, the renderer
    /// bypasses the legacy DSL's SQL builder and routes through
    /// `nbrs_metricsql::evaluate` against the session db's
    /// `SqliteDataSource`. The result `Vec<Series>` projects to
    /// the same `(series_name → Vec<(x, y)>)` shape the legacy
    /// path produces. Set via:
    ///
    ///   - `--query "<metricsql>"` on the CLI, or
    ///   - a `query: <metricsql>` directive in a `report:`-block
    ///     plot/table body.
    query: Option<String>,
}

impl Default for PlotMetricsOpts {
    fn default() -> Self {
        Self {
            metric: None,
            x_label: None,
            series_labels: Vec::new(),
            filters: Vec::new(),
            agg: String::new(),
            db: None,
            dbs: Vec::new(),
            output: None,
            title: None,
            xlabel: None,
            ylabel: None,
            xscale: String::new(),
            yscale: String::new(),
            width: 0,
            height: 0,
            verbose: false,
            csv_also: None,
            report: None,
            report_mode: crate::report::WriteMode::Update,
            report_disabled: false,
            figure_num: None,
            label: None,
            palette: None,
            line: None,
            line_width: None,
            marker: None,
            marker_size: None,
            x_min: None,
            x_max: None,
            y_min: None,
            y_max: None,
            legend: None,
            query: None,
        }
    }
}

/// Entry point — called from `plot::plot_command` when the
/// first arg isn't `gk`.
pub fn plot_metrics_command(args: &[String]) {
    register_bundled_font();

    // Two stored-spec entry points before the normal flag-parse
    // path:
    //   - `nbrs plot --name <N>`  → render the stored plot
    //   - `nbrs plot all`         → render every stored plot
    // Stored specs live in the metrics db's `session_metadata`
    // table under `plot.<name>` keys (written by the runner at
    // end-of-run; see `nbrs-activity::runner`).
    if let Some(stored_args) = peel_stored_mode(args) {
        run_stored(stored_args);
        return;
    }

    let opts = match parse_args(args) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("nbrs plot: {e}");
            print_usage();
            std::process::exit(1);
        }
    };
    if let Err(e) = render_one(opts) {
        eprintln!("nbrs plot: {e}");
        std::process::exit(1);
    }
}

/// Same as [`plot_metrics_command`] but returns errors instead
/// of `process::exit`-ing. Used by `nbrs report all` so a
/// per-item failure (e.g. a metric with no rows in the db)
/// doesn't abort the rest of the batch.
///
/// Mirrors the full dispatch in `plot_metrics_command`: stored-
/// mode (`--name <N>` / `all`) routes through `run_stored`, the
/// ad-hoc form goes through `parse_args` + `render_one`.
pub fn plot_metrics_command_result(args: &[String]) -> Result<(), String> {
    register_bundled_font();
    if let Some(stored_args) = peel_stored_mode(args) {
        run_stored_result(stored_args)
    } else {
        let opts = parse_args(args)?;
        render_one(opts)
    }
}

/// Run one plot end-to-end given pre-parsed options.
/// Extracted so `run_stored` (the `--name <N>` / `all` modes)
/// can reuse the same pipeline.
fn render_one(opts: PlotMetricsOpts) -> Result<(), String> {
    // Effective db list: explicit `dbs` if non-empty, else
    // fall back to `db` (single), else `logs/latest/metrics.db`.
    let dbs: Vec<PathBuf> = if !opts.dbs.is_empty() {
        opts.dbs.clone()
    } else if let Some(p) = opts.db.clone() {
        vec![p]
    } else {
        vec![PathBuf::from("logs/latest/metrics.db")]
    };
    for db in &dbs {
        if !db.exists() {
            return Err(format!("metrics db not found at '{}'.", db.display()));
        }
    }
    let primary_db = dbs[0].clone();

    // `metric` is an opaque label used for the title and the
    // default output filename. With `query:` set, the metricsql
    // expression itself is the metric; pull a synthetic name
    // out of it (or fall back to "result") so the rest of the
    // pipeline doesn't need to special-case the query path.
    let synthetic_metric = opts.query.as_ref().map(|q| {
        // Extract a leading identifier-shape token from the
        // expression — e.g. `avg(recall_at_10_mean)` → "avg".
        // Best-effort; user can pin via `--metric` to override.
        q.chars().take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
    }).filter(|s| !s.is_empty());
    let metric_owned: String = opts.metric.clone()
        .or(synthetic_metric)
        .ok_or_else(|| "--metric <pattern> is required (or pass `--query <metricsql>` / a positional spec)".to_string())?;
    let metric = metric_owned.as_str();
    let Some(x_label) = opts.x_label.as_deref() else {
        return Err("--x <label_key> is required (or `over <label>` in the spec)".to_string());
    };

    // 1. Pull rows from every db that matches the metric pattern
    //    + filters. Multi-db: merge into a temp db first so
    //    `session=` labels collapse and same-workload sessions
    //    aggregate as one logical run (consistent with
    //    `nbrs summary --db a --db b …`).
    let query_db: PathBuf = if dbs.len() > 1 {
        match crate::db_merge::merge_dbs(&dbs) {
            Ok(p) => {
                eprintln!("merge: {} dbs → {}", dbs.len(), p.display());
                p
            }
            Err(e) => return Err(format!("merge failed: {e}")),
        }
    } else {
        dbs[0].clone()
    };
    // Two source paths:
    //   - `query: <metricsql>` → evaluate via the metricsql
    //     engine, project the resulting `Vec<Series>` into
    //     `DbRow`s keyed by each Series's labels.
    //   - else → legacy SQL builder over `metric_instance.spec`.
    let rows = if let Some(q) = opts.query.as_deref() {
        rows_via_metricsql(&query_db, q)
            .map_err(|e| format!("metricsql failed against '{}': {e}", query_db.display()))?
    } else {
        query_rows(&query_db, metric, &opts.filters)
            .map_err(|e| format!("query failed against '{}': {e}", query_db.display()))?
    };
    // Default-output paths anchor on the first user-supplied db
    // (not the merge temp) so artifacts live next to real
    // session data.
    let db_path = &primary_db;
    if rows.is_empty() {
        let mut msg = if let Some(q) = opts.query.as_deref() {
            format!("metricsql query returned no series in '{}': `{q}`",
                db_path.display())
        } else {
            format!("no matching rows in '{}' for metric '{metric}'",
                db_path.display())
        };
        if !opts.filters.is_empty() && opts.query.is_none() {
            msg.push_str(&format!(" with filters {}",
                opts.filters.iter().map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>().join(", ")));
        }
        return Err(msg);
    }

    // 2. Resolve series labels.
    //   - Empty → single series.
    //   - `["*"]` → auto-detect: every label that has >1 distinct
    //     value across the rows (excluding x and session).
    //   - Otherwise → explicit list of label keys; one series
    //     per distinct *tuple* of values across them.
    let series_labels: Vec<String> = if opts.series_labels.iter().any(|s| s == "*") {
        let auto = auto_detect_series_labels(&rows, x_label);
        eprintln!("series: auto-detected discriminants: [{}]", auto.join(", "));
        auto
    } else {
        opts.series_labels.clone()
    };

    // Group by (series-tuple, x_value) and aggregate per cell.
    let series: BTreeMap<String, BTreeMap<F64Key, Vec<f64>>> =
        bucket_rows(&rows, x_label, &series_labels);
    if series.is_empty() {
        return Err(format!(
            "rows matched but none yielded a usable ({x_label}, value) pair — \
             check that '{x_label}' is a label on the matched rows."
        ));
    }

    // Aggregate to (x, value) pairs per series. Also keep the
    // n-rows count per cell — we surface it via `--verbose` and
    // `--csv-also` so users can audit the aggregation against
    // raw db queries.
    let aggregated_with_counts: BTreeMap<String, Vec<(f64, f64, usize)>> = series.iter()
        .map(|(sname, by_x)| {
            let mut points: Vec<(f64, f64, usize)> = by_x.iter()
                .map(|(xk, ys)| (xk.0, aggregate(&opts.agg, ys), ys.len()))
                .collect();
            points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            (sname.clone(), points)
        })
        .collect();
    let aggregated: BTreeMap<String, Vec<(f64, f64)>> = aggregated_with_counts.iter()
        .map(|(s, pts)| (s.clone(), pts.iter().map(|(x, y, _)| (*x, *y)).collect()))
        .collect();

    // --verbose: dump the aggregation table to stderr.
    if opts.verbose {
        emit_verbose_table(&aggregated_with_counts, x_label,
            &series_labels, &opts.agg);
    }

    // --csv-also: write the same data as CSV.
    if let Some(csv_path) = opts.csv_also.as_ref() {
        write_csv(csv_path, &aggregated_with_counts, x_label,
            &series_labels, metric, &opts.agg)
            .map_err(|e| format!("failed to write CSV '{}': {e}", csv_path.display()))?;
        eprintln!("csv:  {}", csv_path.display());
    }

    // 3. Output path. Default to PNG — the bundled DejaVu Sans
    //    font (registered by `register_bundled_font`) makes the
    //    bitmap backend hermetic, no system fonts required.
    //    Pass `--output …svg` for vector output (also works,
    //    same code path picks the SVG backend by extension).
    let out_path = opts.output.clone().unwrap_or_else(|| {
        let dir = db_path.parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let safe_metric = sanitize_filename(metric);
        let safe_x = sanitize_filename(x_label);
        dir.join(format!("plot_{safe_metric}_over_{safe_x}.png"))
    });

    // 4. Render.
    render_plot(&aggregated, &opts, x_label, metric, &out_path)
        .map_err(|e| format!("render failed: {e}"))?;

    let total_points: usize = aggregated.values().map(|v| v.len()).sum();
    let series_count = aggregated.len();
    eprintln!("plot: {} ({} series, {} points)",
        out_path.display(), series_count, total_points);

    // Upsert into the framing markdown report (default
    // `<db_dir>/summary.md`) unless `--no-report` /
    // `--report=skip` was passed. The anchor uses the plot's
    // file stem so `nbrs plot --name X` and a re-run of the
    // same name update the same section in place.
    if !opts.report_disabled {
        let report_path = opts.report.clone().unwrap_or_else(|| {
            let dir = db_path.parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            dir.join("summary.md")
        });
        let stem = out_path.file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "plot".into());
        let anchor_name = stem.strip_prefix("plot_").unwrap_or(&stem).to_string();
        let body = crate::report::image_section_body(&report_path, &out_path);
        let label = opts.label.clone()
            .unwrap_or_else(|| crate::report::prettify_name(&anchor_name));
        let heading_display = match opts.figure_num {
            Some(n) => format!("{n}. {label} (plot)"),
            None => format!("{label} (plot)"),
        };
        match crate::report::write_named_section(
            &report_path,
            &anchor_name,
            &heading_display,
            &body,
            opts.report_mode,
        ) {
            Ok(true) => eprintln!("report: {}", report_path.display()),
            Ok(false) => eprintln!("report: {} (skipped — section exists, --add-to-markdown mode)",
                report_path.display()),
            Err(e) => eprintln!("warning: failed to update report '{}': {e}",
                report_path.display()),
        }
    }
    Ok(())
}

/// Detect stored-spec invocation modes (`--name <N>` or `all`)
/// and return a bundle the dispatcher can execute. Returns
/// `None` if the args look like the normal direct-spec form.
struct StoredArgs {
    /// `Some(name)` for `--name <N>`, `None` for `all`.
    target: Option<String>,
    db: Option<PathBuf>,
    /// Path to a workload YAML to use as the spec source
    /// instead of the metrics db's `session_metadata`. When
    /// `Some`, stored plots come from `workload.plots`; the
    /// db is still queried for the actual data values.
    workload: Option<PathBuf>,
    /// Pass-through flags applied to every rendered stored
    /// plot (e.g. `--output`, `--verbose`, `--csv-also`).
    /// Note: when rendering multiple plots, `--output` is
    /// ignored with a warning since each plot needs a distinct
    /// filename.
    extra: Vec<String>,
}

fn peel_stored_mode(args: &[String]) -> Option<StoredArgs> {
    let mut target: Option<String> = None;
    let mut db: Option<PathBuf> = None;
    let mut workload: Option<PathBuf> = None;
    let mut bare_all = false;
    let mut extra: Vec<String> = Vec::new();
    let mut iter = args.iter().peekable();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--name" => {
                target = iter.next().cloned();
            }
            "--db" => {
                db = iter.next().cloned().map(PathBuf::from);
            }
            "all" if !args.iter().any(|x| x == "--name") => {
                // Bare `all` is the "render every stored" mode
                // — only matters when no `--name` is given.
                bare_all = true;
            }
            other if other.starts_with("--db=") => {
                db = Some(PathBuf::from(other.trim_start_matches("--db=")));
            }
            other if other.starts_with("--name=") => {
                target = Some(other.trim_start_matches("--name=").to_string());
            }
            other if other.starts_with("workload=") => {
                let path = other.trim_start_matches("workload=");
                workload = Some(PathBuf::from(
                    crate::cli::resolve_workload_path(path)
                        .unwrap_or_else(|| path.to_string()),
                ));
            }
            _ => {
                extra.push(a.clone());
            }
        }
    }
    if target.is_some() || bare_all {
        Some(StoredArgs { target, db, workload, extra })
    } else {
        None
    }
}

fn run_stored(stored: StoredArgs) {
    let db_path = stored.db.clone().unwrap_or_else(
        || PathBuf::from("logs/latest/metrics.db"));
    if !db_path.exists() {
        eprintln!("nbrs plot: metrics db not found at '{}'.",
            db_path.display());
        std::process::exit(1);
    }
    // Source the spec list: `workload=<path>` wins (use the
    // workload's `plot:` block); otherwise use the metrics
    // db's `session_metadata` table.
    let stored_specs: Vec<(String, String)> = match &stored.workload {
        Some(path) => match load_workload_plots(path) {
            Ok(specs) => specs,
            Err(e) => {
                eprintln!("nbrs plot: workload '{}': {e}", path.display());
                std::process::exit(1);
            }
        },
        None => read_stored_plots(&db_path),
    };
    if stored_specs.is_empty() {
        match &stored.workload {
            Some(path) => eprintln!("nbrs plot: workload '{}' has no `plot:` entries.",
                path.display()),
            None => {
                eprintln!("nbrs plot: '{}' has no stored named plots.", db_path.display());
                eprintln!();
                eprintln!("Use `nbrs plot \"<spec>\"` for an ad-hoc plot, define");
                eprintln!("a `plot:` block in a workload YAML, or pass");
                eprintln!("`workload=<file.yaml>` to source named plots from one.");
            }
        }
        std::process::exit(1);
    }
    let to_render: Vec<(String, String)> = match stored.target {
        Some(name) => {
            let Some(spec) = stored_specs.iter().find(|(n, _)| n == &name) else {
                eprintln!("nbrs plot: no stored plot named '{name}' in '{}'",
                    db_path.display());
                eprintln!();
                eprintln!("Available:");
                for (n, _) in &stored_specs { eprintln!("  {n}"); }
                std::process::exit(1);
            };
            vec![spec.clone()]
        }
        None => stored_specs,
    };
    let multi = to_render.len() > 1;
    if multi && stored.extra.iter().any(|a| a == "--output" || a.starts_with("--output=")) {
        eprintln!("warning: --output is ignored when rendering multiple stored plots; \
                   per-name filenames are derived from each plot's name.");
    }
    let mut any_failed = false;
    for (name, spec) in to_render {
        // Build the args we'd have used for the direct path.
        let mut child_args: Vec<String> = vec![spec.clone()];
        // db override
        child_args.push("--db".into());
        child_args.push(db_path.to_string_lossy().into_owned());
        // Unless multi-render, pass through all extras.
        // Multi-render: omit any --output flag (would clash) and
        // synthesize a distinct output path per name.
        if multi {
            for a in &stored.extra {
                if a == "--output" || a.starts_with("--output=") {
                    continue;
                }
                child_args.push(a.clone());
            }
            let out_path = derive_stored_output_path(&db_path, &name);
            child_args.push("--output".into());
            child_args.push(out_path.to_string_lossy().into_owned());
        } else {
            for a in &stored.extra { child_args.push(a.clone()); }
        }
        eprintln!("--- plot '{name}' ---");
        // Recurse into the normal direct path. We can't actually
        // call plot_metrics_command (it'd re-enter peel mode if
        // `--name` re-appeared, but we don't add that flag), so
        // it's safe.
        let opts = match parse_args(&child_args) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("nbrs plot '{name}': {e}");
                any_failed = true;
                continue;
            }
        };
        if let Err(e) = render_one(opts) {
            eprintln!("nbrs plot '{name}': {e}");
            any_failed = true;
        }
    }
    if any_failed { std::process::exit(1); }
}

/// Result-returning sibling of [`run_stored`] used by
/// `nbrs report all`. Same logic, but errors bubble up
/// instead of `process::exit`-ing.
fn run_stored_result(stored: StoredArgs) -> Result<(), String> {
    let db_path = stored.db.clone().unwrap_or_else(
        || PathBuf::from("logs/latest/metrics.db"));
    if !db_path.exists() {
        return Err(format!("metrics db not found at '{}'", db_path.display()));
    }
    let stored_specs: Vec<(String, String)> = match &stored.workload {
        Some(path) => load_workload_plots(path)
            .map_err(|e| format!("workload '{}': {e}", path.display()))?,
        None => read_stored_plots(&db_path),
    };
    if stored_specs.is_empty() {
        return match &stored.workload {
            Some(path) => Err(format!(
                "workload '{}' has no `plot:` entries", path.display())),
            None => Err(format!(
                "'{}' has no stored named plots", db_path.display())),
        };
    }
    let to_render: Vec<(String, String)> = match stored.target {
        Some(name) => {
            let Some(spec) = stored_specs.iter().find(|(n, _)| n == &name) else {
                return Err(format!(
                    "no stored plot named '{name}' in '{}'", db_path.display()));
            };
            vec![spec.clone()]
        }
        None => stored_specs,
    };
    let mut last_err: Option<String> = None;
    for (name, spec) in to_render {
        let mut child_args: Vec<String> = vec![spec.clone()];
        child_args.push("--db".into());
        child_args.push(db_path.to_string_lossy().into_owned());
        // Stored-mode invocations always render to a per-name
        // output path so the markdown anchor and PNG filename
        // are unique per item — otherwise two plots with the
        // same metric (e.g. `recall@10.mean over limit`) collide
        // on `plot_recall_10.mean_over_limit.png` and the second
        // overwrites the first in both file and section.
        let mut output_overridden = false;
        for a in &stored.extra {
            if a == "--output" || a.starts_with("--output=") { output_overridden = true; }
            child_args.push(a.clone());
        }
        if !output_overridden {
            let out_path = derive_stored_output_path(&db_path, &name);
            child_args.push("--output".into());
            child_args.push(out_path.to_string_lossy().into_owned());
        }
        eprintln!("--- plot '{name}' ---");
        let opts = match parse_args(&child_args) {
            Ok(o) => o,
            Err(e) => { last_err = Some(format!("'{name}': {e}")); continue; }
        };
        if let Err(e) = render_one(opts) {
            last_err = Some(format!("'{name}': {e}"));
        }
    }
    match last_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Public listing for shell completion. Returns stored plot
/// names from the metrics db (alphabetical order). Empty Vec
/// when the db is missing, unreadable, or has none — completion
/// callers expect best-effort, not hard errors.
pub fn list_stored_plot_names(db_path: &Path) -> Vec<String> {
    if !db_path.exists() { return Vec::new(); }
    read_stored_plots(db_path).into_iter().map(|(n, _)| n).collect()
}

/// Public listing for shell completion: every named plot in a
/// workload YAML's `plot:` block. Empty Vec on any error —
/// completion is best-effort.
pub fn list_workload_plot_names(workload_path: &Path) -> Vec<String> {
    load_workload_plots(workload_path)
        .map(|specs| specs.into_iter().map(|(n, _)| n).collect())
        .unwrap_or_default()
}

/// Resolve a named plot's metric family from either a workload
/// YAML's `plot:` block or the metrics db's `session_metadata`
/// table. Returns `None` when the name isn't found.
pub fn metric_for_plot_name(
    db_path: &Path,
    workload_path: Option<&Path>,
    name: &str,
) -> Option<String> {
    let mut spec: Option<String> = None;
    if let Some(wp) = workload_path
        && let Ok(plots) = load_workload_plots(wp) {
        spec = plots.into_iter()
            .find_map(|(n, s)| if n == name { Some(s) } else { None });
    }
    if spec.is_none() {
        spec = read_stored_plots(db_path).into_iter()
            .find_map(|(n, s)| if n == name { Some(s) } else { None });
    }
    let spec = spec?;
    parse_spec(&spec).ok().and_then(|o| o.metric)
}

/// Public listing for shell completion: every distinct label
/// All distinct metric family names recorded in `db_path`.
/// Used by tab-completion for `nbrs plot --metric` so the user
/// gets the closed vocabulary of the session's actual metrics
/// rather than having to remember exact identifiers.
///
/// Empty Vec on any error — completion is best-effort and never
/// panics.
pub fn list_metric_families(db_path: &Path) -> Vec<String> {
    if !db_path.exists() { return Vec::new(); }
    let Ok(conn) = rusqlite::Connection::open(db_path) else { return Vec::new(); };
    let Ok(mut stmt) = conn.prepare(
        "SELECT name FROM metric_family ORDER BY name"
    ) else { return Vec::new(); };
    let Ok(iter) = stmt.query_map([], |row| row.get::<_, String>(0)) else {
        return Vec::new();
    };
    iter.flatten().collect()
}

/// key found across `metric_instance.spec` rows in the db.
/// Optionally restrict to a single metric family (the prefix
/// before `{`). Empty Vec on any error — completion is best-
/// effort.
pub fn list_label_keys(db_path: &Path, metric_pattern: Option<&str>) -> Vec<String> {
    if !db_path.exists() { return Vec::new(); }
    let Ok(conn) = rusqlite::Connection::open(db_path) else { return Vec::new(); };
    let (sql, glob) = match metric_pattern {
        Some(m) => (
            "SELECT DISTINCT spec FROM metric_instance WHERE spec GLOB ?1",
            Some(format!("{m}{{*}}")),
        ),
        None => ("SELECT DISTINCT spec FROM metric_instance", None),
    };
    let Ok(mut stmt) = conn.prepare(sql) else { return Vec::new(); };
    let mut keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut absorb = |spec: String| {
        for k in parse_labels(&spec).into_keys() {
            keys.insert(k);
        }
    };
    if let Some(g) = glob {
        if let Ok(iter) = stmt.query_map([g], |row| row.get::<_, String>(0)) {
            for s in iter.flatten() { absorb(s); }
        }
    } else if let Ok(iter) = stmt.query_map([], |row| row.get::<_, String>(0)) {
        for s in iter.flatten() { absorb(s); }
    }
    keys.into_iter().collect()
}

/// Read a workload YAML's `report:` block. Returns
/// `(name, spec)` pairs for every `plot` item, in
/// declaration order (SRD-46).
fn load_workload_plots(path: &Path) -> Result<Vec<(String, String)>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read: {e}"))?;
    let workload = nbrs_workload::parse::parse_workload(
        &text, &std::collections::HashMap::new(),
    ).map_err(|e| format!("parse: {e}"))?;
    let entries: Vec<(String, String)> = workload.report.items()
        .filter(|i| matches!(i.kind, nbrs_workload::report::Kind::Plot))
        .map(|i| (i.name.clone(), i.body.clone()))
        .collect();
    Ok(entries)
}

fn read_stored_plots(db_path: &Path) -> Vec<(String, String)> {
    // SRD-46: read `report.<name>` rows whose body starts with
    // `plot ...`. Strips the kind keyword + name + optional
    // `label "..."` line so the returned spec is the body the
    // legacy plot renderer expects.
    let Ok(conn) = rusqlite::Connection::open(db_path) else { return Vec::new(); };
    let mut stmt = match conn.prepare(
        "SELECT key, value FROM session_metadata \
         WHERE key LIKE 'report.%' ORDER BY rowid"
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    if let Ok(iter) = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    }) {
        for entry in iter.flatten() {
            let mut lines = entry.1.lines();
            let head = match lines.next() { Some(h) => h, None => continue };
            let name = match head.strip_prefix("plot ") {
                Some(rest) => rest.trim().to_string(),
                None => continue, // skip table items
            };
            let body: String = lines
                .filter(|l| !l.starts_with("label ") && !l.starts_with("target "))
                .collect::<Vec<_>>().join("\n");
            out.push((name, body));
        }
    }
    out
}

fn derive_stored_output_path(db_path: &Path, name: &str) -> PathBuf {
    let dir = db_path.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    // Allow stored names like "myplot.svg" to override the
    // default extension; otherwise default to PNG.
    let p = PathBuf::from(name);
    if p.extension().is_some() {
        dir.join(format!("plot_{name}"))
    } else {
        dir.join(format!("plot_{name}.png"))
    }
}

fn print_usage() {
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  nbrs plot --metric <pattern> --x <label_key> [options...]");
    eprintln!();
    eprintln!("Required:");
    eprintln!("  --metric <pattern>     Metric family name (e.g. recall@10.mean)");
    eprintln!("  --x <label_key>        Label whose values become the X axis (e.g. limit)");
    eprintln!();
    eprintln!("Optional:");
    eprintln!("  --series <label_key>   One line per distinct value of this label");
    eprintln!("  --filter <key>=<value> Restrict to matching rows (repeatable)");
    eprintln!("  --agg <name>           mean (default) | min | max | p50 | p99");
    eprintln!("  --db <path>            Default: logs/latest/metrics.db");
    eprintln!("  --output <path>        Default: <db_dir>/plot_<metric>_over_<x>.png");
    eprintln!("  --title <text>         Plot title");
    eprintln!("  --xlabel <text>        X-axis label (default: <x_label>)");
    eprintln!("  --ylabel <text>        Y-axis label (default: <metric>)");
    eprintln!("  --xscale linear|log    X-axis scale (default: linear)");
    eprintln!("  --yscale linear|log    Y-axis scale (default: linear)");
    eprintln!("  --width <px>           Image width (default: 1024)");
    eprintln!("  --height <px>          Image height (default: 640)");
    eprintln!();
    eprintln!("Example — recall@10 vs limit at k=10:");
    eprintln!("  nbrs plot --metric recall@10.mean --x limit --filter k=10");
}

fn parse_args(args: &[String]) -> Result<PlotMetricsOpts, String> {
    let mut opts = PlotMetricsOpts {
        agg: "mean".to_string(),
        xscale: "linear".to_string(),
        yscale: "linear".to_string(),
        width: 1024,
        height: 640,
        ..Default::default()
    };
    // Honour `--session` / `--session-path` / `--session-name`
    // here so plot output (PNG, summary.md) lands inside the
    // user-specified session dir, not in the `logs/latest`
    // symlink target. Goes through the shared resolver so every
    // session-accessing tool (`plot`, `report`, `metrics ...`)
    // interprets the flag identically.
    //
    // `--db` overrides this — explicit db wins over inferred
    // session.
    if let Some(session_dir) = nbrs_activity::session::read_session_dir(args) {
        opts.db = Some(session_dir.join("metrics.db"));
    }
    // First, sweep for a bare positional spec (one whose token
    // doesn't start with `--`) — that's the single-string DSL
    // form. Apply it as the base, then let flags layer on top
    // and override.
    //
    // Flags-with-values must be skipped: in `--session local/foo
    // --metric recall@1.mean`, the values `local/foo` and
    // `recall@1.mean` aren't `--`-prefixed but they're not
    // positional specs either. Walk pairwise so a flag swallows
    // its value.
    let mut positional: Option<&str> = None;
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if let Some(stripped) = a.strip_prefix("--") {
            // `--flag=value` consumes itself only.
            if stripped.contains('=') { i += 1; continue; }
            // `--flag` followed by a value the parser will read
            // — skip both. `--verbose` and similar bool flags
            // don't take a value, so leave `i+1` for the next
            // pass.
            if FLAGS_TAKING_VALUE.contains(&a.as_str()) && i + 1 < args.len() {
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        // Bare token that isn't a flag-value pair → positional.
        positional = Some(a.as_str());
        break;
    }
    if let Some(spec) = positional {
        opts = parse_spec(spec)?;
    }
    let mut iter = args.iter().peekable();
    while let Some(a) = iter.next() {
        let next = |it: &mut std::iter::Peekable<std::slice::Iter<String>>, flag: &str| {
            it.next().cloned().ok_or_else(|| format!("--{flag} requires a value"))
        };
        match a.as_str() {
            "--metric" => opts.metric = Some(next(&mut iter, "metric")?),
            "--x" => opts.x_label = Some(next(&mut iter, "x")?),
            "--series" => {
                let raw = next(&mut iter, "series")?;
                for k in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    opts.series_labels.push(k.to_string());
                }
            }
            "--filter" => {
                let f = next(&mut iter, "filter")?;
                let (k, v) = f.split_once('=')
                    .ok_or_else(|| format!("--filter expects <key>=<value>, got '{f}'"))?;
                opts.filters.push((k.to_string(), v.to_string()));
            }
            "--agg" => opts.agg = next(&mut iter, "agg")?,
            "--db" => {
                let raw = next(&mut iter, "db")?;
                for path in split_db_arg(&raw) {
                    opts.dbs.push(PathBuf::from(path));
                }
                if opts.db.is_none() {
                    opts.db = opts.dbs.first().cloned();
                }
            }
            "--output" => opts.output = Some(PathBuf::from(next(&mut iter, "output")?)),
            "--title" => opts.title = Some(next(&mut iter, "title")?),
            "--label" => opts.label = Some(next(&mut iter, "label")?),
            "--palette" => opts.palette = Some(next(&mut iter, "palette")?),
            "--line" => opts.line = Some(next(&mut iter, "line")?),
            "--line-width" => opts.line_width = Some(next(&mut iter, "line-width")?
                .parse().map_err(|_| "--line-width must be a number".to_string())?),
            "--marker" => opts.marker = Some(next(&mut iter, "marker")?),
            "--marker-size" => opts.marker_size = Some(next(&mut iter, "marker-size")?
                .parse().map_err(|_| "--marker-size must be a number".to_string())?),
            "--figure-num" => opts.figure_num = Some(next(&mut iter, "figure-num")?
                .parse().map_err(|_| "--figure-num must be a positive integer".to_string())?),
            "--xlabel" => opts.xlabel = Some(next(&mut iter, "xlabel")?),
            "--ylabel" => opts.ylabel = Some(next(&mut iter, "ylabel")?),
            "--xscale" => opts.xscale = next(&mut iter, "xscale")?,
            "--yscale" => opts.yscale = next(&mut iter, "yscale")?,
            "--width" => opts.width = next(&mut iter, "width")?
                .parse().map_err(|_| "--width must be a positive integer".to_string())?,
            "--height" => opts.height = next(&mut iter, "height")?
                .parse().map_err(|_| "--height must be a positive integer".to_string())?,
            "--x-min" => opts.x_min = Some(next(&mut iter, "x-min")?
                .parse().map_err(|_| "--x-min must be a number".to_string())?),
            "--x-max" => opts.x_max = Some(next(&mut iter, "x-max")?
                .parse().map_err(|_| "--x-max must be a number".to_string())?),
            "--y-min" => opts.y_min = Some(next(&mut iter, "y-min")?
                .parse().map_err(|_| "--y-min must be a number".to_string())?),
            "--y-max" => opts.y_max = Some(next(&mut iter, "y-max")?
                .parse().map_err(|_| "--y-max must be a number".to_string())?),
            "--legend" => opts.legend = Some(next(&mut iter, "legend")?),
            "--query" => opts.query = Some(next(&mut iter, "query")?),
            "--verbose" | "-v" => opts.verbose = true,
            // Global flags consumed at startup
            // (`apply_session_directory_at_startup`, SRD-15
            // strict mode). The plot parser sees them in the
            // arg list but has nothing to do — silently
            // accept and skip the value when one is expected.
            "--session" | "--session-name" | "--session-path"
            | "--session-reuse" | "--session-keep" | "--session-shelflife" => {
                let _ = iter.next();
            }
            "--strict" | "--no-prompt" | "--resume-latest" | "--force-retry-failed" => {}
            "--resume" | "--gk-lib" => { let _ = iter.next(); }
            "--csv-also" => opts.csv_also = Some(PathBuf::from(next(&mut iter, "csv-also")?)),
            "--report" | "--update-markdown" => {
                let v = next(&mut iter, "report")?;
                if v == "skip" || v.is_empty() {
                    opts.report_disabled = true;
                } else {
                    opts.report = Some(PathBuf::from(v));
                    opts.report_mode = crate::report::WriteMode::Update;
                }
            }
            "--add-to-markdown" => {
                let v = next(&mut iter, "add-to-markdown")?;
                opts.report = Some(PathBuf::from(v));
                opts.report_mode = crate::report::WriteMode::AddIfMissing;
            }
            "--no-report" => opts.report_disabled = true,
            other if !other.starts_with("--") => {
                // Positional spec — already consumed by the
                // pre-sweep above; skip so it's not treated as
                // an unknown flag.
                continue;
            }
            other => {
                if let Some((k, v)) = other.strip_prefix("--").and_then(|s| s.split_once('=')) {
                    match k {
                        "metric" => opts.metric = Some(v.to_string()),
                        "x" => opts.x_label = Some(v.to_string()),
                        "series" => {
                            for k in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                                opts.series_labels.push(k.to_string());
                            }
                        }
                        "filter" => {
                            let (fk, fv) = v.split_once('=')
                                .ok_or_else(|| format!("--filter expects <key>=<value>, got '{v}'"))?;
                            opts.filters.push((fk.to_string(), fv.to_string()));
                        }
                        "agg" => opts.agg = v.to_string(),
                        "db" => {
                            for path in split_db_arg(v) {
                                opts.dbs.push(PathBuf::from(path));
                            }
                            if opts.db.is_none() {
                                opts.db = opts.dbs.first().cloned();
                            }
                        }
                        "output" => opts.output = Some(PathBuf::from(v)),
                        "title" => opts.title = Some(v.to_string()),
                        "xlabel" => opts.xlabel = Some(v.to_string()),
                        "ylabel" => opts.ylabel = Some(v.to_string()),
                        "xscale" => opts.xscale = v.to_string(),
                        "yscale" => opts.yscale = v.to_string(),
                        "width" => opts.width = v.parse()
                            .map_err(|_| "--width must be a positive integer".to_string())?,
                        "height" => opts.height = v.parse()
                            .map_err(|_| "--height must be a positive integer".to_string())?,
                        "x-min" => opts.x_min = Some(v.parse()
                            .map_err(|_| "--x-min must be a number".to_string())?),
                        "x-max" => opts.x_max = Some(v.parse()
                            .map_err(|_| "--x-max must be a number".to_string())?),
                        "y-min" => opts.y_min = Some(v.parse()
                            .map_err(|_| "--y-min must be a number".to_string())?),
                        "y-max" => opts.y_max = Some(v.parse()
                            .map_err(|_| "--y-max must be a number".to_string())?),
                        "legend" => opts.legend = Some(v.to_string()),
                        "query" => opts.query = Some(v.to_string()),
                        "csv-also" => opts.csv_also = Some(PathBuf::from(v)),
                        "report" | "update-markdown" => {
                            if v == "skip" || v.is_empty() {
                                opts.report_disabled = true;
                            } else {
                                opts.report = Some(PathBuf::from(v));
                                opts.report_mode = crate::report::WriteMode::Update;
                            }
                        }
                        "add-to-markdown" => {
                            opts.report = Some(PathBuf::from(v));
                            opts.report_mode = crate::report::WriteMode::AddIfMissing;
                        }
                        _ => return Err(format!("unknown option: {other}")),
                    }
                } else if matches!(other, "--strict" | "--no-prompt"
                    | "--resume-latest" | "--force-retry-failed")
                    || other.starts_with("--session")
                    || other.starts_with("--gk-lib=")
                    || other.starts_with("--resume=")
                {
                    // Global flag consumed at startup; ignore.
                } else {
                    return Err(format!("unknown argument: {other}"));
                }
            }
        }
    }
    Ok(opts)
}

/// One row from the metrics db. The metric family name already
/// encodes the value channel (e.g. `recall@10.mean`,
/// `latency.p99`), so we only carry `mean` here — `sample_value`
/// stores it under the `mean` column regardless of channel.
#[derive(Debug, Clone)]
struct DbRow {
    /// Full PromQL-style spec: `metric_name{key="val", ...}`.
    spec: String,
    mean: Option<f64>,
    /// Parsed labels — `key → value`.
    labels: std::collections::HashMap<String, String>,
}

/// Evaluate a MetricsQL expression against the session db and
/// project the resulting `Vec<Series>` into the same `DbRow`
/// shape `query_rows` returns, so the downstream
/// `bucket_rows` + per-cell aggregation pipeline doesn't need
/// a separate code path. SRD-46 v2 §"Renderers consume
/// `Vec<Series>`".
///
/// Each `Series` has a label set + a sequence of samples; each
/// sample becomes one `DbRow` with the series's labels and the
/// sample's value as `mean`. The `spec` is synthesised in
/// PromQL-ish form (`__name__{labels...}`) for diagnostics.
fn rows_via_metricsql(db_path: &Path, expr: &str) -> Result<Vec<DbRow>, String> {
    use nbrs_metricsql::adapters::sqlite::SqliteDataSource;
    use nbrs_metricsql::eval::{EvalContext, evaluate};

    let ds = SqliteDataSource::open(db_path)
        .map_err(|e| format!("open metricsql sqlite adapter: {e}"))?;
    let parsed = nbrs_metricsql::parse(expr)
        .map_err(|e| format!("parse metricsql: {e}"))?;
    // Anchor at the latest sample timestamp in the db so the
    // instant query picks up the freshest values. Lookback
    // covers cadence skew (counters and summaries land within
    // ~ms of each other but not always at the exact same
    // timestamp).
    let (start_ms, end_ms) = match latest_sample_window(db_path) {
        Some((s, e)) => (s, e),
        None => return Ok(Vec::new()),
    };
    let ctx = EvalContext {
        data: &ds,
        start_ms,
        end_ms,
        step_ms: 60_000,
        lookback_ms: Some(300_000),
        query_start_ms: Some(start_ms),
        query_end_ms: Some(end_ms),
    };
    let series = evaluate(&ctx, &parsed)
        .map_err(|e| format!("evaluate metricsql: {e}"))?;
    let mut rows: Vec<DbRow> = Vec::new();
    for s in series {
        let labels: std::collections::HashMap<String, String> = s.labels.iter()
            .filter(|(k, _)| k != "__name__")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let name = s.labels.iter()
            .find(|(k, _)| k == "__name__")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "result".into());
        let label_pairs: Vec<String> = labels.iter()
            .map(|(k, v)| format!("{k}=\"{v}\""))
            .collect();
        let spec = if label_pairs.is_empty() {
            name.clone()
        } else {
            format!("{name}{{{}}}", label_pairs.join(","))
        };
        for sample in s.samples {
            rows.push(DbRow {
                spec: spec.clone(),
                mean: Some(sample.value),
                labels: labels.clone(),
            });
        }
    }
    Ok(rows)
}

/// Find the time window for an instant query: anchor at the
/// latest sample timestamp in the db, with a wide enough
/// reach to pull historical samples. `None` when the db has
/// no samples.
fn latest_sample_window(db_path: &Path) -> Option<(i64, i64)> {
    let conn = rusqlite::Connection::open(db_path).ok()?;
    let (min_ts, max_ts): (i64, i64) = conn.query_row(
        "SELECT COALESCE(MIN(timestamp_ms), 0), COALESCE(MAX(timestamp_ms), 0) \
         FROM sample_value",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).ok()?;
    if max_ts == 0 { return None; }
    Some((min_ts, max_ts))
}

/// Pull every row whose metric family matches `metric_pattern`
/// and whose labels include every `filter` k=v pair. The metric
/// pattern matches a leading prefix on the spec — i.e. `metric_pattern`
/// is the metric family name, with the labels following in `{...}`.
fn query_rows(
    db_path: &Path,
    metric_pattern: &str,
    filters: &[(String, String)],
) -> Result<Vec<DbRow>, String> {
    use rusqlite::Connection;
    let conn = Connection::open(db_path)
        .map_err(|e| format!("open db: {e}"))?;

    // The metric family is the prefix before `{`. Use SQLite's
    // GLOB to match `<metric>{*}` exactly so `recall@10.mean`
    // doesn't accidentally match `recall@100.mean`.
    let pattern = format!("{metric_pattern}{{*}}");
    let mut stmt = conn.prepare(
        "SELECT mi.spec, sv.mean \
         FROM sample_value sv \
         JOIN metric_instance mi ON sv.instance_id = mi.id \
         WHERE mi.spec GLOB ?1",
    ).map_err(|e| format!("prepare: {e}"))?;

    let mut rows = Vec::new();
    let iter = stmt.query_map([pattern], |r| {
        Ok(DbRow {
            spec: r.get::<_, String>(0)?,
            mean: r.get::<_, Option<f64>>(1)?,
            labels: std::collections::HashMap::new(),
        })
    }).map_err(|e| format!("query_map: {e}"))?;
    for row in iter.flatten() {
        let mut row = row;
        row.labels = parse_labels(&row.spec);
        // Filter match: exact for `key=value`, substring for
        // values prefixed with `~` (the in-band marker the
        // `key~pattern` directive form maps onto). Anchored
        // wildcards aren't a thing yet — `~x` matches any
        // value that contains `x` as a substring.
        if !filters.iter().all(|(k, v)| {
            row.labels.get(k).map(|x| {
                if let Some(pat) = v.strip_prefix('~') {
                    x.contains(pat)
                } else {
                    x == v
                }
            }).unwrap_or(false)
        }) {
            continue;
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Parse the `metric_name{key="value", key="value"}` shape into
/// a key→value map.
fn parse_labels(spec: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    let Some(open) = spec.find('{') else { return out; };
    let Some(close) = spec.rfind('}') else { return out; };
    if close <= open + 1 { return out; }
    let body = &spec[open + 1..close];
    // Split on commas at depth 0; values are quoted.
    let mut depth = 0;
    let mut start = 0;
    let bytes = body.as_bytes();
    let mut parts = Vec::new();
    for i in 0..bytes.len() {
        let c = bytes[i];
        if c == b'"' {
            // Skip to matching quote
            depth = 1 - depth;
        } else if c == b',' && depth == 0 {
            parts.push(&body[start..i]);
            start = i + 1;
        }
    }
    parts.push(&body[start..]);
    for p in parts {
        let p = p.trim();
        if let Some((k, v)) = p.split_once('=') {
            let v = v.trim().trim_start_matches('"').trim_end_matches('"');
            out.insert(k.trim().to_string(), v.to_string());
        }
    }
    out
}

/// Split a `--db` value: a comma-separated list yields N
/// paths; a single path yields one. Trims whitespace; empty
/// fragments are dropped silently (so `,,` = "no extra db").
fn split_db_arg(raw: &str) -> Vec<String> {
    raw.split(',').map(str::trim).filter(|s| !s.is_empty())
        .map(|s| s.to_string()).collect()
}

/// Wraps an f64 so it can be a BTreeMap key. NaN compares equal to
/// itself for this purpose; NaN inputs from the db are rare and
/// would not produce a meaningful X-axis position.
#[derive(Debug, Clone, Copy)]
struct F64Key(f64);
impl PartialEq for F64Key {
    fn eq(&self, other: &Self) -> bool { self.0.to_bits() == other.0.to_bits() }
}
impl Eq for F64Key {}
impl PartialOrd for F64Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for F64Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Group rows by (series-tuple, x_label_value) and collect the
/// per-cell `mean` samples. `series_labels` is the ordered list
/// of label keys whose values, taken as a tuple, define a
/// series. Empty list → single series (everything aggregates
/// into one line).
///
/// Series-tuple keys are formatted as `k1=v1, k2=v2, …` so the
/// plot legend reads naturally — for the user's "(k=10,
/// optimize_for=RECALL)" use case, the legend literally shows
/// `k=10, optimize_for=RECALL`.
fn bucket_rows(
    rows: &[DbRow],
    x_label: &str,
    series_labels: &[String],
) -> BTreeMap<String, BTreeMap<F64Key, Vec<f64>>> {
    let mut out: BTreeMap<String, BTreeMap<F64Key, Vec<f64>>> = BTreeMap::new();
    for row in rows {
        let Some(x_str) = row.labels.get(x_label) else { continue; };
        let Ok(x_val) = x_str.parse::<f64>() else { continue; };
        let Some(y_val) = row.mean else { continue; };
        let series_key = series_tuple_key(&row.labels, series_labels);
        out.entry(series_key)
            .or_default()
            .entry(F64Key(x_val))
            .or_default()
            .push(y_val);
    }
    out
}

/// Build the legend key for a row given the ordered series
/// labels. Format: `key1=value1, key2=value2, …`. Missing
/// labels render as `key=(unset)` so the bucketing stays
/// deterministic even when a row is missing one of the
/// series dims (rather than silently grouping it with rows
/// that have a different value).
fn series_tuple_key(
    labels: &std::collections::HashMap<String, String>,
    series_labels: &[String],
) -> String {
    if series_labels.is_empty() {
        return String::new();
    }
    series_labels.iter()
        .map(|k| {
            let v = labels.get(k).cloned().unwrap_or_else(|| "(unset)".to_string());
            format!("{k}={v}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Auto-detect series labels: every label key that has more than
/// one distinct value across the supplied rows, excluding
/// `x_label` (which is the X axis, not a series discriminator).
/// Returns the keys in stable alphabetical order so the legend
/// formatting is deterministic.
fn auto_detect_series_labels(rows: &[DbRow], x_label: &str) -> Vec<String> {
    let mut by_key: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    for row in rows {
        for (k, v) in &row.labels {
            if k == x_label { continue; }
            // `session` was already stripped by db_merge for
            // multi-db; the per-db case still has it. Exclude it
            // from auto-series so single-db plots don't
            // accidentally dimensionalize on session id.
            if k == "session" { continue; }
            by_key.entry(k.clone()).or_default().insert(v.clone());
        }
    }
    let mut varying: Vec<String> = by_key.into_iter()
        .filter(|(_, vs)| vs.len() > 1)
        .map(|(k, _)| k)
        .collect();
    varying.sort();
    varying
}

fn aggregate(name: &str, vals: &[f64]) -> f64 {
    if vals.is_empty() { return f64::NAN; }
    match name {
        "mean" => vals.iter().sum::<f64>() / vals.len() as f64,
        "min" => vals.iter().cloned().fold(f64::INFINITY, f64::min),
        "max" => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        "p50" | "median" => percentile(vals, 0.50),
        "p99" => percentile(vals, 0.99),
        _ => vals.iter().sum::<f64>() / vals.len() as f64,
    }
}

fn percentile(vals: &[f64], p: f64) -> f64 {
    let mut sorted = vals.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn sanitize_filename(s: &str) -> String {
    s.chars().map(|c| match c {
        'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '-' | '.' => c,
        _ => '_',
    }).collect()
}

fn render_plot(
    series: &BTreeMap<String, Vec<(f64, f64)>>,
    opts: &PlotMetricsOpts,
    x_label: &str,
    metric: &str,
    out_path: &Path,
) -> Result<(), String> {
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create output dir '{}': {e}", parent.display()))?;
        }
    }

    // Compute axis ranges across all series.
    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    let mut y_min = f64::INFINITY;
    let mut y_max = f64::NEG_INFINITY;
    for points in series.values() {
        for &(x, y) in points {
            if x.is_finite() {
                if x < x_min { x_min = x; }
                if x > x_max { x_max = x; }
            }
            if y.is_finite() {
                if y < y_min { y_min = y; }
                if y > y_max { y_max = y; }
            }
        }
    }
    if !x_min.is_finite() || !x_max.is_finite() || !y_min.is_finite() || !y_max.is_finite() {
        return Err("no finite (x, y) points to plot".to_string());
    }
    // Padding so points don't sit on the axis edges. User-set
    // bounds (via `--x-min` / `--x-max` / `--y-min` / `--y-max`)
    // win verbatim — no padding applied — so two charts can
    // share scales when the user pins them explicitly.
    let x_pad = ((x_max - x_min) * 0.05).max(1e-9);
    let y_pad = ((y_max - y_min) * 0.05).max(1e-9);
    let x_lo = opts.x_min.unwrap_or(x_min - x_pad);
    let x_hi = opts.x_max.unwrap_or(x_max + x_pad);
    let y_lo = opts.y_min.unwrap_or(y_min - y_pad);
    let y_hi = opts.y_max.unwrap_or(y_max + y_pad);
    let x_range = x_lo..x_hi;
    let y_range = y_lo..y_hi;

    let title = opts.title.clone().unwrap_or_else(|| {
        let filter_summary = if opts.filters.is_empty() {
            String::new()
        } else {
            format!(
                " [{}]",
                opts.filters.iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>().join(", "),
            )
        };
        format!("{metric} vs {x_label}{filter_summary}")
    });

    let x_axis = opts.xlabel.clone().unwrap_or_else(|| x_label.to_string());
    let y_axis = opts.ylabel.clone().unwrap_or_else(|| metric.to_string());

    // Pick the backend by output extension. SVG is the default
    // hermetic path; PNG goes through the bitmap backend (which
    // needs system fonts to render text labels — fails fast with
    // a clear error if they're missing).
    let is_svg = out_path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("svg"))
        .unwrap_or(false);

    let legend_spec = parse_legend_spec(opts.legend.as_deref())?;
    let legend_explicit = opts.legend.is_some();

    if is_svg {
        let root = SVGBackend::new(out_path, (opts.width, opts.height)).into_drawing_area();
        draw_chart(&root, series, &title, &x_axis, &y_axis, x_range, y_range, metric,
            opts.palette.as_deref(), opts.line.as_deref(), opts.line_width,
            opts.marker.as_deref(), opts.marker_size, legend_spec, legend_explicit)?;
        root.present().map_err(|e| format!("present: {e}"))?;
    } else {
        let root = BitMapBackend::new(out_path, (opts.width, opts.height)).into_drawing_area();
        draw_chart(&root, series, &title, &x_axis, &y_axis, x_range, y_range, metric,
            opts.palette.as_deref(), opts.line.as_deref(), opts.line_width,
            opts.marker.as_deref(), opts.marker_size, legend_spec, legend_explicit)?;
        root.present().map_err(|e| format!("present: {e}"))?;
    }
    Ok(())
}

/// Resolved legend placement. `Suppressed` = `--legend none` /
/// `--legend off`.
#[derive(Debug, Clone)]
enum LegendSpec {
    Position(SeriesLabelPosition),
    Suppressed,
}

/// Parse a user-supplied legend code into a [`LegendSpec`].
/// Accepts the long form (`top-left`, `bottom-right`, `center`,
/// `top`, `bottom`, `left`, `right`, plus `top-center`,
/// `bottom-center`) or one/two-letter codes (`tl`, `tr`, `bl`,
/// `br`, `t`, `b`, `l`, `r`, `c`, `tc`, `bc`).
///
/// Returns the existing default (UpperRight) when input is
/// `None`. Returns `Suppressed` for `none` / `off`.
fn parse_legend_spec(arg: Option<&str>) -> Result<LegendSpec, String> {
    let Some(raw) = arg else {
        return Ok(LegendSpec::Position(SeriesLabelPosition::UpperRight));
    };
    let key = raw.trim().to_ascii_lowercase();
    let pos = match key.as_str() {
        "none" | "off" | "hide" => return Ok(LegendSpec::Suppressed),
        // Long form
        "top-left"     | "upper-left"     => SeriesLabelPosition::UpperLeft,
        "top"          | "top-center"     | "upper" | "upper-center"
                                          => SeriesLabelPosition::UpperMiddle,
        "top-right"    | "upper-right"    => SeriesLabelPosition::UpperRight,
        "left"         | "middle-left"    => SeriesLabelPosition::MiddleLeft,
        "center"       | "middle"         => SeriesLabelPosition::MiddleMiddle,
        "right"        | "middle-right"   => SeriesLabelPosition::MiddleRight,
        "bottom-left"  | "lower-left"     => SeriesLabelPosition::LowerLeft,
        "bottom"       | "bottom-center"  | "lower" | "lower-center"
                                          => SeriesLabelPosition::LowerMiddle,
        "bottom-right" | "lower-right"    => SeriesLabelPosition::LowerRight,
        // Single / two-letter shortcodes
        "tl" | "ul" => SeriesLabelPosition::UpperLeft,
        "t"  | "tc" | "uc" | "u" => SeriesLabelPosition::UpperMiddle,
        "tr" | "ur" => SeriesLabelPosition::UpperRight,
        "l"  | "ml" | "cl" => SeriesLabelPosition::MiddleLeft,
        "c"  | "m"  | "mm" | "mc" => SeriesLabelPosition::MiddleMiddle,
        "r"  | "mr" | "cr" => SeriesLabelPosition::MiddleRight,
        "bl" | "ll" => SeriesLabelPosition::LowerLeft,
        "b"  | "bc" | "lc" => SeriesLabelPosition::LowerMiddle,
        "br" | "lr" => SeriesLabelPosition::LowerRight,
        other => return Err(format!(
            "--legend: unknown position '{other}' (try: top-left, top, \
             top-right, left, center, right, bottom-left, bottom, \
             bottom-right; shortcodes tl/t/tr/l/c/r/bl/b/br; or `none` to suppress)"
        )),
    };
    Ok(LegendSpec::Position(pos))
}

fn draw_chart<DB>(
    root: &DrawingArea<DB, plotters::coord::Shift>,
    series: &BTreeMap<String, Vec<(f64, f64)>>,
    title: &str,
    x_axis: &str,
    y_axis: &str,
    x_range: std::ops::Range<f64>,
    y_range: std::ops::Range<f64>,
    metric: &str,
    palette_spec: Option<&str>,
    line_style: Option<&str>,
    line_width: Option<f32>,
    marker_shape: Option<&str>,
    marker_size: Option<f32>,
    legend_spec: LegendSpec,
    legend_explicit: bool,
) -> Result<(), String>
where
    DB: DrawingBackend,
    DB::ErrorType: 'static,
{
    root.fill(&WHITE).map_err(|e| format!("fill: {e}"))?;

    let mut chart_builder = ChartBuilder::on(root);
    chart_builder
        .caption(title, ("sans-serif", 24))
        .margin(20)
        .x_label_area_size(50)
        .y_label_area_size(70)
        .right_y_label_area_size(20);

    let mut chart = chart_builder
        .build_cartesian_2d(x_range, y_range)
        .map_err(|e| format!("build chart: {e}"))?;

    chart.configure_mesh()
        .x_desc(x_axis)
        .y_desc(y_axis)
        .label_style(("sans-serif", 14))
        .axis_desc_style(("sans-serif", 16))
        .draw()
        .map_err(|e| format!("draw mesh: {e}"))?;

    let palette = crate::palette::resolve_or_default(palette_spec);
    let stroke_width = line_width.map(|w| w.max(0.0) as u32).unwrap_or(2);
    let line_kind = line_style.unwrap_or("solid");
    let marker_kind = marker_shape.unwrap_or("circle");
    let m_size = marker_size.map(|s| s.max(0.0) as i32).unwrap_or(3);

    for (idx, (series_name, points)) in series.iter().enumerate() {
        let color = crate::palette::series_color(palette, idx);
        let series_label_for_legend = if series_name.is_empty() {
            metric.to_string()
        } else {
            series_name.clone()
        };
        let pts = points.clone();

        // Line component — solid (default), dashed, dotted (visual
        // approximation: short dashes), or none.
        match line_kind {
            "none" => {} // no line
            "dashed" => {
                chart.draw_series(plotters::series::DashedLineSeries::new(
                    pts.iter().cloned(), 8, 8, color.stroke_width(stroke_width),
                ))
                .map_err(|e| format!("draw dashed line: {e}"))?
                .label(series_label_for_legend.clone())
                .legend(move |(x, y)| PathElement::new(
                    vec![(x, y), (x + 20, y)], color.stroke_width(stroke_width)));
            }
            "dotted" => {
                chart.draw_series(plotters::series::DashedLineSeries::new(
                    pts.iter().cloned(), 2, 4, color.stroke_width(stroke_width),
                ))
                .map_err(|e| format!("draw dotted line: {e}"))?
                .label(series_label_for_legend.clone())
                .legend(move |(x, y)| PathElement::new(
                    vec![(x, y), (x + 20, y)], color.stroke_width(stroke_width)));
            }
            _ => {
                chart.draw_series(LineSeries::new(
                    pts.iter().cloned(), color.stroke_width(stroke_width)))
                    .map_err(|e| format!("draw line: {e}"))?
                    .label(series_label_for_legend.clone())
                    .legend(move |(x, y)| PathElement::new(
                        vec![(x, y), (x + 20, y)], color.stroke_width(stroke_width)));
            }
        }

        // Marker component — overlay shapes at each datapoint.
        // Hand-rolled shapes so we don't need extra plotters
        // features.
        match marker_kind {
            "none" => {}
            "circle" => {
                chart.draw_series(pts.iter()
                    .map(|p| Circle::new(*p, m_size, color.filled())))
                    .map_err(|e| format!("draw circles: {e}"))?;
            }
            "square" => {
                let off = m_size as f64;
                chart.draw_series(pts.iter().map(|p| {
                    Rectangle::new(
                        [(p.0 - off, p.1 - off), (p.0 + off, p.1 + off)],
                        color.filled(),
                    )
                })).map_err(|e| format!("draw squares: {e}"))?;
            }
            "triangle" => {
                chart.draw_series(pts.iter()
                    .map(|p| TriangleMarker::new(*p, m_size, color.filled())))
                    .map_err(|e| format!("draw triangles: {e}"))?;
            }
            "diamond" => {
                let off = m_size as f64;
                chart.draw_series(pts.iter().map(|p| {
                    Polygon::new(vec![
                        (p.0, p.1 - off),
                        (p.0 + off, p.1),
                        (p.0, p.1 + off),
                        (p.0 - off, p.1),
                    ], color.filled())
                })).map_err(|e| format!("draw diamonds: {e}"))?;
            }
            "plus" => {
                chart.draw_series(pts.iter()
                    .map(|p| Cross::new(*p, m_size, color.stroke_width(stroke_width))))
                    .map_err(|e| format!("draw plus: {e}"))?;
            }
            "cross" => {
                let off = m_size as f64;
                chart.draw_series(pts.iter().flat_map(|p| {
                    let p = *p;
                    [
                        PathElement::new(vec![
                            (p.0 - off, p.1 - off), (p.0 + off, p.1 + off),
                        ], color.stroke_width(stroke_width)),
                        PathElement::new(vec![
                            (p.0 - off, p.1 + off), (p.0 + off, p.1 - off),
                        ], color.stroke_width(stroke_width)),
                    ]
                })).map_err(|e| format!("draw crosses: {e}"))?;
            }
            other => {
                eprintln!("warning: unknown marker '{other}'; falling back to circle");
                chart.draw_series(pts.iter()
                    .map(|p| Circle::new(*p, m_size, color.filled())))
                    .map_err(|e| format!("draw circles: {e}"))?;
            }
        }
    }

    // Draw the legend whenever the user has either:
    //   - multiple series / non-default series naming (the
    //     "the legend has something useful to show" case), OR
    //   - explicitly requested a position via `--legend` (any
    //     value other than the default — `--legend tl`,
    //     `--legend center`, …) — override the auto-skip so a
    //     single-series plot still gets a labelled box when
    //     the user asked for one.
    //
    // `LegendSpec::Suppressed` (`--legend none|off|hide`) skips
    // unconditionally.
    let auto_show = series.len() > 1 || series.keys().any(|k| !k.is_empty());
    let force_show = matches!(legend_spec, LegendSpec::Position(_)) && legend_explicit;
    if (auto_show || force_show) && matches!(legend_spec, LegendSpec::Position(_)) {
        if let LegendSpec::Position(position) = legend_spec {
            chart.configure_series_labels()
                .background_style(WHITE.mix(0.85))
                .border_style(BLACK)
                .label_font(("sans-serif", 14))
                .position(position)
                .draw()
                .map_err(|e| format!("draw legend: {e}"))?;
        }
    }

    Ok(())
}

/// Print the aggregation table to stderr — same data the
/// renderer drew, in plain text for inspection. Columns:
/// optional series label, x label, n_rows aggregated, agg
/// value. Aligned for legibility.
fn emit_verbose_table(
    aggregated: &BTreeMap<String, Vec<(f64, f64, usize)>>,
    x_label: &str,
    series_labels: &[String],
    agg_name: &str,
) {
    // Header for the series tuple — joined like the values:
    // `k, optimize_for` for `["k", "optimize_for"]`. Empty when
    // no series labels are configured.
    let series_header = series_labels.join(", ");
    let value_header = format!("{agg_name}(value)");

    let mut series_w = series_header.len();
    let mut x_w = x_label.len();
    let mut n_w = 1usize;
    let mut v_w = value_header.len();
    for (sname, pts) in aggregated {
        if !series_header.is_empty() {
            series_w = series_w.max(sname.len());
        }
        for (x, v, n) in pts {
            x_w = x_w.max(format_x(*x).len());
            n_w = n_w.max(format!("{n}").len());
            v_w = v_w.max(format!("{v:.6}").len());
        }
    }

    let mut header = String::new();
    if !series_header.is_empty() {
        header.push_str(&format!("{:<w$}  ", series_header, w = series_w));
    }
    header.push_str(&format!("{:>w$}  ", x_label, w = x_w));
    header.push_str(&format!("{:>w$}  ", "n", w = n_w));
    header.push_str(&format!("{:>w$}", value_header, w = v_w));
    eprintln!();
    eprintln!("{header}");
    eprintln!("{}", "-".repeat(header.len()));

    for (sname, pts) in aggregated {
        for (x, v, n) in pts {
            let mut row = String::new();
            if !series_header.is_empty() {
                row.push_str(&format!("{:<w$}  ", sname, w = series_w));
            }
            row.push_str(&format!("{:>w$}  ", format_x(*x), w = x_w));
            row.push_str(&format!("{:>w$}  ", n, w = n_w));
            row.push_str(&format!("{:>w$.6}", v, w = v_w));
            eprintln!("{row}");
        }
    }
    eprintln!();
}

/// Format an x-axis value compactly: integers without decimals,
/// otherwise up to 6 significant digits.
fn format_x(x: f64) -> String {
    if x.fract().abs() < 1e-9 && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        format!("{x:.6}")
    }
}

/// Write the aggregated points as CSV. Columns:
/// `[<series_label_1>, <series_label_2>, …,] <x_label>, n_rows, <agg>(<metric>)`.
/// Multi-key series get one column per series-label so the
/// data is machine-friendly without parsing the joined-tuple
/// string back apart.
fn write_csv(
    path: &Path,
    aggregated: &BTreeMap<String, Vec<(f64, f64, usize)>>,
    x_label: &str,
    series_labels: &[String],
    metric: &str,
    agg_name: &str,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create output dir '{}': {e}", parent.display()))?;
        }
    }
    let mut out = String::new();
    if series_labels.is_empty() {
        out.push_str(&format!("{x_label},n_rows,{agg_name}({metric})\n"));
        for pts in aggregated.values() {
            for (x, v, n) in pts {
                out.push_str(&format!("{},{},{:.6}\n", format_x(*x), n, v));
            }
        }
    } else {
        let header_keys: String = series_labels.iter()
            .map(|k| csv_escape(k))
            .collect::<Vec<_>>().join(",");
        out.push_str(&format!("{header_keys},{x_label},n_rows,{agg_name}({metric})\n"));
        for (sname, pts) in aggregated {
            // Reconstruct the per-key values from the joined
            // tuple key (`k1=v1, k2=v2, …`). Reconstruction is
            // deterministic because `series_tuple_key` always
            // emits the keys in `series_labels` order.
            let values = parse_tuple_key(sname, series_labels);
            for (x, v, n) in pts {
                let cells: Vec<String> = values.iter()
                    .map(|s| csv_escape(s)).collect();
                out.push_str(&format!(
                    "{},{},{},{:.6}\n",
                    cells.join(","),
                    format_x(*x),
                    n,
                    v,
                ));
            }
        }
    }
    std::fs::write(path, out)
        .map_err(|e| format!("write csv: {e}"))?;
    Ok(())
}

/// Inverse of `series_tuple_key`: given `"k=10, optimize_for=RECALL"`
/// and `["k", "optimize_for"]`, returns `["10", "RECALL"]`.
fn parse_tuple_key(key: &str, labels: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(labels.len());
    let map: std::collections::HashMap<&str, &str> = key
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|kv| kv.split_once('='))
        .collect();
    for label in labels {
        out.push(map.get(label.as_str()).unwrap_or(&"").to_string());
    }
    out
}

/// CSV-escape: double quotes any field that contains a comma,
/// newline, or quote; otherwise pass through.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('\n') || s.contains('"') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Aggregator keywords that may appear as a prefix on a plot
/// directive (`mean recall@10 over limit` ≡
/// `recall@10.mean over limit`). Lower-case only — directive
/// parsing is case-sensitive.
const AGG_PREFIXES: &[&str] = &[
    "mean", "min", "max", "p50", "p99", "p999", "sum", "count",
];

/// If `directive` starts with one of the aggregator keywords
/// followed by whitespace, return `(agg, rest_after_agg)`.
fn strip_agg_prefix(directive: &str) -> Option<(&str, &str)> {
    for agg in AGG_PREFIXES {
        if let Some(rest) = directive.strip_prefix(agg)
            && let Some(c) = rest.chars().next()
            && c.is_whitespace() {
            return Some((agg, rest.trim_start()));
        }
    }
    None
}

/// Function-call aggregator form: `mean(recall@10) over limit`
/// ≡ `recall@10.mean over limit`. Returns `(agg, metric, rest_after_close_paren)`
/// when the directive starts with `<agg>(<metric>)` followed by
/// whitespace or end-of-string.
fn parse_function_agg(directive: &str) -> Option<(&str, &str, &str)> {
    let open = directive.find('(')?;
    let agg = directive[..open].trim();
    if !AGG_PREFIXES.contains(&agg) { return None; }
    let close_rel = directive[open + 1..].find(')')?;
    let metric = directive[open + 1..open + 1 + close_rel].trim();
    if metric.is_empty() { return None; }
    let after = directive[open + 1 + close_rel + 1..].trim();
    Some((agg, metric, after))
}

/// Strip `#` line comments from a multi-line spec body. A `#`
/// starts a comment only when it's at line-start or preceded by
/// whitespace — so hex colors (`#117733`) and JSON sub-blocks
/// (`{"color": "#fff"}`) survive. Quoted strings are honoured.
fn strip_line_comments(s: &str) -> String {
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
mod tests {
    use super::*;

    #[test]
    fn agg_prefix_rewrites_to_dotted() {
        // `mean recall@10 over limit` ≡ `recall@10.mean over limit`.
        let opts = parse_spec("mean recall@10 over limit where k=10").unwrap();
        assert_eq!(opts.metric.as_deref(), Some("recall@10.mean"));
        assert_eq!(opts.x_label.as_deref(), Some("limit"));
    }

    #[test]
    fn function_agg_with_substring_filter() {
        // From SRD-46 example workload: function-call agg form,
        // multi-key `over` with `~` substring filter on profile.
        let opts = parse_spec(
            "mean(recall) over profile~label,limit where k=1"
        ).unwrap();
        assert_eq!(opts.metric.as_deref(), Some("recall.mean"));
        assert_eq!(opts.x_label.as_deref(), Some("limit"));
        // `profile~label` becomes a substring filter.
        assert!(opts.filters.contains(&("profile".into(), "~label".into())),
            "filters: {:?}", opts.filters);
        // `where k=1` is a regular exact filter.
        assert!(opts.filters.contains(&("k".into(), "1".into())));
    }

    #[test]
    fn agg_prefix_other_aggs() {
        let p99 = parse_spec("p99 latency over rate").unwrap();
        assert_eq!(p99.metric.as_deref(), Some("latency.p99"));
        let max = parse_spec("max recall over k").unwrap();
        assert_eq!(max.metric.as_deref(), Some("recall.max"));
    }

    #[test]
    fn hash_line_comments_in_spec_stripped() {
        let spec = "recall@10.mean over limit where k=10  # narrow to k=10";
        let opts = parse_spec(spec).unwrap();
        assert_eq!(opts.metric.as_deref(), Some("recall@10.mean"));
        // Filter parsed correctly even with trailing comment.
        assert_eq!(opts.filters, vec![("k".to_string(), "10".to_string())]);
    }

    #[test]
    fn parses_label_spec() {
        let spec = "recall@10.mean{session=\"abc\",profile=\"label_03\",k=\"10\",limit=\"50\"}";
        let labels = parse_labels(spec);
        assert_eq!(labels.get("k").map(String::as_str), Some("10"));
        assert_eq!(labels.get("limit").map(String::as_str), Some("50"));
        assert_eq!(labels.get("profile").map(String::as_str), Some("label_03"));
    }

    #[test]
    fn aggregate_mean() {
        assert!((aggregate("mean", &[1.0, 2.0, 3.0]) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn aggregate_min_max() {
        assert!((aggregate("min", &[3.0, 1.0, 2.0]) - 1.0).abs() < 1e-9);
        assert!((aggregate("max", &[3.0, 1.0, 2.0]) - 3.0).abs() < 1e-9);
    }

    #[test]
    fn aggregate_p50() {
        assert!((aggregate("p50", &[1.0, 2.0, 3.0, 4.0, 5.0]) - 3.0).abs() < 1e-9);
    }

    #[test]
    fn bucket_rows_groups_by_series_and_x() {
        fn row(spec: &str, mean: f64) -> DbRow {
            DbRow {
                spec: spec.to_string(),
                mean: Some(mean),
                labels: parse_labels(spec),
            }
        }
        let rows = vec![
            row("recall{profile=\"a\",limit=\"10\"}", 0.8),
            row("recall{profile=\"a\",limit=\"20\"}", 0.9),
            row("recall{profile=\"b\",limit=\"10\"}", 0.7),
            row("recall{profile=\"b\",limit=\"20\"}", 0.85),
        ];
        let buckets = bucket_rows(&rows, "limit", &["profile".to_string()]);
        assert_eq!(buckets.len(), 2);
        assert!(buckets.contains_key("profile=a"));
        assert!(buckets.contains_key("profile=b"));
        assert_eq!(buckets["profile=a"].len(), 2);
    }

    #[test]
    fn spec_simple_one_liner() {
        let opts = parse_spec("recall@10.mean over limit where k=10").unwrap();
        assert_eq!(opts.metric.as_deref(), Some("recall@10.mean"));
        assert_eq!(opts.x_label.as_deref(), Some("limit"));
        assert_eq!(opts.filters, vec![("k".to_string(), "10".to_string())]);
        assert!(opts.series_labels.is_empty());
    }

    #[test]
    fn spec_with_series() {
        let opts = parse_spec("recall@10.mean over limit by profile where k=10").unwrap();
        assert_eq!(opts.metric.as_deref(), Some("recall@10.mean"));
        assert_eq!(opts.x_label.as_deref(), Some("limit"));
        assert_eq!(opts.series_labels, vec!["profile".to_string()]);
        assert_eq!(opts.filters, vec![("k".to_string(), "10".to_string())]);
    }

    #[test]
    fn spec_multi_key_by() {
        let opts = parse_spec(
            "recall@10.mean over limit by k,optimize_for where phase=ann_query"
        ).unwrap();
        assert_eq!(opts.series_labels,
            vec!["k".to_string(), "optimize_for".to_string()]);
        assert_eq!(opts.x_label.as_deref(), Some("limit"));
    }

    #[test]
    fn spec_by_star_for_auto_detect() {
        let opts = parse_spec("recall@10.mean over limit by *").unwrap();
        assert_eq!(opts.series_labels, vec!["*".to_string()]);
    }

    #[test]
    fn spec_multi_key_with_spaces() {
        let opts = parse_spec(
            "recall@10.mean over limit by k, optimize_for ,phase"
        ).unwrap();
        assert_eq!(opts.series_labels,
            vec!["k".to_string(), "optimize_for".to_string(), "phase".to_string()]);
    }

    #[test]
    fn series_tuple_key_formats_pairs() {
        let labels = std::collections::HashMap::from([
            ("k".to_string(), "10".to_string()),
            ("optimize_for".to_string(), "RECALL".to_string()),
            ("profile".to_string(), "label_03".to_string()),
        ]);
        let series = vec!["k".to_string(), "optimize_for".to_string()];
        assert_eq!(series_tuple_key(&labels, &series), "k=10, optimize_for=RECALL");
    }

    #[test]
    fn series_tuple_key_handles_missing_label() {
        let labels = std::collections::HashMap::from([
            ("k".to_string(), "10".to_string()),
        ]);
        let series = vec!["k".to_string(), "optimize_for".to_string()];
        assert_eq!(series_tuple_key(&labels, &series), "k=10, optimize_for=(unset)");
    }

    fn row(spec: &str, mean: f64) -> DbRow {
        DbRow {
            spec: spec.to_string(),
            mean: Some(mean),
            labels: parse_labels(spec),
        }
    }

    #[test]
    fn aggregate_mean_arithmetic_average() {
        // 4-value mean: (0.8+0.9+0.7+0.6)/4 = 0.75 exactly.
        let v = vec![0.8, 0.9, 0.7, 0.6];
        let r = aggregate("mean", &v);
        assert!((r - 0.75).abs() < 1e-12, "got {r}");
    }

    #[test]
    fn aggregate_min_max_extreme_values() {
        let v = vec![3.5, 1.2, 7.8, 4.4];
        assert_eq!(aggregate("min", &v), 1.2);
        assert_eq!(aggregate("max", &v), 7.8);
    }

    #[test]
    fn aggregate_p99_picks_high_index() {
        // 100 ascending values 0..100. The percentile function
        // uses `((n-1) * p).round()` → for n=100, p=0.99 → 98.01
        // → 98 → value 98.0. (The 99th percentile of 100 evenly-
        // spaced points lands at index 98 by this convention.)
        let v: Vec<f64> = (0..100).map(|i| i as f64).collect();
        assert_eq!(aggregate("p99", &v), 98.0);
    }

    #[test]
    fn aggregate_p50_median_of_odd_count() {
        // Sorted [1,2,3,4,5] → median is index ((5-1)*0.5).round() = 2 → value 3.
        let v = vec![5.0, 3.0, 1.0, 4.0, 2.0];
        assert_eq!(aggregate("p50", &v), 3.0);
    }

    #[test]
    fn aggregate_empty_returns_nan() {
        let r = aggregate("mean", &[]);
        assert!(r.is_nan());
    }

    #[test]
    fn bucketing_mean_across_three_profiles_matches_hand_calc() {
        // At limit=10, three profiles report recall {0.91, 0.92, 0.93}.
        // Mean = 2.76/3 = 0.92.
        let rows = vec![
            row("recall{profile=\"a\",limit=\"10\"}", 0.91),
            row("recall{profile=\"b\",limit=\"10\"}", 0.92),
            row("recall{profile=\"c\",limit=\"10\"}", 0.93),
        ];
        let buckets = bucket_rows(&rows, "limit", &[]);
        let cell = &buckets[""];
        let series_pts: Vec<f64> = cell.values()
            .map(|samples| aggregate("mean", samples))
            .collect();
        assert_eq!(series_pts.len(), 1);
        assert!((series_pts[0] - 0.92).abs() < 1e-9);
    }

    #[test]
    fn bucketing_two_x_points_distinct_means() {
        // limit=10 → mean 0.85, limit=20 → mean 0.95
        let rows = vec![
            row("recall{profile=\"a\",limit=\"10\"}", 0.8),
            row("recall{profile=\"b\",limit=\"10\"}", 0.9),
            row("recall{profile=\"a\",limit=\"20\"}", 0.92),
            row("recall{profile=\"b\",limit=\"20\"}", 0.98),
        ];
        let buckets = bucket_rows(&rows, "limit", &[]);
        let cell = &buckets[""];
        let m_at_10 = aggregate("mean", &cell[&F64Key(10.0)]);
        let m_at_20 = aggregate("mean", &cell[&F64Key(20.0)]);
        assert!((m_at_10 - 0.85).abs() < 1e-9, "expected 0.85, got {m_at_10}");
        assert!((m_at_20 - 0.95).abs() < 1e-9, "expected 0.95, got {m_at_20}");
    }

    #[test]
    fn bucketing_multi_key_series_separates_tuples() {
        // Two distinct (k, optimize_for) tuples; means computed
        // separately within each tuple.
        let rows = vec![
            row("recall{k=\"10\",optimize_for=\"RECALL\",profile=\"a\",limit=\"10\"}", 0.90),
            row("recall{k=\"10\",optimize_for=\"RECALL\",profile=\"b\",limit=\"10\"}", 0.92),
            row("recall{k=\"100\",optimize_for=\"LATENCY\",profile=\"a\",limit=\"10\"}", 0.70),
            row("recall{k=\"100\",optimize_for=\"LATENCY\",profile=\"b\",limit=\"10\"}", 0.74),
        ];
        let series = vec!["k".to_string(), "optimize_for".to_string()];
        let buckets = bucket_rows(&rows, "limit", &series);
        assert_eq!(buckets.len(), 2,
            "expected 2 series tuples, got {}: {:?}", buckets.len(),
            buckets.keys().collect::<Vec<_>>());
        let recall_series = &buckets["k=10, optimize_for=RECALL"];
        let latency_series = &buckets["k=100, optimize_for=LATENCY"];
        let m_recall = aggregate("mean", &recall_series[&F64Key(10.0)]);
        let m_latency = aggregate("mean", &latency_series[&F64Key(10.0)]);
        assert!((m_recall - 0.91).abs() < 1e-9, "RECALL mean {m_recall} ≠ 0.91");
        assert!((m_latency - 0.72).abs() < 1e-9, "LATENCY mean {m_latency} ≠ 0.72");
    }

    #[test]
    fn bucketing_n_count_tracks_samples_per_cell() {
        // Three profiles × two limits = 6 rows total. Bucketing
        // should yield 3 samples per (limit) cell when no series.
        let rows = vec![
            row("recall{profile=\"a\",limit=\"10\"}", 0.8),
            row("recall{profile=\"b\",limit=\"10\"}", 0.9),
            row("recall{profile=\"c\",limit=\"10\"}", 0.7),
            row("recall{profile=\"a\",limit=\"20\"}", 0.85),
            row("recall{profile=\"b\",limit=\"20\"}", 0.95),
            row("recall{profile=\"c\",limit=\"20\"}", 0.75),
        ];
        let buckets = bucket_rows(&rows, "limit", &[]);
        let cell = &buckets[""];
        assert_eq!(cell[&F64Key(10.0)].len(), 3);
        assert_eq!(cell[&F64Key(20.0)].len(), 3);
    }

    #[test]
    fn auto_detect_finds_varying_labels() {
        fn row(spec: &str, mean: f64) -> DbRow {
            DbRow {
                spec: spec.to_string(),
                mean: Some(mean),
                labels: parse_labels(spec),
            }
        }
        let rows = vec![
            row("recall{k=\"10\",optimize_for=\"RECALL\",limit=\"10\",profile=\"a\"}", 0.9),
            row("recall{k=\"10\",optimize_for=\"LATENCY\",limit=\"10\",profile=\"a\"}", 0.8),
            row("recall{k=\"100\",optimize_for=\"RECALL\",limit=\"10\",profile=\"a\"}", 0.7),
        ];
        // x = limit (constant); session not in data
        // varying: k, optimize_for; constant: limit, profile
        let auto = auto_detect_series_labels(&rows, "limit");
        assert_eq!(auto, vec!["k".to_string(), "optimize_for".to_string()]);
    }

    #[test]
    fn spec_multiple_filters() {
        let opts = parse_spec("recall@10.mean over limit where k=10, phase=ann_query").unwrap();
        assert_eq!(opts.filters.len(), 2);
        assert_eq!(opts.filters[0], ("k".to_string(), "10".to_string()));
        assert_eq!(opts.filters[1], ("phase".to_string(), "ann_query".to_string()));
    }

    #[test]
    fn spec_semicolon_form() {
        let opts = parse_spec("recall@10.mean; over limit; where k=10; agg=mean").unwrap();
        assert_eq!(opts.metric.as_deref(), Some("recall@10.mean"));
        assert_eq!(opts.x_label.as_deref(), Some("limit"));
        assert_eq!(opts.filters, vec![("k".to_string(), "10".to_string())]);
        assert_eq!(opts.agg, "mean");
    }

    #[test]
    fn spec_agg_directive() {
        let opts = parse_spec("recall@10.mean over limit; agg=p99").unwrap();
        assert_eq!(opts.agg, "p99");
    }

    #[test]
    fn bucket_rows_aggregates_when_no_series() {
        fn row(spec: &str, mean: f64) -> DbRow {
            DbRow {
                spec: spec.to_string(),
                mean: Some(mean),
                labels: parse_labels(spec),
            }
        }
        let rows = vec![
            row("recall{profile=\"a\",limit=\"10\"}", 0.8),
            row("recall{profile=\"b\",limit=\"10\"}", 0.7),
            row("recall{profile=\"a\",limit=\"20\"}", 0.9),
            row("recall{profile=\"b\",limit=\"20\"}", 0.85),
        ];
        let buckets = bucket_rows(&rows, "limit", &[]);
        assert_eq!(buckets.len(), 1);
        let cell = &buckets[""];
        assert_eq!(cell.len(), 2);
        // limit=10 has both profiles a (0.8) and b (0.7) — mean 0.75
        let agg10 = aggregate("mean", &cell[&F64Key(10.0)]);
        assert!((agg10 - 0.75).abs() < 1e-9);
    }
}
