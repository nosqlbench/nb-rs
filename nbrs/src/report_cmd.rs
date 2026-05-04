// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `nbrs report` — list and render report items defined in a
//! workload's `report:` block (SRD-46).
//!
//! Subcommands:
//!
//! - `nbrs report` (no args) — list every defined item.
//! - `nbrs report all` — render every item.
//! - `nbrs report <glob>` — render items whose names match the
//!   glob.
//! - `nbrs report figure <N>` — render by global index.
//! - `nbrs report plot <glob>` / `nbrs report table <glob>` —
//!   kind-filtered name lookup.
//!
//! All forms accept `workload=<file>` positionally, falling back
//! to `logs/latest/metrics.db`'s persisted items when no source
//! is given.

use std::path::{Path, PathBuf};

/// Top-level dispatch for `nbrs report ...` and the unadvertised
/// `nbrs plot ...` / `nbrs table ...` aliases.
///
/// The `workload=<file>` token may appear anywhere in the args
/// list — it's pulled out for source resolution, then re-injected
/// when forwarding render commands to plot_metrics / summary.
pub fn report_command(args: &[String], kind_filter: KindFilter) {
    let (workload_path, rest) = extract_workload(args);
    let workload_arg = workload_path.as_ref()
        .map(|p| format!("workload={}", p.display()));

    // Promote `nbrs report plot ...` / `nbrs report table ...` to
    // the kind-filtered form, peeling the kind keyword off so the
    // remaining arg list looks like a top-level `nbrs plot ...`
    // / `nbrs table ...` invocation.
    let (kind_filter, rest) = if matches!(kind_filter, KindFilter::Any) {
        match rest.first().map(String::as_str) {
            Some("plot") => (KindFilter::Plot, rest[1..].to_vec()),
            Some("table") => (KindFilter::Table, rest[1..].to_vec()),
            _ => (kind_filter, rest),
        }
    } else {
        (kind_filter, rest)
    };

    let items = match resolve_items(workload_path.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("nbrs report: {e}");
            std::process::exit(2);
        }
    };

    match rest.first().map(String::as_str) {
        // Listing form — no selector after the (optional) kind
        // keyword.
        None => print_listing(&items, kind_filter),
        Some("all") => render_all(&items, kind_filter, &rest[1..], workload_arg.as_deref()),
        Some("figure") => {
            let n_arg = rest.get(1).cloned().unwrap_or_default();
            let pass = rest.get(2..).unwrap_or(&[]);
            render_by_index(&items, kind_filter, &n_arg, pass, workload_arg.as_deref());
        }
        // Flag-form: `nbrs plot --name X --series Y ...` — the
        // user is driving the renderer directly with its own
        // flags. Pass the whole arg list straight through
        // without trying to interpret any token as a glob.
        // Equivalent to the positional form for stored-name
        // selection (`nbrs plot X`) but lets the user supply
        // ad-hoc `--metric`/`--filter`/etc.
        Some(arg) if arg.starts_with("--") => {
            forward_renderer_flags(kind_filter, &rest, workload_arg.as_deref());
        }
        Some(glob) => {
            render_by_glob(&items, kind_filter, glob, &rest[1..], workload_arg.as_deref());
        }
    }
}

/// Pass-through for the flag-form invocation: the user typed
/// `nbrs plot --name X` (or `nbrs table --filter k=v`). Forward
/// everything to the renderer, including the workload= token
/// if present.
fn forward_renderer_flags(
    kind_filter: KindFilter,
    args: &[String],
    workload_arg: Option<&str>,
) {
    let mut full: Vec<String> = Vec::new();
    if let Some(w) = workload_arg { full.push(w.to_string()); }
    full.extend(args.iter().cloned());
    match kind_filter {
        KindFilter::Plot => crate::plot_metrics::plot_metrics_command(&full),
        KindFilter::Table => crate::summary::summary_command(&full),
        KindFilter::Any => {
            eprintln!("nbrs report: flag-form selection requires a kind \
                (use `nbrs plot --<flag>...` or `nbrs table --<flag>...`)");
            std::process::exit(2);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum KindFilter {
    Any,
    Plot,
    Table,
}

impl KindFilter {
    fn matches(&self, k: nbrs_workload::report::Kind) -> bool {
        use nbrs_workload::report::Kind;
        match (self, k) {
            (KindFilter::Any, _) => true,
            (KindFilter::Plot, Kind::Plot) => true,
            (KindFilter::Table, Kind::Table) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedItem {
    pub name: String,
    pub kind: nbrs_workload::report::Kind,
    pub label: Option<String>,
    pub body: String,
    /// Resolved palette name/index after the cascade (workload
    /// `defaults` → group `defaults` → item style). `None` ⇒ no
    /// override; renderer uses the default palette.
    pub palette: Option<String>,
    /// SRD-46 line dash style (`solid`, `dashed`, `dotted`,
    /// `none`).
    pub line: Option<String>,
    /// Stroke width in pixels.
    pub width: Option<f32>,
    /// Marker shape (`none`, `circle`, `square`, `triangle`,
    /// `diamond`, `plus`, `cross`).
    pub marker: Option<String>,
    /// Marker size (radius in pixels).
    pub marker_size: Option<f32>,
    /// SRD-46 target output file. `None` ⇒ default
    /// `summary.md`. Set by a preceding `file <filename>`
    /// directive in the same group.
    pub target_file: Option<String>,
}

fn extract_workload(args: &[String]) -> (Option<PathBuf>, Vec<String>) {
    let mut workload_path: Option<PathBuf> = None;
    let mut rest: Vec<String> = Vec::new();
    for a in args {
        if let Some(p) = a.strip_prefix("workload=") {
            workload_path = Some(PathBuf::from(p));
        } else {
            rest.push(a.clone());
        }
    }
    (workload_path, rest)
}

fn resolve_items(workload_path: Option<&Path>) -> Result<Vec<ResolvedItem>, String> {
    if let Some(p) = workload_path {
        let resolved = crate::cli::resolve_workload_path(&p.to_string_lossy())
            .map(PathBuf::from)
            .unwrap_or_else(|| p.to_path_buf());
        if !resolved.exists() {
            return Err(format!("workload '{}' not found", resolved.display()));
        }
        let text = std::fs::read_to_string(&resolved)
            .map_err(|e| format!("read '{}': {e}", resolved.display()))?;
        let workload = nbrs_workload::parse::parse_workload(
            &text, &std::collections::HashMap::new())?;
        let mut out: Vec<ResolvedItem> = Vec::new();
        for group in &workload.report.groups {
            for item in &group.items {
                let style = workload.report.effective_style(group, item);
                out.push(ResolvedItem {
                    name: item.name.clone(),
                    kind: item.kind,
                    label: item.label.clone(),
                    body: item.body.clone(),
                    palette: style.palette.clone(),
                    line: style.line.clone(),
                    width: style.width,
                    marker: style.marker.clone(),
                    marker_size: style.size,
                    target_file: item.target_file.clone(),
                });
            }
        }
        Ok(out)
    } else {
        // Db fallback: read `report.<name>` rows from the
        // session db's session_metadata table (SRD-46). Each
        // value carries the kind keyword + name + optional
        // `label "..."` + spec body — the same shape the
        // report parser ingests.
        let db_path = std::path::PathBuf::from("logs/latest/metrics.db");
        if !db_path.exists() { return Ok(Vec::new()); }
        let conn = match rusqlite::Connection::open(&db_path) {
            Ok(c) => c,
            Err(_) => return Ok(Vec::new()),
        };
        let mut stmt = match conn.prepare(
            "SELECT key, value FROM session_metadata \
             WHERE key LIKE 'report.%' ORDER BY rowid"
        ) {
            Ok(s) => s,
            Err(_) => return Ok(Vec::new()),
        };
        let rows = match stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        }) {
            Ok(it) => it,
            Err(_) => return Ok(Vec::new()),
        };
        let mut out: Vec<ResolvedItem> = Vec::new();
        for row in rows.flatten() {
            if let Some(item) = parse_persisted_item(&row.0, &row.1) {
                out.push(item);
            }
        }
        Ok(out)
    }
}

fn parse_persisted_item(key: &str, value: &str) -> Option<ResolvedItem> {
    let _name = key.strip_prefix("report.")?;
    let mut lines = value.lines();
    let head = lines.next()?;
    use nbrs_workload::report::Kind;
    let (kind, name) = if let Some(rest) = head.strip_prefix("plot ") {
        (Kind::Plot, rest.trim().to_string())
    } else if let Some(rest) = head.strip_prefix("table ") {
        (Kind::Table, rest.trim().to_string())
    } else if let Some(rest) = head.strip_prefix("text ") {
        (Kind::Text, rest.trim().to_string())
    } else if let Some(rest) = head.strip_prefix("file ") {
        (Kind::File, rest.trim().to_string())
    } else {
        return None;
    };
    let mut label: Option<String> = None;
    let mut target_file: Option<String> = None;
    let mut body_lines: Vec<&str> = Vec::new();
    for line in lines {
        if let Some(rest) = line.strip_prefix("label ") {
            let s = rest.trim();
            let s = s.strip_prefix('"').and_then(|x| x.strip_suffix('"'))
                .or_else(|| s.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')))
                .unwrap_or(s);
            label = Some(s.to_string());
        } else if let Some(rest) = line.strip_prefix("target ") {
            target_file = Some(rest.trim().to_string());
        } else {
            body_lines.push(line);
        }
    }
    Some(ResolvedItem {
        name,
        kind,
        label,
        body: body_lines.join("\n"),
        palette: None,
        line: None,
        width: None,
        marker: None,
        marker_size: None,
        target_file,
    })
}

fn print_listing(items: &[ResolvedItem], filter: KindFilter) {
    use nbrs_workload::report::Kind;
    let kind_label = match filter {
        KindFilter::Any => "items",
        KindFilter::Plot => "plots",
        KindFilter::Table => "tables",
    };
    let total = items.iter().filter(|i| filter.matches(i.kind)).count();
    if total == 0 {
        eprintln!("(no report items defined)");
        return;
    }
    println!("# Report {kind_label} ({total} total)");

    // Figure numbers count only plot+table; text/file are
    // skipped (SRD-46). The number prints alongside each
    // figure; text shows a `T` prefix; file shows the section
    // header.
    let mut fig_num: usize = 0;
    let mut last_target: Option<String> = None;
    for item in items.iter() {
        if !filter.matches(item.kind) {
            // Bump fig counter to keep numbering stable even
            // when the listing is filtered.
            if item.kind.is_figure() { fig_num += 1; }
            continue;
        }
        // Section banner when target_file changes.
        let this_target = item.target_file.clone();
        if this_target != last_target {
            match this_target.as_deref() {
                Some(t) => println!("\nfile {t}:"),
                None => println!("\n(default → summary.md):"),
            }
            last_target = this_target;
        }
        match item.kind {
            Kind::Plot | Kind::Table => {
                fig_num += 1;
                let label = item.label.as_deref().unwrap_or("");
                println!("  {fig_num:3} — {name:24} {kind:6} \"{label}\"",
                    name = item.name, kind = item.kind.as_str());
            }
            Kind::Text => {
                let label = item.label.as_deref().unwrap_or("");
                let preview = item.body.lines().next().unwrap_or("").trim();
                let preview = if preview.len() > 40 {
                    format!("{}…", &preview[..40])
                } else { preview.to_string() };
                let display = if !label.is_empty() {
                    label.to_string()
                } else { preview };
                println!("    T — {name:24} {kind:6} \"{display}\"",
                    name = item.name, kind = item.kind.as_str());
            }
            Kind::File => {
                let label = item.label.as_deref().unwrap_or("");
                println!("    F — {name:24} {kind:6} \"{label}\"",
                    name = item.name, kind = item.kind.as_str());
            }
            Kind::Details => {
                let label = item.label.as_deref().unwrap_or("run details");
                println!("    D — {name:24} {kind:6} \"{label}\"",
                    name = item.name, kind = item.kind.as_str());
            }
        }
    }
}

fn render_all(
    items: &[ResolvedItem],
    filter: KindFilter,
    passthrough: &[String],
    workload_arg: Option<&str>,
) {
    // SRD-46: figure numbers count only plot+table items, in
    // their order across the whole resolved item list. The
    // counter advances even when the kind filter excludes the
    // item, so numbers stay stable regardless of which subset
    // the operator renders.
    let mut fig_num: usize = 0;
    for item in items.iter() {
        if item.kind.is_figure() { fig_num += 1; }
        if !filter.matches(item.kind) { continue; }
        render_one(fig_num, item, passthrough, workload_arg);
    }
}

fn render_by_index(
    items: &[ResolvedItem],
    filter: KindFilter,
    n_arg: &str,
    passthrough: &[String],
    workload_arg: Option<&str>,
) {
    let n: usize = match n_arg.parse() {
        Ok(v) if v >= 1 => v,
        _ => {
            eprintln!("nbrs report figure: argument must be a positive integer (got '{n_arg}')");
            std::process::exit(2);
        }
    };
    if let Some(item) = items.get(n - 1) {
        if !filter.matches(item.kind) {
            eprintln!("nbrs report: figure {n} is a {} but the kind filter requires {:?}",
                item.kind.as_str(), filter);
            std::process::exit(2);
        }
        render_one(n, item, passthrough, workload_arg);
    } else {
        eprintln!("nbrs report: figure {n} out of range (1..{})", items.len());
        std::process::exit(2);
    }
}

fn render_by_glob(
    items: &[ResolvedItem],
    filter: KindFilter,
    glob: &str,
    passthrough: &[String],
    workload_arg: Option<&str>,
) {
    // Build (figure_num, item) pairs for figures that pass the
    // kind filter and the glob. Counter advances over every
    // figure in declaration order so the numbers stay stable.
    let mut fig_num: usize = 0;
    let mut matches: Vec<(usize, &ResolvedItem)> = Vec::new();
    for item in items.iter() {
        if item.kind.is_figure() { fig_num += 1; }
        if !filter.matches(item.kind) { continue; }
        if !glob_matches(glob, &item.name) { continue; }
        matches.push((fig_num, item));
    }
    if matches.is_empty() {
        eprintln!("nbrs report: no items match '{glob}'");
        std::process::exit(2);
    }
    for (n, item) in matches {
        render_one(n, item, passthrough, workload_arg);
    }
}

fn render_one(
    n: usize,
    item: &ResolvedItem,
    passthrough: &[String],
    workload_arg: Option<&str>,
) {
    use nbrs_workload::report::Kind;
    // File items are scope directives — they don't render
    // anything themselves; their children do (during normal
    // iteration through the items list).
    if matches!(item.kind, Kind::File) {
        return;
    }
    if matches!(item.kind, Kind::Text) {
        render_text(item);
        return;
    }
    let mut base: Vec<String> = Vec::new();
    if let Some(w) = workload_arg { base.push(w.to_string()); }
    base.push(format!("--name={}", item.name));
    base.push("--figure-num".into());
    base.push(n.to_string());
    if let Some(l) = item.label.as_deref() {
        base.push("--label".into());
        base.push(l.to_string());
    }
    if let Some(t) = item.target_file.as_deref() {
        base.push("--report".into());
        base.push(format!("logs/latest/{t}"));
    }
    // Plot-only style flags — appended only when forwarding to
    // the plot renderer. The summary (table) renderer doesn't
    // know `--palette` / `--line` / etc. and would mis-capture
    // their values as a positional spec.
    if matches!(item.kind, Kind::Plot) {
        if let Some(p) = item.palette.as_deref() {
            base.push("--palette".into());
            base.push(p.to_string());
        }
        if let Some(l) = item.line.as_deref() {
            base.push("--line".into());
            base.push(l.to_string());
        }
        if let Some(w) = item.width {
            base.push("--line-width".into());
            base.push(w.to_string());
        }
        if let Some(m) = item.marker.as_deref() {
            base.push("--marker".into());
            base.push(m.to_string());
        }
        if let Some(s) = item.marker_size {
            base.push("--marker-size".into());
            base.push(s.to_string());
        }
    }
    base.extend(passthrough.iter().cloned());
    match item.kind {
        Kind::Plot => {
            // Use the result-returning variant so a no-rows
            // failure on one plot doesn't abort the rest of a
            // `report all` batch.
            if let Err(e) = crate::plot_metrics::plot_metrics_command_result(&base) {
                eprintln!("nbrs report: plot '{}' skipped: {e}", item.name);
            }
        }
        Kind::Table => crate::summary::summary_command(&base),
        Kind::Text | Kind::File | Kind::Details => unreachable!(),
    }
}

/// Render a text item by writing its body verbatim into the
/// target markdown file (or `summary.md` when no `target_file`
/// is set). The heading uses the label, falling back to a
/// prettified canonical name. No figure number — text isn't a
/// figure (SRD-46).
fn render_text(item: &ResolvedItem) {
    let target = item.target_file.as_deref().unwrap_or("summary.md");
    let path = std::path::PathBuf::from("logs/latest").join(target);
    let label = item.label.clone()
        .unwrap_or_else(|| crate::report::prettify_name(&item.name));
    let heading_display = format!("{label} (text)");
    if let Err(e) = crate::report::write_named_section(
        &path, &item.name, &heading_display, &item.body,
        crate::report::WriteMode::Update,
    ) {
        eprintln!("warning: failed to write text section to '{}': {e}", path.display());
    }
}

/// Tiny glob matcher: supports `*` (any), `?` (any one char).
/// `[abc]` brackets and brace expansion are out of scope —
/// add only when an example workload needs them.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_star_matches() {
        assert!(glob_matches("recall*", "recall_at_k10"));
        assert!(glob_matches("*at_k10", "recall_at_k10"));
        assert!(glob_matches("*", "anything"));
        assert!(glob_matches("*at*", "recall_at_k10"));
        assert!(!glob_matches("plot*", "recall"));
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_matches("plot?", "plot1"));
        assert!(!glob_matches("plot?", "plot12"));
    }

    #[test]
    fn glob_exact() {
        assert!(glob_matches("recall", "recall"));
        assert!(!glob_matches("recall", "recall_at_k10"));
    }
}
