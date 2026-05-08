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
    // Resolve `--session` once at the top so every downstream
    // path (item lookup in db, forwarded render commands,
    // markdown output, text-section writes) sees the same
    // session dir. Read-side only — never mutates `logs/latest`.
    let session_dir: Option<PathBuf> =
        nbrs_activity::session::read_session_dir(args);
    let session_db: Option<PathBuf> =
        session_dir.as_ref().map(|d| d.join("metrics.db"));
    let output_root: PathBuf = session_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("logs/latest"));

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

    let items = match resolve_items(workload_path.as_deref(), session_db.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("nbrs report: {e}");
            std::process::exit(2);
        }
    };

    // `--rebuild` (or `NBRS_REPORT_REBUILD=1`) wipes the
    // declared target markdown files before any renderer
    // touches them. The set comes from the resolved item
    // list — files outside the declared targets stay
    // intact. Idempotent: missing files are silently
    // skipped (a fresh session has nothing to wipe).
    if is_rebuild_mode(args) {
        rebuild_wipe_targets(&items, &output_root);
    }

    // SRD-15 strict mode: when `--strict` is on the arg list
    // (or `NBRS_STRICT` is set), figure-render no-data errors
    // remain hard failures. Without strict mode, "no-data"
    // results from incremental / auto-render paths are
    // downgraded to warnings so a workload reporting before
    // its data has accumulated doesn't fail the run.
    let strict = is_strict_mode(args);

    let failures: Vec<String> = match rest.first().map(String::as_str) {
        // Listing form — no selector after the (optional) kind
        // keyword. `list` is an explicit alias for the bare
        // form: `nbrs report list` and `nbrs report` produce
        // the same output.
        None | Some("list") => { print_listing(&items, kind_filter); Vec::new() }
        Some("all") => render_all(&items, kind_filter, &rest[1..], workload_arg.as_deref(), &output_root, session_db.as_deref(), strict),
        Some("figure") => {
            let n_arg = rest.get(1).cloned().unwrap_or_default();
            let pass = rest.get(2..).unwrap_or(&[]);
            render_by_index(&items, kind_filter, &n_arg, pass, workload_arg.as_deref(), &output_root, session_db.as_deref(), strict)
        }
        Some("scratch") => {
            crate::report_scratch::scratch_subcommand(&output_root, &rest[1..]);
            Vec::new()
        }
        Some("rename") => {
            run_rename(&rest[1..], &output_root, workload_path.as_deref());
            Vec::new()
        }
        // Flag-form: `nbrs plot --name X --series Y ...` — the
        // user is driving the renderer directly with its own
        // flags. Pass the whole arg list straight through
        // without trying to interpret any token as a glob.
        // Equivalent to the positional form for stored-name
        // selection (`nbrs plot X`) but lets the user supply
        // ad-hoc `--metric`/`--filter`/etc.
        Some(arg) if arg.starts_with("--") => {
            forward_renderer_flags(
                kind_filter, &rest, workload_arg.as_deref(),
                session_db.as_deref(),
            );
            Vec::new()
        }
        // SRD-64 flag-form: `nbrs report <kind> <name> [--<flag> ...]`.
        // When a vocab-defined `--flag` appears anywhere in the
        // argument tail, the user is constructing a new item from
        // CLI flags rather than selecting an existing stored one.
        // Route through the Phase A vocab-driven builder + Phase C
        // scratch render path.
        Some(_name) if matches!(kind_filter, KindFilter::Plot | KindFilter::Table)
            && tail_has_vocab_flag(&rest)
        => {
            let kind = match kind_filter {
                KindFilter::Plot => nbrs_workload::report::Kind::Plot,
                KindFilter::Table => nbrs_workload::report::Kind::Table,
                _ => unreachable!(),
            };
            dispatch_new_item(kind, &rest, &output_root, workload_path.as_deref());
            Vec::new()
        }
        Some(arg) => {
            // Numeric-selector forms (`5`, `2-4`, `2..4`,
            // `2..=4`, `1,3-5,7`) route through the figure
            // index path. Item names follow the OpenMetrics
            // metric-name ABNF which forbids hyphens / dots /
            // commas, so a bare `2-4` can't be a literal item
            // name — safe to reinterpret as a figure
            // selector.
            if let Some(indices) = parse_figure_selector(arg) {
                render_by_indices(&items, kind_filter, &indices, &rest[1..], workload_arg.as_deref(), &output_root, session_db.as_deref(), strict)
            } else {
                render_by_glob(&items, kind_filter, arg, &rest[1..], workload_arg.as_deref(), &output_root, session_db.as_deref(), strict)
            }
        }
    };

    // SRD: figure-render failures are not skip-overable. The
    // workload defined a figure; the operator asked for it; the
    // run produced no output. That is a defect worth a nonzero
    // exit, even if the rest of the batch produced their
    // markdown/png artifacts. Each failure was already printed
    // line-by-line as `ERROR: ...` above; the trailing summary
    // gives the count and the exit-time signal.
    if !failures.is_empty() {
        eprintln!();
        eprintln!("nbrs report: {} figure(s) failed to render:", failures.len());
        for f in &failures {
            eprintln!("  - {f}");
        }
        std::process::exit(2);
    }
}

/// True if `args` contains at least one `--<flag>` token that
/// the SRD-64 vocab recognises. Used by the dispatcher to
/// distinguish "new item from CLI flags" from "select existing
/// stored item by name."
fn tail_has_vocab_flag(args: &[String]) -> bool {
    args.iter().any(|a| {
        a.starts_with("--")
            && nbrs_workload::report::vocab::directive_by_cli_flag(a).is_some()
    })
}

/// SRD-64 flag-form dispatch for `nbrs report <kind> <name>
/// [flags]`. Builds a [`ReportItem`] from the CLI flag list,
/// renders to the session's scratch directory, and (when
/// `--add` is set) routes through the workload-edit primitive.
///
/// Phase C lands the build + scratch render. Phase D wires the
/// `--add` path; until then `--add` errors with a pending
/// message so the user-facing surface is visible.
fn dispatch_new_item(
    kind: nbrs_workload::report::Kind,
    args: &[String],
    session_dir: &std::path::Path,
    workload_path: Option<&std::path::Path>,
) {
    let mut result = match crate::report_build::build_item(kind, args) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("nbrs report {}: {e}", kind.as_str());
            std::process::exit(2);
        }
    };
    // `extract_workload` peels `--workload <path>` off the
    // top-level args before dispatch, so by the time we get
    // here the builder hasn't seen it. Backfill the dispatch's
    // `workload` field from the captured path so `--add`'s
    // workload-resolution can use it.
    if result.dispatch.workload.is_none() {
        if let Some(p) = workload_path {
            result.dispatch.workload = Some(p.to_string_lossy().into_owned());
        }
    }

    if result.dispatch.add {
        run_add(&result, session_dir);
        return;
    }

    if result.dispatch.dry_run {
        println!("# dry-run: would render to scratch (no workload edit, --add not set)");
        println!("{}", result.item.to_yaml_directive_string());
        return;
    }

    let paths = match crate::report_scratch::scratch_paths(session_dir, &result.item) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("nbrs report {}: scratch path: {e}", kind.as_str());
            std::process::exit(2);
        }
    };

    let stub = format!(
        "<!-- {} {} -->\n\n```yaml\n{}```\n",
        kind.as_str(), result.item.name,
        result.item.to_yaml_directive_string(),
    );

    if result.dispatch.stdout {
        if matches!(kind, nbrs_workload::report::Kind::Plot)
            && !result.dispatch.ascii
        {
            eprintln!(
                "--stdout is not compatible with `plot` kind; \
                 use --ascii for terminal rendering");
            std::process::exit(2);
        }
        print!("{stub}");
        return;
    }

    if let Err(e) = std::fs::write(&paths.md, &stub) {
        eprintln!("nbrs report {}: write '{}': {e}",
            kind.as_str(), paths.md.display());
        std::process::exit(2);
    }
    eprintln!("scratch render: {}", paths.md.display());
    if let Some(png) = &paths.png {
        eprintln!("(png path reserved — renderer integration lands in Phase D): {}",
            png.display());
    }
}

/// Phase D `--add` driver: parse the dispatch's anchor flag,
/// resolve the anchor against the active session, discover the
/// workload to mutate, and route through
/// [`nbrs_workload::edit::add_item`].
fn run_add(
    result: &crate::report_build::BuildResult,
    session_dir: &std::path::Path,
) {
    use nbrs_activity::report_anchor::{self, AnchorFlag};

    // The builder enforced `--at` and `--contextual` are
    // mutually exclusive; here we just translate the captured
    // strings into the typed enum.
    let flag = match (&result.dispatch.at, &result.dispatch.contextual) {
        (Some(at), None) => match AnchorFlag::parse_at(at) {
            Ok(f) => f,
            Err(e) => die("nbrs report --add", &e),
        },
        (None, Some(ctx)) => match AnchorFlag::parse_contextual(ctx) {
            Ok(f) => f,
            Err(e) => die("nbrs report --add", &e),
        },
        (None, None) => AnchorFlag::None,
        (Some(_), Some(_)) => unreachable!("builder enforces mutual exclusion"),
    };

    let db_path = session_dir.join("metrics.db");
    let resolution = match report_anchor::resolve(&db_path, &result.item, &flag) {
        Ok(r) => r,
        Err(e) => die("nbrs report --add", &e),
    };

    eprintln!("{}", resolution.diagnostic);

    let workload_path = match resolve_workload_for_add(
        result.dispatch.workload.as_deref(),
        session_dir,
    ) {
        Ok(p) => p,
        Err(e) => die("nbrs report --add", &e),
    };

    if result.dispatch.dry_run {
        // SRD-64 §6.1 dry-run: print the chosen anchor + the
        // emit body that would land. Nothing on disk changes.
        println!("# dry-run: would write to {}", workload_path.display());
        println!("# anchor:  {}", resolution.diagnostic);
        println!("# group:   {}", result.dispatch.group);
        println!("# replace: {}", result.dispatch.replace);
        println!("---");
        println!("{}", result.item.to_yaml_directive_string());
        return;
    }

    let outcome = match nbrs_workload::edit::add_item(
        &workload_path,
        &resolution.anchor,
        &result.dispatch.group,
        &result.item,
        result.dispatch.replace,
    ) {
        Ok(o) => o,
        Err(e) => die("nbrs report --add", &e.to_string()),
    };

    let verb = match outcome {
        nbrs_workload::edit::AddOutcome::Inserted => "inserted",
        nbrs_workload::edit::AddOutcome::Replaced => "replaced",
    };
    eprintln!(
        "{verb} `{}` in {} (backup at {}.bak)",
        result.item.name,
        workload_path.display(),
        workload_path.display(),
    );
}

fn die(prefix: &str, msg: &str) -> ! {
    eprintln!("{prefix}: {msg}");
    std::process::exit(2);
}

/// Phase E `nbrs report rename <old> <new> [flags]` driver.
///
/// SRD-64 §6.6: pure metadata edit through the Phase B
/// workload-edit primitive. Anchor stays at the existing
/// site. Collision policy:
/// - default: error if `<new>` is already in use;
/// - `--replace`: destructive overwrite of the existing
///   `<new>` item.
///
/// Flags accepted:
/// - `--workload <path>` — explicit workload override.
/// - `--replace` — destructive overwrite.
/// - `--dry-run` — print intended change, don't write.
///
/// Session-resolution flags (`--session-path`, `--session`,
/// `--session-name`) are honoured for the workload-discovery
/// fallback (via `<session>/checkpoint.jsonl::workload_path`).
fn run_rename(
    args: &[String],
    session_dir: &std::path::Path,
    extracted_workload: Option<&std::path::Path>,
) {
    let mut parsed = match parse_rename_args(args) {
        Ok(p) => p,
        Err(e) => die("nbrs report rename", &e),
    };
    // `extract_workload` peels `--workload <path>` off before
    // dispatch reaches us; backfill so the workload-resolution
    // sees the user-supplied path.
    if parsed.workload.is_none() {
        if let Some(p) = extracted_workload {
            parsed.workload = Some(p.to_string_lossy().into_owned());
        }
    }

    let workload_path = match resolve_workload_for_add(
        parsed.workload.as_deref(),
        session_dir,
    ) {
        Ok(p) => p,
        Err(e) => die("nbrs report rename", &e),
    };

    if parsed.dry_run {
        println!("# dry-run: would rename '{}' → '{}' in {}",
            parsed.old, parsed.new, workload_path.display());
        if parsed.replace {
            println!("# replace: drop existing '{}' if present", parsed.new);
        }
        return;
    }

    if let Err(e) = nbrs_workload::edit::rename_item(
        &workload_path, &parsed.old, &parsed.new, parsed.replace,
    ) {
        die("nbrs report rename", &e.to_string());
    }
    eprintln!(
        "renamed `{}` → `{}` in {} (backup at {}.bak)",
        parsed.old, parsed.new,
        workload_path.display(),
        workload_path.display(),
    );
}

#[derive(Debug)]
struct RenameArgs {
    old: String,
    new: String,
    workload: Option<String>,
    replace: bool,
    dry_run: bool,
}

fn parse_rename_args(args: &[String]) -> Result<RenameArgs, String> {
    let mut old: Option<String> = None;
    let mut new: Option<String> = None;
    let mut workload: Option<String> = None;
    let mut replace = false;
    let mut dry_run = false;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "--replace" => { replace = true; i += 1; }
            "--dry-run" => { dry_run = true; i += 1; }
            "--workload" => {
                workload = Some(args.get(i + 1)
                    .ok_or("--workload requires a value")?
                    .clone());
                i += 2;
            }
            // Session-resolution flags pass through (consumed
            // by the top-level resolver before run_rename is
            // called).
            "--session" | "--session-path" | "--session-name" | "--db" => {
                i += 2;
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown flag '{other}'"));
            }
            _ => {
                if old.is_none() {
                    old = Some(arg.clone());
                } else if new.is_none() {
                    new = Some(arg.clone());
                } else {
                    return Err(format!(
                        "unexpected positional '{arg}'; \
                         usage: nbrs report rename <old> <new> [flags]"
                    ));
                }
                i += 1;
            }
        }
    }

    Ok(RenameArgs {
        old: old.ok_or(
            "missing <old> name; usage: nbrs report rename <old> <new>",
        )?,
        new: new.ok_or(
            "missing <new> name; usage: nbrs report rename <old> <new>",
        )?,
        workload,
        replace,
        dry_run,
    })
}

/// Resolve which workload YAML the `--add` should mutate.
///
/// Order of precedence:
/// 1. `--workload <path>` if explicitly passed.
/// 2. `<session>/checkpoint.jsonl::workload_path`, when the
///    session was launched with a workload-file invocation
///    that recorded the path.
/// 3. Error with a remediation hint.
fn resolve_workload_for_add(
    explicit: Option<&str>,
    session_dir: &std::path::Path,
) -> Result<std::path::PathBuf, String> {
    if let Some(p) = explicit {
        let path = std::path::PathBuf::from(p);
        if !path.exists() {
            return Err(format!(
                "--workload '{}' does not exist", path.display(),
            ));
        }
        return Ok(path);
    }
    // Pre-SRD-44a, this fallback tried to read a
    // `workload_path` field from `checkpoint.json`, but the
    // checkpoint schema (then or now) never carries that
    // field, so the fallback path could only ever return an
    // error. SRD-44a converted the file to a JSONL event log,
    // which makes a one-shot `serde_json::from_slice` parse
    // wrong anyway. Surface the missing-flag diagnostic
    // directly until a future event type carries the workload
    // path explicitly.
    Err(format!(
        "no --workload <path> given; pass --workload <file.yaml> to \
         point at the workload to mutate (session at {})",
        session_dir.display(),
    ))
}

/// Pass-through for the flag-form invocation: the user typed
/// `nbrs plot --name X` (or `nbrs table --filter k=v`). Forward
/// everything to the renderer, including the workload= token
/// if present.
fn forward_renderer_flags(
    kind_filter: KindFilter,
    args: &[String],
    workload_arg: Option<&str>,
    session_db: Option<&Path>,
) {
    let mut full: Vec<String> = Vec::new();
    if let Some(w) = workload_arg { full.push(w.to_string()); }
    // Re-inject the resolved session db as an explicit `--db`
    // (overridable by anything in `args` that supplies its own
    // `--db`) so the downstream renderer doesn't fall back to
    // `logs/latest/metrics.db` after we stripped `--session`
    // out of `args` in `extract_workload`.
    if let Some(db) = session_db {
        let already_has_db = args.iter().any(|a| a == "--db" || a.starts_with("--db="));
        if !already_has_db {
            full.push("--db".to_string());
            full.push(db.to_string_lossy().into_owned());
        }
    }
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
    /// Per-series style overrides — one entry per `series
    /// <key>=<value>:<directives>` body line. Each entry binds a
    /// (key, value) discriminator to a `Style` with the same
    /// fields the item-level cascade uses (line / width /
    /// marker / size / color / palette). Forwarded to the plot
    /// renderer via `--series-override key=value:k=v k=v` so
    /// the per-series loop in `draw_chart` can substitute the
    /// override for the matching series's palette default.
    pub series_overrides: Vec<nbrs_workload::report::SeriesOverride>,
    /// SRD-46 plot-only `with-table: true` directive — when
    /// set on a `Kind::Plot` item, the renderer emits a
    /// companion table immediately after the plot in the
    /// same markdown file. The table reuses the plot's
    /// query data (each series query becomes one column).
    pub with_table: bool,
}

fn extract_workload(args: &[String]) -> (Option<PathBuf>, Vec<String>) {
    // Global flags consumed elsewhere (`--session*` by
    // `read_session_dir`, `workload=` here, startup flags by
    // `apply_session_directory_at_startup`). Peel them so the
    // dispatch loop's `rest.first()` classification sees only
    // the report-subcommand vocabulary (`all`, `figure`,
    // glob, `--name`, etc.). Without this, `nbrs report
    // --session local/foo` would route to flag-form because
    // `--session` is `--`-prefixed.
    const FLAGS_WITH_VALUES: &[&str] = &[
        "--session", "--session-name", "--session-path",
        "--session-reuse", "--session-keep", "--session-shelflife",
        "--resume", "--gk-lib",
    ];
    const BOOL_FLAGS: &[&str] = &[
        "--strict", "--no-prompt", "--resume-latest",
        "--force-retry-failed",
        // `--rebuild` wipes the report markdown files that
        // the workload's `report:` block declares before
        // rendering, so a workload that *removed* a plot
        // since the last `nbrs report` doesn't leave the
        // stale section sitting in summary.md. Consumed by
        // `is_rebuild_mode`; stripped here so it doesn't
        // confuse the dispatch loop.
        "--rebuild",
    ];
    let mut workload_path: Option<PathBuf> = None;
    let mut rest: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        // Capture the workload path from any of:
        //   workload=<path>        (positional key=value form)
        //   --workload <path>      (space-separated flag form)
        //   --workload=<path>      (= form)
        if let Some(p) = a.strip_prefix("workload=") {
            workload_path = Some(PathBuf::from(p));
            i += 1;
            continue;
        }
        if let Some(p) = a.strip_prefix("--workload=") {
            workload_path = Some(PathBuf::from(p));
            i += 1;
            continue;
        }
        if a == "--workload" {
            if let Some(v) = args.get(i + 1) {
                workload_path = Some(PathBuf::from(v));
                i += 2;
                continue;
            }
            // Trailing `--workload` with no value — let the
            // downstream parser surface the error.
            i += 1;
            continue;
        }
        if FLAGS_WITH_VALUES.contains(&a.as_str()) {
            // Skip the flag and its value.
            i += 2;
            continue;
        }
        if FLAGS_WITH_VALUES.iter().any(|f| a.starts_with(&format!("{f}="))) {
            i += 1;
            continue;
        }
        if BOOL_FLAGS.contains(&a.as_str()) {
            i += 1;
            continue;
        }
        rest.push(a.clone());
        i += 1;
    }
    (workload_path, rest)
}

fn resolve_items(
    workload_path: Option<&Path>,
    session_db: Option<&Path>,
) -> Result<Vec<ResolvedItem>, String> {
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
        // Workload-param interpolation: report items (the
        // `label "..."` and the body lines) routinely
        // contain `{cql_dialect}`-style placeholders that
        // operators expect to render with the workload's
        // declared param values. Expand them once here so
        // every downstream consumer (the markdown
        // assembler, the plot renderer that parses the
        // body) sees the resolved literals.
        let params: std::collections::HashMap<String, String> = workload.params.clone();
        let mut out: Vec<ResolvedItem> = Vec::new();
        for group in &workload.report.groups {
            for item in &group.items {
                let style = workload.report.effective_style(group, item);
                let label = item.label.as_deref().map(|s|
                    nbrs_activity::runner::expand_workload_params(s, &params));
                let body = nbrs_activity::runner::expand_workload_params(&item.body, &params);
                let target_file = item.target_file.as_deref().map(|s|
                    nbrs_activity::runner::expand_workload_params(s, &params));
                out.push(ResolvedItem {
                    name: item.name.clone(),
                    kind: item.kind,
                    label,
                    body,
                    palette: style.palette.clone(),
                    line: style.line.clone(),
                    width: style.width,
                    marker: style.marker.clone(),
                    marker_size: style.size,
                    target_file,
                    series_overrides: style.series.clone(),
                    with_table: item.with_table,
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
        let db_path = session_db
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("logs/latest/metrics.db"));
        if !db_path.exists() { return Ok(Vec::new()); }
        let conn = match rusqlite::Connection::open(&db_path) {
            Ok(c) => c,
            Err(_) => return Ok(Vec::new()),
        };
        // Pull the workload's persisted params first so we
        // can expand `{name}` placeholders in stored item
        // labels / bodies / target_file paths. The runner
        // writes one `param.<key> → <value>` row per
        // declared workload param at session start
        // (`runner.rs:774`); here we read them back for the
        // expansion. Same substitution as the YAML path.
        let mut params: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        if let Ok(mut pstmt) = conn.prepare(
            "SELECT key, value FROM session_metadata WHERE key LIKE 'param.%'"
        ) {
            if let Ok(prows) = pstmt.query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            }) {
                for row in prows.flatten() {
                    if let Some(name) = row.0.strip_prefix("param.") {
                        params.insert(name.to_string(), row.1);
                    }
                }
            }
        }
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
            if let Some(mut item) = parse_persisted_item(&row.0, &row.1) {
                // Expand workload-param placeholders.
                if let Some(label) = item.label.as_deref() {
                    item.label = Some(nbrs_activity::runner::expand_workload_params(label, &params));
                }
                item.body = nbrs_activity::runner::expand_workload_params(&item.body, &params);
                if let Some(tf) = item.target_file.as_deref() {
                    item.target_file = Some(nbrs_activity::runner::expand_workload_params(tf, &params));
                }
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
        // Persisted item form doesn't carry per-series style;
        // those live on the workload-source side. Round-trip
        // through `nbrs report` against a session-db fallback
        // sees an empty list, which is the correct behavior:
        // there's no source-of-truth to pull overrides from
        // here.
        series_overrides: Vec::new(),
        // `with-table` is workload-source-only (it controls
        // whether to ALSO emit a table; the persisted db row
        // is whatever the renderer chose to save). Db
        // fallback never auto-emits a companion.
        with_table: false,
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
    output_root: &Path,
    session_db: Option<&Path>,
    strict: bool,
) -> Vec<String> {
    // SRD-46: figure numbers count only plot+table items, in
    // their order across the whole resolved item list. The
    // counter advances even when the kind filter excludes the
    // item, so numbers stay stable regardless of which subset
    // the operator renders.
    let mut fig_num: usize = 0;
    let mut failures: Vec<String> = Vec::new();
    for item in items.iter() {
        if item.kind.is_figure() { fig_num += 1; }
        if !filter.matches(item.kind) { continue; }
        if let Err(e) = render_one(fig_num, item, passthrough, workload_arg, output_root, session_db) {
            classify_render_error(e, strict, &mut failures);
        }
    }
    failures
}

/// Detect SRD-15 strict mode from the args passed to
/// `report_command`. Mirrors the convention used by the
/// runner: `--strict` literally on the arg list, or the
/// `NBRS_STRICT` env var set. When strict is on, no-data
/// figure-render errors keep their hard-failure semantics.
fn is_strict_mode(args: &[String]) -> bool {
    args.iter().any(|a| a == "--strict")
        || std::env::var("NBRS_STRICT").is_ok()
}

/// True when `--rebuild` is on the arg list. Activates the
/// "fresh markdown" code path that deletes every report file
/// the workload would write to *before* the renderer runs.
/// Use case: the operator removed a plot from the workload's
/// `report:` block and wants the resulting markdown to match
/// the new declaration set without orphan sections from the
/// previous render.
fn is_rebuild_mode(args: &[String]) -> bool {
    args.iter().any(|a| a == "--rebuild")
        || std::env::var("NBRS_REPORT_REBUILD").is_ok()
}

/// Delete every report markdown file that the resolved
/// items would write to. Called before rendering when
/// `--rebuild` is set so a re-render reflects the *current*
/// workload declaration set, not a union of every prior
/// run's sections.
///
/// The wipe is scoped to declared-target files only: items
/// without a `target_file` set fall through to the default
/// `summary.md`, and that's deleted exactly once even when
/// many items share it. Files outside the resolved-target
/// set are left untouched — ad-hoc `nbrs report scratch`
/// output, hand-edited notes, prior-run images, all
/// preserved.
fn rebuild_wipe_targets(items: &[ResolvedItem], output_root: &Path) {
    use std::collections::HashSet;
    let mut targets: HashSet<PathBuf> = HashSet::new();
    for item in items {
        let target = item.target_file.as_deref().unwrap_or("summary.md");
        targets.insert(output_root.join(target));
    }
    for path in &targets {
        match std::fs::remove_file(path) {
            Ok(()) => eprintln!("nbrs report: --rebuild removed '{}'", path.display()),
            // ENOENT is expected — first-run rebuild has
            // nothing to wipe; that's fine. Other errors
            // (permission, I/O) print so the operator
            // notices, but don't abort the render — the
            // renderer will fail with a clearer message
            // if it can't write.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => eprintln!(
                "nbrs report: --rebuild: could not remove '{}': {e}",
                path.display(),
            ),
        }
    }
}

/// Classify a render error as a warning vs failure based on
/// strict-mode and the `[no-data]` sentinel. In strict mode
/// every error is a failure (legacy behaviour); otherwise
/// no-data errors print as warnings and don't trigger a
/// nonzero exit. Used by every render-batch entry point.
fn classify_render_error(e: String, strict: bool, failures: &mut Vec<String>) {
    let is_no_data = crate::plot_metrics::is_no_data_error(&e);
    let display = crate::plot_metrics::strip_no_data_prefix(&e);
    if is_no_data && !strict {
        eprintln!("WARNING: {display}");
    } else {
        eprintln!("ERROR: {display}");
        failures.push(display);
    }
}

fn render_by_index(
    items: &[ResolvedItem],
    filter: KindFilter,
    n_arg: &str,
    passthrough: &[String],
    workload_arg: Option<&str>,
    output_root: &Path,
    session_db: Option<&Path>,
    strict: bool,
) -> Vec<String> {
    let indices = match parse_figure_selector(n_arg) {
        Some(v) if !v.is_empty() => v,
        _ => {
            eprintln!(
                "nbrs report figure: argument must be a positive integer, range, or list (got '{n_arg}')\n  \
                 accepted forms: `5`, `2-4`, `2..4`, `2..=4`, `1,3,5`, `1,3-5,7`"
            );
            std::process::exit(2);
        }
    };
    render_by_indices(items, filter, &indices, passthrough, workload_arg, output_root, session_db, strict)
}

fn render_by_indices(
    items: &[ResolvedItem],
    filter: KindFilter,
    indices: &[usize],
    passthrough: &[String],
    workload_arg: Option<&str>,
    output_root: &Path,
    session_db: Option<&Path>,
    strict: bool,
) -> Vec<String> {
    let mut failures: Vec<String> = Vec::new();
    // Walk in given order so the user sees output in the
    // order they requested (`5,3,1` renders 5 then 3 then 1).
    for &n in indices {
        let Some(item) = items.get(n.saturating_sub(1)) else {
            eprintln!("nbrs report: figure {n} out of range (1..{})", items.len());
            std::process::exit(2);
        };
        if !filter.matches(item.kind) {
            eprintln!(
                "nbrs report: figure {n} is a {} but the kind filter requires {:?}",
                item.kind.as_str(), filter,
            );
            std::process::exit(2);
        }
        if let Err(e) = render_one(n, item, passthrough, workload_arg, output_root, session_db) {
            classify_render_error(e, strict, &mut failures);
        }
    }
    failures
}

/// Parse a figure-number selector into a list of 1-based
/// indices. Accepted forms (mix-and-match):
///
/// - `5` — single index
/// - `2-4` — inclusive range (hyphen)
/// - `2..4` — inclusive range (Rust-style; both `..` and `..=`
///   are inclusive in this CLI surface — the human-typing
///   convention overrides the Rust half-open convention here)
/// - `1,3,5` — explicit list
/// - `1,3-5,7` — list with embedded ranges
///
/// Returns `None` if any token isn't numeric / range-shaped.
/// Out-of-order ranges (`5-2`) error rather than treating as
/// reversed iteration. Empty input → `None`.
fn parse_figure_selector(s: &str) -> Option<Vec<usize>> {
    let s = s.trim();
    if s.is_empty() { return None; }
    let mut out: Vec<usize> = Vec::new();
    for token in s.split(',') {
        let token = token.trim();
        if token.is_empty() { return None; }
        // Try Rust-style range first so `2..4` doesn't get
        // hyphen-split. `..=` is the inclusive form; for this
        // CLI we treat `..` as inclusive too — humans typing
        // `2..4` usually mean "2 through 4."
        let (lo, hi) = if let Some((l, r)) = token.split_once("..=") {
            (l.trim(), Some(r.trim()))
        } else if let Some((l, r)) = token.split_once("..") {
            (l.trim(), Some(r.trim()))
        } else if let Some((l, r)) = token.split_once('-') {
            // `-` ambiguity: the empty-LHS case (`-5`) would be
            // a negative literal — figure indices are positive,
            // so reject it.
            if l.is_empty() { return None; }
            (l.trim(), Some(r.trim()))
        } else {
            (token, None)
        };
        let lo: usize = lo.parse().ok()?;
        if lo == 0 { return None; }
        match hi {
            Some(h) => {
                let hi: usize = h.parse().ok()?;
                if hi < lo { return None; }
                for i in lo..=hi { out.push(i); }
            }
            None => out.push(lo),
        }
    }
    Some(out)
}

fn render_by_glob(
    items: &[ResolvedItem],
    filter: KindFilter,
    glob: &str,
    passthrough: &[String],
    workload_arg: Option<&str>,
    output_root: &Path,
    session_db: Option<&Path>,
    strict: bool,
) -> Vec<String> {
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
    let mut failures: Vec<String> = Vec::new();
    for (n, item) in matches {
        if let Err(e) = render_one(n, item, passthrough, workload_arg, output_root, session_db) {
            classify_render_error(e, strict, &mut failures);
        }
    }
    failures
}

/// Render a single resolved item. Returns `Err(message)`
/// when a plot fails to render — the caller is responsible
/// for surfacing the failure and exiting nonzero. Plot
/// failures must not be silently dropped: a missing figure
/// is a real defect in the workload or its data, not a
/// recoverable condition (see "Never Ignore Silently"
/// guidance). Tables route through `summary_command`,
/// which currently exits the process on its own errors —
/// when that gets refactored to return Result, table
/// failures should funnel through the same path as plots.
fn render_one(
    n: usize,
    item: &ResolvedItem,
    passthrough: &[String],
    workload_arg: Option<&str>,
    output_root: &Path,
    session_db: Option<&Path>,
) -> Result<(), String> {
    use nbrs_workload::report::Kind;
    // File items are scope directives — they don't render
    // anything themselves; their children do (during normal
    // iteration through the items list).
    if matches!(item.kind, Kind::File) {
        return Ok(());
    }
    if matches!(item.kind, Kind::Text) {
        render_text(item, output_root);
        return Ok(());
    }
    let mut base: Vec<String> = Vec::new();
    if let Some(w) = workload_arg { base.push(w.to_string()); }
    // Re-inject the resolved session db as an explicit `--db`
    // so the downstream renderer (which sees only `base` plus
    // `passthrough`, neither containing the original `--session`
    // since `extract_workload` peeled it off) reads from the
    // user-named session and not from `logs/latest`.
    if let Some(db) = session_db {
        base.push("--db".into());
        base.push(db.to_string_lossy().into_owned());
    }
    base.push(format!("--name={}", item.name));
    base.push("--figure-num".into());
    base.push(n.to_string());
    if let Some(l) = item.label.as_deref() {
        base.push("--label".into());
        base.push(l.to_string());
    }
    if let Some(t) = item.target_file.as_deref() {
        base.push("--report".into());
        base.push(output_root.join(t).to_string_lossy().into_owned());
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
        // Per-series style overrides. One `--style` flag per
        // override, repeated. Value form is the brace-free
        // directive list `key=value:k=v k=v` so the renderer
        // can parse it back identically to how the YAML body
        // / CLI surface emit them.
        for so in &item.series_overrides {
            base.push("--style".into());
            let mut s = format!("{}={}:", so.key, so.value);
            let mut first = true;
            for line in so.style.scalar_directive_lines() {
                if !first { s.push(' '); }
                first = false;
                s.push_str(&line);
            }
            base.push(s);
        }
    }
    base.extend(passthrough.iter().cloned());
    match item.kind {
        Kind::Plot => {
            // Use the result-returning variant so a no-rows
            // failure on one plot doesn't abort the rest of a
            // `report all` batch — but we surface the failure
            // to the caller so the overall report exits nonzero
            // when any figure failed. Silent skip would let a
            // broken workload masquerade as a successful run.
            //
            // Preserve the `[no-data]` sentinel from
            // `plot_metrics` through the wrap so the upstream
            // collector can downgrade it to a warning under
            // non-strict mode (incremental / auto-render
            // legitimately produces empty results before data
            // accumulates).
            let plot_result = crate::plot_metrics::plot_metrics_command_result(&base)
                .map_err(|e| {
                    if crate::plot_metrics::is_no_data_error(&e) {
                        format!(
                            "{}plot '{}' has no data: {}",
                            crate::plot_metrics::PLOT_NO_DATA_PREFIX,
                            item.name,
                            crate::plot_metrics::strip_no_data_prefix(&e),
                        )
                    } else {
                        format!("plot '{}' failed: {e}", item.name)
                    }
                });

            // SRD-46 plot-only `with-table: true` companion.
            // Render the table view immediately after the
            // plot so the markdown carries both views in the
            // same section flow. The companion uses the same
            // body as the plot; the summary renderer reads
            // off `y / y1 / y2 / y3 / y4 / x-ticks` queries
            // and tabulates them. Failures here are
            // *secondary* — the plot's already rendered (or
            // already failed); the companion's outcome is
            // logged but doesn't replace the plot's
            // success/failure return.
            if item.with_table && plot_result.is_ok() {
                if let Err(e) = render_companion_table(n, item, output_root, session_db) {
                    eprintln!(
                        "WARNING: companion table for plot '{}' failed: {e}",
                        item.name,
                    );
                }
            }
            plot_result
        }
        Kind::Table => {
            crate::summary::summary_command(&base);
            Ok(())
        }
        Kind::Text | Kind::File | Kind::Details => unreachable!(),
    }
}

/// Render a companion table for a plot whose body declared
/// `with-table: true`. Reuses the plot's body (each `y` /
/// `y1` / `y2` / `y3` / `y4` line becomes one table column;
/// `x` / `x-ticks` provide the row key). The table writes
/// into the same target markdown file as the plot,
/// immediately after the plot's section, with an anchor
/// derived from the plot's name plus a `_table` suffix so
/// users can link to either view independently.
///
/// Implemented as a thin facade over the existing summary
/// renderer: we synthesise a table-shaped argv from the
/// plot's `y*` queries, call `summary_command`, and let it
/// do the markdown emission via the same path tables use
/// today.
fn render_companion_table(
    plot_figure_num: usize,
    item: &ResolvedItem,
    output_root: &Path,
    session_db: Option<&Path>,
) -> Result<(), String> {
    let columns = extract_y_queries(&item.body);
    if columns.is_empty() {
        return Err("no y/y1/y2/y3/y4 query lines in plot body".into());
    }
    let group_by = extract_x_query(&item.body)
        .or_else(|| extract_xticks_query(&item.body))
        .map(|q| extract_label_keys(&q))
        .unwrap_or_default();

    let mut argv: Vec<String> = vec![
        format!("--name={}_table", item.name),
        "--figure-num".into(),
        // Numbering convention: companion uses the parent
        // plot's number with an `a` suffix in the heading,
        // but for the tools that need a numeric arg we
        // pass the plot's number — the heading is
        // overridden via --label below.
        plot_figure_num.to_string(),
    ];
    let plot_label = item.label.clone()
        .unwrap_or_else(|| crate::report::prettify_name(&item.name));
    argv.push("--label".into());
    argv.push(format!("{plot_label} (data)"));
    if let Some(target) = item.target_file.as_deref() {
        argv.push("--report".into());
        argv.push(output_root.join(target).to_string_lossy().into_owned());
    }
    if let Some(db) = session_db {
        argv.push("--db".into());
        argv.push(db.to_string_lossy().into_owned());
    }
    if !group_by.is_empty() {
        argv.push("--group-by".into());
        argv.push(group_by.join(","));
    }
    for (col_name, query) in columns {
        argv.push("--column".into());
        argv.push(format!("{col_name}={query}"));
    }
    crate::summary::summary_command(&argv);
    Ok(())
}

/// Pull every `y[N]: <query>` line out of a plot body,
/// returning `(column_name, query)` pairs. The column name
/// for `y` / `y1` is `recall_y1` (etc.) — namespaced so
/// the companion table's column headings stay distinct
/// from any other columns the user might add.
fn extract_y_queries(body: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        for prefix in ["y:", "y1:", "y2:", "y3:", "y4:"] {
            if let Some(rest) = line.strip_prefix(prefix) {
                let key = match prefix {
                    "y:" | "y1:" => "y1",
                    "y2:" => "y2",
                    "y3:" => "y3",
                    "y4:" => "y4",
                    _ => unreachable!(),
                };
                out.push((key.to_string(), rest.trim().to_string()));
                break;
            }
        }
    }
    out
}

/// Pull the `x: <query>` line if present.
fn extract_x_query(body: &str) -> Option<String> {
    for line in body.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("x:") {
            return Some(rest.trim().to_string());
        }
    }
    None
}

/// Pull the `x-ticks: <query>` line if present.
fn extract_xticks_query(body: &str) -> Option<String> {
    for line in body.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("x-ticks:") {
            return Some(rest.trim().to_string());
        }
    }
    None
}

/// Pull the label keys out of a `... by (k1, k2, k3)` clause
/// in a metricsql query. Used to seed the companion
/// table's group_by so each row corresponds to one (k1,
/// k2, k3) tuple of the plot's series discriminators.
fn extract_label_keys(query: &str) -> Vec<String> {
    let lower = query.to_ascii_lowercase();
    let by_idx = lower.rfind(" by ");
    let by_idx = match by_idx { Some(i) => i, None => return Vec::new() };
    let after = &query[by_idx + 4..];
    let after = after.trim();
    let inner = after.strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(after);
    inner.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Render a text item by writing its body verbatim into the
/// target markdown file (or `summary.md` when no `target_file`
/// is set). The heading uses the label, falling back to a
/// prettified canonical name. No figure number — text isn't a
/// figure (SRD-46).
fn render_text(item: &ResolvedItem, output_root: &Path) {
    let target = item.target_file.as_deref().unwrap_or("summary.md");
    let path = output_root.join(target);
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

    // ── Figure-selector parser ──────────────────────────────

    #[test]
    fn figure_selector_single_index() {
        assert_eq!(parse_figure_selector("5"), Some(vec![5]));
        assert_eq!(parse_figure_selector(" 5 "), Some(vec![5]));
    }

    #[test]
    fn figure_selector_hyphen_range() {
        assert_eq!(parse_figure_selector("2-4"), Some(vec![2, 3, 4]));
        assert_eq!(parse_figure_selector("1-1"), Some(vec![1]));
    }

    #[test]
    fn figure_selector_rust_range_inclusive() {
        // Both `..` and `..=` resolve to inclusive in this
        // CLI surface — humans typing `2..4` usually mean
        // "through 4," not "stop short of 4."
        assert_eq!(parse_figure_selector("2..4"), Some(vec![2, 3, 4]));
        assert_eq!(parse_figure_selector("2..=4"), Some(vec![2, 3, 4]));
    }

    #[test]
    fn figure_selector_comma_list() {
        assert_eq!(parse_figure_selector("2,3,4"), Some(vec![2, 3, 4]));
        assert_eq!(parse_figure_selector("1, 3 ,5"), Some(vec![1, 3, 5]));
    }

    #[test]
    fn figure_selector_mixed_list_with_ranges() {
        assert_eq!(
            parse_figure_selector("1,3-5,7"),
            Some(vec![1, 3, 4, 5, 7]),
        );
        assert_eq!(
            parse_figure_selector("1,3..5,7"),
            Some(vec![1, 3, 4, 5, 7]),
        );
    }

    #[test]
    fn figure_selector_preserves_user_order() {
        // No automatic sort/dedup — `5,3,1` renders 5 then 3
        // then 1.
        assert_eq!(parse_figure_selector("5,3,1"), Some(vec![5, 3, 1]));
    }

    #[test]
    fn figure_selector_rejects_non_numeric() {
        assert_eq!(parse_figure_selector("recall"), None);
        assert_eq!(parse_figure_selector("recall_at_k10"), None);
        assert_eq!(parse_figure_selector("2,abc"), None);
    }

    #[test]
    fn figure_selector_rejects_zero_and_negative() {
        // 0 is invalid (1-based indexing); `-5` looks like a
        // negative literal which is a missing-LHS hyphen
        // range and rejected.
        assert_eq!(parse_figure_selector("0"), None);
        assert_eq!(parse_figure_selector("-5"), None);
        assert_eq!(parse_figure_selector("0-5"), None);
    }

    #[test]
    fn figure_selector_rejects_reversed_ranges() {
        // `5-2` is rejected rather than treated as reversed
        // iteration — disambiguate via comma list (`5,4,3,2`).
        assert_eq!(parse_figure_selector("5-2"), None);
        assert_eq!(parse_figure_selector("4..2"), None);
    }

    #[test]
    fn figure_selector_rejects_empty() {
        assert_eq!(parse_figure_selector(""), None);
        assert_eq!(parse_figure_selector("  "), None);
        assert_eq!(parse_figure_selector(",,"), None);
    }
}

// ── cli_spec entry ─────────────────────────────────────────

/// Build a child Command for one report subcommand. Every
/// child is `raw_args=true` because its parser is owned by
/// `report_command` (vocab-driven for plot/table/etc.,
/// hand-rolled for list/all/show/figure/rename/scratch).
/// Handler reconstructs `[subname, ...raw]` and forwards to
/// the dispatcher so the legacy parser sees its expected
/// shape.
fn report_subleaf(subname: &'static str, help: &'static str)
    -> crate::cli_spec::Command
{
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    // One-off handlers per subname — fn pointers can't capture
    // so we route through a shared dispatch table by name.
    fn h(subname: &'static str)
        -> fn(ParsedCommand) -> Result<(), String>
    {
        match subname {
            "plot"     => |p| { dispatch(p, "plot");    Ok(()) },
            "table"    => |p| { dispatch(p, "table");   Ok(()) },
            "text"     => |p| { dispatch(p, "text");    Ok(()) },
            "file"     => |p| { dispatch(p, "file");    Ok(()) },
            "details"  => |p| { dispatch(p, "details"); Ok(()) },
            "list"     => |p| { dispatch(p, "list");    Ok(()) },
            "all"      => |p| { dispatch(p, "all");     Ok(()) },
            "show"     => |p| { dispatch(p, "show");    Ok(()) },
            "figure"   => |p| { dispatch(p, "figure");  Ok(()) },
            "rename"   => |p| { dispatch(p, "rename");  Ok(()) },
            "scratch"  => |p| { dispatch(p, "scratch"); Ok(()) },
            _ => |_| Err(format!("report: unknown subcommand")),
        }
    }
    fn dispatch(p: ParsedCommand, subname: &str) {
        let mut argv: Vec<String> = vec![subname.to_string()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
    }
    Command {
        name: subname, help,
        category: Category::Tools, level: Level::Secondary,
        flags: Vec::new(),
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(h(subname))),
        raw_args: true,
        completion_override: None,
    }
}

/// `nbrs report …` — SRD-46/64 report surface. raw_args at
/// each leaf because the existing parser is vocab-driven and
/// richer than the generic walker can express today. The spec
/// declares every subcommand so tab on `nbrs report <TAB>`
/// surfaces them; handlers reconstruct the legacy argv shape
/// and dispatch.
///
/// **Open gap:** vocab-driven *flag* completion under each
/// kind leaf (e.g. `nbrs report plot --<TAB>`) is still
/// served by the legacy `kind_subcommand_node()` in
/// `completion.rs` because cli_spec's Flag model doesn't yet
/// import vocab Directives. Future work: a
/// `vocab::Directive → cli_spec::Flag` adapter so the spec
/// becomes the only flag-source.
pub fn spec() -> crate::cli_spec::Command {
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    // Group-level handler covers the "no subcommand matched"
    // path: bare `nbrs report` (lists figures), `nbrs report
    // <glob>` (matches items by name), and pass-through for
    // unknown args. `raw_args: true` lets the walker hand
    // every remaining token to this handler verbatim — the
    // legacy `report_command` does the dispatch.
    fn handle(p: ParsedCommand) -> Result<(), String> {
        report_command(&p.raw, KindFilter::Any);
        Ok(())
    }
    Command {
        name: "report",
        help: "Render report items defined in a workload's `report:` block.",
        category: Category::Tools,
        level: Level::Secondary,
        flags: Vec::new(),
        positionals: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: true,
        completion_override: None,
        subcommands: vec![
            // Vocab-driven kind subcommands route their
            // *completion* through the legacy kind_subcommand_node
            // helper (which iterates `vocab::ALL_DIRECTIVES` to
            // produce flag completions + per-flag value
            // providers). The cli_spec adapter honors
            // `completion_override` and uses that Node verbatim.
            //
            // Only `plot` and `table` are advertised here:
            // the legacy parser only kind-promotes those two
            // (`text`/`file`/`details` exist in the workload
            // YAML grammar but `nbrs report <kind>` doesn't
            // accept them on the CLI today). Surfacing them
            // would mislead users — the bare-form glob path
            // would error with "no items match".
            kind_subleaf("plot",    "Render plot items by name (kind-filtered).",  nbrs_workload::report::Kind::Plot),
            kind_subleaf("table",   "Render table items by name (kind-filtered).", nbrs_workload::report::Kind::Table),
            // `list` and bare-form `nbrs report` produce the
            // same listing — the user's canonical command is
            // `nbrs report list`, the bare form is the shortcut.
            report_subleaf("list",    "List figures defined in the report."),
            report_subleaf("all",     "Render every report item."),
            report_subleaf("figure",  "Render by figure number / range."),
            rename_subleaf(),
            scratch_subleaf(),
        ],
    }
}

/// Build a kind subcommand (`plot` / `table` / `text` / `file` /
/// `details`) whose completion routes through
/// [`crate::completion::kind_subcommand_node`] for vocab-driven
/// flag + value-provider plumbing.
fn kind_subleaf(
    subname: &'static str,
    help: &'static str,
    kind: nbrs_workload::report::Kind,
) -> crate::cli_spec::Command {
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    fn handle_plot(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = vec!["plot".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    fn handle_table(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = vec!["table".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    fn handle_text(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = vec!["text".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    fn handle_file(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = vec!["file".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    fn handle_details(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = vec!["details".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    let handler = match subname {
        "plot"    => Handler::Sync(handle_plot),
        "table"   => Handler::Sync(handle_table),
        "text"    => Handler::Sync(handle_text),
        "file"    => Handler::Sync(handle_file),
        "details" => Handler::Sync(handle_details),
        _ => unreachable!(),
    };
    let override_fn: fn() -> veks_completion::Node = match kind {
        nbrs_workload::report::Kind::Plot =>
            || crate::completion::kind_subcommand_node(nbrs_workload::report::Kind::Plot),
        nbrs_workload::report::Kind::Table =>
            || crate::completion::kind_subcommand_node(nbrs_workload::report::Kind::Table),
        nbrs_workload::report::Kind::Text =>
            || crate::completion::kind_subcommand_node(nbrs_workload::report::Kind::Text),
        nbrs_workload::report::Kind::File =>
            || crate::completion::kind_subcommand_node(nbrs_workload::report::Kind::File),
        nbrs_workload::report::Kind::Details =>
            || crate::completion::kind_subcommand_node(nbrs_workload::report::Kind::Details),
    };
    Command {
        name: subname, help,
        category: Category::Tools, level: Level::Secondary,
        flags: Vec::new(),
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(handler),
        raw_args: true,
        completion_override: Some(override_fn),
    }
}

/// `nbrs report rename` — typed flag set declared in
/// cli_spec so completion offers `--replace`, `--dry-run`,
/// `--workload`.
fn rename_subleaf() -> crate::cli_spec::Command {
    use crate::cli_spec::{Arity, Category, Command, Flag, Handler,
        Level, ParsedCommand, ValueProvider};
    fn handle(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = vec!["rename".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    Command {
        name: "rename",
        help: "Rename a workload report item.",
        category: Category::Tools,
        level: Level::Secondary,
        flags: vec![
            Flag {
                long: "--workload", short: None, aliases: &[],
                arity: Arity::Value, value: ValueProvider::Path,
                help: "Override the workload file to mutate.",
                repeatable: false,
            },
            Flag {
                long: "--replace", short: None, aliases: &[],
                arity: Arity::Bool, value: ValueProvider::None,
                help: "Overwrite if `<new>` already exists.",
                repeatable: false,
            },
            Flag {
                long: "--dry-run", short: None, aliases: &[],
                arity: Arity::Bool, value: ValueProvider::None,
                help: "Print intended change without writing.",
                repeatable: false,
            },
        ],
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: true,
        completion_override: None,
    }
}

/// `nbrs report scratch` — has its own list/clean/promote
/// children. Modelled as a Command group with raw_args leaves.
fn scratch_subleaf() -> crate::cli_spec::Command {
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    fn h_list(p: ParsedCommand) -> Result<(), String> {
        let mut argv = vec!["scratch".into(), "list".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    fn h_clean(p: ParsedCommand) -> Result<(), String> {
        let mut argv = vec!["scratch".into(), "clean".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    fn h_promote(p: ParsedCommand) -> Result<(), String> {
        let mut argv = vec!["scratch".into(), "promote".into()];
        argv.extend(p.raw.iter().cloned());
        report_command(&argv, KindFilter::Any);
        Ok(())
    }
    fn child(name: &'static str, help: &'static str,
             handler: fn(ParsedCommand) -> Result<(), String>)
        -> Command
    {
        Command {
            name, help,
            category: Category::Tools, level: Level::Secondary,
            flags: Vec::new(),
            positionals: Vec::new(),
            subcommands: Vec::new(),
            handler: Some(Handler::Sync(handler)),
            raw_args: true,
            completion_override: None,
        }
    }
    Command {
        name: "scratch",
        help: "Inspect / clean / promote scratch renders.",
        category: Category::Tools,
        level: Level::Secondary,
        flags: Vec::new(),
        positionals: Vec::new(),
        handler: None,
        raw_args: false,
        completion_override: None,
        subcommands: vec![
            child("list",    "List scratch entries.",    h_list),
            child("clean",   "Remove scratch entries.",  h_clean),
            child("promote", "Promote scratch to workload.", h_promote),
        ],
    }
}

/// `nbrs plot …` — unadvertised alias for `nbrs report plot …`.
pub fn plot_alias_spec() -> crate::cli_spec::Command {
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    fn handle(p: ParsedCommand) -> Result<(), String> {
        report_command(&p.raw, KindFilter::Plot);
        Ok(())
    }
    Command {
        name: "plot",
        help: "Alias for `nbrs report plot`.",
        category: Category::Tools,
        level: Level::Secondary,
        flags: Vec::new(),
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: true,
        completion_override: None,
    }
}

/// `nbrs table …` — unadvertised alias for `nbrs report table …`.
pub fn table_alias_spec() -> crate::cli_spec::Command {
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    fn handle(p: ParsedCommand) -> Result<(), String> {
        report_command(&p.raw, KindFilter::Table);
        Ok(())
    }
    Command {
        name: "table",
        help: "Alias for `nbrs report table`.",
        category: Category::Tools,
        level: Level::Secondary,
        flags: Vec::new(),
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: true,
        completion_override: None,
    }
}
