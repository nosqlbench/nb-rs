// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `nbrs replay` — write the readout snapshots from a
//! session db to stdout, reproducing the operator-visible
//! status / DONE lines from a finished run.
//!
//! See SRD-63 §6 for the snapshot store contract and
//! SRD-76 for the per-phase outcome store. Replay walks
//! both: the `phase_outcomes` + `phase_errors` tables
//! (SRD-76 — structured terminal state, rendered via the
//! `phase_outcome` readout) and the `readout_snapshots`
//! table (SRD-63 — per-fire pre-rendered text). Both
//! channels coexist; pre-SRD-76 sessions degrade to
//! `readout_snapshots`-only.
//!
//! Usage:
//!
//! ```text
//! nbrs replay                          # default: logs/latest/metrics.db
//! nbrs replay --session=logs/foo       # explicit session dir
//! nbrs replay --plain                  # strip ANSI styling
//! nbrs replay --errors                 # only phases that failed
//! nbrs replay --phase=<name>           # filter to one phase identity
//! nbrs replay --json                   # machine-readable per-outcome JSON
//! ```

use std::io::Write as _;
use std::path::PathBuf;

pub fn replay_command(args: &[String]) {
    let opts = match parse_args(args) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("nbrs replay: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = run(opts) {
        eprintln!("nbrs replay: {e}");
        std::process::exit(1);
    }
}

struct Opts {
    db_path: PathBuf,
    plain: bool,
    /// SRD-76 — only render phases whose status is `Failed`.
    /// Skips Completed / Skipped / CursorSuspended rows from
    /// `phase_outcomes` entirely. Does not affect the
    /// `readout_snapshots` fallback channel — that path has
    /// no structured status to filter on.
    errors_only: bool,
    /// SRD-76 — when set, only render the named phase.
    /// Matches against the bare `phase_name` (not the
    /// `name@labels` form), so a sweep over multiple cells
    /// shows every cell of that phase.
    phase_filter: Option<String>,
    /// SRD-76 — dump structured `PhaseOutcome` as one JSON
    /// object per line to stdout, instead of running the
    /// readout binder. Useful for CI / downstream tooling.
    json: bool,
    /// SRD-77 — which execution(s) of the session to render.
    /// `None` (the default) auto-picks the maximum `exec_id`
    /// observed in `phase_outcomes` — i.e. "the most recent
    /// execution" — so a session with multiple refines shows
    /// the latest layer's outcomes by default.
    /// `Some(n)` filters to exactly `exec_id = n`.
    /// `Some(0)` is the sentinel for "all executions"
    /// (`--all-executions`).
    execution: Option<u64>,
}

fn parse_args(args: &[String]) -> Result<Opts, String> {
    let mut db_path: Option<PathBuf> = None;
    let mut plain = false;
    let mut errors_only = false;
    let mut phase_filter: Option<String> = None;
    let mut json = false;
    let mut execution: Option<u64> = None;
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a == "--plain" {
            plain = true;
        } else if a == "--errors" {
            errors_only = true;
        } else if a == "--json" {
            json = true;
        } else if let Some(rest) = a.strip_prefix("--phase=") {
            phase_filter = Some(rest.to_string());
        } else if a == "--phase" {
            let v = args.get(i + 1)
                .ok_or_else(|| "--phase requires a value".to_string())?;
            phase_filter = Some(v.to_string());
            i += 1;
        } else if let Some(rest) = a.strip_prefix("--session=") {
            db_path = Some(session_db(rest)?);
        } else if a == "--session" {
            let v = args.get(i + 1)
                .ok_or_else(|| "--session requires a value".to_string())?;
            db_path = Some(session_db(v)?);
            i += 1;
        } else if let Some(rest) = a.strip_prefix("--db=") {
            db_path = Some(PathBuf::from(rest));
        } else if a == "--db" {
            let v = args.get(i + 1)
                .ok_or_else(|| "--db requires a value".to_string())?;
            db_path = Some(PathBuf::from(v));
            i += 1;
        } else if let Some(rest) = a.strip_prefix("--execution=") {
            execution = Some(rest.parse().map_err(|e|
                format!("--execution requires an integer: {e}"))?);
        } else if a == "--execution" {
            let v = args.get(i + 1)
                .ok_or_else(|| "--execution requires a value".to_string())?;
            execution = Some(v.parse().map_err(|e|
                format!("--execution requires an integer: {e}"))?);
            i += 1;
        } else if a == "--all-executions" {
            // Sentinel: `Some(0)` means "no exec_id filter". Real
            // exec_ids are 1-indexed (Execution::first → 1), so 0
            // doesn't collide with any actual stored row.
            execution = Some(0);
        } else if a == "-h" || a == "--help" {
            print_usage();
            std::process::exit(0);
        } else {
            return Err(format!("unexpected arg '{a}' (use --session=<dir> or --db=<path>)"));
        }
        i += 1;
    }
    let db_path = db_path.unwrap_or_else(|| nbrs_activity::session::latest_metrics_db());
    if !db_path.exists() {
        return Err(format!(
            "session db not found at '{}' — pass --session=<dir> or --db=<path>",
            db_path.display(),
        ));
    }
    Ok(Opts { db_path, plain, errors_only, phase_filter, json, execution })
}

fn session_db(session_arg: &str) -> Result<PathBuf, String> {
    let p = PathBuf::from(session_arg);
    if p.is_dir() {
        Ok(p.join("metrics.db"))
    } else if p.exists() {
        // Treat as a direct .db file.
        Ok(p)
    } else {
        // Try as a logs/<name> session directory.
        let candidate = nbrs_activity::session::session_dir_named(session_arg).join("metrics.db");
        if candidate.exists() {
            Ok(candidate)
        } else {
            Err(format!(
                "session '{session_arg}' not found (looked for {p:?} and {candidate:?})"
            ))
        }
    }
}

fn print_usage() {
    eprintln!("Usage: nbrs replay [options]");
    eprintln!();
    eprintln!("Walks the session's structured outcome store (SRD-76");
    eprintln!("phase_outcomes / phase_errors) and renders each terminal");
    eprintln!("disposition via the `phase_outcome` readout. Falls back");
    eprintln!("to the readout-snapshot store for pre-SRD-76 sessions.");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --session <dir>  Session directory (default: logs/latest)");
    eprintln!("  --db <path>      Direct path to metrics.db");
    eprintln!("  --plain          Strip ANSI styling from output");
    eprintln!("  --errors         Only show phases that failed");
    eprintln!("  --phase <name>   Filter to a single phase identity");
    eprintln!("  --json                Machine-readable JSON dump (one outcome per line)");
    eprintln!("  --execution=<n>       Filter to one execution (default: most recent)");
    eprintln!("  --all-executions      Render every execution's outcomes");
    eprintln!("  -h, --help            Show this message");
}

fn run(opts: Opts) -> Result<(), String> {
    let reporter = nbrs_metrics::reporters::sqlite::SqliteReporter::new(&opts.db_path)
        .map_err(|e| format!("opening {}: {e}", opts.db_path.display()))?;

    // SRD-76 — prefer the structured phase_outcomes store.
    // When it's populated, render each outcome via the
    // `phase_outcome` readout (the same renderer the live
    // surface uses). When it's empty (pre-SRD-76 session,
    // or a session that ran without any phase outcomes
    // recorded), fall back to the SRD-63 readout_snapshots
    // pre-rendered store.
    // SRD-77 — execution qualification at the storage boundary.
    // Default (`None` in `opts.execution`): qualify to the
    // most-recent execution via the shared resolver. An
    // explicit `--execution=<n>` narrows; `--all-executions`
    // (sentinel `0`) opts into the aggregate-across-executions
    // intent. The storage layer applies the WHERE filter so
    // we don't pull every execution's rows just to filter them
    // out in memory.
    let session_dir = opts.db_path.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_default();
    let exec_id_filter: Option<u64> = match opts.execution {
        Some(0) => None,
        Some(n) => Some(n),
        None => {
            // Only emit the multi-exec banner under the
            // implicit-default path; explicit `--execution=`
            // or `--all-executions` means the operator is
            // already aware of the choice and doesn't need
            // the disambiguation hint.
            nbrs_activity::refine_plan::warn_multi_execution_default(&session_dir);
            nbrs_activity::refine_plan::ExecutionQualifier::latest(&session_dir)
                .specific_id()
        }
    };
    let mut outcomes = reporter.read_phase_outcomes(exec_id_filter);
    if let Some(name) = opts.phase_filter.as_deref() {
        outcomes.retain(|o| o.phase_name == name);
    }
    if opts.errors_only {
        outcomes.retain(|o| o.status == "failed");
    }

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    if !outcomes.is_empty() {
        if opts.json {
            for o in &outcomes {
                let json = outcome_to_json(o);
                writeln!(out, "{json}").map_err(|e| e.to_string())?;
            }
        } else {
            for o in &outcomes {
                let rendered = render_outcome(o, opts.plain);
                writeln!(out, "{rendered}").map_err(|e| e.to_string())?;
            }
        }
        return Ok(());
    }

    // Pre-SRD-76 fallback: the structured store is empty,
    // so walk the per-fire snapshot store. `--errors` /
    // `--phase=` / `--json` don't apply here (no structured
    // axis to filter on).
    if opts.errors_only || opts.phase_filter.is_some() || opts.json {
        eprintln!(
            "nbrs replay: '{}' has no phase_outcomes rows; \
             --errors / --phase / --json are SRD-76 features and \
             aren't supported against the legacy snapshot store.",
            opts.db_path.display(),
        );
        return Ok(());
    }
    let rows = reporter.read_readout_snapshots();
    if rows.is_empty() {
        eprintln!(
            "nbrs replay: no outcomes or snapshots in '{}' \
             (was readouts capture enabled during the run?)",
            opts.db_path.display(),
        );
        return Ok(());
    }
    for row in rows {
        let body = if opts.plain {
            row.body_plain
        } else {
            row.body_ansi
                .as_deref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or(row.body_plain)
        };
        writeln!(out, "{body}").map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// SRD-76 — render a stored [`PhaseOutcomeRow`] via the same
/// `phase_outcome` readout the live surface uses. Reconstructs
/// the minimum [`ReadoutContext`] needed to drive the
/// Labeled-LOD render: subject identity (name + labels),
/// elapsed wall-clock, and the structured outcome (status +
/// errors). Other counter / chip / coord-fold fields stay at
/// their defaults — replay doesn't have the per-cycle metrics
/// the live surface fed in.
fn render_outcome(
    row: &nbrs_metrics::reporters::sqlite::PhaseOutcomeRow,
    plain: bool,
) -> String {
    use nbrs_activity::readouts::{
        Lod, ContentMode, ReadoutContext,
        ReadoutOptions,
    };
    use nbrs_activity::lifecycle::EventType;
    use nbrs_activity::readouts::buf::StringBuf;
    use nbrs_activity::phase_outcome::{
        PhaseStatus, PhaseErrorDetail, ResumeCursor,
    };

    struct ReplayCtx<'a> {
        name: &'a str,
        labels: &'a str,
        elapsed_secs: f64,
        status: PhaseStatus,
        errors: Vec<PhaseErrorDetail>,
        use_color: bool,
    }

    impl<'a> ReadoutContext for ReplayCtx<'a> {
        fn subject_name(&self) -> &str { self.name }
        fn subject_labels(&self) -> &str { self.labels }
        fn elapsed_secs(&self) -> f64 { self.elapsed_secs }
        fn use_color(&self) -> bool { self.use_color }
        fn event(&self) -> EventType { EventType::PhaseEnd }
        fn outcome_status(&self) -> PhaseStatus { self.status }
        fn outcome_errors(&self) -> &[PhaseErrorDetail] { &self.errors }
        fn outcome_resume_cursor(&self) -> Option<&ResumeCursor> { None }
    }

    let status = parse_status(&row.status);
    let errors: Vec<PhaseErrorDetail> = row.errors.iter().map(|e| PhaseErrorDetail {
        class: e.class.clone(),
        message: e.message.clone(),
        op_name: e.op_name.clone(),
        cycle: e.cycle,
        op_template: e.op_template.clone(),
        op_resolved: e.op_resolved.clone(),
        at_nanos: e.at_nanos as u64,
        retryable: e.retryable,
    }).collect();

    let ctx = ReplayCtx {
        name: &row.phase_name,
        labels: &row.phase_labels,
        elapsed_secs: row.duration_secs,
        status,
        errors,
        use_color: !plain,
    };
    let readout = nbrs_activity::readouts::Registry::lookup("phase_outcome")
        .expect("phase_outcome registered");
    let mut s = String::with_capacity(192);
    let mut buf = StringBuf::new(&mut s);
    readout.render(
        &ctx, Lod::Labeled, ContentMode::Value,
        &ReadoutOptions::new(), &mut buf,
    );
    s
}

fn parse_status(label: &str) -> nbrs_activity::phase_outcome::PhaseStatus {
    use nbrs_activity::phase_outcome::PhaseStatus;
    match label {
        "completed"        => PhaseStatus::Completed,
        "failed"           => PhaseStatus::Failed,
        "skipped"          => PhaseStatus::Skipped,
        "cursor_suspended" => PhaseStatus::CursorSuspended,
        // Defensive: unrecognised statuses render as Failed
        // so the operator notices instead of seeing a silent
        // ✓ for an unknown state.
        _                  => PhaseStatus::Failed,
    }
}

/// One-line JSON object per outcome. Hand-rolled rather than
/// pulling in a serializer dep on the storage-row shape — the
/// fields are stable and few.
fn outcome_to_json(
    row: &nbrs_metrics::reporters::sqlite::PhaseOutcomeRow,
) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(256);
    let _ = write!(
        &mut s,
        r#"{{"session":{:?},"exec_id":{},"phase_name":{:?},"phase_labels":{:?},"status":{:?},"duration_secs":{},"started_at_nanos":{},"ended_at_nanos":{},"errors":["#,
        row.session, row.exec_id, row.phase_name, row.phase_labels,
        row.status, row.duration_secs, row.started_at_nanos, row.ended_at_nanos,
    );
    for (i, e) in row.errors.iter().enumerate() {
        if i > 0 { s.push(','); }
        let _ = write!(
            &mut s,
            r#"{{"class":{:?},"message":{:?},"op_name":{},"cycle":{},"op_template":{},"op_resolved":{},"at_nanos":{},"retryable":{}}}"#,
            e.class, e.message,
            opt_str_json(e.op_name.as_deref()),
            opt_u64_json(e.cycle),
            opt_str_json(e.op_template.as_deref()),
            opt_str_json(e.op_resolved.as_deref()),
            e.at_nanos, e.retryable,
        );
    }
    s.push_str("]}");
    s
}

fn opt_str_json(v: Option<&str>) -> String {
    match v { Some(s) => format!("{:?}", s), None => "null".into() }
}

fn opt_u64_json(v: Option<u64>) -> String {
    match v { Some(n) => n.to_string(), None => "null".into() }
}

// ── cli_spec entry ─────────────────────────────────────────

/// `nbrs replay` — walk readout snapshots from a session db.
/// Walker-parsed: small, flat flag set fits the generic
/// walker cleanly, no need for raw_args.
pub fn spec() -> crate::cli_spec::Command {
    use crate::cli_spec::{Arity, Category, Command, Flag, Handler,
        Level, ParsedCommand, ValueProvider};
    fn handle(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = Vec::new();
        if p.bool("--plain") { argv.push("--plain".into()); }
        if p.bool("--errors") { argv.push("--errors".into()); }
        if p.bool("--json") { argv.push("--json".into()); }
        if let Some(v) = p.flag("--db") {
            argv.push("--db".into()); argv.push(v.into());
        }
        if let Some(v) = p.flag("--session") {
            argv.push("--session".into()); argv.push(v.into());
        }
        if let Some(v) = p.flag("--phase") {
            argv.push("--phase".into()); argv.push(v.into());
        }
        if let Some(v) = p.flag("--execution") {
            argv.push("--execution".into()); argv.push(v.into());
        }
        if p.bool("--all-executions") { argv.push("--all-executions".into()); }
        replay_command(&argv);
        Ok(())
    }
    Command {
        name: "replay",
        help: "Walk readout snapshots from a session db.",
        category: Category::Tools,
        level: Level::Secondary,
        flags: vec![
            Flag {
                long: "--db", short: None, aliases: &[],
                arity: Arity::Value, value: ValueProvider::Path,
                help: "Path to metrics.db.",
                repeatable: false,
            },
            Flag {
                long: "--session", short: None, aliases: &[],
                arity: Arity::Value,
                value: ValueProvider::Custom(crate::completion::session_name_provider),
                help: "SRD-04 session umbrella (path or name).",
                repeatable: false,
            },
            Flag {
                long: "--plain", short: None, aliases: &[],
                arity: Arity::Bool, value: ValueProvider::None,
                help: "Plain-text output (no ANSI).",
                repeatable: false,
            },
            Flag {
                long: "--errors", short: None, aliases: &[],
                arity: Arity::Bool, value: ValueProvider::None,
                help: "Only render phases that failed (SRD-76).",
                repeatable: false,
            },
            Flag {
                long: "--phase", short: None, aliases: &[],
                arity: Arity::Value,
                value: ValueProvider::Custom(crate::completion::phase_name_db_provider),
                help: "Filter to a single phase identity (SRD-76).",
                repeatable: false,
            },
            Flag {
                long: "--json", short: None, aliases: &[],
                arity: Arity::Bool, value: ValueProvider::None,
                help: "Machine-readable JSON per outcome (SRD-76).",
                repeatable: false,
            },
            // SRD-77 — execution selection. Default (no flag)
            // auto-picks the highest `exec_id` from the session's
            // outcomes; `--execution=<n>` targets one; the
            // bool `--all-executions` disables the filter
            // (sentinel `0` inside `replay_command`).
            Flag {
                long: "--execution", short: None, aliases: &[],
                arity: Arity::Value,
                value: ValueProvider::Custom(crate::completion::execution_id_provider),
                help: "Filter to one execution_id (default: most recent).",
                repeatable: false,
            },
            Flag {
                long: "--all-executions", short: None, aliases: &[],
                arity: Arity::Bool, value: ValueProvider::None,
                help: "Render every execution's outcomes.",
                repeatable: false,
            },
        ],
        kv_params: &[],
        dynamic_options: None,
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: false,
        completion_override: None,
    }
}
