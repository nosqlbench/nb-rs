// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `nbrs replay` — write the readout snapshots from a
//! session db to stdout, reproducing the operator-visible
//! status / DONE lines from a finished run.
//!
//! See SRD-63 §6 for the snapshot store contract. The
//! snapshot table is populated during the run by
//! `nbrs-activity::readouts::snapshot::capture` at every
//! `binder.fire()` call site; this command is the read
//! side.
//!
//! Push 6 ships the minimal CLI: walks the table in stable
//! order and writes the latest render per tuple to stdout.
//! No interactive UI, no filtering — that's a follow-on if
//! demand surfaces. Usage:
//!
//! ```text
//! nbrs replay                          # default: logs/latest/metrics.db
//! nbrs replay --session=logs/foo       # explicit session dir
//! nbrs replay --plain                  # strip ANSI styling
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
}

fn parse_args(args: &[String]) -> Result<Opts, String> {
    let mut db_path: Option<PathBuf> = None;
    let mut plain = false;
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a == "--plain" {
            plain = true;
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
        } else if a == "-h" || a == "--help" {
            print_usage();
            std::process::exit(0);
        } else {
            return Err(format!("unexpected arg '{a}' (use --session=<dir> or --db=<path>)"));
        }
        i += 1;
    }
    let db_path = db_path.unwrap_or_else(|| PathBuf::from("logs/latest/metrics.db"));
    if !db_path.exists() {
        return Err(format!(
            "session db not found at '{}' — pass --session=<dir> or --db=<path>",
            db_path.display(),
        ));
    }
    Ok(Opts { db_path, plain })
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
        let candidate = PathBuf::from("logs").join(session_arg).join("metrics.db");
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
    eprintln!("Walks the session's readout-snapshot store and prints");
    eprintln!("the latest render of each (slot, subject, readout) tuple.");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --session <dir>  Session directory (default: logs/latest)");
    eprintln!("  --db <path>      Direct path to metrics.db");
    eprintln!("  --plain          Strip ANSI styling from output");
    eprintln!("  -h, --help       Show this message");
}

fn run(opts: Opts) -> Result<(), String> {
    let reporter = nbrs_metrics::reporters::sqlite::SqliteReporter::new(&opts.db_path)
        .map_err(|e| format!("opening {}: {e}", opts.db_path.display()))?;
    let rows = reporter.read_readout_snapshots();
    if rows.is_empty() {
        eprintln!(
            "nbrs replay: no snapshots in '{}' (was readouts capture enabled during the run?)",
            opts.db_path.display(),
        );
        return Ok(());
    }
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for row in rows {
        let body = if opts.plain {
            row.body_plain
        } else {
            row.body_ansi
                .as_deref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or(row.body_plain)
        };
        // Each snapshot is one line of rendered output;
        // newline separates entries so stdout reads as a
        // chronological log.
        writeln!(out, "{body}").map_err(|e| e.to_string())?;
    }
    Ok(())
}
