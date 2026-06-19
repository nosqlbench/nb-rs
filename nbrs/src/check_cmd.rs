// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The `check` subcommand: run a workload — or every workload under a directory
//! — and verify its output against the rules it declares (`#@` comments or a
//! `verify:` block). Exits non-zero on any failure, so it drops into CI. The
//! verification logic lives in [`nbrs_workload::verify`], shared with the
//! example-walker test, so users check their own workloads exactly the way CI
//! checks the bundled examples.

use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};

pub fn spec() -> Command {
    Command {
        name: "check",
        help: "Run a workload (or directory) and verify its output against the\n\
               `#@` / `verify:` rules it declares. Non-zero exit on failure.",
        category: Category::Workloads,
        level: Level::Workload,
        flags: Vec::new(),
        kv_params: &[],
        dynamic_options: None,
        positionals: vec![crate::cli_spec::Positional {
            name: "workload",
            help: "Workload file or directory to verify (or `workload=<path>`).",
            kind: crate::cli_spec::PositionalKind::ZeroOrOne,
            value: crate::cli_spec::ValueProvider::Custom(
                crate::completion::workload_positional_provider,
            ),
        }],
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: true,
        completion_override: None,
    }
}

fn handle(p: ParsedCommand) -> Result<(), String> {
    check_command(&p.raw)
}

fn check_command(args: &[String]) -> Result<(), String> {
    // Path from `workload=<path>` or the first bare positional.
    let raw_path = args
        .iter()
        .find_map(|a| a.strip_prefix("workload=").map(String::from))
        .or_else(|| args.iter().find(|a| !a.contains('=')).cloned())
        .ok_or("usage: nbrs check workload=<file|dir>  (or: nbrs check <file|dir>)")?;

    // Canonicalise — the verifier runs each workload from a sandbox cwd, so the
    // path it hands to `nbrs run` must be absolute.
    let path = std::path::Path::new(&raw_path)
        .canonicalize()
        .map_err(|e| format!("no such workload path '{raw_path}': {e}"))?;

    let binary = std::env::current_exe()
        .map_err(|e| format!("cannot locate the nbrs binary: {e}"))?;
    let sandbox = std::env::temp_dir().join(format!("nbrs-check-{}", std::process::id()));

    let sum = nbrs_workload::verify::verify_path(&binary, &path, &sandbox);

    for s in &sum.skipped {
        println!("  ⃠  skip  {s}");
    }
    let total = sum.passed + sum.failures.len();
    if sum.failures.is_empty() {
        let skip = if sum.skipped.is_empty() {
            String::new()
        } else {
            format!(", {} skipped", sum.skipped.len())
        };
        println!("✓ {} check{} passed{skip}", sum.passed, if sum.passed == 1 { "" } else { "s" });
        Ok(())
    } else {
        for f in &sum.failures {
            eprintln!("  ✗  {f}");
        }
        Err(format!("{} of {total} checks failed", sum.failures.len()))
    }
}
