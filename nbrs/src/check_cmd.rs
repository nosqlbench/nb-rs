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
        help: "Run a workload and verify its output against the `#@` / `verify:`\n\
               rules it declares. Accepts a file, a directory (checks every\n\
               workload under it), or a bundled catalog name — the same\n\
               reference `nbrs run` takes. Non-zero exit on failure.",
        category: Category::Workloads,
        level: Level::Workload,
        flags: Vec::new(),
        kv_params: &[],
        dynamic_options: None,
        positionals: vec![crate::cli_spec::Positional {
            name: "workload",
            help: "Workload file, directory, or bundled catalog name to verify \
                   (or `workload=<ref>`).",
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
    // Target from `workload=<ref>` or the first bare positional — a file, a
    // directory, or a bundled catalog name, resolved by the verifier exactly
    // the way `nbrs run` resolves `workload=…`.
    let target = args
        .iter()
        .find_map(|a| a.strip_prefix("workload=").map(String::from))
        .or_else(|| args.iter().find(|a| !a.contains('=')).cloned())
        .ok_or("usage: nbrs check workload=<file|dir|name>  (or: nbrs check <file|dir|name>)")?;

    let binary = std::env::current_exe()
        .map_err(|e| format!("cannot locate the nbrs binary: {e}"))?;
    let sandbox = std::env::temp_dir().join(format!("nbrs-check-{}", std::process::id()));

    let sum = nbrs_workload::verify::verify_target(&binary, &target, &sandbox);

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
