// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `nbrs copy <name> [to=<path>]` — SRD-85 materialization.
//!
//! Copies a bundled workload's source to a local file for
//! editing. The copy is stamped with a provenance header
//! (catalog name + nbrs version) so a diverged local copy can
//! always be traced to its origin — for humans and support
//! tooling, not a sync mechanism. Refuses to overwrite.
//!
//! The lighter-weight alternative is SRD-72 `extends:` — a
//! local child declaring `extends: <catalog-name>` inherits the
//! bundled parent without forking it.

/// Handle `nbrs copy <name> [to=<path>]`.
pub fn copy_command(args: &[String]) -> Result<(), String> {
    let name = args
        .iter()
        .find(|a| !a.starts_with('-') && !a.contains('='))
        .ok_or_else(|| {
            "usage: nbrs copy <bundled-workload-name> [to=<path>]\n\
             `nbrs describe workloads` lists what this binary carries."
                .to_string()
        })?;

    let bundled = nbrs_workload::catalog::lookup(name).ok_or_else(|| {
        format!(
            "no bundled workload named `{name}` — `nbrs describe workloads` \
             (or `--all` for the examples tier) lists the catalog"
        )
    })?;

    let dest: std::path::PathBuf = args
        .iter()
        .find_map(|a| a.strip_prefix("to="))
        .map(Into::into)
        .unwrap_or_else(|| {
            // Default: basename of the catalog name, flattened
            // (`cql/keyvalue` → `cql_keyvalue.yaml`) so the copy
            // lands in the cwd without surprise directories.
            std::path::PathBuf::from(format!("{}.yaml", bundled.name.replace('/', "_")))
        });

    if dest.exists() {
        return Err(format!(
            "refusing to overwrite existing file {} — pass `to=<path>` to \
             choose another destination",
            dest.display()
        ));
    }

    let provenance = format!(
        "# Copied from bundled workload `{}` (nbrs {}).\n\
         # This copy is yours — edits here never sync back. To inherit the\n\
         # bundled parent instead of forking it, use `extends: {}`.\n",
        bundled.name,
        env!("CARGO_PKG_VERSION"),
        bundled.name,
    );
    std::fs::write(&dest, format!("{provenance}{}", bundled.source))
        .map_err(|e| format!("write {}: {e}", dest.display()))?;
    println!("copied bundled workload `{}` to {}", bundled.name, dest.display());
    Ok(())
}

pub fn spec() -> crate::cli_spec::Command {
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    fn handle(p: ParsedCommand) -> Result<(), String> {
        copy_command(&p.raw)
    }
    Command {
        name: "copy",
        help: "Copy a bundled workload to a local file for editing (provenance-stamped).",
        category: Category::Documentation,
        level: Level::FullSurface,
        flags: Vec::new(),
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: true,
        completion_override: None,
    }
}
