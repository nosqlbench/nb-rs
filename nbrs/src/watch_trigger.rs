// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `watch=...` CLI-flag triggers — subprocess-based plot /
//! report re-renderers that fire after every phase
//! completion.
//!
//! Architecture: each `watch=<spec>` registers a
//! [`nbrs_runtime::phase_end_triggers::PhaseEndTrigger`]
//! that spawns `nbrs report ...` (or `nbrs plot ...`) as a
//! detached subprocess and waits for it. The subprocess runs
//! against the live session db, so its output reflects the
//! state immediately after the most recent phase. The
//! triggers run on the registry's worker thread so a slow
//! re-render doesn't block the executor.
//!
//! Supported specs:
//!
//! - `report`            → `nbrs report all --session <S>`
//! - `report:<args>`     → `nbrs report <args> --session <S>`
//! - `plot`              → `nbrs plot all --session <S>`
//! - `plot:<name>`       → `nbrs plot --name <name> --session <S>`
//!
//! `<S>` is the active run's session directory (resolved at
//! registration time from the same logic as
//! `nbrs_runtime::session::read_session_dir`). If
//! resolution fails the trigger registration is skipped with
//! a warning — the run continues uninterrupted.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use nbrs_runtime::phase_end_triggers::{
    PhaseEndEvent, PhaseEndTrigger,
};

/// A trigger that runs a child `nbrs` subprocess with a
/// fixed argv after every phase end. The first element of
/// `argv` is the subcommand (e.g. `report`), `--session
/// <dir>` is appended automatically.
struct SubprocessTrigger {
    label: String,
    nbrs_binary: PathBuf,
    argv: Vec<String>,
    session_dir: PathBuf,
}

impl PhaseEndTrigger for SubprocessTrigger {
    fn name(&self) -> &str { &self.label }
    fn fire(&self, _event: &PhaseEndEvent) {
        let mut cmd = Command::new(&self.nbrs_binary);
        cmd.args(&self.argv)
           .arg("--session")
           .arg(&self.session_dir);
        // Suppress child stdout (the report's user-facing
        // text is the side-effect of generated files; pumping
        // its text through the parent's stderr would clutter
        // the run log). Surface stderr so a failed render
        // still leaves a breadcrumb.
        cmd.stdout(std::process::Stdio::null());
        match cmd.status() {
            Ok(s) if s.success() => {}
            Ok(s) => {
                nbrs_runtime::diag!(
                    nbrs_runtime::observer::LogLevel::Warn,
                    "watch trigger '{label}' exited with status {s}",
                    label = self.label,
                );
            }
            Err(e) => {
                nbrs_runtime::diag!(
                    nbrs_runtime::observer::LogLevel::Warn,
                    "watch trigger '{label}' failed to spawn: {e}",
                    label = self.label,
                );
            }
        }
    }
}

/// Register every `watch=<spec>` listed in `specs`. Returns
/// the list of registered trigger ids so the caller can
/// unregister them on run cleanup if desired. (Triggers
/// outliving the run are usually fine — the worker thread
/// holds no state beyond the trigger list, and the registry
/// is process-global.)
pub fn register_watch_triggers(specs: &[String])
    -> Vec<nbrs_runtime::phase_end_triggers::TriggerId>
{
    let mut ids = Vec::new();
    let session_dir = match resolve_session_dir() {
        Some(p) => p,
        None => {
            if !specs.is_empty() {
                eprintln!(
                    "watch: cannot resolve session directory — \
                     triggers disabled (requested: {})",
                    specs.join(", "),
                );
            }
            return ids;
        }
    };
    let nbrs_binary = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("watch: cannot resolve self path: {e} — \
                       triggers disabled");
            return ids;
        }
    };
    for spec in specs {
        match spec_to_argv(spec) {
            Some(argv) => {
                let trigger = Arc::new(SubprocessTrigger {
                    label: format!("watch:{spec}"),
                    nbrs_binary: nbrs_binary.clone(),
                    argv,
                    session_dir: session_dir.clone(),
                });
                let id = nbrs_runtime::phase_end_triggers::register(trigger);
                ids.push(id);
            }
            None => {
                eprintln!("watch: unknown spec '{spec}' — \
                           accepted forms: report | report:<args> | \
                           plot | plot:<name>");
            }
        }
    }
    ids
}

/// Translate one `watch=<spec>` value into the `nbrs`
/// subcommand argv (without the `--session <dir>` tail —
/// that's appended at fire time).
fn spec_to_argv(spec: &str) -> Option<Vec<String>> {
    let trimmed = spec.trim();
    if trimmed == "report" {
        return Some(vec!["report".into(), "all".into()]);
    }
    if let Some(args_tail) = trimmed.strip_prefix("report:") {
        let mut argv = vec!["report".into()];
        argv.extend(args_tail.split_whitespace().map(String::from));
        return Some(argv);
    }
    if trimmed == "plot" {
        return Some(vec!["plot".into(), "all".into()]);
    }
    if let Some(name) = trimmed.strip_prefix("plot:") {
        return Some(vec![
            "plot".into(),
            "--name".into(),
            name.trim().into(),
        ]);
    }
    None
}

/// Look up the current session directory using the same
/// resolver `nbrs report` uses. Reads from the live latest-
/// session symlink if no `--session` was specified on the
/// command line. Returns `None` when neither path is set.
fn resolve_session_dir() -> Option<PathBuf> {
    let argv: Vec<String> = std::env::args().collect();
    nbrs_runtime::session::read_session_dir(&argv).or_else(|| {
        let p = nbrs_runtime::session::latest_session_dir();
        if p.exists() { Some(p) } else { None }
    })
}

/// Comma-split the `watch=<a>,<b>,...` CLI param into one
/// spec per element. Empty / whitespace-only entries are
/// dropped so `watch=report,` doesn't register a phantom
/// trigger.
pub fn split_watch_param(value: &str) -> Vec<String> {
    value.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_report_default() {
        assert_eq!(spec_to_argv("report").unwrap(),
            vec!["report".to_string(), "all".to_string()]);
    }

    #[test]
    fn spec_report_with_args() {
        assert_eq!(
            spec_to_argv("report:fmt=html").unwrap(),
            vec!["report".to_string(), "fmt=html".to_string()],
        );
    }

    #[test]
    fn spec_plot_default() {
        assert_eq!(spec_to_argv("plot").unwrap(),
            vec!["plot".to_string(), "all".to_string()]);
    }

    #[test]
    fn spec_plot_named() {
        assert_eq!(
            spec_to_argv("plot:throughput").unwrap(),
            vec!["plot".to_string(), "--name".to_string(),
                 "throughput".to_string()],
        );
    }

    #[test]
    fn spec_unknown_returns_none() {
        assert!(spec_to_argv("garbage").is_none());
        assert!(spec_to_argv("").is_none());
    }

    #[test]
    fn split_watch_skips_empty_segments() {
        assert_eq!(
            split_watch_param("report,,plot:throughput, "),
            vec!["report".to_string(), "plot:throughput".to_string()],
        );
    }
}
