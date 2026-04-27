// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Shared shell-completion support for persona binaries.
//!
//! Every persona (nbrs, cassnbrs, …) accepts the same core shape on
//! the command line — `<binary> run <param>=<value> …` — so the
//! interesting completion candidates (workload files, scenario names,
//! adapter names, known param keys) are identical across personas.
//! This module owns that logic so each persona's `main` only needs
//! a five-line wire-up: pick a binary name, pick a list of subcommands,
//! pick a param list, delegate.
//!
//! ## Usage
//!
//! Each persona calls [`handle_if_match`] at the top of `main`:
//!
//! ```ignore
//! let spec = nb_activity::completions::CompletionSpec {
//!     binary_name: "cassnbrs",
//!     subcommands: &["run", "completions"],
//!     run_params: nb_activity::runner::KNOWN_PARAMS,
//! };
//! if nb_activity::completions::handle_if_match(&spec, &args) {
//!     return;
//! }
//! ```
//!
//! This handles both the user-facing `completions` subcommand
//! (prints a bash `source <(...)` preamble or the completion script
//! itself) and a hidden `__complete` subcommand the emitted bash
//! function reinvokes the binary through for each tab press.
//!
//! ## Where the work happens
//!
//! All parsing lives in the binary. The bash shim is deliberately
//! small — it hands the raw `$COMP_LINE` and `$COMP_POINT` to
//! `<bin> __complete` and sets `COMPREPLY` from whatever comes back.
//! That means the only thing that can drift between shell and binary
//! is the handoff interface, not the completion rules themselves.
//!
//! ## What gets completed
//!
//! - **Top-level words** — candidates from `spec.subcommands`.
//! - `workload=<TAB>` — yaml files under `./`, `./workloads/`, and
//!   `./examples/`.
//! - `scenario=<TAB>` — scenario names parsed from the workload file
//!   that appears earlier in the same command line.
//! - `adapter=<TAB>` / `driver=<TAB>` — names registered via
//!   [`crate::adapter::registered_driver_names`].
//! - `profiler=` / `tui=` / `dryrun=` — fixed option sets.
//! - Anything else after `run` — keys from `spec.run_params` with `=`
//!   appended so tab doesn't insert a space and the user can keep
//!   typing the value.

use std::path::Path;

/// Per-binary completion configuration. Pass to [`handle_if_match`].
pub struct CompletionSpec {
    /// Binary basename as invoked on the command line (e.g., `"cassnbrs"`).
    /// Used to name the generated bash function and `complete -F` target.
    pub binary_name: &'static str,
    /// Top-level subcommands offered when the user hits TAB on the
    /// first word. Include `"completions"` so it self-lists.
    pub subcommands: &'static [&'static str],
    /// Param keys offered after the `run` subcommand. Each is emitted
    /// with a trailing `=` so the reader stays on the current token.
    pub run_params: &'static [&'static str],
}

/// Top-level dispatch. Returns `true` if the args were consumed by a
/// completion subcommand (caller should exit 0), `false` if the caller
/// should fall through to its own arg parsing.
pub fn handle_if_match(spec: &CompletionSpec, args: &[String]) -> bool {
    match args.first().map(|s| s.as_str()) {
        Some("completions") => {
            run_completions(spec, &args[1..]);
            true
        }
        Some("__complete") => {
            run_complete_helper(spec, &args[1..]);
            true
        }
        _ => false,
    }
}

fn run_completions(spec: &CompletionSpec, args: &[String]) {
    let shell = args.iter()
        .position(|a| a == "--shell")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("");
    if shell == "bash" {
        emit_bash_script(spec);
    } else {
        emit_preamble(spec);
    }
}

fn emit_preamble(spec: &CompletionSpec) {
    let bin = spec.binary_name;
    let exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.into_os_string().into_string().ok())
        .unwrap_or_else(|| bin.to_string());
    println!("# {bin} tab-completion for bash");
    println!("# To activate:  eval \"$({bin} completions)\"");
    println!("# To persist:   echo 'eval \"$({bin} completions)\"' >> ~/.bashrc");
    println!("source <(\"{exe}\" completions --shell bash)");
}

fn emit_bash_script(spec: &CompletionSpec) {
    // The shim is intentionally tiny: forward $COMP_LINE and
    // $COMP_POINT to the binary and splice the newline-separated
    // response into COMPREPLY. All word-parsing and candidate logic
    // runs in Rust — the user-facing behavior can't drift without a
    // binary change.
    //
    // The `complete` directive omits `-o default` so that when the
    // binary returns no candidates bash does NOT silently fall back
    // to filename completion (which leaks unrelated files like
    // `results.db` into a `workload=<TAB>` reply).
    let fname = format!("_{}_complete", spec.binary_name);
    let bin = spec.binary_name;
    let script = format!(r#"{fname}() {{
    local IFS=$'\n'
    COMPREPLY=($({bin} __complete "$COMP_LINE" "$COMP_POINT" 2>/dev/null))
    if [[ ${{#COMPREPLY[@]}} -ge 1 ]] \
        && [[ "${{COMPREPLY[0]}}" == *= || "${{COMPREPLY[0]}}" == */ ]]; then
        compopt -o nospace 2>/dev/null
    fi
}}
complete -F {fname} {bin}
"#);
    print!("{script}");
}

/// Hidden helper: `<binary> __complete "$COMP_LINE" "$COMP_POINT"`.
/// Emits newline-separated candidates on stdout. Never writes to
/// stderr (a stray warning would appear in the user's shell mid-tab).
fn run_complete_helper(spec: &CompletionSpec, args: &[String]) {
    let line = args.first().cloned().unwrap_or_default();
    let point: usize = args.get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(line.len());
    // Clamp `point` to `line.len()` — if the shell sent something
    // weird we'd rather complete at the end than panic.
    let point = point.min(line.len());
    let (prior, cur) = split_line(&line[..point]);

    for c in compute_candidates(spec, &cur, &prior) {
        println!("{c}");
    }
}

/// Walk the shell input up to the cursor, producing the "prior"
/// tokens and the current (in-progress) token. Honors single and
/// double quotes and preserves `=` as part of a token. The first
/// token (the binary name) is stripped before returning — callers
/// don't need to know their own name.
fn split_line(line: &str) -> (Vec<String>, String) {
    let mut words: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_quote: Option<char> = None;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match in_quote {
            Some(q) if ch == q => { in_quote = None; }
            Some(_) => cur.push(ch),
            None => match ch {
                '\'' | '"' => { in_quote = Some(ch); }
                '\\' => {
                    // Escape — take the next char literally.
                    if let Some(next) = chars.next() { cur.push(next); }
                }
                ' ' | '\t' => {
                    if !cur.is_empty() {
                        words.push(std::mem::take(&mut cur));
                    }
                }
                _ => cur.push(ch),
            }
        }
    }
    // `cur` now holds the final (possibly empty) token — the one
    // under the cursor. Drop the binary name from `words`.
    if !words.is_empty() {
        words.remove(0);
    }
    (words, cur)
}

fn compute_candidates(spec: &CompletionSpec, cur: &str, prior: &[String]) -> Vec<String> {
    // First positional word: subcommand.
    if prior.is_empty() {
        return filter_prefix(spec.subcommands, cur);
    }

    // `key=value` value-side completions.
    //
    // Candidates are emitted WITHOUT the `key=` prefix. Bash's default
    // `COMP_WORDBREAKS` includes `=`, so readline treats the current
    // word as just the part after the `=` sign. Emitting `key=value`
    // would cause bash to replace "" (post-`=`) with `key=value`,
    // producing `key=key=value`. Emitting bare `value` reads
    // correctly: bash splices it in after the `=` that's already on
    // the line.
    if let Some(value) = cur.strip_prefix("workload=") {
        return workload_file_candidates(value);
    }
    if let Some(value) = cur.strip_prefix("scenario=") {
        return scenario_candidates(value, prior);
    }
    if let Some(value) = cur.strip_prefix("adapter=") {
        return adapter_candidates(value);
    }
    if let Some(value) = cur.strip_prefix("driver=") {
        return adapter_candidates(value);
    }
    if let Some(value) = cur.strip_prefix("profiler=") {
        return filter_prefix(&["off", "flamegraph", "perf"], value);
    }
    if let Some(value) = cur.strip_prefix("tui=") {
        return filter_prefix(&["on", "off"], value);
    }
    if let Some(value) = cur.strip_prefix("dryrun=") {
        return filter_prefix(&["phase", "cycle", "full", "gk", "labels"], value);
    }

    // Generic param-name suggestion. The trailing `=` is deliberate
    // and stays put: the cursor is before any `=`, so bash's current
    // word is the whole key. Replacing it with `key=` works. The
    // emitted bash script detects the `=` suffix and flips on
    // `compopt -o nospace` so the user can type the value immediately.
    let first_prior = prior.first().map(|s| s.as_str()).unwrap_or("");
    if first_prior == "run" {
        let mut names: Vec<String> = spec.run_params.iter()
            .map(|k| format!("{k}="))
            .filter(|s| s.starts_with(cur))
            .collect();
        names.sort();
        names.dedup();
        return names;
    }

    Vec::new()
}

fn filter_prefix<S: AsRef<str>>(opts: &[S], cur: &str) -> Vec<String> {
    opts.iter()
        .map(|s| s.as_ref().to_string())
        .filter(|s| s.starts_with(cur))
        .collect()
}

/// Discover workload candidates for `workload=` tab-completion.
///
/// When the user has typed a path-with-slash, descend into the
/// deepest real directory covered by that prefix and list its
/// immediate children (yaml files + yaml-bearing subdirs). When the
/// user has typed only a bare prefix (no slash), seed from three
/// conventional roots — `./`, `workloads/`, `examples/` — so
/// `workload=<TAB>` works without forcing them to type a directory
/// name first.
fn workload_file_candidates(cur: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if cur.contains('/') {
        // Descend: `examples/workloads/dy` → dir `examples/workloads`,
        // name-prefix `dy`, emit-prefix `examples/workloads/`.
        let split = cur.rfind('/').unwrap();
        let dir_prefix = &cur[..=split]; // includes trailing `/`
        let name_prefix = &cur[split + 1..];
        collect_yaml_entries(Path::new(dir_prefix.trim_end_matches('/')),
            dir_prefix, name_prefix, &mut out);
    } else {
        // Seed roots: the user's still at the top level, so show yaml
        // files and yaml-bearing dirs from each search root.
        let roots: &[(&str, &str)] = &[
            (".", ""),
            ("workloads", "workloads/"),
            ("examples", "examples/"),
        ];
        for (dir, prefix) in roots {
            collect_yaml_entries(Path::new(dir), prefix, cur, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

/// List one directory's immediate children, emitting yaml files
/// directly and yaml-bearing subdirectories with a trailing `/`.
/// Each emitted candidate is prepended with `emit_prefix` so the
/// reader sees a full path relative to the shell's cwd. Entries are
/// filtered by `name_prefix` (the partial filename under the cursor).
fn collect_yaml_entries(dir: &Path, emit_prefix: &str, name_prefix: &str, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let Some(name_os) = entry.path().file_name().map(|n| n.to_owned()) else { continue };
        let name = name_os.to_string_lossy().to_string();
        // Hide dotfiles — the user can still type `.` explicitly to
        // see them if they really need to.
        if name.starts_with('.') { continue; }
        if !name.starts_with(name_prefix) { continue; }
        let path = entry.path();
        if path.is_dir() {
            // Skip noisy build/output dirs that definitely don't hold
            // workload files.
            if matches!(name.as_str(), "target" | "node_modules" | "logs") { continue; }
            if directory_contains_yaml(&path) {
                out.push(format!("{emit_prefix}{name}/"));
            }
            continue;
        }
        if let Some(ext) = path.extension()
            && (ext == "yaml" || ext == "yml") {
            out.push(format!("{emit_prefix}{name}"));
        }
    }
}

/// True if `dir` (checked one level deep only) has any yaml file,
/// directly or in any immediate subdirectory. One-level-deep is
/// enough for completion: the user will tab again to descend.
fn directory_contains_yaml(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else { return false };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file()
            && let Some(ext) = path.extension()
            && (ext == "yaml" || ext == "yml") {
            return true;
        }
        if path.is_dir() {
            let Ok(inner) = std::fs::read_dir(&path) else { continue };
            for sub in inner.flatten() {
                let sub_path = sub.path();
                if sub_path.is_file()
                    && let Some(ext) = sub_path.extension()
                    && (ext == "yaml" || ext == "yml") {
                    return true;
                }
            }
        }
    }
    false
}

/// Parse scenario names from the workload file that appears earlier
/// in the same command line. Accepts `workload=<path>` or a bare
/// `<path>.yaml` positional.
fn scenario_candidates(cur: &str, prior: &[String]) -> Vec<String> {
    let workload = prior.iter().find_map(|w| {
        if let Some(v) = w.strip_prefix("workload=") {
            Some(v.to_string())
        } else if w.ends_with(".yaml") || w.ends_with(".yml") {
            Some(w.clone())
        } else {
            None
        }
    });
    let Some(name) = workload else { return Vec::new(); };
    let Some(path) = crate::runner::resolve_workload_file_public(&name) else { return Vec::new(); };
    let mut scenarios = crate::runner::scenarios_in_workload_file(&path);
    scenarios.retain(|s| s.starts_with(cur));
    scenarios.sort();
    scenarios
}

fn adapter_candidates(cur: &str) -> Vec<String> {
    let mut names: Vec<String> = crate::adapter::registered_driver_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    names.retain(|n| n.starts_with(cur));
    names.sort();
    names
}
