// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Stratified shell completion for the `nbrs` CLI, built on
//! [`veks_completion`].
//!
//! ## Tap progression
//!
//! Three tap tiers, gated by per-command metadata (see SRD-15 if
//! ever written; for now the contract is encoded in this file).
//!
//! - **Tap 1** — primary commands the user reaches for daily:
//!   `run` (start a workload) and `attach` (connect to a
//!   running one over the OOB socket).
//! - **Tap 2** — adds secondary commands (`summary`).
//! - **Tap 3** — full surface (subcommands like `describe`,
//!   `bench`, `plot`, `web`, `completions`).
//!
//! Categories are a closed set defined by the [`Category`] enum
//! (which implements [`veks_completion::CategoryTag`]); tap tiers
//! are likewise a closed set defined by the [`Level`] enum
//! (implementing [`veks_completion::LevelTag`]). Renderers can
//! group commands by `Category::tag()` and order by
//! `Level::rank()`.
//!
//! The tree is built in [`build_tree`] using
//! [`veks_completion::CommandTree::strict_command`], which
//! requires every node to declare both a category and a level
//! at the **type** level — undertagged commands fail to compile.

use veks_completion::{CategoryTag, CommandTree, LevelTag, StrictNode, fn_provider};

use nbrs_activity::adapter::registered_driver_names;
use nbrs_activity::runner::{
    KNOWN_PARAMS, resolve_workload_file_public, scenarios_in_workload_file,
};

// ---------------------------------------------------------------------------
// Categories — closed enum implementing veks_completion::CategoryTag so the
// set of valid categories is defined once and the compiler enforces variants
// rather than a scattered constellation of `&str` constants. Renderers can
// group commands by `tag()` (the stable lowercase key).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub enum Category {
    Workloads,
    Tools,
    Documentation,
    Benchmark,
    Server,
    Shell,
}

impl CategoryTag for Category {
    fn tag(&self) -> &'static str {
        match self {
            Category::Workloads => "workloads",
            Category::Tools => "tools",
            Category::Documentation => "documentation",
            Category::Benchmark => "benchmark",
            Category::Server => "server",
            Category::Shell => "shell",
        }
    }
}

// ---------------------------------------------------------------------------
// Tap tiers — closed enum implementing veks_completion::LevelTag. The Nth
// tab tap reveals every root command with `rank() <= N`. Naming the tiers
// keeps build_tree() self-describing instead of bare 1/2/3 sprinkled through.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub enum Level {
    /// Tap 1 — primary commands the user reaches for daily
    /// (`run`, `shell`).
    Workload,
    /// Tap 2 — secondary commands (`summary`).
    Secondary,
    /// Tap 3 — the full subcommand surface (describe, bench,
    /// plot, …).
    FullSurface,
}

impl LevelTag for Level {
    fn rank(&self) -> u32 {
        match self {
            Level::Workload => 1,
            Level::Secondary => 2,
            Level::FullSurface => 3,
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Level::Workload => "workload",
            Level::Secondary => "secondary",
            Level::FullSurface => "full-surface",
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Build the full nbrs completion tree. Strictly typed —
/// every command must declare both a category and a tap level
/// or the build fails to compile (`StrictNode<true, true>`
/// gating on `strict_command`).
pub fn build_tree() -> CommandTree {
    CommandTree::new("nbrs")
        // ---- tap 1: primary commands ----
        .strict_command("run", run_node())
        .strict_command("attach", attach_node())
        // ---- tap 2: secondary commands ----
        .strict_command("summary", summary_node())
        // ---- tap 3: less-frequent subcommands ----
        .strict_command("describe", describe_node())
        .strict_command("bench", bench_node())
        .strict_command("plot", plot_node())
        .strict_command("web", web_node())
        .strict_command("completions", completions_node())
        // OpenAPI commands are gated behind a feature; register
        // them only when built in. Keeping them at level 3
        // matches their discoverability (subcommands users
        // graduate into, not the first thing they reach for).
        .with_openapi_commands()
        // ---- value providers shared across leaves ----
        .global_value_provider("workload=", fn_provider(workload_provider))
        .global_value_provider("scenario=", fn_provider(scenario_provider))
        .global_value_provider("adapter=", fn_provider(adapter_provider))
        .global_value_provider("driver=", fn_provider(adapter_provider))
        .global_value_provider("profiler=", fn_provider(static_profiler))
        .global_value_provider("tui=", fn_provider(static_tui))
        .global_value_provider("dryrun=", fn_provider(static_dryrun))
        .global_value_provider("--socket", fn_provider(socket_path_provider))
        .global_value_provider("--pid", fn_provider(pid_provider))
}

/// Handle the `completions` subcommand:
///
/// - `nbrs completions` (no args) — print one `source <(...)`
///   activation line on **stdout** and explanatory comments on
///   **stderr**. Splitting streams matters: with comments on
///   stdout, `` eval `nbrs completions` `` collapses newlines
///   via word-splitting and the leading `#` makes the whole
///   joined line a comment that does nothing. Stderr is
///   visible standalone but invisible to substitution, so
///   every common eval form works:
///   `eval "$(nbrs completions)"`, `eval $(nbrs completions)`,
///   `` eval `nbrs completions` ``.
/// - `nbrs completions --shell bash` — emit the raw bash shim
///   that registers the binary as the completer. This is what
///   the activation line's `source <(... --shell bash)` pulls
///   in.
pub fn print_completions(args: &[String]) {
    let shell = args.iter()
        .position(|a| a == "--shell")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str());
    match shell {
        Some(name) => {
            let s = match name {
                "bash" => veks_completion::Shell::Bash,
                "zsh" => veks_completion::Shell::Zsh,
                "fish" => veks_completion::Shell::Fish,
                "elvish" => veks_completion::Shell::Elvish,
                "powershell" => veks_completion::Shell::PowerShell,
                other => {
                    eprintln!("nbrs: unknown shell '{other}' (try bash, zsh, fish, elvish, powershell)");
                    return;
                }
            };
            veks_completion::print_completions("nbrs", s);
        }
        None => {
            print_activation_line();
        }
    }
}

/// Resolve the path the user invoked us as, so the bash shim's
/// `source <(...)` re-invocation reaches the same binary.
fn current_exe_path() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.into_os_string().into_string().ok())
        .unwrap_or_else(|| "nbrs".to_string())
}

fn print_activation_line() {
    let exe = current_exe_path();
    eprintln!("# nbrs tab-completion for bash");
    eprintln!("# To activate:  eval \"$(nbrs completions)\"");
    eprintln!("# To persist:   echo 'eval \"$(nbrs completions)\"' >> ~/.bashrc");
    println!("source <(\"{exe}\" completions --shell bash)");
}

/// Handle the bash-side completion callback (`_NBRS_COMPLETE=bash`).
/// Returns `true` if the env var was set and candidates were
/// emitted — the caller should exit immediately.
pub fn handle_complete_env(tree: &CommandTree) -> bool {
    veks_completion::handle_complete_env("nbrs", tree)
}

// ---------------------------------------------------------------------------
// Per-command nodes
// ---------------------------------------------------------------------------

/// Helper: build the leaf-level option list for `run`-shaped
/// commands. Includes every workload param the runner knows
/// about (with `=` suffixed to keep the cursor on the same
/// token) plus a couple of bare flags.
fn run_options() -> StrictNode<false, false> {
    let mut opts: Vec<String> = KNOWN_PARAMS.iter()
        .map(|k| format!("{k}="))
        .collect();
    opts.sort();
    opts.dedup();
    let opts_refs: Vec<&str> = opts.iter().map(|s| s.as_str()).collect();
    StrictNode::leaf_with_flags(&opts_refs, &["--strict"])
        .with_dynamic_options(workload_dynamic_params)
}

fn run_node() -> StrictNode<true, true> {
    run_options()
        .with_category(Category::Workloads.tag())
        .with_level(Level::Workload.rank())
}

fn attach_node() -> StrictNode<true, true> {
    StrictNode::leaf_with_flags(
        &["--pid", "--socket", "-c", "--command", "tui=on", "tui=off"],
        &["--no-tui"],
    )
        .with_category(Category::Shell.tag())
        .with_level(Level::Workload.rank())
}

fn summary_node() -> StrictNode<true, true> {
    StrictNode::leaf_with_flags(
        &["--db", "--format", "--output", "--name"],
        &["--create"],
    )
        .with_value_provider("--name", fn_provider(summary_name_provider))
        // `workload=<file.yaml>` sources named summaries from
        // the YAML's `summary:` block instead of the metrics
        // db. Same provider as `nbrs run workload=`.
        .with_value_provider("workload=", fn_provider(workload_provider))
        .with_category(Category::Tools.tag())
        .with_level(Level::Secondary.rank())
}

fn describe_node() -> StrictNode<true, true> {
    StrictNode::leaf(&[])
        .with_category(Category::Documentation.tag())
        .with_level(Level::FullSurface.rank())
}

fn bench_node() -> StrictNode<true, true> {
    StrictNode::leaf(&[])
        .with_category(Category::Benchmark.tag())
        .with_level(Level::FullSurface.rank())
}

fn plot_node() -> StrictNode<true, true> {
    StrictNode::leaf_with_flags(
        &[
            "--db", "--output", "--metric", "--x", "--series",
            "--filter", "--agg", "--name", "--title", "--xlabel",
            "--ylabel", "--xscale", "--yscale", "--width", "--height",
            "--csv-also",
        ],
        &["--verbose"],
    )
        .with_value_provider("--name", fn_provider(plot_name_provider))
        // `workload=<file.yaml>` sources named plots from the
        // YAML's `plot:` block instead of the metrics db.
        .with_value_provider("workload=", fn_provider(workload_provider))
        .with_category(Category::Tools.tag())
        // Same tier as `summary` — both are post-hoc analysis
        // tools over the metrics db, both replay stored named
        // specs by `--name`. Surfacing them at the same TAB
        // level keeps the UX symmetrical.
        .with_level(Level::Secondary.rank())
}

fn web_node() -> StrictNode<true, true> {
    StrictNode::leaf(&[])
        .with_category(Category::Server.tag())
        .with_level(Level::FullSurface.rank())
}

fn completions_node() -> StrictNode<true, true> {
    StrictNode::leaf(&["--shell"])
        .with_category(Category::Shell.tag())
        .with_level(Level::FullSurface.rank())
}

// ---------------------------------------------------------------------------
// Value providers (hoisted from nbrs-activity::completions)
// ---------------------------------------------------------------------------

fn workload_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    workload_file_candidates(partial)
}

fn scenario_provider(partial: &str, ctx: &[&str]) -> Vec<String> {
    let context_strings: Vec<String> = ctx.iter().map(|s| s.to_string()).collect();
    scenario_candidates(partial, &context_strings)
}

fn adapter_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    let mut names: Vec<String> = registered_driver_names()
        .into_iter()
        .map(|s| s.to_string())
        .filter(|n| n.starts_with(partial))
        .collect();
    names.sort();
    names
}

fn static_profiler(partial: &str, _ctx: &[&str]) -> Vec<String> {
    filter_prefix(&["off", "flamegraph", "perf"], partial)
}

fn static_tui(partial: &str, _ctx: &[&str]) -> Vec<String> {
    filter_prefix(&["on", "off"], partial)
}

fn static_dryrun(partial: &str, _ctx: &[&str]) -> Vec<String> {
    filter_prefix(&["phase", "cycle", "full", "gk", "labels"], partial)
}

/// Inspector socket discovery — same logic as the legacy
/// `nbrs-activity::completions::socket_path_candidates`.
fn socket_path_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    let dir = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
    let read = match std::fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<String> = Vec::new();
    for entry in read.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
        if !(name.starts_with("nbrs-") && name.ends_with(".sock")) { continue; }
        let full = path.to_string_lossy().into_owned();
        if full.starts_with(partial) { out.push(full); }
    }
    out.sort();
    out
}

fn pid_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    let dir = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
    let read = match std::fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<String> = Vec::new();
    for entry in read.flatten() {
        let Some(name) = entry.path().file_name().and_then(|s| s.to_str().map(str::to_string))
            else { continue };
        let Some(rest) = name.strip_prefix("nbrs-") else { continue };
        let Some(pid_str) = rest.strip_suffix(".sock") else { continue };
        if pid_str.parse::<u32>().is_ok() && pid_str.starts_with(partial) {
            out.push(pid_str.to_string());
        }
    }
    out.sort();
    out
}

/// Stored-summary-name completion for `nbrs summary --name`.
/// `workload=<path>` on the line wins (sources from the
/// workload's `summary:` block). Otherwise falls back to the
/// metrics db's `session_metadata` table.
fn summary_name_provider(partial: &str, ctx: &[&str]) -> Vec<String> {
    if let Some(path) = workload_from_context(ctx) {
        return crate::summary::list_workload_summary_names(&path)
            .into_iter()
            .filter(|n| n.starts_with(partial))
            .collect();
    }
    let db_path = db_path_from_context(ctx);
    let names: Vec<String> = crate::summary::list_stored_summary_names(&db_path)
        .into_iter()
        .filter(|n| n.starts_with(partial))
        .collect();
    names
}

/// Stored-plot-name completion for `nbrs plot --name`. Same
/// rules as `summary_name_provider`: `workload=<path>` overrides
/// db lookup.
fn plot_name_provider(partial: &str, ctx: &[&str]) -> Vec<String> {
    if let Some(path) = workload_from_context(ctx) {
        return crate::plot_metrics::list_workload_plot_names(&path)
            .into_iter()
            .filter(|n| n.starts_with(partial))
            .collect();
    }
    let db_path = db_path_from_context(ctx);
    crate::plot_metrics::list_stored_plot_names(&db_path)
        .into_iter()
        .filter(|n| n.starts_with(partial))
        .collect()
}

/// Pull `workload=<path>` from the in-progress command line, if
/// present. Resolves through the standard workload-path search
/// (`./<name>`, `<name>.yaml`, `workloads/<name>`, …) so the
/// completion provider sees the same file the command would.
fn workload_from_context(ctx: &[&str]) -> Option<std::path::PathBuf> {
    for word in ctx {
        if let Some(v) = word.strip_prefix("workload=") {
            let resolved = crate::cli::resolve_workload_path(v)
                .unwrap_or_else(|| v.to_string());
            let p = std::path::PathBuf::from(resolved);
            if p.exists() {
                return Some(p);
            }
        }
    }
    None
}

fn db_path_from_context(ctx: &[&str]) -> std::path::PathBuf {
    let mut iter = ctx.iter();
    while let Some(&w) = iter.next() {
        if w == "--db" {
            if let Some(&v) = iter.next() {
                return std::path::PathBuf::from(v);
            }
        }
        if let Some(v) = w.strip_prefix("--db=") {
            return std::path::PathBuf::from(v);
        }
    }
    std::path::PathBuf::from("logs/latest/metrics.db")
}

/// Dynamic option discovery: when a `workload=…` is on the
/// line, parse the workload file and surface its declared
/// `params:` keys as completion targets.
fn workload_dynamic_params(_partial: &str, ctx: &[&str]) -> Vec<String> {
    let mut workload_path: Option<String> = None;
    for word in ctx {
        if let Some(p) = word.strip_prefix("workload=") {
            workload_path = Some(p.to_string());
            break;
        }
        if word.ends_with(".yaml") || word.ends_with(".yml") {
            workload_path = Some((*word).to_string());
            break;
        }
    }
    let Some(name) = workload_path else { return Vec::new(); };
    let Some(path) = resolve_workload_file_public(&name) else { return Vec::new(); };
    let Ok(yaml) = std::fs::read_to_string(&path) else { return Vec::new(); };
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&yaml) else {
        return Vec::new();
    };
    let Some(params) = doc.get("params").and_then(|v| v.as_mapping()) else {
        return Vec::new();
    };
    params.keys()
        .filter_map(|k| k.as_str().map(|s| format!("{s}=")))
        .collect()
}

// ---------------------------------------------------------------------------
// Workload-file / scenario name discovery (hoisted from
// nbrs-activity::completions)
// ---------------------------------------------------------------------------

fn workload_file_candidates(cur: &str) -> Vec<String> {
    use std::path::Path;
    let mut out: Vec<String> = Vec::new();
    if cur.contains('/') {
        let split = cur.rfind('/').unwrap();
        let dir_prefix = &cur[..=split];
        let name_prefix = &cur[split + 1..];
        collect_yaml_entries(Path::new(dir_prefix.trim_end_matches('/')),
            dir_prefix, name_prefix, &mut out);
    } else {
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

fn collect_yaml_entries(
    dir: &std::path::Path,
    emit_prefix: &str,
    name_prefix: &str,
    out: &mut Vec<String>,
) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let Some(name_os) = entry.path().file_name().map(|n| n.to_owned()) else { continue };
        let name = name_os.to_string_lossy().to_string();
        if name.starts_with('.') { continue; }
        if !name.starts_with(name_prefix) { continue; }
        let path = entry.path();
        if path.is_dir() {
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

fn directory_contains_yaml(dir: &std::path::Path) -> bool {
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
    let Some(path) = resolve_workload_file_public(&name) else { return Vec::new(); };
    let mut scenarios = scenarios_in_workload_file(&path);
    scenarios.retain(|s| s.starts_with(cur));
    scenarios.sort();
    scenarios
}

fn filter_prefix(opts: &[&str], cur: &str) -> Vec<String> {
    opts.iter()
        .filter(|s| s.starts_with(cur))
        .map(|s| s.to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// Feature-gated branches: extends CommandTree with optional
// commands without violating the strict-typestate gate.
// ---------------------------------------------------------------------------

trait CommandTreeExt {
    fn with_openapi_commands(self) -> Self;
}

impl CommandTreeExt for CommandTree {
    #[cfg(feature = "openapi")]
    fn with_openapi_commands(self) -> Self {
        self.strict_command("describe-openapi",
            StrictNode::leaf(&[])
                .with_category(Category::Documentation.tag())
                .with_level(Level::FullSurface.rank()))
            .strict_command("run-openapi",
                StrictNode::leaf(&[])
                    .with_category(Category::Workloads.tag())
                    .with_level(Level::FullSurface.rank()))
    }

    #[cfg(not(feature = "openapi"))]
    fn with_openapi_commands(self) -> Self { self }
}
