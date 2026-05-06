// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

// Module-level allow: this file holds the legacy shell-completion
// builder API (`build_tree`, the `*_node` family, `Category` /
// `Level` enums duplicated in `cli_spec`). Retained per the doc
// comment on `build_tree`: "main.rs uses
// `cli_spec::completion::build_command_tree(&root)` instead, so
// the spec is the single source of truth." The orphan tree
// stays compileable as a fallback path for downstream tooling
// that hasn't migrated to the spec-driven builder yet.
#![allow(dead_code)]

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

use veks_completion::{CategoryTag, CommandTree, LevelTag, Node, StrictNode, fn_provider};

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
///
/// Retained for legacy callers; main.rs uses
/// `cli_spec::completion::build_command_tree(&root)` instead,
/// so the spec is the single source of truth.
pub fn build_tree() -> CommandTree {
    let root = crate::cli_spec::root::root();
    let tree = crate::cli_spec::completion::build_command_tree(&root);
    attach_global_value_providers(tree)
}

/// Attach the cross-leaf value providers (`workload=`,
/// `scenario=`, etc.) to a CommandTree. These aren't tied to
/// a specific Command — they fire whenever the tab cursor sits
/// on a matching token regardless of which leaf is active.
///
/// **Open gap:** the cli_spec model doesn't yet express
/// "tree-global" value providers. Workaround for now: this
/// function is called from main.rs after spec→tree
/// conversion. Future veks-completion enhancement could lift
/// global providers into the spec itself.
pub fn attach_global_value_providers(tree: CommandTree) -> CommandTree {
    tree
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
///
/// Wraps `veks_completion::handle_complete_env` with one
/// post-process: when the cursor sits on a flag that requires
/// a value (e.g. `--name`, `--metric`, …), advance past it and
/// run completion on the value position. This means
/// `nbrs plot ... --name<TAB>` produces the available plot
/// names instead of just echoing back `--name` — there's only
/// one possible continuation (a value), so we may as well
/// take it.
pub fn handle_complete_env(tree: &CommandTree) -> bool {
    let env_set = std::env::var("_NBRS_COMPLETE").ok().as_deref() == Some("bash")
        || std::env::var("COMPLETE").ok().as_deref() == Some("bash");
    if !env_set { return false; }

    let argv: Vec<String> = std::env::args().collect();
    let line = argv.get(1).cloned().unwrap_or_default();
    let point: usize = argv.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(line.len());
    let (prior, cur) = split_line_local(&line, point);

    // Specialised dispatch: `nbrs metrics match <pattern>`
    // taps into the dimensional-label cache rather than
    // veks's flag-list. Caught before the flag-takes-value
    // pre-process so the partial pattern isn't auto-advanced.
    if matches_metrics_match(&prior) {
        let db_path = match db_path_from_args(&prior) {
            Some(p) => p,
            None => std::path::PathBuf::from("logs/latest/metrics.db"),
        };
        if db_path.exists() {
            for c in crate::metrics_cache::match_completions(&cur, &db_path) {
                println!("{c}");
            }
        }
        return true;
    }

    let (eff_prior, eff_cur) = if flag_takes_value(&cur) {
        let mut p = prior.clone();
        p.push(cur.clone());
        (p, String::new())
    } else { (prior, cur) };

    let mut words_owned: Vec<String> = vec!["nbrs".to_string()];
    words_owned.extend(eff_prior);
    words_owned.push(eff_cur);
    let words: Vec<&str> = words_owned.iter().map(String::as_str).collect();

    for c in veks_completion::complete(tree, &words) {
        println!("{c}");
    }
    true
}

/// True when the prior tokens land at the positional pattern
/// of `nbrs metrics match`. Honours intervening `--db` /
/// `--session` flag pairs that the user might type before the
/// pattern; the test is "the last two non-flag-pair tokens
/// are `metrics` and `match`".
fn matches_metrics_match(prior: &[String]) -> bool {
    let tokens: Vec<&String> = strip_flag_value_pairs(prior);
    let n = tokens.len();
    n >= 2 && tokens[n - 2] == "metrics" && tokens[n - 1] == "match"
}

/// Remove flag/value pairs (e.g. `--db PATH`, `--session NAME`)
/// from a token list so positional-relative checks see only
/// the bare positional words.
fn strip_flag_value_pairs(tokens: &[String]) -> Vec<&String> {
    let mut out: Vec<&String> = Vec::new();
    let mut iter = tokens.iter().peekable();
    while let Some(t) = iter.next() {
        if flag_takes_value(t) {
            // Skip the value too — assumed to be the next
            // token (space-form). `=`-form flags are single
            // tokens and are dropped here as well.
            let _ = iter.next();
            continue;
        }
        if t.starts_with("--") {
            // Bare flag (no value) — drop and continue.
            continue;
        }
        out.push(t);
    }
    out
}

/// Pull `--db <path>` (space- or `=`-form) out of an arg list
/// so the metrics-match completer can target the right db.
/// Falls back to None — caller defaults to `logs/latest`.
fn db_path_from_args(args: &[String]) -> Option<std::path::PathBuf> {
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == "--db" {
            return iter.next().map(std::path::PathBuf::from);
        }
        if let Some(v) = a.strip_prefix("--db=") {
            return Some(std::path::PathBuf::from(v));
        }
    }
    None
}

/// Tokenize a shell line up to `point`, mirroring veks's
/// internal `split_line`: honors quotes + escapes, preserves
/// `=` as part of a token, drops the binary name.
fn split_line_local(line: &str, point: usize) -> (Vec<String>, String) {
    let point = point.min(line.len());
    let head = &line[..point];
    let mut words: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_quote: Option<char> = None;
    let mut chars = head.chars().peekable();
    while let Some(ch) = chars.next() {
        match in_quote {
            Some(q) if ch == q => { in_quote = None; }
            Some(_) => cur.push(ch),
            None => match ch {
                '\'' | '"' => { in_quote = Some(ch); }
                '\\' => { if let Some(n) = chars.next() { cur.push(n); } }
                ' ' | '\t' => {
                    if !cur.is_empty() {
                        words.push(std::mem::take(&mut cur));
                    }
                }
                _ => cur.push(ch),
            }
        }
    }
    if !words.is_empty() { words.remove(0); }
    (words, cur)
}

/// Flags whose grammar requires a value (the `--flag value` /
/// `--flag=value` shape). When the cursor sits on one of these
/// with no trailing whitespace, completion auto-advances to
/// value-position so the user gets one tab instead of two.
fn flag_takes_value(cur: &str) -> bool {
    matches!(cur,
        "--name" | "--metric" | "--x" | "--series" | "--filter"
        | "--db" | "--output" | "--label" | "--palette"
        | "--line" | "--line-width" | "--marker" | "--marker-size"
        | "--figure-num" | "--title" | "--xlabel" | "--ylabel"
        | "--xscale" | "--yscale" | "--width" | "--height"
        | "--csv-also" | "--report" | "--update-markdown"
        | "--add-to-markdown" | "--format" | "--create"
        | "--session" | "--session-name" | "--session-path"
        | "--session-reuse" | "--session-keep" | "--session-shelflife"
        | "--resume" | "--gk-lib" | "--pid" | "--socket"
    )
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

fn report_node() -> StrictNode<true, true> {
    // `nbrs report ...` — SRD-64 dispatch tree. Each kind
    // subcommand gets its own per-kind flag list sourced from
    // `nbrs_workload::report::vocab`, so completion for
    // `nbrs report plot --<TAB>` only offers flags applicable
    // to plots (axis flags, marker flags, etc.) and the same
    // for table / text / file / details.
    //
    // Per-flag value providers cover both closed sets
    // (palette / line / marker / agg / xscale / yscale) and
    // db-derived sets (--metric, --over, --by, --where).
    StrictNode::group(vec![
        ("plot",     kind_subcommand_node(nbrs_workload::report::Kind::Plot)),
        ("table",    kind_subcommand_node(nbrs_workload::report::Kind::Table)),
        ("text",     kind_subcommand_node(nbrs_workload::report::Kind::Text)),
        ("file",     kind_subcommand_node(nbrs_workload::report::Kind::File)),
        ("details",  kind_subcommand_node(nbrs_workload::report::Kind::Details)),
        ("list",     Node::leaf_with_flags(
            &["--db", "--session", "--workload"], &[])),
        ("all",      Node::leaf_with_flags(
            &["--db", "--session", "--workload"], &[])),
        ("show",     Node::leaf_with_flags(
            &["--db", "--session", "--workload"], &[])
            .with_value_provider("--name", fn_provider(report_any_name_provider))),
        ("figure",   Node::leaf_with_flags(
            &["--db", "--session", "--workload"], &[])),
        ("rename",   Node::leaf_with_flags(
            &["--workload", "--session", "--db"],
            &["--replace", "--dry-run"])),
        ("scratch",  Node::group(vec![
            ("list",    Node::leaf_with_flags(&["--session", "--db"], &[])),
            ("clean",   Node::leaf_with_flags(&["--session", "--db"], &[])),
            ("promote", Node::leaf_with_flags(&["--session", "--db", "--workload"], &[])),
        ])),
    ])
        .with_category(Category::Tools.tag())
        .with_level(Level::Secondary.rank())
}

/// Build the per-kind subcommand node from the SRD-64 vocab
/// registry. Every flag applicable to `kind` is exposed; closed-
/// set value providers attach for directives whose vocab entry
/// declares one. Db-derived providers (metric / label-key /
/// label-value-pair) re-use the existing
/// [`metric_provider`] / [`series_provider`] / [`filter_provider`]
/// plumbing.
pub(crate) fn kind_subcommand_node(kind: nbrs_workload::report::Kind) -> Node {
    use nbrs_workload::report::vocab::{self, ValueProvider};

    let flags: Vec<&'static str> = vocab::cli_flags_for(kind);
    // Orthogonal dispatch flags (not vocab-driven) — same set
    // the builder recognises in `report_build::Dispatch`.
    let mut all_value_flags: Vec<&'static str> = flags.clone();
    all_value_flags.extend([
        "--name", "--at", "--contextual", "--rename", "--group",
        "--workload", "--session", "--db", "--body", "--body-file",
    ]);
    let bool_flags: &[&str] = &[
        "--add", "--replace", "--stdout", "--ascii", "--dry-run",
    ];

    let mut node = Node::leaf_with_flags(&all_value_flags, bool_flags);

    // Per-vocab-flag value providers. `fn_provider` takes a
    // function pointer (not a closure), so we route each
    // closed set through a dedicated tiny `fn` rather than a
    // factory closure.
    for d in vocab::ALL_DIRECTIVES {
        if !d.applies_to.contains(kind) { continue; }
        match d.value {
            ValueProvider::Closed(_) => {
                if let Some(provider) = closed_set_provider_for(d.yaml_directive) {
                    node = node.with_value_provider(d.cli_flag, fn_provider(provider));
                }
            }
            ValueProvider::DbMetricNames => {
                node = node.with_value_provider(
                    d.cli_flag, fn_provider(metric_provider));
            }
            ValueProvider::DbLabelKeys => {
                node = node.with_value_provider(
                    d.cli_flag, fn_provider(series_provider));
            }
            ValueProvider::DbLabelKeyValuePairs => {
                node = node.with_value_provider(
                    d.cli_flag, fn_provider(filter_provider));
            }
            // Number / HexColor / Json / Text / Path:
            // suggestions don't help (free-form). Leave the
            // flag declared so the parser accepts it; the
            // user types the value freely.
            _ => {}
        }
    }

    // Orthogonal dispatch-flag providers.
    node = node
        .with_value_provider("--name", fn_provider(report_any_name_provider))
        .with_value_provider("--at", fn_provider(at_anchor_provider))
        .with_value_provider("--contextual", fn_provider(contextual_mode_provider));

    node
}

/// Map a vocab directive's `yaml_directive` keyword to the
/// matching closed-set provider fn-pointer. `None` means the
/// directive's value space isn't a closed set (handled by
/// the calling match arm).
fn closed_set_provider_for(yaml_directive: &str)
    -> Option<fn(&str, &[&str]) -> Vec<String>>
{
    match yaml_directive {
        "palette" => Some(palette_provider),
        "line"    => Some(line_styles_provider),
        "marker"  => Some(marker_shapes_provider),
        "agg"     => Some(agg_fns_provider),
        "xscale" | "yscale" => Some(axis_scales_provider),
        _ => None,
    }
}

fn palette_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    nbrs_workload::report::vocab::PALETTE_NAMES.iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string()).collect()
}

fn line_styles_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    nbrs_workload::report::vocab::LINE_STYLES.iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string()).collect()
}

fn marker_shapes_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    nbrs_workload::report::vocab::MARKER_SHAPES.iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string()).collect()
}

fn agg_fns_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    nbrs_workload::report::vocab::AGG_FNS.iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string()).collect()
}

fn axis_scales_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    nbrs_workload::report::vocab::AXIS_SCALES.iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string()).collect()
}

/// Closed set for `--at <scope>`: `root` plus the prefix forms
/// `scenario:`, `phase:`, `op:`. Past the prefix the value
/// space depends on the workload + active session, which is
/// out of scope for this surface — completion stops at the
/// prefix and the user types the name.
fn at_anchor_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    ["root", "scenario:", "phase:", "op:"].iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string())
        .collect()
}

/// Closed set for `--contextual <mode>`.
fn contextual_mode_provider(partial: &str, _ctx: &[&str]) -> Vec<String> {
    ["auto", "root", "scenario", "phase", "op"].iter()
        .filter(|s| s.starts_with(partial))
        .map(|s| s.to_string())
        .collect()
}

fn table_node() -> StrictNode<true, true> {
    // Unadvertised alias for `nbrs report table ...` (SRD-46).
    StrictNode::leaf_with_flags(
        &["--db", "--format", "--output", "--name"],
        &["--create"],
    )
        .with_value_provider("--name", fn_provider(summary_name_provider))
        .with_value_provider("workload=", fn_provider(workload_provider))
        .with_category(Category::Tools.tag())
        .with_level(Level::Secondary.rank())
}

fn gk_node() -> StrictNode<true, true> {
    // `nbrs gk visualize <expr|file.gk>`. Lone subcommand for
    // now; sibling slots (`gk functions`, `gk dag`) live under
    // `describe gk` until a broader gk-subcommand refactor.
    StrictNode::leaf_with_flags(&[], &[])
        .with_category(Category::Tools.tag())
        .with_level(Level::FullSurface.rank())
}

fn metrics_node() -> StrictNode<true, true> {
    // `nbrs metrics <list|show|match> [<expr>]` — read-side
    // introspection over the active session db. Flag lists are
    // sourced from `metrics_cmd` (LIST_FLAGS / MATCH_FLAGS) so
    // the parser and completion stay in lockstep — adding a
    // flag in one place is enough to surface it in tab.
    let list_flags  = crate::metrics_cmd::list_all_flags();
    let list_bools  = crate::metrics_cmd::LIST_BOOL_FLAGS;
    let match_flags = crate::metrics_cmd::match_all_flags();
    StrictNode::group(vec![
        ("list",  Node::leaf_with_flags(&list_flags,  list_bools)
            .with_value_provider("--format", fn_provider(static_metrics_format))),
        ("show",  Node::leaf_with_flags(&list_flags,  list_bools)
            .with_value_provider("--format", fn_provider(static_metrics_format))),
        ("match", Node::leaf_with_flags(&match_flags, &[])),
    ])
        .with_category(Category::Tools.tag())
        .with_level(Level::Secondary.rank())
}

/// Closed-set value provider for `nbrs metrics list/show
/// --format`. Sourced from `metrics_cmd::FORMAT_VALUES` so
/// adding a format keyword automatically appears in tab.
fn static_metrics_format(partial: &str, _ctx: &[&str]) -> Vec<String> {
    filter_prefix(crate::metrics_cmd::FORMAT_VALUES, partial)
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
        .with_value_provider("--metric", fn_provider(metric_provider))
        .with_value_provider("--series", fn_provider(series_provider))
        .with_value_provider("--x", fn_provider(series_provider))
        .with_value_provider("--filter", fn_provider(filter_provider))
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

/// SRD-46 cross-kind name provider for `nbrs report --name`:
/// emits every plot AND table item the workload defines (or
/// the session db has persisted). Kind-filtered providers stay
/// separate so `nbrs plot` / `nbrs table` aliases offer only
/// their own kind.
fn report_any_name_provider(partial: &str, ctx: &[&str]) -> Vec<String> {
    let mut all: Vec<String> = Vec::new();
    if let Some(path) = workload_from_context(ctx) {
        all.extend(crate::plot_metrics::list_workload_plot_names(&path));
        all.extend(crate::summary::list_workload_summary_names(&path));
    } else {
        let db_path = db_path_from_context(ctx);
        all.extend(crate::plot_metrics::list_stored_plot_names(&db_path));
        all.extend(crate::summary::list_stored_summary_names(&db_path));
        // Db-stored items are populated by the runner only for
        // sessions produced post-SRD-46-persistence-wiring.
        // For older sessions (or any session whose runner didn't
        // persist) `session_metadata.workload` still records the
        // workload's bare name — recover the declared items
        // from there.
        if all.is_empty()
            && let Some(yaml) = workload_path_from_session_db(&db_path)
        {
            all.extend(crate::plot_metrics::list_workload_plot_names(&yaml));
            all.extend(crate::summary::list_workload_summary_names(&yaml));
        }
    }
    all.retain(|n| n.starts_with(partial));
    all.sort();
    all.dedup();
    all
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
    let stored: Vec<String> = crate::summary::list_stored_summary_names(&db_path);
    if !stored.is_empty() {
        return stored.into_iter()
            .filter(|n| n.starts_with(partial))
            .collect();
    }
    // Db has no persisted summaries — fall back to the workload
    // recorded in `session_metadata.workload`. Same shape as
    // `report_any_name_provider`.
    if let Some(yaml) = workload_path_from_session_db(&db_path) {
        return crate::summary::list_workload_summary_names(&yaml)
            .into_iter()
            .filter(|n| n.starts_with(partial))
            .collect();
    }
    Vec::new()
}

/// Metric-family completion for `nbrs plot --metric`. Reads
/// the session db's `metric_family` table so the user gets the
/// closed vocabulary of metrics actually recorded in this
/// session (recall_at_10_mean, cycles_total, errors_total, …).
///
/// Honours `--db`, `--session-path`, and `--session` on the
/// line so the suggestions match wherever the eventual command
/// will read.
fn metric_provider(partial: &str, ctx: &[&str]) -> Vec<String> {
    let db_path = db_path_from_context(ctx);
    crate::plot_metrics::list_metric_families(&db_path)
        .into_iter()
        .filter(|n| n.starts_with(partial))
        .collect()
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
    let stored: Vec<String> = crate::plot_metrics::list_stored_plot_names(&db_path);
    if !stored.is_empty() {
        return stored.into_iter()
            .filter(|n| n.starts_with(partial))
            .collect();
    }
    // Fallback: when the session db doesn't carry persisted
    // plot specs (older runs, or a session that finished before
    // SRD-46 plot persistence wired up), look up the workload
    // YAML from `session_metadata.workload` and read its
    // `report:` block directly.
    if let Some(yaml) = workload_path_from_session_db(&db_path) {
        return crate::plot_metrics::list_workload_plot_names(&yaml)
            .into_iter()
            .filter(|n| n.starts_with(partial))
            .collect();
    }
    Vec::new()
}

/// Label-key completion for `nbrs plot --series` and `--x`.
///
/// Surfaces every distinct label key recorded in the metrics
/// db, narrowed to the metric family in scope when one can be
/// determined: `--metric <X>` wins, else `--name <X>` resolves
/// through the workload's `plot:` block or the db's stored
/// plots. Keys already present in `--x` / earlier `--series`
/// args are filtered out so suggestions move forward.
///
/// `--series` is comma-separated, so when the partial token
/// contains commas, the prefix part is preserved verbatim and
/// matching candidates are appended after the last comma.
fn series_provider(partial: &str, ctx: &[&str]) -> Vec<String> {
    let db_path = db_path_from_context(ctx);
    let workload_path = workload_from_context(ctx);
    let metric_pattern = metric_from_context(ctx, &db_path, workload_path.as_deref());

    let mut keys = crate::plot_metrics::list_label_keys(&db_path, metric_pattern.as_deref());

    let mut used: std::collections::HashSet<String> = used_label_keys(ctx)
        .into_iter().map(|s| s.to_string()).collect();
    let (head, tail) = match partial.rfind(',') {
        Some(i) => (&partial[..=i], &partial[i + 1..]),
        None => ("", partial),
    };
    for k in head.split(',') {
        let k = k.trim();
        if !k.is_empty() { used.insert(k.to_string()); }
    }
    keys.retain(|k| !used.contains(k));
    keys.into_iter()
        .filter(|k| k.starts_with(tail))
        .map(|k| format!("{head}{k}"))
        .collect()
}

/// `--filter <key>=<value>`: when the partial has no `=`, suggest
/// label keys followed by `=`. Once `=` is typed we let the user
/// supply the value freely (no enumeration — the value space is
/// arbitrary strings).
fn filter_provider(partial: &str, ctx: &[&str]) -> Vec<String> {
    if partial.contains('=') { return Vec::new(); }
    let db_path = db_path_from_context(ctx);
    let workload_path = workload_from_context(ctx);
    let metric_pattern = metric_from_context(ctx, &db_path, workload_path.as_deref());
    crate::plot_metrics::list_label_keys(&db_path, metric_pattern.as_deref())
        .into_iter()
        .filter(|k| k.starts_with(partial))
        .map(|k| format!("{k}="))
        .collect()
}

/// Find the metric family the user is plotting from `--metric`
/// or, failing that, the metric encoded in the named plot
/// referenced by `--name`.
fn metric_from_context(
    ctx: &[&str],
    db_path: &std::path::Path,
    workload_path: Option<&std::path::Path>,
) -> Option<String> {
    let mut iter = ctx.iter();
    while let Some(&w) = iter.next() {
        if w == "--metric" && let Some(&v) = iter.next() {
            return Some(v.to_string());
        }
        if let Some(v) = w.strip_prefix("--metric=") {
            return Some(v.to_string());
        }
    }
    let mut iter = ctx.iter();
    while let Some(&w) = iter.next() {
        if w == "--name" && let Some(&v) = iter.next() {
            return crate::plot_metrics::metric_for_plot_name(db_path, workload_path, v);
        }
        if let Some(v) = w.strip_prefix("--name=") {
            return crate::plot_metrics::metric_for_plot_name(db_path, workload_path, v);
        }
    }
    None
}

/// Collect label keys already pinned by `--x` and any prior
/// `--series` value(s) on the line. Comma-split because
/// `--series` accepts comma-separated lists.
fn used_label_keys<'a>(ctx: &'a [&'a str]) -> std::collections::HashSet<&'a str> {
    let mut out: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut iter = ctx.iter();
    while let Some(&w) = iter.next() {
        let val = if w == "--x" || w == "--series" {
            iter.next().copied()
        } else if let Some(v) = w.strip_prefix("--x=") {
            Some(v)
        } else if let Some(v) = w.strip_prefix("--series=") {
            Some(v)
        } else {
            None
        };
        if let Some(v) = val {
            for k in v.split(',') {
                let k = k.trim();
                if !k.is_empty() { out.insert(k); }
            }
        }
    }
    out
}

/// Pull `workload=<path>` from the in-progress command line, if
/// present. Resolves through the standard workload-path search
/// (`./<name>`, `<name>.yaml`, `workloads/<name>`, …) so the
/// completion provider sees the same file the command would.
fn workload_from_context(ctx: &[&str]) -> Option<std::path::PathBuf> {
    for word in ctx {
        if let Some(v) = word.strip_prefix("workload=") {
            // Three resolution shapes:
            //
            //   1. Direct path/name → `resolve_workload_path` →
            //      yaml file.
            //   2. Path to a session directory (`local/foo/`) →
            //      read `session_metadata.workload` from its
            //      `metrics.db` and resolve THAT name.
            //   3. Path to a metrics.db itself → same lookup.
            //
            // Shape (2)/(3) lets `workload=<session>` flow back
            // to the original yaml so completion / `nbrs report`
            // can find the declared plot/table names without
            // requiring the user to know where the yaml lives.
            if let Some(p) = crate::cli::resolve_workload_path(v) {
                let pb = std::path::PathBuf::from(p);
                if pb.exists() { return Some(pb); }
            }
            let candidate = std::path::PathBuf::from(v);
            if candidate.exists() {
                if candidate.is_file() {
                    return Some(candidate);
                }
                // Directory: try `<dir>/metrics.db`.
                let db = candidate.join("metrics.db");
                if db.exists()
                    && let Some(name) = workload_name_from_db(&db)
                    && let Some(yaml) = crate::cli::resolve_workload_path(&name)
                {
                    let p = std::path::PathBuf::from(yaml);
                    if p.exists() { return Some(p); }
                }
            } else if candidate.extension().is_none()
                && let Some(name) = workload_name_from_db(&candidate)
                && let Some(yaml) = crate::cli::resolve_workload_path(&name)
            {
                // Bare `workload=metrics.db`-style — try as-is.
                let p = std::path::PathBuf::from(yaml);
                if p.exists() { return Some(p); }
            }
        }
    }
    None
}

/// Read `session_metadata.workload` from a session db. The
/// runner records the bare workload name (no extension, no
/// path) so completion can map back to the declared yaml via
/// `resolve_workload_path`.
fn workload_name_from_db(db_path: &std::path::Path) -> Option<String> {
    if !db_path.exists() { return None; }
    let conn = rusqlite::Connection::open(db_path).ok()?;
    conn.query_row(
        "SELECT value FROM session_metadata WHERE key = 'workload' LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    ).ok()
}

/// Combine `workload_name_from_db` with `resolve_workload_path`
/// so a session db's recorded workload name flows back to the
/// declared yaml file. Returns `None` when either step fails
/// (e.g. db without metadata, or workload yaml has been moved
/// since the session ran).
fn workload_path_from_session_db(db_path: &std::path::Path) -> Option<std::path::PathBuf> {
    let name = workload_name_from_db(db_path)?;
    let yaml = crate::cli::resolve_workload_path(&name)?;
    let p = std::path::PathBuf::from(yaml);
    if p.exists() { Some(p) } else { None }
}

fn db_path_from_context(ctx: &[&str]) -> std::path::PathBuf {
    // `--db <path>` is the most explicit form — wins over any
    // session resolution.
    let mut iter = ctx.iter();
    while let Some(&w) = iter.next() {
        if w == "--db"
            && let Some(&v) = iter.next() {
            return std::path::PathBuf::from(v);
        }
        if let Some(v) = w.strip_prefix("--db=") {
            return std::path::PathBuf::from(v);
        }
    }
    // `--session` / `--session-path` / `--session-name` go
    // through the shared resolver so completion sees the same
    // db path the command itself will read. Single source of
    // truth for "what does --session mean".
    let owned: Vec<String> = ctx.iter().map(|s| s.to_string()).collect();
    if let Some(dir) = nbrs_activity::session::read_session_dir(&owned) {
        return dir.join("metrics.db");
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

/// Maximum directory depth the walker descends from each seed
/// root. Caps the cost of `workload=<TAB>` in deep trees.
const WORKLOAD_MAX_DEPTH: usize = 4;

/// Maximum number of directory entries the walker visits in
/// one completion call. Bounds the cost of a `<TAB>` press in
/// a large tree.
const WORKLOAD_MAX_FILES_SCANNED: usize = 1000;

/// Discover workload candidates for `workload=` tab-completion.
///
/// Recursively scans up to [`WORKLOAD_MAX_DEPTH`] levels deep
/// (or [`WORKLOAD_MAX_FILES_SCANNED`] entries, whichever comes
/// first) under each seed root, emitting every yaml file as a
/// full relative path so nested workloads surface without the
/// user having to tab through each level manually.
fn workload_file_candidates(cur: &str) -> Vec<String> {
    use std::path::Path;
    let mut out: Vec<String> = Vec::new();
    let mut budget = WORKLOAD_MAX_FILES_SCANNED;
    if cur.contains('/') {
        let split = cur.rfind('/').unwrap();
        let dir_prefix = &cur[..=split];
        let name_prefix = &cur[split + 1..];
        let seed = Path::new(dir_prefix.trim_end_matches('/'));
        collect_yaml_recursive(
            seed, dir_prefix, name_prefix, &mut out, &mut budget, 0,
        );
    } else {
        let roots: &[(&str, &str)] = &[
            (".", ""),
            ("workloads", "workloads/"),
            ("examples", "examples/"),
        ];
        for (dir, prefix) in roots {
            collect_yaml_recursive(
                Path::new(dir), prefix, cur, &mut out, &mut budget, 0,
            );
            if budget == 0 { break; }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Recursive walker. Emits every yaml file at depth ≤
/// [`WORKLOAD_MAX_DEPTH`] under `dir`. At depth 0 the leaf
/// filename is filtered by `name_prefix` (the user-typed
/// partial); deeper levels descend regardless so subdir-buried
/// workloads still surface by full relative path.
fn collect_yaml_recursive(
    dir: &std::path::Path,
    emit_prefix: &str,
    name_prefix: &str,
    out: &mut Vec<String>,
    budget: &mut usize,
    current_depth: usize,
) {
    if *budget == 0 { return; }
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        if *budget == 0 { return; }
        *budget -= 1;
        let Some(name_os) = entry.path().file_name().map(|n| n.to_owned()) else { continue };
        let name = name_os.to_string_lossy().to_string();
        if name.starts_with('.') { continue; }
        if current_depth == 0 && !name.starts_with(name_prefix) { continue; }
        let path = entry.path();
        if path.is_dir() {
            if matches!(name.as_str(), "target" | "node_modules" | "logs") { continue; }
            // Descend so files at depth N (N = WORKLOAD_MAX_DEPTH)
            // remain visible. The cap counts the deepest dir
            // entries we read, not the dir we recurse into.
            if current_depth < WORKLOAD_MAX_DEPTH {
                let child_prefix = format!("{emit_prefix}{name}/");
                collect_yaml_recursive(
                    &path, &child_prefix, "", out, budget, current_depth + 1,
                );
            }
            continue;
        }
        if let Some(ext) = path.extension()
            && (ext == "yaml" || ext == "yml") {
            out.push(format!("{emit_prefix}{name}"));
        }
    }
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

#[cfg(test)]
mod walker_tests {
    use super::*;

    fn tempdir(tag: &str) -> std::path::PathBuf {
        // /tmp deliberately, not env::temp_dir(): on some setups
        // (e.g. cargo test under TMPDIR=target/test-tmp) the env
        // path lives under `target/` which the walker
        // unconditionally skips, so a tempdir under it would
        // make the walker treat its own root as noise and find
        // nothing.
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let d = std::path::PathBuf::from("/tmp")
            .join(format!("nbrs-completion-{tag}-{n:x}"));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn write_yaml(path: &std::path::Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "#test\n").unwrap();
    }

    #[test]
    fn walker_finds_yaml_in_nested_subdirs() {
        let root = tempdir("nested");
        write_yaml(&root.join("a.yaml"));
        write_yaml(&root.join("sub/b.yaml"));
        write_yaml(&root.join("sub/deeper/c.yaml"));
        write_yaml(&root.join("sub/deeper/even/d.yaml"));

        let mut out = Vec::new();
        let mut budget = WORKLOAD_MAX_FILES_SCANNED;
        let prefix = format!("{}/", root.display());
        collect_yaml_recursive(&root, &prefix, "", &mut out, &mut budget, 0);
        assert!(out.iter().any(|p| p.ends_with("a.yaml")), "got: {out:?}");
        assert!(out.iter().any(|p| p.ends_with("sub/b.yaml")), "got: {out:?}");
        assert!(out.iter().any(|p| p.ends_with("sub/deeper/c.yaml")),
            "got: {out:?}");
        assert!(out.iter().any(|p| p.ends_with("sub/deeper/even/d.yaml")),
            "got: {out:?}");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn walker_stops_at_max_depth() {
        let root = tempdir("depth-cap");
        write_yaml(&root.join("L1/L2/L3/L4/inside4.yaml"));
        write_yaml(&root.join("L1/L2/L3/L4/L5/too_deep.yaml"));

        let mut out = Vec::new();
        let mut budget = WORKLOAD_MAX_FILES_SCANNED;
        let prefix = format!("{}/", root.display());
        collect_yaml_recursive(&root, &prefix, "", &mut out, &mut budget, 0);
        assert!(out.iter().any(|p| p.ends_with("L4/inside4.yaml")),
            "depth-4 entry visible: {out:?}");
        assert!(!out.iter().any(|p| p.ends_with("too_deep.yaml")),
            "depth-5 entry NOT visible: {out:?}");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn walker_respects_budget_cap() {
        let root = tempdir("budget");
        write_yaml(&root.join("a.yaml"));
        write_yaml(&root.join("sub/b.yaml"));

        let mut out = Vec::new();
        let mut budget: usize = 1;
        let prefix = format!("{}/", root.display());
        collect_yaml_recursive(&root, &prefix, "", &mut out, &mut budget, 0);
        assert_eq!(budget, 0, "budget should be exhausted");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn walker_skips_target_node_modules_logs() {
        let root = tempdir("noise-skip");
        write_yaml(&root.join("good.yaml"));
        write_yaml(&root.join("target/never.yaml"));
        write_yaml(&root.join("node_modules/never.yaml"));
        write_yaml(&root.join("logs/never.yaml"));

        let mut out = Vec::new();
        let mut budget = WORKLOAD_MAX_FILES_SCANNED;
        let prefix = format!("{}/", root.display());
        collect_yaml_recursive(&root, &prefix, "", &mut out, &mut budget, 0);
        assert!(out.iter().any(|p| p.ends_with("good.yaml")));
        assert!(!out.iter().any(|p| p.contains("target/")), "target/ skipped: {out:?}");
        assert!(!out.iter().any(|p| p.contains("node_modules/")), "node_modules/ skipped: {out:?}");
        assert!(!out.iter().any(|p| p.contains("/logs/")), "logs/ skipped: {out:?}");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn walker_filters_top_level_by_name_prefix() {
        let root = tempdir("name-prefix");
        write_yaml(&root.join("alpha.yaml"));
        write_yaml(&root.join("beta.yaml"));
        write_yaml(&root.join("sub/anything.yaml"));

        let mut out = Vec::new();
        let mut budget = WORKLOAD_MAX_FILES_SCANNED;
        let prefix = format!("{}/", root.display());
        collect_yaml_recursive(&root, &prefix, "alp", &mut out, &mut budget, 0);
        assert!(out.iter().any(|p| p.ends_with("alpha.yaml")), "got: {out:?}");
        assert!(!out.iter().any(|p| p.ends_with("beta.yaml")),
            "beta filtered: {out:?}");
        assert!(!out.iter().any(|p| p.ends_with("anything.yaml")),
            "non-matching subdir not descended: {out:?}");

        let _ = std::fs::remove_dir_all(&root);
    }

    // ── SRD-64 §4 — report completion: per-kind subtree contract ──
    //
    // Drives `complete_at_tap` against the published tree and
    // asserts each kind subcommand offers the right vocab flags.

    use veks_completion::complete_at_tap;

    fn complete(words: &[&str]) -> Vec<String> {
        let tree = build_tree();
        // Tap 3 = full surface — `report` is Tap 2, `report
        // <kind>` traversal needs the deepest tier.
        complete_at_tap(&tree, words, 3)
    }

    #[test]
    fn report_lists_all_subcommands() {
        // Only subcommands the parser actually accepts. The
        // pre-cli_spec completion tree advertised `show`,
        // `text`, `file`, `details` too — but `report_command`
        // never handled them as keywords (they fell through to
        // glob-match and errored). Cleaned up so completion
        // doesn't lie about the surface.
        let cands = complete(&["nbrs", "report", ""]);
        for required in ["plot", "table", "list", "all", "figure",
                         "rename", "scratch"]
        {
            assert!(cands.iter().any(|c| c == required),
                "missing subcommand `{required}` in: {cands:?}");
        }
    }

    #[test]
    fn report_plot_offers_plot_directives() {
        let cands = complete(&["nbrs", "report", "plot", "demo", "--"]);
        for required in ["--over", "--by", "--where", "--agg",
                         "--label", "--palette", "--line",
                         "--width", "--marker", "--size",
                         "--color", "--metric",
                         "--xlabel", "--ylabel", "--xscale", "--yscale",
                         "--series",
                         // Orthogonal dispatch flags.
                         "--add", "--at", "--contextual", "--replace",
                         "--rename", "--group", "--workload", "--dry-run",
                         "--name"]
        {
            assert!(cands.iter().any(|c| c == required),
                "missing flag `{required}` in plot completions: {cands:?}");
        }
    }

    #[test]
    fn report_table_excludes_plot_only_directives() {
        let cands = complete(&["nbrs", "report", "table", "demo", "--"]);
        for forbidden in ["--xlabel", "--ylabel", "--xscale", "--yscale",
                          "--marker", "--line", "--width", "--size"]
        {
            assert!(!cands.iter().any(|c| c == forbidden),
                "table completions should not offer `{forbidden}` (plot-only): {cands:?}");
        }
        // Data-shape directives still apply to tables.
        for required in ["--over", "--by", "--where", "--agg",
                         "--metric", "--label", "--palette", "--color"]
        {
            assert!(cands.iter().any(|c| c == required),
                "table completions missing `{required}`: {cands:?}");
        }
    }

    #[test]
    #[ignore = "nbrs report text/file/details are not currently \
                accepted by the parser; the completion tree no \
                longer advertises them. Re-enable when the SRD-64 \
                flag-form path is extended to non-figure kinds."]
    fn report_text_excludes_figure_directives() {
        let cands = complete(&["nbrs", "report", "text", "intro", "--"]);
        for forbidden in ["--over", "--by", "--where", "--agg",
                          "--metric", "--xscale", "--yscale", "--marker"]
        {
            assert!(!cands.iter().any(|c| c == forbidden),
                "text completions should not offer `{forbidden}`: {cands:?}");
        }
        for required in ["--label", "--body", "--body-file"] {
            assert!(cands.iter().any(|c| c == required),
                "text completions missing `{required}`: {cands:?}");
        }
    }

    #[test]
    fn palette_value_completion_offers_closed_set() {
        let cands = complete(&["nbrs", "report", "plot", "demo",
                               "--palette", ""]);
        for required in nbrs_workload::report::vocab::PALETTE_NAMES {
            assert!(cands.iter().any(|c| c == required),
                "palette completion missing `{required}`: {cands:?}");
        }
        // Sanity: should NOT offer arbitrary strings.
        assert!(!cands.iter().any(|c| c == "nope"),
            "completion shouldn't offer arbitrary values: {cands:?}");
    }

    #[test]
    fn agg_value_completion_offers_closed_set() {
        let cands = complete(&["nbrs", "report", "plot", "demo",
                               "--agg", ""]);
        for required in nbrs_workload::report::vocab::AGG_FNS {
            assert!(cands.iter().any(|c| c == required),
                "agg completion missing `{required}`: {cands:?}");
        }
    }

    #[test]
    fn xscale_value_completion_offers_linear_log() {
        let cands = complete(&["nbrs", "report", "plot", "demo",
                               "--xscale", ""]);
        assert!(cands.iter().any(|c| c == "linear"));
        assert!(cands.iter().any(|c| c == "log"));
    }

    #[test]
    fn marker_value_completion_offers_shape_set() {
        let cands = complete(&["nbrs", "report", "plot", "demo",
                               "--marker", ""]);
        for required in ["circle", "square", "triangle", "diamond",
                         "plus", "cross", "none"]
        {
            assert!(cands.iter().any(|c| c == required),
                "marker completion missing `{required}`: {cands:?}");
        }
    }

    #[test]
    fn at_anchor_completion_offers_scope_prefixes() {
        let cands = complete(&["nbrs", "report", "plot", "demo", "--at", ""]);
        for required in ["root", "scenario:", "phase:", "op:"] {
            assert!(cands.iter().any(|c| c == required),
                "--at completion missing `{required}`: {cands:?}");
        }
    }

    #[test]
    fn contextual_completion_offers_all_modes() {
        let cands = complete(&["nbrs", "report", "plot", "demo",
                               "--contextual", ""]);
        for required in ["auto", "root", "scenario", "phase", "op"] {
            assert!(cands.iter().any(|c| c == required),
                "--contextual completion missing `{required}`: {cands:?}");
        }
    }

    #[test]
    fn report_scratch_subcommands_listed() {
        let cands = complete(&["nbrs", "report", "scratch", ""]);
        for required in ["list", "clean", "promote"] {
            assert!(cands.iter().any(|c| c == required),
                "scratch subcommand `{required}` missing: {cands:?}");
        }
    }

    #[test]
    fn report_rename_offers_replace_and_dry_run() {
        let cands = complete(&["nbrs", "report", "rename", "old_name", "new_name", "--"]);
        for required in ["--replace", "--dry-run", "--workload"] {
            assert!(cands.iter().any(|c| c == required),
                "rename completion missing `{required}`: {cands:?}");
        }
    }

    #[test]
    fn closed_set_filters_by_partial() {
        // `--palette w` should narrow to palettes starting with `w`.
        let cands = complete(&["nbrs", "report", "plot", "demo",
                               "--palette", "w"]);
        assert!(cands.iter().any(|c| c == "wong"));
        // Other palettes filtered.
        assert!(!cands.iter().any(|c| c == "ibm"),
            "filter should remove non-matching: {cands:?}");
    }
}

// ── cli_spec entry for `nbrs completions` ─────────────────

/// `nbrs completions [--shell <name>]` — emit the bash/zsh
/// completion shim or the activation eval line. Walker-parsed.
pub fn spec() -> crate::cli_spec::Command {
    use crate::cli_spec::{Arity, Category, Command, Flag, Handler,
        Level, ParsedCommand, ValueProvider};
    fn shells(p: &str, _: &[&str]) -> Vec<String> {
        ["bash","zsh","fish","elvish","powershell"].iter()
            .filter(|s| s.starts_with(p))
            .map(|s| s.to_string()).collect()
    }
    fn handle(p: ParsedCommand) -> Result<(), String> {
        let mut argv: Vec<String> = Vec::new();
        if let Some(v) = p.flag("--shell") {
            argv.push("--shell".into());
            argv.push(v.into());
        }
        super::completion::print_completions(&argv);
        Ok(())
    }
    Command {
        name: "completions",
        help: "Print shell-completion shim or activation line.",
        category: Category::Shell,
        level: Level::FullSurface,
        flags: vec![Flag {
            long: "--shell", short: None, aliases: &[],
            arity: Arity::Value, value: ValueProvider::Custom(shells),
            help: "bash | zsh | fish | elvish | powershell. Omit for activation line.",
            repeatable: false,
        }],
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: false,
        completion_override: None,
    }
}
