// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The `run` subcommand: execute a workload against an adapter.

use std::collections::HashMap;
use std::sync::Arc;

use nb_activity::activity::{Activity, ActivityConfig};
use nb_activity::bindings::compile_bindings_with_libs_excluding;
use nb_activity::opseq::{OpSequence, SequencerType};
use nb_activity::synthesis::OpBuilder;
use nb_adapter_stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
use nb_metrics::labels::Labels;
use nb_tui::app::App;
use nb_tui::reporter::TuiReporter;
use nb_workload::parse::parse_workload;
use nb_workload::tags::TagFilter;

use crate::daemon;
use crate::web_push;

/// Single source of truth for all known `key=value` params accepted
/// by `nbrs run`. Used for completion, param validation, and "did
/// you mean?" suggestions.
///
/// **DO NOT duplicate this list.** If you need to check whether a
/// param is known, reference `KNOWN_PARAMS`. If you need completion
/// candidates, call `run_completion()`. Both derive from this array.
pub const KNOWN_PARAMS: &[&str] = &[
    // Activity-level
    "adapter", "driver", "workload", "op", "cycles", "threads",
    "rate", "stanzarate", "errors", "seq", "tags", "format",
    "filename", "separator", "header", "color",
    "stanza_concurrency", "sc", "scenario",
    // CQL adapter
    "hosts", "host", "port", "keyspace", "consistency",
    "username", "password", "request_timeout_ms",
    // HTTP adapter
    "base_url", "timeout",
];

/// Single source of truth for all known flags and prefixed options.
pub const KNOWN_FLAGS: &[&str] = &[
    "--strict", "--dry-run", "--tui", "--diagnose", "--color",
    "--dry-run=emit", "--dry-run=json",
    "--gk-lib=", "--report-openmetrics-to=",
];

/// Completion candidates for `run`, derived from the canonical param
/// lists above. No separate lists to maintain.
pub fn run_completion() -> (&'static [&'static str], &'static [&'static str]) {
    static OPTIONS: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
        let mut opts: Vec<&str> = KNOWN_PARAMS.iter()
            .map(|p| {
                let s = format!("{p}=");
                &*s.leak()
            })
            .collect();
        for f in KNOWN_FLAGS {
            if f.ends_with('=') { opts.push(f); }
        }
        opts
    });
    static FLAGS: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
        KNOWN_FLAGS.iter().filter(|f| !f.ends_with('=')).copied().collect()
    });
    (OPTIONS.as_slice(), FLAGS.as_slice())
}

/// Find the closest match to `input` from `candidates` using
/// Levenshtein distance. Returns None if the best match is too
/// distant (more than 40% of the input length).
fn closest_match<'a>(input: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let mut best: Option<(&str, usize)> = None;
    for &candidate in candidates {
        let d = levenshtein(input, candidate);
        if best.is_none() || d < best.unwrap().1 {
            best = Some((candidate, d));
        }
    }
    best.filter(|(_, d)| *d <= (input.len() / 2).max(2))
        .map(|(s, _)| s)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());
    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];
    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

/// Parse `key=value` pairs from command line args.
pub fn parse_params(args: &[String]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for arg in args {
        if let Some(eq_pos) = arg.find('=') {
            let key = arg[..eq_pos].to_string();
            let value = arg[eq_pos + 1..].to_string();
            params.insert(key, value);
        }
    }
    params
}

/// Parse a cycle count that may have suffixes: K, M, B.
pub fn parse_count(s: &str) -> Option<u64> {
    let s = s.trim().to_uppercase();
    if let Some(n) = s.strip_suffix('K') {
        n.trim().parse::<u64>().ok().map(|v| v * 1_000)
    } else if let Some(n) = s.strip_suffix('M') {
        n.trim().parse::<u64>().ok().map(|v| v * 1_000_000)
    } else if let Some(n) = s.strip_suffix('B') {
        n.trim().parse::<u64>().ok().map(|v| v * 1_000_000_000)
    } else {
        s.parse().ok()
    }
}

pub async fn run_command(args: &[String]) {
    // Skip "run" if present
    let args = if args.first().map(|s| s.as_str()) == Some("run") {
        &args[1..]
    } else {
        args
    };

    let params = parse_params(args);

    // Load workload — from inline op= or YAML file.
    let mut workload_file: Option<String> = None;
    let workload = if let Some(op_str) = params.get("op") {
        if params.contains_key("workload") {
            eprintln!("nbrs: warning: op= overrides workload=");
        }
        match nb_workload::inline::synthesize_inline_workload(op_str) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("error: failed to synthesize inline workload: {e}");
                std::process::exit(1);
            }
        }
    } else {
        let workload_path = params.get("workload")
            .or_else(|| {
                // Look for a .yaml file in the args
                args.iter().find(|a| a.ends_with(".yaml") || a.ends_with(".yml"))
            });

        let workload_path = match workload_path {
            Some(p) => p.clone(),
            None => {
                eprintln!("error: no workload specified");
                eprintln!("  use: nbrs run workload=file.yaml ...");
                eprintln!("   or: nbrs run op='hello {{{{cycle}}}}' cycles=10");
                std::process::exit(1);
            }
        };

        workload_file = Some(workload_path.clone());

        let yaml_source = match std::fs::read_to_string(&workload_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: failed to read workload file '{}': {}", workload_path, e);
                std::process::exit(1);
            }
        };

        match parse_workload(&yaml_source, &params) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("error: failed to parse workload: {e}");
                std::process::exit(1);
            }
        }
    };

    // Merge CLI params over workload params. CLI takes priority.
    // This lets workloads set defaults for cycles, threads, etc.
    let mut merged_params = workload.params.clone();
    for (k, v) in &params {
        merged_params.insert(k.clone(), v.clone());
    }

    let driver = merged_params.get("adapter")
        .or_else(|| merged_params.get("driver"))
        .map(|s| s.as_str())
        .unwrap_or("stdout");
    let explicit_cycles: Option<u64> = merged_params.get("cycles").and_then(|s| parse_count(s));
    let threads: usize = merged_params.get("threads").and_then(|s| s.parse().ok()).unwrap_or(1);
    let cycle_rate: Option<f64> = merged_params.get("rate").and_then(|s| s.parse().ok());
    let stanza_rate: Option<f64> = merged_params.get("stanzarate").and_then(|s| s.parse().ok());
    let tag_filter = merged_params.get("tags").map(|s| s.as_str());
    let seq_type = merged_params.get("seq")
        .map(|s| SequencerType::parse(s).unwrap_or(SequencerType::Bucket))
        .unwrap_or(SequencerType::Bucket);
    let error_spec = merged_params.get("errors")
        .cloned()
        .unwrap_or_else(|| ".*:warn,counter".to_string());
    let format = merged_params.get("format")
        .map(|s| StdoutFormat::parse(s).unwrap_or(StdoutFormat::Assignments))
        .unwrap_or(StdoutFormat::Statement);
    let filename = merged_params.get("filename")
        .cloned()
        .unwrap_or_else(|| "stdout".to_string());
    let separator = merged_params.get("separator")
        .cloned()
        .unwrap_or_else(|| ",".to_string());
    let header = merged_params.get("header")
        .map(|s| s == "true" || s == "1" || s == "yes")
        .unwrap_or(false);
    let color = merged_params.get("color")
        .map(|s| s == "true" || s == "1" || s == "yes")
        .or_else(|| {
            if args.iter().any(|a| a == "--color") { Some(true) } else { None }
        })
        .unwrap_or(false);

    // Warn about unrecognized CLI parameters
    for key in params.keys() {
        if !KNOWN_PARAMS.contains(&key.as_str())
            && !workload.declared_params.contains(key)
        {
            let all_known: Vec<&str> = KNOWN_PARAMS.iter().copied()
                .chain(workload.declared_params.iter().map(|s| s.as_str()))
                .collect();
            let suggestion = closest_match(key, &all_known);
            if let Some(closest) = suggestion {
                eprintln!("warning: unrecognized parameter '{key}='. Did you mean '{closest}='?");
            } else {
                eprintln!("warning: unrecognized parameter '{key}=' — \
                    not a known activity parameter or workload-declared param");
            }
        }
    }

    // Extract workload params and phase definitions before consuming ops
    let workload_params = workload.params;
    let phases = workload.phases;
    let phase_order = workload.phase_order;
    let scenarios = workload.scenarios;

    // Collect ALL ops: top-level ops + all phase inline ops.
    // All ops are needed for GK kernel compilation so that the shared
    // program covers every binding referenced by any phase.
    let mut ops = workload.ops;

    // Filter top-level ops by tags (CLI-level tag filter)
    if let Some(filter) = tag_filter {
        ops = match TagFilter::filter_ops(&ops, filter) {
            Ok(filtered) => filtered,
            Err(e) => {
                eprintln!("error: invalid tag filter: {e}");
                std::process::exit(1);
            }
        };
    }

    // Collect phase inline ops for GK compilation (separate from top-level ops)
    let mut phase_ops_for_compile: Vec<nb_workload::model::ParsedOp> = Vec::new();
    for phase in phases.values() {
        phase_ops_for_compile.extend(phase.ops.iter().cloned());
    }

    // For non-phased workloads, require at least some ops
    if ops.is_empty() && phases.is_empty() {
        eprintln!("error: no ops selected (tag filter may have excluded all ops)");
        std::process::exit(1);
    }

    // Warn about ops without explicit adapter selection, auto-assign to default
    let has_explicit_adapter = params.contains_key("adapter") || params.contains_key("driver");
    let warn_no_adapter = |op_list: &[nb_workload::model::ParsedOp]| {
        for op in op_list {
            let op_adapter = op.params.get("adapter")
                .and_then(|v| v.as_str())
                .or_else(|| op.params.get("driver").and_then(|v| v.as_str()));
            if op_adapter.is_none() && !has_explicit_adapter {
                eprintln!("warning: op '{}' has no adapter selection — using '{driver}'", op.name);
            }
        }
    };
    warn_no_adapter(&ops);
    warn_no_adapter(&phase_ops_for_compile);

    if phases.is_empty() {
        eprintln!("nbrs: {} ops selected, {} cycles, {} threads, adapter={}",
            ops.len(), explicit_cycles.map(|c| c.to_string()).unwrap_or("auto".into()), threads, driver);
    } else {
        eprintln!("nbrs: {} phases, {} top-level ops, adapter={}",
            phases.len(), ops.len(), driver);
    }

    // Check for --strict and --dry-run flags
    let strict = args.iter().any(|a| a == "--strict");
    let dry_run = args.iter().find_map(|a| {
        if a == "--dry-run" { Some("silent") }
        else if let Some(mode) = a.strip_prefix("--dry-run=") { Some(mode) }
        else { None }
    });

    // Collect --gk-lib=path flags
    let gk_lib_paths: Vec<std::path::PathBuf> = args.iter()
        .filter_map(|a| a.strip_prefix("--gk-lib="))
        .map(std::path::PathBuf::from)
        .collect();

    // Merge all ops for param expansion and GK compilation.
    // Phase inline ops are appended so they share the same GK program.
    let mut all_ops_for_compile: Vec<nb_workload::model::ParsedOp> = ops.clone();
    all_ops_for_compile.extend(phase_ops_for_compile);

    // Expand workload params in GK binding source before compilation.
    // After string substitution, inject standalone GK bindings for any
    // params referenced in op templates that aren't already defined as
    // GK bindings. This lets {dataset} resolve from the GK output
    // namespace without a separate globals mechanism.
    if !workload_params.is_empty() {
        // Phase 1: string substitution inside GK source (existing behavior)
        for op in &mut all_ops_for_compile {
            if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                for (key, value) in &workload_params {
                    let placeholder = format!("{{{key}}}");
                    if src.contains(&placeholder) {
                        *src = src.replace(&placeholder, value);
                    }
                }
            }
        }

        // Phase 2: inject param bindings into GK source for params
        // referenced in op templates or phase config but not already
        // defined as GK bindings.
        let mut op_refs: std::collections::HashSet<String> = std::collections::HashSet::new();
        for op in all_ops_for_compile.iter() {
            for value in op.op.values() {
                if let Some(s) = value.as_str() {
                    for name in nb_workload::bindpoints::referenced_bindings(s) {
                        if workload_params.contains_key(&name) {
                            op_refs.insert(name);
                        }
                    }
                }
            }
        }
        // Also scan phase config values (cycles, rate, etc.) for {name} refs
        for phase in phases.values() {
            for config_val in [&phase.cycles].into_iter().flatten() {
                if config_val.starts_with('{') && config_val.ends_with('}') {
                    let name = &config_val[1..config_val.len()-1];
                    if workload_params.contains_key(name) {
                        op_refs.insert(name.to_string());
                    }
                }
            }
        }
        if !op_refs.is_empty() {
            for op in &mut all_ops_for_compile {
                if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                    for name in &op_refs {
                        // Skip if the GK source already defines this binding
                        let def_pattern = format!("{name} :=");
                        if src.contains(&def_pattern) {
                            continue;
                        }
                        let value = &workload_params[name];
                        // Numeric params: inject as bare literal; string params: inject quoted
                        let binding = if value.parse::<u64>().is_ok() || value.parse::<f64>().is_ok() {
                            format!("\n{name} := {value}")
                        } else {
                            format!("\n{name} := \"{value}\"")
                        };
                        src.push_str(&binding);
                    }
                }
            }
        }
    }

    // Phase 3: rewrite inline expressions in op templates to GK bindings.
    // Detect {func(...)}, {:=expr}, {:=expr:=} bind points and inject them
    // as named GK bindings (__expr_N), then rewrite the op template strings
    // to use {__expr_N} references. This ensures the synthesis resolver
    // only sees Reference bind points, not InlineDefinition.
    {
        let mut inline_idx = 0usize;
        let mut expr_to_name: std::collections::HashMap<String, String> = std::collections::HashMap::new();

        // First pass: collect all inline expressions across all ops
        for op in all_ops_for_compile.iter() {
            for value in op.op.values() {
                if let Some(s) = value.as_str() {
                    for bp in nb_workload::bindpoints::extract_bind_points(s) {
                        if let nb_workload::bindpoints::BindPoint::InlineDefinition(ref expr) = bp {
                            if !expr_to_name.contains_key(expr) {
                                let name = format!("__expr_{inline_idx}");
                                inline_idx += 1;
                                expr_to_name.insert(expr.clone(), name);
                            }
                        }
                    }
                }
            }
        }

        if !expr_to_name.is_empty() {
            // Inject GK bindings for each discovered inline expression
            for op in all_ops_for_compile.iter_mut() {
                if let nb_workload::model::BindingsDef::GkSource(ref mut src) = op.bindings {
                    for (expr, name) in &expr_to_name {
                        let def_pattern = format!("{name} :=");
                        if !src.contains(&def_pattern) {
                            src.push_str(&format!("\n{name} := {expr}"));
                        }
                    }
                }
            }

            // Rewrite op templates: replace inline expressions with binding refs.
            // Handles {{expr}}, {:=expr}, {:=expr:=}, and auto-detected {expr}.
            for op in all_ops_for_compile.iter_mut() {
                for value in op.op.values_mut() {
                    if let Some(s) = value.as_str() {
                        let mut rewritten = s.to_string();
                        for (expr, name) in &expr_to_name {
                            // Double-brace: {{expr}}
                            rewritten = rewritten.replace(
                                &format!("{{{{{expr}}}}}"),
                                &format!("{{{name}}}"),
                            );
                            // {:=expr:=}
                            rewritten = rewritten.replace(
                                &format!("{{:={expr}:=}}"),
                                &format!("{{{name}}}"),
                            );
                            // {:=expr}
                            rewritten = rewritten.replace(
                                &format!("{{:={expr}}}"),
                                &format!("{{{name}}}"),
                            );
                            // Auto-detected {expr} (single brace)
                            rewritten = rewritten.replace(
                                &format!("{{{expr}}}"),
                                &format!("{{{name}}}"),
                            );
                        }
                        *value = serde_json::Value::String(rewritten);
                    }
                }
            }
        }
    }

    // Compile bindings into GK kernel, with module resolution from the workload directory.
    // The kernel is compiled ONCE from all ops (top-level + phase inline).
    let workload_dir: Option<&std::path::Path> = workload_file.as_ref()
        .and_then(|p| std::path::Path::new(p).parent())
        .or_else(|| Some(std::path::Path::new(".")));

    // Scan CLI params AND phase cycles for GK constant references like
    // cycles={train_count}. These bindings must survive DCE so
    // get_constant() can read them.
    let mut config_refs: Vec<String> = params.values()
        .filter(|v| v.starts_with('{') && v.ends_with('}'))
        .map(|v| v[1..v.len()-1].to_string())
        .collect();
    // Also preserve GK constants referenced in phase cycle configs
    for phase in phases.values() {
        if let Some(ref c) = phase.cycles {
            if c.starts_with('{') && c.ends_with('}') {
                config_refs.push(c[1..c.len()-1].to_string());
            }
        }
    }

    // No exclusions needed — workload params are now injected as GK
    // bindings (Phase 2 above), so they resolve from the GK namespace.
    let kernel = match compile_bindings_with_libs_excluding(
        &all_ops_for_compile, workload_dir, gk_lib_paths, strict, &[], &config_refs,
    ) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("error: failed to compile bindings: {e}");
            std::process::exit(1);
        }
    };

    // GK config expression resolution: resolve {name} references in
    // activity config values from GK folded constants. This enables
    // data-driven config like cycles={train_count}.
    // We eagerly resolve all config values before moving kernel into OpBuilder.
    // Resolution order:
    //   1. Named binding from GK kernel constants
    //   2. Inline const expression (zero-input GK eval)
    //   3. Plain numeric parse (with K/M/B suffixes)
    /// Convert a GK Value to u64, handling f64→u64 truncation.
    fn value_to_u64(v: &nb_variates::node::Value) -> u64 {
        match v {
            nb_variates::node::Value::U64(n) => *n,
            nb_variates::node::Value::F64(f) => *f as u64,
            nb_variates::node::Value::Bool(b) => if *b { 1 } else { 0 },
            _ => 0,
        }
    }

    let resolve_gk_config_with_kernel = |value: &str, kernel: &nb_variates::kernel::GkKernel| -> Option<u64> {
        if value.starts_with('{') && value.ends_with('}') {
            let inner = &value[1..value.len() - 1];
            // Try named binding first
            if let Some(v) = kernel.get_constant(inner) {
                return Some(value_to_u64(v));
            }
            // Try inline const expression
            match nb_variates::dsl::compile::eval_const_expr(inner) {
                Ok(v) => Some(value_to_u64(&v)),
                Err(e) => {
                    eprintln!("error: const expression failed: '{{{inner}}}'");
                    eprintln!("  {e}");
                    None
                }
            }
        } else {
            parse_count(value)
        }
    };

    // Pre-resolve CLI config values that reference GK constants
    let resolved_cli_cycles: Option<u64> = explicit_cycles.or_else(||
        params.get("cycles").and_then(|s| resolve_gk_config_with_kernel(s, &kernel))
    );
    let resolved_cli_threads: Option<u64> = params.get("threads")
        .and_then(|s| resolve_gk_config_with_kernel(s, &kernel));

    // Pre-resolve phase cycle counts from GK constants
    let mut resolved_phase_cycles: HashMap<String, Option<u64>> = HashMap::new();
    for (name, phase) in &phases {
        let resolved = phase.cycles.as_ref()
            .and_then(|s| resolve_gk_config_with_kernel(s, &kernel));
        resolved_phase_cycles.insert(name.clone(), resolved);
    }

    let builder = Arc::new(OpBuilder::new(kernel));
    let program = builder.program();

    // Resolve the adapter for a given driver name string.
    let make_adapter = |driver_name: &str| -> Arc<dyn nb_activity::adapter::DriverAdapter> {
        match driver_name {
            "stdout" => {
                Arc::new(StdoutAdapter::with_config(StdoutConfig {
                    filename: filename.clone(),
                    format,
                    separator: separator.clone(),
                    header,
                    color,
                    ..Default::default()
                }))
            }
            "model" => {
                use nb_adapter_model::{ModelAdapter, ModelConfig};
                let diagnose = args.iter().any(|a| a == "--diagnose");
                Arc::new(ModelAdapter::with_config(ModelConfig {
                    stdout: StdoutConfig {
                        filename: filename.clone(),
                        format,
                        separator: separator.clone(),
                        header,
                        ..Default::default()
                    },
                    diagnose,
                }))
            }
            "http" => {
                use nb_adapter_http::{HttpAdapter, HttpConfig};
                let base_url = params.get("base_url").or_else(|| params.get("host")).cloned();
                let timeout = params.get("timeout")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(30_000);
                Arc::new(HttpAdapter::with_config(HttpConfig {
                    base_url,
                    timeout_ms: timeout,
                    follow_redirects: true,
                }))
            }
            other => {
                eprintln!("error: unknown driver '{other}' (supported: stdout, model, http)");
                std::process::exit(1);
            }
        }
    };

    // --- Phased execution ---
    if !phases.is_empty() {
        // Determine which scenario to run
        let scenario_name = params.get("scenario").map(|s| s.as_str()).unwrap_or("default");
        let phase_names = resolve_scenario(&scenarios, &phase_order, scenario_name);

        eprintln!("nbrs: scenario '{scenario_name}' with {} phases: [{}]",
            phase_names.len(), phase_names.join(", "));

        for phase_name in &phase_names {
            let phase = match phases.get(phase_name) {
                Some(p) => p,
                None => {
                    eprintln!("error: phase '{phase_name}' referenced in scenario '{scenario_name}' not found in phases section");
                    std::process::exit(1);
                }
            };

            eprintln!("nbrs: === phase: {phase_name} ===");

            // Resolve ops for this phase: inline ops, tag-filtered from
            // top-level ops, or all top-level ops.
            // The ops used here come from all_ops_for_compile (already
            // had param expansion applied). We need to split them back
            // out. Phase inline ops were appended after top-level ops.
            let phase_ops = if !phase.ops.is_empty() {
                // Use inline ops (find them in all_ops_for_compile by matching names)
                let phase_op_names: std::collections::HashSet<String> =
                    phase.ops.iter().map(|o| o.name.clone()).collect();
                all_ops_for_compile.iter()
                    .filter(|o| {
                        phase_op_names.contains(&o.name)
                            && o.tags.get("phase").map(|p| p == phase_name).unwrap_or(false)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            } else if let Some(ref tag_spec) = phase.tags {
                // Filter from top-level ops by tag
                match TagFilter::filter_ops(&all_ops_for_compile, tag_spec) {
                    Ok(filtered) => filtered,
                    Err(e) => {
                        eprintln!("error: invalid tag filter in phase '{phase_name}': {e}");
                        std::process::exit(1);
                    }
                }
            } else {
                // No filter — use top-level ops (first `ops.len()` entries)
                all_ops_for_compile[..ops.len()].to_vec()
            };

            if phase_ops.is_empty() {
                eprintln!("warning: phase '{phase_name}' has no ops, skipping");
                continue;
            }

            // Build op sequence for this phase
            let op_sequence = OpSequence::from_ops(phase_ops, seq_type);
            let stanza_len = op_sequence.stanza_length() as u64;

            // Resolve phase config with GK constant support.
            // Phase cycles specifies number of stanzas to execute.
            // Multiply by stanza_length to get the raw cycle count that
            // the activity engine consumes (it claims stanza_length
            // cycles per stanza from the CycleSource).
            let phase_stanzas = resolved_phase_cycles.get(phase_name)
                .copied()
                .flatten()
                .unwrap_or(1);
            let phase_cycles = phase_stanzas * stanza_len;
            let phase_concurrency = phase.concurrency.unwrap_or(1);
            let phase_rate = phase.rate.or(cycle_rate);
            let phase_error_spec = phase.errors.clone().unwrap_or_else(|| error_spec.clone());

            eprintln!("nbrs: phase '{phase_name}': {} ops, cycles={phase_cycles}, concurrency={phase_concurrency}",
                op_sequence.stanza_length());

            let config = ActivityConfig {
                name: phase_name.clone(),
                cycles: phase_cycles,
                concurrency: phase_concurrency,
                cycle_rate: phase_rate,
                stanza_rate: None,
                sequencer: seq_type,
                error_spec: phase_error_spec,
                max_retries: 3,
                stanza_concurrency: 1,
            };

            let phase_driver = phase.adapter.as_deref().unwrap_or(driver);
            let adapter = make_adapter(phase_driver);
            let activity = Activity::with_params(
                config, &Labels::of("session", "cli"), op_sequence, workload_params.clone(),
            );
            activity.run_with_driver(adapter, program.clone()).await;

            eprintln!("nbrs: phase '{phase_name}' complete");
        }

        eprintln!("nbrs: all phases complete");
    } else {
        // --- Legacy single-activity execution (no phases) ---

        // Build op sequence
        // Use the param-expanded ops from all_ops_for_compile (which equals
        // ops when there are no phases).
        let ops = all_ops_for_compile;
        let op_sequence = OpSequence::from_ops(ops, seq_type);
        eprintln!("nbrs: stanza length={}, sequencer={:?}", op_sequence.stanza_length(), seq_type);

        let cycles = if let Some(c) = resolved_cli_cycles {
            if explicit_cycles.is_none() && params.contains_key("cycles") {
                eprintln!("nbrs: cycles={c} (from GK constant)");
            }
            c
        } else {
            op_sequence.stanza_length() as u64
        };

        let threads = if let Some(c) = resolved_cli_threads {
            c as usize
        } else {
            threads
        };

        let stanza_concurrency: usize = params.get("stanza_concurrency")
            .or_else(|| params.get("sc"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        let config = ActivityConfig {
            name: "main".into(),
            cycles,
            concurrency: threads,
            cycle_rate,
            stanza_rate,
            sequencer: seq_type,
            error_spec,
            max_retries: 3,
            stanza_concurrency,
        };

        let activity = Activity::with_params(config, &Labels::of("session", "cli"), op_sequence, workload_params);

        // Check for --tui flag
        let use_tui = args.iter().any(|a| a == "--tui");

        // Get shared metrics before activity is consumed by run()
        let shared_metrics = activity.shared_metrics();

        // If TUI mode, spawn metrics capture thread + TUI thread
        let tui_handle = if use_tui {
            let (tui_reporter, tui_rx) = TuiReporter::channel();

            // Start a metrics capture thread that periodically snapshots
            // the activity's instruments and sends frames to the TUI.
            let capture_metrics = shared_metrics.clone();
            let capture_interval = std::time::Duration::from_millis(500);
            let capture_running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let capture_flag = capture_running.clone();

            let mut reporter = tui_reporter;
            std::thread::spawn(move || {
                use nb_metrics::scheduler::Reporter;
                while capture_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(capture_interval);
                    let frame = capture_metrics.capture(capture_interval);
                    reporter.report(&frame);
                }
            });

            // Start TUI on its own thread
            let mut app = App::with_metrics(tui_rx);
            app.metrics.activity_name = "main".to_string();
            app.metrics.driver_name = driver.to_string();
            app.metrics.threads = threads;
            app.metrics.total_target = cycles;
            app.metrics.rate_config = cycle_rate.map(|r| format!("{r}/s")).unwrap_or("unlimited".into());

            let tui_thread = std::thread::spawn(move || {
                if let Err(e) = app.run() {
                    eprintln!("error: TUI failed: {e}");
                }
            });

            Some((tui_thread, capture_running))
        } else {
            None
        };

        // Determine the openmetrics push URL: explicit flag, or auto-discover
        // from a running `nbrs web` instance in this directory.
        let explicit_url: Option<String> = args.iter()
            .find_map(|a| a.strip_prefix("--report-openmetrics-to=")
                .or_else(|| a.strip_prefix("report-openmetrics-to=")))
            .map(|s| s.to_string());
        let push_url = explicit_url.or_else(|| {
            let url = daemon::discover_web_instance()?;
            eprintln!("nbrs: discovered local web instance, auto-pushing metrics");
            Some(url)
        });

        // If we have a push URL, spawn a metrics push thread.
        let openmetrics_push_flag = push_url.map(|url| {
            let mut reporter = web_push::OpenMetricsPushReporter::new(&url);
            let capture_metrics = shared_metrics.clone();
            let capture_interval = std::time::Duration::from_secs(1);
            let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let flag = running.clone();
            eprintln!("nbrs: pushing openmetrics to {url}");
            std::thread::spawn(move || {
                use nb_metrics::scheduler::Reporter;
                while flag.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(capture_interval);
                    let frame = capture_metrics.capture(capture_interval);
                    reporter.report(&frame);
                }
            });
            running
        });

        // Handle --dry-run: override adapter with a no-op or printing adapter
        if let Some(dry_mode) = dry_run {
            match dry_mode {
                "emit" => {
                    let adapter: Arc<dyn nb_activity::adapter::DriverAdapter> =
                        Arc::new(StdoutAdapter::with_config(StdoutConfig {
                            format: StdoutFormat::Statement,
                            ..Default::default()
                        }));
                    activity.run_with_driver(adapter, program).await;
                }
                "json" => {
                    let adapter: Arc<dyn nb_activity::adapter::DriverAdapter> =
                        Arc::new(StdoutAdapter::with_config(StdoutConfig {
                            format: StdoutFormat::Json,
                            ..Default::default()
                        }));
                    activity.run_with_driver(adapter, program).await;
                }
                _ => {
                    // Silent: assemble but don't execute
                    use nb_activity::adapter::{DriverAdapter, OpDispenser, OpResult, ExecutionError, ResolvedFields};
                    struct NoopDriverAdapter;
                    impl DriverAdapter for NoopDriverAdapter {
                        fn name(&self) -> &str { "noop" }
                        fn map_op(&self, _template: &nb_workload::model::ParsedOp)
                            -> Result<Box<dyn OpDispenser>, String> {
                            struct NoopDispenser;
                            impl OpDispenser for NoopDispenser {
                                fn execute<'a>(&'a self, _cycle: u64, _fields: &'a ResolvedFields)
                                    -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
                                    Box::pin(async { Ok(OpResult { body: None, captures: std::collections::HashMap::new(), skipped: false }) })
                                }
                            }
                            Ok(Box::new(NoopDispenser))
                        }
                    }
                    activity.run_with_driver(Arc::new(NoopDriverAdapter), program).await;
                }
            }
            eprintln!("nbrs: dry-run complete");
        } else {
            let adapter = make_adapter(driver);
            activity.run_with_driver(adapter, program).await;
        }

        // Stop the openmetrics push thread if running.
        if let Some(running) = openmetrics_push_flag {
            running.store(false, std::sync::atomic::Ordering::Relaxed);
        }

        if let Some((tui_thread, capture_running)) = tui_handle {
            // Stop the capture thread
            capture_running.store(false, std::sync::atomic::Ordering::Relaxed);
            // TUI will exit when user presses q
            eprintln!("nbrs: activity complete. Press q in TUI to exit.");
            let _ = tui_thread.join();
        } else {
            eprintln!("nbrs: done");
        }
    }
}

/// Resolve a scenario name to a list of phase names.
///
/// If the scenario is defined in the `scenarios:` section, returns the
/// listed phase names. If no scenarios section exists but phases are
/// defined and the requested name is `"default"`, returns all phases
/// in YAML definition order.
fn resolve_scenario(
    scenarios: &HashMap<String, Vec<nb_workload::model::ScenarioStep>>,
    phase_order: &[String],
    name: &str,
) -> Vec<String> {
    // Check if a scenario with this name exists
    if let Some(steps) = scenarios.get(name) {
        return steps.iter().map(|s| s.name.clone()).collect();
    }

    // If no scenarios section but phases exist, and name is "default",
    // run all phases in definition order
    if name == "default" && !phase_order.is_empty() {
        return phase_order.to_vec();
    }

    eprintln!("error: scenario '{name}' not found");
    std::process::exit(1);
}
