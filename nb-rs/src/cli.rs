// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CLI tree definition, usage text, and utility functions shared
//! across subcommands.

use crate::bench::bench_gk_completion;
use crate::plot::plot_gk_completion;
use crate::run::run_completion;

/// Discover workload-declared parameters for dynamic completion.
///
/// When `workload=somefile.yaml` is on the command line, parse the
/// file's `params:` section and return param names as `key=` completions.
fn discover_workload_params(_partial: &str, context: &[&str]) -> Vec<String> {
    for word in context {
        let path = if let Some(p) = word.strip_prefix("workload=") {
            p
        } else if word.ends_with(".yaml") || word.ends_with(".yml") {
            word
        } else {
            continue;
        };
        // Try to read the YAML and extract top-level params
        if let Ok(source) = std::fs::read_to_string(path)
            && let Ok(doc) = serde_yaml::from_str::<serde_json::Value>(&source)
                && let Some(params) = doc.get("params").and_then(|v| v.as_object()) {
                    return params.keys().map(|k| format!("{k}=")).collect();
                }
    }
    Vec::new()
}

/// Build the definitive CLI command tree. Completion candidates are
/// derived from each subcommand's definition — no separate lists
/// to keep in sync.
pub fn cli_tree() -> veks_completion::CommandTree {
    use veks_completion::Node;

    let (run_opts, run_flags) = run_completion();
    let (bench_opts, bench_flags) = bench_gk_completion();
    let (plot_opts, plot_flags) = plot_gk_completion();

    veks_completion::CommandTree::new("nbrs")
        .command("run", Node::leaf_with_flags(run_opts, run_flags)
            .with_dynamic_options(discover_workload_params))
        .command("describe", Node::group(vec![
            ("gk", Node::group(vec![
                ("functions", Node::leaf(&[])),
                ("stdlib", Node::leaf(&[])),
                ("dag", Node::leaf(&[])),
                ("modules", Node::leaf(&[])),
            ])),
        ]))
        .command("bench", Node::group(vec![
            ("gk", Node::leaf_with_flags(bench_opts, bench_flags)),
        ]))
        .command("plot", Node::group(vec![
            ("gk", Node::leaf_with_flags(plot_opts, plot_flags)),
        ]))
        .command("web", Node::leaf_with_flags(
            &["bind=", "port="],
            &["--daemon", "--stop", "--restart"],
        ))
        .command("completions", Node::leaf(&["bash", "zsh", "fish"]))
}

pub fn print_usage() {
    eprintln!("nbrs — nosqlbench for Rust");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  nbrs run adapter=stdout workload=file.yaml cycles=100 threads=4");
    eprintln!("  nbrs run workload=file.yaml tags=block:main rate=1000 format=json");
    eprintln!("  nbrs run op='hello {{{{cycle}}}}' cycles=10");
    eprintln!("  nbrs run op='id={{{{mod(hash(cycle), 1000)}}}}' cycles=100 format=json");
    eprintln!("  nbrs describe gk functions    List all GK node functions");
    eprintln!("  nbrs describe gk stdlib       List standard library modules");
    eprintln!("  nbrs describe gk dag <file>   Render a .gk file as DOT/Mermaid/SVG");
    eprintln!("  nbrs bench gk <expr>    Benchmark a GK expression at all compilation levels");
    eprintln!("  nbrs plot gk <expr>     Evaluate a GK expression and plot outputs to terminal");
    eprintln!("  nbrs plot gk <file.gk>  Plot a .gk file's outputs to the terminal");
    eprintln!("  nbrs web [bind=0.0.0.0] [port=8080]  Start the web dashboard");
    eprintln!("  nbrs web --daemon             Start web dashboard in the background");
    eprintln!("  nbrs web --stop               Stop a running background web dashboard");
    eprintln!("  nbrs web --restart            Restart with the same arguments");
    eprintln!();
    eprintln!("Parameters:");
    eprintln!("  workload=<file.yaml>   Workload definition file");
    eprintln!("  adapter=<name>         Adapter type (default: stdout)");
    eprintln!("  cycles=<n>             Number of cycles to execute");
    eprintln!("  threads=<n>            Concurrency level (default: 1)");
    eprintln!("  rate=<n>               Per-cycle rate limit (ops/sec)");
    eprintln!("  stanzarate=<n>         Per-stanza rate limit (stanzas/sec)");
    eprintln!("  tags=<filter>          Tag filter for op selection");
    eprintln!("  seq=<type>             Sequencer: bucket|interval|concat");
    eprintln!("  format=<type>          Output format: assignments|json|csv|stmt");
    eprintln!("  errors=<spec>          Error handler spec");
    eprintln!("  filename=<path>        Output file (default: stdout)");
    eprintln!("  --report-openmetrics-to=<url>  Push metrics in OpenMetrics format");
    eprintln!("                         e.g. http://localhost:8080/api/v1/import/prometheus");
}

/// Resolve a potential workload path, trying extensions if needed.
///
/// Returns `Some(path)` if a workload file exists, `None` otherwise.
pub fn resolve_workload_path(name: &str) -> Option<String> {
    if name.ends_with(".yaml") || name.ends_with(".yml") {
        if std::path::Path::new(name).exists() {
            return Some(name.to_string());
        }
        return None;
    }

    for ext in &[".yaml", ".yml"] {
        let path = format!("{name}{ext}");
        if std::path::Path::new(&path).exists() {
            return Some(path);
        }
    }

    for ext in &["", ".yaml", ".yml"] {
        let path = format!("workloads/{name}{ext}");
        if std::path::Path::new(&path).exists() {
            return Some(path);
        }
    }

    None
}

/// Parse a bind address flexibly: bare IP, host:port, or full URL.
pub fn parse_bind_address(raw: &str, port_override: Option<&str>) -> (String, u16) {
    let default_port = 8080u16;

    let without_scheme = raw
        .strip_prefix("http://").or_else(|| raw.strip_prefix("https://"))
        .unwrap_or(raw);

    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);

    let (host, embedded_port) = if let Some(colon_pos) = host_port.rfind(':') {
        let maybe_port = &host_port[colon_pos + 1..];
        if let Ok(p) = maybe_port.parse::<u16>() {
            (host_port[..colon_pos].to_string(), Some(p))
        } else {
            (host_port.to_string(), None)
        }
    } else {
        (host_port.to_string(), None)
    };

    let port = port_override
        .and_then(|s| s.parse::<u16>().ok())
        .or(embedded_port)
        .unwrap_or(default_port);

    let host = if host.is_empty() { "0.0.0.0".to_string() } else { host };
    (host, port)
}

#[allow(dead_code)]
pub fn format_duration(d: std::time::Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", ns as f64 / 1_000_000_000.0)
    }
}
