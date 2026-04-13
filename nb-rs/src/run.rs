// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The `run` subcommand: delegates to the shared runner.
//!
//! nbrs registers its adapters (stdout, http, testkit, plotter) via
//! inventory at link time, then calls `nb_activity::runner::run()`.

// Link adapter crates for inventory registration.
extern crate nb_adapter_stdout;
extern crate nb_adapter_http;
extern crate nb_adapter_testkit;
extern crate nb_adapter_plotter;

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
    "filename", "separator", "header", "color", "mode", "fade", "lanes",
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
/// and flag lists. Available for shell-completion integrations.
pub fn run_completion() -> (&'static [&'static str], &'static [&'static str]) {
    // Options (key=value prefixes) and flags (standalone switches)
    static OPTIONS: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
        let mut opts: Vec<&str> = KNOWN_PARAMS.iter().map(|p| *p).collect();
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

pub async fn run_command(args: &[String]) {
    if let Err(e) = nb_activity::runner::run(args).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

