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

pub async fn run_command(args: &[String]) {
    if let Err(e) = nb_activity::runner::run(args).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

