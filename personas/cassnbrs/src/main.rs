// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! cassnbrs — Cassandra/CQL persona for nb-rs.
//!
//! A thin shell that links the CQL adapter crate (which registers
//! itself via inventory) and delegates to the shared runner.

// Link the CQL adapter + GK nodes (inventory registration happens at link time).
extern crate cassnbrs_adapter_cql;

// Link the standard adapters.
extern crate nb_adapter_stdout;
extern crate nb_adapter_http;
extern crate nb_adapter_testkit;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args.first().map(|s| s.as_str()) == Some("--help") {
        print_usage();
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        if let Err(e) = nb_activity::runner::run(&args).await {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    });
}

fn print_usage() {
    eprintln!("cassnbrs — Cassandra/CQL workload testing with nb-rs");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  cassnbrs run adapter=cql hosts=<hosts> workload=<file.yaml> cycles=<n>");
    eprintln!("  cassnbrs run adapter=stdout workload=file.yaml cycles=10");
    eprintln!();
    eprintln!("Adapters: {}", nb_activity::adapter::registered_driver_names().join(", "));
}
