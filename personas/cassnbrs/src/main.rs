// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! cassnbrs — Cassandra/CQL persona for nb-rs.
//!
//! Includes all core adapters (stdout, http, model) plus the native
//! CQL adapter using the Apache Cassandra C++ driver.
//!
//! Usage:
//!   cassnbrs run adapter=cql hosts=localhost workload=cql_keyvalue.yaml cycles=1000
//!   cassnbrs run adapter=cql hosts=localhost op="SELECT * FROM system.local LIMIT 1" cycles=1
//!   cassnbrs run adapter=stdout workload=file.yaml cycles=10

use std::sync::Arc;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args.first().map(|s| s.as_str()) == Some("--help") {
        print_usage();
        return;
    }

    // Skip "run" if present
    let run_args: Vec<String> = if args.first().map(|s| s.as_str()) == Some("run") {
        args[1..].to_vec()
    } else {
        args
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        run_command(&run_args).await;
    });
}

async fn run_command(args: &[String]) {
    // Prepare the activity (parse workload, compile GK, build op sequence)
    let prepared = match nb_activity::runner::prepare(args) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    // Dispatch to the appropriate adapter
    match prepared.driver.as_str() {
        "cql" | "cassandra" => {
            use cassnbrs_adapter_cql::{CqlAdapter, CqlConfig};
            let config = CqlConfig::from_params(&prepared.params);
            eprintln!("cassnbrs: connecting to {} (keyspace: {})",
                config.hosts,
                if config.keyspace.is_empty() { "<none>" } else { &config.keyspace });
            let adapter = match CqlAdapter::connect(&config).await {
                Ok(a) => Arc::new(a),
                Err(e) => {
                    eprintln!("error: CQL connection failed: {e}");
                    std::process::exit(1);
                }
            };
            prepared.run_with_driver(adapter).await;
        }

        "stdout" => {
            use nb_adapter_stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
            let format = match prepared.params.get("format").map(|s| s.as_str()) {
                Some("json") => StdoutFormat::Json,
                Some("csv") => StdoutFormat::Csv,
                Some("stmt") | Some("statement") => StdoutFormat::Statement,
                _ => StdoutFormat::Assignments,
            };
            let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
                filename: prepared.params.get("filename").cloned().unwrap_or("stdout".into()),
                newline: true,
                format,
                fields_filter: Vec::new(),
            }));
            prepared.run_with_driver(adapter).await;
        }

        "http" => {
            use nb_adapter_http::{HttpAdapter, HttpConfig};
            let adapter = Arc::new(HttpAdapter::with_config(HttpConfig {
                base_url: prepared.params.get("base_url").or(prepared.params.get("host")).cloned(),
                timeout_ms: prepared.params.get("timeout")
                    .and_then(|s| s.parse().ok()).unwrap_or(30_000),
                follow_redirects: true,
            }));
            prepared.run_with_driver(adapter).await;
        }

        "model" => {
            use nb_adapter_model::{ModelAdapter, ModelConfig};
            use nb_adapter_stdout::{StdoutConfig, StdoutFormat};
            let adapter = Arc::new(ModelAdapter::with_config(ModelConfig {
                stdout: StdoutConfig {
                    filename: prepared.params.get("filename").cloned().unwrap_or("stdout".into()),
                    newline: true,
                    format: StdoutFormat::Statement,
                fields_filter: Vec::new(),
                },
                diagnose: false,
            }));
            prepared.run_with_driver(adapter).await;
        }

        other => {
            eprintln!("error: unknown adapter '{other}'");
            eprintln!("  available: cql, stdout, http, model");
            std::process::exit(1);
        }
    }

    eprintln!("cassnbrs: done.");
}

fn print_usage() {
    eprintln!("cassnbrs — Cassandra/CQL workload testing with nb-rs");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  cassnbrs run adapter=cql hosts=<hosts> workload=<file.yaml> cycles=<n>");
    eprintln!("  cassnbrs run adapter=cql hosts=localhost op=\"SELECT * FROM system.local\" cycles=1");
    eprintln!();
    eprintln!("CQL Parameters:");
    eprintln!("  hosts=<host1,host2>        Contact points (default: 127.0.0.1)");
    eprintln!("  port=<n>                   CQL port (default: 9042)");
    eprintln!("  keyspace=<name>            Keyspace to USE");
    eprintln!("  consistency=<level>        ONE, QUORUM, LOCAL_QUORUM, etc.");
    eprintln!("  username=<user>            Authentication username");
    eprintln!("  password=<pass>            Authentication password");
    eprintln!();
    eprintln!("General Parameters:");
    eprintln!("  adapter=<name>             cql, stdout, http, model");
    eprintln!("  workload=<file.yaml>       Workload definition");
    eprintln!("  op=\"<stmt>\"                Inline op (alternative to workload file)");
    eprintln!("  cycles=<n>                 Number of cycles");
    eprintln!("  concurrency=<n>            Async fibers in flight (default: 1)");
    eprintln!("  tags=<filter>              Tag filter for op selection");
    eprintln!("  format=<type>              Output format: assignments|json|csv|stmt");
}
