// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Data-driven walker over **every** example workload — a thin wrapper around
//! the shared verifier (`nbrs_workload::verify`), the same code that backs the
//! `nbrs check` subcommand. Adding an example needs no Rust code: just the
//! workload plus its verification rules, declared either as `#@` comment
//! directives or a `verify:` block (see the `verify` module docs). So "how CI
//! checks the bundled examples" and "how a user checks their own workload with
//! `nbrs check`" are literally the same code path.

use std::path::Path;

#[test]
fn all_example_workloads_match_their_rules() {
    let bin = env!("CARGO_BIN_EXE_nbrs");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let examples = root.join("examples/workloads");
    let sandbox = root.join("target/test-tmp/workload-walker");

    let sum = nbrs_workload::verify::verify_path(Path::new(bin), &examples, &sandbox);

    eprintln!("\n=== example-workload walker ===");
    eprintln!("  passed cases: {}", sum.passed);
    if !sum.skipped.is_empty() {
        eprintln!("  skipped ({}):", sum.skipped.len());
        for s in &sum.skipped {
            eprintln!("    - {s}");
        }
    }
    if !sum.failures.is_empty() {
        eprintln!("  FAILURES ({}):", sum.failures.len());
        for f in &sum.failures {
            eprintln!("    ✗ {f}");
        }
        panic!("{} example-workload case(s) failed — see the list above", sum.failures.len());
    }
}
