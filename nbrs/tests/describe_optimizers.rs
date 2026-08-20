// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-86 — end-to-end test of the optimizer service boundary through the
//! real binary: the core contract (`nbrs-runtime`) discovers the
//! `nbrs-optimizers` plugin registrations via `inventory` at link time (the
//! crate is force-linked in `run.rs`, like the adapters), and
//! `nbrs describe optimizers` renders them. The core never depends on the
//! algorithm crate; discovery is purely link-time.

use std::process::Command;

const EXPECTED: &[&str] = &[
    "sweep",
    "cost_greedy_traversal",
    "centroid_variant",
    "nelder_mead",
    "hooke_jeeves",
    "bobyqa",
    "cmaes",
    "bayes_opt",
    "hyperband",
];

#[test]
fn describe_optimizers_lists_every_registered_optimizer() {
    let out = Command::new(env!("CARGO_BIN_EXE_nbrs"))
        .args(["describe", "optimizers"])
        .output()
        .expect("run `nbrs describe optimizers`");
    assert!(
        out.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    for name in EXPECTED {
        assert!(
            stdout.contains(name),
            "`{name}` missing from listing:\n{stdout}"
        );
    }
}

#[test]
fn describe_optimizer_detail_prints_full_markdown() {
    let out = Command::new(env!("CARGO_BIN_EXE_nbrs"))
        .args(["describe", "optimizers", "cmaes"])
        .output()
        .expect("run `nbrs describe optimizers cmaes`");
    assert!(
        out.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    // The markdown title + body are embedded in the plugin crate.
    assert!(stdout.contains("# cmaes"), "no markdown title:\n{stdout}");
    assert!(stdout.contains("CMA-ES"), "no body:\n{stdout}");
}
