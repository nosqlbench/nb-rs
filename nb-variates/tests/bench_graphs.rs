// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Benchmark DAG graph generators.
//!
//! 3x3 matrix: {10, 100, 1000} nodes × {low, medium, high} connectivity.
//! Each generates a GK source string that compiles to a DAG of the
//! specified size and connectivity pattern.
//!
//! Nodes use hash() and mod(x, K) — both single-wire-input nodes.
//! Connectivity is structural: how many predecessors feed into each
//! node's position in the DAG, creating reuse of upstream outputs.

use std::fmt::Write;
use std::path::PathBuf;

fn output_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/bench_graphs")
}

/// Low connectivity: linear chain. Each node depends on exactly
/// one predecessor. Edge degree = 1 per node.
///
/// Structure: n0 → n1 → n2 → ... → n(N-1)
fn generate_chain(n: usize) -> String {
    let mut s = String::new();
    writeln!(s, "// Benchmark: {n}-node chain (degree=1)").unwrap();
    writeln!(s, "inputs := (cycle)\n").unwrap();
    writeln!(s, "n0 := hash(cycle)").unwrap();
    for i in 1..n {
        if i % 2 == 1 {
            writeln!(s, "n{i} := mod(n{}, {})", i - 1, 1000 + i).unwrap();
        } else {
            writeln!(s, "n{i} := hash(n{})", i - 1).unwrap();
        }
    }
    writeln!(s, "out := n{}", n - 1).unwrap();
    s
}

/// Medium connectivity: layered DAG with moderate fan-out.
/// Nodes in layer L are each derived from 2-3 nodes in layer L-1.
/// Average in-degree ~2.5 (each node is a hash of a predecessor,
/// and some nodes also appear as inputs to 2-3 successors).
///
/// Structure: layers of width W, each node hashes one of several
/// predecessors. Multiple output paths create reuse.
fn generate_layered(n: usize) -> String {
    let width = ((n as f64).sqrt()).ceil().max(3.0) as usize;
    let layers = (n + width - 1) / width;
    let mut s = String::new();
    let mut count = 0usize;

    writeln!(s, "// Benchmark: {n}-node layered DAG (degree~3, width={width}, layers={layers})").unwrap();
    writeln!(s, "inputs := (cycle)\n").unwrap();

    // Layer 0: seed nodes from cycle
    for j in 0..width.min(n) {
        if j == 0 {
            writeln!(s, "n0 := hash(cycle)").unwrap();
        } else {
            // Each seed is a hash of the previous — creates a chain base
            writeln!(s, "n{j} := hash(n{})", j - 1).unwrap();
        }
        count += 1;
    }

    // Subsequent layers
    for layer in 1..layers {
        let layer_base = layer * width;
        let prev_base = (layer - 1) * width;
        let prev_count = width.min(n.saturating_sub(prev_base));
        if prev_count == 0 { break; }

        for j in 0..width {
            let idx = layer_base + j;
            if idx >= n { break; }

            // Pick a predecessor: rotate through prev layer with offset
            // This creates ~2-3 fan-in because each prev node is used
            // by ~2-3 successor nodes (overlapping windows)
            let src = prev_base + ((j * 3 + layer) % prev_count);
            if idx % 3 == 0 {
                writeln!(s, "n{idx} := hash(n{src})").unwrap();
            } else {
                let k = 100 + (idx % 997);
                writeln!(s, "n{idx} := mod(n{src}, {k})").unwrap();
            }
            count += 1;
        }
    }

    writeln!(s, "out := n{}", count - 1).unwrap();
    s
}

/// High connectivity: dense DAG with wide fan-out. Each node is
/// derived from one predecessor, but predecessors are reused
/// extensively (each feeds 5-8 successors). Intermediate hash
/// chains create depth. Average out-degree ~6.
///
/// Structure: layers with wide fan-out. Each layer has multiple
/// "groups" that share predecessors from the prior layer.
fn generate_dense(n: usize) -> String {
    let width = ((n as f64).sqrt() * 1.2).ceil().max(4.0) as usize;
    let layers = (n + width - 1) / width;
    let mut s = String::new();
    let mut count = 0usize;

    writeln!(s, "// Benchmark: {n}-node dense DAG (degree~6, width={width}, layers={layers})").unwrap();
    writeln!(s, "inputs := (cycle)\n").unwrap();

    // Layer 0: seed nodes
    let seed_count = width.min(n);
    for j in 0..seed_count {
        if j == 0 {
            writeln!(s, "n0 := hash(cycle)").unwrap();
        } else {
            writeln!(s, "n{j} := hash(n{})", j - 1).unwrap();
        }
        count += 1;
    }

    // Subsequent layers: each node picks from prev layer with
    // high overlap — the same prev node feeds many successors
    for layer in 1..layers {
        let layer_base = layer * width;
        let prev_base = (layer - 1) * width;
        let prev_count = width.min(n.saturating_sub(prev_base));
        if prev_count == 0 { break; }

        // Reduce effective source count to force high reuse
        // Each of ~(prev_count/3) sources feeds ~6 successors
        let source_count = (prev_count / 3).max(1);

        for j in 0..width {
            let idx = layer_base + j;
            if idx >= n { break; }

            // Map many successors to few sources
            let src = prev_base + (j % source_count);

            // Alternate between hash and mod for variety
            match idx % 4 {
                0 => writeln!(s, "n{idx} := hash(n{src})").unwrap(),
                1 => writeln!(s, "n{idx} := mod(n{src}, {})", 100 + idx % 900).unwrap(),
                2 => writeln!(s, "n{idx} := hash(n{src})").unwrap(),
                _ => writeln!(s, "n{idx} := mod(n{src}, {})", 50 + idx % 500).unwrap(),
            }
            count += 1;
        }
    }

    writeln!(s, "out := n{}", count - 1).unwrap();
    s
}

pub fn generate_all() {
    let dir = output_dir();
    std::fs::create_dir_all(&dir).unwrap();

    let configs: Vec<(usize, &str, fn(usize) -> String)> = vec![
        (10, "chain", generate_chain as fn(usize) -> String),
        (100, "chain", generate_chain),
        (1000, "chain", generate_chain),
        (10, "layered", generate_layered),
        (100, "layered", generate_layered),
        (1000, "layered", generate_layered),
        (10, "dense", generate_dense),
        (100, "dense", generate_dense),
        (1000, "dense", generate_dense),
    ];

    for (size, topo, genfn) in configs {
        let source = genfn(size);
        let filename = format!("{topo}_{size}.gk");
        let path = dir.join(&filename);
        std::fs::write(&path, &source).unwrap();
        let line_count = source.lines().count();
        eprintln!("  wrote {filename} ({line_count} lines)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile_and_verify(src: &str, expected_min_nodes: usize) {
        let k = nb_variates::dsl::compile::compile_gk(src)
            .unwrap_or_else(|e| panic!("compile failed: {e}"));
        let p = k.program();
        assert!(p.output_names().contains(&"out"),
            "missing 'out' output");
        assert!(p.node_count() >= expected_min_nodes,
            "expected >= {expected_min_nodes} nodes, got {}",
            p.node_count());
    }

    fn compile_and_eval(src: &str) {
        let k = nb_variates::dsl::compile::compile_gk(src).unwrap();
        let p = k.into_program();
        let mut state = p.create_state();
        // Evaluate at a few different inputs
        for cycle in [0u64, 1, 42, 999] {
            state.set_inputs(&[cycle]);
            let v = state.pull(&p, "out");
            assert!(!matches!(v, nb_variates::node::Value::None),
                "output should not be None at cycle={cycle}");
        }
    }

    // --- Chain (low connectivity) ---

    #[test]
    fn chain_10_compiles() { compile_and_verify(&generate_chain(10), 9); }

    #[test]
    fn chain_100_compiles() { compile_and_verify(&generate_chain(100), 90); }

    #[test]
    fn chain_1000_compiles() { compile_and_verify(&generate_chain(1000), 900); }

    #[test]
    fn chain_10_evals() { compile_and_eval(&generate_chain(10)); }

    #[test]
    fn chain_100_evals() { compile_and_eval(&generate_chain(100)); }

    #[test]
    fn chain_1000_evals() { compile_and_eval(&generate_chain(1000)); }

    // --- Layered (medium connectivity) ---

    #[test]
    fn layered_10_compiles() { compile_and_verify(&generate_layered(10), 9); }

    #[test]
    fn layered_100_compiles() { compile_and_verify(&generate_layered(100), 90); }

    #[test]
    fn layered_1000_compiles() { compile_and_verify(&generate_layered(1000), 900); }

    #[test]
    fn layered_10_evals() { compile_and_eval(&generate_layered(10)); }

    #[test]
    fn layered_100_evals() { compile_and_eval(&generate_layered(100)); }

    #[test]
    fn layered_1000_evals() { compile_and_eval(&generate_layered(1000)); }

    // --- Dense (high connectivity) ---

    #[test]
    fn dense_10_compiles() { compile_and_verify(&generate_dense(10), 9); }

    #[test]
    fn dense_100_compiles() { compile_and_verify(&generate_dense(100), 90); }

    #[test]
    fn dense_1000_compiles() { compile_and_verify(&generate_dense(1000), 900); }

    #[test]
    fn dense_10_evals() { compile_and_eval(&generate_dense(10)); }

    #[test]
    fn dense_100_evals() { compile_and_eval(&generate_dense(100)); }

    #[test]
    fn dense_1000_evals() { compile_and_eval(&generate_dense(1000)); }

    // --- Generate files ---

    #[test]
    #[ignore] // Run manually: cargo test --test bench_graphs -- --ignored
    fn write_bench_graph_files() {
        generate_all();
    }
}
