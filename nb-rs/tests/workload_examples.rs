// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration tests that run example workloads end-to-end via the
//! stdout adapter, verifying that the full pipeline (YAML parsing →
//! GK compilation → phased execution → adapter output) works correctly.
//!
//! Each test runs `nbrs run` as a subprocess and checks the output.

use std::process::Command;

fn nbrs() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nbrs"));
    // Run from workspace root so workload paths resolve correctly
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    cmd.current_dir(workspace_root);
    cmd.arg("run");
    cmd
}

fn run_workload(workload: &str, extra_args: &[&str]) -> (String, String) {
    let mut cmd = nbrs();
    cmd.arg(format!("workload={workload}"));
    for arg in extra_args {
        cmd.arg(arg);
    }
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (stdout, stderr)
}

fn run_inline(op: &str, extra_args: &[&str]) -> (String, String) {
    let mut cmd = nbrs();
    cmd.arg(format!("op={op}"));
    for arg in extra_args {
        cmd.arg(arg);
    }
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (stdout, stderr)
}

// ─── Example workloads ─────────────────────────────────────────

#[test]
fn basic_workload_runs() {
    let (_stdout, stderr) = run_workload("examples/workloads/getting_started/basic_workload.yaml", &["cycles=3"]);
    assert!(stderr.contains("done"), "stderr: {stderr}");
}

#[test]
fn gk_bindings_produces_output() {
    let (stdout, stderr) = run_workload("examples/workloads/getting_started/gk_bindings.yaml", &["cycles=3"]);
    assert!(stderr.contains("done"), "stderr: {stderr}");
    assert!(stdout.contains("INSERT INTO telemetry"), "stdout: {stdout}");
}

#[test]
fn math_and_bitwise_operators() {
    let (stdout, stderr) = run_workload("examples/workloads/math_and_bitwise.yaml", &["cycles=8"]);
    assert!(stderr.contains("done"), "stderr: {stderr}");
    assert!(stdout.contains("doubled="), "should have arithmetic output");
    assert!(stdout.contains("wave="), "should have float output");
    assert!(stdout.contains("low_byte="), "should have bitwise output");
}

#[test]
fn conditional_ops_skip_falsy() {
    let (stdout, stderr) = run_workload("examples/workloads/conditional_ops.yaml", &["cycles=18"]);
    assert!(stderr.contains("done"), "stderr: {stderr}");
    // 'always' op should appear for every stanza (6 stanzas = 6 lines)
    let always_count = stdout.lines().filter(|l| l.contains("(always)")).count();
    assert_eq!(always_count, 6, "should have 6 'always' lines, got {always_count}");
    // Conditional ops should appear less than 6 times
    let even_count = stdout.lines().filter(|l| l.contains("(even")).count();
    assert!(even_count < 6, "even_only should be skipped sometimes, got {even_count}");
    assert!(even_count > 0, "even_only should appear sometimes");
}

#[test]
fn feature_showcase_phased_execution() {
    let (stdout, stderr) = run_workload("examples/workloads/feature_showcase.yaml", &[]);
    assert!(stderr.contains("phase: setup"), "should run setup phase");
    assert!(stderr.contains("phase: load"), "should run load phase");
    assert!(stderr.contains("phase: verify"), "should run verify phase");
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("CREATE KEYSPACE"), "should have DDL");
    assert!(stdout.contains("INSERT INTO"), "should have inserts");
    assert!(stdout.contains("SELECT *"), "should have selects");
}

#[test]
fn feature_showcase_quick_scenario() {
    let (_stdout, stderr) = run_workload(
        "examples/workloads/feature_showcase.yaml",
        &["scenario=quick"],
    );
    assert!(stderr.contains("phase: setup"), "should run setup");
    assert!(stderr.contains("phase: verify"), "should run verify");
    assert!(!stderr.contains("phase: load"), "quick scenario should skip load");
}

#[test]
fn feature_showcase_param_override() {
    let (_stdout, stderr) = run_workload(
        "examples/workloads/feature_showcase.yaml",
        &["num_items=3"],
    );
    assert!(stderr.contains("done") || stderr.contains("all phases complete"), "stderr: {stderr}");
}

#[test]
fn service_model_mixed_ops() {
    let (stdout, stderr) = run_workload("examples/workloads/service_model.yaml", &["cycles=22"]);
    assert!(stderr.contains("done"), "stderr: {stderr}");
    assert!(stdout.contains("SELECT * FROM service.users"), "should have user reads");
    assert!(stdout.contains("SELECT * FROM service.products"), "should have product reads");
    assert!(stdout.contains("INSERT INTO service.orders"), "should have order writes");
    // Analytics should appear (conditional, every 5th)
    let analytics = stdout.lines().filter(|l| l.contains("avg(price)")).count();
    assert!(analytics > 0, "analytics query should run at least once");
}

// ─── Inline ops ────────────────────────────────────────────────

#[test]
fn inline_simple_expression() {
    let (stdout, stderr) = run_inline("hello {{hash(cycle)}}", &["cycles=3"]);
    assert!(stderr.contains("done"), "stderr: {stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "should have 3 lines");
    for line in &lines {
        assert!(line.starts_with("hello "), "line: {line}");
    }
}

#[test]
fn inline_multiple_ops_with_ratios() {
    let (stdout, stderr) = run_inline(
        "3:read {{cycle}};1:write {{mod(hash(cycle),100)}}",
        &["cycles=8"],
    );
    assert!(stderr.contains("done"), "stderr: {stderr}");
    let reads = stdout.lines().filter(|l| l.starts_with("read")).count();
    let writes = stdout.lines().filter(|l| l.starts_with("write")).count();
    assert!(reads > writes, "should have more reads than writes: {reads} reads, {writes} writes");
}

#[test]
fn inline_math_expression() {
    let (stdout, stderr) = run_inline(
        "val={{sin(to_f64(cycle) * 0.1)}}",
        &["cycles=5"],
    );
    assert!(stderr.contains("done"), "stderr: {stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 5);
    // First cycle (cycle=0): sin(0) = 0
    assert!(lines[0].contains("val=0"), "sin(0) should be 0: {}", lines[0]);
}

// ─── Bang path (shebang) ───────────────────────────────────────

#[test]
fn bare_file_invocation() {
    // nbrs <file.yaml> should work without 'run' subcommand
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_nbrs"))
        .current_dir(workspace_root)
        .arg("examples/workloads/visual/maze.yaml")
        .arg("cycles=3")
        .output()
        .expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("done"), "should complete: {stderr}");
    assert_eq!(stdout.lines().count(), 3, "should have 3 lines");
}

// ─── GK features ───────────────────────────────────────────────

#[test]
fn const_expression_in_cycles() {
    // cycles={4*4} should evaluate to 16 via GK const expression
    let (stdout, stderr) = run_inline("tick", &["cycles={4*4}"]);
    assert!(stderr.contains("done"), "stderr: {stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 16, "4*4=16 cycles expected, got {}", lines.len());
}

#[test]
fn deterministic_output() {
    // Same cycle should produce same output
    let (out1, _) = run_inline("v={{hash(cycle)}}", &["cycles=5"]);
    let (out2, _) = run_inline("v={{hash(cycle)}}", &["cycles=5"]);
    assert_eq!(out1, out2, "same workload should produce identical output");
}
