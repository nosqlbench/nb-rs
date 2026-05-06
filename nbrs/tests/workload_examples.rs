// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration tests that run example workloads end-to-end via the
//! stdout adapter, verifying that the full pipeline (YAML parsing →
//! GK compilation → phased execution → adapter output) works correctly.
//!
//! Each test runs `nbrs run` as a subprocess and checks the output.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Per-invocation session directory so concurrent test runs
/// don't collide on `logs/default_<timestamp>`. cargo runs
/// integration tests in parallel by default, and the wall-
/// clock-second-grained default session name is too coarse
/// to keep them apart.
///
/// We compute a unique path but DON'T create the directory —
/// nbrs's session bootstrap will mkdir it. Pre-creating would
/// trigger nbrs's "directory already contains artifacts"
/// reuse-policy check.
struct SessionDir {
    path: PathBuf,
}

impl SessionDir {
    fn new() -> Self {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Nest each test's session under its own parent so
        // nbrs's `purge_stale_sessions` (which scans the
        // session-path's parent dir) can't see sibling
        // tests' sessions.
        let parent = std::env::temp_dir()
            .join(format!("nbrs-workload-examples-{pid}-{nanos}"));
        std::fs::create_dir_all(&parent).expect("create session parent");
        let path = parent.join("session");
        Self { path }
    }

    fn parent(&self) -> &Path {
        self.path.parent().expect("session dir has parent")
    }
}

impl Drop for SessionDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.parent());
    }
}

fn nbrs(session: &SessionDir) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nbrs"));
    // Run from workspace root so workload paths resolve correctly
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    cmd.current_dir(workspace_root);
    cmd.arg("run");
    cmd.arg("--session-path");
    cmd.arg(&session.path);
    cmd
}

fn run_workload(workload: &str, extra_args: &[&str]) -> (String, String) {
    let session = SessionDir::new();
    let mut cmd = nbrs(&session);
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
    let session = SessionDir::new();
    let mut cmd = nbrs(&session);
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
    // Phase-starting row is `[name] (coords): …` after the
    // SRD-46 status-line redesign. Match the bracketed name.
    assert!(stderr.contains("[setup]"), "should run setup phase, stderr: {stderr}");
    assert!(stderr.contains("[load]"), "should run load phase, stderr: {stderr}");
    assert!(stderr.contains("[verify]"), "should run verify phase, stderr: {stderr}");
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
    assert!(stderr.contains("[setup]"), "should run setup, stderr: {stderr}");
    assert!(stderr.contains("[verify]"), "should run verify, stderr: {stderr}");
    assert!(!stderr.contains("[load]"), "quick scenario should skip load, stderr: {stderr}");
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
    let session = SessionDir::new();
    let output = Command::new(env!("CARGO_BIN_EXE_nbrs"))
        .current_dir(workspace_root)
        .arg("examples/workloads/visual/maze.yaml")
        .arg("cycles=3")
        .arg("--session-path")
        .arg(&session.path)
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

// ─── Shared cells (SRD-16 §"Mutability Rules: Shared Mutable") ─────
//
// Round-trip test for `shared X := <literal>` end-to-end via
// stdout. Each scenario in `shared_cells.yaml` emits stable,
// grep-able lines so the assertions can check exact values
// rather than substring-fuzzing.

#[test]
fn shared_cells_basic_emits_all_four_types() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/shared_cells.yaml",
        &["scenario=basic"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    let lines: Vec<&str> = stdout.lines()
        .filter(|l| l.starts_with("shared/basic "))
        .collect();
    assert_eq!(lines.len(), 3,
        "basic scenario should emit one line per cycle (3 cycles), got {} lines:\n{}",
        lines.len(), stdout);

    // All three lines carry the same shared values (no writers,
    // values stay at compile-time initials). Only `cycle=N`
    // varies. Verify each shared type's literal initializer
    // round-trips exactly through the cell-aware lookup path.
    for (i, line) in lines.iter().enumerate() {
        assert!(line.contains(&format!("cycle={i} ")),
            "line {i} should report cycle={i}: {line}");
        assert!(line.contains("budget=100"),
            "u64 shared cell value must round-trip: {line}");
        assert!(line.contains("rate=0.5"),
            "f64 shared cell value must round-trip: {line}");
        assert!(line.contains("label=alpha"),
            "String shared cell value must round-trip: {line}");
        assert!(line.contains("enabled=true"),
            "bool shared cell value must round-trip: {line}");
        assert!(line.contains("scaled=200"),
            "derived binding consuming a shared cell value must \
             see the cell value (mul(budget_u64, 2) = 200): {line}");
    }
}

#[test]
fn shared_cells_loop_iterates_with_visible_counter() {
    // do_while: "{i} < {iterations}" with iterations=3 should
    // produce three iterations. The counter `i` is set on the
    // loop kernel's input slot each iteration; children bind
    // it through the standard scope chain. Each iteration's
    // child phase reads `i` via cell-aware lookup.
    let (stdout, stderr) = run_workload(
        "examples/workloads/shared_cells.yaml",
        &["scenario=loop"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    let lines: Vec<&str> = stdout.lines()
        .filter(|l| l.starts_with("shared/loop "))
        .collect();
    assert_eq!(lines.len(), 3,
        "loop scenario should emit 3 iterations, got {} lines:\n{}",
        lines.len(), stdout);

    // Counter increments 0, 1, 2; the workload-scoped `budget`
    // shared cell is visible to the child every iteration.
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line, &format!("shared/loop iter={i} budget=100"),
            "iter {i} line shape mismatch: {line}");
    }
}

#[test]
fn shared_cells_mixed_types_round_trip() {
    // One cycle, four ops — each emitting a single shared
    // value tagged by type. Verifies that all four scalar
    // types accepted by `try_fold_shared_init` make it through
    // the full pipeline (compile → cell creation →
    // bind_outer_scope → op-template interpolation → stdout).
    let (stdout, stderr) = run_workload(
        "examples/workloads/shared_cells.yaml",
        &["scenario=mixed_types"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    assert!(stdout.contains("shared/typed kind=u64 value=100"),
        "u64 shared value missing from output:\n{stdout}");
    assert!(stdout.contains("shared/typed kind=f64 value=0.5"),
        "f64 shared value missing from output:\n{stdout}");
    assert!(stdout.contains("shared/typed kind=str value=alpha"),
        "String shared value missing from output:\n{stdout}");
    assert!(stdout.contains("shared/typed kind=bool value=true"),
        "bool shared value missing from output:\n{stdout}");
}

#[test]
fn shared_cells_default_scenario_runs_all() {
    // The default scenario chains all three demonstrations
    // back-to-back, exercising the full surface in one
    // session. The combined output must contain at least one
    // line of each prefix.
    let (stdout, stderr) = run_workload(
        "examples/workloads/shared_cells.yaml",
        &[],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    assert!(stdout.lines().any(|l| l.starts_with("shared/basic ")),
        "default scenario must include the basic demo:\n{stdout}");
    assert!(stdout.lines().any(|l| l.starts_with("shared/loop ")),
        "default scenario must include the loop demo:\n{stdout}");
    assert!(stdout.lines().any(|l| l.starts_with("shared/typed ")),
        "default scenario must include the mixed-types demo:\n{stdout}");
}

// ─── Coverage matrix: workload_coverage_matrix.yaml ─────────────
//
// Single-file matrix exercising every user-accessible workload
// construct that's wired end-to-end. Each scenario emits
// stable line prefixes (`cm/<scenario>/...`) so assertions
// match exact shapes rather than substring-fuzzing.
//
// Scenarios *not* exercised here (constructs that are documented
// but not yet wired or not deterministically testable via stdout)
// are listed at the bottom of this section as commented-out
// `#[test]` stubs paired with the `# ... :` blocks at the end of
// `workload_coverage_matrix.yaml`. When the corresponding
// construct lands, uncomment both halves together.

#[test]
fn coverage_matrix_shared_types_all_four_with_edge_values() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=shared_types"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    let expected_lines = [
        "cm/shared_types kind=u64 sub=zero value=0",
        "cm/shared_types kind=u64 sub=big value=1000",
        "cm/shared_types kind=f64 sub=zero value=0",
        "cm/shared_types kind=f64 sub=pi value=3.14159",
        "cm/shared_types kind=str sub=empty value=[]",
        "cm/shared_types kind=str sub=text value=matrix",
        "cm/shared_types kind=bool sub=on value=true",
        "cm/shared_types kind=bool sub=off value=false",
    ];
    for expected in expected_lines {
        assert!(stdout.contains(expected),
            "missing line `{expected}` in:\n{stdout}");
    }
}

#[test]
fn coverage_matrix_final_modifier_round_trips() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=final_modifier"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("cm/final base_dim=256"),
        "final-modifier value missing:\n{stdout}");
}

#[test]
fn coverage_matrix_derived_binding_consumes_shared_cell() {
    // `doubled_count := mul(count_big, 2)` reads the cell
    // value (1000) through standard GK wiring. Output must
    // show the multiplied result, proving the cell value
    // flows downstream (not just visible via `lookup`).
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=derived_from_shared"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("cm/derived count_big=1000 doubled=2000"),
        "derived binding output missing:\n{stdout}");
}

#[test]
fn coverage_matrix_for_each_chain_three_levels() {
    // 3-level chain: workload → for_each → phase. The phase
    // sees both the for_each iter var (0,1,2 from the CSV)
    // and the workload-scope shared values via the
    // bind_outer_scope chain.
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=for_each_chain"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    let lines: Vec<&str> = stdout.lines()
        .filter(|l| l.starts_with("cm/for_each_chain "))
        .collect();
    assert_eq!(lines.len(), 3, "expected 3 iterations:\n{stdout}");
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line, &format!(
            "cm/for_each_chain iter={i} count_big=1000 label=matrix"
        ), "line {i} shape mismatch: {line}");
    }
}

#[test]
fn coverage_matrix_do_while_chain_three_levels() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=do_while_chain"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    let lines: Vec<&str> = stdout.lines()
        .filter(|l| l.starts_with("cm/do_while_chain "))
        .collect();
    assert_eq!(lines.len(), 3, "expected 3 do-while iterations:\n{stdout}");
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line, &format!(
            "cm/do_while_chain i={i} count_big=1000 label=matrix"
        ), "line {i} shape mismatch: {line}");
    }
}

#[test]
fn coverage_matrix_conditional_op_gated_by_shared_bool() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=conditional_op"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    // gated_on (if: flag_on=true) fires, gated_off (if:
    // flag_off=false) is suppressed, always fires.
    assert!(stdout.contains("cm/conditional gated=on result=fired"),
        "gated_on (true) must fire:\n{stdout}");
    assert!(stdout.contains("cm/conditional gated=always result=fired"),
        "always must fire:\n{stdout}");
    assert!(!stdout.contains("cm/conditional gated=off"),
        "gated_off (false) must be suppressed:\n{stdout}");
}

#[test]
fn coverage_matrix_multi_cell_no_cross_name_interference() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=multi_cell"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains(
        "cm/multi_cell zero=0 big=1000 pi=3.14159 text=matrix on=true off=false"
    ), "multi-cell output missing or malformed:\n{stdout}");
}

#[test]
fn coverage_matrix_nested_for_each_inner_sees_outer_iter_var() {
    // Inner for_each spec `inner in pre_{outer}` references the
    // outer for_each's iter var. The fix routes the inner's
    // dispatcher to the outer's *live execution* kernel (via
    // `effective_parent_kernel` preferring `ctx.current_parent_kernel`
    // over the scope-tree canonical ancestor), so spec
    // interpolation resolves `{outer}` to the current iteration's
    // value rather than the canonical default.
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &["scenario=nested_for_each"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    // Outer iterates over a,b,c — three iterations. Each
    // inner_for_each enumerates a single value (`pre_<outer>`),
    // so the leaf phase fires three times total.
    let lines: Vec<&str> = stdout.lines()
        .filter(|l| l.starts_with("cm/nested "))
        .collect();
    assert_eq!(lines.len(), 3,
        "expected 3 nested-iteration leaf calls:\n{stdout}");
    for (outer, expected) in [("a", "pre_a"), ("b", "pre_b"), ("c", "pre_c")] {
        let target = format!("cm/nested outer={outer} inner={expected}");
        assert!(lines.contains(&target.as_str()),
            "missing nested line `{target}`:\n{stdout}");
    }
}

#[test]
fn coverage_matrix_default_runs_full_matrix() {
    // Round-trip: every other scenario's identifying line
    // appears at least once in the default-scenario combined
    // output.
    let (stdout, stderr) = run_workload(
        "examples/workloads/workload_coverage_matrix.yaml",
        &[],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    let prefixes = [
        "cm/shared_types ",
        "cm/final ",
        "cm/derived ",
        "cm/for_each_chain ",
        "cm/do_while_chain ",
        "cm/conditional ",
        "cm/multi_cell ",
    ];
    for prefix in prefixes {
        assert!(stdout.lines().any(|l| l.starts_with(prefix)),
            "default scenario missing prefix `{prefix}`:\n{stdout}");
    }
}

#[test]
fn json_shaped_workload_param_values_round_trip() {
    // Workload params whose values are JSON-shaped or
    // CQL-map-shaped (`{'class': 'SimpleStrategy', ...}`,
    // `{"a": 1}`) must round-trip through op-template
    // `{name}` substitution unchanged. The `{...}` content of
    // the value is literal text, never a bind point — same
    // disambiguation rule that
    // `nbrs_workload::bindpoints::is_literal_content` applies
    // to op-template parsing, also enforced inside the GK
    // string-interpolation desugar (SRD-10 §"String
    // Interpolation": invalid GK expression bodies leave the
    // literal alone).
    let (stdout, stderr) = run_workload(
        "examples/workloads/json_param.yaml",
        &[],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");

    assert!(stdout.contains(
        "json_param/cql_replication value={'class': 'SimpleStrategy', 'replication_factor': '1'}"
    ), "CQL replication map must round-trip:\n{stdout}");
    assert!(stdout.contains(
        "json_param/json_object value={\"a\": 1, \"b\": [2, 3]}"
    ), "JSON object must round-trip:\n{stdout}");
    assert!(stdout.contains("json_param/scalar_string value=plain"),
        "scalar string param must round-trip:\n{stdout}");
}

#[test]
fn unresolved_placeholder_error_carries_yaml_location() {
    // `for_each: "color in {undeclared_thing}"` references a name
    // that's neither a workload param, an outer iter var, nor an
    // inherited binding. The resulting interpolation error must
    // be prefixed with `<path>:<line>:<col>:` so the user can
    // jump directly to the offending spec in the YAML.
    let (_stdout, stderr) = run_workload(
        "examples/workloads/diag_unresolved.yaml",
        &[],
    );
    assert!(stderr.contains("examples/workloads/diag_unresolved.yaml:"),
        "error must carry workload path:\n{stderr}");
    // The for_each line is at line 13 in the file; column points
    // at the spec text's start. Don't pin the exact column (it's
    // formatting-sensitive) but verify line:col format is present.
    assert!(stderr.contains("diag_unresolved.yaml:13:"),
        "error must carry the line of the failing for_each:\n{stderr}");
    assert!(stderr.contains("unresolved placeholder '{undeclared_thing}'"),
        "error must still describe the failing placeholder:\n{stderr}");
}

// ── Documented-but-not-yet-exercised tests ──────────────────
//
// Each `#[test]` below pairs with a commented-out scenario at
// the bottom of `workload_coverage_matrix.yaml`. When the
// corresponding construct lands, uncomment both halves
// together. Keeping them here makes the gap visible in the
// test binary's symbol table without breaking compilation.

// #[test]
// fn coverage_matrix_concurrent_shared_writes_lwwins() {
//     // Concurrent for_each branches all decrement the same
//     // shared cell. Last-write-wins baseline (SRD-16
//     // §"Concurrent semantics: last-write-wins") makes the
//     // exact final count non-deterministic, so this test is
//     // gated on the templated patterns shipping
//     // (`shared(atomic)` / `shared(sum)`). The kernel-level
//     // test `shared_last_write_wins_under_concurrent_writers`
//     // covers the Mutex serialization API surface today.
// }

// #[test]
// fn coverage_matrix_state_driven_do_while_termination() {
//     // do_while: "{count_big} > 0" with a child phase that
//     // decrements `count_big`. Requires per-cycle propagation
//     // from the leaf phase's per-cycle kernel back to the
//     // loop scope's SharedCell, which isn't yet wired (no GK
//     // binding syntax targets an outer shared cell, and the
//     // dispatcher only writes the counter, not arbitrary
//     // shared names). Tracked in SRD-18b §"Open: per-cycle
//     // propagation".
// }

// #[test]
// fn coverage_matrix_cursor_driven_phase() {
//     // `cycles: ===auto` with a cursor-bound source. Needs a
//     // mock source the adapter-testkit doesn't currently
//     // provide. Vectordata integration tests cover this with
//     // real datasets; the synthetic single-file demo is gated
//     // on a mock source crate.
// }

// #[test]
// fn coverage_matrix_delay_field_reads_shared_cell() {
//     // `delay: rate_pi` resolves the cell value per cycle.
//     // Mechanically should work today, but verifying delay
//     // application requires timing measurement that stdout
//     // grepping can't perform. Gated on a metric-based
//     // assertion path that compares wall-clock to expected.
// }

// ─── Synthetic metrics (SRD-40b) end-to-end ─────────────────

/// Run `synthetic_metrics.yaml` and verify every declared
/// synthetic metric flows through the pipeline into the
/// session's `metrics.db`. Covers SRD-40b §1 (the schema:
/// mapping form, bare-string sugar, list form with
/// `wire := <expr>` entries) plus SRD-40a §4.3 (unit-suffix
/// + `metric_family.unit` invariant).
///
/// Owns its `SessionDir` for the whole test so the metrics.db
/// can be inspected after the run; the session parent is
/// removed by `SessionDir`'s `Drop` impl when the test exits.
#[test]
fn synthetic_metrics_workload_populates_metric_family() {
    let session = SessionDir::new();
    let mut cmd = nbrs(&session);
    cmd.arg("workload=examples/workloads/synthetic_metrics.yaml");
    cmd.arg("cycle_count=12");
    let output = cmd.output().expect("failed to run nbrs");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        output.status.success() && stderr.contains("done"),
        "synthetic_metrics workload did not complete:\n{stderr}",
    );

    let db_path = session.path.join("metrics.db");
    assert!(db_path.exists(), "metrics.db missing at {db_path:?}");
    let conn = rusqlite::Connection::open(&db_path)
        .expect("open metrics.db");

    // (family_name, expected_type, expected_unit). SRD-40b §1
    // says `unit:` lands in BOTH the `_<unit>` suffix on
    // `metric_family.name` AND in `metric_family.unit`.
    let expectations: &[(&str, &str, Option<&str>)] = &[
        // Mapping form: explicit unit "ms" → name suffix + unit column.
        ("latency_curve_ms", "gauge", Some("ms")),
        // Bare-string sugar: `metrics: load` — gauge default, no unit.
        ("load", "gauge", None),
        // List form with `:= <expr>` — auto-injected wires.
        ("forecast_low", "gauge", None),
        ("forecast_high", "gauge", None),
        // Counter with explicit unit "ops".
        ("step_counter_ops", "counter", Some("ops")),
        // Histogram defaults: stored as "summary" per OpenMetrics
        // mapping (HDR-backed → summary).
        ("observation_dist", "summary", None),
    ];

    for (name, expected_type, expected_unit) in expectations {
        let row: Result<(String, Option<String>), _> = conn.query_row(
            "SELECT type, unit FROM metric_family WHERE name = ?1",
            [name],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
        );
        let (got_type, got_unit) = row.unwrap_or_else(|e| {
            panic!("metric_family row missing for {name:?}: {e}");
        });
        assert_eq!(
            got_type, *expected_type,
            "metric_family.type mismatch for {name}",
        );
        assert_eq!(
            got_unit.as_deref(), *expected_unit,
            "metric_family.unit mismatch for {name}",
        );
    }
}
