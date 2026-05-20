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

// ─── Coverage matrix tests moved to nbrs/tests/scope.rs ─────────
//
// The `scope_coverage.yaml` workload (formerly
// `workload_coverage_matrix.yaml`) and its matching
// `scope_*` tests now live in their own thematic file, per the
// `<theme>_coverage.yaml` + `nbrs/tests/<theme>.rs` pattern
// established alongside `cursor_partitions_coverage`.

#[test]
fn shared_bool_through_for_each_into_consumer_phase_bindings() {
    // SRD-66 §"Surface 2" + §"Surface 3" integration:
    //
    //   1. Workload-root `shared X := <bool literal>` produces a
    //      SharedCell-backed Bool input slot at root (SRD-13c
    //      §"Shared Mutable" step 1 + step 2).
    //   2. The cell propagates down the scope chain to a consumer
    //      phase whose own `bindings:` references X via `pick(...)`
    //      (SRD-13d §1 + SRD-66 §"Surface 3").
    //   3. The consumer phase reads X as Bool, `pick` selects
    //      the matching value, the op emits it.
    //
    // Failure mode this catches: if any synthesizer in the chain
    // (synthesize_for_each_scope or build_phase_scope_kernel)
    // mis-classifies X as a Coordinate-kind U64 input slot, then
    // the standard per-cycle `set_inputs(&[u64])` propagation
    // clobbers the cell with `Value::U64(cycle)` and `pick`
    // panics with "non-bool type U64". The workload completes
    // (no panic at chain assembly), and the output line carries
    // the correct picked value (`alpha_table`, since has_a=true
    // and has_b=false).
    //
    // This is the same chain shape that surfaces in
    // `adapters/cql/workloads/full_cql_vector.yaml`'s
    // `await_index` phase — minus the CQL driver and minus the
    // probe-phase result-binding writeback (the writeback is a
    // separate dimension, exercised by other tests). Here the
    // workload-root literal is the entire write surface, so the
    // failure must localise to type preservation through the
    // scope chain.
    let (stdout, stderr) = run_workload(
        "examples/workloads/shared_pick_through_for_each.yaml",
        &[],
    );
    assert!(stderr.contains("all phases complete"),
        "workload did not complete cleanly. stderr:\n{stderr}\nstdout:\n{stdout}");

    let lines: Vec<&str> = stdout.lines()
        .filter(|l| l.starts_with("spc/consume "))
        .collect();
    // Nested for_each: outer={p1,p2} × inner={lo,hi} = 4 cycles.
    assert_eq!(lines.len(), 4,
        "nested for_each should produce 4 consume lines, got {}:\n{stdout}",
        lines.len());

    // Every iteration must show the picked value — has_a=true
    // selects index 0 → "alpha_table". If the chain corrupts the
    // type, pick panics and we'd see fewer than 4 lines (or
    // none).
    for line in &lines {
        assert!(line.contains("chosen=alpha_table"),
            "pick should select alpha_table (has_a=true); got line: {line}");
    }
}

// ─── Scenario-tree `set:` (workload-param shadowing) ──────────
//
// Six scenarios in `scenario_param_overrides.yaml` exercise the
// canonical surfaces of `set:`: bare-token shadow, multi-key
// shadow, expression-with-interpolation value, set-wrapping-
// for_each composition, and nested-set composition. All resolve
// through the GK scope-chain (no HashMap merges, no synthesizer
// side-channels). Failures surface as the wrong shadow being
// applied or the shadow being silently dropped.

#[test]
fn set_baseline_picks_up_workload_param_defaults() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/scenario_param_overrides.yaml",
        &["scenario=baseline"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("set/default/small"),
        "baseline must show workload-root param values:\n{stdout}");
}

#[test]
fn set_single_shadow_overrides_one_param() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/scenario_param_overrides.yaml",
        &["scenario=single_shadow"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("set/verbose/small"),
        "single_shadow must show mode=verbose with size unchanged:\n{stdout}");
}

#[test]
fn set_multi_shadow_overrides_two_params_at_once() {
    let (stdout, stderr) = run_workload(
        "examples/workloads/scenario_param_overrides.yaml",
        &["scenario=multi_shadow"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("set/verbose/bulk"),
        "multi_shadow must show both mode=verbose AND size=bulk:\n{stdout}");
}

#[test]
fn set_value_supports_workload_param_interpolation() {
    // `set: { mode: "mode_for_{size}" }` — the RHS goes through
    // the same two-pass evaluator as for_each clause specs:
    // workload-param `{size}` interpolation first, then GK
    // const-expression eval. So `mode` ends up as the literal
    // string "mode_for_small" (since size's workload-root
    // default is "small").
    let (stdout, stderr) = run_workload(
        "examples/workloads/scenario_param_overrides.yaml",
        &["scenario=computed_value"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("set/mode_for_small/small"),
        "computed_value must show interpolated mode:\n{stdout}");
}

#[test]
fn set_composes_with_for_each_inside_phases() {
    // The override (`mode=swept`) is in force for every
    // iteration of the inner for_each (`n in 1,2,3`). Confirms
    // scenario-tree bindings don't interfere with iter-var
    // propagation: the iter-var's per-step kernel chains
    // through the bindings scope, so descendants see both
    // the iter-var AND the shadowed workload param.
    let (stdout, stderr) = run_workload(
        "examples/workloads/scenario_param_overrides.yaml",
        &["scenario=with_for_each"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    for n in [1, 2, 3] {
        let expected = format!("set/n={n}/swept");
        assert!(stdout.contains(&expected),
            "with_for_each iteration n={n} missing `{expected}`:\n{stdout}");
    }
}

#[test]
fn set_nesting_composes_overrides_across_layers() {
    // Outer `set: { mode: outer }` wraps an inner
    // `set: { size: inner_size }`. The inner scope sees BOTH
    // overrides simultaneously: mode from the outer scope's
    // `init mode = "outer"` (propagated through the chain as
    // an init output) AND size from the inner scope's own
    // `init size = "inner_size"`. Validates the local-
    // final/init transit-suppression behaviour in
    // materialize_wiring_from_outer: the outer's init must
    // shadow workload-root's "default" cell so it reaches the
    // inner phase via the chain.
    let (stdout, stderr) = run_workload(
        "examples/workloads/scenario_param_overrides.yaml",
        &["scenario=nested"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("set/outer/inner_size"),
        "nested must show outer's mode AND inner's size:\n{stdout}");
}

#[test]
fn bindings_long_form_equivalent_to_set_sugar() {
    // Direct `bindings: | const NAME := …` body — the canonical
    // form `set:` desugars to. Same shadowing behaviour,
    // same chain semantics, just written explicitly so
    // authors can mix in any other GK construct (derived
    // bindings, shared cells, `final` for true compile-time
    // constants). This test pins the parity with the
    // sugared form.
    let (stdout, stderr) = run_workload(
        "examples/workloads/scenario_param_overrides.yaml",
        &["scenario=long_form"],
    );
    assert!(stderr.contains("all phases complete"), "stderr: {stderr}");
    assert!(stdout.contains("set/verbose/bulk"),
        "long_form must shadow both mode and size:\n{stdout}");
}

#[test]
fn empty_bindings_and_set_blocks_emit_no_op_warning() {
    // A scenario-tree `set:` or `bindings:` block with no
    // `phases:` body is structurally a no-op: the scope is
    // entered and immediately exited with no descendants
    // reading any of its declared names. The parser warns at
    // workload-load time and keeps the scope node out of the
    // resolved tree (almost always an author error — typed
    // the override and forgot the body). Pin both forms here
    // so a future refactor that silences the warning is
    // caught.
    let yaml = r#"
params:
  mode: default

scenarios:
  good:
    - set: { mode: verbose }
      phases:
        - just_say
  empty_set:
    - set: { mode: verbose }
  empty_bindings:
    - bindings: |
        const mode := "verbose"

phases:
  just_say:
    adapter: stdout
    cycles: 1
    ops:
      msg:
        stmt: "mode={mode}"
"#;
    let (path, session) = write_inline_workload("empty_set_warning", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    // Pick the `good` scenario so the run succeeds — the
    // warnings still fire because they're parser-level
    // (emitted once per workload-load, regardless of which
    // scenario the operator picked).
    cmd.arg("scenario=good");
    let output = cmd.output().expect("failed to run nbrs");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();

    assert!(stderr.contains("`set:` block (overriding [\"mode\"])"),
        "empty set: must emit the no-op warning naming the \
         overridden keys. stderr:\n{stderr}");
    assert!(stderr.contains("scenario-tree `bindings:` block has no"),
        "empty bindings: must emit the no-op warning. stderr:\n{stderr}");

    // The `good` scenario still completes and substitutes
    // mode correctly — the empty blocks are warnings, not
    // errors, and they don't poison the run.
    assert!(stderr.contains("all phases complete"),
        "good scenario must still complete. stderr:\n{stderr}");
    assert!(stdout.contains("mode=verbose"),
        "good scenario must still produce mode=verbose. stdout:\n{stdout}");
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
        // Mapping form with non-bare `value:` — auto-injected
        // into op-template bindings (SRD-13d Phase 9 §1).
        ("latency_window", "gauge", None),
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

/// SRD-13d Phase 9 follow-up §3 — value-correctness check.
///
/// The sibling test above only asserts metric_family rows
/// exist; this one runs the workload with a fixed
/// cycle_count and asserts the recorded values are
/// consistent with the workload's per-cycle formulas.
/// Catches any per-fiber kernel-instancing or cross-scope-
/// snapshot bug that the row-existence test would miss.
///
/// `cycles: "{cycle_count}"` plus the implicit
/// `cycles: ===auto` shape means the phase runs
/// `cycle_count * stanza_len` total cycles (one per op per
/// stanza). With cycle_count=6 and 4 ops, total cycles = 24.
/// Per-cycle formulas (from
/// `examples/workloads/synthetic_metrics.yaml`):
///   load           = cycle + 1
///   latency_curve  = load * 2             (per phase)
///   forecast_low   = latency_curve * 0.9  (synth_op_list)
///   forecast_high  = latency_curve * 1.1  (synth_op_list)
///   step           = 1                    (synth_op_kinds)
///   observation    = cycle % 100          (synth_op_kinds)
///
/// Each metric instance is a distinct (family, op-label)
/// pair; the test asserts shape (positive values, plausible
/// bounds, formula-consistent ratios) rather than pinning
/// the exact last-cycle value, which depends on the op
/// sequencer's ordering.
#[test]
fn synthetic_metrics_workload_records_correct_values() {
    let session = SessionDir::new();
    let mut cmd = nbrs(&session);
    cmd.arg("workload=examples/workloads/synthetic_metrics.yaml");
    cmd.arg("cycle_count=6");
    let output = cmd.output().expect("failed to run nbrs");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        output.status.success() && stderr.contains("done"),
        "synthetic_metrics workload did not complete:\n{stderr}",
    );

    let db_path = session.path.join("metrics.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open metrics.db");

    /// For each instance of `family`, return (op_label,
    /// last_mean, last_count, last_sum, last_max). Joins
    /// across the schema; orders samples per instance by
    /// timestamp.
    fn all_instance_samples(
        conn: &rusqlite::Connection,
        family: &str,
    ) -> Vec<(String, f64, i64, f64, f64)> {
        let mut stmt = conn.prepare(
            "SELECT i.id, COALESCE(i.spec, '') FROM metric_instance i \
             JOIN metric_family f ON i.family_id = f.id \
             WHERE f.name = ?1",
        ).unwrap();
        let instances: Vec<(i64, String)> = stmt
            .query_map([family], |r| Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
            )))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        instances.into_iter().filter_map(|(id, label_set_json)| {
            let row: rusqlite::Result<(Option<f64>, Option<i64>, Option<f64>, Option<f64>)> =
                conn.query_row(
                    "SELECT s.mean, s.count, s.sum, s.max FROM sample_value s \
                     WHERE s.instance_id = ?1 \
                     ORDER BY s.timestamp_ms DESC LIMIT 1",
                    [id],
                    |r| Ok((
                        r.get::<_, Option<f64>>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<f64>>(2)?,
                        r.get::<_, Option<f64>>(3)?,
                    )),
                );
            row.ok().map(|(mean, count, sum, max)| (
                label_set_json,
                mean.unwrap_or(0.0),
                count.unwrap_or(0),
                sum.unwrap_or(0.0),
                max.unwrap_or(0.0),
            ))
        }).collect()
    }

    // Every metric must have at least one instance with at
    // least one sample (i.e. the dispenser path actually wrote
    // through to the cadence reporter).
    for family in &["load", "latency_curve_ms", "latency_window",
                    "forecast_low", "forecast_high",
                    "step_counter_ops", "observation_dist"] {
        let samples = all_instance_samples(&conn, family);
        assert!(!samples.is_empty(),
            "metric {family}: no instance samples in metrics.db");
    }

    // Gauges. All recorded values must be positive (cycles
    // start at 0 → cycle+1 ≥ 1, mul by positive constant
    // stays positive).
    for family in &["load", "latency_curve_ms", "latency_window",
                    "forecast_low", "forecast_high"] {
        for (label, mean, _, _, _) in all_instance_samples(&conn, family) {
            assert!(mean > 0.0,
                "{family} ({label}): gauge value {mean} should be positive");
        }
    }

    // Counter. step_counter_ops always increments by 1 per
    // synth_op_kinds execution, so the cumulative count must
    // be ≥ 1 and ≤ cycle_count (synth_op_kinds runs once per
    // stanza of the 4-op cycle).
    for (label, _, count, _, _) in all_instance_samples(&conn, "step_counter_ops") {
        assert!(count >= 1,
            "step_counter_ops ({label}): expected ≥1, got {count}");
        assert!(count <= 6,
            "step_counter_ops ({label}): expected ≤6 (cycle_count=6), got {count}");
    }

    // Histogram. observation = cycle % 100. With 24 total
    // cycles (cycle_count=6, 4 ops, every 4th is
    // synth_op_kinds) the values recorded are at cycles 3, 7,
    // 11, 15, 19, 23 → observations 3, 7, 11, 15, 19, 23.
    // count = 6, max ≤ 23.
    for (label, _, count, _sum, max) in all_instance_samples(&conn, "observation_dist") {
        assert!(count >= 1,
            "observation_dist ({label}): expected ≥1 sample, got {count}");
        assert!(count <= 6,
            "observation_dist ({label}): expected ≤6 samples, got {count}");
        assert!(max <= 23.0,
            "observation_dist ({label}): max={max}, expected ≤23");
    }

    // Cross-formula invariant — for instances of forecast_low
    // and forecast_high writing on the same cycles, the
    // ratio matches the formula (forecast_high =
    // latency_curve * 1.1, forecast_low = latency_curve *
    // 0.9 → forecast_high / forecast_low = 1.1/0.9 ≈ 1.222).
    let lows = all_instance_samples(&conn, "forecast_low");
    let highs = all_instance_samples(&conn, "forecast_high");
    if let (Some((_, low, _, _, _)), Some((_, high, _, _, _))) =
        (lows.first(), highs.first())
    {
        let ratio = high / low;
        let expected = 1.1 / 0.9;
        assert!((ratio - expected).abs() < 1e-3,
            "forecast_high / forecast_low = {ratio}, expected ≈ {expected}");
    }
}

// ─── SRD-13d Phase 9 §4: wrapper smoke tests under
// materialised op-templates ─────────────────────────────────

/// Write `yaml` to a temporary file under
/// `target/test-tmp/<unique>/workload.yaml` so test workloads
/// can be authored inline. Returns (workload_path, session_dir).
fn write_inline_workload(name: &str, yaml: &str) -> (PathBuf, SessionDir) {
    let session = SessionDir::new();
    let workload_path = session.parent().join(format!("{name}.yaml"));
    std::fs::write(&workload_path, yaml).expect("write inline workload");
    (workload_path, session)
}

/// Conditional dispenser under a materialised op-template:
/// the op carries its own `bindings:` block (forcing per-op
/// kernel synthesis under Phase 9), and `if:` references one
/// of those op-local bindings. Verifies the wrapper resolves
/// its `PullHandle` against the op-template kernel's state
/// rather than the workload-root state — the op-local
/// `mod(cycle, 2)` binding must change per cycle so the
/// gating actually flips. We assert via the per-op skips
/// counter in metrics.db rather than parsing stdout, since
/// the adapter's bind-point rendering is on a different
/// resolve path than the wrapper pulls.
#[test]
fn conditional_under_materialised_op_template() {
    let yaml = r#"
phases:
  predict:
    cycles: 10
    ops:
      gated:
        adapter: stdout
        params:
          stdout: eventlog
        bindings: |
          // Op-local: forces materialisation of the op-template kernel.
          local_pred := mod(cycle, 2)
        if: local_pred
        stmt: "ran"
"#;
    let (path, session) = write_inline_workload("conditional_op_template", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    let output = cmd.output().expect("failed to run nbrs");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(output.status.success() && stderr.contains("done"),
        "workload did not complete:\nstderr: {stderr}\nstdout: {stdout}");
    // 10 cycles, only odd ones (1,3,5,7,9) have local_pred != 0,
    // so the conditional must fire skips_total == 5 and the op
    // must execute the other 5. If `cycle` weren't propagated
    // to the op-template kernel, local_pred would stay 0 for
    // every cycle and the op would skip every time (10 skips,
    // 0 executions).
    let conn = rusqlite::Connection::open(session.path.join("metrics.db"))
        .expect("open metrics.db");
    let count_for = |family: &str| -> i64 {
        conn.query_row(
            "SELECT s.count FROM metric_family f
                JOIN metric_instance i ON i.family_id = f.id
                JOIN sample_value s ON s.instance_id = i.id
              WHERE f.name = ?1 LIMIT 1",
            [family],
            |r| r.get::<_, i64>(0),
        ).unwrap_or(0)
    };
    let total = count_for("cycles_total");
    let skips = count_for("skips_total");
    assert_eq!(total, 10, "cycles_total = {total}, expected 10");
    assert_eq!(skips, 5,
        "skips_total = {skips}, expected 5 (odd cycles run, even skip)");
}

/// Throttle dispenser under a materialised op-template: the
/// op has its own `bindings:` declaring the delay binding.
/// Verifies the throttle wrapper reads the delay from the
/// op-template kernel's state per cycle. We check the value
/// *was* observed (not the wall-clock effect) by reading the
/// declared metric, since precise sleep timing is brittle.
#[test]
fn throttle_under_materialised_op_template() {
    let yaml = r#"
phases:
  predict:
    cycles: 4
    ops:
      delayed:
        adapter: stdout
        params:
          stdout: eventlog
        bindings: |
          // Op-local: forces materialisation. Delay scaled
          // small so the test stays fast — value verified
          // via the declared metric below, not wall clock.
          local_delay_ns := mod(cycle, 2)
        delay: local_delay_ns
        stmt: "ran cycle={cycle}"
        metrics:
          delay_witness:
            value: local_delay_ns
            kind: gauge
"#;
    let (path, session) = write_inline_workload("throttle_op_template", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    let output = cmd.output().expect("failed to run nbrs");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success() && stderr.contains("done"),
        "workload did not complete: {stderr}");
    let conn = rusqlite::Connection::open(session.path.join("metrics.db"))
        .expect("open metrics.db");
    // delay_witness must exist as a registered family — proving
    // the dispenser wrapping path saw the op-local binding.
    let row: Result<(String,), _> = conn.query_row(
        "SELECT type FROM metric_family WHERE name = ?1",
        ["delay_witness"],
        |r| Ok((r.get::<_, String>(0)?,)),
    );
    let (kind,) = row.expect("delay_witness family missing");
    assert_eq!(kind, "gauge");
}

/// Validation dispenser under a materialised op-template:
/// the op has its own `bindings:` and `verify:` block
/// referencing op-local wires. Verifies the validator
/// resolves its expected/observed handles against the
/// op-template kernel's state.
#[test]
fn validation_under_materialised_op_template() {
    let yaml = r#"
phases:
  predict:
    cycles: 4
    ops:
      checked:
        adapter: stdout
        params:
          stdout: eventlog
        bindings: |
          // Op-local: forces materialisation.
          local_doubled := mul(cycle, 2)
        stmt: "n={cycle}"
        verify:
          min_rows: 0
"#;
    let (path, session) = write_inline_workload("validation_op_template", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    let output = cmd.output().expect("failed to run nbrs");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success() && stderr.contains("done"),
        "workload did not complete: {stderr}");
}

// ─── SRD-71: cursor partitioning ────────────────────────────────

#[test]
fn cursor_over_narrows_to_first_percentage() {
    // Cursor declared `over cursor` with the operator passing
    // `cursor=0..10%` should narrow `range(0, 1000)` to
    // `[0, 100)` — emitting 100 cycles instead of 1000.
    let yaml = r#"
params:
  cursor: "0..100%"

phases:
  walk:
    concurrency: 1
    bindings: |
      cursor row = range(0, 1000) over cursor
      n := row
    ops:
      emit:
        adapter: stdout
        stmt: "row={n}"
"#;
    let (path, session) = write_inline_workload("cursor_over_pct", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    cmd.arg("cursor=0..10%");
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "workload failed: {stderr}");
    let count = stdout.lines().filter(|l| l.starts_with("row=")).count();
    assert_eq!(count, 100,
        "expected 100 narrowed rows from `cursor=0..10%`, got {count}.\nstdout:\n{stdout}");
}

#[test]
fn cursor_over_with_literal_ordinals() {
    // Literal-ordinal partition spec: `cursor=100..200` should
    // narrow `range(0, 1000)` to `[100, 200)` — 100 cycles
    // starting at row 100.
    let yaml = r#"
params:
  cursor: "0..100%"

phases:
  walk:
    concurrency: 1
    bindings: |
      cursor row = range(0, 1000) over cursor
      n := row
    ops:
      emit:
        adapter: stdout
        stmt: "row={n}"
"#;
    let (path, session) = write_inline_workload("cursor_over_literal", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    cmd.arg("cursor=100..200");
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "workload failed: {stderr}");
    let rows: Vec<u64> = stdout
        .lines()
        .filter_map(|l| l.strip_prefix("row="))
        .filter_map(|s| s.parse().ok())
        .collect();
    assert_eq!(rows.len(), 100, "expected 100 rows, got {}", rows.len());
    assert_eq!(*rows.iter().min().unwrap(), 100);
    assert_eq!(*rows.iter().max().unwrap(), 199);
}

#[test]
fn cursor_without_over_ignores_cursor_param() {
    // A cursor declared without `over` should be unaffected by
    // the `cursor=...` parameter — the cursor uses its full
    // declared extent even when the operator passes a narrowing
    // spec. The workload must still declare `cursor` in its
    // `params:` for the runtime to accept the CLI override at
    // all; this test verifies that even with the param set, the
    // cursor that doesn't opt in via `over` stays at full extent.
    let yaml = r#"
params:
  cursor: "0..100%"

phases:
  walk:
    concurrency: 1
    bindings: |
      cursor row = range(0, 50)
      n := row
    ops:
      emit:
        adapter: stdout
        stmt: "row={n}"
"#;
    let (path, session) = write_inline_workload("cursor_no_over", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    cmd.arg("cursor=0..10%");
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let count = stdout.lines().filter(|l| l.starts_with("row=")).count();
    assert_eq!(count, 50,
        "cursor without `over` must ignore `cursor=...`; got {count} rows");
}

#[test]
fn mod_in_maps_cycle_into_narrowed_partition() {
    // `mod_in(cycle, row.cursor)` maps cycle to an ordinal that
    // stays inside the cursor's narrowed range. With
    // `cursor=100..200` against `range(0, 1000)`, the cursor
    // narrows to [100, 200), and mod_in wraps `cycle` (0..100)
    // into that range — yielding 100, 101, ..., 199.
    let yaml = r#"
params:
  cursor: "0..100%"

phases:
  walk:
    concurrency: 1
    bindings: |
      cursor row = range(0, 1000) over cursor
      n := mod_in(cycle, row.cursor)
    ops:
      emit:
        adapter: stdout
        stmt: "n={n}"
"#;
    let (path, session) = write_inline_workload("cursor_mod_in", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    cmd.arg("cursor=100..200");
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "workload failed: {stderr}");
    let ns: Vec<u64> = stdout
        .lines()
        .filter_map(|l| l.strip_prefix("n="))
        .filter_map(|s| s.parse().ok())
        .collect();
    assert_eq!(ns.len(), 100, "expected 100 outputs, got {}", ns.len());
    assert_eq!(*ns.iter().min().unwrap(), 100);
    assert_eq!(*ns.iter().max().unwrap(), 199);
}

#[test]
fn comprehension_iterates_partition_list_per_partition() {
    // SRD-71 §"Comprehension iteration": a scenario-tree
    // `for: "p in <expr>"` over `partitions(...)` iterates
    // partition-by-partition. Each iteration's bound kernel
    // carries the per-partition `Partition` value to descendant
    // phases, where `mod_in(cycle, p)` (or `over p` on a cursor
    // decl) consumes it as an Ext-typed wire.
    let yaml = r#"
scenarios:
  sweep:
    - for: "p in partitions(\"linear:3\", 99)"
      phases:
        - walk

phases:
  walk:
    cycles: 5
    concurrency: 1
    bindings: |
      lo := start_of(p)
      hi := end_of(p)
      i := idx_of(p)
    ops:
      emit:
        adapter: stdout
        stmt: "part={i} lo={lo} hi={hi}"
"#;
    let (path, session) = write_inline_workload("partition_comprehension", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    cmd.arg("scenario=sweep");
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "workload failed: {stderr}");

    // 3 partitions × 5 cycles = 15 emit lines.
    let emit_lines: Vec<&str> = stdout
        .lines()
        .filter(|l| l.starts_with("part="))
        .collect();
    assert_eq!(emit_lines.len(), 15,
        "expected 3 partitions × 5 cycles = 15 emits, got {}.\nstdout:\n{stdout}",
        emit_lines.len());

    // Each partition emits 5 lines; the three partition indices
    // (0, 1, 2) all appear, and the lo/hi values are distinct
    // per partition.
    let parts_seen: std::collections::HashSet<&str> = emit_lines.iter()
        .filter_map(|l| l.split(' ').next())
        .collect();
    assert_eq!(parts_seen.len(), 3, "expected 3 distinct partition indices");
    assert!(parts_seen.contains("part=0"));
    assert!(parts_seen.contains("part=1"));
    assert!(parts_seen.contains("part=2"));

    // Partition 0 covers [0..33), partition 1 [33..66), partition 2 [66..99).
    let part0_line = emit_lines.iter().find(|l| l.starts_with("part=0 ")).unwrap();
    assert!(part0_line.contains("lo=0") && part0_line.contains("hi=33"),
        "partition 0 should be [0, 33), got: {part0_line}");
    let part2_line = emit_lines.iter().find(|l| l.starts_with("part=2 ")).unwrap();
    assert!(part2_line.contains("lo=66") && part2_line.contains("hi=99"),
        "partition 2 should be [66, 99), got: {part2_line}");
}

#[test]
fn cardinality_and_start_of_expose_partition_metadata() {
    // `cardinality(row.cursor)` and `start_of(row.cursor)` are
    // effectively-const for the activation — they should
    // produce the same value every cycle.
    let yaml = r#"
params:
  cursor: "0..100%"

phases:
  walk:
    cycles: 3
    concurrency: 1
    bindings: |
      cursor row = range(0, 1000) over cursor
      card := cardinality(row.cursor)
      lo := start_of(row.cursor)
    ops:
      emit:
        adapter: stdout
        stmt: "card={card} lo={lo}"
"#;
    let (path, session) = write_inline_workload("partition_metadata", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    cmd.arg("cursor=100..200");
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(output.status.success(), "workload failed: {stderr}");
    let lines: Vec<&str> = stdout.lines().filter(|l| l.contains("card=")).collect();
    assert!(!lines.is_empty(), "no card= lines emitted:\n{stdout}");
    for line in &lines {
        assert!(line.contains("card=100"),
            "expected card=100 (200-100), got: {line}");
        assert!(line.contains("lo=100"),
            "expected lo=100, got: {line}");
    }
}

#[test]
fn cursor_param_quote_elision_works_end_to_end() {
    // Quote elision on the CLI surface: `cursor='0..10%'` and
    // `'cursor=0..10%'` and `cursor="0..10%"` should all parse
    // identically and narrow the cursor to 10%.
    let yaml = r#"
params:
  cursor: "0..100%"

phases:
  walk:
    concurrency: 1
    bindings: |
      cursor row = range(0, 1000) over cursor
      n := row
    ops:
      emit:
        adapter: stdout
        stmt: "row={n}"
"#;
    let (path, session) = write_inline_workload("cursor_over_quoted", yaml);
    let mut cmd = nbrs(&session);
    cmd.arg(format!("workload={}", path.display()));
    cmd.arg("cursor='0..10%'");
    let output = cmd.output().expect("failed to run nbrs");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let count = stdout.lines().filter(|l| l.starts_with("row=")).count();
    assert_eq!(count, 100,
        "single-quoted cursor='0..10%' should narrow to 100 rows; got {count}");
}
