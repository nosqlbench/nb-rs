// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-71 cursor partitioning — end-to-end coverage tests.
//!
//! Each test runs one scenario from
//! `examples/workloads/cursors/cursor_partitions_coverage.yaml` and
//! asserts the expected behavior of one shape:
//!
//! - Form 1 single sub-range (number forms + bracket tolerance)
//! - Form 2 contiguous delta lists (pct / fraction / literal / mixed)
//! - Form 3 pre-baked recipes
//! - `over` clause shapes (workload-param, iter-var, cross-cursor)
//! - Stdlib partition functions
//! - Reified custom-named cursor parameters
//! - Cursor without `over` ignoring the parameter
//!
//! The workload YAML carries the operator-readable shape
//! vocabulary; the tests verify the behavior numerically.

use std::path::{Path, PathBuf};
use std::process::Command;

const WORKLOAD: &str = "examples/workloads/cursors/cursor_partitions_coverage.yaml";

struct SessionDir { path: PathBuf }

impl SessionDir {
    fn new() -> Self {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let parent = std::env::temp_dir()
            .join(format!("nbrs-cursor-partitions-{pid}-{nanos}"));
        std::fs::create_dir_all(&parent).expect("create session parent");
        Self { path: parent.join("session") }
    }
    fn parent(&self) -> &Path { self.path.parent().unwrap() }
}

impl Drop for SessionDir {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(self.parent()); }
}

fn run_scenario(scenario: &str, extra_args: &[&str]) -> (String, String, bool) {
    let session = SessionDir::new();
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nbrs"));
    cmd.current_dir(workspace_root)
        .arg("run")
        .arg("--session-path").arg(&session.path)
        .arg(format!("workload={WORKLOAD}"))
        .arg(format!("scenario={scenario}"));
    for a in extra_args { cmd.arg(a); }
    let out = cmd.output().expect("run nbrs");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    (stdout, stderr, out.status.success())
}

/// Collect lines that start with the given prefix. Useful for
/// filtering out informational stdout and counting / asserting
/// against the `cp/...` emits the workload phases produce.
fn lines_with_prefix(stdout: &str, prefix: &str) -> Vec<String> {
    stdout.lines()
        .filter(|l| l.starts_with(prefix))
        .map(|l| l.to_string())
        .collect()
}

/// Distinct lines with the given prefix. Form 1 scenarios emit
/// the same `lo=X hi=Y` repeatedly (one per cycle in the
/// narrowed range); the test asserts on the distinct content.
fn distinct_lines_with_prefix(stdout: &str, prefix: &str) -> Vec<String> {
    let mut lines: Vec<String> = lines_with_prefix(stdout, prefix);
    lines.sort();
    lines.dedup();
    lines
}

// ─────────────────────────────────────────────────────────────────
// Form 1: single sub-range
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_form1_percentage_end() {
    // `over "0..53%"` against range(0, 1000) → cursor [0, 530).
    let (stdout, stderr, ok) = run_scenario("form1_percentage_end", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/form1 "),
        vec!["cp/form1 lo=0 hi=530".to_string()]);
}

#[test]
fn cursor_partitions_form1_fraction_end() {
    // `over "0..0.53"` (fraction form) === `over "0..53%"`.
    let (stdout, stderr, ok) = run_scenario("form1_fraction_end", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/form1 "),
        vec!["cp/form1 lo=0 hi=530".to_string()]);
}

#[test]
fn cursor_partitions_form1_literal_ordinals() {
    // `over "100..500"` (bare integers) → cursor [100, 500).
    let (stdout, stderr, ok) = run_scenario("form1_literal_ordinals", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/form1 "),
        vec!["cp/form1 lo=100 hi=500".to_string()]);
}

#[test]
fn cursor_partitions_form1_mixed_literal_then_pct() {
    // `over "100..50%"` — literal start, percentage end.
    let (stdout, stderr, ok) = run_scenario("form1_mixed_literal_then_pct", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/form1 "),
        vec!["cp/form1 lo=100 hi=500".to_string()]);
}

#[test]
fn cursor_partitions_form1_mixed_frac_then_literal() {
    // `over "0.10..800"` — fraction start, literal end.
    let (stdout, stderr, ok) = run_scenario("form1_mixed_frac_then_literal", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/form1 "),
        vec!["cp/form1 lo=100 hi=800".to_string()]);
}

#[test]
fn cursor_partitions_form1_brackets_tolerated() {
    // `over "[0..53%)"` — bracket / closure markers stripped.
    let (stdout, stderr, ok) = run_scenario("form1_brackets_tolerated", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/form1 "),
        vec!["cp/form1 lo=0 hi=530".to_string()]);
}

// ─────────────────────────────────────────────────────────────────
// Form 2: delta lists
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_form2_pct_with_star() {
    // `partitions("2%,10%,*%")` against default extent 100 →
    //   [0..2), [2..12), [12..100).
    let (stdout, stderr, ok) = run_scenario("form2_pct_with_star", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=2",
        "cp/iter idx=1 lo=2 hi=12",
        "cp/iter idx=2 lo=12 hi=100",
    ]);
}

#[test]
fn cursor_partitions_form2_fraction_with_star() {
    // `partitions("0.02,0.10,*")` equivalent to percentage form.
    let (stdout, stderr, ok) = run_scenario("form2_fraction_with_star", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=2",
        "cp/iter idx=1 lo=2 hi=12",
        "cp/iter idx=2 lo=12 hi=100",
    ]);
}

#[test]
fn cursor_partitions_form2_literal_with_star() {
    // `partitions("1000,5000,*", 10000)` — literal ordinal deltas.
    let (stdout, stderr, ok) = run_scenario("form2_literal_with_star", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=1000",
        "cp/iter idx=1 lo=1000 hi=6000",
        "cp/iter idx=2 lo=6000 hi=10000",
    ]);
}

#[test]
fn cursor_partitions_form2_mixed_literal_pct_with_star() {
    // `partitions("1000,10%,*", 10000)` — first delta literal,
    // second a percentage of the extent (10% of 10000 = 1000),
    // third the remainder (8000).
    let (stdout, stderr, ok) = run_scenario("form2_mixed_literal_pct_with_star", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=1000",
        "cp/iter idx=1 lo=1000 hi=2000",
        "cp/iter idx=2 lo=2000 hi=10000",
    ]);
}

#[test]
fn cursor_partitions_form2_short_list_drops_gap() {
    // `partitions("20%,30%")` — no `*`, sum < 100% → trailing
    // 50% gap is dropped, only 2 partitions emitted.
    let (stdout, stderr, ok) = run_scenario("form2_short_list_drops_gap", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=20",
        "cp/iter idx=1 lo=20 hi=50",
    ]);
}

// ─────────────────────────────────────────────────────────────────
// Form 2 tail tokens: `...` fill and `*/N` split
// ─────────────────────────────────────────────────────────────────

/// The expected partitions for "first 90%, then the rest in ten
/// 1%-of-the-whole chunks" against the default extent 100 —
/// shared by the fill and split spellings, which must coincide
/// at head=90%.
fn ninety_ten_by_one() -> Vec<String> {
    let mut expected = vec!["cp/iter idx=0 lo=0 hi=90".to_string()];
    for i in 0..10u64 {
        expected.push(format!("cp/iter idx={} lo={} hi={}", i + 1, 90 + i, 91 + i));
    }
    expected.sort();
    expected
}

#[test]
fn cursor_partitions_form2_fill() {
    // `partitions("90%,1%,...")` — the fill token repeats the
    // 1% delta until the extent is used up → 11 partitions.
    let (stdout, stderr, ok) = run_scenario("form2_fill", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), ninety_ten_by_one());
}

#[test]
fn cursor_partitions_form2_star_split() {
    // `partitions("90%,*/10")` — the split token divides the
    // remainder into 10 → identical partitions to the fill
    // spelling at head=90%.
    let (stdout, stderr, ok) = run_scenario("form2_star_split", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), ninety_ten_by_one());
}

#[test]
fn cursor_partitions_form2_star_split_whole() {
    // `partitions("*/4")` — no head, so the remainder is the
    // whole extent: resolves exactly like `linear:4`.
    let (stdout, stderr, ok) = run_scenario("form2_star_split_whole", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=25",
        "cp/iter idx=1 lo=25 hi=50",
        "cp/iter idx=2 lo=50 hi=75",
        "cp/iter idx=3 lo=75 hi=100",
    ]);
}

#[test]
fn cursor_partitions_form2_fill_truncates() {
    // `partitions("3,2,...", 10)` — the final fill chunk
    // truncates at the extent (1 ordinal instead of 2); "until
    // used up" emits the short tail rather than dropping it.
    let (stdout, stderr, ok) = run_scenario("form2_fill_truncates", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=3",
        "cp/iter idx=1 lo=3 hi=5",
        "cp/iter idx=2 lo=5 hi=7",
        "cp/iter idx=3 lo=7 hi=9",
        "cp/iter idx=4 lo=9 hi=10",
    ]);
}

#[test]
fn cursor_partitions_form2_repetition() {
    // `partitions("90%,1%x10")` — finite repetition, the fourth
    // spelling of the 90/10 family: identical partitions to the
    // fill and split spellings.
    let (stdout, stderr, ok) = run_scenario("form2_repetition", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), ninety_ten_by_one());
}

#[test]
fn cursor_partitions_form2_gap() {
    // `partitions("10%,~80%,10%")` — the gap consumes 80% of
    // the extent without emitting; idx counts emitted
    // partitions only.
    let (stdout, stderr, ok) = run_scenario("form2_gap", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=10",
        "cp/iter idx=1 lo=90 hi=100",
    ]);
}

#[test]
fn cursor_partitions_form2_star_shaped() {
    // `partitions("50%,*/ratios:1,3", 1000)` — the remainder
    // (500 ordinals) shaped 25%/75% by the recipe weights.
    let (stdout, stderr, ok) = run_scenario("form2_star_shaped", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=500",
        "cp/iter idx=1 lo=500 hi=625",
        "cp/iter idx=2 lo=625 hi=1000",
    ]);
}

#[test]
fn cursor_partitions_windowed_chunking() {
    // `partitions("linear:4 in 20%..100%", 1000)` — the
    // chunking resolves against the window [200, 1000), so the
    // four chunks are 200 ordinals each starting at 200.
    let (stdout, stderr, ok) = run_scenario("windowed_chunking", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=200 hi=400",
        "cp/iter idx=1 lo=400 hi=600",
        "cp/iter idx=2 lo=600 hi=800",
        "cp/iter idx=3 lo=800 hi=1000",
    ]);
}

#[test]
fn cursor_partitions_order_largest_first() {
    // `partitions("fib:5 largest_first")` — iteration runs the
    // biggest partition first; idx keeps identifying the
    // generation position, so the emitted idx sequence is 4..0.
    // Order matters here, so this uses the order-preserving
    // helper.
    let (stdout, stderr, ok) = run_scenario("order_largest_first", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=4 lo=58 hi=100",
        "cp/iter idx=3 lo=32 hi=58",
        "cp/iter idx=2 lo=16 hi=32",
        "cp/iter idx=1 lo=5 hi=16",
        "cp/iter idx=0 lo=0 hi=5",
    ]);
}

#[test]
fn cursor_partitions_order_random_is_deterministic() {
    // `partitions("linear:4 random")` — a deterministic
    // shuffle: same spec, same order, every run.
    let (stdout, stderr, ok) = run_scenario("order_random", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let first = lines_with_prefix(&stdout, "cp/iter ");
    assert_eq!(first.len(), 4, "four partitions: {first:?}");
    let mut sorted = first.clone();
    sorted.sort();
    assert_eq!(sorted, vec![
        "cp/iter idx=0 lo=0 hi=25",
        "cp/iter idx=1 lo=25 hi=50",
        "cp/iter idx=2 lo=50 hi=75",
        "cp/iter idx=3 lo=75 hi=100",
    ], "shuffle is a permutation of the same partitions");
    let (stdout2, stderr2, ok2) = run_scenario("order_random", &[]);
    assert!(ok2, "second run failed: {stderr2}");
    assert_eq!(lines_with_prefix(&stdout2, "cp/iter "), first,
        "random order must be deterministic across runs");
}

// ─────────────────────────────────────────────────────────────────
// Form 3: pre-baked recipes
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_recipe_linear() {
    // `linear:4` → 4 uniform quarter-partitions [0..25), [25..50),
    // [50..75), [75..100).
    let (stdout, stderr, ok) = run_scenario("recipe_linear", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=25",
        "cp/iter idx=1 lo=25 hi=50",
        "cp/iter idx=2 lo=50 hi=75",
        "cp/iter idx=3 lo=75 hi=100",
    ]);
}

#[test]
fn cursor_partitions_recipe_ratios() {
    // `ratios:1,1,2` (sum 4) → 25%, 25%, 50%.
    let (stdout, stderr, ok) = run_scenario("recipe_ratios", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=25",
        "cp/iter idx=1 lo=25 hi=50",
        "cp/iter idx=2 lo=50 hi=100",
    ]);
}

#[test]
fn cursor_partitions_recipe_bin() {
    // `bin:5` → C(4, 0..4) = [1, 4, 6, 4, 1], sum 16 →
    // [6.25%, 25%, 37.5%, 25%, 6.25%].
    let (stdout, stderr, ok) = run_scenario("recipe_bin", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=6",
        "cp/iter idx=1 lo=6 hi=31",
        "cp/iter idx=2 lo=31 hi=69",
        "cp/iter idx=3 lo=69 hi=94",
        "cp/iter idx=4 lo=94 hi=100",
    ]);
}

#[test]
fn cursor_partitions_recipe_fib() {
    // `fib:5` → distinct fib (skipping leading 1,1) = [1, 2, 3, 5, 8].
    let (stdout, stderr, ok) = run_scenario("recipe_fib", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=5",
        "cp/iter idx=1 lo=5 hi=16",
        "cp/iter idx=2 lo=16 hi=32",
        "cp/iter idx=3 lo=32 hi=58",
        "cp/iter idx=4 lo=58 hi=100",
    ]);
}

#[test]
fn cursor_partitions_recipe_ln() {
    // `ln:5` → monotonically increasing weights from ln(2)..ln(6).
    let (stdout, stderr, ok) = run_scenario("recipe_ln", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let lines = distinct_lines_with_prefix(&stdout, "cp/iter ");
    assert_eq!(lines.len(), 5, "ln:5 should produce 5 partitions");
    // Last partition ends at 100% (no trailing gap because
    // sum-of-deltas equals extent exactly).
    assert!(lines.last().unwrap().contains("hi=100"),
        "last partition should reach 100, got: {lines:?}");
}

#[test]
fn cursor_partitions_recipe_mul_decay() {
    // `mul:0.5` (decay) terminates when current < start*0.001 —
    // 11 terms (1, 0.5, 0.25, …, 1/1024 ≈ 0.00098).
    let (stdout, stderr, ok) = run_scenario("recipe_mul_decay", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let lines = distinct_lines_with_prefix(&stdout, "cp/count ");
    assert_eq!(lines.len(), 11, "mul:0.5 decay should produce 11 partitions");
}

#[test]
fn cursor_partitions_recipe_mul_with_start() {
    // `mul:5,0.5` — same termination rule, different starting weight.
    let (stdout, stderr, ok) = run_scenario("recipe_mul_with_start", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let lines = distinct_lines_with_prefix(&stdout, "cp/count ");
    assert_eq!(lines.len(), 11);
}

#[test]
fn cursor_partitions_recipe_geom() {
    // `geom:4,2` → fixed 4 terms: 1, 2, 4, 8 (sum 15).
    let (stdout, stderr, ok) = run_scenario("recipe_geom", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=7",
        "cp/iter idx=1 lo=7 hi=20",
        "cp/iter idx=2 lo=20 hi=47",
        "cp/iter idx=3 lo=47 hi=100",
    ]);
}

#[test]
fn cursor_partitions_recipe_zipf() {
    // `zipf:1,4` → Zipfian weights 1/1, 1/2, 1/3, 1/4. Test
    // asserts only on partition count; exact boundaries are
    // numerically sensitive across libm versions.
    let (stdout, stderr, ok) = run_scenario("recipe_zipf", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/count ").len(), 4);
}

#[test]
fn cursor_partitions_recipe_pareto() {
    // `pareto:1,4` → Pareto-distributed weights (1/n)^1 for n=1..4.
    let (stdout, stderr, ok) = run_scenario("recipe_pareto", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/count ").len(), 4);
}

#[test]
fn cursor_partitions_recipe_front_heavy() {
    // `front_heavy:4` → 4, 3, 2, 1 (sum 10). 40%, 30%, 20%, 10%.
    let (stdout, stderr, ok) = run_scenario("recipe_front_heavy", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=40",
        "cp/iter idx=1 lo=40 hi=70",
        "cp/iter idx=2 lo=70 hi=90",
        "cp/iter idx=3 lo=90 hi=100",
    ]);
}

#[test]
fn cursor_partitions_recipe_back_heavy() {
    // `back_heavy:4` → 1, 2, 3, 4 (sum 10). 10%, 20%, 30%, 40%.
    let (stdout, stderr, ok) = run_scenario("recipe_back_heavy", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/iter "), vec![
        "cp/iter idx=0 lo=0 hi=10",
        "cp/iter idx=1 lo=10 hi=30",
        "cp/iter idx=2 lo=30 hi=60",
        "cp/iter idx=3 lo=60 hi=100",
    ]);
}

// ─────────────────────────────────────────────────────────────────
// `over` clause shapes
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_over_workload_param() {
    // `cursor q = range(0, 1000) over cursor` + CLI cursor=0..50%
    // narrows to [0, 500).
    let (stdout, stderr, ok) = run_scenario("over_workload_param", &["cursor=0..50%"]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/over_cursor "),
        vec!["cp/over_cursor lo=0 hi=500".to_string()]);
}

#[test]
fn cursor_partitions_phase_scoped_override_exact() {
    // SRD 71 P3: `<phase>.cursor=<spec>` overrides the param for
    // that phase only, beating the workload-wide `cursor=`.
    let (stdout, stderr, ok) = run_scenario(
        "over_workload_param",
        &["cursor=0..50%", "over_cursor_phase.cursor=0..25%"],
    );
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/over_cursor "),
        vec!["cp/over_cursor lo=0 hi=250".to_string()]);
}

#[test]
fn cursor_partitions_phase_scoped_override_glob() {
    // Glob form: `over_*.cursor=` matches `over_cursor_phase`.
    let (stdout, stderr, ok) = run_scenario(
        "over_workload_param",
        &["over_*.cursor=0..10%"],
    );
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/over_cursor "),
        vec!["cp/over_cursor lo=0 hi=100".to_string()]);
}

#[test]
fn cursor_partitions_phase_override_never_matching_is_startup_error() {
    // A pattern that matches no phase is a typo, not a no-op.
    let (stdout, stderr, ok) = run_scenario(
        "over_workload_param",
        &["nosuch_*.cursor=0..10%"],
    );
    assert!(!ok, "never-matching override pattern must fail at startup");
    let combined = format!("{stdout}\n{stderr}");
    assert!(combined.contains("matches no phase"), "diagnostic: {combined}");
}

#[test]
fn cursor_partitions_phase_override_ambiguous_globs_fatal() {
    // Two distinct globs matching the same phase for the same
    // param is ambiguous.
    let (stdout, stderr, ok) = run_scenario(
        "over_workload_param",
        &["over_*.cursor=0..10%", "*_phase.cursor=0..20%"],
    );
    assert!(!ok, "ambiguous globs must fail");
    let combined = format!("{stdout}\n{stderr}");
    assert!(combined.contains("ambiguous"), "diagnostic: {combined}");
}

#[test]
fn cursor_partitions_over_param_multi_partition_is_startup_error() {
    // SRD 71 §"Single-partition / no-iteration form": a cursor
    // consuming a multi-partition spec directly (no enclosing
    // `for:` iteration) is a startup error naming the missing
    // iteration — never a silent partition-0 run.
    let (stdout, stderr, ok) = run_scenario("over_workload_param", &["cursor=2%,10%,*"]);
    assert!(!ok, "multi-partition spec on a direct `over` must fail");
    let combined = format!("{stdout}\n{stderr}");
    assert!(combined.contains("for:"),
        "diagnostic should point at the `for:` iteration form: {combined}");
    assert!(combined.contains("partitions"),
        "diagnostic should name the partition list: {combined}");
}

#[test]
fn cursor_partitions_over_iter_var() {
    // `over p` where `p` is bound by an outer for-clause.
    // Three partitions (linear:3 against extent 1000) → three
    // per-iteration cursor narrowings.
    let (stdout, stderr, ok) = run_scenario("over_iter_var", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/over_p "), vec![
        "cp/over_p idx=0 lo=0 hi=333",
        "cp/over_p idx=1 lo=333 hi=667",
        "cp/over_p idx=2 lo=667 hi=1000",
    ]);
}

#[test]
fn cursor_partitions_over_cross_cursor() {
    // `cursor q2 = range(...) over q1.cursor` — q2 reads q1's
    // resolved partition. Both cursors narrow identically.
    let (stdout, stderr, ok) = run_scenario("over_cross_cursor", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/cross "), vec![
        "cp/cross idx=0 q1=[0..500) q2=[0..500)",
        "cp/cross idx=1 q1=[500..1000) q2=[500..1000)",
    ]);
}

// ─────────────────────────────────────────────────────────────────
// Stdlib partition functions
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_fn_cardinality() {
    // `linear:3` against extent 300 → each partition has cardinality 100.
    let (stdout, stderr, ok) = run_scenario("fn_cardinality", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let lines = distinct_lines_with_prefix(&stdout, "cp/fn cardinality ");
    assert_eq!(lines.len(), 3);
    for line in &lines {
        assert!(line.contains("n=100"), "expected cardinality 100, got: {line}");
    }
}

#[test]
fn cursor_partitions_fn_idx_and_bounds() {
    // `linear:4` against default extent 100 → idx 0..3, bounds in 25-step quarters.
    let (stdout, stderr, ok) = run_scenario("fn_idx_and_bounds", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/fn bounds "), vec![
        "cp/fn bounds idx=0 lo=0 hi=25",
        "cp/fn bounds idx=1 lo=25 hi=50",
        "cp/fn bounds idx=2 lo=50 hi=75",
        "cp/fn bounds idx=3 lo=75 hi=100",
    ]);
}

#[test]
fn cursor_partitions_fn_mod_in() {
    // `mod_in(cycle, p)` where p is the sole linear:1 partition
    // [0..100). cycle 0..4 maps to 0, 1, 2, 3, 4 (no wrap).
    let (stdout, stderr, ok) = run_scenario("fn_mod_in", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/fn mod_in "), vec![
        "cp/fn mod_in v=0",
        "cp/fn mod_in v=1",
        "cp/fn mod_in v=2",
        "cp/fn mod_in v=3",
        "cp/fn mod_in v=4",
    ]);
}

#[test]
fn cursor_partitions_fn_at() {
    // `at(p, cycle)` where p is [0..100). cycle 0..2 → 0, 1, 2.
    let (stdout, stderr, ok) = run_scenario("fn_at", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/fn at "), vec![
        "cp/fn at v=0",
        "cp/fn at v=1",
        "cp/fn at v=2",
    ]);
}

#[test]
fn cursor_partitions_fn_clamp_in() {
    // `clamp_in(cycle, p)` where p is [0..100). cycle 0..4 are
    // all inside the range → no saturation, just pass-through.
    let (stdout, stderr, ok) = run_scenario("fn_clamp_in", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/fn clamp_in "), vec![
        "cp/fn clamp_in v=0",
        "cp/fn clamp_in v=1",
        "cp/fn clamp_in v=2",
        "cp/fn clamp_in v=3",
        "cp/fn clamp_in v=4",
    ]);
}

#[test]
fn cursor_partitions_fn_random_in() {
    // `random_in(p, cycle)` where p is [0..100): every value
    // lands inside the partition, and the mapping is a hash —
    // a second run reproduces the same sequence.
    let (stdout, stderr, ok) = run_scenario("fn_random_in", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let lines = lines_with_prefix(&stdout, "cp/fn random_in ");
    assert_eq!(lines.len(), 5, "five cycles → five values: {lines:?}");
    for line in &lines {
        let v: u64 = line.rsplit("v=").next().unwrap().trim().parse()
            .unwrap_or_else(|e| panic!("unparseable value in `{line}`: {e}"));
        assert!(v < 100, "random_in escaped the partition: {line}");
    }
    let (stdout2, stderr2, ok2) = run_scenario("fn_random_in", &[]);
    assert!(ok2, "second run failed: {stderr2}");
    assert_eq!(lines_with_prefix(&stdout2, "cp/fn random_in "), lines,
        "random_in must be deterministic per seed");
}

#[test]
fn cursor_partitions_dotted_scalar_projections() {
    // SRD 71 §"Cursor metadata wires": `q.cursor.idx`,
    // `.partition_count`, `.start_ordinal`, `.end_ordinal`
    // resolve as typed scalar wires in bindings AND in `{...}`
    // text interpolation (the `ord=` field reads the dotted
    // name directly in the op template).
    let (stdout, stderr, ok) = run_scenario("dotted_projections", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/dotted "), vec![
        "cp/dotted i=0 n=3 lo=0 hi=333 ord=333",
        "cp/dotted i=1 n=3 lo=333 hi=667 ord=667",
        "cp/dotted i=2 n=3 lo=667 hi=1000 ord=1000",
    ]);
}

#[test]
fn cursor_partitions_fn_subdivide() {
    // `subdivide(p, 4)` over [0..100) → a PartitionList of four
    // quarter-partitions, boundary-identical to the `*/4` spec
    // token.
    let (stdout, stderr, ok) = run_scenario("fn_subdivide", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/fn subdivide "), vec![
        "cp/fn subdivide subs=PartitionList[4]=[0..25),[25..50),[50..75),[75..100)",
    ]);
}

// ─────────────────────────────────────────────────────────────────
// Partition-bound open-extent cursors
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_open_extent_capped_by_partition() {
    // `until_passes(10, 5)` over partition [0, 25): the policy
    // wants 5 passes (50 cycles), the partition caps it at 25 —
    // the source terminates the moment the partition is
    // exhausted, per SRD 71 §"Interaction with existing cursor
    // surface".
    let (stdout, stderr, ok) = run_scenario("open_extent_over", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let lines = lines_with_prefix(&stdout, "cp/open_extent ");
    assert_eq!(lines.len(), 25, "exactly the partition's cardinality: {lines:?}");
    // Ordinals stay inside the partition.
    for line in &lines {
        let n: u64 = line.rsplit("n=").next().unwrap().trim().parse()
            .unwrap_or_else(|e| panic!("unparseable `{line}`: {e}"));
        assert!(n < 25, "ordinal escaped the partition: {line}");
    }
}

// ─────────────────────────────────────────────────────────────────
// Status banner — `partition i/n [lo..hi)`
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_status_banner_on_iteration() {
    // SRD 71 §"Status / report integration": each iteration of a
    // multi-partition sweep announces its active partition
    // (1-based for display) with the condensed effective range.
    let (stdout, stderr, ok) = run_scenario("over_iter_var", &[]);
    assert!(ok, "scenario failed: {stderr}");
    let combined = format!("{stdout}\n{stderr}");
    for needle in [
        "partition 1/3 [0..333)",
        "partition 2/3 [333..667)",
        "partition 3/3 [667..1000)",
    ] {
        assert!(combined.contains(needle),
            "missing banner `{needle}` in output:\n{combined}");
    }
}

#[test]
fn cursor_partitions_status_banner_suppressed_for_single() {
    // A single-partition spec (no iteration) stays silent — no
    // `partition 1/1` noise.
    let (stdout, stderr, ok) = run_scenario("over_workload_param", &["cursor=0..50%"]);
    assert!(ok, "scenario failed: {stderr}");
    let combined = format!("{stdout}\n{stderr}");
    assert!(!combined.contains("partition 1/1"),
        "single-partition runs must not emit the banner:\n{combined}");
}

// ─────────────────────────────────────────────────────────────────
// Nested subdivision — `subdivide(outer, n)` as a source
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_nested_subdivide_source() {
    // Outer `partitions("50%,*", 1000)` → two windows; each
    // subdivides into 2 → four leaf runs with contiguous
    // 250-ordinal bounds. The inner clause resolves `outer`
    // through the kernel's scope chain.
    let (stdout, stderr, ok) = run_scenario("hierarchical_sweep", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/nested "), vec![
        "cp/nested outer=0 inner=0 lo=0 hi=250",
        "cp/nested outer=0 inner=1 lo=250 hi=500",
        "cp/nested outer=1 inner=0 lo=500 hi=750",
        "cp/nested outer=1 inner=1 lo=750 hi=1000",
    ]);
}

// ─────────────────────────────────────────────────────────────────
// Reified custom-named cursor parameters
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_reified_warmup_steady() {
    // Workload declares `warmup_cursor: "0..10%"` and
    // `steady_cursor: "10%..100%"`. Each phase's cursor names
    // its corresponding parameter; the two are independently
    // controlled. Operator sees `warmup_cursor=` / `steady_cursor=`
    // as the public surface.
    let (stdout, stderr, ok) = run_scenario("reified_warmup_steady", &[]);
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/warmup "),
        vec!["cp/warmup lo=0 hi=100".to_string()]);
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/steady "),
        vec!["cp/steady lo=100 hi=1000".to_string()]);
}

#[test]
fn cursor_partitions_reified_warmup_steady_with_cli_override() {
    // Operator overrides `warmup_cursor=0..1%` — cursor narrows
    // to [0, 10). `steady_cursor` keeps its workload default.
    let (stdout, stderr, ok) = run_scenario(
        "reified_warmup_steady",
        &["warmup_cursor=0..1%"],
    );
    assert!(ok, "scenario failed: {stderr}");
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/warmup "),
        vec!["cp/warmup lo=0 hi=10".to_string()]);
    assert_eq!(distinct_lines_with_prefix(&stdout, "cp/steady "),
        vec!["cp/steady lo=100 hi=1000".to_string()]);
}

// ─────────────────────────────────────────────────────────────────
// Negative: cursor without `over` ignores cursor=...
// ─────────────────────────────────────────────────────────────────

#[test]
fn cursor_partitions_no_over_ignores_cursor_param() {
    // Phase's cursor declared without `over` should keep its
    // full extent (50) regardless of the CLI `cursor=` value.
    let (stdout, stderr, ok) = run_scenario(
        "no_over_ignores_cursor_param",
        &["cursor=0..10%"],
    );
    assert!(ok, "scenario failed: {stderr}");
    // 50 emits because the cursor's full extent is 50 ordinals.
    assert_eq!(lines_with_prefix(&stdout, "cp/no_over ").len(), 50,
        "expected 50 emits (cursor's full extent), got {}",
        lines_with_prefix(&stdout, "cp/no_over ").len());
}
