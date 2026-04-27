// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the per-level concurrent scheduler
//! (SRD 18b §"Scheduler abstraction").
//!
//! Wall-clock comparisons are too noisy on shared infrastructure,
//! so each test inspects stderr log ordering: with two latency-
//! bounded sibling phases, a serial run finishes the first phase
//! before announcing the second, while a concurrent run logs both
//! phases as in-flight before either completes. The latency-bound
//! ops give the scheduler enough wall-time slack that the entry
//! lines for both phases land before the completion lines under
//! any reasonable concurrent dispatch.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

fn nbrs() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nbrs"));
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap();
    cmd.current_dir(workspace_root);
    cmd
}

fn write_workload(label: &str, body: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    std::fs::create_dir_all(&dir)
        .unwrap_or_else(|e| panic!("create_dir_all {dir:?}: {e}"));
    dir.push(format!(
        "nbrs_sched_{label}_{}_{}.yaml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    ));
    let mut f = std::fs::File::create(&dir)
        .unwrap_or_else(|e| panic!("create {dir:?}: {e}"));
    f.write_all(body.as_bytes())
        .unwrap_or_else(|e| panic!("write {dir:?}: {e}"));
    dir
}

/// Two sibling phases. Each runs one op with a 1-second simulated
/// latency — enough to make concurrent dispatch observable in the
/// log stream but not so long it slows CI.
const TWO_PHASE_LATENCY_YAML: &str = r#"
scenarios:
  default:
    - left
    - right

phases:
  left:
    cycles: ===auto
    concurrency: 1
    ops:
      ping_left:
        stmt: ping_left
        result-latency: 1000
  right:
    cycles: ===auto
    concurrency: 1
    ops:
      ping_right:
        stmt: ping_right
        result-latency: 1000
"#;

fn run(workload: &std::path::Path, extra: &[&str]) -> (String, String, bool) {
    let mut cmd = nbrs();
    cmd.arg("run");
    cmd.arg(format!("workload={}", workload.display()));
    cmd.arg("tui=off");
    cmd.arg("adapter=testkit");
    for a in extra { cmd.arg(a); }
    let out = cmd.output().expect("failed to exec nbrs");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.success(),
    )
}

/// Find the line index of the first occurrence of `needle`.
fn line_of(stream: &str, needle: &str) -> Option<usize> {
    stream.lines().position(|l| l.contains(needle))
}

/// True iff both phases enter before either completes — the
/// concurrent-dispatch signature in the log stream.
fn both_in_flight(stream: &str) -> bool {
    let left_enter = line_of(stream, "=== phase: left ===");
    let right_enter = line_of(stream, "=== phase: right ===");
    let left_done = line_of(stream, "phase 'left' complete");
    let right_done = line_of(stream, "phase 'right' complete");
    let (Some(le), Some(re), Some(ld), Some(rd)) =
        (left_enter, right_enter, left_done, right_done) else { return false };
    let last_enter = le.max(re);
    let first_done = ld.min(rd);
    last_enter < first_done
}

#[test]
fn serial_baseline_does_not_overlap_phases() {
    let path = write_workload("serial", TWO_PHASE_LATENCY_YAML);
    let (stdout, stderr, ok) = run(&path, &[]);
    assert!(ok, "nbrs failed: stdout={stdout}\nstderr={stderr}");
    assert!(
        !both_in_flight(&stderr),
        "serial baseline must complete each phase before starting the next.\nstderr={stderr}",
    );
}

#[test]
fn schedule_unlimited_overlaps_sibling_phases() {
    let path = write_workload("unlimited", TWO_PHASE_LATENCY_YAML);
    let (stdout, stderr, ok) = run(&path, &["schedule=*"]);
    assert!(ok, "nbrs failed: stdout={stdout}\nstderr={stderr}");
    assert!(
        both_in_flight(&stderr),
        "schedule=* must dispatch both siblings before either completes.\nstderr={stderr}",
    );
}

#[test]
fn schedule_bounded_two_overlaps_two_siblings() {
    let path = write_workload("bounded2", TWO_PHASE_LATENCY_YAML);
    let (stdout, stderr, ok) = run(&path, &["schedule=2"]);
    assert!(ok, "nbrs failed: stdout={stdout}\nstderr={stderr}");
    assert!(
        both_in_flight(&stderr),
        "schedule=2 must dispatch both siblings before either completes.\nstderr={stderr}",
    );
}
