// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-83 — workload-shell stop-condition evaluation, end to end.
//!
//! The workload is SRD-82's outermost execution shell: as each child
//! phase finishes, the executor folds its outcome into the shell's
//! runtime-state aggregate (`children_*`, `op_count`, `error_count`)
//! and evaluates the workload's `stop_when:` predicates. The first
//! trip latches a walk-stop that every dispatch loop consults before
//! starting the next sibling — halting the remaining walk.
//!
//! These tests drive the `testkit` adapter (which echoes each op's
//! `stmt` to stdout) so the presence / absence of a phase's marker in
//! stdout is a direct witness of whether that phase ran.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

// ─── Harness ──────────────────────────────────────────────────────

fn nbrs() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nbrs"));
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap();
    cmd.current_dir(workspace_root);
    cmd
}

fn write_workload(label: &str, body: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    // `.cargo/config.toml` redirects TMPDIR to `target/test-tmp/`,
    // which cargo doesn't create — make it on demand.
    std::fs::create_dir_all(&dir)
        .unwrap_or_else(|e| panic!("create_dir_all {dir:?}: {e}"));
    dir.push(format!("nbrs_wlshell_{label}_{}.yaml", std::process::id()));
    let mut f = std::fs::File::create(&dir)
        .unwrap_or_else(|e| panic!("create {dir:?}: {e}"));
    f.write_all(body.as_bytes())
        .unwrap_or_else(|e| panic!("write {dir:?}: {e}"));
    dir
}

/// Run `nbrs run workload=... <extra>` with a per-invocation
/// `--session-path` (so parallel tests don't race on the session dir)
/// and return (stdout, stderr, success).
fn run(workload: &Path, extra: &[&str]) -> (String, String, bool) {
    let session_parent = std::env::temp_dir().join(format!(
        "nbrs-wlshell-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    ));
    std::fs::create_dir_all(&session_parent).expect("create session parent");
    let session_path = session_parent.join("session");
    let mut cmd = nbrs();
    cmd.arg("run");
    cmd.arg(format!("workload={}", workload.display()));
    cmd.arg("tui=off");
    cmd.arg("--session-path");
    cmd.arg(&session_path);
    for a in extra {
        cmd.arg(a);
    }
    let out = cmd.output().expect("failed to exec nbrs");
    let _ = std::fs::remove_dir_all(&session_parent);
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.success(),
    )
}

// ─── Tests ────────────────────────────────────────────────────────

/// A workload-level `stop_when:` (`each: workload`) halts the
/// remaining walk after the condition trips on the aggregate.
///
/// `phase_one` runs 10 ops; at its completion the workload aggregate
/// `op_count` is 10, which trips `op_count > 5`, latching the
/// walk-stop. `phase_two` is then never dispatched — its marker is
/// absent from stdout. This is a *graceful* stop (no phase failed), so
/// the only thing that can have skipped `phase_two` is the workload
/// shell — the `Err`-cascade stop-on-error path can't, because nothing
/// errored.
#[test]
fn workload_stop_when_halts_later_phase() {
    let wl = write_workload("opcount", r#"
stop_when:
  - when: "op_count > 5"
    each: workload
phases:
  phase_one:
    cycles: 10
    concurrency: 1
    ops:
      mark:
        stmt: "PHASE_ONE_OP"
  phase_two:
    cycles: 10
    concurrency: 1
    ops:
      mark:
        stmt: "PHASE_TWO_OP"
scenarios:
  default: [phase_one, phase_two]
"#);
    let (stdout, stderr, _ok) = run(&wl, &["adapter=testkit"]);

    let p1 = stdout.lines().filter(|l| l.trim() == "PHASE_ONE_OP").count();
    let p2 = stdout.lines().filter(|l| l.trim() == "PHASE_TWO_OP").count();
    assert_eq!(p1, 10,
        "phase_one should have run all 10 ops; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert_eq!(p2, 0,
        "phase_two must NOT run — the workload stop condition should have \
         halted the walk; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stderr.contains("workload stop condition tripped"),
        "expected the workload-shell stop log; stderr:\n{stderr}");
}

/// Without a tripping `stop_when:`, every phase runs — the shell folds
/// outcomes into its aggregate but never latches the walk-stop.
#[test]
fn no_trip_runs_all_phases() {
    let wl = write_workload("notrip", r#"
stop_when:
  - when: "op_count > 1000000"
    each: workload
phases:
  phase_one:
    cycles: 4
    concurrency: 1
    ops:
      mark:
        stmt: "PHASE_ONE_OP"
  phase_two:
    cycles: 4
    concurrency: 1
    ops:
      mark:
        stmt: "PHASE_TWO_OP"
scenarios:
  default: [phase_one, phase_two]
"#);
    let (stdout, stderr, ok) = run(&wl, &["adapter=testkit"]);
    assert!(ok, "run should succeed; stderr:\n{stderr}");

    let p1 = stdout.lines().filter(|l| l.trim() == "PHASE_ONE_OP").count();
    let p2 = stdout.lines().filter(|l| l.trim() == "PHASE_TWO_OP").count();
    assert_eq!(p1, 4, "phase_one should run; stdout:\n{stdout}");
    assert_eq!(p2, 4,
        "phase_two should run — the condition never trips; stdout:\n{stdout}");
    assert!(!stderr.contains("workload stop condition tripped"),
        "no stop should have fired; stderr:\n{stderr}");
}
