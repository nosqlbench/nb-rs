// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-83 stop-condition coverage across execution shell levels.
//!
//! Each test runs one scenario from
//! `examples/workloads/controls/stop_conditions_coverage.yaml` and asserts that
//! a stop condition trips when a time-evolving runtime-state wire
//! crosses its threshold:
//!
//! - **Phase shell** — `op_count` (`phase_op_count`) and `elapsed_ms`
//!   (`phase_elapsed_ms`): the activity drain loop re-snapshots these
//!   every ~5 ms, so a `> N` predicate fails the phase the moment the
//!   live value crosses it — well before the phase's 600 cycles.
//! - **Workload shell** — `children_done` (`workload_children`): the
//!   walker re-snapshots the child-phase aggregate after each finished
//!   phase, so `children_done >= 2` halts the remaining walk after two
//!   phases, skipping the third.
//!
//! A phase-level trip fails the phase (non-zero exit + reason on
//! stderr); the workload-level trip halts the walk gracefully (zero
//! exit, later phase never dispatched). The two-axis `Outcome` effect
//! mapping is a later SRD-83 step.
//!
//! The harness runs every scenario with cwd set to a throwaway sandbox
//! AND an explicit `--session-path` under it, so a test never writes
//! session state into the project root (per `feedback_tests_no_project_root`).

use std::path::{Path, PathBuf};
use std::process::Command;

const WORKLOAD: &str = "examples/workloads/controls/stop_conditions_coverage.yaml";

/// A throwaway session sandbox under the project tmpdir
/// (`target/test-tmp` via `.cargo/config.toml`), removed on drop.
struct Sandbox {
    dir: PathBuf,
}

impl Sandbox {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir()
            .join(format!("nbrs-stopcond-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create sandbox");
        Self { dir }
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Run one scenario in its own sandbox. The cwd IS the sandbox (never
/// the project root), the workload is referenced by absolute path, and
/// the session lands under the sandbox via `--session-path` — so
/// nothing touches the repo's `./sessions` (which a concurrently
/// running real session may own).
fn run_scenario(scenario: &str) -> (String, String, bool) {
    let sandbox = Sandbox::new(scenario);
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let workload = workspace_root.join(WORKLOAD);
    let session = sandbox.dir.join("session");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nbrs"));
    cmd.current_dir(&sandbox.dir)
        .arg("run")
        .arg(format!("workload={}", workload.display()))
        .arg(format!("scenario={scenario}"))
        .arg("tui=off")
        .arg("--session-path")
        .arg(&session);
    let out = cmd.output().expect("run nbrs");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.success(),
    )
}

fn count_lines(stdout: &str, needle: &str) -> usize {
    stdout.lines().filter(|l| l.trim() == needle).count()
}

// ─── Phase shell ──────────────────────────────────────────────────

/// `op_count > 25` over a 600-cycle phase trips at ~op 26, failing the
/// phase far short of 600. The op-count at stop equals the number of
/// `OP_TICK` lines emitted.
#[test]
fn phase_cycles_total_trips_and_fails() {
    let (stdout, stderr, ok) = run_scenario("phase_cycles_total");
    assert!(!ok, "a tripped phase stop condition must fail the run; stderr:\n{stderr}");
    let ticks = count_lines(&stdout, "OP_TICK");
    // Crossed the threshold (> 25 ⇒ at least 26 ops ran) but stopped
    // far short of the 600-cycle ceiling. The loose upper bound
    // absorbs drain-loop timing slop without flaking.
    assert!(ticks > 25, "expected the phase to pass cycles_total=25 before stopping, got {ticks}");
    assert!(ticks < 300, "expected an early stop, not ~600 ops; got {ticks}\nstderr:\n{stderr}");
    assert!(stderr.contains("stop condition tripped"),
        "expected the stop-condition reason on stderr:\n{stderr}");
    assert!(stderr.contains("cycles_total > 25"),
        "expected the tripping predicate in the reason:\n{stderr}");
}

/// `elapsed_ms > 150` trips on the phase wall clock, not an op count —
/// the trigger is time, so the phase stops after ~150 ms regardless of
/// how many ops that turned out to be.
#[test]
fn phase_elapsed_ms_trips_on_wall_clock() {
    let (stdout, stderr, ok) = run_scenario("phase_elapsed_ms");
    assert!(!ok, "a tripped phase stop condition must fail the run; stderr:\n{stderr}");
    let ticks = count_lines(&stdout, "TIME_TICK");
    assert!(ticks > 0, "the phase should have run some ops before the 150ms stop");
    assert!(ticks < 400, "expected an early time-based stop, got {ticks} ops\nstderr:\n{stderr}");
    assert!(stderr.contains("stop condition tripped"),
        "expected the stop-condition reason on stderr:\n{stderr}");
    assert!(stderr.contains("elapsed_ms > 150"),
        "expected the tripping predicate in the reason:\n{stderr}");
}

// ─── Workload shell ───────────────────────────────────────────────

/// `children_done >= 2` (declared `each: workload`) halts the walk
/// after two phases complete: STEP_A and STEP_B run, STEP_C is never
/// dispatched, and — because nothing FAILED — the session exits zero.
#[test]
fn workload_children_done_halts_remaining_walk() {
    let (stdout, stderr, ok) = run_scenario("workload_children");
    assert!(ok, "a graceful workload-shell stop should exit zero; stderr:\n{stderr}");
    assert_eq!(count_lines(&stdout, "STEP_A"), 2,
        "step_a should run fully; stdout:\n{stdout}");
    assert_eq!(count_lines(&stdout, "STEP_B"), 2,
        "step_b should run fully; stdout:\n{stdout}");
    assert_eq!(count_lines(&stdout, "STEP_C"), 0,
        "step_c must be skipped once children_done reaches 2; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stderr.contains("workload stop condition tripped"),
        "expected the workload-shell stop log:\n{stderr}");
    assert!(stderr.contains("children_done >= 2"),
        "expected the tripping predicate in the reason:\n{stderr}");
}
