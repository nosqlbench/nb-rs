// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration test: metrics frame → channel → RunState update.

use std::time::{Duration, Instant};

use nbrs_metrics::labels::Labels;
use nbrs_metrics::snapshot::MetricSet;
use nbrs_tui::state::RunState;
use nbrs_tui::reporter::TuiReporter;

#[test]
fn run_state_tracks_phase_lifecycle() {
    // Exercise the RunState methods directly. The actor's
    // command-driven mutation is tested separately in
    // `actor_publishes_snapshots`; this test just checks that
    // the phase-lifecycle helpers do what they say.
    let mut s = RunState::new("test.yaml", "repro", "stdout");

    // No pre-mapped tree here, so lifecycle routes by name via the
    // runtime-materialized fallback (`scene_node_id = 0` is the root,
    // never a Phase node, so `resolve_phase_node` falls through to
    // `find_phase`). SRD-100 P1c.
    s.set_phase_running(0, "schema", "", 4);
    assert_eq!(s.phases.len(), 1);
    assert_eq!(s.phases[0].name, "schema");
    assert!(matches!(s.phases[0].status, nbrs_tui::state::PhaseStatus::Running));

    s.set_phase_completed(0, "schema", "", 1.5, nbrs_tui::state::PhaseSummary::default());
    assert!(matches!(s.phases[0].status, nbrs_tui::state::PhaseStatus::Completed));
    assert_eq!(s.phases[0].duration_secs, Some(1.5));
}

#[test]
fn actor_publishes_snapshots() {
    use nbrs_tui::run_state_actor::{spawn_run_state_actor, RunStateCmd};

    let (handle, _join) = spawn_run_state_actor(
        RunState::new("test.yaml", "repro", "stdout"),
    );
    handle.send(RunStateCmd::PhaseStarting {
        exec_id: 1,
        scene_node_id: 0,
        name: "schema".into(),
        labels: "".into(),
        op_templates: 4,
        total_cycles: 4,
        concurrency: 1,
    });
    // Snapshots are eventually-consistent — the actor processes
    // commands on its own thread. Spin briefly until the phase
    // appears.
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.load().phases.is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let snap = handle.load();
    assert_eq!(snap.phases.len(), 1, "phase should be visible on snapshot");
    assert_eq!(snap.phases[0].name, "schema");
}

/// Build a per-cell [`PhaseProgressUpdate`] carrying a distinct `ops_finished`
/// so the actor's completion-summary differs per cell.
fn progress(exec_id: u64, name: &str, labels: &str, ops_finished: u64)
    -> nbrs_runtime::observer::PhaseProgressUpdate
{
    nbrs_runtime::observer::PhaseProgressUpdate {
        exec_id,
        name: name.into(),
        labels: labels.into(),
        cursor_name: "cycle".into(),
        cursor_extent: ops_finished,
        fibers: 1,
        ops_started: ops_finished,
        ops_finished,
        ops_ok: ops_finished,
        skips: 0,
        errors: 0,
        retries: 0,
        ops_per_sec: 0.0,
        adapter_counters: Vec::new(),
        rows_per_batch: 0.0,
        relevancy: Vec::new(),
    }
}

/// SRD-100 P1c — the consolidated end-to-end attribution test. Drives TWO
/// concurrent same-name phase cells through the REAL display pipeline
/// (`RunStateCmd` → single-writer actor → `resolve_phase_node` → published
/// `ArcSwap` snapshot), completing them in REVERSED order with distinct
/// per-cell durations AND distinct progress-fed summaries, and asserts each
/// cell's scene node keeps ITS OWN final state. Fails against the pre-P1c
/// by-name `find_phase(Running)` routing (which would attribute x=2's numbers
/// to the first-DFS node) and against same-name node aliasing (which would
/// collapse both onto one row, last-writer-wins). The executor side — that a
/// concurrent sweep actually materializes the two DISTINCT `scene_node_id`s
/// this test feeds — is pinned by `nbrs/tests/phase_for_each_topology.rs`.
#[test]
fn actor_attributes_concurrent_same_name_cells_to_correct_nodes() {
    use nbrs_tui::run_state_actor::{spawn_run_state_actor, RunStateCmd};
    use nbrs_tui::state::{EntryKind, PhaseStatus};
    use nbrs_runtime::scene_tree::SceneTree;

    // Two distinct same-name "p" cells under their own per-iter scopes — the
    // distinct-node topology a sweep produces (post-SRD-100, phase-level
    // `for_each` too).
    let mut tree = SceneTree::new();
    let s1 = tree.push(tree.root(), EntryKind::Scope, "x=1", "");
    let a = tree.push(s1, EntryKind::Phase, "p", "x=1");
    let s2 = tree.push(tree.root(), EntryKind::Scope, "x=2", "");
    let b = tree.push(s2, EntryKind::Phase, "p", "x=2");
    assert_ne!(a, b);

    let (handle, _join) = spawn_run_state_actor(RunState::new("", "", ""));
    handle.send(RunStateCmd::InstallTree(tree));

    // Both cells start concurrently (dispatch order x=1 then x=2).
    handle.send(RunStateCmd::PhaseStarting {
        exec_id: 1, scene_node_id: a, name: "p".into(), labels: "x=1".into(),
        op_templates: 1, total_cycles: 10, concurrency: 1,
    });
    handle.send(RunStateCmd::PhaseStarting {
        exec_id: 1, scene_node_id: b, name: "p".into(), labels: "x=2".into(),
        op_templates: 1, total_cycles: 20, concurrency: 1,
    });
    // Distinct per-cell progress -> distinct completion summaries.
    handle.send(RunStateCmd::PhaseProgress(progress(1, "p", "x=1", 10)));
    handle.send(RunStateCmd::PhaseProgress(progress(1, "p", "x=2", 20)));
    // Complete in REVERSED order with distinct durations.
    handle.send(RunStateCmd::PhaseCompleted {
        exec_id: 1, scene_node_id: b, name: "p".into(), labels: "x=2".into(),
        duration_secs: 20.0,
    });
    handle.send(RunStateCmd::PhaseCompleted {
        exec_id: 1, scene_node_id: a, name: "p".into(), labels: "x=1".into(),
        duration_secs: 10.0,
    });

    // Wait until both cells are Completed in the published snapshot.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let snap = handle.load();
        let done = snap.phases.iter()
            .filter(|e| e.name == "p" && matches!(e.status, PhaseStatus::Completed))
            .count();
        if done == 2 || Instant::now() >= deadline { break; }
        std::thread::sleep(Duration::from_millis(5));
    }

    let snap = handle.load();
    let row = |id| snap.phases.iter().find(|e| e.node_id == id).expect("row for id");
    // Each cell's node carries ITS OWN duration AND summary — not a sibling's.
    assert_eq!(row(a).duration_secs, Some(10.0), "x=1 node keeps its own duration");
    assert_eq!(row(b).duration_secs, Some(20.0), "x=2 node keeps its own duration");
    assert_eq!(row(a).summary.as_ref().map(|s| s.ops_finished), Some(10),
        "x=1 node keeps its own summary (ops_finished)");
    assert_eq!(row(b).summary.as_ref().map(|s| s.ops_finished), Some(20),
        "x=2 node keeps its own summary (ops_finished)");
    assert!(matches!(row(a).status, PhaseStatus::Completed));
    assert!(matches!(row(b).status, PhaseStatus::Completed));
}

#[test]
fn sparkline_history_caps_at_120() {
    let mut state = RunState::new("test.yaml", "repro", "stdout");
    for i in 0..200 {
        state.push_ops_sample(i as f64);
    }
    assert_eq!(state.ops_history.len(), 120);
    // Oldest samples were evicted
    assert_eq!(state.ops_history[0] as u64, 80);
}

#[test]
fn reporter_channel_delivers_frames() {
    let (mut reporter, rx) = TuiReporter::channel();

    let mut h = hdrhistogram::Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
    for i in 1..=100 {
        let _ = h.record(i * 1_000_000);
    }

    let mut snapshot = MetricSet::new(Duration::from_secs(1));
    snapshot.insert_histogram("cycles_servicetime", Labels::default(), h, Instant::now());

    use nbrs_metrics::scheduler::Reporter;
    reporter.report(&snapshot);

    let received = rx.try_recv();
    assert!(received.is_ok(), "snapshot should be received on channel");
}
