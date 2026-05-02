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

    s.set_phase_running("schema", "", 4);
    assert_eq!(s.phases.len(), 1);
    assert_eq!(s.phases[0].name, "schema");
    assert!(matches!(s.phases[0].status, nbrs_tui::state::PhaseStatus::Running));

    s.set_phase_completed("schema", "", 1.5, nbrs_tui::state::PhaseSummary::default());
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
