// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration test: metrics frame → channel → RunState update.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nb_metrics::frame::{MetricsFrame, Sample};
use nb_metrics::labels::Labels;
use nb_tui::state::RunState;
use nb_tui::reporter::TuiReporter;

#[test]
fn run_state_tracks_phase_lifecycle() {
    let state = Arc::new(RwLock::new(RunState::new("test.yaml", "repro", "stdout")));

    {
        let mut s = state.write().unwrap();
        s.set_phase_running("schema", "", 4);
    }
    {
        let s = state.read().unwrap();
        assert_eq!(s.phases.len(), 1);
        assert_eq!(s.phases[0].name, "schema");
        assert!(matches!(s.phases[0].status, nb_tui::state::PhaseStatus::Running));
    }
    {
        let mut s = state.write().unwrap();
        s.set_phase_completed("schema", "", 1.5);
    }
    {
        let s = state.read().unwrap();
        assert!(matches!(s.phases[0].status, nb_tui::state::PhaseStatus::Completed));
        assert_eq!(s.phases[0].duration_secs, Some(1.5));
    }
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

    let frame = MetricsFrame {
        captured_at: Instant::now(),
        interval: Duration::from_secs(1),
        samples: vec![
            Sample::Timer {
                labels: Labels::of("name", "cycles_servicetime"),
                count: 1000,
                histogram: h,
            },
        ],
    };

    use nb_metrics::scheduler::Reporter;
    reporter.report(&frame);

    // Verify frame arrives on the channel
    let received = rx.try_recv();
    assert!(received.is_ok(), "frame should be received on channel");
}
