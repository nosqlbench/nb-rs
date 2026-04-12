// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration test: metrics frame → channel → TUI state update.

use std::time::{Duration, Instant};

use nb_metrics::frame::{MetricsFrame, Sample};
use nb_metrics::labels::Labels;
use nb_tui::app::App;
use nb_tui::reporter::TuiReporter;

#[test]
fn tui_receives_frames_via_channel() {
    let (mut reporter, rx) = TuiReporter::channel();
    let mut app = App::with_metrics(rx);

    // Build a test frame with counter and timer
    let mut h = hdrhistogram::Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
    for i in 1..=100 {
        let _ = h.record(i * 1_000_000); // 1ms to 100ms
    }

    let frame = MetricsFrame {
        captured_at: Instant::now(),
        interval: Duration::from_secs(1),
        samples: vec![
            Sample::Counter {
                labels: Labels::of("name", "cycles_total").with("activity", "test"),
                value: 1000,
            },
            Sample::Counter {
                labels: Labels::of("name", "errors_total").with("activity", "test"),
                value: 5,
            },
            Sample::Timer {
                labels: Labels::of("name", "cycles_servicetime").with("activity", "test"),
                count: 1000,
                histogram: h,
            },
        ],
    };

    // Reporter sends the frame
    use nb_metrics::scheduler::Reporter;
    reporter.report(&frame);

    // TUI processes it headlessly
    app.run_headless(1);

    // Verify state was updated
    assert_eq!(app.metrics.total_cycles, 1000);
    assert_eq!(app.metrics.total_errors, 5);
    assert!(app.metrics.p50_nanos > 0, "p50 should be populated");
    assert!(app.metrics.p99_nanos > app.metrics.p50_nanos, "p99 > p50");
    assert!(app.metrics.max_nanos > 0, "max should be populated");
}

#[test]
fn tui_handles_multiple_frames() {
    let (mut reporter, rx) = TuiReporter::channel();
    let mut app = App::with_metrics(rx);

    use nb_metrics::scheduler::Reporter;

    // Send two frames
    for count in [100, 200] {
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Counter {
                labels: Labels::of("name", "cycles_total"),
                value: count,
            }],
        };
        reporter.report(&frame);
    }

    app.run_headless(1);

    // Should have the latest value (200, not 100)
    assert_eq!(app.metrics.total_cycles, 200);
}

#[test]
fn tui_works_without_channel() {
    let mut app = App::new();
    app.metrics.total_cycles = 42;
    app.run_headless(1);
    assert!(app.metrics.elapsed_secs > 0.0);
    assert_eq!(app.metrics.total_cycles, 42);
}
