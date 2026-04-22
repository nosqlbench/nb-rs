// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Layout rendering tests using ratatui's TestBackend.
//!
//! Renders the TUI into a headless terminal buffer and verifies
//! key elements appear in the right positions.

use std::sync::{Arc, RwLock, mpsc};
use std::time::Instant;

use ratatui::backend::TestBackend;
use ratatui::Terminal;

use nb_tui::app::App;
use nb_tui::state::{RunState, ActivePhase, PhaseStatus};

fn test_metrics_query() -> Arc<nb_metrics::metrics_query::MetricsQuery> {
    use nb_metrics::cadence::{Cadences, CadenceTree};
    use nb_metrics::cadence_reporter::CadenceReporter;
    use nb_metrics::component::Component;
    use nb_metrics::labels::Labels;

    let tree = CadenceTree::plan_default(Cadences::defaults());
    let reporter = Arc::new(CadenceReporter::new(tree));
    let root = Component::root(Labels::of("session", "test"), std::collections::HashMap::new());
    Arc::new(nb_metrics::metrics_query::MetricsQuery::new(reporter, root))
}

fn make_test_state() -> Arc<RwLock<RunState>> {
    let mut state = RunState::new("full_cql_vector.yaml", "fknn_rampup", "cql");
    state.profiler = "off".into();
    state.limit = "5000".into();

    // Add some completed phases
    state.set_phase_running("teardown", "table=fknn_default", 3);
    state.set_phase_completed("teardown", "table=fknn_default", 0.2, nb_tui::state::PhaseSummary::default());
    state.set_phase_running("schema", "table=fknn_default", 4);
    state.set_phase_completed("schema", "table=fknn_default", 1.1, nb_tui::state::PhaseSummary::default());

    // Active phase
    state.set_phase_running("fknn_rampup_data", "optimize_for=RECALL", 1);
    state.active = Some(ActivePhase {
        name: "fknn_rampup_data".into(),
        labels: "optimize_for=RECALL".into(),
        cursor_name: "row".into(),
        cursor_extent: 5000,
        fibers: 100,
        started_at: Instant::now(),
        ops_started: 2600,
        ops_finished: 2500,
        ops_ok: 2500,
        errors: 0,
        retries: 0,
        ops_per_sec: 220.0,
        adapter_counters: vec![("rows_inserted".into(), 19500, 1700.0)],
        rows_per_batch: 7.8,
        relevancy: Vec::new(),
    });

    // Pending phases
    state.add_phase("pvs_query", "k=10", 0);
    state.add_phase("pvs_query", "k=100", 0);

    // Latency
    state.p50_nanos = 1_200_000;   // 1.2ms
    state.p90_nanos = 3_800_000;   // 3.8ms
    state.p99_nanos = 12_400_000;  // 12.4ms
    state.p999_nanos = 45_100_000; // 45.1ms
    state.max_nanos = 89_200_000;  // 89.2ms

    // Sparkline history
    for i in 0..30 {
        state.push_ops_sample(180.0 + i as f64 * 1.5);
        state.push_rows_sample(1400.0 + i as f64 * 12.0);
    }

    Arc::new(RwLock::new(state))
}

#[test]
fn render_layout_has_all_sections() {
    let state = make_test_state();
    let (_tx, rx) = mpsc::channel();
    let app = App::new(rx, state.clone(), test_metrics_query());

    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    terminal.draw(|frame| app.draw(frame)).unwrap();

    let buf = terminal.backend().buffer().clone();
    let text: String = (0..buf.area.height)
        .map(|y| {
            (0..buf.area.width)
                .map(|x| buf[(x, y)].symbol().chars().next().unwrap_or(' '))
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Header section
    assert!(text.contains("nbrs"), "missing header title:\n{text}");
    assert!(text.contains("full_cql_vector.yaml"), "missing workload name:\n{text}");
    assert!(text.contains("fknn_rampup"), "missing scenario name:\n{text}");

    // Phase panel
    assert!(text.contains("fknn_rampup_data"), "missing active phase name:\n{text}");
    assert!(text.contains("cursor:row"), "missing cursor name:\n{text}");
    assert!(text.contains("concurrency:100"), "missing concurrency count:\n{text}");
    assert!(text.contains("active:100"), "missing active count:\n{text}");

    // Latency section
    assert!(text.contains("p50"), "missing p50 label:\n{text}");
    assert!(text.contains("p99"), "missing p99 label:\n{text}");
    assert!(text.contains("1.2"), "missing p50 value (1.2ms):\n{text}");

    // Sparkline section
    assert!(text.contains("ops/s"), "missing ops/s sparkline:\n{text}");
    assert!(text.contains("rows/s"), "missing rows/s sparkline:\n{text}");

    // Scenario tree
    assert!(text.contains("✓"), "missing completed phase marker:\n{text}");
    assert!(text.contains("▶"), "missing running phase marker:\n{text}");
    assert!(text.contains("○"), "missing pending phase marker:\n{text}");
    assert!(text.contains("teardown"), "missing teardown in tree:\n{text}");
    assert!(text.contains("pvs_query"), "missing pvs_query in tree:\n{text}");

    // Footer
    assert!(text.contains("quit"), "missing quit hint:\n{text}");
}

#[test]
fn render_layout_no_active_phase() {
    let state = Arc::new(RwLock::new(
        RunState::new("test.yaml", "smoke", "stdout")
    ));

    let (_tx, rx) = mpsc::channel();
    let app = App::new(rx, state, test_metrics_query());

    let backend = TestBackend::new(80, 20);
    let mut terminal = Terminal::new(backend).unwrap();

    terminal.draw(|frame| app.draw(frame)).unwrap();

    let buf = terminal.backend().buffer().clone();
    let text: String = (0..buf.area.height)
        .map(|y| {
            (0..buf.area.width)
                .map(|x| buf[(x, y)].symbol().chars().next().unwrap_or(' '))
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(text.contains("waiting"), "should show 'waiting' when no active phase:\n{text}");
    assert!(text.contains("nbrs"), "header should always render:\n{text}");
}

#[test]
fn render_layout_narrow_terminal() {
    let state = make_test_state();
    let (_tx, rx) = mpsc::channel();
    let app = App::new(rx, state, test_metrics_query());

    // Narrow terminal — should not panic
    let backend = TestBackend::new(40, 15);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| app.draw(frame)).unwrap();

    // Just verify it rendered without panicking
    let buf = terminal.backend().buffer().clone();
    assert!(buf.area.width == 40);
}

#[test]
fn render_prints_full_buffer() {
    let state = make_test_state();
    let (_tx, rx) = mpsc::channel();
    let app = App::new(rx, state, test_metrics_query());

    let backend = TestBackend::new(120, 35);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| app.draw(frame)).unwrap();

    let buf = terminal.backend().buffer().clone();
    eprintln!("=== TUI Render (120x35) ===");
    for y in 0..buf.area.height {
        let line: String = (0..buf.area.width)
            .map(|x| buf[(x, y)].symbol().chars().next().unwrap_or(' '))
            .collect();
        eprintln!("{}", line.trim_end());
    }
    eprintln!("=== END ===");
}
