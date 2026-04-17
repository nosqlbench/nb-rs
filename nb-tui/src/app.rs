// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! TUI application: event loop, terminal setup, frame rendering.
//!
//! The TUI runs on a dedicated std::thread (not tokio) to avoid
//! blocking the async runtime. It reads from two sources:
//! - MetricsFrame channel (latency histograms from scheduler)
//! - Arc<RwLock<RunState>> (phase progress from executor)

use std::io;
use std::sync::{mpsc, Arc, RwLock};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::*;

use nb_metrics::frame::MetricsFrame;
use crate::state::{RunState, PhaseStatus};
use crate::widgets::{self, colors};

/// TUI application.
pub struct App {
    pub should_quit: bool,
    pub tick_rate: Duration,
    frame_rx: mpsc::Receiver<MetricsFrame>,
    run_state: Arc<RwLock<RunState>>,
}

impl App {
    /// Create with a metrics channel and shared run state.
    pub fn new(
        frame_rx: mpsc::Receiver<MetricsFrame>,
        run_state: Arc<RwLock<RunState>>,
    ) -> Self {
        Self {
            should_quit: false,
            tick_rate: Duration::from_millis(250),
            frame_rx,
            run_state,
        }
    }

    /// Run the TUI event loop. Blocks until quit or run completes.
    pub fn run(&mut self) -> io::Result<()> {
        terminal::enable_raw_mode()?;
        io::stderr().execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(io::stderr());
        let mut terminal = Terminal::new(backend)?;

        let result = self.event_loop(&mut terminal);

        terminal::disable_raw_mode()?;
        io::stderr().execute(LeaveAlternateScreen)?;

        result
    }

    fn event_loop(&mut self, terminal: &mut Terminal<CrosstermBackend<io::Stderr>>) -> io::Result<()> {
        let mut last_tick = Instant::now();

        loop {
            terminal.draw(|frame| self.draw(frame))?;

            let timeout = self.tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or(Duration::ZERO);

            if event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
                            _ => {}
                        }
                    }
                }
            }

            // Check if run finished
            if let Ok(state) = self.run_state.read() {
                if state.finished {
                    // Show final state for a moment then exit
                    std::thread::sleep(Duration::from_millis(500));
                    self.should_quit = true;
                }
            }

            if self.should_quit {
                break;
            }

            if last_tick.elapsed() >= self.tick_rate {
                self.drain_frames();
                last_tick = Instant::now();
            }
        }

        Ok(())
    }

    /// Drain pending metrics frames and update latency/sparkline state.
    fn drain_frames(&mut self) {
        while let Ok(frame) = self.frame_rx.try_recv() {
            if let Ok(mut state) = self.run_state.write() {
                for sample in &frame.samples {
                    match sample {
                        nb_metrics::frame::Sample::Timer { labels, histogram, .. } => {
                            if labels.get("name") == Some("cycles_servicetime") {
                                state.p50_nanos = histogram.value_at_quantile(0.50);
                                state.p90_nanos = histogram.value_at_quantile(0.90);
                                state.p99_nanos = histogram.value_at_quantile(0.99);
                                state.p999_nanos = histogram.value_at_quantile(0.999);
                                state.max_nanos = histogram.max();
                            }
                        }
                        _ => {}
                    }
                }
                // Push sparkline samples from active phase
                let (ops_sample, rows_sample) = if let Some(ref active) = state.active {
                    let rows_rate = active.adapter_counters.iter()
                        .find(|(n, _, _)| n == "rows_inserted")
                        .map(|(_, _, r)| *r)
                        .unwrap_or(0.0);
                    (Some(active.ops_per_sec), Some(rows_rate))
                } else {
                    (None, None)
                };
                if let Some(ops) = ops_sample { state.push_ops_sample(ops); }
                if let Some(rows) = rows_sample { state.push_rows_sample(rows); }
            }
        }
    }

    /// Render one frame. Public for testing with TestBackend.
    pub fn draw(&self, frame: &mut Frame) {
        let state = match self.run_state.read() {
            Ok(s) => s,
            Err(_) => return,
        };
        let area = frame.area();

        // Overall vertical layout
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),   // header
                Constraint::Length(4),   // phase panel
                Constraint::Length(7),   // latency
                Constraint::Length(4),   // sparklines
                Constraint::Min(3),     // scenario tree
                Constraint::Length(1),   // footer
            ])
            .split(area);

        self.draw_header(frame, sections[0], &state);
        self.draw_phase(frame, sections[1], &state);
        self.draw_latency(frame, sections[2], &state);
        self.draw_sparklines(frame, sections[3], &state);
        self.draw_tree(frame, sections[4], &state);
        self.draw_footer(frame, sections[5]);
    }

    fn draw_header(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let elapsed_s = state.elapsed_secs();
        let elapsed = widgets::format_elapsed(elapsed_s);

        // Phase ETA: based on cursor progress in the active phase
        let phase_eta = state.active.as_ref().and_then(|a| {
            if a.ops_finished > 0 && a.cursor_extent > 0 {
                let phase_elapsed = a.started_at.elapsed().as_secs_f64();
                let fraction = a.ops_finished as f64 / a.cursor_extent as f64;
                if fraction > 0.01 {
                    let total_est = phase_elapsed / fraction;
                    Some(widgets::format_elapsed(total_est - phase_elapsed))
                } else { None }
            } else { None }
        });

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Span::styled(" nbrs ", Style::default().fg(colors::EMPHASIS).bold()));

        let mut spans = vec![
            Span::styled(" workload: ", Style::default().fg(colors::DIM)),
            Span::styled(&state.workload_file, Style::default().fg(colors::TEXT)),
            Span::styled("  scenario: ", Style::default().fg(colors::DIM)),
            Span::styled(&state.scenario_name, Style::default().fg(colors::TEXT)),
            Span::styled("  elapsed: ", Style::default().fg(colors::DIM)),
            Span::styled(elapsed, Style::default().fg(colors::EMPHASIS).bold()),
        ];
        if let Some(eta) = phase_eta {
            spans.push(Span::styled("  phase ETA: ", Style::default().fg(colors::DIM)));
            spans.push(Span::styled(eta, Style::default().fg(colors::PHASE_ACTIVE)));
        }
        let line1 = Line::from(spans);

        let para = Paragraph::new(line1).block(block);
        frame.render_widget(para, area);
    }

    fn draw_phase(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Span::styled(" Phase ", Style::default().fg(colors::PHASE_ACTIVE)));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        if let Some(ref active) = state.active {
            let active_count = active.ops_started.saturating_sub(active.ops_finished);
            let pending = active.cursor_extent.saturating_sub(active.ops_started);
            let complete = active.ops_finished;

            // Line 1: phase name, cursor, progress
            let pct = if active.cursor_extent > 0 {
                active.ops_started as f64 * 100.0 / active.cursor_extent as f64
            } else { 0.0 };
            let progress_width = inner.width.saturating_sub(50) as usize;
            let progress = widgets::bar_str(pct / 100.0, progress_width.max(10));

            // Phase ETA
            let phase_elapsed = active.started_at.elapsed().as_secs_f64();
            let eta_str = if active.ops_finished > 0 && pct > 1.0 {
                let fraction = active.ops_finished as f64 / active.cursor_extent as f64;
                let remaining = (phase_elapsed / fraction) - phase_elapsed;
                format!("  ETA:{}", widgets::format_elapsed(remaining))
            } else {
                String::new()
            };

            let line1 = Line::from(vec![
                Span::styled(" ▶ ", Style::default().fg(colors::PHASE_ACTIVE).bold()),
                Span::styled(&active.name, Style::default().fg(colors::EMPHASIS).bold()),
                Span::styled(format!("  cursor:{}", active.cursor_name), Style::default().fg(colors::DIM)),
                Span::styled(format!(" {}", progress), Style::default().fg(colors::PROGRESS_HIGH)),
                Span::styled(format!(" {:.1}%", pct), Style::default().fg(colors::TEXT)),
                Span::styled(&eta_str, Style::default().fg(colors::PHASE_ACTIVE)),
            ]);

            // Line 2: fibers, active, rates, batch
            let rows_rate = active.adapter_counters.iter()
                .find(|(n, _, _)| n == "rows_inserted")
                .map(|(_, _, r)| *r)
                .unwrap_or(0.0);

            let line2 = Line::from(vec![
                Span::styled(format!("   fibers:{}", active.fibers), Style::default().fg(colors::DIM)),
                Span::styled(format!("  active:{active_count}"), Style::default().fg(
                    if active_count > 0 { colors::PHASE_ACTIVE } else { colors::DIM }
                )),
                Span::styled(format!("  ops/s:{}", widgets::format_rate(active.ops_per_sec)), Style::default().fg(colors::TEXT)),
                Span::styled(format!("  rows/s:{}", widgets::format_rate(rows_rate)), Style::default().fg(colors::TEXT)),
                if active.rows_per_batch > 1.0 {
                    Span::styled(format!("  rows/batch:{:.1}", active.rows_per_batch), Style::default().fg(colors::DIM))
                } else {
                    Span::raw("")
                },
                Span::styled(format!("  pending:{}", widgets::format_count(pending)), Style::default().fg(colors::DIM)),
                Span::styled(format!("  done:{}", widgets::format_count(complete)), Style::default().fg(colors::PHASE_DONE)),
            ]);

            if inner.height >= 2 {
                frame.render_widget(Paragraph::new(line1), Rect { y: inner.y, height: 1, ..inner });
                frame.render_widget(Paragraph::new(line2), Rect { y: inner.y + 1, height: 1, ..inner });
            } else {
                frame.render_widget(Paragraph::new(line1), inner);
            }
        } else {
            let msg = Paragraph::new(Span::styled(
                " waiting for phase...",
                Style::default().fg(colors::DIM),
            ));
            frame.render_widget(msg, inner);
        }
    }

    fn draw_latency(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Span::styled(" Latency (service time) ", Style::default().fg(colors::LAT_P50)));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        let max_val = state.p999_nanos.max(1);
        let bar_width = inner.width.saturating_sub(20) as usize;

        let percentiles = [
            ("p50 ", state.p50_nanos, colors::LAT_P50),
            ("p90 ", state.p90_nanos, colors::LAT_P90),
            ("p99 ", state.p99_nanos, colors::LAT_P99),
            ("p999", state.p999_nanos, colors::LAT_MAX),
            ("max ", state.max_nanos, colors::LAT_MAX),
        ];

        for (i, (label, nanos, color)) in percentiles.iter().enumerate() {
            if i as u16 >= inner.height { break; }
            let frac = if max_val > 0 { *nanos as f64 / max_val as f64 } else { 0.0 };
            let bar = widgets::bar_str(frac.min(1.0), bar_width.max(5));
            let line = Line::from(vec![
                Span::styled(format!("  {label}"), Style::default().fg(colors::DIM)),
                Span::styled(format!(" {:>8}", widgets::format_nanos(*nanos)), Style::default().fg(*color).bold()),
                Span::styled(format!("  {bar}"), Style::default().fg(*color)),
            ]);
            frame.render_widget(
                Paragraph::new(line),
                Rect { y: inner.y + i as u16, height: 1, ..inner },
            );
        }
    }

    fn draw_sparklines(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Span::styled(" Throughput ", Style::default().fg(colors::SPARK)));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Reserve: 9 chars prefix ("  ops/s  ") + 7 chars suffix (" 1.6K\n")
        let spark_width = inner.width.saturating_sub(17) as usize;

        if inner.height >= 1 {
            let ops_spark = widgets::sparkline_str(&state.ops_history, spark_width);
            let ops_label = state.ops_history.last().map(|v| widgets::format_rate(*v)).unwrap_or_default();
            let line = Line::from(vec![
                Span::styled("  ops/s  ", Style::default().fg(colors::DIM)),
                Span::styled(ops_spark, Style::default().fg(colors::SPARK)),
                Span::styled(format!(" {ops_label}"), Style::default().fg(colors::TEXT)),
            ]);
            frame.render_widget(Paragraph::new(line), Rect { y: inner.y, height: 1, ..inner });
        }

        if inner.height >= 2 && !state.rows_history.is_empty() {
            let rows_spark = widgets::sparkline_str(&state.rows_history, spark_width);
            let rows_label = state.rows_history.last().map(|v| widgets::format_rate(*v)).unwrap_or_default();
            let line = Line::from(vec![
                Span::styled("  rows/s ", Style::default().fg(colors::DIM)),
                Span::styled(rows_spark, Style::default().fg(colors::PHASE_ACTIVE)),
                Span::styled(format!(" {rows_label}"), Style::default().fg(colors::TEXT)),
            ]);
            frame.render_widget(
                Paragraph::new(line),
                Rect { y: inner.y + 1, height: 1, ..inner },
            );
        }
    }

    fn draw_tree(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Span::styled(" Scenario Tree ", Style::default().fg(colors::TEXT)));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for phase in &state.phases {
            let indent = "  ".repeat(phase.depth);
            let (icon, icon_color) = match &phase.status {
                PhaseStatus::Completed => ("✓", colors::PHASE_DONE),
                PhaseStatus::Running => ("▶", colors::PHASE_ACTIVE),
                PhaseStatus::Pending => ("○", colors::PHASE_PENDING),
                PhaseStatus::Failed(_) => ("✗", colors::PHASE_FAILED),
            };
            let name_color = match &phase.status {
                PhaseStatus::Running => colors::EMPHASIS,
                PhaseStatus::Completed => colors::TEXT,
                _ => colors::DIM,
            };

            let mut spans = vec![
                Span::styled(format!("  {indent}{icon} "), Style::default().fg(icon_color)),
                Span::styled(&phase.name, Style::default().fg(name_color)),
            ];

            if !phase.labels.is_empty() {
                spans.push(Span::styled(
                    format!(" ({})", phase.labels),
                    Style::default().fg(colors::DIM),
                ));
            }

            if phase.op_count > 0 {
                spans.push(Span::styled(
                    format!("  {} ops", phase.op_count),
                    Style::default().fg(colors::DIM),
                ));
            }

            match &phase.status {
                PhaseStatus::Completed => {
                    if let Some(dur) = phase.duration_secs {
                        spans.push(Span::styled(
                            format!("  {:.1}s", dur),
                            Style::default().fg(colors::DIM),
                        ));
                    }
                }
                PhaseStatus::Failed(err) => {
                    spans.push(Span::styled(
                        format!("  {err}"),
                        Style::default().fg(colors::PHASE_FAILED),
                    ));
                }
                _ => {}
            }

            lines.push(Line::from(spans));
        }

        // Scroll to show the latest entries if too many
        let visible = inner.height as usize;
        let start = if lines.len() > visible { lines.len() - visible } else { 0 };
        let visible_lines: Vec<Line> = lines.into_iter().skip(start).collect();

        frame.render_widget(Paragraph::new(visible_lines), inner);
    }

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let line = Line::from(vec![
            Span::styled(" q", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": quit  ", Style::default().fg(colors::DIM)),
        ]);
        frame.render_widget(Paragraph::new(line), area);
    }
}
