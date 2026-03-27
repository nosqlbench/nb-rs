// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! TUI application: event loop, terminal setup, frame rendering.

use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::*;

use nb_metrics::frame::MetricsFrame;
use crate::widgets::MetricsState;

/// TUI application state.
pub struct App {
    pub metrics: MetricsState,
    pub should_quit: bool,
    pub tick_rate: Duration,
    /// Receives metrics frames from the scheduler.
    frame_rx: Option<mpsc::Receiver<MetricsFrame>>,
}

impl App {
    /// Create without a metrics channel (standalone/testing mode).
    pub fn new() -> Self {
        Self {
            metrics: MetricsState::new(),
            should_quit: false,
            tick_rate: Duration::from_millis(250),
            frame_rx: None,
        }
    }

    /// Create with a metrics channel for live updates.
    pub fn with_metrics(rx: mpsc::Receiver<MetricsFrame>) -> Self {
        Self {
            metrics: MetricsState::new(),
            should_quit: false,
            tick_rate: Duration::from_millis(250),
            frame_rx: Some(rx),
        }
    }

    /// Run the TUI event loop. Blocks until the user quits (q/Esc)
    /// or the activity signals completion.
    pub fn run(&mut self) -> io::Result<()> {
        terminal::enable_raw_mode()?;
        io::stdout().execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(io::stdout());
        let mut terminal = Terminal::new(backend)?;

        let result = self.event_loop(&mut terminal);

        terminal::disable_raw_mode()?;
        io::stdout().execute(LeaveAlternateScreen)?;

        result
    }

    /// Run without terminal (for testing) — just process N frames.
    pub fn run_headless(&mut self, max_frames: usize) {
        for _ in 0..max_frames {
            self.drain_frames();
            self.metrics.tick();
        }
    }

    fn event_loop(&mut self, terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
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

            if self.should_quit {
                break;
            }

            if last_tick.elapsed() >= self.tick_rate {
                self.drain_frames();
                self.metrics.tick();
                last_tick = Instant::now();
            }
        }

        Ok(())
    }

    /// Drain all pending metrics frames from the channel.
    fn drain_frames(&mut self) {
        if let Some(ref rx) = self.frame_rx {
            while let Ok(frame) = rx.try_recv() {
                self.metrics.update_from_frame(&frame);
            }
        }
    }

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(area);

        // Header
        let header = Block::default()
            .borders(Borders::ALL)
            .title(" nbrs ")
            .title_alignment(Alignment::Center);
        let header_text = Paragraph::new(format!(
            " cycles: {}  ops/s: {:.0}  errors: {}  elapsed: {:.1}s",
            self.metrics.total_cycles,
            self.metrics.ops_per_sec,
            self.metrics.total_errors,
            self.metrics.elapsed_secs,
        ))
        .block(header);
        frame.render_widget(header_text, chunks[0]);

        // Main panels
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(60),
                Constraint::Percentage(40),
            ])
            .split(chunks[1]);

        // Latency panel
        let latency_block = Block::default()
            .borders(Borders::ALL)
            .title(" Latency ");
        let p50 = format_nanos(self.metrics.p50_nanos);
        let p90 = format_nanos(self.metrics.p90_nanos);
        let p99 = format_nanos(self.metrics.p99_nanos);
        let p999 = format_nanos(self.metrics.p999_nanos);
        let max_l = format_nanos(self.metrics.max_nanos);
        let latency_rows = vec![
            Row::new(vec!["p50".to_string(), p50]),
            Row::new(vec!["p90".to_string(), p90]),
            Row::new(vec!["p99".to_string(), p99]),
            Row::new(vec!["p999".to_string(), p999]),
            Row::new(vec!["max".to_string(), max_l]),
        ];
        let latency_table = Table::new(
            latency_rows,
            [Constraint::Length(6), Constraint::Min(10)],
        )
        .block(latency_block)
        .header(Row::new(vec!["Pctl", "Value"]).style(Style::default().bold()));
        frame.render_widget(latency_table, main_chunks[0]);

        // Activity panel
        let activity_block = Block::default()
            .borders(Borders::ALL)
            .title(" Activity ");
        let activity_text = Paragraph::new(vec![
            Line::from(format!(" Activity: {}", self.metrics.activity_name)),
            Line::from(format!(" Driver:   {}", self.metrics.driver_name)),
            Line::from(format!(" Threads:  {}", self.metrics.threads)),
            Line::from(format!(" Stanza:   {}", self.metrics.stanza_length)),
            Line::from(""),
            Line::from(format!(" Rate cfg: {}", self.metrics.rate_config)),
        ])
        .block(activity_block);
        frame.render_widget(activity_text, main_chunks[1]);

        // Footer
        let footer = Block::default()
            .borders(Borders::ALL)
            .title(" Press q to quit ");
        let progress = if self.metrics.total_target > 0 {
            let pct = (self.metrics.total_cycles as f64 / self.metrics.total_target as f64 * 100.0).min(100.0);
            format!(" Progress: {:.1}% ({}/{})", pct, self.metrics.total_cycles, self.metrics.total_target)
        } else {
            " Progress: --".to_string()
        };
        let footer_text = Paragraph::new(progress).block(footer);
        frame.render_widget(footer_text, chunks[2]);
    }
}

fn format_nanos(nanos: u64) -> String {
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.1}us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.1}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
    }
}
