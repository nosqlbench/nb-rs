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

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::*;

use nb_metrics::metrics_query::{MetricsQuery, Selection};
use nb_metrics::snapshot::{MetricSet, MetricValue};
use crate::state::{RunState, PhaseStatus};
use crate::widgets::{self, colors};

/// Which scrollable pane currently owns mouse-wheel input.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FocusedPane {
    Tree,
}

/// Level of detail for the scenario tree rendering. Cycled by the
/// left/right arrow keys.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TreeLod {
    /// Nothing — tree pane renders empty. Left-arrow below Minimal
    /// reaches this state, letting the operator reclaim the screen
    /// area when they don't want a tree at all.
    Hidden,
    /// Phase lines only — no scope/closure headers, no detail blocks.
    /// Useful for a compact skim of what ran in what order.
    Minimal,
    /// Dynamic "dashboard" mode: only running phases are visible,
    /// each expanded to its full detail block. Scope headers are
    /// suppressed. With exactly one phase running the screen reads
    /// as a single-phase status dashboard; with two or more, the
    /// layout auto-widens to cover every running phase — still no
    /// scope headers or pending/completed noise. When zero phases
    /// are running it shows a "waiting for phase…" placeholder
    /// (or a final-state banner if the scenario is done).
    ///
    /// See SRD 62 §"LOD controls govern phase expansion" and
    /// §"Design notes → Scenario done? is a component-tree query".
    Focus,
    /// Phases + scope headers, with at most one entry optionally
    /// expanded via Enter. This is the default view.
    Default,
    /// Everything expanded: phases, scope headers, and the full detail
    /// block under every phase that has summary data.
    Maximal,
}

impl TreeLod {
    /// Next step toward Maximal, clamped at the top.
    fn next(self) -> Self {
        match self {
            Self::Hidden => Self::Minimal,
            Self::Minimal => Self::Focus,
            Self::Focus => Self::Default,
            Self::Default => Self::Maximal,
            Self::Maximal => Self::Maximal,
        }
    }

    /// Previous step toward Hidden, clamped at the bottom.
    fn prev(self) -> Self {
        match self {
            Self::Maximal => Self::Default,
            Self::Default => Self::Focus,
            Self::Focus => Self::Minimal,
            Self::Minimal => Self::Hidden,
            Self::Hidden => Self::Hidden,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Hidden => "off",
            Self::Minimal => "min",
            Self::Focus => "focus",
            Self::Default => "def",
            Self::Maximal => "max",
        }
    }
}

/// TUI application.
pub struct App {
    pub should_quit: bool,
    /// If true, the entire nbrs process should exit after the TUI
    /// cleans up its terminal state. Set by triple-tapping `q`.
    pub should_exit_process: bool,
    pub tick_rate: Duration,
    /// Whether the log panel is visible (toggled with `l` key).
    pub show_log: bool,
    frame_rx: mpsc::Receiver<MetricSet>,
    run_state: Arc<RwLock<RunState>>,
    /// Scrollable pane currently owning mouse-wheel input. Set by
    /// left-clicking inside a pane's rect.
    focused: Option<FocusedPane>,
    /// Scenario-tree scroll offset in lines from the top. `None` means
    /// follow the tail automatically (default). Set to a concrete offset
    /// the first time the user scrolls or clicks the tree.
    tree_scroll: Option<usize>,
    /// Current animated scroll position (fractional so tweening reads
    /// smooth). The actual render uses `.floor()` of this. In sync with
    /// `tree_scroll` when no animation is in flight.
    tree_display: f32,
    /// Active scroll animation: `(from, to, started_at, duration)`.
    /// Each mouse-wheel event starts or retargets this so the viewport
    /// glides into place instead of teleporting.
    tree_anim: Option<(f32, f32, Instant, Duration)>,
    /// Index into `state.phases` of the selected phase entry. Only
    /// meaningful when `focused == Some(Tree)`. When focus is active
    /// this is always `Some` — a selection is always present.
    tree_selected: Option<usize>,
    /// If true, selection auto-follows the most recently active phase
    /// (Running > latest Completed). Flipped to false as soon as the
    /// user moves the selection manually.
    tree_selection_auto: bool,
    /// Index into `state.phases` of the currently expanded entry (if
    /// any). At most one entry is expanded at a time, keeping the
    /// tree compact. Enter toggles, Escape collapses.
    tree_expanded: Option<usize>,
    /// Last-known tree panel rect — cached each frame so keyboard
    /// handlers can compute viewport geometry (visible rows, tail
    /// offset) without having to reach back into the crossterm
    /// backend for the terminal size.
    last_tree_rect: Rect,
    /// Current level of detail for the scenario tree. Left/right
    /// arrow keys cycle through [`TreeLod`].
    tree_lod: TreeLod,
    /// Whether the one-shot startup reset has already run. Flipped on
    /// the first frame that has at least one selectable phase entry so
    /// the user doesn't have to press Space to get the initial focus.
    startup_reset_done: bool,
    /// Whether the help overlay is currently visible. Toggled by `?`
    /// and dismissed by `Esc`.
    show_help: bool,
    /// Running count of consecutive recent `q` presses, tracked so
    /// three rapid taps can hard-exit the whole process rather than
    /// just dismissing the TUI. Reset whenever the gap since the last
    /// tap exceeds [`Self::Q_TAP_WINDOW_MS`].
    q_tap_count: u32,
    /// Timestamp of the previous `q` press, used to decide whether the
    /// current press continues an existing tap streak or starts a
    /// fresh one.
    q_tap_last: Option<Instant>,
    /// Which latency view is rendered. Cycled with Shift+L. See
    /// [`LatencyView`] for the three styles.
    latency_view: LatencyView,
    /// When true, the TUI freezes its metrics / anim state so the
    /// user can read values without them shifting. The screen keeps
    /// redrawing (so a PAUSED banner renders and keys stay live), but
    /// `drain_frames`, scroll-anim ticking, and auto-selection refresh
    /// are skipped. Toggled with `p`.
    paused: bool,
    /// Frozen snapshot of [`RunState`], populated when the user
    /// toggles pause on. While `Some`, every `draw`-time read of
    /// `run_state` routes to this clone instead of the live
    /// `Arc<RwLock<RunState>>`, so observer-driven mutations
    /// (ops/s, latency percentiles, relevancy, new log entries)
    /// don't leak into the rendered frame. Cleared on unpause.
    frozen_state: Option<crate::state::RunState>,
    /// Shift+P sets this; the event loop captures the just-drawn
    /// buffer after the next `terminal.draw()` and writes it to a
    /// dump file. Read-after-draw is the only way to capture the
    /// front buffer — `current_buffer_mut` returns the back buffer
    /// (reset to empty after each draw).
    dump_requested: bool,
    /// User-pinned "past span" for the latency bar chart overlay
    /// (SRD-42 phase 9). `Some(d)` adds a `past <d>` row to each
    /// percentile group rendered via
    /// `MetricsQuery::recent_window(d)`.
    past_span: Option<Duration>,
    /// Currently-editing past-span input, if any. `/` opens it;
    /// Enter commits; Esc cancels.
    past_input: Option<PastInputState>,
    /// Shared `MetricsQuery` access layer (SRD-42). The barchart
    /// view reads per-cadence percentile snapshots out of this —
    /// no TaggedFrame channels, no per-component windowed state.
    metrics_query: Arc<MetricsQuery>,
}

/// A compact per-percentile snapshot extracted from a [`MetricSet`]'s
/// `cycles_servicetime` histogram family.
#[derive(Clone, Debug, Default)]
struct LatencySnapshot {
    sample_count: u64,
    min: u64,
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    max: u64,
}

/// Inline input buffer for the latency panel's `past <span>` overlay.
#[derive(Clone, Debug, Default)]
struct PastInputState {
    /// The duration string being typed (e.g., `5m`, `1h30m`).
    buf: String,
    /// Most recent parse error from the last Enter attempt, if any.
    /// Cleared when the user types.
    error: Option<String>,
}

impl LatencySnapshot {
    /// Extract percentiles from the first series in the
    /// `cycles_servicetime` family. Selection filtering at the
    /// `MetricsQuery` layer is expected to have already scoped the
    /// returned `MetricSet` to a single phase's labels — rendering
    /// is per-phase by requirement, not cross-phase-merged.
    fn from_metric_set(snap: &MetricSet) -> Self {
        let Some(family) = snap.family("cycles_servicetime") else {
            return Self::default();
        };
        let Some(metric) = family.metrics().next() else { return Self::default(); };
        let Some(point) = metric.point() else { return Self::default(); };
        let MetricValue::Histogram(h) = point.value() else { return Self::default(); };
        let r = &h.reservoir;
        Self {
            sample_count: h.count,
            min:  if h.count == 0 { 0 } else { r.min() },
            p50:  r.value_at_quantile(0.50),
            p90:  r.value_at_quantile(0.90),
            p99:  r.value_at_quantile(0.99),
            p999: r.value_at_quantile(0.999),
            max:  if h.count == 0 { 0 } else { r.max() },
        }
    }
}

/// Build a [`Selection`] scoped to the currently-active phase's
/// dimensional labels. Returns `None` if no phase is active — in
/// which case the bar chart renders empty, consistent with "there's
/// no phase-specific latency data to show".
fn active_phase_selection(state: &RunState, family: &str) -> Option<Selection> {
    let active = state.first_active()?;
    let mut sel = Selection::family(family);
    // `phase=<name>` is always present as a component label.
    sel = sel.with_label("phase", &active.name);
    // Additional dimensional labels come from the for_each bindings
    // in the phase_labels string: "key=value, key2=value2".
    for part in active.labels.split(',') {
        let part = part.trim();
        if part.is_empty() { continue; }
        if let Some((k, v)) = part.split_once('=') {
            sel = sel.with_label(k.trim(), v.trim());
        }
    }
    Some(sel)
}

/// Build the per-phase latency rows for the tree's expanded detail
/// block. Mirrors the old top-level Latency panel (the `Percentiles`
/// variant): one `range` row showing the axis endpoints and a
/// distribution bar with cross-bar peak markers, plus one row per
/// percentile showing its value and a bar anchored at 0..max_val.
/// Rows that would render with zero value are suppressed.
///
/// `peak_5s` / `peak_10s` are the rolling max latencies from the
/// phase's [`nb_metrics::summaries::peak_tracker::PeakTracker`]s.
/// `None` means "no data yet" (e.g. completed phase, or phase
/// that hasn't received any frames) — no marker renders in that
/// case.
fn latency_detail_lines(
    min_ns: u64,
    p50_ns: u64,
    p90_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
    peak_5s: Option<u64>,
    peak_10s: Option<u64>,
) -> Vec<Line<'static>> {
    let max_val = max_ns.max(p999_ns).max(p99_ns).max(p90_ns).max(p50_ns).max(1);
    // Fixed bar width so the detail block lines up predictably
    // regardless of terminal size. 60 cells is wide enough to show
    // percentile spread without eating the whole 120-col budget.
    let bar_w: usize = 60;
    let pos = |nanos: u64| -> usize {
        if max_val == 0 { return 0; }
        ((nanos as f64 / max_val as f64) * bar_w as f64).round() as usize
    };

    let mut out: Vec<Line<'static>> = Vec::new();

    // Range row: colored distribution bar with segment transitions
    // at each percentile boundary.
    let mut cells: Vec<(char, ratatui::style::Color)> =
        vec![('╌', colors::BORDER); bar_w];
    let points = [
        (0u64, colors::LAT_P50),
        (p50_ns, colors::LAT_P50),
        (p90_ns, colors::LAT_P90),
        (p99_ns, colors::LAT_P99),
        (p999_ns, colors::LAT_MAX),
        (max_ns, colors::LAT_MAX),
    ];
    for w in points.windows(2) {
        let start = pos(w[0].0).min(bar_w);
        let end = pos(w[1].0).min(bar_w);
        for c in cells.iter_mut().take(end).skip(start) {
            c.0 = '━';
            c.1 = w[1].1;
        }
    }
    // Peak markers from the phase's PeakTracker summaries —
    //   ╪  5s peak
    //   ╫  10s peak
    //   ╬  both peaks share the same cell (collapsed)
    let peak_pos = |nanos: u64| -> Option<usize> {
        if nanos == 0 || bar_w == 0 { return None; }
        Some(pos(nanos).min(bar_w.saturating_sub(1)))
    };
    let p5 = peak_5s.and_then(peak_pos);
    let p10 = peak_10s.and_then(peak_pos);
    match (p5, p10) {
        (Some(a), Some(b)) if a == b => {
            cells[a] = ('╬', colors::EMPHASIS);
        }
        _ => {
            if let Some(p) = p10 { cells[p] = ('╫', colors::LAT_MAX); }
            if let Some(p) = p5  { cells[p] = ('╪', colors::EMPHASIS); }
        }
    }
    // Group adjacent cells of the same color into one span so the
    // render is O(bands) not O(cells).
    let range_label = if min_ns > 0 {
        format!("{}..{}", widgets::format_nanos(min_ns),
                          widgets::format_nanos(max_val))
    } else {
        format!("0..{}", widgets::format_nanos(max_val))
    };
    let mut spans: Vec<Span<'static>> = vec![
        Span::styled(" range", Style::default().fg(colors::DIM)),
        Span::styled(format!(" {:>14}  ", range_label),
            Style::default().fg(colors::DIM)),
    ];
    let mut i = 0;
    while i < cells.len() {
        let col = cells[i].1;
        let mut j = i;
        let mut run = String::new();
        while j < cells.len() && cells[j].1 == col {
            run.push(cells[j].0);
            j += 1;
        }
        spans.push(Span::styled(run, Style::default().fg(col)));
        i = j;
    }
    out.push(Line::from(spans));

    // One row per percentile; skip if the value is zero (no data).
    let rows: [(&str, u64, ratatui::style::Color); 6] = [
        ("min ", min_ns,  colors::LAT_P50),
        ("p50 ", p50_ns,  colors::LAT_P50),
        ("p90 ", p90_ns,  colors::LAT_P90),
        ("p99 ", p99_ns,  colors::LAT_P99),
        ("p999", p999_ns, colors::LAT_MAX),
        ("max ", max_ns,  colors::LAT_MAX),
    ];
    for (label, nanos, color) in rows.iter() {
        if *nanos == 0 { continue; }
        let frac = *nanos as f64 / max_val as f64;
        let bar = widgets::bar_str(frac.min(1.0), bar_w);
        out.push(Line::from(vec![
            Span::styled(format!(" {label}"), Style::default().fg(colors::DIM)),
            Span::styled(
                format!(" {:>14}  ", widgets::format_nanos(*nanos)),
                Style::default().fg(*color).bold(),
            ),
            Span::styled(bar, Style::default().fg(*color)),
        ]));
    }
    out
}

/// Plural-form label for cursor counts. `ops_finished` ticks once
/// per cycle = once per cursor position, so the natural unit is the
/// cursor name pluralized. Falls back to `cycles` when the phase
/// declares no cursor.
fn cursor_count_label(cursor_name: &str) -> String {
    let name = cursor_name.trim();
    if name.is_empty() || name == "?" {
        return "cycles".to_string();
    }
    if name.ends_with('s') {
        name.to_string()
    } else {
        format!("{name}s")
    }
}

/// Per-second form of [`cursor_count_label`] (e.g. `row` → `rows/s`).
fn cursor_rate_label(cursor_name: &str) -> String {
    format!("{}/s", cursor_count_label(cursor_name))
}

/// Health-tint color for a phase entry in the scenario tree.
///
/// Running phases always render yellow — error counts aren't
/// meaningful mid-flight. Completed phases grade green → orange →
/// red as the observed error rate rises past the clean/warning/bad
/// thresholds. The hue gradient was picked for colorblind safety;
/// paired with the phase glyph (`▶` / `✓` / `✗`) the meaning is
/// redundant-encoded.
fn phase_health_color(phase: &crate::state::PhaseEntry) -> ratatui::style::Color {
    match &phase.status {
        PhaseStatus::Running => colors::PHASE_RUNNING_TINT,
        PhaseStatus::Pending => colors::PHASE_PENDING,
        PhaseStatus::Failed(_) => colors::PHASE_FAILED,
        PhaseStatus::Completed => {
            // Error *rate*, not raw count: a 1M-op phase with 100
            // errors (0.01%) is effectively clean, but a 500-op phase
            // with 100 errors (20%) is degraded. `errors / ops_finished`
            // falls to zero for 0-error runs without special-casing.
            let Some(sm) = phase.summary.as_ref() else {
                return colors::PHASE_DONE;
            };
            if sm.errors == 0 {
                return colors::PHASE_DONE;
            }
            let denom = sm.ops_finished.max(1);
            let rate = sm.errors as f64 / denom as f64;
            // Thresholds: < 1% warn, ≥ 5% bad. Between 1%-5% = warn.
            if rate >= 0.05 {
                colors::PHASE_DONE_BAD
            } else {
                colors::PHASE_DONE_WARN
            }
        }
    }
}

/// Three alternate visualizations of the latency panel. Cycled with
/// Shift+L so the user can pick the framing most useful for the
/// current question.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LatencyView {
    /// Custom per-percentile bars plus a range row with cross-bar
    /// peak markers (the original view).
    Percentiles,
    /// Ratatui [`BarChart`] of the p50 / p90 / p99 / p999 / max
    /// values as categorical bars.
    BarChart,
    /// Ratatui [`Chart`] plotting p50 / p99 / max over time as
    /// scrolling line graphs.
    Timeseries,
}

impl LatencyView {
    fn next(self) -> Self {
        match self {
            Self::Percentiles => Self::BarChart,
            Self::BarChart    => Self::Timeseries,
            Self::Timeseries  => Self::Percentiles,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Percentiles => "percentiles",
            Self::BarChart    => "barchart",
            Self::Timeseries  => "timeseries",
        }
    }
}

impl App {
    /// Create with a metrics channel and shared run state.
    pub fn new(
        frame_rx: mpsc::Receiver<MetricSet>,
        run_state: Arc<RwLock<RunState>>,
        metrics_query: Arc<MetricsQuery>,
    ) -> Self {
        Self {
            should_quit: false,
            should_exit_process: false,
            tick_rate: Duration::from_millis(250),
            show_log: false,
            frame_rx,
            run_state,
            focused: None,
            tree_scroll: None,
            tree_display: 0.0,
            tree_anim: None,
            tree_selected: None,
            tree_selection_auto: true,
            tree_expanded: None,
            last_tree_rect: Rect::new(0, 0, 0, 0),
            tree_lod: TreeLod::Default,
            startup_reset_done: false,
            show_help: false,
            q_tap_count: 0,
            q_tap_last: None,
            latency_view: LatencyView::Percentiles,
            paused: false,
            frozen_state: None,
            dump_requested: false,
            past_span: None,
            past_input: None,
            metrics_query,
        }
    }

    /// Smoothstep easing — S-curve between 0 and 1. Used for scroll
    /// animations so the viewport eases in and out instead of lurching.
    #[inline]
    fn smoothstep(t: f32) -> f32 {
        let t = t.clamp(0.0, 1.0);
        t * t * (3.0 - 2.0 * t)
    }

    /// Per-scroll-wheel step in lines. Small so a single tick feels like
    /// a proportionate motion rather than a jump.
    const SCROLL_STEP: i32 = 3;

    /// Duration of a single scroll tween. Short enough to feel snappy,
    /// long enough that a rendered transition is visible.
    const SCROLL_ANIM_MS: u64 = 140;

    /// Max gap (ms) between `q` presses to count as part of the same
    /// tap streak. Three taps inside this window hard-exits the
    /// process; isolated taps still just dismiss the TUI.
    const Q_TAP_WINDOW_MS: u128 = 600;

    /// Number of `q` presses inside [`Q_TAP_WINDOW_MS`] that triggers a
    /// full process exit.
    const Q_TAP_EXIT_COUNT: u32 = 3;

    /// Run the TUI event loop. Blocks until quit or run completes.
    pub fn run(&mut self) -> io::Result<()> {
        terminal::enable_raw_mode()?;
        io::stderr().execute(EnterAlternateScreen)?;
        io::stderr().execute(EnableMouseCapture)?;
        let backend = CrosstermBackend::new(io::stderr());
        let mut terminal = Terminal::new(backend)?;

        let result = self.event_loop(&mut terminal);

        let _ = io::stderr().execute(DisableMouseCapture);
        terminal::disable_raw_mode()?;
        io::stderr().execute(LeaveAlternateScreen)?;

        // Triple-tap q requests a full process exit. Do it here, after
        // the terminal has been restored, so the shell doesn't inherit
        // raw mode or an alternate-screen buffer.
        if self.should_exit_process {
            std::process::exit(130);
        }

        result
    }

    fn event_loop(&mut self, terminal: &mut Terminal<CrosstermBackend<io::Stderr>>) -> io::Result<()> {
        let mut last_drain = Instant::now();

        loop {
            // Advance any in-flight scroll tween BEFORE drawing so the
            // intermediate eased offset is what gets rendered.
            let size = terminal.size()?;
            let tree_rect = self.tree_rect(Rect::new(0, 0, size.width, size.height));
            self.last_tree_rect = tree_rect;
            let visible = tree_rect.height.saturating_sub(2) as usize;
            let total = self.run_state.read()
                .map(|s| s.phases.len())
                .unwrap_or(0);
            let tail = total.saturating_sub(visible);
            let target = self.tree_scroll.unwrap_or(tail).min(tail) as f32;
            // Pause freezes the state the user is looking at — skip
            // the anim tick and auto-tracking refresh so nothing
            // shifts. The frame still redraws so the PAUSED banner
            // can render and key input stays live.
            if !self.paused {
                self.advance_scroll_anim(target);
                // One-shot: when the first selectable phase appears AND
                // the layout has settled, run the same state change as
                // pressing Space so the user never sees the idle
                // "waiting for phase" state with no tree focus.
                if !self.startup_reset_done && self.default_tree_selection().is_some() {
                    let layout_ready = self.run_state.read()
                        .map(|s| s.active_phases.values().any(|a| a.cursor_extent > 0)
                              || s.phases.iter().any(|p| p.summary.is_some()))
                        .unwrap_or(false);
                    if layout_ready {
                        self.reset_tree_to_active();
                        self.startup_reset_done = true;
                    }
                }
                self.refresh_auto_selection();
            }

            let completed = terminal.draw(|frame| self.draw(frame))?;
            // If the user pressed Shift+P, dump the just-rendered
            // buffer now — CompletedFrame holds a reference to the
            // freshly-flushed front buffer (the one the user can see).
            if self.dump_requested {
                let area = completed.area;
                // Borrow the buffer via a dedicated scope so the
                // completed-frame lifetime doesn't conflict with the
                // rest of the loop.
                let mut text = String::new();
                let ts = nb_activity::session::now_log_timestamp();
                let w = area.width as usize;
                let h = area.height as usize;
                text.push_str(&format!("# nb-tui screen dump — {ts}\n"));
                text.push_str(&format!("# dimensions: {w}x{h}\n"));
                for y in area.y..(area.y + area.height) {
                    for x in area.x..(area.x + area.width) {
                        text.push_str(completed.buffer[(x, y)].symbol());
                    }
                    text.push('\n');
                }
                self.write_dump(text, &ts);
                self.dump_requested = false;
            }
            let last_draw = Instant::now();

            // While a scroll animation is in flight, target ~60Hz so the
            // tween reads as motion; otherwise sit at the baseline 4Hz
            // metrics tick. `last_draw` tracks the independent draw
            // cadence so the drain_frames schedule below stays stable.
            let effective_rate = if self.tree_anim.is_some() {
                Duration::from_millis(16)
            } else {
                self.tick_rate
            };
            let timeout = effective_rate
                .checked_sub(last_draw.elapsed())
                .unwrap_or(Duration::ZERO);

            if event::poll(timeout)? {
                match event::read()? {
                    Event::Key(key) if key.kind == KeyEventKind::Press => {
                        // Ctrl+L follows the long-standing terminal
                        // convention of "refresh screen", handled as a
                        // dedicated branch so it doesn't collapse into
                        // the plain `l` log toggle.
                        if key.modifiers.contains(KeyModifiers::CONTROL)
                            && matches!(key.code, KeyCode::Char('l') | KeyCode::Char('L'))
                        {
                            let _ = terminal.clear();
                            continue;
                        }
                        // Past-span input mode consumes all key events
                        // until the user presses Enter or Esc.
                        if self.past_input.is_some() {
                            self.handle_past_input_key(key.code);
                            continue;
                        }
                        match key.code {
                            KeyCode::Char('q') => self.handle_q_tap(),
                            KeyCode::Esc => self.handle_escape(),
                            KeyCode::Char('l') => self.show_log = !self.show_log,
                            KeyCode::Char('L') => self.latency_view = self.latency_view.next(),
                            KeyCode::Char('p') => self.toggle_pause(),
                            KeyCode::Char('P') => self.dump_requested = true,
                            KeyCode::Char('?') => self.show_help = !self.show_help,
                            KeyCode::Char('/') => self.open_past_input(),
                            KeyCode::Up => self.move_tree_selection(-1),
                            KeyCode::Down => self.move_tree_selection(1),
                            KeyCode::Left => self.adjust_tree_lod(-1),
                            KeyCode::Right => self.adjust_tree_lod(1),
                            KeyCode::Enter => self.toggle_tree_expansion(),
                            KeyCode::Char(' ') => self.reset_tree_to_active(),
                            _ => {}
                        }
                    }
                    Event::Mouse(me) => {
                        let size = terminal.size()?;
                        self.handle_mouse(me, Rect::new(0, 0, size.width, size.height));
                    }
                    _ => {}
                }
            }

            // Apply any pending q-tap dismiss once the triple window
            // has elapsed without a third tap.
            self.poll_q_tap_timeout();

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

            if !self.paused && last_drain.elapsed() >= self.tick_rate {
                self.drain_frames();
                last_drain = Instant::now();
            }
        }

        Ok(())
    }

    /// Compute the scenario-tree panel rect for a given terminal size.
    /// Kept in sync with the layout in `draw()` — Header / Scenario /
    /// Footer, with an optional log split under the Scenario section.
    fn tree_rect(&self, area: Rect) -> Rect {
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),   // header
                Constraint::Min(3),      // scenario (+ log panel)
                Constraint::Length(1),   // footer
            ])
            .split(area);
        if self.show_log {
            let bottom = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(50),
                    Constraint::Percentage(50),
                ])
                .split(sections[1]);
            bottom[0]
        } else {
            sections[1]
        }
    }

    /// Is (col, row) inside `rect`?
    fn rect_contains(rect: Rect, col: u16, row: u16) -> bool {
        col >= rect.x && col < rect.x + rect.width
            && row >= rect.y && row < rect.y + rect.height
    }

    /// Dispatch a mouse event to the focused scrollable pane.
    fn handle_mouse(&mut self, me: MouseEvent, area: Rect) {
        let tree = self.tree_rect(area);
        match me.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if Self::rect_contains(tree, me.column, me.row) {
                    // Take focus. On first grab, seed the scroll offset
                    // at the current auto-tail position so the view
                    // doesn't jump — the user expects the tree to stay
                    // put at the moment they clicked.
                    let tail = self.tree_tail_offset(tree);
                    if self.tree_scroll.is_none() {
                        self.tree_scroll = Some(tail);
                        self.tree_display = tail as f32;
                    }
                    let was_unfocused = self.focused != Some(FocusedPane::Tree);
                    self.focused = Some(FocusedPane::Tree);

                    // Figure out which tree entry sits under the click
                    // and select it. The row inside the tree panel maps
                    // to rendered-line index plus the current scroll
                    // offset; the rendered-line → entry mapping accounts
                    // for expanded detail blocks.
                    let inner_y = me.row.saturating_sub(tree.y + 1); // minus border
                    let start = self.tree_display.round().max(0.0) as usize;
                    let clicked_line = start + inner_y as usize;
                    if let Some(idx) = self.line_to_entry_index(clicked_line) {
                        self.tree_selected = Some(idx);
                        self.tree_selection_auto = false;
                    } else if was_unfocused {
                        // Empty area click — just take focus, seed default.
                        self.tree_selected = self.default_tree_selection();
                    }
                } else {
                    self.focused = None;
                    self.tree_selected = None;
                    self.tree_selection_auto = true;
                    self.tree_expanded = None;
                }
            }
            // Direction follows the "content tracks the wheel" convention:
            // rolling the wheel up moves the viewport down (reveals later
            // content) and vice versa. Matches typical IDE / pager
            // trackpad behavior on macOS and browsers with natural scroll.
            MouseEventKind::ScrollUp => {
                if self.focused == Some(FocusedPane::Tree) {
                    let tail = self.tree_tail_offset(tree);
                    let current = self.tree_scroll.unwrap_or(tail);
                    let next = (current as i32 + Self::SCROLL_STEP).clamp(0, tail as i32) as usize;
                    // At the bottom — snap back to auto-follow so new
                    // entries keep scrolling into view.
                    let target_opt = if next >= tail { None } else { Some(next) };
                    self.retarget_tree_scroll(target_opt, tail);
                }
            }
            MouseEventKind::ScrollDown => {
                if self.focused == Some(FocusedPane::Tree) {
                    let tail = self.tree_tail_offset(tree);
                    let current = self.tree_scroll.unwrap_or(tail);
                    let next = (current as i32 - Self::SCROLL_STEP).max(0) as usize;
                    self.retarget_tree_scroll(Some(next), tail);
                }
            }
            _ => {}
        }
    }

    /// Update the scroll target and start (or retarget) the tween from
    /// the current displayed position. A fresh mouse-wheel event mid-
    /// animation keeps the visual position put and just re-aims — it
    /// doesn't snap back to the previous starting point.
    fn retarget_tree_scroll(&mut self, target: Option<usize>, tail: usize) {
        self.tree_scroll = target;
        let target_f = target.unwrap_or(tail) as f32;
        self.tree_anim = Some((
            self.tree_display,
            target_f,
            Instant::now(),
            Duration::from_millis(Self::SCROLL_ANIM_MS),
        ));
    }

    /// Persist a rendered screen dump to disk. Writes two files in
    /// the current session directory (via `logs/latest`):
    ///   - `tui_{ts}.dump` — a timestamped archive (one per dump)
    ///   - `tui.dump`       — a stable-named copy of the latest dump
    ///
    /// The `logs/tui.dump` symlink is pre-created at session start
    /// (see [`nb_activity::session::Session::new`]) and points at
    /// `latest/tui.dump`, so it becomes live the moment this
    /// function writes the stable-named copy.
    fn write_dump(&mut self, text: String, ts: &str) {
        let logs_dir = std::path::PathBuf::from("logs");
        let session_dir = logs_dir.join("latest");
        // `logs/latest` exists as a symlink once Session::new has run.
        // If it's not there yet (very early failure), fall back to the
        // logs root — the eager `logs/tui.dump` symlink won't resolve,
        // but the timestamped archive still lands somewhere visible.
        let target_dir = if session_dir.exists() { &session_dir } else { &logs_dir };
        let fname_ts = ts.replace([':', ' ', '.'], "_").replace('-', "");
        let archive_name = format!("tui_{fname_ts}.dump");
        let archive_path = target_dir.join(&archive_name);
        let stable_path = target_dir.join("tui.dump");
        let archive_result = std::fs::write(&archive_path, &text);
        let stable_result = std::fs::write(&stable_path, &text);
        match (archive_result, stable_result) {
            (Ok(()), Ok(())) => {
                nb_activity::observer::log(
                    nb_activity::observer::LogLevel::Info,
                    &format!("tui dump written to {} (also {})",
                        archive_path.display(), stable_path.display()),
                );
            }
            (Err(e), _) | (_, Err(e)) => {
                nb_activity::observer::log(
                    nb_activity::observer::LogLevel::Warn,
                    &format!("failed to write tui dump {}: {e}", archive_path.display()),
                );
            }
        }
    }

    /// Handle a `q` keypress.
    ///
    /// Single/double tap → dismisses the TUI (same as Esc at top level),
    /// but the dismiss is DEFERRED until the triple-tap window expires.
    /// Otherwise a fast third tap would arrive after the TUI was already
    /// tearing down, never triggering the process-exit path the user
    /// Toggle the paused flag. On pause, clones the live `RunState`
    /// into `self.frozen_state` so subsequent `draw` calls render
    /// from the snapshot instead of the live store — otherwise
    /// observer updates (ops/s, latency, phase progress) continue
    /// to leak into the rendered frame even though `drain_frames`
    /// has stopped. On unpause, the snapshot is dropped and the
    /// next draw picks up the live state again.
    fn toggle_pause(&mut self) {
        if self.paused {
            self.paused = false;
            self.frozen_state = None;
        } else {
            // Clone under the read lock, then release before
            // flipping `paused`. A poisoned lock leaves us with
            // nothing to freeze — fall back to not pausing at all
            // so the display keeps working.
            let snap = self.run_state.read().ok().map(|g| g.clone());
            if let Some(s) = snap {
                self.frozen_state = Some(s);
                self.paused = true;
            }
        }
    }

    /// actually wanted. The loop calls `poll_q_tap_timeout` every frame
    /// to apply the deferred action once the streak is confirmed.
    ///
    /// Triple tap inside [`Q_TAP_WINDOW_MS`] fires immediately: hard-
    /// exit the whole nbrs process after the terminal is restored.
    fn handle_q_tap(&mut self) {
        let now = Instant::now();
        let in_streak = self.q_tap_last
            .map(|prev| now.duration_since(prev).as_millis() <= Self::Q_TAP_WINDOW_MS)
            .unwrap_or(false);
        self.q_tap_count = if in_streak { self.q_tap_count + 1 } else { 1 };
        self.q_tap_last = Some(now);
        if self.q_tap_count >= Self::Q_TAP_EXIT_COUNT {
            // Triple confirmed — act now.
            self.should_exit_process = true;
            self.should_quit = true;
        }
        // Otherwise wait. poll_q_tap_timeout applies the dismiss once
        // we're confident no further tap is coming.
    }

    /// Called each event-loop iteration. If a q-tap streak is open and
    /// the window has expired with 1 or 2 presses, apply the deferred
    /// TUI dismiss. Three-press streaks already acted inline.
    fn poll_q_tap_timeout(&mut self) {
        let Some(last) = self.q_tap_last else { return; };
        if self.q_tap_count == 0 { return; }
        if last.elapsed().as_millis() < Self::Q_TAP_WINDOW_MS { return; }
        if self.q_tap_count < Self::Q_TAP_EXIT_COUNT {
            self.should_quit = true;
        }
        self.q_tap_count = 0;
        self.q_tap_last = None;
    }

    /// Escape is a cascading dismiss:
    /// 1) If the help overlay is up, close it.
    /// 2) If an entry is expanded, collapse it.
    /// 3) Otherwise if the tree has focus, release focus + selection.
    /// 4) Otherwise quit the TUI (matches `q`).
    fn handle_escape(&mut self) {
        if self.show_help {
            self.show_help = false;
        } else if self.tree_expanded.is_some() {
            self.tree_expanded = None;
        } else if self.focused == Some(FocusedPane::Tree) {
            self.focused = None;
            self.tree_selected = None;
            self.tree_selection_auto = true;
        } else {
            self.should_quit = true;
        }
    }

    /// Open the latency panel's `past span` input. Pre-fills the
    /// buffer with the currently-pinned span (if any) so the user
    /// can edit it rather than retype.
    fn open_past_input(&mut self) {
        let buf = self.past_span
            .map(format_span_short)
            .unwrap_or_default();
        self.past_input = Some(PastInputState { buf, error: None });
    }

    /// Handle one keystroke while the past-span input is open.
    fn handle_past_input_key(&mut self, code: KeyCode) {
        let Some(state) = self.past_input.as_mut() else { return; };
        match code {
            KeyCode::Esc => { self.past_input = None; }
            KeyCode::Enter => {
                let input = state.buf.trim().to_string();
                if input.is_empty() {
                    self.past_span = None;
                    self.past_input = None;
                    return;
                }
                match parse_span_short(&input) {
                    Ok(d) => {
                        self.past_span = Some(d);
                        self.past_input = None;
                    }
                    Err(msg) => {
                        state.error = Some(msg);
                    }
                }
            }
            KeyCode::Backspace => {
                state.buf.pop();
                state.error = None;
            }
            KeyCode::Char(c) => {
                // Only accept chars that could be part of a duration
                // (digits + unit letters). Keeps stray keys from
                // polluting the buffer.
                if c.is_ascii_digit() || matches!(c, 's' | 'm' | 'h' | 'd') {
                    state.buf.push(c);
                    state.error = None;
                }
            }
            _ => {}
        }
    }

    /// Move the tree selection by `delta` phase entries, skipping scope
    /// headers. Grabs tree focus if it wasn't already held, and seeds
    /// the selection with the most-recently-active phase on first use.
    /// Any manual movement disables auto-tracking so the selection
    /// stops drifting with newly-running phases.
    fn move_tree_selection(&mut self, delta: i32) {
        if self.focused != Some(FocusedPane::Tree) {
            self.focused = Some(FocusedPane::Tree);
        }
        if self.tree_selected.is_none() {
            self.tree_selected = self.default_tree_selection();
        }
        self.tree_selection_auto = false;
        if let Some(start) = self.tree_selected {
            if let Some(next) = self.step_selectable_for_lod(start, delta) {
                self.tree_selected = Some(next);
            }
        }
        self.scroll_selection_into_view();
    }

    /// LOD-aware step. Skips over entries that render zero rows
    /// (scope headers in Minimal/Focus, pending/completed phases
    /// in Focus) so the selection never parks at an invisible
    /// line index — which would cause `scroll_selection_into_view`
    /// to silently bail and leave the viewport out of sync with
    /// `tree_selected`.
    fn step_selectable_for_lod(&self, start: usize, delta: i32) -> Option<usize> {
        let s = self.run_state.read().ok()?;
        let len = s.phases.len() as i32;
        if len == 0 { return None; }
        let step = if delta >= 0 { 1 } else { -1 };
        let mut idx = start as i32 + step;
        while idx >= 0 && idx < len {
            let i = idx as usize;
            let phase = &s.phases[i];
            if phase.kind == crate::state::EntryKind::Phase
                && self.rendered_lines_for(i, phase) > 0
            {
                return Some(i);
            }
            idx += step;
        }
        None
    }

    /// If the currently selected entry's rendered line sits outside the
    /// visible viewport, retarget the scroll animation so it just comes
    /// into view. Top-aligned if the selection is above the window,
    /// bottom-aligned if below. No-op when already in view.
    fn scroll_selection_into_view(&mut self) {
        let Some(sel) = self.tree_selected else { return; };
        let visible = self.last_tree_rect.height.saturating_sub(2) as usize;
        if visible == 0 { return; }

        // Rendered-line index of the selected entry, accounting for any
        // currently expanded detail block above it.
        let Some(line) = self.entry_to_line_index(sel) else { return; };

        let total = self.total_rendered_lines();
        let tail = total.saturating_sub(visible);
        let current_top = self.tree_scroll.unwrap_or(tail);
        let current_bottom = current_top.saturating_add(visible).saturating_sub(1);

        let new_top = if line < current_top {
            // Selection is above the viewport — bring it to the top.
            line
        } else if line > current_bottom {
            // Selection is below the viewport — pin it to the bottom row.
            line.saturating_sub(visible.saturating_sub(1))
        } else {
            return; // already visible
        };
        let clamped = new_top.min(tail);
        let target = if clamped >= tail { None } else { Some(clamped) };
        self.retarget_tree_scroll(target, tail);
    }

    /// Total number of rendered lines in the tree (phase rows + detail
    /// rows of any expanded entry). Matches the iteration in `draw_tree`.
    fn total_rendered_lines(&self) -> usize {
        let Ok(s) = self.run_state.read() else { return 0; };
        s.phases.iter().enumerate()
            .map(|(i, p)| self.rendered_lines_for(i, p))
            .sum()
    }

    /// Inverse of [`line_to_entry_index`]: given a `state.phases` index,
    /// return its rendered-line index (0-based, pre-scroll). `None` if
    /// the entry doesn't exist.
    fn entry_to_line_index(&self, entry: usize) -> Option<usize> {
        let s = self.run_state.read().ok()?;
        if entry >= s.phases.len() { return None; }
        // Entries that render zero rows (scope headers in Minimal) have
        // no line position — treat them as absent for navigation.
        if self.rendered_lines_for(entry, &s.phases[entry]) == 0 {
            return None;
        }
        let mut cursor = 0usize;
        for (i, phase) in s.phases.iter().enumerate() {
            if i == entry { return Some(cursor); }
            cursor += self.rendered_lines_for(i, phase);
        }
        None
    }


    /// Default tree selection when focus is first grabbed: the most
    /// recently active phase. Prefers the current Running entry, falls
    /// back to the latest Completed entry, then the first Phase.
    fn default_tree_selection(&self) -> Option<usize> {
        let s = self.run_state.read().ok()?;
        let phases: &[crate::state::PhaseEntry] = &s.phases;
        // Only consider entries that actually render under the
        // current LOD — Focus hides non-running phases, so the
        // default selection can't usefully land on one.
        let renderable = |(i, p): &(usize, &crate::state::PhaseEntry)| {
            p.kind == crate::state::EntryKind::Phase
                && self.rendered_lines_for(*i, p) > 0
        };
        phases.iter().enumerate()
            .filter(renderable)
            .rfind(|(_, p)| matches!(p.status, crate::state::PhaseStatus::Running))
            .or_else(|| phases.iter().enumerate()
                .filter(renderable)
                .rfind(|(_, p)| matches!(p.status, crate::state::PhaseStatus::Completed)))
            .or_else(|| phases.iter().enumerate()
                .find(renderable))
            .map(|(i, _)| i)
    }

    /// Cycle the tree level of detail. Positive `delta` increases LOD
    /// (more detail), negative decreases. Clamped at the ends.
    ///
    /// Grabs tree focus if not held, and scrolls the current selection
    /// back into view since the rendered-line coordinates shift when
    /// the detail model changes.
    fn adjust_tree_lod(&mut self, delta: i32) {
        if self.focused != Some(FocusedPane::Tree) {
            self.focused = Some(FocusedPane::Tree);
        }
        if self.tree_selected.is_none() {
            self.tree_selected = self.default_tree_selection();
            self.tree_selection_auto = false;
        }
        self.tree_lod = if delta > 0 { self.tree_lod.next() } else { self.tree_lod.prev() };
        self.scroll_selection_into_view();
    }

    /// Number of rendered lines an entry contributes under the current
    /// LOD + expansion state. Rules:
    ///   * Scope entry in Minimal or Focus → hidden (0 rows).
    ///   * Focus mode also hides non-running phases.
    ///   * Phase entry in Maximal or Focus → 1 + full detail block.
    ///   * Any phase the user has explicitly pinned via Enter → 1 +
    ///     detail, regardless of LOD.
    ///   * Otherwise → 1 row.
    fn rendered_lines_for(&self, entry_idx: usize, phase: &crate::state::PhaseEntry) -> usize {
        if matches!(self.tree_lod, TreeLod::Minimal | TreeLod::Focus)
            && phase.kind == crate::state::EntryKind::Scope
        {
            return 0;
        }
        if self.tree_lod == TreeLod::Focus
            && phase.kind == crate::state::EntryKind::Phase
            && !matches!(phase.status, crate::state::PhaseStatus::Running)
        {
            return 0;
        }
        if phase.kind != crate::state::EntryKind::Phase {
            return 1;
        }
        let expanded = self.tree_expanded == Some(entry_idx)
            || self.tree_lod == TreeLod::Maximal
            || self.tree_lod == TreeLod::Focus;
        if expanded {
            1 + self.detail_line_count_for(phase)
        } else {
            1
        }
    }

    /// Count of detail lines for a phase using live state when the
    /// phase is currently running. Keeps line-offset math in
    /// `total_rendered_lines` / `entry_to_line_index` /
    /// `line_to_entry_index` consistent with `draw_tree` output.
    fn detail_line_count_for(&self, phase: &crate::state::PhaseEntry) -> usize {
        if let Ok(s) = self.run_state.read() {
            if let Some(a) = s.active_phase(&phase.name, &phase.labels) {
                // Clone the active snapshot out of the read guard so we
                // can drop the lock before calling format_phase_detail.
                let a = a.clone();
                drop(s);
                return self.format_phase_detail_with_live(phase, Some(&a)).len();
            }
        }
        self.format_phase_detail(phase).len()
    }

    /// Whether a detail block is rendered below the given phase. The
    /// user-pinned expansion (via Enter) always wins: even in Minimal
    /// mode the selected phase can still be drilled into. Otherwise
    /// Default LOD renders no details, Maximal renders them for every
    /// phase that has data.
    fn show_detail_for(&self, entry_idx: usize, phase: &crate::state::PhaseEntry) -> bool {
        if phase.kind != crate::state::EntryKind::Phase { return false; }
        if self.tree_expanded == Some(entry_idx) { return true; }
        match self.tree_lod {
            TreeLod::Hidden => false,
            TreeLod::Minimal => false,
            TreeLod::Default => false,
            TreeLod::Focus => matches!(phase.status, crate::state::PhaseStatus::Running),
            TreeLod::Maximal => !self.format_phase_detail(phase).is_empty(),
        }
    }

    /// Toggle expansion of the currently selected phase. Scope entries
    /// and missing selections are ignored. Works in every LOD: a
    /// user-pinned expansion overrides the LOD baseline, so in Minimal
    /// or Maximal the selected phase still gets its full detail block.
    fn toggle_tree_expansion(&mut self) {
        let Some(sel) = self.tree_selected else { return; };
        let is_phase = self.run_state.read()
            .map(|s| s.phases.get(sel)
                .map(|p| p.kind == crate::state::EntryKind::Phase)
                .unwrap_or(false))
            .unwrap_or(false);
        if !is_phase { return; }
        self.tree_expanded = match self.tree_expanded {
            Some(i) if i == sel => None,
            _ => Some(sel),
        };
    }

    /// Map a rendered-line index (0-based within the full rendered tree,
    /// pre-scroll) back to the `state.phases` index it represents. When
    /// a phase is expanded, its entry "owns" the detail lines below it,
    /// so clicks on those rows select the parent phase. Returns `None`
    /// if the line sits beyond the last entry.
    fn line_to_entry_index(&self, line: usize) -> Option<usize> {
        let s = self.run_state.read().ok()?;
        let mut cursor = 0usize;
        for (i, phase) in s.phases.iter().enumerate() {
            let rendered_rows = self.rendered_lines_for(i, phase);
            if rendered_rows == 0 { continue; }
            if line < cursor + rendered_rows {
                return Some(i);
            }
            cursor += rendered_rows;
        }
        None
    }

    /// Refresh the selection to track the most recently active phase
    /// when the user hasn't manually moved it. Called each frame so the
    /// marker drifts along with the workload's progress. When the
    /// tracked selection *changes*, the viewport re-pins the new row to
    /// the top edge so the active phase is always the anchor.
    fn refresh_auto_selection(&mut self) {
        if self.focused != Some(FocusedPane::Tree) || !self.tree_selection_auto {
            return;
        }
        let prev = self.tree_selected;
        if let Some(i) = self.default_tree_selection() {
            self.tree_selected = Some(i);
            if prev != Some(i) {
                // Move the expansion to follow the new active phase
                // FIRST — expansion state feeds into the line-index
                // math, so if the previously expanded phase is still
                // marked as expanded while we compute the new scroll
                // target, the target lands below the real position by
                // the old phase's detail-block height.
                if self.tree_expanded.is_some() {
                    self.tree_expanded = Some(i);
                }
                // Now re-pin to top with the correct expansion state.
                self.scroll_selection_to_top();
            }
        }
    }

    /// Pin the currently selected entry's rendered line to the top of
    /// the tree viewport by retargeting the scroll animation.
    fn scroll_selection_to_top(&mut self) {
        let Some(sel) = self.tree_selected else { return; };
        let Some(line) = self.entry_to_line_index(sel) else { return; };
        let visible = self.last_tree_rect.height.saturating_sub(2) as usize;
        if visible == 0 { return; }
        let total = self.total_rendered_lines();
        let tail = total.saturating_sub(visible);
        let clamped = line.min(tail);
        let target = if clamped >= tail { None } else { Some(clamped) };
        self.retarget_tree_scroll(target, tail);
    }

    /// Space-bar reset: pivot the tree view around the most recently
    /// active phase. Takes focus, seeds selection to the active phase,
    /// forces LOD back to Default, expands the selection, enables
    /// auto-tracking, and pins it to the top of the window.
    fn reset_tree_to_active(&mut self) {
        self.focused = Some(FocusedPane::Tree);
        self.tree_lod = TreeLod::Default;
        self.tree_selection_auto = true;
        if let Some(i) = self.default_tree_selection() {
            self.tree_selected = Some(i);
            self.tree_expanded = Some(i);
        }
        self.scroll_selection_to_top();
    }

    /// Build the detail lines shown when a phase is expanded. The
    /// caller adds the indent/gutter prefix. If `live` is supplied the
    /// detail reads from that live snapshot (matches the non-TUI
    /// progress bar exactly); otherwise it reads from
    /// `phase.summary` (captured at phase_completed time).
    fn format_phase_detail(
        &self,
        phase: &crate::state::PhaseEntry,
    ) -> Vec<Line<'static>> {
        self.format_phase_detail_with_live(phase, None)
    }

    fn format_phase_detail_with_live(
        &self,
        phase: &crate::state::PhaseEntry,
        live: Option<&crate::state::ActivePhase>,
    ) -> Vec<Line<'static>> {
        let mut out: Vec<Line<'static>> = Vec::new();

        // Status line — duplicates the parent row but gives us a first
        // detail line when a phase has no summary yet (pending).
        let status_text = match &phase.status {
            PhaseStatus::Pending => "pending — not yet started".to_string(),
            PhaseStatus::Running => "running…".to_string(),
            PhaseStatus::Completed => {
                let dur = phase.duration_secs.map(|d| format!(" in {d:.2}s")).unwrap_or_default();
                format!("completed{dur}")
            }
            PhaseStatus::Failed(err) => format!("failed: {err}"),
        };
        out.push(Line::from(Span::styled(status_text, Style::default().fg(colors::DIM))));

        if !phase.labels.is_empty() {
            out.push(Line::from(Span::styled(
                format!("bindings: {}", phase.labels),
                Style::default().fg(colors::TEXT),
            )));
        }

        if phase.op_count > 0 {
            out.push(Line::from(Span::styled(
                format!("active op templates: {}", phase.op_count),
                Style::default().fg(colors::TEXT),
            )));
        }

        // Extract the same fields the non-TUI progress bar prints, from
        // whichever source is available. `live` wins when the phase is
        // currently running; otherwise the completion snapshot.
        struct Snap {
            ops_started: u64,
            ops_finished: u64,
            ops_ok: u64,
            errors: u64,
            retries: u64,
            fibers: usize,
            ops_per_sec: f64,
            cursor_name: String,
            cursor_extent: u64,
            adapter_counters: Vec<(String, u64, f64)>,
            rows_per_batch: f64,
        }
        let snap: Option<Snap> = if let Some(a) = live {
            Some(Snap {
                ops_started: a.ops_started,
                ops_finished: a.ops_finished,
                ops_ok: a.ops_ok,
                errors: a.errors,
                retries: a.retries,
                fibers: a.fibers,
                ops_per_sec: a.ops_per_sec,
                cursor_name: a.cursor_name.clone(),
                cursor_extent: a.cursor_extent,
                adapter_counters: a.adapter_counters.clone(),
                rows_per_batch: a.rows_per_batch,
            })
        } else {
            phase.summary.as_ref().map(|sm| Snap {
                ops_started: sm.ops_started,
                ops_finished: sm.ops_finished,
                ops_ok: sm.ops_ok,
                errors: sm.errors,
                retries: sm.retries,
                fibers: sm.fibers,
                ops_per_sec: sm.ops_per_sec,
                cursor_name: sm.cursor_name.clone(),
                cursor_extent: sm.cursor_extent,
                adapter_counters: sm.adapter_counters.clone(),
                rows_per_batch: sm.rows_per_batch,
            })
        };

        if let Some(s) = snap {
            // Labeled tuple + visual bar. "82.9K of 83.0K" on its own is
            // ambiguous; spelling out pending/active/complete and
            // anchoring on the cursor name makes the scale obvious.
            let active = s.ops_started.saturating_sub(s.ops_finished);
            let pending = s.cursor_extent.saturating_sub(s.ops_started);
            let complete = s.ops_finished;
            // Percentage matches the "N of M" display on the same
            // line — both are ops_finished / extent. Previously
            // this used ops_started, which showed 100% while
            // "complete of extent" was still < extent (in-flight
            // ops made the two disagree).
            let pct = if s.cursor_extent > 0 {
                complete as f64 * 100.0 / s.cursor_extent as f64
            } else {
                0.0
            };

            // Line 1: cursor identity + mini progress bar + absolute ratio.
            let bar = widgets::bar_str(pct / 100.0, 16);
            let cursor_label = if s.cursor_name.is_empty() {
                "cursor".to_string()
            } else {
                format!("cursor {}", s.cursor_name)
            };
            out.push(Line::from(Span::styled(
                format!("{cursor_label}: {bar}  {} of {}  ({:.2}%)",
                    widgets::format_count(complete),
                    widgets::format_count(s.cursor_extent),
                    pct),
                Style::default().fg(colors::TEXT),
            )));

            // Line 2: named breakdown of the same numbers.
            out.push(Line::from(Span::styled(
                format!("  pending: {}  •  active: {}  •  complete: {}  (extent: {})",
                    widgets::format_count(pending),
                    widgets::format_count(active),
                    widgets::format_count(complete),
                    widgets::format_count(s.cursor_extent)),
                Style::default().fg(colors::DIM),
            )));

            // Line 3: throughput + reliability + concurrency.
            // The rate labeled here is the cursor-advance rate —
            // `ops_finished` increments once per cycle, which is
            // once per position of the declared cursor. Naming it
            // after the cursor ("row/s", "q/s", …) keeps the
            // label honest with the adjacent `cursor row:` line
            // above. Falls back to "cycles/s" when no cursor is
            // declared. Domain-level counters (rows_inserted/s
            // for batched writes, where the cycle rate ≠ the row
            // rate) still show separately under `adapter:` below.
            let throughput_label = cursor_rate_label(&s.cursor_name);
            // Smoothed rate via the phase's Ewma summary when
            // live; completed phases use the snapshot's raw
            // ops_per_sec (no EWMA retained post-phase).
            let rate_display = live
                .and_then(|a| a.rate_ewma.peek())
                .unwrap_or(s.ops_per_sec);
            out.push(Line::from(Span::styled(
                format!("{throughput_label}: {}   ok: {:.1}%   errors: {}   retries: {}   concurrency: {}",
                    widgets::format_rate(rate_display),
                    if s.ops_finished > 0 {
                        s.ops_ok as f64 * 100.0 / s.ops_finished as f64
                    } else { 100.0 },
                    s.errors,
                    s.retries,
                    s.fibers),
                Style::default().fg(colors::TEXT),
            )));

            // Latency percentile bars — render the existing per-row
            // visualization (from the retired top-level Latency panel)
            // inline under this phase's detail. Running phases pull
            // live values from the TUI's drained state; completed
            // phases use their `summary.*_nanos` snapshot.
            let (min_ns, p50_ns, p90_ns, p99_ns, p999_ns, max_ns, lat_src) =
                if live.is_some() {
                    if let Ok(sr) = self.run_state.read() {
                        (sr.min_nanos, sr.p50_nanos, sr.p90_nanos,
                         sr.p99_nanos, sr.p999_nanos, sr.max_nanos, "live")
                    } else {
                        (0, 0, 0, 0, 0, 0, "")
                    }
                } else if let Some(ref sm) = phase.summary {
                    // `summary` only captures min/p50/p99/max today —
                    // p90/p999 default to 0 and the bar renderer
                    // skips zero-valued rows.
                    (sm.min_nanos, sm.p50_nanos, 0, sm.p99_nanos,
                     0, sm.max_nanos, "summary")
                } else {
                    (0, 0, 0, 0, 0, 0, "")
                };
            let _ = lat_src;
            // Peak cross-bar markers come from the phase's
            // PeakTracker summaries when live; completed phases
            // have no live trackers, so peaks are omitted.
            let now = std::time::Instant::now();
            let peak_5s = live.and_then(|a| a.latency_peak_5s.peek(now));
            let peak_10s = live.and_then(|a| a.latency_peak_10s.peek(now));
            if max_ns > 0 || p99_ns > 0 || p50_ns > 0 {
                out.extend(latency_detail_lines(min_ns, p50_ns, p90_ns,
                    p99_ns, p999_ns, max_ns, peak_5s, peak_10s));
            }

            // Throughput sparkline. Two sources, one render:
            //   - Live phase → snapshot of the live
            //     `Arc<BinomialSummary>` (updates every frame).
            //   - Completed phase → frozen `throughput_samples`
            //     captured at phase_completed time.
            // Failed / pending phases render nothing (no samples).
            // The sparkline is the *durable artifact* per
            // SRD 62 §"Per-phase sparkline": scrolling back to
            // a completed phase must still show its curve.
            let (tp_samples, tp_cursor_name): (Vec<f64>, String) = if let Some(a) = live {
                (a.throughput_summary.snapshot(), a.cursor_name.clone())
            } else if let Some(ref sm) = phase.summary {
                (sm.throughput_samples.clone(), sm.cursor_name.clone())
            } else {
                (Vec::new(), String::new())
            };
            if !tp_samples.is_empty() {
                let spark = widgets::sparkline_str(&tp_samples, 60);
                let latest = tp_samples.last().copied().unwrap_or(0.0);
                let rate_label = cursor_rate_label(&tp_cursor_name);
                out.push(Line::from(vec![
                    Span::styled(" rate ", Style::default().fg(colors::DIM)),
                    Span::styled(
                        format!(" {:>14}  ", widgets::format_rate(latest)),
                        Style::default().fg(colors::SPARK).bold(),
                    ),
                    Span::styled(spark, Style::default().fg(colors::SPARK)),
                    Span::styled(format!("  {rate_label}"),
                        Style::default().fg(colors::DIM)),
                ]));
            }

            if phase.summary.as_ref()
                .map(|sm| sm.p50_nanos > 0 || sm.p99_nanos > 0 || sm.max_nanos > 0)
                .unwrap_or(false)
            {
                if let Some(ref sm) = phase.summary {
                    out.push(Line::from(Span::styled(
                        format!("latency: min {}  p50 {}  p99 {}  max {}",
                            widgets::format_nanos(sm.min_nanos),
                            widgets::format_nanos(sm.p50_nanos),
                            widgets::format_nanos(sm.p99_nanos),
                            widgets::format_nanos(sm.max_nanos)),
                        Style::default().fg(colors::LAT_P50),
                    )));
                }
            }

            // Relevancy aggregates (recall / precision / f1 / …), one
            // line per metric. Shows the moving-window mean (last N
            // cycles for the phase's op template) and the running
            // all-time mean across the phase. For ANN workloads this
            // is a KEY metric — kept separate so it's impossible to
            // miss in the expanded view.
            let relevancy: &[(String, f64, f64, u64, usize)] = if let Some(a) = live {
                &a.relevancy
            } else if let Some(ref sm) = phase.summary {
                &sm.relevancy
            } else {
                &[]
            };
            for (name, window_mean, total_mean, total_count, window_len) in relevancy {
                out.push(Line::from(Span::styled(
                    format!("{name}: all-time {:.4}  •  last {window_len}: {:.4}  (n={total_count})",
                        total_mean, window_mean),
                    Style::default().fg(colors::EMPHASIS),
                )));
            }

            // Adapter-specific status counters with rates (rows/s etc.),
            // same series the progress bar prefixes after the core stats.
            if !s.adapter_counters.is_empty() {
                let mut parts: Vec<String> = Vec::new();
                for (name, total, rate) in &s.adapter_counters {
                    parts.push(format!("{name}: {} @ {}/s",
                        widgets::format_count(*total),
                        widgets::format_rate(*rate)));
                }
                out.push(Line::from(Span::styled(
                    format!("adapter: {}", parts.join("   ")),
                    Style::default().fg(colors::DIM),
                )));
            }

            if s.rows_per_batch > 1.0 {
                out.push(Line::from(Span::styled(
                    format!("rows/batch: {:.1}", s.rows_per_batch),
                    Style::default().fg(colors::DIM),
                )));
            }
        }

        out
    }

    /// Offset that would put the tail (last entries) at the bottom of
    /// the tree view — i.e. the auto-follow position.
    fn tree_tail_offset(&self, tree_rect: Rect) -> usize {
        let visible = tree_rect.height.saturating_sub(2) as usize; // minus borders
        let total = self.run_state.read().map(|s| s.phases.len()).unwrap_or(0);
        total.saturating_sub(visible)
    }

    /// Drain pending base-cadence metrics frames. Updates the live
    /// latency values, history rings, and sparkline samples.
    ///
    /// Per-cadence windowed percentiles (10s / 1m / etc.) no longer
    /// flow through this channel — they're read directly from the
    /// shared [`MetricsQuery`] at render time, so we don't duplicate
    /// state.
    fn drain_frames(&mut self) {
        while let Ok(frame) = self.frame_rx.try_recv() {
            if let Ok(mut state) = self.run_state.write() {
                if let Some(w) = extract_latency_from_frame(&frame) {
                    state.min_nanos  = w.min;
                    state.p50_nanos  = w.p50;
                    state.p90_nanos  = w.p90;
                    state.p99_nanos  = w.p99;
                    state.p999_nanos = w.p999;
                    state.max_nanos  = w.max;

                    const HISTORY_CAP: usize = 300; // 5 min at 1 Hz
                    state.min_history.push(w.min);
                    state.p50_history.push(w.p50);
                    state.p90_history.push(w.p90);
                    state.p99_history.push(w.p99);
                    state.p999_history.push(w.p999);
                    state.max_history.push(w.max);
                    let trim = |h: &mut Vec<u64>| {
                        if h.len() > HISTORY_CAP { h.remove(0); }
                    };
                    trim(&mut state.min_history);
                    trim(&mut state.p50_history);
                    trim(&mut state.p90_history);
                    trim(&mut state.p99_history);
                    trim(&mut state.p999_history);
                    trim(&mut state.max_history);

                    // Feed each running phase's rolling-peak
                    // trackers with this frame's max latency.
                    // Frames are session-labeled today, so all
                    // active phases observe the same max — fine
                    // for single-phase scenarios; multi-phase
                    // will eventually require per-phase frame
                    // demux. The PeakTracker replaces a hand-
                    // rolled max-over-last-N scan of
                    // `max_history`.
                    let now = std::time::Instant::now();
                    for active in state.active_phases.values() {
                        active.latency_peak_5s.record(w.max, now);
                        active.latency_peak_10s.record(w.max, now);
                    }
                }

                // Sparkline samples from the active phase.
                // The adapter-counter sparkline tracks the FIRST
                // counter the active dispenser reports, whatever its
                // name. If the dispenser doesn't report one, no
                // sample is pushed and the secondary sparkline stays
                // empty.
                let (ops_sample, rows_sample, rows_name) = if let Some(active) = state.first_active() {
                    let (rate, name) = active.adapter_counters.first()
                        .map(|(n, _, r)| (*r, Some(n.clone())))
                        .unwrap_or((0.0, None));
                    let sample: Option<f64> = name.as_ref().map(|_| rate);
                    (Some(active.ops_per_sec), sample, name)
                } else {
                    (None::<f64>, None::<f64>, None::<String>)
                };
                if let Some(ops) = ops_sample { state.push_ops_sample(ops); }
                if let Some(rows) = rows_sample { state.push_rows_sample(rows); }
                state.rows_sparkline_label = rows_name;
            }
        }
    }

    /// Render one frame. Public for testing with TestBackend.
    ///
    /// While paused, every read routes through the snapshot clone
    /// in `self.frozen_state` instead of the live state. Live
    /// observer updates keep flowing into `self.run_state` — they
    /// just don't affect the rendered frame until the user resumes.
    pub fn draw(&self, frame: &mut Frame) {
        // Prefer the frozen snapshot when paused. Fall back to the
        // live read (same as pre-pause behavior) in every other case.
        let live_guard;
        let state: &RunState = if let Some(ref frozen) = self.frozen_state {
            frozen
        } else {
            live_guard = match self.run_state.read() {
                Ok(s) => s,
                Err(_) => return,
            };
            &live_guard
        };
        let area = frame.area();

        // Top-level layout — SRD 62 §"Panel composition".
        //
        // Phase, Latency, and Throughput are no longer top-level
        // panels; their content lives inside per-phase detail blocks
        // in the scenario tree. The canvas is a 4-section vertical
        // stack: Header, optional Log, Scenario (tree), Footer.
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),   // header
                Constraint::Min(3),      // scenario (+ log panel)
                Constraint::Length(1),   // footer
            ])
            .split(area);

        self.draw_header(frame, sections[0], &state);

        // Bottom section routing:
        //   - tree hidden (LOD:off): always show the log in the full
        //     section so something useful is there and the hint in
        //     the log title tells the user how to restore the tree.
        //   - tree visible + log off: tree takes the whole section.
        //   - tree visible + log on: split 50/50.
        if self.tree_lod == TreeLod::Hidden {
            self.draw_log(frame, sections[1], &state);
        } else if self.show_log {
            let bottom = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(50),
                    Constraint::Percentage(50),
                ])
                .split(sections[1]);
            self.draw_tree(frame, bottom[0], &state);
            self.draw_log(frame, bottom[1], &state);
        } else {
            self.draw_tree(frame, sections[1], &state);
        }

        self.draw_footer(frame, sections[2]);

        // Paused banner, top-right of the header row. Small and
        // readable, doesn't displace other header content.
        if self.paused {
            let banner = " ⏸ PAUSED (p to resume) ";
            let w = banner.chars().count() as u16;
            if sections[0].width > w + 2 {
                let x = sections[0].x + sections[0].width - w - 1;
                let r = Rect { x, y: sections[0].y, width: w, height: 1 };
                frame.render_widget(Clear, r);
                frame.render_widget(
                    Paragraph::new(Span::styled(banner,
                        Style::default().fg(colors::PHASE_FAILED).bold())),
                    r,
                );
            }
        }

        // Help overlay is rendered last so it sits above everything.
        if self.show_help {
            self.draw_help(frame, area);
        }
    }

    /// Centered help overlay listing every key binding. Dismissed with
    /// `?` (toggle) or `Esc`.
    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let entries: &[(&str, &str)] = &[
            ("q",     "dismiss TUI (single tap); triple-tap to exit nbrs"),
            ("?",     "toggle this help panel"),
            ("␣",     "focus tree, snap LOD=Default, track active phase"),
            ("↑ / ↓", "move tree selection"),
            ("← / →", "decrease / increase tree level of detail"),
            ("⏎",     "expand / collapse selected phase"),
            ("esc",   "close help, then collapse, then unfocus, then quit"),
            ("l",     "toggle log panel"),
            ("L",     "cycle latency views (percentiles / barchart / timeseries)"),
            ("C-l",   "force a full screen redraw"),
            ("p",     "pause / resume the display"),
            ("P",     "dump the current screen to logs/<session>/tui_<ts>.dump"),
            ("/",     "edit the latency 'past <span>' overlay (5m, 1h30m, …)"),
            ("click", "focus tree at the clicked phase"),
            ("wheel", "scroll tree (tweened); snaps back to tail at bottom"),
            ("╪",     "latency range: peak over the last 5s"),
            ("╫",     "latency range: peak over the last 10s"),
            ("╬",     "latency range: 5s + 10s peaks on the same cell"),
        ];

        // Size the panel around the content: widest "key" + " │ " + widest desc.
        let key_w = entries.iter().map(|(k, _)| k.chars().count()).max().unwrap_or(3);
        let desc_w = entries.iter().map(|(_, d)| d.chars().count()).max().unwrap_or(0);
        let inner_w = key_w + 3 + desc_w; // "key │ desc"
        let panel_w = (inner_w as u16 + 4).min(area.width.saturating_sub(4));
        let panel_h = (entries.len() as u16 + 4).min(area.height.saturating_sub(2));

        let x = area.x + area.width.saturating_sub(panel_w) / 2;
        let y = area.y + area.height.saturating_sub(panel_h) / 2;
        let rect = Rect { x, y, width: panel_w, height: panel_h };

        // Clear underneath so the overlay reads cleanly over whatever
        // was drawn below.
        frame.render_widget(Clear, rect);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::EMPHASIS))
            .title(Span::styled(" Help ", Style::default().fg(colors::EMPHASIS).bold()));
        let inner = block.inner(rect);
        frame.render_widget(block, rect);

        let mut lines: Vec<Line> = Vec::with_capacity(entries.len());
        for (key, desc) in entries {
            lines.push(Line::from(vec![
                Span::styled(format!(" {:key_w$} ", key, key_w = key_w),
                    Style::default().fg(colors::EMPHASIS).bold()),
                Span::styled("│ ", Style::default().fg(colors::BORDER)),
                Span::styled(*desc, Style::default().fg(colors::TEXT)),
            ]));
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_header(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let elapsed_s = state.elapsed_secs();
        let elapsed = widgets::format_elapsed(elapsed_s);

        // Phase ETA: based on cursor progress in whichever phase is
        // currently running (first one, by map iteration order).
        // Multi-phase scenarios will need a per-phase ETA attached
        // to each phase's detail block; header only shows one.
        let phase_eta = state.first_active().and_then(|a| {
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

    #[allow(dead_code)]
    fn draw_phase(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        // Retired — Phase panel content lives in the per-phase tree
        // detail block now (SRD 62). Kept temporarily for reference;
        // will be removed once the tree-detail layout is locked in.
        let mut title_spans: Vec<Span> = vec![
            Span::styled(" Phase ", Style::default().fg(colors::PHASE_ACTIVE)),
        ];
        if let Some(active) = state.first_active() {
            title_spans.push(Span::styled(
                active.name.clone(),
                Style::default().fg(colors::EMPHASIS).bold(),
            ));
            if !active.labels.is_empty() {
                title_spans.push(Span::styled(
                    format!(" [{}]", active.labels),
                    Style::default().fg(colors::DIM),
                ));
            }
            title_spans.push(Span::raw(" "));
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Line::from(title_spans));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        if let Some(active) = state.first_active() {
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

            // Line 2: concurrency + labeled pending/active/complete +
            // rates + batch. Labels come first so a glance tells the
            // reader what each number represents.
            let mut line2_spans = vec![
                Span::styled(format!("   concurrency:{}", active.fibers), Style::default().fg(colors::DIM)),
                Span::styled(format!("  active:{}", widgets::format_count(active_count)), Style::default().fg(
                    if active_count > 0 { colors::PHASE_ACTIVE } else { colors::DIM }
                )),
                Span::styled(format!("  pending:{}", widgets::format_count(pending)), Style::default().fg(colors::DIM)),
                Span::styled(format!("  done:{}", widgets::format_count(complete)), Style::default().fg(colors::PHASE_DONE)),
                Span::styled(format!("  of:{}", widgets::format_count(active.cursor_extent)), Style::default().fg(colors::DIM)),
                Span::styled(format!("  ops/s:{}", widgets::format_rate(active.ops_per_sec)), Style::default().fg(colors::TEXT)),
            ];
            // Adapter-specific counters: render one `{name}/s:{rate}`
            // span per counter the dispenser actually reports.
            // Counter-agnostic — the TUI doesn't hardcode "rows/s" or
            // any other name, so driver-specific counters appear
            // automatically when present and stay out of the way
            // when the active dispenser doesn't produce them.
            for (name, _total, rate) in &active.adapter_counters {
                line2_spans.push(Span::styled(
                    format!("  {name}/s:{}", widgets::format_rate(*rate)),
                    Style::default().fg(colors::TEXT),
                ));
            }
            if active.rows_per_batch > 1.0 {
                line2_spans.push(Span::styled(format!("  rows/batch:{:.1}", active.rows_per_batch), Style::default().fg(colors::DIM)));
            }
            // Relevancy aggregates (e.g. recall@10) inline next to the
            // throughput data — key metric for ANN workloads.
            for (name, window_mean, total_mean, _, window_len) in &active.relevancy {
                line2_spans.push(Span::styled(
                    format!("  {name}: {:.3} (last {window_len}: {:.3})",
                        total_mean, window_mean),
                    Style::default().fg(colors::EMPHASIS),
                ));
            }
            let line2 = Line::from(line2_spans);

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

    fn draw_latency_percentiles(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Line::from(self.latency_title_spans()));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Normalize against the largest thing we'll plot: current max,
        // p999, and the historical peaks so markers past the current
        // max don't land off the right edge.
        let history_peak = state.max_history.iter().copied().max().unwrap_or(0);
        let max_val = state.max_nanos
            .max(state.p999_nanos)
            .max(history_peak)
            .max(1);
        // Reserved space: 2 (pad) + 5 (label) + 1 (sep) + 14 (value) +
        // 2 (sep) + bar + 3 (right margin) = inner.width.
        // The 14-char value slot fits a "min..max" label on the range
        // row (e.g. "58.98ms..2.92s") without pushing the bar out of
        // alignment with the percentile rows.
        let bar_width = inner.width.saturating_sub(27) as usize;

        // Top row: distribution "range bar" made of colored segments,
        // each spanning from one percentile to the next. Visualizes
        // error/spread at a glance — thick bands = long tails.
        // Overlaid with cross-bar tick marks at the peak-max values
        // seen over the last 5s and last 10s windows (`╪` / `╫`).
        if inner.height >= 1 {
            let bar_w = bar_width.max(5);
            let pos = |nanos: u64| -> usize {
                if max_val == 0 { return 0; }
                ((nanos as f64 / max_val as f64) * bar_w as f64).round() as usize
            };

            // Build the base spread bar as a character buffer so we can
            // overlay the cross-bar tick marks on top.
            let mut cells: Vec<(char, Color)> =
                vec![('╌', colors::BORDER); bar_w];
            let points = [
                (0u64, colors::LAT_P50),
                (state.p50_nanos, colors::LAT_P50),
                (state.p90_nanos, colors::LAT_P90),
                (state.p99_nanos, colors::LAT_P99),
                (state.p999_nanos, colors::LAT_MAX),
                (state.max_nanos, colors::LAT_MAX),
            ];
            for w in points.windows(2) {
                let start = pos(w[0].0).min(bar_w);
                let end = pos(w[1].0).min(bar_w);
                for c in cells.iter_mut().take(end).skip(start) {
                    c.0 = '━';
                    c.1 = w[1].1;
                }
            }

            // Peak markers. History rings hold one sample per metrics
            // frame (~1 Hz), so 5 samples ≈ 5s, 10 ≈ 10s.
            //   ╪  5s peak
            //   ╫  10s peak
            //   ╬  both peaks (same cell) — collapsed marker
            let peak_over = |n: usize| -> u64 {
                let h = &state.max_history;
                let start = h.len().saturating_sub(n);
                h[start..].iter().copied().max().unwrap_or(0)
            };
            let peak_5s = peak_over(5);
            let peak_10s = peak_over(10);
            let pos_or_none = |nanos: u64| -> Option<usize> {
                if nanos == 0 || bar_w == 0 { None }
                else { Some(pos(nanos).min(bar_w.saturating_sub(1))) }
            };
            let mark = |cells: &mut [(char, Color)], idx: usize, ch: char, col: Color| {
                cells[idx].0 = ch;
                cells[idx].1 = col;
            };
            let p5 = pos_or_none(peak_5s);
            let p10 = pos_or_none(peak_10s);
            match (p5, p10) {
                (Some(a), Some(b)) if a == b => {
                    // Both peaks share a cell — render a distinct
                    // combined glyph so neither gets overwritten.
                    mark(&mut cells, a, '╬', colors::EMPHASIS);
                }
                _ => {
                    // Draw the longer window first so if there's any
                    // z-order collision (adjacent cells overlapping
                    // visually in the terminal) the shorter window
                    // still reads on top.
                    if let Some(p) = p10 { mark(&mut cells, p, '╫', colors::LAT_MAX); }
                    if let Some(p) = p5  { mark(&mut cells, p, '╪', colors::EMPHASIS); }
                }
            }

            // Emit as one span per color run. Prefix layout matches
            // the percentile rows below — label (6) + value (15) +
            // "  " (2) = 23 chars before the bar. The range row's
            // "value" is the axis endpoints: `min..max`. The bar's
            // colored segments then show where each percentile falls
            // within that interval, so the reader can cross-reference
            // the range label and the bar without ambiguity.
            let range_label = if state.min_nanos > 0 {
                format!(
                    "{}..{}",
                    widgets::format_nanos(state.min_nanos),
                    widgets::format_nanos(max_val),
                )
            } else {
                format!("0..{}", widgets::format_nanos(max_val))
            };
            let mut spans: Vec<Span> = vec![
                Span::styled("  range", Style::default().fg(colors::DIM)),
                Span::styled(
                    format!(" {:>14}", range_label),
                    Style::default().fg(colors::DIM),
                ),
                Span::raw("  "),
            ];
            let mut i = 0;
            while i < cells.len() {
                let col = cells[i].1;
                let mut j = i;
                let mut run = String::new();
                while j < cells.len() && cells[j].1 == col {
                    run.push(cells[j].0);
                    j += 1;
                }
                spans.push(Span::styled(run, Style::default().fg(col)));
                i = j;
            }
            frame.render_widget(
                Paragraph::new(Line::from(spans)),
                Rect { y: inner.y, height: 1, ..inner },
            );
        }

        let percentiles = [
            ("min ", state.min_nanos,  colors::LAT_P50),
            ("p50 ", state.p50_nanos,  colors::LAT_P50),
            ("p90 ", state.p90_nanos,  colors::LAT_P90),
            ("p99 ", state.p99_nanos,  colors::LAT_P99),
            ("p999", state.p999_nanos, colors::LAT_MAX),
            ("max ", state.max_nanos,  colors::LAT_MAX),
        ];

        for (i, (label, nanos, color)) in percentiles.iter().enumerate() {
            let row = i as u16 + 1; // row 0 is the range bar
            if row >= inner.height { break; }
            let frac = if max_val > 0 { *nanos as f64 / max_val as f64 } else { 0.0 };
            let bar = widgets::bar_str(frac.min(1.0), bar_width.max(5));
            // Value slot matches the range row's 14-char field so the
            // bar starts at the same column on every row.
            let line = Line::from(vec![
                Span::styled(format!("  {label}"), Style::default().fg(colors::DIM)),
                Span::styled(format!(" {:>14}", widgets::format_nanos(*nanos)), Style::default().fg(*color).bold()),
                Span::styled(format!("  {bar}"), Style::default().fg(*color)),
            ]);
            frame.render_widget(
                Paragraph::new(line),
                Rect { y: inner.y + row, height: 1, ..inner },
            );
        }
    }

    /// Latency as a ratatui [`BarChart`]. Each percentile
    /// (min/p50/p90/p99/p999/max) gets four adjacent bars showing
    /// different time horizons: current, 5s max, 15s max, and the
    /// all-time mean (`avg-total`). Groups are visually separated by
    /// an extra gap so the reader can scan per-percentile columns
    /// easily.
    /// Build the common latency-panel title, including the (optional)
    /// past-span input prompt or pinned-past tag. Shared across all
    /// three latency views so the prompt appears regardless of which
    /// one is active when the user hits `/`.
    fn latency_title_spans(&self) -> Vec<Span<'_>> {
        let mut out: Vec<Span> = vec![
            Span::styled(" Latency (service time) ", Style::default().fg(colors::LAT_P50)),
            Span::styled(format!("[{}] ", self.latency_view.label()),
                Style::default().fg(colors::DIM)),
        ];
        if let Some(ref state) = self.past_input {
            out.push(Span::styled(
                format!("past: {}\u{2581} ", state.buf),
                Style::default().fg(colors::LAT_P99),
            ));
            if let Some(ref e) = state.error {
                out.push(Span::styled(
                    format!("({e}) "),
                    Style::default().fg(colors::LAT_MAX),
                ));
            }
        } else if let Some(span) = self.past_span {
            out.push(Span::styled(
                format!("past={} ", format_span_short(span)),
                Style::default().fg(colors::DIM),
            ));
        }
        out
    }

    fn draw_latency_barchart(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Line::from(self.latency_title_spans()));

        // Pick a percentile value out of a windowed snapshot. Falls
        // back to the live `current` when that window hasn't filled
        // yet (count == 0), so the bar never collapses to zero just
        // because the 1-minute window hasn't closed in the first
        // 60 s of a run.
        type Getter = fn(&LatencySnapshot) -> u64;
        let picker = |w: &LatencySnapshot, which: Getter, current: u64| {
            if w.sample_count == 0 { current } else { which(w) }
        };

        // Scope every read to the active phase's dimensional label
        // set so the barchart is never cross-phase-aggregated. Without
        // an active phase, the chart renders empty — deliberately, so
        // no stale Stopped-phase data leaks into what's supposed to
        // be a live view.
        let Some(sel) = active_phase_selection(state, "cycles_servicetime") else {
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(colors::BORDER))
                .title(Line::from(self.latency_title_spans()));
            frame.render_widget(block, area);
            return;
        };
        let now_snap = LatencySnapshot::from_metric_set(&self.metrics_query.now(&sel));
        let lifetime_snap = LatencySnapshot::from_metric_set(
            &self.metrics_query.session_lifetime(&sel),
        );
        // User-pinned `past <span>` overlay (SRD-42 phase 9), if any.
        let past_snap: Option<(String, LatencySnapshot)> = self.past_span.map(|d| (
            format!("past·{}", format_span_short(d)),
            LatencySnapshot::from_metric_set(&self.metrics_query.recent_window(d, &sel)),
        ));

        // Per-percentile: label, getter, live-instrument nanos, and color.
        struct P {
            label: &'static str,
            get: Getter,
            current: u64,
            col: Color,
        }
        let percs: [P; 6] = [
            P { label: "min",  get: |w| w.min,  current: now_snap.min,  col: colors::LAT_P50 },
            P { label: "p50",  get: |w| w.p50,  current: now_snap.p50,  col: colors::LAT_P50 },
            P { label: "p90",  get: |w| w.p90,  current: now_snap.p90,  col: colors::LAT_P90 },
            P { label: "p99",  get: |w| w.p99,  current: now_snap.p99,  col: colors::LAT_P99 },
            P { label: "p999", get: |w| w.p999, current: now_snap.p999, col: colors::LAT_MAX },
            P { label: "max",  get: |w| w.max,  current: now_snap.max,  col: colors::LAT_MAX },
        ];

        // Canonical cadence set (SRD-42): user-declared, in order.
        // Each percentile gets one "now" bar + one per cadence +
        // "avg-tot". For a user-configured `10s,1m,5m` that's 5 bars
        // per percentile → 30 total.
        let cadences: Vec<std::time::Duration> =
            self.metrics_query.reporter().declared_cadences().iter().collect();
        // Pre-compute each cadence's LatencySnapshot once (not per
        // percentile) — cheaper than six queries per cadence.
        let per_cadence_snaps: Vec<(String, LatencySnapshot)> = cadences.iter()
            .map(|d| (
                widgets::format_cadence(*d),
                LatencySnapshot::from_metric_set(&self.metrics_query.cadence_window(*d, &sel)),
            ))
            .collect();
        // 1 now + N cadences + (optional) 1 past overlay + 1 avg-tot.
        let per_perc = 1 + cadences.len() + usize::from(past_snap.is_some()) + 1;
        let mut bars: Vec<Bar> = Vec::with_capacity(percs.len() * per_perc);
        for p in &percs {
            // Pre-build the cadence windows once per percentile.
            let window_vals: Vec<(String, u64)> = per_cadence_snaps.iter()
                .map(|(label, snap)| (label.clone(), picker(snap, p.get, p.current)))
                .collect();

            let mut variants: Vec<(String, u64)> =
                Vec::with_capacity(per_perc);
            variants.push(("now".into(), p.current));
            variants.extend(window_vals);
            // User-pinned past-span overlay — sits between the
            // cadence columns and avg-tot so it reads as "slightly
            // longer horizon than the smallest cadence, shorter
            // than lifetime".
            if let Some((ref label, ref snap)) = past_snap {
                variants.push((label.clone(), picker(snap, p.get, p.current)));
            }
            // Session-lifetime percentile from the same MetricsQuery.
            // Falls back to live when lifetime has no data yet (first
            // seconds of a run).
            let lifetime_val = if lifetime_snap.sample_count == 0 {
                p.current
            } else {
                (p.get)(&lifetime_snap)
            };
            variants.push(("avg-tot".into(), lifetime_val));
            for (i, (vlabel, nanos)) in variants.iter().enumerate() {
                let bar_label = if i == 0 {
                    format!("{}·{}", p.label, vlabel)
                } else {
                    vlabel.to_string()
                };
                bars.push(
                    Bar::default()
                        .value(*nanos)
                        .text_value(widgets::format_nanos(*nanos))
                        .label(Line::from(Span::styled(
                            bar_label,
                            Style::default().fg(p.col),
                        )))
                        .style(Style::default().fg(p.col))
                );
            }
        }

        let n_bars = bars.len() as u16;
        let inner_w = block.inner(area).width;
        let gap: u16 = 1;
        let gaps_total = gap * n_bars.saturating_sub(1);
        // Want the bar wide enough for the value text (e.g., "1.2ms",
        // "232µs") — roughly 7 chars is a safe floor.
        let bar_w = (inner_w.saturating_sub(gaps_total) / n_bars.max(1))
            .max(7);

        let chart = BarChart::default()
            .block(block)
            .bar_width(bar_w)
            .bar_gap(gap)
            .data(BarGroup::default().bars(&bars));
        frame.render_widget(chart, area);
    }

    /// Latency as a ratatui [`Chart`] time-series — p50, p99, and max
    /// plotted over the last ~30 s of samples.
    fn draw_latency_timeseries(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Line::from(self.latency_title_spans()));

        // Convert history series to (x, y) points where x is the
        // sample index (seconds-ago ≈ index / 4) and y is milliseconds
        // — milliseconds are more readable than nanos on the axis.
        let to_points = |hist: &[u64]| -> Vec<(f64, f64)> {
            hist.iter().enumerate()
                .map(|(i, &n)| (i as f64, n as f64 / 1_000_000.0))
                .collect()
        };
        let min_pts = to_points(&state.min_history);
        let p50_pts = to_points(&state.p50_history);
        let p99_pts = to_points(&state.p99_history);
        let max_pts = to_points(&state.max_history);

        let y_max: f64 = state.max_history.iter()
            .chain(state.p99_history.iter())
            .copied()
            .max()
            .unwrap_or(1)
            as f64 / 1_000_000.0;
        let y_max = if y_max <= 0.0 { 1.0 } else { y_max * 1.1 };
        let x_max = (state.max_history.len().max(1)) as f64;

        let datasets = vec![
            Dataset::default()
                .name("min")
                .marker(ratatui::symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors::PHASE_DONE))
                .data(&min_pts),
            Dataset::default()
                .name("p50")
                .marker(ratatui::symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors::LAT_P50))
                .data(&p50_pts),
            Dataset::default()
                .name("p99")
                .marker(ratatui::symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors::LAT_P99))
                .data(&p99_pts),
            Dataset::default()
                .name("max")
                .marker(ratatui::symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(colors::LAT_MAX))
                .data(&max_pts),
        ];

        let chart = Chart::new(datasets)
            .block(block)
            .x_axis(
                Axis::default()
                    .style(Style::default().fg(colors::DIM))
                    .bounds([0.0, x_max])
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().fg(colors::DIM))
                    .bounds([0.0, y_max])
                    .labels::<Vec<Span>>(vec![
                        Span::raw("0"),
                        Span::raw(format!("{:.1}ms", y_max / 2.0)),
                        Span::raw(format!("{:.1}ms", y_max)),
                    ])
            );
        frame.render_widget(chart, area);
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
            let rows_value = state.rows_history.last().map(|v| widgets::format_rate(*v)).unwrap_or_default();
            let label_text = state.rows_sparkline_label
                .as_deref()
                .map(|n| format!("  {n}/s "))
                .unwrap_or_else(|| "  ???/s ".to_string());
            let line = Line::from(vec![
                Span::styled(label_text, Style::default().fg(colors::DIM)),
                Span::styled(rows_spark, Style::default().fg(colors::PHASE_ACTIVE)),
                Span::styled(format!(" {rows_value}"), Style::default().fg(colors::TEXT)),
            ]);
            frame.render_widget(
                Paragraph::new(line),
                Rect { y: inner.y + 1, height: 1, ..inner },
            );
        }
    }

    fn draw_tree(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        let has_focus = self.focused == Some(FocusedPane::Tree);
        let title_style = if has_focus {
            Style::default().fg(colors::EMPHASIS).bold()
        } else {
            Style::default().fg(colors::TEXT)
        };
        let lod_tag = format!(" [LOD:{}] ", self.tree_lod.label());
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(
                if has_focus { colors::EMPHASIS } else { colors::BORDER }
            ))
            .title(Line::from(vec![
                Span::styled(" Scenario Tree ", title_style),
                Span::styled(lod_tag, Style::default().fg(colors::DIM)),
            ]));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Hidden LOD renders nothing — the block border + title stay
        // (so the user can see they're in a tree pane with LOD=off
        // and press right-arrow to re-reveal).
        if self.tree_lod == TreeLod::Hidden {
            return;
        }

        let mut lines: Vec<Line> = Vec::new();
        // Focus-mode bookkeeping: if no running phases exist, we
        // need to show a waiting/done placeholder instead of an
        // empty canvas. Tracked here because we can emit it after
        // the regular iteration finds no running phases to render.
        let mut focus_emitted_any = false;
        for (i, phase) in state.phases.iter().enumerate() {
            // LOD gating:
            //   Minimal + Focus: omit scope headers.
            //   Focus also omits pending/completed/failed phases —
            //     only running ones render, each fully expanded.
            //   Default/Maximal show scopes as grouping anchors.
            if matches!(self.tree_lod, TreeLod::Minimal | TreeLod::Focus)
                && phase.kind == crate::state::EntryKind::Scope
            {
                continue;
            }
            if self.tree_lod == TreeLod::Focus
                && phase.kind == crate::state::EntryKind::Phase
                && !matches!(phase.status, crate::state::PhaseStatus::Running)
            {
                continue;
            }

            // Indentation: at Minimal / Focus LOD, scopes are
            // hidden, so preserving `phase.depth` would make
            // children of hidden scopes look like descendants of
            // the previous visible phase. Flatten to depth 0 —
            // dimensional labels already disambiguate the
            // iterations, so the hierarchy isn't lost.
            let effective_depth = if matches!(self.tree_lod, TreeLod::Minimal | TreeLod::Focus) {
                0
            } else {
                phase.depth
            };
            focus_emitted_any = true;
            let indent = "  ".repeat(effective_depth);
            // Two-column left margin: selection marker + its trailing space.
            // `●` in column 0 means "this entry has focus". Non-selected
            // entries render two spaces, so the following content lines up
            // the same either way.
            let is_selected = has_focus && self.tree_selected == Some(i);
            let marker: Span = if is_selected {
                Span::styled("● ", Style::default().fg(colors::EMPHASIS).bold())
            } else {
                Span::raw("  ")
            };

            // Scope headers render as a distinct line: no status glyph,
            // italic-ish group label. Keeps the hierarchy visible without
            // making peer phases look like children of each other.
            if phase.kind == crate::state::EntryKind::Scope {
                lines.push(Line::from(vec![
                    marker,
                    Span::styled(format!("{indent}┬ "), Style::default().fg(colors::BORDER)),
                    Span::styled(&phase.labels, Style::default().fg(colors::TEXT).italic()),
                ]));
                continue;
            }

            // Health tint = line color. Running phases are always
            // yellow (they haven't finished, so error counts aren't
            // meaningful yet). Completed phases are green/orange/red
            // on a sliding scale keyed to the observed error rate,
            // so the user can see at a glance whether a phase went
            // clean, had blips, or degraded badly.
            let health_color = phase_health_color(phase);
            let (icon, icon_color) = match &phase.status {
                PhaseStatus::Completed => ("✓", health_color),
                PhaseStatus::Running => ("▶", health_color),
                PhaseStatus::Pending => ("○", colors::PHASE_PENDING),
                PhaseStatus::Failed(_) => ("✗", colors::PHASE_FAILED),
            };
            let is_running = matches!(phase.status, PhaseStatus::Running);
            let name_style = match &phase.status {
                PhaseStatus::Running => Style::default().fg(health_color).bold(),
                PhaseStatus::Completed => Style::default().fg(health_color),
                PhaseStatus::Failed(_) => Style::default().fg(colors::PHASE_FAILED).bold(),
                _ => Style::default().fg(colors::DIM),
            };
            // Labels stay dim for completed so the health color on the
            // phase name is the eye-catching cue; for running they pick
            // up the yellow tint too so the whole live line reads hot.
            let label_color = if is_running { health_color } else { colors::DIM };

            let mut spans = vec![
                marker,
                Span::styled(format!("{indent}{icon} "), Style::default().fg(icon_color)),
                Span::styled(&phase.name, name_style),
            ];

            if !phase.labels.is_empty() {
                spans.push(Span::styled(
                    format!(" ({})", phase.labels),
                    Style::default().fg(label_color),
                ));
            }

            if phase.op_count > 0 {
                spans.push(Span::styled(
                    format!("  {} op templates", phase.op_count),
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
                    // Attach the completion summary so the tree acts
                    // as a post-hoc record of what each phase produced.
                    // Ordered for "tell me the key story" reading:
                    //   1. cycles + rate (the engine-level throughput)
                    //   2. adapter-declared domain metrics with rates
                    //      (rows_inserted/s, etc. — the thing the
                    //      workload actually cares about)
                    //   3. relevancy metrics (recall/precision) — the
                    //      KEY metric for ANN/search workloads, so it
                    //      must be visible without expansion
                    //   4. latency percentiles
                    if let Some(ref sm) = phase.summary {
                        if sm.ops_finished > 0 {
                            // Label the cycle count/rate after the
                            // cursor name (`rows:N @R/s`) so it
                            // matches the phase detail's wording.
                            // Falls back to `cycles:` when the
                            // phase declares no cursor. Adapter
                            // counters (rows_inserted, etc.) show
                            // separately below.
                            let unit = cursor_count_label(&sm.cursor_name);
                            spans.push(Span::styled(
                                format!("  {unit}:{} @{}/s",
                                    widgets::format_count(sm.ops_finished),
                                    widgets::format_rate(sm.ops_per_sec)),
                                Style::default().fg(colors::TEXT),
                            ));
                        }
                        // Adapter-declared workload counters with rates
                        // — this is "rows/s" for CQL INSERT workloads,
                        // "queries/s" for search workloads, etc.
                        for (name, total, rate) in &sm.adapter_counters {
                            if *total == 0 { continue; }
                            spans.push(Span::styled(
                                format!("  {name}:{}@{}/s",
                                    widgets::format_count(*total),
                                    widgets::format_rate(*rate)),
                                Style::default().fg(colors::PHASE_ACTIVE),
                            ));
                        }
                        // Relevancy (recall@k, precision@k, ...) — the
                        // headline metric for ANN workloads. Rendered
                        // in EMPHASIS + bold so it reads louder than
                        // plain counts.
                        for (name, _window_mean, total_mean, total_count, _window_len) in &sm.relevancy {
                            if *total_count == 0 { continue; }
                            spans.push(Span::styled(
                                format!("  {name}:{:.4}",  total_mean),
                                Style::default().fg(colors::EMPHASIS).bold(),
                            ));
                        }
                        if sm.p50_nanos > 0 {
                            spans.push(Span::styled(
                                format!("  p50:{}  p99:{}",
                                    widgets::format_nanos(sm.p50_nanos),
                                    widgets::format_nanos(sm.p99_nanos)),
                                Style::default().fg(colors::LAT_P50),
                            ));
                        }
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

            // Detail block: rendered under the phase when the user has
            // expanded it (Default LOD), or under every phase with data
            // (Maximal LOD). Same indent as the parent plus a pipe
            // gutter so the relationship is visible at a glance.
            if self.show_detail_for(i, phase) {
                // If this entry is the currently active phase, pull
                // live progress stats from the active-phases map;
                // otherwise fall back to the end-of-phase summary
                // snapshot.
                let live = state.active_phase(&phase.name, &phase.labels);
                // Detail pipe placement:
                //   phase icon sits at column 2+(depth*2); we want
                //   the continuation pipe one level deeper so it
                //   reads as "this belongs to the phase above"
                //   rather than drifting into the next sibling's
                //   indent column. `indent + 2 spaces` puts the
                //   pipe directly under the phase name's first
                //   character, matching the tree's visual rhythm.
                let detail_indent = format!("{indent}  ");
                for detail_line in self.format_phase_detail_with_live(phase, live) {
                    let mut spans: Vec<Span<'static>> = Vec::with_capacity(
                        detail_line.spans.len() + 2,
                    );
                    spans.push(Span::raw("  "));
                    spans.push(Span::styled(
                        format!("{detail_indent}│ "),
                        Style::default().fg(colors::BORDER),
                    ));
                    spans.extend(detail_line.spans);
                    lines.push(Line::from(spans));
                }
            }
        }

        // Focus LOD with zero running phases: emit a placeholder so
        // the canvas isn't empty. Live vs. done is a component-tree
        // question (SRD 62 §"Scenario done?"):
        //   Live   → running_phase_count > 0
        //   Waiting → any pre-mapped Pending phase (component tree
        //             doesn't know about pre-map; that info lives
        //             in state.phases from scenario_pre_mapped)
        //   Done   → neither — scenario has nothing left to do.
        if self.tree_lod == TreeLod::Focus && !focus_emitted_any {
            let live = self.metrics_query.running_phase_count() > 0;
            let any_pending = state.phases.iter()
                .any(|p| p.kind == crate::state::EntryKind::Phase
                    && matches!(p.status, crate::state::PhaseStatus::Pending));
            let (glyph, msg, color) = if live {
                // Running phases exist in the component tree but
                // haven't appeared in `state.phases` yet — a tiny
                // race window during phase start. Show waiting.
                ("○", "waiting for phase…", colors::PHASE_PENDING)
            } else if any_pending {
                ("○", "waiting for phase…", colors::PHASE_PENDING)
            } else {
                ("✓", "scenario complete", colors::PHASE_DONE)
            };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(format!("{glyph} "), Style::default().fg(color)),
                Span::styled(msg, Style::default().fg(colors::DIM).italic()),
            ]));
        }

        // Pick the visible window. Default to auto-tail; if the user
        // has grabbed scroll focus, honor their offset. `tree_display`
        // carries the fractional, eased position — advanced in the
        // event loop before each draw — so the viewport tweens between
        // targets instead of snapping.
        let visible = inner.height as usize;
        let tail = lines.len().saturating_sub(visible);
        let start = self.tree_display.round().clamp(0.0, tail as f32) as usize;
        let total = lines.len();
        let visible_lines: Vec<Line> = lines.into_iter().skip(start).collect();

        frame.render_widget(Paragraph::new(visible_lines), inner);

        // Scrollbar on the right edge of the block, drawn only when the
        // content overflows. Gives the user a visual sense of where
        // they are in the list and how much is off-screen.
        if total > visible {
            let mut sb_state = ScrollbarState::new(total)
                .position(start)
                .viewport_content_length(visible);
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .style(Style::default().fg(colors::BORDER))
                .thumb_style(Style::default().fg(colors::EMPHASIS));
            frame.render_stateful_widget(scrollbar, area, &mut sb_state);
        }
    }

    /// Advance the scroll animation toward the current target. Called
    /// once per frame while `draw_tree` runs, so the render reflects the
    /// tween's current intermediate position. If the target changed out
    /// from under the anim (e.g. auto-tail shifted because a new phase
    /// was added), re-aim without resetting time.
    fn advance_scroll_anim(&mut self, target: f32) {
        match self.tree_anim {
            None => {
                // No anim — just sync the display to the target.
                self.tree_display = target;
            }
            Some((from, to, started, duration)) => {
                // If the live target drifted away from the anim's `to`,
                // rewrite `to`. Don't restart the clock; the motion just
                // glides to the new endpoint.
                let to = if (to - target).abs() > 0.01 { target } else { to };
                let t = (started.elapsed().as_secs_f32() / duration.as_secs_f32())
                    .clamp(0.0, 1.0);
                let eased = Self::smoothstep(t);
                self.tree_display = from + (to - from) * eased;
                if t >= 1.0 {
                    self.tree_display = to;
                    self.tree_anim = None;
                } else {
                    self.tree_anim = Some((from, to, started, duration));
                }
            }
        }
    }

    fn draw_log(&self, frame: &mut Frame, area: Rect, state: &RunState) {
        // When the tree is hidden (LOD:off), the log panel is solo:
        // add a hint to the title so the user knows how to bring the
        // tree back and where help lives.
        let title: Vec<Span> = if self.tree_lod == TreeLod::Hidden {
            vec![
                Span::styled(" Log ", Style::default().fg(colors::LOG_INFO)),
                Span::styled(
                    "— tree hidden: ",
                    Style::default().fg(colors::DIM),
                ),
                Span::styled("→", Style::default().fg(colors::EMPHASIS).bold()),
                Span::styled(" to restore, ", Style::default().fg(colors::DIM)),
                Span::styled("?", Style::default().fg(colors::EMPHASIS).bold()),
                Span::styled(" for help ", Style::default().fg(colors::DIM)),
            ]
        } else {
            vec![Span::styled(" Log ", Style::default().fg(colors::LOG_INFO))]
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BORDER))
            .title(Line::from(title));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        let visible = inner.height as usize;
        let start = if state.log_messages.len() > visible {
            state.log_messages.len() - visible
        } else {
            0
        };

        let lines: Vec<Line> = state.log_messages[start..].iter().map(|entry| {
            let (prefix, color) = match entry.severity {
                crate::state::LogSeverity::Debug => ("DBG", colors::LOG_DEBUG),
                crate::state::LogSeverity::Info  => ("INF", colors::LOG_INFO),
                crate::state::LogSeverity::Warn  => ("WRN", colors::LOG_WARN),
                crate::state::LogSeverity::Error => ("ERR", colors::LOG_ERROR),
            };
            Line::from(vec![
                Span::styled(format!("  {prefix} "), Style::default().fg(color).bold()),
                Span::styled(&entry.message, Style::default().fg(colors::TEXT)),
            ])
        }).collect();

        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let line = Line::from(vec![
            Span::styled(" q", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": quit  ", Style::default().fg(colors::DIM)),
            Span::styled("↑↓", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": select  ", Style::default().fg(colors::DIM)),
            Span::styled("←→", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": LOD  ", Style::default().fg(colors::DIM)),
            Span::styled("⏎", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": expand  ", Style::default().fg(colors::DIM)),
            Span::styled("␣", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": track active  ", Style::default().fg(colors::DIM)),
            Span::styled("?", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": help  ", Style::default().fg(colors::DIM)),
            Span::styled("esc", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": collapse/unfocus  ", Style::default().fg(colors::DIM)),
            Span::styled("l", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": log  ", Style::default().fg(colors::DIM)),
            Span::styled("L", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": latency view  ", Style::default().fg(colors::DIM)),
            Span::styled("p", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": pause  ", Style::default().fg(colors::DIM)),
            Span::styled("P", Style::default().fg(colors::EMPHASIS).bold()),
            Span::styled(": dump  ", Style::default().fg(colors::DIM)),
        ]);
        frame.render_widget(Paragraph::new(line), area);
    }
}

/// Pull the cycles-servicetime percentiles out of a base-cadence
/// frame. Used to update live display fields and history rings —
/// per-cadence percentile data is served from `MetricsQuery`, not
/// this path.
struct LiveLatency {
    min: u64,
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    max: u64,
}

/// Parse a short duration string like `5m`, `1h30m`, `45s`, `2h` into
/// a `Duration`. Accepts combinations of `d` (days), `h`, `m`, `s` —
/// each unit may appear at most once, in any order. Returns a user-
/// facing error string on failure (rendered in the input prompt).
fn parse_span_short(s: &str) -> Result<Duration, String> {
    if s.is_empty() { return Err("empty span".into()); }
    let mut total = Duration::ZERO;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        if num.is_empty() {
            return Err(format!("expected digits before '{c}'"));
        }
        let n: u64 = num.parse().map_err(|_| format!("bad number '{num}'"))?;
        let mult = match c {
            's' => 1,
            'm' => 60,
            'h' => 3600,
            'd' => 86_400,
            other => return Err(format!("unknown unit '{other}'")),
        };
        total += Duration::from_secs(n.saturating_mul(mult));
        num.clear();
    }
    if !num.is_empty() {
        // Trailing digits without a unit — default to seconds.
        let n: u64 = num.parse().map_err(|_| format!("bad number '{num}'"))?;
        total += Duration::from_secs(n);
    }
    if total.is_zero() { return Err("zero span".into()); }
    Ok(total)
}

/// Format a `Duration` in the same short form `parse_span_short`
/// accepts (`5m`, `1h30m`, …).
fn format_span_short(d: Duration) -> String {
    let mut total = d.as_secs();
    if total == 0 { return "0s".into(); }
    let days  = total / 86_400; total %= 86_400;
    let hours = total / 3600;   total %= 3600;
    let mins  = total / 60;     total %= 60;
    let secs  = total;
    let mut out = String::new();
    if days  > 0 { out.push_str(&format!("{days}d")); }
    if hours > 0 { out.push_str(&format!("{hours}h")); }
    if mins  > 0 { out.push_str(&format!("{mins}m")); }
    if secs  > 0 { out.push_str(&format!("{secs}s")); }
    out
}

fn extract_latency_from_frame(snapshot: &MetricSet) -> Option<LiveLatency> {
    use nb_metrics::snapshot::MetricValue;
    let family = snapshot.family("cycles_servicetime")?;
    for metric in family.metrics() {
        if let Some(point) = metric.point() {
            if let MetricValue::Histogram(h) = point.value() {
                let r = &h.reservoir;
                return Some(LiveLatency {
                    min:  r.min(),
                    p50:  r.value_at_quantile(0.50),
                    p90:  r.value_at_quantile(0.90),
                    p99:  r.value_at_quantile(0.99),
                    p999: r.value_at_quantile(0.999),
                    max:  r.max(),
                });
            }
        }
    }
    None
}
