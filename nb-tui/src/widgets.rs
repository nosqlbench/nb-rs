// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Widget helpers: sparkline rendering, latency bars, color palette.

use ratatui::style::Color;

/// 24-bit color palette for the TUI.
pub mod colors {
    use super::Color;

    pub const BG: Color = Color::Rgb(26, 26, 46);
    pub const BORDER: Color = Color::Rgb(58, 58, 92);
    pub const TEXT: Color = Color::Rgb(224, 224, 224);
    pub const EMPHASIS: Color = Color::Rgb(255, 255, 255);
    pub const DIM: Color = Color::Rgb(96, 96, 96);

    pub const PHASE_ACTIVE: Color = Color::Rgb(122, 193, 66);
    pub const PHASE_PENDING: Color = Color::Rgb(128, 128, 128);
    pub const PHASE_DONE: Color = Color::Rgb(76, 175, 80);
    pub const PHASE_FAILED: Color = Color::Rgb(244, 67, 54);

    // Phase-tint palette for the scenario tree:
    //   RUNNING_TINT  — currently running (yellow)
    //   DONE_CLEAN    — completed with no errors (green — reuse PHASE_DONE)
    //   DONE_WARN     — completed with some errors (orange)
    //   DONE_BAD      — completed with many errors (red-orange)
    //
    // Chosen for colorblind safety: the yellow/orange/red progression
    // preserves hue separation under protanopia/deuteranopia (the
    // common red-green forms), and the bundled glyphs (`▶`, `✓`, `✗`)
    // plus brightness differences reinforce the meaning for users
    // with tritanopia or monochrome terminals.
    pub const PHASE_RUNNING_TINT: Color = Color::Rgb(247, 201, 72);
    pub const PHASE_DONE_WARN: Color = Color::Rgb(255, 140, 0);
    pub const PHASE_DONE_BAD: Color = Color::Rgb(214, 70, 40);

    pub const PROGRESS_LOW: Color = Color::Rgb(45, 90, 39);
    pub const PROGRESS_HIGH: Color = Color::Rgb(122, 193, 66);

    pub const OK_BADGE: Color = Color::Rgb(76, 175, 80);
    pub const ERROR_BADGE: Color = Color::Rgb(244, 67, 54);

    pub const LAT_P50: Color = Color::Rgb(77, 201, 246);
    pub const LAT_P90: Color = Color::Rgb(247, 201, 72);
    pub const LAT_P99: Color = Color::Rgb(247, 127, 0);
    pub const LAT_MAX: Color = Color::Rgb(214, 40, 40);

    pub const SPARK: Color = Color::Rgb(77, 201, 246);

    pub const LOG_DEBUG: Color = Color::Rgb(96, 96, 96);
    pub const LOG_INFO: Color = Color::Rgb(77, 201, 246);
    pub const LOG_WARN: Color = Color::Rgb(247, 201, 72);
    pub const LOG_ERROR: Color = Color::Rgb(244, 67, 54);
}

/// Render a sparkline from a slice of values into a string of
/// Unicode block characters: ▁▂▃▄▅▆▇█
///
/// Auto-ranges to the local min/max of the visible window so
/// micro-variations are visible even when throughput is stable.
/// A perfectly flat line renders as mid-height bars.
pub fn sparkline_str(values: &[f64], width: usize) -> String {
    let blocks = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    if values.is_empty() {
        return " ".repeat(width);
    }
    let start = if values.len() > width { values.len() - width } else { 0 };
    let visible = &values[start..];

    let min = visible.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = visible.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = max - min;

    let mut s = String::with_capacity(width * 3);
    for &v in visible {
        if range <= 0.0 {
            // Flat line — show mid-height
            s.push(blocks[4]);
        } else {
            let normalized = (v - min) / range;
            let idx = (normalized * 7.0).round() as usize;
            s.push(blocks[idx.min(7)]);
        }
    }
    // Pad if fewer values than width
    while s.chars().count() < width {
        s.insert(0, ' ');
    }
    s
}

/// Format nanoseconds into a human-readable duration string.
/// Short human label for a cadence Duration: `10s`, `1m`, `5m`, `1h`.
pub fn format_cadence(d: std::time::Duration) -> String {
    let total = d.as_secs();
    if total >= 3600 && total % 3600 == 0 {
        format!("{}h", total / 3600)
    } else if total >= 60 && total % 60 == 0 {
        format!("{}m", total / 60)
    } else {
        format!("{}s", total)
    }
}

pub fn format_nanos(nanos: u64) -> String {
    if nanos == 0 {
        return "—".to_string();
    }
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.1}µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
    }
}

/// Format elapsed seconds into M:SS or H:MM:SS.
pub fn format_elapsed(secs: f64) -> String {
    let total = secs as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    }
}

/// Format a rate value with auto-scaling (K/M suffix).
///
/// Uses a consistent decimal width within each magnitude band so
/// values that oscillate across a boundary don't flip formats
/// frame-to-frame (e.g. 0.99 ↔ 1.00, not 0.99 ↔ 1).
pub fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K", rate / 1_000.0)
    } else {
        format!("{:.2}", rate)
    }
}

/// Format a count with auto-scaling.
pub fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{n}")
    }
}

/// Build a horizontal bar string of given width, filled proportionally.
pub fn bar_str(fraction: f64, width: usize) -> String {
    let fill = (fraction.clamp(0.0, 1.0) * width as f64).round() as usize;
    let mut s = String::with_capacity(width);
    for _ in 0..fill { s.push('━'); }
    for _ in fill..width { s.push('╌'); }
    s
}
