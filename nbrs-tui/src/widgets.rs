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

/// Reverse a leaf-first scope-coordinate label string into its
/// root-first form for display. The canonical
/// [`format_scope_coordinate_path`](nbrs_variates::kernel::scope_coords::format_scope_coordinate_path)
/// emits `(inner_a=…, inner_b=…), (outer=…)` (leaf-first) so it
/// stays stable as the canonical structural identity used for
/// pre-map ↔ runtime matching. Display surfaces (the terminal
/// observer's phase row and the TUI active-phase panel) prefer
/// root-first reading order — outer scopes first — to mirror
/// the scenario tree the user wrote.
///
/// Splits on the unambiguous group boundary `"), ("` (paren-comma-
/// space-paren never appears inside a binding's value), reverses
/// the segment list, and rejoins. Empty input → empty output.
/// Single-group input is returned unchanged.
pub fn coords_root_first(leaf_first: &str) -> String {
    if leaf_first.is_empty() { return String::new(); }
    let inner = leaf_first
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(leaf_first);
    let parts: Vec<&str> = inner.split("), (").collect();
    if parts.len() <= 1 {
        return leaf_first.to_string();
    }
    let rev: Vec<&str> = parts.into_iter().rev().collect();
    format!("({})", rev.join("), ("))
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

/// Build a horizontal bar string of given width using full-cell
/// block glyphs — denser/heavier than [`bar_str`]. Used for the
/// cursor-progress bar in the phase detail block where visual
/// weight matters more than alignment with nearby latency bars.
pub fn bar_str_thick(fraction: f64, width: usize) -> String {
    let fill = (fraction.clamp(0.0, 1.0) * width as f64).round() as usize;
    let mut s = String::with_capacity(width);
    for _ in 0..fill { s.push('█'); }
    for _ in fill..width { s.push('░'); }
    s
}

/// Build a horizontal Braille-pip progress bar of `width` cells,
/// returning the filled and unfilled halves as separate strings
/// so the caller can color them differently.
///
/// Each cell is a Braille pattern (2 columns × 4 dots = 8 pips).
/// Sub-cell precision: 8 pips per cell × `width` cells. A 100-cell
/// bar resolves to 800 distinct positions.
///
/// Pip ordering within a cell is **bottom-up, left column first
/// then right column** — pips light up like a rising tide:
///
/// ```text
///   .  .       .  .       .  .       .  .       *  .       *  *
///   .  .       .  .       .  .       *  .  →    *  .  →    *  *
///   .  .       .  .       *  .  →    *  .       *  .       *  *
///   .  .  →    *  .  →    *  .       *  .       *  .       *  *
///   1/8        2/8        4/8        5/8        6/8        8/8
/// ```
///
/// The unfilled half uses `⣀` (U+28C0, bottom-row dots in both
/// columns) — a low-key baseline that shows the bar's total
/// width without competing with the filled portion.
///
/// Returns `(filled, unfilled)`. The filled string ends with the
/// boundary cell's partial-pip pattern (or is exactly `width`
/// cells of `⣿` at 100%); the unfilled string is the remaining
/// cells of `⣀`.
pub fn bar_str_braille(fraction: f64, width: usize) -> (String, String) {
    if width == 0 {
        return (String::new(), String::new());
    }
    // 8 pips per cell × `width` cells = total resolution.
    let total_pips = width * 8;
    let lit_pips = (fraction.clamp(0.0, 1.0) * total_pips as f64).round() as usize;
    let full_cells = lit_pips / 8;
    let partial = lit_pips % 8;

    // Unicode 8-dot Braille bit positions:
    //   dot 1: top-left      (0x01)
    //   dot 2: middle-top-l  (0x02)
    //   dot 3: middle-bot-l  (0x04)
    //   dot 4: top-right     (0x08)
    //   dot 5: middle-top-r  (0x10)
    //   dot 6: middle-bot-r  (0x20)
    //   dot 7: bottom-left   (0x40)
    //   dot 8: bottom-right  (0x80)
    //
    // Light pips bottom-up within each column so a partially-
    // filled cell reads as a rising silhouette rather than a
    // hanging-from-the-top one. Left column first (the
    // earlier-progress side), then right column.
    const PIP_ORDER: [u32; 8] = [
        0x40, 0x04, 0x02, 0x01,  // left column: bottom → top
        0x80, 0x20, 0x10, 0x08,  // right column: bottom → top
    ];

    let mut filled = String::with_capacity(width * 3);
    // Solid block (`█`) for fully-filled cells — crisper and
    // more "settled" than Braille's all-pips glyph (`⣿`).
    // Braille is reserved for the boundary cell where the
    // sub-cell animation actually happens, so the eye is
    // drawn to the moving frontier rather than the static
    // bulk of the bar.
    for _ in 0..full_cells { filled.push('\u{2588}'); }
    if partial > 0 && full_cells < width {
        let mut bits: u32 = 0;
        for i in 0..partial {
            bits |= PIP_ORDER[i];
        }
        // Unicode Braille block starts at U+2800; the low byte
        // is the pip-pattern bitmask.
        let codepoint = 0x2800u32 + bits;
        if let Some(c) = char::from_u32(codepoint) {
            filled.push(c);
        } else {
            filled.push('\u{2800}');
        }
    }

    let drawn = full_cells + if partial > 0 && full_cells < width { 1 } else { 0 };
    let mut unfilled = String::with_capacity((width - drawn) * 3);
    // `⣀` (U+28C0) — dots 7 + 8, the bottom row of both
    // columns. Reads as a low-key baseline that grounds the
    // unfilled portion without grabbing focus from the lit
    // pips above.
    for _ in drawn..width { unfilled.push('\u{28C0}'); }

    (filled, unfilled)
}
