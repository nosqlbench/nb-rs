// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Small presentation helpers shared across readouts.
//!
//! Duration formatting, rate auto-scaling, the braille
//! progress bar, and the spinner frame cycle. These were
//! private to `nbrs-activity::activity` before Push 2 and
//! now live next to the readouts that consume them.

/// Standard 10-frame braille spinner cycle. Picks a frame
/// deterministically from `tick % 10` so a refresh actor
/// firing at a steady cadence renders smooth animation.
pub fn spinner_frame(tick: u64) -> char {
    static FRAMES: [char; 10] = [
        '⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏',
    ];
    FRAMES[(tick as usize) % FRAMES.len()]
}

/// 10-character braille completion bar. `pct` is clamped to
/// [0, 100]; each char represents 10 percentage points
/// with 8 within-char sub-levels via the standard bottom-up
/// braille fill pattern, so the bar fills smoothly at
/// ~1.25-percent resolution.
pub fn braille_bar(pct: f64, width: usize) -> String {
    static FILL: [char; 9] = [
        '\u{2800}', // ⠀  empty
        '\u{2840}', // ⡀  +dot 7
        '\u{28C0}', // ⣀  +dot 8
        '\u{28C4}', // ⣄  +dot 3
        '\u{28E4}', // ⣤  +dot 6
        '\u{28E6}', // ⣦  +dot 2
        '\u{28F6}', // ⣶  +dot 5
        '\u{28F7}', // ⣷  +dot 1
        '\u{28FF}', // ⣿  full (+dot 4)
    ];
    if width == 0 { return String::new(); }
    let bounded = pct.clamp(0.0, 100.0);
    let total = (bounded / 100.0 * (width as f64) * 8.0).round() as usize;
    let total = total.min(width * 8);
    let full = total / 8;
    let part = total % 8;
    let mut s = String::with_capacity(width * 3);
    for _ in 0..full { s.push(FILL[8]); }
    if full < width {
        s.push(FILL[part]);
        for _ in (full + 1)..width { s.push(FILL[0]); }
    }
    s
}

/// Compact ETA ladder: under a minute → `Ns`; under an
/// hour → `NmMMs`; otherwise → `NhMMm`. Returns `—` for
/// non-finite / negative inputs so a stalled rate doesn't
/// produce a misleading number.
pub fn format_eta(remaining_secs: f64) -> String {
    if !remaining_secs.is_finite() || remaining_secs < 0.0 {
        return "—".to_string();
    }
    let secs = remaining_secs.round() as u64;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Auto-scaled throughput rate.
pub fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.1}M/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K/s", rate / 1_000.0)
    } else {
        format!("{:.0}/s", rate)
    }
}
