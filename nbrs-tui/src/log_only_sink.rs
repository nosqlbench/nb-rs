// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `LogOnlySink` — `DisplaySink` impl that renders every
//! log entry to stderr.
//!
//! ## Role
//!
//! In Phase 2 of the display-sink refactor (see
//! [`crate::display_sink`]) this sink is the canonical renderer
//! for `tui=off` mode. The
//! [`crate::log_only_observer::LogOnlyObserver`] sends every
//! log event to the actor; this sink polls the actor's published
//! [`crate::state::RunState`] snapshot, drains anything new from
//! the log ring (tracked via
//! [`crate::state::RunState::log_seq_total`]), applies the
//! observer-supplied severity filter, and emits stderr lines
//! identical to the legacy `StderrObserver` path.
//!
//! ## Coordination with the observer
//!
//! The sink takes a shared `sink_active` flag from the observer
//! it's paired with (see
//! [`crate::log_only_observer::LogOnlyObserver::sink_active_flag`]).
//! The startup sequence is:
//!
//! 1. `LogOnlySink::new` records `last_seen_seq = 0` (provisional).
//! 2. `start()` reads the actor's current snapshot, sets
//!    `last_seen_seq = snapshot.log_seq_total` so any pre-sink
//!    entries already on stderr (from the observer's synchronous
//!    write) aren't re-rendered.
//! 3. `start()` sets `sink_active = true` — from this moment the
//!    observer suppresses its own stderr writes and the sink owns
//!    the surface.
//! 4. The render thread polls every ~50 ms, drains
//!    `(log_seq_total - last_seen_seq)` entries off the tail of
//!    `log_messages`, prints those that pass `min_level`, and
//!    advances `last_seen_seq`.
//!
//! `shutdown()` clears `sink_active` so the observer resumes
//! synchronous writes for any straggler logs that fire after the
//! sink is gone.
//!
//! ## Polling cadence
//!
//! 50 ms. Fast enough that a human operator doesn't perceive lag
//! between an event and its line appearing; slow enough that
//! idle ticks have negligible CPU cost. The cadence reporter's
//! frame channel is also drained on the same loop so it never
//! reports a full / disconnected channel.
//!
//! ## Drop-on-overflow
//!
//! If `(log_seq_total - last_seen_seq)` exceeds the ring's
//! capacity (200), the sink lost some entries — i.e. the
//! observer logged faster than the sink could drain. The
//! diagnostic notes the count and continues; the dropped lines
//! are still in `session.log` (the async sink in
//! `nbrs_activity::log_sink` takes every level unconditionally,
//! see SRD 02 §"Display and Diagnostic Decoupling").

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use nbrs_activity::observer::LogLevel;

use crate::display_sink::{DisplayInputs, DisplaySink, SinkHandle};
use crate::state::LogSeverity;

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const LOG_RING_CAPACITY: u64 = 200;

/// `tui=off` log-stream sink.
pub struct LogOnlySink {
    /// Severity floor. The paired observer's filter, propagated
    /// here so pre-sink and post-sink stderr output use the same
    /// rule. Lines below `min_level` are silently dropped (still
    /// captured by the async session-log sink).
    min_level: LogLevel,
    /// Coordination flag shared with
    /// [`crate::log_only_observer::LogOnlyObserver`]. The sink
    /// flips it `true` on `start`, `false` on `shutdown`.
    sink_active: Arc<AtomicBool>,
}

impl LogOnlySink {
    pub fn new(min_level: LogLevel, sink_active: Arc<AtomicBool>) -> Self {
        Self { min_level, sink_active }
    }
}

fn severity_to_level(s: LogSeverity) -> LogLevel {
    match s {
        LogSeverity::Debug => LogLevel::Debug,
        LogSeverity::Info  => LogLevel::Info,
        LogSeverity::Warn  => LogLevel::Warn,
        LogSeverity::Error => LogLevel::Error,
    }
}

impl DisplaySink for LogOnlySink {
    fn start(self: Box<Self>, inputs: DisplayInputs) -> Box<dyn SinkHandle> {
        let DisplayInputs { state, frame_rx, metrics_query: _ } = inputs;
        let LogOnlySink { min_level, sink_active } = *self;

        // Snapshot once before claiming the surface so we don't
        // re-emit anything the observer already printed pre-flag.
        let initial_seq = state.load().log_seq_total;
        sink_active.store(true, Ordering::Release);

        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let sink_active_for_thread = sink_active.clone();

        let join = std::thread::Builder::new()
            .name("log-only-sink".into())
            .spawn(move || {
                run_render_loop(
                    state,
                    frame_rx,
                    initial_seq,
                    min_level,
                    stop_for_thread,
                );
                // Render thread exited (stop signaled or channel
                // disconnected). Clear the flag so the observer
                // resumes synchronous writes.
                sink_active_for_thread.store(false, Ordering::Release);
            })
            .expect("spawn log-only-sink thread");

        Box::new(LogOnlySinkHandle {
            stop,
            join: Some(join),
            sink_active,
        })
    }
}

fn run_render_loop(
    state: crate::run_state_actor::RunStateHandle,
    frame_rx: Option<std::sync::mpsc::Receiver<nbrs_metrics::snapshot::MetricSet>>,
    mut last_seen: u64,
    min_level: LogLevel,
    stop: Arc<AtomicBool>,
) {
    let mut stderr = io::stderr();
    // The raw status string most recently published by the
    // actor and reflected on the terminal. We compare against
    // *this* (not the clamped form actually written to the
    // surface) so identity checks are stable across ticks.
    let mut status_published: Option<String> = None;
    // The clamped, per-line-truncated text actually drawn at
    // the bottom of the surface. Tracked so the next clear
    // knows how many rows to climb past. `None` means nothing
    // is drawn.
    let mut status_drawn: Option<String> = None;
    while !stop.load(Ordering::Acquire) {
        // Drain any metrics frames that arrived since the last
        // tick — only when a frame channel was actually wired in.
        // For pure log-only mode no reporter is registered, so
        // `frame_rx == None` and there's nothing to drain. Phase
        // 2b's FakeTuiSink will use these to drive a periodic
        // status line; the LogOnlySink discards them.
        if let Some(rx) = &frame_rx {
            loop {
                match rx.try_recv() {
                    Ok(_frame) => {}
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                }
            }
        }

        // Drain new log entries.
        let snap = state.load();
        let total = snap.log_seq_total;
        let next_status: Option<String> = snap.status_render.clone();
        let need_log_emit = total > last_seen;
        let status_changed = next_status != status_published;

        // If there's a status region currently occupied AND
        // anything is about to print (log lines or the status
        // itself changing), wipe it first. We're the only writer
        // to this surface — clearing here means the cursor
        // returns to a known starting column before logs append.
        let must_clear_status =
            status_drawn.is_some() && (need_log_emit || status_changed);
        if must_clear_status {
            clear_status_region(&mut stderr, status_drawn.as_deref());
            status_drawn = None;
        }

        if need_log_emit {
            let new_count = total - last_seen;
            let take = new_count.min(LOG_RING_CAPACITY) as usize;
            let ring = &snap.log_messages;
            let start_idx = ring.len().saturating_sub(take);

            // Note any drop. The session-log sink still has
            // every entry; this is a render-side warning only.
            if new_count > LOG_RING_CAPACITY {
                let dropped = new_count - LOG_RING_CAPACITY;
                let _ = write!(
                    stderr,
                    "log-only-sink: dropped {dropped} log line(s) (renderer too slow); see session.log\r\n",
                );
            }

            for entry in &ring[start_idx..] {
                let entry_level = severity_to_level(entry.severity);
                if entry_level < min_level {
                    continue;
                }
                // Match the observer's cosmetic blank line before
                // the Ctrl-C / force-exit banners.
                if entry.message.starts_with("session: graceful shutdown requested")
                    || entry.message.starts_with("session: force-exit on second")
                {
                    let _ = write!(stderr, "\r\n");
                }
                // Colorize by severity. `colorize_log_line` is
                // a no-op on non-tty / NO_COLOR so log captures
                // stay clean.
                let painted = nbrs_activity::observer::colorize_log_line(
                    entry_level, &entry.message);
                // Split on embedded `\n` so multi-line log
                // messages (e.g. the two-line phase_done
                // render) get `\r\n` after every row. Raw mode
                // needs explicit `\r` to return to col 0; a
                // bare `\n` leaves the cursor under the
                // previous row's last character.
                for row in painted.split('\n') {
                    let _ = write!(stderr, "{row}\r\n");
                }
            }
            let _ = stderr.flush();
            last_seen = total;
        }

        // Redraw the status line at the bottom if there is one.
        // The redraw happens AFTER the log drain so any log
        // lines just emitted scroll past while the status line
        // stays visually anchored to the current cursor row.
        if let Some(s) = &next_status {
            // Always redraw when the published status changed or
            // when the previous draw was cleared by a log emit.
            if status_changed || status_drawn.is_none() {
                let cols = nbrs_activity::activity::terminal_cols().unwrap_or(200);
                let clamped = clamp_multiline(s, cols.saturating_sub(1));
                draw_status_region(&mut stderr, &clamped);
                status_drawn = Some(clamped);
            }
        }
        status_published = next_status;

        std::thread::sleep(POLL_INTERVAL);
    }

    // Sink shutting down — wipe the status region we own so
    // the post-run terminal state isn't littered with our
    // final tick's text.
    if status_drawn.is_some() {
        clear_status_region(&mut stderr, status_drawn.as_deref());
    }
}

/// Clear the status region drawn by [`draw_status_region`].
/// Counts the embedded `\n`s in the prior render so a multi-
/// line status (future expansion) clears all of its rows, not
/// just the bottom one. Single-line callers see `\r\x1b[K`
/// (the legacy in-place clear).
fn clear_status_region<W: Write>(out: &mut W, prior: Option<&str>) {
    let lines = prior.map(|s| s.matches('\n').count() as u16 + 1).unwrap_or(1);
    if lines > 1 {
        // Cursor sits at end of the prior render's last row;
        // climb back to the first row, then `\x1b[J` wipes
        // from the cursor through end of screen.
        let _ = write!(out, "\r\x1b[{}A\x1b[J", lines - 1);
    } else {
        let _ = write!(out, "\r\x1b[K");
    }
    let _ = out.flush();
}

/// Write the status region. Caller has ensured the cursor is
/// on a clean row (either freshly cleared by
/// [`clear_status_region`] or just after a log line's `\r\n`).
/// No trailing newline so the cursor stays at the end of the
/// final status row — the next [`clear_status_region`] call
/// computes its climb from there.
///
/// Embedded `\n` row breaks are upgraded to `\r\n` so the
/// cursor returns to column 0 even when the sink supervisor
/// has stdin in raw mode for the Ctrl-T watcher.
fn draw_status_region<W: Write>(out: &mut W, status: &str) {
    let mut first = true;
    for row in status.split('\n') {
        if !first { let _ = write!(out, "\r\n"); }
        let _ = write!(out, "{row}");
        first = false;
    }
    let _ = out.flush();
}

/// Clamp each `\n`-delimited row of `s` to `max_cols` columns
/// independently, then rejoin with `\n`. `\n` itself is not a
/// visible column and must not consume the budget; per-row
/// clamping prevents the second line of a two-line status
/// from being chopped off when the first line is long.
fn clamp_multiline(s: &str, max_cols: usize) -> String {
    let mut out = String::with_capacity(s.len());
    let mut first = true;
    for row in s.split('\n') {
        if !first { out.push('\n'); }
        out.push_str(&nbrs_activity::activity::truncate_to_width(row, max_cols));
        first = false;
    }
    out
}

struct LogOnlySinkHandle {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    sink_active: Arc<AtomicBool>,
}

impl SinkHandle for LogOnlySinkHandle {
    fn shutdown(mut self: Box<Self>) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        // Defensively clear in case the thread exited early
        // before the post-loop store ran (e.g. spawn failure
        // in some future variant).
        self.sink_active.store(false, Ordering::Release);
    }

    fn owns_terminal(&self) -> bool { false }
}
