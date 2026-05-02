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
        if total > last_seen {
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
                if severity_to_level(entry.severity) < min_level {
                    continue;
                }
                // Match the observer's cosmetic blank line before
                // the Ctrl-C / force-exit banners.
                if entry.message.starts_with("session: graceful shutdown requested")
                    || entry.message.starts_with("session: force-exit on second")
                {
                    let _ = write!(stderr, "\r\n");
                }
                // `\r\n` line endings so output stays correct
                // when the sink supervisor has stdin in raw mode
                // for the Ctrl-T watcher. In cooked mode the
                // extra `\r` is a no-op (already at col 0).
                let _ = write!(stderr, "{}\r\n", entry.message);
            }
            let _ = stderr.flush();
            last_seen = total;
        }

        std::thread::sleep(POLL_INTERVAL);
    }
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
