// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `FormattedLineSink` — non-interactive, non-positioning display
//! sink for log-archive / piped-stderr runs.
//!
//! ## Why it exists
//!
//! `LogOnlySink` renders a live status block by writing absolute-
//! cursor escape sequences (`\x1b[r;c;H`, `\x1b[K`) — invisible in
//! a real terminal, but garbage in a piped or redirected stderr
//! stream. The previous fallback was `tui=off`: the observer
//! writes per-event log lines synchronously, but nothing emits
//! the running-phase progress, so an operator tailing
//! `2> session.log` couldn't tell whether their long-running
//! workload was still making forward progress.
//!
//! This sink fills that gap. It runs alongside the observer's
//! synchronous log writes (no `sink_active` interception) and
//! periodically appends a one-line status snapshot to stderr:
//!
//! ```text
//! [status @ 123.5s] phase 7/10 pvs_query_sweep 45% (ok:100% e:0 r:0 P50:1.2ms)
//! ```
//!
//! Snapshots are append-only — no escape sequences for cursor
//! positioning, no clearing of prior rows. Each tick is a
//! permanent record in whatever stream stderr ends up in. The
//! emitted line is fully self-contained and tail/grep-friendly.
//!
//! ## Cadence
//!
//! Default 5 s. Slower than `LogOnlySink`'s 50 ms because every
//! tick produces a NEW line in the archive — at 5 s a 1 h run
//! adds ~720 status lines, manageable next to the per-event log
//! stream. The cadence is fixed today; if operators want to tune
//! it, add a `tui-status-cadence=` knob.
//!
//! ## Coexistence with the observer
//!
//! The observer's synchronous stderr path (`tui=off` /
//! `sink_active=false`) keeps emitting per-event log lines as
//! usual. This sink interleaves periodic snapshots into the same
//! stream. No locking — stderr writes are line-buffered by the
//! OS, and the two writers don't share state beyond the shared
//! `RunStateHandle`.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::display_sink::{DisplayInputs, DisplaySink, SinkHandle};
use crate::run_state_actor::RunStateHandle;

/// Cadence between status snapshots. 5 s is the default —
/// frequent enough to give an operator tailing a log file a
/// usable sense of forward progress, slow enough that an hour-
/// long run only adds ~720 status lines on top of the per-event
/// log stream.
const STATUS_CADENCE: Duration = Duration::from_secs(5);

/// Non-interactive, non-positioning status emitter. Periodically
/// appends a one-line snapshot of the running state to stderr;
/// coexists with the observer's synchronous log writes.
pub struct FormattedLineSink;

impl Default for FormattedLineSink {
    fn default() -> Self { Self }
}

impl DisplaySink for FormattedLineSink {
    fn start(self: Box<Self>, inputs: DisplayInputs) -> Box<dyn SinkHandle> {
        let DisplayInputs { state, .. } = inputs;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let join = std::thread::Builder::new()
            .name("formatted-line-sink".into())
            .spawn(move || run_emit_loop(state, stop_for_thread))
            .expect("spawn formatted-line-sink thread");
        Box::new(FormattedLineSinkHandle { stop, join: Some(join) })
    }
}

struct FormattedLineSinkHandle {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl SinkHandle for FormattedLineSinkHandle {
    fn shutdown(mut self: Box<Self>) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn run_emit_loop(state: RunStateHandle, stop: Arc<AtomicBool>) {
    // Poll interval: short enough to honor a shutdown request
    // quickly; the cadence-aware emission below only fires once
    // per STATUS_CADENCE.
    const POLL: Duration = Duration::from_millis(250);
    let mut next_emit = std::time::Instant::now() + STATUS_CADENCE;

    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(POLL);
        if std::time::Instant::now() < next_emit {
            continue;
        }
        next_emit = std::time::Instant::now() + STATUS_CADENCE;

        let snap = state.load();
        let line = format_status_snapshot(&snap);
        // Direct stderr write — line-buffered by the OS, no
        // sink_active coordination needed because the observer's
        // synchronous stderr path is the authoritative log
        // emitter and we're only injecting status snapshots
        // alongside it.
        let mut err = std::io::stderr().lock();
        let _ = writeln!(err, "{line}");
        let _ = err.flush();
    }
}

/// Build the one-line status snapshot. Format:
///
/// ```text
/// [status @ 123.5s] phase 7/10 pvs_query_sweep 45% (ok:100% e:0 r:0 P50:1.2ms)
/// ```
///
/// Components elided when the corresponding state is absent
/// (no active phase → just the timestamp + total counter; no
/// total → just `phase N` without a denominator).
fn format_status_snapshot(snap: &Arc<crate::state::RunState>) -> String {
    let elapsed = snap.elapsed_secs();
    let total = snap.expected_total_phases;

    // Phase counter — same logic as the LogOnlySink margin's
    // numerator, derived from the live tree.
    let phase_only: Vec<_> = snap.phases.iter()
        .filter(|p| matches!(p.kind, crate::state::EntryKind::Phase))
        .collect();
    let active_pos = phase_only.iter().position(|p| {
        matches!(p.status, crate::state::PhaseStatus::Running)
    });
    let phase_str = match (active_pos, total) {
        (Some(i), n) if n > 0 => format!("{}/{}", i + 1, n),
        (None,    n) if n > 0 => {
            let done = phase_only.iter().filter(|p|
                !matches!(p.status, crate::state::PhaseStatus::Pending)).count();
            format!("{done}/{n}")
        }
        _ => "?/?".to_string(),
    };

    let active_part = match snap.active_phases.values().next() {
        Some(a) => {
            let pct = if a.cursor_extent > 0 {
                (a.ops_finished as f64) * 100.0 / (a.cursor_extent as f64)
            } else { 0.0 };
            // ok% excludes SKIPS — an `if:`-gated skip is neither a
            // success nor a failure, so the basis is result-producing
            // ops only (`ops_finished - skips`).
            let ok_denom = a.ops_finished.saturating_sub(a.skips);
            let ok_pct = if ok_denom > 0 {
                (a.ops_ok as f64) * 100.0 / (ok_denom as f64)
            } else { 100.0 };
            format!(" {name} {pct:.0}% (ok:{ok_pct:.0}% e:{e} r:{r})",
                name = a.name,
                e = a.errors,
                r = a.retries,
            )
        }
        None => String::new(),
    };

    format!("[status @ {elapsed:.1}s] phase {phase_str}{active_part}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::RunState;

    fn make_state() -> RunState {
        RunState::new("w.yaml", "default", "stdout")
    }

    /// Empty snapshot — no phases, no active. Emits a minimal
    /// well-formed line; the `phase ?/?` marker tells the
    /// operator no plan was registered (or pre-map didn't run).
    #[test]
    fn empty_state_emits_minimal_line() {
        let snap = Arc::new(make_state());
        let line = format_status_snapshot(&snap);
        assert!(line.starts_with("[status @ "),
            "line should be prefixed with the status sentinel: {line}");
        assert!(line.contains("phase ?/?"),
            "missing-plan marker should show `?/?`: {line}");
    }

    /// With a pre-mapped plan but no live phase entries (start
    /// of a run, before any phase has transitioned), the
    /// counter reads `0/total` — denominator pinned to the
    /// pre-map, numerator counts non-pending entries (none).
    #[test]
    fn pre_mapped_no_running_phase_emits_zero_over_total() {
        let mut state = make_state();
        state.expected_total_phases = 7;
        let snap = Arc::new(state);
        let line = format_status_snapshot(&snap);
        assert!(line.contains("phase 0/7"),
            "should show `0/7` when no phases have transitioned: {line}");
        // No active phase → no name/pct/counters tail.
        assert!(!line.contains("ok:"),
            "no active-phase tail should appear: {line}");
    }
}
