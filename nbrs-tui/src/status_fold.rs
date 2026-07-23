// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-100 P2 — consumer-side per-phase status renderer.
//!
//! The live multi-phase status is produced by **folding the snapshot at
//! the consumer**, not by N per-phase producer threads submitting strings
//! to one slot (SRD-100 §6). Each display surface walks `active_phases`
//! and calls [`render_phase_status`] on every entry to re-derive *that
//! phase's* status line from its [`PhaseRenderHandle`]
//! (`nbrs_runtime::observer`). This replaces the retired inline-status
//! refresh thread + the single `RunState::status_render` scalar that the
//! threads stomped under concurrency.
//!
//! **Byte-identity (SRD-100 §12 A1):** the render reuses the producer's
//! own `build_inline_refresh_context` and fires the same `BakedBody`
//! template, so a single-phase run renders identical bytes to the
//! pre-SRD-100 producer path. `BakedBody` is `Send + Sync`, so the format
//! template rides the ArcSwap snapshot as pure data; only the `!Sync`
//! *binder* is kept out (bodies fire with `&self`).

use nbrs_runtime::build_inline_refresh_context;
use nbrs_runtime::readouts::{ContentMode, StringSink};

use crate::state::{ActivePhase, PhaseStatus, RunState};
use crate::widgets::format_dur_compact;

/// Collect the active phases in **stable dispatch order** — pre-map `seq`
/// first (so sibling order matches the plan), then `(name, labels)` for a
/// total order independent of `HashMap` iteration. Shared by every status
/// fold so all surfaces agree on ordering (SRD-100 §7a/§12).
pub fn active_phases_ordered(snap: &RunState) -> Vec<&ActivePhase> {
    let mut phases: Vec<&ActivePhase> = snap.active_phases.values().collect();
    phases.sort_by(|a, b| {
        let ka = (a.render.as_ref().and_then(|h| h.seq), &a.name, &a.labels);
        let kb = (b.render.as_ref().and_then(|h| h.seq), &b.name, &b.labels);
        ka.cmp(&kb)
    });
    phases
}

/// Fold every active phase into one status block — one phase's status
/// render per stable-ordered entry, joined by newlines. This is the P2
/// consumer-side replacement for the single `status_render` scalar; P3
/// layers the height cap, multi-running counter, and overflow roll-up on
/// top. Returns `None` when no active phase has a renderable status (so a
/// surface clears its footer exactly as the old `status(None)` did).
pub fn render_active_status(snap: &RunState) -> Option<String> {
    render_active_status_with_gutters(snap).map(|(text, _)| text)
}

/// Per-line CONTEXTUAL GUTTER content for the footer's left margin.
/// The gutter cell beside each phase's DETAIL row (the stats line
/// under its header) belongs to that phase — a completion bar for
/// metered phases, a latency trend for open-ended pollers. Header
/// rows stay aligned with the workload-level tracking timers (row 0's
/// triad); every other row gets the plain divider.
#[derive(Clone, Debug, PartialEq)]
pub enum RowGutter {
    /// Plain blank-aligned divider.
    Blank,
    /// Metered phase: completion-bar fill fraction.
    Bar(f64),
    /// Open-ended phase (daemon poller): latency trend. `key`
    /// identifies the phase so the sink's sample ring persists
    /// across ticks; p50/p99 are the current service-time
    /// percentiles in nanos.
    Latency { key: String, p50: u64, p99: u64 },
    /// Workload-declared layout text (`gutter: "<template>"`),
    /// placed in the cell verbatim (truncated to fit).
    Text(String),
    /// Workload-declared trend sample (`gutter: {spark: ...}`):
    /// sparkline ring + current value. `key` persists the ring
    /// across ticks, like `Latency`.
    Spark { key: String, value: f64 },
    /// Open-ended phase, LIFETIME-histogram form: the sink's
    /// decimating trend buffer keeps the phase's whole latency
    /// history renderable at cell resolution (one sample per cell
    /// until the width fills, then re-averaged at half resolution,
    /// etc.), labeled with the discrete lifetime min∕max. Distinct
    /// renderable from the rolling `Latency` form.
    LatencyHist { key: String, p50: u64 },
}

/// As [`render_active_status`], additionally returning one
/// [`RowGutter`] per rendered line (indices align with the '\n'
/// split of the returned text).
pub fn render_active_status_with_gutters(
    snap: &RunState,
) -> Option<(String, Vec<RowGutter>)> {
    let mut lines: Vec<String> = Vec::new();
    let mut gutters: Vec<RowGutter> = Vec::new();
    for p in active_phases_ordered(snap) {
        if let Some(status) = render_phase_status(p, snap.elapsed_secs()) {
            // Tag this phase's block: memo rows (leading `[[`) and the
            // header row get Blank; the DETAIL row (first line after
            // the header) carries the phase's contextual gutter.
            let block: Vec<&str> = status.split('\n').collect();
            let header_idx = block.iter()
                .position(|l| !l.trim_start().starts_with("[["))
                .unwrap_or(0);
            let detail_idx = header_idx + 1;
            let ctx_gutter = phase_context_gutter(p);
            for (i, line) in block.iter().enumerate() {
                lines.push((*line).to_string());
                gutters.push(if i == detail_idx {
                    ctx_gutter.clone()
                } else {
                    RowGutter::Blank
                });
            }
            for leaf in render_op_leaves(snap, p) {
                lines.push(leaf);
                gutters.push(RowGutter::Blank);
            }
        }
    }
    if lines.is_empty() {
        None
    } else {
        Some((lines.join("\n"), gutters))
    }
}

/// The contextual gutter payload for one phase: latency trend for
/// open-ended daemons, completion bar for metered phases (fraction
/// basis mirrors `ReadoutContext::progress_fraction`: override →
/// rows → cycles), Blank when nothing meaningful exists.
fn phase_context_gutter(p: &ActivePhase) -> RowGutter {
    let Some(handle) = p.render.as_ref() else { return RowGutter::Blank };
    // A workload-declared `gutter:` wrapper spec overrides the
    // automatic derivation — the phase owns its cell.
    if let Some(spec) = handle.gutter.load_full() {
        use nbrs_runtime::wrappers::gutter::GutterSpec;
        return match spec.as_ref() {
            GutterSpec::Text(s) => RowGutter::Text(s.clone()),
            GutterSpec::Bar(f) => RowGutter::Bar(*f),
            GutterSpec::Spark(v) => RowGutter::Spark {
                key: format!("spark:{}@{}", p.name, p.labels),
                value: *v,
            },
        };
    }
    if p.daemon {
        // Sliding-window view, NOT the delta reservoir: the metrics
        // reporter drains the reservoir on its cadence, so a peek
        // right after a drain sees an empty histogram and the cell
        // would blink out. The 10 s rolling window keeps the
        // percentiles time-averaged and persistent between
        // refreshes; only a genuinely silent daemon blanks the cell.
        let ring = handle.metrics.service_time.enable_live_window(
            nbrs_metrics::summaries::live_window::LiveWindowConfig {
                window: std::time::Duration::from_secs(10),
                ..Default::default()
            });
        let h = ring.peek();
        if h.is_empty() {
            return RowGutter::Blank;
        }
        // Lifetime-histogram form: the whole phase's latency history
        // stays visible (decimated to cell width) rather than a
        // rolling last-N-ticks window. The rolling `Latency` form
        // remains available as the alternative renderable.
        return RowGutter::LatencyHist {
            key: format!("{}@{}", p.name, p.labels),
            p50: h.value_at_quantile(0.50),
        };
    }
    let frac = handle.metrics.progress_override()
        .or_else(|| (p.rows_total > 0).then(|| {
            (p.rows_consumed as f64 / p.rows_total as f64).clamp(0.0, 1.0)
        }))
        .or_else(|| (p.cursor_extent > 0).then(|| {
            (handle.metrics.cycles_completed() as f64 / p.cursor_extent as f64)
                .clamp(0.0, 1.0)
        }));
    match frac {
        Some(f) => RowGutter::Bar(f),
        None => RowGutter::Blank,
    }
}

/// SRD-63 — render an active phase's op-level status leaves (ops that declared
/// `readout: visible`) as plain footer lines nested under the phase: status
/// icon · name · `[i/N]` · this op's execution time. Single-placement rule:
/// a RUNNING row shows its leaf timer only (the live session clock is the
/// margin's datum, one row up); a TERMINAL row appends `@ <session>` — the
/// session stamp at which it finished, which has no other home on this
/// surface. The sink prepends its own footer gutter to every line.
///
/// The leaves live in `phase_ops` keyed by the phase's scene-node id, which the
/// `ActivePhase` doesn't carry; we resolve it from the running tree row with a
/// matching `(name, labels)`. Returns empty (no allocation of leaf lines) when
/// the phase has no row yet or no opted-in ops.
fn render_op_leaves(snap: &RunState, phase: &ActivePhase) -> Vec<String> {
    let node_id = match snap.phases.iter().find(|e| {
        e.name == phase.name
            && e.labels == phase.labels
            && matches!(e.status, PhaseStatus::Running)
    }) {
        Some(e) => e.node_id,
        None => return Vec::new(),
    };
    let ops = match snap.phase_ops.get(&node_id) {
        Some(ops) if !ops.is_empty() => ops,
        _ => return Vec::new(),
    };
    let total = ops.len();
    ops.iter()
        .map(|op| {
            let (icon, leaf, sess): (&str, Option<f64>, Option<f64>) = match &op.status {
                PhaseStatus::Running => (
                    op_spinner((snap.elapsed_secs() - op.session_started).max(0.0)),
                    Some((snap.elapsed_secs() - op.session_started).max(0.0)),
                    // Live session clock belongs to the margin, not here.
                    None,
                ),
                PhaseStatus::Completed => ("✓", op.duration_secs, op.session_elapsed),
                PhaseStatus::Failed(_) => ("✗", op.duration_secs, op.session_elapsed),
                PhaseStatus::Pending => ("○", None, None),
            };
            let leaf_s = leaf.map(format_dur_compact).unwrap_or_else(|| "—".to_string());
            // Terminal rows carry their session finish-stamp (`@ 12.3s`) —
            // unique info on this surface; running rows carry the leaf
            // timer alone (session is the margin's, one row up).
            let time_part = match sess {
                Some(v) => format!("{leaf_s} @ {}", format_dur_compact(v)),
                None => leaf_s,
            };
            let mut line = format!(
                "    {icon} {name}  [{seq}/{total}]  {time_part}",
                name = op.name,
                seq = op.seq + 1,
            );
            if let PhaseStatus::Failed(err) = &op.status {
                line.push_str("  ");
                line.push_str(err);
            }
            line
        })
        .collect()
}

/// Per-op running spinner, derived from the op's own elapsed time (250 ms
/// cadence) so it animates without threading a sink-side tick counter through —
/// the same elapsed-derived approach `render_phase_status` uses for its frame.
fn op_spinner(elapsed: f64) -> &'static str {
    use throbber_widgets_tui::symbols::throbber::BRAILLE_SIX;
    let idx = ((elapsed * 4.0) as usize) % BRAILLE_SIX.symbols.len();
    BRAILLE_SIX.symbols[idx]
}

/// Render one active phase's status line by folding its live render
/// handle. Returns `None` when the phase has no handle yet (the brief
/// window between `phase_starting` and the executor's on-task attach), no
/// bound `on_update` bodies, or an empty render.
///
/// The readout's spinner frame is derived from the phase's own elapsed
/// time at the retired producer thread's 500 ms cadence (`elapsed * 2`),
/// so the animation rate stays identical and per-phase — no sink-side
/// tick counter is threaded through.
pub fn render_phase_status(phase: &ActivePhase, session_now: f64) -> Option<String> {
    let handle = phase.render.as_ref()?;
    if handle.bodies.is_empty() {
        return None;
    }
    // `cursor_extent` is the live source extent: the executor's progress
    // thread re-reads `global_extent()` each tick and feeds it through
    // `PhaseProgressUpdate`, so a growing (`until_elapsed`) source's total
    // tracks here rather than pinning at the initial base. `elapsed` is
    // derived at the consumer.
    // Session-clock delta — same basis as the margin's session column,
    // so `session_started + elapsed == session_now` reconciles exactly.
    let elapsed = (session_now - phase.session_started).max(0.0);
    // Spinner cadence: the inline thread ticked once per 500 ms sleep, so
    // `tick ≈ elapsed * 2`. Matching it keeps the spinner frame stable
    // across the producer→consumer move (SRD-100 §12 A1).
    let tick = (elapsed * 2.0) as u64;
    let ctx = build_inline_refresh_context(
        &handle.metrics,
        &handle.activity_name,
        handle.concurrency,
        phase.cursor_extent,
        phase.rows_consumed,
        phase.rows_total,
        elapsed,
        tick,
        &handle.status_metrics,
        &handle.memo,
        handle.seq,
        handle.depth_indent.clone(),
        // Daemons are open-ended background pollers: no progress meter,
        // latency chip in its place.
        phase.daemon,
    );
    // Fire the resolved `on_update` bodies in declaration order with
    // `ContentMode::Value` — exactly what `DefaultBinder::fire` did for
    // the producer thread, minus the (now consumer-owned) binder state.
    let mut sink = StringSink::with_capacity(192);
    for body in handle.bodies.iter() {
        body.fire(&ctx, ContentMode::Value, &mut sink);
    }
    let rendered = sink.take();
    if rendered.trim().is_empty() {
        return None;
    }
    Some(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nbrs_metrics::labels::Labels;
    use nbrs_metrics::summaries::binomial_summary::BinomialSummary;
    use nbrs_metrics::summaries::ewma::Ewma;
    use nbrs_metrics::summaries::peak_tracker::PeakTracker;
    use nbrs_runtime::activity::ActivityMetrics;
    use nbrs_runtime::observer::PhaseRenderHandle;
    use nbrs_runtime::readouts::{BakedBody, RenderStep};

    use crate::state::{ActivePhaseId, RunState};

    /// A live phase whose `on_update` template is a single literal — the
    /// fold fires it and yields exactly `text`, so a test can assert the
    /// fold's *composition* (ordering, joining, clearing) without coupling
    /// to the `phase_status` readout's format.
    fn literal_phase(name: &str, seq: Option<usize>, text: Option<&str>) -> ActivePhase {
        let render = text.map(|t| PhaseRenderHandle {
            exec_id: 1,
            name: name.to_string(),
            labels: String::new(),
            activity_name: name.to_string(),
            metrics: Arc::new(ActivityMetrics::new(&Labels::empty())),
            bodies: Arc::new(vec![BakedBody::from_steps(vec![
                RenderStep::Literal(t.to_string()),
            ])]),
            memo: Arc::new(arc_swap::ArcSwap::from_pointee(String::new())),
            gutter: Arc::new(arc_swap::ArcSwapOption::empty()),
            status_metrics: Arc::from(Vec::<String>::new()),
            concurrency: 1,
            seq: seq.map(|s| (s, 2)),
            depth_indent: String::new(),
        });
        ActivePhase {
            name: name.to_string(),
            labels: String::new(),
            cursor_name: "c".into(),
            daemon: false,
            cursor_extent: 100,
            rows_consumed: 0,
            rows_total: 0,
            fibers: 1,
            started_at: Instant::now(),
            session_started: 0.0,
            ops_started: 0,
            ops_finished: 0,
            ops_ok: 0,
            skips: 0,
            errors: 0,
            retries: 0,
            ops_per_sec: 0.0,
            adapter_counters: Vec::new(),
            rows_per_batch: 0.0,
            relevancy: Vec::new(),
            throughput_summary: Arc::new(BinomialSummary::new(60)),
            rate_ewma: Arc::new(Ewma::new(Duration::from_secs(1))),
            latency_peak_5s: Arc::new(PeakTracker::max(Duration::from_secs(5))),
            latency_peak_10s: Arc::new(PeakTracker::max(Duration::from_secs(10))),
            render,
        }
    }

    fn state_with(phases: Vec<ActivePhase>) -> RunState {
        let mut s = RunState::new("w.yaml", "default", "stdout");
        for p in phases {
            s.active_phases
                .insert(ActivePhaseId::new(1, p.name.clone(), p.labels.clone()), p);
        }
        s
    }

    #[test]
    fn single_phase_renders_its_body() {
        let s = state_with(vec![literal_phase("run", Some(1), Some("ops=5 ok=5"))]);
        assert_eq!(render_active_status(&s).as_deref(), Some("ops=5 ok=5"));
    }

    #[test]
    fn two_phases_fold_in_seq_order_not_map_order() {
        // Insert B (seq 2) and A (seq 1); the fold must emit A before B
        // regardless of `HashMap` iteration order (SRD-100 §7a/§12).
        let s = state_with(vec![
            literal_phase("b", Some(2), Some("B-status")),
            literal_phase("a", Some(1), Some("A-status")),
        ]);
        assert_eq!(
            render_active_status(&s).as_deref(),
            Some("A-status\nB-status"),
        );
    }

    #[test]
    fn fold_is_deterministic_for_a_fixed_snapshot() {
        // Same snapshot → same bytes (the §12 multi-phase determinism
        // property — the literal body removes elapsed/tick variance).
        let s = state_with(vec![
            literal_phase("a", Some(1), Some("A")),
            literal_phase("b", Some(2), Some("B")),
        ]);
        assert_eq!(render_active_status(&s), render_active_status(&s));
    }

    #[test]
    fn phase_without_handle_contributes_nothing() {
        // A phase still in the attach window (render = None) is skipped; a
        // concurrent phase with a handle still renders (no peer-wipe).
        let s = state_with(vec![
            literal_phase("pending", Some(1), None),
            literal_phase("live", Some(2), Some("LIVE")),
        ]);
        assert_eq!(render_active_status(&s).as_deref(), Some("LIVE"));
    }

    #[test]
    fn no_renderable_phase_yields_none() {
        // Empties the footer exactly as the retired `status(None)` did.
        assert!(render_active_status(&state_with(vec![])).is_none());
        let s = state_with(vec![literal_phase("pending", Some(1), None)]);
        assert!(render_active_status(&s).is_none());
    }

    #[test]
    fn op_leaves_nest_under_their_active_phase() {
        // SRD-63: a phase's `readout: visible` op leaves render as indented
        // lines directly under its status line, in arrival order, with the
        // status icon, `[i/N]` count and compact times.
        use crate::state::{EntryKind, OpEntry, PhaseEntry, PhaseStatus};

        let mut s = state_with(vec![literal_phase("flush", Some(1), Some("flush-status"))]);
        let node_id = 1usize; // SceneNodeId is a usize; value only has to be consistent below
        s.phases.push(PhaseEntry {
            node_id,
            name: "flush".into(),
            labels: String::new(),
            status: PhaseStatus::Running,
            kind: EntryKind::Phase,
            op_count: 0,
            duration_secs: None,
            session_elapsed: None,
            session_started: Some(0.0),
            depth: 0,
            summary: None,
            op_names: Vec::new(),
            seq: Some(1),
        });
        s.phase_ops.insert(
            node_id,
            vec![
                OpEntry {
                    name: "encode".into(),
                    status: PhaseStatus::Completed,
                    started_at: Instant::now(),
            session_started: 0.0,
                    duration_secs: Some(1.5),
                    session_elapsed: Some(10.0),
                    seq: 0,
                },
                OpEntry {
                    name: "write".into(),
                    status: PhaseStatus::Running,
                    started_at: Instant::now(),
            session_started: 0.0,
                    duration_secs: None,
                    session_elapsed: None,
                    seq: 1,
                },
            ],
        );

        let out = render_active_status(&s).expect("some status");
        let mut lines = out.lines();
        assert_eq!(lines.next(), Some("flush-status")); // phase line leads
        let l1 = lines.next().expect("op leaf 1");
        assert!(l1.contains("✓ encode") && l1.contains("[1/2]"), "got: {l1}");
        let l2 = lines.next().expect("op leaf 2");
        assert!(l2.contains("write") && l2.contains("[2/2]"), "got: {l2}");
        assert_eq!(lines.next(), None); // nothing extra
    }

    #[test]
    fn phase_without_op_leaves_is_unchanged() {
        // The interleave must not alter output for phases with no opted-in ops.
        let s = state_with(vec![literal_phase("run", Some(1), Some("ops=5 ok=5"))]);
        assert_eq!(render_active_status(&s).as_deref(), Some("ops=5 ok=5"));
    }
}
