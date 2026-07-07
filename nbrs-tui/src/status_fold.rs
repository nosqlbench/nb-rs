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

use crate::state::{ActivePhase, RunState};

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
    let rendered: Vec<String> = active_phases_ordered(snap)
        .iter()
        .filter_map(|p| render_phase_status(p))
        .collect();
    if rendered.is_empty() {
        None
    } else {
        Some(rendered.join("\n"))
    }
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
pub fn render_phase_status(phase: &ActivePhase) -> Option<String> {
    let handle = phase.render.as_ref()?;
    if handle.bodies.is_empty() {
        return None;
    }
    // `cursor_extent` is the live source extent: the executor's progress
    // thread re-reads `global_extent()` each tick and feeds it through
    // `PhaseProgressUpdate`, so a growing (`until_elapsed`) source's total
    // tracks here rather than pinning at the initial base. `elapsed` is
    // derived at the consumer.
    let elapsed = phase.started_at.elapsed().as_secs_f64();
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
            status_metrics: Arc::from(Vec::<String>::new()),
            concurrency: 1,
            seq: seq.map(|s| (s, 2)),
            depth_indent: String::new(),
        });
        ActivePhase {
            name: name.to_string(),
            labels: String::new(),
            cursor_name: "c".into(),
            cursor_extent: 100,
            rows_consumed: 0,
            rows_total: 0,
            fibers: 1,
            started_at: Instant::now(),
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
}
