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

/// Per-line CONTEXTUAL GUTTER content for the footer's left margin
/// (SRD-92). Each block renders header-first: the header row carries
/// the node's own timing triad, and every detail row under it owns
/// one gutter cell (possibly blank) — cells stack vertically under
/// the header, single-placement intact.
#[derive(Clone, Debug, PartialEq)]
pub enum RowGutter {
    /// Plain blank-aligned divider.
    Blank,
    /// Node HEADER row (SRD-92 R1): the pre-rendered, uncolored
    /// margin body (`session · [n/N] · node-time` triad) for this
    /// node — the sink wraps it in the color + divider dressing.
    Header(String),
    /// Key-metric detail row (SRD-92 R4): the metric macro's live
    /// view — a bright TREND sparkline of the phase's primary key
    /// metric, labeled by metric name. The current numeric lives
    /// only in the row body's chips (single placement); the cell
    /// carries what the body can't — the history. `key` persists
    /// the sink-side sample ring across ticks like `Spark`.
    Metric { key: String, name: String, value: f64 },
    /// Metered phase: completion-bar fill fraction.
    Bar(f64),
    /// Open-ended phase (daemon poller): latency trend. `key`
    /// identifies the phase so the sink's sample ring persists
    /// across ticks; p50/p99 are the current service-time
    /// percentiles in nanos.
    Latency { key: String, p50: u64, p99: u64, count: u64 },
    /// Workload-declared layout text (`gutter: "<template>"`),
    /// placed in the cell verbatim (truncated to fit).
    Text(String),
    /// Key metric: label at the cell's left edge, value against the divider.
    Labeled { name: String, value: String },
    /// Workload-declared layout text COMPOSED with the phase's own
    /// completion fraction (SRD-92 R3): the cell shows the auto
    /// bar and the workload text side by side, so a custom cell
    /// (e.g. a measured units/s readout) doesn't cost the operator
    /// the progress indicator. Built by the fold whenever a Text
    /// spec lands on a phase that has a completion fraction.
    BarText { frac: f64, text: String },
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
    /// `count` is the timer's LIFETIME sample count — the sink
    /// re-renders (and pushes a trend sample) only when it advanced
    /// since the last draw, so a slow poller (1 op/s) keeps a stable
    /// cell between ops instead of restating the same value every
    /// redraw tick.
    LatencyHist { key: String, p50: u64, count: u64 },
}

/// Row roles within one rendered block (SRD-92): the header leads,
/// details follow. Classification is positional per the contract —
/// renderers compose header-first — with memo rows recognized by
/// their `[[` banner form at any position.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RowRole {
    Header,
    Memo,
    /// The standard counters detail line (rate/ok/att/e/r/c/…).
    Standard,
    /// Key-metric detail rows (status-metric / adapter chips).
    KeyMetrics,
}

/// Strip SGR escape sequences (`ESC [ … <alpha>`) so role matching
/// sees the text an operator sees. The old classifier tested the
/// RAW bytes and a color-styled memo banner defeated it, landing
/// the standard-detail gutter cell on the header row.
pub fn strip_ansi(s: &str) -> String {
    let mut plain = String::with_capacity(s.len());
    let mut esc = false;
    for c in s.chars() {
        if esc {
            if c.is_ascii_alphabetic() { esc = false; }
            continue;
        }
        if c == '\u{1b}' { esc = true; continue; }
        plain.push(c);
    }
    plain
}

/// Classify each row of a rendered block per SRD-92: memo rows are
/// `[[ … ]]` banners (ANSI-stripped match), the first non-memo row
/// is the header, the next non-memo row the standard detail line,
/// and remaining non-memo rows are key-metric details.
pub fn classify_block(rows: &[&str]) -> Vec<RowRole> {
    let mut roles = Vec::with_capacity(rows.len());
    let mut seen_header = false;
    let mut seen_standard = false;
    for r in rows {
        let plain = strip_ansi(r);
        if plain.trim_start().starts_with("[[") {
            roles.push(RowRole::Memo);
        } else if !seen_header {
            seen_header = true;
            roles.push(RowRole::Header);
        } else if !seen_standard {
            seen_standard = true;
            roles.push(RowRole::Standard);
        } else {
            roles.push(RowRole::KeyMetrics);
        }
    }
    roles
}

/// As [`render_active_status`], additionally returning one
/// [`RowGutter`] per rendered line (indices align with the '\n'
/// split of the returned text).
pub fn render_active_status_with_gutters(
    snap: &RunState,
) -> Option<(String, Vec<RowGutter>)> {
    let mut lines: Vec<String> = Vec::new();
    let mut gutters: Vec<RowGutter> = Vec::new();
    let session_now = snap.elapsed_secs();
    for p in active_phases_ordered(snap) {
        if let Some((status, chips)) = render_phase_status_parts(p, session_now) {
            let block: Vec<&str> = status.split('\n').collect();
            let roles = classify_block(&block);
            let ctx_gutter = phase_context_gutter(p);
            let header_gutter = phase_header_gutter(p, session_now);
            let metric_cell = phase_metric_gutter(p, &chips);
            for (i, line) in block.iter().enumerate() {
                lines.push((*line).to_string());
                gutters.push(match roles[i] {
                    RowRole::Header => header_gutter.clone(),
                    RowRole::Memo => RowGutter::Blank,
                    RowRole::Standard => ctx_gutter.clone(),
                    RowRole::KeyMetrics => metric_cell.clone(),
                });
            }
            for (leaf, cell) in render_op_leaves(snap, p) {
                lines.push(leaf);
                gutters.push(cell);
            }
        }
    }
    if lines.is_empty() {
        None
    } else {
        Some((lines.join("\n"), gutters))
    }
}

/// SRD-92 R1 — the header row's margin: this node's OWN timing triad
/// (`session · [n/N] · phase-time`), same body format the actor
/// stamps on scrollback headers, so every concurrently-visible
/// phase's header carries its own [n/N] and clock rather than only
/// footer row 0 wearing the workload-level stamp.
fn phase_header_gutter(p: &ActivePhase, session_now: f64) -> RowGutter {
    let Some((s, n)) = p.render.as_ref().and_then(|h| h.seq) else {
        return RowGutter::Blank;
    };
    let elapsed = (session_now - p.session_started).max(0.0);
    RowGutter::Header(crate::widgets::margin_body(
        n, &format!("[{s}/{n}]"), Some(elapsed), Some(session_now)))
}

/// SRD-92 R4 — the key-metric detail row's default gutter cell: the
/// metric macro, a bright trend of the phase's PRIMARY key metric
/// (first `status_metrics:` match, numeric form). Blank when the
/// phase publishes no chips or no primary numeric is measurable —
/// the row body still shows the chips text either way.
fn phase_metric_gutter(p: &ActivePhase, chips: &str) -> RowGutter {
    if strip_ansi(chips).trim().is_empty() {
        return RowGutter::Blank;
    }
    let Some(handle) = p.render.as_ref() else { return RowGutter::Blank };
    match handle.metrics.collect_status_primary(&handle.status_metrics) {
        Some((name, value)) => RowGutter::Metric {
            key: format!("metric:{}@{}", p.name, p.labels),
            name,
            value,
        },
        None => RowGutter::Blank,
    }
}

/// The contextual gutter payload for one phase: latency trend for
/// open-ended daemons, completion bar for metered phases (fraction
/// basis mirrors `ReadoutContext::progress_fraction`: override →
/// rows → cycles), Blank when nothing meaningful exists.
fn phase_context_gutter(p: &ActivePhase) -> RowGutter {
    let Some(handle) = p.render.as_ref() else { return RowGutter::Blank };
    // A workload-declared `gutter:` wrapper spec overrides the
    // automatic derivation — the phase owns its cell. A TEXT spec on
    // a metered phase COMPOSES with the completion fraction instead
    // of replacing it (SRD-92 R3): custom readout and progress bar
    // share the cell.
    if let Some(spec) = handle.gutter.load_full() {
        use nbrs_runtime::wrappers::gutter::GutterSpec;
        return match spec.as_ref() {
            GutterSpec::Labeled { name, value } =>
                RowGutter::Labeled { name: name.clone(), value: value.clone() },
            GutterSpec::Text(s) => match (!p.daemon).then(|| metered_fraction(p, handle)).flatten() {
                Some(f) => RowGutter::BarText { frac: f, text: s.clone() },
                None => RowGutter::Text(s.clone()),
            },
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
            count: handle.metrics.service_time.count(),
        };
    }
    match metered_fraction(p, handle) {
        Some(f) => RowGutter::Bar(f),
        None => RowGutter::Blank,
    }
}

/// The phase's completion fraction on the single agreed basis
/// (override → rows → cycles), `None` when nothing meters it.
fn metered_fraction(
    p: &ActivePhase,
    handle: &nbrs_runtime::observer::PhaseRenderHandle,
) -> Option<f64> {
    handle.metrics.progress_override()
        .or_else(|| (p.rows_total > 0).then(|| {
            (p.rows_consumed as f64 / p.rows_total as f64).clamp(0.0, 1.0)
        }))
        .or_else(|| (p.cursor_extent > 0).then(|| {
            (handle.metrics.cycles_completed() as f64 / p.cursor_extent as f64)
                .clamp(0.0, 1.0)
        }))
}

/// SRD-63 / SRD-92 — render an active phase's op-level status leaves (ops
/// that declared `readout: visible`) as footer lines nested under the
/// phase: status icon · name · `[i/N]` (+ `@ <session>` stamp on terminal
/// rows). Each leaf row's GUTTER CELL carries the node's own execution
/// time — cumulative while running, final once terminal — the default
/// cell for a visible node with no declared gutter. Single placement:
/// duration in the cell, session stamp in the body.
///
/// The leaves live in `phase_ops` keyed by the phase's scene-node id, which the
/// `ActivePhase` doesn't carry; we resolve it from the running tree row with a
/// matching `(name, labels)`. Returns empty (no allocation of leaf lines) when
/// the phase has no row yet or no opted-in ops.
fn render_op_leaves(snap: &RunState, phase: &ActivePhase) -> Vec<(String, RowGutter)> {
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
            // Duration is the leaf's GUTTER CELL (cumulative while
            // running, final once terminal); terminal rows keep their
            // session finish-stamp in the body — its only home.
            let cell = match leaf {
                Some(v) => RowGutter::Text(format_dur_compact(v)),
                None => RowGutter::Blank,
            };
            let stamp = match sess {
                Some(v) => format!("  @ {}", format_dur_compact(v)),
                None => String::new(),
            };
            let mut line = format!(
                "    {icon} {name}  [{seq}/{total}]{stamp}",
                name = op.name,
                seq = op.seq + 1,
            );
            if let PhaseStatus::Failed(err) = &op.status {
                line.push_str("  ");
                line.push_str(err);
            }
            (line, cell)
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
    render_phase_status_parts(phase, session_now).map(|(s, _)| s)
}

/// As [`render_phase_status`], additionally returning the phase's
/// key-metric chips text (adapter + batch + status-metric chips, as
/// the context composes them) so the fold can derive the key-metric
/// row's default gutter cell (SRD-92 R4) from the same context build.
pub fn render_phase_status_parts(
    phase: &ActivePhase,
    session_now: f64,
) -> Option<(String, String)> {
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
    use nbrs_runtime::readouts::context::ReadoutContext as _;
    let chips = format!(
        "{}{}{}",
        ctx.adapter_counters_text(),
        ctx.batch_info_text(),
        ctx.status_metric_chips(),
    );
    Some((rendered, chips))
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

    #[test]
    fn classify_block_is_ansi_immune() {
        // SRD-92: role classification strips SGR before matching. A
        // color-styled memo banner (`\x1b[1;33m[[ … ]]`) used to defeat
        // the raw `starts_with("[[")` test, sliding the standard-detail
        // gutter cell onto the header row (bar-beside-header bug).
        let rows = [
            "  \u{1b}[1m\u{1b}[34mfinalize_index\u{1b}[0m 64%",
            "    \u{1b}[1;33m[[ finalize step 3/4 ]]\u{1b}[0m",
            "      0/s ok:100% e:0 r:0 c:1",
            "      rows/s=12.5K recall:97.84%",
        ];
        assert_eq!(
            classify_block(&rows),
            vec![
                RowRole::Header,
                RowRole::Memo,
                RowRole::Standard,
                RowRole::KeyMetrics,
            ],
        );
    }

    #[test]
    fn classify_block_memo_anywhere_never_shifts_roles() {
        // Memo rows are recognized positionally-independently; the
        // header is always the first NON-memo row and the standard
        // detail the next, wherever the memo lands.
        let rows = ["[[ memo ]]", "head", "stats"];
        assert_eq!(
            classify_block(&rows),
            vec![RowRole::Memo, RowRole::Header, RowRole::Standard],
        );
    }

    #[test]
    fn text_gutter_on_metered_phase_composes_with_bar() {
        // SRD-92 R3: a workload Text cell on a phase that has a
        // completion fraction keeps the progress indicator — the fold
        // emits the composed BarText, not a bare Text that would cost
        // the operator the bar.
        let p = literal_phase("run", Some(1), Some("body"));
        let handle = p.render.as_ref().unwrap();
        handle.gutter.store(Some(std::sync::Arc::new(
            nbrs_runtime::wrappers::gutter::GutterSpec::Text("≈42 units/s".into()))));
        match phase_context_gutter(&p) {
            RowGutter::BarText { frac, text } => {
                assert_eq!(text, "≈42 units/s");
                assert!((0.0..=1.0).contains(&frac));
            }
            other => panic!("metered phase + text spec must compose: {other:?}"),
        }

        // A daemon (no fraction) keeps the bare text cell.
        let mut d = literal_phase("watch", Some(2), Some("body"));
        d.daemon = true;
        d.render.as_ref().unwrap().gutter.store(Some(std::sync::Arc::new(
            nbrs_runtime::wrappers::gutter::GutterSpec::Text("t".into()))));
        assert!(matches!(phase_context_gutter(&d), RowGutter::Text(_)),
            "open-ended phase has no fraction to compose");
    }

    #[test]
    fn header_rows_carry_their_own_triad_gutter() {
        // SRD-92 R1: every phase's header row gets a Header gutter
        // carrying that node's OWN margin body ([n/N] from its seq) —
        // not just footer row 0.
        let s = state_with(vec![
            literal_phase("a", Some(1), Some("A-status")),
            literal_phase("b", Some(2), Some("B-status")),
        ]);
        let (_, gutters) = render_active_status_with_gutters(&s).expect("some");
        match (&gutters[0], &gutters[1]) {
            (RowGutter::Header(a), RowGutter::Header(b)) => {
                assert!(a.contains("[1/2]"), "phase a triad: {a:?}");
                assert!(b.contains("[2/2]"), "phase b triad: {b:?}");
            }
            other => panic!("both header rows must carry Header gutters: {other:?}"),
        }
    }
}
