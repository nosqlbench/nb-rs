// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`ActivityReadoutContext`] — concrete `ReadoutContext`
//! impl built from the activity-side data the
//! ✓ DONE block already gathers.
//!
//! Push 1 surface only. Built up by `nbrs-activity::activity`
//! at end-of-activity right before invoking the `phase_done`
//! readout. Each later push grows this struct as new
//! built-ins (and new `ReadoutContext` methods) arrive.
//!
//! Owned, not borrowed: every field is computed at the call
//! site (counter snapshots, the rendered chip string, the
//! depth-indent string) and parked here for the readout's
//! duration. This keeps the readout call free of borrow
//! plumbing through the activity's locks.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::readouts::{Event, LifecycleState, ReadoutContext};

/// Snapshot of everything `phase_done` needs to render at
/// `Lod::Labeled / ContentMode::Value`. Constructed at
/// end-of-activity in `nbrs-activity::activity`; thrown
/// away after the render returns.
pub struct ActivityReadoutContext {
    pub phase_name: String,
    pub phase_seq: Option<(usize, usize)>,
    pub phase_labels: String,
    pub cycles_completed: u64,
    pub cycles_total: u64,
    pub ops_ok: u64,
    pub errors: u64,
    pub retries: u64,
    pub concurrency: usize,
    pub elapsed_secs: f64,
    pub consumed: u64,
    pub status_metric_chips: String,
    pub depth_indent: String,
    pub use_color: bool,
    /// Snapshot of the activity's memo at context-build time.
    /// Empty when no `memo:` wrapper is active on any op.
    pub memo: String,
}

impl ReadoutContext for ActivityReadoutContext {
    fn subject_name(&self) -> &str { &self.phase_name }
    fn subject_seq(&self) -> Option<(usize, usize)> { self.phase_seq }
    fn subject_labels(&self) -> &str { &self.phase_labels }
    fn cycles_completed(&self) -> u64 { self.cycles_completed }
    fn cycles_total(&self) -> u64 { self.cycles_total }
    fn ops_ok(&self) -> u64 { self.ops_ok }
    fn errors(&self) -> u64 { self.errors }
    fn retries(&self) -> u64 { self.retries }
    fn concurrency(&self) -> usize { self.concurrency }
    fn elapsed_secs(&self) -> f64 { self.elapsed_secs }
    fn consumed(&self) -> u64 { self.consumed }
    fn status_metric_chips(&self) -> String { self.status_metric_chips.clone() }
    fn depth_indent(&self) -> &str { &self.depth_indent }
    fn use_color(&self) -> bool { self.use_color }
    fn event(&self) -> Event { Event::PhaseEnd }
    fn subject_state(&self) -> LifecycleState { LifecycleState::Completed }
    fn phase_memo(&self) -> &str { &self.memo }
}

/// Per-event context for lifecycle fires (Push 9a):
/// `on_session_start` / `on_session_end`,
/// `on_phase_start`, `on_each_start` / `on_each_end`,
/// `on_scope_start` / `on_scope_end`.
///
/// Carries just the fields a structural readout
/// (`scope_header`, `session_banner`, `each_close`, …)
/// needs — subject name, root-first labels, depth indent,
/// colour flag, plus the firing event so a wildcard-bound
/// readout can branch.
///
/// Counter-shaped methods all return zero / empty since
/// lifecycle readouts don't depend on per-cycle progress;
/// the `Default` impl on the trait handles those.
pub struct LifecycleContext {
    pub event: crate::readouts::Event,
    pub subject_name: String,
    pub subject_labels: String,
    pub depth_indent: String,
    pub use_color: bool,
}

impl ReadoutContext for LifecycleContext {
    fn subject_name(&self) -> &str { &self.subject_name }
    fn subject_seq(&self) -> Option<(usize, usize)> { None }
    fn subject_labels(&self) -> &str { &self.subject_labels }
    fn cycles_completed(&self) -> u64 { 0 }
    fn cycles_total(&self) -> u64 { 0 }
    fn ops_ok(&self) -> u64 { 0 }
    fn errors(&self) -> u64 { 0 }
    fn retries(&self) -> u64 { 0 }
    fn concurrency(&self) -> usize { 0 }
    fn elapsed_secs(&self) -> f64 { 0.0 }
    fn consumed(&self) -> u64 { 0 }
    fn status_metric_chips(&self) -> String { String::new() }
    fn depth_indent(&self) -> &str { &self.depth_indent }
    fn use_color(&self) -> bool { self.use_color }
    fn event(&self) -> crate::readouts::Event { self.event }
    fn subject_state(&self) -> LifecycleState {
        // Lifecycle events fire at the boundary; the
        // subject is in transition. `Running` is the safe
        // default for `on_*_start` (the subject is now
        // in flight); `_end` events technically transition
        // to `Completed` but the readouts that fire here
        // (scope_header, session_banner, etc.) don't
        // branch on subject_state anyway, so a single
        // default keeps things simple.
        LifecycleState::Running
    }
}

/// Per-tick context for the inline-status refresh thread
/// (Push 2). Identifies as [`Event::Update`]; carries a
/// monotonic refresh tick for spinner cycling, the full
/// activity name with leaf coord, and pre-formatted
/// adapter / batch tails (the iteration over registered
/// dispensers stays in the surface for now — Push 4
/// migrates the trait to expose the typed iterator).
pub struct InlineRefreshContext {
    pub phase_name: String,
    pub activity_name: String,
    pub phase_seq: Option<(usize, usize)>,
    pub phase_labels: String,
    pub cycles_completed: u64,
    pub cycles_total: u64,
    pub ops_started: u64,
    pub ops_finished: u64,
    pub ops_ok: u64,
    pub errors: u64,
    pub retries: u64,
    pub concurrency: usize,
    pub elapsed_secs: f64,
    pub consumed: u64,
    pub status_metric_chips: String,
    pub adapter_counters_text: String,
    pub batch_info_text: String,
    pub depth_indent: String,
    pub refresh_tick: u64,
    pub use_color: bool,
    /// Snapshot of the activity's memo at tick build time.
    /// Empty when no `memo:` wrapper has published anything.
    pub memo: String,
}

impl ReadoutContext for InlineRefreshContext {
    fn subject_name(&self) -> &str { &self.phase_name }
    fn activity_name(&self) -> &str { &self.activity_name }
    fn subject_seq(&self) -> Option<(usize, usize)> { self.phase_seq }
    fn subject_labels(&self) -> &str { &self.phase_labels }
    fn cycles_completed(&self) -> u64 { self.cycles_completed }
    fn cycles_total(&self) -> u64 { self.cycles_total }
    fn ops_started(&self) -> u64 { self.ops_started }
    fn ops_finished(&self) -> u64 { self.ops_finished }
    fn ops_ok(&self) -> u64 { self.ops_ok }
    fn errors(&self) -> u64 { self.errors }
    fn retries(&self) -> u64 { self.retries }
    fn concurrency(&self) -> usize { self.concurrency }
    fn elapsed_secs(&self) -> f64 { self.elapsed_secs }
    fn consumed(&self) -> u64 { self.consumed }
    fn status_metric_chips(&self) -> String { self.status_metric_chips.clone() }
    fn adapter_counters_text(&self) -> String { self.adapter_counters_text.clone() }
    fn batch_info_text(&self) -> String { self.batch_info_text.clone() }
    fn depth_indent(&self) -> &str { &self.depth_indent }
    fn use_color(&self) -> bool { self.use_color }
    fn event(&self) -> Event { Event::Update }
    fn refresh_tick(&self) -> u64 { self.refresh_tick }
    fn phase_memo(&self) -> &str { &self.memo }
    /// SRD-63 Push 9f: derive ETA from `cycles_total -
    /// ops_finished` divided by the observed throughput
    /// rate (`ops_finished / elapsed`). `None` when the
    /// extent isn't known (sourceless phase running by
    /// time / open-ended) or no progress has been made
    /// yet (rate would divide-by-zero).
    fn eta_secs(&self) -> Option<f64> {
        if self.cycles_total == 0 || self.elapsed_secs <= 0.0 {
            return None;
        }
        let rate = self.ops_finished as f64 / self.elapsed_secs;
        if rate <= 0.0 { return None; }
        let remaining = self.cycles_total.saturating_sub(self.ops_finished) as f64;
        Some(remaining / rate)
    }
}

/// One-shot lifecycle fire helper. Builds a binder for
/// `event` against `bindings`, runs every bound body
/// against `ctx`, writes the rendered text via
/// `crate::diag!` (so it lands in stderr / log file
/// uniformly), and captures to the snapshot store via
/// `subject_kind` / `subject_id`.
///
/// Best-effort: errors building the binder log a warning
/// and the fire is skipped — a malformed `readouts:`
/// binding never blocks the run. Bindings that resolve
/// to zero bodies (the usual case for structural slots
/// with no built-in default and no workload binding)
/// produce no output.
pub fn fire_lifecycle(
    event: crate::readouts::Event,
    bindings: &nbrs_workload::model::ReadoutsBindings,
    default: Option<crate::readouts::BakedBody>,
    ctx: &dyn crate::readouts::ReadoutContext,
    snapshot_writer: Option<&crate::readouts::snapshot::SnapshotWriter>,
) {
    use crate::readouts::ReadoutBinder;

    // Use the built-in default when supplied (currently
    // only PhaseEnd/Update have defaults); otherwise the
    // slot starts empty and falls through to whatever the
    // workload bound. `build_event_binder` always seeds
    // the default — pass an empty body when none exists
    // so unbound slots stay quiet.
    let seed = default.unwrap_or_else(crate::readouts::BakedBody::new);
    let mut binder = match crate::readouts::build_event_binder(bindings, event, seed) {
        Ok(b) => b,
        Err(e) => {
            crate::diag!(crate::observer::LogLevel::Warn,
                "readouts: failed to bind {slot} — {e}",
                slot = event.slot_name());
            return;
        }
    };
    let mut sink = crate::readouts::StringSink::with_capacity(128);
    binder.fire(event, ctx, &mut sink);
    let rendered = sink.take();
    if rendered.trim().is_empty() {
        return; // no bound body for this slot — quiet exit
    }
    crate::diag!(crate::observer::LogLevel::Info, "{}", rendered);

    // Snapshot capture per Push 6. Subject identity comes
    // straight from the context: `subject_kind` from the
    // firing event (the sole source of truth for which
    // table dimension this row belongs to), `subject_id`
    // from `ctx.subject_id()`'s default `name@labels`
    // shape (overridden for session-scope contexts that
    // collapse to a literal `"session"`). Replay reads
    // stable tuples (slot, subject_kind, subject_id, ...).
    let subject_id = ctx.subject_id();
    crate::readouts::snapshot::capture(
        snapshot_writer,
        event.slot_name(),
        event.subject_kind().as_str(),
        &subject_id,
        "binder",
        crate::readouts::snapshot::lod_str(crate::readouts::Lod::Labeled),
        &rendered,
    );
}

/// Build an [`InlineRefreshContext`] from the per-tick
/// counter snapshots the inline-status thread takes. This
/// preserves the byte-equivalence target by constructing
/// the same intermediate values the prior `format!()`
/// inlined (adapter counter chips, batch info, the
/// scene-tree-walk for `seq` + depth indent) — they're
/// each derived once per tick, then handed to the
/// [`crate::readouts::builtins::phase_status::PhaseStatus`]
/// readout for actual rendering.
pub fn build_inline_refresh_context(
    progress_metrics: &Arc<crate::activity::ActivityMetrics>,
    activity_name: &str,
    concurrency: usize,
    total_extent: u64,
    elapsed_secs: f64,
    refresh_tick: u64,
    status_metrics: &[String],
    memo: &arc_swap::ArcSwap<String>,
) -> InlineRefreshContext {
    // Counter snapshots — must match the prior inline-status
    // formulas so byte equivalence holds.
    let started = progress_metrics.ops_started.load(Ordering::Relaxed);
    let finished = progress_metrics.ops_finished.load(Ordering::Relaxed);
    let ops_completed = progress_metrics.cycles_completed();
    let successes = progress_metrics.successes_total.get();
    let errors = progress_metrics.errors_total.get();
    let failed_ops = ops_completed
        .saturating_sub(successes)
        .saturating_sub(progress_metrics.skips_total.get());
    let retries = errors.saturating_sub(failed_ops);
    let consumed = finished;

    // Adapter-status chips: ` <name>:<rate>/s` per registered
    // dispenser counter. `collect_status_counters` aggregates
    // every dispenser's typed counters into a flat
    // `(name, total)` list — same data the inline thread used
    // to read directly from `progress_metrics.dispensers`,
    // exposed through the public accessor.
    let mut adapter_counters_text = String::new();
    let counters = progress_metrics.collect_status_counters();
    for (name, total) in &counters {
        let item_rate = if elapsed_secs > 0.0 {
            *total as f64 / elapsed_secs
        } else { 0.0 };
        let rate_str = if item_rate >= 1_000_000.0 {
            format!("{:.1}M", item_rate / 1_000_000.0)
        } else if item_rate >= 1_000.0 {
            format!("{:.1}K", item_rate / 1_000.0)
        } else {
            format!("{:.0}", item_rate)
        };
        adapter_counters_text.push_str(&format!(" {name}:{rate_str}/s"));
    }

    // Batch info: only fires when `rows_inserted > stanzas`
    // (a batched write was observed). Same formula as before.
    let stanzas = progress_metrics.stanzas_total.get();
    let mut batch_info_text = String::new();
    if stanzas > 0 {
        for (name, total) in &counters {
            if name == "rows_inserted" && *total > stanzas {
                let avg = *total as f64 / stanzas as f64;
                batch_info_text = format!(" rows/batch:{avg:.1}");
            }
        }
    }

    // Pre-rendered status-metric chip string.
    let status_metric_chips = progress_metrics
        .collect_status_values(status_metrics)
        .concat();

    // Pre-map sequence + depth — single scene-tree walk.
    // Activity name carries the leaf coord; the scene-tree
    // node was registered under the bare phase name.
    let bare_name = activity_name
        .split_once(" (")
        .map(|(n, _)| n)
        .unwrap_or(activity_name);
    let (phase_seq, depth_indent) = crate::scene_tree::current()
        .and_then(|t| {
            let node = t.dfs_phases()
                .find(|n| n.name == bare_name
                    && matches!(n.status,
                        crate::scene_tree::PhaseStatus::Running))?
                .clone();
            let seq = node.seq?;
            let depth = node.depth.saturating_sub(1);
            Some((
                Some((seq, t.total_phases())),
                "  ".repeat(depth),
            ))
        })
        .unwrap_or((None, String::new()));

    let memo_snapshot: String = memo.load().as_str().to_string();
    InlineRefreshContext {
        phase_name: bare_name.to_string(),
        activity_name: activity_name.to_string(),
        phase_seq,
        phase_labels: String::new(),
        cycles_completed: ops_completed,
        cycles_total: total_extent,
        ops_started: started,
        ops_finished: finished,
        ops_ok: successes,
        errors,
        retries,
        concurrency,
        elapsed_secs,
        consumed,
        status_metric_chips,
        adapter_counters_text,
        batch_info_text,
        depth_indent,
        refresh_tick,
        use_color: crate::observer::use_color(),
        memo: memo_snapshot,
    }
}
