// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`ReadoutContext`] — the data facade every readout draws
//! from. See SRD-63 §2.
//!
//! Subject-kind validation is keyed off the firing
//! [`Event`](super::Event) (via
//! [`Event::subject_kind`](super::Event::subject_kind)) —
//! one source of truth, no parallel `ctx.subject_kind()`
//! that could drift. Builtins declare which kinds they
//! accept via [`Readout::accepts`](super::Readout::accepts);
//! the binder rejects mis-matches at bake-time so a
//! workload mistakenly binding `phase_status` to
//! `on_session_end` fails loudly rather than rendering
//! silent zeros.
//!
//! The trait is one flat surface (rather than four
//! per-kind traits) because every renderer takes
//! `&dyn ReadoutContext` and runtime-downcasting between
//! traits is hostile to call sites. Accessors that don't
//! apply to every kind have defaults that return zero /
//! empty so a context impl only fills the slots its kind
//! actually owns.

use super::Event;

/// What kind of subject a context (and a render) is
/// scoped to. Determined by the firing event and the
/// surface that built the context. Builtins declare
/// which kinds they accept; the binder validates at
/// bake-time.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum SubjectKind {
    /// The whole run. Used for `on_session_start` /
    /// `on_session_end`.
    Session,
    /// A single phase activity. Used for `on_phase_start` /
    /// `on_phase_end` / `on_update`.
    Phase,
    /// One iteration of a `for_each` / `for_combinations`
    /// scope. Used for `on_each_start` / `on_each_end`.
    Iteration,
    /// A non-iteration scope group (`do_while` /
    /// `do_until`). Used for `on_scope_start` /
    /// `on_scope_end`.
    Scope,
}

impl SubjectKind {
    /// Lower-snake-case name for the storage / replay
    /// surface. Stored in the `readout_snapshots.subject_kind`
    /// column so `nbrs replay` can group rows by subject.
    pub fn as_str(self) -> &'static str {
        match self {
            SubjectKind::Session   => "session",
            SubjectKind::Phase     => "phase",
            SubjectKind::Iteration => "iteration",
            SubjectKind::Scope     => "scope",
        }
    }
}

/// Lifecycle state of the subject (phase / iteration /
/// scope / session) the readout is rendering for. See
/// SRD-63 §2.
///
/// Used by readouts that branch on terminal status —
/// `phase_summary` picks `[ok]` / `[!!]` / `[..]` / `[  ]`
/// based on this; the post-run observer's tree walk routes
/// these into the per-row marker.
///
/// Deliberately no `Default` impl: callers must pick a
/// state explicitly. Defaulting to `Running` would have
/// lifecycle-end fires silently claim "still in flight."
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleState {
    /// Not yet started (a subject that the scenario tree
    /// declared but the executor hasn't reached).
    Pending,
    /// Currently executing.
    Running,
    /// Completed cleanly.
    Completed,
    /// Failed with the given error string.
    Failed(String),
}

/// The data facade a [`Readout`](super::Readout) reads from.
/// A single implementation per surface (terminal observer,
/// TUI, post-run summary, …) covers every readout in the
/// registry; per-event contexts (the `SessionSummaryContext`
/// in nbrs-tui, the per-phase contexts in
/// `crate::readout_context`) populate the slots that apply
/// to their [`SubjectKind`].
///
/// Method additions over the pushes have default impls
/// where reasonable so existing context impls don't have
/// to grow on every push. Methods that don't yet exist on
/// the trait can't be referenced by readouts, so the
/// contract stays in sync with what's actually
/// implemented — there are no panic stubs to forget about.
pub trait ReadoutContext {
    // ── Subject identity ──────────────────────────────────

    /// Bare subject name. Phase: `setup` / `run` /
    /// `ann_query`. Iteration / Scope: the scope keyword
    /// (`for_each`, `do_while`). Session: the scenario name
    /// (or empty).
    fn subject_name(&self) -> &str;

    /// Pre-map sequence number `(idx, total)` matching the
    /// TUI tree row and post-run summary numbering. `None`
    /// when no scene tree is available (inline-CLI form,
    /// pre-map didn't run) or the kind doesn't carry a seq.
    fn subject_seq(&self) -> Option<(usize, usize)> { None }

    /// Root-first display form of the subject's scope
    /// coords, already produced by
    /// `nbrs_variates::kernel::format_scope_coordinate_path`
    /// applied to the reversed
    /// `parent_kernel.scope_coordinates()`. Empty for root-
    /// scope subjects.
    fn subject_labels(&self) -> &str { "" }

    /// Stable identifier used as part of the snapshot
    /// primary key. Default: `subject_name` when no labels,
    /// `name@labels` otherwise. Surfaces that need a
    /// different shape (e.g. session uses the literal
    /// `"session"`) override this method.
    fn subject_id(&self) -> String {
        let name = self.subject_name();
        let labels = self.subject_labels();
        if labels.is_empty() {
            name.to_string()
        } else {
            format!("{name}@{labels}")
        }
    }

    /// Full activity name including the leaf coord, matching
    /// the value the inline-progress thread prints today
    /// (e.g. `run (profile=alpha, bucket=1, kind=READ)`).
    /// Defaults to [`subject_name`](Self::subject_name) —
    /// override gives the inline form what it expected.
    fn activity_name(&self) -> &str { self.subject_name() }

    // ── Lifecycle / counters (Phase) ──────────────────────

    /// Cycles completed in this phase (from
    /// `ActivityMetrics::cycles_completed`). Default 0 —
    /// non-Phase contexts return 0.
    fn cycles_completed(&self) -> u64 { 0 }

    /// Total extent the phase planned to consume — either
    /// the source-driven extent or the configured
    /// `cycles=N`. Used for `pct` denominator. Default 0.
    fn cycles_total(&self) -> u64 { 0 }

    /// Cumulative success count (counter, not delta).
    /// Default 0.
    fn ops_ok(&self) -> u64 { 0 }

    /// Cumulative error count (includes retries). Default 0.
    fn errors(&self) -> u64 { 0 }

    /// Retries — derived as `errors - failed_ops` per the
    /// existing convention in `nbrs-activity::activity`.
    /// Default 0.
    fn retries(&self) -> u64 { 0 }

    /// Effective fiber count (concurrency). Default 0.
    fn concurrency(&self) -> usize { 0 }

    /// Wallclock seconds since the subject started.
    /// Default 0.0.
    fn elapsed_secs(&self) -> f64 { 0.0 }

    /// Items consumed from the source factory — drives the
    /// throughput rate. Distinct from `cycles_completed`
    /// because data-driven phases consume one source item
    /// per op while the cycle counter tracks ops finished;
    /// for sourceless phases the two are identical.
    /// Default 0.
    fn consumed(&self) -> u64 { 0 }

    /// Ops dispatched to the adapter. Distinct from
    /// `consumed`: ops_started increments at dispatch,
    /// `consumed` increments at the source pull. The inline
    /// progress line uses `ops_started` for `pct` so a
    /// rate-limited phase shows pending vs. dispatched
    /// vs. finished correctly. Default 0 — context impls
    /// that don't track this just see "no progress" in the
    /// inline line, which is correct for them.
    fn ops_started(&self) -> u64 { 0 }

    /// Ops returned from the adapter (atomic, not the
    /// histogram counter). Distinct from
    /// [`cycles_completed`](Self::cycles_completed) which
    /// reads the `cycles_total` Counter; the two coincide
    /// in steady state but the inline-progress line uses
    /// `ops_finished` for its rate / ETA calculations and
    /// the `(rate = finished / elapsed)` shape must
    /// preserve. Default falls through to `cycles_completed`
    /// so contexts without the atomic split see equivalent
    /// behaviour.
    fn ops_finished(&self) -> u64 { self.cycles_completed() }

    /// Estimated remaining seconds until the phase finishes,
    /// or `None` when not computable (no `cycles_total` or
    /// `rate` is zero). Used by readouts that show ETA;
    /// readouts decide whether to render anything when
    /// `None`.
    fn eta_secs(&self) -> Option<f64> { None }

    // ── Workload-emphasised metrics ───────────────────────

    /// Pre-rendered status-metric chip string (e.g.
    /// ` recall_at_10:79.62% latency_p99:1.23ms`).
    /// Matches today's `ActivityMetrics::collect_status_values`
    /// output concatenated. Default empty.
    fn status_metric_chips(&self) -> String { String::new() }

    /// Pre-formatted adapter-counter tail. Today's
    /// inline-status line builds this by iterating
    /// `progress_metrics.dispensers` and concatenating
    /// `name=<count>/s` chips. Default empty.
    fn adapter_counters_text(&self) -> String { String::new() }

    /// Pre-formatted batching tail (`r/b=12.5` style).
    /// Default empty.
    fn batch_info_text(&self) -> String { String::new() }

    // ── Surface conveniences ──────────────────────────────

    /// Indent string for the depth this subject sits at in
    /// the scene tree. Matches the value
    /// `nbrs_activity::scene_tree::running_phase_indent`
    /// produces today. Default empty.
    fn depth_indent(&self) -> &str { "" }

    /// True when the surface accepts ANSI styling. Honours
    /// `NO_COLOR`, TTY presence, and explicit operator
    /// overrides — the readout queries this once and emits
    /// styling tokens (or not) on the basis of the
    /// returned bool. The §5.2 colour / style sub-language
    /// (Push 4) replaces inline ANSI with typed style tokens.
    /// Default false.
    fn use_color(&self) -> bool { false }

    // ── Event / refresh ───────────────────────────────────

    /// Which slot fired this render. Required: every
    /// context must declare what event it represents so
    /// readouts that branch on lifecycle (the `trace`
    /// diagnostic, future wildcard-bound readouts) can't
    /// misreport. No default — a phase-end fire that
    /// silently claimed `Update` would be a bug, so the
    /// type system makes the caller pick.
    fn event(&self) -> Event;

    /// Monotonic refresh-tick counter. Advances once per
    /// refresh fire of the same subject. Used by readouts
    /// that animate (the spinner glyph in `phase_status`).
    /// Default 0 — fine for one-shot lifecycle renders.
    fn refresh_tick(&self) -> u64 { 0 }

    // ── Lifecycle state ───────────────────────────────────

    /// Lifecycle state of the subject. Default
    /// [`LifecycleState::Running`] — the most common case at
    /// `on_update` fire. Lifecycle readouts (`phase_done`,
    /// `phase_summary`) branch on this to pick markers /
    /// glyphs / coloration.
    fn subject_state(&self) -> LifecycleState { LifecycleState::Running }

    // ── Session-scope identity ────────────────────────────

    /// Scenario name for the current run. Used by
    /// `session_banner`. Default empty — only session-scoped
    /// contexts populate it.
    fn session_scenario_name(&self) -> &str { "" }

    /// Workload file path for the current run. Used by
    /// `session_banner`. Default empty.
    fn session_workload_file(&self) -> &str { "" }

    // ── Session-scope totals ──────────────────────────────

    /// Total phases that completed cleanly across the run.
    /// Default 0 — only session-scoped readouts use this.
    fn session_phases_completed(&self) -> usize { 0 }

    /// Total phases that failed across the run.
    fn session_phases_failed(&self) -> usize { 0 }

    /// Total phases that didn't run (pre-mapped but skipped).
    fn session_phases_pending(&self) -> usize { 0 }

    /// Total phases the scenario tree planned.
    fn session_phases_total(&self) -> usize { 0 }

    /// Number of phases that were truncated from the
    /// post-run summary tail because they followed the last
    /// failure. Used by the `truncated_phases` readout to
    /// render the `(… and N more phases not listed)` rollup
    /// without scaling display to thousands of pending
    /// rows on a long-running scenario that failed early.
    /// Default 0 — no truncation.
    fn session_phases_truncated(&self) -> usize { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PhaseLikeCtx { name: String, labels: String }
    impl ReadoutContext for PhaseLikeCtx {
        fn subject_name(&self) -> &str { &self.name }
        fn subject_labels(&self) -> &str { &self.labels }
        fn event(&self) -> Event { Event::PhaseEnd }
    }

    #[test]
    fn default_subject_id_collapses_to_name_when_no_labels() {
        let ctx = PhaseLikeCtx { name: "setup".into(), labels: String::new() };
        assert_eq!(ctx.subject_id(), "setup");
    }

    #[test]
    fn default_subject_id_appends_labels_with_at_sign() {
        let ctx = PhaseLikeCtx {
            name: "ann_query".into(),
            labels: "(profile=alpha), (k=10)".into(),
        };
        assert_eq!(ctx.subject_id(), "ann_query@(profile=alpha), (k=10)");
    }

    struct SessionLikeCtx;
    impl ReadoutContext for SessionLikeCtx {
        fn subject_name(&self) -> &str { "session" }
        fn subject_id(&self) -> String { "session".to_string() }
        fn event(&self) -> Event { Event::SessionEnd }
    }

    #[test]
    fn session_context_overrides_subject_id_to_literal() {
        let ctx = SessionLikeCtx;
        assert_eq!(ctx.subject_id(), "session");
        // SubjectKind comes from the event, not the ctx.
        assert_eq!(ctx.event().subject_kind(), SubjectKind::Session);
    }

    #[test]
    fn subject_kind_as_str_round_trips_via_table() {
        assert_eq!(SubjectKind::Phase.as_str(), "phase");
        assert_eq!(SubjectKind::Session.as_str(), "session");
        assert_eq!(SubjectKind::Iteration.as_str(), "iteration");
        assert_eq!(SubjectKind::Scope.as_str(), "scope");
    }
}
