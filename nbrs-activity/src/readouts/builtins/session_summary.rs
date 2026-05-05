// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `session_summary` — the session-level rollup line:
//! `phases:  X completed, Y failed, Z not run (of N total)`.
//!
//! Reads session-scope totals from
//! [`ReadoutContext::session_phases_*`]. Default slot is
//! `on_session_end`; workloads can also bind it elsewhere
//! (e.g. mid-run snapshot via `on_update`) but the totals
//! only make sense at session-scope contexts.

use std::fmt::Write as _;

use crate::readouts::buf::ReadoutBuf;
use crate::readouts::context::{ReadoutContext, SubjectKind};
use crate::readouts::readout::{ContentMode, Lod, Readout, ReadoutOptions};

pub struct SessionSummary;

impl Readout for SessionSummary {
    fn name(&self) -> &'static str { "session_summary" }
    fn accepts(&self) -> &'static [SubjectKind] { &[SubjectKind::Session] }

    fn render(
        &self,
        ctx: &dyn ReadoutContext,
        lod: Lod,
        mode: ContentMode,
        _opts: &ReadoutOptions,
        out: &mut dyn ReadoutBuf,
    ) -> usize {
        match (lod, mode) {
            (Lod::Compact,  ContentMode::Value) => render_compact(ctx, out),
            (Lod::Labeled,  ContentMode::Value) => render_labeled(ctx, out),
            (Lod::Expanded, ContentMode::Value) => render_expanded(ctx, out),
            (_, ContentMode::Explanation) => render_explanation(out),
        }
    }
}

/// Compact: single-line tallies, no labels — matches the
/// existing observer's bracket form.
fn render_compact(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let mut tmp = String::with_capacity(48);
    let _ = write!(
        &mut tmp,
        "{c}/{f}/{p}/{t}",
        c = ctx.session_phases_completed(),
        f = ctx.session_phases_failed(),
        p = ctx.session_phases_pending(),
        t = ctx.session_phases_total(),
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Labeled: full-prose form matching the observer's
/// pre-engine rollup `phases:  X completed, Y failed,
/// Z not run (of N total)`.
fn render_labeled(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let mut tmp = String::with_capacity(96);
    let _ = write!(
        &mut tmp,
        "phases:  {c} completed, {f} failed, {p} not run (of {t} total)",
        c = ctx.session_phases_completed(),
        f = ctx.session_phases_failed(),
        p = ctx.session_phases_pending(),
        t = ctx.session_phases_total(),
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Expanded: per-line breakdown — same data, friendlier
/// to scan for debugging / scrollback.
fn render_expanded(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let mut tmp = String::with_capacity(160);
    let _ = write!(
        &mut tmp,
        "session totals\n  \
         completed:  {c}\n  \
         failed:     {f}\n  \
         not run:    {p}\n  \
         total:      {t}",
        c = ctx.session_phases_completed(),
        f = ctx.session_phases_failed(),
        p = ctx.session_phases_pending(),
        t = ctx.session_phases_total(),
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn render_explanation(out: &mut dyn ReadoutBuf) -> usize {
    let s = "phases:  <completed-count> completed, <failed-count> failed, \
             <pending-count> not run (of <total> total)";
    let _ = out.write_str(s);
    s.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readouts::buf::StringBuf;

    #[derive(Default)]
    struct TestCtx {
        completed: usize,
        failed: usize,
        pending: usize,
        total: usize,
    }
    impl ReadoutContext for TestCtx {
        fn subject_name(&self) -> &str { "session" }
        fn subject_seq(&self) -> Option<(usize, usize)> { None }
        fn subject_labels(&self) -> &str { "" }
        fn cycles_completed(&self) -> u64 { 0 }
        fn cycles_total(&self) -> u64 { 0 }
        fn ops_ok(&self) -> u64 { 0 }
        fn errors(&self) -> u64 { 0 }
        fn retries(&self) -> u64 { 0 }
        fn concurrency(&self) -> usize { 0 }
        fn elapsed_secs(&self) -> f64 { 0.0 }
        fn consumed(&self) -> u64 { 0 }
        fn status_metric_chips(&self) -> String { String::new() }
        fn depth_indent(&self) -> &str { "" }
        fn use_color(&self) -> bool { false }
        fn event(&self) -> crate::readouts::Event { crate::readouts::Event::SessionEnd }
        fn session_phases_completed(&self) -> usize { self.completed }
        fn session_phases_failed(&self) -> usize { self.failed }
        fn session_phases_pending(&self) -> usize { self.pending }
        fn session_phases_total(&self) -> usize { self.total }
    }

    fn render(ctx: &TestCtx, lod: Lod) -> String {
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        SessionSummary.render(
            ctx, lod, ContentMode::Value, &ReadoutOptions::new(), &mut buf,
        );
        s
    }

    #[test]
    fn labeled_matches_pre_engine_format() {
        let ctx = TestCtx {
            completed: 7, failed: 1, pending: 0, total: 8,
        };
        assert_eq!(
            render(&ctx, Lod::Labeled),
            "phases:  7 completed, 1 failed, 0 not run (of 8 total)",
        );
    }

    #[test]
    fn compact_packs_into_slash_form() {
        let ctx = TestCtx {
            completed: 5, failed: 2, pending: 1, total: 8,
        };
        assert_eq!(render(&ctx, Lod::Compact), "5/2/1/8");
    }

    #[test]
    fn expanded_breaks_onto_multiple_lines() {
        let ctx = TestCtx {
            completed: 1, failed: 0, pending: 0, total: 1,
        };
        let out = render(&ctx, Lod::Expanded);
        assert!(out.lines().count() >= 4);
        assert!(out.contains("completed:  1"));
        assert!(out.contains("total:      1"));
    }

    #[test]
    fn explanation_shows_field_descriptors() {
        let ctx = TestCtx::default();
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        SessionSummary.render(
            &ctx, Lod::Labeled, ContentMode::Explanation,
            &ReadoutOptions::new(), &mut buf,
        );
        assert!(s.contains("completed-count"));
        assert!(s.contains("failed-count"));
        assert!(s.contains("pending-count"));
    }
}
