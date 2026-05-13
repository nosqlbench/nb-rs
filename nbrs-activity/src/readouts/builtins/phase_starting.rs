// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `phase_starting` — opt-in pre-phase header. Push 2
//! removed the unconditional `▶ phase 'X' starting` row
//! from the live output; this readout brings it back as a
//! workload-bound option, default-targetting `on_phase_start`.
//!
//! Compact / Labeled / Expanded show progressively more
//! context (name only → name + seq → name + seq + iter
//! tuple labels). Explanation describes the row's purpose.

use std::fmt::Write as _;

use crate::readouts::buf::ReadoutBuf;
use crate::readouts::context::{ReadoutContext, SubjectKind};
use crate::readouts::readout::{ContentMode, Lod, Readout, ReadoutOptions};

pub struct PhaseStarting;

impl Readout for PhaseStarting {
    fn name(&self) -> &'static str { "phase_starting" }
    fn accepts(&self) -> &'static [SubjectKind] { &[SubjectKind::Phase] }

    fn render(
        &self,
        ctx: &dyn ReadoutContext,
        lod: Lod,
        mode: ContentMode,
        _opts: &ReadoutOptions,
        out: &mut dyn ReadoutBuf,
    ) -> usize {
        match (lod, mode) {
            (Lod::Compact,  ContentMode::Value)       => render_compact(ctx, out),
            (Lod::Labeled,  ContentMode::Value)       => render_labeled(ctx, out),
            (Lod::Expanded, ContentMode::Value)       => render_expanded(ctx, out),
            (_,             ContentMode::Explanation) => render_explanation(out),
        }
    }
}

fn render_compact(ctx: &dyn ReadoutContext, out: &mut dyn ReadoutBuf) -> usize {
    let color = ctx.use_color();
    let green = if color { "\x1b[32m" } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };
    let name = ctx.subject_name();
    let depth_indent = ctx.depth_indent();
    let mut tmp = String::with_capacity(64);
    let _ = write!(&mut tmp, "{depth_indent}{green}▶{reset} {name}");
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn render_labeled(ctx: &dyn ReadoutContext, out: &mut dyn ReadoutBuf) -> usize {
    // Same prefix shape as `phase_done` so the START / DONE
    // pair line up vertically:
    //   {indent}▶ [idx/total] [name] (coords) starting
    //   {indent}✓ [idx/total] [name] (coords) 100%
    let color = ctx.use_color();
    let green = if color { "\x1b[32m" } else { "" };
    let bold  = if color { "\x1b[1m"  } else { "" };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let blue  = if color { "\x1b[34m" } else { "" };
    let yellow= if color { "\x1b[33m" } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };
    let name = ctx.subject_name();
    let labels = ctx.subject_labels();
    let depth_indent = ctx.depth_indent();
    let seq_part: String = match ctx.subject_seq() {
        Some((s, t)) => format!("{dim}[{s}/{t}]{reset} "),
        None => String::new(),
    };
    let coords_part: String = if labels.is_empty() {
        String::new()
    } else {
        format!(" {bold}{yellow}{labels}{reset}")
    };
    let mut tmp = String::with_capacity(160);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{green}▶{reset} {seq_part}{bold}{blue}[{name}]{reset}{coords_part} starting",
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn render_expanded(ctx: &dyn ReadoutContext, out: &mut dyn ReadoutBuf) -> usize {
    // Same prefix as Labeled; iter-tuple coords on their own
    // line below (matching phase_done's Expanded shape).
    let color = ctx.use_color();
    let green = if color { "\x1b[32m" } else { "" };
    let bold  = if color { "\x1b[1m"  } else { "" };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let blue  = if color { "\x1b[34m" } else { "" };
    let yellow= if color { "\x1b[33m" } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };
    let name = ctx.subject_name();
    let labels = ctx.subject_labels();
    let depth_indent = ctx.depth_indent();
    let seq_part: String = match ctx.subject_seq() {
        Some((s, t)) => format!("{dim}[{s}/{t}]{reset} "),
        None => String::new(),
    };
    let mut tmp = String::with_capacity(192);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{green}▶{reset} {seq_part}{bold}{blue}[{name}]{reset} starting",
    );
    if !labels.is_empty() {
        let _ = write!(&mut tmp, "\n{depth_indent}  {bold}{yellow}{labels}{reset}");
    }
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn render_explanation(out: &mut dyn ReadoutBuf) -> usize {
    let s = "▶ {phase-name} ({idx}/{total}) starting — opt-in pre-phase header";
    let _ = out.write_str(s);
    s.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readouts::buf::StringBuf;

    #[derive(Default)]
    struct TestCtx {
        name: String,
        seq: Option<(usize, usize)>,
        labels: String,
        use_color: bool,
    }
    impl ReadoutContext for TestCtx {
        fn subject_name(&self) -> &str { &self.name }
        fn subject_seq(&self) -> Option<(usize, usize)> { self.seq }
        fn subject_labels(&self) -> &str { &self.labels }
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
        fn use_color(&self) -> bool { self.use_color }
        fn event(&self) -> crate::readouts::Event { crate::readouts::Event::PhaseStart }
    }

    fn render(ctx: &TestCtx, lod: Lod) -> String {
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        PhaseStarting.render(ctx, lod, ContentMode::Value, &ReadoutOptions::new(), &mut buf);
        s
    }

    #[test]
    fn labeled_with_seq() {
        let ctx = TestCtx {
            name: "run".into(),
            seq: Some((3, 8)),
            ..Default::default()
        };
        // Prefix shape matches phase_done so the start/done
        // pair line up: ▶ [seq] [name] [(coords)] starting.
        assert_eq!(render(&ctx, Lod::Labeled), "▶ [3/8] [run] starting");
    }

    #[test]
    fn labeled_without_seq() {
        let ctx = TestCtx { name: "setup".into(), ..Default::default() };
        assert_eq!(render(&ctx, Lod::Labeled), "▶ [setup] starting");
    }

    #[test]
    fn compact_drops_seq_and_starting_word() {
        let ctx = TestCtx {
            name: "run".into(),
            seq: Some((3, 8)),
            ..Default::default()
        };
        assert_eq!(render(&ctx, Lod::Compact), "▶ run");
    }

    #[test]
    fn expanded_includes_iter_labels() {
        let ctx = TestCtx {
            name: "run".into(),
            seq: Some((3, 8)),
            labels: "(profile=alpha, k=10)".into(),
            ..Default::default()
        };
        let out = render(&ctx, Lod::Expanded);
        assert!(out.contains("[3/8]"), "got: {out:?}");
        assert!(out.contains("[run]"), "got: {out:?}");
        assert!(out.contains("(profile=alpha, k=10)"), "got: {out:?}");
        assert!(out.lines().count() >= 2);
    }
}
