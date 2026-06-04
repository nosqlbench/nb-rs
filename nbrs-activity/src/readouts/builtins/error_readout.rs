// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `error_readout` — phase-end error block.
//!
//! Renders the structured error list the executor collected
//! on `PhaseOutcome.errors` during the phase. Designed to
//! sit AFTER `phase_outcome` in the `on_phase_end` slot so
//! the operator sees the normative status line first
//! (✓/✗/~/…) and then the error detail block when the phase
//! failed.
//!
//! Empty errors → renders nothing (the slot is a no-op
//! when there's nothing to show), so binding this readout to
//! every `on_phase_end` is safe: successful phases produce
//! no extra output.
//!
//! Per-LOD shape:
//! - **Compact** — single-line summary chip: `(errors:N)`,
//!   or empty when no errors.
//! - **Labeled** — header line + first error line:
//!     `errors: [class] message`
//!     `  (+M more)` when more than one.
//! - **Expanded** — full per-error block with class, message,
//!   cycle, op-template, op-resolved. Mirrors the existing
//!   Expanded LOD of `phase_outcome` but stands alone so it
//!   can be bound separately.
//!
//! Subject kind: Phase (the error list lives on the phase
//! outcome).

use std::fmt::Write as _;

use crate::readouts::buf::ReadoutBuf;
use crate::readouts::context::{ReadoutContext, SubjectKind};
use crate::readouts::readout::{ContentMode, Lod, Readout, ReadoutOptions};

pub struct ErrorReadout;

impl Readout for ErrorReadout {
    fn name(&self) -> &'static str { "error_readout" }
    fn accepts(&self) -> &'static [SubjectKind] { &[SubjectKind::Phase] }

    fn render(
        &self,
        ctx: &dyn ReadoutContext,
        lod: Lod,
        mode: ContentMode,
        _opts: &ReadoutOptions,
        out: &mut dyn ReadoutBuf,
    ) -> usize {
        let errors = ctx.outcome_errors();
        if errors.is_empty() {
            return 0;
        }
        match (lod, mode) {
            (Lod::Compact,  ContentMode::Value)       => render_compact(ctx, out),
            (Lod::Labeled,  ContentMode::Value)       => render_labeled(ctx, out),
            (Lod::Expanded, ContentMode::Value)       => render_expanded(ctx, out),
            (_,             ContentMode::Explanation) => render_explanation(out),
        }
    }
}

fn render_compact(ctx: &dyn ReadoutContext, out: &mut dyn ReadoutBuf) -> usize {
    let errors = ctx.outcome_errors();
    let color = ctx.use_color();
    let red   = if color { "\x1b[31m" } else { "" };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };
    let mut tmp = String::with_capacity(32);
    let _ = write!(&mut tmp, "{dim}({reset}{red}errors:{n}{reset}{dim}){reset}",
        n = errors.len());
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Re-indent every line after the first to `prefix` so an
/// embedded newline in a driver-supplied message doesn't break
/// out of the surrounding readout block's indent.
///
/// CQL `cassandra-cpp` driver errors carry the offending
/// statement inline as a multi-line string (e.g. an INSERT
/// formatted across rows); without re-indentation those rows
/// land at column 0 instead of nesting under the `errors:`
/// header.
fn indent_continuations(s: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(s.len() + prefix.len() * 4);
    let mut first = true;
    for line in s.split('\n') {
        if first {
            first = false;
        } else {
            out.push('\n');
            out.push_str(prefix);
        }
        out.push_str(line);
    }
    out
}

fn render_labeled(ctx: &dyn ReadoutContext, out: &mut dyn ReadoutBuf) -> usize {
    // No leading newline — the readout binder's Block layout
    // inserts the separator between bound readouts itself. A
    // leading `\n` here would produce a doubled separator
    // (`\n\n` = visible blank row above the error block) when
    // bound after `phase_outcome` in the `on_phase_end` slot.
    let errors = ctx.outcome_errors();
    let color = ctx.use_color();
    let red   = if color { "\x1b[31m" } else { "" };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };
    let indent = ctx.depth_indent();
    let first = errors.first().expect("checked non-empty by caller");
    let extra_count = errors.len().saturating_sub(1);
    let continuation = format!("{indent}    ");
    let msg = indent_continuations(&first.message, &continuation);
    let mut tmp = String::with_capacity(96 + msg.len());
    let _ = write!(&mut tmp,
        "{indent}  {red}errors:{reset} {red}[{class}]{reset} {msg}",
        class = first.class,
    );
    if extra_count > 0 {
        let _ = write!(&mut tmp,
            "\n{indent}  {dim}(+{extra_count} more — see Expanded LOD or session.log){reset}");
    }
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn render_expanded(ctx: &dyn ReadoutContext, out: &mut dyn ReadoutBuf) -> usize {
    let errors = ctx.outcome_errors();
    let color = ctx.use_color();
    let red   = if color { "\x1b[31m" } else { "" };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };
    let indent = ctx.depth_indent();
    let msg_continuation = format!("{indent}    ");
    let detail_continuation = format!("{indent}      ");
    let mut tmp = String::with_capacity(256);
    // No leading newline — the binder's Block layout inserts
    // the separator. See `render_labeled` for the rationale.
    let _ = write!(&mut tmp, "{indent}  {red}errors{reset} {dim}({n}){reset}:",
        n = errors.len());
    for e in errors {
        let msg = indent_continuations(&e.message, &msg_continuation);
        let _ = write!(&mut tmp,
            "\n{indent}  {red}[{class}]{reset} {msg}",
            class = e.class);
        if let Some(c) = e.cycle {
            let _ = write!(&mut tmp,
                "\n{indent}    {dim}cycle:{reset} {c}");
        }
        if let Some(op) = &e.op_name {
            let _ = write!(&mut tmp,
                "\n{indent}    {dim}op:{reset} {op}");
        }
        if let Some(t) = &e.op_template {
            let t = indent_continuations(t, &detail_continuation);
            let _ = write!(&mut tmp,
                "\n{indent}    {dim}op-template:{reset} {t}");
        }
        if let Some(r) = &e.op_resolved {
            let r = indent_continuations(r, &detail_continuation);
            let _ = write!(&mut tmp,
                "\n{indent}    {dim}op-resolved:{reset} {r}");
        }
    }
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn render_explanation(out: &mut dyn ReadoutBuf) -> usize {
    let s = "error_readout — per-phase error block; renders only when the phase recorded \
             at least one structured error. Compact = count summary; Labeled = first error \
             line with extra-count tail; Expanded = full per-error block with cycle / \
             op-template / op-resolved detail";
    let _ = out.write_str(s);
    s.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readouts::buf::StringBuf;
    use crate::phase_outcome::PhaseErrorDetail;

    #[derive(Default)]
    struct TestCtx {
        errors: Vec<PhaseErrorDetail>,
        indent: String,
        color: bool,
    }

    impl ReadoutContext for TestCtx {
        fn subject_name(&self) -> &str { "" }
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
        fn depth_indent(&self) -> &str { &self.indent }
        fn use_color(&self) -> bool { self.color }
        fn event(&self) -> crate::readouts::Event { crate::readouts::Event::PhaseEnd }
        fn outcome_errors(&self) -> &[PhaseErrorDetail] { &self.errors }
    }

    fn err(class: &str, msg: &str, cycle: Option<u64>) -> PhaseErrorDetail {
        PhaseErrorDetail {
            class: class.into(),
            message: msg.into(),
            op_name: None,
            cycle,
            op_template: None,
            op_resolved: None,
            at_nanos: 0,
            retryable: false,
        }
    }

    fn render(ctx: &TestCtx, lod: Lod) -> String {
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        ErrorReadout.render(ctx, lod, ContentMode::Value, &ReadoutOptions::new(), &mut buf);
        s
    }

    #[test]
    fn empty_errors_renders_nothing() {
        let ctx = TestCtx::default();
        assert_eq!(render(&ctx, Lod::Labeled), "");
        assert_eq!(render(&ctx, Lod::Compact), "");
        assert_eq!(render(&ctx, Lod::Expanded), "");
    }

    #[test]
    fn compact_shows_count_only() {
        let ctx = TestCtx {
            errors: vec![err("X", "one", None), err("Y", "two", None)],
            ..Default::default()
        };
        let out = render(&ctx, Lod::Compact);
        assert!(out.contains("errors:2"));
        assert!(!out.contains("one"));
        assert!(!out.contains("two"));
    }

    #[test]
    fn labeled_shows_first_error_with_more_count() {
        let ctx = TestCtx {
            errors: vec![
                err("CqlParseError", "syntax", Some(7)),
                err("CqlParseError", "syntax", Some(8)),
                err("CqlParseError", "syntax", Some(9)),
            ],
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        // No leading newline — the binder's Block-layout
        // dispatcher inserts the separator between bound
        // readouts. A leading `\n` here would double up to
        // `\n\n` (visible blank row) above the error block.
        assert!(!out.starts_with('\n'),
            "no leading newline — binder owns the separator: {out:?}");
        assert!(out.contains("[CqlParseError]"));
        assert!(out.contains("syntax"));
        assert!(out.contains("+2 more"));
    }

    #[test]
    fn labeled_single_error_omits_more_suffix() {
        let ctx = TestCtx {
            errors: vec![err("X", "only", None)],
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        assert!(out.contains("[X]"));
        assert!(out.contains("only"));
        assert!(!out.contains("more"));
    }

    #[test]
    fn expanded_lists_every_error_with_cycle() {
        let ctx = TestCtx {
            errors: vec![
                err("A", "first", Some(0)),
                err("B", "second", Some(5)),
            ],
            ..Default::default()
        };
        let out = render(&ctx, Lod::Expanded);
        assert!(out.contains("[A]"));
        assert!(out.contains("first"));
        assert!(out.contains("cycle:"));
        assert!(out.contains("[B]"));
        assert!(out.contains("second"));
    }

    #[test]
    fn ansi_emitted_when_color_enabled() {
        let ctx = TestCtx {
            errors: vec![err("X", "msg", None)],
            color: true,
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        assert!(out.contains("\x1b[31m"));
    }

    /// CQL driver errors carry the offending statement inline
    /// as a multi-line string. Every line after the first must
    /// inherit the surrounding readout's indent so the output
    /// doesn't break out to column 0.
    #[test]
    fn labeled_multiline_message_keeps_continuation_indent() {
        let multi = "Cassandra error: timeout\n\
                     statement: INSERT INTO ks.t\n\
                     (a, b, c) VALUES\n\
                     (?, ?, ?)";
        let ctx = TestCtx {
            errors: vec![err("cql_error", multi, None)],
            indent: "                ".into(),  // 16-space depth indent
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        // Every line that ISN'T the leading newline or the
        // first error line must start with at least the
        // depth indent — never column 0, never < indent.
        let depth = "                "; // matches ctx.indent
        for (i, line) in out.split('\n').enumerate() {
            if i == 0 || line.is_empty() { continue; }
            assert!(
                line.starts_with(depth),
                "line {i} broke indent at column 0: {line:?}"
            );
        }
    }

    #[test]
    fn expanded_multiline_message_keeps_continuation_indent() {
        let multi = "first line\nsecond line\nthird line";
        let ctx = TestCtx {
            errors: vec![err("X", multi, None)],
            indent: "      ".into(),
            ..Default::default()
        };
        let out = render(&ctx, Lod::Expanded);
        let depth = "      ";
        for (i, line) in out.split('\n').enumerate() {
            if i == 0 || line.is_empty() { continue; }
            assert!(
                line.starts_with(depth),
                "line {i} broke indent at column 0: {line:?}"
            );
        }
    }
}
