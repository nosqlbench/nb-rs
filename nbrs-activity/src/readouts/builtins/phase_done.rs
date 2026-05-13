// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `phase_done` — the ✓ DONE summary line.
//!
//! Push 1 byte-equivalence target: the line emitted by
//! `nbrs-activity::activity`'s end-of-activity block prior
//! to this push. The format string was:
//!
//! ```text
//! {depth_indent}{green}✓{reset} {seq_prefix}{bold}{blue}[{phase_name}]{reset}{coords_part} \
//!  {pct:.0}% {rate_str} ok:{ok_pct:.0}% \
//!  {err_color}e:{errors} r:{retries}{reset} c:{concurrency}{relevancy_str} \
//!  {dim}({elapsed:.2}s){reset}
//! ```
//!
//! Where:
//! - `seq_prefix` = `{dim}[{idx}/{total}]{reset} ` if seq is
//!   `Some`, else `""`.
//! - `coords_part` = ` {bold}{yellow}{labels}{reset}` if
//!   labels are non-empty, else `""`.
//! - `err_color` = yellow when errors or retries > 0, else dim.
//! - `rate_str` = auto-scaled (M/s | K/s | /s) per the
//!   helper logic in the prior implementation.
//!
//! That string is the canonical render at
//! `Lod::Labeled, ContentMode::Value`. Compact and Expanded
//! ship in Push 9g (closes G17) per SRD-63 §3.3
//! monotonicity:
//!
//! - **Compact** — `{depth}✓ [name] {pct}% ({elapsed:.2}s)`
//!   — trained-operator scan form: status glyph + identity
//!   + completion percentage + wallclock. Drops the seq
//!   prefix, rate, ok-pct, error / retry / concurrency
//!   counts, scope coords, and chip tail. Every retained
//!   field also appears in Labeled (monotonicity).
//! - **Expanded** — multi-line labelled block. Same data
//!   as Labeled but split across lines with a label per
//!   field, and the chip stream broken into one chip per
//!   line. Adds nothing new (monotonicity flows the
//!   other way too: every Labeled field is in Expanded).
//!
//! Explanation overlay is per SRD-63 §3.2: same shape as
//! the value at the same LOD, with field labels swapped
//! for descriptors.

use std::fmt::Write as _;

use crate::readouts::buf::ReadoutBuf;
use crate::readouts::context::{ReadoutContext, SubjectKind};
use crate::readouts::readout::{ContentMode, Lod, Readout, ReadoutOptions};

pub struct PhaseDone;

impl Readout for PhaseDone {
    fn name(&self) -> &'static str { "phase_done" }
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
            (Lod::Compact,  ContentMode::Value)       => render_compact_value(ctx, out),
            (Lod::Compact,  ContentMode::Explanation) => render_compact_explanation(ctx, out),
            (Lod::Labeled,  ContentMode::Value)       => render_labeled_value(ctx, out),
            (Lod::Labeled,  ContentMode::Explanation) => render_labeled_explanation(ctx, out),
            (Lod::Expanded, ContentMode::Value)       => render_expanded_value(ctx, out),
            (Lod::Expanded, ContentMode::Explanation) => render_expanded_explanation(ctx, out),
        }
    }
}

// ── Compact LOD ───────────────────────────────────────────

/// Compact LOD: `{depth}✓ [name] {pct}% ({elapsed:.2}s)`.
/// Trained-operator scan form: status glyph + identity +
/// completion percentage + wallclock. Per §3.3
/// monotonicity, every field is present in Labeled.
fn render_compact_value(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let bold   = if color { "\x1b[1m"  } else { "" };
    let dim    = if color { "\x1b[2m"  } else { "" };
    let blue   = if color { "\x1b[34m" } else { "" };
    let green  = if color { "\x1b[32m" } else { "" };
    let reset  = if color { "\x1b[0m"  } else { "" };

    let cycles = ctx.cycles_completed();
    let total_extent = ctx.cycles_total();
    let pct: f64 = if total_extent > 0 {
        cycles as f64 * 100.0 / total_extent as f64
    } else {
        100.0
    };
    let elapsed = ctx.elapsed_secs();
    let depth_indent = ctx.depth_indent();
    let name = ctx.subject_name();

    let mut tmp = String::with_capacity(64);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{green}✓{reset} {bold}{blue}[{name}]{reset} \
{pct:.0}% {dim}({elapsed:.2}s){reset}",
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Compact LOD explanation overlay. Same skeleton as the
/// value form, with field tokens swapped for descriptors.
fn render_compact_explanation(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let bold   = if color { "\x1b[1m"  } else { "" };
    let dim    = if color { "\x1b[2m"  } else { "" };
    let blue   = if color { "\x1b[34m" } else { "" };
    let green  = if color { "\x1b[32m" } else { "" };
    let reset  = if color { "\x1b[0m"  } else { "" };

    let depth_indent = ctx.depth_indent();
    let mut tmp = String::with_capacity(96);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{green}done{reset} {bold}{blue}[phase-name]{reset} \
progress% {dim}(elapsed){reset}",
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

// ── Expanded LOD ──────────────────────────────────────────

/// Expanded LOD: multi-line labelled block. Same data as
/// Labeled, organised one field per line. Per §3.3
/// monotonicity, every Labeled field is here too.
fn render_expanded_value(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let bold   = if color { "\x1b[1m"  } else { "" };
    let dim    = if color { "\x1b[2m"  } else { "" };
    let yellow = if color { "\x1b[33m" } else { "" };
    let blue   = if color { "\x1b[34m" } else { "" };
    let green  = if color { "\x1b[32m" } else { "" };
    let reset  = if color { "\x1b[0m"  } else { "" };

    let cycles = ctx.cycles_completed();
    let errors = ctx.errors();
    let retries = ctx.retries();
    let ok = ctx.ops_ok();
    let concurrency = ctx.concurrency();
    let elapsed = ctx.elapsed_secs();
    let consumed = ctx.consumed();
    let total_extent = ctx.cycles_total();

    let ok_pct: f64 = if cycles > 0 {
        ok as f64 * 100.0 / cycles as f64
    } else { 100.0 };
    let pct: f64 = if total_extent > 0 {
        cycles as f64 * 100.0 / total_extent as f64
    } else { 100.0 };
    let rate: f64 = if elapsed > 0.0 { consumed as f64 / elapsed } else { 0.0 };
    let rate_str = format_rate(rate);

    let err_color = if errors > 0 || retries > 0 { yellow } else { dim };
    let labels = ctx.subject_labels();
    let depth_indent = ctx.depth_indent();
    let seq_part: String = match ctx.subject_seq() {
        Some((s, t)) => format!(" {dim}[{s}/{t}]{reset}"),
        None => String::new(),
    };
    let coords_line = if labels.is_empty() {
        String::new()
    } else {
        format!("\n{depth_indent}  coords:      {bold}{yellow}{labels}{reset}")
    };
    // Chip stream: convert `name:value` chips into one per
    // line so the Expanded block reads vertically. Empty
    // chip strings render no metrics block.
    let chips_block = render_chips_block(
        &ctx.status_metric_chips(), depth_indent, dim, reset,
    );

    let mut tmp = String::with_capacity(384);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{green}✓{reset} {bold}{blue}[{name}]{reset}{seq}{coords}\n\
{depth_indent}  progress:    {pct:.0}% ({cycles} of {total})\n\
{depth_indent}  throughput:  {rate_str}\n\
{depth_indent}  ok:          {ok_pct:.0}%  ({ok} of {cycles})\n\
{depth_indent}  reliability: {err_color}e:{errors} r:{retries}{reset}\n\
{depth_indent}  concurrency: {concurrency}\n\
{chips}\
{depth_indent}  elapsed:     {dim}{elapsed:.2}s{reset}",
        name = ctx.subject_name(),
        seq = seq_part,
        coords = coords_line,
        chips = chips_block,
        total = total_extent,
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Format the chip stream as one chip per line under a
/// `metrics:` header. `chips` follows the convention from
/// `ActivityMetrics::collect_status_values`: leading-space-
/// separated `name:value` tokens. Returns empty when
/// `chips` is empty (the metrics block is skipped).
fn render_chips_block(chips: &str, indent: &str, dim: &str, reset: &str) -> String {
    let entries: Vec<&str> = chips.split_whitespace().collect();
    if entries.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(64 + entries.len() * 24);
    let _ = write!(&mut out, "{indent}  metrics:\n");
    for chip in &entries {
        // Each chip is `name:value`; align the name in a
        // 16-char field so values columnise.
        let (name, value) = chip.split_once(':')
            .map(|(n, v)| (n, v))
            .unwrap_or((chip, ""));
        let _ = write!(&mut out,
            "{indent}    {name:<16} {dim}{value}{reset}\n");
    }
    out
}

/// Expanded LOD explanation overlay. Same multi-line shape
/// as the value form; field labels stay (they're already
/// descriptors), values are replaced with token names.
fn render_expanded_explanation(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let bold  = if color { "\x1b[1m"  } else { "" };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let blue  = if color { "\x1b[34m" } else { "" };
    let green = if color { "\x1b[32m" } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };

    let depth_indent = ctx.depth_indent();
    let mut tmp = String::with_capacity(384);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{green}done{reset} {bold}{blue}[phase-name]{reset}\n\
{depth_indent}  progress:    progress% (cycles_completed of cycles_total)\n\
{depth_indent}  throughput:  rate (auto-scaled K/s, M/s)\n\
{depth_indent}  ok:          ok-pct% (ops_ok of cycles_completed)\n\
{depth_indent}  reliability: e:errors r:retries\n\
{depth_indent}  concurrency: fiber count\n\
{depth_indent}  elapsed:     {dim}wallclock seconds{reset}",
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Explanation overlay (SRD-63 §3.2 / Push 7). Same
/// shape as the value render, with each glyph / cluster
/// replaced by text describing what it means. Width
/// parity is the readout author's contract — we keep
/// the structural skeleton (`✓`, `[name]`, percentages,
/// `e:`/`r:`/`c:` tail) and rewrite each token's *text*
/// to its meaning.
fn render_labeled_explanation(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let bold   = if color { "\x1b[1m"  } else { "" };
    let dim    = if color { "\x1b[2m"  } else { "" };
    let yellow = if color { "\x1b[33m" } else { "" };
    let blue   = if color { "\x1b[34m" } else { "" };
    let green  = if color { "\x1b[32m" } else { "" };
    let reset  = if color { "\x1b[0m"  } else { "" };

    let depth_indent = ctx.depth_indent();
    let seq_part: String = match ctx.subject_seq() {
        Some(_) => format!("{dim}[idx/total]{reset} "),
        None => String::new(),
    };
    let coords_part: String = if ctx.subject_labels().is_empty() {
        String::new()
    } else {
        format!(" {bold}{yellow}(scope-coords){reset}")
    };

    // Width-parity bars: pct → "100%", rate → "rate/s",
    // ok_pct → "ok-pct%", and so on. Each replacement is
    // *short* enough to overlay without wrapping; the
    // user's expectation is "what does this glyph mean?"
    // not "every detail spelled out."
    let mut tmp = String::with_capacity(160);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{green}done{reset} {seq}{bold}{blue}[phase-name]{reset}{coords} \
progress% throughput ok:{ok}% \
errors retries concurrency \
{dim}(elapsed){reset}",
        seq = seq_part,
        coords = coords_part,
        ok = "ok",
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn render_labeled_value(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let bold   = if color { "\x1b[1m"  } else { "" };
    let dim    = if color { "\x1b[2m"  } else { "" };
    let yellow = if color { "\x1b[33m" } else { "" };
    let blue   = if color { "\x1b[34m" } else { "" };
    let green  = if color { "\x1b[32m" } else { "" };
    let reset  = if color { "\x1b[0m"  } else { "" };

    let cycles = ctx.cycles_completed();
    let errors = ctx.errors();
    let retries = ctx.retries();
    let ok = ctx.ops_ok();
    let concurrency = ctx.concurrency();
    let elapsed = ctx.elapsed_secs();
    let consumed = ctx.consumed();
    let total_extent = ctx.cycles_total();

    let ok_pct: f64 = if cycles > 0 {
        ok as f64 * 100.0 / cycles as f64
    } else {
        100.0
    };
    let pct: f64 = if total_extent > 0 {
        cycles as f64 * 100.0 / total_extent as f64
    } else {
        100.0
    };
    let rate: f64 = if elapsed > 0.0 {
        consumed as f64 / elapsed
    } else {
        0.0
    };
    let rate_str = format_rate(rate);

    let err_color = if errors > 0 || retries > 0 { yellow } else { dim };

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
    let chips = ctx.status_metric_chips();

    // Memo header (if any) — see phase_status for rationale.
    // The memo carries the latest published state at phase
    // end; useful when a phase's last activity (e.g. "compacted
    // table_X") is the takeaway the operator needs.
    let memo = ctx.phase_memo();
    let memo_header = if memo.is_empty() {
        String::new()
    } else {
        let bold_yellow = if color { "\x1b[1;33m" } else { "" };
        format!("{depth_indent}{bold_yellow}[[ {memo} ]]{reset}\n")
    };

    // Two-line layout mirroring `phase_status` Labeled:
    //   line 1: {depth}✓ {seq}[{name}]{coords} {pct}%
    //   line 2: {depth}    {rate} ok:{ok}% e:{e} r:{r} c:{c}{chips} (elapsed)
    // Break after the progress percentage keeps the head row
    // narrow (status glyph + identity + completion) and lets
    // the tail carry all the throughput/counter detail
    // without exceeding terminal width.
    let mut tmp = String::with_capacity(256);
    let _ = write!(
        &mut tmp,
        "{memo_header}\
{depth_indent}{green}✓{reset} {seq}{bold}{blue}[{name}]{reset}{coords} {pct:.0}%\n\
{depth_indent}    {rate_str} ok:{ok_pct:.0}% \
{err_color}e:{errors} r:{retries}{reset} c:{concurrency}{chips} \
{dim}({elapsed:.2}s){reset}",
        depth_indent = depth_indent,
        green = green,
        reset = reset,
        seq = seq_part,
        bold = bold,
        blue = blue,
        name = ctx.subject_name(),
        coords = coords_part,
        pct = pct,
        rate_str = rate_str,
        ok_pct = ok_pct,
        err_color = err_color,
        errors = errors,
        retries = retries,
        concurrency = concurrency,
        chips = chips,
        dim = dim,
        elapsed = elapsed,
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.1}M/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K/s", rate / 1_000.0)
    } else {
        format!("{:.0}/s", rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readouts::buf::StringBuf;

    /// Tiny in-test context that lets us hand-pick every
    /// field. Lives here so the `phase_done` golden can run
    /// without pulling in `nbrs-activity`.
    #[derive(Default)]
    struct TestCtx {
        phase_name: String,
        phase_seq: Option<(usize, usize)>,
        phase_labels: String,
        cycles_completed: u64,
        cycles_total: u64,
        ops_ok: u64,
        errors: u64,
        retries: u64,
        concurrency: usize,
        elapsed_secs: f64,
        consumed: u64,
        chips: String,
        depth_indent: String,
        use_color: bool,
    }

    impl ReadoutContext for TestCtx {
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
        fn status_metric_chips(&self) -> String { self.chips.clone() }
        fn depth_indent(&self) -> &str { &self.depth_indent }
        fn use_color(&self) -> bool { self.use_color }
        fn event(&self) -> crate::readouts::Event { crate::readouts::Event::PhaseEnd }
    }

    fn render(ctx: &TestCtx) -> String {
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        PhaseDone.render(
            ctx, Lod::Labeled, ContentMode::Value,
            &ReadoutOptions::new(), &mut buf,
        );
        s
    }

    #[test]
    fn no_color_no_coords_no_chips() {
        let ctx = TestCtx {
            phase_name: "setup".into(),
            phase_seq: Some((1, 2)),
            cycles_completed: 3,
            cycles_total: 3,
            ops_ok: 3,
            concurrency: 1,
            elapsed_secs: 0.01,
            consumed: 3,
            ..Default::default()
        };
        assert_eq!(
            render(&ctx),
            "✓ [1/2] [setup] 100%\n    300/s ok:100% e:0 r:0 c:1 (0.01s)"
        );
    }

    #[test]
    fn no_color_with_coords_and_chips() {
        let ctx = TestCtx {
            phase_name: "run".into(),
            phase_seq: Some((1, 8)),
            phase_labels: "(profile=alpha), (bucket=1, kind=READ)".into(),
            cycles_completed: 162,
            cycles_total: 162,
            ops_ok: 162,
            concurrency: 1,
            elapsed_secs: 0.01,
            consumed: 162,
            chips: " recall_at_10:79.62%".into(),
            ..Default::default()
        };
        assert_eq!(
            render(&ctx),
            "✓ [1/8] [run] (profile=alpha), (bucket=1, kind=READ) 100%\n    16.2K/s ok:100% e:0 r:0 c:1 recall_at_10:79.62% (0.01s)"
        );
    }

    fn render_at(ctx: &TestCtx, lod: Lod, mode: ContentMode) -> String {
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        PhaseDone.render(
            ctx, lod, mode, &ReadoutOptions::new(), &mut buf,
        );
        s
    }

    #[test]
    fn compact_value_drops_seq_rate_counts_chips() {
        // Push 9g (G17): Compact form is the trained-
        // operator scan version. Status glyph + name + pct
        // + elapsed only. Seq prefix, rate, ok-pct,
        // errors / retries / concurrency, scope coords, and
        // chips are all dropped. Every retained field
        // appears in Labeled (§3.3 monotonicity).
        let ctx = TestCtx {
            phase_name: "setup".into(),
            phase_seq: Some((1, 2)),
            phase_labels: "(profile=alpha)".into(),
            cycles_completed: 3,
            cycles_total: 3,
            ops_ok: 3,
            errors: 0,
            retries: 0,
            concurrency: 1,
            elapsed_secs: 0.01,
            consumed: 3,
            chips: " recall_at_10:79.62%".into(),
            ..Default::default()
        };
        assert_eq!(
            render_at(&ctx, Lod::Compact, ContentMode::Value),
            "✓ [setup] 100% (0.01s)",
        );
    }

    #[test]
    fn compact_value_pct_zero_when_no_extent() {
        // total_extent == 0 → 100% per the readout's no-
        // extent convention (the phase ran without a
        // declared cycle count and is by definition complete
        // at this fire).
        let ctx = TestCtx {
            phase_name: "x".into(),
            cycles_completed: 0,
            cycles_total: 0,
            elapsed_secs: 0.5,
            ..Default::default()
        };
        assert_eq!(
            render_at(&ctx, Lod::Compact, ContentMode::Value),
            "✓ [x] 100% (0.50s)",
        );
    }

    #[test]
    fn compact_explanation_describes_each_field() {
        let ctx = TestCtx { phase_name: "x".into(), ..Default::default() };
        let s = render_at(&ctx, Lod::Compact, ContentMode::Explanation);
        assert!(s.contains("done"),       "expected 'done': {s}");
        assert!(s.contains("phase-name"), "expected 'phase-name': {s}");
        assert!(s.contains("progress%"),  "expected 'progress%': {s}");
        assert!(s.contains("(elapsed)"),  "expected '(elapsed)': {s}");
        // Compact's overlay must NOT describe fields it
        // doesn't show (rate, ok-pct, errors, retries,
        // concurrency, coords, chips, seq).
        assert!(!s.contains("idx/total"),
            "compact must not describe seq prefix it doesn't render: {s}");
        assert!(!s.contains("throughput"),
            "compact must not describe throughput it doesn't render: {s}");
    }

    #[test]
    fn expanded_value_emits_multi_line_block() {
        // Expanded form: same data as Labeled, organised
        // one field per line.
        let ctx = TestCtx {
            phase_name: "ann_query".into(),
            phase_seq: Some((1, 8)),
            phase_labels: "profile=alpha, k=10".into(),
            cycles_completed: 100,
            cycles_total: 100,
            ops_ok: 99,
            errors: 1,
            retries: 0,
            concurrency: 4,
            elapsed_secs: 1.5,
            consumed: 100,
            chips: " recall_at_10:79.62% latency_p99:1.23ms".into(),
            ..Default::default()
        };
        let s = render_at(&ctx, Lod::Expanded, ContentMode::Value);
        // Header line carries the same identity as Labeled.
        assert!(s.contains("✓ [ann_query]"));
        assert!(s.contains("[1/8]"));
        assert!(s.contains("profile=alpha, k=10"));
        // Per-field labelled rows (every Labeled field
        // appears here too — §3.3 monotonicity).
        assert!(s.contains("progress:    100% (100 of 100)"));
        assert!(s.contains("throughput:"));
        assert!(s.contains("ok:          99%  (99 of 100)"));
        assert!(s.contains("reliability: e:1 r:0"));
        assert!(s.contains("concurrency: 4"));
        assert!(s.contains("metrics:"));
        // Chips broken into one-per-line under `metrics:`.
        assert!(s.contains("recall_at_10"));
        assert!(s.contains("latency_p99"));
        assert!(s.contains("elapsed:     1.50s"));
        // Multi-line block — verify line count.
        let line_count = s.lines().count();
        assert!(line_count >= 8,
            "expanded should be multi-line (got {line_count}): {s}");
    }

    #[test]
    fn expanded_value_omits_metrics_block_when_no_chips() {
        // metrics: header only renders when there are chips
        // to show under it.
        let ctx = TestCtx {
            phase_name: "setup".into(),
            cycles_completed: 1,
            cycles_total: 1,
            ops_ok: 1,
            concurrency: 1,
            elapsed_secs: 0.01,
            consumed: 1,
            chips: String::new(),
            ..Default::default()
        };
        let s = render_at(&ctx, Lod::Expanded, ContentMode::Value);
        assert!(!s.contains("metrics:"),
            "expected no metrics: header when chips empty: {s}");
    }

    #[test]
    fn expanded_value_omits_coords_line_when_no_labels() {
        let ctx = TestCtx {
            phase_name: "x".into(),
            phase_labels: String::new(),
            cycles_completed: 1,
            cycles_total: 1,
            ops_ok: 1,
            concurrency: 1,
            elapsed_secs: 0.01,
            consumed: 1,
            ..Default::default()
        };
        let s = render_at(&ctx, Lod::Expanded, ContentMode::Value);
        assert!(!s.contains("coords:"),
            "expected no coords: line when labels empty: {s}");
    }

    #[test]
    fn expanded_explanation_describes_each_row() {
        let ctx = TestCtx { phase_name: "x".into(), ..Default::default() };
        let s = render_at(&ctx, Lod::Expanded, ContentMode::Explanation);
        assert!(s.contains("phase-name"));
        assert!(s.contains("progress:"));
        assert!(s.contains("throughput:"));
        assert!(s.contains("ok:"));
        assert!(s.contains("reliability:"));
        assert!(s.contains("concurrency:"));
        assert!(s.contains("elapsed:"));
        // Multi-line.
        assert!(s.lines().count() >= 7);
    }

    #[test]
    fn monotonicity_compact_subset_of_labeled() {
        // §3.3 invariant: every field shown at Compact
        // appears at Labeled too. Verified pragmatically:
        // the compact rendering's stripped form (depth,
        // glyph, name, pct, elapsed, parens) is
        // substring-present in the labeled rendering once
        // we drop the seq / rate / ok / counts / chips
        // additions.
        let ctx = TestCtx {
            phase_name: "setup".into(),
            cycles_completed: 3,
            cycles_total: 3,
            ops_ok: 3,
            concurrency: 1,
            elapsed_secs: 0.01,
            consumed: 3,
            ..Default::default()
        };
        let labeled = render_at(&ctx, Lod::Labeled, ContentMode::Value);
        let compact = render_at(&ctx, Lod::Compact, ContentMode::Value);
        // Identity: "[setup]" appears in both.
        assert!(labeled.contains("[setup]") && compact.contains("[setup]"));
        // Status glyph appears in both.
        assert!(labeled.contains('✓') && compact.contains('✓'));
        // Pct appears in both.
        assert!(labeled.contains("100%") && compact.contains("100%"));
        // Elapsed appears in both.
        assert!(labeled.contains("(0.01s)") && compact.contains("(0.01s)"));
    }

    #[test]
    fn explanation_mode_describes_each_field() {
        // SRD-63 §3.2: explanation overlay describes glyph
        // meaning. Width parity is the author's contract.
        let ctx = TestCtx {
            phase_name: "setup".into(),
            phase_seq: Some((1, 2)),
            phase_labels: "(profile=alpha)".into(),
            ..Default::default()
        };
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        let n = PhaseDone.render(
            &ctx, Lod::Labeled, ContentMode::Explanation,
            &ReadoutOptions::new(), &mut buf,
        );
        assert!(n > 0, "explanation should render");
        // Spot-check semantic descriptors are present —
        // the user reads "phase-name", "progress%", etc.
        // rather than concrete data.
        assert!(s.contains("done"),         "expected 'done' descriptor: {s}");
        assert!(s.contains("phase-name"),   "expected 'phase-name': {s}");
        assert!(s.contains("scope-coords"), "expected 'scope-coords': {s}");
        assert!(s.contains("idx/total"),    "expected seq descriptor: {s}");
        assert!(s.contains("progress%"),    "expected 'progress%': {s}");
        assert!(s.contains("throughput"),   "expected 'throughput': {s}");
        assert!(s.contains("ok:ok%"),       "expected ok descriptor: {s}");
        assert!(s.contains("(elapsed)"),    "expected '(elapsed)': {s}");
    }

    #[test]
    fn err_color_promotes_when_errors_or_retries() {
        let ctx = TestCtx {
            phase_name: "run".into(),
            phase_seq: Some((1, 1)),
            cycles_completed: 10,
            cycles_total: 10,
            ops_ok: 9,
            errors: 1,
            retries: 0,
            concurrency: 1,
            elapsed_secs: 1.0,
            consumed: 10,
            use_color: true,
            ..Default::default()
        };
        let out = render(&ctx);
        // Yellow used (errors > 0) — confirm the ANSI code
        // sequence appears around the `e:` chunk. Tail line
        // carries the counters under the new two-line layout.
        assert!(out.contains("\x1b[33me:1 r:0\x1b[0m"),
            "expected yellow err_color around `e:1 r:0`, got: {out:?}");
        assert!(out.contains('\n'),
            "expected two-line break in labeled render: {out:?}");
    }
}
