// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `phase_status` — the live-status readout.
//!
//! Renders the inline-progress line that the activity's
//! refresh thread emits via `\r\x1b[K…` every 0.5 s.
//! Push 2 byte-equivalence target — the format string the
//! prior implementation used:
//!
//! ```text
//! {depth_indent}{cyan}{spinner}{reset}{bar} {seq_prefix}{activity_name} \
//!   {pct:.0}% {rate_str} ok:{ok_pct:.0}% e:{errors} r:{retries} c:{concurrency}\
//!   {adapter_status}{batch_info}{relevancy_str}{eta}
//! ```
//!
//! Width clamping (`truncate_to_width`) is the surface's
//! job — see the inline-status driver in `nbrs-activity::activity`.
//! Other LODs and the explanation overlay render zero bytes
//! in Push 2; Push 5 (`Lod::Expanded`) and Push 7
//! (`ContentMode::Explanation`) fill them in.

use std::fmt::Write as _;

use crate::readouts::buf::ReadoutBuf;
use crate::readouts::context::{ReadoutContext, SubjectKind};
use crate::readouts::format::{braille_bar, format_eta, format_rate, spinner_frame};
use crate::readouts::readout::{ContentMode, Lod, Readout, ReadoutOptions};

pub struct PhaseStatus;

impl Readout for PhaseStatus {
    fn name(&self) -> &'static str { "phase_status" }
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
            (Lod::Compact,  ContentMode::Explanation) => render_compact_explanation(ctx, out),
            (Lod::Labeled,  ContentMode::Explanation) => render_labeled_explanation(ctx, out),
            (Lod::Expanded, ContentMode::Explanation) => render_expanded_explanation(ctx, out),
        }
    }
}

/// Compact LOD explanation overlay. Same shape as
/// `render_compact` (`{spinner} {pct}% {rate}`); each
/// token replaced with a meaning descriptor.
fn render_compact_explanation(
    _ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let s = "spin progress% rate/s";
    let _ = out.write_str(s);
    s.len()
}

/// Labeled LOD explanation overlay. Same shape as the
/// labeled value form — spinner + bar + name + counters
/// + ETA.
fn render_labeled_explanation(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let coords = if ctx.subject_labels().is_empty() {
        ""
    } else {
        " (scope-coords)"
    };
    let mut tmp = String::with_capacity(160);
    let _ = write!(
        &mut tmp,
        "spin (bar) [phase-name]{coords} \
progress% throughput ok:ok% e:errors r:retries c:concurrency \
(metrics) ETA remaining",
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Expanded LOD explanation overlay. Multi-line block,
/// same shape as `render_expanded` — one descriptor per
/// row.
fn render_expanded_explanation(
    _ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let s = "\
spin [phase-name]\n  \
progress:   progress% (bar)  ETA remaining\n  \
throughput: throughput  ok:ok%\n  \
counters:   e:errors r:retries c:concurrency\n  \
adapter:    adapter-counters\n  \
batch:      batch-info\n  \
metrics:    workload-emphasised metrics";
    let _ = out.write_str(s);
    s.len()
}

fn render_labeled(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let cyan  = if color { "\x1b[36m" } else { "" };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };

    let total_extent = ctx.cycles_total();
    let started      = ctx.ops_started();
    let finished     = ctx.ops_finished();
    let ops_completed= ctx.cycles_completed();
    let successes    = ctx.ops_ok();
    let errors       = ctx.errors();
    let retries      = ctx.retries();
    let elapsed      = ctx.elapsed_secs();
    let concurrency  = ctx.concurrency();

    let pct: f64 = if total_extent > 0 {
        started as f64 * 100.0 / total_extent as f64
    } else {
        0.0
    };
    let ok_pct: f64 = if ops_completed > 0 {
        successes as f64 * 100.0 / ops_completed as f64
    } else {
        100.0
    };
    let rate: f64 = if elapsed > 0.0 { finished as f64 / elapsed } else { 0.0 };
    let rate_str = format_rate(rate);

    let spinner = spinner_frame(ctx.refresh_tick());
    let bar = if total_extent > 0 {
        format!(" {dim}{}{reset}", braille_bar(pct, 10))
    } else {
        String::new()
    };
    // SRD-63 Push 9f: read ETA from the context's accessor
    // rather than recomputing from `finished`/`rate`. Falls
    // back to the inline derivation when the context returns
    // `None` so contexts that haven't been updated to populate
    // `eta_secs` (mock test contexts) keep working.
    let eta = match ctx.eta_secs() {
        Some(secs) => format!(" {dim}ETA {}{reset}", format_eta(secs)),
        None if total_extent > 0 && rate > 0.0 => {
            let remaining = total_extent.saturating_sub(finished) as f64;
            format!(" {dim}ETA {}{reset}", format_eta(remaining / rate))
        }
        None => String::new(),
    };

    let seq_prefix: String = match ctx.subject_seq() {
        Some((s, t)) => format!("[{s}/{t}] "),
        None => String::new(),
    };
    let depth_indent = ctx.depth_indent();
    let activity_name = ctx.activity_name();
    let chips = ctx.status_metric_chips();
    let adapter_status = ctx.adapter_counters_text();
    let batch_info = ctx.batch_info_text();

    let mut tmp = String::with_capacity(192);
    let _ = write!(
        &mut tmp,
        "{depth_indent}{cyan}{spinner}{reset}{bar} {seq_prefix}{activity_name} \
         {pct:.0}% {rate_str} ok:{ok_pct:.0}% e:{errors} r:{retries} c:{concurrency}\
{adapter_status}{batch_info}{chips}{eta}",
    );
    // The prior implementation lived inside an indented
    // `format!` macro: Rust's macro multi-line continuation
    // collapses leading whitespace after `\` to a single
    // space. Reproduce that here so the output is byte-
    // identical to the prior eprint!.
    // (The format! string above is intentionally laid out
    // so that there are no `\` continuations — every space
    // is in the literal payload, and chip / adapter /
    // batch / eta concatenate without inserted whitespace.)
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Expanded LOD: each field on its own line. Block-rendered
/// (the binder's layout classification picks `Block` for
/// expanded automatically). SRD-63 §3.3 monotonicity:
/// every field present at Labeled is present here too;
/// new fields are the explicit per-aggregate breakdowns.
fn render_expanded(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let color = ctx.use_color();
    let dim   = if color { "\x1b[2m"  } else { "" };
    let cyan  = if color { "\x1b[36m" } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };

    let total_extent = ctx.cycles_total();
    let started      = ctx.ops_started();
    let finished     = ctx.ops_finished();
    let ops_completed= ctx.cycles_completed();
    let successes    = ctx.ops_ok();
    let errors       = ctx.errors();
    let retries      = ctx.retries();
    let elapsed      = ctx.elapsed_secs();
    let concurrency  = ctx.concurrency();

    let pct: f64 = if total_extent > 0 {
        started as f64 * 100.0 / total_extent as f64
    } else { 0.0 };
    let ok_pct: f64 = if ops_completed > 0 {
        successes as f64 * 100.0 / ops_completed as f64
    } else { 100.0 };
    let rate: f64 = if elapsed > 0.0 { finished as f64 / elapsed } else { 0.0 };
    let rate_str = format_rate(rate);
    let bar = if total_extent > 0 { braille_bar(pct, 20) } else { String::new() };
    // Push 9f: prefer `ctx.eta_secs()`; fall back to the
    // inline derivation when the context doesn't supply one.
    let eta = match ctx.eta_secs() {
        Some(secs) => format!("ETA {}", format_eta(secs)),
        None if total_extent > 0 && rate > 0.0 => {
            let remaining = total_extent.saturating_sub(finished) as f64;
            format!("ETA {}", format_eta(remaining / rate))
        }
        None => String::from("ETA —"),
    };

    let activity_name = ctx.activity_name();
    let chips = ctx.status_metric_chips();
    let adapter_status = ctx.adapter_counters_text();
    let batch_info = ctx.batch_info_text();
    let seq_prefix: String = match ctx.subject_seq() {
        Some((s, t)) => format!("[{s}/{t}] "),
        None => String::new(),
    };

    let mut tmp = String::with_capacity(384);
    let _ = write!(
        &mut tmp,
        "{cyan}{spinner}{reset} {seq_prefix}{activity_name}\n  \
         progress:   {pct:.0}% {dim}{bar}{reset}  {eta}\n  \
         throughput: {rate_str}  ok:{ok_pct:.0}%\n  \
         counters:   e:{errors} r:{retries} c:{concurrency}",
        spinner = spinner_frame(ctx.refresh_tick()),
    );
    if !adapter_status.is_empty() {
        let _ = write!(&mut tmp, "\n  adapter:   {adapter_status}");
    }
    if !batch_info.is_empty() {
        let _ = write!(&mut tmp, "\n  batch:     {batch_info}");
    }
    if !chips.is_empty() {
        let _ = write!(&mut tmp, "\n  metrics:   {chips}");
    }
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

/// Compact LOD: a stripped-down one-token cluster. Per
/// SRD-63 §3.3's monotonicity invariant this is a strict
/// subset of `Labeled`. Used by the TUI tree row at the
/// default LOD setting (see Push 5).
fn render_compact(
    ctx: &dyn ReadoutContext,
    out: &mut dyn ReadoutBuf,
) -> usize {
    let total_extent = ctx.cycles_total();
    let started      = ctx.ops_started();
    let finished     = ctx.ops_finished();
    let elapsed      = ctx.elapsed_secs();
    let pct: f64 = if total_extent > 0 {
        started as f64 * 100.0 / total_extent as f64
    } else {
        0.0
    };
    let rate: f64 = if elapsed > 0.0 { finished as f64 / elapsed } else { 0.0 };
    let mut tmp = String::with_capacity(32);
    let _ = write!(
        &mut tmp,
        "{spin} {pct:.0}% {rate}",
        spin = spinner_frame(ctx.refresh_tick()),
        pct = pct,
        rate = format_rate(rate),
    );
    let len = tmp.len();
    let _ = out.write_str(&tmp);
    len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readouts::Event;
    use crate::readouts::buf::StringBuf;

    #[derive(Default)]
    struct TestCtx {
        phase_name: String,
        activity_name: String,
        phase_seq: Option<(usize, usize)>,
        cycles_completed: u64,
        cycles_total: u64,
        ops_started: u64,
        ops_finished: u64,
        ops_ok: u64,
        errors: u64,
        retries: u64,
        concurrency: usize,
        elapsed_secs: f64,
        consumed: u64,
        chips: String,
        adapter: String,
        batch: String,
        depth_indent: String,
        refresh_tick: u64,
        use_color: bool,
    }

    impl ReadoutContext for TestCtx {
        fn subject_name(&self) -> &str { &self.phase_name }
        fn activity_name(&self) -> &str {
            if self.activity_name.is_empty() { &self.phase_name }
            else { &self.activity_name }
        }
        fn subject_seq(&self) -> Option<(usize, usize)> { self.phase_seq }
        fn subject_labels(&self) -> &str { "" }
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
        fn status_metric_chips(&self) -> String { self.chips.clone() }
        fn adapter_counters_text(&self) -> String { self.adapter.clone() }
        fn batch_info_text(&self) -> String { self.batch.clone() }
        fn depth_indent(&self) -> &str { &self.depth_indent }
        fn use_color(&self) -> bool { self.use_color }
        fn event(&self) -> Event { Event::Update }
        fn refresh_tick(&self) -> u64 { self.refresh_tick }
    }

    fn render(ctx: &TestCtx, lod: Lod) -> String {
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        PhaseStatus.render(
            ctx, lod, ContentMode::Value,
            &ReadoutOptions::new(), &mut buf,
        );
        s
    }

    #[test]
    fn labeled_no_color_minimal() {
        let ctx = TestCtx {
            phase_name: "run".into(),
            activity_name: "run".into(),
            phase_seq: Some((1, 1)),
            cycles_completed: 50,
            cycles_total: 100,
            ops_started: 50,
            ops_finished: 50,
            ops_ok: 50,
            errors: 0,
            retries: 0,
            concurrency: 1,
            elapsed_secs: 1.0,
            consumed: 50,
            refresh_tick: 0,
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        // Spinner frame at tick=0 is ⠋. Pct 50%. Rate 50/s.
        assert!(out.starts_with("⠋"),
            "spinner frame missing: {out}");
        assert!(out.contains(" 50% 50/s ok:100% e:0 r:0 c:1"),
            "labeled body wrong: {out}");
        assert!(out.contains(" ETA "),
            "ETA missing for finite-rate phase: {out}");
    }

    #[test]
    fn labeled_no_eta_when_no_extent() {
        let ctx = TestCtx {
            phase_name: "x".into(),
            activity_name: "x".into(),
            cycles_total: 0,
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        assert!(!out.contains("ETA "),
            "ETA should be suppressed when cycles_total=0: {out}");
    }

    #[test]
    fn labeled_chips_and_adapter_and_batch() {
        let ctx = TestCtx {
            phase_name: "run".into(),
            activity_name: "run".into(),
            phase_seq: Some((1, 1)),
            cycles_completed: 100,
            cycles_total: 100,
            ops_started: 100,
            ops_finished: 100,
            ops_ok: 100,
            concurrency: 4,
            elapsed_secs: 1.0,
            consumed: 100,
            chips: " recall_at_10:80.00%".into(),
            adapter: " rows/s=12.5K".into(),
            batch: " r/b=12.5".into(),
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        assert!(out.contains(" rows/s=12.5K r/b=12.5 recall_at_10:80.00%"),
            "adapter / batch / chips ordering wrong: {out}");
    }

    #[test]
    fn compact_is_short_and_starts_with_spinner() {
        let ctx = TestCtx {
            phase_name: "x".into(),
            cycles_total: 10,
            ops_started: 5,
            ops_finished: 5,
            elapsed_secs: 1.0,
            ..Default::default()
        };
        let out = render(&ctx, Lod::Compact);
        assert!(out.starts_with("⠋"),
            "compact missing spinner: {out}");
        assert_eq!(out, "⠋ 50% 5/s");
    }

    #[test]
    fn explanation_mode_renders_descriptors_at_every_lod() {
        // SRD-63 §3.2 / Push 7: Explanation overlay has a
        // descriptor for every LOD. Width-parity with the
        // value render is the author's contract.
        let ctx = TestCtx {
            phase_name: "x".into(),
            cycles_total: 100,
            ops_started: 50,
            ops_finished: 50,
            elapsed_secs: 1.0,
            ..Default::default()
        };
        for lod in [Lod::Compact, Lod::Labeled, Lod::Expanded] {
            let mut s = String::new();
            let mut buf = StringBuf::new(&mut s);
            let n = PhaseStatus.render(
                &ctx, lod, ContentMode::Explanation,
                &ReadoutOptions::new(), &mut buf,
            );
            assert!(n > 0, "{lod:?}/Explanation should render");
            assert!(s.contains("progress"),
                "{lod:?}/Explanation missing 'progress' descriptor: {s}");
        }
    }

    #[test]
    fn expanded_renders_multi_line_block() {
        let ctx = TestCtx {
            phase_name: "run".into(),
            activity_name: "run".into(),
            phase_seq: Some((1, 1)),
            cycles_completed: 100,
            cycles_total: 200,
            ops_started: 100,
            ops_finished: 100,
            ops_ok: 100,
            concurrency: 4,
            elapsed_secs: 1.0,
            consumed: 100,
            chips: " recall_at_10:80.00%".into(),
            adapter: " rows/s=12.5K".into(),
            ..Default::default()
        };
        let out = render(&ctx, Lod::Expanded);
        // Expanded renders multi-line: progress, throughput,
        // counters at minimum. Adapter / metrics tails when
        // present.
        assert!(out.contains("progress:"),    "expanded missing 'progress:': {out}");
        assert!(out.contains("throughput:"),  "expanded missing 'throughput:': {out}");
        assert!(out.contains("counters:"),    "expanded missing 'counters:': {out}");
        assert!(out.contains("adapter:"),     "expanded missing 'adapter:' tail: {out}");
        assert!(out.contains("metrics:"),     "expanded missing 'metrics:' tail: {out}");
        assert!(out.lines().count() >= 5,     "expanded should be multi-line: {out}");
    }

    #[test]
    fn refresh_tick_advances_spinner_frame() {
        let mut ctx = TestCtx {
            phase_name: "x".into(),
            cycles_total: 10,
            ops_started: 1,
            ops_finished: 1,
            elapsed_secs: 1.0,
            ..Default::default()
        };
        let mut frames = std::collections::HashSet::new();
        for tick in 0..10 {
            ctx.refresh_tick = tick;
            let out = render(&ctx, Lod::Compact);
            // First non-empty char is the spinner.
            let first = out.chars().next().unwrap();
            frames.insert(first);
        }
        assert_eq!(frames.len(), 10, "spinner cycle not 10 distinct frames");
    }
}
