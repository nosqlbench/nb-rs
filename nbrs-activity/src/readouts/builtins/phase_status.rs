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
    // Palette per docs/guide/color_style.md:
    //   spinner → cyan (motion cue)
    //   activity name → bold + INFO (sky/blue)
    //   bar / rate / ok% / c: / ETA → MUTED (dim)
    //   pct → default (the headline number; not styled)
    //   e:N r:N → WARN (yellow) when >0, MUTED when 0
    //   memo header → EMPHASIS (bold yellow) — sits above
    let color = ctx.use_color();
    let cyan   = if color { "\x1b[36m"   } else { "" };
    let dim    = if color { "\x1b[2m"    } else { "" };
    let bold   = if color { "\x1b[1m"    } else { "" };
    let blue   = if color { "\x1b[34m"   } else { "" };
    let yellow = if color { "\x1b[33m"   } else { "" };
    let reset  = if color { "\x1b[0m"    } else { "" };

    let total_extent = ctx.cycles_total();
    let _started     = ctx.ops_started();
    let finished     = ctx.ops_finished();
    let ops_completed= ctx.cycles_completed();
    let successes    = ctx.ops_ok();
    let errors       = ctx.errors();
    let retries      = ctx.retries();
    let elapsed      = ctx.elapsed_secs();
    let concurrency  = ctx.concurrency();

    // Progress percentage uses *completed* cycles, not
    // dispatched ones. The previous `ops_started`-based
    // formula reported 100% the moment the only fiber
    // dispatched its sole op — for long synchronous calls
    // (jolokia_compact, schema migrations) the bar pinned at
    // 100% for the whole wait. `cycles_completed` matches
    // what `phase_done` reports and what rate / ETA derive
    // from, so the running bar and the final DONE line agree.
    let pct: f64 = if total_extent > 0 {
        ops_completed as f64 * 100.0 / total_extent as f64
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
    // Bar styling: bright-white braille dots on a dark-grey
    // truecolor background. The background makes the empty
    // leading cells visible as a defined region instead of
    // a gap — so an early-phase 5% bar reads as `▮▯▯▯▯▯▯▯▯▯`
    // rather than `▮          ` (where the trailing cells
    // were braille blanks against the terminal default
    // background).
    let bar = if total_extent > 0 {
        let bg = if color { "\x1b[48;2;50;50;50m" } else { "" };
        let fg = if color { "\x1b[97m"            } else { "" };
        format!(" {bg}{fg}{}{reset}", braille_bar(pct, 10))
    } else {
        String::new()
    };
    // Time span: cumulative elapsed / ETA remaining, packed
    // into a single dim parenthesised pair. The slash reads
    // as past→future without needing a label. When ETA can't
    // be computed (no extent / no progress) the span
    // degenerates to just elapsed.
    let elapsed_str = format_eta(elapsed);
    let eta = match ctx.eta_secs() {
        Some(secs) =>
            format!(" {dim}({elapsed_str}/{}){reset}", format_eta(secs)),
        None if total_extent > 0 && rate > 0.0 => {
            let remaining = total_extent.saturating_sub(finished) as f64;
            format!(" {dim}({elapsed_str}/{}){reset}", format_eta(remaining / rate))
        }
        None =>
            format!(" {dim}({elapsed_str}){reset}"),
    };

    let seq_prefix: String = match ctx.subject_seq() {
        Some((s, t)) => format!("{dim}[{s}/{t}]{reset} "),
        None => String::new(),
    };
    let depth_indent = ctx.depth_indent();
    let activity_name = ctx.activity_name();
    let chips = ctx.status_metric_chips();
    let adapter_status = ctx.adapter_counters_text();
    let batch_info = ctx.batch_info_text();

    // Counters tone follows the rule from phase_done: yellow
    // when something abnormal (errors/retries > 0), dim when
    // clean. ok% gets the same treatment so a 100% / 99%
    // distinction reads at a glance.
    let err_tone   = if errors > 0 || retries > 0 { yellow } else { dim };
    let ok_tone    = if ok_pct >= 100.0 { dim } else { yellow };

    // Memo header (if any): operator-visible state string
    // published by the `memo:` wrapper. Sits ABOVE the regular
    // status line in EMPHASIS color so it's hard to miss.
    let memo = ctx.phase_memo();
    let memo_header = if memo.is_empty() {
        String::new()
    } else {
        let bold_yellow = if color { "\x1b[1;33m" } else { "" };
        format!("{depth_indent}{bold_yellow}[[ {memo} ]]{reset}\n")
    };

    // Two-line layout: break after the progress percentage so
    // the head line stays narrow (spinner/bar/name/pct) and
    // the tail line carries the counters and emphasized
    // metrics. Indentation on the second line aligns roughly
    // under the activity name. The surface sink
    // (`LogOnlySink`) handles multi-line region clearing.
    let mut tmp = String::with_capacity(320);
    let _ = write!(
        &mut tmp,
        "{memo_header}\
{depth_indent}{cyan}{spinner}{reset}{bar} {seq_prefix}{bold}{blue}{activity_name}{reset} {pct:.0}%\n\
{depth_indent}    {dim}{rate_str}{reset} {ok_tone}ok:{ok_pct:.0}%{reset} \
{err_tone}e:{errors} r:{retries}{reset} {dim}c:{concurrency}{reset}\
{adapter_status}{batch_info}{chips}{eta}",
    );
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
    let _started     = ctx.ops_started();
    let finished     = ctx.ops_finished();
    let ops_completed= ctx.cycles_completed();
    let successes    = ctx.ops_ok();
    let errors       = ctx.errors();
    let retries      = ctx.retries();
    let elapsed      = ctx.elapsed_secs();
    let concurrency  = ctx.concurrency();

    // See `render_labeled` — pct must use completed cycles so
    // dispatched-but-not-yet-returned ops don't pin the bar
    // at 100% during long synchronous waits.
    let pct: f64 = if total_extent > 0 {
        ops_completed as f64 * 100.0 / total_extent as f64
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
    let finished     = ctx.ops_finished();
    let ops_completed= ctx.cycles_completed();
    let elapsed      = ctx.elapsed_secs();
    // Pct from completed cycles — see `render_labeled`.
    let pct: f64 = if total_extent > 0 {
        ops_completed as f64 * 100.0 / total_extent as f64
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
        // Two-line layout: head ends with " 50%\n",
        // tail begins with the indented counters.
        assert!(out.contains(" 50%\n"),
            "two-line break after pct missing: {out:?}");
        assert!(out.contains("50/s ok:100% e:0 r:0 c:1"),
            "labeled body wrong: {out:?}");
        // Time span: `(elapsed/eta)`. Both 1s here (elapsed=1,
        // remaining=cycles_total/rate=50/50=1s).
        assert!(out.contains("(1s/1s)"),
            "elapsed/ETA span missing for finite-rate phase: {out:?}");
    }

    #[test]
    fn memo_header_renders_above_status_when_non_empty() {
        // Memo wrapper publishes "compacting tableX"; the
        // status readout must surface it as
        // `[[ compacting tableX ]]` on its own line above the
        // regular two-line body. Empty memo (default) renders
        // nothing extra (the other tests guard that path).
        struct MemoCtx;
        impl ReadoutContext for MemoCtx {
            fn subject_name(&self) -> &str { "x" }
            fn activity_name(&self) -> &str { "x" }
            fn subject_seq(&self) -> Option<(usize, usize)> { None }
            fn subject_labels(&self) -> &str { "" }
            fn cycles_completed(&self) -> u64 { 1 }
            fn cycles_total(&self) -> u64 { 1 }
            fn ops_started(&self) -> u64 { 1 }
            fn ops_finished(&self) -> u64 { 1 }
            fn ops_ok(&self) -> u64 { 1 }
            fn errors(&self) -> u64 { 0 }
            fn retries(&self) -> u64 { 0 }
            fn concurrency(&self) -> usize { 1 }
            fn elapsed_secs(&self) -> f64 { 1.0 }
            fn consumed(&self) -> u64 { 1 }
            fn status_metric_chips(&self) -> String { String::new() }
            fn depth_indent(&self) -> &str { "" }
            fn use_color(&self) -> bool { false }
            fn event(&self) -> Event { Event::Update }
            fn refresh_tick(&self) -> u64 { 0 }
            fn phase_memo(&self) -> &str { "compacting tableX" }
        }
        let ctx = MemoCtx;
        let mut s = String::new();
        let mut buf = StringBuf::new(&mut s);
        PhaseStatus.render(
            &ctx, Lod::Labeled, ContentMode::Value,
            &ReadoutOptions::new(), &mut buf,
        );
        assert!(s.starts_with("[[ compacting tableX ]]\n"),
            "memo header must lead the output, got: {s:?}");
        // Body still present below the header.
        assert!(s.contains("100%"),
            "regular status body missing: {s:?}");
    }

    #[test]
    fn labeled_no_eta_when_no_extent() {
        // When cycles_total=0 there's no `/ETA` half of the
        // span; the time pair degenerates to elapsed-only —
        // `(0s)` here since the test fixture has elapsed=0.
        // The slash MUST NOT appear in this branch.
        let ctx = TestCtx {
            phase_name: "x".into(),
            activity_name: "x".into(),
            cycles_total: 0,
            ..Default::default()
        };
        let out = render(&ctx, Lod::Labeled);
        // The time span itself collapses to elapsed-only;
        // `(0s)` appears, but no `0s/...` ETA half.
        assert!(out.contains("(0s)"),
            "elapsed-only time span missing: {out}");
        assert!(!out.contains("0s/"),
            "ETA half should be suppressed when cycles_total=0: {out}");
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
        // Pct is driven by cycles_completed (completed cycles),
        // NOT ops_started — dispatched-but-not-returned ops
        // don't count toward the displayed percentage.
        let ctx = TestCtx {
            phase_name: "x".into(),
            cycles_total: 10,
            cycles_completed: 5,
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
    fn pct_uses_completed_not_started() {
        // Regression guard for the off-by-one bug: a single
        // long-running op is dispatched (ops_started=1) but
        // hasn't returned yet (ops_finished=0,
        // cycles_completed=0). Pct must read 0%, not 100%.
        let ctx = TestCtx {
            phase_name: "x".into(),
            cycles_total: 1,
            cycles_completed: 0,
            ops_started: 1,
            ops_finished: 0,
            elapsed_secs: 5.0,
            ..Default::default()
        };
        let labeled = render(&ctx, Lod::Labeled);
        assert!(labeled.contains(" 0%\n"),
            "in-flight op should read 0%, not 100%: {labeled:?}");
        let compact = render(&ctx, Lod::Compact);
        assert!(compact.contains(" 0% "),
            "compact in-flight should also read 0%: {compact:?}");
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
