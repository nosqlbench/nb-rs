// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `LogOnlySink` — `DisplaySink` impl that renders every
//! log entry to stderr.
//!
//! ## Role
//!
//! In Phase 2 of the display-sink refactor (see
//! [`crate::display_sink`]) this sink is the canonical renderer
//! for `tui=off` mode. The
//! [`crate::log_only_observer::LogOnlyObserver`] sends every
//! log event to the actor; this sink polls the actor's published
//! [`crate::state::RunState`] snapshot, drains anything new from
//! the log ring (tracked via
//! [`crate::state::RunState::log_seq_total`]), applies the
//! observer-supplied severity filter, and emits stderr lines
//! identical to the legacy `StderrObserver` path.
//!
//! ## Coordination with the observer
//!
//! The sink takes a shared `sink_active` flag from the observer
//! it's paired with (see
//! [`crate::log_only_observer::LogOnlyObserver::sink_active_flag`]).
//! The startup sequence is:
//!
//! 1. `LogOnlySink::new` records `last_seen_seq = 0` (provisional).
//! 2. `start()` reads the actor's current snapshot, sets
//!    `last_seen_seq = snapshot.log_seq_total` so any pre-sink
//!    entries already on stderr (from the observer's synchronous
//!    write) aren't re-rendered.
//! 3. `start()` sets `sink_active = true` — from this moment the
//!    observer suppresses its own stderr writes and the sink owns
//!    the surface.
//! 4. The render thread polls every ~50 ms, drains
//!    `(log_seq_total - last_seen_seq)` entries off the tail of
//!    `log_messages`, prints those that pass `min_level`, and
//!    advances `last_seen_seq`.
//!
//! `shutdown()` clears `sink_active` so the observer resumes
//! synchronous writes for any straggler logs that fire after the
//! sink is gone.
//!
//! ## Polling cadence
//!
//! 50 ms. Fast enough that a human operator doesn't perceive lag
//! between an event and its line appearing; slow enough that
//! idle ticks have negligible CPU cost. The cadence reporter's
//! frame channel is also drained on the same loop so it never
//! reports a full / disconnected channel.
//!
//! ## Drop-on-overflow
//!
//! If `(log_seq_total - last_seen_seq)` exceeds the ring's
//! capacity (200), the sink lost some entries — i.e. the
//! observer logged faster than the sink could drain. The
//! diagnostic notes the count and continues; the dropped lines
//! are still in `session.log` (the async sink in
//! `nbrs_runtime::log_sink` takes every level unconditionally,
//! see SRD 02 §"Display and Diagnostic Decoupling").

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use nbrs_runtime::observer::LogLevel;

use crate::display_sink::{DisplayInputs, DisplaySink, SinkHandle};
use crate::key_watcher::WatcherSignal;
use crate::prompt_state::{PromptAction, PromptState};
use crate::state::LogSeverity;

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const LOG_RING_CAPACITY: u64 = 200;

/// Sentinel `resume_from` value meaning "fresh start": the sink
/// seeds `last_seen` from the live `log_seq_total` so it doesn't
/// re-emit anything the observer already printed. Any other value
/// is a swap re-entry — the sink resumes from that exact seq so the
/// log lines that scrolled while the TUI owned the alternate screen
/// are re-printed into the restored scrollback (terminal history IS
/// the log stream, reliable across mode swaps, with no managed
/// screen-buffer to reconstruct).
pub(crate) const RESUME_FRESH: u64 = u64::MAX;

/// A fresh cross-swap resume cursor to share across a sink's
/// lifetime — starts at [`RESUME_FRESH`] so the first sink seeds
/// from the live log position and each post-swap sink resumes from
/// the prior sink's final cursor. Used by the supervisor and the
/// `tui_display_harness` example (which simulates a Ctrl-T swap by
/// tearing a sink down and starting a fresh one on the same cursor).
/// See [`LogOnlySink::with_resume`].
pub fn fresh_resume_cursor() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(RESUME_FRESH))
}

/// `tui=off` log-stream sink.
pub struct LogOnlySink {
    /// Severity floor. The paired observer's filter, propagated
    /// here so pre-sink and post-sink stderr output use the same
    /// rule. Lines below `min_level` are silently dropped (still
    /// captured by the async session-log sink).
    min_level: LogLevel,
    /// Coordination flag shared with
    /// [`crate::log_only_observer::LogOnlyObserver`]. The sink
    /// flips it `true` on `start`, `false` on `shutdown`.
    sink_active: Arc<AtomicBool>,
    /// Channel from the supervisor's key forwarding. When
    /// present, the sink renders an interactive prompt at the
    /// bottom of the surface and dispatches Enter-submitted
    /// commands to the inspector handler. `None` falls back to
    /// the legacy "log stream + status region" layout used by
    /// piped / CI / no-tty runs.
    key_rx: Option<mpsc::Receiver<WatcherSignal>>,
    /// Tokio runtime handle threaded through to the inspector
    /// dispatcher so prompt-submitted `set <name> <value>`
    /// commands can write controls. `None` disables the `set`
    /// command at the prompt (everything else still works).
    runtime: Option<tokio::runtime::Handle>,
    /// Cross-swap log cursor shared with the supervisor. The sink
    /// seeds `last_seen` from it on `start` (unless it holds
    /// [`RESUME_FRESH`]) and writes its final `last_seen` back to it
    /// on shutdown, so the next terminal-mode sink the supervisor
    /// brings up after a TUI swap re-emits exactly the lines that
    /// scrolled while the alternate screen was up. `None` for
    /// standalone (non-supervised) sinks, which always start fresh.
    resume_from: Option<Arc<AtomicU64>>,
}

impl LogOnlySink {
    pub fn new(min_level: LogLevel, sink_active: Arc<AtomicBool>) -> Self {
        Self {
            min_level,
            sink_active,
            key_rx: None,
            runtime: None,
            resume_from: None,
        }
    }

    /// Share a cross-swap log cursor with the supervisor so this
    /// sink re-emits the lines that scrolled while a TUI swap held
    /// the alternate screen, instead of skipping them (see
    /// [`RESUME_FRESH`]).
    pub fn with_resume(mut self, resume_from: Arc<AtomicU64>) -> Self {
        self.resume_from = Some(resume_from);
        self
    }

    /// Wire an interactive prompt into this sink. The receiver
    /// will be drained on every render tick; each
    /// `WatcherSignal::Key(...)` feeds the [`PromptState`]'s
    /// keymap and the resize / help / interrupt chords adjust
    /// layout directly. Without a key receiver the sink runs
    /// in its legacy log-only mode.
    pub fn with_keys(
        mut self,
        key_rx: mpsc::Receiver<WatcherSignal>,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        self.key_rx = Some(key_rx);
        self.runtime = runtime;
        self
    }
}

/// Choose the initial log cursor for a (re)started terminal-mode
/// sink. With no shared cursor, or one still holding
/// [`RESUME_FRESH`], the sink seeds from the live `current`
/// `log_seq_total` so it skips history the observer already
/// printed. Any other shared value is a swap re-entry: the sink
/// resumes from that exact prior cursor so the lines that scrolled
/// while the TUI's alternate screen was up get re-emitted into the
/// restored scrollback.
fn seed_last_seen(resume: Option<u64>, current: u64) -> u64 {
    match resume {
        Some(v) if v != RESUME_FRESH => v,
        _ => current,
    }
}

fn severity_to_level(s: LogSeverity) -> LogLevel {
    match s {
        LogSeverity::Debug => LogLevel::Debug,
        LogSeverity::Info  => LogLevel::Info,
        LogSeverity::Warn  => LogLevel::Warn,
        LogSeverity::Error => LogLevel::Error,
    }
}

impl DisplaySink for LogOnlySink {
    fn start(self: Box<Self>, inputs: DisplayInputs) -> Box<dyn SinkHandle> {
        let DisplayInputs { state, frame_rx, metrics_query: _ } = inputs;
        let LogOnlySink { min_level, sink_active, key_rx, runtime, resume_from } = *self;

        // Seed the log cursor. A fresh sink snapshots the current
        // `log_seq_total` so it doesn't re-emit anything the observer
        // already printed pre-flag. A swap re-entry (the supervisor's
        // `resume_from` holds the prior sink's final cursor, not
        // `RESUME_FRESH`) resumes from that exact seq so the lines
        // that scrolled under the TUI's alternate screen are
        // re-printed into the restored scrollback.
        let initial_seq = seed_last_seen(
            resume_from.as_ref().map(|a| a.load(Ordering::Acquire)),
            state.load().log_seq_total,
        );
        sink_active.store(true, Ordering::Release);

        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let sink_active_for_thread = sink_active.clone();
        let state_for_thread = state.clone();

        let join = std::thread::Builder::new()
            .name("log-only-sink".into())
            .spawn(move || {
                run_render_loop(
                    state_for_thread,
                    frame_rx,
                    initial_seq,
                    min_level,
                    stop_for_thread,
                    key_rx,
                    runtime,
                    resume_from,
                );
                // Render thread exited (stop signaled or channel
                // disconnected). Clear the flag so the observer
                // resumes synchronous writes.
                sink_active_for_thread.store(false, Ordering::Release);
            })
            .expect("spawn log-only-sink thread");

        Box::new(LogOnlySinkHandle {
            stop,
            join: Some(join),
            sink_active,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn run_render_loop(
    state: crate::run_state_actor::RunStateHandle,
    frame_rx: Option<std::sync::mpsc::Receiver<nbrs_metrics::snapshot::MetricSet>>,
    mut last_seen: u64,
    min_level: LogLevel,
    stop: Arc<AtomicBool>,
    key_rx: Option<mpsc::Receiver<WatcherSignal>>,
    runtime: Option<tokio::runtime::Handle>,
    resume_from: Option<Arc<AtomicU64>>,
) {
    let mut stderr = io::stderr();
    // The raw status string most recently published by the
    // actor and reflected on the terminal. We compare against
    // *this* (not the clamped form actually written to the
    // surface) so identity checks are stable across ticks.
    let mut status_published: Option<String> = None;
    // True until this sink's first footer paint commits. The
    // return-to-footer-top + clear in step (A) must NOT run on the
    // first paint: no footer is drawn yet and the cursor sits just
    // after the last emitted log (or on a freshly restored surface),
    // so the first paint just emits logs and draws the footer in
    // place. On a fresh surface there is nothing above the cursor to
    // clear; clearing to end-of-screen from a higher cursor would
    // wipe live content.
    let mut first_paint = true;
    // Follow-the-log sticky footer: rows the cursor sits BELOW the
    // footer's first row after a committed redraw (the prompt input
    // row when an inline bar is shown, else the last status row). The
    // next tick climbs back up by this much to reach the footer top
    // before clearing it. The terminal's `?1049` cursor save/restore
    // keeps this valid across a console alt-screen excursion, so it
    // needs no separate save/restore.
    let mut footer_return_up: u16 = 0;
    // nb-shell prompt — owned by the render thread when a key
    // channel is wired in. Tracks line buffer, history, window
    // rows, and help overlay. `None` falls back to the legacy
    // log-only behaviour.
    let mut prompt: Option<PromptState> = key_rx.as_ref().map(|_| PromptState::new());
    let mut prompt_dirty = prompt.is_some();
    // Tracks the REPL visibility that the last redraw committed
    // to. Drives a redraw on the tick after a `~` / `Ctrl-~`
    // toggle so the prompt show/hide takes effect promptly.
    let mut repl_visibility_drawn = crate::repl_state::current();
    // Console-on-alternate-screen state. While the REPL is visible
    // (Bar or Window) the console renders on the alternate screen,
    // exactly like the Ctrl-T TUI: the terminal saves the primary
    // surface (logs + status block + scrollback) on enter and
    // restores it byte-exact on leave — including the cursor, so the
    // follow-the-log `footer_return_up` stays valid across the
    // excursion. Opening / closing the console can't scroll the
    // primary or leave a residual gap.
    let mut console_alt = false;
    // Force a fresh REPL paint on the alt-screen (set on enter and
    // on a Bar<->Window change).
    let mut repl_alt_dirty = false;
    // Transcript line count last painted on the alt-screen console.
    // A change (from a dispatched command, a completion row, or any
    // other source) triggers a console repaint.
    let mut transcript_len_drawn: usize = 0;
    while !stop.load(Ordering::Acquire) {
        // Drain any metrics frames that arrived since the last
        // tick — only when a frame channel was actually wired in.
        // For pure log-only mode no reporter is registered, so
        // `frame_rx == None` and there's nothing to drain. Phase
        // 2b's FakeTuiSink will use these to drive a periodic
        // status line; the LogOnlySink discards them.
        if let Some(rx) = &frame_rx {
            loop {
                match rx.try_recv() {
                    Ok(_frame) => {}
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                }
            }
        }

        // Drain key events: prompt mutations, window-resize
        // chords, help toggle, interrupt. Every event marks
        // the prompt dirty so the redraw at the bottom of the
        // tick picks up the change.
        let mut submitted_commands: Vec<String> = Vec::new();
        let mut completion_lists: Vec<Vec<String>> = Vec::new();
        if let (Some(rx), Some(p)) = (key_rx.as_ref(), prompt.as_mut()) {
            loop {
                match rx.try_recv() {
                    Ok(WatcherSignal::Key(ke)) => {
                        match p.handle_key(ke) {
                            PromptAction::Continue => {}
                            PromptAction::Submit(line) => submitted_commands.push(line),
                            PromptAction::ShowCompletions(list) =>
                                completion_lists.push(list),
                            PromptAction::GrowWindow => { p.grow_window(); }
                            PromptAction::ShrinkWindow => { p.shrink_window(); }
                            PromptAction::ToggleHelp => { p.toggle_help(); }
                            PromptAction::Interrupt => {
                                // Match supervisor's Ctrl-C path: re-
                                // raise SIGINT so the runtime's
                                // graceful-shutdown handler picks it
                                // up. (The supervisor itself also
                                // forwards Ctrl-C separately; we get
                                // here only when a Ctrl-C lands in
                                // the key stream rather than as
                                // WatcherSignal::Interrupt.)
                                unsafe { libc::raise(libc::SIGINT); }
                            }
                        }
                        prompt_dirty = true;
                    }
                    Ok(WatcherSignal::GrowPrompt)   => { p.grow_window(); prompt_dirty = true; }
                    Ok(WatcherSignal::ShrinkPrompt) => { p.shrink_window(); prompt_dirty = true; }
                    Ok(WatcherSignal::ToggleHelp)   => { p.toggle_help(); prompt_dirty = true; }
                    Ok(_) => { /* ignored — supervisor handles other signals */ }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        // Supervisor torn down — drop the prompt
                        // and continue rendering log-only.
                        prompt = None;
                        prompt_dirty = false;
                        break;
                    }
                }
            }
        }

        // Drain new log entries.
        let snap = state.load();
        let total = snap.log_seq_total;
        // SRD-100 P2 — fold the live `active_phases` snapshot into the
        // status block at the consumer, rather than reading a single
        // pre-rendered scalar that N producer threads stomped. Single-phase
        // output is byte-identical (same builder + bodies); multi-phase
        // stacks per-phase renders (P3 adds the cap / multi-running counter).
        let next_status: Option<String> = crate::status_fold::render_active_status(&snap);
        let need_log_emit = total > last_seen;
        let status_changed = next_status != status_published;

        // Console (REPL) output is CONTAINED in the frame: it
        // goes into the transcript ring, never to stderr
        // scrollback, so a command's echo/response/completions
        // can't scroll the terminal state. The frame (Bar grown
        // or Window) renders the transcript tail in its bounded
        // region with internal scrollback. `dispatched_lines`
        // still flags a redraw so the frame picks the new lines
        // up this tick.
        let dispatched_lines = !submitted_commands.is_empty()
            || !completion_lists.is_empty();
        for list in &completion_lists {
            // One space-separated suggestion row into the frame;
            // column grouping is a future polish.
            crate::repl_state::push_transcript_line(&list.join("  "));
        }
        // Dispatch any submitted commands through the inspector
        // handler. Command echo + each response line land in the
        // transcript ring; the frame renders them. Nothing is
        // written to the shared stderr stream here.
        for line in &submitted_commands {
            let response = crate::inspector_server::dispatch(
                line, &state, runtime.as_ref());
            crate::repl_state::push_transcript(line, &response);
        }

        // === Console on the alternate screen ===
        // When the REPL is visible (Bar or Window) the console
        // renders on the alternate screen, exactly like the Ctrl-T
        // TUI: entering saves the primary surface (logs + managed
        // region + scrollback); leaving restores it byte-exact.
        // This is the "backing buffer" — opening / closing the
        // console can't scroll the primary or leave a residual gap,
        // and there's no incremental re-stream on close. While the
        // console is up the log drain is frozen (logs accumulate in
        // the ring) and flushed on leave.
        let repl_now = crate::repl_state::current();
        let console_visible = !matches!(repl_now,
            crate::repl_state::ReplVisibility::Hidden);

        if console_visible && !console_alt {
            // Enter: switch to the alternate screen (the terminal saves
            // the primary surface AND cursor — so `footer_return_up`
            // stays valid for the post-close redraw).
            let _ = write!(stderr, "\x1b[?1049h\x1b[2J\x1b[H");
            let _ = stderr.flush();
            console_alt = true;
            repl_alt_dirty = true;
        } else if !console_visible && console_alt {
            // Leave: restore the primary surface (the terminal brings
            // back logs + status block + scrollback + cursor byte-
            // exact). Sync `repl_visibility_drawn` to the now-current
            // Hidden state so the primary path does NOT treat this as a
            // visibility change — when nothing else changed the
            // restored surface is left untouched (byte-exact, no
            // re-stream). Real catch-up (logs buffered during the
            // console, phase progress, the session timer) flows through
            // the normal dirty signals below: `need_log_emit`
            // (last_seen was frozen) and `status_changed`.
            let _ = write!(stderr, "\x1b[?1049l");
            let _ = stderr.flush();
            repl_visibility_drawn = repl_now;
            console_alt = false;
        }

        if console_alt {
            // Render the console full-screen on the alternate
            // surface. Redraw only on change (a key edit, a
            // dispatched command, a Bar<->Window switch, or first
            // entry). The log drain below is skipped — `last_seen`
            // stays put so buffered logs flush after we leave.
            let repl_switch = repl_now != repl_visibility_drawn;
            repl_visibility_drawn = repl_now;
            let tlen = crate::repl_state::transcript_len();
            let transcript_changed = tlen != transcript_len_drawn;
            if repl_alt_dirty || prompt_dirty || dispatched_lines
                || repl_switch || transcript_changed
            {
                let (cols, rows) = terminal_size_via_ioctl().unwrap_or((200, 50));
                redraw_console_altscreen(&mut stderr, prompt.as_ref(), cols, rows);
                repl_alt_dirty = false;
                prompt_dirty = false;
                transcript_len_drawn = tlen;
            }
            status_published = next_status;
            std::thread::sleep(POLL_INTERVAL);
            continue;
        }

        // Region anchored at the bottom of the terminal:
        // [logs ...] [status] [separator] [prompt input].
        // The status + prompt rows are absolutely-positioned
        // every redraw so the relative-cursor accounting (which
        // is fragile across scrolls) goes away. Log lines flow
        // upward naturally — they're written at the current
        // cursor position, which after a clear sits at the top
        // of the bottom region.
        //
        // We only touch the screen when something actually
        // changed this tick (logs, status, dispatched command,
        // prompt key event) OR when this is the first render of
        // the prompt (initial bring-up). A steady-state tick
        // with no changes leaves the cursor and screen alone —
        // critical fix to keep the prompt from drifting down
        // the screen as `\r\n` separators stack up.
        // REPL visibility transitions drive a redraw too —
        // toggling `~` flips whether the prompt row exists,
        // shifting the status block's absolute position; the
        // existing dirty signals can't catch it because they're
        // all derived from log/prompt content, not visibility.
        // Reached only when the console is Hidden — a visible
        // console renders on the alternate screen above and
        // `continue`s — so `repl_now` is `Hidden` here and the
        // prompt / window branches below collapse to the plain
        // logs + status layout. On the tick right after leaving the
        // console `repl_visibility_drawn` was already synced to
        // Hidden, so `repl_changed` is false and an unchanged
        // restored surface is left byte-exact.
        let repl_changed = repl_now != repl_visibility_drawn;
        repl_visibility_drawn = repl_now;
        let must_redraw = need_log_emit || status_changed
            || dispatched_lines || prompt_dirty || repl_changed;

        if must_redraw {
            // Compute the new footer content + geometry, then run the
            // follow-the-log pass below: return to the footer top,
            // clear, emit new logs (they scroll), redraw the footer
            // just beneath them. A steady-state tick with no changes
            // never reaches here, so nothing drifts down the screen.
            let (cols, rows) = terminal_size_via_ioctl().unwrap_or((200, 50));

            // Re-format the margin at draw time so the session
            // timer + phase counter tick along with the running
            // phase, matching the log lines above.
            let status_margin = format_margin_prefix(&snap,
                nbrs_runtime::observer::use_color());
            let margin_visible_width = visible_width(&status_margin) as u16;
            // Row-2 margin for the running-phase status block:
            // progress bar + ETA + spinner replacing the `│`
            // divider, padded to the row-1 margin width so the
            // divider columns align. Empty when no phase runs.
            let row2_margin = format_running_phase_row2_margin(
                &snap,
                margin_visible_width,
                nbrs_runtime::observer::use_color(),
            );
            let status_cols = (cols as usize)
                .saturating_sub(margin_visible_width as usize);

            // REPL visibility (sampled once this tick as `repl_now`):
            // - Hidden: no prompt; status fills the bottom region.
            // - Bar: single-row prompt; status block above it.
            // - Window: full-screen console — the prompt expands,
            //   the live status is swapped for a one-line REPL
            //   header, and the phase-history region is suppressed
            //   (the console owns the surface; closing it repaints
            //   the region via the `repl_changed` redraw).
            let window_mode = matches!(repl_now,
                crate::repl_state::ReplVisibility::Window);
            let repl_visible = !matches!(repl_now,
                crate::repl_state::ReplVisibility::Hidden);

            let new_status_text: Option<String> = if window_mode {
                let color = nbrs_runtime::observer::use_color();
                let dim   = if color { "\x1b[2m" } else { "" };
                let reset = if color { "\x1b[0m" } else { "" };
                Some(format!(
                    "{dim}REPL · ` close · ~ hide{reset}"
                ))
            } else {
                next_status.as_ref().map(|s| {
                    clamp_multiline(s, status_cols.saturating_sub(1))
                })
            };
            // Window mode expands the prompt to fill the screen
            // minus the header row. `set_window_rows` is the single
            // geometry chokepoint; re-applied each tick so the
            // REPL-mode override wins over Alt-Up / Alt-Down.
            if window_mode
                && let Some(p) = prompt.as_mut() {
                    let target = rows.saturating_sub(2).max(1);
                    if p.window_rows() != target {
                        p.set_window_rows(target);
                    }
                }
            let prompt_input = if repl_visible {
                prompt.as_ref().map(|p| {
                    let win_rows = p.window_rows() as usize;
                    // Any multi-row console frame (Window, or a Bar
                    // grown past one row) composes its `rendered`
                    // bytes by hand: the top rows are the contained
                    // transcript tail (oldest at top, newest just
                    // above the input) with internal scrollback; the
                    // bottom row is the standard prompt-input render.
                    // A single-row Bar falls through to the plain
                    // input render (its output lives in the ring,
                    // viewable by growing the bar or opening the
                    // window — it never scrolls the terminal).
                    let rendered = if win_rows > 1 {
                        let transcript_rows = win_rows.saturating_sub(1);
                        let tail = crate::repl_state::transcript_tail(transcript_rows);
                        let cols_usize = cols as usize;
                        let mut composed = String::with_capacity(cols_usize * win_rows);
                        // Top-pad with blanks when the transcript is
                        // shorter than the window so the input stays
                        // anchored at the bottom row rather than
                        // rising as history grows.
                        let blanks = transcript_rows.saturating_sub(tail.len());
                        for _ in 0..blanks {
                            composed.push_str("\r\n");
                        }
                        for line in tail.iter() {
                            let row = nbrs_runtime::activity::truncate_to_width(
                                line, cols_usize);
                            composed.push_str(&row);
                            composed.push_str("\r\n");
                        }
                        // Final row: the prompt's own single-row
                        // render of the input buffer (`❯ <buffer>`
                        // plus cursor) — the last row of its
                        // multi-row render is what we want.
                        let mut input_row = String::with_capacity(128);
                        p.render(&mut input_row, cols_usize, true);
                        let last = input_row.split('\n').next_back().unwrap_or("");
                        composed.push_str(last.strip_suffix('\r').unwrap_or(last));
                        composed
                    } else {
                        let mut f = String::with_capacity(128);
                        p.render(&mut f, cols as usize, true);
                        f
                    };
                    PromptInput {
                        rendered,
                        window_rows: p.window_rows(),
                        cursor_col: p.cursor_col() as u16,
                    }
                })
            } else {
                None
            };

            // FOLLOW-THE-LOG sticky footer.
            //
            // The status block floats at the bottom by being cleared
            // and reprinted just below the log stream every tick; the
            // log stream owns everything above it and scrolls
            // naturally. There is no absolute positioning and no
            // scroll-on-growth — a height change is absorbed by the
            // terminal's own scroll as the footer is reprinted, so a
            // blank is never stranded between the logs and the status.
            // (The absolute-positioned approach left the cursor on a
            // trailing blank line after the last log; the moment a
            // status appeared or changed height that blank got scrolled
            // up and baked in, accumulating one gap per status op —
            // and a log emit colliding with a height change could even
            // drop the log. See the `tui_display_harness` tests.)
            //
            // Invariant across ticks: after a committed redraw the
            // cursor sits `footer_return_up` rows BELOW the footer's
            // first row.

            // (A) Return to the footer top and clear it plus anything
            //     below. Skipped on the first paint: no footer is
            //     drawn yet and the cursor sits just after the last
            //     emitted log (or on a freshly restored surface), so a
            //     clear-to-end-of-screen from there is correct.
            if !first_paint {
                if footer_return_up > 0 {
                    let _ = write!(stderr, "\x1b[{footer_return_up}A");
                }
                let _ = write!(stderr, "\r\x1b[J");
            }

            // (B) Emit any new log lines. Each ends with `\r\n`, so it
            //     scrolls the surface up and leaves the cursor on the
            //     next fresh line — exactly where the footer begins,
            //     so the footer's first row reclaims that line with no
            //     gap.
            if need_log_emit {
                let new_count = total - last_seen;
                let take = new_count.min(LOG_RING_CAPACITY) as usize;
                let ring = &snap.log_messages;
                let start_idx = ring.len().saturating_sub(take);
                // Note any drop. The session-log sink still has
                // every entry; this is a render-side warning only.
                if new_count > LOG_RING_CAPACITY {
                    let dropped = new_count - LOG_RING_CAPACITY;
                    let _ = write!(
                        stderr,
                        "log-only-sink: dropped {dropped} log line(s) (renderer too slow); see session.log\r\n",
                    );
                }
                let margin = format_margin_prefix(&snap,
                    nbrs_runtime::observer::use_color());
                for entry in &ring[start_idx..] {
                    let entry_level = severity_to_level(entry.severity);
                    if entry_level < min_level {
                        continue;
                    }
                    // Phase-START lifecycle lines are suppressed from
                    // scrollback: the live status block already shows
                    // the running phase, and the rich per-phase ✓
                    // summary (a Diagnostic log, not tagged here) is
                    // the completion marker — a phase-start line on
                    // top would be redundant noise. All entries are
                    // kept in session.log unconditionally.
                    if entry.category == crate::state::LogCategory::PhaseLifecycle {
                        continue;
                    }
                    // Match the observer's cosmetic blank line before
                    // the Ctrl-C / force-exit banners.
                    if entry.message.starts_with("session: graceful shutdown requested")
                        || entry.message.starts_with("session: force-exit on second")
                    {
                        let _ = write!(stderr, "\r\n");
                    }
                    // Colorize by severity. `colorize_log_line` is a
                    // no-op on non-tty / NO_COLOR so log captures stay
                    // clean. Split on embedded `\n` so multi-line
                    // messages get `\r\n` (raw mode needs the explicit
                    // `\r`); each row gets the margin and a trailing
                    // newline so it scrolls the surface.
                    let painted = nbrs_runtime::observer::colorize_log_line(
                        entry_level, &entry.message);
                    for row in painted.split('\n') {
                        let _ = write!(stderr, "{margin}{row}\r\n");
                    }
                }
                last_seen = total;
            }

            // (C) Draw the footer (status [+ inline prompt]) at the
            //     cursor the log emit left us on, top-aligned and with
            //     NO trailing newline. It returns how far below the
            //     footer top the cursor was left, for the next tick's
            //     climb-back in (A).
            let (_drawn_rows, cursor_below_top) = draw_footer_at_cursor(
                &mut stderr,
                new_status_text.as_deref(),
                prompt_input.as_ref(),
                cols,
                &status_margin,
                margin_visible_width,
                &row2_margin,
            );
            footer_return_up = cursor_below_top;
            let _ = _drawn_rows;
            first_paint = false;
            let _ = stderr.flush();
            prompt_dirty = false;
        }
        status_published = next_status;

        // Publish the combined render height so the signal
        // handler in `crate::app` can walk past everything we
        std::thread::sleep(POLL_INTERVAL);
    }

    // Sink shutting down. If the console was up we're on the
    // alternate screen — leave it first so the terminal isn't
    // stranded there. The terminal restores the primary surface AND
    // cursor, so `footer_return_up` still describes where the footer
    // sits for the clear below.
    if console_alt {
        let _ = write!(stderr, "\x1b[?1049l");
        let _ = stderr.flush();
    }
    // Wipe the footer we own so the post-run terminal state isn't
    // littered with our final tick's text. Follow-the-log left the
    // cursor `footer_return_up` rows below the footer's first row (the
    // `?1049` cursor save/restore preserved this across any console
    // excursion above), so climb back to the footer top and clear to
    // end of screen. The signal-handler cleanup path in `crate::app`
    // instead uses `\x1b[999;1H` to park at the viewport bottom, so it
    // needs no row count of ours.
    if footer_return_up > 0 {
        let _ = write!(stderr, "\x1b[{footer_return_up}A");
    }
    let _ = write!(stderr, "\r\x1b[J");
    let _ = stderr.flush();

    // Hand our final log cursor to the supervisor. The next
    // terminal-mode sink it brings up after a TUI swap seeds
    // `last_seen` from here and re-emits the lines that scrolled
    // while the alternate screen was up, so the restored scrollback
    // catches up to the full phase history. Harmless at run-end —
    // no successor sink reads it.
    if let Some(a) = &resume_from {
        a.store(last_seen, Ordering::Release);
    }
}

/// Build the left-margin prefix carrying the session timer
/// (compact 8-char clock from
/// [`nbrs_runtime::readouts::format::format_compact_session_elapsed`]),
/// plus the current phase index, followed by a `│` gutter. The
/// magnitude-tracking color span from
/// [`nbrs_runtime::readouts::format::session_elapsed_color`]
/// wraps the clock so glance-readability tracks how deep
/// into the run the log line is (dim under a minute,
/// default under an hour, bold beyond).
fn format_margin_prefix(
    snap: &std::sync::Arc<crate::state::RunState>,
    color: bool,
) -> String {
    use nbrs_runtime::readouts::format::{
        format_compact_session_elapsed, session_elapsed_color,
    };

    let dim   = if color { "\x1b[2m"  } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };

    let secs = snap.elapsed_secs();
    let elapsed_str = format_compact_session_elapsed(secs);
    let (clock_open, clock_close) = session_elapsed_color(secs, color);

    // Count phases only — scope nodes (for_each / do_while
    // wrappers) live in `snap.phases` but they're not what
    // the operator wants in the step counter.
    //
    // Denominator: `expected_total_phases`, pinned by
    // `install_tree` from the pre-map walk. Stable across
    // the run regardless of whether the executor materializes
    // additional phases at runtime (param-driven `for_each`
    // expansion the structural pass couldn't resolve).
    //
    // Numerator: the pre-mapped `seq` of the currently-running
    // phase (or the latest-completed phase when nothing is
    // running). `seq` is the 1-based pre-map sequence number
    // assigned at SceneTree::push time — STABLE under runtime
    // mutation. The previous logic used `phase_only.iter()
    // .position(running)`, which silently drifted as the live
    // phase list grew (auto-extern materialization appended new
    // pending phases, shifting the running one's index even
    // though its pre-mapped slot hadn't moved).
    //
    // Runtime-materialized phases (no `seq`) report `None`
    // here; we fall back to a sequential count among the
    // non-pending phases so the operator still sees forward
    // progress in that edge case.
    let phase_only: Vec<_> = snap.phases.iter()
        .filter(|p| matches!(p.kind, crate::state::EntryKind::Phase))
        .collect();
    let total = snap.expected_total_phases;
    let running_seq = phase_only.iter()
        .find(|p| matches!(p.status, crate::state::PhaseStatus::Running))
        .and_then(|p| p.seq);
    let latest_done_seq = phase_only.iter()
        .filter(|p| !matches!(p.status, crate::state::PhaseStatus::Pending))
        .filter_map(|p| p.seq)
        .max();
    let fallback_done = phase_only.iter()
        .filter(|p| !matches!(p.status, crate::state::PhaseStatus::Pending))
        .count();
    let phase_str = match (running_seq, latest_done_seq, total) {
        (Some(s), _, n) if n > 0 => format!("{s:>3}/{n}"),
        (None, Some(s), n) if n > 0 => format!("{s:>3}/{n}"),
        (None, None, n) if n > 0 && fallback_done > 0 =>
            format!("{fallback_done:>3}/{n}"),
        (_, _, n) if n > 0 => format!("  0/{n}"),
        _ => "   /  ".to_string(),
    };

    format!("{clock_open}{elapsed_str}{clock_close} {dim}{phase_str} │{reset} ")
}

/// Direct `TIOCGWINSZ` ioctl on `fd 2` (stderr). Returns
/// `(cols, rows)` when stderr is a terminal and the call
/// succeeds; `None` otherwise. Mirrors
/// [`nbrs_runtime::activity::terminal_cols`] but also captures
/// the row count we need for absolute positioning of the
/// bottom region.
fn terminal_size_via_ioctl() -> Option<(u16, u16)> {
    #[repr(C)]
    struct WinSize { ws_row: u16, ws_col: u16, ws_xpixel: u16, ws_ypixel: u16 }
    let mut ws = WinSize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    let rc = unsafe { libc::ioctl(2, libc::TIOCGWINSZ, &mut ws as *mut _) };
    if rc < 0 || ws.ws_col == 0 || ws.ws_row == 0 {
        return None;
    }
    Some((ws.ws_col, ws.ws_row))
}

/// Prompt-rendering inputs handed to [`redraw_bottom_region`].
/// Decoupled from `PromptState` so the renderer is testable
/// with simple data.
pub(crate) struct PromptInput {
    /// The prompt's render() output — the bytes to write for
    /// the prompt window's rows, separated by `\r\n` (carriage
    /// return + newline) between rows.
    pub rendered: String,
    /// How many rows the prompt window occupies.
    pub window_rows: u16,
    /// Cursor column within the input row (0-based; the
    /// renderer adds 1 to translate to 1-based escape coords).
    pub cursor_col: u16,
}

/// Draw the bottom "footer" — the live status block, plus the inline
/// console prompt when one is shown — starting at the CURRENT cursor
/// row (wherever the log emit left it), top-aligned, joining rows with
/// `\r\n` and leaving NO trailing newline. Returns
/// `(rows_drawn, cursor_rows_below_footer_top)`; the caller climbs back
/// up by the latter on the next tick to reach the footer top.
///
/// This is the follow-the-log counterpart to the absolute-positioned
/// [`redraw_bottom_region`]: drawing relative to where the log stream
/// left the cursor keeps the footer contiguous with the logs above it,
/// and a height change is absorbed by the terminal's own scroll as the
/// footer is reprinted — never stranding a blank row between the logs
/// and the status. Each row is `\r`-homed and erase-to-EOL'd so residue
/// from a wider prior render is scrubbed.
///
/// Row 0 of the status carries `status_margin` (timer + phase counter +
/// `│ ` divider); rows 1+ carry `row2_margin` (the running-phase
/// progress/ETA/spinner variant, padded so the divider column aligns)
/// when present, else the standard margin.
pub(crate) fn draw_footer_at_cursor<W: Write>(
    out: &mut W,
    status_text: Option<&str>,
    prompt: Option<&PromptInput>,
    term_cols: u16,
    status_margin: &str,
    margin_width: u16,
    row2_margin: &str,
) -> (u16, u16) {
    let _ = term_cols;
    let status_lines: Vec<&str> = status_text
        .map(|s| s.split('\n').collect())
        .unwrap_or_default();
    let status_rows_n = status_lines.len() as u16;
    let prompt_rows = prompt.map(|p| p.window_rows).unwrap_or(0);
    let total = status_rows_n + prompt_rows;
    if total == 0 {
        return (0, 0);
    }

    let mut first = true;
    for (i, row) in status_lines.iter().enumerate() {
        if !first {
            let _ = write!(out, "\r\n");
        }
        first = false;
        let margin = if i == 0 || row2_margin.is_empty() {
            status_margin
        } else {
            row2_margin
        };
        let _ = write!(out, "\r\x1b[K{margin}{row}");
    }
    // Prompt rows — present only for the inline console bar (the full
    // console renders on the alternate screen, not here). The prompt's
    // `>` sits at the same column as the log content (right of the
    // margin) so the operator reads the input row as the bottom of the
    // same column.
    if let Some(p) = prompt {
        for row in p.rendered.split('\n') {
            if !first {
                let _ = write!(out, "\r\n");
            }
            first = false;
            let row = row.strip_suffix('\r').unwrap_or(row);
            let row = row.strip_prefix('\r').unwrap_or(row);
            let _ = write!(out, "\r\x1b[K{status_margin}{row}");
        }
        // Park the cursor at the prompt's input column on the last
        // (current) row for typing — `margin_width` shifts it past the
        // margin into the content area. `\x1b[NG` is column-absolute.
        let target_col = margin_width + p.cursor_col + 1;
        let _ = write!(out, "\x1b[{target_col}G");
    }
    (total, total.saturating_sub(1))
}

/// Render the console (REPL) full-screen on the alternate screen.
///
/// Layout: a one-line header at the top, the transcript tail
/// filling the middle (oldest at top, newest just above the
/// input), and the prompt input on the bottom row. Each row is
/// absolutely positioned and erase-to-EOL'd so residue from a
/// prior, wider paint is scrubbed. The cursor parks at the input
/// column. The caller owns the alternate-screen enter/leave (the
/// terminal saves / restores the primary surface), so this only
/// composes the console surface itself.
fn redraw_console_altscreen<W: Write>(
    out: &mut W,
    prompt: Option<&PromptState>,
    cols: u16,
    rows: u16,
) {
    let color = nbrs_runtime::observer::use_color();
    let dim   = if color { "\x1b[2m" } else { "" };
    let reset = if color { "\x1b[0m" } else { "" };
    let cols_usize = cols as usize;

    // Header (row 1).
    let _ = write!(out,
        "\x1b[1;1H\x1b[K{dim}REPL · ~ or ` to close · ↑↓ history{reset}");

    // Transcript tail fills rows 2 ..= rows-1 — oldest at the top,
    // top-padded with blanks so the newest line sits just above the
    // input even when the history is short.
    let body_rows = (rows as usize).saturating_sub(2);
    let tail = crate::repl_state::transcript_tail(body_rows);
    let blanks = body_rows.saturating_sub(tail.len());
    let mut row: u16 = 2;
    for _ in 0..blanks {
        let _ = write!(out, "\x1b[{row};1H\x1b[K");
        row += 1;
    }
    for line in &tail {
        let painted = nbrs_runtime::activity::truncate_to_width(line, cols_usize);
        let _ = write!(out, "\x1b[{row};1H\x1b[K{painted}");
        row += 1;
    }

    // Input row (bottom): the prompt's own single-line input
    // render (last row of its multi-row render).
    let mut cursor_col: u16 = 0;
    if let Some(p) = prompt {
        let mut input = String::with_capacity(128);
        p.render(&mut input, cols_usize, true);
        let last = input.split('\n').next_back().unwrap_or("");
        let last = last.strip_suffix('\r').unwrap_or(last);
        let _ = write!(out, "\x1b[{rows};1H\x1b[K{last}");
        cursor_col = p.cursor_col() as u16;
    } else {
        let _ = write!(out, "\x1b[{rows};1H\x1b[K");
    }
    // Park the cursor at the input column (1-based).
    let _ = write!(out, "\x1b[{rows};{}H", cursor_col + 1);
    let _ = out.flush();
}

/// Build the row-2 margin for a running-phase status block.
/// Layout: `<bar><eta-padded><spinner>` so the bar reads as
/// "progress" and the spinner replaces the standard `│`
/// divider as a still-ticking indicator. Padded with spaces
/// so its visible width matches `target_width` — the row-1
/// margin width — making the divider columns line up
/// vertically across rows.
///
/// Returns an empty string when no phase is currently running
/// — the caller falls back to the standard margin.
fn format_running_phase_row2_margin(
    snap: &std::sync::Arc<crate::state::RunState>,
    target_width: u16,
    color: bool,
) -> String {
    use nbrs_runtime::readouts::format::{
        braille_bar, format_eta, spinner_frame,
    };
    let Some(active) = snap.active_phases.values().next() else {
        return String::new();
    };
    let dim   = if color { "\x1b[2m"  } else { "" };
    let cyan  = if color { "\x1b[36m" } else { "" };
    let reset = if color { "\x1b[0m"  } else { "" };
    let elapsed = active.started_at.elapsed().as_secs_f64();
    let pct = if active.cursor_extent > 0 {
        (active.ops_finished as f64) * 100.0 / (active.cursor_extent as f64)
    } else { 0.0 };
    let bar = braille_bar(pct, 10);
    let eta_str = if active.cursor_extent > 0 && active.ops_per_sec > 0.0 {
        let remaining = active.cursor_extent.saturating_sub(active.ops_finished) as f64;
        format_eta(remaining / active.ops_per_sec)
    } else {
        format_eta(elapsed)
    };
    // Spinner ticks once per render. Use elapsed-secs * 10 so
    // the frame advances at ~10 Hz independent of redraw cadence.
    let tick = (elapsed * 10.0) as u64;
    let spinner = spinner_frame(tick);
    // Pad the ETA out so the whole margin is exactly `target_width`
    // visible columns and the spinner lands under the row-1 `│`.
    let pad = row2_margin_pad(eta_str.chars().count(), target_width as usize);
    format!("{bar} {dim}{eta_str}{reset}{:<pad$}{cyan}{spinner}{reset} ", "", pad = pad)
}

/// Spaces between the ETA and the spinner-divider in the running-
/// phase row-2 margin, sized so the full margin is exactly
/// `target_width` visible columns and the spinner sits under the
/// row-1 `│` divider.
///
/// Layout: `bar(10) " "(1) eta(E) <pad> spinner(1) " "(1)`, so the
/// total is `13 + E + pad` and the spinner (the column before the
/// trailing space) must land at `target_width - 1` — i.e.
/// `pad = target_width - 13 - E`. (The previous code counted a
/// non-existent space after the ETA, leaving the spinner one
/// column shy of the divider except when `saturating_sub` happened
/// to clamp the pad to zero.)
fn row2_margin_pad(eta_visible: usize, target_width: usize) -> usize {
    // 10 (bar) + 1 (space) + eta + pad + 1 (spinner) + 1 (space)
    target_width.saturating_sub(10 + 1 + eta_visible + 1 + 1)
}

/// Approximate visible width of a string with ANSI SGR escape
/// codes stripped. SGR sequences (`\x1b[...m`) carry no
/// columns; everything else counts as one column per char.
/// Good enough for the margin prefix, which has no
/// double-width glyphs.
fn visible_width(s: &str) -> usize {
    let mut width = 0;
    let mut in_escape = false;
    for ch in s.chars() {
        if in_escape {
            if ch == 'm' { in_escape = false; }
            continue;
        }
        if ch == '\x1b' { in_escape = true; continue; }
        width += 1;
    }
    width
}

/// Clamp each `\n`-delimited row of `s` to `max_cols` columns
/// independently, then rejoin with `\n`. `\n` itself is not a
/// visible column and must not consume the budget; per-row
/// clamping prevents the second line of a two-line status
/// from being chopped off when the first line is long.
fn clamp_multiline(s: &str, max_cols: usize) -> String {
    let mut out = String::with_capacity(s.len());
    let mut first = true;
    for row in s.split('\n') {
        if !first { out.push('\n'); }
        out.push_str(&nbrs_runtime::activity::truncate_to_width(row, max_cols));
        first = false;
    }
    out
}

struct LogOnlySinkHandle {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    sink_active: Arc<AtomicBool>,
}

impl SinkHandle for LogOnlySinkHandle {
    fn shutdown(mut self: Box<Self>) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        // Defensively clear in case the thread exited early
        // before the post-loop store ran (e.g. spawn failure
        // in some future variant).
        self.sink_active.store(false, Ordering::Release);
    }

    fn owns_terminal(&self) -> bool { false }
}

#[cfg(test)]
mod redraw_tests {
    //! Layout tests for [`redraw_bottom_region`]. Drives the
    //! pure renderer through a sequence of mocked status
    //! updates and asserts the emitted byte sequence — pinning
    //! the absolute-positioning invariants that produced the
    //! "stacked status snapshots" bug.
    use super::*;

    /// One simulated render tick with a left margin: draw the footer
    /// at the cursor and capture the bytes. 80-col terminal; the
    /// follow-the-log footer is relative-positioned so no row count is
    /// needed.
    fn render_tick_with_margin(
        status: Option<&str>,
        prompt: Option<&PromptInput>,
        margin: &str,
    ) -> String {
        let mut out: Vec<u8> = Vec::new();
        let width = super::visible_width(margin) as u16;
        draw_footer_at_cursor(&mut out, status, prompt, 80, margin, width, "");
        String::from_utf8(out).expect("rendered bytes are utf-8")
    }

    fn one_row_prompt(buf: &str) -> PromptInput {
        // Mimic PromptState::render output for a 1-row window.
        let rendered = format!("\x1b[36m❯\x1b[0m {buf}");
        let cursor_col = (2 + buf.chars().count()) as u16;
        PromptInput {
            rendered,
            window_rows: 1,
            cursor_col,
        }
    }

    /// Each footer row is `\r`-homed and erase-to-EOL'd (`\x1b[K`) to
    /// scrub residue from a wider prior tick, rows are joined with
    /// `\r\n`, and the LAST row leaves no trailing newline so the log
    /// stream below isn't pushed down. The returned offset is the
    /// cursor's row distance below the footer top.
    #[test]
    fn footer_rows_home_erase_and_have_no_trailing_newline() {
        let mut out: Vec<u8> = Vec::new();
        let (rows, below) = draw_footer_at_cursor(
            &mut out, Some("head row\ntail row"), None, 80, "", 0, "");
        let s = String::from_utf8(out).expect("utf-8");
        assert_eq!((rows, below), (2, 1), "two rows, cursor 1 below the top");
        assert!(s.contains("\r\x1b[Khead row"), "head row homed + erased: {s:?}");
        assert!(s.contains("\r\x1b[Ktail row"), "tail row homed + erased: {s:?}");
        // Exactly one `\r\n` separates the two rows; none trails.
        assert_eq!(s.matches("\r\n").count(), 1, "one separator only: {s:?}");
        assert!(!s.ends_with("\r\n"), "no trailing newline: {s:?}");
    }

    /// An empty footer (no status, no prompt) draws nothing and
    /// reports a zero offset.
    #[test]
    fn empty_footer_draws_nothing() {
        let mut out: Vec<u8> = Vec::new();
        let (rows, below) = draw_footer_at_cursor(&mut out, None, None, 80, "", 0, "");
        assert_eq!((rows, below), (0, 0));
        assert!(out.is_empty(), "empty footer emits no bytes: {out:?}");
    }

    /// Status + prompt rows are prefixed with the same margin the log
    /// lines above carry, so content columns line up across the
    /// log/status divide. The cursor lands in the prompt's content
    /// area (past the margin), set with a column-absolute `\x1b[NG`.
    #[test]
    fn footer_rows_carry_log_margin_for_column_alignment() {
        let margin = "12.34s 5/9 │ ";
        let status = "running";
        let prompt = one_row_prompt("hi");
        let out = render_tick_with_margin(Some(status), Some(&prompt), margin);
        assert!(out.contains(&format!("\r\x1b[K{margin}running")),
            "status row must carry the margin: {out:?}");
        assert!(out.contains(&format!("\r\x1b[K{margin}\x1b[36m❯\x1b[0m hi")),
            "prompt row must carry the margin: {out:?}");
        // Column = margin_width + cursor_col + 1.
        // margin is 13 visible; "❯ hi" puts cursor_col at 4 → 13+4+1=18.
        assert!(out.contains("\x1b[18G"),
            "cursor must skip past the margin width: {out:?}");
    }

    /// When the running-phase row-2 margin is supplied (the
    /// bar+ETA+spinner gutter), line 0 of the status inherits the
    /// standard row-1 margin and lines 1+ inherit the row-2 margin.
    #[test]
    fn row2_margin_applies_to_line_two_only() {
        let row1 = "12.34s 5/9 │ ";
        let row2 = "⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀ 3s   │ ";
        let status = "running\nstats line";
        let mut out: Vec<u8> = Vec::new();
        draw_footer_at_cursor(
            &mut out, Some(status), None,
            80, row1, super::visible_width(row1) as u16, row2);
        let out = String::from_utf8(out).expect("utf-8");
        assert!(out.contains(&format!("\r\x1b[K{row1}running")),
            "line 0 MUST carry row1 margin: {out:?}");
        assert!(out.contains(&format!("\r\x1b[K{row2}stats line")),
            "line 1 MUST carry row2 margin: {out:?}");
    }

    /// Empty row2 margin → every line falls back to row1 (the
    /// no-active-phase baseline path).
    #[test]
    fn empty_row2_margin_uses_row1_for_every_line() {
        let row1 = "12.34s 5/9 │ ";
        let status = "first\nsecond";
        let mut out: Vec<u8> = Vec::new();
        draw_footer_at_cursor(
            &mut out, Some(status), None,
            80, row1, super::visible_width(row1) as u16, "");
        let out = String::from_utf8(out).expect("utf-8");
        assert!(out.contains(&format!("\r\x1b[K{row1}first")),
            "line 0 MUST carry row1 margin: {out:?}");
        assert!(out.contains(&format!("\r\x1b[K{row1}second")),
            "line 1 MUST also carry row1 margin when row2 is empty: {out:?}");
    }

    /// The running-phase row-2 margin pads the ETA so the spinner-
    /// divider lands under the row-1 `│` — for EVERY ETA width that
    /// fits, not just the one width where the old off-by-one
    /// happened to cancel out. Margin layout:
    /// `bar(10) " " eta <pad> spinner(1) " "`; the spinner column
    /// (1-based) must equal `target - 1` (the `│` column).
    #[test]
    fn row2_margin_spinner_aligns_under_divider_for_all_eta_widths() {
        // e.g. row-1 margin "21.3901s   3/53 │ " is 18 wide; `│`
        // at column 17, trailing space at 18.
        let target = 18;
        for eta in 1..=5 {
            let pad = super::row2_margin_pad(eta, target);
            let total = 10 + 1 + eta + pad + 1 + 1;
            assert_eq!(total, target,
                "margin width must equal target (eta={eta})");
            let spinner_col = 10 + 1 + eta + pad + 1; // 1-based
            assert_eq!(spinner_col, target - 1,
                "spinner must sit under the `│` divider (eta={eta})");
        }
        // The reported case: eta "1s" (2 cols) → pad 3, not 2.
        assert_eq!(super::row2_margin_pad(2, 18), 3);
    }

    /// A fresh sink (no shared cursor, or one still holding the
    /// `RESUME_FRESH` sentinel) seeds from the live `log_seq_total`
    /// so it doesn't re-print history the observer already wrote.
    #[test]
    fn seed_last_seen_fresh_uses_current() {
        assert_eq!(super::seed_last_seen(None, 42), 42);
        assert_eq!(super::seed_last_seen(Some(super::RESUME_FRESH), 42), 42);
    }

    /// A swap re-entry (the supervisor's shared cursor holds the
    /// prior sink's final `last_seen`, not the sentinel) resumes
    /// from that cursor so the lines that scrolled under the TUI's
    /// alternate screen are re-emitted into the restored scrollback.
    #[test]
    fn seed_last_seen_resume_uses_stored_cursor() {
        assert_eq!(super::seed_last_seen(Some(10), 42), 10);
        assert_eq!(super::seed_last_seen(Some(0), 42), 0);
    }

    /// `visible_width` strips ANSI SGR sequences so the
    /// margin-offset arithmetic still lines up when the margin
    /// is color-painted.
    #[test]
    fn visible_width_strips_sgr_sequences() {
        assert_eq!(super::visible_width(""), 0);
        assert_eq!(super::visible_width("plain text"), 10);
        // `\x1b[2m...\x1b[0m` carries no columns.
        assert_eq!(super::visible_width("\x1b[2mdim\x1b[0m text"), 8);
        // Nested / multiple escapes.
        assert_eq!(super::visible_width("\x1b[1;31mAB\x1b[0m\x1b[32mCD\x1b[0m"),
            4);
    }
}
