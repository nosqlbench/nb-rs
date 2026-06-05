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
//! `nbrs_activity::log_sink` takes every level unconditionally,
//! see SRD 02 §"Display and Diagnostic Decoupling").

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use nbrs_activity::observer::LogLevel;

use crate::display_sink::{DisplayInputs, DisplaySink, SinkHandle};
use crate::key_watcher::WatcherSignal;
use crate::prompt_state::{PromptAction, PromptState};
use crate::state::LogSeverity;

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const LOG_RING_CAPACITY: u64 = 200;

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
}

impl LogOnlySink {
    pub fn new(min_level: LogLevel, sink_active: Arc<AtomicBool>) -> Self {
        Self {
            min_level,
            sink_active,
            key_rx: None,
            runtime: None,
        }
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
        let LogOnlySink { min_level, sink_active, key_rx, runtime } = *self;

        // Snapshot once before claiming the surface so we don't
        // re-emit anything the observer already printed pre-flag.
        let initial_seq = state.load().log_seq_total;
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
) {
    let mut stderr = io::stderr();
    // The raw status string most recently published by the
    // actor and reflected on the terminal. We compare against
    // *this* (not the clamped form actually written to the
    // surface) so identity checks are stable across ticks.
    let mut status_published: Option<String> = None;
    // The clamped, per-line-truncated text actually drawn at
    // the bottom of the surface. Tracked so the next clear
    // knows how many rows to climb past. `None` means nothing
    // is drawn.
    let mut status_drawn: Option<String> = None;
    // nb-shell prompt — owned by the render thread when a key
    // channel is wired in. Tracks line buffer, history, window
    // rows, and help overlay. `None` falls back to the legacy
    // log-only behaviour.
    let mut prompt: Option<PromptState> = key_rx.as_ref().map(|_| PromptState::new());
    // Last rendered prompt window row count — must match
    // `prompt.window_rows()` plus any side-effects of the help
    // overlay; the renderer recomputes it each tick.
    let mut prompt_drawn_rows: u16 = 0;
    let mut prompt_dirty = prompt.is_some();
    // Tracks the REPL visibility that the last redraw committed
    // to. Drives a redraw on the tick after a `~` / `Ctrl-~`
    // toggle so the prompt show/hide takes effect promptly.
    let mut repl_visibility_drawn = crate::repl_state::current();
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
        let next_status: Option<String> = snap.status_render.clone();
        let need_log_emit = total > last_seen;
        let status_changed = next_status != status_published;

        // Render completion lists as ephemeral log-style rows
        // above the prompt. They scroll past on the next
        // render tick (same lifetime as a command response),
        // which is the conventional shell completion UX.
        let dispatched_lines = !submitted_commands.is_empty()
            || !completion_lists.is_empty();
        for list in &completion_lists {
            // Pretty-print as a single space-separated row;
            // grouping into columns is a future polish.
            let _ = write!(stderr, "{}\r\n", list.join("  "));
        }

        // Dispatch any submitted commands through the
        // inspector handler. Each response becomes a synthetic
        // log entry (one log emit per line) so the scrollback
        // discipline above carries them past the status region
        // identically to any other diag! line.
        for line in &submitted_commands {
            let response = crate::inspector_server::dispatch(
                line, &state, runtime.as_ref());
            // Persist command + response into the REPL
            // transcript ring so Window mode can render the
            // scrollback above the input. The same data flows
            // to stderr below for Bar / Hidden modes (where
            // the inline scroll is what the operator sees).
            crate::repl_state::push_transcript(line, &response);
            // Echo the command then its response so the user
            // sees what they typed.
            for row in std::iter::once(format!("> {line}").as_str())
                .chain(response.split('\n'))
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
            {
                // Push directly to stderr — bypassing the
                // observer route since this is the same
                // render thread and the actor's log_messages
                // ring is only for diag!/observer-originated
                // entries.
                let _ = write!(stderr, "{row}\r\n");
            }
        }
        if dispatched_lines {
            let _ = stderr.flush();
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
        let repl_now = crate::repl_state::current();
        let repl_changed = repl_now != repl_visibility_drawn;
        repl_visibility_drawn = repl_now;
        let region_dirty = need_log_emit || status_changed
            || dispatched_lines || prompt_dirty || repl_changed;
        let prompt_first_render = prompt.is_some() && prompt_drawn_rows == 0
            && !matches!(repl_now, crate::repl_state::ReplVisibility::Hidden);
        let must_redraw = region_dirty || prompt_first_render;

        if must_redraw {
            // Step 1: clear the existing region. No-op if
            // nothing was drawn.
            if status_drawn.is_some() || prompt_drawn_rows > 0 {
                clear_combined_region(
                    &mut stderr,
                    status_drawn.as_deref(),
                    prompt_drawn_rows,
                );
                status_drawn = None;
                prompt_drawn_rows = 0;
            }
        }

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

            // Left-margin prefix carrying session elapsed +
            // current phase index. Computed once per tick from
            // the snapshot; prepended to every emitted log row
            // so the operator's eye can track wall-clock and
            // scenario progress against the log stream without
            // scrolling up to find the most recent phase
            // header. Color is dim so the margin reads as a
            // gutter rather than competing with content.
            let margin = format_margin_prefix(&snap,
                nbrs_activity::observer::use_color());

            for entry in &ring[start_idx..] {
                let entry_level = severity_to_level(entry.severity);
                if entry_level < min_level {
                    continue;
                }
                // Match the observer's cosmetic blank line before
                // the Ctrl-C / force-exit banners.
                if entry.message.starts_with("session: graceful shutdown requested")
                    || entry.message.starts_with("session: force-exit on second")
                {
                    let _ = write!(stderr, "\r\n");
                }
                // Colorize by severity. `colorize_log_line` is
                // a no-op on non-tty / NO_COLOR so log captures
                // stay clean.
                let painted = nbrs_activity::observer::colorize_log_line(
                    entry_level, &entry.message);
                // Split on embedded `\n` so multi-line log
                // messages (e.g. the two-line phase_outcome
                // render) get `\r\n` after every row. Raw mode
                // needs explicit `\r` to return to col 0; a
                // bare `\n` leaves the cursor under the
                // previous row's last character. Each row gets
                // the same margin prefix.
                for row in painted.split('\n') {
                    let _ = write!(stderr, "{margin}{row}\r\n");
                }
            }
            let _ = stderr.flush();
            last_seen = total;
        }

        // Redraw the status + prompt at absolute positions
        // anchored to the bottom of the terminal. Only fires
        // when must_redraw is true — steady-state ticks leave
        // the screen alone.
        let (cols, rows) = terminal_size_via_ioctl().unwrap_or((200, 50));
        if must_redraw {
            // Re-format the margin at status-draw time so the
            // session timer + phase counter on the status rows
            // tick along with the running phase, matching what
            // the log lines above show.
            let status_margin = format_margin_prefix(&snap,
                nbrs_activity::observer::use_color());
            let margin_visible_width = visible_width(&status_margin) as u16;
            // Row-2 margin replacement for the running-phase
            // status block: progress bar + ETA + spinner
            // (replacing the `│` divider while the phase
            // is in flight). Padded to the same visible
            // width as the row-1 margin so the divider
            // columns align across both rows. Empty string
            // when no phase is active — callers fall back to
            // the standard margin.
            let row2_margin = format_running_phase_row2_margin(
                &snap,
                margin_visible_width,
                nbrs_activity::observer::use_color(),
            );
            let status_cols = (cols as usize)
                .saturating_sub(margin_visible_width as usize);
            // Window mode replaces the status block with a
            // one-line REPL header so the operator knows the
            // surface they're on and how to dismiss it. Bar
            // and Hidden modes use the live status text as
            // usual.
            let new_status_text: Option<String> = if matches!(
                crate::repl_state::current(),
                crate::repl_state::ReplVisibility::Window
            ) {
                let color = nbrs_activity::observer::use_color();
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
            // REPL visibility gate. Three states:
            //
            // - Hidden: no prompt drawn, status block fills
            //   the bottom region normally.
            // - Bar: single-row prompt at the bottom; status
            //   block sits above.
            // - Window: full-screen REPL — the prompt window
            //   expands to fill the available vertical space
            //   (term_rows - 1, leaving one row for the header
            //   label rendered as the first status line) and
            //   the status block is suppressed so the REPL
            //   surface dominates.
            //
            // The state is toggled by `~` / `Ctrl-~`
            // keystrokes the supervisor watches for.
            let repl_vis = crate::repl_state::current();
            let window_mode = matches!(repl_vis,
                crate::repl_state::ReplVisibility::Window);
            let repl_visible = !matches!(repl_vis,
                crate::repl_state::ReplVisibility::Hidden);
            // In Window mode, expand the prompt window to
            // claim (rows - 1) so it fills the screen minus
            // the header label. `set_window_rows` is the
            // single chokepoint into the prompt's geometry —
            // we recompute it every tick because the
            // operator's `Alt-Up` / `Alt-Down` keys also
            // touch it; the REPL-mode override wins per tick
            // by being the last writer.
            if let Some(p) = prompt.as_mut() {
                if window_mode {
                    let target = rows.saturating_sub(2).max(1);
                    if p.window_rows() != target {
                        p.set_window_rows(target);
                        // The `repl_changed` redraw trigger
                        // computed downstream catches the
                        // visibility transition; resizing
                        // within Window mode (e.g. on a
                        // terminal resize) is rare enough
                        // that we accept a one-tick lag
                        // rather than threading another
                        // dirty bit through.
                    }
                }
            }
            let prompt_input = if repl_visible {
                prompt.as_ref().map(|p| {
                    let win_rows = p.window_rows() as usize;
                    // Window mode composes its `rendered`
                    // bytes by hand: top rows are the
                    // transcript tail (oldest at top, newest
                    // just above the input), bottom row is
                    // the standard prompt-input render. Bar
                    // mode falls through to the plain
                    // PromptState render.
                    let rendered = if window_mode && win_rows > 1 {
                        let transcript_rows = win_rows.saturating_sub(1);
                        let tail = crate::repl_state::transcript_tail(transcript_rows);
                        let cols_usize = cols as usize;
                        let mut composed = String::with_capacity(cols_usize * win_rows);
                        // Top-pad with blanks when the
                        // transcript is shorter than the
                        // window so the input stays anchored
                        // at the bottom row of the prompt
                        // region rather than rising as
                        // history grows.
                        let blanks = transcript_rows.saturating_sub(tail.len());
                        for _ in 0..blanks {
                            composed.push_str("\r\n");
                        }
                        for line in tail.iter() {
                            let row = nbrs_activity::activity::truncate_to_width(
                                line, cols_usize);
                            composed.push_str(&row);
                            composed.push_str("\r\n");
                        }
                        // Final row: the prompt's own
                        // single-row render of the input
                        // buffer (`❯ <buffer>` plus cursor).
                        let mut input_row = String::with_capacity(128);
                        // `with_window_override=false` would
                        // be the right shape if we had it;
                        // for now use render() and trust the
                        // prompt has been resized to `target`
                        // — the last row of its multi-row
                        // render is what we want anyway.
                        // Render with window_rows=1 by
                        // grabbing only the last row of the
                        // multi-row render.
                        p.render(&mut input_row, cols_usize, true);
                        let last = input_row.split('\n').last().unwrap_or("");
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
            redraw_bottom_region(
                &mut stderr,
                new_status_text.as_deref(),
                prompt_input.as_ref(),
                cols,
                rows,
                &status_margin,
                margin_visible_width,
                &row2_margin,
            );
            status_drawn = new_status_text;
            prompt_drawn_rows = prompt_input.map(|p| p.window_rows).unwrap_or(0);
            let _ = stderr.flush();
            prompt_dirty = false;
        }
        status_published = next_status;

        // Publish the combined render height so the signal
        // handler in `crate::app` can walk past everything we
        std::thread::sleep(POLL_INTERVAL);
    }

    // Sink shutting down — wipe the regions we own so the
    // post-run terminal state isn't littered with our final
    // tick's text. The signal-handler cleanup path uses
    // `\x1b[999;1H` to park the cursor at the bottom of the
    // viewport regardless of where we left it, so no atomic
    // row count needs to be tracked.
    if prompt_drawn_rows > 0 {
        clear_combined_region(&mut stderr, status_drawn.as_deref(), prompt_drawn_rows);
    } else if status_drawn.is_some() {
        clear_status_region(&mut stderr, status_drawn.as_deref());
    }
}

/// Clear the combined `[status + prompt]` region anchored at
/// the bottom of the terminal. Computes the absolute row of
/// the region's first cell from the terminal height and the
/// prior region's row counts, then erases from there to end
/// of screen.
///
/// `status_rows` is computed from the prior status text's
/// embedded `\n` count + 1. The region's layout (used at
/// redraw time) puts status directly above prompt with no
/// separator row, so the climb math is just
/// `status_rows + prompt_rows`.
fn clear_combined_region<W: Write>(
    out: &mut W,
    prior_status: Option<&str>,
    prompt_rows: u16,
) {
    let status_rows = prior_status
        .map(|s| s.matches('\n').count() as u16 + 1)
        .unwrap_or(0);
    let total_rows = status_rows + prompt_rows;
    if total_rows == 0 {
        let _ = out.flush();
        return;
    }
    // Move cursor to the region's first row at column 0 using
    // absolute positioning relative to the terminal bottom, so
    // a previous scroll doesn't drift the climb math.
    let (_, term_rows) = crossterm::terminal::size().unwrap_or((200, 50));
    let region_top = term_rows.saturating_sub(total_rows.saturating_sub(1));
    let _ = write!(out, "\x1b[{region_top};1H\x1b[J");
    let _ = out.flush();
}

/// Clear the status region most recently rendered into `out`.
/// Counts the embedded `\n`s in the prior render so a multi-
/// line status (future expansion) clears all of its rows, not
/// just the bottom one. Single-line callers see `\r\x1b[K`
/// (the legacy in-place clear).
///
/// Uses relative cursor movement (`\x1b[<N>A`) rather than
/// absolute save/restore (DECSC/DECRC): the latter doesn't
/// survive the screen-scroll that happens when status is
/// rendered at the bottom of the terminal (which is the
/// normal case for an inline log+status sink).
fn clear_status_region<W: Write>(out: &mut W, prior: Option<&str>) {
    let lines = prior.map(|s| s.matches('\n').count() as u16 + 1).unwrap_or(1);
    if lines > 1 {
        // Cursor sits at end of the prior render's last row;
        // climb back to the first row, then `\x1b[J` wipes
        // from the cursor through end of screen.
        let _ = write!(out, "\r\x1b[{}A\x1b[J", lines - 1);
    } else {
        let _ = write!(out, "\r\x1b[K");
    }
    let _ = out.flush();
}

/// Row count a multi-line status string would render to (the
/// number of `\n`-separated rows). Zero when there's nothing
/// drawn.
#[allow(dead_code)]
fn status_rows(prior: Option<&str>) -> u16 {
    prior.map(|s| s.matches('\n').count() as u16 + 1).unwrap_or(0)
}

/// Build the left-margin prefix carrying the session timer
/// (compact 8-char clock from
/// [`nbrs_activity::readouts::format::format_compact_session_elapsed`])
/// + current phase index, followed by a `│` gutter. The
/// magnitude-tracking color span from
/// [`nbrs_activity::readouts::format::session_elapsed_color`]
/// wraps the clock so glance-readability tracks how deep
/// into the run the log line is (dim under a minute,
/// default under an hour, bold beyond).
fn format_margin_prefix(
    snap: &std::sync::Arc<crate::state::RunState>,
    color: bool,
) -> String {
    use nbrs_activity::readouts::format::{
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
/// [`nbrs_activity::activity::terminal_cols`] but also captures
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

/// Redraw the status + prompt block anchored to the bottom of
/// the terminal at absolute positions. Pure-ish helper — takes
/// terminal size + region content explicitly so the call is
/// trivial to unit-test against a `Vec<u8>` writer.
///
/// Layout (rows are 1-indexed; `term_rows` is the bottom):
///
/// ```text
///   [log area]                rows 1 .. status_top - 1
///   [status region]           rows status_top .. status_top + status_rows - 1
///   [prompt region]           rows prompt_top .. term_rows
/// ```
///
/// Cursor on return is at `(term_rows, prompt.cursor_col + 1)`
/// — the prompt's input row at the buffer's cursor column.
pub(crate) fn redraw_bottom_region<W: Write>(
    out: &mut W,
    status_text: Option<&str>,
    prompt: Option<&PromptInput>,
    term_cols: u16,
    term_rows: u16,
    status_margin: &str,
    margin_width: u16,
    row2_margin: &str,
) {
    let _ = term_cols;
    let status_lines: Vec<&str> = status_text
        .map(|s| s.split('\n').collect())
        .unwrap_or_default();
    let status_rows_n = status_lines.len() as u16;
    let prompt_rows = prompt.map(|p| p.window_rows).unwrap_or(0);

    // The combined region's first row is `term_rows -
    // (total - 1)`. Status sits at the top of the region;
    // prompt follows directly below. No separator row.
    let total = status_rows_n + prompt_rows;
    let region_top = term_rows.saturating_sub(total.saturating_sub(1));
    let status_top = region_top;
    let prompt_top = region_top + status_rows_n;

    // Status — position-per-line with erase-to-end-of-line so
    // residue from a wider prior render is scrubbed.
    //
    // Row 0 carries `status_margin` (session timer + phase
    // counter + `│ ` divider). Rows 1+ carry `row2_margin`
    // when present — the running-phase variant: 10-glyph
    // progress bar + ETA + spinner-as-divider, padded to the
    // same visible width so the divider column lines up
    // vertically. When `row2_margin` is empty (no active
    // phase, or a single-line status), every row falls back
    // to the standard margin.
    for (i, row) in status_lines.iter().enumerate() {
        let row_abs = status_top + i as u16;
        let margin = if i == 0 || row2_margin.is_empty() {
            status_margin
        } else {
            row2_margin
        };
        let _ = write!(out, "\x1b[{row_abs};1H\x1b[K{margin}{row}");
    }

    // Prompt — same discipline. `rendered` is the prompt's own
    // render() output; split on `\n` and strip stray `\r`
    // (the prompt's renderer separates rows with `\r\n`). The
    // prompt's `>` glyph sits at the same column as the log
    // content (right of the margin), so the operator reads the
    // input row as the bottom of the same column.
    if let Some(p) = prompt {
        for (i, row) in p.rendered.split('\n').enumerate() {
            let row_abs = prompt_top + i as u16;
            let row = row.strip_suffix('\r').unwrap_or(row);
            let row = row.strip_prefix('\r').unwrap_or(row);
            let _ = write!(out, "\x1b[{row_abs};1H\x1b[K{status_margin}{row}");
        }
        // Place the cursor at the prompt's input row + cursor
        // column. Add `margin_width` so the cursor lands inside
        // the input row's content area, not under the margin.
        let target_col = margin_width + p.cursor_col + 1;
        let _ = write!(out, "\x1b[{term_rows};{target_col}H");
    }
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
    use nbrs_activity::readouts::format::{
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
    // Content visible width = bar (10) + " " + eta + " "
    // Reserve trailing 2 cells for `<spinner><space>` to
    // mirror the standard `│ ` divider shape.
    let content_visible = 10 + 1 + eta_str.chars().count() + 1;
    let divider_visible = 2; // spinner + space
    let total_target = target_width as usize;
    let pad = total_target.saturating_sub(content_visible + divider_visible);
    format!("{bar} {dim}{eta_str}{reset}{:<pad$}{cyan}{spinner}{reset} ", "", pad = pad)
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
        out.push_str(&nbrs_activity::activity::truncate_to_width(row, max_cols));
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

    /// One simulated render tick: status text + prompt.
    fn render_tick(
        status: Option<&str>,
        prompt: Option<&PromptInput>,
    ) -> String {
        let mut out: Vec<u8> = Vec::new();
        // 24-row × 80-col terminal — small enough to make the
        // absolute row arithmetic auditable by eye.
        // Empty margin: legacy tests pin the no-margin layout;
        // the margin-aware behavior is covered in a separate
        // test below.
        redraw_bottom_region(&mut out, status, prompt, 80, 24, "", 0, "");
        String::from_utf8(out).expect("rendered bytes are utf-8")
    }

    fn render_tick_with_margin(
        status: Option<&str>,
        prompt: Option<&PromptInput>,
        margin: &str,
    ) -> String {
        let mut out: Vec<u8> = Vec::new();
        let width = super::visible_width(margin) as u16;
        redraw_bottom_region(&mut out, status, prompt, 80, 24, margin, width, "");
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

    /// Every status row writes to its own absolute terminal
    /// row with `\x1b[K` first to scrub leftovers from a wider
    /// prior tick. Status sits directly above the prompt; no
    /// `\r\n` cursor-advance between regions.
    #[test]
    fn two_line_status_plus_prompt_uses_absolute_positions_at_bottom() {
        let status = "head row\ntail row";
        let prompt = one_row_prompt("");
        let out = render_tick(Some(status), Some(&prompt));
        // Status head at row 22, tail at row 23, prompt at
        // row 24 (terminal is 24 rows; prompt_top = 24 -
        // (1-1) = 24; status_top = 24 - 2 = 22).
        assert!(out.contains("\x1b[22;1H\x1b[Khead row"),
            "status head should anchor at row 22: {out:?}");
        assert!(out.contains("\x1b[23;1H\x1b[Ktail row"),
            "status tail should anchor at row 23: {out:?}");
        assert!(out.contains("\x1b[24;1H\x1b[K\x1b[36m❯\x1b[0m "),
            "prompt should anchor at row 24: {out:?}");
        // Cursor parks at (term_rows, cursor_col + 1).
        assert!(out.contains("\x1b[24;3H"),
            "cursor home should target (24, 3): {out:?}");
    }

    /// Successive status updates each write to the SAME
    /// absolute rows — they don't drift downward across ticks.
    /// This is the invariant the original bug violated (the
    /// user saw status snapshots stacking up in scrollback).
    #[test]
    fn repeated_status_updates_target_identical_absolute_rows() {
        let prompt = one_row_prompt("");
        let mut head_anchors: Vec<bool> = Vec::new();
        for spinner in &["⠙", "⠸", "⠼", "⠏"] {
            let status = format!("{spinner} head row\n      tail row");
            let out = render_tick(Some(&status), Some(&prompt));
            head_anchors.push(
                out.contains(&format!("\x1b[22;1H\x1b[K{spinner} head row"))
            );
        }
        assert!(head_anchors.iter().all(|&ok| ok),
            "every tick must anchor at row 22: {head_anchors:?}");
    }

    /// Status-only render (no prompt). prompt_top falls
    /// through to term_rows, status_top to (term_rows -
    /// status_rows).
    #[test]
    fn status_only_anchors_two_rows_above_bottom() {
        let status = "a\nb";
        let out = render_tick(Some(status), None);
        assert!(out.contains("\x1b[23;1H\x1b[Ka"));
        assert!(out.contains("\x1b[24;1H\x1b[Kb"));
        // No prompt → no final cursor home escape.
        assert!(!out.contains("\x1b[24;3H"),
            "no prompt → no cursor home: {out:?}");
    }

    /// Prompt-only render (no status). Prompt sits at the
    /// bottom row; nothing above it.
    #[test]
    fn prompt_only_anchors_at_bottom_row() {
        let prompt = one_row_prompt("ls");
        let out = render_tick(None, Some(&prompt));
        assert!(out.contains("\x1b[24;1H\x1b[K\x1b[36m❯\x1b[0m ls"),
            "prompt should anchor at row 24: {out:?}");
        // Cursor at column 5 (`❯ ls` = 5 visible cells).
        assert!(out.contains("\x1b[24;5H"),
            "cursor home should target (24, 5): {out:?}");
    }

    // Session-elapsed formatting is now owned by
    // `nbrs_activity::readouts::format::format_compact_session_elapsed`
    // — width invariance and bucket boundaries are pinned by
    // the tests in that module's `tests` mod.

    /// When status shrinks (e.g., a memo header goes away),
    /// the renderer writes the NEW (shorter) status at the
    /// new absolute row. Erase-line on each row scrubs any
    /// leftover content; nothing accumulates above.
    #[test]
    fn status_shrink_writes_new_rows_at_new_absolute_positions() {
        // Tick 1: 3-row status.
        let big = "memo\nhead\ntail";
        let prompt = one_row_prompt("");
        let out = render_tick(Some(big), Some(&prompt));
        assert!(out.contains("\x1b[21;1H\x1b[Kmemo"));
        assert!(out.contains("\x1b[22;1H\x1b[Khead"));
        assert!(out.contains("\x1b[23;1H\x1b[Ktail"));
        assert!(out.contains("\x1b[24;1H\x1b[K\x1b[36m❯\x1b[0m "));

        // Tick 2: 2-row status.
        let small = "head2\ntail2";
        let out = render_tick(Some(small), Some(&prompt));
        assert!(out.contains("\x1b[22;1H\x1b[Khead2"));
        assert!(out.contains("\x1b[23;1H\x1b[Ktail2"));
        assert!(out.contains("\x1b[24;1H\x1b[K\x1b[36m❯\x1b[0m "));
        // The OLD memo row (was at row 21) is NOT touched
        // here — clearing the stale row is the caller's job
        // via `clear_combined_region`. This test only pins
        // the absolute-positioning invariant for the new
        // content; the prior-region clear has its own test.
    }

    /// Status + prompt rows are prefixed with the same margin
    /// the log lines above carry, so content columns line up
    /// vertically across the log/status divide. The cursor lands
    /// in the prompt's content area (past the margin), not under
    /// the margin itself.
    #[test]
    fn status_and_prompt_carry_log_margin_for_column_alignment() {
        let margin = "12.34s 5/9 │ ";
        let status = "running";
        let prompt = one_row_prompt("hi");
        let out = render_tick_with_margin(Some(status), Some(&prompt), margin);
        // Status row inherits the margin prefix so the
        // `running` text starts at the same column as a log
        // row's content.
        assert!(out.contains(&format!("\x1b[23;1H\x1b[K{margin}running")),
            "status row must carry the margin: {out:?}");
        // Prompt row also gets the margin prefix.
        assert!(out.contains(&format!("\x1b[24;1H\x1b[K{margin}\x1b[36m❯\x1b[0m hi")),
            "prompt row must carry the margin: {out:?}");
        // Cursor offset = margin_width + cursor_col + 1.
        // margin is 13 chars visible; "❯ hi" puts cursor_col at
        // 4; final col = 13 + 4 + 1 = 18.
        assert!(out.contains("\x1b[24;18H"),
            "cursor must skip past the margin width: {out:?}");
    }

    /// When the running-phase row-2 margin is supplied (the
    /// new bar+ETA+spinner gutter), line 0 of the status block
    /// inherits the standard row-1 margin and lines 1+ inherit
    /// the row-2 margin. Both columns are drawn at the same
    /// absolute row offsets — the per-row margin choice is the
    /// only thing that changes.
    #[test]
    fn row2_margin_applies_to_line_two_only() {
        let row1 = "12.34s 5/9 │ ";
        let row2 = "⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀ 3s   │ ";
        let status = "running\nstats line";
        let mut out: Vec<u8> = Vec::new();
        redraw_bottom_region(
            &mut out, Some(status), None,
            80, 24,
            row1, super::visible_width(row1) as u16,
            row2,
        );
        let out = String::from_utf8(out).expect("utf-8");
        // Line 0 → row1 margin + `running`.
        assert!(out.contains(&format!("\x1b[23;1H\x1b[K{row1}running")),
            "line 0 MUST carry row1 margin: {out:?}");
        // Line 1 → row2 margin + `stats line`.
        assert!(out.contains(&format!("\x1b[24;1H\x1b[K{row2}stats line")),
            "line 1 MUST carry row2 margin: {out:?}");
    }

    /// Empty row2 margin → every line falls back to row1 (the
    /// no-active-phase baseline path).
    #[test]
    fn empty_row2_margin_uses_row1_for_every_line() {
        let row1 = "12.34s 5/9 │ ";
        let status = "first\nsecond";
        let mut out: Vec<u8> = Vec::new();
        redraw_bottom_region(
            &mut out, Some(status), None,
            80, 24,
            row1, super::visible_width(row1) as u16,
            "", // empty row2_margin → fallback
        );
        let out = String::from_utf8(out).expect("utf-8");
        assert!(out.contains(&format!("\x1b[23;1H\x1b[K{row1}first")),
            "line 0 MUST carry row1 margin: {out:?}");
        assert!(out.contains(&format!("\x1b[24;1H\x1b[K{row1}second")),
            "line 1 MUST also carry row1 margin when row2 is empty: {out:?}");
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
