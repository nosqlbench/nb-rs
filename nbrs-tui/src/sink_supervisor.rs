// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `SinkSupervisor` — coordinates the active
//! [`crate::display_sink::DisplaySink`] and the
//! [`crate::key_watcher::KeyWatcher`] across a `tui=terminal`
//! run.
//!
//! ## Why
//!
//! `tui=terminal` is interactive: the operator can hit Ctrl-T
//! at any moment to swap from line-mode rendering
//! (`LogOnlySink`) up to the full TUI (`TuiSink`), and Ctrl-T
//! again (or `q`) inside the TUI to swap back. The supervisor
//! is the one component that watches for those signals and
//! drives the sink lifecycle.
//!
//! ## State machine
//!
//! ```text
//!   ┌─ Terminal ──── Ctrl-T ──→  TUI ───────┐
//!   │   • LogOnlySink                       │
//!   │   • KeyWatcher (raw stdin)            │
//!   │                                       │
//!   └────────── Ctrl-T or q ←───────────────┘
//!                              (App writes
//!                              `yielded_to_terminal`)
//! ```
//!
//! Stdin ownership is exclusive: only one of {KeyWatcher, App}
//! has raw-mode read access at a time. The transition function
//! always tears the outgoing side down before bringing the
//! incoming side up.
//!
//! ## Lifetime
//!
//! Owns its own OS thread (not async). The runner's async
//! future runs in parallel; when it completes, the runner
//! drops a sentinel into the supervisor's "run done" channel
//! and the supervisor tears the active sink + watcher down
//! before exiting.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::display_sink::{DisplayInputs, DisplaySink, SinkHandle};
use crate::key_watcher::{KeyWatcher, WatcherSignal};
use crate::log_only_observer::LogOnlyObserver;
use crate::log_only_sink::LogOnlySink;
use crate::run_state_actor::RunStateHandle;
use crate::tui_sink::{TuiSink, TuiSinkSync};

/// Handle held by the runner-side caller. The supervisor runs
/// on its own OS thread; [`Self::shutdown`] is called once
/// the runner future completes.
pub struct SinkSupervisor {
    /// Sentinel channel: drop the sender (in `shutdown`) and
    /// the supervisor's `try_recv` returns `Disconnected`,
    /// breaking the loop.
    done_tx: Option<mpsc::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl SinkSupervisor {
    /// Spawn the supervisor. Initial state is the terminal sink
    /// (`LogOnlySink` + `KeyWatcher`). If stdin isn't a TTY,
    /// the watcher refuses to spawn and the supervisor exits
    /// immediately — leaving the observer's synchronous
    /// stderr write path active (legacy `tui=off` behaviour).
    pub fn spawn(
        observer: Arc<LogOnlyObserver>,
        state: RunStateHandle,
    ) -> Self {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join = std::thread::Builder::new()
            .name("sink-supervisor".into())
            .spawn(move || run_supervision(observer, state, done_rx))
            .expect("spawn sink-supervisor thread");
        Self { done_tx: Some(done_tx), join: Some(join) }
    }

    /// Tell the supervisor the run has finished. Tears down
    /// the active sink + watcher and joins. Idempotent.
    pub fn shutdown(mut self) {
        drop(self.done_tx.take());
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// What the supervisor is currently driving. Tear-down + bring-
/// up between modes is sequential — never side-by-side stdin.
enum ActiveSink {
    Terminal {
        sink_handle: Box<dyn SinkHandle>,
        watcher: KeyWatcher,
        signal_rx: mpsc::Receiver<WatcherSignal>,
    },
    Tui {
        sink_handle: Box<dyn SinkHandle>,
        sync: TuiSinkSync,
    },
}

fn run_supervision(
    observer: Arc<LogOnlyObserver>,
    state: RunStateHandle,
    done_rx: mpsc::Receiver<()>,
) {
    // Two distinct flags the supervisor manages:
    //   • sink_active     — observer's synchronous-stderr
    //                       suppression. Held high while *any*
    //                       sink is rendering (LogOnlySink
    //                       sets/clears it on its own; the
    //                       supervisor re-asserts during the
    //                       TUI swap).
    //   • inline_suppress — activity's inline-status thread
    //                       suppression. Held high *only*
    //                       while an alt-screen TUI owns the
    //                       terminal — i.e. during the TuiSink
    //                       window. Cleared in plain
    //                       `tui=terminal` so the per-cycle
    //                       status line keeps rendering
    //                       alongside the LogOnlySink's log
    //                       drain (they share stderr without
    //                       conflict).
    let sink_active_flag = observer.sink_active_flag();
    let inline_suppress = observer.inline_suppress_flag();
    let mut active = match start_terminal(&observer, &state) {
        Some(a) => a,
        None => {
            // No TTY — KeyWatcher refused. Fall through:
            // the observer's synchronous stderr-write path is
            // already active (sink_active stays false), so the
            // operator gets the same output the legacy
            // `tui=off` mode delivered.
            wait_for_done(&done_rx);
            return;
        }
    };

    const TICK: Duration = Duration::from_millis(100);

    loop {
        // Runner finished?
        match done_rx.try_recv() {
            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => {}
        }

        // Per-state handling. Each branch may swap `active`
        // and `continue`-loop, restarting the polling cycle.
        let mut swap_to: Option<Transition> = None;
        match &active {
            ActiveSink::Terminal { signal_rx, .. } => {
                while let Ok(sig) = signal_rx.try_recv() {
                    match sig {
                        WatcherSignal::ToggleTui => {
                            swap_to = Some(Transition::TerminalToTui);
                            break;
                        }
                        WatcherSignal::Interrupt => {
                            // Re-raise SIGINT so the runtime's
                            // graceful-shutdown handler picks
                            // it up via the same path as a
                            // shell-issued Ctrl-C.
                            unsafe { libc::raise(libc::SIGINT); }
                        }
                        WatcherSignal::Suspend => {
                            // Honour the user's expectation that
                            // Ctrl-Z suspends the process, just
                            // as a cooked-mode shell would. We
                            // briefly drop raw mode so the
                            // foreground/background dance behaves
                            // sensibly, raise SIGTSTP, and re-
                            // enable raw mode after the OS
                            // resumes us via `fg`.
                            let _ = crossterm::terminal::disable_raw_mode();
                            unsafe { libc::raise(libc::SIGTSTP); }
                            // Execution resumes here when `fg`
                            // delivers SIGCONT. Re-arm the
                            // terminal for keystroke detection.
                            let _ = crossterm::terminal::enable_raw_mode();
                        }
                        WatcherSignal::Redraw => {
                            // Standard cooked-terminal Ctrl-L:
                            // clear the screen and park the
                            // cursor at the home position. The
                            // LogOnlySink will continue printing
                            // new lines on its normal cadence —
                            // no replay of historical buffer
                            // (matches what users see from a
                            // cooked-shell `clear`).
                            use std::io::Write;
                            let mut err = std::io::stderr();
                            let _ = err.write_all(b"\x1b[2J\x1b[H");
                            let _ = err.flush();
                        }
                    }
                }
            }
            ActiveSink::Tui { sync, .. } => {
                if sync.yielded.load(Ordering::Acquire) {
                    swap_to = Some(Transition::TuiToTerminal);
                }
            }
        }

        if let Some(t) = swap_to {
            active = match t {
                Transition::TerminalToTui => swap_to_tui(&observer, &state, active),
                Transition::TuiToTerminal => {
                    // Wait for the App thread to fully exit and
                    // restore the terminal before bringing the
                    // KeyWatcher back up (otherwise both might
                    // claim raw mode at once).
                    teardown(active);
                    // Release the inline-status suppression now
                    // that the alt-screen is gone — the
                    // LogOnlySink's log drain and the inline
                    // status line coexist on stderr without
                    // conflict, so the per-cycle status should
                    // be visible again. `sink_active` is
                    // re-asserted by `start_terminal` →
                    // `LogOnlySink::start`.
                    observer.inline_suppress_flag()
                        .store(false, Ordering::Release);
                    match start_terminal(&observer, &state) {
                        Some(a) => a,
                        None => {
                            // Lost the TTY (unexpected). Fall
                            // through to no-supervisor; runner
                            // continues with synchronous stderr.
                            return;
                        }
                    }
                }
            };
            continue;
        }

        std::thread::sleep(TICK);
    }

    teardown(active);
    // Final clear: whichever sink we tore down might or might
    // not have left the flag set (LogOnlySink clears
    // sink_active on shutdown; TuiSink doesn't touch it).
    // After supervisor exit there's nothing rendering, so the
    // inline-status thread (if still alive) and any straggler
    // synchronous-stderr writes from the observer should be
    // unsuppressed.
    sink_active_flag.store(false, Ordering::Release);
    inline_suppress.store(false, Ordering::Release);
}

fn wait_for_done(done_rx: &mpsc::Receiver<()>) {
    // No supervised state — just block until the runner
    // signals completion.
    let _ = done_rx.recv();
}

enum Transition {
    TerminalToTui,
    TuiToTerminal,
}

fn start_terminal(
    observer: &Arc<LogOnlyObserver>,
    state: &RunStateHandle,
) -> Option<ActiveSink> {
    let (signal_tx, signal_rx) = mpsc::channel::<WatcherSignal>();
    let watcher = KeyWatcher::spawn(signal_tx)?;

    let min_level = observer.min_level();
    let sink_active = observer.sink_active_flag();
    let sink = Box::new(LogOnlySink::new(min_level, sink_active));
    let sink_handle = sink.start(DisplayInputs {
        state: state.clone(),
        frame_rx: None,
        metrics_query: None,
    });

    Some(ActiveSink::Terminal { sink_handle, watcher, signal_rx })
}

fn swap_to_tui(
    observer: &Arc<LogOnlyObserver>,
    state: &RunStateHandle,
    active: ActiveSink,
) -> ActiveSink {
    // Tear down terminal mode first — the watcher disables
    // raw mode + releases stdin so the App can claim it.
    if let ActiveSink::Terminal { sink_handle, watcher, .. } = active {
        sink_handle.shutdown();
        watcher.shutdown();
    } else {
        unreachable!("swap_to_tui called outside Terminal state");
    }

    let frame_rx = observer.subscribe_frames();
    let query = match observer.metrics_query() {
        Some(q) => q,
        None => {
            // Cadence reporter not yet wired by the runner —
            // this can only happen if the user hits Ctrl-T
            // before the first phase fires. Fall back to
            // terminal mode with a one-line notice.
            let _ = std::io::Write::write_all(
                &mut std::io::stderr(),
                b"Ctrl-T: TUI not yet ready (metrics scheduler pending) - retry once the run is underway\r\n",
            );
            return start_terminal(observer, state)
                .expect("re-entering terminal mode after deferred swap");
        }
    };

    // Two flags now flip on the way into TUI mode:
    //   • sink_active — re-asserted because LogOnlySink's
    //     shutdown cleared it, and the TUI also "renders log
    //     lines" (inside its own panel) so the observer must
    //     stay quiet on stderr.
    //   • inline_suppress — first becomes true here. The TUI
    //     owns the alt-screen; the inline-status thread's
    //     `\r\x1b[K…` would otherwise overwrite the TUI's
    //     bottom-row content.
    observer.sink_active_flag().store(true, Ordering::Release);
    observer.inline_suppress_flag().store(true, Ordering::Release);

    let sync = TuiSinkSync::default();
    let sink = Box::new(TuiSink::new(frame_rx, query, sync.clone()));
    let sink_handle = sink.start(DisplayInputs {
        state: state.clone(),
        frame_rx: None,
        metrics_query: None,
    });

    ActiveSink::Tui { sink_handle, sync }
}

fn teardown(active: ActiveSink) {
    match active {
        ActiveSink::Terminal { sink_handle, watcher, .. } => {
            sink_handle.shutdown();
            watcher.shutdown();
        }
        ActiveSink::Tui { sink_handle, .. } => {
            sink_handle.shutdown();
        }
    }
}
