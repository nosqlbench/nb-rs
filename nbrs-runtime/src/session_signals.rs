// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Session-wide signal handling — the THREE-LEVEL shutdown ladder.
//!
//! A Ctrl-C (SIGINT, or the raw-mode key-watcher's translated
//! keystroke) advances one rung at a time:
//!
//! - **Level 1 — graceful, cooperative.** The session-stop flag is
//!   set; active fiber loops observe it at their cycle boundary and
//!   exit cleanly; end-of-run cleanup runs in the normal order
//!   (profiler flush, cadence reporter shutdown, metrics.db WAL
//!   consolidation, summary writes). A visible 10-second countdown
//!   starts: if the drain hasn't finished when it expires, the
//!   ladder advances to level 2 automatically.
//! - **Level 2 — cancel in-flight ops, keep process cleanup.**
//!   Entered by the countdown expiring or a second Ctrl-C. Ops
//!   parked inside a hung adapter call (a request that will only
//!   ever end by client timeout) are CANCELLED — their futures are
//!   dropped at the fiber's dispatch point — so the drain completes
//!   and the process-level graceful shutdown (WAL consolidation,
//!   summaries) still runs. This is the rung that used to not
//!   exist: previously a second Ctrl-C hard-exited, skipping WAL
//!   cleanup exactly when hung ops made it matter.
//! - **Level 3 — force-exit.** A further Ctrl-C once level 2 is in
//!   force exits immediately (`process::exit(130)`); metrics and
//!   profiler output may be incomplete.
//!
//! The state is intentionally global: there is one session per
//! process by construction. Tests shouldn't need to install or
//! consult it (no `RunObserver` test sets up signals).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

/// Shared session-stop flag. Initialized lazily on first read or
/// the call to [`install_signal_handler`].
static SESSION_STOP: OnceLock<Arc<AtomicBool>> = OnceLock::new();

fn flag() -> &'static Arc<AtomicBool> {
    SESSION_STOP.get_or_init(|| Arc::new(AtomicBool::new(false)))
}

/// Returns `true` once a stop has been requested for the current execution.
/// Cheap relaxed atomic load — safe to call from a hot fiber loop.
///
/// SRD-88: a fiber running inside an [`ExecutionContext`](crate::execution_context)
/// stops when EITHER the process-global session stop (Ctrl-C, which halts
/// *every* execution) OR its own per-execution stop flag is set — so a stop
/// scoped to one execution isolates to that execution while Ctrl-C still stops
/// all. Outside any execution scope (single-run / CLI / tests) only the global
/// flag applies — behavior is identical to before the seam (axiom A1).
#[inline]
pub fn stop_requested() -> bool {
    let global = SESSION_STOP.get()
        .map(|f| f.load(Ordering::Relaxed))
        .unwrap_or(false);
    let local = crate::execution_context::current_stop()
        .map(|f| f.load(Ordering::Relaxed))
        .unwrap_or(false);
    global || local
}

/// Programmatically request a session-wide stop. Used by the
/// signal handler, but also available to other lifecycle code
/// that wants to short-circuit the run.
pub fn request_stop() {
    flag().store(true, Ordering::Relaxed);
}

// ─── The shutdown ladder (module doc) ────────────────────────────────

/// Countdown from level 1 (graceful) to level 2 (cancel in-flight ops).
const SHUTDOWN_COUNTDOWN_SECS: u64 = 10;

/// Ladder level, published through a `watch` channel so per-fiber op
/// dispatch can race a pending adapter call against the cancel rung
/// without polling. 0 = running, 1 = graceful, 2 = cancel-ops.
/// (Level 3 — force-exit — never publishes; it exits.)
static SHUTDOWN: OnceLock<tokio::sync::watch::Sender<u8>> = OnceLock::new();

/// Set once the runner's process-level shutdown has completed — the
/// countdown thread goes quiet instead of announcing op cancellation
/// for a run that has already drained.
static SHUTDOWN_DONE: OnceLock<Arc<AtomicBool>> = OnceLock::new();

fn shutdown_tx() -> &'static tokio::sync::watch::Sender<u8> {
    SHUTDOWN.get_or_init(|| tokio::sync::watch::channel(0u8).0)
}

fn done_flag() -> &'static Arc<AtomicBool> {
    SHUTDOWN_DONE.get_or_init(|| Arc::new(AtomicBool::new(false)))
}

/// The ladder level currently in force.
#[inline]
pub fn shutdown_level() -> u8 {
    SHUTDOWN.get().map(|tx| *tx.borrow()).unwrap_or(0)
}

/// True once level 2 has been entered — in-flight ops should cancel.
#[inline]
pub fn cancel_ops_requested() -> bool {
    shutdown_level() >= 2
}

/// Subscribe to ladder-level changes. Each fiber holds one receiver
/// and races its op dispatch against [`ops_cancelled`].
pub fn subscribe_shutdown() -> tokio::sync::watch::Receiver<u8> {
    shutdown_tx().subscribe()
}

/// Resolves when the cancel rung (level ≥ 2) is in force. Ready
/// immediately if it already is. Used in a `select!` against the op
/// future at the fiber's dispatch point — dropping the op future is
/// the cancellation.
pub async fn ops_cancelled(rx: &mut tokio::sync::watch::Receiver<u8>) {
    loop {
        if *rx.borrow() >= 2 {
            return;
        }
        if rx.changed().await.is_err() {
            // Sender can't drop (static), but never spin if it did.
            std::future::pending::<()>().await;
        }
    }
}

/// Mark the runner's process-level shutdown complete: the countdown
/// (if still running) goes quiet, and further escalations are moot.
pub fn mark_shutdown_complete() {
    done_flag().store(true, Ordering::Relaxed);
}

/// What advanced the shutdown ladder — used only to phrase the level-1
/// announcement accurately. A programmatic `action: abort` trip drives the
/// SAME ladder as Ctrl-C, so it must not be reported as a Ctrl-C.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOrigin {
    /// The SIGINT / Ctrl-C watcher (`install_signal_handler`).
    CtrlC,
    /// A `stop_when` condition with `action: abort` (SRD-83 follow-up).
    StopAction,
}

impl ShutdownOrigin {
    /// The leading clause of the level-1 log line, naming the trigger. The
    /// trailing `(Ctrl-C: cancel them now …)` guidance stays fixed — Ctrl-C
    /// escalates the ladder regardless of what first advanced it.
    fn lead(self) -> &'static str {
        match self {
            ShutdownOrigin::CtrlC =>
                "session: graceful shutdown requested (Ctrl-C).",
            ShutdownOrigin::StopAction =>
                "session: shutdown requested by stop action `abort`.",
        }
    }
}

/// Advance the ladder ONE rung: 0 → 1 (graceful + countdown),
/// 1 → 2 (cancel in-flight ops). At ≥ 2 this is a no-op returning the
/// current level — the FORCE-EXIT decision (level 3) stays with the
/// caller, which owns its own terminal hygiene (the raw-mode
/// key-watcher must restore the terminal before exiting; the SIGINT
/// watcher just exits). `origin` phrases the level-1 line only. Returns
/// the level now in force.
pub fn escalate_shutdown(origin: ShutdownOrigin) -> u8 {
    let tx = shutdown_tx();
    let mut entered: u8 = 0;
    tx.send_if_modified(|level| {
        if *level >= 2 {
            entered = *level;
            false
        } else {
            *level += 1;
            entered = *level;
            true
        }
    });
    match entered {
        1 => {
            request_stop();
            crate::diag!(
                crate::observer::LogLevel::Info,
                "{} Active fibers exit at the next cycle boundary; profiler / \
                 metrics / summaries will flush. In-flight ops will be \
                 CANCELLED in {SHUTDOWN_COUNTDOWN_SECS}s (Ctrl-C: cancel \
                 them now; a further Ctrl-C force-exits).",
                origin.lead()
            );
            // Not under `cfg(test)`: the in-crate unit tests exercise the
            // ladder's transitions against process-global state; a live
            // countdown escalating that state seconds later would race
            // every concurrently-running test. Integration tests (their
            // own processes) compile the lib without `test` and get the
            // real countdown.
            #[cfg(not(test))]
            spawn_cancel_countdown();
        }
        2 => announce_cancel_ops(),
        _ => {}
    }
    entered
}

/// Enter the cancel rung directly (countdown expiry). Idempotent.
/// (Only the countdown calls this, and the countdown is compiled out
/// of the in-crate unit-test build — see `escalate_shutdown`.)
#[cfg_attr(test, allow(dead_code))]
fn escalate_cancel_ops() {
    let tx = shutdown_tx();
    let modified = tx.send_if_modified(|level| {
        if *level < 2 {
            *level = 2;
            true
        } else {
            false
        }
    });
    if modified {
        announce_cancel_ops();
    }
}

fn announce_cancel_ops() {
    crate::diag!(
        crate::observer::LogLevel::Warn,
        "session: cancelling in-flight ops — process-level cleanup \
         (metrics flush, WAL consolidation, summaries) continues. \
         Ctrl-C again to force-exit."
    );
}

/// The visible level-1 → level-2 countdown. A plain thread (both
/// entry points can spawn it — the raw-mode key-watcher runs outside
/// the tokio runtime): one line per second, silenced the moment the
/// run drains ([`mark_shutdown_complete`]) or the ladder advances by
/// keypress; escalates to the cancel rung on expiry.
#[cfg_attr(test, allow(dead_code))]
fn spawn_cancel_countdown() {
    let done = done_flag().clone();
    std::thread::Builder::new()
        .name("shutdown-countdown".into())
        .spawn(move || {
            for remaining in (1..=SHUTDOWN_COUNTDOWN_SECS).rev() {
                if done.load(Ordering::Relaxed) || shutdown_level() >= 2 {
                    return;
                }
                crate::diag!(
                    crate::observer::LogLevel::Info,
                    "session: cancelling in-flight ops in {remaining}s \
                     (Ctrl-C to cancel now)"
                );
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            if !done.load(Ordering::Relaxed) {
                escalate_cancel_ops();
            }
        })
        .expect("spawn shutdown-countdown thread");
}

/// Shared "a stop condition gracefully halted the walk" flag. Distinct
/// from [`SESSION_STOP`] (Ctrl-C): set when a workload-shell stop
/// condition (SRD-83) intentionally halts the remaining walk. The
/// end-of-run unreached-phase check consults it to distinguish an
/// *intended* early stop (later phases deliberately skipped, not an
/// error) from the genuine "a phase failed and stranded downstream
/// phases" case.
static GRACEFUL_STOP: OnceLock<Arc<AtomicBool>> = OnceLock::new();

/// True once a stop condition has gracefully halted the walk.
#[inline]
pub fn graceful_stop_requested() -> bool {
    GRACEFUL_STOP.get()
        .map(|f| f.load(Ordering::Relaxed))
        .unwrap_or(false)
}

/// Record that a stop condition gracefully halted the remaining walk
/// (SRD-83 workload shell). Idempotent.
pub fn request_graceful_stop() {
    GRACEFUL_STOP.get_or_init(|| Arc::new(AtomicBool::new(false)))
        .store(true, Ordering::Relaxed);
}

/// Why a shell-driven stop halted the walk (SRD-82 Part 4). A
/// [`StopCause::Fault`] is a `fail`-effect trip — a child phase failed,
/// so the run's validity is `Failed` and the halt records
/// `Interrupted + Failed`. A [`StopCause::Interrupt`] is a clean `stop`
/// (a graceful condition or user Ctrl-C) — later phases are deliberately
/// skipped and the result is re-usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCause {
    Interrupt,
    Fault,
}

/// Shared "a `fail`-effect stop condition halted the walk" flag (SRD-82
/// Part 4). Distinct from [`GRACEFUL_STOP`]: a fault halt skips the tail
/// phases (so the unreached-phase check stays quiet, like graceful) but
/// the run still exits non-zero — the failing phase's own `Err` carries
/// that via the `run_result` path. The flag records the *cause* so the
/// end-of-run accounting and the trip log read "fault", not "graceful".
static FAULT_STOP: OnceLock<Arc<AtomicBool>> = OnceLock::new();

/// True once a `fail`-effect stop condition has halted the walk.
#[inline]
pub fn fault_stop_requested() -> bool {
    FAULT_STOP.get()
        .map(|f| f.load(Ordering::Relaxed))
        .unwrap_or(false)
}

/// Record that a `fail`-effect stop condition halted the remaining walk
/// (SRD-82 Part 4, `StopCause::Fault`). Idempotent.
pub fn request_fault_stop() {
    FAULT_STOP.get_or_init(|| Arc::new(AtomicBool::new(false)))
        .store(true, Ordering::Relaxed);
}

/// Route a shell stop to the right session signal by its
/// [`StopCause`]. Both halt the tail; `Fault` additionally marks the
/// run failed (non-zero exit via the failing phase's `Err`).
pub fn request_shell_stop(cause: StopCause) {
    match cause {
        StopCause::Fault => request_fault_stop(),
        StopCause::Interrupt => request_graceful_stop(),
    }
}

/// SRD-92 Step 0 — one cooperative-stop view, consulted at every boundary.
/// Bundles the per-execution stop sources so a boundary check is a single
/// call, and so a unit that previously held only ONE flag (the `while:`
/// wrapper held only the activity `stop_flag`) observes ALL of them. The
/// global / per-execution session stop ([`stop_requested`]) is always
/// folded in; a `fail`-effect global ([`fault_stop_requested`]) is folded
/// into [`StopView::poll`].
///
/// The `daemon` source (the SRD-82 Part 6 daemon-group completion) is a
/// CLEAN termination: it ends a loop ([`StopView::stopped`]) but is NOT a
/// fault, so it is excluded from [`StopView::abnormal`] (the
/// failure-determining set). Fiber-pool scale-down
/// ([`crate::fiber_pool::StopFlag`]) is a per-fiber concern, deliberately
/// NOT part of this view.
#[derive(Clone, Default)]
pub struct StopView {
    activity: Option<Arc<AtomicBool>>,
    walk: Option<Arc<AtomicBool>>,
    daemon: Option<Arc<AtomicBool>>,
}

impl StopView {
    /// Build from the per-execution stop sources (any may be absent).
    pub fn new(
        activity: Option<Arc<AtomicBool>>,
        walk: Option<Arc<AtomicBool>>,
        daemon: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self { activity, walk, daemon }
    }

    #[inline]
    fn on(f: &Option<Arc<AtomicBool>>) -> bool {
        f.as_ref().is_some_and(|b| b.load(Ordering::Relaxed))
    }

    /// Any cooperative stop — incl. the clean daemon-group completion and
    /// the global / per-execution session stop. Use at loop BREAK boundaries.
    #[inline]
    pub fn stopped(&self) -> bool {
        Self::on(&self.activity)
            || stop_requested()
            || Self::on(&self.walk)
            || Self::on(&self.daemon)
    }

    /// A stop that marks the unit FAILED / abnormal — EXCLUDES the clean
    /// daemon-group stop. Use for the failure-determining return.
    #[inline]
    pub fn abnormal(&self) -> bool {
        Self::on(&self.activity) || stop_requested() || Self::on(&self.walk)
    }

    /// The stop CAUSE for shell-level recording (SRD-82 Part 4): a fault
    /// (the activity error-handler `stop_flag`, or a `fail`-effect global)
    /// outranks a clean interrupt.
    #[inline]
    pub fn poll(&self) -> Option<StopCause> {
        if fault_stop_requested() || Self::on(&self.activity) {
            Some(StopCause::Fault)
        } else if self.stopped() {
            Some(StopCause::Interrupt)
        } else {
            None
        }
    }
}

/// Install a tokio task that watches `ctrl_c()` and drives SIGINT
/// through the three-level ladder described in the module doc.
/// Idempotent — only the first call wins; subsequent calls are
/// no-ops. Must be called from inside a tokio runtime context.
pub fn install_signal_handler() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    if INSTALLED.set(()).is_err() {
        return;
    }
    // Touch the flag to ensure it's initialized before any
    // observer or fiber checks `stop_requested()`.
    let _ = flag();
    tokio::spawn(async move {
        // Every Ctrl-C advances one rung. The messages route through
        // `crate::diag!` so they reach every sink the rest of the
        // runtime uses (session.log via the async sink, plus the
        // registered RunObserver — the TUI log panel in TUI mode, the
        // stderr fallback otherwise). The leading-newline cosmetics
        // for the terminal-echoed `^C` live in [`StderrObserver::log`]
        // so the structured log isn't littered with blank lines.
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                return;
            }
            if shutdown_level() >= 2 {
                // Level 3: force-exit.
                crate::diag!(
                    crate::observer::LogLevel::Warn,
                    "session: force-exit (Ctrl-C past the cancel rung) — \
                     profiler output and metrics may be incomplete."
                );
                std::process::exit(130);
            }
            escalate_shutdown(ShutdownOrigin::CtrlC);
        }
    });
}

/// Test-only: serialize the tests that touch the process-global
/// `SESSION_STOP` flag. `SESSION_STOP` is a never-reset
/// `OnceLock<Arc<AtomicBool>>`, so a test that sets it would
/// otherwise leak into every sibling test in the same binary
/// (notably the per-execution isolation test, which requires the
/// global to be clear). Tests acquire this lock and clear the flag
/// before asserting.
#[cfg(test)]
pub(crate) static STOP_GLOBAL_TEST_LOCK: std::sync::Mutex<()> =
    std::sync::Mutex::new(());

/// Test-only: reset the process-global stop flag to unset. Paired
/// with [`STOP_GLOBAL_TEST_LOCK`] so global-flag tests don't leak
/// state into each other.
#[cfg(test)]
pub(crate) fn clear_session_stop_for_test() {
    flag().store(false, Ordering::Relaxed);
}

/// Test-only: reset the shutdown ladder to level 0 (and clear the
/// done flag). Same locking discipline as
/// [`clear_session_stop_for_test`].
#[cfg(test)]
pub(crate) fn reset_shutdown_ladder_for_test() {
    let _ = shutdown_tx().send_replace(0);
    done_flag().store(false, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_starts_unset_and_responds_to_request() {
        // Serialize with the other global-flag test and reset the
        // process-global flag around the assertions so this test
        // neither sees nor leaks a stale stop.
        let _guard = STOP_GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_session_stop_for_test();
        assert!(!stop_requested());
        request_stop();
        assert!(stop_requested());
        clear_session_stop_for_test();
    }

    /// The ladder advances one rung per escalation — 0 → 1 (graceful,
    /// session stop set) → 2 (cancel ops) — and holds at 2:
    /// `escalate_shutdown` never enters level 3 itself (force-exit is
    /// the CALLER's decision, with its own terminal hygiene).
    #[test]
    fn ladder_advances_one_rung_per_escalation_and_holds_at_cancel() {
        let _guard = STOP_GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_session_stop_for_test();
        reset_shutdown_ladder_for_test();

        assert_eq!(shutdown_level(), 0);
        assert!(!cancel_ops_requested());

        assert_eq!(escalate_shutdown(ShutdownOrigin::CtrlC), 1, "first rung: graceful");
        assert!(stop_requested(), "graceful rung sets the session stop");
        assert!(!cancel_ops_requested());

        assert_eq!(escalate_shutdown(ShutdownOrigin::CtrlC), 2, "second rung: cancel ops");
        assert!(cancel_ops_requested());

        assert_eq!(escalate_shutdown(ShutdownOrigin::CtrlC), 2, "ladder holds at cancel");
        assert!(cancel_ops_requested());

        clear_session_stop_for_test();
        reset_shutdown_ladder_for_test();
    }

    /// `ops_cancelled` resolves the moment the cancel rung is in force
    /// — including when it already was at subscribe time — and does
    /// NOT resolve for the graceful rung alone.
    #[tokio::test]
    async fn ops_cancelled_resolves_at_cancel_rung_only() {
        let _guard = STOP_GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_session_stop_for_test();
        reset_shutdown_ladder_for_test();

        // Graceful rung alone must NOT resolve the cancel future.
        let mut rx = subscribe_shutdown();
        escalate_shutdown(ShutdownOrigin::CtrlC); // → 1
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            ops_cancelled(&mut rx),
        ).await;
        assert!(pending.is_err(), "graceful rung must not cancel ops");

        // Cancel rung resolves it — and resolves immediately for a
        // subscriber that arrives after the fact.
        escalate_shutdown(ShutdownOrigin::CtrlC); // → 2
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            ops_cancelled(&mut rx),
        ).await.expect("cancel rung resolves the in-flight race");
        let mut late = subscribe_shutdown();
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            ops_cancelled(&mut late),
        ).await.expect("already-cancelled resolves immediately");

        clear_session_stop_for_test();
        reset_shutdown_ladder_for_test();
    }
}
