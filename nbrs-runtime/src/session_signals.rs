// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Session-wide signal handling.
//!
//! Installs a `tokio::signal::ctrl_c` watcher that translates
//! SIGINT into a cooperative shutdown:
//!
//! - **First Ctrl-C** sets the session-stop flag. Active fiber
//!   loops observe the flag at their cycle boundary (alongside
//!   the existing per-activity `stop_flag`) and exit cleanly.
//!   Control returns up the runner stack so end-of-run cleanup
//!   runs in the normal order: profiler flush, cadence reporter
//!   shutdown, summary writes.
//! - **Second Ctrl-C** within the active session forces an
//!   immediate `process::exit(130)` — the operator has decided
//!   they don't want to wait for graceful shutdown.
//!
//! The flag is intentionally a global: there is one session per
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

/// Install a tokio task that watches `ctrl_c()` and translates
/// SIGINT into the two-stage shutdown described in the module
/// doc. Idempotent — only the first call wins; subsequent calls
/// are no-ops. Must be called from inside a tokio runtime
/// context.
pub fn install_signal_handler() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    if INSTALLED.set(()).is_err() {
        return;
    }
    // Touch the flag to ensure it's initialized before any
    // observer or fiber checks `stop_requested()`.
    let stop = flag().clone();
    tokio::spawn(async move {
        // First Ctrl-C: set the flag, log, return. Routed
        // through `crate::diag!` so the message reaches every
        // sink the rest of the runtime uses (session.log via
        // the async sink, plus the registered RunObserver — the
        // TUI log panel in TUI mode, the stderr fallback
        // otherwise). The leading-newline cosmetics for the
        // terminal-echoed `^C` live in [`StderrObserver::log`]
        // so the structured log isn't littered with blank lines.
        if tokio::signal::ctrl_c().await.is_ok() {
            stop.store(true, Ordering::Relaxed);
            crate::diag!(
                crate::observer::LogLevel::Info,
                "session: graceful shutdown requested (Ctrl-C). \
                 Active fibers will exit at the next cycle \
                 boundary; profiler / metrics / summaries will \
                 flush. Press Ctrl-C again to force-exit."
            );
        }
        // Second Ctrl-C: hard exit.
        if tokio::signal::ctrl_c().await.is_ok() {
            crate::diag!(
                crate::observer::LogLevel::Warn,
                "session: force-exit on second Ctrl-C — \
                 profiler output and metrics may be incomplete."
            );
            std::process::exit(130);
        }
    });
}

/// Drive the graceful-shutdown stage directly — for callers that detect a
/// Ctrl-C WITHOUT a SIGINT. In the interactive raw-mode key-watcher the
/// terminal's Ctrl-C→SIGINT translation is OFF, so the keystroke never
/// becomes a signal; and re-raising SIGINT there is unreliable because the
/// TUI's `install_signal_terminal_restore` sigaction handler intercepts
/// SIGINT and hard-terminates before tokio's graceful handler can run. The
/// key-watcher supervisor calls this instead, setting the same stop flag the
/// SIGINT handler sets and emitting the same operator notice. Idempotent:
/// only the first call (flag false→true) sets the flag and logs; the caller
/// escalates a subsequent Ctrl-C to a force-exit itself.
pub fn trigger_graceful_stop() {
    if !flag().swap(true, Ordering::Relaxed) {
        crate::diag!(
            crate::observer::LogLevel::Info,
            "session: graceful shutdown requested (Ctrl-C). Active fibers \
             will exit at the next cycle boundary; profiler / metrics / \
             summaries will flush. Press Ctrl-C again to force-exit."
        );
    }
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
}
