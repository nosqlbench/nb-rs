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

/// Returns `true` once a session-wide stop has been requested.
/// Cheap relaxed atomic load — safe to call from a hot fiber loop.
#[inline]
pub fn stop_requested() -> bool {
    SESSION_STOP.get()
        .map(|f| f.load(Ordering::Relaxed))
        .unwrap_or(false)
}

/// Programmatically request a session-wide stop. Used by the
/// signal handler, but also available to other lifecycle code
/// that wants to short-circuit the run.
pub fn request_stop() {
    flag().store(true, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_starts_unset_and_responds_to_request() {
        // The flag is process-global; if another test in this
        // same process has already set it, just verify the
        // request_stop / stop_requested wiring works.
        if !stop_requested() {
            assert!(!stop_requested());
            request_stop();
            assert!(stop_requested());
        } else {
            // Already set by a prior test in this process.
            assert!(stop_requested());
        }
    }
}
