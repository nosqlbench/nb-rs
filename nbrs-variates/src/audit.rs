// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cycle-time data-source audit log.
//!
//! Emits one structured line per significant data-source event so
//! the operator can spot mismatches between what
//! [`crate::nodes::vectors::do_dataset_prebuffer`] *covered* and
//! what cycle-time accessors *opened*. The typical failure mode
//! this catches: prebuffer reports success but readers still hit
//! HTTP per cycle, because the facets the workload reads aren't
//! in the active profile's manifest.
//!
//! Output is routed through [`set_log_fn`] when the caller (the
//! activity runner, the test harness, …) installs a sink. With no
//! sink installed, lines go to stderr — preserves visibility from
//! contexts that don't carry an `observer::log` plumbing path
//! (unit tests, the `dryrun=` paths).

use std::sync::OnceLock;

/// Severity for audit-channel events. Mirrors the activity-layer
/// observer levels so the runner's installed sink can map 1:1 to
/// `nbrs_activity::observer::LogLevel` without reformatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

type LogFn = Box<dyn Fn(LogLevel, &str) + Send + Sync>;

static LOG_FN: OnceLock<LogFn> = OnceLock::new();

/// Install the audit sink. Called once by the activity runner so
/// audit lines flow through `nbrs_activity::observer::log` and
/// land in `session.log` alongside the rest of the run output.
/// Subsequent calls are no-ops.
pub fn set_log_fn<F>(f: F)
where F: Fn(LogLevel, &str) + Send + Sync + 'static
{
    let _ = LOG_FN.set(Box::new(f));
}

/// Emit a leveled message through the configured sink, falling
/// back to stderr when no sink is installed (unit tests, dryrun
/// paths, pre-init).
pub fn log(level: LogLevel, msg: &str) {
    if let Some(f) = LOG_FN.get() {
        f(level, msg);
    } else {
        let tag = match level {
            LogLevel::Trace => "TRC",
            LogLevel::Debug => "DBG",
            LogLevel::Info  => "INF",
            LogLevel::Warn  => "WRN",
            LogLevel::Error => "ERR",
        };
        eprintln!("{tag} {msg}");
    }
}

/// Convenience helpers for callsite ergonomics.
pub fn debug(msg: &str) { log(LogLevel::Debug, msg); }
pub fn info(msg: &str)  { log(LogLevel::Info,  msg); }
pub fn warn(msg: &str)  { log(LogLevel::Warn,  msg); }
pub fn error(msg: &str) { log(LogLevel::Error, msg); }

/// Record that `dataset_prebuffer(...)` was invoked. Emitted at
/// the top of `do_dataset_prebuffer` *unconditionally* — fires
/// even if the function bails on a resolve / profile-missing
/// error, so the absence of this line in `session.log` is
/// definitive evidence that `init prebuffer = ...` never
/// evaluated. Pairs with `record_prebuffered` (per-facet) and
/// `log_prebuffer_summary` (tail).
pub fn record_prebuffer_entered(source: &str) {
    debug(&format!("prebuffer: entered dataset_prebuffer({source:?})"));
}

/// Record that the prebuffer pass covered a facet. Emitted from
/// inside the `view.prebuffer_all_with_progress` callback, once
/// per facet the manifest declared.
pub fn record_prebuffered(source: &str, profile: &str, facet: &str) {
    debug(&format!("prebuffer: covered {source}:{profile}/{facet}"));
}

/// Record that a reader was opened for a facet. Emitted from
/// every `vectors::*` reader-open path *before* the actual
/// `view.<facet>()` / `open_facet_typed` call so the line lands
/// even if the open errors. `kind` distinguishes the open shape
/// (`uniform`, `ivvec32`, `generic-typed`, …) for at-a-glance
/// debugging.
pub fn record_opened(source: &str, profile: &str, facet: &str, kind: &str) {
    debug(&format!("vectordata: opened {source}:{profile}/{facet} (kind={kind})"));
}

/// One-line summary at the end of `dataset_prebuffer`. Pairs
/// with the per-facet `prebuffer: covered …` lines above and
/// the `vectordata: opened …` lines below to make the
/// covered-vs-opened delta easy to read.
pub fn log_prebuffer_summary(source: &str, profile: &str, facet_count: u64) {
    debug(&format!(
        "prebuffer: done {source}:{profile} (covered {facet_count} facet(s))"
    ));
}
