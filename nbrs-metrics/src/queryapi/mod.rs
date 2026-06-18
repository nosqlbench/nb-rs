// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The metrics **query API** — the data-access *service* boundary
//! (SRD-86 §"The metric-reader surface").
//!
//! `nbrs-metrics` is the foundational data-access library; this module
//! exposes its query surface as a service. It provides the native
//! result shape ([`Vector`] — multiple [`Series`], each with sample
//! points), the selector semantics ([`Matcher`]), and a small access
//! contract ([`MetricAccess`]) whose signatures map **1:1 onto a
//! MetricsQL parsed selector**:
//!
//! - a bare / instant selector → [`MetricAccess::select_instant`];
//! - a range selector (`m[w]`) → [`MetricAccess::select_range`].
//!
//! Deliberately **not** here: aggregation, rollups, arithmetic — the
//! "bells and whistles" of the query language. Those stay in the
//! MetricsQL engine, layered *over* this access surface. Keeping the
//! contract this thin is what lets the engine sit on any data service.
//!
//! ## Service location (a "data service object")
//!
//! Consumers (the MetricsQL engine) **locate** a service at runtime
//! rather than binding a concrete impl:
//!
//! - the **live in-process** service wraps the session's `MetricsQuery`
//!   (which is not static), so the runner [`install_live_access`]es it
//!   per session and consumers read it via [`live_access`];
//! - **file / external** backends (e.g. the sqlite reader) register an
//!   [`AccessProvider`] via `inventory`, so a consumer can [`provider`]
//!   one by scheme and open it — without the engine depending on the
//!   backend's crate or features.

pub mod catalog;
mod live;
mod shapes;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub use catalog::{
    CachedCatalog, ExemplarPoint, LabelSet, MetricCatalog, MetricFamilyMeta, MetricType,
};
pub use live::MetricsQueryAccess;
pub use shapes::{MatchOp, Matcher, Sample, Series, Vector};

use std::sync::{Arc, LazyLock, Mutex};

/// Error from a metrics access backend. Backends own their taxonomy;
/// the engine treats these as opaque from a flow-control standpoint.
#[derive(Debug, Clone)]
pub struct QueryError {
    pub message: String,
}

impl QueryError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { message: msg.into() }
    }
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics query: {}", self.message)
    }
}

impl std::error::Error for QueryError {}

/// The metrics data-access **service**. Backends implement it; a query
/// layer locates one at runtime and reads `Vector`s through it. See the
/// module docs for the access/aggregation cut line.
pub trait MetricAccess: Send + Sync {
    /// Range selection: series matching every `matcher`, with every
    /// sample in `[start_ms, end_ms]` (ascending). Yields a range
    /// vector — the input a rollup (`rate(m[w])`, `*_over_time`)
    /// consumes. This is the one required method; [`select_instant`]
    /// derives from it.
    ///
    /// [`select_instant`]: MetricAccess::select_instant
    fn select_range(
        &self,
        matchers: &[Matcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vector, QueryError>;

    /// Instant selection: series matching every `matcher`, each reduced
    /// to its latest sample within `[at_ms - lookback, at_ms]` (the
    /// PromQL stale-tolerance window; `lookback_ms = None` means a
    /// strict `[at_ms, at_ms]`). Yields an instant vector — one sample
    /// per series.
    ///
    /// Default: `select_range` over the lookback window, then the
    /// latest sample per series. A backend can override if it can do
    /// the reduction more cheaply (e.g. push it into SQL).
    fn select_instant(
        &self,
        matchers: &[Matcher],
        at_ms: i64,
        lookback_ms: Option<i64>,
    ) -> Result<Vector, QueryError> {
        let start_ms = at_ms - lookback_ms.unwrap_or(0);
        let range = self.select_range(matchers, start_ms, at_ms)?;
        let reduced = range
            .into_series()
            .into_iter()
            .filter_map(|s| {
                s.samples
                    .last()
                    .copied()
                    .map(|last| Series { labels: s.labels, samples: vec![last] })
            })
            .collect();
        Ok(Vector::new(reduced))
    }
}

// ---------------------------------------------------------------------
// Runtime service location
// ---------------------------------------------------------------------

/// The live in-process access service for the current session. Wraps a
/// per-session `MetricsQuery`, so it's *installed* (not static).
static LIVE: LazyLock<Mutex<Option<Arc<dyn MetricAccess>>>> =
    LazyLock::new(|| Mutex::new(None));

/// Install the live in-process access service. Called once by the
/// runner when the session's `MetricsQuery` is built.
pub fn install_live_access(service: Arc<dyn MetricAccess>) {
    *LIVE.lock().unwrap_or_else(|e| e.into_inner()) = Some(service);
}

/// The live in-process access service, if a session has installed one.
pub fn live_access() -> Option<Arc<dyn MetricAccess>> {
    LIVE.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// A pluggable access backend, discovered at runtime via `inventory`.
/// A consumer opens a service for a scheme-specific `target` (e.g. a
/// db path). The sqlite reader registers one; future backends can too,
/// without the query engine depending on them.
pub struct AccessProvider {
    /// The scheme this provider answers for (e.g. `"sqlite"`).
    pub scheme: &'static str,
    /// Open an access service for `target` (scheme-specific).
    pub open: fn(target: &str) -> Result<Box<dyn MetricAccess>, QueryError>,
}

inventory::collect!(AccessProvider);

/// Locate a registered [`AccessProvider`] by scheme.
pub fn provider(scheme: &str) -> Option<&'static AccessProvider> {
    inventory::iter::<AccessProvider>
        .into_iter()
        .find(|p| p.scheme == scheme)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_access_installs_and_reads_back() {
        struct Stub;
        impl MetricAccess for Stub {
            fn select_instant(&self, _: &[Matcher], _: i64, _: Option<i64>) -> Result<Vector, QueryError> {
                Ok(Vector::default())
            }
            fn select_range(&self, _: &[Matcher], _: i64, _: i64) -> Result<Vector, QueryError> {
                Ok(Vector::default())
            }
        }
        install_live_access(Arc::new(Stub));
        assert!(live_access().is_some());
    }

    #[test]
    fn unknown_provider_scheme_is_none() {
        assert!(provider("no-such-scheme").is_none());
    }
}
