// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Session: the root context for a workload run.
//!
//! A session has a human-readable ID, a directory for all diagnostic
//! artifacts (metrics, logs, flamegraphs), and is the root of the
//! component tree for metrics labeling.
//!
//! Session ID format: `{scenario}_{YYYYMMDD_HHmmss}`
//! Session directory: `logs/{session_id}/`
//!
//! All files from a run live under the session directory:
//! - `metrics.db` — SQLite metrics
//! - `flamegraph.svg` — profiler output
//! - `session.log` — diagnostic log (future)

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use nb_metrics::component::Component;
use nb_metrics::labels::Labels;
use nb_metrics::metrics_query::MetricsQuery;

/// A workload run session.
///
/// The session is the root of the component tree and — once the
/// runner has installed one — holds the shared [`MetricsQuery`] that
/// every in-process reader (TUI, summary, GK metric nodes) reads
/// through. See SRD-42 §"MetricsQuery — the unified read interface".
pub struct Session {
    /// Human-readable session identifier.
    pub id: String,
    /// Output directory for diagnostic artifacts (metrics, logs, flamegraphs).
    /// Located at `logs/{session_id}/`. Not the working directory.
    pub output_dir: PathBuf,
    /// Workload file path (for metadata).
    pub workload: String,
    /// Scenario name.
    pub scenario: String,
    /// Session root component (owns the component tree for metrics labeling).
    pub component: Arc<RwLock<Component>>,
    /// Shared `MetricsQuery` handle — installed by the runner once the
    /// cadence reporter is built. `None` before the runner wires it.
    pub metrics_query: Mutex<Option<Arc<MetricsQuery>>>,
}

impl Session {
    /// Create a new session. Creates the session directory under `logs/`.
    pub fn new(workload: &str, scenario: &str) -> Self {
        let timestamp = format_timestamp();
        let workload_stem = Path::new(workload)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("workload");
        let id = format!("{scenario}_{timestamp}");
        let output_dir = PathBuf::from("logs").join(&id);

        // Create the output directory (and logs/ parent if needed)
        if let Err(e) = std::fs::create_dir_all(&output_dir) {
            crate::observer::log(
                crate::observer::LogLevel::Warn,
                &format!("warning: failed to create session output directory {}: {e}", output_dir.display()),
            );
        }

        // Create/update symlink `logs/latest` → this session,
        // then well-known per-artifact symlinks pointing through
        // `latest/` so `logs/{name}` always resolves to the most
        // recent session's copy. Symlinks are created eagerly —
        // even if the target file doesn't exist yet, the link is
        // in place so the artifact becomes accessible the moment
        // its writer creates the underlying file.
        let logs = PathBuf::from("logs");
        let latest = logs.join("latest");
        let _ = std::fs::remove_file(&latest);
        let _ = std::os::unix::fs::symlink(&id, &latest);
        for artifact in [
            "metrics.db",
            "summary.md",
            "session.log",
            "flamegraph.svg",
            "flamegraph-perf.svg",
            "tui.dump",
        ] {
            let link = logs.join(artifact);
            let _ = std::fs::remove_file(&link);
            // Relative target routes through the `latest` symlink
            // so swapping sessions updates every artifact link
            // in a single `latest` update.
            let target = PathBuf::from("latest").join(artifact);
            let _ = std::os::unix::fs::symlink(&target, &link);
        }

        let component = Component::root(
            Labels::of("session", &id),
            std::collections::HashMap::new(),
        );

        Self {
            id,
            output_dir,
            workload: workload_stem.to_string(),
            scenario: scenario.to_string(),
            component,
            metrics_query: Mutex::new(None),
        }
    }

    /// Install the shared [`MetricsQuery`] handle. Called once by the
    /// runner after it has planned the cadence tree and built the
    /// cadence reporter. Panics if called twice.
    pub fn set_metrics_query(&self, query: Arc<MetricsQuery>) {
        let mut slot = self.metrics_query.lock().unwrap_or_else(|e| e.into_inner());
        assert!(slot.is_none(), "session metrics_query already installed");
        *slot = Some(query);
    }

    /// Borrow the installed [`MetricsQuery`]. Returns `None` before
    /// the runner wires it.
    pub fn metrics_query(&self) -> Option<Arc<MetricsQuery>> {
        self.metrics_query.lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Path to the SQLite metrics file for this session.
    pub fn metrics_path(&self) -> PathBuf {
        self.output_dir.join("metrics.db")
    }

    /// Path for a profiler output file.
    pub fn profiler_path(&self, suffix: &str) -> PathBuf {
        self.output_dir.join(format!("flamegraph{suffix}.svg"))
    }

    /// Path for an arbitrary session artifact.
    pub fn artifact_path(&self, filename: &str) -> PathBuf {
        self.output_dir.join(filename)
    }
}

/// Format the current time as `YYYYMMDD_HHmmss`.
fn format_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Convert epoch seconds to date/time components.
    // Simple implementation without chrono dependency.
    let days = secs / 86400;
    let time = secs % 86400;
    let hours = time / 3600;
    let minutes = (time % 3600) / 60;
    let seconds = time % 60;

    // Days since epoch to Y/M/D (simplified Gregorian)
    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}{month:02}{day:02}_{hours:02}{minutes:02}{seconds:02}")
}

/// Current wall-clock time as `YYYY-MM-DD HH:MM:SS.mmm` (UTC).
/// Used by the session log writer for human-readable line timestamps.
pub fn now_log_timestamp() -> String {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();
    let days = secs / 86400;
    let time = secs % 86400;
    let hours = time / 3600;
    let minutes = (time % 3600) / 60;
    let seconds = time % 60;
    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02} {hours:02}:{minutes:02}:{seconds:02}.{millis:03}")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from Howard Hinnant's date library
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_format() {
        let ts = format_timestamp();
        // Should be 15 chars: YYYYMMDD_HHmmss
        assert_eq!(ts.len(), 15, "timestamp: {ts}");
        assert!(ts.contains('_'), "timestamp should contain underscore: {ts}");
    }

    #[test]
    fn session_id_format() {
        let session = Session::new("full_cql_vector.yaml", "fknn_rampup");
        assert!(session.id.starts_with("fknn_rampup_"), "id: {}", session.id);
        assert!(session.output_dir.starts_with("logs/"), "output_dir: {}", session.output_dir.display());
    }

    #[test]
    fn session_paths() {
        let session = Session::new("test.yaml", "smoke");
        assert!(session.metrics_path().ends_with("metrics.db"));
        assert!(session.profiler_path("").ends_with("flamegraph.svg"));
        assert!(session.profiler_path("-perf").ends_with("flamegraph-perf.svg"));
    }
}
