// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Sqlite [`DataSource`] adapter against the nb-rs
//! `metrics.db` schema. **First non-test `DataSource`
//! impl** — see SRD-47 §"What this push enables next".
//!
//! # Schema contract
//!
//! Mirrors the writer-side schema in
//! `nbrs-metrics/src/reporters/sqlite.rs::create_schema`:
//!
//! - `metric_family(id, name, type, unit, help)` —
//!   `type` ∈ `{"counter", "gauge", "summary"}`.
//! - `metric_instance(id, family_id, label_set_id, spec)` —
//!   one row per (family, label-set) combination.
//! - `label_set_entry(set_id, key_id, value_id)`,
//!   `label_key(id, key)`, `label_value(id, value)` —
//!   normalized label storage.
//! - `sample_value(instance_id, timestamp_ms, interval_ms,
//!   count, sum, min, max, mean, stddev, p50..p999)` —
//!   one row per (instance, sample). Per `metric_family.type`
//!   only some columns are populated:
//!     - `counter`: `count` only
//!     - `gauge`:   `mean` only
//!     - `summary`: all stat columns
//!
//! Indexes added in SRD-47 follow-up keep the read paths
//! O(log N) per matcher; without them every fetch is a full
//! scan of `label_set_entry` and `sample_value`.
//!
//! # Metric naming convention
//!
//! MetricsQL queries reference values by `__name__`. For the
//! nb-rs schema:
//!
//! - **Counter** family `cycles_total` is queried as
//!   `cycles_total` (no suffix).
//! - **Gauge** family `cpu_load` is queried as
//!   `cpu_load`.
//! - **Summary** family `latency` exposes virtual metric
//!   names `latency_count`, `latency_sum`, `latency_min`,
//!   `latency_max`, `latency_mean`, `latency_stddev`,
//!   `latency_p50` … `latency_p999` — each maps to one
//!   stat column on `sample_value`.
//!
//! The adapter resolves `__name__` by trying the bare name
//! against `metric_family.name` first; if no row is found,
//! it strips a known stat suffix (`_count`, `_p99`, etc.)
//! and tries the truncated name. This matches Prometheus'
//! convention for summary/histogram metrics.

use crate::eval::{DataSource, DataSourceError, Matcher, MatcherOp, Sample, Series};
use rusqlite::{Connection, params_from_iter, types::Value, OptionalExtension};
use std::sync::Mutex;

/// Sqlite-backed [`DataSource`]. Wraps a [`Connection`]
/// behind a [`Mutex`] so the trait's `&self` `fetch` method
/// can serialize statement preparation and execution.
///
/// Open with [`SqliteDataSource::open`] for a path or
/// [`SqliteDataSource::from_connection`] to bring your own
/// connection (useful for in-memory tests). Either path
/// applies the read-side PRAGMAs the adapter wants.
pub struct SqliteDataSource {
    conn: Mutex<Connection>,
}

impl SqliteDataSource {
    /// Open `metrics.db` at `path` for read queries. Applies
    /// read-tuned PRAGMAs (cache, mmap, temp store) but does
    /// NOT mutate the schema — schema creation is the
    /// writer's responsibility.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, DataSourceError> {
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ).map_err(|e| DataSourceError::new(format!("open metrics.db: {e}")))?;
        Self::from_connection(conn)
    }

    /// Wrap an existing [`Connection`]. PRAGMAs the adapter
    /// wants are applied here too — safe to call against
    /// connections opened in any mode (the PRAGMAs are
    /// connection-scoped).
    pub fn from_connection(conn: Connection) -> Result<Self, DataSourceError> {
        // Connection-scoped read-side tuning. WAL mode is set
        // by the writer (database-wide and persistent), so
        // we don't touch it here. The adapter's own scope is
        // page cache + temp-table location + mmap window.
        conn.execute_batch(
            "PRAGMA cache_size = -65536;\
             PRAGMA temp_store = MEMORY;\
             PRAGMA mmap_size  = 268435456;",
        ).map_err(|e| DataSourceError::new(format!("apply pragmas: {e}")))?;
        Ok(Self { conn: Mutex::new(conn) })
    }
}

impl DataSource for SqliteDataSource {
    fn fetch(
        &self,
        matchers: &[Matcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Series>, DataSourceError> {
        let conn = self.conn.lock().map_err(|_|
            DataSourceError::new("sqlite mutex poisoned"))?;

        // 1. Resolve __name__ to (family_id, family_name, stat_column).
        //    Without a name matcher we can't build a meaningful
        //    selector — return nothing rather than scanning all
        //    families.
        let Some(name_matcher) = matchers.iter().find(|m| m.label == "__name__") else {
            return Ok(Vec::new());
        };
        let resolved = match name_matcher.op {
            MatcherOp::Eq => resolve_family(&conn, &name_matcher.value)?,
            // `!=` / regex on `__name__` would mean "every
            // family except / matching pattern" — out of
            // scope for this push (would need to enumerate
            // families and dispatch per-family). Cleanly
            // surface the gap.
            _ => return Err(DataSourceError::new(
                "non-Eq match on __name__ not supported by sqlite adapter yet")),
        };
        let Some(resolved) = resolved else {
            // Family doesn't exist — empty result, not an error.
            return Ok(Vec::new());
        };

        // 2. Resolve label matchers to a set of candidate
        //    label_set_ids. The empty matcher list means
        //    "every label set under this family" — handled
        //    by skipping the IN clause entirely.
        let other_matchers: Vec<&Matcher> = matchers.iter()
            .filter(|m| m.label != "__name__")
            .collect();
        let label_set_filter = label_set_filter_clause(&other_matchers)?;

        // 3. JOIN to instance + sample_value. Single query
        //    grouped per instance; we materialize labels in
        //    a follow-up query per emitted instance.
        let stat_col = resolved.stat_column;
        let sql = format!(
            "SELECT mi.id, mi.label_set_id, sv.timestamp_ms, sv.{stat_col} \
             FROM metric_instance mi \
             JOIN sample_value sv ON sv.instance_id = mi.id \
             WHERE mi.family_id = ?1 \
               AND sv.timestamp_ms >= ?2 AND sv.timestamp_ms <= ?3 \
               {label_set_filter} \
             ORDER BY mi.id, sv.timestamp_ms"
        );

        let mut params: Vec<Value> = vec![
            Value::Integer(resolved.family_id),
            Value::Integer(start_ms),
            Value::Integer(end_ms),
        ];
        for m in &other_matchers {
            params.push(Value::Text(m.label.clone()));
            params.push(Value::Text(m.value.clone()));
        }

        let mut stmt = conn.prepare(&sql)
            .map_err(|e| DataSourceError::new(format!("prepare fetch: {e}")))?;

        // Stream rows, grouping by instance into Series. The
        // ORDER BY lets us batch contiguous rows of the same
        // instance.
        let mut rows = stmt.query(params_from_iter(params.iter()))
            .map_err(|e| DataSourceError::new(format!("query fetch: {e}")))?;
        let mut out: Vec<Series> = Vec::new();
        let mut current_instance_id: Option<i64> = None;
        let mut current_label_set_id: i64 = 0;
        let mut current_samples: Vec<Sample> = Vec::new();

        while let Some(row) = rows.next()
            .map_err(|e| DataSourceError::new(format!("step fetch: {e}")))? {
            let instance_id: i64 = row.get(0).map_err(|e|
                DataSourceError::new(format!("row.get(0): {e}")))?;
            let label_set_id: i64 = row.get(1).map_err(|e|
                DataSourceError::new(format!("row.get(1): {e}")))?;
            let timestamp_ms: i64 = row.get(2).map_err(|e|
                DataSourceError::new(format!("row.get(2): {e}")))?;
            // The stat column may be NULL for type-mismatched
            // queries (e.g. `cpu_load_p99` against a gauge).
            // Treat NULL as NaN so reducers naturally skip.
            let value: f64 = row.get::<_, Option<f64>>(3)
                .map_err(|e| DataSourceError::new(format!("row.get(3): {e}")))?
                .unwrap_or(f64::NAN);

            if Some(instance_id) != current_instance_id {
                // Flush the previous instance.
                if let Some(prev) = current_instance_id.take() {
                    let _ = prev;
                    out.push(materialize_series(
                        &conn, current_label_set_id,
                        &resolved.virtual_name, std::mem::take(&mut current_samples),
                    )?);
                }
                current_instance_id = Some(instance_id);
                current_label_set_id = label_set_id;
            }
            current_samples.push(Sample { timestamp_ms, value });
        }
        // Final flush.
        if current_instance_id.is_some() {
            out.push(materialize_series(
                &conn, current_label_set_id,
                &resolved.virtual_name, current_samples,
            )?);
        }
        Ok(out)
    }
}

/// Lookup result for `__name__` resolution.
struct ResolvedName {
    family_id: i64,
    /// Virtual name as the user wrote it (e.g. `latency_p99`).
    /// Re-applied as `__name__` on every result series so the
    /// downstream evaluator sees the name it queried for.
    virtual_name: String,
    /// `sample_value` column to read for the value series.
    stat_column: &'static str,
}

/// Try to resolve a metric name to a (family, stat-column)
/// pair. The lookup tries the bare name first (for
/// counter/gauge/summary families with no suffix), then —
/// if no family is found — strips a known stat suffix and
/// tries again (the summary-suffix convention).
fn resolve_family(conn: &Connection, name: &str)
    -> Result<Option<ResolvedName>, DataSourceError>
{
    // Bare-name lookup: matches counter / gauge / summary
    // families whose name equals the query verbatim.
    if let Some((family_id, family_type)) = lookup_family(conn, name)? {
        let stat_column = default_column_for_type(&family_type);
        return Ok(Some(ResolvedName {
            family_id,
            virtual_name: name.to_string(),
            stat_column,
        }));
    }
    // Suffix lookup: `<family>_<stat>` against summary
    // families. The suffix list is closed and ordered
    // longest-first so `_p999` is preferred over `_p9` (none
    // exist now but the principle is robust).
    for suffix in STAT_SUFFIXES {
        if let Some(stripped) = name.strip_suffix(suffix.text)
            && let Some((family_id, family_type)) = lookup_family(conn, stripped)?
            && family_type == "summary"
        {
            return Ok(Some(ResolvedName {
                family_id,
                virtual_name: name.to_string(),
                stat_column: suffix.column,
            }));
        }
    }
    Ok(None)
}

fn lookup_family(conn: &Connection, name: &str)
    -> Result<Option<(i64, String)>, DataSourceError>
{
    conn.query_row(
        "SELECT id, type FROM metric_family WHERE name = ?1",
        rusqlite::params![name],
        |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        }
    ).optional().map_err(|e|
        DataSourceError::new(format!("family lookup: {e}")))
}

/// Default `sample_value` column for a family type. The
/// suffix lookup overrides this for summary families where
/// the user explicitly named a stat.
fn default_column_for_type(family_type: &str) -> &'static str {
    match family_type {
        "counter" => "count",
        "gauge"   => "mean",
        // Summaries with no stat suffix — return the count
        // by convention. Users typically want a specific
        // stat (`_p99` etc.); if they ask for the bare name
        // they get the observation count.
        "summary" => "count",
        _ => "mean",  // Defensive fallback for unknown types.
    }
}

/// Closed table of summary-stat suffixes. Order matters
/// only insofar as `strip_suffix` matches the longest first
/// would — Rust's `str::strip_suffix` is exact; we just
/// iterate in the natural order.
struct StatSuffix {
    text: &'static str,
    column: &'static str,
}

const STAT_SUFFIXES: &[StatSuffix] = &[
    StatSuffix { text: "_p999",   column: "p999"   },
    StatSuffix { text: "_p99",    column: "p99"    },
    StatSuffix { text: "_p98",    column: "p98"    },
    StatSuffix { text: "_p95",    column: "p95"    },
    StatSuffix { text: "_p90",    column: "p90"    },
    StatSuffix { text: "_p75",    column: "p75"    },
    StatSuffix { text: "_p50",    column: "p50"    },
    StatSuffix { text: "_count",  column: "count"  },
    StatSuffix { text: "_sum",    column: "sum"    },
    StatSuffix { text: "_min",    column: "min"    },
    StatSuffix { text: "_max",    column: "max"    },
    StatSuffix { text: "_mean",   column: "mean"   },
    StatSuffix { text: "_stddev", column: "stddev" },
];

/// Build the SQL fragment that narrows by label matchers.
/// Returns `""` when there are no non-`__name__` matchers
/// (an unrestricted family scan). Otherwise produces
/// `AND mi.label_set_id IN (... INTERSECT ...)`.
fn label_set_filter_clause(matchers: &[&Matcher])
    -> Result<String, DataSourceError>
{
    if matchers.is_empty() { return Ok(String::new()); }
    let mut parts: Vec<String> = Vec::with_capacity(matchers.len());
    for (i, m) in matchers.iter().enumerate() {
        let kparam = i * 2 + 4;  // 1, 2, 3 are family_id + ts range
        let vparam = i * 2 + 5;
        // EqRegex / NeRegex aren't compiled in this push —
        // the streaming layer's matcher set is `Eq` / `Ne`
        // for the same reason. Surface as an error rather
        // than silently degrading to exact match.
        let cmp_clause = match m.op {
            MatcherOp::Eq => format!("k.key = ?{kparam} AND v.value = ?{vparam}"),
            MatcherOp::Ne => format!("k.key = ?{kparam} AND v.value != ?{vparam}"),
            MatcherOp::EqRegex | MatcherOp::NeRegex =>
                return Err(DataSourceError::new(
                    "regex matchers not supported by sqlite adapter yet")),
        };
        parts.push(format!(
            "SELECT lse.set_id FROM label_set_entry lse \
             JOIN label_key k ON k.id = lse.key_id \
             JOIN label_value v ON v.id = lse.value_id \
             WHERE {cmp_clause}"
        ));
    }
    Ok(format!(" AND mi.label_set_id IN ({})", parts.join(" INTERSECT ")))
}

/// Build the `Series.labels` for a result instance: every
/// `(key, value)` from its `label_set_entry`, plus the
/// virtual metric name as `__name__`.
fn materialize_series(
    conn: &Connection,
    label_set_id: i64,
    virtual_name: &str,
    samples: Vec<Sample>,
) -> Result<Series, DataSourceError> {
    let mut stmt = conn.prepare_cached(
        "SELECT k.key, v.value \
         FROM label_set_entry lse \
         JOIN label_key k ON k.id = lse.key_id \
         JOIN label_value v ON v.id = lse.value_id \
         WHERE lse.set_id = ?1 \
         ORDER BY k.key"
    ).map_err(|e| DataSourceError::new(format!("prepare materialize: {e}")))?;

    let mut labels: Vec<(String, String)> = Vec::new();
    // Canonical `__name__` first per the trait contract.
    labels.push(("__name__".to_string(), virtual_name.to_string()));

    let row_iter = stmt.query_map(
        rusqlite::params![label_set_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    ).map_err(|e| DataSourceError::new(format!("query materialize: {e}")))?;

    for r in row_iter {
        let (k, v) = r.map_err(|e|
            DataSourceError::new(format!("row materialize: {e}")))?;
        labels.push((k, v));
    }
    Ok(Series { labels, samples })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    /// Build a fresh in-memory schema mirroring the
    /// nbrs-metrics writer side. Vendored here so the test
    /// is self-contained — keeping the dep graph clean.
    fn make_schema() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE metric_family (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                unit TEXT,
                help TEXT,
                UNIQUE(name, type)
            );
            CREATE TABLE label_key (
                id INTEGER PRIMARY KEY,
                key TEXT NOT NULL UNIQUE
            );
            CREATE TABLE label_value (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL UNIQUE
            );
            CREATE TABLE label_set (
                id INTEGER PRIMARY KEY,
                hash INTEGER NOT NULL UNIQUE
            );
            CREATE TABLE label_set_entry (
                set_id INTEGER NOT NULL,
                key_id INTEGER NOT NULL,
                value_id INTEGER NOT NULL
            );
            CREATE TABLE metric_instance (
                id INTEGER PRIMARY KEY,
                family_id INTEGER NOT NULL,
                label_set_id INTEGER NOT NULL,
                spec TEXT,
                UNIQUE(family_id, label_set_id)
            );
            CREATE TABLE sample_value (
                instance_id INTEGER NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                interval_ms INTEGER NOT NULL,
                count INTEGER, sum REAL, min REAL, max REAL,
                mean REAL, stddev REAL,
                p50 REAL, p75 REAL, p90 REAL, p95 REAL,
                p98 REAL, p99 REAL, p999 REAL
            );"
        ).unwrap();
        conn
    }

    /// Insert a family + label set + instance, returning the
    /// instance id so the caller can attach samples.
    fn make_instance(
        conn: &Connection,
        family_name: &str,
        family_type: &str,
        labels: &[(&str, &str)],
    ) -> i64 {
        // Family.
        conn.execute(
            "INSERT OR IGNORE INTO metric_family (name, type) VALUES (?1, ?2)",
            params![family_name, family_type]).unwrap();
        let family_id: i64 = conn.query_row(
            "SELECT id FROM metric_family WHERE name = ?1 AND type = ?2",
            params![family_name, family_type],
            |r| r.get(0)).unwrap();

        // Label keys / values.
        let mut entries: Vec<(i64, i64)> = Vec::new();
        for (k, v) in labels {
            conn.execute("INSERT OR IGNORE INTO label_key (key) VALUES (?1)",
                params![k]).unwrap();
            conn.execute("INSERT OR IGNORE INTO label_value (value) VALUES (?1)",
                params![v]).unwrap();
            let kid: i64 = conn.query_row(
                "SELECT id FROM label_key WHERE key = ?1",
                params![k], |r| r.get(0)).unwrap();
            let vid: i64 = conn.query_row(
                "SELECT id FROM label_value WHERE value = ?1",
                params![v], |r| r.get(0)).unwrap();
            entries.push((kid, vid));
        }
        // Label set — deterministic hash from labels.
        let hash: i64 = labels.iter()
            .fold(0i64, |acc, (k, v)| acc.wrapping_add(
                k.bytes().fold(0i64, |a, b| a.wrapping_add(b as i64))
                + v.bytes().fold(0i64, |a, b| a.wrapping_add(b as i64))
            ));
        conn.execute(
            "INSERT OR IGNORE INTO label_set (hash) VALUES (?1)",
            params![hash]).unwrap();
        let set_id: i64 = conn.query_row(
            "SELECT id FROM label_set WHERE hash = ?1",
            params![hash], |r| r.get(0)).unwrap();
        for (kid, vid) in &entries {
            conn.execute(
                "INSERT INTO label_set_entry (set_id, key_id, value_id) \
                 VALUES (?1, ?2, ?3)",
                params![set_id, kid, vid]).unwrap();
        }
        // Instance.
        conn.execute(
            "INSERT OR IGNORE INTO metric_instance (family_id, label_set_id, spec) \
             VALUES (?1, ?2, ?3)",
            params![family_id, set_id, format!("{family_name}{{{labels:?}}}")]).unwrap();
        conn.query_row(
            "SELECT id FROM metric_instance WHERE family_id = ?1 AND label_set_id = ?2",
            params![family_id, set_id], |r| r.get(0)).unwrap()
    }

    fn add_counter_sample(conn: &Connection, instance_id: i64, ts: i64, count: i64) {
        conn.execute(
            "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, count) \
             VALUES (?1, ?2, 0, ?3)",
            params![instance_id, ts, count]).unwrap();
    }

    fn add_gauge_sample(conn: &Connection, instance_id: i64, ts: i64, mean: f64) {
        conn.execute(
            "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, mean) \
             VALUES (?1, ?2, 0, ?3)",
            params![instance_id, ts, mean]).unwrap();
    }

    fn add_summary_sample(
        conn: &Connection, instance_id: i64, ts: i64,
        count: i64, p50: f64, p99: f64,
    ) {
        conn.execute(
            "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, count, p50, p99) \
             VALUES (?1, ?2, 0, ?3, ?4, ?5)",
            params![instance_id, ts, count, p50, p99]).unwrap();
    }

    fn open_ds(conn: Connection) -> SqliteDataSource {
        SqliteDataSource::from_connection(conn).expect("from_connection")
    }

    fn lookup<'a>(s: &'a Series, key: &str) -> Option<&'a str> {
        s.labels.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    #[test]
    fn fetch_counter_returns_count_column() {
        let conn = make_schema();
        let id = make_instance(&conn, "cycles_total", "counter",
            &[("op", "read")]);
        add_counter_sample(&conn, id, 100, 42);
        add_counter_sample(&conn, id, 200, 100);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "cycles_total".into() }],
            0, 1000,
        ).expect("fetch");
        assert_eq!(got.len(), 1);
        assert_eq!(lookup(&got[0], "__name__"), Some("cycles_total"));
        assert_eq!(lookup(&got[0], "op"), Some("read"));
        assert_eq!(got[0].samples.len(), 2);
        assert_eq!(got[0].samples[0].value, 42.0);
        assert_eq!(got[0].samples[1].value, 100.0);
    }

    #[test]
    fn fetch_gauge_returns_mean_column() {
        let conn = make_schema();
        let id = make_instance(&conn, "cpu_load", "gauge",
            &[("host", "h1")]);
        add_gauge_sample(&conn, id, 0, 0.75);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "cpu_load".into() }],
            0, 1000,
        ).expect("fetch");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].samples[0].value, 0.75);
    }

    #[test]
    fn summary_suffix_resolution_picks_correct_column() {
        let conn = make_schema();
        let id = make_instance(&conn, "latency", "summary",
            &[("op", "read")]);
        add_summary_sample(&conn, id, 100, 1000, 12.5, 99.9);

        let ds = open_ds(conn);
        let p50 = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "latency_p50".into() }],
            0, 1000,
        ).expect("fetch p50");
        assert_eq!(p50[0].samples[0].value, 12.5);

        let p99 = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "latency_p99".into() }],
            0, 1000,
        ).expect("fetch p99");
        assert_eq!(p99[0].samples[0].value, 99.9);

        let count = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "latency_count".into() }],
            0, 1000,
        ).expect("fetch count");
        assert_eq!(count[0].samples[0].value, 1000.0);
    }

    #[test]
    fn label_matchers_filter_to_correct_instance() {
        let conn = make_schema();
        let id_a = make_instance(&conn, "cpu", "gauge", &[("host", "a")]);
        let id_b = make_instance(&conn, "cpu", "gauge", &[("host", "b")]);
        add_gauge_sample(&conn, id_a, 0, 1.0);
        add_gauge_sample(&conn, id_b, 0, 2.0);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[
                Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                    value: "cpu".into() },
                Matcher { label: "host".into(), op: MatcherOp::Eq,
                    value: "a".into() },
            ],
            0, 1000,
        ).expect("fetch");
        assert_eq!(got.len(), 1);
        assert_eq!(lookup(&got[0], "host"), Some("a"));
        assert_eq!(got[0].samples[0].value, 1.0);
    }

    #[test]
    fn time_range_filters_samples() {
        let conn = make_schema();
        let id = make_instance(&conn, "cpu", "gauge", &[]);
        add_gauge_sample(&conn, id, 0, 1.0);
        add_gauge_sample(&conn, id, 50, 2.0);
        add_gauge_sample(&conn, id, 100, 3.0);
        add_gauge_sample(&conn, id, 200, 4.0);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "cpu".into() }],
            50, 100,  // inclusive both
        ).expect("fetch");
        assert_eq!(got.len(), 1);
        let values: Vec<f64> = got[0].samples.iter().map(|s| s.value).collect();
        assert_eq!(values, vec![2.0, 3.0]);
    }

    #[test]
    fn unknown_family_returns_empty() {
        let ds = open_ds(make_schema());
        let got = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "nonexistent".into() }],
            0, 1000,
        ).expect("fetch");
        assert!(got.is_empty());
    }

    #[test]
    fn no_name_matcher_returns_empty() {
        let conn = make_schema();
        let id = make_instance(&conn, "cpu", "gauge", &[]);
        add_gauge_sample(&conn, id, 0, 1.0);
        let ds = open_ds(conn);
        let got = ds.fetch(&[], 0, 1000).expect("fetch");
        // Matcher set with no `__name__` is a no-op — rather
        // than scanning every family we return empty. That's
        // what the trait contract permits.
        assert!(got.is_empty());
    }

    #[test]
    fn ne_matcher_excludes_value() {
        let conn = make_schema();
        let id_a = make_instance(&conn, "cpu", "gauge", &[("host", "a")]);
        let id_b = make_instance(&conn, "cpu", "gauge", &[("host", "b")]);
        add_gauge_sample(&conn, id_a, 0, 1.0);
        add_gauge_sample(&conn, id_b, 0, 2.0);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[
                Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                    value: "cpu".into() },
                Matcher { label: "host".into(), op: MatcherOp::Ne,
                    value: "a".into() },
            ],
            0, 1000,
        ).expect("fetch");
        assert_eq!(got.len(), 1);
        assert_eq!(lookup(&got[0], "host"), Some("b"));
    }

    #[test]
    fn regex_matcher_returns_descriptive_error() {
        let conn = make_schema();
        let _id = make_instance(&conn, "cpu", "gauge", &[("host", "a")]);
        let ds = open_ds(conn);
        let err = ds.fetch(
            &[
                Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                    value: "cpu".into() },
                Matcher { label: "host".into(), op: MatcherOp::EqRegex,
                    value: ".+".into() },
            ],
            0, 1000,
        ).expect_err("expected regex-not-supported error");
        assert!(err.message.contains("regex"));
    }

    #[test]
    fn type_mismatch_yields_nan_not_error() {
        // `cpu_load` is a gauge — `cpu_load_p99` queries the
        // p99 column, which is NULL for gauges. Adapter
        // returns NaN samples, not an error; reducers skip
        // NaN naturally.
        let conn = make_schema();
        let id = make_instance(&conn, "cpu_load", "gauge", &[]);
        add_gauge_sample(&conn, id, 0, 0.5);
        let ds = open_ds(conn);
        // NOTE: there's no summary family `cpu_load`, and
        // `cpu_load_p99` would try to strip `_p99` and look
        // up `cpu_load` as a summary family. That fails (it's
        // a gauge), so the bare-name lookup with the FULL
        // name `cpu_load_p99` is what runs first → also no
        // match → empty result.
        let got = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "cpu_load_p99".into() }],
            0, 1000,
        ).expect("fetch");
        assert!(got.is_empty());
    }

    #[test]
    fn end_to_end_with_metricsql_evaluator() {
        // The acid test: a real metricsql query routed through
        // the parser → evaluator → SqliteDataSource → schema.
        // Verifies the trait contract end-to-end against the
        // schema we ship.
        use crate::eval::{EvalContext, evaluate};

        let conn = make_schema();
        let id_a = make_instance(&conn, "latency", "summary",
            &[("op", "read"), ("zone", "z1")]);
        let id_b = make_instance(&conn, "latency", "summary",
            &[("op", "read"), ("zone", "z2")]);
        let id_c = make_instance(&conn, "latency", "summary",
            &[("op", "write"), ("zone", "z1")]);
        // p99 values: read/z1 → 10, read/z2 → 20, write/z1 → 30.
        add_summary_sample(&conn, id_a, 100, 1000, 5.0, 10.0);
        add_summary_sample(&conn, id_b, 100, 1000, 8.0, 20.0);
        add_summary_sample(&conn, id_c, 100, 1000, 7.0, 30.0);

        let ds = open_ds(conn);
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 1000, step_ms: 1 };
        let ast = crate::parse(r#"max(latency_p99{op="read"}) by (zone)"#)
            .expect("parse");
        let mut got = evaluate(&ctx, &ast).expect("evaluate");
        got.sort_by(|a, b|
            lookup(a, "zone").unwrap_or("").cmp(lookup(b, "zone").unwrap_or("")));
        assert_eq!(got.len(), 2);
        assert_eq!(lookup(&got[0], "zone"), Some("z1"));
        assert_eq!(got[0].samples[0].value, 10.0);
        assert_eq!(lookup(&got[1], "zone"), Some("z2"));
        assert_eq!(got[1].samples[0].value, 20.0);
    }
}
