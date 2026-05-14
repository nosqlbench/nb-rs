// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Sqlite [`DataSource`] adapter against the nb-rs
//! `metrics.db` schema. **First non-test `DataSource`
//! impl** — see SRD-47 §"What this push enables next".
//!
//! # Schema contract
//!
//! Mirrors the writer-side schema in
//! `nbrs-metrics/src/reporters/sqlite.rs::create_schema`
//! (post-cutover, fully denormalised — no `label_set`):
//!
//! - `metric_family(id, name, type, unit, help)` —
//!   `type` ∈ `{"counter", "gauge", "summary"}`.
//! - `metric_instance(id, family_id, spec UNIQUE)` —
//!   `spec` is the OpenMetrics-canonical sample identifier
//!   `name{k="v",…}` (sorted by key). Two logical label sets
//!   that are equal as a mapping produce equal spec text and
//!   resolve to the same instance row.
//! - `instance_label(instance_id, key, value)` —
//!   one row per label pair, including `__name__` (so
//!   queries filter on metric family the same way they
//!   filter on any other dimension).
//! - `sample_value(instance_id, timestamp_ms, interval_ms,
//!   count, sum, min, max, mean, stddev, p50..p999)` —
//!   one row per (instance, sample). Per `metric_family.type`
//!   only some columns are populated:
//!     - `counter`: `count` only
//!     - `gauge`:   `mean` only
//!     - `summary`: all stat columns
//!
//! Indexes: `instance_label(key, value, instance_id)` covers
//! matcher resolution; `instance_label(instance_id)` covers
//! per-instance label materialisation.
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

use crate::catalog::{ExemplarPoint, LabelSet, MetricCatalog, MetricFamilyMeta, MetricType};
use crate::eval::{DataSource, DataSourceError, Matcher, MatcherOp, Sample, Series};
use rusqlite::{Connection, params_from_iter, types::Value, OptionalExtension};
use std::path::PathBuf;
use std::sync::Mutex;

/// Sqlite-backed [`DataSource`] (and [`MetricCatalog`]).
/// Wraps a [`Connection`] behind a [`Mutex`] so the trait's
/// `&self` methods can serialize statement preparation and
/// execution.
///
/// Open with [`SqliteDataSource::open`] for a path or
/// [`SqliteDataSource::from_connection`] to bring your own
/// connection (useful for in-memory tests). Either path
/// applies the read-side PRAGMAs the adapter wants.
///
/// `db_path` is captured when [`Self::open`] is used so the
/// catalog cache layer can mtime-invalidate against the
/// on-disk file. Sources opened via
/// [`Self::from_connection`] don't have a path and thus
/// can't drive mtime-based invalidation; their cache layer
/// has to fall back to TTL + manual `invalidate()`.
pub struct SqliteDataSource {
    conn: Mutex<Connection>,
    db_path: Option<PathBuf>,
}

impl SqliteDataSource {
    /// Open `metrics.db` at `path` for read queries. Applies
    /// read-tuned PRAGMAs (cache, mmap, temp store) but does
    /// NOT mutate the schema — schema creation is the
    /// writer's responsibility.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, DataSourceError> {
        let path_ref = path.as_ref();
        let conn = Connection::open_with_flags(
            path_ref,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ).map_err(|e| DataSourceError::new(format!("open metrics.db: {e}")))?;
        let mut src = Self::from_connection(conn)?;
        src.db_path = Some(path_ref.to_path_buf());
        Ok(src)
    }

    /// Path the source was opened against, if any.
    /// `None` when constructed via [`Self::from_connection`]
    /// (e.g. `:memory:` test fixtures).
    ///
    /// Used by [`Self::mtime_fn`] so `CachedCatalog` can
    /// invalidate against on-disk changes when the writer
    /// flushes new data.
    pub fn db_path(&self) -> Option<&std::path::Path> {
        self.db_path.as_deref()
    }

    /// Closure suitable for
    /// [`crate::catalog::CachedCatalog::with_mtime_fn`].
    /// Returns the latest `mtime` of the underlying file as
    /// a monotonic [`Instant`] (computed by anchoring the
    /// system-time-difference against an initial fixed
    /// epoch — `Instant` itself isn't constructable from a
    /// system time, but the comparison is what matters and
    /// the offset-relative-to-epoch survives that).
    ///
    /// Returns `None` when the source was opened via
    /// [`Self::from_connection`] (no path available) or when
    /// the file has disappeared since open.
    pub fn mtime_fn(
        &self,
    ) -> Option<impl Fn() -> Option<std::time::Instant> + Send + Sync + 'static> {
        let path = self.db_path.clone()?;
        // Anchor the mtime translation: capture
        // `Instant::now() - SystemTime::now()` once, then
        // mtime → Instant = anchor_instant + (mtime -
        // anchor_system_time). This stays monotonic for as
        // long as the system clock doesn't run backwards.
        let anchor_instant = std::time::Instant::now();
        let anchor_system = std::time::SystemTime::now();
        Some(move || -> Option<std::time::Instant> {
            let meta = std::fs::metadata(&path).ok()?;
            let mtime = meta.modified().ok()?;
            let delta = mtime.duration_since(anchor_system).ok()?;
            anchor_instant.checked_add(delta)
        })
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
        register_regexp(&conn)?;
        Ok(Self { conn: Mutex::new(conn), db_path: None })
    }
}

/// Register a connection-scoped `REGEXP(pattern, value)`
/// scalar function backed by the Rust `regex` crate. SQLite
/// recognises `value REGEXP pattern` as syntactic sugar for
/// `regexp(pattern, value)` (note the argument order — SQLite
/// passes the pattern first, the value second), so the
/// matcher-emitted SQL `v.value REGEXP ?` resolves through
/// this function for every row scan.
///
/// MetricsQL regex matchers are anchored — `label=~"pat"`
/// matches when `pat` matches the full label value, not a
/// substring. We anchor with `^(?:...)$` here so a bare
/// pattern like `label.*` matches values like `label_00`
/// without inadvertently matching `prefix_label_x`.
///
/// The compiled `Regex` is cached per query via a small LRU
/// (capacity 16) so repeated row evaluations don't re-compile
/// the pattern. Compilation errors surface as a sqlite
/// runtime error so the metricsql evaluator's error path
/// reports a useful diagnostic.
fn register_regexp(conn: &Connection) -> Result<(), DataSourceError> {
    use rusqlite::functions::FunctionFlags;
    use std::sync::Mutex as StdMutex;
    use std::collections::HashMap;
    // Compiled regex cache keyed by pattern string. Bounded
    // by the number of distinct patterns within one query —
    // metricsql evaluators only emit a handful of regex
    // matchers per expression, so a `HashMap` without
    // eviction is the right shape; cleared at the next
    // open() since the closure owns it.
    let cache: StdMutex<HashMap<String, regex::Regex>> = StdMutex::new(HashMap::new());
    conn.create_scalar_function(
        "regexp", 2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let pattern: String = ctx.get(0)?;
            let value: String = ctx.get(1)?;
            let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
            let re = match guard.get(&pattern) {
                Some(r) => r.clone(),
                None => {
                    let anchored = format!("^(?:{pattern})$");
                    let r = regex::Regex::new(&anchored)
                        .map_err(|e| rusqlite::Error::UserFunctionError(
                            format!("regexp pattern '{pattern}': {e}").into()
                        ))?;
                    guard.insert(pattern, r.clone());
                    r
                }
            };
            Ok(re.is_match(&value))
        },
    ).map_err(|e| DataSourceError::new(
        format!("register REGEXP function: {e}")))?;
    Ok(())
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
        let label_filter = instance_label_filter_clause(&other_matchers)?;

        // 3. JOIN to sample_value. Single query grouped per
        //    instance; per-instance labels are materialised
        //    in a follow-up query. `stat_expr` is a full SQL
        //    expression (referring to `sv.*` columns) so
        //    derived stats like `_rate` can blend `count` and
        //    `interval_ms` per row.
        //
        //    For `_rate` queries we additionally project
        //    `sv.interval_ms` so the loop below can warn when
        //    any sample's underlying window was sub-second —
        //    `_rate` over a sub-second window quantizes harshly
        //    (a phase that completed 10k ops in 800ms gives
        //    `12500 ops/sec`; 700ms or 900ms give the same to
        //    integer precision). Surfacing the warning prevents
        //    silent fictional precision.
        let stat_expr = resolved.stat_expr;
        let interval_proj = if resolved.is_rate { ", sv.interval_ms" } else { "" };
        let sql = format!(
            "SELECT mi.id, sv.timestamp_ms, {stat_expr}{interval_proj} \
             FROM metric_instance mi \
             JOIN sample_value sv ON sv.instance_id = mi.id \
             WHERE mi.family_id = ?1 \
               AND sv.timestamp_ms >= ?2 AND sv.timestamp_ms <= ?3 \
               {label_filter} \
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
        let mut current_samples: Vec<Sample> = Vec::new();
        // Sub-1s rate warning state: track the minimum interval
        // observed for any `_rate` row across the whole query.
        // Emit a single summary warning post-fetch so a
        // multi-series query doesn't fan out one warning per
        // instance.
        let mut min_rate_interval_ms: Option<i64> = None;

        while let Some(row) = rows.next()
            .map_err(|e| DataSourceError::new(format!("step fetch: {e}")))? {
            let instance_id: i64 = row.get(0).map_err(|e|
                DataSourceError::new(format!("row.get(0): {e}")))?;
            let timestamp_ms: i64 = row.get(1).map_err(|e|
                DataSourceError::new(format!("row.get(1): {e}")))?;
            let value: f64 = row.get::<_, Option<f64>>(2)
                .map_err(|e| DataSourceError::new(format!("row.get(2): {e}")))?
                .unwrap_or(f64::NAN);
            if resolved.is_rate {
                let iv: i64 = row.get(3).map_err(|e|
                    DataSourceError::new(format!("row.get(3): {e}")))?;
                if iv > 0 && iv < 1000 {
                    min_rate_interval_ms = Some(min_rate_interval_ms
                        .map(|m| m.min(iv))
                        .unwrap_or(iv));
                }
            }

            if Some(instance_id) != current_instance_id {
                if let Some(prev) = current_instance_id.take() {
                    out.push(materialize_series(
                        &conn, prev,
                        &resolved.virtual_name, std::mem::take(&mut current_samples),
                    )?);
                }
                current_instance_id = Some(instance_id);
            }
            current_samples.push(Sample { timestamp_ms, value });
        }
        if let Some(iv) = min_rate_interval_ms {
            eprintln!(
                "warning: `{}` evaluated over samples with sub-1s windows \
                 (shortest seen: {iv}ms). _rate divides count by interval, so \
                 a {iv}ms window quantizes the result heavily — consider \
                 rate({}[30s]) for a steady-state view over a longer span.",
                resolved.virtual_name,
                resolved.virtual_name.trim_end_matches("_rate"),
            );
        }
        if let Some(last) = current_instance_id {
            out.push(materialize_series(
                &conn, last,
                &resolved.virtual_name, current_samples,
            )?);
        }
        Ok(out)
    }
}

// =====================================================================
// MetricCatalog impl
// =====================================================================

impl MetricCatalog for SqliteDataSource {
    fn metric_families(&self) -> Result<Vec<MetricFamilyMeta>, DataSourceError> {
        let conn = self.conn.lock()
            .map_err(|_| DataSourceError::new("sqlite mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT name, type, unit, help FROM metric_family ORDER BY name",
        ).map_err(|e| DataSourceError::new(format!("prepare families: {e}")))?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        }).map_err(|e| DataSourceError::new(format!("query families: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (name, ty_str, unit, help) = row
                .map_err(|e| DataSourceError::new(format!("decode family row: {e}")))?;
            out.push(MetricFamilyMeta {
                name,
                ty: MetricType::parse(&ty_str),
                unit,
                help,
            });
        }
        Ok(out)
    }

    fn label_keys(
        &self,
        family_filter: Option<&str>,
    ) -> Result<Vec<String>, DataSourceError> {
        let conn = self.conn.lock()
            .map_err(|_| DataSourceError::new("sqlite mutex poisoned"))?;
        let mut keys: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        // `__name__` lives in `instance_label` for matcher
        // uniformity (so `{__name__="x"}` works the same way
        // as any other label filter), but the catalog's
        // `label_keys()` enumerates dimensional labels — the
        // metric family name surfaces through
        // `metric_families()` instead.
        let sql = match family_filter {
            Some(_) => {
                "SELECT DISTINCT il.key \
                 FROM instance_label il \
                 JOIN metric_instance mi ON mi.id = il.instance_id \
                 JOIN metric_family mf ON mf.id = mi.family_id \
                 WHERE mf.name = ?1 AND il.key != '__name__' \
                 ORDER BY il.key"
            }
            None => {
                "SELECT DISTINCT key FROM instance_label \
                 WHERE key != '__name__' \
                 ORDER BY key"
            }
        };

        let mut stmt = conn.prepare(sql)
            .map_err(|e| DataSourceError::new(format!("prepare label_keys: {e}")))?;
        let mut rows: Box<dyn Iterator<Item = rusqlite::Result<String>>> = match family_filter {
            Some(name) => Box::new(stmt
                .query_map([name], |r| r.get::<_, String>(0))
                .map_err(|e| DataSourceError::new(format!("query label_keys: {e}")))?
                .collect::<Vec<_>>().into_iter()),
            None => Box::new(stmt
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(|e| DataSourceError::new(format!("query label_keys: {e}")))?
                .collect::<Vec<_>>().into_iter()),
        };
        for row in &mut rows {
            let k = row.map_err(|e| DataSourceError::new(format!("decode label_key: {e}")))?;
            keys.insert(k);
        }
        Ok(keys.into_iter().collect())
    }

    fn label_values(
        &self,
        key: &str,
        family_filter: Option<&str>,
    ) -> Result<Vec<String>, DataSourceError> {
        let conn = self.conn.lock()
            .map_err(|_| DataSourceError::new("sqlite mutex poisoned"))?;
        let sql = match family_filter {
            Some(_) => {
                "SELECT DISTINCT il.value \
                 FROM instance_label il \
                 JOIN metric_instance mi ON mi.id = il.instance_id \
                 JOIN metric_family mf ON mf.id = mi.family_id \
                 WHERE il.key = ?1 AND mf.name = ?2 \
                 ORDER BY il.value"
            }
            None => {
                "SELECT DISTINCT value FROM instance_label \
                 WHERE key = ?1 \
                 ORDER BY value"
            }
        };
        let mut stmt = conn.prepare(sql)
            .map_err(|e| DataSourceError::new(format!("prepare label_values: {e}")))?;
        let mut out = Vec::new();
        let rows: Vec<rusqlite::Result<String>> = match family_filter {
            Some(name) => stmt
                .query_map([key, name], |r| r.get::<_, String>(0))
                .map_err(|e| DataSourceError::new(format!("query label_values: {e}")))?
                .collect(),
            None => stmt
                .query_map([key], |r| r.get::<_, String>(0))
                .map_err(|e| DataSourceError::new(format!("query label_values: {e}")))?
                .collect(),
        };
        for row in rows {
            out.push(row.map_err(|e| DataSourceError::new(format!("decode value: {e}")))?);
        }
        Ok(out)
    }

    fn series(
        &self,
        matchers: &[Matcher],
    ) -> Result<Vec<LabelSet>, DataSourceError> {
        let conn = self.conn.lock()
            .map_err(|_| DataSourceError::new("sqlite mutex poisoned"))?;

        // `__name__` matcher restricts to a single family;
        // bare-Eq is supported, regex / Ne is not (same
        // restriction as `fetch`).
        let name_matcher = matchers.iter().find(|m| m.label == "__name__");
        let resolved = match name_matcher.map(|m| m.op) {
            Some(MatcherOp::Eq) => {
                resolve_family(&conn, &name_matcher.unwrap().value)?
            }
            Some(_) => {
                return Err(DataSourceError::new(
                    "non-Eq match on __name__ not supported by sqlite catalog yet",
                ));
            }
            None => None,
        };

        let other_matchers: Vec<&Matcher> = matchers.iter()
            .filter(|m| m.label != "__name__")
            .collect();

        let label_filter = instance_label_filter_clause(&other_matchers)?;

        let sql_with_family = format!(
            "SELECT mi.id FROM metric_instance mi \
             WHERE mi.family_id = ?1 \
             {label_filter} \
             ORDER BY mi.id"
        );
        let sql_no_family = format!(
            "SELECT mi.id FROM metric_instance mi \
             WHERE 1=1 \
             {label_filter} \
             ORDER BY mi.id"
        );
        let sql = if resolved.is_some() { sql_with_family } else { sql_no_family };

        let mut params: Vec<Value> = Vec::new();
        if let Some(r) = &resolved {
            params.push(Value::Integer(r.family_id));
        }
        for m in &other_matchers {
            params.push(Value::Text(m.label.clone()));
            params.push(Value::Text(m.value.clone()));
        }

        let mut stmt = conn.prepare(&sql)
            .map_err(|e| DataSourceError::new(format!("prepare series: {e}")))?;
        let rows = stmt.query_map(params_from_iter(params.iter()), |r| {
            r.get::<_, i64>(0)
        }).map_err(|e| DataSourceError::new(format!("query series: {e}")))?;

        let mut out = Vec::new();
        for row in rows {
            let instance_id = row
                .map_err(|e| DataSourceError::new(format!("decode series row: {e}")))?;
            // `__name__` is just another row in instance_label
            // (the writer stores it canonically); pull labels
            // sorted, then promote `__name__` to first
            // position so the trait's caller sees the
            // OpenMetrics convention up front.
            let mut labels = materialize_instance_labels(&conn, instance_id)?;
            if let Some(pos) = labels.iter().position(|(k, _)| k == "__name__") {
                if pos != 0 {
                    let pair = labels.remove(pos);
                    labels.insert(0, pair);
                }
            }
            out.push(labels);
        }
        Ok(out)
    }

    fn exemplars(
        &self,
        matchers: &[Matcher],
        time_range: Option<(i64, i64)>,
    ) -> Result<Vec<ExemplarPoint>, DataSourceError> {
        // Translate matchers into the same instance-id
        // selection `series` uses, then JOIN onto exemplar
        // rows on the (instance_id, sample_timestamp_ms)
        // pair-key.
        let conn = self.conn.lock()
            .map_err(|_| DataSourceError::new("sqlite mutex poisoned"))?;

        let name_matcher = matchers.iter().find(|m| m.label == "__name__");
        let resolved = match name_matcher.map(|m| m.op) {
            Some(MatcherOp::Eq) => {
                resolve_family(&conn, &name_matcher.unwrap().value)?
            }
            Some(_) => {
                return Err(DataSourceError::new(
                    "non-Eq match on __name__ not supported by sqlite catalog yet",
                ));
            }
            None => None,
        };
        let other_matchers: Vec<&Matcher> = matchers.iter()
            .filter(|m| m.label != "__name__")
            .collect();
        let label_filter = instance_label_filter_clause(&other_matchers)?;

        let (start_ms, end_ms) = time_range.unwrap_or((i64::MIN, i64::MAX));

        let sql_with_family = format!(
            "SELECT mi.id, \
                    e.sample_timestamp_ms, e.value, \
                    e.timestamp_ms, e.labels_spec \
             FROM exemplar e \
             JOIN metric_instance mi ON mi.id = e.instance_id \
             WHERE mi.family_id = ?1 \
               AND e.sample_timestamp_ms >= ?2 \
               AND e.sample_timestamp_ms <= ?3 \
               {label_filter} \
             ORDER BY e.sample_timestamp_ms"
        );
        let sql_no_family = format!(
            "SELECT mi.id, \
                    e.sample_timestamp_ms, e.value, \
                    e.timestamp_ms, e.labels_spec \
             FROM exemplar e \
             JOIN metric_instance mi ON mi.id = e.instance_id \
             WHERE e.sample_timestamp_ms >= ?1 \
               AND e.sample_timestamp_ms <= ?2 \
               {label_filter} \
             ORDER BY e.sample_timestamp_ms"
        );

        let mut params: Vec<Value> = Vec::new();
        let sql = match &resolved {
            Some(r) => {
                params.push(Value::Integer(r.family_id));
                params.push(Value::Integer(start_ms));
                params.push(Value::Integer(end_ms));
                sql_with_family
            }
            None => {
                params.push(Value::Integer(start_ms));
                params.push(Value::Integer(end_ms));
                sql_no_family
            }
        };
        for m in &other_matchers {
            params.push(Value::Text(m.label.clone()));
            params.push(Value::Text(m.value.clone()));
        }

        let mut stmt = conn.prepare(&sql)
            .map_err(|e| DataSourceError::new(format!("prepare exemplars: {e}")))?;
        let rows = stmt.query_map(params_from_iter(params.iter()), |r| {
            Ok((
                r.get::<_, i64>(0)?, // instance_id
                r.get::<_, i64>(1)?, // sample_timestamp_ms
                r.get::<_, f64>(2)?, // value
                r.get::<_, Option<i64>>(3)?, // timestamp_ms
                r.get::<_, String>(4)?, // labels_spec
            ))
        }).map_err(|e| DataSourceError::new(format!("query exemplars: {e}")))?;

        let mut out = Vec::new();
        for row in rows {
            let (instance_id, sample_ts, value, ts, labels_spec) = row
                .map_err(|e| DataSourceError::new(format!("decode exemplar: {e}")))?;
            let mut series = materialize_instance_labels(&conn, instance_id)?;
            if let Some(pos) = series.iter().position(|(k, _)| k == "__name__") {
                if pos != 0 {
                    let pair = series.remove(pos);
                    series.insert(0, pair);
                }
            }
            let labels = parse_labels_spec(&labels_spec);
            out.push(ExemplarPoint {
                series,
                sample_timestamp_ms: sample_ts,
                value,
                timestamp_ms: ts,
                labels,
            });
        }
        Ok(out)
    }
}

/// Parse the `key="value",key="value"` spec encoding back
/// into a label list. Inverse of the writer's spec
/// formatter. Tolerant of trailing whitespace / empty input.
fn parse_labels_spec(spec: &str) -> Vec<(String, String)> {
    let s = spec.trim();
    if s.is_empty() { return Vec::new(); }
    // Manual tokenizer — quoted values may contain commas,
    // which serde_json would parse cleanly but we don't
    // want a JSON dep on this read path. Two-state walker.
    let mut out = Vec::new();
    let mut cur_key = String::new();
    let mut cur_val = String::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Read key up to '='.
        while i < bytes.len() && bytes[i] != b'=' {
            cur_key.push(bytes[i] as char);
            i += 1;
        }
        if i >= bytes.len() { break; }
        i += 1; // consume '='
        // Optional quote.
        let quoted = i < bytes.len() && bytes[i] == b'"';
        if quoted { i += 1; }
        while i < bytes.len() {
            if quoted {
                if bytes[i] == b'"' { i += 1; break; }
            } else if bytes[i] == b',' {
                break;
            }
            cur_val.push(bytes[i] as char);
            i += 1;
        }
        out.push((
            cur_key.trim().to_string(),
            cur_val.clone(),
        ));
        cur_key.clear();
        cur_val.clear();
        // Skip optional ',' and any whitespace.
        while i < bytes.len() && (bytes[i] == b',' || bytes[i].is_ascii_whitespace()) {
            i += 1;
        }
    }
    out
}

/// Read every label pair for an instance into a sorted Vec.
/// Post-cutover the labels live denormalised on
/// `instance_label`, including `__name__`.
fn materialize_instance_labels(
    conn: &Connection,
    instance_id: i64,
) -> Result<Vec<(String, String)>, DataSourceError> {
    let mut stmt = conn.prepare_cached(
        "SELECT key, value FROM instance_label \
         WHERE instance_id = ?1 \
         ORDER BY key",
    ).map_err(|e| DataSourceError::new(format!("prepare label set: {e}")))?;
    let rows = stmt.query_map([instance_id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    }).map_err(|e| DataSourceError::new(format!("query label set: {e}")))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| DataSourceError::new(format!("decode label entry: {e}")))?);
    }
    Ok(out)
}

/// Lookup result for `__name__` resolution.
struct ResolvedName {
    family_id: i64,
    /// Virtual name as the user wrote it (e.g. `latency_p99`).
    /// Re-applied as `__name__` on every result series so the
    /// downstream evaluator sees the name it queried for.
    virtual_name: String,
    /// Full SQL expression that produces the sample's value
    /// from a `sample_value` row aliased as `sv`. For native
    /// columns this is `sv.<col>`; the synthetic `_rate`
    /// suffix uses a derived expression that blends `count`
    /// and `interval_ms` so single-snapshot counters yield a
    /// useful per-second value without needing PromQL's
    /// `rate([window])` (which requires ≥2 samples).
    stat_expr: &'static str,
    /// `true` when the resolved name carries the synthetic
    /// `_rate` suffix. Used by [`fetch`] to project
    /// `sv.interval_ms` alongside the value column and warn
    /// when any sample's interval is below 1s — a `_rate`
    /// computation over a sub-second window quantizes
    /// heavily (a 50ms sample of a counter that counted N
    /// items gives `N × 20 ops/sec` granularity), and
    /// operators rarely want that quietly.
    is_rate: bool,
}

/// Try to resolve a metric name to a (family, stat-expr)
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
        let stat_expr = default_column_for_type(&family_type);
        return Ok(Some(ResolvedName {
            family_id,
            virtual_name: name.to_string(),
            stat_expr,
            is_rate: false,
        }));
    }
    // Suffix lookup: `<family>_<stat>` against summary,
    // histogram, counter or gauge-with-count families. The
    // suffix list is closed and ordered longest-first so
    // `_p999` is preferred over `_p9`.
    //
    // Each suffix lists which family types it applies to:
    // `_p99` only makes sense on summary/histogram (with a
    // p99 column); `_rate` makes sense on anything with a
    // `count` column (counters, summaries, histograms).
    for suffix in STAT_SUFFIXES {
        if let Some(stripped) = name.strip_suffix(suffix.text)
            && let Some((family_id, family_type)) = lookup_family(conn, stripped)?
            && suffix.applies_to(&family_type)
        {
            return Ok(Some(ResolvedName {
                family_id,
                virtual_name: name.to_string(),
                stat_expr: suffix.expr,
                is_rate: suffix.text == "_rate",
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

/// Default expression for a family type's bare-name value.
/// Returns a SQL fragment that reads from `sv.*` columns;
/// the suffix lookup overrides this when the user names a
/// specific stat.
fn default_column_for_type(family_type: &str) -> &'static str {
    match family_type {
        // Counters: cumulative observation total.
        "counter" => "sv.count",
        // Gauges: instantaneous reading.
        "gauge"   => "sv.mean",
        // Summaries: bare name returns observation count;
        // suffixes route to specific stat columns
        // (`_sum` → sum, `_p99` → p99, etc.).
        "summary" => "sv.count",
        // Histograms (bucketed): bare name returns the
        // cumulative count column. Bucket samples are
        // distinguished via the `le` label, not via the
        // metric name; selectors typically look like
        // `latency_bucket{le="0.5"}`. The `_bucket` suffix
        // resolves to the same `count` column, while
        // `_sum` and `_count` siblings resolve through the
        // STAT_SUFFIXES table.
        "histogram" => "sv.count",
        // GaugeHistogram: identical schema shape to
        // Histogram; the type label tells consumers
        // (metricsql evaluator) to allow non-monotonic
        // buckets.
        "gaugehistogram" => "sv.count",
        // Info: always-1 metric whose data lives in its
        // labels.
        "info" => "sv.count",
        // StateSet: one series per state, value 0 or 1
        // indicating whether the state is active. Stored
        // in the `mean` column (gauge convention).
        "stateset" => "sv.mean",
        // Unknown / OpenMetrics fallback: treat as gauge
        // (mean column). Defensive — any type tag we don't
        // explicitly recognise returns the mean column so
        // the query at least produces something rather
        // than empty.
        "unknown" => "sv.mean",
        _ => "sv.mean",  // Same fallback for non-spec types.
    }
}

/// Closed table of stat-name suffixes. Order matters
/// only insofar as `strip_suffix` matches the longest first
/// would — Rust's `str::strip_suffix` is exact; we just
/// iterate in the natural order.
struct StatSuffix {
    text: &'static str,
    /// SQL expression read from `sv.*` columns. For native
    /// columns this is `sv.<col>`; the synthetic `_rate`
    /// derives `(count * 1000.0 / interval_ms)` so per-sample
    /// counters yield a per-second rate without needing a
    /// range vector.
    expr: &'static str,
    /// Family-type predicate. Returns true if the suffix can
    /// be applied to a family of the given type. Percentile
    /// stats only make sense on histograms; `_rate` makes
    /// sense on anything with a `count` column.
    applies_to_fn: fn(&str) -> bool,
}

impl StatSuffix {
    fn applies_to(&self, family_type: &str) -> bool {
        (self.applies_to_fn)(family_type)
    }
}

fn applies_summary(t: &str) -> bool {
    matches!(t, "summary" | "histogram" | "gaugehistogram")
}

fn applies_counted(t: &str) -> bool {
    // Anything with a `count` column on `sample_value`. The
    // openmetrics types that fall in here: counter, summary,
    // histogram (bucketed), info. Gauges don't have count.
    matches!(t, "counter" | "summary" | "histogram"
        | "gaugehistogram" | "info")
}

const STAT_SUFFIXES: &[StatSuffix] = &[
    StatSuffix { text: "_p999",   expr: "sv.p999",   applies_to_fn: applies_summary },
    StatSuffix { text: "_p99",    expr: "sv.p99",    applies_to_fn: applies_summary },
    StatSuffix { text: "_p98",    expr: "sv.p98",    applies_to_fn: applies_summary },
    StatSuffix { text: "_p95",    expr: "sv.p95",    applies_to_fn: applies_summary },
    StatSuffix { text: "_p90",    expr: "sv.p90",    applies_to_fn: applies_summary },
    StatSuffix { text: "_p75",    expr: "sv.p75",    applies_to_fn: applies_summary },
    StatSuffix { text: "_p50",    expr: "sv.p50",    applies_to_fn: applies_summary },
    StatSuffix { text: "_count",  expr: "sv.count",  applies_to_fn: applies_summary },
    StatSuffix { text: "_sum",    expr: "sv.sum",    applies_to_fn: applies_summary },
    StatSuffix { text: "_min",    expr: "sv.min",    applies_to_fn: applies_summary },
    StatSuffix { text: "_max",    expr: "sv.max",    applies_to_fn: applies_summary },
    StatSuffix { text: "_mean",   expr: "sv.mean",   applies_to_fn: applies_summary },
    StatSuffix { text: "_stddev", expr: "sv.stddev", applies_to_fn: applies_summary },
    // Synthetic per-sample rate: count divided by the
    // sample's interval, expressed in counts-per-second.
    // Works on counters, summaries, and histograms — anything
    // whose `count` column is meaningful. For a phase that
    // produced exactly one cadence snapshot, this gives
    // `total_count / phase_duration_seconds` — the per-phase
    // throughput. For a phase with multiple snapshots, each
    // sample is the rate over its own interval; downstream
    // aggregation (`avg(... ) by (...)`) collapses them.
    // NULLIF guards a div-by-zero on the very first sample
    // where interval_ms might still be 0.
    StatSuffix {
        text: "_rate",
        expr: "(CAST(sv.count AS REAL) * 1000.0 / NULLIF(sv.interval_ms, 0))",
        applies_to_fn: applies_counted,
    },
];

/// Build the SQL fragment that narrows by label matchers.
/// Returns `""` when there are no non-`__name__` matchers.
/// Otherwise produces `AND mi.id IN (... INTERSECT ...)` —
/// one subquery per matcher hits the
/// `instance_label(key, value, instance_id)` covering index.
fn instance_label_filter_clause(matchers: &[&Matcher])
    -> Result<String, DataSourceError>
{
    if matchers.is_empty() { return Ok(String::new()); }
    let mut parts: Vec<String> = Vec::with_capacity(matchers.len());
    for (i, m) in matchers.iter().enumerate() {
        let kparam = i * 2 + 4;  // 1, 2, 3 are family_id + ts range
        let vparam = i * 2 + 5;
        let cmp_clause = match m.op {
            MatcherOp::Eq      => format!("il.key = ?{kparam} AND il.value = ?{vparam}"),
            MatcherOp::Ne      => format!("il.key = ?{kparam} AND il.value != ?{vparam}"),
            MatcherOp::EqRegex => format!("il.key = ?{kparam} AND il.value REGEXP ?{vparam}"),
            MatcherOp::NeRegex => format!("il.key = ?{kparam} AND NOT (il.value REGEXP ?{vparam})"),
        };
        parts.push(format!(
            "SELECT il.instance_id FROM instance_label il WHERE {cmp_clause}"
        ));
    }
    Ok(format!(" AND mi.id IN ({})", parts.join(" INTERSECT ")))
}

/// Build the `Series.labels` for an instance: every label
/// row, with `__name__` promoted to the first slot per the
/// trait contract. Uses `materialize_instance_labels` so the
/// single materialiser stays the chokepoint.
fn materialize_series(
    conn: &Connection,
    instance_id: i64,
    virtual_name: &str,
    samples: Vec<Sample>,
) -> Result<Series, DataSourceError> {
    let mut labels = materialize_instance_labels(conn, instance_id)?;
    // Replace any stored `__name__` with the virtual name
    // (suffix-stripping resolves `latency_p99` → family
    // `latency` + stat `p99`; we want callers to see the
    // virtual name they queried). Promote to first slot.
    labels.retain(|(k, _)| k != "__name__");
    labels.insert(0, ("__name__".to_string(), virtual_name.to_string()));
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
            CREATE TABLE metric_instance (
                id INTEGER PRIMARY KEY,
                family_id INTEGER NOT NULL,
                spec TEXT NOT NULL UNIQUE
            );
            CREATE TABLE instance_label (
                instance_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (instance_id, key)
            );
            CREATE INDEX idx_instance_label_kv
                ON instance_label(key, value, instance_id);
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

    /// Insert a family + instance with the supplied labels.
    /// Mirrors the post-cutover writer: `__name__` is stored
    /// in `instance_label` alongside every other pair; the
    /// canonical spec drives `metric_instance.spec`.
    fn make_instance(
        conn: &Connection,
        family_name: &str,
        family_type: &str,
        labels: &[(&str, &str)],
    ) -> i64 {
        conn.execute(
            "INSERT OR IGNORE INTO metric_family (name, type) VALUES (?1, ?2)",
            params![family_name, family_type]).unwrap();
        let family_id: i64 = conn.query_row(
            "SELECT id FROM metric_family WHERE name = ?1 AND type = ?2",
            params![family_name, family_type],
            |r| r.get(0)).unwrap();

        // Build the OpenMetrics-canonical spec (sorted, with
        // `__name__` excluded from the labels block).
        let mut sorted: Vec<(&str, &str)> = labels.iter()
            .filter(|(k, _)| *k != "__name__")
            .copied().collect();
        sorted.sort();
        let mut spec = String::new();
        spec.push_str(family_name);
        spec.push('{');
        for (i, (k, v)) in sorted.iter().enumerate() {
            if i > 0 { spec.push(','); }
            spec.push_str(&format!(r#"{k}="{v}""#));
        }
        spec.push('}');

        conn.execute(
            "INSERT OR IGNORE INTO metric_instance (family_id, spec) VALUES (?1, ?2)",
            params![family_id, &spec]).unwrap();
        let instance_id: i64 = conn.query_row(
            "SELECT id FROM metric_instance WHERE spec = ?1",
            params![&spec], |r| r.get(0)).unwrap();

        // `__name__` + every other label as `instance_label` rows.
        conn.execute(
            "INSERT OR IGNORE INTO instance_label (instance_id, key, value) VALUES (?1, '__name__', ?2)",
            params![instance_id, family_name]).unwrap();
        for (k, v) in &sorted {
            conn.execute(
                "INSERT OR IGNORE INTO instance_label (instance_id, key, value) VALUES (?1, ?2, ?3)",
                params![instance_id, k, v]).unwrap();
        }
        instance_id
    }

    fn add_counter_sample(conn: &Connection, instance_id: i64, ts: i64, count: i64) {
        conn.execute(
            "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, count) \
             VALUES (?1, ?2, 0, ?3)",
            params![instance_id, ts, count]).unwrap();
    }

    fn add_counter_sample_with_interval(
        conn: &Connection, instance_id: i64, ts: i64, interval_ms: i64, count: i64,
    ) {
        conn.execute(
            "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, count) \
             VALUES (?1, ?2, ?3, ?4)",
            params![instance_id, ts, interval_ms, count]).unwrap();
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
    fn rate_suffix_resolves_sub_second_window_precisely() {
        // Regression guard for the precision fix: a 7843ms
        // interval (the kind a real-elapsed phase-end flush
        // produces) must NOT round to 8000ms before the rate
        // is computed. The expected rate is 10000 / 7.843 =
        // ~1275.0 ops/sec, NOT the previous 1250 ops/sec
        // quantization that came from interval being stamped
        // to integer seconds.
        let conn = make_schema();
        let id = make_instance(&conn, "cycles_total", "counter",
            &[("limit", "1"), ("phase", "pvs_query")]);
        // 10000 ops in 7843 ms.
        add_counter_sample_with_interval(&conn, id, 7_843, 7_843, 10_000);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "cycles_total_rate".into() }],
            0, 100_000,
        ).expect("fetch cycles_total_rate");
        assert_eq!(got.len(), 1);
        let r = got[0].samples[0].value;
        // 10000 / 7.843 = 1275.020...
        // Verify it's distinctly above the old 1250 quantization
        // floor — within 0.1 of the true rate.
        assert!((r - 1275.0).abs() < 0.1,
            "expected ~1275 ops/sec for 10000 ops in 7843ms, got {r}");
        assert!(r > 1265.0,
            "rate must NOT round down to the old 1250 cluster: {r}");
    }

    #[test]
    fn rate_suffix_derives_per_second_value_from_counter() {
        // Regression guard for the throughput plot's
        // `cycles_total_rate` query path. A counter sample
        // carrying 857 ops over a 1000ms cadence interval
        // must resolve to 857 ops/sec via the synthetic
        // `_rate` suffix — no `rate([window])` rollup involved.
        let conn = make_schema();
        let id = make_instance(&conn, "cycles_total", "counter",
            &[("k", "10"), ("phase", "ann_query")]);
        add_counter_sample_with_interval(&conn, id, 1_000, 1_000, 857);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                value: "cycles_total_rate".into() }],
            0, 10_000,
        ).expect("fetch cycles_total_rate");
        assert_eq!(got.len(), 1, "one series expected, got: {got:?}");
        assert_eq!(got[0].samples.len(), 1);
        assert!((got[0].samples[0].value - 857.0).abs() < 1e-9,
            "expected 857.0 ops/s, got {}", got[0].samples[0].value);
        // Series virtual name must echo the queried suffix
        // so downstream metricsql operators see what was asked.
        assert_eq!(lookup(&got[0], "__name__"), Some("cycles_total_rate"));
    }

    #[test]
    fn rate_suffix_with_label_matchers_filters_correctly() {
        // Mirror the production plot query: filter by label
        // matchers AND apply the synthetic `_rate` suffix in
        // one fetch. Confirms the WHERE clause and the
        // synthetic stat expression cooperate.
        let conn = make_schema();
        let id_match = make_instance(&conn, "cycles_total", "counter",
            &[("k", "10"), ("phase", "ann_query"), ("profile", "label_00")]);
        let id_other = make_instance(&conn, "cycles_total", "counter",
            &[("k", "1"), ("phase", "ann_query"), ("profile", "label_00")]);
        add_counter_sample_with_interval(&conn, id_match, 100, 500, 200);
        add_counter_sample_with_interval(&conn, id_other, 100, 500, 999);

        let ds = open_ds(conn);
        let got = ds.fetch(
            &[
                Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                    value: "cycles_total_rate".into() },
                Matcher { label: "k".into(), op: MatcherOp::Eq,
                    value: "10".into() },
                Matcher { label: "phase".into(), op: MatcherOp::Eq,
                    value: "ann_query".into() },
            ],
            0, 10_000,
        ).expect("fetch");
        assert_eq!(got.len(), 1,
            "label filter should narrow to one instance; got: {got:?}");
        // 200 ops in 500ms = 400 ops/s.
        assert!((got[0].samples[0].value - 400.0).abs() < 1e-9,
            "expected 400.0 ops/s for the k=10 instance, got {}",
            got[0].samples[0].value);
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
    fn regex_matcher_filters_by_pattern() {
        // `EqRegex` on the `host` label routes through the
        // connection-scoped REGEXP function. The pattern is
        // anchored, so `label.*` matches `label_00` but not
        // `prefix_label_00` — same semantics MetricsQL uses.
        let conn = make_schema();
        let id_a = make_instance(&conn, "cpu", "gauge", &[("host", "label_00")]);
        let id_b = make_instance(&conn, "cpu", "gauge", &[("host", "label_01")]);
        let id_c = make_instance(&conn, "cpu", "gauge", &[("host", "other")]);
        for (id, v) in [(id_a, 1.0), (id_b, 2.0), (id_c, 3.0)] {
            add_gauge_sample(&conn, id, 0, v);
        }
        let ds = open_ds(conn);
        let got = ds.fetch(
            &[
                Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                    value: "cpu".into() },
                Matcher { label: "host".into(), op: MatcherOp::EqRegex,
                    value: "label.*".into() },
            ],
            0, 1000,
        ).expect("fetch");
        let mut hosts: Vec<&str> = got.iter()
            .map(|s| lookup(s, "host").unwrap_or(""))
            .collect();
        hosts.sort();
        assert_eq!(hosts, vec!["label_00", "label_01"]);
    }

    #[test]
    fn ne_regex_matcher_filters_negated() {
        // `NeRegex` is the negation: every series whose label
        // does NOT match the pattern.
        let conn = make_schema();
        let id_a = make_instance(&conn, "cpu", "gauge", &[("host", "label_00")]);
        let id_b = make_instance(&conn, "cpu", "gauge", &[("host", "other")]);
        for (id, v) in [(id_a, 1.0), (id_b, 2.0)] {
            add_gauge_sample(&conn, id, 0, v);
        }
        let ds = open_ds(conn);
        let got = ds.fetch(
            &[
                Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                    value: "cpu".into() },
                Matcher { label: "host".into(), op: MatcherOp::NeRegex,
                    value: "label.*".into() },
            ],
            0, 1000,
        ).expect("fetch");
        assert_eq!(got.len(), 1);
        assert_eq!(lookup(&got[0], "host"), Some("other"));
    }

    #[test]
    fn regex_matcher_invalid_pattern_errors() {
        // Compilation failure surfaces as a sqlite runtime
        // error from the regexp UDF; the adapter wraps it into
        // a `DataSourceError`. The metric MUST have at least
        // one sample so the regex evaluator actually runs —
        // an empty family scans zero rows and never invokes
        // the UDF.
        let conn = make_schema();
        let id = make_instance(&conn, "cpu", "gauge", &[("host", "a")]);
        add_gauge_sample(&conn, id, 0, 1.0);
        let ds = open_ds(conn);
        let err = ds.fetch(
            &[
                Matcher { label: "__name__".into(), op: MatcherOp::Eq,
                    value: "cpu".into() },
                Matcher { label: "host".into(), op: MatcherOp::EqRegex,
                    value: "[unclosed".into() },
            ],
            0, 1000,
        ).expect_err("expected regex compile error");
        assert!(err.message.to_lowercase().contains("regex")
                || err.message.to_lowercase().contains("regexp"),
            "diagnostic should mention regex/regexp: {err:?}");
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
        let ctx = EvalContext { data: &ds, start_ms: 0, end_ms: 1000, step_ms: 1, lookback_ms: None, query_start_ms: None, query_end_ms: None };
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

    // ── MetricCatalog impl tests ─────────────────────────────

    use crate::catalog::{MetricCatalog, MetricType};

    fn make_catalog_fixture() -> SqliteDataSource {
        let conn = make_schema();
        let unit_help = |name: &str, ty: &str, unit: Option<&str>, help: Option<&str>| {
            conn.execute(
                "INSERT INTO metric_family (name, type, unit, help) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![name, ty, unit, help],
            ).unwrap();
        };
        unit_help("ops_total",   "counter",   None,            Some("operations completed"));
        unit_help("cpu_load",    "gauge",     Some("ratio"),   None);
        unit_help("latency",     "histogram", Some("seconds"), Some("op latency"));
        // A separate instance per family so we have label data.
        let _ = make_instance(&conn, "ops_total", "counter", &[("phase", "setup")]);
        let _ = make_instance(&conn, "ops_total", "counter", &[("phase", "run")]);
        let _ = make_instance(&conn, "cpu_load",  "gauge",   &[("zone", "z1")]);
        let _ = make_instance(&conn, "cpu_load",  "gauge",   &[("zone", "z2")]);
        SqliteDataSource::from_connection(conn).unwrap()
    }

    #[test]
    fn catalog_metric_families_returns_full_metadata() {
        let ds = make_catalog_fixture();
        let fams = ds.metric_families().unwrap();
        let by_name: std::collections::HashMap<String, _> =
            fams.into_iter().map(|f| (f.name.clone(), f)).collect();

        let counter = by_name.get("ops_total").unwrap();
        assert_eq!(counter.ty, MetricType::Counter);
        assert_eq!(counter.unit, None);
        assert_eq!(counter.help.as_deref(), Some("operations completed"));

        let gauge = by_name.get("cpu_load").unwrap();
        assert_eq!(gauge.ty, MetricType::Gauge);
        assert_eq!(gauge.unit.as_deref(), Some("ratio"));

        let hist = by_name.get("latency").unwrap();
        assert_eq!(hist.ty, MetricType::Histogram);
        assert_eq!(hist.unit.as_deref(), Some("seconds"));
        assert_eq!(hist.help.as_deref(), Some("op latency"));
    }

    #[test]
    fn catalog_label_keys_global_and_per_family() {
        let ds = make_catalog_fixture();
        // Global view: every observed key.
        let mut all = ds.label_keys(None).unwrap();
        all.sort();
        assert!(all.contains(&"phase".to_string()));
        assert!(all.contains(&"zone".to_string()));

        // Per-family restriction.
        let ops_keys = ds.label_keys(Some("ops_total")).unwrap();
        assert_eq!(ops_keys, vec!["phase".to_string()]);
        let cpu_keys = ds.label_keys(Some("cpu_load")).unwrap();
        assert_eq!(cpu_keys, vec!["zone".to_string()]);
        let unknown = ds.label_keys(Some("nope")).unwrap();
        assert!(unknown.is_empty());
    }

    #[test]
    fn catalog_label_values_global_and_per_family() {
        let ds = make_catalog_fixture();
        let mut phases = ds.label_values("phase", None).unwrap();
        phases.sort();
        assert_eq!(phases, vec!["run".to_string(), "setup".to_string()]);

        let mut zones = ds.label_values("zone", Some("cpu_load")).unwrap();
        zones.sort();
        assert_eq!(zones, vec!["z1".to_string(), "z2".to_string()]);

        // Cross-family probe: zone isn't on ops_total.
        let none = ds.label_values("zone", Some("ops_total")).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn catalog_series_returns_label_sets_with_synthetic_name() {
        let ds = make_catalog_fixture();
        let m = vec![Matcher {
            label: "__name__".into(),
            op: MatcherOp::Eq,
            value: "ops_total".into(),
        }];
        let mut got = ds.series(&m).unwrap();
        got.sort_by(|a, b| {
            // Sort by phase value for stable assertions.
            let av = a.iter().find(|(k, _)| k == "phase").map(|(_, v)| v.as_str()).unwrap_or("");
            let bv = b.iter().find(|(k, _)| k == "phase").map(|(_, v)| v.as_str()).unwrap_or("");
            av.cmp(bv)
        });
        assert_eq!(got.len(), 2);
        for ls in &got {
            // Every series must carry the synthetic __name__.
            assert!(ls.iter().any(|(k, v)| k == "__name__" && v == "ops_total"),
                "series missing __name__: {ls:?}");
        }
        // First entry is the run phase (alphabetic).
        assert!(got[0].iter().any(|(k, v)| k == "phase" && v == "run"));
        assert!(got[1].iter().any(|(k, v)| k == "phase" && v == "setup"));
    }

    #[test]
    fn catalog_series_no_name_matcher_returns_every_series() {
        let ds = make_catalog_fixture();
        // Empty matcher list returns every (family, label-set).
        // Each family contributes 2 series in our fixture, so 4 total.
        let got = ds.series(&[]).unwrap();
        assert_eq!(got.len(), 4);
    }

    #[test]
    fn catalog_metric_type_unknown_in_db_maps_to_unknown() {
        // Manually insert a family with a non-standard type
        // string. The catalog should surface it as Unknown
        // rather than failing.
        let conn = make_schema();
        conn.execute(
            "INSERT INTO metric_family (name, type) VALUES (?1, ?2)",
            params!["weird", "untyped"]).unwrap();
        let ds = SqliteDataSource::from_connection(conn).unwrap();
        let fams = ds.metric_families().unwrap();
        assert_eq!(fams.len(), 1);
        assert_eq!(fams[0].ty, MetricType::Unknown);
    }

    // ── SRD-49: round-trip every OpenMetrics 1.0 type ──
    //
    // The catalog must surface each of the 8 OpenMetrics
    // types correctly when stored under its canonical
    // type-tag string. The writer-side `write_native_sample`
    // API (in nbrs-metrics) is the production path; here we
    // exercise the read side directly to keep the test
    // self-contained.

    fn insert_family_with_type(conn: &Connection, name: &str, ty: &str) {
        conn.execute(
            "INSERT INTO metric_family (name, type) VALUES (?1, ?2)",
            params![name, ty]).unwrap();
    }

    #[test]
    fn catalog_round_trip_histogram_type() {
        let conn = make_schema();
        insert_family_with_type(&conn, "latency", "histogram");
        // One bucket instance per `le` boundary.
        let _ = make_instance(&conn, "latency", "histogram", &[("le", "0.1")]);
        let _ = make_instance(&conn, "latency", "histogram", &[("le", "0.5")]);
        let _ = make_instance(&conn, "latency", "histogram", &[("le", "+Inf")]);
        let ds = SqliteDataSource::from_connection(conn).unwrap();
        let fams = ds.metric_families().unwrap();
        assert_eq!(fams.len(), 1);
        assert_eq!(fams[0].ty, MetricType::Histogram);
        // Bucket boundaries surface as label values for `le`.
        let mut le_values = ds.label_values("le", Some("latency")).unwrap();
        le_values.sort();
        assert_eq!(le_values, vec!["+Inf".to_string(), "0.1".to_string(), "0.5".to_string()]);
    }

    #[test]
    fn catalog_round_trip_gauge_histogram_type() {
        let conn = make_schema();
        insert_family_with_type(&conn, "queue_size_buckets", "gaugehistogram");
        let _ = make_instance(&conn, "queue_size_buckets", "gaugehistogram",
            &[("le", "10")]);
        let ds = SqliteDataSource::from_connection(conn).unwrap();
        let fams = ds.metric_families().unwrap();
        assert_eq!(fams[0].ty, MetricType::GaugeHistogram);
    }

    #[test]
    fn catalog_round_trip_info_type() {
        let conn = make_schema();
        insert_family_with_type(&conn, "build_info", "info");
        let _ = make_instance(&conn, "build_info", "info",
            &[("version", "1.2.3"), ("commit", "abc")]);
        let ds = SqliteDataSource::from_connection(conn).unwrap();
        let fams = ds.metric_families().unwrap();
        assert_eq!(fams[0].ty, MetricType::Info);
        // Info types have a known label vocabulary; ensure the
        // catalog surfaces them.
        let mut keys = ds.label_keys(Some("build_info")).unwrap();
        keys.sort();
        assert_eq!(keys, vec!["commit".to_string(), "version".to_string()]);
    }

    #[test]
    fn catalog_round_trip_stateset_type() {
        let conn = make_schema();
        insert_family_with_type(&conn, "feature_flags", "stateset");
        // One instance per state name.
        let _ = make_instance(&conn, "feature_flags", "stateset",
            &[("feature", "alpha")]);
        let _ = make_instance(&conn, "feature_flags", "stateset",
            &[("feature", "beta")]);
        let ds = SqliteDataSource::from_connection(conn).unwrap();
        let fams = ds.metric_families().unwrap();
        assert_eq!(fams[0].ty, MetricType::StateSet);
        let mut features = ds.label_values("feature", Some("feature_flags")).unwrap();
        features.sort();
        assert_eq!(features, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn catalog_round_trip_summary_type() {
        // Already tested implicitly via fetch_summary tests,
        // but pin it for the SRD-49 round-trip matrix.
        let conn = make_schema();
        insert_family_with_type(&conn, "request_latency", "summary");
        let _ = make_instance(&conn, "request_latency", "summary",
            &[("phase", "run")]);
        let ds = SqliteDataSource::from_connection(conn).unwrap();
        assert_eq!(ds.metric_families().unwrap()[0].ty, MetricType::Summary);
    }

    #[test]
    fn catalog_exemplars_round_trips() {
        // Drive both writer and reader sides — write
        // exemplars via raw SQL (the writer-side tests in
        // nbrs-metrics exercise `write_exemplar` separately),
        // then read them back through the catalog.
        let conn = make_schema();
        // Add the exemplar table the writer-side schema
        // creates. The catalog reader expects this shape.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS exemplar (
                id INTEGER PRIMARY KEY,
                instance_id INTEGER NOT NULL,
                sample_timestamp_ms INTEGER NOT NULL,
                value REAL NOT NULL,
                timestamp_ms INTEGER,
                labels_spec TEXT NOT NULL
            );"
        ).unwrap();
        insert_family_with_type(&conn, "ops_total", "counter");
        let inst_id = make_instance(&conn, "ops_total", "counter",
            &[("phase", "run")]);
        // Exemplar 1: with timestamp + trace label.
        conn.execute(
            "INSERT INTO exemplar (instance_id, sample_timestamp_ms, value, timestamp_ms, labels_spec) \
             VALUES (?1, 1000, 42.0, 1010, 'trace_id=\"abc\",span_id=\"def\"')",
            params![inst_id],
        ).unwrap();
        // Exemplar 2: without timestamp.
        conn.execute(
            "INSERT INTO exemplar (instance_id, sample_timestamp_ms, value, timestamp_ms, labels_spec) \
             VALUES (?1, 2000, 84.0, NULL, 'trace_id=\"xyz\"')",
            params![inst_id],
        ).unwrap();

        let ds = SqliteDataSource::from_connection(conn).unwrap();
        let m = vec![Matcher {
            label: "__name__".into(),
            op: MatcherOp::Eq,
            value: "ops_total".into(),
        }];
        let got = ds.exemplars(&m, None).unwrap();
        assert_eq!(got.len(), 2);
        // Sorted by sample_timestamp_ms.
        assert_eq!(got[0].sample_timestamp_ms, 1000);
        assert_eq!(got[0].value, 42.0);
        assert_eq!(got[0].timestamp_ms, Some(1010));
        // Synthetic __name__ + the instance's labels.
        assert!(got[0].series.iter().any(|(k, v)| k == "__name__" && v == "ops_total"));
        assert!(got[0].series.iter().any(|(k, v)| k == "phase" && v == "run"));
        // Exemplar's own labels parsed correctly.
        assert!(got[0].labels.iter().any(|(k, v)| k == "trace_id" && v == "abc"));
        assert!(got[0].labels.iter().any(|(k, v)| k == "span_id" && v == "def"));

        assert_eq!(got[1].sample_timestamp_ms, 2000);
        assert_eq!(got[1].timestamp_ms, None);
        assert!(got[1].labels.iter().any(|(k, v)| k == "trace_id" && v == "xyz"));
    }

    #[test]
    fn catalog_exemplars_time_range_filter() {
        let conn = make_schema();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS exemplar (
                id INTEGER PRIMARY KEY,
                instance_id INTEGER NOT NULL,
                sample_timestamp_ms INTEGER NOT NULL,
                value REAL NOT NULL,
                timestamp_ms INTEGER,
                labels_spec TEXT NOT NULL
            );"
        ).unwrap();
        insert_family_with_type(&conn, "ops_total", "counter");
        let inst_id = make_instance(&conn, "ops_total", "counter", &[]);
        for ts in [500, 1500, 2500, 3500] {
            conn.execute(
                "INSERT INTO exemplar (instance_id, sample_timestamp_ms, value, timestamp_ms, labels_spec) \
                 VALUES (?1, ?2, 1.0, NULL, 'trace_id=\"t\"')",
                params![inst_id, ts as i64],
            ).unwrap();
        }
        let ds = SqliteDataSource::from_connection(conn).unwrap();
        let m = vec![Matcher {
            label: "__name__".into(),
            op: MatcherOp::Eq,
            value: "ops_total".into(),
        }];
        let in_window = ds.exemplars(&m, Some((1000, 3000))).unwrap();
        assert_eq!(in_window.len(), 2,
            "expected 1500 + 2500; got: {:?}",
            in_window.iter().map(|e| e.sample_timestamp_ms).collect::<Vec<_>>());
        assert_eq!(in_window[0].sample_timestamp_ms, 1500);
        assert_eq!(in_window[1].sample_timestamp_ms, 2500);
    }

    #[test]
    fn parse_labels_spec_handles_quoted_values() {
        let lab = parse_labels_spec(r#"trace_id="abc",span_id="d e f""#);
        assert_eq!(lab, vec![
            ("trace_id".into(), "abc".into()),
            ("span_id".into(), "d e f".into()),
        ]);
    }

    #[test]
    fn parse_labels_spec_handles_empty_input() {
        assert_eq!(parse_labels_spec(""), Vec::<(String, String)>::new());
        assert_eq!(parse_labels_spec("   "), Vec::<(String, String)>::new());
    }

    #[test]
    fn catalog_default_column_for_type_covers_all_eight_types() {
        // Pin the expression-routing convention from
        // [`default_column_for_type`]. Each type has a
        // canonical sample column the bare-name selector
        // returns; expressions are now fully-qualified
        // (`sv.<col>`) so the SQL template can blend in
        // derived stat suffixes like `_rate`.
        assert_eq!(default_column_for_type("counter"),         "sv.count");
        assert_eq!(default_column_for_type("gauge"),           "sv.mean");
        assert_eq!(default_column_for_type("summary"),         "sv.count");
        assert_eq!(default_column_for_type("histogram"),       "sv.count");
        assert_eq!(default_column_for_type("gaugehistogram"),  "sv.count");
        assert_eq!(default_column_for_type("info"),            "sv.count");
        assert_eq!(default_column_for_type("stateset"),        "sv.mean");
        assert_eq!(default_column_for_type("unknown"),         "sv.mean");
    }
}
