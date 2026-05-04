// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SQLite metrics reporter with normalized OpenMetrics-aligned schema.
//!
//! Feature-gated behind `sqlite`.

#[cfg(feature = "sqlite")]
mod inner {
    use std::collections::HashMap;
    use std::path::Path;

    use rusqlite::{Connection, params};

    use crate::labels::Labels;
    use crate::scheduler::Reporter;
    use crate::snapshot::{Metric, MetricFamily, MetricSet, MetricType, MetricValue};

    pub struct SqliteReporter {
        conn: Connection,
        // Caches for deduplication
        family_cache: HashMap<String, i64>,
        label_key_cache: HashMap<String, i64>,
        label_value_cache: HashMap<String, i64>,
        label_set_cache: HashMap<u64, i64>,
        instance_cache: HashMap<(i64, i64), i64>,
    }

    impl SqliteReporter {
        pub fn new(path: impl AsRef<Path>) -> Result<Self, String> {
            let conn = Connection::open(path)
                .map_err(|e| format!("failed to open SQLite: {e}"))?;
            // WAL mode: readers don't block writers, no fsync on every commit.
            // synchronous=NORMAL: fsync only on WAL checkpoint, not every transaction.
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;\
                 PRAGMA synchronous=NORMAL;\
                 PRAGMA wal_autocheckpoint=1000;"
            ).map_err(|e| format!("failed to set SQLite pragmas: {e}"))?;
            let mut reporter = Self {
                conn,
                family_cache: HashMap::new(),
                label_key_cache: HashMap::new(),
                label_value_cache: HashMap::new(),
                label_set_cache: HashMap::new(),
                instance_cache: HashMap::new(),
            };
            reporter.create_schema()?;
            Ok(reporter)
        }

        pub fn in_memory() -> Result<Self, String> {
            let conn = Connection::open_in_memory()
                .map_err(|e| format!("failed to open in-memory SQLite: {e}"))?;
            let mut reporter = Self {
                conn,
                family_cache: HashMap::new(),
                label_key_cache: HashMap::new(),
                label_value_cache: HashMap::new(),
                label_set_cache: HashMap::new(),
                instance_cache: HashMap::new(),
            };
            reporter.create_schema()?;
            Ok(reporter)
        }

        /// Wholesale-purge every sample row whose owning
        /// `metric_instance` carries the supplied label set as a
        /// **superset match** — i.e. every (key, value) pair in
        /// `labels` is present on the instance, regardless of
        /// extra labels the instance may also carry.
        ///
        /// Used by the checkpoint resume path (SRD-44 §"Wholesale
        /// metrics-purge"): a phase that re-runs from scratch on
        /// resume must invalidate the prior invocation's rows so
        /// downstream summaries don't double-count.
        ///
        /// Returns the number of `sample_value` rows deleted.
        /// Best-effort under SQL errors — logs and returns 0
        /// rather than propagating, since a purge failure
        /// shouldn't abort the run (it surfaces as a duplicate-
        /// counting metric, not silent corruption of state).
        pub fn purge_samples_with_labels(
            &mut self,
            labels: &Labels,
        ) -> usize {
            // Build the AND-of-EXISTS query: for each (k, v) in
            // labels, the instance's label_set must have an
            // entry whose label_key.key = k AND label_value.value
            // = v. Subquery enumerates instance ids matching all
            // pairs.
            let pairs: Vec<(String, String)> = labels.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            if pairs.is_empty() {
                return 0;
            }
            let exists_clauses: Vec<String> = (0..pairs.len()).map(|i| {
                let kparam = i * 2 + 1;
                let vparam = i * 2 + 2;
                format!(
                    "EXISTS (SELECT 1 FROM label_set_entry e \
                     JOIN label_key k ON k.id = e.key_id \
                     JOIN label_value v ON v.id = e.value_id \
                     WHERE e.set_id = mi.label_set_id \
                     AND k.key = ?{kparam} AND v.value = ?{vparam})",
                )
            }).collect();
            let sql = format!(
                "DELETE FROM sample_value WHERE instance_id IN (\
                   SELECT mi.id FROM metric_instance mi WHERE {})",
                exists_clauses.join(" AND "),
            );
            let mut bound: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in &pairs {
                bound.push(k as &dyn rusqlite::ToSql);
                bound.push(v as &dyn rusqlite::ToSql);
            }
            match self.conn.execute(&sql, rusqlite::params_from_iter(bound.iter().copied())) {
                Ok(n) => n,
                Err(e) => {
                    crate::diag::warn(&format!(
                        "warning: sqlite purge_samples_with_labels failed: {e}"));
                    0
                }
            }
        }

        /// Store a session metadata key-value pair.
        pub fn set_metadata(&mut self, key: &str, value: &str) {
            self.conn.execute(
                "INSERT OR REPLACE INTO session_metadata (key, value) VALUES (?1, ?2)",
                params![key, value],
            ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite metadata write: {e}")); 0 });
        }

        fn create_schema(&mut self) -> Result<(), String> {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS metric_family (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    unit TEXT,
                    help TEXT,
                    UNIQUE(name, type)
                );
                CREATE TABLE IF NOT EXISTS label_key (
                    id INTEGER PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS label_value (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS label_set (
                    id INTEGER PRIMARY KEY,
                    hash INTEGER NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS label_set_entry (
                    set_id INTEGER NOT NULL REFERENCES label_set(id),
                    key_id INTEGER NOT NULL REFERENCES label_key(id),
                    value_id INTEGER NOT NULL REFERENCES label_value(id)
                );
                CREATE TABLE IF NOT EXISTS metric_instance (
                    id INTEGER PRIMARY KEY,
                    family_id INTEGER NOT NULL REFERENCES metric_family(id),
                    label_set_id INTEGER NOT NULL REFERENCES label_set(id),
                    -- Denormalized spec for easy querying without joins.
                    spec TEXT,
                    UNIQUE(family_id, label_set_id)
                );
                CREATE TABLE IF NOT EXISTS sample_value (
                    instance_id INTEGER NOT NULL REFERENCES metric_instance(id),
                    timestamp_ms INTEGER NOT NULL,
                    interval_ms INTEGER NOT NULL,
                    count INTEGER,
                    sum REAL,
                    min REAL,
                    max REAL,
                    mean REAL,
                    stddev REAL,
                    p50 REAL, p75 REAL, p90 REAL, p95 REAL,
                    p98 REAL, p99 REAL, p999 REAL
                );
                CREATE TABLE IF NOT EXISTS session_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                -- Indexes for read paths. All `IF NOT EXISTS`
                -- so existing databases pick them up on next
                -- open without a separate migration. See
                -- SRD-47 §\"What's missing right now\" — the
                -- metricsql DataSource adapter uses these
                -- to avoid full table scans.
                --
                -- (label_set_entry.key_id, value_id, set_id):
                --   matcher resolution — \"which sets have
                --   label X = value Y\" is the inner loop of
                --   every selector.
                CREATE INDEX IF NOT EXISTS idx_label_set_entry_kv
                    ON label_set_entry(key_id, value_id, set_id);
                -- (label_set_entry.set_id):
                --   materializing a result series's labels
                --   from its label_set_id.
                CREATE INDEX IF NOT EXISTS idx_label_set_entry_set
                    ON label_set_entry(set_id);
                -- (sample_value.instance_id, timestamp_ms):
                --   range scans for time-window queries. Not
                --   a primary key (would require a schema
                --   break) but co-locates samples per
                --   instance in time order.
                CREATE INDEX IF NOT EXISTS idx_sample_value_inst_ts
                    ON sample_value(instance_id, timestamp_ms);
                -- (metric_instance.family_id):
                --   bypasses the UNIQUE composite when only
                --   the family side is known (e.g. \"all
                --   instances of `latency`\").
                CREATE INDEX IF NOT EXISTS idx_metric_instance_family
                    ON metric_instance(family_id);"
            ).map_err(|e| format!("schema creation failed: {e}"))?;
            Ok(())
        }

        fn get_or_insert_family(&mut self, name: &str, typ: &str) -> i64 {
            let key = format!("{name}:{typ}");
            if let Some(&id) = self.family_cache.get(&key) {
                return id;
            }
            self.conn.execute(
                "INSERT OR IGNORE INTO metric_family (name, type) VALUES (?1, ?2)",
                params![name, typ],
            ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
            let id: i64 = self.conn.query_row(
                "SELECT id FROM metric_family WHERE name=?1 AND type=?2",
                params![name, typ],
                |row| row.get(0),
            ).unwrap_or(0);
            self.family_cache.insert(key, id);
            id
        }

        fn get_or_insert_label_key(&mut self, key: &str) -> i64 {
            if let Some(&id) = self.label_key_cache.get(key) {
                return id;
            }
            self.conn.execute(
                "INSERT OR IGNORE INTO label_key (key) VALUES (?1)",
                params![key],
            ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
            let id: i64 = self.conn.query_row(
                "SELECT id FROM label_key WHERE key=?1",
                params![key], |row| row.get(0),
            ).unwrap_or(0);
            self.label_key_cache.insert(key.to_string(), id);
            id
        }

        fn get_or_insert_label_value(&mut self, value: &str) -> i64 {
            if let Some(&id) = self.label_value_cache.get(value) {
                return id;
            }
            self.conn.execute(
                "INSERT OR IGNORE INTO label_value (value) VALUES (?1)",
                params![value],
            ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
            let id: i64 = self.conn.query_row(
                "SELECT id FROM label_value WHERE value=?1",
                params![value], |row| row.get(0),
            ).unwrap_or(0);
            self.label_value_cache.insert(value.to_string(), id);
            id
        }

        fn get_or_insert_label_set(&mut self, labels: &Labels) -> i64 {
            let hash = labels.identity_hash();
            if let Some(&id) = self.label_set_cache.get(&hash) {
                return id;
            }
            self.conn.execute(
                "INSERT OR IGNORE INTO label_set (hash) VALUES (?1)",
                params![hash as i64],
            ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
            let set_id: i64 = self.conn.query_row(
                "SELECT id FROM label_set WHERE hash=?1",
                params![hash as i64], |row| row.get(0),
            ).unwrap_or(0);

            // Insert label entries
            for (k, v) in labels.iter() {
                let key_id = self.get_or_insert_label_key(k);
                let val_id = self.get_or_insert_label_value(v);
                self.conn.execute(
                    "INSERT OR IGNORE INTO label_set_entry (set_id, key_id, value_id) VALUES (?1, ?2, ?3)",
                    params![set_id, key_id, val_id],
                ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
            }

            self.label_set_cache.insert(hash, set_id);
            set_id
        }

        fn get_or_insert_instance(&mut self, family_id: i64, label_set_id: i64, name: &str, labels: &Labels) -> i64 {
            let key = (family_id, label_set_id);
            if let Some(&id) = self.instance_cache.get(&key) {
                return id;
            }
            // Build denormalized spec: name{key="value",key="value"}
            let label_pairs: Vec<String> = labels.iter()
                .filter(|(k, _)| *k != "name")
                .map(|(k, v)| format!("{k}=\"{v}\""))
                .collect();
            let spec = if label_pairs.is_empty() {
                name.to_string()
            } else {
                format!("{name}{{{}}}", label_pairs.join(","))
            };
            self.conn.execute(
                "INSERT OR IGNORE INTO metric_instance (family_id, label_set_id, spec) VALUES (?1, ?2, ?3)",
                params![family_id, label_set_id, spec],
            ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
            let id: i64 = self.conn.query_row(
                "SELECT id FROM metric_instance WHERE family_id=?1 AND label_set_id=?2",
                params![family_id, label_set_id], |row| row.get(0),
            ).unwrap_or(0);
            self.instance_cache.insert(key, id);
            id
        }

        fn insert_metric(&mut self, snapshot: &MetricSet, family: &MetricFamily, metric: &Metric) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let interval_ms = snapshot.interval().as_millis() as i64;
            let name = family.name();
            let labels = metric.labels();
            let Some(point) = metric.point() else { return };

            match point.value() {
                MetricValue::Counter(c) => {
                    let family_id = self.get_or_insert_family(name, "counter");
                    let label_set_id = self.get_or_insert_label_set(labels);
                    let instance_id = self.get_or_insert_instance(family_id, label_set_id, name, labels);

                    self.conn.execute(
                        "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, count) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params![instance_id, now_ms, interval_ms, c.total as i64],
                    ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
                }
                MetricValue::Gauge(g) => {
                    let family_id = self.get_or_insert_family(name, "gauge");
                    let label_set_id = self.get_or_insert_label_set(labels);
                    let instance_id = self.get_or_insert_instance(family_id, label_set_id, name, labels);

                    self.conn.execute(
                        "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, mean) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params![instance_id, now_ms, interval_ms, g.value],
                    ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
                }
                MetricValue::Histogram(h) => {
                    let family_id = self.get_or_insert_family(name, "summary");
                    let label_set_id = self.get_or_insert_label_set(labels);
                    let instance_id = self.get_or_insert_instance(family_id, label_set_id, name, labels);

                    let r = &h.reservoir;
                    let obs = h.count as i64;
                    let min = r.min() as f64;
                    let max = r.max() as f64;
                    let mean = r.mean();
                    let stddev = r.stdev();
                    let sum = h.sum;

                    let p50 = r.value_at_quantile(0.50) as f64;
                    let p75 = r.value_at_quantile(0.75) as f64;
                    let p90 = r.value_at_quantile(0.90) as f64;
                    let p95 = r.value_at_quantile(0.95) as f64;
                    let p98 = r.value_at_quantile(0.98) as f64;
                    let p99 = r.value_at_quantile(0.99) as f64;
                    let p999 = r.value_at_quantile(0.999) as f64;

                    self.conn.execute(
                        "INSERT INTO sample_value \
                         (instance_id, timestamp_ms, interval_ms, count, sum, min, max, mean, stddev, \
                          p50, p75, p90, p95, p98, p99, p999) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                        params![
                            instance_id, now_ms, interval_ms, obs, sum, min, max, mean, stddev,
                            p50, p75, p90, p95, p98, p99, p999
                        ],
                    ).unwrap_or_else(|e| { crate::diag::warn(&format!("warning: sqlite write failed: {e}")); 0 });
                }
            }
            let _ = MetricType::Counter; // silence unused-import on the Counter path
        }
    }

    /// Configuration for the summary report, passed from the runner.
    ///
    /// This is the nbrs-metrics–local mirror of the workload-level
    /// `SummaryConfig`. The runner converts one to the other so that
    /// nbrs-metrics does not depend on nbrs-workload.
    pub struct ReportConfig {
        /// Gauge column filter patterns. Empty = show all.
        pub columns: Vec<String>,
        /// Row filter regex patterns on activity labels.
        pub row_filters: Vec<String>,
        /// Aggregate expressions.
        pub aggregates: Vec<ReportAggregate>,
        /// Whether to show individual data rows.
        pub show_details: bool,
    }

    /// An aggregate expression for the summary report. Two
    /// shapes:
    ///
    /// 1. Single-key filter (`label_key`/`label_pattern` set,
    ///    `group_by` empty): one aggregate row across rows
    ///    matching the filter.
    /// 2. Multi-key grouping (`group_by` non-empty): one
    ///    aggregate row per distinct tuple of values across
    ///    `group_by` keys, taken across the rows that have
    ///    those values.
    pub struct ReportAggregate {
        /// Function name: `"mean"`, `"min"`, or `"max"`.
        pub function: String,
        /// Column name pattern — only matching gauge columns are aggregated.
        pub column_pattern: String,
        /// Label key to filter rows on (single-key form). Empty
        /// for multi-key grouping.
        pub label_key: String,
        /// Substring pattern for the label value (single-key form).
        pub label_pattern: String,
        /// Multi-key grouping: when non-empty, group rows by
        /// every distinct value-tuple across these label keys
        /// and emit one aggregate row per group.
        pub group_by: Vec<String>,
    }

    impl SqliteReporter {
        /// Print a data-driven summary of all metrics collected in this session.
        ///
        /// Thin wrapper around [`format_summary`] that emits to stdout.
        /// See [`format_summary`] for column-discovery semantics.
        pub fn print_summary(&self, config: &ReportConfig) {
            let rendered = self.format_summary(config);
            if !rendered.is_empty() {
                print!("{rendered}");
            }
        }

        /// Render the data-driven summary as a string.
        ///
        /// One row per distinct label set that has `cycles_total > 0`.
        /// Columns are discovered from the metrics that exist:
        /// - cycles and rate are always shown
        /// - latency columns appear when `cycles_servicetime` data exists
        /// - gauge columns appear when gauge data exists
        ///
        /// The `config` controls column filters, row filters, aggregate
        /// expressions, and whether detail rows are shown. Returns an
        /// empty string when there is no data to report.
        pub fn format_summary(&self, config: &ReportConfig) -> String {
            self.format_summary_with_format(config, "md")
        }

        /// Render the summary in the requested format. Recognized
        /// formats: `"md"` (Markdown table — same as
        /// [`Self::format_summary`]) and `"csv"`. Unknown
        /// formats fall back to Markdown.
        ///
        /// Both formats share the same data-extraction pipeline
        /// (filters, gauge discovery, aggregates) — only the
        /// final stringify step differs.
        pub fn format_summary_with_format(
            &self,
            config: &ReportConfig,
            format: &str,
        ) -> String {
            let Some((headers, grid)) = self.build_summary_grid(config) else {
                return String::new();
            };
            match format {
                "csv" => render_csv(&headers, &grid),
                _ => render_markdown(&headers, &grid),
            }
        }

        /// Read every named summary previously persisted into
        /// the metrics db's `session_metadata` table under the
        /// `summary.<name>` key namespace. Returns
        /// `(name, spec_text)` pairs in deterministic
        /// (alphabetical) order so output filenames are stable
        /// across regeneration runs.
        ///
        /// Used by `nbrs --summary` (no spec given) to enumerate
        /// every report defined by the workload that produced
        /// this db, regenerating each one without needing the
        /// original workload file.
        pub fn read_stored_summaries(&self) -> Vec<(String, String)> {
            // SRD-46: persisted items live under `report.<name>`
            // with a kind keyword on the first line. This call
            // enumerates only the `table` items, stripping the
            // kind/name/label prelude so the returned spec is
            // the body the table renderer expects.
            let mut stmt = match self.conn.prepare(
                "SELECT key, value FROM session_metadata \
                 WHERE key LIKE 'report.%' ORDER BY rowid"
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let mut out = Vec::new();
            if let Ok(iter) = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            }) {
                for entry in iter.flatten() {
                    let mut lines = entry.1.lines();
                    let head = match lines.next() { Some(h) => h, None => continue };
                    let name = match head.strip_prefix("table ") {
                        Some(rest) => rest.trim().to_string(),
                        None => continue,
                    };
                    let body: String = lines
                        .filter(|l| !l.starts_with("label "))
                        .collect::<Vec<_>>().join("\n");
                    out.push((name, body));
                }
            }
            out
        }

        /// Build the headers + grid (data rows + aggregates) for
        /// a summary, applying every filter and aggregate from
        /// `config`. Returns `None` if there's nothing to
        /// render. Shared between every output-format renderer
        /// (`md`, `csv`, …).
        fn build_summary_grid(&self, config: &ReportConfig)
            -> Option<(Vec<String>, Vec<Vec<String>>)>
        {
            let row_patterns: Vec<regex::Regex> = config.row_filters.iter()
                .filter_map(|p| regex::Regex::new(p.trim()).ok())
                .collect();

            let rows = self.query_all_activities();
            if rows.is_empty() { return None; }

            // Discover which optional column groups have data
            let has_latency = rows.iter().any(|r| r.latency_p50_ns.is_some());
            let mut gauge_names: Vec<String> = Vec::new();
            for row in &rows {
                for (name, _) in &row.gauges {
                    if !gauge_names.contains(name) {
                        let include = if config.columns.is_empty() {
                            true
                        } else {
                            config.columns.iter().any(|p| name.contains(p))
                        };
                        if include {
                            gauge_names.push(name.clone());
                        }
                    }
                }
            }

            // Build column headers
            let mut headers: Vec<String> = vec![
                "Activity".into(), "Cycles".into(), "Rate".into(),
            ];
            if has_latency {
                headers.extend(["p50".into(), "p99".into(), "mean".into()]);
            }
            for name in &gauge_names {
                headers.push(name.clone());
            }

            // Build cell grid from data rows
            let mut grid: Vec<Vec<String>> = Vec::new();
            for row in &rows {
                if !row_patterns.is_empty()
                    && !row_patterns.iter().any(|p| p.is_match(&row.activity))
                {
                    continue;
                }
                let cells = format_data_row(row, has_latency, &gauge_names);
                grid.push(cells);
            }

            // Compute aggregate rows
            let agg_rows = compute_aggregates(
                &config.aggregates, &rows, has_latency, &gauge_names,
            );

            // If details=hide, drop data rows and show only aggregates
            if !config.show_details {
                grid.clear();
            }

            if grid.is_empty() && agg_rows.is_empty() { return None; }

            // Align label components within the Activity column (data rows only).
            align_activity_column(&mut grid);

            // Append aggregate rows after a blank separator
            if !agg_rows.is_empty() && !grid.is_empty() {
                let blank: Vec<String> = (0..headers.len()).map(|_| String::new()).collect();
                grid.push(blank);
            }
            grid.extend(agg_rows);

            Some((headers, grid))
        }

        /// Query all activities that produced data, returning one row per
        /// distinct label set. No hardcoded phase name patterns — the
        /// summary is projected directly from whatever the workload produced.
        fn query_all_activities(&self) -> Vec<ActivityRow> {
            // Find every distinct label set that has cycles_total > 0.
            // Phase-level inclusion / exclusion is gone — every
            // active phase contributes a row by default; the
            // `report:` block (SRD-46) decides what gets
            // rendered into which file.
            let mut stmt = match self.conn.prepare(
                "SELECT mi.spec, MAX(sv.count)
                 FROM sample_value sv
                 JOIN metric_instance mi ON sv.instance_id = mi.id
                 WHERE mi.spec LIKE 'cycles_total%'
                 GROUP BY mi.id
                 HAVING MAX(sv.count) > 0
                 ORDER BY mi.id"
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };

            let mut rows: Vec<(Vec<(String, String)>, ActivityRow)> = Vec::new();
            let iter = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            });

            if let Ok(iter) = iter {
                for r in iter.filter_map(|r| r.ok()) {
                    let labels = Self::spec_labels(&r.0);
                    if labels.is_empty() { continue; }
                    let display = extract_labels_display(&r.0);
                    let cycles = r.1 as u64;

                    let elapsed = self.query_elapsed_ms(labels);
                    let rate = if elapsed > 0.0 { cycles as f64 * 1000.0 / elapsed } else { 0.0 };
                    let latency = self.query_latency(labels);
                    let gauges = self.query_gauges_for_labels(labels);

                    let sort_key = parse_label_pairs(labels);
                    rows.push((sort_key, ActivityRow {
                        activity: display,
                        cycles,
                        rate,
                        latency_p50_ns: latency.map(|l| l.0),
                        latency_p99_ns: latency.map(|l| l.1),
                        latency_mean_ns: latency.map(|l| l.2),
                        gauges,
                    }));
                }
            }

            // Canonical presentation order: sort rows by the
            // alphabetised (key, value) tuples extracted from
            // each row's labels. Values that look like integers
            // compare numerically (`limit=10` after `limit=2`,
            // not before).
            rows.sort_by(|a, b| compare_label_tuples(&a.0, &b.0));
            rows.into_iter().map(|(_, r)| r).collect()
        }

        /// Query latency stats for a label set.
        ///
        /// Returns `(p50_ns, p99_ns, mean_ns)` in nanoseconds, or `None`.
        /// Uses the sample with the most observations (highest `count`)
        /// rather than the chronologically last one, because delta-histogram
        /// snapshots can produce empty trailing samples when a phase ends
        /// between capture intervals.
        fn query_latency(&self, label_part: &str) -> Option<(f64, f64, f64)> {
            let spec = format!("cycles_servicetime{{{label_part}}}");
            self.conn.query_row(
                "SELECT sv.p50, sv.p99, sv.mean
                 FROM sample_value sv
                 JOIN metric_instance mi ON sv.instance_id = mi.id
                 WHERE mi.spec = ?1
                 ORDER BY sv.count DESC
                 LIMIT 1",
                params![spec],
                |row| Ok((
                    row.get::<_, f64>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, f64>(2)?,
                )),
            ).ok()
        }

        /// Query all gauge values matching a label set.
        /// Returns `(short_name, value)` pairs. Gauge names have the
        /// `.mean`/`.p50`/etc. suffix stripped — only `.mean` is collected.
        ///
        /// Gauge labels may be a superset of the activity labels (e.g.
        /// they include `n="100"`), so we match both exact and extended.
        fn query_gauges_for_labels(&self, label_part: &str) -> Vec<(String, f64)> {
            let exact = format!("%{{{label_part}}}");
            let extended = format!("%{{{label_part},%");
            let mut stmt = match self.conn.prepare(
                "SELECT mi.spec, sv.mean FROM sample_value sv
                 JOIN metric_instance mi ON sv.instance_id = mi.id
                 JOIN metric_family mf ON mi.family_id = mf.id
                 WHERE mf.type = 'gauge'
                   AND (mi.spec LIKE ?1 OR mi.spec LIKE ?2)
                 ORDER BY mi.spec"
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let mut seen = std::collections::HashSet::new();
            stmt.query_map(params![exact, extended], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
            }).ok()
                .map(|r| r.filter_map(|r| r.ok())
                    .filter_map(|(spec, val)| {
                        let name = spec.split('{').next().unwrap_or(&spec);
                        // Only collect .mean variants, strip the suffix
                        if !name.ends_with(".mean") { return None; }
                        let short = name.strip_suffix(".mean").unwrap_or(name);
                        if seen.contains(short) { return None; }
                        seen.insert(short.to_string());
                        Some((short.to_string(), val))
                    })
                    .collect())
                .unwrap_or_default()
        }

        /// Extract the labels portion of a spec (everything inside {}).
        fn spec_labels(spec: &str) -> &str {
            spec.split('{').nth(1)
                .and_then(|s| s.strip_suffix('}'))
                .unwrap_or("")
        }

        /// Get elapsed wall-clock time for a label set by finding the time
        /// range across all metrics sharing those labels.
        /// Total activity duration in milliseconds for a given label
        /// set. Uses the sum of `cycles_total` sample intervals — each
        /// row is one closed cadence window, so the sum is the total
        /// time the phase produced data. This is the correct rate
        /// denominator.
        ///
        /// An earlier implementation used `MAX(ts) - MIN(ts)` across
        /// every family, which conflated write-time spread (~ms) with
        /// phase duration (seconds to minutes) — a 2-second phase
        /// would report elapsed ≈ 2ms and blow rates into the hundreds
        /// of thousands per second.
        fn query_elapsed_ms(&self, label_part: &str) -> f64 {
            let spec = format!("cycles_total{{{label_part}}}");
            let result: Result<i64, _> = self.conn.query_row(
                "SELECT COALESCE(SUM(sv.interval_ms), 0)
                 FROM sample_value sv
                 JOIN metric_instance mi ON sv.instance_id = mi.id
                 WHERE mi.spec = ?1",
                params![spec],
                |row| row.get(0),
            );
            result.ok().map(|ms| ms as f64).unwrap_or(0.0)
        }
    }

    /// One row in the summary table — one per distinct label set.
    struct ActivityRow {
        activity: String,
        cycles: u64,
        rate: f64,
        /// Latency percentiles in nanoseconds (sysref: all internal time = nanos).
        latency_p50_ns: Option<f64>,
        latency_p99_ns: Option<f64>,
        latency_mean_ns: Option<f64>,
        /// Gauge values keyed by short name (e.g. "recall@10").
        gauges: Vec<(String, f64)>,
    }

    /// Parse a `key="value", key="value"` label string (the
    /// portion between `{...}` in a Prometheus-style spec) into
    /// a `Vec<(key, value)>` sorted alphabetically by key. Used
    /// as the canonical sort tuple for rows in
    /// `build_summary_grid` so dimensional labels — not metric-
    /// instance insertion order — establish presentation order.
    pub(crate) fn parse_label_pairs(label_part: &str) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        let bytes = label_part.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b',') { i += 1; }
            if i >= bytes.len() { break; }
            let key_start = i;
            while i < bytes.len() && bytes[i] != b'=' { i += 1; }
            if i >= bytes.len() { break; }
            let key = label_part[key_start..i].trim().to_string();
            i += 1; // consume '='
            if i < bytes.len() && bytes[i] == b'"' {
                i += 1;
                let val_start = i;
                while i < bytes.len() && bytes[i] != b'"' { i += 1; }
                let val = label_part[val_start..i].to_string();
                if i < bytes.len() { i += 1; }
                out.push((key, val));
            } else {
                let val_start = i;
                while i < bytes.len() && !matches!(bytes[i], b',' | b' ' | b'\t') {
                    i += 1;
                }
                let val = label_part[val_start..i].to_string();
                out.push((key, val));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Lexicographic compare on alphabetised label tuples with
    /// natural-numeric value compare (so `limit=10` lands after
    /// `limit=2`, not before). Keys are already sorted by
    /// [`parse_label_pairs`]; this just zips and compares.
    pub(crate) fn compare_label_tuples(
        a: &[(String, String)],
        b: &[(String, String)],
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        for (av, bv) in a.iter().zip(b.iter()) {
            match av.0.cmp(&bv.0) {
                Ordering::Equal => {}
                other => return other,
            }
            match natural_value_cmp(&av.1, &bv.1) {
                Ordering::Equal => {}
                other => return other,
            }
        }
        a.len().cmp(&b.len())
    }

    fn natural_value_cmp(a: &str, b: &str) -> std::cmp::Ordering {
        match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(x), Ok(y)) => x.cmp(&y),
            _ => a.cmp(b),
        }
    }

    /// Auto-select the time unit suffix so the numeric part has significant digits.
    ///
    /// Input is nanoseconds (sysref standard). Output always includes a unit suffix.
    fn format_duration(nanos: f64) -> String {
        if nanos >= 1_000_000_000.0 {
            format!("{:.2}s", nanos / 1_000_000_000.0)
        } else if nanos >= 1_000_000.0 {
            format!("{:.2}ms", nanos / 1_000_000.0)
        } else if nanos >= 1_000.0 {
            format!("{:.2}µs", nanos / 1_000.0)
        } else {
            format!("{:.2}ns", nanos)
        }
    }

    /// Format a single data row into cell strings.
    fn format_data_row(
        row: &ActivityRow,
        has_latency: bool,
        gauge_names: &[String],
    ) -> Vec<String> {
        let rate_str = if row.rate > 0.0 {
            format!("{:.0}/s", row.rate)
        } else {
            "-".to_string()
        };
        let mut cells: Vec<String> = vec![
            row.activity.clone(),
            row.cycles.to_string(),
            rate_str,
        ];
        if has_latency {
            if let (Some(p50), Some(p99), Some(mean)) =
                (row.latency_p50_ns, row.latency_p99_ns, row.latency_mean_ns)
            {
                cells.push(format_duration(p50));
                cells.push(format_duration(p99));
                cells.push(format_duration(mean));
            } else {
                cells.extend(["-".into(), "-".into(), "-".into()]);
            }
        }
        for name in gauge_names {
            let val = row.gauges.iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| format!("{v:.4}"))
                .unwrap_or_else(|| "-".to_string());
            cells.push(val);
        }
        cells
    }

    /// Compute aggregate rows from the data.
    ///
    /// Each `ReportAggregate` produces one row. The activity column shows
    /// the expression (e.g., `mean(recall) over profile~label`). Gauge
    /// columns matching `column_pattern` are aggregated; others show `-`.
    fn compute_aggregates(
        aggregates: &[ReportAggregate],
        rows: &[ActivityRow],
        has_latency: bool,
        gauge_names: &[String],
    ) -> Vec<Vec<String>> {
        let mut agg_rows = Vec::new();

        for agg in aggregates {
            if !agg.group_by.is_empty() {
                // Multi-key grouping: one aggregate row per
                // distinct value-tuple across `group_by` keys.
                agg_rows.extend(compute_grouped_aggregate(
                    agg, rows, has_latency, gauge_names));
                continue;
            }

            // Single-key filter form: filter rows by
            // `<label_key>~<pattern>`, emit one aggregate row.
            let matching: Vec<&ActivityRow> = rows.iter()
                .filter(|r| {
                    // Look for key=value in the activity string where value contains pattern
                    for segment in r.activity.split(", ") {
                        if let Some((k, v)) = segment.split_once('=') {
                            if k.trim() == agg.label_key && v.trim().contains(&agg.label_pattern) {
                                return true;
                            }
                        }
                    }
                    false
                })
                .collect();

            let label = format!(
                "**{}({}) over {}~{}**",
                agg.function, agg.column_pattern, agg.label_key, agg.label_pattern,
            );

            let mut cells: Vec<String> = vec![
                label,
                "-".into(),  // Cycles
                "-".into(),  // Rate
            ];

            if has_latency {
                cells.extend(["-".into(), "-".into(), "-".into()]);
            }

            for gauge_name in gauge_names {
                if !gauge_name.contains(&agg.column_pattern) {
                    cells.push("-".into());
                    continue;
                }
                // Collect all values for this gauge across matching rows
                let values: Vec<f64> = matching.iter()
                    .filter_map(|r| {
                        r.gauges.iter()
                            .find(|(n, _)| n == gauge_name)
                            .map(|(_, v)| *v)
                    })
                    .collect();

                if values.is_empty() {
                    cells.push("-".into());
                } else {
                    let result = match agg.function.as_str() {
                        "mean" => values.iter().sum::<f64>() / values.len() as f64,
                        "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
                        "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                        _ => 0.0,
                    };
                    cells.push(format!("{result:.4}"));
                }
            }

            agg_rows.push(cells);
        }

        agg_rows
    }

    /// Multi-key grouping form: emit one aggregate row per
    /// distinct value-tuple across `agg.group_by` keys. The
    /// label column reads
    /// `**mean(recall) over k,limit,optimize_for [k=10, limit=20, optimize_for=RECALL]**`
    /// so the user can identify the group from the report.
    fn compute_grouped_aggregate(
        agg: &ReportAggregate,
        rows: &[ActivityRow],
        has_latency: bool,
        gauge_names: &[String],
    ) -> Vec<Vec<String>> {
        use std::collections::BTreeMap;
        let mut groups: BTreeMap<String, Vec<&ActivityRow>> = BTreeMap::new();
        for row in rows {
            let label_map: std::collections::HashMap<&str, &str> = row.activity
                .split(", ")
                .filter_map(|seg| seg.split_once('='))
                .map(|(k, v)| (k.trim(), v.trim()))
                .collect();
            let mut tuple_parts: Vec<String> = Vec::with_capacity(agg.group_by.len());
            let mut all_present = true;
            for key in &agg.group_by {
                match label_map.get(key.as_str()) {
                    Some(v) => tuple_parts.push(format!("{key}={v}")),
                    None => { all_present = false; break; }
                }
            }
            if !all_present { continue; }
            let tuple_key = tuple_parts.join(", ");
            groups.entry(tuple_key).or_default().push(row);
        }

        let mut out = Vec::new();
        let group_by_header = agg.group_by.join(",");
        for (tuple_key, group_rows) in groups {
            let label = format!(
                "**{}({}) over {} [{tuple_key}]**",
                agg.function, agg.column_pattern, group_by_header,
            );
            let mut cells: Vec<String> = vec![label, "-".into(), "-".into()];
            if has_latency { cells.extend(["-".into(), "-".into(), "-".into()]); }
            for gauge_name in gauge_names {
                if !gauge_name.contains(&agg.column_pattern) {
                    cells.push("-".into());
                    continue;
                }
                let values: Vec<f64> = group_rows.iter()
                    .filter_map(|r| {
                        r.gauges.iter().find(|(n, _)| n == gauge_name).map(|(_, v)| *v)
                    })
                    .collect();
                if values.is_empty() {
                    cells.push("-".into());
                } else {
                    let result = match agg.function.as_str() {
                        "mean" => values.iter().sum::<f64>() / values.len() as f64,
                        "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
                        "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                        _ => 0.0,
                    };
                    cells.push(format!("{result:.4}"));
                }
            }
            out.push(cells);
        }
        out
    }

    /// Extract all labels from a spec string into a display-friendly format.
    /// Skips session and n (sample count) — shows the meaningful dimensions.
    fn extract_labels_display(spec: &str) -> String {
        let labels_part = spec.split('{').nth(1)
            .and_then(|s| s.strip_suffix('}'))
            .unwrap_or("");
        let parts: Vec<&str> = labels_part.split(',')
            .filter(|p| !p.trim().starts_with("session=")
                && !p.trim().starts_with("n=")
                && !p.trim().starts_with("name=")
                && !p.trim().starts_with("nosummary="))
            .collect();
        parts.join(", ").replace('"', "")
    }

    /// Align label components within the Activity column (column 0).
    ///
    /// Each activity string is `"key=val, key=val, ..."`. This function
    /// discovers all distinct keys, orders them so that keys appearing
    /// in more rows sort first (ties broken alphabetically), computes
    /// the max `key=value` width for each key slot, and pads each row
    /// so that the same key starts at the same character position.
    fn align_activity_column(grid: &mut [Vec<String>]) {
        if grid.is_empty() { return; }

        // Parse each activity into (key, "key=value") pairs
        let parsed: Vec<Vec<(String, String)>> = grid.iter()
            .map(|row| {
                row[0].split(", ")
                    .filter_map(|seg| {
                        let key = seg.split('=').next().unwrap_or("").to_string();
                        if key.is_empty() { None }
                        else { Some((key, seg.to_string())) }
                    })
                    .collect()
            })
            .collect();

        // Discover all keys in component-tree order. Use the row with
        // the most segments as the canonical ordering — it has all the
        // nesting levels. Additional keys from other rows are appended.
        let mut all_keys: Vec<String> = Vec::new();
        let longest = parsed.iter().max_by_key(|r| r.len());
        if let Some(row) = longest {
            for (key, _) in row {
                if !all_keys.contains(key) {
                    all_keys.push(key.clone());
                }
            }
        }
        for row in &parsed {
            for (key, _) in row {
                if !all_keys.contains(key) {
                    all_keys.push(key.clone());
                }
            }
        }

        // Compute max width per key slot
        let mut slot_widths: Vec<usize> = vec![0; all_keys.len()];
        for row in &parsed {
            for (i, key) in all_keys.iter().enumerate() {
                if let Some((_, seg)) = row.iter().find(|(k, _)| k == key) {
                    let w = seg.chars().count();
                    if w > slot_widths[i] { slot_widths[i] = w; }
                }
            }
        }

        // Rebuild each activity string with aligned slots.
        // Each slot occupies a fixed width (segment + separator).
        // Absent keys become blank padding of the same width.
        let sep = ", ";
        let sep_len = sep.len();
        for (row_idx, row) in parsed.iter().enumerate() {
            let mut buf = String::new();
            for (i, key) in all_keys.iter().enumerate() {
                let is_last = i + 1 == all_keys.len();
                let total_w = slot_widths[i] + if is_last { 0 } else { sep_len };
                if let Some((_, seg)) = row.iter().find(|(k, _)| k == key) {
                    if is_last {
                        buf.push_str(&format!("{:<w$}", seg, w = slot_widths[i]));
                    } else {
                        // Pad segment + separator to fixed total width
                        let with_sep = format!("{}{}", seg, sep);
                        buf.push_str(&format!("{:<w$}", with_sep, w = total_w));
                    }
                } else {
                    buf.push_str(&" ".repeat(total_w));
                }
            }
            grid[row_idx][0] = buf.trim_end().to_string();
        }
    }

    /// Render a Markdown table from a (headers, grid) pair.
    /// Same output shape the in-run summary produced before
    /// formats were pluggable.
    fn render_markdown(headers: &[String], grid: &[Vec<String>]) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let ncols = headers.len();
        let mut widths: Vec<usize> = headers.iter()
            .map(|h| h.chars().count()).collect();
        for row in grid {
            for (i, cell) in row.iter().enumerate() {
                let w = cell.chars().count();
                if i < ncols && w > widths[i] {
                    widths[i] = w;
                }
            }
        }
        let _ = writeln!(out);
        let _ = writeln!(out, "## Summary");
        let _ = writeln!(out);

        let mut line = String::from("|");
        for (i, h) in headers.iter().enumerate() {
            let _ = write!(line, " {:<w$} |", h, w = widths[i]);
        }
        let _ = writeln!(out, "{line}");

        let mut sep = String::from("|");
        for w in &widths {
            let _ = write!(sep, "-{}-|", "-".repeat(*w));
        }
        let _ = writeln!(out, "{sep}");

        for row in grid {
            let mut line = String::from("|");
            for (i, cell) in row.iter().enumerate() {
                if i < ncols {
                    if i == 0 {
                        let _ = write!(line, " {:<w$} |", cell, w = widths[i]);
                    } else {
                        let _ = write!(line, " {:>w$} |", cell, w = widths[i]);
                    }
                }
            }
            let _ = writeln!(out, "{line}");
        }
        let _ = writeln!(out);
        out
    }

    /// Render a CSV file from a (headers, grid) pair (RFC 4180
    /// quoting). Same data the Markdown renderer sees, just
    /// machine-readable.
    fn render_csv(headers: &[String], grid: &[Vec<String>]) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        // Headers
        let row: Vec<String> = headers.iter().map(|h| csv_quote(h)).collect();
        let _ = writeln!(out, "{}", row.join(","));
        // Data + aggregate rows
        for row in grid {
            let cells: Vec<String> = row.iter().map(|c| csv_quote(c)).collect();
            let _ = writeln!(out, "{}", cells.join(","));
        }
        out
    }

    /// Quote a field for CSV per RFC 4180: wrap in `"..."` and
    /// double inner quotes when the field contains `,`, `"`,
    /// `\n`, or `\r`. Otherwise pass through.
    fn csv_quote(s: &str) -> String {
        if s.contains([',', '"', '\n', '\r']) {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s.to_string()
        }
    }

    impl Reporter for SqliteReporter {
        fn report(&mut self, snapshot: &MetricSet) {
            for family in snapshot.families() {
                for metric in family.metrics() {
                    self.insert_metric(snapshot, family, metric);
                }
            }
        }

        fn flush(&mut self) {
            // SQLite auto-commits
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::time::{Duration, Instant};

        fn build_activity_row(activity: &str, gauges: &[(&str, f64)]) -> super::ActivityRow {
            super::ActivityRow {
                activity: activity.to_string(),
                cycles: 0,
                rate: 0.0,
                latency_p50_ns: None,
                latency_p99_ns: None,
                latency_mean_ns: None,
                gauges: gauges.iter().map(|(n, v)| (n.to_string(), *v)).collect(),
            }
        }

        #[test]
        fn aggregate_single_key_filter_mean_of_three_profiles() {
            // Three rows, each a different profile, with recall@10
            // values 0.91 / 0.92 / 0.93. Single-key filter on
            // profile~label keeps all three; mean = 0.92 exactly.
            let rows = vec![
                build_activity_row("profile=label_01", &[("recall@10", 0.91)]),
                build_activity_row("profile=label_02", &[("recall@10", 0.92)]),
                build_activity_row("profile=label_03", &[("recall@10", 0.93)]),
            ];
            let agg = ReportAggregate {
                function: "mean".into(),
                column_pattern: "recall@10".into(),
                label_key: "profile".into(),
                label_pattern: "label".into(),
                group_by: Vec::new(),
            };
            let result = compute_aggregates(
                &[agg], &rows, false, &["recall@10".to_string()],
            );
            assert_eq!(result.len(), 1);
            // Cells: [label, "-", "-", "0.9200"] (no latency)
            assert_eq!(result[0][3], "0.9200");
        }

        #[test]
        fn aggregate_multi_key_grouping_emits_row_per_tuple() {
            // Six rows across two (k, optimize_for) tuples × three
            // profiles. Multi-key grouping should emit two rows:
            //   k=10, optimize_for=RECALL: mean of (0.90, 0.92, 0.94) = 0.92
            //   k=10, optimize_for=LATENCY: mean of (0.70, 0.74, 0.78) = 0.74
            let rows = vec![
                build_activity_row(
                    "k=10, optimize_for=RECALL, profile=label_01",
                    &[("recall@10", 0.90)]),
                build_activity_row(
                    "k=10, optimize_for=RECALL, profile=label_02",
                    &[("recall@10", 0.92)]),
                build_activity_row(
                    "k=10, optimize_for=RECALL, profile=label_03",
                    &[("recall@10", 0.94)]),
                build_activity_row(
                    "k=10, optimize_for=LATENCY, profile=label_01",
                    &[("recall@10", 0.70)]),
                build_activity_row(
                    "k=10, optimize_for=LATENCY, profile=label_02",
                    &[("recall@10", 0.74)]),
                build_activity_row(
                    "k=10, optimize_for=LATENCY, profile=label_03",
                    &[("recall@10", 0.78)]),
            ];
            let agg = ReportAggregate {
                function: "mean".into(),
                column_pattern: "recall@10".into(),
                label_key: String::new(),
                label_pattern: String::new(),
                group_by: vec!["k".into(), "optimize_for".into()],
            };
            let result = compute_aggregates(
                &[agg], &rows, false, &["recall@10".to_string()],
            );
            assert_eq!(result.len(), 2);
            // BTreeMap orders alphabetically by tuple key —
            // "k=10, optimize_for=LATENCY" sorts before
            // "k=10, optimize_for=RECALL".
            assert_eq!(result[0][3], "0.7400",
                "LATENCY group mean (0.70+0.74+0.78)/3 ≠ 0.74");
            assert_eq!(result[1][3], "0.9200",
                "RECALL group mean (0.90+0.92+0.94)/3 ≠ 0.92");
        }

        #[test]
        fn aggregate_multi_key_min_picks_lowest_per_group() {
            let rows = vec![
                build_activity_row("k=10, opt=A", &[("g", 0.9)]),
                build_activity_row("k=10, opt=A", &[("g", 0.5)]),
                build_activity_row("k=20, opt=A", &[("g", 0.7)]),
                build_activity_row("k=20, opt=A", &[("g", 0.3)]),
            ];
            let agg = ReportAggregate {
                function: "min".into(),
                column_pattern: "g".into(),
                label_key: String::new(),
                label_pattern: String::new(),
                group_by: vec!["k".into()],
            };
            let result = compute_aggregates(
                &[agg], &rows, false, &["g".to_string()],
            );
            assert_eq!(result.len(), 2);
            assert_eq!(result[0][3], "0.5000", "k=10 min ≠ 0.5");
            assert_eq!(result[1][3], "0.3000", "k=20 min ≠ 0.3");
        }

        #[test]
        fn aggregate_multi_key_max_picks_highest_per_group() {
            let rows = vec![
                build_activity_row("k=10", &[("g", 0.5)]),
                build_activity_row("k=10", &[("g", 0.7)]),
                build_activity_row("k=20", &[("g", 0.6)]),
                build_activity_row("k=20", &[("g", 0.9)]),
            ];
            let agg = ReportAggregate {
                function: "max".into(),
                column_pattern: "g".into(),
                label_key: String::new(),
                label_pattern: String::new(),
                group_by: vec!["k".into()],
            };
            let result = compute_aggregates(
                &[agg], &rows, false, &["g".to_string()],
            );
            assert_eq!(result.len(), 2);
            assert_eq!(result[0][3], "0.7000", "k=10 max ≠ 0.7");
            assert_eq!(result[1][3], "0.9000", "k=20 max ≠ 0.9");
        }

        #[test]
        fn aggregate_multi_key_skips_rows_missing_group_label() {
            // Row missing the `optimize_for` label is excluded
            // from groups (rather than silently grouping it with
            // a different tuple).
            let rows = vec![
                build_activity_row("k=10, optimize_for=RECALL",
                    &[("g", 0.9)]),
                build_activity_row("k=10",  // missing optimize_for
                    &[("g", 0.5)]),
            ];
            let agg = ReportAggregate {
                function: "mean".into(),
                column_pattern: "g".into(),
                label_key: String::new(),
                label_pattern: String::new(),
                group_by: vec!["k".into(), "optimize_for".into()],
            };
            let result = compute_aggregates(
                &[agg], &rows, false, &["g".to_string()],
            );
            assert_eq!(result.len(), 1, "row missing group label was excluded");
            assert_eq!(result[0][3], "0.9000");
        }

        #[test]
        fn sqlite_creates_schema() {
            let reporter = SqliteReporter::in_memory().unwrap();
            let count: i64 = reporter.conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [], |row| row.get(0),
            ).unwrap();
            assert!(count >= 7, "expected 7+ tables, got {count}");
        }

        #[test]
        fn sqlite_inserts_counter() {
            let mut reporter = SqliteReporter::in_memory().unwrap();
            let mut snapshot = MetricSet::new(Duration::from_secs(1));
            snapshot.insert_counter(
                "ops_total",
                Labels::of("activity", "write"),
                42,
                Instant::now(),
            );
            reporter.report(&snapshot);

            let count: i64 = reporter.conn.query_row(
                "SELECT COUNT(*) FROM sample_value", [], |row| row.get(0),
            ).unwrap();
            assert_eq!(count, 1);
        }

        #[test]
        fn sqlite_inserts_timer() {
            let mut reporter = SqliteReporter::in_memory().unwrap();
            let mut h = hdrhistogram::Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
            for i in 1..=100 { let _ = h.record(i * 1_000_000); }

            let mut snapshot = MetricSet::new(Duration::from_secs(1));
            snapshot.insert_histogram(
                "latency",
                Labels::of("activity", "read"),
                h,
                Instant::now(),
            );
            reporter.report(&snapshot);

            let p99: f64 = reporter.conn.query_row(
                "SELECT p99 FROM sample_value", [], |row| row.get(0),
            ).unwrap();
            assert!(p99 > 0.0, "p99 should be recorded");
        }

        #[test]
        fn sqlite_deduplicates_families() {
            let mut reporter = SqliteReporter::in_memory().unwrap();
            let mut snapshot = MetricSet::new(Duration::from_secs(1));
            snapshot.insert_counter("ops", Labels::of("activity", "a"), 1, Instant::now());
            snapshot.insert_counter("ops", Labels::of("activity", "b"), 2, Instant::now());
            reporter.report(&snapshot);

            let families: i64 = reporter.conn.query_row(
                "SELECT COUNT(*) FROM metric_family", [], |row| row.get(0),
            ).unwrap();
            assert_eq!(families, 1, "same metric name should be one family");

            let instances: i64 = reporter.conn.query_row(
                "SELECT COUNT(*) FROM metric_instance", [], |row| row.get(0),
            ).unwrap();
            assert_eq!(instances, 2, "different labels should be different instances");
        }

        /// Visual test: prints a summary table to stderr so you can
        /// verify column alignment. Run with `--nocapture`.
        #[test]
        fn sqlite_summary_alignment() {
            let mut r = SqliteReporter::in_memory().unwrap();
            let now = Instant::now();
            let interval = Duration::from_secs(1);

            // Helper: insert a counter + timer for a phase
            let mut inject = |labels: Labels, cycles: u64, mean_ns: f64| {
                let mut h = hdrhistogram::Histogram::new_with_bounds(
                    1, 3_600_000_000_000, 3).unwrap();
                let _ = h.record(mean_ns as u64);
                let mut snapshot = MetricSet::new(interval);
                snapshot.insert_counter("cycles_total", labels.clone(), cycles, now);
                snapshot.insert_histogram("cycles_servicetime", labels.clone(), h, now);
                r.report(&snapshot);
            };

            let rampup = Labels::of("session", "test")
                .with("profile", "label_00").with("phase", "rampup");
            inject(rampup, 82993, 146_000_000.0);

            let search_k10 = Labels::of("session", "test")
                .with("profile", "label_00").with("k", "10")
                .with("phase", "search_pre_compaction");
            inject(search_k10, 100, 3_740_000.0);

            let search_k100_pre = Labels::of("session", "test")
                .with("profile", "label_00").with("k", "100")
                .with("phase", "search_pre_compaction");
            inject(search_k100_pre, 100, 17_940_000.0);

            let await_idx = Labels::of("session", "test")
                .with("profile", "label_00").with("phase", "await_index");
            inject(await_idx, 1, 550_000.0);

            let search_k10_post = Labels::of("session", "test")
                .with("profile", "label_00").with("k", "10")
                .with("phase", "search_post_compaction");
            inject(search_k10_post, 100, 4_550_000.0);

            let search_k100_post = Labels::of("session", "test")
                .with("profile", "label_00").with("k", "100")
                .with("phase", "search_post_compaction");
            inject(search_k100_post, 100, 17_580_000.0);

            // Gauges: recall for all search phases
            let mut gauges = MetricSet::new(interval);
            gauges.insert_gauge("recall@10.mean",
                Labels::of("session", "test").with("profile", "label_00").with("k", "10")
                    .with("phase", "search_pre_compaction").with("n", "100"),
                0.8410, now);
            gauges.insert_gauge("recall@100.mean",
                Labels::of("session", "test").with("profile", "label_00").with("k", "100")
                    .with("phase", "search_pre_compaction").with("n", "100"),
                0.9837, now);
            gauges.insert_gauge("recall@10.mean",
                Labels::of("session", "test").with("profile", "label_00").with("k", "10")
                    .with("phase", "search_post_compaction").with("n", "100"),
                0.8410, now);
            gauges.insert_gauge("recall@100.mean",
                Labels::of("session", "test").with("profile", "label_00").with("k", "100")
                    .with("phase", "search_post_compaction").with("n", "100"),
                0.9837, now);
            r.report(&gauges);

            eprintln!("--- summary output (all columns, no aggregates) ---");
            let config = ReportConfig {
                columns: vec![],
                row_filters: vec![],
                aggregates: vec![],
                show_details: true,
            };
            r.print_summary(&config);

            eprintln!("--- summary with aggregate ---");
            let config_agg = ReportConfig {
                columns: vec!["recall".into()],
                row_filters: vec![],
                aggregates: vec![ReportAggregate {
                    function: "mean".into(),
                    column_pattern: "recall".into(),
                    label_key: "profile".into(),
                    label_pattern: "label".into(),
                    group_by: Vec::new(),
                }],
                show_details: true,
            };
            r.print_summary(&config_agg);
            eprintln!("--- end ---");
        }
    }
}

#[cfg(feature = "sqlite")]
pub use inner::SqliteReporter;
#[cfg(feature = "sqlite")]
pub use inner::{ReportConfig, ReportAggregate};

/// Split a summary name into `(basename, format)`.
///
/// Names without an extension default to Markdown:
///
/// - `recall`         → `("recall", "md")`
/// - `recallnmore`    → `("recallnmore", "md")`
///
/// Names with a recognized extension select the format from
/// the suffix:
///
/// - `recallnmore.csv` → `("recallnmore", "csv")`
/// - `recall.md`       → `("recall", "md")`
///
/// Output filenames combine the two as `{basename}_summary.{format}`,
/// so all three of the above produce filenames matching the
/// user's desired shape (`recall_summary.md`, etc.).
///
/// Unrecognized extensions fall through to Markdown — better to
/// produce something than to panic on an unknown suffix.
pub fn derive_name_and_format(name: &str) -> (String, String) {
    if let Some(idx) = name.rfind('.') {
        let suffix = &name[idx + 1..];
        if matches!(suffix, "md" | "csv") {
            return (name[..idx].to_string(), suffix.to_string());
        }
    }
    (name.to_string(), "md".to_string())
}
