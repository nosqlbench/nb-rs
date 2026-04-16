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

    use crate::frame::{MetricsFrame, Sample};
    use crate::labels::Labels;
    use crate::scheduler::Reporter;

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

        /// Store a session metadata key-value pair.
        pub fn set_metadata(&mut self, key: &str, value: &str) {
            self.conn.execute(
                "INSERT OR REPLACE INTO session_metadata (key, value) VALUES (?1, ?2)",
                params![key, value],
            ).unwrap_or_else(|e| { eprintln!("warning: sqlite metadata write: {e}"); 0 });
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
                );"
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
            ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
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
            ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
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
            ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
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
            ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
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
                ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
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
            ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
            let id: i64 = self.conn.query_row(
                "SELECT id FROM metric_instance WHERE family_id=?1 AND label_set_id=?2",
                params![family_id, label_set_id], |row| row.get(0),
            ).unwrap_or(0);
            self.instance_cache.insert(key, id);
            id
        }

        fn insert_sample(&mut self, frame: &MetricsFrame, sample: &Sample) {
            let _timestamp_ms = frame.captured_at.elapsed().as_millis() as i64;
            // Use wall clock approximation
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let interval_ms = frame.interval.as_millis() as i64;

            match sample {
                Sample::Counter { labels, value } => {
                    let name = labels.get("name").unwrap_or("unknown");
                    let family_id = self.get_or_insert_family(name, "counter");
                    let label_set_id = self.get_or_insert_label_set(labels);
                    let instance_id = self.get_or_insert_instance(family_id, label_set_id, name, labels);

                    self.conn.execute(
                        "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, count) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params![instance_id, now_ms, interval_ms, *value as i64],
                    ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
                }
                Sample::Gauge { labels, value } => {
                    let name = labels.get("name").unwrap_or("unknown");
                    let family_id = self.get_or_insert_family(name, "gauge");
                    let label_set_id = self.get_or_insert_label_set(labels);
                    let instance_id = self.get_or_insert_instance(family_id, label_set_id, name, labels);

                    self.conn.execute(
                        "INSERT INTO sample_value (instance_id, timestamp_ms, interval_ms, mean) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params![instance_id, now_ms, interval_ms, *value],
                    ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
                }
                Sample::Timer { labels, count: _, histogram } => {
                    let name = labels.get("name").unwrap_or("unknown");
                    let family_id = self.get_or_insert_family(name, "summary");
                    let label_set_id = self.get_or_insert_label_set(labels);
                    let instance_id = self.get_or_insert_instance(family_id, label_set_id, name, labels);

                    let obs = histogram.len() as i64;
                    let min = histogram.min() as f64;
                    let max = histogram.max() as f64;
                    let mean = histogram.mean();
                    let stddev = histogram.stdev();
                    let sum = mean * obs as f64;

                    let p50 = histogram.value_at_quantile(0.50) as f64;
                    let p75 = histogram.value_at_quantile(0.75) as f64;
                    let p90 = histogram.value_at_quantile(0.90) as f64;
                    let p95 = histogram.value_at_quantile(0.95) as f64;
                    let p98 = histogram.value_at_quantile(0.98) as f64;
                    let p99 = histogram.value_at_quantile(0.99) as f64;
                    let p999 = histogram.value_at_quantile(0.999) as f64;

                    self.conn.execute(
                        "INSERT INTO sample_value \
                         (instance_id, timestamp_ms, interval_ms, count, sum, min, max, mean, stddev, \
                          p50, p75, p90, p95, p98, p99, p999) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                        params![
                            instance_id, now_ms, interval_ms, obs, sum, min, max, mean, stddev,
                            p50, p75, p90, p95, p98, p99, p999
                        ],
                    ).unwrap_or_else(|e| { eprintln!("warning: sqlite write failed: {e}"); 0 });
                }
            }
        }
    }

    /// Configuration for the summary report, passed from the runner.
    ///
    /// This is the nb-metrics–local mirror of the workload-level
    /// `SummaryConfig`. The runner converts one to the other so that
    /// nb-metrics does not depend on nb-workload.
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

    /// An aggregate expression for the summary report.
    pub struct ReportAggregate {
        /// Function name: `"mean"`, `"min"`, or `"max"`.
        pub function: String,
        /// Column name pattern — only matching gauge columns are aggregated.
        pub column_pattern: String,
        /// Label key to filter rows on (e.g., `"profile"`).
        pub label_key: String,
        /// Substring pattern for the label value (e.g., `"label"`).
        pub label_pattern: String,
    }

    impl SqliteReporter {
        /// Print a data-driven summary of all metrics collected in this session.
        ///
        /// One row per distinct label set that has `cycles_total > 0`.
        /// Columns are discovered from the metrics that exist:
        /// - cycles and rate are always shown
        /// - latency columns appear when `cycles_servicetime` data exists
        /// - gauge columns appear when gauge data exists
        ///
        /// The `config` controls column filters, row filters, aggregate
        /// expressions, and whether detail rows are shown.
        pub fn print_summary(&self, config: &ReportConfig) {
            let row_patterns: Vec<regex::Regex> = config.row_filters.iter()
                .filter_map(|p| regex::Regex::new(p.trim()).ok())
                .collect();

            let rows = self.query_all_activities();
            if rows.is_empty() { return; }

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

            if grid.is_empty() && agg_rows.is_empty() { return; }

            // Align label components within the Activity column (data rows only).
            align_activity_column(&mut grid);

            // Append aggregate rows after a blank separator
            if !agg_rows.is_empty() && !grid.is_empty() {
                let blank: Vec<String> = (0..headers.len()).map(|_| String::new()).collect();
                grid.push(blank);
            }
            grid.extend(agg_rows);

            // Compute column widths using char count (not byte length)
            let ncols = headers.len();
            let mut widths: Vec<usize> = headers.iter()
                .map(|h| h.chars().count()).collect();
            for row in &grid {
                for (i, cell) in row.iter().enumerate() {
                    let w = cell.chars().count();
                    if i < ncols && w > widths[i] {
                        widths[i] = w;
                    }
                }
            }

            // Print column-aligned markdown table
            println!();
            println!("## Summary");
            println!();

            // Header row
            let mut line = String::from("|");
            for (i, h) in headers.iter().enumerate() {
                line.push_str(&format!(" {:<w$} |", h, w = widths[i]));
            }
            println!("{line}");

            // Separator row
            let mut sep = String::from("|");
            for w in &widths {
                sep.push_str(&format!("-{}-|", "-".repeat(*w)));
            }
            println!("{sep}");

            // Data + aggregate rows
            for row in &grid {
                let mut line = String::from("|");
                for (i, cell) in row.iter().enumerate() {
                    if i < ncols {
                        if i == 0 {
                            line.push_str(&format!(" {:<w$} |", cell, w = widths[i]));
                        } else {
                            line.push_str(&format!(" {:>w$} |", cell, w = widths[i]));
                        }
                    }
                }
                println!("{line}");
            }
            println!();
        }

        /// Query all activities that produced data, returning one row per
        /// distinct label set. No hardcoded phase name patterns — the
        /// summary is projected directly from whatever the workload produced.
        fn query_all_activities(&self) -> Vec<ActivityRow> {
            // Find every distinct label set that has cycles_total > 0.
            // Phases tagged nosummary="true" are excluded.
            let mut stmt = match self.conn.prepare(
                "SELECT mi.spec, MAX(sv.count)
                 FROM sample_value sv
                 JOIN metric_instance mi ON sv.instance_id = mi.id
                 WHERE mi.spec LIKE 'cycles_total%'
                   AND mi.spec NOT LIKE '%nosummary=%'
                 GROUP BY mi.id
                 HAVING MAX(sv.count) > 0
                 ORDER BY mi.id"
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };

            let mut rows = Vec::new();
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

                    // Latency from cycles_servicetime (if this activity has it)
                    let latency = self.query_latency(labels);

                    // All gauges for this label set (relevancy, etc.)
                    let gauges = self.query_gauges_for_labels(labels);

                    rows.push(ActivityRow {
                        activity: display,
                        cycles,
                        rate,
                        latency_p50_ns: latency.map(|l| l.0),
                        latency_p99_ns: latency.map(|l| l.1),
                        latency_mean_ns: latency.map(|l| l.2),
                        gauges,
                    });
                }
            }
            rows
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
        fn query_elapsed_ms(&self, label_part: &str) -> f64 {
            let pattern = format!("%{{{label_part}}}");
            let result: Result<(i64, i64), _> = self.conn.query_row(
                "SELECT MIN(sv.timestamp_ms), MAX(sv.timestamp_ms)
                 FROM sample_value sv
                 JOIN metric_instance mi ON sv.instance_id = mi.id
                 WHERE mi.spec LIKE ?1",
                params![pattern],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );
            match result {
                Ok((min, max)) => (max - min) as f64,
                Err(_) => 0.0,
            }
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
            // Filter rows where the activity contains key=...pattern...
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

    impl Reporter for SqliteReporter {
        fn report(&mut self, frame: &MetricsFrame) {
            for sample in &frame.samples {
                self.insert_sample(frame, sample);
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
            let frame = MetricsFrame {
                captured_at: Instant::now(),
                interval: Duration::from_secs(1),
                samples: vec![Sample::Counter {
                    labels: Labels::of("name", "ops_total").with("activity", "write"),
                    value: 42,
                }],
            };
            reporter.report(&frame);

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

            let frame = MetricsFrame {
                captured_at: Instant::now(),
                interval: Duration::from_secs(1),
                samples: vec![Sample::Timer {
                    labels: Labels::of("name", "latency").with("activity", "read"),
                    count: 100,
                    histogram: h,
                }],
            };
            reporter.report(&frame);

            let p99: f64 = reporter.conn.query_row(
                "SELECT p99 FROM sample_value", [], |row| row.get(0),
            ).unwrap();
            assert!(p99 > 0.0, "p99 should be recorded");
        }

        #[test]
        fn sqlite_deduplicates_families() {
            let mut reporter = SqliteReporter::in_memory().unwrap();
            let frame = MetricsFrame {
                captured_at: Instant::now(),
                interval: Duration::from_secs(1),
                samples: vec![
                    Sample::Counter {
                        labels: Labels::of("name", "ops").with("activity", "a"),
                        value: 1,
                    },
                    Sample::Counter {
                        labels: Labels::of("name", "ops").with("activity", "b"),
                        value: 2,
                    },
                ],
            };
            reporter.report(&frame);

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
                r.report(&MetricsFrame {
                    captured_at: now,
                    interval,
                    samples: vec![
                        Sample::Counter {
                            labels: labels.with("name", "cycles_total"),
                            value: cycles,
                        },
                        Sample::Timer {
                            labels: labels.with("name", "cycles_servicetime"),
                            count: cycles,
                            histogram: h,
                        },
                    ],
                });
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
            r.report(&MetricsFrame {
                captured_at: now,
                interval,
                samples: vec![
                    Sample::Gauge {
                        labels: Labels::of("session", "test")
                            .with("profile", "label_00").with("k", "10")
                            .with("phase", "search_pre_compaction")
                            .with("name", "recall@10.mean").with("n", "100"),
                        value: 0.8410,
                    },
                    Sample::Gauge {
                        labels: Labels::of("session", "test")
                            .with("profile", "label_00").with("k", "100")
                            .with("phase", "search_pre_compaction")
                            .with("name", "recall@100.mean").with("n", "100"),
                        value: 0.9837,
                    },
                    Sample::Gauge {
                        labels: Labels::of("session", "test")
                            .with("profile", "label_00").with("k", "10")
                            .with("phase", "search_post_compaction")
                            .with("name", "recall@10.mean").with("n", "100"),
                        value: 0.8410,
                    },
                    Sample::Gauge {
                        labels: Labels::of("session", "test")
                            .with("profile", "label_00").with("k", "100")
                            .with("phase", "search_post_compaction")
                            .with("name", "recall@100.mean").with("n", "100"),
                        value: 0.9837,
                    },
                ],
            });

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
