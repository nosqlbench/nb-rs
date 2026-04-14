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

    use crate::frame::{MetricsFrame, Sample, QUANTILES};
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
            let timestamp_ms = frame.captured_at.elapsed().as_millis() as i64;
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
                Sample::Timer { labels, count, histogram } => {
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

    impl SqliteReporter {
        /// Print a markdown summary of relevancy metrics (recall, precision).
        pub fn print_summary(&self) {
            let mut stmt = match self.conn.prepare(
                "SELECT mi.spec, sv.mean
                 FROM sample_value sv
                 JOIN metric_instance mi ON sv.instance_id = mi.id
                 WHERE mi.spec LIKE 'recall%.mean%' OR mi.spec LIKE 'precision%.mean%'
                 ORDER BY mi.spec"
            ) {
                Ok(s) => s,
                Err(_) => return,
            };

            let rows: Vec<(String, f64)> = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
            }).ok()
                .map(|r| r.filter_map(|r| r.ok()).collect())
                .unwrap_or_default();

            if rows.is_empty() { return; }

            println!();
            println!("## Summary");
            println!();
            println!("| Metric | Activity | Score |");
            println!("|--------|----------|-------|");
            for (spec, value) in &rows {
                // Parse spec: "recall@100.mean{activity="search (k=100)",n="100"}"
                let metric = spec.split('{').next().unwrap_or(spec);
                let activity = spec.split("activity=\"")
                    .nth(1)
                    .and_then(|s| s.split('"').next())
                    .unwrap_or("");
                println!("| {metric} | {activity} | {value:.4} |");
            }
            println!();
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
    }
}

#[cfg(feature = "sqlite")]
pub use inner::SqliteReporter;
