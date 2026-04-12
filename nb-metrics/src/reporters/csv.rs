// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CSV metrics reporter: one file per metric, timestamp + stats columns.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::frame::{MetricsFrame, Sample};
use crate::scheduler::Reporter;

struct CsvFile {
    file: File,
    header_written: bool,
}

pub struct CsvReporter {
    dir: PathBuf,
    files: HashMap<String, CsvFile>,
}

impl CsvReporter {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, String> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .map_err(|e| format!("failed to create CSV dir {:?}: {e}", dir))?;
        Ok(Self { dir, files: HashMap::new() })
    }

    fn ensure_file(&mut self, name: &str, header: &str) -> &mut File {
        let entry = self.files.entry(name.to_string()).or_insert_with(|| {
            let path = self.dir.join(format!("{name}.csv"));
            let file = File::create(&path)
                .unwrap_or_else(|e| panic!("failed to create CSV {:?}: {e}", path));
            CsvFile { file, header_written: false }
        });
        if !entry.header_written {
            if let Err(e) = writeln!(entry.file, "{header}") {
                eprintln!("warning: CSV header write failed for {name}: {e}");
            }
            entry.header_written = true;
        }
        &mut entry.file
    }
}

impl Reporter for CsvReporter {
    fn report(&mut self, frame: &MetricsFrame) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        for sample in &frame.samples {
            let name = sample.labels().get("name").unwrap_or("unknown").to_string();
            match sample {
                Sample::Counter { value, .. } => {
                    let file = self.ensure_file(&name, "timestamp_ms,count");
                    if let Err(e) = writeln!(file, "{now_ms},{value}") {
                        eprintln!("warning: CSV write failed for {name}: {e}");
                    }
                }
                Sample::Gauge { value, .. } => {
                    let file = self.ensure_file(&name, "timestamp_ms,value");
                    if let Err(e) = writeln!(file, "{now_ms},{value}") {
                        eprintln!("warning: CSV write failed for {name}: {e}");
                    }
                }
                Sample::Timer { histogram, .. } => {
                    let file = self.ensure_file(&name,
                        "timestamp_ms,count,min,max,mean,stddev,p50,p75,p90,p95,p98,p99,p999");
                    if let Err(e) = writeln!(file, "{},{},{},{},{},{},{},{},{},{},{},{},{}",
                        now_ms, histogram.len(),
                        histogram.min(), histogram.max(),
                        histogram.mean(), histogram.stdev(),
                        histogram.value_at_quantile(0.50),
                        histogram.value_at_quantile(0.75),
                        histogram.value_at_quantile(0.90),
                        histogram.value_at_quantile(0.95),
                        histogram.value_at_quantile(0.98),
                        histogram.value_at_quantile(0.99),
                        histogram.value_at_quantile(0.999),
                    ) {
                        eprintln!("warning: CSV write failed for {name}: {e}");
                    }
                }
            }
        }
    }

    fn flush(&mut self) {
        for cf in self.files.values_mut() {
            if let Err(e) = cf.file.flush() {
                eprintln!("warning: CSV flush failed: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Labels;
    use std::time::{Duration, Instant};

    #[test]
    fn csv_writes_files() {
        let dir = std::env::temp_dir().join("nb_metrics_csv_test");
        let _ = fs::remove_dir_all(&dir);

        let mut reporter = CsvReporter::new(&dir).unwrap();
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![
                Sample::Counter {
                    labels: Labels::of("name", "ops_total"),
                    value: 42,
                },
                Sample::Gauge {
                    labels: Labels::of("name", "heap_mb"),
                    value: 256.5,
                },
            ],
        };
        reporter.report(&frame);
        reporter.flush();

        assert!(dir.join("ops_total.csv").exists());
        assert!(dir.join("heap_mb.csv").exists());

        let content = fs::read_to_string(dir.join("ops_total.csv")).unwrap();
        assert!(content.contains("timestamp_ms,count"));
        assert!(content.contains(",42"));

        let _ = fs::remove_dir_all(&dir);
    }
}
