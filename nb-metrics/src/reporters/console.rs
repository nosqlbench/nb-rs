// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Console reporter: human-readable delta rates and percentiles.

use std::io::Write;

use crate::frame::{MetricsFrame, Sample, QUANTILES};
use crate::scheduler::Reporter;

/// Writes human-readable metrics summaries to a `Write` target.
pub struct ConsoleReporter {
    out: Box<dyn Write + Send>,
    /// Previous counter values for delta rate computation.
    prev_counts: std::collections::HashMap<u64, u64>,
}

impl ConsoleReporter {
    pub fn stdout() -> Self {
        Self { out: Box::new(std::io::stdout()), prev_counts: Default::default() }
    }

    pub fn stderr() -> Self {
        Self { out: Box::new(std::io::stderr()), prev_counts: Default::default() }
    }

    pub fn new(out: Box<dyn Write + Send>) -> Self {
        Self { out, prev_counts: Default::default() }
    }

    fn format_nanos(nanos: u64) -> String {
        if nanos < 1_000 {
            format!("{nanos}ns")
        } else if nanos < 1_000_000 {
            format!("{:.1}µs", nanos as f64 / 1_000.0)
        } else if nanos < 1_000_000_000 {
            format!("{:.1}ms", nanos as f64 / 1_000_000.0)
        } else {
            format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
        }
    }
}

impl Reporter for ConsoleReporter {
    fn report(&mut self, frame: &MetricsFrame) {
        let interval_secs = frame.interval.as_secs_f64();
        if interval_secs <= 0.0 { return; }

        let _ = writeln!(self.out);

        // Group samples by activity label
        let mut by_activity: std::collections::BTreeMap<String, Vec<&Sample>> =
            std::collections::BTreeMap::new();
        for sample in &frame.samples {
            let activity = sample.labels().get("activity")
                .unwrap_or("global")
                .to_string();
            by_activity.entry(activity).or_default().push(sample);
        }

        for (activity, samples) in &by_activity {
            let _ = writeln!(self.out, "── {activity} ({:.1}s) ──────────────────",
                interval_secs);

            for sample in samples {
                let name = sample.labels().get("name").unwrap_or("?");
                match sample {
                    Sample::Counter { labels, value } => {
                        let key = labels.identity_hash();
                        let prev = self.prev_counts.get(&key).copied().unwrap_or(0);
                        let delta = value.saturating_sub(prev);
                        let rate = delta as f64 / interval_secs;
                        self.prev_counts.insert(key, *value);

                        if delta > 0 || *value > 0 {
                            let _ = writeln!(self.out,
                                "  {name}  count={value}  delta={delta}  rate={rate:.0}/s");
                        }
                    }
                    Sample::Timer { labels: _, count: _, histogram } => {
                        let obs = histogram.len();
                        if obs == 0 { continue; }
                        let rate = obs as f64 / interval_secs;

                        let _ = writeln!(self.out,
                            "  {name}  count={obs}  rate={rate:.0}/s");

                        let p50 = Self::format_nanos(histogram.value_at_quantile(0.50));
                        let p90 = Self::format_nanos(histogram.value_at_quantile(0.90));
                        let p99 = Self::format_nanos(histogram.value_at_quantile(0.99));
                        let p999 = Self::format_nanos(histogram.value_at_quantile(0.999));
                        let max = Self::format_nanos(histogram.max());

                        let _ = writeln!(self.out,
                            "    p50={p50}  p90={p90}  p99={p99}  p999={p999}  max={max}");
                    }
                    Sample::Gauge { labels: _, value } => {
                        let _ = writeln!(self.out, "  {name}  {value:.2}");
                    }
                }
            }
        }

        let _ = self.out.flush();
    }

    fn flush(&mut self) {
        let _ = self.out.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Labels;
    use std::time::{Duration, Instant};

    #[test]
    fn format_nanos_ranges() {
        assert_eq!(ConsoleReporter::format_nanos(500), "500ns");
        assert_eq!(ConsoleReporter::format_nanos(1_500), "1.5µs");
        assert_eq!(ConsoleReporter::format_nanos(1_500_000), "1.5ms");
        assert_eq!(ConsoleReporter::format_nanos(1_500_000_000), "1.50s");
    }
}
