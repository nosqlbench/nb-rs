// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Metrics state that the TUI renders from.
//!
//! This is updated by the metrics frame consumer and read by the
//! TUI render loop. For now it holds simple scalar values; future
//! versions will hold time-series for sparklines.

/// Snapshot of metrics state for TUI rendering.
pub struct MetricsState {
    // Counters
    pub total_cycles: u64,
    pub total_errors: u64,
    pub total_target: u64,

    // Rates
    pub ops_per_sec: f64,

    // Latency percentiles (nanoseconds)
    pub p50_nanos: u64,
    pub p90_nanos: u64,
    pub p99_nanos: u64,
    pub p999_nanos: u64,
    pub max_nanos: u64,

    // Activity info
    pub activity_name: String,
    pub driver_name: String,
    pub threads: usize,
    pub stanza_length: usize,
    pub rate_config: String,

    // Timing
    pub elapsed_secs: f64,
    pub start: std::time::Instant,
}

impl MetricsState {
    pub fn new() -> Self {
        Self {
            total_cycles: 0,
            total_errors: 0,
            total_target: 0,
            ops_per_sec: 0.0,
            p50_nanos: 0,
            p90_nanos: 0,
            p99_nanos: 0,
            p999_nanos: 0,
            max_nanos: 0,
            activity_name: "none".into(),
            driver_name: "none".into(),
            threads: 0,
            stanza_length: 0,
            rate_config: "unlimited".into(),
            elapsed_secs: 0.0,
            start: std::time::Instant::now(),
        }
    }

    /// Called every tick to update derived values.
    pub fn tick(&mut self) {
        self.elapsed_secs = self.start.elapsed().as_secs_f64();
        if self.elapsed_secs > 0.0 {
            self.ops_per_sec = self.total_cycles as f64 / self.elapsed_secs;
        }
    }

    /// Update from a metrics frame snapshot.
    pub fn update_from_frame(&mut self, frame: &nb_metrics::frame::MetricsFrame) {
        for sample in &frame.samples {
            match sample {
                nb_metrics::frame::Sample::Counter { labels, value } => {
                    match labels.get("name") {
                        Some("cycles_total") => self.total_cycles = *value,
                        Some("errors_total") => self.total_errors = *value,
                        _ => {}
                    }
                }
                nb_metrics::frame::Sample::Timer { labels, histogram, .. } => {
                    if labels.get("name") == Some("cycles_servicetime") {
                        self.p50_nanos = histogram.value_at_quantile(0.50);
                        self.p90_nanos = histogram.value_at_quantile(0.90);
                        self.p99_nanos = histogram.value_at_quantile(0.99);
                        self.p999_nanos = histogram.value_at_quantile(0.999);
                        self.max_nanos = histogram.max();
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_state_defaults() {
        let state = MetricsState::new();
        assert_eq!(state.total_cycles, 0);
        assert_eq!(state.ops_per_sec, 0.0);
        assert_eq!(state.activity_name, "none");
    }

    #[test]
    fn metrics_state_tick_computes_rate() {
        let mut state = MetricsState::new();
        state.total_cycles = 1000;
        // Simulate some elapsed time
        std::thread::sleep(std::time::Duration::from_millis(10));
        state.tick();
        assert!(state.ops_per_sec > 0.0);
        assert!(state.elapsed_secs > 0.0);
    }
}
