// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Metrics frame: immutable snapshot of all instruments for one interval.
//!
//! Frames are captured atomically (single pass) and coalesced for
//! reporters at coarser intervals via HDR histogram merging.

use std::time::{Duration, Instant};
use hdrhistogram::Histogram as HdrHistogram;
use crate::labels::Labels;

/// An immutable snapshot of all metrics for one capture interval.
#[derive(Clone)]
pub struct MetricsFrame {
    pub captured_at: Instant,
    pub interval: Duration,
    pub samples: Vec<Sample>,
}

/// A single metric sample within a frame.
#[derive(Clone)]
pub enum Sample {
    Counter {
        labels: Labels,
        value: u64,
    },
    Gauge {
        labels: Labels,
        value: f64,
    },
    Timer {
        labels: Labels,
        count: u64,
        histogram: HdrHistogram<u64>,
    },
}

impl Sample {
    pub fn labels(&self) -> &Labels {
        match self {
            Sample::Counter { labels, .. } => labels,
            Sample::Gauge { labels, .. } => labels,
            Sample::Timer { labels, .. } => labels,
        }
    }
}

/// Standard quantiles reported for timer/histogram samples.
pub const QUANTILES: &[f64] = &[0.5, 0.75, 0.90, 0.95, 0.98, 0.99, 0.999];

impl MetricsFrame {
    /// Coalesce multiple frames into one.
    ///
    /// Counters are summed. Gauges are weighted-averaged by interval.
    /// Timer histograms are merged via `Histogram::add()` for accurate
    /// quantiles across the combined interval.
    pub fn coalesce(frames: &[MetricsFrame]) -> MetricsFrame {
        if frames.is_empty() {
            return MetricsFrame {
                captured_at: Instant::now(),
                interval: Duration::ZERO,
                samples: Vec::new(),
            };
        }
        if frames.len() == 1 {
            return frames[0].clone();
        }

        let captured_at = frames.last().unwrap().captured_at;
        let interval: Duration = frames.iter().map(|f| f.interval).sum();

        // Group samples by labels identity
        let mut counter_acc: Vec<(Labels, u64)> = Vec::new();
        let mut gauge_acc: Vec<(Labels, f64, f64)> = Vec::new(); // (labels, weighted_sum, weight)
        let mut timer_acc: Vec<(Labels, u64, Option<HdrHistogram<u64>>)> = Vec::new();

        for frame in frames {
            let frame_weight = frame.interval.as_secs_f64();
            for sample in &frame.samples {
                match sample {
                    Sample::Counter { labels, value } => {
                        if let Some(entry) = counter_acc.iter_mut().find(|(l, _)| l == labels) {
                            entry.1 += value;
                        } else {
                            counter_acc.push((labels.clone(), *value));
                        }
                    }
                    Sample::Gauge { labels, value } => {
                        if let Some(entry) = gauge_acc.iter_mut().find(|(l, _, _)| l == labels) {
                            entry.1 += value * frame_weight;
                            entry.2 += frame_weight;
                        } else {
                            gauge_acc.push((labels.clone(), value * frame_weight, frame_weight));
                        }
                    }
                    Sample::Timer { labels, count, histogram } => {
                        if let Some(entry) = timer_acc.iter_mut().find(|(l, _, _)| l == labels) {
                            entry.1 = *count; // count is cumulative, take latest
                            if let Some(ref mut merged) = entry.2 {
                                if let Err(e) = merged.add(histogram) {
                                    eprintln!("warning: histogram merge failed: {e}");
                                }
                            } else {
                                entry.2 = Some(histogram.clone());
                            }
                        } else {
                            timer_acc.push((labels.clone(), *count, Some(histogram.clone())));
                        }
                    }
                }
            }
        }

        let mut samples = Vec::new();
        for (labels, value) in counter_acc {
            samples.push(Sample::Counter { labels, value });
        }
        for (labels, weighted_sum, weight) in gauge_acc {
            let value = if weight > 0.0 { weighted_sum / weight } else { 0.0 };
            samples.push(Sample::Gauge { labels, value });
        }
        for (labels, count, histogram) in timer_acc {
            if let Some(h) = histogram {
                samples.push(Sample::Timer { labels, count, histogram: h });
            }
        }

        MetricsFrame { captured_at, interval, samples }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_counter_frame(label: &str, value: u64, interval_ms: u64) -> MetricsFrame {
        MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_millis(interval_ms),
            samples: vec![Sample::Counter {
                labels: Labels::of("name", label),
                value,
            }],
        }
    }

    fn make_timer_frame(label: &str, values: &[u64], count: u64, interval_ms: u64) -> MetricsFrame {
        let mut h = HdrHistogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        for &v in values { let _ = h.record(v); }
        MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_millis(interval_ms),
            samples: vec![Sample::Timer {
                labels: Labels::of("name", label),
                count,
                histogram: h,
            }],
        }
    }

    #[test]
    fn coalesce_counters_sum() {
        let f1 = make_counter_frame("ops", 100, 1000);
        let f2 = make_counter_frame("ops", 150, 1000);
        let f3 = make_counter_frame("ops", 200, 1000);
        let merged = MetricsFrame::coalesce(&[f1, f2, f3]);
        assert_eq!(merged.interval, Duration::from_millis(3000));
        match &merged.samples[0] {
            Sample::Counter { value, .. } => assert_eq!(*value, 450),
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn coalesce_timers_merge_histograms() {
        let f1 = make_timer_frame("latency", &[1000, 2000, 3000], 3, 1000);
        let f2 = make_timer_frame("latency", &[4000, 5000], 5, 1000);
        let merged = MetricsFrame::coalesce(&[f1, f2]);

        match &merged.samples[0] {
            Sample::Timer { histogram, count, .. } => {
                assert_eq!(histogram.len(), 5); // all 5 observations merged
                assert_eq!(*count, 5); // latest cumulative count
                assert!(histogram.min() >= 900);
                assert!(histogram.max() <= 5500);
            }
            _ => panic!("expected timer"),
        }
    }

    #[test]
    fn coalesce_gauges_weighted_average() {
        let f1 = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Gauge {
                labels: Labels::of("name", "temp"),
                value: 10.0,
            }],
        };
        let f2 = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(2),
            samples: vec![Sample::Gauge {
                labels: Labels::of("name", "temp"),
                value: 20.0,
            }],
        };
        let merged = MetricsFrame::coalesce(&[f1, f2]);
        match &merged.samples[0] {
            Sample::Gauge { value, .. } => {
                // (10*1 + 20*2) / 3 = 16.67
                assert!((*value - 16.67).abs() < 0.1, "got {value}");
            }
            _ => panic!("expected gauge"),
        }
    }

    #[test]
    fn coalesce_empty() {
        let merged = MetricsFrame::coalesce(&[]);
        assert!(merged.samples.is_empty());
    }

    #[test]
    fn coalesce_single() {
        let f = make_counter_frame("ops", 42, 1000);
        let merged = MetricsFrame::coalesce(&[f]);
        match &merged.samples[0] {
            Sample::Counter { value, .. } => assert_eq!(*value, 42),
            _ => panic!("expected counter"),
        }
    }
}
