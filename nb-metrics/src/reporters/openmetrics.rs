// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! OpenMetrics / Prometheus text exposition format renderer.
//!
//! Renders a MetricsFrame as Prometheus text format, suitable for
//! pushing to VictoriaMetrics `/api/v1/import/prometheus` or serving
//! via a scrape endpoint.

use crate::frame::{MetricsFrame, Sample, QUANTILES};
use crate::labels::Labels;

/// Render a MetricsFrame as Prometheus text exposition format.
pub fn render_prometheus_text(frame: &MetricsFrame) -> String {
    let mut out = String::new();
    let interval_secs = frame.interval.as_secs_f64().max(0.001);

    for sample in &frame.samples {
        match sample {
            Sample::Counter { labels, value } => {
                let name = sanitize_name(labels.get("name").unwrap_or("unknown"));
                let name_total = format!("{name}_total");
                let label_str = render_labels(labels, &["name"]);
                out.push_str(&format!("{name_total}{label_str} {value}\n"));
            }
            Sample::Gauge { labels, value } => {
                let name = sanitize_name(labels.get("name").unwrap_or("unknown"));
                let label_str = render_labels(labels, &["name"]);
                out.push_str(&format!("{name}{label_str} {value}\n"));
            }
            Sample::Timer { labels, count: _, histogram } => {
                let name = sanitize_name(labels.get("name").unwrap_or("unknown"));
                let label_str = render_labels(labels, &["name"]);
                let obs = histogram.len();

                // Count and sum
                let sum_nanos: f64 = histogram.mean() * obs as f64;
                let sum_seconds = sum_nanos / 1_000_000_000.0;
                out.push_str(&format!("{name}_count{label_str} {obs}\n"));
                out.push_str(&format!("{name}_sum{label_str} {sum_seconds:.6}\n"));

                // Quantiles
                for &q in QUANTILES {
                    let val_nanos = histogram.value_at_quantile(q);
                    let val_seconds = val_nanos as f64 / 1_000_000_000.0;
                    let q_label = if label_str.is_empty() {
                        format!("{{quantile=\"{q}\"}}")
                    } else {
                        let inner = &label_str[1..label_str.len()-1]; // strip {}
                        format!("{{{inner},quantile=\"{q}\"}}")
                    };
                    out.push_str(&format!("{name}{q_label} {val_seconds:.9}\n"));
                }

                // Rate (as a gauge)
                let rate = obs as f64 / interval_secs;
                out.push_str(&format!("{name}_rate{label_str} {rate:.2}\n"));
            }
        }
    }

    out
}

/// Sanitize a metric name to valid Prometheus identifier.
fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' })
        .collect()
}

/// Render labels as Prometheus format, excluding specified keys.
fn render_labels(labels: &Labels, exclude: &[&str]) -> String {
    let pairs: Vec<String> = labels.iter()
        .filter(|(k, _)| !exclude.contains(k))
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect();
    if pairs.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", pairs.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn render_counter() {
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Counter {
                labels: Labels::of("name", "ops").with("activity", "write"),
                value: 42,
            }],
        };
        let text = render_prometheus_text(&frame);
        assert!(text.contains("ops_total{activity=\"write\"} 42"));
    }

    #[test]
    fn render_gauge() {
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Gauge {
                labels: Labels::of("name", "heap_used"),
                value: 1234567.0,
            }],
        };
        let text = render_prometheus_text(&frame);
        assert!(text.contains("heap_used 1234567"));
    }

    #[test]
    fn render_timer_has_quantiles() {
        let mut h = hdrhistogram::Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        for i in 1..=1000 {
            let _ = h.record(i * 1_000_000); // 1ms to 1000ms
        }
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(5),
            samples: vec![Sample::Timer {
                labels: Labels::of("name", "latency").with("activity", "read"),
                count: 1000,
                histogram: h,
            }],
        };
        let text = render_prometheus_text(&frame);
        assert!(text.contains("latency_count"));
        assert!(text.contains("latency_sum"));
        assert!(text.contains("quantile=\"0.99\""));
        assert!(text.contains("latency_rate"));
    }
}
