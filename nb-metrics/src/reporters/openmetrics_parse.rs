// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! OpenMetrics / Prometheus text exposition format parser.
//!
//! Inverse of [`render_prometheus_text()`](super::openmetrics::render_prometheus_text).
//! Parses the text format back into a [`MetricsFrame`] so that metrics
//! can be pushed over HTTP and reconstructed on the receiving side.
//!
//! Timer histograms are reconstructed approximately — the full HDR
//! precision is lost in the text format, but the dashboard only needs
//! observation count and quantile values, which are preserved.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram as HdrHistogram;

use crate::frame::{MetricsFrame, Sample};
use crate::labels::Labels;

/// Parse Prometheus text exposition format into a `MetricsFrame`.
///
/// Recognises the line formats produced by `render_prometheus_text()`:
///
/// - `name_total{labels} value` → Counter
/// - `name{labels} value` → Gauge (unless part of a timer group)
/// - `name_count{labels} value` → Timer observation count
/// - `name_sum{labels} value` → Timer sum (seconds)
/// - `name{labels,quantile="Q"} value` → Timer quantile
/// - `name_rate{labels} value` → skipped (derived gauge)
///
/// Timer metrics are identified by the presence of `_count` / `_sum`
/// suffixes sharing a common base name. Quantile lines are grouped
/// by base name and used to reconstruct an approximate histogram.
pub fn parse_prometheus_text(text: &str) -> MetricsFrame {
    // First pass: collect all parsed lines and identify timer base names.
    let mut lines: Vec<ParsedLine> = Vec::new();
    let mut timer_bases: std::collections::HashSet<String> = std::collections::HashSet::new();

    for raw in text.lines() {
        let raw = raw.trim();
        if raw.is_empty() || raw.starts_with('#') {
            continue;
        }
        if let Some(parsed) = parse_line(raw) {
            // Detect timer families by _count suffix.
            if let LineKind::TimerCount { .. } = &parsed.kind {
                timer_bases.insert(parsed.base_name.clone());
            }
            lines.push(parsed);
        }
    }

    // Second pass: build samples.
    let mut samples: Vec<Sample> = Vec::new();

    // Accumulate timer parts keyed by (base_name, labels_without_quantile).
    let mut timer_parts: HashMap<(String, String), TimerAccum> = HashMap::new();

    for line in &lines {
        match &line.kind {
            LineKind::Counter { value } => {
                let labels = Labels::of("name", &line.base_name)
                    .extend(&line.labels);
                samples.push(Sample::Counter { labels, value: *value });
            }
            LineKind::Gauge { value } => {
                // A gauge line could be a standalone gauge or a _rate line.
                // Skip _rate lines (derived from timer).
                if line.is_rate {
                    continue;
                }
                // If this base_name is a known timer family, this is a
                // quantile line without a quantile label — skip.
                if timer_bases.contains(&line.base_name) && line.quantile.is_none() {
                    continue;
                }
                if let Some(q) = line.quantile {
                    // Timer quantile line — accumulate.
                    let key = (line.base_name.clone(), line.label_key.clone());
                    timer_parts.entry(key).or_default()
                        .quantiles.push((q, *value));
                } else {
                    let labels = Labels::of("name", &line.base_name)
                        .extend(&line.labels);
                    samples.push(Sample::Gauge { labels, value: *value });
                }
            }
            LineKind::TimerCount { value } => {
                let key = (line.base_name.clone(), line.label_key.clone());
                let accum = timer_parts.entry(key).or_default();
                accum.count = *value;
                accum.labels = line.labels.clone();
            }
            LineKind::TimerSum { value } => {
                let key = (line.base_name.clone(), line.label_key.clone());
                timer_parts.entry(key).or_default().sum_seconds = *value;
            }
        }
    }

    // Build timer samples from accumulated parts.
    for ((base_name, _), accum) in timer_parts {
        let labels = Labels::of("name", &base_name)
            .extend(&accum.labels);

        // Reconstruct approximate histogram from quantile values.
        let mut histogram = HdrHistogram::new_with_bounds(1, 3_600_000_000_000, 3)
            .expect("histogram bounds");

        if accum.quantiles.is_empty() && accum.count > 0 {
            // No quantile data — record sum-derived mean for count observations.
            let mean_nanos = if accum.count > 0 {
                (accum.sum_seconds / accum.count as f64 * 1_000_000_000.0) as u64
            } else {
                0
            };
            for _ in 0..accum.count.min(10_000) {
                if let Err(e) = histogram.record(mean_nanos.max(1)) {
                    eprintln!("warning: histogram record failed: {e}");
                }
            }
        } else {
            // Distribute observations across quantile buckets.
            // Sort quantiles ascending so we can compute per-bucket counts.
            let mut qs = accum.quantiles.clone();
            qs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

            let total = accum.count.max(1);
            let mut prev_q = 0.0_f64;
            for (q, val_seconds) in &qs {
                let val_nanos = (*val_seconds * 1_000_000_000.0) as u64;
                let bucket_fraction = q - prev_q;
                let bucket_count = (bucket_fraction * total as f64).round() as u64;
                for _ in 0..bucket_count.max(1).min(10_000) {
                    if let Err(e) = histogram.record(val_nanos.max(1)) {
                        eprintln!("warning: histogram record failed: {e}");
                    }
                }
                prev_q = *q;
            }
        }

        samples.push(Sample::Timer {
            labels,
            count: accum.count,
            histogram,
        });
    }

    MetricsFrame {
        captured_at: Instant::now(),
        interval: Duration::from_secs(1), // default; actual interval not in text format
        samples,
    }
}

// ─── Internal Types ─────────────────────────────────────────

struct TimerAccum {
    count: u64,
    sum_seconds: f64,
    quantiles: Vec<(f64, f64)>, // (quantile, value_seconds)
    labels: Labels,
}

impl Default for TimerAccum {
    fn default() -> Self {
        Self {
            count: 0,
            sum_seconds: 0.0,
            quantiles: Vec::new(),
            labels: Labels::empty(),
        }
    }
}

enum LineKind {
    Counter { value: u64 },
    Gauge { value: f64 },
    TimerCount { value: u64 },
    TimerSum { value: f64 },
}

struct ParsedLine {
    base_name: String,
    labels: Labels,
    label_key: String, // labels serialised for grouping (excludes quantile)
    kind: LineKind,
    quantile: Option<f64>,
    is_rate: bool,
}

// ─── Line Parser ────────────────────────────────────────────

/// Parse a single Prometheus text line into its components.
fn parse_line(line: &str) -> Option<ParsedLine> {
    // Format: metric_name{labels} value
    // Or:     metric_name value
    let (name_and_labels, value_str) = line.rsplit_once(' ')?;

    let (raw_name, labels_str) = if let Some(brace_start) = name_and_labels.find('{') {
        let name = &name_and_labels[..brace_start];
        let labels_inner = name_and_labels[brace_start + 1..]
            .strip_suffix('}')?;
        (name, Some(labels_inner))
    } else {
        (name_and_labels, None)
    };

    // Parse labels, extracting quantile separately.
    let mut labels = Labels::empty();
    let mut quantile: Option<f64> = None;
    let mut label_parts: Vec<String> = Vec::new();

    if let Some(inner) = labels_str {
        for pair in split_label_pairs(inner) {
            let (k, v) = pair.split_once('=')
                .map(|(k, v)| (k, v.trim_matches('"')))?;
            if k == "quantile" {
                quantile = v.parse().ok();
            } else {
                labels = labels.with(k, v);
                label_parts.push(format!("{k}={v}"));
            }
        }
    }

    let label_key = label_parts.join(",");

    // Classify the metric line.
    let is_rate = raw_name.ends_with("_rate");
    if is_rate {
        let base = raw_name.strip_suffix("_rate").unwrap().to_string();
        let value: f64 = value_str.parse().ok()?;
        return Some(ParsedLine {
            base_name: base,
            labels,
            label_key,
            kind: LineKind::Gauge { value },
            quantile: None,
            is_rate: true,
        });
    }

    if raw_name.ends_with("_total") {
        let base = raw_name.strip_suffix("_total").unwrap().to_string();
        let value: u64 = value_str.parse::<f64>().ok()? as u64;
        return Some(ParsedLine {
            base_name: base,
            labels,
            label_key,
            kind: LineKind::Counter { value },
            quantile: None,
            is_rate: false,
        });
    }

    if raw_name.ends_with("_count") {
        let base = raw_name.strip_suffix("_count").unwrap().to_string();
        let value: u64 = value_str.parse::<f64>().ok()? as u64;
        // Store labels on the timer accumulator.
        return Some(ParsedLine {
            base_name: base,
            labels,
            label_key,
            kind: LineKind::TimerCount { value },
            quantile: None,
            is_rate: false,
        });
    }

    if raw_name.ends_with("_sum") {
        let base = raw_name.strip_suffix("_sum").unwrap().to_string();
        let value: f64 = value_str.parse().ok()?;
        return Some(ParsedLine {
            base_name: base,
            labels,
            label_key,
            kind: LineKind::TimerSum { value },
            quantile: None,
            is_rate: false,
        });
    }

    // Plain gauge or timer quantile line.
    let value: f64 = value_str.parse().ok()?;
    Some(ParsedLine {
        base_name: raw_name.to_string(),
        labels,
        label_key,
        kind: LineKind::Gauge { value },
        quantile,
        is_rate: false,
    })
}

/// Split comma-separated label pairs, respecting quoted values.
fn split_label_pairs(s: &str) -> Vec<&str> {
    let mut pairs = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;

    for (i, c) in s.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                let part = s[start..i].trim();
                if !part.is_empty() {
                    pairs.push(part);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        pairs.push(last);
    }
    pairs
}

// ─── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporters::openmetrics::render_prometheus_text;

    #[test]
    fn round_trip_counter() {
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Counter {
                labels: Labels::of("name", "ops").with("activity", "write"),
                value: 42,
            }],
        };
        let text = render_prometheus_text(&frame);
        let parsed = parse_prometheus_text(&text);

        assert_eq!(parsed.samples.len(), 1);
        match &parsed.samples[0] {
            Sample::Counter { labels, value } => {
                assert_eq!(labels.get("name"), Some("ops"));
                assert_eq!(labels.get("activity"), Some("write"));
                assert_eq!(*value, 42);
            }
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn round_trip_gauge() {
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Gauge {
                labels: Labels::of("name", "heap_used"),
                value: 1234567.0,
            }],
        };
        let text = render_prometheus_text(&frame);
        let parsed = parse_prometheus_text(&text);

        assert_eq!(parsed.samples.len(), 1);
        match &parsed.samples[0] {
            Sample::Gauge { labels, value } => {
                assert_eq!(labels.get("name"), Some("heap_used"));
                assert!((value - 1234567.0).abs() < 1.0);
            }
            _ => panic!("expected gauge"),
        }
    }

    #[test]
    fn round_trip_timer() {
        let mut h = HdrHistogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        for i in 1..=1000 {
            let _ = h.record(i * 1_000_000); // 1ms to 1000ms
        }
        let original_p99 = h.value_at_quantile(0.99);

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
        let parsed = parse_prometheus_text(&text);

        // Find the timer sample.
        let timer = parsed.samples.iter().find(|s| matches!(s, Sample::Timer { .. }));
        assert!(timer.is_some(), "expected a timer sample");

        match timer.unwrap() {
            Sample::Timer { labels, count, histogram } => {
                assert_eq!(labels.get("name"), Some("latency"));
                assert_eq!(labels.get("activity"), Some("read"));
                assert_eq!(*count, 1000);
                // Reconstructed p99 should be close to original.
                let parsed_p99 = histogram.value_at_quantile(0.99);
                let original_seconds = original_p99 as f64 / 1_000_000_000.0;
                let parsed_seconds = parsed_p99 as f64 / 1_000_000_000.0;
                assert!(
                    (original_seconds - parsed_seconds).abs() < 0.01,
                    "p99 mismatch: original={original_seconds:.6}s, parsed={parsed_seconds:.6}s"
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn parse_empty_input() {
        let parsed = parse_prometheus_text("");
        assert!(parsed.samples.is_empty());
    }

    #[test]
    fn parse_ignores_comments() {
        let text = "# HELP foo A counter\n# TYPE foo counter\nfoo_total 42\n";
        let parsed = parse_prometheus_text(text);
        assert_eq!(parsed.samples.len(), 1);
    }
}
