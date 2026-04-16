// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! In-memory summary report from the InProcessMetricsStore.
//!
//! Builds `ActivityRow`s from cumulative MetricsFrame views, then
//! renders the same markdown table as the SQLite reporter. This
//! eliminates the SQLite round-trip for the summary report.

use crate::frame::{MetricsFrame, Sample};
use crate::labels::Labels;
use crate::store::InProcessMetricsStore;

#[cfg(feature = "sqlite")]
pub use crate::reporters::sqlite::{ReportConfig, ReportAggregate};

/// One row in the summary table — one per distinct component.
pub struct ActivityRow {
    pub activity: String,
    pub cycles: u64,
    pub rate: f64,
    pub latency_p50_ns: Option<f64>,
    pub latency_p99_ns: Option<f64>,
    pub latency_mean_ns: Option<f64>,
    pub gauges: Vec<(String, f64)>,
}

/// Build ActivityRows from the in-process store's cumulative views.
///
/// Each component with `cycles_total > 0` and without `nosummary=true`
/// becomes one row. Latency comes from `cycles_servicetime` timer
/// histograms. Gauges come from gauge samples (`.mean` suffix only).
pub fn rows_from_store(store: &InProcessMetricsStore) -> Vec<ActivityRow> {
    let entries = store.query_cumulative(|_| true);
    let mut rows = Vec::new();

    for (labels, frame) in entries {
        // Skip nosummary components
        if labels.get("nosummary") == Some("true") { continue; }

        let activity = format_activity_labels(labels);

        // Find cycles_total counter
        let cycles = frame.samples.iter()
            .filter_map(|s| match s {
                Sample::Counter { labels: sl, value } if has_name(sl, "cycles_total") => Some(*value),
                _ => None,
            })
            .next()
            .unwrap_or(0);

        if cycles == 0 { continue; }

        // Compute rate from elapsed time
        let rate = cycles as f64 / frame.interval.as_secs_f64().max(0.001);

        // Find cycles_servicetime timer for latency
        let latency = frame.samples.iter()
            .filter_map(|s| match s {
                Sample::Timer { labels: sl, histogram, .. } if has_name(sl, "cycles_servicetime") => {
                    if histogram.len() == 0 { return None; }
                    Some((
                        histogram.value_at_quantile(0.50) as f64,
                        histogram.value_at_quantile(0.99) as f64,
                        histogram.mean(),
                    ))
                }
                _ => None,
            })
            .next();

        // Collect gauge values (only .mean variants)
        let mut gauges: Vec<(String, f64)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for sample in &frame.samples {
            if let Sample::Gauge { labels: sl, value } = sample {
                if let Some(name) = sl.get("name") {
                    if name.ends_with(".mean") {
                        let short = name.strip_suffix(".mean").unwrap_or(name);
                        if !seen.contains(short) {
                            seen.insert(short.to_string());
                            gauges.push((short.to_string(), *value));
                        }
                    }
                }
            }
        }

        rows.push(ActivityRow {
            activity,
            cycles,
            rate,
            latency_p50_ns: latency.map(|l| l.0),
            latency_p99_ns: latency.map(|l| l.1),
            latency_mean_ns: latency.map(|l| l.2),
            gauges,
        });
    }

    rows
}

/// Auto-select the time unit suffix so the numeric part has significant digits.
/// Input is nanoseconds (sysref standard).
pub fn format_duration(nanos: f64) -> String {
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

/// Print a summary report from the in-process store.
#[cfg(feature = "sqlite")]
pub fn print_summary_from_store(store: &InProcessMetricsStore, config: &ReportConfig) {
    let row_patterns: Vec<regex::Regex> = config.row_filters.iter()
        .filter_map(|p| regex::Regex::new(p.trim()).ok())
        .collect();

    let rows = rows_from_store(store);
    if rows.is_empty() { return; }

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

    let mut headers: Vec<String> = vec![
        "Activity".into(), "Cycles".into(), "Rate".into(),
    ];
    if has_latency {
        headers.extend(["p50".into(), "p99".into(), "mean".into()]);
    }
    for name in &gauge_names {
        headers.push(name.clone());
    }

    let mut grid: Vec<Vec<String>> = Vec::new();
    for row in &rows {
        if !row_patterns.is_empty()
            && !row_patterns.iter().any(|p| p.is_match(&row.activity))
        {
            continue;
        }
        grid.push(format_row(row, has_latency, &gauge_names));
    }

    // Compute aggregate rows
    let agg_rows = compute_aggregates(&config.aggregates, &rows, has_latency, &gauge_names);

    if !config.show_details { grid.clear(); }
    if grid.is_empty() && agg_rows.is_empty() { return; }

    align_activity_column(&mut grid);

    if !agg_rows.is_empty() && !grid.is_empty() {
        let blank: Vec<String> = (0..headers.len()).map(|_| String::new()).collect();
        grid.push(blank);
    }
    grid.extend(agg_rows);

    let ncols = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in &grid {
        for (i, cell) in row.iter().enumerate() {
            let w = cell.chars().count();
            if i < ncols && w > widths[i] { widths[i] = w; }
        }
    }

    println!();
    println!("## Summary");
    println!();

    let mut line = String::from("|");
    for (i, h) in headers.iter().enumerate() {
        line.push_str(&format!(" {:<w$} |", h, w = widths[i]));
    }
    println!("{line}");

    let mut sep = String::from("|");
    for w in &widths { sep.push_str(&format!("-{}-|", "-".repeat(*w))); }
    println!("{sep}");

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

fn format_row(row: &ActivityRow, has_latency: bool, gauge_names: &[String]) -> Vec<String> {
    let rate_str = if row.rate > 0.0 {
        format!("{:.0}/s", row.rate)
    } else {
        "-".to_string()
    };
    let mut cells = vec![row.activity.clone(), row.cycles.to_string(), rate_str];
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

#[cfg(feature = "sqlite")]
fn compute_aggregates(
    aggregates: &[ReportAggregate],
    rows: &[ActivityRow],
    has_latency: bool,
    gauge_names: &[String],
) -> Vec<Vec<String>> {
    let mut agg_rows = Vec::new();
    for agg in aggregates {
        let matching: Vec<&ActivityRow> = rows.iter()
            .filter(|r| {
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
        let mut cells = vec![label, "-".into(), "-".into()];
        if has_latency { cells.extend(["-".into(), "-".into(), "-".into()]); }

        for gauge_name in gauge_names {
            if !gauge_name.contains(&agg.column_pattern) {
                cells.push("-".into());
                continue;
            }
            let values: Vec<f64> = matching.iter()
                .filter_map(|r| r.gauges.iter().find(|(n, _)| n == gauge_name).map(|(_, v)| *v))
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

/// Format activity labels for display (skip internal labels).
fn format_activity_labels(labels: &Labels) -> String {
    let parts: Vec<String> = labels.iter()
        .filter(|(k, _)| {
            !matches!(*k, "session" | "n" | "name" | "nosummary")
        })
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    parts.join(", ")
}

fn has_name(labels: &Labels, name: &str) -> bool {
    labels.get("name") == Some(name)
}

/// Align label components within the Activity column.
fn align_activity_column(grid: &mut [Vec<String>]) {
    if grid.is_empty() { return; }

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

    let mut all_keys: Vec<String> = Vec::new();
    let longest = parsed.iter().max_by_key(|r| r.len());
    if let Some(row) = longest {
        for (key, _) in row {
            if !all_keys.contains(key) { all_keys.push(key.clone()); }
        }
    }
    for row in &parsed {
        for (key, _) in row {
            if !all_keys.contains(key) { all_keys.push(key.clone()); }
        }
    }

    let mut slot_widths: Vec<usize> = vec![0; all_keys.len()];
    for row in &parsed {
        for (i, key) in all_keys.iter().enumerate() {
            if let Some((_, seg)) = row.iter().find(|(k, _)| k == key) {
                let w = seg.chars().count();
                if w > slot_widths[i] { slot_widths[i] = w; }
            }
        }
    }

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
