// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polydat-side metric-reading node registration.
//!
//! Formerly lived at `polydat::library::metrics`. Moved here so polydat
//! can publish to crates.io without a reverse dep on `nbrs-metrics`.
//! `inventory` is the registration channel — polydat's
//! `register_nodes!` macro emits an `inventory::submit!` block from
//! this crate; polydat picks it up at link time without knowing
//! who registered it.
//!
//! Polydat node functions for reading live metrics from the unified
//! [`MetricsQuery`] (SRD-42 §"MetricsQuery").
//!
//! - `metric(label_pattern, stat)` — reads
//!   [`MetricsQuery::session_lifetime`]: session running **totals**
//!   (`cycles`/`errors`) and the session-average `rate` (total ÷ session age).
//! - `metric_window(label_pattern, stat)` — at the smallest cadence: the
//!   latest window's per-window **increase** via
//!   [`MetricsQuery::increase_over`] (`cycles`/`errors`, and `rate` =
//!   increase ÷ interval), and its latency **distribution** via
//!   [`MetricsQuery::distribution_over`] (`p50`/`p99`/`mean`).
//!
//! Both are non-deterministic context nodes. In strict mode they
//! require explicit acknowledgment. The query reference is captured
//! at node construction from a global static set by the runner.
//!
//! ## Stat accessors (PromQL-aligned word stems)
//!
//! - `"cycles"` — cycles_total: session **total** (`metric`) / per-window
//!   **increase** (`metric_window`)
//! - `"errors"` — errors_total: session total / per-window increase
//! - `"rate"` — cycles/second (session-average / per-window)
//! - `"p50"`, `"p99"`, `"mean"` — latency quantiles from cycles_servicetime (nanos)

use std::sync::{Arc, LazyLock, Mutex};

// Node metadata + registration are emitted by `#[polydat::polydat_node]`
// (fully-qualified `polydat::…` paths, including the `Const<…>` marker), so
// no `polydat::ast` / `polydat::dsl::registry` imports are needed here.
use crate::metrics_query::{MetricsQuery, Selection};
use crate::snapshot::{MetricSet, MetricValue};

/// Global metrics query reference. Set by the runner once the
/// cadence reporter is built. Polydat metric nodes capture this at
/// construction time.
static METRICS_QUERY: LazyLock<Mutex<Option<Arc<MetricsQuery>>>> =
    LazyLock::new(|| Mutex::new(None));

/// Set the global metrics query for Polydat node access.
pub fn set_global_query(query: Arc<MetricsQuery>) {
    *METRICS_QUERY.lock().unwrap_or_else(|e| e.into_inner()) = Some(query);
}

/// Get the global metrics query reference.
fn get_query() -> Option<Arc<MetricsQuery>> {
    METRICS_QUERY.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Build a [`Selection`] from a `"family, key=value, key~substring"`
/// pattern, returning the family name (when one was given) alongside.
/// A bare token — no `=` / `~` — names the metric FAMILY, i.e. the
/// registered instrument name (`result_failure`, `attempt_failure`,
/// `cycles_servicetime`, …): the same names metrics.db carries, so
/// the reader vocabulary and the metric namespace are one. Labeled
/// parts narrow to matching series within the selection.
fn selection_from_pattern(pattern: &str) -> (Selection, Option<String>) {
    let mut sel = Selection::all();
    let mut family: Option<String> = None;
    for part in pattern.split(',').map(str::trim) {
        if part.is_empty() { continue; }
        if let Some((key, value)) = part.split_once('=') {
            sel = sel.with_label(key.trim(), value.trim());
        } else if let Some((key, substring)) = part.split_once('~') {
            sel = sel.with_label_containing(key.trim(), substring.trim());
        } else {
            sel = sel.with_family(part);
            family = Some(part.to_string());
        }
    }
    (sel, family)
}

/// Read a stat from the canonical session-lifetime view.
///
/// Signature: `metric(label_pattern: const str, stat: const str) -> f64`.
/// Reads [`MetricsQuery::session_lifetime`] — session running totals
/// (`cycles`/`errors`) and the session-average `rate`. Authored via
/// `#[polydat::polydat_node]` (SRD-80b).
///
/// Intrinsically `Nondeterministic`: reads the live session-lifetime view
/// the cadence pipeline frames, so the node is never const-folded and is
/// re-evaluated on every pull — a metrics reader can never cache a stale
/// (e.g. compile-time-empty) value.
#[polydat::polydat_node(
    category = Context,
    purity = Nondeterministic("reads live session-lifetime metrics; value changes over the run"),
)]
fn metric(label_pattern: Const<&str>, stat: Const<&str>) -> f64 {
    let (sel, family) = selection_from_pattern(label_pattern.0);
    get_query()
        .map(|q| q.session_lifetime(&sel))
        .and_then(|snap| match &family {
            Some(fam) => extract_family_stat(&snap, fam, stat.0),
            None => extract_stat(&snap, stat.0),
        })
        .unwrap_or(0.0)
}

/// Read a stat from the latest closed smallest-cadence window.
///
/// Signature: `metric_window(label_pattern: const str, stat: const str) ->
/// f64`. Counter stats read the per-window INCREASE (`cycles`/`errors`, and
/// `rate` = increase ÷ interval); latency quantiles (`p50`/`p99`/`mean`)
/// read the merged window DISTRIBUTION. Contrast [`metric`], which reads
/// session-lifetime running totals.
///
/// Intrinsically `Nondeterministic` — see [`metric`].
#[polydat::polydat_node(
    category = Context,
    purity = Nondeterministic("reads the latest framed cadence window; value changes over the run"),
)]
fn metric_window(label_pattern: Const<&str>, stat: Const<&str>) -> f64 {
    let (sel, family) = selection_from_pattern(label_pattern.0);
    get_query()
        .and_then(|q| {
            let smallest = q.reporter().declared_cadences().smallest();
            if smallest.is_zero() { return None; }
            let snap = match stat.0 {
                "p50" | "p99" | "mean" => q.distribution_over(smallest, &sel),
                _ => q.increase_over(smallest, &sel),
            };
            match &family {
                Some(fam) => extract_family_stat(&snap, fam, stat.0),
                None => extract_stat(&snap, stat.0),
            }
        })
        .unwrap_or(0.0)
}

/// Extract a named stat from a [`MetricSet`].
fn extract_stat(snapshot: &MetricSet, stat: &str) -> Option<f64> {
    fn counter_total(snapshot: &MetricSet, name: &str) -> Option<u64> {
        let f = snapshot.family(name)?;
        let m = f.metrics().next()?;
        match m.point()?.value() {
            MetricValue::Counter(c) => Some(c.cumulative),
            _ => None,
        }
    }

    match stat {
        "cycles" => counter_total(snapshot, "cycles_total").map(|v| v as f64),
        "errors" => counter_total(snapshot, "errors_total").map(|v| v as f64),
        "rate" => {
            let cycles = counter_total(snapshot, "cycles_total")? as f64;
            let secs = snapshot.interval().as_secs_f64().max(0.001);
            Some(cycles / secs)
        }
        "p50" | "p99" | "mean" => {
            let f = snapshot.family("cycles_servicetime")?;
            let m = f.metrics().next()?;
            match m.point()?.value() {
                MetricValue::Histogram(h) if h.count > 0 => Some(match stat {
                    "p50" => h.reservoir.value_at_quantile(0.50) as f64,
                    "p99" => h.reservoir.value_at_quantile(0.99) as f64,
                    "mean" => h.reservoir.mean(),
                    _ => 0.0,
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Read `stat` from the NAMED family's first series — the general
/// instrument-reader arm: any registered counter / gauge / histogram
/// is readable by its own name, no canned vocabulary. Stats:
/// `count` (counter cumulative or histogram count), `value` (gauge,
/// falling back to counter cumulative), `mean` / `p50` / `p99`
/// (histograms). Returns `None` — surfaced as `0.0` by the reader
/// nodes — when the family is absent from the snapshot or the stat
/// doesn't apply to its type.
fn extract_family_stat(snapshot: &MetricSet, family: &str, stat: &str) -> Option<f64> {
    let f = snapshot.family(family)?;
    let m = f.metrics().next()?;
    match (m.point()?.value(), stat) {
        (MetricValue::Counter(c), "count" | "value") => Some(c.cumulative as f64),
        (MetricValue::Gauge(g), "value") => Some(g.value),
        (MetricValue::Histogram(h), "count") => Some(h.count as f64),
        (MetricValue::Histogram(h), "mean") if h.count > 0 => Some(h.reservoir.mean()),
        (MetricValue::Histogram(h), "p50") if h.count > 0 =>
            Some(h.reservoir.value_at_quantile(0.50) as f64),
        (MetricValue::Histogram(h), "p99") if h.count > 0 =>
            Some(h.reservoir.value_at_quantile(0.99) as f64),
        _ => None,
    }
}

// `metric` / `metric_window` are authored via `#[polydat::polydat_node]`
// above — their FuncSig + builder are macro-generated and registered through
// the macro's own `inventory::submit!`, so no hand-written `signatures()` /
// `build_node()` / `register_nodes!` is needed here.
