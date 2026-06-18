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

use polydat::dsl::registry::{FuncSig, FuncCategory as C, ParamSpec, Arity};
use polydat::ast::{PolydatNode, NodeMeta, Port, PortType, Purity, SlotType, Value};
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

/// Build a [`Selection`] from a `"key=value,key~substring"` pattern.
fn selection_from_pattern(pattern: &str) -> Selection {
    let mut sel = Selection::all();
    for part in pattern.split(',').map(str::trim) {
        if part.is_empty() { continue; }
        if let Some((key, value)) = part.split_once('=') {
            sel = sel.with_label(key.trim(), value.trim());
        } else if let Some((key, substring)) = part.split_once('~') {
            sel = sel.with_label_containing(key.trim(), substring.trim());
        }
    }
    sel
}

/// Read a stat from the canonical session-lifetime view.
///
/// Signature: `metric(label_pattern: str, stat: str) -> f64`
pub struct MetricCumulative {
    meta: NodeMeta,
    label_pattern: String,
    stat: String,
    query: Option<Arc<MetricsQuery>>,
}

impl MetricCumulative {
    pub fn new(label_pattern: &str, stat: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "metric".into(),
                outs: vec![Port::new("output", PortType::F64)],
                ins: Vec::new(),
            },
            label_pattern: label_pattern.to_string(),
            stat: stat.to_string(),
            query: get_query(),
        }
    }
}

impl PolydatNode for MetricCumulative {
    fn meta(&self) -> &NodeMeta { &self.meta }
    /// Intrinsically volatile: reads the live session-lifetime view the
    /// cadence pipeline frames. Per polydat R1.v this marks the node
    /// `Dynamic` — never const-folded, re-evaluated on every pull — so a
    /// metrics reader can never cache a stale (e.g. compile-time-empty) value.
    fn purity(&self) -> Purity {
        Purity::Nondeterministic {
            reason: "reads live session-lifetime metrics; value changes over the run",
        }
    }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let sel = selection_from_pattern(&self.label_pattern);
        let value = self.query.as_ref()
            .map(|q| q.session_lifetime(&sel))
            .and_then(|snap| extract_stat(&snap, &self.stat))
            .unwrap_or(0.0);
        outputs[0] = Value::F64(value);
    }

    /// `None`, intentionally: this reader has **no bounded lookback** —
    /// it reads [`MetricsQuery::session_lifetime`], a running total
    /// across ALL coordinates. The SRD-86 settle viability gate sizes
    /// warmup to a window so it clears the prior coordinate, but a
    /// session-cumulative value never clears the prior coordinate, so no
    /// gate can scope it. The settle instead *warns* when an optimizer
    /// objective reads this node, steering authors to `metric_window` or
    /// `metricsql_scalar(rate(...[W]))` for a per-coordinate objective.
    fn temporal_window_ms(&self) -> Option<i64> {
        None
    }
}

/// Read a stat from the latest closed smallest-cadence window.
///
/// Signature: `metric_window(label_pattern: str, stat: str) -> f64`
pub struct MetricWindow {
    meta: NodeMeta,
    label_pattern: String,
    stat: String,
    query: Option<Arc<MetricsQuery>>,
}

impl MetricWindow {
    pub fn new(label_pattern: &str, stat: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "metric_window".into(),
                outs: vec![Port::new("output", PortType::F64)],
                ins: Vec::new(),
            },
            label_pattern: label_pattern.to_string(),
            stat: stat.to_string(),
            query: get_query(),
        }
    }
}

impl PolydatNode for MetricWindow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    /// Intrinsically volatile: reads the latest closed cadence window the
    /// pipeline frames. Per polydat R1.v this marks the node `Dynamic` —
    /// never const-folded, re-evaluated on every pull.
    fn purity(&self) -> Purity {
        Purity::Nondeterministic {
            reason: "reads the latest framed cadence window; value changes over the run",
        }
    }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let sel = selection_from_pattern(&self.label_pattern);
        let value = self.query.as_ref()
            .and_then(|q| {
                let smallest = q.reporter().declared_cadences().smallest();
                if smallest.is_zero() { return None; }
                // Per-window derivations off the finest ring: counter stats read
                // the span INCREASE (`cycles`/`errors`, and `rate` = increase ÷
                // interval); the latency quantiles read the merged window
                // DISTRIBUTION. Contrast `metric(...)`, which reads
                // `session_lifetime` running totals.
                let snap = match self.stat.as_str() {
                    "p50" | "p99" | "mean" => q.distribution_over(smallest, &sel),
                    _ => q.increase_over(smallest, &sel),
                };
                extract_stat(&snap, &self.stat)
            })
            .unwrap_or(0.0);
        outputs[0] = Value::F64(value);
    }
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

/// Function signatures for the registry.
pub fn signatures() -> &'static [FuncSig] {
    &[
        FuncSig {
            name: "metric", category: C::Context, outputs: 1,
            description: "read cumulative metric value from in-process store",
            help: "Read a stat from the cumulative metrics view.\n\
                   Parameters:\n  label_pattern — comma-separated key=value or key~substring filters\n  \
                   stat — one of: cycles, errors, rate, p50, p99, mean\n\
                   Example: metric(\"phase=rampup\", \"p99\")\n\
                   Non-deterministic: value changes as metrics accumulate.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "label_pattern", slot_type: SlotType::ConstStr, required: true, example: "\"phase=rampup\"", constraint: None },
                ParamSpec { name: "stat", slot_type: SlotType::ConstStr, required: true, example: "\"p99\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: polydat::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: polydat::dsl::registry::OutputType::Fixed,
            // Both metric readers emit PortType::F64 (see the
            // NodeMeta above) — declared here so DSL infix typing
            // flows from the registry (FuncSig::output_port).
            output_port: Some(polydat::ast::PortType::F64),
        },
        FuncSig {
            name: "metric_window", category: C::Context, outputs: 1,
            description: "read last-window metric value from in-process store",
            help: "Read a stat from the most recent capture window.\n\
                   Parameters:\n  label_pattern — comma-separated key=value or key~substring filters\n  \
                   stat — one of: cycles, errors, rate, p50, p99, mean\n\
                   Example: metric_window(\"phase=search\", \"rate\")\n\
                   Non-deterministic: value changes each capture interval.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "label_pattern", slot_type: SlotType::ConstStr, required: true, example: "\"phase=search\"", constraint: None },
                ParamSpec { name: "stat", slot_type: SlotType::ConstStr, required: true, example: "\"rate\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: polydat::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: polydat::dsl::registry::OutputType::Fixed,
            // Both metric readers emit PortType::F64 (see the
            // NodeMeta above) — declared here so DSL infix typing
            // flows from the registry (FuncSig::output_port).
            output_port: Some(polydat::ast::PortType::F64),
        },
    ]
}

/// Build a metric node from function name and const args.
fn build_node(
    name: &str,
    _wires: &[polydat::compile::assembly::WireRef], _wire_types: &[polydat::ast::PortType],
    consts: &[polydat::dsl::ConstArg],
) -> Option<Result<Box<dyn PolydatNode>, String>> {
    match name {
        "metric" => {
            let pattern = consts.first().map(|c| c.as_str()).unwrap_or("");
            let stat = consts.get(1).map(|c| c.as_str()).unwrap_or("cycles");
            Some(Ok(Box::new(MetricCumulative::new(pattern, stat))))
        }
        "metric_window" => {
            let pattern = consts.first().map(|c| c.as_str()).unwrap_or("");
            let stat = consts.get(1).map(|c| c.as_str()).unwrap_or("cycles");
            Some(Ok(Box::new(MetricWindow::new(pattern, stat))))
        }
        _ => None,
    }
}

polydat::register_nodes!(signatures, build_node);
