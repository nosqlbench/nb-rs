// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK node functions for reading live metrics from the unified
//! [`MetricsQuery`] (SRD-42 §"MetricsQuery").
//!
//! - `metric(label_pattern, stat)` — reads
//!   [`MetricsQuery::session_lifetime`] (canonical session totals;
//!   subsumes the old cumulative view).
//! - `metric_window(label_pattern, stat)` — reads the smallest
//!   declared cadence's most-recently-closed window via
//!   [`MetricsQuery::cadence_window`].
//!
//! Both are non-deterministic context nodes. In strict mode they
//! require explicit acknowledgment. The query reference is captured
//! at node construction from a global static set by the runner.
//!
//! ## Stat accessors
//!
//! - `"cycles"` — cycles_total counter value
//! - `"rate"` — cycles/second
//! - `"p50"`, `"p99"`, `"mean"` — latency quantiles from cycles_servicetime (nanos)
//! - `"errors"` — errors_total counter value

use std::sync::{Arc, LazyLock, Mutex};

use crate::dsl::registry::{FuncSig, FuncCategory as C, ParamSpec, Arity};
use crate::node::{GkNode, NodeMeta, Port, PortType, SlotType, Value};
use nb_metrics::metrics_query::{MetricsQuery, Selection};
use nb_metrics::snapshot::{MetricSet, MetricValue};

/// Global metrics query reference. Set by the runner once the
/// cadence reporter is built. GK metric nodes capture this at
/// construction time.
static METRICS_QUERY: LazyLock<Mutex<Option<Arc<MetricsQuery>>>> =
    LazyLock::new(|| Mutex::new(None));

/// Set the global metrics query for GK node access.
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

impl GkNode for MetricCumulative {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let sel = selection_from_pattern(&self.label_pattern);
        let value = self.query.as_ref()
            .map(|q| q.session_lifetime(&sel))
            .and_then(|snap| extract_stat(&snap, &self.stat))
            .unwrap_or(0.0);
        outputs[0] = Value::F64(value);
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

impl GkNode for MetricWindow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let sel = selection_from_pattern(&self.label_pattern);
        let value = self.query.as_ref()
            .and_then(|q| {
                let smallest = q.reporter().declared_cadences().smallest();
                if smallest.is_zero() { return None; }
                let snap = q.cadence_window(smallest, &sel);
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
            MetricValue::Counter(c) => Some(c.total),
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
                ParamSpec { name: "label_pattern", slot_type: SlotType::ConstStr, required: true, example: "\"phase=rampup\"" },
                ParamSpec { name: "stat", slot_type: SlotType::ConstStr, required: true, example: "\"p99\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
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
                ParamSpec { name: "label_pattern", slot_type: SlotType::ConstStr, required: true, example: "\"phase=search\"" },
                ParamSpec { name: "stat", slot_type: SlotType::ConstStr, required: true, example: "\"rate\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Build a metric node from function name and const args.
pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef],
    consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn GkNode>, String>> {
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

crate::register_nodes!(signatures, build_node);
