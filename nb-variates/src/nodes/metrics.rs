// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK node functions for reading live metrics from the in-process store.
//!
//! - `metric(label_pattern, stat)` — reads the **cumulative** view.
//! - `metric_window(label_pattern, stat)` — reads the **last window** (most recent delta).
//!
//! Both are non-deterministic context nodes. In strict mode they require
//! explicit acknowledgment. The store reference is captured at node
//! construction from a global static set by the runner.
//!
//! ## Stat accessors
//!
//! - `"cycles"` — cycles_total counter value
//! - `"rate"` — cycles/second
//! - `"p50"`, `"p99"`, `"mean"` — latency quantiles from cycles_servicetime (nanos)
//! - `"errors"` — errors_total counter value

use std::sync::{Arc, LazyLock, Mutex, RwLock};

use crate::dsl::registry::{FuncSig, FuncCategory as C, ParamSpec, Arity};
use crate::node::{GkNode, NodeMeta, Port, PortType, SlotType, Value};
use nb_metrics::frame::{MetricsFrame, Sample};
use nb_metrics::store::InProcessMetricsStore;

/// Global metrics store reference. Set by the runner at startup.
/// GK metric nodes capture this at construction time.
static METRICS_STORE: LazyLock<Mutex<Option<Arc<RwLock<InProcessMetricsStore>>>>> =
    LazyLock::new(|| Mutex::new(None));

/// Set the global metrics store for GK node access.
///
/// Called once by the runner after creating the store.
pub fn set_global_store(store: Arc<RwLock<InProcessMetricsStore>>) {
    *METRICS_STORE.lock().unwrap_or_else(|e| e.into_inner()) = Some(store);
}

/// Get the global metrics store reference.
fn get_store() -> Option<Arc<RwLock<InProcessMetricsStore>>> {
    METRICS_STORE.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Read a stat from the cumulative view.
///
/// Signature: `metric(label_pattern: str, stat: str) -> f64`
pub struct MetricCumulative {
    meta: NodeMeta,
    label_pattern: String,
    stat: String,
    store: Option<Arc<RwLock<InProcessMetricsStore>>>,
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
            store: get_store(),
        }
    }
}

impl GkNode for MetricCumulative {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let value = self.store.as_ref()
            .and_then(|s| s.read().ok())
            .and_then(|store| {
                let results = store.query_cumulative(|l| {
                    label_matches(l, &self.label_pattern)
                });
                results.first().and_then(|(_, frame)| extract_stat(frame, &self.stat))
            })
            .unwrap_or(0.0);
        outputs[0] = Value::F64(value);
    }
}

/// Read a stat from the last window (most recent delta).
///
/// Signature: `metric_window(label_pattern: str, stat: str) -> f64`
pub struct MetricWindow {
    meta: NodeMeta,
    label_pattern: String,
    stat: String,
    store: Option<Arc<RwLock<InProcessMetricsStore>>>,
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
            store: get_store(),
        }
    }
}

impl GkNode for MetricWindow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let value = self.store.as_ref()
            .and_then(|s| s.read().ok())
            .and_then(|store| {
                let results = store.query_last_window(|l| {
                    label_matches(l, &self.label_pattern)
                });
                results.first().and_then(|(_, frame)| extract_stat(frame, &self.stat))
            })
            .unwrap_or(0.0);
        outputs[0] = Value::F64(value);
    }
}

/// Check if labels match a simple key=value pattern.
///
/// Pattern format: `"key=value"` or `"key~substring"`.
/// Multiple patterns separated by `,` — all must match.
fn label_matches(labels: &nb_metrics::labels::Labels, pattern: &str) -> bool {
    for part in pattern.split(',').map(str::trim) {
        if let Some((key, value)) = part.split_once('=') {
            if labels.get(key.trim()) != Some(value.trim()) {
                return false;
            }
        } else if let Some((key, substring)) = part.split_once('~') {
            match labels.get(key.trim()) {
                Some(v) if v.contains(substring.trim()) => {}
                _ => return false,
            }
        }
    }
    true
}

/// Extract a named stat from a MetricsFrame.
fn extract_stat(frame: &MetricsFrame, stat: &str) -> Option<f64> {
    match stat {
        "cycles" => {
            frame.samples.iter().find_map(|s| match s {
                Sample::Counter { labels, value } if labels.get("name") == Some("cycles_total") =>
                    Some(*value as f64),
                _ => None,
            })
        }
        "errors" => {
            frame.samples.iter().find_map(|s| match s {
                Sample::Counter { labels, value } if labels.get("name") == Some("errors_total") =>
                    Some(*value as f64),
                _ => None,
            })
        }
        "rate" => {
            let cycles = frame.samples.iter().find_map(|s| match s {
                Sample::Counter { labels, value } if labels.get("name") == Some("cycles_total") =>
                    Some(*value as f64),
                _ => None,
            })?;
            let secs = frame.interval.as_secs_f64().max(0.001);
            Some(cycles / secs)
        }
        "p50" | "p99" | "mean" => {
            frame.samples.iter().find_map(|s| match s {
                Sample::Timer { labels, histogram, .. }
                    if labels.get("name") == Some("cycles_servicetime") =>
                {
                    if histogram.len() == 0 { return None; }
                    Some(match stat {
                        "p50" => histogram.value_at_quantile(0.50) as f64,
                        "p99" => histogram.value_at_quantile(0.99) as f64,
                        "mean" => histogram.mean(),
                        _ => 0.0,
                    })
                }
                _ => None,
            })
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
