// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The four `metricsql_*` polydat nodes. Each parses its MetricsQL
//! expression once at construction, then on every eval locates a live
//! metrics-access service ([`nbrs_metrics::queryapi::live_access`]),
//! evaluates the expression through this crate's engine, and projects
//! the result [`Vector`] into a `Value` by result-type affinity
//! ([`super::project`]).
//!
//! | node | shape | output |
//! |---|---|---|
//! | `metricsql` | general | `Value::Json` (labeled series) |
//! | `metricsql_scalar` | scalar | `Value::F64` |
//! | `metricsql_vector` | instant vector | `Value::VecF64` |
//! | `metricsql_window` | range vector (single series) | `Value::VecF64` |
//!
//! Registered through `polydat::register_nodes!` (inventory), like the
//! `metric()` stat-readers — polydat discovers them at link time.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use polydat::ast::{NodeMeta, PolydatNode, Port, PortType, Purity, SliceArc, Value};
use polydat::dsl::registry::{Arity, FuncCategory as C, FuncSig, OutputType, ParamSpec};
use polydat::ast::{Commutativity, SlotType};

use nbrs_metrics::queryapi::{Vector, live_access};

use super::project::{Shape, project};
use crate::ast::Expr;
use crate::eval::{EvalContext, evaluate};

/// Stale-tolerance lookback for the final instant evaluation — absorbs
/// cadence skew, matching the `nbrs metrics query` CLI default. Inner
/// rollups (`rate(m[5m])`) carry their own window.
const INSTANT_LOOKBACK_MS: i64 = 300_000;

/// A `metricsql_*` node: a parsed query + the result shape it projects.
struct MetricsqlNode {
    meta: NodeMeta,
    /// Node name, for diagnostics.
    label: &'static str,
    expr: Expr,
    shape: Shape,
}

impl MetricsqlNode {
    fn new(label: &'static str, expr: Expr, shape: Shape, out: PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: label.into(),
                outs: vec![Port::new("output", out)],
                ins: Vec::new(),
            },
            label,
            expr,
            shape,
        }
    }

    /// Locate a service, evaluate the query at "now", project the result.
    fn try_read(&self) -> Result<Value, String> {
        let service = live_access().ok_or("no live metrics service installed")?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let ctx = EvalContext {
            data: &*service,
            start_ms: now,
            end_ms: now,
            step_ms: 60_000,
            lookback_ms: Some(INSTANT_LOOKBACK_MS),
            query_start_ms: None,
            query_end_ms: None,
        };
        let series = evaluate(&ctx, &self.expr).map_err(|e| e.to_string())?;
        project(&Vector::new(series), self.shape).map_err(|e| e.message)
    }

    /// Type-appropriate empty value for a failed/empty read (no service,
    /// unsupported expression, or shape mismatch) — warned, not poisoned.
    fn default_value(&self) -> Value {
        match self.shape {
            Shape::Scalar => Value::F64(0.0),
            Shape::Vector | Shape::Window => Value::VecF64(SliceArc::from_vec(Vec::new())),
            Shape::General => Value::Json(Arc::new(serde_json::Value::Array(Vec::new()))),
        }
    }
}

impl PolydatNode for MetricsqlNode {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    /// Intrinsically volatile: the typed return is not a function of
    /// declared inputs — it reads the live metrics the cadence pipeline
    /// frames, which change over the run. Per polydat R1.v this marks the
    /// node `Dynamic` so it is never const-folded and re-evaluates on every
    /// pull (a metrics reader must never cache a stale snapshot).
    fn purity(&self) -> Purity {
        Purity::Nondeterministic {
            reason: "reads live metrics framed by the cadence pipeline; value changes over the run",
        }
    }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = self.try_read().unwrap_or_else(|e| {
            polydat::audit::warn(&format!("{}: {e}", self.label));
            self.default_value()
        });
    }

    /// The widest rollup window this query reads over (e.g. `[3s]` in
    /// `rate(errors_total[3s])`). The SRD-86 optimizer settle gate sizes
    /// its warmup to this so the window clears the prior coordinate
    /// before the objective is trusted. A 1 s step resolves any
    /// unit-less `[N]` window. `None` for a pure instant query.
    fn temporal_window_ms(&self) -> Option<i64> {
        crate::eval::max_rollup_window_ms(&self.expr, 1_000)
    }
}

const QUERY_PARAM: &[ParamSpec] = &[ParamSpec {
    name: "query",
    slot_type: SlotType::ConstStr,
    required: true,
    example: "\"sum(rate(errors_total[1m]))\"",
    constraint: None,
}];

const fn sig(name: &'static str, description: &'static str, output_port: PortType) -> FuncSig {
    FuncSig {
        name,
        category: C::Context,
        outputs: 1,
        description,
        help: "Evaluate a MetricsQL expression against the live in-process \
               metrics and project the result.\n\
               Parameter:\n  query — a MetricsQL expression string.\n\
               Non-deterministic: reads live metrics that change over the run.",
        identity: None,
        variadic_ctor: None,
        params: QUERY_PARAM,
        arity: Arity::Fixed,
        commutativity: Commutativity::Positional,
        default_resolver: None,
        output_type: OutputType::Fixed,
        output_port: Some(output_port),
    }
}

static SIGS: [FuncSig; 4] = [
    sig("metricsql", "evaluate a MetricsQL query → JSON (full labeled result)", PortType::Json),
    sig("metricsql_scalar", "evaluate a MetricsQL query → f64 (single value)", PortType::F64),
    sig("metricsql_vector", "evaluate a MetricsQL query → VecF64 (instant vector)", PortType::VecF64),
    sig("metricsql_window", "evaluate a MetricsQL query → VecF64 (windowed series)", PortType::VecF64),
];

/// Function signatures for the registry.
pub fn signatures() -> &'static [FuncSig] {
    &SIGS
}

/// Build a `metricsql_*` node from its name + the query const arg.
pub fn build_node(
    name: &str,
    _wires: &[polydat::compile::assembly::WireRef],
    _wire_types: &[PortType],
    consts: &[polydat::dsl::ConstArg],
) -> Option<Result<Box<dyn PolydatNode>, String>> {
    let (label, shape, out): (&'static str, Shape, PortType) = match name {
        "metricsql" => ("metricsql", Shape::General, PortType::Json),
        "metricsql_scalar" => ("metricsql_scalar", Shape::Scalar, PortType::F64),
        "metricsql_vector" => ("metricsql_vector", Shape::Vector, PortType::VecF64),
        "metricsql_window" => ("metricsql_window", Shape::Window, PortType::VecF64),
        _ => return None,
    };
    let query = consts.first().map(|c| c.as_str()).unwrap_or("");
    let expr = match crate::parse(query) {
        Ok(e) => e,
        Err(e) => return Some(Err(format!("{label}: parse error: {e}"))),
    };
    Some(Ok(Box::new(MetricsqlNode::new(label, expr, shape, out))))
}

polydat::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_dispatches_each_name_to_its_shape() {
        for (name, port) in [
            ("metricsql", PortType::Json),
            ("metricsql_scalar", PortType::F64),
            ("metricsql_vector", PortType::VecF64),
            ("metricsql_window", PortType::VecF64),
        ] {
            let consts = [polydat::dsl::ConstArg::Str("up".into())];
            let node = build_node(name, &[], &[], &consts).unwrap().unwrap();
            assert_eq!(node.meta().outs[0].typ, port, "wrong output port for {name}");
        }
    }

    #[test]
    fn unknown_name_is_none() {
        assert!(build_node("metricsql_nope", &[], &[], &[]).is_none());
    }

    #[test]
    fn registered_and_discoverable_through_the_polydat_compiler() {
        // The `register_nodes!`/inventory wiring makes the node findable
        // by name when polydat compiles a program that uses it.
        let k = polydat::dsl::compile::compile_polydat("score := metricsql_scalar(\"up\")");
        assert!(k.is_ok(), "metricsql_scalar should be a registered node: {k:?}");
    }

    #[test]
    fn parse_error_surfaces() {
        let consts = [polydat::dsl::ConstArg::Str("((((".into())];
        assert!(build_node("metricsql_scalar", &[], &[], &consts).unwrap().is_err());
    }

    #[test]
    fn eval_without_a_service_warns_and_defaults() {
        // No live service installed in this unit test → type-appropriate
        // empty value, no panic.
        let consts = [polydat::dsl::ConstArg::Str("up".into())];
        let node = build_node("metricsql_scalar", &[], &[], &consts).unwrap().unwrap();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        match &out[0] {
            Value::F64(f) => assert_eq!(*f, 0.0),
            other => panic!("expected F64 default, got {other:?}"),
        }
    }
}
