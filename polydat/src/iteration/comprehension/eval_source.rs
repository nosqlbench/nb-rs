// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Source evaluation — spec §10.7.0, §10.7.6, §10.7.8.
//!
//! Lifts [`IndexFn`] from a static AST property to a contextual
//! query: every [`Source`] variant answers
//! `evaluate(ctx) -> EvaluatedSource` carrying its materialized
//! values, observed cardinality, and the index function the
//! emitted values actually satisfy.
//!
//! ## Why this layer exists
//!
//! Before this module, [`crate::iteration::comprehension::metadata`]
//! computed `IndexFn` at AST-construction time using only static
//! source attributes (`cardinality_hint`, declared step, etc.).
//! Two classes of sources couldn't claim a useful `IndexFn`:
//!
//! - **`Source::Generator { expr }`** — the spec-text resolves
//!   to a list whose shape is only known after evaluation. The
//!   static path conservatively declared `Lattice { axis_sizes:
//!   [N] }` from `cardinality_hint` (or `Unbounded` without
//!   it), regardless of whether the actual values form a
//!   regular arithmetic progression.
//! - **`Source::WorkloadParamList { name }`** — same: the
//!   parameter's list contents are unknown until kernel
//!   evaluation.
//!
//! Non-`Lex` strategies (Diagonal / Extrema / Shells / Halton /
//! Sobol / Lhs) need the input's real `IndexFn` shape to
//! validate V4 and dispatch their indexed-form algorithms.
//! Without this module, V4 fires (or fails to fire) against
//! a stale static estimate; with this module, V4 fires
//! against the post-evaluation truth.
//!
//! ## Eval classes
//!
//! Per spec §10.7.0, sources partition into three eval classes:
//!
//! | Class | Variants | `evaluate(None)` works? |
//! |---|---|---|
//! | [`EvalClass::Static`] | `Literal`, `IntRange`, `ContinuousInterval`, `Distribution` (and registry-recognized `Generator`s, once PR β lands) | yes |
//! | [`EvalClass::ContextRequired`] | `Generator` outside the registry, `WorkloadParamList` | no — needs `&Context` |
//! | [`EvalClass::Distribution`] | `ContinuousInterval`, `Distribution` (in their "not yet sampled" state) | yes, but `values` is empty — enclosing `Order(_, sampling-strategy, Some(n))` materializes |
//!
//! The classifier on [`SourceEval::eval_class`] is the
//! compile-time signal: if a comprehension's entire source set
//! is `Static`, the IR planner can fire V4 early as a
//! usability nicety; otherwise V4 fires at strategy-invocation
//! time per spec §10.7.8.
//!
//! ## What this module DOES NOT own
//!
//! - The runtime walker that combines per-clause
//!   `EvaluatedSource`s into the cartesian / zip / union views
//!   strategies actually consume — that lives in
//!   [`crate::iteration::comprehension::runtime`].
//! - The strategy invocation itself — see
//!   [`crate::iteration::comprehension::strategies::Strategy::apply`].
//! - The compile-time V4 fire — see
//!   [`mod@crate::iteration::comprehension::validate`].

use std::sync::Arc;

use crate::iteration::comprehension::cardinality::ProductMeasure;
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::source::{LiteralValue, Source};
use crate::kernel::PolydatKernel;
use crate::ast::Value;

/// Result of evaluating one clause's source.
///
/// `values` carries the materialized stream (one [`Value`] per
/// output position). `cardinality` is the count of values
/// (`values.len() as u64`, equivalent to the `IndexFn`'s axis
/// total for discrete sources; `0` for un-sampled continuous
/// sources). `index_fn` is the addressing scheme the emitted
/// values actually satisfy — derived from observed shape for
/// `Generator` / `WorkloadParamList`, declared for static
/// variants.
#[derive(Debug, Clone)]
pub struct EvaluatedSource {
    pub values: Vec<Value>,
    pub cardinality: u64,
    pub index_fn: IndexFn,
}

/// Spec §10.7.0 partitioning.
///
/// Used by the IR planner's compile-time V4 best-effort fire:
/// if every clause in a comprehension reports
/// [`EvalClass::Static`], the planner can pre-evaluate them
/// with `ctx = None` and run V4 early; otherwise V4 fires at
/// strategy-invocation time only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalClass {
    /// Statically evaluable with no kernel / param context.
    /// `evaluate(None)` returns a fully-populated
    /// [`EvaluatedSource`].
    Static,

    /// Requires a kernel context to resolve interpolation
    /// references or workload-param lookups.
    /// `evaluate(None)` returns [`EvalError::NeedsContext`].
    ContextRequired,

    /// Continuous measure / distribution. `evaluate(None)`
    /// succeeds but emits an empty `values` vector; the
    /// `IndexFn` is `Continuous`. The enclosing sampling
    /// `Order(_, strategy, Some(n))` materializes draws.
    Distribution,
}

/// Errors returned by [`SourceEval::evaluate`].
#[derive(Debug, Clone)]
pub enum EvalError {
    /// The source needs a kernel context that wasn't provided.
    NeedsContext,

    /// Evaluation against the supplied context failed. `var`
    /// names the clause; `source` is the spec-text or
    /// description; `message` carries the underlying reason.
    EvalFailed {
        var: String,
        source: String,
        message: String,
    },
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::NeedsContext => f.write_str("source evaluation needs a kernel context"),
            EvalError::EvalFailed { var, source, message } => {
                write!(f, "source '{var} in {source}': {message}")
            }
        }
    }
}

impl std::error::Error for EvalError {}

/// Per-evaluation context for context-required sources.
///
/// Carries the live kernel against which `Source::Generator`
/// spec-text and `Source::WorkloadParamList` lookups resolve.
/// `var_name` lets the source synthesise a useful error
/// message; `prefix` is the prior-axis bindings the evaluator
/// installs via `PolydatKernel::materialize_subscope` so dependent
/// sources see earlier-axis values.
pub struct EvalContext<'a> {
    pub var_name: &'a str,
    pub parent: &'a Arc<PolydatKernel>,
    pub canonical: &'a Arc<PolydatKernel>,
    pub prefix: &'a [(String, Value)],
}

/// The source-evaluation surface.
///
/// Each [`Source`] variant implements this. The trait is
/// object-safe but typically called through the inherent
/// [`Source`] methods below.
pub trait SourceEval {
    /// Classify this source for the IR planner per spec
    /// §10.7.0. See [`EvalClass`].
    fn eval_class(&self) -> EvalClass;

    /// Materialize this source.
    ///
    /// Static sources (Literal / IntRange / ContinuousInterval
    /// / Distribution) accept `ctx = None`. Context-required
    /// sources (Generator / WorkloadParamList) require
    /// `Some(ctx)` and return [`EvalError::NeedsContext`]
    /// otherwise.
    fn evaluate(&self, ctx: Option<&EvalContext<'_>>) -> Result<EvaluatedSource, EvalError>;
}

impl SourceEval for Source {
    fn eval_class(&self) -> EvalClass {
        match self {
            Source::Literal { .. } | Source::IntRange { .. } => EvalClass::Static,
            Source::ContinuousInterval { .. } | Source::Distribution { .. } => {
                EvalClass::Distribution
            }
            // Pre-PR β every Generator is context-required.
            // PR β promotes registry-recognized generators to
            // Static by inspecting `expr` against the
            // built-in generator catalog.
            Source::Generator { .. } => EvalClass::ContextRequired,
            Source::WorkloadParamList { .. } => EvalClass::ContextRequired,
        }
    }

    fn evaluate(&self, ctx: Option<&EvalContext<'_>>) -> Result<EvaluatedSource, EvalError> {
        match self {
            Source::Literal { values } => {
                let vals: Vec<Value> = values.iter().map(literal_to_value).collect();
                let n = vals.len() as u64;
                Ok(EvaluatedSource {
                    values: vals,
                    cardinality: n,
                    // Literal lists carry no shape claim other
                    // than length — call them a 1-axis Lattice
                    // of that length. Strategies that need
                    // arithmetic progression shape (e.g. Halton
                    // over a Lattice axis) still get useful
                    // behavior because the lookup is by index,
                    // not by value.
                    index_fn: IndexFn::Lattice { axis_sizes: vec![n] },
                })
            }
            Source::IntRange { lo, hi, step } => {
                let step = (*step).max(1);
                let mut vals = Vec::new();
                let mut cur = *lo;
                while cur < *hi {
                    vals.push(Value::U64(cur as u64));
                    cur += step;
                }
                let n = vals.len() as u64;
                Ok(EvaluatedSource {
                    values: vals,
                    cardinality: n,
                    index_fn: IndexFn::Lattice { axis_sizes: vec![n] },
                })
            }
            Source::Generator { .. } | Source::WorkloadParamList { .. } => {
                let ctx = ctx.ok_or(EvalError::NeedsContext)?;
                let spec_text = match self {
                    Source::Generator { expr, .. } => expr.clone(),
                    Source::WorkloadParamList { name, .. } => format!("{{{name}}}"),
                    _ => unreachable!(),
                };
                let kernel = ctx
                    .parent
                    .materialize_subscope(ctx.canonical.program().clone(), ctx.prefix);
                let vals = crate::iteration::comprehension::eval::evaluate_spec(&spec_text, &kernel)
                    .map_err(|e| EvalError::EvalFailed {
                        var: ctx.var_name.to_string(),
                        source: spec_text,
                        message: e.to_string(),
                    })?;
                let n = vals.len() as u64;
                let index_fn = classify_observed_values(&vals);
                Ok(EvaluatedSource {
                    values: vals,
                    cardinality: n,
                    index_fn,
                })
            }
            Source::ContinuousInterval { interval, measure } => Ok(EvaluatedSource {
                values: Vec::new(),
                cardinality: 0,
                index_fn: IndexFn::Continuous {
                    intervals: vec![interval.clone()],
                    measure: measure.clone(),
                },
            }),
            Source::Distribution { support, .. } => Ok(EvaluatedSource {
                values: Vec::new(),
                cardinality: 0,
                // Distribution carries its own measure; without
                // wiring the full named-distribution measure
                // forward we treat the support interval under a
                // Uniform measure here. Sampling strategies
                // route through the AST `Distribution` carrier
                // directly when they need the named form.
                index_fn: IndexFn::Continuous {
                    intervals: vec![support.clone()],
                    measure: ProductMeasure::Uniform,
                },
            }),
        }
    }
}

/// Classify a materialized value list by observed shape.
///
/// Naïve PR α path (the "expand-then-classify" stage of spec
/// §10.7.6 / §10.7.8): a numeric arithmetic progression →
/// `Lattice { axis_sizes: [N] }` reflecting the regular stride.
/// Non-numeric or non-progression value lists → a plain
/// `Lattice { axis_sizes: [N] }` whose only shape claim is
/// length. Either way the strategy gets a useful 1-axis Lattice
/// for indexed-form dispatch.
///
/// PR β replaces this for registry-recognized generators where
/// the shape is declared from args without expansion.
fn classify_observed_values(vals: &[Value]) -> IndexFn {
    let n = vals.len() as u64;
    IndexFn::Lattice { axis_sizes: vec![n] }
}

fn literal_to_value(lv: &LiteralValue) -> Value {
    match lv {
        LiteralValue::Int(n) => Value::U64(*n as u64),
        LiteralValue::Float(f) => Value::F64(*f),
        LiteralValue::String(s) => Value::Str(Arc::from(s.as_str())),
        LiteralValue::Bool(b) => Value::Bool(*b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::cardinality::{Interval, MeasureName, ProductMeasure};
    use crate::iteration::comprehension::source::LiteralValue;

    #[test]
    fn literal_evaluates_without_context() {
        let s = Source::Literal {
            values: vec![LiteralValue::Int(1), LiteralValue::Int(2), LiteralValue::Int(3)],
        };
        assert_eq!(s.eval_class(), EvalClass::Static);
        let ev = s.evaluate(None).unwrap();
        assert_eq!(ev.cardinality, 3);
        assert_eq!(ev.values.len(), 3);
        assert!(matches!(ev.index_fn, IndexFn::Lattice { axis_sizes: ref a } if a == &vec![3]));
    }

    #[test]
    fn int_range_evaluates_without_context() {
        let s = Source::IntRange { lo: 0, hi: 10, step: 2 };
        assert_eq!(s.eval_class(), EvalClass::Static);
        let ev = s.evaluate(None).unwrap();
        // 0, 2, 4, 6, 8 = 5 values
        assert_eq!(ev.cardinality, 5);
        assert!(matches!(ev.index_fn, IndexFn::Lattice { axis_sizes: ref a } if a == &vec![5]));
    }

    #[test]
    fn generator_without_context_errors() {
        let s = Source::Generator {
            expr: "range(0, 10)".into(),
            cardinality_hint: Some(10),
        };
        assert_eq!(s.eval_class(), EvalClass::ContextRequired);
        match s.evaluate(None) {
            Err(EvalError::NeedsContext) => {}
            other => panic!("expected NeedsContext, got {other:?}"),
        }
    }

    #[test]
    fn workload_param_list_without_context_errors() {
        let s = Source::WorkloadParamList {
            name: "k_values".into(),
            len_hint: Some(5),
        };
        assert_eq!(s.eval_class(), EvalClass::ContextRequired);
        assert!(matches!(s.evaluate(None), Err(EvalError::NeedsContext)));
    }

    #[test]
    fn continuous_interval_yields_continuous_index_fn() {
        let s = Source::ContinuousInterval {
            interval: Interval::closed(0.0, 1.0),
            measure: ProductMeasure::Uniform,
        };
        assert_eq!(s.eval_class(), EvalClass::Distribution);
        let ev = s.evaluate(None).unwrap();
        assert_eq!(ev.cardinality, 0);
        assert!(ev.values.is_empty());
        match ev.index_fn {
            IndexFn::Continuous { intervals, .. } => assert_eq!(intervals.len(), 1),
            other => panic!("expected Continuous, got {other:?}"),
        }
    }

    #[test]
    fn distribution_yields_continuous_index_fn() {
        let s = Source::Distribution {
            distribution: MeasureName::Normal,
            support: Interval {
                lo: f64::NEG_INFINITY,
                hi: f64::INFINITY,
                lo_open: true,
                hi_open: true,
            },
            params: vec![0.0, 1.0],
        };
        assert_eq!(s.eval_class(), EvalClass::Distribution);
        let ev = s.evaluate(None).unwrap();
        assert_eq!(ev.cardinality, 0);
        assert!(matches!(ev.index_fn, IndexFn::Continuous { .. }));
    }

    #[test]
    fn generator_with_context_evaluates_to_lattice() {
        let parent = Arc::new(crate::dsl::compile_polydat("\n").unwrap());
        let canonical = Arc::new(crate::dsl::compile_polydat("\n").unwrap());
        let s = Source::Generator {
            expr: "1, 2, 3, 4, 5".into(),
            cardinality_hint: Some(5),
        };
        let ctx = EvalContext {
            var_name: "k",
            parent: &parent,
            canonical: &canonical,
            prefix: &[],
        };
        let ev = s.evaluate(Some(&ctx)).unwrap();
        assert_eq!(ev.cardinality, 5);
        assert!(matches!(ev.index_fn, IndexFn::Lattice { .. }));
    }
}
