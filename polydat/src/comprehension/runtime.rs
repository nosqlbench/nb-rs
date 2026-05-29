// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Runtime evaluator — walks an algebra [`Comprehension`] AST
//! against a live parent kernel context to produce typed
//! coordinate tuples.
//!
//! ## Why this is separate from the static IR interpreter
//!
//! The static IR interpreter (`super::ir::interpreter`) walks
//! a compiled stack-machine program over fully-statically-
//! resolvable `Source` variants (`IntRange`, `Literal`). It
//! has no notion of a runtime parent kernel, which is correct
//! for the spec §9.5 consumption surfaces it serves.
//!
//! Runtime comprehension evaluation is fundamentally different:
//!
//! - **Source-text evaluation requires the parent kernel.**
//!   `Source::Generator { expr: "pre_{outer}" }` and
//!   `Source::WorkloadParamList { name }` resolve against the
//!   parent kernel's chain — `{outer}` substitution via
//!   [`interpolate_via_kernel`], `kernel.lookup(name)` for
//!   workload params.
//! - **Cartesian is dependent-tuple, not independent.** Clause
//!   N's spec text may reference iter-vars from clauses
//!   1..N-1. Per-branch kernels (via
//!   [`GkKernel::materialize_subscope`]) carry the prior
//!   values forward so each clause evaluates against the
//!   correct context. This is SRD-18b §"Dependent Tuple
//!   Iteration".
//! - **Filter predicates evaluate against per-tuple kernels.**
//!   The predicate text is interpolated against a kernel with
//!   every tuple value installed, then run through
//!   `eval_const_expr`.
//!
//! All three depend on polydat-side primitives that exist
//! today; this evaluator is the algebra-typed entry point for
//! them.
//!
//! ## What this owns
//!
//! [`evaluate_for_iteration`] is the public surface:
//! `(algebra AST + parent kernel + canonical kernel +
//! workload params) → Vec<Tuple>`. The returned tuples carry
//! [`TupleValue`]s ready for per-iteration kernel
//! construction via [`GkKernel::for_iteration`].
//!
//! Order modifiers route through the existing algebra
//! [`Strategy`] implementations; the evaluator just applies
//! the strategy's `naive_apply` after producing the tuple
//! list.
//!
//! ## What this does NOT own
//!
//! - Per-iteration kernel construction. The evaluator returns
//!   tuples; the caller (executor or stream surface) builds
//!   the per-iter kernel via `GkKernel::for_iteration`.
//! - Empty-clause policy (strict / warn). The caller passes
//!   an `on_empty` callback the same way `enumerate_tuples`
//!   does today.

use std::collections::HashMap;
use std::sync::Arc;

use crate::comprehension::ast::Comprehension;
use crate::comprehension::source::{LiteralValue, Source};
use crate::comprehension::strategies::{Tuple, TupleValue};
use crate::comprehension::strategy::StrategyName;
use crate::comprehension::eval::evaluate_spec;
use crate::dsl::compile::eval_const_expr;
use crate::kernel::GkKernel;
use crate::kernel::interp::interpolate_via_kernel;
use crate::node::Value;

/// Runtime tuple type — polydat-Value-based to preserve Ext
/// typing (Partition / Json / etc.) through the iteration
/// pipeline. The algebra layer's [`Tuple`] uses
/// [`TupleValue`] which is scalar-only; this `RuntimeTuple`
/// is what the executor actually wants for per-iteration
/// kernel binding via [`GkKernel::for_iteration`].
pub type RuntimeTuple = Vec<(String, Value)>;

// (gk_kernel conversion helpers no longer needed at module
// level — algebra-strategy ordering uses
// polydat_value_to_tuple_value locally; runtime evaluation
// uses polydat Value end-to-end.)

/// Reason a clause produced no values, for the caller's
/// empty-clause policy callback.
#[derive(Debug)]
pub struct EmptyClause<'a> {
    pub var: &'a str,
    pub spec_expr: Option<&'a str>,
}

/// Errors the runtime evaluator surfaces.
#[derive(Debug, Clone)]
pub enum RuntimeError {
    /// Source evaluation failed (interpolation error,
    /// eval_const_expr error, unsupported source shape, etc.).
    SourceEval { var: String, source: String, message: String },
    /// Filter predicate evaluation failed.
    FilterEval { predicate: String, message: String },
    /// Strategy application failed.
    OrderEval { strategy: StrategyName, message: String },
    /// The runtime evaluator encountered an algebra-AST shape
    /// it doesn't support (e.g., nested Filter under Order).
    UnsupportedShape(String),
    /// Caller's `on_empty` callback returned an error.
    EmptyPolicy(String),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::SourceEval { var, source, message } => write!(
                f,
                "for_each clause '{var} in {source}': {message}"
            ),
            RuntimeError::FilterEval { predicate, message } => write!(
                f,
                "comprehension filter '{predicate}': {message}"
            ),
            RuntimeError::OrderEval { strategy, message } => write!(
                f,
                "order strategy {strategy:?}: {message}"
            ),
            RuntimeError::UnsupportedShape(msg) => write!(f, "{msg}"),
            RuntimeError::EmptyPolicy(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

/// Evaluate a comprehension against the parent kernel and
/// produce the typed coordinate-tuple list.
///
/// `canonical` is the comprehension scope's kernel program
/// (used for per-branch `materialize_subscope` calls).
/// `parent` is the runtime parent — the kernel chain root for
/// all source evaluation and shadow resolution.
/// `workload_params` provides the fallback for
/// `Source::WorkloadParamList` names not yet promoted into
/// the kernel chain.
///
/// `on_empty` is called with each empty clause (zero values
/// after evaluation). Caller decides whether to abort (strict)
/// or warn-and-skip (relaxed) — same shape as
/// `enumerate_tuples`'s callback.
pub fn evaluate_for_iteration<F>(
    comp: &Comprehension,
    parent: &Arc<GkKernel>,
    canonical: &Arc<GkKernel>,
    workload_params: &HashMap<String, String>,
    on_empty: F,
) -> Result<Vec<RuntimeTuple>, RuntimeError>
where
    F: FnMut(EmptyClause<'_>) -> Result<(), String>,
{
    let mut state = EvalState {
        parent: parent.clone(),
        canonical: canonical.clone(),
        workload_params,
        on_empty,
    };
    state.evaluate_node(comp, &[])
}

/// Internal walker state — bundles the closures and shared
/// references so the recursive walker doesn't have to thread
/// them through every call.
struct EvalState<'a, F> {
    parent: Arc<GkKernel>,
    canonical: Arc<GkKernel>,
    /// Workload-param fallback. The polydat-owned
    /// `evaluate_spec` already routes through the parent
    /// kernel chain for shadow-aware resolution (SRD-21),
    /// so this is unused at the runtime evaluator level —
    /// kept on the surface for symmetry with the legacy
    /// `iterate_scope` signature in case some future
    /// `Source` variant needs param-aware evaluation that
    /// can't go through the kernel.
    #[allow(dead_code)]
    workload_params: &'a HashMap<String, String>,
    on_empty: F,
}

impl<F> EvalState<'_, F>
where
    F: FnMut(EmptyClause<'_>) -> Result<(), String>,
{
    fn evaluate_node(
        &mut self,
        node: &Comprehension,
        prefix: &[(String, Value)],
    ) -> Result<Vec<RuntimeTuple>, RuntimeError> {
        match node {
            Comprehension::Clause { name, source } => {
                let values = self.evaluate_source(name, source, prefix)?;
                if values.is_empty() {
                    let spec_text = source_display_text(source);
                    let err = (self.on_empty)(EmptyClause {
                        var: name,
                        spec_expr: spec_text.as_deref(),
                    })
                    .map_err(RuntimeError::EmptyPolicy)?;
                    let _ = err;
                    return Ok(Vec::new());
                }
                Ok(values
                    .into_iter()
                    .map(|v| vec![(name.clone(), v)])
                    .collect())
            }
            Comprehension::Cartesian { children } => {
                self.evaluate_cartesian(children, prefix)
            }
            Comprehension::Zip { children, mode } => {
                self.evaluate_zip(children, *mode, prefix)
            }
            Comprehension::Union { children } => {
                let mut out = Vec::new();
                for child in children {
                    let sub = self.evaluate_node(child, prefix)?;
                    out.extend(sub);
                }
                Ok(out)
            }
            Comprehension::Filter { child, predicate } => {
                let tuples = self.evaluate_node(child, prefix)?;
                self.apply_filter(tuples, predicate)
            }
            Comprehension::Order { child, strategy, truncation } => {
                let tuples = self.evaluate_node(child, prefix)?;
                self.apply_order(tuples, *strategy, *truncation)
            }
        }
    }

    fn evaluate_cartesian(
        &mut self,
        children: &[Comprehension],
        prefix: &[(String, Value)],
    ) -> Result<Vec<RuntimeTuple>, RuntimeError> {
        if children.is_empty() {
            return Ok(vec![Vec::new()]);
        }
        let (head, tail) = children.split_first().unwrap();
        let head_tuples = self.evaluate_node(head, prefix)?;
        if tail.is_empty() {
            return Ok(head_tuples);
        }
        let mut out = Vec::new();
        for head_tuple in head_tuples {
            let mut extended_prefix: Vec<(String, Value)> = prefix.to_vec();
            extended_prefix.extend(head_tuple.iter().cloned());
            let tail_tuples = self.evaluate_cartesian(tail, &extended_prefix)?;
            for tail_tuple in tail_tuples {
                let mut merged = head_tuple.clone();
                merged.extend(tail_tuple);
                out.push(merged);
            }
        }
        Ok(out)
    }

    fn evaluate_zip(
        &mut self,
        children: &[Comprehension],
        mode: crate::comprehension::strategy::ZipMode,
        prefix: &[(String, Value)],
    ) -> Result<Vec<RuntimeTuple>, RuntimeError> {
        use crate::comprehension::strategy::ZipMode;
        if children.is_empty() {
            return Ok(vec![Vec::new()]);
        }
        let per_child: Vec<Vec<RuntimeTuple>> = children
            .iter()
            .map(|c| self.evaluate_node(c, prefix))
            .collect::<Result<_, _>>()?;
        let lengths: Vec<usize> = per_child.iter().map(|t| t.len()).collect();
        let iter_count = match mode {
            ZipMode::Strict => {
                let first = lengths.first().copied().unwrap_or(0);
                if lengths.iter().any(|&n| n != first) {
                    return Err(RuntimeError::UnsupportedShape(format!(
                        "zip strict: child lengths differ ({lengths:?})"
                    )));
                }
                first
            }
            ZipMode::Truncate => lengths.iter().copied().min().unwrap_or(0),
            ZipMode::Cycle => lengths.iter().copied().max().unwrap_or(0),
        };
        let mut out = Vec::with_capacity(iter_count);
        for i in 0..iter_count {
            let mut bindings: RuntimeTuple = Vec::new();
            for (child_tuples, &len) in per_child.iter().zip(lengths.iter()) {
                if len == 0 {
                    continue;
                }
                let idx = match mode {
                    ZipMode::Cycle => i % len,
                    _ => i,
                };
                bindings.extend(child_tuples[idx].iter().cloned());
            }
            out.push(bindings);
        }
        Ok(out)
    }

    fn evaluate_source(
        &mut self,
        var: &str,
        source: &Source,
        prefix: &[(String, Value)],
    ) -> Result<Vec<Value>, RuntimeError> {
        match source {
            Source::IntRange { lo, hi, step } => {
                let step = (*step).max(1);
                let mut out = Vec::new();
                let mut cur = *lo;
                while cur < *hi {
                    // i64 → u64 bitcast: matches the legacy
                    // evaluator's choice (polydat Value's
                    // integer is u64; comprehension coords are
                    // typically non-negative).
                    out.push(Value::U64(cur as u64));
                    cur += step;
                }
                Ok(out)
            }
            Source::Literal { values } => {
                Ok(values.iter().map(literal_to_polydat_value).collect())
            }
            Source::Generator { .. } | Source::WorkloadParamList { .. } => {
                // Delegate to the polydat-owned `evaluate_spec`
                // which already implements the full layered
                // evaluation (range, generator, setop,
                // sequencer, eval_const_expr, literal-list
                // fallback, partition unpacking).
                let spec_text = match source {
                    Source::Generator { expr, .. } => expr.clone(),
                    Source::WorkloadParamList { name, .. } => format!("{{{name}}}"),
                    _ => unreachable!(),
                };
                let kernel = self
                    .parent
                    .materialize_subscope(self.canonical.program().clone(), prefix);
                evaluate_spec(&spec_text, &kernel).map_err(|message| {
                    RuntimeError::SourceEval {
                        var: var.to_string(),
                        source: spec_text,
                        message,
                    }
                })
            }
            Source::ContinuousInterval { .. } | Source::Distribution { .. } => {
                Err(RuntimeError::UnsupportedShape(format!(
                    "clause '{var}': continuous / distribution source needs an enclosing Order"
                )))
            }
        }
    }

    fn apply_filter(
        &mut self,
        tuples: Vec<RuntimeTuple>,
        predicate: &str,
    ) -> Result<Vec<RuntimeTuple>, RuntimeError> {
        let mut out = Vec::with_capacity(tuples.len());
        for tuple in tuples {
            let kernel = self
                .parent
                .materialize_subscope(self.canonical.program().clone(), &tuple);
            let interpolated = interpolate_via_kernel(predicate, &kernel).map_err(|e| {
                RuntimeError::FilterEval { predicate: predicate.to_string(), message: e }
            })?;
            let result = eval_const_expr(&interpolated).map_err(|e| RuntimeError::FilterEval {
                predicate: predicate.to_string(),
                message: e,
            })?;
            let keep = match result {
                Value::Bool(b) => b,
                Value::U64(n) => n != 0,
                Value::F64(n) => n != 0.0,
                other => {
                    return Err(RuntimeError::FilterEval {
                        predicate: predicate.to_string(),
                        message: format!("expected bool/u64/f64, got {other:?}"),
                    })
                }
            };
            if keep {
                out.push(tuple);
            }
        }
        Ok(out)
    }

    fn apply_order(
        &mut self,
        tuples: Vec<RuntimeTuple>,
        strategy: StrategyName,
        truncation: Option<u64>,
    ) -> Result<Vec<RuntimeTuple>, RuntimeError> {
        // Route through algebra strategies' naive_apply,
        // converting the polydat-Value-typed tuples to
        // algebra Tuples for the strategies' contract. The
        // strategies operate on TupleValue (which is fine
        // for ordering — sort/permute don't care about Ext
        // type fidelity), then we map back to the original
        // RuntimeTuples via index.
        use crate::comprehension::strategies::{
            antidiagonal::Antidiagonal, diagonal::Diagonal, extrema::Extrema, halton::Halton,
            lex::Lex, lhs::Lhs, reverse_lex::ReverseLex, shells::Shells, shuffle::Shuffle,
            sobol::Sobol, Strategy,
        };
        use crate::comprehension::surfaces::polydat_value_to_tuple_value;

        // Build the algebra-shape tuples for the strategy,
        // keeping the original RuntimeTuple alongside indexed.
        let algebra_tuples: Vec<Tuple> = tuples
            .iter()
            .map(|rt| Tuple {
                bindings: rt
                    .iter()
                    .map(|(n, v)| {
                        let tv = polydat_value_to_tuple_value(v)
                            .unwrap_or(TupleValue::Str(v.to_display_string()));
                        (n.clone(), tv)
                    })
                    .collect(),
            })
            .collect();
        let trunc = truncation;
        let ordered: Vec<Tuple> = match strategy {
            StrategyName::Lex => Lex.naive_apply(algebra_tuples, trunc),
            StrategyName::ReverseLex => ReverseLex.naive_apply(algebra_tuples, trunc),
            StrategyName::Diagonal => Diagonal.naive_apply(algebra_tuples, trunc),
            StrategyName::Antidiagonal => Antidiagonal.naive_apply(algebra_tuples, trunc),
            StrategyName::Extrema => Extrema.naive_apply(algebra_tuples, trunc),
            StrategyName::Shells => Shells.naive_apply(algebra_tuples, trunc),
            StrategyName::Halton => Halton.naive_apply(algebra_tuples, trunc),
            StrategyName::Sobol => Sobol.naive_apply(algebra_tuples, trunc),
            StrategyName::Lhs => Lhs.naive_apply(algebra_tuples, trunc),
            StrategyName::Shuffle => Shuffle.naive_apply(algebra_tuples, trunc),
        };
        // Index map the ordered tuples back to RuntimeTuples
        // by matching on display-string equivalents. Cheap
        // because ordering is the last step and tuple counts
        // are small at the comprehension-iteration scale.
        let mut out = Vec::with_capacity(ordered.len());
        for ordered_tuple in ordered {
            let target_key: Vec<(String, String)> = ordered_tuple
                .bindings
                .iter()
                .map(|(n, tv)| (n.clone(), tuple_value_display(tv)))
                .collect();
            let runtime = tuples
                .iter()
                .find(|rt| {
                    let rt_key: Vec<(String, String)> = rt
                        .iter()
                        .map(|(n, v)| (n.clone(), v.to_display_string()))
                        .collect();
                    rt_key == target_key
                })
                .cloned()
                .ok_or_else(|| RuntimeError::OrderEval {
                    strategy,
                    message: "ordered tuple lost reference to runtime source".into(),
                })?;
            out.push(runtime);
        }
        Ok(out)
    }
}

fn tuple_value_display(tv: &TupleValue) -> String {
    match tv {
        TupleValue::U64(n) => n.to_string(),
        TupleValue::I64(n) => n.to_string(),
        TupleValue::F64(f) => f.to_string(),
        TupleValue::Str(s) => s.clone(),
        TupleValue::Bool(b) => b.to_string(),
    }
}

fn literal_to_polydat_value(lv: &LiteralValue) -> Value {
    use std::sync::Arc;
    match lv {
        LiteralValue::Int(n) => Value::U64(*n as u64),
        LiteralValue::Float(f) => Value::F64(*f),
        LiteralValue::String(s) => Value::Str(Arc::from(s.as_str())),
        LiteralValue::Bool(b) => Value::Bool(*b),
    }
}

fn source_display_text(source: &Source) -> Option<String> {
    match source {
        Source::Generator { expr, .. } => Some(expr.clone()),
        Source::WorkloadParamList { name, .. } => Some(format!("{{{name}}}")),
        _ => None,
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn empty_kernel() -> Arc<GkKernel> {
        Arc::new(crate::dsl::compile_gk("\n").unwrap())
    }

    /// Canonical kernel with `extern k: u64` so the runtime
    /// evaluator can install per-clause `k` values via
    /// materialize_subscope — matches the shape
    /// `build_for_each_scope_kernel` produces in production.
    fn canonical_with_k() -> Arc<GkKernel> {
        Arc::new(crate::dsl::compile_gk("extern k: u64\n").unwrap())
    }

    #[test]
    fn int_range_yields_values() {
        let comp = Comprehension::Clause {
            name: "k".into(),
            source: Source::IntRange { lo: 1, hi: 5, step: 1 },
        };
        let parent = empty_kernel();
        let canonical = empty_kernel();
        let params = HashMap::new();
        let tuples = evaluate_for_iteration(&comp, &parent, &canonical, &params, |_| Ok(()))
            .unwrap();
        assert_eq!(tuples.len(), 4);
        assert_eq!(tuples[0][0].1, Value::U64(1));
        assert_eq!(tuples[3][0].1, Value::U64(4));
    }

    #[test]
    fn literal_list_yields_values() {
        let comp = Comprehension::Clause {
            name: "x".into(),
            source: Source::Literal {
                values: vec![LiteralValue::Int(10), LiteralValue::Int(20)],
            },
        };
        let parent = empty_kernel();
        let canonical = empty_kernel();
        let params = HashMap::new();
        let tuples = evaluate_for_iteration(&comp, &parent, &canonical, &params, |_| Ok(()))
            .unwrap();
        assert_eq!(tuples.len(), 2);
    }

    #[test]
    fn cartesian_produces_product() {
        let comp = Comprehension::cartesian(vec![
            Comprehension::Clause {
                name: "x".into(),
                source: Source::IntRange { lo: 1, hi: 3, step: 1 },
            },
            Comprehension::Clause {
                name: "y".into(),
                source: Source::IntRange { lo: 10, hi: 30, step: 10 },
            },
        ]);
        let parent = empty_kernel();
        let canonical = empty_kernel();
        let params = HashMap::new();
        let tuples = evaluate_for_iteration(&comp, &parent, &canonical, &params, |_| Ok(()))
            .unwrap();
        // 2 × 2 = 4
        assert_eq!(tuples.len(), 4);
    }

    #[test]
    fn union_produces_concatenation() {
        let comp = Comprehension::union(vec![
            Comprehension::Clause {
                name: "k".into(),
                source: Source::Literal { values: vec![LiteralValue::Int(1)] },
            },
            Comprehension::Clause {
                name: "k".into(),
                source: Source::Literal {
                    values: vec![LiteralValue::Int(10), LiteralValue::Int(20)],
                },
            },
        ]);
        let parent = empty_kernel();
        let canonical = empty_kernel();
        let params = HashMap::new();
        let tuples = evaluate_for_iteration(&comp, &parent, &canonical, &params, |_| Ok(()))
            .unwrap();
        assert_eq!(tuples.len(), 3);
    }

    #[test]
    fn filter_drops_non_matching() {
        let comp = Comprehension::filter(
            Comprehension::Clause {
                name: "k".into(),
                source: Source::IntRange { lo: 1, hi: 6, step: 1 },
            },
            "{k} > 3",
        );
        let parent = empty_kernel();
        let canonical = canonical_with_k();
        let params = HashMap::new();
        let tuples = evaluate_for_iteration(&comp, &parent, &canonical, &params, |_| Ok(()))
            .unwrap();
        // 1..6 = [1,2,3,4,5]; filter > 3 keeps [4, 5]
        assert_eq!(tuples.len(), 2);
    }

    #[test]
    fn order_lex_truncate() {
        let comp = Comprehension::order(
            Comprehension::Clause {
                name: "k".into(),
                source: Source::IntRange { lo: 1, hi: 100, step: 1 },
            },
            StrategyName::Lex,
            Some(5),
        );
        let parent = empty_kernel();
        let canonical = empty_kernel();
        let params = HashMap::new();
        let tuples = evaluate_for_iteration(&comp, &parent, &canonical, &params, |_| Ok(()))
            .unwrap();
        assert_eq!(tuples.len(), 5);
    }
}
