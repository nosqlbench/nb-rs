// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-86 — the optimizer seam on the runtime side.
//!
//! The **contract** (the `Optimizer`/`Objective` traits, the search-space
//! types, the registry) lives in [`contract`] — defined in the core with
//! no dependency on any optimizer-algorithm crate. Algorithm crates (e.g.
//! `nbrs-optimizers`) register implementations via `inventory` and are
//! discovered through [`contract::by_name`] / [`contract::describe`] at
//! link time; the core never names them.
//!
//! This module is the *runtime half* of the seam: it maps a runtime
//! [`OptimizeSpec`] to a [`SearchSpace`], and provides [`PolydatObjective`]
//! — a [`contract::Objective`] that evaluates the objective at a coordinate
//! by binding the axis values into a phase kernel's externs and pulling the
//! objective wire (the same SRD-40b synthetic-metric evaluation). This is
//! the `Coordinate`-class realization (SRD-86 §3); the `Control`/`Fixture`
//! realizations and the workload `optimize:` configuration surface +
//! executor dispatch are staged (SRD-86 §Staging). [`run_optimization`] is
//! the entry the executor dispatch will call.

// WIP, not dead: the executor `optimize:` dispatch that calls
// `run_optimization` is staged (along with the workload config surface and
// the Control/Fixture realizations), so these seam items are exercised only
// by this module's and the binary's integration tests for now.
#![allow(dead_code)]

pub mod contract;
pub mod phase_pulse;
pub mod settle;

pub use contract::{
    by_name, describe, registered_names, Axis, AxisImpact, AxisKind, AxisValue, Budget, Changeover,
    Coord, CoordinateSource, FeedbackSource, LexSource, NullOptimizer, Objective, Observation,
    Optimizer, OptimizerInfo, OptimizerParams, OptimizerRegistration, PullOnly, PullSource, Report,
    SearchSpace, StopReason,
};

use polydat::ast::Value;
use polydat::kernel::{Dataflow, Metadata, PolydatKernel};

use crate::phase_outcome::Outcome;

/// The domain of one optimization axis (runtime form, pre-`SearchSpace`).
#[derive(Debug, Clone)]
pub enum AxisDomain {
    Continuous { lo: f64, hi: f64, min_step: f64 },
    Discrete { detents: Vec<AxisValue> },
    Categorical { options: Vec<AxisValue> },
}

/// One axis of the runtime optimize spec.
#[derive(Debug, Clone)]
pub struct AxisSpec {
    pub name: String,
    pub domain: AxisDomain,
    pub changeover: Changeover,
}

/// The runtime optimize configuration for one objective phase. The executor
/// builds this from the (future) `optimize:` block; tests build it directly.
#[derive(Debug, Clone)]
pub struct OptimizeSpec {
    /// Registered optimizer name (`null`, `centroid_variant`, `cmaes`, …).
    pub method: String,
    /// The wire whose value the optimizer maximizes (the objective).
    pub objective_wire: String,
    pub axes: Vec<AxisSpec>,
    pub max_evals: usize,
    pub seed: u64,
    pub params: OptimizerParams,
}

impl OptimizeSpec {
    /// Project the axes onto a [`SearchSpace`].
    pub fn search_space(&self) -> SearchSpace {
        SearchSpace::new(
            self.axes
                .iter()
                .map(|a| {
                    let kind = match &a.domain {
                        AxisDomain::Continuous { lo, hi, min_step } => {
                            AxisKind::Continuous { lo: *lo, hi: *hi, min_step: *min_step }
                        }
                        AxisDomain::Discrete { detents } => {
                            AxisKind::Discrete { detents: detents.clone() }
                        }
                        AxisDomain::Categorical { options } => {
                            AxisKind::Categorical { options: options.clone() }
                        }
                    };
                    Axis { name: a.name.clone(), kind, changeover: a.changeover }
                })
                .collect(),
        )
    }

    pub fn budget(&self) -> Budget {
        Budget::seeded(self.max_evals, self.seed)
    }

    fn axis_names(&self) -> Vec<String> {
        self.axes.iter().map(|a| a.name.clone()).collect()
    }
}

/// Materialize an [`AxisValue`] coordinate into the polydat [`Value`] bound
/// into the kernel as the axis's input (labels → `Str`, bools → `Bool`).
fn axis_value_to_polydat(v: &AxisValue) -> Value {
    match v {
        AxisValue::Num(f) => Value::F64(*f),
        AxisValue::Bool(b) => Value::Bool(*b),
        AxisValue::Label(s) => Value::Str(s.as_str().into()),
    }
}

/// Project a polydat [`Value`] onto the `f64` the optimizer maximizes.
fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::F64(f) => Some(*f),
        Value::U64(u) => Some(*u as f64),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// Bridges a polydat phase kernel to [`contract::Objective`]: each query
/// binds the axis coordinate into the kernel's externs and pulls the
/// objective wire (the SRD-40b synthetic-metric evaluation). The objective
/// binding should be `volatile` so each pull re-evaluates.
pub struct PolydatObjective<'a> {
    kernel: &'a mut PolydatKernel,
    axis_names: Vec<String>,
    objective_wire: String,
}

impl<'a> PolydatObjective<'a> {
    pub fn new(
        kernel: &'a mut PolydatKernel,
        axis_names: Vec<String>,
        objective_wire: impl Into<String>,
    ) -> Self {
        Self { kernel, axis_names, objective_wire: objective_wire.into() }
    }
}

impl Objective for PolydatObjective<'_> {
    fn query(&mut self, x: &[AxisValue]) -> Observation {
        for (i, name) in self.axis_names.iter().enumerate() {
            if let Some(idx) = self.kernel.find_input(name) {
                let _ = self.kernel.set_wire_idx(idx, axis_value_to_polydat(&x[i]));
            }
        }
        match value_to_f64(self.kernel.pull(&self.objective_wire)) {
            Some(v) if v.is_finite() => Observation::value(v),
            _ => Observation::infeasible(),
        }
    }
}

/// Map a [`StopReason`] to the SRD-83/-86 two-axis [`Outcome`]. A search
/// that converged / exhausted its budget / was aborted is a clean stop with
/// a usable best (`Interrupted+Succeeded`); a search that never found a
/// feasible point is a failure (`Interrupted+Failed`).
pub fn outcome_for(stop: StopReason) -> Outcome {
    match stop {
        StopReason::Converged | StopReason::BudgetExhausted | StopReason::Aborted => {
            Outcome::interrupted()
        }
        StopReason::NoFeasiblePoint => Outcome::failed(),
    }
}

/// Run the optimizer named by `spec` against the phase `kernel`, returning
/// the [`Report`] and the search-level [`Outcome`]. Discovers the optimizer
/// via the link-time registry ([`contract::by_name`]). This is the entry the
/// executor's `optimize:` dispatch will call (SRD-86 §Staging).
pub fn run_optimization(
    spec: &OptimizeSpec,
    kernel: &mut PolydatKernel,
) -> Result<(Report, Outcome), String> {
    let space = spec.search_space();
    let optimizer = by_name(&spec.method, &spec.params)
        .ok_or_else(|| format!("unknown optimizer method: {}", spec.method))?;
    let budget = spec.budget();
    let report = {
        let mut obj = PolydatObjective::new(kernel, spec.axis_names(), spec.objective_wire.clone());
        optimizer.optimize(&space, &mut obj, &budget)
    };
    let outcome = outcome_for(report.stop);
    Ok((report, outcome))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build numeric (`Num`) coordinate values from raw `f64`s.
    fn nums(xs: &[f64]) -> Vec<AxisValue> {
        xs.iter().map(|f| AxisValue::Num(*f)).collect()
    }

    /// A polydat phase kernel whose `objective` wire is a negated paraboloid
    /// maximized (value 0) at the interior point `(3, 5)`.
    fn manifold_kernel() -> PolydatKernel {
        let src = "extern x: f64 = 0.0\n\
                   extern y: f64 = 0.0\n\
                   volatile objective := 0.0 - ((x - 3.0) * (x - 3.0) + (y - 5.0) * (y - 5.0))";
        polydat::dsl::compile_polydat(src).expect("compile manifold kernel")
    }

    #[test]
    fn polydat_objective_is_a_function_of_the_coordinate() {
        let mut k = manifold_kernel();
        let mut obj = PolydatObjective::new(&mut k, vec!["x".into(), "y".into()], "objective");
        assert!((obj.query(&nums(&[3.0, 5.0])).value - 0.0).abs() < 1e-9);
        assert!((obj.query(&nums(&[0.0, 0.0])).value - (-(9.0 + 25.0))).abs() < 1e-9);
        assert!((obj.query(&nums(&[3.0, 5.0])).value).abs() < 1e-9);
    }

    /// The built-in `null` optimizer drives the seam over a discrete grid
    /// containing the optimum — proving the runtime half end-to-end through
    /// real polydat evaluation. (The plugin optimizers are tested where they
    /// are linked — `nbrs/tests`.)
    fn discrete_spec(method: &str) -> OptimizeSpec {
        OptimizeSpec {
            method: method.into(),
            objective_wire: "objective".into(),
            axes: vec![
                AxisSpec {
                    name: "x".into(),
                    domain: AxisDomain::Discrete { detents: nums(&[1.0, 2.0, 3.0, 4.0, 5.0]) },
                    changeover: Changeover::Fixture,
                },
                AxisSpec {
                    name: "y".into(),
                    domain: AxisDomain::Discrete { detents: nums(&[3.0, 4.0, 5.0, 6.0, 7.0]) },
                    changeover: Changeover::Control,
                },
            ],
            max_evals: 100,
            seed: 1,
            params: OptimizerParams::new(),
        }
    }

    #[test]
    fn null_finds_grid_optimum_through_the_seam() {
        let mut k = manifold_kernel();
        let (report, outcome) = run_optimization(&discrete_spec("null"), &mut k).expect("run");
        assert_eq!(report.best, nums(&[3.0, 5.0]));
        assert!(report.best_value.abs() < 1e-9);
        assert!(!outcome.is_failure());
    }

    #[test]
    fn unknown_method_is_rejected() {
        let mut k = manifold_kernel();
        assert!(run_optimization(&discrete_spec("no_such_optimizer"), &mut k).is_err());
    }
}
