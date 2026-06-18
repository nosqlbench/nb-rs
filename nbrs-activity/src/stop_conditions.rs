// Copyright (c) nosqlbench
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! SRD-83 — stop conditions. **Steps 1–2: runtime-state wires + the
//! scoped predicate.**
//!
//! A stop-condition predicate (SRD-83 Part 2) reads a shell's live
//! state — how many ops ran, how many errored, how long it's been
//! going. Those values are exposed as **volatile externs**; the
//! executor injects a [`RuntimeState`] snapshot before each evaluation.
//!
//! The predicate is compiled (`compile_stop_condition`) as an SRD-84
//! **shape 2** [`ScopedExpr`] bound to the shell's phase kernel — a
//! scope-bound, callable expression over the runtime-state externs —
//! *not* baked into the phase kernel's own matter. The firing events /
//! settle daemon (step 3) drive [`RuntimeState::trips`] per trigger.

use polydat::ast::Value;
use polydat::dsl::stub::{ExprStub, GraphMatter, ScopedExpr};
use polydat::kernel::{Dataflow, PolydatKernel};

use crate::phase_outcome::Outcome;

/// Canonical runtime-state wire names a stop-condition predicate may
/// read. A predicate references the ones it needs; the rest are absent
/// from its kernel and skipped at injection.
pub mod wire {
    /// Ops dispatched / cycles completed so far (`u64`).
    pub const OP_COUNT: &str = "op_count";
    /// Errors recorded so far (`u64`).
    pub const ERROR_COUNT: &str = "error_count";
    /// Errored fraction of ops, `error_count / op_count` (`f64`).
    pub const ERROR_RATE: &str = "error_rate";
    /// Wall time since the shell started (`u64`, milliseconds).
    pub const ELAPSED_MS: &str = "elapsed_ms";
    /// Scenario shells — child phases declared / failed / finished.
    pub const CHILDREN_TOTAL: &str = "children_total";
    pub const CHILDREN_FAILED: &str = "children_failed";
    pub const CHILDREN_DONE: &str = "children_done";
}

/// A snapshot of a shell's runtime state, injected into its
/// stop-condition predicate kernel before each evaluation. SRD-83
/// Part 2. Cheap to copy; the executor builds one from the live
/// activity counters (`cycles_completed` / `errors_total`) plus the
/// phase clock, and from child outcomes for a scenario shell.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RuntimeState {
    pub op_count: u64,
    pub error_count: u64,
    pub elapsed_ms: u64,
    pub children_total: u64,
    pub children_failed: u64,
    pub children_done: u64,
}

impl RuntimeState {
    /// Errored fraction of ops; `0.0` before any op completes (so a
    /// `error_rate > X` predicate never trips on an empty phase).
    pub fn error_rate(&self) -> f64 {
        if self.op_count == 0 {
            0.0
        } else {
            self.error_count as f64 / self.op_count as f64
        }
    }

    /// The `(wire, value)` pairs this snapshot supplies. The single
    /// source for the wire→`Value` mapping, so the injector and any
    /// extern-declaration list stay in agreement.
    fn wires(&self) -> [(&'static str, Value); 7] {
        [
            (wire::OP_COUNT, Value::U64(self.op_count)),
            (wire::ERROR_COUNT, Value::U64(self.error_count)),
            (wire::ERROR_RATE, Value::F64(self.error_rate())),
            (wire::ELAPSED_MS, Value::U64(self.elapsed_ms)),
            (wire::CHILDREN_TOTAL, Value::U64(self.children_total)),
            (wire::CHILDREN_FAILED, Value::U64(self.children_failed)),
            (wire::CHILDREN_DONE, Value::U64(self.children_done)),
        ]
    }

    /// Inject this snapshot into a predicate kernel: for each runtime-
    /// state wire the program declares as an input, write the current
    /// value. Wires the predicate doesn't reference are absent
    /// (`find_input` → `None`) and skipped; a type-mismatch on a
    /// mis-declared extern is swallowed (the step-2 predicate compile
    /// declares the externs with matching types).
    pub fn inject_into<D: Dataflow>(&self, ctx: &mut D) {
        for (name, value) in self.wires() {
            if let Some(idx) = ctx.find_input(name) {
                let _ = ctx.set_wire_idx(idx, value);
            }
        }
    }

    /// Evaluate a stop-condition predicate against this snapshot:
    /// inject the runtime-state wires, then read the predicate's
    /// truthiness. `true` means the condition has tripped.
    pub fn trips(&self, condition: &mut ScopedExpr) -> bool {
        self.inject_into(condition.dataflow());
        condition.is_true()
    }
}

/// Graph matter (SRD-84 shape 1) declaring the runtime-state wires as
/// typed externs, in a stable order with types matching
/// [`RuntimeState::wires`]. Built — not string-parsed — so a
/// stop-condition predicate compiled over it can reference any wire;
/// `inject_into` / `trips` then populate the ones it actually uses.
pub fn extern_matter() -> GraphMatter {
    let mut m = GraphMatter::new();
    m.extern_wire::<u64>(wire::OP_COUNT)
        .extern_wire::<u64>(wire::ERROR_COUNT)
        .extern_wire::<f64>(wire::ERROR_RATE)
        .extern_wire::<u64>(wire::ELAPSED_MS)
        .extern_wire::<u64>(wire::CHILDREN_TOTAL)
        .extern_wire::<u64>(wire::CHILDREN_FAILED)
        .extern_wire::<u64>(wire::CHILDREN_DONE);
    m
}

/// Compile a stop-condition predicate (SRD-83 Part 2) as an SRD-84
/// **shape 2** [`ScopedExpr`] bound to the shell's `phase_kernel`: a
/// `volatile` expression living in that kernel's lexical scope, over
/// the runtime-state externs, coerced to `u64` truthiness. The executor
/// (step 3) evaluates it per trigger via [`RuntimeState::trips`].
///
/// The predicate is *not* baked into the phase kernel's own matter — it
/// is a separate sub-context, so authored phase bindings and evaluated
/// stop predicates stay orthogonal concerns.
pub fn compile_stop_condition(
    phase_kernel: &PolydatKernel,
    idx: usize,
    when: &str,
) -> Result<ScopedExpr, String> {
    let name = format!("__stop_cond_{idx}");
    let mut matter = extern_matter();
    matter.bind(
        ExprStub::parse(&name, when)
            .map_err(|e| format!("stop condition {idx} predicate `{when}`: {e}"))?
            .returning::<u64>()
            .volatile(),
    );
    ScopedExpr::bind(phase_kernel, name, matter)
        .map_err(|e| format!("stop condition {idx} predicate `{when}`: {e}"))
}

/// A declared stop condition resolved to its predicate text and its
/// SRD-83 Part 5 `effect` — the two-axis [`Outcome`] the shell adopts
/// when the predicate trips. Built from a `StopConditionSpec` at the
/// gather sites (executor / runner), so this module need not depend on
/// the workload model's spec shape.
#[derive(Debug, Clone)]
pub struct StopConditionDecl {
    /// The polydat predicate over runtime-state wires.
    pub when: String,
    /// The Outcome the shell adopts when `when` trips.
    pub effect: Outcome,
}

impl StopConditionDecl {
    /// Map an SRD-83 `effect:` string to its [`Outcome`]. `"stop"` is a
    /// clean halt (`Interrupted + Succeeded`, keep the partial result);
    /// `"fail"` is the failure halt (`Interrupted + Failed`). `None` —
    /// and any unrecognized value — resolves to `default`, which the
    /// caller sets per shell level (phase trips default to `fail`,
    /// workload trips default to a graceful `stop`). Effect-string
    /// validation proper belongs to workload load (dryrun).
    pub fn effect_from_str(effect: Option<&str>, default: Outcome) -> Outcome {
        match effect {
            Some("stop") => Outcome::interrupted(),
            Some("fail") => Outcome::failed(),
            _ => default,
        }
    }
}

/// A shell's compiled stop conditions (SRD-83 steps 3–5): the default
/// `error_rate > error_rate_max` condition (when a max is set) plus each
/// phase-declared predicate, every one a scope-bound [`ScopedExpr`].
/// Built once when the shell's kernel exists; evaluated per firing event
/// against a [`RuntimeState`] snapshot. This is the polydat-predicate
/// successor to the interim `AggregateGuard`.
pub struct StopConditionSet {
    conditions: Vec<StopCondition>,
}

struct StopCondition {
    expr: ScopedExpr,
    /// SRD-83 Part 5 — the two-axis Outcome the shell adopts when this
    /// condition trips. `fail` → Interrupted+Failed; `stop` →
    /// Interrupted+Succeeded.
    effect: Outcome,
    /// The error/reason class recorded when this condition trips.
    reason: String,
}

impl StopConditionSet {
    /// Build the set for a shell whose kernel is `phase_kernel`. The
    /// default error-rate condition (if `error_rate_max` is set) is
    /// installed first, then each declared `when` predicate. A predicate
    /// that fails to compile is a hard error (surfaced at build time,
    /// not swallowed per [`crate::feedback`] "never ignore silently").
    pub fn build_for_phase(
        phase_kernel: &PolydatKernel,
        error_rate_max: Option<f64>,
        declared: &[StopConditionDecl],
    ) -> Result<Self, String> {
        let mut conditions = Vec::new();
        let mut idx = 0;
        if let Some(max) = error_rate_max {
            // Preserve the interim `AggregateGuard`'s 50-op floor: don't
            // judge the error rate until enough ops have run for it to be
            // meaningful (a single early error must not abort the phase).
            // The rate breach is a `fail` effect — the result is not
            // trustworthy once the error rate is over the bound.
            let expr = compile_stop_condition(
                phase_kernel, idx, &format!("op_count >= 50 && error_rate > {max}"))?;
            conditions.push(StopCondition {
                expr,
                effect: Outcome::failed(),
                reason: "error_rate_exceeded".into(),
            });
            idx += 1;
        }
        for decl in declared {
            let expr = compile_stop_condition(phase_kernel, idx, &decl.when)?;
            conditions.push(StopCondition {
                expr,
                effect: decl.effect,
                reason: format!("stop_condition: {}", decl.when),
            });
            idx += 1;
        }
        Ok(Self { conditions })
    }

    /// An installed-nothing set (used as the safe fallback when a
    /// predicate fails to compile at runtime).
    pub fn empty() -> Self {
        Self { conditions: Vec::new() }
    }

    /// True when no condition is installed (skip evaluation entirely).
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    /// Evaluate every condition against `state`; return the error class
    /// of the first that trips, or `None`. (First-match: any trip stops
    /// the shell, so the order only affects which reason is recorded.)
    pub fn evaluate(&mut self, state: &RuntimeState) -> Option<(Outcome, String)> {
        for cond in &mut self.conditions {
            if state.trips(&mut cond.expr) {
                return Some((cond.effect, cond.reason.clone()));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_rate_is_safe_at_zero_ops() {
        assert_eq!(RuntimeState::default().error_rate(), 0.0);
        let s = RuntimeState { op_count: 100, error_count: 10, ..Default::default() };
        assert_eq!(s.error_rate(), 0.1);
        let s = RuntimeState { op_count: 50, error_count: 50, ..Default::default() };
        assert_eq!(s.error_rate(), 1.0);
    }

    #[test]
    fn injects_referenced_wires_by_name_and_re_evaluates() {
        // A predicate that reads two runtime-state wires; the injector
        // must reach exactly those, by name. `volatile` forces a fresh
        // evaluation on each pull (as a stop-condition predicate does
        // per trigger).
        let src = "\
            extern op_count: u64 = 0\n\
            extern error_count: u64 = 0\n\
            volatile sum := op_count + error_count";
        let mut k = polydat::dsl::compile_polydat(src).expect("compile predicate kernel");

        RuntimeState { op_count: 10, error_count: 5, ..Default::default() }
            .inject_into(&mut k);
        assert_eq!(*k.pull("sum"), Value::U64(15));

        RuntimeState { op_count: 40, error_count: 2, ..Default::default() }
            .inject_into(&mut k);
        assert_eq!(*k.pull("sum"), Value::U64(42));
    }

    #[test]
    fn compiles_and_trips_a_scoped_stop_condition() {
        // SRD-83 Part 2 re-pointed onto SRD-84 shape 2: the predicate is
        // a `ScopedExpr` bound to the phase kernel, evaluated per trigger
        // against an injected runtime-state snapshot — never baked into
        // the phase matter.
        let phase_kernel = polydat::dsl::compile_polydat("input cycle: u64\nx := 5")
            .expect("phase kernel");
        let mut cond = compile_stop_condition(
            &phase_kernel, 0, "op_count > 50 && error_rate > 0.1")
            .expect("compile scoped stop condition");
        // op_count under threshold → does not trip (error_rate is 0.5
        // here, but the `&&` short of op_count > 50 fails).
        assert!(!RuntimeState { op_count: 40, error_count: 20, ..Default::default() }
            .trips(&mut cond));
        // op_count 100 (> 50) and error_rate 0.2 (> 0.1) → trips.
        assert!(RuntimeState { op_count: 100, error_count: 20, ..Default::default() }
            .trips(&mut cond));
        // op_count over but error_rate 0.01 under → does not trip.
        assert!(!RuntimeState { op_count: 100, error_count: 1, ..Default::default() }
            .trips(&mut cond));
    }

    #[test]
    fn stop_condition_set_installs_default_error_rate_and_declared_predicates() {
        let phase_kernel = polydat::dsl::compile_polydat("input cycle: u64\nx := 5")
            .expect("phase kernel");
        // Default error-rate (0.1) + a declared op-count predicate.
        let mut set = StopConditionSet::build_for_phase(
            &phase_kernel, Some(0.1),
            &[StopConditionDecl { when: "op_count > 1000".to_string(), effect: Outcome::failed() }])
            .expect("build set");
        // The compiled set is independent of the parent kernel — drop it
        // and evaluation still works (each predicate is its own
        // sub-context). This is what lets the activity bind against a
        // throwaway root until the phase kernel is plumbed.
        drop(phase_kernel);
        assert!(!set.is_empty());
        // Below the 50-op floor: even 100% errors does not trip yet.
        assert!(set.evaluate(
            &RuntimeState { op_count: 10, error_count: 10, ..Default::default() }).is_none());
        // 5% errors, 100 ops → neither trips.
        assert!(set.evaluate(
            &RuntimeState { op_count: 100, error_count: 5, ..Default::default() }).is_none());
        // 20% errors → the default error-rate condition trips.
        assert_eq!(
            set.evaluate(&RuntimeState { op_count: 100, error_count: 20, ..Default::default() }),
            Some((Outcome::failed(), "error_rate_exceeded".to_string())));
        // Low errors but op_count over 1000 → the declared predicate trips.
        assert_eq!(
            set.evaluate(&RuntimeState { op_count: 2000, error_count: 1, ..Default::default() }),
            Some((Outcome::failed(), "stop_condition: op_count > 1000".to_string())));
    }
}
