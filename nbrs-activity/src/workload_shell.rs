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

//! SRD-83 — the workload execution shell.
//!
//! The workload is the outermost execution shell (SRD-82): its
//! children are the phases the scenario walk runs. This module holds
//! the shell's live aggregate — how many child phases were declared,
//! how many failed, and how many finished, plus the running op / error
//! totals across them — and the shell's compiled stop conditions
//! ([`StopConditionSet`]).
//!
//! One [`WorkloadShell`] exists per run, shared (`Arc`) across every
//! cloned [`crate::executor::ExecCtx`] task, so a phase finishing
//! anywhere in the scenario tree feeds the *same* accumulator. As each
//! phase produces its [`crate::phase_outcome::PhaseOutcome`] the
//! executor calls [`WorkloadShell::record_phase`], which folds the
//! outcome into the [`RuntimeState`] wires (`children_*`, `op_count`,
//! `error_count`) and evaluates the shell's stop conditions against the
//! new snapshot. The first trip latches `walk_stop`; every dispatch
//! loop consults [`WorkloadShell::should_stop`] before starting the
//! next sibling and halts the remaining walk on a latch — the scenario
//! stop-on-error default expressed as a stop condition.
//!
//! The two-axis `Outcome` effect mapping (a `fail`-effect trip vs a
//! `stop`-effect trip) is the SRD-83 step-4 follow-up; today a trip
//! halts the walk and records its reason, and the session-level
//! `Validity` is carried by the failing phase's own `Err` (the existing
//! `run_siblings_concurrently` cascade) rather than re-derived here.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use crate::stop_conditions::{RuntimeState, StopConditionSet};

/// The workload shell's live aggregate plus its compiled stop
/// conditions. See the module docs for the shared-`Arc` lifecycle.
pub struct WorkloadShell {
    /// Child phases that produced an outcome (failed + done).
    children_total: AtomicU64,
    /// Child phases whose outcome was `Failed`.
    children_failed: AtomicU64,
    /// Child phases whose outcome was `Completed`.
    children_done: AtomicU64,
    /// Ops dispatched across every child phase so far.
    op_count: AtomicU64,
    /// Errors recorded across every child phase so far.
    error_count: AtomicU64,
    /// The workload's stop conditions, built once against the
    /// `ScopeKind::Workload` node's cached kernel.
    ///
    /// `Mutex` because [`StopConditionSet::evaluate`] needs `&mut`
    /// (each predicate is a `ScopedExpr` that re-evaluates in place)
    /// and the set is shared across concurrent phase-finisher tasks.
    /// The lock is taken, the predicates evaluated, and the lock
    /// released within [`Self::evaluate`] — never held across an
    /// `.await` (cf. `feedback_no_blocking_in_async`).
    stop_set: Mutex<StopConditionSet>,
    /// Latched `true` by the first condition to trip. Read by every
    /// dispatch loop before starting the next sibling.
    walk_stop: AtomicBool,
    /// The reason recorded when `walk_stop` latched (the tripping
    /// condition's error class), for diagnostics.
    stop_reason: Mutex<Option<String>>,
    /// Wall clock the shell started — supplies the `elapsed_ms` wire
    /// at the workload level.
    start: Instant,
}

impl WorkloadShell {
    /// Build a shell with the given (already-compiled) stop-condition
    /// set. The accumulator starts empty and the wall clock starts now.
    pub fn new(stop_set: StopConditionSet) -> Self {
        Self {
            children_total: AtomicU64::new(0),
            children_failed: AtomicU64::new(0),
            children_done: AtomicU64::new(0),
            op_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            stop_set: Mutex::new(stop_set),
            walk_stop: AtomicBool::new(false),
            stop_reason: Mutex::new(None),
            start: Instant::now(),
        }
    }

    /// A shell with no stop conditions (the common case until a
    /// workload declares `stop_when:`). Its `record_phase` still folds
    /// outcomes into the aggregate but never trips.
    #[allow(dead_code)] // WIP: SRD-83 stop-condition shell — the no-stop-conditions constructor
    pub fn inert() -> Self {
        Self::new(StopConditionSet::empty())
    }

    /// Fold one finished child phase's outcome into the workload
    /// aggregate, then evaluate the stop conditions against the new
    /// runtime state. Returns the tripping condition's reason iff *this*
    /// call latched the stop (so the caller logs it exactly once);
    /// `None` otherwise (no trip, or the stop was already latched).
    pub fn record_phase(&self, failed: bool, ops: u64, errors: u64) -> Option<String> {
        self.children_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.children_failed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.children_done.fetch_add(1, Ordering::Relaxed);
        }
        self.op_count.fetch_add(ops, Ordering::Relaxed);
        self.error_count.fetch_add(errors, Ordering::Relaxed);
        self.evaluate()
    }

    /// The current aggregate as a [`RuntimeState`] snapshot.
    fn snapshot(&self) -> RuntimeState {
        RuntimeState {
            op_count: self.op_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            elapsed_ms: self.start.elapsed().as_millis() as u64,
            children_total: self.children_total.load(Ordering::Relaxed),
            children_failed: self.children_failed.load(Ordering::Relaxed),
            children_done: self.children_done.load(Ordering::Relaxed),
        }
    }

    /// Evaluate the stop conditions against the current snapshot,
    /// latching `walk_stop` on the first trip. Serialised by the
    /// `stop_set` mutex: at most one finisher evaluates at a time, so
    /// the latch + reason are set exactly once.
    fn evaluate(&self) -> Option<String> {
        let mut set = self.stop_set.lock().ok()?;
        // Already stopped, or nothing to evaluate.
        if set.is_empty() || self.walk_stop.load(Ordering::Relaxed) {
            return None;
        }
        let state = self.snapshot();
        let reason = set.evaluate(&state)?;
        self.walk_stop.store(true, Ordering::Relaxed);
        if let Ok(mut slot) = self.stop_reason.lock() {
            *slot = Some(reason.clone());
        }
        Some(reason)
    }

    /// Whether a stop condition has latched. Consulted by the walker
    /// before dispatching each sibling — `true` halts the remaining
    /// walk.
    pub fn should_stop(&self) -> bool {
        self.walk_stop.load(Ordering::Relaxed)
    }

    /// The reason the shell stopped, if it has.
    #[allow(dead_code)] // WIP: SRD-83 stop-condition shell — reason readout
    pub fn stop_reason(&self) -> Option<String> {
        self.stop_reason.lock().ok().and_then(|g| g.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A shell over a kernel-bound `children_failed > 0` condition
    /// (the stop-on-error default expressed as a stop condition):
    /// the first failed child latches the walk-stop, and the reason is
    /// the declared predicate.
    #[test]
    fn stop_on_error_latches_on_first_failed_child() {
        let root = polydat::dsl::compile_polydat("input cycle: u64\nx := 5")
            .expect("root kernel");
        let set = StopConditionSet::build_for_phase(
            &root, None, &["children_failed > 0".to_string()])
            .expect("build set");
        let shell = WorkloadShell::new(set);

        // A successful child does not trip.
        assert_eq!(shell.record_phase(false, 100, 0), None);
        assert!(!shell.should_stop());
        // The first failed child latches the stop, returning the reason.
        assert_eq!(
            shell.record_phase(true, 50, 50),
            Some("stop_condition: children_failed > 0".to_string()));
        assert!(shell.should_stop());
        assert_eq!(shell.stop_reason().as_deref(),
            Some("stop_condition: children_failed > 0"));
        // A subsequent finisher sees the latch and reports no fresh trip.
        assert_eq!(shell.record_phase(true, 10, 10), None);
        assert!(shell.should_stop());
    }

    /// An aggregate predicate over the running op total trips only once
    /// the cumulative count crosses the threshold — proving the
    /// accumulator folds across phases, not per-phase.
    #[test]
    fn aggregate_op_count_trips_across_phases() {
        let root = polydat::dsl::compile_polydat("input cycle: u64")
            .expect("root kernel");
        let set = StopConditionSet::build_for_phase(
            &root, None, &["op_count > 1000".to_string()])
            .expect("build set");
        let shell = WorkloadShell::new(set);

        assert_eq!(shell.record_phase(false, 600, 0), None);
        assert!(!shell.should_stop());
        // Cumulative op_count is now 1200 > 1000 → trips.
        assert_eq!(
            shell.record_phase(false, 600, 0),
            Some("stop_condition: op_count > 1000".to_string()));
        assert!(shell.should_stop());
    }

    /// An inert shell (no stop conditions) accumulates outcomes but
    /// never latches.
    #[test]
    fn inert_shell_never_stops() {
        let shell = WorkloadShell::inert();
        assert_eq!(shell.record_phase(true, 10, 10), None);
        assert_eq!(shell.record_phase(true, 10, 10), None);
        assert!(!shell.should_stop());
        assert_eq!(shell.stop_reason(), None);
    }
}
