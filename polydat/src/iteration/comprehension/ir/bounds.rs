// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! §9.3 resource-bound checker.
//!
//! Given an IR `Program`, compute the closed-form peak-memory
//! bound symbolically. Per spec §9.3:
//!
//! ```text
//! memory(C) ≤
//!     O(depth(C))                                 // operator stack
//!   + Σ (per-operator steady-state, see §6.2)     // O(1) for streaming ops
//!   + Σ (zip(Cycle) shorter-child cardinality)    // barrier 1
//!   + Σ (ORDER_MATERIALIZE working-set size)      // barrier 2
//! ```
//!
//! This checker emits a [`ResourceBound`] structure rather than
//! a single number — consumers (TUI, planner diagnostics, the
//! optimizer's bounds-improvement test) get separated terms.

use serde::{Deserialize, Serialize};

use super::op::Op;
use super::program::Program;
use crate::iteration::comprehension::strategy::ZipMode;

/// Closed-form peak-memory estimate for an IR `Program`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceBound {
    /// Operator-stack depth term (O(depth(C))).
    pub stack_depth: usize,

    /// Sum of streaming-operator steady-state (O(1) per op
    /// above its arity). Reported as an opcode count so the
    /// caller can multiply by their per-op cost estimate.
    pub streaming_op_count: usize,

    /// Per-barrier working-set bound. Each entry is one
    /// barrier in the program; the total memory cost is the
    /// sum.
    pub barriers: Vec<Bound>,
}

/// A single barrier's working-set bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bound {
    /// Which IR position the barrier sits at.
    pub op_index: usize,
    /// Symbolic description of the barrier (e.g.,
    /// `"ORDER_MATERIALIZE(Halton, indexed=true, n=50)"`).
    pub description: String,
    /// Closed-form working-set size in tuples. `None` when
    /// the size depends on a runtime cardinality (the
    /// optimizer's input is assumed to be V6-rejected if any
    /// barrier has unbounded input, so `None` should not occur
    /// for well-formed programs).
    pub working_set_size: Option<u64>,
}

impl ResourceBound {
    /// Sum the per-barrier working sets. Returns `None` when
    /// any barrier has an unbounded working set.
    pub fn total_barrier_working_set(&self) -> Option<u64> {
        let mut total: u64 = 0;
        for b in &self.barriers {
            let w = b.working_set_size?;
            total = total.saturating_add(w);
        }
        Some(total)
    }
}

/// Compute the resource bound for a program.
pub fn check_bounds(program: &Program) -> ResourceBound {
    let mut bounds = ResourceBound {
        stack_depth: program.stack_depth(),
        streaming_op_count: 0,
        barriers: Vec::new(),
    };

    for (i, op) in program.ops().iter().enumerate() {
        if op.is_barrier() {
            bounds.barriers.push(barrier_for(i, op));
        } else if !matches!(op, Op::Dispense) {
            bounds.streaming_op_count += 1;
        }
    }

    bounds
}

fn barrier_for(op_index: usize, op: &Op) -> Bound {
    match op {
        Op::OrderMaterialize {
            strategy,
            truncation,
            indexed,
            ..
        } => {
            // R2 push-down: working set = truncation count.
            // Naïve form: unknown without input cardinality —
            // reported as the truncation cap (conservative
            // lower-bound; actual is input cardinality).
            let ws = *truncation;
            let description = format!(
                "ORDER_MATERIALIZE({}, indexed={indexed}, truncation={truncation:?})",
                strategy.as_str()
            );
            Bound {
                op_index,
                description,
                working_set_size: ws,
            }
        }
        Op::Zip {
            n,
            mode: ZipMode::Cycle,
        } => {
            // zip(Cycle) shorter-child barrier: working set =
            // sum of non-longest child cardinalities. Without
            // child cardinalities at this layer, report
            // None — the metadata propagator carries the actual
            // computed working set per spec §10.7.2.
            Bound {
                op_index,
                description: format!("ZIP(Cycle, {n})"),
                working_set_size: None,
            }
        }
        _ => unreachable!("non-barrier op classified as barrier"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::Source;
    use crate::iteration::comprehension::strategy::StrategyName;

    fn push_clause(name: &str) -> Op {
        Op::PushClause {
            name: name.into(),
            source: Source::Literal { values: vec![] },
        }
    }

    #[test]
    fn empty_program_has_zero_bounds() {
        let p = Program::new(vec![]);
        let b = check_bounds(&p);
        assert_eq!(b.stack_depth, 0);
        assert_eq!(b.streaming_op_count, 0);
        assert!(b.barriers.is_empty());
    }

    #[test]
    fn streaming_program_has_no_barriers() {
        let p = Program::new(vec![
            push_clause("a"),
            push_clause("b"),
            Op::Cartesian { n: 2 },
            Op::Filter { predicate: "true".into() },
            Op::Dispense,
        ]);
        let b = check_bounds(&p);
        assert_eq!(b.stack_depth, 2);
        assert!(b.barriers.is_empty());
        assert_eq!(b.streaming_op_count, 4); // push, push, cartesian, filter (dispense excluded)
    }

    #[test]
    fn order_materialize_reports_barrier() {
        let p = Program::new(vec![
            push_clause("a"),
            push_clause("b"),
            Op::Cartesian { n: 2 },
            Op::OrderMaterialize {
                strategy: StrategyName::Halton,
                truncation: Some(50),
                indexed: true,
                input_index_fn: None,
            },
            Op::Dispense,
        ]);
        let b = check_bounds(&p);
        assert_eq!(b.barriers.len(), 1);
        assert_eq!(b.barriers[0].working_set_size, Some(50));
        assert_eq!(b.total_barrier_working_set(), Some(50));
    }

    #[test]
    fn zip_cycle_reports_barrier_with_unknown_size_at_ir_layer() {
        let p = Program::new(vec![
            push_clause("a"),
            push_clause("b"),
            Op::Zip { n: 2, mode: ZipMode::Cycle },
            Op::Dispense,
        ]);
        let b = check_bounds(&p);
        assert_eq!(b.barriers.len(), 1);
        // IR layer doesn't know child cardinalities.
        assert!(b.barriers[0].working_set_size.is_none());
        assert!(b.total_barrier_working_set().is_none());
    }

    #[test]
    fn multiple_barriers_sum() {
        let p = Program::new(vec![
            push_clause("a"),
            Op::OrderMaterialize {
                strategy: StrategyName::Halton,
                truncation: Some(10),
                indexed: true,
                input_index_fn: None,
            },
            Op::OrderMaterialize {
                strategy: StrategyName::Shuffle,
                truncation: Some(20),
                indexed: true,
                input_index_fn: None,
            },
            Op::Dispense,
        ]);
        let b = check_bounds(&p);
        assert_eq!(b.barriers.len(), 2);
        assert_eq!(b.total_barrier_working_set(), Some(30));
    }
}
