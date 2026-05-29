// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Diagonal` strategy — spec §3.6.
//!
//! Emits multi-indices in index-sum-ascending order:
//! `(0,0,…), (0,…,1), (0,1,…,0), (1,0,…,0), …`
//! Discrete `Lattice` with ≥2 axes is the native shape;
//! 1-axis input is degenerate (collapses to Lex). Continuous
//! rejected.

use super::{lex::Lex, Strategy, MultiIndex, Tuple, index_fn_size};
use crate::comprehension::metadata::IndexFn;
use crate::comprehension::strategy::StrategyName;

pub struct Diagonal;

impl Strategy for Diagonal {
    fn name(&self) -> StrategyName {
        StrategyName::Diagonal
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        match idx {
            None => false,
            Some(i) => !i.has_continuous_axis(),
        }
    }

    fn has_closed_form_for(&self, idx: &IndexFn) -> bool {
        matches!(idx, IndexFn::Lattice { .. })
    }

    fn naive_apply(&self, input: Vec<Tuple>, truncation: Option<u64>) -> Vec<Tuple> {
        // Without metadata at the naïve layer, we can't reorder
        // by lattice position — fall back to Lex order (the
        // identity case for 1-D, which is the only naïve-form
        // case that's likely to reach this layer).
        match truncation {
            Some(n) => input.into_iter().take(n as usize).collect(),
            None => input,
        }
    }

    fn indexed_apply(&self, idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
        let total = index_fn_size(idx);
        let n = match truncation {
            Some(t) => t.min(total),
            None => total,
        };

        let axis_sizes = match idx {
            IndexFn::Lattice { axis_sizes } => axis_sizes.clone(),
            // Non-Lattice: fall through to Lex (the naive
            // form for 1-D index spaces — Diagonal in 1-D is
            // just Lex).
            _ => return Lex.indexed_apply(idx, truncation),
        };

        diagonal_walk(&axis_sizes, n, false)
    }
}

pub struct Antidiagonal;

impl Strategy for Antidiagonal {
    fn name(&self) -> StrategyName {
        StrategyName::Antidiagonal
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        match idx {
            None => false,
            Some(i) => !i.has_continuous_axis(),
        }
    }

    fn has_closed_form_for(&self, idx: &IndexFn) -> bool {
        matches!(idx, IndexFn::Lattice { .. })
    }

    fn naive_apply(&self, input: Vec<Tuple>, truncation: Option<u64>) -> Vec<Tuple> {
        match truncation {
            Some(n) => input.into_iter().take(n as usize).collect(),
            None => input,
        }
    }

    fn indexed_apply(&self, idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
        let total = index_fn_size(idx);
        let n = match truncation {
            Some(t) => t.min(total),
            None => total,
        };

        let axis_sizes = match idx {
            IndexFn::Lattice { axis_sizes } => axis_sizes.clone(),
            _ => return Lex.indexed_apply(idx, truncation),
        };

        diagonal_walk(&axis_sizes, n, true)
    }
}

/// Enumerate multi-indices by ascending (or descending) index
/// sum. Within each shell of constant sum, tiebreak by Lex
/// order so the walk is fully deterministic.
fn diagonal_walk(axis_sizes: &[u64], n: u64, descending: bool) -> Vec<MultiIndex> {
    if axis_sizes.is_empty() {
        return Vec::new();
    }
    let total: u64 = axis_sizes.iter().copied().fold(1u64, |a, b| a.saturating_mul(b));
    let _ = n; // we'll truncate at the end

    // Maximum possible sum is Σ(axis_size - 1).
    let max_sum: u64 = axis_sizes.iter().map(|s| s.saturating_sub(1)).sum();

    let mut out: Vec<MultiIndex> = Vec::with_capacity(total as usize);
    let sums: Box<dyn Iterator<Item = u64>> = if descending {
        Box::new((0..=max_sum).rev())
    } else {
        Box::new(0..=max_sum)
    };

    for s in sums {
        // Enumerate all multi-indices with index-sum == s.
        enumerate_index_sum(axis_sizes, s, &mut Vec::with_capacity(axis_sizes.len()), &mut out);
        if out.len() as u64 >= n {
            break;
        }
    }
    out.truncate(n as usize);
    out
}

/// Recursively enumerate multi-indices whose components sum to
/// `target_sum`. Visits in Lex order within the shell.
fn enumerate_index_sum(
    axis_sizes: &[u64],
    target_sum: u64,
    current: &mut Vec<u64>,
    out: &mut Vec<MultiIndex>,
) {
    let dim = axis_sizes.len();
    if current.len() == dim {
        let s: u64 = current.iter().sum();
        if s == target_sum {
            out.push(current.clone());
        }
        return;
    }
    let current_sum: u64 = current.iter().sum();
    // Optional remaining max sum after this axis: sum of
    // (size - 1) for remaining axes.
    let max_after_this: u64 = axis_sizes[current.len() + 1..]
        .iter()
        .map(|s| s.saturating_sub(1))
        .sum();

    let size = axis_sizes[current.len()];
    for v in 0..size {
        let new_sum = current_sum + v;
        if new_sum > target_sum {
            break;
        }
        // Prune: even with max from remaining axes, can we
        // still hit the target?
        if new_sum + max_after_this < target_sum {
            continue;
        }
        current.push(v);
        enumerate_index_sum(axis_sizes, target_sum, current, out);
        current.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagonal_2x2() {
        // 2x2 lattice — sums: (0,0)=0, (0,1)=1, (1,0)=1, (1,1)=2.
        // Ascending sum + Lex tiebreak:
        //   sum 0: (0,0)
        //   sum 1: (0,1), (1,0)
        //   sum 2: (1,1)
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = Diagonal.indexed_apply(&idx, None);
        assert_eq!(out, vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]);
    }

    #[test]
    fn diagonal_3x3_truncated() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3] };
        let out = Diagonal.indexed_apply(&idx, Some(4));
        assert_eq!(out.len(), 4);
        // First 4 by ascending sum + Lex.
        // sum 0: (0,0). sum 1: (0,1), (1,0). sum 2: (0,2), (1,1), (2,0).
        assert_eq!(out[0], vec![0, 0]);
        assert_eq!(out[1], vec![0, 1]);
        assert_eq!(out[2], vec![1, 0]);
        assert_eq!(out[3], vec![0, 2]);
    }

    #[test]
    fn antidiagonal_2x2() {
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = Antidiagonal.indexed_apply(&idx, None);
        // Descending sum:
        //   sum 2: (1,1)
        //   sum 1: (0,1), (1,0)
        //   sum 0: (0,0)
        assert_eq!(out, vec![vec![1, 1], vec![0, 1], vec![1, 0], vec![0, 0]]);
    }

    #[test]
    fn diagonal_3d_first_few() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3, 3] };
        let out = Diagonal.indexed_apply(&idx, Some(4));
        // sum 0: (0,0,0). sum 1: (0,0,1), (0,1,0), (1,0,0).
        assert_eq!(out[0], vec![0, 0, 0]);
        assert_eq!(out[1], vec![0, 0, 1]);
        assert_eq!(out[2], vec![0, 1, 0]);
        assert_eq!(out[3], vec![1, 0, 0]);
    }

    #[test]
    fn rejects_continuous() {
        use crate::comprehension::cardinality::{Interval, ProductMeasure};
        let cont = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        assert!(!Diagonal.accepts_input(Some(&cont)));
        assert!(!Antidiagonal.accepts_input(Some(&cont)));
    }
}
