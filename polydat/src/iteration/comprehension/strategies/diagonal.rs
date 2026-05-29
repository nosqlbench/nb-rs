// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Diagonal` / `Antidiagonal` strategies — spec §3.6.
//!
//! Emits multi-indices in index-sum-ascending (or descending)
//! order: `(0,0,…), (0,…,1), (0,1,…,0), (1,0,…,0), …`
//! Discrete `Lattice` with ≥2 axes is the native shape;
//! 1-axis input is degenerate (collapses to Lex). Continuous
//! rejected.

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_size,
    index_fn_supports_lookup, lex::lex_multi_indices, multi_index_to_flat,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

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

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        if index_fn_supports_lookup(&input.index_fn) {
            let mis = diagonal_multi_indices(&input.index_fn, truncation, false);
            mis.into_iter()
                .filter_map(|mi| multi_index_to_flat(&input.index_fn, &mi))
                .filter_map(|flat| input.tuples.get(flat).cloned())
                .collect()
        } else {
            naive_lex_prefix(&input.tuples, truncation)
        }
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

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        if index_fn_supports_lookup(&input.index_fn) {
            let mis = diagonal_multi_indices(&input.index_fn, truncation, true);
            mis.into_iter()
                .filter_map(|mi| multi_index_to_flat(&input.index_fn, &mi))
                .filter_map(|flat| input.tuples.get(flat).cloned())
                .collect()
        } else {
            naive_lex_prefix(&input.tuples, truncation)
        }
    }
}

fn naive_lex_prefix(input: &[Tuple], truncation: Option<u64>) -> Vec<Tuple> {
    match truncation {
        Some(n) => input.iter().take(n as usize).cloned().collect(),
        None => input.to_vec(),
    }
}

pub(crate) fn diagonal_multi_indices(
    idx: &IndexFn,
    truncation: Option<u64>,
    descending: bool,
) -> Vec<MultiIndex> {
    let total = index_fn_size(idx);
    let n = match truncation {
        Some(t) => t.min(total),
        None => total,
    };

    let axis_sizes = match idx {
        IndexFn::Lattice { axis_sizes } => axis_sizes.clone(),
        // Non-Lattice: fall through to Lex (Diagonal in 1-D
        // is just Lex; multi-axis Modular / Lockstep /
        // Concatenation collapse to 1-D so the same applies).
        _ => return lex_multi_indices(idx, truncation),
    };

    diagonal_walk(&axis_sizes, n, descending)
}

fn diagonal_walk(axis_sizes: &[u64], n: u64, descending: bool) -> Vec<MultiIndex> {
    if axis_sizes.is_empty() {
        return Vec::new();
    }
    let total: u64 = axis_sizes
        .iter()
        .copied()
        .fold(1u64, |a, b| a.saturating_mul(b));
    let _ = total;

    let max_sum: u64 = axis_sizes.iter().map(|s| s.saturating_sub(1)).sum();

    let mut out: Vec<MultiIndex> = Vec::with_capacity(n as usize);
    let sums: Box<dyn Iterator<Item = u64>> = if descending {
        Box::new((0..=max_sum).rev())
    } else {
        Box::new(0..=max_sum)
    };

    for s in sums {
        enumerate_index_sum(
            axis_sizes,
            s,
            &mut Vec::with_capacity(axis_sizes.len()),
            &mut out,
        );
        if out.len() as u64 >= n {
            break;
        }
    }
    out.truncate(n as usize);
    out
}

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
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = diagonal_multi_indices(&idx, None, false);
        assert_eq!(out, vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]);
    }

    #[test]
    fn diagonal_3x3_truncated() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3] };
        let out = diagonal_multi_indices(&idx, Some(4), false);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0], vec![0, 0]);
        assert_eq!(out[1], vec![0, 1]);
        assert_eq!(out[2], vec![1, 0]);
        assert_eq!(out[3], vec![0, 2]);
    }

    #[test]
    fn antidiagonal_2x2() {
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = diagonal_multi_indices(&idx, None, true);
        assert_eq!(out, vec![vec![1, 1], vec![0, 1], vec![1, 0], vec![0, 0]]);
    }

    #[test]
    fn diagonal_3d_first_few() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3, 3] };
        let out = diagonal_multi_indices(&idx, Some(4), false);
        assert_eq!(out[0], vec![0, 0, 0]);
        assert_eq!(out[1], vec![0, 0, 1]);
        assert_eq!(out[2], vec![0, 1, 0]);
        assert_eq!(out[3], vec![1, 0, 0]);
    }

    #[test]
    fn rejects_continuous() {
        use crate::iteration::comprehension::cardinality::{Interval, ProductMeasure};
        let cont = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        assert!(!Diagonal.accepts_input(Some(&cont)));
        assert!(!Antidiagonal.accepts_input(Some(&cont)));
    }
}
