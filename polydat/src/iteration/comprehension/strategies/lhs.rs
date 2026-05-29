// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Lhs` (Latin Hypercube Sampling) strategy — spec §3.6.
//!
//! Per-axis stratified permutation. For K-D + n samples:
//!
//! 1. Stratify each axis's `[0, size)` into n equal bins.
//! 2. For each axis, generate a random permutation of `0..n`.
//! 3. Sample i = zip per-axis permutations[i].
//!
//! The result is n tuples that cover each axis's bins
//! uniformly (Latin square property in K-D). Native to
//! continuous K-D boxes — the stratification is the
//! mathematical definition. Over discrete inputs, the
//! stratified positions are floored to integer indices.
//!
//! 1-axis Lhs is degenerate (equivalent to Shuffle); spec
//! §5.8 emits a warning when this composition is detected
//! (handled in `validate.rs`).

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_dim, index_fn_size,
    index_fn_supports_lookup, multi_index_to_flat, prng::Prng,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

pub struct Lhs;

/// Algebra-layer seed; replaced by per-streamer seed in Phase 7.
const SEED: u64 = 0x1A50_4577_3EED_BEEF;

impl Strategy for Lhs {
    fn name(&self) -> StrategyName {
        StrategyName::Lhs
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        idx.is_some()
    }

    fn has_closed_form_for(&self, _idx: &IndexFn) -> bool {
        true
    }

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        if index_fn_supports_lookup(&input.index_fn) {
            let mis = lhs_multi_indices(&input.index_fn, truncation);
            mis.into_iter()
                .filter_map(|mi| multi_index_to_flat(&input.index_fn, &mi))
                .filter_map(|flat| input.tuples.get(flat).cloned())
                .collect()
        } else {
            naive_lhs_over_tuples(&input.tuples, truncation)
        }
    }
}

fn naive_lhs_over_tuples(input: &[Tuple], truncation: Option<u64>) -> Vec<Tuple> {
    let total = input.len() as u64;
    if total == 0 {
        return Vec::new();
    }
    let n = match truncation {
        Some(t) => t.min(total),
        None => total,
    };
    let mut rng = Prng::new(SEED.wrapping_add(total));
    let mut indices: Vec<u64> = (0..total).collect();
    rng.shuffle(&mut indices);
    indices
        .into_iter()
        .take(n as usize)
        .map(|i| input[i as usize].clone())
        .collect()
}

pub(crate) fn lhs_multi_indices(idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
    let dim = index_fn_dim(idx);
    if dim == 0 {
        return Vec::new();
    }
    let total = index_fn_size(idx);
    let n = match (truncation, total) {
        (Some(t), 0) => t,
        (Some(t), tot) => t.min(tot),
        (None, 0) => return Vec::new(),
        (None, tot) => tot,
    };
    if n == 0 {
        return Vec::new();
    }

    let axis_sizes = axis_sizes_for(idx, dim);

    let mut rng = Prng::new(SEED.wrapping_add(n));
    let mut per_axis_perms: Vec<Vec<u64>> = Vec::with_capacity(dim);
    for _ in 0..dim {
        let mut perm: Vec<u64> = (0..n).collect();
        rng.shuffle(&mut perm);
        per_axis_perms.push(perm);
    }

    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n {
        let mi: MultiIndex = (0..dim)
            .map(|axis| {
                let stratum = per_axis_perms[axis][i as usize];
                let size = axis_sizes[axis];
                if size == u64::MAX {
                    stratum
                } else if size >= n {
                    (stratum * size) / n
                } else {
                    stratum % size
                }
            })
            .collect();
        out.push(mi);
    }
    out
}

fn axis_sizes_for(idx: &IndexFn, dim: usize) -> Vec<u64> {
    match idx {
        IndexFn::Lattice { axis_sizes } | IndexFn::Modular { axis_sizes } => axis_sizes.clone(),
        IndexFn::Lockstep { length } => vec![*length],
        IndexFn::Concatenation { segment_sizes } => vec![segment_sizes.iter().sum()],
        IndexFn::Continuous { .. } => vec![u64::MAX; dim],
        IndexFn::Hybrid {
            discrete_axes,
            continuous_axes,
            ..
        } => {
            let mut s = discrete_axes.clone();
            s.extend(continuous_axes.iter().map(|_| u64::MAX));
            s
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lhs_per_axis_stratification_2d_discrete() {
        let idx = IndexFn::Lattice { axis_sizes: vec![10, 10] };
        let out = lhs_multi_indices(&idx, Some(5));
        assert_eq!(out.len(), 5);

        let axis_0_values: std::collections::HashSet<u64> = out.iter().map(|mi| mi[0]).collect();
        let axis_1_values: std::collections::HashSet<u64> = out.iter().map(|mi| mi[1]).collect();
        assert_eq!(axis_0_values.len(), 5);
        assert_eq!(axis_1_values.len(), 5);
    }

    #[test]
    fn lhs_continuous_each_stratum_used() {
        use crate::iteration::comprehension::cardinality::{Interval, ProductMeasure};
        let idx = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0), Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        let out = lhs_multi_indices(&idx, Some(10));
        assert_eq!(out.len(), 10);
        let axis_0: std::collections::HashSet<u64> = out.iter().map(|mi| mi[0]).collect();
        let axis_1: std::collections::HashSet<u64> = out.iter().map(|mi| mi[1]).collect();
        assert_eq!(axis_0.len(), 10);
        assert_eq!(axis_1.len(), 10);
    }

    #[test]
    fn deterministic() {
        let idx = IndexFn::Lattice { axis_sizes: vec![20, 20] };
        let a = lhs_multi_indices(&idx, Some(10));
        let b = lhs_multi_indices(&idx, Some(10));
        assert_eq!(a, b);
    }
}
