// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Shells` strategy — spec §3.6.
//!
//! Emits multi-indices stratified by concentric shells around
//! the lattice center, outermost first. A "shell" is the set
//! of multi-indices at Chebyshev distance `d` from the center
//! (max-norm). Discrete `Lattice` is the native shape;
//! continuous rejected per spec §3.6 ("ill-defined without
//! discretization parameter").
//!
//! Emission within a shell uses Lex order as tiebreak so the
//! walk is fully deterministic.
//!
//! ## References
//!
//! - The shell metric is the Chebyshev / L∞ (max-norm) distance,
//!   named for P. L. Chebyshev; see e.g. M. M. Deza & E. Deza,
//!   *Encyclopedia of Distances*, 4th ed., Springer (2016), §1.1.
//!   A "shell" is the set of points at a fixed L∞ distance from the
//!   centre — the square (hyper-cube) ring at radius `d`. This
//!   differs from [`super::extrema`], whose strata are by *interior
//!   count* (k-faces), not a distance. Outermost-first ordering and
//!   the shell metric are cross-checked in
//!   `tests::shells_are_chebyshev_strata_outermost_first`.

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_size,
    index_fn_supports_lookup, lex::lex_multi_indices, multi_index_to_flat,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

pub struct Shells;

impl Strategy for Shells {
    fn name(&self) -> StrategyName {
        StrategyName::Shells
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
            let mis = shells_multi_indices(&input.index_fn, truncation);
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

pub(crate) fn shells_multi_indices(idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
    let total = index_fn_size(idx);
    let n = match truncation {
        Some(t) => t.min(total),
        None => total,
    };

    let axis_sizes = match idx {
        IndexFn::Lattice { axis_sizes } => axis_sizes.clone(),
        _ => return lex_multi_indices(idx, truncation),
    };

    if axis_sizes.is_empty() {
        return Vec::new();
    }

    let centers: Vec<f64> = axis_sizes.iter().map(|s| (*s as f64 - 1.0) / 2.0).collect();

    // Bucket by *-2 + rounded-int Chebyshev to avoid fp issues
    // with half-integer centers.
    let mut buckets: std::collections::BTreeMap<i64, Vec<MultiIndex>> =
        std::collections::BTreeMap::new();

    enumerate_all(&axis_sizes, &mut Vec::with_capacity(axis_sizes.len()), &mut |mi| {
        let r = chebyshev_distance(mi, &centers);
        let bucket_key = (r * 2.0).round() as i64;
        buckets.entry(bucket_key).or_default().push(mi.clone());
    });

    let mut out: Vec<MultiIndex> = Vec::with_capacity(n as usize);
    for (_key, mut shell) in buckets.into_iter().rev() {
        shell.sort();
        for mi in shell {
            if out.len() as u64 >= n {
                return out;
            }
            out.push(mi);
        }
    }
    out
}

fn chebyshev_distance(mi: &[u64], center: &[f64]) -> f64 {
    mi.iter()
        .zip(center.iter())
        .map(|(c, ctr)| ((*c as f64) - ctr).abs())
        .fold(0.0f64, f64::max)
}

fn enumerate_all(
    axis_sizes: &[u64],
    current: &mut Vec<u64>,
    callback: &mut dyn FnMut(&MultiIndex),
) {
    if current.len() == axis_sizes.len() {
        callback(current);
        return;
    }
    let size = axis_sizes[current.len()];
    for v in 0..size {
        current.push(v);
        enumerate_all(axis_sizes, current, callback);
        current.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shells_3x3() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3] };
        let out = shells_multi_indices(&idx, None);
        assert_eq!(out.len(), 9);
        // Center (1,1) is innermost.
        assert_eq!(out[8], vec![1, 1]);
    }

    #[test]
    fn shells_outermost_first_for_5x5() {
        let idx = IndexFn::Lattice { axis_sizes: vec![5, 5] };
        let out = shells_multi_indices(&idx, Some(4));
        for mi in &out {
            let chebyshev = chebyshev_distance(mi, &[2.0, 2.0]);
            assert!(
                (chebyshev - 2.0).abs() < 1e-9,
                "expected distance 2.0, got {chebyshev:?}"
            );
        }
    }

    #[test]
    fn shells_are_chebyshev_strata_outermost_first() {
        // 5×5 about centre (2,2): three L∞ shells — radius 2 (the 16
        // boundary points), radius 1 (the 8-point inner ring), radius
        // 0 (the centre). The full walk visits them outermost-first,
        // so Chebyshev distance is monotonically non-increasing.
        let idx = IndexFn::Lattice { axis_sizes: vec![5, 5] };
        let out = shells_multi_indices(&idx, None);
        assert_eq!(out.len(), 25);
        let dists: Vec<f64> = out.iter().map(|mi| chebyshev_distance(mi, &[2.0, 2.0])).collect();
        assert!(
            dists.windows(2).all(|w| w[0] >= w[1] - 1e-9),
            "shell distances not outermost-first: {dists:?}"
        );
        // Exactly three distinct radii {2,1,0} with the documented
        // populations 16 / 8 / 1.
        let r2 = dists.iter().filter(|d| (**d - 2.0).abs() < 1e-9).count();
        let r1 = dists.iter().filter(|d| (**d - 1.0).abs() < 1e-9).count();
        let r0 = dists.iter().filter(|d| **d < 1e-9).count();
        assert_eq!((r2, r1, r0), (16, 8, 1));
    }

    #[test]
    fn rejects_continuous() {
        use crate::iteration::comprehension::cardinality::{Interval, ProductMeasure};
        let cont = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        assert!(!Shells.accepts_input(Some(&cont)));
    }
}
