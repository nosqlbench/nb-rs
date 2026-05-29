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

use super::{lex::Lex, Strategy, MultiIndex, Tuple, index_fn_size};
use crate::comprehension::metadata::IndexFn;
use crate::comprehension::strategy::StrategyName;

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

        if axis_sizes.is_empty() {
            return Vec::new();
        }

        // Max Chebyshev radius = max over axes of max(c -
        // center_i) for c in 0..axis_size_i.
        let centers: Vec<f64> = axis_sizes.iter().map(|s| (*s as f64 - 1.0) / 2.0).collect();
        let max_radius = (0..axis_sizes.len())
            .map(|i| ((axis_sizes[i].saturating_sub(1)) as f64 - 0.0).max(centers[i]))
            .fold(0.0f64, f64::max);

        // Walk shells from outermost (max_radius) inward.
        // For integer center halves, radii are also halves; we
        // bucket by f64-rounded-to-int * 2 to avoid fp issues.
        let mut buckets: std::collections::BTreeMap<i64, Vec<MultiIndex>> =
            std::collections::BTreeMap::new();

        enumerate_all(&axis_sizes, &mut Vec::with_capacity(axis_sizes.len()), &mut |mi| {
            let r = chebyshev_distance(mi, &centers);
            // Use *-2 + rounded-int as bucket key so axis-size-3
            // → center 1.0, radii are 0.0 or 1.0 (integer);
            // axis-size-4 → center 1.5, radii are 0.5 or 1.5.
            let bucket_key = (r * 2.0).round() as i64;
            buckets.entry(bucket_key).or_default().push(mi.clone());
        });

        let _ = max_radius;
        // Outermost-first emission: walk buckets in descending key order.
        let mut out: Vec<MultiIndex> = Vec::with_capacity(n as usize);
        for (_key, mut shell) in buckets.into_iter().rev() {
            // Lex order within the shell.
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
        let out = Shells.indexed_apply(&idx, None);
        assert_eq!(out.len(), 9);
        // Outermost shell: distance 1.0 from center (1.0, 1.0)
        // = 8 boundary positions. Innermost: distance 0 = (1,1).
        // First 8 are outer, last is center.
        assert_eq!(out[8], vec![1, 1]);
    }

    #[test]
    fn shells_outermost_first_for_5x5() {
        let idx = IndexFn::Lattice { axis_sizes: vec![5, 5] };
        let out = Shells.indexed_apply(&idx, Some(4));
        // Center is (2.0, 2.0). Outermost shell has Chebyshev
        // distance 2.0 = the 16 boundary positions. First 4
        // by Lex tiebreak.
        for mi in &out {
            let chebyshev = chebyshev_distance(mi, &[2.0, 2.0]);
            assert!((chebyshev - 2.0).abs() < 1e-9, "expected distance 2.0, got {chebyshev:?}");
        }
    }

    #[test]
    fn rejects_continuous() {
        use crate::comprehension::cardinality::{Interval, ProductMeasure};
        let cont = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        assert!(!Shells.accepts_input(Some(&cont)));
    }
}
