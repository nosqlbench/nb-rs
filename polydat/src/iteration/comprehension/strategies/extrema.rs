// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Extrema` strategy — spec §3.6.
//!
//! Emits the K-D lattice corners (2^N tuples) sorted by
//! distance metric, truncated to top k.
//!
//! - Discrete `Lattice` with N≥2 axes is the native shape; 1-D
//!   collapses to `{first, last}` (degenerate).
//! - Continuous box: the 2^N corners are the per-axis interval
//!   endpoints (with appropriate open/closed treatment).
//!
//! Distance metric: distance from the lattice center, with
//! lex order as tiebreak. Specifically, for axis sizes
//! `(s_0, …, s_{N-1})` the center is `(s_i / 2, …)`; a corner
//! position `(c_0, …, c_{N-1})` has distance `Σ|c_i - center_i|`.
//! Corners equidistant from center are emitted in Lex order.

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_dim,
    index_fn_supports_lookup, multi_index_to_flat,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

pub struct Extrema;

impl Strategy for Extrema {
    fn name(&self) -> StrategyName {
        StrategyName::Extrema
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        // Per spec §3.6: discrete Lattice (any axis count;
        // 1-axis is degenerate but defined) OR continuous box.
        // Lockstep / Modular / Concatenation also accepted as
        // degenerate forms (per V4's per-strategy table).
        idx.is_some()
    }

    fn has_closed_form_for(&self, idx: &IndexFn) -> bool {
        matches!(
            idx,
            IndexFn::Lattice { .. } | IndexFn::Continuous { .. } | IndexFn::Hybrid { .. }
        )
    }

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        // Always go through the indexed path when the input
        // supports lookup — Extrema's correctness over a 1-D
        // Lattice (giving {first, last}) and over a multi-axis
        // Lattice (giving the 2^N corners) both come from the
        // indexed form. The pre-α "naive_apply" path only
        // produced first/last and was the source of the
        // observed Generator+Extrema bug.
        if index_fn_supports_lookup(&input.index_fn) {
            let mis = extrema_multi_indices(&input.index_fn, truncation);
            mis.into_iter()
                .filter_map(|mi| multi_index_to_flat(&input.index_fn, &mi))
                .filter_map(|flat| input.tuples.get(flat).cloned())
                .collect()
        } else {
            // Continuous / Hybrid: no pre-materialized tuples
            // exist — the continuous-sampling executor path is
            // a SRD-18c follow-up. Fall back to naive prefix
            // ordering for safety; current runtime errors
            // before reaching here.
            naive_extrema_prefix(&input.tuples, truncation)
        }
    }
}

fn naive_extrema_prefix(input: &[Tuple], truncation: Option<u64>) -> Vec<Tuple> {
    if input.is_empty() {
        return Vec::new();
    }
    let n = truncation
        .unwrap_or(input.len() as u64)
        .min(input.len() as u64);
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![input[0].clone()];
    }
    let mut out = Vec::with_capacity(n as usize);
    out.push(input[0].clone());
    out.push(input[input.len() - 1].clone());
    for i in 1..input.len() - 1 {
        if (out.len() as u64) >= n {
            break;
        }
        out.push(input[i].clone());
    }
    out
}

/// Extrema multi-indices over `idx`. Public to crate for tests.
pub(crate) fn extrema_multi_indices(
    idx: &IndexFn,
    truncation: Option<u64>,
) -> Vec<MultiIndex> {
    let dim = index_fn_dim(idx);
    if dim == 0 {
        return Vec::new();
    }

    let axis_sizes = match idx {
        IndexFn::Lattice { axis_sizes } => axis_sizes.clone(),
        IndexFn::Continuous { intervals, .. } => vec![2u64; intervals.len()],
        IndexFn::Hybrid {
            discrete_axes,
            continuous_axes,
            ..
        } => {
            let mut s = Vec::with_capacity(discrete_axes.len() + continuous_axes.len());
            s.extend(discrete_axes.iter().copied());
            s.extend(continuous_axes.iter().map(|_| 2u64));
            s
        }
        IndexFn::Lockstep { length } => {
            let mut out = Vec::new();
            if *length > 0 {
                out.push(vec![0u64]);
            }
            if *length > 1 {
                out.push(vec![length - 1]);
            }
            return apply_truncation(out, truncation);
        }
        IndexFn::Modular { axis_sizes } => {
            let length = axis_sizes.iter().copied().max().unwrap_or(0);
            let mut out = Vec::new();
            if length > 0 {
                out.push(vec![0u64]);
            }
            if length > 1 {
                out.push(vec![length - 1]);
            }
            return apply_truncation(out, truncation);
        }
        IndexFn::Concatenation { segment_sizes } => {
            let total: u64 = segment_sizes.iter().sum();
            let mut out = Vec::new();
            if total > 0 {
                out.push(vec![0u64]);
            }
            if total > 1 {
                out.push(vec![total - 1]);
            }
            return apply_truncation(out, truncation);
        }
    };

    // Enumerate 2^dim corner positions.
    let corner_count = 1u64 << dim.min(63);
    let mut corners: Vec<MultiIndex> = Vec::with_capacity(corner_count as usize);
    for bits in 0..corner_count {
        let mut mi = Vec::with_capacity(dim);
        for axis in 0..dim {
            let bit = (bits >> axis) & 1;
            let pos = if bit == 0 {
                0u64
            } else {
                axis_sizes[axis].saturating_sub(1)
            };
            mi.push(pos);
        }
        corners.push(mi);
    }

    // Sort by distance from center (descending — outermost
    // corners first), Lex tiebreak.
    let centers: Vec<f64> = axis_sizes.iter().map(|s| (*s as f64 - 1.0) / 2.0).collect();
    corners.sort_by(|a, b| {
        let da = corner_distance(a, &centers);
        let db = corner_distance(b, &centers);
        db.partial_cmp(&da)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.cmp(b))
    });

    // De-duplicate: when axis_size == 1, "0" and "axis_size - 1"
    // coincide.
    corners.dedup();

    apply_truncation(corners, truncation)
}

fn corner_distance(mi: &[u64], center: &[f64]) -> f64 {
    mi.iter()
        .zip(center.iter())
        .map(|(c, ctr)| ((*c as f64) - ctr).abs())
        .sum()
}

fn apply_truncation(mut out: Vec<MultiIndex>, truncation: Option<u64>) -> Vec<MultiIndex> {
    if let Some(n) = truncation {
        out.truncate(n as usize);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extrema_2x2_emits_4_corners() {
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = extrema_multi_indices(&idx, None);
        assert_eq!(out.len(), 4);
        let mut sorted = out.clone();
        sorted.sort();
        assert_eq!(sorted, vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]);
    }

    #[test]
    fn extrema_3x3_truncated_to_top_4() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3] };
        let out = extrema_multi_indices(&idx, Some(4));
        assert_eq!(out.len(), 4);
        let mut sorted = out.clone();
        sorted.sort();
        assert_eq!(sorted, vec![vec![0, 0], vec![0, 2], vec![2, 0], vec![2, 2]]);
    }

    #[test]
    fn extrema_1d_degenerate() {
        let idx = IndexFn::Lattice { axis_sizes: vec![5] };
        let out = extrema_multi_indices(&idx, None);
        assert_eq!(out.len(), 2);
        let mut sorted = out.clone();
        sorted.sort();
        assert_eq!(sorted, vec![vec![0], vec![4]]);
    }

    #[test]
    fn extrema_3d_8_corners() {
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2, 2] };
        let out = extrema_multi_indices(&idx, None);
        assert_eq!(out.len(), 8);
    }

    #[test]
    fn continuous_box_corners() {
        use crate::iteration::comprehension::cardinality::{Interval, ProductMeasure};
        let idx = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0), Interval::closed(-1.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        let out = extrema_multi_indices(&idx, None);
        assert_eq!(out.len(), 4);
        let mut sorted = out.clone();
        sorted.sort();
        assert_eq!(sorted, vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]);
    }

    #[test]
    fn lockstep_degenerate_first_last() {
        let idx = IndexFn::Lockstep { length: 10 };
        let out = extrema_multi_indices(&idx, None);
        assert_eq!(out, vec![vec![0], vec![9]]);
    }
}
