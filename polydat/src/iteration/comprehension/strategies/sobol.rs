// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Sobol` strategy — spec §3.6.
//!
//! Low-discrepancy sequence using base-2 direction numbers.
//! Native to continuous K-D boxes; discretizes to integer
//! multi-indices for discrete inputs.
//!
//! For this initial implementation, uses the base-2
//! van-der-Corput sequence per axis (degenerate Sobol with
//! identity direction numbers). A production-grade Sobol
//! implementation would use Joe-Kuo direction numbers for
//! higher dimensions; the test gates in this layer don't
//! depend on the K-D discrepancy quality, just on
//! determinism + within-range output. Joe-Kuo upgrade is
//! tracked in the implementation plan as a Phase 4 follow-up
//! to land before benchmark-quality QMC consumers depend on
//! this strategy.

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_dim, index_fn_size,
    index_fn_supports_lookup, multi_index_to_flat,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

pub struct Sobol;

fn van_der_corput_base_2(mut i: u64) -> f64 {
    let mut result = 0.0f64;
    let mut f = 0.5f64;
    while i > 0 {
        if i & 1 == 1 {
            result += f;
        }
        i >>= 1;
        f *= 0.5;
    }
    result
}

fn sobol_point(i: u64, dim: usize) -> Vec<f64> {
    (0..dim)
        .map(|k| {
            let shifted = i.wrapping_add(((k as u64) * 0x9E37_79B9) % (1u64 << 32));
            van_der_corput_base_2(shifted)
        })
        .collect()
}

impl Strategy for Sobol {
    fn name(&self) -> StrategyName {
        StrategyName::Sobol
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        idx.is_some()
    }

    fn has_closed_form_for(&self, _idx: &IndexFn) -> bool {
        true
    }

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        if index_fn_supports_lookup(&input.index_fn) {
            let mis = sobol_multi_indices(&input.index_fn, truncation);
            mis.into_iter()
                .filter_map(|mi| multi_index_to_flat(&input.index_fn, &mi))
                .filter_map(|flat| input.tuples.get(flat).cloned())
                .collect()
        } else {
            naive_sobol_over_tuples(&input.tuples, truncation)
        }
    }
}

fn naive_sobol_over_tuples(input: &[Tuple], truncation: Option<u64>) -> Vec<Tuple> {
    let total = input.len() as u64;
    if total == 0 {
        return Vec::new();
    }
    let n = match truncation {
        Some(t) => t.min(total),
        None => total,
    };
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(n as usize);
    let mut i = 1u64;
    let mut attempts = 0u64;
    let max_attempts = total.saturating_mul(8).max(64);
    while (out.len() as u64) < n && attempts < max_attempts {
        let pt = van_der_corput_base_2(i);
        let idx = (pt * total as f64).floor() as u64;
        let idx = idx.min(total - 1);
        if seen.insert(idx) {
            out.push(input[idx as usize].clone());
        }
        i += 1;
        attempts += 1;
    }
    out
}

pub(crate) fn sobol_multi_indices(idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
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
    let mut out = Vec::with_capacity(n as usize);
    let mut i = 1u64;
    let mut seen_discrete = std::collections::HashSet::new();
    let max_attempts = n.saturating_mul(8).max(256);
    let mut attempts = 0u64;
    let is_continuous = matches!(idx, IndexFn::Continuous { .. } | IndexFn::Hybrid { .. });

    while (out.len() as u64) < n && attempts < max_attempts {
        let pt = sobol_point(i, dim);
        let mi = point_to_multi_index(&pt, &axis_sizes, idx);
        if is_continuous {
            out.push(mi);
        } else if seen_discrete.insert(mi.clone()) {
            out.push(mi);
        }
        i += 1;
        attempts += 1;
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

fn point_to_multi_index(pt: &[f64], axis_sizes: &[u64], idx: &IndexFn) -> MultiIndex {
    match idx {
        IndexFn::Continuous { .. } => pt
            .iter()
            .map(|f| (f * (1u64 << 53) as f64) as u64)
            .collect(),
        IndexFn::Hybrid { discrete_axes, .. } => {
            let mut mi = Vec::with_capacity(pt.len());
            for (i, f) in pt.iter().enumerate() {
                if i < discrete_axes.len() {
                    let size = discrete_axes[i];
                    mi.push(((f * size as f64).floor() as u64).min(size.saturating_sub(1)));
                } else {
                    mi.push((f * (1u64 << 53) as f64) as u64);
                }
            }
            mi
        }
        _ => pt
            .iter()
            .zip(axis_sizes.iter())
            .map(|(f, size)| ((f * *size as f64).floor() as u64).min(size.saturating_sub(1)))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn van_der_corput_first_few() {
        assert!((van_der_corput_base_2(1) - 0.5).abs() < 1e-12);
        assert!((van_der_corput_base_2(2) - 0.25).abs() < 1e-12);
        assert!((van_der_corput_base_2(3) - 0.75).abs() < 1e-12);
    }

    #[test]
    fn deterministic() {
        let idx = IndexFn::Lattice { axis_sizes: vec![20, 20] };
        let a = sobol_multi_indices(&idx, Some(10));
        let b = sobol_multi_indices(&idx, Some(10));
        assert_eq!(a, b);
    }

    #[test]
    fn produces_unique_discrete() {
        let idx = IndexFn::Lattice { axis_sizes: vec![50, 50] };
        let out = sobol_multi_indices(&idx, Some(20));
        assert_eq!(out.len(), 20);
        let mut seen = std::collections::HashSet::new();
        for mi in &out {
            assert!(seen.insert(mi.clone()));
        }
    }

    #[test]
    fn continuous_box_draws() {
        use crate::iteration::comprehension::cardinality::{Interval, ProductMeasure};
        let idx = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0), Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        let out = sobol_multi_indices(&idx, Some(50));
        assert_eq!(out.len(), 50);
    }
}
