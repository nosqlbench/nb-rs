// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Halton` strategy — spec §3.6.
//!
//! Low-discrepancy sequence. For K-D, uses the first K primes
//! as bases; the i-th Halton point is `(phi_2(i), phi_3(i),
//! phi_5(i), …)` where `phi_b(i)` is the radical-inverse of i
//! in base b — i.e., write i in base b, reverse the digits, and
//! interpret as a fractional value in `[0, 1)`.
//!
//! Native to continuous K-D boxes (the canonical use case per
//! spec §3.6). Over discrete inputs, the continuous draws are
//! discretized to integer multi-indices.
//!
//! ## References
//!
//! - J. H. Halton, "On the efficiency of certain quasi-random
//!   sequences of points in evaluating multi-dimensional integrals,"
//!   *Numerische Mathematik* 2 (1960), 84–90.
//!   doi:[10.1007/BF01386213](https://doi.org/10.1007/BF01386213).
//!   Defines the K-D sequence as the radical inverses in the first K
//!   prime bases.
//! - J. G. van der Corput, "Verteilungsfunktionen I & II," *Proc.
//!   Akad. Wet. Amsterdam* 38 (1935), 813–821, 1058–1066. The base-`b`
//!   radical-inverse [`radical_inverse`] is the van der Corput
//!   sequence; Halton is its multi-base generalization.
//! - The canonical base-2 van der Corput prefix is
//!   `1/2, 1/4, 3/4, 1/8, 5/8, 3/8, 7/8, …` (digit-reversal of the
//!   naturals), cross-checked in `tests::van_der_corput_base2_published_prefix`.

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_dim, index_fn_size,
    index_fn_supports_lookup, multi_index_to_flat,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

pub struct Halton;

const PRIMES: &[u64] = &[
    2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
];

/// Radical-inverse of `i` in base `b` — `phi_b(i)` in `[0, 1)`.
/// The standard van der Corput / Halton building block.
fn radical_inverse(mut i: u64, base: u64) -> f64 {
    let mut result = 0.0f64;
    let mut f = 1.0f64 / base as f64;
    while i > 0 {
        let digit = i % base;
        result += digit as f64 * f;
        i /= base;
        f /= base as f64;
    }
    result
}

/// K-D Halton point at index `i`. Returns a vector of K floats
/// in `[0, 1)` using the first K primes as bases.
fn halton_point(i: u64, dim: usize) -> Vec<f64> {
    (0..dim)
        .map(|k| radical_inverse(i, PRIMES[k.min(PRIMES.len() - 1)]))
        .collect()
}

impl Strategy for Halton {
    fn name(&self) -> StrategyName {
        StrategyName::Halton
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        idx.is_some()
    }

    fn has_closed_form_for(&self, _idx: &IndexFn) -> bool {
        true
    }

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        if index_fn_supports_lookup(&input.index_fn) {
            let mis = halton_multi_indices(&input.index_fn, truncation);
            mis.into_iter()
                .filter_map(|mi| multi_index_to_flat(&input.index_fn, &mi))
                .filter_map(|flat| input.tuples.get(flat).cloned())
                .collect()
        } else {
            naive_halton_over_tuples(&input.tuples, truncation)
        }
    }
}

fn naive_halton_over_tuples(input: &[Tuple], truncation: Option<u64>) -> Vec<Tuple> {
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
        let pt = radical_inverse(i, 2);
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

pub(crate) fn halton_multi_indices(idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
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
        let pt = halton_point(i, dim);
        let mi = halton_point_to_multi_index(&pt, &axis_sizes, idx);

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

fn halton_point_to_multi_index(pt: &[f64], axis_sizes: &[u64], idx: &IndexFn) -> MultiIndex {
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
                    let v = (f * size as f64).floor() as u64;
                    mi.push(v.min(size.saturating_sub(1)));
                } else {
                    mi.push((f * (1u64 << 53) as f64) as u64);
                }
            }
            mi
        }
        _ => pt
            .iter()
            .zip(axis_sizes.iter())
            .map(|(f, size)| {
                let v = (f * *size as f64).floor() as u64;
                v.min(size.saturating_sub(1))
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radical_inverse_base_2_known_values() {
        assert!((radical_inverse(1, 2) - 0.5).abs() < 1e-12);
        assert!((radical_inverse(2, 2) - 0.25).abs() < 1e-12);
        assert!((radical_inverse(3, 2) - 0.75).abs() < 1e-12);
        assert!((radical_inverse(4, 2) - 0.125).abs() < 1e-12);
    }

    #[test]
    fn van_der_corput_base2_published_prefix() {
        // The published base-2 van der Corput sequence (Halton 1960
        // §2; the digit-reversal of 1,2,3,…): 1/2, 1/4, 3/4, 1/8,
        // 5/8, 3/8, 7/8, 1/16, …
        let expected = [
            1.0 / 2.0, 1.0 / 4.0, 3.0 / 4.0, 1.0 / 8.0,
            5.0 / 8.0, 3.0 / 8.0, 7.0 / 8.0, 1.0 / 16.0,
        ];
        for (k, want) in expected.iter().enumerate() {
            let got = radical_inverse(k as u64 + 1, 2);
            assert!((got - want).abs() < 1e-12, "phi_2({}) = {got}, want {want}", k + 1);
        }
    }

    #[test]
    fn van_der_corput_base3_published_prefix() {
        // Base-3 van der Corput: 1/3, 2/3, 1/9, 4/9, 7/9, 2/9, …
        let expected = [
            1.0 / 3.0, 2.0 / 3.0, 1.0 / 9.0, 4.0 / 9.0, 7.0 / 9.0, 2.0 / 9.0,
        ];
        for (k, want) in expected.iter().enumerate() {
            let got = radical_inverse(k as u64 + 1, 3);
            assert!((got - want).abs() < 1e-12, "phi_3({}) = {got}, want {want}", k + 1);
        }
    }

    #[test]
    fn radical_inverse_base_3_known_values() {
        assert!((radical_inverse(1, 3) - 1.0 / 3.0).abs() < 1e-12);
        assert!((radical_inverse(2, 3) - 2.0 / 3.0).abs() < 1e-12);
        assert!((radical_inverse(3, 3) - 1.0 / 9.0).abs() < 1e-12);
    }

    #[test]
    fn halton_point_2d_first_few() {
        let pt = halton_point(1, 2);
        assert!((pt[0] - 0.5).abs() < 1e-12);
        assert!((pt[1] - 1.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn halton_multi_indices_lattice() {
        let idx = IndexFn::Lattice { axis_sizes: vec![10, 10] };
        let out = halton_multi_indices(&idx, Some(5));
        assert_eq!(out.len(), 5);
        for mi in &out {
            assert!(mi[0] < 10);
            assert!(mi[1] < 10);
        }
        let mut seen = std::collections::HashSet::new();
        for mi in &out {
            assert!(seen.insert(mi.clone()));
        }
    }

    #[test]
    fn halton_multi_indices_continuous() {
        use crate::iteration::comprehension::cardinality::{Interval, ProductMeasure};
        let idx = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0), Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        let out = halton_multi_indices(&idx, Some(100));
        assert_eq!(out.len(), 100);
        for mi in &out {
            assert_eq!(mi.len(), 2);
        }
    }

    #[test]
    fn deterministic() {
        let idx = IndexFn::Lattice { axis_sizes: vec![100, 100] };
        let a = halton_multi_indices(&idx, Some(20));
        let b = halton_multi_indices(&idx, Some(20));
        assert_eq!(a, b);
    }
}
