// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Extrema` strategy — spec §3.6.
//!
//! Emits the K-D lattice corners (2^N tuples), ordered by distance
//! from the center, truncated to a number of complete *strata*.
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
//!
//! **Truncation is by complete strata, never mid-stratum.** Every
//! corner of a hypercube is equidistant from the center (each axis
//! contributes the same `(s-1)/2` whichever extreme is taken), so all
//! `2^N` corners form a *single* stratum. Consequently `extrema` is
//! the whole corner SET and `extrema/N` for any `N ≥ 1` yields all of
//! it — the `/N` selects strata, and partial counts within an
//! equidistant set are not a meaningful subset (use `lex/N` /
//! `halton/N` / `sobol/N` for count subsampling, `shells/N` for
//! concentric-shell depth). See [`take_n_strata`].

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
///
/// SRD-18d §214: tuples are grouped into **strata by *interior
/// count*** — the number of axes whose index is NOT at `0` or
/// `len-1` (i.e. not at an extreme value). Stratum 0 = corners
/// (all-extreme), 1 = edges (one interior axis), 2 = faces (two),
/// …, N = the single all-interior point. Strata are emitted
/// corners-first (interior count ascending), Lex within a stratum;
/// `/N` keeps the first N strata.
///
/// This is a *combinatorial-shell* (k-face) decomposition of the
/// index hypercube, not a metric one — it depends only on how many
/// axes sit on a boundary, never on a distance. It is the standard
/// k-faces stratification of an N-cube: an N-cube has
/// `C(N,k)·2^(N-k)` k-faces, and here a value list of size `s`
/// contributes `s-2` interior positions to each interior axis. See
/// the n-cube face lattice (Coxeter, *Regular Polytopes*, 3rd ed.,
/// 1973, §7.2; <https://en.wikipedia.org/wiki/Hypercube>) and
/// SRD-18d §214 for the worked 3×3×3 example.
pub(crate) fn extrema_multi_indices(
    idx: &IndexFn,
    truncation: Option<u64>,
) -> Vec<MultiIndex> {
    if index_fn_dim(idx) == 0 {
        return Vec::new();
    }

    // Reduce every supported index shape to a list of per-axis sizes.
    // The 1-D-like forms (Lockstep / Modular / Concatenation) are a
    // single axis of `length`, over which `multi_index_to_flat` maps
    // `[i] -> i`. Continuous / Hybrid give each continuous axis its 2
    // endpoints, so those axes are always at an extreme (interior
    // count 0 — corners).
    let axis_sizes: Vec<u64> = match idx {
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
        IndexFn::Lockstep { length } => vec![*length],
        IndexFn::Modular { axis_sizes } => {
            vec![axis_sizes.iter().copied().max().unwrap_or(0)]
        }
        IndexFn::Concatenation { segment_sizes } => {
            vec![segment_sizes.iter().copied().sum()]
        }
    };

    extrema_strata(&axis_sizes, truncation)
}

/// Interior count of `mi`: the number of axes whose position is
/// strictly between the two extremes `0` and `size-1` (SRD-18d
/// §214). A `size-1` axis has only position `0`, which is both
/// extremes at once, so it never counts as interior; for a
/// 2-value axis every position is an extreme, so its interior
/// count is always 0.
fn interior_count(mi: &[u64], axis_sizes: &[u64]) -> u64 {
    let mut count = 0;
    for (c, s) in mi.iter().zip(axis_sizes.iter()) {
        if *c != 0 && *c != s.saturating_sub(1) {
            count += 1;
        }
    }
    count
}

/// Enumerate the full index space of `axis_sizes`, group into strata
/// by [`interior_count`] (corners-first, Lex within a stratum), and
/// keep the first `truncation` complete strata. `None` keeps every
/// stratum (the whole space, just reordered); `Some(0)` keeps none.
///
/// Enumerates the whole space (`Π sizes`), which is exactly the
/// already-materialized tuple set the strategy indexes into — so it
/// adds no asymptotic cost over the comprehension's own
/// materialization. (A by-stratum generator that emits only the
/// outer k-faces would be `O(2^N)` for `extrema/1`; deferred.)
fn extrema_strata(axis_sizes: &[u64], truncation: Option<u64>) -> Vec<MultiIndex> {
    let total: u64 = axis_sizes.iter().product();
    if total == 0 {
        return Vec::new();
    }
    // Mixed-radix enumeration yields multi-indices in Lex order
    // (leftmost axis most significant); the stable sort preserves
    // that Lex order within each interior-count stratum.
    let mut scored: Vec<(u64, MultiIndex)> = Vec::with_capacity(total as usize);
    let mut mi = vec![0u64; axis_sizes.len()];
    for _ in 0..total {
        scored.push((interior_count(&mi, axis_sizes), mi.clone()));
        for axis in (0..axis_sizes.len()).rev() {
            mi[axis] += 1;
            if mi[axis] < axis_sizes[axis] {
                break;
            }
            mi[axis] = 0;
        }
    }
    scored.sort_by(|(ia, a), (ib, b)| ia.cmp(ib).then_with(|| a.cmp(b)));
    take_n_strata(scored, truncation)
}

/// Keep the first `n` complete strata of `scored` (pairs of
/// `(stratum-key, multi-index)`, already sorted by key ascending
/// with a Lex tiebreak). A *stratum* is a maximal run of equal key;
/// truncation keeps whole strata and never splits one.
///
/// The load-bearing correctness point: for a 2-values-per-axis grid
/// every index is at an extreme, so the interior count is `0` for
/// all of them — they form a *single* corner stratum and any
/// `n >= 1` yields the whole space. A flat `truncate(n)` would
/// instead keep an arbitrary Lex-first slice (`extrema/1` → one of
/// the equally-extreme corners), which is the bug this fixes.
/// `None` keeps every stratum; `Some(0)` keeps none.
fn take_n_strata(scored: Vec<(u64, MultiIndex)>, n: Option<u64>) -> Vec<MultiIndex> {
    let limit = match n {
        None => return scored.into_iter().map(|(_, mi)| mi).collect(),
        Some(0) => return Vec::new(),
        Some(k) => k,
    };
    let mut out = Vec::with_capacity(scored.len());
    let mut strata_seen = 0u64;
    let mut last: Option<u64> = None;
    for (key, mi) in scored {
        if last != Some(key) {
            if strata_seen >= limit {
                break;
            }
            strata_seen += 1;
            last = Some(key);
        }
        out.push(mi);
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
    fn extrema_3x3x3_strata_match_srd_18d_example() {
        // SRD-18d §214 worked example — the canonical cross-reference.
        // k-face counts of a 3-cube (Coxeter, *Regular Polytopes*
        // §7.2): corners 2^3 = 8, edges C(3,1)·2^2 = 12, faces
        // C(3,2)·2 = 6, interior 1; 8+12+6+1 = 27.
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3, 3] };
        assert_eq!(extrema_multi_indices(&idx, Some(1)).len(), 8); // corners
        assert_eq!(extrema_multi_indices(&idx, Some(2)).len(), 20); // + edges
        assert_eq!(extrema_multi_indices(&idx, Some(3)).len(), 26); // + faces
        assert_eq!(extrema_multi_indices(&idx, Some(4)).len(), 27); // + interior
        assert_eq!(extrema_multi_indices(&idx, None).len(), 27); // all strata

        // Stratum 0 is exactly the 8 corners, in lex order (SRD example).
        assert_eq!(
            extrema_multi_indices(&idx, Some(1)),
            vec![
                vec![0, 0, 0], vec![0, 0, 2], vec![0, 2, 0], vec![0, 2, 2],
                vec![2, 0, 0], vec![2, 0, 2], vec![2, 2, 0], vec![2, 2, 2],
            ]
        );
        // The all-interior point (1,1,1) is the final tuple emitted.
        assert_eq!(
            extrema_multi_indices(&idx, None).last(),
            Some(&vec![1u64, 1, 1])
        );
    }

    #[test]
    fn extrema_3x3_edges_then_center() {
        // 3×3: corners (4) → edges (4) → center (1). `/2` = corners+edges.
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3] };
        assert_eq!(extrema_multi_indices(&idx, Some(1)).len(), 4);
        assert_eq!(extrema_multi_indices(&idx, Some(2)).len(), 8);
        assert_eq!(extrema_multi_indices(&idx, None).len(), 9);
        let corners = extrema_multi_indices(&idx, Some(1));
        let mut sorted = corners.clone();
        sorted.sort();
        assert_eq!(sorted, vec![vec![0, 0], vec![0, 2], vec![2, 0], vec![2, 2]]);
    }

    #[test]
    fn extrema_partial_count_keeps_whole_stratum() {
        // The fix: a partial count over an equidistant corner set must
        // not split it. `extrema/1` over a 2×2 (all four corners one
        // stratum) keeps ALL four, not an arbitrary Lex-first one.
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        for n in [1u64, 2, 3, 4] {
            let out = extrema_multi_indices(&idx, Some(n));
            assert_eq!(out.len(), 4, "extrema/{n} should keep the whole stratum");
        }
        // 3-axis hypercube: 8 corners, still one stratum.
        let idx3 = IndexFn::Lattice { axis_sizes: vec![2, 2, 2] };
        assert_eq!(extrema_multi_indices(&idx3, Some(1)).len(), 8);
    }

    #[test]
    fn extrema_zero_strata_is_empty() {
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        assert!(extrema_multi_indices(&idx, Some(0)).is_empty());
    }

    #[test]
    fn extrema_1d_partial_keeps_both_endpoints() {
        // {first, last} are equidistant → one stratum; `/1` keeps both.
        let idx = IndexFn::Lattice { axis_sizes: vec![5] };
        let out = extrema_multi_indices(&idx, Some(1));
        let mut sorted = out.clone();
        sorted.sort();
        assert_eq!(sorted, vec![vec![0], vec![4]]);
    }

    #[test]
    fn extrema_1d_two_strata() {
        // 1-D size-5: stratum 0 = endpoints {0,4}, stratum 1 =
        // interior {1,2,3}. `/1` = endpoints; `None` = all 5,
        // endpoints first.
        let idx = IndexFn::Lattice { axis_sizes: vec![5] };
        let mut endpoints = extrema_multi_indices(&idx, Some(1));
        endpoints.sort();
        assert_eq!(endpoints, vec![vec![0], vec![4]]);
        assert_eq!(extrema_multi_indices(&idx, None).len(), 5);
        assert_eq!(extrema_multi_indices(&idx, None)[0..2], [vec![0], vec![4]]);
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
    fn lockstep_endpoints_then_interior() {
        // Lockstep is one axis of `length`; `/1` = endpoints, `None`
        // = the whole reordered sequence (endpoints first).
        let idx = IndexFn::Lockstep { length: 10 };
        assert_eq!(extrema_multi_indices(&idx, Some(1)), vec![vec![0], vec![9]]);
        let all = extrema_multi_indices(&idx, None);
        assert_eq!(all.len(), 10);
        assert_eq!(all[0..2], [vec![0], vec![9]]);
    }
}
