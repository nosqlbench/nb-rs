// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Shuffle` strategy — spec §3.6.
//!
//! Random permutation. PRNG seed captured at materialization
//! per spec §3.6 — same comprehension instance produces the
//! same shuffle on every dispense pass. Different
//! `PolyStreamer` instances against the same comprehension get
//! independent shuffles (the seed is captured per-streamer per
//! spec §9.5.2 independence contract).
//!
//! For the algebra-layer implementation here, the seed is
//! derived from a stable function of the strategy arguments
//! (truncation + a default starter constant). When integrated
//! into the IR interpreter (Phase 7), the seed will be
//! threaded from the streamer's per-instance state instead.
//!
//! Accepts any non-`None` `IndexFn` including continuous —
//! Shuffle over continuous works by sampling per the
//! underlying measure (the IR layer dispatches the actual
//! continuous draw).

use super::{prng::Prng, Strategy, MultiIndex, Tuple, index_fn_size, index_fn_dim};
use crate::comprehension::metadata::IndexFn;
use crate::comprehension::strategy::StrategyName;

pub struct Shuffle;

/// Algebra-layer default seed. Production wiring (Phase 7)
/// replaces this with a per-streamer seed captured at
/// materialization.
const DEFAULT_SEED: u64 = 0xD1CE_5EED_C0FF_EE42;

impl Strategy for Shuffle {
    fn name(&self) -> StrategyName {
        StrategyName::Shuffle
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        idx.is_some()
    }

    fn has_closed_form_for(&self, _idx: &IndexFn) -> bool {
        // Shuffle has a closed-form indexed push-down for every
        // index-addressable input: emit n PRNG draws.
        true
    }

    fn naive_apply(&self, mut input: Vec<Tuple>, truncation: Option<u64>) -> Vec<Tuple> {
        let mut rng = Prng::new(DEFAULT_SEED.wrapping_add(input.len() as u64));
        rng.shuffle(&mut input);
        match truncation {
            Some(n) => input.into_iter().take(n as usize).collect(),
            None => input,
        }
    }

    fn indexed_apply(&self, idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
        let total = index_fn_size(idx);
        let n = match truncation {
            Some(t) => t.min(total).max(0),
            None => total,
        };
        if n == 0 {
            return Vec::new();
        }

        let dim = index_fn_dim(idx);
        let axis_sizes = axis_sizes_for(idx);
        let mut rng = Prng::new(DEFAULT_SEED.wrapping_add(total));

        // For continuous / Hybrid inputs the indexed form
        // produces draws in the per-axis sample space; the IR
        // interpreter converts integer indices ↔ continuous
        // samples per axis. At this layer we emit the raw
        // index-space draws.
        match idx {
            IndexFn::Continuous { intervals, .. } => {
                // Draw n random points in `[0, 1)^K` discretized
                // to u64 fractions. The IR layer maps these to
                // the actual interval.
                let _ = intervals;
                (0..n)
                    .map(|_| (0..dim).map(|_| rng.next_u64()).collect())
                    .collect()
            }
            IndexFn::Hybrid {
                discrete_axes,
                continuous_axes,
                ..
            } => {
                let _ = continuous_axes;
                (0..n)
                    .map(|_| {
                        let mut mi = Vec::with_capacity(dim);
                        for size in discrete_axes {
                            mi.push(rng.next_bounded(*size));
                        }
                        for _ in 0..continuous_axes.len() {
                            mi.push(rng.next_u64());
                        }
                        mi
                    })
                    .collect()
            }
            _ => {
                // Discrete: produce n unique multi-indices via
                // partial Fisher-Yates on a virtual `0..total`
                // index range, then split each linear index
                // back into per-axis components.
                if n == total {
                    // Full shuffle.
                    let mut indices: Vec<u64> = (0..total).collect();
                    rng.shuffle(&mut indices);
                    indices
                        .into_iter()
                        .map(|i| linear_to_multi(i, &axis_sizes))
                        .collect()
                } else {
                    // Partial: do n swaps from a position pool.
                    let mut pool: Vec<u64> = (0..total).collect();
                    let mut out = Vec::with_capacity(n as usize);
                    for i in 0..n {
                        let j = rng.next_bounded(total - i);
                        let pick = pool[j as usize];
                        out.push(linear_to_multi(pick, &axis_sizes));
                        let last = pool.len() - 1;
                        pool.swap(j as usize, last);
                        pool.pop();
                    }
                    out
                }
            }
        }
    }
}

fn axis_sizes_for(idx: &IndexFn) -> Vec<u64> {
    match idx {
        IndexFn::Lattice { axis_sizes } | IndexFn::Modular { axis_sizes } => axis_sizes.clone(),
        IndexFn::Lockstep { length } => vec![*length],
        IndexFn::Concatenation { segment_sizes } => vec![segment_sizes.iter().sum()],
        IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => Vec::new(),
    }
}

/// Convert a linear index `0..product(axis_sizes)` to a
/// per-axis multi-index in Lex order (rightmost varies fastest).
fn linear_to_multi(mut linear: u64, axis_sizes: &[u64]) -> MultiIndex {
    let mut out = vec![0u64; axis_sizes.len()];
    for i in (0..axis_sizes.len()).rev() {
        out[i] = linear % axis_sizes[i];
        linear /= axis_sizes[i];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::strategies::TupleValue;

    fn tup(k: i64) -> Tuple {
        Tuple::new().with("k", TupleValue::I64(k))
    }

    #[test]
    fn naive_preserves_elements() {
        let input = vec![tup(1), tup(2), tup(3), tup(4), tup(5)];
        let mut out = Shuffle.naive_apply(input.clone(), None);
        let mut sorted_in = input;
        out.sort_by_key(|t| match t.bindings[0].1 {
            TupleValue::I64(v) => v,
            _ => panic!(),
        });
        sorted_in.sort_by_key(|t| match t.bindings[0].1 {
            TupleValue::I64(v) => v,
            _ => panic!(),
        });
        assert_eq!(out, sorted_in);
    }

    #[test]
    fn naive_deterministic() {
        let input = vec![tup(1), tup(2), tup(3), tup(4), tup(5)];
        let a = Shuffle.naive_apply(input.clone(), None);
        let b = Shuffle.naive_apply(input, None);
        assert_eq!(a, b);
    }

    #[test]
    fn indexed_apply_produces_unique_multi_indices_discrete() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 4] };
        let out = Shuffle.indexed_apply(&idx, Some(10));
        assert_eq!(out.len(), 10);
        // All unique
        let mut seen = std::collections::HashSet::new();
        for mi in &out {
            assert!(seen.insert(mi.clone()), "duplicate: {mi:?}");
        }
        // All in range
        for mi in &out {
            assert!(mi[0] < 3);
            assert!(mi[1] < 4);
        }
    }

    #[test]
    fn indexed_apply_full_lattice() {
        // n == total: full permutation
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = Shuffle.indexed_apply(&idx, None);
        assert_eq!(out.len(), 4);
        let mut sorted = out.clone();
        sorted.sort();
        assert_eq!(sorted, vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]);
    }

    #[test]
    fn linear_to_multi_round_trip() {
        let sizes = vec![3u64, 4, 5];
        for linear in 0..60u64 {
            let mi = linear_to_multi(linear, &sizes);
            // Convert back: rightmost-fastest accumulation.
            let mut back = 0u64;
            for (s, m) in sizes.iter().zip(mi.iter()) {
                back = back * s + m;
            }
            assert_eq!(back, linear);
        }
    }

    #[test]
    fn accepts_any_non_none() {
        assert!(Shuffle.accepts_input(Some(&IndexFn::Lattice { axis_sizes: vec![3] })));
        assert!(!Shuffle.accepts_input(None));
    }
}
