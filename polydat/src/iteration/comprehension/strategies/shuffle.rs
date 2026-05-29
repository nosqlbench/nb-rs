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

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_dim, index_fn_size,
    index_fn_supports_lookup, multi_index_to_flat, prng::Prng,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

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
        true
    }

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        if index_fn_supports_lookup(&input.index_fn) {
            let mis = shuffle_multi_indices(&input.index_fn, truncation);
            mis.into_iter()
                .filter_map(|mi| multi_index_to_flat(&input.index_fn, &mi))
                .filter_map(|flat| input.tuples.get(flat).cloned())
                .collect()
        } else {
            naive_shuffle_over_tuples(input.tuples.clone(), truncation)
        }
    }
}

fn naive_shuffle_over_tuples(mut input: Vec<Tuple>, truncation: Option<u64>) -> Vec<Tuple> {
    let mut rng = Prng::new(DEFAULT_SEED.wrapping_add(input.len() as u64));
    rng.shuffle(&mut input);
    match truncation {
        Some(n) => input.into_iter().take(n as usize).collect(),
        None => input,
    }
}

pub(crate) fn shuffle_multi_indices(idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
    let total = index_fn_size(idx);
    let n = match truncation {
        Some(t) => t.min(total),
        None => total,
    };
    if n == 0 {
        return Vec::new();
    }

    let dim = index_fn_dim(idx);
    let axis_sizes = axis_sizes_for(idx);
    let mut rng = Prng::new(DEFAULT_SEED.wrapping_add(total));

    match idx {
        IndexFn::Continuous { intervals, .. } => {
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
            if n == total {
                let mut indices: Vec<u64> = (0..total).collect();
                rng.shuffle(&mut indices);
                indices
                    .into_iter()
                    .map(|i| linear_to_multi(i, &axis_sizes))
                    .collect()
            } else {
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

fn axis_sizes_for(idx: &IndexFn) -> Vec<u64> {
    match idx {
        IndexFn::Lattice { axis_sizes } | IndexFn::Modular { axis_sizes } => axis_sizes.clone(),
        IndexFn::Lockstep { length } => vec![*length],
        IndexFn::Concatenation { segment_sizes } => vec![segment_sizes.iter().sum()],
        IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => Vec::new(),
    }
}

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
    use crate::iteration::comprehension::strategies::TupleValue;

    fn tup(k: i64) -> Tuple {
        Tuple::new().with("k", TupleValue::I64(k))
    }

    fn input_with(tuples: Vec<Tuple>) -> EvaluatedInput {
        let n = tuples.len() as u64;
        EvaluatedInput {
            tuples,
            cardinality: n,
            index_fn: IndexFn::Lattice { axis_sizes: vec![n] },
        }
    }

    #[test]
    fn apply_preserves_elements() {
        let inp = input_with(vec![tup(1), tup(2), tup(3), tup(4), tup(5)]);
        let mut out = Shuffle.apply(&inp, None);
        let mut sorted_in = inp.tuples.clone();
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
    fn apply_deterministic() {
        let inp = input_with(vec![tup(1), tup(2), tup(3), tup(4), tup(5)]);
        let a = Shuffle.apply(&inp, None);
        let b = Shuffle.apply(&inp, None);
        assert_eq!(a, b);
    }

    #[test]
    fn shuffle_multi_indices_produces_unique_discrete() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 4] };
        let out = shuffle_multi_indices(&idx, Some(10));
        assert_eq!(out.len(), 10);
        let mut seen = std::collections::HashSet::new();
        for mi in &out {
            assert!(seen.insert(mi.clone()), "duplicate: {mi:?}");
        }
        for mi in &out {
            assert!(mi[0] < 3);
            assert!(mi[1] < 4);
        }
    }

    #[test]
    fn shuffle_multi_indices_full_lattice() {
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = shuffle_multi_indices(&idx, None);
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
