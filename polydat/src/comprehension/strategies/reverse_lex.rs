// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `ReverseLex` strategy — spec §3.6.
//!
//! Reverses the input's Lex enumeration order. Index-sampling
//! family: accepts any non-`None` IndexFn (per spec §3.6's
//! per-strategy table). Continuous input is rejected (no
//! canonical reverse over a measure).

use super::{lex::Lex, Strategy, MultiIndex, Tuple, index_fn_size};
use crate::comprehension::metadata::IndexFn;
use crate::comprehension::strategy::StrategyName;

pub struct ReverseLex;

impl Strategy for ReverseLex {
    fn name(&self) -> StrategyName {
        StrategyName::ReverseLex
    }

    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool {
        // Discrete only (spec §3.6: continuous rejected).
        match idx {
            None => false,
            Some(i) => !i.has_continuous_axis(),
        }
    }

    fn has_closed_form_for(&self, idx: &IndexFn) -> bool {
        !idx.has_continuous_axis()
    }

    fn naive_apply(&self, mut input: Vec<Tuple>, truncation: Option<u64>) -> Vec<Tuple> {
        input.reverse();
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
        // Generate the Lex enumeration, then reverse + truncate.
        // Generating in reverse directly would save the allocation;
        // for clarity (and because R2 truncation is small) we
        // reuse Lex and reverse the prefix.
        let lex = Lex.indexed_apply(idx, None);
        let mut rev: Vec<MultiIndex> = lex.into_iter().rev().collect();
        rev.truncate(n as usize);
        rev
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::strategies::{TupleValue};

    fn tup(k: i64) -> Tuple {
        Tuple::new().with("k", TupleValue::I64(k))
    }

    #[test]
    fn naive_reverses() {
        let input = vec![tup(1), tup(2), tup(3)];
        let out = ReverseLex.naive_apply(input, None);
        assert_eq!(out, vec![tup(3), tup(2), tup(1)]);
    }

    #[test]
    fn naive_truncates_after_reverse() {
        let input = vec![tup(1), tup(2), tup(3), tup(4)];
        let out = ReverseLex.naive_apply(input, Some(2));
        assert_eq!(out, vec![tup(4), tup(3)]);
    }

    #[test]
    fn indexed_apply_reverses_lex_2d() {
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 2] };
        let out = ReverseLex.indexed_apply(&idx, None);
        // Lex of 2x2: (0,0), (0,1), (1,0), (1,1). Reverse:
        assert_eq!(out, vec![vec![1, 1], vec![1, 0], vec![0, 1], vec![0, 0]]);
    }

    #[test]
    fn accepts_discrete_only() {
        use crate::comprehension::cardinality::{Interval, ProductMeasure};
        assert!(ReverseLex.accepts_input(Some(&IndexFn::Lattice { axis_sizes: vec![3] })));
        assert!(!ReverseLex.accepts_input(Some(&IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        })));
        assert!(!ReverseLex.accepts_input(None));
    }
}
