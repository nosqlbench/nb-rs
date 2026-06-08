// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Lex` strategy — spec §3.6.
//!
//! Pass-through. The default emission order — rightmost axis
//! varies fastest in a cartesian. `Lex` is the only strategy
//! whose materialization is `Streaming` (R1: compiles to
//! `ORDER_STREAMING`, not `ORDER_MATERIALIZE`).
//!
//! ## References
//!
//! - D. E. Knuth, *The Art of Computer Programming*, Vol. 4A:
//!   Combinatorial Algorithms, Part 1, §7.2.1.1 ("Generating all
//!   n-tuples"). Lexicographic / mixed-radix order — the rightmost
//!   coordinate is least significant and varies fastest. Cross-checked
//!   in `tests::lex_multi_indices_is_mixed_radix_order`.

use super::{
    EvaluatedInput, MultiIndex, Strategy, Tuple, index_fn_dim, index_fn_size,
};
use crate::iteration::comprehension::metadata::IndexFn;
use crate::iteration::comprehension::strategy::StrategyName;

pub struct Lex;

impl Strategy for Lex {
    fn name(&self) -> StrategyName {
        StrategyName::Lex
    }

    fn accepts_input(&self, _idx: Option<&IndexFn>) -> bool {
        // Lex accepts any input including `None`.
        true
    }

    fn has_closed_form_for(&self, _idx: &IndexFn) -> bool {
        // Always — Lex is the natural enumeration order.
        true
    }

    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple> {
        // Lex is identity over the natural source order; no
        // need to round-trip through indexed_multi_indices /
        // multi_index_to_flat.
        match truncation {
            Some(n) => input.tuples.iter().take(n as usize).cloned().collect(),
            None => input.tuples.clone(),
        }
    }
}

/// Enumerate multi-indices in Lex order over `idx`.
///
/// Public to crate so other strategies (ReverseLex, Diagonal
/// fallbacks) can layer on it.
pub(crate) fn lex_multi_indices(idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex> {
    let total = index_fn_size(idx);
    let n = match truncation {
        Some(t) => t.min(total),
        None => total,
    };
    let dim = index_fn_dim(idx);
    let axis_sizes: Vec<u64> = match idx {
        IndexFn::Lattice { axis_sizes } | IndexFn::Modular { axis_sizes } => axis_sizes.clone(),
        IndexFn::Lockstep { length } => vec![*length],
        IndexFn::Concatenation { segment_sizes } => vec![segment_sizes.iter().sum()],
        // Continuous / Hybrid don't have integer multi-indices;
        // Lex over them is identity over the natural sampling
        // order, which is strategy-specific. Empty sequence —
        // V8 rejects Lex over continuous inputs anyway.
        IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => return Vec::new(),
    };

    let mut out = Vec::with_capacity(n as usize);
    let mut cursor: Vec<u64> = vec![0; dim];
    for _ in 0..n {
        out.push(cursor.clone());
        for i in (0..dim).rev() {
            cursor[i] += 1;
            if cursor[i] < axis_sizes[i] {
                break;
            }
            cursor[i] = 0;
        }
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

    fn input_with(tuples: Vec<Tuple>, axis_sizes: Vec<u64>) -> EvaluatedInput {
        let card = tuples.len() as u64;
        EvaluatedInput {
            tuples,
            cardinality: card,
            index_fn: IndexFn::Lattice { axis_sizes },
        }
    }

    #[test]
    fn apply_is_identity_without_truncation() {
        let inp = input_with(vec![tup(1), tup(2), tup(3)], vec![3]);
        let result = Lex.apply(&inp, None);
        assert_eq!(result, inp.tuples);
    }

    #[test]
    fn lex_multi_indices_is_mixed_radix_order() {
        // Knuth TAOCP 4A §7.2.1.1: mixed-radix order, rightmost axis
        // least significant. A 2×3 lattice enumerates as
        // (0,0)(0,1)(0,2)(1,0)(1,1)(1,2).
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 3] };
        let out = lex_multi_indices(&idx, None);
        assert_eq!(
            out,
            vec![
                vec![0, 0], vec![0, 1], vec![0, 2],
                vec![1, 0], vec![1, 1], vec![1, 2],
            ]
        );
    }

    #[test]
    fn apply_truncates() {
        let inp = input_with(vec![tup(1), tup(2), tup(3), tup(4)], vec![4]);
        let result = Lex.apply(&inp, Some(2));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], tup(1));
        assert_eq!(result[1], tup(2));
    }

    #[test]
    fn lex_multi_indices_2d() {
        // 2x3 lattice — Lex emits (0,0), (0,1), (0,2), (1,0), (1,1), (1,2).
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 3] };
        let result = lex_multi_indices(&idx, None);
        assert_eq!(result.len(), 6);
        assert_eq!(result[0], vec![0, 0]);
        assert_eq!(result[1], vec![0, 1]);
        assert_eq!(result[2], vec![0, 2]);
        assert_eq!(result[3], vec![1, 0]);
        assert_eq!(result[4], vec![1, 1]);
        assert_eq!(result[5], vec![1, 2]);
    }

    #[test]
    fn lex_multi_indices_respects_truncation() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3] };
        let result = lex_multi_indices(&idx, Some(4));
        assert_eq!(result.len(), 4);
        assert_eq!(result[3], vec![1, 0]);
    }

    #[test]
    fn accepts_anything() {
        assert!(Lex.accepts_input(None));
        assert!(Lex.accepts_input(Some(&IndexFn::Lattice { axis_sizes: vec![3] })));
    }
}
