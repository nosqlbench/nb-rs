// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `Lex` strategy — spec §3.6.
//!
//! Pass-through. The default emission order — rightmost axis
//! varies fastest in a cartesian. `Lex` is the only strategy
//! whose materialization is `Streaming` (R1: compiles to
//! `ORDER_STREAMING`, not `ORDER_MATERIALIZE`).

use super::{
    Strategy, MultiIndex, Tuple, index_fn_dim, index_fn_size,
};
use crate::comprehension::metadata::IndexFn;
use crate::comprehension::strategy::StrategyName;

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
        let dim = index_fn_dim(idx);
        // Enumerate multi-indices in Lex order: rightmost axis
        // varies fastest.
        let axis_sizes: Vec<u64> = match idx {
            IndexFn::Lattice { axis_sizes } | IndexFn::Modular { axis_sizes } => {
                axis_sizes.clone()
            }
            IndexFn::Lockstep { length } => vec![*length],
            IndexFn::Concatenation { segment_sizes } => vec![segment_sizes.iter().sum()],
            // Continuous / Hybrid don't have integer multi-indices in
            // the strict sense; Lex over them is identity over the
            // natural sampling order, which is strategy-specific.
            // For Lex we emit an empty sequence — the runtime is
            // expected not to invoke Lex's indexed form over a
            // continuous input (Lex is rejected by V8 in that case).
            IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => return Vec::new(),
        };

        let mut out = Vec::with_capacity(n as usize);
        let mut cursor: Vec<u64> = vec![0; dim];
        for _ in 0..n {
            out.push(cursor.clone());
            // Increment as little-endian with rightmost-fastest.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::strategies::TupleValue;

    fn tup(k: i64) -> Tuple {
        Tuple::new().with("k", TupleValue::I64(k))
    }

    #[test]
    fn naive_apply_is_identity_without_truncation() {
        let input = vec![tup(1), tup(2), tup(3)];
        let result = Lex.naive_apply(input.clone(), None);
        assert_eq!(result, input);
    }

    #[test]
    fn naive_apply_truncates() {
        let input = vec![tup(1), tup(2), tup(3), tup(4)];
        let result = Lex.naive_apply(input, Some(2));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], tup(1));
        assert_eq!(result[1], tup(2));
    }

    #[test]
    fn indexed_apply_lex_order_2d() {
        // 2x3 lattice — Lex emits (0,0), (0,1), (0,2), (1,0), (1,1), (1,2).
        let idx = IndexFn::Lattice { axis_sizes: vec![2, 3] };
        let result = Lex.indexed_apply(&idx, None);
        assert_eq!(result.len(), 6);
        assert_eq!(result[0], vec![0, 0]);
        assert_eq!(result[1], vec![0, 1]);
        assert_eq!(result[2], vec![0, 2]);
        assert_eq!(result[3], vec![1, 0]);
        assert_eq!(result[4], vec![1, 1]);
        assert_eq!(result[5], vec![1, 2]);
    }

    #[test]
    fn indexed_apply_respects_truncation() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 3] };
        let result = Lex.indexed_apply(&idx, Some(4));
        assert_eq!(result.len(), 4);
        assert_eq!(result[3], vec![1, 0]);
    }

    #[test]
    fn accepts_anything() {
        assert!(Lex.accepts_input(None));
        assert!(Lex.accepts_input(Some(&IndexFn::Lattice { axis_sizes: vec![3] })));
    }
}
