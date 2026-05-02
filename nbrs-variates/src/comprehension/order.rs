// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Tuple traversal-order implementations. See SRD-18d.
//!
//! Each function takes the post-filter tuple stream (in default
//! lex order, since that's what `enumerate_tuples` produces) and
//! returns a reordered (possibly truncated) version. All
//! functions are **pure** — no kernel access, no IO, no
//! randomness without an explicit seed. Determinism is a hard
//! requirement of this layer.
//!
//! Implementations operate in **index space** when the strategy
//! has geometric meaning (extrema, shells, space-filling). Each
//! tuple's per-clause index is recovered from its position in
//! the original lex enumeration: position `p` in a Cartesian
//! space of sizes `[s₀, s₁, …, sₙ₋₁]` corresponds to indices
//! `(p / (s₁·s₂·…·sₙ₋₁), (p / (s₂·…·sₙ₋₁)) % s₀, …, p % sₙ₋₁)`.
//!
//! Today's implementations cover the geometric strategies
//! (lex / reverse_lex / diagonal / antidiagonal / extrema /
//! shells). The space-filling family (halton / sobol / lhs)
//! and `custom` are stubbed with explicit "not yet implemented"
//! errors — landing them is straightforward extension work.

use crate::node::Value;

use super::ast::{ShellOrigin, TraversalOrder};

/// One emitted tuple — pairs of (var name, typed value).
pub type Tuple = Vec<(String, Value)>;

/// Apply a traversal order to a tuple stream that arrived in
/// default lex order.
///
/// `clause_sizes` is the per-clause cardinality of the original
/// Cartesian product. For Union mode, callers concatenate
/// per-sub-space tuple streams *before* calling this, and pass
/// the dominant sub-space's sizes — geometric strategies degrade
/// gracefully (their notion of "index space" is the lex
/// enumeration of the concatenated stream, not a true Cartesian
/// lattice).
///
/// `total_emitted_count` is the length the lex-ordered tuple
/// vector had before any filter ran — used to recover index
/// positions for the geometric strategies. When filter is
/// active, `tuples.len()` may be less than `total_emitted_count`;
/// the index recovery uses the tuples' values to find their
/// original positions in the lex enumeration.
///
/// For now (until filter+order interaction is fully designed),
/// callers can pass `tuples.len()` as `total_emitted_count` and
/// the geometric strategies will treat the post-filter set as
/// the "lattice" — losing some geometric precision when filter
/// is active but staying deterministic.
pub fn apply_order(
    tuples: Vec<Tuple>,
    clause_sizes: &[usize],
    order: &TraversalOrder,
) -> Result<Vec<Tuple>, String> {
    match order {
        TraversalOrder::Lex { count } => Ok(truncate(tuples, *count)),
        TraversalOrder::ReverseLex { count } => Ok(order_reverse_lex(tuples, clause_sizes, *count)),
        TraversalOrder::Diagonal { count } => Ok(order_diagonal(tuples, clause_sizes, *count, false)),
        TraversalOrder::Antidiagonal { count } => Ok(order_diagonal(tuples, clause_sizes, *count, true)),
        TraversalOrder::Extrema { strata } => Ok(order_extrema(tuples, clause_sizes, *strata)),
        TraversalOrder::Shells { origin, depth } => Ok(order_shells(tuples, clause_sizes, *origin, *depth)),
        TraversalOrder::Halton { .. } => Err(not_yet_implemented("halton")),
        TraversalOrder::Sobol { .. } => Err(not_yet_implemented("sobol")),
        TraversalOrder::Lhs { .. } => Err(not_yet_implemented("lhs")),
        TraversalOrder::Custom { function } => Err(format!(
            "order custom({function}): user-supplied ordering functions are not yet implemented"
        )),
    }
}

fn not_yet_implemented(name: &str) -> String {
    format!(
        "order {name}: low-discrepancy sequences (halton/sobol/lhs) are designed in SRD-18d but not yet implemented"
    )
}

fn truncate(mut tuples: Vec<Tuple>, count: Option<usize>) -> Vec<Tuple> {
    if let Some(n) = count {
        tuples.truncate(n);
    }
    tuples
}

/// Reverse the lex order — leftmost clause varies fastest. With
/// per-clause sizes `[s₀, s₁, …, sₙ₋₁]`, position `p` in the
/// original lex order maps to a tuple of indices
/// `(i₀, i₁, …, iₙ₋₁)` where iₙ₋₁ is the fastest-varying. The
/// reverse traversal reads positions in column-major order
/// (i₀ fastest), which is computed as
/// `p_reversed = i_n-1 * s_n-2*s_n-3*...*s_0 + ... + i_0`.
fn order_reverse_lex(
    tuples: Vec<Tuple>,
    sizes: &[usize],
    count: Option<usize>,
) -> Vec<Tuple> {
    if sizes.is_empty() || tuples.is_empty() {
        return truncate(tuples, count);
    }
    let n_clauses = sizes.len();
    let total: usize = sizes.iter().product();
    if total != tuples.len() {
        // Filter or other transform changed the size; fall back
        // to a stable reverse of the input.
        let mut t = tuples;
        t.reverse();
        return truncate(t, count);
    }

    // Map lex-position → reverse-lex-position
    // For each lex position p, decode indices, then encode in
    // reverse-major order.
    let strides_lex = compute_lex_strides(sizes);
    let strides_rev = compute_reverse_strides(sizes);
    let mut indexed: Vec<(usize, Tuple)> = tuples.into_iter().enumerate()
        .map(|(p, t)| {
            let indices = decode_lex(p, &strides_lex, n_clauses, sizes);
            let p_rev = encode_reverse(&indices, &strides_rev);
            (p_rev, t)
        })
        .collect();
    indexed.sort_by_key(|(p, _)| *p);
    let result: Vec<Tuple> = indexed.into_iter().map(|(_, t)| t).collect();
    truncate(result, count)
}

/// Sort by sum-of-indices ascending (BFS through the lattice).
/// Ties broken by lex order. With `descending=true`, antidiagonal
/// order — sum descending.
fn order_diagonal(
    tuples: Vec<Tuple>,
    sizes: &[usize],
    count: Option<usize>,
    descending: bool,
) -> Vec<Tuple> {
    if sizes.is_empty() || tuples.is_empty() {
        return truncate(tuples, count);
    }
    let total: usize = sizes.iter().product();
    if total != tuples.len() {
        return truncate(tuples, count);
    }
    let strides = compute_lex_strides(sizes);
    let n = sizes.len();
    let mut indexed: Vec<(usize, usize, Tuple)> = tuples.into_iter().enumerate()
        .map(|(p, t)| {
            let indices = decode_lex(p, &strides, n, sizes);
            let sum: usize = indices.iter().sum();
            (sum, p, t)
        })
        .collect();
    indexed.sort_by(|a, b| {
        if descending {
            b.0.cmp(&a.0).then(b.1.cmp(&a.1))
        } else {
            a.0.cmp(&b.0).then(a.1.cmp(&b.1))
        }
    });
    let result: Vec<Tuple> = indexed.into_iter().map(|(_, _, t)| t).collect();
    truncate(result, count)
}

/// Stratify tuples by their interior count — number of clause
/// indices that are *not* at the boundary (index 0 or `len-1`).
/// All-extrema (interior count = 0) emit first, then by
/// increasing interior count. Within each stratum, lex order.
fn order_extrema(
    tuples: Vec<Tuple>,
    sizes: &[usize],
    strata: Option<usize>,
) -> Vec<Tuple> {
    if sizes.is_empty() || tuples.is_empty() {
        return tuples;
    }
    let total: usize = sizes.iter().product();
    if total != tuples.len() {
        return tuples;
    }
    let strides = compute_lex_strides(sizes);
    let n = sizes.len();
    let mut indexed: Vec<(usize, usize, Tuple)> = tuples.into_iter().enumerate()
        .map(|(p, t)| {
            let indices = decode_lex(p, &strides, n, sizes);
            let interior_count = indices.iter().enumerate()
                .filter(|&(axis, &idx)| {
                    let s = sizes[axis];
                    s > 1 && idx != 0 && idx != s - 1
                })
                .count();
            (interior_count, p, t)
        })
        .collect();
    indexed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    if let Some(strata_keep) = strata {
        // strata_keep is "number of strata to retain", so keep
        // tuples whose interior_count is in [0, strata_keep).
        indexed.retain(|(c, _, _)| *c < strata_keep);
    }
    indexed.into_iter().map(|(_, _, t)| t).collect()
}

/// Stratify tuples by L∞ distance from the chosen origin.
/// `outer` origin: distance is min(idx, size - 1 - idx) across
/// axes — boundary = 0, interior = max. `center`: L∞ distance
/// from the center index. `corner`: L∞ distance from (0, …, 0).
fn order_shells(
    tuples: Vec<Tuple>,
    sizes: &[usize],
    origin: ShellOrigin,
    depth: Option<usize>,
) -> Vec<Tuple> {
    if sizes.is_empty() || tuples.is_empty() {
        return tuples;
    }
    let total: usize = sizes.iter().product();
    if total != tuples.len() {
        return tuples;
    }
    let strides = compute_lex_strides(sizes);
    let n = sizes.len();
    let mut indexed: Vec<(usize, usize, Tuple)> = tuples.into_iter().enumerate()
        .map(|(p, t)| {
            let indices = decode_lex(p, &strides, n, sizes);
            let d = shell_distance(&indices, sizes, origin);
            (d, p, t)
        })
        .collect();
    indexed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    if let Some(d_keep) = depth {
        indexed.retain(|(d, _, _)| *d < d_keep);
    }
    indexed.into_iter().map(|(_, _, t)| t).collect()
}

/// L∞ distance from the chosen origin. For `outer`, distance is
/// "how many layers in from the boundary" — boundary = 0,
/// deepest interior = max. For `center`, it's L∞ from the
/// midpoint. For `corner`, it's L∞ from (0, 0, …, 0) which
/// equals max index across axes.
fn shell_distance(indices: &[usize], sizes: &[usize], origin: ShellOrigin) -> usize {
    match origin {
        ShellOrigin::Outer => {
            // Distance to nearest boundary on each axis;
            // overall = the minimum (the closest boundary wins).
            indices.iter().enumerate()
                .map(|(axis, &idx)| {
                    let s = sizes[axis];
                    if s <= 1 { 0 } else {
                        let to_min = idx;
                        let to_max = s - 1 - idx;
                        to_min.min(to_max)
                    }
                })
                .min()
                .unwrap_or(0)
        }
        ShellOrigin::Center => {
            indices.iter().enumerate()
                .map(|(axis, &idx)| {
                    let s = sizes[axis];
                    if s <= 1 { 0 } else {
                        let center = (s - 1) / 2;
                        idx.abs_diff(center)
                    }
                })
                .max()
                .unwrap_or(0)
        }
        ShellOrigin::Corner => {
            indices.iter().copied().max().unwrap_or(0)
        }
    }
}

// ============================================================
// Index-space helpers
// ============================================================

/// Per-axis strides for lex ordering: position `p` decodes via
/// `i_axis = (p / strides[axis]) % sizes[axis]`. Rightmost axis
/// has stride 1 (fastest-varying).
fn compute_lex_strides(sizes: &[usize]) -> Vec<usize> {
    let n = sizes.len();
    let mut strides = vec![1usize; n];
    for axis in (0..n.saturating_sub(1)).rev() {
        strides[axis] = strides[axis + 1] * sizes[axis + 1];
    }
    strides
}

/// Per-axis strides for reverse-lex ordering — leftmost axis
/// is fastest-varying (stride 1), rightmost axis has the
/// largest stride.
fn compute_reverse_strides(sizes: &[usize]) -> Vec<usize> {
    let n = sizes.len();
    let mut strides = vec![1usize; n];
    for axis in 1..n {
        strides[axis] = strides[axis - 1] * sizes[axis - 1];
    }
    strides
}

/// Decode a lex-order position into per-axis indices.
fn decode_lex(p: usize, strides: &[usize], n: usize, sizes: &[usize]) -> Vec<usize> {
    (0..n).map(|axis| (p / strides[axis]) % sizes[axis]).collect()
}

/// Encode per-axis indices into a position using the supplied
/// per-axis strides (chooses ordering: lex vs reverse-lex).
fn encode_reverse(indices: &[usize], strides: &[usize]) -> usize {
    indices.iter().zip(strides.iter()).map(|(i, s)| i * s).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(vars: &[(&str, u64)]) -> Tuple {
        vars.iter().map(|(n, v)| (n.to_string(), Value::U64(*v))).collect()
    }

    fn lex_3x3() -> (Vec<Tuple>, Vec<usize>) {
        // (x in 1..=3, y in 1..=3) — 9 tuples in lex order
        let mut tuples = Vec::new();
        for x in 1..=3 {
            for y in 1..=3 {
                tuples.push(tuple(&[("x", x), ("y", y)]));
            }
        }
        (tuples, vec![3, 3])
    }

    fn names(t: &Tuple) -> Vec<u64> {
        t.iter().map(|(_, v)| match v {
            Value::U64(n) => *n,
            _ => 0,
        }).collect()
    }

    #[test]
    fn lex_no_truncate_is_identity() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples.clone(), &sizes,
            &TraversalOrder::Lex { count: None }).unwrap();
        assert_eq!(result.len(), 9);
        assert_eq!(names(&result[0]), vec![1, 1]);
        assert_eq!(names(&result[8]), vec![3, 3]);
    }

    #[test]
    fn lex_with_count_truncates() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Lex { count: Some(4) }).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(names(&result[3]), vec![2, 1]);
    }

    #[test]
    fn reverse_lex_swaps_axis_speed() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::ReverseLex { count: None }).unwrap();
        // Expected reverse-lex (leftmost fastest): (1,1), (2,1), (3,1), (1,2), (2,2), …
        assert_eq!(names(&result[0]), vec![1, 1]);
        assert_eq!(names(&result[1]), vec![2, 1]);
        assert_eq!(names(&result[2]), vec![3, 1]);
        assert_eq!(names(&result[3]), vec![1, 2]);
    }

    #[test]
    fn diagonal_is_index_sum_ascending() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Diagonal { count: None }).unwrap();
        // diag 0 (sum=0): (1,1)
        // diag 1: (1,2), (2,1)
        // diag 2: (1,3), (2,2), (3,1)
        // diag 3: (2,3), (3,2)
        // diag 4: (3,3)
        assert_eq!(names(&result[0]), vec![1, 1]);
        assert_eq!(names(&result[1]), vec![1, 2]);
        assert_eq!(names(&result[2]), vec![2, 1]);
        assert_eq!(names(&result[3]), vec![1, 3]);
        assert_eq!(names(&result[8]), vec![3, 3]);
    }

    #[test]
    fn antidiagonal_is_index_sum_descending() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Antidiagonal { count: None }).unwrap();
        assert_eq!(names(&result[0]), vec![3, 3]);
        assert_eq!(names(&result[8]), vec![1, 1]);
    }

    #[test]
    fn extrema_corners_first() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Extrema { strata: None }).unwrap();
        // Stratum 0 (interior count 0): four corners
        //   (1,1) (1,3) (3,1) (3,3)
        // Stratum 1 (interior count 1): four edge centers
        //   (1,2) (2,1) (2,3) (3,2)
        // Stratum 2 (interior count 2): face center
        //   (2,2)
        let first_four: Vec<Vec<u64>> = result[..4].iter().map(names).collect();
        assert!(first_four.contains(&vec![1, 1]));
        assert!(first_four.contains(&vec![1, 3]));
        assert!(first_four.contains(&vec![3, 1]));
        assert!(first_four.contains(&vec![3, 3]));
        // Stratum 1 edge centers
        let next_four: Vec<Vec<u64>> = result[4..8].iter().map(names).collect();
        assert!(next_four.contains(&vec![1, 2]));
        assert!(next_four.contains(&vec![2, 1]));
        // Final stratum: (2, 2)
        assert_eq!(names(&result[8]), vec![2, 2]);
    }

    #[test]
    fn extrema_strata_1_keeps_corners_only() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Extrema { strata: Some(1) }).unwrap();
        assert_eq!(result.len(), 4);
        let yielded: Vec<Vec<u64>> = result.iter().map(names).collect();
        assert!(yielded.contains(&vec![1, 1]));
        assert!(yielded.contains(&vec![3, 3]));
    }

    #[test]
    fn shells_outer_emits_boundary_first() {
        // 3x3: shell 0 = boundary (8 tuples), shell 1 = (2,2)
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Shells {
                origin: ShellOrigin::Outer,
                depth: None,
            }).unwrap();
        assert_eq!(result.len(), 9);
        // First eight are the boundary; last is the center.
        assert_eq!(names(&result[8]), vec![2, 2]);
    }

    #[test]
    fn shells_outer_depth_1_keeps_only_boundary() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Shells {
                origin: ShellOrigin::Outer,
                depth: Some(1),
            }).unwrap();
        assert_eq!(result.len(), 8);
    }

    #[test]
    fn shells_center_emits_center_first() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Shells {
                origin: ShellOrigin::Center,
                depth: None,
            }).unwrap();
        // Center is (2, 2) at index (1, 1) in 0-based — distance 0
        assert_eq!(names(&result[0]), vec![2, 2]);
    }

    #[test]
    fn space_filling_strategies_error_until_implemented() {
        let (tuples, sizes) = lex_3x3();
        let err = apply_order(tuples, &sizes,
            &TraversalOrder::Halton { count: Some(4) }).unwrap_err();
        assert!(err.contains("halton"));
    }
}
