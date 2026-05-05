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
/// `clause_sizes` is the per-axis cardinality of the original
/// Cartesian lattice — one entry per scope axis. A parallel-iter
/// clause counts as **one** axis (the zip-step count under its
/// [`super::ast::ZipMode`]), not N. See SRD-18e Push 2.
///
/// **Invariant**: for every index-space ordering (everything
/// except `Lex` / `Custom` / `Sobol`), the tuple stream must
/// be a complete Cartesian product matching `clause_sizes`:
/// `tuples.len() == product(clause_sizes)`. Mismatches are
/// rejected up front rather than producing wrong geometric
/// orderings — see SRD-18e §"Index-space contract for
/// orderings". This means filter-applied streams (where
/// tuples.len() drops below the lattice product) and Union
/// mode (no single Cartesian lattice) are not supported by
/// index-space orderings; callers must pre-validate via
/// [`super::ast::Comprehension::validate`].
///
/// For Union mode, only `Lex` and `Custom` orderings are
/// well-defined — those preserve emission order and don't
/// invoke the index-space recovery path.
pub fn apply_order(
    tuples: Vec<Tuple>,
    clause_sizes: &[usize],
    order: &TraversalOrder,
) -> Result<Vec<Tuple>, String> {
    // Index-space orderings recover per-axis indices from
    // tuple positions in `clause_sizes`'s lattice. If
    // `tuples.len() != product(clause_sizes)`, the recovered
    // indices are nonsense — and the strategy will silently
    // emit a wrong ordering. Reject mismatched inputs at the
    // boundary so the symptom is clear.
    //
    // Lex / Custom / Sobol are exempt: Lex is
    // order-preserving (no index recovery); Sobol / Custom
    // error before reaching geometric reasoning.
    let geometric = !matches!(order,
        TraversalOrder::Lex { .. }
        | TraversalOrder::Custom { .. }
        | TraversalOrder::Sobol { .. }
    );
    if geometric {
        let expected: usize = clause_sizes.iter().product();
        if !clause_sizes.is_empty() && tuples.len() != expected {
            return Err(format!(
                "apply_order: tuple count ({}) doesn't match the lattice \
                 product ({}) for clause_sizes {clause_sizes:?}. \
                 Index-space orderings require a complete Cartesian \
                 lattice — filter-applied streams or Union-mode \
                 concatenations break this invariant.",
                tuples.len(), expected,
            ));
        }
    }
    match order {
        TraversalOrder::Lex { count } => Ok(truncate(tuples, *count)),
        TraversalOrder::ReverseLex { count } => Ok(order_reverse_lex(tuples, clause_sizes, *count)),
        TraversalOrder::Diagonal { count } => Ok(order_diagonal(tuples, clause_sizes, *count, false)),
        TraversalOrder::Antidiagonal { count } => Ok(order_diagonal(tuples, clause_sizes, *count, true)),
        TraversalOrder::Extrema { strata } => Ok(order_extrema(tuples, clause_sizes, *strata)),
        TraversalOrder::Shells { origin, depth } => Ok(order_shells(tuples, clause_sizes, *origin, *depth)),
        TraversalOrder::Halton { count } => Ok(order_halton(tuples, clause_sizes, *count)),
        TraversalOrder::Sobol { .. } => Err(
            "order sobol: Sobol sequences require tabulated Joe-Kuo direction \
             numbers (public domain but not yet bundled). Use `order: halton/N` \
             for low-discrepancy coverage, or `order: lhs/N seed=K` for stratified \
             sampling.".to_string()
        ),
        TraversalOrder::Lhs { count, seed } => Ok(order_lhs(tuples, clause_sizes, *count, *seed)),
        TraversalOrder::Custom { function } => Err(format!(
            "order custom({function}): user-supplied ordering functions are not yet implemented"
        )),
    }
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
// Halton low-discrepancy sequence (SRD-18d / SRD-18e Push 5)
// ============================================================

/// First few primes — used as Halton bases per axis. Up to
/// 25 dimensions covered; comprehensions with more clauses
/// than this would need to extend the table (or switch to a
/// sieve-generated prime list).
const HALTON_PRIMES: &[u64] = &[
    2, 3, 5, 7, 11, 13, 17, 19, 23, 29,
    31, 37, 41, 43, 47, 53, 59, 61, 67, 71,
    73, 79, 83, 89, 97,
];

/// Order tuples by their nearest-position match against
/// successive Halton sequence points in the unit hypercube.
/// Each axis uses a different prime base; the walk
/// deterministically covers the parameter space far more
/// evenly than the first-N tuples of the lex order.
///
/// For each Halton point in `[0, 1)^n`, find the closest
/// **unemitted** lattice tuple in the same fraction space
/// (L₂ distance). Emit, mark, repeat. If `count` is supplied
/// we stop once we've emitted that many; otherwise we walk
/// until every tuple is emitted.
///
/// Filter-rejected tuples are silently skipped during
/// emission — the Halton walk advances past them looking
/// for the closest unemitted-and-still-present tuple.
fn order_halton(
    tuples: Vec<Tuple>,
    sizes: &[usize],
    count: Option<usize>,
) -> Vec<Tuple> {
    let n = sizes.len();
    if n == 0 || tuples.is_empty() {
        return tuples;
    }
    if n > HALTON_PRIMES.len() {
        // Too many dimensions for our prime table —
        // degrade to lex order rather than emit a partial
        // walk. Surfacing as silent fallback is
        // acceptable per SRD-18d "deterministic by default";
        // future work expands the prime list.
        return truncate(tuples, count);
    }

    // Build per-tuple fraction-space points in [0, 1)^n
    // from each tuple's lattice indices. Two tuples with
    // the same indices (shouldn't happen in a Cartesian
    // product) collide cleanly.
    let strides = compute_lex_strides(sizes);
    let total: usize = sizes.iter().product();
    // Map original lex position → tuple index in the input
    // vector. Filter-rejected tuples are absent from this
    // map; the Halton walk skips past those positions.
    let mut lex_to_input: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::with_capacity(tuples.len());
    for (input_idx, tup) in tuples.iter().enumerate() {
        // Reconstruct the lex position from the tuple's
        // values — recover the index per axis by matching
        // the value to the axis's distinct values in
        // first-occurrence order. Since tuples come in
        // lex order from `enumerate_tuples`, the input_idx
        // *is* the lex position when no filter has run.
        // For now we treat the input order as the lex
        // order (matching the comment in apply_order's
        // doc). This means filter-active runs use the
        // surviving-tuple's input position as its lattice
        // position, not its true Cartesian-product
        // position — the per-strategy doc covers this
        // approximation.
        lex_to_input.insert(input_idx, input_idx);
        let _ = (strides.as_slice(), total, tup); // suppress unused warning when total > tuples.len()
    }

    // Generate the Halton points and find closest unemitted.
    let want = count.unwrap_or(tuples.len()).min(tuples.len());
    let mut emitted: Vec<bool> = vec![false; tuples.len()];
    let mut out: Vec<Tuple> = Vec::with_capacity(want);

    // Build per-tuple fraction-space points once.
    let points: Vec<Vec<f64>> = (0..tuples.len())
        .map(|input_idx| {
            let indices = decode_lex(input_idx, &strides, n, sizes);
            indices.iter().enumerate().map(|(axis, idx)| {
                let s = sizes[axis] as f64;
                if s <= 1.0 { 0.5 } else { *idx as f64 / (s - 1.0) }
            }).collect()
        })
        .collect();

    let mut halton_idx: u64 = 1; // Halton starts at index 1 by convention
    while out.len() < want {
        let target: Vec<f64> = (0..n)
            .map(|axis| halton_value(halton_idx, HALTON_PRIMES[axis]))
            .collect();
        // Find closest unemitted tuple by L₂ distance.
        let mut best: Option<(usize, f64)> = None;
        for (i, p) in points.iter().enumerate() {
            if emitted[i] { continue; }
            let d2: f64 = p.iter().zip(target.iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum();
            if best.map_or(true, |(_, bd)| d2 < bd) {
                best = Some((i, d2));
            }
        }
        match best {
            Some((i, _)) => {
                emitted[i] = true;
                out.push(tuples[i].clone());
            }
            None => break, // every tuple emitted
        }
        halton_idx = halton_idx.saturating_add(1);
    }
    out
}

/// One element of the Halton sequence: the radical-inverse
/// of `index` in `base`. Returns a value in `[0, 1)`.
fn halton_value(index: u64, base: u64) -> f64 {
    let mut result = 0.0_f64;
    let mut f = 1.0_f64 / base as f64;
    let mut i = index;
    while i > 0 {
        result += f * (i % base) as f64;
        i /= base;
        f /= base as f64;
    }
    result
}

// ============================================================
// Latin Hypercube sampling (SRD-18d / SRD-18e Push 5c)
// ============================================================

/// Stratified-random sampling over the unit hypercube. For
/// each axis, divide `[0, 1)` into `count` strata; draw one
/// sample per stratum. Pair samples across axes by a
/// deterministic permutation seeded by `seed` (default 0).
/// Snap each chosen point to the closest unemitted lattice
/// tuple by L₂ distance.
///
/// Filter-rejected tuples are silently skipped. When `count`
/// exceeds the survivor set size we emit all survivors and
/// stop.
fn order_lhs(
    tuples: Vec<Tuple>,
    sizes: &[usize],
    count: Option<usize>,
    seed: Option<u64>,
) -> Vec<Tuple> {
    let n_axes = sizes.len();
    if n_axes == 0 || tuples.is_empty() {
        return tuples;
    }
    let want = count.unwrap_or(tuples.len()).min(tuples.len());
    if want == 0 {
        return Vec::new();
    }
    let seed = seed.unwrap_or(0);

    // Build per-tuple fraction-space points.
    let strides = compute_lex_strides(sizes);
    let points: Vec<Vec<f64>> = (0..tuples.len())
        .map(|input_idx| {
            let indices = decode_lex(input_idx, &strides, n_axes, sizes);
            indices.iter().enumerate().map(|(axis, idx)| {
                let s = sizes[axis] as f64;
                if s <= 1.0 { 0.5 } else { *idx as f64 / (s - 1.0) }
            }).collect()
        })
        .collect();

    // For each LHS draw `i in 0..want`, pick a point in
    // `[i/want, (i+1)/want)` per axis. The per-axis pairing
    // uses a deterministic Fisher-Yates permutation seeded
    // by `seed + axis` so each axis's stratum order is
    // independent.
    let stratum_width = 1.0 / want as f64;
    let mut targets: Vec<Vec<f64>> = (0..want)
        .map(|i| vec![(i as f64 + 0.5) * stratum_width; n_axes])
        .collect();
    // Permute each axis's stratum order independently.
    for axis in 0..n_axes {
        let perm = fisher_yates_permutation(want, seed.wrapping_add(axis as u64));
        let original: Vec<f64> = (0..want).map(|i| targets[i][axis]).collect();
        for (new_pos, &old_pos) in perm.iter().enumerate() {
            targets[new_pos][axis] = original[old_pos];
        }
    }

    let mut emitted: Vec<bool> = vec![false; tuples.len()];
    let mut out: Vec<Tuple> = Vec::with_capacity(want);
    for target in &targets {
        let mut best: Option<(usize, f64)> = None;
        for (i, p) in points.iter().enumerate() {
            if emitted[i] { continue; }
            let d2: f64 = p.iter().zip(target.iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum();
            if best.map_or(true, |(_, bd)| d2 < bd) {
                best = Some((i, d2));
            }
        }
        match best {
            Some((i, _)) => {
                emitted[i] = true;
                out.push(tuples[i].clone());
            }
            None => break,
        }
    }
    out
}

/// Deterministic Fisher-Yates permutation of `[0, n)` using
/// a splitmix64-style PRNG seeded by `seed`. Same seed
/// always produces the same permutation.
fn fisher_yates_permutation(n: usize, seed: u64) -> Vec<usize> {
    let mut perm: Vec<usize> = (0..n).collect();
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for i in (1..n).rev() {
        // splitmix64
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let j = (z as usize) % (i + 1);
        perm.swap(i, j);
    }
    perm
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
    fn sobol_returns_clear_error_pointing_at_alternatives() {
        // Push 5b: Sobol stays stubbed because we don't yet
        // ship Joe-Kuo direction numbers. The error directs
        // the user at halton or lhs.
        let (tuples, sizes) = lex_3x3();
        let err = apply_order(tuples, &sizes,
            &TraversalOrder::Sobol { count: Some(4) }).unwrap_err();
        assert!(err.to_lowercase().contains("sobol"), "{err}");
        assert!(err.to_lowercase().contains("halton") || err.to_lowercase().contains("lhs"),
            "error should suggest halton or lhs as alternatives: {err}");
    }

    // ── SRD-18d / SRD-18e Push 5c: LHS ordering ──

    #[test]
    fn lhs_with_default_seed_is_deterministic() {
        let (tuples, sizes) = lex_3x3();
        let a = apply_order(tuples.clone(), &sizes,
            &TraversalOrder::Lhs { count: Some(4), seed: None }).unwrap();
        let b = apply_order(tuples, &sizes,
            &TraversalOrder::Lhs { count: Some(4), seed: None }).unwrap();
        assert_eq!(a, b, "lhs default seed should be deterministic");
        assert_eq!(a.len(), 4);
    }

    #[test]
    fn lhs_different_seeds_produce_different_orderings() {
        let (tuples, sizes) = lex_3x3();
        let a = apply_order(tuples.clone(), &sizes,
            &TraversalOrder::Lhs { count: Some(4), seed: Some(1) }).unwrap();
        let b = apply_order(tuples, &sizes,
            &TraversalOrder::Lhs { count: Some(4), seed: Some(42) }).unwrap();
        assert_ne!(a, b, "different seeds should produce different orderings");
    }

    #[test]
    fn lhs_count_none_emits_every_tuple() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples.clone(), &sizes,
            &TraversalOrder::Lhs { count: None, seed: Some(0) }).unwrap();
        assert_eq!(result.len(), tuples.len());
        for orig in &tuples {
            assert!(result.contains(orig), "lhs dropped tuple {orig:?}");
        }
    }

    #[test]
    fn lhs_emits_unique_tuples() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Lhs { count: Some(5), seed: Some(7) }).unwrap();
        assert_eq!(result.len(), 5);
        let unique: std::collections::HashSet<_> = result.iter()
            .map(|t| t.iter().map(|(_, v)| format!("{v:?}")).collect::<Vec<_>>())
            .collect();
        assert_eq!(unique.len(), 5, "lhs should emit unique tuples");
    }

    #[test]
    fn fisher_yates_seeded_is_deterministic() {
        let p1 = fisher_yates_permutation(10, 42);
        let p2 = fisher_yates_permutation(10, 42);
        assert_eq!(p1, p2);
        // The permutation should cover [0, 10) exactly.
        let mut sorted = p1.clone();
        sorted.sort();
        assert_eq!(sorted, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn fisher_yates_different_seeds_differ() {
        let p1 = fisher_yates_permutation(20, 1);
        let p2 = fisher_yates_permutation(20, 2);
        assert_ne!(p1, p2);
    }

    // ── SRD-18d / SRD-18e Push 5: Halton ordering ──

    #[test]
    fn halton_emits_count_tuples_in_deterministic_order() {
        let (tuples, sizes) = lex_3x3();
        let result_a = apply_order(tuples.clone(), &sizes,
            &TraversalOrder::Halton { count: Some(4) }).unwrap();
        let result_b = apply_order(tuples, &sizes,
            &TraversalOrder::Halton { count: Some(4) }).unwrap();
        assert_eq!(result_a.len(), 4);
        // Determinism: same input → same output across runs.
        assert_eq!(result_a, result_b);
        // Every emitted tuple is unique (no duplicates).
        let unique: std::collections::HashSet<_> = result_a.iter()
            .map(|t| t.iter().map(|(_, v)| format!("{v:?}"))
                .collect::<Vec<_>>())
            .collect();
        assert_eq!(unique.len(), 4, "duplicates in halton emission");
    }

    #[test]
    fn halton_count_none_emits_every_tuple() {
        let (tuples, sizes) = lex_3x3();
        let result = apply_order(tuples.clone(), &sizes,
            &TraversalOrder::Halton { count: None }).unwrap();
        assert_eq!(result.len(), tuples.len());
        // Every input tuple must be in the output (permutation).
        for orig in &tuples {
            assert!(result.contains(orig),
                "halton dropped tuple {orig:?}");
        }
    }

    #[test]
    fn halton_count_larger_than_set_emits_all_and_stops() {
        let (tuples, sizes) = lex_3x3();
        let total = tuples.len();
        let result = apply_order(tuples, &sizes,
            &TraversalOrder::Halton { count: Some(total + 100) }).unwrap();
        assert_eq!(result.len(), total);
    }

    #[test]
    fn halton_two_dim_covers_better_than_lex() {
        // 5x5 grid; halton/4 should hit corners spread out
        // across the space, while lex/4 stays in the first
        // row.
        let mut tuples: Vec<Tuple> = Vec::new();
        for i in 0..5 {
            for j in 0..5 {
                tuples.push(vec![
                    ("x".to_string(), Value::U64(i)),
                    ("y".to_string(), Value::U64(j)),
                ]);
            }
        }
        let halton_4 = apply_order(tuples.clone(), &[5, 5],
            &TraversalOrder::Halton { count: Some(4) }).unwrap();
        // Sanity: 4 distinct tuples.
        assert_eq!(halton_4.len(), 4);
        // Sanity: not all on the same row (which `lex/4`
        // would produce — first row only).
        let xs: std::collections::HashSet<_> = halton_4.iter()
            .map(|t| match t[0].1 { Value::U64(n) => n, _ => 999 })
            .collect();
        assert!(xs.len() >= 2,
            "halton/4 should cover at least 2 distinct x rows in a 5×5 grid; got xs={xs:?}");
    }

    #[test]
    fn halton_value_radical_inverse_is_correct() {
        // van der Corput in base 2: 1 → 0.5, 2 → 0.25, 3 → 0.75, 4 → 0.125
        assert!((halton_value(1, 2) - 0.5).abs() < 1e-12);
        assert!((halton_value(2, 2) - 0.25).abs() < 1e-12);
        assert!((halton_value(3, 2) - 0.75).abs() < 1e-12);
        assert!((halton_value(4, 2) - 0.125).abs() < 1e-12);
        // base 3: 1 → 1/3, 2 → 2/3, 3 → 1/9, 4 → 4/9
        assert!((halton_value(1, 3) - (1.0/3.0)).abs() < 1e-12);
        assert!((halton_value(2, 3) - (2.0/3.0)).abs() < 1e-12);
        assert!((halton_value(3, 3) - (1.0/9.0)).abs() < 1e-12);
    }

    // ---- Invariant guard ------------------------------------

    #[test]
    fn apply_order_rejects_lattice_size_mismatch_for_index_space_strategies() {
        // 3 tuples in a lattice claimed to be 3×3 = 9. The
        // index-space strategies need a complete lattice; the
        // guard catches this before the geometric code emits
        // garbage.
        let tuples: Vec<Tuple> = (1..=3).map(|x|
            tuple(&[("x", x), ("y", x * 10)])
        ).collect();
        let sizes = vec![3, 3];

        for ord in [
            TraversalOrder::ReverseLex { count: None },
            TraversalOrder::Diagonal { count: None },
            TraversalOrder::Antidiagonal { count: None },
            TraversalOrder::Extrema { strata: None },
            TraversalOrder::Shells { origin: ShellOrigin::Outer, depth: None },
            TraversalOrder::Halton { count: None },
            TraversalOrder::Lhs { count: None, seed: None },
        ] {
            let err = apply_order(tuples.clone(), &sizes, &ord).unwrap_err();
            assert!(err.contains("lattice product"),
                "{ord:?}: should reject mismatch — got: {err}");
        }
    }

    #[test]
    fn apply_order_lex_passes_through_mismatched_sizes() {
        // Lex preserves emission order — no index recovery,
        // so the guard exempts it. Useful for filter-applied
        // streams where caller wants a stable reorder cap.
        let tuples: Vec<Tuple> = (1..=3).map(|x|
            tuple(&[("x", x), ("y", x * 10)])
        ).collect();
        let result = apply_order(tuples.clone(), &[3, 3],
            &TraversalOrder::Lex { count: None }).unwrap();
        assert_eq!(result.len(), 3);
    }
}
