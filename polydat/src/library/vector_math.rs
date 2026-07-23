// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Element-wise vector math nodes (type_system_alignment.md §8.2).
//!
//! These are the first compute-shaped (rather than I/O-shaped)
//! consumers of the typed-vector family: perturbing query vectors,
//! computing ground-truth distances in evaluations, normalizing
//! embeddings. On `jit` builds the f32 hot loops execute through
//! the cranelift-SIMD kernels (`compile::jit::simd` — F32X4
//! chunked with scalar tails); without `jit`, or if host ISA
//! construction fails, the scalar reference loops below run
//! instead. SIMD accumulation reassociates float addition, so dot
//! products may differ from the scalar reference in the final
//! ulps — both orders are equally valid IEEE 754 sums.
//!
//! Length mismatches panic with both lengths named: silently
//! truncating to the shorter operand would corrupt distance
//! semantics ("never ignore silently").

// ── Scalar reference implementations ──────────────────────────
// Used directly on non-jit builds and as the equivalence oracle
// in tests.

pub(crate) fn dot_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

pub(crate) fn l2sq_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn check_lens(name: &str, a: usize, b: usize) {
    if a != b {
        panic!("{name}: operand lengths differ ({a} vs {b})");
    }
}

// ── Kernel dispatch ────────────────────────────────────────────

fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(feature = "jit")]
    if let Some(k) = crate::compile::jit::simd::kernels() {
        // SAFETY: both slices live for the call; len is the
        // (equal) element count.
        return unsafe { (k.dot_f32)(a.as_ptr(), b.as_ptr(), a.len() as u64) };
    }
    dot_scalar(a, b)
}

fn l2sq_f32(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(feature = "jit")]
    if let Some(k) = crate::compile::jit::simd::kernels() {
        return unsafe { (k.l2sq_f32)(a.as_ptr(), b.as_ptr(), a.len() as u64) };
    }
    l2sq_scalar(a, b)
}

fn add_f32(a: &[f32], b: &[f32]) -> Vec<f32> {
    let mut out = vec![0.0f32; a.len()];
    #[cfg(feature = "jit")]
    if let Some(k) = crate::compile::jit::simd::kernels() {
        // SAFETY: out was just allocated with a.len() elements.
        unsafe { (k.add_f32)(a.as_ptr(), b.as_ptr(), out.as_mut_ptr(), a.len() as u64) };
        return out;
    }
    for i in 0..a.len() {
        out[i] = a[i] + b[i];
    }
    out
}

fn scale_f32(a: &[f32], k_val: f32) -> Vec<f32> {
    let mut out = vec![0.0f32; a.len()];
    #[cfg(feature = "jit")]
    if let Some(k) = crate::compile::jit::simd::kernels() {
        unsafe { (k.scale_f32)(a.as_ptr(), k_val, out.as_mut_ptr(), a.len() as u64) };
        return out;
    }
    for i in 0..a.len() {
        out[i] = a[i] * k_val;
    }
    out
}

// ── Library nodes ──────────────────────────────────────────────

/// `vec_add(a, b)` — element-wise sum of two f32 vectors.
/// Panics when the lengths differ.
#[crate::polydat_node(category = Arithmetic)]
fn vec_add(a: &[f32], b: &[f32]) -> Vec<f32> {
    check_lens("vec_add", a.len(), b.len());
    add_f32(a, b)
}

/// `vec_scale(a, k)` — multiply every element of an f32 vector by
/// scalar `k` (applied at f32 precision).
#[crate::polydat_node(category = Arithmetic)]
fn vec_scale(a: &[f32], k: f64) -> Vec<f32> {
    scale_f32(a, k as f32)
}

/// `vec_dot(a, b)` — dot product of two f32 vectors, widened to
/// f64 on the output wire. Panics when the lengths differ.
#[crate::polydat_node(category = Arithmetic)]
fn vec_dot(a: &[f32], b: &[f32]) -> f64 {
    check_lens("vec_dot", a.len(), b.len());
    dot_f32(a, b) as f64
}

/// `vec_l2(a, b)` — Euclidean (L2) distance between two f32
/// vectors. Panics when the lengths differ.
#[crate::polydat_node(category = Arithmetic)]
fn vec_l2(a: &[f32], b: &[f32]) -> f64 {
    check_lens("vec_l2", a.len(), b.len());
    (l2sq_f32(a, b) as f64).sqrt()
}

/// `vec_cosine(a, b)` — cosine similarity of two f32 vectors:
/// `dot(a,b) / (|a| * |b|)`. Returns 0.0 when either vector has
/// zero magnitude (the conventional degenerate-case value: no
/// direction, no similarity). Panics when the lengths differ.
#[crate::polydat_node(category = Arithmetic)]
fn vec_cosine(a: &[f32], b: &[f32]) -> f64 {
    check_lens("vec_cosine", a.len(), b.len());
    let dot = dot_f32(a, b) as f64;
    let na = (dot_f32(a, a) as f64).sqrt();
    let nb = (dot_f32(b, b) as f64).sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

/// `vec_norm(a)` — scale an f32 vector to unit L2 magnitude.
/// A zero vector passes through unchanged (there is no direction
/// to normalize onto, and emitting NaNs would poison downstream
/// distance math silently).
#[crate::polydat_node(category = Arithmetic)]
fn vec_norm(a: &[f32]) -> Vec<f32> {
    let mag = (dot_f32(a, a) as f64).sqrt();
    if mag == 0.0 {
        a.to_vec()
    } else {
        scale_f32(a, (1.0 / mag) as f32)
    }
}

/// `lid_mle(distances, k)` — Levina–Bickel maximum-likelihood estimate of
/// the **local intrinsic dimensionality** at one query point, from its
/// sorted ground-truth nearest-neighbor distances.
///
/// Given ascending distances `r_1 ≤ … ≤ r_k` to the `k` nearest neighbors,
///
/// ```text
///     d̂ = m / Σ_{j=1..m} ln(r_k / r_j)        (m = number of valid terms)
/// ```
///
/// the standard MLE (Levina & Bickel, NIPS 2004) with `r_k` as the cutoff
/// radius. `k` is clamped to the available length. Terms with a
/// non-positive `r_j` (exact duplicates / self-matches at distance 0, where
/// `ln(r_k/r_j)` is undefined) are skipped — the conventional handling.
/// Returns `0.0` for a degenerate query (fewer than one valid term, or a
/// non-positive log-sum, e.g. all-duplicate neighbors) so the caller can
/// filter it out of the aggregate.
///
/// Reporting the *distribution* of this per-query estimate (mean / p50 /
/// p90) characterizes both the intrinsic dimension and its heterogeneity —
/// the quantity that governs whether a 1-D locality ordering can work (a
/// low, tight LID favors it; a high or skewed LID does not).
///
/// ASSUMES `distances` are true metric distances in ascending order (the
/// dataset's `neighbor_distances` / `filtered_neighbor_distances` facet).
/// If that facet stores *squared* distances the estimate is scaled by ½
/// (double it); if it stores *similarities* (higher = closer) the result is
/// meaningless — validate against the dataset's metric first.
#[crate::polydat_node(category = Arithmetic)]
fn lid_mle(distances: &[f32], k: f64) -> f64 {
    let k = (k as usize).min(distances.len());
    if k < 2 {
        return 0.0;
    }
    let r_k = distances[k - 1] as f64;
    if r_k <= 0.0 {
        return 0.0;
    }
    let ln_rk = r_k.ln();
    let mut logsum = 0.0_f64;
    let mut terms = 0u32;
    for &r_j in &distances[..k - 1] {
        let r_j = r_j as f64;
        if r_j > 0.0 {
            logsum += ln_rk - r_j.ln(); // ln(r_k / r_j) ≥ 0 since r_j ≤ r_k
            terms += 1;
        }
    }
    if terms == 0 || logsum <= 0.0 {
        0.0
    } else {
        terms as f64 / logsum
    }
}

/// `hash_vec(seed, dim)` — deterministic synthetic f32 vector:
/// element `i` is the xxh3 hash of `(seed, i)` mapped into
/// `[-1, 1)`. The canonical generator for synthetic embeddings —
/// equal seeds always produce the identical vector, so dataset-
/// free vector workloads stay replayable. Pairs with `vec_norm`
/// for unit vectors.
#[crate::polydat_node(category = Hashing)]
fn hash_vec(seed: u64, dim: u64) -> Vec<f32> {
    (0..dim)
        .map(|i| {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&seed.to_le_bytes());
            key[8..].copy_from_slice(&i.to_le_bytes());
            let h = xxhash_rust::xxh3::xxh3_64(&key);
            (h as f64 / u64::MAX as f64 * 2.0 - 1.0) as f32
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, SliceArc, Value};

    fn vecv(v: Vec<f32>) -> Value {
        Value::VecF32(SliceArc::from_vec(v))
    }

    fn test_vec(n: usize, seed: u64) -> Vec<f32> {
        (0..n)
            .map(|i| {
                let h = xxhash_rust::xxh3::xxh3_64(&(seed ^ i as u64).to_le_bytes());
                (h as f64 / u64::MAX as f64 * 2.0 - 1.0) as f32
            })
            .collect()
    }

    fn eval2<N: PolydatNode>(node: &N, a: Vec<f32>, b: Vec<f32>) -> Value {
        let mut out = [Value::None];
        node.eval(&[vecv(a), vecv(b)], &mut out);
        out[0].clone()
    }

    #[test]
    fn vec_math_matches_scalar_reference() {
        // 1029 = 257 SIMD chunks + 1 tail element.
        let a = test_vec(1029, 1);
        let b = test_vec(1029, 2);

        let dot = eval2(&VecDot::new(), a.clone(), b.clone()).as_f64();
        let dot_ref = dot_scalar(&a, &b) as f64;
        assert!((dot - dot_ref).abs() / dot_ref.abs().max(1e-6) < 1e-4);

        let l2 = eval2(&VecL2::new(), a.clone(), b.clone()).as_f64();
        let l2_ref = (l2sq_scalar(&a, &b) as f64).sqrt();
        assert!((l2 - l2_ref).abs() / l2_ref.max(1e-6) < 1e-4);

        let sum = eval2(&VecAdd::new(), a.clone(), b.clone());
        let sum = sum.as_vec_f32();
        for i in 0..a.len() {
            assert_eq!(sum[i], a[i] + b[i], "vec_add lane {i}");
        }

        let cos_self = eval2(&VecCosine::new(), a.clone(), a.clone()).as_f64();
        assert!((cos_self - 1.0).abs() < 1e-4, "self-cosine = {cos_self}");
    }

    #[test]
    fn vec_scale_and_norm() {
        let a = test_vec(37, 3);
        let mut out = [Value::None];
        VecScale::new().eval(&[vecv(a.clone()), Value::F64(2.0)], &mut out);
        let scaled = out[0].as_vec_f32();
        for i in 0..a.len() {
            assert_eq!(scaled[i], a[i] * 2.0, "vec_scale lane {i}");
        }

        let mut out = [Value::None];
        VecNorm::new().eval(&[vecv(a.clone())], &mut out);
        let unit = out[0].as_vec_f32().to_vec();
        let mag = (dot_scalar(&unit, &unit) as f64).sqrt();
        assert!((mag - 1.0).abs() < 1e-4, "norm magnitude = {mag}");

        // Zero vector passes through unchanged.
        let mut out = [Value::None];
        VecNorm::new().eval(&[vecv(vec![0.0; 4])], &mut out);
        assert_eq!(out[0].as_vec_f32(), &[0.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn lid_mle_matches_closed_form_and_handles_degenerate() {
        // r_j = e^{j}, j=0..9 → ln(r_k/r_j) = 9-j; Σ_{j=0}^{8}(9-j)=45,
        // 9 valid terms → d̂ = 9/45 = 0.2. Deterministic arithmetic check.
        let dists: Vec<f32> = (0..10).map(|j| (j as f32).exp()).collect();
        let mut out = [Value::None];
        LidMle::new().eval(&[vecv(dists), Value::F64(10.0)], &mut out);
        assert!((out[0].as_f64() - 0.2).abs() < 1e-4, "got {}", out[0].as_f64());

        // Fewer than 2 distances → degenerate → 0.0.
        let mut out = [Value::None];
        LidMle::new().eval(&[vecv(vec![1.0]), Value::F64(10.0)], &mut out);
        assert_eq!(out[0].as_f64(), 0.0);

        // All-zero (duplicate) neighbors, r_k = 0 → degenerate → 0.0.
        let mut out = [Value::None];
        LidMle::new().eval(&[vecv(vec![0.0, 0.0, 0.0]), Value::F64(3.0)], &mut out);
        assert_eq!(out[0].as_f64(), 0.0);

        // Zero-distance self-match at r_1 is skipped, not fatal: distances
        // [0, e, e^2] with k=3 → r_k=e^2 (ln=2), only r_2=e (ln=1) valid →
        // 1 term, logsum=1 → d̂ = 1.0.
        let mut out = [Value::None];
        let d: Vec<f32> = vec![0.0, std::f32::consts::E, std::f32::consts::E * std::f32::consts::E];
        LidMle::new().eval(&[vecv(d), Value::F64(3.0)], &mut out);
        assert!((out[0].as_f64() - 1.0).abs() < 1e-4, "got {}", out[0].as_f64());
    }

    #[test]
    fn hash_vec_is_deterministic_and_seed_sensitive() {
        let mut out = [Value::None];
        HashVec::new().eval(&[Value::U64(7), Value::U64(16)], &mut out);
        let v1 = out[0].as_vec_f32().to_vec();
        let mut out = [Value::None];
        HashVec::new().eval(&[Value::U64(7), Value::U64(16)], &mut out);
        assert_eq!(v1, out[0].as_vec_f32(), "same seed must reproduce");
        let mut out = [Value::None];
        HashVec::new().eval(&[Value::U64(8), Value::U64(16)], &mut out);
        assert_ne!(v1, out[0].as_vec_f32(), "different seed must differ");
        assert_eq!(v1.len(), 16);
        assert!(v1.iter().all(|x| (-1.0..1.0).contains(x)));
    }

    /// §8.4 layer 3 end-to-end: a vector dataflow (synthetic
    /// producer → element-wise math → horizontal reduce) rides
    /// compiled kernels via the (ptr, len) slot protocol with
    /// kernel-owned scratch, and must match typed eval. The same
    /// flow through the hybrid kernel runs the scalar segments as
    /// native JIT and the slice ops as slot closures (whose
    /// bodies already execute the cranelift-SIMD kernels).
    #[test]
    fn vec_flow_rides_compiled_kernels() {
        let src = r#"
            input cycle: u64
            a := hash_vec(cycle, 37)
            b := hash_vec(hash(cycle), 37)
            s := vec_add(a, b)
            out := vec_dot(s, b)
        "#;
        let mut p1 = crate::dsl::compile_polydat(src).unwrap();

        let asm = crate::dsl::compile::compile_polydat_to_assembler(src).unwrap();
        let mut p2 = asm
            .try_compile_raw()
            .expect("slice-bearing nodes are P2-eligible via compiled_slot");

        for cycle in [0u64, 7, 0xFEED] {
            p1.set_inputs(&[cycle]);
            let want = p1.pull("out").as_f64();
            let slot = p2.resolve_output("out").unwrap();
            let got = f64::from_bits(p2.eval_for_slot(&[cycle], slot));
            assert_eq!(got, want, "P2 vec flow mismatch at cycle={cycle}");
        }

        #[cfg(feature = "jit")]
        {
            let asm = crate::dsl::compile::compile_polydat_to_assembler(src).unwrap();
            let mut hy = asm.compile_hybrid().unwrap();
            for cycle in [0u64, 7, 0xFEED] {
                p1.set_inputs(&[cycle]);
                let want = p1.pull("out").as_f64();
                let slot = hy.resolve_output("out").unwrap();
                hy.eval(&[cycle]);
                let got = f64::from_bits(hy.get_slot(slot));
                assert_eq!(got, want, "hybrid vec flow mismatch at cycle={cycle}");
            }
        }
    }

    /// Scratch reuse: re-evaluating the same compiled kernel must
    /// republish coherent (ptr, len) views even as vector contents
    /// change cycle to cycle.
    #[test]
    fn vec_scratch_reuses_across_evals() {
        let src = r#"
            input cycle: u64
            a := hash_vec(cycle, 16)
            out := vec_l2(a, vec_scale(a, 2.0))
        "#;
        let mut p1 = crate::dsl::compile_polydat(src).unwrap();
        let asm = crate::dsl::compile::compile_polydat_to_assembler(src).unwrap();
        let mut p2 = asm.try_compile_raw().expect("P2-eligible");
        let slot = p2.resolve_output("out").unwrap();
        // Interleave cycles so stale-scratch bugs would surface as
        // cross-cycle contamination.
        for cycle in [1u64, 9, 1, 42, 9, 1] {
            p1.set_inputs(&[cycle]);
            let want = p1.pull("out").as_f64();
            let got = f64::from_bits(p2.eval_for_slot(&[cycle], slot));
            assert_eq!(got, want, "scratch reuse mismatch at cycle={cycle}");
        }
    }

    #[test]
    fn vec_length_mismatch_panics() {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut out = [Value::None];
            VecDot::new().eval(&[vecv(vec![1.0]), vecv(vec![1.0, 2.0])], &mut out);
        }));
        assert!(r.is_err(), "vec_dot accepted mismatched lengths");
    }
}
