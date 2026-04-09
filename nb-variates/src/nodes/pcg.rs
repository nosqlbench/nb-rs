// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! PCG-RXS-M-XS 64/64 random number generator nodes.
//!
//! These nodes implement the PCG (Permuted Congruential Generator) family
//! algorithm with the RXS-M-XS output permutation. The key property is
//! O(log N) seek: any position in the sequence can be computed directly
//! without iterating from the beginning. This makes it ideal for
//! deterministic parallel workloads where each thread jumps to its own
//! region of the sequence.
//!
//! Three nodes are provided:
//!
//! - [`Pcg`] — fixed seed and stream, position is the wire input
//! - [`PcgStream`] — fixed seed, both position and stream are wire inputs
//! - [`CycleWalk`] — bijective permutation of `[0, range)` via cycle-walking

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Slot, Value};

// =================================================================
// PCG-RXS-M-XS 64/64 core algorithm
// =================================================================

/// LCG multiplier for the 64-bit state.
const MULT: u64 = 6364136223846793005;

/// Apply the RXS-M-XS output permutation to an LCG state.
///
/// This is the bit-mixing function that turns correlated LCG state
/// into high-quality pseudo-random output.
#[inline]
fn pcg_output(state: u64) -> u64 {
    let word = ((state >> ((state >> 59) + 5)) ^ state)
        .wrapping_mul(12605985483714917081);
    (word >> 43) ^ word
}

/// Seek to an arbitrary position in the PCG sequence in O(log N) time.
///
/// Uses the "distance" algorithm that exponentiates the LCG recurrence
/// via repeated squaring, equivalent to computing `state_N` directly
/// from `seed` without iterating through positions 0..N.
///
/// - `seed`: initial LCG state
/// - `inc`: LCG increment (must be odd; typically `2 * stream + 1`)
/// - `position`: the sequence index to seek to
#[inline]
fn pcg_seek(seed: u64, inc: u64, position: u64) -> u64 {
    let mut cur_mult = MULT;
    let mut cur_plus = inc;
    let mut acc_mult: u64 = 1;
    let mut acc_plus: u64 = 0;
    let mut delta = position;
    while delta > 0 {
        if delta & 1 != 0 {
            acc_mult = acc_mult.wrapping_mul(cur_mult);
            acc_plus = acc_plus.wrapping_mul(cur_mult).wrapping_add(cur_plus);
        }
        cur_plus = cur_mult.wrapping_add(1).wrapping_mul(cur_plus);
        cur_mult = cur_mult.wrapping_mul(cur_mult);
        delta >>= 1;
    }
    let state = acc_mult.wrapping_mul(seed).wrapping_add(acc_plus);
    pcg_output(state)
}

// =================================================================
// GK Nodes
// =================================================================

/// PCG-RXS-M-XS 64/64 random number generator with fixed seed and stream.
///
/// Signature: `pcg(position: u64) -> u64`
///
/// The `seed` and `stream` are init-time constants baked into the node.
/// The `position` wire input selects which element of the sequence to
/// return. Seeking is O(log N) so any position can be accessed directly.
///
/// Use this when every thread/cycle needs an independent, deterministic
/// random value from the same generator. The output is a full 64-bit
/// pseudo-random value suitable for feeding into range reduction,
/// unit-interval mapping, or distribution sampling.
///
/// JIT level: P2 (compiled_u64 closure with captured seed and inc).
/// Exposes `jit_constants`: `[seed, inc]`.
pub struct Pcg {
    meta: NodeMeta,
    seed: u64,
    inc: u64,
}

impl Pcg {
    /// Create a new PCG node.
    ///
    /// - `seed`: initial LCG state
    /// - `stream`: stream selector; the LCG increment is `2 * stream + 1`
    pub fn new(seed: u64, stream: u64) -> Self {
        let inc = 2u64.wrapping_mul(stream).wrapping_add(1);
        Self {
            meta: NodeMeta {
                name: "pcg".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("position")),
                    Slot::const_u64("seed", seed),
                    Slot::const_u64("inc", inc),
                ],
            },
            seed,
            inc,
        }
    }
}

impl GkNode for Pcg {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let position = inputs[0].as_u64();
        outputs[0] = Value::U64(pcg_seek(self.seed, self.inc, position));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let seed = self.seed;
        let inc = self.inc;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = pcg_seek(seed, inc, inputs[0]);
        }))
    }

    fn jit_constants(&self) -> Vec<u64> {
        vec![self.seed, self.inc]
    }
}

/// PCG-RXS-M-XS 64/64 with runtime stream selection.
///
/// Signature: `pcg_stream(position: u64, stream_id: u64) -> u64`
///
/// Like [`Pcg`], but the stream is a wire input rather than a constant.
/// This allows each row or partition to use a different stream while
/// sharing the same seed, producing independent sequences that are
/// statistically uncorrelated.
///
/// Use this when the stream identity is data-dependent (e.g., derived
/// from a partition key) and cannot be fixed at assembly time.
///
/// JIT level: P2 (compiled_u64 closure with captured seed).
pub struct PcgStream {
    meta: NodeMeta,
    seed: u64,
}

impl PcgStream {
    /// Create a new PcgStream node.
    ///
    /// - `seed`: initial LCG state (init-time constant)
    pub fn new(seed: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "pcg_stream".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("position")), Slot::Wire(Port::u64("stream_id"))],
            },
            seed,
        }
    }
}

impl GkNode for PcgStream {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let position = inputs[0].as_u64();
        let stream_id = inputs[1].as_u64();
        let inc = 2u64.wrapping_mul(stream_id).wrapping_add(1);
        outputs[0] = Value::U64(pcg_seek(self.seed, inc, position));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let seed = self.seed;
        Some(Box::new(move |inputs, outputs| {
            let inc = 2u64.wrapping_mul(inputs[1]).wrapping_add(1);
            outputs[0] = pcg_seek(seed, inc, inputs[0]);
        }))
    }
}

/// Bijective permutation of `[0, range)` via cycle-walking over PCG.
///
/// Signature: `cycle_walk(position: u64) -> u64`
///
/// Maps every integer in `[0, range)` to a unique integer in `[0, range)`
/// (a permutation). Internally uses a 3-round Feistel network operating
/// on the bit-width of range, with PCG-derived round keys, then
/// cycle-walks: if the Feistel output is >= range, it is fed back as
/// input. Because the Feistel cipher is a bijection on the power-of-two
/// domain and the mask is at most 2x range, each cycle-walk iteration
/// has >= 50% chance of landing in range, giving fast expected
/// termination (~2 iterations).
///
/// Use this when you need a shuffle or bijective mapping: e.g., visiting
/// every row in a table exactly once in a pseudo-random order, or
/// generating unique IDs without a tracking structure.
///
/// The `range`, `seed`, and `stream` are init-time constants.
///
/// JIT level: P2 (compiled_u64 closure with captured constants).
/// Exposes `jit_constants`: `[range, seed, inc]`.
pub struct CycleWalk {
    meta: NodeMeta,
    range: u64,
    seed: u64,
    inc: u64,
    /// Number of bits per Feistel half (total domain is 2^(2*half_bits)).
    half_bits: u32,
    /// Bitmask for each half: `(1 << half_bits) - 1`.
    half_mask: u64,
    /// Pre-computed round keys derived from seed and stream.
    round_keys: [u64; FEISTEL_ROUNDS],
}

/// Number of Feistel rounds. 6 rounds provides good diffusion.
const FEISTEL_ROUNDS: usize = 6;

impl CycleWalk {
    /// Create a new CycleWalk node.
    ///
    /// - `range`: the permutation domain `[0, range)`
    /// - `seed`: initial LCG state
    /// - `stream`: stream selector; the LCG increment is `2 * stream + 1`
    ///
    /// # Panics
    ///
    /// Panics if `range` is 0.
    pub fn new(range: u64, seed: u64, stream: u64) -> Self {
        assert!(range > 0, "CycleWalk range must be > 0");
        let inc = 2u64.wrapping_mul(stream).wrapping_add(1);

        // Compute the total bit width needed, then round up to even
        // so the Feistel halves are balanced.
        let min_bits = if range <= 1 {
            2 // minimum 2 bits for a balanced Feistel
        } else {
            let b = 64 - (range - 1).leading_zeros();
            if b % 2 != 0 { b + 1 } else { b.max(2) }
        };
        let half_bits = min_bits / 2;
        let half_mask = (1u64 << half_bits) - 1;

        // Derive round keys from seed and inc using the PCG itself.
        let mut round_keys = [0u64; FEISTEL_ROUNDS];
        for (i, key) in round_keys.iter_mut().enumerate() {
            *key = pcg_seek(seed, inc, i as u64 + 1_000_000_000);
        }

        Self {
            meta: NodeMeta {
                name: "cycle_walk".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("position")),
                    Slot::const_u64("range", range),
                    Slot::const_u64("seed", seed),
                    Slot::const_u64("inc", inc),
                ],
            },
            range,
            seed,
            inc,
            half_bits,
            half_mask,
            round_keys,
        }
    }
}

/// Feistel round function: mix the half-block with a round key.
#[inline]
fn feistel_round_fn(half: u64, round_key: u64) -> u64 {
    let x = half.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(round_key);
    let x = ((x >> 32) ^ x).wrapping_mul(0xD6E8FEB86659FD93);
    (x >> 32) ^ x
}

/// Apply a balanced Feistel network: a bijection on `[0, 2^total_bits)`.
///
/// The value is split into two halves of `half_bits` each (total_bits
/// is always even -- we round up). Standard 6-round balanced Feistel
/// with pre-computed round keys ensures bijectivity.
#[inline]
fn feistel_encrypt(
    value: u64,
    half_bits: u32,
    half_mask: u64,
    round_keys: &[u64; FEISTEL_ROUNDS],
) -> u64 {
    let mut left = (value >> half_bits) & half_mask;
    let mut right = value & half_mask;

    for key in round_keys.iter() {
        let new_right = left ^ (feistel_round_fn(right, *key) & half_mask);
        left = right;
        right = new_right;
    }

    (left << half_bits) | right
}

/// Apply cycle-walking with the Feistel bijection.
///
/// The input `value` is first reduced to `[0, range)` via modular
/// reduction so that out-of-range inputs are accepted gracefully.
/// Starting from a value in `[0, range)`, cycle-walking is guaranteed
/// to terminate because the Feistel permutation's cycle through that
/// value must re-enter `[0, range)`.
#[inline]
fn cycle_walk_inner(
    mut value: u64,
    range: u64,
    half_bits: u32,
    half_mask: u64,
    round_keys: &[u64; FEISTEL_ROUNDS],
) -> u64 {
    if range == 1 {
        return 0;
    }
    // Ensure we start in [0, range) so cycle-walk terminates.
    value = value % range;
    loop {
        value = feistel_encrypt(value, half_bits, half_mask, round_keys);
        if value < range {
            return value;
        }
    }
}

impl GkNode for CycleWalk {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let position = inputs[0].as_u64();
        outputs[0] = Value::U64(cycle_walk_inner(
            position, self.range, self.half_bits, self.half_mask, &self.round_keys,
        ));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let range = self.range;
        let half_bits = self.half_bits;
        let half_mask = self.half_mask;
        let round_keys = self.round_keys;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = cycle_walk_inner(inputs[0], range, half_bits, half_mask, &round_keys);
        }))
    }

    fn jit_constants(&self) -> Vec<u64> {
        vec![self.range, self.seed, self.inc]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ----- pcg_seek / pcg_output unit tests -----

    #[test]
    fn pcg_output_deterministic() {
        // Same state must always produce the same output.
        let a = pcg_output(123456789);
        let b = pcg_output(123456789);
        assert_eq!(a, b);
    }

    #[test]
    fn pcg_seek_position_zero_vs_one() {
        let seed = 42u64;
        let inc = 1u64; // stream 0
        let v0 = pcg_seek(seed, inc, 0);
        let v1 = pcg_seek(seed, inc, 1);
        assert_ne!(v0, v1, "different positions must produce different values");
    }

    #[test]
    fn pcg_seek_deterministic() {
        let seed = 0xDEAD_BEEF;
        let inc = 3;
        let a = pcg_seek(seed, inc, 1000);
        let b = pcg_seek(seed, inc, 1000);
        assert_eq!(a, b);
    }

    #[test]
    fn pcg_seek_sequential_matches_step() {
        // Verify that seek(N) produces the same result as stepping
        // through the LCG N times.
        let seed = 77u64;
        let inc = 5u64;
        let n = 50u64;

        // Step through manually
        let mut state = seed;
        for _ in 0..n {
            state = state.wrapping_mul(MULT).wrapping_add(inc);
        }
        let stepped = pcg_output(state);

        let seeked = pcg_seek(seed, inc, n);
        assert_eq!(stepped, seeked,
            "seek({n}) must match {n} sequential LCG steps");
    }

    // ----- Pcg node tests -----

    #[test]
    fn pcg_node_deterministic() {
        let node = Pcg::new(42, 0);
        let mut out = [Value::None];
        node.eval(&[Value::U64(100)], &mut out);
        let first = out[0].as_u64();
        node.eval(&[Value::U64(100)], &mut out);
        assert_eq!(first, out[0].as_u64(), "same position must give same result");
    }

    #[test]
    fn pcg_node_different_positions() {
        let node = Pcg::new(42, 0);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(0)], &mut out1);
        node.eval(&[Value::U64(1)], &mut out2);
        assert_ne!(out1[0].as_u64(), out2[0].as_u64());
    }

    #[test]
    fn pcg_node_different_seeds() {
        let a = Pcg::new(1, 0);
        let b = Pcg::new(2, 0);
        let mut out_a = [Value::None];
        let mut out_b = [Value::None];
        a.eval(&[Value::U64(50)], &mut out_a);
        b.eval(&[Value::U64(50)], &mut out_b);
        assert_ne!(out_a[0].as_u64(), out_b[0].as_u64(),
            "different seeds should produce different values");
    }

    #[test]
    fn pcg_node_different_streams() {
        let a = Pcg::new(42, 0);
        let b = Pcg::new(42, 1);
        let mut out_a = [Value::None];
        let mut out_b = [Value::None];
        a.eval(&[Value::U64(50)], &mut out_a);
        b.eval(&[Value::U64(50)], &mut out_b);
        assert_ne!(out_a[0].as_u64(), out_b[0].as_u64(),
            "different streams should produce different values");
    }

    #[test]
    fn pcg_compiled_matches_eval() {
        let node = Pcg::new(99, 7);
        let compiled = node.compiled_u64().expect("Pcg must provide compiled_u64");
        for pos in 0..100u64 {
            let mut eval_out = [Value::None];
            node.eval(&[Value::U64(pos)], &mut eval_out);
            let mut comp_out = [0u64];
            compiled(&[pos], &mut comp_out);
            assert_eq!(eval_out[0].as_u64(), comp_out[0],
                "compiled and eval must agree at position {pos}");
        }
    }

    #[test]
    fn pcg_jit_constants() {
        let node = Pcg::new(42, 7);
        let consts = node.jit_constants();
        assert_eq!(consts.len(), 2);
        assert_eq!(consts[0], 42, "first constant is seed");
        assert_eq!(consts[1], 2 * 7 + 1, "second constant is inc = 2*stream+1");
    }

    // ----- PcgStream node tests -----

    #[test]
    fn pcg_stream_deterministic() {
        let node = PcgStream::new(42);
        let mut out = [Value::None];
        node.eval(&[Value::U64(100), Value::U64(3)], &mut out);
        let first = out[0].as_u64();
        node.eval(&[Value::U64(100), Value::U64(3)], &mut out);
        assert_eq!(first, out[0].as_u64());
    }

    #[test]
    fn pcg_stream_independence() {
        let node = PcgStream::new(42);
        let mut out_a = [Value::None];
        let mut out_b = [Value::None];
        node.eval(&[Value::U64(50), Value::U64(0)], &mut out_a);
        node.eval(&[Value::U64(50), Value::U64(1)], &mut out_b);
        assert_ne!(out_a[0].as_u64(), out_b[0].as_u64(),
            "different stream_ids should produce different values");
    }

    #[test]
    fn pcg_stream_matches_fixed_pcg() {
        // PcgStream with a fixed stream_id should produce the same
        // output as Pcg constructed with that stream.
        let fixed = Pcg::new(42, 5);
        let dynamic = PcgStream::new(42);
        for pos in 0..50u64 {
            let mut f_out = [Value::None];
            let mut d_out = [Value::None];
            fixed.eval(&[Value::U64(pos)], &mut f_out);
            dynamic.eval(&[Value::U64(pos), Value::U64(5)], &mut d_out);
            assert_eq!(f_out[0].as_u64(), d_out[0].as_u64(),
                "PcgStream must match Pcg for same seed/stream at position {pos}");
        }
    }

    #[test]
    fn pcg_stream_compiled_matches_eval() {
        let node = PcgStream::new(99);
        let compiled = node.compiled_u64().expect("PcgStream must provide compiled_u64");
        for pos in 0..50u64 {
            for stream in 0..5u64 {
                let mut eval_out = [Value::None];
                node.eval(&[Value::U64(pos), Value::U64(stream)], &mut eval_out);
                let mut comp_out = [0u64];
                compiled(&[pos, stream], &mut comp_out);
                assert_eq!(eval_out[0].as_u64(), comp_out[0],
                    "compiled and eval must agree at pos={pos}, stream={stream}");
            }
        }
    }

    // ----- CycleWalk node tests -----

    #[test]
    fn cycle_walk_bounded() {
        let node = CycleWalk::new(100, 42, 0);
        let mut out = [Value::None];
        for i in 0..200u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(out[0].as_u64() < 100, "output {} >= range 100", out[0].as_u64());
        }
    }

    #[test]
    fn cycle_walk_deterministic() {
        let node = CycleWalk::new(1000, 42, 0);
        let mut out = [Value::None];
        node.eval(&[Value::U64(77)], &mut out);
        let first = out[0].as_u64();
        node.eval(&[Value::U64(77)], &mut out);
        assert_eq!(first, out[0].as_u64());
    }

    #[test]
    fn cycle_walk_bijective_small() {
        // For inputs [0, range), the mapping must be a permutation:
        // every output is unique and within [0, range).
        let range = 50u64;
        let node = CycleWalk::new(range, 42, 0);
        let mut seen = HashSet::new();
        let mut out = [Value::None];
        for i in 0..range {
            node.eval(&[Value::U64(i)], &mut out);
            let v = out[0].as_u64();
            assert!(v < range, "output {v} out of range [0, {range})");
            assert!(seen.insert(v), "duplicate output {v} at position {i}");
        }
        assert_eq!(seen.len(), range as usize,
            "must produce exactly {range} distinct values");
    }

    #[test]
    fn cycle_walk_bijective_power_of_two() {
        // Powers of two are a common edge case.
        let range = 64u64;
        let node = CycleWalk::new(range, 123, 7);
        let mut seen = HashSet::new();
        let mut out = [Value::None];
        for i in 0..range {
            node.eval(&[Value::U64(i)], &mut out);
            let v = out[0].as_u64();
            assert!(v < range);
            assert!(seen.insert(v), "duplicate at {i}");
        }
        assert_eq!(seen.len(), range as usize);
    }

    #[test]
    fn cycle_walk_compiled_matches_eval() {
        let node = CycleWalk::new(200, 42, 3);
        let compiled = node.compiled_u64().expect("CycleWalk must provide compiled_u64");
        for pos in 0..200u64 {
            let mut eval_out = [Value::None];
            node.eval(&[Value::U64(pos)], &mut eval_out);
            let mut comp_out = [0u64];
            compiled(&[pos], &mut comp_out);
            assert_eq!(eval_out[0].as_u64(), comp_out[0],
                "compiled and eval must agree at position {pos}");
        }
    }

    #[test]
    fn cycle_walk_jit_constants() {
        let node = CycleWalk::new(500, 42, 7);
        let consts = node.jit_constants();
        assert_eq!(consts.len(), 3);
        assert_eq!(consts[0], 500, "first constant is range");
        assert_eq!(consts[1], 42, "second constant is seed");
        assert_eq!(consts[2], 2 * 7 + 1, "third constant is inc");
    }

    #[test]
    #[should_panic(expected = "range must be > 0")]
    fn cycle_walk_zero_range_panics() {
        CycleWalk::new(0, 42, 0);
    }

    #[test]
    fn cycle_walk_range_one() {
        // With range=1, every input must map to 0.
        let node = CycleWalk::new(1, 42, 0);
        let mut out = [Value::None];
        for i in 0..10u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 0);
        }
    }
}
