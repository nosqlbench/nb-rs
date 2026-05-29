// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Deterministic PRNG for strategy-layer use — thin wrapper
//! over polydat's PCG-RXS-M-XS implementation in
//! [`crate::nodes::pcg`].
//!
//! The strategy layer doesn't need its own PRNG family —
//! polydat already has PCG with O(log N) seek as the
//! workhorse generator for deterministic data. This wrapper
//! exposes a stateful `next_u64` / `next_bounded` / `shuffle`
//! interface for strategy use; the underlying algorithm is
//! the same PCG that powers polydat's `pcg(position)` GK
//! node.
//!
//! Determinism: same `(seed, stream)` → same sequence on
//! every materialization. The strategy layer captures the
//! seed at order-instantiation time (per spec §3.6's
//! "Shuffle: PRNG seed captured at materialization"); Phase 7
//! wires the per-streamer seed into the IR interpreter.

use crate::nodes::pcg::pcg_seek;

/// Stateful PCG wrapper. Internally just a `(seed, inc,
/// position)` triple — the underlying `pcg_seek` is a pure
/// function so this struct is a thin convenience.
pub struct Prng {
    seed: u64,
    inc: u64,
    position: u64,
}

impl Prng {
    /// Construct a new PRNG with the given seed. The default
    /// stream is 0; for parallel-independent streams use
    /// [`with_stream`].
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            inc: 1, // 2 * stream + 1 with stream = 0
            position: 0,
        }
    }

    /// Construct with explicit stream selector. Streams
    /// produce statistically independent sequences for the
    /// same seed.
    pub fn with_stream(seed: u64, stream: u64) -> Self {
        Self {
            seed,
            inc: 2u64.wrapping_mul(stream).wrapping_add(1),
            position: 0,
        }
    }

    /// Next `u64` from the stream.
    pub fn next_u64(&mut self) -> u64 {
        let v = pcg_seek(self.seed, self.inc, self.position);
        self.position = self.position.wrapping_add(1);
        v
    }

    /// Next `u64` in `0..n` via rejection sampling (no modulo
    /// bias). `n == 0` returns 0.
    pub fn next_bounded(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        let threshold = ((u64::MAX - n) + 1) % n;
        loop {
            let candidate = self.next_u64();
            if candidate >= threshold {
                return candidate % n;
            }
        }
    }

    /// Fisher-Yates shuffle in place. Stable seed → stable
    /// permutation.
    pub fn shuffle<T>(&mut self, slice: &mut [T]) {
        let n = slice.len();
        if n < 2 {
            return;
        }
        for i in (1..n).rev() {
            let j = self.next_bounded((i as u64) + 1) as usize;
            slice.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn determinism() {
        let mut a = Prng::new(42);
        let mut b = Prng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Prng::new(1);
        let mut b = Prng::new(2);
        let mut differ = false;
        for _ in 0..10 {
            if a.next_u64() != b.next_u64() {
                differ = true;
                break;
            }
        }
        assert!(differ);
    }

    #[test]
    fn different_streams_diverge() {
        let mut a = Prng::with_stream(42, 0);
        let mut b = Prng::with_stream(42, 1);
        let mut differ = false;
        for _ in 0..10 {
            if a.next_u64() != b.next_u64() {
                differ = true;
                break;
            }
        }
        assert!(differ);
    }

    #[test]
    fn bounded_within_range() {
        let mut rng = Prng::new(7);
        for _ in 0..1000 {
            let v = rng.next_bounded(10);
            assert!(v < 10);
        }
    }

    #[test]
    fn shuffle_preserves_elements() {
        let mut rng = Prng::new(123);
        let mut data: Vec<u64> = (0..50).collect();
        rng.shuffle(&mut data);
        let mut sorted = data.clone();
        sorted.sort();
        assert_eq!(sorted, (0..50).collect::<Vec<_>>());
    }

    #[test]
    fn shuffle_deterministic_for_seed() {
        let mut rng1 = Prng::new(42);
        let mut rng2 = Prng::new(42);
        let mut data1: Vec<u64> = (0..20).collect();
        let mut data2: Vec<u64> = (0..20).collect();
        rng1.shuffle(&mut data1);
        rng2.shuffle(&mut data2);
        assert_eq!(data1, data2);
    }
}
