// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Galois LFSR-based deterministic permutation (MetaShift / Shuffle).
//!
//! Provides bijective, deterministic, O(1)-space permutations of integer
//! ranges. Given a range [0, N), the LFSR visits every value exactly once
//! before cycling, in a pseudo-random order determined by the feedback
//! polynomial.
//!
//! This is useful for:
//! - Generating all values in a range without repetition or memory
//! - Shuffling sequences without materializing them
//! - Deterministic reordering across distributed workers (via bank selection)
//!
//! The core algorithm is a Galois-configuration LFSR. The `Shuffle` GK
//! node wraps it with range normalization and rejection sampling.

use crate::node::{Commutativity, CompiledU64Op, GkNode, NodeMeta, Port, Value};

// -----------------------------------------------------------------
// LFSR feedback polynomials (one per register width 4..64)
// -----------------------------------------------------------------

/// Number of banks (feedback polynomials) stored per register width.
const BANKS_PER_WIDTH: usize = 8;

/// Galois LFSR feedback polynomials, 8 banks per register width 4..64.
/// Indexed as FEEDBACK_BANKS[(width - 4) * 8 + bank].
/// Widths with fewer than 8 known polynomials repeat the last one.
const FEEDBACK_BANKS: [u64; 61 * BANKS_PER_WIDTH] = include!("metashift_banks.inc");

/// Return the feedback polynomial for a given register width and bank.
///
/// `width` must be 4..=64. `bank` selects among different polynomials
/// for the same width (modulo the number of available banks). Different
/// banks produce different permutation orderings over the same range.
fn feedback_for_width_and_bank(width: u32, bank: usize) -> u64 {
    assert!((4..=64).contains(&width), "LFSR width must be 4..64, got {width}");
    let base = (width as usize - 4) * BANKS_PER_WIDTH;
    FEEDBACK_BANKS[base + (bank % BANKS_PER_WIDTH)]
}

/// Return the default (bank 0) feedback polynomial for a given width.
#[allow(dead_code)]
fn feedback_for_width(width: u32) -> u64 {
    feedback_for_width_and_bank(width, 0)
}

/// Return the minimum register width needed to represent `period` values.
fn width_for_period(period: u64) -> u32 {
    assert!(period > 0, "period must be positive");
    let bits = 64 - period.leading_zeros();
    bits.max(4) // minimum 4-bit LFSR
}

// -----------------------------------------------------------------
// Core LFSR step
// -----------------------------------------------------------------

/// Single Galois LFSR step.
///
/// This is the fundamental bijective operation: given a register value,
/// produce the next value in the LFSR sequence.
#[inline]
fn lfsr_step(register: u64, feedback: u64) -> u64 {
    let lsb = register & 1;
    let shifted = register >> 1;
    // If LSB was 1, XOR with feedback polynomial; otherwise just shift.
    // The (-lsb) trick: if lsb=1, -1u64 = all 1s (mask passes feedback);
    // if lsb=0, 0u64 (mask blocks feedback).
    shifted ^ (lsb.wrapping_neg() & feedback)
}

// -----------------------------------------------------------------
// Shuffle: bounded bijective permutation
// -----------------------------------------------------------------

/// Configuration for a bounded LFSR shuffle.
struct ShuffleConfig {
    feedback: u64,
    size: u64,
    min: u64,
}

/// Deterministic, bijective permutation of a bounded integer range.
///
/// Signature: `shuffle(input: u64, min: u64, size: u64) -> (u64)`
///
/// Maps every value in [min, min+size) to itself in a pseudo-random
/// order, visiting each value exactly once per cycle. Uses a Galois
/// LFSR with rejection sampling to handle ranges that are not exact
/// powers of 2.
///
/// Use when you need every key in a range visited exactly once without
/// repetition and without materializing the full sequence in memory.
/// Common patterns: generating unique primary keys for bulk inserts
/// (`shuffle(cycle, 0, 10_000_000)`), distributing work across
/// partitions without collision, or simulating a deck-of-cards draw.
/// Select different `bank` values for independent permutation orderings
/// across distributed workers.
///
/// JIT level: P3 (compiled_u64 with jit_constants for feedback, size,
/// and min; the LFSR loop compiles to a tight branch sequence).
pub struct Shuffle {
    meta: NodeMeta,
    config: ShuffleConfig,
}

impl Shuffle {
    /// Create a shuffle over [min, min + size) using bank 0.
    pub fn new(min: u64, size: u64) -> Self {
        Self::with_bank(min, size, 0)
    }

    /// Create a shuffle over [min, min + size) with a specific bank.
    ///
    /// Different banks produce different permutation orderings over the
    /// same range. Useful for distributed workers that need different
    /// but reproducible shuffles.
    pub fn with_bank(min: u64, size: u64, bank: usize) -> Self {
        assert!(size > 0, "shuffle size must be positive");
        let width = width_for_period(size);
        let feedback = feedback_for_width_and_bank(width, bank);
        Self {
            meta: NodeMeta {
                name: "shuffle".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
                commutativity: Commutativity::Positional,
            },
            config: ShuffleConfig { feedback, size, min },
        }
    }

    /// Create a shuffle over [0, size) using bank 0.
    pub fn zero_based(size: u64) -> Self {
        Self::new(0, size)
    }

    /// Create a shuffle over [0, size) with a specific bank.
    pub fn zero_based_with_bank(size: u64, bank: usize) -> Self {
        Self::with_bank(0, size, bank)
    }

    /// Apply the shuffle to a single value.
    #[inline]
    fn apply(&self, input: u64) -> u64 {
        // Normalize to 1-based LFSR range (LFSR cannot produce 0)
        let mut register = (input % self.config.size) + 1;

        // Apply LFSR with rejection sampling: if result exceeds size,
        // step again until it's in range.
        loop {
            register = lfsr_step(register, self.config.feedback);
            if register <= self.config.size {
                break;
            }
        }

        // Denormalize back to [min, min+size)
        (register - 1) + self.config.min
    }
}

impl GkNode for Shuffle {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.apply(inputs[0].as_u64()));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let feedback = self.config.feedback;
        let size = self.config.size;
        let min = self.config.min;
        Some(Box::new(move |inputs, outputs| {
            let mut register = (inputs[0] % size) + 1;
            loop {
                register = lfsr_step(register, feedback);
                if register <= size {
                    break;
                }
            }
            outputs[0] = (register - 1) + min;
        }))
    }

    fn jit_constants(&self) -> Vec<u64> {
        vec![self.config.feedback, self.config.size, self.config.min]
    }
}

// -----------------------------------------------------------------
// Raw LFSR step as a GK node (for advanced use)
// -----------------------------------------------------------------

/// Single Galois LFSR step as a GK node.
///
/// Signature: `(input: u64) -> (u64)`
///
/// This is the raw bijective LFSR operation without range bounding.
/// The period is 2^width - 1. Useful for building custom permutation
/// patterns.
pub struct LfsrStep {
    meta: NodeMeta,
    feedback: u64,
}

impl LfsrStep {
    /// Create for a specific register width (4..=64) using bank 0.
    pub fn new(width: u32) -> Self {
        Self::with_bank(width, 0)
    }

    /// Create with a specific bank for a different permutation ordering.
    pub fn with_bank(width: u32, bank: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "lfsr_step".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
                commutativity: Commutativity::Positional,
            },
            feedback: feedback_for_width_and_bank(width, bank),
        }
    }
}

impl GkNode for LfsrStep {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(lfsr_step(inputs[0].as_u64(), self.feedback));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let feedback = self.feedback;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = lfsr_step(inputs[0], feedback);
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lfsr_step_nonzero() {
        // LFSR should never produce 0 from a nonzero input
        let feedback = feedback_for_width(8);
        let mut reg = 1u64;
        for _ in 0..255 {
            reg = lfsr_step(reg, feedback);
            assert_ne!(reg, 0, "LFSR must never produce 0");
        }
    }

    #[test]
    fn lfsr_full_cycle() {
        // An 8-bit LFSR should visit all 255 nonzero values exactly once
        let feedback = feedback_for_width(8);
        let mut seen = vec![false; 256];
        let mut reg = 1u64;
        for _ in 0..255 {
            reg = lfsr_step(reg, feedback);
            assert!(!seen[reg as usize], "duplicate value {reg}");
            seen[reg as usize] = true;
        }
        // Verify all nonzero values visited
        for i in 1..=255u64 {
            assert!(seen[i as usize], "value {i} not visited");
        }
    }

    #[test]
    fn lfsr_period_returns_to_start() {
        let feedback = feedback_for_width(8);
        let start = 42u64;
        let mut reg = start;
        for _ in 0..255 {
            reg = lfsr_step(reg, feedback);
        }
        assert_eq!(reg, start, "LFSR should return to start after 2^N-1 steps");
    }

    #[test]
    fn shuffle_bijective_small() {
        // Shuffle over [0, 31) should produce a permutation
        let shuf = Shuffle::zero_based(31);
        let mut seen = vec![false; 31];
        for i in 0..31u64 {
            let out = shuf.apply(i);
            assert!(out < 31, "out of range: {out}");
            assert!(!seen[out as usize], "duplicate at input {i}: {out}");
            seen[out as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "not all values produced");
    }

    #[test]
    fn shuffle_bijective_non_power_of_two() {
        // Shuffle over [0, 50) — not a power of 2, requires rejection sampling
        let shuf = Shuffle::zero_based(50);
        let mut seen = vec![false; 50];
        for i in 0..50u64 {
            let out = shuf.apply(i);
            assert!(out < 50, "out of range: {out}");
            assert!(!seen[out as usize], "duplicate at input {i}: {out}");
            seen[out as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "not all values produced");
    }

    #[test]
    fn shuffle_with_min_offset() {
        let shuf = Shuffle::new(100, 20);
        let mut seen = vec![false; 20];
        for i in 0..20u64 {
            let out = shuf.apply(i);
            assert!((100..120).contains(&out), "out of range: {out}");
            seen[(out - 100) as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "not all values produced");
    }

    #[test]
    fn shuffle_deterministic() {
        let shuf = Shuffle::zero_based(100);
        let a = shuf.apply(42);
        let b = shuf.apply(42);
        assert_eq!(a, b);
    }

    #[test]
    fn shuffle_not_identity() {
        // The shuffle should reorder, not pass through
        let shuf = Shuffle::zero_based(100);
        let mut identity_count = 0;
        for i in 0..100u64 {
            if shuf.apply(i) == i {
                identity_count += 1;
            }
        }
        // Some fixed points are expected, but not all
        assert!(identity_count < 50, "shuffle should reorder most values");
    }

    #[test]
    fn shuffle_gk_node() {
        let node = Shuffle::zero_based(100);
        let mut out = [Value::None];
        node.eval(&[Value::U64(7)], &mut out);
        assert!(out[0].as_u64() < 100);
    }

    #[test]
    fn shuffle_compiled() {
        let node = Shuffle::zero_based(100);
        let op = node.compiled_u64().expect("should compile");
        let mut out = [0u64];
        op(&[7], &mut out);
        assert!(out[0] < 100);

        // Matches eval path
        let mut eval_out = [Value::None];
        node.eval(&[Value::U64(7)], &mut eval_out);
        assert_eq!(out[0], eval_out[0].as_u64());
    }

    #[test]
    fn lfsr_step_node() {
        let node = LfsrStep::new(8);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1)], &mut out);
        let v = out[0].as_u64();
        assert_ne!(v, 0);
        assert_ne!(v, 1);
    }

    #[test]
    fn shuffle_large_range() {
        // Verify shuffle works for a larger range (1000)
        let shuf = Shuffle::zero_based(1000);
        let mut seen = vec![false; 1000];
        for i in 0..1000u64 {
            let out = shuf.apply(i);
            assert!(out < 1000, "out of range: {out}");
            seen[out as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "not all values produced");
    }

    #[test]
    fn different_banks_different_orderings() {
        let shuf0 = Shuffle::zero_based_with_bank(100, 0);
        let shuf1 = Shuffle::zero_based_with_bank(100, 1);
        // Both should be bijective permutations
        let mut seen0 = vec![false; 100];
        let mut seen1 = vec![false; 100];
        let mut differ = false;
        for i in 0..100u64 {
            let a = shuf0.apply(i);
            let b = shuf1.apply(i);
            assert!(a < 100);
            assert!(b < 100);
            seen0[a as usize] = true;
            seen1[b as usize] = true;
            if a != b {
                differ = true;
            }
        }
        assert!(seen0.iter().all(|&s| s), "bank 0 not bijective");
        assert!(seen1.iter().all(|&s| s), "bank 1 not bijective");
        assert!(differ, "different banks should produce different orderings");
    }

    #[test]
    fn width_for_period_table() {
        assert_eq!(width_for_period(1), 4);   // minimum is 4
        assert_eq!(width_for_period(15), 4);  // 15 < 2^4
        assert_eq!(width_for_period(16), 5);  // 16 = 2^4, needs 5 bits
        assert_eq!(width_for_period(31), 5);
        assert_eq!(width_for_period(32), 6);
        assert_eq!(width_for_period(255), 8);
        assert_eq!(width_for_period(256), 9);
        assert_eq!(width_for_period(1000), 10);
    }
}
