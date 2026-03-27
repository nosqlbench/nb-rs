// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Vose's alias method for O(1) sampling from discrete distributions.
//!
//! Given N outcomes with associated weights, an alias table pre-computes
//! a structure that allows selecting an outcome in constant time from a
//! single uniform u64 input.
//!
//! Two variants:
//! - [`AliasTable<T>`]: generic, works with any `Clone` outcome type.
//! - [`AliasTableU64`]: flat parallel arrays, optimized for the Phase 2
//!   compiled kernel path where outcomes are indices 0..N.

use std::collections::VecDeque;

// -----------------------------------------------------------------
// Generic alias table
// -----------------------------------------------------------------

struct AliasSlot<T> {
    bias: f64,
    primary: T,
    alias: T,
}

/// Generic alias table for O(1) weighted sampling.
///
/// Construct with [`AliasTable::from_weights`] or [`AliasTable::uniform`],
/// then sample with [`AliasTable::sample`].
pub struct AliasTable<T> {
    slots: Vec<AliasSlot<T>>,
}

impl<T: Clone> AliasTable<T> {
    /// Build an alias table from outcomes and their weights.
    ///
    /// Weights do not need to be normalized — they are scaled
    /// internally. All weights must be non-negative; at least one
    /// must be positive.
    pub fn from_weights(outcomes: &[T], weights: &[f64]) -> Self {
        assert_eq!(outcomes.len(), weights.len(), "outcomes and weights must have equal length");
        let n = outcomes.len();
        assert!(n > 0, "must have at least one outcome");

        let sum: f64 = weights.iter().sum();
        assert!(sum > 0.0, "total weight must be positive");

        // Normalize so weights sum to N
        let scale = n as f64 / sum;
        let mut scaled: Vec<f64> = weights.iter().map(|w| w * scale).collect();

        // Partition into small and large queues
        let mut small: VecDeque<usize> = VecDeque::new();
        let mut large: VecDeque<usize> = VecDeque::new();
        for (i, &w) in scaled.iter().enumerate() {
            if w < 1.0 {
                small.push_back(i);
            } else {
                large.push_back(i);
            }
        }

        // Build slots
        let mut slots: Vec<AliasSlot<T>> = (0..n)
            .map(|i| AliasSlot {
                bias: 1.0,
                primary: outcomes[i].clone(),
                alias: outcomes[i].clone(),
            })
            .collect();

        while let (Some(s), Some(l)) = (small.pop_front(), large.pop_front()) {
            slots[s].bias = scaled[s];
            slots[s].alias = outcomes[l].clone();

            scaled[l] -= 1.0 - scaled[s];
            if scaled[l] < 1.0 {
                small.push_back(l);
            } else {
                large.push_back(l);
            }
        }

        // Remaining items (due to floating-point drift) are their own alias
        for &i in small.iter().chain(large.iter()) {
            slots[i].bias = 1.0;
        }

        Self { slots }
    }

    /// Build a uniform alias table (all outcomes equally weighted).
    pub fn uniform(outcomes: &[T]) -> Self {
        let weights = vec![1.0; outcomes.len()];
        Self::from_weights(outcomes, &weights)
    }

    /// Sample an outcome from a uniform u64 input.
    ///
    /// The input is split into two independent parts: the low bits
    /// select a slot, the high bits determine the bias test. This
    /// avoids correlation between slot selection and the bias coin
    /// flip.
    #[inline]
    pub fn sample(&self, input: u64) -> &T {
        let n = self.slots.len();
        let slot_idx = (input as usize) % n;
        // Use upper bits for the bias test (independent of slot selection)
        let frac = (input >> 32) as f64 / u32::MAX as f64;
        let slot = &self.slots[slot_idx];
        if frac < slot.bias {
            &slot.primary
        } else {
            &slot.alias
        }
    }

    /// Number of outcomes in the table.
    pub fn len(&self) -> usize {
        self.slots.len()
    }
}

// -----------------------------------------------------------------
// u64-specialized alias table (flat parallel arrays)
// -----------------------------------------------------------------

/// Alias table optimized for u64 outcomes (indices 0..N).
///
/// Uses three parallel arrays for cache-friendly access. Suitable
/// for the Phase 2 compiled kernel path.
pub struct AliasTableU64 {
    biases: Vec<f64>,
    primaries: Vec<u64>,
    aliases: Vec<u64>,
}

impl AliasTableU64 {
    /// Build from weights. Outcomes are implicitly 0..N.
    pub fn from_weights(weights: &[f64]) -> Self {
        let n = weights.len();
        assert!(n > 0, "must have at least one outcome");

        let sum: f64 = weights.iter().sum();
        assert!(sum > 0.0, "total weight must be positive");

        let scale = n as f64 / sum;
        let mut scaled: Vec<f64> = weights.iter().map(|w| w * scale).collect();

        let mut small: VecDeque<usize> = VecDeque::new();
        let mut large: VecDeque<usize> = VecDeque::new();
        for (i, &w) in scaled.iter().enumerate() {
            if w < 1.0 {
                small.push_back(i);
            } else {
                large.push_back(i);
            }
        }

        let mut biases = vec![1.0f64; n];
        let primaries: Vec<u64> = (0..n as u64).collect();
        let mut aliases: Vec<u64> = (0..n as u64).collect();

        while let (Some(s), Some(l)) = (small.pop_front(), large.pop_front()) {
            biases[s] = scaled[s];
            aliases[s] = l as u64;

            scaled[l] -= 1.0 - scaled[s];
            if scaled[l] < 1.0 {
                small.push_back(l);
            } else {
                large.push_back(l);
            }
        }

        for &i in small.iter().chain(large.iter()) {
            biases[i] = 1.0;
        }

        Self { biases, primaries, aliases }
    }

    /// Build a uniform table (all outcomes equally weighted).
    pub fn uniform(n: usize) -> Self {
        Self::from_weights(&vec![1.0; n])
    }

    /// Sample an outcome index from a uniform u64 input.
    ///
    /// Low bits select the slot, high bits test the bias.
    #[inline]
    pub fn sample(&self, input: u64) -> u64 {
        let n = self.biases.len();
        let slot_idx = (input as usize) % n;
        let frac = (input >> 32) as f64 / u32::MAX as f64;
        if frac < self.biases[slot_idx] {
            self.primaries[slot_idx]
        } else {
            self.aliases[slot_idx]
        }
    }

    /// Number of outcomes in the table.
    pub fn len(&self) -> usize {
        self.biases.len()
    }

    /// Access the bias array (for compiled kernel closure capture).
    pub fn biases(&self) -> &[f64] {
        &self.biases
    }

    /// Access the primary outcome array.
    pub fn primaries(&self) -> &[u64] {
        &self.primaries
    }

    /// Access the alias outcome array.
    pub fn aliases(&self) -> &[u64] {
        &self.aliases
    }
}

// -----------------------------------------------------------------
// GK node wrapping the alias table
// -----------------------------------------------------------------

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Value};

/// GK node that samples from a pre-built alias table.
///
/// Signature: `(input: u64) -> (u64)`
///
/// The input is a uniform u64 (hash upstream for pseudo-random
/// dispersion). The output is an outcome index.
pub struct AliasSample {
    meta: NodeMeta,
    table: AliasTableU64,
}

impl AliasSample {
    /// Create from explicit weights. Outcomes are 0..weights.len().
    pub fn from_weights(weights: &[f64]) -> Self {
        Self {
            meta: NodeMeta {
                name: "alias_sample".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
            },
            table: AliasTableU64::from_weights(weights),
        }
    }

    /// Create a uniform sampler over 0..n.
    pub fn uniform(n: usize) -> Self {
        Self::from_weights(&vec![1.0; n])
    }
}

impl GkNode for AliasSample {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.table.sample(inputs[0].as_u64()));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        // Build a standalone table for the closure to capture
        let biases = self.table.biases.clone();
        let primaries = self.table.primaries.clone();
        let aliases = self.table.aliases.clone();

        let n_usize = biases.len();
        Some(Box::new(move |inputs, outputs| {
            let input = inputs[0];
            let slot_idx = (input as usize) % n_usize;
            let frac = (input >> 32) as f64 / u32::MAX as f64;
            if frac < biases[slot_idx] {
                outputs[0] = primaries[slot_idx];
            } else {
                outputs[0] = aliases[slot_idx];
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uniform_table_all_outcomes_reachable() {
        use xxhash_rust::xxh3::xxh3_64;

        let table = AliasTableU64::uniform(4);
        let mut seen = [false; 4];
        for i in 0..10_000u64 {
            let hashed = xxh3_64(&i.to_le_bytes());
            let outcome = table.sample(hashed) as usize;
            assert!(outcome < 4, "outcome {outcome} out of range");
            seen[outcome] = true;
        }
        for (i, &s) in seen.iter().enumerate() {
            assert!(s, "outcome {i} was never sampled");
        }
    }

    #[test]
    fn weighted_table_respects_distribution() {
        use xxhash_rust::xxh3::xxh3_64;

        // Heavily weighted: outcome 0 should dominate.
        // Inputs must be well-distributed (hashed), matching how the
        // GK uses alias tables — hash is always upstream.
        let table = AliasTableU64::from_weights(&[100.0, 1.0, 1.0]);
        let mut counts = [0u64; 3];
        let n = 100_000u64;
        for i in 0..n {
            let hashed = xxh3_64(&i.to_le_bytes());
            counts[table.sample(hashed) as usize] += 1;
        }
        // Outcome 0 has weight 100/102 ≈ 98%
        let ratio = counts[0] as f64 / n as f64;
        assert!(
            ratio > 0.90,
            "expected outcome 0 to dominate, got ratio {ratio} (counts: {counts:?})"
        );
    }

    #[test]
    fn deterministic() {
        let table = AliasTableU64::from_weights(&[1.0, 2.0, 3.0]);
        let a = table.sample(42);
        let b = table.sample(42);
        assert_eq!(a, b, "same input must produce same output");
    }

    #[test]
    fn generic_table_strings() {
        use xxhash_rust::xxh3::xxh3_64;

        let outcomes = vec!["alpha", "beta", "gamma"];
        let weights = vec![1.0, 1.0, 1.0];
        let table = AliasTable::from_weights(&outcomes, &weights);
        let mut seen = [false; 3];
        for i in 0..10_000u64 {
            let hashed = xxh3_64(&i.to_le_bytes());
            let result = *table.sample(hashed);
            match result {
                "alpha" => seen[0] = true,
                "beta" => seen[1] = true,
                "gamma" => seen[2] = true,
                other => panic!("unexpected outcome: {other}"),
            }
        }
        for (i, &s) in seen.iter().enumerate() {
            assert!(s, "outcome {i} never seen");
        }
    }

    #[test]
    fn gk_node_eval() {
        let node = AliasSample::from_weights(&[1.0, 1.0, 1.0, 1.0]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert!(out[0].as_u64() < 4);
    }

    #[test]
    fn gk_node_compiled() {
        let node = AliasSample::from_weights(&[1.0, 1.0, 1.0, 1.0]);
        let op = node.compiled_u64().expect("should compile");
        let mut out = [0u64];
        op(&[42], &mut out);
        assert!(out[0] < 4);

        // Deterministic
        let mut out2 = [0u64];
        op(&[42], &mut out2);
        assert_eq!(out[0], out2[0]);
    }

    #[test]
    fn single_outcome() {
        let table = AliasTableU64::from_weights(&[1.0]);
        for i in 0..1000 {
            assert_eq!(table.sample(i), 0);
        }
    }

    #[test]
    fn two_outcomes_50_50() {
        use xxhash_rust::xxh3::xxh3_64;

        let table = AliasTableU64::from_weights(&[1.0, 1.0]);
        let mut counts = [0u64; 2];
        let n = 100_000u64;
        for i in 0..n {
            let hashed = xxh3_64(&i.to_le_bytes());
            counts[table.sample(hashed) as usize] += 1;
        }
        let ratio = counts[0] as f64 / n as f64;
        assert!(
            (0.40..0.60).contains(&ratio),
            "expected ~50/50, got ratio {ratio}"
        );
    }
}
