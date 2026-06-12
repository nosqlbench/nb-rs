// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Hash function nodes.
//!
//! SRD-80b S8 (Phase E): migrated from hand-written
//! `impl PolydatNode for X` blocks to `#[polydat_node]`
//! free-function authoring. The macro emits the struct,
//! `new()`, `eval()`, `compiled_u64()` (auto-emitted because
//! every arg + return maps to a `JitType`), `jit_constants()`
//! (carrying the captured `Const<...>` field values), and
//! the inventory registration.
//!
//! Greenfield rename: the historical `Hash64` Rust struct
//! is now `Hash` — the macro derives PascalCase struct names
//! from the snake_case function ident, and the operator-facing
//! DSL name "hash" already matched. No alias shim per
//! SRD-80b §"No transitional aliases".

use crate::compile::fusion::{DecomposedGraph, DecomposedWire, FusedNode};
use crate::derive_support::Const;
use xxhash_rust::xxh3::xxh3_64;

/// 64-bit hash using xxHash3.
///
/// Signature: `hash(input: u64) -> (u64)`
///
/// The fundamental entropy source for deterministic data generation.
/// Place at the head of nearly every pipeline to scatter sequential
/// cycle counters into uniformly distributed u64 values. The output
/// feeds directly into `hash_range`, `unit_interval`, distribution
/// samplers, or any node that expects pseudo-random input.
///
/// JIT level: P2 (compiled_u64 closure; xxh3 call prevents full inlining).
#[crate::polydat_node(category = Hashing)]
fn hash(input: u64) -> u64 {
    xxh3_64(&input.to_le_bytes())
}

/// Hash a u64 into a bounded range `[0, max)`.
///
/// Signature: `hash_range(input: u64, max: u64) -> (u64)`
///
/// Combines hashing and modular reduction in a single node. Use when
/// you need a bounded integer directly, for example selecting a row
/// index: `hash_range(cycle, 1_000_000)` gives a uniformly distributed
/// key in [0, 1M). Equivalent to `hash(cycle) % max` but expressed as
/// one composable node.
///
/// JIT level: P2 (compiled_u64 closure with captured `max`).
#[crate::polydat_node(category = Hashing)]
fn hash_range(input: u64, max: Const<u64>) -> u64 {
    xxh3_64(&input.to_le_bytes()) % *max
}

impl FusedNode for HashRange {
    /// `hash_range(x, K)` decomposes to `mod(hash(x), K)`.
    fn decomposed(&self) -> DecomposedGraph {
        use crate::library::arithmetic::Mod;
        let mut g = DecomposedGraph::new(1);
        let h = g.add_node(Box::new(Hash::new()), vec![DecomposedWire::Input(0)]);
        let m = g.add_node(Box::new(Mod::new(self.max)), vec![DecomposedWire::Node(h, 0)]);
        g.set_outputs(vec![DecomposedWire::Node(m, 0)]);
        g
    }
}

/// Hash a u64 into a float interval `[min, max)`.
///
/// Signature: `hash_interval(input: u64, min: f64, max: f64) -> (f64)`
///
/// Convenience node that hashes, normalizes to [0,1), and scales in one
/// step. Useful when a uniform f64 in a specific range is needed without
/// wiring separate `hash` + `unit_interval` + `lerp` nodes. Example:
/// `hash_interval(cycle, 0.0, 360.0)` produces a random bearing.
///
/// JIT level: P2 (compiled_u64 closure with captured `min`/`max`).
#[crate::polydat_node(category = Hashing)]
fn hash_interval(input: u64, min: Const<f64>, max: Const<f64>) -> f64 {
    let h = xxh3_64(&input.to_le_bytes());
    // Map u64 to [0, 1) then scale to [min, max)
    let unit = (h as f64) / (u64::MAX as f64);
    *min + unit * (*max - *min)
}

impl FusedNode for HashInterval {
    /// `hash_interval(x, lo, hi)` decomposes to `lerp(unit_interval(hash(x)), lo, hi)`.
    fn decomposed(&self) -> DecomposedGraph {
        use crate::library::lerp::Lerp;
        use crate::library::sampling::icd::UnitInterval;
        let mut g = DecomposedGraph::new(1);
        let h = g.add_node(Box::new(Hash::new()), vec![DecomposedWire::Input(0)]);
        let ui = g.add_node(Box::new(UnitInterval::new()), vec![DecomposedWire::Node(h, 0)]);
        let lerp = g.add_node(
            Box::new(Lerp::new(self.min, self.max)),
            vec![DecomposedWire::Node(ui, 0)],
        );
        g.set_outputs(vec![DecomposedWire::Node(lerp, 0)]);
        g
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn hash_deterministic() {
        let node = Hash::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        let first = out[0].as_u64();
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(first, out[0].as_u64(), "same input must produce same output");
    }

    #[test]
    fn hash_different_inputs_differ() {
        let node = Hash::new();
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(0)], &mut out1);
        node.eval(&[Value::U64(1)], &mut out2);
        assert_ne!(out1[0].as_u64(), out2[0].as_u64());
    }

    #[test]
    fn hash_range_bounded() {
        let node = HashRange::new(100);
        let mut out = [Value::None];
        for i in 0..1000 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(out[0].as_u64() < 100);
        }
    }

    #[test]
    fn hash_interval_bounded() {
        let node = HashInterval::new(10.0, 20.0);
        let mut out = [Value::None];
        for i in 0..1000 {
            node.eval(&[Value::U64(i)], &mut out);
            let v = out[0].as_f64();
            assert!((10.0..20.0).contains(&v), "got {v}");
        }
    }
}
