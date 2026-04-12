// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Hash function nodes.

use crate::node::{
    CompiledU64Op,
    GkNode, NodeMeta, Port, Slot, Value,
};
use crate::fusion::{DecomposedGraph, DecomposedWire, FusedNode};
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
pub struct Hash64 {
    meta: NodeMeta,
}

impl Default for Hash64 {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "hash".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for Hash64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        outputs[0] = Value::U64(xxh3_64(&v.to_le_bytes()));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = xxh3_64(&inputs[0].to_le_bytes());
        }))
    }
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
pub struct HashRange {
    meta: NodeMeta,
    max: u64,
}

impl HashRange {
    pub fn new(max: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "hash_range".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("max", max),
                ],
            },
            max,
        }
    }
}

impl GkNode for HashRange {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        let h = xxh3_64(&v.to_le_bytes());
        outputs[0] = Value::U64(h % self.max);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let max = self.max;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = xxh3_64(&inputs[0].to_le_bytes()) % max;
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.max] }
}

impl FusedNode for HashRange {
    /// `hash_range(x, K)` decomposes to `mod(hash(x), K)`.
    fn decomposed(&self) -> DecomposedGraph {
        use crate::nodes::arithmetic::ModU64;
        let mut g = DecomposedGraph::new(1);
        let h = g.add_node(Box::new(Hash64::new()), vec![DecomposedWire::Input(0)]);
        let m = g.add_node(Box::new(ModU64::new(self.max)), vec![DecomposedWire::Node(h, 0)]);
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
/// JIT level: P1 (no compiled_u64; output is f64, not u64).
pub struct HashInterval {
    meta: NodeMeta,
    min: f64,
    max: f64,
}

impl HashInterval {
    pub fn new(min: f64, max: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "hash_interval".into(),
                outs: vec![Port::f64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_f64("min", min),
                    Slot::const_f64("max", max),
                ],
            },
            min,
            max,
        }
    }
}

impl GkNode for HashInterval {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        let h = xxh3_64(&v.to_le_bytes());
        // Map u64 to [0, 1) then scale to [min, max)
        let unit = (h as f64) / (u64::MAX as f64);
        outputs[0] = Value::F64(self.min + unit * (self.max - self.min));
    }
}

impl FusedNode for HashInterval {
    /// `hash_interval(x, lo, hi)` decomposes to `lerp(unit_interval(hash(x)), lo, hi)`.
    fn decomposed(&self) -> DecomposedGraph {
        use crate::nodes::lerp::LerpConst;
        use crate::sampling::icd::UnitInterval;
        let mut g = DecomposedGraph::new(1);
        let h = g.add_node(Box::new(Hash64::new()), vec![DecomposedWire::Input(0)]);
        let ui = g.add_node(Box::new(UnitInterval::new()), vec![DecomposedWire::Node(h, 0)]);
        let lerp = g.add_node(
            Box::new(LerpConst::new(self.min, self.max)),
            vec![DecomposedWire::Node(ui, 0)],
        );
        g.set_outputs(vec![DecomposedWire::Node(lerp, 0)]);
        g
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for hash-related nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "hash", category: C::Hashing, outputs: 1,
            description: "64-bit xxHash3",
            help: "Deterministic 64-bit hash using xxHash3.\nThis is the fundamental entropy source: feed a cycle counter in,\nget pseudo-random bits out. Hash before mod/lerp to avoid patterns.\nParameters:\n  input — any u64 value (typically a cycle ordinal)\nExample: hash(cycle) -> mod(1000)\nTheory: xxHash3 is a non-cryptographic hash with excellent\navalanche properties and very high throughput.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a hash node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "hash" => Some(Ok(Box::new(Hash64::new()))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash64_deterministic() {
        let node = Hash64::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        let first = out[0].as_u64();
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(first, out[0].as_u64(), "same input must produce same output");
    }

    #[test]
    fn hash64_different_inputs_differ() {
        let node = Hash64::new();
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
            assert!(v >= 10.0 && v < 20.0, "got {v}");
        }
    }
}
