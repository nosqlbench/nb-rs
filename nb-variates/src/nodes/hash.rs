// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Hash function nodes.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Value};
use xxhash_rust::xxh3::xxh3_64;

/// 64-bit hash using xxHash3.
///
/// Signature: `(input: u64) -> (u64)`
pub struct Hash64 {
    meta: NodeMeta,
}

impl Hash64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "hash".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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
/// Signature: `(input: u64) -> (u64)`
/// Param: `max: u64`
pub struct HashRange {
    meta: NodeMeta,
    max: u64,
}

impl HashRange {
    pub fn new(max: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "hash_range".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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
}

/// Hash a u64 into a float interval `[min, max)`.
///
/// Signature: `(input: u64) -> (f64)`
/// Params: `min: f64, max: f64`
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
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::f64("output")],
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
