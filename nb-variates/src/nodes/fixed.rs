// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Fixed value and value-list nodes across fundamental types.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, PortType, Value};

// =================================================================
// Constants (0→1 nodes)
// =================================================================

/// Emit a fixed f64 value.
///
/// Signature: `() -> (f64)`
pub struct ConstF64 {
    meta: NodeMeta,
    value: f64,
}

impl ConstF64 {
    pub fn new(value: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "const_f64".into(),
                inputs: vec![],
                outputs: vec![Port::f64("output")],
            },
            value,
        }
    }
}

impl GkNode for ConstF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(self.value);
    }
}

/// Emit a fixed bool value.
///
/// Signature: `() -> (bool)`
pub struct ConstBool {
    meta: NodeMeta,
    value: bool,
}

impl ConstBool {
    pub fn new(value: bool) -> Self {
        Self {
            meta: NodeMeta {
                name: "const_bool".into(),
                inputs: vec![],
                outputs: vec![Port::bool("output")],
            },
            value,
        }
    }
}

impl GkNode for ConstBool {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bool(self.value);
    }
}

// =================================================================
// Fixed value lists (1→1 nodes, input selects by index)
// =================================================================

/// Select from a fixed list of u64 values by index.
///
/// Signature: `(input: u64) -> (u64)`
/// The input is taken modulo the list length.
pub struct FixedValuesU64 {
    meta: NodeMeta,
    values: Vec<u64>,
}

impl FixedValuesU64 {
    pub fn new(values: Vec<u64>) -> Self {
        assert!(!values.is_empty(), "value list must not be empty");
        Self {
            meta: NodeMeta {
                name: "fixed_values_u64".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
            },
            values,
        }
    }
}

impl GkNode for FixedValuesU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = (inputs[0].as_u64() as usize) % self.values.len();
        outputs[0] = Value::U64(self.values[idx]);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let values = self.values.clone();
        Some(Box::new(move |inputs, outputs| {
            let idx = (inputs[0] as usize) % values.len();
            outputs[0] = values[idx];
        }))
    }
}

/// Select from a fixed list of f64 values by index.
///
/// Signature: `(input: u64) -> (f64)`
pub struct FixedValuesF64 {
    meta: NodeMeta,
    values: Vec<f64>,
}

impl FixedValuesF64 {
    pub fn new(values: Vec<f64>) -> Self {
        assert!(!values.is_empty(), "value list must not be empty");
        Self {
            meta: NodeMeta {
                name: "fixed_values_f64".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::f64("output")],
            },
            values,
        }
    }
}

impl GkNode for FixedValuesF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = (inputs[0].as_u64() as usize) % self.values.len();
        outputs[0] = Value::F64(self.values[idx]);
    }
}

/// Select from a fixed list of strings by index.
///
/// Signature: `(input: u64) -> (String)`
pub struct FixedValuesStr {
    meta: NodeMeta,
    values: Vec<String>,
}

impl FixedValuesStr {
    pub fn new(values: Vec<String>) -> Self {
        assert!(!values.is_empty(), "value list must not be empty");
        Self {
            meta: NodeMeta {
                name: "fixed_values_str".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            values,
        }
    }
}

impl GkNode for FixedValuesStr {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = (inputs[0].as_u64() as usize) % self.values.len();
        outputs[0] = Value::Str(self.values[idx].clone());
    }
}

// =================================================================
// CoinFlip: probabilistic boolean
// =================================================================

/// Probabilistic boolean: true with a given probability.
///
/// Signature: `(input: u64) -> (bool)`
///
/// The input is expected to be hashed (uniform). The threshold is
/// precomputed from the probability at init time.
pub struct CoinFlip {
    meta: NodeMeta,
    threshold: u64,
}

impl CoinFlip {
    /// Create with a probability of true in [0.0, 1.0].
    pub fn new(probability: f64) -> Self {
        let threshold = (probability.clamp(0.0, 1.0) * u64::MAX as f64) as u64;
        Self {
            meta: NodeMeta {
                name: "coin_flip".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::bool("output")],
            },
            threshold,
        }
    }
}

impl GkNode for CoinFlip {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bool(inputs[0].as_u64() < self.threshold);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn const_f64() {
        let node = ConstF64::new(3.14);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 3.14);
    }

    #[test]
    fn const_bool() {
        let node = ConstBool::new(true);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert!(out[0].as_bool());
    }

    #[test]
    fn fixed_values_u64_cycles() {
        let node = FixedValuesU64::new(vec![10, 20, 30]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_u64(), 10);
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), 20);
        node.eval(&[Value::U64(2)], &mut out);
        assert_eq!(out[0].as_u64(), 30);
        node.eval(&[Value::U64(3)], &mut out);
        assert_eq!(out[0].as_u64(), 10); // wraps
    }

    #[test]
    fn fixed_values_u64_compiled() {
        let node = FixedValuesU64::new(vec![10, 20, 30]);
        let op = node.compiled_u64().expect("should compile");
        let mut out = [0u64];
        op(&[1], &mut out);
        assert_eq!(out[0], 20);
    }

    #[test]
    fn fixed_values_f64() {
        let node = FixedValuesF64::new(vec![1.1, 2.2, 3.3]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_f64(), 2.2);
    }

    #[test]
    fn fixed_values_str() {
        let node = FixedValuesStr::new(vec!["alpha".into(), "beta".into(), "gamma".into()]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(2)], &mut out);
        assert_eq!(out[0].as_str(), "gamma");
    }

    #[test]
    fn coin_flip_always_true() {
        let node = CoinFlip::new(1.0);
        let mut out = [Value::None];
        for i in 0..100 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(out[0].as_bool());
        }
    }

    #[test]
    fn coin_flip_always_false() {
        let node = CoinFlip::new(0.0);
        let mut out = [Value::None];
        for i in 0..100 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(!out[0].as_bool());
        }
    }

    #[test]
    fn coin_flip_roughly_half() {
        use xxhash_rust::xxh3::xxh3_64;
        let node = CoinFlip::new(0.5);
        let mut true_count = 0;
        let n = 10_000u64;
        let mut out = [Value::None];
        for i in 0..n {
            let hashed = xxh3_64(&i.to_le_bytes());
            node.eval(&[Value::U64(hashed)], &mut out);
            if out[0].as_bool() {
                true_count += 1;
            }
        }
        let ratio = true_count as f64 / n as f64;
        assert!(
            (ratio - 0.5).abs() < 0.05,
            "expected ~50%, got {ratio}"
        );
    }
}
