// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Convenience weighted output selection nodes.
//!
//! These are "fat" convenience nodes that combine alias sampling with
//! value lookup in one step. They parse an inline spec string at init
//! time and perform weighted selection at cycle time.

use crate::node::{GkNode, NodeMeta, Port, PortType, Value};
use crate::sampling::alias::AliasTableU64;

/// Parse a weighted spec like "alpha:0.3;beta:0.5;gamma:0.2"
/// into parallel vectors of values and weights.
fn parse_weighted_str_spec(spec: &str) -> (Vec<String>, Vec<f64>) {
    let mut values = Vec::new();
    let mut weights = Vec::new();
    for elem in spec.split([';', ',']) {
        let elem = elem.trim();
        if elem.is_empty() { continue; }
        let parts: Vec<&str> = elem.splitn(2, ':').collect();
        assert_eq!(parts.len(), 2, "expected 'value:weight', got '{elem}'");
        values.push(parts[0].to_string());
        weights.push(parts[1].parse::<f64>().expect("invalid weight"));
    }
    (values, weights)
}

fn parse_weighted_u64_spec(spec: &str) -> (Vec<u64>, Vec<f64>) {
    let mut values = Vec::new();
    let mut weights = Vec::new();
    for elem in spec.split([';', ',']) {
        let elem = elem.trim();
        if elem.is_empty() { continue; }
        let parts: Vec<&str> = elem.splitn(2, ':').collect();
        assert_eq!(parts.len(), 2, "expected 'value:weight', got '{elem}'");
        values.push(parts[0].parse::<u64>().expect("invalid value"));
        weights.push(parts[1].parse::<f64>().expect("invalid weight"));
    }
    (values, weights)
}

/// Weighted string selection from an inline spec.
///
/// Signature: `(input: u64) -> (String)`
///
/// Spec format: `"alpha:0.3;beta:0.5;gamma:0.2"`
/// Input should be hashed for uniform distribution.
pub struct WeightedStrings {
    meta: NodeMeta,
    values: Vec<String>,
    table: AliasTableU64,
}

impl WeightedStrings {
    pub fn new(spec: &str) -> Self {
        let (values, weights) = parse_weighted_str_spec(spec);
        let table = AliasTableU64::from_weights(&weights);
        Self {
            meta: NodeMeta {
                name: "weighted_strings".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            values,
            table,
        }
    }
}

impl GkNode for WeightedStrings {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = self.table.sample(inputs[0].as_u64()) as usize;
        outputs[0] = Value::Str(self.values[idx].clone());
    }
}

/// Weighted u64 selection from an inline spec.
///
/// Signature: `(input: u64) -> (u64)`
///
/// Spec format: `"10:0.5;20:0.3;30:0.2"`
pub struct WeightedU64 {
    meta: NodeMeta,
    values: Vec<u64>,
    table: AliasTableU64,
}

impl WeightedU64 {
    pub fn new(spec: &str) -> Self {
        let (values, weights) = parse_weighted_u64_spec(spec);
        let table = AliasTableU64::from_weights(&weights);
        Self {
            meta: NodeMeta {
                name: "weighted_u64".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
            },
            values,
            table,
        }
    }
}

impl GkNode for WeightedU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = self.table.sample(inputs[0].as_u64()) as usize;
        outputs[0] = Value::U64(self.values[idx]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn weighted_strings_valid_outputs() {
        let node = WeightedStrings::new("alpha:0.3;beta:0.5;gamma:0.2");
        let valid = ["alpha", "beta", "gamma"];
        let mut out = [Value::None];
        for i in 0..1000u64 {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            assert!(valid.contains(&out[0].as_str()));
        }
    }

    #[test]
    fn weighted_strings_respects_weights() {
        let node = WeightedStrings::new("rare:0.01;common:0.99");
        let mut common_count = 0u64;
        let mut out = [Value::None];
        let n = 10_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            if out[0].as_str() == "common" {
                common_count += 1;
            }
        }
        let ratio = common_count as f64 / n as f64;
        assert!(ratio > 0.90, "common should dominate, got {ratio}");
    }

    #[test]
    fn weighted_u64_valid_outputs() {
        let node = WeightedU64::new("10:0.5;20:0.3;30:0.2");
        let valid = [10u64, 20, 30];
        let mut out = [Value::None];
        for i in 0..1000u64 {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            assert!(valid.contains(&out[0].as_u64()));
        }
    }
}
