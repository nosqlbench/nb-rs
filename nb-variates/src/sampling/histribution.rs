// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Histribution: inline discrete histogram distribution.
//!
//! Parse a frequency spec string into an alias table at init time.
//! The name is a portmanteau of "histogram" + "distribution."
//!
//! Two formats:
//! - Implicit labels: `"50 25 13 12"` → outcomes 0,1,2,3 with those weights
//! - Explicit labels: `"234:50 33:25 17:13 3:12"` → outcomes 234,33,17,3

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Value};
use crate::sampling::alias::AliasTableU64;

/// Parse a histribution spec and build an alias table.
///
/// Returns `(labels, table)` where labels[i] is the outcome for
/// alias table index i.
pub fn parse_histribution(spec: &str) -> (Vec<u64>, AliasTableU64) {
    let labeled = spec.contains(':');
    let mut labels = Vec::new();
    let mut weights = Vec::new();

    for (i, elem) in spec.split([' ', ',', ';']).enumerate() {
        let elem = elem.trim();
        if elem.is_empty() {
            continue;
        }
        if labeled {
            let parts: Vec<&str> = elem.splitn(2, ':').collect();
            assert_eq!(parts.len(), 2, "all elements must be labeled: {elem}");
            labels.push(parts[0].parse::<u64>().expect("invalid label"));
            weights.push(parts[1].parse::<f64>().expect("invalid weight"));
        } else {
            labels.push(i as u64);
            weights.push(elem.parse::<f64>().expect("invalid weight"));
        }
    }

    assert!(!weights.is_empty(), "histribution spec must not be empty");
    let table = AliasTableU64::from_weights(&weights);
    (labels, table)
}

/// GK node that samples from a histribution spec.
///
/// Signature: `(input: u64) -> (u64)`
///
/// The input should be hashed (uniform). The output is one of the
/// labeled outcomes, selected by weighted alias sampling.
pub struct Histribution {
    meta: NodeMeta,
    labels: Vec<u64>,
    table: AliasTableU64,
}

impl Histribution {
    /// Create from a frequency spec string.
    ///
    /// Implicit labels: `"50 25 13 12"` → outcomes 0,1,2,3
    /// Explicit labels: `"234:50 33:25 17:13 3:12"` → outcomes 234,33,17,3
    pub fn new(spec: &str) -> Self {
        let (labels, table) = parse_histribution(spec);
        Self {
            meta: NodeMeta {
                name: "histribution".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
            },
            labels,
            table,
        }
    }
}

impl GkNode for Histribution {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = self.table.sample(inputs[0].as_u64()) as usize;
        outputs[0] = Value::U64(self.labels[idx]);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let labels = self.labels.clone();
        let biases = self.table.biases().to_vec();
        let primaries = self.table.primaries().to_vec();
        let aliases = self.table.aliases().to_vec();
        let n = biases.len();

        Some(Box::new(move |inputs, outputs| {
            let input = inputs[0];
            let slot_idx = (input as usize) % n;
            let frac = (input >> 32) as f64 / u32::MAX as f64;
            let alias_idx = if frac < biases[slot_idx] {
                primaries[slot_idx]
            } else {
                aliases[slot_idx]
            };
            outputs[0] = labels[alias_idx as usize];
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn parse_implicit_labels() {
        let (labels, table) = parse_histribution("50 25 13 12");
        assert_eq!(labels, vec![0, 1, 2, 3]);
        assert_eq!(table.len(), 4);
    }

    #[test]
    fn parse_explicit_labels() {
        let (labels, table) = parse_histribution("234:50 33:25 17:13 3:12");
        assert_eq!(labels, vec![234, 33, 17, 3]);
        assert_eq!(table.len(), 4);
    }

    #[test]
    fn parse_comma_separated() {
        let (labels, _) = parse_histribution("10,20,30");
        assert_eq!(labels, vec![0, 1, 2]);
    }

    #[test]
    fn parse_semicolon_separated() {
        let (labels, _) = parse_histribution("10;20;30");
        assert_eq!(labels, vec![0, 1, 2]);
    }

    #[test]
    fn histribution_samples_valid_labels() {
        let node = Histribution::new("234:50 33:25 17:13 3:12");
        let valid = [234u64, 33, 17, 3];
        let mut out = [Value::None];
        for i in 0..1000u64 {
            let hashed = xxh3_64(&i.to_le_bytes());
            node.eval(&[Value::U64(hashed)], &mut out);
            assert!(valid.contains(&out[0].as_u64()),
                "unexpected outcome: {}", out[0].as_u64());
        }
    }

    #[test]
    fn histribution_weighted() {
        // Outcome 0 has weight 100, others have weight 1 each
        let node = Histribution::new("100 1 1");
        let mut counts = [0u64; 3];
        for i in 0..10_000u64 {
            let hashed = xxh3_64(&i.to_le_bytes());
            let mut out = [Value::None];
            node.eval(&[Value::U64(hashed)], &mut out);
            counts[out[0].as_u64() as usize] += 1;
        }
        let ratio = counts[0] as f64 / 10_000.0;
        assert!(ratio > 0.90, "outcome 0 should dominate, got {ratio}");
    }

    #[test]
    fn histribution_compiled() {
        let node = Histribution::new("234:50 33:25 17:13 3:12");
        let op = node.compiled_u64().expect("should compile");
        let valid = [234u64, 33, 17, 3];
        let mut out = [0u64];
        for i in 0..100u64 {
            let hashed = xxh3_64(&i.to_le_bytes());
            op(&[hashed], &mut out);
            assert!(valid.contains(&out[0]));
        }
    }

    #[test]
    fn histribution_deterministic() {
        let node = Histribution::new("50 25 13 12");
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        let hashed = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(hashed)], &mut out1);
        node.eval(&[Value::U64(hashed)], &mut out2);
        assert_eq!(out1[0].as_u64(), out2[0].as_u64());
    }
}
