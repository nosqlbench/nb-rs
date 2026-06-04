// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Histribution: inline discrete histogram distribution.
//!
//! Parse a frequency spec string into an alias table at init time.
//! The name is a portmanteau of "histogram" + "distribution."
//!
//! Two formats:
//! - Implicit labels: `"50 25 13 12"` → outcomes 0,1,2,3 with those weights
//! - Explicit labels: `"234:50 33:25 17:13 3:12"` → outcomes 234,33,17,3

use crate::derive_support::PolydatSetup;
use crate::library::sampling::alias::AliasTableU64;

/// Parsed histribution: labels + weighted alias table. The
/// `#[poly_const]` setup returns this single struct so the macro
/// can hand the body a borrow of one cached field rather than a
/// tuple. Labeled by index `i` → outcome `labels[i]`; sampling
/// goes through `table`.
pub struct ParsedHistribution {
    pub labels: Vec<u64>,
    pub table: AliasTableU64,
}

impl PolydatSetup for ParsedHistribution {}

/// Parse a histribution spec and build an alias table.
///
/// Returns `(labels, table)` where `labels[i]` is the outcome for
/// alias table index `i`. Kept as a free function for tests that
/// want the parsed form without constructing the node.
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

/// `#[poly_const]` setup: parse the spec into a `ParsedHistribution`
/// struct once at construction time.
fn parse_histribution_setup(spec: &str) -> ParsedHistribution {
    let (labels, table) = parse_histribution(spec);
    ParsedHistribution { labels, table }
}

/// Sample from a histogram-spec distribution. The input should
/// be hashed (uniform); the output is one of the labeled
/// outcomes, selected by weighted alias sampling.
#[crate::polydat_node(category = Probability)]
fn histribution(
    input: u64,
    spec: crate::derive_support::Const<&str>,
    #[poly_const(parse_histribution_setup, from = spec)]
    parsed: &ParsedHistribution,
) -> u64 {
    let idx = parsed.table.sample(input) as usize;
    parsed.labels[idx]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};
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
        let node = Histribution::new("234:50 33:25 17:13 3:12".to_string());
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
        let node = Histribution::new("100 1 1".to_string());
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

    // SRD-80b Phase C — `histribution` is JIT-ineligible by the
    // `#[poly_const]` cached-state design; the typed-eval path
    // above covers correctness.

    #[test]
    fn histribution_deterministic() {
        let node = Histribution::new("50 25 13 12".to_string());
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        let hashed = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(hashed)], &mut out1);
        node.eval(&[Value::U64(hashed)], &mut out2);
        assert_eq!(out1[0].as_u64(), out2[0].as_u64());
    }
}
