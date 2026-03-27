// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Bundled real-world data for realistic data generation.
//!
//! Provides grab-and-go nodes for generating person names, country
//! names, US state codes, and nationalities from embedded Census and
//! geographic datasets. All data is compiled into the binary via
//! `include_str!` — no runtime file I/O.
//!
//! Each node takes a u64 input (should be hashed for uniform
//! distribution) and returns a String. Weighted variants select
//! proportionally to Census frequency data.

use crate::node::{GkNode, NodeMeta, Port, PortType, Value};
use crate::sampling::alias::AliasTableU64;

// =================================================================
// Bundled CSV data
// =================================================================

static FEMALE_FIRSTNAMES_CSV: &str = include_str!("../../data/census/female_firstnames.csv");
static MALE_FIRSTNAMES_CSV: &str = include_str!("../../data/census/male_firstnames.csv");
static STATES_CSV: &str = include_str!("../../data/census/census_state_abbrev.csv");
static COUNTRIES_CSV: &str = include_str!("../../data/census/countries.csv");
static NATIONALITIES_CSV: &str = include_str!("../../data/census/nationalities.csv");

// =================================================================
// CSV parsing helpers
// =================================================================

/// Parse a name+weight CSV (skipping header). Returns (names, weights).
fn parse_name_weight_csv(csv: &str) -> (Vec<String>, Vec<f64>) {
    let mut names = Vec::new();
    let mut weights = Vec::new();
    for line in csv.lines().skip(1) {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 2 {
            let name = parts[0].trim().to_string();
            if let Ok(w) = parts[1].trim().parse::<f64>() {
                if !name.is_empty() && w > 0.0 {
                    names.push(name);
                    weights.push(w);
                }
            }
        }
    }
    (names, weights)
}

/// Parse a single-column CSV (skipping header). Returns list of values.
fn parse_single_column_csv(csv: &str) -> Vec<String> {
    csv.lines()
        .skip(1)
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Parse a two-column CSV with code,name (skipping header).
fn parse_code_name_csv(csv: &str) -> Vec<(String, String)> {
    csv.lines()
        .skip(1)
        .filter_map(|l| {
            let parts: Vec<&str> = l.split(',').collect();
            if parts.len() >= 2 {
                Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
            } else {
                None
            }
        })
        .collect()
}

// =================================================================
// Generic weighted name sampler
// =================================================================

/// A weighted name sampler backed by an alias table.
struct WeightedNameSampler {
    names: Vec<String>,
    table: AliasTableU64,
}

impl WeightedNameSampler {
    fn new(names: Vec<String>, weights: Vec<f64>) -> Self {
        let table = AliasTableU64::from_weights(&weights);
        Self { names, table }
    }

    fn sample(&self, input: u64) -> &str {
        let idx = self.table.sample(input) as usize;
        &self.names[idx]
    }
}

/// A uniform name sampler (no weights, just mod index).
struct UniformNameSampler {
    names: Vec<String>,
}

impl UniformNameSampler {
    fn new(names: Vec<String>) -> Self {
        Self { names }
    }

    fn sample(&self, input: u64) -> &str {
        let idx = (input as usize) % self.names.len();
        &self.names[idx]
    }
}

// =================================================================
// GK Nodes
// =================================================================

/// Female first names weighted by Census frequency.
///
/// Signature: `(input: u64) -> (String)`
pub struct FirstNames {
    meta: NodeMeta,
    sampler: WeightedNameSampler,
}

impl FirstNames {
    pub fn female() -> Self {
        let (names, weights) = parse_name_weight_csv(FEMALE_FIRSTNAMES_CSV);
        Self {
            meta: NodeMeta {
                name: "first_names".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            sampler: WeightedNameSampler::new(names, weights),
        }
    }

    pub fn male() -> Self {
        let (names, weights) = parse_name_weight_csv(MALE_FIRSTNAMES_CSV);
        Self {
            meta: NodeMeta {
                name: "first_names".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            sampler: WeightedNameSampler::new(names, weights),
        }
    }
}

impl GkNode for FirstNames {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.sampler.sample(inputs[0].as_u64()).to_string());
    }
}

/// US state abbreviations (uniform selection).
///
/// Signature: `(input: u64) -> (String)`
pub struct StateCodes {
    meta: NodeMeta,
    sampler: UniformNameSampler,
}

impl StateCodes {
    pub fn new() -> Self {
        let names = parse_single_column_csv(STATES_CSV);
        Self {
            meta: NodeMeta {
                name: "state_codes".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            sampler: UniformNameSampler::new(names),
        }
    }
}

impl GkNode for StateCodes {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.sampler.sample(inputs[0].as_u64()).to_string());
    }
}

/// Country names (uniform selection).
///
/// Signature: `(input: u64) -> (String)`
pub struct CountryNames {
    meta: NodeMeta,
    sampler: UniformNameSampler,
}

impl CountryNames {
    pub fn new() -> Self {
        let pairs = parse_code_name_csv(COUNTRIES_CSV);
        let names: Vec<String> = pairs.into_iter().map(|(_, name)| name).collect();
        Self {
            meta: NodeMeta {
                name: "country_names".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            sampler: UniformNameSampler::new(names),
        }
    }
}

impl GkNode for CountryNames {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.sampler.sample(inputs[0].as_u64()).to_string());
    }
}

/// Country codes (uniform selection).
///
/// Signature: `(input: u64) -> (String)`
pub struct CountryCodes {
    meta: NodeMeta,
    sampler: UniformNameSampler,
}

impl CountryCodes {
    pub fn new() -> Self {
        let pairs = parse_code_name_csv(COUNTRIES_CSV);
        let codes: Vec<String> = pairs.into_iter().map(|(code, _)| code).collect();
        Self {
            meta: NodeMeta {
                name: "country_codes".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            sampler: UniformNameSampler::new(codes),
        }
    }
}

impl GkNode for CountryCodes {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.sampler.sample(inputs[0].as_u64()).to_string());
    }
}

/// Nationality names (uniform selection).
///
/// Signature: `(input: u64) -> (String)`
pub struct Nationalities {
    meta: NodeMeta,
    sampler: UniformNameSampler,
}

impl Nationalities {
    pub fn new() -> Self {
        let names = parse_single_column_csv(NATIONALITIES_CSV);
        Self {
            meta: NodeMeta {
                name: "nationalities".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            sampler: UniformNameSampler::new(names),
        }
    }
}

impl GkNode for Nationalities {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.sampler.sample(inputs[0].as_u64()).to_string());
    }
}

/// Full names: combines a first name and last name.
///
/// Signature: `(input: u64) -> (String)`
///
/// Uses two hash-derived values from the input to independently
/// select a first name and last name.
pub struct FullNames {
    meta: NodeMeta,
    first_female: WeightedNameSampler,
    first_male: WeightedNameSampler,
    last: UniformNameSampler,
}

impl FullNames {
    pub fn new() -> Self {
        let (f_names, f_weights) = parse_name_weight_csv(FEMALE_FIRSTNAMES_CSV);
        let (m_names, m_weights) = parse_name_weight_csv(MALE_FIRSTNAMES_CSV);
        Self {
            meta: NodeMeta {
                name: "full_names".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::new("output", PortType::Str)],
            },
            first_female: WeightedNameSampler::new(f_names, f_weights),
            first_male: WeightedNameSampler::new(m_names, m_weights),
            last: UniformNameSampler::new(
                crate::nodes::random::LASTNAMES.lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| l.to_string())
                    .collect()
            ),
        }
    }
}

impl GkNode for FullNames {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        use xxhash_rust::xxh3::xxh3_64;
        let h = inputs[0].as_u64();
        let h2 = xxh3_64(&h.to_le_bytes());
        let h3 = xxh3_64(&h2.to_le_bytes());
        // Use h2 bit 0 to select male/female
        let first = if h2 & 1 == 0 {
            self.first_female.sample(h2)
        } else {
            self.first_male.sample(h2)
        };
        let last = self.last.sample(h3);
        outputs[0] = Value::Str(format!("{first} {last}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn first_names_female() {
        let node = FirstNames::female();
        let mut out = [Value::None];
        let h = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out);
        let name = out[0].as_str();
        assert!(!name.is_empty());
        assert!(name.chars().all(|c| c.is_alphabetic()));
    }

    #[test]
    fn first_names_male() {
        let node = FirstNames::male();
        let mut out = [Value::None];
        let h = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out);
        assert!(!out[0].as_str().is_empty());
    }

    #[test]
    fn first_names_weighted() {
        // "Mary" is the most common female name — should appear often
        let node = FirstNames::female();
        let mut mary_count = 0;
        let mut out = [Value::None];
        for i in 0..10_000u64 {
            let h = xxh3_64(&i.to_le_bytes());
            node.eval(&[Value::U64(h)], &mut out);
            if out[0].as_str() == "Mary" { mary_count += 1; }
        }
        assert!(mary_count > 50, "Mary should appear frequently, got {mary_count}");
    }

    #[test]
    fn state_codes_valid() {
        let node = StateCodes::new();
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let code = out[0].as_str();
            assert_eq!(code.len(), 2, "state code should be 2 chars: {code}");
            assert!(code.chars().all(|c| c.is_ascii_uppercase()));
        }
    }

    #[test]
    fn country_names_nonempty() {
        let node = CountryNames::new();
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(!out[0].as_str().is_empty());
        }
    }

    #[test]
    fn country_codes_two_char() {
        let node = CountryCodes::new();
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_str().len(), 2);
        }
    }

    #[test]
    fn nationalities_nonempty() {
        let node = Nationalities::new();
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(!out[0].as_str().is_empty());
        }
    }

    #[test]
    fn full_names_format() {
        let node = FullNames::new();
        let mut out = [Value::None];
        let h = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out);
        let name = out[0].as_str();
        assert!(name.contains(' '), "full name should have a space: {name}");
        assert!(name.len() > 3, "full name too short: {name}");
    }

    #[test]
    fn full_names_deterministic() {
        let node = FullNames::new();
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        let h = xxh3_64(&99u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out1);
        node.eval(&[Value::U64(h)], &mut out2);
        assert_eq!(out1[0].as_str(), out2[0].as_str());
    }
}
