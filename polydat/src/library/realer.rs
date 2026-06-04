// Copyright 2024-2026 Jonathan Shook
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
//!
//! SRD-80b Phase E: migrated to `#[polydat_node]`. The bundled
//! datasets are parsed once into process-global `OnceLock`s
//! (the samplers are stateless after construction and the data
//! is `include_str!`-baked, so there's nothing per-instance to
//! configure). Hand-written `impl PolydatNode for X` blocks and
//! the `signatures()` / `build_node` / `register_nodes!` trio
//! deleted; the macro emits the registry entries directly.
//!
//! Naming change: the previous `FirstNames` struct had two Rust
//! constructors `female()` / `male()` but the DSL registry only
//! exposed the female variant. After migration, `first_names`
//! (struct `FirstNames`) keeps the female-by-default behaviour
//! (no regression) and `first_names_male` (struct `FirstNamesMale`)
//! makes the male variant a first-class DSL node.

use crate::library::sampling::alias::AliasTableU64;
use std::sync::OnceLock;

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
            if let Ok(w) = parts[1].trim().parse::<f64>()
                && !name.is_empty() && w > 0.0 {
                    names.push(name);
                    weights.push(w);
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
pub struct WeightedNameSampler {
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
pub struct UniformNameSampler {
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
// Process-global sampler caches (`include_str!` data is static —
// the samplers are stateless after parse — so one global instance
// per dataset is the right cache granularity).
// =================================================================

fn female_first_names() -> &'static WeightedNameSampler {
    static CELL: OnceLock<WeightedNameSampler> = OnceLock::new();
    CELL.get_or_init(|| {
        let (names, weights) = parse_name_weight_csv(FEMALE_FIRSTNAMES_CSV);
        WeightedNameSampler::new(names, weights)
    })
}

fn male_first_names() -> &'static WeightedNameSampler {
    static CELL: OnceLock<WeightedNameSampler> = OnceLock::new();
    CELL.get_or_init(|| {
        let (names, weights) = parse_name_weight_csv(MALE_FIRSTNAMES_CSV);
        WeightedNameSampler::new(names, weights)
    })
}

fn state_codes_data() -> &'static UniformNameSampler {
    static CELL: OnceLock<UniformNameSampler> = OnceLock::new();
    CELL.get_or_init(|| UniformNameSampler::new(parse_single_column_csv(STATES_CSV)))
}

fn country_names_data() -> &'static UniformNameSampler {
    static CELL: OnceLock<UniformNameSampler> = OnceLock::new();
    CELL.get_or_init(|| {
        let pairs = parse_code_name_csv(COUNTRIES_CSV);
        UniformNameSampler::new(pairs.into_iter().map(|(_, name)| name).collect())
    })
}

fn country_codes_data() -> &'static UniformNameSampler {
    static CELL: OnceLock<UniformNameSampler> = OnceLock::new();
    CELL.get_or_init(|| {
        let pairs = parse_code_name_csv(COUNTRIES_CSV);
        UniformNameSampler::new(pairs.into_iter().map(|(code, _)| code).collect())
    })
}

fn nationalities_data() -> &'static UniformNameSampler {
    static CELL: OnceLock<UniformNameSampler> = OnceLock::new();
    CELL.get_or_init(|| UniformNameSampler::new(parse_single_column_csv(NATIONALITIES_CSV)))
}

fn last_names_data() -> &'static UniformNameSampler {
    static CELL: OnceLock<UniformNameSampler> = OnceLock::new();
    CELL.get_or_init(|| {
        UniformNameSampler::new(
            crate::library::random::LASTNAMES.lines()
                .filter(|l| !l.is_empty())
                .map(|l| l.to_string())
                .collect(),
        )
    })
}

// =================================================================
// Polydat Nodes
// =================================================================

/// `first_names(input) -> String` — Census female first name,
/// weighted by frequency.
#[crate::polydat_node(category = RealData)]
fn first_names(input: u64) -> String {
    female_first_names().sample(input).to_string()
}

/// `first_names_male(input) -> String` — Census male first name,
/// weighted by frequency. Companion to `first_names` (female).
#[crate::polydat_node(category = RealData)]
fn first_names_male(input: u64) -> String {
    male_first_names().sample(input).to_string()
}

/// `state_codes(input) -> String` — US state abbreviation
/// (uniform selection).
#[crate::polydat_node(category = RealData)]
fn state_codes(input: u64) -> String {
    state_codes_data().sample(input).to_string()
}

/// `country_names(input) -> String` — country name (uniform
/// selection over the full ISO list).
#[crate::polydat_node(category = RealData)]
fn country_names(input: u64) -> String {
    country_names_data().sample(input).to_string()
}

/// `country_codes(input) -> String` — country code (uniform
/// selection over the full ISO list).
#[crate::polydat_node(category = RealData)]
fn country_codes(input: u64) -> String {
    country_codes_data().sample(input).to_string()
}

/// `nationalities(input) -> String` — nationality name (uniform
/// selection).
#[crate::polydat_node(category = RealData)]
fn nationalities(input: u64) -> String {
    nationalities_data().sample(input).to_string()
}

/// `full_names(input) -> String` — combined first + last name.
///
/// Uses two hash-derived values from the input to independently
/// select a first name and last name. The first name's gender
/// is decided by bit 0 of the secondary hash.
#[crate::polydat_node(category = RealData)]
fn full_names(input: u64) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    let h2 = xxh3_64(&input.to_le_bytes());
    let h3 = xxh3_64(&h2.to_le_bytes());
    let first = if h2 & 1 == 0 {
        female_first_names().sample(h2)
    } else {
        male_first_names().sample(h2)
    };
    let last_name = last_names_data().sample(h3);
    format!("{first} {last_name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn first_names_female() {
        let node = FirstNames::new();
        let mut out = [Value::None];
        let h = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out);
        let name = out[0].as_str();
        assert!(!name.is_empty());
        assert!(name.chars().all(|c| c.is_alphabetic()));
    }

    #[test]
    fn first_names_male() {
        let node = FirstNamesMale::new();
        let mut out = [Value::None];
        let h = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out);
        assert!(!out[0].as_str().is_empty());
    }

    #[test]
    fn first_names_weighted() {
        // "Mary" is the most common female name — should appear often
        let node = FirstNames::new();
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
