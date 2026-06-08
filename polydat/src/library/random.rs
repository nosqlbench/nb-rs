// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Non-deterministic random data generators for prototyping and testing.
//!
//! These nodes use thread-local RNG and produce different outputs on
//! each call regardless of input coordinates. They are NOT reproducible
//! across runs. Use the deterministic hash-based nodes for production
//! workloads.
//!
//! All "random" nodes are 0→1 (no inputs) to make the non-deterministic
//! nature clear. The "hashed line/extract" nodes are 1→1 (deterministic,
//! coordinate-driven) and use the bundled text data files.

use std::cell::RefCell;

use xxhash_rust::xxh3::xxh3_64;

// =================================================================
// Bundled data files (included at compile time)
// =================================================================

/// ~93KB of Lorem Ipsum text from nosqlbench's data files.
pub static LOREM_IPSUM: &str = include_str!("../../data/lorem_ipsum_full.txt");
/// First names
pub static NAMES: &str = include_str!("../../data/names.txt");
/// Last names
pub static LASTNAMES: &str = include_str!("../../data/lastnames.txt");
/// Career titles
pub static CAREERS: &str = include_str!("../../data/careers.txt");
/// Company names
pub static COMPANIES: &str = include_str!("../../data/companies.txt");
/// Variable/metric words
pub static VARIABLE_WORDS: &str = include_str!("../../data/variable_words.txt");

// =================================================================
// Thread-local xorshift64 PRNG
// =================================================================

thread_local! {
    static RNG: RefCell<u64> = RefCell::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    );
}

fn next_u64() -> u64 {
    RNG.with(|r| {
        let mut s = *r.borrow();
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        *r.borrow_mut() = s;
        s
    })
}

fn next_f64() -> f64 {
    next_u64() as f64 / u64::MAX as f64
}

// =================================================================
// Non-deterministic random nodes (0→1)
// =================================================================

/// Random u64 in [min, max). SRD-80 PR B.13 migration —
/// inline-compute `range = max - min` per call (non-det node,
/// per-call subtraction is noise).
#[crate::polydat_node(
    category = Probability,
    purity = Nondeterministic("thread-local PRNG"),
)]
fn random_range(
    #[poly_default(0u64)] min: crate::derive_support::Const<u64>,
    #[poly_default(100u64)] max: crate::derive_support::Const<u64>,
) -> u64 {
    // Saturate the range to a non-zero value so a misconfigured
    // workload (min == max, or min > max) doesn't trap on the
    // modulus. `max.saturating_sub(min)` is 0 when min >= max.
    let range = max.saturating_sub(*min).max(1);
    *min + (next_u64() % range)
}

/// Random f64 in [min, max). SRD-80 PR B.13 migration.
#[crate::polydat_node(
    category = Probability,
    purity = Nondeterministic("thread-local PRNG"),
)]
fn random_f64(
    #[poly_default(0.0f64)] min: crate::derive_support::Const<f64>,
    #[poly_default(1.0f64)] max: crate::derive_support::Const<f64>,
) -> f64 {
    *min + next_f64() * (*max - *min)
}

/// Random byte buffer. SRD-80 PR B.13 migration.
#[crate::polydat_node(
    category = Probability,
    purity = Nondeterministic("thread-local PRNG"),
)]
fn random_bytes(
    #[poly_default(8u64)] size: crate::derive_support::Const<u64>,
) -> Vec<u8> {
    let sz = *size as usize;
    let mut buf = Vec::with_capacity(sz);
    while buf.len() < sz {
        let take = (sz - buf.len()).min(8);
        buf.extend_from_slice(&next_u64().to_le_bytes()[..take]);
    }
    buf
}

/// Random string from a character set. Charset parsed each
/// call — for hot-path use, prefer the deterministic
/// `combinations` node which precompiles the charset.
/// SRD-80 PR B.13 migration.
#[crate::polydat_node(
    category = Probability,
    purity = Nondeterministic("thread-local PRNG"),
)]
fn random_string(
    #[poly_default("A-Za-z0-9")] charset: crate::derive_support::Const<&str>,
    #[poly_default(8u64)] length: crate::derive_support::Const<u64>,
) -> String {
    let chars = parse_charset(&charset);
    if chars.is_empty() {
        return String::new();
    }
    (0..*length)
        .map(|_| chars[(next_u64() as usize) % chars.len()])
        .collect()
}

/// Random boolean with probability of true. SRD-80 PR B.13.
#[crate::polydat_node(
    category = Probability,
    purity = Nondeterministic("thread-local PRNG"),
)]
fn random_bool(
    #[poly_default(0.5f64)] probability: crate::derive_support::Const<f64>,
) -> bool {
    let threshold = (probability.clamp(0.0, 1.0) * u64::MAX as f64) as u64;
    next_u64() < threshold
}

impl RandomString {
    pub fn alphanumeric(length: u64) -> Self {
        Self::new("A-Za-z0-9".to_string(), length)
    }
}

// =================================================================
// Deterministic text extraction nodes (1→1, hash-based)
// =================================================================

/// Extract a substring from bundled lorem ipsum text using a hash-based
/// offset. Deterministic: same input → same extract.
///
/// Signature: `hashed_lorem_extract(input: u64, min_len: u64, max_len: u64) -> String`
///
/// SRD-80b Phase E migration — equivalent to nosqlbench's
/// `HashedLoremExtractToString`.
#[crate::polydat_node(category = String)]
fn hashed_lorem_extract(
    input: u64,
    min_len: crate::derive_support::Const<u64>,
    max_len: crate::derive_support::Const<u64>,
) -> String {
    let min_len = *min_len as usize;
    let max_len = *max_len as usize;
    let len_range = max_len.saturating_sub(min_len) + 1;
    let extract_len = min_len + ((input as usize) % len_range);
    let max_offset = LOREM_IPSUM.len().saturating_sub(extract_len);
    let h2 = xxh3_64(&input.to_le_bytes());
    let offset = if max_offset > 0 { (h2 as usize) % (max_offset + 1) } else { 0 };
    let end = (offset + extract_len).min(LOREM_IPSUM.len());
    // Align to char boundaries
    let start = LOREM_IPSUM.floor_char_boundary(offset);
    let end = LOREM_IPSUM.ceil_char_boundary(end);
    LOREM_IPSUM[start..end].to_string()
}

/// Pre-split list of non-empty lines from a bundled text source.
/// SRD-80b Phase E — derived state for `hashed_line_to_string`,
/// computed once per node instance via `split_lines`.
pub struct HashedLines(pub Vec<String>);

impl crate::derive_support::PolydatSetup for HashedLines {}

impl HashedLines {
    /// Single-call setup. The `#[polydat_node]` macro invokes
    /// this exactly once in the generated `HashedLineToString::new()`.
    pub fn split_lines(text: &str) -> Self {
        let lines: Vec<String> = text
            .lines()
            .map(|l| l.to_string())
            .filter(|l| !l.is_empty())
            .collect();
        assert!(!lines.is_empty(), "text source must have at least one line");
        Self(lines)
    }
}

/// Select a deterministic line from a bundled text source using
/// the input hash as an index. Deterministic: same input → same line.
///
/// Signature: `hashed_line_to_string(input: u64, source: &str) -> String`
///
/// SRD-80b Phase E migration — equivalent to nosqlbench's
/// `HashedLineToString`. The text source is split into lines at
/// node-construction time (setup-derived state).
#[crate::polydat_node(category = String)]
fn hashed_line_to_string(
    input: u64,
    source: crate::derive_support::Const<&str>,
    #[poly_const(HashedLines::split_lines, from = source)]
    lines: &HashedLines,
) -> String {
    let _ = source;
    let idx = (input as usize) % lines.0.len();
    lines.0[idx].clone()
}

impl HashedLineToString {
    /// From bundled first names.
    pub fn names() -> Self { Self::new(NAMES.to_string()) }
    /// From bundled last names.
    pub fn lastnames() -> Self { Self::new(LASTNAMES.to_string()) }
    /// From bundled careers.
    pub fn careers() -> Self { Self::new(CAREERS.to_string()) }
    /// From bundled company names.
    pub fn companies() -> Self { Self::new(COMPANIES.to_string()) }
}

fn parse_charset(spec: &str) -> Vec<char> {
    let mut chars = Vec::new();
    let spec_chars: Vec<char> = spec.chars().collect();
    let mut i = 0;
    while i < spec_chars.len() {
        if i + 2 < spec_chars.len() && spec_chars[i + 1] == '-' {
            for c in spec_chars[i]..=spec_chars[i + 2] { chars.push(c); }
            i += 3;
        } else {
            chars.push(spec_chars[i]);
            i += 1;
        }
    }
    chars
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn lorem_ipsum_bundled() {
        assert!(LOREM_IPSUM.len() > 90_000, "lorem ipsum should be ~93KB");
        assert!(LOREM_IPSUM.starts_with("Lorem ipsum"));
    }

    #[test]
    fn names_bundled() {
        assert!(!NAMES.is_empty());
        assert!(NAMES.lines().count() > 10);
    }

    #[test]
    fn random_range_bounded() {
        let node = RandomRange::new(10, 20);
        let mut out = [Value::None];
        for _ in 0..1000 {
            node.eval(&[], &mut out);
            assert!((10..20).contains(&out[0].as_u64()));
        }
    }

    #[test]
    fn random_f64_bounded() {
        let node = RandomF64::new(1.0, 5.0);
        let mut out = [Value::None];
        for _ in 0..1000 {
            node.eval(&[], &mut out);
            let v = out[0].as_f64();
            assert!(v >= 1.0 && v < 5.0, "out of range: {v}");
        }
    }

    #[test]
    fn random_string_charset() {
        let node = RandomString::alphanumeric(20);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str().len(), 20);
        assert!(out[0].as_str().chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn hashed_lorem_extract_deterministic() {
        let node = HashedLoremExtract::new(50, 100);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(42)], &mut out1);
        node.eval(&[Value::U64(42)], &mut out2);
        assert_eq!(out1[0].as_str(), out2[0].as_str());
    }

    #[test]
    fn hashed_lorem_extract_size_range() {
        let node = HashedLoremExtract::new(20, 50);
        let mut out = [Value::None];
        for i in 0..100u64 {
            let h = xxh3_64(&i.to_le_bytes());
            node.eval(&[Value::U64(h)], &mut out);
            let len = out[0].as_str().len();
            assert!(len >= 19 && len <= 55, "len={len}"); // char boundary wiggle
        }
    }

    #[test]
    fn hashed_lorem_extract_varies() {
        let node = HashedLoremExtract::new(10, 20);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        let h1 = xxh3_64(&0u64.to_le_bytes());
        let h2 = xxh3_64(&1u64.to_le_bytes());
        node.eval(&[Value::U64(h1)], &mut out1);
        node.eval(&[Value::U64(h2)], &mut out2);
        assert_ne!(out1[0].as_str(), out2[0].as_str());
    }

    #[test]
    fn hashed_line_names() {
        let node = HashedLineToString::names();
        let mut out = [Value::None];
        let h = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out);
        assert!(!out[0].as_str().is_empty());
    }

    #[test]
    fn hashed_line_careers() {
        let node = HashedLineToString::careers();
        let mut out = [Value::None];
        let h = xxh3_64(&42u64.to_le_bytes());
        node.eval(&[Value::U64(h)], &mut out);
        assert!(!out[0].as_str().is_empty());
    }

    #[test]
    fn hashed_line_deterministic() {
        let node = HashedLineToString::names();
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(12345)], &mut out1);
        node.eval(&[Value::U64(12345)], &mut out2);
        assert_eq!(out1[0].as_str(), out2[0].as_str());
    }
}
