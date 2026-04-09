// Copyright 2024-2026 nosqlbench contributors
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

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
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

/// Random u64 in [min, max).
///
/// Signature: `() -> (u64)`
pub struct RandomRange {
    meta: NodeMeta,
    min: u64,
    range: u64,
}

impl RandomRange {
    pub fn new(min: u64, max: u64) -> Self {
        assert!(max > min);
        Self {
            meta: NodeMeta {
                name: "random_range".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            min,
            range: max - min,
        }
    }
}

impl GkNode for RandomRange {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.min + (next_u64() % self.range));
    }
}

/// Random f64 in [min, max).
///
/// Signature: `() -> (f64)`
pub struct RandomF64 {
    meta: NodeMeta,
    min: f64,
    range: f64,
}

impl RandomF64 {
    pub fn new(min: f64, max: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "random_f64".into(),
                outs: vec![Port::f64("output")],
                ins: Vec::new(),
            },
            min,
            range: max - min,
        }
    }
}

impl GkNode for RandomF64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(self.min + next_f64() * self.range);
    }
}

/// Random byte buffer of a fixed size.
///
/// Signature: `() -> (bytes)`
pub struct RandomBytes {
    meta: NodeMeta,
    size: usize,
}

impl RandomBytes {
    pub fn new(size: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "random_bytes".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: Vec::new(),
            },
            size,
        }
    }
}

impl GkNode for RandomBytes {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let mut buf = Vec::with_capacity(self.size);
        while buf.len() < self.size {
            let take = (self.size - buf.len()).min(8);
            buf.extend_from_slice(&next_u64().to_le_bytes()[..take]);
        }
        outputs[0] = Value::Bytes(buf);
    }
}

/// Random string from a character set.
///
/// Signature: `() -> (String)`
pub struct RandomString {
    meta: NodeMeta,
    chars: Vec<char>,
    length: usize,
}

impl RandomString {
    pub fn alphanumeric(length: usize) -> Self {
        Self::from_charset("A-Za-z0-9", length)
    }

    pub fn from_charset(spec: &str, length: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "random_string".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: Vec::new(),
            },
            chars: parse_charset(spec),
            length,
        }
    }
}

impl GkNode for RandomString {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let s: String = (0..self.length)
            .map(|_| self.chars[(next_u64() as usize) % self.chars.len()])
            .collect();
        outputs[0] = Value::Str(s);
    }
}

/// Random boolean with a given probability of true.
///
/// Signature: `() -> (bool)`
pub struct RandomBool {
    meta: NodeMeta,
    threshold: u64,
}

impl RandomBool {
    pub fn new(probability: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "random_bool".into(),
                outs: vec![Port::bool("output")],
                ins: Vec::new(),
            },
            threshold: (probability.clamp(0.0, 1.0) * u64::MAX as f64) as u64,
        }
    }
}

impl GkNode for RandomBool {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bool(next_u64() < self.threshold);
    }
}

// =================================================================
// Deterministic text extraction nodes (1→1, hash-based)
// =================================================================

/// Extract a substring from bundled lorem ipsum text using a hash-based
/// offset. Deterministic: same input → same extract.
///
/// Signature: `(input: u64) -> (String)`
///
/// This is the equivalent of nosqlbench's `HashedLoremExtractToString`.
pub struct HashedLoremExtract {
    meta: NodeMeta,
    min_len: usize,
    max_len: usize,
}

impl HashedLoremExtract {
    pub fn new(min_len: usize, max_len: usize) -> Self {
        assert!(max_len >= min_len);
        Self {
            meta: NodeMeta {
                name: "hashed_lorem_extract".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            min_len,
            max_len,
        }
    }
}

impl GkNode for HashedLoremExtract {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let h = inputs[0].as_u64();
        let len_range = self.max_len - self.min_len + 1;
        let extract_len = self.min_len + ((h as usize) % len_range);
        let max_offset = LOREM_IPSUM.len().saturating_sub(extract_len);
        let h2 = xxh3_64(&h.to_le_bytes());
        let offset = if max_offset > 0 { (h2 as usize) % (max_offset + 1) } else { 0 };
        let end = (offset + extract_len).min(LOREM_IPSUM.len());
        // Align to char boundaries
        let start = LOREM_IPSUM.floor_char_boundary(offset);
        let end = LOREM_IPSUM.ceil_char_boundary(end);
        outputs[0] = Value::Str(LOREM_IPSUM[start..end].to_string());
    }
}

/// Select a deterministic line from a bundled text file using hash.
///
/// Signature: `(input: u64) -> (String)`
///
/// Equivalent to nosqlbench's `HashedLineToString`. The text file is
/// pre-split into lines at init time.
pub struct HashedLineToString {
    meta: NodeMeta,
    lines: Vec<String>,
}

impl HashedLineToString {
    /// Create from a bundled text source.
    pub fn new(text: &str) -> Self {
        let lines: Vec<String> = text.lines()
            .map(|l| l.to_string())
            .filter(|l| !l.is_empty())
            .collect();
        assert!(!lines.is_empty(), "text source must have at least one line");
        Self {
            meta: NodeMeta {
                name: "hashed_line_to_string".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            lines,
        }
    }

    /// From bundled first names.
    pub fn names() -> Self { Self::new(NAMES) }
    /// From bundled last names.
    pub fn lastnames() -> Self { Self::new(LASTNAMES) }
    /// From bundled careers.
    pub fn careers() -> Self { Self::new(CAREERS) }
    /// From bundled company names.
    pub fn companies() -> Self { Self::new(COMPANIES) }
}

impl GkNode for HashedLineToString {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let h = inputs[0].as_u64();
        let idx = (h as usize) % self.lines.len();
        outputs[0] = Value::Str(self.lines[idx].clone());
    }
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
