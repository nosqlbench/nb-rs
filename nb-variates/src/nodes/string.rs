// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! String generation and transformation nodes.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

// =================================================================
// Combinations: mixed-radix character set mapping
// =================================================================

/// Map a u64 to a formatted string via mixed-radix indexing into
/// character sets.
///
/// Signature: `combinations(input: u64, pattern: &str) -> (String)`
///
/// The pattern is a semicolon-delimited list of character set specs.
/// Each spec is a character range (`A-Z`), literal characters, or
/// both. A single literal character (like `-`) is emitted as-is
/// without consuming a radix digit.
///
/// Use for generating structured identifiers with fixed character
/// classes per position. Examples: phone numbers
/// (`"0-9;0-9;0-9;-;0-9;0-9;0-9;-;0-9;0-9;0-9;0-9"` yields
/// `"372-841-9205"`), license plates (`"A-Z;A-Z;A-Z;-;0-9;0-9;0-9"`),
/// or hex tokens (`"0-9a-f;0-9a-f;0-9a-f;0-9a-f"`). Input wraps at
/// `cardinality()`, so every value in the cycle space maps to a valid
/// string.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct Combinations {
    meta: NodeMeta,
    segments: Vec<Segment>,
    modulus: u64,
}

enum Segment {
    /// Variable: select one char from the charset based on a radix digit.
    Charset(Vec<char>),
    /// Fixed: always emit this string (e.g., a literal separator).
    Literal(String),
}

impl Combinations {
    pub fn new(pattern: &str) -> Self {
        let mut segments = Vec::new();
        let mut modulus: u64 = 1;

        for spec in pattern.split(';') {
            let chars = parse_charset(spec);
            if chars.len() == 1 && !spec.contains('-') {
                // Single literal character (no range), emit as-is
                segments.push(Segment::Literal(chars[0].to_string()));
            } else if chars.is_empty() {
                segments.push(Segment::Literal(spec.to_string()));
            } else {
                modulus = modulus.saturating_mul(chars.len() as u64);
                segments.push(Segment::Charset(chars));
            }
        }

        Self {
            meta: NodeMeta {
                name: "combinations".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            segments,
            modulus,
        }
    }

    /// The total number of unique combinations before wrapping.
    pub fn cardinality(&self) -> u64 {
        self.modulus
    }
}

impl GkNode for Combinations {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut remainder = inputs[0].as_u64() % self.modulus;
        let mut result = String::with_capacity(self.segments.len());

        for seg in &self.segments {
            match seg {
                Segment::Literal(s) => result.push_str(s),
                Segment::Charset(chars) => {
                    let radix = chars.len() as u64;
                    let idx = (remainder % radix) as usize;
                    result.push(chars[idx]);
                    remainder /= radix;
                }
            }
        }

        outputs[0] = Value::Str(result);
    }
}

/// Parse a charset spec like "A-Z", "0-9", "a-z0-9", "A-Za-z0-9 _|/"
fn parse_charset(spec: &str) -> Vec<char> {
    let mut chars = Vec::new();
    let spec_chars: Vec<char> = spec.chars().collect();
    let mut i = 0;
    while i < spec_chars.len() {
        if i + 2 < spec_chars.len() && spec_chars[i + 1] == '-' {
            // Range: A-Z, 0-9, etc.
            let start = spec_chars[i];
            let end = spec_chars[i + 2];
            for c in start..=end {
                chars.push(c);
            }
            i += 3;
        } else {
            chars.push(spec_chars[i]);
            i += 1;
        }
    }
    chars
}

// =================================================================
// NumberToWords: spell out numbers in English
// =================================================================

/// Convert a u64 to its English word representation.
///
/// Signature: `number_to_words(input: u64) -> (String)`
///
/// Examples: 0 produces "zero", 42 produces "forty-two", 1000
/// produces "one thousand". Supports the full u64 range up through
/// quintillions.
///
/// Use for generating human-readable text fields from numeric keys,
/// creating natural-language test data, or populating string columns
/// with deterministic variable-length content. Commonly chained after
/// `hash_range` to produce bounded vocabulary:
/// `number_to_words(hash_range(h, 1000))`.
///
/// JIT level: P1 (String output; no compiled_u64 path).
pub struct NumberToWords {
    meta: NodeMeta,
}

impl NumberToWords {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "number_to_words".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for NumberToWords {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(u64_to_words(inputs[0].as_u64()));
    }
}

const ONES: [&str; 20] = [
    "zero", "one", "two", "three", "four", "five", "six", "seven",
    "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen",
    "fifteen", "sixteen", "seventeen", "eighteen", "nineteen",
];

const TENS: [&str; 10] = [
    "", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy",
    "eighty", "ninety",
];

const SCALES: [&str; 7] = [
    "", "thousand", "million", "billion", "trillion", "quadrillion",
    "quintillion",
];

fn u64_to_words(n: u64) -> String {
    if n < 20 {
        return ONES[n as usize].to_string();
    }

    let mut parts: Vec<String> = Vec::new();
    let mut remaining = n;
    let mut scale_idx = 0;

    while remaining > 0 {
        let chunk = (remaining % 1000) as u32;
        if chunk > 0 {
            let chunk_words = chunk_to_words(chunk);
            if scale_idx > 0 && scale_idx < SCALES.len() {
                parts.push(format!("{} {}", chunk_words, SCALES[scale_idx]));
            } else {
                parts.push(chunk_words);
            }
        }
        remaining /= 1000;
        scale_idx += 1;
    }

    parts.reverse();
    parts.join(" ")
}

fn chunk_to_words(n: u32) -> String {
    let mut parts = Vec::new();

    let hundreds = n / 100;
    let remainder = n % 100;

    if hundreds > 0 {
        parts.push(format!("{} hundred", ONES[hundreds as usize]));
    }

    if remainder >= 20 {
        let tens = remainder / 10;
        let ones = remainder % 10;
        if ones > 0 {
            parts.push(format!("{}-{}", TENS[tens as usize], ONES[ones as usize]));
        } else {
            parts.push(TENS[tens as usize].to_string());
        }
    } else if remainder > 0 {
        parts.push(ONES[remainder as usize].to_string());
    }

    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Combinations tests ---

    #[test]
    fn combinations_digits() {
        let node = Combinations::new("0-9;0-9;0-9");
        let mut out = [Value::None];
        node.eval(&[Value::U64(123)], &mut out);
        let s = out[0].as_str();
        assert_eq!(s.len(), 3);
        assert!(s.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn combinations_with_separator() {
        let node = Combinations::new("0-9;0-9;0-9;-;0-9;0-9;0-9");
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        let s = out[0].as_str();
        assert_eq!(s.len(), 7); // 3 digits + dash + 3 digits
        assert_eq!(&s[3..4], "-");
    }

    #[test]
    fn combinations_alpha() {
        let node = Combinations::new("A-Z;A-Z;A-Z");
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_str(), "AAA");
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_str(), "BAA");
    }

    #[test]
    fn combinations_cardinality() {
        let node = Combinations::new("0-9;0-9;-;A-Z");
        // 10 * 10 * 26 = 2600 (separator doesn't count)
        assert_eq!(node.cardinality(), 2600);
    }

    #[test]
    fn combinations_deterministic() {
        let node = Combinations::new("A-Z;0-9");
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(42)], &mut out1);
        node.eval(&[Value::U64(42)], &mut out2);
        assert_eq!(out1[0].as_str(), out2[0].as_str());
    }

    #[test]
    fn combinations_wraps() {
        let node = Combinations::new("0-9");
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        let a = out[0].as_str().to_string();
        node.eval(&[Value::U64(10)], &mut out);
        assert_eq!(out[0].as_str(), &a, "should wrap at cardinality");
    }

    // --- NumberToWords tests ---

    #[test]
    fn number_to_words_zero() {
        assert_eq!(u64_to_words(0), "zero");
    }

    #[test]
    fn number_to_words_teens() {
        assert_eq!(u64_to_words(1), "one");
        assert_eq!(u64_to_words(11), "eleven");
        assert_eq!(u64_to_words(19), "nineteen");
    }

    #[test]
    fn number_to_words_tens() {
        assert_eq!(u64_to_words(20), "twenty");
        assert_eq!(u64_to_words(42), "forty-two");
        assert_eq!(u64_to_words(99), "ninety-nine");
    }

    #[test]
    fn number_to_words_hundreds() {
        assert_eq!(u64_to_words(100), "one hundred");
        assert_eq!(u64_to_words(123), "one hundred twenty-three");
        assert_eq!(u64_to_words(500), "five hundred");
    }

    #[test]
    fn number_to_words_thousands() {
        assert_eq!(u64_to_words(1000), "one thousand");
        assert_eq!(u64_to_words(1001), "one thousand one");
        assert_eq!(u64_to_words(12345), "twelve thousand three hundred forty-five");
    }

    #[test]
    fn number_to_words_millions() {
        assert_eq!(u64_to_words(1_000_000), "one million");
        assert_eq!(
            u64_to_words(1_234_567),
            "one million two hundred thirty-four thousand five hundred sixty-seven"
        );
    }

    #[test]
    fn number_to_words_large() {
        let s = u64_to_words(1_000_000_000_000);
        assert!(s.starts_with("one trillion"), "got: {s}");
    }

    #[test]
    fn number_to_words_node() {
        let node = NumberToWords::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "forty-two");
    }
}
