// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! String generation and transformation nodes.

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
/// SRD-80 PR B.6 — derived state for `combinations`. Computed
/// once per node instance via `from_pattern`; the macro stores
/// the instance in a struct field and hands the eval body a
/// `&ParsedCombinations` borrow each call.
pub struct ParsedCombinations {
    pub segments: Vec<Segment>,
    pub modulus: u64,
}

impl crate::derive_support::PolydatSetup for ParsedCombinations {}

pub enum Segment {
    /// Variable: select one char from the charset based on a radix digit.
    Charset(Vec<char>),
    /// Fixed: always emit this string (e.g., a literal separator).
    Literal(String),
}

impl ParsedCombinations {
    /// Single-call setup. The `#[polydat_node]` macro invokes
    /// this exactly once in the generated `Combinations::new()`;
    /// no other call path exists.
    pub fn from_pattern(pattern: &str) -> Self {
        let mut segments = Vec::new();
        let mut modulus: u64 = 1;
        for spec in pattern.split(';') {
            let chars = parse_charset(spec);
            if chars.len() == 1 && !spec.contains('-') {
                segments.push(Segment::Literal(chars[0].to_string()));
            } else if chars.is_empty() {
                segments.push(Segment::Literal(spec.to_string()));
            } else {
                modulus = modulus.saturating_mul(chars.len() as u64);
                segments.push(Segment::Charset(chars));
            }
        }
        Self { segments, modulus }
    }
}

/// Map a u64 input to a deterministic string by interpreting
/// it as a multi-positional choice over the pattern's charsets.
#[crate::polydat_node(category = String)]
fn combinations(
    input: u64,
    pattern: crate::derive_support::Const<&str>,
    #[poly_const(ParsedCombinations::from_pattern, from = pattern)]
    parsed: &ParsedCombinations,
) -> String {
    let mut remainder = input % parsed.modulus;
    let mut result = String::with_capacity(parsed.segments.len());
    for seg in &parsed.segments {
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
    result
}

impl Combinations {
    /// The total number of unique combinations before wrapping.
    pub fn cardinality(&self) -> u64 {
        self.parsed.modulus
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
/// Spell a u64 input as English words. Migrated to
/// `#[polydat_node]` per SRD-80 PR B.4.
#[crate::polydat_node(category = String)]
fn number_to_words(input: u64) -> String {
    u64_to_words(input)
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

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::ast::SlotType;

/// Signatures for string generation nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        // `combinations` migrated to `#[polydat_node]` per
        // SRD-80 PR B.6 — Setup<ParsedCombinations> with
        // PolydatSetup-compatible from_pattern.
        // `number_to_words` and `hashed_uuid` migrated to
        // `#[polydat_node]` per SRD-80 PR B.4.
        // `char_buf` migrated to `#[polydat_node]` per SRD-80 PR B.6.
        FuncSig {
            name: "file_line_at", category: C::String, outputs: 1,
            description: "select a line from a file by index",
            help: "Read a file at construction time and return a line at cycle-time index.\nIndex wraps modulo line count so every u64 input is valid.\nFile path is a const string argument.\nParameters:\n  index    — u64 wire input\n  filename — ConstStr path to file\nExample: file_line_at(mod(hash(cycle), 1000), \"words.txt\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true, example: "\"test.csv\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
            // Hand registration: no static return-port declaration;
            // type inference falls back to the name heuristic.
            output_port: None,
        },
        // `str_concat` migrated to `#[polydat_node]` per SRD-80 PR B.9.
        // `str_lower` and `str_upper` migrated to
        // `#[polydat_node]` per SRD-80 PR B.4 — their FuncSig
        // entries flow through the proc-macro-emitted
        // NodeRegistration, no manual SIGS entry needed.
    ]
}

// =================================================================
// HashedUuid: deterministic UUID v4 from a u64 seed
// =================================================================

/// Generate a deterministic UUID v4 string from a u64 seed.
/// Same seed always produces the same UUID; the hash output
/// fills the 128 UUID bits with version (4) and variant
/// (RFC 4122) bits set per spec.
///
/// Signature: `hashed_uuid(input: u64) -> (String)`
///
/// SRD-80 PR B.4 migration.
#[crate::polydat_node(category = String)]
fn hashed_uuid(input: u64) -> String {
    // Two hashes fill 128 bits.
    let h1 = xxhash_rust::xxh3::xxh3_64(&input.to_le_bytes());
    let h2 = xxhash_rust::xxh3::xxh3_64(&h1.to_le_bytes());
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&h1.to_le_bytes());
    bytes[8..].copy_from_slice(&h2.to_le_bytes());
    // Version 4 (bits 12-15 of byte 6).
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    // Variant RFC 4122 (bits 6-7 of byte 8).
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

// =================================================================
// CharBuf: deterministic string from seed + charset + length
// =================================================================

/// Generate a deterministic string of a given length from a seed
/// and character set.
///
/// The seed is hashed and used to index into the charset repeatedly.
/// Same seed + charset + length always produces the same string.
///
/// Signature: `char_buf(seed: u64, charset: &str, length: u64) -> (String)`
/// Expand a charset spec like "A-Za-z0-9" into a Vec<char>.
/// SRD-80 PR B.6 setup helper for `char_buf`.
fn expand_charset(charset: &str) -> Vec<char> {
    if charset.is_empty() {
        return ('a'..='z').collect();
    }
    let mut result = Vec::new();
    let chars_vec: Vec<char> = charset.chars().collect();
    let mut i = 0;
    while i < chars_vec.len() {
        if i + 2 < chars_vec.len() && chars_vec[i + 1] == '-' {
            for c in chars_vec[i]..=chars_vec[i + 2] { result.push(c); }
            i += 3;
        } else {
            result.push(chars_vec[i]);
            i += 1;
        }
    }
    if result.is_empty() { ('a'..='z').collect() } else { result }
}

/// Generate a deterministic string of a given length from a
/// seed and character set. SRD-80 PR B.6 migration.
#[crate::polydat_node(category = String)]
fn char_buf(
    seed: u64,
    charset: crate::derive_support::Const<&str>,
    length: u64,
    #[poly_const(expand_charset, from = charset)]
    chars: &Vec<char>,
) -> String {
    let n = chars.len();
    let len = length as usize;
    if n == 0 || len == 0 {
        return String::new();
    }
    let mut result = String::with_capacity(len);
    let mut h = seed;
    for _ in 0..len {
        h = xxhash_rust::xxh3::xxh3_64(&h.to_le_bytes());
        result.push(chars[(h as usize) % n]);
    }
    result
}

// =================================================================
// FileLineAt: index into a file of lines at cycle time
// =================================================================

/// Select a line from a file by index, wrapping modulo line count.
///
/// The file is read once at node construction (init time). The index
impl crate::derive_support::PolydatSetup for Vec<String> {}

/// Read `filename` and split it into lines. Panics on file
/// I/O failure; the macro's build-closure `catch_unwind`
/// surfaces it as a clean compile error.
fn read_file_lines(filename: &str) -> Vec<String> {
    let content = std::fs::read_to_string(filename)
        .unwrap_or_else(|e| panic!("failed to read file '{filename}': {e}"));
    let lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();
    if lines.is_empty() {
        panic!("file '{filename}' has no lines");
    }
    lines
}

/// Cycle-time line lookup over a pre-loaded text file. SRD-80b
/// Phase E migration: `filename` is read at construction time
/// via `#[poly_const]`; the cycle input selects a line modulo
/// the total count.
#[crate::polydat_node(category = String)]
fn file_line_at(
    index: u64,
    filename: crate::derive_support::Const<&str>,
    #[poly_const(read_file_lines, from = filename)]
    lines: &Vec<String>,
) -> String {
    let _ = filename;
    let idx = index as usize;
    lines[idx % lines.len()].clone()
}

// =================================================================
// StrConcat: variadic string concatenation
// =================================================================

/// Concatenate N wire inputs into a single Str output.
///
/// Each input is rendered to its display form: Str passes through,
/// numerics format as decimal, Bool as `true`/`false`, Json via
/// `to_string`. Mixed-type inputs are accepted — the assembler skips
/// type checking for str_concat (like printf), so any upstream wire
/// type composes.
///
/// Used by the DSL desugar of `+` between Str-typed operands; also
/// callable directly as `str_concat(a, b, c, ...)`.
///
/// Signature: `str_concat(in_0, in_1, ...) -> (String)`
/// Concatenate N values, rendering each as its display form.
/// SRD-80 PR B.9 — variadic over `&[Value]`. The body stringifies
/// per element so mixed-type inputs (Str + U64 + Bool, etc.)
/// produce a single concatenated string; this matches the
/// DSL's lowering of `+` between Str-typed operands.
#[crate::polydat_node(category = String)]
fn str_concat(parts: &[polydat::ast::Value]) -> String {
    use polydat::ast::Value;
    let mut out = String::new();
    for v in parts {
        match v {
            Value::Str(s) => out.push_str(s),
            Value::U64(n) => out.push_str(&n.to_string()),
            Value::F64(n) => out.push_str(&n.to_string()),
            Value::Bool(b) => out.push_str(&b.to_string()),
            Value::Json(j) => out.push_str(&j.to_string()),
            Value::Bytes(b) => out.push_str(&String::from_utf8_lossy(b)),
            other => out.push_str(&format!("{other:?}")),
        }
    }
    out
}

// =================================================================
// StrLower / StrUpper: Unicode case-folding helpers
// =================================================================

/// Fold a string to lowercase (`str.to_lowercase()` semantics).
///
/// Signature: `str_lower(input: Str) -> (Str)`
///
/// SRD-80 PR B.4 — migrated to `#[polydat_node]`. `String`
/// (not `&str`) so `FromValue<String>` honors the legacy
/// "stringify any input via `to_display_string`" behavior;
/// switching to `&str` would tighten this to require Str
/// inputs only, which the type-checker doesn't yet enforce
/// (deferred to SRD-79's type-driven resolution).
#[crate::polydat_node(category = String)]
fn str_lower(input: String) -> String {
    input.to_lowercase()
}

/// Fold a string to uppercase (`str.to_uppercase()` semantics).
///
/// Signature: `str_upper(input: Str) -> (Str)`
#[crate::polydat_node(category = String)]
fn str_upper(input: String) -> String {
    input.to_uppercase()
}

/// Try to build a string node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    match name {
        // `combinations` routes through proc-macro-emitted
        // NodeRegistration per SRD-80 PR B.6.
        // `number_to_words` / `hashed_uuid` route through
        // proc-macro-emitted NodeRegistration per SRD-80 PR B.4.
        // `char_buf` routes via proc-macro-emitted NodeRegistration per SRD-80 PR B.6.
        // `file_line_at` routes via proc-macro-emitted NodeRegistration per SRD-80b Phase E.
        // `str_concat` routes via proc-macro NodeRegistration per SRD-80 PR B.9.
        // `str_lower` / `str_upper` route through the
        // proc-macro-emitted NodeRegistration per SRD-80 PR B.4.
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    // --- Combinations tests ---

    #[test]
    fn combinations_digits() {
        let node = Combinations::new("0-9;0-9;0-9".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::U64(123)], &mut out);
        let s = out[0].as_str();
        assert_eq!(s.len(), 3);
        assert!(s.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn combinations_with_separator() {
        let node = Combinations::new("0-9;0-9;0-9;-;0-9;0-9;0-9".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        let s = out[0].as_str();
        assert_eq!(s.len(), 7); // 3 digits + dash + 3 digits
        assert_eq!(&s[3..4], "-");
    }

    #[test]
    fn combinations_alpha() {
        let node = Combinations::new("A-Z;A-Z;A-Z".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_str(), "AAA");
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_str(), "BAA");
    }

    #[test]
    fn combinations_cardinality() {
        let node = Combinations::new("0-9;0-9;-;A-Z".to_string());
        // 10 * 10 * 26 = 2600 (separator doesn't count)
        assert_eq!(node.cardinality(), 2600);
    }

    #[test]
    fn combinations_deterministic() {
        let node = Combinations::new("A-Z;0-9".to_string());
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(42)], &mut out1);
        node.eval(&[Value::U64(42)], &mut out2);
        assert_eq!(out1[0].as_str(), out2[0].as_str());
    }

    #[test]
    fn combinations_wraps() {
        let node = Combinations::new("0-9".to_string());
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

    // --- StrConcat tests ---

    #[test]
    fn str_concat_basic() {
        let node = StrConcat::new(2);
        let mut out = [Value::None];
        node.eval(
            &[Value::Str("hello ".into()), Value::Str("world".into())],
            &mut out,
        );
        assert_eq!(out[0].as_str(), "hello world");
    }

    #[test]
    fn str_concat_mixed_types() {
        let node = StrConcat::new(4);
        let mut out = [Value::None];
        node.eval(
            &[
                Value::Str("id=".into()),
                Value::U64(42),
                Value::Str(" v=".into()),
                Value::F64(3.14),
            ],
            &mut out,
        );
        assert_eq!(out[0].as_str(), "id=42 v=3.14");
    }

    #[test]
    fn str_concat_empty() {
        let node = StrConcat::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str(), "");
    }

    #[test]
    fn str_lower_ascii_and_unicode() {
        let node = StrLower::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("OTHER_M8".into())], &mut out);
        assert_eq!(out[0].as_str(), "other_m8");
        // Unicode folding (Rust's str::to_lowercase is full Unicode).
        node.eval(&[Value::Str("ÄPFEL".into())], &mut out);
        assert_eq!(out[0].as_str(), "äpfel");
    }

    #[test]
    fn str_lower_idempotent_on_already_lowercase() {
        let node = StrLower::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("fknn_oat_other".into())], &mut out);
        assert_eq!(out[0].as_str(), "fknn_oat_other");
    }

    #[test]
    fn str_upper_ascii_and_unicode() {
        let node = StrUpper::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("other_m8".into())], &mut out);
        assert_eq!(out[0].as_str(), "OTHER_M8");
        node.eval(&[Value::Str("äpfel".into())], &mut out);
        assert_eq!(out[0].as_str(), "ÄPFEL");
    }

    // `str_lower_accepts_non_string_via_display` retired: SRD-80b
    // Wire trait dispatch panics on shape mismatch instead of
    // silently display-coercing. Workload-level support for
    // chained `str_lower(format_u64(...))` flows through assembler-
    // inserted Str adapters (e.g. polyfill U64ToStr), not through
    // a lying Wire impl. Tests that want to exercise the coercion
    // path should construct a Str via `format_u64` upstream and
    // feed that into str_lower's eval.
}
