// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Function registry: known function signatures for DSL validation.
//!
//! Each registered function declares its name, category, expected wire
//! inputs, constant parameters, output count, and variadic behavior.
//! The compiler uses this to validate calls at parse time and to
//! generically dispatch variadic functions.
//!
//! Categories are a type-safe enum — every function must declare one.
//! The `describe gk functions` command groups by category automatically.
//! Stdlib modules declare their category via `// @category: Name`
//! comment syntax.

pub use crate::node::CompileLevel;
use crate::nodes::arithmetic::{SumN, ProductN, MinN, MaxN};

/// Functional category for a GK node function.
///
/// Every native node and stdlib module belongs to exactly one category.
/// Categories drive the `describe gk functions` grouping and provide
/// semantic organization for documentation and discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FuncCategory {
    /// Core deterministic hashing.
    Hashing,
    /// Integer arithmetic with constant parameters.
    Arithmetic,
    /// Variadic N-ary operations (sum, product, min, max).
    Variadic,
    /// Type conversions between u64, f64, String, etc.
    Conversions,
    /// Statistical distribution LUT builders and samplers.
    Distributions,
    /// Date and time generation and decomposition.
    Datetime,
    /// HTML, URL, hex, base64 encoding/decoding.
    Encoding,
    /// Linear interpolation, range mapping, quantization.
    Interpolation,
    /// Probability modeling: coins, selection, conditionals.
    Probability,
    /// Weighted categorical selection.
    Weighted,
    /// Printf-style and structured string formatting.
    Formatting,
    /// String generation: combinations, number words.
    String,
    /// JSON construction, serialization, merging.
    Json,
    /// Byte buffer construction and manipulation.
    ByteBuffers,
    /// Cryptographic and non-cryptographic digests.
    Digest,
    /// Coherent noise: Perlin, simplex.
    Noise,
    /// Regular expression matching and substitution.
    Regex,
    /// Bijective permutations and shuffles.
    Permutation,
    /// Real-world data: names, places, codes.
    RealData,
    /// Non-deterministic context: wall clock, counters.
    Context,
    /// Debugging and introspection.
    Diagnostic,
}

impl FuncCategory {
    /// Display name for the category (used in describe output).
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Hashing => "Hashing",
            Self::Arithmetic => "Arithmetic",
            Self::Variadic => "Variadic",
            Self::Conversions => "Conversions",
            Self::Distributions => "Distributions",
            Self::Datetime => "Datetime",
            Self::Encoding => "Encoding",
            Self::Interpolation => "Interpolation",
            Self::Probability => "Probability",
            Self::Weighted => "Weighted",
            Self::Formatting => "Formatting",
            Self::String => "String",
            Self::Json => "JSON",
            Self::ByteBuffers => "Byte Buffers",
            Self::Digest => "Digest",
            Self::Noise => "Noise",
            Self::Regex => "Regex",
            Self::Permutation => "Permutation",
            Self::RealData => "Real Data",
            Self::Context => "Context",
            Self::Diagnostic => "Diagnostic",
        }
    }

    /// Parse a category name from a string (case-insensitive).
    /// Used for `// @category: Name` syntax in stdlib modules.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "hashing" => Some(Self::Hashing),
            "arithmetic" => Some(Self::Arithmetic),
            "variadic" => Some(Self::Variadic),
            "conversions" | "conversion" => Some(Self::Conversions),
            "distributions" | "distribution" => Some(Self::Distributions),
            "datetime" | "date" | "time" => Some(Self::Datetime),
            "encoding" => Some(Self::Encoding),
            "interpolation" | "lerp" => Some(Self::Interpolation),
            "probability" => Some(Self::Probability),
            "weighted" => Some(Self::Weighted),
            "formatting" | "format" | "printf" => Some(Self::Formatting),
            "string" | "strings" => Some(Self::String),
            "json" => Some(Self::Json),
            "byte buffers" | "bytebuffers" | "bytes" => Some(Self::ByteBuffers),
            "digest" => Some(Self::Digest),
            "noise" => Some(Self::Noise),
            "regex" => Some(Self::Regex),
            "permutation" | "shuffle" => Some(Self::Permutation),
            "real data" | "realdata" | "realer" => Some(Self::RealData),
            "context" => Some(Self::Context),
            "diagnostic" | "diagnostics" | "debug" => Some(Self::Diagnostic),
            _ => None,
        }
    }

    /// Canonical ordering for display (same order as the enum definition).
    pub fn display_order() -> &'static [Self] {
        &[
            Self::Hashing, Self::Arithmetic, Self::Variadic,
            Self::Conversions, Self::Distributions, Self::Datetime,
            Self::Encoding, Self::Interpolation, Self::Probability,
            Self::Weighted, Self::Formatting, Self::String,
            Self::Json, Self::ByteBuffers, Self::Digest, Self::Noise,
            Self::Regex, Self::Permutation, Self::RealData,
            Self::Context, Self::Diagnostic,
        ]
    }
}

/// Description of a registered function's signature.
pub struct FuncSig {
    /// Function name as used in the DSL.
    pub name: &'static str,
    /// Functional category.
    pub category: FuncCategory,
    /// Number of wire (cycle-time) inputs. For variadic functions,
    /// this is the minimum required (may be 0).
    pub wire_inputs: usize,
    /// Description of constant parameters (name, required).
    pub const_params: &'static [(&'static str, bool)],
    /// Number of output ports (0 = dynamic, determined at compile time).
    pub outputs: usize,
    /// Short description for help/error messages.
    pub description: &'static str,
    /// Whether the wire inputs are variadic (*-arity).
    pub variadic: bool,
    /// For variadic functions: the identity element for zero inputs.
    pub identity: Option<u64>,
    /// Factory for variadic nodes: takes wire count, returns node.
    pub variadic_ctor: Option<fn(usize) -> Box<dyn crate::node::GkNode>>,
}

impl std::fmt::Debug for FuncSig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuncSig")
            .field("name", &self.name)
            .field("category", &self.category)
            .field("wire_inputs", &self.wire_inputs)
            .field("variadic", &self.variadic)
            .finish()
    }
}

impl Clone for FuncSig {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            category: self.category,
            wire_inputs: self.wire_inputs,
            const_params: self.const_params,
            outputs: self.outputs,
            description: self.description,
            variadic: self.variadic,
            identity: self.identity,
            variadic_ctor: self.variadic_ctor,
        }
    }
}

use FuncCategory as C;

macro_rules! fixed {
    ($cat:expr, $name:expr, $wi:expr, $cp:expr, $out:expr, $desc:expr) => {
        FuncSig {
            name: $name, category: $cat, wire_inputs: $wi, const_params: $cp,
            outputs: $out, description: $desc,
            variadic: false, identity: None, variadic_ctor: None,
        }
    };
}

macro_rules! variadic {
    ($cat:expr, $name:expr, $min_wi:expr, $out:expr, $identity:expr, $ctor:expr, $desc:expr) => {
        FuncSig {
            name: $name, category: $cat, wire_inputs: $min_wi, const_params: &[],
            outputs: $out, description: $desc,
            variadic: true, identity: $identity,
            variadic_ctor: Some($ctor),
        }
    };
}

/// Return the full registry of known functions.
pub fn registry() -> Vec<FuncSig> {
    vec![
        // --- Hashing ---
        fixed!(C::Hashing, "hash", 1, &[], 1, "64-bit xxHash3"),

        // --- Variadic arithmetic ---
        variadic!(C::Variadic, "sum", 0, 1, Some(0),
            |n| Box::new(SumN::new(n)),
            "sum of N inputs (wrapping); identity = 0"),
        variadic!(C::Variadic, "product", 0, 1, Some(1),
            |n| Box::new(ProductN::new(n)),
            "product of N inputs (wrapping); identity = 1"),
        variadic!(C::Variadic, "min", 0, 1, Some(u64::MAX),
            |n| Box::new(MinN::new(n)),
            "minimum of N inputs; identity = u64::MAX"),
        variadic!(C::Variadic, "max", 0, 1, Some(0),
            |n| Box::new(MaxN::new(n)),
            "maximum of N inputs; identity = 0"),

        // --- Formatting ---
        FuncSig {
            name: "printf", category: C::Formatting, wire_inputs: 0,
            const_params: &[("format", true)],
            outputs: 1, description: "printf-style formatting: printf(fmt, a, b, ...) -> String",
            variadic: true, identity: None, variadic_ctor: None,
        },

        // --- Arithmetic ---
        fixed!(C::Arithmetic, "add", 1, &[("addend", true)], 1, "add a constant (wrapping)"),
        fixed!(C::Arithmetic, "mul", 1, &[("factor", true)], 1, "multiply by a constant (wrapping)"),
        fixed!(C::Arithmetic, "div", 1, &[("divisor", true)], 1, "divide by a constant"),
        fixed!(C::Arithmetic, "mod", 1, &[("modulus", true)], 1, "modulo by a constant"),
        fixed!(C::Arithmetic, "clamp", 1, &[("min", true), ("max", true)], 1, "clamp u64 to [min, max]"),
        fixed!(C::Arithmetic, "interleave", 2, &[], 1, "interleave bits of two u64 values"),
        FuncSig { name: "mixed_radix", category: C::Arithmetic, wire_inputs: 1, const_params: &[], outputs: 0,
            description: "decompose into mixed-radix digits (output count = number of radixes)",
            variadic: false, identity: None, variadic_ctor: None },
        fixed!(C::Arithmetic, "identity", 1, &[], 1, "passthrough"),

        // --- Conversions ---
        fixed!(C::Conversions, "unit_interval", 1, &[], 1, "normalize u64 to f64 in [0, 1)"),
        fixed!(C::Conversions, "clamp_f64", 1, &[("min", true), ("max", true)], 1, "clamp f64 to [min, max]"),
        fixed!(C::Conversions, "f64_to_u64", 1, &[], 1, "truncate f64 to u64 (lossy)"),
        fixed!(C::Conversions, "round_to_u64", 1, &[], 1, "round f64 to nearest u64"),
        fixed!(C::Conversions, "floor_to_u64", 1, &[], 1, "floor f64 to u64"),
        fixed!(C::Conversions, "ceil_to_u64", 1, &[], 1, "ceil f64 to u64"),
        fixed!(C::Conversions, "discretize", 1, &[("range", true), ("buckets", true)], 1, "bin f64 into N equal-width buckets"),
        fixed!(C::Conversions, "format_u64", 1, &[("radix", false)], 1, "format u64 as string (decimal/hex/octal/binary)"),
        fixed!(C::Conversions, "format_f64", 1, &[("precision", true)], 1, "format f64 with decimal precision"),
        fixed!(C::Conversions, "zero_pad_u64", 1, &[("width", true)], 1, "zero-pad u64 to fixed width string"),

        // --- Distributions ---
        fixed!(C::Distributions, "dist_normal", 0, &[("mean", true), ("stddev", true)], 1, "build normal distribution LUT"),
        fixed!(C::Distributions, "dist_exponential", 0, &[("rate", true)], 1, "build exponential distribution LUT"),
        fixed!(C::Distributions, "dist_uniform", 0, &[("min", true), ("max", true)], 1, "build uniform distribution LUT"),
        fixed!(C::Distributions, "dist_pareto", 0, &[("scale", true), ("shape", true)], 1, "build Pareto distribution LUT"),
        fixed!(C::Distributions, "dist_zipf", 0, &[("n", true), ("exponent", true)], 1, "build Zipf distribution LUT"),
        fixed!(C::Distributions, "lut_sample", 1, &[], 1, "interpolating lookup table sample"),
        fixed!(C::Distributions, "icd_normal", 1, &[("mean", true), ("stddev", true)], 1, "sample from normal distribution"),
        fixed!(C::Distributions, "icd_exponential", 1, &[("rate", true)], 1, "sample from exponential distribution"),

        // --- Datetime ---
        fixed!(C::Datetime, "epoch_scale", 1, &[("factor", true)], 1, "scale u64 to epoch millis"),
        fixed!(C::Datetime, "epoch_offset", 1, &[("base", true)], 1, "add base epoch offset"),
        fixed!(C::Datetime, "to_timestamp", 1, &[], 1, "format epoch millis as ISO-8601"),
        fixed!(C::Datetime, "date_components", 1, &[], 7, "decompose epoch to year/month/day/hour/min/sec/ms"),

        // --- Encoding ---
        fixed!(C::Encoding, "html_encode", 1, &[], 1, "HTML entity encode"),
        fixed!(C::Encoding, "html_decode", 1, &[], 1, "HTML entity decode"),
        fixed!(C::Encoding, "url_encode", 1, &[], 1, "URL percent-encode"),
        fixed!(C::Encoding, "url_decode", 1, &[], 1, "URL percent-decode"),

        // --- Interpolation ---
        fixed!(C::Interpolation, "lerp", 1, &[("a", true), ("b", true)], 1, "linear interpolation with fixed endpoints"),
        fixed!(C::Interpolation, "scale_range", 1, &[("min", true), ("max", true)], 1, "map u64 to f64 range"),
        fixed!(C::Interpolation, "quantize", 1, &[("step", true)], 1, "round to nearest multiple of step"),

        // --- Probability ---
        fixed!(C::Probability, "fair_coin", 1, &[], 1, "50/50 binary outcome (0 or 1)"),
        fixed!(C::Probability, "unfair_coin", 1, &[("p", true)], 1, "biased coin: 1 with probability p, else 0"),
        fixed!(C::Probability, "select", 3, &[], 1, "binary conditional: if_true when cond != 0, else if_false"),
        fixed!(C::Probability, "chance", 1, &[("p", true)], 1, "like unfair_coin but returns f64 (0.0 or 1.0)"),
        fixed!(C::Probability, "n_of", 1, &[("n", true), ("m", true)], 1, "exactly n of every m inputs return 1"),
        fixed!(C::Probability, "one_of", 1, &[("values", true)], 1, "uniform selection from N constant values"),
        fixed!(C::Probability, "one_of_weighted", 1, &[("spec", true)], 1, "weighted selection from 'val:weight,...' spec"),
        fixed!(C::Probability, "blend", 2, &[("mix", true)], 1, "weighted mix: a*(1-mix) + b*mix"),

        // --- Weighted ---
        fixed!(C::Weighted, "weighted_strings", 1, &[("spec", true)], 1, "weighted string selection from inline spec"),
        fixed!(C::Weighted, "weighted_u64", 1, &[("spec", true)], 1, "weighted u64 selection from inline spec"),

        // --- String ---
        fixed!(C::String, "combinations", 1, &[("pattern", true)], 1, "mixed-radix character set mapping"),
        fixed!(C::String, "number_to_words", 1, &[], 1, "spell out number in English"),

        // --- Diagnostic ---
        fixed!(C::Diagnostic, "type_of", 1, &[], 1, "emit type name as string"),
        fixed!(C::Diagnostic, "debug_repr", 1, &[], 1, "emit Debug representation as string"),
        fixed!(C::Diagnostic, "inspect", 1, &[], 1, "passthrough with stderr logging"),

        // --- Byte buffer ---
        fixed!(C::ByteBuffers, "u64_to_bytes", 1, &[], 1, "convert u64 to 8 bytes LE"),
        fixed!(C::ByteBuffers, "bytes_from_hash", 1, &[("size", true)], 1, "generate N deterministic bytes"),
        fixed!(C::ByteBuffers, "to_hex", 1, &[], 1, "encode bytes as hex string"),
        fixed!(C::ByteBuffers, "from_hex", 1, &[], 1, "decode hex string to bytes"),

        // --- Digest ---
        fixed!(C::Digest, "sha256", 1, &[], 1, "SHA-256 digest"),
        fixed!(C::Digest, "md5", 1, &[], 1, "MD5 digest"),
        fixed!(C::Digest, "to_base64", 1, &[], 1, "base64 encode"),
        fixed!(C::Digest, "from_base64", 1, &[], 1, "base64 decode"),

        // --- JSON ---
        fixed!(C::Json, "to_json", 1, &[], 1, "promote value to JSON"),
        fixed!(C::Json, "json_to_str", 1, &[], 1, "serialize JSON to compact string"),
        fixed!(C::Json, "escape_json", 1, &[], 1, "escape string for JSON embedding"),
        fixed!(C::Json, "json_merge", 2, &[], 1, "shallow merge two JSON objects"),

        // --- Context ---
        fixed!(C::Context, "current_epoch_millis", 0, &[], 1, "current wall-clock time (non-deterministic)"),
        fixed!(C::Context, "counter", 0, &[], 1, "monotonic counter (non-deterministic)"),
        fixed!(C::Context, "session_start_millis", 0, &[], 1, "session start time (frozen at init)"),

        // --- Noise ---
        fixed!(C::Noise, "perlin_1d", 1, &[("seed", true), ("frequency", true)], 1, "1D Perlin noise"),
        fixed!(C::Noise, "perlin_2d", 2, &[("seed", true), ("frequency", true)], 1, "2D Perlin noise"),
        fixed!(C::Noise, "simplex_2d", 2, &[("seed", true), ("frequency", true)], 1, "2D simplex noise"),

        // --- Regex ---
        fixed!(C::Regex, "regex_replace", 1, &[("pattern", true), ("replacement", true)], 1, "regex substitution"),
        fixed!(C::Regex, "regex_match", 1, &[("pattern", true)], 1, "test if string matches regex"),

        // --- PCG RNG ---
        fixed!(C::Permutation, "pcg", 1, &[("seed", true), ("stream", true)], 1, "PCG-RXS-M-XS seekable RNG (pure function)"),
        fixed!(C::Permutation, "pcg_stream", 2, &[("seed", true)], 1, "PCG with dynamic stream ID from wire"),
        fixed!(C::Permutation, "cycle_walk", 1, &[("range", true), ("seed", true), ("stream", true)], 1, "bijective permutation via Feistel + cycle-walking"),

        // --- Permutation ---
        fixed!(C::Permutation, "shuffle", 1, &[("min", false), ("size", true)], 1, "bijective LFSR permutation"),

        // --- Real-world data ---
        fixed!(C::RealData, "first_names", 1, &[], 1, "Census first name (weighted)"),
        fixed!(C::RealData, "full_names", 1, &[], 1, "full name (first + last)"),
        fixed!(C::RealData, "state_codes", 1, &[], 1, "US state abbreviation"),
        fixed!(C::RealData, "country_names", 1, &[], 1, "country name"),
    ]
}

/// Return functions grouped by category in display order.
pub fn by_category() -> Vec<(FuncCategory, Vec<FuncSig>)> {
    let reg = registry();
    let mut groups: std::collections::HashMap<FuncCategory, Vec<FuncSig>> = std::collections::HashMap::new();
    for sig in reg {
        groups.entry(sig.category).or_default().push(sig);
    }
    FuncCategory::display_order().iter()
        .filter_map(|cat| {
            groups.remove(cat).map(|funcs| (*cat, funcs))
        })
        .collect()
}

/// Find the closest function name to a misspelling.
pub fn suggest_function(name: &str) -> Option<&'static str> {
    let reg = registry();
    let mut best: Option<(&str, usize)> = None;
    for sig in &reg {
        let dist = edit_distance(name, sig.name);
        if dist <= 3 {
            if best.is_none() || dist < best.unwrap().1 {
                best = Some((sig.name, dist));
            }
        }
    }
    best.map(|(name, _)| name)
}

/// Find a registered function by name.
pub fn lookup(name: &str) -> Option<&'static FuncSig> {
    let reg = registry();
    for sig in &reg {
        if sig.name == name {
            return Some(Box::leak(Box::new(sig.clone())));
        }
    }
    None
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut matrix = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in 0..=a.len() { matrix[i][0] = i; }
    for j in 0..=b.len() { matrix[0][j] = j; }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            matrix[i][j] = (matrix[i - 1][j] + 1)
                .min(matrix[i][j - 1] + 1)
                .min(matrix[i - 1][j - 1] + cost);
        }
    }
    matrix[a.len()][b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggest_close_match() {
        assert_eq!(suggest_function("hsh"), Some("hash"));
        assert_eq!(suggest_function("hahs"), Some("hash"));
        assert_eq!(suggest_function("interleav"), Some("interleave"));
    }

    #[test]
    fn suggest_no_match() {
        assert_eq!(suggest_function("zzzzzzzzz"), None);
    }

    #[test]
    fn lookup_exists() {
        let sig = lookup("hash").unwrap();
        assert_eq!(sig.wire_inputs, 1);
        assert_eq!(sig.outputs, 1);
        assert!(!sig.variadic);
        assert_eq!(sig.category, FuncCategory::Hashing);
    }

    #[test]
    fn lookup_missing() {
        assert!(lookup("nonexistent").is_none());
    }

    #[test]
    fn lookup_variadic() {
        let sig = lookup("sum").unwrap();
        assert!(sig.variadic);
        assert_eq!(sig.identity, Some(0));
        assert_eq!(sig.category, FuncCategory::Variadic);
    }

    #[test]
    fn every_function_has_category() {
        let reg = registry();
        for sig in &reg {
            // Just verify the category display name is non-empty
            assert!(!sig.category.display_name().is_empty(),
                "function '{}' has no category display name", sig.name);
        }
    }

    #[test]
    fn by_category_covers_all() {
        let grouped = by_category();
        let total: usize = grouped.iter().map(|(_, funcs)| funcs.len()).sum();
        let reg = registry();
        assert_eq!(total, reg.len(), "by_category must cover all registered functions");
    }

    #[test]
    fn category_parse_roundtrip() {
        for cat in FuncCategory::display_order() {
            let name = cat.display_name();
            let parsed = FuncCategory::parse(name);
            assert_eq!(parsed, Some(*cat), "failed to parse category '{name}'");
        }
    }

    #[test]
    fn registry_has_entries() {
        let reg = registry();
        assert!(reg.len() > 50, "registry should have 50+ functions");
    }
}
