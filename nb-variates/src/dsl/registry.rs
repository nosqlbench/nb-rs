// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Function registry: known function signatures for DSL validation.
//!
//! Each registered function declares its name, expected wire inputs,
//! expected constant parameters, and output count. The compiler uses
//! this to validate calls at parse time with clear error messages.

pub use crate::node::CompileLevel;

/// Description of a registered function's signature.
#[derive(Debug, Clone)]
pub struct FuncSig {
    /// Function name as used in the DSL.
    pub name: &'static str,
    /// Number of wire (cycle-time) inputs.
    pub wire_inputs: usize,
    /// Description of constant parameters (name, required).
    pub const_params: &'static [(&'static str, bool)],
    /// Number of output ports.
    pub outputs: usize,
    /// Short description for help/error messages.
    pub description: &'static str,
}

/// Return the full registry of known functions.
pub fn registry() -> Vec<FuncSig> {
    vec![
        // --- Hashing ---
        FuncSig { name: "hash", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "64-bit xxHash3" },

        // --- Arithmetic ---
        FuncSig { name: "add", wire_inputs: 1, const_params: &[("addend", true)], outputs: 1,
            description: "add a constant (wrapping)" },
        FuncSig { name: "mul", wire_inputs: 1, const_params: &[("factor", true)], outputs: 1,
            description: "multiply by a constant (wrapping)" },
        FuncSig { name: "div", wire_inputs: 1, const_params: &[("divisor", true)], outputs: 1,
            description: "divide by a constant" },
        FuncSig { name: "mod", wire_inputs: 1, const_params: &[("modulus", true)], outputs: 1,
            description: "modulo by a constant" },
        FuncSig { name: "clamp", wire_inputs: 1, const_params: &[("min", true), ("max", true)], outputs: 1,
            description: "clamp u64 to [min, max]" },
        FuncSig { name: "interleave", wire_inputs: 2, const_params: &[], outputs: 1,
            description: "interleave bits of two u64 values" },
        FuncSig { name: "mixed_radix", wire_inputs: 1, const_params: &[], outputs: 0, // dynamic
            description: "decompose into mixed-radix digits (output count = number of radixes)" },
        FuncSig { name: "identity", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "passthrough" },

        // --- Conversions ---
        FuncSig { name: "unit_interval", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "normalize u64 to f64 in [0, 1)" },
        FuncSig { name: "clamp_f64", wire_inputs: 1, const_params: &[("min", true), ("max", true)], outputs: 1,
            description: "clamp f64 to [min, max]" },
        FuncSig { name: "f64_to_u64", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "truncate f64 to u64 (lossy)" },
        FuncSig { name: "round_to_u64", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "round f64 to nearest u64" },
        FuncSig { name: "floor_to_u64", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "floor f64 to u64" },
        FuncSig { name: "ceil_to_u64", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "ceil f64 to u64" },
        FuncSig { name: "discretize", wire_inputs: 1, const_params: &[("range", true), ("buckets", true)], outputs: 1,
            description: "bin f64 into N equal-width buckets" },
        FuncSig { name: "format_u64", wire_inputs: 1, const_params: &[("radix", false)], outputs: 1,
            description: "format u64 as string (decimal/hex/octal/binary)" },
        FuncSig { name: "format_f64", wire_inputs: 1, const_params: &[("precision", true)], outputs: 1,
            description: "format f64 with decimal precision" },
        FuncSig { name: "zero_pad_u64", wire_inputs: 1, const_params: &[("width", true)], outputs: 1,
            description: "zero-pad u64 to fixed width string" },

        // --- Distributions ---
        FuncSig { name: "dist_normal", wire_inputs: 0, const_params: &[("mean", true), ("stddev", true)], outputs: 1,
            description: "build normal distribution LUT" },
        FuncSig { name: "dist_exponential", wire_inputs: 0, const_params: &[("rate", true)], outputs: 1,
            description: "build exponential distribution LUT" },
        FuncSig { name: "dist_uniform", wire_inputs: 0, const_params: &[("min", true), ("max", true)], outputs: 1,
            description: "build uniform distribution LUT" },
        FuncSig { name: "dist_pareto", wire_inputs: 0, const_params: &[("scale", true), ("shape", true)], outputs: 1,
            description: "build Pareto distribution LUT" },
        FuncSig { name: "dist_zipf", wire_inputs: 0, const_params: &[("n", true), ("exponent", true)], outputs: 1,
            description: "build Zipf distribution LUT" },
        FuncSig { name: "lut_sample", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "interpolating lookup table sample" },
        FuncSig { name: "icd_normal", wire_inputs: 1, const_params: &[("mean", true), ("stddev", true)], outputs: 1,
            description: "sample from normal distribution" },
        FuncSig { name: "icd_exponential", wire_inputs: 1, const_params: &[("rate", true)], outputs: 1,
            description: "sample from exponential distribution" },

        // --- Datetime ---
        FuncSig { name: "epoch_scale", wire_inputs: 1, const_params: &[("factor", true)], outputs: 1,
            description: "scale u64 to epoch millis" },
        FuncSig { name: "epoch_offset", wire_inputs: 1, const_params: &[("base", true)], outputs: 1,
            description: "add base epoch offset" },
        FuncSig { name: "to_timestamp", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "format epoch millis as ISO-8601" },
        FuncSig { name: "date_components", wire_inputs: 1, const_params: &[], outputs: 7,
            description: "decompose epoch to year/month/day/hour/min/sec/ms" },

        // --- Encoding ---
        FuncSig { name: "html_encode", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "HTML entity encode" },
        FuncSig { name: "html_decode", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "HTML entity decode" },
        FuncSig { name: "url_encode", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "URL percent-encode" },
        FuncSig { name: "url_decode", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "URL percent-decode" },

        // --- Lerp ---
        FuncSig { name: "lerp", wire_inputs: 1, const_params: &[("a", true), ("b", true)], outputs: 1,
            description: "linear interpolation with fixed endpoints" },
        FuncSig { name: "scale_range", wire_inputs: 1, const_params: &[("min", true), ("max", true)], outputs: 1,
            description: "map u64 to f64 range" },
        FuncSig { name: "quantize", wire_inputs: 1, const_params: &[("step", true)], outputs: 1,
            description: "round to nearest multiple of step" },

        // --- Weighted ---
        FuncSig { name: "weighted_strings", wire_inputs: 1, const_params: &[("spec", true)], outputs: 1,
            description: "weighted string selection from inline spec" },
        FuncSig { name: "weighted_u64", wire_inputs: 1, const_params: &[("spec", true)], outputs: 1,
            description: "weighted u64 selection from inline spec" },

        // --- String ---
        FuncSig { name: "combinations", wire_inputs: 1, const_params: &[("pattern", true)], outputs: 1,
            description: "mixed-radix character set mapping" },
        FuncSig { name: "number_to_words", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "spell out number in English" },

        // --- Diagnostic ---
        FuncSig { name: "type_of", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "emit type name as string" },
        FuncSig { name: "debug_repr", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "emit Debug representation as string" },
        FuncSig { name: "inspect", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "passthrough with stderr logging" },

        // --- Byte buffer ---
        FuncSig { name: "u64_to_bytes", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "convert u64 to 8 bytes LE" },
        FuncSig { name: "bytes_from_hash", wire_inputs: 1, const_params: &[("size", true)], outputs: 1,
            description: "generate N deterministic bytes" },
        FuncSig { name: "to_hex", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "encode bytes as hex string" },
        FuncSig { name: "from_hex", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "decode hex string to bytes" },

        // --- Digest ---
        FuncSig { name: "sha256", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "SHA-256 digest" },
        FuncSig { name: "md5", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "MD5 digest" },
        FuncSig { name: "to_base64", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "base64 encode" },
        FuncSig { name: "from_base64", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "base64 decode" },

        // --- JSON ---
        FuncSig { name: "to_json", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "promote value to JSON" },
        FuncSig { name: "json_to_str", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "serialize JSON to compact string" },
        FuncSig { name: "escape_json", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "escape string for JSON embedding" },
        FuncSig { name: "json_merge", wire_inputs: 2, const_params: &[], outputs: 1,
            description: "shallow merge two JSON objects" },

        // --- Context ---
        FuncSig { name: "current_epoch_millis", wire_inputs: 0, const_params: &[], outputs: 1,
            description: "current wall-clock time (non-deterministic)" },
        FuncSig { name: "counter", wire_inputs: 0, const_params: &[], outputs: 1,
            description: "monotonic counter (non-deterministic)" },
        FuncSig { name: "session_start_millis", wire_inputs: 0, const_params: &[], outputs: 1,
            description: "session start time (frozen at init)" },

        // --- Noise ---
        FuncSig { name: "perlin_1d", wire_inputs: 1, const_params: &[("seed", true), ("frequency", true)], outputs: 1,
            description: "1D Perlin noise" },
        FuncSig { name: "perlin_2d", wire_inputs: 2, const_params: &[("seed", true), ("frequency", true)], outputs: 1,
            description: "2D Perlin noise" },
        FuncSig { name: "simplex_2d", wire_inputs: 2, const_params: &[("seed", true), ("frequency", true)], outputs: 1,
            description: "2D simplex noise" },

        // --- Regex ---
        FuncSig { name: "regex_replace", wire_inputs: 1, const_params: &[("pattern", true), ("replacement", true)], outputs: 1,
            description: "regex substitution" },
        FuncSig { name: "regex_match", wire_inputs: 1, const_params: &[("pattern", true)], outputs: 1,
            description: "test if string matches regex" },

        // --- Shuffle ---
        FuncSig { name: "shuffle", wire_inputs: 1, const_params: &[("min", false), ("size", true)], outputs: 1,
            description: "bijective LFSR permutation" },

        // --- Realer ---
        FuncSig { name: "first_names", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "Census first name (weighted)" },
        FuncSig { name: "full_names", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "full name (first + last)" },
        FuncSig { name: "state_codes", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "US state abbreviation" },
        FuncSig { name: "country_names", wire_inputs: 1, const_params: &[], outputs: 1,
            description: "country name" },
    ]
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
    // Leak the registry into static — this is called rarely and the
    // data is small. A proper approach would use lazy_static or OnceCell.
    let reg = registry();
    for sig in &reg {
        if sig.name == name {
            // SAFETY: the registry is built from static data and we
            // return a reference. We leak to get 'static lifetime.
            // This is fine for a DSL compiler that runs once.
            return Some(Box::leak(Box::new(sig.clone())));
        }
    }
    None
}

/// Simple Levenshtein edit distance for "did you mean?" suggestions.
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
    }

    #[test]
    fn lookup_missing() {
        assert!(lookup("nonexistent").is_none());
    }

    #[test]
    fn edit_distance_basic() {
        assert_eq!(edit_distance("hash", "hash"), 0);
        assert_eq!(edit_distance("hash", "hsh"), 1);
        assert_eq!(edit_distance("hash", "hahs"), 2);
        assert_eq!(edit_distance("", "abc"), 3);
    }

    #[test]
    fn registry_has_entries() {
        let reg = registry();
        assert!(reg.len() > 50, "registry should have 50+ functions");
    }
}
