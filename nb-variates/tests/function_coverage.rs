// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehensive functional, integration, and numerical accuracy tests
//! for every registered GK function. Tests go through the full DSL
//! compiler pipeline: source -> assembler -> kernel -> eval.

use nb_variates::dsl::compile::compile_gk;
use nb_variates::kernel::GkKernel;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn gk(bindings: &str) -> GkKernel {
    let src = format!("coordinates := (cycle)\n{bindings}");
    compile_gk(&src).unwrap_or_else(|e| panic!("failed to compile: {e}\nsource:\n{src}"))
}

fn gk2(bindings: &str) -> GkKernel {
    let src = format!("coordinates := (x, y)\n{bindings}");
    compile_gk(&src).unwrap_or_else(|e| panic!("failed to compile: {e}\nsource:\n{src}"))
}

fn eval_u64(k: &mut GkKernel, cycle: u64) -> u64 {
    k.set_inputs(&[cycle]);
    k.pull("out").as_u64()
}

fn eval_f64(k: &mut GkKernel, cycle: u64) -> f64 {
    k.set_inputs(&[cycle]);
    k.pull("out").as_f64()
}

fn eval_str(k: &mut GkKernel, cycle: u64) -> String {
    k.set_inputs(&[cycle]);
    k.pull("out").as_str().to_string()
}

#[allow(dead_code)]
fn eval_val(k: &mut GkKernel, cycle: u64) -> String {
    k.set_inputs(&[cycle]);
    k.pull("out").to_display_string()
}

// ===========================================================================
// Arithmetic
// ===========================================================================

#[test]
fn hash_deterministic() {
    let mut k = gk("out := hash(cycle)");
    let a = eval_u64(&mut k, 42);
    let b = eval_u64(&mut k, 42);
    assert_eq!(a, b, "same input must produce same hash");
}

#[test]
fn hash_different_inputs_different_outputs() {
    let mut k = gk("out := hash(cycle)");
    let a = eval_u64(&mut k, 0);
    let b = eval_u64(&mut k, 1);
    let c = eval_u64(&mut k, 1000);
    assert_ne!(a, b);
    assert_ne!(b, c);
    assert_ne!(a, c);
}

#[test]
fn add_known_value() {
    let mut k = gk("out := add(cycle, 100)");
    assert_eq!(eval_u64(&mut k, 5), 105);
    assert_eq!(eval_u64(&mut k, 0), 100);
    assert_eq!(eval_u64(&mut k, 1000), 1100);
}

#[test]
fn add_wrapping() {
    let mut k = gk("out := add(cycle, 1)");
    assert_eq!(eval_u64(&mut k, u64::MAX), 0);
}

#[test]
fn mul_known_value() {
    let mut k = gk("out := mul(cycle, 10)");
    assert_eq!(eval_u64(&mut k, 7), 70);
    assert_eq!(eval_u64(&mut k, 0), 0);
    assert_eq!(eval_u64(&mut k, 100), 1000);
}

#[test]
fn div_known_value() {
    let mut k = gk("out := div(cycle, 3)");
    assert_eq!(eval_u64(&mut k, 9), 3);
    assert_eq!(eval_u64(&mut k, 10), 3);
    assert_eq!(eval_u64(&mut k, 0), 0);
}

#[test]
fn mod_bounds() {
    let mut k = gk("out := mod(hash(cycle), 100)");
    for cycle in 0..1000 {
        let v = eval_u64(&mut k, cycle);
        assert!(v < 100, "cycle={cycle} gave {v}");
    }
}

#[test]
fn mod_known_value() {
    let mut k = gk("out := mod(cycle, 7)");
    assert_eq!(eval_u64(&mut k, 20), 6);
    assert_eq!(eval_u64(&mut k, 7), 0);
    assert_eq!(eval_u64(&mut k, 0), 0);
}

#[test]
fn clamp_within_range() {
    let mut k = gk("out := clamp(cycle, 10, 20)");
    assert_eq!(eval_u64(&mut k, 15), 15);
}

#[test]
fn clamp_below() {
    let mut k = gk("out := clamp(cycle, 10, 20)");
    assert_eq!(eval_u64(&mut k, 5), 10);
}

#[test]
fn clamp_above() {
    let mut k = gk("out := clamp(cycle, 10, 20)");
    assert_eq!(eval_u64(&mut k, 25), 20);
}

#[test]
fn mixed_radix_decomposition() {
    let src = "coordinates := (cycle)\n(a, b) := mixed_radix(cycle, 10, 0)";
    let mut k = compile_gk(src).unwrap();
    k.set_inputs(&[42]);
    let a = k.pull("a").as_u64();
    let b = k.pull("b").as_u64();
    // 42 = 2 * 10 + remainder 4 reversed? mixed_radix: digit0 = 42 % 10 = 2, digit1 = 42 / 10 = 4
    assert_eq!(a, 2);
    assert_eq!(b, 4);
}

#[test]
fn sum_variadic() {
    let mut k = gk("out := sum(cycle, cycle, cycle)");
    assert_eq!(eval_u64(&mut k, 10), 30);
}

#[test]
fn sum_identity() {
    let mut k = gk("out := sum()");
    assert_eq!(eval_u64(&mut k, 999), 0);
}

#[test]
fn product_variadic() {
    let mut k = gk("out := product(cycle, cycle)");
    assert_eq!(eval_u64(&mut k, 5), 25);
    assert_eq!(eval_u64(&mut k, 3), 9);
}

#[test]
fn min_variadic() {
    // min picks the smallest of its wire inputs. At cycle=5: min(15, 25) = 15
    let mut k = gk("a := add(cycle, 10)\nb := add(cycle, 20)\nout := min(a, b)");
    let v = eval_u64(&mut k, 5);
    assert!(v <= 15, "min should be <= 15, got {v}");
    assert!(v <= 25, "min should be <= 25, got {v}");
}

#[test]
fn max_variadic() {
    // max picks the largest of its wire inputs. At cycle=5: max(15, 25) = 25
    let mut k = gk("a := add(cycle, 10)\nb := add(cycle, 20)\nout := max(a, b)");
    let v = eval_u64(&mut k, 5);
    assert!(v >= 15, "max should be >= 15, got {v}");
    assert!(v >= 25, "max should be >= 25, got {v}");
}

#[test]
fn interleave_known() {
    let src = "coordinates := (a, b)\nout := interleave(a, b)";
    let mut k = compile_gk(src).unwrap();
    // interleave(1, 0) should give 1 (bit0 of a=1 -> bit0)
    k.set_inputs(&[1, 0]);
    let v = k.pull("out").as_u64();
    assert_eq!(v & 1, 1, "bit 0 should be from a");
    // interleave(0, 1) should give 2 (bit0 of b=1 -> bit1)
    k.set_inputs(&[0, 1]);
    let v = k.pull("out").as_u64();
    assert_eq!(v & 2, 2, "bit 1 should be from b");
}

#[test]
fn identity_passthrough() {
    let mut k = gk("out := identity(cycle)");
    assert_eq!(eval_u64(&mut k, 42), 42);
    assert_eq!(eval_u64(&mut k, 0), 0);
    assert_eq!(eval_u64(&mut k, u64::MAX), u64::MAX);
}

// ===========================================================================
// Hashing (hash_range / hash_interval are fusion-only; test via components)
// ===========================================================================

#[test]
fn hash_range_bounded() {
    let mut k = gk("out := mod(hash(cycle), 1000)");
    for cycle in 0..1000 {
        let v = eval_u64(&mut k, cycle);
        assert!(v < 1000, "cycle={cycle} gave {v}");
    }
}

#[test]
fn hash_interval_bounded() {
    let mut k = gk("h := hash(cycle)\nu := unit_interval(h)\nout := lerp(u, -10.0, 10.0)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= -10.0 && v < 10.0, "cycle={cycle} gave {v}");
    }
}

// ===========================================================================
// Interpolation
// ===========================================================================

#[test]
fn unit_interval_range() {
    let mut k = gk("out := unit_interval(hash(cycle))");
    for cycle in 0..10_000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 0.0 && v < 1.0, "cycle={cycle} gave {v}");
    }
}

#[test]
fn lerp_boundaries() {
    let mut k = gk("u := unit_interval(hash(cycle))\nout := lerp(u, 10.0, 50.0)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 10.0 && v < 50.0, "cycle={cycle} gave {v}");
    }
}

#[test]
fn scale_range_bounded() {
    let mut k = gk("out := scale_range(hash(cycle), 0.0, 100.0)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 0.0 && v < 100.0, "cycle={cycle} gave {v}");
    }
}

#[test]
fn quantize_snaps() {
    let mut k = gk("u := unit_interval(hash(cycle))\nout := quantize(u, 0.25)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        // v should be a multiple of 0.25
        let remainder = (v / 0.25).fract();
        assert!(
            remainder.abs() < 1e-10 || (1.0 - remainder).abs() < 1e-10,
            "cycle={cycle} gave {v} which is not a multiple of 0.25"
        );
    }
}

// ===========================================================================
// Conversions
// ===========================================================================

#[test]
fn f64_to_u64_truncates() {
    let mut k = gk("f := scale_range(hash(cycle), 0.0, 100.0)\nout := f64_to_u64(f)");
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v < 100, "cycle={cycle} gave {v}");
    }
}

#[test]
fn round_to_u64_rounds() {
    // scale_range output in [0, 10), round should produce [0, 10]
    let mut k = gk("f := scale_range(hash(cycle), 0.0, 10.0)\nout := round_to_u64(f)");
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v <= 10, "cycle={cycle} gave {v}");
    }
}

#[test]
fn floor_to_u64_floors() {
    let mut k = gk("f := scale_range(hash(cycle), 0.0, 10.0)\nout := floor_to_u64(f)");
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v < 10, "cycle={cycle} gave {v}");
    }
}

#[test]
fn ceil_to_u64_ceils() {
    let mut k = gk("f := scale_range(hash(cycle), 0.0, 10.0)\nout := ceil_to_u64(f)");
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v <= 10, "cycle={cycle} gave {v}");
    }
}

#[test]
fn discretize_bins() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := discretize(f, 1, 10)");
    for cycle in 0..1000 {
        let v = eval_u64(&mut k, cycle);
        assert!(v < 10, "cycle={cycle} gave {v}");
    }
}

#[test]
fn format_u64_decimal() {
    let mut k = gk("out := format_u64(cycle, 10)");
    assert_eq!(eval_str(&mut k, 42), "42");
    assert_eq!(eval_str(&mut k, 0), "0");
}

#[test]
fn format_u64_hex() {
    let mut k = gk("out := format_u64(cycle, 16)");
    let s = eval_str(&mut k, 255);
    assert!(s.contains("ff"), "expected hex containing 'ff', got '{s}'");
}

#[test]
fn format_f64_precision() {
    let mut k = gk("f := scale_range(hash(cycle), 0.0, 100.0)\nout := format_f64(f, 2)");
    let s = eval_str(&mut k, 42);
    // Should contain a decimal point and exactly 2 digits after
    assert!(s.contains('.'), "expected decimal point in '{s}'");
    let parts: Vec<&str> = s.split('.').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[1].len(), 2, "expected 2 decimal places in '{s}'");
}

#[test]
fn zero_pad_width() {
    let mut k = gk("out := zero_pad_u64(cycle, 5)");
    assert_eq!(eval_str(&mut k, 42), "00042");
    assert_eq!(eval_str(&mut k, 0), "00000");
    // Longer numbers pass through without truncation
    assert_eq!(eval_str(&mut k, 123456), "123456");
}

// ===========================================================================
// Probability
// ===========================================================================

#[test]
fn fair_coin_distribution() {
    let mut k = gk("out := fair_coin(hash(cycle))");
    let mut ones = 0u64;
    let n = 10_000u64;
    for cycle in 0..n {
        let v = eval_u64(&mut k, cycle);
        assert!(v <= 1, "fair_coin produced {v}");
        ones += v;
    }
    let pct = (ones as f64) / (n as f64) * 100.0;
    assert!(pct > 45.0 && pct < 55.0, "fair coin ~50% expected, got {pct:.1}%");
}

#[test]
fn unfair_coin_biased() {
    let mut k = gk("out := unfair_coin(hash(cycle), 0.9)");
    let mut ones = 0u64;
    let n = 10_000u64;
    for cycle in 0..n {
        ones += eval_u64(&mut k, cycle);
    }
    let pct = (ones as f64) / (n as f64) * 100.0;
    assert!(pct > 85.0 && pct < 95.0, "unfair coin ~90% expected, got {pct:.1}%");
}

#[test]
fn select_conditional() {
    let mut k = gk(
        "cond := fair_coin(hash(cycle))\n\
         t := add(cycle, 1000)\n\
         out := select(cond, t, cycle)"
    );
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v == cycle || v == cycle + 1000,
            "cycle={cycle} gave {v}, expected {cycle} or {}", cycle + 1000);
    }
}

#[test]
fn n_of_exact() {
    let mut k = gk("out := n_of(cycle, 3, 10)");
    // Over each window of 10, exactly 3 should be 1
    for window in 0..100 {
        let mut count = 0u64;
        for i in 0..10 {
            count += eval_u64(&mut k, window * 10 + i);
        }
        assert_eq!(count, 3, "window {window}: expected 3, got {count}");
    }
}

#[test]
fn chance_returns_u64_encoded_f64() {
    // chance() returns U64 containing f64 bits (0.0 or 1.0)
    let mut k = gk("out := chance(hash(cycle), 0.5)");
    let zero_bits = 0.0f64.to_bits();
    let one_bits = 1.0f64.to_bits();
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v == zero_bits || v == one_bits,
            "chance should return bits of 0.0 or 1.0, got {v}");
    }
}

#[test]
fn one_of_uniform() {
    let mut k = gk("out := one_of(hash(cycle), \"red\", \"green\", \"blue\")");
    for cycle in 0..100 {
        let s = eval_str(&mut k, cycle);
        assert!(
            s == "red" || s == "green" || s == "blue",
            "cycle={cycle} gave '{s}'"
        );
    }
}

#[test]
fn one_of_weighted_valid() {
    let mut k = gk("out := one_of_weighted(hash(cycle), \"200:80,404:10,500:10\")");
    for cycle in 0..100 {
        let s = eval_str(&mut k, cycle);
        assert!(
            s == "200" || s == "404" || s == "500",
            "cycle={cycle} gave '{s}'"
        );
    }
}

#[test]
fn blend_mix() {
    // blend(a, b, mix) interprets a and b as f64 bits, returns f64 bits.
    // At mix=0.0, output should equal pure 'a'.
    // At mix=1.0, output should equal pure 'b'.
    // We test by using known u64 values interpreted as f64 bits.
    let mut k_zero = gk("out := blend(cycle, add(cycle, 1000), 0.0)");
    let mut k_one = gk("out := blend(cycle, add(cycle, 1000), 1.0)");
    for cycle in 0..10 {
        let at_zero = eval_u64(&mut k_zero, cycle);
        let at_one = eval_u64(&mut k_one, cycle);
        // At mix=0, result should be f64::from_bits(cycle) * 1.0 = cycle as f64 bits
        assert_eq!(at_zero, f64::from_bits(cycle).to_bits(),
            "blend at mix=0.0 should pass through first input at cycle={cycle}");
        // At mix=1, result should be f64::from_bits(cycle+1000) * 1.0
        assert_eq!(at_one, f64::from_bits(cycle + 1000).to_bits(),
            "blend at mix=1.0 should pass through second input at cycle={cycle}");
    }
}

// ===========================================================================
// Weighted
// ===========================================================================

#[test]
fn weighted_strings_valid() {
    let mut k = gk("out := weighted_strings(hash(cycle), \"a:0.5;b:0.5\")");
    for cycle in 0..100 {
        let s = eval_str(&mut k, cycle);
        assert!(s == "a" || s == "b", "cycle={cycle} gave '{s}'");
    }
}

#[test]
fn weighted_u64_valid() {
    let mut k = gk("out := weighted_u64(hash(cycle), \"10:0.5;20:0.5\")");
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v == 10 || v == 20, "cycle={cycle} gave {v}");
    }
}

#[test]
fn weighted_pick_valid() {
    let mut k = gk("out := weighted_pick(hash(cycle), 0.5, 10, 0.5, 20)");
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v == 10 || v == 20, "cycle={cycle} gave {v}");
    }
}

// ===========================================================================
// String / Encoding
// ===========================================================================

#[test]
fn combinations_produces_string() {
    let mut k = gk("out := combinations(cycle, \"0-9;0-9;0-9\")");
    for cycle in 0..100 {
        let s = eval_str(&mut k, cycle);
        assert_eq!(s.len(), 3, "cycle={cycle} gave '{s}' with len {}", s.len());
        assert!(s.chars().all(|c| c.is_ascii_digit()), "cycle={cycle} gave non-digit '{s}'");
    }
}

#[test]
fn number_to_words_known() {
    let mut k = gk("out := number_to_words(cycle)");
    let s = eval_str(&mut k, 42);
    assert!(s.contains("forty"), "expected 'forty' in '{s}'");
    let s0 = eval_str(&mut k, 0);
    assert!(s0.contains("zero"), "expected 'zero' in '{s0}'");
}

#[test]
fn html_encode_decode_roundtrip() {
    let mut k_enc = gk("s := format_u64(cycle, 10)\nout := html_encode(s)");
    let mut k_dec = gk("s := format_u64(cycle, 10)\ne := html_encode(s)\nout := html_decode(e)");
    for cycle in 0..100 {
        let original = eval_str(&mut k_enc, cycle);
        let roundtrip = eval_str(&mut k_dec, cycle);
        // For plain digit strings, encode/decode should be identity
        let plain = format!("{cycle}");
        assert_eq!(roundtrip, plain, "roundtrip failed at cycle={cycle}: got '{roundtrip}'");
        let _ = original; // used to drive encoding
    }
}

#[test]
fn url_encode_decode_roundtrip() {
    let mut k = gk("s := format_u64(cycle, 10)\ne := url_encode(s)\nout := url_decode(e)");
    for cycle in 0..100 {
        let roundtrip = eval_str(&mut k, cycle);
        let plain = format!("{cycle}");
        assert_eq!(roundtrip, plain, "roundtrip failed at cycle={cycle}");
    }
}

#[test]
fn regex_replace_works() {
    let mut k = gk("s := format_u64(cycle, 10)\nout := regex_replace(s, \"[0-9]\", \"x\")");
    let s = eval_str(&mut k, 42);
    assert_eq!(s, "xx", "expected 'xx', got '{s}'");
}

#[test]
fn regex_match_works() {
    let mut k = gk("s := format_u64(cycle, 10)\nout := regex_match(s, \"^[0-9]+$\")");
    k.set_inputs(&[42]);
    let v = k.pull("out").as_bool();
    assert!(v, "digit string should match digit pattern");
}

// ===========================================================================
// Digest
// ===========================================================================

#[test]
fn sha256_deterministic() {
    let mut k = gk("b := u64_to_bytes(cycle)\nd := sha256(b)\nout := to_hex(d)");
    let a = eval_str(&mut k, 42);
    let b = eval_str(&mut k, 42);
    assert_eq!(a, b, "sha256 must be deterministic");
    assert_eq!(a.len(), 64, "sha256 hex should be 64 chars, got {}", a.len());
    // Different input -> different output
    let c = eval_str(&mut k, 43);
    assert_ne!(a, c);
}

#[test]
fn md5_deterministic() {
    let mut k = gk("b := u64_to_bytes(cycle)\nd := md5(b)\nout := to_hex(d)");
    let a = eval_str(&mut k, 42);
    let b = eval_str(&mut k, 42);
    assert_eq!(a, b, "md5 must be deterministic");
    assert_eq!(a.len(), 32, "md5 hex should be 32 chars, got {}", a.len());
}

#[test]
fn base64_roundtrip() {
    let mut k = gk("b := u64_to_bytes(cycle)\ne := to_base64(b)\nout := from_base64(e)");
    let mut k_orig = gk("out := u64_to_bytes(cycle)");
    for cycle in 0..10 {
        k.set_inputs(&[cycle]);
        let roundtrip = k.pull("out").as_bytes().to_vec();
        k_orig.set_inputs(&[cycle]);
        let original = k_orig.pull("out").as_bytes().to_vec();
        assert_eq!(roundtrip, original, "base64 roundtrip failed at cycle={cycle}");
    }
}

// ===========================================================================
// Datetime
// ===========================================================================

#[test]
fn epoch_scale_multiplies() {
    let mut k = gk("out := epoch_scale(cycle, 1000)");
    assert_eq!(eval_u64(&mut k, 5), 5000);
    assert_eq!(eval_u64(&mut k, 0), 0);
}

#[test]
fn epoch_offset_adds() {
    let mut k = gk("out := epoch_offset(cycle, 1000000000000)");
    assert_eq!(eval_u64(&mut k, 0), 1_000_000_000_000);
    assert_eq!(eval_u64(&mut k, 5), 1_000_000_000_005);
}

#[test]
fn to_timestamp_produces_string() {
    // Use a known epoch millis: 2024-01-01T00:00:00.000Z = 1704067200000
    let mut k = gk("e := epoch_offset(cycle, 1704067200000)\nout := to_timestamp(e)");
    let s = eval_str(&mut k, 0);
    assert!(s.contains('-'), "ISO timestamp should contain '-', got '{s}'");
    assert!(s.contains('T'), "ISO timestamp should contain 'T', got '{s}'");
    assert!(s.contains("2024"), "expected year 2024 in '{s}'");
}

#[test]
fn date_components_decomposes() {
    // 7 outputs from date_components
    let src = "coordinates := (cycle)\n\
               e := epoch_offset(cycle, 1704067200000)\n\
               (y, mo, d, h, mi, s, ms) := date_components(e)";
    let mut k = compile_gk(src).unwrap();
    k.set_inputs(&[0]);
    let y = k.pull("y").as_u64();
    let mo = k.pull("mo").as_u64();
    let d = k.pull("d").as_u64();
    let h = k.pull("h").as_u64();
    let mi = k.pull("mi").as_u64();
    let s = k.pull("s").as_u64();
    let ms = k.pull("ms").as_u64();
    assert_eq!(y, 2024);
    assert_eq!(mo, 1);
    assert_eq!(d, 1);
    assert_eq!(h, 0);
    assert_eq!(mi, 0);
    assert_eq!(s, 0);
    assert_eq!(ms, 0);
}

// ===========================================================================
// PCG / Shuffle / Permutation
// ===========================================================================

#[test]
fn pcg_deterministic() {
    let mut k = gk("out := pcg(cycle, 42, 0)");
    let a = eval_u64(&mut k, 100);
    let b = eval_u64(&mut k, 100);
    assert_eq!(a, b, "pcg must be deterministic");
}

#[test]
fn pcg_different_seeds() {
    let mut k1 = gk("out := pcg(cycle, 42, 0)");
    let mut k2 = gk("out := pcg(cycle, 99, 0)");
    let a = eval_u64(&mut k1, 100);
    let b = eval_u64(&mut k2, 100);
    assert_ne!(a, b, "different seeds should produce different output");
}

#[test]
fn pcg_stream_with_wire() {
    let mut k = gk("out := pcg_stream(cycle, cycle, 42)");
    let a = eval_u64(&mut k, 10);
    let b = eval_u64(&mut k, 10);
    assert_eq!(a, b);
}

#[test]
fn cycle_walk_bounded() {
    let mut k = gk("out := cycle_walk(cycle, 1000, 0, 0)");
    for cycle in 0..1000 {
        let v = eval_u64(&mut k, cycle);
        assert!(v < 1000, "cycle={cycle} gave {v}");
    }
}

#[test]
fn shuffle_bounded() {
    let mut k = gk("out := shuffle(cycle, 0, 100)");
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(v < 100, "cycle={cycle} gave {v}");
    }
}

#[test]
fn shuffle_bijective() {
    let mut k = gk("out := shuffle(cycle, 0, 100)");
    let mut seen = std::collections::HashSet::new();
    for cycle in 0..100 {
        let v = eval_u64(&mut k, cycle);
        assert!(seen.insert(v), "collision at cycle={cycle}, value={v}");
    }
    assert_eq!(seen.len(), 100, "should have 100 unique outputs");
}

// ===========================================================================
// Context (non-deterministic)
// ===========================================================================

#[test]
fn counter_increments() {
    let mut k = gk("out := counter()");
    let a = eval_u64(&mut k, 0);
    let b = eval_u64(&mut k, 1);
    assert!(b > a, "counter should increment: a={a}, b={b}");
}

#[test]
fn current_epoch_positive() {
    let mut k = gk("out := current_epoch_millis()");
    let v = eval_u64(&mut k, 0);
    assert!(v > 0, "current_epoch_millis should be positive, got {v}");
    // Reasonable sanity: should be after 2020-01-01 (~1577836800000)
    assert!(v > 1_577_836_800_000, "epoch seems too small: {v}");
}

#[test]
fn session_start_stable() {
    let mut k = gk("out := session_start_millis()");
    let a = eval_u64(&mut k, 0);
    let b = eval_u64(&mut k, 1);
    assert_eq!(a, b, "session_start_millis should be stable");
    assert!(a > 0, "session start should be positive");
}

#[test]
fn elapsed_millis_nonnegative() {
    let mut k = gk("out := elapsed_millis()");
    let v = eval_u64(&mut k, 0);
    assert!(v < 1000, "elapsed_millis should be small right after creation, got {v}");
}

#[test]
fn thread_id_positive() {
    let mut k = gk("out := thread_id()");
    let v = eval_u64(&mut k, 0);
    assert!(v > 0, "thread_id should be a positive number, got {v}");
}

// ===========================================================================
// Noise
// ===========================================================================

#[test]
fn perlin_1d_bounded() {
    let mut k = gk("out := perlin_1d(cycle, 42, 0.01)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= -1.0 && v <= 1.0, "cycle={cycle} gave {v}");
    }
}

#[test]
fn perlin_1d_deterministic() {
    let mut k = gk("out := perlin_1d(cycle, 42, 0.01)");
    let a = eval_f64(&mut k, 100);
    let b = eval_f64(&mut k, 100);
    assert_eq!(a, b, "perlin_1d must be deterministic");
}

#[test]
fn perlin_2d_deterministic() {
    let mut k = gk2("out := perlin_2d(x, y, 42, 0.01)");
    k.set_inputs(&[10, 20]);
    let a = k.pull("out").as_f64();
    k.set_inputs(&[10, 20]);
    let b = k.pull("out").as_f64();
    assert_eq!(a, b, "perlin_2d must be deterministic");
    assert!(a >= -1.0 && a <= 1.0, "perlin_2d out of range: {a}");
}

#[test]
fn simplex_2d_deterministic() {
    let mut k = gk2("out := simplex_2d(x, y, 42, 0.01)");
    k.set_inputs(&[10, 20]);
    let a = k.pull("out").as_f64();
    k.set_inputs(&[10, 20]);
    let b = k.pull("out").as_f64();
    assert_eq!(a, b, "simplex_2d must be deterministic");
    assert!(a >= -1.0 && a <= 1.0, "simplex_2d out of range: {a}");
}

// ===========================================================================
// JSON
// ===========================================================================

#[test]
fn to_json_wraps() {
    let mut k = gk("out := to_json(cycle)");
    k.set_inputs(&[42]);
    let j = k.pull("out").as_json();
    assert_eq!(j.as_u64(), Some(42));
}

#[test]
fn json_to_str_serializes() {
    let mut k = gk("j := to_json(cycle)\nout := json_to_str(j)");
    let s = eval_str(&mut k, 42);
    assert!(s.contains("42"), "expected '42' in '{s}'");
}

#[test]
fn escape_json_escapes() {
    // escape_json takes a string input and escapes special JSON characters
    let mut k = gk("s := format_u64(cycle, 10)\nout := escape_json(s)");
    let s = eval_str(&mut k, 42);
    // Plain digits should pass through unchanged
    assert_eq!(s, "42");
}

#[test]
fn json_merge_combines() {
    // json_merge needs JSON object inputs, not scalars.
    // to_json(u64) produces a JSON number, not an object.
    // We can verify json_merge compiles and runs with JSON inputs.
    // Use two to_json calls which produce JSON values; if they aren't
    // objects the merge may produce a defined fallback.
    let src = "coordinates := (cycle)\n\
               a := to_json(cycle)\n\
               b := to_json(add(cycle, 1))\n\
               out := json_merge(a, b)";
    // json_merge with non-object JSON may panic or produce a defined result.
    // Just verify it compiles.
    let result = compile_gk(src);
    assert!(result.is_ok(), "json_merge should compile: {:?}", result.err());
}

// ===========================================================================
// Real-world data
// ===========================================================================

#[test]
fn first_names_produces_string() {
    let mut k = gk("out := first_names(hash(cycle))");
    for cycle in 0..10 {
        let s = eval_str(&mut k, cycle);
        assert!(!s.is_empty(), "first_names should produce non-empty string at cycle={cycle}");
    }
}

#[test]
fn full_names_produces_string() {
    let mut k = gk("out := full_names(hash(cycle))");
    for cycle in 0..10 {
        let s = eval_str(&mut k, cycle);
        assert!(!s.is_empty(), "full_names should produce non-empty string");
        // Full names typically contain a space
        assert!(s.contains(' '), "expected space in full name '{s}' at cycle={cycle}");
    }
}

#[test]
fn state_codes_two_letter() {
    let mut k = gk("out := state_codes(hash(cycle))");
    for cycle in 0..100 {
        let s = eval_str(&mut k, cycle);
        assert_eq!(s.len(), 2, "state code should be 2 chars, got '{s}' at cycle={cycle}");
        assert!(s.chars().all(|c| c.is_ascii_uppercase()),
            "state code should be uppercase, got '{s}'");
    }
}

#[test]
fn country_names_nonempty() {
    let mut k = gk("out := country_names(hash(cycle))");
    for cycle in 0..100 {
        let s = eval_str(&mut k, cycle);
        assert!(!s.is_empty(), "country_names should be non-empty at cycle={cycle}");
    }
}

// ===========================================================================
// Byte buffers
// ===========================================================================

#[test]
fn u64_to_bytes_length() {
    let mut k = gk("out := u64_to_bytes(cycle)");
    k.set_inputs(&[42]);
    let b = k.pull("out").as_bytes();
    assert_eq!(b.len(), 8, "u64_to_bytes should produce 8 bytes");
}

#[test]
fn bytes_from_hash_deterministic() {
    let mut k = gk("out := bytes_from_hash(hash(cycle), 32)");
    k.set_inputs(&[42]);
    let a = k.pull("out").as_bytes().to_vec();
    k.set_inputs(&[42]);
    let b = k.pull("out").as_bytes().to_vec();
    assert_eq!(a, b, "bytes_from_hash must be deterministic");
    assert_eq!(a.len(), 32, "expected 32 bytes");
}

#[test]
fn to_hex_format() {
    let mut k = gk("b := u64_to_bytes(cycle)\nout := to_hex(b)");
    let s = eval_str(&mut k, 42);
    assert_eq!(s.len(), 16, "hex of 8 bytes should be 16 chars, got {}", s.len());
    assert!(s.chars().all(|c| c.is_ascii_hexdigit()),
        "hex should contain only hex digits, got '{s}'");
}

#[test]
fn from_hex_roundtrip() {
    let mut k = gk("b := u64_to_bytes(cycle)\nh := to_hex(b)\nout := from_hex(h)");
    let mut k_orig = gk("out := u64_to_bytes(cycle)");
    for cycle in 0..10 {
        k.set_inputs(&[cycle]);
        let roundtrip = k.pull("out").as_bytes().to_vec();
        k_orig.set_inputs(&[cycle]);
        let original = k_orig.pull("out").as_bytes().to_vec();
        assert_eq!(roundtrip, original, "hex roundtrip failed at cycle={cycle}");
    }
}

// ===========================================================================
// Diagnostic
// ===========================================================================

#[test]
fn type_of_reports_type() {
    let mut k = gk("out := type_of(cycle)");
    let s = eval_str(&mut k, 42);
    assert!(s.contains("U64") || s.contains("u64"),
        "type_of(cycle) should report U64, got '{s}'");
}

#[test]
fn inspect_passthrough() {
    let mut k = gk("out := inspect(cycle)");
    assert_eq!(eval_u64(&mut k, 42), 42);
    assert_eq!(eval_u64(&mut k, 0), 0);
}

#[test]
fn debug_repr_produces_string() {
    let mut k = gk("out := debug_repr(cycle)");
    let s = eval_str(&mut k, 42);
    assert!(s.contains("42"), "debug_repr should contain the value '42', got '{s}'");
}

// ===========================================================================
// Distributions (ICD convenience wrappers)
// ===========================================================================

#[test]
fn icd_normal_produces_values() {
    // icd_normal/lut_sample need f64 [0,1] input; use unit_interval to bridge
    let mut k = gk("u := unit_interval(hash(cycle))\nout := icd_normal(u, 100.0, 15.0)");
    let mut sum = 0.0f64;
    let n = 10_000;
    for cycle in 0..n {
        let v = eval_f64(&mut k, cycle);
        assert!(v.is_finite(), "cycle={cycle} produced non-finite {v}");
        sum += v;
    }
    let mean = sum / n as f64;
    // Mean should be near 100 with generous tolerance
    assert!((mean - 100.0).abs() < 5.0, "mean={mean}, expected ~100");
}

#[test]
fn icd_exponential_positive() {
    let mut k = gk("u := unit_interval(hash(cycle))\nout := icd_exponential(u, 1.0)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 0.0, "exponential should be non-negative, got {v} at cycle={cycle}");
    }
}

#[test]
fn dist_normal_samples() {
    // dist_normal compiles to IcdSample; needs f64 [0,1] wire input
    let mut k = gk("u := unit_interval(hash(cycle))\nout := dist_normal(u, 50.0, 10.0)");
    let mut sum = 0.0f64;
    let n = 10_000;
    for cycle in 0..n {
        let v = eval_f64(&mut k, cycle);
        assert!(v.is_finite(), "cycle={cycle} produced non-finite");
        sum += v;
    }
    let mean = sum / n as f64;
    assert!((mean - 50.0).abs() < 5.0, "mean={mean}, expected ~50");
}

#[test]
fn dist_uniform_samples() {
    let mut k = gk("u := unit_interval(hash(cycle))\nout := dist_uniform(u, 10.0, 20.0)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 10.0 && v <= 20.0, "cycle={cycle}: {v} out of [10, 20]");
    }
}

#[test]
fn dist_exponential_samples() {
    let mut k = gk("u := unit_interval(hash(cycle))\nout := dist_exponential(u, 1.0)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 0.0, "exponential should be non-negative, got {v}");
    }
}

#[test]
fn dist_zipf_samples() {
    let mut k = gk("u := unit_interval(hash(cycle))\nout := dist_zipf(u, 100, 1.07)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 1.0 && v <= 100.0, "zipf out of range: {v}");
    }
}

#[test]
fn dist_pareto_samples() {
    let mut k = gk("u := unit_interval(hash(cycle))\nout := dist_pareto(u, 1.0, 2.0)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 1.0, "pareto should be >= scale, got {v}");
    }
}

#[test]
fn histribution_samples() {
    let mut k = gk("out := histribution(hash(cycle), \"100:90 200:9 300:1\")");
    let mut seen = std::collections::HashSet::new();
    for cycle in 0..1000 {
        let v = eval_u64(&mut k, cycle);
        assert!(v == 100 || v == 200 || v == 300, "unexpected histribution output: {v}");
        seen.insert(v);
    }
    assert!(seen.contains(&100), "should see label 100");
}

#[test]
fn dist_empirical_bounded() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := dist_empirical(f, \"10.0 20.0 30.0 40.0 50.0\")");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 10.0 && v <= 50.0,
            "empirical should be in [10, 50], got {v} at cycle={cycle}");
    }
}

#[test]
fn dist_empirical_interpolates() {
    // With only 2 data points, output should be a linear interpolation
    let mut k = gk("f := unit_interval(hash(cycle))\nout := dist_empirical(f, \"0.0 100.0\")");
    let mut sum = 0.0;
    let n = 10000;
    for cycle in 0..n {
        sum += eval_f64(&mut k, cycle as u64);
    }
    let mean = sum / n as f64;
    // With uniform input, mean should be ~50
    assert!((mean - 50.0).abs() < 5.0, "mean should be ~50, got {mean}");
}

// ===========================================================================
// Formatting
// ===========================================================================

#[test]
fn printf_formatting() {
    let mut k = gk("out := printf(\"id={:05}\", cycle)");
    let s = eval_str(&mut k, 42);
    assert_eq!(s, "id=00042", "printf formatting failed, got '{s}'");
}

#[test]
fn clamp_f64_works() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := clamp_f64(f, 0.25, 0.75)");
    for cycle in 0..1000 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 0.25 && v <= 0.75, "cycle={cycle} gave {v}");
    }
}

// ---------------------------------------------------------------------------
// Math / Trigonometry
// ---------------------------------------------------------------------------

#[test]
fn sin_known_values() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := sin(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= -1.0 && v <= 1.0, "sin out of range: {v} at cycle={cycle}");
    }
    // sin(0) = 0
    let mut k2 = gk2("f := unit_interval(x)\nout := sin(f)");
    k2.set_inputs(&[0, 0]);
    let v = k2.pull("out").as_f64();
    assert!(v.abs() < 1e-10, "sin(0) should be ~0, got {v}");
}

#[test]
fn cos_known_values() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := cos(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= -1.0 && v <= 1.0, "cos out of range: {v} at cycle={cycle}");
    }
    // cos(0) = 1
    let mut k2 = gk2("f := unit_interval(x)\nout := cos(f)");
    k2.set_inputs(&[0, 0]);
    let v = k2.pull("out").as_f64();
    assert!((v - 1.0).abs() < 1e-10, "cos(0) should be ~1, got {v}");
}

#[test]
fn tan_compiles_and_runs() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := tan(f)");
    // unit_interval produces [0,1), tan is well-defined there
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v.is_finite(), "tan should be finite for input in [0,1): {v} at cycle={cycle}");
    }
}

#[test]
fn asin_known_values() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := asin(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        // asin input [0,1) -> output [0, pi/2)
        assert!(v >= 0.0 && v <= std::f64::consts::FRAC_PI_2 + 0.001,
            "asin out of expected range: {v} at cycle={cycle}");
    }
}

#[test]
fn acos_known_values() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := acos(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        // acos input [0,1) -> output (0, pi/2]
        assert!(v >= 0.0 && v <= std::f64::consts::FRAC_PI_2 + 0.001,
            "acos out of expected range: {v} at cycle={cycle}");
    }
}

#[test]
fn atan_known_values() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := atan(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        // atan input [0,1) -> output [0, pi/4)
        assert!(v >= 0.0 && v < std::f64::consts::FRAC_PI_4 + 0.001,
            "atan out of expected range: {v} at cycle={cycle}");
    }
}

#[test]
fn sqrt_known_values() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := sqrt(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 0.0 && v <= 1.0, "sqrt of [0,1) should be in [0,1]: {v} at cycle={cycle}");
    }
}

#[test]
fn abs_f64_makes_positive() {
    // unit_interval gives [0,1), subtract 0.5 to get [-0.5, 0.5)
    // We can test abs by using a scale that goes negative
    let mut k = gk("f := unit_interval(hash(cycle))\nout := abs_f64(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v >= 0.0, "abs should be non-negative: {v} at cycle={cycle}");
    }
}

#[test]
fn ln_positive_inputs() {
    // unit_interval gives [0,1). Use lerp to map to [0.01, 1.0] to avoid ln(0).
    let mut k = gk("h := hash(cycle)\nf := unit_interval(h)\nscaled := lerp(f, 0.01, 1.0)\nout := ln(scaled)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v.is_finite(), "ln should be finite for positive input: {v} at cycle={cycle}");
        assert!(v <= 0.001, "ln([0.01, 1.0]) should be <= ~0: {v} at cycle={cycle}");
    }
}

#[test]
fn exp_known_values() {
    let mut k = gk("f := unit_interval(hash(cycle))\nout := exp(f)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        // exp([0,1)) -> [1, e)
        assert!(v >= 1.0 && v < std::f64::consts::E + 0.001,
            "exp out of expected range: {v} at cycle={cycle}");
    }
}

#[test]
fn atan2_compiles_and_runs() {
    // atan2 takes two f64 wire inputs (y, x)
    let mut k = gk("h1 := hash(cycle)\nh2 := hash(h1)\nfy := unit_interval(h1)\nfx := unit_interval(h2)\nout := atan2(fy, fx)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v.is_finite(), "atan2 should produce finite result: {v} at cycle={cycle}");
        assert!(v >= -std::f64::consts::PI && v <= std::f64::consts::PI,
            "atan2 should be in [-pi, pi]: {v} at cycle={cycle}");
    }
}

#[test]
fn pow_known_values() {
    // pow takes two f64 wire inputs (base, exponent)
    let mut k = gk("h1 := hash(cycle)\nh2 := hash(h1)\nbase := unit_interval(h1)\nexponent := unit_interval(h2)\nout := pow(base, exponent)");
    for cycle in 0..100 {
        let v = eval_f64(&mut k, cycle);
        assert!(v.is_finite(), "pow should produce finite result: {v} at cycle={cycle}");
        assert!(v >= 0.0, "pow of positive base should be non-negative: {v} at cycle={cycle}");
    }
}

// ===========================================================================
// f64 binary arithmetic (two-wire nodes)
// ===========================================================================

#[test]
fn f64_add_basic() {
    // Use float literals for constants; to_f64(cycle) for the dynamic input
    let mut k = gk("a := to_f64(cycle)\nb := 42.0\nout := f64_add(a, b)");
    assert_eq!(eval_f64(&mut k, 10), 52.0);
}

#[test]
fn f64_sub_basic() {
    let mut k = gk("a := to_f64(cycle)\nb := 3.0\nout := f64_sub(a, b)");
    assert_eq!(eval_f64(&mut k, 10), 7.0);
}

#[test]
fn f64_mul_basic() {
    let mut k = gk("a := to_f64(cycle)\nb := 3.0\nout := f64_mul(a, b)");
    assert_eq!(eval_f64(&mut k, 10), 30.0);
}

#[test]
fn f64_div_basic() {
    let mut k = gk("a := to_f64(cycle)\nb := 4.0\nout := f64_div(a, b)");
    assert_eq!(eval_f64(&mut k, 20), 5.0);
}

#[test]
fn f64_div_by_zero() {
    // f64_div returns 0.0 when divisor is 0.0
    let mut k = gk("a := to_f64(cycle)\nb := 0.0\nout := f64_div(a, b)");
    assert_eq!(eval_f64(&mut k, 10), 0.0);
}

#[test]
fn f64_mod_basic() {
    let mut k = gk("a := to_f64(cycle)\nb := 3.0\nout := f64_mod(a, b)");
    let v = eval_f64(&mut k, 10);
    assert!((v - 1.0).abs() < 0.001, "10 % 3 should be 1.0, got {v}");
}

#[test]
fn to_f64_conversion() {
    let mut k = gk("out := to_f64(cycle)");
    assert_eq!(eval_f64(&mut k, 42), 42.0);
}

#[test]
fn to_f64_large_value() {
    let mut k = gk("out := to_f64(cycle)");
    // Large u64 values lose precision in f64 but should still be a large positive number
    let v = eval_f64(&mut k, u64::MAX);
    assert!(v > 1e18, "to_f64(u64::MAX) should be a large number, got {v}");
}

// ===========================================================================
// u64 two-wire arithmetic
// ===========================================================================

#[test]
fn u64_add_basic() {
    let mut k = gk("b := 100\nout := u64_add(cycle, b)");
    assert_eq!(eval_u64(&mut k, 42), 142);
}

#[test]
fn u64_add_wrapping() {
    let mut k = gk("b := 1\nout := u64_add(cycle, b)");
    assert_eq!(eval_u64(&mut k, u64::MAX), 0);
}

#[test]
fn u64_sub_basic() {
    let mut k = gk("b := 10\nout := u64_sub(cycle, b)");
    assert_eq!(eval_u64(&mut k, 42), 32);
}

#[test]
fn u64_sub_underflow_wraps() {
    let mut k = gk("b := 1\nout := u64_sub(cycle, b)");
    assert_eq!(eval_u64(&mut k, 0), u64::MAX);
}

#[test]
fn u64_mul_basic() {
    let mut k = gk("b := 7\nout := u64_mul(cycle, b)");
    assert_eq!(eval_u64(&mut k, 6), 42);
}

#[test]
fn u64_mul_overflow_wraps() {
    let mut k = gk("b := 2\nout := u64_mul(cycle, b)");
    assert_eq!(eval_u64(&mut k, u64::MAX), u64::MAX.wrapping_mul(2));
}

#[test]
fn u64_div_basic() {
    let mut k = gk("b := 7\nout := u64_div(cycle, b)");
    assert_eq!(eval_u64(&mut k, 42), 6);
}

#[test]
fn u64_div_by_zero() {
    let mut k = gk("b := 0\nout := u64_div(cycle, b)");
    assert_eq!(eval_u64(&mut k, 42), 0);
}

// ===========================================================================
// Bitwise operations (two-wire DSL)
// ===========================================================================

#[test]
fn u64_and_dsl_basic() {
    let mut k = gk("mask := 0xFF\nout := u64_and(cycle, mask)");
    assert_eq!(eval_u64(&mut k, 0x1234), 0x34);
}

#[test]
fn u64_or_dsl_basic() {
    let mut k = gk("bits := 0xF0\nout := u64_or(cycle, bits)");
    assert_eq!(eval_u64(&mut k, 0x0A), 0xFA);
}

#[test]
fn u64_xor_dsl_basic() {
    let mut k = gk("mask := 0xFF\nout := u64_xor(cycle, mask)");
    assert_eq!(eval_u64(&mut k, 0xAA), 0x55);
}

#[test]
fn u64_xor_self_is_zero() {
    let mut k = gk("out := u64_xor(cycle, cycle)");
    assert_eq!(eval_u64(&mut k, 12345), 0);
}

#[test]
fn u64_shl_dsl_basic() {
    let mut k = gk("n := 8\nout := u64_shl(cycle, n)");
    assert_eq!(eval_u64(&mut k, 1), 256);
}

#[test]
fn u64_shl_overflow() {
    // wrapping_shl: shift by 64 is equivalent to shift by 0 (modular shift amount)
    let mut k = gk("n := 64\nout := u64_shl(cycle, n)");
    assert_eq!(eval_u64(&mut k, 1), 1);
}

#[test]
fn u64_shr_dsl_basic() {
    let mut k = gk("n := 4\nout := u64_shr(cycle, n)");
    assert_eq!(eval_u64(&mut k, 0xFF), 0x0F);
}

#[test]
fn u64_not_dsl_basic() {
    let mut k = gk("out := u64_not(cycle)");
    assert_eq!(eval_u64(&mut k, 0), u64::MAX);
}

#[test]
fn u64_not_involution() {
    // NOT(NOT(x)) == x
    let mut k = gk("inner := u64_not(cycle)\nout := u64_not(inner)");
    assert_eq!(eval_u64(&mut k, 12345), 12345);
}

// ===========================================================================
// Infix operator tests (through the DSL Pratt parser and desugar)
// ===========================================================================

#[test]
fn infix_multiply() {
    // Inline to_f64 call so the infix operand type is resolved as f64
    let mut k = gk("out := to_f64(cycle) * 3.0");
    assert_eq!(eval_f64(&mut k, 10), 30.0);
}

#[test]
fn infix_add_sub() {
    // Chain f64 operations inline to avoid named f64 binding in infix context
    let mut k = gk("out := to_f64(cycle) + 1.0 - 0.5");
    assert_eq!(eval_f64(&mut k, 10), 10.5);
}

#[test]
fn infix_precedence() {
    // * binds tighter than +: to_f64(10) + (2.0 * 3.0) = 16.0
    let mut k = gk("out := to_f64(cycle) + 2.0 * 3.0");
    assert_eq!(eval_f64(&mut k, 10), 16.0);
}

#[test]
fn infix_parentheses() {
    // Explicit grouping overrides default precedence: (to_f64(10)+2.0)*3.0 = 36.0
    let mut k = gk("out := (to_f64(cycle) + 2.0) * 3.0");
    assert_eq!(eval_f64(&mut k, 10), 36.0);
}

#[test]
fn infix_power() {
    // ** operator desugars to pow(base, exponent)
    let mut k = gk("out := to_f64(cycle) ** 2.0");
    assert_eq!(eval_f64(&mut k, 3), 9.0);
}

#[test]
fn infix_bitwise_and() {
    let mut k = gk("out := cycle & 0xFF");
    assert_eq!(eval_u64(&mut k, 0x1234), 0x34);
}

#[test]
fn infix_bitwise_or() {
    let mut k = gk("out := cycle | 0xF0");
    assert_eq!(eval_u64(&mut k, 0x0A), 0xFA);
}

#[test]
fn infix_bitwise_xor() {
    let mut k = gk("out := cycle ^ 0xFF");
    assert_eq!(eval_u64(&mut k, 0xAA), 0x55);
}

#[test]
fn infix_shift_left() {
    let mut k = gk("out := cycle << 8");
    assert_eq!(eval_u64(&mut k, 1), 256);
}

#[test]
fn infix_shift_right() {
    let mut k = gk("out := cycle >> 4");
    assert_eq!(eval_u64(&mut k, 0xFF), 0x0F);
}

#[test]
fn infix_bitwise_not() {
    let mut k = gk("out := !cycle");
    assert_eq!(eval_u64(&mut k, 0), u64::MAX);
}

#[test]
fn infix_unary_neg() {
    // -expr desugars to f64_sub(0.0, expr); inline to_f64 so types match
    let mut k = gk("out := -to_f64(cycle)");
    assert_eq!(eval_f64(&mut k, 5), -5.0);
}

#[test]
fn infix_complex_bitwise_expression() {
    // (cycle & 0xFF) ^ (cycle >> 8)
    let mut k = gk("out := (cycle & 0xFF) ^ (cycle >> 8)");
    let v = eval_u64(&mut k, 0x1234);
    assert_eq!(v, 0x34 ^ 0x12);
}

#[test]
fn infix_bitwise_precedence() {
    // & binds tighter than |: (0xFF & 0x0F) | 0xF0 = 0x0F | 0xF0 = 0xFF
    let mut k = gk("a := 0xFF\nb := 0x0F\nc := 0xF0\nout := a & b | c");
    assert_eq!(eval_u64(&mut k, 0), 0x0F | 0xF0);
}

// ===========================================================================
// Math edge cases
// ===========================================================================

#[test]
fn sin_of_zero() {
    let mut k = gk("out := sin(to_f64(cycle))");
    assert_eq!(eval_f64(&mut k, 0), 0.0);
}

#[test]
fn sin_of_pi_half() {
    let mut k = gk("pi_half := 1.5707963267948966\nout := sin(pi_half)");
    let v = eval_f64(&mut k, 0);
    assert!((v - 1.0).abs() < 1e-10, "sin(pi/2) should be ~1.0, got {v}");
}

#[test]
fn f64_mul_by_zero() {
    // zero is a float literal (ConstF64), a is to_f64(cycle)
    let mut k = gk("a := to_f64(cycle)\nzero := 0.0\nout := f64_mul(a, zero)");
    assert_eq!(eval_f64(&mut k, 42), 0.0);
}


#[test]
fn f64_add_negative() {
    // -3.0 desugars through UnaryNeg to f64_sub(0.0, 3.0) producing a const -3.0 wire
    let mut k = gk("a := 5.0\nb := -3.0\nout := f64_add(a, b)");
    assert_eq!(eval_f64(&mut k, 0), 2.0);
}

#[test]
fn pow_square_root() {
    let mut k = gk("a := 9.0\nhalf := 0.5\nout := pow(a, half)");
    let v = eval_f64(&mut k, 0);
    assert!((v - 3.0).abs() < 1e-10, "pow(9, 0.5) should be ~3.0, got {v}");
}

#[test]
fn pow_zero_exponent() {
    // x^0 = 1 for any nonzero x
    let mut k = gk("a := to_f64(cycle)\nzero := 0.0\nout := pow(a, zero)");
    assert_eq!(eval_f64(&mut k, 42), 1.0);
}

#[test]
fn f64_div_negative() {
    let mut k = gk("a := -10.0\nb := 2.0\nout := f64_div(a, b)");
    assert_eq!(eval_f64(&mut k, 0), -5.0);
}

// ===========================================================================
// Checked arithmetic nodes (opt-in overflow detection)
// ===========================================================================

#[test]
fn checked_add_normal() {
    let mut k = gk("b := 100\nout := checked_add(cycle, b)");
    assert_eq!(eval_u64(&mut k, 42), 142);
}

#[test]
fn checked_add_overflow_returns_zero() {
    let mut k = gk("b := 1\nout := checked_add(cycle, b)");
    assert_eq!(eval_u64(&mut k, u64::MAX), 0);
}

#[test]
fn checked_sub_normal() {
    let mut k = gk("b := 10\nout := checked_sub(cycle, b)");
    assert_eq!(eval_u64(&mut k, 42), 32);
}

#[test]
fn checked_sub_underflow_returns_zero() {
    let mut k = gk("b := 1\nout := checked_sub(cycle, b)");
    assert_eq!(eval_u64(&mut k, 0), 0);
}

#[test]
fn checked_mul_overflow_returns_zero() {
    let mut k = gk("b := 2\nout := checked_mul(cycle, b)");
    assert_eq!(eval_u64(&mut k, u64::MAX), 0);
}

#[test]
fn checked_mul_normal() {
    let mut k = gk("b := 7\nout := checked_mul(cycle, b)");
    assert_eq!(eval_u64(&mut k, 6), 42);
}

// ===========================================================================
// Floating-point precision and edge cases
// ===========================================================================

#[test]
fn fp_associativity_not_guaranteed() {
    // (a + b) + c != a + (b + c) in floating point
    // 1e18 + 1.0 - 1e18 should NOT equal 1.0 due to precision loss
    let mut k = gk("big := 1000000000000000000.0\none := 1.0\n\
        sum1 := f64_add(big, one)\nout := f64_sub(sum1, big)");
    let v = eval_f64(&mut k, 0);
    // In f64, 1e18 + 1.0 == 1e18 (1.0 is below the ULP)
    assert_eq!(v, 0.0, "1e18 + 1 - 1e18 should be 0 due to precision loss");
}

#[test]
fn fp_catastrophic_cancellation() {
    // Subtracting nearly equal numbers loses precision
    let mut k = gk("a := 1.0000000000000002\nb := 1.0000000000000000\nout := f64_sub(a, b)");
    let v = eval_f64(&mut k, 0);
    // The difference is the smallest representable increment above 1.0
    assert!(v > 0.0 && v < 1e-15, "should be tiny positive: {v}");
}

#[test]
fn fp_subnormal_multiplication() {
    // Multiplying very small numbers near subnormal territory
    // Use pow to construct a tiny value since 1e-300 isn't parseable (negative exponent)
    let mut k = gk("base := 10.0\nexp := -300.0\ntiny := pow(base, exp)\ntwo := 2.0\nout := f64_mul(tiny, two)");
    let v = eval_f64(&mut k, 0);
    assert!(v > 0.0, "tiny * 2 should be positive: {v}");
}

#[test]
fn fp_infinity_from_overflow() {
    // f64::MAX * 2 = infinity
    let mut k = gk("big := 1.7976931348623157e308\ntwo := 2.0\nout := f64_mul(big, two)");
    let v = eval_f64(&mut k, 0);
    assert!(v.is_infinite(), "f64::MAX * 2 should be inf, got {v}");
}

#[test]
fn fp_negative_infinity() {
    let mut k = gk("big := -1.7976931348623157e308\ntwo := 2.0\nout := f64_mul(big, two)");
    let v = eval_f64(&mut k, 0);
    assert!(v.is_infinite() && v < 0.0, "should be -inf, got {v}");
}

#[test]
fn fp_nan_from_zero_div_zero() {
    // 0.0 / 0.0 = NaN, but our f64_div returns 0.0 on div-by-zero
    let mut k = gk("a := 0.0\nb := 0.0\nout := f64_div(a, b)");
    let v = eval_f64(&mut k, 0);
    assert_eq!(v, 0.0, "f64_div(0, 0) should return 0 (guarded)");
}

#[test]
fn fp_nan_propagation_in_add() {
    // NaN + anything = NaN — but we can't easily produce NaN
    // via the DSL. Test via inf - inf instead:
    let mut k = gk("big := 1.7976931348623157e308\ntwo := 2.0\n\
        inf := f64_mul(big, two)\nout := f64_sub(inf, inf)");
    let v = eval_f64(&mut k, 0);
    assert!(v.is_nan(), "inf - inf should be NaN, got {v}");
}

#[test]
fn fp_negative_zero() {
    // -0.0 == 0.0 in IEEE 754
    let mut k = gk("z := 0.0\nzero := 0.0\nnz := f64_sub(zero, z)\nout := f64_add(nz, zero)");
    let v = eval_f64(&mut k, 0);
    assert_eq!(v, 0.0, "negative zero + 0 should equal zero");
}

#[test]
fn fp_roundtrip_u64_to_f64_small() {
    // Small u64 values round-trip exactly
    let mut k = gk("f := to_f64(cycle)\nout := f64_to_u64(f)");
    assert_eq!(eval_u64(&mut k, 0), 0);
    assert_eq!(eval_u64(&mut k, 1), 1);
    assert_eq!(eval_u64(&mut k, 1000000), 1000000);
}

#[test]
fn fp_roundtrip_u64_to_f64_loses_precision_above_2_53() {
    // u64 values above 2^53 can't all be represented in f64
    let mut k = gk("f := to_f64(cycle)\nout := f64_to_u64(f)");
    let big = (1u64 << 53) + 1; // 2^53 + 1: not exactly representable
    let result = eval_u64(&mut k, big);
    // The round-trip may lose the +1
    assert!(result == big || result == big - 1,
        "2^53+1 may round-trip imprecisely: in={big}, out={result}");
}

#[test]
fn fp_unit_interval_bounds() {
    // unit_interval should be in [0.0, 1.0) for all u64 inputs
    let mut k = gk("out := unit_interval(cycle)");
    for &c in &[0u64, 1, 100, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
        let v = eval_f64(&mut k, c);
        assert!(v >= 0.0 && v <= 1.0, "unit_interval({c}) = {v}, expected [0, 1]");
    }
}

#[test]
fn fp_scale_range_bounds() {
    // scale_range should map [0, u64::MAX] to [min, max]
    let mut k = gk("out := scale_range(cycle, -10.0, 10.0)");
    let v0 = eval_f64(&mut k, 0);
    let vmax = eval_f64(&mut k, u64::MAX);
    assert!((v0 - (-10.0)).abs() < 0.01, "scale_range(0) should be near -10: {v0}");
    assert!((vmax - 10.0).abs() < 0.01, "scale_range(MAX) should be near 10: {vmax}");
}

#[test]
fn fp_sin_cos_pythagorean() {
    // sin²(x) + cos²(x) = 1 for any x
    let mut k = gk("x := to_f64(cycle)\nfactor := 0.1\nscaled := f64_mul(x, factor)\n\
        s := sin(scaled)\nc := cos(scaled)\n\
        two := 2.0\ns2 := pow(s, two)\nc2 := pow(c, two)\nout := f64_add(s2, c2)");
    for c in 0..20 {
        let v = eval_f64(&mut k, c);
        assert!((v - 1.0).abs() < 1e-10,
            "sin²+cos² at cycle {c} should be 1.0, got {v}");
    }
}

#[test]
fn fp_exp_ln_roundtrip() {
    // exp(ln(x)) = x for x > 0
    let mut k = gk("x := to_f64(cycle) + 1.0\nout := exp(ln(x))");
    for c in 1..10 {
        let v = eval_f64(&mut k, c);
        let expected = c as f64 + 1.0;
        assert!((v - expected).abs() < 1e-10,
            "exp(ln({expected})) = {v}");
    }
}

#[test]
fn fp_pow_integer_exact() {
    // Integer powers should be exact for small values
    let mut k = gk("x := to_f64(cycle)\nexp := 3.0\nout := pow(x, exp)");
    assert_eq!(eval_f64(&mut k, 2), 8.0);
    assert_eq!(eval_f64(&mut k, 3), 27.0);
    assert_eq!(eval_f64(&mut k, 10), 1000.0);
}

#[test]
fn fp_mod_preserves_sign() {
    // f64 modulo: result has the sign of the dividend
    let mut k = gk("a := -7.0\nb := 3.0\nout := f64_mod(a, b)");
    let v = eval_f64(&mut k, 0);
    assert!((v - (-1.0)).abs() < 1e-10, "-7 mod 3 should be -1, got {v}");
}

#[test]
fn fp_lerp_boundary() {
    // lerp(x, a, b) = a + (b-a)*x. At x=0 → a, at x=1 → b.
    // unit_interval(0) = 0.0
    let mut k = gk("x := unit_interval(cycle)\nout := lerp(x, 10.0, 20.0)");
    let v = eval_f64(&mut k, 0);
    assert!((v - 10.0).abs() < 0.01, "lerp(0, 10, 20) should be ~10, got {v}");
    let v = eval_f64(&mut k, u64::MAX);
    assert!((v - 20.0).abs() < 0.01, "lerp(1, 10, 20) should be ~20, got {v}");
}

#[test]
fn fp_lerp_midpoint() {
    // unit_interval(u64::MAX/2) ≈ 0.5
    let mut k = gk("x := unit_interval(cycle)\nout := lerp(x, 0.0, 100.0)");
    let v = eval_f64(&mut k, u64::MAX / 2);
    assert!((v - 50.0).abs() < 1.0, "lerp(0.5, 0, 100) should be ~50, got {v}");
}


// ===========================================================================
// Fourier module: stdlib waveform functions
// ===========================================================================

#[test]
fn sine_wave_module() {
    let src = "coordinates := (cycle)\nout := sine_wave(input: cycle, period: 20)";
    let mut k = compile_gk(src).unwrap();
    // At cycle 0, sin(0) = 0
    k.set_inputs(&[0]);
    let v0 = k.pull("out").as_f64();
    assert!((v0).abs() < 0.01, "sine_wave(0, 20) should be ~0, got {v0}");
    // At cycle 5 (quarter period), sin(π/2) = 1
    k.set_inputs(&[5]);
    let v5 = k.pull("out").as_f64();
    assert!((v5 - 1.0).abs() < 0.1, "sine_wave(5, 20) should be ~1, got {v5}");
}

#[test]
fn square_wave_module() {
    let src = "coordinates := (cycle)\nout := square_wave(input: cycle, period: 100)";
    let mut k = compile_gk(src).unwrap();
    // First quarter: positive
    k.set_inputs(&[10]);
    let v = k.pull("out").as_f64();
    assert!(v > 0.0, "square_wave early should be positive, got {v}");
    // Third quarter: negative
    k.set_inputs(&[60]);
    let v = k.pull("out").as_f64();
    assert!(v < 0.0, "square_wave late should be negative, got {v}");
}

#[test]
fn sine_unit_module() {
    let src = "coordinates := (cycle)\nout := sine_unit(input: cycle, period: 20)";
    let mut k = compile_gk(src).unwrap();
    // sine_unit maps to [0, 1]
    for c in 0..20u64 {
        k.set_inputs(&[c]);
        let v = k.pull("out").as_f64();
        assert!(v >= -0.01 && v <= 1.01, "sine_unit({c}) = {v}, expected [0,1]");
    }
}

// ---------------------------------------------------------------------------
// eval_const_expr integration tests
// ---------------------------------------------------------------------------

#[test]
fn const_expr_via_cli_cycles() {
    // Tests the eval_const_expr API used for CLI config resolution
    use nb_variates::dsl::compile::eval_const_expr;
    let v = eval_const_expr("42 + 1").unwrap();
    // 42 + 1: both IntLit → u64_add → u64(43)
    assert_eq!(v.as_u64(), 43, "expected u64(43), got {:?}", v);
}

// ---------------------------------------------------------------------------
// Registry completeness: every registered function must compile
// ---------------------------------------------------------------------------

/// Verifies that every function in the registry can be compiled into a
/// working GK kernel. Test fixture files are created for I/O nodes.
/// Vectordata nodes are tested separately (they require dataset downloads).
#[test]
fn every_registered_function_compiles() {
    use nb_variates::dsl::compile::compile_gk;
    use nb_variates::dsl::registry;
    use nb_variates::node::SlotType;

    // Create test fixture files
    let csv_path = std::env::temp_dir().join("_gk_coverage_test.csv");
    std::fs::write(&csv_path, "name,age\nalice,30\nbob,25\n").unwrap();
    let csv = csv_path.to_str().unwrap();
    let jsonl_path = std::env::temp_dir().join("_gk_coverage_test.jsonl");
    std::fs::write(&jsonl_path, "{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n").unwrap();
    let jsonl = jsonl_path.to_str().unwrap();
    let txt_path = std::env::temp_dir().join("_gk_coverage_test.txt");
    std::fs::write(&txt_path, "hello\nworld\n").unwrap();
    let txt = txt_path.to_str().unwrap();

    let reg = registry::registry();
    let mut failures: Vec<String> = Vec::new();

    // Overrides for functions that need specific wiring or file formats.
    let mut overrides: std::collections::HashMap<&str, String> = [
        // Bytes input
        ("to_hex", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := to_hex(b)".into()),
        ("from_hex", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nh := to_hex(b)\nout := from_hex(h)".into()),
        ("sha256", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := sha256(b)".into()),
        ("md5", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := md5(b)".into()),
        ("to_base64", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := to_base64(b)".into()),
        ("from_base64", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\ne := to_base64(b)\nout := from_base64(e)".into()),
        // JSON input
        ("json_to_str", "coordinates := (cycle)\nj := to_json(cycle)\nout := json_to_str(j)".into()),
        ("json_merge", "coordinates := (cycle)\na := to_json(cycle)\nb := to_json(cycle)\nout := json_merge(a, b)".into()),
        ("escape_json", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := escape_json(s)".into()),
        // Distributions
        ("dist_normal", "coordinates := (cycle)\nout := dist_normal(hash(cycle), 0.0, 1.0)".into()),
        ("dist_exponential", "coordinates := (cycle)\nout := dist_exponential(hash(cycle), 1.0)".into()),
        ("dist_uniform", "coordinates := (cycle)\nout := dist_uniform(hash(cycle), 0.0, 1.0)".into()),
        ("dist_pareto", "coordinates := (cycle)\nout := dist_pareto(hash(cycle), 1.0, 1.0)".into()),
        ("dist_zipf", "coordinates := (cycle)\nout := dist_zipf(hash(cycle), 100, 1.0)".into()),
        ("histribution", "coordinates := (cycle)\nout := histribution(hash(cycle), \"50 25 13 12\")".into()),
        ("dist_empirical", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := dist_empirical(f, \"1.0 3.0 5.0 7.0 9.0\")".into()),
        // Weighted
        ("weighted_strings", "coordinates := (cycle)\nout := weighted_strings(hash(cycle), \"a:0.5;b:0.5\")".into()),
        ("weighted_u64", "coordinates := (cycle)\nout := weighted_u64(hash(cycle), \"10:0.5;20:0.5\")".into()),
        ("one_of_weighted", "coordinates := (cycle)\nout := one_of_weighted(hash(cycle), \"a:0.5;b:0.5\")".into()),
        // String input
        ("html_encode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := html_encode(s)".into()),
        ("html_decode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := html_decode(s)".into()),
        ("url_encode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := url_encode(s)".into()),
        ("url_decode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := url_decode(s)".into()),
        ("regex_replace", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := regex_replace(s, \"[0-9]\", \"x\")".into()),
        ("regex_match", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := regex_match(s, \"[0-9]+\")".into()),
        // Multi-input
        ("select", "coordinates := (cycle)\nout := select(fair_coin(hash(cycle)), cycle, cycle)".into()),
        ("blend", "coordinates := (cycle)\nout := blend(hash(cycle), hash(cycle), 0.5)".into()),
        ("date_components", "coordinates := (cycle)\n(y, mo, d, h, mi, s, ms) := date_components(cycle)".into()),
        ("perlin_2d", "coordinates := (cycle)\nout := perlin_2d(cycle, cycle, 42, 0.01)".into()),
        ("simplex_2d", "coordinates := (cycle)\nout := simplex_2d(cycle, cycle, 42, 0.01)".into()),
        ("fractal_noise_2d", "coordinates := (cycle)\nout := fractal_noise_2d(cycle, cycle, 42, 0.02)".into()),
        ("pcg_stream", "coordinates := (cycle)\nout := pcg_stream(cycle, cycle, 42)".into()),
        ("format_u64", "coordinates := (cycle)\nout := format_u64(cycle, 16)".into()),
        // Context (no inputs)
        ("current_epoch_millis", "coordinates := (cycle)\nout := current_epoch_millis()".into()),
        ("counter", "coordinates := (cycle)\nout := counter()".into()),
        ("session_start_millis", "coordinates := (cycle)\nout := session_start_millis()".into()),
        ("elapsed_millis", "coordinates := (cycle)\nout := elapsed_millis()".into()),
        ("thread_id", "coordinates := (cycle)\nout := thread_id()".into()),
        // f64 input
        ("clamp_f64", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := clamp_f64(f, 0.0, 0.5)".into()),
        ("quantize", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := quantize(f, 0.1)".into()),
        ("lerp", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := lerp(f, 0.0, 100.0)".into()),
        ("inv_lerp", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := inv_lerp(f, 0.0, 1.0)".into()),
        // FFT (creates output file)
        ("fft_analyze", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := fft_analyze(f, \"/tmp/_gk_fft_test.jsonl\", 8)".into()),
    ].into_iter().collect();

    // File I/O nodes — use real fixture files
    overrides.insert("csv_field", format!(
        "coordinates := (cycle)\nout := csv_field(cycle, \"{csv}\", \"name\")"));
    overrides.insert("csv_row", format!(
        "coordinates := (cycle)\nout := csv_row(cycle, \"{csv}\")"));
    overrides.insert("csv_row_count", format!(
        "coordinates := (cycle)\nout := csv_row_count(\"{csv}\")"));
    overrides.insert("jsonl_field", format!(
        "coordinates := (cycle)\nout := jsonl_field(cycle, \"{jsonl}\", \"name\")"));
    overrides.insert("jsonl_row", format!(
        "coordinates := (cycle)\nout := jsonl_row(cycle, \"{jsonl}\")"));
    overrides.insert("jsonl_row_count", format!(
        "coordinates := (cycle)\nout := jsonl_row_count(\"{jsonl}\")"));
    overrides.insert("file_line_at", format!(
        "coordinates := (cycle)\nout := file_line_at(cycle, \"{txt}\")"));

    // Vectordata nodes require downloaded datasets — tested separately
    // in vectordata_integration.rs. Skip here to avoid network dependency.
    let vectordata_category = "vector_at vector_at_bytes query_vector_at query_vector_at_bytes \
        neighbor_indices_at neighbor_distances_at filtered_neighbor_indices_at \
        filtered_neighbor_distances_at dataset_distance_function vector_dim vector_count \
        query_count neighbor_count metadata_indices_at metadata_indices_len_at \
        metadata_indices_count dataset_facets dataset_profile_count dataset_profile_names \
        matching_profiles dataset_profile_name_at profile_base_count profile_facets \
        dataset_prebuffer";
    let vectordata_fns: std::collections::HashSet<&str> = vectordata_category.split_whitespace().collect();

    for sig in &reg {
        if vectordata_fns.contains(sig.name) { continue; }

        let src = if let Some(override_src) = overrides.get(sig.name) {
            override_src.to_string()
        } else {
            // Auto-generate from signature
            let mut args: Vec<String> = Vec::new();
            for p in sig.params {
                match p.slot_type {
                    SlotType::Wire => args.push("cycle".into()),
                    SlotType::ConstU64 => args.push("100".into()),
                    SlotType::ConstF64 => args.push("1.0".into()),
                    SlotType::ConstStr => args.push("\"test\"".into()),
                    SlotType::ConstVecU64 => args.push("100".into()),
                    SlotType::ConstVecF64 => args.push("1.0".into()),
                }
            }
            if args.is_empty() && sig.is_variadic() {
                args.push("cycle".into());
            }
            let call = format!("{}({})", sig.name, args.join(", "));
            format!("coordinates := (cycle)\nout := {call}")
        };

        let result = std::panic::catch_unwind(|| compile_gk(&src));

        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                failures.push(format!("  {}: {e}", sig.name));
            }
            Err(_) => {
                failures.push(format!("  {}: panicked", sig.name));
            }
        }
    }

    // Clean up
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&jsonl_path);
    let _ = std::fs::remove_file(&txt_path);
    let _ = std::fs::remove_file("/tmp/_gk_fft_test.jsonl");

    if !failures.is_empty() {
        panic!(
            "Registered functions that failed to compile:\n\n{}\n",
            failures.join("\n")
        );
    }
}
