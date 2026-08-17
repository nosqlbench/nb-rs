// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Probability modeling nodes.
//!
//! Deterministic building blocks for modeling probabilistic behavior in
//! Polydat graphs. All hash-based nodes are pure functions — "randomness"
//! comes from hashing the input, not from a stateful RNG. The same input
//! always produces the same output.
//!
//! Primary use cases: model adapter result kernels (simulated latency,
//! error injection, bimodal distributions), but usable anywhere in a
//! Polydat pipeline.
//!
//! SRD-80b Phase E migration: every probability node goes through
//! `#[polydat_node]`. `DefaultOr` rides the SRD-80b in-spirit rule
//! that PolyWire (`Value`-typed) args auto-emit
//! `accepts_none_inputs() -> true`, so the body's coalesce logic
//! sees `Value::None` instead of the kernel's Rule 1 short-circuit.
//!
//! `OneOf` rides the `Const<Vec<String>>` workload-list shape; its
//! non-empty-values check now fires at eval time rather than at
//! construction (the macro-emitted `new()` is infallible).
//! `OneOfWeighted` rides the `#[poly_const]` setup pattern, parsing
//! the spec once into a cached `WeightedTable`.

use crate::ast::Value;
#[cfg(test)]
use crate::ast::{PolydatNode, PortType};
use crate::derive_support::{Const, PolydatSetup};

/// Convert a u64 hash to a value in the unit interval [0.0, 1.0).
///
/// Uses the same method as `UnitInterval`: divide by (u64::MAX + 1) as f64.
#[inline]
fn hash_to_unit(v: u64) -> f64 {
    (v as f64) / ((u64::MAX as f64) + 1.0)
}

// ---------------------------------------------------------------------------
// FairCoin: 50/50 binary outcome from a hashed input.
// ---------------------------------------------------------------------------

/// Fair coin flip: returns 0 or 1 with 50/50 probability.
///
/// Signature: `fair_coin(input: u64) -> u64`
///
/// Equivalent to `mod(hash(input), 2)`. Use when you need a simple
/// binary decision with equal weight — for example, choosing between
/// two data centers or two code paths during workload modeling.
///
/// Deterministic: the same input always produces the same output.
///
/// JIT level: P2 — the macro auto-emits `compiled_u64` from the
/// scalar `u64 -> u64` body.
#[crate::polydat_node(category = Probability)]
fn fair_coin(input: u64) -> u64 {
    let h = crate::library::hash::splitmix64_u64(input);
    h & 1
}

// ---------------------------------------------------------------------------
// UnfairCoin: biased binary outcome from a hashed input.
// ---------------------------------------------------------------------------

/// Unfair coin flip: returns 1 with probability `p`, else 0.
///
/// Signature: `unfair_coin(input: u64, p: f64) -> u64`
///
/// The `p` parameter is an init-time constant in [0.0, 1.0]. The input
/// is hashed to a unit interval and compared against `p`: if the hashed
/// value is less than `p`, the output is 1; otherwise 0.
///
/// Use for modeling probabilistic events: error injection rates,
/// cache miss ratios, slow-path probability. Compose with `select()`
/// to branch on the outcome:
///
/// ```polydat
/// is_slow := unfair_coin(cycle, 0.1)
/// latency := select(is_slow, slow_latency, fast_latency)
/// ```
///
/// Unlike `n_of`, which guarantees exact counts over a window,
/// `unfair_coin` treats each input independently — over large
/// sample sizes the fraction converges to `p`, but any given
/// window may vary.
///
/// JIT level: P2 — macro-emitted compiled closure captures `p`.
#[crate::polydat_node(category = Probability)]
fn unfair_coin(input: u64, p: Const<f64>) -> u64 {
    if !(0.0..=1.0).contains(&*p) {
        panic!("unfair_coin probability p must be in [0.0, 1.0], got {}", *p);
    }
    let h = crate::library::hash::splitmix64_u64(input);
    let unit = hash_to_unit(h);
    if unit < *p { 1 } else { 0 }
}

// ---------------------------------------------------------------------------
// Select: 3-way conditional selection between two u64 values.
// ---------------------------------------------------------------------------

/// Binary conditional selection: returns `if_true` when `cond != 0`, else `if_false`.
///
/// Signature: `select(cond: u64, if_true: u64, if_false: u64) -> u64`
///
/// Three wire inputs. All inputs are always evaluated (no short-circuit)
/// because Polydat is a DAG, not a control flow graph. Use to pick between
/// two pre-computed alternatives based on a boolean signal:
///
/// ```polydat
/// latency := select(is_slow, slow_latency, fast_latency)
/// ```
///
/// Combine with `fair_coin`, `unfair_coin`, or `n_of` for the condition,
/// and any pair of compatible values for the branches.
///
/// JIT level: P3 — macro-emitted compiled closure is a branchless
/// conditional move when the LLVM optimiser folds the if.
#[crate::polydat_node(category = Probability)]
fn select(cond: u64, if_true: u64, if_false: u64) -> u64 {
    if cond != 0 { if_true } else { if_false }
}

// ---------------------------------------------------------------------------
// Chance: like UnfairCoin but the output is an f64-bit-encoded 0.0/1.0.
// ---------------------------------------------------------------------------

/// Probability chance returning f64-bits in u64 form: returns
/// `1.0_f64.to_bits()` with probability `p`, else `0.0_f64.to_bits()`.
///
/// Signature: `chance(input: u64, p: f64) -> u64`
///
/// Like `unfair_coin` but the u64 output carries the bit-pattern of
/// an f64 (0.0 or 1.0). Use when the result feeds directly into f64
/// arithmetic without an explicit type conversion step:
///
/// ```polydat
/// surcharge := mul(chance(cycle, 0.3), 0.05)
/// ```
///
/// The `p` parameter is an init-time constant in [0.0, 1.0].
///
/// JIT level: P2 — macro-emitted compiled closure captures `p`.
#[crate::polydat_node(category = Probability)]
fn chance(input: u64, p: Const<f64>) -> u64 {
    if !(0.0..=1.0).contains(&*p) {
        panic!("chance probability p must be in [0.0, 1.0], got {}", *p);
    }
    let h = crate::library::hash::splitmix64_u64(input);
    let unit = hash_to_unit(h);
    let result: f64 = if unit < *p { 1.0 } else { 0.0 };
    result.to_bits()
}

// ---------------------------------------------------------------------------
// NofM: deterministic exact-count selection (renamed operator surface
// stays `n_of`; struct emitted as `NOf`).
// ---------------------------------------------------------------------------

/// N-of-M deterministic fractional selection.
///
/// Signature: `n_of(input: u64, n: u64, m: u64) -> u64`
///
/// Returns 1 for exactly `n` out of every `m` consecutive inputs, 0
/// otherwise. Which specific inputs are selected within each window
/// is determined by hashing, so the pattern is not simply "first n".
///
/// This differs from `unfair_coin(input, n/m)`: unfair_coin is
/// probabilistic (each input independently has probability n/m),
/// while `n_of` guarantees exact counts over each window of m inputs.
///
/// Use for precise fraction control: exactly 3 out of every 10 cycles
/// are "special", exactly 1 out of every 100 is an error, etc.
///
/// ```polydat
/// is_special := n_of(cycle, 3, 10)
/// ```
///
/// Both `n` and `m` are init-time constant parameters. Panics if
/// `m == 0` or `n > m` (preserved from the Phase E migration —
/// the relational check can't ride on a per-param `ParamSpec`
/// constraint, so the assertion lives in the body and fires on
/// the first eval).
///
/// JIT level: P2 — macro-emitted compiled closure captures n and m.
#[crate::polydat_node(category = Probability)]
fn n_of(input: u64, n: Const<u64>, m: Const<u64>) -> u64 {
    if *m == 0 {
        panic!("n_of: m must be > 0");
    }
    if *n > *m {
        panic!("n_of: n ({}) must be <= m ({})", *n, *m);
    }
    n_of_m_eval(input, *n, *m)
}

/// Core n-of-m evaluation: hash the input's position within its window
/// and check whether its rank falls within the selected n.
///
/// Algorithm: within each window of m consecutive inputs, hash each
/// position (0..m) and sort by hash. The n positions with the smallest
/// hashes are selected. To avoid sorting at runtime, we count how many
/// of the m positions hash lower than the current one — if fewer than
/// n do, this position is selected.
#[inline]
fn n_of_m_eval(input: u64, n: u64, m: u64) -> u64 {
    let window = input / m;
    let pos = input % m;
    // Hash this position within the window using fast register mix
    let my_hash = crate::library::hash::splitmix64_u64(window.wrapping_mul(0x517cc1b727220a95) ^ pos.wrapping_mul(0x9e3779b97f4a7c15));
    // Count how many positions in the same window hash lower
    let mut rank: u64 = 0;
    for i in 0..m {
        if i == pos {
            continue;
        }
        let other_hash = crate::library::hash::splitmix64_u64(window.wrapping_mul(0x517cc1b727220a95) ^ i.wrapping_mul(0x9e3779b97f4a7c15));
        if other_hash < my_hash || (other_hash == my_hash && i < pos) {
            rank += 1;
        }
    }
    // Selected if rank < n (i.e., among the n smallest hashes)
    if rank < n { 1 } else { 0 }
}

// ---------------------------------------------------------------------------
// OneOf: uniform selection from a Const<Vec<String>> workload-list.
//
// SRD-80b Phase E: migrated via `Const<Vec<C>>`. The macro's
// VariadicConsts arity consumes every trailing string literal in
// the call site as the `values` vector, matching the pre-migration
// "all constants" call shape. Non-empty check stays in the body
// and fires on first eval; the workload-author-facing
// `validate_node` entry below trips at assembly time so a bad
// `one_of(cycle)` call never reaches eval.
// ---------------------------------------------------------------------------

/// Uniform selection from N constant string values.
///
/// Signature: `one_of(input: u64, values...) -> String`
///
/// Takes one wire input (u64) and N constant string values captured at
/// construction time. Hashes the input, takes mod N, and returns the
/// corresponding value. All values have equal probability.
///
/// Use for simple uniform selection when all outcomes are equally likely —
/// data center names, partition keys, categorical labels.
///
/// ```polydat
/// color := one_of(cycle, "red", "green", "blue")
/// ```
///
/// JIT level: P1 only — `Const<Vec<C>>` is JIT-ineligible by design
/// (per derive_support), so no compiled_u64 path.
#[crate::polydat_node(category = Probability)]
fn one_of(input: u64, values: Const<Vec<String>>) -> String {
    assert!(!values.is_empty(), "one_of: values must be non-empty");
    let h = crate::library::hash::splitmix64_u64(input);
    let idx = (h % values.len() as u64) as usize;
    values[idx].clone()
}

// ---------------------------------------------------------------------------
// OneOfWeighted: weighted selection driven by a parsed const spec.
//
// SRD-80b Phase E: migrated via `#[poly_const]` setup. The spec
// is parsed once at construction into a `WeightedTable`, then a
// borrow of the cached struct is handed to the eval body. Bad
// specs panic inside `parse_weighted_spec`, which the macro
// invokes from `OneOfWeighted::new`, preserving the
// construction-time panic contract.
// ---------------------------------------------------------------------------

/// Pre-parsed value table for `one_of_weighted`. The cumulative
/// vector is normalised so the last entry is exactly 1.0, letting
/// the eval body locate the matching bucket with a single binary
/// search.
pub struct WeightedTable {
    /// Output values in declaration order.
    pub values: Vec<String>,
    /// Cumulative weights, normalised to [0.0, 1.0]. The last
    /// entry is always 1.0.
    pub cumulative: Vec<f64>,
}

impl PolydatSetup for WeightedTable {}

impl WeightedTable {
    /// Single-call setup. The `#[polydat_node]` macro invokes
    /// this exactly once in the generated `OneOfWeighted::new()`.
    /// Panics on a malformed spec — same diagnostics as the
    /// pre-migration hand-written constructor.
    pub fn parse(spec: &str) -> Self {
        let mut values = Vec::new();
        let mut weights = Vec::new();
        for elem in spec.split([';', ',']) {
            let elem = elem.trim();
            if elem.is_empty() { continue; }
            let parts: Vec<&str> = elem.splitn(2, ':').collect();
            assert_eq!(parts.len(), 2, "one_of_weighted: expected 'value:weight', got '{elem}'");
            values.push(parts[0].to_string());
            let w: f64 = parts[1].parse().expect("one_of_weighted: invalid weight");
            assert!(w > 0.0, "one_of_weighted: weight must be positive, got {w}");
            weights.push(w);
        }
        assert!(!values.is_empty(), "one_of_weighted: spec must be non-empty");

        let total: f64 = weights.iter().sum();
        assert!(total > 0.0, "one_of_weighted: total weight must be > 0");

        let mut cumulative = Vec::with_capacity(weights.len());
        let mut running = 0.0;
        for w in &weights {
            running += w / total;
            cumulative.push(running);
        }
        // Clamp the last entry to exactly 1.0 to avoid floating-point edge cases.
        if let Some(last) = cumulative.last_mut() {
            *last = 1.0;
        }

        Self { values, cumulative }
    }
}

/// Weighted selection from a spec string, returning a String.
///
/// Signature: `one_of_weighted(input: u64, spec: &str) -> String`
///
/// The `spec` parameter is an init-time constant string with the format
/// `"value:weight,value:weight,..."`. Weights are positive numbers that
/// do not need to sum to any particular total — they are normalised
/// internally. Example: `"red:60,blue:30,green:10"`.
///
/// Implementation: at init time, weights are normalised to cumulative
/// proportions. At eval time, the input is hashed to the unit interval
/// and a binary search locates the matching bucket.
///
/// Use when outcomes have unequal probability — error codes with
/// realistic frequency distributions, region selection weighted by
/// traffic share, etc.
///
/// ```polydat
/// status := one_of_weighted(cycle, "200:80,404:10,500:5,503:5")
/// ```
///
/// JIT level: P1 only (String output prevents compiled_u64).
#[crate::polydat_node(category = Probability)]
fn one_of_weighted(
    input: u64,
    spec: Const<&str>,
    #[poly_const(WeightedTable::parse, from = spec)] table: &WeightedTable,
) -> String {
    let _ = spec;
    let h = crate::library::hash::splitmix64_u64(input);
    let unit = hash_to_unit(h);
    // Binary search: find the first cumulative entry >= unit.
    let idx = match table.cumulative.binary_search_by(|c| {
        c.partial_cmp(&unit).unwrap()
    }) {
        Ok(i) => i,
        Err(i) => i,
    };
    // Clamp to valid range (should not be needed, but defensive).
    let idx = idx.min(table.values.len() - 1);
    table.values[idx].clone()
}

// ---------------------------------------------------------------------------
// Blend: weighted linear blend of two f64 values carried as u64-bits.
// ---------------------------------------------------------------------------

/// Weighted linear blend of two f64 values.
///
/// Signature: `blend(a: u64, b: u64, mix: f64) -> u64`
///
/// Computes `a * (1.0 - mix) + b * mix` where `mix` is an init-time
/// constant in [0.0, 1.0]. Inputs `a` and `b` are f64 values carried
/// in the u64 buffer via `to_bits` / `from_bits`.
///
/// Use when you need to crossfade between two signal sources —
/// blending a fast-path latency model with a slow-path model,
/// interpolating between two noise generators, etc.
///
/// ```polydat
/// blended := blend(fast_latency, slow_latency, 0.3)
/// ```
///
/// JIT level: P2 — macro-emitted compiled closure captures `mix`.
#[crate::polydat_node(category = Probability)]
fn blend(a: u64, b: u64, mix: Const<f64>) -> u64 {
    if !(0.0..=1.0).contains(&*mix) {
        panic!("blend: mix must be in [0.0, 1.0], got {}", *mix);
    }
    let a_f = f64::from_bits(a);
    let b_f = f64::from_bits(b);
    let result = a_f * (1.0 - *mix) + b_f * *mix;
    result.to_bits()
}

// ---------------------------------------------------------------------------
// DefaultOr: None-aware coalesce. Migrated to `#[polydat_node]` via
// PolyWire (`Value`-typed) args — the macro auto-emits
// `accepts_none_inputs() -> true` because every PolyWire arg is
// inherently None-tolerant (None is one of the polymorphic variants).
// The return-type `Value` rides the `SameAsInput` output-type
// dispatch keyed off the first PolyWire arg (`value`), preserving
// the variant-preserving semantics: U64 in → U64 out, Str in → Str
// out, etc.
// ---------------------------------------------------------------------------

/// Returns the first input if it is not `None`, otherwise the second.
///
/// Signature: `default_or(value: Value, fallback: Value) -> Value`
///
/// This is the Polydat equivalent of SQL's `COALESCE` or Rust's
/// `Option::unwrap_or`. The node is polymorphic over the `Value`
/// variant — it passes whatever variant comes in (U64 / F64 / Bool /
/// Str / etc.) through unchanged. The output port type tracks the
/// first PolyWire arg's runtime port type via SRD-80b
/// `OutputType::SameAsInput`.
#[crate::polydat_node(category = Probability)]
fn default_or(value: Value, fallback: Value) -> Value {
    if matches!(value, Value::None) { fallback } else { value }
}

// `default_or` now self-registers via `#[polydat_node]`; the
// hand-written `signatures()` / `build_node()` / `register_nodes!`
// entries from the pre-Phase-E form have been removed.

#[cfg(test)]
mod tests {
    use super::*;

    // --- FairCoin ---

    #[test]
    fn fair_coin_returns_0_or_1() {
        let node = FairCoin::new();
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let v = out[0].as_u64();
            assert!(v == 0 || v == 1, "fair_coin({i}) returned {v}, expected 0 or 1");
        }
    }

    #[test]
    fn fair_coin_deterministic() {
        let node = FairCoin::new();
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(42)], &mut out1);
        node.eval(&[Value::U64(42)], &mut out2);
        assert_eq!(out1[0].as_u64(), out2[0].as_u64());
    }

    #[test]
    fn fair_coin_roughly_balanced() {
        let node = FairCoin::new();
        let mut out = [Value::None];
        let mut ones = 0u64;
        let n = 10_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(i)], &mut out);
            ones += out[0].as_u64();
        }
        // Expect roughly 50%, allow 45-55% range
        let ratio = ones as f64 / n as f64;
        assert!(
            (0.45..=0.55).contains(&ratio),
            "fair_coin ratio {ratio} outside expected 0.45-0.55"
        );
    }

    #[test]
    fn fair_coin_compiled_u64() {
        let node = FairCoin::new();
        let compiled = node.compiled_u64().expect("should have compiled_u64");
        let inputs = [42u64];
        let mut outputs = [0u64];
        compiled(&inputs, &mut outputs);
        assert!(outputs[0] == 0 || outputs[0] == 1);

        // Should match eval
        let mut eval_out = [Value::None];
        node.eval(&[Value::U64(42)], &mut eval_out);
        assert_eq!(outputs[0], eval_out[0].as_u64());
    }

    // --- UnfairCoin ---

    #[test]
    fn unfair_coin_always_0_when_p_is_0() {
        let node = UnfairCoin::new(0.0);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 0, "unfair_coin(p=0.0) should always return 0");
        }
    }

    #[test]
    fn unfair_coin_always_1_when_p_is_1() {
        let node = UnfairCoin::new(1.0);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 1, "unfair_coin(p=1.0) should always return 1");
        }
    }

    #[test]
    fn unfair_coin_respects_probability() {
        let node = UnfairCoin::new(0.2);
        let mut out = [Value::None];
        let mut ones = 0u64;
        let n = 10_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(i)], &mut out);
            ones += out[0].as_u64();
        }
        let ratio = ones as f64 / n as f64;
        assert!(
            (0.15..=0.25).contains(&ratio),
            "unfair_coin(p=0.2) ratio {ratio} outside expected 0.15-0.25"
        );
    }

    #[test]
    fn unfair_coin_compiled_u64() {
        let node = UnfairCoin::new(0.5);
        let compiled = node.compiled_u64().expect("should have compiled_u64");
        let inputs = [42u64];
        let mut outputs = [0u64];
        compiled(&inputs, &mut outputs);
        assert!(outputs[0] == 0 || outputs[0] == 1);

        let mut eval_out = [Value::None];
        node.eval(&[Value::U64(42)], &mut eval_out);
        assert_eq!(outputs[0], eval_out[0].as_u64());
    }

    #[test]
    #[should_panic(expected = "unfair_coin probability p must be in [0.0, 1.0]")]
    fn unfair_coin_rejects_invalid_p() {
        // SRD-80b Phase E: range assertion now fires on eval rather
        // than at construction (macro-emitted `new` is infallible).
        let node = UnfairCoin::new(1.5);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
    }

    // --- Select ---

    #[test]
    fn select_true_branch() {
        let node = Select::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(1), Value::U64(100), Value::U64(200)], &mut out);
        assert_eq!(out[0].as_u64(), 100);
    }

    #[test]
    fn select_false_branch() {
        let node = Select::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0), Value::U64(100), Value::U64(200)], &mut out);
        assert_eq!(out[0].as_u64(), 200);
    }

    #[test]
    fn select_nonzero_is_true() {
        let node = Select::new();
        let mut out = [Value::None];
        // Any nonzero value is truthy
        node.eval(&[Value::U64(999), Value::U64(10), Value::U64(20)], &mut out);
        assert_eq!(out[0].as_u64(), 10);
    }

    #[test]
    fn select_compiled_u64() {
        let node = Select::new();
        let compiled = node.compiled_u64().expect("should have compiled_u64");
        let mut outputs = [0u64];

        compiled(&[1, 100, 200], &mut outputs);
        assert_eq!(outputs[0], 100);

        compiled(&[0, 100, 200], &mut outputs);
        assert_eq!(outputs[0], 200);
    }

    // --- Chance ---

    #[test]
    fn chance_returns_f64_bits() {
        let node = Chance::new(0.5);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let bits = out[0].as_u64();
            let f = f64::from_bits(bits);
            assert!(
                f == 0.0 || f == 1.0,
                "chance({i}) returned f64 {f}, expected 0.0 or 1.0"
            );
        }
    }

    #[test]
    fn chance_always_0_when_p_is_0() {
        let node = Chance::new(0.0);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let f = f64::from_bits(out[0].as_u64());
            assert_eq!(f, 0.0);
        }
    }

    #[test]
    fn chance_always_1_when_p_is_1() {
        let node = Chance::new(1.0);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let f = f64::from_bits(out[0].as_u64());
            assert_eq!(f, 1.0);
        }
    }

    #[test]
    fn chance_compiled_u64() {
        let node = Chance::new(0.5);
        let compiled = node.compiled_u64().expect("should have compiled_u64");
        let inputs = [42u64];
        let mut outputs = [0u64];
        compiled(&inputs, &mut outputs);
        let f = f64::from_bits(outputs[0]);
        assert!(f == 0.0 || f == 1.0);

        let mut eval_out = [Value::None];
        node.eval(&[Value::U64(42)], &mut eval_out);
        assert_eq!(outputs[0], eval_out[0].as_u64());
    }

    // --- NofM (operator `n_of`, struct `NOf`) ---

    #[test]
    fn n_of_m_exact_count() {
        let node = NOf::new(3, 10);
        let mut out = [Value::None];
        // Check multiple windows
        for window in 0..10u64 {
            let mut count = 0u64;
            for pos in 0..10u64 {
                let input = window * 10 + pos;
                node.eval(&[Value::U64(input)], &mut out);
                count += out[0].as_u64();
            }
            assert_eq!(
                count, 3,
                "n_of(3, 10) window {window}: expected exactly 3 selected, got {count}"
            );
        }
    }

    #[test]
    fn n_of_m_all_selected() {
        let node = NOf::new(5, 5);
        let mut out = [Value::None];
        for i in 0..20u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 1, "n_of(5, 5) should always return 1");
        }
    }

    #[test]
    fn n_of_m_none_selected() {
        let node = NOf::new(0, 5);
        let mut out = [Value::None];
        for i in 0..20u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 0, "n_of(0, 5) should always return 0");
        }
    }

    #[test]
    fn n_of_m_deterministic() {
        let node = NOf::new(2, 7);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        for i in 0..50u64 {
            node.eval(&[Value::U64(i)], &mut out1);
            node.eval(&[Value::U64(i)], &mut out2);
            assert_eq!(out1[0].as_u64(), out2[0].as_u64());
        }
    }

    #[test]
    fn n_of_m_compiled_u64() {
        let node = NOf::new(3, 10);
        let compiled = node.compiled_u64().expect("should have compiled_u64");

        // Check that compiled matches eval for a full window
        for i in 0..10u64 {
            let mut c_out = [0u64];
            compiled(&[i], &mut c_out);

            let mut e_out = [Value::None];
            node.eval(&[Value::U64(i)], &mut e_out);

            assert_eq!(c_out[0], e_out[0].as_u64(), "compiled/eval mismatch at input {i}");
        }
    }

    #[test]
    #[should_panic(expected = "n_of: m must be > 0")]
    fn n_of_m_rejects_zero_m() {
        // SRD-80b Phase E: relational check fires on eval (macro-emitted
        // `new` is infallible; `validate_node` covers the assembly path).
        let node = NOf::new(0, 0);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
    }

    #[test]
    #[should_panic(expected = "n_of: n (5) must be <= m (3)")]
    fn n_of_m_rejects_n_greater_than_m() {
        let node = NOf::new(5, 3);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
    }

    #[test]
    fn n_of_m_not_first_n() {
        // Verify that the selected positions are shuffled, not just 0..n
        let node = NOf::new(1, 10);
        let mut out = [Value::None];
        let mut selected_positions = Vec::new();
        for window in 0..20u64 {
            for pos in 0..10u64 {
                let input = window * 10 + pos;
                node.eval(&[Value::U64(input)], &mut out);
                if out[0].as_u64() == 1 {
                    selected_positions.push(pos);
                }
            }
        }
        // With 20 windows and 1-of-10, we get 20 positions.
        // If they were all position 0, the set would be {0}.
        // With hashing, we should see multiple distinct positions.
        let unique: std::collections::HashSet<u64> = selected_positions.iter().copied().collect();
        assert!(
            unique.len() > 1,
            "n_of should select different positions across windows, got only {:?}",
            unique
        );
    }

    // --- OneOf ---

    #[test]
    fn one_of_selects_from_values() {
        let node = OneOf::new(vec!["alpha".into(), "beta".into(), "gamma".into()]);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let s = out[0].as_str().to_string();
            assert!(
                s == "alpha" || s == "beta" || s == "gamma",
                "one_of({i}) returned '{s}', expected one of alpha/beta/gamma"
            );
        }
    }

    #[test]
    fn one_of_deterministic() {
        let node = OneOf::new(vec!["x".into(), "y".into(), "z".into()]);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        for i in 0..50u64 {
            node.eval(&[Value::U64(i)], &mut out1);
            node.eval(&[Value::U64(i)], &mut out2);
            assert_eq!(out1[0].as_str(), out2[0].as_str());
        }
    }

    #[test]
    fn one_of_roughly_uniform() {
        let values = vec!["a".into(), "b".into(), "c".into()];
        let node = OneOf::new(values);
        let mut out = [Value::None];
        let mut counts = [0u64; 3];
        let n = 9_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(i)], &mut out);
            match out[0].as_str() {
                "a" => counts[0] += 1,
                "b" => counts[1] += 1,
                "c" => counts[2] += 1,
                other => panic!("unexpected value: {other}"),
            }
        }
        // Each should be roughly n/3 = 3000, allow 25-42% range
        for (idx, &c) in counts.iter().enumerate() {
            let ratio = c as f64 / n as f64;
            assert!(
                (0.25..=0.42).contains(&ratio),
                "one_of bucket {idx} ratio {ratio} outside expected 0.25-0.42"
            );
        }
    }

    #[test]
    fn one_of_single_value() {
        let node = OneOf::new(vec!["only".into()]);
        let mut out = [Value::None];
        for i in 0..20u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_str(), "only");
        }
    }

    #[test]
    #[should_panic(expected = "one_of: values must be non-empty")]
    fn one_of_rejects_empty() {
        // SRD-80b Phase E: non-empty check fires on eval (macro-emitted
        // `new` is infallible). Workload-author-facing assembly path
        // catches this earlier via the macro's VariadicConsts arity.
        let node = OneOf::new(vec![]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
    }

    // --- OneOfWeighted ---

    #[test]
    fn one_of_weighted_selects_from_spec() {
        let node = OneOfWeighted::new("red:60,blue:30,green:10".to_string());
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let s = out[0].as_str().to_string();
            assert!(
                s == "red" || s == "blue" || s == "green",
                "one_of_weighted({i}) returned '{s}'"
            );
        }
    }

    #[test]
    fn one_of_weighted_deterministic() {
        let node = OneOfWeighted::new("a:50,b:50".to_string());
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        for i in 0..50u64 {
            node.eval(&[Value::U64(i)], &mut out1);
            node.eval(&[Value::U64(i)], &mut out2);
            assert_eq!(out1[0].as_str(), out2[0].as_str());
        }
    }

    #[test]
    fn one_of_weighted_respects_weights() {
        let node = OneOfWeighted::new("heavy:90,light:10".to_string());
        let mut out = [Value::None];
        let mut heavy = 0u64;
        let n = 10_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(i)], &mut out);
            if out[0].as_str() == "heavy" {
                heavy += 1;
            }
        }
        let ratio = heavy as f64 / n as f64;
        // Expect ~90%, allow 80-97% range
        assert!(
            (0.80..=0.97).contains(&ratio),
            "one_of_weighted heavy ratio {ratio} outside expected 0.80-0.97"
        );
    }

    #[test]
    fn one_of_weighted_single_value() {
        let node = OneOfWeighted::new("only:1".to_string());
        let mut out = [Value::None];
        for i in 0..20u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_str(), "only");
        }
    }

    #[test]
    fn one_of_weighted_semicolon_delimiter() {
        let node = OneOfWeighted::new("x:50;y:50".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        let s = out[0].as_str().to_string();
        assert!(s == "x" || s == "y");
    }

    #[test]
    #[should_panic(expected = "one_of_weighted: spec must be non-empty")]
    fn one_of_weighted_rejects_empty() {
        OneOfWeighted::new("".to_string());
    }

    #[test]
    #[should_panic(expected = "one_of_weighted: expected 'value:weight'")]
    fn one_of_weighted_rejects_bad_format() {
        OneOfWeighted::new("noweight".to_string());
    }

    // --- Blend ---

    #[test]
    fn blend_pure_a_when_mix_is_0() {
        let node = Blend::new(0.0);
        let a: f64 = 10.0;
        let b: f64 = 20.0;
        let mut out = [Value::None];
        node.eval(
            &[Value::U64(a.to_bits()), Value::U64(b.to_bits())],
            &mut out,
        );
        let result = f64::from_bits(out[0].as_u64());
        assert!((result - 10.0).abs() < 1e-10, "blend(mix=0) should return a, got {result}");
    }

    #[test]
    fn blend_pure_b_when_mix_is_1() {
        let node = Blend::new(1.0);
        let a: f64 = 10.0;
        let b: f64 = 20.0;
        let mut out = [Value::None];
        node.eval(
            &[Value::U64(a.to_bits()), Value::U64(b.to_bits())],
            &mut out,
        );
        let result = f64::from_bits(out[0].as_u64());
        assert!((result - 20.0).abs() < 1e-10, "blend(mix=1) should return b, got {result}");
    }

    #[test]
    fn blend_half_mix() {
        let node = Blend::new(0.5);
        let a: f64 = 10.0;
        let b: f64 = 20.0;
        let mut out = [Value::None];
        node.eval(
            &[Value::U64(a.to_bits()), Value::U64(b.to_bits())],
            &mut out,
        );
        let result = f64::from_bits(out[0].as_u64());
        assert!(
            (result - 15.0).abs() < 1e-10,
            "blend(mix=0.5) of 10.0 and 20.0 should be 15.0, got {result}"
        );
    }

    #[test]
    fn blend_quarter_mix() {
        let node = Blend::new(0.25);
        let a: f64 = 0.0;
        let b: f64 = 100.0;
        let mut out = [Value::None];
        node.eval(
            &[Value::U64(a.to_bits()), Value::U64(b.to_bits())],
            &mut out,
        );
        let result = f64::from_bits(out[0].as_u64());
        assert!(
            (result - 25.0).abs() < 1e-10,
            "blend(mix=0.25) of 0.0 and 100.0 should be 25.0, got {result}"
        );
    }

    #[test]
    fn blend_compiled_u64() {
        let node = Blend::new(0.5);
        let compiled = node.compiled_u64().expect("should have compiled_u64");
        let a: f64 = 10.0;
        let b: f64 = 20.0;
        let inputs = [a.to_bits(), b.to_bits()];
        let mut outputs = [0u64];
        compiled(&inputs, &mut outputs);
        let result = f64::from_bits(outputs[0]);
        assert!((result - 15.0).abs() < 1e-10);

        // Should match eval
        let mut eval_out = [Value::None];
        node.eval(
            &[Value::U64(a.to_bits()), Value::U64(b.to_bits())],
            &mut eval_out,
        );
        assert_eq!(outputs[0], eval_out[0].as_u64());
    }

    #[test]
    #[should_panic(expected = "blend: mix must be in [0.0, 1.0]")]
    fn blend_rejects_invalid_mix() {
        // SRD-80b Phase E: range assertion fires on eval.
        let node = Blend::new(1.5);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0), Value::U64(0)], &mut out);
    }

    #[test]
    #[should_panic(expected = "blend: mix must be in [0.0, 1.0]")]
    fn blend_rejects_negative_mix() {
        let node = Blend::new(-0.1);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0), Value::U64(0)], &mut out);
    }

    // --- DefaultOr ---

    #[test]
    fn default_or_returns_value_when_not_none() {
        // Macro-emitted `new(value_type, fallback_type)` — PolyWire
        // args contribute a `<argname>_type: PortType` ctor param
        // each (SRD-80b).
        let node = DefaultOr::new(PortType::Str, PortType::Str);
        let mut out = [Value::None];
        node.eval(&[Value::Str("alice".into()), Value::Str("fallback".into())], &mut out);
        assert_eq!(out[0].as_str(), "alice");
    }

    #[test]
    fn default_or_returns_fallback_when_none() {
        let node = DefaultOr::new(PortType::Str, PortType::Str);
        let mut out = [Value::None];
        node.eval(&[Value::None, Value::Str("fallback".into())], &mut out);
        assert_eq!(out[0].as_str(), "fallback");
    }

    #[test]
    fn default_or_works_with_u64() {
        let node = DefaultOr::new(PortType::U64, PortType::U64);
        let mut out = [Value::None];
        // Non-None u64 passes through
        node.eval(&[Value::U64(42), Value::U64(0)], &mut out);
        assert!(matches!(out[0], Value::U64(42)));
        // None falls back
        node.eval(&[Value::None, Value::U64(99)], &mut out);
        assert!(matches!(out[0], Value::U64(99)));
    }

    #[test]
    fn default_or_with_extern_input() {
        // Full integration: build a Polydat program with an extern input,
        // wire through default_or, verify None→fallback and set→value.
        use crate::compile::assembly::{PolydatAssembler, WireRef};
        use crate::library::identity::PortPassthrough;

        let mut asm = PolydatAssembler::new(vec!["cycle".into()]);
        // Add an extern input (defaults to None)
        asm.add_input("captured_name", Value::None, PortType::Str, crate::kernel::InputKind::ExternalWrite);
        // Passthrough so the input is a node
        asm.add_node("__port_captured_name",
            Box::new(PortPassthrough::new("captured_name", PortType::Str)),
            vec![WireRef::input("captured_name")]);
        // Fallback constant
        asm.add_node("fallback",
            Box::new(crate::library::identity::ConstStr::new("anonymous".to_string())),
            vec![]);
        // default_or wired to extern input + fallback
        asm.add_node("greeting",
            Box::new(DefaultOr::new(PortType::Str, PortType::Str)),
            vec![WireRef::node("__port_captured_name"), WireRef::node("fallback")]);
        asm.add_output("greeting", WireRef::node("greeting"));

        let kernel = asm.compile().unwrap();
        let program = kernel.into_program();
        let mut state = program.create_state();

        // Before any capture: input is None → should get fallback
        state.set_inputs(&[0]);
        let val = state.pull(&program, "greeting");
        assert_eq!(val.to_display_string(), "anonymous",
            "unset extern should produce fallback, got: {:?}", val);

        // Set the capture input
        let input_idx = program.find_input("captured_name").unwrap();
        state.set_input(input_idx, Value::Str("alice".into()));
        let val = state.pull(&program, "greeting");
        assert_eq!(val.to_display_string(), "alice",
            "set extern should produce captured value, got: {:?}", val);

        // Reset captures → back to None → fallback again
        state.reset_inputs_from(program.coord_count());
        let val = state.pull(&program, "greeting");
        assert_eq!(val.to_display_string(), "anonymous",
            "reset extern should produce fallback again, got: {:?}", val);
    }
}
