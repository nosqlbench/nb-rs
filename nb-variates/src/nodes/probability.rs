// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Probability modeling nodes.
//!
//! Deterministic building blocks for modeling probabilistic behavior in
//! GK graphs. All nodes are pure functions — "randomness" comes from
//! hashing the input, not from a stateful RNG. The same input always
//! produces the same output.
//!
//! Primary use cases: model adapter result kernels (simulated latency,
//! error injection, bimodal distributions), but usable anywhere in a
//! GK pipeline.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Slot, Value};
use xxhash_rust::xxh3::xxh3_64;

/// Convert a u64 hash to a value in the unit interval [0.0, 1.0).
///
/// Uses the same method as `UnitInterval`: divide by (u64::MAX + 1) as f64.
#[inline]
fn hash_to_unit(v: u64) -> f64 {
    (v as f64) / ((u64::MAX as f64) + 1.0)
}

/// Fair coin flip: returns 0 or 1 with 50/50 probability.
///
/// Signature: `fair_coin(input: u64) -> (u64)`
///
/// Equivalent to `mod(hash(input), 2)`. Use when you need a simple
/// binary decision with equal weight — for example, choosing between
/// two data centers or two code paths during workload modeling.
///
/// Deterministic: the same input always produces the same output.
///
/// JIT level: P2 (compiled_u64 closure; xxh3 call prevents full inlining).
pub struct FairCoin {
    meta: NodeMeta,
}

impl Default for FairCoin {
    fn default() -> Self {
        Self::new()
    }
}

impl FairCoin {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "fair_coin".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for FairCoin {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let h = xxh3_64(&inputs[0].as_u64().to_le_bytes());
        outputs[0] = Value::U64(h % 2);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let h = xxh3_64(&inputs[0].to_le_bytes());
            outputs[0] = h % 2;
        }))
    }
}

/// Unfair coin flip: returns 1 with probability `p`, else 0.
///
/// Signature: `unfair_coin(input: u64, p: f64) -> (u64)`
///
/// The `p` parameter is an init-time constant in [0.0, 1.0]. The input
/// is hashed to a unit interval and compared against `p`: if the hashed
/// value is less than `p`, the output is 1; otherwise 0.
///
/// Use for modeling probabilistic events: error injection rates,
/// cache miss ratios, slow-path probability. Compose with `select()`
/// to branch on the outcome:
///
/// ```gk
/// is_slow := unfair_coin(cycle, 0.1)
/// latency := select(is_slow, slow_latency, fast_latency)
/// ```
///
/// Unlike `n_of`, which guarantees exact counts over a window,
/// `unfair_coin` treats each input independently — over large
/// sample sizes the fraction converges to `p`, but any given
/// window may vary.
///
/// JIT level: P2 (compiled_u64 closure).
pub struct UnfairCoin {
    meta: NodeMeta,
    p: f64,
}

impl UnfairCoin {
    pub fn new(p: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&p),
            "unfair_coin probability p must be in [0.0, 1.0], got {p}"
        );
        Self {
            meta: NodeMeta {
                name: "unfair_coin".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_f64("p", p),
                ],
            },
            p,
        }
    }
}

impl GkNode for UnfairCoin {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let h = xxh3_64(&inputs[0].as_u64().to_le_bytes());
        let unit = hash_to_unit(h);
        outputs[0] = Value::U64(if unit < self.p { 1 } else { 0 });
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let p = self.p;
        Some(Box::new(move |inputs, outputs| {
            let h = xxh3_64(&inputs[0].to_le_bytes());
            let unit = hash_to_unit(h);
            outputs[0] = if unit < p { 1 } else { 0 };
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.p.to_bits()] }
}

/// Binary conditional selection: returns `if_true` when `cond != 0`, else `if_false`.
///
/// Signature: `select(cond: u64, if_true: u64, if_false: u64) -> (u64)`
///
/// Three wire inputs. All inputs are always evaluated (no short-circuit)
/// because GK is a DAG, not a control flow graph. Use to pick between
/// two pre-computed alternatives based on a boolean signal:
///
/// ```gk
/// latency := select(is_slow, slow_latency, fast_latency)
/// ```
///
/// Combine with `fair_coin`, `unfair_coin`, or `n_of` for the condition,
/// and any pair of compatible values for the branches.
///
/// JIT level: P3 (branchless conditional move).
pub struct Select {
    meta: NodeMeta,
}

impl Default for Select {
    fn default() -> Self {
        Self::new()
    }
}

impl Select {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "select".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("cond")),
                    Slot::Wire(Port::u64("if_true")),
                    Slot::Wire(Port::u64("if_false")),
                ],
            },
        }
    }
}

impl GkNode for Select {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let cond = inputs[0].as_u64();
        outputs[0] = if cond != 0 {
            Value::U64(inputs[1].as_u64())
        } else {
            Value::U64(inputs[2].as_u64())
        };
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = if inputs[0] != 0 { inputs[1] } else { inputs[2] };
        }))
    }
}

/// Probability chance returning f64: returns 1.0 with probability `p`, else 0.0.
///
/// Signature: `chance(input: u64, p: f64) -> (u64)`
///
/// Like `unfair_coin` but returns 0.0 or 1.0 as f64 (bit-encoded in the
/// u64 output buffer). Use when the result feeds directly into f64
/// arithmetic without an explicit type conversion step:
///
/// ```gk
/// surcharge := mul(chance(cycle, 0.3), 0.05)
/// ```
///
/// The `p` parameter is an init-time constant in [0.0, 1.0].
///
/// JIT level: P2 (compiled_u64 closure).
pub struct Chance {
    meta: NodeMeta,
    p: f64,
}

impl Chance {
    pub fn new(p: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&p),
            "chance probability p must be in [0.0, 1.0], got {p}"
        );
        Self {
            meta: NodeMeta {
                name: "chance".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_f64("p", p),
                ],
            },
            p,
        }
    }
}

impl GkNode for Chance {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let h = xxh3_64(&inputs[0].as_u64().to_le_bytes());
        let unit = hash_to_unit(h);
        let result: f64 = if unit < self.p { 1.0 } else { 0.0 };
        outputs[0] = Value::U64(result.to_bits());
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let p = self.p;
        Some(Box::new(move |inputs, outputs| {
            let h = xxh3_64(&inputs[0].to_le_bytes());
            let unit = hash_to_unit(h);
            let result: f64 = if unit < p { 1.0 } else { 0.0 };
            outputs[0] = result.to_bits();
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.p.to_bits()] }
}

/// N-of-M deterministic fractional selection.
///
/// Signature: `n_of(input: u64, n: u64, m: u64) -> (u64)`
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
/// ```gk
/// is_special := n_of(cycle, 3, 10)
/// ```
///
/// Both `n` and `m` are init-time constant parameters. Panics if
/// `m == 0` or `n > m`.
///
/// JIT level: P2 (compiled_u64 closure).
pub struct NofM {
    meta: NodeMeta,
    n: u64,
    m: u64,
}

impl NofM {
    pub fn new(n: u64, m: u64) -> Self {
        assert!(m > 0, "n_of: m must be > 0");
        assert!(n <= m, "n_of: n ({n}) must be <= m ({m})");
        Self {
            meta: NodeMeta {
                name: "n_of".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("n", n),
                    Slot::const_u64("m", m),
                ],
            },
            n,
            m,
        }
    }
}

impl GkNode for NofM {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let input = inputs[0].as_u64();
        outputs[0] = Value::U64(n_of_m_eval(input, self.n, self.m));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let n = self.n;
        let m = self.m;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = n_of_m_eval(inputs[0], n, m);
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.n, self.m] }
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
    // Hash this position within the window
    let my_hash = xxh3_64(&[window.to_le_bytes(), pos.to_le_bytes()].concat());
    // Count how many positions in the same window hash lower
    let mut rank: u64 = 0;
    for i in 0..m {
        if i == pos {
            continue;
        }
        let other_hash = xxh3_64(&[window.to_le_bytes(), i.to_le_bytes()].concat());
        if other_hash < my_hash || (other_hash == my_hash && i < pos) {
            rank += 1;
        }
    }
    // Selected if rank < n (i.e., among the n smallest hashes)
    if rank < n { 1 } else { 0 }
}

/// Uniform selection from N constant string values.
///
/// Signature: `one_of(input: u64, values...) -> (String)`
///
/// Takes one wire input (u64) and N constant string values captured at
/// construction time. Hashes the input, takes mod N, and returns the
/// corresponding value. All values have equal probability.
///
/// Use for simple uniform selection when all outcomes are equally likely —
/// data center names, partition keys, categorical labels.
///
/// ```gk
/// color := one_of(cycle, "red", "green", "blue")
/// ```
///
/// JIT level: P1 only (String output prevents compiled_u64).
pub struct OneOf {
    meta: NodeMeta,
    values: Vec<String>,
}

impl OneOf {
    /// Create a new `OneOf` node with the given constant values.
    ///
    /// Panics if `values` is empty.
    pub fn new(values: Vec<String>) -> Self {
        assert!(!values.is_empty(), "one_of: values must be non-empty");
        Self {
            meta: NodeMeta {
                name: "one_of".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            values,
        }
    }
}

impl GkNode for OneOf {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let h = xxh3_64(&inputs[0].as_u64().to_le_bytes());
        let idx = (h % self.values.len() as u64) as usize;
        outputs[0] = Value::Str(self.values[idx].clone());
    }
}

/// Weighted selection from a spec string, returning a String.
///
/// Signature: `one_of_weighted(input: u64) -> (String)`
///
/// The `spec` parameter is an init-time constant string with the format
/// `"value:weight,value:weight,..."`. Weights are positive numbers that
/// do not need to sum to any particular total — they are normalized
/// internally. Example: `"red:60,blue:30,green:10"`.
///
/// Implementation: at init time, weights are normalized to cumulative
/// proportions. At eval time, the input is hashed to the unit interval
/// and a binary search locates the matching bucket.
///
/// Use when outcomes have unequal probability — error codes with
/// realistic frequency distributions, region selection weighted by
/// traffic share, etc.
///
/// ```gk
/// status := one_of_weighted(cycle, "200:80,404:10,500:5,503:5")
/// ```
///
/// JIT level: P1 only (String output prevents compiled_u64).
pub struct OneOfWeighted {
    meta: NodeMeta,
    values: Vec<String>,
    /// Cumulative weights, normalized to [0.0, 1.0]. The last entry is
    /// always 1.0.
    cumulative: Vec<f64>,
}

impl OneOfWeighted {
    /// Create a new `OneOfWeighted` node from a spec string.
    ///
    /// Spec format: `"value:weight,value:weight,..."` where weights are
    /// positive numbers. Delimiter can be `,` or `;`.
    ///
    /// Panics if the spec is empty, any weight is non-positive, or the
    /// total weight is zero.
    pub fn new(spec: &str) -> Self {
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

        Self {
            meta: NodeMeta {
                name: "one_of_weighted".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            values,
            cumulative,
        }
    }
}

impl GkNode for OneOfWeighted {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let h = xxh3_64(&inputs[0].as_u64().to_le_bytes());
        let unit = hash_to_unit(h);
        // Binary search: find the first cumulative entry >= unit.
        let idx = match self.cumulative.binary_search_by(|c| {
            c.partial_cmp(&unit).unwrap()
        }) {
            Ok(i) => i,
            Err(i) => i,
        };
        // Clamp to valid range (should not be needed, but defensive).
        let idx = idx.min(self.values.len() - 1);
        outputs[0] = Value::Str(self.values[idx].clone());
    }
}

/// Weighted linear blend of two f64 values.
///
/// Signature: `blend(a: u64, b: u64) -> (u64)`
///
/// Computes `a * (1.0 - mix) + b * mix` where `mix` is an init-time
/// constant in [0.0, 1.0]. Inputs `a` and `b` are f64 values carried
/// in the u64 buffer via `to_bits`/`from_bits`.
///
/// Use when you need to crossfade between two signal sources —
/// blending a fast-path latency model with a slow-path model,
/// interpolating between two noise generators, etc.
///
/// ```gk
/// blended := blend(fast_latency, slow_latency, 0.3)
/// ```
///
/// JIT level: P2 (compiled_u64 closure).
pub struct Blend {
    meta: NodeMeta,
    mix: f64,
}

impl Blend {
    /// Create a new `Blend` node with the given mix factor.
    ///
    /// `mix` must be in [0.0, 1.0]. A mix of 0.0 outputs pure `a`,
    /// a mix of 1.0 outputs pure `b`.
    pub fn new(mix: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&mix),
            "blend: mix must be in [0.0, 1.0], got {mix}"
        );
        Self {
            meta: NodeMeta {
                name: "blend".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("a")),
                    Slot::Wire(Port::u64("b")),
                    Slot::const_f64("mix", mix),
                ],
            },
            mix,
        }
    }
}

impl GkNode for Blend {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let a = f64::from_bits(inputs[0].as_u64());
        let b = f64::from_bits(inputs[1].as_u64());
        let result = a * (1.0 - self.mix) + b * self.mix;
        outputs[0] = Value::U64(result.to_bits());
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let mix = self.mix;
        Some(Box::new(move |inputs, outputs| {
            let a = f64::from_bits(inputs[0]);
            let b = f64::from_bits(inputs[1]);
            let result = a * (1.0 - mix) + b * mix;
            outputs[0] = result.to_bits();
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.mix.to_bits()] }
}

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
        UnfairCoin::new(1.5);
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

    // --- NofM ---

    #[test]
    fn n_of_m_exact_count() {
        let node = NofM::new(3, 10);
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
        let node = NofM::new(5, 5);
        let mut out = [Value::None];
        for i in 0..20u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 1, "n_of(5, 5) should always return 1");
        }
    }

    #[test]
    fn n_of_m_none_selected() {
        let node = NofM::new(0, 5);
        let mut out = [Value::None];
        for i in 0..20u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 0, "n_of(0, 5) should always return 0");
        }
    }

    #[test]
    fn n_of_m_deterministic() {
        let node = NofM::new(2, 7);
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
        let node = NofM::new(3, 10);
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
        NofM::new(0, 0);
    }

    #[test]
    #[should_panic(expected = "n_of: n (5) must be <= m (3)")]
    fn n_of_m_rejects_n_greater_than_m() {
        NofM::new(5, 3);
    }

    #[test]
    fn n_of_m_not_first_n() {
        // Verify that the selected positions are shuffled, not just 0..n
        let node = NofM::new(1, 10);
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
        OneOf::new(vec![]);
    }

    // --- OneOfWeighted ---

    #[test]
    fn one_of_weighted_selects_from_spec() {
        let node = OneOfWeighted::new("red:60,blue:30,green:10");
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
        let node = OneOfWeighted::new("a:50,b:50");
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
        let node = OneOfWeighted::new("heavy:90,light:10");
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
        let node = OneOfWeighted::new("only:1");
        let mut out = [Value::None];
        for i in 0..20u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_str(), "only");
        }
    }

    #[test]
    fn one_of_weighted_semicolon_delimiter() {
        let node = OneOfWeighted::new("x:50;y:50");
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        let s = out[0].as_str().to_string();
        assert!(s == "x" || s == "y");
    }

    #[test]
    #[should_panic(expected = "one_of_weighted: spec must be non-empty")]
    fn one_of_weighted_rejects_empty() {
        OneOfWeighted::new("");
    }

    #[test]
    #[should_panic(expected = "one_of_weighted: expected 'value:weight'")]
    fn one_of_weighted_rejects_bad_format() {
        OneOfWeighted::new("noweight");
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
        Blend::new(1.5);
    }

    #[test]
    #[should_panic(expected = "blend: mix must be in [0.0, 1.0]")]
    fn blend_rejects_negative_mix() {
        Blend::new(-0.1);
    }
}
