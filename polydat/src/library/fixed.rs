// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Fixed value and value-list nodes across fundamental types.

// =================================================================
// Constants (0→1 nodes)
// =================================================================

/// Emit a fixed f64 value.
///
/// Signature: `() -> (f64)`
/// Emit a fixed f64 value. SRD-80 PR B.15 migration.
#[crate::polydat_node(category = Math)]
fn const_f64(#[poly_default(0.0f64)] value: crate::derive_support::Const<f64>) -> f64 {
    *value
}

/// Emit a fixed bool value. SRD-80 PR B.15 migration.
#[crate::polydat_node(category = Math)]
fn const_bool(#[poly_default(false)] value: crate::derive_support::Const<bool>) -> bool {
    *value
}

// =================================================================
// Fixed value lists (1→1 nodes, input selects by index)
// =================================================================
//
// SRD-80b Phase C — migrated to `#[polydat_node]` via the
// `Const<Vec<C>>` workload-list combinator. The macro recognises
// the trailing `Const<Vec<C>>` arg and packages `consts[1..]`
// into a `Vec<C>` field at build time via
// `<C as ConstSource>::extract` per element. Empty lists are
// rejected in the body (the body panics) rather than at the
// macro level — the FuncSig's `Arity::VariadicConsts { min_consts: 0 }`
// would otherwise have to be `min_consts: 1`, which is per-node
// validation the macro can't auto-infer.

/// Select from a fixed list of u64 values by index. The input
/// is taken modulo the list length.
#[crate::polydat_node(category = Math)]
fn fixed_values_u64(input: u64, values: crate::derive_support::Const<Vec<u64>>) -> u64 {
    assert!(!values.is_empty(), "fixed_values_u64: value list must not be empty");
    let idx = (input as usize) % values.len();
    values[idx]
}

/// Select from a fixed list of f64 values by index.
#[crate::polydat_node(category = Math)]
fn fixed_values_f64(input: u64, values: crate::derive_support::Const<Vec<f64>>) -> f64 {
    assert!(!values.is_empty(), "fixed_values_f64: value list must not be empty");
    let idx = (input as usize) % values.len();
    values[idx]
}

/// Select from a fixed list of strings by index.
#[crate::polydat_node(category = Math)]
fn fixed_values_str(input: u64, values: crate::derive_support::Const<Vec<String>>) -> String {
    assert!(!values.is_empty(), "fixed_values_str: value list must not be empty");
    let idx = (input as usize) % values.len();
    values[idx].clone()
}

// =================================================================
// CoinFlip: probabilistic boolean
// =================================================================

/// Probabilistic boolean: true with a given probability.
///
/// Signature: `(input: u64) -> (bool)`
///
/// The input is expected to be hashed (uniform). The threshold is
/// precomputed from the probability at init time.
fn compute_threshold(probability: f64) -> u64 {
    (probability.clamp(0.0, 1.0) * u64::MAX as f64) as u64
}

/// Probabilistic boolean with a precomputed threshold from a
/// const probability arg. SRD-80 PR B.15 migration.
#[crate::polydat_node(category = Probability)]
fn coin_flip(
    input: u64,
    #[poly_default(0.5f64)] probability: crate::derive_support::Const<f64>,
    #[poly_const(compute_threshold, from = probability)] threshold: &u64,
) -> bool {
    input < *threshold
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn const_f64() {
        let node = ConstF64::new(3.14);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 3.14);
    }

    #[test]
    fn const_bool() {
        let node = ConstBool::new(true);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert!(out[0].as_bool());
    }

    #[test]
    fn fixed_values_u64_cycles() {
        let node = FixedValuesU64::new(vec![10, 20, 30]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_u64(), 10);
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_u64(), 20);
        node.eval(&[Value::U64(2)], &mut out);
        assert_eq!(out[0].as_u64(), 30);
        node.eval(&[Value::U64(3)], &mut out);
        assert_eq!(out[0].as_u64(), 10); // wraps
    }

    // SRD-80b Phase C — `fixed_values_u64` migrated to the
    // macro's `Const<Vec<u64>>` shape, which is JIT-ineligible
    // (the JIT u64 buffer has no slot shape for a variable-length
    // captured list). The eval-path test above still covers
    // correctness; a future `compiled_u64_override` could
    // reinstate the closure form if perf demands it.

    #[test]
    fn fixed_values_f64() {
        let node = FixedValuesF64::new(vec![1.1, 2.2, 3.3]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_f64(), 2.2);
    }

    #[test]
    fn fixed_values_str() {
        let node = FixedValuesStr::new(vec!["alpha".into(), "beta".into(), "gamma".into()]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(2)], &mut out);
        assert_eq!(out[0].as_str(), "gamma");
    }

    #[test]
    fn coin_flip_always_true() {
        let node = CoinFlip::new(1.0);
        let mut out = [Value::None];
        for i in 0..100 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(out[0].as_bool());
        }
    }

    #[test]
    fn coin_flip_always_false() {
        let node = CoinFlip::new(0.0);
        let mut out = [Value::None];
        for i in 0..100 {
            node.eval(&[Value::U64(i)], &mut out);
            assert!(!out[0].as_bool());
        }
    }

    #[test]
    fn coin_flip_roughly_half() {
        use xxhash_rust::xxh3::xxh3_64;
        let node = CoinFlip::new(0.5);
        let mut true_count = 0;
        let n = 10_000u64;
        let mut out = [Value::None];
        for i in 0..n {
            let hashed = xxh3_64(&i.to_le_bytes());
            node.eval(&[Value::U64(hashed)], &mut out);
            if out[0].as_bool() {
                true_count += 1;
            }
        }
        let ratio = true_count as f64 / n as f64;
        assert!(
            (ratio - 0.5).abs() < 0.05,
            "expected ~50%, got {ratio}"
        );
    }
}
