// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Round-number selector nodes.
//!
//! General-purpose "snap this magnitude to a nice round number" math on
//! f64 values. Four scale families — powers of ten (`base10`), multiples
//! of the base-ten magnitude (`decade`), Fibonacci numbers (`fibonacci`),
//! and powers of two (`binomial`) — each in `floor_` / `ceiling_` /
//! `closest_` variants, plus a general arbitrary-`interval` rounder.
//!
//! These are pure numeric utilities (no adapter, no prefix), used to pick
//! human-friendly axis ticks, bucket boundaries, and magnitude labels.
//!
//! Edge handling: every family selector returns `0.0` for `x <= 0` and for
//! non-finite `x` — no panics, no NaN/inf leaking. Fractional `x` in `(0,1)`
//! yields fractional powers for `base10`/`binomial` (e.g. `floor_base10(0.5)
//! = 0.1`), which is correct and preserved. The interval rounders return
//! `x` unchanged when `interval` is non-positive or non-finite (identity —
//! never divide by zero).

// ---------------------------------------------------------------------------
// Private helpers (not nodes — plain module fns callable from node bodies).
// ---------------------------------------------------------------------------

/// True only for a strictly-positive, finite `x`. All family selectors gate
/// on this and return `0.0` otherwise, so `x <= 0`, `NaN`, and `±inf` all
/// fold to the zero magnitude without special-casing each node.
#[inline]
fn positive_finite(x: f64) -> bool {
    x.is_finite() && x > 0.0
}

/// Largest power of ten `10^n <= x`. Assumes `positive_finite(x)`.
///
/// The exponent is taken from `log10(x).floor()` but reconstructed with
/// `powi` (exact for `|result| < 2^53`) and corrected by at most one step,
/// so a one-ulp `log10` error at an exact power-of-ten boundary can't leak.
///
/// `pub` so non-node callers can reuse the exact `floor_base10` formula
/// without re-deriving it (DRY). `floor_base10` the node is just this
/// function behind the `positive_finite` guard, so a caller that already
/// guarantees a strictly-positive, finite `x` (e.g. the CQL batch-stride
/// planner, which floors `budget / row_size`) can call this directly.
#[inline]
pub fn floor_pow10(x: f64) -> f64 {
    let mut e = x.log10().floor() as i32;
    let mut p = 10f64.powi(e);
    if p > x {
        e -= 1;
        p = 10f64.powi(e);
    } else if p * 10.0 <= x {
        e += 1;
        p = 10f64.powi(e);
    }
    p
}

/// Largest power of two `2^n <= x`. Assumes `positive_finite(x)`.
///
/// Same `powi` + one-step-correction scheme as [`floor_pow10`].
#[inline]
fn floor_pow2(x: f64) -> f64 {
    let mut e = x.log2().floor() as i32;
    let mut p = 2f64.powi(e);
    if p > x {
        e -= 1;
        p = 2f64.powi(e);
    } else if p * 2.0 <= x {
        e += 1;
        p = 2f64.powi(e);
    }
    p
}

/// Pick whichever of `lo` / `hi` is nearer to `x` by absolute distance.
/// Ties resolve to `lo` (the floor), per the `closest_*` contract.
#[inline]
fn pick_closest(x: f64, lo: f64, hi: f64) -> f64 {
    if (hi - x).abs() < (x - lo).abs() {
        hi
    } else {
        lo
    }
}

/// Largest Fibonacci number (`1, 2, 3, 5, 8, …`) that is `<= x`, or `0.0`
/// when `x < 1` (nothing in the sequence is that small). Assumes finite `x`.
fn floor_fibonacci_val(x: f64) -> f64 {
    if x < 1.0 {
        return 0.0;
    }
    let (mut a, mut b) = (1.0f64, 2.0f64);
    while b <= x {
        let next = a + b;
        a = b;
        b = next;
        if !b.is_finite() {
            return a;
        }
    }
    a
}

/// Smallest Fibonacci number (`1, 2, 3, 5, 8, …`) that is `>= x`; `1.0` for
/// `x <= 1`. Assumes `positive_finite(x)`.
fn ceiling_fibonacci_val(x: f64) -> f64 {
    let (mut a, mut b) = (1.0f64, 2.0f64);
    if x <= a {
        return a;
    }
    loop {
        if b >= x {
            return b;
        }
        let next = a + b;
        a = b;
        b = next;
        if !b.is_finite() {
            return b;
        }
    }
}

// ---------------------------------------------------------------------------
// base10 — powers of ten (10^n).
// ---------------------------------------------------------------------------

/// Largest power of ten `<= x`: `10^floor(log10(x))`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn floor_base10(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    floor_pow10(x)
}

/// Smallest power of ten `>= x`: `10^ceil(log10(x))`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn ceiling_base10(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    let lo = floor_pow10(x);
    if lo == x { lo } else { lo * 10.0 }
}

/// Power of ten nearest to `x` by absolute distance (ties → floor).
/// `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn closest_base10(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    let lo = floor_pow10(x);
    let hi = if lo == x { lo } else { lo * 10.0 };
    pick_closest(x, lo, hi)
}

// ---------------------------------------------------------------------------
// decade — multiples of the base-ten magnitude `base = 10^floor(log10(x))`.
// ---------------------------------------------------------------------------

/// Round `x` down to a multiple of its base-ten magnitude:
/// `floor(x/base)*base`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn floor_decade(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    let base = floor_pow10(x);
    (x / base).floor() * base
}

/// Round `x` up to a multiple of its base-ten magnitude:
/// `ceil(x/base)*base`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn ceiling_decade(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    let base = floor_pow10(x);
    (x / base).ceil() * base
}

/// Round `x` to the nearest multiple of its base-ten magnitude:
/// `round(x/base)*base`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn closest_decade(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    let base = floor_pow10(x);
    (x / base).round() * base
}

// ---------------------------------------------------------------------------
// fibonacci — Fibonacci numbers 1, 2, 3, 5, 8, 13, … (start 1, 2).
// ---------------------------------------------------------------------------

/// Largest Fibonacci number `<= x`. `x < 1` (incl. `x <= 0`) → `0.0`.
#[crate::polydat_node(category = Math)]
fn floor_fibonacci(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    floor_fibonacci_val(x)
}

/// Smallest Fibonacci number `>= x`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn ceiling_fibonacci(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    ceiling_fibonacci_val(x)
}

/// Fibonacci number nearest to `x` by absolute distance (ties → floor).
/// `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn closest_fibonacci(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    pick_closest(x, floor_fibonacci_val(x), ceiling_fibonacci_val(x))
}

// ---------------------------------------------------------------------------
// binomial — powers of two (2^n).
// ---------------------------------------------------------------------------

/// Largest power of two `<= x`: `2^floor(log2(x))`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn floor_binomial(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    floor_pow2(x)
}

/// Smallest power of two `>= x`: `2^ceil(log2(x))`. `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn ceiling_binomial(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    let lo = floor_pow2(x);
    if lo == x { lo } else { lo * 2.0 }
}

/// Power of two nearest to `x` by absolute distance (ties → floor).
/// `x <= 0` → `0.0`.
#[crate::polydat_node(category = Math)]
fn closest_binomial(x: f64) -> f64 {
    if !positive_finite(x) {
        return 0.0;
    }
    let lo = floor_pow2(x);
    let hi = if lo == x { lo } else { lo * 2.0 };
    pick_closest(x, lo, hi)
}

// ---------------------------------------------------------------------------
// General arbitrary-interval rounders.
// ---------------------------------------------------------------------------

/// Round `x` down to a multiple of `interval`: `floor(x/interval)*interval`.
/// `interval <= 0` or non-finite → returns `x` unchanged (identity).
#[crate::polydat_node(category = Math)]
fn round_floor(x: f64, interval: f64) -> f64 {
    if !(interval.is_finite() && interval > 0.0) {
        return x;
    }
    (x / interval).floor() * interval
}

/// Round `x` up to a multiple of `interval`: `ceil(x/interval)*interval`.
/// `interval <= 0` or non-finite → returns `x` unchanged (identity).
#[crate::polydat_node(category = Math)]
fn round_ceiling(x: f64, interval: f64) -> f64 {
    if !(interval.is_finite() && interval > 0.0) {
        return x;
    }
    (x / interval).ceil() * interval
}

/// Round `x` to the nearest multiple of `interval`: `round(x/interval)*interval`.
/// `interval <= 0` or non-finite → returns `x` unchanged (identity).
#[crate::polydat_node(category = Math)]
fn round_nearest(x: f64, interval: f64) -> f64 {
    if !(interval.is_finite() && interval > 0.0) {
        return x;
    }
    (x / interval).round() * interval
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    fn run1(node: &dyn PolydatNode, x: f64) -> f64 {
        let mut out = [Value::None];
        node.eval(&[Value::F64(x)], &mut out);
        out[0].as_f64()
    }

    fn run2(node: &dyn PolydatNode, x: f64, interval: f64) -> f64 {
        let mut out = [Value::None];
        node.eval(&[Value::F64(x), Value::F64(interval)], &mut out);
        out[0].as_f64()
    }

    // ── base10 ────────────────────────────────────────────
    #[test]
    fn base10_vectors() {
        assert_eq!(run1(&FloorBase10::new(), 1732.234), 1000.0);
        assert_eq!(run1(&CeilingBase10::new(), 1732.0), 10000.0);
        assert_eq!(run1(&ClosestBase10::new(), 1732.0), 1000.0);
        assert_eq!(run1(&ClosestBase10::new(), 6000.0), 10000.0);
    }

    #[test]
    fn base10_exact_power_is_stable() {
        // On an exact power of ten, floor == ceiling == x.
        assert_eq!(run1(&FloorBase10::new(), 1000.0), 1000.0);
        assert_eq!(run1(&CeilingBase10::new(), 1000.0), 1000.0);
    }

    #[test]
    fn base10_fractional_below_one() {
        // (0,1): powers of ten stay fractional — keep it.
        assert!((run1(&FloorBase10::new(), 0.5) - 0.1).abs() < 1e-12);
    }

    // ── decade ────────────────────────────────────────────
    #[test]
    fn decade_vectors() {
        assert_eq!(run1(&FloorDecade::new(), 2734.0), 2000.0);
        assert_eq!(run1(&CeilingDecade::new(), 2734.0), 3000.0);
        assert_eq!(run1(&ClosestDecade::new(), 2734.0), 3000.0);
        assert_eq!(run1(&FloorDecade::new(), 1732.0), 1000.0);
        assert_eq!(run1(&ClosestDecade::new(), 1732.0), 2000.0);
        assert_eq!(run1(&CeilingDecade::new(), 1732.0), 2000.0);
    }

    // ── fibonacci ─────────────────────────────────────────
    #[test]
    fn fibonacci_vectors() {
        assert_eq!(run1(&FloorFibonacci::new(), 1732.0), 1597.0);
        assert_eq!(run1(&CeilingFibonacci::new(), 1732.0), 2584.0);
        assert_eq!(run1(&ClosestFibonacci::new(), 1732.0), 1597.0);
    }

    #[test]
    fn fibonacci_floor_below_one_is_zero() {
        assert_eq!(run1(&FloorFibonacci::new(), 0.5), 0.0);
    }

    #[test]
    fn fibonacci_exact_member_is_stable() {
        assert_eq!(run1(&FloorFibonacci::new(), 1597.0), 1597.0);
        assert_eq!(run1(&CeilingFibonacci::new(), 1597.0), 1597.0);
    }

    // ── binomial ──────────────────────────────────────────
    #[test]
    fn binomial_vectors() {
        assert_eq!(run1(&FloorBinomial::new(), 1732.0), 1024.0);
        assert_eq!(run1(&CeilingBinomial::new(), 1732.0), 2048.0);
        assert_eq!(run1(&ClosestBinomial::new(), 1732.0), 2048.0);
    }

    #[test]
    fn binomial_exact_power_is_stable() {
        assert_eq!(run1(&FloorBinomial::new(), 1024.0), 1024.0);
        assert_eq!(run1(&CeilingBinomial::new(), 1024.0), 1024.0);
    }

    // ── non-positive / non-finite edges ───────────────────
    #[test]
    fn non_positive_inputs_are_zero() {
        assert_eq!(run1(&FloorBase10::new(), 0.0), 0.0);
        assert_eq!(run1(&FloorBase10::new(), -5.0), 0.0);
        assert_eq!(run1(&CeilingBinomial::new(), -1.0), 0.0);
        assert_eq!(run1(&ClosestFibonacci::new(), 0.0), 0.0);
        assert_eq!(run1(&ClosestDecade::new(), -1000.0), 0.0);
        assert_eq!(run1(&FloorBase10::new(), f64::INFINITY), 0.0);
        assert_eq!(run1(&FloorBinomial::new(), f64::NAN), 0.0);
    }

    // ── general interval rounders ─────────────────────────
    #[test]
    fn round_interval_vectors() {
        assert_eq!(run2(&RoundFloor::new(), 1732.0, 500.0), 1500.0);
        assert_eq!(run2(&RoundCeiling::new(), 1732.0, 500.0), 2000.0);
        // (1732/500).round() = 3, so nearest multiple of 500 is 1500.
        assert_eq!(run2(&RoundNearest::new(), 1732.0, 500.0), 1500.0);
        assert_eq!(run2(&RoundNearest::new(), 1700.0, 500.0), 1500.0);
        // interval <= 0 → identity.
        assert_eq!(run2(&RoundFloor::new(), 1732.0, 0.0), 1732.0);
    }

    #[test]
    fn round_interval_identity_on_bad_interval() {
        assert_eq!(run2(&RoundNearest::new(), 1732.0, -5.0), 1732.0);
        assert_eq!(run2(&RoundCeiling::new(), 1732.0, f64::INFINITY), 1732.0);
        assert_eq!(run2(&RoundFloor::new(), 1732.0, f64::NAN), 1732.0);
    }

    // ── registry discovery ────────────────────────────────
    #[test]
    fn all_fifteen_registered_under_math() {
        for name in [
            "floor_base10", "ceiling_base10", "closest_base10",
            "floor_decade", "ceiling_decade", "closest_decade",
            "floor_fibonacci", "ceiling_fibonacci", "closest_fibonacci",
            "floor_binomial", "ceiling_binomial", "closest_binomial",
            "round_floor", "round_ceiling", "round_nearest",
        ] {
            let sig = crate::dsl::registry::lookup(name)
                .unwrap_or_else(|| panic!("node '{name}' not registered"));
            assert_eq!(
                sig.category,
                crate::dsl::registry::FuncCategory::Math,
                "node '{name}' registered under wrong category",
            );
        }
    }
}
