// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Signal-settling / steady-state detection nodes.
//!
//! SRD-86 §"Causal ordering, the freshness register, and
//! settling". An optimizer objective declared over a run-produced
//! metric (e.g. `score := 0 - metric("errors","rate")`) is a
//! *volatile* wire: its per-cycle value chases the live metric
//! window, and at phase completion the trailing window is empty so
//! a naïve post-execution read returns the empty-window value
//! rather than the level the phase actually produced.
//!
//! [`is_stable`] conditions that volatile wire into a stable
//! register the phase executor can read after completion. It is a
//! *stateful* node — its cross-cycle state (a bounded ring of the
//! most recent samples) lives in a `Mutex<SettleState>` setup
//! field, exactly as `fft_analyze` carries its window buffer.
//! Because the state is an internal register and not a global,
//! every output is a deterministic function of the input *history*
//! — the node is fully verifiable in polydat function space by
//! feeding a sample sequence and asserting the `(stable_value,
//! stable)` output sequence (see this module's tests).
//!
//! Cross-cycle *wire* reference is not expressible in polydat (a
//! wire cannot read its own prior value), so the register is held
//! internally and re-published each cycle as the `stable_value`
//! output rather than threaded back in as an input wire.

use std::collections::VecDeque;
use std::sync::Mutex;

/// Cross-cycle register for [`is_stable`]: a bounded ring of the
/// most recent objective samples. The ring is the only state; the
/// reported `stable_value` (median) and `stable` (steady-state)
/// outputs are derived from it each eval, so a re-evaluation after
/// the run (e.g. the executor's completion read) that pushes one
/// trailing empty-window sample cannot move the median off the
/// settled level.
struct SettleState {
    samples: VecDeque<f64>,
}

impl crate::derive_support::PolydatSetup for Mutex<SettleState> {}

/// Build the per-call settle register pre-sized to `horizon`. The
/// horizon is clamped to a minimum of 1; the eval body enforces it
/// as the ring bound each cycle.
fn settle_register(horizon: u64) -> Mutex<SettleState> {
    let cap = horizon.max(1) as usize;
    Mutex::new(SettleState {
        samples: VecDeque::with_capacity(cap),
    })
}

/// Condition a per-cycle objective signal into a settled register
/// and a steady-state signal.
///
/// Signature: `is_stable(objective_value: f64, margin: f64,
/// min_samples: u64, horizon: u64) -> (stable_value: f64, stable:
/// u64)`
///
/// Each cycle pushes `objective_value` onto a bounded ring of the
/// most recent `horizon` samples and reports two outputs:
///
/// - `stable_value` — the **median** of the ring, a robust
///   central-tendency estimate of the level the phase is
///   currently producing. This is the register the phase executor
///   reads as the optimizer objective after completion. It is
///   populated from the first sample and, being a median over the
///   window, is resistant to a single trailing empty-window
///   outlier.
/// - `stable` — `1` when the signal has reached steady state:
///   the ring holds at least `min_samples` samples *and* the
///   sample standard deviation is within `margin · max(|median|,
///   1)` (a relative band with an absolute floor so a level near
///   zero still settles). Otherwise `0`. The executor reads this
///   to decide when settling is complete and the phase may stop.
///
/// Declared `Nondeterministic`: the output depends on the sample
/// history accumulated across calls, not on the current input
/// alone. The eval-spanning ring is the load-bearing aspect.
#[crate::polydat_node(
    category = Math,
    purity = Nondeterministic("accumulates objective samples across cycles; outputs depend on prior history"),
    output_names(stable_value, stable),
)]
fn is_stable(
    objective_value: f64,
    #[poly_default(0.05f64)] margin: crate::derive_support::Const<f64>,
    #[poly_default(8u64)] min_samples: crate::derive_support::Const<u64>,
    #[poly_default(32u64)] horizon: crate::derive_support::Const<u64>,
    #[poly_const(settle_register, from = horizon)] register: &Mutex<SettleState>,
) -> (f64, u64) {
    let cap = (*horizon).max(1) as usize;
    let min_n = (*min_samples) as usize;

    let mut st = register.lock().unwrap();
    st.samples.push_back(objective_value);
    while st.samples.len() > cap {
        st.samples.pop_front();
    }
    let n = st.samples.len();
    if n == 0 {
        return (0.0, 0);
    }

    // Robust central tendency: median of the recent window. This
    // is the register the executor reads as the objective — always
    // populated, and resistant to a single trailing empty-window
    // outlier (one sample among `horizon`).
    let mut sorted: Vec<f64> = st.samples.iter().copied().collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if n % 2 == 1 {
        sorted[n / 2]
    } else {
        0.5 * (sorted[n / 2 - 1] + sorted[n / 2])
    };

    // Strict steady-state gate: enough samples AND low spread.
    let stable = if n < min_n {
        0u64
    } else {
        let mean = st.samples.iter().sum::<f64>() / n as f64;
        let var = st
            .samples
            .iter()
            .map(|x| {
                let d = x - mean;
                d * d
            })
            .sum::<f64>()
            / n as f64;
        let stddev = var.sqrt();
        let threshold = (*margin) * median.abs().max(1.0);
        if stddev <= threshold { 1 } else { 0 }
    };

    (median, stable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    /// Regression: a declared `input x: f64` must keep its f64 type
    /// through the DSL. It used to be typed `U64` (the assembler seeded
    /// every input as U64 and dropped the declared type), forcing a
    /// spurious `U64→F64` adapter at every f64 consumer that panicked
    /// at runtime when the f64 value met the adapter's `as_u64`. Here
    /// the f64 input flows through the heterogeneous `(f64, u64)`
    /// `is_stable` destructure without an adapter.
    #[test]
    fn declared_f64_input_flows_without_a_spurious_adapter() {
        let mut k = crate::dsl::compile::compile_polydat(
            "input source: f64\n(stable_value, stable) := is_stable(source, 0.05, 4, 8)",
        )
        .expect("compile");
        if let Some(idx) = k.program().find_input("source") {
            k.state().set_input(idx, Value::F64(5.0));
        }
        // A wrong U64→F64 adapter would panic pulling these (f64 read
        // as u64). The destructure types resolve: value=f64, signal=u64.
        assert_eq!(k.pull("stable").as_u64(), 0, "n=1 < min_samples");
        assert!((k.pull("stable_value").as_f64() - 5.0).abs() < 1e-9);
    }

    /// Feed a noisy ramp that ages out of an 8-deep window, then a
    /// steady tail: the strict gate latches and the register tracks
    /// the steady level.
    #[test]
    fn settles_on_a_steady_signal_and_reports_the_level() {
        let node = IsStable::new(0.05, 4, 8);
        let mut out = [Value::None, Value::None];
        for x in [0.0, 1.0, 3.0, 4.5] {
            node.eval(&[Value::F64(x)], &mut out);
        }
        // Eight steady samples fully evict the ramp from the ring.
        for _ in 0..8 {
            node.eval(&[Value::F64(5.0)], &mut out);
        }
        assert_eq!(out[1].as_u64(), 1, "steady tail should report stable");
        assert!(
            (out[0].as_f64() - 5.0).abs() < 1e-9,
            "register should track the steady level, got {}",
            out[0].as_f64()
        );
    }

    /// Once settled, a single trailing empty-window outlier (the
    /// shape of the executor's post-completion read) must not move
    /// the register off the produced level — the median absorbs it.
    #[test]
    fn a_trailing_outlier_does_not_corrupt_the_register() {
        let node = IsStable::new(0.05, 4, 8);
        let mut out = [Value::None, Value::None];
        for _ in 0..8 {
            node.eval(&[Value::F64(5.0)], &mut out);
        }
        assert!((out[0].as_f64() - 5.0).abs() < 1e-9, "settled at 5.0");

        // One trailing outlier: window becomes [5×7, 0]; median 5.
        node.eval(&[Value::F64(0.0)], &mut out);
        assert!(
            (out[0].as_f64() - 5.0).abs() < 1e-9,
            "median is robust to one outlier, got {}",
            out[0].as_f64()
        );
        assert_eq!(
            out[1].as_u64(),
            0,
            "one outlier breaks strict steady-state"
        );
    }

    /// Viability floor: below `min_samples` the signal is never
    /// stable, however clean the data.
    #[test]
    fn reports_unstable_until_min_samples() {
        let node = IsStable::new(0.05, 4, 8);
        let mut out = [Value::None, Value::None];
        node.eval(&[Value::F64(5.0)], &mut out);
        assert_eq!(out[1].as_u64(), 0, "1 sample < min_samples");
        node.eval(&[Value::F64(5.0)], &mut out);
        node.eval(&[Value::F64(5.0)], &mut out);
        assert_eq!(out[1].as_u64(), 0, "3 samples < min_samples=4");
        node.eval(&[Value::F64(5.0)], &mut out);
        assert_eq!(
            out[1].as_u64(),
            1,
            "4 steady samples reach min_samples with zero spread"
        );
    }

    /// A level near zero still settles thanks to the absolute floor
    /// in the threshold (relevant to `score = -err_rate` objectives
    /// whose settled value sits at or near 0).
    #[test]
    fn a_near_zero_level_still_settles() {
        let node = IsStable::new(0.05, 4, 8);
        let mut out = [Value::None, Value::None];
        for _ in 0..8 {
            node.eval(&[Value::F64(0.0)], &mut out);
        }
        assert_eq!(out[1].as_u64(), 1, "steady zero is stable");
        assert!((out[0].as_f64()).abs() < 1e-9, "register at the zero level");
    }
}
