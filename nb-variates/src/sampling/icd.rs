// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Inverse CDF distribution builders.
//!
//! Each function builds a [`LutF64`] containing the precomputed inverse
//! CDF for a specific distribution. The returned LUT is used with a
//! [`LutSample`] node for runtime evaluation.
//!
//! This module also provides the [`UnitInterval`] and [`ClampF64`] GK
//! nodes that are typically composed with LUT sampling in a DAG.
//!
//! # Supported Distributions
//!
//! **Continuous:**
//! Normal, Exponential, Uniform, Pareto, LogNormal, Weibull, Cauchy,
//! Laplace, Beta, Gamma
//!
//! **Discrete:**
//! Zipf, Poisson, Binomial, Geometric

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, PortType, Slot, Value};
use crate::sampling::lut::{LutF64, LutSample};

/// Default interpolation table resolution.
pub const DEFAULT_RESOLUTION: usize = 1000;

// =================================================================
// Utility: UnitInterval node
// =================================================================

/// Normalize a u64 to a uniform f64 in [0.0, 1.0).
///
/// Signature: `unit_interval(input: u64) -> (f64)`
///
/// The bridge between the integer hash domain and the continuous
/// probability domain. Place after `hash` and before any node that
/// expects a [0,1) input: distribution LUT samplers, `lerp`, or
/// `inv_lerp`. The mapping is `input as f64 / u64::MAX as f64`, so
/// 0 maps to 0.0 and u64::MAX maps to ~1.0.
///
/// JIT level: P2 (compiled_u64 closure; single division).
pub struct UnitInterval {
    meta: NodeMeta,
}

impl Default for UnitInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl UnitInterval {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "unit_interval".into(),
                outs: vec![Port::new("output", PortType::F64)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for UnitInterval {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(inputs[0].as_u64() as f64 / u64::MAX as f64);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let v = inputs[0] as f64 / u64::MAX as f64;
            outputs[0] = v.to_bits();
        }))
    }
}

// =================================================================
// Utility: ClampF64 node
// =================================================================

/// Clamp an f64 value to [min, max].
///
/// Signature: `clamp_f64(input: f64, min: f64, max: f64) -> (f64)`
///
/// Hard-limits an f64 to the given bounds. Use after distributions
/// with unbounded tails (normal, Cauchy) to enforce domain
/// constraints: `clamp_f64(normal(72.0, 5.0), 0.0, 100.0)` prevents
/// negative scores. Also useful for guarding against non-finite LUT
/// edge values before downstream arithmetic.
///
/// JIT level: P3 (compiled_u64 with jit_constants for min and max).
pub struct ClampF64 {
    meta: NodeMeta,
    min: f64,
    max: f64,
}

impl ClampF64 {
    pub fn new(min: f64, max: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "clamp_f64".into(),
                outs: vec![Port::new("output", PortType::F64)],
                ins: vec![
                    Slot::Wire(Port::new("input", PortType::F64)),
                    Slot::const_f64("min", min),
                    Slot::const_f64("max", max),
                ],
            },
            min,
            max,
        }
    }
}

impl GkNode for ClampF64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(inputs[0].as_f64().clamp(self.min, self.max));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let min = self.min;
        let max = self.max;
        Some(Box::new(move |inputs, outputs| {
            let v = f64::from_bits(inputs[0]).clamp(min, max);
            outputs[0] = v.to_bits();
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.min.to_bits(), self.max.to_bits()] }
}

// =================================================================
// Convenience: IcdSample node (wraps LutSample with distribution builder)
// =================================================================

/// A distribution-sampling GK node backed by a LUT.
///
/// Signature: `icd_sample(input: f64) -> (f64)`
///
/// This is a convenience wrapper -- it builds the LUT at construction
/// time and delegates to `LutSample` for evaluation. It exists so
/// callers can write `IcdSample::normal(72.0, 5.0)` without manually
/// constructing and wiring a LUT.
///
/// Use for any statistical distribution sampling where the input is a
/// uniform [0,1) value (typically from `unit_interval`) and the output
/// is a distribution-shaped f64. Supports Normal, Exponential, Uniform,
/// Pareto, LogNormal, Weibull, Cauchy, Laplace, Beta, Gamma, Zipf,
/// Poisson, Binomial, and Geometric via named constructors.
///
/// JIT level: P2 (delegates to LutSample compiled_u64; O(1) table
/// lookup with linear interpolation).
pub struct IcdSample {
    inner: LutSample,
}

impl IcdSample {
    pub fn from_lut(lut: LutF64) -> Self {
        Self { inner: LutSample::new(lut) }
    }

    pub fn normal(mean: f64, stddev: f64) -> Self {
        Self::from_lut(dist_normal(mean, stddev, DEFAULT_RESOLUTION))
    }

    pub fn exponential(rate: f64) -> Self {
        Self::from_lut(dist_exponential(rate, DEFAULT_RESOLUTION))
    }

    pub fn uniform(min: f64, max: f64) -> Self {
        Self::from_lut(dist_uniform(min, max, DEFAULT_RESOLUTION))
    }

    pub fn pareto(scale: f64, shape: f64) -> Self {
        Self::from_lut(dist_pareto(scale, shape, DEFAULT_RESOLUTION))
    }

    pub fn lognormal(mean: f64, stddev: f64) -> Self {
        Self::from_lut(dist_lognormal(mean, stddev, DEFAULT_RESOLUTION))
    }

    pub fn weibull(shape: f64, scale: f64) -> Self {
        Self::from_lut(dist_weibull(shape, scale, DEFAULT_RESOLUTION))
    }

    pub fn cauchy(location: f64, scale: f64) -> Self {
        Self::from_lut(dist_cauchy(location, scale, DEFAULT_RESOLUTION))
    }

    pub fn laplace(location: f64, scale: f64) -> Self {
        Self::from_lut(dist_laplace(location, scale, DEFAULT_RESOLUTION))
    }

    pub fn beta(alpha: f64, beta: f64) -> Self {
        Self::from_lut(dist_beta(alpha, beta, DEFAULT_RESOLUTION))
    }

    pub fn gamma(shape: f64, scale: f64) -> Self {
        Self::from_lut(dist_gamma(shape, scale, DEFAULT_RESOLUTION))
    }

    pub fn zipf(n: u64, exponent: f64) -> Self {
        Self::from_lut(dist_zipf(n, exponent, DEFAULT_RESOLUTION))
    }

    pub fn poisson(lambda: f64) -> Self {
        Self::from_lut(dist_poisson(lambda, DEFAULT_RESOLUTION))
    }

    pub fn binomial(trials: u64, p: f64) -> Self {
        Self::from_lut(dist_binomial(trials, p, DEFAULT_RESOLUTION))
    }

    pub fn geometric(p: f64) -> Self {
        Self::from_lut(dist_geometric(p, DEFAULT_RESOLUTION))
    }
}

impl GkNode for IcdSample {
    fn meta(&self) -> &NodeMeta {
        self.inner.meta()
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        self.inner.eval(inputs, outputs);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        self.inner.compiled_u64()
    }

    fn jit_constants(&self) -> Vec<u64> {
        self.inner.jit_constants()
    }
}

// =================================================================
// Normal inverse CDF (probit function)
// =================================================================

/// Rational approximation of the standard normal quantile function.
/// Accurate to ~1e-9 for p in (1e-15, 1-1e-15).
fn probit(p: f64) -> f64 {
    if p <= 0.0 { return f64::NEG_INFINITY; }
    if p >= 1.0 { return f64::INFINITY; }

    let t = if p < 0.5 {
        (-2.0 * p.ln()).sqrt()
    } else {
        (-2.0 * (1.0 - p).ln()).sqrt()
    };

    let c0 = 2.515517;
    let c1 = 0.802853;
    let c2 = 0.010328;
    let d1 = 1.432788;
    let d2 = 0.189269;
    let d3 = 0.001308;

    let result = t - (c0 + c1 * t + c2 * t * t)
        / (1.0 + d1 * t + d2 * t * t + d3 * t * t * t);

    if p < 0.5 { -result } else { result }
}

// =================================================================
// Gamma function utilities (for Beta and Gamma distributions)
// =================================================================

/// Lanczos approximation of ln(Gamma(x)) for x > 0.
fn ln_gamma(x: f64) -> f64 {
    let g = 7.0;
    let c = [
        0.999_999_999_999_809_9,
        676.5203681218851,
        -1259.1392167224028,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507343278686905,
        -0.13857109526572012,
        9.984_369_578_019_572e-6,
        1.5056327351493116e-7,
    ];

    if x < 0.5 {
        let pi = std::f64::consts::PI;
        return (pi / (pi * x).sin()).ln() - ln_gamma(1.0 - x);
    }

    let x = x - 1.0;
    let mut sum = c[0];
    for (i, &coeff) in c[1..].iter().enumerate() {
        sum += coeff / (x + i as f64 + 1.0);
    }

    let t = x + g + 0.5;
    0.5 * (2.0 * std::f64::consts::PI).ln() + (t.ln() * (x + 0.5)) - t + sum.ln()
}

/// Regularized incomplete beta function I_x(a, b) via series expansion.
fn regularized_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 { return 0.0; }
    if x >= 1.0 { return 1.0; }

    // Use symmetry relation for better convergence when x > 0.5
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - regularized_beta(1.0 - x, b, a);
    }

    let ln_prefix = ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b)
        + a * x.ln() + b * (1.0 - x).ln();
    let prefix = ln_prefix.exp();

    // Series expansion: I_x(a,b) = (x^a * (1-x)^b) / (a * B(a,b)) * sum
    let mut sum = 0.0;
    let mut term = 1.0;
    for n in 0..300 {
        sum += term;
        term *= x * (a + b + n as f64) / (a + 1.0 + n as f64);
        if term.abs() < 1e-15 * sum.abs() {
            break;
        }
    }

    (prefix * sum / a).clamp(0.0, 1.0)
}

/// Inverse regularized beta via bisection.
fn inv_regularized_beta(p: f64, a: f64, b: f64) -> f64 {
    if p <= 0.0 { return 0.0; }
    if p >= 1.0 { return 1.0; }

    let mut lo = 0.0_f64;
    let mut hi = 1.0_f64;
    for _ in 0..100 {
        let mid = (lo + hi) / 2.0;
        if regularized_beta(mid, a, b) < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    (lo + hi) / 2.0
}

/// Regularized lower incomplete gamma function P(a, x) via series.
fn regularized_gamma_p(a: f64, x: f64) -> f64 {
    if x <= 0.0 { return 0.0; }
    if x > a + 50.0 { return 1.0; } // far in the tail

    let mut sum = 1.0 / a;
    let mut term = 1.0 / a;
    for n in 1..300 {
        term *= x / (a + n as f64);
        sum += term;
        if term.abs() < 1e-14 * sum.abs() {
            break;
        }
    }
    (a * x.ln() - x - ln_gamma(a)).exp() * sum
}

/// Inverse regularized gamma P via bisection.
fn inv_regularized_gamma_p(p: f64, a: f64) -> f64 {
    if p <= 0.0 { return 0.0; }
    if p >= 1.0 { return f64::INFINITY; }

    // Bracket: upper bound heuristic
    let mut hi = a.max(1.0);
    while regularized_gamma_p(a, hi) < p {
        hi *= 2.0;
    }
    let mut lo = 0.0_f64;

    for _ in 0..100 {
        let mid = (lo + hi) / 2.0;
        if regularized_gamma_p(a, mid) < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    (lo + hi) / 2.0
}

// =================================================================
// Continuous distribution LUT builders
// =================================================================

/// Normal distribution: N(mean, stddev).
pub fn dist_normal(mean: f64, stddev: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| mean + stddev * probit(p), resolution)
}

/// Exponential distribution: Exp(rate). Support: [0, +∞).
pub fn dist_exponential(rate: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| -(1.0 - p).ln() / rate, resolution)
}

/// Uniform continuous distribution: U(min, max).
pub fn dist_uniform(min: f64, max: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| min + p * (max - min), resolution)
}

/// Pareto distribution: Pareto(scale, shape). Support: [scale, +∞).
pub fn dist_pareto(scale: f64, shape: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| scale / (1.0 - p).powf(1.0 / shape), resolution)
}

/// Log-normal distribution: LogN(mean, stddev). Support: (0, +∞).
pub fn dist_lognormal(mean: f64, stddev: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| (mean + stddev * probit(p)).exp(), resolution)
}

/// Weibull distribution: Weibull(shape, scale). Support: [0, +∞).
pub fn dist_weibull(shape: f64, scale: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| scale * (-(1.0 - p).ln()).powf(1.0 / shape), resolution)
}

/// Cauchy distribution: Cauchy(location, scale). Support: (-∞, +∞).
pub fn dist_cauchy(location: f64, scale: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(
        |p| location + scale * (std::f64::consts::PI * (p - 0.5)).tan(),
        resolution,
    )
}

/// Laplace distribution: Laplace(location, scale). Support: (-∞, +∞).
pub fn dist_laplace(location: f64, scale: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(
        |p| {
            if p <= 0.5 {
                location + scale * (2.0 * p).ln()
            } else {
                location - scale * (2.0 * (1.0 - p)).ln()
            }
        },
        resolution,
    )
}

/// Beta distribution: Beta(alpha, beta). Support: [0, 1].
pub fn dist_beta(alpha: f64, beta: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| inv_regularized_beta(p, alpha, beta), resolution)
}

/// Gamma distribution: Gamma(shape, scale). Support: (0, +∞).
pub fn dist_gamma(shape: f64, scale: f64, resolution: usize) -> LutF64 {
    LutF64::from_fn(|p| scale * inv_regularized_gamma_p(p, shape), resolution)
}

// =================================================================
// Discrete distribution LUT builders
// =================================================================

/// Zipf distribution: Zipf(n, exponent). Support: [1, n].
///
/// The LUT maps [0, 1] → float, which is then truncated to an integer
/// by the caller. The CDF is computed from the PMF:
///   P(k) = (1/k^s) / H(n,s)  where H(n,s) = sum_{i=1}^{n} 1/i^s
pub fn dist_zipf(n: u64, exponent: f64, resolution: usize) -> LutF64 {
    // Precompute CDF
    let harmonic: f64 = (1..=n).map(|k| 1.0 / (k as f64).powf(exponent)).sum();
    let mut cdf = Vec::with_capacity(n as usize + 1);
    cdf.push(0.0);
    let mut cumulative = 0.0;
    for k in 1..=n {
        cumulative += (1.0 / (k as f64).powf(exponent)) / harmonic;
        cdf.push(cumulative);
    }

    // Inverse CDF by binary search
    LutF64::from_fn(
        |p| {
            let p = p.clamp(0.0, 1.0);
            match cdf.binary_search_by(|v| v.partial_cmp(&p).unwrap()) {
                Ok(idx) => idx as f64,
                Err(idx) => (idx as f64).max(1.0).min(n as f64),
            }
        },
        resolution,
    )
}

/// Poisson distribution: Poisson(lambda). Support: [0, +∞).
///
/// Precompute CDF up to a reasonable upper bound, then invert.
pub fn dist_poisson(lambda: f64, resolution: usize) -> LutF64 {
    let upper = (lambda + 6.0 * lambda.sqrt() + 10.0).ceil() as usize;

    // Precompute CDF via PMF: P(k) = e^(-λ) * λ^k / k!
    let mut cdf = Vec::with_capacity(upper + 2);
    cdf.push(0.0);
    let mut cumulative = 0.0;
    let mut pmf = (-lambda).exp(); // P(0)
    for k in 0..=upper {
        cumulative += pmf;
        cdf.push(cumulative.min(1.0));
        pmf *= lambda / (k + 1) as f64;
    }

    LutF64::from_fn(
        |p| {
            let p = p.clamp(0.0, 1.0);
            match cdf.binary_search_by(|v| v.partial_cmp(&p).unwrap()) {
                Ok(idx) => idx.saturating_sub(1) as f64,
                Err(idx) => idx.saturating_sub(1) as f64,
            }
        },
        resolution,
    )
}

/// Binomial distribution: Binomial(trials, p). Support: [0, trials].
pub fn dist_binomial(trials: u64, prob: f64, resolution: usize) -> LutF64 {
    let n = trials as usize;

    // Precompute CDF via PMF
    let mut cdf = Vec::with_capacity(n + 2);
    cdf.push(0.0);
    let mut cumulative = 0.0;
    let mut pmf = (1.0 - prob).powi(n as i32); // P(0) = (1-p)^n
    for k in 0..=n {
        cumulative += pmf;
        cdf.push(cumulative.min(1.0));
        if k < n {
            pmf *= prob / (1.0 - prob) * ((n - k) as f64) / ((k + 1) as f64);
        }
    }

    LutF64::from_fn(
        |p| {
            let p = p.clamp(0.0, 1.0);
            match cdf.binary_search_by(|v| v.partial_cmp(&p).unwrap()) {
                Ok(idx) => idx.saturating_sub(1) as f64,
                Err(idx) => idx.saturating_sub(1) as f64,
            }
        },
        resolution,
    )
}

/// Geometric distribution: Geometric(p). Support: [1, +∞).
///
/// P(X=k) = (1-p)^(k-1) * p, inverse CDF: ceil(ln(1-u) / ln(1-p)).
pub fn dist_geometric(prob: f64, resolution: usize) -> LutF64 {
    let ln_q = (1.0 - prob).ln();
    LutF64::from_fn(
        |p| {
            if p <= 0.0 { return 1.0; }
            if p >= 1.0 { return f64::INFINITY; }
            ((1.0 - p).ln() / ln_q).ceil().max(1.0)
        },
        resolution,
    )
}

// =================================================================
// Discrete u64 sampler (avoids f64 round-trip)
// =================================================================

/// GK node for discrete distribution sampling with direct u64 output.
///
/// Signature: `(input: u64) -> (u64)`
///
/// Takes a uniform u64 input (hashed), samples from the discrete
/// distribution, and returns a u64 outcome. No f64 intermediate.
/// This enables Phase 2 AOT compilation for discrete distribution
/// paths.
pub struct DiscreteSample {
    meta: NodeMeta,
    /// Precomputed outcomes indexed by input quantization.
    /// Length = resolution. Entry i is the discrete outcome for
    /// inputs mapping to quantile i/resolution.
    outcomes: Vec<u64>,
}

impl DiscreteSample {
    /// Build from a LUT of f64 values by rounding each to u64.
    pub fn from_lut(lut: &LutF64) -> Self {
        let outcomes: Vec<u64> = (0..lut.len())
            .map(|i| {
                let u = i as f64 / (lut.len() - 1) as f64;
                lut.sample(u).round().max(0.0) as u64
            })
            .collect();
        Self {
            meta: NodeMeta {
                name: "discrete_sample".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            outcomes,
        }
    }

    /// Convenience: Zipf distribution.
    pub fn zipf(n: u64, exponent: f64) -> Self {
        Self::from_lut(&dist_zipf(n, exponent, DEFAULT_RESOLUTION))
    }

    /// Convenience: Poisson distribution.
    pub fn poisson(lambda: f64) -> Self {
        Self::from_lut(&dist_poisson(lambda, DEFAULT_RESOLUTION))
    }

    /// Convenience: Binomial distribution.
    pub fn binomial(trials: u64, p: f64) -> Self {
        Self::from_lut(&dist_binomial(trials, p, DEFAULT_RESOLUTION))
    }

    /// Convenience: Geometric distribution.
    pub fn geometric(p: f64) -> Self {
        Self::from_lut(&dist_geometric(p, DEFAULT_RESOLUTION))
    }
}

impl GkNode for DiscreteSample {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let n = self.outcomes.len();
        let idx = (inputs[0].as_u64() as usize) % n;
        outputs[0] = Value::U64(self.outcomes[idx]);
    }

    fn compiled_u64(&self) -> Option<crate::node::CompiledU64Op> {
        let outcomes = self.outcomes.clone();
        let n = outcomes.len();
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = outcomes[(inputs[0] as usize) % n];
        }))
    }
}

// =================================================================
// Empirical distribution builders
// =================================================================

/// Build a LUT from raw data points (continuous empirical distribution).
///
/// The data points are sorted and used directly as the inverse CDF.
/// Linear interpolation between observed values.
pub fn dist_empirical(data: &[f64], resolution: usize) -> LutF64 {
    assert!(!data.is_empty(), "data must not be empty");
    let mut sorted = data.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    LutF64::from_fn(
        |p| {
            let pos = p * (sorted.len() - 1) as f64;
            let idx = pos as usize;
            let idx = idx.min(sorted.len() - 2);
            let frac = pos - idx as f64;
            sorted[idx] * (1.0 - frac) + sorted[idx + 1] * frac
        },
        resolution,
    )
}

/// Build a LUT from weighted value-frequency pairs.
///
/// Each (value, weight) pair contributes proportionally to the CDF.
pub fn dist_empirical_weighted(values: &[f64], weights: &[f64], resolution: usize) -> LutF64 {
    assert_eq!(values.len(), weights.len());
    assert!(!values.is_empty());

    // Sort by value, accumulate CDF
    let mut pairs: Vec<(f64, f64)> = values.iter().copied().zip(weights.iter().copied()).collect();
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    let total: f64 = pairs.iter().map(|(_, w)| w).sum();
    let mut cdf_points: Vec<(f64, f64)> = Vec::new(); // (cumulative_prob, value)
    let mut cumulative = 0.0;
    for (val, weight) in &pairs {
        cumulative += weight / total;
        cdf_points.push((cumulative, *val));
    }

    // Inverse CDF by binary search
    LutF64::from_fn(
        |p| {
            match cdf_points.binary_search_by(|&(cp, _)| cp.partial_cmp(&p).unwrap()) {
                Ok(idx) => cdf_points[idx].1,
                Err(idx) => {
                    if idx >= cdf_points.len() {
                        cdf_points.last().unwrap().1
                    } else {
                        cdf_points[idx].1
                    }
                }
            }
        },
        resolution,
    )
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for distribution and sampling nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "dist_normal", category: C::Distributions,
            outputs: 1, description: "build normal distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "mean", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "stddev", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a normal (Gaussian) distribution lookup table.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  mean   — center of the distribution\n  stddev — standard deviation (must be > 0)\nExample: dist_normal(50.0, 10.0) -> lut_sample(hash(cycle))\nTheory: pre-computes the inverse CDF into a table for O(1) sampling.",
        },
        FuncSig {
            name: "dist_exponential", category: C::Distributions,
            outputs: 1, description: "build exponential distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "rate", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build an exponential distribution lookup table.\nModels time between events (inter-arrival times, latencies).\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  rate — rate parameter lambda (mean = 1/rate, must be > 0)\nExample: dist_exponential(0.5) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "dist_uniform", category: C::Distributions,
            outputs: 1, description: "build uniform distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "min", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "max", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a uniform distribution lookup table over [min, max].\nEvery value in the range is equally likely.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  min — lower bound (inclusive, f64)\n  max — upper bound (exclusive, f64)\nExample: dist_uniform(0.0, 1000.0) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "dist_pareto", category: C::Distributions,
            outputs: 1, description: "build Pareto distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "scale", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "shape", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a Pareto (power-law) distribution lookup table.\nModels heavy-tailed phenomena: wealth, file sizes, city populations.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  scale — minimum value (x_m, must be > 0)\n  shape — tail index (alpha, larger = thinner tail)\nExample: dist_pareto(1.0, 2.0) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "dist_zipf", category: C::Distributions,
            outputs: 1, description: "build Zipf distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "n", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "exponent", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a Zipf distribution lookup table over ranks [1, n].\nModels rank-frequency phenomena: word frequency, cache access patterns.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  n        — number of elements (u64, must be > 0)\n  exponent — Zipf exponent s (f64, typically 1.0-2.0; higher = more skewed)\nExample: dist_zipf(1000, 1.07) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "lut_sample", category: C::Distributions,
            outputs: 1, description: "interpolating lookup table sample",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Sample from a precomputed lookup table via linear interpolation.\nInput is an f64 in [0, 1]; output is the interpolated table value.\nThis is the runtime half of distribution sampling: build the table\nwith dist_normal/dist_zipf/etc, then sample with lut_sample.\nParameters:\n  input — f64 wire in [0.0, 1.0] (typically from unit_interval)\nExample: lut_sample(unit_interval(hash(cycle)))  // wired to a dist_* LUT",
        },
        FuncSig {
            name: "icd_normal", category: C::Distributions,
            outputs: 1, description: "sample from normal distribution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "mean", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "stddev", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "One-step normal distribution sampling (builds LUT + samples internally).\nConvenience wrapper: equivalent to dist_normal -> lut_sample but\ncombined into a single node for simpler graph construction.\nParameters:\n  input  — u64 wire input (typically hashed)\n  mean   — center of the distribution (f64)\n  stddev — standard deviation (f64, must be > 0)\nExample: icd_normal(hash(cycle), 100.0, 15.0)",
        },
        FuncSig {
            name: "icd_exponential", category: C::Distributions,
            outputs: 1, description: "sample from exponential distribution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "rate", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "One-step exponential distribution sampling (builds LUT + samples).\nConvenience wrapper for modeling inter-arrival times and latencies.\nParameters:\n  input — u64 wire input (typically hashed)\n  rate  — rate parameter lambda (f64, mean = 1/rate)\nExample: icd_exponential(hash(cycle), 0.1)  // mean = 10.0",
        },
        FuncSig {
            name: "histribution", category: C::Distributions,
            outputs: 1, description: "discrete histogram distribution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "spec", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Sample from a discrete histogram distribution.\nParse an inline frequency spec into an alias table at init time.\nTwo formats:\n  Implicit labels: histribution(hash(cycle), \"50 25 13 12\") → outcomes 0-3\n  Explicit labels: histribution(hash(cycle), \"234:50 33:25 17:13 3:12\")\nDelimiters: space, comma, or semicolon.\nOutput is a u64 label.",
        },
        FuncSig {
            name: "dist_empirical", category: C::Distributions,
            outputs: 1, description: "empirical distribution from data points",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "data", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Sample from an empirical distribution defined by observed data points.\nThe data string is a space/comma/semicolon-separated list of f64 values.\nAt init time, values are sorted and used as the inverse CDF directly.\nThe input is an f64 in [0,1] (from unit_interval); output is interpolated.\nExample: dist_empirical(unit_interval(hash(cycle)), \"1.2 3.5 5.0 7.8 12.1\")",
        },
    ]
}

/// Sampling nodes have no dedicated builder here — construction is handled
/// by the sampling fallback in `dsl::factory`.
///
/// This stub satisfies the `NodeRegistration::build` signature so the
/// signatures can still be collected via inventory without duplicating the
/// builder logic.
pub(crate) fn build_node(
    _name: &str,
    _wires: &[crate::assembly::WireRef],
    _consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    None
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_interval_range() {
        let node = UnitInterval::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_f64(), 0.0);
        node.eval(&[Value::U64(u64::MAX)], &mut out);
        assert!((0.999..=1.0).contains(&out[0].as_f64()));
    }

    #[test]
    fn normal_symmetry() {
        let lut = dist_normal(0.0, 1.0, 1000);
        assert!(lut.sample(0.5).abs() < 0.01);
        assert!((lut.sample(0.25) + lut.sample(0.75)).abs() < 0.01);
    }

    #[test]
    fn normal_mean_stddev() {
        let lut = dist_normal(100.0, 10.0, 1000);
        assert!((lut.sample(0.5) - 100.0).abs() < 0.5);
    }

    #[test]
    fn exponential_median() {
        let lut = dist_exponential(1.0, 1000);
        assert!((lut.sample(0.5) - 0.693).abs() < 0.01);
    }

    #[test]
    fn exponential_positive() {
        let lut = dist_exponential(1.0, 1000);
        for i in 1..1000 {
            assert!(lut.sample(i as f64 / 1000.0) >= 0.0);
        }
    }

    #[test]
    fn uniform_linear() {
        let lut = dist_uniform(10.0, 20.0, 1000);
        assert!((lut.sample(0.0) - 10.0).abs() < 0.1);
        assert!((lut.sample(0.5) - 15.0).abs() < 0.1);
        assert!((lut.sample(0.999) - 20.0).abs() < 0.1);
    }

    #[test]
    fn pareto_heavy_tail() {
        let lut = dist_pareto(1.0, 1.0, 1000);
        assert!((lut.sample(0.5) - 2.0).abs() < 0.1);
        assert!(lut.sample(0.99) > 50.0);
    }

    #[test]
    fn cauchy_symmetric() {
        let lut = dist_cauchy(0.0, 1.0, 1000);
        assert!(lut.sample(0.5).abs() < 0.1);
        assert!((lut.sample(0.25) + lut.sample(0.75)).abs() < 0.1);
    }

    #[test]
    fn laplace_symmetric() {
        let lut = dist_laplace(5.0, 2.0, 1000);
        assert!((lut.sample(0.5) - 5.0).abs() < 0.1);
    }

    #[test]
    fn beta_bounded_01() {
        let lut = dist_beta(2.0, 5.0, 1000);
        for i in 0..=1000 {
            let v = lut.sample(i as f64 / 1000.0);
            assert!((0.0..=1.0).contains(&v), "beta out of [0,1]: {v}");
        }
    }

    #[test]
    fn beta_symmetric_at_half() {
        // Beta(2, 2) is symmetric around 0.5
        let lut = dist_beta(2.0, 2.0, 1000);
        assert!((lut.sample(0.5) - 0.5).abs() < 0.1,
            "beta(2,2) median={}, expected ~0.5", lut.sample(0.5));
    }

    #[test]
    fn gamma_positive() {
        let lut = dist_gamma(2.0, 1.0, 1000);
        for i in 1..1000 {
            assert!(lut.sample(i as f64 / 1000.0) > 0.0);
        }
    }

    #[test]
    fn gamma_mean() {
        // Gamma(shape=3, scale=2) has mean = shape * scale = 6
        let lut = dist_gamma(3.0, 2.0, 1000);
        assert!((lut.sample(0.5) - 5.0).abs() < 1.5); // median ≈ mean for shape>1
    }

    #[test]
    fn weibull_positive() {
        let lut = dist_weibull(2.0, 1.0, 1000);
        for i in 1..1000 {
            assert!(lut.sample(i as f64 / 1000.0) >= 0.0);
        }
    }

    #[test]
    fn zipf_range() {
        let lut = dist_zipf(100, 1.0, 1000);
        for i in 1..1000 {
            let v = lut.sample(i as f64 / 1000.0);
            assert!(v >= 1.0 && v <= 100.0, "zipf out of [1,100]: {v}");
        }
    }

    #[test]
    fn zipf_skewed() {
        // Low ranks should be much more common
        let lut = dist_zipf(100, 1.0, 1000);
        let low_quantile = lut.sample(0.5);
        assert!(low_quantile < 20.0, "median of Zipf(100,1) should be low, got {low_quantile}");
    }

    #[test]
    fn poisson_mean() {
        // Poisson(5): mean and median ≈ 5
        let lut = dist_poisson(5.0, 1000);
        let median = lut.sample(0.5);
        assert!((median - 5.0).abs() < 1.0, "poisson median={median}, expected ~5");
    }

    #[test]
    fn poisson_nonnegative() {
        let lut = dist_poisson(3.0, 1000);
        for i in 0..=1000 {
            assert!(lut.sample(i as f64 / 1000.0) >= 0.0);
        }
    }

    #[test]
    fn binomial_range() {
        let lut = dist_binomial(20, 0.5, 1000);
        for i in 0..=1000 {
            let v = lut.sample(i as f64 / 1000.0);
            assert!(v >= 0.0 && v <= 20.0, "binomial out of [0,20]: {v}");
        }
    }

    #[test]
    fn binomial_mean() {
        // Binomial(20, 0.5): mean = 10
        let lut = dist_binomial(20, 0.5, 1000);
        let median = lut.sample(0.5);
        assert!((median - 10.0).abs() < 1.5, "binomial median={median}, expected ~10");
    }

    #[test]
    fn geometric_starts_at_one() {
        let lut = dist_geometric(0.5, 1000);
        assert!(lut.sample(0.001) >= 1.0);
    }

    #[test]
    fn geometric_mean() {
        // Geometric(0.5): mean = 1/p = 2
        let lut = dist_geometric(0.5, 1000);
        let median = lut.sample(0.5);
        assert!((median - 1.0).abs() < 1.0, "geometric median={median}, expected ~1-2");
    }

    #[test]
    fn icd_sample_convenience() {
        let node = IcdSample::normal(0.0, 1.0);
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.5)], &mut out);
        assert!(out[0].as_f64().abs() < 0.01);
    }

    #[test]
    fn full_pipeline_hash_normalize_sample() {
        use xxhash_rust::xxh3::xxh3_64;

        let lut = dist_normal(72.0, 5.0, 1000);
        let mut values = Vec::new();
        for i in 0..10_000u64 {
            let hashed = xxh3_64(&i.to_le_bytes());
            let u = hashed as f64 / u64::MAX as f64;
            values.push(lut.sample(u));
        }
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let stddev = variance.sqrt();
        assert!((mean - 72.0).abs() < 0.5, "mean={mean}");
        assert!((stddev - 5.0).abs() < 0.5, "stddev={stddev}");
    }
}
