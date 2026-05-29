// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cardinality classes — spec §6.1.
//!
//! Six classes describe every comprehension's dispense count.
//! Three are discrete (`Bounded`, `BoundedAtMost`, `Unbounded`);
//! two are continuous-domain (`Continuous`, `ContinuousAtMost`);
//! one is hybrid (`Hybrid`). The class propagates through every
//! constructor per spec §6.1's table.

use serde::{Deserialize, Serialize};

/// Cardinality of a comprehension's dispense stream.
///
/// Six variants per spec §6.1:
///
/// - **Discrete classes** enumerate distinct tuples; the count
///   may be known exactly (`Bounded`), bounded above
///   (`BoundedAtMost`), or unknown (`Unbounded`).
/// - **Continuous classes** describe a measure-theoretic value
///   space; they cannot enumerate and must be sampled via an
///   enclosing `order(_, strategy, Some(n))` per V8.
/// - **Hybrid** is a cartesian whose children mix discrete and
///   continuous axes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CardinalityClass {
    /// Discrete, exactly `n` tuples.
    Bounded(u64),

    /// Discrete, between 0 and `n` tuples (post-filter).
    BoundedAtMost(u64),

    /// Discrete, no known upper bound (generator, live stream).
    Unbounded,

    /// Continuous source — bounded or unbounded real intervals
    /// with an integrable product measure. Sampled rather than
    /// enumerated; V8 requires an enclosing
    /// `order(_, strategy, Some(n))` before reaching a
    /// `PolyStreamer`.
    Continuous {
        intervals: Vec<Interval>,
        measure: ProductMeasure,
    },

    /// Filtered continuous source. Measure reduced by the
    /// predicate; still requires sampling.
    ContinuousAtMost {
        intervals: Vec<Interval>,
        measure_at_most: ProductMeasure,
    },

    /// Mixed discrete × continuous cartesian. The discrete part
    /// is enumerable; the continuous part needs sampling. V8
    /// applies to the continuous component.
    Hybrid(Hybrid),
}

/// Mixed discrete × continuous cartesian shape.
///
/// Each `discrete_axes` entry is the axis size in tuples; each
/// `continuous_axes` entry is the interval the axis spans.
/// `measure` covers the continuous part.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hybrid {
    /// Per-axis cardinality for the discrete axes, in
    /// declaration order.
    pub discrete_axes: Vec<u64>,
    /// Per-axis intervals for the continuous axes, in
    /// declaration order.
    pub continuous_axes: Vec<Interval>,
    /// Product measure over the continuous axes.
    pub measure: ProductMeasure,
}

/// Real interval `[lo, hi]` (or open variants) for continuous
/// sources. Unbounded sides use `f64::NEG_INFINITY` /
/// `f64::INFINITY`; the V8 integrability check determines
/// whether such intervals are valid given the measure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Interval {
    pub lo: f64,
    pub hi: f64,
    pub lo_open: bool,
    pub hi_open: bool,
}

impl Interval {
    /// Closed-closed interval `[lo, hi]`.
    pub fn closed(lo: f64, hi: f64) -> Self {
        Self { lo, hi, lo_open: false, hi_open: false }
    }

    /// Half-open `[lo, hi)`.
    pub fn half_open(lo: f64, hi: f64) -> Self {
        Self { lo, hi, lo_open: false, hi_open: true }
    }

    /// Open interval `(lo, hi)`.
    pub fn open(lo: f64, hi: f64) -> Self {
        Self { lo, hi, lo_open: true, hi_open: true }
    }

    /// `true` if the interval has finite Lebesgue measure
    /// (both endpoints finite). Used by V8's integrability
    /// check together with the measure variant.
    pub fn is_bounded(&self) -> bool {
        self.lo.is_finite() && self.hi.is_finite()
    }
}

/// Product measure over one or more continuous axes.
///
/// `Uniform` is the Lebesgue measure scaled by interval width
/// (requires bounded intervals; V8 rejects unbounded + Uniform).
/// `Named(D)` is a probability distribution with proper density
/// over its declared support — Normal, Exponential, Pareto,
/// Beta, etc. `Product(_)` carries a per-axis product of measures
/// for K-D continuous cartesians.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProductMeasure {
    Uniform,
    Named(MeasureName),
    Product(Vec<ProductMeasure>),
}

impl ProductMeasure {
    /// `true` if this measure has finite total mass given the
    /// supplied intervals. Used by V8.
    ///
    /// - `Uniform` is integrable iff every interval is bounded.
    /// - `Named(D)` is integrable per its distribution: proper
    ///   probability distributions are always integrable
    ///   (they have unit total mass by definition).
    /// - `Product(children)` is integrable iff every child is.
    pub fn is_integrable(&self, intervals: &[Interval]) -> bool {
        match self {
            ProductMeasure::Uniform => intervals.iter().all(Interval::is_bounded),
            ProductMeasure::Named(name) => name.is_proper_probability_measure(),
            ProductMeasure::Product(children) => {
                if children.len() != intervals.len() {
                    return false;
                }
                children
                    .iter()
                    .zip(intervals.iter())
                    .all(|(m, i)| m.is_integrable(std::slice::from_ref(i)))
            }
        }
    }
}

/// Named continuous distribution. Closed enum per spec
/// §10.7.5's "User-defined extensions" non-goal — new
/// distributions land as coordinated additions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MeasureName {
    Normal,
    Exponential,
    Pareto,
    Beta,
    LogNormal,
    Gamma,
    Uniform01,
}

impl MeasureName {
    /// All currently-named distributions are proper probability
    /// measures (unit total mass). V8 accepts them on any
    /// interval that matches the distribution's support.
    pub fn is_proper_probability_measure(self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_interval_is_bounded() {
        assert!(Interval::closed(0.0, 1.0).is_bounded());
        assert!(Interval::open(-1.0, 1.0).is_bounded());
    }

    #[test]
    fn unbounded_interval_is_not_bounded() {
        let i = Interval { lo: 0.0, hi: f64::INFINITY, lo_open: false, hi_open: true };
        assert!(!i.is_bounded());
    }

    #[test]
    fn uniform_integrable_on_bounded_interval() {
        let m = ProductMeasure::Uniform;
        assert!(m.is_integrable(&[Interval::closed(0.0, 1.0)]));
    }

    #[test]
    fn uniform_not_integrable_on_unbounded_interval() {
        let m = ProductMeasure::Uniform;
        let unbounded = Interval { lo: 0.0, hi: f64::INFINITY, lo_open: false, hi_open: true };
        assert!(!m.is_integrable(&[unbounded]));
    }

    #[test]
    fn named_measure_always_integrable() {
        let m = ProductMeasure::Named(MeasureName::Normal);
        let unbounded = Interval {
            lo: f64::NEG_INFINITY,
            hi: f64::INFINITY,
            lo_open: true,
            hi_open: true,
        };
        assert!(m.is_integrable(&[unbounded]));
    }

    #[test]
    fn product_measure_requires_matching_arity() {
        let m = ProductMeasure::Product(vec![ProductMeasure::Uniform, ProductMeasure::Uniform]);
        assert!(m.is_integrable(&[Interval::closed(0.0, 1.0), Interval::closed(0.0, 1.0)]));
        assert!(!m.is_integrable(&[Interval::closed(0.0, 1.0)]));
    }

    #[test]
    fn cardinality_class_round_trip_serde() {
        let c = CardinalityClass::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: CardinalityClass = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }
}
