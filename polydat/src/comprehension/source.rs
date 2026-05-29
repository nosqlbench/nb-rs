// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Clause source values — spec §3.1.
//!
//! A `clause(name, source)` binds a name to the values
//! produced by its source. Sources split into two families:
//!
//! - **Discrete stream producers** — literal lists, integer
//!   ranges, generator functions, workload-param references.
//!   Cardinality is `Bounded`, `BoundedAtMost`, or `Unbounded`.
//! - **Continuous measures** — real intervals with an
//!   integrable measure (uniform on bounded intervals; named
//!   probability distributions like Normal / Exponential).
//!   Cardinality is `Continuous`; V8 requires an enclosing
//!   sampling `order(_, strategy, Some(n))` before dispense.
//!
//! Sources are stream producers — they do not pre-materialize
//! into `Vec<Value>`. This is the load-bearing model property
//! per spec §3.1 + §6.2.

use serde::{Deserialize, Serialize};

use super::cardinality::{CardinalityClass, Interval, MeasureName, ProductMeasure};

/// A clause's source of values.
///
/// Discrete variants produce a stream of `Value` via the
/// runtime evaluator; continuous variants describe a measure
/// that a downstream sampling strategy will draw from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Source {
    /// Literal comma list (e.g., `[1, 2, 4, 8]`). Stream
    /// producer over the list contents.
    Literal { values: Vec<LiteralValue> },

    /// Integer half-open range `lo..hi` with optional step.
    /// Default step is 1.
    IntRange { lo: i64, hi: i64, step: i64 },

    /// Generator function call expressed as a GK source string.
    /// Resolved at clause construction; cardinality may be
    /// `Unbounded` if the generator is open-ended.
    Generator { expr: String, cardinality_hint: Option<u64> },

    /// Reference to a workload-level parameter that resolves to
    /// a list of values. Cardinality is the parameter's
    /// declared list length.
    WorkloadParamList { name: String, len_hint: Option<u64> },

    /// Real interval (continuous source). Combined with a
    /// `measure` to form a `Continuous` cardinality.
    /// Integrability is checked at parse via V8.
    ContinuousInterval { interval: Interval, measure: ProductMeasure },

    /// Named continuous distribution. The distribution carries
    /// its own support; the `support` field records the
    /// effective interval for V8's check.
    Distribution {
        distribution: MeasureName,
        support: Interval,
        params: Vec<f64>,
    },
}

/// A literal value carried in a `Source::Literal`. Subset of
/// the polydat `Value` type — the kinds clauses can directly
/// bind. Extension to richer value types lives in the source
/// evaluator, not the AST.
///
/// Serialized untagged because the variants are primitives;
/// the JSON/YAML representation is just the bare value
/// (`1` / `"x"` / `true` / `1.5`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl Source {
    /// Declare this source's cardinality class for use by
    /// `clause` metadata propagation.
    pub fn cardinality(&self) -> CardinalityClass {
        match self {
            Source::Literal { values } => CardinalityClass::Bounded(values.len() as u64),
            Source::IntRange { lo, hi, step } => {
                let step = (*step).max(1).unsigned_abs();
                if hi <= lo {
                    CardinalityClass::Bounded(0)
                } else {
                    let span = (hi - lo) as u64;
                    let n = span.div_ceil(step);
                    CardinalityClass::Bounded(n)
                }
            }
            Source::Generator { cardinality_hint, .. } => match cardinality_hint {
                Some(n) => CardinalityClass::Bounded(*n),
                None => CardinalityClass::Unbounded,
            },
            Source::WorkloadParamList { len_hint, .. } => match len_hint {
                Some(n) => CardinalityClass::Bounded(*n),
                None => CardinalityClass::Unbounded,
            },
            Source::ContinuousInterval { interval, measure } => CardinalityClass::Continuous {
                intervals: vec![interval.clone()],
                measure: measure.clone(),
            },
            Source::Distribution { support, .. } => CardinalityClass::Continuous {
                intervals: vec![support.clone()],
                measure: ProductMeasure::Named(*self.distribution_name()),
            },
        }
    }

    /// `true` if this source is continuous (Continuous /
    /// Distribution variants). Used by V7 (zip must be all
    /// discrete) and V9 (union must be all discrete) without
    /// a full cardinality computation.
    pub fn is_continuous(&self) -> bool {
        matches!(
            self,
            Source::ContinuousInterval { .. } | Source::Distribution { .. }
        )
    }

    /// `true` if this source is discrete (every variant except
    /// the continuous ones).
    pub fn is_discrete(&self) -> bool {
        !self.is_continuous()
    }

    fn distribution_name(&self) -> &MeasureName {
        match self {
            Source::Distribution { distribution, .. } => distribution,
            _ => panic!("distribution_name called on non-Distribution source"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_cardinality_is_list_length() {
        let s = Source::Literal {
            values: vec![LiteralValue::Int(1), LiteralValue::Int(2), LiteralValue::Int(3)],
        };
        assert!(matches!(s.cardinality(), CardinalityClass::Bounded(3)));
    }

    #[test]
    fn int_range_step_1() {
        let s = Source::IntRange { lo: 1, hi: 10, step: 1 };
        assert!(matches!(s.cardinality(), CardinalityClass::Bounded(9)));
    }

    #[test]
    fn int_range_with_step() {
        let s = Source::IntRange { lo: 0, hi: 10, step: 2 };
        // 0,2,4,6,8 = 5 values
        assert!(matches!(s.cardinality(), CardinalityClass::Bounded(5)));
    }

    #[test]
    fn int_range_empty() {
        let s = Source::IntRange { lo: 5, hi: 5, step: 1 };
        assert!(matches!(s.cardinality(), CardinalityClass::Bounded(0)));
    }

    #[test]
    fn generator_without_hint_is_unbounded() {
        let s = Source::Generator { expr: "live_query()".into(), cardinality_hint: None };
        assert!(matches!(s.cardinality(), CardinalityClass::Unbounded));
    }

    #[test]
    fn generator_with_hint_is_bounded() {
        let s = Source::Generator { expr: "first_100()".into(), cardinality_hint: Some(100) };
        assert!(matches!(s.cardinality(), CardinalityClass::Bounded(100)));
    }

    #[test]
    fn continuous_interval_produces_continuous_class() {
        let s = Source::ContinuousInterval {
            interval: Interval::closed(0.0, 1.0),
            measure: ProductMeasure::Uniform,
        };
        match s.cardinality() {
            CardinalityClass::Continuous { intervals, measure } => {
                assert_eq!(intervals.len(), 1);
                assert!(matches!(measure, ProductMeasure::Uniform));
            }
            other => panic!("expected Continuous, got {other:?}"),
        }
        assert!(s.is_continuous());
        assert!(!s.is_discrete());
    }

    #[test]
    fn distribution_source_classification() {
        let s = Source::Distribution {
            distribution: MeasureName::Normal,
            support: Interval { lo: f64::NEG_INFINITY, hi: f64::INFINITY, lo_open: true, hi_open: true },
            params: vec![0.0, 1.0],
        };
        assert!(s.is_continuous());
        match s.cardinality() {
            CardinalityClass::Continuous { measure: ProductMeasure::Named(MeasureName::Normal), .. } => {}
            other => panic!("expected Continuous with Named(Normal), got {other:?}"),
        }
    }
}
