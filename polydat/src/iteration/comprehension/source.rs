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

    /// Generator function call expressed as a Polydat source string.
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

// ── SRD-18f: iteration interior + string-comprehension striping ──

/// The canonical "is this value peelable, and into what?" predicate
/// (SRD-18f §4). Returns `Some(interior)` when `v` has an iteration
/// interior — peeling it one level yields these elements — and
/// `None` when `v` is an iteration scalar (relaxed wraps it; an
/// explicit `[v…]` destructure errors).
///
/// This is the single place the peel/wrap decision is made,
/// replacing the scattered per-type special cases (the
/// `as_partition_list()`-peel / `Ok(other)`-wrap arms) that used
/// to live in `eval::evaluate_spec_internal`.
///
/// `Value::Str` is iterable — its interior is its
/// **string-comprehension tokens** ([`strip_string_tokens`]). The
/// single-quoted *atomic* form is resolved earlier, at the
/// source-text layer (the parser produces a one-element literal),
/// so by the time a bare `Value::Str` reaches this predicate the
/// intent is "iterate it." A whole-string binding is reached via
/// the no-peel form `x in [s]`.
pub fn iteration_interior(v: &crate::ast::Value) -> Option<Vec<crate::ast::Value>> {
    use crate::ast::Value;
    match v {
        Value::VecF32(s) => Some(s.as_slice().iter().map(|x| Value::F64(*x as f64)).collect()),
        Value::VecF64(s) => Some(s.as_slice().iter().map(|x| Value::F64(*x)).collect()),
        Value::VecF16(s) => Some(s.as_slice().iter().map(|x| Value::F64(x.to_f64())).collect()),
        // Signed lanes peel to the honest signed carrier — a
        // VecI64 holding -5 iterates as I64(-5), not the unsigned
        // bit-reinterpretation (type_system_alignment.md §5).
        Value::VecI32(s) => Some(s.as_slice().iter().map(|x| Value::I64(*x as i64)).collect()),
        Value::VecI64(s) => Some(s.as_slice().iter().map(|x| Value::I64(*x)).collect()),
        Value::VecI16(s) => Some(s.as_slice().iter().map(|x| Value::I64(*x as i64)).collect()),
        Value::VecI8(s) => Some(s.as_slice().iter().map(|x| Value::I64(*x as i64)).collect()),
        // A JSON array peels to its elements (each carried as a
        // Json value); a non-array JSON is an iteration scalar.
        Value::Json(j) => j.as_array().map(|arr| {
            arr.iter().map(|e| Value::Json(std::sync::Arc::new(e.clone()))).collect()
        }),
        // Ext carrying a PartitionList peels into its partitions
        // (SRD-71). Other Ext values are opaque scalars.
        Value::Ext(_) => v.as_partition_list().map(|list| {
            list.as_slice().iter().map(|p| Value::from_partition(*p)).collect()
        }),
        // Lane-typed register views peel like the Vec* family
        // (a reg_f32x4 is a fixed-width typed vector); the Raw
        // view is an opaque buffer-state word — an iteration
        // scalar.
        Value::Reg128(b, view) => {
            use crate::ast::RegLanes;
            match view {
                RegLanes::Raw => None,
                RegLanes::I8x16 => Some(b.lanes_i8().iter().map(|x| Value::I64(*x as i64)).collect()),
                RegLanes::I16x8 => Some(b.lanes_i16().iter().map(|x| Value::I64(*x as i64)).collect()),
                RegLanes::I32x4 => Some(b.lanes_i32().iter().map(|x| Value::I64(*x as i64)).collect()),
                RegLanes::I64x2 => Some(b.lanes_i64().iter().map(|x| Value::I64(*x)).collect()),
                RegLanes::F16x8 => Some(b.lanes_f16().iter().map(|x| Value::F64(x.to_f64())).collect()),
                RegLanes::F32x4 => Some(b.lanes_f32().iter().map(|x| Value::F64(*x as f64)).collect()),
                RegLanes::F64x2 => Some(b.lanes_f64().iter().map(|x| Value::F64(*x)).collect()),
            }
        }
        // A string's interior is its comprehension tokens.
        Value::Str(s) => Some(strip_string_tokens(s)),
        // Iteration scalars — relaxed wraps, `[v…]` errors.
        Value::U64(_) | Value::I64(_) | Value::U128(_) | Value::I128(_)
        | Value::F64(_) | Value::Bool(_)
        | Value::Bytes(_) | Value::Handle(_) | Value::None => None,
    }
}

/// Split a string-comprehension source into its token values
/// (SRD-18f §3.2). Separators are runs of comma, semicolon, and
/// ASCII whitespace; every other character — notably `:` (k:v /
/// `a:b:c` tuples), `.` (floats), `-` (negatives / hyphenated
/// labels), `/` — stays inside the token. Each token is typed
/// like a literal-list element (u64 / f64 / bool / else Str), so
/// `"1, 2, 3"` yields numeric values and `"a, b"` yields strings.
///
/// A single-token string (`"OTHER"`) yields a one-element vec, so
/// the single-value case degenerates to the no-peel binding for
/// free.
pub fn strip_string_tokens(s: &str) -> Vec<crate::ast::Value> {
    use crate::ast::Value;
    split_string_comprehension(s)
        .into_iter()
        .map(|t| {
            if let Ok(n) = t.parse::<u64>() {
                Value::U64(n)
            } else if let Ok(f) = t.parse::<f64>() {
                Value::F64(f)
            } else if t == "true" {
                Value::Bool(true)
            } else if t == "false" {
                Value::Bool(false)
            } else {
                Value::Str(t.to_string().into())
            }
        })
        .collect()
}

/// The SRD-18f string-comprehension separator rule, in one place
/// so the parse-time (`source_parser`) and runtime (`eval`)
/// striping can never drift: split on runs of comma / semicolon /
/// ASCII whitespace, trim, drop empties. Every other character
/// (`:` `.` `-` `/` …) stays in the token. Returns the raw token
/// substrings; callers type them (Value or LiteralValue).
pub fn split_string_comprehension(s: &str) -> Vec<&str> {
    s.split(|c: char| c == ',' || c == ';' || c.is_ascii_whitespace())
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Value;

    #[test]
    fn string_strips_on_comma_semicolon_whitespace_retaining_colons() {
        // SRD-18f §3.2: separators are comma / semicolon / ws;
        // colons (and dots, dashes) stay in-token.
        let got = strip_string_tokens("a:1, b:2; c:3 d:4");
        assert_eq!(got, vec![
            Value::Str("a:1".into()), Value::Str("b:2".into()),
            Value::Str("c:3".into()), Value::Str("d:4".into()),
        ]);
    }

    #[test]
    fn string_tokens_are_typed_like_literals() {
        // Floats survive (dot retained), ints type as U64.
        assert_eq!(strip_string_tokens("1, 2, 3"),
            vec![Value::U64(1), Value::U64(2), Value::U64(3)]);
        assert_eq!(strip_string_tokens("1.5, 2.5"),
            vec![Value::F64(1.5), Value::F64(2.5)]);
    }

    #[test]
    fn single_token_string_degenerates_to_singleton() {
        assert_eq!(strip_string_tokens("OTHER"), vec![Value::Str("OTHER".into())]);
    }

    #[test]
    fn iteration_interior_string_is_its_tokens() {
        let v = Value::Str("x, y, z".into());
        assert_eq!(iteration_interior(&v),
            Some(vec![Value::Str("x".into()), Value::Str("y".into()), Value::Str("z".into())]));
    }

    #[test]
    fn iteration_interior_vector_peels_to_elements() {
        let v = Value::VecI32(crate::ast::SliceArc::from_vec(vec![10, 20, 30]));
        assert_eq!(iteration_interior(&v),
            Some(vec![Value::I64(10), Value::I64(20), Value::I64(30)]));
    }

    #[test]
    fn iteration_interior_signed_vector_peels_signed() {
        // The honest-I64 alignment fix: negative lane elements
        // iterate as their signed values, not the unsigned
        // bit-reinterpretation (type_system_alignment.md §5).
        let v = Value::VecI64(crate::ast::SliceArc::from_vec(vec![-5_i64, 7]));
        assert_eq!(iteration_interior(&v),
            Some(vec![Value::I64(-5), Value::I64(7)]));
    }

    #[test]
    fn iteration_interior_scalars_are_none() {
        assert_eq!(iteration_interior(&Value::U64(5)), None);
        assert_eq!(iteration_interior(&Value::F64(1.5)), None);
        assert_eq!(iteration_interior(&Value::Bool(true)), None);
    }

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
