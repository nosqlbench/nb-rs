// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `PredicateInfo` and supporting enums — spec §10.9.3.
//!
//! Five independent assertions the analyzer makes about a
//! predicate:
//!
//! - `factorization` — how the predicate decomposes by
//!   coordinate.
//! - `monotonicity` — per-axis direction of truth.
//! - `range_constraint` — per-axis value-bound implications.
//! - `determinism` — whether the predicate is referentially
//!   transparent.
//! - `coords_referenced` — the set of `{name}` references in
//!   the predicate text.
//!
//! All fields are independent — a predicate may have rich
//! factorization but no monotonicity, etc.

use serde::{Deserialize, Serialize};

/// Structured analysis output for one predicate.
///
/// Construction is the analyzer's responsibility; consumers
/// (R5 et al.) only read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PredicateInfo {
    /// How the predicate decomposes by coordinate.
    pub factorization: Factorization,

    /// Per-axis monotonicity assertions. Missing axes have no
    /// monotonicity claim.
    pub monotonicity: PerAxisMap<Monotonicity>,

    /// Per-axis range constraint implied by the predicate.
    /// Missing axes have no constraint claim.
    pub range_constraint: PerAxisMap<RangeConstraint>,

    /// Whether the predicate is referentially transparent.
    pub determinism: Determinism,

    /// Names referenced by `{name}` interpolations in the
    /// predicate text. Coords NOT in the wrapped
    /// comprehension's coordinate set may still appear here
    /// (parent-scope references — the link-time consumer
    /// handles them per V3).
    pub coords_referenced: Vec<String>,
}

/// Per-coordinate decomposition of a predicate.
///
/// Spec §10.9.3 defines the four variants. `Opaque` carries
/// an [`OpaqueReason`] explaining why the analyzer couldn't
/// decompose further.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Factorization {
    /// Predicate factorizes per-axis: `p ≡ p_a({a}) && p_b({b}) && …`
    /// Each entry is the per-axis sub-predicate (as a string,
    /// matching the input predicate's lexical form).
    PerAxis(PerAxisMap<String>),

    /// Conjunction of sub-predicates where each may still
    /// cross-cut multiple axes. R5 can fire partially on the
    /// per-axis subset.
    Conjunctive(Vec<String>),

    /// Disjunction of sub-predicates. R5 fires only if every
    /// disjunct is `PerAxis` itself (per spec §10.9.5's
    /// disjunction rule).
    Disjunctive(Vec<String>),

    /// Analyzer can't structurally decompose. R5 doesn't fire.
    Opaque(OpaqueReason),
}

/// Why the analyzer marked a predicate `Opaque`. Spec §10.9.3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpaqueReason {
    /// Predicate shape isn't in the §10.9.5 recognizer
    /// catalog.
    UnknownPattern,

    /// Predicate references a non-deterministic Polydat function
    /// (PRNG draw, time-of-day, etc.). Detected
    /// conservatively — any function call we don't recognize
    /// as deterministic falls here.
    NonDeterministic,

    /// Predicate depends on previously-emitted tuples. Not
    /// expressible in current GK; reserved for future use.
    CrossTupleState,

    /// Predicate has observable side effects.
    SideEffecting,

    /// Predicate references one or more continuous-cardinality
    /// coordinates. Continuous-coord predicate analysis is
    /// deliberately deferred per spec §14.
    Continuous,
}

/// Per-axis monotonicity direction. "Increasing" means: once
/// the predicate becomes true for `axis = k`, it stays true
/// for all `axis ≥ k`. Used by the deferred R10
/// (monotonic-cutoff truncation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Monotonicity {
    Increasing,
    Decreasing,
    None,
}

/// Per-axis value-bound implied by the predicate. Used by the
/// deferred R8 (range-narrowing) and R9 (discrete-set
/// substitution).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RangeConstraint {
    /// `lo ≤ axis ≤ hi` (with open/closed flags).
    Bounded {
        lo: Option<ConstValue>,
        hi: Option<ConstValue>,
        lo_inclusive: bool,
        hi_inclusive: bool,
    },
    /// `axis ∈ {v_1, v_2, …}` (e.g., from an `in` predicate).
    Discrete(Vec<ConstValue>),
    /// No constraint asserted for this axis.
    None,
}

/// Whether a predicate is referentially transparent. Same
/// `(predicate, coords)` always produces the same boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Determinism {
    Deterministic,
    Opaque,
}

/// Constant value used inside `RangeConstraint::Bounded` / `Discrete`.
/// Subset of polydat's `Value` sufficient for the initial
/// recognizer catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConstValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

/// Map keyed by coordinate name. Insertion order preserves
/// declaration order so downstream consumers can iterate
/// per-axis in the comprehension's tuple-shape order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerAxisMap<T> {
    entries: Vec<(String, T)>,
}

impl<T> PerAxisMap<T> {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn insert<K: Into<String>>(&mut self, key: K, value: T) {
        let key = key.into();
        // Replace if already present; preserves position.
        if let Some(slot) = self.entries.iter_mut().find(|(k, _)| *k == key) {
            slot.1 = value;
        } else {
            self.entries.push((key, value));
        }
    }

    pub fn get(&self, key: &str) -> Option<&T> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &T)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(k, _)| k.as_str())
    }
}

impl<T> Default for PerAxisMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> FromIterator<(String, T)> for PerAxisMap<T> {
    fn from_iter<I: IntoIterator<Item = (String, T)>>(iter: I) -> Self {
        let mut m = Self::new();
        for (k, v) in iter {
            m.insert(k, v);
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_axis_map_insertion_order() {
        let mut m: PerAxisMap<i64> = PerAxisMap::new();
        m.insert("k", 10);
        m.insert("limit", 20);
        let keys: Vec<&str> = m.keys().collect();
        assert_eq!(keys, vec!["k", "limit"]);
    }

    #[test]
    fn per_axis_map_replace_preserves_position() {
        let mut m: PerAxisMap<i64> = PerAxisMap::new();
        m.insert("k", 10);
        m.insert("limit", 20);
        m.insert("k", 100); // replace
        let keys: Vec<&str> = m.keys().collect();
        assert_eq!(keys, vec!["k", "limit"]);
        assert_eq!(*m.get("k").unwrap(), 100);
    }

    #[test]
    fn predicate_info_round_trip_serde() {
        let info = PredicateInfo {
            factorization: Factorization::PerAxis(
                vec![
                    ("k".to_string(), "{k} > 0".to_string()),
                    ("limit".to_string(), "{limit} < 100".to_string()),
                ]
                .into_iter()
                .collect(),
            ),
            monotonicity: PerAxisMap::new(),
            range_constraint: PerAxisMap::new(),
            determinism: Determinism::Deterministic,
            coords_referenced: vec!["k".to_string(), "limit".to_string()],
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: PredicateInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }

    #[test]
    fn opaque_reason_serde() {
        let info = PredicateInfo {
            factorization: Factorization::Opaque(OpaqueReason::Continuous),
            monotonicity: PerAxisMap::new(),
            range_constraint: PerAxisMap::new(),
            determinism: Determinism::Deterministic,
            coords_referenced: vec!["theta".to_string()],
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("continuous"));
        let back: PredicateInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }
}
