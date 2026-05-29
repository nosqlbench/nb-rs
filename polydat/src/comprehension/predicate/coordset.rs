// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Coordinate set with per-coord classification — spec §10.9.2.
//!
//! The predicate analyzer takes a `CoordSet` (not a bare list
//! of names) so it can detect continuous-coord references and
//! mark them `Opaque(Continuous)`. The set's per-coord kind
//! is supplied by the caller — typically derived from the
//! wrapped comprehension's metadata (`Metadata::index_addressable`
//! variant + cardinality classification).

use serde::{Deserialize, Serialize};

use crate::comprehension::metadata::{IndexFn, Metadata};

/// A coordinate's name plus its discrete/continuous
/// classification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoordInfo {
    pub name: String,
    pub kind: CoordKind,
}

/// Coordinate cardinality classification used by the
/// predicate analyzer. Mirrors the discrete-vs-continuous
/// split that drives `OpaqueReason::Continuous`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordKind {
    Discrete,
    Continuous,
}

/// Coordinate-name set with per-coord classification.
/// Preserves declaration order so the analyzer's output (and
/// downstream `R5`) sees axes in the same order they appear in
/// the wrapped comprehension's tuple shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoordSet {
    coords: Vec<CoordInfo>,
}

impl CoordSet {
    pub fn new() -> Self {
        Self { coords: Vec::new() }
    }

    pub fn push(&mut self, info: CoordInfo) {
        self.coords.push(info);
    }

    /// All-discrete coord set from a name list. Convenience
    /// for tests and discrete-only call sites.
    pub fn all_discrete<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            coords: names
                .into_iter()
                .map(|n| CoordInfo {
                    name: n.into(),
                    kind: CoordKind::Discrete,
                })
                .collect(),
        }
    }

    /// Build a `CoordSet` from a comprehension's coordinate
    /// names and its propagated metadata. The metadata's
    /// `index_addressable` variant determines whether each
    /// axis is discrete or continuous.
    ///
    /// For comprehensions with `None` `index_addressable`
    /// (raw filter output, dependent cartesian), every axis is
    /// classified as discrete — the conservative choice that
    /// keeps the analyzer running. Continuous classification
    /// requires a `Continuous` or `Hybrid` `IndexFn`, where
    /// the per-axis split is unambiguous.
    pub fn from_metadata(coord_names: &[String], metadata: &Metadata) -> Self {
        let kinds = classify_axes(metadata.index_addressable.as_ref(), coord_names.len());
        let coords = coord_names
            .iter()
            .zip(kinds)
            .map(|(name, kind)| CoordInfo {
                name: name.clone(),
                kind,
            })
            .collect();
        Self { coords }
    }

    pub fn iter(&self) -> impl Iterator<Item = &CoordInfo> {
        self.coords.iter()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.coords.iter().map(|c| c.name.as_str())
    }

    pub fn get(&self, name: &str) -> Option<&CoordInfo> {
        self.coords.iter().find(|c| c.name == name)
    }

    pub fn is_continuous(&self, name: &str) -> bool {
        matches!(
            self.get(name).map(|c| c.kind),
            Some(CoordKind::Continuous)
        )
    }

    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn len(&self) -> usize {
        self.coords.len()
    }

    pub fn is_empty(&self) -> bool {
        self.coords.is_empty()
    }
}

impl Default for CoordSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-axis classification given the input's `IndexFn`.
fn classify_axes(idx: Option<&IndexFn>, expected_count: usize) -> Vec<CoordKind> {
    match idx {
        None => vec![CoordKind::Discrete; expected_count],
        Some(IndexFn::Lattice { axis_sizes }) => {
            vec![CoordKind::Discrete; axis_sizes.len()]
        }
        Some(IndexFn::Lockstep { .. }) | Some(IndexFn::Modular { .. }) => {
            vec![CoordKind::Discrete; expected_count]
        }
        Some(IndexFn::Concatenation { .. }) => {
            vec![CoordKind::Discrete; expected_count]
        }
        Some(IndexFn::Continuous { intervals, .. }) => {
            vec![CoordKind::Continuous; intervals.len()]
        }
        Some(IndexFn::Hybrid {
            discrete_axes,
            continuous_axes,
            ..
        }) => {
            let mut kinds = vec![CoordKind::Discrete; discrete_axes.len()];
            kinds.extend(vec![CoordKind::Continuous; continuous_axes.len()]);
            kinds
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::cardinality::{
        CardinalityClass, Interval, ProductMeasure,
    };
    use crate::comprehension::metadata::{Materialization, NaturalOrder};

    fn dummy_metadata(idx: Option<IndexFn>) -> Metadata {
        Metadata {
            cardinality: CardinalityClass::Bounded(0),
            index_addressable: idx,
            natural_order: NaturalOrder::Lex,
            materialization: Materialization::Streaming,
        }
    }

    #[test]
    fn all_discrete_convenience() {
        let s = CoordSet::all_discrete(["k", "limit"]);
        assert_eq!(s.len(), 2);
        assert!(!s.is_continuous("k"));
        assert!(!s.is_continuous("limit"));
        assert!(s.contains("k"));
        assert!(!s.contains("missing"));
    }

    #[test]
    fn from_metadata_lattice_all_discrete() {
        let m = dummy_metadata(Some(IndexFn::Lattice { axis_sizes: vec![3, 4] }));
        let s = CoordSet::from_metadata(&["k".to_string(), "limit".to_string()], &m);
        assert_eq!(s.len(), 2);
        assert!(!s.is_continuous("k"));
        assert!(!s.is_continuous("limit"));
    }

    #[test]
    fn from_metadata_continuous_all_continuous() {
        let m = dummy_metadata(Some(IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0), Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        }));
        let s = CoordSet::from_metadata(&["alpha".to_string(), "beta".to_string()], &m);
        assert!(s.is_continuous("alpha"));
        assert!(s.is_continuous("beta"));
    }

    #[test]
    fn from_metadata_hybrid_per_axis_split() {
        let m = dummy_metadata(Some(IndexFn::Hybrid {
            discrete_axes: vec![5],
            continuous_axes: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        }));
        // Order in CoordSet must match: discrete axes first,
        // then continuous (matches metadata.rs's combine_cartesian_index_fn).
        let s = CoordSet::from_metadata(&["k".to_string(), "theta".to_string()], &m);
        assert!(!s.is_continuous("k"));
        assert!(s.is_continuous("theta"));
    }

    #[test]
    fn from_metadata_none_index_fn_all_discrete() {
        let m = dummy_metadata(None);
        let s = CoordSet::from_metadata(&["a".to_string(), "b".to_string()], &m);
        // Conservative — no Continuous classification when we
        // can't determine kinds.
        assert!(!s.is_continuous("a"));
        assert!(!s.is_continuous("b"));
    }
}
