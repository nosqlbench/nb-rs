// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`ChildName`] — structured identifier for a spawned child.
//!
//! Per SRD-67 §"Named-child registry": each parent records the
//! names it has spawned children under so duplicate spawn under
//! the same name is caught at the API boundary. Names are
//! `PathBuf`-shaped (hierarchical, comparable, debug-printable);
//! the runtime constructs them from workload scope-tree node
//! labels (phase / op-template / iteration coordinate).

use std::fmt;

/// Hierarchical identifier for a spawned sub-context.
///
/// Internally a slash-separated string of segments. Constructors
/// match common workload-tree shapes:
///
/// - [`ChildName::phase`] — `phase/<name>`
/// - [`ChildName::op`] — `op/<name>`
/// - [`ChildName::iteration`] — `iter/<coord>`
/// - [`ChildName::compose`] — append a segment under a parent name
///
/// Two names compare equal when their segment vectors are equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChildName {
    segments: Vec<String>,
}

impl ChildName {
    /// Create from raw segments. Used by tests / advanced callers
    /// that have a pre-built path; production code prefers the
    /// shape-specific constructors below.
    pub fn from_segments<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            segments: segments.into_iter().map(Into::into).collect(),
        }
    }

    /// `phase/<name>` — for a workload phase scope.
    pub fn phase(name: impl Into<String>) -> Self {
        Self {
            segments: vec!["phase".into(), name.into()],
        }
    }

    /// `op/<name>` — for an op-template scope under a phase.
    pub fn op(name: impl Into<String>) -> Self {
        Self {
            segments: vec!["op".into(), name.into()],
        }
    }

    /// `iter/<coord>` — for one iteration of a comprehension scope.
    /// `coord` is the coordinate-tuple debug rendering used by the
    /// scope-tree pre-walk.
    pub fn iteration(coord: impl Into<String>) -> Self {
        Self {
            segments: vec!["iter".into(), coord.into()],
        }
    }

    /// Compose: append `segment` to `parent_name`'s path.
    pub fn compose(parent_name: &ChildName, segment: impl Into<String>) -> Self {
        let mut segments = parent_name.segments.clone();
        segments.push(segment.into());
        Self { segments }
    }

    /// Borrow the segment list.
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Render as a slash-joined path for diagnostics.
    pub fn display(&self) -> String {
        self.segments.join("/")
    }
}

impl fmt::Display for ChildName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display())
    }
}
