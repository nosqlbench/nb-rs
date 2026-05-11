// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Scope coordinates — the formal GK-side model of the
//! iteration position a kernel occupies inside an enclosing
//! comprehension chain.
//!
//! ## Definition
//!
//! A **scope coordinate set** for a single scope is the
//! ordered name→value tuple of every iteration extern that
//! scope owns — i.e. variables the scope declared via
//! `extern <var>: <type>` and that aren't mirroring an outer
//! scope (`is_inherited` returns false in this scope's program).
//! The order is the declaration order from the comprehension's
//! source (preserved by `IndexMap`'s insertion semantics).
//!
//! A **scope coordinate path** is the leaf-first list of
//! coordinate sets, walking from the kernel's own scope up
//! through every enclosing comprehension scope. Workload-root
//! params (top-level `params:` in the document) don't
//! contribute — they're configuration, not iteration
//! coordinates.
//!
//! ## Invariant
//!
//! Every kernel that has been *initialised in its scope* —
//! either via [`super::GkKernel::materialize_wiring_from_outer`] (post-bind
//! the path is `[own] ++ outer.scope_coordinates()`), or as a
//! root scope (path is `[own]` if non-empty, else empty) —
//! has [`super::GkKernel::scope_coordinates`] populated. This
//! is treated as a structural invariant of the GK model, not
//! an optional add-on: the runtime contract is that any
//! consumer (presentation layer, inspector, future scope-aware
//! diagnostics) can call `scope_coordinates()` on an
//! initialised kernel and get the full path back without
//! needing to walk the scope tree itself.
//!
//! ## Use
//!
//! Presentation-layer consumers (the inline status line, TUI
//! phase rows, the inspector socket) render the path as
//! striated parens — `(leaf coords), (parent coords), …` —
//! so the operator can read the active iteration off each
//! enclosing scope at a glance. Without striation the
//! operator can't tell which `k=10` belongs to the inner
//! comprehension vs. an outer one with the same coord name
//! in a different shape.
//!
//! See SRD 18b §"Iteration variables as scope outputs" for
//! how comprehension scopes synthesise the `extern` slots
//! that this module classifies as coordinates.

use indexmap::IndexMap;

use crate::node::Value;

/// One scope's worth of iteration coordinates — the LHS names
/// and current values of every `extern <var>: <type>` clause
/// that scope declared (excluding ones inherited from a parent).
///
/// Ordered by declaration position. The map is empty for scopes
/// that don't own any coordinates (e.g. a scenario node that's
/// just a list of phases — no comprehension at that level).
#[derive(Clone, Debug, Default)]
pub struct ScopeCoord {
    pub vars: IndexMap<String, Value>,
}

impl ScopeCoord {
    pub fn new() -> Self {
        Self { vars: IndexMap::new() }
    }
    pub fn is_empty(&self) -> bool {
        self.vars.is_empty()
    }
    pub fn len(&self) -> usize {
        self.vars.len()
    }
}

/// Helper for building a coord set from `(name, Value)` pairs.
impl<I> From<I> for ScopeCoord
where
    I: IntoIterator<Item = (String, Value)>,
{
    fn from(it: I) -> Self {
        Self { vars: it.into_iter().collect() }
    }
}

/// Format a scope-coordinate path as striated parens, leaf-first:
/// `(k=10, limit=20), (table=…, optimize_for=…)`. Empty strata
/// are skipped, so a chain that passes through a non-comprehension
/// scope (e.g. a scenario node that's just a phase list) doesn't
/// render an empty `()`. Returns `""` for an empty path so callers
/// can wrap with `(…)` parens at their own discretion.
///
/// **Canonical structural identity.** This is the formatter every
/// consumer reasoning about a kernel's iteration position runs
/// through — runtime executor labels, pre-map walker labels,
/// inline status lines, scene-tree labels, error messages. Pre-map
/// and runtime producing the same string for the same iteration
/// position is what lets observer lifecycle calls bind to
/// pre-mapped scene nodes without a parallel matching scheme.
pub fn format_scope_coordinate_path(path: &[ScopeCoord]) -> String {
    let strata: Vec<String> = path.iter()
        .filter(|c| !c.is_empty())
        .map(|coord| {
            let inner = coord.vars.iter()
                .map(|(k, v)| format!("{k}={}", v.to_display_string()))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({inner})")
        })
        .collect();
    strata.join(", ")
}
