// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Scope-iteration driver — the GK-side primitive both the
//! runtime executor and the pre-map walker consume.
//!
//! ## What this owns
//!
//! Given a comprehension scope's canonical and parent kernels
//! plus the clause list, [`iterate_scope`] enumerates the
//! dependent-tuple space, applies any `order:` permutation, and
//! yields a stream of [`IterationStep`]s. Each step is a fully-
//! prepared iteration position: the typed bindings, a per-
//! iteration kernel constructed via
//! [`GkKernel::for_iteration`], and the root-first scope-
//! coordinate path including this iteration.
//!
//! ## Why this lives here
//!
//! Pre-map (scene-tree builder) and runtime (phase dispatcher)
//! both walk the same scenario tree in the same DFS order. The
//! historical pattern was each path running its own
//! `TupleComprehension` + per-iteration kernel construction
//! loop, with the two implementations drifting (different empty-
//! tuple handling, different label-string formatting,
//! optimize_for vs. optimize_for_values cascade quirks). Putting
//! the iteration engine here — alongside the kernel, the program,
//! the scope-coordinate model — makes the GK side the single
//! source of truth for "what does one iteration of this
//! comprehension look like?". Consumers only decide what to *do*
//! with each step.
//!
//! ## Streaming, not Vec
//!
//! [`ScopeIterations`] implements [`Iterator`]. The underlying
//! tuple list is enumerated eagerly by [`enumerate_tuples`] (the
//! dependent-tuple walk needs all earlier branches resolved
//! before later clauses can evaluate), but kernel construction
//! and coord-path extension happen lazily per `next()`. Runtime
//! consumers that spawn concurrent `JoinSet` tasks per step keep
//! their existing shape — just `.collect()` to materialize, then
//! distribute. Pre-map consumes serially.

use std::sync::Arc;

use crate::kernel::{GkKernel, ScopeCoord};
use crate::node::Value;

use super::ast::TraversalOrder;
use super::eval::enumerate_tuples;
use super::order::apply_order;

/// One iteration position of a comprehension scope, ready for
/// downstream consumption.
///
/// All three fields are derived from the same source data
/// (a typed tuple drained out of the dependent-tuple walk),
/// so the runtime and pre-map walkers see identical content
/// for identical iteration positions.
#[derive(Clone, Debug)]
pub struct IterationStep {
    /// Typed `(var, value)` pairs for this iteration. Same
    /// shape the runtime previously plumbed through
    /// `run_one_iteration`'s `bindings: &[(String, Value)]`
    /// parameter.
    pub bindings: Vec<(String, Value)>,

    /// Per-iteration kernel: clone of the comprehension's
    /// canonical, bound to the parent scope, with every input
    /// in [`Self::bindings`] populated. This is what
    /// downstream descendants treat as their effective
    /// parent kernel — both for nested comprehension
    /// interpolation (`vec_{profile}`) and for runtime phase
    /// dispatch.
    pub bound_kernel: Arc<GkKernel>,

    /// Root-first scope-coordinate chain ending at this
    /// iteration. Pass through
    /// [`crate::kernel::format_scope_coordinate_path`] (after
    /// reversing to leaf-first) to get the canonical structural
    /// label string.
    pub coord_path: Vec<ScopeCoord>,
}

/// Streaming iterator over a scope's prepared iteration steps.
///
/// Construct via [`iterate_scope`]; consume via the standard
/// [`Iterator`] surface. `collect()` to materialise for
/// concurrent fan-out (`tokio::JoinSet`); iterate directly for
/// serial / pre-map walks.
pub struct ScopeIterations {
    canonical: Arc<GkKernel>,
    parent: Arc<GkKernel>,
    parent_coords: Vec<ScopeCoord>,
    tuples: std::vec::IntoIter<Vec<(String, Value)>>,
}

impl Iterator for ScopeIterations {
    type Item = IterationStep;

    fn next(&mut self) -> Option<Self::Item> {
        let typed = self.tuples.next()?;
        let bound_kernel = GkKernel::for_iteration(&self.canonical, &self.parent, &typed);
        let mut coord_path = self.parent_coords.clone();
        coord_path.push(ScopeCoord::from(typed.iter().cloned()));
        Some(IterationStep { bindings: typed, bound_kernel, coord_path })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.tuples.size_hint()
    }
}

impl ExactSizeIterator for ScopeIterations {
    fn len(&self) -> usize { self.tuples.len() }
}

/// Build a [`ScopeIterations`] driver for one comprehension
/// scope.
///
/// Mirrors [`enumerate_tuples`]'s argument list (canonical /
/// parent kernels, clause list, optional filter, empty-clause
/// callback) and adds:
///
/// - `parent_coords` — root-first scope-coordinate chain of the
///   enclosing iteration. Each yielded [`IterationStep`]'s
///   `coord_path` is `parent_coords ++ [own]`.
/// - `order` — optional [`TraversalOrder`] permutation applied
///   to the dependent-tuple list before iteration begins.
/// - `clause_sizes` — per-clause cardinality hints used by some
///   `order:` permutations (e.g. interleaved). Pass `&[]` to
///   skip.
///
/// Empty-clause policy is delegated to the caller (same shape
/// as [`enumerate_tuples`]) so the activity layer's
/// strict-vs-warn lifting stays out of GK.
#[allow(clippy::too_many_arguments)]
pub fn iterate_scope<F>(
    canonical: &Arc<GkKernel>,
    parent: &Arc<GkKernel>,
    parent_coords: &[ScopeCoord],
    clauses: &[super::ast::Clause],
    filter: Option<&str>,
    order: Option<&TraversalOrder>,
    clause_sizes: &[usize],
    on_empty_clause: F,
) -> Result<ScopeIterations, String>
where
    F: FnMut(&super::ast::Clause) -> Result<(), String>,
{
    let mut tuples = enumerate_tuples(canonical, parent, clauses, filter, on_empty_clause)?;
    if let Some(o) = order {
        tuples = apply_order(tuples, clause_sizes, o)?;
    }
    Ok(ScopeIterations {
        canonical: canonical.clone(),
        parent: parent.clone(),
        parent_coords: parent_coords.to_vec(),
        tuples: tuples.into_iter(),
    })
}
