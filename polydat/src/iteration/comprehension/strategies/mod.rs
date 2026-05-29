// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Strategy implementations — spec §3.6 + §10.2 R2 + §10.7.8.
//!
//! ## Single invocation surface
//!
//! Every named strategy exposes one public entry point —
//! [`Strategy::apply`]. The caller passes an [`EvaluatedInput`]
//! carrying the materialized tuples, their cardinality, and the
//! `IndexFn` they actually satisfy. The strategy decides
//! internally whether to dispatch its closed-form indexed
//! algorithm (when `has_closed_form_for(&input.index_fn)`) or
//! its fallback reorder over the materialized tuples.
//!
//! Per spec §10.7.8 this is the **strategy invocation
//! contract**: V4 fires at `apply` time against the
//! `EvaluatedInput`'s `index_fn` — definitively, regardless of
//! how the input source was authored (literal, range,
//! registry-recognized generator, or workload-param).
//!
//! ## Internal split
//!
//! Per-strategy modules organise the implementation into two
//! private helpers (`apply_indexed` for the R2 closed-form path
//! when applicable, `apply_naive` for the generic fallback);
//! [`Strategy::apply`] is the dispatcher. The trait surface
//! exposes only the dispatcher plus the V4/R2 introspection
//! predicates ([`Strategy::accepts_input`],
//! [`Strategy::has_closed_form_for`]).
//!
//! Strategies are selected by [`StrategyName`]; [`for_name`]
//! dispatches a strategy name to its boxed [`Strategy`] impl.

use super::metadata::IndexFn;
use super::strategy::StrategyName;

pub mod antidiagonal;
pub mod diagonal;
pub mod extrema;
pub mod halton;
pub mod lex;
pub mod lhs;
pub mod prng;
pub mod reverse_lex;
pub mod shells;
pub mod shuffle;
pub mod sobol;

/// A multi-coordinate index. Each component is the per-axis
/// position in the input's index space. Length equals the
/// input's dimensionality (1 for `Lockstep` / `Modular` /
/// `Concatenation`; N for `Lattice` / `Continuous` /
/// `Hybrid`).
///
/// `MultiIndex` is the indexed-form output type. The R2 IR
/// opcode emitted by the optimizer consumes these and resolves
/// each through the input's `IndexFn` to dispense the actual
/// tuple.
pub type MultiIndex = Vec<u64>;

/// A named-tuple value. Subset of the polydat `Value` set
/// sufficient for naïve-form strategy testing; the production
/// strategy layer will operate on the full polydat `Value` type
/// via the IR interpreter (Phase 7). For the strategy module
/// in isolation, this lightweight type lets tests run without
/// pulling in the broader runtime.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple {
    pub bindings: Vec<(String, TupleValue)>,
}

/// Subset of polydat's `Value` enum used by strategy tests.
/// Production-side `naive_apply` will wrap polydat's full
/// `Value`; this type is the algebraic-layer testing currency.
#[derive(Debug, Clone, PartialEq)]
pub enum TupleValue {
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bool(bool),
}

impl Tuple {
    pub fn new() -> Self {
        Self { bindings: Vec::new() }
    }

    pub fn with<K: Into<String>>(mut self, key: K, value: TupleValue) -> Self {
        self.bindings.push((key.into(), value));
        self
    }
}

impl Default for Tuple {
    fn default() -> Self {
        Self::new()
    }
}

/// The materialized input to a strategy at invocation time
/// (spec §10.7.8).
///
/// `tuples` are the input stream's tuples in source order (the
/// natural enumeration of the upstream comprehension subtree).
/// `cardinality` matches `tuples.len() as u64`. `index_fn` is
/// the addressing scheme the input actually satisfies —
/// derived from observed shape for Generator /
/// WorkloadParamList leaves via the [`crate::iteration::comprehension::eval_source`]
/// layer, combined upward by the runtime walker per spec
/// §10.7.2 propagation rules.
pub struct EvaluatedInput {
    pub tuples: Vec<Tuple>,
    pub cardinality: u64,
    pub index_fn: IndexFn,
}

/// The strategy invocation surface per spec §10.7.8.
///
/// Implementations are stateless — every call to [`apply`]
/// produces the same output given the same inputs
/// (deterministic). PRNG-based strategies (`Shuffle`) take
/// their seed from the truncation companion — the seed is
/// captured at the `Comprehension::Order { strategy, truncation }`
/// level by the runtime, not by the strategy itself.
pub trait Strategy {
    /// The strategy's name. Mirrors [`StrategyName`].
    fn name(&self) -> StrategyName;

    /// V4 input-shape check (spec §3.6). `None` represents an
    /// input with no closed-form index function; only `Lex`
    /// accepts that. Concrete `IndexFn` variants are accepted
    /// per the per-strategy rules in spec §3.6's table.
    fn accepts_input(&self, idx: Option<&IndexFn>) -> bool;

    /// R2 push-down eligibility (spec §10.2 R2). `true` if
    /// this strategy has a closed-form indexed lookup over the
    /// given input. If `false`, [`apply`] uses the strategy's
    /// fallback reorder over the materialized tuples.
    fn has_closed_form_for(&self, idx: &IndexFn) -> bool;

    /// Apply this strategy to the given input.
    ///
    /// Internally dispatches: when the strategy has a
    /// closed-form rule for `input.index_fn`, it uses the
    /// indexed-form algorithm (compute multi-indices over the
    /// index space, look up against `input.tuples` via
    /// [`multi_index_to_flat`]). Otherwise it falls back to a
    /// per-strategy reorder over `input.tuples` directly.
    ///
    /// V4 is the caller's responsibility — call
    /// `accepts_input(Some(&input.index_fn))` before `apply`
    /// to fire V4 at strategy-invocation time per spec §10.7.8.
    fn apply(&self, input: &EvaluatedInput, truncation: Option<u64>) -> Vec<Tuple>;
}

/// Dispatch a [`StrategyName`] to its concrete [`Strategy`]
/// implementation. The returned trait object is stateless;
/// callers can hold a single instance per strategy name for
/// the life of the process if desired.
pub fn for_name(name: StrategyName) -> Box<dyn Strategy + Send + Sync> {
    match name {
        StrategyName::Lex => Box::new(lex::Lex),
        StrategyName::ReverseLex => Box::new(reverse_lex::ReverseLex),
        StrategyName::Shuffle => Box::new(shuffle::Shuffle),
        StrategyName::Halton => Box::new(halton::Halton),
        StrategyName::Sobol => Box::new(sobol::Sobol),
        StrategyName::Lhs => Box::new(lhs::Lhs),
        StrategyName::Extrema => Box::new(extrema::Extrema),
        StrategyName::Shells => Box::new(shells::Shells),
        StrategyName::Diagonal => Box::new(diagonal::Diagonal),
        StrategyName::Antidiagonal => Box::new(antidiagonal::Antidiagonal),
    }
}

/// Resolve a [`MultiIndex`] to a flat position in the
/// input's tuple list, given the input's [`IndexFn`].
///
/// The flat position matches the natural enumeration order
/// the runtime walker produces:
///
/// - `Lattice { axis_sizes: [s0, s1, …, sN-1] }` — row-major
///   over the axes: `flat = i0 * s1 * s2 * … + i1 * s2 * … + … + iN-1`.
///   This matches the runtime walker's cartesian enumeration
///   (head axis varies slowest, tail nested).
/// - `Lockstep { length }` — one-axis identity:
///   `flat = mi[0]`.
/// - `Modular { axis_sizes }` — one-axis identity over `max(axis_sizes)`:
///   `flat = mi[0]`.
/// - `Concatenation { segment_sizes }` — one-axis identity
///   over `Σ segment_sizes`: `flat = mi[0]`.
/// - `Continuous` / `Hybrid` — `None`; these inputs have no
///   pre-materialized tuple list (the strategy's multi-indices
///   are quantiles, not lookups).
///
/// Returns `None` for out-of-range positions or dimension
/// mismatches.
pub fn multi_index_to_flat(idx: &IndexFn, mi: &MultiIndex) -> Option<usize> {
    match idx {
        IndexFn::Lattice { axis_sizes } => {
            if mi.len() != axis_sizes.len() {
                return None;
            }
            let mut flat: u64 = 0;
            let mut stride: u64 = 1;
            for i in (0..axis_sizes.len()).rev() {
                let pos = mi[i];
                let size = axis_sizes[i];
                if pos >= size {
                    return None;
                }
                flat = flat.checked_add(pos.checked_mul(stride)?)?;
                stride = stride.checked_mul(size)?;
            }
            Some(flat as usize)
        }
        IndexFn::Lockstep { length } => {
            if mi.len() != 1 || mi[0] >= *length {
                return None;
            }
            Some(mi[0] as usize)
        }
        IndexFn::Modular { axis_sizes } => {
            let max = axis_sizes.iter().copied().max().unwrap_or(0);
            if mi.len() != 1 || mi[0] >= max {
                return None;
            }
            Some(mi[0] as usize)
        }
        IndexFn::Concatenation { segment_sizes } => {
            let total: u64 = segment_sizes.iter().copied().sum();
            if mi.len() != 1 || mi[0] >= total {
                return None;
            }
            Some(mi[0] as usize)
        }
        IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => None,
    }
}

/// `true` when [`multi_index_to_flat`] returns a usable
/// position for in-range multi-indices over this `IndexFn`.
/// `false` for `Continuous` / `Hybrid` where the indexed
/// strategy emits quantiles, not lookups.
pub fn index_fn_supports_lookup(idx: &IndexFn) -> bool {
    !matches!(idx, IndexFn::Continuous { .. } | IndexFn::Hybrid { .. })
}

/// Cardinality of an `IndexFn`. Used by strategies to size
/// their output when no truncation is specified. Mirrors the
/// helper in `metadata.rs` but lives here to avoid a circular
/// dependency.
pub(crate) fn index_fn_size(idx: &IndexFn) -> u64 {
    match idx {
        IndexFn::Lattice { axis_sizes } => {
            axis_sizes.iter().copied().fold(1u64, |a, b| a.saturating_mul(b))
        }
        IndexFn::Lockstep { length } => *length,
        IndexFn::Modular { axis_sizes } => {
            axis_sizes.iter().copied().max().unwrap_or(0)
        }
        IndexFn::Concatenation { segment_sizes } => {
            segment_sizes.iter().copied().fold(0u64, |a, b| a.saturating_add(b))
        }
        IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => 0,
    }
}

/// Lattice dimensionality of an `IndexFn`. Used by strategies
/// that branch on dimensionality (Extrema's corner count,
/// Lhs's per-axis stratification).
pub(crate) fn index_fn_dim(idx: &IndexFn) -> usize {
    match idx {
        IndexFn::Lattice { axis_sizes } => axis_sizes.len(),
        IndexFn::Continuous { intervals, .. } => intervals.len(),
        IndexFn::Hybrid {
            discrete_axes,
            continuous_axes,
            ..
        } => discrete_axes.len() + continuous_axes.len(),
        IndexFn::Lockstep { .. } | IndexFn::Modular { .. } => 1,
        IndexFn::Concatenation { segment_sizes } => segment_sizes.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_name_dispatches_to_correct_strategy() {
        assert_eq!(for_name(StrategyName::Lex).name(), StrategyName::Lex);
        assert_eq!(for_name(StrategyName::Halton).name(), StrategyName::Halton);
        assert_eq!(for_name(StrategyName::Extrema).name(), StrategyName::Extrema);
    }

    #[test]
    fn index_fn_size_lattice() {
        let idx = IndexFn::Lattice { axis_sizes: vec![3, 4, 5] };
        assert_eq!(index_fn_size(&idx), 60);
    }

    #[test]
    fn index_fn_size_concatenation() {
        let idx = IndexFn::Concatenation { segment_sizes: vec![10, 20, 30] };
        assert_eq!(index_fn_size(&idx), 60);
    }

    #[test]
    fn index_fn_dim_classifies_correctly() {
        assert_eq!(index_fn_dim(&IndexFn::Lattice { axis_sizes: vec![3, 4] }), 2);
        assert_eq!(index_fn_dim(&IndexFn::Lockstep { length: 10 }), 1);
        assert_eq!(index_fn_dim(&IndexFn::Concatenation { segment_sizes: vec![1, 2, 3] }), 3);
    }
}
