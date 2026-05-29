// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Strategy implementations — spec §3.6 + §10.2 R2.
//!
//! Every named strategy has two algorithmic surfaces:
//!
//! - **Naïve form** ([`Strategy::naive_apply`]) — given a
//!   materialized `Vec<Tuple>`, return the reordered (and
//!   optionally truncated) `Vec<Tuple>`. Used when the
//!   optimizer's R2 push-down does NOT fire (input is not
//!   index-addressable, or no closed-form push-down rule exists
//!   for the strategy + input combination).
//! - **Indexed form** ([`Strategy::indexed_apply`]) — given an
//!   input's [`IndexFn`] + truncation, return a sequence of
//!   [`MultiIndex`] values to look up in the input's
//!   enumeration. This is the R2 push-down realization: the
//!   strategy operates over the input's index space without
//!   materializing the input.
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

/// The strategy interface used by the optimizer (R2) and the
/// IR interpreter.
///
/// Implementations are stateless — every call to
/// [`naive_apply`] / [`indexed_apply`] produces the same output
/// given the same inputs (deterministic). PRNG-based
/// strategies (`Shuffle`) take their seed via the
/// `truncation` companion call — the seed is captured at the
/// `Comprehension::Order { strategy, truncation }` level by
/// the runtime, not by the strategy itself.
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
    /// given input. If `false`, the optimizer falls back to
    /// the naïve form.
    fn has_closed_form_for(&self, idx: &IndexFn) -> bool;

    /// Naïve form: reorder a materialized tuple list. Used
    /// when R2 doesn't fire.
    fn naive_apply(&self, input: Vec<Tuple>, truncation: Option<u64>) -> Vec<Tuple>;

    /// Indexed form: produce a sequence of multi-indices to
    /// look up in the input's enumeration. Used by R2's
    /// indexed_order IR opcode.
    ///
    /// The returned `Vec<MultiIndex>` is the dispense order;
    /// the consumer (IR interpreter) resolves each through the
    /// input's `IndexFn` to get an actual tuple.
    fn indexed_apply(&self, idx: &IndexFn, truncation: Option<u64>) -> Vec<MultiIndex>;
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
