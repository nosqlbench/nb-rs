// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Metadata algebra — spec §10.7.
//!
//! Every well-formed comprehension AST node carries a four-field
//! [`Metadata`] bundle computed bottom-up from its children's
//! metadata and its own scalar parameters. The bundle is a
//! monoid: propagation composes under composition, and every
//! field is either a closed enum (capability bit) or a
//! closed-form numeric/symbolic descriptor.
//!
//! This module owns:
//!
//! - [`Metadata`] — the four-field bundle.
//! - [`IndexFn`] — closed-form addressing schemes (six variants
//!   covering cartesian, zip Strict/Truncate, zip Cycle, union,
//!   continuous, hybrid).
//! - [`NaturalOrder`] — how a node enumerates by default.
//! - [`Materialization`] — streaming or sized-barrier
//!   classification (spec §6.2).
//! - [`Comprehension::metadata`] — propagation entry point.
//!
//! The propagation rules are total, constant-time per node, and
//! cannot fail. Dependent-source cartesians produce
//! `index_addressable = None`; this is the **only** place
//! metadata propagation consults child-internal information
//! beyond the published bundles — and it does so at the
//! cartesian node, by walking the children's source expressions
//! for back-references to earlier-axis names.

use serde::{Deserialize, Serialize};

use super::ast::Comprehension;
use super::cardinality::{CardinalityClass, Hybrid, Interval, ProductMeasure};
use super::source::Source;
use super::strategy::{StrategyName, ZipMode};

/// The metadata bundle carried by every well-formed AST node.
///
/// Computed bottom-up; never mutated after propagation. Each
/// field is a closed enum or a closed-form descriptor — no
/// callbacks, no fail-able analyses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    /// Cardinality class per spec §6.1.
    pub cardinality: CardinalityClass,

    /// Closed-form bijection from `0..|c|` to the node's
    /// dispensed tuples. `None` when the node has no
    /// addressable index space (raw filter output, dependent
    /// cartesian, non-Lex order output at the AST level).
    pub index_addressable: Option<IndexFn>,

    /// How this node enumerates by default.
    pub natural_order: NaturalOrder,

    /// Streaming-vs-barrier classification per spec §6.2.
    pub materialization: Materialization,
}

/// Closed-form addressing schemes — spec §10.7.1.
///
/// Six variants. Each describes the bijection from a
/// `0..cardinality` index range to the node's tuple shape.
/// `Continuous` and `Hybrid` carry the cardinality's
/// interval+measure descriptors directly so the R2 push-down
/// rules (Phase 6) can dispatch on them without recomputing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexFn {
    /// Discrete cartesian. `axis_sizes[i]` is the i-th axis's
    /// element count. Multi-index `(i₀, i₁, …)` maps to the
    /// per-axis tuple at those positions.
    Lattice { axis_sizes: Vec<u64> },

    /// Zip Strict / Truncate. One index `i ∈ 0..length` maps
    /// to the per-child tuple at position i.
    Lockstep { length: u64 },

    /// Zip Cycle. Modular addressing — index `i` maps to each
    /// child at `i mod child.cardinality`. At least one child
    /// must be bounded (the cycling target).
    Modular { axis_sizes: Vec<u64> },

    /// Union of index-addressable children. Index `i ∈
    /// 0..Σsegment_sizes` maps to segment k where k is the
    /// smallest such that `Σ₀^k segment_sizes > i`, position
    /// `i - Σ₀^{k-1} segment_sizes` within that segment.
    Concatenation { segment_sizes: Vec<u64> },

    /// Continuous K-D box. Strategy push-down rules (Halton /
    /// Sobol / Lhs / Extrema on Continuous) draw from this
    /// directly; the discrete-to-continuous mapping is
    /// strategy-specific.
    Continuous {
        intervals: Vec<Interval>,
        measure: ProductMeasure,
    },

    /// Mixed discrete × continuous cartesian. Discrete axes get
    /// integer indexing; continuous axes get measure-weighted
    /// sampling. Strategy push-down dispatches per-axis.
    Hybrid {
        discrete_axes: Vec<u64>,
        continuous_axes: Vec<Interval>,
        measure: ProductMeasure,
    },
}

impl IndexFn {
    /// `true` if this index function carries any continuous
    /// axis. Used by per-strategy V4 checks to reject
    /// strategies that don't accept continuous inputs.
    pub fn has_continuous_axis(&self) -> bool {
        matches!(self, IndexFn::Continuous { .. } | IndexFn::Hybrid { .. })
    }

    /// `true` if this index function is a multi-axis Lattice
    /// (discrete cartesian with ≥2 axes). Required by
    /// lattice-geometric strategies (Extrema / Shells /
    /// Diagonal / Antidiagonal) for non-degenerate behavior.
    pub fn is_multi_axis_lattice(&self) -> bool {
        matches!(self, IndexFn::Lattice { axis_sizes } if axis_sizes.len() >= 2)
    }
}

/// Natural enumeration order — spec §10.7.1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NaturalOrder {
    /// Lex order — rightmost axis varies fastest. Produced by
    /// cartesian, single-axis clause, and `order(_, Lex, _)`.
    Lex,

    /// Lockstep — zip's natural order. One tuple per i, all
    /// children at position i.
    Lockstep,

    /// Sequential — union's natural order. Drain child 0,
    /// then child 1, etc.
    Sequential,

    /// Strategy-driven — produced by `order(_, non-Lex, _)`.
    /// The wrapped strategy determines the emission order.
    Strategy(StrategyName),

    /// Pending — continuous source not yet wrapped by a
    /// sampling order. V8 requires resolution before dispense.
    PendingSampling,
}

/// Streaming-vs-barrier classification per spec §6.2.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Materialization {
    /// O(operator-local state) per pull; no input materialized.
    Streaming,

    /// Holds a finite working set; size declared at compile
    /// time. The two natural barriers per spec §6.3:
    /// `zip(Cycle)` shorter children + non-Lex `order`.
    BoundedBarrier { working_set_size: u64 },

    /// Working set is unbounded. Always V6-rejected per spec
    /// §5; this variant exists for representational
    /// completeness but should never propagate through to a
    /// valid AST's metadata.
    UnboundedBarrier,
}

impl Comprehension {
    /// Compute this node's metadata bundle per spec §10.7.2.
    ///
    /// Bottom-up: every child's metadata is computed first,
    /// then this node's. Constant-time per node above the
    /// child cost. Total — never fails, never partial.
    ///
    /// For non-leaf nodes the metadata is recomputed on every
    /// call (no caching at this layer); consumers that need
    /// memoization should wrap externally. This is fine
    /// because the propagation cost is O(N) total nodes and
    /// the optimizer (Phase 6) re-propagates after each
    /// rewrite anyway.
    pub fn metadata(&self) -> Metadata {
        match self {
            Comprehension::Clause { source, .. } => clause_metadata(source),
            Comprehension::Cartesian { children } => cartesian_metadata(children),
            Comprehension::Zip { children, mode } => zip_metadata(children, *mode),
            Comprehension::Union { children } => union_metadata(children),
            Comprehension::Filter { child, .. } => filter_metadata(child),
            Comprehension::Order { child, strategy, truncation } => {
                order_metadata(child, *strategy, *truncation)
            }
        }
    }
}

fn clause_metadata(source: &Source) -> Metadata {
    let cardinality = source.cardinality();
    let (index_addressable, natural_order) = match &cardinality {
        CardinalityClass::Bounded(n) => (
            Some(IndexFn::Lattice { axis_sizes: vec![*n] }),
            NaturalOrder::Lex,
        ),
        CardinalityClass::Continuous { intervals, measure } => (
            Some(IndexFn::Continuous {
                intervals: intervals.clone(),
                measure: measure.clone(),
            }),
            NaturalOrder::PendingSampling,
        ),
        // BoundedAtMost / Unbounded / ContinuousAtMost — no
        // closed-form addressing function exists.
        _ => (None, NaturalOrder::Lex),
    };
    Metadata {
        cardinality,
        index_addressable,
        natural_order,
        materialization: Materialization::Streaming,
    }
}

fn cartesian_metadata(children: &[Comprehension]) -> Metadata {
    // First detect dependent sources: any child whose source
    // expression references an earlier child's coordinate name.
    // Dependent → index_addressable = None.
    let dependent = detect_dependent_sources(children);

    let child_meta: Vec<Metadata> = children.iter().map(|c| c.metadata()).collect();
    let cardinality = combine_cartesian_cardinality(&child_meta);

    let index_addressable = if dependent {
        None
    } else {
        combine_cartesian_index_fn(&child_meta)
    };

    let natural_order = if matches!(cardinality, CardinalityClass::Continuous { .. } | CardinalityClass::Hybrid(_)) {
        NaturalOrder::PendingSampling
    } else {
        NaturalOrder::Lex
    };

    Metadata {
        cardinality,
        index_addressable,
        natural_order,
        materialization: Materialization::Streaming,
    }
}

fn zip_metadata(children: &[Comprehension], mode: ZipMode) -> Metadata {
    let child_meta: Vec<Metadata> = children.iter().map(|c| c.metadata()).collect();
    let cardinality = combine_zip_cardinality(&child_meta, mode);
    let index_addressable = combine_zip_index_fn(&child_meta, mode);

    let materialization = match mode {
        ZipMode::Strict | ZipMode::Truncate => Materialization::Streaming,
        ZipMode::Cycle => {
            // Shorter children's cardinalities sum into the
            // barrier working set (each non-longest child must
            // replay).
            let cards: Vec<u64> = child_meta
                .iter()
                .filter_map(|m| match &m.cardinality {
                    CardinalityClass::Bounded(n) | CardinalityClass::BoundedAtMost(n) => Some(*n),
                    _ => None,
                })
                .collect();
            if cards.is_empty() {
                Materialization::Streaming
            } else {
                let max = cards.iter().copied().max().unwrap_or(0);
                let sum_non_longest: u64 = cards.iter().filter(|n| **n != max).sum();
                Materialization::BoundedBarrier {
                    working_set_size: sum_non_longest,
                }
            }
        }
    };

    Metadata {
        cardinality,
        index_addressable,
        natural_order: NaturalOrder::Lockstep,
        materialization,
    }
}

fn union_metadata(children: &[Comprehension]) -> Metadata {
    let child_meta: Vec<Metadata> = children.iter().map(|c| c.metadata()).collect();
    let cardinality = combine_union_cardinality(&child_meta);
    let index_addressable = combine_union_index_fn(&child_meta);
    Metadata {
        cardinality,
        index_addressable,
        natural_order: NaturalOrder::Sequential,
        materialization: Materialization::Streaming,
    }
}

fn filter_metadata(child: &Comprehension) -> Metadata {
    let child_meta = child.metadata();
    let cardinality = match &child_meta.cardinality {
        CardinalityClass::Bounded(n) | CardinalityClass::BoundedAtMost(n) => {
            CardinalityClass::BoundedAtMost(*n)
        }
        CardinalityClass::Unbounded => CardinalityClass::Unbounded,
        CardinalityClass::Continuous { intervals, measure }
        | CardinalityClass::ContinuousAtMost {
            intervals,
            measure_at_most: measure,
        } => CardinalityClass::ContinuousAtMost {
            intervals: intervals.clone(),
            measure_at_most: measure.clone(),
        },
        CardinalityClass::Hybrid(h) => CardinalityClass::Hybrid(h.clone()),
    };
    Metadata {
        cardinality,
        index_addressable: None, // filter destroys the bijection
        natural_order: child_meta.natural_order,
        materialization: child_meta.materialization,
    }
}

fn order_metadata(
    child: &Comprehension,
    strategy: StrategyName,
    truncation: Option<u64>,
) -> Metadata {
    let child_meta = child.metadata();
    let cardinality = match (&child_meta.cardinality, truncation) {
        // Continuous + sampling + Some(n) → Bounded(n) (V8 discharge).
        (CardinalityClass::Continuous { .. }, Some(n))
        | (CardinalityClass::ContinuousAtMost { .. }, Some(n))
        | (CardinalityClass::Hybrid(_), Some(n))
            if !matches!(strategy, StrategyName::Lex) =>
        {
            CardinalityClass::Bounded(n)
        }
        // Discrete + truncation: min of (child, n).
        (CardinalityClass::Bounded(child_n), Some(n)) => {
            CardinalityClass::Bounded((*child_n).min(n))
        }
        (CardinalityClass::BoundedAtMost(child_n), Some(n)) => {
            CardinalityClass::BoundedAtMost((*child_n).min(n))
        }
        (_, Some(n)) => CardinalityClass::Bounded(n), // unbounded + Some(n) → Bounded(n)
        // No truncation: inherit child's cardinality.
        (c, None) => c.clone(),
    };

    let (index_addressable, natural_order, materialization) = match strategy {
        StrategyName::Lex => (
            child_meta.index_addressable, // inherit through Lex
            NaturalOrder::Lex,
            child_meta.materialization,   // counter wrapper at most
        ),
        non_lex => {
            // R2 (Phase 6) rewrites this into an indexed_order
            // IR opcode; AST-level metadata stops here.
            let working_set_size = strategy_working_set(
                non_lex,
                &child_meta.index_addressable,
                truncation,
            );
            (
                None,
                NaturalOrder::Strategy(non_lex),
                Materialization::BoundedBarrier { working_set_size },
            )
        }
    };

    Metadata {
        cardinality,
        index_addressable,
        natural_order,
        materialization,
    }
}

// ---- cardinality combinators ----

fn combine_cartesian_cardinality(children: &[Metadata]) -> CardinalityClass {
    let mut has_continuous = false;
    let mut has_discrete = false;
    let mut has_unbounded = false;
    let mut product: u64 = 1;
    let mut overflow = false;
    let mut discrete_axes: Vec<u64> = Vec::new();
    let mut continuous_intervals: Vec<Interval> = Vec::new();
    let mut continuous_measures: Vec<ProductMeasure> = Vec::new();

    for m in children {
        match &m.cardinality {
            CardinalityClass::Bounded(n) => {
                has_discrete = true;
                discrete_axes.push(*n);
                product = product.checked_mul(*n).unwrap_or_else(|| {
                    overflow = true;
                    u64::MAX
                });
            }
            CardinalityClass::BoundedAtMost(n) => {
                has_discrete = true;
                discrete_axes.push(*n); // upper bound
                product = product.checked_mul(*n).unwrap_or_else(|| {
                    overflow = true;
                    u64::MAX
                });
            }
            CardinalityClass::Unbounded => {
                has_unbounded = true;
                has_discrete = true;
                discrete_axes.push(0);
            }
            CardinalityClass::Continuous { intervals, measure }
            | CardinalityClass::ContinuousAtMost { intervals, measure_at_most: measure } => {
                has_continuous = true;
                continuous_intervals.extend(intervals.iter().cloned());
                continuous_measures.push(measure.clone());
            }
            CardinalityClass::Hybrid(h) => {
                has_continuous = true;
                has_discrete = true;
                discrete_axes.extend(h.discrete_axes.iter().copied());
                continuous_intervals.extend(h.continuous_axes.iter().cloned());
                continuous_measures.push(h.measure.clone());
            }
        }
    }

    let _ = overflow; // discard; saturating product is the policy

    if has_continuous && has_discrete {
        CardinalityClass::Hybrid(Hybrid {
            discrete_axes,
            continuous_axes: continuous_intervals,
            measure: simplify_measures(continuous_measures),
        })
    } else if has_continuous {
        CardinalityClass::Continuous {
            intervals: continuous_intervals,
            measure: simplify_measures(continuous_measures),
        }
    } else if has_unbounded {
        CardinalityClass::Unbounded
    } else {
        CardinalityClass::Bounded(product)
    }
}

fn combine_cartesian_index_fn(children: &[Metadata]) -> Option<IndexFn> {
    // All children must be addressable for the cartesian to be.
    let all_addressable = children.iter().all(|m| m.index_addressable.is_some());
    if !all_addressable {
        return None;
    }

    let mut all_discrete = true;
    let mut all_continuous = true;
    let mut discrete_axes: Vec<u64> = Vec::new();
    let mut continuous_intervals: Vec<Interval> = Vec::new();
    let mut continuous_measures: Vec<ProductMeasure> = Vec::new();

    for m in children {
        match m.index_addressable.as_ref().unwrap() {
            IndexFn::Lattice { axis_sizes } => {
                all_continuous = false;
                discrete_axes.extend(axis_sizes.iter().copied());
            }
            IndexFn::Continuous { intervals, measure } => {
                all_discrete = false;
                continuous_intervals.extend(intervals.iter().cloned());
                continuous_measures.push(measure.clone());
            }
            IndexFn::Hybrid {
                discrete_axes: d,
                continuous_axes: c,
                measure,
            } => {
                all_discrete = false;
                all_continuous = false;
                discrete_axes.extend(d.iter().copied());
                continuous_intervals.extend(c.iter().cloned());
                continuous_measures.push(measure.clone());
            }
            // Lockstep / Modular / Concatenation — these don't
            // combine as cartesian axes (they're 1-D index
            // spaces of their own); cartesian-of-zip / cartesian-
            // of-union would need a richer addressing scheme.
            // For now, fall back to None.
            IndexFn::Lockstep { .. } | IndexFn::Modular { .. } | IndexFn::Concatenation { .. } => {
                return None;
            }
        }
    }

    if all_discrete {
        Some(IndexFn::Lattice { axis_sizes: discrete_axes })
    } else if all_continuous {
        Some(IndexFn::Continuous {
            intervals: continuous_intervals,
            measure: simplify_measures(continuous_measures),
        })
    } else {
        Some(IndexFn::Hybrid {
            discrete_axes,
            continuous_axes: continuous_intervals,
            measure: simplify_measures(continuous_measures),
        })
    }
}

fn combine_zip_cardinality(children: &[Metadata], mode: ZipMode) -> CardinalityClass {
    // V7 should have rejected mixed-class / continuous; here we
    // assume discrete children.
    let counts: Vec<Option<u64>> = children
        .iter()
        .map(|m| match &m.cardinality {
            CardinalityClass::Bounded(n) | CardinalityClass::BoundedAtMost(n) => Some(*n),
            CardinalityClass::Unbounded => None,
            // Continuous / Hybrid here would be a V7 failure
            // that slipped through; treat as Unbounded for
            // metadata purposes.
            _ => None,
        })
        .collect();

    match mode {
        ZipMode::Strict => {
            // V7 should have caught mismatch. Use any bounded child's count.
            counts
                .iter()
                .find_map(|c| *c)
                .map(CardinalityClass::Bounded)
                .unwrap_or(CardinalityClass::Unbounded)
        }
        ZipMode::Truncate => {
            let bounded: Vec<u64> = counts.iter().filter_map(|c| *c).collect();
            if bounded.is_empty() {
                CardinalityClass::Unbounded
            } else {
                CardinalityClass::Bounded(*bounded.iter().min().unwrap())
            }
        }
        ZipMode::Cycle => {
            let bounded: Vec<u64> = counts.iter().filter_map(|c| *c).collect();
            if counts.iter().any(Option::is_none) {
                CardinalityClass::Unbounded
            } else if let Some(max) = bounded.iter().max() {
                CardinalityClass::Bounded(*max)
            } else {
                CardinalityClass::Bounded(0)
            }
        }
    }
}

fn combine_zip_index_fn(children: &[Metadata], mode: ZipMode) -> Option<IndexFn> {
    let all_addressable = children.iter().all(|m| m.index_addressable.is_some());
    if !all_addressable {
        return None;
    }
    let counts: Vec<u64> = children
        .iter()
        .filter_map(|m| match &m.cardinality {
            CardinalityClass::Bounded(n) | CardinalityClass::BoundedAtMost(n) => Some(*n),
            _ => None,
        })
        .collect();
    if counts.len() != children.len() {
        return None;
    }
    match mode {
        ZipMode::Strict | ZipMode::Truncate => {
            let length = match mode {
                ZipMode::Strict => counts[0],
                ZipMode::Truncate => *counts.iter().min().unwrap(),
                ZipMode::Cycle => unreachable!(),
            };
            Some(IndexFn::Lockstep { length })
        }
        ZipMode::Cycle => Some(IndexFn::Modular { axis_sizes: counts }),
    }
}

fn combine_union_cardinality(children: &[Metadata]) -> CardinalityClass {
    let mut sum: u64 = 0;
    let mut any_unbounded = false;
    let mut any_atmost = false;
    for m in children {
        match &m.cardinality {
            CardinalityClass::Bounded(n) => {
                sum = sum.saturating_add(*n);
            }
            CardinalityClass::BoundedAtMost(n) => {
                sum = sum.saturating_add(*n);
                any_atmost = true;
            }
            CardinalityClass::Unbounded => {
                any_unbounded = true;
            }
            // V9 should have caught continuous-in-union.
            _ => any_unbounded = true,
        }
    }
    if any_unbounded {
        CardinalityClass::Unbounded
    } else if any_atmost {
        CardinalityClass::BoundedAtMost(sum)
    } else {
        CardinalityClass::Bounded(sum)
    }
}

fn combine_union_index_fn(children: &[Metadata]) -> Option<IndexFn> {
    let all_addressable = children.iter().all(|m| m.index_addressable.is_some());
    if !all_addressable {
        return None;
    }
    let segment_sizes: Vec<u64> = children
        .iter()
        .filter_map(|m| match &m.cardinality {
            CardinalityClass::Bounded(n) | CardinalityClass::BoundedAtMost(n) => Some(*n),
            _ => None,
        })
        .collect();
    if segment_sizes.len() != children.len() {
        return None;
    }
    Some(IndexFn::Concatenation { segment_sizes })
}

// ---- supporting helpers ----

fn simplify_measures(measures: Vec<ProductMeasure>) -> ProductMeasure {
    match measures.len() {
        0 => ProductMeasure::Uniform,
        1 => measures.into_iter().next().unwrap(),
        _ => ProductMeasure::Product(measures),
    }
}

/// Strategy-specific working-set size for use as
/// `BoundedBarrier.working_set_size`. Pre-R2, the naïve form
/// uses the input cardinality; with R2 push-down, the size
/// shrinks to the strategy's closed-form minimum. The metadata
/// here records the **R2-realized** size (the size the
/// optimizer will achieve), so consumers reading metadata see
/// the post-optimization budget.
fn strategy_working_set(
    strategy: StrategyName,
    input: &Option<IndexFn>,
    truncation: Option<u64>,
) -> u64 {
    match (strategy, input, truncation) {
        // Halton / Sobol / Shuffle over an index-addressable
        // input + truncation: O(n) draws.
        (StrategyName::Halton, Some(_), Some(n))
        | (StrategyName::Sobol, Some(_), Some(n))
        | (StrategyName::Shuffle, Some(_), Some(n))
        | (StrategyName::ReverseLex, Some(_), Some(n)) => n,
        // Lhs: O(n * dim).
        (StrategyName::Lhs, Some(idx), Some(n)) => {
            let dim = lattice_dim(idx).max(1);
            n.saturating_mul(dim as u64)
        }
        // Extrema (SRD-18d §214): `/k` selects the first k *strata*
        // (interior count 0..k-1), not k tuples — the output is
        // `≥ 2^dim` corners for k≥1 and grows to the full space. The
        // materialize step buffers the whole input regardless, so the
        // safe working-set bound is the input cardinality. (A tight
        // first-k-strata sum would need per-axis interior sizes;
        // deferred — over-reporting here is safe, under-reporting is
        // not.)
        (StrategyName::Extrema, Some(idx), Some(_k)) => index_fn_cardinality(idx),
        // Shells / Diagonal / Antidiagonal: per-emitted O(N).
        (StrategyName::Shells, Some(_), Some(n))
        | (StrategyName::Diagonal, Some(_), Some(n))
        | (StrategyName::Antidiagonal, Some(_), Some(n)) => n,
        // No truncation: fall back to the input's cardinality.
        (_, Some(idx), None) => index_fn_cardinality(idx),
        // No addressable input: we can't compute a closed form;
        // use the naïve "input cardinality" placeholder so the
        // metadata still has a number (consumers should treat
        // this as a conservative upper bound).
        (_, None, Some(n)) => n,
        (_, None, None) => 0,
        // Lex with truncation over addressable input — counter
        // wrapper, working set equals output size.
        (StrategyName::Lex, Some(_), Some(n)) => n,
    }
}

fn lattice_dim(idx: &IndexFn) -> usize {
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

fn index_fn_cardinality(idx: &IndexFn) -> u64 {
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
        // Continuous index has no integer cardinality.
        IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => 0,
    }
}

/// Walk children's source expressions for back-references to
/// earlier-axis coordinate names. Used by cartesian metadata
/// propagation to detect dependent sources per spec §3.2.
fn detect_dependent_sources(children: &[Comprehension]) -> bool {
    let mut prior_names: Vec<String> = Vec::new();
    for child in children {
        // First check if the child references any prior name in
        // its source(s).
        for name in collect_source_name_references(child) {
            if prior_names.contains(&name) {
                return true;
            }
        }
        // Then add this child's coordinates to the prior set.
        for n in child.coordinate_names() {
            if !prior_names.contains(&n) {
                prior_names.push(n);
            }
        }
    }
    false
}

/// Extract `{name}` interpolation references from source
/// expressions in a comprehension subtree. Sources that carry
/// raw strings (`Generator`, `WorkloadParamList`) are walked;
/// `Literal`, `IntRange`, `ContinuousInterval`, `Distribution`
/// contain no string references.
fn collect_source_name_references(c: &Comprehension) -> Vec<String> {
    let mut out = Vec::new();
    walk_source_refs(c, &mut out);
    out
}

fn walk_source_refs(c: &Comprehension, out: &mut Vec<String>) {
    match c {
        Comprehension::Clause { source, .. } => {
            extract_source_refs(source, out);
        }
        Comprehension::Cartesian { children } | Comprehension::Zip { children, .. } | Comprehension::Union { children } => {
            for c in children {
                walk_source_refs(c, out);
            }
        }
        Comprehension::Filter { child, .. } | Comprehension::Order { child, .. } => {
            walk_source_refs(child, out);
        }
    }
}

fn extract_source_refs(source: &Source, out: &mut Vec<String>) {
    let s = match source {
        Source::Generator { expr, .. } => expr.as_str(),
        Source::WorkloadParamList { name, .. } => name.as_str(),
        _ => return,
    };
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{'
            && let Some(close) = s[i + 1..].find('}') {
                let name = s[i + 1..i + 1 + close].trim();
                if !name.is_empty()
                    && name.chars().all(|c| c.is_alphanumeric() || c == '_')
                    && !out.contains(&name.to_string())
                {
                    out.push(name.to_string());
                }
                i += close + 2;
                continue;
            }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::{LiteralValue, Source};

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    fn continuous_clause(name: &str) -> Comprehension {
        Comprehension::clause(
            name,
            Source::ContinuousInterval {
                interval: Interval::closed(0.0, 1.0),
                measure: ProductMeasure::Uniform,
            },
        )
    }

    #[test]
    fn clause_metadata_for_bounded_source() {
        let m = clause("k", &[1, 2, 3]).metadata();
        assert_eq!(m.cardinality, CardinalityClass::Bounded(3));
        assert_eq!(
            m.index_addressable,
            Some(IndexFn::Lattice { axis_sizes: vec![3] })
        );
        assert_eq!(m.natural_order, NaturalOrder::Lex);
        assert_eq!(m.materialization, Materialization::Streaming);
    }

    #[test]
    fn clause_metadata_for_continuous_source() {
        let m = continuous_clause("alpha").metadata();
        assert!(matches!(m.cardinality, CardinalityClass::Continuous { .. }));
        assert!(matches!(m.index_addressable, Some(IndexFn::Continuous { .. })));
        assert_eq!(m.natural_order, NaturalOrder::PendingSampling);
        assert_eq!(m.materialization, Materialization::Streaming);
    }

    #[test]
    fn cartesian_metadata_combines_lattice_axes() {
        let c = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("limit", &[10, 20, 30])]);
        let m = c.metadata();
        assert_eq!(m.cardinality, CardinalityClass::Bounded(6));
        assert_eq!(
            m.index_addressable,
            Some(IndexFn::Lattice { axis_sizes: vec![2, 3] })
        );
        assert_eq!(m.natural_order, NaturalOrder::Lex);
    }

    #[test]
    fn cartesian_metadata_for_hybrid() {
        let c = Comprehension::cartesian(vec![clause("k", &[1, 2, 3, 4]), continuous_clause("theta")]);
        let m = c.metadata();
        match m.cardinality {
            CardinalityClass::Hybrid(h) => {
                assert_eq!(h.discrete_axes, vec![4]);
                assert_eq!(h.continuous_axes.len(), 1);
            }
            other => panic!("expected Hybrid, got {other:?}"),
        }
        assert!(matches!(m.index_addressable, Some(IndexFn::Hybrid { .. })));
        assert_eq!(m.natural_order, NaturalOrder::PendingSampling);
    }

    #[test]
    fn dependent_cartesian_produces_none_addressable() {
        // clause replicas references {k} from the prior clause.
        let dependent = Comprehension::cartesian(vec![
            clause("k", &[1, 2, 3]),
            Comprehension::clause(
                "replicas",
                Source::Generator {
                    expr: "range(0, 2 * {k})".into(),
                    cardinality_hint: Some(6),
                },
            ),
        ]);
        let m = dependent.metadata();
        assert!(m.index_addressable.is_none());
    }

    #[test]
    fn zip_strict_produces_lockstep_index_fn() {
        let c = Comprehension::zip(
            vec![clause("x", &[1, 2, 3]), clause("y", &[10, 20, 30])],
            ZipMode::Strict,
        );
        let m = c.metadata();
        assert_eq!(m.index_addressable, Some(IndexFn::Lockstep { length: 3 }));
        assert_eq!(m.natural_order, NaturalOrder::Lockstep);
        assert_eq!(m.materialization, Materialization::Streaming);
    }

    #[test]
    fn zip_cycle_produces_modular_index_fn_and_barrier() {
        let c = Comprehension::zip(
            vec![clause("k", &[1, 2, 3, 4, 5]), clause("color", &[1, 2, 3])],
            ZipMode::Cycle,
        );
        let m = c.metadata();
        match m.index_addressable {
            Some(IndexFn::Modular { axis_sizes }) => {
                assert_eq!(axis_sizes, vec![5, 3]);
            }
            other => panic!("expected Modular, got {other:?}"),
        }
        // shorter child cardinality = 3 → barrier size 3
        assert_eq!(
            m.materialization,
            Materialization::BoundedBarrier { working_set_size: 3 }
        );
    }

    #[test]
    fn union_produces_concatenation_index_fn() {
        let a = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("limit", &[10])]);
        let b = Comprehension::cartesian(vec![clause("k", &[3, 4]), clause("limit", &[20])]);
        let u = Comprehension::union(vec![a, b]);
        let m = u.metadata();
        assert_eq!(m.cardinality, CardinalityClass::Bounded(4));
        assert_eq!(
            m.index_addressable,
            Some(IndexFn::Concatenation { segment_sizes: vec![2, 2] })
        );
        assert_eq!(m.natural_order, NaturalOrder::Sequential);
    }

    #[test]
    fn filter_destroys_addressability() {
        let inner = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("limit", &[10, 20])]);
        let filtered = Comprehension::filter(inner, "{k} > 0");
        let m = filtered.metadata();
        assert_eq!(m.cardinality, CardinalityClass::BoundedAtMost(4));
        assert_eq!(m.index_addressable, None);
    }

    #[test]
    fn lex_order_inherits_addressability() {
        let inner = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("limit", &[10, 20])]);
        let ordered = Comprehension::order(inner, StrategyName::Lex, Some(2));
        let m = ordered.metadata();
        assert_eq!(m.cardinality, CardinalityClass::Bounded(2));
        assert!(matches!(m.index_addressable, Some(IndexFn::Lattice { .. })));
        assert_eq!(m.natural_order, NaturalOrder::Lex);
    }

    #[test]
    fn non_lex_order_drops_ast_level_addressability() {
        let inner = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("limit", &[10, 20])]);
        let ordered = Comprehension::order(inner, StrategyName::Halton, Some(2));
        let m = ordered.metadata();
        assert!(m.index_addressable.is_none());
        match m.natural_order {
            NaturalOrder::Strategy(StrategyName::Halton) => {}
            other => panic!("expected Strategy(Halton), got {other:?}"),
        }
        assert_eq!(
            m.materialization,
            Materialization::BoundedBarrier { working_set_size: 2 }
        );
    }

    #[test]
    fn continuous_sampling_yields_bounded_cardinality() {
        let inner = Comprehension::cartesian(vec![continuous_clause("alpha"), continuous_clause("beta")]);
        let ordered = Comprehension::order(inner, StrategyName::Halton, Some(100));
        let m = ordered.metadata();
        assert_eq!(m.cardinality, CardinalityClass::Bounded(100));
        assert_eq!(
            m.materialization,
            Materialization::BoundedBarrier { working_set_size: 100 }
        );
    }

    #[test]
    fn metadata_propagation_is_idempotent() {
        let c = Comprehension::order(
            Comprehension::filter(
                Comprehension::cartesian(vec![clause("k", &[1, 2, 3]), clause("limit", &[10, 20])]),
                "{k} * {limit} > 5",
            ),
            StrategyName::Halton,
            Some(5),
        );
        let m1 = c.metadata();
        let m2 = c.metadata();
        assert_eq!(m1, m2);
    }

    #[test]
    fn has_continuous_axis_classifier() {
        let lat = IndexFn::Lattice { axis_sizes: vec![3, 4] };
        assert!(!lat.has_continuous_axis());

        let cont = IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        };
        assert!(cont.has_continuous_axis());
    }

    #[test]
    fn multi_axis_lattice_classifier() {
        assert!(IndexFn::Lattice { axis_sizes: vec![3, 4] }.is_multi_axis_lattice());
        assert!(!IndexFn::Lattice { axis_sizes: vec![3] }.is_multi_axis_lattice());
        assert!(!IndexFn::Continuous {
            intervals: vec![Interval::closed(0.0, 1.0), Interval::closed(0.0, 1.0)],
            measure: ProductMeasure::Uniform,
        }
        .is_multi_axis_lattice());
    }
}
