// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Operator IR — spec §9.1.
//!
//! Every well-formed comprehension AST compiles to a finite
//! sequence of these 8 opcodes. Every operator is a stream
//! transducer; operands flow as tuple streams via
//! `advance() -> Option<Tuple>`, never as materialized
//! `Vec<Tuple>`. The two materialization barriers — non-Lex
//! `ORDER_MATERIALIZE` and `ZIP(Cycle)`'s shorter-child
//! buffering — are called out explicitly.

use serde::{Deserialize, Serialize};

use crate::iteration::comprehension::source::Source;
use crate::iteration::comprehension::strategy::{StrategyName, ZipMode};

/// The 8-opcode IR set (spec §9.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// Push a single-name tuple stream produced by `source`.
    /// Streaming; O(1) per pull above the source's own state.
    PushClause { name: String, source: Source },

    /// Replace the top-N stream operands with one stream that
    /// enumerates their cross product in Lex order. Streaming.
    Cartesian { n: usize },

    /// Replace the top-N stream operands with their lockstep
    /// diagonal. Streaming under Strict/Truncate; `Cycle`
    /// buffers each non-longest child.
    Zip { n: usize, mode: ZipMode },

    /// Replace the top-N stream operands with a stream that
    /// concatenates them in operand order. Streaming.
    Union { n: usize },

    /// Wrap the top operand with a per-tuple predicate check.
    /// Streaming.
    Filter { predicate: String },

    /// Wrap the top operand with a counter / pass-through.
    /// Used for `order(Lex, _)` per spec §10.2 R1; this is
    /// the "streaming" order opcode.
    OrderStreaming {
        kind: OrderStreamingKind,
        truncation: Option<u64>,
    },

    /// MATERIALIZATION BARRIER. Build a working set sufficient
    /// for the strategy, apply the strategy, emit permuted
    /// tuples. Truncation is the output cap.
    ///
    /// Per spec §10.2 R2: when the input is index-addressable
    /// and the strategy has a closed-form push-down rule, the
    /// working set shrinks from O(input) to O(output) — the
    /// interpreter realizes this by drawing strategy-specific
    /// multi-indices and looking each up against the input's
    /// `IndexFn` rather than materializing the full input.
    /// Whether R2 fires is encoded in `indexed`.
    ///
    /// `input_index_fn` carries the upstream comprehension's
    /// addressing scheme (per spec §10.7.6 / §10.7.8) so the
    /// strategy's indexed-form algorithms can dispatch
    /// correctly without re-deriving the shape from observed
    /// tuples (which would lose multi-axis lattice structure
    /// after the flat materialization). `None` when the
    /// upstream metadata propagator couldn't claim a closed-
    /// form addressing function.
    OrderMaterialize {
        strategy: StrategyName,
        truncation: Option<u64>,
        /// `true` when R2 push-down applies: the interpreter
        /// should use the strategy's indexed form (draw
        /// multi-indices, look up via input IndexFn).
        /// `false` for the naïve form (materialize input,
        /// then apply).
        indexed: bool,
        /// Upstream input's IndexFn at compile time (spec
        /// §10.7.6). The interpreter passes this into the
        /// [`crate::iteration::comprehension::strategies::EvaluatedInput`]
        /// it builds for [`crate::iteration::comprehension::strategies::Strategy::apply`].
        input_index_fn: Option<crate::iteration::comprehension::metadata::IndexFn>,
    },

    /// Bind the top stream as the comprehension's result.
    /// Must be the last opcode in a well-formed Program.
    Dispense,
}

/// Variant marker for [`Op::OrderStreaming`]. Today only Lex
/// is streaming (per spec §6.2's "streaming order" table);
/// the enum exists so future streaming strategies can land
/// without changing the IR opcode set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStreamingKind {
    Lex,
}

impl Op {
    /// Arity for stack-effect computation: how many stream
    /// operands this opcode pops, and how many it pushes.
    /// Always pushes 1 for stream-producing ops; `Dispense`
    /// pushes 0 (it consumes the final stream).
    pub fn stack_effect(&self) -> (usize, usize) {
        match self {
            Op::PushClause { .. } => (0, 1),
            Op::Cartesian { n } => (*n, 1),
            Op::Zip { n, .. } => (*n, 1),
            Op::Union { n } => (*n, 1),
            Op::Filter { .. } => (1, 1),
            Op::OrderStreaming { .. } => (1, 1),
            Op::OrderMaterialize { .. } => (1, 1),
            Op::Dispense => (1, 0),
        }
    }

    /// `true` if this opcode is a materialization barrier per
    /// spec §6.2 + §6.3. Used by the bounds checker.
    pub fn is_barrier(&self) -> bool {
        match self {
            Op::OrderMaterialize { .. } => true,
            Op::Zip { mode: ZipMode::Cycle, .. } => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stack_effect_basics() {
        assert_eq!(Op::PushClause {
            name: "k".into(),
            source: Source::Literal { values: vec![] },
        }.stack_effect(), (0, 1));
        assert_eq!(Op::Cartesian { n: 3 }.stack_effect(), (3, 1));
        assert_eq!(Op::Dispense.stack_effect(), (1, 0));
    }

    #[test]
    fn barrier_classification() {
        assert!(Op::OrderMaterialize {
            strategy: StrategyName::Halton,
            truncation: Some(10),
            indexed: true,
            input_index_fn: None,
        }.is_barrier());
        assert!(Op::Zip { n: 2, mode: ZipMode::Cycle }.is_barrier());
        assert!(!Op::Zip { n: 2, mode: ZipMode::Strict }.is_barrier());
        assert!(!Op::OrderStreaming {
            kind: OrderStreamingKind::Lex,
            truncation: None,
        }.is_barrier());
    }

    #[test]
    fn serde_round_trip() {
        let op = Op::OrderMaterialize {
            strategy: StrategyName::Halton,
            truncation: Some(50),
            indexed: true,
            input_index_fn: Some(crate::iteration::comprehension::metadata::IndexFn::Lattice {
                axis_sizes: vec![10, 5],
            }),
        };
        let json = serde_json::to_string(&op).unwrap();
        let back: Op = serde_json::from_str(&json).unwrap();
        assert_eq!(op, back);
    }
}
