// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-92 / ExecUnification — Step 5a: the unified child-stream contract.
//!
//! One interface by which a unit exposes its children to the executor:
//! [`ChildSource`] (`poll_next` + `realizability`). The executor STEERS on the
//! realizability level (the 03b companion) to pick a drive — bounded-spawn for
//! small/realized child sets, cursor-reserve for huge/ranged ones — while the
//! two drive primitives stay distinct (SRD-02 "One Walker = one configured-limit
//! shape, not one literal harness").
//!
//! This is the ADDITIVE foundation (Step 5a): the trait + enums, with no
//! callers yet. Step 5b implements it over the as-built sources (the scenario
//! node slice, the comprehension generator, the polydat `DataSource` cursor);
//! 5c routes by level; 5d/5e migrate the aggregate drives and the per-cycle
//! stream onto it. The op-curry stays FROZEN (not migrated) — see
//! `local/ExecUnification/11_step5_plan.md`.

// WIP SRD-92 Step 5a — the contract has no non-test callers yet; the allow is
// removed when 5b/5d wire the as-built sources and drives onto it.
#![allow(dead_code)]

use std::ops::Range;

/// How knowable a [`ChildSource`]'s child set is — a graded PROPERTY the
/// executor reads to steer (03b). Cumulative ladder (higher subsumes lower):
/// - `Dynamic`   — pull-only; count unknown ahead (poll / open-ended).
/// - `Countable` — total count known ([`ChildSource::extent`]), not ordinal-indexable.
/// - `Rangeable` — ordinal-indexable; a pull may reserve a whole `Range` (the
///   cursor / cycle stream — `poll_next` IS `DataSource::reserve(stride)`).
/// - `Realizable` — fully materializable ahead (pre-map / dryrun / plan).
///
/// Declared in ascending order so the executor can compare with `>=` when
/// steering. NOT the same as `WrapperLevel` (which is *where a layer is legal*).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Realizability {
    Dynamic,
    Countable,
    Rangeable,
    Realizable,
}

/// One child yielded by [`ChildSource::poll_next`]. Two admissible shapes (the
/// `10` contract) — the AGGREGATE side only; the frozen op-curry is NOT a
/// `ChildSource`:
/// - `Node(index)` — a distinct sub-unit at `index` in the parent's child list
///   (scenario node / comprehension instance); the walker drives `nodes[index]`,
///   resolving its scope by position (as today). The `Realizable`/`Countable`
///   shape: small, distinct sub-units.
/// - `Ordinals(range)` — a reserved ordinal range over the SHARED body (the
///   op-chain built once); the cursor drive runs it per ordinal. The
///   `Rangeable`/`Dynamic` shape: how the 1M cycle stream avoids materializing
///   N units (a pull yields a range, not a unit).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Child {
    Node(usize),
    Ordinals(Range<u64>),
}

/// SRD-92 / ExecUnification — the one interface by which a unit exposes its
/// children. Lazy: `poll_next` produces children on demand and may yield a whole
/// range, so nothing is materialized ahead of need. Re-invocable — the LOOP owns
/// re-driving (Step 6); the source does not know it is being looped. Stable
/// SOURCE order is part of the contract (cursor ordinal order; declaration order
/// for realized children) — execution/completion order may differ under
/// concurrency, which the two-latch fold (Step 3) tolerates.
///
/// Object-safe (no generics, no `Self`-by-value) so the executor can hold a
/// `&mut dyn ChildSource`.
pub trait ChildSource {
    /// Pull the next child, or `None` when drained.
    fn poll_next(&mut self) -> Option<Child>;

    /// The steering property (see [`Realizability`]).
    fn realizability(&self) -> Realizability;

    /// Total child count when known (`Countable` and up) — queried for planning
    /// (pre-map / progress / partition) WITHOUT draining the stream. `None` for
    /// `Dynamic`. Best-effort for extending streams (a re-readable snapshot).
    fn extent(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A realized, distinct-sub-unit source (the scenario/comprehension shape).
    struct VecSource(std::vec::IntoIter<usize>, u64);
    impl ChildSource for VecSource {
        fn poll_next(&mut self) -> Option<Child> {
            self.0.next().map(Child::Node)
        }
        fn realizability(&self) -> Realizability {
            Realizability::Realizable
        }
        fn extent(&self) -> Option<u64> {
            Some(self.1)
        }
    }

    /// A ranged, shared-body source (the cursor/cycle shape) — `poll_next`
    /// reserves a stride-2 ordinal range, the way the cursor does.
    struct RangeSource {
        next: u64,
        end: u64,
    }
    impl ChildSource for RangeSource {
        fn poll_next(&mut self) -> Option<Child> {
            if self.next >= self.end {
                return None;
            }
            let lo = self.next;
            self.next = (lo + 2).min(self.end);
            Some(Child::Ordinals(lo..self.next))
        }
        fn realizability(&self) -> Realizability {
            Realizability::Rangeable
        }
        fn extent(&self) -> Option<u64> {
            Some(self.end)
        }
    }

    #[test]
    fn realizable_vec_source_drains_in_order() {
        let mut s = VecSource(vec![0, 1, 2].into_iter(), 3);
        assert_eq!(s.realizability(), Realizability::Realizable);
        assert_eq!(s.extent(), Some(3));
        let mut got = vec![];
        while let Some(Child::Node(i)) = s.poll_next() {
            got.push(i);
        }
        assert_eq!(got, vec![0, 1, 2]);
    }

    #[test]
    fn rangeable_source_yields_ordinal_ranges() {
        let mut s = RangeSource { next: 0, end: 5 };
        assert_eq!(s.realizability(), Realizability::Rangeable);
        let mut ranges = vec![];
        while let Some(Child::Ordinals(r)) = s.poll_next() {
            ranges.push(r);
        }
        assert_eq!(ranges, vec![0..2, 2..4, 4..5]);
    }

    #[test]
    fn realizability_ladder_is_ordered() {
        assert!(Realizability::Dynamic < Realizability::Countable);
        assert!(Realizability::Countable < Realizability::Rangeable);
        assert!(Realizability::Rangeable < Realizability::Realizable);
    }
}
