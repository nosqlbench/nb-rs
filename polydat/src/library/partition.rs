// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Partition-typed stdlib nodes — SRD 71 §"Functions that consume
//! partitions".
//!
//! Each node takes a [`crate::iteration::cursor_partition::Partition`] value
//! (carried through Polydat wires as `Value::Ext`) via the
//! [`crate::derive_support::Ext`] combinator and projects it into
//! the u64 ordinal space the rest of the workload expects. These
//! are the canonical primitives for "use the active partition's
//! range in a per-cycle binding":
//!
//! - `cardinality` — partition size.
//! - `start_of`    — partition's lower bound (inclusive).
//! - `end_of`      — partition's upper bound (exclusive).
//! - `idx_of`      — 0-based partition index.
//! - `mod_in`      — modulo-mapped ordinal that stays inside
//!   the partition.
//! - `at`          — bounds-checked offset into the partition.
//! - `clamp_in`    — saturating projection into the partition.
//! - `random_in`   — hash-mapped ordinal inside the partition,
//!   deterministic per seed.
//! - `subdivide`   — split a partition into n near-equal
//!   sub-partitions.
//! - `partitions`  — parse a string spec into a `PartitionList`.
//!
//! All of these are deterministic and JIT-friendly at the call
//! site (the partition value is effectively-const for a scope
//! activation, so the eval reduces to a small constant
//! arithmetic expression).
//!
//! Naming note: `subdivide` here takes a *partition* and
//! returns sub-partitions. The numeric comprehension generator
//! that yields evenly spaced *values* over a `[start, end)`
//! interval is `linear_starts(start, end, n)` (with
//! `linear_steps` as its inclusive fence-post sibling) — see
//! SRD 18c.

use crate::derive_support::Ext;
use crate::iteration::cursor_partition::{Partition, PartitionList};

/// Number of ordinals in the partition.
#[crate::polydat_node(category = Arithmetic)]
fn cardinality(partition: Ext<Partition>) -> u64 {
    partition.cardinality()
}

/// Partition's start ordinal (inclusive).
#[crate::polydat_node(category = Arithmetic)]
fn start_of(partition: Ext<Partition>) -> u64 {
    partition.start_ord
}

/// Partition's end ordinal (exclusive).
#[crate::polydat_node(category = Arithmetic)]
fn end_of(partition: Ext<Partition>) -> u64 {
    partition.end_ord
}

/// 0-based position in the partition list.
#[crate::polydat_node(category = Arithmetic)]
fn idx_of(partition: Ext<Partition>) -> u64 {
    partition.idx
}

/// Total number of partitions in the list this partition was
/// resolved as part of. `1` for a single-partition spec. The
/// function spelling of the `partition_count` projection —
/// pairs with `idx_of` for "i of n" labelling.
#[crate::polydat_node(category = Arithmetic)]
fn count_of(partition: Ext<Partition>) -> u64 {
    partition.count
}

/// `mod_in(n, p) = p.start_ord + (n mod cardinality(p))`. Maps an
/// arbitrary integer (typically a per-cycle ordinal) into the
/// partition's range, wrapping. Degenerate cardinality=0 returns
/// the partition's start ordinal.
#[crate::polydat_node(category = Arithmetic)]
fn mod_in(n: u64, partition: Ext<Partition>) -> u64 {
    let card = partition.cardinality();
    if card == 0 { partition.start_ord } else { partition.start_ord + (n % card) }
}

/// `at(p, i)` — bounds-checked `p.start_ord + i`. Use when
/// iteration is meant to consume each ordinal exactly once.
/// Panics at eval time if `i >= cardinality(p)`. Prefer `mod_in`
/// for the wrapping case.
#[crate::polydat_node(category = Arithmetic)]
fn at(partition: Ext<Partition>, i: u64) -> u64 {
    let card = partition.cardinality();
    if i >= card {
        panic!(
            "at({}, {i}): index out of range — partition #{} cardinality is {card}",
            partition.start_ord, partition.idx
        );
    }
    partition.start_ord + i
}

/// `clamp_in(n, p)` — saturating projection into the partition.
/// `max(p.start_ord, min(n, p.end_ord - 1))`. Unlike `mod_in`,
/// values outside the partition saturate at the boundary rather
/// than wrapping. Degenerate cardinality=0 returns the start.
#[crate::polydat_node(category = Arithmetic)]
fn clamp_in(n: u64, partition: Ext<Partition>) -> u64 {
    if partition.cardinality() == 0 {
        partition.start_ord
    } else {
        n.max(partition.start_ord).min(partition.end_ord - 1)
    }
}

/// `random_in(p, seed)` — deterministic hash-mapped ordinal
/// inside the partition: `p.start_ord + hash(seed) mod
/// cardinality(p)`. Same xxHash3 entropy source as `hash(...)`,
/// so equal seeds always land on the same ordinal. Use for
/// random-access patterns that must stay inside the active
/// partition; prefer `mod_in` when sequential coverage matters.
/// Degenerate cardinality=0 returns the partition's start.
#[crate::polydat_node(category = Hashing)]
fn random_in(partition: Ext<Partition>, seed: u64) -> u64 {
    let card = partition.cardinality();
    if card == 0 {
        partition.start_ord
    } else {
        partition.start_ord + crate::library::hash::splitmix64_u64(seed) % card
    }
}

/// `subdivide(p, n)` — split a partition into `n` contiguous
/// sub-partitions whose sizes differ by at most one ordinal.
/// Indices restart at 0; `base_extent` propagates from the
/// parent; the percentage fields interpolate the parent's
/// span. Boundaries match the `*/N` spec tail token exactly
/// (both route through the same splitter). Panics at eval time
/// when `n` is 0 or exceeds the partition's cardinality —
/// every sub-partition must be non-empty.
#[crate::polydat_node(category = Arithmetic)]
fn subdivide(partition: Ext<Partition>, n: u64) -> Ext<PartitionList> {
    let parts = crate::iteration::cursor_partition::subdivide_partition(&partition, n)
        .unwrap_or_else(|e| panic!("{e}"));
    Ext(PartitionList(std::sync::Arc::new(parts)))
}

/// Parse a string spec into a `PartitionList`. The base extent
/// for resolution comes from a constant arg (default 100, so
/// pure-percentage specs produce partitions in [0, 100) ordinal
/// space). Useful for constructing partition values inline when
/// a cursor's `over` clause needs an explicit list.
#[crate::polydat_node(category = Arithmetic)]
fn partitions(
    spec: &str,
    #[poly_default(100u64)] extent: crate::derive_support::Const<u64>,
) -> Ext<PartitionList> {
    let parsed = crate::iteration::cursor_partition::parse(spec)
        .unwrap_or_else(|e| panic!("partitions: bad spec `{spec}`: {e}"));
    let parts = crate::iteration::cursor_partition::resolve(&parsed, 0, *extent)
        .unwrap_or_else(|e| panic!("partitions: resolve failed: {e}"));
    Ext(PartitionList(std::sync::Arc::new(parts)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    fn fixture(idx: u64, start: u64, end: u64) -> Partition {
        Partition {
            idx,
            count: idx + 1,
            start_ord: start,
            end_ord: end,
            start_pct: 0.0,
            end_pct: 0.0,
            base_extent: end,
        }
    }

    #[test]
    fn cardinality_returns_end_minus_start() {
        let node = Cardinality::new();
        let mut out = [Value::None];
        node.eval(&[Value::from_partition(fixture(0, 100, 500))], &mut out);
        assert_eq!(out[0].as_u64(), 400);
    }

    #[test]
    fn start_of_returns_start_ord() {
        let node = StartOf::new();
        let mut out = [Value::None];
        node.eval(&[Value::from_partition(fixture(2, 100, 500))], &mut out);
        assert_eq!(out[0].as_u64(), 100);
    }

    #[test]
    fn end_of_returns_end_ord() {
        let node = EndOf::new();
        let mut out = [Value::None];
        node.eval(&[Value::from_partition(fixture(0, 100, 500))], &mut out);
        assert_eq!(out[0].as_u64(), 500);
    }

    #[test]
    fn idx_of_returns_idx() {
        let node = IdxOf::new();
        let mut out = [Value::None];
        node.eval(&[Value::from_partition(fixture(3, 100, 500))], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }

    #[test]
    fn mod_in_wraps_inside_partition() {
        let node = ModIn::new();
        let mut out = [Value::None];
        let p = Value::from_partition(fixture(0, 100, 200));
        for (n, expected) in [(0, 100), (50, 150), (99, 199), (100, 100), (250, 150)] {
            node.eval(&[Value::U64(n), p.clone()], &mut out);
            assert_eq!(out[0].as_u64(), expected, "mod_in({n}) over [100, 200)");
        }
    }

    #[test]
    fn mod_in_zero_cardinality_returns_start() {
        let node = ModIn::new();
        let mut out = [Value::None];
        let p = Value::from_partition(fixture(0, 100, 100));
        node.eval(&[Value::U64(42), p], &mut out);
        assert_eq!(out[0].as_u64(), 100);
    }

    #[test]
    fn at_offset_within_bounds() {
        let node = At::new();
        let mut out = [Value::None];
        let p = Value::from_partition(fixture(0, 100, 200));
        node.eval(&[p, Value::U64(15)], &mut out);
        assert_eq!(out[0].as_u64(), 115);
    }

    #[test]
    #[should_panic(expected = "index out of range")]
    fn at_offset_out_of_range_panics() {
        let node = At::new();
        let mut out = [Value::None];
        let p = Value::from_partition(fixture(0, 100, 200));
        node.eval(&[p, Value::U64(100)], &mut out);
    }

    #[test]
    fn clamp_in_saturates_at_bounds() {
        let node = ClampIn::new();
        let mut out = [Value::None];
        let p = Value::from_partition(fixture(0, 100, 200));
        for (n, expected) in [(50, 100), (100, 100), (150, 150), (199, 199), (200, 199), (1000, 199)] {
            node.eval(&[Value::U64(n), p.clone()], &mut out);
            assert_eq!(out[0].as_u64(), expected, "clamp_in({n}) over [100, 200)");
        }
    }

    #[test]
    fn random_in_deterministic_and_bounded() {
        let node = RandomIn::new();
        let mut out = [Value::None];
        let p = Value::from_partition(fixture(0, 100, 200));
        let mut first = Vec::new();
        for seed in 0..32u64 {
            node.eval(&[p.clone(), Value::U64(seed)], &mut out);
            let v = out[0].as_u64();
            assert!((100..200).contains(&v), "random_in(seed={seed}) = {v} outside [100, 200)");
            first.push(v);
        }
        // Deterministic: same seeds, same ordinals.
        for (seed, expected) in first.iter().enumerate() {
            node.eval(&[p.clone(), Value::U64(seed as u64)], &mut out);
            assert_eq!(out[0].as_u64(), *expected);
        }
        // Not constant across seeds.
        assert!(first.windows(2).any(|w| w[0] != w[1]));
    }

    #[test]
    fn random_in_zero_cardinality_returns_start() {
        let node = RandomIn::new();
        let mut out = [Value::None];
        node.eval(&[Value::from_partition(fixture(0, 100, 100)), Value::U64(7)], &mut out);
        assert_eq!(out[0].as_u64(), 100);
    }

    #[test]
    fn subdivide_splits_into_near_equal_contiguous_parts() {
        let node = Subdivide::new();
        let mut out = [Value::None];
        let parent = Partition {
            idx: 1,
            count: 2,
            start_ord: 900,
            end_ord: 1000,
            start_pct: 90.0,
            end_pct: 100.0,
            base_extent: 1000,
        };
        node.eval(&[Value::from_partition(parent), Value::U64(10)], &mut out);
        let list = out[0].as_partition_list().expect("PartitionList");
        assert_eq!(list.len(), 10);
        let subs = list.as_slice();
        assert_eq!(subs[0].start_ord, 900);
        assert_eq!(subs[9].end_ord, 1000);
        for (i, s) in subs.iter().enumerate() {
            assert_eq!(s.idx, i as u64, "indices restart at 0");
            assert_eq!(s.cardinality(), 10);
            assert_eq!(s.base_extent, 1000, "base_extent propagates");
        }
        for w in subs.windows(2) {
            assert_eq!(w[0].end_ord, w[1].start_ord, "contiguous");
        }
        // Percentage fields interpolate the parent's span.
        assert!((subs[0].start_pct - 90.0).abs() < 1e-9);
        assert!((subs[4].end_pct - 95.0).abs() < 1e-9);
        assert!((subs[9].end_pct - 100.0).abs() < 1e-9);
    }

    #[test]
    #[should_panic(expected = "non-empty sub-partitions")]
    fn subdivide_finer_than_cardinality_panics() {
        let node = Subdivide::new();
        let mut out = [Value::None];
        node.eval(&[Value::from_partition(fixture(0, 0, 5)), Value::U64(10)], &mut out);
    }

    #[test]
    #[should_panic(expected = "must be >= 1")]
    fn subdivide_zero_count_panics() {
        let node = Subdivide::new();
        let mut out = [Value::None];
        node.eval(&[Value::from_partition(fixture(0, 0, 100)), Value::U64(0)], &mut out);
    }

    #[test]
    fn partitions_node_resolves_spec_against_extent() {
        let node = Partitions::new(1000);
        let mut out = [Value::None];
        node.eval(&[Value::Str("linear:4".into())], &mut out);
        let list = out[0].as_partition_list().expect("PartitionList");
        assert_eq!(list.len(), 4);
        for (i, p) in list.as_slice().iter().enumerate() {
            assert_eq!(p.idx, i as u64);
            assert_eq!(p.cardinality(), 250);
        }
    }

    #[test]
    fn partitions_node_handles_form1_single_range() {
        let node = Partitions::new(1000);
        let mut out = [Value::None];
        node.eval(&[Value::Str("0..50%".into())], &mut out);
        let list = out[0].as_partition_list().expect("PartitionList");
        assert_eq!(list.len(), 1);
        assert_eq!(list.as_slice()[0].start_ord, 0);
        assert_eq!(list.as_slice()[0].end_ord, 500);
    }
}
