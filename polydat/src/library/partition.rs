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
//!                   the partition.
//! - `at`          — bounds-checked offset into the partition.
//! - `clamp_in`    — saturating projection into the partition.
//! - `partitions`  — parse a string spec into a `PartitionList`.
//!
//! All eight functions are deterministic and JIT-friendly at the
//! call site (the partition value is effectively-const for a
//! scope activation, so the eval reduces to a small constant
//! arithmetic expression).

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
