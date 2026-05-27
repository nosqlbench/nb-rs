// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Partition-typed stdlib nodes — SRD 71 §"Functions that consume
//! partitions".
//!
//! Each node takes a [`crate::cursor_partition::Partition`] value
//! (carried through GK wires as `Value::Ext`) and projects it
//! into the u64 ordinal space the rest of the workload expects.
//! These are the canonical primitives for "use the active
//! partition's range in a per-cycle binding":
//!
//! - [`Cardinality`] — partition size.
//! - [`StartOf`]      — partition's lower bound (inclusive).
//! - [`EndOf`]        — partition's upper bound (exclusive).
//! - [`IdxOf`]        — 0-based partition index.
//! - [`ModIn`]        — modulo-mapped ordinal that stays inside
//!                      the partition.
//! - [`At`]           — bounds-checked offset into the partition.
//! - [`ClampIn`]      — saturating projection into the partition.
//!
//! All seven functions are deterministic and JIT-friendly at
//! the call site (the partition value is effectively-const for
//! a scope activation, so the eval reduces to a small constant
//! arithmetic expression). Per-cycle uses (e.g. `mod_in(cycle, q.cursor)`)
//! pull the partition once per activation and the per-cycle work
//! is just integer math.

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, OutputType, ParamSpec};
use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, SlotType, Value};

/// Helper: downcast a value to a `Partition`, panicking with a
/// clear diagnostic when the input isn't partition-typed.
/// Per-cycle hot path — callers have already gone through
/// validation at compile time.
fn expect_partition<'a>(value: &'a Value, fn_name: &str) -> &'a crate::cursor_partition::Partition {
    value.as_partition().unwrap_or_else(|| {
        panic!(
            "{fn_name}: expected a Partition value, got {:?}; \
             pass a cursor's `.cursor` projection or an iter-var \
             bound by `for: \"p in <param>.partitions\"`",
            value
        )
    })
}

/// `cardinality(p) -> u64` — number of ordinals in the partition.
pub struct Cardinality { meta: NodeMeta }
impl Cardinality {
    pub fn new() -> Self {
        Self { meta: NodeMeta {
            name: "cardinality".into(),
            outs: vec![Port::u64("output")],
            ins: vec![Slot::Wire(Port::new("partition", PortType::Ext))],
        } }
    }
}
impl GkNode for Cardinality {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(expect_partition(&inputs[0], "cardinality").cardinality());
    }
}

/// `start_of(p) -> u64` — partition's start ordinal (inclusive).
pub struct StartOf { meta: NodeMeta }
impl StartOf {
    pub fn new() -> Self {
        Self { meta: NodeMeta {
            name: "start_of".into(),
            outs: vec![Port::u64("output")],
            ins: vec![Slot::Wire(Port::new("partition", PortType::Ext))],
        } }
    }
}
impl GkNode for StartOf {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(expect_partition(&inputs[0], "start_of").start_ord);
    }
}

/// `end_of(p) -> u64` — partition's end ordinal (exclusive).
pub struct EndOf { meta: NodeMeta }
impl EndOf {
    pub fn new() -> Self {
        Self { meta: NodeMeta {
            name: "end_of".into(),
            outs: vec![Port::u64("output")],
            ins: vec![Slot::Wire(Port::new("partition", PortType::Ext))],
        } }
    }
}
impl GkNode for EndOf {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(expect_partition(&inputs[0], "end_of").end_ord);
    }
}

/// `idx_of(p) -> u64` — 0-based position in the partition list.
pub struct IdxOf { meta: NodeMeta }
impl IdxOf {
    pub fn new() -> Self {
        Self { meta: NodeMeta {
            name: "idx_of".into(),
            outs: vec![Port::u64("output")],
            ins: vec![Slot::Wire(Port::new("partition", PortType::Ext))],
        } }
    }
}
impl GkNode for IdxOf {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(expect_partition(&inputs[0], "idx_of").idx);
    }
}

/// `mod_in(n, p) -> u64` — `p.start_ord + (n mod cardinality(p))`.
///
/// Maps an arbitrary integer (typically a per-cycle ordinal) into
/// the partition's range, wrapping. Degenerate cardinality=0
/// returns the partition's start ordinal.
pub struct ModIn { meta: NodeMeta }
impl ModIn {
    pub fn new() -> Self {
        Self { meta: NodeMeta {
            name: "mod_in".into(),
            outs: vec![Port::u64("output")],
            ins: vec![
                Slot::Wire(Port::u64("n")),
                Slot::Wire(Port::new("partition", PortType::Ext)),
            ],
        } }
    }
}
impl GkNode for ModIn {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let n = inputs[0].as_u64();
        let p = expect_partition(&inputs[1], "mod_in");
        let card = p.cardinality();
        outputs[0] = Value::U64(if card == 0 { p.start_ord } else { p.start_ord + (n % card) });
    }
}

/// `at(p, i) -> u64` — bounds-checked `p.start_ord + i`.
///
/// Use when iteration is meant to consume each ordinal exactly
/// once. Panics at eval time if `i >= cardinality(p)`. Prefer
/// `mod_in` for the wrapping case.
pub struct At { meta: NodeMeta }
impl At {
    pub fn new() -> Self {
        Self { meta: NodeMeta {
            name: "at".into(),
            outs: vec![Port::u64("output")],
            ins: vec![
                Slot::Wire(Port::new("partition", PortType::Ext)),
                Slot::Wire(Port::u64("i")),
            ],
        } }
    }
}
impl GkNode for At {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let p = expect_partition(&inputs[0], "at");
        let i = inputs[1].as_u64();
        let card = p.cardinality();
        if i >= card {
            panic!(
                "at({}, {i}): index out of range — partition #{} cardinality is {card}",
                p.start_ord, p.idx
            );
        }
        outputs[0] = Value::U64(p.start_ord + i);
    }
}

/// `partitions(spec) -> PartitionList` — parse a string spec
/// into a list of partitions. The base extent for resolution
/// is taken from a constant arg or defaults to 100 (so a
/// pure-percentage spec produces partitions in [0, 100) ordinal
/// space). Useful for constructing partition values inline
/// when a cursor's `over` clause needs an explicit list, and
/// for inspecting / iterating partitions outside a cursor
/// context.
pub struct PartitionsOf {
    meta: NodeMeta,
    extent: u64,
}
impl PartitionsOf {
    pub fn new(extent: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "partitions".into(),
                outs: vec![Port::new("output", PortType::Ext)],
                ins: vec![
                    Slot::Wire(Port::new("spec", PortType::Str)),
                    Slot::const_u64("extent", extent),
                ],
            },
            extent,
        }
    }
}
impl GkNode for PartitionsOf {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let spec_str = match &inputs[0] {
            Value::Str(s) => s.to_string(),
            other => panic!(
                "partitions: expected a Str spec, got {:?}; \
                 pass a literal like \"fib:7\" or a string-typed wire",
                other
            ),
        };
        let spec = crate::cursor_partition::parse(&spec_str)
            .unwrap_or_else(|e| panic!("partitions: bad spec `{spec_str}`: {e}"));
        let parts = crate::cursor_partition::resolve(&spec, 0, self.extent)
            .unwrap_or_else(|e| panic!("partitions: resolve failed: {e}"));
        outputs[0] = Value::from_partition_list(parts);
    }
}

/// `clamp_in(n, p) -> u64` — saturating projection into the partition.
///
/// `max(p.start_ord, min(n, p.end_ord - 1))`. Unlike `mod_in`,
/// values outside the partition saturate at the boundary rather
/// than wrapping. Degenerate cardinality=0 returns the start.
pub struct ClampIn { meta: NodeMeta }
impl ClampIn {
    pub fn new() -> Self {
        Self { meta: NodeMeta {
            name: "clamp_in".into(),
            outs: vec![Port::u64("output")],
            ins: vec![
                Slot::Wire(Port::u64("n")),
                Slot::Wire(Port::new("partition", PortType::Ext)),
            ],
        } }
    }
}
impl GkNode for ClampIn {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let n = inputs[0].as_u64();
        let p = expect_partition(&inputs[1], "clamp_in");
        outputs[0] = Value::U64(if p.cardinality() == 0 {
            p.start_ord
        } else {
            n.max(p.start_ord).min(p.end_ord - 1)
        });
    }
}

/// Signatures for partition-typed nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "cardinality", category: C::Arithmetic, outputs: 1,
            description: "number of ordinals in a Partition",
            help: "Returns end_ord - start_ord. The partition is effectively-const for the scope activation, so this evaluates once.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "partition", slot_type: SlotType::Wire, required: true, example: "q.cursor", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
        FuncSig {
            name: "start_of", category: C::Arithmetic, outputs: 1,
            description: "partition's start ordinal (inclusive)",
            help: "Returns the absolute ordinal at the partition's lower bound.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "partition", slot_type: SlotType::Wire, required: true, example: "q.cursor", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
        FuncSig {
            name: "end_of", category: C::Arithmetic, outputs: 1,
            description: "partition's end ordinal (exclusive)",
            help: "Returns the absolute ordinal at the partition's upper bound.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "partition", slot_type: SlotType::Wire, required: true, example: "q.cursor", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
        FuncSig {
            name: "idx_of", category: C::Arithmetic, outputs: 1,
            description: "partition's 0-based index in its list",
            help: "Returns the partition's position in the partition list. Use for labels, metric tags, and iteration-aware diagnostics.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "partition", slot_type: SlotType::Wire, required: true, example: "q.cursor", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
        FuncSig {
            name: "mod_in", category: C::Arithmetic, outputs: 1,
            description: "modulo-map an integer into a partition's range",
            help: "p.start_ord + (n mod cardinality(p)). The canonical \"pick a deterministic per-cycle ordinal that stays inside the active partition\" idiom.\nExample: mod_in(cycle, q.cursor) — wraps cycle into q's narrowed range.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "n", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "partition", slot_type: SlotType::Wire, required: true, example: "q.cursor", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
        FuncSig {
            name: "at", category: C::Arithmetic, outputs: 1,
            description: "bounds-checked offset into a partition",
            help: "p.start_ord + i. Panics at eval time if i >= cardinality(p). Use when iteration is meant to consume each ordinal exactly once; prefer mod_in for the wrapping case.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "partition", slot_type: SlotType::Wire, required: true, example: "q.cursor", constraint: None },
                ParamSpec { name: "i", slot_type: SlotType::Wire, required: true, example: "row", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
        FuncSig {
            name: "partitions", category: C::Arithmetic, outputs: 1,
            description: "parse a partition spec string into a list of resolved partitions",
            help: "partitions(\"fib:7\") → 7-element PartitionList. The optional `extent` const arg (default 100) is the ordinal space the partitions are resolved against; pure-percentage specs produce [0, extent) partitions, useful for inline list construction outside a cursor's `over` clause.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "spec", slot_type: SlotType::Wire, required: true, example: "\"fib:7\"", constraint: None },
                ParamSpec { name: "extent", slot_type: SlotType::ConstU64, required: false, example: "1000", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
        FuncSig {
            name: "clamp_in", category: C::Arithmetic, outputs: 1,
            description: "saturating projection of an integer into a partition's range",
            help: "max(p.start_ord, min(n, p.end_ord - 1)). Unlike mod_in, values outside the partition saturate at the boundary rather than wrapping.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "n", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "partition", slot_type: SlotType::Wire, required: true, example: "q.cursor", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: OutputType::Fixed,
        },
    ]
}

/// Try to build a partition-typed node from a function name.
pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef],
    _wire_types: &[PortType],
    consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn GkNode>, String>> {
    match name {
        "cardinality" => Some(Ok(Box::new(Cardinality::new()))),
        "start_of"    => Some(Ok(Box::new(StartOf::new()))),
        "end_of"      => Some(Ok(Box::new(EndOf::new()))),
        "idx_of"      => Some(Ok(Box::new(IdxOf::new()))),
        "mod_in"      => Some(Ok(Box::new(ModIn::new()))),
        "at"          => Some(Ok(Box::new(At::new()))),
        "clamp_in"    => Some(Ok(Box::new(ClampIn::new()))),
        "partitions"  => {
            let extent = consts.first().map(|c| c.as_u64()).unwrap_or(100);
            Some(Ok(Box::new(PartitionsOf::new(extent))))
        }
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor_partition::Partition;

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
        // n=0 → 100, n=99 → 199, n=100 → 100 (wraps), n=250 → 150
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
        // Inside → as-is. Below start → start. Above end → end-1.
        for (n, expected) in [(50, 100), (100, 100), (150, 150), (199, 199), (200, 199), (1000, 199)] {
            node.eval(&[Value::U64(n), p.clone()], &mut out);
            assert_eq!(out[0].as_u64(), expected, "clamp_in({n}) over [100, 200)");
        }
    }

    #[test]
    #[should_panic(expected = "expected a Partition value")]
    fn non_partition_input_panics_with_clear_diagnostic() {
        let node = Cardinality::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
    }

    #[test]
    fn partitions_node_resolves_spec_against_extent() {
        let node = PartitionsOf::new(1000);
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
        let node = PartitionsOf::new(1000);
        let mut out = [Value::None];
        node.eval(&[Value::Str("0..50%".into())], &mut out);
        let list = out[0].as_partition_list().expect("PartitionList");
        assert_eq!(list.len(), 1);
        assert_eq!(list.as_slice()[0].start_ord, 0);
        assert_eq!(list.as_slice()[0].end_ord, 500);
    }
}
