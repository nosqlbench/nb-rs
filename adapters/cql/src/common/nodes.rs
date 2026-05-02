// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL-specific GK nodes.
//!
//! Currently just [`CqlTimeuuid`] — a deterministic RFC 4122
//! version-1 UUID generator suited for `timeuuid` columns. Lives
//! here (rather than in any one engine adapter) so that every
//! CQL persona registers the same node set and workloads using
//! `cql_timeuuid(...)` are portable across engines.

use nbrs_variates::node::{
    Commutativity, GkNode, NodeMeta, Port, PortType, Slot, SlotType, Value,
};
use nbrs_variates::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};

/// A deterministic CQL `timeuuid` from a `u64` seed.
///
/// Signature: `cql_timeuuid(seed: u64) -> (String)`
///
/// Two xxhash3 passes over the seed produce a 128-bit pattern;
/// the version (`1`, time-based) and variant (`10`, RFC 4122)
/// fields are forced to spec. Same seed always yields the same
/// UUID — useful for replayable inserts into `timeuuid` columns
/// without coordinating a real clock.
///
/// JIT level: P1 (eval only; the `format!` allocates a string).
pub struct CqlTimeuuid {
    meta: NodeMeta,
}

impl Default for CqlTimeuuid {
    fn default() -> Self { Self::new() }
}

impl CqlTimeuuid {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "cql_timeuuid".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("seed"))],
            },
        }
    }
}

impl GkNode for CqlTimeuuid {
    fn meta(&self) -> &NodeMeta { &self.meta }

    /// Derive UUID bits from two xxhash3 passes over the seed.
    /// Bit layout follows RFC 4122 §4.1.
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let seed = inputs[0].as_u64();
        let h1 = xxhash_rust::xxh3::xxh3_64(&seed.to_le_bytes());
        let h2 = xxhash_rust::xxh3::xxh3_64(&h1.to_le_bytes());

        let time_low: u32 = (h1 & 0xFFFF_FFFF) as u32;
        let time_mid: u16 = ((h1 >> 32) & 0xFFFF) as u16;
        let time_hi:  u16 = (((h1 >> 48) & 0x0FFF) as u16) | 0x1000; // version 1
        let clock_seq: u16 = ((h2 & 0x3FFF) as u16) | 0x8000;        // variant RFC 4122
        let node:     u64 = (h2 >> 16) & 0xFFFF_FFFF_FFFF;           // 48-bit node

        outputs[0] = Value::Str(format!(
            "{time_low:08x}-{time_mid:04x}-{time_hi:04x}-{clock_seq:04x}-{node:012x}"
        ));
    }
}

// ---------------------------------------------------------------------------
// GK registry integration
// ---------------------------------------------------------------------------

pub fn cql_signatures() -> &'static [FuncSig] {
    static SIGS: std::sync::OnceLock<Vec<FuncSig>> = std::sync::OnceLock::new();
    SIGS.get_or_init(|| {
        vec![FuncSig {
            name: "cql_timeuuid",
            category: FuncCategory::RealData,
            outputs: 1,
            description: "deterministic CQL timeuuid from seed",
            help: "Generate a deterministic RFC 4122 version-1 UUID string suitable \
                   for CQL timeuuid columns. The same seed always produces the same UUID.\n\
                   Example: cql_timeuuid(hash(cycle))",
            identity: None,
            variadic_ctor: None,
            params: &[ParamSpec {
                name: "seed",
                slot_type: SlotType::Wire,
                required: true,
                example: "cycle",
                constraint: None,
            }],
            arity: Arity::Fixed,
            commutativity: Commutativity::Positional,
    default_resolver: None,
        }]
    })
}

pub(crate) fn cql_build_node(
    name: &str,
    _wires: &[nbrs_variates::assembly::WireRef],
    _consts: &[nbrs_variates::dsl::ConstArg],
) -> Option<Result<Box<dyn GkNode>, String>> {
    match name {
        "cql_timeuuid" => Some(Ok(Box::new(CqlTimeuuid::new()))),
        _ => None,
    }
}

nbrs_variates::register_nodes!(cql_signatures, cql_build_node);

#[cfg(test)]
mod tests {
    use super::*;

    fn run(seed: u64) -> String {
        let node = CqlTimeuuid::new();
        let mut outs = vec![Value::None];
        node.eval(&[Value::U64(seed)], &mut outs);
        match outs.into_iter().next().unwrap() {
            Value::Str(s) => s,
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn deterministic() {
        assert_eq!(run(42), run(42));
    }

    #[test]
    fn different_seeds_differ() {
        assert_ne!(run(0), run(1));
    }

    #[test]
    fn shape_is_uuid_v1() {
        let s = run(0xCAFE_BABE);
        // 8-4-4-4-12 hex
        let parts: Vec<&str> = s.split('-').collect();
        assert_eq!(parts.len(), 5, "expected 5 hyphen-separated fields, got {s}");
        assert_eq!(parts[0].len(), 8, "{s}");
        assert_eq!(parts[1].len(), 4, "{s}");
        assert_eq!(parts[2].len(), 4, "{s}");
        assert_eq!(parts[3].len(), 4, "{s}");
        assert_eq!(parts[4].len(), 12, "{s}");
        // Version field: third group's first hex char must be '1'.
        assert!(parts[2].starts_with('1'), "version must be 1, got {s}");
        // Variant field: fourth group's first hex char must be 8/9/a/b.
        let v = parts[3].chars().next().unwrap();
        assert!(matches!(v, '8' | '9' | 'a' | 'b'), "variant byte must be 10xx, got {s}");
    }
}
