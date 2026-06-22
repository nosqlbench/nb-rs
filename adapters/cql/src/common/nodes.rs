// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL-specific Polydat nodes.
//!
//! Currently just [`CqlTimeuuid`] — a deterministic RFC 4122
//! version-1 UUID generator suited for `timeuuid` columns. Lives
//! here (rather than in any one engine adapter) so that every
//! CQL engine registers the same node set and workloads using
//! `cql_timeuuid(...)` are portable across engines.

// Node metadata + registration are emitted by `#[polydat::polydat_node]`
// (fully-qualified `polydat::…` paths), so no `polydat::ast` /
// `polydat::dsl::registry` imports are needed here.

/// A deterministic CQL `timeuuid` from a `u64` seed.
///
/// Signature: `cql_timeuuid(seed: u64) -> str`. Two xxhash3 passes over the
/// seed produce a 128-bit pattern; the version (`1`, time-based) and variant
/// (`10`, RFC 4122) fields are forced to spec (bit layout per RFC 4122 §4.1).
/// Same seed always yields the same UUID — useful for replayable inserts into
/// `timeuuid` columns without coordinating a real clock.
///
/// Authored via `#[polydat::polydat_node]` (SRD-80b). Pure (deterministic in
/// its seed): const-folds when the seed is const, evaluates per-cycle when
/// the seed is a dynamic wire.
#[polydat::polydat_node(category = RealData)]
fn cql_timeuuid(seed: u64) -> String {
    let h1 = xxhash_rust::xxh3::xxh3_64(&seed.to_le_bytes());
    let h2 = xxhash_rust::xxh3::xxh3_64(&h1.to_le_bytes());

    let time_low: u32 = (h1 & 0xFFFF_FFFF) as u32;
    let time_mid: u16 = ((h1 >> 32) & 0xFFFF) as u16;
    let time_hi:  u16 = (((h1 >> 48) & 0x0FFF) as u16) | 0x1000; // version 1
    let clock_seq: u16 = ((h2 & 0x3FFF) as u16) | 0x8000;        // variant RFC 4122
    let node:     u64 = (h2 >> 16) & 0xFFFF_FFFF_FFFF;           // 48-bit node

    format!("{time_low:08x}-{time_mid:04x}-{time_hi:04x}-{clock_seq:04x}-{node:012x}")
}

#[cfg(test)]
mod tests {
    use polydat::dsl::compile::compile_polydat;

    /// Drive the macro-authored node by feeding the seed as a const literal
    /// (folds through the wire) and pulling the result.
    fn run(seed: u64) -> String {
        let mut k = compile_polydat(&format!("out := cql_timeuuid({seed})"))
            .expect("compile cql_timeuuid");
        k.pull("out").as_str().to_string()
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
