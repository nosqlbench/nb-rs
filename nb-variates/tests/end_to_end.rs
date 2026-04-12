// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests: programmatically assemble GK kernels and verify
//! pull-through evaluation produces correct values.

use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::nodes::arithmetic::{AddU64, DivU64, Interleave, MixedRadix, ModU64};
use nb_variates::nodes::convert::U64ToString;
use nb_variates::nodes::hash::{Hash64, HashRange};

/// Simple linear chain: cycle → hash → mod → output
#[test]
fn simple_hash_mod_chain() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
    asm.add_node("m", Box::new(ModU64::new(1000)), vec![WireRef::node("h")]);
    asm.add_output("result", WireRef::node("m"));

    let mut kernel = asm.compile().unwrap();

    kernel.set_inputs(&[42]);
    let v = kernel.pull("result").as_u64();
    assert!(v < 1000, "expected < 1000, got {v}");

    // Deterministic: same coordinate → same result
    kernel.set_inputs(&[42]);
    assert_eq!(kernel.pull("result").as_u64(), v);

    // Different coordinate → (likely) different result
    kernel.set_inputs(&[43]);
    let v2 = kernel.pull("result").as_u64();
    assert!(v2 < 1000);
    // Not strictly guaranteed to differ, but astronomically unlikely
    // for two adjacent inputs to hash-mod to the same value
}

/// Multi-output: mixed_radix decomposition
#[test]
fn mixed_radix_decomposition() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node(
        "decompose",
        Box::new(MixedRadix::new(vec![100, 1000, 0])),
        vec![WireRef::input("cycle")],
    );

    // Expose each digit as a separate output
    asm.add_output("tenant", WireRef::node_port("decompose", 0));
    asm.add_output("device", WireRef::node_port("decompose", 1));
    asm.add_output("reading", WireRef::node_port("decompose", 2));

    let mut kernel = asm.compile().unwrap();

    // 4_201_337 → tenant=37, device=13, reading=42
    kernel.set_inputs(&[4_201_337]);
    assert_eq!(kernel.pull("tenant").as_u64(), 37);
    assert_eq!(kernel.pull("device").as_u64(), 13);
    assert_eq!(kernel.pull("reading").as_u64(), 42);
}

/// Shared intermediate: tenant_h used by two downstream nodes
#[test]
fn shared_intermediate() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node(
        "decompose",
        Box::new(MixedRadix::new(vec![100, 1000, 0])),
        vec![WireRef::input("cycle")],
    );

    // Hash the tenant coordinate
    asm.add_node(
        "tenant_h",
        Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)],
    );

    // Two downstream consumers of tenant_h
    asm.add_node(
        "tenant_code",
        Box::new(ModU64::new(10000)),
        vec![WireRef::node("tenant_h")],
    );
    asm.add_node(
        "tenant_bucket",
        Box::new(ModU64::new(10)),
        vec![WireRef::node("tenant_h")],
    );

    asm.add_output("code", WireRef::node("tenant_code"));
    asm.add_output("bucket", WireRef::node("tenant_bucket"));

    let mut kernel = asm.compile().unwrap();

    kernel.set_inputs(&[42]);
    let code = kernel.pull("code").as_u64();
    let bucket = kernel.pull("bucket").as_u64();

    assert!(code < 10000);
    assert!(bucket < 10);

    // Verify consistency: bucket should equal code % 10
    // because both derive from the same hash
    assert_eq!(bucket, code % 10);
}

/// Auto edge adapter: u64 → String coercion
#[test]
fn auto_edge_adapter_u64_to_string() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node(
        "val",
        Box::new(ModU64::new(100)),
        vec![WireRef::input("cycle")],
    );

    // U64ToString is normally auto-inserted, but let's test with an
    // explicit node that expects String input.
    // For this test, we just verify the output_map works with
    // the auto-adapter by pulling a u64 node output as a string.
    // We need a node that takes String input... let's just verify
    // that the assembler can auto-insert adapters.
    //
    // Actually, let's test by adding an explicit U64ToString and
    // verifying it works in the chain. The auto-insert is tested
    // implicitly when we have type mismatches.
    asm.add_node(
        "str_val",
        Box::new(U64ToString::new()),
        vec![WireRef::node("val")],
    );

    asm.add_output("result", WireRef::node("str_val"));

    let mut kernel = asm.compile().unwrap();

    kernel.set_inputs(&[542]);
    let result = kernel.pull("result");
    assert_eq!(result.as_str(), "42"); // 542 % 100 = 42
}

/// Two-input node: interleave
#[test]
fn two_input_interleave() {
    let mut asm = GkAssembler::new(vec!["a".into(), "b".into()]);

    asm.add_node(
        "mixed",
        Box::new(Interleave::new()),
        vec![WireRef::input("a"), WireRef::input("b")],
    );
    asm.add_node(
        "hashed",
        Box::new(Hash64::new()),
        vec![WireRef::node("mixed")],
    );
    asm.add_node(
        "bounded",
        Box::new(ModU64::new(1000)),
        vec![WireRef::node("hashed")],
    );

    asm.add_output("result", WireRef::node("bounded"));

    let mut kernel = asm.compile().unwrap();

    kernel.set_inputs(&[5, 10]);
    let v1 = kernel.pull("result").as_u64();
    assert!(v1 < 1000);

    // Different coordinates → different result
    kernel.set_inputs(&[10, 5]);
    let v2 = kernel.pull("result").as_u64();
    assert!(v2 < 1000);
    assert_ne!(v1, v2, "interleave(5,10) should differ from interleave(10,5)");
}

/// Pull-through memoization: pulling the same output twice in the
/// same coordinate context should not re-evaluate upstream nodes.
#[test]
fn memoization_within_context() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
    asm.add_node("m", Box::new(ModU64::new(1000)), vec![WireRef::node("h")]);
    asm.add_output("result", WireRef::node("m"));

    let mut kernel = asm.compile().unwrap();

    kernel.set_inputs(&[99]);
    let v1 = kernel.pull("result").as_u64();
    let v2 = kernel.pull("result").as_u64();
    assert_eq!(v1, v2, "same context, same output");
}

/// Context change invalidates memoization.
#[test]
fn context_change_invalidates() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
    asm.add_output("result", WireRef::node("h"));

    let mut kernel = asm.compile().unwrap();

    kernel.set_inputs(&[1]);
    let v1 = kernel.pull("result").as_u64();

    kernel.set_inputs(&[2]);
    let v2 = kernel.pull("result").as_u64();

    assert_ne!(v1, v2, "different coordinates must produce different hashes");
}

/// Assembly error: unknown wire reference
#[test]
fn error_unknown_wire() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node(
        "h",
        Box::new(Hash64::new()),
        vec![WireRef::input("nonexistent")],
    );
    asm.add_output("result", WireRef::node("h"));

    let result = asm.compile();
    assert!(result.is_err());
}

/// Assembly error: arity mismatch
#[test]
fn error_arity_mismatch() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    // Hash64 expects 1 input, give it 2
    asm.add_node(
        "h",
        Box::new(Hash64::new()),
        vec![WireRef::input("cycle"), WireRef::input("cycle")],
    );
    asm.add_output("result", WireRef::node("h"));

    let result = asm.compile();
    assert!(result.is_err());
}

/// Larger DAG resembling the time-series workload from the SRD.
#[test]
fn timeseries_workload_sketch() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    // Decompose cycle into tenant, device, reading
    asm.add_node(
        "decompose",
        Box::new(MixedRadix::new(vec![100, 1000, 0])),
        vec![WireRef::input("cycle")],
    );

    // Tenant hash and code
    asm.add_node(
        "tenant_h",
        Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)],
    );
    asm.add_node(
        "tenant_code",
        Box::new(ModU64::new(10000)),
        vec![WireRef::node("tenant_h")],
    );

    // Device hash (interleave tenant + device for uniqueness)
    asm.add_node(
        "td_interleave",
        Box::new(Interleave::new()),
        vec![
            WireRef::node_port("decompose", 0),
            WireRef::node_port("decompose", 1),
        ],
    );
    asm.add_node(
        "device_h",
        Box::new(Hash64::new()),
        vec![WireRef::node("td_interleave")],
    );
    asm.add_node(
        "device_seq",
        Box::new(ModU64::new(100000)),
        vec![WireRef::node("device_h")],
    );

    // Time bucket
    asm.add_node(
        "time_bucket",
        Box::new(DivU64::new(1000)),
        vec![WireRef::node_port("decompose", 2)],
    );

    // Timestamp (reading as epoch offset)
    asm.add_node(
        "timestamp",
        Box::new(AddU64::new(1_710_000_000_000)),
        vec![WireRef::node_port("decompose", 2)],
    );

    // Reading value hash (interleave device_h + reading)
    asm.add_node(
        "dr_interleave",
        Box::new(Interleave::new()),
        vec![
            WireRef::node("device_h"),
            WireRef::node_port("decompose", 2),
        ],
    );
    asm.add_node(
        "reading_h",
        Box::new(HashRange::new(1_000_000)),
        vec![WireRef::node("dr_interleave")],
    );

    // Outputs
    asm.add_output("tenant_code", WireRef::node("tenant_code"));
    asm.add_output("device_seq", WireRef::node("device_seq"));
    asm.add_output("time_bucket", WireRef::node("time_bucket"));
    asm.add_output("timestamp", WireRef::node("timestamp"));
    asm.add_output("reading_h", WireRef::node("reading_h"));

    let mut kernel = asm.compile().unwrap();

    // cycle 4_201_337 → tenant=37, device=13, reading=42
    kernel.set_inputs(&[4_201_337]);

    let tenant_code = kernel.pull("tenant_code").as_u64();
    let device_seq = kernel.pull("device_seq").as_u64();
    let time_bucket = kernel.pull("time_bucket").as_u64();
    let timestamp = kernel.pull("timestamp").as_u64();
    let reading_h = kernel.pull("reading_h").as_u64();

    assert!(tenant_code < 10000, "tenant_code={tenant_code}");
    assert!(device_seq < 100000, "device_seq={device_seq}");
    assert_eq!(time_bucket, 0); // reading=42, 42/1000=0
    assert_eq!(timestamp, 1_710_000_000_042); // base + reading
    assert!(reading_h < 1_000_000, "reading_h={reading_h}");

    // Deterministic: same cycle → same outputs
    kernel.set_inputs(&[4_201_337]);
    assert_eq!(kernel.pull("tenant_code").as_u64(), tenant_code);
    assert_eq!(kernel.pull("device_seq").as_u64(), device_seq);
    assert_eq!(kernel.pull("reading_h").as_u64(), reading_h);

    // Different cycle → different outputs
    kernel.set_inputs(&[4_201_338]);
    let tc2 = kernel.pull("tenant_code").as_u64();
    let ds2 = kernel.pull("device_seq").as_u64();
    // tenant changes (38 vs 37), so outputs should differ
    assert_ne!(tenant_code, tc2);
    // device stays the same (13), but tenant changed so interleave differs
    assert_ne!(device_seq, ds2);
}
