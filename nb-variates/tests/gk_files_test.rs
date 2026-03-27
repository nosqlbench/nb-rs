// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Parse and execute .gk example files as live tests.
//!
//! These tests load the literate .gk files from tests/examples/,
//! parse them through the DSL compiler, and verify the kernels
//! produce correct outputs.

use nb_variates::dsl::compile::compile_gk;

#[test]
fn gk_hello_world() {
    let src = include_str!("examples/hello_world.gk");
    let mut kernel = compile_gk(src).unwrap();

    // Bounded
    for cycle in 0..1000u64 {
        kernel.set_coordinates(&[cycle]);
        let uid = kernel.pull("user_id").as_u64();
        assert!(uid < 1_000_000, "cycle {cycle}: user_id={uid}");
    }

    // Deterministic
    kernel.set_coordinates(&[42]);
    let first = kernel.pull("user_id").as_u64();
    kernel.set_coordinates(&[42]);
    assert_eq!(kernel.pull("user_id").as_u64(), first);
}

#[test]
fn gk_cartesian_space() {
    let src = include_str!("examples/cartesian_space.gk");
    let mut kernel = compile_gk(src).unwrap();

    // First 50 cycles: region 0..49, store=0, tx=0
    for cycle in 0u64..50 {
        kernel.set_coordinates(&[cycle]);
        assert_eq!(kernel.pull("region").as_u64(), cycle);
        assert_eq!(kernel.pull("store").as_u64(), 0);
        assert_eq!(kernel.pull("tx").as_u64(), 0);
    }

    // Cycle 50 wraps to region=0, store=1
    kernel.set_coordinates(&[50]);
    assert_eq!(kernel.pull("region").as_u64(), 0);
    assert_eq!(kernel.pull("store").as_u64(), 1);

    // Codes bounded
    for cycle in 0..100u64 {
        kernel.set_coordinates(&[cycle]);
        assert!(kernel.pull("region_code").as_u64() < 10000);
        assert!(kernel.pull("store_code").as_u64() < 100000);
        assert!(kernel.pull("tx_id").as_u64() < 1_000_000_000);
    }
}

#[test]
fn gk_shared_computation() {
    let src = include_str!("examples/shared_computation.gk");
    let mut kernel = compile_gk(src).unwrap();

    // bucket = user_h % 64, shard = user_h % 16
    // Since 64 is a multiple of 16: shard == bucket % 16
    for cycle in 0..100u64 {
        kernel.set_coordinates(&[cycle]);
        let bucket = kernel.pull("user_bucket").as_u64();
        let shard = kernel.pull("user_shard").as_u64();
        assert!(bucket < 64);
        assert!(shard < 16);
        assert_eq!(shard, bucket % 16);
    }
}

#[test]
fn gk_multi_coordinate() {
    let src = include_str!("examples/multi_coordinate.gk");
    let mut kernel = compile_gk(src).unwrap();

    // Same thread, different cycles → same partition
    kernel.set_coordinates(&[0, 7]);
    let p1 = kernel.pull("partition").as_u64();
    kernel.set_coordinates(&[100, 7]);
    let p2 = kernel.pull("partition").as_u64();
    assert_eq!(p1, p2);

    // All bounded
    for cycle in 0..50u64 {
        for thread in 0..4u64 {
            kernel.set_coordinates(&[cycle, thread]);
            assert!(kernel.pull("partition").as_u64() < 256);
            assert!(kernel.pull("row_key").as_u64() < 1_000_000);
            assert!(kernel.pull("value").as_u64() < 1000);
        }
    }
}

#[test]
fn gk_hashing_provenance() {
    let src = include_str!("examples/hashing_provenance.gk");
    let mut kernel = compile_gk(src).unwrap();

    // Same tenant across cycles that map to the same tenant
    kernel.set_coordinates(&[5]);
    let tid1 = kernel.pull("tenant_id").as_u64();
    let a1 = kernel.pull("field_a").as_u64();
    kernel.set_coordinates(&[105]); // also tenant=5
    let tid2 = kernel.pull("tenant_id").as_u64();
    let a2 = kernel.pull("field_a").as_u64();
    assert_eq!(tid1, tid2);
    assert_eq!(a1, a2);

    // Chained hashes produce different fields
    kernel.set_coordinates(&[42]);
    let a = kernel.pull("field_a").as_u64();
    let b = kernel.pull("field_b").as_u64();
    let c = kernel.pull("field_c").as_u64();
    assert!(a < 1000);
    assert!(b < 1000);
    assert!(c < 1000);
    // At least one pair should differ
    assert!(a != b || b != c, "chained hashes should produce different values");
}
