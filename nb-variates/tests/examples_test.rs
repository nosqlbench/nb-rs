// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Living tests for the literate .gk examples in tests/examples/.
//!
//! Each test programmatically assembles the DAG described by its
//! corresponding .gk file and verifies the expected behavior.
//! When the DSL parser is implemented, these tests will be replaced
//! by direct parsing of the .gk files.

use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::nodes::arithmetic::{
    AddU64, DivU64, Interleave, MixedRadix, ModU64,
};
use nb_variates::nodes::hash::{Hash64, HashRange};

// ---------------------------------------------------------------
// hello_world.gk
//
//   coordinates := (cycle)
//   hashed := hash(cycle)
//   user_id := mod(hashed, 1000000)
// ---------------------------------------------------------------

fn build_hello_world() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
    asm.add_node("user_id", Box::new(ModU64::new(1_000_000)), vec![WireRef::node("hashed")]);
    asm.add_output("user_id", WireRef::node("user_id"));
    asm.compile().unwrap()
}

#[test]
fn hello_world_bounded() {
    let mut k = build_hello_world();
    for cycle in 0..1000 {
        k.set_inputs(&[cycle]);
        let uid = k.pull("user_id").as_u64();
        assert!(uid < 1_000_000, "cycle {cycle}: user_id={uid}");
    }
}

#[test]
fn hello_world_deterministic() {
    let mut k = build_hello_world();
    k.set_inputs(&[42]);
    let first = k.pull("user_id").as_u64();
    k.set_inputs(&[42]);
    assert_eq!(k.pull("user_id").as_u64(), first);
}

#[test]
fn hello_world_dispersed() {
    // Sequential cycles should not produce sequential user_ids.
    let mut k = build_hello_world();
    let mut vals = Vec::new();
    for cycle in 0..100 {
        k.set_inputs(&[cycle]);
        vals.push(k.pull("user_id").as_u64());
    }
    // Check that the values are not monotonically increasing
    let monotonic = vals.windows(2).all(|w| w[1] > w[0]);
    assert!(!monotonic, "hash should disperse sequential inputs");
}

// ---------------------------------------------------------------
// cartesian_space.gk
//
//   coordinates := (cycle)
//   (region, store, tx) := mixed_radix(cycle, 50, 200, 0)
//   region_h := hash(region)
//   region_code := mod(region_h, 10000)
//   store_h := hash(interleave(region, store))
//   store_code := mod(store_h, 100000)
//   tx_h := hash(interleave(store_h, tx))
//   tx_id := mod(tx_h, 1000000000)
// ---------------------------------------------------------------

fn build_cartesian_space() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("decompose", Box::new(MixedRadix::new(vec![50, 200, 0])),
        vec![WireRef::input("cycle")]);

    asm.add_node("region_h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);
    asm.add_node("region_code", Box::new(ModU64::new(10000)),
        vec![WireRef::node("region_h")]);

    asm.add_node("rs_interleave", Box::new(Interleave::new()),
        vec![WireRef::node_port("decompose", 0), WireRef::node_port("decompose", 1)]);
    asm.add_node("store_h", Box::new(Hash64::new()),
        vec![WireRef::node("rs_interleave")]);
    asm.add_node("store_code", Box::new(ModU64::new(100000)),
        vec![WireRef::node("store_h")]);

    asm.add_node("st_interleave", Box::new(Interleave::new()),
        vec![WireRef::node("store_h"), WireRef::node_port("decompose", 2)]);
    asm.add_node("tx_h", Box::new(Hash64::new()),
        vec![WireRef::node("st_interleave")]);
    asm.add_node("tx_id", Box::new(ModU64::new(1_000_000_000)),
        vec![WireRef::node("tx_h")]);

    asm.add_output("region", WireRef::node_port("decompose", 0));
    asm.add_output("store", WireRef::node_port("decompose", 1));
    asm.add_output("tx", WireRef::node_port("decompose", 2));
    asm.add_output("region_code", WireRef::node("region_code"));
    asm.add_output("store_code", WireRef::node("store_code"));
    asm.add_output("tx_id", WireRef::node("tx_id"));

    asm.compile().unwrap()
}

#[test]
fn cartesian_decomposition_covers_space() {
    let mut k = build_cartesian_space();
    // cycles 0..49 should cover regions 0..49 with store=0, tx=0
    for cycle in 0u64..50 {
        k.set_inputs(&[cycle]);
        assert_eq!(k.pull("region").as_u64(), cycle);
        assert_eq!(k.pull("store").as_u64(), 0);
        assert_eq!(k.pull("tx").as_u64(), 0);
    }
    // cycle 50 wraps to region=0, store=1
    k.set_inputs(&[50]);
    assert_eq!(k.pull("region").as_u64(), 0);
    assert_eq!(k.pull("store").as_u64(), 1);
}

#[test]
fn cartesian_tx_increments() {
    let mut k = build_cartesian_space();
    // 50 regions × 200 stores = 10,000 cycles per tx increment
    k.set_inputs(&[0]);
    assert_eq!(k.pull("tx").as_u64(), 0);
    k.set_inputs(&[10_000]);
    assert_eq!(k.pull("tx").as_u64(), 1);
    k.set_inputs(&[20_000]);
    assert_eq!(k.pull("tx").as_u64(), 2);
}

#[test]
fn cartesian_codes_bounded() {
    let mut k = build_cartesian_space();
    for cycle in 0..500 {
        k.set_inputs(&[cycle]);
        assert!(k.pull("region_code").as_u64() < 10000);
        assert!(k.pull("store_code").as_u64() < 100000);
        assert!(k.pull("tx_id").as_u64() < 1_000_000_000);
    }
}

// ---------------------------------------------------------------
// shared_computation.gk
//
//   coordinates := (cycle)
//   user_h := hash(cycle)
//   user_id := mod(user_h, 10000000)
//   user_bucket := mod(user_h, 64)
//   user_shard := mod(user_h, 16)
//   name_h := hash(user_h)
//   name_idx := mod(name_h, 50000)
//   age_h := hash(name_h)
//   account_age_days := mod(age_h, 3650)
// ---------------------------------------------------------------

fn build_shared_computation() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("user_h", Box::new(Hash64::new()),
        vec![WireRef::input("cycle")]);
    asm.add_node("user_id", Box::new(ModU64::new(10_000_000)),
        vec![WireRef::node("user_h")]);
    asm.add_node("user_bucket", Box::new(ModU64::new(64)),
        vec![WireRef::node("user_h")]);
    asm.add_node("user_shard", Box::new(ModU64::new(16)),
        vec![WireRef::node("user_h")]);

    asm.add_node("name_h", Box::new(Hash64::new()),
        vec![WireRef::node("user_h")]);
    asm.add_node("name_idx", Box::new(ModU64::new(50000)),
        vec![WireRef::node("name_h")]);

    asm.add_node("age_h", Box::new(Hash64::new()),
        vec![WireRef::node("name_h")]);
    asm.add_node("account_age_days", Box::new(ModU64::new(3650)),
        vec![WireRef::node("age_h")]);

    asm.add_output("user_id", WireRef::node("user_id"));
    asm.add_output("user_bucket", WireRef::node("user_bucket"));
    asm.add_output("user_shard", WireRef::node("user_shard"));
    asm.add_output("name_idx", WireRef::node("name_idx"));
    asm.add_output("account_age_days", WireRef::node("account_age_days"));

    asm.compile().unwrap()
}

#[test]
fn shared_bucket_shard_consistent() {
    // user_bucket = user_h % 64, user_shard = user_h % 16
    // Since 64 is a multiple of 16: user_shard == user_bucket % 16
    let mut k = build_shared_computation();
    for cycle in 0..500 {
        k.set_inputs(&[cycle]);
        let bucket = k.pull("user_bucket").as_u64();
        let shard = k.pull("user_shard").as_u64();
        assert_eq!(shard, bucket % 16,
            "cycle {cycle}: shard={shard} != bucket%16={}", bucket % 16);
    }
}

#[test]
fn shared_fields_bounded() {
    let mut k = build_shared_computation();
    for cycle in 0..500 {
        k.set_inputs(&[cycle]);
        assert!(k.pull("user_id").as_u64() < 10_000_000);
        assert!(k.pull("user_bucket").as_u64() < 64);
        assert!(k.pull("user_shard").as_u64() < 16);
        assert!(k.pull("name_idx").as_u64() < 50000);
        assert!(k.pull("account_age_days").as_u64() < 3650);
    }
}

#[test]
fn shared_chained_hashes_differ() {
    // user_id, name_idx, and account_age_days should generally differ
    // because they derive from different hash chain depths.
    let mut k = build_shared_computation();
    let mut all_same = true;
    for cycle in 0..100 {
        k.set_inputs(&[cycle]);
        let uid = k.pull("user_id").as_u64();
        let nidx = k.pull("name_idx").as_u64();
        let age = k.pull("account_age_days").as_u64();
        if uid != nidx as u64 || uid != age as u64 {
            all_same = false;
            break;
        }
    }
    assert!(!all_same, "chained hashes should produce different field values");
}

// ---------------------------------------------------------------
// multi_coordinate.gk
//
//   coordinates := (cycle, thread)
//   combined := interleave(cycle, thread)
//   row_h := hash(combined)
//   partition := mod(hash(thread), 256)
//   row_key := mod(row_h, 1000000)
//   value_h := hash(row_h)
//   value := mod(value_h, 1000)
// ---------------------------------------------------------------

fn build_multi_coordinate() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into(), "thread".into()]);

    asm.add_node("combined", Box::new(Interleave::new()),
        vec![WireRef::input("cycle"), WireRef::input("thread")]);
    asm.add_node("row_h", Box::new(Hash64::new()),
        vec![WireRef::node("combined")]);

    asm.add_node("thread_h", Box::new(Hash64::new()),
        vec![WireRef::input("thread")]);
    asm.add_node("partition", Box::new(ModU64::new(256)),
        vec![WireRef::node("thread_h")]);

    asm.add_node("row_key", Box::new(ModU64::new(1_000_000)),
        vec![WireRef::node("row_h")]);

    asm.add_node("value_h", Box::new(Hash64::new()),
        vec![WireRef::node("row_h")]);
    asm.add_node("value", Box::new(ModU64::new(1000)),
        vec![WireRef::node("value_h")]);

    asm.add_output("partition", WireRef::node("partition"));
    asm.add_output("row_key", WireRef::node("row_key"));
    asm.add_output("value", WireRef::node("value"));

    asm.compile().unwrap()
}

#[test]
fn multi_coord_same_thread_stable_partition() {
    // The partition depends only on thread, not cycle.
    let mut k = build_multi_coordinate();
    k.set_inputs(&[0, 7]);
    let p1 = k.pull("partition").as_u64();
    k.set_inputs(&[100, 7]);
    let p2 = k.pull("partition").as_u64();
    k.set_inputs(&[99999, 7]);
    let p3 = k.pull("partition").as_u64();
    assert_eq!(p1, p2);
    assert_eq!(p2, p3);
}

#[test]
fn multi_coord_different_threads_different_partitions() {
    let mut k = build_multi_coordinate();
    k.set_inputs(&[0, 0]);
    let p0 = k.pull("partition").as_u64();
    k.set_inputs(&[0, 1]);
    let p1 = k.pull("partition").as_u64();
    // Not strictly guaranteed to differ, but for two small inputs
    // through a good hash + mod 256, collision is very unlikely.
    assert_ne!(p0, p1, "different threads should usually get different partitions");
}

#[test]
fn multi_coord_same_cycle_different_thread_different_row() {
    let mut k = build_multi_coordinate();
    k.set_inputs(&[100, 0]);
    let r0 = k.pull("row_key").as_u64();
    k.set_inputs(&[100, 1]);
    let r1 = k.pull("row_key").as_u64();
    assert_ne!(r0, r1, "interleave should make (cycle,thread) order-dependent");
}

#[test]
fn multi_coord_bounded() {
    let mut k = build_multi_coordinate();
    for cycle in 0..100 {
        for thread in 0..8 {
            k.set_inputs(&[cycle, thread]);
            assert!(k.pull("partition").as_u64() < 256);
            assert!(k.pull("row_key").as_u64() < 1_000_000);
            assert!(k.pull("value").as_u64() < 1000);
        }
    }
}

// ---------------------------------------------------------------
// hashing_provenance.gk
//
//   coordinates := (cycle)
//   (tenant, device) := mixed_radix(cycle, 100, 0)
//   tenant_h := hash(tenant)
//   tenant_id := mod(tenant_h, 10000)
//   device_h := hash(interleave(tenant, device))
//   device_id := mod(device_h, 100000)
//   field_a := mod(tenant_h, 1000)
//   field_b := mod(hash(tenant_h), 1000)
//   field_c := mod(hash(hash(tenant_h)), 1000)
// ---------------------------------------------------------------

fn build_hashing_provenance() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("decompose", Box::new(MixedRadix::new(vec![100, 0])),
        vec![WireRef::input("cycle")]);

    // Pattern 1: direct hash
    asm.add_node("tenant_h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);
    asm.add_node("tenant_id", Box::new(ModU64::new(10000)),
        vec![WireRef::node("tenant_h")]);

    // Pattern 2: combined hash
    asm.add_node("td_interleave", Box::new(Interleave::new()),
        vec![WireRef::node_port("decompose", 0), WireRef::node_port("decompose", 1)]);
    asm.add_node("device_h", Box::new(Hash64::new()),
        vec![WireRef::node("td_interleave")]);
    asm.add_node("device_id", Box::new(ModU64::new(100000)),
        vec![WireRef::node("device_h")]);

    // Pattern 3: chained hash
    asm.add_node("field_a", Box::new(ModU64::new(1000)),
        vec![WireRef::node("tenant_h")]);
    asm.add_node("chain_1", Box::new(Hash64::new()),
        vec![WireRef::node("tenant_h")]);
    asm.add_node("field_b", Box::new(ModU64::new(1000)),
        vec![WireRef::node("chain_1")]);
    asm.add_node("chain_2", Box::new(Hash64::new()),
        vec![WireRef::node("chain_1")]);
    asm.add_node("field_c", Box::new(ModU64::new(1000)),
        vec![WireRef::node("chain_2")]);

    asm.add_output("tenant_id", WireRef::node("tenant_id"));
    asm.add_output("device_id", WireRef::node("device_id"));
    asm.add_output("field_a", WireRef::node("field_a"));
    asm.add_output("field_b", WireRef::node("field_b"));
    asm.add_output("field_c", WireRef::node("field_c"));

    asm.compile().unwrap()
}

#[test]
fn provenance_direct_hash_bounded() {
    let mut k = build_hashing_provenance();
    for cycle in 0..500 {
        k.set_inputs(&[cycle]);
        assert!(k.pull("tenant_id").as_u64() < 10000);
    }
}

#[test]
fn provenance_combined_hash_order_matters() {
    // (tenant=1, device=2) should differ from (tenant=2, device=1)
    let mut k = build_hashing_provenance();
    // cycle that gives tenant=1, device=0: cycle=1 → tenant=1, device=0
    // cycle that gives tenant=2, device=0: cycle=2 → tenant=2, device=0
    // These have different tenants, so device_id should differ even with same device=0
    k.set_inputs(&[1]);
    let d1 = k.pull("device_id").as_u64();
    k.set_inputs(&[2]);
    let d2 = k.pull("device_id").as_u64();
    assert_ne!(d1, d2);
}

#[test]
fn provenance_chained_hashes_produce_different_fields() {
    // field_a, field_b, field_c all derive from the same tenant but
    // through different hash chain depths, so they should differ.
    let mut k = build_hashing_provenance();
    let mut any_differ = false;
    for cycle in 0..100 {
        k.set_inputs(&[cycle]);
        let a = k.pull("field_a").as_u64();
        let b = k.pull("field_b").as_u64();
        let c = k.pull("field_c").as_u64();
        assert!(a < 1000);
        assert!(b < 1000);
        assert!(c < 1000);
        if a != b || b != c {
            any_differ = true;
        }
    }
    assert!(any_differ, "chained hashes should produce distinct fields");
}

#[test]
fn provenance_same_tenant_same_fields() {
    // Cycles that map to the same tenant should produce the same
    // tenant_id, field_a, field_b, field_c (tenant repeats every 100 cycles)
    let mut k = build_hashing_provenance();

    k.set_inputs(&[5]); // tenant=5
    let tid1 = k.pull("tenant_id").as_u64();
    let a1 = k.pull("field_a").as_u64();
    let b1 = k.pull("field_b").as_u64();
    let c1 = k.pull("field_c").as_u64();

    k.set_inputs(&[105]); // also tenant=5 (105 % 100 = 5)
    let tid2 = k.pull("tenant_id").as_u64();
    let a2 = k.pull("field_a").as_u64();
    let b2 = k.pull("field_b").as_u64();
    let c2 = k.pull("field_c").as_u64();

    assert_eq!(tid1, tid2, "same tenant → same tenant_id");
    assert_eq!(a1, a2, "same tenant → same field_a");
    assert_eq!(b1, b2, "same tenant → same field_b");
    assert_eq!(c1, c2, "same tenant → same field_c");
}

// ---------------------------------------------------------------
// timeseries.gk — Full workload (abbreviated, matches end_to_end.rs)
//
// Already covered in tests/end_to_end.rs::timeseries_workload_sketch.
// Here we add a few additional invariant checks.
// ---------------------------------------------------------------

fn build_timeseries() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("decompose", Box::new(MixedRadix::new(vec![100, 1000, 0])),
        vec![WireRef::input("cycle")]);

    asm.add_node("tenant_h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);
    asm.add_node("tenant_code", Box::new(ModU64::new(10000)),
        vec![WireRef::node("tenant_h")]);

    asm.add_node("td_interleave", Box::new(Interleave::new()),
        vec![WireRef::node_port("decompose", 0), WireRef::node_port("decompose", 1)]);
    asm.add_node("device_h", Box::new(Hash64::new()),
        vec![WireRef::node("td_interleave")]);
    asm.add_node("device_seq", Box::new(ModU64::new(100000)),
        vec![WireRef::node("device_h")]);

    asm.add_node("time_bucket", Box::new(DivU64::new(1000)),
        vec![WireRef::node_port("decompose", 2)]);
    asm.add_node("timestamp", Box::new(AddU64::new(1_710_000_000_000)),
        vec![WireRef::node_port("decompose", 2)]);

    asm.add_node("dr_interleave", Box::new(Interleave::new()),
        vec![WireRef::node("device_h"), WireRef::node_port("decompose", 2)]);
    asm.add_node("reading_h", Box::new(HashRange::new(1_000_000)),
        vec![WireRef::node("dr_interleave")]);

    asm.add_output("tenant", WireRef::node_port("decompose", 0));
    asm.add_output("device", WireRef::node_port("decompose", 1));
    asm.add_output("reading", WireRef::node_port("decompose", 2));
    asm.add_output("tenant_code", WireRef::node("tenant_code"));
    asm.add_output("device_seq", WireRef::node("device_seq"));
    asm.add_output("time_bucket", WireRef::node("time_bucket"));
    asm.add_output("timestamp", WireRef::node("timestamp"));
    asm.add_output("reading_h", WireRef::node("reading_h"));

    asm.compile().unwrap()
}

#[test]
fn timeseries_timestamp_tracks_reading() {
    let mut k = build_timeseries();
    // Readings 0, 1, 2 → timestamps base+0, base+1, base+2
    for reading in 0u64..10 {
        let cycle = reading * 100_000; // reading = cycle / (100 * 1000)
        k.set_inputs(&[cycle]);
        assert_eq!(k.pull("reading").as_u64(), reading);
        assert_eq!(k.pull("timestamp").as_u64(), 1_710_000_000_000 + reading);
    }
}

#[test]
fn timeseries_same_tenant_across_devices() {
    // All cycles in 0..99 are tenant 0..99, device 0.
    // Cycles 100..199 are tenant 0..99, device 1.
    // Tenant code should be the same for same tenant, different device.
    let mut k = build_timeseries();

    k.set_inputs(&[5]); // tenant=5, device=0
    let tc_d0 = k.pull("tenant_code").as_u64();

    k.set_inputs(&[105]); // tenant=5, device=1
    let tc_d1 = k.pull("tenant_code").as_u64();

    assert_eq!(tc_d0, tc_d1, "same tenant across devices → same tenant_code");
}

#[test]
fn timeseries_different_tenant_same_device() {
    // Tenants 5 and 6 with device 0 should produce different device_seq
    // because interleave(tenant, device) differs.
    let mut k = build_timeseries();

    k.set_inputs(&[5]); // tenant=5, device=0
    let ds1 = k.pull("device_seq").as_u64();

    k.set_inputs(&[6]); // tenant=6, device=0
    let ds2 = k.pull("device_seq").as_u64();

    assert_ne!(ds1, ds2, "different tenant, same device → different device_seq");
}

#[test]
fn timeseries_time_bucket_groups_readings() {
    let mut k = build_timeseries();
    // mixed_radix(cycle, 100, 1000, 0):
    //   reading = cycle / (100 * 1000) = cycle / 100_000
    //   time_bucket = reading / 1000

    k.set_inputs(&[0]); // reading=0, bucket=0
    assert_eq!(k.pull("reading").as_u64(), 0);
    assert_eq!(k.pull("time_bucket").as_u64(), 0);

    // reading=999 → cycle = 999 * 100_000 = 99_900_000
    k.set_inputs(&[99_900_000]);
    assert_eq!(k.pull("reading").as_u64(), 999);
    assert_eq!(k.pull("time_bucket").as_u64(), 0); // 999 / 1000 = 0

    // reading=1000 → cycle = 1000 * 100_000 = 100_000_000
    k.set_inputs(&[100_000_000]);
    assert_eq!(k.pull("reading").as_u64(), 1000);
    assert_eq!(k.pull("time_bucket").as_u64(), 1); // 1000 / 1000 = 1
}
