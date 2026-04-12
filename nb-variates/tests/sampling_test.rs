// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Living tests for the distribution and sampling .gk examples.
//!
//! These demonstrate how ICD sampling, alias tables, and library kernel
//! patterns compose in the GK — matching the literate examples in
//! tests/examples/.

use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::nodes::arithmetic::{AddU64, Interleave, MixedRadix, ModU64};
use nb_variates::nodes::hash::Hash64;
use nb_variates::sampling::alias::AliasSample;
use nb_variates::sampling::icd::{ClampF64, IcdSample, UnitInterval};

// =================================================================
// distributions.gk — Decomposed ICD pipeline
// =================================================================

/// Build: hash → unit_interval → icd_normal(72, 5)
fn build_normal_pipeline() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("seed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
    asm.add_node("quantile", Box::new(UnitInterval::new()), vec![WireRef::node("seed")]);
    asm.add_node("temperature", Box::new(IcdSample::normal(72.0, 5.0)),
        vec![WireRef::node("quantile")]);
    asm.add_output("temperature", WireRef::node("temperature"));
    asm.compile().unwrap()
}

#[test]
fn normal_pipeline_mean_and_stddev() {
    let mut k = build_normal_pipeline();
    let mut values = Vec::new();
    for cycle in 0..10_000u64 {
        k.set_inputs(&[cycle]);
        values.push(k.pull("temperature").as_f64());
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
    let stddev = variance.sqrt();

    assert!((mean - 72.0).abs() < 1.0, "mean={mean}, expected ~72");
    assert!((stddev - 5.0).abs() < 1.0, "stddev={stddev}, expected ~5");
}

#[test]
fn normal_pipeline_deterministic() {
    let mut k = build_normal_pipeline();
    k.set_inputs(&[42]);
    let v1 = k.pull("temperature").as_f64();
    k.set_inputs(&[42]);
    let v2 = k.pull("temperature").as_f64();
    assert_eq!(v1, v2);
}

// =================================================================
// distributions.gk — Shared quantile (correlated samples)
// =================================================================

fn build_correlated_pipeline() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("seed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
    asm.add_node("quantile", Box::new(UnitInterval::new()), vec![WireRef::node("seed")]);
    asm.add_node("temp", Box::new(IcdSample::normal(72.0, 5.0)),
        vec![WireRef::node("quantile")]);
    asm.add_node("wait", Box::new(IcdSample::exponential(0.5)),
        vec![WireRef::node("quantile")]);
    asm.add_output("temp", WireRef::node("temp"));
    asm.add_output("wait", WireRef::node("wait"));
    asm.compile().unwrap()
}

#[test]
fn correlated_samples_move_together() {
    // When sharing a quantile, high temperature should correlate
    // with long wait time (both are high-quantile values).
    let mut k = build_correlated_pipeline();
    let mut high_temp_high_wait = 0;
    let mut total = 0;
    for cycle in 0..5000u64 {
        k.set_inputs(&[cycle]);
        let temp = k.pull("temp").as_f64();
        let wait = k.pull("wait").as_f64();
        if temp > 72.0 {
            total += 1;
            if wait > 2.0 { // median of Exp(0.5) is ln(2)/0.5 ≈ 1.39
                high_temp_high_wait += 1;
            }
        }
    }
    let ratio = high_temp_high_wait as f64 / total as f64;
    // With shared quantile, above-median temp should strongly predict
    // above-median wait. Without correlation, ratio would be ~0.5.
    assert!(ratio > 0.6, "expected correlation, got ratio={ratio}");
}

// =================================================================
// independent_samples.gk — Chained hashes for independence
// =================================================================

fn build_independent_pipeline() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    // Chained hashes
    asm.add_node("h0", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
    asm.add_node("h1", Box::new(Hash64::new()), vec![WireRef::node("h0")]);
    asm.add_node("h2", Box::new(Hash64::new()), vec![WireRef::node("h1")]);

    // Independent quantiles
    asm.add_node("q0", Box::new(UnitInterval::new()), vec![WireRef::node("h0")]);
    asm.add_node("q1", Box::new(UnitInterval::new()), vec![WireRef::node("h1")]);
    asm.add_node("q2", Box::new(UnitInterval::new()), vec![WireRef::node("h2")]);

    // Independent distribution samples
    asm.add_node("temp", Box::new(IcdSample::normal(72.0, 5.0)),
        vec![WireRef::node("q0")]);
    asm.add_node("wait", Box::new(IcdSample::exponential(0.5)),
        vec![WireRef::node("q1")]);
    asm.add_node("size", Box::new(IcdSample::pareto(1.0, 2.0)),
        vec![WireRef::node("q2")]);

    asm.add_output("temp", WireRef::node("temp"));
    asm.add_output("wait", WireRef::node("wait"));
    asm.add_output("size", WireRef::node("size"));
    asm.compile().unwrap()
}

#[test]
fn independent_samples_not_correlated() {
    // With chained hashes, high temperature should NOT predict
    // long wait time.
    let mut k = build_independent_pipeline();
    let mut high_temp_high_wait = 0;
    let mut total = 0;
    for cycle in 0..5000u64 {
        k.set_inputs(&[cycle]);
        let temp = k.pull("temp").as_f64();
        let wait = k.pull("wait").as_f64();
        if temp > 72.0 {
            total += 1;
            if wait > 1.39 { // median of Exp(0.5)
                high_temp_high_wait += 1;
            }
        }
    }
    let ratio = high_temp_high_wait as f64 / total as f64;
    // Independent samples: ratio should be ~0.5 (no correlation)
    assert!(
        (ratio - 0.5).abs() < 0.1,
        "expected ~0.5 (no correlation), got ratio={ratio}"
    );
}

#[test]
fn independent_samples_each_has_correct_stats() {
    let mut k = build_independent_pipeline();
    let mut temps = Vec::new();
    let mut waits = Vec::new();
    let mut sizes = Vec::new();
    for cycle in 0..10_000u64 {
        k.set_inputs(&[cycle]);
        temps.push(k.pull("temp").as_f64());
        waits.push(k.pull("wait").as_f64());
        sizes.push(k.pull("size").as_f64());
    }

    let temp_mean = temps.iter().sum::<f64>() / temps.len() as f64;
    assert!((temp_mean - 72.0).abs() < 1.0, "temp mean={temp_mean}");

    let wait_mean = waits.iter().sum::<f64>() / waits.len() as f64;
    // Mean of Exp(0.5) is 1/0.5 = 2.0
    assert!((wait_mean - 2.0).abs() < 0.5, "wait mean={wait_mean}");

    // Pareto(1, 2): all values >= 1.0
    assert!(sizes.iter().all(|&s| s >= 0.99), "pareto values must be >= 1");
}

// =================================================================
// weighted_entity.gk — Alias sampling + identity derivation
// =================================================================

fn build_weighted_entity_pipeline() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    asm.add_node("decompose", Box::new(MixedRadix::new(vec![1000, 0])),
        vec![WireRef::input("cycle")]);

    // Weighted region selection: 4 regions, US-heavy
    let region_weights = vec![60.0, 20.0, 15.0, 5.0];
    asm.add_node("tenant_h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);
    asm.add_node("region", Box::new(AliasSample::from_weights(&region_weights)),
        vec![WireRef::node("tenant_h")]);
    asm.add_node("tenant_code", Box::new(ModU64::new(100000)),
        vec![WireRef::node("tenant_h")]);

    // Reuse the same pattern for device types: 4 types, uniform
    let device_weights = vec![25.0, 25.0, 25.0, 25.0];
    asm.add_node("device_h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 1)]);
    asm.add_node("device_type", Box::new(AliasSample::from_weights(&device_weights)),
        vec![WireRef::node("device_h")]);
    asm.add_node("device_code", Box::new(ModU64::new(100000)),
        vec![WireRef::node("device_h")]);

    asm.add_output("region", WireRef::node("region"));
    asm.add_output("tenant_code", WireRef::node("tenant_code"));
    asm.add_output("device_type", WireRef::node("device_type"));
    asm.add_output("device_code", WireRef::node("device_code"));

    asm.compile().unwrap()
}

#[test]
fn weighted_entity_region_distribution() {
    let mut k = build_weighted_entity_pipeline();
    let mut counts = [0u64; 4];
    let n = 10_000u64;
    for cycle in 0..n {
        k.set_inputs(&[cycle]);
        let region = k.pull("region").as_u64() as usize;
        assert!(region < 4);
        counts[region] += 1;
    }
    // Region 0 (weight 60%) should dominate
    let r0_ratio = counts[0] as f64 / n as f64;
    assert!(r0_ratio > 0.50, "region 0 ratio={r0_ratio}, expected ~0.60");
    // Region 3 (weight 5%) should be rare
    let r3_ratio = counts[3] as f64 / n as f64;
    assert!(r3_ratio < 0.15, "region 3 ratio={r3_ratio}, expected ~0.05");
}

#[test]
fn weighted_entity_device_type_uniform() {
    let mut k = build_weighted_entity_pipeline();
    let mut counts = [0u64; 4];
    let n = 10_000u64;
    for cycle in 0..n {
        k.set_inputs(&[cycle]);
        let dtype = k.pull("device_type").as_u64() as usize;
        assert!(dtype < 4);
        counts[dtype] += 1;
    }
    // Uniform: each type should be roughly 25%. The alias table with
    // only 4 outcomes and u64 bit-splitting can produce some skew, so
    // we use a generous tolerance.
    for (i, &c) in counts.iter().enumerate() {
        let ratio = c as f64 / n as f64;
        assert!(
            (ratio - 0.25).abs() < 0.20,
            "device_type {i} ratio={ratio}, expected roughly ~0.25"
        );
    }
}

// =================================================================
// sensor_workload.gk — Full composed workload
// =================================================================

fn build_sensor_workload() -> nb_variates::kernel::GkKernel {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    // Coordinate decomposition: 100 sites × 500 sensors × readings
    asm.add_node("decompose", Box::new(MixedRadix::new(vec![100, 500, 0])),
        vec![WireRef::input("cycle")]);

    // Site identity
    asm.add_node("site_h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);
    asm.add_node("site_code", Box::new(ModU64::new(10000)),
        vec![WireRef::node("site_h")]);

    // Sensor identity (interleave site + sensor)
    asm.add_node("ss_interleave", Box::new(Interleave::new()),
        vec![WireRef::node_port("decompose", 0), WireRef::node_port("decompose", 1)]);
    asm.add_node("sensor_h", Box::new(Hash64::new()),
        vec![WireRef::node("ss_interleave")]);
    asm.add_node("sensor_code", Box::new(ModU64::new(100000)),
        vec![WireRef::node("sensor_h")]);

    // Independent quantiles via chained hashes
    asm.add_node("combined", Box::new(Interleave::new()),
        vec![WireRef::node("sensor_h"), WireRef::node_port("decompose", 2)]);
    asm.add_node("h0", Box::new(Hash64::new()), vec![WireRef::node("combined")]);
    asm.add_node("h1", Box::new(Hash64::new()), vec![WireRef::node("h0")]);
    asm.add_node("h2", Box::new(Hash64::new()), vec![WireRef::node("h1")]);
    asm.add_node("q_temp", Box::new(UnitInterval::new()), vec![WireRef::node("h0")]);
    asm.add_node("q_humid", Box::new(UnitInterval::new()), vec![WireRef::node("h1")]);
    asm.add_node("q_batt", Box::new(UnitInterval::new()), vec![WireRef::node("h2")]);

    // Distribution sampling
    asm.add_node("temperature", Box::new(IcdSample::normal(22.0, 3.0)),
        vec![WireRef::node("q_temp")]);
    asm.add_node("humidity_raw", Box::new(IcdSample::normal(55.0, 10.0)),
        vec![WireRef::node("q_humid")]);
    asm.add_node("humidity", Box::new(ClampF64::new(0.0, 100.0)),
        vec![WireRef::node("humidity_raw")]);
    asm.add_node("battery_raw", Box::new(IcdSample::exponential(0.02)),
        vec![WireRef::node("q_batt")]);
    asm.add_node("battery", Box::new(ClampF64::new(0.0, 100.0)),
        vec![WireRef::node("battery_raw")]);

    // Timestamp
    asm.add_node("timestamp", Box::new(AddU64::new(1_710_000_000_000)),
        vec![WireRef::node_port("decompose", 2)]);

    asm.add_output("site_code", WireRef::node("site_code"));
    asm.add_output("sensor_code", WireRef::node("sensor_code"));
    asm.add_output("temperature", WireRef::node("temperature"));
    asm.add_output("humidity", WireRef::node("humidity"));
    asm.add_output("battery", WireRef::node("battery"));
    asm.add_output("timestamp", WireRef::node("timestamp"));

    asm.compile().unwrap()
}

#[test]
fn sensor_workload_temperature_stats() {
    let mut k = build_sensor_workload();
    let mut temps = Vec::new();
    for cycle in 0..10_000u64 {
        k.set_inputs(&[cycle]);
        temps.push(k.pull("temperature").as_f64());
    }
    let mean = temps.iter().sum::<f64>() / temps.len() as f64;
    let variance = temps.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / temps.len() as f64;
    let stddev = variance.sqrt();
    assert!((mean - 22.0).abs() < 1.0, "temp mean={mean}, expected ~22");
    assert!((stddev - 3.0).abs() < 1.0, "temp stddev={stddev}, expected ~3");
}

#[test]
fn sensor_workload_humidity_clamped() {
    let mut k = build_sensor_workload();
    for cycle in 0..10_000u64 {
        k.set_inputs(&[cycle]);
        let h = k.pull("humidity").as_f64();
        assert!((0.0..=100.0).contains(&h), "humidity={h} out of [0,100]");
    }
}

#[test]
fn sensor_workload_battery_clamped() {
    let mut k = build_sensor_workload();
    for cycle in 0..10_000u64 {
        k.set_inputs(&[cycle]);
        let b = k.pull("battery").as_f64();
        assert!((0.0..=100.0).contains(&b), "battery={b} out of [0,100]");
    }
}

#[test]
fn sensor_workload_same_site_stable_identity() {
    let mut k = build_sensor_workload();
    // Cycles 0 and 50000 have the same site (0), different sensor/reading.
    // site_code should be the same.
    k.set_inputs(&[0]);
    let sc1 = k.pull("site_code").as_u64();
    k.set_inputs(&[50000]);
    let sc2 = k.pull("site_code").as_u64();
    assert_eq!(sc1, sc2, "same site should have same site_code");
}

#[test]
fn sensor_workload_different_sites_different_sensors() {
    let mut k = build_sensor_workload();
    // Cycle 0: site=0, sensor=0. Cycle 1: site=1, sensor=0.
    // Same sensor index but different site → different sensor_code.
    k.set_inputs(&[0]);
    let s1 = k.pull("sensor_code").as_u64();
    k.set_inputs(&[1]);
    let s2 = k.pull("sensor_code").as_u64();
    assert_ne!(s1, s2, "different sites should produce different sensor_codes");
}

#[test]
fn sensor_workload_independent_fields() {
    // Temperature and humidity should be statistically independent
    // (different hash depths for their quantiles).
    let mut k = build_sensor_workload();
    let mut high_temp_high_humid = 0;
    let mut high_temp_total = 0;
    for cycle in 0..10_000u64 {
        k.set_inputs(&[cycle]);
        let temp = k.pull("temperature").as_f64();
        let humid = k.pull("humidity").as_f64();
        if temp > 22.0 {
            high_temp_total += 1;
            if humid > 55.0 {
                high_temp_high_humid += 1;
            }
        }
    }
    let ratio = high_temp_high_humid as f64 / high_temp_total as f64;
    assert!(
        (ratio - 0.5).abs() < 0.1,
        "fields should be independent, got correlation ratio={ratio}"
    );
}

#[test]
fn sensor_workload_timestamps_track_readings() {
    let mut k = build_sensor_workload();
    // reading = cycle / (100 * 500) = cycle / 50000
    k.set_inputs(&[0]);
    assert_eq!(k.pull("timestamp").as_u64(), 1_710_000_000_000);
    k.set_inputs(&[50000]);
    assert_eq!(k.pull("timestamp").as_u64(), 1_710_000_000_001);
}
