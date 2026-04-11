// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for vectordata node functions.
//!
//! These tests use the "glove-100" dataset and require network access.
//! They are #[ignore] by default — run explicitly with:
//!   cargo test -p nb-variates --test vectordata_integration -- --ignored

#![cfg(feature = "vectordata")]

use nb_variates::dsl::compile_gk;

#[test]
#[ignore]
fn vector_at_produces_100d_vector() {
    let mut k = compile_gk(r#"
        coordinates := (cycle)
        vec := vector_at(cycle, "glove-100")
    "#).expect("compile failed");

    k.set_coordinates(&[0]);
    let s = k.pull("vec").to_display_string();
    assert!(s.starts_with('['), "should be a JSON array: {}", &s[..60.min(s.len())]);
    assert!(s.ends_with(']'));
    // glove-100 vectors have 100 dimensions → 99 commas
    let commas = s.chars().filter(|c| *c == ',').count();
    assert_eq!(commas, 99, "glove-100 should have 100 dimensions (99 commas), got {commas}");
}

#[test]
#[ignore]
fn vector_at_bytes_correct_length() {
    let mut k = compile_gk(r#"
        coordinates := (cycle)
        vec := vector_at_bytes(cycle, "glove-100")
    "#).expect("compile failed");

    k.set_coordinates(&[0]);
    let val = k.pull("vec");
    match val {
        nb_variates::node::Value::Bytes(b) => {
            // 100 dimensions × 4 bytes per f32 = 400 bytes
            assert_eq!(b.len(), 400, "glove-100 vector bytes should be 400, got {}", b.len());
        }
        other => panic!("expected Bytes, got {:?}", other),
    }
}

#[test]
#[ignore]
fn vector_count_and_dim() {
    let mut k = compile_gk(r#"
        coordinates := (cycle)
        count := vector_count("glove-100")
        dim := vector_dim("glove-100")
    "#).expect("compile failed");

    k.set_coordinates(&[0]);
    let count = k.pull("count").as_u64();
    let dim = k.pull("dim").as_u64();
    assert!(count > 0, "dataset should have vectors, got count={count}");
    assert_eq!(dim, 100, "glove-100 should have dim=100, got {dim}");
    eprintln!("glove-100: {count} vectors, {dim} dimensions");
}

#[test]
#[ignore]
fn query_vector_at_produces_output() {
    let mut k = compile_gk(r#"
        coordinates := (cycle)
        qvec := query_vector_at(cycle, "glove-100")
    "#).expect("compile failed");

    k.set_coordinates(&[0]);
    let s = k.pull("qvec").to_display_string();
    assert!(s.starts_with('[') && s.ends_with(']'), "should be array: {}", &s[..60.min(s.len())]);
}

#[test]
#[ignore]
fn neighbor_indices_at_produces_output() {
    let mut k = compile_gk(r#"
        coordinates := (cycle)
        neighbors := neighbor_indices_at(cycle, "glove-100")
    "#).expect("compile failed");

    k.set_coordinates(&[0]);
    let s = k.pull("neighbors").to_display_string();
    assert!(s.starts_with('[') && s.ends_with(']'), "should be array: {}", &s[..60.min(s.len())]);
    // Should have multiple neighbor indices
    let commas = s.chars().filter(|c| *c == ',').count();
    assert!(commas > 0, "should have multiple neighbors, got: {}", &s[..80.min(s.len())]);
}

#[test]
#[ignore]
fn vector_at_deterministic() {
    let mut k = compile_gk(r#"
        coordinates := (cycle)
        vec := vector_at(cycle, "glove-100")
    "#).expect("compile failed");

    k.set_coordinates(&[42]);
    let a = k.pull("vec").to_display_string();
    k.set_coordinates(&[42]);
    let b = k.pull("vec").to_display_string();
    assert_eq!(a, b, "same cycle should produce same vector");
}

#[test]
#[ignore]
fn vector_at_wraps_modulo() {
    let mut k = compile_gk(r#"
        coordinates := (cycle)
        count := vector_count("glove-100")
        vec0 := vector_at(cycle, "glove-100")
    "#).expect("compile failed");

    k.set_coordinates(&[0]);
    let count = k.pull("count").as_u64();
    let v0 = k.pull("vec0").to_display_string();

    // Accessing at index=count should wrap to index=0
    k.set_coordinates(&[count]);
    let v_wrap = k.pull("vec0").to_display_string();
    assert_eq!(v0, v_wrap, "index should wrap modulo count");
}
