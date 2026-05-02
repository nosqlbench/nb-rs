// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for vectordata node functions.
//!
//! These tests use the "glove-100" dataset and require network access.
//! They are #[ignore] by default — run explicitly with:
//!   cargo test -p nbrs-variates --test vectordata_integration -- --ignored
//!
//! Per SRD 53 §"Native Vector Binding", vector accessors produce
//! typed `Value::VecF32` / `Value::VecI32` directly. Display
//! rendering goes through `to_display_string()` which formats as a
//! JSON array — these tests check that path.

#![cfg(feature = "vectordata")]

use nbrs_variates::dsl::compile_gk;
use nbrs_variates::node::Value;

#[test]
#[ignore]
fn vector_at_produces_100d_vector() {
    let mut k = compile_gk(r#"
        inputs := (cycle)
        vec := vector_at("glove-100", cycle)
    "#).expect("compile failed");

    k.set_inputs(&[0]);
    let val = k.pull("vec").clone();
    match val {
        Value::VecF32(arc) => {
            assert_eq!(arc.len(), 100, "glove-100 should have 100 dimensions");
        }
        other => panic!("expected VecF32, got {:?}", other),
    }
}

#[test]
#[ignore]
fn vector_at_display_renders_json_array() {
    let mut k = compile_gk(r#"
        inputs := (cycle)
        vec := vector_at("glove-100", cycle)
    "#).expect("compile failed");

    k.set_inputs(&[0]);
    let s = k.pull("vec").to_display_string();
    assert!(s.starts_with('['), "should be a JSON array: {}", &s[..60.min(s.len())]);
    assert!(s.ends_with(']'));
    // glove-100 vectors have 100 dimensions → 99 commas
    let commas = s.chars().filter(|c| *c == ',').count();
    assert_eq!(commas, 99, "glove-100 should have 100 dimensions (99 commas), got {commas}");
}

#[test]
#[ignore]
fn vector_count_and_dim() {
    let mut k = compile_gk(r#"
        inputs := (cycle)
        count := vector_count("glove-100")
        dim := vector_dim("glove-100")
    "#).expect("compile failed");

    k.set_inputs(&[0]);
    let count = k.pull("count").as_u64();
    let dim = k.pull("dim").as_u64();
    assert!(count > 0, "dataset should have vectors, got count={count}");
    assert_eq!(dim, 100, "glove-100 should have dim=100, got {dim}");
    eprintln!("glove-100: {count} vectors, {dim} dimensions");
}

#[test]
#[ignore]
fn query_vector_at_produces_vec_f32() {
    let mut k = compile_gk(r#"
        inputs := (cycle)
        qvec := query_vector_at("glove-100", cycle)
    "#).expect("compile failed");

    k.set_inputs(&[0]);
    let val = k.pull("qvec").clone();
    assert!(matches!(val, Value::VecF32(_)), "expected VecF32, got {:?}", val);
}

#[test]
#[ignore]
fn neighbor_indices_at_produces_vec_i32() {
    let mut k = compile_gk(r#"
        inputs := (cycle)
        neighbors := neighbor_indices_at("glove-100", cycle)
    "#).expect("compile failed");

    k.set_inputs(&[0]);
    let val = k.pull("neighbors").clone();
    match val {
        Value::VecI32(arc) => {
            assert!(arc.len() > 0, "should have multiple neighbors");
        }
        other => panic!("expected VecI32, got {:?}", other),
    }
}

#[test]
#[ignore]
fn vector_at_deterministic() {
    let mut k = compile_gk(r#"
        inputs := (cycle)
        vec := vector_at("glove-100", cycle)
    "#).expect("compile failed");

    k.set_inputs(&[42]);
    let a = k.pull("vec").clone();
    k.set_inputs(&[42]);
    let b = k.pull("vec").clone();
    match (a, b) {
        (Value::VecF32(a), Value::VecF32(b)) => {
            assert_eq!(*a, *b, "same cycle should produce same vector");
        }
        _ => panic!("expected VecF32"),
    }
}
