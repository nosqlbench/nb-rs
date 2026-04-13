// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for GK scope composition (sysref 16).
//!
//! Tests the GK API primitives that enable scope composition:
//! bind_outer_scope, scope_values, shared/final modifiers,
//! and extern input wiring.

use nb_variates::dsl::compile::compile_gk;
use nb_variates::dsl::ast::BindingModifier;
use nb_variates::node::Value;

// =========================================================================
// bind_outer_scope: basic wiring
// =========================================================================

#[test]
fn bind_outer_scope_wires_constants() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        dim := 128
        count := 1000
    "#).unwrap();

    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern dim: u64
        extern count: u64
    "#).unwrap();

    inner.bind_outer_scope(&outer);

    let dim_idx = inner.program().find_input("dim").unwrap();
    let count_idx = inner.program().find_input("count").unwrap();
    assert_eq!(inner.state().get_input(dim_idx).as_u64(), 128);
    assert_eq!(inner.state().get_input(count_idx).as_u64(), 1000);
}

#[test]
fn bind_outer_scope_only_matches_by_name() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        dim := 128
    "#).unwrap();

    // Inner has an extern named 'offset' — not in outer scope
    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern offset: u64
    "#).unwrap();

    inner.bind_outer_scope(&outer);

    // 'offset' should still be at its default (None for extern)
    let idx = inner.program().find_input("offset").unwrap();
    assert!(matches!(inner.state().get_input(idx), Value::None));
}

#[test]
fn bind_outer_scope_does_not_affect_coordinates() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        dim := 128
    "#).unwrap();

    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern dim: u64
        h := hash(cycle)
    "#).unwrap();

    inner.bind_outer_scope(&outer);

    // Coordinate input should still work normally
    inner.set_inputs(&[42]);
    let v1 = inner.pull("h").as_u64();
    inner.set_inputs(&[43]);
    let v2 = inner.pull("h").as_u64();
    assert_ne!(v1, v2, "different cycles should produce different hashes");
}

// =========================================================================
// scope_values: extraction for fiber replication
// =========================================================================

#[test]
fn scope_values_extracts_bound_inputs() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        dim := 128
        count := 500
    "#).unwrap();

    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern dim: u64
        extern count: u64
    "#).unwrap();

    inner.bind_outer_scope(&outer);

    let values = inner.scope_values();
    // Should have entries for dim and count (and possibly cycle default)
    let dim_val = values.iter().find(|(idx, _)| {
        inner.program().input_names()[*idx] == "dim"
    });
    assert!(dim_val.is_some(), "scope_values should include dim");
    assert_eq!(dim_val.unwrap().1.as_u64(), 128);
}

#[test]
fn scope_values_empty_when_no_externs() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        dim := 128
    "#).unwrap();

    let mut inner = compile_gk(r#"
        inputs := (cycle)
        h := hash(cycle)
    "#).unwrap();

    inner.bind_outer_scope(&outer);

    // Inner has no externs, so scope_values only has coordinate defaults
    let values = inner.scope_values();
    // All values should be the coordinate default (U64(0))
    for (_, val) in &values {
        assert!(matches!(val, Value::U64(0)),
            "only coordinate defaults expected, got {:?}", val);
    }
}

// =========================================================================
// Shadowing: inner scope redefines outer names
// =========================================================================

#[test]
fn inner_scope_shadows_outer_binding() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        dim := 128
    "#).unwrap();
    assert_eq!(outer.get_constant("dim").unwrap().as_u64(), 128);

    // Inner scope redefines dim — should use its own value
    let inner = compile_gk(r#"
        inputs := (cycle)
        dim := 256
    "#).unwrap();
    assert_eq!(inner.get_constant("dim").unwrap().as_u64(), 256);

    // Inner scope has no extern for dim — bind_outer_scope won't wire it
    let mut inner2 = compile_gk(r#"
        inputs := (cycle)
        dim := 256
    "#).unwrap();
    inner2.bind_outer_scope(&outer);
    // dim is still 256 (inner definition), not 128 (outer)
    assert_eq!(inner2.get_constant("dim").unwrap().as_u64(), 256);
}

// =========================================================================
// shared modifier: metadata queries
// =========================================================================

#[test]
fn shared_modifier_survives_compilation_pipeline() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        shared running_total := hash(cycle)
        shared error_count := mod(hash(cycle), 10)
        normal_val := hash(cycle)
    "#).unwrap();

    let prog = kernel.program();
    assert_eq!(prog.output_modifier("running_total"), BindingModifier::Shared);
    assert_eq!(prog.output_modifier("error_count"), BindingModifier::Shared);
    assert_eq!(prog.output_modifier("normal_val"), BindingModifier::None);

    let mut shared = prog.shared_outputs();
    shared.sort();
    assert_eq!(shared, vec!["error_count", "running_total"]);
}

#[test]
fn shared_init_constant_folds() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        shared init budget = 100
    "#).unwrap();

    assert_eq!(kernel.program().output_modifier("budget"), BindingModifier::Shared);
    assert_eq!(kernel.get_constant("budget").unwrap().as_u64(), 100);
}

// =========================================================================
// final modifier: metadata queries
// =========================================================================

#[test]
fn final_modifier_survives_compilation_pipeline() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        final dataset := "sift1m"
        final dim := 128
        mutable_val := hash(cycle)
    "#).unwrap();

    let prog = kernel.program();
    assert_eq!(prog.output_modifier("dataset"), BindingModifier::Final);
    assert_eq!(prog.output_modifier("dim"), BindingModifier::Final);
    assert_eq!(prog.output_modifier("mutable_val"), BindingModifier::None);

    let mut finals = prog.final_outputs();
    finals.sort();
    assert_eq!(finals, vec!["dataset", "dim"]);
}

#[test]
fn final_init_constant_folds() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        final init max_dim = 512
    "#).unwrap();

    assert_eq!(kernel.program().output_modifier("max_dim"), BindingModifier::Final);
    assert_eq!(kernel.get_constant("max_dim").unwrap().as_u64(), 512);
}

// =========================================================================
// Extern inputs used as wire arguments (compiler fix)
// =========================================================================

#[test]
fn extern_wired_into_hash() {
    let src = r#"
        inputs := (cycle)
        extern seed: u64
        result := hash(seed)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("seed").unwrap();

    kernel.state().set_input(idx, Value::U64(42));
    kernel.set_inputs(&[0]);
    let v1 = kernel.pull("result").as_u64();

    kernel.state().set_input(idx, Value::U64(99));
    kernel.set_inputs(&[0]);
    let v2 = kernel.pull("result").as_u64();

    assert_ne!(v1, v2, "different seeds should produce different hashes");
}

#[test]
fn extern_in_binary_expression() {
    let src = r#"
        inputs := (cycle)
        extern multiplier: u64
        result := cycle * multiplier
    "#;
    let mut kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("multiplier").unwrap();

    kernel.state().set_input(idx, Value::U64(7));
    kernel.set_inputs(&[6]);
    assert_eq!(kernel.pull("result").as_u64(), 42);
}

#[test]
fn extern_in_function_chain() {
    let src = r#"
        inputs := (cycle)
        extern base: u64
        h := hash(base)
        result := mod(h, 100)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("base").unwrap();

    kernel.state().set_input(idx, Value::U64(42));
    kernel.set_inputs(&[0]);
    let v = kernel.pull("result").as_u64();
    assert!(v < 100);
}

#[test]
fn extern_and_coordinate_mixed() {
    let src = r#"
        inputs := (cycle)
        extern offset: u64
        result := hash(cycle) + offset
    "#;
    let mut kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("offset").unwrap();

    kernel.state().set_input(idx, Value::U64(1000));
    kernel.set_inputs(&[42]);
    let v1 = kernel.pull("result").as_u64();

    kernel.state().set_input(idx, Value::U64(2000));
    kernel.set_inputs(&[42]);
    let v2 = kernel.pull("result").as_u64();

    assert_eq!(v2 - v1, 1000, "offset difference should be reflected");
}

// =========================================================================
// Scope composition: full outer → inner pipeline
// =========================================================================

#[test]
fn full_scope_pipeline_outer_to_inner() {
    // Simulate workload scope → phase scope composition
    let outer = compile_gk(r#"
        inputs := (cycle)
        dim := 128
        base_count := 10000
    "#).unwrap();

    // Inner scope uses outer constants via extern + GK wire
    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern dim: u64
        extern base_count: u64
        id := hash(cycle) + base_count
    "#).unwrap();

    inner.bind_outer_scope(&outer);

    // Verify both externs were bound correctly
    inner.set_inputs(&[0]);
    let dim_val = inner.pull("dim").as_u64();
    assert_eq!(dim_val, 128);

    inner.set_inputs(&[42]);
    let id = inner.pull("id").as_u64();
    // id = hash(42) + 10000, should be > 10000
    assert!(id >= 10000, "id should include base_count offset, got {id}");
}

#[test]
fn scope_pipeline_with_shared_and_final() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        shared error_budget := 100
        final max_dim := 256
        normal := hash(cycle)
    "#).unwrap();

    let prog = outer.program();
    assert_eq!(prog.output_modifier("error_budget"), BindingModifier::Shared);
    assert_eq!(prog.output_modifier("max_dim"), BindingModifier::Final);
    assert_eq!(prog.output_modifier("normal"), BindingModifier::None);

    // Inner scope sees the outer's constants via bind
    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern error_budget: u64
        extern max_dim: u64
    "#).unwrap();
    inner.bind_outer_scope(&outer);

    let eb_idx = inner.program().find_input("error_budget").unwrap();
    let md_idx = inner.program().find_input("max_dim").unwrap();
    assert_eq!(inner.state().get_input(eb_idx).as_u64(), 100);
    assert_eq!(inner.state().get_input(md_idx).as_u64(), 256);
}

// =========================================================================
// Multiple sequential scopes (simulates phases)
// =========================================================================

#[test]
fn sequential_inner_scopes_are_independent() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        seed := 42
    "#).unwrap();

    // First inner scope
    let mut inner1 = compile_gk(r#"
        inputs := (cycle)
        extern seed: u64
        h := hash(seed)
    "#).unwrap();
    inner1.bind_outer_scope(&outer);
    inner1.set_inputs(&[0]);
    let v1 = inner1.pull("h").as_u64();

    // Second inner scope — should produce identical result
    let mut inner2 = compile_gk(r#"
        inputs := (cycle)
        extern seed: u64
        h := hash(seed)
    "#).unwrap();
    inner2.bind_outer_scope(&outer);
    inner2.set_inputs(&[0]);
    let v2 = inner2.pull("h").as_u64();

    assert_eq!(v1, v2, "identical inner scopes with same outer should be deterministic");
}

// =========================================================================
// Diagnostic contract: source and context
// =========================================================================

#[test]
fn all_kernels_have_diagnostic_context() {
    let src = r#"
        inputs := (cycle)
        h := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    let source = kernel.program().source();
    assert!(source.contains("hash(cycle)"), "source should be preserved");
}

#[test]
fn extern_kernel_has_source() {
    let src = r#"
        inputs := (cycle)
        extern dim: u64
        h := hash(dim)
    "#;
    let kernel = compile_gk(src).unwrap();
    assert!(kernel.program().source().contains("extern dim"));
}
