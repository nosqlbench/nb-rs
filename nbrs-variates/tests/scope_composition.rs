// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for GK scope composition (sysref 16).
//!
//! Tests the GK API primitives that enable scope composition:
//! bind_outer_scope, scope_values, shared/final modifiers,
//! and extern input wiring.

use nbrs_variates::dsl::compile::compile_gk;
use nbrs_variates::dsl::ast::BindingModifier;
use nbrs_variates::node::Value;
use nbrs_variates::subcontext::chain_kernel_under_parent;

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

    chain_kernel_under_parent(&mut inner, &outer);

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

    chain_kernel_under_parent(&mut inner, &outer);

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

    chain_kernel_under_parent(&mut inner, &outer);

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

    chain_kernel_under_parent(&mut inner, &outer);

    let values = inner.scope_values();
    // Should have entries for dim and count (and possibly cycle default)
    let dim_val = values.iter().find(|(name, _)| name == "dim");
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

    chain_kernel_under_parent(&mut inner, &outer);

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
    chain_kernel_under_parent(&mut inner2, &outer);
    // dim is still 256 (inner definition), not 128 (outer)
    assert_eq!(inner2.get_constant("dim").unwrap().as_u64(), 256);
}

// =========================================================================
// shared modifier: metadata queries
// =========================================================================

#[test]
fn shared_modifier_survives_compilation_pipeline() {
    // Literal-init shared bindings — the only currently-supported
    // shape; non-literal RHS is rejected at compile time.
    let kernel = compile_gk(r#"
        inputs := (cycle)
        shared running_total := 0
        shared error_count := 0
        normal_val := hash(cycle)
    "#).unwrap();

    let prog = kernel.program();
    assert_eq!(prog.output_modifier("running_total"), BindingModifier::SHARED);
    assert_eq!(prog.output_modifier("error_count"), BindingModifier::SHARED);
    assert_eq!(prog.output_modifier("normal_val"), BindingModifier::NONE);

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

    assert_eq!(kernel.program().output_modifier("budget"), BindingModifier::SHARED);
    assert_eq!(kernel.get_constant("budget").unwrap().as_u64(), 100);
}

// =========================================================================
// final modifier: metadata queries
// =========================================================================

#[test]
fn final_modifier_survives_compilation_pipeline() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        final dataset := "example"
        final dim := 128
        mutable_val := hash(cycle)
    "#).unwrap();

    let prog = kernel.program();
    assert_eq!(prog.output_modifier("dataset"), BindingModifier::FINAL);
    assert_eq!(prog.output_modifier("dim"), BindingModifier::FINAL);
    assert_eq!(prog.output_modifier("mutable_val"), BindingModifier::NONE);

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

    assert_eq!(kernel.program().output_modifier("max_dim"), BindingModifier::FINAL);
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

    chain_kernel_under_parent(&mut inner, &outer);

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
    assert_eq!(prog.output_modifier("error_budget"), BindingModifier::SHARED);
    assert_eq!(prog.output_modifier("max_dim"), BindingModifier::FINAL);
    assert_eq!(prog.output_modifier("normal"), BindingModifier::NONE);

    // Inner scope sees the outer's constants via bind
    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern error_budget: u64
        extern max_dim: u64
    "#).unwrap();
    chain_kernel_under_parent(&mut inner, &outer);

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
    chain_kernel_under_parent(&mut inner1, &outer);
    inner1.set_inputs(&[0]);
    let v1 = inner1.pull("h").as_u64();

    // Second inner scope — should produce identical result
    let mut inner2 = compile_gk(r#"
        inputs := (cycle)
        extern seed: u64
        h := hash(seed)
    "#).unwrap();
    chain_kernel_under_parent(&mut inner2, &outer);
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

// (Earlier tests of `propagate_shared_to` retired alongside the
// API itself — `SharedCell`-backed input slots replace the
// explicit-propagate mechanism. Cell-based behavior is covered
// by the `shared_*` tests at the end of this file.)

// =========================================================================
// Extern default expressions (`extern name: type = default`)
// =========================================================================
//
// `evaluate_default_expr` accepts literal forms only — IntLit,
// FloatLit, StringLit, plus the `true`/`false` identifiers for
// bool ports. Non-literal expressions surface a clear error at
// compile time. These tests pin down both the accepted shapes
// and the rejected ones.

// All read-side checks go through `lookup()`, the canonical
// two-tier read on `GkKernel`. That exercises the same path
// that `interpolate_via_kernel`, `bind_outer_scope`, and
// `propagate_shared_to` use, so a regression in any of them
// would surface here too.

#[test]
fn extern_default_u64_literal() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern counter: u64 = 42
    "#).unwrap();
    assert_eq!(kernel.lookup("counter").unwrap().as_u64(), 42);
}

#[test]
fn extern_default_u64_zero() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern counter: u64 = 0
    "#).unwrap();
    assert_eq!(kernel.lookup("counter").unwrap().as_u64(), 0);
}

#[test]
fn extern_default_f64_float_literal() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern temperature: f64 = 3.14
    "#).unwrap();
    assert_eq!(kernel.lookup("temperature").unwrap().as_f64(), 3.14);
}

#[test]
fn extern_default_f64_int_literal_widens() {
    // Integer literal in an f64 slot widens to f64 — common YAML
    // convention (`5` rather than `5.0`).
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern threshold: f64 = 5
    "#).unwrap();
    assert_eq!(kernel.lookup("threshold").unwrap().as_f64(), 5.0);
}

#[test]
fn extern_default_string_literal() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern name: String = "guest"
    "#).unwrap();
    match kernel.lookup("name").unwrap() {
        Value::Str(s) => assert_eq!(s, "guest"),
        other => panic!("expected Str, got {other:?}"),
    }
}

#[test]
fn extern_default_no_default_starts_unset() {
    // No default → input slot is `Value::None` (unset). `lookup`
    // filters None internally, so it returns `None` for unset
    // names — distinguishing them from set-but-zero values.
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern unset: u64
    "#).unwrap();
    assert!(kernel.lookup("unset").is_none(),
        "unset extern should not resolve via lookup");
}

#[test]
fn extern_default_visible_through_passthrough_output() {
    // The auto-passthrough output named after the extern should
    // surface the default value through `lookup` (the canonical
    // two-tier read). Any caller using `interpolate_via_kernel`
    // or `bind_outer_scope` against this kernel sees the default.
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern budget: u64 = 100
    "#).unwrap();

    let v = kernel.lookup("budget").expect("budget should resolve");
    assert_eq!(v.as_u64(), 100);
}

#[test]
fn extern_default_function_call_rejected() {
    // Function calls aren't const literals — the compiler must
    // reject them with a clear error.
    let err = compile_gk(r#"
        inputs := (cycle)
        extern x: u64 = hash(0)
    "#).expect_err("function call default must error");
    assert!(err.contains("extern 'x' default"),
        "error should name the extern: {err}");
    assert!(err.contains("literal"),
        "error should explain that literals are required: {err}");
}

#[test]
fn extern_default_identifier_rejected() {
    // Bare identifiers (referencing other bindings) are not
    // const literals.
    let err = compile_gk(r#"
        inputs := (cycle)
        extern x: u64 = somewhere
    "#).expect_err("identifier default must error");
    assert!(err.contains("extern 'x' default"), "error: {err}");
}

#[test]
fn extern_default_type_mismatch_string_for_u64_rejected() {
    let err = compile_gk(r#"
        inputs := (cycle)
        extern n: u64 = "not a number"
    "#).expect_err("string default for u64 port must error");
    assert!(err.contains("extern 'n' default"), "error: {err}");
}

#[test]
fn extern_default_type_mismatch_float_for_u64_rejected() {
    let err = compile_gk(r#"
        inputs := (cycle)
        extern n: u64 = 1.5
    "#).expect_err("float default for u64 port must error");
    assert!(err.contains("extern 'n' default"), "error: {err}");
}

#[test]
fn extern_default_negative_for_u64_rejected_with_clear_message() {
    // u64 can't represent negative numbers. Negative-literal
    // defaults parse as `UnaryNeg(IntLit)` — not a recognized
    // literal shape — so the compiler should reject with the
    // same "literal required" message as other non-literal
    // expressions.
    let err = compile_gk(r#"
        inputs := (cycle)
        extern n: u64 = -5
    "#).expect_err("negative literal default for u64 must error");
    assert!(err.contains("extern 'n' default"), "error: {err}");
}

#[test]
fn extern_default_bool_true_works() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern enabled: bool = true
    "#).unwrap();
    match kernel.lookup("enabled").unwrap() {
        Value::Bool(true) => {}
        other => panic!("expected Bool(true), got {other:?}"),
    }
}

#[test]
fn extern_default_bool_false_works() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        extern enabled: bool = false
    "#).unwrap();
    match kernel.lookup("enabled").unwrap() {
        Value::Bool(false) => {}
        other => panic!("expected Bool(false), got {other:?}"),
    }
}

// =========================================================================
// `shared X := <literal>` compiles to a SharedCell-backed slot
// =========================================================================
//
// Per SRD-16 §"Mutability Rules: Shared Mutable", a literal-init
// `shared` binding gives outer scope a real input slot AND a
// passthrough output. Outer's construction auto-creates a
// SharedCell on the slot; inner `bind_outer_scope` shares the
// cell so writes from any kernel propagate to the others.

#[test]
fn shared_init_compiles_to_slot_with_initial_value() {
    let kernel = compile_gk(r#"
        inputs := (cycle)
        shared counter := 0
    "#).unwrap();

    // Output exists with Shared modifier.
    assert_eq!(kernel.program().output_modifier("counter"),
        BindingModifier::SHARED);
    // And it's also a real input slot — the compiler created
    // the slot+passthrough pair.
    assert!(kernel.program().find_input("counter").is_some(),
        "shared literal-init must create an input slot");
    // Initial value visible via the canonical lookup path.
    assert_eq!(kernel.lookup("counter").unwrap().as_u64(), 0);
}

#[test]
fn shared_inner_write_propagates_to_outer_via_cell() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        shared counter := 5
    "#).unwrap();

    let mut inner = compile_gk(r#"
        inputs := (cycle)
        extern counter: u64
    "#).unwrap();

    chain_kernel_under_parent(&mut inner, &outer);

    // Inner's lookup goes through the cell — sees initial 5.
    assert_eq!(inner.lookup("counter").unwrap().as_u64(), 5);

    // Inner writes through the shared cell.
    let inner_idx = inner.program().find_input("counter").unwrap();
    inner.state().set_input(inner_idx, Value::U64(42));

    // Outer's `lookup` is cell-aware — sees inner's write
    // intrinsically, no refresh step needed.
    assert_eq!(outer.lookup("counter").unwrap().as_u64(), 42,
        "outer's cell-aware lookup must reflect inner's write");
}

#[test]
fn shared_two_inners_see_each_others_writes_via_cell() {
    let outer = compile_gk(r#"
        inputs := (cycle)
        shared budget := 100
    "#).unwrap();

    let mut a = compile_gk(r#"
        inputs := (cycle)
        extern budget: u64
    "#).unwrap();
    let mut b = compile_gk(r#"
        inputs := (cycle)
        extern budget: u64
    "#).unwrap();

    chain_kernel_under_parent(&mut a, &outer);
    chain_kernel_under_parent(&mut b, &outer);

    // Both start at 100 — `lookup` reads the cell.
    assert_eq!(a.lookup("budget").unwrap().as_u64(), 100);
    assert_eq!(b.lookup("budget").unwrap().as_u64(), 100);

    // A decrements through the shared cell.
    let a_idx = a.program().find_input("budget").unwrap();
    a.state().set_input(a_idx, Value::U64(99));

    // B's `lookup` sees A's write intrinsically — no refresh
    // step. The cell is shared between all kernels bound from
    // the same outer.
    assert_eq!(b.lookup("budget").unwrap().as_u64(), 99,
        "second inner kernel must see the first's write through the shared cell");
}

#[test]
fn shared_last_write_wins_under_concurrent_writers() {
    // Even without concurrent threads in the test, the
    // last-write-wins ordering is a property of the cell's
    // serialization. Demonstrate by interleaving writes from
    // two inner kernels and verifying the cell reflects the
    // most recent one.
    // Cycle bindings infer their type from the RHS literal —
    // no `: String` annotation. The compiler's
    // `try_fold_shared_init` matches `Expr::StringLit` and
    // creates a Str-typed input slot.
    let outer = compile_gk(r#"
        inputs := (cycle)
        shared status := "init"
    "#).unwrap();

    let mut a = compile_gk(r#"
        inputs := (cycle)
        extern status: String
    "#).unwrap();
    let mut b = compile_gk(r#"
        inputs := (cycle)
        extern status: String
    "#).unwrap();

    chain_kernel_under_parent(&mut a, &outer);
    chain_kernel_under_parent(&mut b, &outer);

    let a_idx = a.program().find_input("status").unwrap();
    let b_idx = b.program().find_input("status").unwrap();

    a.state().set_input(a_idx, Value::Str("from-a".into()));
    b.state().set_input(b_idx, Value::Str("from-b".into()));
    a.state().set_input(a_idx, Value::Str("from-a-again".into()));

    // Both kernels see the most recent write through cell-aware
    // `lookup` — no refresh step.
    let expected = Value::Str("from-a-again".into());
    assert_eq!(a.lookup("status").unwrap(), expected);
    assert_eq!(b.lookup("status").unwrap(), expected);
}

#[test]
fn shared_non_literal_init_rejected() {
    // `shared X := <non-literal>` is rejected at compile time —
    // a shared cell needs a single well-defined initial value
    // and a computed RHS doesn't have one. See SRD-16
    // §"Non-literal `shared` initializers".
    let err = compile_gk(r#"
        inputs := (cycle)
        shared rolling := hash(cycle)
    "#).expect_err("non-literal shared init must error");
    assert!(err.contains("shared binding 'rolling'"), "error: {err}");
    assert!(err.contains("literal initial value"), "error: {err}");
}
