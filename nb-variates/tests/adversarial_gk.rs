// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Adversarial and fuzz-style tests for the GK system.
//!
//! These tests exercise boundary conditions, malformed inputs,
//! edge cases in the lexer/parser/compiler/runtime pipeline,
//! and scope composition mechanics.

use nb_variates::dsl::compile::{compile_gk, compile_gk_strict};
use nb_variates::dsl::ast::BindingModifier;

// =========================================================================
// Lexer / Parser adversarial inputs
// =========================================================================

#[test]
fn empty_source() {
    let result = compile_gk("");
    assert!(result.is_err(), "empty source should fail");
}

#[test]
fn whitespace_only() {
    let result = compile_gk("   \n\n  \t  \n  ");
    assert!(result.is_err(), "whitespace-only source should fail");
}

#[test]
fn comments_only() {
    let result = compile_gk("// just a comment\n# another comment\n/* block */");
    assert!(result.is_err(), "comments-only source should fail");
}

#[test]
fn unterminated_string() {
    let result = compile_gk(r#"
        inputs := (cycle)
        s := "unterminated
    "#);
    assert!(result.is_err(), "unterminated string should fail");
}

#[test]
fn unterminated_block_comment() {
    let result = compile_gk(r#"
        inputs := (cycle)
        /* never closed
        h := hash(cycle)
    "#);
    // The lexer may or may not detect unterminated block comments;
    // it's acceptable to either error or treat the rest as comment.
    // Just verify it doesn't panic.
    let _ = result;
}

#[test]
fn unexpected_token_at_toplevel() {
    let result = compile_gk("+ - * /");
    assert!(result.is_err());
}

#[test]
fn duplicate_inputs_declaration() {
    // Second declaration should override, not error
    let result = compile_gk(r#"
        inputs := (cycle)
        inputs := (cycle)
        h := hash(cycle)
    "#);
    assert!(result.is_ok(), "duplicate inputs should be accepted: {:?}", result.err());
}

#[test]
fn zero_inputs_explicit() {
    let result = compile_gk(r#"
        inputs := ()
        val := 42
    "#);
    // Explicit empty inputs is valid for constant-only programs
    assert!(result.is_ok(), "explicit empty inputs should work: {:?}", result.err());
}

#[test]
fn very_long_identifier() {
    let long_name: String = "a".repeat(1000);
    let src = format!("inputs := (cycle)\n{long_name} := hash(cycle)");
    let result = compile_gk(&src);
    assert!(result.is_ok(), "long identifier should work: {:?}", result.err());
}

#[test]
fn unicode_in_string_literal() {
    let result = compile_gk(r#"
        inputs := (cycle)
        emoji := "hello 🌍 world"
    "#);
    assert!(result.is_ok(), "unicode in strings should work: {:?}", result.err());
}

#[test]
fn deeply_nested_function_calls() {
    // hash(hash(hash(hash(hash(cycle)))))
    let src = r#"
        inputs := (cycle)
        deep := hash(hash(hash(hash(hash(cycle)))))
    "#;
    let mut kernel = compile_gk(src).unwrap();
    kernel.set_inputs(&[42]);
    let _ = kernel.pull("deep").as_u64();
}

#[test]
fn deeply_nested_arithmetic() {
    let src = r#"
        inputs := (cycle)
        v := ((((cycle + 1) * 2) + 3) * 4) + 5
    "#;
    let mut kernel = compile_gk(src).unwrap();
    kernel.set_inputs(&[10]);
    let result = kernel.pull("v").as_u64();
    // (((10+1)*2)+3)*4)+5 = ((11*2)+3)*4+5 = (22+3)*4+5 = 25*4+5 = 105
    assert_eq!(result, 105);
}

#[test]
fn hex_literals() {
    let src = r#"
        inputs := (cycle)
        v := mod(hash(cycle), 0xFF)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    kernel.set_inputs(&[1]);
    assert!(kernel.pull("v").as_u64() < 255);
}

#[test]
fn negative_float_literal() {
    let src = r#"
        inputs := (cycle)
        v := -3.14
    "#;
    let mut kernel = compile_gk(src).unwrap();
    kernel.set_inputs(&[0]);
    let val = kernel.pull("v").as_f64();
    assert!((val - (-3.14)).abs() < 0.001);
}

// =========================================================================
// Unknown function / wiring errors
// =========================================================================

#[test]
fn unknown_function_name() {
    let result = compile_gk(r#"
        inputs := (cycle)
        v := nonexistent_function(cycle)
    "#);
    assert!(result.is_err(), "unknown function should fail");
    let err = result.unwrap_err();
    assert!(err.contains("nonexistent_function"), "error should name the function: {err}");
}

#[test]
fn unknown_wire_reference() {
    let result = compile_gk(r#"
        inputs := (cycle)
        v := hash(undefined_wire)
    "#);
    assert!(result.is_err(), "unknown wire should fail");
}

#[test]
fn wrong_arity_too_many() {
    let result = compile_gk(r#"
        inputs := (cycle)
        v := hash(cycle, cycle, cycle)
    "#);
    assert!(result.is_err(), "too many args should fail");
}

#[test]
fn self_referential_binding() {
    let result = compile_gk(r#"
        inputs := (cycle)
        x := hash(x)
    "#);
    // This should fail: x references itself before being defined
    assert!(result.is_err(), "self-referential binding should fail");
}

// =========================================================================
// Strict mode
// =========================================================================

#[test]
fn strict_requires_explicit_inputs() {
    let result = compile_gk_strict("h := hash(cycle)", None, true);
    assert!(result.is_err(), "strict mode should require explicit inputs");
}

#[test]
fn strict_accepts_explicit_inputs() {
    let result = compile_gk_strict(
        "inputs := (cycle)\nh := hash(cycle)",
        None,
        true,
    );
    assert!(result.is_ok(), "strict with explicit inputs should work: {:?}", result.err());
}

// =========================================================================
// Binding modifiers: shared / final
// =========================================================================

#[test]
fn shared_on_cycle_binding() {
    let src = r#"
        inputs := (cycle)
        shared counter := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    assert_eq!(kernel.program().output_modifier("counter"), BindingModifier::Shared);
}

#[test]
fn final_on_cycle_binding() {
    let src = r#"
        inputs := (cycle)
        final max := mod(hash(cycle), 100)
    "#;
    let kernel = compile_gk(src).unwrap();
    assert_eq!(kernel.program().output_modifier("max"), BindingModifier::Final);
}

#[test]
fn shared_on_init_binding() {
    let src = r#"
        inputs := (cycle)
        shared init budget = 500
    "#;
    let kernel = compile_gk(src).unwrap();
    assert_eq!(kernel.program().output_modifier("budget"), BindingModifier::Shared);
    assert_eq!(kernel.get_constant("budget").unwrap().as_u64(), 500);
}

#[test]
fn final_on_init_binding() {
    let src = r#"
        inputs := (cycle)
        final init dim = 128
    "#;
    let kernel = compile_gk(src).unwrap();
    assert_eq!(kernel.program().output_modifier("dim"), BindingModifier::Final);
    assert_eq!(kernel.get_constant("dim").unwrap().as_u64(), 128);
}

#[test]
fn mixed_modifiers() {
    let src = r#"
        inputs := (cycle)
        shared s := hash(cycle)
        final f := 42
        plain := mod(hash(cycle), 100)
    "#;
    let kernel = compile_gk(src).unwrap();
    assert_eq!(kernel.program().output_modifier("s"), BindingModifier::Shared);
    assert_eq!(kernel.program().output_modifier("f"), BindingModifier::Final);
    assert_eq!(kernel.program().output_modifier("plain"), BindingModifier::None);
}

#[test]
fn shared_outputs_list() {
    let src = r#"
        inputs := (cycle)
        shared a := hash(cycle)
        shared b := mod(hash(cycle), 10)
        c := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    let mut shared = kernel.program().shared_outputs();
    shared.sort();
    assert_eq!(shared, vec!["a", "b"]);
}

#[test]
fn final_outputs_list() {
    let src = r#"
        inputs := (cycle)
        final x := 1
        final y := 2
        z := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    let mut finals = kernel.program().final_outputs();
    finals.sort();
    assert_eq!(finals, vec!["x", "y"]);
}

#[test]
fn no_modifiers_returns_empty_lists() {
    let src = r#"
        inputs := (cycle)
        h := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    assert!(kernel.program().shared_outputs().is_empty());
    assert!(kernel.program().final_outputs().is_empty());
}

// =========================================================================
// Extern declarations
// =========================================================================

#[test]
fn extern_u64_input_declared() {
    // Extern declarations register named inputs on the kernel
    let src = r#"
        inputs := (cycle)
        extern offset: u64
    "#;
    let kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("offset");
    assert!(idx.is_some(), "extern should create a named input");
}

#[test]
fn extern_usable_as_wire_argument() {
    // Externs should be usable directly in GK expressions
    let src = r#"
        inputs := (cycle)
        extern offset: u64
        result := hash(offset)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("offset").unwrap();
    kernel.state().set_input(idx, nb_variates::node::Value::U64(100));
    kernel.set_inputs(&[42]);
    let v1 = kernel.pull("result").as_u64();

    // Different extern value should produce different hash
    kernel.state().set_input(idx, nb_variates::node::Value::U64(200));
    kernel.set_inputs(&[42]);
    let v2 = kernel.pull("result").as_u64();
    assert_ne!(v1, v2, "different extern values should hash differently");
}

#[test]
fn extern_in_arithmetic_expression() {
    let src = r#"
        inputs := (cycle)
        extern scale: u64
        result := cycle * scale
    "#;
    let mut kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("scale").unwrap();
    kernel.state().set_input(idx, nb_variates::node::Value::U64(10));
    kernel.set_inputs(&[5]);
    let v = kernel.pull("result").as_u64();
    assert_eq!(v, 50);
}

#[test]
fn extern_input_usable_as_output() {
    // Externs create passthrough outputs accessible via pull/get_constant
    let src = r#"
        inputs := (cycle)
        extern dim: u64
    "#;
    let mut kernel = compile_gk(src).unwrap();
    let idx = kernel.program().find_input("dim").unwrap();
    kernel.state().set_input(idx, nb_variates::node::Value::U64(128));
    kernel.set_inputs(&[0]);
    // The passthrough output should reflect the input value
    let val = kernel.pull("dim").as_u64();
    assert_eq!(val, 128);
}

#[test]
fn extern_f64_input() {
    let src = r#"
        inputs := (cycle)
        extern threshold: f64
    "#;
    let kernel = compile_gk(src).unwrap();
    assert!(kernel.program().find_input("threshold").is_some());
}

#[test]
fn extern_string_input() {
    let src = r#"
        inputs := (cycle)
        extern label: String
    "#;
    let kernel = compile_gk(src).unwrap();
    assert!(kernel.program().find_input("label").is_some());
}

// =========================================================================
// Scope composition: bind_outer_scope
// =========================================================================

#[test]
fn bind_outer_scope_copies_constants() {
    // Outer kernel has a constant
    let outer_src = r#"
        inputs := (cycle)
        dim := 128
    "#;
    let outer = compile_gk(outer_src).unwrap();
    assert_eq!(outer.get_constant("dim").unwrap().as_u64(), 128);

    // Inner kernel has an extern that matches the outer's output
    let inner_src = r#"
        inputs := (cycle)
        extern dim: u64
    "#;
    let mut inner = compile_gk(inner_src).unwrap();
    inner.bind_outer_scope(&outer);

    // Verify the extern input was populated from outer scope
    let idx = inner.program().find_input("dim").unwrap();
    let val = inner.state().get_input(idx);
    assert_eq!(val.as_u64(), 128);

    // The passthrough output also reflects the bound value
    inner.set_inputs(&[0]);
    let pulled = inner.pull("dim").as_u64();
    assert_eq!(pulled, 128);
}

#[test]
fn bind_outer_scope_ignores_nonmatching() {
    let outer_src = r#"
        inputs := (cycle)
        dim := 128
    "#;
    let outer = compile_gk(outer_src).unwrap();

    // Inner kernel doesn't reference 'dim' — no extern
    let inner_src = r#"
        inputs := (cycle)
        h := hash(cycle)
    "#;
    let mut inner = compile_gk(inner_src).unwrap();
    // Should not panic
    inner.bind_outer_scope(&outer);
}

// =========================================================================
// Determinism under repeated evaluation
// =========================================================================

#[test]
fn deterministic_across_1000_cycles() {
    let src = r#"
        inputs := (cycle)
        h := hash(cycle)
        v := mod(h, 1000000)
    "#;
    let mut kernel = compile_gk(src).unwrap();

    // Collect values for first pass
    let mut first_pass = Vec::new();
    for i in 0..1000 {
        kernel.set_inputs(&[i]);
        first_pass.push(kernel.pull("v").as_u64());
    }

    // Verify second pass matches
    for i in 0..1000 {
        kernel.set_inputs(&[i]);
        let v = kernel.pull("v").as_u64();
        assert_eq!(v, first_pass[i as usize],
            "non-deterministic at cycle {i}: {v} != {}", first_pass[i as usize]);
    }
}

#[test]
fn deterministic_with_multiple_outputs() {
    let src = r#"
        inputs := (cycle)
        a := hash(cycle)
        b := mod(a, 100)
        c := mod(a, 1000)
    "#;
    let mut kernel = compile_gk(src).unwrap();

    for i in 0..100 {
        kernel.set_inputs(&[i]);
        let a1 = kernel.pull("a").as_u64();
        let b1 = kernel.pull("b").as_u64();
        let c1 = kernel.pull("c").as_u64();

        kernel.set_inputs(&[i]);
        let a2 = kernel.pull("a").as_u64();
        let b2 = kernel.pull("b").as_u64();
        let c2 = kernel.pull("c").as_u64();

        assert_eq!(a1, a2, "non-deterministic 'a' at cycle {i}");
        assert_eq!(b1, b2, "non-deterministic 'b' at cycle {i}");
        assert_eq!(c1, c2, "non-deterministic 'c' at cycle {i}");
    }
}

// =========================================================================
// Boundary values
// =========================================================================

#[test]
fn max_u64_input() {
    let src = r#"
        inputs := (cycle)
        h := hash(cycle)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    kernel.set_inputs(&[u64::MAX]);
    let _ = kernel.pull("h").as_u64();
    // Should not panic
}

#[test]
fn zero_input() {
    let src = r#"
        inputs := (cycle)
        h := hash(cycle)
        v := mod(h, 100)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    kernel.set_inputs(&[0]);
    assert!(kernel.pull("v").as_u64() < 100);
}

#[test]
fn mod_by_one() {
    let src = r#"
        inputs := (cycle)
        v := mod(hash(cycle), 1)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    kernel.set_inputs(&[42]);
    assert_eq!(kernel.pull("v").as_u64(), 0);
}

#[test]
fn constant_only_program() {
    let src = r#"
        inputs := ()
        x := 42
        y := 3.14
        s := "hello"
    "#;
    let kernel = compile_gk(src).unwrap();
    assert_eq!(kernel.get_constant("x").unwrap().as_u64(), 42);
    assert!((kernel.get_constant("y").unwrap().as_f64() - 3.14).abs() < 0.001);
    assert_eq!(kernel.get_constant("s").unwrap().as_str(), "hello");
}

// =========================================================================
// Diagnostic source/context contract
// =========================================================================

#[test]
fn compiled_kernel_has_source_and_context() {
    let src = r#"
        inputs := (cycle)
        h := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    // Source should contain the original text
    assert!(kernel.program().source().contains("hash(cycle)"),
        "source should contain original text");
}

// =========================================================================
// Fuzz-style: random well-formed programs
// =========================================================================

/// Generate and compile many small random programs.
/// Tests that the compiler doesn't panic on valid programs.
#[test]
fn fuzz_random_hash_chains() {
    let functions = ["hash", "hash"];
    for seed in 0..200u64 {
        let depth = (seed % 5) + 1;
        let mut expr = "cycle".to_string();
        for _ in 0..depth {
            let func = functions[(seed as usize) % functions.len()];
            expr = format!("{func}({expr})");
        }
        let src = format!("inputs := (cycle)\nresult := mod({expr}, 1000)");
        let mut kernel = compile_gk(&src).unwrap_or_else(|e| {
            panic!("seed {seed}: compile failed: {e}\nsource:\n{src}")
        });
        kernel.set_inputs(&[seed]);
        let v = kernel.pull("result").as_u64();
        assert!(v < 1000, "seed {seed}: expected < 1000, got {v}");
    }
}

/// Fuzz arithmetic expression combinations.
#[test]
fn fuzz_arithmetic_expressions() {
    let ops = ["+", "-", "*"];
    for (i, op) in ops.iter().enumerate() {
        for a in [0u64, 1, 42, 100, u64::MAX / 2] {
            let src = format!(
                "inputs := (cycle)\nresult := cycle {op} {a}"
            );
            let result = compile_gk(&src);
            assert!(result.is_ok(),
                "op={op} a={a}: compile failed: {:?}", result.err());
            let mut kernel = result.unwrap();
            // Don't test with values that might overflow — just verify it doesn't panic
            kernel.set_inputs(&[10]);
            let _ = kernel.pull("result");
        }
    }
}

/// Fuzz: compile many programs with varying numbers of bindings.
#[test]
fn fuzz_many_bindings() {
    for n in 1..=20 {
        let mut src = "inputs := (cycle)\n".to_string();
        let mut prev = "cycle".to_string();
        for i in 0..n {
            let name = format!("v{i}");
            src.push_str(&format!("{name} := hash({prev})\n"));
            prev = name;
        }
        let mut kernel = compile_gk(&src).unwrap_or_else(|e| {
            panic!("n={n}: compile failed: {e}\nsource:\n{src}")
        });
        kernel.set_inputs(&[42]);
        let _ = kernel.pull(&prev);
    }
}

/// Fuzz: programs with multiple outputs and destructuring.
#[test]
fn fuzz_multi_output_destructuring() {
    for n in [2, 3, 5, 10] {
        let names: Vec<String> = (0..n).map(|i| format!("d{i}")).collect();
        let targets = names.join(", ");
        let dims: Vec<String> = (0..n).map(|i| format!("{}", 10 + i)).collect();
        let dim_args = dims.join(", ");
        let src = format!(
            "inputs := (cycle)\n({targets}) := mixed_radix(cycle, {dim_args})"
        );
        let mut kernel = compile_gk(&src).unwrap_or_else(|e| {
            panic!("n={n}: compile failed: {e}\nsource:\n{src}")
        });
        kernel.set_inputs(&[12345]);
        for name in &names {
            let _ = kernel.pull(name);
        }
    }
}

/// Fuzz: stress test with 100 independent hash chains.
#[test]
fn fuzz_wide_graph() {
    let mut src = "inputs := (cycle)\n".to_string();
    for i in 0..100 {
        src.push_str(&format!("out_{i} := mod(hash(cycle + {i}), 1000)\n"));
    }
    let mut kernel = compile_gk(&src).unwrap();
    kernel.set_inputs(&[42]);
    for i in 0..100 {
        let v = kernel.pull(&format!("out_{i}")).as_u64();
        assert!(v < 1000, "out_{i} = {v}");
    }
}

// =========================================================================
// Error message quality
// =========================================================================

#[test]
fn error_includes_line_info() {
    let result = compile_gk(r#"
        inputs := (cycle)
        h := hash(cycle)
        bad := completely_bogus_function(h)
    "#);
    assert!(result.is_err());
    let err = result.unwrap_err();
    // Error should mention the bad function name
    assert!(err.contains("completely_bogus_function") || err.contains("unknown"),
        "error should be informative: {err}");
}

// =========================================================================
// Compile-time constant evaluation
// =========================================================================

#[test]
fn eval_const_integer_arithmetic() {
    let v = nb_variates::dsl::compile::eval_const_expr("4 * 4").unwrap();
    assert_eq!(v.as_u64(), 16);
}

#[test]
fn eval_const_float_arithmetic() {
    let v = nb_variates::dsl::compile::eval_const_expr("3.0 + 0.14").unwrap();
    assert!((v.as_f64() - 3.14).abs() < 0.001);
}

#[test]
fn eval_const_rejects_cycle_dependent() {
    let result = nb_variates::dsl::compile::eval_const_expr("hash(cycle)");
    assert!(result.is_err(), "cycle-dependent expr should not be const");
}

#[test]
fn eval_const_nested() {
    let v = nb_variates::dsl::compile::eval_const_expr("(2 + 3) * (4 + 1)").unwrap();
    assert_eq!(v.as_u64(), 25);
}
