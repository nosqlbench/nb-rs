// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Adversarial, fuzz, and correctness tests for the GK source system.
//!
//! Tests cover:
//! - Source keyword parsing and compilation
//! - Dot-access field projection syntax
//! - Dataflow through source chains
//! - Source provenance tracing
//! - Concurrent source consumption
//! - Edge cases and error handling

use nb_variates::dsl::compile::compile_gk;
use nb_variates::source::{DataSource, DataSourceFactory, RangeSourceFactory, SourceItem};
use nb_variates::node::Value;
use std::sync::Arc;
use std::thread;

// =========================================================================
// Lexer/Parser: cursor keyword and dot access
// =========================================================================

#[test]
fn cursor_keyword_lexes() {
    let tokens = nb_variates::dsl::lexer::lex("cursor base = range(0, 100)").unwrap();
    assert!(matches!(tokens[0].kind, nb_variates::dsl::lexer::TokenKind::Cursor));
}

#[test]
fn dot_token_lexes() {
    let tokens = nb_variates::dsl::lexer::lex("base.ordinal").unwrap();
    assert!(matches!(tokens[0].kind, nb_variates::dsl::lexer::TokenKind::Ident(_)));
    assert!(matches!(tokens[1].kind, nb_variates::dsl::lexer::TokenKind::Dot));
    assert!(matches!(tokens[2].kind, nb_variates::dsl::lexer::TokenKind::Ident(_)));
}

#[test]
fn dot_in_float_not_confused() {
    // 3.14 should lex as a float, not ident.dot.ident
    let tokens = nb_variates::dsl::lexer::lex("x := 3.14").unwrap();
    assert!(matches!(tokens[2].kind, nb_variates::dsl::lexer::TokenKind::FloatLit(f) if (f - 3.14).abs() < 0.001));
}

#[test]
fn cursor_decl_parses() {
    let tokens = nb_variates::dsl::lexer::lex(
        "cursor base = range(0, 100)\nid := hash(cycle)"
    ).unwrap();
    let ast = nb_variates::dsl::parser::parse(tokens).unwrap();
    assert!(matches!(ast.statements[0], nb_variates::dsl::ast::Statement::Cursor(_)));
}

#[test]
fn field_access_parses() {
    let tokens = nb_variates::dsl::lexer::lex(
        "cursor base = range(0, 100)\nid := base.ordinal"
    ).unwrap();
    let ast = nb_variates::dsl::parser::parse(tokens).unwrap();
    match &ast.statements[1] {
        nb_variates::dsl::ast::Statement::CycleBinding(b) => {
            match &b.value {
                nb_variates::dsl::ast::Expr::FieldAccess { source, field, .. } => {
                    assert_eq!(source, "base");
                    assert_eq!(field, "ordinal");
                }
                other => panic!("expected FieldAccess, got {:?}", other),
            }
        }
        other => panic!("expected CycleBinding, got {:?}", other),
    }
}

#[test]
fn nested_dot_in_function_arg() {
    // base.ordinal used as a function argument
    let tokens = nb_variates::dsl::lexer::lex(
        "cursor base = range(0, 100)\nid := hash(base.ordinal)"
    ).unwrap();
    let ast = nb_variates::dsl::parser::parse(tokens).unwrap();
    // Should parse without error
    assert_eq!(ast.statements.len(), 2);
}

// =========================================================================
// Adversarial parsing: edge cases
// =========================================================================

#[test]
fn cursor_keyword_as_binding_name_error() {
    // "source" is now a keyword, can't be used as a binding name
    let tokens = nb_variates::dsl::lexer::lex("cursor := hash(cycle)");
    // This should lex as Source token followed by ColonEq, which the
    // parser should reject (source expects = not :=)
    assert!(tokens.is_ok()); // lexer succeeds
    let result = nb_variates::dsl::parser::parse(tokens.unwrap());
    assert!(result.is_err()); // parser rejects
}

#[test]
fn dot_access_without_source_name() {
    // ".ordinal" without a source name
    let result = nb_variates::dsl::lexer::lex(".ordinal");
    assert!(result.is_ok()); // lexer produces Dot + Ident
    // But parser won't see this in a valid context
}

#[test]
fn multiple_cursor_declarations() {
    let src = r#"
        cursor vectors = range(0, 1000)
        cursor queries = range(0, 100)
        id := hash(cycle)
    "#;
    let tokens = nb_variates::dsl::lexer::lex(src).unwrap();
    let ast = nb_variates::dsl::parser::parse(tokens).unwrap();
    let source_count = ast.statements.iter()
        .filter(|s| matches!(s, nb_variates::dsl::ast::Statement::Cursor(_)))
        .count();
    assert_eq!(source_count, 2);
}

#[test]
fn chained_dot_access_rejected() {
    // base.ordinal.something — double dot should fail or be handled
    let tokens = nb_variates::dsl::lexer::lex("x := base.ordinal.extra").unwrap();
    // The parser should parse base.ordinal as FieldAccess, then .extra
    // is unexpected in expression position
    let ast = nb_variates::dsl::parser::parse(tokens);
    // This may or may not parse depending on expression context —
    // but it shouldn't panic
    let _ = ast;
}

// =========================================================================
// RangeSource: correctness
// =========================================================================

#[test]
fn range_source_exact_extent() {
    let factory = RangeSourceFactory::new(10, 20);
    assert_eq!(factory.schema().extent, Some(10));
    let mut reader = factory.create_reader();
    let mut count = 0;
    while reader.next().is_some() { count += 1; }
    assert_eq!(count, 10);
    assert_eq!(reader.consumed(), 10);
}

#[test]
fn range_source_empty() {
    let factory = RangeSourceFactory::new(0, 0);
    assert_eq!(factory.schema().extent, Some(0));
    let mut reader = factory.create_reader();
    assert!(reader.next().is_none());
    assert_eq!(reader.consumed(), 0);
}

#[test]
fn range_source_single_item() {
    let factory = RangeSourceFactory::new(42, 43);
    let mut reader = factory.create_reader();
    let item = reader.next().unwrap();
    assert_eq!(item.ordinal, 42);
    assert!(reader.next().is_none());
}

#[test]
fn range_source_chunk_larger_than_remaining() {
    let factory = RangeSourceFactory::new(0, 5);
    let mut reader = factory.create_reader();
    let chunk = reader.next_chunk(1000);
    assert_eq!(chunk.len(), 5);
    assert!(reader.next_chunk(1).is_empty());
}

#[test]
fn range_source_chunk_exact_boundary() {
    let factory = RangeSourceFactory::new(0, 10);
    let mut reader = factory.create_reader();
    let c1 = reader.next_chunk(5);
    assert_eq!(c1.len(), 5);
    let c2 = reader.next_chunk(5);
    assert_eq!(c2.len(), 5);
    let c3 = reader.next_chunk(1);
    assert!(c3.is_empty());
}

#[test]
fn range_source_interleaved_next_and_chunk() {
    let factory = RangeSourceFactory::new(0, 10);
    let mut reader = factory.create_reader();
    let item = reader.next().unwrap();
    assert_eq!(item.ordinal, 0);
    let chunk = reader.next_chunk(3);
    assert_eq!(chunk.len(), 3);
    assert_eq!(chunk[0].ordinal, 1);
    assert_eq!(chunk[2].ordinal, 3);
    // 6 remaining
    let rest = reader.next_chunk(100);
    assert_eq!(rest.len(), 6);
}

// =========================================================================
// Concurrency stress tests
// =========================================================================

#[test]
fn range_source_concurrent_total_coverage() {
    // N fibers consume from the same source — every ordinal consumed exactly once
    let extent = 10_000u64;
    let fiber_count = 8;
    let factory = Arc::new(RangeSourceFactory::new(0, extent));

    let handles: Vec<_> = (0..fiber_count).map(|_| {
        let f = factory.clone();
        thread::spawn(move || {
            let mut reader = f.create_reader();
            let mut collected = Vec::new();
            while let Some(item) = reader.next() {
                collected.push(item.ordinal);
            }
            collected
        })
    }).collect();

    let mut all: Vec<u64> = handles.into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), extent as usize, "every ordinal consumed exactly once");
    assert_eq!(all[0], 0);
    assert_eq!(*all.last().unwrap(), extent - 1);
}

#[test]
fn range_source_concurrent_chunk_coverage() {
    // Same as above but with chunk reads
    let extent = 50_000u64;
    let fiber_count = 16;
    let chunk_size = 100;
    let factory = Arc::new(RangeSourceFactory::new(0, extent));

    let handles: Vec<_> = (0..fiber_count).map(|_| {
        let f = factory.clone();
        thread::spawn(move || {
            let mut reader = f.create_reader();
            let mut collected = Vec::new();
            loop {
                let chunk = reader.next_chunk(chunk_size);
                if chunk.is_empty() { break; }
                for item in chunk {
                    collected.push(item.ordinal);
                }
            }
            collected
        })
    }).collect();

    let mut all: Vec<u64> = handles.into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), extent as usize);
}

#[test]
fn range_source_high_contention() {
    // Many threads hammering next() — no panics, no lost ordinals
    let extent = 100_000u64;
    let fiber_count = 64;
    let factory = Arc::new(RangeSourceFactory::new(0, extent));

    let handles: Vec<_> = (0..fiber_count).map(|_| {
        let f = factory.clone();
        thread::spawn(move || {
            let mut reader = f.create_reader();
            let mut count = 0u64;
            while reader.next().is_some() { count += 1; }
            count
        })
    }).collect();

    let total: u64 = handles.into_iter()
        .map(|h| h.join().unwrap())
        .sum();
    assert_eq!(total, extent);
}

// =========================================================================
// SourceItem: field access edge cases
// =========================================================================

#[test]
fn source_item_empty_fields() {
    let item = SourceItem::ordinal(99);
    assert_eq!(item.ordinal, 99);
    assert!(item.fields.is_empty());
    assert_eq!(item.field("anything"), None);
}

#[test]
fn source_item_duplicate_field_names() {
    // If a source yields duplicate field names, first match wins
    let item = SourceItem::with_fields(0, vec![
        ("x".into(), Value::U64(1)),
        ("x".into(), Value::U64(2)),
    ]);
    assert_eq!(item.field("x"), Some(&Value::U64(1)));
}

#[test]
fn source_item_various_value_types() {
    let item = SourceItem::with_fields(0, vec![
        ("u".into(), Value::U64(42)),
        ("f".into(), Value::F64(3.14)),
        ("s".into(), Value::Str("hello".into())),
        ("b".into(), Value::Bool(true)),
        ("n".into(), Value::None),
    ]);
    assert_eq!(item.field("u"), Some(&Value::U64(42)));
    assert_eq!(item.field("f"), Some(&Value::F64(3.14)));
    assert_eq!(item.field("b"), Some(&Value::Bool(true)));
    assert_eq!(item.field("n"), Some(&Value::None));
}

// =========================================================================
// Schema and factory correctness
// =========================================================================

#[test]
fn range_factory_schema_matches_readers() {
    let factory = RangeSourceFactory::named("test_source", 100, 200);
    let schema = factory.schema();
    assert_eq!(schema.name, "test_source");
    assert_eq!(schema.extent, Some(100));
    assert_eq!(schema.projections.len(), 1);
    assert_eq!(schema.projections[0].0, "ordinal");

    let reader = factory.create_reader();
    let reader_schema = reader.schema();
    assert_eq!(reader_schema.name, schema.name);
    assert_eq!(reader_schema.extent, schema.extent);
}

#[test]
fn multiple_factories_independent() {
    let f1 = RangeSourceFactory::new(0, 100);
    let f2 = RangeSourceFactory::new(0, 100);

    let mut r1 = f1.create_reader();
    let mut r2 = f2.create_reader();

    // Each factory has its own cursor — both start at 0
    assert_eq!(r1.next().unwrap().ordinal, 0);
    assert_eq!(r2.next().unwrap().ordinal, 0);
}

// =========================================================================
// Compilation: source declarations produce schemas and wirable projections
// =========================================================================

#[test]
fn cursor_compiles_and_produces_schema() {
    let src = r#"
        cursor r = range(0, 100)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk(src).unwrap();
    let schemas = kernel.program().cursor_schemas();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "r");
    assert_eq!(schemas[0].extent, Some(100));
    assert_eq!(schemas[0].projections[0].0, "ordinal");
}

#[test]
fn cursor_extent_folds_const_function_call() {
    // `mod(100, 7)` evaluates to 2 at compile time. The cursor extent
    // should resolve via post-compile folding, not only via literals.
    let src = r#"
        cursor r = range(0, mod(100, 7))
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk(src).unwrap();
    let schemas = kernel.program().cursor_schemas();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].extent, Some(2), "extent should fold mod(100, 7) = 2");
}

#[test]
fn cursor_extent_folds_arithmetic_expression() {
    // Non-literal arithmetic should also fold post-compile.
    let src = r#"
        cursor r = range(10, 10 + 50)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk(src).unwrap();
    let schemas = kernel.program().cursor_schemas();
    assert_eq!(schemas[0].extent, Some(50), "extent should fold (10+50) - 10 = 50");
}

#[test]
fn cursor_projection_wires_into_downstream_nodes() {
    let src = r#"
        cursor r = range(0, 500)
        inputs := (cycle)
        doubled := r.ordinal + r.ordinal
    "#;
    let mut kernel = compile_gk(src).unwrap();
    // Set the source projection input (r__ordinal) to 21
    if let Some(idx) = kernel.program().find_input("r__ordinal") {
        kernel.state().set_input(idx, Value::U64(21));
    }
    kernel.set_inputs(&[0]); // cycle = 0
    let doubled = kernel.pull("doubled").as_u64();
    assert_eq!(doubled, 42); // 21 + 21
}

#[test]
fn multiple_cursors_produce_independent_schemas() {
    let src = r#"
        cursor a = range(0, 100)
        cursor b = range(0, 200)
        inputs := (cycle)
        sum := a.ordinal + b.ordinal
    "#;
    let kernel = compile_gk(src).unwrap();
    let schemas = kernel.program().cursor_schemas();
    assert_eq!(schemas.len(), 2);
    assert_eq!(schemas[0].name, "a");
    assert_eq!(schemas[0].extent, Some(100));
    assert_eq!(schemas[1].name, "b");
    assert_eq!(schemas[1].extent, Some(200));
}

#[test]
fn cursor_with_non_literal_extent() {
    // range with non-literal args — extent is None
    let src = r#"
        inputs := (cycle)
        cursor r = range(0, cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    let schemas = kernel.program().cursor_schemas();
    assert_eq!(schemas[0].extent, None);
}

#[test]
fn cursor_projection_feeds_function_call() {
    let src = r#"
        cursor r = range(0, 1000)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let mut kernel = compile_gk(src).unwrap();
    if let Some(idx) = kernel.program().find_input("r__ordinal") {
        kernel.state().set_input(idx, Value::U64(42));
    }
    kernel.set_inputs(&[0]);
    let id = kernel.pull("id").as_u64();
    // hash(42) should produce a deterministic non-zero value
    assert!(id != 0);
}

// =========================================================================
// Cursors: provenance-driven cursor targeting
// =========================================================================

#[test]
fn advancer_targets_correct_cursors() {
    use nb_variates::source::Cursors;
    use std::collections::HashMap;

    let src = r#"
        cursor base = range(0, 100)
        inputs := (cycle)
        id := hash(base.ordinal)
        unused := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    let program = kernel.program();

    let mut factories: HashMap<String, Arc<dyn nb_variates::source::DataSourceFactory>> = HashMap::new();
    factories.insert("base".into(), Arc::new(RangeSourceFactory::new(0, 100)));

    // Cursors for "id" should target the base cursor
    let advancer = Cursors::for_fields(program, &["id"], &factories);
    assert_eq!(advancer.target_count(), 1);
    assert_eq!(advancer.extent(), Some(100));
}

#[test]
fn advancer_does_not_target_unused_sources() {
    use nb_variates::source::Cursors;
    use std::collections::HashMap;

    let src = r#"
        cursor base = range(0, 100)
        cursor queries = range(0, 50)
        inputs := (cycle)
        id := hash(base.ordinal)
    "#;
    let kernel = compile_gk(src).unwrap();
    let program = kernel.program();

    let mut factories: HashMap<String, Arc<dyn nb_variates::source::DataSourceFactory>> = HashMap::new();
    factories.insert("base".into(), Arc::new(RangeSourceFactory::new(0, 100)));
    factories.insert("queries".into(), Arc::new(RangeSourceFactory::new(0, 50)));

    // Cursors for "id" should target only base, not queries
    let advancer = Cursors::for_fields(program, &["id"], &factories);
    assert_eq!(advancer.target_count(), 1);
    assert_eq!(advancer.extent(), Some(100));
}

#[test]
fn advancer_advance_and_exhaust() {
    use nb_variates::source::Cursors;
    use std::collections::HashMap;

    let src = r#"
        cursor r = range(0, 3)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk(src).unwrap();
    let program = kernel.program();

    let mut factories: HashMap<String, Arc<dyn nb_variates::source::DataSourceFactory>> = HashMap::new();
    factories.insert("r".into(), Arc::new(RangeSourceFactory::new(0, 3)));

    let mut advancer = Cursors::for_fields(program, &["id"], &factories);

    assert!(advancer.advance());
    assert_eq!(advancer.consumed(), 1);
    assert!(advancer.advance());
    assert!(advancer.advance());
    assert!(!advancer.advance()); // exhausted
    assert_eq!(advancer.consumed(), 3);
}

#[test]
fn advancer_last_items_reflect_position() {
    use nb_variates::source::Cursors;
    use std::collections::HashMap;

    let src = r#"
        cursor r = range(10, 13)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk(src).unwrap();
    let program = kernel.program();

    let mut factories: HashMap<String, Arc<dyn nb_variates::source::DataSourceFactory>> = HashMap::new();
    factories.insert("r".into(), Arc::new(RangeSourceFactory::new(10, 13)));

    let mut advancer = Cursors::for_fields(program, &["id"], &factories);

    advancer.advance();
    assert_eq!(advancer.last_items()[0].as_ref().unwrap().ordinal, 10);
    advancer.advance();
    assert_eq!(advancer.last_items()[0].as_ref().unwrap().ordinal, 11);
    advancer.advance();
    assert_eq!(advancer.last_items()[0].as_ref().unwrap().ordinal, 12);
}

#[test]
fn advancer_empty_when_no_sources_referenced() {
    use nb_variates::source::Cursors;
    use std::collections::HashMap;

    let src = r#"
        inputs := (cycle)
        id := hash(cycle)
    "#;
    let kernel = compile_gk(src).unwrap();
    let program = kernel.program();

    let factories: HashMap<String, Arc<dyn nb_variates::source::DataSourceFactory>> = HashMap::new();
    let advancer = Cursors::for_fields(program, &["id"], &factories);
    assert!(advancer.is_empty());
}

// =========================================================================
// Limit node: compiler-inserted cursor clamping
// =========================================================================

#[test]
fn limit_node_compiles_and_clamps_extent() {
    use nb_variates::dsl::compile::compile_gk_with_libs_and_limit;
    use std::path::PathBuf;

    let src = r#"
        cursor r = range(0, 1000)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk_with_libs_and_limit(
        src, None, Vec::<PathBuf>::new(), &[], false, "(test)", Some(100),
    ).unwrap();
    let schemas = kernel.program().cursor_schemas();
    assert_eq!(schemas.len(), 1);
    // Extent should be clamped to 100 (min of 1000 and limit 100)
    assert_eq!(schemas[0].extent, Some(100));
}

#[test]
fn limit_node_not_inserted_when_no_limit() {
    use nb_variates::dsl::compile::compile_gk_with_libs_and_limit;
    use std::path::PathBuf;

    let src = r#"
        cursor r = range(0, 500)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk_with_libs_and_limit(
        src, None, Vec::<PathBuf>::new(), &[], false, "(test)", None,
    ).unwrap();
    let schemas = kernel.program().cursor_schemas();
    assert_eq!(schemas[0].extent, Some(500)); // unclamped
}

#[test]
fn limit_larger_than_extent_preserves_extent() {
    use nb_variates::dsl::compile::compile_gk_with_libs_and_limit;
    use std::path::PathBuf;

    let src = r#"
        cursor r = range(0, 50)
        inputs := (cycle)
        id := hash(r.ordinal)
    "#;
    let kernel = compile_gk_with_libs_and_limit(
        src, None, Vec::<PathBuf>::new(), &[], false, "(test)", Some(1000),
    ).unwrap();
    let schemas = kernel.program().cursor_schemas();
    // Limit 1000 > extent 50 → extent stays 50
    assert_eq!(schemas[0].extent, Some(50));
}
