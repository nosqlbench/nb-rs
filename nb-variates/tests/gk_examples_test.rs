// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for .gk example files in tests/examples/gk/.
//!
//! Each test compiles the .gk file through the full DSL pipeline,
//! captures the GK event stream, runs a few cycles to verify output,
//! and checks that expected optimization events were emitted.

use nb_variates::dsl::compile::compile_gk_to_assembler;
use nb_variates::dsl::events::{CompileEvent, CompileEventLog};

/// Compile a .gk source with event logging, returning (kernel, events).
fn compile_with_events(source: &str) -> (nb_variates::kernel::GkKernel, CompileEventLog) {
    let mut log = CompileEventLog::new();

    let tokens = nb_variates::dsl::lexer::lex(source)
        .unwrap_or_else(|e| panic!("lex failed: {e}"));
    let ast = nb_variates::dsl::parser::parse(tokens)
        .unwrap_or_else(|e| panic!("parse failed: {e}"));
    log.push(CompileEvent::Parsed { statements: ast.statements.len() });

    let asm = compile_gk_to_assembler(source)
        .unwrap_or_else(|e| panic!("compile failed: {e}"));
    for name in asm.output_names() {
        log.push(CompileEvent::OutputDeclared { name: name.to_string() });
    }

    let kernel = asm.compile_with_log(Some(&mut log))
        .unwrap_or_else(|e| panic!("assembly failed: {e}"));

    let program = kernel.program();
    for name in program.output_names() {
        if let Some((idx, _)) = program.resolve_output(name) {
            let level = program.node_compile_level(idx);
            log.push(CompileEvent::CompileLevelSelected {
                node: name.to_string(),
                level: format!("{level:?}"),
            });
        }
    }
    log.push(CompileEvent::Summary {
        nodes: program.node_count(),
        outputs: program.output_names().len(),
        constants_folded: kernel.constants_folded,
    });

    (kernel, log)
}

fn load_gk(name: &str) -> String {
    let path = format!("tests/examples/gk/{name}");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {path}: {e}"))
}

fn has_event(log: &CompileEventLog, pred: impl Fn(&CompileEvent) -> bool) -> bool {
    log.events().iter().any(pred)
}

// =========================================================================
// constant_folding.gk
// =========================================================================

#[test]
fn constant_folding_compiles_and_folds() {
    let source = load_gk("constant_folding.gk");
    let (mut kernel, log) = compile_with_events(&source);

    // Should have folded base and seed
    assert!(has_event(&log, |e| matches!(e, CompileEvent::ConstantFolded { node, .. } if node == "const")),
        "base (const 42) should be folded\n{}", log.format());
    assert!(has_event(&log, |e| matches!(e, CompileEvent::ConstantFolded { node, .. } if node == "hash")),
        "seed (hash of base) should be folded\n{}", log.format());

    // user_id should vary per cycle (not folded)
    kernel.set_inputs(&[0]);
    let a = kernel.pull("user_id").as_u64();
    kernel.set_inputs(&[1]);
    let b = kernel.pull("user_id").as_u64();
    assert_ne!(a, b, "user_id should vary per cycle");
    assert!(a < 1_000_000 && b < 1_000_000);

    // seed should be constant across cycles
    kernel.set_inputs(&[0]);
    let s0 = kernel.pull("seed").as_u64();
    kernel.set_inputs(&[999]);
    let s1 = kernel.pull("seed").as_u64();
    assert_eq!(s0, s1, "seed should be folded to a constant");
}

// =========================================================================
// type_adapters.gk
// =========================================================================

#[test]
fn type_adapters_compiles() {
    let source = load_gk("type_adapters.gk");
    let (mut kernel, log) = compile_with_events(&source);

    // sin and cos should produce values in [-1, 1]
    for cycle in 0..100 {
        kernel.set_inputs(&[cycle]);
        let s = kernel.pull("s").as_f64();
        let c = kernel.pull("c").as_f64();
        assert!(s >= -1.0 && s <= 1.0, "sin out of range: {s}");
        assert!(c >= -1.0 && c <= 1.0, "cos out of range: {c}");
    }

    eprintln!("{}", log.format());
}

// =========================================================================
// multi_output.gk
// =========================================================================

#[test]
fn multi_output_compiles() {
    let source = load_gk("multi_output.gk");
    let (mut kernel, log) = compile_with_events(&source);

    kernel.set_inputs(&[0]);
    assert_eq!(kernel.pull("region").as_u64(), 0);
    assert_eq!(kernel.pull("store").as_u64(), 0);

    kernel.set_inputs(&[51]);
    assert_eq!(kernel.pull("region").as_u64(), 1); // 51 % 50 = 1
    assert_eq!(kernel.pull("store").as_u64(), 1); // 51 / 50 = 1

    // region_id and store_id should be bounded
    for cycle in 0..200 {
        kernel.set_inputs(&[cycle]);
        assert!(kernel.pull("region_id").as_u64() < 10000);
        assert!(kernel.pull("store_id").as_u64() < 100000);
    }

    eprintln!("{}", log.format());
}

// =========================================================================
// string_generation.gk
// =========================================================================

#[test]
fn string_generation_compiles() {
    let source = load_gk("string_generation.gk");
    let (mut kernel, log) = compile_with_events(&source);

    for cycle in 0..10 {
        kernel.set_inputs(&[cycle]);
        let code = kernel.pull("code").to_display_string();
        let decimal = kernel.pull("decimal").to_display_string();
        let hex = kernel.pull("hex").to_display_string();
        assert!(!code.is_empty(), "combinations should produce output: {code}");
        assert!(!decimal.is_empty());
        assert!(!hex.is_empty());
    }

    eprintln!("{}", log.format());
}

// =========================================================================
// distributions.gk
// =========================================================================

#[test]
fn distributions_compiles() {
    let source = load_gk("distributions.gk");
    let (mut kernel, log) = compile_with_events(&source);

    let mut normal_sum = 0.0;
    let valid_outcomes = [100u64, 200, 300];
    for cycle in 0..1000 {
        kernel.set_inputs(&[cycle]);
        normal_sum += kernel.pull("normal").as_f64();
        let outcome = kernel.pull("outcome").as_u64();
        assert!(valid_outcomes.contains(&outcome), "unexpected outcome: {outcome}");
    }
    let mean = normal_sum / 1000.0;
    assert!((mean - 100.0).abs() < 10.0, "normal mean should be ~100, got {mean}");

    eprintln!("{}", log.format());
}

// =========================================================================
// weighted_selection.gk
// =========================================================================

#[test]
fn weighted_selection_compiles() {
    let source = load_gk("weighted_selection.gk");
    let (mut kernel, log) = compile_with_events(&source);

    let mut heads = 0u64;
    for cycle in 0..1000 {
        kernel.set_inputs(&[cycle]);
        if kernel.pull("coin").as_u64() == 1 { heads += 1; }
        let color = kernel.pull("color").to_display_string();
        assert!(["red", "blue", "green"].contains(&color.as_str()),
            "unexpected color: {color}");
        let tier = kernel.pull("tier").as_u64();
        assert!(tier >= 1 && tier <= 3, "unexpected tier: {tier}");
    }
    // Fair coin should be roughly 50%
    assert!(heads > 400 && heads < 600, "fair coin: {heads}/1000 heads");

    eprintln!("{}", log.format());
}

// =========================================================================
// datetime_context.gk
// =========================================================================

#[test]
fn datetime_context_compiles() {
    let source = load_gk("datetime_context.gk");
    let (mut kernel, log) = compile_with_events(&source);

    kernel.set_inputs(&[0]);
    let ts = kernel.pull("ts").to_display_string();
    assert!(ts.contains("2024"), "timestamp should contain year 2024: {ts}");

    let wall = kernel.pull("wall").as_u64();
    assert!(wall > 1_700_000_000_000, "wall clock should be recent: {wall}");

    let tid = kernel.pull("tid").as_u64();
    assert!(tid > 0, "thread_id should be positive: {tid}");

    eprintln!("{}", log.format());
}

// =========================================================================
// math_trig.gk
// =========================================================================

#[test]
fn math_trig_compiles() {
    let source = load_gk("math_trig.gk");
    let (mut kernel, log) = compile_with_events(&source);

    for cycle in 0..100 {
        kernel.set_inputs(&[cycle]);
        let sine = kernel.pull("sine").as_f64();
        let cosine = kernel.pull("cosine").as_f64();
        let root = kernel.pull("root").as_f64();
        let exp = kernel.pull("exponential").as_f64();
        let scaled = kernel.pull("scaled").as_f64();
        let clamped = kernel.pull("clamped").as_f64();

        assert!(sine >= -1.0 && sine <= 1.0, "sin: {sine}");
        assert!(cosine >= -1.0 && cosine <= 1.0, "cos: {cosine}");
        assert!(root >= 0.0 && root <= 1.0, "sqrt: {root}");
        assert!(exp >= 1.0 && exp < std::f64::consts::E + 0.01, "exp: {exp}");
        assert!(scaled >= -100.0 && scaled <= 100.0, "scaled: {scaled}");
        assert!(clamped >= -50.0 && clamped <= 50.0, "clamped: {clamped}");
    }

    eprintln!("{}", log.format());
}

// =========================================================================
// json_encoding.gk
// =========================================================================

#[test]
fn json_encoding_compiles() {
    let source = load_gk("json_encoding.gk");
    let (mut kernel, log) = compile_with_events(&source);

    kernel.set_inputs(&[42]);
    let js = kernel.pull("js").to_display_string();
    assert!(!js.is_empty(), "json_to_str should produce output");

    let encoded = kernel.pull("encoded").to_display_string();
    assert!(!encoded.is_empty(), "url_encode should produce output");

    let b64 = kernel.pull("b64").to_display_string();
    assert!(!b64.is_empty(), "to_base64 should produce output");

    eprintln!("{}", log.format());
}

// =========================================================================
// noise_pcg.gk
// =========================================================================

#[test]
fn noise_pcg_compiles() {
    let source = load_gk("noise_pcg.gk");
    let (mut kernel, log) = compile_with_events(&source);

    for cycle in 0..100 {
        kernel.set_inputs(&[cycle]);
        let noise = kernel.pull("noise").as_f64();
        assert!(noise >= -1.0 && noise <= 1.0, "perlin_1d: {noise}");

        let _shuffled = kernel.pull("shuffled").as_u64();
        let _walked = kernel.pull("walked").as_u64();
    }

    eprintln!("{}", log.format());
}

// =========================================================================
// real_data.gk
// =========================================================================

#[test]
fn real_data_compiles() {
    let source = load_gk("real_data.gk");
    let (mut kernel, log) = compile_with_events(&source);

    for cycle in 0..10 {
        kernel.set_inputs(&[cycle]);
        let fname = kernel.pull("fname").to_display_string();
        let fullname = kernel.pull("fullname").to_display_string();
        let state = kernel.pull("state").to_display_string();
        let country = kernel.pull("country").to_display_string();

        assert!(!fname.is_empty(), "first_names should produce output");
        assert!(!fullname.is_empty(), "full_names should produce output");
        assert!(fullname.contains(' '), "full_names should have space: {fullname}");
        assert_eq!(state.len(), 2, "state_codes should be 2 chars: {state}");
        assert!(!country.is_empty(), "country_names should produce output");
    }

    eprintln!("{}", log.format());
}

// =========================================================================
// empirical_dist.gk
// =========================================================================

#[test]
fn empirical_dist_compiles() {
    let source = load_gk("empirical_dist.gk");
    let (mut kernel, log) = compile_with_events(&source);

    for cycle in 0..1000 {
        kernel.set_inputs(&[cycle]);
        let v = kernel.pull("latency").as_f64();
        assert!(v >= 0.5 && v <= 100.0,
            "empirical should be in [0.5, 100.0]: {v} at cycle={cycle}");
    }

    eprintln!("{}", log.format());
}

// =========================================================================
// Meta: all .gk files compile
// =========================================================================

#[test]
fn all_gk_examples_compile() {
    let dir = std::path::Path::new("tests/examples/gk");
    let mut count = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().map(|e| e == "gk").unwrap_or(false) {
            let name = path.file_name().unwrap().to_str().unwrap();
            let source = std::fs::read_to_string(&path).unwrap();
            let (kernel, log) = compile_with_events(&source);
            assert!(kernel.program().node_count() > 0,
                "{name}: should produce at least one node");
            eprintln!("--- {name} ---\n{}\n", log.format());
            count += 1;
        }
    }
    assert!(count >= 10, "expected at least 10 .gk examples, found {count}");
}
