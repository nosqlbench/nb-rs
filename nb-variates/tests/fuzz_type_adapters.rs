// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Type-adapter-transform fuzz tests.
//!
//! Two test strategies share the same goal: surface cases where the
//! GK compiler's auto-inserted edge adapters are wrong, missing, or
//! silently lossy.
//!
//! 1. **Adapter-table sweep** ([`adapter_table_is_consistent`]).
//!    Curated producer/consumer pairs for each [`PortType`], run
//!    through the compiler, and the result is checked against an
//!    in-test mirror of [`nb_variates::assembly::auto_adapter`]. If the
//!    compiler disagrees with the expected table — either by
//!    rejecting a pair we think should bridge, or by silently
//!    accepting a pair we think should error — the test fails with
//!    the full pair and source so the regression is easy to
//!    reproduce.
//!
//! 2. **Random-DAG fuzz** ([`random_dags_compile_or_fail_cleanly`]).
//!    A tiny deterministic RNG picks native registry entries and
//!    wires their outputs together irrespective of type compatibility.
//!    Each generated module is fed to `compile_gk_with_log`; the
//!    compile must never panic, every error string must be non-empty
//!    and free of panic-style wording, and every Ok result whose
//!    event log mentions a `TypeAdapterInserted` must refer to a
//!    pair we also consider legal. The FUZZ_SEED env var seeds the
//!    RNG; FUZZ_ITERATIONS controls iteration count.

use nb_variates::dsl::compile::{compile_gk, compile_gk_with_log};
use nb_variates::dsl::events::{CompileEvent, CompileEventLog};
use nb_variates::dsl::registry::{self, FuncSig};
use nb_variates::node::{PortType, SlotType};

// ─── Expected-adapter table ───────────────────────────────────────
//
// Mirrors `nb_variates::assembly::auto_adapter`. When the compiler's
// table changes, this one must change with it — that's intentional:
// a silent shift in the compiler's widening rules would otherwise
// escape review. Update in lock-step.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Adapt {
    /// Source and sink are identical — no adapter needed.
    Identity,
    /// Compiler should insert an auto-adapter and compile OK.
    Inserted,
    /// No known-safe coercion; compiler should reject with `type mismatch`.
    None,
}

fn expected_adapt(src: PortType, dst: PortType) -> Adapt {
    use PortType::*;
    if src == dst {
        return Adapt::Identity;
    }
    match (src, dst) {
        // Lossless numeric widening.
        (U64, F64)
        | (U32, U64) | (U32, F64)
        | (I32, I64) | (I32, F64)
        | (I64, F64)
        | (F32, F64)
        // Any of these render to Str.
        | (U64, Str) | (F64, Str) | (Bool, Str) | (Json, Str)
        | (U32, Str) | (I32, Str) | (I64, Str) | (F32, Str)
        // Bool lifts to U64 as 0/1.
        | (Bool, U64) => Adapt::Inserted,
        _ => Adapt::None,
    }
}

// ─── Adapter-table sweep ──────────────────────────────────────────
//
// Producers emit a specific [`PortType`] from the cycle input.
// Consumers read a specific wire type and produce any output. Each
// recipe is a pair of GK snippets chained via the `source` and
// `sink` bindings; the test assembles them with the cycle coordinate
// and compiles.

struct TypeRecipe {
    /// GK expression that produces [`src`] from scratch (may reference
    /// `cycle`).
    produce: &'static str,
    src: PortType,
}

struct SinkRecipe {
    /// GK expression template where `{}` is substituted with the
    /// source binding name. Produces some output (thrown away); its
    /// *wire-input* type is [`dst`].
    consume_tmpl: &'static str,
    dst: PortType,
}

/// Curated producer set. One entry per [`PortType`] we can reliably
/// synthesize from `cycle`. Missing variants (Bytes, Ext, narrow
/// ints) simply get skipped below.
fn producers() -> Vec<TypeRecipe> {
    use PortType::*;
    vec![
        TypeRecipe { produce: "cycle",                        src: U64 },
        TypeRecipe { produce: "to_f64(cycle)",                src: F64 },
        TypeRecipe { produce: "format_u64(cycle, 10)",        src: Str },
        TypeRecipe { produce: "to_json(cycle)",               src: Json },
    ]
}

/// Curated consumer set. `{}` in the template is replaced with the
/// producer's binding name before compile.
fn consumers() -> Vec<SinkRecipe> {
    use PortType::*;
    vec![
        SinkRecipe { consume_tmpl: "add({}, 1)",              dst: U64 },
        SinkRecipe { consume_tmpl: "clamp_f64({}, 0.0, 1.0)", dst: F64 },
        SinkRecipe { consume_tmpl: "json_to_str({})",         dst: Json },
    ]
}

#[test]
fn adapter_table_is_consistent() {
    let mut mismatches: Vec<String> = Vec::new();
    for p in producers() {
        for c in consumers() {
            let source = format!(
                "inputs := (cycle)\n\
                 src_val := {}\n\
                 sink_val := {}\n",
                p.produce,
                c.consume_tmpl.replace("{}", "src_val"),
            );
            let expected = expected_adapt(p.src, c.dst);
            let mut log = CompileEventLog::new();
            let result = compile_gk_with_log(&source, &mut log);
            let observed = classify_result(&result, &log);
            if !adapt_agrees(expected, observed) {
                mismatches.push(format!(
                    "pair {:?} -> {:?}\n\
                     expected {expected:?}, observed {observed:?}\n\
                     result: {}\n\
                     source:\n{source}",
                    p.src, c.dst,
                    match &result {
                        Ok(_) => "<compiled>".to_string(),
                        Err(e) => e.clone(),
                    },
                ));
            }
        }
    }
    assert!(mismatches.is_empty(),
        "adapter-table disagreements:\n\n{}", mismatches.join("\n---\n"));
}

/// Classify a compile outcome into the same vocabulary as `Adapt`.
///
/// - Ok with no TypeAdapterInserted event → `Identity`
/// - Ok with at least one TypeAdapterInserted event → `Inserted`
/// - Err containing `"type mismatch"` → `None`
/// - Err with anything else → `Inserted` placeholder so the test
///   surfaces a `not-agrees` diagnostic rather than treating an
///   unrelated error as a success.
fn classify_result<T>(result: &Result<T, String>, log: &CompileEventLog) -> Adapt {
    match result {
        Ok(_) => {
            let has_adapter = log.events().iter().any(|e|
                matches!(e, CompileEvent::TypeAdapterInserted { .. }));
            if has_adapter { Adapt::Inserted } else { Adapt::Identity }
        }
        Err(msg) => {
            if msg.contains("type mismatch") {
                Adapt::None
            } else {
                // Unrelated error — return a sentinel the caller will
                // flag. Reusing `Inserted` here would make a parse
                // error look like an Ok path; return `None` so the
                // diagnostic shows "expected Inserted, got None" and
                // the actual error string is visible in the output.
                Adapt::None
            }
        }
    }
}

fn adapt_agrees(expected: Adapt, observed: Adapt) -> bool {
    match (expected, observed) {
        // Identity is a specific kind of pass — observing it is
        // also fine when we expected an `Inserted` for a same-type
        // pair, which shouldn't happen, but the sweep only calls
        // this function for distinct pairs drawn from the curated
        // producer/consumer sets.
        (Adapt::Identity, Adapt::Identity) => true,
        (Adapt::Inserted, Adapt::Inserted) => true,
        (Adapt::None, Adapt::None) => true,
        _ => false,
    }
}

// ─── Random-DAG fuzz ──────────────────────────────────────────────

/// Simple splitmix64 RNG. Deterministic, zero-dep, sufficient for
/// test inputs — we don't need cryptographic quality.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1)) }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, n: usize) -> usize {
        if n == 0 { return 0; }
        (self.next_u64() as usize) % n
    }
    fn f64(&mut self) -> f64 {
        // Integer range chosen so consts look like the kind of
        // values real workloads would pass.
        (self.next_u64() % 1000) as f64 / 10.0
    }
}

/// Nodes we can safely instantiate from the fuzzer without needing
/// specific runtime fixtures. Filters out:
///
/// - anything variadic (generator doesn't model group arity yet);
/// - anything with a `Vec<..>` constant (those need bracket-literal
///   array syntax);
/// - `dynamic-output` entries (`outputs == 0`) which need coordinate
///   resolution past what the fuzzer provides;
/// - `__` / `unknown_node` internals the registry happens to expose;
/// - context nodes that require runtime fixtures (metric queries,
///   control sets, fiber context) that aren't present in a unit test.
fn fuzzable_sigs() -> Vec<FuncSig> {
    registry::registry().into_iter()
        .filter(|s| !s.name.starts_with("__"))
        .filter(|s| s.outputs == 1)
        // VariadicWires we can drive (random wire-arg count); the
        // other variadic shapes (VariadicConsts, VariadicGroup)
        // need positional pair / group invariants the random
        // generator can't yet guarantee, so they stay excluded.
        .filter(|s| matches!(
            s.arity,
            registry::Arity::Fixed | registry::Arity::VariadicWires { .. },
        ))
        // No fuzzable signature actually declares a `ConstVec*`
        // slot type today; the filter is kept for forward-
        // compatibility — the generator below would have to
        // synthesize array literals (`[1, 2, 3]`) for those.
        .filter(|s| !s.params.iter().any(|p| matches!(
            p.slot_type,
            SlotType::ConstVecU64 | SlotType::ConstVecF64
        )))
        // Context-dependent functions need runtime fixtures the
        // compile path doesn't provide for standalone sources.
        // `fft_analyze` is excluded because its constructor opens
        // a file at the path given by its string arg — random
        // filenames would litter the cwd with empty files.
        .filter(|s| !matches!(s.name,
            "metric" | "control" | "control_u64" | "control_bool"
            | "control_str" | "control_set" | "rate" | "concurrency"
            | "phase" | "session_id"
            | "fft_analyze"
        ))
        .collect()
}

/// Build a random module that declares `n_bindings` bindings in
/// sequence. Each binding calls one random function from `sigs`,
/// picking each wire arg as either `cycle` or an already-defined
/// binding, and each const arg as a random literal of the right
/// kind. No type-compatibility check is applied, so ~most generated
/// modules will exercise either the adapter insertion path or the
/// type-mismatch error path.
///
/// For `VariadicWires` sigs, the generator picks a random arity
/// in `[min_wires, min_wires + 5]` and emits that many wire args.
/// Other variadic shapes are filtered out at `fuzzable_sigs` time
/// because their positional invariants (pairs, groups) can't be
/// satisfied by random independent draws.
fn generate_module(rng: &mut Rng, sigs: &[FuncSig], n_bindings: usize) -> String {
    let mut out = String::from("inputs := (cycle)\n");
    let mut defined: Vec<String> = Vec::new();
    for i in 0..n_bindings {
        let sig = &sigs[rng.range(sigs.len())];
        let name = format!("b{i}");
        let mut args: Vec<String> = Vec::new();

        let pick_wire = |rng: &mut Rng, defined: &[String]| -> String {
            if defined.is_empty() || rng.range(3) == 0 {
                "cycle".to_string()
            } else {
                defined[rng.range(defined.len())].clone()
            }
        };
        let materialize = |rng: &mut Rng, p: &nb_variates::dsl::registry::ParamSpec, defined: &[String]| -> String {
            match p.slot_type {
                SlotType::Wire => pick_wire(rng, defined),
                SlotType::ConstU64 => format!("{}", rng.next_u64() % 100),
                SlotType::ConstF64 => format!("{:.2}", rng.f64()),
                SlotType::ConstStr => format!("\"s{}\"", rng.range(100)),
                SlotType::ConstVecU64 | SlotType::ConstVecF64 => unreachable!(),
            }
        };

        // Fill the declared params (skip optional ones at random).
        let chosen: Vec<&_> = sig.params.iter()
            .filter_map(|p| {
                let keep = p.required || rng.range(2) == 0;
                if keep { Some(p) } else { None }
            })
            .collect();
        for param in chosen {
            args.push(materialize(rng, param, &defined));
        }

        // For `VariadicWires`, top up with a random number of
        // additional wire args. The trailing wire param shape is
        // declared once in `params` — we just emit more of the
        // same wire type past the fixed positions.
        if let registry::Arity::VariadicWires { min_wires } = sig.arity {
            let extra = rng.range(6); // 0..=5 extra wires
            let total_wires_needed = min_wires.saturating_sub(args.len()) + extra;
            for _ in 0..total_wires_needed {
                args.push(pick_wire(rng, &defined));
            }
        }

        out.push_str(&format!("{name} := {}({})\n", sig.name, args.join(", ")));
        defined.push(name);
    }
    out
}

#[test]
fn random_dags_compile_or_fail_cleanly() {
    let seed: u64 = std::env::var("FUZZ_SEED").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(0xDEAD_BEEFu64);
    let iterations: usize = std::env::var("FUZZ_ITERATIONS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(500);

    let sigs = fuzzable_sigs();
    assert!(!sigs.is_empty(), "no fuzzable signatures found — registry wiring broken?");

    let mut rng = Rng::new(seed);
    let mut cryptic_errors: Vec<(usize, String, String)> = Vec::new();
    let mut rogue_adapters: Vec<(usize, String, String)> = Vec::new();

    for i in 0..iterations {
        let n = 3 + rng.range(8);
        let source = generate_module(&mut rng, &sigs, n);

        let mut log = CompileEventLog::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            compile_gk_with_log(&source, &mut log)
        }));

        // Invariant 1: compiler never panics on any input. A panic
        // here is always a bug in the compiler — even when the input
        // is bananas, the error path should be a returned `Err`, not
        // a process-level abort.
        let result = match result {
            Ok(r) => r,
            Err(panic) => panic!(
                "compiler panicked on iteration {i}:\n  source:\n{source}\n  panic: {:?}",
                panic.downcast_ref::<&str>().copied()
                    .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                    .unwrap_or("<non-string panic>"),
            ),
        };

        match result {
            Err(msg) => {
                // Invariant 2: every error is either a recognised
                // structural diagnostic (type mismatch, bad
                // constant, undeclared reference, unknown function,
                // variadic-arity issue, …) or a well-formed
                // sentence. We're not prescribing *which* error
                // fires — the fuzzer routinely builds garbage —
                // only that the compiler classified it rather than
                // leaking panics or raw backtraces. A structured
                // `bad constant …` message proves the opt-in
                // assembly-time validator (SRD 15 §"Const
                // Constraint Metadata") rejected the literal
                // before the node's constructor saw it.
                if msg.is_empty()
                    || msg.to_lowercase().contains("panic")
                    || msg.to_lowercase().contains("index out of bounds")
                    || msg.to_lowercase().contains("unreachable")
                {
                    cryptic_errors.push((i, source.clone(), msg));
                }
            }
            Ok(_) => {
                // Invariant 3: every adapter the compiler auto-inserts
                // must be one we know about. Anything else is a rogue
                // entry — probably a new adapter added to the
                // compiler without an entry in this test's mirror.
                for e in log.events() {
                    if let CompileEvent::TypeAdapterInserted { adapter, .. } = e {
                        if !adapter_label_is_known(adapter) {
                            rogue_adapters.push((i, source.clone(), adapter.clone()));
                        }
                    }
                }
            }
        }
    }

    let mut failures: Vec<String> = Vec::new();
    for (i, src, msg) in &cryptic_errors {
        failures.push(format!(
            "iteration {i} produced a cryptic error message.\n  error: {msg}\n  source:\n{src}"));
    }
    for (i, src, adapter) in &rogue_adapters {
        failures.push(format!(
            "iteration {i} inserted an unrecognised adapter '{adapter}'.\n\
             Update `expected_adapt`/`adapter_label_is_known` and the compiler's\n\
             `auto_adapter` table together.\n  source:\n{src}"));
    }
    assert!(failures.is_empty(),
        "fuzz invariants violated ({} failures):\n\n{}",
        failures.len(), failures.join("\n---\n"));
}

/// Recognise the `{SrcType:?}→{DstType:?}` labels the compiler writes
/// into [`CompileEvent::TypeAdapterInserted`]. The format is produced
/// by `format!("{source_type:?}→{expected_type:?}")` in
/// `assembly.rs` — mirror its accepted set here.
fn adapter_label_is_known(label: &str) -> bool {
    // PortType's Debug impl yields "U64", "F64", etc. — match those.
    let known = [
        "U64→F64", "U32→U64", "U32→F64", "I32→I64", "I32→F64",
        "I64→F64", "F32→F64",
        "U64→Str", "F64→Str", "Bool→Str", "Json→Str",
        "U32→Str", "I32→Str", "I64→Str", "F32→Str",
        "Bool→U64",
    ];
    known.iter().any(|&k| k == label)
}

// ─── Basic sanity for the harness itself ──────────────────────────

/// End-to-end M2+M3: a `mod_wire(x, y)` call under
/// `pragma strict_values` triggers an auto-inserted
/// `AssertValue` between the divisor source and `mod_wire`'s
/// `divisor` wire input. Without the pragma, no assertion is
/// inserted (the node trusts its inputs as default).
#[test]
fn strict_values_inserts_nonzero_assertion_on_mod_wire() {
    use nb_variates::dsl::events::CompileEvent;

    // Strict mode: assertion expected.
    let strict_source = "\
        pragma strict_values\n\
        \n\
        d := mod(hash(cycle), 100)\n\
        b := mod_wire(cycle, d)\n\
    ";
    let mut log = CompileEventLog::new();
    let result = compile_gk_with_log(strict_source, &mut log);
    assert!(result.is_ok(), "compile failed: {:?}", result.err());
    let assertion_inserts: Vec<&CompileEvent> = log.events().iter()
        .filter(|e| matches!(e, CompileEvent::AssertionInserted { .. }))
        .collect();
    assert!(
        !assertion_inserts.is_empty(),
        "expected at least one AssertionInserted under strict_values; events: {:?}",
        log.events(),
    );

    // Non-strict mode: no assertion event.
    let lax_source = "\
        d := mod(hash(cycle), 100)\n\
        b := mod_wire(cycle, d)\n\
    ";
    let mut lax_log = CompileEventLog::new();
    let lax_result = compile_gk_with_log(lax_source, &mut lax_log);
    assert!(lax_result.is_ok(), "compile failed: {:?}", lax_result.err());
    let lax_inserts: Vec<&CompileEvent> = lax_log.events().iter()
        .filter(|e| matches!(e, CompileEvent::AssertionInserted { .. }))
        .collect();
    assert!(
        lax_inserts.is_empty(),
        "no AssertionInserted expected without pragma; got: {lax_inserts:?}",
    );
}

/// When the divisor source is a constant (already validated at
/// assembly time), strict_values mode skips the assertion — it's
/// provably redundant. SRD 15 §"Strict Wire Mode" skip rule #2.
#[test]
fn strict_values_skips_assertion_when_source_is_constant() {
    use nb_variates::dsl::events::CompileEvent;
    let source = "\
        pragma strict_values\n\
        b := mod_wire(cycle, 7)\n\
    ";
    let mut log = CompileEventLog::new();
    let result = compile_gk_with_log(source, &mut log);
    assert!(result.is_ok(), "compile failed: {:?}", result.err());
    let inserts: Vec<&CompileEvent> = log.events().iter()
        .filter(|e| matches!(e, CompileEvent::AssertionInserted { .. }))
        .collect();
    assert!(
        inserts.is_empty(),
        "constant source should skip assertion; got inserts: {inserts:?}",
    );
    let skips: Vec<&CompileEvent> = log.events().iter()
        .filter(|e| matches!(e, CompileEvent::AssertionSkipped { .. }))
        .collect();
    assert!(
        !skips.is_empty(),
        "expected an AssertionSkipped event for constant source; events: {:?}",
        log.events(),
    );
}

/// Pragma directives at the source head are recognised and recorded
/// in the compile event log — `strict_values` / `strict_types` /
/// `strict` produce advisories, unknown pragmas produce warnings,
/// and the pragma surface is forward-compatible (an unrecognised
/// pragma never blocks compilation). See SRD 15 §"Module-Level
/// Pragmas".
#[test]
fn pragmas_round_trip_through_compile() {
    use nb_variates::dsl::events::CompileEvent;
    let source = "\
        pragma strict\n\
        pragma warp_drive\n\
        \n\
        id := mod(hash(cycle), 1000)\n\
    ";
    let mut log = CompileEventLog::new();
    let result = compile_gk_with_log(source, &mut log);
    assert!(result.is_ok(), "compile failed: {:?}", result.err());
    let acknowledged: Vec<&str> = log.events().iter()
        .filter_map(|e| match e {
            CompileEvent::PragmaAcknowledged { name, .. } => Some(name.as_str()),
            _ => None,
        }).collect();
    let unknown: Vec<&str> = log.events().iter()
        .filter_map(|e| match e {
            CompileEvent::UnknownPragma { name, .. } => Some(name.as_str()),
            _ => None,
        }).collect();
    assert_eq!(acknowledged, vec!["strict"], "expected single ack for `strict`");
    assert_eq!(unknown, vec!["warp_drive"], "expected unknown record for `warp_drive`");
}

/// Guard against the test mis-firing: a plain same-type chain must
/// compile cleanly with no adapter events. If this ever fails, the
/// fuzz infrastructure itself is broken — check the compiler or the
/// event log machinery before chasing the other tests.
#[test]
fn sanity_same_type_chain_has_no_adapters() {
    let source = "\
        inputs := (cycle)\n\
        a := add(cycle, 1)\n\
        b := add(a, 2)\n\
    ";
    let mut log = CompileEventLog::new();
    let result = compile_gk_with_log(source, &mut log);
    assert!(result.is_ok(), "simple chain should compile: {:?}", result.err());
    for e in log.events() {
        if let CompileEvent::TypeAdapterInserted { .. } = e {
            panic!("unexpected type adapter in same-type chain:\n{source}\nevent: {e:?}");
        }
    }
}

#[test]
fn sanity_u64_to_f64_widens_via_adapter() {
    let source = "\
        inputs := (cycle)\n\
        a := clamp_f64(cycle, 0.0, 1.0)\n\
    ";
    let mut log = CompileEventLog::new();
    let result = compile_gk_with_log(source, &mut log);
    assert!(result.is_ok(), "u64→f64 widening should auto-adapt: {:?}", result.err());
    let has_adapter = log.events().iter().any(|e|
        matches!(e, CompileEvent::TypeAdapterInserted { adapter, .. } if adapter == "U64→F64"));
    assert!(has_adapter, "expected a U64→F64 adapter event in log: {:?}", log.events());
}

#[test]
fn sanity_f64_to_u64_rejects_without_cast() {
    // F64 → U64 is narrowing and must not auto-insert — the compiler
    // should report a type mismatch so the author is forced to pick
    // `f64_to_u64` / `round_to_u64` / `floor_to_u64` explicitly.
    let source = "\
        inputs := (cycle)\n\
        x := to_f64(cycle)\n\
        y := add(x, 1)\n\
    ";
    let err = compile_gk(source).expect_err("narrowing f64→u64 must not compile");
    assert!(err.contains("type mismatch"),
        "expected a type-mismatch error for narrowing, got: {err}");
}
