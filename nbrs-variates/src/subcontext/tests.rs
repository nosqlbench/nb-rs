// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Phase 1 surface tests for SRD-67. Verifies the typed builder
//! / spawn / release path coexists with the existing kernel API
//! and enforces the named-child registry contract.

use std::sync::Arc;

use crate::dsl::compile::compile_gk;
use crate::node::PortType;

use super::builder::SubcontextBuilder;
use super::error::{ContractViolation, SourceContext};
use super::kernel::{wrap_root_kernel, RootMarker, ScopeKernel};
use super::module::{BodyFragment, ScopeModule};
use super::name::ChildName;
use super::pull::{NamedPullConsumer, PullConsumer};
use super::spec::{ExportSpec, ImportSpec};
use crate::node::Value;

/// Build a parent kernel with a couple of exports for tests to
/// import against. `dataset` is a final-folded string; `cycle`
/// is the standard coordinate input.
fn parent_kernel() -> Arc<ScopeKernel<RootMarker>> {
    let kernel = compile_gk(
        "inputs := (cycle)\n\
         final dataset := \"sift1m\"\n\
         seed := hash(cycle)\n",
    )
    .expect("parent kernel compile");
    wrap_root_kernel(kernel, "test-root")
}

#[test]
fn subcontext_builder_yields_typed_builder() {
    let parent = parent_kernel();
    // Type witness: `subcontext_builder` returns
    // `SubcontextBuilder<RootMarker>` from
    // `Arc<ScopeKernel<RootMarker>>`.
    let _b: SubcontextBuilder<RootMarker> = parent.subcontext_builder();
}

#[test]
fn finalize_compiles_simple_gk_source_block() {
    let parent = parent_kernel();
    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("simple-gk-source"));
    b.body(BodyFragment::GkSource(
        "inputs := (cycle)\nx := 5\n".to_string(),
    ));
    let module = b.finalize().expect("finalize should succeed");
    assert!(module.program().output_names().iter().any(|n| *n == "x"));
    assert_eq!(module.context().label, "simple-gk-source");
}

#[test]
fn finalize_rejects_unbound_import() {
    let parent = parent_kernel();
    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("unbound-import"));
    // Body references `foo` which is not declared anywhere —
    // neither as a local binding nor as a declared import. The
    // GK compiler surfaces this as a wiring / resolution error,
    // which the builder wraps as `ContractViolation::Compile`.
    b.body(BodyFragment::GkSource(
        "inputs := (cycle)\nout := mul(cycle, foo)\n".to_string(),
    ));
    let err = b.finalize().expect_err("should fail to compile");
    match err {
        ContractViolation::Compile(msg) => {
            // The exact error wording is not load-bearing; the
            // load-bearing property is that an undeclared free
            // identifier surfaces as a `Compile` violation
            // rather than silently producing a kernel that
            // panics at runtime.
            assert!(!msg.is_empty(), "compile error message should not be empty");
        }
        other => panic!("expected Compile, got {other:?}"),
    }
}

#[test]
fn finalize_rejects_import_with_no_matching_parent_name() {
    // Direct test of Rule 1: the import names `nonexistent`,
    // which the parent doesn't export. Phase 1's name-presence
    // check fires before compilation.
    let parent = parent_kernel();
    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("rule1-direct"));
    b.import(ImportSpec::extern_("nonexistent", PortType::U64));
    b.body(BodyFragment::GkSource("inputs := (cycle)\nx := 5\n".to_string()));
    let err = b.finalize().expect_err("should reject");
    match err {
        ContractViolation::UnboundImport { import, .. } => {
            assert_eq!(import, "nonexistent");
        }
        other => panic!("expected UnboundImport, got {other:?}"),
    }
}

#[test]
fn spawn_records_named_child() {
    let parent = parent_kernel();
    let mut b = parent.clone().subcontext_builder();
    b.context(SourceContext::for_phase("p1"));
    b.body(BodyFragment::GkSource(
        "inputs := (cycle)\ny := mul(cycle, 2)\n".to_string(),
    ));
    let module: ScopeModule<_> = b.finalize().expect("finalize");

    let name = ChildName::phase("p1");
    let _child = parent
        .spawn(name.clone(), module)
        .expect("spawn should succeed");

    assert!(parent.has_child(&name), "parent registry should record `p1`");
}

#[test]
fn duplicate_spawn_errors() {
    let parent = parent_kernel();

    let module1 = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("dup"));
        b.body(BodyFragment::GkSource("inputs := (cycle)\na := 1\n".to_string()));
        b.finalize().expect("first finalize")
    };
    let module2 = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("dup-again"));
        b.body(BodyFragment::GkSource("inputs := (cycle)\nb := 2\n".to_string()));
        b.finalize().expect("second finalize")
    };

    let name = ChildName::phase("dup");
    parent.spawn(name.clone(), module1).expect("first spawn");
    let err = parent
        .spawn(name.clone(), module2)
        .expect_err("second spawn should fail");
    match err {
        ContractViolation::DuplicateChild {
            name: collided,
            prior_site,
            this_site,
        } => {
            assert_eq!(collided, name);
            assert_eq!(prior_site.label, "phase:dup");
            assert_eq!(this_site.label, "phase:dup-again");
        }
        other => panic!("expected DuplicateChild, got {other:?}"),
    }
}

#[test]
fn release_child_allows_respawn() {
    let parent = parent_kernel();

    let module1 = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("rel"));
        b.body(BodyFragment::GkSource("inputs := (cycle)\na := 1\n".to_string()));
        b.finalize().expect("first finalize")
    };
    let module2 = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("rel-again"));
        b.body(BodyFragment::GkSource("inputs := (cycle)\na := 1\n".to_string()));
        b.finalize().expect("second finalize")
    };

    let name = ChildName::phase("rel");
    parent.spawn(name.clone(), module1).expect("first spawn");
    parent.release_child(&name);
    assert!(!parent.has_child(&name));
    parent
        .spawn(name.clone(), module2)
        .expect("respawn after release should succeed");
    assert!(parent.has_child(&name));
}

#[test]
fn register_pull_persists_into_artifact() {
    let parent = parent_kernel();
    let mut b = parent.clone().subcontext_builder();
    b.context(SourceContext::for_phase("with-pulls"));

    let consumer: Arc<dyn PullConsumer> = Arc::new(NamedPullConsumer::new(
        "validation",
        ["seed".to_string(), "dataset".to_string()],
    ));
    b.register_pull(consumer);
    b.body(BodyFragment::GkSource(
        "inputs := (cycle)\nz := mul(cycle, 3)\n".to_string(),
    ));

    let module = b.finalize().expect("finalize");
    assert_eq!(module.consumers().len(), 1);
    assert_eq!(module.consumers()[0].label(), "validation");
    assert_eq!(
        module.consumers()[0].names(),
        &["seed".to_string(), "dataset".to_string()]
    );

    // The spawned kernel exposes the consumers — mirrors
    // `ScopeFixture::seal()` semantics: the activity-side
    // adapter pulls them off the spawned kernel at seal time.
    let name = ChildName::phase("with-pulls");
    let child = parent.spawn(name, module).expect("spawn");
    let consumers = child.consumers();
    assert_eq!(consumers.len(), 1);
    assert_eq!(
        consumers[0].names(),
        &["seed".to_string(), "dataset".to_string()]
    );
}

#[test]
fn body_fragment_statements_compile_to_program() {
    use crate::dsl::lexer;
    use crate::dsl::parser;

    let parent = parent_kernel();

    // Parse a snippet to obtain `Vec<Statement>` directly, then
    // submit via BodyFragment::Statements. This is the path
    // synthesisers will use in Phase 2+.
    let src = "inputs := (cycle)\nq := add(cycle, 7)\n";
    let tokens = lexer::lex(src).expect("lex");
    let file = parser::parse(tokens).expect("parse");

    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("statements-fragment"));
    b.body(BodyFragment::Statements(file.statements));

    let module = b.finalize().expect("finalize");
    assert!(module.program().output_names().iter().any(|n| *n == "q"));
}

// ---------------------------------------------------------------------------
// Phase 2 — Rule 2 (write-through rewrite for shared parent exports).
// ---------------------------------------------------------------------------

/// Build a parent kernel with a `shared <name> := <init>` export.
fn parent_with_shared_u64(name: &str, init: u64) -> Arc<ScopeKernel<RootMarker>> {
    let src = format!(
        "inputs := (cycle)\n\
         shared {name} := {init}\n",
    );
    let kernel = compile_gk(&src).expect("parent kernel compile");
    wrap_root_kernel(kernel, "test-root-shared")
}

#[test]
fn parent_shared_export_collision_rewrites_to_cell_write() {
    // Setup: parent has `shared X := 0`. Child body says
    // `X := 42`. The Phase 2 rewrite should:
    //   1. NOT raise FinalShadow / Phase2WriteThrough.
    //   2. Record a write-through binding on the artifact.
    //   3. After spawn + commit_write_throughs, the parent's
    //      cell carries `42`.
    let parent = parent_with_shared_u64("X", 0);

    let mut b = parent.clone().subcontext_builder();
    b.context(SourceContext::for_phase("rule2-rewrite"));
    b.export(ExportSpec::shared("X", crate::node::PortType::U64));
    b.body(BodyFragment::GkSource(
        "inputs := (cycle)\nX := 42\n".to_string(),
    ));

    let module = b.finalize().expect("finalize should succeed under Rule 2");
    let wts = module.write_throughs();
    assert_eq!(wts.len(), 1, "expected one write-through binding");
    assert_eq!(wts[0].export_name, "X");
    assert_eq!(wts[0].source_output, "__write_X");
    // The synthetic output is in the program.
    assert!(module
        .program()
        .output_names()
        .iter()
        .any(|n| *n == "__write_X"));
    // The export name surfaced as an input slot (so spawn's
    // bind_outer_scope can attach the parent's cell).
    assert!(module.program().find_input("X").is_some());

    // Spawn and commit. After commit, the parent should see X = 42.
    let name = ChildName::phase("rule2-rewrite");
    let child = parent.spawn(name, module).expect("spawn");
    assert_eq!(child.write_throughs().len(), 1);

    // Pre-commit: parent's cell still carries the literal init.
    assert_eq!(parent.lock_inner().lookup("X"), Some(Value::U64(0)));

    child.commit_write_throughs();

    // Post-commit: the parent's `lookup("X")` reads through the
    // shared cell (cell-aware) and surfaces `42`.
    assert_eq!(parent.lock_inner().lookup("X"), Some(Value::U64(42)));
}

#[test]
fn parent_shared_export_collision_propagates_through_siblings() {
    // Two children spawned under the same parent: one writes
    // via the rewrite, the other reads via standard import. The
    // reader should see the writer's value.
    let parent = parent_with_shared_u64("flag", 0);

    let writer_module = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("writer"));
        b.export(ExportSpec::shared("flag", crate::node::PortType::U64));
        b.body(BodyFragment::GkSource(
            "inputs := (cycle)\nflag := 7\n".to_string(),
        ));
        b.finalize().expect("writer finalize")
    };
    let reader_module = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("reader"));
        // Standard import — the reader expects parent to expose
        // `flag` as a shared cell-bound value.
        b.import(ImportSpec::shared("flag", crate::node::PortType::U64));
        b.body(BodyFragment::GkSource(
            "inputs := (cycle)\nextern flag: u64\nseen := flag\n".to_string(),
        ));
        b.finalize().expect("reader finalize")
    };

    let writer = parent
        .spawn(ChildName::phase("writer"), writer_module)
        .expect("writer spawn");
    let reader = parent
        .spawn(ChildName::phase("reader"), reader_module)
        .expect("reader spawn");

    // Writer fires; cell now carries 7. Reader observes it
    // via its own cell-bound input slot.
    writer.commit_write_throughs();

    // Reader's `flag` slot is bound to the same cell; pulling
    // `seen` resolves to the cell value.
    let seen = {
        let mut inner = reader.lock_inner();
        inner.pull("seen").clone()
    };
    assert_eq!(
        seen,
        Value::U64(7),
        "reader should observe writer's write through the shared cell"
    );
}

// ---------------------------------------------------------------------------
// Phase 3 — bridge extensions: `bind_program_under_parent` (rebind helper)
// and `CompileOptions` (lib paths / strict / required-output threading).
// ---------------------------------------------------------------------------

#[test]
fn bind_program_under_parent_rebinds_compiled_program() {
    // The rebind helper is the post-Phase-3 entry point for the
    // `from_program → bind_outer_scope` pair used by the phase-
    // scope cache-and-rebind path and the OpBuilder's per-op-
    // template instancing loop. Verify it produces a kernel
    // whose `lookup` resolves a parent constant — the same
    // behaviour the legacy two-call dance produced.
    let parent_kernel = compile_gk(
        "inputs := (cycle)\n\
         final n := 7\n",
    )
    .expect("parent compile");

    // Compile the child program standalone. The rebind helper
    // does NOT compile — it takes a pre-compiled `Arc<GkProgram>`.
    let child_kernel = compile_gk(
        "inputs := (cycle)\n\
         extern n: u64\n\
         passthrough := mul(n, 1)\n",
    )
    .expect("child compile");
    let program = child_kernel.program().clone();

    let mut bound = super::kernel::bind_program_under_parent(
        &parent_kernel,
        program,
        Vec::new(),
    );
    // After bind, the child's `n` extern reads through the
    // parent's folded constant. `pull` evaluates the output node.
    let v = bound.pull("passthrough").clone();
    assert_eq!(v.as_u64(), 7);
}

#[test]
fn build_kernel_under_parent_threads_compile_options() {
    // CompileOptions threads `gk_lib_paths` / `strict` /
    // `required_outputs` / `workload_dir` / `context_label`
    // through the bridge so synthesisers that previously called
    // `compile_gk_with_libs` directly produce byte-identical
    // kernels via the builder. Verify the bridge accepts a
    // non-default options struct and produces a working kernel.
    let parent_kernel = compile_gk("inputs := (cycle)\nfinal n := 5\n")
        .expect("parent compile");

    let opts = super::builder::CompileOptions {
        workload_dir: None,
        gk_lib_paths: Vec::new(),
        strict: false,
        required_outputs: Vec::new(),
        context_label: Some("phase-3-options-test".to_string()),
    };
    let (mut kernel, _write_throughs) = super::kernel::build_kernel_under_parent_with_options(
        &parent_kernel,
        "phase-3-options-test",
        "extern n: u64\ndoubled := mul(n, 2)\n".to_string(),
        Vec::new(),
        opts,
    )
    .expect("bridge with options");

    let v = kernel.pull("doubled").clone();
    assert_eq!(v.as_u64(), 10);
}

#[test]
fn parent_final_export_collision_still_errors() {
    // `final X := ...` on the parent + same-named child binding
    // is an immutable-export violation. Rule 2 routes shared
    // collisions but final collisions remain hard errors.
    let kernel = compile_gk(
        "inputs := (cycle)\n\
         final fixed := 42\n",
    )
    .expect("parent kernel compile");
    let parent = wrap_root_kernel(kernel, "test-root-final");

    let mut b = parent.clone().subcontext_builder();
    b.context(SourceContext::for_phase("final-shadow"));
    b.export(ExportSpec::local("fixed", crate::node::PortType::U64));
    b.body(BodyFragment::GkSource(
        "inputs := (cycle)\nfixed := 99\n".to_string(),
    ));
    let err = b.finalize().expect_err("final-shadow must error");
    match err {
        ContractViolation::FinalShadow { export, .. } => {
            assert_eq!(export, "fixed");
        }
        other => panic!("expected FinalShadow, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Phase 5 — `add_result_bindings` (SRD-66 result-wire kernel-driven path).
// ---------------------------------------------------------------------------

#[test]
fn add_result_bindings_injects_only_referenced_magic_externs() {
    // Closure-binding economy (SRD-67 Rule 5): only the magic
    // externs the source actually references get input slots.
    // Source references `body` only; `count` and `ok` slots
    // should be absent.
    let parent = parent_kernel();
    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("rb-closure"));
    b.body(BodyFragment::GkSource("inputs := (cycle)\n".to_string()));
    b.add_result_bindings("started_with_x := regex_match(body, \"^x\")\n")
        .expect("add_result_bindings");
    let module = b.finalize().expect("finalize");
    assert!(module.program().find_input("body").is_some(),
        "body input slot should be present");
    assert!(module.program().find_input("count").is_none(),
        "count slot should be absent (not referenced)");
    assert!(module.program().find_input("ok").is_none(),
        "ok slot should be absent (not referenced)");
    assert!(module.program().output_names().iter().any(|n| *n == "started_with_x"),
        "result LHS should surface as an output");
}

#[test]
fn add_result_bindings_rule2_writethrough_to_parent_shared() {
    // The motivating SRD-66 use case: workload-root has
    // `shared X := false`; result-bindings declare
    // `X := regex_match(body, "...")`. Rule 2 should rewrite
    // the binding into a write-through that propagates to the
    // parent's shared cell.
    let parent_src = "\
        inputs := (cycle)\n\
        shared count_seen := 0\n\
    ";
    let parent_kernel = compile_gk(parent_src).expect("parent compile");
    let parent = wrap_root_kernel(parent_kernel, "rb-rule2-root");

    let mut b = parent.clone().subcontext_builder();
    b.context(SourceContext::new("rb-rule2"));
    b.body(BodyFragment::GkSource("inputs := (cycle)\n".to_string()));
    // Use a numeric expression on a magic extern so the test
    // exercises both the closure-binding economy AND Rule 2.
    // RHS uses `count` which the magic-extern injector adds as
    // an extern slot; LHS `count_seen` collides with the parent
    // shared cell so Rule 2 rewrites the binding.
    b.add_result_bindings("count_seen := count\n")
        .expect("add_result_bindings");
    let module = b.finalize().expect("finalize");

    let wts = module.write_throughs();
    assert_eq!(wts.len(), 1, "expected one write-through binding");
    assert_eq!(wts[0].export_name, "count_seen");
    assert_eq!(wts[0].source_output, "__write_count_seen");
    assert!(module.program().find_input("count_seen").is_some(),
        "count_seen input slot should be present (cell-bound)");
    assert!(module.program().find_input("count").is_some(),
        "count magic-extern slot should be present (referenced)");
}

#[test]
fn add_result_bindings_rejects_reassignment_of_magic_wire() {
    // SRD-66 §"Schema": `body` / `count` / `ok` are runtime-
    // injected wires. Assigning to them in result-bindings is a
    // hard error.
    let parent = parent_kernel();
    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("rb-reassign"));
    b.body(BodyFragment::GkSource("inputs := (cycle)\n".to_string()));
    let err = match b.add_result_bindings("body := \"oops\"\n") {
        Ok(_) => panic!("reassigning body must error"),
        Err(e) => e,
    };
    match err {
        ContractViolation::Compile(msg) => {
            assert!(
                msg.contains("body") && msg.contains("runtime-injected"),
                "diagnostic should name the wire and the cause: {msg}"
            );
        }
        other => panic!("expected Compile, got {other:?}"),
    }
}

#[test]
fn add_result_bindings_empty_source_is_noop() {
    let parent = parent_kernel();
    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("rb-empty"));
    b.body(BodyFragment::GkSource("inputs := (cycle)\n".to_string()));
    b.add_result_bindings("").expect("empty source is a no-op");
    b.add_result_bindings("   \n   \n").expect("whitespace-only source is a no-op");
    let module = b.finalize().expect("finalize");
    assert!(module.program().find_input("body").is_none());
}
