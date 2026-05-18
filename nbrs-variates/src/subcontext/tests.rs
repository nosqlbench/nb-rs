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
        "input cycle: u64\n\
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
        "input cycle: u64\nx := 5\n".to_string(),
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
        "input cycle: u64\nout := mul(cycle, foo)\n".to_string(),
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
    b.body(BodyFragment::GkSource("input cycle: u64\nx := 5\n".to_string()));
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
        "input cycle: u64\ny := mul(cycle, 2)\n".to_string(),
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
        b.body(BodyFragment::GkSource("input cycle: u64\na := 1\n".to_string()));
        b.finalize().expect("first finalize")
    };
    let module2 = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("dup-again"));
        b.body(BodyFragment::GkSource("input cycle: u64\nb := 2\n".to_string()));
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
        b.body(BodyFragment::GkSource("input cycle: u64\na := 1\n".to_string()));
        b.finalize().expect("first finalize")
    };
    let module2 = {
        let mut b = parent.clone().subcontext_builder();
        b.context(SourceContext::for_phase("rel-again"));
        b.body(BodyFragment::GkSource("input cycle: u64\na := 1\n".to_string()));
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
        "input cycle: u64\nz := mul(cycle, 3)\n".to_string(),
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
    let src = "input cycle: u64\nq := add(cycle, 7)\n";
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
        "input cycle: u64\n\
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
        "input cycle: u64\nX := 42\n".to_string(),
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
    // materialize_wiring_from_outer can attach the parent's cell).
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
            "input cycle: u64\nflag := 7\n".to_string(),
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
            "input cycle: u64\nextern flag: u64\nseen := flag\n".to_string(),
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

/// Verifies the workload's exact regex pattern against
/// realistic CQL `DESCRIBE KEYSPACE system_views` text shapes.
///
/// CQL describe output for a keyspace has the form:
///
/// ```text
/// CREATE TABLE system_views.sai_column_indexes (
///     keyspace_name text,
///     ...
/// );
/// ```
///
/// or for virtual tables:
///
/// ```text
/// CREATE VIRTUAL TABLE system_views.indexes (
///     ...
/// );
/// ```
///
/// The workload's regex is:
///   `(?im)^\s*(VIRTUAL\s+)?TABLE\s+system_views\.<name>\s*\(`
///
/// This anchors at start-of-line with optional whitespace, then
/// optional `VIRTUAL `, then `TABLE`. It does NOT account for
/// the leading `CREATE\s+` that CQL's describe emits — so the
/// match silently fails against actual server output.
#[test]
fn workload_regex_does_not_match_actual_describe_keyspace_output() {
    use regex::Regex;

    let workload_pattern = r"(?im)^\s*(VIRTUAL\s+)?TABLE\s+system_views\.sai_column_indexes\s*\(";
    let re = Regex::new(workload_pattern).expect("regex compiles");

    // Realistic CQL output for a Cassandra cluster that DOES
    // have system_views.sai_column_indexes (post-5.0).
    let realistic_describe = "\
CREATE KEYSPACE system_views WITH replication = {'class': 'LocalStrategy'};

CREATE TABLE system_views.sai_column_indexes (
    keyspace_name text,
    table_name text,
    index_name text,
    column_name text,
    PRIMARY KEY ((keyspace_name), table_name, index_name)
);

CREATE VIRTUAL TABLE system_views.indexes (
    keyspace_name text,
    table_name text,
    index_name text,
    PRIMARY KEY ((keyspace_name), table_name, index_name)
);
";

    // The workload regex MISSES — it anchors at start-of-line
    // expecting `TABLE` (with optional VIRTUAL prefix), but the
    // actual emission is `CREATE TABLE` / `CREATE VIRTUAL TABLE`.
    assert!(
        !re.is_match(realistic_describe),
        "the workload regex was expected to MISS this realistic CQL describe \
         output (because it lacks the `CREATE\\s+` prefix); if this assertion \
         starts failing, the regex was fixed and this test should be inverted"
    );

    // A pattern that DOES match — the fix the workload needs.
    let fixed_pattern = r"(?im)^\s*(?:CREATE\s+)?(?:VIRTUAL\s+)?TABLE\s+system_views\.sai_column_indexes\s*\(";
    let fixed = Regex::new(fixed_pattern).expect("fixed regex compiles");
    assert!(
        fixed.is_match(realistic_describe),
        "the corrected regex (allowing optional `CREATE\\s+`) should match"
    );
}

/// CQL `DESCRIBE KEYSPACE <name>` returns multiple rows (one
/// per object — the keyspace itself, plus each table, view,
/// index, function, etc). Each row has columns like
/// `keyspace_name`, `type`, `name`, `create_statement`. So the
/// body shape is `Array([Object{...}, Object{...}, ...])` —
/// multi-row, multi-column.
///
/// The workload's `exactly_one_value(body)` requires a 1×1
/// unary shape. On a real describe-keyspace body it would
/// PANIC with a structural-shape diagnostic — not silently
/// produce `Bool(false)` cells. So if the workload error log
/// shows `Bool(false), Bool(false)` (cells at init) rather
/// than an `exactly_one_value` panic, EITHER the result-
/// binding kernel never evaluated for some reason, OR the
/// CQL adapter folded the multi-row response into a
/// non-rowset (empty? error?) and exactly_one_value silently
/// degraded.
///
/// Either way, the path the workload is taking with
/// `exactly_one_value(body)` over a `DESCRIBE KEYSPACE` body
/// is fragile. This test documents the shape expected by
/// `exactly_one_value` — calling code should know that
/// describe-keyspace bodies are multi-row.
#[test]
fn describe_keyspace_body_is_multirow_not_unary() {
    use crate::nodes::exactly_one::ExactlyOneValue;
    use crate::node::GkNode;

    let multi_row_body = Value::Json(std::sync::Arc::new(serde_json::json!([
        {"keyspace_name": "system_views", "type": "keyspace", "name": "system_views",
         "create_statement": "CREATE KEYSPACE system_views WITH ..."},
        {"keyspace_name": "system_views", "type": "table", "name": "sai_column_indexes",
         "create_statement": "CREATE TABLE system_views.sai_column_indexes (..."},
        {"keyspace_name": "system_views", "type": "table", "name": "indexes",
         "create_statement": "CREATE VIRTUAL TABLE system_views.indexes (..."},
    ])));

    let node = ExactlyOneValue::new();
    let mut out = [Value::None];
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        node.eval(&[multi_row_body], &mut out);
    }));
    assert!(
        result.is_err(),
        "exactly_one_value must reject a 3-row describe-keyspace body — \
         the workload would never see a meaningful schema_text from this shape"
    );
}

/// End-to-end emulation of the failing workload pattern in
/// `full_cql_vector.yaml`:
///
/// - Workload-root declares `shared has_match := false`.
/// - A detect-phase op-template, built via Source matter,
///   uses a result-binding that names `has_match` — Rule 2
///   detects the parent shared cell and rewrites to a
///   write-through.
/// - A per-fiber instance of the detect op-template is built
///   via Program matter (mirroring
///   `synthesis.rs::create_fiber_builder` which re-instances
///   the op-template kernel from the scope tree's cached
///   program).
/// - The fiber instance commits its write-through. The cell
///   should receive the new value.
/// - A consumer op-template (analog of `await_index`) reads
///   the cell via an `extern has_match: bool` slot. Both the
///   scope-tree-time canonical instance AND the per-fiber
///   instance must see the new value.
///
/// This test must reproduce ANY break in the cell-cascade /
/// write-through pipeline that affects the workload. If it
/// passes, the workload bug is in the activity layer's
/// orchestration, not in the kernel's subscope construction
/// or write-through commit.
#[test]
fn workload_emulation_shared_cell_through_op_template_chain() {
    // 1. Workload-root carrying the shared cell. Mirror of the
    //    `shared has_sai_column_indexes := false` declaration
    //    in the workload bindings block.
    let workload_canonical = compile_gk(
        "input cycle: u64\nshared has_match := false\n"
    ).expect("workload-root compile");

    // 2. Detect-phase op-template program built via Source
    //    matter. The result-binding's LHS `has_match` collides
    //    with the parent shared cell — Rule 2 rewrites the
    //    binding LHS to `__write_has_match` and synthesises
    //    `extern has_match: bool` on the child program.
    //    `cycle == cycle` evaluates to U64(1) (truthy).
    // The result-binding uses `regex_match(<input>, "...")` —
    // matches the workload's
    // `has_sai_column_indexes := log_info(regex_match(...))`
    // shape. regex_match produces Bool, the cell-declared type.
    let detect_matter = super::GkMatter::builder()
        .label("detect_op")
        .source("input cycle: u64\n".to_string())
        .result_bindings(
            "has_match := log_info(regex_match(\
             exactly_one_value(body), \"hello\"))\n"
        )
        .build()
        .expect("detect matter");
    let detect_canonical = workload_canonical
        .build_subscope(detect_matter)
        .expect("detect-canonical subscope");

    // 3. Per-fiber main kernel — analog of FiberBuilder's
    //    `main_kernel` field. Built as a subscope of the
    //    workload-canonical using its OWN program (state
    //    fork). Cells propagate via the cascade.
    let fiber_main_matter = super::GkMatter::builder()
        .program(workload_canonical.program().clone())
        .build()
        .expect("fiber main matter");
    let fiber_main = workload_canonical
        .build_subscope(fiber_main_matter)
        .expect("fiber main subscope");

    // 4. Per-fiber instance of the detect op-template, built
    //    as a subscope of the FIBER main kernel. Mirrors
    //    create_fiber_builder iterating
    //    `op_template_programs` and binding each under
    //    `fb.main_kernel`. Cell handles flow:
    //        workload_canonical → fiber_main → detect_fiber
    let detect_fiber_matter = super::GkMatter::builder()
        .program(detect_canonical.program().clone())
        .build()
        .expect("detect fiber matter");
    let mut detect_fiber = fiber_main
        .build_subscope(detect_fiber_matter)
        .expect("detect-fiber subscope");

    // 4. Feed the magic `body` extern with a JSON value whose
    //    leaf string matches the regex. Mirrors what the
    //    activity's ResultDispenser does at end-of-op.
    let body_idx = detect_fiber.program().find_input("body").expect("body slot");
    detect_fiber.state().set_input(
        body_idx,
        Value::Json(std::sync::Arc::new(serde_json::json!([{"col": "hello world"}]))),
    );

    // 5. Run the detect fiber's per-cycle write-through commit.
    //    Pulls `__write_has_match` (regex_match → Bool(true)),
    //    writes Bool through the cell-bound input slot for
    //    `has_match`. Single-register semantics: the cell IS
    //    the slot's register.
    detect_fiber.commit_write_throughs();

    // 5. Verify the cell observed the write — root reads via
    //    its own input slot, which is the same Arc<Mutex<…>>
    //    the detect fiber's slot was attached to.
    let root_value = workload_canonical.get_input("has_match");
    match root_value {
        Some(Value::U64(1)) | Some(Value::Bool(true)) => {} // expected
        other => panic!(
            "workload-root should observe the detect fiber's write through \
             the shared cell; got {other:?}",
        ),
    }

    // 6. Consumer op-template (analog of `await_index`). Built
    //    via Source matter; its `extern has_match: bool` slot
    //    receives the cell via cascade.
    let consumer_matter = super::GkMatter::builder()
        .label("consumer_op")
        .source(
            "input cycle: u64\nextern has_match: bool\nseen := has_match\n".to_string()
        )
        .build()
        .expect("consumer matter");
    let consumer_canonical = workload_canonical
        .build_subscope(consumer_matter)
        .expect("consumer-canonical subscope");

    // 7. Per-fiber consumer instance — under fiber_main, same
    //    chain as the detect fiber so the cell handle is
    //    shared end-to-end.
    let consumer_fiber_matter = super::GkMatter::builder()
        .program(consumer_canonical.program().clone())
        .build()
        .expect("consumer fiber matter");
    let mut consumer_fiber = fiber_main
        .build_subscope(consumer_fiber_matter)
        .expect("consumer-fiber subscope");

    // 8. Consumer fiber pulls `seen` (which reads the
    //    cell-bound input). Must observe the detect fiber's
    //    write — proves the cell handle is shared end-to-end
    //    through the canonical/fiber-instance fork.
    let seen = consumer_fiber.pull("seen").clone();
    match seen {
        Value::U64(1) | Value::Bool(true) => {} // expected
        other => panic!(
            "consumer fiber should see the detect fiber's cell write \
             (workload-emulation pattern); got {other:?}",
        ),
    }
}

#[test]
fn shared_bool_literal_init_does_not_leak_false_as_named_input() {
    // The workload `shared has_sai_column_indexes := false`
    // should produce one input slot per LHS and zero stray
    // input slots named `false` / `true`.
    //
    // The inferred-inputs pass (compile.rs) walks every
    // CycleBinding's RHS expression and adds any `Expr::Ident`
    // that isn't itself defined as a binding LHS. Bool
    // literals share the Ident encoding (no `BoolLit` AST
    // variant), so without an explicit filter they leaked as
    // input slots and surfaced in the runtime kernel-input
    // dump as `false=0`. Both with and without an explicit
    // `input cycle: u64` declaration is exercised — the
    // workload-root path takes the inferred branch.
    for src in [
        "input cycle: u64\n\
         shared has_sai_column_indexes := false\n\
         shared has_indexes := false\n",
        "shared has_sai_column_indexes := false\n\
         shared has_indexes := true\n",
    ] {
        let kernel = compile_gk(src).expect("compile");
        let inputs = kernel.program().input_names();
        assert!(
            !inputs.iter().any(|n| n == "false" || n == "true"),
            "boolean literal leaked as a named input slot; got inputs {inputs:?}"
        );
        assert!(inputs.iter().any(|n| n == "has_sai_column_indexes"));
        assert!(inputs.iter().any(|n| n == "has_indexes"));
    }
}

#[test]
fn log_info_preserves_bool_type_through_result_binding_cell() {
    // SRD-66 canonical probe-phase shape:
    //   has_sai_column_indexes := log_info(regex_match(text, "..."))
    //
    // `regex_match` produces Bool; the cell on workload-root is
    // declared Bool. Without log_* in the assembler's
    // skip-type-check list, the wire regex_match → log_info
    // mismatched (Bool source vs declared-Str input) and
    // auto_adapter inserted a Bool→Str converter — corrupting
    // the value to Str("false") in transit. The cell received
    // Str, downstream `pick` rejected it as non-bool.
    //
    // Post-fix: log_* is type-polymorphic at the assembler;
    // the Bool flows through unchanged and lands in the cell.
    use super::CompileOptions;

    let root = compile_gk(
        "input cycle: u64\nshared has_match := false\n"
    ).expect("root compile");

    // Phase scope (silent intermediate — body never names has_match).
    let phase_program = compile_gk(
        "input cycle: u64\nlocal := cycle\n"
    ).expect("phase compile").program().clone();
    let phase_kernel = root.materialize_subscope(phase_program, &[]);

    // Op-template: result-binding wraps regex_match with log_info.
    let opts = CompileOptions {
        workload_dir: None,
        gk_lib_paths: Vec::new(),
        strict: false,
        required_outputs: Vec::new(),
        context_label: Some("op-template".to_string()),
        cursor_limit: None,
        ..Default::default()
    };
    let body = "input cycle: u64\n".to_string();
    // The body magic-extern is auto-injected by
    // add_result_bindings; exactly_one_value walks its
    // structural shape. The schema_text wire feeds regex_match.
    let result_bindings = "schema_text := exactly_one_value(body)\nhas_match := log_info(regex_match(schema_text, \"hello\"))\n";
    let matter = super::GkMatter::builder()
        .label("op-template")
        .source(body)
        .result_bindings(result_bindings)
        .options(opts)
        .build()
        .expect("matter build");
    let kernel = phase_kernel.build_subscope(matter).expect("op-template build");

    let mut kernel = kernel;
    // Feed the body magic-extern with a unary JSON value whose
    // leaf string matches the regex. This drives the same data
    // path the live workload's `describe_system_views` op uses.
    let body_idx = kernel.program().find_input("body").expect("body slot");
    kernel.state().set_input(
        body_idx,
        Value::Json(std::sync::Arc::new(serde_json::json!([{"col": "hello world"}]))),
    );
    kernel.commit_write_throughs();

    // The cell value must be Bool, not Str — the type the
    // shared declaration uses and the type downstream `pick`
    // selectors require.
    match root.lookup("has_match") {
        Some(Value::Bool(true)) => {} // expected
        other => panic!(
            "expected Bool(true) in cell after regex_match wrapped in log_info, got {other:?}"
        ),
    }
}

#[test]
fn build_kernel_under_parent_full_sees_live_parents_cells() {
    // Exercises the activity layer's actual op-template
    // construction path (`build_op_template_scope_kernel` →
    // `build_kernel_under_parent_full`). The bridge wraps the
    // live parent into a transient `ScopeKernel<RootMarker>`
    // for the typed builder; that transient MUST mirror the
    // live parent's cell view, not just its program shape, or
    // Rule 2 in finalize sees zero cells and produces zero
    // write-throughs.
    //
    // Pre-fix: transient was `from_program(parent.program())`
    // — fresh state, no cells from inherited transit. The leaf
    // kernel got `write_throughs.len() == 0`,
    // `commit_write_throughs` was a no-op, the result-binding
    // nodes never evaluated, the cell stayed at its init
    // value, downstream `pick(...)` panicked with both
    // selectors `Bool(false)`.
    //
    // Post-fix: `snapshot_with_cells` clones the parent's
    // program AND attaches its cell handles, so the transient
    // observes the same cells. Rule 2 fires; write-throughs
    // are populated; per-cycle commit fans values back through
    // the cell.
    use super::CompileOptions;

    // Workload root with a `shared` cell.
    let root = compile_gk(
        "input cycle: u64\nshared has_sai_column_indexes := false\n"
    ).expect("root compile");

    // Phase scope built under root via the typed subscope path
    // — the canonical activity-layer path. Phase body never
    // names the shared wire; the cell rides as transit.
    let phase_program = compile_gk(
        "input cycle: u64\nlocal := cycle\n"
    ).expect("phase compile").program().clone();
    let phase_kernel = root.materialize_subscope(phase_program, &[]);

    // Op-template kernel built under the phase via the typed
    // subscope-from-source method, with a result-binding source
    // that writes through the shared cell.
    let opts = CompileOptions {
        workload_dir: None,
        gk_lib_paths: Vec::new(),
        strict: false,
        required_outputs: Vec::new(),
        context_label: Some("op-template".to_string()),
        cursor_limit: None,
        ..Default::default()
    };
    let body = "input cycle: u64\n".to_string();
    // RHS uses a comparison so the value is unambiguously a
    // Bool produced by a node (compile_binding's expression
    // path doesn't recognise bare `true`/`false` literals on
    // the RHS — they're only special-cased in
    // try_fold_shared_init / evaluate_default_expr).
    let result_bindings = "has_sai_column_indexes := cycle == cycle\n";
    let matter = super::GkMatter::builder()
        .label("op-template")
        .source(body)
        .result_bindings(result_bindings)
        .options(opts)
        .build()
        .expect("matter build");
    let mut kernel = phase_kernel.build_subscope(matter).expect("op-template build");

    // Per-cycle commit propagates the result-binding's value
    // back through the cell. Root observes the write — proves
    // Rule 2 saw the transitively-inherited shared cell at
    // finalize and baked the write-through onto the program.
    kernel.commit_write_throughs();
    // GK comparison ops produce U64(1) for true; the cell-
    // bound input slot snapshots whatever the source pulled.
    // The assertion is "cell observed a non-init write" — the
    // shape (U64 vs Bool) is incidental to this test.
    let observed = root.lookup("has_sai_column_indexes")
        .expect("root cell should be set");
    assert!(
        !matches!(observed, Value::Bool(false)),
        "root should observe a write-through (cell still at init): {observed:?}",
    );
}

#[test]
fn shared_cell_cascade_survives_for_iteration_through_silent_intermediates() {
    // Mirrors the workload-of-record structure:
    //   workload-root  shared flag := 0
    //     ↓ (for_iteration)
    //   scenario       (body never names flag)
    //     ↓ (bind_program_under_parent)
    //   for_each       (body never names flag — iter-var only)
    //     ↓ (bind_program_under_parent)
    //   phase op       writes __write_flag → flag
    //
    // Pre-fix, the cell would be lost at the first silent
    // intermediate. Post-fix, every layer's `materialize_wiring_from_outer`
    // forwards the cell as transit even when no slot exists,
    // so the leaf phase's slot picks it up via the cascade.
    use crate::kernel::GkKernel;
    use std::sync::Arc;

    let root_kernel = compile_gk(
        "input cycle: u64\nshared flag := 0\n"
    ).expect("root compile");

    // Scenario: synthesised via for_iteration (the comprehension
    // code path).
    let scenario_canon = compile_gk(
        "input cycle: u64\nlocal := cycle\n"
    ).expect("scenario compile");
    let scenario = GkKernel::for_iteration(
        &Arc::new(scenario_canon),
        &Arc::new(root_kernel),
        &[],
    );

    // for_each scope: iter-var only.
    let foreach_program = compile_gk(
        "input cycle: u64\nextern profile: String\n"
    ).expect("for_each compile").program().clone();
    let mut foreach = scenario.materialize_subscope(foreach_program, &[]);
    let pidx = foreach.program().find_input("profile").expect("profile slot");
    foreach.state().set_input(pidx, Value::Str("p0".into()));

    // Phase op: writes through the shared cell.
    let leaf_program = compile_gk(
        "input cycle: u64\n\
         extern flag: u64\n\
         __write_flag := 7\n"
    ).expect("leaf compile").program().clone();
    let mut leaf = foreach.materialize_subscope(leaf_program, &[]);
    leaf.set_write_throughs(vec![crate::kernel::KernelWriteThrough {
        export_name: "flag".to_string(),
        source_output: "__write_flag".to_string(),
    }]);
    leaf.commit_write_throughs();

    assert_eq!(
        leaf.get_input("flag"),
        Some(Value::U64(7)),
        "leaf's cell-bound input should reflect its own write through the cell",
    );

    // Sibling readback: a second subscope attached to the same
    // chain should observe the write too. This proves the
    // cell handle (not just the local snapshot) carried the
    // value.
    let reader_program = compile_gk(
        "input cycle: u64\nextern flag: u64\nseen := flag\n"
    ).expect("reader compile").program().clone();
    let mut reader = foreach.materialize_subscope(reader_program, &[]);
    let seen = reader.pull("seen").clone();
    assert_eq!(seen, Value::U64(7), "sibling reader should observe write through cell");
}

#[test]
fn shared_cell_cascade_survives_legacy_bind_program_under_parent_chain() {
    // Activity-layer integration shape: phase / scenario /
    // for_each kernels are NOT built through the typed
    // `ScopeKernel::spawn` — they go through the
    // `bind_program_under_parent` bridge (`from_program +
    // materialize_wiring_from_outer`). This test asserts the cascade
    // survives that bridge end-to-end.
    //
    // Pre-fix, `materialize_wiring_from_outer` walked only outer's
    // `output_names()` and missed any cell whose name wasn't
    // re-declared as an output on every intermediate scope.
    // Post-fix, `materialize_wiring_from_outer` itself owns the typed
    // cell-cascade primitive (`shared_cells_in_scope` walks
    // input slots + transit cells), so the activity layer's
    // existing call sites pick the fix up automatically.
    // Workload root: `shared X := <literal>`.
    let root_kernel = compile_gk(
        "input cycle: u64\nshared counter := 0\n"
    ).expect("root compile");

    // Scenario kernel: body never names `counter`. Built via
    // the typed subscope path — exactly how the activity layer
    // builds phase / scenario / for_each kernels.
    let mid_program = compile_gk("input cycle: u64\nlocal := cycle\n")
        .expect("mid compile")
        .program()
        .clone();
    let mid = root_kernel.materialize_subscope(mid_program, &[]);

    // Op-template kernel: body writes to `counter` via the
    // Rule-2-equivalent shape (`extern counter: u64` + write
    // through). The cell cascade must reach this kernel for
    // the write to land.
    let leaf_program = compile_gk(
        "input cycle: u64\n\
         extern counter: u64\n\
         __write_counter := 42\n"
    ).expect("leaf compile").program().clone();
    let mut leaf = mid.materialize_subscope(leaf_program, &[]);

    // Verify the leaf's `counter` input slot is wired to
    // root's shared cell — via two layers of
    // `materialize_wiring_from_outer` with mid acting as a transit
    // station for the cell.
    leaf.set_write_throughs(vec![crate::kernel::KernelWriteThrough {
        export_name: "counter".to_string(),
        source_output: "__write_counter".to_string(),
    }]);
    leaf.commit_write_throughs();

    assert_eq!(
        root_kernel.lookup("counter"),
        Some(Value::U64(42)),
        "root should observe leaf's write through the transit-cell cascade",
    );
}

#[test]
fn parent_shared_cell_cascades_to_grandchild_through_silent_intermediate() {
    // Three-level chain: root has `shared flag := 0`. The mid
    // scope's body never references `flag` (closure-binding
    // economy — no input slot for it on mid). The leaf scope
    // writes `flag := 9`.
    //
    // Pre-fix bug: spawn's cell-attach iterated the immediate
    // parent's `output_names`. Mid's program has no `flag`
    // output → leaf's `extern flag` slot got no cell → Rule 2
    // write-through silently no-op'd → root never saw the write.
    //
    // Post-fix: `parent.shared_cells_in_scope()` walks every
    // input slot with an attached cell, exposing root's cell to
    // every descendant regardless of intermediate body content.
    let root = parent_with_shared_u64("flag", 0);

    // Mid scope: body declares no use of `flag`. Spawn it as
    // a child of root.
    let mid_module = {
        let mut b = root.clone().subcontext_builder();
        b.context(SourceContext::for_phase("mid"));
        b.body(BodyFragment::GkSource(
            "input cycle: u64\nlocal := cycle\n".to_string(),
        ));
        b.finalize().expect("mid finalize")
    };
    let mid = Arc::new(
        root.spawn(ChildName::phase("mid"), mid_module)
            .expect("mid spawn"),
    );

    // Leaf scope: writes to `flag`. Spawn under mid.
    let leaf_module = {
        let mut b = mid.clone().subcontext_builder();
        b.context(SourceContext::for_phase("leaf"));
        b.export(ExportSpec::shared("flag", crate::node::PortType::U64));
        b.body(BodyFragment::GkSource(
            "input cycle: u64\nflag := 9\n".to_string(),
        ));
        b.finalize().expect("leaf finalize — Rule 2 must see root's cell through mid")
    };
    let leaf = mid
        .spawn(ChildName::phase("leaf"), leaf_module)
        .expect("leaf spawn");

    // Sanity: leaf carries one write-through (the rewrite saw
    // root's cell while looking up exports under mid).
    assert_eq!(
        leaf.write_throughs().len(),
        1,
        "leaf should carry the Rule 2 write-through bound to root's transitive cell"
    );

    // Pre-commit: root's cell still carries the literal init.
    assert_eq!(root.lock_inner().lookup("flag"), Some(Value::U64(0)));

    leaf.commit_write_throughs();

    // Post-commit: leaf wrote through the cell handle that
    // ultimately lives at root. Root observes the value.
    assert_eq!(
        root.lock_inner().lookup("flag"),
        Some(Value::U64(9)),
        "root should observe leaf's write through the transitive shared cell"
    );
}

// ---------------------------------------------------------------------------
// Phase 3 — bridge extensions: `bind_program_under_parent` (rebind helper)
// and `CompileOptions` (lib paths / strict / required-output threading).
// ---------------------------------------------------------------------------

#[test]
fn bind_program_under_parent_rebinds_compiled_program() {
    // The rebind helper is the post-Phase-3 entry point for the
    // `from_program → materialize_wiring_from_outer` pair used by the phase-
    // scope cache-and-rebind path and the OpBuilder's per-op-
    // template instancing loop. Verify it produces a kernel
    // whose `lookup` resolves a parent constant — the same
    // behaviour the legacy two-call dance produced.
    let parent_kernel = compile_gk(
        "input cycle: u64\n\
         final n := 7\n",
    )
    .expect("parent compile");

    // Compile the child program standalone. The rebind helper
    // does NOT compile — it takes a pre-compiled `Arc<GkProgram>`.
    let child_kernel = compile_gk(
        "input cycle: u64\n\
         extern n: u64\n\
         passthrough := mul(n, 1)\n",
    )
    .expect("child compile");
    let program = child_kernel.program().clone();

    let mut bound = parent_kernel.materialize_subscope(program, &[]);
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
    let parent_kernel = compile_gk("input cycle: u64\nfinal n := 5\n")
        .expect("parent compile");

    let opts = super::builder::CompileOptions {
        workload_dir: None,
        gk_lib_paths: Vec::new(),
        strict: false,
        required_outputs: Vec::new(),
        context_label: Some("phase-3-options-test".to_string()),
        cursor_limit: None,
        ..Default::default()
    };
    let matter = super::GkMatter::builder()
        .label("phase-3-options-test")
        .source("extern n: u64\ndoubled := mul(n, 2)\n")
        .options(opts)
        .build()
        .expect("matter build");
    let mut kernel = parent_kernel
        .build_subscope(matter)
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
        "input cycle: u64\n\
         final fixed := 42\n",
    )
    .expect("parent kernel compile");
    let parent = wrap_root_kernel(kernel, "test-root-final");

    let mut b = parent.clone().subcontext_builder();
    b.context(SourceContext::for_phase("final-shadow"));
    b.export(ExportSpec::local("fixed", crate::node::PortType::U64));
    b.body(BodyFragment::GkSource(
        "input cycle: u64\nfixed := 99\n".to_string(),
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
    b.body(BodyFragment::GkSource("input cycle: u64\n".to_string()));
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
fn add_result_bindings_diagnostic_force_allocates_unreferenced_magic_externs() {
    // Diagnostic opt level relaxes the closure-binding economy:
    // every magic extern (`body` / `count` / `ok`) gets a slot
    // regardless of whether the source references it, so step-
    // debug / cycle-replay can show the operator the values the
    // runtime would otherwise drop. Same source as the
    // closure-economy test above, but `count` and `ok` slots
    // are now present.
    let parent = parent_kernel();
    let mut b = parent.subcontext_builder();
    b.context(SourceContext::new("rb-diagnostic"));
    b.with_compile_options(super::CompileOptions {
        kernel_opt: crate::kernel::KernelOptLevel::Diagnostic,
        ..Default::default()
    });
    b.body(BodyFragment::GkSource("input cycle: u64\n".to_string()));
    b.add_result_bindings("started_with_x := regex_match(body, \"^x\")\n")
        .expect("add_result_bindings");
    let module = b.finalize().expect("finalize");
    assert!(module.program().find_input("body").is_some(),
        "body slot present (referenced)");
    assert!(module.program().find_input("count").is_some(),
        "count slot present under Diagnostic (force-allocated)");
    assert!(module.program().find_input("ok").is_some(),
        "ok slot present under Diagnostic (force-allocated)");
}

#[test]
fn add_result_bindings_rule2_writethrough_to_parent_shared() {
    // The motivating SRD-66 use case: workload-root has
    // `shared X := false`; result-bindings declare
    // `X := regex_match(body, "...")`. Rule 2 should rewrite
    // the binding into a write-through that propagates to the
    // parent's shared cell.
    let parent_src = "\
        input cycle: u64\n\
        shared count_seen := 0\n\
    ";
    let parent_kernel = compile_gk(parent_src).expect("parent compile");
    let parent = wrap_root_kernel(parent_kernel, "rb-rule2-root");

    let mut b = parent.clone().subcontext_builder();
    b.context(SourceContext::new("rb-rule2"));
    b.body(BodyFragment::GkSource("input cycle: u64\n".to_string()));
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
    b.body(BodyFragment::GkSource("input cycle: u64\n".to_string()));
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
    b.body(BodyFragment::GkSource("input cycle: u64\n".to_string()));
    b.add_result_bindings("").expect("empty source is a no-op");
    b.add_result_bindings("   \n   \n").expect("whitespace-only source is a no-op");
    let module = b.finalize().expect("finalize");
    assert!(module.program().find_input("body").is_none());
}
