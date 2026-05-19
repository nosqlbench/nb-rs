// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`SubcontextBuilder<P>`] — accumulator for module matter.
//!
//! Per SRD-67 §"Step 2 — Builder accumulates module matter": the
//! builder owns an `Arc<ScopeKernel<P>>` for the parent, records
//! imports / exports / body fragments / pull consumers, and at
//! `finalize` validates the import contract against the parent's
//! exports + compiles the body via the existing `compile_gk` /
//! `compile_ast` pipeline. The result is a closed
//! [`ScopeModule<Child<P>>`] artifact.

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use crate::dsl::ast::{Arg, CallExpr, Expr, ExternPort, GkFile, Statement};
use crate::dsl::compile::{compile_ast, compile_ast_with_libs};
use crate::dsl::lexer::{lex, Span};
use crate::dsl::parser::parse;
use crate::node::PortType;

use super::error::{ContractViolation, SourceContext};
use super::kernel::{Child, ScopeKernel};
use super::module::{BodyFragment, ScopeContract, ScopeModule, WriteThroughBinding};
use super::pull::{PullConsumer, RegisteredPullConsumer};
use super::spec::{ExportSpec, ImportSpec};

/// Prefix applied to the synthetic write-through output produced
/// by the Rule 2 rewrite. The child program emits this output as
/// a normal local computation; spawn pulls it per cycle and
/// fans the value through the parent's `SharedCell`.
const WRITE_THROUGH_PREFIX: &str = "__write_";

fn port_type_keyword(pt: PortType) -> &'static str {
    match pt {
        PortType::U64 | PortType::U32 => "u64",
        PortType::I64 | PortType::I32 => "i64",
        PortType::F64 | PortType::F32 => "f64",
        PortType::Bool => "bool",
        // Everything that isn't a numeric / bool maps to the
        // string keyword — matches the existing synthesiser
        // convention used by `build_do_loop_scope_kernel`.
        _ => "String",
    }
}

/// Optional compile-time configuration passed through to
/// [`compile_gk_with_libs`] when finalize compiles the body. When
/// every field is at its default, finalize falls back to the
/// minimal [`compile_ast`] path used by the do-loop bridge — no
/// behaviour change for the simplest synthesisers.
///
/// SRD-67 Phase 3 bridge hook: the for_each / op-template
/// synthesisers used to call `compile_gk_with_libs` directly with
/// `gk_lib_paths`, `workload_dir`, `strict`, and a context label.
/// Routing those concerns through the builder preserves byte-
/// identical compile output during migration.
#[derive(Clone, Debug, Default)]
pub struct CompileOptions {
    pub workload_dir: Option<PathBuf>,
    pub gk_lib_paths: Vec<PathBuf>,
    pub strict: bool,
    pub required_outputs: Vec<String>,
    pub context_label: Option<String>,
    pub cursor_limit: Option<u64>,
    /// Session-wide optimization level for op-template synthesis.
    /// `Release` (the default) lets the closure-binding economy
    /// DCE unreferenced slots; `Diagnostic` force-allocates every
    /// magic-extern and result-binding-LHS slot so step-debug /
    /// cycle-replay sees writes that the runtime would otherwise
    /// drop on the floor. See [`KernelOptLevel`].
    pub kernel_opt: crate::kernel::KernelOptLevel,
}

impl CompileOptions {
    fn is_default(&self) -> bool {
        self.workload_dir.is_none()
            && self.gk_lib_paths.is_empty()
            && !self.strict
            && self.required_outputs.is_empty()
            && self.context_label.is_none()
            && self.cursor_limit.is_none()
            && self.kernel_opt == crate::kernel::KernelOptLevel::default()
    }
}

/// Module-matter accumulator. Construction is gated by
/// [`ScopeKernel::subcontext_builder`] — the parent is the only
/// way in.
pub struct SubcontextBuilder<P> {
    parent: Arc<ScopeKernel<P>>,
    imports: Vec<ImportSpec>,
    exports: Vec<ExportSpec>,
    body: Vec<BodyFragment>,
    consumers: Vec<RegisteredPullConsumer>,
    context: SourceContext,
    /// Names to apply via `mark_inherited_outputs` on the
    /// compiled kernel before its program Arc is shared. Set by
    /// the legacy-synthesis bridge ([`super::build_kernel_under_parent`])
    /// to preserve the pre-SRD-67 ordering of cascade-extern
    /// names; explicit synthesisers that don't need cascade
    /// pass-through leave this empty.
    inherited_outputs: Vec<String>,
    /// Compile-time options forwarded into the AST compile. Empty
    /// for callers that don't need libs / strict / required-output
    /// filtering.
    compile_options: CompileOptions,
}

impl<P> SubcontextBuilder<P> {
    pub(crate) fn new(parent: Arc<ScopeKernel<P>>) -> Self {
        Self {
            parent,
            imports: Vec::new(),
            exports: Vec::new(),
            body: Vec::new(),
            consumers: Vec::new(),
            context: SourceContext::default(),
            inherited_outputs: Vec::new(),
            compile_options: CompileOptions::default(),
        }
    }

    /// SRD-67 Phase 3 bridge hook: route the legacy
    /// [`compile_gk_with_libs`] knobs (lib paths, strict mode,
    /// required-output filter, workload dir, context label)
    /// through the builder. Synthesisers that previously called
    /// `compile_gk_with_libs` directly fold those calls into a
    /// single `with_compile_options(...)` invocation; the do-loop
    /// bridge leaves this at its default and finalize uses
    /// [`compile_ast`].
    pub fn with_compile_options(&mut self, options: CompileOptions) -> &mut Self {
        self.compile_options = options;
        self
    }

    /// SRD-67 Phase 2 bridge hook: declare names whose outputs
    /// the body emits purely to cascade values from an outer
    /// scope to descendants (so they don't double up the parent's
    /// iter-coord, etc.). The compiled kernel will have these
    /// names flagged via `mark_inherited_outputs` before its
    /// program Arc is shared.
    ///
    /// Used by [`super::build_kernel_under_parent`] to migrate
    /// `build_do_loop_scope_kernel` and similar synthesisers
    /// without semantic drift; explicit-import callers leave this
    /// empty.
    pub fn mark_inherited_outputs(&mut self, names: Vec<String>) -> &mut Self {
        self.inherited_outputs = names;
        self
    }

    /// Borrow the parent kernel — used by tests / advanced
    /// callers that need to inspect parent state during build.
    pub fn parent(&self) -> &Arc<ScopeKernel<P>> {
        &self.parent
    }

    /// Declare an import.
    pub fn import(&mut self, spec: ImportSpec) -> &mut Self {
        self.imports.push(spec);
        self
    }

    /// Declare an export.
    pub fn export(&mut self, spec: ExportSpec) -> &mut Self {
        self.exports.push(spec);
        self
    }

    /// Append a body fragment. Multiple fragments are
    /// concatenated in registration order at finalize.
    pub fn body(&mut self, fragment: BodyFragment) -> &mut Self {
        self.body.push(fragment);
        self
    }

    /// Set the diagnostic context. Replaces any prior context.
    pub fn context(&mut self, ctx: SourceContext) -> &mut Self {
        self.context = ctx;
        self
    }

    /// Register a [`PullConsumer`]. Per SRD-67 §"Decision 7"
    /// this is the single init-time accumulator surface;
    /// SRD-32's `ScopeFixture::register_consumer` migrates to
    /// this entry point in Phase 2.
    pub fn register_pull(&mut self, consumer: Arc<dyn PullConsumer>) -> &mut Self {
        self.consumers.push(RegisteredPullConsumer::new(consumer));
        self
    }

    /// SRD-67 Phase 5 — fold a SRD-66 `result:` source block
    /// into this child's module matter. Single entry point for
    /// result-bindings kernel-driven path; applies the closure-
    /// binding economy (Rule 5) to magic externs and lets the
    /// existing finalize Rule 2 rewrite fire when result-LHS
    /// names collide with parent `shared` exports.
    ///
    /// `source` is GK source — the same `<name> := <expr>` form
    /// `bindings:` accepts. Both string-shape (`ResultSpec::String`)
    /// and map-shape (`ResultSpec::Map { name, source }` flattened
    /// to `<name> := <source>`) end up here.
    ///
    /// What this method does:
    ///
    /// 1. Parses `source` into `Vec<Statement>`.
    /// 2. Walks the body's free identifiers; for each magic
    ///    pre-bound name (`body`, `count`, `ok`) the source
    ///    references but doesn't already declare locally,
    ///    prepends an `extern <name>: <type>` declaration so
    ///    finalize compiles cleanly. Names not in the magic set
    ///    fall through to the standard import / cascade /
    ///    auto-extern path.
    /// 3. Records each `<name> := <expr>` LHS as an export, so
    ///    Rule 2 fires when the parent has a matching `shared`
    ///    export. The body fragment is appended; finalize's
    ///    existing rewrite is the load-bearing path.
    ///
    /// Path expressions (map-shape entries with no `:=` in the
    /// source) are NOT supported here — the caller flattens them
    /// to `<name> := <source>` and the GK compiler rejects them
    /// as unbound-identifier failures, surfacing the SRD-66
    /// "deferred until structural body wire lands" diagnostic.
    pub fn add_result_bindings(&mut self, source: &str) -> Result<&mut Self, ContractViolation> {
        let trimmed = source.trim();
        if trimmed.is_empty() {
            return Ok(self);
        }
        let tokens = lex(source).map_err(ContractViolation::Compile)?;
        let file = parse(tokens).map_err(ContractViolation::Compile)?;

        // Collect locally-declared names (LHS of `:=` and
        // `init <name> = ...` and `extern <name>` so the magic-
        // extern injector skips them). These are the result-wire
        // exports we'll declare to the parent for Rule 2.
        let mut local_decls: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut result_lhs: Vec<String> = Vec::new();
        for stmt in &file.statements {
            match stmt {
                Statement::Binding(b) => {
                    for t in &b.targets {
                        local_decls.insert(t.clone());
                        if !result_lhs.contains(t) {
                            result_lhs.push(t.clone());
                        }
                    }
                }
                Statement::ExternPort(ep) => {
                    local_decls.insert(ep.name.clone());
                }
                Statement::InputDecl(d) => {
                    local_decls.insert(d.name.clone());
                }
                _ => {}
            }
        }

        // Walk free identifiers across the body. Used for both
        // (a) magic-extern injection (Rule 5 closure-binding
        // economy — only what's referenced gets a slot) and
        // (b) hard-error detection for the SRD-66 "user-written
        // body :=" case.
        let mut free_idents: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for stmt in &file.statements {
            collect_free_idents(stmt, &mut free_idents);
        }

        // SRD-66 §"Strict-mode interactions" / §"Schema":
        // assigning to a pre-bound wire is a hard error. Catch
        // it before the magic-extern injector — otherwise the
        // injection would fight the LHS rename.
        for forbidden in ["body", "count", "ok"] {
            if result_lhs.iter().any(|n| n == forbidden) {
                return Err(ContractViolation::Compile(format!(
                    "result-bindings: '{forbidden}' is a runtime-injected wire and \
                     cannot be reassigned in `result:`. SRD-66 Surface 1 §Schema."
                )));
            }
        }

        // Magic-extern injection: only for names the source
        // actually references AND that aren't already declared
        // locally (the body might re-declare via `extern body`
        // explicitly — let that win).
        // SRD-66 §"Surface 4 §Open: body type" resolved to
        // `Value::Json` — body is a structural value the
        // workload assertively unwraps via `exactly_one_value`.
        // The Json shape preserves row × column structure so
        // shape-mismatch diagnostics can name actual
        // dimensions; for unary results, `exactly_one_value`
        // collapses to a `Str` carrier which downstream
        // string predicates (regex_match, etc.) consume.
        let magic_externs: &[(&str, PortType, &str)] = &[
            ("body", PortType::Json, "Json"),
            ("count", PortType::U64, "u64"),
            ("ok", PortType::Bool, "bool"),
        ];
        let span0 = Span { line: 0, col: 0 };
        let mut prepended: Vec<Statement> = Vec::new();
        // Magic-extern slot allocation. Release: only inject when
        // the result-binding RHS actually references the name (the
        // closure-binding economy's DCE). Diagnostic: force-allocate
        // every magic extern not already locally declared, so writes
        // for `body` / `count` / `ok` always have a kernel slot to
        // land in regardless of whether anything reads them. The
        // diagnostic mode is for step-debug / cycle-replay; the
        // unused slots have no eval cone and add a fixed handful of
        // bytes to per-op-template state.
        let force_all = self.compile_options.kernel_opt.keep_unreferenced_slots();
        for (name, _pt, type_kw) in magic_externs {
            let referenced = free_idents.contains(*name);
            let already_local = local_decls.contains(*name);
            if (force_all || referenced) && !already_local {
                prepended.push(Statement::ExternPort(ExternPort {
                    name: (*name).to_string(),
                    typ: (*type_kw).to_string(),
                    default: None,
                    span: span0,
                }));
            }
        }

        // Each result LHS may become a Rule 2 write-through when
        // the parent has a same-named `shared` cell visible in
        // scope. Without that match the binding stays a local
        // output and no export needs to be registered — the
        // result-LHS still becomes a kernel output through the
        // regular cycle-binding compile path, so wrappers /
        // metrics readers can still see it via wires.get.
        //
        // Conditioning registration on actual collision avoids
        // the U64-default port-type leak that used to surface
        // when a non-colliding LHS expression produced a non-u64
        // value (e.g. an f64 metric expression): the export
        // pre-allocated a u64 output port for the LHS and the
        // compiler hit a type mismatch wiring the f64 RHS
        // through it.
        {
            let in_scope_cells = self.parent.shared_cells_in_scope();
            let parent_shared_by_name: std::collections::HashMap<&str, PortType> = in_scope_cells
                .iter()
                .map(|c| (c.name.as_str(), c.port_type))
                .collect();
            for name in &result_lhs {
                if let Some(&pt) = parent_shared_by_name.get(name.as_str()) {
                    self.exports.push(ExportSpec::shared(name.clone(), pt));
                }
            }
        }

        // Compose the prepended externs with the user's
        // statements and submit as a single Statements fragment.
        // This sidesteps the source-string round-trip the
        // GkSource fragment shape would force when prepended
        // declarations need to lead the user's source.
        let mut combined: Vec<Statement> = prepended;
        combined.extend(file.statements);
        self.body.push(BodyFragment::Statements(combined));

        Ok(self)
    }

    /// Close the builder. Validates the import contract against
    /// the parent's exports, compiles the body, and seals the
    /// pull consumers into the artifact.
    ///
    /// SRD-67 Phase 2 — Rule 2 (write-through rewrite): when a
    /// child export name collides with a parent `shared` export,
    /// the body's `X := <expr>` is rewritten before compile to:
    ///
    /// 1. `extern X: <type>` — opens an input slot the parent's
    ///    `SharedCell` attaches to via `materialize_wiring_from_outer`.
    /// 2. `__write_X := <expr>` — a synthetic local computation
    ///    that produces the value to write through.
    ///
    /// At spawn time, the spawned child carries a write-through
    /// binding `(X, __write_X)`; per-cycle eval pulls
    /// `__write_X` and stores its value through the child's
    /// input slot for `X`, which propagates to the cell.
    pub fn finalize(self) -> Result<ScopeModule<Child<P>>, ContractViolation> {
        let SubcontextBuilder {
            parent,
            imports,
            exports,
            body,
            consumers,
            context,
            inherited_outputs,
            compile_options,
        } = self;

        let mut diagnostics: Vec<String> = Vec::new();

        // ----- Rule 1 — import resolution against parent
        // exports (Phase 1: name-presence check + name-only
        // closure validation; full type / modifier validation
        // is Phase 2 once the parent kernel exposes typed
        // export specs uniformly). -----
        let parent_inner = parent.lock_inner();
        let parent_outputs: std::collections::HashSet<String> = parent_inner
            .program()
            .output_names()
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let parent_inputs: std::collections::HashSet<String> = parent_inner
            .program()
            .input_names()
            .into_iter()
            .collect();

        for imp in &imports {
            if !parent_outputs.contains(&imp.name) && !parent_inputs.contains(&imp.name) {
                return Err(ContractViolation::UnboundImport {
                    import: imp.name.clone(),
                    site: context.clone(),
                });
            }
        }

        // ----- Rule 2 — export collision detection. -----
        // For each declared export, check the parent for a same-
        // named modifier:
        //
        // * `final` parent → `FinalShadow` error (immutable,
        //   can't be redefined).
        // * `shared` cell visible at parent → record the export
        //   as a write-through candidate. The kernel-synthesis
        //   rewrite below renames the child's binding LHS to
        //   `__write_<name>` and inserts an `extern <name>`
        //   declaration; spawn's typed cell-attach pass then
        //   wires the input slot to the parent's `SharedCell`.
        //
        //   "Visible at parent" walks the typed
        //   `shared_cells_in_scope()` enumeration so an ancestral
        //   `shared X` cell propagates transitively even when an
        //   intermediate scope's body never names X. Without
        //   this, Rule 2 silently no-ops for grand-children and
        //   their write-throughs go nowhere.
        //
        // * No parent export and no in-scope cell → child-only
        //   export, registered locally (no rewrite).
        drop(parent_inner);
        let in_scope_cells = parent.shared_cells_in_scope();
        let in_scope_cells_by_name: std::collections::HashMap<&str, &super::kernel::SharedCellInScope> = in_scope_cells
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        let parent_inner = parent.lock_inner();
        let mut write_through_specs: Vec<(String, PortType)> = Vec::new();
        for exp in &exports {
            let parent_modifier = parent_inner.program().output_modifier(&exp.name);
            if parent_modifier.is_const() && parent_outputs.contains(&exp.name) {
                return Err(ContractViolation::FinalShadow {
                    export: exp.name.clone(),
                    site: context.clone(),
                });
            }
            if let Some(in_scope) = in_scope_cells_by_name.get(exp.name.as_str()) {
                // Port type comes from the typed in-scope record
                // (sourced from the cell-bound input slot at the
                // owning ancestor). Authoritative; falls back to
                // the export spec's declared port type only if
                // the lookup somehow misses — never observed.
                write_through_specs.push((exp.name.clone(), in_scope.port_type));
            }
        }
        drop(parent_inner);

        // ----- Lower every body fragment into a single
        // Vec<Statement>. The Rule 2 rewrite operates on the AST
        // directly so it doesn't need a source-string round-trip;
        // GkSource fragments parse here once. -----
        if body.is_empty() {
            return Err(ContractViolation::Compile(
                "scope module body is empty — at least one fragment is required".into(),
            ));
        }
        let mut statements: Vec<Statement> = Vec::new();
        for fragment in &body {
            match fragment {
                BodyFragment::GkSource(src) => {
                    let tokens = lex(src).map_err(ContractViolation::Compile)?;
                    let file = parse(tokens).map_err(ContractViolation::Compile)?;
                    statements.extend(file.statements);
                }
                BodyFragment::Statements(stmts) => statements.extend(stmts.iter().cloned()),
            }
        }

        // ----- Apply Rule 2 rewrite over the statement vector. -----
        let mut write_throughs: Vec<WriteThroughBinding> = Vec::new();
        if !write_through_specs.is_empty() {
            let already_extern: std::collections::HashSet<String> = statements
                .iter()
                .filter_map(|s| match s {
                    Statement::ExternPort(p) => Some(p.name.clone()),
                    _ => None,
                })
                .collect();
            let span0 = Span { line: 0, col: 0 };

            // Inject `extern <name>: <type>` declarations for
            // every write-through that the child body doesn't
            // already extern. Prepend them so the input slot is
            // present before the compiler sees the renamed
            // binding.
            let mut prepended: Vec<Statement> = Vec::new();
            for (name, pt) in &write_through_specs {
                if already_extern.contains(name) {
                    continue;
                }
                prepended.push(Statement::ExternPort(ExternPort {
                    name: name.clone(),
                    typ: port_type_keyword(*pt).to_string(),
                    default: None,
                    span: span0,
                }));
            }
            // Rename single-target CycleBindings whose LHS
            // matches a write-through export. Multi-target
            // bindings (tuple unpacks) aren't valid for shared
            // write-through (a tuple has no single value to
            // store in the cell); leave them alone — they'll
            // surface as a duplicate-port compile error if the
            // collision is real. The single-target shape is the
            // SRD-66 motivating case.
            for stmt in statements.iter_mut() {
                if let Statement::Binding(b) = stmt
                    && b.targets.len() == 1
                {
                    let target = &b.targets[0];
                    if write_through_specs.iter().any(|(n, _)| n == target) {
                        let original = target.clone();
                        let renamed = format!("{WRITE_THROUGH_PREFIX}{original}");
                        b.targets[0] = renamed.clone();
                        write_throughs.push(WriteThroughBinding {
                            export_name: original,
                            source_output: renamed,
                        });
                    }
                }
            }
            // Splice the synthetic externs in front. Order:
            // [externs] ++ [original statements (with renamed
            // LHS)].
            prepended.extend(statements);
            statements = prepended;
        }

        // ----- Compile the rewritten AST. -----
        //
        // When `compile_options` carries non-default knobs (lib
        // paths, strict mode, required-output filter, source dir,
        // context label) we route through `compile_gk_with_libs`
        // so the same code path the for_each / op-template
        // synthesisers have always used handles them.
        // `compile_gk_with_libs` takes a source string; when the
        // caller supplies a single `GkSource` fragment that's the
        // raw input. If the body was AST-only (or fragments are
        // mixed) the source is re-emitted by concatenating
        // GkSource fragments — the existing synthesisers all
        // produce a single `GkSource(String)` body so this path
        // is the byte-identical replacement.
        //
        // If a Rule 2 write-through rewrite needs to fire AND
        // compile-options are set, the caller's source string
        // would no longer reflect the rewritten AST. None of the
        // current Phase 3 migration sites combine the two
        // (for_each / op-template / phase scopes don't collide
        // with `shared` parent exports). Reject the combination
        // explicitly so a future caller hits a clear diagnostic
        // rather than silently dropping the rewrite.
        let mut kernel = if compile_options.is_default() {
            compile_ast(&GkFile {
                statements: statements.clone(),
            })
            .map_err(ContractViolation::Compile)?
        } else if !write_throughs.is_empty()
            || body
                .iter()
                .any(|f| matches!(f, BodyFragment::Statements(_)))
        {
            // SRD-67 Phase 5 — when the AST has been rewritten in
            // place (Rule 2 write-through) OR the body was
            // submitted as `Statements` (no source-string
            // round-trip), feed the rewritten AST through the
            // libs-aware compile path directly. Avoids the prior
            // restriction that combined Rule 2 with non-default
            // compile options.
            let context_label = compile_options
                .context_label
                .as_deref()
                .unwrap_or(context.label.as_str());
            compile_ast_with_libs(
                &GkFile {
                    statements: statements.clone(),
                },
                compile_options.workload_dir.as_deref(),
                compile_options.gk_lib_paths.clone(),
                &compile_options.required_outputs,
                compile_options.strict,
                context_label,
            )
            .map_err(ContractViolation::Compile)?
        } else {
            // No rewrite, no Statements fragments — reconstruct
            // the source string and use the source-aware
            // `compile_gk_with_libs` so the legacy synthesiser
            // pathway preserves byte-identical output (the
            // compiler stashes `source_text` for diagnostics).
            let mut src = String::new();
            for fragment in &body {
                match fragment {
                    BodyFragment::GkSource(s) => {
                        src.push_str(s);
                        if !s.ends_with('\n') {
                            src.push('\n');
                        }
                    }
                    BodyFragment::Statements(_) => unreachable!(
                        "Statements fragments routed through compile_ast_with_libs above"
                    ),
                }
            }
            let context_label = compile_options
                .context_label
                .as_deref()
                .unwrap_or(context.label.as_str());
            crate::dsl::compile::compile_gk_with_libs_and_limit(
                &src,
                compile_options.workload_dir.as_deref(),
                compile_options.gk_lib_paths.clone(),
                &compile_options.required_outputs,
                compile_options.strict,
                context_label,
                compile_options.cursor_limit,
            )
            .map_err(ContractViolation::Compile)?
        };

        // ----- Apply legacy-bridge inherited-output marking.
        // Must happen before the program Arc is cloned out into
        // the artifact (mark_inherited_outputs requires unique
        // ownership of the program Arc).
        if !inherited_outputs.is_empty() {
            kernel.mark_inherited_outputs(inherited_outputs);
        }

        // ----- Bake Rule 2 write-throughs into the program. -----
        // The program is the single source of truth for these
        // bindings: any kernel built from this program (including
        // per-fiber re-instances via `bind_program_under_parent`)
        // will inherit them via `from_program`'s automatic seeding,
        // eliminating the side-channel that used to thread
        // write-throughs through the activity-layer scope tree.
        let kernel_write_throughs: Vec<crate::kernel::KernelWriteThrough> = write_throughs
            .iter()
            .map(|wt| crate::kernel::KernelWriteThrough {
                export_name: wt.export_name.clone(),
                source_output: wt.source_output.clone(),
            })
            .collect();
        if !kernel_write_throughs.is_empty() {
            kernel.bake_write_throughs(kernel_write_throughs);
        }

        // ----- Validate that every declared import shows up as
        // an input slot or a previously-folded constant on the
        // compiled program (Rule 5 — closure-binding economy
        // diagnostic; an unused import is a finalize-time
        // warning rather than an error). -----
        for imp in &imports {
            if kernel.program().find_input(&imp.name).is_none()
                && kernel.program().output_map_lookup(&imp.name).is_none()
            {
                diagnostics.push(format!(
                    "import `{}` declared but unused in body — Rule 5 closure-binding economy will drop it at spawn",
                    imp.name
                ));
            }
        }

        // ----- Validate Rule 2 invariants: the rewrite must
        // have produced (1) a child input slot for the export
        // name (so `materialize_wiring_from_outer` attaches the cell), and
        // (2) the synthetic `__write_<name>` output. -----
        for wt in &write_throughs {
            if kernel.program().find_input(&wt.export_name).is_none() {
                return Err(ContractViolation::Compile(format!(
                    "Rule 2 write-through rewrite for `{}` produced no input slot — \
                     check that the body's binding compiled to an input/output pair",
                    wt.export_name
                )));
            }
            if kernel.program().output_map_lookup(&wt.source_output).is_none() {
                return Err(ContractViolation::Compile(format!(
                    "Rule 2 write-through rewrite produced no `{}` output — \
                     the rewritten binding did not surface as a kernel output",
                    wt.source_output
                )));
            }
        }

        let program = kernel.program().clone();
        let contract = ScopeContract::from_specs(&imports, &exports);

        Ok(ScopeModule {
            imports,
            exports,
            program,
            contract,
            context,
            consumers,
            write_throughs,
            diagnostics,
            _module: PhantomData,
        })
    }
}

/// Collect free identifiers referenced by a statement's RHS. Used
/// by [`SubcontextBuilder::add_result_bindings`] to apply the
/// closure-binding economy (Rule 5) — only inject magic externs
/// (`body` / `count` / `ok`) the source actually references.
fn collect_free_idents(stmt: &Statement, out: &mut std::collections::HashSet<String>) {
    match stmt {
        Statement::Binding(b) => collect_expr_idents(&b.value, out),
        Statement::Cursor(c) => collect_expr_idents(&c.constructor, out),
        Statement::ModuleDef(_)
        | Statement::ExternPort(_)
        | Statement::InputDecl(_)
        | Statement::Pragma { .. } => {}
    }
}

fn collect_expr_idents(expr: &Expr, out: &mut std::collections::HashSet<String>) {
    match expr {
        Expr::Ident(name, _) => {
            out.insert(name.clone());
        }
        Expr::IntLit(_, _) | Expr::FloatLit(_, _) => {}
        Expr::StringLit(_, _) => {
            // String interpolation `{name}` references aren't
            // expanded at the AST level — they're resolved by
            // the compiler during desugaring. Conservatively skip
            // them for the magic-extern injector (the user's
            // body / count / ok can't appear inside an
            // interpolation in any current SRD-66 use case);
            // unresolved interpolations surface as standard
            // unbound-identifier diagnostics downstream.
        }
        Expr::ArrayLit(items, _) => {
            for e in items {
                collect_expr_idents(e, out);
            }
        }
        Expr::Call(call) => collect_call_idents(call, out),
        Expr::BinOp(a, _, b) => {
            collect_expr_idents(a, out);
            collect_expr_idents(b, out);
        }
        Expr::UnaryNeg(e, _) | Expr::UnaryBitNot(e, _) => collect_expr_idents(e, out),
        Expr::FieldAccess { source, .. } => {
            // Source-field projections reference a source name,
            // not a wire — but the magic-extern set is a closed
            // {body, count, ok}, so the only way `body.x` could
            // appear is the user wrote a structural body access.
            // Record `source` as a referenced ident so the
            // magic-extern check sees it; the GK compiler will
            // produce the canonical "field access on non-source"
            // diagnostic if it doesn't resolve.
            out.insert(source.clone());
        }
    }
}

fn collect_call_idents(call: &CallExpr, out: &mut std::collections::HashSet<String>) {
    for arg in &call.args {
        match arg {
            Arg::Positional(e) => collect_expr_idents(e, out),
            Arg::Named(_, e) => collect_expr_idents(e, out),
        }
    }
}
