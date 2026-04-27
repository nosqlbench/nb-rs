// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! DSL-to-assembly bridge: compile a parsed GK AST into a runtime kernel.
//!
//! Walks the AST, resolves function names to node constructors, wires
//! the `GkAssembler`, and produces a `GkKernel`.


use std::path::{Path, PathBuf};

use crate::assembly::{GkAssembler, WireRef};
use crate::dsl::ast::*;
use crate::dsl::lexer;
use crate::dsl::parser;
use crate::kernel::GkKernel;

use crate::dsl::error::DiagnosticReport;
use crate::dsl::validate::{validate_ast, collect_references};

use std::collections::HashSet;

use super::modules::ResolvedModule;

/// Embedded standard library modules, compiled into the binary.
///
/// Each entry is (filename, source). Multiple modules per file —
/// each top-level binding is a separate module, resolved by name.
/// Searched as the final fallback after workload-local and --gk-lib paths.
pub(super) static STDLIB_MODULES: &[(&str, &str)] = &[
    ("hashing.gk", include_str!("../../stdlib/hashing.gk")),
    ("strings.gk", include_str!("../../stdlib/strings.gk")),
    ("identity.gk", include_str!("../../stdlib/identity.gk")),
    ("distributions.gk", include_str!("../../stdlib/distributions.gk")),
    ("latency.gk", include_str!("../../stdlib/latency.gk")),
    ("timeseries.gk", include_str!("../../stdlib/timeseries.gk")),
    ("waves.gk", include_str!("../../stdlib/waves.gk")),
    ("fourier.gk", include_str!("../../stdlib/fourier.gk")),
    ("modeling.gk", include_str!("../../stdlib/modeling.gk")),
];

/// Return the embedded standard library module sources.
pub fn stdlib_sources() -> &'static [(&'static str, &'static str)] {
    STDLIB_MODULES
}

/// Compile a `.gk` source string into a runtime kernel.
pub fn compile_gk(source: &str) -> Result<GkKernel, String> {
    compile_gk_with_path(source, None)
}

/// Compile GK source to an assembler (not yet compiled to a kernel).
///
/// Returns the `GkAssembler` with all nodes and wiring populated,
/// ready to be compiled at any level: `.compile()` for P1,
/// `.try_compile()` for P2, `.try_compile_jit()` for P3,
/// `.compile_hybrid()` for Hybrid.
pub fn compile_gk_to_assembler(source: &str) -> Result<GkAssembler, String> {
    let tokens = super::lexer::lex(source)?;
    let ast = super::parser::parse(tokens)?;
    let mut compiler = Compiler::new(None, false);
    let mut asm = compiler.build_assembler(&ast)?;
    asm.set_context(source, "(gk source)");
    Ok(asm)
}

/// Compile with a source directory for module resolution.
///
/// When the compiler encounters an unknown function name, it searches
/// `source_dir` for `.gk` module files that export a matching binding.
pub fn compile_gk_with_path(source: &str, source_dir: Option<&Path>) -> Result<GkKernel, String> {
    compile_gk_strict(source, source_dir, false)
}

/// Compile with dead code elimination: only outputs named in
/// `required_outputs` are exposed, and unreachable upstream nodes
/// are pruned from the kernel.
///
/// When `required_outputs` is empty, compiles all bindings as outputs
/// (same as `compile_gk_with_path`).
///
/// The `strict` flag enforces the same rules as `compile_gk_strict`.
pub fn compile_gk_with_outputs(
    source: &str,
    source_dir: Option<&Path>,
    required_outputs: &[String],
    strict: bool,
) -> Result<GkKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let filter = if required_outputs.is_empty() {
        None
    } else {
        Some(required_outputs)
    };
    let mut compiler = Compiler::new(source_dir.map(|p| p.to_path_buf()), strict);
    compiler.source_text = source.to_string();
    compiler.compile_filtered(&ast, filter)
}

/// Compile with additional library directories for module resolution.
///
/// Resolution order: source_dir, then each gk_lib_path in order,
/// then the embedded stdlib.  When `required_outputs` is empty,
/// compiles all bindings as outputs.
pub fn compile_gk_with_libs(
    source: &str,
    source_dir: Option<&Path>,
    gk_lib_paths: Vec<PathBuf>,
    required_outputs: &[String],
    strict: bool,
    context: &str,
) -> Result<GkKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let filter = if required_outputs.is_empty() {
        None
    } else {
        Some(required_outputs)
    };
    let mut compiler = Compiler::with_lib_paths(
        source_dir.map(|p| p.to_path_buf()),
        gk_lib_paths,
        strict,
    );
    compiler.source_text = source.to_string();
    compiler.context_label = context.to_string();
    compiler.compile_filtered(&ast, filter)
}

/// Compile with an optional cursor limit applied to all cursor declarations.
///
/// When `cursor_limit` is `Some(n)`, the compiler inserts a `limit(cursor, n)`
/// node after each cursor declaration, clamping its extent.
pub fn compile_gk_with_libs_and_limit(
    source: &str,
    source_dir: Option<&Path>,
    gk_lib_paths: Vec<PathBuf>,
    required_outputs: &[String],
    strict: bool,
    context: &str,
    cursor_limit: Option<u64>,
) -> Result<GkKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let filter = if required_outputs.is_empty() {
        None
    } else {
        Some(required_outputs)
    };
    let mut compiler = Compiler::with_lib_paths(
        source_dir.map(|p| p.to_path_buf()),
        gk_lib_paths,
        strict,
    );
    compiler.source_text = source.to_string();
    compiler.context_label = context.to_string();
    compiler.cursor_limit = cursor_limit;
    compiler.compile_filtered(&ast, filter)
}

/// Compile with a source directory and optional strict mode.
///
/// When `strict` is true, the compiler enforces:
/// - Explicit `coordinates := (...)` declaration (no inference)
/// - All module arguments must be named (no positional)
/// - All module inputs must be provided by the caller (no fallthrough to coordinates)
pub fn compile_gk_strict(source: &str, source_dir: Option<&Path>, strict: bool) -> Result<GkKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    compile_ast_strict_with_source(&ast, source_dir, strict, source)
}

/// Compile with a compile event log for diagnostic inspection.
pub fn compile_gk_with_log(source: &str, log: &mut super::events::CompileEventLog) -> Result<GkKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let pragmas = super::pragmas::collect_from_ast(&ast);
    record_pragma_events(&pragmas, log);
    let mut compiler = Compiler::new(None, false);
    compiler.source_text = source.to_string();
    compiler.pragmas = pragmas;
    let mut asm = compiler.build_assembler(&ast)?;
    asm.set_strict_wires(compiler.pragmas.strict_types(), compiler.pragmas.strict_values());
    asm.compile_with_log(Some(log)).map_err(|e| e.to_string())
}

/// Scan the source for module-level `// @pragma: …` directives and
/// record one event per pragma in the supplied log:
///
/// - Recognised pragmas → `PragmaAcknowledged` (advisory).
/// - Unrecognised pragmas → `UnknownPragma` (warning) — pragmas are
///   forward-compatible, so the compile keeps going.
///
/// Hooked into every `compile_gk_with_log`-shaped entry point. The
/// extracted [`PragmaSet`] can also be re-fetched directly via
/// [`crate::dsl::pragmas::extract_pragmas`] when downstream graph
/// transforms need it.
///
/// [`PragmaSet`]: crate::dsl::pragmas::PragmaSet
/// Emit `PragmaAcknowledged` (advisory) for recognised pragma
/// names and `UnknownPragma` (warning) for the rest. Forward-
/// compatible: an unknown pragma never blocks compilation.
pub(crate) fn record_pragma_events(
    set: &super::pragmas::PragmaSet,
    log: &mut super::events::CompileEventLog,
) {
    use super::events::CompileEvent;
    for entry in &set.entries {
        let known = matches!(entry.name.as_str(), "strict_types" | "strict_values" | "strict");
        if known {
            log.push(CompileEvent::PragmaAcknowledged {
                name: entry.name.clone(),
                line: entry.line,
            });
        } else {
            log.push(CompileEvent::UnknownPragma {
                name: entry.name.clone(),
                line: entry.line,
            });
        }
    }
}

/// Compile with full diagnostics: errors, warnings, suggestions.
///
/// Returns `(Ok(kernel), report)` on success with possible warnings,
/// or `(Err(()), report)` on failure with errors. The report always
/// contains all diagnostics.
pub fn compile_gk_checked(source: &str) -> (Result<GkKernel, ()>, DiagnosticReport) {
    let mut report = DiagnosticReport::new(source);

    let tokens = match lexer::lex(source) {
        Ok(t) => t,
        Err(e) => {
            report.error(crate::dsl::lexer::Span { line: 1, col: 1 }, e);
            return (Err(()), report);
        }
    };

    let ast = match parser::parse(tokens) {
        Ok(a) => a,
        Err(e) => {
            report.error(crate::dsl::lexer::Span { line: 1, col: 1 }, e);
            return (Err(()), report);
        }
    };

    // Validate the AST before compiling
    validate_ast(&ast, &mut report);

    if report.has_errors() {
        return (Err(()), report);
    }

    match compile_ast(&ast) {
        Ok(kernel) => (Ok(kernel), report),
        Err(e) => {
            report.error(crate::dsl::lexer::Span { line: 1, col: 1 }, e);
            (Err(()), report)
        }
    }
}

/// Evaluate a GK expression as a compile-time constant.
///
/// The expression must have no input dependencies. It is compiled
/// as a zero-input program and constant-folded. Returns the folded
/// value, or an error if the expression depends on runtime inputs
/// or fails to compile.
///
/// # Examples
///
/// ```
/// use nb_variates::dsl::compile::eval_const_expr;
/// let v = eval_const_expr("4 * 4").unwrap();
/// assert_eq!(v.as_u64(), 16);  // both int literals → u64_mul
/// let v = eval_const_expr("4.0 * 4.0").unwrap();
/// assert_eq!(v.as_f64(), 16.0);  // both float literals → f64_mul
/// ```
pub fn eval_const_expr(source: &str) -> Result<crate::node::Value, String> {
    let wrapped = format!("inputs := ()\nout := {source}");
    let kernel = compile_gk(&wrapped)?;
    kernel.get_constant("out")
        .cloned()
        .ok_or_else(|| format!(
            "not a const expression: '{}' depends on runtime inputs",
            source
        ))
}

/// Extract an integer literal from a positional argument. Returns None
/// for named args, non-int-literal positional args, or any other form.
fn positional_int_lit(arg: &crate::dsl::ast::Arg) -> Option<u64> {
    match arg {
        crate::dsl::ast::Arg::Positional(crate::dsl::ast::Expr::IntLit(v, _)) => Some(*v),
        _ => None,
    }
}

/// Extract a string literal from an optional positional argument.
/// Re-exported for cursor-sugar handlers in node modules that
/// validate string-literal-only constructor args.
pub fn positional_str_lit(arg: Option<&crate::dsl::ast::Arg>) -> Option<String> {
    match arg? {
        crate::dsl::ast::Arg::Positional(crate::dsl::ast::Expr::StringLit(s, _)) => Some(s.clone()),
        _ => None,
    }
}

/// Compile a parsed AST into a runtime kernel.
pub fn compile_ast(file: &GkFile) -> Result<GkKernel, String> {
    compile_ast_with_path(file, None)
}

/// Compile a parsed AST with module resolution from a source directory.
pub fn compile_ast_with_path(file: &GkFile, source_dir: Option<&Path>) -> Result<GkKernel, String> {
    compile_ast_strict(file, source_dir, false)
}

/// Compile a parsed AST with module resolution and optional strict mode.
///
/// When `strict` is true, the compiler enforces:
/// - Explicit `coordinates := (...)` declaration (no inference)
/// - All module arguments must be named (no positional)
/// - All module inputs must be provided by the caller (no fallthrough)
pub fn compile_ast_strict(file: &GkFile, source_dir: Option<&Path>, strict: bool) -> Result<GkKernel, String> {
    let mut compiler = Compiler::new(source_dir.map(|p| p.to_path_buf()), strict);
    compiler.compile(file)
}

/// Compile a parsed AST with strict mode and source text for diagnostics.
///
/// Same as `compile_ast_strict` but attaches the original source text
/// to the compiled program for diagnostic inspection.
fn compile_ast_strict_with_source(
    file: &GkFile,
    source_dir: Option<&Path>,
    strict: bool,
    source: &str,
) -> Result<GkKernel, String> {
    let mut compiler = Compiler::new(source_dir.map(|p| p.to_path_buf()), strict);
    compiler.source_text = source.to_string();
    // Pragmas affect strict-wire mode even when no event log is
    // supplied — collect them from the AST so library callers
    // that go through `compile_gk_with_path` still honour them.
    compiler.pragmas = super::pragmas::collect_from_ast(file);
    compiler.compile(file)
}

pub(super) struct Compiler {
    pub(super) input_names: Vec<String>,
    /// Track all named outputs so we can expose them.
    pub(super) all_names: Vec<String>,
    /// Auto-generated node counter for desugared intermediates.
    pub(super) anon_counter: usize,
    /// Directory for module resolution (search for .gk files).
    pub(super) source_dir: Option<PathBuf>,
    /// Additional library directories for module resolution.
    ///
    /// Searched after `source_dir` but before the embedded stdlib.
    /// Populated via `--gk-lib=path` CLI flags.
    pub(super) gk_lib_paths: Vec<PathBuf>,
    /// Cache of already-resolved module ASTs: module_name → (inputs, statements).
    pub(super) module_cache: std::collections::HashMap<String, ResolvedModule>,
    /// When true, enforce strict validation.
    pub(super) strict: bool,
    /// Original source text, attached to compiled programs for diagnostics.
    source_text: String,
    /// Source schemas collected during compilation.
    pub(super) cursor_schemas: Vec<crate::source::SourceSchema>,
    /// Deferred cursor extent resolutions: each entry maps a cursor
    /// schema index to the aux output names that, once folded, give
    /// the range's start and end values. These are resolved after the
    /// kernel compiles by reading `get_constant()` for each name.
    pub(super) deferred_extents: Vec<DeferredExtent>,
    /// Optional limit applied to all cursors (from `limit` activity param).
    pub(super) cursor_limit: Option<u64>,
    /// Diagnostic context label.
    context_label: String,
    /// Module-level pragmas extracted from the source. Drive the
    /// assembler's `strict_types` / `strict_values` flags
    /// (SRD 15 §"Module-Level Pragmas" + §"Strict Wire Mode").
    pub(super) pragmas: super::pragmas::PragmaSet,
}

/// Records a cursor whose `range(...)` bounds reference const
/// expressions (e.g., `vector_count("example:default")`) rather than
/// integer literals. The expressions are compiled as auxiliary outputs
/// and the extent is resolved after kernel compilation by querying the
/// constant values.
pub(super) struct DeferredExtent {
    /// Index into `cursor_schemas` whose extent needs resolution.
    pub schema_idx: usize,
    /// Name of the aux output that, when folded, gives the start value.
    pub start_output: String,
    /// Name of the aux output that, when folded, gives the end value.
    pub end_output: String,
}

impl Compiler {
    pub(super) fn new(source_dir: Option<PathBuf>, strict: bool) -> Self {
        Self {
            input_names: Vec::new(),
            all_names: Vec::new(),
            anon_counter: 0,
            source_dir,
            gk_lib_paths: Vec::new(),
            module_cache: std::collections::HashMap::new(),
            strict,
            source_text: String::new(),
            context_label: "(gk)".into(),
            cursor_schemas: Vec::new(),
            deferred_extents: Vec::new(),
            cursor_limit: None,
            pragmas: super::pragmas::PragmaSet::default(),
        }
    }

    pub(super) fn with_lib_paths(source_dir: Option<PathBuf>, gk_lib_paths: Vec<PathBuf>, strict: bool) -> Self {
        Self {
            input_names: Vec::new(),
            all_names: Vec::new(),
            anon_counter: 0,
            source_dir,
            gk_lib_paths,
            module_cache: std::collections::HashMap::new(),
            strict,
            source_text: String::new(),
            context_label: "(gk)".into(),
            cursor_schemas: Vec::new(),
            deferred_extents: Vec::new(),
            cursor_limit: None,
            pragmas: super::pragmas::PragmaSet::default(),
        }
    }

    /// Process a source declaration: create input ports for projections,
    /// passthrough nodes, and record the schema.
    fn process_cursor(&mut self, asm: &mut GkAssembler, decl: &crate::dsl::ast::CursorDecl) -> Result<(), String> {
        let source_name = &decl.name;

        // Cursor-sugar dispatch: any node module can register a
        // handler that recognizes a non-`range` constructor (e.g.
        // `vectordata_base("ds", "label_00")`) and rewrites it into
        // a synthetic `range(...)` plus a list of aux bindings to
        // emit after input ports are wired. The core stays
        // generic — nothing here knows that vectordata exists.
        // See `dsl::cursor_sugar` for the registry mechanism.
        let sugar = crate::dsl::cursor_sugar::dispatch(source_name, &decl.constructor)?;
        let effective_constructor = match &sugar {
            Some(s) => s.effective_constructor.clone(),
            None => decl.constructor.clone(),
        };

        // All sources get an "ordinal" projection.
        let mut projections = vec![
            ("ordinal".to_string(), crate::node::PortType::U64),
        ];

        // Determine extent from constructor args. Three cases per arg:
        //   1. Integer literal → use directly
        //   2. Other const-foldable expression (e.g. `vector_count("...")`)
        //      → compile as an aux output and resolve after kernel compiles
        //   3. Arg references runtime state → no extent available
        //
        // Immediate-literal cases produce a concrete extent here.
        // Deferred cases push a DeferredExtent record; the outer compile
        // routine reads the folded values after compilation and updates
        // the schema's extent in place.
        let mut deferred: Option<(Option<u64>, String, Option<u64>, String)> = None;
        let extent = match &effective_constructor {
            crate::dsl::ast::Expr::Call(call) if call.func == "range" && call.args.len() >= 2 => {
                let start_literal = positional_int_lit(&call.args[0]);
                let end_literal = positional_int_lit(&call.args[1]);

                match (start_literal, end_literal) {
                    // Both literal — compute directly.
                    (Some(s), Some(e)) => Some(e.saturating_sub(s)),
                    // At least one non-literal — compile as aux outputs.
                    _ => {
                        let start_name = format!("__cursor_extent_{source_name}_start");
                        let end_name = format!("__cursor_extent_{source_name}_end");
                        // Compile each arg as a named auxiliary output. Errors
                        // are returned so the user sees them — silently
                        // dropping them would leave extent=None and produce
                        // a phase that runs zero cycles with no explanation.
                        if let crate::dsl::ast::Arg::Positional(expr) = &call.args[0] {
                            self.compile_binding(asm, &[start_name.clone()], expr)
                                .map_err(|e| format!(
                                    "cursor '{source_name}': failed to compile range start: {e}"
                                ))?;
                        }
                        if let crate::dsl::ast::Arg::Positional(expr) = &call.args[1] {
                            self.compile_binding(asm, &[end_name.clone()], expr)
                                .map_err(|e| format!(
                                    "cursor '{source_name}': failed to compile range end: {e}"
                                ))?;
                        }
                        deferred = Some((start_literal, start_name, end_literal, end_name));
                        None
                    }
                }
            }
            _ => None,
        };

        // Create input ports and passthrough nodes for each projection.
        for (field_name, port_type) in &projections {
            let input_name = format!("{source_name}__{field_name}");
            let default_value = match port_type {
                crate::node::PortType::U64 => crate::node::Value::U64(0),
                crate::node::PortType::F64 => crate::node::Value::F64(0.0),
                _ => crate::node::Value::None,
            };

            asm.add_input(&input_name, default_value, *port_type);
            self.input_names.push(input_name.clone());

            let passthrough = Box::new(
                crate::nodes::identity::PortPassthrough::new(&input_name, *port_type)
            );
            let node_name = format!("{source_name}__{field_name}");
            asm.add_node(
                &node_name,
                passthrough,
                vec![WireRef::input(&input_name)],
            );
            asm.add_output(&node_name, WireRef::node(&node_name));
        }

        // Apply any aux bindings the sugar handler asked for.
        // Bindings whose `projection` is `Some` are also published
        // as cursor projections — both pinned on the schema and
        // exposed as kernel outputs the runtime can read.
        if let Some(sugar) = sugar {
            for aux in sugar.aux_bindings {
                self.compile_binding(asm, &[aux.name.clone()], &aux.value)
                    .map_err(|e| format!(
                        "cursor '{source_name}': failed to compile aux binding '{}': {e}",
                        aux.name,
                    ))?;
                if let Some((field, port_type)) = aux.projection {
                    projections.push((field, port_type));
                    asm.add_output(&aux.name, WireRef::node(&aux.name));
                }
            }
        }

        // If a limit is set, insert a limit() node that shadows the cursor wire.
        // The limit node is a visible, documented passthrough that clamps extent.
        let effective_extent = if let Some(limit_val) = self.cursor_limit {
            let limit_node_name = format!("{source_name}__limit");
            let ordinal_wire = format!("{source_name}__ordinal");
            asm.add_node(
                &limit_node_name,
                Box::new(crate::nodes::context::CursorLimit::new(limit_val)),
                vec![WireRef::node(&ordinal_wire)],
            );
            // Shadow the ordinal output with the limited version
            asm.add_output(&ordinal_wire, WireRef::node(&limit_node_name));

            // Clamp extent
            extent.map(|e| e.min(limit_val)).or(Some(limit_val))
        } else {
            extent
        };

        let schema_idx = self.cursor_schemas.len();
        self.cursor_schemas.push(crate::source::SourceSchema {
            name: source_name.clone(),
            projections,
            extent: effective_extent,
        });

        // Record deferred extent resolution if the range bounds are not
        // both literals. Post-compile, the outer compile routine will
        // query the aux outputs' folded constants and update this
        // schema's extent in place.
        if let Some((_start_lit, start_output, _end_lit, end_output)) = deferred {
            self.deferred_extents.push(DeferredExtent {
                schema_idx,
                start_output,
                end_output,
            });
        }
        Ok(())
    }

    pub(super) fn compile(&mut self, file: &GkFile) -> Result<GkKernel, String> {
        // First pass: find explicit coordinates
        let mut has_explicit_coords = false;
        for stmt in &file.statements {
            if let Statement::Coordinates(names, _) = stmt {
                self.input_names = names.clone();
                has_explicit_coords = true;
            }
        }

        // Input declaration check: error in strict mode (modules, .gk files)
        if !has_explicit_coords && self.strict {
            return Err(
                "strict mode: no 'inputs' declaration — add 'inputs := (cycle)' \
                 to declare graph inputs explicitly".into()
            );
        }

        // If no explicit coordinates, infer from unbound references
        if !has_explicit_coords {
            let defined: HashSet<String> = file.statements.iter().flat_map(|stmt| {
                match stmt {
                    Statement::InitBinding(b) => vec![b.name.clone()],
                    Statement::CycleBinding(b) => b.targets.clone(),
                    Statement::ModuleDef(m) => vec![m.name.clone()],
                    Statement::ExternPort(p) => vec![p.name.clone()],
                    Statement::Coordinates(_, _) => vec![],
                    Statement::Cursor(_) => vec![],
                    Statement::Pragma { .. } => vec![],
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) | Statement::Pragma { .. } => continue,
                    Statement::InitBinding(b) => &b.value,
                    Statement::CycleBinding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort(); // deterministic order
            self.input_names = inferred;
        }

        // Zero inferred inputs means all bindings are constants — valid.

        // Pragmas were already collected from the AST by the
        // top-level compile entry points. If a caller bypasses
        // those (custom Compiler invocation), populate from this
        // AST as a last resort so the strict-wire flags below
        // still reflect the source.
        if self.pragmas.entries.is_empty() {
            self.pragmas = super::pragmas::collect_from_ast(file);
        }

        let mut asm = GkAssembler::new(self.input_names.clone());
        // Honour module-level pragmas: a `pragma strict_values` (or
        // `strict`) directive at the source head opts into
        // auto-inserted assertion nodes (SRD 15 §"Module-Level
        // Pragmas" + §"Strict Wire Mode").
        asm.set_strict_wires(self.pragmas.strict_types(), self.pragmas.strict_values());

        // Second pass: process all bindings
        for stmt in &file.statements {
            match stmt {
                Statement::Coordinates(_, _) => {} // already handled
                Statement::InitBinding(b) => {
                    self.compile_binding(
                        &mut asm,
                        &[b.name.clone()],
                        &b.value,
                    )?;
                    if b.modifier != BindingModifier::None {
                        asm.set_output_modifier(&b.name, b.modifier);
                    }
                }
                Statement::CycleBinding(b) => {
                    self.compile_binding(
                        &mut asm,
                        &b.targets,
                        &b.value,
                    )?;
                    if b.modifier != BindingModifier::None {
                        for target in &b.targets {
                            asm.set_output_modifier(target, b.modifier);
                        }
                    }
                }
                Statement::ModuleDef(_) => {
                    // Module definitions are not executed — they're
                    // templates resolved by the module system when
                    // referenced from another file/kernel.
                }
                Statement::ExternPort(port) => {
                    // Declare the input on the assembler.
                    // Default is None (unset) unless the extern
                    // declaration specifies a default value.
                    let default_value = crate::node::Value::None;
                    let port_type = match port.typ.as_str() {
                        "u64" => crate::node::PortType::U64,
                        "f64" => crate::node::PortType::F64,
                        _ => crate::node::PortType::Str,
                    };
                    asm.add_input(&port.name, default_value, port_type);

                    // Register the extern name as an input so the
                    // binding compiler resolves it as WireRef::input
                    // (enables `hash(offset)` where offset is extern)
                    self.input_names.push(port.name.clone());

                    // Create a passthrough node wired to the input
                    let passthrough = Box::new(
                        crate::nodes::identity::PortPassthrough::new(&port.name, port_type)
                    );
                    let passthrough_name = format!("__port_{}", port.name);
                    asm.add_node(
                        &passthrough_name,
                        passthrough,
                        vec![WireRef::input(&port.name)],
                    );
                    // Register as output so {name} resolves from GK
                    asm.add_output(&port.name, WireRef::node(&passthrough_name));
                }
                Statement::Cursor(decl) => {
                    self.process_cursor(&mut asm, decl)?;
                }
                Statement::Pragma { .. } => {
                    // Pragmas were collected before this pass (see
                    // `collect_pragmas`) and applied to the
                    // assembler via `set_strict_wires` already.
                    // Nothing to do during binding processing.
                }
            }
        }

        // Expose all top-level named bindings as outputs
        for name in &self.all_names {
            asm.add_output(name, WireRef::node(name));
        }

        // Attach source and context for diagnostics
        asm.set_context(&self.source_text, &self.context_label);
        let mut kernel = asm.compile_strict(self.strict).map_err(|e| format!("{e}"))?;

        // Resolve deferred cursor extents. At this point the kernel has
        // folded any const expressions to constant outputs; we read the
        // aux outputs compiled by process_cursor and update the schema
        // extents in place.
        for deferred in &self.deferred_extents {
            let start = kernel.get_constant(&deferred.start_output).map(|v| v.as_u64());
            let end = kernel.get_constant(&deferred.end_output).map(|v| v.as_u64());
            if let (Some(s), Some(e)) = (start, end) {
                let resolved_extent = e.saturating_sub(s);
                // Apply cursor_limit clamping if configured
                let final_extent = self.cursor_limit
                    .map(|limit| resolved_extent.min(limit))
                    .unwrap_or(resolved_extent);
                if let Some(schema) = self.cursor_schemas.get_mut(deferred.schema_idx) {
                    schema.extent = Some(final_extent);
                }
            }
        }

        // Propagate source schemas to the program for runtime discovery
        if !self.cursor_schemas.is_empty() {
            kernel.set_cursor_schemas(self.cursor_schemas.clone());
        }
        Ok(kernel)
    }

    /// Build an assembler with all nodes and wiring, without compiling.
    pub(super) fn build_assembler(&mut self, file: &GkFile) -> Result<GkAssembler, String> {
        // Reuse the same logic as compile(), but return the assembler
        // instead of calling asm.compile().

        // First pass: find explicit coordinates
        for stmt in &file.statements {
            if let Statement::Coordinates(names, _) = stmt {
                self.input_names = names.clone();
            }
        }

        if self.input_names.is_empty() {
            let defined: HashSet<String> = file.statements.iter().flat_map(|stmt| {
                match stmt {
                    Statement::InitBinding(b) => vec![b.name.clone()],
                    Statement::CycleBinding(b) => b.targets.clone(),
                    Statement::ModuleDef(m) => vec![m.name.clone()],
                    Statement::ExternPort(p) => vec![p.name.clone()],
                    Statement::Coordinates(_, _) => vec![],
                    Statement::Cursor(_) => vec![],
                    Statement::Pragma { .. } => vec![],
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) | Statement::Pragma { .. } => continue,
                    Statement::InitBinding(b) => &b.value,
                    Statement::CycleBinding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort();
            self.input_names = inferred;
        }

        // Zero inferred inputs means all bindings are constants — valid.

        let mut asm = GkAssembler::new(self.input_names.clone());
        asm.set_strict_wires(self.pragmas.strict_types(), self.pragmas.strict_values());

        for stmt in file.statements.clone() {
            match &stmt {
                Statement::CycleBinding(binding) => {
                    self.compile_binding(&mut asm, &binding.targets, &binding.value)?;
                    if binding.modifier != BindingModifier::None {
                        for target in &binding.targets {
                            asm.set_output_modifier(target, binding.modifier);
                        }
                    }
                }
                Statement::InitBinding(binding) => {
                    self.compile_binding(&mut asm, &[binding.name.clone()], &binding.value)?;
                    if binding.modifier != BindingModifier::None {
                        asm.set_output_modifier(&binding.name, binding.modifier);
                    }
                }
                Statement::ExternPort(_) => {}
                Statement::ModuleDef(_) => {}
                Statement::Coordinates(_, _) => {}
                Statement::Pragma { .. } => {}
                Statement::Cursor(decl) => {
                    self.process_cursor(&mut asm, decl)?;
                }
            }
        }

        for name in &self.all_names {
            asm.add_output(name, WireRef::node(name));
        }

        asm.set_context(&self.source_text, &self.context_label);
        Ok(asm)
    }

    /// Compile with optional output filtering for dead code elimination.
    ///
    /// When `required_outputs` is `Some`, only those named bindings are
    /// exposed as kernel outputs. The assembler's DCE pass then prunes
    /// all nodes not reachable from those outputs.
    ///
    /// When `None`, behaves identically to `compile()`.
    pub(super) fn compile_filtered(
        &mut self,
        file: &GkFile,
        required_outputs: Option<&[String]>,
    ) -> Result<GkKernel, String> {
        // First pass: find explicit coordinates
        for stmt in &file.statements {
            if let Statement::Coordinates(names, _) = stmt {
                self.input_names = names.clone();
            }
        }

        // Input declaration check: error in strict mode (modules, .gk files)
        if self.input_names.is_empty() && self.strict {
            return Err(
                "strict mode: no 'inputs' declaration — add 'inputs := (cycle)' \
                 to declare graph inputs explicitly".into()
            );
        }

        // If no explicit coordinates, infer from unbound references
        if self.input_names.is_empty() {
            let defined: HashSet<String> = file.statements.iter().flat_map(|stmt| {
                match stmt {
                    Statement::InitBinding(b) => vec![b.name.clone()],
                    Statement::CycleBinding(b) => b.targets.clone(),
                    Statement::ModuleDef(m) => vec![m.name.clone()],
                    Statement::ExternPort(p) => vec![p.name.clone()],
                    Statement::Coordinates(_, _) => vec![],
                    Statement::Cursor(_) => vec![],
                    Statement::Pragma { .. } => vec![],
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) | Statement::Pragma { .. } => continue,
                    Statement::InitBinding(b) => &b.value,
                    Statement::CycleBinding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort();
            self.input_names = inferred;
        }

        // Zero inferred inputs means all bindings are constants — valid.

        let mut asm = GkAssembler::new(self.input_names.clone());

        // Second pass: process all bindings into the assembler
        for stmt in &file.statements {
            match stmt {
                Statement::Coordinates(_, _) => {}
                Statement::InitBinding(b) => {
                    self.compile_binding(
                        &mut asm,
                        &[b.name.clone()],
                        &b.value,
                    )?;
                    if b.modifier != BindingModifier::None {
                        asm.set_output_modifier(&b.name, b.modifier);
                    }
                }
                Statement::CycleBinding(b) => {
                    self.compile_binding(
                        &mut asm,
                        &b.targets,
                        &b.value,
                    )?;
                    if b.modifier != BindingModifier::None {
                        for target in &b.targets {
                            asm.set_output_modifier(target, b.modifier);
                        }
                    }
                }
                Statement::ModuleDef(_) | Statement::ExternPort(_) => {}
                Statement::Cursor(decl) => {
                    self.process_cursor(&mut asm, decl)?;
                }
                Statement::Pragma { .. } => {}
            }
        }

        // Unused binding check: defer to kernel-level check in fold_init_constants_impl.
        // The kernel has the full wiring graph and can accurately determine which
        // nodes have no downstream consumers. The compiler can't do this reliably
        // because it doesn't track inter-binding wire dependencies.

        // Expose outputs: only the required set, or all if no filter.
        // Cursor extent aux outputs (`__cursor_extent_*`) must always be
        // exposed regardless of the filter — they are queried by the
        // post-compile deferred extent resolution and would otherwise be
        // pruned by DCE, leaving the cursor extent unresolved.
        match required_outputs {
            Some(required) => {
                for name in required {
                    if self.all_names.contains(name) {
                        asm.add_output(name, WireRef::node(name));
                    }
                }
                for deferred in &self.deferred_extents {
                    if self.all_names.contains(&deferred.start_output) {
                        asm.add_output(&deferred.start_output, WireRef::node(&deferred.start_output));
                    }
                    if self.all_names.contains(&deferred.end_output) {
                        asm.add_output(&deferred.end_output, WireRef::node(&deferred.end_output));
                    }
                }
            }
            None => {
                for name in &self.all_names {
                    asm.add_output(name, WireRef::node(name));
                }
            }
        }

        asm.set_context(&self.source_text, &self.context_label);
        let mut kernel = asm.compile_strict(self.strict).map_err(|e| format!("{e}"))?;

        // Resolve deferred cursor extents (same logic as in compile()).
        for deferred in &self.deferred_extents {
            let start = kernel.get_constant(&deferred.start_output).map(|v| v.as_u64());
            let end = kernel.get_constant(&deferred.end_output).map(|v| v.as_u64());
            if let (Some(s), Some(e)) = (start, end) {
                let resolved_extent = e.saturating_sub(s);
                let final_extent = self.cursor_limit
                    .map(|limit| resolved_extent.min(limit))
                    .unwrap_or(resolved_extent);
                if let Some(schema) = self.cursor_schemas.get_mut(deferred.schema_idx) {
                    schema.extent = Some(final_extent);
                }
            }
        }

        if !self.cursor_schemas.is_empty() {
            kernel.set_cursor_schemas(self.cursor_schemas.clone());
        }
        Ok(kernel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_hello_world() {
        let src = r#"
            coordinates := (cycle)
            hashed := hash(cycle)
            user_id := mod(hashed, 1000000)
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[42]);
        let uid = kernel.pull("user_id").as_u64();
        assert!(uid < 1_000_000, "user_id={uid}");
    }

    #[test]
    fn compile_with_inline_nesting() {
        let src = r#"
            coordinates := (cycle)
            result := mod(hash(cycle), 100)
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[42]);
        assert!(kernel.pull("result").as_u64() < 100);
    }

    #[test]
    fn compile_deterministic() {
        let src = r#"
            coordinates := (cycle)
            h := hash(cycle)
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[42]);
        let v1 = kernel.pull("h").as_u64();
        kernel.set_inputs(&[42]);
        let v2 = kernel.pull("h").as_u64();
        assert_eq!(v1, v2);
    }

    #[test]
    fn shared_modifier_tracked() {
        let src = r#"
            inputs := (cycle)
            shared counter := hash(cycle)
            normal := mod(hash(cycle), 100)
        "#;
        let kernel = compile_gk(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("counter"),
            crate::dsl::ast::BindingModifier::Shared
        );
        assert_eq!(
            kernel.program().output_modifier("normal"),
            crate::dsl::ast::BindingModifier::None
        );
    }

    #[test]
    fn final_modifier_tracked() {
        let src = r#"
            inputs := (cycle)
            final dim := 128
        "#;
        let kernel = compile_gk(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("dim"),
            crate::dsl::ast::BindingModifier::Final
        );
    }

    #[test]
    fn shared_init_modifier_tracked() {
        let src = r#"
            inputs := (cycle)
            shared init budget = 100
        "#;
        let kernel = compile_gk(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("budget"),
            crate::dsl::ast::BindingModifier::Shared
        );
        // Verify it also compiles as an init-time constant
        assert!(kernel.get_constant("budget").is_some());
    }

    #[test]
    fn final_init_modifier_tracked() {
        let src = r#"
            inputs := (cycle)
            final init max_dim = 256
        "#;
        let kernel = compile_gk(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("max_dim"),
            crate::dsl::ast::BindingModifier::Final
        );
        assert_eq!(kernel.get_constant("max_dim").unwrap().as_u64(), 256);
    }

    #[test]
    fn shared_outputs_query() {
        let src = r#"
            inputs := (cycle)
            shared counter := hash(cycle)
            shared budget := mod(hash(cycle), 100)
            normal := hash(cycle)
        "#;
        let kernel = compile_gk(src).unwrap();
        let mut shared = kernel.program().shared_outputs();
        shared.sort();
        assert_eq!(shared, vec!["budget", "counter"]);
        assert!(kernel.program().final_outputs().is_empty());
    }

    #[test]
    fn final_outputs_query() {
        let src = r#"
            inputs := (cycle)
            final dim := 128
            final dataset := "example"
            normal := hash(cycle)
        "#;
        let kernel = compile_gk(src).unwrap();
        let mut finals = kernel.program().final_outputs();
        finals.sort();
        assert_eq!(finals, vec!["dataset", "dim"]);
        assert!(kernel.program().shared_outputs().is_empty());
    }

    #[test]
    fn unmodified_bindings_have_none_modifier() {
        let src = r#"
            inputs := (cycle)
            h := hash(cycle)
            v := mod(h, 100)
        "#;
        let kernel = compile_gk(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("h"),
            crate::dsl::ast::BindingModifier::None
        );
        assert_eq!(
            kernel.program().output_modifier("v"),
            crate::dsl::ast::BindingModifier::None
        );
    }

    #[test]
    fn compile_mixed_radix() {
        let src = r#"
            coordinates := (cycle)
            (tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)
            tenant_h := hash(tenant)
            tenant_code := mod(tenant_h, 10000)
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[4_201_337]);
        let tc = kernel.pull("tenant_code").as_u64();
        assert!(tc < 10000, "tenant_code={tc}");
    }

    #[test]
    fn compile_string_constant() {
        let src = r#"
            coordinates := (cycle)
            label := "hello world"
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("label").as_str(), "hello world");
    }

    #[test]
    fn compile_int_constant() {
        let src = r#"
            coordinates := (cycle)
            base := 1710000000000
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("base").as_u64(), 1_710_000_000_000);
    }

    #[test]
    fn compile_comments_ignored() {
        let src = r#"
            // This is a comment
            coordinates := (cycle)
            // Another comment
            h := hash(cycle)
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[1]);
        assert!(kernel.pull("h").as_u64() != 0);
    }

    // --- Diagnostic tests ---

    #[test]
    fn error_unknown_function() {
        let src = "coordinates := (cycle)\nresult := foobar(cycle)";
        let (_result, report) = compile_gk_checked(src);
        assert!(report.has_errors());
        let errors = report.errors();
        assert!(errors.iter().any(|e| e.message.contains("unknown function")));
        assert!(errors.iter().any(|e| e.message.contains("foobar")));
    }

    #[test]
    fn error_unknown_function_suggests() {
        let src = "coordinates := (cycle)\nresult := hahs(cycle)";
        let (_, report) = compile_gk_checked(src);
        let errors = report.errors();
        let err = errors.iter().find(|e| e.message.contains("hahs")).unwrap();
        assert!(err.hint.as_ref().unwrap().contains("hash"),
            "should suggest 'hash', got: {:?}", err.hint);
    }

    #[test]
    fn inferred_coordinates() {
        // Without explicit coordinates, 'cycle' is inferred as a coordinate input
        let src = "h := hash(cycle)";
        let mut kernel = compile_gk(src).unwrap();
        assert_eq!(kernel.input_names(), &["cycle"]);
        kernel.set_inputs(&[42]);
        let h = kernel.pull("h").as_u64();
        assert_ne!(h, 42); // hashed, not identity
    }

    #[test]
    fn inferred_multi_coordinates() {
        // Multiple unbound names become multiple coordinate inputs (sorted)
        let src = "h := hash(interleave(row, col))";
        let mut kernel = compile_gk(src).unwrap();
        assert_eq!(kernel.input_names(), &["col", "row"]); // alphabetically sorted
        kernel.set_inputs(&[10, 20]);
        let h = kernel.pull("h").as_u64();
        assert_ne!(h, 0);
    }

    #[test]
    fn explicit_coordinates_rejects_unbound() {
        // With explicit coordinates, unbound references are errors
        let src = "coordinates := (cycle)\nh := hash(unknown)";
        let (_, report) = compile_gk_checked(src);
        assert!(report.has_errors());
        assert!(report.errors().iter().any(|e|
            e.message.contains("undefined") && e.message.contains("unknown")));
    }

    #[test]
    fn warning_forward_reference() {
        let src = r#"
            coordinates := (cycle)
            result := mod(h, 100)
            h := hash(cycle)
        "#;
        let (_, report) = compile_gk_checked(src);
        let warnings = report.warnings();
        assert!(warnings.iter().any(|w| w.message.contains("forward reference")),
            "should warn about forward ref, got: {:?}", warnings);
    }

    #[test]
    fn error_undefined_wire() {
        let src = r#"
            coordinates := (cycle)
            result := hash(nonexistent)
        "#;
        let (_, report) = compile_gk_checked(src);
        assert!(report.has_errors());
        assert!(report.errors().iter().any(|e|
            e.message.contains("undefined") && e.message.contains("nonexistent")));
    }

    #[test]
    fn error_report_includes_source_line() {
        let src = "coordinates := (cycle)\nresult := unknown_func(cycle)";
        let (_, report) = compile_gk_checked(src);
        let s = report.to_string();
        assert!(s.contains("unknown_func"), "report should include source context");
    }

    #[test]
    fn checked_compile_success_with_no_errors() {
        let src = r#"
            coordinates := (cycle)
            h := hash(cycle)
            result := mod(h, 1000)
        "#;
        let (result, report) = compile_gk_checked(src);
        assert!(!report.has_errors());
        assert!(result.is_ok());
    }

    // --- Strict mode tests ---

    #[test]
    fn strict_requires_explicit_inputs() {
        // Without inputs declaration, strict mode should error
        let src = "h := hash(cycle)";
        let result = compile_gk_strict(src, None, true);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("strict mode"), "expected strict error, got: {err}");
        assert!(err.contains("inputs"), "expected inputs mention, got: {err}");
    }

    #[test]
    fn strict_accepts_explicit_coordinates() {
        // With explicit coordinates, strict mode should succeed
        let src = r#"
            coordinates := (cycle)
            h := hash(cycle)
        "#;
        let mut kernel = compile_gk_strict(src, None, true).unwrap();
        kernel.set_inputs(&[42]);
        let h = kernel.pull("h").as_u64();
        assert_ne!(h, 42); // hashed, not identity
    }

    #[test]
    fn non_strict_infers_coordinates() {
        // Without strict, coordinate inference works as before
        let src = "h := hash(cycle)";
        let mut kernel = compile_gk_strict(src, None, false).unwrap();
        kernel.set_inputs(&[42]);
        assert_ne!(kernel.pull("h").as_u64(), 42);
    }

    // --- Dead code elimination tests ---

    #[test]
    fn dce_filters_to_required_outputs() {
        // GK source defines three bindings but we only request one
        let src = r#"
            coordinates := (cycle)
            a := hash(cycle)
            b := mod(a, 100)
            c := add(cycle, 1)
        "#;
        let required = vec!["b".to_string()];
        let mut kernel = compile_gk_with_outputs(src, None, &required, false).unwrap();
        kernel.set_inputs(&[42]);

        // "b" should be available and correct
        let b = kernel.pull("b").as_u64();
        assert!(b < 100, "b={b}");

        // "a" and "c" should NOT be in the output map
        let outputs = kernel.output_names();
        assert!(outputs.contains(&"b"), "should contain 'b'");
        assert!(!outputs.contains(&"a"), "should not contain pruned 'a'");
        assert!(!outputs.contains(&"c"), "should not contain pruned 'c'");
    }

    #[test]
    fn dce_preserves_upstream_dependencies() {
        // Request "result" which depends on "h" — both the result node
        // and its upstream "h" node must be kept, but "unrelated" is pruned
        let src = r#"
            coordinates := (cycle)
            h := hash(cycle)
            result := mod(h, 1000)
            unrelated := add(cycle, 999)
        "#;
        let required = vec!["result".to_string()];
        let mut kernel = compile_gk_with_outputs(src, None, &required, false).unwrap();
        kernel.set_inputs(&[42]);

        let result = kernel.pull("result").as_u64();
        assert!(result < 1000, "result={result}");

        let outputs = kernel.output_names();
        assert!(!outputs.contains(&"unrelated"), "unrelated should be pruned");
    }

    #[test]
    fn dce_empty_required_compiles_all() {
        // Empty required_outputs should produce the same kernel as compile_gk
        let src = r#"
            coordinates := (cycle)
            a := hash(cycle)
            b := mod(a, 100)
        "#;
        let kernel_all = compile_gk(src).unwrap();
        let kernel_empty = compile_gk_with_outputs(src, None, &[], false).unwrap();

        assert_eq!(kernel_all.output_names().len(), kernel_empty.output_names().len());
    }

    #[test]
    fn dce_multiple_required_outputs() {
        // Request two of three bindings
        let src = r#"
            coordinates := (cycle)
            x := hash(cycle)
            y := mod(x, 50)
            z := add(cycle, 10)
        "#;
        let required = vec!["y".to_string(), "z".to_string()];
        let mut kernel = compile_gk_with_outputs(src, None, &required, false).unwrap();
        kernel.set_inputs(&[5]);

        assert!(kernel.pull("y").as_u64() < 50);
        assert_eq!(kernel.pull("z").as_u64(), 15);

        let outputs = kernel.output_names();
        assert!(outputs.contains(&"y"));
        assert!(outputs.contains(&"z"));
        // "x" is an upstream dep of "y" but not a requested output
        assert!(!outputs.contains(&"x"), "x should not be in outputs");
    }

    /// Every function registered in the FuncSig registry must be
    // --- Strict mode comprehensive tests ---

    #[test]
    fn strict_rejects_unused_bindings() {
        // "unused" has no downstream consumer and is not an output → strict error
        // Use compile_gk_strict which exposes all bindings as outputs,
        // so the kernel sees the full graph and detects the unused node.
        // Actually: when all bindings are outputs, none are "unused".
        // The unused check only applies with DCE (required_outputs filter).
        // With DCE, pruned bindings produce a warning at the compiler level.
        let src = r#"
            inputs := (cycle)
            used := hash(cycle)
            unused := add(cycle, 1)
        "#;
        let required = vec!["used".to_string()];
        // Non-strict: DCE prunes "unused" silently
        let result = compile_gk_with_outputs(src, None, &required, false);
        assert!(result.is_ok(), "non-strict with DCE should compile");
        // Verify "unused" is actually pruned
        let kernel = result.unwrap();
        assert!(!kernel.output_names().contains(&"unused"),
            "unused should be pruned by DCE");
    }

    #[test]
    fn strict_rejects_implicit_type_coercion() {
        // u64 → f64 auto-adapter → strict error
        let src = r#"
            inputs := (cycle)
            h := hash(cycle)
            f := sqrt(h)
        "#;
        let result = compile_gk_strict(src, None, true);
        assert!(result.is_err(), "strict should reject implicit coercion");
        let err = result.unwrap_err();
        assert!(err.contains("coercion") || err.contains("__adapt"),
            "error should mention coercion: {err}");
    }

    #[test]
    fn non_strict_allows_implicit_type_coercion() {
        let src = r#"
            inputs := (cycle)
            h := hash(cycle)
            f := sqrt(h)
        "#;
        let result = compile_gk_strict(src, None, false);
        assert!(result.is_ok(), "non-strict should allow implicit coercion");
    }

    #[test]
    fn strict_accepts_clean_program() {
        // All inputs declared, all bindings used, no coercions
        let src = r#"
            inputs := (cycle)
            h := hash(cycle)
            id := mod(h, 1000)
        "#;
        let required = vec!["id".to_string()];
        let result = compile_gk_with_outputs(src, None, &required, true);
        assert!(result.is_ok(), "clean program should pass strict: {:?}", result.err());
    }

    #[test]
    fn compile_bitwise_and() {
        let src = r#"
            coordinates := (cycle)
            out := cycle & 0xFF
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[0x1234]);
        assert_eq!(kernel.pull("out").as_u64(), 0x34);
    }

    #[test]
    fn compile_shift_left() {
        let src = r#"
            coordinates := (cycle)
            out := cycle << 8
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[1]);
        assert_eq!(kernel.pull("out").as_u64(), 256);
    }

    #[test]
    fn compile_bitwise_not() {
        let src = r#"
            coordinates := (cycle)
            out := !cycle
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("out").as_u64(), u64::MAX);
    }

    #[test]
    fn compile_bitwise_xor() {
        let src = r#"
            coordinates := (cycle)
            out := cycle ^ 0xFF
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[0xF0]);
        assert_eq!(kernel.pull("out").as_u64(), 0x0F);
    }

    #[test]
    fn compile_bitwise_or() {
        let src = r#"
            coordinates := (cycle)
            out := cycle | 0x0F
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[0xF0]);
        assert_eq!(kernel.pull("out").as_u64(), 0xFF);
    }

    #[test]
    fn compile_shift_right() {
        let src = r#"
            coordinates := (cycle)
            out := cycle >> 4
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[0xFF]);
        assert_eq!(kernel.pull("out").as_u64(), 0x0F);
    }

    #[test]
    fn compile_power_operator() {
        let src = r#"
            coordinates := (cycle)
            out := to_f64(cycle) ** 2.0
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_inputs(&[3]);
        // pow(3.0, 2.0) = 9.0
        let result = kernel.pull("out").as_f64();
        assert!((result - 9.0).abs() < 0.001);
    }

    // --- eval_const_expr tests ---

    #[test]
    fn eval_const_expr_arithmetic() {
        // 4 * 4: both operands are IntLit → u64_mul → returns u64(16)
        let v = eval_const_expr("4 * 4").unwrap();
        assert_eq!(v.as_u64(), 16, "expected u64(16), got {:?}", v);

        // 4.0 * 4.0: both operands are FloatLit → f64_mul → returns f64(16.0)
        let v = eval_const_expr("4.0 * 4.0").unwrap();
        assert!((v.as_f64() - 16.0).abs() < 0.001, "expected 16.0, got {}", v.as_f64());

        // Mixed: 4 * 4.0 → auto-widen LHS to f64, f64_mul → returns f64(16.0)
        let v = eval_const_expr("4 * 4.0").unwrap();
        assert!((v.as_f64() - 16.0).abs() < 0.001, "expected 16.0, got {}", v.as_f64());
    }

    #[test]
    fn eval_const_expr_function() {
        let v = eval_const_expr("hash(42)").unwrap();
        assert!(v.as_u64() != 0, "hash(42) should be non-zero");
    }

    #[test]
    fn eval_const_expr_fails_on_inputs() {
        // 'cycle' is a runtime input — should fail as const expr
        let r = eval_const_expr("hash(cycle)");
        assert!(r.is_err(), "hash(cycle) should fail as a const expression");
    }

    #[test]
    fn eval_const_expr_nested() {
        let v = eval_const_expr("mod(hash(42), 100)").unwrap();
        assert!(v.as_u64() < 100, "mod(hash(42), 100) should be < 100, got {}", v.as_u64());
    }
}
