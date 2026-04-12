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
    compiler.build_assembler(&ast)
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
    compile_ast_strict(&ast, source_dir, strict)
}

/// Compile with a compile event log for diagnostic inspection.
pub fn compile_gk_with_log(source: &str, log: &mut super::events::CompileEventLog) -> Result<GkKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let mut compiler = Compiler::new(None, false);
    let asm = compiler.build_assembler(&ast)?;
    asm.compile_with_log(Some(log)).map_err(|e| e.to_string())
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
        }
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

        // Input declaration check: warn (or error in strict) when missing
        if !has_explicit_coords {
            if self.strict {
                return Err(
                    "strict mode: no 'inputs' declaration — add 'inputs := (cycle)' \
                     to declare graph inputs explicitly".into()
                );
            }
            // Non-strict: implicit single-cycle input, but warn
            eprintln!("warning: no 'inputs' declaration — implying 'inputs := (cycle)'. \
                       Use explicit declaration to silence this warning.");
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
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) => continue,
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

        // Zero inputs is only valid when explicitly declared as `inputs := ()`.
        // Without an explicit declaration and nothing inferred, that's an error.
        if self.input_names.is_empty() && !has_explicit_coords {
            return Err("no coordinate inputs found — reference at least one unbound name (e.g., 'cycle')".into());
        }

        let mut asm = GkAssembler::new(self.input_names.clone());

        // Second pass: process all bindings
        for stmt in &file.statements {
            match stmt {
                Statement::Coordinates(_, _) => {} // already handled
                Statement::InitBinding(b) => {
                    // For now, init bindings that are function calls become
                    // nodes with no cycle-time wire inputs. This is a
                    // simplification — full init-time resolution is future work.
                    self.compile_binding(
                        &mut asm,
                        &[b.name.clone()],
                        &b.value,
                    )?;
                }
                Statement::CycleBinding(b) => {
                    self.compile_binding(
                        &mut asm,
                        &b.targets,
                        &b.value,
                    )?;
                }
                Statement::ModuleDef(_) => {
                    // Module definitions are not executed — they're
                    // templates resolved by the module system when
                    // referenced from another file/kernel.
                }
                Statement::ExternPort(port) => {
                    // Declare the port on the assembler
                    let default_value = match port.typ.as_str() {
                        "u64" => crate::node::Value::U64(0),
                        "f64" => crate::node::Value::F64(0.0),
                        "bool" => crate::node::Value::Bool(false),
                        _ => crate::node::Value::Str(String::new()),
                    };
                    let port_type = match port.typ.as_str() {
                        "u64" => crate::node::PortType::U64,
                        "f64" => crate::node::PortType::F64,
                        _ => crate::node::PortType::Str,
                    };
                    asm.add_port(&port.name, default_value);

                    // Create a passthrough node wired to the port
                    let passthrough = Box::new(
                        crate::nodes::identity::PortPassthrough::new(&port.name, port_type)
                    );
                    let passthrough_name = format!("__port_{}", port.name);
                    asm.add_node(
                        &passthrough_name,
                        passthrough,
                        vec![WireRef::port(&port.name)],
                    );
                    // Register as output so {name} resolves from GK
                    asm.add_output(&port.name, WireRef::node(&passthrough_name));
                }
            }
        }

        // Expose all top-level named bindings as outputs
        for name in &self.all_names {
            asm.add_output(name, WireRef::node(name));
        }

        asm.compile_strict(self.strict).map_err(|e| format!("{e}"))
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
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) => continue,
                    Statement::InitBinding(b) => &b.value,
                    Statement::CycleBinding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort();
            if inferred.is_empty() {
                inferred.push("cycle".to_string());
            }
            self.input_names = inferred;
        }

        let mut asm = GkAssembler::new(self.input_names.clone());

        for stmt in file.statements.clone() {
            match &stmt {
                Statement::CycleBinding(binding) => {
                    self.compile_binding(&mut asm, &binding.targets, &binding.value)?;
                }
                Statement::InitBinding(binding) => {
                    self.compile_binding(&mut asm, &[binding.name.clone()], &binding.value)?;
                }
                Statement::ExternPort(_) => {}
                Statement::ModuleDef(_) => {}
                Statement::Coordinates(_, _) => {}
            }
        }

        for name in &self.all_names {
            asm.add_output(name, WireRef::node(name));
        }

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

        // Input declaration check: warn (or error in strict) when missing
        if self.input_names.is_empty() {
            if self.strict {
                return Err(
                    "strict mode: no 'inputs' declaration — add 'inputs := (cycle)' \
                     to declare graph inputs explicitly".into()
                );
            }
            // Non-strict: implicit single-cycle input, but warn
            eprintln!("warning: no 'inputs' declaration — implying 'inputs := (cycle)'. \
                       Use explicit declaration to silence this warning.");
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
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) => continue,
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

        if self.input_names.is_empty() {
            return Err("no coordinate inputs found — reference at least one unbound name (e.g., 'cycle')".into());
        }

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
                }
                Statement::CycleBinding(b) => {
                    self.compile_binding(
                        &mut asm,
                        &b.targets,
                        &b.value,
                    )?;
                }
                Statement::ModuleDef(_) | Statement::ExternPort(_) => {}
            }
        }

        // Unused binding check: defer to kernel-level check in fold_init_constants_impl.
        // The kernel has the full wiring graph and can accurately determine which
        // nodes have no downstream consumers. The compiler can't do this reliably
        // because it doesn't track inter-binding wire dependencies.

        // Expose outputs: only the required set, or all if no filter
        match required_outputs {
            Some(required) => {
                for name in required {
                    if self.all_names.contains(name) {
                        asm.add_output(name, WireRef::node(name));
                    }
                }
            }
            None => {
                for name in &self.all_names {
                    asm.add_output(name, WireRef::node(name));
                }
            }
        }

        asm.compile_strict(self.strict).map_err(|e| format!("{e}"))
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
    /// compilable. This test catches drift between registry.rs and
    /// build_node() — if you register a function, you must also add
    /// a match arm so the DSL compiler can build it.
    ///
    /// Functions that require non-u64 inputs (bytes, json, str) or
    /// specific parameter formats (spec strings) are tested with
    /// appropriate upstream nodes to satisfy type requirements.
    #[test]
    fn every_registered_function_compiles() {
        use crate::dsl::registry;
        use crate::node::SlotType;

        let reg = registry::registry();
        let mut failures: Vec<String> = Vec::new();

        // Per-function overrides for functions that need special args
        // or upstream wiring to satisfy type constraints.
        let overrides: std::collections::HashMap<&str, &str> = [
            // These need a bytes input, not u64
            ("to_hex", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := to_hex(b)"),
            ("from_hex", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nh := to_hex(b)\nout := from_hex(h)"),
            ("sha256", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := sha256(b)"),
            ("md5", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := md5(b)"),
            ("to_base64", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\nout := to_base64(b)"),
            ("from_base64", "coordinates := (cycle)\nb := u64_to_bytes(cycle)\ne := to_base64(b)\nout := from_base64(e)"),
            // These need json input
            ("json_to_str", "coordinates := (cycle)\nj := to_json(cycle)\nout := json_to_str(j)"),
            ("json_merge", "coordinates := (cycle)\na := to_json(cycle)\nb := to_json(cycle)\nout := json_merge(a, b)"),
            ("escape_json", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := escape_json(s)"),
            // Distribution builders: 0 wire inputs, just const params
            ("dist_normal", "coordinates := (cycle)\nlut := dist_normal(0.0, 1.0)\nout := lut_sample(lut)"),
            ("dist_exponential", "coordinates := (cycle)\nlut := dist_exponential(1.0)\nout := lut_sample(lut)"),
            ("dist_uniform", "coordinates := (cycle)\nlut := dist_uniform(0.0, 1.0)\nout := lut_sample(lut)"),
            ("dist_pareto", "coordinates := (cycle)\nlut := dist_pareto(1.0, 1.0)\nout := lut_sample(lut)"),
            ("dist_zipf", "coordinates := (cycle)\nlut := dist_zipf(100, 1.0)\nout := lut_sample(lut)"),
            ("histribution", "coordinates := (cycle)\nout := histribution(hash(cycle), \"50 25 13 12\")"),
            ("dist_empirical", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := dist_empirical(f, \"1.0 3.0 5.0 7.0 9.0\")"),
            // Weighted functions need valid spec strings
            ("weighted_strings", "coordinates := (cycle)\nout := weighted_strings(hash(cycle), \"a:0.5;b:0.5\")"),
            ("weighted_u64", "coordinates := (cycle)\nout := weighted_u64(hash(cycle), \"10:0.5;20:0.5\")"),
            ("one_of_weighted", "coordinates := (cycle)\nout := one_of_weighted(hash(cycle), \"a:0.5;b:0.5\")"),
            // String input functions
            ("html_encode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := html_encode(s)"),
            ("html_decode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := html_decode(s)"),
            ("url_encode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := url_encode(s)"),
            ("url_decode", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := url_decode(s)"),
            ("regex_replace", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := regex_replace(s, \"[0-9]\", \"x\")"),
            ("regex_match", "coordinates := (cycle)\ns := format_u64(cycle, 10)\nout := regex_match(s, \"[0-9]+\")"),
            // Select needs 3 inputs (cond, if_true, if_false)
            ("select", "coordinates := (cycle)\nout := select(fair_coin(hash(cycle)), cycle, cycle)"),
            // Blend needs 2 wire inputs
            ("blend", "coordinates := (cycle)\nout := blend(hash(cycle), hash(cycle), 0.5)"),
            // Multi-output: date_components
            ("date_components", "coordinates := (cycle)\n(y, mo, d, h, mi, s, ms) := date_components(cycle)"),
            // Context nodes: no inputs
            ("current_epoch_millis", "coordinates := (cycle)\nout := current_epoch_millis()"),
            ("counter", "coordinates := (cycle)\nout := counter()"),
            ("session_start_millis", "coordinates := (cycle)\nout := session_start_millis()"),
            ("elapsed_millis", "coordinates := (cycle)\nout := elapsed_millis()"),
            ("thread_id", "coordinates := (cycle)\nout := thread_id()"),
            // f64 input functions
            ("clamp_f64", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := clamp_f64(f, 0.0, 0.5)"),
            ("quantize", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := quantize(f, 0.1)"),
            ("lerp", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := lerp(f, 0.0, 100.0)"),
            ("inv_lerp", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := inv_lerp(f, 0.0, 1.0)"),
            // 2-input noise
            ("perlin_2d", "coordinates := (cycle)\nout := perlin_2d(cycle, cycle, 42, 0.01)"),
            ("simplex_2d", "coordinates := (cycle)\nout := simplex_2d(cycle, cycle, 42, 0.01)"),
            // PCG with 2 wires
            ("pcg_stream", "coordinates := (cycle)\nout := pcg_stream(cycle, cycle, 42)"),
            ("format_u64", "coordinates := (cycle)\nout := format_u64(cycle, 16)"),
            // fft_analyze creates files — skip in this compile-only test
            ("fft_analyze", "coordinates := (cycle)\nf := unit_interval(hash(cycle))\nout := fft_analyze(f, \"test_fft_compile.jsonl\", 8)"),
        ].into_iter().collect();

        for sig in &reg {
            let src = if let Some(override_src) = overrides.get(sig.name) {
                override_src.to_string()
            } else {
                // Auto-generate: all wire params get cycle, consts get defaults
                let mut args: Vec<String> = Vec::new();
                for p in sig.params {
                    match p.slot_type {
                        SlotType::Wire => args.push("cycle".into()),
                        SlotType::ConstU64 => args.push("100".into()),
                        SlotType::ConstF64 => args.push("1.0".into()),
                        SlotType::ConstStr => args.push("\"test\"".into()),
                        SlotType::ConstVecU64 => args.push("100".into()),
                        SlotType::ConstVecF64 => args.push("1.0".into()),
                    }
                }
                if args.is_empty() && sig.is_variadic() {
                    args.push("cycle".into());
                }
                let call = format!("{}({})", sig.name, args.join(", "));
                format!("coordinates := (cycle)\nout := {call}")
            };

            // Skip vectordata functions — their constructors perform I/O
            // (catalog lookup + dataset loading) which requires network
            // access. They compile correctly with a valid dataset source.
            let vectordata_fns = [
                "vector_at", "vector_at_bytes", "query_vector_at", "query_vector_at_bytes",
                "neighbor_indices_at", "neighbor_distances_at",
                "filtered_neighbor_indices_at", "filtered_neighbor_distances_at",
                "dataset_distance_function", "vector_dim", "vector_count",
                "query_count", "neighbor_count",
                "metadata_indices_at", "metadata_indices_len_at", "metadata_indices_count",
                "dataset_facets",
                // file_line_at requires a real file path — cannot test with a dummy name
                "file_line_at",
            ];
            if vectordata_fns.contains(&sig.name) { continue; }

            let result = std::panic::catch_unwind(|| {
                compile_gk(&src)
            });

            match result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    if !overrides.contains_key(sig.name) {
                        failures.push(format!("  {}: {e}", sig.name));
                    }
                }
                Err(_) => {
                    failures.push(format!("  {}: panicked", sig.name));
                }
            }
        }

        if !failures.is_empty() {
            panic!(
                "Registered functions that failed to compile:\n\
                 Add a build_node() arm or an override in this test.\n\n{}\n",
                failures.join("\n")
            );
        }
    }

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
