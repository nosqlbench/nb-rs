// Copyright 2024-2026 nosqlbench contributors
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
use crate::node::GkNode;

use crate::nodes::hash::*;
use crate::nodes::arithmetic::*;
use crate::nodes::identity::*;
use crate::nodes::convert::*;
use crate::nodes::fixed::*;
use crate::nodes::datetime::*;
use crate::nodes::lerp::*;
use crate::nodes::encoding::*;
use crate::nodes::diagnostic::*;
use crate::nodes::weighted::*;
use crate::nodes::format::*;
use crate::nodes::json::*;
use crate::nodes::context::*;
use crate::nodes::noise::*;
use crate::nodes::regex::*;
use crate::sampling::icd::*;

use crate::dsl::error::DiagnosticReport;
use crate::dsl::registry;
use std::collections::HashSet;

/// Compile a `.gk` source string into a runtime kernel.
pub fn compile_gk(source: &str) -> Result<GkKernel, String> {
    compile_gk_with_path(source, None)
}

/// Compile with a source directory for module resolution.
///
/// When the compiler encounters an unknown function name, it searches
/// `source_dir` for `.gk` module files that export a matching binding.
pub fn compile_gk_with_path(source: &str, source_dir: Option<&Path>) -> Result<GkKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    compile_ast_with_path(&ast, source_dir)
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

/// Validate the AST: check function names, argument counts, wire
/// references, unused bindings, forward references.
///
/// Coordinate inference: if no `coordinates` declaration is present,
/// any referenced name that is not defined as a node output is
/// automatically promoted to a coordinate input. If a `coordinates`
/// declaration IS present, any unbound reference not in that list
/// is an error.
fn validate_ast(file: &GkFile, report: &mut DiagnosticReport) {
    let mut defined: HashSet<String> = HashSet::new();
    let mut referenced: HashSet<String> = HashSet::new();
    let mut coord_names: HashSet<String> = HashSet::new();
    let mut has_explicit_coords = false;
    let mut definition_order: Vec<(String, crate::dsl::lexer::Span)> = Vec::new();

    // First pass: collect explicit coordinates and all defined names
    for stmt in &file.statements {
        match stmt {
            Statement::Coordinates(names, _) => {
                has_explicit_coords = true;
                for n in names {
                    coord_names.insert(n.clone());
                    defined.insert(n.clone());
                }
            }
            Statement::InitBinding(b) => {
                defined.insert(b.name.clone());
                definition_order.push((b.name.clone(), b.span));
            }
            Statement::CycleBinding(b) => {
                for t in &b.targets {
                    defined.insert(t.clone());
                    definition_order.push((t.clone(), b.span));
                }
            }
        }
    }

    // Second pass: validate function calls and collect references
    for stmt in &file.statements {
        let expr = match stmt {
            Statement::Coordinates(_, _) => continue,
            Statement::InitBinding(b) => &b.value,
            Statement::CycleBinding(b) => &b.value,
        };
        validate_expr(expr, &defined, &coord_names, &mut referenced, report);
    }

    // Coordinate inference or validation
    if has_explicit_coords {
        // Explicit mode: unbound references are errors
        for name in &referenced {
            if !defined.contains(name) {
                report.error_with_hint(
                    crate::dsl::lexer::Span { line: 1, col: 1 },
                    format!("undefined wire reference: '{name}'"),
                    if coord_names.contains(name) {
                        // shouldn't happen — coord_names are in defined
                        "internal error".into()
                    } else if let Some(suggestion) = find_close_name(name, &defined) {
                        format!("did you mean '{suggestion}'?")
                    } else {
                        format!("'{name}' is not declared as a coordinate — add it to your coordinates declaration, or define it as a binding")
                    },
                );
            }
        }
    } else {
        // Infer mode: unbound references become coordinates
        let mut inferred: Vec<String> = referenced.iter()
            .filter(|name| !defined.contains(*name))
            .cloned()
            .collect();
        inferred.sort(); // deterministic order

        if inferred.is_empty() && !file.statements.is_empty() {
            report.error_with_hint(
                crate::dsl::lexer::Span { line: 1, col: 1 },
                "no coordinate inputs found",
                "reference at least one unbound name (e.g., 'cycle') to define the kernel's input",
            );
        } else {
            // Promote inferred names to coordinates
            for name in &inferred {
                coord_names.insert(name.clone());
                defined.insert(name.clone());
            }
        }
    }

    // Check for unused bindings (warning, not error)
    for (name, _span) in &definition_order {
        if !referenced.contains(name) && !coord_names.contains(name) {
            // It's an output variate — not consumed internally.
            // This is fine, don't warn. Outputs are consumed externally.
        }
    }

    // Check for forward references (warning)
    let mut seen_defs: HashSet<String> = coord_names.clone();
    for stmt in &file.statements {
        match stmt {
            Statement::Coordinates(_, _) => {}
            Statement::InitBinding(b) => {
                check_forward_refs(&b.value, &seen_defs, b.span, report);
                seen_defs.insert(b.name.clone());
            }
            Statement::CycleBinding(b) => {
                check_forward_refs(&b.value, &seen_defs, b.span, report);
                for t in &b.targets {
                    seen_defs.insert(t.clone());
                }
            }
        }
    }
}

fn validate_expr(
    expr: &Expr,
    defined: &HashSet<String>,
    coords: &HashSet<String>,
    referenced: &mut HashSet<String>,
    report: &mut DiagnosticReport,
) {
    match expr {
        Expr::Ident(name, _) => {
            referenced.insert(name.clone());
        }
        Expr::Call(call) => {
            // Validate function name
            if registry::lookup(&call.func).is_none() {
                let msg = format!("unknown function: '{}'", call.func);
                let hint = if let Some(suggestion) = registry::suggest_function(&call.func) {
                    format!("did you mean '{suggestion}'?")
                } else {
                    "check the function name or see the function reference".into()
                };
                report.error_with_hint(call.span, msg, hint);
            }

            // Validate arguments recursively
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) => e,
                    Arg::Named(_, e) => e,
                };
                validate_expr(inner, defined, coords, referenced, report);
            }
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems {
                validate_expr(e, defined, coords, referenced, report);
            }
        }
        Expr::StringLit(s, _) => {
            // Extract {name} references from string interpolation
            let chars: Vec<char> = s.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] == '{' {
                    i += 1;
                    let start = i;
                    while i < chars.len() && chars[i] != '}' { i += 1; }
                    let name: String = chars[start..i].iter().collect();
                    if !name.is_empty() {
                        referenced.insert(name);
                    }
                    i += 1;
                } else {
                    i += 1;
                }
            }
        }
        _ => {}
    }
}

/// Collect all identifier references from an expression tree (no validation).
fn collect_references(expr: &Expr, referenced: &mut HashSet<String>) {
    match expr {
        Expr::Ident(name, _) => { referenced.insert(name.clone()); }
        Expr::Call(call) => {
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) => e,
                    Arg::Named(_, e) => e,
                };
                collect_references(inner, referenced);
            }
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems { collect_references(e, referenced); }
        }
        Expr::StringLit(s, _) => {
            let chars: Vec<char> = s.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] == '{' {
                    i += 1;
                    let start = i;
                    while i < chars.len() && chars[i] != '}' { i += 1; }
                    let name: String = chars[start..i].iter().collect();
                    if !name.is_empty() { referenced.insert(name); }
                    i += 1;
                } else {
                    i += 1;
                }
            }
        }
        _ => {}
    }
}

fn check_forward_refs(
    expr: &Expr,
    seen: &HashSet<String>,
    stmt_span: crate::dsl::lexer::Span,
    report: &mut DiagnosticReport,
) {
    match expr {
        Expr::Ident(name, span) => {
            if !seen.contains(name) {
                report.warning_with_hint(
                    *span,
                    format!("forward reference: '{name}' is used before it is defined"),
                    "consider reordering bindings so definitions come before uses",
                );
            }
        }
        Expr::Call(call) => {
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) => e,
                    Arg::Named(_, e) => e,
                };
                check_forward_refs(inner, seen, stmt_span, report);
            }
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems {
                check_forward_refs(e, seen, stmt_span, report);
            }
        }
        _ => {}
    }
}

fn find_close_name(name: &str, defined: &HashSet<String>) -> Option<String> {
    let mut best: Option<(String, usize)> = None;
    for d in defined {
        let dist = simple_edit_distance(name, d);
        if dist <= 3 && (best.is_none() || dist < best.as_ref().unwrap().1) {
            best = Some((d.clone(), dist));
        }
    }
    best.map(|(n, _)| n)
}

fn simple_edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut m = vec![vec![0; b.len() + 1]; a.len() + 1];
    for i in 0..=a.len() { m[i][0] = i; }
    for j in 0..=b.len() { m[0][j] = j; }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let c = if a[i-1] == b[j-1] { 0 } else { 1 };
            m[i][j] = (m[i-1][j]+1).min(m[i][j-1]+1).min(m[i-1][j-1]+c);
        }
    }
    m[a.len()][b.len()]
}

/// Compile a parsed AST into a runtime kernel.
pub fn compile_ast(file: &GkFile) -> Result<GkKernel, String> {
    compile_ast_with_path(file, None)
}

/// Compile a parsed AST with module resolution from a source directory.
pub fn compile_ast_with_path(file: &GkFile, source_dir: Option<&Path>) -> Result<GkKernel, String> {
    let mut compiler = Compiler::new(source_dir.map(|p| p.to_path_buf()));
    compiler.compile(file)
}

struct Compiler {
    coord_names: Vec<String>,
    /// Track all named outputs so we can expose them.
    all_names: Vec<String>,
    /// Auto-generated node counter for desugared intermediates.
    anon_counter: usize,
    /// Directory for module resolution (search for .gk files).
    source_dir: Option<PathBuf>,
    /// Cache of already-resolved module ASTs: module_name → (inputs, statements).
    module_cache: std::collections::HashMap<String, ResolvedModule>,
}

/// A resolved GK module ready for inlining.
struct ResolvedModule {
    /// Inferred input names (unbound references), sorted.
    inputs: Vec<String>,
    /// The module's AST statements.
    statements: Vec<Statement>,
}

impl Compiler {
    fn new(source_dir: Option<PathBuf>) -> Self {
        Self {
            coord_names: Vec::new(),
            all_names: Vec::new(),
            anon_counter: 0,
            source_dir,
            module_cache: std::collections::HashMap::new(),
        }
    }

    fn anon_name(&mut self) -> String {
        let name = format!("__anon_{}", self.anon_counter);
        self.anon_counter += 1;
        name
    }

    /// Try to resolve a function call as a GK module and inline it.
    ///
    /// Returns Ok(true) if the module was found and inlined, Ok(false)
    /// if no module was found, or Err on resolution/inlining failure.
    fn try_inline_module(
        &mut self,
        asm: &mut GkAssembler,
        func_name: &str,
        caller_args: &[Arg],
        targets: &[String],
    ) -> Result<bool, String> {
        // Resolve the module (load + parse + cache)
        let module = match self.resolve_module(func_name)? {
            Some(m) => m,
            None => return Ok(false),
        };

        let module_inputs = module.inputs.clone();
        let module_stmts = module.statements.clone();

        // Build argument mapping: module input name → caller's wire/const
        // Named args map by name, positional args map by order of module_inputs
        let mut arg_map: std::collections::HashMap<String, Arg> = std::collections::HashMap::new();
        let mut positional_idx = 0;

        for arg in caller_args {
            match arg {
                Arg::Named(name, _) => {
                    arg_map.insert(name.clone(), arg.clone());
                }
                Arg::Positional(_) => {
                    if positional_idx < module_inputs.len() {
                        arg_map.insert(module_inputs[positional_idx].clone(), arg.clone());
                        positional_idx += 1;
                    }
                }
            }
        }

        // Generate a unique prefix for this module inlining
        let prefix = format!("__{func_name}_{}_", self.anon_counter);
        self.anon_counter += 1;

        // Inline each statement from the module, rewriting names
        for stmt in &module_stmts {
            match stmt {
                Statement::Coordinates(_, _) => {} // skip — coords handled by caller
                Statement::InitBinding(b) => {
                    let prefixed_name = format!("{prefix}{}", b.name);
                    let rewritten = self.rewrite_module_expr(
                        &b.value, &prefix, &module_inputs, &arg_map,
                    );
                    self.compile_binding(asm, &[prefixed_name], &rewritten)?;
                }
                Statement::CycleBinding(b) => {
                    let prefixed_targets: Vec<String> = b.targets.iter()
                        .map(|t| format!("{prefix}{t}"))
                        .collect();
                    let rewritten = self.rewrite_module_expr(
                        &b.value, &prefix, &module_inputs, &arg_map,
                    );
                    self.compile_binding(asm, &prefixed_targets, &rewritten)?;
                }
            }
        }

        // Wire module outputs to caller's targets.
        // Module outputs = its terminal bindings. We take the last binding
        // for each target name.
        let module_output_names: Vec<String> = module_stmts.iter().flat_map(|s| {
            match s {
                Statement::CycleBinding(b) => b.targets.clone(),
                Statement::InitBinding(b) => vec![b.name.clone()],
                _ => vec![],
            }
        }).collect();

        // Map caller targets to module outputs (by position)
        for (i, target) in targets.iter().enumerate() {
            let module_output = if i < module_output_names.len() {
                // Use the last output (terminal binding)
                &module_output_names[module_output_names.len() - 1 - (targets.len() - 1 - i)]
            } else {
                return Err(format!(
                    "module '{func_name}' has {} outputs but caller expects {}",
                    module_output_names.len(), targets.len()
                ));
            };
            let prefixed = format!("{prefix}{module_output}");
            // Add an identity node aliasing the prefixed output to the caller's target
            asm.add_node(
                target,
                Box::new(Identity::new()),
                vec![WireRef::node(&prefixed)],
            );
            self.all_names.push(target.clone());
        }

        Ok(true)
    }

    /// Rewrite an expression from a module, substituting input references
    /// with the caller's arguments and prefixing internal names.
    fn rewrite_module_expr(
        &self,
        expr: &Expr,
        prefix: &str,
        module_inputs: &[String],
        arg_map: &std::collections::HashMap<String, Arg>,
    ) -> Expr {
        match expr {
            Expr::Ident(name, span) => {
                if module_inputs.contains(name) {
                    // Replace with caller's argument
                    if let Some(arg) = arg_map.get(name) {
                        match arg {
                            Arg::Positional(e) | Arg::Named(_, e) => e.clone(),
                        }
                    } else {
                        // Unresolved module input — keep as-is (becomes
                        // a coordinate reference in the caller)
                        Expr::Ident(name.clone(), *span)
                    }
                } else {
                    // Internal name — prefix it
                    Expr::Ident(format!("{prefix}{name}"), *span)
                }
            }
            Expr::Call(call) => {
                let rewritten_args: Vec<Arg> = call.args.iter().map(|arg| {
                    match arg {
                        Arg::Positional(e) => Arg::Positional(
                            self.rewrite_module_expr(e, prefix, module_inputs, arg_map)
                        ),
                        Arg::Named(n, e) => Arg::Named(
                            n.clone(),
                            self.rewrite_module_expr(e, prefix, module_inputs, arg_map)
                        ),
                    }
                }).collect();
                Expr::Call(CallExpr {
                    func: call.func.clone(),
                    args: rewritten_args,
                    span: call.span,
                })
            }
            Expr::ArrayLit(elems, span) => {
                Expr::ArrayLit(
                    elems.iter().map(|e| self.rewrite_module_expr(e, prefix, module_inputs, arg_map)).collect(),
                    *span,
                )
            }
            other => other.clone(),
        }
    }

    /// Resolve a module by name: search for .gk files in source_dir.
    fn resolve_module(&mut self, name: &str) -> Result<Option<&ResolvedModule>, String> {
        if self.module_cache.contains_key(name) {
            return Ok(self.module_cache.get(name));
        }

        let source_dir = match &self.source_dir {
            Some(d) => d.clone(),
            None => return Ok(None),
        };

        // Strategy 1: look for <name>.gk in source_dir
        let module_path = source_dir.join(format!("{name}.gk"));
        if module_path.exists() {
            let source = std::fs::read_to_string(&module_path)
                .map_err(|e| format!("failed to read module '{}': {e}", module_path.display()))?;
            let resolved = Self::parse_module(&source, name)?;
            self.module_cache.insert(name.to_string(), resolved);
            return Ok(self.module_cache.get(name));
        }

        // Strategy 2: scan all .gk files in source_dir for a matching export
        if let Ok(entries) = std::fs::read_dir(&source_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("gk") {
                    let source = match std::fs::read_to_string(&path) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    // Check if any binding in this file matches the function name
                    if let Ok(resolved) = Self::parse_module(&source, name) {
                        self.module_cache.insert(name.to_string(), resolved);
                        return Ok(self.module_cache.get(name));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Parse a .gk source and extract a module definition.
    ///
    /// Infers inputs (unbound references) and validates that the module
    /// has at least one output binding.
    fn parse_module(source: &str, _expected_name: &str) -> Result<ResolvedModule, String> {
        let tokens = lexer::lex(source)?;
        let ast = parser::parse(tokens)?;

        // Collect defined names and referenced names
        let mut defined: HashSet<String> = HashSet::new();
        let mut referenced: HashSet<String> = HashSet::new();

        for stmt in &ast.statements {
            match stmt {
                Statement::Coordinates(names, _) => {
                    for n in names { defined.insert(n.clone()); }
                }
                Statement::InitBinding(b) => { defined.insert(b.name.clone()); }
                Statement::CycleBinding(b) => {
                    for t in &b.targets { defined.insert(t.clone()); }
                }
            }
        }

        for stmt in &ast.statements {
            let expr = match stmt {
                Statement::Coordinates(_, _) => continue,
                Statement::InitBinding(b) => &b.value,
                Statement::CycleBinding(b) => &b.value,
            };
            collect_references(expr, &mut referenced);
        }

        let mut inputs: Vec<String> = referenced.into_iter()
            .filter(|name| !defined.contains(name))
            .collect();
        inputs.sort();

        if inputs.is_empty() && ast.statements.is_empty() {
            return Err("module has no statements".into());
        }

        Ok(ResolvedModule {
            inputs,
            statements: ast.statements,
        })
    }

    fn compile(&mut self, file: &GkFile) -> Result<GkKernel, String> {
        // First pass: find explicit coordinates
        for stmt in &file.statements {
            if let Statement::Coordinates(names, _) = stmt {
                self.coord_names = names.clone();
            }
        }

        // If no explicit coordinates, infer from unbound references
        if self.coord_names.is_empty() {
            let defined: HashSet<String> = file.statements.iter().flat_map(|stmt| {
                match stmt {
                    Statement::InitBinding(b) => vec![b.name.clone()],
                    Statement::CycleBinding(b) => b.targets.clone(),
                    Statement::Coordinates(_, _) => vec![],
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::Coordinates(_, _) => continue,
                    Statement::InitBinding(b) => &b.value,
                    Statement::CycleBinding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort(); // deterministic order
            self.coord_names = inferred;
        }

        if self.coord_names.is_empty() {
            return Err("no coordinate inputs found — reference at least one unbound name (e.g., 'cycle')".into());
        }

        let mut asm = GkAssembler::new(self.coord_names.clone());

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
            }
        }

        // Expose all top-level named bindings as outputs
        for name in &self.all_names {
            asm.add_output(name, WireRef::node(name));
        }

        asm.compile().map_err(|e| format!("{e}"))
    }

    fn compile_binding(
        &mut self,
        asm: &mut GkAssembler,
        targets: &[String],
        value: &Expr,
    ) -> Result<(), String> {
        match value {
            Expr::Call(call) => {
                let node_name = if targets.len() == 1 {
                    targets[0].clone()
                } else {
                    // For destructuring, the node gets the first target's name
                    // as a base, and we wire the output ports separately.
                    targets[0].clone()
                };

                // Resolve arguments to wire refs
                let mut wire_refs = Vec::new();
                let mut const_args = Vec::new();

                for arg in &call.args {
                    let (expr, _name) = match arg {
                        Arg::Positional(e) => (e, None),
                        Arg::Named(n, e) => (e, Some(n.as_str())),
                    };
                    match expr {
                        Expr::Ident(id, _) => {
                            // Is it a coordinate?
                            if self.coord_names.contains(id) {
                                wire_refs.push(WireRef::coord(id));
                            } else {
                                wire_refs.push(WireRef::node(id));
                            }
                        }
                        Expr::IntLit(v, _) => {
                            const_args.push(ConstArg::Int(*v));
                            // Constants don't become wire refs — they're
                            // baked into the node constructor.
                        }
                        Expr::FloatLit(v, _) => {
                            const_args.push(ConstArg::Float(*v));
                        }
                        Expr::StringLit(s, _) => {
                            const_args.push(ConstArg::Str(s.clone()));
                        }
                        Expr::Call(inner) => {
                            // Inline nesting: desugar to an anonymous node
                            let anon = self.anon_name();
                            self.compile_binding(asm, &[anon.clone()], &Expr::Call(inner.clone()))?;
                            wire_refs.push(WireRef::node(anon));
                        }
                        Expr::ArrayLit(elems, _) => {
                            let floats: Vec<f64> = elems.iter().map(|e| match e {
                                Expr::FloatLit(v, _) => *v,
                                Expr::IntLit(v, _) => *v as f64,
                                _ => 0.0,
                            }).collect();
                            const_args.push(ConstArg::FloatArray(floats));
                        }
                    }
                }

                let node = match build_node(&call.func, &wire_refs, &const_args) {
                    Ok(n) => n,
                    Err(e) if e.contains("unknown function") => {
                        // Try module resolution before giving up
                        if self.try_inline_module(asm, &call.func, &call.args, targets)? {
                            return Ok(());
                        }
                        return Err(e);
                    }
                    Err(e) => return Err(e),
                };

                if targets.len() == 1 {
                    asm.add_node(&node_name, node, wire_refs);
                    self.all_names.push(node_name);
                } else {
                    // Multi-output: add the node under an internal name,
                    // then add identity nodes for each destructured target
                    // that reference specific output ports.
                    let internal_name = format!("__destruct_{}", self.anon_counter);
                    self.anon_counter += 1;
                    asm.add_node(&internal_name, node, wire_refs);

                    for (i, target) in targets.iter().enumerate() {
                        asm.add_node(
                            target,
                            Box::new(Identity::new()),
                            vec![WireRef::node_port(&internal_name, i)],
                        );
                        self.all_names.push(target.clone());
                    }
                }
            }
            Expr::StringLit(s, _) => {
                // String interpolation: "{code}-{seq}" desugars to a
                // string template node. For now, just check if it has
                // bind points and create appropriate wiring.
                if s.contains('{') && s.contains('}') {
                    // Parse bind points
                    let mut bind_names = Vec::new();
                    let mut i = 0;
                    let chars: Vec<char> = s.chars().collect();
                    while i < chars.len() {
                        if chars[i] == '{' {
                            i += 1;
                            let start = i;
                            while i < chars.len() && chars[i] != '}' { i += 1; }
                            let name: String = chars[start..i].iter().collect();
                            if !bind_names.contains(&name) {
                                bind_names.push(name);
                            }
                            i += 1;
                        } else {
                            i += 1;
                        }
                    }

                    // For now, use a Printf node with U64ToString adapters
                    // This is a simplified desugar — a full implementation
                    // would handle mixed types.
                    let wire_refs: Vec<WireRef> = bind_names.iter()
                        .map(|n| {
                            if self.coord_names.contains(n) {
                                WireRef::coord(n)
                            } else {
                                WireRef::node(n)
                            }
                        })
                        .collect();

                    // Build format string by replacing {name} with {}
                    let _fmt = s.replace(|_: char| false, ""); // keep as-is for now
                    // Actually, we need to replace each {name} with {} for Printf
                    let mut fmt_str = String::new();
                    let mut ci = 0;
                    while ci < chars.len() {
                        if chars[ci] == '{' {
                            fmt_str.push_str("{}");
                            while ci < chars.len() && chars[ci] != '}' { ci += 1; }
                            ci += 1;
                        } else {
                            fmt_str.push(chars[ci]);
                            ci += 1;
                        }
                    }

                    // All inputs treated as Str via auto-adapters
                    let input_types = vec![crate::node::PortType::Str; bind_names.len()];
                    let node = Box::new(Printf::new(&fmt_str, &input_types));
                    let name = &targets[0];
                    asm.add_node(name, node, wire_refs);
                    self.all_names.push(name.clone());
                } else {
                    // Plain string constant
                    let name = &targets[0];
                    asm.add_node(name, Box::new(ConstStr::new(s.clone())), vec![]);
                    self.all_names.push(name.clone());
                }
            }
            Expr::Ident(id, _) => {
                // Simple alias: target := source
                let name = &targets[0];
                let wire = if self.coord_names.contains(id) {
                    WireRef::coord(id)
                } else {
                    WireRef::node(id)
                };
                asm.add_node(name, Box::new(Identity::new()), vec![wire]);
                self.all_names.push(name.clone());
            }
            Expr::IntLit(v, _) => {
                let name = &targets[0];
                asm.add_node(name, Box::new(ConstU64::new(*v)), vec![]);
                self.all_names.push(name.clone());
            }
            Expr::FloatLit(v, _) => {
                let name = &targets[0];
                asm.add_node(name, Box::new(ConstF64::new(*v)), vec![]);
                self.all_names.push(name.clone());
            }
            _ => {
                return Err(format!("unsupported expression in binding"));
            }
        }
        Ok(())
    }
}

/// Constant arguments extracted from the AST.
enum ConstArg {
    Int(u64),
    Float(f64),
    Str(String),
    FloatArray(#[allow(dead_code)] Vec<f64>),
}

impl ConstArg {
    fn as_u64(&self) -> u64 {
        match self { ConstArg::Int(v) => *v, _ => 0 }
    }
    fn as_f64(&self) -> f64 {
        match self { ConstArg::Float(v) => *v, ConstArg::Int(v) => *v as f64, _ => 0.0 }
    }
    fn as_str(&self) -> &str {
        match self { ConstArg::Str(s) => s, _ => "" }
    }
    #[allow(dead_code)]
    fn as_float_array(&self) -> &[f64] {
        match self { ConstArg::FloatArray(v) => v, _ => &[] }
    }
}

/// Build a node from a function name and its arguments.
///
/// `wires` are the cycle-time wire inputs.
/// `consts` are the assembly-time constant arguments.
fn build_node(
    func: &str,
    _wires: &[WireRef],
    consts: &[ConstArg],
) -> Result<Box<dyn GkNode>, String> {
    match func {
        // --- Hashing ---
        "hash" => Ok(Box::new(Hash64::new())),

        // --- Arithmetic ---
        "add" => Ok(Box::new(AddU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(0)))),
        "mul" => Ok(Box::new(MulU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(1)))),
        "div" => Ok(Box::new(DivU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(1)))),
        "mod" => Ok(Box::new(ModU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(1)))),
        "clamp" => Ok(Box::new(ClampU64::new(
            consts.get(0).map(|c| c.as_u64()).unwrap_or(0),
            consts.get(1).map(|c| c.as_u64()).unwrap_or(u64::MAX),
        ))),
        "interleave" => Ok(Box::new(Interleave::new())),
        "mixed_radix" => {
            let radixes: Vec<u64> = consts.iter().map(|c| c.as_u64()).collect();
            Ok(Box::new(MixedRadix::new(radixes)))
        }

        // --- Identity & constants ---
        "identity" => Ok(Box::new(Identity::new())),

        // --- Conversions ---
        "unit_interval" => Ok(Box::new(UnitInterval::new())),
        "clamp_f64" => Ok(Box::new(ClampF64::new(
            consts.get(0).map(|c| c.as_f64()).unwrap_or(f64::MIN),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(f64::MAX),
        ))),
        "f64_to_u64" => Ok(Box::new(F64ToU64::new())),
        "round_to_u64" => Ok(Box::new(RoundToU64::new())),
        "floor_to_u64" => Ok(Box::new(FloorToU64::new())),
        "ceil_to_u64" => Ok(Box::new(CeilToU64::new())),

        // --- Distribution sampling ---
        "lut_sample" | "icd_normal" => {
            // For now, IcdSample convenience
            Ok(Box::new(IcdSample::normal(
                consts.get(0).map(|c| c.as_f64()).unwrap_or(0.0),
                consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
            )))
        }
        "icd_exponential" | "dist_exponential" => {
            Ok(Box::new(IcdSample::exponential(
                consts.get(0).map(|c| c.as_f64()).unwrap_or(1.0),
            )))
        }
        "dist_normal" => {
            Ok(Box::new(IcdSample::normal(
                consts.get(0).map(|c| c.as_f64()).unwrap_or(0.0),
                consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
            )))
        }

        // --- Datetime ---
        "epoch_scale" => Ok(Box::new(EpochScale::new(consts.first().map(|c| c.as_u64()).unwrap_or(1)))),
        "epoch_offset" => Ok(Box::new(EpochOffset::new(consts.first().map(|c| c.as_u64()).unwrap_or(0)))),
        "to_timestamp" => Ok(Box::new(ToTimestamp::new())),

        // --- Encoding ---
        "html_encode" => Ok(Box::new(HtmlEncode::new())),
        "html_decode" => Ok(Box::new(HtmlDecode::new())),
        "url_encode" => Ok(Box::new(UrlEncode::new())),
        "url_decode" => Ok(Box::new(UrlDecode::new())),

        // --- Lerp ---
        "lerp" => Ok(Box::new(LerpConst::new(
            consts.get(0).map(|c| c.as_f64()).unwrap_or(0.0),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
        ))),
        "scale_range" => Ok(Box::new(ScaleRange::new(
            consts.get(0).map(|c| c.as_f64()).unwrap_or(0.0),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
        ))),
        "quantize" => Ok(Box::new(Quantize::new(
            consts.get(0).map(|c| c.as_f64()).unwrap_or(1.0),
        ))),

        // --- Weighted ---
        "weighted_strings" => Ok(Box::new(WeightedStrings::new(
            consts.get(0).map(|c| c.as_str()).unwrap_or(""),
        ))),
        "weighted_u64" => Ok(Box::new(WeightedU64::new(
            consts.get(0).map(|c| c.as_str()).unwrap_or(""),
        ))),

        // --- Diagnostic ---
        "type_of" => Ok(Box::new(TypeOf::for_u64())),
        "inspect" => Ok(Box::new(Inspect::u64("inspect"))),

        // --- Context ---
        "current_epoch_millis" => Ok(Box::new(CurrentEpochMillis::new())),
        "counter" => Ok(Box::new(Counter::new())),

        // --- JSON ---
        "to_json" => Ok(Box::new(ToJson::new(crate::node::PortType::U64))),
        "json_to_str" => Ok(Box::new(JsonToStr::new())),
        "escape_json" => Ok(Box::new(EscapeJson::new())),

        // --- Noise ---
        "perlin_1d" => Ok(Box::new(Perlin1D::new(
            consts.get(0).map(|c| c.as_u64()).unwrap_or(0),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(0.01),
        ))),
        "perlin_2d" => Ok(Box::new(Perlin2D::new(
            consts.get(0).map(|c| c.as_u64()).unwrap_or(0),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(0.01),
        ))),

        // --- Regex ---
        "regex_replace" => Ok(Box::new(RegexReplace::new(
            consts.get(0).map(|c| c.as_str()).unwrap_or(""),
            consts.get(1).map(|c| c.as_str()).unwrap_or(""),
        ))),

        _ => Err(format!("unknown function: {func}")),
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
        kernel.set_coordinates(&[42]);
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
        kernel.set_coordinates(&[42]);
        assert!(kernel.pull("result").as_u64() < 100);
    }

    #[test]
    fn compile_deterministic() {
        let src = r#"
            coordinates := (cycle)
            h := hash(cycle)
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_coordinates(&[42]);
        let v1 = kernel.pull("h").as_u64();
        kernel.set_coordinates(&[42]);
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
        kernel.set_coordinates(&[4_201_337]);
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
        kernel.set_coordinates(&[0]);
        assert_eq!(kernel.pull("label").as_str(), "hello world");
    }

    #[test]
    fn compile_int_constant() {
        let src = r#"
            coordinates := (cycle)
            base := 1710000000000
        "#;
        let mut kernel = compile_gk(src).unwrap();
        kernel.set_coordinates(&[0]);
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
        kernel.set_coordinates(&[1]);
        assert!(kernel.pull("h").as_u64() != 0);
    }

    // --- Diagnostic tests ---

    #[test]
    fn error_unknown_function() {
        let src = "coordinates := (cycle)\nresult := foobar(cycle)";
        let (result, report) = compile_gk_checked(src);
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
        assert_eq!(kernel.coord_names(), &["cycle"]);
        kernel.set_coordinates(&[42]);
        let h = kernel.pull("h").as_u64();
        assert_ne!(h, 42); // hashed, not identity
    }

    #[test]
    fn inferred_multi_coordinates() {
        // Multiple unbound names become multiple coordinate inputs (sorted)
        let src = "h := hash(interleave(row, col))";
        let mut kernel = compile_gk(src).unwrap();
        assert_eq!(kernel.coord_names(), &["col", "row"]); // alphabetically sorted
        kernel.set_coordinates(&[10, 20]);
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
}
