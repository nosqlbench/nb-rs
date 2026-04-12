// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Module resolution subsystem for the GK DSL compiler.
//!
//! Handles locating, parsing, and caching `.gk` module files so the
//! compiler can inline them at call sites.  Resolution order:
//!
//! 1. In-process cache (already-resolved modules)
//! 2. `<name>.gk` in the workload-local `source_dir`
//! 3. Any `.gk` file in `source_dir` that exports a binding named `<name>`
//! 4. The same two searches repeated for each `--gk-lib` path
//! 5. The embedded standard library

use std::collections::HashSet;

use crate::assembly::{GkAssembler, WireRef};
use crate::dsl::ast::*;
use crate::dsl::lexer;
use crate::dsl::parser;
use crate::dsl::validate::collect_references;

use super::compile::{Compiler, STDLIB_MODULES};

/// A resolved GK module ready for inlining.
pub(super) struct ResolvedModule {
    /// Input parameter names (from formal signature or inferred).
    pub(super) inputs: Vec<String>,
    /// Input parameter types (from formal signature; empty if inferred).
    pub(super) input_types: Vec<Option<String>>,
    /// Output binding names (from formal signature or last binding).
    pub(super) outputs: Vec<String>,
    /// Output types (from formal signature; empty if inferred).
    /// Reserved for future strict-mode type checking of downstream consumers.
    #[allow(dead_code)]
    pub(super) output_types: Vec<Option<String>>,
    /// Whether this module has a formal typed signature.
    pub(super) is_formal: bool,
    /// The module's AST statements.
    pub(super) statements: Vec<Statement>,
}

impl Compiler {
    /// Generate a fresh anonymous node name for desugared intermediates.
    pub(super) fn anon_name(&mut self) -> String {
        let name = format!("__anon_{}", self.anon_counter);
        self.anon_counter += 1;
        name
    }

    /// Try to resolve a function call as a GK module and inline it.
    ///
    /// Returns `Ok(true)` if the module was found and inlined, `Ok(false)`
    /// if no module was found, or `Err` on resolution/inlining failure.
    pub(super) fn try_inline_module(
        &mut self,
        asm: &mut GkAssembler,
        func_name: &str,
        caller_args: &[Arg],
        targets: &[String],
    ) -> Result<bool, String> {
        use crate::dsl::validate::{literal_type, types_compatible};

        // Resolve the module (load + parse + cache)
        let module = match self.resolve_module(func_name)? {
            Some(m) => m,
            None => return Ok(false),
        };

        let module_inputs = module.inputs.clone();
        let module_input_types = module.input_types.clone();
        let module_outputs = module.outputs.clone();
        let module_is_formal = module.is_formal;
        let module_stmts = module.statements.clone();

        // Strict mode: require all module arguments to be named
        if self.strict {
            for arg in caller_args {
                if matches!(arg, Arg::Positional(_)) {
                    return Err(format!(
                        "strict mode: module '{}' called with positional args — use named args (e.g., param_name: value)",
                        func_name
                    ));
                }
            }
        }

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

        // Strict mode: require all module inputs to be provided by the caller
        if self.strict {
            for input_name in &module_inputs {
                if !arg_map.contains_key(input_name) {
                    return Err(format!(
                        "strict mode: module '{}' input '{}' not provided — add '{}: <value>'",
                        func_name, input_name, input_name
                    ));
                }
            }
        }

        // --- Type validation for formal modules ---
        if module_is_formal {
            // Arity check
            let provided = arg_map.len();
            let expected = module_inputs.len();
            if provided != expected {
                return Err(format!(
                    "module '{}' expects {} arguments, got {}",
                    func_name, expected, provided
                ));
            }

            // Named argument validation: all names must match declared params
            for arg_name in arg_map.keys() {
                if !module_inputs.contains(arg_name) {
                    return Err(format!(
                        "module '{}' has no parameter named '{}' — available: {}",
                        func_name, arg_name, module_inputs.join(", ")
                    ));
                }
            }

            // Missing parameter check
            for input_name in &module_inputs {
                if !arg_map.contains_key(input_name) {
                    return Err(format!(
                        "module '{}' parameter '{}' not provided",
                        func_name, input_name
                    ));
                }
            }

            // Literal type checking against declared param types
            for (i, input_name) in module_inputs.iter().enumerate() {
                if let Some(Some(declared_type)) = module_input_types.get(i)
                    && let Some(arg) = arg_map.get(input_name) {
                        let expr = match arg {
                            Arg::Positional(e) | Arg::Named(_, e) => e,
                        };
                        if let Some(lit_type) = literal_type(expr)
                            && !types_compatible(&lit_type, declared_type) {
                                return Err(format!(
                                    "module '{}' parameter '{}' expects {}, got {} literal",
                                    func_name, input_name, declared_type, lit_type
                                ));
                            }
                        // Wire arguments: type checked when the assembler validates wiring
                    }
            }

            // Output arity check
            if targets.len() > module_outputs.len() {
                return Err(format!(
                    "module '{}' produces {} outputs: outputs, but {} targets requested",
                    func_name, module_outputs.len(), targets.len()
                ));
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
                Statement::ModuleDef(_) | Statement::ExternPort(_) => {} // nested module defs not inlined
            }
        }

        // Wire module outputs to caller's targets.
        // Use the module's declared output names (from formal signature
        // or inferred from binding names).
        for (i, target) in targets.iter().enumerate() {
            let output_name = module_outputs.get(i).cloned()
                .unwrap_or_else(|| func_name.to_string());
            let prefixed = format!("{prefix}{output_name}");
            // Register as output directly — don't add to all_names since
            // the main compile loop would try to add_output(target, node(target))
            // which would fail (target is an alias, not a node).
            asm.add_output(target, WireRef::node(&prefixed));
        }

        Ok(true)
    }

    /// Rewrite an expression from a module, substituting input references
    /// with the caller's arguments and prefixing internal names.
    pub(super) fn rewrite_module_expr(
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

    /// Resolve a module by name.
    ///
    /// Resolution order:
    /// 1. Cache (already resolved)
    /// 2. `<name>.gk` in `source_dir` (workload-local)
    /// 3. Any `.gk` in `source_dir` containing a matching binding
    /// 4. Same two searches for each `--gk-lib` path
    /// 5. Embedded stdlib
    pub(super) fn resolve_module(&mut self, name: &str) -> Result<Option<&ResolvedModule>, String> {
        if self.module_cache.contains_key(name) {
            return Ok(self.module_cache.get(name));
        }

        // Strategy 1-2: filesystem search in source_dir
        if let Some(source_dir) = &self.source_dir {
            let source_dir = source_dir.clone();

            // 1. Look for <name>.gk in source_dir
            let module_path = source_dir.join(format!("{name}.gk"));
            if module_path.exists() {
                let source = std::fs::read_to_string(&module_path)
                    .map_err(|e| format!("failed to read module '{}': {e}", module_path.display()))?;
                let resolved = Self::parse_module(&source, name)?;
                self.module_cache.insert(name.to_string(), resolved);
                return Ok(self.module_cache.get(name));
            }

            // 2. Scan all .gk files in source_dir for a matching export
            if let Ok(entries) = std::fs::read_dir(&source_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("gk") {
                        let source = match std::fs::read_to_string(&path) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        if let Ok(resolved) = Self::parse_module(&source, name) {
                            self.module_cache.insert(name.to_string(), resolved);
                            return Ok(self.module_cache.get(name));
                        }
                    }
                }
            }
        }

        // Strategy 3: search --gk-lib directories
        let lib_paths = self.gk_lib_paths.clone();
        for lib_dir in &lib_paths {
            // 3a. Look for <name>.gk in lib_dir
            let module_path = lib_dir.join(format!("{name}.gk"));
            if module_path.exists() {
                let source = std::fs::read_to_string(&module_path)
                    .map_err(|e| format!("failed to read module '{}': {e}", module_path.display()))?;
                let resolved = Self::parse_module(&source, name)?;
                self.module_cache.insert(name.to_string(), resolved);
                return Ok(self.module_cache.get(name));
            }

            // 3b. Scan all .gk files in lib_dir for a matching export
            if let Ok(entries) = std::fs::read_dir(lib_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("gk") {
                        let source = match std::fs::read_to_string(&path) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        if let Ok(resolved) = Self::parse_module(&source, name) {
                            self.module_cache.insert(name.to_string(), resolved);
                            return Ok(self.module_cache.get(name));
                        }
                    }
                }
            }
        }

        // Strategy 4: embedded stdlib
        if let Some(resolved) = self.resolve_stdlib(name)? {
            self.module_cache.insert(name.to_string(), resolved);
            return Ok(self.module_cache.get(name));
        }

        Ok(None)
    }

    /// Search the embedded stdlib for a module.
    fn resolve_stdlib(&self, name: &str) -> Result<Option<ResolvedModule>, String> {
        for (_filename, source) in STDLIB_MODULES {
            if let Ok(resolved) = Self::parse_module(source, name) {
                return Ok(Some(resolved));
            }
        }
        Ok(None)
    }

    /// Parse a `.gk` source and extract a module by name.
    ///
    /// First checks for a formal `ModuleDef` statement matching the name.
    /// If found, uses its typed signature and body directly.
    /// Otherwise, falls back to subgraph extraction by binding name.
    pub(super) fn parse_module(source: &str, target_name: &str) -> Result<ResolvedModule, String> {
        let tokens = lexer::lex(source)?;
        let ast = parser::parse(tokens)?;

        // Strategy 1: look for a formal ModuleDef with matching name
        for stmt in &ast.statements {
            if let Statement::ModuleDef(mdef) = stmt
                && mdef.name == target_name {
                    let inputs: Vec<String> = mdef.params.iter()
                        .map(|p| p.name.clone())
                        .collect();
                    let input_types: Vec<Option<String>> = mdef.params.iter()
                        .map(|p| Some(p.typ.clone()))
                        .collect();
                    let outputs: Vec<String> = mdef.outputs.iter()
                        .map(|o| o.name.clone())
                        .collect();
                    let output_types: Vec<Option<String>> = mdef.outputs.iter()
                        .map(|o| Some(o.typ.clone()))
                        .collect();
                    return Ok(ResolvedModule {
                        inputs,
                        input_types,
                        outputs,
                        output_types,
                        is_formal: true,
                        statements: mdef.body.clone(),
                    });
                }
        }

        // Strategy 2: subgraph extraction by binding name
        // Build a map: binding name → (statement index, references)
        let mut name_to_idx: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut stmt_refs: Vec<HashSet<String>> = Vec::new();

        for (i, stmt) in ast.statements.iter().enumerate() {
            let (names, expr) = match stmt {
                Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) => {
                    stmt_refs.push(HashSet::new());
                    continue;
                }
                Statement::InitBinding(b) => (vec![b.name.clone()], &b.value),
                Statement::CycleBinding(b) => (b.targets.clone(), &b.value),
            };
            for name in &names {
                name_to_idx.insert(name.clone(), i);
            }
            let mut refs = HashSet::new();
            collect_references(expr, &mut refs);
            stmt_refs.push(refs);
        }

        // Check that the target binding exists in this file
        let target_idx = match name_to_idx.get(target_name) {
            Some(&idx) => idx,
            None => return Err(format!("no binding named '{target_name}' in module")),
        };

        // Trace backward from target to find all needed statements
        let mut needed: HashSet<usize> = HashSet::new();
        let mut worklist = vec![target_idx];
        while let Some(idx) = worklist.pop() {
            if !needed.insert(idx) { continue; }
            if idx < stmt_refs.len() {
                for ref_name in &stmt_refs[idx] {
                    if let Some(&dep_idx) = name_to_idx.get(ref_name) {
                        worklist.push(dep_idx);
                    }
                }
            }
        }

        // Extract only the needed statements, preserving order
        let extracted: Vec<Statement> = ast.statements.iter().enumerate()
            .filter(|(i, _)| needed.contains(i))
            .map(|(_, s)| s.clone())
            .collect();

        if extracted.is_empty() {
            return Err(format!("empty subgraph for '{target_name}'"));
        }

        // Infer inputs: referenced names not defined within the subgraph
        let mut defined: HashSet<String> = HashSet::new();
        let mut referenced: HashSet<String> = HashSet::new();
        for stmt in &extracted {
            match stmt {
                Statement::Coordinates(names, _) => {
                    for n in names { defined.insert(n.clone()); }
                }
                Statement::InitBinding(b) => { defined.insert(b.name.clone()); }
                Statement::CycleBinding(b) => {
                    for t in &b.targets { defined.insert(t.clone()); }
                }
                Statement::ModuleDef(_) | Statement::ExternPort(_) => {}
            }
        }
        for stmt in &extracted {
            let expr = match stmt {
                Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) => continue,
                Statement::InitBinding(b) => &b.value,
                Statement::CycleBinding(b) => &b.value,
            };
            collect_references(expr, &mut referenced);
        }

        let mut inputs: Vec<String> = referenced.into_iter()
            .filter(|name| !defined.contains(name))
            .collect();
        inputs.sort();

        Ok(ResolvedModule {
            input_types: inputs.iter().map(|_| None).collect(),
            inputs,
            outputs: vec![target_name.to_string()],
            output_types: vec![None],
            is_formal: false,
            statements: extracted,
        })
    }
}
