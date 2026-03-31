// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Binding expression compiler: parses nosqlbench-style binding chains
//! into GK kernel node wiring.
//!
//! Binding syntax: `FuncA(args); FuncB(args); FuncC(args)`
//!
//! This is a semicolon-delimited chain where the output of each
//! function feeds the input of the next, starting from the `cycle`
//! coordinate. So `Hash(); Mod(1000000)` becomes:
//!
//! ```text
//! __binding_h_0 := hash(cycle)
//! binding_name  := mod(__binding_h_0, 1000000)
//! ```

use std::collections::HashMap;

use nb_variates::kernel::GkKernel;

use nb_workload::model::ParsedOp;
use nb_workload::bindpoints;

/// A parsed function call in a binding chain.
#[derive(Debug, Clone)]
struct BindingFunc {
    name: String,
    args: Vec<String>,
}

/// Parse a binding expression into a chain of function calls.
///
/// `"Hash(); Mod(1000000)"` → `[BindingFunc{name:"Hash", args:[]}, BindingFunc{name:"Mod", args:["1000000"]}]`
fn parse_binding_chain(expr: &str) -> Vec<BindingFunc> {
    let mut funcs = Vec::new();

    for segment in expr.split(';') {
        let segment = segment.trim();
        if segment.is_empty() { continue; }

        // Find function name and args
        if let Some(paren_pos) = segment.find('(') {
            let name = segment[..paren_pos].trim().to_string();
            let args_str = &segment[paren_pos + 1..];
            let args_str = args_str.trim_end_matches(')').trim();

            let args: Vec<String> = if args_str.is_empty() {
                Vec::new()
            } else {
                // Split on commas, respecting nested parens
                split_args(args_str)
            };

            funcs.push(BindingFunc { name, args });
        } else {
            // No parens — treat as a nullary function
            funcs.push(BindingFunc {
                name: segment.trim().to_string(),
                args: Vec::new(),
            });
        }
    }

    funcs
}

/// Split comma-separated arguments, respecting nested parentheses and quotes.
fn split_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    let mut in_quote = false;

    for c in s.chars() {
        match c {
            '\'' if !in_quote => { in_quote = true; current.push(c); }
            '\'' if in_quote => { in_quote = false; current.push(c); }
            '(' if !in_quote => { depth += 1; current.push(c); }
            ')' if !in_quote => { depth -= 1; current.push(c); }
            ',' if depth == 0 && !in_quote => {
                args.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        args.push(current.trim().to_string());
    }
    args
}

// build_chain_node and its helpers have been removed.
// Legacy semicolon-chain bindings are now translated to GK source
// and compiled through the unified GK compiler. See compile_bindings_with_opts.


/// Compile all bindings from a set of ParsedOps into a GK kernel.
///
/// Collects all unique binding names and expressions, plus any
/// unreferenced bind points in op fields (auto-bound to hash+mod).
/// Wires them through the GK assembler as proper node chains.
/// Probe the compile level of a GK function by name.
///
/// Instantiates a dummy node and calls its intrinsic `compile_level()`.
/// This is the single source of truth — no external classification needed.
/// Probe the compile level of a GK function by name.
///
/// Instantiates a node with a representative constant and probes
/// its intrinsic compile level. This is the single source of truth.
/// Probe the compile level of a GK function by name.
///
/// Constructs a representative GK program containing the function
/// and inspects the resulting kernel's node for its compile level.
/// Uses the unified GK compiler — no separate dispatch table.
pub fn probe_compile_level(func_name: &str) -> nb_variates::node::CompileLevel {
    let sig = match nb_variates::dsl::registry::lookup(func_name) {
        Some(s) => s,
        None => return nb_variates::node::CompileLevel::Phase1,
    };

    // Generate representative const args (1000 for ints, 1000.0 for floats)
    let const_args: Vec<String> = sig.const_params.iter().map(|(_name, _req)| {
        "1000".to_string()
    }).collect();

    // Build wire args
    let mut parts = Vec::new();
    for _ in 0..std::cmp::max(sig.wire_inputs, 1) {
        parts.push("cycle".to_string());
    }
    parts.extend(const_args);

    let source = format!("coordinates := (cycle)\nout := {func_name}({})", parts.join(", "));
    // Catch panics from nodes that validate const params (e.g.
    // unfair_coin rejects probabilities outside [0,1]).
    match std::panic::catch_unwind(|| nb_variates::dsl::compile_gk(&source)) {
        Ok(Ok(kernel)) => kernel.program().last_node_compile_level(),
        _ => nb_variates::node::CompileLevel::Phase1,
    }
}

pub fn compile_bindings(ops: &[ParsedOp]) -> Result<GkKernel, String> {
    compile_bindings_with_path(ops, None)
}

pub fn compile_bindings_with_path(ops: &[ParsedOp], source_dir: Option<&std::path::Path>) -> Result<GkKernel, String> {
    compile_bindings_with_opts(ops, source_dir, false)
}

/// Compile all bindings with additional GK library directories.
///
/// Each path in `gk_lib_paths` is searched (in order) for `.gk` module
/// files after `source_dir` but before the embedded stdlib.
pub fn compile_bindings_with_libs(
    ops: &[ParsedOp],
    source_dir: Option<&std::path::Path>,
    gk_lib_paths: Vec<std::path::PathBuf>,
    strict: bool,
) -> Result<GkKernel, String> {
    use nb_workload::model::BindingsDef;

    // Check if any op uses GK source mode
    let gk_source = ops.iter().find_map(|op| {
        if let BindingsDef::GkSource(src) = &op.bindings {
            if !src.trim().is_empty() { Some(src.clone()) } else { None }
        } else {
            None
        }
    });

    if let Some(source) = gk_source {
        let mut required: Vec<String> = Vec::new();
        for op in ops {
            for (_, value) in &op.op {
                if let Some(s) = value.as_str() {
                    for name in nb_workload::bindpoints::referenced_bindings(s) {
                        if !required.contains(&name) {
                            required.push(name);
                        }
                    }
                }
            }
        }
        return nb_variates::dsl::compile_gk_with_libs(&source, source_dir, gk_lib_paths, &required, strict);
    }

    // Legacy mode: translate semicolon-chain bindings into GK source
    let mut all_bindings: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for op in ops {
        if let BindingsDef::Map(map) = &op.bindings {
            for (name, expr) in map {
                all_bindings.entry(name.clone()).or_insert_with(|| expr.clone());
            }
        }
    }

    let mut required: Vec<String> = Vec::new();
    for op in ops {
        for (_, value) in &op.op {
            if let Some(s) = value.as_str() {
                for name in nb_workload::bindpoints::referenced_bindings(s) {
                    if !required.contains(&name) {
                        required.push(name);
                    }
                }
            }
        }
    }

    let mut gk_lines: Vec<String> = Vec::new();
    gk_lines.push("coordinates := (cycle)".into());

    for (binding_name, expr) in &all_bindings {
        let chain = parse_binding_chain(expr);
        if chain.is_empty() {
            return Err(format!("empty binding expression for '{binding_name}'"));
        }

        let mut prev_wire = "cycle".to_string();
        for (i, func) in chain.iter().enumerate() {
            let is_last = i == chain.len() - 1;
            let target = if is_last {
                binding_name.clone()
            } else {
                format!("__chain_{binding_name}_{i}")
            };

            let func_name = func.name.to_lowercase();
            let mut call_args = vec![prev_wire.clone()];
            for arg in &func.args {
                call_args.push(arg.trim().to_string());
            }

            gk_lines.push(format!("{target} := {func_name}({args})",
                args = call_args.join(", ")));

            prev_wire = target;
        }
    }

    let mut missing: Vec<String> = Vec::new();
    for name in &required {
        if name == "cycle" {
            if !all_bindings.contains_key(name) {
                gk_lines.push(format!("{name} := identity(cycle)"));
            }
        } else if !all_bindings.contains_key(name) {
            missing.push(name.clone());
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "undeclared bind point references: {}. Add these to your bindings section.",
            missing.join(", ")
        ));
    }

    let gk_source = gk_lines.join("\n");
    nb_variates::dsl::compile_gk_with_libs(&gk_source, source_dir, gk_lib_paths, &required, strict)
}

/// Compile all bindings from a set of ParsedOps into a GK kernel.
///
/// When `strict` is true, the GK compiler enforces:
/// - Explicit `coordinates := (...)` declaration (no inference)
/// - All module arguments must be named (no positional)
/// - All module inputs must be provided by caller (no fallthrough)
pub fn compile_bindings_with_opts(ops: &[ParsedOp], source_dir: Option<&std::path::Path>, strict: bool) -> Result<GkKernel, String> {
    use nb_workload::model::BindingsDef;

    // Check if any op uses GK source mode
    let gk_source = ops.iter().find_map(|op| {
        if let BindingsDef::GkSource(src) = &op.bindings {
            if !src.trim().is_empty() { Some(src.clone()) } else { None }
        } else {
            None
        }
    });

    if let Some(source) = gk_source {
        // Native GK grammar mode: collect referenced bind points from
        // op templates for dead code elimination. Only the bindings
        // actually used by ops are compiled into the kernel.
        let mut required: Vec<String> = Vec::new();
        for op in ops {
            for (_, value) in &op.op {
                if let Some(s) = value.as_str() {
                    for name in bindpoints::referenced_bindings(s) {
                        if !required.contains(&name) {
                            required.push(name);
                        }
                    }
                }
            }
        }
        return nb_variates::dsl::compile_gk_with_outputs(&source, source_dir, &required, strict);
    }

    // Legacy mode: translate semicolon-chain bindings into GK source
    // and compile through the unified GK compiler. No separate dispatch
    // table — every node function available in GK grammar is automatically
    // available in legacy chain syntax.
    let mut all_bindings: HashMap<String, String> = HashMap::new();
    for op in ops {
        if let BindingsDef::Map(map) = &op.bindings {
            for (name, expr) in map {
                all_bindings.entry(name.clone()).or_insert_with(|| expr.clone());
            }
        }
    }

    // Collect required outputs from op templates
    let mut required: Vec<String> = Vec::new();
    for op in ops {
        for (_, value) in &op.op {
            if let Some(s) = value.as_str() {
                for name in bindpoints::referenced_bindings(s) {
                    if !required.contains(&name) {
                        required.push(name);
                    }
                }
            }
        }
    }

    // Translate each legacy chain into GK source lines
    let mut gk_lines: Vec<String> = Vec::new();
    gk_lines.push("coordinates := (cycle)".into());

    for (binding_name, expr) in &all_bindings {
        let chain = parse_binding_chain(expr);
        if chain.is_empty() {
            return Err(format!("empty binding expression for '{binding_name}'"));
        }

        // Convert each chain step into a GK binding.
        // Chain: FuncA(args); FuncB(args) → sequential wiring from cycle.
        let mut prev_wire = "cycle".to_string();

        for (i, func) in chain.iter().enumerate() {
            let is_last = i == chain.len() - 1;
            let target = if is_last {
                binding_name.clone()
            } else {
                format!("__chain_{binding_name}_{i}")
            };

            // Format args: lowercase function name, prev wire + const args
            let func_name = func.name.to_lowercase();
            let mut call_args = vec![prev_wire.clone()];
            for arg in &func.args {
                // Pass through literal args (numbers, strings)
                let trimmed = arg.trim();
                call_args.push(trimmed.to_string());
            }

            gk_lines.push(format!("{target} := {func_name}({args})",
                args = call_args.join(", ")));

            prev_wire = target;
        }
    }

    // Validate: all bind point references must have a binding or be "cycle"
    let mut missing: Vec<String> = Vec::new();
    for name in &required {
        if name == "cycle" {
            // Coordinate — add an identity passthrough
            if !all_bindings.contains_key(name) {
                gk_lines.push(format!("{name} := identity(cycle)"));
            }
        } else if !all_bindings.contains_key(name) {
            missing.push(name.clone());
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "undeclared bind point references: {}. Add these to your bindings section.",
            missing.join(", ")
        ));
    }

    let gk_source = gk_lines.join("\n");
    nb_variates::dsl::compile_gk_with_outputs(&gk_source, source_dir, &required, strict)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_chain() {
        let chain = parse_binding_chain("Hash(); Mod(1000000)");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].name, "Hash");
        assert!(chain[0].args.is_empty());
        assert_eq!(chain[1].name, "Mod");
        assert_eq!(chain[1].args, vec!["1000000"]);
    }

    #[test]
    fn parse_identity() {
        let chain = parse_binding_chain("Identity()");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].name, "Identity");
    }

    #[test]
    fn parse_with_string_arg() {
        let chain = parse_binding_chain("Template('user-{}', ToString())");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].name, "Template");
        assert_eq!(chain[0].args.len(), 2);
    }

    #[test]
    fn parse_long_chain() {
        let chain = parse_binding_chain("Add(10); Hash(); Mod(100); ToString()");
        assert_eq!(chain.len(), 4);
        assert_eq!(chain[0].name, "Add");
        assert_eq!(chain[1].name, "Hash");
        assert_eq!(chain[2].name, "Mod");
        assert_eq!(chain[3].name, "ToString");
    }

    #[test]
    fn parse_with_long_suffix() {
        let chain = parse_binding_chain("Mod(1000000000L)");
        assert_eq!(chain[0].args, vec!["1000000000L"]);
        // The L suffix is preserved in the chain parse.
        // The GK compiler handles it during node construction.
    }

    #[test]
    fn compile_identity_binding() {
        let ops = vec![{
            let mut op = ParsedOp::simple("test", "{myval}");
            op.bindings.insert("myval".into(), "Identity()".into());
            op
        }];
        let mut kernel = compile_bindings(&ops).unwrap();
        kernel.set_coordinates(&[42]);
        assert_eq!(kernel.pull("myval").as_u64(), 42);
    }

    #[test]
    fn compile_hash_mod_binding() {
        let ops = vec![{
            let mut op = ParsedOp::simple("test", "{id}");
            op.bindings.insert("id".into(), "Hash(); Mod(1000000)".into());
            op
        }];
        let mut kernel = compile_bindings(&ops).unwrap();
        kernel.set_coordinates(&[42]);
        let val = kernel.pull("id").as_u64();
        assert!(val < 1_000_000, "got {val}");
    }

    #[test]
    fn compile_hash_mod_deterministic() {
        let ops = vec![{
            let mut op = ParsedOp::simple("test", "{id}");
            op.bindings.insert("id".into(), "Hash(); Mod(100)".into());
            op
        }];
        let mut kernel = compile_bindings(&ops).unwrap();
        kernel.set_coordinates(&[42]);
        let v1 = kernel.pull("id").as_u64();
        kernel.set_coordinates(&[42]);
        let v2 = kernel.pull("id").as_u64();
        assert_eq!(v1, v2);
    }

    #[test]
    fn compile_multiple_bindings() {
        let ops = vec![{
            let mut op = ParsedOp::simple("test", "{a} {b}");
            op.bindings.insert("a".into(), "Identity()".into());
            op.bindings.insert("b".into(), "Hash(); Mod(100)".into());
            op
        }];
        let mut kernel = compile_bindings(&ops).unwrap();
        kernel.set_coordinates(&[5]);
        assert_eq!(kernel.pull("a").as_u64(), 5);
        assert!(kernel.pull("b").as_u64() < 100);
    }

    #[test]
    fn compile_rejects_undeclared_bind_points() {
        // Op references {mystery} but no binding declared → error
        let ops = vec![ParsedOp::simple("test", "val={mystery}")];
        let result = compile_bindings(&ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("undeclared bind point"));
    }

    #[test]
    fn compile_add_chain() {
        let ops = vec![{
            let mut op = ParsedOp::simple("test", "{val}");
            op.bindings.insert("val".into(), "Add(100); Mod(1000)".into());
            op
        }];
        let mut kernel = compile_bindings(&ops).unwrap();
        kernel.set_coordinates(&[5]);
        // 5 + 100 = 105, 105 % 1000 = 105
        assert_eq!(kernel.pull("val").as_u64(), 105);
    }

    #[test]
    fn compile_provides_cycle_output() {
        let ops = vec![ParsedOp::simple("test", "cycle={cycle}")];
        let mut kernel = compile_bindings(&ops).unwrap();
        kernel.set_coordinates(&[99]);
        assert_eq!(kernel.pull("cycle").as_u64(), 99);
    }
}
