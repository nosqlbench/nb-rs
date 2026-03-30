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

use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::kernel::GkKernel;
use nb_variates::node::GkNode;
use nb_variates::nodes::arithmetic::*;
use nb_variates::nodes::convert::*;
use nb_variates::nodes::hash::*;
use nb_variates::nodes::identity::*;
use nb_variates::nodes::lerp::*;
use nb_variates::nodes::string::*;
use nb_variates::nodes::datetime::*;
use nb_variates::sampling::icd::{UnitInterval, ClampF64, IcdSample};
use nb_variates::sampling::metashift::Shuffle;

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

/// Build a GK node from a function name and its constant arguments.
///
/// Returns (node, expected_wire_inputs). Most chain functions take 1
/// wire input (from the previous function in the chain).
fn build_chain_node(func: &BindingFunc) -> Result<(Box<dyn GkNode>, usize), String> {
    let name = func.name.to_lowercase();
    let name = name.trim();

    match name {
        // --- Hashing ---
        "hash" | "fullhash" => Ok((Box::new(Hash64::new()), 1)),
        "hashrange" => {
            let max = parse_u64_arg(&func.args, 0, 1_000_000)?;
            Ok((Box::new(HashRange::new(max)), 1))
        }
        "hashinterval" => {
            let min = parse_f64_arg(&func.args, 0, 0.0)?;
            let max = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(HashInterval::new(min, max)), 1))
        }

        // --- Arithmetic ---
        "add" => {
            let v = parse_u64_arg(&func.args, 0, 0)?;
            Ok((Box::new(AddU64::new(v)), 1))
        }
        "mul" => {
            let v = parse_u64_arg(&func.args, 0, 1)?;
            Ok((Box::new(MulU64::new(v)), 1))
        }
        "div" => {
            let v = parse_u64_arg(&func.args, 0, 1)?;
            Ok((Box::new(DivU64::new(v)), 1))
        }
        "mod" | "modulo" => {
            let v = parse_u64_arg(&func.args, 0, 1_000_000)?;
            Ok((Box::new(ModU64::new(v)), 1))
        }
        "clamp" => {
            let min = parse_u64_arg(&func.args, 0, 0)?;
            let max = parse_u64_arg(&func.args, 1, u64::MAX)?;
            Ok((Box::new(ClampU64::new(min, max)), 1))
        }
        "interleave" => Ok((Box::new(Interleave::new()), 2)),
        "mixed_radix" | "mixedradix" => {
            let radixes: Vec<u64> = func.args.iter()
                .map(|s| s.trim().parse::<u64>().unwrap_or(0))
                .collect();
            if radixes.is_empty() {
                return Err("mixed_radix requires at least one radix argument".into());
            }
            Ok((Box::new(MixedRadix::new(radixes)), 1))
        }

        // --- Identity ---
        "identity" => Ok((Box::new(Identity::new()), 1)),

        // --- Type conversions ---
        "tostring" | "longtostring" => Ok((Box::new(U64ToString::new()), 1)),
        "tohexstring" => Ok((Box::new(FormatU64::hex()), 1)),

        // --- String ---
        "numbernametostring" => Ok((Box::new(NumberToWords::new()), 1)),
        "combinations" => {
            let pattern = func.args.first()
                .map(|s| s.trim_matches('\'').trim_matches('"').to_string())
                .unwrap_or_else(|| "0-9".to_string());
            Ok((Box::new(Combinations::new(&pattern)), 1))
        }

        // --- Datetime ---
        "todate" | "todatetime" | "totimestamp" => Ok((Box::new(ToTimestamp::new()), 1)),
        "startingepochmillis" => {
            // Parse epoch from string arg or default
            let base = if let Some(arg) = func.args.first() {
                let arg = arg.trim_matches('\'').trim_matches('"');
                // Simple: try to parse as number
                arg.parse::<u64>().unwrap_or(0)
            } else {
                0
            };
            Ok((Box::new(EpochOffset::new(base)), 1))
        }

        // --- f64 conversions ---
        "unit_interval" | "unitinterval" => Ok((Box::new(UnitInterval::new()), 1)),
        "clamp_f64" | "clampf64" => {
            let min = parse_f64_arg(&func.args, 0, 0.0)?;
            let max = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(ClampF64::new(min, max)), 1))
        }
        "f64_to_u64" | "f64tou64" => Ok((Box::new(F64ToU64::new()), 1)),
        "round_to_u64" | "roundtou64" => Ok((Box::new(RoundToU64::new()), 1)),
        "floor_to_u64" | "floortou64" => Ok((Box::new(FloorToU64::new()), 1)),
        "ceil_to_u64" | "ceiltou64" => Ok((Box::new(CeilToU64::new()), 1)),
        "discretize" => {
            let range = parse_f64_arg(&func.args, 0, 100.0)?;
            let buckets = parse_u64_arg(&func.args, 1, 10)?;
            Ok((Box::new(Discretize::new(range, buckets)), 1))
        }

        // --- Interpolation ---
        "lerp" => {
            let a = parse_f64_arg(&func.args, 0, 0.0)?;
            let b = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(LerpConst::new(a, b)), 1))
        }
        "scale_range" | "scalerange" => {
            let min = parse_f64_arg(&func.args, 0, 0.0)?;
            let max = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(ScaleRange::new(min, max)), 1))
        }
        "quantize" => {
            let step = parse_f64_arg(&func.args, 0, 1.0)?;
            Ok((Box::new(Quantize::new(step)), 1))
        }

        // --- Shuffle ---
        "shuffle" => {
            let size = parse_u64_arg(&func.args, 0, 1_000_000)?;
            Ok((Box::new(Shuffle::new(0, size)), 1))
        }

        // --- Distribution sampling (LUT-backed, P3 via extern) ---
        "icd_normal" | "dist_normal" => {
            let mean = parse_f64_arg(&func.args, 0, 0.0)?;
            let stddev = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(IcdSample::normal(mean, stddev)), 1))
        }
        "icd_exponential" | "dist_exponential" => {
            let rate = parse_f64_arg(&func.args, 0, 1.0)?;
            Ok((Box::new(IcdSample::exponential(rate)), 1))
        }
        "dist_uniform" => {
            let min = parse_f64_arg(&func.args, 0, 0.0)?;
            let max = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(IcdSample::uniform(min, max)), 1))
        }
        "dist_pareto" => {
            let scale = parse_f64_arg(&func.args, 0, 1.0)?;
            let shape = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(IcdSample::pareto(scale, shape)), 1))
        }
        "dist_zipf" => {
            let n = parse_u64_arg(&func.args, 0, 1000)?;
            let exponent = parse_f64_arg(&func.args, 1, 1.0)?;
            Ok((Box::new(IcdSample::zipf(n, exponent)), 1))
        }
        "lut_sample" => {
            // Probe: build a simple identity LUT for probing
            Ok((Box::new(IcdSample::normal(0.0, 1.0)), 1))
        }

        // --- Integer uniform ---
        "uniform" => {
            let min = parse_u64_arg(&func.args, 0, 0)?;
            let max = parse_u64_arg(&func.args, 1, 1_000_000)?;
            // Uniform integer: just hash + mod(max-min) + add(min)
            Ok((Box::new(ModU64::new(max - min)), 1))
        }

        // --- Fixed values ---
        "fixedvalue" => {
            let v = parse_u64_arg(&func.args, 0, 0)?;
            Ok((Box::new(ConstU64::new(v)), 0)) // 0 wire inputs
        }

        _ => Err(format!("unknown binding function: '{}' (in expression)", func.name)),
    }
}

fn parse_u64_arg(args: &[String], idx: usize, default: u64) -> Result<u64, String> {
    args.get(idx)
        .map(|s| {
            let s = s.trim().trim_end_matches('L').trim_end_matches('l');
            s.parse::<u64>().map_err(|e| format!("invalid integer arg '{}': {}", s, e))
        })
        .unwrap_or(Ok(default))
}

fn parse_f64_arg(args: &[String], idx: usize, default: f64) -> Result<f64, String> {
    args.get(idx)
        .map(|s| {
            let s = s.trim().trim_end_matches('d').trim_end_matches('D')
                .trim_end_matches('f').trim_end_matches('F');
            s.parse::<f64>().map_err(|e| format!("invalid float arg '{}': {}", s, e))
        })
        .unwrap_or(Ok(default))
}

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
pub fn probe_compile_level(func_name: &str) -> nb_variates::node::CompileLevel {
    // Supply a representative constant so nodes like Mod don't panic on 0
    let representative_arg = match func_name.to_lowercase().as_str() {
        "mod" | "div" | "mul" | "add" => vec!["1000".to_string()],
        "clamp" => vec!["0".to_string(), "1000".to_string()],
        "mixed_radix" => vec!["100".to_string(), "1000".to_string()],
        "clamp_f64" => vec!["0.0".to_string(), "1.0".to_string()],
        "lerp" => vec!["0.0".to_string(), "1.0".to_string()],
        "scale_range" => vec!["0.0".to_string(), "100.0".to_string()],
        "quantize" => vec!["10.0".to_string()],
        "discretize" => vec!["100.0".to_string(), "10".to_string()],
        "shuffle" => vec!["1000000".to_string()],
        "icd_normal" | "dist_normal" => vec!["0.0".to_string(), "1.0".to_string()],
        "icd_exponential" | "dist_exponential" => vec!["1.0".to_string()],
        "dist_uniform" => vec!["0.0".to_string(), "1.0".to_string()],
        "dist_pareto" => vec!["1.0".to_string(), "1.0".to_string()],
        "dist_zipf" => vec!["1000".to_string(), "1.0".to_string()],
        _ => Vec::new(),
    };
    let dummy = BindingFunc { name: func_name.to_string(), args: representative_arg };
    match build_chain_node(&dummy) {
        Ok((node, _)) => nb_variates::node::compile_level_of(node.as_ref()),
        Err(_) => nb_variates::node::CompileLevel::Phase1,
    }
}

pub fn compile_bindings(ops: &[ParsedOp]) -> Result<GkKernel, String> {
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
        // Native GK grammar mode: compile the source directly
        return nb_variates::dsl::compile_gk(&source);
    }

    // Legacy mode: collect all map-style bindings
    let mut all_bindings: HashMap<String, String> = HashMap::new();
    for op in ops {
        if let BindingsDef::Map(map) = &op.bindings {
            for (name, expr) in map {
                all_bindings.entry(name.clone()).or_insert_with(|| expr.clone());
            }
        }
    }

    // Validate: all bind point references in op fields must have
    // a corresponding binding declaration.
    let mut missing: Vec<String> = Vec::new();
    for op in ops {
        for (_, value) in &op.op {
            if let Some(s) = value.as_str() {
                for name in bindpoints::referenced_bindings(s) {
                    if name != "cycle" && !all_bindings.contains_key(&name) {
                        if !missing.contains(&name) {
                            missing.push(name);
                        }
                    }
                }
            }
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "undeclared bind point references: {}. Add these to your bindings section.",
            missing.join(", ")
        ));
    }

    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    let mut node_counter = 0usize;

    for (binding_name, expr) in &all_bindings {
        let chain = parse_binding_chain(expr);
        if chain.is_empty() {
            return Err(format!("empty binding expression for '{binding_name}'"));
        }

        // Wire the chain: each function's output feeds the next's input
        let mut current_wire = WireRef::coord("cycle");

        for (i, func) in chain.iter().enumerate() {
            let is_last = i == chain.len() - 1;
            let node_name = if is_last {
                binding_name.clone()
            } else {
                let name = format!("__bind_{binding_name}_{node_counter}");
                node_counter += 1;
                name
            };

            let (node, wire_inputs) = build_chain_node(func)
                .map_err(|e| format!("in binding '{binding_name}': {e}"))?;

            let inputs = if wire_inputs == 0 {
                vec![] // constant node, no wire inputs
            } else {
                vec![current_wire.clone()]
            };

            asm.add_node(&node_name, node, inputs);

            if !is_last {
                current_wire = WireRef::node(&node_name);
            }
        }

        asm.add_output(binding_name, WireRef::node(binding_name));
    }

    // Always provide a "cycle" output
    if !all_bindings.contains_key("cycle") {
        asm.add_node("__cycle_identity", Box::new(Identity::new()), vec![WireRef::coord("cycle")]);
        asm.add_output("cycle", WireRef::node("__cycle_identity"));
    }

    asm.compile().map_err(|e| format!("GK kernel compilation failed: {e}"))
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
        // The L suffix should be handled by parse_u64_arg
        let val = parse_u64_arg(&chain[0].args, 0, 0).unwrap();
        assert_eq!(val, 1_000_000_000);
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
