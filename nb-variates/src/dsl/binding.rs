// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Per-statement compilation for the GK DSL compiler.
//!
//! `compile_binding` maps each AST statement to assembler operations:
//! function calls become nodes, literals become constant nodes, and
//! identifiers become identity (alias) nodes.

use crate::assembly::{GkAssembler, WireRef};
use crate::dsl::ast::*;
use crate::dsl::factory::{build_node, ConstArg};
use crate::nodes::fixed::*;
use crate::nodes::identity::*;
use crate::nodes::format::*;

use super::compile::Compiler;

impl Compiler {
    /// Compile one binding statement into the assembler.
    ///
    /// `targets` are the LHS names, `value` is the RHS expression.
    /// Single-target bindings produce a single node.  Multi-target
    /// bindings (destructuring) produce an internal node plus one
    /// `Identity` node per target that fans out each output port.
    pub(super) fn compile_binding(
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
                            if self.input_names.contains(id) {
                                wire_refs.push(WireRef::input(id));
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
                            if self.input_names.contains(n) {
                                WireRef::input(n)
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
                let wire = if self.input_names.contains(id) {
                    WireRef::input(id)
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
                return Err("unsupported expression in binding".to_string());
            }
        }
        Ok(())
    }
}
