// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Per-statement compilation for the GK DSL compiler.
//!
//! `compile_binding` maps each AST statement to assembler operations:
//! function calls become nodes, literals become constant nodes, and
//! identifiers become identity (alias) nodes.
//!
//! Literal promotion: when a literal constant (integer, float, string)
//! appears in a position where the function's signature expects a wire
//! input, the compiler automatically inserts an anonymous `ConstU64`,
//! `ConstF64`, or `ConstStr` node and wires from it.  This lets you
//! write `pow(x, 3.0)` or `f64_mul(x, 0.1)` directly without first
//! binding the constant to a name.

use crate::assembly::{GkAssembler, WireRef};
use crate::dsl::ast::*;
use crate::dsl::factory::{build_node, ConstArg};
use crate::dsl::registry;
use crate::node::{PortType, SlotType};
use crate::nodes::fixed::*;
use crate::nodes::identity::*;
use crate::nodes::format::*;

use super::compile::Compiler;

/// Infer the output `PortType` of an expression without compiling it.
///
/// Uses heuristics to determine whether an expression produces a u64 or f64
/// value. This drives type-aware operator dispatch: when both operands of
/// an arithmetic operator are u64, the compiler selects the u64 variant
/// (e.g. `u64_mul`) instead of the f64 variant (`f64_mul`).
///
/// For unknown cases, defaults to `PortType::U64`.
fn infer_expr_type(
    expr: &Expr,
    asm: &GkAssembler,
    input_names: &[String],
) -> PortType {
    match expr {
        Expr::IntLit(_, _) => PortType::U64,
        Expr::FloatLit(_, _) => PortType::F64,
        Expr::StringLit(_, _) => PortType::Str,
        Expr::Ident(name, _) => {
            // Coordinate inputs are always u64.
            if input_names.contains(name) {
                return PortType::U64;
            }
            // Check if it's a known binding — look up output type from assembler.
            asm.output_type(name).unwrap_or(PortType::U64)
        }
        Expr::Call(call) => {
            // Heuristic based on function name prefix/membership.
            let f = call.func.as_str();
            if f.starts_with("f64_") || f.starts_with("to_f64")
                || ["sin", "cos", "tan", "asin", "acos", "atan", "atan2",
                    "sqrt", "abs_f64", "ln", "exp", "pow", "lerp",
                    "scale_range", "unit_interval", "clamp_f64",
                    "quantize", "discretize", "f64_mod",
                    "icd_normal", "icd_uniform",
                ].contains(&f) {
                PortType::F64
            } else {
                PortType::U64
            }
        }
        Expr::BinOp(lhs, op, rhs) => {
            match op {
                BinOpKind::BitAnd | BinOpKind::BitOr | BinOpKind::BitXor |
                BinOpKind::Shl | BinOpKind::Shr => PortType::U64,
                BinOpKind::Pow => PortType::F64,
                _ => {
                    let lt = infer_expr_type(lhs, asm, input_names);
                    let rt = infer_expr_type(rhs, asm, input_names);
                    if lt == PortType::F64 || rt == PortType::F64 {
                        PortType::F64
                    } else {
                        PortType::U64
                    }
                }
            }
        }
        Expr::UnaryNeg(_, _) => PortType::F64,
        Expr::UnaryBitNot(_, _) => PortType::U64,
        Expr::ArrayLit(_, _) => PortType::U64,
        Expr::FieldAccess { source, field, .. } => {
            // Treat as an identifier reference — check assembler for output type.
            let wire_name = format!("{source}__{field}");
            asm.output_type(&wire_name).unwrap_or(PortType::U64)
        }
    }
}

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

                // Look up the function signature so we can determine which
                // arg positions are wire slots vs const slots.  When a
                // literal appears in a wire slot we promote it to an
                // anonymous const node and wire from it instead of
                // treating it as a build-time constant.
                let sig = registry::lookup(&call.func);

                // Build an iterator over the param slot-types.  We step
                // through it once per argument, wire-position by wire-position
                // or const-position by const-position, depending on what each
                // arg is.  For functions without a registry entry (or variadic
                // ones) we fall back to the original literal-as-const behaviour.
                let param_slot_types: Vec<SlotType> = sig
                    .map(|s| s.params.iter().map(|p| p.slot_type).collect())
                    .unwrap_or_default();

                // Cursor into the param list.  We advance it for each call arg.
                let mut param_cursor = 0usize;

                for arg in &call.args {
                    let (expr, _name) = match arg {
                        Arg::Positional(e) => (e, None),
                        Arg::Named(n, e) => (e, Some(n.as_str())),
                    };

                    // Determine the expected slot type for this argument
                    // position, if known from the signature.
                    let expected_slot = param_slot_types.get(param_cursor).copied();
                    param_cursor += 1;

                    // Helper: is this arg position expected to be a wire input?
                    let wants_wire = expected_slot.map(|s| s.is_wire()).unwrap_or(false);

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
                            if wants_wire {
                                // Promote to an anonymous ConstU64 wire node.
                                let anon = self.anon_name();
                                asm.add_node(&anon, Box::new(ConstU64::new(*v)), vec![]);
                                wire_refs.push(WireRef::node(anon));
                            } else {
                                const_args.push(ConstArg::Int(*v));
                            }
                        }
                        Expr::FloatLit(v, _) => {
                            if wants_wire {
                                // Promote to an anonymous ConstF64 wire node.
                                let anon = self.anon_name();
                                asm.add_node(&anon, Box::new(ConstF64::new(*v)), vec![]);
                                wire_refs.push(WireRef::node(anon));
                            } else {
                                const_args.push(ConstArg::Float(*v));
                            }
                        }
                        Expr::StringLit(s, _) => {
                            if wants_wire {
                                // Promote to an anonymous ConstStr wire node.
                                let anon = self.anon_name();
                                asm.add_node(&anon, Box::new(ConstStr::new(s.clone())), vec![]);
                                wire_refs.push(WireRef::node(anon));
                            } else {
                                const_args.push(ConstArg::Str(s.clone()));
                            }
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
                        Expr::UnaryNeg(inner, _) => {
                            // Special case: `-literal` as a constant or wire argument.
                            // e.g. lerp(u, -10.0, 10.0) treats -10.0 as const.
                            // e.g. pow(x, -2.0) promotes to a ConstF64 wire node.
                            match inner.as_ref() {
                                Expr::FloatLit(v, _) => {
                                    if wants_wire {
                                        let anon = self.anon_name();
                                        asm.add_node(&anon, Box::new(ConstF64::new(-v)), vec![]);
                                        wire_refs.push(WireRef::node(anon));
                                    } else {
                                        const_args.push(ConstArg::Float(-v));
                                    }
                                }
                                Expr::IntLit(v, _) => {
                                    if wants_wire {
                                        // Negative integer: promote as ConstF64
                                        let anon = self.anon_name();
                                        asm.add_node(&anon, Box::new(ConstF64::new(-(*v as f64))), vec![]);
                                        wire_refs.push(WireRef::node(anon));
                                    } else {
                                        // Wrapping negate for u64 (effectively i64 reinterpret)
                                        const_args.push(ConstArg::Float(-(*v as f64)));
                                    }
                                }
                                _ => {
                                    // General case: wire input via anonymous node
                                    let anon = self.anon_name();
                                    self.compile_binding(asm, &[anon.clone()], expr)?;
                                    wire_refs.push(WireRef::node(anon));
                                }
                            }
                        }
                        Expr::BinOp(..) | Expr::UnaryBitNot(..) => {
                            // Inline arithmetic: desugar to an anonymous node
                            let anon = self.anon_name();
                            self.compile_binding(asm, &[anon.clone()], expr)?;
                            wire_refs.push(WireRef::node(anon));
                        }
                        Expr::FieldAccess { source, field, .. } => {
                            let wire_name = format!("{source}__{field}");
                            wire_refs.push(WireRef::node(&wire_name));
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
            Expr::BinOp(lhs, op, rhs) => {
                // Desugar infix arithmetic to the equivalent function call node.
                // Type-aware dispatch: infer operand types and select the
                // appropriate u64 or f64 variant. When one operand is u64 and
                // the other is f64, the u64 operand is auto-widened via to_f64.
                let lhs_type = infer_expr_type(lhs, asm, &self.input_names);
                let rhs_type = infer_expr_type(rhs, asm, &self.input_names);

                let (func_name, need_widen_lhs, need_widen_rhs) = match op {
                    BinOpKind::Add | BinOpKind::Sub | BinOpKind::Mul |
                    BinOpKind::Div | BinOpKind::Mod => {
                        if lhs_type == PortType::U64 && rhs_type == PortType::U64 {
                            let name = match op {
                                BinOpKind::Add => "u64_add",
                                BinOpKind::Sub => "u64_sub",
                                BinOpKind::Mul => "u64_mul",
                                BinOpKind::Div => "u64_div",
                                BinOpKind::Mod => "u64_mod",
                                _ => unreachable!(),
                            };
                            (name, false, false)
                        } else {
                            let name = match op {
                                BinOpKind::Add => "f64_add",
                                BinOpKind::Sub => "f64_sub",
                                BinOpKind::Mul => "f64_mul",
                                BinOpKind::Div => "f64_div",
                                BinOpKind::Mod => "f64_mod",
                                _ => unreachable!(),
                            };
                            (name, lhs_type == PortType::U64, rhs_type == PortType::U64)
                        }
                    }
                    BinOpKind::Pow => ("pow", lhs_type == PortType::U64, rhs_type == PortType::U64),
                    BinOpKind::BitAnd => ("u64_and", false, false),
                    BinOpKind::BitOr  => ("u64_or", false, false),
                    BinOpKind::BitXor => ("u64_xor", false, false),
                    BinOpKind::Shl    => ("u64_shl", false, false),
                    BinOpKind::Shr    => ("u64_shr", false, false),
                };

                // Compile each operand. Simple identifiers and literals
                // are resolved directly as wire references to avoid
                // inserting Identity nodes that lose type information.
                let lhs_ref = self.compile_binop_operand(asm, lhs)?;
                let rhs_ref = self.compile_binop_operand(asm, rhs)?;

                // Insert to_f64 widening adapters where needed.
                let lhs_final = if need_widen_lhs {
                    let adapted = self.anon_name();
                    asm.add_node(
                        &adapted,
                        Box::new(crate::nodes::convert::ToF64::new()),
                        vec![lhs_ref],
                    );
                    WireRef::node(&adapted)
                } else {
                    lhs_ref
                };
                let rhs_final = if need_widen_rhs {
                    let adapted = self.anon_name();
                    asm.add_node(
                        &adapted,
                        Box::new(crate::nodes::convert::ToF64::new()),
                        vec![rhs_ref],
                    );
                    WireRef::node(&adapted)
                } else {
                    rhs_ref
                };

                let wire_refs = vec![lhs_final, rhs_final];
                let node = build_node(func_name, &wire_refs, &[])?;
                let name = &targets[0];
                asm.add_node(name, node, wire_refs);
                self.all_names.push(name.clone());
            }
            Expr::UnaryNeg(inner, _) => {
                // Desugar `-x` to `f64_sub(0.0, x)`.
                let zero_name = self.anon_name();
                asm.add_node(&zero_name, Box::new(ConstF64::new(0.0)), vec![]);

                let inner_name = self.anon_name();
                self.compile_binding(asm, &[inner_name.clone()], inner)?;

                let wire_refs = vec![WireRef::node(&zero_name), WireRef::node(&inner_name)];
                let node = build_node("f64_sub", &wire_refs, &[])?;
                let name = &targets[0];
                asm.add_node(name, node, wire_refs);
                self.all_names.push(name.clone());
            }
            Expr::UnaryBitNot(inner, _) => {
                // Desugar `!x` to `u64_not(x)`.
                let inner_name = self.anon_name();
                self.compile_binding(asm, &[inner_name.clone()], inner)?;

                let wire_refs = vec![WireRef::node(&inner_name)];
                let node = build_node("u64_not", &wire_refs, &[])?;
                let name = &targets[0];
                asm.add_node(name, node, wire_refs);
                self.all_names.push(name.clone());
            }
            Expr::FieldAccess { source, field, .. } => {
                // Source projection: wire target to the source__field node.
                let wire_name = format!("{source}__{field}");
                let name = &targets[0];
                // Create an identity passthrough wired to the projection node
                let identity = Box::new(
                    crate::nodes::identity::PortPassthrough::new(name, crate::node::PortType::U64)
                );
                asm.add_node(name, identity, vec![WireRef::node(&wire_name)]);
                self.all_names.push(name.clone());
            }
            _ => {
                return Err("unsupported expression in binding".to_string());
            }
        }
        Ok(())
    }

    /// Compile a BinOp operand, returning a `WireRef` without creating
    /// unnecessary Identity intermediates.
    ///
    /// Simple identifiers resolve directly to wire refs. Literals become
    /// anonymous const nodes. Complex expressions (calls, nested BinOps)
    /// are compiled into anonymous intermediate nodes.
    fn compile_binop_operand(
        &mut self,
        asm: &mut GkAssembler,
        expr: &Expr,
    ) -> Result<WireRef, String> {
        match expr {
            Expr::Ident(id, _) => {
                if self.input_names.contains(id) {
                    Ok(WireRef::input(id))
                } else {
                    Ok(WireRef::node(id))
                }
            }
            Expr::FieldAccess { source, field, .. } => {
                Ok(WireRef::node(&format!("{source}__{field}")))
            }
            Expr::IntLit(v, _) => {
                let anon = self.anon_name();
                asm.add_node(&anon, Box::new(ConstU64::new(*v)), vec![]);
                Ok(WireRef::node(anon))
            }
            Expr::FloatLit(v, _) => {
                let anon = self.anon_name();
                asm.add_node(&anon, Box::new(ConstF64::new(*v)), vec![]);
                Ok(WireRef::node(anon))
            }
            _ => {
                let anon = self.anon_name();
                self.compile_binding(asm, &[anon.clone()], expr)?;
                Ok(WireRef::node(anon))
            }
        }
    }
}
