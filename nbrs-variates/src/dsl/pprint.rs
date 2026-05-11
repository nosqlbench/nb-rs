// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! AST → `.gk` source pretty-printer.
//!
//! Used by the subscope synthesizer (SRD-13f §"Wire-reference
//! classification") to re-emit retained AST statements pulled
//! from a parent program's `binding_ast_for` into a child scope's
//! source-text input. The pretty-printer is the bridge between
//! AST-as-metadata (canonical) and the current string-based
//! synthesizer pipeline. A direct AST-mode compile path is the
//! eventual end state, but until then the synthesizer needs a
//! faithful AST → source round-trip.
//!
//! ## Round-trip contract
//!
//! For every `Statement`/`Expr` produced by the parser,
//! `pp_statement` / `pp_expr` produces source text that re-parses
//! into a semantically equivalent AST. "Semantically equivalent"
//! means same node types and identical inner data (modulo
//! `Span`s, which capture parser position and are not preserved
//! across re-parse).
//!
//! ## Precedence and parens
//!
//! `BinOp` expressions are emitted with parens around the whole
//! expression. This is uniformly safe — re-parsing produces the
//! same tree structure — at the cost of extra parens. The
//! synthesizer's output is not user-facing; legibility is not a
//! concern.

use crate::dsl::ast::{
    Arg, BindingModifier, CallExpr, CycleBinding, Expr, GkFile, InitBinding,
    ModuleDef, Statement, BinOpKind, ExternPort, CursorDecl, WireModifier,
};

/// Pretty-print a full file: every statement, separated by
/// newlines.
pub fn pp_file(file: &GkFile) -> String {
    let mut out = String::new();
    for stmt in &file.statements {
        out.push_str(&pp_statement(stmt));
        out.push('\n');
    }
    out
}

/// Pretty-print a top-level statement.
pub fn pp_statement(stmt: &Statement) -> String {
    match stmt {
        Statement::Inputs(names, _) => {
            format!("inputs := ({})", names.join(", "))
        }
        Statement::InitBinding(b) => pp_init_binding(b),
        Statement::CycleBinding(b) => pp_cycle_binding(b),
        Statement::ModuleDef(m) => pp_module_def(m),
        Statement::ExternPort(p) => pp_extern_port(p),
        Statement::Cursor(c) => pp_cursor(c),
        Statement::Pragma { name, .. } => format!("pragma {name}"),
    }
}

/// Pretty-print an expression. Always emits parens around
/// `BinOp` for round-trip safety.
pub fn pp_expr(expr: &Expr) -> String {
    match expr {
        Expr::Ident(name, _) => name.clone(),
        Expr::IntLit(v, _) => v.to_string(),
        Expr::FloatLit(v, _) => format_float(*v),
        Expr::StringLit(s, _) => format!("\"{}\"", escape_string(s)),
        Expr::ArrayLit(elts, _) => {
            let parts: Vec<String> = elts.iter().map(pp_expr).collect();
            format!("[{}]", parts.join(", "))
        }
        Expr::Call(c) => pp_call(c),
        Expr::BinOp(lhs, op, rhs) => {
            format!("({} {} {})", pp_expr(lhs), pp_binop(*op), pp_expr(rhs))
        }
        Expr::UnaryNeg(e, _) => format!("(-{})", pp_expr(e)),
        Expr::UnaryBitNot(e, _) => format!("(!{})", pp_expr(e)),
        Expr::FieldAccess { source, field, .. } => format!("{source}.{field}"),
    }
}

fn pp_init_binding(b: &InitBinding) -> String {
    let prefix = pp_modifier_prefix(b.modifier);
    if b.modifier.has(WireModifier::Final) || prefix.is_empty() {
        // No `init` keyword; either bare or `final`/`shared`/`volatile` prefix.
        // The compiler treats `final x := lit` and `init x = lit` differently
        // (CycleBinding vs InitBinding). InitBinding is specifically the
        // `init name = expr` syntax. So this branch only fires when an
        // InitBinding genuinely has the `init` keyword — meaning prefix is
        // empty (no other modifier).
        format!("init {} = {}", b.name, pp_expr(&b.value))
    } else {
        format!("init {} = {}", b.name, pp_expr(&b.value))
    }
}

fn pp_cycle_binding(b: &CycleBinding) -> String {
    let target = if b.targets.len() == 1 {
        b.targets[0].clone()
    } else {
        format!("({})", b.targets.join(", "))
    };
    let prefix = pp_modifier_prefix(b.modifier);
    if prefix.is_empty() {
        format!("{} := {}", target, pp_expr(&b.value))
    } else {
        format!("{} {} := {}", prefix, target, pp_expr(&b.value))
    }
}

fn pp_extern_port(p: &ExternPort) -> String {
    if let Some(default) = &p.default {
        format!("extern {}: {} = {}", p.name, p.typ, pp_expr(default))
    } else {
        format!("extern {}: {}", p.name, p.typ)
    }
}

fn pp_cursor(c: &CursorDecl) -> String {
    format!("cursor {} = {}", c.name, pp_expr(&c.constructor))
}

fn pp_module_def(m: &ModuleDef) -> String {
    let params: Vec<String> = m.params.iter()
        .map(|p| format!("{}: {}", p.name, p.typ))
        .collect();
    let outputs: Vec<String> = m.outputs.iter()
        .map(|p| format!("{}: {}", p.name, p.typ))
        .collect();
    let mut body = String::new();
    for s in &m.body {
        body.push_str("    ");
        body.push_str(&pp_statement(s));
        body.push('\n');
    }
    format!("{}({}) -> ({}) := {{\n{}}}", m.name, params.join(", "), outputs.join(", "), body)
}

fn pp_call(c: &CallExpr) -> String {
    let args: Vec<String> = c.args.iter().map(pp_arg).collect();
    format!("{}({})", c.func, args.join(", "))
}

fn pp_arg(arg: &Arg) -> String {
    match arg {
        Arg::Positional(e) => pp_expr(e),
        Arg::Named(name, e) => format!("{}: {}", name, pp_expr(e)),
    }
}

fn pp_modifier_prefix(m: BindingModifier) -> String {
    let mut parts: Vec<&str> = Vec::new();
    if m.has(WireModifier::Final)    { parts.push("final"); }
    if m.has(WireModifier::Shared)   { parts.push("shared"); }
    if m.has(WireModifier::Volatile) { parts.push("volatile"); }
    parts.join(" ")
}

fn pp_binop(op: BinOpKind) -> &'static str {
    match op {
        BinOpKind::Add => "+",
        BinOpKind::Sub => "-",
        BinOpKind::Mul => "*",
        BinOpKind::Div => "/",
        BinOpKind::Mod => "%",
        BinOpKind::Pow => "**",
        BinOpKind::BitAnd => "&",
        BinOpKind::BitOr => "|",
        BinOpKind::BitXor => "^",
        BinOpKind::Shl => "<<",
        BinOpKind::Shr => ">>",
        BinOpKind::Eq => "==",
        BinOpKind::Ne => "!=",
        BinOpKind::Lt => "<",
        BinOpKind::Gt => ">",
        BinOpKind::Le => "<=",
        BinOpKind::Ge => ">=",
    }
}

fn escape_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out
}

fn format_float(v: f64) -> String {
    if v.is_finite() && v == v.trunc() && v.abs() < 1e18 {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{lexer, parser};

    fn parse(src: &str) -> GkFile {
        let tokens = lexer::lex(src).expect("lex");
        parser::parse(tokens).expect("parse")
    }

    fn round_trip(src: &str) {
        let ast1 = parse(src);
        let printed = pp_file(&ast1);
        let ast2 = parse(&printed);
        let printed2 = pp_file(&ast2);
        assert_eq!(
            printed, printed2,
            "second-pass print should be idempotent.\n\
             original source:\n{src}\n\n\
             first print:\n{printed}\n\n\
             second print:\n{printed2}"
        );
    }

    #[test]
    fn round_trip_simple_const() {
        round_trip("final x := 42\n");
    }

    #[test]
    fn round_trip_string_const() {
        round_trip("final dataset := \"sift1m\"\n");
    }

    #[test]
    fn round_trip_init_binding() {
        round_trip("init prebuffer = dataset_prebuffer(\"example\")\n");
    }

    #[test]
    fn round_trip_function_call() {
        round_trip("ratio := mod(cycle, 100)\n");
    }

    #[test]
    fn round_trip_named_args() {
        round_trip("v := dist_normal(mean: 72.0, stddev: 5.0)\n");
    }

    #[test]
    fn round_trip_binop() {
        round_trip("y := (x + 1)\n");
    }

    #[test]
    fn round_trip_inputs() {
        round_trip("inputs := (cycle, thread)\n");
    }

    #[test]
    fn round_trip_extern() {
        round_trip("extern dataset: String\n");
    }

    #[test]
    fn round_trip_tuple_destructure() {
        round_trip("(a, b) := unpack(cycle)\n");
    }

    #[test]
    fn round_trip_workload_typical() {
        // Mirrors the shape of full_cql_vector workload bindings.
        let src = "\
final dataset := \"sift1m\"
final prefix := \"vec_default\"
profiles := matching_profiles(dataset, prefix)
table := first(profiles)
";
        round_trip(src);
    }

    #[test]
    fn round_trip_string_escapes() {
        round_trip("final s := \"hello \\\"world\\\"\"\n");
    }

    #[test]
    fn round_trip_array_literal() {
        round_trip("final weights := [60.0, 20.0, 15.0, 5.0]\n");
    }
}
