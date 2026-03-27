// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Abstract syntax tree for the GK DSL.

use crate::dsl::lexer::Span;

/// A complete `.gk` file.
#[derive(Debug, Clone)]
pub struct GkFile {
    pub statements: Vec<Statement>,
}

/// A top-level statement.
#[derive(Debug, Clone)]
pub enum Statement {
    /// `coordinates := (name1, name2, ...)`
    Coordinates(Vec<String>, Span),
    /// `init name = expr`
    InitBinding(InitBinding),
    /// `name := expr` or `(a, b) := expr`
    CycleBinding(CycleBinding),
}

/// An init-time binding: `init name = expr`
#[derive(Debug, Clone)]
pub struct InitBinding {
    pub name: String,
    pub value: Expr,
    pub span: Span,
}

/// A cycle-time binding: `name := expr` or `(a, b, c) := expr`
#[derive(Debug, Clone)]
pub struct CycleBinding {
    pub targets: Vec<String>,
    pub value: Expr,
    pub span: Span,
}

/// An expression (right-hand side of a binding).
#[derive(Debug, Clone)]
pub enum Expr {
    /// A bare identifier referencing a wire or init binding: `cycle`, `lut`
    Ident(String, Span),
    /// An integer literal: `1000`
    IntLit(u64, Span),
    /// A float literal: `72.0`
    FloatLit(f64, Span),
    /// A string literal (may contain `{name}` interpolation): `"hello {name}"`
    StringLit(String, Span),
    /// An array literal: `[60.0, 20.0, 15.0]`
    ArrayLit(Vec<Expr>, Span),
    /// A function call: `hash(cycle)`, `dist_normal(mean: 72.0, stddev: 5.0)`
    Call(CallExpr),
}

/// A function call expression.
#[derive(Debug, Clone)]
pub struct CallExpr {
    pub func: String,
    pub args: Vec<Arg>,
    pub span: Span,
}

/// A function argument: positional or named.
#[derive(Debug, Clone)]
pub enum Arg {
    /// Positional: just an expression
    Positional(Expr),
    /// Named: `name: expr`
    Named(String, Expr),
}
