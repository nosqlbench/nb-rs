// Copyright 2024-2026 Jonathan Shook
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
    /// `name(param: type, ...) -> (output: type, ...) := { body }`
    ModuleDef(ModuleDef),
    /// `extern name: type = default`
    ExternPort(ExternPort),
    /// `cursor name = Cursor()` or `cursor name = constructor_expr`
    Cursor(CursorDecl),
    /// `pragma <name>` — a module-level directive opting into a
    /// compile-time graph transform (SRD 15 §"Module-Level
    /// Pragmas"). First-class grammar, distinct from line
    /// comments. Recognised pragmas trigger
    /// `CompileEvent::PragmaAcknowledged`; unknown names trigger
    /// `CompileEvent::UnknownPragma` and are otherwise ignored
    /// (forward-compatible).
    Pragma { name: String, span: Span },
}

/// An external input port declaration.
///
/// Ports persist across `set_inputs()` calls within a stanza.
/// Written by capture extraction, read by GK nodes.
///
/// ```text
/// extern balance: f64 = 0.0
/// extern session_id: u64 = 0
/// ```
#[derive(Debug, Clone)]
pub struct ExternPort {
    pub name: String,
    pub typ: String,
    pub default: Option<Expr>,
    pub span: Span,
}

/// Modifier on a binding declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingModifier {
    /// No modifier — default behavior.
    None,
    /// `shared` — mutable across iteration boundaries.
    /// The runtime propagates the value from iteration N's
    /// end state into iteration N+1's start state.
    Shared,
    /// `final` — immutable; cannot be shadowed by inner scopes.
    Final,
}

/// An init-time binding: `init name = expr`
#[derive(Debug, Clone)]
pub struct InitBinding {
    pub name: String,
    pub value: Expr,
    pub modifier: BindingModifier,
    pub span: Span,
}

/// A cycle-time binding: `name := expr` or `(a, b, c) := expr`
#[derive(Debug, Clone)]
pub struct CycleBinding {
    pub targets: Vec<String>,
    pub value: Expr,
    pub modifier: BindingModifier,
    pub span: Span,
}

/// A cursor declaration: `cursor name = Cursor()`
///
/// Declares a named positional cursor. The cursor's extent is
/// discovered at init time by interrogating its downstream consumers
/// for cardinality. The runtime advances the cursor to drive
/// phase iteration.
#[derive(Debug, Clone)]
pub struct CursorDecl {
    pub name: String,
    pub constructor: Expr,
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
    /// A binary arithmetic operation: `a + b`, `x * 0.25`.
    /// Desugared by the compiler into the equivalent function call.
    BinOp(Box<Expr>, BinOpKind, Box<Expr>),
    /// Unary negation: `-x`.
    /// Desugared to `f64_sub(0.0, x)`.
    UnaryNeg(Box<Expr>, Span),
    /// Unary bitwise NOT: `!x`.
    /// Desugared to `u64_not(x)`.
    UnaryBitNot(Box<Expr>, Span),
    /// Source field projection: `base.ordinal`, `base.vector`.
    /// Resolved by the compiler to a node that reads from the source item.
    FieldAccess {
        source: String,
        field: String,
        span: Span,
    },
}

/// Binary arithmetic operator kind.
#[derive(Debug, Clone, Copy)]
pub enum BinOpKind {
    /// `+` — desugars to `u64_add` or `f64_add` based on operand types
    Add,
    /// `-` — desugars to `u64_sub` or `f64_sub` based on operand types
    Sub,
    /// `*` — desugars to `u64_mul` or `f64_mul` based on operand types
    Mul,
    /// `/` — desugars to `u64_div` or `f64_div` based on operand types
    Div,
    /// `%` — desugars to `u64_mod` or `f64_mod` based on operand types
    Mod,
    /// `**` — desugars to `pow(a, b)` (always f64)
    Pow,
    /// `&` — desugars to `u64_and(a, b)`
    BitAnd,
    /// `|` — desugars to `u64_or(a, b)`
    BitOr,
    /// `^` — desugars to `u64_xor(a, b)`
    BitXor,
    /// `<<` — desugars to `u64_shl(a, b)`
    Shl,
    /// `>>` — desugars to `u64_shr(a, b)`
    Shr,
    /// `==` — desugars to `u64_eq` / `f64_eq`. Output type is `u64`
    /// (0 = false, 1 = true).
    Eq,
    /// `!=` — desugars to `u64_ne` / `f64_ne`. Output type is `u64`.
    Ne,
    /// `<` — desugars to `u64_lt` / `f64_lt`. Output type is `u64`.
    Lt,
    /// `>` — desugars to `u64_gt` / `f64_gt`. Output type is `u64`.
    Gt,
    /// `<=` — desugars to `u64_le` / `f64_le`. Output type is `u64`.
    Le,
    /// `>=` — desugars to `u64_ge` / `f64_ge`. Output type is `u64`.
    Ge,
}

/// A typed parameter in a module signature.
#[derive(Debug, Clone)]
pub struct TypedParam {
    pub name: String,
    pub typ: String,  // "u64", "f64", "String", "bytes", etc.
}

/// A formal module definition with typed interface.
///
/// ```text
/// hash_range(input: u64, max: u64) -> (value: u64) := {
///     h := hash(input)
///     value := mod(h, max)
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ModuleDef {
    pub name: String,
    pub params: Vec<TypedParam>,
    pub outputs: Vec<TypedParam>,
    pub body: Vec<Statement>,
    pub span: Span,
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
