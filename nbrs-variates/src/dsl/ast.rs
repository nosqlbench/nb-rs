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
    /// `input name[: type]` — declares one per-cycle kernel input slot.
    /// The name becomes both an input slot (settable via `set_input`)
    /// and a passthrough output (readable via `get_constant`/`pull`).
    ///
    /// Surface forms (parser desugars the tuple into N `InputDecl`s,
    /// mirroring the module-signature param-list shape from
    /// `nbrs/stdlib/modeling.gk`):
    /// ```text
    /// input cycle: u64
    /// input (cycle: u64, q: f64)
    /// ```
    InputDecl(InputDecl),
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

/// One per-cycle kernel input slot.
///
/// Declared by `input <name>[: <type>]` (single) or
/// `input (<name>[: <type>], ...)` (tuple, sugar for N decls).
/// The name participates in the kernel's input-port wiring just
/// like `extern` participates in its port set, but inputs are
/// driven by the runtime cycle pump (cursors, captures, etc.)
/// rather than by external port writes.
///
/// `ty` is `None` when the author omitted the annotation; typed
/// downstream by inference. Authors are encouraged to declare
/// the type for clarity and editor support.
#[derive(Debug, Clone)]
pub struct InputDecl {
    pub name: String,
    pub ty: Option<String>,
    pub span: Span,
}

/// One wire-coloring keyword. The single enum that names every
/// modifier the grammar recognises before a binding name.
/// Future modifiers are new variants here.
///
/// Each variant maps to a token the lexer emits and a parser
/// branch in `parse_modified_binding`. A binding can carry zero
/// or more of these, stored as a [`BindingModifier`] set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WireModifier {
    /// `final` — folded into a const slot at compile/init time;
    /// immutable, cannot be shadowed by inner scopes.
    Final,
    /// `shared` — mutable cell visible across kernel instances.
    /// The runtime propagates iteration N's end state into
    /// iteration N+1's start state.
    Shared,
    /// `volatile` — wire's value is excluded from `hash_const`
    /// (the const-folded identity hash). Authors mark wires
    /// whose value should NOT contribute to resume-identity
    /// even when the source's structural detection would
    /// otherwise allow folding. See SRD-44 + design memo
    /// `resumable_test_fixture.md`.
    Volatile,
}

/// Set of wire modifiers carried by one binding declaration.
/// Stored as a bitset under the hood; consumers use
/// [`Self::has`] to test for individual modifiers and
/// [`Self::insert`] / [`Self::from_iter`] to build instances.
///
/// **Validity:** the combination `final` + `volatile` is
/// rejected at parse time as contradictory ([`Self::from_iter`]
/// is the validating builder). All other combinations are
/// representable.
///
/// **AST distinction is preserved separately** — `InitBinding` /
/// `CycleBinding` / `ExternPort` remain distinct AST node types
/// because their right-hand-side contracts differ. The
/// modifier set lives on the binding-kind structs that have a
/// notion of wire identity (currently `InitBinding` and
/// `CycleBinding`); kinds without that notion don't carry it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BindingModifier {
    bits: u8,
}

impl BindingModifier {
    /// All-modifiers-off; the default state of an unannotated
    /// binding.
    pub const NONE: Self = Self { bits: 0 };

    /// Single-modifier convenience constants. Tests reach for
    /// these to express their intent compactly.
    pub const FINAL:    Self = Self { bits: Self::bit(WireModifier::Final) };
    pub const SHARED:   Self = Self { bits: Self::bit(WireModifier::Shared) };
    pub const VOLATILE: Self = Self { bits: Self::bit(WireModifier::Volatile) };

    /// `true` iff `m` is set.
    pub const fn has(&self, m: WireModifier) -> bool {
        self.bits & Self::bit(m) != 0
    }

    /// `true` iff at least one modifier is set.
    pub const fn has_any(&self) -> bool {
        self.bits != 0
    }

    /// Add `m` to the set.
    pub fn insert(&mut self, m: WireModifier) {
        self.bits |= Self::bit(m);
    }

    /// Build a modifier set from an iterator of variants. The
    /// parser uses this after collecting tokens. Rejects the
    /// contradictory `final` + `volatile` combo with a clear
    /// error.
    pub fn from_iter<I: IntoIterator<Item = WireModifier>>(items: I) -> Result<Self, &'static str> {
        let mut out = Self::NONE;
        for m in items {
            out.insert(m);
        }
        if out.has(WireModifier::Final) && out.has(WireModifier::Volatile) {
            return Err(
                "modifier conflict: `final` and `volatile` are contradictory \
                 — `final` folds the value into a const slot at compile time, \
                 `volatile` excludes it from const-fold. Drop one.",
            );
        }
        Ok(out)
    }

    /// Iterate the modifiers in the set, in fixed declaration
    /// order (`Final`, `Shared`, `Volatile`). Used for
    /// re-emission and stable hash output.
    pub fn iter(&self) -> impl Iterator<Item = WireModifier> + '_ {
        const ORDER: &[WireModifier] = &[
            WireModifier::Final,
            WireModifier::Shared,
            WireModifier::Volatile,
        ];
        ORDER.iter().copied().filter(move |m| self.has(*m))
    }

    /// Direct field-style accessors retained for sites that
    /// pattern-match on individual flags. Mechanically derive
    /// from `has(...)` so adding a new modifier is one variant
    /// + one bit assignment + (optionally) one accessor.
    #[inline] pub const fn is_final(&self)    -> bool { self.has(WireModifier::Final) }
    #[inline] pub const fn is_shared(&self)   -> bool { self.has(WireModifier::Shared) }
    #[inline] pub const fn is_volatile(&self) -> bool { self.has(WireModifier::Volatile) }

    /// Compile-time bit index for a modifier.
    const fn bit(m: WireModifier) -> u8 {
        match m {
            WireModifier::Final    => 1 << 0,
            WireModifier::Shared   => 1 << 1,
            WireModifier::Volatile => 1 << 2,
        }
    }
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

#[cfg(test)]
mod modifier_tests {
    use super::*;

    #[test]
    fn empty_set_has_no_modifiers() {
        let m = BindingModifier::NONE;
        assert!(!m.has_any());
        assert!(!m.is_final() && !m.is_shared() && !m.is_volatile());
    }

    #[test]
    fn single_modifier_consts_match_expected_flags() {
        assert!(BindingModifier::FINAL.is_final());
        assert!(!BindingModifier::FINAL.is_shared());
        assert!(!BindingModifier::FINAL.is_volatile());

        assert!(BindingModifier::SHARED.is_shared());
        assert!(!BindingModifier::SHARED.is_final());

        assert!(BindingModifier::VOLATILE.is_volatile());
        assert!(!BindingModifier::VOLATILE.is_final());
    }

    #[test]
    fn from_iter_collects_combinations() {
        let m = BindingModifier::from_iter(
            [WireModifier::Final, WireModifier::Shared]
        ).expect("final+shared is valid");
        assert!(m.is_final() && m.is_shared());
        assert!(!m.is_volatile());

        let m = BindingModifier::from_iter(
            [WireModifier::Shared, WireModifier::Volatile]
        ).expect("shared+volatile is valid");
        assert!(m.is_shared() && m.is_volatile());
    }

    #[test]
    fn from_iter_rejects_final_plus_volatile() {
        let err = BindingModifier::from_iter(
            [WireModifier::Final, WireModifier::Volatile]
        ).expect_err("final+volatile must be rejected");
        assert!(err.contains("final") && err.contains("volatile"),
            "error should name both keywords: {err}");
    }

    #[test]
    fn from_iter_rejects_final_shared_volatile() {
        // Triple combination subsumes the contradiction.
        let err = BindingModifier::from_iter(
            [WireModifier::Final, WireModifier::Shared, WireModifier::Volatile]
        ).expect_err("triple combo includes the contradictory pair");
        assert!(err.contains("final") && err.contains("volatile"));
    }

    #[test]
    fn iter_yields_modifiers_in_stable_order() {
        let m = BindingModifier::from_iter(
            [WireModifier::Volatile, WireModifier::Shared]
        ).unwrap();
        // Insertion order was Volatile, Shared — but iter yields
        // in fixed declaration order: Final, Shared, Volatile.
        let collected: Vec<_> = m.iter().collect();
        assert_eq!(collected, vec![WireModifier::Shared, WireModifier::Volatile]);
    }

    #[test]
    fn equality_distinguishes_combinations() {
        let final_only = BindingModifier::FINAL;
        let final_shared = BindingModifier::from_iter(
            [WireModifier::Final, WireModifier::Shared]
        ).unwrap();
        assert_ne!(final_only, final_shared,
            "final-only must not equal final+shared");
    }
}
