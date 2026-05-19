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
    /// A name-to-expression binding. The modifier on the
    /// binding determines its lifecycle:
    ///
    /// - **no modifier** — per-cycle: re-evaluated every cycle.
    /// - **`const`** — effectively-const for the scope's
    ///   lifetime: materialized at the earliest opportunity
    ///   (compile-time fold if the RHS is fold-eligible,
    ///   otherwise scope-init pull after materialize-wiring
    ///   has populated extern slots). Authors don't need to
    ///   know which path the runtime takes — the contract is
    ///   "fixed once, then immutable."
    /// - **`shared`** — cell-backed, mutable across kernel
    ///   instances in the same lineage. See SRD-16
    ///   §"Mutability Rules: Shared Mutable".
    /// - **`volatile`** — per-cycle, excluded from
    ///   `hash_const`. See SRD-44.
    ///
    /// Surface forms:
    /// ```text
    /// x := mul(cycle, 2)                  // per-cycle
    /// const pi := 3.14                    // const, folds at compile
    /// const ann_opts := str_concat(...)   // const, materializes at scope-init
    /// shared budget := 100                // shared cell
    /// (a, b) := split_pair(...)           // tuple destructuring
    /// ```
    Binding(Binding),
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
    /// `const` — effectively-const for the scope's lifetime.
    /// Materialized at the earliest opportunity: compile-time
    /// const-fold when the RHS is fold-eligible, otherwise the
    /// scope-init pull pass after materialize-wiring has
    /// populated extern slots. The runtime contract is "fixed
    /// once per scope activation, then immutable for the rest of
    /// the scope's lifetime." Replaces the former `final` /
    /// `init` distinction — the two were redundant axes of the
    /// same lifecycle, and the surface now collapses to one
    /// keyword whose materialization timing is an internal
    /// optimization.
    Const,
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
/// **Validity:** the combination `const` + `volatile` is
/// rejected at parse time as contradictory ([`Self::from_iter`]
/// is the validating builder). All other combinations are
/// representable.
///
/// Lives on every [`Statement::Binding`] — the modifier set
/// determines the binding's lifecycle. Other statement kinds
/// (`ExternPort`, `InputDecl`, etc.) don't carry modifiers
/// because their semantics are fixed by their statement form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BindingModifier {
    bits: u8,
}

impl BindingModifier {
    /// All-modifiers-off; the default state of an unannotated
    /// binding (per-cycle).
    pub const NONE: Self = Self { bits: 0 };

    /// Single-modifier convenience constants. Tests reach for
    /// these to express their intent compactly.
    pub const CONST:    Self = Self { bits: Self::bit(WireModifier::Const) };
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
    /// contradictory `const` + `volatile` combo with a clear
    /// error.
    pub fn from_iter<I: IntoIterator<Item = WireModifier>>(items: I) -> Result<Self, &'static str> {
        let mut out = Self::NONE;
        for m in items {
            out.insert(m);
        }
        if out.has(WireModifier::Const) && out.has(WireModifier::Volatile) {
            return Err(
                "modifier conflict: `const` and `volatile` are contradictory \
                 — `const` materializes the value once and freezes it; \
                 `volatile` excludes the wire from const-fold and signals \
                 per-cycle variability. Drop one.",
            );
        }
        Ok(out)
    }

    /// Iterate the modifiers in the set, in fixed declaration
    /// order (`Const`, `Shared`, `Volatile`). Used for
    /// re-emission and stable hash output.
    pub fn iter(&self) -> impl Iterator<Item = WireModifier> + '_ {
        const ORDER: &[WireModifier] = &[
            WireModifier::Const,
            WireModifier::Shared,
            WireModifier::Volatile,
        ];
        ORDER.iter().copied().filter(move |m| self.has(*m))
    }

    /// Direct field-style accessors retained for sites that
    /// pattern-match on individual flags. Mechanically derive
    /// from `has(...)` so adding a new modifier is one variant
    /// + one bit assignment + (optionally) one accessor.
    #[inline] pub const fn is_const(&self)    -> bool { self.has(WireModifier::Const) }
    #[inline] pub const fn is_shared(&self)   -> bool { self.has(WireModifier::Shared) }
    #[inline] pub const fn is_volatile(&self) -> bool { self.has(WireModifier::Volatile) }

    /// Compile-time bit index for a modifier.
    const fn bit(m: WireModifier) -> u8 {
        match m {
            WireModifier::Const    => 1 << 0,
            WireModifier::Shared   => 1 << 1,
            WireModifier::Volatile => 1 << 2,
        }
    }
}

/// A name-to-expression binding (per-cycle by default;
/// `const`/`shared`/`volatile` modifier changes the
/// lifecycle). Replaces the former `CycleBinding` and
/// `InitBinding` AST variants — the surface unified to one
/// shape `name := expr` (or `(a, b, c) := expr` for tuple
/// destructuring), with the modifier driving runtime
/// lifecycle.
#[derive(Debug, Clone)]
pub struct Binding {
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
        assert!(!m.is_const() && !m.is_shared() && !m.is_volatile());
    }

    #[test]
    fn single_modifier_consts_match_expected_flags() {
        assert!(BindingModifier::CONST.is_const());
        assert!(!BindingModifier::CONST.is_shared());
        assert!(!BindingModifier::CONST.is_volatile());

        assert!(BindingModifier::SHARED.is_shared());
        assert!(!BindingModifier::SHARED.is_const());

        assert!(BindingModifier::VOLATILE.is_volatile());
        assert!(!BindingModifier::VOLATILE.is_const());
    }

    #[test]
    fn from_iter_collects_combinations() {
        let m = BindingModifier::from_iter(
            [WireModifier::Const, WireModifier::Shared]
        ).expect("const+shared is valid");
        assert!(m.is_const() && m.is_shared());
        assert!(!m.is_volatile());

        let m = BindingModifier::from_iter(
            [WireModifier::Shared, WireModifier::Volatile]
        ).expect("shared+volatile is valid");
        assert!(m.is_shared() && m.is_volatile());
    }

    #[test]
    fn from_iter_rejects_const_plus_volatile() {
        let err = BindingModifier::from_iter(
            [WireModifier::Const, WireModifier::Volatile]
        ).expect_err("const+volatile must be rejected");
        assert!(err.contains("const") && err.contains("volatile"),
            "error should name both keywords: {err}");
    }

    #[test]
    fn from_iter_rejects_const_shared_volatile() {
        // Triple combination subsumes the contradiction.
        let err = BindingModifier::from_iter(
            [WireModifier::Const, WireModifier::Shared, WireModifier::Volatile]
        ).expect_err("triple combo includes the contradictory pair");
        assert!(err.contains("const") && err.contains("volatile"));
    }

    #[test]
    fn iter_yields_modifiers_in_stable_order() {
        let m = BindingModifier::from_iter(
            [WireModifier::Volatile, WireModifier::Shared]
        ).unwrap();
        // Insertion order was Volatile, Shared — but iter yields
        // in fixed declaration order: Const, Shared, Volatile.
        let collected: Vec<_> = m.iter().collect();
        assert_eq!(collected, vec![WireModifier::Shared, WireModifier::Volatile]);
    }

    #[test]
    fn equality_distinguishes_combinations() {
        let const_only = BindingModifier::CONST;
        let const_shared = BindingModifier::from_iter(
            [WireModifier::Const, WireModifier::Shared]
        ).unwrap();
        assert_ne!(const_only, const_shared,
            "const-only must not equal const+shared");
    }
}
