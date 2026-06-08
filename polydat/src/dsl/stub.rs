// Copyright (c) nosqlbench
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! SRD-84 Part 3 — caller-native, typed polydat expression stubs.
//!
//! Lets Rust code build a polydat binding from an expression and emit
//! it as a [`Statement`] for a grammar-safe
//! [`crate::kernel::subcontext::module::BodyFragment::Statements`]
//! (SRD-84 Part 2) — **without** concatenating source strings. The
//! return type is bound at the call site via the SRD-80b [`Wire`]
//! trait, so the Rust generic and the polydat target type are one and
//! the same.
//!
//! Synthesizers (metrics, poll, stop conditions) build stubs; only
//! user-authored predicate *text* is parsed, once, at the boundary
//! ([`ExprStub::parse`]).

use crate::ast::{PortType, Value};
use crate::derive_support::Wire;
use crate::dsl::ast::{Binding, BindingModifier, Expr, ExternPort, Statement, WireModifier};
use crate::dsl::lexer::Span;
use crate::kernel::{Dataflow, Metadata, PolydatKernel};

/// A caller-native expression stub: a named binding over a polydat
/// expression, optionally type-coerced (via the SRD-84 Part 1b `as`
/// cast) and `volatile`.
pub struct ExprStub {
    name: String,
    expr: Expr,
    modifier: BindingModifier,
}

impl ExprStub {
    /// Build a stub from an already-constructed expression.
    pub fn new(name: impl Into<String>, expr: Expr) -> Self {
        Self { name: name.into(), expr, modifier: BindingModifier::default() }
    }

    /// Build a stub by parsing a single expression from source — the
    /// *boundary parse* for user-authored predicate text. Thereafter
    /// the stub is grammar-safe (it flows as AST, never re-rendered to
    /// a string).
    pub fn parse(name: impl Into<String>, source: &str) -> Result<Self, String> {
        let tokens = crate::dsl::lexer::lex(source)?;
        let expr = crate::dsl::parser::parse_expression(tokens)?;
        Ok(Self::new(name, expr))
    }

    /// Coerce the stub's value to `T`'s polydat type via the SRD-84
    /// Part 1b `as <type>` cast — alignment-only, a no-op when the
    /// expression is already `T::PORT`. The Rust generic *is* the
    /// polydat target type.
    pub fn returning<T: Wire>(mut self) -> Self {
        self.expr = Expr::Cast(Box::new(self.expr), T::PORT, Span { line: 0, col: 0 });
        self
    }

    /// Mark the binding `volatile` — re-evaluated on every pull (e.g. a
    /// stop-condition predicate evaluated per trigger).
    pub fn volatile(mut self) -> Self {
        self.modifier.insert(WireModifier::Volatile);
        self
    }

    /// The binding statement this stub becomes — drop it into a
    /// `BodyFragment::Statements` and the kernel builder consumes it
    /// directly, no re-parse.
    pub fn into_statement(self) -> Statement {
        Statement::Binding(Binding {
            targets: vec![self.name],
            value: self.expr,
            modifier: self.modifier,
            span: Span { line: 0, col: 0 },
        })
    }
}

/// SRD-84 **shape 1** — grammar-safe *graph matter*: a bundle of
/// statements the polydat kernel compiler turns into a kernel. Built
/// programmatically (typed externs + `ExprStub` bindings), never from a
/// source string. Feeds `PolydatMatter` / `BodyFragment::Statements`.
#[derive(Default)]
pub struct GraphMatter {
    statements: Vec<Statement>,
}

impl GraphMatter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a typed `extern` wire (a runtime input the kernel reads),
    /// **constructed** from the `Wire` type — not string-parsed. The
    /// Rust generic *is* the polydat port type.
    pub fn extern_wire<T: Wire>(&mut self, name: impl Into<String>) -> &mut Self {
        let span = Span { line: 0, col: 0 };
        let default = match T::PORT {
            PortType::F64 => Expr::FloatLit(0.0, span),
            _ => Expr::IntLit(0, span),
        };
        self.statements.push(Statement::ExternPort(ExternPort {
            name: name.into(),
            typ: T::PORT.to_keyword().to_string(),
            default: Some(default),
            span,
        }));
        self
    }

    /// Append an [`ExprStub`]'s binding statement.
    pub fn bind(&mut self, stub: ExprStub) -> &mut Self {
        self.statements.push(stub.into_statement());
        self
    }

    /// The statements, for `PolydatMatter::builder().statements(...)` or
    /// a `BodyFragment::Statements`.
    pub fn into_statements(self) -> Vec<Statement> {
        self.statements
    }
}

/// SRD-84 **shape 2** — a polydat expression *bound to a parent
/// kernel's lexical scope*. Compiled into a sub-context whose named
/// output is the expression, evaluable many times against injected
/// inputs. The return is whatever `Wire` type the bound stub was
/// qualified with (`ExprStub::returning::<T>`), or its natural
/// truthiness (`is_true`). A general-purpose, scope-bound, callable
/// expression holder.
pub struct ScopedExpr {
    kernel: PolydatKernel,
    output: String,
}

impl ScopedExpr {
    /// Bind `matter` — which must define the named `output` (plus any
    /// extern wires it reads) — into a sub-context of `parent`. The
    /// expression is compiled once; call it repeatedly via `eval` /
    /// `is_true` after `set`-ing its inputs.
    pub fn bind(
        parent: &PolydatKernel,
        output: impl Into<String>,
        matter: GraphMatter,
    ) -> Result<Self, String> {
        let pm = crate::kernel::subcontext::PolydatMatter::builder()
            .statements(matter.into_statements())
            .build()
            .map_err(|e| format!("scoped-expr matter: {e:?}"))?;
        let kernel = parent.build_subscope(pm).map_err(|e| format!("scoped-expr subscope: {e:?}"))?;
        Ok(Self { kernel, output: output.into() })
    }

    /// Set a runtime input wire by name before evaluating. No-op for a
    /// name the expression doesn't read.
    pub fn set(&mut self, name: &str, value: Value) -> &mut Self {
        if let Some(idx) = self.kernel.find_input(name) {
            let _ = self.kernel.set_wire_idx(idx, value);
        }
        self
    }

    /// The bound sub-context as a [`Dataflow`], for callers that inject
    /// a batch of inputs through a `Dataflow`-based injector (e.g. a
    /// runtime-state snapshot) before evaluating.
    pub fn dataflow(&mut self) -> &mut PolydatKernel {
        &mut self.kernel
    }

    /// Evaluate (pull) the bound expression's output.
    pub fn eval(&mut self) -> Value {
        self.kernel.pull(&self.output).clone()
    }

    /// Evaluate as a boolean — the default truthiness sense. Polydat
    /// comparisons / `&&` / `||` yield `U64` `0/1` (not `Bool`), so
    /// truthiness is "non-zero".
    pub fn is_true(&mut self) -> bool {
        match self.eval() {
            Value::Bool(b) => b,
            Value::F64(v) => v != 0.0,
            v => v.as_u64() != 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::PortType;

    #[test]
    fn parse_stub_builds_typed_volatile_binding() {
        // A predicate parsed at the boundary, coerced to u64 truthiness,
        // marked volatile — the stop-condition stub shape.
        let stmt = ExprStub::parse("__pred", "op_count > 50")
            .expect("parse")
            .returning::<u64>()
            .volatile()
            .into_statement();
        match stmt {
            Statement::Binding(b) => {
                assert_eq!(b.targets, vec!["__pred".to_string()]);
                assert!(b.modifier.has(WireModifier::Volatile), "must be volatile");
                // The value is `(<comparison>) as u64` — a grammar-safe
                // Cast wrapping the parsed comparison, no string round-trip.
                assert!(matches!(b.value, Expr::Cast(_, PortType::U64, _)),
                    "value must be a Cast to U64, got {:?}", b.value);
            }
            other => panic!("expected a Binding statement, got {other:?}"),
        }
    }

    #[test]
    fn returning_binds_the_rust_generic_as_the_polydat_type() {
        // The Rust generic and the polydat target are the same: f64 here.
        let stmt = ExprStub::parse("__m", "elapsed_ms")
            .expect("parse")
            .returning::<f64>()
            .into_statement();
        let Statement::Binding(b) = stmt else { panic!("expected Binding") };
        assert!(matches!(b.value, Expr::Cast(_, PortType::F64, _)));
        assert!(!b.modifier.has(WireModifier::Volatile), "no volatile unless requested");
    }

    #[test]
    fn scoped_expr_binds_to_a_kernel_scope_and_is_callable() {
        // SRD-84 shape 2 — a scoped, callable expression. Shape 1
        // (`GraphMatter`) builds the matter: a typed extern wire plus a
        // predicate stub. Shape 2 (`ScopedExpr`) binds it into a
        // sub-context of a parent kernel and evaluates it many times
        // against injected inputs, as truthiness.
        use crate::ast::Value;
        let parent = crate::dsl::compile_polydat("input cycle: u64\nx := 5")
            .expect("parent kernel");
        let mut matter = GraphMatter::new();
        matter
            .extern_wire::<u64>("threshold")
            .bind(ExprStub::parse("__pred", "threshold > 50")
                .expect("parse").returning::<u64>().volatile());
        let mut scoped = ScopedExpr::bind(&parent, "__pred", matter)
            .expect("bind to parent scope");
        assert!(scoped.set("threshold", Value::U64(100)).is_true(), "100 > 50 → true");
        assert!(!scoped.set("threshold", Value::U64(10)).is_true(), "10 > 50 → false");
    }
}
