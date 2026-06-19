// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polydat DSL: lexer, parser, and AST for `.polydat` kernel definition files.

pub mod lexer;
pub mod ast;
pub mod parser;
pub mod compile;
pub mod error;
pub mod registry;
pub mod events;
pub mod const_constraints;
pub mod pragmas;
pub mod cursor_sugar;
pub mod pprint;
/// External-facing factory module. Made `pub` (was `pub(crate)`) so
/// crates that host polydat nodes outside the polydat crate
/// (host runtimes) can reach `ConstArg`, `compile_ctx`,
/// and `build_node` from their `register_nodes!` invocations.
pub mod factory;
pub mod factories;
pub(crate) mod validate;

/// Grammar-based free-name extraction — the canonical way for
/// validators and the YAML-fusion layer to learn which names an
/// expression references (no byte-scanning). See [`refs`].
pub mod refs;
pub mod stub;

/// Re-exported for external crates that register Polydat nodes via `register_nodes!`.
pub use factory::ConstArg;
mod modules;
mod binding;

pub use compile::{compile_polydat, compile_polydat_checked, compile_polydat_with_path, compile_polydat_strict, compile_polydat_with_outputs, compile_polydat_with_libs, compile_polydat_with_libs_and_limit, eval_const_expr};

/// Collect identifier references from an `Expr` tree into `out`.
///
/// Walks every `Ident`, function call argument, binary/unary
/// operand, array element, and field-access source. String-
/// literal placeholders (`{name}` form) contribute their
/// identifier-shaped placeholder bodies.
///
/// Cross-crate consumers (e.g. the host's SRD-13f
/// synthesizer) use this to discover transitive wire refs from
/// a binding's RHS without depending on the private `validate`
/// module.
pub fn collect_expr_references(
    expr: &ast::Expr,
    out: &mut std::collections::HashSet<String>,
) {
    validate::collect_references(expr, out);
}

/// Return the embedded standard library module sources.
///
/// Each entry is `(filename, source_text)` — the same data used by the
/// compiler's module resolver at build time.
pub fn stdlib_sources() -> &'static [(&'static str, &'static str)] {
    compile::stdlib_sources()
}
