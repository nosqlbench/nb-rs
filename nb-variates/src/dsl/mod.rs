// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK DSL: lexer, parser, and AST for `.gk` kernel definition files.

pub mod lexer;
pub mod ast;
pub mod parser;
pub mod compile;
pub mod error;
pub mod registry;
pub mod events;
pub(crate) mod factory;
pub(crate) mod validate;

/// Re-exported for external crates that register GK nodes via `register_nodes!`.
///
/// External node crates need to name `ConstArg` in their builder function
/// signatures.  Exporting it here (via `pub use`) makes it reachable as
/// `nb_variates::dsl::ConstArg` without exposing the full factory module.
pub use factory::ConstArg;
mod modules;
mod binding;

pub use compile::{compile_gk, compile_gk_checked, compile_gk_with_path, compile_gk_strict, compile_gk_with_outputs, compile_gk_with_libs, compile_gk_with_libs_and_limit, eval_const_expr};

/// Return the embedded standard library module sources.
///
/// Each entry is `(filename, source_text)` — the same data used by the
/// compiler's module resolver at build time.
pub fn stdlib_sources() -> &'static [(&'static str, &'static str)] {
    compile::stdlib_sources()
}
