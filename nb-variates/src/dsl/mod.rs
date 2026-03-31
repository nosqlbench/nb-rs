// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK DSL: lexer, parser, and AST for `.gk` kernel definition files.

pub mod lexer;
pub mod ast;
pub mod parser;
pub mod compile;
pub mod error;
pub mod registry;

pub use compile::{compile_gk, compile_gk_checked, compile_gk_with_path, compile_gk_strict, compile_gk_with_outputs, compile_gk_with_libs};

/// Return the embedded standard library module sources.
///
/// Each entry is `(filename, source_text)` — the same data used by the
/// compiler's module resolver at build time.
pub fn stdlib_sources() -> &'static [(&'static str, &'static str)] {
    compile::stdlib_sources()
}
