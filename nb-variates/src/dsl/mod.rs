// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK DSL: lexer, parser, and AST for `.gk` kernel definition files.

pub mod lexer;
pub mod ast;
pub mod parser;
pub mod compile;
pub mod error;
pub mod registry;

pub use compile::{compile_gk, compile_gk_checked, compile_gk_with_path};
