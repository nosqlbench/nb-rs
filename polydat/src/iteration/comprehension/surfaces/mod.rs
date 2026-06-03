// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Consumption surfaces — spec §9.5.
//!
//! Three independent first-class consumption surfaces over a
//! shared compiled IR:
//!
//! - [`CoordinateStream`] (first-order) — dispenses coordinate
//!   tuples (`Vec<(String, TupleValue)>`).
//! - [`ScopedKernelStream<K>`] (second-order) — dispenses
//!   scoped kernel instances; functor over the first-order
//!   via `K`'s `KernelScope` impl.
//! - [`scope_once`] (one-shot) — non-streamed; takes a single
//!   coord tuple and produces a single scoped kernel instance.
//!
//! All three surfaces share the underlying [`Program`] via
//! `Arc<Program>` but maintain independent dispense state per
//! spec §9.5.2's independence contract:
//!
//! > Each call to `coordinate_stream` or
//! > `scoped_kernel_stream` returns a fresh streamer with its
//! > own dispense cursor. The streamers share the underlying
//! > compiled IR but allocate their own per-streamer state.
//!
//! The entry point is [`CompiledComprehension`], obtained via
//! [`Comprehension::compile`] (an extension method on the AST
//! type).

use std::sync::Arc;

use super::ast::Comprehension;
use super::ir::compile as compile_to_ir;

pub mod compiled;
pub mod coord_stream;
pub mod polydat_kernel;
pub mod instance;
pub mod scope_once;
pub mod scoped_stream;

pub use compiled::CompiledComprehension;
pub use coord_stream::CoordinateStream;
pub use polydat_kernel::{
    polydat_value_to_tuple_value, tuple_value_to_polydat_value, PolydatKernelScope,
};
pub use instance::{KernelScope, ScopedKernelInstance};
pub use scope_once::scope_once;
pub use scoped_stream::ScopedKernelStream;

/// Convenience: compile an AST into a [`CompiledComprehension`]
/// ready to dispense. Equivalent to
/// `CompiledComprehension::from_ast(ast)`.
pub fn compile(ast: &Comprehension) -> CompiledComprehension {
    let program = Arc::new(compile_to_ir(ast));
    CompiledComprehension::from_program(program)
}
