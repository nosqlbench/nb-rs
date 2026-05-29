// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `CompiledComprehension` — the entry point for the three
//! consumption surfaces.
//!
//! Holds an `Arc<Program>` (immutable IR per spec §9.1). Each
//! factory method on this handle (`coordinate_stream`,
//! `scoped_kernel_stream`, `scope_once`) returns a fresh
//! streamer with its own dispense state but shares the
//! `Arc<Program>` — no recompilation across siblings (spec
//! §9.5.2's "IR-sharing test" property).

use std::sync::Arc;

use crate::iteration::comprehension::ast::Comprehension;
use crate::iteration::comprehension::ir::{compile as compile_to_ir, Program};

use super::coord_stream::CoordinateStream;
use super::instance::{KernelScope, ScopedKernelInstance};
use super::scope_once::scope_once_with;
use super::scoped_stream::ScopedKernelStream;

/// A comprehension that has been compiled to immutable IR
/// and is ready to dispense. The single source of truth for
/// the underlying program across the consumption surfaces.
///
/// Construction is via [`from_ast`] (compiles once) or
/// [`from_program`] (when the IR was compiled elsewhere).
/// Cloning a `CompiledComprehension` is cheap — just an
/// `Arc::clone` on the program.
#[derive(Debug, Clone)]
pub struct CompiledComprehension {
    program: Arc<Program>,
}

impl CompiledComprehension {
    /// Compile an AST. Performs the AST → IR pass once.
    pub fn from_ast(ast: &Comprehension) -> Self {
        Self {
            program: Arc::new(compile_to_ir(ast)),
        }
    }

    /// Wrap an already-compiled program (the optimizer is the
    /// canonical caller; tests use this for hand-built IR).
    pub fn from_program(program: Arc<Program>) -> Self {
        Self { program }
    }

    /// Access the underlying compiled program (immutable per
    /// spec §9.1).
    pub fn program(&self) -> &Program {
        &self.program
    }

    /// Clone the `Arc<Program>` for sharing with other
    /// handles. Used internally by the streamer factories.
    pub(crate) fn program_arc(&self) -> Arc<Program> {
        Arc::clone(&self.program)
    }

    /// **First-order surface** (spec §9.5).
    ///
    /// Return a fresh [`CoordinateStream`]. Each call
    /// allocates new per-streamer state; siblings share the
    /// underlying IR but dispense independently per spec
    /// §9.5.2's independence contract.
    pub fn coordinate_stream(&self) -> CoordinateStream {
        CoordinateStream::new(self.program_arc())
    }

    /// **Second-order surface** (spec §9.5).
    ///
    /// Return a fresh [`ScopedKernelStream`] wrapping the
    /// supplied parent kernel. Each `advance()` pulls one
    /// coord tuple from the underlying IR and applies
    /// `parent.scope(&coords)` to produce a
    /// [`ScopedKernelInstance`].
    ///
    /// Independence: pulling from this stream does NOT
    /// advance any [`CoordinateStream`] obtained from the
    /// same `CompiledComprehension`.
    pub fn scoped_kernel_stream<K: KernelScope>(
        &self,
        parent: K,
    ) -> ScopedKernelStream<K> {
        ScopedKernelStream::new(self.program_arc(), parent)
    }

    /// **One-shot surface** (spec §9.5.3).
    ///
    /// Apply `parent.scope(coords)` directly, without
    /// constructing any streamer. Pure function — no
    /// cursor consulted, no dispense state advanced. Used
    /// for replay, debugging, and point queries where a
    /// specific coord tuple is already known.
    pub fn scope_once<K: KernelScope>(
        &self,
        parent: &K,
        coords: &crate::iteration::comprehension::strategies::Tuple,
    ) -> ScopedKernelInstance<K::Scoped> {
        scope_once_with(parent, coords)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::{LiteralValue, Source};

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn from_ast_compiles_once() {
        let ast = clause("k", &[1, 2, 3]);
        let compiled = CompiledComprehension::from_ast(&ast);
        assert!(!compiled.program().is_empty());
    }

    #[test]
    fn cloning_compiled_shares_arc() {
        let ast = clause("k", &[1, 2, 3]);
        let a = CompiledComprehension::from_ast(&ast);
        let b = a.clone();
        // Same Arc — strong_count goes up.
        let count = Arc::strong_count(&a.program);
        assert!(count >= 2, "expected shared Arc, count = {count}");
        drop(b);
    }

    #[test]
    fn two_coordinate_streams_share_program() {
        let ast = clause("k", &[1, 2, 3]);
        let compiled = CompiledComprehension::from_ast(&ast);
        let _s1 = compiled.coordinate_stream();
        let _s2 = compiled.coordinate_stream();
        // Both streams hold an Arc; count is at least 3 (compiled +
        // two streamers, possibly more if internal clones happen).
        let count = Arc::strong_count(&compiled.program);
        assert!(count >= 3, "expected shared program across streamers, count = {count}");
    }
}
