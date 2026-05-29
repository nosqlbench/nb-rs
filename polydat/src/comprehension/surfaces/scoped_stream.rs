// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `ScopedKernelStream<K>` — second-order consumption surface
//! (spec §9.5).
//!
//! Wraps a [`CoordinateStream`] + a parent `K`. Each
//! `advance()` pulls one coord tuple from the underlying IR
//! and applies `parent.scope(&coords)` to produce a
//! [`ScopedKernelInstance`].
//!
//! Per spec §9.5.2's independence contract: this stream's
//! cursor is **independent** of any other `CoordinateStream`
//! or `ScopedKernelStream` instantiated from the same
//! `CompiledComprehension`. Pulling from this stream does NOT
//! advance any sibling.

use std::sync::Arc;

use crate::comprehension::ir::Program;

use super::coord_stream::CoordinateStream;
use super::instance::{KernelScope, ScopedKernelInstance};
use super::scope_once::scope_once_with;

/// Second-order stream. Each `advance()` yields one
/// `ScopedKernelInstance<K::Scoped>` or `None`.
///
/// Construct via [`crate::comprehension::surfaces::CompiledComprehension::scoped_kernel_stream`].
pub struct ScopedKernelStream<K: KernelScope> {
    /// Underlying first-order stream. Owns its dispense
    /// cursor; independent of any sibling streamer.
    coord_stream: CoordinateStream,
    /// Captured parent kernel. Each `advance` uses `scope`
    /// against this same parent.
    parent: K,
}

impl<K: KernelScope> ScopedKernelStream<K> {
    pub(crate) fn new(program: Arc<Program>, parent: K) -> Self {
        let coord_stream = CoordinateStream::new(program);
        Self { coord_stream, parent }
    }

    /// Pull the next scoped instance. Internally:
    /// 1. Pull one coord tuple from the underlying
    ///    `CoordinateStream`.
    /// 2. Apply `parent.scope(&coords)` (spec §9.5.3's
    ///    `scope_once` semantic).
    /// 3. Return the wrapped instance.
    pub fn advance(&mut self) -> Option<ScopedKernelInstance<K::Scoped>> {
        let coords = self.coord_stream.advance()?;
        let instance = scope_once_with(&self.parent, &coords);
        Some(instance)
    }
}

impl<K: KernelScope> Iterator for ScopedKernelStream<K> {
    type Item = ScopedKernelInstance<K::Scoped>;
    fn next(&mut self) -> Option<Self::Item> {
        self.advance()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::ast::Comprehension;
    use crate::comprehension::source::{LiteralValue, Source};
    use crate::comprehension::strategies::{Tuple, TupleValue};
    use crate::comprehension::surfaces::compile;

    #[derive(Debug, Clone)]
    struct MockKernel(String);

    impl KernelScope for MockKernel {
        type Scoped = (String, Vec<(String, TupleValue)>);
        fn scope(&self, coords: &Tuple) -> Self::Scoped {
            (self.0.clone(), coords.bindings.clone())
        }
    }

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn advance_produces_scoped_instances() {
        let compiled = compile(&clause("k", &[1, 2, 3]));
        let parent = MockKernel("p".into());
        let mut stream = compiled.scoped_kernel_stream(parent);
        let mut count = 0;
        while let Some(inst) = stream.advance() {
            assert_eq!(inst.scoped.0, "p");
            assert_eq!(inst.coords.bindings.len(), 1);
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn dispense_order_matches_coord_stream() {
        let compiled = compile(&clause("k", &[10, 20, 30]));
        let parent = MockKernel("p".into());
        let coord_values: Vec<TupleValue> = compiled
            .coordinate_stream()
            .map(|t| t.bindings[0].1.clone())
            .collect();
        let scoped_values: Vec<TupleValue> = compiled
            .scoped_kernel_stream(parent)
            .map(|inst| inst.coords.bindings[0].1.clone())
            .collect();
        assert_eq!(coord_values, scoped_values);
    }
}
