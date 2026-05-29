// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `CoordinateStream` — first-order consumption surface (spec
//! §9.5.1).
//!
//! Dispenses coordinate tuples (`Tuple` from the strategies
//! layer). Each instance holds its own dispense state — the
//! interpreter's tuple stream — but shares the
//! `Arc<Program>` with sibling streamers obtained from the
//! same `CompiledComprehension`.

use std::sync::Arc;

use crate::iteration::comprehension::ir::{interpret, Program, TupleStream};
use crate::iteration::comprehension::strategies::Tuple;

/// First-order coordinate stream. Each `advance()` yields one
/// coordinate tuple or `None` when the stream is exhausted.
///
/// Construct via [`crate::iteration::comprehension::surfaces::CompiledComprehension::coordinate_stream`].
/// Implements [`Iterator`] so callers can use standard Rust
/// iterator combinators (`.take(n)`, `.collect()`, etc.).
pub struct CoordinateStream {
    /// Held to keep the underlying program alive while this
    /// stream's interpreter graph references it.
    #[allow(dead_code)]
    program: Arc<Program>,
    /// The per-streamer interpreter — independent dispense
    /// state per spec §9.5.2.
    stream: Box<dyn TupleStream>,
}

impl CoordinateStream {
    pub(crate) fn new(program: Arc<Program>) -> Self {
        let stream = interpret(&program);
        Self { program, stream }
    }

    /// Pull the next coordinate tuple. Returns `None` when
    /// exhausted.
    pub fn advance(&mut self) -> Option<Tuple> {
        self.stream.advance()
    }
}

impl Iterator for CoordinateStream {
    type Item = Tuple;
    fn next(&mut self) -> Option<Self::Item> {
        self.advance()
    }
}

impl std::fmt::Debug for CoordinateStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinateStream")
            .field("program_ops", &self.program.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::ast::Comprehension;
    use crate::iteration::comprehension::source::{LiteralValue, Source};
    use crate::iteration::comprehension::strategies::TupleValue;
    use crate::iteration::comprehension::surfaces::compile;

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn advance_returns_tuples_then_none() {
        let compiled = compile(&clause("k", &[1, 2, 3]));
        let mut stream = compiled.coordinate_stream();
        assert!(stream.advance().is_some());
        assert!(stream.advance().is_some());
        assert!(stream.advance().is_some());
        assert!(stream.advance().is_none());
        assert!(stream.advance().is_none()); // exhausted
    }

    #[test]
    fn iterator_collect() {
        let compiled = compile(&clause("k", &[10, 20, 30]));
        let stream = compiled.coordinate_stream();
        let tuples: Vec<Tuple> = stream.collect();
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].bindings[0].1, TupleValue::I64(10));
        assert_eq!(tuples[2].bindings[0].1, TupleValue::I64(30));
    }

    #[test]
    fn iterator_take_truncates() {
        let compiled = compile(&clause("k", &[1, 2, 3, 4, 5]));
        let stream = compiled.coordinate_stream();
        let tuples: Vec<Tuple> = stream.take(2).collect();
        assert_eq!(tuples.len(), 2);
    }
}
