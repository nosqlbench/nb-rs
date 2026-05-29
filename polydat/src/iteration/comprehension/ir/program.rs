// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Immutable IR Program wrapper — spec §9.1.
//!
//! `Program` is the public surface of the compiled IR. It's
//! `#[non_exhaustive]` and accessible by value but cannot be
//! mutated after construction: the optimizer (§10) is the only
//! path from AST to IR, and the resulting program is frozen.

use serde::{Deserialize, Serialize};

use super::op::Op;

/// An immutable IR program — a finite, ordered sequence of
/// [`Op`]s ending in [`Op::Dispense`].
///
/// `Program` is the load-bearing immutable public API per
/// spec §9.1. The optimizer is the only constructor; consumers
/// read via [`ops`] and [`stack_depth`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Program {
    ops: Vec<Op>,
}

impl Program {
    /// Construct a Program. The compiler (`super::compile`) is
    /// the canonical caller; external code shouldn't call this
    /// directly except in tests.
    pub fn new(ops: Vec<Op>) -> Self {
        Self { ops }
    }

    /// The opcode sequence. Borrowed; the program owns the
    /// vec.
    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    /// Number of opcodes.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Maximum stack depth this program reaches during
    /// interpretation. Used by the bounds checker (§9.3's
    /// `O(depth(C))` operator-stack term).
    pub fn stack_depth(&self) -> usize {
        let mut depth: i64 = 0;
        let mut max_depth: i64 = 0;
        for op in &self.ops {
            let (pop, push) = op.stack_effect();
            depth -= pop as i64;
            depth += push as i64;
            if depth > max_depth {
                max_depth = depth;
            }
        }
        max_depth as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::Source;
    use crate::iteration::comprehension::strategy::ZipMode;

    fn push_clause(name: &str) -> Op {
        Op::PushClause {
            name: name.into(),
            source: Source::Literal { values: vec![] },
        }
    }

    #[test]
    fn stack_depth_simple() {
        // PUSH, PUSH, CARTESIAN(2), DISPENSE — max depth 2.
        let p = Program::new(vec![
            push_clause("a"),
            push_clause("b"),
            Op::Cartesian { n: 2 },
            Op::Dispense,
        ]);
        assert_eq!(p.stack_depth(), 2);
    }

    #[test]
    fn stack_depth_nested() {
        // PUSH, PUSH, PUSH, CARTESIAN(3), DISPENSE — max depth 3.
        let p = Program::new(vec![
            push_clause("a"),
            push_clause("b"),
            push_clause("c"),
            Op::Cartesian { n: 3 },
            Op::Dispense,
        ]);
        assert_eq!(p.stack_depth(), 3);
    }

    #[test]
    fn stack_depth_zip_then_cartesian() {
        // PUSH a, PUSH b, ZIP(2), PUSH c, CARTESIAN(2)
        let p = Program::new(vec![
            push_clause("a"),
            push_clause("b"),
            Op::Zip { n: 2, mode: ZipMode::Strict },
            push_clause("c"),
            Op::Cartesian { n: 2 },
            Op::Dispense,
        ]);
        assert_eq!(p.stack_depth(), 2);
    }

    #[test]
    fn round_trip_serde() {
        let p = Program::new(vec![
            push_clause("a"),
            Op::Dispense,
        ]);
        let json = serde_json::to_string(&p).unwrap();
        let back: Program = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}
