// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `ReducibilityFinding` and related types — spec §10.10.2.
//!
//! A finding represents either "no rewrite applies" (empty) or
//! "this rule rewrites C into the witness AST C'." Findings
//! also carry a [`ComplexityDelta`] declaring the improvement
//! the rewrite achieves in compute and/or memory complexity.

use serde::{Deserialize, Serialize};

use crate::iteration::comprehension::ast::Comprehension;

/// Identifier for each R-rule in the optimizer catalog
/// (§10.2 + §10.10.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuleId {
    R0a,
    R0b,
    R1,
    R2,
    R3,
    R4,
    R5,
    R6,
    R7,
    // Deferred per spec §14.1:
    R8,
    R9,
    R10,
}

/// Output of the reducibility analyzer (spec §10.10.2).
#[derive(Debug, Clone, PartialEq)]
pub struct ReducibilityFinding {
    /// `Some` if a rewrite applies; `None` is the empty
    /// finding (no rule fires).
    pub reduction: Option<Reduction>,
    /// Which rule fired (mirrors `reduction.rule` if it's a
    /// Rewrite). Useful for logging / diagnostics.
    pub rule: Option<RuleId>,
    /// Asymptotic improvement of the witness over the input.
    pub improvement: ComplexityDelta,
}

/// The rewrite carried by a non-empty finding.
#[derive(Debug, Clone, PartialEq)]
pub enum Reduction {
    /// Replace the entire AST with `with` (whole-tree swap).
    Replace { with: Comprehension },
    /// Rewrite via a tagged R-rule. The `witness` is the new
    /// AST; `rule` is the catalog identifier.
    Rewrite { rule: RuleId, witness: Comprehension },
}

/// Strict-improvement vector along the (compute, memory)
/// dimensions (spec §10.10.2 + §10.10.3 catalog table).
///
/// A non-empty finding must have at least one dimension
/// `Less` and the other ≤ `Equal`. The optimizer rejects
/// findings that are `Equal` on both dimensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexityDelta {
    pub compute_order: Ordering,
    pub memory_order: Ordering,
    pub rationale: &'static str,
}

/// Three-way asymptotic ordering for one complexity dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    Less,
    Equal,
    Greater,
}

impl ComplexityDelta {
    /// Rule reduces compute, no change to memory.
    pub fn less_compute() -> Self {
        Self {
            compute_order: Ordering::Less,
            memory_order: Ordering::Equal,
            rationale: "strictly less compute",
        }
    }

    /// Rule reduces memory, no change to compute.
    pub fn less_memory() -> Self {
        Self {
            compute_order: Ordering::Equal,
            memory_order: Ordering::Less,
            rationale: "strictly less memory",
        }
    }

    /// Rule reduces both compute and memory.
    pub fn less_both() -> Self {
        Self {
            compute_order: Ordering::Less,
            memory_order: Ordering::Less,
            rationale: "strictly less compute and memory",
        }
    }

    /// No change in either dimension. Used for the empty
    /// finding; the optimizer never produces a `Reduction`
    /// with this delta.
    pub fn equal() -> Self {
        Self {
            compute_order: Ordering::Equal,
            memory_order: Ordering::Equal,
            rationale: "no asymptotic change",
        }
    }

    /// `true` if at least one dimension is strictly Less and
    /// the other is at most Equal. Per spec §10.10.2 this is
    /// the condition for a non-empty finding to be valid.
    pub fn is_strict_improvement(&self) -> bool {
        match (self.compute_order, self.memory_order) {
            (Ordering::Less, Ordering::Less) => true,
            (Ordering::Less, Ordering::Equal) => true,
            (Ordering::Equal, Ordering::Less) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_strict_improvement_classifier() {
        assert!(ComplexityDelta::less_compute().is_strict_improvement());
        assert!(ComplexityDelta::less_memory().is_strict_improvement());
        assert!(ComplexityDelta::less_both().is_strict_improvement());
        assert!(!ComplexityDelta::equal().is_strict_improvement());
    }

    #[test]
    fn rule_id_round_trip_serde() {
        let r = RuleId::R5;
        let json = serde_json::to_string(&r).unwrap();
        let back: RuleId = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
