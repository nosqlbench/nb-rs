// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Predicate analyzer — spec §10.9.
//!
//! Structured analysis of GK boolean expressions used as
//! `filter` predicates. Output is [`PredicateInfo`] —
//! consumed by the optimizer's R5 (per-axis filter pushdown)
//! and the deferred R8 / R9 / R10 rules.
//!
//! The analyzer operates on the predicate **string** (the form
//! carried by [`crate::comprehension::ast::Comprehension::Filter`]).
//! A future revision may take a pre-parsed `GkExpr`; for the
//! initial implementation, recognizers walk the source text
//! and match the §10.9.5 pattern catalog.
//!
//! ## Module layout
//!
//! - [`info`] — `PredicateInfo` and supporting enums.
//! - [`coordset`] — `CoordSet` carrying per-coord discrete /
//!   continuous classification.
//! - [`recognizers`] — the §10.9.5 pattern catalog.
//! - [`analyzer`] — entry point + dispatch.

pub mod analyzer;
pub mod coordset;
pub mod info;
pub mod recognizers;

pub use analyzer::analyze;
pub use coordset::{CoordInfo, CoordKind, CoordSet};
pub use info::{
    Determinism, Factorization, Monotonicity, OpaqueReason, PerAxisMap, PredicateInfo,
    RangeConstraint,
};
