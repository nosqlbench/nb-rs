// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehensions — the formal model of iteration shape in GK.
//!
//! ## What it is
//!
//! A *comprehension* is a structured description of the
//! iteration position a scope occupies — the variables it
//! binds, where their value lists come from, and how those
//! lists combine. The algebra of six constructors
//! (`clause`, `cartesian`, `zip`, `union`, `filter`, `order`),
//! closed under composition, is the canonical representation;
//! see `polydat/docs/design/comprehension_forms.md` for the
//! full spec.
//!
//! It's the static-shape counterpart to the run-time
//! [`crate::kernel::ScopeCoord`]: the comprehension says
//! "this scope binds `k` and `limit`, drawn from `{k_values}`
//! and `{k_{k}_limits}`"; the scope coordinate says "right
//! now `k=10` and `limit=20`."
//!
//! ## Module layout
//!
//! The algebra modules ([`ast`], [`source`], [`strategy`],
//! [`spec`], [`runtime`], [`surfaces`], [`ir`], [`optimize`],
//! [`predicate`], [`metadata`], [`validate`], [`cardinality`],
//! [`strategies`]) are the canonical comprehension layer.
//! Top-level re-exports surface the common types
//! ([`Comprehension`], [`Source`], [`ZipMode`], etc.) for
//! ergonomic consumer access.
//!
//! [`ast_legacy`] and [`parse`] retain the older flat-struct
//! comprehension types as parse-pipeline implementation
//! details: the YAML loader uses [`parse::parse_clause_list`]
//! etc. to lex the textual form, then
//! [`spec::ComprehensionSpec::into_algebra`] converts to the
//! canonical algebra AST via [`spec::legacy_to_algebra`].
//! [`eval`] is the runtime-evaluation helper module
//! ([`eval::evaluate_spec`], [`eval::pre_evaluate_clause`]) that
//! both the scope-walker and the runtime evaluator consume.
//!
//! ## Why GK owns it
//!
//! Comprehensions cut across three subsystems:
//!
//! - The **YAML parser** (`nbrs-workload`) needs to recognise
//!   the textual shapes (`for_each`, `for_combinations`,
//!   `for_each_union`).
//! - The **scope synthesiser** (`nbrs-activity::scope_synth`)
//!   needs to emit the GK source for each comprehension's child
//!   kernel — extern declarations for the coordinates, final
//!   injections for workload params the spec interpolates, etc.
//! - The **executor** (`nbrs-activity::executor`) needs to
//!   enumerate the iteration tuples, drive the per-iteration
//!   `materialize_wiring_from_outer`, and run the children.
//!
//! All three flow through this module's canonical algebra AST.

// --- Algebra modules — the canonical comprehension layer.
pub mod ast;
pub mod cardinality;
pub mod eval_source;
pub mod ir;
pub mod metadata;
pub mod optimize;
pub mod predicate;
pub mod runtime;
pub mod source;
pub mod spec;
pub mod strategies;
pub mod strategy;
pub mod surfaces;
pub mod validate;

// --- Parse-pipeline support modules. `ast_legacy` and `parse`
// produce the older flat-struct form that the YAML parser
// generates; `spec::ComprehensionSpec::into_algebra` converts
// that into the canonical algebra AST above via
// `spec::legacy_to_algebra`. `eval` is the runtime-evaluation
// helper used by both the algebra runtime evaluator and the
// scope-walker.
pub mod ast_legacy;
pub mod eval;
pub mod parse;

// --- Canonical algebra re-exports — `polydat::iteration::comprehension::Comprehension`
// resolves to the algebra type; same for Source, ZipMode, etc.
pub use ast::Comprehension;
pub use cardinality::{CardinalityClass, Hybrid, Interval, MeasureName, ProductMeasure};
pub use eval_source::{EvalClass, EvalContext, EvalError, EvaluatedSource, SourceEval};
pub use metadata::{IndexFn, Materialization, Metadata, NaturalOrder};
pub use source::Source;
pub use strategy::{StrategyName, ZipMode};
pub use validate::{Mode, ValidationError, ValidationReport, ValidationWarning, validate};

// --- Parse-pipeline support re-exports. These are evaluator
// utilities used by the algebra runtime evaluator and the
// scope-walker — not part of the comprehension AST surface.
//
// `enumerate_tuples` and `parse_list_with_types` are crate-
// private — only `runtime` and `eval` use them internally
// after the synthesis dissolve. Kept available via
// `eval::*` for crate-internal callers.
pub use eval::{evaluate_spec, pre_evaluate_clause, value_to_gk_type_name};
