// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehensions — the formal model of iteration shape in GK.
//!
//! ## What it is
//!
//! A *comprehension* is a structured description of the
//! iteration position a scope occupies — the variables it
//! binds, where their value lists come from, and how those
//! lists combine (Cartesian product vs. union of sub-spaces).
//! It's the static-shape counterpart to the run-time
//! [`crate::kernel::ScopeCoord`]: the comprehension says
//! "this scope binds `k` and `limit`, drawn from `{k_values}`
//! and `{k_{k}_limits}`"; the scope coordinate says "right
//! now `k=10` and `limit=20`."
//!
//! ## Why GK owns it
//!
//! Comprehensions cut across three subsystems:
//!
//! - The **YAML parser** (`nbrs-workload`) needs to recognise
//!   the textual shapes (`for_each`, `for_combinations`,
//!   `for_each_union`).
//! - The **scope synthesiser** (`nbrs-activity::scope`) needs
//!   to emit the GK source for each comprehension's child
//!   kernel — extern declarations for the coordinates, final
//!   injections for workload params the spec interpolates, etc.
//! - The **executor** (`nbrs-activity::executor`) needs to
//!   enumerate the iteration tuples, drive the per-iteration
//!   `bind_outer_scope`, and run the children.
//!
//! Each subsystem currently carries its own representation of
//! the same shape (`ScenarioNode::ForEach{,Combinations,Union}`,
//! `ScopeKind::*`, `TupleComprehension`'s clause list). The
//! goal of this module is to be the **one** representation
//! everyone consults — a single source of truth, owned by GK
//! since iteration ultimately resolves to GK kernel state.
//!
//! ## Migration scope
//!
//! Phase A: this module exists as a type definition only.
//! Phase B+ moves the parser, evaluator, and synthesiser in
//! on top of the types defined here. See
//! `docs/internals/50_comprehensions_first_class.md` for the
//! full plan.

pub mod ast;
pub mod parse;
pub mod eval;
pub mod synthesis;
pub mod order;
pub mod iteration;

pub use ast::{Clause, Comprehension, ComprehensionMode, ShellOrigin, TraversalOrder};
pub use iteration::{iterate_scope, IterationStep, ScopeIterations};
pub use parse::{
    comprehension_from_subspaces, parse_clause, parse_clause_list,
    parse_comprehension_text, parse_order_spec, split_at_order, split_at_where,
    split_respecting_parens,
};
pub use eval::{
    collect_string_interp_refs, enumerate_tuples, evaluate_spec,
    interpolate_via_kernel, interpolate_with_lookup, parse_list_with_types,
    pre_evaluate_clause, value_to_gk_type_name,
};
pub use synthesis::{
    collect_leaf_placeholders, format_workload_param_as_gk_literal,
    iterate, propagate_parent_inputs, scan_one, synthesize_for_each_scope,
    workload_param_type_name, ComprehensionIter,
};
pub use order::{apply_order, Tuple};
