// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nbrs-workload
//!
//! Workload specification parsing and processing for nb-rs. Owns
//! the YAML schema definition (op templates, bindings, phases,
//! scenarios, tags, parameter inheritance) plus the inline
//! `op='...'` shorthand the CLI accepts in place of a workload
//! file.
//!
//! The crate's deliberately one-way: it parses YAML / inline
//! shorthand into a [`model::Workload`] tree and exposes helpers
//! to extract bind-points, tag-filter ops, and resolve template
//! parameters. It does not execute anything — that's
//! `nbrs-activity`'s job.
//!
//! ## Pieces
//!
//! - [`model`] — the parsed `Workload` AST. `Workload` →
//!   `WorkloadPhase`s + ops + `ScenarioNode`s; an op carries its
//!   field map plus parsed bindings.
//! - [`parse`] — YAML → `Workload`, with phase-name preservation
//!   and friendly error messages.
//! - [`inline`] — `nbrs run op='hello {{cycle}}'` synthesizer:
//!   builds a one-op `Workload` from a single CLI string.
//! - [`bindpoints`] — extracts `{name}`, `{{expr}}`, qualified
//!   refs (`{coord:cycle}`, `{capture:foo}`) from any string.
//! - [`tags`] — `tags=block:main,phase:read` filters during op
//!   selection.
//! - [`template`] — `{param}` template substitution with
//!   default-value support.
//!
//! ## Quick examples
//!
//! Bind-point extraction:
//!
//! ```
//! use nbrs_workload::bindpoints::{extract_bind_points, BindPoint, BindQualifier};
//!
//! let pts = extract_bind_points("INSERT INTO t VALUES ({id}, {{hash(cycle)}})");
//! assert_eq!(pts.len(), 2);
//! assert!(matches!(
//!     &pts[0],
//!     BindPoint::Reference { name, qualifier: BindQualifier::None } if name == "id",
//! ));
//! assert!(matches!(&pts[1], BindPoint::InlineDefinition(expr) if expr == "hash(cycle)"));
//! ```
//!
//! Template substitution at workload-build time:
//!
//! ```
//! use nbrs_workload::template::expand_templates;
//! use std::collections::HashMap;
//!
//! let mut params = HashMap::new();
//! params.insert("table".into(), "users".into());
//! let resolved = expand_templates(
//!     "SELECT * FROM TEMPLATE(table,defaultvalue)",
//!     &params,
//! );
//! assert_eq!(resolved, "SELECT * FROM users");
//! ```
//!
//! ## See also
//!
//! - SRD 20 (`docs/sysref/20_workload_model.md`) — workload model
//! - SRD 21 (`docs/sysref/21_parameters.md`) — parameter resolution
//!   precedence
//! - SRD 18 (`docs/sysref/18_control_flow.md`) — scenario-tree
//!   constructs (`for_each`, `do_while`, `do_until`,
//!   `for_combinations`)

pub mod model;
pub mod template;
pub mod parse;
pub mod inline;
pub mod bindpoints;
pub mod tags;
pub mod spectest;
