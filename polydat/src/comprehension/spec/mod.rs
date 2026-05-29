// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehension specification surface — author-friendly input
//! form for YAML / JSON consumers.
//!
//! Polydat owns the conversion from a YAML/JSON-native
//! structural form into the algebra-layer [`Comprehension`]
//! AST. Consumers (nb-workload, REPL, tooling) deserialize
//! into [`ComprehensionSpec`] via serde and call
//! [`ComprehensionSpec::into_algebra`]; text-block consumers
//! call [`parse_text`] which routes through serde for them.
//!
//! ## Single `for` verb
//!
//! Per spec §8.1, the surface has one keyword. The RHS shape
//! disambiguates which constructor:
//!
//! ```yaml
//! # Single clause
//! for: "k in 1..10"
//!
//! # Multi-clause cartesian (one inline string)
//! for: "k in 1..10, limit in [10, 100, 1000]"
//!
//! # Multi-clause cartesian (list of strings)
//! for:
//!   - "k in 1..10"
//!   - "limit in [10, 100, 1000]"
//!
//! # Union of sub-spaces (list of clause lists)
//! for:
//!   - ["k in 10",  "limit in 1..50"]
//!   - ["k in 100", "limit in 1..500"]
//! ```
//!
//! With optional modifiers:
//!
//! ```yaml
//! for: "k in 1..10, limit in 1..100"
//! where: "{k} * {limit} <= 1000"
//! order: "halton/50"
//! ```
//!
//! ## Architecture
//!
//! The friendly surface delegates **structural parsing** to
//! the existing legacy parsers in
//! [`crate::comprehension::parse`] — `parse_clause_list`,
//! `parse_comprehension_text`, `parse_order_spec`. Those
//! parsers produce a legacy [`Comprehension`] AST with raw
//! string sources. The [`legacy_convert`] module then walks
//! the legacy AST and assembles the algebra-layer AST,
//! using [`source_parser::parse_source`] for typed-source
//! classification of each clause's RHS string.
//!
//! This is the **single bridge**: every conversion of a
//! polydat-grammar input to the algebra layer funnels through
//! [`legacy_convert::legacy_to_algebra`].

pub mod legacy_convert;
pub mod source_parser;
pub mod spec;
pub mod text;

pub use legacy_convert::{legacy_to_algebra, ConvertError};
pub use source_parser::{parse_source, SourceParseError};
pub use spec::{ComprehensionSpec, ForSpec, SpecConvertError};
pub use text::{parse_text, TextParseError};

// Leaf grammar utilities — re-exported here so external
// consumers (nbrs-workload, nbrs-activity) reach the polydat
// grammar through a single chokepoint module. The
// implementations live in `crate::comprehension::parse` but
// that module is not external API after Surface 1.
//
// What's re-exported (leaf utilities, no comprehension-build
// pipeline):
// - `parse_clause` — one `var in expr` clause text → `Clause`
// - `parse_clause_list` — comma-separated clauses → `Vec<Clause>`
// - `parse_order_spec` — order-spec text → `TraversalOrder`
// - `parse_comprehension_text` — full `for ... where ... order`
//   text → legacy `Comprehension` (used for inline-text shapes
//   where the where/order are not separate keys).
//
// What's deliberately NOT re-exported (comprehension-build
// pipeline — callers route through [`ComprehensionSpec`]):
// - `comprehension_from_subspaces` — internal to
//   `ComprehensionSpec::into_legacy` / `into_algebra`.
// - `split_at_order`, `split_at_where`, `split_respecting_parens` —
//   internal parser helpers.
pub use crate::comprehension::parse::{
    parse_clause, parse_clause_list, parse_comprehension_text, parse_order_spec,
};
