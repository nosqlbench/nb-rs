// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! MetricsQL parser + evaluator. Rust port of
//! [VictoriaMetrics/metricsql](https://github.com/VictoriaMetrics/metricsql)
//! plus the relevant chunks of `vmselect/promql` for query
//! evaluation.
//!
//! ## Goals
//!
//! - **Parser parity**: every test case in upstream's
//!   `parser_test.go` and `lexer_test.go` round-trips through
//!   the Rust parser → AST → prettifier with the same output
//!   the Go implementation produces.
//! - **Pluggable data source**: the evaluator never touches
//!   storage directly; consumers implement [`DataSource`] to
//!   feed time series into the engine. nbrs's `metrics.db`
//!   reader is one such implementation.
//! - **Subset evaluator**: the function table starts with the
//!   selectors / aggregations / binary-ops / common rollups
//!   that nbrs's plot/table specs need. Less-common functions
//!   land as queries demand them.
//!
//! ## Status
//!
//! Foundation phase. Lexer + parser + AST in flight. The
//! evaluator is a stub that returns empty results.
//!
//! ## Module layout
//!
//! - [`lexer`] — token stream over a query string
//! - [`ast`]   — query AST nodes
//! - [`parser`]— token stream → AST
//! - [`prettifier`] — AST → canonical query string (used by
//!   the round-trip parity tests)
//! - [`eval`]  — AST → query plan → result, against a
//!   [`DataSource`]
//!
//! ## See also
//!
//! - Upstream Go: <https://github.com/VictoriaMetrics/metricsql>
//! - Linked in this repo at `links/metricsql/`
//! - Test fixtures: `tests/fixtures/*.json`, harvested from
//!   the upstream `_test.go` files via
//!   `scripts/extract_fixtures.go`.

pub mod lexer;
pub mod ast;
pub mod parser;
pub mod prettifier;
pub mod eval;
pub mod streaming;
pub mod adapters;
#[cfg(feature = "runtime")]
pub mod runtime;

pub use ast::Expr;
pub use eval::{DataSource, DataSourceError, EvalContext, EvalError, evaluate, evaluate_range};
pub use streaming::{StreamingPlan, CompileError, compile_streaming};
pub use parser::{parse, parse_for_prettify, ParseError};
