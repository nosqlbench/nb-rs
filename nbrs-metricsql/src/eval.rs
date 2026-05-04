// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! MetricsQL evaluator. AST → query plan → result, against a
//! pluggable [`DataSource`].
//!
//! Stub. The first concrete pieces will be:
//!   1. Selector evaluation (delegates to `DataSource::fetch`)
//!   2. Aggregation (`sum/avg/min/max/count` by/without)
//!   3. Binary ops with vector matching
//!
//! Rollups (range vectors) and the long tail of transform
//! functions land in later phases.

use crate::ast::Expr;

/// One observation: time + value, with the labels that
/// identify the producing series. Aligns with VM's
/// `Timeseries` shape but keeps the type name domain-neutral.
#[derive(Debug, Clone)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// One time series — its identifying label set plus the
/// observed samples within the query range.
#[derive(Debug, Clone)]
pub struct Series {
    pub labels: Vec<(String, String)>,
    pub samples: Vec<Sample>,
}

/// Pluggable data backend. Implementations adapt their
/// underlying storage (sqlite, in-memory, remote) to the
/// engine's selector contract: given a label-matcher set and
/// a time range, return the matching series.
pub trait DataSource {
    /// Fetch all series whose `labels` satisfy every matcher,
    /// containing samples in `[start_ms, end_ms]`.
    fn fetch(
        &self,
        matchers: &[Matcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Vec<Series>;
}

/// One label-matcher in a selector. Mirrors
/// [`crate::ast::LabelFilter`] but flattened for evaluator
/// consumers.
#[derive(Debug, Clone)]
pub struct Matcher {
    pub label: String,
    pub op: MatcherOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherOp {
    Eq, Ne, EqRegex, NeRegex,
}

/// Evaluation context: the data source plus the time range
/// the query operates over. Step size matters for range
/// queries; instant queries use `start_ms == end_ms`.
pub struct EvalContext<'a> {
    pub data: &'a dyn DataSource,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
}

#[derive(Debug, Clone)]
pub enum EvalError {
    NotYetImplemented(&'static str),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::NotYetImplemented(what) => {
                write!(f, "evaluator: {what} is not yet implemented")
            }
        }
    }
}

impl std::error::Error for EvalError {}

/// Evaluate a parsed MetricsQL expression against the
/// context's data source.
pub fn evaluate(_ctx: &EvalContext<'_>, _expr: &Expr) -> Result<Vec<Series>, EvalError> {
    Err(EvalError::NotYetImplemented("evaluator"))
}
