// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # Scylla engine
//!
//! Pure-Rust CQL engine backed by the [scylla 1.6](https://crates.io/crates/scylla)
//! driver. Speaks the Apache Cassandra wire protocol — works
//! against both Apache Cassandra and ScyllaDB.
//!
//! `scylla` is the internal driver name — `adapter=cql` is the
//! only user-facing adapter; `cqldriver=scylla` selects this
//! driver from inside that adapter. The `DriverImpl` below
//! carries the factory and the driver-specific known params;
//! there is no separate `AdapterRegistration` because `scylla`
//! is never an adapter name on its own.

mod batch;
mod binders;
mod prepared;
mod raw;
mod result;

use std::sync::Arc;

use nbrs_activity::adapter::{
    AdapterError, DriverImpl, DriverAdapter, ExecutionError, OpDispenser, StatusMetric,
};
use crate::common::{CqlConfig, CqlConsistency, OpMode, STMT_FIELD_NAMES};
use nbrs_workload::model::ParsedOp;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;

use result::ScyllaResultBody;

/// Bridge: [`crate::common::CqlConsistency`] →
/// `scylla::statement::Consistency`. Each engine keeps its own
/// driver-specific consistency type; the shared enum stays
/// driver-agnostic.
fn to_scylla_consistency(c: CqlConsistency) -> Consistency {
    match c {
        CqlConsistency::Any         => Consistency::Any,
        CqlConsistency::One         => Consistency::One,
        CqlConsistency::Two         => Consistency::Two,
        CqlConsistency::Three       => Consistency::Three,
        CqlConsistency::Quorum      => Consistency::Quorum,
        CqlConsistency::All         => Consistency::All,
        CqlConsistency::LocalQuorum => Consistency::LocalQuorum,
        CqlConsistency::EachQuorum  => Consistency::EachQuorum,
        CqlConsistency::LocalOne    => Consistency::LocalOne,
    }
}

/// CQL adapter using the scylla pure-Rust driver.
pub struct ScyllaCqlAdapter {
    session: Arc<Session>,
    consistency: Consistency,
}

impl ScyllaCqlAdapter {
    pub async fn connect(config: &CqlConfig) -> Result<Self, String> {
        let consistency = to_scylla_consistency(config.consistency);

        let mut builder = SessionBuilder::new();
        for host in config.hosts.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            // Hosts may be `host:port`; if not, append the configured port.
            if host.contains(':') {
                builder = builder.known_node(host);
            } else {
                builder = builder.known_node(format!("{host}:{}", config.port));
            }
        }
        if let (Some(u), Some(p)) = (&config.username, &config.password) {
            builder = builder.user(u, p);
        }
        builder = builder.connection_timeout(std::time::Duration::from_millis(
            config.request_timeout_ms,
        ));
        if !config.keyspace.is_empty() {
            builder = builder.use_keyspace(config.keyspace.clone(), false);
        }

        let session = builder.build().await
            .map_err(|e| format!("scylla connect: {e}"))?;
        Ok(Self {
            session: Arc::new(session),
            consistency,
        })
    }
}

impl DriverAdapter for ScyllaCqlAdapter {
    // The user-facing adapter is `cql`, regardless of which
    // engine backs it. `scylla` is an internal driver choice
    // selected via `cqldriver=`; it never appears in the
    // adapter-lookup table or in op-level `adapter: …` fields.
    fn name(&self) -> &str { "cql" }

    fn default_status_metrics(&self) -> Vec<StatusMetric> {
        crate::common::default_status_metrics()
    }

    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        let (stmt_text, stmt_field) = STMT_FIELD_NAMES.iter()
            .find_map(|key| -> Option<(String, &'static str)> {
                let v = template.op.get(*key)?;
                Some((v.as_str()?.to_string(), *key))
            })
            .ok_or_else(|| {
                "CQL op requires a 'raw:', 'simple:', 'prepared:', or 'stmt:' field".to_string()
            })?;

        // Replace bind points with `?` markers for prepared mode
        // and capture the bind-point names in `?` order so we can
        // look up typed values by name from `ResolvedFields`. The
        // runtime puts every op field (including the stmt text
        // itself) into `fields.values`; only the named bind points
        // belong on the wire, in the order they appeared in the
        // statement text.
        let bind_names = nbrs_workload::bindpoints::referenced_bindings(&stmt_text);
        let prepared_text = nbrs_workload::bindpoints::replace_bind_points_with_markers(&stmt_text);

        let has_batch = template.params.contains_key("batch");
        let mode = OpMode::from_stmt_field(stmt_field, has_batch);

        match mode {
            OpMode::Raw => Ok(Box::new(raw::ScyllaRawDispenser::new(
                self.session.clone(),
                self.consistency,
                stmt_field.to_string(),
            ))),
            OpMode::Prepared => Ok(Box::new(prepared::ScyllaPreparedDispenser::new(
                self.session.clone(),
                self.consistency,
                prepared_text,
                bind_names,
            ))),
            OpMode::Batch => {
                let batch_type = template.params.get("batchtype")
                    .and_then(|v| v.as_str())
                    .map(|s| match s.to_lowercase().as_str() {
                        "logged"  => scylla::statement::batch::BatchType::Logged,
                        "counter" => scylla::statement::batch::BatchType::Counter,
                        _         => scylla::statement::batch::BatchType::Unlogged,
                    })
                    .unwrap_or(scylla::statement::batch::BatchType::Unlogged);
                Ok(Box::new(batch::ScyllaBatchDispenser::new(
                    self.session.clone(),
                    self.consistency,
                    prepared_text,
                    bind_names,
                    batch_type,
                )))
            }
        }
    }
}

// =========================================================================
// Inventory registration
// =========================================================================

// Register `scylla` as a driver implementation of the `cql`
// adapter. Higher rank than cassandra-cpp (100) so binaries
// that link both default to cassandra-cpp; flip with
// `cqldriver=scylla`.
inventory::submit! {
    DriverImpl {
        adapter: "cql",
        driver: "scylla",
        default_rank: 200,
        create: |params| Box::pin(async move {
            let config = CqlConfig::from_params(&params)
                .map_err(|e| format!("scylla config error: {e}"))?;
            ScyllaCqlAdapter::connect(&config).await
                .map(|a| Arc::new(a) as Arc<dyn DriverAdapter>)
                .map_err(|e| format!("scylla connection failed: {e}"))
        }),
        known_params: || &[
            "hosts", "host", "port", "keyspace", "connect_keyspace", "consistency",
            "username", "password", "request_timeout_ms",
            // Accepted for parity with the cassandra-cpp engine,
            // so workloads that switch driver via `cqldriver=`
            // don't trip an unknown-param guard. The scylla
            // engine doesn't yet honor per-statement tracing —
            // the surface stays declared but inert until wired.
            "trace_rate", "trace_log",
        ],
    }
}

// =========================================================================
// Helpers shared across dispenser modules
// =========================================================================

pub(super) fn op_error(error_name: &str, msg: impl Into<String>, retryable: bool) -> ExecutionError {
    ExecutionError::Op(AdapterError {
        error_name: error_name.into(),
        message: msg.into(),
        retryable,
    })
}

pub(super) fn truncate_stmt(text: &str) -> String {
    if text.len() > 200 {
        format!("{}...", &text[..200])
    } else {
        text.to_string()
    }
}

/// Render a CQL execution error in a rustc-like format with the
/// offending statement and a caret at the reported position.
///
/// The driver returns errors like `"... line 1:31 no viable
/// alternative at character '_'"`. When a `line N:M` (1-based)
/// position is present, we extract it, find the matching line
/// in `stmt`, and underline the column with a caret. Otherwise
/// we fall back to a single-line `error\n  statement: …` form
/// that's still readable.
///
/// Example:
///
/// ```text
/// error: cql syntax: no viable alternative at character '_'
///   --> line 1, column 31
///    |
///  1 | DROP INDEX IF EXISTS baselines._meta_idx
///    |                               ^
/// ```
pub(super) fn format_cql_error(err: &str, stmt: &str) -> String {
    let err_str = err.to_string();
    let (line_no, col_no, message) = match parse_line_col(&err_str) {
        Some(p) => p,
        None => {
            return format!("cql error: {err_str}\n  statement: {}", truncate_stmt(stmt));
        }
    };

    let lines: Vec<&str> = stmt.lines().collect();
    if lines.is_empty() || line_no == 0 || line_no > lines.len() {
        return format!("cql error: {err_str}\n  statement: {}", truncate_stmt(stmt));
    }

    let target_line = lines[line_no - 1];
    let line_num_str = line_no.to_string();
    let gutter_w = line_num_str.len();
    // The content line is rendered as
    // ` <line_num> | <text>`. The blank-gutter lines (`-->`,
    // the divider above the content, and the caret line below
    // it) need their `|` at the same column. That column is
    // `1 + gutter_w + 1` (leading indent + width of the line
    // number + the space before `|`). `gutter_pad` is the
    // padding that puts `|` at that column on the no-line-
    // number rows.
    let gutter_pad = " ".repeat(1 + gutter_w + 1);

    // Caret column. The driver reports 1-based char positions;
    // anything else gets clamped into range so we still show
    // the line.
    let caret_col = col_no.saturating_sub(1).min(target_line.chars().count());
    let caret_pad = " ".repeat(caret_col);

    let mut out = String::new();
    out.push_str(&format!("cql syntax: {message}\n"));
    out.push_str(&format!("{gutter_pad}--> line {line_no}, column {col_no}\n"));
    out.push_str(&format!("{gutter_pad}|\n"));
    out.push_str(&format!(" {line_num_str} | {target_line}\n"));
    out.push_str(&format!("{gutter_pad}| {caret_pad}^"));
    out
}

/// Pull `line N:M` from an error string (1-based) and return
/// `(line, col, trailing_message)`. The trailing message is the
/// substring after the position, with a leading "no viable
/// alternative…"-style descriptor when the driver provides one.
fn parse_line_col(err: &str) -> Option<(usize, usize, String)> {
    // Look for the `line N:M` shape anywhere in the error
    // string. Cassandra's wire-protocol error variants embed it
    // verbatim regardless of which preamble the driver wraps it
    // in.
    let bytes = err.as_bytes();
    let needle = b"line ";
    let start = (0..bytes.len().saturating_sub(needle.len()))
        .find(|&i| &bytes[i..i + needle.len()] == needle)?;
    let after = &err[start + needle.len()..];

    let (line_str, rest) = after.split_once(':')?;
    let line: usize = line_str.trim().parse().ok()?;

    // Column: digits up to the next non-digit.
    let col_end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    let col: usize = rest[..col_end].parse().ok()?;

    let mut message = rest[col_end..].trim_start().to_string();
    if message.is_empty() {
        // Fall back to the whole error string if there's no
        // trailing descriptor — the position alone is the
        // signal.
        message = err.to_string();
    }
    Some((line, col, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_line_col_with_message() {
        let err = "Database returned an error: The submitted query has a syntax error, \
                   Error message: line 1:31 no viable alternative at character '_'";
        let (l, c, m) = parse_line_col(err).expect("should parse");
        assert_eq!(l, 1);
        assert_eq!(c, 31);
        assert!(m.starts_with("no viable alternative"));
    }

    #[test]
    fn parses_line_col_multiline() {
        let err = "syntax error: line 3:7 mismatched input";
        let (l, c, _) = parse_line_col(err).expect("should parse");
        assert_eq!(l, 3);
        assert_eq!(c, 7);
    }

    #[test]
    fn returns_none_when_no_position() {
        assert!(parse_line_col("connection refused").is_none());
    }

    #[test]
    fn renders_with_caret() {
        let stmt = "DROP INDEX IF EXISTS baselines._meta_idx";
        let err = "Database returned an error: line 1:31 no viable alternative at character '_'";
        let out = format_cql_error(err, stmt);
        // Header
        assert!(out.starts_with("cql syntax: no viable alternative"), "got:\n{out}");
        // Statement appears
        assert!(out.contains("DROP INDEX IF EXISTS baselines._meta_idx"), "got:\n{out}");
        // Caret line ends with `^`
        assert!(out.trim_end().ends_with('^'), "got:\n{out}");
    }

    #[test]
    fn falls_back_when_no_line_col() {
        let stmt = "SELECT 1";
        let err = "no host available";
        let out = format_cql_error(err, stmt);
        assert!(out.starts_with("cql error: no host available"));
        assert!(out.contains("SELECT 1"));
    }
}
