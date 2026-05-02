// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Engine-agnostic CQL surface shared by every engine module.
//!
//! Each engine — `cassandra_cpp` (DataStax C++ driver), `scylla`
//! (pure-Rust driver), and any future addition — implements its
//! own [`nbrs_activity::adapter::DriverAdapter`] but consumes this
//! module for everything that isn't transport-specific:
//!
//! - [`CqlConfig`] and its [`from_params`](CqlConfig::from_params)
//!   parser. Every engine reads the same workload params.
//! - [`CqlConsistency`] — engine-agnostic consistency enum + parser.
//!   Each engine maps to its driver's native consistency type.
//! - [`OpMode`] and [`STMT_FIELD_NAMES`] — the op-field naming
//!   convention that selects raw vs prepared vs batch dispatch.
//! - The [`cql_timeuuid`](nodes::CqlTimeuuid) GK node, registered
//!   once via inventory and available wherever this adapter is
//!   linked.
//! - [`default_status_metrics`] — the `rows_inserted` rate metric
//!   surfaced in the TUI status line.
//! - [`resolver`] — the `cql` adapter registration. The
//!   factory walks the registered `DriverImpl`s for `cql` and
//!   instantiates one based on the user's `cqldriver=…`
//!   selector (or default rank).
//!
//! Pure Rust, no driver dependencies. Always compiled
//! regardless of which driver features are enabled.

pub mod config;
pub mod nodes;
pub mod opmode;
pub mod resolver;
pub mod status;

pub use config::{CqlConfig, CqlConsistency};
pub use opmode::{OpMode, STMT_FIELD_NAMES};
pub use status::default_status_metrics;
