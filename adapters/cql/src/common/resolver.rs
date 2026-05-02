// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Adapter registration for `cql` and driver dispatch.
//!
//! The `cql` adapter is the single user-facing CQL adapter.
//! Internally it has multiple driver implementations
//! (`scylla` — pure Rust; `cassandra-cpp` — DataStax C++ via
//! FFI), each registered as a [`DriverImpl`]
//! (nbrs_activity::adapter::DriverImpl) under
//! `adapter = "cql"`. Driver names are *internal* — they're
//! never user-facing adapter names.
//!
//! At session start the runner walks the registered
//! [`DriverImpl`]s for `cql`, picks one (user override via
//! `cqldriver=…`, or default by ascending
//! [`DriverImpl::default_rank`]), and instantiates it. The
//! returned [`DriverAdapter`](nbrs_activity::adapter::DriverAdapter)
//! reports its name as `"cql"` regardless of which driver
//! backs it.
//!
//! User-facing examples:
//!
//! ```text
//! nbrs run adapter=cql                            # default driver
//! nbrs run adapter=cql cqldriver=scylla          # force scylla
//! nbrs run adapter=cql cqldriver=cassandra-cpp   # force cassandra-cpp
//! ```

use nbrs_activity::adapter::{AdapterRegistration, DisplayPreference, instantiate_with_driver};

/// The workload-param name a user sets to pick a specific CQL
/// driver, overriding the rank-derived default. Single value
/// (not a list) — name one driver.
pub const CQL_DRIVER_PARAM: &str = "cqldriver";

// `cql` adapter registration. The factory delegates to
// `instantiate_with_driver`, which picks among the registered
// `DriverImpl`s for `adapter = "cql"` (user override via
// `cqldriver=…`, otherwise the lowest-rank driver compiled in).
//
// Driver-specific params (`hosts`, `port`, `keyspace`, ...) are
// declared on each `DriverImpl` and unioned into the adapter's
// known-params surface by `registered_adapter_params()`; this
// registration only declares the driver-selector itself.
inventory::submit! {
    AdapterRegistration {
        names: || &["cql"],
        known_params: || &[CQL_DRIVER_PARAM],
        display_preference: || DisplayPreference::Auto,
        create: |params| Box::pin(instantiate_with_driver("cql", CQL_DRIVER_PARAM, params)),
    }
}

/// Echoed in startup banners. Returns the rank-sorted list of
/// CQL driver implementations compiled into this binary. Empty
/// when no driver feature is enabled.
pub fn default_cql_drivers() -> Vec<&'static str> {
    nbrs_activity::adapter::default_drivers("cql")
}

/// Convenience helper for diagnostic surfaces (CLI banner, web
/// dashboard, error messages). Joins the driver names with `, `.
pub fn default_cql_drivers_display() -> String {
    let drivers = default_cql_drivers();
    if drivers.is_empty() {
        "(none — build with engine-scylla / engine-cassandra-cpp)".into()
    } else {
        drivers.join(", ")
    }
}
