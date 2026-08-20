// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nbrs-adapter-openapi
//!
//! OpenAPI workload-source adapter for nb-rs. Reads an OpenAPI
//! 3.x spec, discovers endpoints, and synthesizes [`ParsedOp`]s
//! plus Polydat binding source so a runner can drive traffic against
//! the target API through the standard `http` adapter.
//!
//! Unlike a transport adapter (`stdout`, `cql`, `http`), this
//! crate is a *workload generator*. It doesn't implement
//! [`DriverAdapter`](nbrs_runtime::adapter::DriverAdapter); it
//! produces synthesized ops that the http adapter executes.
//!
//! ## Usage
//!
//! ```ignore
//! let yaml = std::fs::read_to_string("petstore.yaml")?;
//! let (api, ops) = nbrs_adapter_openapi::parse_spec(&yaml)?;
//! let (parsed_ops, bindings) =
//!     nbrs_adapter_openapi::generate_ops(&ops, "http://localhost:8080");
//! ```
//!
//! The nbrs binary integrates this via its `openapi` feature:
//!
//! ```text
//! nbrs --features openapi describe spec=petstore.yaml
//! nbrs --features openapi run spec=petstore.yaml base_url=http://localhost:8080
//! ```

pub mod spec;
pub mod workload;

pub use spec::{ApiOperation, BodyInfo, FieldInfo, ParamInfo, describe_operations, parse_spec};
pub use workload::generate_ops;
