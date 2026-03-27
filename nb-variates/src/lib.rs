// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! # nb-variates
//!
//! Generation kernel (GK) for deterministic variate generation.
//!
//! Transforms named u64 coordinate tuples into typed output variates
//! via a compiled DAG of composable function nodes.

pub mod node;
pub mod kernel;
pub mod compiled;
pub mod assembly;
pub mod nodes;
pub mod sampling;
pub mod dsl;
