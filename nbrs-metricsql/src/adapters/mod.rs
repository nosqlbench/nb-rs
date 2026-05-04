// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`crate::eval::DataSource`] implementations against
//! concrete storage. Each adapter sits behind its own
//! Cargo feature so the core crate stays light for consumers
//! that bring their own data layer.

#[cfg(feature = "sqlite")]
pub mod sqlite;
