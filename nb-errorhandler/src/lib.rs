// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nb-errorhandler
//!
//! Modular composable error handler. Errors are classified by type
//! name (regex) and routed through a chain of handlers that can
//! log, count, meter, retry, or stop execution.
//!
//! Config syntax: `"TimeoutError:retry,warn,counter;.*:stop"`

mod detail;
mod handler;
mod router;
mod handlers;

pub use detail::{ErrorDetail, Retry};
pub use handler::ErrorHandler;
pub use router::ErrorRouter;
