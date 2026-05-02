// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nbrs-errorhandler
//!
//! Modular composable error handler. Errors are classified by
//! type name (regex) and routed through a chain of handlers that
//! can log, count, meter, retry, or stop execution.
//!
//! Designed for nb-rs's op-dispatch loop, where every adapter op
//! result might be `Ok` or one of dozens of named error variants
//! — and operators want different policies for different error
//! families (retry timeouts, count + ignore `WriteFailures`,
//! stop on `BadCredentials`).
//!
//! ## Pieces
//!
//! - [`ErrorDetail`] is the structured error a producer hands
//!   in. Carries a name (used for routing), a retryable flag,
//!   and an optional result code.
//! - [`ErrorHandler`] is the trait every leaf handler implements
//!   (`StopHandler`, `WarnHandler`, `RetryHandler`,
//!   `CounterHandler`, …). Each one decides how to react to the
//!   incoming detail and may flip `retryable` or `stop` flags on it.
//! - [`ErrorRouter`] holds a list of `(regex, handler chain)`
//!   entries and dispatches each incoming detail to the first
//!   matching chain.
//!
//! ## Config syntax
//!
//! Routes are declared as semicolon-separated `pattern:chain`
//! pairs. Inside a chain, `,` separates handler names:
//!
//! ```text
//! "TimeoutError:retry,warn,counter;.*:stop"
//! ```
//!
//! Reads as: a `TimeoutError` retries (and warns + counts on
//! each attempt); anything else stops the run.
//!
//! ```
//! use nbrs_errorhandler::ErrorRouter;
//!
//! let router = ErrorRouter::parse(
//!     "TimeoutError:retry,warn,counter;.*:stop",
//! ).expect("config parses");
//! # drop(router); // exercised by integration tests in nbrs/tests/
//! ```
//!
//! ## Building details
//!
//! [`ErrorDetail`] uses a builder-style API:
//!
//! ```
//! use nbrs_errorhandler::ErrorDetail;
//!
//! let d = ErrorDetail::retryable("TimeoutError")
//!     .with_result_code(503);
//! assert!(d.is_retryable());
//! assert_eq!(d.name, "TimeoutError");
//! ```
//!
//! ## Defaults
//!
//! For tests and one-line setups:
//!
//! - [`ErrorRouter::default_stop`] — stop on any error.
//! - [`ErrorRouter::default_warn_count`] — warn + count on any
//!   error, never stop. Convenient for diagnostic runs.

mod detail;
mod handler;
mod router;
pub mod handlers;

pub use detail::{ErrorDetail, Retry};
pub use handler::ErrorHandler;
pub use router::ErrorRouter;
