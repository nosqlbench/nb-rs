// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! # nb-rate
//!
//! Async-ready rate limiter for nb-rs. Token bucket with time-scaled
//! permits and burst recovery.
//!
//! See `docs/design/19_rate_limiter.md` for the design brief.

mod spec;
mod limiter;

pub use spec::{RateSpec, TimeUnit, Verb};
pub use limiter::RateLimiter;
