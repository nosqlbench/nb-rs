// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Standard GK node implementations.

pub mod hash;
pub mod arithmetic;
pub mod bitwise;
pub mod identity;
pub mod convert;
pub mod fixed;
pub mod string;
pub mod datetime;
pub mod diagnostic;
pub mod encoding;
pub mod lerp;
pub mod format;
pub mod bytebuf;
pub mod digest;
pub mod weighted;
pub mod regex;
pub mod context;
pub mod noise;
pub mod json;
pub mod random;
pub mod realer;
pub mod probability;
pub mod pcg;
pub mod math;
pub mod datafile;
pub mod param_helpers;
pub mod runtime_context;
pub mod assertions;
pub mod compare;
pub mod pick;
pub mod exactly_one;
pub mod log_levels;
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "vectordata")]
pub mod vectors;

/// Env-gated debug-level diagnostic for selection / matching nodes.
///
/// Returns true when `NBRS_DEBUG_NODES` is set to a non-empty,
/// non-`"0"` value. Cached on first read so the variable can be set
/// once at process start and the per-cycle check is a load.
///
/// Used by `regex_match`, `exactly_one_value`, and `pick` to emit
/// pre-eval / pre-panic context (input shape, match result, selector
/// states) when probe phases produce surprising values. The user's
/// expected workflow:
///
/// ```sh
/// NBRS_DEBUG_NODES=1 nbrs run my-workload …
/// ```
///
/// then read the stderr trace to see what each matching node saw.
pub fn debug_nodes_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("NBRS_DEBUG_NODES")
            .map(|v| !v.is_empty() && v != "0")
            .unwrap_or(false)
    })
}
