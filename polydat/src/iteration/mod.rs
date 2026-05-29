// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Iteration & coordinate algebra.
//!
//! Three concerns that together describe "how a workload cycles
//! through coordinate space":
//!
//! - [`comprehension`]: the formal §3 algebra of iteration
//!   shape (Cartesian / Zip / Union / Filter / Order). The
//!   compile-time + runtime layer that turns a structural
//!   comprehension into a stream of typed coordinate tuples.
//!   See `polydat/docs/design/comprehension_forms.md`.
//! - [`source`]: workload-facing data-source abstraction —
//!   the typed sequences that drive cycle dispense (range
//!   factories, extension policies, cursor kinds).
//! - [`cursor_partition`]: SRD-71 cursor partition specs —
//!   how the operator slices a source's index range across
//!   concurrent fibers.

pub mod comprehension;
pub mod source;
pub mod cursor_partition;
