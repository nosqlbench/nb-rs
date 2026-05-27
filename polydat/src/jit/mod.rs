// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Phase 3: Cranelift JIT compilation of GK kernels.
//!
//! Generates native machine code from the DAG. The entire kernel
//! becomes a single function: `fn(coords: *const u64, buffer: *mut u64)`.
//! No closures, no function pointers, no gather/scatter — direct
//! buffer reads and writes with inlined arithmetic.
//!
//! The buffer is `Vec<u64>`. For f64 values, they are stored as their
//! bit representation (`f64::to_bits()` / `f64::from_bits()`). The JIT
//! uses Cranelift `bitcast` (free, no instruction emitted) to convert
//! between i64 and f64 representations when crossing type boundaries.
//!
//! Feature-gated behind `jit`.
//!
//! For nodes that can't be JIT-compiled inline (hash, shuffle, interleave),
//! we emit a call to an extern function. Simple ops are fully inlined,
//! complex ops are extern calls with zero overhead beyond the call itself.

#[cfg(feature = "jit")]
mod kernels;
#[cfg(feature = "jit")]
mod codegen;

#[cfg(feature = "jit")]
pub use kernels::*;
#[cfg(feature = "jit")]
pub(crate) use codegen::*;
