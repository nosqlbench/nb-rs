// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Kernel compilation: assembled DAG → fast executable kernel.
//!
//! Everything in this module is on the path between
//! [`assembly::PolydatAssembler`] and an executable kernel. The
//! pipeline:
//!
//! ```text
//! PolydatAssembler  ──(fusion pass)──▶  fused DAG
//!                                       │
//!                            (select::choose_kernel)
//!                                       │
//!                  ┌────────────────────┼────────────────────┐
//!                  ▼                    ▼                    ▼
//!         closures::Kernel       hybrid::Kernel       jit::Kernel
//!         (Phase 2 u64 closures) (per-node optimal)   (Phase 3 native)
//! ```
//!
//! - [`assembly`]: the public construction surface
//!   ([`assembly::PolydatAssembler`] + [`assembly::WireRef`]).
//! - [`fusion`]: graph-level subgraph fusion pass; runs during
//!   assembly after wiring resolution.
//! - [`select`]: variant-selection heuristic; chooses the
//!   monomorphic kernel type at construction time.
//! - [`closures`]: Phase 2 monomorphic u64-only kernels.
//! - [`hybrid`]: per-node optimal kernel (JIT segments + closure
//!   segments sharing a flat u64 buffer).
//! - [`jit`]: Phase 3 Cranelift JIT compilation
//!   (feature-gated on `jit`).

pub mod assembly;
pub mod fusion;
pub mod select;
pub mod closures;
pub mod hybrid;
#[cfg(feature = "jit")]
pub mod jit;
