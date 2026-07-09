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
pub mod cone;
pub mod lattice;
#[cfg(all(test, feature = "jit"))]
mod cone_tests;

/// Axiom S2 typed accessors, shared by the P2 and hybrid kernel
/// types (both expose `self.core.ref_entry(slot)`). Each returns
/// a borrow whose lifetime ties to `&self`, so the borrow checker
/// statically prevents holding a slice across the next
/// `eval(&mut self)` — stale Ref reads are compile errors.
macro_rules! ref_readers {
    () => {
        /// Borrow a `vec_f32` output's current contents.
        pub fn read_vec_f32(&self, slot: usize) -> &[f32] {
            match self.core.ref_entry(slot) {
                crate::ast::ScratchBuf::F32(v) => v,
                other => panic!("slot {slot} is not f32-lane scratch: {other:?}"),
            }
        }
        /// Borrow a `vec_f64` output's current contents.
        pub fn read_vec_f64(&self, slot: usize) -> &[f64] {
            match self.core.ref_entry(slot) {
                crate::ast::ScratchBuf::F64(v) => v,
                other => panic!("slot {slot} is not f64-lane scratch: {other:?}"),
            }
        }
        /// Borrow a `vec_f16` output's current contents.
        pub fn read_vec_f16(&self, slot: usize) -> &[half::f16] {
            match self.core.ref_entry(slot) {
                crate::ast::ScratchBuf::F16(v) => v,
                other => panic!("slot {slot} is not f16-lane scratch: {other:?}"),
            }
        }
        /// Borrow a `vec_i8` output's current contents.
        pub fn read_vec_i8(&self, slot: usize) -> &[i8] {
            match self.core.ref_entry(slot) {
                crate::ast::ScratchBuf::I8(v) => v,
                other => panic!("slot {slot} is not i8-lane scratch: {other:?}"),
            }
        }
        /// Borrow a `vec_i16` output's current contents.
        pub fn read_vec_i16(&self, slot: usize) -> &[i16] {
            match self.core.ref_entry(slot) {
                crate::ast::ScratchBuf::I16(v) => v,
                other => panic!("slot {slot} is not i16-lane scratch: {other:?}"),
            }
        }
        /// Borrow a `vec_i32` output's current contents.
        pub fn read_vec_i32(&self, slot: usize) -> &[i32] {
            match self.core.ref_entry(slot) {
                crate::ast::ScratchBuf::I32(v) => v,
                other => panic!("slot {slot} is not i32-lane scratch: {other:?}"),
            }
        }
        /// Borrow a `vec_i64` output's current contents.
        pub fn read_vec_i64(&self, slot: usize) -> &[i64] {
            match self.core.ref_entry(slot) {
                crate::ast::ScratchBuf::I64(v) => v,
                other => panic!("slot {slot} is not i64-lane scratch: {other:?}"),
            }
        }
    };
}
pub(crate) use ref_readers;
