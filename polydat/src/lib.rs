// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # polydat (formerly nbrs-variates)
//!
//! Deterministic variate generation kernel (GK) for workload testing.
//!
//! Transforms named `u64` coordinate tuples into typed output variates
//! via a compiled DAG of composable function nodes. The same coordinate
//! always produces the same outputs — deterministic, reproducible, and
//! parallelizable with zero shared mutable state.
//!
//! ## Quick Start
//!
//! ### From DSL source
//!
//! The simplest way to build a kernel is from Polydat DSL source:
//!
//! ```rust
//! use polydat::dsl::compile_polydat;
//!
//! let mut kernel = compile_polydat(r#"
//!     input cycle: u64
//!     hashed := hash(cycle)
//!     user_id := mod(hashed, 1000000)
//! "#).unwrap();
//!
//! kernel.set_inputs(&[42]);
//! let user_id = kernel.pull("user_id").as_u64();
//! assert!(user_id < 1_000_000);
//! ```
//!
//! ### From the assembler API
//!
//! For programmatic construction:
//!
//! ```rust
//! use polydat::compile::assembly::{PolydatAssembler, WireRef};
//! use polydat::library::hash::Hash;
//! use polydat::library::arithmetic::Mod;
//!
//! let mut asm = PolydatAssembler::new(vec!["cycle".into()]);
//! asm.add_node("hashed", Box::new(Hash::new()), vec![WireRef::input("cycle")]);
//! asm.add_node("user_id", Box::new(Mod::new(1_000_000)), vec![WireRef::node("hashed")]);
//! asm.add_output("user_id", WireRef::node("user_id"));
//!
//! let mut kernel = asm.compile().unwrap();
//! kernel.set_inputs(&[42]);
//! assert!(kernel.pull("user_id").as_u64() < 1_000_000);
//! ```
//!
//! ## Architecture
//!
//! ```text
//! coordinates (u64 tuple)
//!     │
//!     ▼
//! ┌─────────────────────────┐
//! │  PolydatProgram (immutable)  │  Shared via Arc across threads
//! │  - nodes: Vec<PolydatNode>   │
//! │  - wiring: Vec<Vec<..>> │
//! │  - output_map           │
//! └──────────┬──────────────┘
//!            │
//!     ┌──────┴──────┐
//!     │  PolydatState    │  One per thread — no locks
//!     │  - buffers  │
//!     │  - coords   │
//!     └──────┬──────┘
//!            │
//!            ▼
//!     pull("user_id") → Value::U64(527897)
//! ```
//!
//! ## Compilation Levels
//!
//! The kernel supports four compilation levels:
//!
//! - **Phase 1** (default): Pull-through interpreter. ~70ns/node.
//! - **Phase 2**: Compiled `u64` closures. ~4.5ns/node.
//! - **Hybrid**: Per-node optimal (JIT where supported, closures elsewhere).
//! - **Phase 3**: Cranelift JIT native code. ~0.2ns/node.
//!   Requires the `jit` feature (enabled by default).
//!
//! ## Features
//!
//! - **`jit`** (default): Cranelift JIT compilation for Phase 3.
//!   Disable with `default-features = false` for a lighter build.
//! - **`vectordata`**: Vector dataset access nodes for ML/AI workloads.
//!
//! ## Modules
//!
//! - [`ast`]: Core types — [`ast::Value`], [`ast::PolydatNode`] trait,
//!   [`ast::Port`], [`ast::PortType`]
//! - [`kernel`]: Runtime — [`kernel::PolydatProgram`], [`kernel::PolydatKernel`],
//!   [`kernel::PolydatState`]
//! - [`compile`]: DAG construction + compilation strategies —
//!   [`compile::assembly::PolydatAssembler`], [`compile::fusion`],
//!   [`compile::closures`] (Phase 2), [`compile::hybrid`]
//!   (per-node optimal), [`compile::jit`] (Phase 3 Cranelift,
//!   feature-gated)
//! - [`dsl`]: Polydat language — [`dsl::compile_polydat`], lexer, parser, registry
//! - [`library`]: 250+ built-in function nodes (hash, arithmetic, string,
//!   math, distributions, datetime, noise, etc.) plus [`library::sampling`]
//!   (alias tables, LUT interpolation, ICD) and [`library::support`]
//!   (library-internal cache + audit infrastructure)
//! - [`viz`]: DAG visualization (DOT, Mermaid)

// SRD-80 PR B.3 — let the `#[polydat_node]` macro's emitted
// `polydat::...` paths resolve when the macro is invoked from
// INSIDE the polydat crate itself (library nodes migrating to
// the macro form). External callers don't need this — they
// reference `polydat` via the regular crate-name lookup.
extern crate self as polydat;

pub mod ast;
pub mod binder;
pub mod kernel;
pub mod iteration;
pub mod compile;
pub mod library;
pub mod dsl;
pub mod viz;

// SRD-80 — proc-macro trait surface. The `polydat-derive`
// crate emits paths like `polydat::derive_support::FromValue` /
// `IntoValue` that resolve here.
pub mod derive_support;

// SRD-80 PR B.5 — `Const<T>` wrapper re-exported at crate root
// for ergonomic use in `#[polydat_node]` function signatures.
pub use derive_support::Const;

// SRD-80 — re-export the `#[polydat_node]` attribute so
// library callers can write `#[polydat::polydat_node]` without
// a separate `use polydat_derive::polydat_node;` line.
pub use polydat_derive::polydat_node;

// SRD-80 — re-export `inventory` so the macro's emitted
// `::polydat::inventory::submit!` path resolves at every call
// site without users having to add `inventory` to their own
// dependencies.
pub use inventory;

/// Re-exported for `#[polydat_node]`-generated Phase-2 buffer
/// casts on `half::f16`-typed wires (the generated code spells
/// `polydat::half::f16`, which `extern crate self as polydat`
/// resolves inside this crate too).
pub use half;
