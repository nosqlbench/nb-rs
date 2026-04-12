// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! # nb-variates
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
//! The simplest way to build a kernel is from GK DSL source:
//!
//! ```rust
//! use nb_variates::dsl::compile_gk;
//!
//! let mut kernel = compile_gk(r#"
//!     coordinates := (cycle)
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
//! use nb_variates::assembly::{GkAssembler, WireRef};
//! use nb_variates::nodes::hash::Hash64;
//! use nb_variates::nodes::arithmetic::ModU64;
//!
//! let mut asm = GkAssembler::new(vec!["cycle".into()]);
//! asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
//! asm.add_node("user_id", Box::new(ModU64::new(1_000_000)), vec![WireRef::node("hashed")]);
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
//! │  GkProgram (immutable)  │  Shared via Arc across threads
//! │  - nodes: Vec<GkNode>   │
//! │  - wiring: Vec<Vec<..>> │
//! │  - output_map           │
//! └──────────┬──────────────┘
//!            │
//!     ┌──────┴──────┐
//!     │  GkState    │  One per thread — no locks
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
//! - [`node`]: Core types — [`node::Value`], [`node::GkNode`] trait,
//!   [`node::Port`], [`node::PortType`]
//! - [`kernel`]: Runtime — [`kernel::GkProgram`], [`kernel::GkKernel`],
//!   [`kernel::GkState`]
//! - [`assembly`]: DAG construction — [`assembly::GkAssembler`],
//!   [`assembly::WireRef`]
//! - [`dsl`]: GK language — [`dsl::compile_gk`], lexer, parser, registry
//! - [`nodes`]: 250+ built-in function nodes (hash, arithmetic, string,
//!   math, distributions, datetime, noise, etc.)
//! - [`sampling`]: Alias tables, LUT interpolation, ICD sampling
//! - [`compiled`]: Phase 2 compiled kernel
//! - [`hybrid`]: Per-node optimal compilation
//! - [`jit`]: Phase 3 Cranelift JIT (feature-gated)
//! - [`fusion`]: Graph-level node fusion optimization
//! - [`viz`]: DAG visualization (DOT, Mermaid)

pub mod node;
pub mod kernel;
pub mod compiled;
pub mod assembly;
pub mod fusion;
pub mod nodes;
pub mod sampling;
pub mod dsl;
#[cfg(feature = "jit")]
pub mod jit;
pub mod hybrid;
pub mod viz;
pub mod engine;
pub mod runtime;
