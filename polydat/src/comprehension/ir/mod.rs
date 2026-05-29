// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! IR + compiler + interpreter — spec §9.1, §9.2, §9.3.
//!
//! The IR is a finite linear sequence of opcodes (the 8-op
//! set in [`Op`]) that compiles from an optimized AST and
//! executes via a stack-machine interpreter ([`Interpreter`])
//! or a stream-fusion compiler (future). Both interpretation
//! models produce identical dispense sequences per spec §9.2.
//!
//! ## Module layout
//!
//! - [`op`] — the 8-opcode enum + supporting parameter types.
//! - [`program`] — `#[non_exhaustive] Program` wrapper:
//!   immutable, accessible by value (spec §9.1).
//! - [`compile`] — bottom-up AST → IR walker.
//! - [`interpreter`] — stack-machine interpreter; produces a
//!   tuple stream that pulls lazily.
//! - [`bounds`] — §9.3 closed-form peak-memory checker.

pub mod bounds;
pub mod compile;
pub mod interpreter;
pub mod op;
pub mod program;

pub use bounds::{check_bounds, Bound, ResourceBound};
pub use compile::compile;
pub use interpreter::{interpret, TupleStream};
pub use op::{Op, OrderStreamingKind};
pub use program::Program;
