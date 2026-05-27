// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK context API — the public-facing surface of a GK context
//! (compiled program + per-fiber evaluation state, fused as
//! one thing for callers).
//!
//! ## Architecture
//!
//! A "context" is the user's view of a GK kernel. Internally it
//! splits into:
//!
//! - **Compiled context** — the program (immutable, shared
//!   across fibers via `Arc<GkProgram>`).
//! - **Context state** — the per-fiber evaluation state (input
//!   buffers, node output buffers, dirty flags). Each fiber has
//!   its own state instance; the program is shared.
//!
//! Externally there is one type ([`GkKernel`]) and three
//! traits that partition its surface:
//!
//! - [`Dataflow`] — write inputs / read wires. Four core
//!   methods: indexed `set_wire(idx, …)` / `get_wire(idx)` and
//!   named `set_wire(name, …)` / `get_wire(name)`. All other
//!   data accessors (memoized handles, plans, projections)
//!   should build on these.
//! - [`Metadata`] — read-only diagnostic and structural data
//!   about the context: input/output names and types, scope
//!   layering, GK graph matter, init-binding sets, scope
//!   coordinates. No data flow.
//! - [`Construction`] — the two sanctioned construction paths:
//!   root from source matter, and subscope built against this
//!   context with new gk matter.
//!
//! [`GkKernel`]: super::GkKernel

use crate::node::{PortType, Value};

/// A wire reference — either a pre-resolved index (fast path)
/// or a name (resolved against the context's input map).
///
/// Lets `set_wire` / `get_wire` accept either form so callers
/// can hold an index when they have one and a name when they
/// don't, without needing two distinct method names.
///
/// Sealed: only the in-crate impls (`usize`, `&str`, `String`)
/// are valid wire keys. External implementors are not
/// permitted because the resolution semantics are tied to the
/// context's input layout.
pub trait WireKey: sealed::Sealed {
    /// Resolve to a wire index in `metadata`. Returns `None`
    /// when the key doesn't match a wire on this context.
    fn resolve<M: Metadata + ?Sized>(self, metadata: &M) -> Option<usize>;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
    impl Sealed for String {}
    impl Sealed for &String {}
}

impl WireKey for usize {
    #[inline]
    fn resolve<M: Metadata + ?Sized>(self, _: &M) -> Option<usize> {
        Some(self)
    }
}

impl WireKey for &str {
    #[inline]
    fn resolve<M: Metadata + ?Sized>(self, metadata: &M) -> Option<usize> {
        metadata.find_input(self)
    }
}

impl WireKey for String {
    #[inline]
    fn resolve<M: Metadata + ?Sized>(self, metadata: &M) -> Option<usize> {
        metadata.find_input(&self)
    }
}

impl WireKey for &String {
    #[inline]
    fn resolve<M: Metadata + ?Sized>(self, metadata: &M) -> Option<usize> {
        metadata.find_input(self)
    }
}

/// Read-only metadata about a GK context: structural shape,
/// types, names, scope layering. Everything that's a property
/// of the compiled program (or fiber-state instance) but
/// isn't itself a runtime value.
pub trait Metadata {
    /// Resolve an input name to its wire index, if present.
    fn find_input(&self, name: &str) -> Option<usize>;

    /// All declared input wire names, in declaration order.
    fn input_names(&self) -> Vec<String>;

    /// All declared output wire names, in declaration order.
    fn output_names(&self) -> Vec<String>;

    /// Number of coordinate inputs (the leading prefix of the
    /// input slot vector — written via the cycle dispatcher).
    fn coord_count(&self) -> usize;

    /// Declared port type of an input wire, if known.
    fn input_port_type(&self, name: &str) -> Option<PortType>;
}

/// Data interface to a GK context: write inputs, read wires.
///
/// Four core methods. The indexed pair is the fast path;
/// the named pair resolves against the context's metadata
/// then delegates to the indexed pair.
///
/// Every other data accessor in the codebase (memoized
/// handles, pull plans, named projections) is built on these
/// four. Callers that don't need to peek at the compiled
/// program or per-fiber state should use this trait
/// exclusively.
pub trait Dataflow: Metadata {
    /// Write a value to wire `idx`. Out-of-range indices are
    /// a programming error (the caller already had a resolved
    /// index); panics in debug, no-ops in release.
    fn set_wire_idx(&mut self, idx: usize, value: Value);

    /// Read the current value of wire `idx`. Out-of-range
    /// behaviour mirrors `set_wire_idx`.
    fn get_wire_idx(&self, idx: usize) -> Value;

    /// Write a value to a wire identified by `key` (index or
    /// name). Returns whether the wire was found.
    #[inline]
    fn set_wire<W: WireKey>(&mut self, key: W, value: Value) -> bool {
        match key.resolve(self) {
            Some(idx) => {
                self.set_wire_idx(idx, value);
                true
            }
            None => false,
        }
    }

    /// Read the current value of a wire identified by `key`
    /// (index or name). Returns `None` when the wire is not
    /// found.
    #[inline]
    fn get_wire<W: WireKey>(&self, key: W) -> Option<Value> {
        key.resolve(self).map(|idx| self.get_wire_idx(idx))
    }
}

/// Construction interface — the two sanctioned construction
/// paths. Per the kernel-construction invariant:
///
/// 1. **Root** — built from gk matter, no parent.
/// 2. **Subscope** — built from gk matter against an existing
///    context.
///
/// Both paths take the same typed gk matter
/// ([`super::super::subcontext::GkMatter`]). The only
/// difference is whether a parent context supervises
/// construction. Nothing else is allowed.
pub trait Construction: Sized {
    /// Construction error type.
    type Error;

    /// Path 1: build a root context from gk matter. No parent.
    /// Subscope-only fields on the matter (result-binding
    /// rewrites, inherited-output cascade, finalize-time
    /// contract checks) are not applicable here and are
    /// ignored.
    fn root(matter: super::super::subcontext::GkMatter<'_>) -> Result<Self, Self::Error>;

    /// Path 2: build a subscope context against `self` from
    /// gk matter. The parent supervises: cell cascade, Rule 2
    /// rewrites, scope-coordinate threading, init-binding
    /// contract checks all flow from `self` into the child.
    fn subscope(&self, matter: super::super::subcontext::GkMatter<'_>)
        -> Result<Self, Self::Error>;
}
