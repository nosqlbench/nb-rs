// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Dependency-inverted resource-accessor bridge (SRD-104, Phase 0).
//!
//! A polydat kernel node sometimes needs a **live, host-owned resource**
//! (the first consumer is a CQL `Session`) addressed by its configuration
//! **fingerprint**. polydat is the dependency floor — it must not depend on
//! the host runtime — so the bridge is expressed here as a **type-erased**
//! trait plus a process-global install point:
//!
//! - The host (nbrs-runtime's resource pool) implements [`ResourceAccessor`]
//!   and installs it into [`RESOURCE_ACCESSOR`] once at session start. The
//!   global is only a *bridge* to the pool; the pool remains the single,
//!   definitive owner of the resource.
//! - A polydat node reaches a resource by calling [`resource_lookup`], which
//!   consults the installed accessor. The payload is erased to
//!   `Arc<dyn Any + Send + Sync>` so polydat needs no host types; the
//!   consuming node downcasts it to its own concrete handle.
//!
//! `eval` is synchronous and context-free, so a node cannot be handed a
//! service reference or await a connect. This process-global registry is the
//! established nb-rs pattern for a node reaching a live resource (the same
//! shape dataset handles use). The lookup is a pure synchronous read of what
//! the host has already attached; vivification timing is the host's concern.

use std::any::Any;
use std::sync::{Arc, OnceLock};

/// Type-erased accessor over the host's live resource store.
///
/// Implemented by the host runtime (nbrs-runtime's resource pool) and
/// installed into [`RESOURCE_ACCESSOR`]. The trait is deliberately minimal
/// and free of host types so polydat stays the dependency floor.
pub trait ResourceAccessor: Send + Sync {
    /// Synchronous lookup of an already-attached resource's accessor
    /// payload by fingerprint `key`. Returns `None` when no live entry
    /// matches that key (never blocks, never connects).
    ///
    /// The `key` is the host's stable rendering of a resource fingerprint;
    /// a single canonical rendering is shared by whoever installs the
    /// payload and whoever looks it up, so the string round-trips exactly.
    fn lookup(&self, key: &str) -> Option<Arc<dyn Any + Send + Sync>>;
}

/// Process-global bridge to the host's resource accessor, installed once by
/// the runtime at session start. `None` (uninstalled) is the norm for any
/// polydat use that has no host — the trait is a bridge, not a requirement.
pub static RESOURCE_ACCESSOR: OnceLock<Arc<dyn ResourceAccessor>> = OnceLock::new();

/// Look up an already-attached resource's accessor payload by fingerprint
/// `key` through the installed [`RESOURCE_ACCESSOR`].
///
/// Returns `None` when no accessor is installed (no host) or when no live
/// entry matches `key`. Consuming nodes downcast the returned
/// `Arc<dyn Any + Send + Sync>` to their concrete handle type.
pub fn resource_lookup(key: &str) -> Option<Arc<dyn Any + Send + Sync>> {
    RESOURCE_ACCESSOR.get()?.lookup(key)
}
