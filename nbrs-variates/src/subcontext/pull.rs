// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`PullConsumer`] — wrapper / dispenser registration shape.
//!
//! ## Cross-crate boundary
//!
//! The runtime fixture / pull-plan machinery (SRD-32) lives in
//! `nbrs-activity`. This crate (`nbrs-variates`) cannot depend on
//! `nbrs-activity` — the dependency runs the other way. The
//! [`PullConsumer`] trait below carries only the *intent*: a list
//! of names the consumer wants to pull at cycle time. The
//! activity-side `ScopeFixture::register_consumer` adapter walks
//! these names and seals them into a `PullPlan` against the
//! spawned kernel's program. SRD-32's pull-plan format and the
//! per-consumer contract stay as 32 specifies; only the
//! accumulator surface unifies under
//! [`crate::subcontext::SubcontextBuilder::register_pull`].
//!
//! Phase 2 (the synthesiser migration) is responsible for moving
//! `ScopeFixture` to consume the consumers stored on a spawned
//! [`crate::subcontext::ScopeKernel`].

use std::sync::Arc;

/// Anything that wants to pull GK values at cycle time.
///
/// Implementors expose the names they intend to read; the spawn
/// path makes those names available on the spawned kernel for
/// the activity-side fixture to seal into a `PullPlan`.
///
/// This trait is `Send + Sync` because consumers are stored on
/// the artifact and may flow across threads with the spawned
/// kernel.
pub trait PullConsumer: Send + Sync {
    /// The GK names this consumer will pull at cycle time.
    /// Returned in registration order so handle indices match the
    /// caller's expected layout.
    fn names(&self) -> &[String];

    /// Diagnostic label — appears in error messages when a
    /// registered name fails to resolve against the kernel
    /// program at seal time. Default: `"<unnamed-consumer>"`.
    fn label(&self) -> &str {
        "<unnamed-consumer>"
    }
}

/// A consumer registration that has been recorded in a
/// [`crate::subcontext::SubcontextBuilder`].
///
/// Wraps an `Arc<dyn PullConsumer>` so consumers can be cheaply
/// shared between the artifact and the spawned kernel without
/// trait-object cloning. The activity layer's fixture adapter
/// owns the seal step in Phase 2; Phase 1 simply records the
/// consumer for later inspection.
#[derive(Clone)]
pub struct RegisteredPullConsumer {
    inner: Arc<dyn PullConsumer>,
}

impl RegisteredPullConsumer {
    pub fn new(consumer: Arc<dyn PullConsumer>) -> Self {
        Self { inner: consumer }
    }

    /// Inspect the names this consumer requested. Used by the
    /// activity-side adapter at seal time and by Phase 1 tests
    /// to verify the registration round-trip.
    pub fn names(&self) -> &[String] {
        self.inner.names()
    }

    /// The consumer's diagnostic label.
    pub fn label(&self) -> &str {
        self.inner.label()
    }

    /// Borrow the underlying trait object — the activity-side
    /// adapter uses this to dispatch to consumer-specific
    /// configuration once it has the kernel program in hand.
    pub fn as_dyn(&self) -> &dyn PullConsumer {
        self.inner.as_ref()
    }
}

impl std::fmt::Debug for RegisteredPullConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredPullConsumer")
            .field("label", &self.label())
            .field("names", &self.names())
            .finish()
    }
}

/// Test-only / scratch consumer: holds a fixed list of names.
/// Production consumers (validation / conditional / throttle /
/// …) implement [`PullConsumer`] themselves on the activity
/// side. This shape exists so Phase 1 tests can exercise the
/// builder + spawn path without importing the activity crate.
#[derive(Debug)]
pub struct NamedPullConsumer {
    label: String,
    names: Vec<String>,
}

impl NamedPullConsumer {
    pub fn new(label: impl Into<String>, names: impl IntoIterator<Item = String>) -> Self {
        Self {
            label: label.into(),
            names: names.into_iter().collect(),
        }
    }
}

impl PullConsumer for NamedPullConsumer {
    fn names(&self) -> &[String] {
        &self.names
    }

    fn label(&self) -> &str {
        &self.label
    }
}
