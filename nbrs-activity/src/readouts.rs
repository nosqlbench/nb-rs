// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Readout engine — see `docs/sysref/63_status_readouts.md`.
//!
//! Push 1 of the implementation plan
//! (`docs/design/readouts_implementation_plan.md`):
//!
//! - The [`Readout`] trait + [`Lod`] / [`ContentMode`] enums.
//! - The minimum [`ReadoutContext`] surface needed by the
//!   `phase_done` built-in. Methods that future built-ins
//!   will need are added in later pushes as needed.
//! - One built-in: [`builtins::phase_done`] at
//!   `Lod::Labeled` / `ContentMode::Value`. Other LODs and
//!   the explanation-overlay mode render zero bytes until
//!   later pushes fill them in.
//! - A small static registry to look up readouts by name.
//!
//! Lives as a module under `nbrs-activity` rather than its
//! own crate: the trait surface and built-ins read from
//! data sources owned here (`ActivityMetrics`, the scene
//! tree, the live-status pipeline), and `nbrs-tui` already
//! depends on `nbrs-activity` so a separate crate would
//! either re-export those types upward or cyclic-depend.
//! The clean split was a fiction; the module organisation
//! gives the same conceptual boundary without the build-graph
//! cost.

pub mod binder;
pub mod buf;
pub mod color;
pub mod context;
pub mod event;
pub mod format;
pub mod parse;
pub mod readout;
pub mod registry;
pub mod snapshot;

pub mod builtins;

pub use binder::{
    BakedBody, BinderKey, DefaultBinder, LayoutHint, LayoutMode,
    ReadoutBinder, ReadoutHandle, ReadoutSink, RenderStep, StringSink,
    TuiReadoutBinder,
    build_binder_from_workload, build_event_binder, build_event_binder_with_cli,
    validate_body_for_event,
};
pub use buf::ReadoutBuf;
pub use context::{LifecycleState, ReadoutContext, SubjectKind};
pub use event::Event;
pub use readout::{ContentMode, Lod, OptionTypeMismatch, OptionValue, Readout, ReadoutOptions};
pub use registry::Registry;
