// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Workload checkpointing — see SRD-44.
//!
//! Submodules:
//!
//! - [`identity`] — `PathSegment`, `PhaseIdentity`, and the
//!   per-phase canonical-program hash. Identity is per-phase
//!   (no workload-level identity tuple) and `(yaml_path,
//!   coords)` is necessary; the program hash is sufficiency.
//! - [`storage`] — JSON file format + atomic-rename writer.
//! - [`writer`] — `CheckpointWriter` actor: subscribes to
//!   phase-lifecycle events, flushes on the metrics-tick
//!   cadence with sqlite-fsync-then-checkpoint-fsync ordering.
//! - [`resume`] — resume planner: loads checkpoint, classifies
//!   each freshly-pre-mapped phase per the resume protocol,
//!   produces a `ResumePlan` the executor consults before
//!   dispatch.

pub mod identity;
pub mod storage;
pub mod writer;
pub mod resume;

pub use identity::{PathSegment, PhaseIdentity};
pub use storage::{Checkpoint, PhaseEntry, PhaseStatus};
pub use writer::CheckpointWriter;
pub use resume::{ResumePlan, ResumeAction};
