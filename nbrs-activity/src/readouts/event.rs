// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`Event`] — the slot that triggered the current render.
//!
//! See SRD-63 §4.1. Two kinds of events exist:
//!
//! - **Lifecycle events** — fire exactly once per
//!   `(slot, subject)`. `_start` and `_end` are
//!   delaminated; nothing fires twice for the same
//!   subject under one slot.
//! - **Refresh events** — fire repeatedly while the
//!   subject is in flight. Currently only [`Event::Update`].
//!
//! Push 2 wires `Update` and `PhaseEnd` through the activity
//! pipeline; the remaining variants are reachable from Push 3
//! onward as the workload-side `readouts:` parser maps slot
//! names to events and Push 4 wires wildcard binding to
//! cover scope and session lifecycles.

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// `on_session_start` — the run is opening.
    SessionStart,
    /// `on_session_end` — the run is closing. Final render
    /// for any session-scoped readout.
    SessionEnd,

    /// `on_phase_start` — a phase is opening. Equivalent to
    /// today's deleted phase-starting log row when the
    /// `phase_starting` readout is bound.
    PhaseStart,
    /// `on_phase_end` — a phase is closing. The ✓ DONE line
    /// (`phase_done` readout) is bound here today.
    PhaseEnd,

    /// `on_each_start` — a `for_each` / `for_combinations`
    /// iteration is opening. The current scope-ancestor
    /// header (`· for_each profile=…`) becomes the
    /// `scope_header` readout bound here in Push 3.
    EachStart,
    /// `on_each_end` — an iteration is closing.
    EachEnd,

    /// `on_scope_start` — a non-iteration scope group is
    /// opening.
    ScopeStart,
    /// `on_scope_end` — a non-iteration scope group is
    /// closing.
    ScopeEnd,

    /// `on_update` — periodic refresh tick. Today's inline
    /// progress thread fires this at 0.5 s; the TUI fires
    /// it per-frame. Drives the live status content.
    Update,
}

impl Event {
    /// Lower-snake-case name matching the `readouts:` slot
    /// keyword (`on_update`, `on_phase_end`, …). Used by
    /// the `trace` diagnostic readout and by Push 3's
    /// workload-block parser.
    pub fn slot_name(self) -> &'static str {
        match self {
            Event::SessionStart => "on_session_start",
            Event::SessionEnd   => "on_session_end",
            Event::PhaseStart   => "on_phase_start",
            Event::PhaseEnd     => "on_phase_end",
            Event::EachStart    => "on_each_start",
            Event::EachEnd      => "on_each_end",
            Event::ScopeStart   => "on_scope_start",
            Event::ScopeEnd     => "on_scope_end",
            Event::Update       => "on_update",
        }
    }

    /// What kind of subject the context that fires this
    /// event represents. Used by the binder to validate at
    /// bind-time that bound readouts accept the slot's
    /// subject kind. `Update` rides on the surrounding
    /// phase, so it reports `Phase`.
    pub fn subject_kind(self) -> super::SubjectKind {
        use super::SubjectKind;
        match self {
            Event::SessionStart | Event::SessionEnd => SubjectKind::Session,
            Event::PhaseStart   | Event::PhaseEnd
            | Event::Update                          => SubjectKind::Phase,
            Event::EachStart    | Event::EachEnd     => SubjectKind::Iteration,
            Event::ScopeStart   | Event::ScopeEnd    => SubjectKind::Scope,
        }
    }
}
