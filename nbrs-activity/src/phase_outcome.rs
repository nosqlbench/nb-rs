// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-76 — Phase Outcome Disposition.
//!
//! Structured per-phase terminal-state record. One canonical
//! shape carries both the per-phase status (Completed /
//! Failed / Skipped / CursorSuspended) and the chronological
//! error list, so the realtime status surface AND `nbrs
//! replay` consume from one projection.
//!
//! This module is Push 1 of the SRD-76 migration plan: it
//! defines the data model and provides the no-side-effects
//! API. Push 2 wires the executor's per-phase population;
//! Push 3 adds sqlite persistence; Push 4 adds the new
//! Readouts; Push 5 wires `nbrs replay`.
//!
//! Cross-refs:
//! - SRD-03 §"Error scoping" — error-class strings here
//!   match the strings the `errors:` policy routes on.
//! - SRD-44 §"Phase identity" — `PhaseIdentity` matches the
//!   checkpoint writer's identity tuple.
//! - SRD-68 §"I-6 Workload-load pre-flight is non-mutating"
//!   — `op_template` is the operator's pristine YAML
//!   verbatim; `op_resolved` is the wire-rendered form.
//! - SRD-63 — The Readout layer that will render this in
//!   Push 4.
//! - SRD-75 — Phase-poll; `poll_timeout` is one of the
//!   error classes that lands here.

use serde::{Deserialize, Serialize};

/// Per-phase terminal status. Mutually exclusive — a phase
/// ends in exactly one of these states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    /// All cycles completed; no terminal error.
    Completed,
    /// Phase terminated via the error router with at least
    /// one [`PhaseErrorDetail`] populated.
    Failed,
    /// Skipped on resume per SRD-44 (idempotent phase whose
    /// identity matched a prior checkpoint entry).
    Skipped,
    /// Cursor-resumable phase partially executed and saved
    /// state. The [`PhaseOutcome::resume_cursor`] field
    /// carries the restart point.
    CursorSuspended,
}

impl PhaseStatus {
    /// Project a per-phase status onto the binary
    /// pass/fail axis used by the **session-level**
    /// [`SessionDisposition`]. `Completed`, `Skipped`, and
    /// `CursorSuspended` are all non-failures from the
    /// operator's perspective; `Failed` is the only one
    /// that contributes a session-level red mark.
    pub fn is_failure(&self) -> bool {
        matches!(self, PhaseStatus::Failed)
    }

    /// Glyph for compact rendering (✓ / ✗ / ~ / …).
    /// Used by the new SRD-76 `phase_outcome` readout and
    /// also as a one-character status indicator anywhere
    /// the operator wants a single byte of feedback.
    pub fn glyph(&self) -> char {
        match self {
            PhaseStatus::Completed => '✓',
            PhaseStatus::Failed => '✗',
            PhaseStatus::Skipped => '~',
            PhaseStatus::CursorSuspended => '…',
        }
    }

    /// All-lowercase short label suitable for log lines and
    /// debug output. Stable across the codebase so an
    /// `errors:` policy or a CI grep can match on it.
    pub fn label(&self) -> &'static str {
        match self {
            PhaseStatus::Completed => "completed",
            PhaseStatus::Failed => "failed",
            PhaseStatus::Skipped => "skipped",
            PhaseStatus::CursorSuspended => "cursor_suspended",
        }
    }
}

/// Session-wide pass/fail disposition. Projected from the
/// per-phase statuses by walking every populated
/// [`PhaseOutcome`] on the scene tree.
///
/// `SessionDisposition` is the single source of truth for
/// "what happened?" — drives the process exit code, the
/// terminating status line, the `nbrs replay` header, and
/// the (future) `session_disposition` readout in
/// SRD-63's `on_session_end` slot.
///
/// SRD-76 §"SessionDisposition" — past callers each did
/// their own ad-hoc walk over phase state with subtly
/// different rules (was Skipped a pass? was Ctrl-C a
/// failure?). This enum centralises the answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDisposition {
    /// Every phase that ran terminated cleanly
    /// (Completed, Skipped, or CursorSuspended).
    /// Interrupted-via-signal sessions where no phase
    /// actually failed land here — interrupted ≠ failed
    /// from the operator's perspective.
    Success,
    /// At least one phase has [`PhaseStatus::Failed`]. The
    /// realtime status surface and `nbrs replay` render
    /// this in red; CI / scripted callers observe via the
    /// process exit code (non-zero) and the JSON output.
    Failure,
}

impl SessionDisposition {
    /// Process exit code for this disposition. 0 for
    /// success; non-zero (today `1`) for failure.
    pub fn exit_code(&self) -> i32 {
        match self {
            SessionDisposition::Success => 0,
            SessionDisposition::Failure => 1,
        }
    }

    /// Uppercase short label suitable for the terminating
    /// status line: `session: idx_sweep (SUCCESS in …)`
    /// vs. `session: idx_sweep (FAILURE: N phases failed)`.
    pub fn label(&self) -> &'static str {
        match self {
            SessionDisposition::Success => "SUCCESS",
            SessionDisposition::Failure => "FAILURE",
        }
    }
}

/// Phase identity tuple per SRD-44 §"Phase identity" —
/// `(name, labels)`. The combination is unique within a
/// session: even sweep cells that share a phase name have
/// distinct labels (the striated coord path).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PhaseIdentity {
    pub name: String,
    /// Striated label path — the `(profile=…), (sm=…), …`
    /// form the executor produces via
    /// `format_scope_coordinate_path`. Empty for
    /// non-iter phases.
    pub labels: String,
}

impl PhaseIdentity {
    pub fn new(name: impl Into<String>, labels: impl Into<String>) -> Self {
        Self { name: name.into(), labels: labels.into() }
    }
}

/// One error recorded against a phase. Captures the
/// originating cycle (when an op error), the dispenser's
/// pristine + resolved op text (per SRD-68), and the
/// class string the `errors:` policy matches on.
///
/// `op_name` / `cycle` / `op_template` / `op_resolved`
/// are `None` when the error originates outside any
/// dispenser — workload-load validation failures,
/// phase-poll deadlines (SRD-75), missing-adapter init
/// errors, etc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseErrorDetail {
    /// Error class string (matches the `errors:` policy
    /// vocabulary): `Timeout`, `cql_error`, `poll_timeout`,
    /// `validate_failure`, `BindError`, …
    pub class: String,
    /// Human-readable message — the detail an operator
    /// needs to act. Multi-line allowed.
    pub message: String,
    /// Op identity (template name) that triggered this
    /// error. `None` for phase-level errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_name: Option<String>,
    /// Cycle number for op errors. `None` for
    /// phase-level errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle: Option<u64>,
    /// Pristine op-template text per SRD-68 §"I-6".
    /// Operator's YAML verbatim, with `{name}` placeholders
    /// intact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_template: Option<String>,
    /// Wire-rendered op text for the failing cycle. What
    /// the adapter actually sent. `None` when no render
    /// attempt happened (pre-dispense errors).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_resolved: Option<String>,
    /// Nanos-since-epoch when the error was recorded.
    /// Used for chronological replay rendering and
    /// cross-correlation with metric snapshots.
    pub at_nanos: u64,
    /// Whether the underlying error was classified as
    /// retryable. Useful for diagnostics: a workload with
    /// 100 retryable errors reads differently from one
    /// with 1 fatal error.
    #[serde(default)]
    pub retryable: bool,
}

/// SRD-44 cursor-resume state. Stub for Push 1 (the
/// resume machinery is independent of this SRD); future
/// pushes populate it with the actual restart-cursor
/// payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ResumeCursor {
    /// Opaque source-factory state to resume from. The
    /// concrete shape is owned by SRD-44's checkpoint
    /// layer; SRD-76 just carries it through. Empty when
    /// no resume state is recorded.
    #[serde(default)]
    pub opaque: Vec<u8>,
}

/// SRD-76 — complete description of how a single phase
/// ended. Built once at phase end by the executor,
/// installed on the scene tree, optionally persisted to
/// sqlite (Push 3), and rendered via the SRD-76 readouts
/// (Push 4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseOutcome {
    pub phase_id: PhaseIdentity,
    pub status: PhaseStatus,
    /// Wall-clock duration from `phase_starting` to this
    /// outcome being recorded. `0.0` for skipped phases.
    pub duration_secs: f64,
    /// Errors collected during the phase. Empty for
    /// `Completed` / `Skipped`. Non-empty for `Failed`.
    /// Chronologically ordered by `at_nanos`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<PhaseErrorDetail>,
    /// Resume state for the next session. `None` when the
    /// phase doesn't support cursor-resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_cursor: Option<ResumeCursor>,
    /// SRD-77 `--scope=changed` — the GK chain-hash for this
    /// phase (`GkProgram::instance_hash` from SRD-44). Hex-
    /// encoded so the storage layer can round-trip as TEXT.
    /// `None` for outcomes recorded before the column was
    /// added (legacy rows) or for skipped phases that never
    /// computed their hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_hash: Option<String>,
}

impl PhaseOutcome {
    /// Build a Completed outcome. Convenience for the
    /// happy path; errors must be empty.
    pub fn completed(phase_id: PhaseIdentity, duration_secs: f64) -> Self {
        Self {
            phase_id,
            status: PhaseStatus::Completed,
            duration_secs,
            errors: Vec::new(),
            resume_cursor: None,
            phase_hash: None,
        }
    }

    /// Build a Failed outcome from a non-empty error list.
    /// Panics in debug builds if `errors` is empty —
    /// `Failed` without an error is structurally
    /// invalid per SRD-76 §"Invariants".
    pub fn failed(
        phase_id: PhaseIdentity,
        duration_secs: f64,
        errors: Vec<PhaseErrorDetail>,
    ) -> Self {
        debug_assert!(!errors.is_empty(),
            "PhaseOutcome::failed requires at least one error");
        Self {
            phase_id,
            status: PhaseStatus::Failed,
            duration_secs,
            errors,
            resume_cursor: None,
            phase_hash: None,
        }
    }

    /// Build a Skipped outcome. Used by SRD-44's
    /// resume-on-checkpoint skip path. Duration is `0.0`
    /// because no actual work was done.
    pub fn skipped(phase_id: PhaseIdentity) -> Self {
        Self {
            phase_id,
            status: PhaseStatus::Skipped,
            duration_secs: 0.0,
            errors: Vec::new(),
            resume_cursor: None,
            phase_hash: None,
        }
    }

    /// Stamp the GK chain-hash on this outcome. Builder-style
    /// so existing callers that don't yet have the hash
    /// available (legacy / partial paths) stay unchanged;
    /// SRD-77-aware callers chain `.completed(...).with_hash(h)`.
    pub fn with_phase_hash(mut self, hex_hash: String) -> Self {
        self.phase_hash = Some(hex_hash);
        self
    }

    /// Convenience: the first error's message, or `None`
    /// when the phase didn't fail. Used by the compact
    /// renderer to give an at-a-glance reason.
    pub fn first_error_message(&self) -> Option<&str> {
        self.errors.first().map(|e| e.message.as_str())
    }

    /// SRD-76 Push 3 — project this outcome into the
    /// storage-layer row shape used by
    /// [`nbrs_metrics::reporters::sqlite::SqliteReporter::write_phase_outcome`].
    /// `session` / `exec_id` come from the active session
    /// (SRD-77); `started_at_nanos` is supplied by the
    /// caller (the executor captures it at phase entry);
    /// `ended_at_nanos` is taken from `SystemTime::now()` so
    /// the on-disk row matches the wall-clock moment the
    /// outcome was sealed.
    pub fn to_sqlite_row(
        &self,
        session: &str,
        exec_id: u64,
        started_at_nanos: i64,
    ) -> nbrs_metrics::reporters::sqlite::PhaseOutcomeRow {
        let ended_at_nanos: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(started_at_nanos);
        nbrs_metrics::reporters::sqlite::PhaseOutcomeRow {
            session:          session.to_string(),
            exec_id,
            phase_name:       self.phase_id.name.clone(),
            phase_labels:     self.phase_id.labels.clone(),
            status:           self.status.label().to_string(),
            duration_secs:    self.duration_secs,
            started_at_nanos,
            ended_at_nanos,
            phase_hash:       self.phase_hash.clone(),
            errors:           self.errors.iter().map(|e| {
                nbrs_metrics::reporters::sqlite::PhaseErrorRow {
                    class:       e.class.clone(),
                    message:     e.message.clone(),
                    op_name:     e.op_name.clone(),
                    cycle:       e.cycle,
                    op_template: e.op_template.clone(),
                    op_resolved: e.op_resolved.clone(),
                    at_nanos:    e.at_nanos as i64,
                    retryable:   e.retryable,
                }
            }).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_status_is_failure_only_true_for_failed() {
        assert!(!PhaseStatus::Completed.is_failure());
        assert!(PhaseStatus::Failed.is_failure());
        assert!(!PhaseStatus::Skipped.is_failure());
        assert!(!PhaseStatus::CursorSuspended.is_failure());
    }

    #[test]
    fn session_disposition_exit_codes() {
        assert_eq!(SessionDisposition::Success.exit_code(), 0);
        assert_ne!(SessionDisposition::Failure.exit_code(), 0);
    }

    #[test]
    fn outcome_completed_has_no_errors() {
        let o = PhaseOutcome::completed(
            PhaseIdentity::new("p", "x=1"),
            1.5,
        );
        assert_eq!(o.status, PhaseStatus::Completed);
        assert!(o.errors.is_empty());
        assert!(o.first_error_message().is_none());
    }

    #[test]
    fn outcome_failed_carries_errors() {
        let errors = vec![PhaseErrorDetail {
            class: "Timeout".into(),
            message: "connection timed out".into(),
            op_name: Some("read_state".into()),
            cycle: Some(0),
            op_template: Some("SELECT ...".into()),
            op_resolved: Some("SELECT * FROM ks.t".into()),
            at_nanos: 1_000_000_000,
            retryable: true,
        }];
        let o = PhaseOutcome::failed(
            PhaseIdentity::new("p", "x=1"), 30.0, errors.clone(),
        );
        assert_eq!(o.status, PhaseStatus::Failed);
        assert_eq!(o.errors, errors);
        assert_eq!(o.first_error_message(), Some("connection timed out"));
    }

    #[test]
    #[should_panic(expected = "at least one error")]
    fn outcome_failed_requires_non_empty_errors() {
        let _ = PhaseOutcome::failed(
            PhaseIdentity::new("p", ""), 1.0, Vec::new(),
        );
    }

    #[test]
    fn outcome_skipped_has_zero_duration_and_no_errors() {
        let o = PhaseOutcome::skipped(PhaseIdentity::new("p", ""));
        assert_eq!(o.status, PhaseStatus::Skipped);
        assert_eq!(o.duration_secs, 0.0);
        assert!(o.errors.is_empty());
    }

    /// Round-trip serde so the persistence layer (Push 3
    /// sqlite, JSON in `nbrs replay --json`) sees a
    /// stable shape.
    #[test]
    fn outcome_round_trips_through_json() {
        let original = PhaseOutcome {
            phase_id: PhaseIdentity::new("ensure_compacted", "(k=10)"),
            status: PhaseStatus::Failed,
            duration_secs: 14400.0,
            errors: vec![PhaseErrorDetail {
                class: "poll_timeout".into(),
                message: "deadline reached".into(),
                op_name: None,
                cycle: None,
                op_template: None,
                op_resolved: None,
                at_nanos: 0,
                retryable: false,
            }],
            resume_cursor: None,
            phase_hash: None,
        };
        let json = serde_json::to_string(&original).expect("serialise");
        let parsed: PhaseOutcome = serde_json::from_str(&json)
            .expect("deserialise");
        assert_eq!(parsed, original);
    }
}
