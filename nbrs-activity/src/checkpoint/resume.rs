// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Resume planner — classify each freshly-pre-mapped phase
//! against the saved checkpoint document and emit a
//! `ResumePlan` the executor consults before dispatch.
//!
//! Per SRD-44 §"Resume protocol", the planner walks the
//! pre-map phase list (already in DFS-of-the-scenario-tree
//! order) and for each phase asks:
//!
//! 1. Is there a structural match (same `yaml_path` and
//!    `coords`) in the saved doc? If not → `ReRun` (this is a
//!    new phase that wasn't in the last invocation).
//! 2. If matched and the saved status is `Completed`:
//!    - phase declared `checkpoint: idempotent` (or long form
//!      with `idempotent: true`) → `Skip` when hashes match,
//!      `IdentityMismatch` (and re-run) when they don't.
//!    - phase declared `checkpoint: none` (or absent) → `ReRun`
//!      regardless of saved status (operator opted out).
//! 3. If matched and the saved status is `Running`:
//!    - the phase has cursor state recorded → `CursorResume`
//!      with that opaque snapshot (Tier 2).
//!    - no cursor state → `ReRun` (Tier 1 inflight crash).
//! 4. If matched and the saved status is `Failed` →
//!    `ReRun` (the errors cascade decides whether to surface
//!    the failure again — see SRD-44 §"Error handling is
//!    invocation-agnostic"). The planner doesn't pre-decide.
//! 5. If matched and the saved status is `Pending` → `ReRun`
//!    (the previous invocation never started this phase).

use std::collections::HashMap;

use super::identity::PhaseIdentity;
use super::storage::{Checkpoint, PhaseStatus};

/// What the executor should do with one pre-mapped phase. The
/// planner emits one of these per phase keyed by identity-key
/// (see [`super::writer`] for the key shape).
#[derive(Clone, Debug)]
pub enum ResumeAction {
    /// Phase already completed successfully on a prior
    /// invocation, identity matches, and the operator declared
    /// it idempotent. Executor emits the post-run summary line
    /// for this phase as `[skipped]` and moves on; the writer
    /// keeps the saved entry intact.
    Skip,
    /// Phase was in-flight (`Running`) and recorded a cursor
    /// snapshot the source factory understands. Executor
    /// reconstructs the source via the factory, calls
    /// `restore_cursor(snapshot)` on it, and runs the remaining
    /// cycles per SRD-44 §"Tier 2".
    CursorResume { cursor_state: serde_json::Value },
    /// Default — the phase runs from scratch this invocation.
    /// Triggered by: no saved entry, declared `checkpoint:
    /// none`, saved status `Pending`/`Failed`, or `Running`
    /// without cursor state.
    ReRun,
    /// Saved entry exists, structural match holds, but the
    /// program hash differs (or some other identity mismatch).
    /// Executor re-runs the phase and the writer overwrites
    /// the stale entry. Carries a human-readable reason for
    /// the resume diagnostic banner.
    IdentityMismatch { reason: String },
}

/// The resume plan for one pre-mapped scenario tree. Maps each
/// phase's identity-key to its [`ResumeAction`]. A "fresh" plan
/// (no saved checkpoint) maps every phase to `ReRun`.
#[derive(Clone, Debug, Default)]
pub struct ResumePlan {
    actions: HashMap<String, ResumeAction>,
    /// `true` when at least one phase was loaded from a
    /// previously-flushed checkpoint document — i.e. this is a
    /// resume run, not a fresh start. Drives the resume banner
    /// in the runner header and the `--- RESUMED ---` separator
    /// in `session.log`.
    pub is_resume: bool,
}

impl ResumePlan {
    /// A plan with no saved state — every phase re-runs.
    pub fn fresh() -> Self {
        Self::default()
    }

    /// Build a plan from a saved checkpoint and a list of
    /// freshly-pre-mapped phase candidates. `candidates` is
    /// `(identity, declared_idempotent)`; the second entry
    /// reflects the workload's `checkpoint:` declaration for
    /// that phase (`true` for idempotent, `false` for
    /// none/absent).
    pub fn from_checkpoint(
        saved: &Checkpoint,
        candidates: &[(PhaseIdentity, bool)],
    ) -> Self {
        let saved_index: HashMap<String, &super::storage::PhaseEntry> =
            saved.phases.iter()
                .map(|e| (identity_key(&e.identity), e))
                .collect();

        let mut actions = HashMap::with_capacity(candidates.len());
        for (cand, declared_idempotent) in candidates {
            let key = identity_key(cand);
            let action = match saved_index.get(&key) {
                None => ResumeAction::ReRun,
                Some(saved_entry) => classify(
                    cand,
                    saved_entry,
                    *declared_idempotent,
                ),
            };
            actions.insert(key, action);
        }
        Self { actions, is_resume: true }
    }

    /// Look up the action for a given phase identity. Returns
    /// `ReRun` for unknown phases — the conservative default so
    /// pre-map drift can never cause a phase to silently skip.
    pub fn action_for(&self, identity: &PhaseIdentity) -> ResumeAction {
        let key = identity_key(identity);
        self.actions.get(&key).cloned().unwrap_or(ResumeAction::ReRun)
    }

    /// Number of phases the plan classifies as `Skip`.
    pub fn skip_count(&self) -> usize {
        self.actions.values()
            .filter(|a| matches!(a, ResumeAction::Skip))
            .count()
    }

    /// Number of phases the plan classifies as `CursorResume`.
    pub fn cursor_resume_count(&self) -> usize {
        self.actions.values()
            .filter(|a| matches!(a, ResumeAction::CursorResume { .. }))
            .count()
    }

    /// Number of phases the plan classifies as
    /// `IdentityMismatch`. Surfaced in the resume banner so
    /// operators can tell at a glance whether their YAML edit
    /// invalidated phases they thought were stable.
    pub fn mismatch_count(&self) -> usize {
        self.actions.values()
            .filter(|a| matches!(a, ResumeAction::IdentityMismatch { .. }))
            .count()
    }
}

fn classify(
    candidate: &PhaseIdentity,
    saved: &super::storage::PhaseEntry,
    declared_idempotent: bool,
) -> ResumeAction {
    // The saved entry might have been written before the
    // operator changed `checkpoint:` for this phase. Honour the
    // *current* declaration: if the workload now says `none`,
    // re-run regardless of saved status.
    if !declared_idempotent {
        return ResumeAction::ReRun;
    }

    match saved.status {
        PhaseStatus::Completed => {
            // Structural match has been established by the
            // identity_key lookup; check sufficiency (hash) only
            // when both sides carry one.
            if !candidate.matches_full(&saved.identity) {
                return ResumeAction::IdentityMismatch {
                    reason: format!(
                        "phase '{}': program hash differs from saved checkpoint — \
                         workload definition changed since this phase last ran",
                        phase_label(&candidate.yaml_path),
                    ),
                };
            }
            if !saved.skip_eligible {
                // Saved entry was written when the phase was
                // declared `none`; even though the current
                // declaration is `idempotent`, the saved
                // execution may not have been idempotent. Be
                // conservative and re-run.
                return ResumeAction::ReRun;
            }
            ResumeAction::Skip
        }
        PhaseStatus::Running => {
            match &saved.cursor_state {
                Some(cs) => ResumeAction::CursorResume {
                    cursor_state: cs.clone(),
                },
                None => ResumeAction::ReRun,
            }
        }
        // Pending: never started in the prior invocation.
        // Failed: errors cascade decides whether to retry; from
        //   the planner's perspective, a Failed phase always
        //   re-runs (the cascade may then mark it Failed again
        //   or surface a different outcome).
        PhaseStatus::Pending | PhaseStatus::Failed => ResumeAction::ReRun,
    }
}

/// Reuse the writer's identity-key shape so both sides agree on
/// equality semantics.
fn identity_key(identity: &PhaseIdentity) -> String {
    let path_json = serde_json::to_string(&identity.yaml_path)
        .unwrap_or_else(|_| String::new());
    format!("{path_json}\x1f{}", identity.coords)
}

fn phase_label(yaml_path: &[super::identity::PathSegment]) -> String {
    use super::identity::PathSegment;
    yaml_path.iter()
        .filter_map(|seg| match seg {
            PathSegment::Phase(n) => Some(n.clone()),
            _ => None,
        })
        .last()
        .unwrap_or_else(|| "<unknown>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{PathSegment, storage::{OpCounts, PhaseEntry}};

    fn ident_with_hash(
        name: &str,
        coords: &str,
        hash: Option<[u8; 32]>,
    ) -> PhaseIdentity {
        PhaseIdentity {
            yaml_path: vec![
                PathSegment::Scenario("s".into()),
                PathSegment::Phase(name.into()),
            ],
            coords: coords.into(),
            phase_hash: hash,
        }
    }

    fn entry(
        identity: PhaseIdentity,
        status: PhaseStatus,
        skip_eligible: bool,
    ) -> PhaseEntry {
        PhaseEntry {
            identity,
            skip_eligible,
            status,
            duration_secs: Some(1.0),
            op_counts: Some(OpCounts::default()),
            cursor_state: None,
            error: None,
        }
    }

    fn checkpoint_with(phases: Vec<PhaseEntry>) -> Checkpoint {
        Checkpoint {
            version: 1,
            session: "s".into(),
            started_at: "t".into(),
            checkpoint_at: "t".into(),
            invocation: 1,
            phases,
        }
    }

    #[test]
    fn fresh_plan_rerun_for_everything() {
        let plan = ResumePlan::fresh();
        assert!(matches!(
            plan.action_for(&ident_with_hash("p", "", None)),
            ResumeAction::ReRun
        ));
        assert!(!plan.is_resume);
    }

    #[test]
    fn completed_idempotent_with_matching_hash_skips() {
        let h = [0xab; 32];
        let id = ident_with_hash("schema", "", Some(h));
        let saved = checkpoint_with(vec![
            entry(id.clone(), PhaseStatus::Completed, true),
        ]);
        let plan = ResumePlan::from_checkpoint(
            &saved, &[(id.clone(), true)],
        );
        assert!(matches!(plan.action_for(&id), ResumeAction::Skip));
        assert_eq!(plan.skip_count(), 1);
    }

    #[test]
    fn completed_with_hash_mismatch_invalidates() {
        let h_old = [0x01; 32];
        let h_new = [0x02; 32];
        let id_saved = ident_with_hash("schema", "", Some(h_old));
        let id_now = ident_with_hash("schema", "", Some(h_new));
        let saved = checkpoint_with(vec![
            entry(id_saved.clone(), PhaseStatus::Completed, true),
        ]);
        let plan = ResumePlan::from_checkpoint(
            &saved, &[(id_now.clone(), true)],
        );
        match plan.action_for(&id_now) {
            ResumeAction::IdentityMismatch { reason } => {
                assert!(reason.contains("schema"), "reason: {reason}");
            }
            other => panic!("expected IdentityMismatch, got {other:?}"),
        }
        assert_eq!(plan.mismatch_count(), 1);
    }

    #[test]
    fn declared_none_always_reruns_even_if_completed() {
        let id = ident_with_hash("schema", "", None);
        let saved = checkpoint_with(vec![
            entry(id.clone(), PhaseStatus::Completed, false),
        ]);
        let plan = ResumePlan::from_checkpoint(
            &saved, &[(id.clone(), false)],
        );
        assert!(matches!(plan.action_for(&id), ResumeAction::ReRun));
    }

    #[test]
    fn running_with_cursor_state_yields_cursor_resume() {
        let id = ident_with_hash("rampup", "", None);
        let mut e = entry(id.clone(), PhaseStatus::Running, true);
        e.cursor_state = Some(serde_json::json!({"next_cycle": 12345}));
        let saved = checkpoint_with(vec![e]);
        let plan = ResumePlan::from_checkpoint(
            &saved, &[(id.clone(), true)],
        );
        match plan.action_for(&id) {
            ResumeAction::CursorResume { cursor_state } => {
                assert_eq!(cursor_state["next_cycle"], 12345);
            }
            other => panic!("expected CursorResume, got {other:?}"),
        }
        assert_eq!(plan.cursor_resume_count(), 1);
    }

    #[test]
    fn running_without_cursor_state_reruns() {
        let id = ident_with_hash("rampup", "", None);
        let saved = checkpoint_with(vec![
            entry(id.clone(), PhaseStatus::Running, true),
        ]);
        let plan = ResumePlan::from_checkpoint(
            &saved, &[(id.clone(), true)],
        );
        assert!(matches!(plan.action_for(&id), ResumeAction::ReRun));
    }

    #[test]
    fn unknown_candidate_reruns() {
        let saved = checkpoint_with(vec![]);
        let id = ident_with_hash("brand_new", "", None);
        let plan = ResumePlan::from_checkpoint(&saved, &[(id.clone(), true)]);
        assert!(matches!(plan.action_for(&id), ResumeAction::ReRun));
    }

    #[test]
    fn failed_phase_reruns() {
        let id = ident_with_hash("flaky", "", None);
        let mut e = entry(id.clone(), PhaseStatus::Failed, true);
        e.error = Some("boom".into());
        let saved = checkpoint_with(vec![e]);
        let plan = ResumePlan::from_checkpoint(&saved, &[(id.clone(), true)]);
        assert!(matches!(plan.action_for(&id), ResumeAction::ReRun));
    }
}
