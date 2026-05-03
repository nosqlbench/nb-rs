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
pub use storage::{Checkpoint, OpCounts, PhaseEntry, PhaseStatus};
pub use writer::CheckpointWriter;
pub use resume::{ResumePlan, ResumeAction};

/// Declare every phase node in a freshly-pre-mapped scene tree
/// to the writer. Called once at session bootstrap, immediately
/// after [`crate::executor::pre_map_tree`] returns. Each phase
/// gets a `Pending` entry with no hash; the runtime updates the
/// hash via [`CheckpointWriter::update_phase_hash`] when the
/// phase compiles.
///
/// The `phases` map carries each phase's `checkpoint:`
/// declaration (parsed by `nbrs-workload`); entries with
/// `checkpoint: idempotent` set `skip_eligible = true`, all
/// others set `false`.
pub fn declare_scene_tree_phases(
    writer: &CheckpointWriter,
    tree: &crate::scene_tree::SceneTree,
    phases: &std::collections::HashMap<String, nbrs_workload::model::WorkloadPhase>,
) {
    for node in tree.dfs_phases() {
        let identity = PhaseIdentity {
            yaml_path: node.yaml_path.clone(),
            coords: node.labels.clone(),
            phase_hash: None,
        };
        let skip_eligible = phases.get(&node.name)
            .and_then(|p| p.checkpoint.as_ref())
            .map(|c| c.idempotent)
            .unwrap_or(false);
        writer.declare_phase(identity, skip_eligible);
    }
}

/// Build a list of `(identity, declared_idempotent)` pairs for
/// every phase the scene tree will execute. The runner feeds
/// this to [`ResumePlan::from_checkpoint`] when resuming, so the
/// planner can classify each freshly-pre-mapped phase against
/// the saved document.
///
/// Each candidate's `phase_hash` is the **ancestor-chain
/// instance hash** — a SHA-256 over the workload-root program's
/// canonical_hash plus every intermediate scope kernel's
/// canonical_hash, in chain order. This is what's computable at
/// pre-map time (the phase's own program compiles lazily during
/// `run_phase` and isn't yet available); it captures every
/// upstream binding edit, which is the dominant source of
/// resume-time identity drift.
///
/// The runtime stamps the **full** instance_hash (own program +
/// ancestor chain) when the phase compiles, so the saved hash
/// includes the phase's own program shape. On resume:
///
/// - If the workload root or intermediate scope kernel changes
///   between runs, candidate.phase_hash differs from any saved
///   hash → IdentityMismatch → ReRun.
/// - If only the phase's own program changes, the candidate's
///   ancestor-only hash still matches the saved hash's
///   ancestor portion (they share that prefix); the hash
///   carrier is `Some` on both sides but they're different
///   shapes — `matches_full` falls back to structural equality
///   in that case (one carries phase+ancestors, the other
///   just ancestors). This is intentional: the phase's own
///   program edits are caught by the wholesale-purge + re-run
///   path (operator-driven workload refactor), not by
///   identity-mismatch invalidation.
pub fn scene_tree_resume_candidates(
    tree: &crate::scene_tree::SceneTree,
    scope_tree: &crate::scope_tree::ScopeTree,
    phases: &std::collections::HashMap<String, nbrs_workload::model::WorkloadPhase>,
) -> Vec<(PhaseIdentity, bool)> {
    tree.dfs_phases().map(|node| {
        let phase_hash = ancestor_chain_hash(scope_tree, &node.name);
        let identity = PhaseIdentity {
            yaml_path: node.yaml_path.clone(),
            coords: node.labels.clone(),
            phase_hash,
        };
        let idempotent = phases.get(&node.name)
            .and_then(|p| p.checkpoint.as_ref())
            .map(|c| c.idempotent)
            .unwrap_or(false);
        (identity, idempotent)
    }).collect()
}

/// Compute a phase candidate's ancestor-chain instance hash by
/// looking up the scope-tree node and walking its installed
/// ancestor kernels. Returns `None` if the scope tree has no
/// installed kernels (defensive — the workload root always has
/// one in production).
fn ancestor_chain_hash(
    scope_tree: &crate::scope_tree::ScopeTree,
    phase_name: &str,
) -> Option<[u8; 32]> {
    let idx = scope_tree.phase_node_by_name(phase_name)?;
    let ancestors = scope_tree.ancestor_kernels(idx);
    if ancestors.is_empty() {
        return None;
    }
    // The chain hash uses GkProgram::instance_hash with the
    // first ancestor as the "self" anchor and the rest as
    // ancestors-of-ancestor. Same shape the runtime produces
    // for its [own_program, ancestors...] chain — just without
    // the own_program prefix. The two hashes are *different*
    // (own_program contributes), so identity match still falls
    // back to structural equality when one side has the longer
    // chain — see `scene_tree_resume_candidates` doc.
    let head = ancestors[0].program();
    let tail: Vec<&nbrs_variates::kernel::GkProgram> = ancestors[1..]
        .iter().map(|k| k.program().as_ref()).collect();
    Some(head.instance_hash(&tail))
}
