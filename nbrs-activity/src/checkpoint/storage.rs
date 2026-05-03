// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint storage — JSON file format + atomic-rename writer.
//!
//! See SRD-44 §"Storage format" for the on-disk shape.
//! `Checkpoint` is the top-level document; one `PhaseEntry`
//! per pre-mapped phase records its identity, status, and any
//! cursor state for in-flight Tier 2 resume.

use serde::{Deserialize, Serialize};
use std::path::Path;

use super::identity::PhaseIdentity;

/// On-disk top-level checkpoint document. One per session at
/// `logs/<session>/checkpoint.json`. The file is rewritten
/// atomically (tmp + fsync + rename + dir-fsync) on every
/// flush; a partial-write crash leaves the previous version
/// intact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    /// File-format version. `1` until we ship a `2`. Each
    /// resume invocation refuses to read a checkpoint whose
    /// version it doesn't recognise — fail fast over silent
    /// schema drift.
    pub version: u32,
    /// Session identifier — same string used in
    /// `logs/<session>/`. The resume CLI's auto-detect picks
    /// the most-recent session; explicit `--resume <id>`
    /// names this directly.
    pub session: String,
    /// RFC 3339 timestamp of session start — first
    /// invocation's `nbrs run` start.
    pub started_at: String,
    /// RFC 3339 timestamp of this flush — updated on every
    /// successful write.
    pub checkpoint_at: String,
    /// 1-based invocation counter. First `nbrs run` is `1`;
    /// each `--resume` increments by 1. Used in `session.log`
    /// separator lines (`--- RESUMED <ts> [#N] ---`) and in
    /// post-run summary diagnostics if needed.
    pub invocation: u32,
    /// One entry per pre-mapped phase. Order matches the
    /// scenario tree's DFS — same order the post-run summary
    /// uses, same order the resume planner walks.
    pub phases: Vec<PhaseEntry>,
}

/// Per-phase entry in the checkpoint document.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhaseEntry {
    /// Phase identity tuple. The resume planner uses
    /// `(yaml_path, coords)` as the structural match key and
    /// (when present) the `phase_hash` as the sufficiency
    /// check. See SRD-44 §"Phase identity".
    #[serde(flatten)]
    pub identity: PhaseIdentity,
    /// Whether this phase is *eligible to skip* on resume
    /// per its `checkpoint:` declaration. `true` means the
    /// resume planner may classify the phase as Skip when
    /// the saved status is Completed and identity matches.
    /// `false` (operator declared `checkpoint: none` or no
    /// declaration at all) means the phase always re-runs,
    /// regardless of saved status.
    pub skip_eligible: bool,
    /// Lifecycle status at the time of the most recent flush.
    pub status: PhaseStatus,
    /// Wall-clock duration of the *successful* execution.
    /// Set when status transitions to Completed; preserved on
    /// subsequent flushes. None for in-flight (`Running`) or
    /// terminally-failed phases.
    #[serde(default)]
    pub duration_secs: Option<f64>,
    /// Op counts from the live activity. Captured on every
    /// flush; useful for ETA and post-run summary in resumed
    /// sessions.
    #[serde(default)]
    pub op_counts: Option<OpCounts>,
    /// Tier 2 only. Opaque cursor-state snapshot from the
    /// active source factory, captured on each flush while
    /// the phase is `Running`. Resume planner restores this
    /// to a freshly-constructed cursor source so the phase
    /// continues from where it left off.
    #[serde(default)]
    pub cursor_state: Option<serde_json::Value>,
    /// Per-phase error message recorded when the phase
    /// transitioned to `Failed` — preserved across flushes
    /// so resume diagnostics can reference the original
    /// failure mode.
    #[serde(default)]
    pub error: Option<String>,
}

/// Lifecycle status as recorded in the checkpoint file.
/// Mirrors `crate::scene_tree::PhaseStatus` semantically; kept
/// as a separate type so the on-disk vocabulary doesn't
/// coupled-evolve with scene-tree internal status (e.g. if we
/// ever add a transient state for a runtime invariant that
/// has no on-disk meaning, the storage type stays clean).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PhaseStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl From<crate::scene_tree::PhaseStatus> for PhaseStatus {
    fn from(s: crate::scene_tree::PhaseStatus) -> Self {
        match s {
            crate::scene_tree::PhaseStatus::Pending => Self::Pending,
            crate::scene_tree::PhaseStatus::Running => Self::Running,
            crate::scene_tree::PhaseStatus::Completed => Self::Completed,
            crate::scene_tree::PhaseStatus::Failed(_) => Self::Failed,
        }
    }
}

/// Op-execution counts captured on every flush.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OpCounts {
    pub started: u64,
    pub finished: u64,
    pub errors: u64,
}

/// Atomically write a `Checkpoint` to `path`. Writes to
/// `<path>.tmp` first, fsyncs the file, renames to `<path>`,
/// fsyncs the parent directory. A crash anywhere along this
/// path leaves the previous `<path>` intact (or absent on
/// first write); the only failure mode is no checkpoint at all
/// for this flush, which the next flush will replace.
pub fn write_atomic(checkpoint: &Checkpoint, path: &Path) -> Result<(), String> {
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    let parent = path.parent().ok_or_else(|| {
        format!("checkpoint path has no parent directory: {}", path.display())
    })?;
    std::fs::create_dir_all(parent).map_err(|e| {
        format!("create checkpoint dir {}: {e}", parent.display())
    })?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| format!("open {} for write: {e}", tmp.display()))?;
        let bytes = serde_json::to_vec_pretty(checkpoint)
            .map_err(|e| format!("serialise checkpoint: {e}"))?;
        f.write_all(&bytes)
            .map_err(|e| format!("write {}: {e}", tmp.display()))?;
        // Push the bytes durably to the storage device. If
        // this fails, downstream rename leaves a phantom
        // tmp file that the next flush will clobber — no
        // data loss, just a one-tick wasted write.
        f.sync_all()
            .map_err(|e| format!("fsync {}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        format!("rename {} → {}: {e}", tmp.display(), path.display())
    })?;
    // fsync the directory so the rename itself is durable.
    // Otherwise a power failure between the rename and the
    // directory's natural sync could leave us with the
    // previous version even though `rename` returned.
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Read a `Checkpoint` from `path`. Returns `Ok(None)` when
/// the file doesn't exist (fresh session) and `Err` when the
/// file exists but is malformed (operator should investigate
/// rather than silently start fresh).
pub fn read(path: &Path) -> Result<Option<Checkpoint>, String> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let cp: Checkpoint = serde_json::from_str(&s)
                .map_err(|e| format!(
                    "parse checkpoint {}: {e}", path.display()
                ))?;
            if cp.version != 1 {
                return Err(format!(
                    "checkpoint {}: unsupported version {} (this build supports v1)",
                    path.display(), cp.version
                ));
            }
            Ok(Some(cp))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("read checkpoint {}: {e}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::PathSegment;

    fn entry(seq_phase_name: &str, status: PhaseStatus) -> PhaseEntry {
        PhaseEntry {
            identity: PhaseIdentity {
                yaml_path: vec![
                    PathSegment::Scenario("test".into()),
                    PathSegment::Phase(seq_phase_name.into()),
                ],
                coords: String::new(),
                phase_hash: Some([0xab; 32]),
            },
            skip_eligible: true,
            status,
            duration_secs: Some(1.5),
            op_counts: Some(OpCounts { started: 100, finished: 100, errors: 0 }),
            cursor_state: None,
            error: None,
        }
    }

    #[test]
    fn round_trip() {
        let cp = Checkpoint {
            version: 1,
            session: "test_20260101_000000".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            checkpoint_at: "2026-01-01T00:01:00Z".into(),
            invocation: 1,
            phases: vec![
                entry("schema", PhaseStatus::Completed),
                entry("rampup", PhaseStatus::Running),
            ],
        };
        let bytes = serde_json::to_vec_pretty(&cp).expect("serialise");
        let parsed: Checkpoint = serde_json::from_slice(&bytes).expect("parse");
        assert_eq!(parsed.session, cp.session);
        assert_eq!(parsed.phases.len(), 2);
        assert_eq!(parsed.phases[0].status, PhaseStatus::Completed);
        assert_eq!(parsed.phases[1].status, PhaseStatus::Running);
    }

    #[test]
    fn atomic_write_creates_file_and_no_tmp_residue() {
        let dir = tempdir();
        let path = dir.join("checkpoint.json");
        let cp = Checkpoint {
            version: 1,
            session: "x".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            checkpoint_at: "2026-01-01T00:00:00Z".into(),
            invocation: 1,
            phases: vec![],
        };
        write_atomic(&cp, &path).expect("write");
        assert!(path.exists(), "checkpoint file should exist");
        let tmp = path.with_extension("json.tmp");
        assert!(!tmp.exists(), "tmp file should be gone after rename");
    }

    #[test]
    fn read_missing_file_yields_none() {
        let dir = tempdir();
        let path = dir.join("nonexistent.json");
        let result = read(&path).expect("read should not error on missing file");
        assert!(result.is_none());
    }

    #[test]
    fn read_unsupported_version_errors() {
        let dir = tempdir();
        let path = dir.join("future.json");
        let body = r#"{"version":99,"session":"x","started_at":"","checkpoint_at":"","invocation":1,"phases":[]}"#;
        std::fs::write(&path, body).expect("write");
        let err = read(&path).expect_err("expected version-mismatch error");
        assert!(err.contains("version 99"), "got: {err}");
    }

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("nbrs-checkpoint-test-{}", rand_suffix()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn rand_suffix() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{n:x}")
    }
}
