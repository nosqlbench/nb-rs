// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `CheckpointWriter` — mutable owner of the in-memory checkpoint
//! document, plus the public API the executor and tick flusher
//! drive.
//!
//! Per SRD-44 §"Writer lifecycle", the writer:
//!
//! - is created at session bootstrap (or restored from a saved
//!   document on resume),
//! - has phases *declared* into it during pre-map (so an entry
//!   exists with `Pending` status for every phase the run plans
//!   to touch — even ones that never get to run, so resume can
//!   tell "didn't run yet" from "wasn't planned"),
//! - receives phase-lifecycle calls (`phase_started`,
//!   `phase_completed`, `phase_failed`) from the executor,
//! - receives op-count and cursor-state updates from the metrics
//!   tick callback,
//! - is flushed (atomic JSON rewrite) on the metrics-tick cadence
//!   and at every lifecycle transition.
//!
//! The writer is `Send + Sync` (interior mutex) so the executor,
//! the tick callback, and the cursor-state collector can all hold
//! one `Arc<CheckpointWriter>`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use super::identity::PhaseIdentity;
use super::storage::{Checkpoint, OpCounts, PhaseEntry, PhaseStatus, write_atomic};

/// On-disk checkpoint version this build emits / accepts. Bump
/// only when an incompatible schema change ships; resume against
/// a different version is rejected at read time
/// (`storage::read`).
pub const CHECKPOINT_VERSION: u32 = 1;

/// Writer-side handle to the per-session checkpoint document.
/// One instance per session; held as `Arc<CheckpointWriter>` and
/// shared between the executor (lifecycle calls) and the metrics
/// tick (count + cursor + flush calls).
pub struct CheckpointWriter {
    /// Absolute path to `logs/<session>/checkpoint.json`.
    path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    /// The in-memory document. Always mirrors the most recent
    /// state declared / observed; flush serialises this out.
    doc: Checkpoint,
    /// Map from `identity_key(&PhaseIdentity)` to the index of
    /// the matching entry in `doc.phases`. Avoids an O(n) linear
    /// scan on every lifecycle call.
    index: HashMap<String, usize>,
    /// `true` when at least one mutation has happened since the
    /// last successful `flush`. The tick flusher uses this to
    /// skip the syscall round-trip when nothing changed.
    dirty: bool,
}

impl CheckpointWriter {
    /// Construct a fresh writer for a brand-new session. The
    /// in-memory document starts with no phases; declare them
    /// via [`Self::declare_phase`] during pre-map.
    pub fn new(
        path: PathBuf,
        session: String,
        started_at: String,
        invocation: u32,
    ) -> Self {
        let doc = Checkpoint {
            version: CHECKPOINT_VERSION,
            session,
            started_at: started_at.clone(),
            checkpoint_at: started_at,
            invocation,
            phases: Vec::new(),
        };
        Self {
            path,
            inner: Mutex::new(Inner {
                doc,
                index: HashMap::new(),
                dirty: false,
            }),
        }
    }

    /// Restore a writer from a previously-written document on
    /// resume. The caller is responsible for having parsed and
    /// version-checked the document via
    /// [`super::storage::read`]. The restored writer keeps the
    /// existing phase entries (so already-Completed phases stay
    /// Completed across the resume boundary) and bumps
    /// `invocation` per the operator-supplied count.
    pub fn from_existing(
        path: PathBuf,
        mut doc: Checkpoint,
        new_checkpoint_at: String,
        new_invocation: u32,
    ) -> Self {
        doc.checkpoint_at = new_checkpoint_at;
        doc.invocation = new_invocation;
        let index = build_index(&doc.phases);
        Self {
            path,
            inner: Mutex::new(Inner { doc, index, dirty: true }),
        }
    }

    /// Declare a phase the run plans to execute. Called during
    /// pre-map for every phase; idempotent (re-declaration is a
    /// no-op so the resume path can declare the same phases the
    /// saved doc already lists).
    ///
    /// `skip_eligible` reflects the phase's `checkpoint:`
    /// declaration in the workload YAML — `true` for
    /// `idempotent` (and the long-form mapping with
    /// `idempotent: true`), `false` for `none` / no declaration.
    pub fn declare_phase(&self, identity: PhaseIdentity, skip_eligible: bool) {
        let mut g = self.inner.lock().unwrap();
        let key = identity_key(&identity);
        if g.index.contains_key(&key) {
            return;
        }
        let entry = PhaseEntry {
            identity,
            skip_eligible,
            status: PhaseStatus::Pending,
            duration_secs: None,
            op_counts: None,
            cursor_state: None,
            error: None,
        };
        g.doc.phases.push(entry);
        let idx = g.doc.phases.len() - 1;
        g.index.insert(key, idx);
        g.dirty = true;
    }

    /// Mark a declared phase as `Running`. No-op if the phase
    /// wasn't declared (defensive: declare-before-start is the
    /// pre-map contract, but a missed declaration shouldn't
    /// crash the run).
    pub fn phase_started(&self, identity: &PhaseIdentity) {
        self.with_entry(identity, |e| {
            e.status = PhaseStatus::Running;
            e.error = None;
        });
    }

    /// Mark a declared phase as `Completed` with the given
    /// wall-clock duration. Per SRD-44 §"Status Completed is
    /// load-bearing", this is the call that makes the phase
    /// eligible to skip on a future resume.
    pub fn phase_completed(&self, identity: &PhaseIdentity, duration_secs: f64) {
        self.with_entry(identity, |e| {
            e.status = PhaseStatus::Completed;
            e.duration_secs = Some(duration_secs);
            e.cursor_state = None;
            e.error = None;
        });
    }

    /// Mark a declared phase as `Failed`. The error message is
    /// preserved for resume diagnostics.
    pub fn phase_failed(&self, identity: &PhaseIdentity, error: &str) {
        let err_owned = error.to_string();
        self.with_entry(identity, |e| {
            e.status = PhaseStatus::Failed;
            e.error = Some(err_owned.clone());
            e.cursor_state = None;
        });
    }

    /// Record op-execution counts from the live activity. Called
    /// from the metrics tick callback for the currently-running
    /// phase.
    pub fn update_op_counts(&self, identity: &PhaseIdentity, counts: OpCounts) {
        self.with_entry(identity, |e| {
            e.op_counts = Some(counts);
        });
    }

    /// Record the latest cursor-state snapshot for a Tier 2
    /// phase. Called from the metrics tick callback.
    pub fn update_cursor(
        &self,
        identity: &PhaseIdentity,
        cursor_state: serde_json::Value,
    ) {
        self.with_entry(identity, |e| {
            e.cursor_state = Some(cursor_state);
        });
    }

    /// Flush the in-memory document to disk via the atomic
    /// rename protocol. No-op when nothing has changed since the
    /// last flush.
    ///
    /// Per SRD-44 §"Durability ordering", the metrics-DB fsync
    /// must complete *before* this call so a checkpoint that
    /// records "phase X completed" never references metrics that
    /// haven't been durably persisted. The caller (tick flusher
    /// / lifecycle hook) is responsible for that ordering.
    pub fn flush(&self) -> Result<(), String> {
        let snapshot = {
            let mut g = self.inner.lock().unwrap();
            if !g.dirty {
                return Ok(());
            }
            g.doc.checkpoint_at = now_rfc3339();
            g.dirty = false;
            g.doc.clone()
        };
        match write_atomic(&snapshot, &self.path) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Restore dirty so the next tick retries.
                self.inner.lock().unwrap().dirty = true;
                Err(e)
            }
        }
    }

    /// Read-only snapshot of the current in-memory document.
    /// Useful for diagnostics and tests.
    pub fn snapshot(&self) -> Checkpoint {
        self.inner.lock().unwrap().doc.clone()
    }

    /// Path the writer flushes to.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn with_entry<F: FnOnce(&mut PhaseEntry)>(&self, identity: &PhaseIdentity, f: F) {
        let mut g = self.inner.lock().unwrap();
        let key = identity_key(identity);
        if let Some(&idx) = g.index.get(&key) {
            f(&mut g.doc.phases[idx]);
            g.dirty = true;
        }
    }
}

/// Build a lookup key for the index map. Identity equality is
/// `(yaml_path, coords)` per SRD-44; the hash is sufficiency,
/// not identity, so it's deliberately excluded from the key.
fn identity_key(identity: &PhaseIdentity) -> String {
    // Serialise the path into a delimited string. The PathSegment
    // variants are simple enums of strings / vec-of-strings, so
    // serde_json gives a canonical, collision-free encoding.
    let path_json = serde_json::to_string(&identity.yaml_path)
        .unwrap_or_else(|_| String::new());
    format!("{path_json}\x1f{}", identity.coords)
}

fn build_index(phases: &[PhaseEntry]) -> HashMap<String, usize> {
    let mut m = HashMap::with_capacity(phases.len());
    for (i, e) in phases.iter().enumerate() {
        m.insert(identity_key(&e.identity), i);
    }
    m
}

/// Format the current wall-clock time as an RFC 3339 UTC
/// timestamp, e.g. `2026-01-01T00:00:00Z`. Matches the shape
/// session.rs uses for human-readable timestamps; kept local
/// to avoid pulling in chrono just for one call site.
fn now_rfc3339() -> String {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_ymd(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z"
    )
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{PathSegment, PhaseIdentity};

    fn ident(name: &str, coords: &str) -> PhaseIdentity {
        PhaseIdentity {
            yaml_path: vec![
                PathSegment::Scenario("s".into()),
                PathSegment::Phase(name.into()),
            ],
            coords: coords.into(),
            phase_hash: Some([0xcd; 32]),
        }
    }

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "nbrs-checkpoint-writer-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn declare_then_complete_then_flush() {
        let dir = tempdir();
        let path = dir.join("checkpoint.json");
        let w = CheckpointWriter::new(
            path.clone(), "sess".into(), "2026-01-01T00:00:00Z".into(), 1,
        );
        let id = ident("schema", "");
        w.declare_phase(id.clone(), true);
        w.phase_started(&id);
        w.phase_completed(&id, 1.5);
        w.flush().expect("flush");

        let parsed = super::super::storage::read(&path)
            .expect("read")
            .expect("present");
        assert_eq!(parsed.phases.len(), 1);
        assert_eq!(parsed.phases[0].status, PhaseStatus::Completed);
        assert_eq!(parsed.phases[0].duration_secs, Some(1.5));
    }

    #[test]
    fn redundant_declare_is_idempotent() {
        let dir = tempdir();
        let w = CheckpointWriter::new(
            dir.join("c.json"), "s".into(), "t".into(), 1,
        );
        let id = ident("p", "(k=1)");
        w.declare_phase(id.clone(), true);
        w.declare_phase(id.clone(), false); // re-declare ignored
        let snap = w.snapshot();
        assert_eq!(snap.phases.len(), 1);
        assert!(snap.phases[0].skip_eligible, "first declare wins");
    }

    #[test]
    fn flush_is_no_op_when_clean() {
        let dir = tempdir();
        let path = dir.join("c.json");
        let w = CheckpointWriter::new(
            path.clone(), "s".into(), "t".into(), 1,
        );
        w.declare_phase(ident("p", ""), true);
        w.flush().expect("first flush");
        let mtime1 = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        w.flush().expect("second flush no-op");
        let mtime2 = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(mtime1, mtime2, "second flush must not rewrite the file");
    }

    #[test]
    fn from_existing_keeps_completed_phases() {
        let dir = tempdir();
        let path = dir.join("c.json");
        // First session writes one Completed phase.
        let w = CheckpointWriter::new(
            path.clone(), "s".into(), "2026-01-01T00:00:00Z".into(), 1,
        );
        let id = ident("schema", "");
        w.declare_phase(id.clone(), true);
        w.phase_completed(&id, 0.5);
        w.flush().expect("flush");
        // Second session resumes.
        let saved = super::super::storage::read(&path).expect("read").expect("present");
        let w2 = CheckpointWriter::from_existing(
            path.clone(), saved, "2026-01-01T00:01:00Z".into(), 2,
        );
        let snap = w2.snapshot();
        assert_eq!(snap.invocation, 2);
        assert_eq!(snap.phases.len(), 1);
        assert_eq!(snap.phases[0].status, PhaseStatus::Completed);
    }
}
