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
    /// File descriptor of the held flock lockfile
    /// (`logs/<session>/checkpoint.lock`). The descriptor is
    /// owned for the lifetime of the writer; closing it (Drop)
    /// releases the advisory lock automatically. `None` when
    /// flock acquisition failed soft (e.g. cross-FS lock not
    /// supported) — the lock is best-effort, not a correctness
    /// barrier.
    _lock_fd: Option<LockHandle>,
}

/// Owning wrapper around a Unix fd that releases the flock when
/// dropped. `flock(LOCK_UN)` is implicit on close — kernel
/// releases the lock when the last fd referencing the inode
/// closes, so `Drop` simply closes the fd.
struct LockHandle(libc::c_int);

impl Drop for LockHandle {
    fn drop(&mut self) {
        unsafe { libc::close(self.0); }
    }
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
        let lock = acquire_flock(&path);
        Self {
            path,
            inner: Mutex::new(Inner {
                doc,
                index: HashMap::new(),
                dirty: false,
            }),
            _lock_fd: lock,
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
        let lock = acquire_flock(&path);
        Self {
            path,
            inner: Mutex::new(Inner { doc, index, dirty: true }),
            _lock_fd: lock,
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

    /// Set the program-canonical hash on a declared phase.
    /// Called once per phase the first time it compiles
    /// (per-phase compile happens during `run_phase`); the
    /// hash flows into the saved entry so a future resume
    /// invocation can detect program drift via
    /// [`PhaseIdentity::matches_full`].
    pub fn update_phase_hash(&self, identity: &PhaseIdentity, hash: [u8; 32]) {
        self.with_entry(identity, |e| {
            e.identity.phase_hash = Some(hash);
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
            g.doc.checkpoint_at = super::storage::now_rfc3339();
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

    /// If the workload has incomplete phases declared
    /// `checkpoint: idempotent` (any phase whose status is
    /// `Failed`, `Running`, or `Pending` and whose
    /// `skip_eligible` flag is `true`), return a multi-line
    /// hint string the runtime can show the operator on exit:
    ///
    /// ```text
    ///   This workload has resumable phases that didn't complete.
    ///   To continue from where it stopped:
    ///     nbrs run <workload> --resume <session>
    ///   To pin the session name for repeatable resumes:
    ///     nbrs run <workload> --session <session>
    /// ```
    ///
    /// Returns `None` when every skip-eligible phase already
    /// reached `Completed` (no resume-worthy state) or when
    /// nothing was declared idempotent (resume isn't useful for
    /// the workload at all).
    pub fn resume_hint(&self) -> Option<String> {
        let cp = self.snapshot();
        let recoverable = cp.phases.iter().any(|e| {
            e.skip_eligible
                && !matches!(e.status, PhaseStatus::Completed)
        });
        if !recoverable {
            return None;
        }
        Some(format!(
            "This session has resumable phases that didn't complete.\n  \
             To continue from where it stopped:\n    \
             nbrs run <workload> --session-dir {} (already set if you exported \
             SESSION_DIRECTORY) --resume\n  \
             To pin the session name for repeatable resumes:\n    \
             nbrs run <workload> --session {} (then add --resume next time)",
            self.path.parent().map(|p| p.display().to_string()).unwrap_or_default(),
            cp.session,
        ))
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

/// Take a non-blocking exclusive advisory lock on a sibling
/// `checkpoint.lock` file alongside the checkpoint document.
/// Returns `Some` on success, `None` when:
///
/// - the parent directory can't be created or opened (the
///   writer falls back to lockless operation — racing two
///   resumes against an unwritable session is already broken),
/// - another process holds the lock (`flock(EX|NB)` returns
///   `EWOULDBLOCK`) — in which case the runner aborts with a
///   clear diagnostic per SRD-44 §"Concurrent-resume protection".
///
/// The flock is **advisory**: only consumers that take the same
/// lock care. The contract is "no two `nbrs run` invocations on
/// the same session at once" — flock enforces that without
/// stopping a stray `cat` or backup tool from reading the
/// checkpoint file out-of-band.
fn acquire_flock(checkpoint_path: &std::path::Path) -> Option<LockHandle> {
    use std::os::unix::ffi::OsStrExt;
    let parent = checkpoint_path.parent()?;
    if let Err(e) = std::fs::create_dir_all(parent) {
        eprintln!(
            "warning: could not create checkpoint dir {}: {e} (concurrent-resume protection skipped)",
            parent.display(),
        );
        return None;
    }
    let lock_path = checkpoint_path.with_extension("lock");
    let c_path = std::ffi::CString::new(lock_path.as_os_str().as_bytes()).ok()?;
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC,
            0o644,
        )
    };
    if fd < 0 {
        let errno = std::io::Error::last_os_error();
        eprintln!(
            "warning: could not open lockfile {}: {errno} (concurrent-resume protection skipped)",
            lock_path.display(),
        );
        return None;
    }
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let errno = std::io::Error::last_os_error();
        unsafe { libc::close(fd); }
        if errno.raw_os_error() == Some(libc::EWOULDBLOCK) {
            // Another process holds the lock — surface this as
            // a hard error via panic-with-message; the runner
            // catches it and reports cleanly.
            panic!(
                "checkpoint: another process holds the resume lock at {} \
                 (concurrent `nbrs run --resume` against the same session?). \
                 If you're certain no other process is running, remove the \
                 lockfile and retry.",
                lock_path.display(),
            );
        } else {
            eprintln!(
                "warning: flock on {} failed: {errno} (concurrent-resume protection skipped)",
                lock_path.display(),
            );
            return None;
        }
    }
    Some(LockHandle(fd))
}

fn build_index(phases: &[PhaseEntry]) -> HashMap<String, usize> {
    let mut m = HashMap::with_capacity(phases.len());
    for (i, e) in phases.iter().enumerate() {
        m.insert(identity_key(&e.identity), i);
    }
    m
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
        // First session writes one Completed phase, then drops
        // (releasing its flock) so the resume writer can claim
        // the lock. Mirrors the production lifecycle: the
        // prior process exits before the resume invocation
        // starts.
        let saved = {
            let w = CheckpointWriter::new(
                path.clone(), "s".into(), "2026-01-01T00:00:00Z".into(), 1,
            );
            let id = ident("schema", "");
            w.declare_phase(id.clone(), true);
            w.phase_completed(&id, 0.5);
            w.flush().expect("flush");
            super::super::storage::read(&path).expect("read").expect("present")
        };
        let w2 = CheckpointWriter::from_existing(
            path.clone(), saved, "2026-01-01T00:01:00Z".into(), 2,
        );
        let snap = w2.snapshot();
        assert_eq!(snap.invocation, 2);
        assert_eq!(snap.phases.len(), 1);
        assert_eq!(snap.phases[0].status, PhaseStatus::Completed);
    }

    #[test]
    fn flock_blocks_concurrent_writer_on_same_path() {
        let dir = tempdir();
        let path = dir.join("c.json");
        let _w = CheckpointWriter::new(
            path.clone(), "s".into(), "t".into(), 1,
        );
        // A second writer on the same path must panic with the
        // resume-lock message — not silently shadow the first.
        let result = std::panic::catch_unwind(|| {
            let _w2 = CheckpointWriter::new(
                path.clone(), "s".into(), "t".into(), 1,
            );
        });
        assert!(result.is_err(), "second writer should panic on flock contention");
    }
}
