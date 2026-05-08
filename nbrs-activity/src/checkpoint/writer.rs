// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `CheckpointWriter` — append-only event-log owner for the
//! per-session checkpoint document. SRD-44a §"Writer behaviour".
//!
//! Per SRD-44a, every state-changing observation is one event
//! line written to `logs/<session>/checkpoint.jsonl`. The writer:
//!
//! - is created at session bootstrap (or restored from a saved
//!   document on resume) — emits a `session_start` event,
//! - has phases *declared* into it during pre-map — one
//!   `phase_declared` event each,
//! - receives phase-lifecycle calls (`phase_started`,
//!   `phase_completed`, `phase_failed`) — one event per call,
//! - receives op-count and cursor-state updates from the
//!   metrics tick callback — one `phase_progress` event per
//!   tick.
//!
//! No whole-document rewrite. The file is opened in append mode
//! (`O_APPEND`); each event is a single `\n`-terminated line.
//! In-memory state is the same `Checkpoint` document as before
//! so `snapshot()` and `resume_hint()` keep working without
//! re-folding from disk.
//!
//! The writer is `Send + Sync` (interior mutex) so the executor,
//! the tick callback, and the cursor-state collector can all
//! hold one `Arc<CheckpointWriter>`.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use super::events::{CheckpointEvent, hash_to_hex};
use super::identity::PhaseIdentity;
use super::storage::{Checkpoint, OpCounts, PhaseEntry, PhaseStatus, now_rfc3339};

/// On-disk checkpoint version this build emits / accepts. Bump
/// only when an incompatible schema change ships; resume against
/// a different version is rejected at read time.
pub const CHECKPOINT_VERSION: u32 = 1;

/// Writer-side handle to the per-session checkpoint event log.
/// One instance per session; held as `Arc<CheckpointWriter>` and
/// shared between the executor (lifecycle calls) and the metrics
/// tick (count + cursor + flush calls).
pub struct CheckpointWriter {
    /// Absolute path to `logs/<session>/checkpoint.jsonl`.
    path: PathBuf,
    inner: Mutex<Inner>,
    /// File descriptor of the held flock lockfile
    /// (`logs/<session>/checkpoint.lock`). The descriptor is
    /// owned for the lifetime of the writer; closing it (Drop)
    /// releases the advisory lock automatically. `None` when
    /// flock acquisition failed soft.
    _lock_fd: Option<LockHandle>,
}

/// Owning wrapper around a Unix fd that releases the flock when
/// dropped. `flock(LOCK_UN)` is implicit on close.
struct LockHandle(libc::c_int);

impl Drop for LockHandle {
    fn drop(&mut self) {
        unsafe { libc::close(self.0); }
    }
}

struct Inner {
    /// In-memory fold mirror. Always reflects the most recent
    /// state declared / observed; consumers (`snapshot`,
    /// `resume_hint`) read this. The on-disk view is the
    /// append-only event stream — these two converge after a
    /// reader folds the log.
    doc: Checkpoint,
    /// Map from `identity_key(&PhaseIdentity)` to the index of
    /// the matching entry in `doc.phases`. Avoids an O(n) linear
    /// scan on every lifecycle call.
    index: HashMap<String, usize>,
    /// Open append-mode handle to the JSONL log. Keeping the
    /// file open avoids one `open(2)` per event; the `O_APPEND`
    /// flag guarantees writes are atomic up to `PIPE_BUF`
    /// (4 KB on Linux) without explicit locking.
    file: File,
}

impl CheckpointWriter {
    /// Construct a fresh writer for a brand-new session. Emits
    /// the leading `session_start` record before returning.
    /// Subsequent `declare_phase` / lifecycle calls append events
    /// onto the same log.
    pub fn new(
        path: PathBuf,
        session: String,
        started_at: String,
        invocation: u32,
    ) -> Self {
        let doc = Checkpoint {
            version: CHECKPOINT_VERSION,
            session: session.clone(),
            started_at: started_at.clone(),
            checkpoint_at: started_at.clone(),
            invocation,
            phases: Vec::new(),
        };
        let lock = acquire_flock(&path);
        let file = open_append(&path);
        let writer = Self {
            path,
            inner: Mutex::new(Inner {
                doc,
                index: HashMap::new(),
                file,
            }),
            _lock_fd: lock,
        };
        // Per SRD-44a §"File location and format" — the first
        // line of a fresh log MUST be a `session_start` record;
        // the reader rejects logs whose first record is anything
        // else. Resume continues the same log by appending its
        // own `session_start`, so this stays correct across
        // invocations too.
        writer.append_event(CheckpointEvent::SessionStart {
            at: now_rfc3339(),
            version: CHECKPOINT_VERSION,
            session,
            started_at,
            invocation,
        });
        writer
    }

    /// Restore a writer from a previously-written document on
    /// resume. The caller is responsible for having parsed and
    /// version-checked the document via [`super::storage::read`].
    /// The restored writer keeps the existing phase entries and
    /// emits a fresh `session_start` event with the new
    /// invocation counter — appending continues onto the same
    /// JSONL log per SRD-44a.
    pub fn from_existing(
        path: PathBuf,
        mut doc: Checkpoint,
        new_checkpoint_at: String,
        new_invocation: u32,
    ) -> Self {
        doc.checkpoint_at = new_checkpoint_at;
        doc.invocation = new_invocation;
        let session = doc.session.clone();
        let started_at = doc.started_at.clone();
        let index = build_index(&doc.phases);
        let lock = acquire_flock(&path);
        let file = open_append(&path);
        let writer = Self {
            path,
            inner: Mutex::new(Inner { doc, index, file }),
            _lock_fd: lock,
        };
        writer.append_event(CheckpointEvent::SessionStart {
            at: now_rfc3339(),
            version: CHECKPOINT_VERSION,
            session,
            started_at,
            invocation: new_invocation,
        });
        writer
    }

    /// Declare a phase the run plans to execute. Called during
    /// pre-map for every phase; idempotent (re-declaration is a
    /// no-op so the resume path can declare the same phases the
    /// saved doc already lists).
    pub fn declare_phase(&self, identity: PhaseIdentity, skip_eligible: bool) {
        let event = {
            let mut g = self.inner.lock().unwrap();
            let key = identity_key(&identity);
            if g.index.contains_key(&key) {
                return;
            }
            let entry = PhaseEntry {
                identity: identity.clone(),
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
            CheckpointEvent::PhaseDeclared {
                at: now_rfc3339(),
                identity,
                skip_eligible,
            }
        };
        self.append_event(event);
    }

    /// Mark a declared phase as `Running`.
    pub fn phase_started(&self, identity: &PhaseIdentity) {
        let updated = self.with_entry(identity, |e| {
            e.status = PhaseStatus::Running;
            e.error = None;
        });
        if updated {
            self.append_event(CheckpointEvent::PhaseStarted {
                at: now_rfc3339(),
                identity: identity.clone(),
            });
        }
    }

    /// Mark a declared phase as `Completed` with the given
    /// wall-clock duration.
    pub fn phase_completed(&self, identity: &PhaseIdentity, duration_secs: f64) {
        let final_counts = {
            let mut g = self.inner.lock().unwrap();
            let key = identity_key(identity);
            if let Some(&idx) = g.index.get(&key) {
                let entry = &mut g.doc.phases[idx];
                let counts = entry.op_counts.clone().unwrap_or_default();
                entry.status = PhaseStatus::Completed;
                entry.duration_secs = Some(duration_secs);
                // Pin the final counts on the in-memory mirror
                // so a subsequent `snapshot()` matches what the
                // event records — and what the reader-fold will
                // produce. Leaving it `None` would diverge from
                // the disk view.
                entry.op_counts = Some(counts.clone());
                entry.cursor_state = None;
                entry.error = None;
                counts
            } else {
                return;
            }
        };
        self.append_event(CheckpointEvent::PhaseCompleted {
            at: now_rfc3339(),
            identity: identity.clone(),
            duration_secs,
            op_counts: final_counts,
        });
    }

    /// Mark a declared phase as `Failed`. The error message is
    /// preserved for resume diagnostics.
    pub fn phase_failed(&self, identity: &PhaseIdentity, error: &str) {
        let counts = {
            let err_owned = error.to_string();
            let mut g = self.inner.lock().unwrap();
            let key = identity_key(identity);
            if let Some(&idx) = g.index.get(&key) {
                let entry = &mut g.doc.phases[idx];
                entry.status = PhaseStatus::Failed;
                entry.error = Some(err_owned);
                entry.cursor_state = None;
                entry.op_counts.clone()
            } else {
                return;
            }
        };
        self.append_event(CheckpointEvent::PhaseFailed {
            at: now_rfc3339(),
            identity: identity.clone(),
            error: error.to_string(),
            op_counts: counts,
        });
    }

    /// Record op-execution counts from the live activity. Called
    /// from the metrics tick callback for the currently-running
    /// phase. Folds into the matching `PhaseEntry` and emits one
    /// `phase_progress` event per call — the reader keeps only
    /// the most recent per identity when folding.
    pub fn update_op_counts(&self, identity: &PhaseIdentity, counts: OpCounts) {
        let cursor_state = {
            let mut g = self.inner.lock().unwrap();
            let key = identity_key(identity);
            if let Some(&idx) = g.index.get(&key) {
                let entry = &mut g.doc.phases[idx];
                entry.op_counts = Some(counts.clone());
                entry.cursor_state.clone()
            } else {
                return;
            }
        };
        self.append_event(CheckpointEvent::PhaseProgress {
            at: now_rfc3339(),
            identity: identity.clone(),
            op_counts: counts,
            cursor_state,
        });
    }

    /// Set the program-canonical hash on a declared phase.
    pub fn update_phase_hash(&self, identity: &PhaseIdentity, hash: [u8; 32]) {
        let updated = self.with_entry(identity, |e| {
            e.identity.phase_hash = Some(hash);
        });
        if updated {
            self.append_event(CheckpointEvent::PhaseHash {
                at: now_rfc3339(),
                identity: identity.clone(),
                hash_hex: hash_to_hex(&hash),
            });
        }
    }

    /// Record the latest cursor-state snapshot for a Tier 2
    /// phase.
    pub fn update_cursor(
        &self,
        identity: &PhaseIdentity,
        cursor_state: serde_json::Value,
    ) {
        let counts = {
            let mut g = self.inner.lock().unwrap();
            let key = identity_key(identity);
            if let Some(&idx) = g.index.get(&key) {
                let entry = &mut g.doc.phases[idx];
                entry.cursor_state = Some(cursor_state.clone());
                entry.op_counts.clone().unwrap_or_default()
            } else {
                return;
            }
        };
        self.append_event(CheckpointEvent::PhaseProgress {
            at: now_rfc3339(),
            identity: identity.clone(),
            op_counts: counts,
            cursor_state: Some(cursor_state),
        });
    }

    /// Emit a `scope_enter` event marking entry into a
    /// `for_each` / `for_combinations` / `do_while` / `do_until`
    /// iteration. Per SRD-44a §"Event taxonomy", `coords` is the
    /// `{var: value}` map for THIS scope's own bindings and
    /// `path` is the leaf-first chain of enclosing scopes' coords
    /// — together they pin the executor's position in the
    /// scenario tree at iteration time. Scope events are
    /// write-and-go: the in-memory mirror has no slot to fold
    /// them into (the reader's fold is also a no-op today), so
    /// this is one direct `append_event`.
    pub fn emit_scope_enter(
        &self,
        kind: &str,
        coords: BTreeMap<String, serde_json::Value>,
        path: Vec<BTreeMap<String, serde_json::Value>>,
    ) {
        self.append_event(CheckpointEvent::ScopeEnter {
            at: now_rfc3339(),
            kind: kind.to_string(),
            coords,
            path,
        });
    }

    /// Emit a `scope_exit` event marking the end of one
    /// iteration. `outcome` is `"completed"` when the iteration's
    /// terminal action returned `Ok`, `"interrupted"` when it
    /// returned an error or the executor unwound through a stop
    /// signal. Same write-and-go shape as
    /// [`emit_scope_enter`](Self::emit_scope_enter).
    pub fn emit_scope_exit(
        &self,
        kind: &str,
        coords: BTreeMap<String, serde_json::Value>,
        path: Vec<BTreeMap<String, serde_json::Value>>,
        outcome: &str,
    ) {
        self.append_event(CheckpointEvent::ScopeExit {
            at: now_rfc3339(),
            kind: kind.to_string(),
            coords,
            path,
            outcome: outcome.to_string(),
        });
    }

    /// Force a `fdatasync(2)` on the underlying log. Per SRD-44a
    /// §"Writer behaviour", lifecycle records (start / completed
    /// / failed) deserve a per-event sync; the periodic
    /// progress tick can batch. The runtime calls this at every
    /// phase-lifecycle boundary so a crash between ticks loses
    /// at most one tick's worth of progress.
    pub fn flush(&self) -> Result<(), String> {
        let g = self.inner.lock().unwrap();
        match g.file.sync_data() {
            Ok(()) => Ok(()),
            Err(e) => Err(format!("fdatasync {}: {e}", self.path.display())),
        }
    }

    /// Read-only snapshot of the current in-memory document.
    /// Useful for diagnostics and tests.
    pub fn snapshot(&self) -> Checkpoint {
        self.inner.lock().unwrap().doc.clone()
    }

    /// If the workload has incomplete phases declared
    /// `checkpoint: idempotent`, return a multi-line hint string
    /// the runtime can show the operator on exit.
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

    /// Append one event line to the log. Mutates the file, but
    /// not the in-memory mirror — the caller is responsible for
    /// updating the mirror first (so a reader-fold and the
    /// in-memory snapshot stay equivalent).
    fn append_event(&self, event: CheckpointEvent) {
        let mut g = self.inner.lock().unwrap();
        let mut line = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(e) => {
                // Serialisation failure is a programming bug —
                // log loudly and drop the event rather than
                // panicking the whole session.
                eprintln!(
                    "checkpoint: serialise event failed: {e}; dropping record",
                );
                return;
            }
        };
        line.push('\n');
        if let Err(e) = g.file.write_all(line.as_bytes()) {
            eprintln!(
                "checkpoint: append to {}: {e}", self.path.display(),
            );
        }
    }

    /// Apply a mutation closure to the entry matching `identity`,
    /// returning `true` when the entry was found and updated. The
    /// caller emits the event after a successful mutation so the
    /// in-memory mirror and the on-disk log stay in lock-step.
    fn with_entry<F: FnOnce(&mut PhaseEntry)>(&self, identity: &PhaseIdentity, f: F) -> bool {
        let mut g = self.inner.lock().unwrap();
        let key = identity_key(identity);
        if let Some(&idx) = g.index.get(&key) {
            f(&mut g.doc.phases[idx]);
            true
        } else {
            false
        }
    }
}

/// Open `path` in append mode, creating it (and the parent
/// directory) if needed. Returns the file or panics — the
/// writer can't function without it, so a hard failure here is
/// the correct response. Production callers wrap construction
/// in a fallible bootstrap path; the panic surfaces as a
/// session-startup error rather than a silent skip.
fn open_append(path: &std::path::Path) -> File {
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            panic!(
                "checkpoint: create parent dir {} for {}: {e}",
                parent.display(),
                path.display(),
            );
        }
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| {
            panic!(
                "checkpoint: open append {} failed: {e}",
                path.display(),
            )
        })
}

/// Build a lookup key for the index map. Identity equality is
/// `(yaml_path, coords)` per SRD-44; the hash is sufficiency,
/// not identity, so it's deliberately excluded from the key.
pub(crate) fn identity_key(identity: &PhaseIdentity) -> String {
    let path_json = serde_json::to_string(&identity.yaml_path)
        .unwrap_or_else(|_| String::new());
    format!("{path_json}\x1f{}", identity.coords)
}

/// Take a non-blocking exclusive advisory lock on a sibling
/// `checkpoint.lock` file alongside the checkpoint document.
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
        let path = dir.join("checkpoint.jsonl");
        let w = CheckpointWriter::new(
            path.clone(), "sess".into(), "2026-01-01T00:00:00Z".into(), 1,
        );
        let id = ident("schema", "");
        w.declare_phase(id.clone(), true);
        w.phase_started(&id);
        w.phase_completed(&id, 1.5);
        w.flush().expect("flush");

        // Verify in-memory mirror reflects the lifecycle.
        let snap = w.snapshot();
        assert_eq!(snap.phases.len(), 1);
        assert_eq!(snap.phases[0].status, PhaseStatus::Completed);
        assert_eq!(snap.phases[0].duration_secs, Some(1.5));

        // Verify the on-disk log carries each event line in order.
        let raw = std::fs::read_to_string(&path).expect("read log");
        let lines: Vec<&str> = raw.lines().collect();
        assert_eq!(lines.len(), 4,
            "expected session_start, phase_declared, phase_started, phase_completed");
        assert!(lines[0].contains("\"type\":\"session_start\""));
        assert!(lines[1].contains("\"type\":\"phase_declared\""));
        assert!(lines[2].contains("\"type\":\"phase_started\""));
        assert!(lines[3].contains("\"type\":\"phase_completed\""));
    }

    #[test]
    fn redundant_declare_is_idempotent() {
        let dir = tempdir();
        let path = dir.join("c.jsonl");
        let w = CheckpointWriter::new(
            path.clone(), "s".into(), "t".into(), 1,
        );
        let id = ident("p", "(k=1)");
        w.declare_phase(id.clone(), true);
        w.declare_phase(id.clone(), false); // re-declare ignored
        let snap = w.snapshot();
        assert_eq!(snap.phases.len(), 1);
        assert!(snap.phases[0].skip_eligible, "first declare wins");

        // Only one phase_declared in the log (plus the leading
        // session_start).
        let raw = std::fs::read_to_string(&path).expect("read");
        let count = raw.lines()
            .filter(|l| l.contains("\"type\":\"phase_declared\""))
            .count();
        assert_eq!(count, 1, "second declare must not emit a duplicate event");
    }

    #[test]
    fn from_existing_emits_fresh_session_start() {
        let dir = tempdir();
        let path = dir.join("c.jsonl");
        // First session writes some events, then drops to release
        // the flock. Mirrors the production lifecycle: prior
        // process exits before resume starts.
        let saved = {
            let w = CheckpointWriter::new(
                path.clone(), "s".into(), "2026-01-01T00:00:00Z".into(), 1,
            );
            let id = ident("schema", "");
            w.declare_phase(id.clone(), true);
            w.phase_completed(&id, 0.5);
            w.flush().expect("flush");
            w.snapshot()
        };
        let w2 = CheckpointWriter::from_existing(
            path.clone(), saved, "2026-01-01T00:01:00Z".into(), 2,
        );
        let snap = w2.snapshot();
        assert_eq!(snap.invocation, 2);
        assert_eq!(snap.phases.len(), 1);
        assert_eq!(snap.phases[0].status, PhaseStatus::Completed);

        // Log carries TWO session_start events, marking the
        // invocation boundary.
        let raw = std::fs::read_to_string(&path).expect("read");
        let count = raw.lines()
            .filter(|l| l.contains("\"type\":\"session_start\""))
            .count();
        assert_eq!(count, 2, "resume must append a fresh session_start, not rewrite");
    }

    #[test]
    fn scope_enter_exit_pairs_for_two_deep_for_each() {
        // SRD-44a Push 3: drive the writer over the event
        // sequence the executor's scope walker produces for a
        // 2-deep `for_each` workload — outer iterates `x in
        // [1, 2]`, inner iterates `y in ["a", "b"]`. Two
        // outer iterations × two inner iterations = four leaf
        // bracket pairs, plus the two outer-loop bracket pairs
        // wrapping each inner sub-walk.
        let dir = tempdir();
        let path = dir.join("c.jsonl");
        let w = CheckpointWriter::new(
            path.clone(), "s".into(), "2026-01-01T00:00:00Z".into(), 1,
        );

        // Scope coords as the executor would synthesize per
        // iteration. `coord_path` is root-first; the writer's
        // event takes the leaf as `coords` and the prefix
        // (reversed to leaf-first) as `path`. We model that
        // shape here directly.
        let outer_coord = |xv: u64| -> BTreeMap<String, serde_json::Value> {
            let mut m = BTreeMap::new();
            m.insert("x".into(), serde_json::Value::from(xv));
            m
        };
        let inner_coord = |yv: &str| -> BTreeMap<String, serde_json::Value> {
            let mut m = BTreeMap::new();
            m.insert("y".into(), serde_json::Value::from(yv));
            m
        };

        for x in [1u64, 2u64] {
            // Outer enter: coords={x=…}, path=[]
            w.emit_scope_enter("for_each", outer_coord(x), Vec::new());
            for y in ["a", "b"] {
                // Inner enter: coords={y=…}, path=[{x=…}]
                w.emit_scope_enter(
                    "for_each",
                    inner_coord(y),
                    vec![outer_coord(x)],
                );
                w.emit_scope_exit(
                    "for_each",
                    inner_coord(y),
                    vec![outer_coord(x)],
                    "completed",
                );
            }
            w.emit_scope_exit("for_each", outer_coord(x), Vec::new(), "completed");
        }
        w.flush().expect("flush");

        // Parse the JSONL log line-by-line, dropping the
        // leading session_start. Confirm the bracket sequence
        // is well-formed and the path/coords carry the
        // expected x/y values for each iteration.
        let raw = std::fs::read_to_string(&path).expect("read log");
        let scope_events: Vec<serde_json::Value> = raw.lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("parse line"))
            .filter(|v| {
                let t = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                t == "scope_enter" || t == "scope_exit"
            })
            .collect();
        // 2 outer (enter+exit) + 2*2 inner (enter+exit) = 12 events.
        assert_eq!(scope_events.len(), 12, "expected 12 scope events, got {}", scope_events.len());

        let kind = |v: &serde_json::Value| v.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();
        let x_at = |v: &serde_json::Value, idx: &str| -> Option<u64> {
            v.pointer(idx).and_then(|n| n.as_u64())
        };
        let y_at = |v: &serde_json::Value, idx: &str| -> Option<String> {
            v.pointer(idx).and_then(|n| n.as_str()).map(|s| s.to_string())
        };

        // Outer entry first: coords={x=1}, path=[].
        assert_eq!(kind(&scope_events[0]), "scope_enter");
        assert_eq!(x_at(&scope_events[0], "/coords/x"), Some(1));
        assert!(scope_events[0].pointer("/path").and_then(|p| p.as_array()).map(|a| a.is_empty()).unwrap_or(false),
            "outer enter must have empty path");

        // First inner enter under x=1, y=a. path leaf-first is
        // [{x=1}].
        assert_eq!(kind(&scope_events[1]), "scope_enter");
        assert_eq!(y_at(&scope_events[1], "/coords/y"), Some("a".to_string()));
        assert_eq!(x_at(&scope_events[1], "/path/0/x"), Some(1));

        // First inner exit, completed.
        assert_eq!(kind(&scope_events[2]), "scope_exit");
        assert_eq!(scope_events[2].pointer("/outcome").and_then(|s| s.as_str()), Some("completed"));

        // Second inner under x=1, y=b.
        assert_eq!(y_at(&scope_events[3], "/coords/y"), Some("b".to_string()));
        assert_eq!(kind(&scope_events[4]), "scope_exit");

        // Outer exit for x=1.
        assert_eq!(kind(&scope_events[5]), "scope_exit");
        assert_eq!(x_at(&scope_events[5], "/coords/x"), Some(1));
        assert_eq!(scope_events[5].pointer("/outcome").and_then(|s| s.as_str()), Some("completed"));

        // Second outer (x=2) entry, plus its inner pairs and exit.
        assert_eq!(kind(&scope_events[6]), "scope_enter");
        assert_eq!(x_at(&scope_events[6], "/coords/x"), Some(2));
        assert_eq!(x_at(&scope_events[7], "/path/0/x"), Some(2));
        assert_eq!(y_at(&scope_events[7], "/coords/y"), Some("a".to_string()));
        assert_eq!(kind(&scope_events[11]), "scope_exit");
        assert_eq!(x_at(&scope_events[11], "/coords/x"), Some(2));

        // The reader's fold treats scope events as no-ops in v1
        // (no scope mirror in the in-memory document), so a
        // round-trip read should succeed and leave the phases
        // list untouched.
        let folded = super::super::storage::read(&path)
            .expect("read folds")
            .expect("non-empty");
        assert!(folded.phases.is_empty(), "no phases declared, fold should be empty");
    }

    #[test]
    fn scope_exit_outcome_distinguishes_interrupted_from_completed() {
        // The kind/outcome surface is what distinguishes a clean
        // bracket from one closed by a stop signal. Verify both
        // outcomes round-trip through the JSONL.
        let dir = tempdir();
        let path = dir.join("c.jsonl");
        let w = CheckpointWriter::new(
            path.clone(), "s".into(), "t".into(), 1,
        );
        let mut coords = BTreeMap::new();
        coords.insert("k".into(), serde_json::Value::from(7u64));
        w.emit_scope_enter("do_while", coords.clone(), Vec::new());
        w.emit_scope_exit("do_while", coords, Vec::new(), "interrupted");
        w.flush().expect("flush");

        let raw = std::fs::read_to_string(&path).expect("read");
        let exit_line = raw.lines()
            .find(|l| l.contains("\"type\":\"scope_exit\""))
            .expect("scope_exit line");
        let ev: serde_json::Value = serde_json::from_str(exit_line).unwrap();
        assert_eq!(ev.pointer("/kind").and_then(|s| s.as_str()), Some("do_while"));
        assert_eq!(ev.pointer("/outcome").and_then(|s| s.as_str()), Some("interrupted"));
    }

    #[test]
    fn flock_blocks_concurrent_writer_on_same_path() {
        let dir = tempdir();
        let path = dir.join("c.jsonl");
        let _w = CheckpointWriter::new(
            path.clone(), "s".into(), "t".into(), 1,
        );
        let result = std::panic::catch_unwind(|| {
            let _w2 = CheckpointWriter::new(
                path.clone(), "s".into(), "t".into(), 1,
            );
        });
        assert!(result.is_err(), "second writer should panic on flock contention");
    }
}
