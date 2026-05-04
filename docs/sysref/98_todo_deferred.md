# SRD-98 — Deferred work

**Status:** living TODO list
**Owner:** project-wide
**Cross-refs:** the SRD each item attaches to (named in the
  item heading)

---

## Purpose

Features whose design has been settled (or is sufficiently
sketched) but which are not on the active implementation
queue. Each entry names:

- the SRD it slots into,
- the rough shape of the work,
- the constraint that lets us defer it without breaking
  current behavior,
- and the trigger that should reactivate the work.

Don't park ambiguous design questions here — those go in a
design memo until decided. Items here are concrete tasks
waiting for capacity.

---

## Tier 2 checkpointing — opaque cursor-state snapshot
(SRD-44)

**Shape.** The current Tier 1 implementation skips wholly-
completed phases on resume by matching `(yaml_path, gk_hash,
op_hash)` identity. Tier 2 adds resumption *inside* a phase:
each cycle source captures an opaque cursor token at
checkpoint-flush time, the writer persists it alongside the
phase identity, and resume restores the cursor so the phase
continues from where it left off rather than restarting.

**What it depends on.** A `CursorSource` trait that exposes
`snapshot()` / `restore(opaque_blob)` on every cycle source
the runtime supports (sequential, hashed, GK-driven,
do-loop). For sources without a stable "where am I" notion,
the trait reports `Snapshot::Unsupported` and resume falls
back to Tier 1 behavior (full re-run).

**Why deferred.** Tier 1 covers the common operator workflow
(crash → fix → resume from the next *phase*). Per-cycle
resume helps on long phases but the engineering surface
includes per-source serialization, cross-process trim of the
metrics db past the cursor point, and a contract for what
"the same cursor in a different run" means. None of that is
load-bearing for current users.

**Reactivation trigger.** Operator hits a long phase that
takes hours to re-run after a transient fault, *and* the
phase's cycle source has a meaningful per-cycle resume
point.

---

## `verify:` op runtime support check
(SRD-44 §"Verify op")

**Shape.** Before honoring a saved `Completed` phase status
on resume, the planner runs the workload's `verify:` op
against the prior session's data. If verify fails, the
phase is reclassified `Incomplete` and re-runs. If verify
passes, the skip stands.

**What it depends on.** A standardized `verify:` op
contract — at minimum: a deterministic op declaration that
returns ok/error against the metrics db or the upstream
data store. Currently `verify` exists as an op kind but
isn't wired to the resume planner.

**Why deferred.** The skip-by-identity rule (yaml_path +
gk_hash + op_hash) is sound when the workload and runtime
haven't changed between runs. Adding verify is belt-and-
suspenders for cases where the *underlying data* changed
between runs (which the identity check can't detect). Most
operators don't have this concern; the feature is a
correctness-paranoia tier.

**Reactivation trigger.** A user reports a resume that
silently kept stale data — or a workload class emerges
where the upstream store can drift between runs.

---

## Do-loop checkpointing
(SRD-44 §"Resume protocol")

**Shape.** Today, declaring `checkpoint: idempotent` on a
phase that lives inside a `do_while` / `do_until` loop is
rejected at workload-load time (SRD-44 + task #165). The
deferred extension snapshots the loop's interior coordinates
(iteration counter, accumulator) and resumes from the next
unfinished iteration.

**What it depends on.** A loop-coordinate cursor type that
extends Tier 2's `CursorSource` to multi-axis state, plus a
contract for "what iteration is `Completed`" — a phase
inside a loop has many incarnations, each of which needs its
own status entry.

**Why deferred.** The current behavior — reject the
combination at load time — is safe and explicit. Operators
who need a do-loop'd phase to skip on resume can wrap the
loop with explicit identity. Implementing this needs both
Tier 2's cursor machinery and a richer phase-identity
schema; no single user request is blocked on it today.

**Reactivation trigger.** A user actively wants resumable
loops and the Tier 2 cursor work has landed.

---

## How items leave this list

- A user reports a need that the deferred feature would
  solve → reactivate, file a fresh task on the queue, link
  back here.
- A design discussion changes the shape of the feature →
  update the entry here, don't shadow-implement an old
  design.
- The feature lands → delete the entry from this file and
  add a sentence to the parent SRD pointing at the
  implementation file.

This file should never grow indefinitely. If something has
been here for a year without movement, ask whether the
shape we sketched is still right or whether the item should
be retired entirely.
