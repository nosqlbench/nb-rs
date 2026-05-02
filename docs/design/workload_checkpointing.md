# Workload Checkpointing — Design Memo

> **Status: superseded by [SRD 44](../sysref/44_workload_checkpointing.md).**
> This memo is the working draft that fed the SRD. Kept as a
> historical record of how the design evolved (the open
> questions and the discussion that resolved them); the
> authoritative spec is the SRD.

## Motivation

Two workload shapes hit the same problem:

1. **Many phases.** A scenario produced by deep nesting
   (`for_each profile × for_combinations [table, optimize_for]
   × for_combinations [k, limit]`) can pre-map to hundreds of
   phases. The full_cql_vector workload at sift1m / 12 profiles
   pre-maps to **546 phases**. A failure at phase 437 currently
   means re-running 1..436 from scratch.

2. **Long phases.** A single rampup phase loading 82,915 cycles
   at concurrency 100 may run for 30+ minutes. A failure at
   cycle 70k is currently total loss.

A checkpointing feature would let an interrupted run **resume
from the last durable boundary** — phase boundary at minimum,
mid-phase cycle boundary as a stretch goal — without redoing
work that's already done.

## What's already in place

The runtime already has the ingredients:

- **Pre-mapped scene tree** (`nbrs-activity::scene_tree::SceneTree`)
  with stable per-phase sequence numbers (`SceneNode::seq`,
  1-based DFS order) and a per-phase status lifecycle
  (`Pending → Running → Completed | Failed`).
- **Metrics database** (`logs/<session>/metrics.db`): SQLite,
  WAL mode, written every 30s. Phase-level totals + cadence
  reports already persist.
- **Cursor sources** (`nbrs-variates::source::DataSourceFactory`)
  with `global_extent()` known up-front. The runtime tracks
  `ops_started` / `ops_finished` per phase.
- **Workload param + scenario hashes** are derivable — both come
  from the parsed YAML model.

The missing piece: a **session-recovery file** that captures
which phases are done, at what offsets, and tied to a
verifiable workload identity, plus a runner path that consumes
it on startup.

## Granularity tiers

Two checkpointing tiers, both useful, both implementable
independently. Phase-boundary first; mid-phase second.

### Tier 1 — Phase boundary (simple)

Save state every time a phase transitions to `Completed` or
`Failed`. Resume skips every phase whose `seq` is already
marked done; the scenario walker no-ops them.

**State per phase**: `seq`, `name`, `status`, `duration_secs`,
`labels` (the canonical scope-coordinate path), `op_counts`
(total cycles, error count). Roughly the existing
`SceneNode` payload.

**State at session level**: workload-identity tuple (see
"Validation contract" below), session start time, last
checkpoint timestamp.

**Resume**: load the file, intersect against the freshly
pre-mapped tree, flip every matching phase to its saved
terminal state, run the rest.

For the "many phases" use case (546-phase workload, fail at
387), Tier 1 alone recovers ~70% of the lost work. The
remaining loss is whatever was running mid-flight at the
moment of failure.

### Tier 2 — Mid-phase cycle boundary (stretch)

Save the cursor-source's high-water mark periodically inside a
running phase. Resume picks up at `cycle = saved_high_water + 1`.

**Hard parts**:

- Coordinating the high-water across a fiber pool. Currently
  fibers pull cycles via a shared atomic counter; the
  checkpointer needs a stable read that's "every cycle <
  HW has finished". `ops_finished` is monotonic but only
  describes successes; failures complicate.
- Capturing per-fiber GK state. The op pipeline's
  `ResolvedPulls` are per-cycle, so they don't need persisting,
  but **`shared`-cell mutations** between fibers (rare) would
  need to be serialised.
- Source-factory fast-forward semantics. `RangeSourceFactory`
  is trivial to seek; non-cursor sources (`do_while`,
  `do_until`) have stateful conditions that don't fast-forward
  cleanly.

**Pragmatic scope**: only `RangeSource`-driven phases support
mid-phase resume. `do_while` / `do_until` resume at phase
boundary only. The cursor name + extent + saved HW is what
gets persisted.

**State**: extends Tier 1's per-phase block with
`cursor_name`, `cursor_high_water`, `cursor_extent`,
`completed_cycles`, `error_count_so_far`.

## Storage format

`logs/<session>/checkpoint.json` plus an atomic-rename
companion at `logs/latest/checkpoint.json`:

```json
{
  "version": 1,
  "session": "fulltest_20260502_104237",
  "workload": {
    "path": "adapters/cql/workloads/full_cql_vector.yaml",
    "yaml_sha256": "ab12…",
    "params_sha256": "f9c0…",
    "scenario": "fulltest"
  },
  "started_at": "2026-05-02T10:42:37Z",
  "checkpoint_at": "2026-05-02T11:08:14Z",
  "total_phases": 546,
  "phases": [
    { "seq": 1, "name": "teardown", "status": "completed",
      "duration_secs": 0.69, "labels": "(table=…), (profile=…)",
      "op_counts": { "started": 3, "finished": 3, "errors": 0 } },
    { "seq": 2, "name": "schema",   "status": "completed", … },
    …
    { "seq": 387, "name": "ann_query", "status": "running",
      "labels": "(k=10, limit=20), (table=…), (profile=…)",
      "cursor": { "name": "q", "extent": 82500, "high_water": 47200 },
      "op_counts": { "started": 47200, "finished": 47200, "errors": 0 } }
  ]
}
```

JSON for legibility (operator can inspect/edit). Atomic rename
on every flush. Flush cadence: phase-boundary always, plus
every 30s (or every N completed cycles for very fast phases).
Same SQLite-write tick the metrics writer already uses — share
the timer.

The metrics db (`metrics.db`) is the system of record for
performance numbers; the checkpoint file is the system of
record for **plan progress**. They're independent: a corrupt
checkpoint never corrupts metrics, and vice versa.

## Validation contract

A checkpoint records the workload it came from. A resume
must verify the workload-identity tuple matches before honouring
any saved progress:

- **`yaml_sha256`**: hash of the workload file's textual
  bytes. Any edit invalidates the checkpoint.
- **`params_sha256`**: hash of the resolved params (CLI +
  workload `params:` block, sorted, key=value joined). Catches
  `dataset=sift1m` → `dataset=sift10m`-style mismatches.
- **`scenario`**: the scenario name. Different scenarios →
  different plans → no resume.
- **Total phase count**: `pre_map_tree(...).total_phases()`
  must equal the saved `total_phases`. A mismatch means the
  scenario tree's shape changed (e.g. a `for_each` extent
  shifted because a workload-param was retuned) — refuse to
  resume.

**On mismatch**: `nbrs run … --resume` exits with a
diagnostic showing which field diverged. The user fixes it
(reverts the YAML edit, or starts a new session). No partial
resume — the contract is "exact replay or fresh".

**On match**: resume proceeds.

## Resume protocol

Two surfaces:

1. **Explicit**: `nbrs run … --resume <session-id>` —
   resume the named session. Reads `logs/<session-id>/checkpoint.json`,
   validates, reconciles against fresh pre-map, executes only
   non-terminal phases.
2. **Auto-detect**: `nbrs run …` (no flag) — if `logs/latest/`
   has a checkpoint AND its workload-identity matches AND it
   contains non-terminal phases, prompt: `Resume session
   fulltest_20260502_104237 (387/546 complete)? [Y/n]`. With
   `--no-prompt` (CI), default `n` (always start fresh). With
   `--resume-latest`, default `y`.

A resumed run **continues into the same `logs/<session>/` dir**
— the metrics db gets appended, session.log gets appended, the
checkpoint file gets updated. The session ID does not change.
This means a single session can span many `nbrs run` invocations
across many machine reboots.

## Eligibility — `checkpoint:` per-phase declaration

A phase **must declare itself eligible** for skip-on-resume.
Without an explicit `checkpoint:` setting, the resume path
re-runs the phase from scratch even if the checkpoint file
says it completed last time. Skip is opt-in, not the default;
this keeps non-idempotent phases (data loaders without unique-
key guards, schema mutations, side-effecting fixtures) safe by
construction.

### Surface

```yaml
phases:
  rampup:
    checkpoint: idempotent      # skip if previously completed
    ops: { … }

  schema:
    checkpoint:                 # full form
      idempotent: true
      hashed: true              # also the default for any set form
      verify:
        # …  see "Verify sub-property" below

  destructive_setup:
    checkpoint: none            # never skip; equivalent to no `checkpoint:` at all

  measurement_phase:
    # no checkpoint declared → not eligible to skip on resume
    ops: { … }
```

### Three forms

1. **Short string** — `checkpoint: idempotent`. Equivalent to
   `checkpoint: { idempotent: true, hashed: true }`. Default
   "I claim this phase is safe to skip if completed".

2. **Disabled** — `checkpoint: none` (also `false`, `no`).
   Equivalent to no `checkpoint:` declaration at all. Documents
   the intent ("considered, decided no") for reviewers.

3. **Full form** — a mapping with sub-properties (see below).
   Used when the phase is eligible to skip but resume needs
   more than "trust the checkpoint file": e.g. a runtime
   verify, a non-default hashing policy.

### Sub-properties

| Key | Default | Meaning |
|-----|---------|---------|
| `idempotent` | `true` (when the map is set) | Marks this phase as skip-eligible. Setting it to `false` inside the map is the same as `checkpoint: none`. |
| `hashed` | `true` | When true, the phase's YAML definition is canonically serialised + hashed; the hash lives in the checkpoint identity tuple. Resume refuses to skip a phase whose hash differs from the saved one — protects against silent edits to the phase body. |
| `verify` | unset | (open question — see below) An op-shaped block that resume runs to confirm "this phase actually is complete in the live system" before honouring the saved status. Same shape as the SRD-03 fixture-verification ops. |

If `verify:` is unset, resume trusts the saved status alone.
With `verify:`, resume runs the verify op first; only on
success does the phase skip. Verify failure causes the phase
to re-run from scratch (or refuse-resume, depending on
verify's own `error_policy:` — composable with SRD-32).

## Phase identity — the checkpoint key

Every checkpoint entry is keyed by an identity tuple, not by
display labels:

```rust
struct PhaseIdentity {
    /// Fully-qualified location in the workload YAML — the
    /// path from the workload root through every nested
    /// scenario / for_each / for_combinations clause down to
    /// the phase declaration. Independent of iteration
    /// values; stable across runs.
    yaml_path: Vec<PathSegment>,
    /// Scope-coordinate set that distinguishes this iteration
    /// instance from its siblings. Same shape `phase_labels`
    /// uses today, but typed (Vec<ScopeCoord>) not stringy.
    coords: Vec<ScopeCoord>,
    /// SHA-256 of the phase's canonical YAML body, when
    /// `hashed: true` is in effect. Resume cross-checks this
    /// against the freshly-parsed workload's phase body —
    /// any drift in the phase definition (op fields, bindings,
    /// timeout, error_policy, …) invalidates the saved status.
    phase_hash: Option<[u8; 32]>,
}
```

### Path segments

```rust
enum PathSegment {
    Scenario(String),          // "fulltest"
    ScenarioInclude(String),   // sub-scenario inclusion
    ForEach { var: String },   // "for: profile in …"
    ForCombinations { vars: Vec<String> },  // "for: table in …, optimize_for in …"
    DoWhile { counter: Option<String> },
    DoUntil { counter: Option<String> },
    Phase(String),             // "ann_query"
}
```

The path captures the *structural* nesting; the iteration
values come from `coords`. Together they're a fully-qualified
identifier that's stable across re-runs of the same workload
even when iteration extents change.

### Hash semantics

`hashed: true` (the default for any set `checkpoint:` block)
means: serialise the phase's YAML mapping in canonical form
(sorted keys, normalised whitespace, no comments) and SHA-256
the bytes. Compare on resume.

Mismatch → the saved status is dropped for this phase; it
re-runs as if no checkpoint existed. The rest of the resume
plan is unaffected — only this phase loses its skip
eligibility.

Hashing is per-phase, not whole-workload. This is intentional:
editing one phase's `cycles:` from 1000 to 2000 should
invalidate that phase's checkpoint without throwing away
progress on the 545 other phases. The whole-workload identity
hash documented earlier (`yaml_sha256`, `params_sha256`) is a
**coarser** check that catches structural reshuffles; the
per-phase hash is the **fine** check that catches body edits.

## Do-loop checkpointing

Do-loops (`do_while` / `do_until`) get their own `checkpoint:`
property, with the same three forms. **Without** an explicit
declaration, a do-loop is **not** snapshotted; resume restarts
the loop at iteration 0. With a declaration, the loop's
**interior do coordinates** are recorded periodically and
resume picks up at `last_recorded_iter + 1`.

```yaml
- do_while:
    condition: "{eligible_count} > 0"
    counter: pass
    checkpoint:
      idempotent: true     # iterations are safe to re-skip
      hashed: true
      record_coords: [pass]   # checkpoint only the named coords
    children:
      - process_batch
```

`record_coords:` (open question — see below) names the iter
coords to capture per checkpoint flush. Default: every coord
the loop's scope owns.

Resume semantics for do-loops:

- The loop's parent scope is rebuilt from the workload (extent,
  externs, condition expression).
- The saved coord set seeds the loop's input slots before the
  first iteration runs.
- The condition is **re-evaluated** before each iteration as
  normal; if the live state has the condition already false,
  the loop exits immediately. This handles the case where
  another process drained the queue while the run was
  interrupted.

The `shared`-cell concern from earlier: do-loops with
`shared:` mutations between iterations need explicit
serialisation if the shared state is part of the loop's
condition. **Open question**: declarative `shared:` snapshot
syntax, or punt and document do-loop-with-shared as
checkpoint-incompatible?

## Durability — serial write order

The metrics writer and the checkpoint writer share an explicit
ordering, not a shared timer:

> **The metrics flush must complete (durably) before the
> checkpoint flush begins.**

Operationally this means: at every checkpoint trigger
(phase boundary, or 30s tick, or do-loop iteration), the
runner first calls `metrics_writer.flush().await` (which
returns only after fsync), then writes the checkpoint file
(also with fsync), then renames atomically.

Crash window: between metrics fsync and checkpoint rename, a
crash leaves metrics ahead of the checkpoint. On resume the
re-run of the (apparently incomplete) phase would double-count
those last-30-seconds-of-metrics. This is acceptable: metrics
double-counting is a known cost of resume granularity, and
the operator can spot it (the resumed phase's count exceeds
the cursor extent).

The reverse window — checkpoint ahead of metrics — would
silently lose metrics on resume (the checkpoint claims
completion but the metrics db doesn't have the rows). Worse
than the forward window. The serial order rules it out.

## What stays unchecked-pointed

Not everything goes in the checkpoint:

- **Per-fiber GK state** — re-derived from the kernel + cycle
  number on every cycle. Stateless.
- **Metrics deltas / aggregates** — already in `metrics.db`
  via the existing 30s writer. The checkpoint doesn't
  duplicate them.
- **Adapter session state** (CQL prepared statements, HTTP
  connections) — re-established at adapter init on resume.
  Idempotent.
- **TUI state** — purely presentation; reconstructed from
  scene tree + metrics on resume.

The checkpoint is **plan progress + cursor positions**. Nothing
that's already durable elsewhere, nothing that's regenerable.

## Phase semantics on resume

Three categories of phase per resume:

1. **Already complete** (`status = Completed`): scene tree node
   is flipped to `Completed` with the saved `duration_secs` and
   `op_counts`. Executor's scenario walker sees it in the
   `Completed` set and **skips dispatch entirely** — the phase's
   activity is never instantiated.

2. **Already failed** (`status = Failed(reason)`): same as above
   but the reason carries forward. The resumed run **stops** at
   the same failure point unless the operator explicitly clears
   it (`--retry-failed`).

3. **Mid-flight at last checkpoint** (`status = Running`,
   cursor info present): scene tree starts as `Pending`, the
   activity dispatches normally, but `RangeSourceFactory` is
   constructed with `start = cursor_high_water + 1`. The phase
   runs only its remaining cycles. Tier 2 only.

## Interaction with the runtime

Where the wiring goes:

- **Save**: a `CheckpointWriter` actor (one per session,
  spawned at runner startup) listening for
  `PhaseCompleted` / `PhaseFailed` / 30s-timer events. Same
  shape as the metrics SQLite writer. Atomic rename on every
  flush. No locking on the hot path — the actor pulls
  snapshots from `nbrs_activity::scene_tree::current()` and
  the per-activity progress metrics.

- **Load**: a `Checkpoint::load(path) -> Result<Checkpoint, _>`
  call early in `Runner::run`, before the scene-tree pre-map.
  Validates against the freshly built pre-map and produces a
  `ResumePlan` that the executor's scenario walker consults
  during dispatch.

- **Skip**: `execute_node` adds an early-return when the
  phase's seq is in `ResumePlan::completed`. No phase activity
  is built, no observer hooks fire (or they fire as
  `phase_skipped` — see Open Questions). Scene tree state is
  pre-flipped to `Completed`.

- **Mid-phase resume** (Tier 2 only): the source factory's
  start offset comes from the resume plan. Done at activity
  construction time; the rest of the activity is unchanged.

## Settled design rules

The original open-question list resolved. The following are
load-bearing decisions for the implementation.

### Verify sub-property reuses the op-template grammar

```yaml
phases:
  await_index:
    checkpoint:
      idempotent: true
      verify:
        raw: "SELECT count(*) FROM {keyspace}.{table} WHERE built = false"
        poll: await_empty
        timeout_ms: 30000
```

`verify:` accepts any op-template body the workload's adapter
understands. It composes with the SRD-32 `ErrorPolicyDispenser`
wrapper (e.g. an explicit `error_policy: warn_log_stop` inside
the verify block) and the SRD-03 status-determination
invariant (verify failure short-circuits, never spins). GK
grammar is extended as needed if some verifier shape isn't
yet expressible — but the surface stays op-template-shaped,
not a parallel typed vocabulary.

### Do-loop coord recording is always-all

The original draft proposed a `record_coords:` whitelist on
do-loop checkpoint blocks. **Dropped.** When a do-loop is
checkpoint-eligible, every own coord its scope declares is
captured on each flush; resume restores all of them. Derived
coords (e.g. `offset := pass * batch_size` recomputed from
the pass counter) get captured redundantly — a few bytes of
storage overhead per flush, no correctness implication. The
captured-value case (a child phase writing a result back into
the loop's scope via `shared`) is handled separately under
the `shared`-cell rule below, not via a coord whitelist.

### `checkpoint: none` records but marks not-skip-eligible

Phases with `checkpoint: none` (or no `checkpoint:` declaration
at all) still get checkpoint-file entries on completion /
failure — for ETA computation, post-run summary continuity,
and sequence-number preservation across resumes. The entry
carries `skip_eligible: false` so the resume planner re-runs
them regardless of saved status.

### No workload-level checkpoint default

Skip eligibility is **strictly per-phase** opt-in. There is
no `defaults: { checkpoint: ... }` block. Every phase that
wants to be skip-eligible declares `checkpoint:` explicitly,
next to its op body where a reader can see the side effects
the claim covers. The `summary:`, `concurrency:` workload-
level defaults are presentational / sizing knobs and don't
change meaning if forgotten; checkpoint eligibility is a
safety claim and must not be inheritable.

### `shared` + `do_while` + `checkpoint` is rejected

A do-loop whose condition transitively reads any `shared`
cell value cannot declare `checkpoint:`. The pre-map / scope-
build pass detects the combination at workload validation
time and refuses to compile.

Why: the condition's value depends on cross-iteration mutable
state in a Mutex-backed cell. Restoring only the iter
coords on resume re-initialises the shared cell from its
binding default, and the loop runs from a stale starting
condition — silent over-execution.

Workaround for operators who hit this: refactor the loop's
condition to read a pure iter coord (a counter, a bound from
a captured value) and reserve `shared` cells for measurement-
only state (which is regenerated naturally on resume from the
metrics writer's checkpoint). Reconsider declarative shared-
cell snapshotting if a real workload needs it.

### Error handling is invocation-agnostic — load-bearing rule

> **The same `errors:` syntax with the same semantics governs
> error handling on first run and on every resumed run. There
> is no separate "resume error handling" vocabulary, no
> resume-specific code path, no special-cased branch in the
> wrapper stack. Resume is not a different execution mode —
> it is a fresh invocation of the same workload that happens
> to start with some phases pre-flagged as already complete.**

This rule constrains the implementation:

1. **Same wrapper stack.** The phase-execution wrapper stack
   built at first-run startup (per SRD-32 §"Phase-execution
   wrappers") is built identically at resume startup. Same
   `errors:` rules, same cascade, same retry budgets, same
   `ErrorRouterDispenser` instances around op stacks. The
   `ResumeSkipPhaseExecutor` outermost wrapper short-circuits
   for already-complete phases; for everything else, the stack
   below it is the same stack first-run uses.

2. **Same per-invocation budgets.** `max_retries`, polling-op
   retry caps, `ErrorRouter` cumulative counters all reset per
   invocation. A phase whose `errors:` rule says
   `Timeout:retry,warn` gets a fresh retry budget on every
   `nbrs run … --resume` because that's what the rule says
   "what to do" *every time the op runs*, regardless of
   whether this is its first or seventh attempt across
   invocations.

3. **Reviewers can't tell which invocation they're reading.**
   Reading a workload's `errors:` block tells you exactly
   what the system does on errors, full stop. Whether it's a
   first-run, a fresh resume, or the seventeenth resume of a
   long-running session, the error-handling behaviour is
   identical. There is no "what does this do on resume?"
   sub-question worth asking.

4. **No new keys for the resume scenario.** Anything that
   *would* require a resume-specific key (`on_resume_*`,
   `failed_phase_action`, etc.) is rejected at design review.
   If a workload wants different behaviour on resume, it's
   asking for two different policies — and the answer is "use
   two different workloads, two different `errors:` blocks,
   two different invocations".

### The settled `errors:` table

| `errors:` rule for the failing error | First-run failure | Resume-encounter of failed phase | Same rule? |
|--------------------------------------|------------------|----------------------------------|-----------|
| `.*:warn,stop` | Phase fails, run stops | Phase re-runs, first failure stops the run again | yes |
| `Timeout:retry,warn;.*:stop` | Phase retries timeouts, stops on others | Phase re-runs from scratch, fresh retry budget, same per-error classification | yes |
| `.*:ignore,count` | Errors counted-but-unsurfaced, phase completes | Phase already marked Completed by checkpoint; skipped if `checkpoint: idempotent` | yes |
| (no rule at any cascade level) | Strict default — phase fails, run stops (SRD-32 §"Default is strict") | Phase re-runs, strict default applies again | yes |

Every row says "yes" in the rightmost column on purpose — the
table is a verification of the rule, not a list of exceptions.

### CLI escape hatch

`--force-retry-failed` is the operator-side override for the
specific case of "I know the previous run hit a transient
network blip; please retry the failed phase regardless of its
declared policy". It synthesises a `.*:retry,warn` rule that
prepends to every phase's `errors:` cascade for this resume
invocation only. The workload's declarations are unchanged;
the operator is asserting one-time intent at the CLI.

### Identity = `(yaml_path, coords)` + hash sufficiency check

The phase identity tuple is structural: `yaml_path` (typed
nesting path) plus `coords` (the iteration values that
distinguish this instance from siblings). Tuple equality is
**necessary** for a checkpoint entry to apply to a freshly-
pre-mapped phase — but not **sufficient**.

Sufficiency is the per-phase hash:

- `hashed: true` (default for any set `checkpoint:` block) —
  resume requires both tuple match AND hash match. A workload
  edit that changes the phase's body (op fields, bindings,
  `cycles:`, error_policy, …) invalidates the saved status
  for *that phase only*; the rest of the resume plan is
  unaffected.
- `hashed: false` (explicit operator opt-out) — resume trusts
  tuple identity alone. Use case: phases whose body the
  operator knows is stable across edits even when the bytes
  drift (e.g. comment changes, formatter passes). Rare.
  Setting this is the operator asserting "I've checked, this
  phase's saved progress remains valid even when the YAML
  bytes don't match".

Collision detection within a single pre-map walk is impossible
by construction — comprehensions enumerate distinct tuples and
DFS is deterministic. If the resume planner ever sees two
checkpoint entries with the same `(yaml_path, coords)`, that's
a GK / pre-map bug, not a workload bug. Hard error at
checkpoint load.

## Out of scope for this design

- **Multi-machine / distributed runs**. Tier 3 territory.
- **Streaming-source phases** (e.g. message-queue consumers
  rather than range cursors). Don't fit the `cursor_extent +
  high_water` model. Tagged checkpoint-incompatible by their
  source factory.
- **Cross-workload checkpoint reuse**. A checkpoint from
  workload A doesn't apply to workload B even if they share
  phase names — the YAML-identity hash is the gate. No
  multi-workload session manager.
