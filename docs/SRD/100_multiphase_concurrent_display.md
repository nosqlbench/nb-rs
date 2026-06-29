# SRD-100: Multi-Phase Concurrent Display

> **Status:** DRAFT — model agreed (2026-06-29). A **projection-completion**
> SRD over [SRD-81](81_event_sourced_display.md): the live *data* fold
> (`RunState.active_phases`) is already multi-phase-correct; the live
> *projection* (status line, footers, TUI header/latency) still collapses
> to a single active phase. This SRD completes the projection so **every**
> display form renders **N concurrent active phases** correctly, gives an
> active phase a real identity, and fixes a cross-execution snapshot
> collision. It introduces **no new data model** — it removes single-phase
> scalars and re-derives every live surface as a keyed fold.
>
> Cross-refs: [SRD-81 event-sourced display](81_event_sourced_display.md)
> (the projection model this completes), [SRD-88 concurrent in-process
> executions](88_concurrent_executions.md) (`exec_id` partition),
> [SRD-77 working sessions](77_working_sessions_and_refine.md)
> (`execution_id` durable key), [SRD-76 PhaseOutcome](76_phase_outcome_disposition.md),
> [SRD-87 output channel](87_output_channel.md) (the status bucket + single
> fd owner), [SRD-63 readout](63_status_readouts.md), and SRD-41/30 console
> ownership.

## 1. Ownership & relationships

Owns **how N simultaneously-active phases are rendered, identified, and
kept distinct across all display surfaces**. Concurrency arrives three
ways, all in scope:

- **Concurrent scenario phases** — sibling phases dispatched together by
  `run_scenario_body` (the SRD-92 unified child drive).
- **Daemon phases** — a daemon phase runs *alongside* the foreground it
  shadows (`executor.rs` foreground/daemon partition).
- **Concurrent in-process executions** — SRD-88: N executions in one
  process, partitioned by `exec_id`.

This SRD sits **over** SRD-81 (it obeys "display = f(snapshot)" and the
single `ReadoutSink` seam) and **consumes** the SRD-88 `exec_id` and the
SRD-76/77 phase-outcome identity. It does **not** own the execution
harness, the metrics store, or the durable record — all three are already
multi-phase/multi-execution correct (see §2).

## 2. The defect

**The data fold is already multi-phase; the live projection is not.**

`RunState.active_phases: HashMap<PhaseKey, ActivePhase>` genuinely holds N
concurrent phases (the run-state actor routes `PhaseStarting` /
`PhaseProgress` / `PhaseCompleted` / `PhaseFailed` by key; per-phase
counters, throughput, rate, and peak trackers live **on** `ActivePhase`).
The scene tree holds independent per-node status. The metrics store is
fully label-keyed by `phase` + `exec_id`. The durable record (SRD-63
`readout_snapshots`, SRD-76 `phase_outcomes`, SRD-77 `+execution_id`) is
multi-phase and multi-execution robust on disk.

The break is **entirely in the in-memory live-status projection**:

1. **A single pre-rendered scalar status.** `RunState.status_render:
   Option<String>` is *one* status line for "the active phase", fed by an
   **unkeyed** status bucket (`output_channel::status(Option<String>)`,
   `RunObserver::set_status_line`). Each phase spawns its own inline-status
   thread that renders *its* phase and submits to the one slot → **N
   writers stomp one slot** (~500 ms last-wins flicker); other active
   phases are **invisible** in line mode; one phase ending with
   `status(None)` **wipes its peers**.
2. **Session-global latency scalars fanned to all phases.** The coalesced
   `LatencyFrame` overwrites session-wide `*_nanos` scalars and records
   one session `max` into **every** phase's peak trackers — so every
   concurrent phase displays **identical** latency (an in-code
   multi-phase-demux TODO already flags this).
3. **Session-global sparkline cleared on phase boundaries.** `PhaseStarting`
   clears the session `ops_history` / `rows_history` rings — a second phase
   starting **wipes the first running phase's** accumulated sparkline.
4. **A pervasive "pick the first phase" rule.** `first_active()` /
   `active_phases.values().next()` in the default sinks (`log_only_sink`,
   `formatted_line_sink`), the TUI header/sparkline, and the inspector —
   N concurrent phases collapse to one **arbitrary** phase.
5. **Racy phase identity.** `find_phase` matches by structural DFS order
   ignoring labels, which races under concurrent dispatch (siblings reach
   `phase_starting` out of pre-map order) and **cannot distinguish** a
   same-named daemon+foreground pair or concurrent executions.
   `exec_id` is absent from the entire display path.
6. **Cross-execution data loss.** `readout_snapshots`' primary key has no
   `exec_id` (`subject_id = name@labels`), so two concurrent SRD-88
   executions of the same phase **upsert-collide** — one execution's render
   is silently lost (unlike `metrics.db`, which is `exec_id`-tagged).

The **default non-TUI sinks are worst** (one footer / one status line for
an arbitrary phase). The **full TUI is best** (its Focus/Maximal tree LODs
already iterate running phases) but still leaks the single-phase model in
its header ETA, session sparkline, and per-phase latency rows.

## 3. Root cause

**The single deepest assumption is that "the active phase" is a scalar the
display fold can cache as a pre-rendered surface** — embodied by
`status_render: Option<String>` and the unkeyed status bucket. Everything
in §2 inherits from it: because the status surface is one slot with no
subject key, the producer submits a finished string for one phase and
last-writer-wins; because the live render is *cached* rather than
*re-derived* from `active_phases`, every "pick the first" consumer and
every session-global scalar is a downstream coping mechanism for a
projection that has one slot where the data has N keyed entries.

The fix is a **projection-completion job**: make every live status/metric
surface a **pure keyed fold over `active_phases`** (SRD-81: "display =
f(snapshot); no surface consumes a string rendered for another surface"),
and the cascade of single-phase scalars collapses out.

## 4. Phase identity model

The canonical live-display identity for one active phase instance is a
first-class struct that **extends** `exec_id`:

```
ActivePhaseId { exec_id: ExecId, name: String, labels: String }
```

replacing the current `PhaseKey = (String, String)` tuple. Rationale:

- `(name, labels)` disambiguates concurrent sweep cells and
  daemon-vs-foreground only *by accident* — it **fails** for a same-named
  daemon+foreground pair and for SRD-88 concurrent executions of the same
  workload.
- `exec_id` is the **sole** existing cross-execution partition key (SRD-88
  A2; monotonic from 1), already keys the durable stores (SRD-77), but
  never reaches `RunState`. Adding it aligns the live fold with the store
  and **defaults cleanly to `exec_id = 1`** for single-run (SRD-88 A1
  byte-identical behavior).

The **stable row key** for lifecycle routing is `(exec_id, SceneNodeId)`,
where `SceneNodeId` is allocated **at dispatch** and threaded through the
observer lifecycle callbacks. `find_phase` / `set_phase_*` /
`running_phase_indent` key on that token instead of first-Pending-by-DFS,
retiring the structural-order match that races under concurrency.
`(exec_id, name, labels)` is the **human-facing/label** key derived from
the row key. `PhaseIdentity` (SRD-76) gains `exec_id`.

A **kind/extent discriminator** — `kind: Foreground | Daemon` and an
`open_extent: bool` — is carried **on `ActivePhase`, NOT in the key**, so
the renderer can badge daemons and skip ETA/progress math for open-ended
phases without fragmenting identity.

> **Invariant (no string-match key):** `find_phase` must not reintroduce
> byte-exact coordinate-label-string equality as the match key (the
> historical "task #19 drift"); the dispatch-time token is the only
> correct key under concurrency.

## 5. The keyed live model

- **Eliminate `status_render`.** No cached pre-rendered status. Each
  surface re-derives N status renders from `active_phases` each tick via
  its own `ReadoutSink` (one single-subject `ReadoutContext` per phase).
  Per-phase `ActivityMetrics` already live on `ActivePhase`, so every
  surface has what it needs. (If a cache is ever reintroduced for cost, it
  must be `HashMap<ActivePhaseId, String>` with per-id clear — never a
  scalar — but the default is **no cache**.)
- **Per-phase latency** is demuxed onto `ActivePhase` (§10) rather than
  stored as session scalars.
- **Per-phase sparkline/history** moves to `ActivePhase`; any remaining
  session-wide sparkline becomes an **explicit aggregate**, not an
  accidental single-phase ring cleared on phase boundaries.
- New/extended `RunStateCmd` variants stay **exhaustive** and are applied
  **only** inside the single-writer run-state actor (no new locks).

## 6. The status-bucket + observer seam

**Decision (cross-cutting): render at the consumer, not the producer.**
The multi-phase status is produced by **one consumer-side renderer that
folds the live `active_phases` snapshot**, not by N per-phase producer
threads submitting keyed strings. This single move removes, together:

- the **last-writer race** on the one status slot,
- the **`std::thread` cross-execution hazard** (the inline-status thread
  can't read the SRD-88 task-local `ExecutionContext.channel`, so it falls
  back to the process-global channel and cross-contaminates executions),
- the **per-phase-clear-wipes-peers** bug.

It also centralizes ordering / height-budget / clamping where the surface
geometry is known, consistent with the **single fd owner** invariant
(SRD-87) and the follow-the-log sticky footer. The racing inline-status
producer threads (`activity.rs`) are **retired**; `output_channel::status`
and `RunObserver::set_status_line` either gain an `ActivePhaseId` (if any
keyed submission survives) or are removed in favor of the consumer fold.

## 7. Per-surface projection contract under N phases

Every live surface drives the **single-subject** `ReadoutContext` **N
times** through its own `ReadoutSink` (SRD-81 §5: a surface never iterates
readouts and never consumes a string built for another surface). The
sink-agreement property test must hold across phases.

### 7a. DEFAULT `LogOnlySink` — stacked, capped footer

The line-mode footer becomes a **stacked block: one status row per active
phase**, in **stable dispatch order** (by phase seq, not `HashMap`
iteration order), with an optional progress-bar margin per row. **A cap
bounds the height**: when active phases exceed the cap, the overflow
collapses to a single **"… N more running"** roll-up line (the cap keeps a
short terminal usable and keeps the follow-the-log geometry stable). The
"running" counter becomes a **multi-running** representation
(e.g. `running 2 · [3/10] [7/10]`) with `expected_total_phases` pinned as
the Y denominator. The block stays a **pure `f(snapshot)` follow-the-log
redraw** (`draw_footer_at_cursor`, relative not absolute), with
cursor-return accounting **hardened for variable N-row height** across
alt-screen excursions and Ctrl-T swaps.

### 7b. DEFAULT `FormattedLineSink` — per-phase segments

Emits **one status segment/line per active phase** (log-tail friendly)
rather than one line from `values().next()`; the single
`.position(Running)` counter becomes the multi-running counter from 7a.

### 7c. TUI app — close the single-phase leaks

The panel and Focus/Maximal tree LODs already stack per-phase blocks. Fix
the remaining leaks: per-phase latency rows source from **demuxed
per-phase fields on `ActivePhase`** (§10), not session scalars; the
session sparkline becomes an **explicit aggregate** (or is retired — the
per-phase `throughput_summary` already exists); the header shows an
aggregate / **"N running"** rather than one phase's ETA; the
`readout_binder` focus/LOD state is **keyed by `ActivePhaseId`** (or scoped
to the tree-selected phase) so `+`/`-`/focus address one phase's row.

### 7d. Reference surface

`inspector_server`'s `render_active` is the canonical reference for the
keyed-fold contract; all surfaces conform to it.

## 8. Daemon / open-extent phases

Daemons render **inline with a daemon badge** (not a separate region), with
**ETA / progress-bar math suppressed** (an open-extent phase has no
meaningful denominator), and **counted toward "N running"**. The
`kind`/`open_extent` discriminator (§4) is carried from the
foreground/daemon partition (`executor.rs`) onto `ActivePhase`; the
renderer reads it to choose the badge + suppress progress. Daemons do
**not** move the pinned Y denominator (§11).

## 9. Concurrent in-process executions (SRD-88) — merged surface

Two parts:

1. **Identity + collision fix (prerequisite).** `exec_id` enters the live
   key (`ActivePhaseId`, §4) **and** the `readout_snapshots` subject id /
   PK (`subject_id` gains `exec_id`, aligning with SRD-77's
   `(execution_id, phase_name, phase_labels)` outcome key). This fixes the
   §2.6 cross-execution upsert collision — a genuine data-loss bug —
   independent of any merged surface.
2. **A merged multi-execution display surface (net-new).** A single display
   spine renders **all** concurrent in-process executions, **grouped by
   `exec_id`**, with per-execution phase rows under each. This replaces the
   current per-execution isolated-spine topology as the default when N > 1
   executions run concurrently; with one execution it is byte-identical to
   today (SRD-88 A1). Grouping, ordering, and the breadth/counter semantics
   are defined over `(exec_id, dispatch-seq)`.

> The merged surface is **net-new architecture** and lands **last** (P6),
> on top of the identity + collision fix (P1) which is required regardless.

## 10. Per-phase latency demux (in scope)

The coalesced `LatencyFrame` currently writes session-global scalars and
fans one session `max` into every phase. This SRD **demuxes the metric
frame by `phase` (+ `exec_id`) label** into **per-phase latency fields
stored on `ActivePhase`**, fed from the already-labeled cadence / queryapi
store (`Selection::with_label("phase", …)` + `exec_id` resolution). Peak
trackers then record **each phase's own** max. One fix resolves the
`LatencyFrame` fan-out, the TUI detail-block leak, and any latency chip in
the status line.

> **Highest-risk change** — it reaches into the lock-free metrics-frame
> ingest path (the extractor that drops labels, the `LatencyFrame` apply in
> the actor, the per-phase peak trackers). It must **read** the existing
> labels, not add locks or a parallel live-latency cache (lock-free metrics
> invariant). Sequenced as its own push (P4) for isolation.

## 11. Invariants honored

This SRD must preserve, and is constrained by:

- **Single-writer actor + ArcSwap** — `RunState` is mutated only by the
  run-state actor via exhaustive `RunStateCmd` variants; readers do one
  atomic `load_full()`. No shared `RwLock<RunState>` (SRD-02). Per-phase
  status/latency state lives on `RunState`/`ActivePhase`, mutated only in
  the actor.
- **display = f(snapshot)** — live surfaces are pure folds of the ArcSwap
  snapshot, re-derived each tick; the high-cadence metric tick is never
  event-sourced (SRD-81 §3). Multi-phase status is a projection over
  `active_phases`, not a persisted render.
- **One `ReadoutSink` seam, render-per-surface** — drive the single-subject
  `ReadoutContext` N times per surface; never make readouts iterate; never
  consume another surface's string (SRD-81 §5). The sink-agreement property
  test holds across phases.
- **No-screen-buffer-for-unseen-state / follow-the-log sticky footer** —
  history lives in the scrollback + the keyed session store, not a managed
  screen region; the live footer redraws from source via
  `draw_footer_at_cursor` (relative). A variable N-row footer keeps this
  discipline with valid cursor-return accounting.
- **One OutputChannel owns the terminal fd** — producers submit to buckets;
  only the impl touches the fd; every method non-blocking (SRD-87,
  SRD-41/30). Multi-phase status funnels through **one** surface owner —
  fold at the consumer, never N writers to the terminal.
- **Lock-free metrics** — cadence + frame ingest are actor+ArcSwap; the
  store is already label-keyed by `(name, LabelSet)` incl. `phase` +
  `exec_id`. The latency demux **reads** those labels; it adds no locks.
- **`exec_id` is the cross-execution partition key** (SRD-88 A2) — display
  identity extends `exec_id + (name, labels)`; any new dimension defaults
  cleanly to `exec_id = 1` (SRD-88 A1).
- **task-local-first channel/observer resolution** — any producer running
  off-task must capture the per-execution channel **by value** before
  spawn (task-locals don't cross OS-thread boundaries). Retiring the
  inline-status thread (§6) removes the last such producer.
- **`expected_total_phases` is pinned at `install_tree`** — it is the
  stable Y denominator; a multi-running **numerator** must not move Y.
  Concurrent/daemon/runtime-materialized phases may push the numerator
  past Y honestly (N > Y) but must not change Y.
- **Per-phase `FrameAck` end-of-phase render guarantee** holds **per phase**
  under concurrency (every phase completion gets ≥1 rendered frame before
  the executor proceeds) — not collapsed into a session-level ack.
- **Per-phase `ActivityMetrics` ownership** — each phase owns its
  `Arc<ActivityMetrics>`; concurrent phases are never collapsed onto a
  shared metrics store (the `LatencyFrame` fan-out is the cautionary
  counter-example).

## 12. Property / equivalence tests

- **Sink-agreement across N phases** — `StringSink` and `TuiReadoutSink`
  agree per phase for ≥2 concurrent phases (extends the SRD-81 property).
- **Multi-phase status-fold determinism** — the stacked footer is a
  deterministic function of the snapshot with stable dispatch ordering;
  same snapshot → same bytes (drive the data source, not a workload, per
  the `shadow_terminal` harness convention).
- **Concurrent-execution snapshot non-collision** — two executions of the
  same phase produce two distinct `readout_snapshots` rows (the §2.6
  regression test).
- **Latency-demux correctness** — with two concurrent phases of different
  latency profiles, each phase's row shows its **own** latency (not the
  aggregate); peak trackers are per-phase.
- **Single-run byte-identity (SRD-88 A1)** — one phase / one execution
  renders byte-identical to pre-SRD-100 on every surface.

## 13. Pushes (sequenced)

- **P1 — Identity foundation.** `ActivePhaseId { exec_id, name, labels }`;
  dispatch-time `SceneNodeId` threaded through observer
  `phase_starting/progress/completed/failed` + `PhaseProgressUpdate`;
  retire structural-order `find_phase`; `PhaseIdentity` gains `exec_id`;
  `readout_snapshots` subject id / PK gains `exec_id` (§2.6 fix). Carries
  the §12 non-collision test.
- **P2 — Keyed/folded status.** Eliminate `status_render`; consumer-side
  renderer folds `active_phases`; retire the inline-status producer threads
  + the unkeyed status bucket (§5, §6).
- **P3 — Default-sink layouts.** `LogOnlySink` stacked-capped footer +
  multi-running counter + follow-the-log height handling; `FormattedLineSink`
  per-phase segments (§7a/7b).
- **P4 — Per-phase latency demux + TUI leaks.** Demux the frame onto
  `ActivePhase`; fix TUI header/sparkline/latency/binder (§7c, §10).
  *(Highest risk — isolated.)*
- **P5 — Daemon / open-extent rendering.** Badge + ETA/progress suppression
  + breadth-counter policy (§8).
- **P6 — Merged multi-execution surface.** Net-new spine grouping by
  `exec_id` (§9.2), on top of P1's identity + collision fix.

## 14. Open items (minor — not blocking the draft)

- **Multi-running counter form** — `running N`, a set of active seqs, a seq
  range, or omission when N > 1 (P3 picks a default; revisit if noisy).
- **TUI Default LOD multi-expand** — whether concurrent inspection of
  multiple simultaneously-expanded entries is required in Default LOD, or
  Focus/Maximal remain the sanctioned multi-phase inspection LODs (Default
  keeps its at-most-one-expanded invariant unless raised).
- **Footer cap value + relevance order** — the P3 cap is a constant +
  dispatch-order; a relevance/priority order (e.g. nearest-to-done first)
  is a possible refinement.

## Settled trade-offs (2026-06-29)

- Default line footer under N phases → **stacked rows, capped** (§7a).
- Per-phase latency demux → **in scope now**, isolated as P4 (§10).
- SRD-88 concurrent-execution topology → **build the merged surface** (§9),
  on top of the required identity + PK fix.
- Daemon / open-extent presentation → **inline badge + suppressed ETA,
  counted toward "N running"** (§8).
