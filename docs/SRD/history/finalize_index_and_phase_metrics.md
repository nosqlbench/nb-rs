# Design Note — `finalize_index` + phase-level `metrics:` block

**Status (updated 2026-06-08, second session):** **DONE.** The phase-`metrics:`
engine feature is implemented + verified end-to-end, AND the `finalize_index`
workload rework (§5–§6) has shipped — `finalize_index` authored in
`full_cql_vector.yaml`, `test_oracles` / `test_fknn` / the sweep's `idx_sweep`
repointed, the four jolokia/ensure phases retired. Validated via
`nbrs run … dryrun=phase` (clean exit) and `dryrun=dispenser` (got past
synthesis + dispenser construction to live-CQL connect). See §"WORKLOAD REWORK"
near the bottom.

The mechanism in §2.5 / §3 of the original draft was **wrong** and has been
corrected — see §"RESOLUTION" immediately below. Read that first; the
crossed-out parts of §2.5 / §3 are kept only as a record of what was tried.

---

## RESOLUTION — how phase duration is actually captured (corrected)

The original plan (`const phase_start := elapsed_s()` at phase start,
`volatile now := elapsed_s()` at phase end, subtract) **cannot work** in
this engine. Proven empirically with a throwaway probe against the real
eval engine (see findings below):

1. **Volatility is a NODE property, not a binding modifier.** A no-input
   clock node is declared `purity = Nondeterministic(...)` in
   `#[polydat_node]` and is volatile *by construction* — no binding
   modifier can make it const-foldable.
2. **`const X := <volatile>` is rejected** by the init-contract
   (`compile.rs` `init_binding_wired_to_nondeterministic_rejected`): an
   init binding must be effectively-const at scope-init. So
   `const phase_start := elapsed_s()` is a compile error.
3. **Two identical clock reads CSE-collapse to one node**, so
   `end - start` is structurally `X - X ≡ 0`.
4. **R1.v contagion** (`program.rs`) marks every consumer of a volatile
   node Dynamic, and Dynamic nodes are re-dirtied on every `set_input`
   (`engines.rs`), so a "frozen" start can't survive a cycle boundary.

**The working mechanism (implemented) — NO dedicated node needed:**

- The phase walker injects an **`extern phase_start: u64 = 0`** origin wire
  into the phase scope; the executor sets it (epoch millis at phase start)
  on the **completion-time metric pull**.
- A phase-level `metrics:` value resolves at completion to
  `now − phase_start` = the phase's wall-clock duration in millis. The
  clock read is its own `volatile` phase binding and the metric subtracts
  the injected origin:
  ```yaml
  bindings: |
    volatile now_ms := current_epoch_millis()
  metrics:
    time_to_index: { value: now_ms - phase_start }
  ```
  `current_epoch_millis()` already exists; it is a *single* clock read, and
  `phase_start` is plain injected data, so there is no CSE collapse and no
  contagion on the origin. Declaring the read as a `volatile` binding (rather
  than nesting `current_epoch_millis()` inside the metric value) explicitly
  acknowledges the non-deterministic node, so the phase kernel stays clean
  under `--strict`. (Clock-skew safety, if ever wanted, is
  `checked_sub(now_ms, phase_start)` — `checked_sub` already exists in
  `library/bitwise.rs`.)
- A **dedicated `phase_elapsed(start)` node was prototyped and then removed**
  as redundant — it was exactly `clock() − arg`, which the inline expression
  already expresses against the existing clock node. (Kept this note so the
  removal isn't re-litigated.)

**Verified:** `nbrs/tests/phase_metrics_e2e.rs` runs a testkit phase doing
~200 ms of work and asserts the `time_to_index` gauge matches the
`phase_outcomes` duration. Observed: `time_to_index = 223 ms` vs real
`phase_duration = 225 ms` (diff 2 ms ≈ 1%).

### Implemented this session

- **No new polydat node.** The general phase-`metrics:` mechanism + the
  existing `current_epoch_millis()` cover phase-duration metrics.
- **Phase-level `metrics:` block**, four touch-points:
  - model: `WorkloadPhase.metrics: HashMap<String, MetricSpec>`
    (`nbrs-workload/src/model.rs`).
  - parse: `parse_phase_metrics_field` — same schema as op metrics, raw
    `value:` preserved (NO auto-inject into bindings)
    (`nbrs-workload/src/parse.rs`).
  - classify: `WorkloadPhase::polydat_matter` ⇒ `Definitions` when metrics
    present (`nbrs-workload/src/polydat_matter.rs`).
  - synthesise: `synthesize_phase_bindings_with_poll` renamed to
    **`synthesize_phase_scope_bindings`**; emits `extern phase_start: u64 = 0`
    + one `volatile __metric_<name> := <value>` per metric
    (`nbrs-activity/src/scope.rs`). `volatile` acknowledges nondeterminism
    for strict mode; the executor pulls each `__metric_<name>` once.
  - emit: `executor::emit_phase_metrics` builds a subscope from the cached
    phase kernel, sets `phase_start`, pulls each metric, records it on the
    phase component (gauge default) before the final cadence flush; gated on
    successful completion (`nbrs-activity/src/executor.rs`).
- for_each + phase-`metrics:` is rejected loudly (initial-ship limit), like
  poll + for_each.

### Original draft, kept for the record (do NOT implement)

The original §2.5 ("No node-level volatility change is needed") and §3
(`elapsed_s()` with `const`/`volatile` bindings) are superseded by the
RESOLUTION above. They remain below only so the reasoning trail is intact.

**Goal (user's words):** collapse the three CQL-vector finalization phases
`ensure_flushed`, `jolokia_compact`, `jolokia_await_compaction` (and supersede
the newer `ensure_compacted`) into **one** phase callable as `finalize_index`,
applied to **write-data workloads only**, with a `time_to_index` phase-duration
metric. Plus: these scenarios must be covered by tests that run under
`cargo test --workspace`.

This needs two small engine features first, then the workload, then the test.

---

## 1. Resolved design decisions (do NOT re-litigate)

- **One phase, not a scenario group.** Daemons dispatch at their op-walk
  position (not phase init), so a single phase's stanza sequences
  `flush → read_sstables → trigger_compact(daemon) → poll_progress` in
  declaration order. Verified below.
- **Compact = long-poll daemon, NOT fire-and-cancel.** The user explicitly
  wants the JMX `forceKeyspaceCompaction` call held open for the whole
  compaction (no `on_timeout: accept`). The daemon fiber holds the reqwest
  future for the duration — this is intentional, overriding the
  `ensure_compacted` comment that called the held socket "unworkable".
- **Compaction is conditional:** only fire the compact when the table has
  **more than one** SSTable (`if: sstables > 1`).
- **Progress read = CQL `system_views.sstable_tasks`** (observable; per-cell
  progress metrics + memo), with an **op-level `poll:`** (`mode: await_empty`)
  — NOT a phase-level poll. Phase-level poll rewinds and re-runs the whole
  phase each tick, which would re-fire the flush; op-level poll loops only the
  read op, so flush + compact run exactly once (correct logical flow).
- **`time_to_index` = exact phase duration**, emitted via a new phase-level
  `metrics:` block referencing a volatile clock node — NOT the op-poll-wait
  approximation.

---

## 2. Mechanisms verified (so the next session doesn't re-derive)

All confirmed against the code this session:

1. **A stanza spans all the phase's ops, run once each in declaration order.**
   `OpSequence::stanza_length() == lut.len()` (`opseq.rs:111`); the cycle loop
   runs them "sequential in declaration order" (`activity.rs:2960`). With
   `cycles: 1` a phase runs exactly one stanza = all ops once, in order.
2. **Daemon ops dispatch at their op-walk position, NOT phase init.** The cycle
   loop spawns the daemon fiber when the walk reaches the daemon op
   (`activity.rs:2961-2995`), pinned by the
   `daemon_op_dispatches_at_cycle_pool_position` test. So a regular op declared
   *before* a daemon runs to completion first. **SRD-75 is stale on this** —
   staleness banner already added to `docs/SRD/75_phase_poll.md`.
3. **`if:` gates the compaction even on a daemon.** The daemon fiber always
   spawns, but the op's `if:` (`template.condition`) is evaluated *inside* the
   daemon's dispenser, reading captures from earlier stanza ops via the phase
   scope. So `if: sstables > 1` (capture from `read_sstables`) correctly skips
   the actual `forceKeyspaceCompaction` when there's ≤1 SSTable.
4. **Op-level poll exists:** `PollingDispenser` (`wrappers/poll.rs`),
   `mode: await_empty` (zero result rows = done) + `interval_ms` / `timeout_ms`
   / `max_error_retries` / `metric_name`. This is what the old
   `jolokia_await_compaction` used.
5. **Polydat volatility exists at the BINDING level:** `volatile <name> := …`
   → `ExportClassification::Volatile` (`kernel/subcontext/spec.rs:52`,
   "excluded from const-fold identity"). A no-input clock node would otherwise
   be const-folded once; `volatile` forces a fresh read. **No node-level
   volatility change is needed.**
6. **Phase duration** is computed at `executor.rs:3421` (`phase_start.elapsed()`)
   and goes into the PhaseOutcome, but there is **no named-metric hook** today —
   that is exactly what the phase `metrics:` block adds.

---

## 3. Engine feature A — `elapsed_s()` polydat library node

- A `#[polydat_node]` (SRD-80b is the sole authoring path) returning `f64`
  seconds since a **process-static monotonic origin** (a `static Instant` /
  `OnceLock<Instant>` captured at first use or process start). **No inputs.**
- **Do NOT use `Date::now()` / `Math::random()` / argless `new Date()`** — those
  are banned in polydat scripts (resume-breaking). This is a Rust node reading a
  monotonic `Instant`, which is fine.
- Used with the existing `const` / `volatile` binding modifiers:
  ```
  const phase_start := elapsed_s()    # const → folded once at phase init = start
  volatile now      := elapsed_s()    # volatile → re-read on each pull
  ```
- Register it in the polydat library + the stdlib node surface (model on the
  `library/math.rs` nodes + the SRD-66 stdlib registration). Add a unit test:
  two reads `interval_ms` apart differ by ~that interval; `volatile` re-reads
  while `const` does not.

## 4. Engine feature B — phase-level `metrics:` block

Mirror the op-level `metrics:` mechanism. Four touch-points:

1. **Model** (`nbrs-workload/src/model.rs`): add
   `WorkloadPhase.metrics: HashMap<String, MetricSpec>` (struct at `:659`),
   mirroring `ParsedOp.metrics` (`:1231`) and reusing `MetricSpec` (`:1807`).
2. **Parse:** read a phase-level `metrics:` map exactly like the op parser does
   (same `MetricSpec` shape: `{ type: gauge|counter|…, value: <polydat expr> }`).
3. **Synthesis** (`nbrs-workload/src/polydat_matter.rs:103-113`): op `metrics`
   already become `__metric_<name> := <value_expr>` Definitions on the op scope.
   Extend the same translation to the **phase scope** for the phase `metrics:`
   map, so the value exprs (incl. `now - phase_start`) bind on the phase kernel.
4. **Executor** (`nbrs-activity/src/executor.rs`, around `:3421` / the
   `phase_completed` sites at `:813,846,1959,2878,3544`): at phase completion,
   for each phase metric pull `__metric_<name>` from the phase scope kernel and
   emit it as the declared instrument (gauge by default), labelled with the
   phase scope coords — same `ctx.wires`/emission path the op-poll `metric_name`
   uses. Pull happens ONCE at completion; `volatile` bindings re-read then, so
   `now - phase_start` resolves to the true phase elapsed.

**Validation:** the phase-metrics value exprs reference phase-scope wires only
(bindings, captures, params, iter-vars, `elapsed_s()`); the workload-init
validator's allow-set already covers bindings + params + iter-vars (see the
`ensure_compacted` forwarding-bindings comment for the pattern).

---

## 5. `finalize_index` workload phase (the target YAML)

Defined once in `adapters/cql/workloads/full_cql_vector.yaml` (the sweep
inherits it via `extends:`). Shape:

```yaml
finalize_index:
  adapter: cql               # default for the CQL reads; flush/compact override to http
  concurrency: 1
  cycles: 1
  bindings: |
    const phase_start := elapsed_s()
    volatile now      := elapsed_s()
    # progress forwarding bindings (pct, pending_tasks, progress_bytes, total_bytes)
    # — copy from ensure_compacted's bindings block
  metrics:
    time_to_index: { type: gauge, value: now - phase_start }
  ops:
    flush:            # = ensure_flushed: jolokia forceKeyspaceFlush, sync, request_timeout_ms 600000,
                      #   verify status==200, strict, memo before/after. Runs once.
    read_sstables:    # read the table's live SSTable count (jolokia LiveSSTableCount MBean, or a CQL
                      #   equivalent) -> capture `sstables`. Runs once; gates the daemon.
    trigger_compact:  # daemon: true; if: "sstables > 1"; jolokia forceKeyspaceCompaction(false,
                      #   {keyspace}, [{table}]); request_timeout_ms 3600000 (LONG-POLL, holds socket);
                      #   NO on_timeout: accept; verify status==200; strict; memo.
    poll_progress:    # = ensure_compacted read_compaction_progress: CQL SELECT completion_ratio,
                      #   progress, total FROM system_views.sstable_tasks WHERE ks/table/kind='compaction';
                      #   capture active_compactions(:count), completion_ratio, progress, total;
                      #   OP-LEVEL poll: { mode: await_empty, interval_ms 5000, timeout_ms 14400000,
                      #   max_error_retries 5 }; memo with live pct/bytes; metrics: the 3 progress gauges.
```

Pull the exact op bodies from the current phases before deleting them:
`ensure_flushed` (`full_cql_vector.yaml:511`), `jolokia_compact` (`:565`),
`jolokia_await_compaction` (`:612`), `ensure_compacted` (`:689`, incl. the
bindings + progress memo + metrics + the LiveSSTableCount read shape from the
SRD-75 example).

---

## 6. Repoint + retire (write-data workloads only)

- **Repoint** to `- finalize_index`:
  - `full_cql_vector.yaml` `test_oracles` (lines 222-224) — drop the 3 jolokia phases.
  - `full_cql_vector.yaml` `test_fknn` (lines 252-254) — drop the 3 jolokia phases.
  - `full_cql_vector_sweep.yaml` `idx_sweep` inner tree (lines 398-399) —
    replace `ensure_flushed` + `ensure_compacted`.
- **Do NOT** touch `test_oracles_query_only` (no writes) or
  `cql_compaction_test.yaml` (separate standalone test with its own
  `jolokia_flush`).
- **Retire** (delete the now-unreferenced phase defs from `full_cql_vector.yaml`):
  `ensure_flushed`, `jolokia_compact`, `jolokia_await_compaction`,
  `ensure_compacted`.

---

## 7. Test coverage (the user's requirement)

Must run under `cargo test --workspace`:

- A minimal **example workload** under `examples/workloads/` exercising the
  pattern with a mock/test adapter: a multi-op phase with (a) a capture-gated
  daemon (`if:` on the daemon), (b) an op-level `poll: await_empty`, (c) a
  phase-level `metrics:` block, (d) `elapsed_s()` via `const` + `volatile`.
- A workspace integration test (model on the existing
  `nbrs-activity/tests/pipeline_e2e.rs` / `nbrs/tests/inline_workload.rs`
  patterns) that loads + runs it against a counting/mock adapter and asserts:
  ops run in declaration order; the daemon's op fires only when the gating
  capture is >1; the op-poll loops until empty; the phase metric
  (`time_to_index`) is emitted with a plausible (>0) value.
- Plus polydat unit test for `elapsed_s()` (const vs volatile re-read), and a
  `polydat_matter` unit test for the phase-metrics → `__metric_<name>` Definition
  translation (mirror the op-metric tests at `polydat_matter.rs:250-290`).

---

## 8. Already done this thread

- `docs/SRD/75_phase_poll.md` — staleness banner added flagging that the
  synchronizer example shows `trigger_compact` as a conditional regular op while
  the shipped code uses a daemon dispatched at op-walk position; lists TODOs
  (rewrite §Workload surface / §Runner integration, cross-ref SRD-79, fix the
  `daemon_pool.rs` "spawned at phase init" doc).

## 9. Cross-refs

SRD-75 (phase poll — stale banner), SRD-79 (daemon ops, `daemon: true`),
SRD-80b (`#[polydat_node]` authoring), SRD-66 (stdlib node surface),
SRD-13c / SRD-11 (const + volatile lifecycles), SRD-15 (strict workload-load
validation). The phase `metrics:` block likely deserves its own SRD number when
implemented (it's a real workload-surface + synthesis + executor seam).

---

## WORKLOAD REWORK — shipped 2026-06-08 (non-obvious authoring notes)

`finalize_index` lives in `adapters/cql/workloads/full_cql_vector.yaml`; the
sweep inherits it via `extends:`. Four ops in declaration order: `flush`
(jolokia, sync) → `read_sstables` (jolokia LiveSSTableCount, capture
`sstables`) → `trigger_compact` (daemon, `if: sstables > 1`, long-poll
`forceKeyspaceCompaction`, NO `on_timeout: accept`) → `poll_progress` (CQL
`system_views.sstable_tasks`, op-level `poll: await_empty`).

Wiring facts that took iteration to get right (validated via dryrun):

1. **Cross-op capture needs a manual `shared` cell.** SRD-75's automatic
   capture→shared-cell synthesis only fires for a **phase-level** `poll:`.
   `finalize_index` uses an **op-level** poll (so `flush` doesn't re-fire each
   tick), so the one cross-op wire — `sstables`, written by `read_sstables`,
   read by `trigger_compact`'s `if:` — is declared `shared sstables := 0` in
   the phase `bindings:`. `read_sstables` must also *reference* `sstables`
   (via its `table_live_sstables` metric `value: sstables`) so its capture
   write attaches to the cell — a capture-only op gets no write slot.
2. **Same-op captures are op-local external-write ports.** `poll_progress`'s
   progress captures (`active_compactions`, `completion_ratio`, `progress`,
   `total`) are read only by its own metrics/memo, so they are declared
   `extern <name>: <type> = <default>` in `poll_progress`'s own `bindings:`
   (NOT phase-scope `shared`). `completion_ratio` is `f64`; the byte counters
   are `u64`.
3. **No `pct` percent math.** `completion_ratio * 100.0` fails — the `*`
   operator maps to the u64 `mul` node, so it can't take an f64 operand. (The
   retired `ensure_compacted` "worked" only because its `shared := 0` capture
   cells were u64, silently truncating the ratio to 0/1.) The gauge reports
   `completion_ratio` (0..1) directly.
4. **Strict-clean clock read.** The phase-duration metric reads the clock via
   a `volatile now_ms := current_epoch_millis()` phase binding and the metric
   is `value: now_ms - phase_start`. Nesting `current_epoch_millis()` inside
   the metric value triggers a strict-mode "non-deterministic node used
   without acknowledgment" error (the `volatile` on `__metric_<name>` doesn't
   reach the nested node through the intervening `-`); the dedicated `volatile`
   binding acknowledges it directly.

Two scanner bugs surfaced + fixed (both: nbrs-side scanners not matching the
Polydat lexer's accepted forms — same class):
- `scope::scan_idents_in_polydat_source` didn't skip `#` hash comments (the
  Polydat lexer does), so comment words inside a `bindings: |` block leaked in
  as phantom wire refs. Also fixed `scan_locally_declared_idents` to skip `#`.
- `runner::scan_polydat_binding_lhs` didn't recognize `extern` declarations
  (it handled `input` + `const`/`shared`/`volatile`/`cursor`), so a `{name}`
  placeholder over an extern-declared wire tripped the undeclared-placeholder
  guard. Routed `extern` through the same `name: type` parser as `input`.
Both have regression tests.

Leave alone: `test_oracles_query_only` (no writes) and
`adapters/cql/workloads/cql_compaction_test.yaml` (standalone, own jolokia_flush).

### Daemon `if:` pull-resolution bug (found via live run, fixed)

The first live run (`cassandra-cpp`, sift1m) got through keyspace_reset →
schema → fknn_rampup_data, then `finalize_index` failed:

```
daemon op 'trigger_compact' panicked: index out of bounds: the len is 0
but the index is 0   (nbrs-activity/src/fixture.rs ResolvedPulls::get)
```

Root cause: `activity::daemon_dispatch` built the daemon op's `ExecCtx` with
`ResolvedPulls::empty()`. Any wrapper on a daemon op that registers a pull —
the IF_COND wrapper for `if: sstables > 1` — then indexed into an empty vec.
The retired `ensure_compacted` daemon had **no** `if:`, so it never hit this;
the conditional gate on a daemon is new here, and design §2.3's "the `if:` is
evaluated inside the daemon's dispenser" was never runtime-verified.

Fix: `daemon_dispatch` now resolves its pull plan via
`fiber.resolve_pulls_for_idx(template_idx, &pull_plans[template_idx])` (the
cycle-pool path), with `pull_plans` threaded through from the spawn site. The
captured `sstables` shared cell is visible because the daemon dispatches at its
op-walk position, after `read_sstables` wrote it. Regression test:
`nbrs-activity/tests/daemon_if_e2e.rs` (daemon op + `if:` through the full
runner; the lightweight Activity harness can't compile conditions).

**Live validation (after fix):** `finalize_index` completes (6.39 s). Recorded
metrics: `time_to_index = 6452 ms` (≈ phase duration), `table_live_sstables = 4`
(so `if: sstables > 1` fired the compaction), and the four `compaction_*`
progress gauges. End-to-end confirmed against a live Cassandra cluster.
