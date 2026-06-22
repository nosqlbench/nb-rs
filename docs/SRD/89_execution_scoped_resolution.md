# SRD-89: Execution-Scoped Dimensional Resolution

**Status:** DRAFT 2026-06-22. Specifies the by-default, component-sourced
qualification of every read a workload makes against a session-shared
service — the metric store **and** the control tree — so concurrent
executions sharing one session (SRD-88) never cross-talk. Supersedes the
ad-hoc pieces in SRD-88 §3c ("Live metric reads are `exec_id`-scoped" and the
deferred `SESSION_ROOT` control-isolation note) and generalizes the report
scoping of SRD-88 §5b.

## 1. The principle

Inside a workload, the **execution component is the dimensional root**. The
component tree (SRD-42 / SRD-88) already establishes the dimensional labels —
`session` at the session tier, `exec_id` at the execution tier, `phase`
below. Those labels are the *reference point* for qualifying every query the
workload issues: a read against a session-shared service is **implicitly,
by default, narrowed to the execution component's dimensional identity**
(`session` + `exec_id`) and its subtree.

This is one rule applied to two shared services:

- **Metric / metricsql queries** are evaluated as if the execution
  component's labels were `AND`-ed into every selector.
- **Control resolution** (`control(...)`, `control_set(...)`, the servo's
  retargets) resolves against the execution component's subtree, not the
  shared session root.

"Being inside a workload execution" is what makes scoping correct — not a
call site remembering to wrap itself.

## 2. Why (the defect)

SRD-88 makes the session's services **shared** across concurrent executions
(one store, one cadence reporter, one component root) and tags writes with
`exec_id`. Reads, however, were only *partially* scoped:

- **Metrics — leak-prone post-filter.** `MetricsQueryAccess::select_range`
  drops result series whose `exec_id` ≠ the reader's, where the reader's
  `exec_id` comes from a task-local hook (`install_read_exec_id_hook`) that
  is only wrapped onto the **settle's** delivery fiber (`settle.rs`
  `context_wrap`). Any read on a path without that task-local set sees
  `None` ⇒ no filter ⇒ every execution's series leak in. It is also (a) a
  **post-filter** — `sum()`/`rate()`/`count()` over a vector still *compute*
  over the unscoped set, only the output is trimmed — and (b) `exec_id`
  only; `session` is never a query constraint (implicit-by-store today).
- **Controls — shared root (deferred in SRD-88).** `control(...)` resolves
  via `find_control_erased_up` from the session-set `SESSION_ROOT`. Two
  concurrent executions of the same workload declare the **same-named**
  control (`concurrency`, `rate`); both resolve to the *same* instance.
  A servo retargeting `concurrency` then drives every neighbour's phase —
  deterministic cross-talk.

The observable failure: with the SRD-88 serial-isolation workaround removed
from the example walker, the **servo** optimizer examples (`control`,
`multiservo`) converge to the wrong coordinate at any harness concurrency > 1,
**deterministically** (not noisy-neighbour) — the signature of shared control
state, not a timing race. The rerun examples (`saturation`, `metricsql`,
`hybrid`) survive because they neither servo a live control nor loop a live
read whose aggregate spans neighbours.

## 2b. Empirical verification (2026-06-22) — the metric path already scopes; the **control** is load-bearing

Instrumenting the whole servo read chain (`foreach` → `dispatch_optimization`
→ `run_control_search` → `start_settle` → settle pulse → metricsql
`read_value` → `select_range`), file-appended to bypass the walker's per-exec
stderr capture, on a **complete** full-walker run (default concurrency):

- The servo pipeline **does fire** under concurrent in-process execution:
  `read_value` **289×**, `servo_runner` 4×, `start_settle` with
  **`has_ctx=true`** (96×). (An earlier "0×" reading was a **false negative**
  — the *narrowed* probe runs timed out before reaching `control.yaml`.)
- The metricsql reads **are exec-scoped**: `read_exec_id=Some(73/74/75/76)` —
  a distinct id per concurrent execution — so `select_range`'s `exec_id`
  post-filter applies and each read sees only its own series. **The metric
  query is not the cross-talk source under concurrency.**
- Single-run reads at `read_exec_id=None` (no exec context — A1); harmless
  with one execution, but it confirms the scoping is *context-driven* and
  single-run is currently the unqualified case.

**Conclusion — the cross-talk is the control, not the query.** The servo
retargets `concurrency`/`rate`, resolved via `find_control_erased_up` from the
**shared `SESSION_ROOT`** (SRD-88 §3c, deferred). Concurrent executions share
one control instance and fight over it; each phase then runs at a neighbour's
setting, so the (correctly exec-scoped) objective reflects the wrong
coordinate → wrong `best`. **§3c control-rooting is the load-bearing fix for
the walker; §3b matcher-injection is a correctness/principle improvement
(single-run qualification, aggregation-correct vector results, the `session`
label) but does not, by itself, turn the walker green.**

## 3. Design

### 3a. The seam — the execution context carries the execution **component**

The per-execution component **already exists** (`session.rs`: each execution's
component is a child of the session component, labelled with its `exec_id` +
workload — SRD-88 §2). The gap is that nothing routes resolution to it: the
task-local `ExecutionContext` (SRD-88 §3) does not reference it, and the
control path still resolves against the *global* session root.

The seam is therefore: **`ExecutionContext` carries its execution component**
(`Arc<RwLock<Component>>`). That one handle is the single source for both
boundaries — its labels give the `(session, exec_id)` dims the metric path
injects (§3b), and it is the root the control path walks up from (§3c). Both
read it through one resolver hook, the indirection SRD-88 already uses for
`exec_id` (the lower crates don't reach up into nbrs-runtime). This replaces
**two** ad-hoc globals — the metric path's bare `exec_id` hook and the
control path's global `SESSION_ROOT` — with one component-rooted source, so
metrics and controls are scoped by *construction*, from the same anchor.

### 3a-i. Why both must change together

The closed-loop (servo) workload tests need **both** to hold under
concurrency: the servo *writes* a control (`concurrency`/`rate`) and *reads*
the resulting objective (a metric). If the control is shared but the metric is
scoped (today's state), each execution reads its own metric off a phase that a
neighbour's control is driving — still wrong. If the metric leaked but the
control were per-exec, the objective would sum neighbours. Only when **both**
the write target and the read are anchored to the same per-execution component
is the loop self-consistent. That is why this SRD specifies them as one design
off one seam, not two independent fixes.

### 3b. Metric / metricsql — matcher injection (replaces the post-filter)

At the query boundary (`MetricAccess::select_range`, which **both** `metric()`
and the metricsql engine read through), the current execution's dimensional
matchers (`session="…"`, `exec_id="…"`) are `AND`-ed into the matcher set
**before** evaluation. The engine then evaluates `rate()`/`sum()` over only
the scoped series — correct aggregation, not a trimmed result. The
`exec_id` post-filter is deleted. `metric()`, every `metricsql_*`, and
workload reports inherit it through the one boundary.

### 3c. Controls — resolve from the execution component (not the global root)

Today `control(...)` / `control_set(...)` resolve via
`runtime_context::session_root()` → `find_control_erased_up`, where
`session_root()` returns the **global `SESSION_ROOT`** set once at the session
tier. That is the load-bearing cross-talk (§2b): every execution's servo
retargets the *same* control instance.

The fix routes resolution through the seam (§3a): `control_gauge_f64` /
`control_value_string` / `ControlSet` resolve against the **current execution
component** taken from `ExecutionContext`, not the process-global
`SESSION_ROOT`. Because the execution component is a *child* of the session
component, `find_control_erased_up`'s walk still reaches genuinely
session-tier controls (shared by design) while a control a workload declares
in its own scope is owned per-execution. Fallback to `SESSION_ROOT` only when
there is no execution context (and per §3d single-run should establish one, so
that fallback is the bare-CLI case, not workloads).

**Implementation note (2026-06-22) — the obvious read-rooting DEADLOCKS.**
A first attempt routed the control reads (`control_gauge_f64` etc.) through
the per-execution phase component carried on `FiberContext::control_root`
(plumbed and left in place). It **validated the diagnosis** — `control.yaml`
*solo* with synthetic saturation found `best [2]`, proving `control_u64` reads
the servo's live per-exec concurrency once rooted there — but it **hung the
concurrent in-process walker**. Cause: `find_control_erased_up` holds the
component `RwLock` read guard across a nested up-walk (parent reads) while the
cadence path concurrently takes *write* locks to register instruments;
`std::RwLock` is writer-preferring on Linux, so the nested reader starves
(the hazard the memory flags). **The read must not walk the component tree
under lock per-cycle** — resolve to a **lock-free per-exec control handle**
(snapshot the `Arc<dyn ErasedControl>` once when the fiber starts, or hand the
servo and the reads a shared handle directly) instead of re-walking the
contended component `RwLock` on every read. The read-side resolution was
reverted to `session_root()`; only the `control_root` plumbing remains.

Design-first care points (why this is not purely mechanical):
- **Declaration tier.** A workload-declared control must be owned by the
  execution component's subtree, not the session root, or the per-exec walk
  won't find a per-exec instance. Audit where `ControlBuilder…declare(…)`
  attaches (`fiber_pool.rs`, `control_catalog.rs`, the adapter
  `declare_controls`) and re-tier the workload-owned ones.
- **`branch_scope`.** Existing controls set `BranchScope::Subtree`; the
  walk-up + subtree semantics must compose with rooting at the exec component
  rather than the session root (SRD-88 flagged exactly this).
- **`set_session_root` becomes per-tier.** It currently installs the session
  component as the sole resolver backing; it must instead expose the session
  component as the *fallback* root while the per-exec component (via the
  context) is the primary.

This retires the SRD-88 §3c deferral.

### 3c-i. The mechanism — a lock-free per-execution control snapshot

The reads must never re-walk the component `RwLock` tree per cycle (that is the
§3c implementation-note deadlock). Instead, **resolve once into a lock-free
handle snapshot, then read the handle directly**:

**The snapshot.** `ControlSnapshot = Arc<HashMap<String, Arc<dyn
ErasedControl>>>` — control name → its resolved erased handle. Immutable once
built; an `Arc` clone is the only per-fiber cost. A read/write looks up the
name (a `HashMap` get, no lock) and calls `gauge_f64()` / `value_string()` /
`set_f64(…)` on the handle. The handle's *own* state is already concurrency-safe
(the control's versioned value), so **no component `RwLock` is touched on the
hot path** — the deadlock is structurally impossible.

**Built once, non-nested.** The snapshot is built **once per phase** (after
the phase's controls are declared in `attach_component` / `declare_adapter_controls`,
before the op loop spawns fibers), by a new `Component` method that walks **up**
from the phase component **acquiring and releasing each tier's lock
separately** — never holding a child's read guard while reading a parent. This
flattens the up-walk's resolution (per-exec phase controls + inherited
session-tier controls, honoring `BranchScope::Subtree`) into the map, so the
snapshot has the *same* visibility `find_control_erased_up` would, computed
without the nested-read hazard. (`find_control_erased_up` stays for the
out-of-fiber / dryrun paths; only the per-cycle fiber path uses the snapshot.)

**Carried per fiber.** The snapshot replaces `FiberContext::control_root`
(`control_snapshot: Option<ControlSnapshot>`). The executor builds it at phase
setup and the fiber-spawn sites (`activity.rs`) pass the `Arc` clone into
`with_fiber_context`. `control_gauge_f64` / `control_value_string` /
`ControlSet::eval` look up `control_snapshot` first; `None` (no fiber, single
run, dryrun) falls back to the `session_root()` walk.

**Same handle the servo writes.** The servo already resolves off
`phase_component` (`servo.rs::retarget`); the snapshot captures *that* handle,
so a read sees the servo's live retarget without either side touching the
component lock per operation.

This makes the §3c rooting concrete and deadlock-free, and is what unblocks
synthetic saturation (`load := control_u64("concurrency")` tracking the
servo) under the concurrent in-process runtime.

**Push-1 attempt finding (2026-06-22, reverted).** Implemented the snapshot:
`Component::control_snapshot` (the non-nested up-walk — KEPT in the codebase,
it is correct), built once per phase in `run_with_adapters`, carried on
`FiberContext`, with the reads/`control_set` reading it (snapshot-then-
`session_root` fallback). Validated SOLO (`control.yaml` → `best [2]`,
`basic_workload` ok) — **and the build itself is not the problem**: a
file-marker probe confirmed **311/311 builds completed** under the concurrent
walker (the non-nested walk does not deadlock). But the wired-in version still
**hung the concurrent in-process walker** (>120 s vs ~4 s), including for
*measured* examples that never read a control — so the hang is **neither the
build nor the reads** but some interaction the snapshot wiring introduces into
the concurrent runtime (most likely the per-phase build's brief component
read-lock perturbing the session-shared cadence under contention, or the
daemon-fiber snapshot propagation). It was **not pinned**. Reverted to the
functional baseline (`session_root`; walker completes, only `control`/
`multiservo` red). **Next attempt:** either (a) instrument to pin the exact
stall (a per-fiber/cadence marker trace, not just the build), or (b) avoid the
live component entirely — capture each control's handle at **declaration
time** (`attach_component` / `declare_adapter_controls`) into a per-exec
lock-free map, so no phase-time component walk happens under concurrency at
all.

### 3d. No-context ⇒ unconstrained; single-run is **not** special

- A query issued with **no** execution context (a bare `nbrs metrics query`,
  or a session-tier rollup report deliberately run outside an exec scope)
  injects no matcher and sees the whole (single-session) store. This is the
  *only* unconstrained case.
- **Single-run is qualified exactly like multi-run.** A single `nbrs run`
  establishes an execution context (one `exec_id`) and its queries carry the
  `(session, exec_id)` matchers like any execution. Because that run owns
  every series in its store, the qualifier selects all of them — so output
  is unchanged (SRD-88 axiom A1 / single-run byte-identity holds), while the
  *rule* is uniform: if multi-run requires qualification, so does single-run.
  There is no `None`-means-single-run scoping branch.

## 4. Pushes (sequenced)

1. **Control rooting via the lock-free snapshot (load-bearing — turns the
   walker green).** Implement §3c-i: (a) a non-nested `Component`
   up-walk-and-flatten that returns a `ControlSnapshot` (name → handle); (b)
   build it once per phase after controls are declared; (c) carry it on
   `FiberContext` (replace the carried `control_root`); (d) point
   `control_gauge_f64` / `control_value_string` / `ControlSet::eval` at the
   snapshot, `session_root()` fallback. This is the §2b root cause and avoids
   the §3c-implementation-note deadlock; with it `control_u64` reads the
   servo's live per-exec concurrency. **Do NOT route reads through the
   per-cycle component `RwLock` walk** (the reverted first attempt — it
   deadlocks). Verify each step against the walker at `CONCURRENCY=20`.
2. **Synthetic-saturation migration (completes the walker fix).** With Push 1,
   `control_u64("concurrency")` reads the live per-exec setting, so the
   optimizer examples can switch from *measured* saturation (host-dependent —
   no reliable signal under concurrent in-process execution, the actual cause
   of the red examples) to **synthetic**: `load := control_u64("concurrency")`
   + `result-load: "{load}"`, overload when `load > result-overload`. This is
   deterministic and host-independent. **Open sub-issue:** validated for
   single-servo (`control` solo → `best [2]`), but the multi-servo case
   (`multiservo`, `servo: [concurrency, rate]`) read `[32, …]` solo — the
   conc=32 grid point didn't overload, i.e. `control_u64` didn't reflect 32 at
   that point. Diagnose whether the multi-axis retarget timing or the snapshot
   read ordering is the cause before applying synthetic to `multiservo`.
3. **Carry dims in the context.** Capture the execution component's
   `(session, exec_id)` into `ExecutionContext` at establishment; widen the
   read hook from `Option<u64>` (exec_id) to the dimensional label set.
   (Correctness/principle, not the walker fix — the exec_id post-filter
   already scopes today.)
4. **Matcher injection.** Inject the dims at `select_range`; delete the
   `exec_id` post-filter so aggregations (`sum`/`count` over vectors) are
   correct, not just trimmed, and `session` is constrained. Verify `metric()`
   + all `metricsql_*` + reports.
5. **Single-run parity.** Ensure the single-run path establishes an execution
   context so its reads are qualified identically (`read_exec_id=Some`, output
   byte-identical — today single-run reads at `None`).
6. **Drop the walker serial-isolation workaround** (already removed in the
   test; Pushes 1–2 make it *pass*).

## 5. Load-bearing test

- **No isolation, full concurrency.** The example walker at **1 hardware
  thread / 20 concurrent executions** (`NBRS_TEST_WORKER_THREADS=1`,
  `NBRS_TEST_CONCURRENCY=20`): every optimizer example — servo (`control`,
  `multiservo`, `hybrid`) and rerun (`saturation`, `metricsql`) — finds its
  correct `best`, with no serial-isolation grouping. Deterministic across
  repeated runs.
- **Two-execution cross-talk unit.** Two `control.yaml` executions in one
  session, retargeting `concurrency` to different settings: each reads its
  own `errors_total` aggregate and resolves its own control — distinct,
  correct bests. Fails on `main` (shared root + post-filter), passes after.
- **Single-run invariance (A1).** A representative `nbrs run` produces
  byte-identical output to pre-change (the qualifier selects the sole
  execution's full series).

## 6. References

- **SRD-88** Concurrent In-Process Executions — the shared/isolated tiers,
  the `ExecutionContext` seam, the `exec_id` post-filter and the deferred
  `SESSION_ROOT` control note this SRD supersedes; §5b report scoping it
  generalizes.
- **SRD-42** MetricsQuery / cadence pipeline — the component tree + series
  labels that are the dimensional source of truth.
- **SRD-47 / SRD-48** MetricsQL streaming / continuous query — the engine
  that reads through `MetricAccess::select_range`.
- **SRD-86** Optimization — the servo/settle that exposed the cross-talk.
- **SRD-23** Dynamic controls — `control(...)` resolution + `control_set`.
