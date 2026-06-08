# SRD-82 — Uniform Execution Shells (Outcomes, Error Handling, Stop Conditions)

**Status:** DRAFT — the canonical model for how execution levels
report results, handle errors, and decide stop conditions. Supersedes
the ad-hoc, per-level error mechanisms and folds SRD-03 (the error
router) and SRD-76 (phase outcome) into one recursive shape.

**Owner:** nbrs-activity (executor walker, `run_phase`, the activity
cycle/stanza loop), nbrs-errorhandler (the router, generalised),
nbrs-metrics (outcome persistence), nbrs-workload (the per-level
`errors:` configuration surface).

**Cross-refs:**
- [SRD-02](02_concurrency_model.md) — One concurrency path; the
  single walker / fiber harness. SRD-82 says every level of that
  harness is the *same* execution shell.
- [SRD-03](03_error_handling.md) — The error router
  (`pattern:actions` cascade) and op/adapter/stanza error scoping.
  SRD-82 generalises the router to operate at every shell, matching
  on *outcomes*, not only op-error names.
- [SRD-18b](18b_scenario_tree_and_scheduler.md) — The scenario tree
  and the single walker. SRD-82 makes the walker's per-sibling error
  behaviour a configurable shell handler instead of an implicit
  `first_err` rule.
- [SRD-76](76_phase_outcome_disposition.md) — `PhaseOutcome`. SRD-82
  splits its single `PhaseStatus` into two orthogonal axes and makes
  the result the return type of *every* shell, not just phases.
- [SRD-77](77_working_sessions_and_refine.md) — `refine` re-uses a
  prior execution's outcomes. The Interrupted-but-Succeeded vs
  Interrupted-and-Failed distinction below is what tells `refine`
  which partial results are re-usable.

---

## Why this exists

A workload runs through four nested **execution shells**:

```
scenario graph   →   phase   →   stanza   →   op
(execute_tree)       (run_phase)  (cycle loop)  (dispenser + adapter)
```

Today each shell has its *own* error mechanism and its own notion of
"result", and they don't compose:

| Shell | Code today | Error mechanism | "Result" today |
|-------|-----------|-----------------|----------------|
| Scenario graph | `executor::run_siblings_concurrently` | implicit `first_err`; halts *further* dispatch but never aborts in-flight siblings and never consults a stop signal | `Result<(), String>` |
| Phase | `executor::run_phase` | a `stop_flag: AtomicBool` boolean + `stop_reason` string | `PhaseOutcome { status }` |
| Stanza | the per-cycle loop in `activity::executor_task` | inline, per-cycle | none distinct |
| Op | `nbrs_errorhandler::ErrorRouter` | the `pattern:actions` cascade (`count`/`warn`/`stop`/`retry`) | per-op error |

Two concrete failures fall out of this inconsistency:

1. **An invalid config completed the whole run.** Query cells with
   `errors: count` errored on 100% of ops but *completed* — no
   `stop_flag` was set, so `run_phase` returned `Ok`, the walker kept
   going, and the entire sweep ran producing garbage that *looked*
   finished. The QPS axis even rendered the all-error cells as
   ultra-high throughput. Nothing in the model says "a phase that is
   ~all-errors is Failed", and nothing says "a Failed phase stops the
   scenario."
2. **A failed phase does not stop the workload.** The scenario walker
   propagates the first child `Err` only far enough to halt *new*
   serial dispatch; concurrent or already-running siblings finish, and
   there is no "stop on error" default at the scenario level.

The fix is not four more patches. It is one recursive shape — a
**uniform execution shell** with a two-axis **outcome** and a
**handler** (the generalised router) that every level uses the same
way.

---

## Part 1 — Outcomes are two orthogonal axes

`PhaseStatus { Completed, Failed, Skipped, CursorSuspended }` conflates
two independent questions. Split them:

```rust
/// How much of the unit's work ran.
pub enum Disposition {
    Running,        // in flight
    Completed,      // ran to its natural end
    Interrupted,    // stopped before its natural end
    Skipped,        // never started (filtered / unreached)
}

/// Whether the produced result is usable.
pub enum Validity {
    Succeeded,      // result is trustworthy
    Failed,         // result is not trustworthy — do not rely on it
}

/// The result of ANY execution shell.
pub struct Outcome {
    pub disposition: Disposition,
    pub validity: Validity,            // meaningless while Running/Skipped
    pub errors: Vec<ErrorDetail>,      // chronological, SRD-76 shape
    pub resume: Option<ResumeCursor>,  // SRD-44 partial-progress handle
    // …identity / duration / hash carried as in SRD-76…
}
```

The two axes are independent. The meaningful combinations:

| | `Succeeded` | `Failed` |
|---|---|---|
| **`Completed`** | clean result | ran fully, result is garbage — the all-error query cells |
| **`Interrupted`** | partial but **re-usable** (keep what ran) | partial and **discard** |

Consequences the single status couldn't express:

- A user **Ctrl-C** is `Interrupted`; its `Validity` is whatever the
  shell had achieved — often `Succeeded` (keep the partial sweep), and
  a consumer (`refine`, replay) may re-use it. It is NOT inherently
  `Failed`.
- An **error-rate breach** is `Failed`; its `Disposition` is whatever
  it managed — usually `Interrupted` (we stopped it), occasionally
  `Completed` (a short phase finished before we judged it).
- `CursorSuspended` (SRD-44/76) collapses to
  `Interrupted + Succeeded` — re-usable partial progress.

`refine` and any result consumer key off **`Validity`** to decide
trust/re-use and off **`Disposition`** to decide completeness — never
a single overloaded enum.

---

## Part 2 — One execution shell, recursively composed

Every level is the *same* shape:

```rust
/// One execution shell. Scenario graph, phase, stanza, and op are
/// all instances at different granularities.
trait ExecShell {
    /// Run the body (children, or ops) and return this shell's
    /// Outcome. The body feeds each child outcome / op error through
    /// `self.handler`, which decides the per-child action and thereby
    /// this shell's aggregate Disposition + Validity.
    async fn run(&self, ctx: &ShellCtx) -> Outcome;
}
```

A shell does three things, identically at every level:

1. **Body** — run its units (child shells, or the leaf ops). Units run
   serially or concurrently per the *one* concurrency path (SRD-02);
   the shell does not own a private concurrency mechanism.
2. **Handle** — feed each unit's result (a child `Outcome`, or an op
   error) through this shell's **handler** (Part 3). The handler emits
   an **action** that the shell applies to itself and to the rest of
   the body.
3. **Aggregate** — emit this shell's `Outcome`. Its `Disposition` is
   `Completed` if the body ran to its natural end, `Interrupted` if a
   handler action stopped it early. Its `Validity` is `Succeeded`
   unless a `fail` action fired (or a child's `Failed` propagated per
   policy). That `Outcome` is, in turn, one *unit result* fed to the
   parent shell's handler.

The recursion is the whole point: a phase is to its stanzas what the
scenario graph is to its phases. The op shell's "child results" are op
errors; every other shell's "child results" are `Outcome`s.

### The four shells as instances

| Shell | Body (its units) | Configured by |
|-------|------------------|---------------|
| **Scenario graph** | child scopes / phases | workload / scenario-level `errors:` |
| **Phase** | its stanza sequence + aggregate op-error conditions | per-phase `errors:` |
| **Stanza** | the linearized op chain (SRD-02) | per-stanza `errors:` |
| **Op** | the single adapter call | the op's resolved error class (leaf) |

This is exactly the hierarchy of handlers the design asks for: the
scenario-graph handler is configured at the scenario level, the phase
handler per phase def, the stanza handler per stanza def, the op
handler per op — each interpreting the results one level down.

---

## Part 3 — The handler is the generalised router

SRD-03's router (`"<pattern>:<actions>;…"`, first-match-wins) is
already the right abstraction. Generalise it from *op-error-name → op
verb* to *unit-result → shell action*.

### Match keys (what the left side matches)

A handler rule matches a **unit result**. The vocabulary widens as it
moves up the shells, but the grammar is identical:

- **Error class** (regex on `error_name`) — the op-leaf key, unchanged
  from SRD-03: `Timeout`, `cql_error`, `.*`.
- **Child validity** — `Failed` / `Succeeded` (matches a child shell's
  `Outcome.validity`). This is how a parent reacts to a failed child.
- **Aggregate condition** — evaluated over the body's running tally,
  not a single unit: `rate>0.1` (errored fraction of ops),
  `count>N`, `consecutive>K`. This is what makes the error-rate
  breach declarative rather than a hardcoded loop check.

### Actions (what the right side does)

| Action | Meaning | Effect on this shell |
|--------|---------|----------------------|
| `count` | tally to a metric; keep going | none |
| `warn` | log at Warn; keep going | none |
| `ignore` | explicit no-op (SRD-03 invariant: never a default) | none |
| `retry` | re-run the failing unit (op or child) | none, on success |
| `fail` | the result is not trustworthy | `Validity = Failed` |
| `stop` | halt the rest of the body now | `Disposition = Interrupted` |

`fail` and `stop` are orthogonal — the two-axis outcome demands it.
`fail` without `stop` lets a unit keep running but marks the result
untrustworthy; `stop` without `fail` is a clean early halt (a user
interrupt); `fail,stop` is the common "this is broken, abandon it."

### Defaults (the load-bearing part)

- **Scenario-graph handler default: `*Failed : stop`.** A child phase
  whose `Validity` is `Failed` halts the remaining scenario body and
  Interrupts it — *stop on error is the default scenario-graph
  behaviour*, expressed as one rule, not special-cased walker code.
- **Phase handler default** keeps SRD-03's op cascade
  (`.*:warn,stop`) *plus* an aggregate breach rule
  `rate>{error_rate_max} : fail,stop` (default `error_rate_max=0.1`,
  evaluated after a minimum op count). The error-rate circuit breaker
  is therefore one phase-handler rule.
- **Stanza handler default** is the SRD-02 linearization response: an
  op error fails the dependent chain (`.*:fail` for the chain),
  surfaced to the phase handler.
- **Op handler** is the adapter's resolved error class — the leaf.

Both features the design started from collapse into the grammar:

```
# error-rate circuit breaker  →  a phase rule
phase.errors = "rate>0.1:fail,stop"

# stop-on-error                →  the scenario-graph default
scenario.errors = "*Failed:stop"        # implicit unless overridden
```

---

## Part 3a — `ErrorPolicy`: the handler is its own resolver

The handler at a shell and the *resolver* that provisions its
children's handlers are the **same type** — an `ErrorPolicy`. There is
no separate dispenser service; consolidating them removes a parallel
object to thread.

```rust
pub struct ErrorPolicy {
    config: PolicyConfig,        // value-equality key (error_spec + error_rate_max)
    pub router: ErrorRouter,     // op-error handling
    pub guard: AggregateGuard,   // aggregate (rate>N) handling
    derived: Mutex<HashMap<PolicyConfig, Arc<ErrorPolicy>>>,  // breadth cache
}
```

Resolving a shell's policy is an **init-time** action with two
moments — both needed, both living in `resolve_child`:

- **Depth (on descend):** a child shell inherits its parent's policy
  unless it overrides. `resolve_child(None)` — or a config equal to the
  parent's — returns the parent `Arc` itself. Inheritance is sharing by
  reference; no new instance.
- **Breadth (within a layer):** sibling shells that override with the
  *same* config get one derived policy, deduplicated by the config's
  **value equality** (the `derived` cache on the parent). A 200-cell
  sweep that shares one `errors:` parses the spec once.

Crucially, resolution happens **once per shell, at scope-init**: the
shell binds the resolved `Arc<ErrorPolicy>` and holds it. There is no
re-resolution *within* a shell after init (no per-cycle, per-fiber
lookup). The session holds the **root** policy (the default); each
shell's policy is `parent.resolve_child(own_config)`.

This answers "one instance per unique configuration" with two
mechanisms rather than two caches: depth-inheritance shares the
ancestor's instance down an unbroken chain; breadth-dedup shares one
derived instance across a layer. The per-node "no re-init" is not a
cache at all — it is the shell holding its bound policy.

```text
session root  ── resolve_child ─▶  scenario policy  ── resolve_child ─▶  phase policy …
   (default)        (inherit / derive, value-equality shared, bound once at scope-init)
```

---

## Part 4 — Stop propagation and abort

When a handler emits `stop`/`fail`, the shell must (a) stop dispatching
new units and (b) abort in-flight concurrent units — *with the right
disposition*. This reuses the existing plumbing, corrected on two
points:

1. **A failed-stop carries `Failed`, not `Interrupted`.** Today the
   only session-wide stop is the Ctrl-C path (`session_signals::
   request_stop`), which marks the run interrupted. A handler `stop`
   from a `fail` rule must propagate a *Failed* disposition so the run
   is recorded `Interrupted + Failed`, distinct from a user Ctrl-C
   (`Interrupted + Succeeded`/policy). The stop signal therefore
   carries a `StopCause { Interrupt | Fault }` rather than a bare
   boolean.
2. **The walker consults the stop and aborts in-flight.**
   `run_siblings_concurrently` must, on a `stop` action (or an
   observed `StopCause`), both stop dispatching siblings *and* signal
   in-flight sibling shells to abort at their next cooperative
   boundary — the same `stop_flag` fibers already poll (activity.rs),
   now sourced from the shell handler rather than only the op router.

A shell never force-kills; abort is cooperative at the existing cycle /
sibling boundaries (SRD-02). In-flight units that reach their boundary
after the stop report `Interrupted`.

---

## Part 5 — Mapping to code + migration

Incremental; each step compiles and is independently testable.

1. **`Outcome` type.** ✅ TYPES DONE; ⏳ storage swap pending.
   `Disposition` + `Validity` + `Outcome` landed in `phase_outcome.rs`
   with the bidirectional projection (`Outcome::to_status` /
   `PhaseStatus::to_outcome`) and `PhaseOutcome::outcome()`. The axes
   are the canonical *type*; the **storage swap** — making
   `disposition`/`validity` the stored fields on `PhaseOutcome` and
   demoting `status` to a derived projection (so producers can set
   `Completed+Failed` vs `Interrupted+Failed` distinctly) — is the
   mechanical follow-up (~73 `.status` sites). Until then every
   producer goes through the standard mapping
   (`Completed→Completed+Succeeded`, `Failed→Interrupted+Failed`,
   `Skipped→Skipped`, `CursorSuspended→Interrupted+Succeeded`).
2. **Generalise the router.** Extend `nbrs_errorhandler::ErrorRouter`
   to match `child validity` and `aggregate` keys and to emit `fail` /
   `stop` actions, alongside the existing op verbs. Keep first-match
   semantics.
3. **Phase shell.** ✅ DONE. `AggregateGuard`
   (`nbrs-errorhandler/src/aggregate.rs`) holds the `rate>N:fail,stop`
   rule; `ErrorPolicy` (`nbrs-activity/src/error_policy.rs`, Part 3a)
   composes it with the op router; the phase shell's drain loop
   delegates the breach decision to `error_policy.guard.assess(cycles,
   errors)` rather than testing a hardcoded threshold. `run_phase`
   binds the phase policy via `ctx.error_policy.resolve_child(...)`; the
   runner seeds the session root. The breach is now a composed rule.
4. **Scenario shell.** ⏳ PENDING. Give `run_siblings_concurrently` the
   scenario policy; apply `*Failed:stop` by default; thread
   `StopCause::Fault` so a fault stop both halts dispatch and aborts
   in-flight siblings, recording `Interrupted + Failed`. **A failed
   phase does not yet stop the workload** — this is the live gap.
5. **Stanza shell.** ⏳ PENDING. Fold the per-cycle stanza error
   handling into the same shape so the stanza's `errors:` configures it.
1. **`Outcome` type.** ⏳ PENDING. Two-axis `(Disposition, Validity)`
   not yet landed; until it does, a guard `fail` is subsumed by the
   `stop → run_phase Err → PhaseOutcome::failed` path (which already
   stops *serial* successor phases).

Steps 1–2 (`Outcome` type, generalised router) and 4–5 remain. The
phase-shell breach is no longer a hardcoded stopgap — it is the
composed `ErrorPolicy`/`AggregateGuard` rule. The remaining gap with
operator-visible impact is step 4: scenario-level stop-on-error.

---

## Invariants (axioms this SRD adds)

- **Every execution level is the same shell.** Scenario, phase,
  stanza, op differ only in granularity and configured handler — never
  in mechanism. A new bespoke per-level error path is a regression.
- **Result = (Disposition, Validity), always two axes.** No single
  status enum mixing "how much ran" with "is it usable." Consumers key
  off the axis they care about.
- **Errors and stops are handler decisions, not control-flow
  specials.** "Stop on error", "fail on >N% errors", "retry timeouts"
  are all rules in one router grammar, configured at the shell whose
  children produced the result.
- **`stop` carries a cause.** `Interrupt` (clean, user) vs `Fault`
  (a `fail`-driven stop) are distinct so the run's `Validity` is
  recorded correctly and partial results can be re-used or discarded
  deliberately.
