# SRD-78 — PolyStreamer: Comprehension-Sourced Scope Streams as a First-Class Polydat Type

> **Ownership note:** Comprehension semantics (constructors,
> validity axioms, optimization, IR, consumption surfaces) are
> owned by the polydat comprehension spec at
> `polydat/docs/design/comprehension_forms.md`. This SRD owns
> the **runtime that hosts** the consumption surfaces polydat
> spec §9.5 defines: `CoordinateStream`, `ScopedKernelStream<K>`,
> and the one-shot `scope_once(parent, coords)` function. The
> compiled IR (polydat spec §9.1) is the shared resource;
> SRD-78 owns the per-streamer dispense state, concurrency
> model, and lifecycle. Where this SRD describes "what
> comprehensions mean", the meaning is polydat's; SRD-78
> describes "how the runtime hosts them."

**Status:** DRAFT — design for the runtime that hosts polydat's
consumption surfaces (polydat spec §9.5). No code lands until
reviewed against SRD-18b (scope tree + scheduler), SRD-13c
(GK scope model), SRD-13e (scope as module), and SRD-67
(parent-gated sub-context construction).

**Owner:** polydat (grammar, type system, comprehension
algebra, compiled IR, consumption-surface contracts — see
polydat spec §3–§10), nbrs-runtime (consumer migration —
`dispatch_comprehension`, `polydat::comprehension::runtime::evaluate_for_iteration`,
the executor's per-iteration kernel construction),
nbrs-workload (YAML→PolyStreamer-binding desugaring).

**Cross-refs:**
- **`polydat/docs/design/comprehension_forms.md`** — the
  authoritative comprehension reference. This SRD implements
  the runtime contracts polydat spec §9.5 ("Consumption
  surfaces") defines. The three runtime types this SRD
  describes (`CoordinateStream`, `ScopedKernelStream<K>`,
  `scope_once`) are direct realizations of the §9.5 surfaces;
  their independence contract (polydat spec §9.5.2) is what
  SRD-78's "one streamer per Arc, each with its own dispense
  cursor" model implements.
- [SRD-18b §"The Comprehension Model"](18b_scenario_tree_and_scheduler.md)
  — scenario-tree integration of polydat comprehensions and
  the executor-side `dispatch_comprehension` driver that
  PolyStreamer replaces.
- [SRD-18c](18c_comprehension_syntax.md) — parser-layer grammar
  that produces polydat ASTs. PolyStreamer is the runtime
  *value* a comprehension binds to, not a replacement for the
  clause syntax.
- [SRD-18d](18d_comprehension_traversal_order.md) — per-strategy
  algorithmic detail. The PolyStreamer's emission order is
  determined by the comprehension's compiled IR (polydat spec
  §9.1 + §10.2 R2 push-down rules).
- [SRD-18e](18e_comprehension_canonical_reference.md) —
  redirect stub. Material formerly here is now in the polydat
  spec; cross-references to SRD-18e should target the polydat
  spec directly.
- [SRD-13c](13c_polydat_scope_model.md) — what a Polydat scope is (kernel
  + bound inputs + scope coordinates). PolyStreamer's `pull`
  yields one of these (specifically, a
  `ScopedKernelInstance<PolydatKernel>` per polydat spec §9.5.3).
- [SRD-13e](13e_scope_as_module.md) — typed import/export
  contracts. The clauses' LHS variable names are the exported
  scope coordinates; the streamer's bound-extern semantics
  match this contract.
- [SRD-67](67_polydat_subcontext_construction.md) — parent-gated
  sub-context construction (`SubcontextBuilder`, `ScopeKernel`).
  PolyStreamer's `scope_once` (polydat spec §9.5.3) is the
  unit-of-work that maps a coordinate tuple to a
  `ScopedKernelInstance<PolydatKernel>`; it shares the
  parent-gating discipline with `SubcontextBuilder`.
- [polydat comprehension spec](../../polydat/docs/design/comprehension_forms.md)
  — the completed migration moved the AST + parser + evaluator
  + synthesizer into polydat. PolyStreamer is the next phase: a
  runtime value with grammar-level identity, lifting the type
  itself into GK.

**Audit alignment note (2026-05-28):** This SRD predates the
polydat spec's §9.5 consumption-surfaces split (defined as part
of polydat's F26 push). The current SRD-78 text often discusses
"PolyStreamer" as a single type that dispenses Polydat kernels;
under §9.5 there are *three* concrete surfaces over the shared
IR — `CoordinateStream` (dispenses coordinate tuples),
`ScopedKernelStream<K>` (dispenses scoped kernel instances),
and `scope_once` (one-shot). Where this SRD describes
"PolyStreamer.pull yields a PolydatKernel" the underlying type is
`ScopedKernelStream<PolydatKernel>`. The §"API surface" and §"Lock-
free internals" sections need a follow-up update push to
explicitly distinguish the two stream types; until that push
lands, treat the existing SRD-78 narrative as describing
`ScopedKernelStream<PolydatKernel>` with the understanding that an
analogous `CoordinateStream` exists alongside it with the same
internal mechanics.

---

## Why this exists

The comprehension model already lives in polydat
(`polydat::comprehension::{ast, parse, eval, synthesis, iteration}`).
Polydat owns "what a comprehension means" — the AST, the
tuple-enumeration pipeline, the per-iteration kernel construction,
the canonical scope-coordinate names.

But comprehensions today are still **not first-class values in the
GK language**. They're a pre-execution structural concept whose
runtime form is `ScopeIterations: Iterator<Item = IterationStep>`
— a Rust iterator constructed by the executor and immediately
consumed. There's no way to:

- Write `myscope := for k in 1..10, profile in {profiles}` in GK
  source and have `myscope` become a value the kernel can pass
  around.
- Hold a reference to a still-emitting comprehension from another
  Polydat binding or a Rust caller and ask "how many tuples have been
  dispensed? what's the next one?" mid-flight.
- Express "give me the next scope context from this comprehension"
  as a uniform operation across deterministic single-pass and
  unbounded sources (cursor-driven, time-driven, externally
  ordered).
- Share one comprehension's stream across multiple consumer
  fibers safely, without each fiber re-enumerating the
  Cartesian product or holding a lock on the iterator.

The current executor handles the first three by walking the
scenario tree itself and pulling tuples out of a freshly-built
`ScopeIterations` per phase. The fourth is sidestepped by giving
each fiber its own `IterationStep` from a pre-materialised tuple
list (`.collect()` before fan-out).

Three forces want a first-class type:

1. **Workload-author expressiveness.** Authors that need a stream
   of pre-derived scopes — synthetic load shapes, replay-driven
   coordinate sets, externally-paced sweeps — currently can't
   express them inside GK. They have to drop down to the scenario
   tree, which is a different language with different mechanics.

2. **Lock-free concurrent dispensing.** SRD-02's "no blocking in
   async contexts" rule has been violated repeatedly when
   per-phase iterators get wrapped in Mutex<...> to share across
   fibers. The fix is structural: the streamer itself is
   lock-free, multiple references to it share one atomically-
   advanced position, and the next-tuple read is wait-free.

3. **Observable mid-flight state.** SRD-44 (checkpoint event log),
   SRD-76 (PhaseOutcome), SRD-77 (refine) all want to know "what
   coordinate is this scope currently on?" and "how many cells
   have it dispensed so far?" These questions exist for every
   comprehension scope in the scenario tree today; the current
   answer is "walk the SceneTree" which is a different layer
   indirecting to the same underlying state. PolyStreamer
   centralizes the answer in the streamer itself.

---

## What PolyStreamer is

A **PolyStreamer** is a runtime polydat value carrying:

- The compiled comprehension (clauses, mode, filter, order — the
  `Comprehension` AST that already exists).
- A monotonic dispense cursor (which tuple position is "next").
- Bounded / unbounded knowledge (Cartesian-product size, union
  total, or `None` for streaming sources).
- A parent-kernel reference for binding inherited externs at
  dispense time.
- A typed per-tuple kernel constructor (the SRD-13f Gate 2
  `IterationKernelFn` already in place).
- A handful of atomic counters for observability (total dispensed,
  last coordinate, next coordinate).

**It is a value with reference semantics.** The streamer itself
cannot be cloned; what callers hold is an `Arc<PolyStreamer>`-
shaped reference (concrete shape below). Multiple references to
the same streamer all advance the same internal cursor — there's
exactly one stream, observed from many viewpoints.

**It is a Polydat port type.** A binding `s := for k in 1..10` produces
a value of Polydat type `PolyStreamer`; subsequent expressions can
reference `s` by name, pass it to functions that accept
`PolyStreamer`, or pull from it via the `pull_scope(s)` node.

**It is the only public iteration surface.** The Rust-side
`ScopeIterations`-as-Iterator stays as an internal implementation
detail of one of the streamer's strategies; everything external
goes through the streamer.

---

## Two creation surfaces

### Surface 1: GK-level wire assignment (declarative)

In Polydat source, the comprehension keyword on the RHS of `:=`
produces a `PolyStreamer`:

```text
input cycle: u64

// Bounded Cartesian streamer
k_stream := for k in 1..10

// Multi-clause Cartesian streamer
sweep := for k in 1..10, profile in {profiles}

// Union streamer (bracketed string list of sub-spaces)
union := for [
    "k in 10, limit in 10, 20",
    "k in 100, limit in 100, 200",
]

// With filter + order
boundary := for k in 1..10, limit in 1..10
            where {k} * {limit} <= 50
            order extrema/3
```

**One keyword, RHS-shape detection.** Polydat expression form uses a
single `for` keyword. The four scenario-tree-layer keywords from
SRD-18c (`for_each`, `for_combinations`, `for_each_union`,
`for`) were a YAML-side disambiguation artifact — the YAML
parser couldn't always tell sub-space lists from clause lists
without a hint. In Polydat expression form the RHS shape is
unambiguous, so one keyword carries every mode:

| RHS shape | Mode emitted |
|---|---|
| `for <var> in <expr>` | Cartesian, one clause |
| `for <var> in <expr>, <var> in <expr>, ...` | Cartesian, multi-clause |
| `for [ "...", "...", ... ]` (bracketed string list) | Union (each string = one sub-space) |
| `for <flat-clause-list>` where var names repeat | Union (inferred per polydat spec §8.4) |

The YAML scenario-tree layer keeps its existing four-keyword
surface (SRD-18c unchanged). Only the Polydat expression form
unifies on `for`.

**Type inference**: the Polydat compiler recognises the `for`
keyword on a binding RHS as producing a wire of port type
`PolyStreamer`. The downstream wires reference `k_stream` /
`sweep` / etc. by name like any other Polydat wire.

**Scope-coordinate semantics inside a pulled context**: when a
consumer pulls a sub-scope from `sweep`, the resulting child
kernel has `k` and `profile` bound as scope coordinates (per
SRD-13c). The comprehension's clause LHS names become the child's
extern names — the exact same contract that today's
`polydat::comprehension::runtime::evaluate_for_iteration`
provides via the typed `(name, Value)` bindings on each
returned tuple.

**Reference, not value**: `k_stream` doesn't materialise the
1..10 list at definition time. The Cartesian-product size is
known (= 10) but no tuple is generated until something pulls.
The wire holds an `Arc<PolyStreamer>` shape; multiple downstream
bindings that reference `k_stream` share one streamer state
(important for observable counters and lock-free dispense
ordering).

### Surface 2: Rust-side method on `PolydatKernel` (programmatic)

For Rust callers that already hold a kernel and need a streamer
without round-tripping through Polydat source:

```rust
let parent: Arc<PolydatKernel> = /* outer-scope kernel */;
let streamer = parent.streamscopes("for k in 1..10, profile in {profiles}")?;
//  streamer: Arc<PolyStreamer>
```

The string argument is parsed by polydat's existing
`comprehension::parse::parse_comprehension_text`; the resulting
`Comprehension` is bound to `parent` as its scope context.

**The returned type is the pure-Rust `Arc<PolyStreamer>` form** —
not a Polydat wire. The two creation surfaces converge on the same
runtime value; the only difference is whether the streamer is
addressable by name from Polydat source (Surface 1) or only from Rust
(Surface 2).

---

## API surface (PolyStreamer)

```rust
/// Stream of scope contexts dispensed in comprehension-declared
/// order. See SRD-78.
///
/// Constructed via [`PolydatKernel::streamscopes`] or as the value of
/// a Polydat comprehension binding. Cannot be cloned — callers share
/// access via [`Arc<PolyStreamer>`].
pub struct PolyStreamer {
    // -- private --
    // No public field surface; all access goes through methods.
}

impl PolyStreamer {
    // ── Pull next subscope ─────────────────────────────────

    /// Atomically claim the next subscope context. Returns
    /// `None` when the streamer is bounded and exhausted.
    /// Lock-free; safe to call from many fibers concurrently —
    /// each caller gets a distinct context in the streamer's
    /// declared order.
    pub fn pull(self: &Arc<Self>) -> Option<ScopeHandle>;

    /// Variant that blocks (in async-aware fashion — never
    /// holds a sync lock past a yield point) until either a
    /// new context is available (unbounded streamers waiting
    /// on upstream production) or the streamer terminates.
    /// Bounded streamers behave identically to `pull` —
    /// `pull_async` only differs when the streamer's source is
    /// itself wait-driven.
    pub async fn pull_async(self: &Arc<Self>) -> Option<ScopeHandle>;

    // ── Observable state ───────────────────────────────────

    /// Total number of contexts dispensed via [`Self::pull`]
    /// (and [`Self::pull_async`]) since construction.
    /// Monotonic; reads are atomic and never block writers.
    pub fn dispensed(&self) -> u64;

    /// Coordinate of the most recently dispensed context, or
    /// `None` if no context has been dispensed yet.
    /// Snapshot — by the time the caller reads it, another
    /// `pull` may have already advanced `dispensed`. The
    /// stale-read contract is documented; callers that need
    /// a consistent (dispensed, last_coord, next_coord)
    /// triple read [`Self::snapshot`].
    pub fn last_coord(&self) -> Option<ScopeCoord>;

    /// Coordinate of the context the NEXT `pull` will return,
    /// or `None` if exhausted or if computing the next
    /// coordinate would require advancing state (unbounded
    /// streamers).
    pub fn next_coord(&self) -> Option<ScopeCoord>;

    /// Atomically consistent snapshot of all three observable
    /// counters. The values are coherent with respect to one
    /// internal dispense epoch; concurrent pulls landing
    /// between snapshot reads of individual fields would
    /// otherwise produce inconsistent triples.
    pub fn snapshot(&self) -> StreamerSnapshot;

    /// Total cardinality of the streamer's space, if known.
    /// Returns `Bounded(n)` for Cartesian (= product of clause
    /// sizes), Union (= sum of sub-space products), and
    /// filtered-Cartesian where the filter has been evaluated
    /// over the full lattice (eager filter). Returns
    /// `Unbounded` for source-driven and time-driven streams.
    /// Returns `BoundedAtMost(n)` for filtered cases where the
    /// upper bound is known but the actual count depends on
    /// per-tuple filter evaluation (lazy filter).
    pub fn cardinality(&self) -> Cardinality;

    /// The comprehension AST this streamer was built from.
    /// Read-only; the comprehension's clauses / mode / filter /
    /// order are immutable after streamer construction.
    pub fn comprehension(&self) -> &Comprehension;

    /// The parent kernel this streamer dispenses contexts from.
    /// Each pulled context's scope chain has `parent_kernel()`
    /// as its immediate parent.
    pub fn parent_kernel(&self) -> &Arc<PolydatKernel>;
}

#[derive(Debug, Clone)]
pub struct StreamerSnapshot {
    pub dispensed: u64,
    pub last_coord: Option<ScopeCoord>,
    pub next_coord: Option<ScopeCoord>,
    pub cardinality: Cardinality,
    pub exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    /// Exactly `n` contexts. Pull `n+1` returns `None`.
    Bounded(u64),
    /// At most `n` contexts; the actual count depends on
    /// filter survival. Pull yields a subset of the lattice.
    BoundedAtMost(u64),
    /// No upper bound declared; pull may yield indefinitely
    /// (or block on upstream production for async pulls).
    Unbounded,
}

/// Handle to one dispensed context. Carries the bindings, the
/// per-iteration kernel, and the coord path — same shape as the
/// existing `IterationStep`, but with the addition of a back-
/// reference to the dispensing streamer for observability.
pub struct ScopeHandle {
    pub bindings: Vec<(String, Value)>,
    pub kernel: Arc<PolydatKernel>,
    pub coord_path: Vec<ScopeCoord>,
    /// Sequence number of this dispense within the streamer.
    /// 0-indexed; equals `streamer.dispensed() - 1` immediately
    /// after the pull that produced it.
    pub seq: u64,
    /// Weak reference to the streamer that produced this handle.
    /// Used by observers (the TUI, replay tooling) to walk back
    /// to the source of truth for cardinality / next_coord
    /// queries; not load-bearing for normal execution.
    pub source: Weak<PolyStreamer>,
}

impl PolydatKernel {
    /// Rust-side entry point for creating a streamer (Surface 2).
    /// `comprehension_text` is parsed via
    /// `polydat::comprehension::parse::parse_comprehension_text`;
    /// the resulting AST is bound against `self` as the parent
    /// scope.
    pub fn streamscopes(
        self: &Arc<Self>,
        comprehension_text: &str,
    ) -> Result<Arc<PolyStreamer>, String>;
}
```

### Why the surface looks like this

- **`Arc<PolyStreamer>` is the only handle type.** Owners can
  store one in a struct field, pass one to a fiber, install one
  on a Polydat wire. Cloning the `Arc` is what produces "multiple
  references to one streamer." Cloning the inner `PolyStreamer`
  is not exposed — the streamer is structurally a single thing.
- **`pull` returns `ScopeHandle`, not `IterationStep`.** Same
  payload but with `seq` + `source` added. `IterationStep` stays
  as the lower-level value type returned by the still-existing
  `ScopeIterations` iterator; `ScopeHandle` is the dispense-
  observable wrapper.
- **`snapshot()` is the consistent-read primitive.** The
  individual `dispensed` / `last_coord` / `next_coord` accessors
  are convenient but read independently — observers needing a
  coherent triple call `snapshot()`. The atomic seqlock
  underneath (see §"Implementation") ensures the snapshot is
  taken from one consistent epoch.
- **`cardinality()` distinguishes three cases.** `Bounded(n)` is
  the common case (Cartesian, Union, filter-eager). `BoundedAtMost(n)`
  signals "we know the lattice size but the actual dispensed
  count depends on filter survival" — the difference matters
  for progress reporting (a 90%-complete bar based on `n` would
  overshoot when filter survival is < 100%). `Unbounded`
  declares "no upper bound" up front.

---

## Polydat type system integration

### Port type

Polydat's `PortType` enum (in `polydat/src/node.rs`) gains a new
variant:

```rust
pub enum PortType {
    U64, F64, Bool, Str, Bytes, Json,
    U32, I32, I64, F32,
    Ext, Handle,
    VecF32, VecI32,
    // NEW:
    /// SRD-78 — `PolyStreamer` value type. Carried as an
    /// `Arc<PolyStreamer>` in the runtime `Value::Streamer`
    /// variant; reference-semantic, non-cloneable inner.
    Streamer,
}
```

The corresponding `Value` enum variant:

```rust
pub enum Value {
    // ... existing variants ...
    /// SRD-78 — reference to a polystreamer. The contained
    /// `Arc` makes sharing free; the streamer's atomic
    /// internals make concurrent pulls safe.
    Streamer(Arc<PolyStreamer>),
}
```

`Value::Streamer` carries an `Arc<PolyStreamer>` so cloning a
`Value` is cheap (one atomic increment) and never duplicates the
streamer state.

### Grammar extension

The Polydat DSL parser (`polydat/src/dsl/`) already understands
comprehension keywords at the *scenario-tree* layer (where
comprehensions currently live). SRD-78 promotes the comprehension
construct to **expressions** the binding-RHS parser also
recognises — collapsed to a single keyword `for` per the
"one keyword, RHS-shape detection" rule above:

```text
// Layer 1 form (single clause)
s := for k in 1..10

// Multi-clause
s := for k in 1..10, profile in {profiles}

// With order / filter
s := for k in 1..10
     where {k} > 3
     order halton/5

// Union form (bracketed string list)
s := for [ "k in 10, limit in 10,20",
           "k in 100, limit in 100,200" ]
```

The expression form lowers to the same `Comprehension` AST the
scenario-tree form already produces. Parse-time validation
(order-with-Union, missing names in filter, etc.) is unchanged.

The compiler emits a node whose output is a `Value::Streamer`
wrapping the constructed `Arc<PolyStreamer>`. The streamer's
parent kernel is the binding's enclosing scope kernel — same
contract as any other binding's `extern` semantics.

### `pull_scope` node

A new stdlib node provides the consume primitive at Polydat level:

```text
input cycle: u64
sweep := for k in 1..10, profile in {profiles}

// Pull one tuple per cycle. The returned context's bindings
// (`k`, `profile`) are visible inside the body of the pull.
ctx := pull_scope(sweep)
```

Signature: `pull_scope(streamer: Streamer) -> Context`, where
`Context` is a new Polydat type representing a pulled child scope.
Bindings made inside the puller's evaluation block see the
context's `k` / `profile` as if they were inherited externs.

`pull_scope` is a context-node (non-deterministic — each call
advances the streamer). In strict mode it requires explicit
acknowledgement, same as other non-deterministic nodes.

### `Context` type

The output of `pull_scope` is a `Value::Context(Arc<ScopeHandle>)`
— the Rust-side `ScopeHandle` lifted into GK. Operations on it:

- Direct binding-RHS reference resolves clause-bound names
  through the handle's kernel (`{k}` inside an expression body
  that came from `pull_scope(sweep)` resolves to the dispensed
  k value).
- `coord_of(ctx)` returns the coordinate as a value the
  workload can format.
- `seq_of(ctx)` returns the dispense sequence number.

---

## Implementation: lock-free internals

### Position state

The streamer's dispense position is a single `AtomicU64` cursor
indexed against a pre-enumerated tuple list:

```rust
struct PolyStreamerInner {
    comprehension: Comprehension,
    parent: Arc<PolydatKernel>,
    canonical: Arc<PolydatKernel>,
    // Pre-materialised tuple list for Cartesian / Union /
    // eager-filtered cases. Built once at streamer
    // construction.
    tuples: Vec<Vec<(String, Value)>>,
    // Per-iteration kernel constructor (SRD-13f Gate 2).
    build_iteration_kernel: Mutex<Option<IterationKernelFn>>,
    // Atomic dispense cursor. `pull` does
    // `fetch_add(1, AcqRel)` and reads `tuples[old]`.
    cursor: AtomicU64,
    // Observability counters — written atomically by the
    // pull path under a seqlock so the `snapshot()` reader
    // sees a coherent triple.
    obs: AtomicSeqlock<ObservableState>,
}

struct ObservableState {
    dispensed: u64,
    last_coord: Option<ScopeCoord>,
    next_coord_hint: Option<ScopeCoord>,
}
```

**Why eager pre-materialisation.** The Cartesian-product walk
already needs all earlier clauses' values resolved before later
clauses can evaluate (dependent-tuple shape — clause N's
expression can reference clause M's bound name for M < N). The
existing `enumerate_tuples` already produces a `Vec<Vec<(String,
Value)>>`. Reusing it means the streamer's per-pull work is
just an atomic increment + a slice index + per-iteration kernel
construction.

**The `Mutex<Option<IterationKernelFn>>` is the one lock.** It
protects `Option::take`-style consumption of a single FnMut
closure that the activity layer installs at streamer
construction. The lock is held for one closure call per pull;
the closure itself doesn't block (it's a pure kernel-building
function that does Polydat source synthesis + compile + bind). For
the common case where multiple fibers pull concurrently from
the same streamer, the lock is the bottleneck — but the
critical section is small (microseconds) and the alternative
(per-fiber closure clones with their own state) is heavier.

For streamers that don't need a per-iteration kernel constructor
(legacy `PolydatKernel::for_iteration` path), the mutex is never
contended.

### Unbounded sources

For source-driven streamers (an externally-paced stream, a
cursor-extending source per SRD-71), the implementation is a
producer/consumer queue rather than a pre-materialised vec:

```rust
struct PolyStreamerInnerUnbounded {
    comprehension: Comprehension,
    parent: Arc<PolydatKernel>,
    canonical: Arc<PolydatKernel>,
    // crossbeam channel: producer pushes tuples; pullers
    // pop. Fully lock-free under uncontested operations.
    queue: crossbeam_queue::SegQueue<Vec<(String, Value)>>,
    // Producer's terminal signal — set when the upstream
    // declares end-of-stream. Pullers reading after this
    // and finding an empty queue return `None`.
    terminated: AtomicBool,
    cursor: AtomicU64,
    obs: AtomicSeqlock<ObservableState>,
}
```

`pull` pops from the queue; `pull_async` awaits on an
async-aware notify if the queue is empty and the stream isn't
terminated.

The producer side is opaque to consumers — it's set up by
whoever constructs the streamer (the scenario-tree executor for
source-driven phases, a future cursor-extending streamer
factory for SRD-71-style cases).

### Snapshot consistency

The `ObservableState` is wrapped in a sequence-locked atomic so
`snapshot()` reads a coherent triple even under heavy concurrent
`pull` traffic:

```rust
impl PolyStreamer {
    pub fn snapshot(&self) -> StreamerSnapshot {
        let obs = self.inner.obs.load();  // seqlock read loop
        StreamerSnapshot {
            dispensed: obs.dispensed,
            last_coord: obs.last_coord.clone(),
            next_coord: obs.next_coord_hint.clone(),
            cardinality: self.cardinality(),
            exhausted: obs.dispensed >= self.inner.tuples.len() as u64,
        }
    }
}
```

The seqlock retry path is bounded — pull writers update obs
once per pull (microseconds), so a reader's retry loop is
essentially never more than 1-2 spins.

For external observers (TUI, replay tooling), `snapshot()` is
the supported API; the individual accessors exist for
convenience when a coherent triple isn't needed.

### Cloning policy

`PolyStreamer` does NOT implement `Clone`. The struct's fields
are intentionally not exposed for direct field-cloning either —
attempting to clone via field-by-field destructuring fails
because the private state is opaque.

The only way to share is `Arc<PolyStreamer>`. The `Arc::clone`
is what callers reach for; it's an atomic increment, no data
duplication.

### Value-enum cloning

`Value::Streamer(Arc<PolyStreamer>)` is cloneable as a Value —
the `Arc` clone is the data movement, the inner streamer state
is untouched. This matches the Polydat convention for `Value::Str`
(`Arc<str>`), `Value::Bytes` (`Arc<[u8]>`), `Value::Json`
(`Arc<serde_json::Value>`) — all reference-semantic with cheap
clones.

---

## Lifecycle and exhaustion

**Bounded streamers** (Cartesian + filter-eager + Union) have a
known total. After the Nth pull, subsequent pulls return `None`
forever. `cardinality()` returns `Bounded(n)`; `exhausted()`
becomes true.

**`BoundedAtMost` streamers** (filter-lazy) have a known upper
bound and an unknown actual. Pull dispenses one filter-passing
tuple at a time; pulls that walk past filter-rejected tuples are
internal (the cursor advances, but `dispensed` only increments
for surviving tuples). When the cursor reaches the lattice end,
subsequent pulls return `None`.

**Unbounded streamers** never declare exhaustion until the
producer side explicitly terminates the underlying queue.
`pull` on an empty-but-not-terminated queue returns `None` in
the sync case (caller polls); `pull_async` awaits.

### Reset / restart

**Initial ship: not exposed.** A streamer is single-pass — once
dispensed, a tuple cannot be re-dispensed by the same streamer.
To replay, construct a fresh streamer from the same
`Comprehension` and parent kernel.

This matches the existing `ScopeIterations` semantic and the
single-consumer expectation of iterators in general.

**Future story: `reset()` / `restart()` is cheap for bounded
streamers, expensive-to-impossible for unbounded ones.** The
cost-benefit splits cleanly by streamer variant:

**Bounded variant** (Cartesian / Union / filter-eager). The
pre-materialised `tuples: Vec<Vec<(String, Value)>>` already
holds every dispense-ready tuple. `reset()` is structurally just
a cursor rewind:

```rust
pub fn reset(self: &Arc<Self>) {
    // One atomic store. The Vec stays put.
    self.inner.cursor.store(0, Ordering::Release);
    // Reset observable counters. The seqlock ensures readers
    // see a coherent post-reset snapshot.
    self.inner.obs.store(ObservableState {
        dispensed: 0,
        last_coord: None,
        next_coord_hint: self.inner.tuples.first().map(coord_of),
    });
}
```

Cost: one atomic store + one seqlock publish. No allocation, no
re-evaluation. Cheaper than building a fresh streamer (which
would re-run `enumerate_tuples` + re-compile the canonical kernel
+ re-bind the parent scope).

The catch: callers mid-pull during a `reset()` race the rewind.
Tuple N may be dispensed twice (once before reset, once after);
tuple N+1 may be skipped (if the pre-reset consumer's `fetch_add`
landed between the reset's `store(0)` and a subsequent puller's
`fetch_add`). For replay / regression use cases this is exactly
wrong. So `reset()` would need an explicit synchronisation
contract:

- **Quiesce-then-reset** (simplest): caller asserts no concurrent
  pulls during reset. Documented precondition; a debug-mode
  assertion via an atomic "pulls in flight" counter catches
  violations.
- **Generation-tagged pulls** (lock-free, more complex): each pull
  reads the streamer's generation counter alongside the cursor;
  reset bumps the generation. Pulls landing across a reset
  boundary detect the mismatch and either retry or return a
  typed `PullError::Restarted`. Cheap-but-fiddly.

The simpler quiesce-then-reset is the right first ship for
`reset()` IF it ever lands — replay tooling and regression
harnesses are inherently single-consumer at the reset point.

**Unbounded variant** (source-driven, queue-backed). Reset is
**not well-defined**. The producer side has already moved past
whatever state would let it re-emit prior tuples — replaying
requires producer cooperation (e.g., a cursor-extending source
that supports rewind, which most don't). The honest answer for
unbounded streamers is "no reset; construct a fresh streamer
against the same comprehension and re-run the producer."

**Recommendation: don't ship reset in Push 1.** Wait for a
concrete forcing function (replay tooling for SRD-77 refine, a
property-based test harness that wants deterministic re-walk).
When it arrives, the bounded-variant implementation is two
public methods and a generation counter; the unbounded variant
gets a documented `Err(Unresettable)` return.

The decision to defer is itself low-cost — the bounded
implementation surface is small (~50 lines), and `reset()`
landing later doesn't invalidate any existing callers (it's
purely additive).

### Shared dispensing across consumers

When multiple Rust callers (or multiple Polydat consumers) hold
`Arc<PolyStreamer>` to the same streamer, all of them pull
from the same atomic cursor. Tuple N is dispensed to exactly
one caller — whichever called `pull` first won the
`fetch_add`. This is the **shared queue** semantic, not the
**broadcast** semantic; if multiple consumers each need every
tuple, they each construct their own streamer.

The streamer's coordinate-emission order is preserved across
callers: caller A getting tuple `(k=1, p=alpha)` and caller B
getting tuple `(k=1, p=beta)` reflects that the cartesian
lattice produced `(k=1, p=alpha)` before `(k=1, p=beta)`. The
order is identical to a single-consumer pull sequence.

---

## Migration plan (pushes)

Each push leaves the tree green. The existing
`ScopeIterations`-based execution stays functional throughout
— SRD-78 lands as an addition first, then nbrs-runtime migrates
to use it, then the legacy `ScopeIterations` public surface
narrows.

### Push 1 — Data shape + non-streaming construction
- `PolyStreamer` struct, `ScopeHandle`, `StreamerSnapshot`,
  `Cardinality` in a new module `polydat::comprehension::streamer`.
- `PolyStreamer::pull` implementation for the bounded
  pre-materialised case (Cartesian + Union, filter-eager).
- `Arc<PolyStreamer>` only; no `Clone` impl.
- `PolydatKernel::streamscopes(text)` Rust-side constructor.
- Unit tests for: bounded exhaustion, snapshot consistency under
  concurrent pulls (loom or shuttle-based), cardinality
  reporting.
- No Polydat grammar / port-type changes yet.

### Push 2 — `Value::Streamer` + `PortType::Streamer`
- Add the enum variant + port type.
- Update existing `Value`-handling code (snapshot writers,
  result-binding accessors) to handle the new variant — most
  fall through to "opaque reference type" defaults (display as
  `<streamer>`, no const evaluation, no equality except by
  Arc identity).
- The `Value::Streamer` is constructable from Rust code only at
  this push; no grammar surface yet.

### Push 3 — Polydat grammar: comprehension RHS in bindings
- DSL parser learns to recognise `for` as an expression on the
  RHS of `:=`, with the four modes (single-clause Cartesian,
  multi-clause Cartesian, Union-by-bracketed-list, Union-by-
  inferred-repetition) detected from the RHS shape.
- The compiler emits a node that builds the `Comprehension`
  AST + constructs an `Arc<PolyStreamer>` against the binding's
  parent kernel, wrapped in `Value::Streamer`.
- Grammar tests + round-trip parse tests; no execution-path
  changes yet.

### Push 4 — `pull_scope` node + `Context` type
- New `Value::Context(Arc<ScopeHandle>)` variant.
- New stdlib node `pull_scope(streamer)` registered via
  inventory (in polydat — this is part of polydat's core grammar
  surface).
- `coord_of` / `seq_of` accessor nodes.
- Strict-mode acknowledgement story (these are non-deterministic
  context nodes per SRD-15).

### Push 5 — Unbounded streamer variant
- `PolyStreamerInnerUnbounded` implementation.
- Producer-side hookup point exposed via a constructor like
  `PolyStreamer::unbounded(comprehension, producer_handle)`.
- `pull_async` becomes meaningfully different from `pull` only
  here.

### Push 6 — nbrs-runtime migration
- `dispatch_comprehension` in nbrs-runtime rewrites to consume
  via `PolyStreamer::pull` instead of `ScopeIterations::next`.
- Per-phase pull-then-spawn pattern unchanged — the difference
  is the source of `IterationStep`s.
- `ScopeIterations` stays as internal polydat machinery used by
  the bounded streamer's construction path; not deleted, just
  no longer the executor's direct dependency.

### Push 7 — Observable counters wired to scene tree
- The scene tree's per-comprehension-scope nodes gain a
  `Weak<PolyStreamer>` slot.
- TUI / replay surfaces query `snapshot()` for live cardinality
  + last-coord + next-coord display.
- SRD-44 checkpoint event log gains per-pull events (optional —
  size-of-checkpoint considerations may push this out).

### Push 8 — Workload-author surfaces + tests
- A workload-author-facing example showing the new Polydat syntax
  (`s := for k in ...; ctx := pull_scope(s)`).
- End-to-end test: workload uses a `PolyStreamer` binding,
  verifies dispense order matches lex semantics, verifies
  snapshot counters from the runtime side.

---

## Invariants

1. **Single dispense per tuple, across all references.** Two
   `Arc<PolyStreamer>` clones can't both receive the same tuple
   even under concurrent pull. The `AtomicU64` cursor's
   `fetch_add(AcqRel)` is the synchronisation primitive.
2. **Order is the comprehension's declared order.** Whatever
   `enumerate_tuples + apply_order` produces is what `pull`
   dispenses, in that exact sequence. Concurrent pulls don't
   reorder — they each grab one slot from the ordered list.
3. **No clone of `PolyStreamer`.** The struct is opaque; the
   only sharing primitive is `Arc::clone`. Cloning the inner
   state would duplicate the cursor — which would silently
   re-dispense already-dispensed tuples to different consumers.
4. **Snapshot reads are coherent within one epoch.** The
   seqlock retry path produces a `StreamerSnapshot` whose three
   fields all reflect the same internal moment. Callers that
   read individual accessors get per-field-atomic but
   inter-field-stale values — documented.
5. **Reified state ≠ extra layer.** The streamer's
   `dispensed` / `last_coord` / `next_coord` ARE the source of
   truth. The scene tree's per-comprehension-scope progress
   slots read from the streamer's snapshot — they don't
   maintain a parallel state machine.

---

## Open questions

- **`pull_scope` and strict mode.** Like other non-deterministic
  context nodes, the puller's Polydat expression that uses
  `pull_scope(s)` requires explicit acknowledgement under
  strict mode. Need to decide whether the acknowledgement
  pragma applies to the binding (e.g.
  `pragma nondeterministic ctx := pull_scope(s)`) or to the
  streamer wire itself (`pragma nondeterministic_consumer s`).
  Lean toward the binding form for locality.
>> 
- **Cross-execution streamers (SRD-77).** A `refine` execution
  that re-runs a phase under a different `exec_id` — does it
  build a fresh streamer or resume the prior one's cursor?
  Resume requires persisting the streamer's cursor at
  checkpoint time; fresh is simpler but loses progress.
  Defer to SRD-77's working-sessions design pass.
- **`Value::Context` lifetime under deeply nested pulls.** A
  context pulled from streamer A that itself contains a
  streamer B — when does B get released? The
  `Arc<ScopeHandle>` cycle through `kernel.children()` could
  leak if pull contexts aren't dropped promptly. The
  drop-on-binding-scope-end rule needs spelling out.
- **Filter-lazy vs filter-eager declaration.** Today's
  comprehensions always evaluate the filter eagerly during
  `enumerate_tuples`. PolyStreamer can support lazy filtering
  (filter at pull time) but it changes `cardinality()` semantics
  (`BoundedAtMost` vs `Bounded`) and adds per-pull cost. Worth
  deciding whether to expose the choice via a `lazy` keyword on
  the comprehension or hard-pick eager for the first ship.
- **`pull` from a streamer that's an output of another streamer's
  context.** Nested streaming — `s1 := for k in 1..10; ctx :=
  pull_scope(s1); s2 := for limit in 1..ctx.k; …`. The dependent
  shape is what `for_combinations` already does declaratively;
  the imperative-via-pull form is more flexible but introduces
  recursion risk. May want a depth limit.

---

## What this enables

- **Workload-author streamers without scenario-tree changes.**
  Authors that want a stream of pre-shaped contexts inside a
  binding (instead of as a phase loop) can write
  `s := for k in 1..1000; ctx := pull_scope(s)` and consume
  one context per cycle without touching `scenarios:`.
- **Lock-free concurrent dispensing.** Fibers in a per-phase
  fiber pool can each call `pull` on the same `Arc<PolyStreamer>`
  and get distinct contexts — no Mutex around the iterator, no
  one-fiber-at-a-time dispense.
- **Live observable progress.** The TUI's per-scope progress
  bars become `streamer.snapshot()` reads instead of
  walk-the-scene-tree-for-the-iteration-counter loops. One
  source of truth; no parallel counter maintenance.
- **First-class iteration in Polydat source.** The comprehension
  becomes a value the workload can name, pass around, and reason
  about. The current "comprehension is a structural concept
  invisible to GK" gap closes.
- **Cleaner SRD-77 forward path.** The "did this phase already
  complete cell N of M?" question (refine's `--scope=changed`
  pivot) becomes a streamer-snapshot lookup instead of a scene-
  tree walk.

---

## See also

- SRD-18b — Scenario tree + scheduler; the executor that
  currently owns `dispatch_comprehension` migrates in Push 6.
- SRD-18c — Comprehension syntax (clauses, layered grammar);
  unchanged.
- SRD-18d — Traversal order; unchanged. The streamer's emission
  order IS the comprehension's declared order.
- SRD-18e — **REDIRECT STUB**, superseded by
  `polydat/docs/design/comprehension_forms.md`; the AST
  PolyStreamer holds is the polydat operator-tree AST per
  polydat spec §3.
- SRD-13c — Polydat scope model; PolyStreamer's `pull` produces a
  child scope per this contract.
- SRD-13e — Scope as module; the streamer's per-tuple kernel
  honors the typed import/export contract.
- SRD-67 — Sub-context construction; PolyStreamer is the
  iteration-driving peer of `SubcontextBuilder`.
- SRD-77 — Working sessions / refine; cross-execution streamer
  resume is an open question deferred to that design.
- `polydat/docs/design/comprehension_forms.md` — the
  completed migration that PolyStreamer builds on top of.
- The deferred `OrderingStrategy` / `SpecExpander` trait
  extractions may retire when PolyStreamer's grammar surface
  forces a third consumer to materialize.
