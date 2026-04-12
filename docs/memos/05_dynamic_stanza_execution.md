# Memo 05: Dynamic and Non-Uniform Stanza Execution

The current op sequencing model uses a fixed-ratio LUT that maps
cycles to op templates uniformly. Every stanza has the same
structure. This memo explores what's needed to support dynamic,
non-uniform stanza execution.

---

## What We Have

The current model (sysref 22):

```
OpSequence:
  templates: [read, write]
  ratios: [4, 1]
  LUT: [R, R, R, R, W]    ← fixed, repeating

Stanza 0: R R R R W
Stanza 1: R R R R W
Stanza 2: R R R R W
...forever
```

Properties: deterministic, O(1) lookup, cycle-reproducible,
easy to reason about. Dependency groups (linearization) are
computed once and reused for every stanza.

## What's Missing

### 1. Conditional Op Selection

"If the SELECT returns no rows, INSERT a default."

The next op depends on the result of the previous one. This
can't be expressed with a fixed LUT — the stanza structure
varies per iteration.

### 2. Phase Transitions

"Run 10,000 rampup stanzas, then switch to steady-state mix."

Currently this requires separate CLI invocations with different
tag filters. A single workload should be able to express
multi-phase execution with transition conditions.

### 3. Dynamic Injection

"Every 100th stanza, run a compaction check."

Periodic or probabilistic injection of ops that don't appear in
the normal stanza. The injection rate may depend on metrics
(backpressure-driven compaction).

### 4. Result-Driven Branching

"If query latency exceeds threshold, switch to a degraded
operation mode."

The stanza structure changes based on observed system behavior.
This is runtime-adaptive, not compile-time deterministic.

---

## Design Constraints

Whatever we add must preserve these properties of the current
model:

1. **Deterministic default.** The fixed-ratio LUT remains the
   default. Simple workloads don't pay for dynamic capabilities.

2. **Cycle reproducibility.** For deterministic stanzas, the same
   cycle number produces the same ops. Dynamic stanzas may break
   this — that must be explicit and acknowledged.

3. **Linearization compatibility.** Dependency groups must work
   with dynamic stanzas. If the stanza structure varies, groups
   are computed per-stanza (not once at init).

4. **Zero cost when unused.** No per-cycle overhead for workloads
   that don't use dynamic features.

5. **No scripting language.** Branching conditions are expressed
   in GK or declarative YAML, not in an embedded scripting
   engine.

---

## Approach: Stanza Templates

Instead of a single fixed op sequence, a stanza is defined by a
**stanza template** — a sequence of op slots that may include
conditional branches:

```yaml
stanza:
  - op: insert
    ratio: 1
  - op: select
    ratio: 1
  - branch:
      condition: "{capture:row_count} == 0"
      then:
        - op: insert_default
      else:
        - op: log_found
```

The stanza template is evaluated per stanza. Fixed ops execute
unconditionally. Branches evaluate a condition (from captures or
GK values) and select a path.

### Condition Evaluation

Conditions are evaluated by the GK kernel — not a separate
expression language. A condition is a GK binding that produces
a boolean:

```yaml
bindings: |
  inputs := (cycle)
  should_insert := eq(capture:row_count, 0)

stanza:
  - op: select
  - branch:
      when: should_insert
      then: [insert_default]
```

The `when` field names a GK output (or capture-derived value)
that must be `true` (nonzero) for the branch to execute.

### Per-Stanza Linearization

With dynamic stanzas, the dependency structure may vary per
iteration. The executor must compute dependency groups per
stanza rather than once at init:

```
Stanza iteration N:
  1. Evaluate stanza template → concrete op list
  2. Analyze dependencies → groups for THIS stanza
  3. Execute groups
```

For fixed stanzas (no branches), the groups are computed once
at init (current behavior). The per-stanza path only activates
when the stanza template contains dynamic elements.

---

## Phase Transitions

Phases are expressed as a sequence of stanza templates with
transition conditions:

```yaml
phases:
  rampup:
    stanza: [insert]
    until: "{cycle} >= {train_count}"
  steady:
    stanza:
      - op: read
        ratio: 4
      - op: write
        ratio: 1
    duration: 300s
  cooldown:
    stanza: [read]
    cycles: 1000
```

The executor processes phases in order. Each phase has its own
stanza template and a termination condition. This replaces the
current pattern of multiple CLI invocations with tag filters.

### Transition Conditions

| Condition | Meaning |
|-----------|---------|
| `cycles: N` | Fixed cycle count |
| `duration: Ns` | Wall-clock time |
| `until: "{expr}"` | GK-evaluated boolean |

---

## Periodic Injection

Ops that should run periodically but aren't part of the normal
stanza:

```yaml
stanza:
  - op: read
    ratio: 4
  - op: write
    ratio: 1

inject:
  - op: compact_check
    every: 100     # every 100th stanza
  - op: stats_query
    every: 50
```

Injected ops are appended to the stanza at the specified
frequency. The dependency analysis includes them when present.

---

## Impact on Cycle Reproducibility

Dynamic stanzas break cycle reproducibility when:
- Branch conditions depend on adapter results (non-deterministic)
- Injection frequency depends on runtime state

Stanzas that branch only on GK-computed conditions (from cycle
number and captures from prior GK-driven ops) remain
deterministic. The workload author controls this — the system
doesn't force non-determinism.

A `--reproducible` flag could enforce that all branch conditions
are GK-deterministic, rejecting conditions that depend on
adapter results.

---

## Implementation Path

1. **Stanza template model.** Data structure representing a
   stanza with optional branches. Evaluated per stanza to
   produce a concrete op list.

2. **Condition evaluation.** GK bindings that produce booleans
   from captures and cycle values. No new expression language.

3. **Per-stanza linearization.** When the stanza template is
   dynamic, compute dependency groups per iteration. When
   static, use init-time groups (current behavior).

4. **Phase sequencing.** Phase list with transition conditions.
   Replaces separate CLI invocations.

5. **Periodic injection.** Op injection at configurable stanza
   frequency.

Each step is independent and backward-compatible. Workloads
that don't use dynamic features get exactly the current behavior.

---

## What This Does Not Cover

- **Adaptive rate control** based on metrics (separate concern)
- **Multi-activity orchestration** (scenario layer)
- **Distributed coordination** across multiple nb-rs instances
- **Interactive steering** from the web UI

These are higher-level concerns that build on top of dynamic
stanza execution but are not part of it.
