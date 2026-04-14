# 18: Uniform Control Flow Primitives

How `for_each`, `do_while`, and `do_until` work as composable,
nestable control flow constructs at any level of the scenario tree.

---

## Principles

1. **`cycle` is immutable.** It's the activity-assigned ordinal —
   the identity of the work unit. Interior loops do not mutate it.
   All ops within a cycle's stanza are part of that cycle's lineage,
   regardless of how many loops they pass through.

2. **Loop counters are explicit.** If a loop needs an iteration
   index, it declares a named `counter` variable. This is a GK
   scope value — visible to all children via the standard scope
   composition mechanism (auto-externs, `shared`/`final`).

3. **Three constructs, one shape.** `for_each`, `do_while`, and
   `do_until` are all tree nodes that wrap children. They differ
   only in termination logic:
   - `for_each` — pre-resolved value list, iterate all
   - `do_while` — evaluate condition after each iteration, continue while true
   - `do_until` — evaluate condition after each iteration, stop when true

4. **Nestable to arbitrary depth.** Any construct can contain any
   other construct. A `for_each` can contain a `do_until` which
   contains another `for_each`. The execution plan is flattened
   recursively.

5. **Uniform at all levels.** The same constructs work at:
   - Scenario level (wrapping phases)
   - Phase level (on the phase's `for_each` field)
   - Op level (future: wrapping individual ops within a stanza)
   The GK scope model (sysref 16) handles variable resolution
   at every level.

---

## Cycle Identity and Data Lineage

A cycle is the fundamental unit of work in an activity. When a
cycle spawns additional operations through loop constructs, those
operations are part of the same cycle's **causal lineage**:

```
cycle 42 → stanza ops → [do_until loop → op A, op B] → op C
```

All of op A, op B (across loop iterations), and op C share
cycle 42. The loop counter is a separate variable — it doesn't
replace or shadow `cycle`.

If an inner op needs a unique ordinal (e.g., for deterministic
data generation within a loop), it derives it from the loop
counter variable combined with `cycle`:

```
inner_id := hash(cycle + attempt * 1000000)
```

This preserves determinism: same cycle + same attempt = same result.

---

## Syntax

### for_each (scenario level)

```yaml
scenarios:
  default:
    - for_each: "profile in matching_profiles('{dataset}', '{prefix}')"
      phases:
        - drop_table
        - create_table
        - rampup
        - for_each: "k in {k_values}"
          phases:
            - search
```

### do_while (scenario level)

```yaml
scenarios:
  default:
    - do_while: "remaining_profiles > 0"
      counter: batch
      phases:
        - process_batch
```

### do_until (scenario level)

```yaml
scenarios:
  default:
    - load_data
    - do_until: "empty"
      counter: attempt
      phases:
        - await_compaction
```

### Phase-level for_each (lifted into tree)

```yaml
phases:
  search:
    for_each: "k in {k_values}"
    ops:
      select_ann: ...
```

This is equivalent to a `ForEach` node wrapping the phase in
the scenario tree. The runner lifts it during plan building.

---

## Condition Evaluation

### for_each

The expression is evaluated once at loop entry. The result is
a comma-separated string. Each value drives one iteration.
The iteration variable is injected as a GK init constant.

### do_while / do_until

The condition is a GK expression or a result body check:

- **GK expression**: evaluated after each iteration using the
  current scope state. Returns a boolean (or u64 where 0=false).
- **`"empty"`**: special keyword — checks if the last op's result
  body has zero rows (same as poll wrapper's await_empty).

The condition is evaluated AFTER each iteration (guarantee at
least one execution), matching do-while/do-until semantics in
most languages.

### Counter variable

When `counter: name` is specified, the variable starts at 0 and
increments by 1 per iteration. It's available to all children as
a GK scope value via `shared` semantics — inner scopes see it,
and it carries across iterations.

---

## Implementation

### ScenarioNode enum

```rust
enum ScenarioNode {
    Phase(String),
    ForEach { spec: String, children: Vec<ScenarioNode> },
    DoWhile { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
    DoUntil { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
}
```

### Plan flattening

`for_each` is pre-resolved into a flat list of `(phase, bindings)`.
`do_while` and `do_until` cannot be pre-resolved — they're
evaluated at runtime. The execution plan has two types of entries:

1. **Static entries** (from `Phase` and `ForEach`): fully resolved
   at plan time, executed sequentially.
2. **Dynamic entries** (from `DoWhile`/`DoUntil`): evaluated
   at runtime, looping until the condition is met.

### GK scope integration

Loop variables (iteration values, counters) are injected as GK
init constants into the inner scope. This uses the same mechanism
as `for_each` — `init {var} = "{value}"` prepended to the GK
source before compilation. The variable is then available as a
GK output for op field resolution, relevancy config, and all
other GK-dependent paths.

No side-channel text substitution. All values flow through GK.

---

## Interaction with Other Features

- **`shared`/`final`**: Loop counters use `shared` semantics.
  `final` variables from outer scopes cannot be modified by loops.
- **Scope modes** (`loop_scope`/`iter_scope`): Apply to `for_each`.
  `do_while`/`do_until` always inherit from the enclosing scope.
- **Diagnostics** (`dryrun=phase,gk`): `for_each` shows all
  iterations. `do_while`/`do_until` show one iteration (the first).
- **Metrics**: Each loop iteration's ops contribute to the same
  activity's metrics (same cycle). The counter variable can be
  used as a dimensional label for per-iteration tracking.
