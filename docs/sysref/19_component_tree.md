# 19: Component Tree and Metric Scoping

How hierarchical execution context provides dimensional labels
for metrics, and how metric lifecycle is bounded by component
scope.

---

## Problem

The cql_vector workload runs a search phase twice per profile:
once immediately after rampup (before index compaction), and
once after awaiting index completion. Both produce recall@k
metrics. Without distinct labeling, the results are ambiguous —
which recall came from which execution context?

More generally: any nested control flow (for_each, do_until)
creates interior scopes where metrics are produced. These
metrics need dimensional labels that capture WHERE in the
execution tree they were generated, not just WHAT they measure.

---

## Design: Component Tree

Every execution scope is a node in a component tree. The tree
mirrors the scenario execution structure:

```
Session
  └── Scenario "default"
        ├── Phase "discover"
        ├── Phase "create_keyspace"
        └── ForEach profile=label_01
              ├── Phase "drop_table"
              ├── Phase "create_table"
              ├── Phase "create_index"
              ├── Phase "rampup"
              ├── Phase "search_pre_index" [label: stage=pre]
              ├── Phase "await_index"
              └── ForEach k=100
                    └── Phase "search" [label: stage=post]
```

Each node carries:
- **Its own labels**: key=value pairs that describe this node
- **Inherited labels**: accumulated from all ancestors
- **Full label set**: own + inherited, used for all metrics
  created within this scope

---

## Label Accumulation

Labels compose downward through the tree. A metric created at
any node carries the full label chain from root to that node:

```
Session:   session="20260415_abc"
  ForEach: profile="label_01"
    Phase: phase="search", stage="post", k="100"

→ metric labels: {session="20260415_abc", profile="label_01", phase="search", stage="post", k="100"}
```

This is automatic — the executor pushes labels onto a stack
as it enters each scope, and metrics created within that scope
inherit the full stack.

### Workload-Declared Labels

Control flow nodes can declare labels in the YAML:

```yaml
- for_each: "profile in matching_profiles(...)"
  label: profile    # use the iteration variable as a label
  phases:
    - search_before_index:
        label: stage=pre
    - await_index
    - search_after_index:
        label: stage=post
```

When `label:` is not specified on a for_each, the iteration
variable name is used as the label key by default.

### Phase Names as Labels

Every phase automatically contributes `phase="{name}"` to the
label set. This is implicit — no declaration needed.

---

## Metric Lifecycle

Metrics are scoped to the component that creates them. When
the component's execution completes, its metrics are:
1. Captured to the SQLite reporter (final snapshot)
2. Available for summary reporting
3. Disassociated from the active tree (no leak)

This means:
- Metrics from "search (stage=pre)" and "search (stage=post)"
  are distinct instances with different label sets
- Each for_each iteration produces its own metric instances
- Aggregation across iterations is done at query time via
  the SQLite dimensional labels

---

## Implementation

### Label Stack in ExecCtx

The executor context carries a label stack that grows as the
tree is traversed:

```rust
struct ExecCtx {
    // ... existing fields ...
    label_stack: Vec<(String, String)>,  // accumulated labels
}
```

`execute_tree` pushes labels on entry, pops on exit:

```rust
ScenarioNode::ForEach { spec, children, .. } => {
    for value in values {
        ctx.push_label(&var, &value);
        execute_tree(ctx, children, &inner).await?;
        ctx.pop_label();
    }
}
```

### Labels → Activity

When `run_phase` creates an Activity, it builds the Labels
from the full label stack:

```rust
let mut labels = Labels::of("session", &ctx.session_id);
for (k, v) in &ctx.label_stack {
    labels = labels.with(k, v);
}
labels = labels.with("phase", phase_name);
```

This replaces the current `Labels::of("session", "cli")`
hardcode.

### Labels → Metrics

ActivityMetrics, ValidationMetrics, and all instruments receive
the composed Labels at creation time. The SQLite reporter
stores the full label set in its normalized schema and the
denormalized `spec` column.

### Labels → Summary

The summary report groups by label dimensions. With the full
label set, queries like:

```sql
SELECT mi.spec, sv.mean FROM sample_value sv
JOIN metric_instance mi ON sv.instance_id = mi.id
WHERE mi.spec LIKE 'recall%'
ORDER BY mi.spec;
```

naturally separate pre-index and post-index results because
`stage=pre` vs `stage=post` appears in the spec.

---

## Interaction with Existing Features

### GK Scope Composition

The label stack parallels the GK scope stack. Each for_each
level that creates a GK scope also pushes a label. The GK
scope handles variable values; the label stack handles metric
identity. They are orthogonal but aligned.

### for_each Variables as Labels

By default, for_each iteration variables become labels
automatically. `for_each: "profile in ..."` pushes
`profile="{value}"` onto the label stack. This is the common
case — no explicit `label:` needed.

### do_while / do_until

Loop constructs push their counter variable as a label if
declared: `do_until: "empty" counter: attempt` pushes
`attempt="{i}"` per iteration. Without a counter, the loop
contributes no label (iterations are anonymous).

### Diagnostic (dryrun=gk)

The GK analysis output includes the full label stack context
for each phase, showing where in the tree the analysis applies.

---

## YAML Syntax

### Explicit labels on phases

```yaml
phases:
  search_pre:
    label: stage=pre
    # ... ops ...
  search_post:
    label: stage=post
    # ... ops ...
```

### Explicit labels on control flow nodes

```yaml
- for_each: "profile in ..."
  label: profile
  phases:
    - rampup
    - for_each: "k in {k_values}"
      label: k
      phases:
        - search
```

### No explicit label (default)

for_each uses its variable name. Phases use their phase name.
do_while/do_until use their counter name (if declared).

---

## Migration

The current system uses `activity="phase_name (var=value)"`
as a flat string. This is replaced by structured labels:
`phase="phase_name", profile="label_01", k="100"`. The
activity name string is still constructed for display, but
metrics use the structured labels for dimensional analysis.

No breaking change — the SQLite schema already supports
arbitrary label sets. The `spec` column gains richer labels
automatically.
