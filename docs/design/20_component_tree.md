# Component Tree — Design Brief

How the component hierarchy drives consistent dimensional labeling
of metrics and runtime elements.

---

## The Core Idea

Every runtime element is a component. Components form a tree. Labels
flow from parent to child. A metric created on any component
automatically carries the full label ancestry — session, container,
activity, name — without the metric needing to know its context.

---

## Tree Structure

```
Session {session="abc123"}
  └── Container {container="scenario1"}
      ├── Activity {activity="write", driver="cql"}
      │   ├── Timer: cycles_servicetime
      │   ├── Timer: cycles_waittime
      │   ├── Counter: cycles_total
      │   └── Motor {motor="0"}
      │       └── Timer: stride_servicetime
      └── Activity {activity="read", driver="cql"}
          └── ...
```

Each level adds its own labels. A metric on the Motor inherits:
`{session="abc123", container="scenario1", activity="write", driver="cql", motor="0", name="stride_servicetime"}`

No metric ever needs to know or declare its full path. The tree does it.

---

## Label Composition

```rust
fn effective_labels(&self) -> Labels {
    let parent_labels = match &self.parent {
        Some(p) => p.effective_labels(),
        None => Labels::empty(),
    };
    parent_labels.extend(&self.own_labels)
}
```

- Root (Session): `{session="id"}`
- Child adds: `{activity="write"}`
- Grandchild adds: `{name="timer"}`
- Effective at grandchild: all three merged

Child labels override parent labels if the same key exists (inner
scope wins, same as workload property inheritance).

---

## Metric Attachment

Metrics are stored per-component, keyed by their effective label set:

```rust
pub trait Component: Send + Sync {
    fn labels(&self) -> Labels;
    fn own_labels(&self) -> Labels;
    fn parent(&self) -> Option<&dyn Component>;
    fn children(&self) -> Vec<Arc<dyn Component>>;
    fn metrics(&self) -> Vec<Arc<dyn Metric>>;
    fn add_metric(&self, metric: Arc<dyn Metric>);
}
```

When the metrics scheduler captures a frame, it traverses the tree
breadth-first and collects all metrics from all components. Each
metric's labels are its component's effective labels + the metric's
own labels (typically just `name`).

---

## Why This Matters

1. **Dimensional consistency** — every metric automatically carries
   its full context. A p99 from activity "write" in session "abc"
   is unambiguously identified without any naming convention.

2. **Hierarchical filtering** — "show me all metrics for activity
   write" is just a label filter `activity=write`. Works at any level.

3. **Dynamic composition** — when activities start/stop at runtime,
   their metrics appear/disappear from the tree. No registry needed.

4. **Reporter agnostic** — the same label set renders to Prometheus
   format, dot-separated format, SQLite columns, or any other scheme.

---

## Labeling Contexts (from nosqlbench devdocs)

| Level | Labels Added | Inherited From |
|-------|-------------|----------------|
| Process | `appname="nosqlbench"` | — |
| Session | `session="<id>"` | Process |
| Container | `container="<name>"` | Session |
| Activity | `activity="<alias>"`, `driver="<type>"` | Container |
| Motor | `motor="<slot>"` | Activity |
| Metric | `name="<metric_name>"` | Motor or Activity |

The effective label set at the metric level is the union of all
ancestor labels — typically 4-6 pairs.

---

## Implications for nb-rs

The `nb-metrics` crate already has `Labels` with `extend()`. The
component tree needs to live in the engine crate (when we build it)
as the structural backbone that ties sessions, activities, and
metrics together. The `MetricSource` trait we defined in `16_metrics_design.md`
is the Rust equivalent of `NBComponent`.
