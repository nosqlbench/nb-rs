# 11: GK Evaluation Model — nbrs-side framing

The substrate half of this SRD (program/state split,
provenance-based invalidation, the two evaluation lifecycles,
the const binding contract Plan A/B, non-deterministic node
exclusion, input spaces, capture context, compilation
levels) has moved into the polydat crate:

- [polydat/docs/design/evaluation_model.md](../../polydat/docs/design/evaluation_model.md)
  — moved 2026-05-30 as part of the import-first reorganization
  (see [docs/polydat_srd_audit.md](../polydat_srd_audit.md))

This file retains the nbrs-activity surface: the FiberBuilder
bridge and the cursor-driven evaluation pump.

---

## FiberBuilder

The per-fiber bridge between GK and the execution engine:

```rust
pub struct FiberBuilder {
    program: Arc<GkProgram>,   // shared, immutable
    state: GkState,            // per-fiber, mutable
}

impl FiberBuilder {
    pub fn new(program: Arc<GkProgram>) -> Self;
    pub fn set_inputs(&mut self, inputs: &[u64]);
    pub fn resolve_with_field_pulls(
        &mut self, template: &ParsedOp, field_pull_names: &[String]
    ) -> ResolvedFields;
    pub fn resolve_pulls(&mut self, plan: &PullPlan) -> ResolvedPulls;
    pub fn capture(&mut self, name: &str, value: Value);
    pub fn reset_captures(&mut self, cycle: u64);
    pub fn apply_captures(&mut self);
}
```

No separate params argument — workload params are injected into
the GK source as constant bindings before compilation and resolve
as normal GK outputs. No globals mechanism needed.

`resolve_with_field_pulls` iterates the op's field map, substitutes
`{name}` bind points from GK outputs and captures, and additionally
pulls each name in `field_pull_names` (the union of bind-point
names referenced by op fields) into `ResolvedFields` for the inner
adapter's name-keyed reads.

`resolve_pulls` materializes a [`PullPlan`] (sealed at init from
the per-template `ScopeFixture`, SRD 32 §"Init-Time Fixture and
Consumer Self-Registration") into a `ResolvedPulls` keyed by
`PullHandle`. This is the wrapper-side read path — distinct from
`ResolvedFields` and bundled alongside it in `ExecCtx` (SRD 31
§"Pull plan vs bind plan").

---

## Cursor-Driven Evaluation

When a GK program declares `cursor` bindings, the evaluation
model extends from counter-driven to cursor-driven iteration.
A cursor is a GK node whose output is a `u64` ordinal. The
runtime advances the cursor externally; downstream accessor
nodes re-evaluate via provenance-based invalidation.

### Advance / Access Separation

The cursor model separates **advance** (moving the position
forward) from **access** (reading data at the current position):

1. **Advance**: The runtime calls `Cursors::advance()` to move
   each targeted cursor to its next position. This is a pull
   from the underlying `DataSource` reader.
2. **Inject**: `Cursors::inject_into_state()` writes the new
   ordinal into the GK state's input slot for the cursor.
3. **Access**: The GK DAG re-evaluates. Accessor functions
   (e.g., `vector_at(base, ...)`) read the updated cursor
   ordinal and produce typed values. Provenance-based
   invalidation ensures only nodes downstream of the changed
   cursor are re-evaluated.

```
loop {
    if !cursors.advance() { break }  // cursor exhausted
    cursors.inject_into_state(&mut state);
    let fields = fiber.resolve_with_field_pulls(template, &field_pulls[idx]);
    let pulls  = fiber.resolve_pulls(&pull_plans[idx]);
    let ctx = ExecCtx::new(&fields, &pulls);
    dispenser.execute(cycle, &ctx).await;
}
```

### Cursors Type

`Cursors` is a provenance-driven advancer that targets only
the cursor nodes relevant to a specific set of output fields:

```rust
pub struct Cursors {
    targets: Vec<CursorTarget>,  // (DataSource reader, GK input index)
    last_items: Vec<Option<SourceItem>>,
    advances: u64,
}
```

Built at phase setup via `Cursors::for_fields()`, which traces
GK provenance from the op template's referenced field names
back to root cursor nodes. Only those cursors advance on each
iteration — unused cursors are left untouched. This enables
phases with multiple cursors where different ops consume
different data sources independently.

### Lazy Evaluation After Cursor Advance

After cursor advance and injection, the GK DAG does not
eagerly re-evaluate all nodes. Values are pulled lazily when
`resolve_with_field_pulls` (or `PullPlan::resolve` for wrapper
reads) requests specific outputs. Only nodes in the provenance
chain of the requested output are evaluated. Combined with
per-node caching, this means accessor functions for unrequested
fields are never called.

### DataSource API

The underlying data readers implement the `DataSource` trait:

```
DataSource (per-cursor, stateful)
  ├── next() → Option<SourceItem>     — pull next item
  ├── next_chunk(n) → Vec<SourceItem> — pull up to n items
  ├── extent() → Option<u64>           — known size
  └── consumed() → u64                 — progress
```

All source API surface (`DataSource`, `SourceItem`,
`SourceSchema`, `DataSourceFactory`, `Cursors`) lives in
`polydat::source`. The runtime crates consume these types
but do not define them.
