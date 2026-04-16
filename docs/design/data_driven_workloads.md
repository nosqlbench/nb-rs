# Design Sketch: Source-Driven Workloads

## The Core Shift

Today: a counter (`cycle`) drives the workload. The GK graph computes values *from* the counter. The op loop iterates the counter.

Proposed: **data sources** drive the workload. The GK graph declares sources. The op loop **pulls** from sources. When sources are exhausted, the phase is done. There is no counter to configure.

## Sources as First-Class Concepts

A **source** is a named, typed sequence of values with known or unknown extent:

```yaml
bindings: |
  # Declare a source — a live sequence with its own cursor
  source vectors = dataset_vectors("sift1m:label_00")
  
  # Pull from the source — advances the cursor
  (id, vector) := vectors.next()
```

`dataset_vectors(...)` creates a source object:
- **Extent**: 82,993 items (known from the dataset)
- **Cursor**: internal position, starts at 0, advances on each `.next()`
- **Yields**: `(ordinal, vector_data)` per item
- **Exhaustion**: `.next()` returns None when cursor reaches extent

The phase runs until the source is exhausted. No `cycles:` needed.

### Chunk Consumption and Idiomatic Exhaustion

Sources support both single and chunk reads. The return types use idiomatic Rust to make exhaustion explicit:

```rust
/// A single item from a source.
trait DataSource: Send + Sync {
    /// Pull the next item. Returns `None` when exhausted.
    fn next(&mut self) -> Option<SourceItem>;

    /// Pull up to `limit` items. Returns a vec that may be shorter
    /// than `limit` if fewer items remain. Returns an empty vec
    /// ONLY when the source is truly exhausted — never a partial
    /// short-read that could be confused with EOD.
    fn next_chunk(&mut self, limit: usize) -> Vec<SourceItem>;

    /// Known extent, if finite. `None` for infinite sources.
    fn extent(&self) -> Option<u64>;

    /// Items consumed so far (for progress reporting).
    fn consumed(&self) -> u64;
}
```

`next()` returns `Option<SourceItem>` — `None` is unambiguous EOD. `next_chunk(10)` returns a `Vec` of 1–10 items, or empty only on true exhaustion. This is important for batch filling: the dispenser calls `next_chunk(budget_remaining)` and always gets a usable chunk or a definitive end signal.

## Sources in the GK Graph

Sources are GK nodes with **state**. Unlike pure functions (`hash(cycle)` is stateless), a source has a cursor that advances:

```
dataset_vectors("sift1m:label_00")
  ├─ extent: 82993
  ├─ cursor: 0 → 1 → 2 → ... → 82992 → exhausted
  └─ yields: (u64, Vec<f32>) per pull
```

The GK graph exposes sources as **coordinate providers**. Today, `inputs := (cycle)` declares one coordinate. With sources:

```yaml
bindings: |
  # The source provides the coordinate, not a counter
  source base = dataset_source("sift1m:label_00", "base")
  
  # These bindings pull from the source's current position
  id := format_u64(base.ordinal, 10)
  train_vector := base.vector
```

`base.ordinal` and `base.vector` are projections of the source's current yield. The source advances once per op dispatch. The graph evaluates with the source's current values.

## Op Construction Pulls from Sources

The executor doesn't iterate a counter. It pulls from sources:

```
loop {
    let item = source.next();    // advance the cursor
    if item.is_none() { break; } // source exhausted → phase done
    
    let fields = gk.eval(item);  // GK graph evaluates with source values
    dispenser.execute(fields);    // adapter builds and sends the op
}
```

For **batching**, the dispenser pulls multiple items:

```
loop {
    let mut batch_items = Vec::new();
    let mut batch_bytes = 0;
    
    while batch_bytes < MAX_BATCH_BYTES {
        let Some(item) = source.next() else { break };
        let fields = gk.eval(item);
        let stmt = render_statement(fields);
        batch_bytes += stmt.len();
        batch_items.push(stmt);
    }
    
    if batch_items.is_empty() { break; }
    adapter.execute_batch(batch_items);  // send whatever we collected
}
```

The batch fills **dynamically** — it doesn't know how many items fit until it tries. A batch of 128-dim vectors holds ~8 rows before hitting the 5KB limit. A batch of tiny key-value pairs holds 200. The dispenser doesn't need to be told the batch size — it fills to the budget.

## The Dispenser Sees the Source

Today: the dispenser receives `ResolvedFields` (pre-computed strings and values). It's a passive consumer.

Proposed: the dispenser has access to the **source** and can pull incrementally:

```rust
trait OpDispenser {
    fn execute(
        &self,
        source: &mut dyn DataSource,
        gk: &mut GkEvaluator,
    ) -> Result<OpResult, ExecutionError>;
}
```

The CQL batch dispenser:

```rust
fn execute(&self, source: &mut dyn DataSource, gk: &mut GkEvaluator) -> ... {
    let mut batch = session.batch(BatchType::UNLOGGED);
    let mut byte_budget = self.max_batch_bytes;  // e.g., 4096
    let mut rows = 0;
    
    while byte_budget > 0 {
        let Some(item) = source.next() else { break };
        let fields = gk.eval(item);
        let stmt_text = fields.render_statement(&self.template);
        byte_budget -= stmt_text.len();
        batch.add_statement(session.statement(&stmt_text));
        rows += 1;
    }
    
    session.execute_batch(&batch).await?;
    self.rows_total.fetch_add(rows, Relaxed);
    Ok(OpResult::default())
}
```

The dispenser **drives the source**. It decides how many items to pull based on the transport constraint (byte budget, max statements, etc.). The batch sizes itself dynamically.

## Source Types

Sources aren't just datasets. Any iterable concept is a source:

```yaml
bindings: |
  # Dataset source — finite, known extent
  source base = dataset_source("sift1m:label_00", "base")
  
  # Range source — finite, synthetic
  source users = range(0, 1000000)
  
  # Random source — infinite, for stress testing  
  source samples = random_range(0, 1000000)
  
  # Composite source — zips multiple sources
  source pairs = zip(users, products)
```

Each source type has:
- **Extent**: finite (82993, 1000000) or infinite (random)
- **Ordering**: sequential, shuffled, or random
- **Cursor**: tracks consumption position
- **Exhaustion**: finite sources end; infinite sources run until `duration:` or `limit:`

## Phase Completion

Without `cycles`, phase completion is determined by the source:

```yaml
phases:
  rampup:
    # No cycles: needed — the source defines the extent
    concurrency: 100
    bindings: |
      source base = dataset_source("{dataset}:{profile}", "base")
      id := format_u64(base.ordinal, 10)
      train_vector := base.vector
    ops:
      insert:
        batch: { max_bytes: 4096 }
        prepared: "INSERT INTO ... VALUES ('{id}', {train_vector})"
```

The phase runs until `base` is exhausted (82,993 items consumed). With `batch: { max_bytes: 4096 }`, each batch holds ~6-8 vectors. The runtime creates ~10K-14K batches. The user doesn't care about the batch count — they care that all vectors are loaded.

For stress tests with infinite sources:

```yaml
phases:
  stress:
    duration: 60s                     # run for 60 seconds
    # or: limit: 100000              # run 100K ops then stop
    concurrency: 50
    bindings: |
      source keys = random_range(0, 1000000)
      key := keys.value
    ops:
      read:
        prepared: "SELECT * FROM users WHERE id = {key}"
```

## Concurrency and Source Partitioning

With 100 concurrent fibers, each fiber needs its own cursor into the source. Two models:

**A. Shared source with atomic cursor** (current model, simple):
```
Source(extent=82993, cursor=AtomicU64)
  Fiber 0: pulls 0, then 100, then 200, ...
  Fiber 1: pulls 1, then 101, then 201, ...
```

**B. Partitioned source** (better cache locality):
```
Source(extent=82993)
  Fiber 0: source[0..830)
  Fiber 1: source[830..1660)
  ...
  Fiber 99: source[82170..82993)
```

>> To optimize source reader performance, there may need to be a intermediary init-time trait which means "source reader" which has all the basic details (type, default chunksize) memoized and inlined as much as possible. It can also hide any work-stealing or other cooperateive rebalancing effects.

### Source Readers: Init-Time Optimization Layer

The `DataSource` trait above is the consumption API. Behind it sits a **SourceReader** — an init-time constructed object that memoizes the source's type, default chunk size, backing data handle, and partition assignment:

```rust
/// Created once per fiber at phase init. Cheap to read from.
struct SourceReader {
    /// Backing data (mmap'd vectors, range state, etc.)
    backing: Arc<dyn SourceBacking>,
    /// This reader's partition range [start, end)
    partition: Range<u64>,
    /// Current cursor within the partition
    cursor: u64,
    /// Shared pool of partitions for work-stealing
    pool: Arc<WorkPool>,
    /// Memoized element type and default chunk size
    element_type: ElementType,
    default_chunk: usize,
}
```

At init time, the source is partitioned across fibers. Each fiber gets a `SourceReader` with its own range. The reader:
- Serves reads from its local partition (fast, contiguous)
- When its partition is exhausted, transparently steals a range from the `WorkPool`
- The `WorkPool` tracks unfinished partitions and subdivides them for stealing

>> partitioned sources are fine so long as we support transparent work stealing on exhaustion, and make any incremental processing which might have a short read from this verboten by the API.

### Work-Stealing Contract

The critical invariant: `next_chunk(n)` never returns a short chunk unless the source is **globally** exhausted. If a reader's local partition runs out mid-chunk:

1. Reader attempts to steal from `WorkPool`
2. If steal succeeds: reader gets a new partition range, continues filling the chunk
3. If steal fails (all partitions exhausted globally): returns whatever was collected — this IS the final chunk

A batch dispenser calling `next_chunk(n)` can trust that any non-empty return is a full allotment to process. An empty return means the phase is done. No ambiguity, no partial short-reads that could be mistaken for EOD.

## Batch Budget

The `batch:` directive on an op becomes a **budget**, not a count:

```yaml
ops:
  insert:
    # Budget-based batching: fill until the constraint is met
    batch:
      max_bytes: 4096               # CQL batch byte limit
      max_rows: 100                 # hard cap on statements per batch
      type: unlogged                # CQL batch type
    prepared: "INSERT INTO ... VALUES ('{id}', {train_vector})"
```

The dispenser fills incrementally:
1. Pull item from source
2. Evaluate GK graph → get resolved fields
3. Render statement text
4. Check byte budget: `accumulated_bytes + stmt.len() <= max_bytes`?
5. If yes: add to batch, continue
6. If no: dispatch current batch, start new one with this item
7. When source exhausted: dispatch remaining items as a final batch

This solves the "batch too large" problem automatically — the dispenser adapts to the data size without the user guessing at batch counts.

## The `source` Keyword in GK

```
source <name> = <constructor>(<args>)
```

This is a new GK keyword (alongside `inputs`, `init`, `shared`, `final`). It declares a stateful data source:

```yaml
bindings: |
  # Source declaration — creates a sequence with cursor
  source base = dataset_source("sift1m:label_00", "base")
  
  # Source projections — read from current cursor position
  id := format_u64(base.ordinal, 10)
  vector := base.vector
  
  # Multiple sources can coexist
  source queries = dataset_source("sift1m:label_00", "queries")
  query_vector := queries.vector
```

Source projections (`base.ordinal`, `base.vector`) are like struct field access. The GK compiler knows the source's schema (what fields it yields) from the constructor.

## Relationship to Coordinates

Today: `inputs := (cycle)` declares one coordinate. All nodes compute from it.

With sources: the source **replaces** the coordinate for its dependents. `base.ordinal` is the effective coordinate for `id` and `vector`. The GK graph wires source projections as inputs to downstream nodes.

Multiple sources in one graph means multiple independent coordinates. The executor advances each source independently, or zips them (for phases that consume from multiple facets in lockstep).

## What This Enables

1. **Self-sizing phases**: the phase knows how much data to process from the source's extent. No `cycles:` computation.

2. **Dynamic batch sizing**: the dispenser fills batches to a byte budget, adapting to data size automatically.

3. **Composable sources**: `zip(vectors, metadata)` combines two sources into one feed. `filter(queries, predicate)` filters a source.

4. **Progress from data**: the progress bar shows "82,993 / 82,993 vectors loaded" — meaningful to the user.

5. **Data-aware parallelism**: the runtime can partition sources across fibers for cache locality.

6. **Infinite workloads**: random/cycling sources for stress tests, bounded by `duration:` or `limit:`.

>> It is markedly different from the nosqlbench approach, where modeling dependent operations, for example, required a numerical alignment of a rigid set of operations and ordinal mappings. This way, data flow and provenance flow naturally through stanzas because the operations are instanced based on entity level scopes.

>> It also means, we now have a solution for making the sequence planner less rigid. There is no such requirement that we use a LUT, for example, which aligns via stride to the previous cycles concept. We will still want to iterate over cohesively defined stanzas, but now the unit of dispatch is a source allotment (or a single ordina-data tuple), and any LUT for execution would be at most an internal optimization. We need to redesign the "planner" layer of nbrs to be flexible now and align with sources concepts squarely.

## Stanza Scheduling

The LUT-based sequencer becomes one of several **stanza schedulers** — an internal mechanism that determines which op template to dispatch next within a stanza. The scheduler is selected based on the phase configuration:

```yaml
# Classic weighted ratios — LUT scheduler (bucket/interval sequencer types)
ops:
  read:
    ratio: 5
    prepared: "SELECT ..."
  write:
    ratio: 1
    prepared: "INSERT ..."

# Source-driven single op — no scheduler needed, just iterate
ops:
  insert:
    batch: { max_bytes: 4096 }
    prepared: "INSERT ..."

# Source-driven multi-op stanza — round-robin or dependency-ordered
ops:
  insert_user:
    prepared: "INSERT INTO users ..."
  insert_order:
    prepared: "INSERT INTO orders ..."
    # depends on insert_user's captures
```

Stanza schedulers:
- **LUT/Bucket**: classic weighted ratio dispatch. Ops selected by cycle ordinal mod total weight. Used when `ratio:` appears on any op.
- **Sequential**: ops execute in declaration order within each stanza iteration. Used for dependency-chain stanzas.
- **Source-driven**: the source determines the dispatch cadence. Each pull from the source yields one stanza execution. No LUT indirection.

The scheduler is an internal optimization — the user doesn't configure it directly. The runtime infers it from the op configuration (ratios present → LUT; captures flowing → sequential; source-only → source-driven).

## Crate Sovereignty

All GK-involved API surface — the `source` keyword, `DataSource` trait, source projections, chunk consumption, and source-to-coordinate wiring — lives in the **nb-variates** (GK) crate. The GK crate defines:

- `DataSource` trait and `SourceItem` types
- `SourceReader` and `WorkPool` for partitioned consumption
- Source constructors (`dataset_source`, `range`, `random_range`)
- Source combinators (`zip`, `filter`, `sample`)
- Source projection nodes for the GK graph

The runtime crates (nb-activity, adapters) consume these types but don't define them. The GK crate maintains sovereignty over the data model — sources are a GK concept, not a runtime concept. The runtime asks the GK graph "what sources exist?" and "what is their extent?" — the GK crate answers authoritatively.

This means:
- `nb-variates` defines `DataSource`, `SourceReader`, source nodes
- `nb-activity` implements the fiber loop, work-stealing pool, and executor
- Adapters (cassnbrs, etc.) implement batch dispensers that consume from `DataSource`
- `nb-workload` parses the `source` keyword in YAML and passes it to GK compilation

## Migration Path

1. **Phase 0** (now): Keep `cycles` working. Add `source` as a GK keyword. Implement `dataset_source()` constructor. Wire source extent into phase completion.

2. **Phase 1**: Implement budget-based batching in the CQL dispenser. The dispenser pulls from the source incrementally and fills to the byte budget.

3. **Phase 2**: Implement `over: auto` which inspects the GK graph for source declarations and derives the extent. `cycles` becomes a fallback/override.

4. **Phase 3**: Implement source partitioning for concurrent fibers. Add `zip`, `filter`, `sample` source combinators.

## Sketch of Runtime Flow

```
Phase setup:
  1. Compile GK graph → detect source declarations
  2. Instantiate sources (dataset handles, cursors, extents)
  3. Partition sources across fibers → create SourceReaders
  4. Determine phase extent from source(s) for progress reporting
  5. Select stanza scheduler (LUT if ratios, sequential if captures, source-driven otherwise)

Fiber loop (source-driven):
  loop {
      match reader.next() {
          Some(item) => {
              let fields = gk.eval(item);
              let result = dispenser.execute(fields).await;
              record_metrics(result);
          }
          None => break,  // source exhausted (local + work-stealing)
      }
  }

Fiber loop (LUT-scheduled, mixed ops with ratios):
  loop {
      let template = scheduler.next_template();  // LUT/bucket/interval
      match reader.next() {
          Some(item) => {
              let fields = gk.eval_for(template, item);
              let result = dispensers[template].execute(fields).await;
              record_metrics(result);
          }
          None => break,
      }
  }

Batch dispenser (budget-driven):
  fn execute(&self, reader, gk) {
      let mut batch = new_batch();
      let mut bytes = 0;
      loop {
          match reader.next() {
              Some(item) => {
                  let fields = gk.eval(item);
                  let stmt = render(fields);
                  if bytes + stmt.len() > self.max_bytes && !batch.is_empty() {
                      // Budget exceeded — dispatch what we have,
                      // push this item back or start new batch
                      break;
                  }
                  bytes += stmt.len();
                  batch.add(stmt);
              }
              None => break,  // source exhausted
          }
      }
      if !batch.is_empty() {
          session.execute_batch(batch).await;
          rows_total += batch.len();
      }
  }

Phase completion:
  All fibers see source exhausted → join → lifecycle flush
```
