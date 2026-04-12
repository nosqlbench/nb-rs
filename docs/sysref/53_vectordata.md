# 53: Vector Data Integration

GK nodes for loading and generating vector search workload data
from the `vectordata` crate ecosystem.

---

## Dataset Resolution

Bare dataset names resolve through the vectordata catalog:

```
"glove-25-angular"
  → vectordata::catalog::Catalog::of(CatalogSources::new().configure_default())
  → https://jvector-datasets-infratest-cleaned.s3.us-east-2.amazonaws.com/...
  → download + parse dataset.yaml
  → load binary slab files
```

>> This needs to support profiles as well, where "dbname:profile" works correctly with the vectordata API, and "dbname" does too (should route to default profile)

Resolution tiers:
1. Direct URL (starts with `http://` or `https://`)
2. Local file path
3. Catalog lookup (bare name → URL via vectordata 0.15 catalog)

---

## Dataset Caching

Datasets are loaded once globally:

```rust
static DATASET_CACHE: LazyLock<Mutex<HashMap<String, Arc<TestDataGroup>>>> = ...;
```

Multiple GK nodes referencing the same dataset share the cached
`TestDataGroup`. Each dataset logs "resolved" once on first load.

---

## GK Node Functions

All feature-gated behind `vectordata` in nb-variates.

### Vector Access

| Node | Signature | Description |
|------|-----------|-------------|
| `vector_at(index, dataset)` | `u64, str → str` | Training vector as float array string |
| `query_vector_at(index, dataset)` | `u64, str → str` | Query vector as float array string |
| `vector_at_bytes(index, dataset)` | `u64, str → bytes` | Training vector as raw bytes |
| `query_vector_at_bytes(index, dataset)` | `u64, str → bytes` | Query vector as raw bytes |

### Ground Truth

| Node | Signature | Description |
|------|-----------|-------------|
| `neighbor_indices_at(index, dataset)` | `u64, str → str` | Nearest neighbor indices |
| `neighbor_distances_at(index, dataset)` | `u64, str → str` | Nearest neighbor distances |
| `filtered_neighbor_indices_at(index, dataset)` | `u64, str → str` | Filtered ground truth (future) |
| `filtered_neighbor_distances_at(index, dataset)` | `u64, str → str` | Filtered distances (future) |

### Metadata

| Node | Signature | Description |
|------|-----------|-------------|
| `vector_dim(dataset)` | `str → u64` | Dimension count (init-time constant) |
| `vector_count(dataset)` | `str → u64` | Training set size (init-time constant) |
| `dataset_distance_function(dataset)` | `str → str` | Similarity metric name |

`vector_dim` and `vector_count` are init-time constants — they
evaluate during constant folding and can drive activity config
(e.g., schema DDL uses `{dim}`, cycle count uses `train_count`).

---

## Workload Pattern

Typical vector search workload with three phases:

```yaml
params:
  dataset: glove-25-angular
  concurrency: "100"

bindings: |
  inputs := (cycle)
  dim := vector_dim("{dataset}")
  train_count := vector_count("{dataset}")
  train_vector := vector_at(cycle, "{dataset}")
  query_vector := query_vector_at(cycle, "{dataset}")
  ground_truth := neighbor_indices_at(cycle, "{dataset}")

ops:
  # Phase 1: Schema (raw DDL, concurrency=1)
  create_table:
    tags: { phase: schema }
    raw: "CREATE TABLE t (key text PRIMARY KEY, value vector<float, {dim}>)"

  # Phase 2: Rampup (prepared inserts, high concurrency)
  insert:
    tags: { phase: rampup }
    prepared: "INSERT INTO t (key, value) VALUES ('{id}', {train_vector})"

  # Phase 3: Search (ANN queries with recall measurement)
  select_ann:
    tags: { phase: search }
    prepared: "SELECT key FROM t ORDER BY value ANN OF {query_vector} LIMIT {k}"
    relevancy:
      actual: key
      expected: "{ground_truth}"
      k: 100
      functions: [recall, precision, f1]
```

---

## Future: Metadata and Predicates (SRD 46)

The `vectordata` crate provides structured metadata (MNode) and
predicates (PNode) via the `veks-anode` wire format. Future GK
nodes will translate these into CQL syntax:

| Function | Description |
|----------|-------------|
| `metadata_at(index, source)` | Decode MNode at index |
| `metadata_cql_values(index, source)` | Render as CQL value list |
| `predicate_cql_where(index, source)` | Render as CQL WHERE clause |

These enable filtered ANN workloads with metadata-driven schema
generation.
