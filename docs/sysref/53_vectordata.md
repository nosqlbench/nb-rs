# 53: Vector Data Integration

GK nodes for loading and generating vector search workload data
from the `vectordata` crate ecosystem.

Upstream API reference:
<https://github.com/nosqlbench/vectordata-rs/blob/main/docs/sysref/02-api.md>

---

## Dataset Resolution

Source specifiers support three formats:

| Format | Example | Description |
|--------|---------|-------------|
| `"dataset"` | `"example"` | Catalog lookup, default profile |
| `"dataset:profile"` | `"example:label-1"` | Catalog lookup, explicit profile |
| `"https://..."` | `"https://host/ds/"` | Direct URL |
| `"/path/to/dir"` | `"/data/example"` | Local filesystem |

Resolution order:
1. In-memory cache hit
2. Direct URL (`http://` or `https://`)
3. Local path (if exists)
4. Local cache (`~/.cache/vectordata/<dataset>/`) — from prebuffering
5. Catalog lookup (`~/.config/vectordata/catalogs.yaml`)

After catalog resolution, `TestDataGroup::load()` provides typed
`VectorReader<T>` and `VvecReader<T>` access to all facets.

---

## Dataset Caching

Datasets are loaded once globally:

```rust
static DATASET_CACHE: LazyLock<Mutex<HashMap<String, Arc<TestDataGroup>>>> = ...;
```

Multiple GK nodes referencing the same dataset share the cached
`TestDataGroup`. Each dataset logs "resolved" once on first load.

---

## Prebuffering

Use `dataset_prebuffer("source")` to eagerly download all facets
before workload execution:

```yaml
bindings: |
  inputs := (cycle)
  _pb := dataset_prebuffer("{dataset}")
  # ... rest of bindings use local mmap access
```

Prebuffering uses `RemoteDatasetView` with `CachedChannel` for
merkle-verified chunk downloads to `~/.cache/vectordata/<dataset>/`.
After prebuffering, `TestDataGroup::load()` resolves from the local
cache with mmap readers (zero HTTP overhead during cycles).

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
| `filtered_neighbor_indices_at(index, dataset)` | `u64, str → str` | Filtered ground-truth indices |
| `filtered_neighbor_distances_at(index, dataset)` | `u64, str → str` | Filtered ground-truth distances |

### Metadata

| Node | Signature | Description |
|------|-----------|-------------|
| `vector_dim(dataset)` | `str → u64` | Dimension count (init-time constant) |
| `vector_count(dataset)` | `str → u64` | Training set size (init-time constant) |
| `query_count(dataset)` | `str → u64` | Query vector count (init-time constant) |
| `neighbor_count(dataset)` | `str → u64` | Ground-truth k per query (init-time constant) |
| `dataset_distance_function(dataset)` | `str → str` | Similarity metric name |
| `dataset_facets(dataset)` | `str → str` | Comma-separated facet list |
| `metadata_indices_count(dataset)` | `str → u64` | Number of predicate result sets |

`vector_dim` and `vector_count` are init-time constants — they
evaluate during constant folding and can drive activity config
(e.g., schema DDL uses `{dim}`, cycle count uses `train_count`).

### Metadata Index Access

| Node | Signature | Description |
|------|-----------|-------------|
| `metadata_indices_len_at(index, dataset)` | `u64, str → u64` | Match count for query (no data load) |
| `metadata_indices_at(index, dataset)` | `u64, str → str` | Matching base ordinals for query |

### Profile Enumeration

| Node | Signature | Description |
|------|-----------|-------------|
| `dataset_profile_count(dataset)` | `str → u64` | Total profile count (init-time constant) |
| `dataset_profile_names(dataset)` | `str → str` | Comma-separated sorted profile names (init-time constant) |
| `dataset_profile_name_at(index, dataset)` | `u64, str → str` | Profile name at sorted index (wrapping) |
| `profile_base_count(index, dataset)` | `u64, str → u64` | Base vector count for profile at index |
| `profile_facets(index, dataset)` | `u64, str → str` | Facet list for profile at index |

Profile names are sorted by base_count (canonical order from
the vectordata crate). Use `dataset_profile_count` as a const
expression for cycle counts to iterate over all profiles:

```yaml
cycles: "{dataset_profile_count('{dataset}')}"
```

### Prebuffer

| Node | Signature | Description |
|------|-----------|-------------|
| `dataset_prebuffer(dataset)` | `str → u64` | Download all facets to cache (init-time) |

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
