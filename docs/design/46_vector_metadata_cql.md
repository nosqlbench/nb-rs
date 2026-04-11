# SRD 46: Vector Metadata and Predicates — CQL Dialect Adapter

## Overview

Vector search workloads need more than just vectors. Real-world ANN
queries involve:

- **Metadata** attached to each vector (e.g., category, price, date,
  tags) — written alongside vectors, queried for filtered search
- **Predicates** that filter the search space (e.g., `price < 100
  AND category = 'electronics'`) — applied at query time
- **Ground truth** with filtering — which neighbors are correct when
  predicates are applied

The `vectordata` crate provides these as structured binary data via
the `veks-anode` wire format (MNode for metadata, PNode for
predicates). The `cassnbrs` persona needs to translate this data
into CQL for Cassandra/SAI vector workloads.

## The Translation Problem

```
vectordata (slab files)
    │
    ├── MNode binary → { price: 42.50, category: "electronics", tags: ["sale", "new"] }
    │                       │
    │                       ▼
    │               CQL INSERT: ... VALUES (42.50, 'electronics', ['sale', 'new'])
    │               CQL prepared: bind(price=42.50, category="electronics", ...)
    │
    └── PNode binary → (price < 100) AND (category = 'electronics')
                            │
                            ▼
                    CQL WHERE: price < 100 AND category = 'electronics'
                    SAI index predicates for filtered ANN
```

The adapter must:
1. Decode MNode/PNode from slab files at init or cycle time
2. Render metadata values in CQL syntax for INSERTs
3. Render predicates in CQL WHERE clause syntax
4. Support CQL prepared statement binding with native types
5. Generate CQL CREATE TABLE DDL from MNode field schemas

## Architecture

### GK Node Functions

New node functions expose metadata and predicates into the GK space:

| Function | Input | Output | Description |
|----------|-------|--------|-------------|
| `metadata_at(index, source)` | u64, str | String (JSON) | Decode MNode at index, render as JSON |
| `metadata_cql_values(index, source, [fields])` | u64, str, [str] | String | Render MNode fields as CQL value list |
| `metadata_cql_columns(source, [fields])` | str, [str] | String | Return CQL column names from schema |
| `metadata_cql_types(source, [fields])` | str, [str] | String | Return CQL CREATE TABLE column defs |
| `predicate_at(index, source)` | u64, str | String (JSON) | Decode PNode at index, render as JSON |
| `predicate_cql_where(index, source)` | u64, str | String | Render PNode as CQL WHERE clause (raw mode) |
| `predicate_result_at(index, source)` | u64, str | String | Get filtered GT indices for predicate |

The `[fields]` parameter is optional; default includes all fields.

The predicate and metadata nodes produce string output for **raw**
mode. For **parameterized** and **prepared** modes, the CQL adapter's
`OpDispenser` reads the underlying typed data from the node via
`Value::Ext` / `ResultBody::as_any()` downcast and binds natively.

These nodes use `veks-anode`'s existing CQL vernacular renderers
internally. The node constructor opens the slab file handle; cycle-
time access is random-access I/O managed by the slabtastic crate.

### CQL Workload Pattern

A typical vector + metadata + predicate workload:

```yaml
params:
  dataset: my-vectors
  keyspace: vector_bench
  table: items

bindings: |
  coordinates := (cycle)
  dim := vector_dim("{dataset}")
  count := vector_count("{dataset}")
  base_vec := vector_at(cycle, "{dataset}")
  query_vec := query_vector_at(mod(cycle, 1000), "{dataset}")
  meta_values := metadata_cql_values(cycle, "{dataset}")
  predicate := predicate_cql_where(mod(cycle, 100), "{dataset}")
  gt := neighbor_indices_at(mod(cycle, 1000), "{dataset}")

ops:
  schema:
    tags: { phase: schema }
    stmt: |
      CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
        id bigint PRIMARY KEY,
        embedding vector<float, {dim}>,
        {metadata_cql_types}
      )

  write:
    tags: { phase: write }
    ratio: 10
    stmt: |
      INSERT INTO {keyspace}.{table} (id, embedding, {metadata_cql_columns})
      VALUES ({cycle}, {base_vec}, {meta_values})

  search:
    tags: { phase: read }
    ratio: 1
    stmt: |
      SELECT id FROM {keyspace}.{table}
      WHERE {predicate}
      ORDER BY embedding ANN OF {query_vec}
      LIMIT 100
```

### Prepared Statement Binding

For the `write` op, the CQL adapter's `OpDispenser` needs to bind
metadata values with native CQL types. The flow:

1. `map_op` at init time: inspect the template, extract
   `metadata_cql_columns` to determine column names and types
2. Per cycle: `metadata_at` returns the MNode as structured data
   (not just a string). The dispenser reads typed `MValue` fields
   and binds them natively via `cassandra-cpp`:
   - `MValue::Int` → `stmt.bind_int64_by_name()`
   - `MValue::Float` → `stmt.bind_double_by_name()`
   - `MValue::Text` → `stmt.bind_string_by_name()`
   - `MValue::UuidV7` → `stmt.bind_uuid_by_name()`
   - `MValue::List` → `stmt.bind_list_by_name()`

This requires the metadata node to output typed `Value::Ext` data
that the CQL dispenser can downcast via `as_any()`.

### veks-anode Integration

The `veks-anode` crate (published on crates.io) provides:

- `ANode::decode(bytes)` — auto-detect and decode MNode/PNode
- `MNode` with 29 typed `MValue` variants
- `PNode` with predicate tree evaluation
- `Vernacular::Cql` renderer — `render(anode, Vernacular::Cql)`
- `Vernacular::CqlSchema` renderer — generates type declarations
- Zero-copy slab scanning via `CompiledScanPredicates`

The node functions compose these primitives:
- `metadata_cql_values` = decode MNode from slab → render each
  field with `mvalue_to_cql()` → join with commas
- `predicate_cql_where` = decode PNode from slab → render with
  `pnode_to_cql()` → returns a WHERE clause fragment
- `metadata_cql_types` = read field schema from first MNode →
  render with `mvalue_type_to_cql()` → returns column definitions

### Dependencies

```toml
[dependencies]
veks-anode = "0.x"       # MNode/PNode codec + vernacular renderers
slabtastic = "0.x"       # Random-access slab file I/O
vectordata = "0.9"       # Dataset loading, facet access
```

These are all published crates from the same workspace.

## Design Decisions

1. **Schema discovery from data.** The metadata schema is presumed
   structurally consistent within a data source. The adapter reads the
   first MNode record at init time to discover field names and types,
   then uses this schema for DDL generation and prepared statement
   column mapping. No explicit schema declaration needed in the
   workload.

2. **Three binding modes for predicates and metadata.** Following the
   pattern established by the Java cqld4 adapter, three CQL execution
   modes are supported:

   - **Raw**: String interpolation after vernacular rendering.
     The predicate or metadata values are rendered to CQL text and
     spliced directly into the query string. No preparation, no
     parameterization. Simplest but least efficient.

   - **Parameterized**: Values are extracted from the MNode/PNode as
     typed arguments and passed as bind variables to an unprepared
     statement (`session.execute("SELECT ... WHERE price < ?", [42.5])`).
     The query string uses `?` placeholders; the values are passed
     separately. No prepared statement caching.

   - **Prepared**: The query template (with `?` placeholders) is
     prepared once at init time. Per-cycle, typed values from
     MNode/PNode are bound to the prepared statement. Maximum
     efficiency — the server parses the query once, bind variables
     avoid serialization overhead.

   The binding logic differs for each mode:
   - Raw: `veks-anode` vernacular renderer → string interpolation
   - Parameterized: extract typed `MValue`/`Comparand` → bind as
     native CQL types via driver API
   - Prepared: same as parameterized, plus init-time `prepare()` call

3. **Slab caching is handled by vectordata.** Verified: the
   `vectordata` crate manages metadata and predicate slab files
   through the same facet/view/cache pipeline as vector data. The
   `TestDataView::metadata_content()` returns a `TypedVectorView`
   backed by `CachedChannel` for remote datasets and `MmapVectorReader`
   for local ones. Multiple nodes referencing the same dataset share
   the view's cached handles. No separate cache needed in nb-rs.

4. **Field filtering with default-all.** A field filter parameter
   controls which metadata fields are included in CQL operations.
   Default is all fields. Syntax: `fields="price,category,tags"`
   as a const parameter on metadata node functions. This allows
   workloads to target specific columns without loading/rendering
   irrelevant fields.
