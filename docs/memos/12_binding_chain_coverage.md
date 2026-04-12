# Memo 12: Covering nosqlbench Binding Chains in GK

How to support every binding pattern from memo 10 using GK's
DAG-native idiom. For each Java chain, this memo shows the GK
equivalent and identifies what's missing.

---

## Principle: DAG over Chain

Java nosqlbench modeled bindings as linear function chains:
`Hash(); Mod(1000); ToString()`. Each function's output fed the
next function's input — a pipeline. Branching required Save/Load
to stash values into a side context and recall them later.

GK models bindings as a DAG. Every intermediate value is a named
node. Branching is just wiring two downstream nodes to the same
upstream node. No Save/Load needed — the DAG IS the context.

This eliminates Save/Load, Expr(), and most HOF patterns. What
remains are genuine gaps: collection builders, templates, and
conditional dispatch.

---

## Chain-by-Chain Translation

### 1. Flight Date (Save/Load + Expr)

Java:
```
HashRange(0,2); Mul(3600000); Save('hour');
Shuffle(0,2); Mul(60000); Save('minute');
HashRange(0,60); Mul(1000); Save('second');
Expr('hour + minute + second');
StartingEpochMillis('2018-10-02 04:00:00'); ToDate(); ToString()
```

GK (today):
```gk
inputs := (cycle)
h := hash(cycle)
hour_raw := mod(h, 3)
hour_ms := mul(hour_raw, 3600000)

m := hash(shuffle(cycle, 0, 3))
minute_raw := mod(m, 3)
minute_ms := mul(minute_raw, 60000)

s := hash(hash(cycle))
second_raw := mod(s, 60)
second_ms := mul(second_raw, 1000)

offset_ms := sum(hour_ms, minute_ms, second_ms)
base_epoch := 1538452800000   // 2018-10-02 04:00:00 UTC
timestamp_ms := add(offset_ms, base_epoch)
flight_date := to_timestamp(timestamp_ms)
```

**Status: Fully supported.** No Save/Load needed — `hour_ms`,
`minute_ms`, `second_ms` are just named nodes that `sum` wires
to. The Expr('hour + minute + second') becomes `sum(a, b, c)`.

**Gap: none.** The `to_timestamp` node returns a formatted
string. If we need date formatting beyond epoch→ISO-8601, a
`format_timestamp(ms, pattern)` node would be useful but not
blocking.

### 2. Bulky CharBuf

Java:
```
Hash(); Mod(1000000000); CharBufImage('A-Za-z0-9 _|/',16000000,HashRange(50000,150000)); ToString()
```

GK:
```gk
inputs := (cycle)
seed := mod(hash(cycle), 1000000000)
buf_len := scale_range(hash(hash(cycle)), 50000, 150000)
value := char_buf(seed, 'A-Za-z0-9 _|/', buf_len)
```

**Gap: `char_buf` node.** Needs a new node that generates a
deterministic string of length `buf_len` from a character set
and a seed. The string content is derived from the seed so it's
reproducible. This is a P1-only node (returns String, not u64).

Implementation: straightforward. The node takes a seed (wire),
charset (const string), and length (wire). Internally it uses
the seed to index into the charset repeatedly.

### 3-5. Collection Builders (Vectors, Lists, Sets, Maps)

Java:
```
HashedDoubleVectors(long->HashRange(1,5)->int, long->HashRange(2.0d,3.0d)->double)
SetSizedStepped(HashRange(3,4), ListSizedStepped(HashRange(2,3), Combinations(...)))
MapSized(2, NumberNameToString(), MapSized(2, NumberNameToString(), long->ToString()))
```

These use higher-order functions (HOFs) where inner functions
are passed as arguments and called repeatedly to fill a
collection. The `long->HashRange(1,5)->int` syntax is a lambda
that takes a long, applies HashRange, and casts to int.

GK approach: **don't use HOFs.** Instead, use vectorized
nodes that generate entire collections deterministically from
a seed:

```gk
inputs := (cycle)
// Vector of 3-5 random doubles in [2.0, 3.0]
dim := scale_range(hash(cycle), 3, 5)
vector := random_vector(hash(hash(cycle)), dim, 2.0, 3.0)
normalized := normalize_vector(vector)

// List of 2-3 combination strings
list_size := scale_range(hash(cycle), 2, 3)
items := random_list(hash(hash(cycle)), list_size, 'A-Z;0-9;a-z')

// Map of N key-value pairs
map := random_map(hash(cycle), 2, number_to_words, number_to_words)
```

**Gap: collection builder nodes.** Need:
- `random_vector(seed, dim, min, max)` → JSON array of f64
- `random_list(seed, size, element_spec)` → JSON array of strings
- `random_map(seed, size, key_spec, value_spec)` → JSON object
- `normalize_vector(vec_json)` → normalized JSON array

The vector nodes (`vector_at`, etc.) already exist for dataset
access. What's missing is *generation* — building vectors/lists
from a seed rather than looking them up from a file.

The element_spec / key_spec / value_spec parameters are the
hard part. In Java, these were HOF lambdas. In GK, we have
two options:

**Option A: Spec strings.** The spec is a DSL-in-a-string:
`random_list(seed, 3, "combinations('A-Z;0-9;a-z')")`. The
node parses the spec at init time and builds an internal
generator. Simple but not composable.

**Option B: Multi-output seed expansion.** The collection node
takes a seed and size, outputs N seeds (one per element). Each
seed feeds a separate GK subgraph. The subgraph output feeds
back into a `collect_json` aggregator node.

Option B is more GK-idiomatic (composable, visible in the DAG)
but requires multi-output nodes feeding variable-width arrays,
which the current buffer model doesn't support. The buffer is
fixed-width u64 slots.

**Recommendation: Option A for now** (spec strings), with the
understanding that the vectordata nodes already handle the
most important case (pre-built dataset vectors). Generated
collections are a workload convenience, not a core capability.

### 6. Conditional (Normal + Clamp + Save + Expr)

Java:
```
Normal(0.0,5.0); Clamp(1,100); Save('riskScore')
Expr('riskScore > 90 ? 0 : 1'); ToBoolean(); ToString()
```

GK:
```gk
inputs := (cycle)
raw_score := dist_normal(hash(cycle), 0.0, 5.0)
risk_score := clamp_f64(raw_score, 1.0, 100.0)

// Conditional: risk_score > 90 ? 0 : 1
// Use select node: select(condition, if_true, if_false)
is_high_risk := select(discretize(risk_score, 90.0, 2), 0, 1)
```

**Gap: `select` node needs a 3-input variant** (condition,
if_true, if_false). The current `select` takes (selector, ...values)
which is close but selector-indexed, not boolean-conditional.

Actually, this works with the existing `select`:
```gk
bucket := discretize(risk_score, 90.0, 2)  // 0 if <90, 1 if >=90
result := select(bucket, 1, 0)  // bucket=0→"1", bucket=1→"0"
```

**Status: Supported** with `discretize` + `select`. The
`discretize` node quantizes a continuous value into buckets,
and `select` picks from a list by index.

For the general case of `Expr('arbitrary expression')`: GK
doesn't need this because any expression can be modeled as
a subgraph of arithmetic/comparison nodes. The DAG IS the
expression tree.

### 7. WeightedStrings

Java:
```
WeightedStrings('android:6;ios:4;linux:2;osx:7;windows:3')
```

GK:
```gk
inputs := (cycle)
platform := weighted_strings(hash(cycle), 'android:6;ios:4;linux:2;osx:7;windows:3')
```

**Status: Fully supported.** `weighted_strings` already exists.

### 8. Digest Pipeline

Java:
```
ByteBufferSizedHashed(1000); DigestToByteBuffer('SHA-256'); ToHexString()
```

GK:
```gk
inputs := (cycle)
buf := bytes_from_hash(hash(cycle), 1000)
digest := sha256(buf)
hex := to_hex(digest)
```

**Status: Fully supported.** `bytes_from_hash`, `sha256`,
`to_hex` all exist.

### 9. Scaled Hash Ranges

Java:
```
Div(2L); Hash(); HashRangeScaled(1.0d); Hash(); ToString()
```

GK:
```gk
inputs := (cycle)
scaled := div(cycle, 2)
h1 := hash(scaled)
f := scale_range(h1, 0.0, 1.0)
h2 := hash(round_to_u64(f))
out := format_u64(h2)
```

**Status: Supported.** `scale_range` does the HashRangeScaled
work. `format_u64` converts to string.

### 10. Multi-Modulo Partitioning

Java:
```
Div(10000); Mod(100); ToHashedUUID()
```

GK:
```gk
inputs := (cycle)
partition := mod(div(cycle, 10000), 100)
uuid := hashed_uuid(hash(partition))
```

**Gap: `hashed_uuid` node.** Needs a node that deterministically
produces a UUID from a u64 seed. Format: standard UUID v4 string
with the random bits derived from the seed.

Implementation: trivial. Take the seed's hash bits, set the
version/variant bits per RFC 4122, format as
`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`.

---

## Gap Summary

| Gap | Priority | Effort | Chains |
|-----|----------|--------|--------|
| `hashed_uuid(seed)` | High | Trivial | #10, common in CQL |
| `char_buf(seed, charset, len)` | Medium | Small | #2 |
| `random_vector(seed, dim, min, max)` | Medium | Small | #3, #4 |
| `normalize_vector(json)` | Medium | Small | #3, #4 |
| `random_list(seed, size, spec)` | Low | Medium | #5 |
| `random_map(seed, size, k_spec, v_spec)` | Low | Medium | #5 |
| `format_timestamp(ms, pattern)` | Low | Small | #1 (optional) |

All gaps are node implementations — no GK architecture changes
needed. The DAG model handles every composition pattern from
the Java chains without Save/Load, Expr, or HOFs.

### What GK eliminates

- **Save/Load**: DAG wiring replaces context stashing. Every
  intermediate value is a named node, accessible by any
  downstream node.

- **Expr()**: Arithmetic expressions become subgraphs.
  `Expr('a + b * c')` → `sum(a, mul(b, c))`. The DAG IS the
  expression tree.

- **HOF lambdas** (`long->HashRange(1,5)->int`): These were
  needed because Java bindings were linear chains. In GK,
  the inner function is just another node in the DAG.

- **Type casting** (`-> int`, `-> long`): GK's u64 buffer
  model eliminates most casts. f64 values are bit-packed.
  String conversions use explicit nodes (`format_u64`,
  `to_json`).

### Implementation order

1. `hashed_uuid` — unblocks CQL workloads that use UUID keys
2. `char_buf` — unblocks bulk value generation workloads
3. `random_vector` + `normalize_vector` — unblocks generated
   vector workloads (dataset vectors already work via vectordata)
4. Collection builders — nice to have, not blocking any known
   workload
