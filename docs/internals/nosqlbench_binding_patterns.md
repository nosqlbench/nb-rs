# Interesting Binding Chains from nosqlbench Workloads

Scan of `links/nosqlbench/**/*.yaml` for the most interesting
bindings graphs to test with GK.

## Top Binding Chains for GK Testing

### Longest / Most Complex Composition

**1. Flight Date (12 nodes!)**
Source: `adapter-cqld4/.../bindings/expr.yaml`
```
HashRange(0,2); Mul(3600000); Save('hour'); Shuffle(0,2); Mul(60000); Save('minute');
HashRange(0,60); Mul(1000); Save('second'); Expr('hour + minute + second');
StartingEpochMillis('2018-10-02 04:00:00'); ToDate(); ToString()
```

GK equivalent:
```gk
inputs := (cycle)
h := hash(cycle)
hour_ms := (h % 3) * 3600000
minute_ms := (hash(shuffle(cycle, 0, 3)) % 3) * 60000
second_ms := (hash(hash(cycle)) % 60) * 1000
offset_ms := hour_ms + minute_ms + second_ms
timestamp_ms := offset_ms + 1538452800000  // 2018-10-02 04:00:00 UTC
flight_date := to_timestamp(timestamp_ms)
```
**Status: Supported.** Infix operators replace Expr(). DAG
wiring replaces Save/Load.

**2. Bulky Key-Value CharBuf (4 nodes, massive buffer)**
Source: `baselines/cql_keyvalue2_bulky.yaml`
```
Hash(); Mod(1000000000); CharBufImage('A-Za-z0-9 _|/',16000000,HashRange(50000,150000)); ToString()
```
**Gap: `char_buf` node** — deterministic string from seed + charset + length.

### Higher-Order / Nested Functions

**3-5. Collection Builders**
HOF patterns like `long->HashRange(1,5)->int` are subsumed by
GK's DAG model. The inner function is a node, not a lambda:
```gk
dim := hash_range(hash(cycle), 1, 5)  // replaces long->HashRange(1,5)->int
```

**Remaining gap:** Collection *builders* that need to call an
inner function N times with different seeds. See memo 12 for
the `random_vector`/`random_list` design.

### Distribution / Probability Chains

**6. Normal + Clamp + Conditional**
```gk
risk_score := clamp_f64(dist_normal(hash(cycle), 0.0, 5.0), 1.0, 100.0)
bucket := discretize(risk_score, 90.0, 2)
is_active := select(bucket, 1, 0)
```
**Status: Supported.** `discretize` + `select` replaces `Expr()`.

**7. WeightedStrings**
```gk
platform := weighted_strings(hash(cycle), "android:6;ios:4;linux:2;osx:7;windows:3")
```
**Status: Supported.** `weighted_strings` exists.

### Crypto / Buffer Chains

**8. Digest Pipeline**
```gk
buf := bytes_from_hash(hash(cycle), 1000)
digest := sha256(buf)
hex := to_hex(digest)
```
**Status: Supported.**

### Access Pattern / Partitioning

**9-10. Scaled Hash Ranges and Partitioning**
```gk
scaled := hash(div(cycle, 2)) * 1.0   // HashRangeScaled
partition := (cycle / 10000) % 100
```
**Status: Supported.** Infix operators make this natural.

## Remaining Gaps

| Node | Priority | Status |
|------|----------|--------|
| `char_buf(seed, charset, len)` | Medium | **Done** — `nodes/string.rs` |
| `hashed_uuid(seed)` | High | **Done** — `nodes/string.rs` |
| `random_vector(seed, dim, min, max)` | Medium | **Done** — `nodes/json.rs` |
| `normalize_vector(json)` | Medium | **Done** — `nodes/json.rs` |
| `file_line_at(seed, filename)` | Low | **Done** — `nodes/string.rs` |
| `random_list(seed, size, spec)` | Low | Not yet |
| `random_map(seed, size, k, v)` | Low | Not yet |
| `format_timestamp(ms, pattern)` | Low | Not yet |

## What GK Eliminates

- **Save/Load** → DAG wiring (every intermediate is a named node)
- **Expr()** → infix operators + const expressions
- **HOF lambdas** → nodes in the DAG
- **Type casting** → auto-widening adapters
- **Chain syntax** → DAG composition with named bindings
