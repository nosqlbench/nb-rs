# Binding Recipe Catalog — Patterns from Java nosqlbench

Survey of 107 YAML workload files containing ~838 distinct binding
expressions across the Java nosqlbench project. This catalogs how users
actually compose data generation pipelines, organized by pattern rather
than by file.

---

## Survey Summary

- **107 YAML files** with `bindings:` sections
- **~838 distinct binding expressions**
- **~145 different function names** used

## Top Functions by Frequency

| Rank | Function | Uses | Category |
|-----:|----------|-----:|----------|
| 1 | TEMPLATE | 520 | Parameterization macro |
| 2 | ToString | 109 | Type conversion |
| 3 | Add | 79 | Arithmetic |
| 4 | Mul | 67 | Arithmetic |
| 5 | ToCqlVector | 40 | Adapter-specific |
| 6 | HashRange | 36 | Hashing |
| 7 | NumberNameToString | 34 | String |
| 8 | HashedFileExtractToString | 32 | File I/O |
| 9 | Uniform | 30 | Distribution |
| 10 | Mod | 29 | Arithmetic |
| 11 | Hash | 25 | Hashing |
| 12 | HdfFileToFloatArray | 28 | Vector I/O |
| 13 | Identity | 20 | Identity |
| 14 | Div | 19 | Arithmetic |
| 15 | CharBufImage | 16 | String/buffer |
| 16 | Prefix | 12 | String |
| 17 | ToHexString | 11 | Type conversion |
| 18 | DoubleVectors | 11 | Vector |
| 19 | ToBigDecimal | 10 | Type conversion |
| 20 | ToHashedUUID | 9 | UUID |

---

## Pattern Categories

### 1. Simple Identity and Conversion (~130 expressions)

Single-function bindings. The starting point for most users.

```
Identity()
ToString()
ToHashedUUID()
NumberNameToString()
AlphaNumericString(50)
```

**GK mapping:** These are either identity coordinates or single unary
nodes. The simplest possible GK topology.

### 2. Two-Function Chains (~108 expressions)

A source function piped into one conversion.

```
Mod(10000000); ToString()
Hash(); ToString()
ByteBufferSizedHashed(30); ToHexString()
Uniform(0,1000000000)->int; ToString()
```

**GK mapping:** Two-node chains. In many cases, the second function is a
type-crossing edge adapter (ToString, ToHexString) that the GK would
auto-insert.

### 3. Multi-Function Chains (3+) (~217 expressions)

The bulk of real-world bindings. Complex pipelines building structured
data through multiple transformations.

```
AddHashRange(0,2419200000L); StartingEpochMillis('2018-02-01'); StringDateWrapper("yyyy-MM-dd HH:mm:ss")
Hash(); Mod(1000000000); CharBufImage('A-Za-z0-9',16000000,HashRange(50000,150000)); ToString()
Div(2L); Hash(); HashRangeScaled(1.0d); ToString()
Mul(25); Add(1); HdfFileToFloatArray("path", "/train"); ToCqlVector()
```

**GK mapping:** Multi-node DAG paths. Notably, the Java model forces
these into linear chains even when the data flow is conceptually
branching. In the GK, shared intermediate values (like a hashed ID used
by multiple downstream fields) would be a single node with multiple
consumers.

### 4. Statistical Distributions (~54 expressions)

20+ distribution types used for shaped randomness.

**Continuous:**
```
Normal(5.0, 1.0)
Uniform(0.0, 100.0)
Exponential(1.5)
Pareto(1.0, 3.0)
LogNormal(1.0, 0.25)
Beta(2.0, 2.0)
Weibull(1.0, 1.5)
```

**Discrete:**
```
Zipf(10, 5.0)
Poisson(5.0)
Binomial(8, 0.5)
Geometric(0.5)
Hypergeometric(40, 20, 10)
```

**GK mapping:** These are Tier 1 nodes in `04_gk_function_library.md`.
In the GK, they require explicit upstream hashing (the distribution
nodes receive uniform input). The Java versions implicitly hash.

### 5. Hashing Patterns (~50+ expressions)

Explicit and implicit hashing for deterministic pseudo-random values.

```
Hash()
Hash(); Mod(1000000000); ToString()
HashRange(0, 1000000)
AddHashRange(0, 2419200000L)
Div(2L); Hash(); HashRangeScaled(1.0d)
```

**GK mapping:** Direct equivalents to Hash64, HashRange in the GK. The
`AddHashRange` pattern (add a hash-derived offset to the input) is a
common idiom that could be a convenience node or a two-node composition
(hash → add_wire).

### 6. Template / String Interpolation (~520 TEMPLATE uses)

The most common pattern overall — parameterizing workloads with
configurable values.

```
TEMPLATE(keycount, 1000000000)
TEMPLATE(valuecount, 1000000000)
Template('{}, {}', LastNames(), FirstNames())
Template("user-{}", ToString())
```

**GK mapping:** Two distinct concepts here:
- `TEMPLATE(key, default)` is a macro evaluated at parse time — maps to
  the surface-level expression language (design decision #9 in
  `04_gk_function_library.md`).
- `Template('{}, {}', fn1(), fn2())` is string interpolation — maps
  directly to the GK's string interpolation sugar:
  `name := "{last}, {first}"`

### 7. Realistic Data (Names, Places) (~23 functions)

CSV-backed weighted lookup for real-world data.

```
FirstNames()
FullNames()
Cities()
CitiesByPopulation()
StateCodes()
ZipCodesByPopulation()
WeightedStrings('Gryffindor:0.2;Hufflepuff:0.2;...')
```

**GK mapping:** All collapse to the single `WeightedLookup` node from
`04_gk_function_library.md`, parameterized with different data files.
The `WeightedStrings` inline form becomes a `WeightedLookup` with an
inline table instead of a CSV file.

### 8. UUID and Time-Based IDs (~20 expressions)

```
ToHashedUUID()
ToEpochTimeUUID('2018-02-01 05:00:00')
AddHashRange(0,2419200000L); ToEpochTimeUUID()
ToFinestTimeUUID('2018-02-01 05:00:00')
```

**GK mapping:** Tier 2 nodes (ToUUID, ToTimeUUID). The epoch offset
pattern maps to: hash → add → to_time_uuid — a three-node chain.

### 9. Collection Builders (~59 expressions)

```
ListSizedHashed(5, HashRange(0.0f, 100.0f))
SetSizedStepped(HashRange(3,4), Identity(), NumberNameToString())
MapSized(2, NumberNameToString(), ToString())
ListFunctions(Identity(), NumberNameToString(), Template(...))
```

**GK mapping:** These are N→1 nodes in the GK (ListOf, MapOf, SetOf).
The Java versions bundle sizing, stepping, and hashing into the
collection constructor because virtdata was unary. In the GK, sizing
comes from an upstream node, and element generation is upstream wiring.

### 10. Save/Load State (~5 expressions, but complex)

```
Save('cycle'); HashRange(0,2); Mul(3600000); Load('cycle');
    Save('hour'); ... Expr('hour + minute + second');
    StartingEpochMillis('2018-10-02'); ToDate()
```

**GK mapping:** This entire category is **eliminated by the DAG model**.
Save/Load existed because Java virtdata couldn't share intermediate
values between bindings. In the GK, named wires provide this naturally:

```
hour := mul(hash_range(cycle, 0, 2), 3600000)
minute := mul(hash_range(cycle, 0, 59), 60000)
second := mul(hash_range(cycle, 0, 59), 1000)
epoch := add(add(add(hour, minute), second), 1538452800000)
```

### 11. Vector / Embedding Operations (~59 expressions)

```
HashedFloatVectors(768); ToCqlVector()
HdfFileToFloatList("path.hdf5", "/train"); ToCqlVector()
FVecReader("path.fvec", 128, 0); ToCqlVector()
DoubleVectors('0-9*10'); NormalizeDoubleVector()
ListSizedHashed(10, HashRange(0.0f, 1.0f))
```

**GK mapping:** Tier 3 (deferred). These are adapter-specific and
depend on external data files. When implemented, they would be
specialized I/O nodes.

### 12. File-Based Data (~32 expressions)

```
HashedFileExtractToString('data/lorem_ipsum_full.txt', 50, 150)
HashedLineToString('data/careers.txt')
CSVSampler('csv/binding_keys.csv', 'key')
```

**GK mapping:** Generic file-backed lookup nodes, similar to
WeightedLookup but with different access patterns (random extract vs.
line selection vs. CSV sampling).

---

## Key Insights for GK Design

1. **TEMPLATE is not a binding function** — it's a macro. Keep this
   in the surface expression language, not in the GK node library.

2. **ToString appears 109 times** — most of these would be auto-inserted
   edge adapters in the GK. Confirms the design decision.

3. **Save/Load is a workaround** — the DAG model eliminates the need
   entirely. This validates the most fundamental design choice.

4. **3+ function chains are the norm** (217 expressions) — users build
   complex pipelines. The GK must handle this efficiently (Phase 2
   compilation matters).

5. **Distribution functions need explicit hashing** — in the GK model,
   the user must wire `hash → distribution`. This is more explicit but
   also more composable. Need good documentation and examples.

6. **Collection builders need rethinking** — Java had 20+ collection
   classes with baked-in sizing/stepping. The GK's *-arity nodes
   handle this more naturally but may need a `Repeat` node for
   generating sized collections from a count + element function.

7. **Parameterization is ubiquitous** (520 TEMPLATE uses) — the
   surface expression language must support this ergonomically.

8. **Vector operations are growing** — 59 expressions, mostly recent.
   Worth considering for earlier inclusion than originally planned.
