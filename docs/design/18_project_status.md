# Project Status — nb-rs

Current state of the nb-rs implementation as of the latest session.

---

## Workspace Structure

```
nb-rs/
├── nb-rs/           — Main binary (stub)
├── nb-variates/     — Generation kernel (complete)
├── nb-metrics/      — Metrics collection (core complete)
├── nb-workload/     — Workload specification (core complete)
├── docs/design/     — 18 SRD documents
└── links/
    ├── nosqlbench   → Java reference implementation
    └── vectordata-rs → Vector dataset crate
```

## Crate Status

### nb-variates (complete for initial scope)

**311 tests. ~114 node types. 23 node modules. 5 sampling modules.**

| Layer | Status | What |
|-------|--------|------|
| Core runtime (Phase 1) | ✓ | GkKernel, pull-through eval, memoization |
| AOT compiler (Phase 2) | ✓ | CompiledKernel, flat u64 buffer, 8-15x speedup |
| DSL parser | ✓ | Lexer, parser, compiler, diagnostics, 70+ registered functions |
| Hashing | ✓ | Hash64, HashRange, HashInterval (xxHash3) |
| Arithmetic | ✓ | Add, Mul, Div, Mod, Clamp, MixedRadix, Interleave, SumN |
| Distributions | ✓ | 14 distributions (Normal, Zipf, Poisson, Beta, etc.) via ICD/LUT |
| Alias sampling | ✓ | AliasTable, AliasTableU64, AliasSample, Histribution |
| MetaShift | ✓ | LFSR shuffle with 8 banks per width (4-64 bit) |
| LUT | ✓ | Standalone interpolating lookup table |
| String ops | ✓ | Combinations, NumberToWords, Printf |
| Datetime | ✓ | EpochScale, EpochOffset, ToTimestamp, DateComponents |
| Encoding | ✓ | HTML, URL, Base64, Base32, Hex encode/decode |
| Digest | ✓ | SHA-256, MD5 |
| Byte buffers | ✓ | BytesFromHash, ByteImageExtract, CharImageExtract |
| JSON | ✓ | First-class Value::Json, JsonObject, JsonArray, merge, field access |
| Noise | ✓ | Perlin 1D/2D, Simplex 2D, Fractal/FBM 1D/2D |
| Weighted | ✓ | WeightedStrings, WeightedU64 |
| Fixed values | ✓ | ConstF64, ConstBool, FixedValues, CoinFlip |
| Type conversions | ✓ | F64ToU64, Round/Floor/Ceil, Discretize, FormatU64/F64, ZeroPad |
| LERP | ✓ | LerpConst, ScaleRange, InvLerp, Remap, Quantize |
| Context | ✓ | CurrentEpochMillis, SessionStartMillis, ElapsedMillis, Counter |
| Diagnostic | ✓ | TypeOf, DebugRepr, Inspect |
| Regex | ✓ | RegexReplace, RegexMatch, RegexExtract |
| Random/fluff | ✓ | RandomRange/F64/Bytes/String/Bool/Lorem, HashedLineToString |
| Real-world data | ✓ | FirstNames, FullNames, StateCodes, CountryNames, Nationalities |
| Vectordata | ✓ | VectorAt, VectorAtBytes, VectorDim, VectorCount (feature-gated) |

### nb-metrics (core complete)

**23 tests. 4 instrument types. Frame capture + coalescing. Scheduler.**

| Layer | Status | What |
|-------|--------|------|
| Labels | ✓ | Arc-shared, composable, OpenMetrics rendering |
| Counter | ✓ | AtomicU64, monotonic |
| Histogram | ✓ | Delta HDR (swap-and-reset), 3 sig digits |
| Timer | ✓ | Histogram + count |
| Gauge | ✓ | FnGauge (closure) + ValueGauge (atomic f64) |
| Frame | ✓ | MetricsFrame, Sample variants, coalescing rules |
| Scheduler | ✓ | Dedicated thread, hierarchical tree, concurrent delivery |
| Console reporter | ✓ | Delta rates, percentiles, grouped by activity |
| CSV reporter | Pending | |
| SQLite reporter | Pending | |
| Prometheus push | Pending | |
| HDR log reporter | Pending | |

### nb-workload (core complete)

**33 tests. Full YAML parsing and normalization.**

| Layer | Status | What |
|-------|--------|------|
| TEMPLATE expansion | ✓ | Pre-YAML macro substitution |
| YAML parsing | ✓ | All shorthand forms normalized |
| Property inheritance | ✓ | Doc → block → op merging |
| Auto-naming | ✓ | stmt1, block0, etc. |
| Auto-tagging | ✓ | block, name, op tags |
| Scenarios | ✓ | Single/multi step, named/unnamed |
| Bind points | ✓ | {name} reference, {{expr}} inline |
| Tag filtering | ✓ | Exact + regex, AND conditions |
| Capture points | Pending | [name] syntax for result capture |
| Spectest validation | Pending | Parse markdown spec files as tests |

---

## Design Documents (SRD)

| # | Document | Status |
|---|----------|--------|
| 00 | Assay Summary | ✓ |
| 01 | Subsystem Scope | ✓ |
| 02 | Variate Generation | ✓ (all Qs resolved) |
| 03 | Virtdata Function Catalog | ✓ |
| 04 | GK Function Library | ✓ |
| 05 | GK DSL (design history) | ✓ |
| 06 | GK Language Layers | ✓ |
| 07 | GK Rust Model | ✓ |
| 08 | Binding Recipe Catalog | ✓ |
| 09 | Alias Method | ✓ |
| 10 | AOT Compilation | ✓ |
| 11 | ICD Sampling | ✓ |
| 12 | Init vs Cycle State | ✓ |
| 13 | Init Objects | ✓ |
| 14 | Unified DSL Syntax | ✓ (canonical) |
| 15 | Metrics Assay | ✓ |
| 16 | Metrics Design | ✓ (locked in) |
| 17 | Workload Spec Assay | ✓ |

---

## Upcoming Design Topics

These need discussion before implementation:

1. **Execution layer** — fresh design for activities, motors, the
   execution loop. User wants to depart from nosqlbench's approach.
2. **Rate limiter** — user has specific requirements to discuss.
3. **Adapter API** — how drivers plug in (trait-based, not SPI).
4. **Capture points** — result capture for verification workflows.

---

## Test Summary

| Crate | Tests |
|-------|------:|
| nb-variates | 311 |
| nb-metrics | 23 |
| nb-workload | 33 |
| **Total** | **367** |
