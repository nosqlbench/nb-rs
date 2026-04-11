# SRD 47: Result Validation and Recall Measurement

Defines the framework for verifying operation results and computing
information retrieval metrics (recall@k, precision@k, F1, reciprocal
rank, average precision). Builds on the existing op-wrapping pipeline
(SRD 38) and capture points (SRD 40) to add composable validation
without touching adapter internals.

---

## Background

### Java nosqlbench

Java nosqlbench supports result validation via `verifier-init:` and
`verifier:` blocks in workload YAML. These are Groovy scripts with
access to a `result` variable and shared `Binding` state:

```yaml
# Java nosqlbench workload pattern
verifier-init: |
  relevancy = new RelevancyMeasures(_parsed_op)
  relevancy.addFunction(RelevancyFunctions.recall("recall", topK))
  relevancy.addFunction(RelevancyFunctions.precision("precision", topK))

verifier: |
  actual = cql_utils.cqlStringColumnToIntArray("key", result)
  relevancy.accept({relevant_indices}, actual)
  return true
```

**What worked well:** Per-template validation, pluggable metric
functions, metrics published as gauges alongside latency.

**What we're dropping:**
- Groovy scripting. nb-rs has no embedded scripting engine.
  Validation logic is expressed declaratively in the workload YAML
  and compiled at init time — no runtime code generation.
- `RelevancyMeasures` as a user-instantiated Java object. In nb-rs,
  relevancy functions are built-in compute functions selected by name.
- Per-adapter utility classes (`CqlUtils`, `WeaviateAdapterUtils`).
  In nb-rs, result extraction goes through the `ResultBody` trait
  (SRD 38) — adapters expose native results via `as_any()`, and
  universal fallback uses `to_json()`.

---

## Design

### Validation as an Op Wrapper

Validation is a **composable dispenser wrapper**, same pattern as
`TraversingDispenser` (SRD 40). The `ValidatingDispenser` wraps an
inner `OpDispenser`, intercepts its `OpResult`, and applies validation
logic before returning the result to the executor.

```
  Init time:
  ┌───────────────┐     ┌──────────────────┐     ┌────────────────────┐
  │ DriverAdapter │     │ TraversingDisp.  │     │ ValidatingDisp.   │
  │   map_op()    │────▶│   wrap()         │────▶│   wrap()          │
  │               │     │ (element/byte    │     │ (result checks,   │
  └───────────────┘     │  counting)       │     │  relevancy funcs) │
                        └──────────────────┘     └────────────────────┘

  Cycle time:
  ┌──────────────────┐
  │ ValidatingDisp.  │
  │   execute()      │
  │                  │     ┌──────────────────┐
  │  1. delegate ────┼────▶│ inner.execute()  │
  │                  │◀────┤                  │
  │  2. extract      │     └──────────────────┘
  │     result data  │
  │  3. compare      │
  │     vs expected  │
  │  4. compute      │
  │     metrics      │
  │  5. record       │
  │     pass/fail    │
  └──────────────────┘
```

The wrapper is **only applied when the template declares validation**.
Templates without `verify:` or `relevancy:` blocks get no wrapper and
pay zero overhead.

### Wrapping Order

Wrappers compose outside-in. The executor sees the outermost wrapper:

```
ValidatingDispenser
  └─ TraversingDispenser
       └─ raw OpDispenser (from adapter)
```

`TraversingDispenser` runs first (innermost), ensuring element/byte
counting and capture extraction happen before validation. The
`ValidatingDispenser` sees the fully-populated `OpResult` with
captures already resolved.

---

## Workload YAML Syntax

### Simple Assertions

```yaml
ops:
  read_user:
    prepared: "SELECT name, balance FROM users WHERE id = {user_id}"
    verify:
      - field: name
        is: not_null
      - field: balance
        gte: 0
```

### Relevancy Metrics (Vector Search)

```yaml
ops:
  ann_search:
    prepared: >
      SELECT key FROM vectors
      ORDER BY embedding ANN OF {query_vector}
      LIMIT {topk}
    relevancy:
      actual: key              # column name to extract from results
      expected: "{relevant}"   # GK binding producing ground truth int[]
      k: 10                    # @k limit
      functions:
        - recall
        - precision
        - f1
        - reciprocal_rank
        - average_precision
```

### Custom Field Comparisons

```yaml
ops:
  exact_match:
    prepared: "SELECT data FROM kv WHERE key = {key}"
    verify:
      - field: data
        eq: "{expected_data}"  # GK binding for expected value
```

---

## Validation Modes

### Mode 1: Field Assertions

Simple predicate checks on result fields. Compiled at init time from
the `verify:` block into a list of `AssertionSpec`:

```rust
/// A single field assertion, parsed from the workload YAML.
struct AssertionSpec {
    /// Field name to extract from result (via ResultBody::to_json)
    field: String,
    /// The predicate to apply
    predicate: AssertionPredicate,
}

enum AssertionPredicate {
    /// Field must equal expected value (string comparison or typed)
    Eq(String),
    /// Field must not be null/missing
    NotNull,
    /// Field must be null/missing
    IsNull,
    /// Numeric: field >= threshold
    Gte(f64),
    /// Numeric: field <= threshold
    Lte(f64),
    /// String: field contains substring
    Contains(String),
    /// String: field matches regex
    Matches(String),
}
```

Assertion predicates are evaluated against the result's JSON
representation. If any assertion fails, the validation records a
failure metric and optionally logs the mismatch.

### Mode 2: Relevancy Functions

Information retrieval metrics for vector/ANN search. The `relevancy:`
block compiles into a `RelevancyConfig` at init time:

```rust
/// Relevancy measurement configuration, parsed from workload YAML.
struct RelevancyConfig {
    /// Column name to extract actual result indices from
    actual_field: String,
    /// GK binding name that produces ground truth indices
    expected_binding: String,
    /// Maximum k for @k metrics
    k: usize,
    /// Which functions to compute
    functions: Vec<RelevancyFn>,
}

enum RelevancyFn {
    Recall,
    Precision,
    F1,
    ReciprocalRank,
    AveragePrecision,
}
```

---

## Relevancy Computation

### Core Algorithms

All functions operate on two sorted `&[i64]` slices: `relevant`
(ground truth) and `actual` (from query result), both truncated to
the first `k` elements.

```rust
/// Compute functions for information retrieval metrics.
///
/// All functions expect sorted, deduplicated slices.
pub mod relevancy {
    /// Count elements present in both sorted slices.
    /// Two-pointer O(n+m) scan — no allocation.
    pub fn intersection_count(a: &[i64], b: &[i64]) -> usize {
        let (mut i, mut j, mut count) = (0, 0, 0usize);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => { count += 1; i += 1; j += 1; }
            }
        }
        count
    }

    /// Recall@k: fraction of relevant items found in top-k results.
    ///
    ///   recall = |relevant ∩ actual| / k
    ///
    /// Both slices are pre-truncated to k elements by the caller.
    pub fn recall(relevant: &[i64], actual: &[i64], k: usize) -> f64 {
        if k == 0 { return 0.0; }
        intersection_count(relevant, actual) as f64 / k as f64
    }

    /// Precision@k: fraction of top-k results that are relevant.
    ///
    ///   precision = |relevant ∩ actual| / |actual|
    pub fn precision(relevant: &[i64], actual: &[i64]) -> f64 {
        if actual.is_empty() { return 0.0; }
        intersection_count(relevant, actual) as f64 / actual.len() as f64
    }

    /// F1@k: harmonic mean of recall and precision.
    ///
    ///   F1 = 2 · (recall · precision) / (recall + precision)
    pub fn f1(relevant: &[i64], actual: &[i64], k: usize) -> f64 {
        let r = recall(relevant, actual, k);
        let p = precision(relevant, actual);
        if r + p == 0.0 { return 0.0; }
        2.0 * r * p / (r + p)
    }

    /// Reciprocal Rank: 1/(position of first relevant result).
    ///
    /// Scans `actual` in order; returns 1/(i+1) for the first
    /// element found in `relevant`. Returns 0 if no match.
    pub fn reciprocal_rank(relevant: &[i64], actual: &[i64]) -> f64 {
        let relevant_set: std::collections::HashSet<i64> =
            relevant.iter().copied().collect();
        for (i, &item) in actual.iter().enumerate() {
            if relevant_set.contains(&item) {
                return 1.0 / (i as f64 + 1.0);
            }
        }
        0.0
    }

    /// Average Precision: mean precision at each relevant position.
    ///
    /// For each position i where actual[i] is relevant:
    ///   precision_at_i = (relevant items in actual[0..=i]) / (i+1)
    /// AP = mean of all precision_at_i values.
    pub fn average_precision(relevant: &[i64], actual: &[i64]) -> f64 {
        let relevant_set: std::collections::HashSet<i64> =
            relevant.iter().copied().collect();
        let mut hits = 0u64;
        let mut sum = 0.0f64;
        for (i, &item) in actual.iter().enumerate() {
            if relevant_set.contains(&item) {
                hits += 1;
                sum += hits as f64 / (i as f64 + 1.0);
            }
        }
        if hits == 0 { 0.0 } else { sum / relevant.len().max(1) as f64 }
    }
}
```

### Result Extraction

The `ValidatingDispenser` must extract actual result indices from the
adapter's `OpResult`. This is a two-tier strategy:

**JSON extraction with type coercion**

The validation wrapper extracts indices via `to_json()`. Column
values may be native integers or text strings containing integers
(CQL `text` columns with numeric keys). The extractor handles both:

```rust
fn extract_indices_from_json(json: &serde_json::Value, field: &str) -> Vec<i64> {
    match json {
        serde_json::Value::Array(rows) => {
            rows.iter()
                .filter_map(|row| json_field_as_i64(row.get(field)?))
                .collect()
        }
        // ... also handles Object with "rows" sub-key
    }
}

/// Coerce a JSON value to i64: native integer, or parse from string.
fn json_field_as_i64(v: &serde_json::Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}
```

The `json_field_as_i64` coercion is critical: CQL vector workloads
typically store keys as `text` (e.g., `"544844"`), and the ground
truth contains integer indices. Without coercion, string keys would
silently produce empty actual vectors and zero recall.

### Ground Truth Resolution

The `expected` binding references a GK output that produces ground
truth indices per cycle. This binding is typically NOT referenced
in the op's field map (it's not part of the CQL query). The binding
compiler scans `params` values (including `relevancy.expected`) for
`{name}` references and includes them in the GK program. At resolve
time, `FiberBuilder::resolve_with_extras()` pulls the extra binding
into `ResolvedFields` alongside the op fields.

```rust
// Binding compiler: scan params for bind point references
fn collect_param_bindings(params: &HashMap<String, Value>, ...) {
    // Recursively scan all JSON values in params for {name} refs
    // This catches relevancy.expected: "{ground_truth}"
}

// Resolver: pull extra bindings alongside op fields
pub fn resolve_with_extras(
    &mut self, template: &ParsedOp, extra_bindings: &[String],
) -> ResolvedFields {
    // ... resolve op fields ...
    for binding in extra_bindings {
        if self.program.resolve_output(binding).is_some() {
            names.push(binding.clone());
            values.push(self.state.pull(&self.program, binding).clone());
        }
    }
}
```

**Missing ground truth is a hard error**, not a silent zero:

```
error: [op] [relevancy_error] relevancy: no ground truth for
'ground_truth'. Available fields: ["prepared"]. Ensure the
binding exists in the GK program.
```

---

## ValidatingDispenser

```rust
/// Op dispenser wrapper that validates results after execution.
///
/// Applied only to templates that declare `verify:` or `relevancy:`
/// blocks. Zero overhead for templates without validation.
pub struct ValidatingDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Field assertions (from `verify:` block)
    assertions: Vec<AssertionSpec>,
    /// Relevancy measurement (from `relevancy:` block)
    relevancy: Option<RelevancyConfig>,
    /// Metrics recording
    metrics: Arc<ValidationMetrics>,
}

pub struct ValidationMetrics {
    /// Pass/fail counters
    pub validations_passed: AtomicU64,
    pub validations_failed: AtomicU64,
    /// Per-function HDR histograms for relevancy scores.
    /// Key: function name with @k suffix ("recall@10", "precision@10")
    /// Value: HDR histogram recording [0.0, 1.0] as [0, 10000] integer
    pub relevancy_histograms: HashMap<String, HdrHistogram>,
    /// Per-function running average (for gauge-style reporting)
    pub relevancy_averages: HashMap<String, AtomicF64Average>,
}

impl ValidatingDispenser {
    /// Wrap a dispenser with validation, if the template declares it.
    /// Returns the inner dispenser unchanged if no validation is declared.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        template: &ParsedOp,
        metrics: Arc<ValidationMetrics>,
    ) -> Arc<dyn OpDispenser> {
        let assertions = parse_assertions(template);
        let relevancy = parse_relevancy(template);
        if assertions.is_empty() && relevancy.is_none() {
            return inner;  // no validation declared — zero overhead
        }
        Arc::new(Self { inner, assertions, relevancy, metrics })
    }
}

impl OpDispenser for ValidatingDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> Pin<Box<dyn Future<Output = Result<OpResult, ExecutionError>>
                 + Send + 'a>>
    {
        Box::pin(async move {
            let result = self.inner.execute(cycle, fields).await?;

            // Phase 1: Field assertions
            let mut all_pass = true;
            for assertion in &self.assertions {
                if !assertion.check(&result, fields) {
                    all_pass = false;
                    // Log first failure per template (rate-limited)
                }
            }

            // Phase 2: Relevancy metrics
            if let Some(config) = &self.relevancy {
                let actual = extract_actual_indices(&result, &config.actual_field);
                let expected = resolve_expected(fields, &config.expected_binding);

                let mut actual_sorted = actual.clone();
                actual_sorted.sort_unstable();
                let mut expected_k: Vec<i64> =
                    expected.into_iter().take(config.k).collect();
                expected_k.sort_unstable();
                let actual_k: Vec<i64> =
                    actual_sorted.into_iter().take(config.k).collect();

                for func in &config.functions {
                    let score = match func {
                        RelevancyFn::Recall =>
                            relevancy::recall(&expected_k, &actual_k, config.k),
                        RelevancyFn::Precision =>
                            relevancy::precision(&expected_k, &actual_k),
                        RelevancyFn::F1 =>
                            relevancy::f1(&expected_k, &actual_k, config.k),
                        RelevancyFn::ReciprocalRank =>
                            relevancy::reciprocal_rank(&expected_k, &actual),
                        RelevancyFn::AveragePrecision =>
                            relevancy::average_precision(&expected_k, &actual),
                    };
                    let metric_name = format!("{}@{}", func.name(), config.k);
                    self.metrics.record_relevancy(&metric_name, score);
                }
            }

            if all_pass {
                self.metrics.validations_passed.fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics.validations_failed.fetch_add(1, Ordering::Relaxed);
            }

            Ok(result)
        })
    }
}
```

---

## Metrics Exposure

Validation metrics flow through the same `ActivityMetrics` /
`MetricsFrame` pipeline as latency and throughput:

```
ValidationMetrics
  ├── validations_passed    (counter)
  ├── validations_failed    (counter)
  ├── recall@10             (HDR histogram, [0.0, 1.0])
  ├── precision@10          (HDR histogram)
  ├── f1@10                 (HDR histogram)
  ├── reciprocal_rank@10    (HDR histogram)
  └── average_precision@10  (HDR histogram)
```

### HDR Histogram for Relevancy Scores

Relevancy scores are continuous values in [0.0, 1.0]. To use HDR
histograms (which store integers), scores are scaled to [0, 10000]
and recorded as 4-decimal-place fixed point:

```rust
fn record_relevancy(&self, name: &str, score: f64) {
    let scaled = (score * 10_000.0).round() as u64;
    if let Some(histo) = self.relevancy_histograms.get(name) {
        histo.record(scaled);
    }
    if let Some(avg) = self.relevancy_averages.get(name) {
        avg.update(score);
    }
}
```

This gives p50/p99/min/max/mean for each relevancy function —
the same statistical fidelity as latency reporting.

### MetricsFrame Integration

The `MetricsFrame` snapshot (used by web UI and log reporting)
includes validation metrics alongside standard counters:

```rust
pub struct MetricsFrame {
    // ... existing fields ...
    pub validations_passed: u64,
    pub validations_failed: u64,
    pub relevancy: HashMap<String, RelevancySnapshot>,
}

pub struct RelevancySnapshot {
    pub mean: f64,
    pub p50: f64,
    pub p99: f64,
    pub min: f64,
    pub max: f64,
    pub count: u64,
}
```

### Console Output

When relevancy metrics are active, the periodic status line includes
a summary:

```
cassnbrs: 1000 cycles, 850 ops/s, p99=12.3ms | recall@10=0.923 precision@10=0.910 f1@10=0.916
```

---

## End-to-End Example: CQL Vector Recall

Verified working against Cassandra 5.0 SAI with glove-25-angular
(1.2M vectors, 25 dimensions). Measured recall@100 = 0.94 mean.

```yaml
params:
  dataset: glove-25-angular
  keyspace: baselines
  table: vectors
  k: "100"
  concurrency: "100"

bindings: |
  coordinates := (cycle)
  query_vector := query_vector_at(cycle, "{dataset}")
  ground_truth := neighbor_indices_at(cycle, "{dataset}")

ops:
  select_ann:
    tags:
      phase: search
    prepared: "SELECT key FROM {keyspace}.{table} ORDER BY value ANN OF {query_vector} LIMIT {k}"
    relevancy:
      actual: key
      expected: "{ground_truth}"
      k: 100
      functions:
        - recall
        - precision
        - f1
        - reciprocal_rank
        - average_precision
```

Note: `ground_truth` is NOT an op field — it's a GK binding
referenced only by `relevancy.expected`. The binding compiler
finds it by scanning params for `{name}` references, and the
resolver pulls it as an extra binding alongside op fields.

**Execution flow for one cycle:**

1. GK kernel evaluates `query_vector_at(42, "glove-25-angular")`
   → 25-dim float vector; `neighbor_indices_at(42, ...)` → ground
   truth `[7, 23, 891, ...]`
2. `CqlRawDispenser` executes ANN query (string-interpolated)
3. Cassandra returns top-100 rows with `key` column (text)
4. `TraversingDispenser` counts 100 elements
5. `ValidatingDispenser`:
   - Extracts `key` values via `json_field_as_i64` (coerces text
     `"544844"` → i64 `544844`)
   - Resolves `ground_truth` from extra bindings
   - Computes recall@100, precision@100, f1@100, etc.
   - Records in HDR histograms
6. Executor records latency as normal

---

## Assertion Failure Handling

Assertion failures are **not execution errors**. The op succeeded at
the protocol level — the result just didn't match expectations. This
is a measurement, not a retry trigger.

```
┌─────────────┬──────────────────┬─────────────────────────────┐
│ Outcome     │ Metric           │ Behavior                    │
├─────────────┼──────────────────┼─────────────────────────────┤
│ Op success, │ validations_     │ Normal. Count and continue. │
│ verify pass │ passed++         │                             │
├─────────────┼──────────────────┼─────────────────────────────┤
│ Op success, │ validations_     │ Count, log first N, cont.   │
│ verify fail │ failed++         │ Do NOT retry.               │
├─────────────┼──────────────────┼─────────────────────────────┤
│ Op error    │ (execution error │ Skip validation entirely.   │
│             │  path, no verify)│ Error handling per SRD 41.  │
└─────────────┴──────────────────┴─────────────────────────────┘
```

An optional `strict: true` on the `verify:` block can promote
failures to `ExecutionError::Op` for use cases where incorrect
results should count as errors (e.g., correctness regression tests).

---

## Relationship to Other SRDs

**SRD 38 (Adapter Interface):** Validation wraps `OpDispenser`, the
same trait defined in SRD 38. The `ResultBody` trait with `as_any()`
and `to_json()` is the extraction mechanism for actual results.

**SRD 40 (Op Execution Semantics):** `TraversingDispenser` and
`ValidatingDispenser` are both composable wrappers applied in the
init-time pipeline. Captures extracted by traversal are available
to validation. Stanza concurrency is orthogonal — validation runs
per-op after execution, regardless of window size.

**SRD 46 (Vector Metadata):** The `neighbor_indices_at` and
`filtered_neighbor_indices_at` GK functions provide the ground truth
for relevancy measurement. Filtered recall uses
`filtered_neighbor_indices_at` with the same predicate applied to
both the query and the ground truth lookup.

---

## Implementation Status

1. **Relevancy compute functions** — DONE. `nb-activity/src/relevancy.rs`.
   25 unit tests. `intersection_count`, `recall`, `precision`, `f1`,
   `reciprocal_rank`, `average_precision` with `RelevancyFn` enum.

2. **ValidationMetrics** — DONE. HDR histograms for [0,1] scores
   (scaled to [0, 10000]). Pass/fail counters. Summary printed at
   end of run with mean/p50/p99/min/max per function.

3. **ValidatingDispenser** — DONE. `nb-activity/src/validation.rs`.
   17 unit tests. Parses `verify:` and `relevancy:` from ParsedOp
   params. Hard error on missing ground truth. `extra_bindings()`
   reports needed GK outputs for the resolver.

4. **CQL result extraction** — DONE. `CqlResultBody` with
   `HashMap<String, serde_json::Value>` rows extracted from
   `CassResult` via `LendingIterator`. `json_field_as_i64` coercion
   handles text-to-integer for CQL text key columns.

5. **Binding visibility** — DONE. Binding compiler scans params
   (not just op fields) for `{name}` references. Resolver pulls
   extra bindings via `resolve_with_extras()`.

6. **Console output** — DONE. Relevancy summary at end of run.

7. **Verified end-to-end** — Cassandra 5.0 SAI, glove-25-angular,
   1.2M vectors, recall@100 = 0.94 mean across 100 queries.

### Remaining

- Filtered recall via `filtered_neighbor_indices_at` (SRD 46)
- MetricsFrame integration for web UI / periodic reporting
- Per-template validation metrics (currently aggregated)
