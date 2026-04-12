# 32: Dispenser Wrappers

Wrappers are composable decorators around `OpDispenser`. They add
cross-cutting behavior without modifying adapter code.

---

## TraversingDispenser

Always applied (unless dry-run). Wraps every raw dispenser.

**Responsibilities:**
1. Count `element_count()` and `byte_count()` from `ResultBody`
2. Extract capture points from result JSON
3. Record traversal metrics

```rust
pub struct TraversingDispenser {
    inner: Arc<dyn OpDispenser>,
    stats: Arc<TraversalStats>,
    captures: Vec<CaptureSpec>,  // parsed at init time
}
```

### Capture Extraction

Capture points are declared in op field strings with `[name]`
syntax:

```yaml
ops:
  read_user:
    prepared: "SELECT [username], [age as user_age] FROM users WHERE id = {id}"
```

At init time, capture specs are parsed from the template. At
cycle time, the traverser extracts named fields from the result's
`to_json()` representation:

```rust
fn extract_captures_from_json(body: &dyn ResultBody, specs: &[CaptureSpec])
    -> HashMap<String, Value>
{
    let json = body.to_json();
    for spec in specs {
        if let Some(val) = json.get(&spec.source) {
            captures.insert(spec.alias.clone(), json_to_value(val));
        }
    }
}
```

---

## ValidatingDispenser

Applied only when the template declares `verify:` or `relevancy:`
blocks. Zero overhead for templates without validation.

**Responsibilities:**
1. Check field assertions against result
2. Compute relevancy metrics (recall, precision, etc.)
3. Record pass/fail counters and score histograms
4. Hard error on missing ground truth

```rust
pub struct ValidatingDispenser {
    inner: Arc<dyn OpDispenser>,
    assertions: Vec<AssertionSpec>,
    relevancy: Option<RelevancyConfig>,
    metrics: Arc<ValidationMetrics>,
    strict: bool,
}
```

**Design note:** Relevancy is not a cross-cutting concern — it
applies only to vector search workloads. It should be a
specialized validator plugin registered via the `relevancy:`
declaration, not a built-in field on `ValidatingDispenser`.
The target design: `ValidatingDispenser` holds a list of
validator implementations (field assertions, relevancy, future
custom validators), each activated by its own YAML block.

### Field Assertions

```yaml
verify:
  - field: name
    is: not_null
  - field: balance
    gte: 0
  - field: status
    eq: "active"
```

Predicates: `eq`, `not_null`, `is_null`, `gte`, `lte`, `contains`.

Assertion failures increment `validations_failed`. With
`strict: true`, failures become `ExecutionError::Op`.

### Relevancy Metrics

```yaml
relevancy:
  actual: key              # column to extract from result
  expected: "{ground_truth}" # GK binding for ground truth
  k: 100
  functions:
    - recall
    - precision
    - f1
    - reciprocal_rank
    - average_precision
```

Metric names include the k value: `recall@100`, `precision@10`,
etc. This matches nosqlbench's naming convention where each
function/k combination produces a distinct metric.

See [33: Result Validation](33_result_validation.md) for details.

---

## Composition Order

```
executor calls →
  ValidatingDispenser.execute()
    → TraversingDispenser.execute()
      → adapter OpDispenser.execute()
      ← OpResult (body + captures)
    ← element/byte counting, capture extraction done
  ← assertions checked, relevancy computed
← metrics recorded
```

The innermost dispenser (adapter) executes first. Each wrapper
processes the result on the way back out.

---

## Dry-Run Mode

When `dry_run=true`, the traversing wrapper is replaced with a
no-op wrapper that skips adapter execution entirely. Fields are
still resolved (GK runs), but no protocol call is made.

Useful for validating workload syntax, GK bindings, and field
resolution without a live target.

### Diagnostic Visibility Levels

The system should support multiple levels of diagnostic
inspection, each revealing progressively deeper pipeline state:

| Level | Shows |
|-------|-------|
| `--show templates` | Parsed op templates after normalization |
| `--show sequence` | Op sequence with ratios and stanza layout |
| `--show dispensers` | Dispenser types selected per template |
| `--show resolve` | Resolved fields for sample cycles |
| `--show execute` | Full execution with wrapper chain visible |
| `--show wrappers` | Wrapper configuration (validators, capture specs) |

Each level should be implemented via **diagnostic wrappers** that
intercept and display pipeline state at the appropriate point,
not via conditional branches scattered throughout the code.
Diagnostic wrappers are assembled at init time based on the
requested visibility level — the same composition pattern used
for traversal and validation.
