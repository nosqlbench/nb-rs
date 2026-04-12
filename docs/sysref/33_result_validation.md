# 33: Result Validation and Recall Measurement

Validation verifies operation results and computes information
retrieval metrics. Implemented as a composable dispenser wrapper
with zero overhead when not declared.

**Status:** Core implementation complete and verified (recall@100
= 0.94 on glove-25-angular). Relevancy should be refactored into
a pluggable validator (see sysref 32 design note).

---

## Relevancy Functions

Pure compute functions for vector/ANN search quality:

| Function | Formula | Description |
|----------|---------|-------------|
| `recall@k` | \|relevant ∩ actual\| / k | Fraction of ground truth found |
| `precision@k` | \|relevant ∩ actual\| / \|actual\| | Fraction of results that are relevant |
| `f1@k` | 2·(R·P)/(R+P) | Harmonic mean of recall and precision |
| `reciprocal_rank` | 1/(first relevant position + 1) | How quickly a relevant result appears |
| `average_precision` | Mean precision at each relevant position | Order-sensitive quality metric |

All operate on sorted `i64` slices. Set intersection uses
two-pointer O(n+m) scan with no allocation.

---

## Workload Declaration

```yaml
ops:
  select_ann:
    prepared: "SELECT key FROM vectors ORDER BY v ANN OF {query} LIMIT {k}"
    relevancy:
      actual: key              # column name in result rows
      expected: "{ground_truth}" # GK binding for neighbor indices
      k: 100
      functions:
        - recall
        - precision
        - f1
```

`relevancy:` is routed to `ParsedOp.params` by the parser (it's
in the `activity_params` list). The adapter never sees it.

---

## Ground Truth Flow

```
GK bindings:
  ground_truth := neighbor_indices_at(cycle, "{dataset}")

    ↓ (compiled because relevancy.expected references it)

Binding compiler scans params for {name} → includes "ground_truth"
    ↓
resolve_with_extras(template, ["ground_truth"])
    ↓
ResolvedFields contains both op fields AND ground_truth
    ↓
ValidatingDispenser reads ground_truth from ResolvedFields
```

**Key**: `ground_truth` is NOT an op field. It's a GK binding
needed only by validation. The binding compiler finds it by
scanning params, and the resolver pulls it as an extra binding.

**Missing ground truth is a hard error**, not a silent zero:
```
error: [op] [relevancy_error] relevancy: no ground truth for
'ground_truth'. Available fields: ["prepared"]. Ensure the
binding exists in the GK program.
```

---

## Result Extraction

Actual result indices are extracted from `ResultBody.to_json()`:

```rust
fn extract_indices_from_json(json: &Value, field: &str) -> Vec<i64> {
    // Array of row objects → extract field from each
    // json_field_as_i64: tries as_i64(), then as_str().parse()
}
```

The `json_field_as_i64` coercion handles text columns containing
numeric keys (e.g., CQL `text` key `"544844"` → `i64 544844`).
Without this, string keys would silently produce empty vectors.

---

## Metrics

### HDR Histograms

Relevancy scores in [0.0, 1.0] stored as [0, 10000] in HDR
histograms (4 decimal place fixed point). Reported as:

```
recall@100: mean=0.9385 p50=0.9503 p99=1.0000 min=0.7800 max=1.0000 (n=100)
precision@100: mean=0.9385 p50=0.9503 ...
f1@100: mean=0.9385 ...
```

### Pass/Fail Counters

```
validation: 100 passed, 0 failed
```

Printed at end of activity run. Assertion failures count as
failures. Relevancy computation (even with low scores) counts
as a pass.

---

## Assertion Failures

Assertion failures are measurements, not execution errors:

| Outcome | Counter | Behavior |
|---------|---------|----------|
| Op success, verify pass | passed++ | Normal |
| Op success, verify fail | failed++ | Count, continue |
| Op error | (error path) | Skip validation |

With `strict: true`, assertion failures become
`ExecutionError::Op` for correctness regression testing.

---

## Verified Results

Tested end-to-end against Cassandra 5.0 SAI:
- Dataset: glove-25-angular (1,183,514 training vectors, 25 dims)
- recall@100 = 0.94 mean across 100 ANN queries
- reciprocal_rank@100 = 1.00 (first result always relevant)
