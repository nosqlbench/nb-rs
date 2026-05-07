# Recall discrepancy analysis: PVS recall > oracle recall

**Status:** investigation in progress
**Started:** 2026-05-06
**Symptom:** `plot_recall_at_k1` shows `phase="pvs_query"` recall@1 (≈0.96 at limit=1) materially higher than `phase="ann_query"` recall@1 (≈0.78 at limit=1) on the same vector subspace.

## Premise (user, confirmed sound)

The labeled tables `vec_label_NN` are constructed to contain exactly the items
where the corresponding metadata predicate would match in `fknn_default`.
Given that, oracle ANN over `vec_label_NN` is the upper bound for any filtered
ANN search over `fknn_default WHERE metadata=N`: filtered ANN cannot exceed
dedicated ANN on the same vector subspace. Observing PVS > oracle by ~18 pp
violates this invariant — it is a real bug, not a definitional artefact.

## Investigation ordering

Independent stages, sorted by yield-per-effort:

1. **Stage 4** — cursor / iteration mismatch in oracle bindings (cheapest).
2. **Stage 3** — `filtered_neighbor_indices_at` semantics (pre- vs post-filter).
3. **Stage 1** — dataset construction asymmetry (`dataset:label_NN` vs
   `dataset:default`); reconcile `n=78k–85k` per-label vs `n=100k` default.
4. **Stages 2 / 5 / 6** — corroboration only if the above don't pin it.

---

## Stage 0 — pin down the conjecture experimentally

Average comparison hides per-pair behaviour. Get per-predicate PVS averages
and pair them with their corresponding oracle labels.

- [ ] Inspect labels: `nbrs metrics show 'recall_at_1_mean{phase="pvs_query"}'`.
      Confirm whether `predicate` (or analogous) is a metric dimension.
- [ ] If absent, instrument `pvs_query` so each sample carries the predicate
      value as a metric label. Without this, you can't compare the right pairs.
- [ ] Once labelled:
      `avg(recall_at_1_mean{phase="pvs_query"}) by (predicate, limit)` paired
      with `avg(recall_at_1_mean{phase="ann_query", profile="label_NN"}) by (limit)`.
- [ ] Confirm violation is per-pair, not just global average.

## Stage 1 — dataset construction (`dataset:label_NN` vs `dataset:default`)

- [x] **Hypothesis rejected**, 2026-05-06 (see "Index-space mismatch
      hypothesis" below). Re-investigating.

### Confirmed dataset layout (sift1m at S3 bucket pvs-testing-20260411)

`profiles:` block in the live dataset.yaml (saved at `/tmp/sift1m.yaml`):

- `default`: 1M base, 10k queries, full neighbor + filtered-neighbor facets.
- `label_00..label_11`: each declares **its own** `base_vectors`,
  `query_vectors`, `neighbor_indices`, `neighbor_distances`. base_count
  is per-label (~83k each). No inheritance involved.

Pipeline produces per-partition ground truth at the
`compute-knn-partition` step:

```yaml
- id: compute-knn-partition
  run: compute knn
  description: Compute KNN for oracle partition profiles
  per_profile: true
  base: ${profile_dir}base_vectors.fvec       # per-profile local base
  query: ${profile_dir}query_vectors.fvec     # per-profile local queries
  indices: neighbor_indices.ivec              # per-profile output
```

So `label_NN/neighbor_indices.ivec` is the top-k of label_NN's queries
against label_NN's local base — indices live in `[0, base_count_NN)`,
the same numbering as the rampup `id := format_u64(row, 10)` keys.
Index spaces match within oracle. Same internal consistency holds for
PVS using `default`.

### Index-space mismatch hypothesis (rejected)

I had hypothesised that label_NN profiles inherited `neighbor_indices`
from default, putting oracle's ground truth in default's global index
space while keys lived in label_NN's local space. **That's not what the
data shows.** Each label_NN profile defines its own neighbor_indices
explicitly, computed against its own local base. The hypothesis was
based on:
1. extrapolation from the synthetic-1k *test fixture* (which has
   stripped-down label profiles), not the live sift1m dataset; and
2. an inference from the write-side serializer comment in
   `vectordata/src/dataset/config.rs:488` about parse-side inheritance
   that I never verified.
Both inputs were unsound; conclusion withdrawn.

### Adjacent finding: plot was pooling across `optimize_for` (fixed 2026-05-07)

The plot's metricsql queries had no `optimize_for=` constraint:

```metricsql
y1: avg(recall_at_1_mean{k="1",phase="ann_query",profile=~"label.*"})
        by (profile, limit)
```

Without that filter, the `avg(...) by (profile, limit)` aggregation
pooled LATENCY and RECALL samples together. For oracle, this dragged
the curve toward the lower LATENCY value (label_00 at limit=1:
mean(0.7795, 0.7888) ≈ 0.784 instead of either build's true value).
For PVS, the two `optimize_for` samples were byte-identical (because
of the rampup-rebuild bug below), so averaging them was a no-op there
— but the comparison was still asymmetric: oracle's 0.78 was a mix of
two real builds, PVS's 0.96 was effectively the RECALL build twice.

Fixed by filtering `optimize_for="RECALL"` on every plot query and
narrowing `optimize_for_values` to `"RECALL"` so there's only one
build per phase. Future expansion to compare LATENCY vs RECALL builds
should use separate plots (or per-axis filters) rather than implicit
pooling.

### Adjacent finding: `test_fknn` rampup/query split bug (fixed 2026-05-07)

While probing the discrepancy, observed PVS recall is **byte-identical**
across `optimize_for={LATENCY, RECALL}` (e.g. 0.9571 = 0.9571 at limit=1)
while oracle shows a small but real gap (e.g. 0.7795 vs 0.7888 at
label_00, limit=1).

Cause: `test_fknn` was split into two sequential top-level scenarios:

```yaml
test_fknn:
  - scenario: rampup_fknn   # iterates LATENCY then RECALL — both rebuild fknn_default
  - scenario: query_fknn    # iterates LATENCY then RECALL — both query fknn_default
```

The table name `fknn_default` doesn't depend on `optimize_for`, so
`rampup_fknn`'s second iteration tore down the LATENCY-built index and
rebuilt under `optimize_for=RECALL` before any queries ran. Both
`query_fknn` iterations then hit the **same** physical index (the
RECALL one). The metric's `optimize_for` label was misleading.

Fixed by inlining rampup + query inside one for-loop iteration over
`optimize_for`, mirroring `test_oracles`. Now each `optimize_for`
value's index is built fresh and queried before the next iteration
tears it down.

This doesn't fully explain the oracle vs PVS gap — both oracle-RECALL
(~0.78) and PVS-effectively-RECALL (~0.96) still differ by ~17 pp on
the same vector subspace — but it does mean any PVS-LATENCY measurement
prior to the fix was conflated with PVS-RECALL.

### Engine-version difference: oldtest = Cassandra 5, current = CNDB (load-bearing)

`logs/oldtest.md` (the historical baseline) was collected against
**Cassandra 5 (OSS)**. The current run targets **CNDB** (DataStax-flavor
Cassandra). These ship different SAI implementations. Anything in
the SAI ANN path can differ between them:

- HNSW / graph build parameters (M, ef_construction).
- ANN+WHERE strategy: pre-filter vs post-filter, two-stage hybrid,
  re-rank policy.
- `optimize_for` semantics on `CREATE INDEX` (the values may map to
  different runtime parameters).
- Internal search depth defaults / overscan behaviour at small
  `LIMIT`.
- Per-segment vs whole-index ANN at query time.

Empirical fingerprint of the gap matches "engine-side change":

| Recall point | OldTest oracle (Cass5) | Current oracle (CNDB) | Current PVS (CNDB) | OldTest fknn (Cass5) |
| --- | --- | --- | --- | --- |
| k=10 / limit=10 | 0.9040 | 0.91 | 0.98 | 0.8992 |
| k=100 / limit=100 | 0.9663 | 0.978 | TBD | 0.9447 |

Oracle's CNDB numbers match the Cassandra-5 baseline closely
(0.91 vs 0.90 at k=10 — within sample variance). PVS drifted ~7-9 pp
upward (0.98 vs 0.90). The drift is on the PVS side, and PVS is the
side that exercises ANN+WHERE — exactly where Cass5 and CNDB are
most likely to differ.

**Probe**: re-run the same workload against a Cassandra-5 target and
compare. Expected outcome if engine-difference is the driver:
- Oracle (Cass5): ~0.90 at k=10 — matches OldTest.
- PVS (Cass5): ~0.90 at k=10 — matches OldTest fknn — i.e. the
  oracle-vs-PVS gap collapses to near zero.
- The user's premise (PVS ≤ oracle on the same vector subspace)
  holds on Cass5 but is violated on CNDB because CNDB's ANN+WHERE
  benefits from the unified-table graph quality more than Cass5's.

If the Cass5 run reproduces the OldTest pattern, no further
investigation is needed: the discrepancy is a real engine
characterization difference between Cass5 and CNDB, not a workload
or measurement bug. If Cass5 ALSO shows PVS > oracle by a similar
margin, then the workload changes since OldTest are the cause and
we should diff full_cql_vector.yaml against whatever workload
shape produced OldTest.

### Reopened: where could the gap actually come from?

With the index-space hypothesis dead, the remaining space:

- [ ] **Test-query distribution**: oracle's per-label queries are the
      subset of source queries whose predicate matches the label
      (per `inspect_filtered_knn.rs:164-173`). PVS's queries are the
      full 10k source queries. If label-N's query subset is harder
      than the average across all 10k, oracle measures a worse
      problem.
  - Probe: pick one label, run PVS on *just* its predicate-matching
    queries (filter by predicate value), compare that subset's PVS
    recall against oracle's recall on the same label.
- [ ] **Per-label index quality**: same Cassandra+SAI engine, but
      `vec_label_NN` (~83k items) and `fknn_default` (1M items, filtered
      to ~83k at query time) get different SAI build paths. SAI's
      ANN index is built per-table; the build for ~83k might use
      different parameters / less mature segments than the build for
      1M. Possible.
  - Probe: a single CQL query that searches `fknn_default` *without*
    the metadata predicate, restricted to keys that match label_NN's
    items. Compare result quality to the same query against
    `vec_label_NN`.
- [ ] **`verify-knn-partition` step output**: the upstream pipeline
      runs `verify knn-groundtruth` on the partition profiles
      (line 135). If those verifications passed, the per-profile
      ground truth is sound. If they failed (or were skipped), bug
      could be there.
  - Probe: locate the verify output (likely
    `${cache}/${profile_name}_verify_knn_partition.json`) on the host
    where the dataset was prepared.
- [ ] **Recall function denominator with a small `expected` slice**:
      relevancy block uses `k="{k}"`, `r="{limit}"`. If oracle's
      `expected` (= ground truth) sometimes has fewer than `k` items
      because the dataset hit an edge (truncated KNN, undersized
      partition), recall could divide oddly.

  Looking at the `synthetic-1k` fixture (representative of the `example`
  dataset's profile structure):

  ```yaml
  profiles:
    default:
      base_vectors, query_vectors, neighbor_indices, neighbor_distances,
      metadata_*, filtered_neighbor_indices, filtered_neighbor_distances
    label_00:
      base_count, base_vectors, query_vectors    # NO neighbor_indices
    label_01: ... same shape, no neighbor_indices
  ```

  Per `links/vectordata-rs/vectordata/src/dataset/config.rs:488,511`,
  non-default profiles **inherit views from `default` at parse time**.
  So when oracle binds:
  ```
  init prebuffered = dataset_prebuffer("{dataset}:label_NN")
  ground_truth := neighbor_indices_at(prebuffered, q)
  ```
  the `neighbor_indices_at` call resolves to the **default's**
  neighbor_indices facet, which is ground truth computed over the
  **default's full ~100k base** — indices in `[0, 99999]`.

  Oracle's `rampup` op:
  ```
  cursor row = range(0, vector_count(prebuffered))
  id := format_u64(row, 10)
  train_vector := vector_at(prebuffered, row)
  INSERT ... VALUES (id, train_vector)
  ```
  inserts label_NN's local base into `vec_label_NN` with key = row index
  **into label_NN's local base** (`[0, ~80k]`).

  At query time the ANN returns keys in the local index space; recall
  compares them against ground-truth indices in the default's global
  index space. **Different numberings.** Recall only "hits" by
  coincidence — when label_NN's local row N happens to be the same
  vector as default's global row N. The 0.78 oracle figure is mostly
  measuring how often that coincidence happens, not the ANN engine's
  recall.

  PVS doesn't have this issue: it uses `profile=default` everywhere, so
  rampup keys (default-global row indices) and `filtered_neighbor_indices`
  (default-global indices) live in the same numbering. Recall is real.

  **Remediation options** (workload + dataset side):

  1. **Per-profile ground truth in the dataset**: have the `example`
     dataset compute `neighbor_indices` per profile (against each
     profile's local base) so label_NN inherits *no* facets from
     default. Cleanest. Requires re-running the vectordata pipeline
     (`compute knn` per profile, not just default).

  2. **Map keys through the default's index space at rampup time**: have
     oracle's rampup emit keys that match the default-global row index
     for each label_NN vector — i.e. write the default-base ordinal as
     the row's CQL key, not the local label_NN row index. Requires
     `metadata_indices` (or equivalent) per profile so the local→global
     mapping is queryable.

  3. **Use the default profile for oracle too**: drop the per-label
     tables; partition the default's 100k base by metadata instead and
     run oracle queries against the partitions via predicate-pushed
     indexes. Effectively makes oracle a special case of PVS, which
     defeats the purpose of having a baseline.

  Recommended path: option 1 (most correct + smallest blast radius).
  Option 2 is workable if the dataset can't be regenerated.

  The `n` label discrepancy (oracle ~78k–85k, PVS 100k) is consistent
  with this picture: per-label profiles override `base_count` to their
  local subset size; default has the full count.

## Stage 2 — ingestion path

Confirm the right items get written to each table.

- [ ] Find `rampup` and `fknn_rampup_data` op definitions:
  - `rampup` (oracle): inserts only `{profile}`-belonging items into `vec_label_NN`.
  - `fknn_rampup_data` (PVS): inserts all items into `fknn_default` with `metadata`.
- [ ] After rampup, query directly:
      `SELECT count(*) FROM vec_label_00;
       SELECT count(*) FROM fknn_default WHERE metadata = <pred>;`
      Counts should match if premise holds.
- [ ] Spot-check 3 keys from `vec_label_00`. Same vectors, same metadata, in
      `fknn_default WHERE metadata = …`?

## Stage 3 — ground-truth functions (most likely individual cause)

- [x] **Ruled out**, 2026-05-06.
  - Both `neighbor_indices_at` and `filtered_neighbor_indices_at` in
    `nbrs-variates/src/nodes/vectors.rs:635-654` are macro-generated facet
    lookups: pure index reads into pre-computed dataset facets
    (`neighbor_indices` and `filtered_neighbor_indices` respectively). No
    filter logic in nbrs.
  - The facet is computed by vectordata at dataset-build time. The pipeline
    step is named `"compute-filtered-knn"` with description **"Compute
    filtered KNN with predicate pre-filtering"** (`veks/src/prepare/import.rs:1948-1950`).
  - Implementation at
    `veks-pipeline/src/pipeline/commands/compute_filtered_knn.rs:531-593`
    confirms: per query, fetch the predicate-matching ordinals via
    `keys_reader.get_ordinals(qi)`, filter to the current partition, run
    `find_top_k_filtered_*` against ONLY those ordinals. Pre-filter is
    correct — the facet is the top-k *amongst* predicate-matching items.
  - So the ground truth is correct *for the dataset that was built*. If
    there's a problem, it's upstream of these functions: either the dataset
    construction itself or how the per-label profiles relate to the default
    profile's predicates. → Stage 1.

## Stage 4 — cursor / iteration mismatch (start here, cheapest)

Workload bindings differ:

```yaml
ann_query:
  cursor q = range(0, query_counts * 100)
  query_vector := query_vector_at(prebuffered, q % query_counts)
  ground_truth := neighbor_indices_at(prebuffered, q)         # bare q
```

```yaml
pvs_query:
  cursor q = range(0, query_counts * 10)
  query_vector := query_vector_at(prebuffered, q % query_counts)
  ground_truth := filtered_neighbor_indices_at(prebuffered, q % query_counts)
```

- [x] **Ruled out**, 2026-05-06. Both functions route through
      `slice_arc_from_uniform` (`nbrs-variates/src/nodes/vectors.rs:567`),
      which does `let idx = index % d.count;` internally before any read.
      The bare-`q` vs `q % query_counts` workload-binding discrepancy is
      benign: the same modulo is applied either way. Cursor mismatch is not
      the cause.

## Stage 5 — recall computation

Both phases share the relevancy block, so unlikely to differ — but quick audit.

- [ ] Locate the recall function. Confirm formula:
      `recall = |actual_top_r ∩ expected_top_k| / k` (not `/ r`, not `/ |actual|`).
- [ ] Confirm both phases instantiate with same `k=1`, `r=limit`.
- [ ] **Edge case**: empty `expected` (no predicate matches). Does recall
      return 0, 1, or NaN? Inflation if 1.

## Stage 6 — server-side ANN+WHERE semantics

- [ ] Determine whether the SAI build pre-filters or post-filters ANN+WHERE.
      The workload `pvs_metadata_query` comments acknowledge the choice
      affects LIMIT semantics.
- [ ] Verify the index plan via `EXPLAIN` or equivalent on one pvs CQL.

## Stage 7 — resolution

- [ ] Fix the identified root cause.
- [ ] Regression test: a synthetic dataset where label_NN ≡ predicate=N by
      construction; run both phases; assert PVS ≤ oracle on every
      (predicate, limit) pair.
- [ ] Re-render `plot_recall_at_k1`; confirm curves obey the upper bound.
