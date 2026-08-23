# Vector-compaction branch map — jvector and the Cassandra fork

Written 2026-08-23 while diagnosing the `settle_compactions` stall in
`sessions/stcs_adaptive_20260821_194852`. Two repositories drive SAI vector
compaction on `jvector-rc-jshooks-cassandra-1`, and the branch names alone do
not say which one owns what. This is the map.

**Confidence marking.** Branches marked **[verified]** were read during the
2026-08-23 investigation — code, diffs and commit bodies. Branches marked
*(tip only)* are described from their tip-commit subject and name; treat those
descriptions as leads, not findings.

---

## 1. What is deployed right now

| | |
|---|---|
| jvector | `xlink-integration` @ `117e856f`, built to `jvector-4.0.1-SNAPSHOT.jar` 2026-08-21 16:09 |
| Cassandra fork | `compaction-integration-referencepoint` @ `7b35421f24`, dse-db-4.0.11.0-SNAPSHOT |
| JVM | JDK 25, up since 2026-08-21 19:47 (same binary across the fast and slow periods) |

Checkpoint tags created 2026-08-23:

- jvector — `checkpoint/xlink-integration-20260823`
- Cassandra — `checkpoint/compaction-integration-referencepoint-20260823`

Both are annotated tags on the committed HEAD. **Neither captures the
uncommitted working tree**, which was dirty in both repos at tag time
(jvector: `ParallelExecutor.java`, `TestParallelExecutor.java`,
untracked `ForkJoinParallelExecutor.java`; Cassandra: `build.xml`,
`conf/jvm-server.options`, `conf/logback.xml`,
`CassandraRelevantProperties.java`).

---

## 2. Ownership — which system owns which behaviour

The regression is jointly owned, and separating the two is the point of this
document.

| concern | owner |
|---|---|
| the merge *algorithm* — cross-source linking, cluster search, `gatherFromOtherSource` | **jvector** |
| packed-PQ neighbour reads (`FusedPQ`), on-disk graph views | **jvector** |
| prefetch: `FrontierPrefetchingView`, `willNeedL0Record`, `prefetchL0Records` | **jvector** |
| `MADV_RANDOM` on source mappings (`MemorySegmentReader`, jvector-native) | **jvector** |
| *whether the merge path runs at all* (`graph_compaction_merge_enabled`) | **Cassandra fork** |
| the `cassandra.sai.vector.*` flag surface + fail-fast validation | **Cassandra fork** |
| integration/adapter: `CompactionGraphMerger`, `SSTableIndexWriter` | **Cassandra fork** |
| thread pools, `compaction_build_threads`, `concurrent_builds` | **Cassandra fork** |

---

## 3. jvector branches

Repository: `/mnt/nvme/opt/jvector`. Base tag for all current work is
`4.0.0-rc.9`.

### 3.1 The deployed line

**`xlink-integration`** @ `117e856f` — **[verified]** — 31 commits past
`4.0.0-rc.9`. This is what is running. It combines cross-source linking
("xlink"), the chunked pre-encode/reverse-candidate buffers, and the
host-execution SPI work. Its merge path is measured IO-bound above RAM: see
§5.

Closely related, same lineage:

- **`origin/compaction-xlink-pr`** (2026-08-20) *(tip only)* — the PR-shaped
  presentation of the xlink work; tip is `9901dbb0 Compaction: refresh
  strategy context after similarity-ordinal reassignment`, which is also in
  `xlink-integration`.
- **`compaction-preencode-chunked-and-xlink`** / `origin/…` (2026-08-10)
  *(tip only)* — the chunking commits (`82f94697`, `fb1607d7`) before they
  landed in the integration branch. Removes the ~2 GiB pre-encode cache cap
  and the ~134M-node cross-link cap.
- **`origin/compaction-cross-link-pr`** (2026-08-05) *(tip only)* — earlier
  cross-link PR line; tip `ac219e77` (retained-only fast path).
- **`main`** (2026-08-12) — tip `711afea5 Compaction: eliminating disk scan
  and updating PQ retrain (#691)` — **[verified]**, see §5.

### 3.2 The IO branches — the ones that matter for this regression

**`origin/source-pretouch`** (2026-07-14) — **[verified]** — carries
`670f5588 "Compaction: prefetch source graphs into the page cache before bulk
phases"`. **This commit is NOT an ancestor of the deployed HEAD.** Its message
is the diagnosis of the current stall, written before it happened:

> Source readers advise MADV_RANDOM on their mappings — correct for
> search-time access, but it disables kernel readahead, so compaction's bulk
> phases … fault one 4KB page at a time on cold caches.

Adds `ReaderSupplier.prefetch()`; streams the backing file through a separate
readahead-enabled fd. Measured (cohere-10M, disk-cold): retraining 37s→3s,
code precompute 42s→7s, total −20%.

**Caveat before cherry-picking:** it *skips the pass when sources exceed
MemAvailable*. On a 1B-row table with 200 GB+ sources against ~333 GB
available, that guard can self-disable the fix in exactly the above-RAM regime
that needs it.

**`origin/io_improvements`** (2026-08-18) *(tip only)* — recent, named for
this problem domain; not inspected. Note the near-duplicate
**`origin/io_improvments`** (2026-06-05, misspelled) which carries
`bb561cad adding instrumentation for io testing`.

**`origin/compaction-rerank-prefetch`** (2026-07-16) *(tip only)* — tip makes
post-compaction refinement optional (default off); prefetch-adjacent.

Branches carrying the windowed-streaming-prefetch commits (`785cea4f` /
`9cca786a`, *"Compaction: windowed streaming prefetch for bulk-phase reads"*),
none of which are ancestors of the deployed HEAD:
`cooperative-embedding`, `embedding-integration`, `livenodes-sync`,
`native-byte-order`, `checkpoint/compaction-retain-largest-20260806`.

### 3.3 The disk-scan-elimination line

**`livenodes-sync`** / `origin/livenodes-sync` (2026-07-25) and
**`compaction-livenodes-enum`** (2026-07-22) — **[verified]** — carry
`11ea5b9e "enumerate L0 nodes from liveNodes bitset, not a disk scan"`.
Benchmarked 2038s→658s (3.1×) on cohere-10M disk-cold. Landed in the deployed
line. Removed the sequential scan that had been incidentally warming page
cache — see §5.

**`compaction-livenodes-enum-integration2`** (2026-07-31) *(tip only)* — tip
is a design note: *"measured confirmation of gap #1 — the searches are the
dominant term"*, i.e. the finding that search, not vector reads, dominates.

### 3.4 The retain-largest experiment

**`compaction-retain-largest`**, **`checkpoint/compaction-retain-largest-20260806`**,
**`jvector-experiments`**, **`checkpoint/experiments-on-retain-largest-20260806`**
(all 2026-08-06) *(tip only, except the flag surface)* — the asymmetric merge:
retain the largest source's graph and search only for the delta rather than
re-deriving every neighbourhood symmetrically. Surfaces in Cassandra as
`cassandra.sai.vector.compaction_retain_largest` (currently `true`). The
Cassandra-side working-tree docs record the motivating measurement: on the
symmetric path a 12-source merge spent 35% of CPU in candidate-heap
maintenance and 15% in per-search setup against 22% in similarity math.

### 3.5 Host-execution / integration SPI

**`integration-infrastructure`**, **`integration-robustness`**,
**`cooperative-embedding`**, **`embedding-integration`**,
**`compaction-conversion`** *(tip only)* — the `ParallelExecutor` /
`EmbeddedExecutionContext` / progress-and-cancellation SPI that lets Cassandra
drive jvector's execution on its own pools. Lands in the deployed line as
`914f83e5`, `e4d90672`, `5088e04a`, `7c9375d1`, `117e856f`. The
`RuntimeMode` gate (`jvector.mode`, `6b528cbc`) comes from here.

### 3.6 Other / not inspected

*(tip only)* — `origin/track_disk_correctly`, `origin/fix_hierarchy_bug`,
`origin/admin_interface`, `origin/file_versioning`, `origin/int8-support`,
`origin/topo-fullprecision`, `origin/deferred-rescore`,
`origin/bucketed-candidate-acquisition`, `origin/compaction-retrain-improve`,
`origin/surface_ml`, `origin/fork-workflow*`.

---

## 4. Cassandra fork branches

Repository: `/mnt/nvme/opt/cassandra`. Note the `trunk`/`origin/trunk` refs
date to 2016 — this fork's own work lives on the `compaction-integration*`
line only.

**`compaction-integration-referencepoint`** @ `7b35421f24` — **[verified]** —
the deployed branch, and **a deliberate measurement branch**. Two commits
define it:

- `b5ac025689 "shrink the explicit-flag surface to the upstream feature set"`
  removed seven required-explicit flags. Prior values, from the
  `jvm-server.options` diff:

  ```
  cassandra.sai.vector.pq_code_cache_enabled=true        <- a PQ code CACHE
  cassandra.sai.vector.pq_code_persistence=true
  cassandra.sai.vector.compaction_decoder_sharing=true
  cassandra.sai.vector.compaction_same_source_adc=true
  cassandra.sai.vector.compaction_codebook_policy=adopt  <- adopted, did not retrain
  cassandra.sai.vector.compaction_codebook_drift_margin=0
  cassandra.sai.vector.pq_drift_stats=true
  ```

- `6e1e437689 "reduce the merge path to what upstream jvector provides"`
  removed the machinery behind them, and states the consequence: *"Upstream
  always retrains the codebook and re-encodes every surviving node."*

That it is a reference point is explicit — `6e1e437689` keeps the in-place
write because a temp-file copy *"would have distorted exactly the IO
measurement this branch exists to take."* **The stripped state is intentional.
The open question is whether a measurement branch was meant to carry a 1B-row
production sweep.**

Twelve `cassandra.sai.vector.*` flags remain, under a fail-fast regime: no
defaults, unrecognized values rejected, and any undeclared key in the
namespace refuses startup. Consequence: the removed flags **cannot** be
restored by editing `jvm-server.options` — the accessors in
`VectorFeatureFlags.java` must come back first, i.e. a revert of
`b5ac025689` + `6e1e437689`.

**`compaction-integration`** (2026-07-27) and
**`compaction-integration-checkpoint-alpha`** (2026-07-27) *(tip only)* — tip
`55036e5c9e "codebook reuse experiment"`, i.e. the state *before* the flag
surface was shrunk. This is where the removed mitigations still exist.

**`origin/compaction-integration`** (2026-07-17) *(tip only)* — older remote
line; tip enables NVQ by default.

**`origin/iostory`** (2016) — unrelated legacy.

---

## 5. Why the deployed combination is slow — one paragraph

`MADV_RANDOM` on source mappings disables kernel readahead, so every uncached
touch is exactly one 4 KB fault. That was survivable while two bulk phases
incidentally streamed the sources first: the `getNodes(0)` sequential scan
(removed by `11ea5b9e`) and PQ retrain via full k-means++ (reduced to
`basePQ.refine()` by `711afea5`). Both were optimised away against
cohere-10M, where everything is cache-resident and the streaming bought
nothing. Meanwhile the xlink work made the dominant phase a graph *search per
base node* into the other source — serially dependent, one demand read in
flight per thread — and the compensating `posix_fadvise` prefetch
(`985bfe1e`) reaches `gatherFromSameSource` and a hardcoded 3-deep frontier,
but **not** `gatherFromOtherSource`, as the code says at
`OnDiskGraphIndexCompactor.java:1483`. Above RAM the result is 113k reads/s at
4.00 KB, md0 at 99% util, 50% iowait, and 0.000 MiB/s of actual compaction
byte progress.

Full detail, including the measured before/after (5,387–27,530 batches/min →
39–46 batches/min for the same ~31k-batch merge size) is in the session
memory note `jvector-compactor-packed-neighbor-io`.

---

## 6. Commit index

| commit | repo | effect |
|---|---|---|
| `11ea5b9e` | jvector | removed the `getNodes(0)` sequential scan (and its incidental cache warming) |
| `711afea5` | jvector | PQ retrain `compute()` → `basePQ.refine()`; fewer passes over source vectors |
| `985bfe1e` | jvector | fadvise prefetch: `FrontierPrefetchingView` + `gatherFromSameSource` only |
| `0a0d8c62` | jvector | bounded cluster search (`clusterSearchL0`) |
| `4c3b4e41` | jvector | pair-asymmetric cross-source linking |
| `a0c2e431` | jvector | full-precision cross-source search seeding |
| `e4a10978` | jvector | compactor-assigned similarity ordinals (default **off**) |
| `670f5588` | jvector | source pretouch — **not in the deployed branch** |
| `8e98e50b` | jvector | original `MADV_RANDOM` (predates rc.9; amplifier, not cause) |
| `b5ac025689` | Cassandra | removed seven required-explicit flags |
| `6e1e437689` | Cassandra | removed the merge-path machinery behind them |

---

## 7. Experimental branch — `experiment/compaction-io-prefetch-20260823`

jvector only, based on the deployed `xlink-integration` HEAD (`117e856f`).
Commit `11cb4acf`. **Not built into `/opt/cassandra/lib` — nothing deployed.**

Four knobs. Three were already-existing behaviour that could not be reached or
observed; one is a new default.

| property | default | effect |
|---|---|---|
| `jvector.compaction.frontierPrefetch` | `3` (unchanged) | `FrontierPrefetchingView.WIDTH`. The class javadoc always documented this property; the code hardcoded 3 and never read it. `0` disables hinting. |
| `jvector.compaction.batchPrefetchDensity` | `8` (unchanged) | Density guard on the own-record batch prefetch. `0` disables, negative makes it unconditional. Now counted (`batchPrefetchIssued` / `batchPrefetchDeclined`). |
| `jvector.compaction.crossSourceSeedPrefetch` | `true` (**new**) | Async-hints the cross-source seed records before the beam search reads them. Counted as `seedHints`. |
| `jvector.disk.adviseRandom` | `true` (unchanged) | `false` leaves kernel readahead on. **Process-wide — affects search mappings too.** A/B knob, not a production default. |

### Suggested arms

Baseline first, to reproduce the current state on the experimental build:

```
# A — baseline (should match the deployed tree)
-Djvector.compaction.crossSourceSeedPrefetch=false

# B — seed prefetch only (the one genuinely new mechanism)
-Djvector.compaction.crossSourceSeedPrefetch=true

# C — B plus a deeper frontier; the "3 is the knee" claim was measured
#     cache-resident, so sweep this
-Djvector.compaction.frontierPrefetch=8

# D — C plus unconditional own-record warming
-Djvector.compaction.batchPrefetchDensity=-1

# E — readahead restored as a fallback. Expect the 4.00 KB mean request size
#     to move; watch search latency too, since this is process-wide.
-Djvector.disk.adviseRandom=false
```

The primary metric is `Compaction I/O progress: N/M batches` in
`/var/log/cassandra/compaction.log`, segmented per merge — the counter resets
between merges, so a whole-file first-to-last delta is meaningless. Reference
numbers for a ~31k-batch merge: **39–46 batches/min** in the current bad state,
**5,387–27,530 batches/min** cache-resident on 2026-08-21.

Secondary: `iostat -x` mean request size on `md0` (currently pinned at 4.00 KB
— that is the signature), `%util`, and iowait.

### Deploying an arm

These are jvector system properties, so they belong in the Cassandra fork's
`conf/jvm-server.options`. That file had uncommitted local edits at the time of
writing and was deliberately left untouched — add the arm's `-D` lines by hand.
Note they are **not** in the `cassandra.sai.vector.*` namespace, so they are not
subject to that namespace's fail-fast validator; a typo will be silently
ignored rather than refusing startup. Check the startup log for
`jvector.disk.adviseRandom=false: mapping left on the kernel default` to
confirm arm E actually took.

### Not done

`670f5588` (source pretouch before bulk phases) is still absent. Cherry-picking
it conflicts in all five files because `985bfe1e` later rebuilt the same
infrastructure; its primitives survived into the tree but no call site warms a
source before the bulk phases. Adding one is the next experiment, and its
"skip when sources exceed MemAvailable" guard needs rethinking first — it
self-disables in exactly the above-RAM regime that motivates it.

No Cassandra-side changes were made. Restoring the seven removed flags means
reverting `b5ac025689` + `6e1e437689`, which is a decision about whether
`compaction-integration-referencepoint` was meant to carry this sweep — not a
change to make unilaterally.
