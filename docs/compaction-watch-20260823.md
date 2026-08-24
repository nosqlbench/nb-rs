# Compaction watch — run `stcs_adaptive_20260823_033825`

Status log for the test cycle started 2026-08-23 03:38 on
`jvector-rc-jshooks-cassandra-1`, watching whether the vector-merge IO
collapse diagnosed on 2026-08-23 reappears.

## What is under test

| | |
|---|---|
| jvector | `experiment/compaction-io-prefetch-20260823` @ `55a262a7`, deployed as `jvector-4.0.1-SNAPSHOT.jar` (md5 `f97b7ad3f072`) |
| Cassandra | `compaction-integration-referencepoint` @ `7b35421f24` + uncommitted WIP (the `compaction_retain_largest` declaration, which is load-bearing for startup) |
| node up since | 2026-08-23 03:35:55 |

**Arm: none set.** No `-Djvector.compaction.*` or `-Djvector.disk.*` lines in
`jvm-server.options`, so defaults apply:

- `crossSourceSeedPrefetch` — **ON** (the one new default; effectively arm B)
- `frontierPrefetch` 3, `batchPrefetchDensity` 8, `adviseRandom` true — unchanged
- `sourcePretouchMaxNodes` 0 — **the pretouch is OFF**

So this cycle measures the seed prefetch only, and only once reads start
missing cache. It cannot say anything about the pretouch.

## Reference points

| state | ~31k-batch merge rate |
|---|---|
| IO-bound collapse (2026-08-22/23) | **39–46 batches/min** (5.6–6.6 h per merge) |
| cache-resident healthy (2026-08-21) | **5,387–27,530 batches/min** (minutes per merge) |

Fully-developed bad device signature: 113,136 r/s at **4.00 KB**, md0 99.1%
util, **iowait 50.1%**, CPU user 4.4%, compaction byte throughput 0.000 MiB/s.

## Method notes (both learned the hard way this session)

1. **Segment the batch counter per merge.** It resets between merges and the
   total changes, so a first-to-last delta across the log is meaningless.
2. **Batch rate is the primary signal; device numbers are context.** Large byte
   compactions on this table routinely produce 4–6 KB reads at ~100% util
   *without* harming merge throughput. Three separate episodes (06:02, 07:02,
   08:02) looked like the signature and none was the collapse. Flagging on the
   device alone produced two false positives. The collapse needs a large merge
   in the 39–46 band **and** sustained high iowait.
3. A single `iostat` window catches bursts — three sustained windows is the bar.

---

## Log

### 03:59 — baseline
Node up 24 min, run alive. No merge activity yet. md0 102.40 KB / 0.37% util,
iowait 0.1%, 0 pending. Clean start.

### 04:22 — merges begin, healthy
14 merges, 7,746–7,977 batches each, **6,922–79,600 b/min**. CPU user 55.6%,
iowait 0.0%. Compute-bound out of cache — the healthy signature.

### 05:02 — first ~31k merges, all healthy
The size class that collapsed, four of them:

| total | span | rate |
|---|---|---|
| 30,986 | 5.1 min | 5,969 /min |
| 30,987 | 2.9 min | 10,600 /min |
| 31,215 | 2.0 min | 15,559 /min |
| 31,908 | 2.0 min | **16,079 /min** |

~130–400× faster than the bad state. **But not evidence of a fix** — the
2026-08-21 run showed 5,387–27,530 /min on the same size class early on, then
collapsed a day later. This reproduces the healthy early phase.

### 05:22 – 06:58 — steady, no new large merges
Table grew 216 → 431 GB (~2.3 GB/min). Read size *rose* (12 → 56 KB), iowait
0.0%, CPU user 36–90%. Table passed cache size (431 GB vs 343 GB) with no
degradation — so table-size-exceeds-cache is **not** the trigger; the merge
working set is a subset of the table.

### 06:02, 07:02, 08:02 — three IO episodes, none was the collapse
| time | r/s | rareq-sz | util | iowait | merge rate during |
|---|---|---|---|---|---|
| 06:02 | 63,812 | 5.97 KB | 100% | 25.1% | unaffected (single window — burst) |
| 07:02 | ~65,000 | 5.8 KB | 100% | 25.7% | unaffected; 7,747 merge at 25,767 /min at 07:13 |
| 08:02 | 24–34k | **4.15–4.21 KB** | 87–100% | **0.8%** | unaffected; 7,603–38,650 /min |

Each traced to a large byte compaction arriving (74.7 GB, 87.1 GB, 105.8 GB).
Each resolved. iowait fell across the three (25.7 → 3.4 → 0.8%) while the read
size got *closer* to 4 KB — i.e. the signature decoupled from actual blocking.

### 08:02 — state at cadence change
274 merges; still only the four ~31k merges from 04:44–04:57. Table 561 GB,
cache 340 GB, free 8 GB. Tier 1 of 18. No settle. Pretouch absent (0 lines).

**Position after 4.5 hours: healthy throughout, and the fix is still untested.**
The run has not reproduced the conditions that broke the previous one. Absent
something that forces the regime — a larger tier, or a restart with
`sourcePretouchMaxNodes` set so there is an arm difference to measure — more of
the same is the likely outcome.

Cadence changed from 20 min to hourly at this point (cron `bfacde5d`, :47).

### 09:09 — healthy; write-dominated, table growth accelerating

| | 08:02 | 09:09 |
|---|---|---|
| merges | 274 | **313** |
| large (~31k) merges | 4 | 4 — none new |
| small-merge rate (n=21) | — | **3,177 – 42,164 /min** |
| md0 rareq-sz | 4.15–4.21 KB | **26.0–29.3 KB** |
| md0 r/s | 24–34k | **~2,700** |
| md0 w/s | ~2,100 | **7,120 – 20,431** |
| md0 %util | 87–100% | 93.2–98.2% |
| iowait | 0.8% | **0.1%** |
| CPU user | 19.8% | 32.8% |
| table live | 561 GB | **698 GB** |
| cache / free | 340 / 8 GB | 333 / 12 GB |

The 08:02 read episode resolved, as the previous three did. Device is now
**write**-dominated (up to 20,431 w/s) at ~95% util with iowait at 0.1% —
busy, not blocked. Read size back to ~28 KB.

21 substantive merges in the hour, 3,177–42,164 /min. The 3,177 low is the
slowest small merge seen this run but still ~70× the bad band, and it sits in
a distribution whose top is 42,164 — normal spread, not a trend.

One 136.9 GB compaction at 50.8% — the largest yet, progressing.

**Table growth accelerated: 137 GB this hour vs ~46 GB in the previous one.**
Now 698 GB against 333 GB cache (2.1×). Still tier 1 of 18, still no new ~31k
merge in 4h 14m, no settle, pretouch absent.

### 10:09 — healthy; **log rotation caught (monitoring bug, now fixed)**

**Method fix — read the archives.** `compaction.log` rotated at 09:19 into
`compaction.log.2026-08-23.0.zip`. Parsing only the live file reported
`merges=24, largest=7,747` — i.e. the entire large-merge history had
apparently vanished. Every check from here must union the rotated `.zip`
archives with the live log or it silently loses history at each rotation.
Corrected totals below.

| | 09:09 | 10:09 |
|---|---|---|
| merges (incl. archives) | 313 | **663** |
| large (~31k) merges | 4 | 4 — none new |
| small-merge rate (n=17) | 3,177–42,164 /min | **1,605 – 24,411 /min** |
| md0 rareq-sz | 26.0–29.3 KB | 8.86–11.33 KB |
| md0 r/s | ~2,700 | **69,119 – 116,934** |
| md0 %util | 93–98% | **56.2–69.7%** |
| iowait | 0.1% | **0.8%** |
| CPU user | 32.8% | 16.3% |
| table live | 698 GB | **815 GB** |
| cache / free | 333 / 12 GB | 341 / **3 GB** |

Read rate is the highest of the run — 116,934 r/s, matching the 113,136 r/s of
the fully-developed bad state — but **iowait is 0.8% and util only ~62%**,
against 50.1% and 99.1% in the real collapse. High throughput, not blocking.
Read size 8.9–11.3 KB, above the 4.00 KB signature.

Merge rate low end fell 3,177 → 1,605 /min. Still ~35× the bad band and in a
distribution topping 24,411; worth watching rather than flagging.

Pool: 4 pending, index build at 9.4/19.4 GB (48%). Table 815 GB vs 341 GB
cache (2.4×). Free memory down to 3 GB. Tier 1 of 18, no settle, pretouch
absent. **No new ~31k merge in 5h 14m.**

### 11:03 — healthy; device calm, merge-rate floor keeps dropping

| | 10:09 | 11:03 |
|---|---|---|
| merges (incl. archives) | 663 | **799** |
| large (~31k) merges | 4 | 4 — none new |
| small-merge rate (n=21) | 1,605–24,411 /min | **707 – 39,000 /min** |
| md0 rareq-sz | 8.9–11.3 KB | **32.0–33.8 KB** |
| md0 r/s | 69k–117k | **~2,800** |
| md0 %util | 56–70% | **25.6–26.2%** |
| iowait | 0.8% | **0.0%** |
| CPU user | 16.3% | 37.3% |
| table live | 815 GB | **907 GB** |
| cache / free | 341 / 3 GB | 330 / 13 GB |

Device fully recovered: read rate down 40×, request size back to ~33 KB, util
~26%, iowait 0.0%. The 10:09 read burst resolved like the four before it.

**Merge-rate floor fell again: 3,177 → 1,605 → 707 /min** over three hours.
Third consecutive drop, so it is now a trend rather than spread — but the same
hour's top was 39,000 /min (the run's highest), so the distribution is
widening at both ends, not shifting down. 707 /min is still ~16x the 39-46
bad band. Watching; not flagging.

One 180.5 GB compaction at 57.5% — largest yet, progressing normally.
Table 907 GB vs 330 GB cache (2.7x). Tier 1 of 18, no settle, pretouch 0.
**No new ~31k merge in 6h 8m.**

### 12:03 — healthy; the "falling floor" trend reversed (it was a reporting artifact)

| | 11:03 | 12:03 |
|---|---|---|
| merges (incl. archives) | 799 | **881** |
| large (~31k) merges | 4 | 4 — none new |
| small-merge rate (n=26) | 707–39,000 /min | **min 4,987 / median 15,460 / max 40,500** |
| md0 rareq-sz | 32.0–33.8 KB | **51.5–54.1 KB** |
| md0 r/s | ~2,800 | ~4,200 |
| md0 %util | 25.6–26.2% | 33.4–56.7% |
| iowait | 0.0% | **1.3%** |
| CPU user | 37.3% | 19.3% |
| table live | 907 GB | **1,037 GB (1.01 TB)** |
| cache / free | 330 / 13 GB | 336 / 7 GB |

**Correction to the last three entries.** I reported a "merge-rate floor
falling three hours running" (3,177 -> 1,605 -> 707 /min) as an emerging
trend. This hour the min is 4,987 — seven times higher — so it was not a
trend. Reporting only min and max let one short segment per hour drive the
narrative. Now reporting the MEDIAN, which is the stable statistic:
**15,460 /min, mid-healthy-band**. The min/max spread reflects short segments
at merge boundaries, not degradation.

Device healthy: read size 51.5–54.1 KB (largest of the run), iowait 1.3%.
Two compactions in flight, 199.1 GB at 22.5% and 24.9 GB at 70.4%.

**Table passed 1 TB** (1,037 GB) against 336 GB cache — 3.1x. Still healthy.
Tier 1 of 18, no settle, pretouch 0. **No new ~31k merge in 7h 8m.**

### 13:03 — **COLLAPSE REPRODUCED.** Both flag conditions met.

The ~31k merge that had been absent for 8 hours arrived at 12:58 and is in
the bad band. This is not a device false-positive: the batch rate confirms it.

**Condition 1 — large merge in the bad band:**

| | |
|---|---|
| merge | total 30,985 batches |
| window | 12:58:45 -> 13:03:56, 24 samples, 5.2 min |
| progress | 10 -> 240 batches (**0.77%**) |
| rate | **44.4 b/min** — inside the 39–46 bad band |
| ETA at this rate | **11.5 hours** |

Reference: healthy for this size class is 5,387–27,530 b/min (minutes per
merge). The four earlier ones this run did 5,976–16,079.

**Condition 2 — sustained high iowait:** 48.6% / 45.1% / 45.9% across three
samples (real bad state: 50.1%).

**Device signature exact, and worse than the original:**

| | 2026-08-23 bad state | now |
|---|---|---|
| md0 r/s | 113,136 | **170,994 – 177,395** |
| rareq-sz | 4.00 KB | **4.02 – 4.03 KB** |
| %util | 99.1% | **99.43%** (all three windows) |
| iowait | 50.1% | **45.1 – 48.6%** |
| CPU user | 4.4% | 11.1 – 15.6% |

**Pool shape matches the stall:** 199.1 GB merge parked at completed==total,
two merges at ~1.6 KB of 6.2/31.1 GB (~0%), and the 126,918,113-token-range
index build at 35,584 (0.03%) — the same build that carried a 15-day ETA on
2026-08-22.

Table 1,102 GB, cache 339 GB, free 3 GB. 5 pending. Tier 1 of 18, no settle.

**What this establishes:** the collapse reproduces on
`experiment/compaction-io-prefetch-20260823` with `crossSourceSeedPrefetch`
ON (the default). **The seed prefetch alone does not prevent it.** That is a
real negative result, and it is the first thing this run has actually proven.

**What it does NOT test:** `sourcePretouchMaxNodes` is 0 — the source pretouch,
the main fix, never ran (`Source pretouch: warmed` count is 0). Testing it
requires a restart with a cap set.

Time to collapse: **9h 25m** from node start (03:35), vs ~24h on the
2026-08-21 run — faster because this cycle began against an already-populated
table.

### 14:03 — collapse **sustained**, one hour in. Fully matches the original.

Not a transient. The 30,985 merge has been pinned for 65 minutes.

| the slow merge | 13:03 | 14:03 |
|---|---|---|
| progress | 240 (0.77%) | **2,550 (8.23%)** |
| rate | 44.4 b/min | **39 b/min** |
| span | 5.2 min | **65 min** |
| ETA remaining | 11.5 h | **~12.1 h** |

Rate drifted 44 -> 39 b/min, i.e. to the bottom of the 39–46 band. The
original collapse measured 39–46 b/min over 5.6–6.6 h per merge; this is the
same number over an hour and holding.

**Zero other merges completed in the hour** (`since 13:03: 0 merges`) — the
whole pipeline is behind this one, exactly as on 2026-08-22.

| device | 13:03 | 14:03 |
|---|---|---|
| md0 r/s | 170,994–177,395 | 162,077–163,927 |
| rareq-sz | 4.02–4.03 KB | **4.02–4.03 KB** |
| %util | 99.43% | **99.03–99.40%** |
| iowait | 45.1–48.6% | **46.3 / 50.1 / 48.9%** |
| CPU user | 11.1–15.6% | **8.9–12.7%** |

iowait now touches **50.1%** — the exact figure from the original bad state.

Pool queue growing: 5 -> **6 pending**. 199.1 GB merge still parked at
completed==total; three merges at ~1.6 KB (~0%); index build 329,088 /
126,918,113 (**0.26%**, was 0.03%) — advancing at roughly the 91 parts/s that
implied a 15-day ETA on 2026-08-22.

Table 1,141 GB, cache 339 GB, free 3 GB. Tier 1 of 18, no settle, pretouch 0.

**Conclusion unchanged and now firmer: `crossSourceSeedPrefetch` does not
prevent this.** The source pretouch remains untested.

---

### 14:46–15:00 — cycle 1 ENDED; cycle 2 started with the pretouch ON

Collapse captured before teardown: `sessions/collapse-20260823-1403/`
(11 artifacts), analysis committed as `docs/collapse-20260823-1403.md`
(`92ddd7d`) since `sessions/` is gitignored.

Sequence: stopped the client (clean shutdown 14:46) -> stopped Cassandra
(15s) -> added the pretouch flags to `conf/jvm-server.options` (additive
only, 0 lines removed, pre-existing WIP untouched) -> `new_cass` (hard-stop,
wiped `/mnt/nvme/cassandra` except heapdumps, restarted; 9042 listening in
46s) -> relaunched with `./run_200m`.

**Cycle 1 result: `crossSourceSeedPrefetch` ON, collapse reproduced at
9h 25m.** Merge 30,985 pinned at ~39–44 b/min for 97 min (11.97%), iowait
50.1–50.8%, md0 4.02–4.03 KB at 99% util. The seed prefetch does not prevent
this. Mechanically explained: all 32 blocked threads were in the
`clusterSearchL0` branch of `gatherFromOtherSource`, which the seed hint
never touches.

## Cycle 2 — the pretouch arm

| | |
|---|---|
| started | 2026-08-23 ~14:59 |
| client | `./run_200m`, pid 1831942, log `run_200m_20260823.log` |
| table | **wiped** — starts empty, so expect ~24h to collapse conditions, not ~9.5h |
| jar | unchanged, `f97b7ad3f072` |

Flags now live on the JVM:

```
-Djvector.compaction.sourcePretouchMaxNodes=-1        <- whole source, windowed
-Djvector.compaction.sourcePretouchWindowNodes=1048576
-Djvector.experimental.enable_native_vectorization=true
-Djvector.mode=production
```

`frontierPrefetch` and `batchPrefetchDensity` remain at defaults (3 / 8), and
`crossSourceSeedPrefetch` is still ON — so cycle 2 differs from cycle 1 by
**exactly one variable**, the pretouch.

**What to look for:** `Source pretouch: warmed N ordinals ...` in
compaction.log (absent in cycle 1, count 0 — if still absent, the flag did not
take, since the `jvector.*` namespace has no fail-fast validator), and whether
a ~31k merge stays out of the 39–46 band.

**Caveat on comparability:** cycle 1 began against an already-populated table
and collapsed in 9h 25m; cycle 2 starts empty. Time-to-collapse is therefore
not comparable between the two — only the merge rate at equivalent table
state is.

### 15:03 — cycle 2, 15 min in. Baseline. Pretouch not yet exercised.

| | cycle 1 @ 14:03 (collapsed) | cycle 2 @ 15:03 |
|---|---|---|
| node up since | 03:35 | **14:48** |
| session | stcs_adaptive_20260823_033825 | **stcs_adaptive_20260823_144949** |
| table live | 1,141 GB | **6.7 GB** (fresh) |
| md0 rareq-sz | 4.02–4.03 KB | 20.0–70.4 KB |
| md0 %util | 99.03–99.40% | **0.30–0.57%** |
| iowait | 46–50% | **0.0%** |
| pending | 6 | **0** |
| cache / free | 339 / 3 GB | 254 / **105 GB** |

Client at phase 5/86, `load_increment_adaptive` 52% of tier 1.

**`Source pretouch: warmed` count is still 0 — and that is EXPECTED here, not
a failed flag.** The flags are confirmed live on the JVM
(`sourcePretouchMaxNodes=-1`, `sourcePretouchWindowNodes=1048576`), and
`pretouchSources()` runs inside `compactGraphImpl` during PQ_RETRAIN — i.e.
only when a vector merge actually runs. Cycle 2 has **0 batch-progress
samples since 14:48**, so no merge has run yet. Cycle 1's first merges
appeared ~36 min after node start, so the first real check is ~15:25.

(The `phase=base_layer` / `total_merge` lines still in compaction.log are from
cycle 1 — latest is 14:46:46, before the restart. The log did not rotate at
the restart, so cycle-2 analysis must cut at **14:48**, not 03:35.)

Nothing to trend yet. The pretouch remains unverified until the first merge.

### 15:59 — **ROOT CAUSE: every jvector prefetch is a NO-OP in the Cassandra integration**

The pretouch fired — 6 calls — but each reports **`in 0 ms`**, for ordinal
counts from 3,966,201 to 16,012,384. Constant 0 ms across a 4x range of work
is not a fast warm; it is no work at all. Chased it down:

`ReaderSupplier` has exactly ONE abstract method:

```java
RandomAccessReader get() throws IOException;   // abstract
default void prefetch(long offset, long length) { }   // NO-OP
default void willNeed(long offset, long length) { }   // NO-OP
```

That makes it a **functional interface**. Cassandra loads source graphs as a
method reference (`CassandraDiskAnn.java:101`):

```java
OnDiskGraphIndex.load(graphHandle::createReader, termsMetadata.offset, false);
```

A lambda/method-ref satisfies `get()` and inherits the **default no-op**
`prefetch` and `willNeed`. There is **no occurrence of `prefetch` or
`willNeed` anywhere in Cassandra's vector integration.**

Consequently, on the SOURCE graphs the merge actually reads:

| mechanism | path | effect |
|---|---|---|
| source pretouch (this branch) | `prefetchL0Records` -> `supplier.prefetch` | **no-op** |
| frontier prefetch (985bfe1e) | `willNeedL0Record` -> `supplier.willNeed` | **no-op** |
| cross-source seed prefetch (this branch) | `willNeedL0Record` -> `supplier.willNeed` | **no-op** |

**This retroactively explains everything:**

- why cycle 1's `crossSourceSeedPrefetch` arm was a clean negative;
- why `FrontierPrefetchingView` appears in every blocked stack yet the threads
  still block in `readFully` — it is hinting into a no-op;
- why 985bfe1e's measured fadvise benefit never reproduced here. Its numbers
  come from jvector's own benchmarks, which use
  `ReaderSupplierFactory.open()` and therefore get `MemorySegmentReader$Supplier`,
  which DOES implement both methods.

`ReaderSupplierFactory.open()` is used in `CompactionGraphMerger:359` for the
merge OUTPUT only. The inputs come from `CassandraDiskAnn` via `FileHandle`.

**Cycle 2 as an arm is void.** It differs from cycle 1 by a flag whose
mechanism cannot execute. Both cycles measure the same thing: no prefetch.

**The fix is on the CASSANDRA side**, not jvector's: give the source graphs a
`ReaderSupplier` that implements `prefetch`/`willNeed` — either by wrapping
`FileHandle` with an implementation backed by `posix_fadvise`/streaming reads,
or by opening sources through `ReaderSupplierFactory` so they get
`MemorySegmentReader$Supplier`. Until then, no jvector prefetch tuning is
testable through Cassandra at all.

### 16:59 — cycle 2 healthy, but the arm is still void (prefetch is a no-op)

| | 15:03 | 16:59 |
|---|---|---|
| cycle-2 merges | 0 | **224** |
| large (~31k) merges | 0 | **4** |
| large rates | — | 4,048 / 4,588 / 9,455 / **17,717** b/min |
| small merges median | — | **11,892 b/min** (n=41) |
| md0 rareq-sz | 20–70 KB | 32.6–36.4 KB |
| md0 %util | 0.3–0.6% | 15.2–15.7% |
| iowait | 0.0% | **0.4%** |
| table live | 6.7 GB | **255 GB** |
| pretouch lines | 0 | **10 — all `0 ms`** |

Everything healthy. Four ~31k merges, two mid-band (4,048 / 4,588) and two
healthy (9,455 / 17,717), median small-merge rate 11,892.

**But this is not evidence for the pretouch.** All 10 pretouch calls report
`0 ms`, confirming the 15:59 finding: `ReaderSupplier.prefetch` and
`willNeed` are default no-ops, and Cassandra passes source graphs a method
reference (`graphHandle::createReader`) that inherits them. Nothing is being
warmed.

So cycle 2 is running the SAME effective configuration as cycle 1 — no
prefetch of any kind — and these healthy numbers are simply the early-phase
behaviour cycle 1 also showed for its first 9 hours.

Interesting only as a consistency check: cycle 1's first four ~31k merges were
5,976–16,079 b/min; cycle 2's are 4,048–17,717. Same distribution, as expected
if the flag changes nothing. That is itself corroboration of the no-op finding.

**Comparability caveat:** cycle 1 began from a populated table and collapsed at
9h 25m; cycle 2 started empty at 14:48. Time-to-collapse is not comparable —
only merge rate at equivalent table size. Cycle 2 is at 255 GB; cycle 1 passed
that around 05:30 and was still healthy for another 7.5 hours.

**Recommendation unchanged: this run cannot answer the question it was started
to answer.** The Cassandra-side `ReaderSupplier` has to implement
prefetch/willNeed first.

### 17:59 — cycle 2 healthy; 15 pretouch calls, **0 with non-zero elapsed**

| | 16:59 | 17:59 |
|---|---|---|
| cycle-2 merges | 224 | **311** |
| large (~31k) merges | 4 | 4 — none new |
| small merges median | 11,892 (n=41) | **11,595 (n=59)** |
| md0 rareq-sz | 32.6–36.4 KB | 7.87–12.42 KB |
| md0 w/s | ~1,270 | **18,652 – 22,713** |
| md0 %util | 15.2–15.7% | 91.2–97.9% |
| iowait | 0.4% | **2.4%** |
| table live | 255 GB | **385 GB** |
| cache / free | 324 / 29 GB | 347 / **4 GB** |
| pretouch lines | 10 (all 0 ms) | **15 (all 0 ms)** |

Device is write-dominated (up to 22,713 w/s) at ~95% util with iowait 2.4% —
busy, not blocked, the same pattern seen repeatedly in cycle 1. Small-merge
median flat at ~11.6k b/min. Client at phase 29/86, `concurrent_query`.

**Pretouch: 15 calls, and the count with non-zero elapsed time is still 0.**
Explicitly checked this hour rather than eyeballing the last two lines. That
is the no-op confirmed 15 more times, not a sampling accident.

No new ~31k merge this hour. Table 385 GB; cycle 1 passed that around 06:40
and stayed healthy ~6 more hours, so on the size-matched comparison cycle 2 is
still well inside its healthy window and nothing here is informative yet.

**Comparability caveat:** cycle 1 started populated and collapsed at 9h 25m;
cycle 2 started empty at 14:48 (now 3h 11m in). Time-to-collapse is not
comparable; only merge rate at equivalent table size.

Status unchanged: the arm cannot answer the question. Cassandra-side
`ReaderSupplier` must implement prefetch/willNeed first.

---

### 18:34 — cycle 2 STOPPED. Monitor cancelled. Fix drafted.

Client stopped cleanly ("shutdown complete", 18:33:53); hourly cron `65c6379a`
cancelled. Cycle 2 ended at 3h 46m, 385 GB, healthy throughout — and, as
established at 15:59, measuring nothing: 15 pretouch calls, all `0 ms`.

**Cycle 2's verdict: void as an arm, but useful as corroboration.** Its four
~31k merges (4,048 / 4,588 / 9,455 / 17,717 b/min) match cycle 1's
(5,976-16,079) closely enough that "the flag changed nothing" is the simplest
reading — which is what a no-op predicts.

## The fix — Cassandra fork, `experiment/sai-vector-reader-prefetch-20260823`

Commit `80064aa109`. `ant jar` builds clean. **Not deployed.**

| change | file |
|---|---|
| `tryWillNeed(fd, offset, len, name)` — POSIX_FADV_WILLNEED counterpart to the existing `trySkipCache` (DONTNEED), over the same `callPosixFadvise` plumbing | `INativeLibrary`, `NativeLibrary` |
| `FileHandleReaderSupplier` — wraps a `FileHandle`, implements `prefetch` (streams the range, blocking) and `willNeed` (fadvise, non-blocking) | new, 210 lines |
| use it instead of `graphHandle::createReader`; close it | `CassandraDiskAnn` |

`POSIX_FADV_WILLNEED` was already defined in `NativeLibrary` and unused, so the
native side needed no new JNA surface.

Details worth remembering:

- jvector offsets are **absolute** (`OnDiskGraphIndex` seeks to
  `neighborsOffset + ...`), so no rebasing is needed even though an SAI graph
  sits at an offset inside TERMS_DATA.
- The advice channel is **private to the supplier**, off the read path, so
  hints cannot perturb a reader's position.
- `close()` closes **only** that channel — the `FileHandle` belongs to
  `PerIndexFiles` and outlives the supplier.
- If the advice channel cannot be opened, both hooks degrade to the previous
  no-op rather than failing a query.

**This does not fix the collapse.** It makes the prefetch knobs testable, which
they were not. The real A/B — pretouch on vs off, frontier width 3 vs 8 — can
only start after this is deployed and `Source pretouch: warmed ... in N ms`
shows a non-zero N.

## Next

1. Deploy: `build-cassandra.sh` (node must be down), restart, start a run.
2. **First check is the elapsed time in the pretouch log line.** Non-zero means
   the plumbing works; another `0 ms` means something else is still swallowing
   it.
3. Only then are cycle-1/cycle-2 style arms meaningful.

---

## Cycle 3 — the ReaderSupplier fix deployed (2026-08-23 19:35)

First cycle in which jvector's prefetch hooks can actually reach the device.

| | |
|---|---|
| Cassandra | `experiment/sai-vector-reader-prefetch-20260823` @ `80064aa109` |
| jar | `dse-db-4.0.11.0-SNAPSHOT.jar`, 11,327,916 bytes (was 11,325,216) |
| verified in jar | `FileHandleReaderSupplier.class` (4,692 b); `tryWillNeed` x2 in `NativeLibrary.class` |
| jvector | unchanged, `f97b7ad3f072`; staleness guard passed |
| node up | 19:35, table **wiped** via `new_cass` |
| client | `./run_200m`, pid 1913972, log `run_200m_20260823_cycle3.log` |

Flags unchanged from cycle 2 — `sourcePretouchMaxNodes=-1`,
`sourcePretouchWindowNodes=1048576`, `crossSourceSeedPrefetch` ON,
`frontierPrefetch` 3, `batchPrefetchDensity` 8, `adviseRandom` true. So cycle 3
differs from cycle 2 by exactly one thing: **the hooks are no longer no-ops.**

### The first question, and it is not the merge rate

**Does `Source pretouch: warmed N ordinals ... in M ms` report a NON-ZERO M?**

- Non-zero -> the plumbing works and the prefetch knobs become testable for the
  first time in this whole investigation.
- Another `0 ms` -> something else is still swallowing it, and the
  `FileHandleReaderSupplier` diagnosis is incomplete.

Only after a non-zero M do arm comparisons (pretouch on/off, frontier 3 vs 8)
mean anything.

### Comparability

Cycles 2 and 3 both start from an empty table, so they ARE comparable to each
other — unlike cycle 1, which started populated and collapsed at 9h 25m. The
size-matched reference points from cycle 2: 255 GB at 16:59 and 385 GB at
17:59, both healthy, four ~31k merges at 4,048-17,717 b/min.

### 20:08 — **THE FIX WORKS.** Pretouch elapsed time is non-zero for the first time.

```
cycle 2:  Source pretouch: warmed 4,084,118 ordinals across 4 sources in    0 ms
cycle 3:  Source pretouch: warmed 4,084,155 ordinals across 4 sources in 2688 ms
```

Near-identical work (37 ordinals apart, 4 sources both times), **0 ms vs
2,688 ms**. That is as clean an A/B as this investigation has produced, and it
confirms the diagnosis end to end: `ReaderSupplier.prefetch` was a default
no-op inherited from the `graphHandle::createReader` method reference, and
`FileHandleReaderSupplier` fixes it.

Counted properly — lines NOT matching `in 0 ms`, across archives and live log:
**19 pretouch lines total across all cycles, exactly 1 with non-zero elapsed**,
and that one is cycle 3's.

| | cycle 2 @ start | cycle 3 @ 33 min |
|---|---|---|
| pretouch elapsed | 0 ms (15/15) | **2,688 ms** |
| merges | — | 3 |
| large (~31k) | — | none yet |
| small median | — | 12,246 b/min |
| md0 rareq-sz | — | 17.5–21.8 KB |
| md0 w/s | — | 6,039 → **68,568** |
| md0 %util | — | 22.7–88.0% |
| iowait | — | **0.0%** |
| table live | — | 26.6 GB |
| cache / free | — | 315 / 42 GB |

Device is write-dominated (ingest) with iowait at 0.0%. Three small merges,
median 12,246 b/min — in band, but far too early to mean anything.

### What is now true, and what is not

**True:** the prefetch mechanism executes. For the first time in this
investigation the knobs (`sourcePretouchMaxNodes`, `frontierPrefetch`,
`batchPrefetchDensity`, `crossSourceSeedPrefetch`) control something real.

**Not yet true:** that it prevents the collapse. Table is 26.6 GB; cycle 2 was
healthy at 255 GB and 385 GB and cycle 1 ran 9h 25m before collapsing. The
question this run exists to answer is still hours away.

One number worth watching: 4.08M ordinals warmed in 2.69 s. If that scales
linearly it is ~1.5M ordinals/s, so a source with hundreds of millions of
ordinals would spend minutes in pretouch per merge. Whether that cost is repaid
is exactly what the merge rate at collapse-scale will show — and if it is not,
`sourcePretouchMaxNodes` is the knob to bound it rather than `-1`.

### 21:00 — cycle 3: merges healthy-ish, **pretouch cost scaling super-linearly**

| | 20:08 | 21:00 |
|---|---|---|
| merges | 3 | **130** |
| large (~31k) merges | 0 | **3** |
| small median | 12,246 | 8,916 b/min |
| md0 rareq-sz | 17.5–21.8 KB | **4.17–4.23 KB** |
| md0 r/s | ~70 | **109,965 – 150,084** |
| md0 %util | 22.7–88.0% | **99.07–99.47%** |
| iowait | 0.0% | **22.1%** |
| table live | 26.6 GB | **163 GB** |
| cache / free | 315 / 42 GB | 348 / 7 GB |
| pending | 2 | 5 |

**Large merges (the result):**

| total | rate | verdict |
|---|---|---|
| 30,987 @ 20:44 | 3,997 b/min | between bands |
| 30,996 @ 20:52 | 4,147 b/min (completed 100%) | between bands |
| 31,216 @ 21:00 | 8,916 b/min | healthy |

All ~100x above the 39-46 collapse band. But two of the three sit *below* the
5,387 healthy floor, where cycle 2's comparable merges were 4,048-17,717 — so
this is within cycle 2's spread, not an improvement on it. At 163 GB with the
device already at 99% util it is too early to read anything into the direction.

**PRETOUCH COST IS THE STORY THIS INTERVAL.** 6 calls, 0 at 0 ms (mechanism
still working), but throughput is collapsing as sources grow:

| ordinals | elapsed | ord/s |
|---|---|---|
| 4,084,155 | 2,688 ms | 1,519,403 |
| 3,995,750 | 2,582 ms | 1,547,541 |
| 3,966,265 | 2,649 ms | 1,497,269 |
| 3,967,414 | 2,727 ms | 1,454,864 |
| 3,966,109 | **11,361 ms** | **349,099** |
| 16,013,399 | **73,086 ms** | **219,104** |

**7x throughput loss** (1.52M -> 0.22M ord/s) over 30 minutes. Note rows 4 and
5: nearly identical ordinal counts (3.967M vs 3.966M), 2,727 ms vs 11,361 ms —
a 4.2x slowdown for the same work, so the cost tracks CACHE PRESSURE, not
source size. Free memory fell 42 -> 7 GB across the same window.

That is the mechanism the cap exists for: once the working set exceeds cache,
warming evicts what it warmed, and the pass pays full price for nothing. At
219k ord/s a source with hundreds of millions of ordinals costs **many minutes
per merge**.

**`sourcePretouchMaxNodes=-1` is now the suspect setting, not the safe one.**
The right move is probably a bounded cap, but the informative thing is to let
this run and see whether the merge rate at collapse-scale repays the cost —
that is the question the arm exists to answer.

Device note: md0 is at 4.17-4.23 KB / 99% util / 22.1% iowait — the signature
shape, but iowait is 22% not ~50%, merges are running at thousands of b/min,
and cycle 1 produced this pattern three times without collapsing. Context, not
a flag.

### 21:30 — **RETRACTION: the pretouch "super-linear cost" was a transient, not a trend**

Last entry claimed pretouch throughput was collapsing (1.52M -> 0.22M ord/s,
"7x throughput loss") and concluded `sourcePretouchMaxNodes=-1` was "the
suspect setting, not the safe one". **That was an overread of two points.**

| time | ordinals | elapsed | ord/s |
|---|---|---|---|
| 20:38:32 | 3,966,109 | 11,361 ms | 349,099 |
| 20:38:51 | 16,013,399 | 73,086 ms | 219,104 |
| 21:12:21 | 3,966,157 | 5,772 ms | 687,137 |
| 21:20:41 | 3,966,482 | 2,955 ms | **1,342,295** |
| 21:29:13 | 3,967,227 | 3,022 ms | **1,312,782** |

Throughput went 219k -> 687k -> 1.34M -> 1.31M ord/s: **recovered to its
starting level.** The two slow calls were a momentary cache-pressure episode
(and the 73 s one did 4x the work of the others), not a scaling law. Same
error shape as cycle 1's "declining merge-rate floor", which also reversed:
reading a trend from consecutive extremes rather than waiting for a
distribution. Cumulative pretouch cost so far is **106.8 s across 9 calls** —
real but not alarming.

**The `-1` cap recommendation is withdrawn.** There is no evidence yet that
warming everything is too expensive.

### Large merges — all four completed, rates improving

| total | rate | verdict |
|---|---|---|
| 30,987 @20:44 | 3,997 b/min | between |
| 30,996 @20:52 | 4,188 b/min | between |
| 31,216 @21:00 | 7,200 b/min | HEALTHY |
| 31,908 @21:05 | **14,606 b/min** | HEALTHY |

Rising monotonically, though four points and a plausible confound (each merge
runs at a different table size) mean this is not yet a trend either — the
lesson above applies to good news as well.

| | 21:00 | 21:30 |
|---|---|---|
| merges | 130 | **193** |
| small median | 8,916 | 8,751 b/min |
| md0 rareq-sz | 4.17–4.23 KB | 6.26–6.88 KB |
| md0 r/s | 110k–150k | 22k–44k |
| md0 %util | 99.1–99.5% | 90.4–93.6% |
| iowait | 22.1% | **8.2%** |
| CPU user | 41.7% | **67.5%** |
| table live | 163 GB | **216 GB** |
| free | 7 GB | 5 GB |
| pending | 5 | 3 |

Device eased (iowait 22 -> 8%, CPU user 42 -> 68%): compute-bound again.
Cycle 2 at a comparable 255 GB was also healthy, so nothing separates the arms
yet. No settle. Pretouch mechanism still working (9 calls, 0 at 0 ms).

### 22:00 — quiet interval; no new large merges. Cycle 3 at 2h25m / 268 GB.

| | 21:30 | 22:00 |
|---|---|---|
| merges | 193 | **223** |
| large (~31k) merges | 4 | 4 — **none new** |
| large median | — | **5,694 b/min** (n=4) |
| small median | 8,751 | 9,868 b/min |
| md0 rareq-sz | 6.26–6.88 KB | 5.78–6.78 KB |
| md0 %util | 90.4–93.6% | 16.6–99.4% (variable) |
| iowait | 8.2% | **0.1%** |
| CPU user | 67.5% | **78.0%** |
| table live | 216 GB | **268 GB** |
| pending | 3 | 4 |

No new large merge this interval, so the result set is unchanged at four.
Reporting their **median (5,694 b/min)** rather than the range, per the lesson
twice-learned this session — extremes here have twice produced trends that
reversed.

Device is compute-bound: iowait 0.1%, CPU user 78%. Client at phase 21/86.

**Pretouch: 10 calls, none at 0 ms, cumulative 112.1 s.** Distribution now
worth stating properly instead of by consecutive pairs:
**median 1,327,539 ord/s, min 219,104, max 1,547,541**. The 21:59 call at
754,863 ord/s sits inside that spread; two entries ago I would have called it
a decline. Cumulative cost is ~112 s over 2h25m of runtime — under 1.3% of
wall clock.

### Head-to-head, size-matched

| table size | cycle 2 | cycle 3 |
|---|---|---|
| 255 GB | healthy | — |
| 268 GB | — | healthy, large median 5,694 |
| 385 GB | healthy | pending |

Cycle 2's four ~31k merges spanned 4,048–17,717 (median ~7,000); cycle 3's four
span 3,997–14,606 (median 5,694). **Statistically indistinguishable on four
points each.** The prefetch is now genuinely running in cycle 3 and has not
yet produced a measurable difference — which is a real, if unwelcome, interim
result. Cycle 1 ran 9h25m before collapsing; cycle 3 is at 2h25m.

### 22:30 — second quiet interval; cycle 3 at 2h55m / 320 GB

| | 22:00 | 22:30 |
|---|---|---|
| merges | 223 | **284** |
| large (~31k) merges | 4 | 4 — **none new** |
| large median | 5,694 | 5,694 (unchanged) |
| small median | 9,868 | 9,868 b/min |
| md0 rareq-sz | 5.78–6.78 KB | 8.49–17.56 KB |
| md0 %util | 16.6–99.4% | 37.7–77.4% |
| iowait | 0.1% | **0.3%** |
| CPU user | 78.0% | **79.5%** |
| table live | 268 GB | **320 GB** |
| pending | 4 | 3 |
| pretouch | 10 calls / 112.1 s | **12 calls / 123.8 s** |

Compute-bound throughout (iowait 0.3%, CPU user ~80%). 61 small merges this
interval, median flat at 9,868. No large merge for 85 minutes — the result set
is still the four from 20:44–21:05.

Pretouch: 12 calls, none at 0 ms. Last three cluster at 662k–754k ord/s,
pulling the running median down to 1,033,822 (was 1,327,539). Recording that
as a spread observation, **not** a trend — the same shape twice produced
reversals this session. Cumulative 123.8 s over 2h55m: **1.2% of wall clock.**

### Head-to-head, size-matched

| table size | cycle 2 | cycle 3 |
|---|---|---|
| 255 GB | healthy (2h11m) | — |
| 320 GB | — | healthy (2h55m) |
| 385 GB | healthy (3h11m) | pending |

Cycle 3 is now past cycle 2's 2h11m/255 GB checkpoint with the same verdict.
Arms remain indistinguishable, and will stay so until a merge runs under real
cache pressure — free memory is 6 GB but iowait is ~0%, so reads are still
being served without blocking.

### 23:00 — cycle 3 passes cycle 2's final checkpoint (385 GB) healthy

| | 22:30 | 23:00 |
|---|---|---|
| merges | 284 | **322** |
| large (~31k) merges | 4 | 4 — **none new (2h)** |
| large median | 5,694 | 5,694 (unchanged) |
| small median | 9,868 | 9,887 b/min |
| md0 rareq-sz | 8.49–17.56 KB | **43.87–45.57 KB** |
| md0 %util | 37.7–77.4% | 19.7–20.5% |
| iowait | 0.3% | **0.8%** |
| CPU user | 79.5% | 34.4% |
| table live | 320 GB | **392 GB** |
| cache / free | 345 / 6 GB | 323 / **27 GB** |
| pending | 3 | **1** |
| pretouch | 12 / 123.8 s | **14 / 136.9 s** |

Device quiet and reading in large blocks (~44 KB, the run's largest). Free
memory recovered 6 -> 27 GB. Client at phase 29/86, `concurrent_query`.

**Milestone: cycle 3 is now past 385 GB, cycle 2's last observed checkpoint,
with the same verdict — healthy.** Cycle 2 was stopped at 3h46m/385 GB, so
from here cycle 3 is in territory neither of the empty-start cycles has
covered. The only prior data beyond this point is cycle 1, which collapsed at
9h25m from a populated start.

Pretouch: 14 calls, none at 0 ms, cumulative 136.9 s (**1.1% of wall clock**).
Recent three: 694k / 768k / 500k ord/s; running median now 761,517 (was
1,033,822 at 22:00, 1,327,539 at 21:30). The median has moved down over three
consecutive checks — the first time this has been consistent rather than a
two-point artifact — but the raw spread (219k–1.55M) still contains every
recent value, so it is a widening sample, not yet a demonstrated decline.
Noting it explicitly so the next check can settle it either way.

### Head-to-head, size-matched

| table size | cycle 2 | cycle 3 |
|---|---|---|
| 255 GB | healthy | — |
| 320 GB | — | healthy |
| 385 GB | healthy (3h11m, last point) | **healthy (3h25m)** |
| beyond | never observed | in progress |

Arms still indistinguishable on merge rate. The test remains whether cycle 3
reaches cycle-1-style cache exhaustion and stays out of the 39–46 band.

### 23:30 — cycle 3 at 3h55m / 451 GB, healthy. Pretouch decline: real but modest.

| | 23:00 | 23:30 |
|---|---|---|
| merges | 322 | **385** |
| large (~31k) merges | 4 | 4 — **none new (2h30m)** |
| large median | 5,694 | 5,694 (unchanged) |
| small median | 9,887 | **10,107 b/min** |
| md0 rareq-sz | 43.9–45.6 KB | 35.4–36.9 KB |
| md0 %util | 19.7–20.5% | 20.0–21.0% |
| iowait | 0.8% | **0.0%** |
| CPU user | 34.4% | 53.5% |
| table live | 392 GB | **451 GB** |
| cache / free | 323 / 27 GB | 341 / 8 GB |
| pretouch | 14 / 136.9 s | **17 / 151.4 s** |

Device steady and quiet: ~36 KB reads, ~20% util, **iowait 0.0%**. 63 small
merges this interval, median up slightly to 10,107. Still no large merge in
2h30m; the result set is unchanged.

**Pretouch trend — settled with a split-half test rather than another
consecutive-pairs guess:**

| | median ord/s |
|---|---|
| first half of calls (n=8) | **1,398,580** |
| second half (n=9) | **754,863** |

That is a real ~45% decline, not sampling noise — the two halves barely
overlap. But the last three calls were 933k / 948k / 647k, i.e. bouncing
around the second-half median rather than continuing down, so it reads as a
**step down to a lower plateau** (cache is fuller than at 20:00) rather than
progressive degradation. Cumulative cost 151.4 s = **1.1% of wall clock**,
flat as a fraction across the last three checks.

Method note: split-half is the right test here. Three earlier "trends" this
session were called from consecutive extremes and two of them reversed; a
half-vs-half comparison of the whole sample would have avoided all three.

### Head-to-head

| table size | cycle 2 | cycle 3 |
|---|---|---|
| 385 GB | healthy (3h11m, last point) | healthy (3h25m) |
| 451 GB | never observed | **healthy (3h55m)** |

Cycle 3 is 66 GB beyond anything the empty-start comparison covers. Arms still
indistinguishable on merge rate; no settle; pretouch mechanism healthy.

### 00:00 (08-24) — cycle 3 at 4h25m / 516 GB, healthy. Pretouch decline has stopped.

| | 23:30 | 00:00 |
|---|---|---|
| merges | 385 | **476** |
| large (~31k) merges | 4 | 4 — **none new (3h)** |
| large median | 5,694 | 5,694 (unchanged) |
| small median | 10,107 | 10,107 b/min |
| md0 rareq-sz | 35.4–36.9 KB | 38.0–39.4 KB |
| md0 %util | 20.0–21.0% | 24.1–45.2% |
| iowait | 0.0% | **0.1%** |
| CPU user | 53.5% | 36.5% |
| table live | 451 GB | **516 GB** |
| cache / free | 341 / 8 GB | 336 / 11 GB |
| pretouch | 17 / 151.4 s | **19 / 160.7 s** |

91 small merges this interval, median flat at 10,107. Still no large merge in
3 hours. Device healthy — ~38 KB reads, iowait 0.1%. Client at phase 37/86.

**Pretouch decline has levelled off.** Last three calls: 647k / 840k / 881k
ord/s — rising, back near the second-half median. Split-half is unchanged from
last check (first 1,342,295 vs second 761,517), which is what a completed step
looks like: the gap is fixed by the early-vs-late boundary and no longer
widening. Reading this as confirmed: **a one-time step to a lower plateau
around 750k-880k ord/s, not progressive degradation.** Cumulative 160.7 s =
**1.0% of wall clock**, down as a fraction for the third check running.

Log rotated (`compaction.log.2026-08-23.158.zip`); the archive union is
handling it — merge count kept rising across the boundary rather than
resetting, which is the failure this method exists to prevent.

### Head-to-head

| table size | cycle 2 | cycle 3 |
|---|---|---|
| 385 GB | healthy (3h11m, last point) | healthy (3h25m) |
| 516 GB | never observed | **healthy (4h25m)** |

Cycle 3 is 131 GB past the empty-start comparison. Still four large merges
each and no separation. Cycle 1 collapsed at 9h25m from a populated start;
cycle 3 is at 4h25m from empty, so on wall clock it is not yet halfway to the
only collapse ever observed.

### 00:30 — cycle 3 at 4h55m / 574 GB, healthy. Still no large merge in 3h30m.

| | 00:00 | 00:30 |
|---|---|---|
| merges | 476 | **534** |
| large (~31k) merges | 4 | 4 — **none new (3h30m)** |
| large median | 5,694 | 5,694 (unchanged) |
| small median | 10,107 | **10,424 b/min** |
| md0 rareq-sz | 38.0–39.4 KB | 32.5–36.6 KB |
| md0 %util | 24.1–45.2% | 20.8–72.1% |
| iowait | 0.1% | **0.1%** |
| CPU user | 36.5% | 37.1% |
| table live | 516 GB | **574 GB** |
| cache / free | 336 / 11 GB | 328 / 18 GB |
| pretouch | 19 / 160.7 s | **21 / 176.2 s** |

58 small merges, median up slightly to 10,424. Device healthy, iowait 0.1%.
Client at phase 41/86, back in `load_increment_adaptive`.

**Pretouch: one slow call, not a resumed decline.** Last three were 881k /
711k / **398k** ord/s. The 398k is the lowest since the 20:38 episode, which is
the shape that fooled me at 21:00. Applying the rule adopted at 23:30: the
split-half is 1,327,539 vs 710,524 — **essentially unchanged** from the 00:00
check (1,342,295 vs 761,517), so the plateau has not moved. One low sample
against a stable split-half is a slow call, not a trend. Recording it; will
revisit only if the split-half itself moves.

Cumulative 176.2 s = **1.0% of wall clock**, unchanged.

### Head-to-head

| table size | cycle 2 | cycle 3 |
|---|---|---|
| 385 GB | healthy (3h11m, last point) | healthy (3h25m) |
| 574 GB | never observed | **healthy (4h55m)** |

189 GB past the empty-start comparison. Four large merges per arm, no
separation. Cycle 1 collapsed at 9h25m from a populated start; cycle 3 is at
4h55m from empty.

Worth noting what the absence of large merges means: the result set has not
grown since 21:05, so the arm comparison has had no new evidence for 3.5 hours
despite the run being healthy and productive. The decisive data only arrives
when STCS schedules another ~31k merge — and it will arrive under heavier
cache pressure than the first four did, which is precisely the condition of
interest.

### 01:00 — cycle 3 at 5h25m / 633 GB, healthy. 4h without a large merge.

| | 00:30 | 01:00 |
|---|---|---|
| merges | 534 | **588** |
| large (~31k) merges | 4 | 4 — **none new (4h)** |
| large median | 5,694 | 5,694 (unchanged) |
| small median | 10,424 | 10,424 b/min |
| md0 rareq-sz | 32.5–36.6 KB | 30.2–46.4 KB |
| md0 %util | 20.8–72.1% | 57.6–99.9% |
| iowait | 0.1% | **0.0%** |
| CPU user | 37.1% | 37.6% |
| table live | 574 GB | **633 GB** |
| cache / free | 328 / 18 GB | 335 / 10 GB |
| pretouch | 21 / 176.2 s | **24 / 194.2 s** |

54 small merges, median flat at 10,424. Device busier (%util up to 99.9%) but
**iowait 0.0%** and reads large (30–46 KB) — write-side pressure from ingest,
not the read-starved pattern.

Pretouch: 24 calls, none at 0 ms. Last three 678k / 735k / 585k — inside the
established plateau. Split-half 1,033,822 vs 722,770; the first-half figure
moved (was 1,327,539) purely because the midpoint shifted as n grew, which is
an artifact of the statistic, not of the data. The gap is stable at ~1.4x
across the last three checks. Cumulative 194.2 s = **1.0% of wall clock**.

### The experiment is stalled on evidence, not on health

**The result set has not grown since 21:05 — four hours.** Cycle 3 is healthy,
productive (588 merges, 98 of them substantive) and 248 GB past anything the
cycle-2 comparison covers, but every one of those merges is small. The arm
comparison still rests on four points per cycle:

| | n | median | range |
|---|---|---|---|
| cycle 2 | 4 | ~7,000 | 4,048–17,717 |
| cycle 3 | 4 | 5,694 | 3,997–14,606 |

No separation, and none obtainable until STCS schedules another ~31k merge.
That merge will land at 633 GB+ rather than the ~100–160 GB the first four saw,
so it carries far more information than they did — but its arrival is not
something the run can be pushed toward.

Cycle 1's collapse came at 9h25m from a populated start. Cycle 3 is at 5h25m
from empty, with iowait still at zero.

### 01:40 — cycle 3 at 6h05m / 669 GB, healthy. Still no large merge since 21:05.

| | 01:00 | 01:40 |
|---|---|---|
| merges | 588 | **636** |
| large (~31k) merges | 4 | 4 — **none new (4h35m)** |
| large median | 5,694 | 5,694 (unchanged) |
| small median (all cycle) | 10,424 | 9,138 — see note |
| md0 rareq-sz | 30.2–46.4 KB | 27.9–31.9 KB |
| md0 %util | 57.6–99.9% | **25.1–28.6%** |
| iowait | 0.0% | **0.0%** |
| CPU user | 37.6% | 38.7% |
| table live | 633 GB | **669 GB** |
| cache / free | 335 / 10 GB | 326 / 18 GB |
| pretouch | 24 / 194.2 s | **27 / 214.1 s** |

Device quieter than at 01:00 (util 25–29% vs up to 99.9%) — that burst was
ingest write pressure, and it passed. iowait still 0.0%.

**Two tooling bugs found and fixed this interval; both were silent.**

1. The pretouch regex required `warmed N,NNN ordinals in M ms`. The real line is
   `warmed 3966096 ordinals across 4 sources in 6431 ms` — no separators, and a
   clause in between. It matched nothing and printed **"PRETOUCH: 0 calls"**,
   which reads exactly like the pretouch reverting to no-op — the flag condition
   for this watch. It is not: 27 calls, none at 0 ms. Pattern now tolerates both
   forms.
2. The median helper used `x[n//2]`, which for even n takes the upper-middle
   rather than averaging the two. Prior checks averaged. Restored, so the
   4-sample large-merge figure stays comparable.

Also: the archive union was reading all 460 `.zip` files and timing out at 5 min.
Now filtered by mtime against the cycle-3 cutoff — 5 archives, same output.

**The small-merge median needs the same care.** The cycle-wide figure fell
10,424 -> 9,138, but that statistic covers all six hours and dilutes as the
table grows. Windowed, over the same data:

| window | n | median |
|---|---|---|
| 22:00–00:30 | 60 | 9,564 |
| since 00:30 | 30 | 9,098 |
| since 01:00 | 18 | 9,371 |

Flat within noise. The cycle-wide drop is the accumulating window moving, not
throughput degrading. (Old vs new statistic on identical data: 9,276 vs 9,138 —
the rest is real drift in the cumulative figure.)

Pretouch: same work each time (~3,966,08x ordinals, constant to 5 digits), but
elapsed has crept 5,846 -> 6,431/6,697/6,769 ms across the last four, so ~592k
ord/s where 01:00 saw 585–735k. A ~10% slowdown at fixed work, not a growth in
work. Cumulative 214.1 s = **1.0% of wall clock**, unchanged.

Position unchanged from 01:00: healthy, productive, 36 GB further on, and the
arm comparison still rests on four points per cycle, none added since 21:05.

### 02:00 — cycle 3 at 6h25m / 717 GB. Healthy. A byte compaction is running.

| | 01:40 | 02:00 |
|---|---|---|
| merges | 636 | **694** |
| large (~31k) merges | 4 | 4 — **none new (4h55m)** |
| large median | 5,694 | 5,694 (unchanged) |
| md0 rareq-sz | 27.9–31.9 KB | **10.6–11.4 KB** |
| md0 r/s | ~3,100 | **17.1k–18.7k** |
| md0 %util | 25.1–28.6% | **91.0–96.9%** |
| iowait | 0.0% | **1.0%** |
| CPU user | 38.7% | 17.9% |
| pending tasks | 1 | 3 |
| table live | 669 GB | **717 GB** |
| cache / free | 326 / 18 GB | 336 / 8 GB |
| pretouch | 27 / 214.1 s | **29 / 228.8 s** |

**The device signature is back and it is NOT the collapse.** 10.6 KB mean
request, 18k r/s, 96% util is the pattern that produced three false alarms in
cycle 1. The two conditions that define the collapse are both absent:

- iowait is **1.0%**, not ~50%
- no >=25k merge is running at all, let alone at 39–46 b/min

Merge throughput during exactly this window is the highest of the night:

| window | n | median |
|---|---|---|
| 22:00–00:00 | 50 | 9,564 |
| 00:00–01:00 | 22 | 8,960 |
| 01:00–01:40 | 18 | 9,371 |
| **01:40–02:00** | **6** | **9,591** |

Flat-to-up across four windows. Device is context; batch rate is the result, and
the result says healthy. Three pending tasks and 48 GB added in 20 minutes says
a large byte compaction — which is what this signature has meant every time.

**Pretouch elapsed is creeping at constant work** — this is the one thing worth
watching:

| time | ordinals | elapsed | ord/s |
|---|---|---|---|
| 01:34 | 3,966,071 | 6,431 ms | 616,711 |
| 01:46 | 3,966,125 | 6,791 ms | 584,027 |
| 01:58 | 3,966,113 | 7,866 ms | 504,210 |

Work is constant to five significant digits, so this is not the super-linear
growth that would argue for bounding `sourcePretouchMaxNodes`. The simplest
reading is contention: the slowest sample lands inside the byte compaction that
has the device at 96%. That is a hypothesis fitted to one interval, not a
finding — if elapsed stays high after the device quiets, it becomes real.
Cumulative 228.8 s = **1.0% of wall clock**, unchanged for four checks.

Position unchanged: the arms are still four points each, none added since 21:05.

### 02:30 — cycle 3 at 6h55m / 772 GB. Healthy. The 02:00 pretouch creep was contention.

| | 02:00 | 02:30 |
|---|---|---|
| merges | 694 | **762** |
| large (~31k) merges | 4 | 4 — **none new (5h25m)** |
| large median | 5,694 | 5,694 (unchanged) |
| md0 rareq-sz | 10.6–11.4 KB | **28.7–30.6 KB** |
| md0 r/s | 17.1k–18.7k | **3.2k–3.7k** |
| md0 %util | 91.0–96.9% | **25.7–33.7%** |
| iowait | 1.0% | **0.0%** |
| CPU user | 17.9% | 30.2% |
| pending tasks | 3 | 1 |
| table live | 717 GB | **772 GB** |
| cache / free | 336 / 8 GB | 327 / 17 GB |
| pretouch | 29 / 228.8 s | **31 / 241.0 s** |

The byte compaction finished. Device back to 29 KB requests at ~30% util,
pending back to 1, and 55 GB landed in the half hour. As logged at 02:00, that
signature was context, not a collapse — confirmed by its passing with merge
throughput never dipping.

**The pretouch creep reversed, exactly as the contention reading predicted.**

| time | ordinals | elapsed | ord/s | device then |
|---|---|---|---|---|
| 01:34 | 3,966,071 | 6,431 ms | 616,711 | quiet |
| 01:46 | 3,966,125 | 6,791 ms | 584,027 | ramping |
| 01:58 | 3,966,113 | **7,866 ms** | 504,210 | **96% util** |
| 02:10 | 3,966,101 | 6,548 ms | 605,697 | draining |
| 02:22 | 3,966,115 | **5,663 ms** | **700,356** | **quiet** |

Elapsed tracked device load up and back down at work held constant to five
digits, and 5,663 ms is the fastest sample of the night. So the 01:34–01:58
"creep" was not a trend — it was the byte compaction, and the right call at
02:00 was to log it as a hypothesis rather than a finding. This is the fourth
time this watch that consecutive extremes looked like a trend and were not.

**It also settles the `sourcePretouchMaxNodes=-1` question for this workload.**
The concern was pretouch cost growing super-linearly with source size, which
would argue for a cap. Six hours in, at 772 GB, the per-call work is *flat* —
3,966,0xx ordinals every time — because `sourcePretouchWindowNodes=1048576`
already bounds it. The window is doing the job the cap would do. Cumulative
241.0 s = **1.0% of wall clock**, unchanged for five checks.

Windowed small-merge medians, unchanged in character:

| window | n | median |
|---|---|---|
| 22:00–00:00 | 50 | 9,564 |
| 00:00–01:00 | 22 | 8,960 |
| 01:00–01:40 | 18 | 9,371 |
| 01:40–02:00 | 6 | 9,591 |
| 02:00–02:30 | 14 | 8,464 |

8.5k–9.6k across five windows spanning 4.5 hours. Flat.

Cycle 3 is now past cycle 2's entire lifetime (3h46m / 385 GB) by 3h and 387 GB,
and past cycle 1's collapse point in bytes though not in hours. Still four
points per arm.

### 03:00 — cycle 3 at 7h25m / 833 GB. Healthy, but a like-for-like series moved.

| | 02:30 | 03:00 |
|---|---|---|
| merges | 762 | **820** |
| large (~31k) merges | 4 | 4 — **none new (5h55m)** |
| large median | 5,694 | 5,694 (unchanged) |
| md0 rareq-sz | 28.7–30.6 KB | 46.4–50.0 KB |
| md0 w/s | 1.1k–2.7k | **2.4k–9.5k** |
| md0 %util | 25.7–33.7% | **34.5–98.1%** |
| iowait | 0.0% | **0.3%** |
| pending tasks | 1 | 2 |
| table live | 772 GB | **833 GB** |
| cache / free | 327 / 17 GB | 336 / 7 GB |
| pretouch | 31 / 241.0 s | **33 / 256.0 s** |

#### A better statistic: the 7,747-batch class

"All small merges" mixes 119-, 120-, 160-, 972- and 7,747-batch merges whose
rates are not comparable, and the mix shifts hour to hour. The 7,747 class now
dominates — 56 of the 58 merges this interval — so track it alone: same batch
count, completed merges only (>=95%), like-for-like.

| window | n | median | median span |
|---|---|---|---|
| 21:00–23:00 | 30 | 10,087 | 46s |
| 23:00–01:00 | 36 | 10,204 | 46s |
| 01:00–02:00 | 18 | 11,862 | 40s |
| 02:00–02:30 | 11 | 12,205 | 38s |
| **02:30–03:00** | **8** | **4,661** | **102s** |

**5.5 hours flat-to-rising, then one window 2.6x down.** This is the first
non-flat movement in a like-for-like series all night, and it is the reason to
introduce the statistic — the all-small median showed the same window as
8,464 -> 6,757, a much weaker version of the same thing, because the mix moved
underneath it.

**It is not the collapse.** 4,661 b/min is 100x above the 39–46 band; iowait is
0.3%, not ~50%; and no >=25k merge is running. A 2.6x slowdown and a 150–400x
collapse are different phenomena.

**And it is not yet a trend.** n=8, one window, and this watch has now produced
four trend calls that reversed on the next check. The obvious candidate is the
same contention seen at 02:00 — device at 98% util, writes up to 9,502/s. But
that reading does not transfer cleanly: at 02:00 the device was *also* pegged
(96% util) and this class ran at its **fastest** of the night. Busy device
therefore does not by itself predict this. Either the write-heavy shape matters
where the read-heavy one did not, or something else is starting.

Pretouch tracks the device the same way it did at 02:00 — 5,663 -> 6,996 ->
7,960 ms at work constant to five digits (3,966,0xx ordinals). Same up-swing as
01:58, which reverted. Cumulative 256.0 s = **1.0% of wall clock**, unchanged
for six checks.

**Next check is the one that matters**: if the 7,747 class returns to ~10–12k
b/min it was contention, matching the 01:58 pretouch reversal. If it stays near
4,661 or falls further, this is the first evidence of degradation in cycle 3 and
the run is heading somewhere at 833 GB that cycle 2 never reached.

### 03:30 — cycle 3 at 7h55m / 881 GB. **The 7,747 class did NOT recover. Page cache is full.**

| | 03:00 | 03:30 |
|---|---|---|
| merges | 820 | **877** |
| large (~31k) merges | 4 | 4 — **none new (6h25m)** |
| large median | 5,694 | 5,694 (unchanged) |
| **7,747 class** | **4,661 b/min** | **3,417 b/min** |
| 7,747 median span | 102s | **138s** |
| md0 rareq-sz | 46.4–50.0 KB | 45.1–49.9 KB |
| md0 %util | 34.5–98.1% | 30.0–90.5% |
| iowait (3x) | 0.3% | **0.0 / 0.1 / 1.3%** |
| pending tasks | 2 | 3 |
| table live | 833 GB | **881 GB** |
| cache / **free** | 336 / 7 GB | 341 / **2 GB** |
| pretouch | 33 / 256.0 s | **36 / 282.7 s** |

#### The 03:00 question is answered: it was not contention

| window | n | median | median span |
|---|---|---|---|
| 21:00–23:00 | 30 | 10,087 | 46s |
| 23:00–01:00 | 36 | 10,204 | 46s |
| 01:00–02:00 | 18 | 11,862 | 40s |
| 02:00–02:30 | 11 | 12,205 | 38s |
| 02:30–03:00 | 8 | 4,661 | 102s |
| **03:00–03:30** | **8** | **3,417** | **138s** |

Two consecutive windows, monotone down, same batch count, completed merges only.
That is the standard set at 03:00 for calling this real, and it is met: **3.5x
off a 5.5-hour baseline**, span 38s -> 138s. This is the first genuine
degradation in cycle 3, and unlike the four earlier trend calls in this watch it
survived the next check rather than reversing.

#### The mechanism is page-cache exhaustion, not the read-starvation collapse

`free` has gone 17 GB (02:30) -> 7 GB (03:00) -> **2 GB**, with cache pinned at
341 G against an 881 GB table. This is the bistability regime recorded on
2026-08-22: merges inflate once the working set stops fitting. It is the
expected consequence of running cycle 3 far past any prior cycle's size, not a
new fault.

`nodetool compactionstats` shows two shapes that also appeared in the cycle-1
collapse capture:

- a 6,222,722,140-byte merge **parked at 100.00%** (completed == total)
- the **Secondary index build at 0.13%** — 5,120 of 3,966,069 token range parts

The index-build total is exactly the pretouch ordinal count (3,966,069), which
confirms the pretouch is warming the source for that build.

**It is still not the collapse**, and the distinction matters:

| | collapse (cycle 1) | now |
|---|---|---|
| merge rate | 39–44 b/min | **3,417** (78x higher) |
| iowait | 50.1% sustained | **0.0 / 0.1 / 1.3%** |
| mean request | 4.02 KB | **45–50 KB** |
| >=25k merge running | yes | no |

Read-starvation is absent: requests are large and iowait is at the floor. What
is happening is byte-compaction pressure plus a working set that no longer fits.

#### Pretouch took its two worst samples of the night, then recovered

| time | ordinals | elapsed | ord/s |
|---|---|---|---|
| 03:01 | 3,966,118 | **9,822 ms** | 403,799 |
| 03:15 | 3,966,108 | **9,933 ms** | 399,286 |
| 03:30 | 3,966,069 | 6,952 ms | 570,493 |

Work constant to five digits throughout; 2.4x worse than the night's best
(5,663 ms) at the trough, recovering within the same interval. Cumulative
282.7 s = **1.0% of wall clock**, unchanged for seven checks — the pretouch is
not what is costing anything here.

#### What to watch next

The A/B still has four points per arm and none since 21:05. But cycle 3 is now
in territory cycle 2 never saw (881 GB vs 385 GB), with free memory at 2 GB.
If a ~31k merge is scheduled from here it lands under real memory pressure —
which is the condition under which cycle 1 collapsed, and therefore the most
informative point this experiment could produce.

### 04:00 — cycle 3 at 8h25m / 930 GB. **Third consecutive down window. Ingest unaffected.**

| | 03:30 | 04:00 |
|---|---|---|
| merges | 877 | **924** |
| large (~31k) merges | 4 | 4 — **none new (6h55m)** |
| **7,747 class** | **3,417 b/min** | **2,279 b/min** |
| 7,747 median span | 138s | **204s** |
| md0 rareq-sz | 45.1–49.9 KB | 43.5–65.0 KB |
| md0 r/s | 4.0k–6.9k | **4.0k–30.6k** |
| md0 %util | 30.0–90.5% | 35.1–82.6% |
| iowait (3x) | 0.0 / 0.1 / 1.3% | **1.0 / 1.3 / 0.0%** |
| pending tasks | 3 | 3 |
| table live | 881 GB | **930 GB** |
| cache / free | 341 / 2 GB | 335 / **8 GB** |
| pretouch | 36 / 282.7 s | **37 / 291.1 s** |

#### The series is now monotone across three windows

| window | n | median | span | ratio |
|---|---|---|---|---|
| 02:00–02:30 | 11 | 12,205 | 38s | — |
| 02:30–03:00 | 8 | 4,661 | 102s | 0.38 |
| 03:00–03:30 | 8 | 3,417 | 138s | 0.73 |
| **03:30–04:00** | **8** | **2,279** | **204s** | **0.67** |

5.4x off the 5.5-hour baseline. Span has gone 38s -> 204s. At the recent
per-window ratio (~0.7) the collapse band would be several hours away, but that
extrapolation deserves no weight: this same series sat flat for 5.5 hours before
it moved, so it is not inherently monotone.

#### Ingest is not affected

Table growth: 55 GB (02:30) -> 48 GB (03:00) -> 49 GB (03:30) -> **49 GB**
this interval. Client phase advanced 39% -> 48% -> 55%, error rate 0. **The
vector-merge slowdown has not propagated to ingest throughput.** That bounds
what this costs so far: it is a compaction-side effect, not a client-visible one.

#### Correction to the 03:30 entry

I flagged a merge "parked at 100.00%" there as a cycle-1 collapse shape. It is
not. This check shows a different merge id at the same 100.00% and nearly the
same byte count (6,222,686,084 vs 6,222,722,140) — these rotate through
normally, so 100.00% is just the terminal display state, not a stall. The
cycle-1 significance came from that merge staying parked while nothing else
completed, which is not what is happening here.

Same for the index build: the 0.13% build from 03:30 is gone, replaced by a new
id at 0.00%. Builds are completing and recycling, unlike cycle 1 where one build
sat at 0.26% for hours. Withdraw both as collapse indicators.

#### Memory pressure eased, and the slowdown continued anyway

`free` went 2 GB -> **8 GB** while the 7,747 class got *worse*. That weakens the
page-cache-exhaustion mechanism proposed at 03:30 as a complete explanation — it
may still be a contributor at the 881 GB threshold, but free memory recovering
without the rate recovering means something else is carrying the trend.

Collapse conditions remain absent: iowait 0.0–1.3%, requests 43–65 KB (larger
than ever, not the 4 KB signature), no >=25k merge running, 2,279 b/min still
50x above the band.

Pretouch: 8,409 ms at 03:45 for the same 3,966,108 ordinals — within the
9,933/6,952 spread of the last interval, no new information. Cumulative 291.1 s
= **1.0% of wall clock**, unchanged for eight checks.

### 04:30 — cycle 3 at 8h55m / 978 GB. **The degradation fully reversed. Mechanism identified.**

| | 04:00 | 04:30 |
|---|---|---|
| merges | 924 | **985** |
| large (~31k) merges | 4 | 4 — **none new (7h25m)** |
| **7,747 class** | **2,279 b/min** | **12,277 b/min** |
| 7,747 median span | 204s | **40s** |
| md0 rareq-sz | 43.5–65.0 KB | 20.7–27.5 KB |
| md0 %util | 35.1–82.6% | 29.3–98.5% |
| iowait (3x) | 1.0 / 1.3 / 0.0% | 1.1 / 1.1 / 0.0% |
| pending tasks | 3 | **5** |
| table live | 930 GB | **978 GB** |
| cache / free | 335 / 8 GB | 339 / 4 GB |
| pretouch | 37 / 291.1 s | **39 / 305.8 s** |

#### Correction: the three-window decline was episodic, not progressive

| window | median | span |
|---|---|---|
| 02:00–02:30 | 12,205 | 38s |
| 02:30–03:00 | 4,661 | 102s |
| 03:00–03:30 | 3,417 | 138s |
| 03:30–04:00 | 2,279 | 204s |
| **04:00–04:30** | **12,277** | **40s** |

Back to baseline in one window — 12,277 vs the 12,205 it started from, span 204s
-> 40s. **I called this a genuine degradation at 03:30 and reaffirmed it at
04:00; both calls were wrong.** The two-window standard I set at 03:00 to guard
against single-window noise was not strict enough: three consecutive monotone
windows in a like-for-like series still reversed completely.

#### The mechanism, and it was contention after all

The 199,140,724,110-byte compaction that read 18.93% at 04:00 now reads
**100.00%**. Tracking it back: 192.9 GB at 44.79% (03:30), 199.1 GB at 18.93%
(04:00) — a run of ~193–199 GB compactions was in flight continuously from
roughly 02:30 to 04:15, which is exactly the span of the decline, and the class
recovered in the window one completed.

So the 02:00 contention reading was right; it was rejected at 03:00 for the
wrong reason. The objection then was that the device had been equally pegged at
02:00 while the class ran its fastest. That compared the wrong things — a
30-second iostat window against a 1.5-hour compaction. The unit of contention
here is the compaction's lifetime, not the instantaneous device state.

**Standing rule going forward: read this series against `compactionstats`
lifetimes, not against iostat windows.** A window where a >=100 GB compaction is
in flight is not comparable to one where none is.

#### New and worth watching: the 126.9M-part index build has started

```
9daba2d0  Secondary index build   0 / 126,916,949 token range parts   0.00%
```

126,916,949 parts is the same build that in the cycle-1 collapse capture sat at
0.26% advancing at ~91 parts/s — a 15-day implied ETA. It has just started here
at 0. This is the first time cycle 3 has reached it. Its rate over the next
checks is a direct comparison against cycle 1 and is worth recording each time.

(The other build at 11.25% of 3,966,070 is the small recurring kind that has
been recycling all night — not this one.)

Also new: a 24,898,876,795-byte compaction at 0.00%, and pending up to 5.

Collapse conditions still absent: iowait 0.0–1.1%, requests 20–27 KB, no >=25k
merge running. Pretouch 6,426–8,409 ms at constant work, cumulative 305.8 s =
**1.0% of wall clock**, unchanged for nine checks.

---

## 05:10 — **THE COLLAPSE REPRODUCED. The ReaderSupplier fix did not prevent it.**

A 30,985-batch merge started at 04:59:08 and is running at **74 b/min**. Full
capture in `docs/captures/collapse-cycle3-20260824-0510.md` (raw files under
`sessions/collapse-cycle3-20260824-0510/`).

| | cycle 1 (prefetch inert) | cycle 3 (prefetch real) |
|---|---|---|
| time to collapse | 9h 25m, populated start | **9h 31m, empty start** |
| merge | 30,985 batches | **30,985 batches** |
| rate | 39–44 b/min | **74 b/min** |
| iowait | 50.1% | **40.3 / 40.3 / 41.1 / 40.3%** |
| mean request | 4.02 KB | **6.31–6.38 KB** |
| md0 r/s | 113k–177k | **234k–264k** |
| %util | 99.0–99.4% | 93.8–98.3% |
| implied merge time | ~13 h | **~7.0 h** |

All four flag conditions are met simultaneously for the first time in this
watch: a >=25k merge far below band, iowait ~41% sustained across four samples,
the small-request signature, and ~98% util. Rate is dead flat — 74 b/min over
the full 9.5 minutes and 74 b/min over the last 10.

**The fix bought 1.7–1.9x and did not change the outcome.** Rate 39 -> 74 b/min,
request 4.02 -> 6.31 KB, implied merge time 13 -> 7 hours. Against a healthy
reference of 5,387–27,530 b/min for this size, 74 is still 73–370x off.

### The hot path is unchanged, and it is the path the hints do not cover

90 RUNNABLE threads: 74 in `MemorySegmentVectorProvider`, 67 in
`FrontierPrefetchingView.processNeighbors`, 62 in `FusedPQ$PackedNeighbors.readInto`,
**40 in `clusterSearchL0`**, 31 blocked in `readFully`.

This is the mechanical prediction from the cycle-1 capture, confirmed: the
cross-source seed prefetch covers only the *seeded* branch of
`gatherFromOtherSource`, while these threads are in the `clusterMode` branch it
never touches. Making the `ReaderSupplier` hooks real made the existing hints
work; it did not put hints on the starving path. Meanwhile 67 threads sit inside
`FrontierPrefetchingView` and block in `readFully` anyway, with `WIDTH` at its
compiled default of 3.

### Flags actually in effect

Only `sourcePretouchMaxNodes=-1` and `sourcePretouchWindowNodes=1048576` are on
the command line. `crossSourceSeedPrefetch`, `frontierPrefetch`,
`batchPrefetchDensity` and `adviseRandom` are at compiled defaults (ON, 3, 8,
true) — nothing in this run moved `frontierPrefetch` off the value the evidence
indicts.

### Pretouch: settled, and not the issue

39 calls, none at 0 ms, cumulative 305.8 s = **1.0% of wall clock**, unchanged
across nine checks. Per-call work stayed flat at ~3.966M ordinals all night
because the window bounds it, so `sourcePretouchMaxNodes=-1` is fine as set. It
works, it is cheap, and it does not address this failure.

### Next

1. Sweep `jvector.compaction.frontierPrefetch` 3 -> 16/32. The one indicted knob
   still at default, now genuinely wired to the device, and no code change.
2. Cover `clusterSearchL0` with hints. Two independent captures point here.
3. Stop spending on the pretouch.

### State at capture

table 1,008 GB, cache 339 G / free 3 GB, pending 6, settle 0, client phase 45/86
at 63% with zero errors — **ingest is still unaffected**, as it was through the
02:30–04:00 episode.

### 05:30 — collapse established and stable. **Correction: the pretouch is NOT exonerated.**

| | 05:10 | 05:30 |
|---|---|---|
| **30,985 merge** | 74 b/min @ 2.29% | **72 b/min @ 7.39%** |
| implied total | 7.0 h | 7.1 h |
| iowait (4x) | 40.3–41.1% | **41.0 / 42.7 / 43.5 / 44.0%** |
| md0 rareq-sz | 6.31–6.38 KB | 6.33–6.43 KB |
| md0 r/s | 234k–264k | 220k–230k |
| md0 **w/s** | 66–1,049 | **44–48** |
| %util | 93.8–98.3% | 97.9–98.3% |
| pending tasks | 6 | **7** |
| table live | 1,008 GB | **1,027 GB** |
| cache / free | 339 / 3 GB | 339 / 3 GB |

Rate is flat to three decimal places of the story: 72 b/min overall, 72 over the
last 30 min, 71 over the last 10. Stable bad equilibrium, confirmed over 31.5
minutes rather than 9.5. Three compactions are now parked at 0.00% and write
throughput has gone to essentially zero (w/s 44–48, from thousands) — all device
capacity is graph-search reads.

**Ingest is now affected**, for the first time: table grew 19 GB this interval
against 48–49 GB in each of the four before it.

#### Correction to the 05:10 entry: the pretouch cost 6.8 minutes and was not repaid

At 05:10 I wrote that the pretouch was "settled and exonerated" at 1.0% of wall
clock with per-call work "flat at ~3.966M ordinals all night". **That was wrong,
and wrong in a way I should have caught**: the figure came from the 04:30 run of
the check script, and the script only ever prints the last three samples, so two
large outliers were in the log the whole time and never displayed.

| | n | cumulative |
|---|---|---|
| calls <=10M ordinals | 39 | 239.9 s |
| **calls >10M ordinals** | **2** | **484.0 s** |
| total | 41 | **724.0 s** |

```
2026-08-23 20:38:51   16,013,399 ord    73,086 ms = 1.2 min  -> 219,104 ord/s
2026-08-24 04:38:28  126,916,949 ord   410,926 ms = 6.8 min  -> 308,856 ord/s
```

Two calls are **67% of all pretouch time**. Cumulative is 2.0% of wall clock,
not 1.0%.

The 04:38 call warmed 126,916,949 ordinals across **32 sources** in 6.8 minutes.
The 30,985-batch merge began 21 minutes later and collapsed to 72 b/min. The two
cannot be tied together with certainty from the logs — the gap is consistent
with PQ retrain running between them, but not proof — yet on either reading the
conclusion is the same: **6.8 minutes of streaming warm-up bought nothing.**
With free at 3 GB against a 1,027 GB table, whatever was warmed was evicted long
before the merge's random reads arrived.

This is exactly the condition the watch brief named: *"a source with hundreds of
millions of ordinals means MINUTES per merge, and if that is not repaid then
`sourcePretouchMaxNodes` should bound it instead of -1."* It happened, it was
not repaid, so **`sourcePretouchMaxNodes` should be bounded.** The window
(`sourcePretouchWindowNodes=1048576`) bounds work per source, not the total
across 32 of them, which is why the earlier flat readings were misleading.

Note the scaling is not explosive: 570k ord/s at 4M vs 309k ord/s at 127M, so
roughly linear in total bytes with a 1.8x rate penalty at scale. The problem is
not super-linearity, it is that the absolute cost is real and the benefit is
zero above RAM.

#### The 126.9M index build matches cycle 1

0 (04:30) -> 80,000 (05:06) -> **305,024 / 126,916,949 = 0.24%** — about 150
parts/s, implying **~9.8 days**. Cycle 1's capture had the same build at 0.26%
and ~91 parts/s implying ~15 days. Same pathology, same order of magnitude.

Collapse conditions all remain met. No change to the 05:10 conclusions about
`frontierPrefetch` or `clusterSearchL0`; this interval adds a third
recommendation ahead of them in cost: cap the pretouch.

### 06:00 — merge decaying toward the band; ingest now choking; one index build parked at exactly 50%.

| | 05:30 | 06:00 |
|---|---|---|
| **30,985 merge (overall)** | 72 b/min @ 7.39% | **69 b/min @ 13.75%** |
| **last 10 min** | 71 b/min | **66 b/min** |
| implied total | 7.1 h | **7.5 h** |
| iowait (4x) | 41.0–44.0% | **42.4 / 43.9 / 45.0 / 44.7%** |
| md0 rareq-sz | 6.33–6.43 KB | 6.39–6.48 KB |
| md0 r/s | 220k–230k | 225k–230k |
| md0 w/s | 44–48 | 44–823 |
| pending tasks | 7 | 7 |
| table live | 1,027 GB | **1,039 GB** |
| cache / free | 339 / 3 GB | 339 / 3 GB |
| pretouch | 41 / 724.0 s | 41 / 724.0 s (no new calls) |

The merge is **slowly decaying, not holding**: 74 -> 72 -> 69 b/min overall, and
the trailing 10-minute figure is 66. It is drifting toward the 39–46 band rather
than sitting above it. iowait has risen with it, 40.3% -> 41.0% -> 45.0% at the
top of each check's samples.

#### Ingest is choking progressively

| interval | GB added | per 30 min |
|---|---|---|
| 03:00–03:30 | 48 | 48 |
| 03:30–04:00 | 49 | 49 |
| 04:00–04:30 | 48 | 48 |
| 04:30–05:10 | 30 | 22 |
| 05:10–05:30 | 19 | 29 |
| **05:30–06:00** | **12** | **12** |

Client phase 63% -> 71% -> 73%, still zero errors, `settle awaiting` still 0.
The workload has not entered `settle_compactions`; it is simply being throttled
by the servo as the node falls behind. Ingest was unaffected right up to the
collapse and has fallen 4x in the 90 minutes since.

#### One index build IS parked — this one is real

The small recurring build has read **exactly 1,983,029 / 3,966,070 = 50.00%**
at 05:06, 05:30 and 06:00 — three consecutive checks, byte-identical count. That
is a genuine stall, and it is worth flagging precisely because at 04:00 I
withdrew "parked at 100%" and "index build at 0%" as collapse indicators after
watching them rotate. Those rotate; this one has stopped. The distinction is
whether the counter *moves* between checks, not what value it shows.

Likewise the three compactions at 0.00% have held identical byte counts
(1637 / 1637 / 1638) across three checks — also genuinely parked, all behind the
one merge.

The 126.9M build does advance: 80,000 -> 305,024 -> **551,168 (0.43%)**, about
137 parts/s, implying **~10.7 days**. Cycle 1's equivalent was ~91 parts/s and
~15 days.

#### Pretouch

No new calls since 04:38 — the compaction executors are all inside the collapsed
merge. Still 41 calls, 724.0 s, none at 0 ms. The 05:30 correction stands: two
calls are 67% of that total and `sourcePretouchMaxNodes` should be bounded.

Collapse conditions remain met on every axis, now with the merge trending down
rather than flat.
