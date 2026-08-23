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
