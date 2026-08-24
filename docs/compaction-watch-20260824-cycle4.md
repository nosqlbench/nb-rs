# Cycle 4 watch — `frontierPrefetch=32`

Started **2026-08-24 06:54:50 UTC**, table wiped, empty start.

## The arm

Single variable against cycle 3. One line added to
`/mnt/nvme/opt/cassandra/conf/jvm-server.options` (backup:
`jvm-server.options.pre-frontier32`):

```
-Djvector.compaction.frontierPrefetch=32
```

Verified live on the JVM command line before starting the client, and verified
present in the deployed jar's bytecode (`FrontierPrefetchingView.class` contains
the property string) — the check that cycles 1 and 2 lacked, and lacking it cost
two full runs.

`FrontierPrefetchingView.WIDTH` defaults to 3 and clamps to `SHADOW_CAP = 32`, so
**32 is the maximum the class can act on** — the whole shadow queue. A hint
beyond it has no entry to name.

### Why this knob

Both collapse captures (2026-08-23 cycle 1, 2026-08-24 cycle 3) found
`FrontierPrefetchingView` in *every* blocked read stack — 67 of 90 RUNNABLE
threads in cycle 3 — with the threads still stalling in `readFully`. The hints
are being issued and are not covering the reads. The class's own javadoc says 3
was measured as the knee **on a cache-resident working set**, which is exactly
the regime that does not apply here.

Nothing else changed. `sourcePretouchMaxNodes` stays at `-1` despite the 05:30
finding that it cost 6.8 minutes unrepaid — capping it is a separate,
independently-motivated arm and folding it in would confound this one.

## Unchanged from cycle 3

| | |
|---|---|
| Cassandra | `experiment/sai-vector-reader-prefetch-20260823` @ 80064aa109 |
| jvector | `experiment/compaction-io-prefetch-20260823` @ 55a262a7, jar md5 f97b7ad3f072 |
| other jvector flags | sourcePretouchMaxNodes=-1, sourcePretouchWindowNodes=1048576 |
| compiled defaults | crossSourceSeedPrefetch ON, batchPrefetchDensity 8, adviseRandom true |
| client | `./run_200m`, log `run_200m_20260824_cycle4.log` |
| session | `sessions/stcs_adaptive_20260824_065450` |

## Metric change: cells/s is now PRIMARY

Cycle 3 established that **batch counts are not comparable across merges**
(see `docs/settle-time-characterization-20260824.md`). This cycle tracks
`SSTableIndexWriter` segment builds instead:

```
Flushed segment with 3966046 cells for a total of 18.114GiB in 299040 ms
```

Fixed unit, real denominator, and directly interpretable as settle work.
Check script: `docs/captures/c4check.py`.

## Targets to beat

| metric | cycle 3 |
|---|---|
| standard 3.966M-cell segment | **median 277 s = 14,318 cells/s, 67 MiB/s** |
| same, during a large merge | 762–828 s = 4,792–6,229 cells/s |
| large-merge collapse | 30,985 batches at 60–74 b/min, ~7 h implied |
| iowait during collapse | 40–45% sustained |
| md0 mean request size | 6.3–6.5 KB (healthy: 20–65 KB) |
| ingest during collapse | 12 GB/30 min, down 4x from 48–49 |
| **time to first collapse** | **9h 31m from empty, at ~1,000 GB** |

**The arm succeeds** if a 30,985-class merge stays out of the 60–75 b/min regime
and mean request size stays well above 6.5 KB — or, more cleanly, if segment
cells/s does not fall to the 4,792–6,229 trough.

**The arm fails informatively** if it collapses on the same schedule; that would
retire hint depth as the lever and leave `clusterSearchL0` coverage, which is a
code change, as the remaining candidate.

## Log

| when | note |
|---|---|
| 06:54:50 | cycle 4 started. Node clean (98 KiB load), cache 259 G / free 108 G. Flag confirmed on the JVM command line. |
| 07:12 | **17 min in — warm-up, nothing to measure yet.** Table 6.2 GB / 1 SSTable, pending 0, device idle (r/s 0, 55 w/s, 0.2% util), iowait 0.0%, cache 279 G / free 80 G, settle 0. No segment builds yet — cycle 3's first came 36 min after start, so this is on schedule. Phase 5/86 `compaction_watch`, which is exactly where cycle 3 sat at the same elapsed time (216 vs 239 status lines in the same phase over the first 20 min). Startup is tracking the baseline. |
| 07:43 | **48 min in — first two segments landed, and they are 18.6% SLOWER than cycle 3's.** Table 85.6 GB / 5 SSTables, pending 2, cache 354 G / free 2 GB, iowait 0.0%, settle 0, phase 9/86. Pretouch 3 calls, 8.2 s cumulative, none >10M ordinals. See the analysis below — the slowdown is expected at this stage and is not yet evidence against the arm. |

### 07:43 analysis — cycle 4 vs cycle 3 at equal segment ordinal

Both cycles start from an empty table, so the *N*th segment of each faces a
comparable table size and SSTable count. That is a far more honest early
comparison than cycle 3's whole-run median (14,318 cells/s), which is dominated
by its healthy mid-run hours.

| # | cycle 3 (`frontier=3`) | | | | cycle 4 (`frontier=32`) | | | | delta |
|---|---|---|---|---|---|---|---|---|---|
| | cells | s | cells/s | t+min | cells | s | cells/s | t+min | |
| 1 | 4,084,155 | 265 | 15,397 | 36 | 4,054,781 | 325 | **12,461** | 34 | **−19.1%** |
| 2 | 3,995,565 | 309 | 12,944 | 44 | 4,025,041 | 380 | **10,600** | 43 | **−18.1%** |

Mean of the first two: 14,171 -> **11,531 cells/s, −18.6%**. The pairs are well
matched — cell counts within 1.5%, and each lands within 2 minutes of its
cycle-3 counterpart's position in the run.

**This is expected here, and it is not yet evidence against the arm.** The
device is essentially idle:

| | |
|---|---|
| md0 | r/s 64–120, **%util 1.3–3.0** |
| iowait (4x) | **0.0 / 0.0 / 0.0 / 0.0%** |
| CPU | **71.8/83.6/47.6/51.8% us + 13–38% sy — 3.5–14.6% idle** |
| table | 85.6 GB against 354 G of page cache |

**At 85 GB the working set is entirely cache-resident and these builds are
CPU-bound, not IO-bound.** That is precisely the regime in which
`FrontierPrefetchingView`'s javadoc says WIDTH=3 was measured as the knee:
"deeper ranks are displaced before expansion and wider hinting buys waste rather
than overlap." Issuing ~10x the hints into a working set that never misses is
pure syscall and bookkeeping cost with nothing to hide. The elevated system CPU
(13–38% sy) is consistent with that, though not proof of it — ingest-driven page
cache management would also show there.

So the arm is doing exactly what theory predicts it should do *below* RAM. The
experiment is entirely about what happens *above* it, and the collapse-relevant
window is ~9 hours out at ~1,000 GB. A −18.6% penalty in the cache-resident
phase is the price of admission, and would only matter as a verdict if it
persisted with no compensating gain once the working set stops fitting.

**Caveat: n=2.** Recorded because it is a clean, well-matched pair and because
the mechanism is checkable, not because two points establish anything.

*Method note:* the first ad-hoc sanity check of the pretouch parser used
`awk '$2 >= "06:54:00"'`, but field 2 is the thread name (`[CompactionExecutor:5]`),
not the timestamp — so it matched every line and appeared to show the script
under-counting 25 calls as 3. Re-checked against the real timestamp field: 3
unique post-cutoff calls, script correct. The cycle-3 rule is to sanity-check the
parser; the corollary is to sanity-check the sanity check.
| 08:13 | **1h18m in — the −20% on segments is holding across five points, and the pretouch gives it a control.** Table 140 GB / 5 SSTables, pending 2, cache 350 G / free 5 GB, iowait 3.0–3.6%, settle 0, phase 13/86. Segments n=5, median **10,455 cells/s** (cycle 3 whole-run median 14,318). md0 4.69–4.70 KB / 157k–205k r/s / 98–100% util while iowait stays at 3%. The 30,996 merge from the monitor is at 65%, **2,563 b/min** vs cycle 3's byte-identical 4,188. |

### 08:13 analysis — the pretouch is a control variable, and it validates the −20%

Three independent work items now exist at matched size in both cycles. Crucially
they do **not** all touch the code the arm changes: `frontierPrefetch` affects
the graph-search path only. The pretouch is a streaming sequential read that
never consults `FrontierPrefetchingView`, so it should show **parity** — and if
it does, it rules out machine state, device health or table layout as the
explanation for any difference on the paths that *do* use it.

**Control — pretouch (should be unaffected):**

| | cycle 3 | cycle 4 | delta |
|---|---|---|---|
| the 16.0M-ordinal call | 16,013,399 ord, 73,086 ms, 219,104 ord/s | 16,016,033 ord, 67,368 ms, **237,739 ord/s** | work +0.016%, rate **+8.5%** |
| small (<=10M) calls, equal ordinal, n=5 | median 1,497,269 ord/s | median **1,449,042 ord/s** | **−3.2%** |

Work differs by 0.016% on the large call — as close to an identical unit of work
as this system produces. **Parity confirmed**: −3.2% on the small calls, +8.5% on
the large one, i.e. noise in both directions and no systematic difference.

**Treatment — segments (graph-search path, equal ordinal):**

| # | cycle 3 cells/s | cycle 4 cells/s | delta |
|---|---|---|---|
| 1 | 15,397 | 12,461 | −19.1% |
| 2 | 12,944 | 10,600 | −18.1% |
| 3 | 13,133 | 10,455 | −20.4% |
| 4 | 13,149 | 10,436 | −20.6% |
| median | 13,133 | **10,455** | **−20.4%** |

Four consecutive points within a 2.5-point spread (−18.1 to −20.6%), at cell
counts matched to within 1.5%. **With the control at parity, the −20% is
attributable to the arm rather than to the machine.** That is a materially
stronger claim than last check's n=2 could support.

**Segment 5 is NOT usable and is excluded above.** Cycle 3's fifth took 1,616 s
(2,455 cells/s — below its own collapse trough) while cycle 4's took 396 s
(10,010), which reads as +308% for the arm. It is not: cycle 3's slow segment sat
inside four back-to-back large merges (30,987 / 30,996 / 31,216 / 31,908 running
continuously 20:44–21:06), while cycle 4's finished at 08:03:15, two minutes
*before* its 30,996 merge began. Different contention, not different behaviour —
exactly the compare-against-lifetimes error that produced a false trend in
cycle 3.

**Secondary, and consistent:** the 30,996-batch merge — byte-identical batch
count to cycle 3's — is at 2,563 b/min against 4,188, **−39%**. Partial (65%) vs
cycle 3's completed run, so weight it accordingly, but it points the same way.

### Where this leaves the arm

Every path that uses the deeper hint is 20–39% slower; the one path that does not
is at parity. At 140 GB against 350 G of cache the working set still fits, which
is the regime the default of 3 was tuned for and where extra hints are waste.
**The cost of the arm is now established. The benefit, if any, cannot appear
until the working set stops fitting** — cycle 3 collapsed at ~1,000 GB, roughly
7 hours out.

Note the device already shows 4.70 KB mean request at 98–100% util — the
small-request signature — while iowait sits at 3.0–3.6% rather than cycle 3's
collapse-time 40–45%. Recorded, not interpreted: cycle 3 also ran cool here, and
the signature means nothing without the iowait.
