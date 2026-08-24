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
