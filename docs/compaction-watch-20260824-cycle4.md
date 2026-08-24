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
