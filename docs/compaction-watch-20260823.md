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
