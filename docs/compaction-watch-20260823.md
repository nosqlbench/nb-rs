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
