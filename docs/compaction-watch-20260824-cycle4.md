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
| 08:43 | **1h48m in — iowait is at collapse levels (36.6/41.1/43.2/41.2%) while merges run healthy and accelerating.** Table 182.6 GB / 9 SSTables, pending 5, cache 349 G / free 4 GB, settle 0, phase 17/86. md0 **4.14–4.18 KB at 214k–229k r/s, 98–99% util**. Segments still n=5 (no flush since 08:03; two index builds in progress at 79% and 25%). Large merges done: 2,110 / 2,612 / 3,175 b/min, with a fourth at 11,440 partial. |

### 08:43 analysis — iowait and throughput have decoupled

Cycle 3's collapse was defined by four things co-occurring. Cycle 4 now has
three of them and not the fourth:

| | cycle 3 collapse (1,008 GB) | cycle 4 now (182.6 GB) |
|---|---|---|
| md0 mean request | 6.31–6.38 KB | **4.14–4.18 KB** (smaller) |
| md0 r/s | 234k–264k | **214k–229k** (comparable) |
| %util | 93.8–98.3% | **98.2–99.0%** (comparable) |
| iowait | 40.3–41.1% | **36.6–43.2%** (comparable) |
| **large merge rate** | **60–74 b/min** | **2,110–11,440 b/min** |

**The device signature and the iowait are at collapse levels; the throughput is
43–190x better.** In cycle 3 those always moved together.

For scale against cycle 3's *healthy* hours, from its own watch log: it ran at
~2,700–4,200 r/s at 26–54 KB for most of the night, with brief small-request
episodes reaching 24–34k r/s (561 GB) and 69k–117k r/s (815 GB), and iowait
never above 1.3% until the collapse. **Cycle 4 is sustaining more IOPS than
cycle 3's collapse produced, at a fifth the table size.**

Two readings, and the data does not yet separate them:

1. **The hints are working.** `posix_fadvise(WILLNEED)` at depth 32 puts many
   reads in flight; the device saturates and iowait rises because there is
   always outstanding IO, but threads are not serialised behind demand faults —
   which is why merges stay fast. On this reading high iowait is the *intended*
   signature, not a warning.
2. **The hints are waste.** 10x the hints generate 10x the readahead, most of it
   displaced before use, saturating the device five times earlier than the
   default did. Benign now because the working set still fits in 349 G of cache;
   damaging once it does not.

**What separates them is whether merge throughput survives the transition above
RAM** — which is exactly the experiment, ~5–6 hours out. Recording the numbers
now so the comparison at 1,000 GB is against a measured baseline rather than a
recollection.

### The merge sequence is converging on cycle 3

| # | cycle 3 | cycle 4 | delta |
|---|---|---|---|
| 1 | 30,987 @t+69m, 3,912 b/min | 30,996 @t+71m, 2,110 | −46% |
| 2 | 30,996 @t+78m, 4,131 | 31,006 @t+85m, 2,612 | −37% |
| 3 | 31,216 @t+85m, 6,933 | 31,446 @t+98m, 3,175 | −54% |
| 4 | 31,908 @t+90m, 14,606 | 31,678 @t+109m, 11,440 (part) | −22% |

Both cycles accelerate through the early large merges; cycle 4 starts at roughly
half the rate and closes to −22% by the fourth. It is also running the sequence
slower in wall clock (t+71/85/98/109 vs t+69/78/85/90). Only the 30,996 pair is
size-matched; the rest are near-neighbours, so read the shape, not the deltas.

### Correction to the 08:13 control claim

Last check I called the pretouch control "parity" from a subset. The full series
at equal ordinal is noisier than that implied — it has large outliers in **both**
directions:

| # | cycle 3 ord/s | cycle 4 ord/s | delta |
|---|---|---|---|
| 1 | 1,519,403 | 1,476,076 | −2.9% |
| 2 | 1,547,541 | 1,557,678 | +0.7% |
| 3 | 1,497,269 | 1,386,699 | −7.4% |
| 4 | 1,454,864 | 1,449,042 | −0.4% |
| 5 | **349,099** | 1,442,789 | **+313.3%** |
| 6 | 219,104 | 237,739 | +8.5% |
| 7 | 687,137 | **289,411** | **−57.9%** |

Calls 5 and 7 are 4–5x off their own neighbours — contention events, one in each
cycle. The robust read is the five non-outlier pairs: **median −0.4%, range −7.4
to +8.5%.** The control still holds and the −20% on segments is still
attributable to the arm, but "parity" should have been stated with the outliers
shown, not from a subset that happened to exclude them.

**Not flagged as a collapse** despite iowait crossing the 25% threshold: merges
are at 2,110–11,440 b/min and accelerating, which is the opposite of the
collapse's defining symptom. Recorded prominently instead.
| 09:13 | **2h18m in — the arm's penalty SCALES WITH SEGMENT SIZE, and that predicts it fails at the collapse.** Table 231 GB / 9 SSTables, pending 3, cache 339 G / free 13 GB, settle 0, phase 21/86. Device recovered: **57.8–78.6 KB at 1.8k–3.2k r/s, 11–22% util, iowait 0.0–2.9%**. Segments n=8, median 10,446 cells/s; one at **2,235 cells/s** and the 16.0M-cell at **4,967** — both in or below the collapse trough. Merges: 2,110 / 2,612 / 3,279 / **12,892** b/min. |

### 09:13 analysis — the penalty grows with the size of the work

Both cycles have now run a 16.0M-cell segment, and the work matches to
**+0.016%** — the closest-matched pair this campaign has produced.

| class | cycle 3 | cycle 4 | delta |
|---|---|---|---|
| 3.97M cells | 13,133 cells/s | 11,141 | **−15.2%** |
| **16.0M cells** | **8,242 cells/s** | **4,967** | **−39.7%** |

**Quadrupling the work more than doubles the penalty.** As a retained fraction:
0.848 at 3.97M, 0.603 at 16.0M, over a 4.04x size step — an implied
`retained ~ size^-0.245`.

Extrapolated to the 126.9M-cell segment on which cycle 3 collapsed (32x the
standard class), that gives a retained fraction of ~0.36, i.e. **~−64%**. Two
points do not establish a power law and this is an order-of-magnitude statement
only — but the direction is unambiguous and it is the direction that matters:
**the arm is worst exactly where the problem is.**

Note the mechanism is coherent with the cost model. A deeper hint helps only if
the hinted record is still resident when the search reaches it. The larger the
segment, the longer the interval between hint and use, and the more likely a
depth-32 hint is evicted before it pays — so waste should rise with size, which
is what the numbers show. This is the javadoc's own argument for WIDTH=3, holding
at larger scale rather than inverting as the above-RAM hypothesis predicted.

### Correction to the 08:43 "decoupling"

Last check I reported iowait at collapse levels (36–43%) while "merges run
healthy and accelerating", and offered two competing readings. That framing was
wrong in an avoidable way: **the 16.0M-cell segment was building throughout that
window** — it ran ~07:57 to 08:51, spanning both the 08:13 and 08:43 checks — at
4,967 cells/s, squarely inside the collapse trough. So throughput was *not*
decoupled from iowait. My designated PRIMARY metric was showing the damage and I
quoted merge rates instead.

The device confirms it retrospectively: with that segment finished, iowait is
back to 0.0–2.9% and mean request to 57.8–78.6 KB. The high-iowait,
4.17 KB regime was that one segment build, not a steady-state property of the
arm. Reading 1 ("the hints are working, iowait is the intended signature") is
retired; reading 2 (the hints are waste) now has the evidence.

**The 2,235 cells/s segment at 08:49:55 is not new information** — cycle 3 had an
equivalent (2,455 cells/s) at the same point, during its own back-to-back large
merges. Both cycles degrade under merge contention; that one is a wash.

### Merges continue to converge, and that is not a contradiction

| # | cycle 3 | cycle 4 | delta |
|---|---|---|---|
| 4 | 31,908 @ 14,606 b/min | 31,678 @ **12,892** | **−11.7%** |

The merge sequence keeps closing (−46, −37, −54, −11.7%) while the segment
penalty widens with size. These measure different things: the batch counter is
not comparable across merge geometries (established in cycle 3), whereas cells/s
is. Where they disagree, cells/s is the metric with the fixed unit.
| 09:43 | **2h48m in — segments recovered to their best of the run, and the distribution reveals the mechanism.** Table 286 GB / 12 SSTables, pending 1, cache 323 G / free 28 GB, iowait **0.0% x4**, settle 0, phase 25/86. Device calm: 53.2–55.1 KB at ~2,200 r/s, 16–43% util. Segments n=11, median 10,600; last three **11,697 / 11,538 / 11,261** — the run's fastest. No new large merges since 08:43. |

### 09:43 analysis — the arm compresses the ceiling, it does not add a slow tail

n=11 is finally enough to compare *distributions* rather than centres. The two
hypotheses look different in the quantiles: a slow tail would drag the minimum
and leave the median and maximum alone; a constant per-operation tax would pull
the whole distribution down and hit the fast cases hardest.

Standard 3.97M-cell segments, cycle 3's first 11 vs cycle 4's 11 (equal ordinal):

| quantile | cycle 3 | cycle 4 | delta |
|---|---|---|---|
| min | 2,455 | 2,235 | **−9.0%** |
| p25 | 12,426 | 10,297 | −17.1% |
| median | 13,149 | 10,600 | −19.4% |
| p75 | 14,401 | 11,399 | −20.8% |
| **max** | **18,330** | **12,461** | **−32.0%** |
| IQR / median | 0.150 | **0.104** | tighter |

**The penalty rises monotonically from the floor to the ceiling.** The minimum is
essentially unchanged (−9%) while the maximum falls a third. Cycle 4 is also
*more consistent* — IQR/median 0.104 against 0.150 — i.e. consistently worse
rather than occasionally worse.

That is the signature of a **constant per-operation cost, not an occasional
stall**. When a segment is already blocked on contention the extra hints cost
nothing measurable, which is why the floor holds; when nothing else is limiting,
the tax is the whole story, which is why the ceiling drops most. Ten times the
`fadvise` calls per expansion is exactly such a tax — paid on every expansion,
whether or not the hint is ever used.

**This unifies with the 09:13 size-scaling result.** More cells means more
expansions means more calls, so a per-expansion tax must grow with segment size —
which is what −15.2% at 3.97M and −39.7% at 16.0M showed. Two independent
measurements, one mechanism.

### The recovery is real but does not change the conclusion

The last three segments (11,697 / 11,538 / 11,261) are cycle 4's best, and the
device is genuinely idle — iowait 0.0% across four samples, 53–55 KB requests,
16–43% util, free back up to 28 GB. But cycle 3's comparable stretch ran
13,000–18,330. **Cycle 4's best is below cycle 3's median.** The ceiling
compression is visible even at cycle 4's most favourable moment, which is the
point of measuring the maximum rather than the average.

### Where the run stands

Table 286 GB at 2h48m. Cycle 3 collapsed at ~1,000 GB and 9h31m; at cycle 4's
current ~48 GB per 30 min that is roughly **7 more hours**, so mid-afternoon
rather than the 16:25 estimated from wall clock alone. Ingest is not the limiter
here — the client is in `load_increment_adaptive` at 6% of phase 25/86 with zero
errors.

Nothing flagged: no segment in the trough this interval, iowait at zero, no large
merge running.
| 10:13 | **3h18m in — the ~20% tax is STABLE across a 4x table growth.** Table 347 GB / 13 SSTables, pending 3, cache 345 G / free 5 GB, iowait **0.0–0.7%**, settle 0, phase 29/86. Device busy but healthy: 14.8–25.2 KB at 3.3k–6.3k r/s, ~100% util; CPU 66–69% us + 22% sy, 9–11% idle. Segments n=13, median **11,143** (up from 10,600); last three 11,261 / 11,368 / 11,480 — still climbing. No new large merges since 08:43. |

### 10:13 analysis — the tax scales with SEGMENT size, not with TABLE size

These are different variables and separating them matters: the collapse is driven
by table growth (working set outgrowing cache), while the 09:13 result showed the
arm's tax growing with segment size. If the tax also grew with table size, the two
would be the same phenomenon. It does not.

Rolling 5-segment median at equal ordinal, standard 3.97M class:

| segments | cycle 3 | cycle 4 | delta |
|---|---|---|---|
| 1–5 | 13,133 | 10,455 | −20.4% |
| 2–6 | 13,133 | 10,436 | −20.5% |
| 3–7 | 13,133 | 10,159 | −22.6% |
| 4–8 | 13,149 | 10,159 | −22.7% |
| 5–9 | 11,908 | 10,159 | −14.7% |
| 6–10 | 13,366 | 11,143 | −16.6% |
| 7–11 | 14,317 | 11,261 | −21.4% |
| 8–12 | 14,454 | 11,368 | −21.3% |
| 9–13 | 14,325 | 11,480 | −19.9% |

**Nine overlapping windows, all between −14.7% and −22.7%, no drift.** Over this
span cycle 4's table went from ~86 GB to 347 GB — a 4x growth — and the penalty
did not move. Both cycles' absolute rates rise together through the same window
(cycle 3 13,133 -> 14,325; cycle 4 10,455 -> 11,480), so the gap is a constant
*ratio*, not a constant offset.

So the picture is now two independent effects:

| | driven by | magnitude |
|---|---|---|
| the arm's tax | **segment size** | −15% at 3.97M cells, −40% at 16.0M |
| the collapse | **table size** (working set vs cache) | 100x+, at ~1,000 GB |

They are independent, which means **they compound**. The collapse merge in cycle 3
was a 126.9M-cell segment: it will carry both the size-dependent tax and the
table-size collapse. That is the arithmetic behind the 09:13 extrapolation, and
this check is what licenses treating the two as multiplicative rather than as one
effect double-counted.

### Method note — log rotation bit an ad-hoc script this interval

`system.log` rotated mid-cycle; only 1 of the 13 segment lines remained in the
live logs. `c4check.py` unions the `.zip` archives and was unaffected, but a
throwaway analysis script written for this check globbed `*.log` only and reported
n=1 without erroring. Caught by comparing against `c4check.py`'s n=13. This is the
cycle-3 rotation lesson recurring in a new place: **the rule has to apply to
one-off scripts too, not just the standing one.**

Nothing flagged: iowait 0.0–0.7%, no segment in the trough, no large merge
running. The ~100% util at 15–25 KB requests is byte-compaction traffic (a 62 GB
compaction at 96%), the pattern that produced three false alarms in cycle 1.
| 10:43 | **3h48m in — the −20% tax has cost ZERO wall-clock progress so far.** Table 401 GB / 16 SSTables, pending 1, cache 321 G / free 28 GB, iowait **0.0–1.1%**, settle 0, phase 29/86. Device calm: 46.5–47.4 KB at ~2,400 r/s, 20–21% util. Segments n=15, median **11,261** (up again); last three 11,480 / 12,278 / 11,385. |

### 10:43 analysis — cumulative work is identical; the tax is being paid out of headroom

Per-segment rate says how fast each unit runs. **Cumulative work says how much has
actually been finished by elapsed time T** — which is what decides when cycle 4
reaches the collapse. They turn out to say opposite things.

| t+min | cycle 3 cells | cycle 4 cells | delta |
|---|---|---|---|
| 60 | 16,013,399 | 16,016,033 | **0.0%** |
| 180 | 63,757,149 | 63,761,104 | **0.0%** |
| 210 | 71,689,328 | 71,693,257 | **0.0%** |
| **217** | **75,655,480** | **75,659,395** | **+0.0%** |

Matching to four or five significant figures is not a coincidence — it means the
**same number of segments completed in the same wall clock**. (The t+120/150 rows
show ±8–17% purely from a segment landing either side of the mark.)

So where did the 20% go? Into idle time:

| by t+217m | segments | time building | **duty cycle** | mean s/segment |
|---|---|---|---|---|
| cycle 3 | 16 | 126 min | **58.2%** | 473 |
| cycle 4 | 15 | 161 min | **74.2%** | 644 |

**Cycle 4 is working 74% of the wall clock to accomplish what cycle 3 did in 58%.**
The compaction pipeline is not the bottleneck at this table size — ingest is — so
the slower segments simply consume slack that was previously idle. The tax is real
and is currently **free**.

That is the most consequential thing measured today, and it cuts both ways:

- It explains why every other metric looked bad while nothing actually fell
  behind, and it retires any worry that the arm is already damaging the run.
- **It also means the arm has spent 38% of the available headroom to buy nothing
  yet.** Cycle 3 entered its collapse with 42% idle to absorb the shock; cycle 4
  will enter with ~26%. When the collapse drives duty cycle toward 100%, cycle 4
  has correspondingly less slack before ingest starts backing up.

### Revised collapse-window estimate

Cycle 3 had completed 170,841,975 cells when it collapsed at t+564m. Cycle 4's
recent pace is 308,406 cells/min, which reaches that work point in ~309 more
minutes — **t+526m, about 15:40 UTC**. Earlier in wall clock than cycle 3 because
cycle 4's ingest ramp is marginally ahead, not because anything is degrading.

### Note on what this does NOT overturn

The −15%/−40% size scaling (09:13) and the ceiling compression (09:43) are
measurements of per-unit rate and stand unchanged. This check adds that per-unit
rate has not yet translated into lost throughput. Both are true: the arm makes
each unit slower, and there is currently enough slack that it does not matter.
The experiment turns on whether that remains true when the slack is gone.

Nothing flagged: iowait 0.0–1.1%, no segment in the trough, no large merge since
08:43, pretouch 17 calls with none >10M since 07:58.
| 11:13 | **4h18m in — corrects last check's headroom method; the conclusion survives with different numbers.** Table 462 GB / 20 SSTables, pending 1, cache 337 G / free 11 GB, iowait **0.0–1.1%**, settle 0, phase 33/86 `concurrent_query`. Device: 45.1–48.9 KB at ~2,500 r/s, util 19–89% (write bursts to 18.5k w/s). Segments n=18, median **11,238**; last three 11,139 / 11,216 / 11,395 — flat. |

### Correction — last check's duty cycle summed concurrent builds

At 10:43 I reported "duty cycle 58.2% vs 74.2%" and concluded cycle 4 had spent
38% of its headroom. **That measure was invalid.** It summed per-segment build
times over wall clock, but `-Dcassandra.sai.vector.concurrent_builds=2` allows two
segments to build simultaneously — so the sum can legitimately reach 200% and is
not an occupancy figure at all. Computing it per-hour made the error obvious: the
t+60–120m window came out at 124% for cycle 3 and 149.9% for cycle 4.

Recomputed as the **union of build intervals** (each segment's span reconstructed
from its completion time and duration):

| window | c3 busy | c3 peak | c4 busy | c4 peak |
|---|---|---|---|---|
| t+0–60m | 32.7% | 1 | 40.7% | 1 |
| t+60–120m | 79.1% | 2 | **97.0%** | 2 |
| t+120–180m | 31.6% | 1 | 51.4% | 2 |
| t+180–240m | 36.1% | 1 | 40.5% | 1 |
| **whole run to t+256m** | **43.9%** | | **57.7%** | |

The conclusion survives, with smaller margins than I claimed: cycle 4 runs at
**57.7% occupancy against cycle 3's 43.9%** — so ~42% headroom remains, not the
26% stated at 10:43. The 10:43 cumulative-work finding is unaffected; that was
pure counting of completed segments and needs no occupancy model.

### The new part — cycle 4 has already touched saturation once

**During t+60–120m cycle 4 hit 97.0% occupancy at concurrency 2.** That window is
07:55–08:55, which contains the 16.0M-cell segment and the four large merges.
Cycle 3 reached 79.1% over the same window. So the arm has already produced one
near-saturation episode where the baseline had ~20 points of slack left — and that
is precisely the regime where a per-unit tax stops being free.

It recovered: t+180–240m is back to 40.5%, barely above cycle 3's 36.1%. So this
is not a trend, it is evidence that **the headroom gap is not uniform** — it is
small when the pipeline is idle and large when it is loaded, which is the same
ceiling-compression shape found at 09:43, now visible in occupancy rather than
rate.

### A limit of this metric, worth recording before the collapse

Cycle 3's collapse hour (t+504–564m) computes to **17.1% busy at concurrency 1** —
which looks idle and is completely misleading. Occupancy is derived from
*completed* segments, and during the collapse the 126.9M-cell segment ran for
hours without completing, so it contributes nothing to any window. **This metric
cannot detect the collapse; it can only measure headroom during healthy
operation.** Use cells/s and the device signature for the collapse itself.

Nothing flagged: iowait 0.0–1.1%, no segment in the trough, no large merge since
08:43. The 89%/79% util spikes are write bursts (8.3k–18.5k w/s) from an 87 GB
byte compaction at 34.6%, not the read-starvation pattern.
| 11:43 | **4h48m in — the exact cumulative match now holds across 4.7 hours.** Table 516 GB at the reading, 517 GB by the end of the check / 20 SSTables, pending 3, cache 339 G / free 8 GB, iowait **0.0–0.4%**, settle 0, phase 37/86. Device: 40.3–40.6 KB at ~2,400 r/s, 98–99% util (write-side, 3.8k–4.3k w/s). Segments n=20, median **11,240**; last three 11,395 / 11,897 / 11,219 — flat for three checks. |

### 11:43 analysis — first equal-TABLE-SIZE comparison, and it disagrees slightly with equal-cells

The cumulative-work match from 10:43 now extends to t+282m and is still exact:

| t+min | cycle 3 cells | cycle 4 cells | delta |
|---|---|---|---|
| 180 | 63,757,149 | 63,761,104 | 0.0% |
| 210 | 71,689,328 | 71,693,257 | 0.0% |
| 240 | 79,621,551 | 79,625,492 | 0.0% |
| 270 | 91,519,941 | 91,523,697 | 0.0% |
| **282** | **95,486,088** | **95,489,791** | **0.0%** |

Five checkpoints, 4.7 hours, agreement to five significant figures. **The arm has
cost zero net index throughput.**

But the equal-bytes comparison — the one this watch has been asking for and could
not do until cycle 4 reached cycle 3's recorded range — gives a slightly different
answer. Cycle 3's own log has 451 GB at t+235m and 574 GB at t+295m, so it passed
516 GB at roughly **t+267m**. Cycle 4 reached 516 GB at **t+282m**:

**~15 minutes, or 5.3%, behind on table bytes while exactly level on index cells.**

The two cannot both be simple truths, so one of them is measuring something else.
The likely resolution is that `Space used (live)` is not a clean progress metric —
it moves with compaction state, not just with data written, and cycle 4 currently
has a 106 GB byte compaction at 30.3% plus another at 100% awaiting release. Index
cells come from a deterministic client write schedule and are counted at segment
completion, which is why they align so precisely.

**Weight accordingly: one interpolated comparison point against five exact ones.**
Recorded because it is the first equal-bytes data available and because if the lag
grows at the next checks it becomes the leading indicator of the transition — the
point where the tax stops being free. If it stays at ~5% or shrinks, it is
compaction-state noise.

### Nothing else new

Segment rate has been flat for three checks (11,238 / 11,240 / 11,240 median), the
device is doing write-side byte-compaction work at 40 KB requests, iowait is at the
floor, and no large merge has run since 08:43. The pretouch is 22 calls / 183.0 s
with nothing above 10M ordinals since 07:58.

Nothing flagged. Cycle 3's collapse came at t+564m; cycle 4 is at t+282m, exactly
halfway.
| 12:13 | **5h18m in — the byte lag is a stable offset, not a divergence.** Table 565 GB / 22 SSTables, pending 3, cache 339 G / free 6 GB, iowait **0.1–0.7%**, settle 0, phase 41/86. Device: **4.46–4.56 KB** at 3.2k–4.7k r/s, 98–99% util, CPU 59–63% us + 25–26% sy. Segments n=22, median **11,240** — unchanged for four consecutive checks. |

### 12:13 analysis — answering last check's question

At 11:43 I flagged a 5.3% equal-bytes lag and said: if it grows it is the leading
indicator of the transition; if it holds at ~5% it is compaction-state noise. Now
there are three points:

| table size | cycle 3 reached | cycle 4 reached | lag |
|---|---|---|---|
| 462 GB | t+240m | t+258m | +18 min (**+7.3%**) |
| 516 GB | t+267m | t+282m | +15 min (**+5.7%**) |
| 565 GB | t+291m | t+318m | +27 min (**+9.4%**) |

**Non-monotone — 7.3 -> 5.7 -> 9.4% — so it is an offset, not a divergence.**
Mean ~7.5%, and the middle point is the smallest, which rules out steady growth.
Against that, cumulative index cells now match at **six** checkpoints:

| t+min | cycle 3 | cycle 4 | delta |
|---|---|---|---|
| 240 | 79,621,551 | 79,625,492 | +0.00% |
| 270 | 91,519,941 | 91,523,697 | +0.00% |
| 300 | 99,452,211 | 99,455,877 | +0.00% |
| **310** | **103,418,316** | **103,421,994** | **+0.00%** |

Exact agreement over 5.2 hours against a noisy ~7.5% byte offset built on
interpolating cycle 3's sparse size record (10 points across 5 hours). The cells
measurement wins on both precision and directness. **Conclusion: the arm has not
started costing throughput.** The byte offset is real but stable and most likely
reflects compaction-state accounting — cycle 4 has 22 SSTables and just finished a
112 GB compaction, and `Space used (live)` moves with what has been released, not
with what has been written.

### Otherwise nothing new

Segment median has been 11,238 / 11,240 / 11,240 / 11,240 across four checks — as
flat as this metric gets. No large merge since 08:43. Pretouch 24 calls / 195.5 s,
nothing above 10M ordinals since 07:58.

The device is showing **4.46–4.56 KB at 98–99% util** — the small-request
signature — but iowait is 0.1–0.7% and CPU is 85–89% busy, so this is the
byte-compaction pattern that produced three false alarms in cycle 1, not read
starvation. Recorded, not flagged.

Cycle 4 is at t+310m; cycle 3 collapsed at t+564m.
| 12:43 | **5h48m in — pre-registering the verdict before the decisive segment arrives.** Table 626 GB / 26 SSTables, pending 1, cache 319 G / free 25 GB, iowait **0.0–1.0%**, settle 0, phase 45/86. Device calm: 36.3–40.3 KB at ~2,700 r/s, 25–26% util. Segments n=25, median **11,219** (was 11,240); last three 10,814 / 10,732 / 11,525. |

### 12:43 analysis — the prediction, on record, before the data

Cycle 3's collapse was one 126.9M-cell segment. Cycle 4 should reach it in about
3.5 hours. Given that this watch has already produced five trend calls that
reversed and one "exonerated" conclusion that had to be retracted, the specific
failure mode to guard against now is **fitting the story to whatever arrives**.
So the prediction goes in the log first, with numbers that can be wrong.

**When.** Cumulative cells match exactly at six checkpoints, so cycle 4 should hit
cycle 3's collapse work point (170,841,975 cells, reached at t+564m) at about
t+564m — **~16:18 UTC**, ±40 min for the ~7.5% byte offset.

**The baseline, converted into the primary metric.** Cycle 3's 126.9M-cell segment
ran at 60–74 b/min; at 4,096 ordinals per batch that is **4,096–5,052 cells/s**,
implying 7.0–8.6 h for the segment.

**The prediction.** The measured size-scaling law (`retained ~ size^-0.245`, fitted
on −15.2% at 3.97M and −39.7% at 16.0M) gives a retained fraction of **0.36** at
126.9M cells:

> **Cycle 4's 126.9M segment will run at roughly 1,486–1,833 cells/s — 19–24 hours
> — and the arm will have made the collapse materially worse.**

**Falsifiable outcomes, decided in advance:**

| result | meaning |
|---|---|
| **below 4,096 cells/s** | ARM FAILS — worse than the `frontierPrefetch=3` baseline |
| 4,096–5,052 cells/s | ARM NEUTRAL — the tax is invisible where it matters |
| **above 5,052 cells/s** | **ARM WORKS** — the only outcome that justifies the setting |

If the result lands above 5,052 cells/s, the size-scaling law is wrong and the
above-RAM hypothesis is right, and this entry should be cited as the prediction
that failed. Two points do not make a power law; that caveat was recorded at 09:13
and stands.

### Otherwise stable

Segment median 11,240 -> 11,219, within noise, flat for five checks. No large merge
since 08:43 — a 3h50m gap, so the STCS schedule has not yet produced another. The
131 GB byte compaction at 46.6% is the only pending work. Pretouch 26 calls /
206.3 s, nothing above 10M ordinals since 07:58. Free memory has *risen* to 25 GB
at 626 GB of table, so the working set still fits comfortably — the collapse
precondition is not yet in place.
