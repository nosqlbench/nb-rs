# Run E watch log — full-fix stack, 200M attempt (2026-08-27 01:57 UTC)

Successor to `runwatch-20260826-runD.md` (Run D: integrity guard fail-stopped the node
at t+2h17m on an EMPTY vector index — root-caused to the `isEmpty()` async race, fixed).
Run E is the first run with every known defect fixed on both ends.

## Stack
- **Client**: `nmbrs` pid **4007198** (process name `nmbrs`; pgrep 'nmbrs run', mind the
  self-match trap; verify via fuser on the session metrics.db), session
  `sessions/stcs_adaptive_20260827_015712`, started 01:57:12. Binary = the 01:37 build:
  breaker_watch server-independent sensor (b8506e1/121fc23), all six dead `{part}`
  selectors fixed (27b27b5 + 673988e: `part_str` + `metricsql_scalar_dyn`), metricsql
  guards (b5c623f). KNOWN COSMETIC: `breaker-watch tick` INF lines at 1/s — the
  stdout-channel excision fix (910709e) landed after this client started; silenced on
  its next restart.
- **Server**: Cassandra pid **4005715** up 01:53:45 (new_cass wipe), jar built 00:49
  **with the `VectorMergeSegmentBuilder.isEmpty()` fix** (rowsWithVector counter, not the
  racing async postingsMap) + the VectorIndexIntegrity fail-stop guard
  (RestartPreventExitStatus=100 drop-in: an abort HOLDS THE NODE DOWN for inspection —
  do not blindly restart). jvector jar unchanged: `33f1202c` (7de94e83). Flags verified
  live: similarityOrdinals=true, clusterSearch=false, sourcePretouchMaxNodes=-1,
  WindowNodes=1048576, frontierPrefetch unset (=3). MemoryHigh=146G (~38 GiB file cache).
- CUT ALL LOG ANALYSIS AT 2026-08-27 01:57:00. compaction.log HAS ROTATED during this
  run already — include .zip/rotated archives when computing per-merge walls.

## Discipline (unchanged — rationale in Run C/D headers)
WALL min/M (`Similarity ordinals assigned` → `TERMS_DATA written in place`, matched per
merge) is the metric of record; batch/ordinal instantaneous rates are segments only.
Log fields $3=date $4=time; the run will cross midnight — filter
`($3=="2026-08-27" && $4>="01:57:00") || $3>"2026-08-27"`.

## References
- 16M class walls: Run D 4.29 min/M; Run C steady 4.5–4.8; Run E's first four:
  ..., fourth = **6.79 min/M** (09:12→10:59:49, co-ran against sibling 16Ms — within the
  historical 1.5–8.5 spread). Cluster-ON 9-src ref 2.31. Collapse era 8.5+.
- 63.6M class: Run C base 3.9±0.3 min/M — CORRECTED 17:2x: that covered COUNTER STREAM 1
  only; Run C's "99.999% wedge" sat at the same mid-merge boundary where Run E's counter
  reset (see the 17:22 entry), so the class's TOTAL cost was never measured before Run E.
  Run E stream-1: 4.6–5.0 min/M (comparable and consistent); full wall projecting
  ~8.0–8.3 min/M, landing ~19:15–19:45 — the campaign's FIRST completed 63.6M wall.
- Ordinal-pass norm 11.5–12.5 µs/node; flag >100 twice per interval. Starved-pass
  outliers seen: 40.5 (Run D), 44.6 (Run E 09:01).
- 100M rows reached at ~t+10h08m; partition 10 (final 100M) began ~12:05.

## Entries

### 12:30 UTC — t+10h33m — armed; 100M in; 63.6M at 28%
- Parts 0–9 complete (100M rows), part 10 at 1.5M. Table 663.6 GB / 21 SSTables.
- 63.6M: 34,960/123,949 (28.2%), clip 414 b/min ⇒ 4.71 min/M. Land ~16:30±30m.
- Gate 0; integrity guard silent through 10.5h (isEmpty fix's first extended run);
  max=0; device 190k r/s @ 6.1 KB 100% util (expected storm).
- Client: breaker wires live (57,325 attempts/window), lint 0, no-data blips 36
  (cold-start cosmetic), ticks 38k (known, silenced next restart).

### 13:14 UTC — t+11h17m — 63.6M at 42.8%, clip locked at 4.70 min/M

- Table: 676.6 GB / 23 SSTables (+13.0 GB, +2 since 12:30). Part 10: 4.10M rows (~3.5M/h, servo-throttled). Client + Cassandra up.
- **63.6M merge: 52,990/123,949 (42.8%)** at 13:14:01. Interval 12:30→13:14: 18,030 b in 43.5 min = **415 b/min ⇒ 4.70 min/M** — identical to the previous window's 414; the clip is locked. Base ends ~16:05, land ~16:25–16:55.
- Completed merges / ordinal passes this interval: none — the window is wholly the 63.6M's, same single-story pattern as Run C's giant.
- Device: 184–189k r/s @ 6.0 KB, 99–100% util, iowait 41.7%. Cgroup: anon 107.1G / file 35.9G, max=0.
- Gate: 0 cluster-cost lines; 0 integrity/assertion lines; breaker wire live (46,309 attempts/window); lint 0.
- Trend: steady-state above-RAM grind, flat within a single b/min across two windows — Run E's 63.6M runs ~12% slower than Run C's 3.9–4.2 base clip but with a fixed pool under it; on pace for the campaign's first completed 63.6M wall this afternoon.

### 13:44 UTC — t+11h47m — 63.6M crosses half: 53.1%, clip 4.57 min/M

- Table: 689.5 GB / 25 SSTables (+12.9 GB, +2). Part 10: 5.62M rows (~3.0M/h). Client + Cassandra up.
- **63.6M merge: 65,810/123,949 (53.1%)** at 13:44:01. Interval 13:14→13:44: 12,820 b in 30.0 min = **427 b/min ⇒ 4.57 min/M** — a nudge faster (415→427). Base ends ~16:00, land ~16:20–16:50.
- Completed merges / ordinal passes this interval: none.
- Device: 188k r/s @ 6.0 KB, 100% util, iowait 47.9%. Cgroup: anon 107.1G / file 35.7G, max=0.
- Gate: 0 cluster-cost; 0 integrity/assertions; breaker wire live (46,310/window — stable attempt rate, not a freeze: value moves); lint 0.
- Trend: past the halfway mark with the clip drifting mildly FASTER (4.70→4.57) — the same hot-set settling Run C showed at this depth; landing window unchanged.

### 14:14 UTC — t+12h17m — 63.6M at 63.3%, clip steady 4.63 min/M

- Table: 696.0 GB / 26 SSTables (+6.5 GB, +1). Part 10: 7.13M rows (3.0M/h, servo steady). Client + Cassandra up.
- **63.6M merge: 78,470/123,949 (63.3%)** at 14:14:01. Interval 13:44→14:14: 12,660 b in 30.0 min = **422 b/min ⇒ 4.63 min/M** — third window in the 4.57–4.70 band. Base ends ~16:02, land ~16:20–16:50.
- Completed merges / ordinal passes this interval: none.
- Device: 186–188k r/s @ 6.0 KB, 100% util, iowait 40.3%. Cgroup: anon 107.1G / file 35.8G, max=0.
- Gate: 0 cluster-cost; 0 integrity/assertions; lint 0. Breaker wire: near-identical values across checks (46,309–46,310) prompted a freeze check — the last 8 samples jitter (46,276…46,310), i.e. a servo-locked ~842 attempts/s producing near-constant window sums. LIVE, verified; future checks needn't re-litigate small-jitter constancy.
- Trend: metronomic — three flat windows inside 0.13 min/M of each other; the giant is two hours from the campaign's first 63.6M wall.

### 14:44 UTC — t+12h47m — 63.6M at 73.0%, clip 4.86 min/M (band widens slightly)

- Table: 708.9 GB / 28 SSTables (+12.9 GB, +2). Part 10: 8.40M rows (~2.5M/h — servo easing as backlog deepens). Client + Cassandra up.
- **63.6M merge: 90,530/123,949 (73.0%)** at 14:44:00. Interval 14:14→14:44: 12,060 b in 30.0 min = **402 b/min ⇒ 4.86 min/M** — coolest of four windows (4.57–4.86, mean ~4.7); consistent with the table growing +45 GB under the fixed cache since the merge began. Base ends ~16:07, land ~16:25–16:55.
- Completed merges / ordinal passes this interval: none.
- Device: 173–182k r/s @ 6.0 KB, 100% util, iowait 44.7%. Cgroup: anon 107.1G / file 35.7G, max=0.
- Gate: 0 cluster-cost; 0 integrity/assertions; wire live (38,392 — dropped with the servo easing, further proof of liveness); lint 0.
- Trend: three-quarters done with a gentle cooling drift inside the band — the wall projects to ~4.8–5.1 min/M total, decisively below the 6 flag and the campaign's first completed 63.6M verdict remains ~2 checks out.

### 14:50 UTC addendum — IO pressure attributed to the stack frame (capture: docs/captures/iosat-20260827-1450/)

- **79% of RUNNABLE samples (95/120) block in ONE call chain**: `GraphSearcher.searchOneLayer:478 → FrontierPrefetchingView.processNeighbors:138 → OnDiskGraphIndex$View.processNeighbors:692 → FusedPQDecoder.enableSimilarityToNeighbors:88 → FusedPQ$PackedNeighbors.readInto:224 → View.getPackedNeighbors:667 → MemorySegmentVectorProvider.readByteSequence:94 → RebufferingInputStream.readFully:141` — the per-expansion fused adjacency+PQ record read of the cross-source symmetric search (group 3/4: rank-2 source, 1 search/node). +5 more in the same family (adjacency `readInts:200`, `seek:336`). **12% (14)** in the exact rerank: `rescore:2414 → getVectorInto:621 → readFloats:186`. Diversity/own-record reads: 2 samples (page-cache-warm as designed).
- **Queue-depth arithmetic closes exactly**: per-member r_await 0.22 ms × ~40 synchronous FJP readers ⇒ 182k IOPS predicted; measured 187k r/s (4× nvme at 46.8k each, 6.03 KB, 100% util, 1.13 GB/s — pidstat: ~37 workers × 30.7 MB/s = the whole stream). The device is saturated by LATENCY-bound sync reads at qd≈40, using ~1/3 of its large-request bandwidth — same physics as the Run C capture, now with the true 220 µs latency measured (the 100 µs figure in earlier docs was optimistic).
- Vs Run C's capture: packed-read share 63%→79%, hint-issuance samples 24%→0 — consistent with group position (single-search group, no seed bursts) rather than a mechanism change; rescore steady at 12%.
- **Starvation forming behind the giant**: 6 builds queued in `acquireBuildPermit:934` and the NEXT merge's similarity-ordinal pass parked in `joinAll:2911` — expect a burst of landings + a slow pass right after the 63.6M completes (the exact burst pattern the isEmpty fix now guards).

### 15:13 UTC — t+13h17m — 63.6M at 82.5%, cooling drift continues: 4.98 min/M

- Table: 715.4 GB / 29 SSTables. Part 10: 9.65M rows (2.5M/h, servo re-locked ~698 rows/s — the wire's 38,393/55s window matches exactly). Client + Cassandra up.
- **63.6M merge: 102,280/123,949 (82.5%)** at 15:13:55. Interval 14:44→15:13: 11,750 b in 29.9 min = **393 b/min ⇒ 4.98 min/M** — fifth window, monotone gentle cooling (427→422→402→393) tracking table growth; still under the 6 flag with ~55 min of base left. Base ends ~16:09, land ~16:25–16:55.
- Completed merges / ordinal passes this interval: none (the starved next-merge pass observed in the 14:50 capture still hasn't printed — consistent with joinAll parking).
- Device: 178–180k r/s @ 6.0 KB, 100% util, iowait 36.9% — softening in step with the clip. Cgroup: anon 107.1G / file 35.6G, max=0.
- Gate: 0 cluster-cost; 0 integrity/assertions; wire live; lint 0.
- Trend: the giant's base layer is in its final hour with a slow, explainable cooling curve; everything else is queued patiently behind it exactly as the 14:50 capture showed.

### 15:43 UTC — t+13h47m — 63.6M at 91.9%, ~26 min of base left

- Table: 721.9 GB / 30 SSTables. Part 10: 10.93M rows (2.6M/h). Client + Cassandra up.
- **63.6M merge: 113,900/123,949 (91.9%)** at 15:43:54. Interval 15:13→15:43: 11,620 b in 30.0 min = **387 b/min ⇒ 5.05 min/M** — sixth window of the gentle cooling curve (427→422→402→393→387). Base ends ~16:10; upper layers + write-back ⇒ **land ~16:25–16:50**; projected total WALL ≈ **5.2–5.4 min/M** (pass 10:58:36 → ~16:35).
- Completed merges / ordinal passes this interval: none — the starved next-pass still parked.
- Device: 179–182k r/s @ 6.0 KB, 100% util, iowait 43.8%. Cgroup: anon 107.1G / file 35.6G, max=0.
- Gate: 0 cluster-cost; 0 integrity/assertions; wire live (38,392 — servo-locked constancy, documented 14:14); lint 0.
- Trend: final half-hour of the base layer, cooling curve fully explainable by +58 GB of table growth since merge start; the landing, the wall verdict, and the queued-build burst all arrive before the next scheduled check.

### 16:26 UTC — t+14h29m — 63.6M BASE LAYER COMPLETE (16:11:22); upper layers in flight; the 5-hour starved pass surfaces

- **63.6M base layer completed 16:11:22** (123,940/123,949 — the usual one-short ending; NOT a wedge: the pool is demonstrably alive). Base-layer span: pass 10:58:36 → L0 done 16:11:22. Now in upper layers — progress counter reset to 3,400/123,950 (the Run-D drift+reset behavior), ~240–690 b/min; upper volume ≈2.5% of nodes but cross-source at depth, ETA minutes-to-tens-of-minutes. Wall verdict lands with TERMS_DATA (~77 GB write-back after upper layers); background waiter armed (bv3xd44ds) → push on landing. Monitor's observed segment: **4.84 min/M over its 213-min window** — consistent with the six cron windows (4.57–5.05).
- **Monitor event folded — SLOW ORDINAL PASS: the queued 4M merge's pass completed at 16:10:35 having run at 4,554.8 µs/node (380× the 12 µs norm)** — this is the pass the 14:50 capture photographed parked in `joinAll:2911`: it started ~11:09, spent ~5 h starved under the giant's base layer, and finished seconds before L0 released the pool. Starvation, not a dispatch regression (single occurrence; the 4M merge is now progressing normally at 1,370/7,747). One pass — below the twice-per-interval push threshold; documented here instead.
- Table: 728.4 GB / 31 SSTables. Part 10: 12.06M rows. Device 176k r/s @ 5.9 KB, util 100%. Cgroup: anon 107.1G / file 35.6G, max=0.
- Gate: 0 cluster-cost; 0 integrity/assertions; wire live; lint 0.
- Trend: the giant's endgame — base done in 5h13m (~4.9 min/M for L0 alone), upper layers now the open question after the 15.86M precedent (44 s) clearly won't scale; the queued-build burst is beginning exactly as the 14:50 capture predicted.

### 16:44 UTC — t+14h47m — 63.6M upper phase larger than modeled: 14,560 batches at 607 b/min; wall still pending

- **63.6M upper phase: 14,560/123,950 batches at 16:44:02** (3,400 at 16:25:40 ⇒ 11,160 b in 18.4 min = **607 b/min**). The upper stream has already covered ~7.5M batch-ordinals — far beyond the naive layer-1 estimate (~1.6M ords) — so the counter's upper-phase semantics differ from L0's (possibly per-source×per-layer sweeps); the landing CANNOT be projected from this counter reliably. The background waiter (bv3xd44ds) on adopt→TERMS_DATA remains the source of truth; no adopt line yet.
- No landings, no ordinal passes this interval (the 4M's own progress interleaves quietly). Gate 0; integrity 0; max=0; wire live; lint 0.
- Table: 734.9 GB / 32 SSTables. Part 10: 13.11M rows (~2.5M/h). Device: 186k r/s @ 6.0 KB, 100% util, iowait 41.7% — the upper phase saturates the device exactly like base.
- Trend: the giant's tail is longer than the small-merge precedent suggested — upper layers are a real cost regime at this scale (novel observation for the campaign; Run C never reached them on this class); wall verdict now likely in the 17:00–18:00 window unless the counter total misleads.

### 17:22 UTC — t+15h25m — CORRECTION: the 63.6M is still in LEVEL 0; the 16:11 "base complete" was a counter artifact

- **Correction to the 16:26 and 16:44 entries.** A thread dump (17:20) shows the giant's executor (CompactionExecutor:41) inside the L0 source-group loop (`compactLevels:1462 → runBatches:2449`) — NOT upper layers. What happened at 16:11:22 was a batch-counter RESET with a +1 total drift (123,940/123,949 → 0/123,950), a display artifact at a group boundary, not L0 completion. No failure, no retry, no re-run ordinal pass, no errors at the boundary — the merge never stopped. New counter-semantics observation for the header rules: the batch numerator can reset mid-merge; the ORDINAL numerator of the current stream (20,132,976/63,610,212 at 17:14, ~365k ords/min) reads monotone-per-merge here, unlike the per-group resets documented in Run C — treat BOTH counters as unreliable for absolute position; only passes, TERMS_DATA, and stacks are ground truth.
- Landing estimate revised on the ordinal stream: ~43.5M ordinals to go at ~365k/min ⇒ **~19:15–19:45** if that counter is truly monotone; earlier if it is per-group. The waiter (bv3xd44ds) on adopt→TERMS_DATA remains authoritative and pushes on landing.
- The wrongly-inferred "upper layers are a new cost regime" note in 16:44 is WITHDRAWN pending the actual upper phase.
- Interval otherwise: 0 landings, 0 passes, gate 0, integrity 0, max=0, wire live, part 10 ~13.9M rows, device unchanged (186k r/s @ 6.0 KB).
- Trend: the giant grinds on in L0 — slower than every projection because the projections trusted a counter that lies at boundaries; the pool is healthy and the stack shows the same 79%-packed-read physics as the 14:50 capture.

### 17:44 UTC — t+15h47m — stream-2 batches steady at 588 b/min; ordinal density varies 5×; landing ~19:30–20:00

- **63.6M stream-2: 51,050/123,950 batches** at 17:44:09 (33,340 at 17:14) = **588 b/min** — the batch clock is steady (607→605→588). The ORDINAL numerator moved only +2.24M in the same window (126 ords/b vs 604 cumulative) — ordinal density varies ~5× by region, so neither counter alone is a clean progress proxy; the batch stream is the steadier clock. If stream-2 runs its full 123,950: **~124 min left ⇒ landing ~19:30–20:00** (wall would print ~8.5–9.0 min/M). Waiter armed.
- One active stream only in the recent window — the 4M's stream has gone quiet without a TERMS_DATA (write-back or queued; not flagged, watching). 0 landings, 0 passes this interval.
- Table: 754.3 GB / 35 SSTables (+3 flushes). Part 10: 15.23M rows (2.1M/h — servo tightest yet). Device: 182k r/s @ 6.0 KB, 100% util, iowait 42.9%. Cgroup: anon 107.1G / file 35.4G, max=0.
- Gate: 0 cluster-cost; 0 integrity/assertions; wire live; lint 0.
- Trend: the giant's second sweep grinds at a constant batch clock with wildly varying ordinal density — the class's first true wall now reads ~8.5–9.0 min/M, roughly twice the stream-1-only figure the old references carried.
