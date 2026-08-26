# Run C diagnostic log — symmetric path, `clusterSearch=false`

Companion to `runwatch-20260826.md` (staging state + corroboration) and
`runwatch-20260825.md` (Runs A/B/B2). 30-minute entries appended below by the watch.

## Context

**Run C** = the overnight-prepared post-fix stack + one flag. Started
**2026-08-26 03:48** (session `stcs_adaptive_20260826_034801`), table wiped, cache-
constrained rig (`MemoryHigh=146G` → ~38 GiB page cache; above-RAM from ~05:00).

| | |
|---|---|
| jvector | `experiment/cluster-rescore-prefetch-20260824` @ `7de94e83`, jar md5 `33f1202c…` |
| cassandra | `4c29e7c6ee` + 29 uncommitted files (jar 01:06) — known provenance debt |
| flags | `similarityOrdinals=true`, **`clusterSearch=false`**, pretouch `-1`/`1048576` |
| decision | symmetric merges are the practical reality (STCS(4) equal sources defeat retain-largest's 0.5 base-share gate); optimize the symmetric path first |

**Config verification (done 05:58):** flag live on pid 3081002's cmdline; 5+ merges
completed with zero `Cluster path cost` lines (that line prints whenever the path
runs — silence + flag = path off); `REORDERED by the compactor` on every merge; zero
read-back assertions; ordinal passes at post-fix 11.5–11.9 µs/node.

**References.** Post-fix cluster-ON 9-src merge: 2.31 min/M. Run A/B2 16M-class on
this rig: 1.5–8.5 min/M, 5.5× spread, collapse ≈ 8.5+ min/M with the 5.6 KB / 300k r/s
/ high-util device signature. Merges are compared in **min per million ordinals**
(the progress line carries ordinals; batch counts are not comparable across merges).
Timestamp fields in Cassandra logs: `$3`=date `$4`=time — filter on **$4** (a $3
filter passed everything and produced one false alarm already this run).

**Early Run C series: RETRACTED — see the 06:55 correction entry.** The
ordinal-delta and batch-window rates both proved unreliable (source-group counter
resets; interleaved concurrent builds fragment the progress stream). Metric of
record is per-merge WALL: `Similarity ordinals assigned` start → `TERMS_DATA
written in place`, in min/M — the same method as the efficiency analysis.

**Watching for:** the larger classes (≥20M ordinals — overnight produced a 35.7M;
the historical decisive class is 126.9M) landing and their min/M; any `Cluster path
cost` line (= flag regression); ordinal passes >100 µs/node (the 05:18 outlier hit
445 µs during peak overlap — recurrence makes it the next symmetric-path target);
`memory.events max>0` (rig misbehaving); the saturation breaker (first run with it
live — `recent_attempt_*` scoped windows).

---

## Entries

| 05:58 (manual baseline) | t+2h10m, ~200 GB / 10 SSTables. Series and device as above. cgroup high-events 562k, max 0. |
| 06:26 | t+2h38m, 231 GB / 5 SSTables. Gate ✓ (0 cost lines, 9 REORDERED, 0 assertions). Completed since baseline: only the 06:00 close (1.37 min/M, already in series). **In flight: 15.9M at 5.69 min/M (17%)** — and the device has flipped to the read-storm signature: **5.35 KB @ 264–269k r/s, 100% util, iowait 29.4%** (was 90 KB @ 30% util). Ordinal passes: two more slow ones under merge overlap (98.9, 29.0 µs/node) between clean 10.4–11.9s; both 16M passes at 11.9. cgroup file 38.2 GiB, high-events 671k, max 0. Trend: the small-request read storm is back **with the cluster path off** — the plain symmetric searches alone produce it above RAM; rate swing 1.37→~5.7 min/M is inside the known 5.5× spread, so watching, not alarmed. |
| 06:55 | **CORRECTION + the real picture.** Three defects in my merge-rate derivation, found via a raw line reading `30940/30985 batches (3960320/15864790 ordinals)` — 99.9% batches, 25% ordinals: (1) the ordinal numerator resets per source group → ordinal-delta rates ~4× pessimistic; (2) batch windows measure only the L0 write phase; (3) with `concurrent_builds=2` two merges interleave one progress stream, so my per-merge segmentation manufactured "four accelerating 16M merges" out of fragments. **Wall-based truth (pass→TERMS pairing), all 9 completions since 03:48:** seven ~4M merges — 0.82/1.11/1.06/0.97 early, then 13.20 (the 52-min one starved under overlap), 2.68/0.58/1.96 — and **one 16M at 4.64 min/M (74-min wall)**. References on the same metric: pre-fix 16M 8.54, post-fix cluster-ON 9-src 2.31 (below-RAM, no overlap). So Run C's 16M sits between them, above RAM with a concurrent build — **not** the sub-reference miracle I reported at 05:58/06:26; those entries' rates are retracted (gate/config verification in them stands). Slow ordinal passes now 2×: 445 and **455 µs/node** (30 min for 4M), both fully overlapped by the 16M's read storm — starvation of the pass's full-precision encode reads, the clearest O4 motivation yet. Gate still ✓ (0 cost lines, 0 assertions). |
| 06:56 | t+3h08m, 261 GB / 10 SSTables. Gate ✓ (0/9/0). **No completions this interval** — two merges in flight: the 15.86M (pass 06:20 @ 12 µs/node; **36+ min elapsed and counting**) and the starved 4M (pass 06:47 @ 455 µs — the monitor's second slow-pass event, folded here). Device escalated to full collapse-era intensity: **5.13 KB @ 238–244k r/s, 100% util, iowait 40.4%** (29.4% at 06:26). cgroup file 37.9 GiB, high 909k, max 0. Trend: storm deepening while both in-flight builds crawl; the 15.86M's completion wall is the discriminator — ≤74 min repeats the 4.64 min/M shape, ≥95 min crosses the 6 min/M flag line. |
| 07:26 | t+3h38m, 279 GB / 13 SSTables. Gate ✓ (0 cost lines, 0 assertions). **Still no completions** — 15.86M now 70 min elapsed (**4.39 min/M and climbing**; crosses the 6 min/M flag line at ~07:51), starved 4M at 69 min (**17.35 min/M so far**). But **the storm broke** in the last minutes: md0 29k→2k r/s, util 60→22%, **iowait 0.0** (was 40.4% at 06:56) — a base layer finished its search phase. cgroup high 1.12M, max 0. Trend: the 15.86M is tracking the first 16M's ~74-min wall shape; its landing decides repeat (~4.6–5.0) vs flag (≥6). |
| 07:56 | t+4h08m, 328 GB / 6 SSTables (13→6 consolidation). Gate ✓ (0/0). **Five completions:** the 15.86M at **71.3 m = 4.49 min/M** (repeat of the first 16M's 4.64 — class now 4.64/4.49, consistent, below the 6 flag line, ~2× the below-RAM 2.31 ref); the starved 4M at **69.9 m = 17.64 min/M** (overlap starvation quantified end-to-end); then three quiet-window 4Ms at 0.60/0.40/3.01. New 15.86M in flight 11 m, storm back for its base layer (5.38 KB @ 266–270k r/s, 100% util, iowait 30.7%). cgroup high 1.26M, max 0. Trend: a stable above-RAM cadence is emerging — ~72-min 16M storms back-to-back, 4M merges fast in the gaps and starved during overlap; no degradation across the 16M class. |
| 08:26 | t+4h38m, 358 GB / 11 SSTables. Gate ✓ (0/0). No completions this interval; the 15.86M in flight is 41 m in (2.57 min/M so far — on the ~72-min cadence for another ~4.5 finish), storm running for its base layer (5.17 KB @ 237–242k r/s, 100% util, iowait 36.4%). cgroup high 1.50M, max 0. Trend: the 07:56 cadence holding exactly — nothing new to add. |
| 08:56 | t+5h08m, 376 GB / 14 SSTables. Gate ✓ (0/0). No completions in the interval; the 15.86M is at **71 m = 4.46 min/M so far** with the storm just broken (24k→2k r/s, iowait 1.2%) — landing imminently, third consecutive ~72-min 16M. The overlapped 4M (monitor's **third** slow pass: 301 µs/n; tally 445/455/301, all under storm) is at 40 m = 10.14 min/M so far. cgroup high 1.71M, max 0. Trend: cadence intact for a third cycle; starvation is now 3-for-3 on small builds that overlap a 16M base layer — the O4 case keeps compounding. |
| 09:26 | t+5h38m, 425 GB / 7 SSTables. Gate ✓ (0/0). **Third 16M landed: 73.0 m = 4.60 min/M** — class now **4.64 / 4.49 / 4.60** (3% spread over three consecutive cycles). Starved 4M landed at 10.64 min/M (starvation completions now 17.64, 10.64); quiet-window 4Ms 1.19 / 1.87 / 1.08. Fourth 16M in flight (11 m), storm running with **iowait 65.7%** — the highest of the run, above the historical 30–50 band; noting as context per the rate-primary rule, watching whether this cycle's wall stretches past ~73 m. cgroup high 1.84M, max 0. Trend: the 16M class is now solidly characterized at ~4.6 min/M above RAM; the open question is whether the deepening iowait bends cycle four. |
| 09:56 | t+6h08m, 455 GB / 12 SSTables. Gate ✓ (0/0). No completions this interval; fourth 16M at 41 m (2.59 min/M so far — on the ~73-min cadence), its overlapped 4M (monitor's **fourth** slow pass, worst yet: 475 µs/n; tally 445/455/301/475) at 40 m = 9.97 so far. Storm steady: 5.21 KB @ 243–246k r/s, 100% util, iowait 39.6% (down from the 65.7 peak). cgroup high 2.09M, max 0. Trend: mid-cycle quiet, cadence unbent so far; cycle-four wall lands next interval and answers the iowait question. |
| 10:26 | t+6h38m, 473 GB / 15 SSTables. Gate ✓ (0/0). No completions; cycle-four 16M at **71 m elapsed with the storm still running** (prior cycles' storms broke by now; walls were 74.2/71.3/73.0) — first sign of stretch, though still ≤ the 4.7 min/M envelope if it closes within ~5 m. Its overlapped 4M at 69 m = 17.51 so far (twin of the 17.64 case). Device 4.52 KB @ 241–243k r/s, 100% util, iowait 37.1%. cgroup high 2.31M, max 0. Trend: cycle four is at the cadence boundary; close now = stable fourth point, drag past ~78 m = the first bend. |
| 10:56 | t+7h08m, 504 GB / 5 SSTables. Gate ✓ (0/0). **Cycle four landed: 75.8 m = 4.78 min/M** — series **4.64 / 4.49 / 4.60 / 4.78** (6% total spread over four consecutive cycles). Starved 4M closed at **18.62** (starvation completions 17.64 / 10.64 / 18.62); quiet-window 4Ms 1.01 / 2.50 / 1.27. Fifth 16M in flight 4 m; device currently in a large-request phase (94 KB @ 20–21k r/s, iowait 0.4%). cgroup high 2.48M, max 0. Trend: last three cycles read 4.49→4.60→4.78 — mild upward drift consistent with table growth (a monotone-three, which this campaign's rule says is not yet a trend); the cadence itself is intact and the 6 flag line is not threatened this cycle. |
| 11:26 | t+7h38m, 534 GB / 10 SSTables. Gate ✓ (0/0). Mid-cycle: fifth 16M at 34 m (2.17 min/M so far, on cadence), storm running (5.77 KB @ 215–221k r/s, 100% util, iowait 38.4%). No overlapped small build this cycle — no starvation event. cgroup high 2.75M, max 0. Trend: quiet interval, cycle five on the established cadence; its wall (~11:50) is the drift test — a fifth point above 4.78 confirms the climb, at/below holds the plateau. |
| 11:56 | t+8h08m, 552 GB / 13 SSTables. Gate ✓ (0/0). Cycle five still open at **64 m (4.06 min/M so far)** — storm running (5.68 KB @ 228k r/s, 100% util, iowait 41.8%); on the prior walls (71–76 m) it lands in the next ~10 m. No completions, no starvation events this interval. cgroup high 2.99M, max 0. Trend: cadence holding; the drift verdict slips to the next check — anything past ~78 m elapsed would itself be the drift signal. |
| 12:26 | t+8h38m, 564 GB / 15 SSTables. Gate ✓ (0/0). **Cycle five is 94 m in and still open — 5.95 min/M so far**, far past the 71–76 m envelope of cycles 1–4. The 11:56 pre-declared criterion ("still open past ~78 m is itself the answer") has fired: **the drift is real** — series 4.49→4.60→4.78→(≥6.0 pending). Storm unchanged (5.60 KB @ 224–230k r/s, 100% util, iowait 41.7%); no second build co-scheduled. cgroup high 3.22M, max 0. Trend: first confirmed cadence bend, at 564 GB against 38 GiB cache (14.8×); completion wall next check — the push condition (two consecutive completed ≥6 with signature) arms if the next cycle repeats. |
| 12:56 | t+9h08m, 582 GB / 18 SSTables. Gate ✓ (0/0). **THE LARGER CLASS IS HERE: a 63.61M-ordinal merge** (4× the 16M class, largest of the campaign) — pass took ~71 m at **67 µs/node** (5.6× solo norm, ran under cycle five's storm), base layer just beginning, 1.23 min/M-so-far from pass start. Meanwhile **cycle five is 124 m in = 7.85 min/M so far, still open** — its wall now confounded by the 63.6M co-running, but it will land ≥8, at the old collapse-era threshold. Device transitioning (87k r/s, mixed 4.0–5.8 KB, iowait 24.7%). cgroup high 3.44M, max 0. Trend: the run has moved into its decisive phase — cycle five's regression and the 63.6M's fate are now one coupled experiment; the 63.6M's wall (~2.31 ref at 9-src below-RAM) is the number the whole symmetric path waits on. |
| 13:26 | t+9h38m, 534 GB / 11 SSTables. Gate ✓ (0/0). **Cycle five landed: 132.9 m = 8.38 min/M** — series 4.64 / 4.49 / 4.60 / 4.78 / **8.38**; at the old collapse-era threshold, but confounded end-to-end by the 63.6M co-tenant (its 71-min pass + base layer ran alongside), so it reads as "drift + co-tenancy", not clean drift. **The 63.6M**: base layer 11,540/123,941 batches (9.3%), 1.70 min/M-so-far from pass start; base-layer pace ≈6.2 min/M projecting a ~7–8 h merge if it holds. iowait 61.8% (second-highest), 6.0 KB @ 220k r/s, 100% util. cgroup high 3.70M, max 0. Trend: one completed ≥6 is on the books (confounded); the push condition arms fully on the 63.6M — its landing is the run's verdict either way. |
| 13:56 | t+10h08m, 558 GB / 15 SSTables. Gate ✓ (0/0). **63.6M base layer at 21.0%** — steady at 0.240–0.252M ord/min (**~4.0 min/M base-layer pace**, drifting only 3.96→4.17 across the hour); remaining ~3.3 h at pace, projected total wall ~5.5–6 h ≈ **5.2–5.7 min/M** vs the 2.31 below-RAM ref. It owns the machine now: 6.05 KB @ 213–214k r/s, 100% util, iowait 35.8%. cgroup high 3.97M, max 0. Trend: the large merge is running the same storm as the 16M class at nearly the same per-ordinal pace — early evidence the above-RAM cost scales ~linearly with size rather than super-linearly, which would be the best available outcome for the symmetric path. |

### 14:30 UTC — t+10h42m — 63.6M merge 35.4%, base layer quickening to ~3.6 min/M

- Table: 625.3 GB live / 19 SSTables. Client 3089591 alive; ingest at 7.1M rows into Partition(10/18) (p99 insert 59 s — servo throttled hard).
- **63.6M merge (123,941 b × 513 ord/b): 43,890 b (35.4%)** at 14:29:51. Interval rate 13:56→14:30 = **549 b/min ⇒ 3.55 min/M** base-layer clip (was ~508 b/min / ~4.0 earlier) — mildly accelerating, mirroring the early-run pattern where per-merge rate improves as the source hot set settles. Remaining base ~80.0k b ⇒ base ends ~16:58; land ~17:15–17:45.
- Completed merges this interval: none. Ordinal passes this interval: none (nothing new started; both build slots presumably held by the 63.6M + a queued peer).
- Device: 201–205k r/s @ ~6.0 KB, 100% util, iowait 40–43% — the storm signature persists, yet the merge rate *rose*; storm is load-bearing for ingest+merge combined, not choking the merge.
- Cgroup: anon 106.8G / file 36.2G, memory.events high=4.25M (normal), **max=0**. Cache cap holding.
- Gate: **0 'Cluster path cost' lines since 03:48**; 0 read-back assertions; no new REORDERED lines (none expected — no merge started this interval).
- Trend: single-story interval — the largest-ever merge grinding through its base layer slightly *faster* than the last window despite full-tilt device saturation; ~4.9–5.4 min/M projected total, comfortably linear vs the 16M class.

### 14:56 UTC — t+11h08m — 63.6M merge 46.3%, base clip steady ~3.8 min/M

- Table: 638.3 GB live / 21 SSTables (+13.0 GB, +2 since 14:30 — ingest still flushing). Client + Cassandra up.
- **63.6M merge: 57,360 b (46.3%)** at 14:56:13. Interval 14:30→14:56: 13,470 b in 26.4 min = **511 b/min ⇒ 3.81 min/M** — the 549 spike cooled slightly; base clip is oscillating in a tight 3.55–4.0 band. Remaining base ~66.6k b ⇒ base ends ~17:04, land ~17:20–17:50.
- Completed merges / ordinal passes / new REORDERED this interval: none — window still wholly owned by the 63.6M.
- Device: 195–205k r/s @ 6.0 KB, 94–100% util, iowait 42.5% — unchanged storm signature.
- Cgroup: anon 106.8G / file 36.2G, max=0.
- Gate: 0 cluster-cost lines since 03:48; 0 assertions.
- Trend: flat-and-healthy — half the base layer done at a stable ~3.6–3.8 min/M clip under full device saturation; no drift in either direction over two consecutive windows.

### 15:15 UTC addendum — IO attribution under saturation (thread-dump capture)

User asked which call paths carry the 200k r/s @ 6 KB / 100% util load. 3× jcmd Thread.print at 15:14 (docs/captures/iosat-20260826-1514/): 120 RUNNABLE FJP samples —
- **63% `gatherFromOtherSource` → `GraphSearcher.searchOneLayer` → `FusedPQDecoder.enableSimilarityToNeighbors` → `FusedPQ$PackedNeighbors.readInto` → `getPackedNeighbors` → `readFully`** — per-visit fused adjacency+PQ row reads of the cross-source symmetric searches (the T×(k−1) term itself).
- **12% `gatherFromOtherSource` → `rescore` → `getVectorInto` → `readFloats`** — exact FP rerank reads of returned candidates.
- **24% issuing `willNeedL0Record` fadvise hints** (FrontierPrefetchingView.hint + seed hints + gatherFromSameSource) — prefetch machinery IS active; workers block in readFully anyway (3-deep frontier can't hide ~100 µs at this miss rate — same conclusion as the 08-23 collapse capture, different call site: symmetric search now, clusterSearchL0 then).
- pidstat: all ~1.2 GB/s is FJP workers (~37 × 32 MB/s); ingest/flush IO negligible. 9 samples WAITING in acquireBuildPermit = co-scheduled builds starved (known).
- Zero clusterSearchL0/clusterFallbackSearch frames — gate corroborated at the stack level.
- Interpretation: this is the *bounded* IO-bound regime the cache cap was built to provoke (working set ≈ sources' fused rows ≈ hundreds of GB vs 36 GiB file cache ⇒ ~every expansion misses), not the 39–46 b/min collapse — progress holds ~3.8 min/M with the device at capacity. The dominant 63% path is the structural cost target: fewer/cheaper cross-source visits (O-track), wider latency hiding, or cache.

> Deep-dive published: `docs/analysis/vector-compaction-readpath-20260826.md` — full call-path→read-forcing analysis with measured 15.86M-merge anatomy and drawio cost diagrams (`docs/analysis/figures-readpath-20260826/`).

### 15:28 UTC — t+11h40m — 63.6M merge 59.3%, clip locked at ~3.9 min/M

- Table: 651.3 GB live / 23 SSTables (+13.0 GB, +2 since 14:56). Client + Cassandra up.
- **63.6M merge: 73,440 b (59.3%)** at 15:28:19. Interval 14:56→15:28: 16,080 b in 32.1 min = **501 b/min ⇒ 3.89 min/M** — third consecutive window inside 3.55–4.0; the clip is locked. Remaining base ~50.5k b ⇒ base ends ~17:08, land ~17:25–17:55.
- Completed merges / ordinal passes / REORDERED this interval: none — window still owned by the 63.6M.
- Device: 191–195k r/s @ 6.0 KB, 100% util, iowait 45.6% — unchanged; per the new read-path analysis this is ~40 workers × 1 outstanding cold expansion read each (docs/analysis/vector-compaction-readpath-20260826.md).
- Cgroup: anon 106.8G / file 36.1G, max=0.
- Gate: 0 cluster-cost lines since 03:48; 0 assertions.
- Trend: no news is the news — flat 3.9 min/M through 59% of the largest-ever base layer; projected total ~5.0–5.5 min/M, linear-scaling verdict on track for the ~17:30 landing.

### 15:56 UTC — t+12h08m — 63.6M merge 70.1%, fourth flat window

- Table: 664.2 GB live / 25 SSTables (+12.9 GB, +2 since 15:28). Client + Cassandra up.
- **63.6M merge: 86,930 b (70.1%)** at 15:56:13. Interval 15:28→15:56: 13,490 b in 27.9 min = **483 b/min ⇒ 4.03 min/M** — fourth consecutive window in the 3.55–4.03 band. Remaining base ~37.0k b ⇒ base ends ~17:11, land ~17:30–18:00.
- Completed merges / ordinal passes / REORDERED this interval: none.
- Device: 192–194k r/s @ 6.0 KB, 100% util, iowait 44.6% — unchanged.
- Cgroup: anon 106.8G / file 36.0G, max=0.
- Gate: 0 cluster-cost lines since 03:48; 0 assertions.
- Trend: unchanged for a fourth window — 70% through the base layer at a locked ~3.9±0.25 min/M clip; next check should catch the base-layer finish and the start of upper layers.

### 16:26 UTC — t+12h38m — 63.6M merge 81.8%, base layer in its final hour

- Table: 677.2 GB live / 27 SSTables (+13.0 GB, +2 since 15:56). Client + Cassandra up.
- **63.6M merge: 101,340 b (81.8%)** at 16:26:17. Interval 15:56→16:26: 14,410 b in 30.1 min = **479 b/min ⇒ 4.06 min/M** — fifth consecutive in-band window. Interval rates 549→511→501→483→479: a mild monotone drift (−13% over 2h) consistent with the table growing +50 GB under the fixed cache cap, not a regime change. Remaining base ~22.6k b ⇒ base ends ~17:13, land ~17:30–18:00.
- Completed merges / ordinal passes / REORDERED this interval: none.
- Device: 183–186k r/s @ 6.0 KB, 100% util, iowait 43.2%.
- Cgroup: anon 106.8G / file 36.0G, max=0.
- Gate: 0 cluster-cost lines since 03:48; 0 assertions.
- Trend: base layer entering its final hour at a stable clip; the only movement is a slow few-percent-per-hour rate decline tracking table growth — next check lands mid upper-layers or at TERMS_DATA.

### 16:56 UTC — t+13h08m — 63.6M merge 93.0%, base layer finishing ~17:15

- Table: 683.6 GB live / 28 SSTables (+6.4 GB, +1 since 16:26). Client + Cassandra up.
- **63.6M merge: 115,230 b (93.0%)** at 16:56:16. Interval 16:26→16:56: 13,890 b in 30.0 min = **463 b/min ⇒ 4.20 min/M** — sixth in-band window; the slow decel continues (549→…→463, −16% over 2.5h, tracking table growth under the fixed cache). Remaining base ~8.7k b ⇒ base ends ~17:15; upper layers + write-back after ⇒ **TERMS_DATA ~17:40±20m**.
- WALL projection (ordinals-assigned 12:49:48 → land): ~290–310 min ⇒ **~4.6–4.9 min/M** — same band as the 16M class (4.5–4.8), i.e. near-linear size scaling across a 4× ordinal span; ~2× the 2.31 cluster-ON small-merge reference.
- Completed merges / ordinal passes / REORDERED this interval: none.
- Device: 183–186k r/s @ 6.0 KB, 100% util, iowait 37.4%.
- Cgroup: anon 106.8G / file 35.9G, max=0.
- Gate: 0 cluster-cost lines since 03:48; 0 assertions.
- Trend: final stretch of the largest-ever base layer with the clip still in band; next check should contain the landed wall verdict.

### 17:30 UTC — t+13h42m — ⚠ POOL DEADLOCK: 63.6M merge wedged at 99.9992%, all vector index work frozen since ~17:18

- **The 63.6M merge froze at 123,940/123,941 batches (17:17:40) — not a straggler: a genuine ForkJoinPool-wide deadlock.** Device idle (1.5k r/s vs 190k during the merge). Full forensics: `docs/captures/deadlock-20260826-1726/`.
- **The cycle** (thread dump 17:26): `CompactionExecutor:51` (the co-running 24.9 GB compaction's inline index build, `SSTableIndexWriter.addRow → SegmentBuilder.addInternalAsync`) hit the mid-build PQ retrain at `CompactionGraph.maybeAddVector:358` → `ProductQuantization.refine → extractTrainingVectors → BoundedParallelExecutor.forEachInt → Drain.settle` — fanning training tasks into the shared FJP **while holding/queued for the `trainingLock` WRITE lock**. All **40 FJP workers** are parked at `CompactionGraph.addGraphNode:434` on `trainingLock.readLock()` (lock `0x772260f43880`, NonfairSync: queued writer blocks new readers). The training tasks are queued behind 40 parked workers → circular wait, total pool deadlock.
- **Victims**: 63.6M merge (`CompactionExecutor:38`) starved in `CompactionSort.sort → forEachInt` (`compactLevels:1462`) — it needed pool workers for its final batch bookkeeping; 5 threads queued in `acquireBuildPermit`; all flush segment builds parked; 8 compactions pending behind. Memtable-side inserts (`CassandraOnHeapGraph.add` on Native-Transport threads) still run — ingest continues until flush backlog bites, then the client's new windowed saturation breaker gets its first live test.
- **Attribution**: new-code surface — `BoundedParallelExecutor` (untracked WIP) + `CompactionGraph` trainingLock + `InsertFanout`; the deadlock needs a mid-build retrain to coincide with a read-locked-saturated pool, which 13.7h of storm-saturated co-scheduling finally produced. The jvector side and the symmetric-path result are NOT implicated: five flat windows at 3.9±0.3 min/M through 99.99% of the base layer stand.
- Gate: 0 cluster-cost lines; 0 assertions; memory.events max=0; client + Cassandra processes up (but the vector pipeline is dead).
- **Decision needed (pushed 17:30)**: restarting Cassandra breaks the deadlock but forfeits the 63.6M merge (4.7h of work, restarts from zero on next trigger); leaving it wedged blocks the run. The fix class is code, not config: refine() must not fan into the pool it blocks (train on the caller thread, or acquire the write lock only after extraction, or use a dedicated executor).
