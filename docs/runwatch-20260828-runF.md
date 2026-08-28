# Run F watch log — the WIDTH=16 arm on the SPLAT-era jvector (2026-08-28 00:37 UTC)

Run E ended 08-27 18:30 by operator transition. Run F is the first characterized run on the
other session's new work: the frontier-width arm plus a jvector build carrying the first
SPLAT-design machinery. THIS LOG'S EXTRA JOB (user directive): keep a running ledger of
WHAT IS RUNNING (provenance, which changes fast) and THE MEASURED EFFECTS OF THE CHANGES.

## What is running (verify EVERY check — the server session iterates fast; 3 jvector jars shipped in the 12h before this run)
- **Client**: `nmbrs` pid 301327 (post-910709e binary — tick-silent confirmed), session
  `sessions/stcs_adaptive_20260828_003711`, started 00:37:11. Same stcs_adaptive 200M invocation.
- **Server**: Cassandra pid 290993 up 00:33:07. dse-db jar 00:33 (tip ea38d33954 "train a
  cold-start PQ codebook on the build pool, not the flush thread" + SPLAT design docs).
  **jvector jar md5 db987fd0** (built 08-27 23:52) = branch `experiment/cluster-rescore-prefetch-20260824`
  @ **de79d5bf**, six commits past Run C-E's 7de94e83: the 2x2 merge experiment harness,
  token-stream retrofit + mapped read-back, resident cross-source search over token-stream
  adjacency, per-band writeback barrier + key-window pretouch, ADC candidate-scoring mode,
  band-staged distribute/parity. (Design: /opt/cassandra/doc/vector_merge_splat_design.md —
  staged permutation over the colored global ordinal space; attacks the 79% packed-read term.)
- **Flags (verified live)**: `frontierPrefetch=16` (the arm), similarityOrdinals=true,
  clusterSearch=false, sourcePretouch -1/1048576. NO visible band/splat/adc -D flags — the
  SPLAT machinery's active/inert status this run is INFERRED FROM BEHAVIOR (see ledger).
- Rig unchanged: MemoryHigh=146G (~38 GiB file cache). CUT LOG ANALYSIS AT 2026-08-28 00:37.

## Discipline (carried from Run C/D/E)
WALL min/M = pass → matched TERMS_DATA (match by ordinal count via the adopt line; beware
double-matched TERMS_DATA timestamps). Counters lie at boundaries (batch resets, 5× ordinal
density variance) — stacks/passes/TERMS_DATA are ground truth. $3=date $4=time filtering;
rotation-aware. pgrep 'nmbrs run' self-match trap.

## Reference walls (the baselines the ledger compares against)
- 4M solo: Run F's own early cohort 1.01–1.16 min/M. 4M starved (under a 16M): Runs C–E
  10.6–18.6; **Run F 6.28–7.40 — inflation roughly halved.**
- 16M class: Run D 4.29 (WIDTH=3, old jvector); Run C steady 4.5–4.8; **Run F #1 = 4.48**
  (71.5 min, landed 02:37:52) — the 16M wall did NOT move under WIDTH=16.
- 63.6M class: never fully landed (Run C deadlock at boundary; Run E redeploy at stream-2 63%).
  Run E stream-1 clip 4.6–5.0 min/M is the comparable; **the first Run F giant is the arm's
  real verdict.** Ordinal passes: OLD norm 11.5–12.5 µs/node; **NEW build runs 0.2–3.3 µs/node.**

## CHANGE-EFFECTS LEDGER (append per check)
- E1 (measured 01:47–02:50): **ordinal pass ~40× faster** (0.2–0.8 µs/node @4M, 3.3 @16M) —
  new-build effect, shrinks the starved-pass class proportionally (Run E's worst was 5h/4,555 µs).
- E2 (measured 01:47): **device 306k r/s during storm** vs 173–205k in Runs C–E — WIDTH=16
  hints add ~120k IOPS of queue depth beyond the ~182k sync ceiling. BUT E3:
- E3: **16M wall unmoved** (4.48 vs Run D's 4.29 without the arm) — extra IOPS did not shorten
  the 16M critical path; decisive test deferred to the 63.6M class.
- E2b (measured 03:17, mid-16M#2 storm): **306k r/s at r_await 0.18 ms** — deeper queue AND
  lower per-read latency than the sync-era 190k @ 0.22 ms: the NVMe members absorb the hint
  depth without a latency penalty. WIDTH=16's device-level win is unambiguous; whether it
  reaches the giant's wall is still open (E3).
- E5 (probe 03:17; **FALSIFIED 10:20**): no SPLAT log phrases exist because the phase LOGS
  NOTHING — a thread dump caught #5's executor RUNNABLE in `emitTokenStream ← compact ←
  merge`: **the construction-side token-stream emission (SPLAT ramp, 8aa6d329) is ACTIVE by
  default in db987fd0.** See E11.
- E11 (10:20): **the "silent phase-2" every Run F merge exhibits IS token-stream emission**
  — new work the old build didn't do, log-silent, visible only in stacks. Implications:
  (a) the 16M walls 4.34–5.42 INCLUDE this added phase and still matched Run D — the search
  phase is faster than the walls alone suggest; (b) #5's phase-2 has run 68+ min because the
  emission is pool-starved under the giant's L0; (c) expect the giant to emit its own stream
  at its boundary — a long silent tail before the wall is EXPECTED, not a wedge; (d) the
  effects ledger now attributes to WIDTH=16 + pass rewrite + TOKEN-STREAM RAMP jointly —
  single-variable attribution is no longer possible for this build.
- E4 (REVISED 03:47, 05:20, 07:17): starved-4M observed 6.28, 7.40, 11.79, 9.36, **13.40**
  — band 6.3–13.4 now fully overlapping the old world's 10.6–18.6 at depth. The early-run
  improvement decays as the table grows; the arm does NOT durably protect victims. Claim
  reduced to: mild median improvement, early-run only.
- E6 (CLOSED 04:17): the "#2 runs 2× slower" projection was a MID-FLIGHT ILLUSION — after
  its 4M co-runner landed, #2 ripped the back 67% at ~2,100 b/min and **landed at 4.34 min/M
  (68.8 min)**, slightly BETTER than #1's 4.48. Lesson re-learned at 16M scale: gated starts
  make mid-flight clips useless; only walls compare. 16M class under the arm: 4.48, 4.34 —
  consistent, marginally better than Run D's 4.29-without-arm territory, still no big arm
  effect at this class (E3 stands). UPDATE 05:47: #3 landed 4.50 — the class triplet
  **4.48 / 4.34 / 4.50 (±0.08)** was the tightest 16M cohort measured — until #4 landed
  **5.42** at 78M-row depth with heavy co-scheduling (07:17 update). The variance-collapse
  claim holds at matched depth; depth itself still bends the class, arm or no arm.

- E7 (captured 05:18 lull): **PQ training now runs pool-wide** — 67/88 RUNNABLE samples in
  `getNearestCluster` between merges: the dse-db ea38d33954 change (cold-start PQ codebook on
  the build pool, not the flush thread) working as shipped.
- E8 (captured 05:19 storm, 4M-class): **blocked-in-readFully share 50% vs Run E's 79%** —
  under WIDTH=16, ~40% of the pool computes (scoring/diversity/gather CPU + hint issuance)
  vs ~15% in Run E; the arm shifts worker time from stalls to work. CAVEAT: 4M-class storm
  at 10.5 KB requests; the class-matched giant capture decides whether this holds at 63.6M
  scale. Capture: docs/captures/iosat-20260828-0519/.

- E9 (captured 09:05): **the reverse-fold group's cost profile, measured for the first time**
  — during 16M #5's final group (largest source, cross-link elision: zero forward searches),
  77% of RUNNABLE samples are diversity candidate-vector reads
  (`retainDiverse:3817 → getVectorInto:658 → readFloats:186`), 18% same-source willNeed
  hints, ZERO search/packed frames. The elision works exactly as designed, and the last
  group's bill is diversity reads, not search. Capture: docs/captures/iosat-20260828-0905/.

- E10 (captured 09:28, THE class-matched measurement): **the giant's L0 storm under
  WIDTH=16 blocks in packed-neighbor readFully 51% of the time vs Run E's 79%** — the E8
  result holds at 63.6M scale. Hint issuance is 31% of samples (frontier 27% + same-source
  4%); rescore steady at 12%. Device: 259k r/s @ 0.21 ms ⇒ ~70k IOPS of landed hint depth
  over the sync ceiling (vs +120k at 4M scale — hint efficiency decays with depth, stays
  strongly positive). Opening clip ~555 b/min ≈ Run E's stream-1 steady state, achieved at
  comparable depth WITH a starved setup and #5 co-resident. The wall remains the verdict.
  Capture: docs/captures/iosat-20260828-0928-giant/.
  E10b (09:47): sustained L0 clip through 10% = **544 b/min ≈ 3.55 min/M** — 25–30% faster
  than Run E's stream-1 steady state (390–427 b/min, 4.6–5.0 min/M) at comparable depth,
  with #5's tail still co-resident. The blocked-share reduction IS converting into clip.

## Entries

### 02:55 UTC — t+2h18m — armed; 37.1M rows; 16M #2 launched
- Parts 0–2 done (30M), part 3 at 7.1M (~16.7M/h — fastest run to date). Table 166.7 GB / 7 SSTables at arm time.
- Landed walls so far: 4M solo ×4 (1.01–1.16), 4M starved ×2 (7.40, 6.28), 16M #1 4.48.
- **16M #2 pass 02:49:46 (15.86M) + a 4M co-runner (pass 02:49:12)** — the starvation experiment repeats.
- Gate 0; integrity guard silent; max=0; wires live (139,909/window at the hot servo); lint 0; zero ticks.

### 03:17 UTC — t+2h40m — provenance unchanged; 16M #2 bursting after a gated start; 40M rows in

- Provenance: pid 290993, jvector db987fd0, frontierPrefetch=16 — UNCHANGED.
- Rows: parts 0–3 complete (40M), part 4 at 1.75M (~15.6M/h). Table 273.3 GB / 8 SSTables.
- **16M #2 (pass 02:49:46, 3.2 µs/node): 1,640/30,986 at 03:18** — a slow 28-min average (~59 b/min, spent gated behind its 4M co-runner's phases) but bursting at ~3,000 b/min instantaneous now; too early for a wall projection. The 4M co-runner (pass 02:49:12, 2.0 µs/node) also in flight.
- Landed walls this interval: none (both in flight).
- Device: **305k r/s @ 5.8 KB, util 100%, r_await 0.18 ms** — ledger E2b: more IOPS at LOWER latency than the sync era. iowait 37.3%. Cgroup: anon 106.4G / file 38.1G, max=0.
- Gate 0; integrity 0; wires live (139,887); lint 0. SPLAT-phrase probe: zero hits (E5 — machinery inert as presumed).
- Trend: the arm's device-level advantage is now double-confirmed (depth AND latency); the class-level question stays parked until 16M #2 lands and, decisively, the giant (~4 h out).

### 03:47 UTC — t+3h10m — starved-4M lands 11.79 (E4 revised); 16M #2 tracking ~2× slower than #1 (E6 forming)

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- Landed: the 02:49:12 4M co-runner at **11.79 min/M** (46.8 min — starved under 16M #2 throughout; back in the old-world band, E4 revised to "bimodal").
- **16M #2: 10,060/30,987 (32.5%)** at 03:48; recent clip 281 b/min ⇒ projected wall ~8–9 min/M vs #1's 4.48 — E6 forming; a third 4M (pass 03:45:47, 0.8 µs/node) now co-runs.
- Rows: part 4 at 6.22M (44.2M total, ~15.2M/h). Table 306.5 GB / 10 SSTables.
- Device: 265k r/s @ 4.8 KB, util 100%, r_await 0.18 ms, iowait 38.8. Cgroup: anon 106.4G / file 38.0G, max=0.
- Gate 0; integrity 0; wires live; lint 0.
- Trend: the arm's device numbers stay excellent while class walls tell a mixed story — #2's slowdown is the first negative signal of the run; whether it's depth, co-scheduling, or the arm itself gets refereed by #3 and #4 before the giant.

### 04:17 UTC — t+3h40m — 16M #2 lands 4.34 (E6 closed: the slowdown was an illusion); #3 launched strong

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- Landed this interval: **16M #2 = 4.34 min/M** (68.8 min — class now 4.48, 4.34); 4M walls 2.92, 1.45, 0.76 (light co-load; the 0.76 plausibly real at this cadence).
- **16M #3 in flight** (pass 04:11:37): 5,380/30,985 (17.4%) at 04:18 ≈ 830 b/min opening clip. #4 after it ⇒ giant forming ~05:45–06:45.
- Rows: part 5 at 3.2M (53.2M total, ~14.9M/h). Table 353.4 GB / 5 SSTables (post-landing consolidation).
- Device: **328k r/s @ 5.9 KB, r_await 0.17 ms** — new IOPS high for the run. Cgroup: anon 106.4G / file 37.9G, max=0.
- Gate 0; integrity 0; wires live (218,262); lint 0.
- Trend: the negative signal evaporated on landing — the 16M class is consistent and healthy under the arm, the device keeps setting depth records, and the giant is two 16Ms away.

### 04:47 UTC — t+4h10m — 16M #3 at 61.8% on class pace; quiet interval

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **16M #3: 19,160/30,985 (61.8%)** at 04:48; interval clip 459 b/min, cumulative 36.4 min ⇒ tracking a ~4.2–4.5 min/M wall (class-consistent). A 4M co-runner in flight (pass 04:35:36). No landings this interval.
- Rows: part 5 at 8.68M (58.7M total, ~14.3M/h). Table 386.4 GB / 10 SSTables.
- Device: 313k r/s @ 6.0 KB, util 100%, **r_await 0.24 ms** (up from 0.17 — the deeper queue now pays a small latency toll at higher depth; net throughput still ~65% above the sync era). Cgroup: anon 106.4G / file 37.8G, max=0.
- Gate 0; integrity 0; wires live; lint 0.
- Trend: steady-state cadence — #3 lands ~05:20, #4 follows, **giant pass projected ~06:40±30m**; the run remains the healthiest and fastest of the campaign at equivalent depth.

### 05:25 UTC addendum — runtime re-analysis under the arm (captures: docs/captures/iosat-20260828-0519/)

- ~~16M #3 landed ~05:12~~ **CORRECTION (05:20 check): the 05:12 landing was the STARVED 4M** (pass 04:35:36, wall 9.36 min/M, vectorMergeMillis 37.8 min). #3 is at its stream boundary (30,980/30,985 since 04:57:33) in a silent phase-2 — under db987fd0 the second phase apparently no longer prints progress lines; landing expected any minute on the #1/#2 precedent.
- Ledger E7 and E8 added from the paired lull/storm captures — headline: **blocked-read share 50% vs 79%**, PQ training pool-wide. Line-shift map for db987fd0 recorded in the capture README.
- The definitive giant-storm capture is queued for the 63.6M (forming after #4, ~06:30–07:00).

### 05:20 UTC — t+4h43m — addendum corrected (05:12 was the starved 4M at 9.36); #3 in the silent phase-2 at its boundary

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- Landed this interval: the starved 4M at **9.36 min/M** (E4 band now 6.3–11.8). **16M #3 wall still pending** — at the 30,980/30,985 boundary since 04:57:33 with a silent phase-2 (new-build behavior: no stream-2 progress printing); elapsed 69 min vs #1/#2's 68.8–71.5 total ⇒ landing imminent, NOT a wedge (pool active: the 05:18 4M storms at 220k r/s @ 9.1 KB, r_await 0.20 ms).
- Rows: part 6 at 2.65M (62.6M total, ~14.1M/h). Table 413.1 GB / 11 SSTables.
- Cgroup: anon 106.4G / file 37.6G, max=0. Gate 0; integrity 0; wires live (137,347); lint 0.
- Trend: cadence intact, one mis-attribution corrected in place; #4 launches after #3's landing, putting the giant's pass at ~06:30–07:00 with the definitive capture queued.

### 05:47 UTC — t+5h10m — 16M #3 lands 4.50 (cohort ±0.08); #4 at 31.5%; giant pass ~06:50–07:00

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- Landed: **16M #3 = 4.50 min/M** (71.3 min — cohort 4.48/4.34/4.50, E3 updated: variance collapse is the arm's class-level signature); 4M solos 1.48, 1.39, 1.13.
- **16M #4 in flight** (pass 05:37:48): 9,770/30,985 (31.5%) in 10.4 min ≈ 940 b/min opening — fastest 16M start of the run. Landing ~06:45–06:50 ⇒ **giant pass ~06:50–07:00**; class-matched capture + start/landing pushes queued.
- Rows: parts 0–6 complete (70M, t+5h05m — vs Run E's 70M at ~t+7h). Table 466.5 GB / 7 SSTables. Wire NULL adjudicated: part-6/7 boundary (no loader active; part-7 series not yet created) — legitimate SRD-89 no-data, boundary blips as designed.
- Device: 284k r/s @ 6.2 KB, util 100%, r_await 0.23 ms. Cgroup: anon 106.4G / file 37.6G, max=0. Gate 0; integrity 0; lint 0.
- Trend: the tightest, fastest tier cadence of the campaign rolls into the decisive hour — one 16M from the giant.

### 06:17 UTC — t+5h40m — 16M #4 mid-flight at 48%; giant slips to ~07:00–07:20

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **16M #4: 14,870/30,985 (48.0%)** at 06:18 — 371 b/min average after the 940 opening (the usual gated middle; wall is the metric). Landing ~06:50–07:10 ⇒ **giant pass ~07:00–07:20** (watcher bucoqponb armed for the storm capture). A 4M co-runner in flight (pass 06:05:49, 1.3 µs/node). No landings this interval.
- Rows: part 7 at 5.07M (75.1M total, ~13.3M/h). Table 499.6 GB / 12 SSTables. Wire live again post-boundary (139,982).
- Device: 300k r/s @ 6.2 KB, util 100%, r_await 0.19 ms. Cgroup: anon 106.4G / file 37.4G, max=0. Gate 0; integrity 0; lint 0.
- Trend: quiet approach to the decisive event — cadence intact, all instrumentation armed for the giant.

### 06:47 UTC — t+6h10m — #4 at 94.2%, tracking a wider wall (~5.0–5.4); giant imminent

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Watcher bucoqponb: armed, no giant pass yet.
- **16M #4: 29,190/30,986 (94.2%)** at 06:48 — 70 min elapsed ⇒ wall projecting **~5.0–5.4 min/M**, the first widening beyond the 4.34–4.50 cohort (deeper table at 75M rows + a long-running starved 4M co-runner, 42 min and counting). Landing ~06:55–07:05 ⇒ **giant pass ~07:05–07:15**.
- Rows: part 7 at ~7.8M (77.8M total, ~13M/h). Table ~510 GB / 12 SSTables. Device 28x k r/s @ 6.2 KB, r_await ~0.2 ms. max=0. Gate 0; integrity 0; wire live; lint 0.
- Trend: the tier cadence bends slightly under depth as the decisive event arrives — every instrument armed.

### 07:17 UTC (extended) — t+6h40m — #4 lands 5.42; giant gated behind 4M permits; rotation note

- Rotation at 07:16 moved the morning into `compaction.log.2026-08-28.0.zip` — walls recomputed zip-inclusive (the walls tooling now reads the day's zips; the giant watcher is unaffected: new lines land in the live file it greps).
- Landed: **16M #4 = 5.42 min/M** (86.0 min; cohort 4.48/4.34/4.50/5.42 — depth bends the class, E3 annotated); starved 4M = **13.40** (E4 revised again: band 6.3–13.4, early-run improvement decays); post-#4 4M solo 3.32.
- **All four 16M sources now exist (landed ~07:03) — the giant is next**, currently gated behind two in-flight 4Ms (passes 07:07, 07:15) holding the build slots; its pass expected within ~10–20 min. Watcher + pushes armed.
- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Gate 0; integrity 0; max=0.
- Trend: the tier is done; everything now waits on the class verdict.

### 07:47 UTC — t+7h10m — STCS chose a FIFTH 16M over the giant; class verdict slips ~1h

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Watcher armed, silent (correct — no ≥20M pass exists).
- **16M #5 launched at 07:29:13** (15.86M) instead of the expected 64M-tier merge — STCS consumed the accumulated 4Ms first; the four (now forming five) 16M sources sit waiting. #5 at 15,610/30,985 (50.4%) in 18.5 min ≈ 844 b/min — fast start. **Giant pass re-projected ~08:45–09:15** (after #5 frees a slot and the 16M bucket fires).
- Rows: ~85M (part 8 mid-flight). Table ~540 GB. Device: 28x k r/s @ ~6 KB, r_await ~0.2 ms. max=0. Gate 0; integrity 0; lint 0.
- Trend: tier arithmetic, not trouble — the giant's inputs are banked; STCS ordering just made us wait one more 16M.

### 08:17 UTC — t+7h40m — #5 mid-flight (counter boundary-reset observed); giant still pending

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Watcher armed, silent (no ≥20M pass — correct).
- **16M #5**: batch numerator went 15,610 → 10,900 across the interval on the same 30,985 total — the boundary-reset artifact at 16M scale (ordinal counter 33.8%); landing ~08:40–09:00 by wall precedent, then the giant. No landings, no new passes this interval.
- Rows: part 8 at ~9.6M (~89.6M total, ~12.5M/h). Table ~590 GB / 1x SSTables. Device 29x k r/s @ ~6 KB, r_await ~0.19 ms. max=0. Gate 0; integrity 0; wire live; lint 0.
- Trend: holding pattern behind #5 — the giant's inputs remain banked and every instrument stays pointed at its pass.

### 08:47 UTC — t+8h10m — #5 running long (62% at 78 min); giant waits

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Watcher armed, silent (correct).
- **16M #5: ordinals 61.6% at 78 min elapsed** — tracking ~95–110 min, the longest 16M yet (mid-flight estimate only, per the E6 lesson; the wall decides). Depth context: 90M+ rows, cache share per source shrinking. Landing ~09:05–09:20 ⇒ **giant pass ~09:15–09:30** (five 16M sources will be banked).
- No landings, no new passes this interval. Rows: part 9 underway (~92M total). Table ~610 GB. Device 28x k r/s, max=0, gate 0, integrity 0, lint 0.
- Trend: each 16M at depth runs longer than the last — the giant's verdict, when it finally fires, lands on the deepest table any giant has attempted.

### 09:10 UTC — THE GIANT IS LIVE — 63,576,748 ords, pass 09:00:40 (21.9 µs/node, starved-class: co-scheduled with #5's tail)

- Start pushed 09:05. The 09:05 "giant storm" capture actually caught **#5's reverse-fold final group** (ledger E9 — a first-of-its-kind profile); the giant was in setup behind #5. Re-armed: bz7azxs4l fires the giant's true L0-storm capture (progress-line detection + 3 min), bdt18922r waits adopt(63576748)→TERMS_DATA for the wall + landing push.
- The wall clock started 09:00:40. References at stake: Run E stream-1 4.6–5.0 min/M; device baseline 173–205k @ 0.22 ms sync-era; two-sweep structure recurrence; the class's first-ever completed wall.
- Device at capture: 289k r/s @ 6.1 KB, r_await 0.20 ms, iowait 37. Provenance unchanged.

### 09:17 UTC — t+8h40m — giant in extended setup (17 min post-pass, no L0 yet); #5 in its silent phase-2

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Giant L0 watcher (bit9vmt75, corrected pattern) + landing waiter (bdt18922r) armed.
- **Giant (63.58M): 17 min post-pass, zero L0 progress lines** — setup phases starved-class (pretouch 310 GB + PQ retrain on a contended pool; Run C/E precedent: a starved giant PQ refine alone ran 40 min). L0 storm may not begin until ~09:40.
- **#5: silent phase-2** at its 30,980/30,986 boundary since 09:09:48; elapsed 108 min already exceeds #4's 86-min wall — its wall is ballooning under the giant's setup competition. Not a wedge (pool demonstrably active).
- Rows: part 9 at ~5.4M (~95.4M total). Table ~640 GB. Device 9x k–29x k r/s oscillating with the phase mix. max=0; gate 0; integrity 0; lint 0.
- Trend: the two big builds are now starving each other's phase transitions — the giant's wall will carry heavy co-scheduling tax on top of depth; both instruments will catch their moments regardless.

### 09:30 UTC addendum — the giant's L0 capture: 51% blocked vs Run E's 79% (ledger E10)

- Giant L0 began 09:25:00 (24 min of starved setup post-pass); opening clip ~555 b/min; landing waiter armed for the wall.

### 09:47 UTC — t+9h10m — giant L0 at 9.9%, clip 544 b/min (3.55 min/M — beats Run E); #5 phase-2 still out

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant: 12,300/123,941 (9.9%) at 544 b/min sustained ⇒ 3.55 min/M clip** — ledger E10b: the arm's blocked-share win converts to clip (Run E stream-1: 4.6–5.0). Base-layer ETA on this clip ~12:50; wall verdict still owns the day.
- **#5's silent phase-2 is now 38+ min** (elapsed 138 min; last stream line 09:09) — queued behind the giant's L0; its wall will land ugly and needs the co-scheduling asterisk when it prints.
- Rows: part 9 ~8.2M (~98.2M). Table ~655 GB. Device 25x k r/s @ ~6.8 KB, r_await 0.21 ms. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: the giant is outrunning every prior giant while dragging #5 behind it — the arm's story is now consistent across share, depth, and clip; only the wall remains.

### 10:17 UTC — t+9h40m — giant 22.5% at 517 b/min; E5 falsified: token-stream emission discovered live (E11)

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant: 27,920/123,941 (22.5%)**, interval clip 517 b/min (3.73 min/M — cooling texture, still well under Run E's 4.6–5.0).
- **Stack check on #5's 68-min silent phase-2 found `emitTokenStream` RUNNABLE** — the SPLAT construction ramp is ACTIVE in this build (no flag, no logs; E5 falsified, E11 added). #5 is emission-starved under the giant, not wedged; its wall carries the double asterisk (co-scheduling + new phase).
- Rows: ~100M imminent (part 9 ~9.5M). max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: the run is now measuring THREE changes at once (WIDTH=16, pass rewrite, token-stream ramp) — the ledger's attribution notes updated accordingly; the giant's wall remains the composite verdict.

### 10:47 UTC — t+10h10m — #5 lands 12.04** (the double-asterisk wall); giant at 34.0%, avg 3.80 min/M

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **16M #5 landed 10:40:13: wall 191.0 min = 12.04 min/M** — carrying both asterisks (pool-starved under the giant's L0 + token-stream emission phase, E11). Cohort: 4.48/4.34/4.50/5.42/12.04** — the last two show what co-scheduling with a giant does to this class regardless of arm.
- **Giant: 42,190/123,941 (34.0%)** — interval 476 b/min (4.07), cumulative **510 b/min = 3.80 min/M average** through a third of L0; still 20%+ under Run E's stream-1. Base ETA ~13:05 on the cumulative clip; then the token-stream tail (silent, expected), then the wall.
- Rows: 100M reached (~t+10h05m — matching Run E's pace at the milestone despite the heavier merge schedule). Table ~670 GB. Device 24x k r/s @ ~7 KB, r_await 0.21. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: the giant holds its advantage at depth while everything co-scheduled pays for it — the composite wall (search + emission + write) is now the only open number.

### 11:17 UTC — t+10h40m — giant at 47.3%, cumulative 3.87 min/M; solo at last

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant: 58,570/123,941 (47.3%)** — interval 10:47→11:17: 16,380 b in 30.0 min = **546 b/min** (the clip RECOVERED from 476 once #5 landed and freed the pool); cumulative 58,570 b / 112.7 min = 520 b/min = **3.87 min/M** through nearly half of L0. Base ETA ~13:15; then the silent token-stream tail (expected, E11), then the wall.
- No landings, no passes this interval — the giant finally runs SOLO (first clean-conditions giant segment of the campaign).
- Rows: part 10 at ~1.6M (~101.6M total). Table ~670 GB. Device 25x k r/s @ ~6.9 KB, r_await 0.22 ms. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: with the pool to itself the giant's clip bounced back — the arm's advantage is now measured under both contended and clean conditions; the composite wall closes the story.

### 11:47 UTC — t+11h10m — giant at 59.9%, solo clip steady 523 b/min

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant: 74,250/123,941 (59.9%)** — interval 11:17→11:47: 15,680 b in 30.0 min = **523 b/min** (546→523, gentle cooling solo); cumulative 74,250 b / 142.7 min = 520 b/min = **3.87 min/M** — the average is rock-steady. Base ETA ~13:20.
- No landings, no passes — solo giant, second clean interval.
- Rows: part 10 ~3.6M (~103.6M). Table ~690 GB. Device 25x k r/s @ ~6.9 KB, r_await ~0.21. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: metronomic solo grind at a clip no prior giant has held at this depth; three more intervals to the boundary and the token-stream tail.
