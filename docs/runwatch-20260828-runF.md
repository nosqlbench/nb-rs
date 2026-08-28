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
  reduced to: mild median improvement, early-run only. EXTREME (18:47): a 4M victim of
  the three-sweep giant landed at **79.3 min/M** (5.24 h parked; vectorMergeMillis 314.6
  min) — an all-time starvation record. The multi-sweep giant triples the monopoly window;
  victim protection needs scheduler work (permit priority/aging), not prefetch width.
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

- E12 (13:47): **the two-sweep structure RECURS under db987fd0 at giant scale — and this
  build PRINTS stream-2 progress** (counter reset to 9,020/123,941 after the ~13:32
  boundary; the 16Ms' phase-2 was silent, the giant's is not — scale- or path-dependent
  logging). Stream-2 opening clip ~570 b/min ≈ stream-1's — and ≈ Run E's stream-2
  (560–605): **the second sweep looked arm-INDEPENDENT** — but by 14:17 its clip rose to
  **795 b/min** (vs Run E's 560–605 ceiling): either partially arm-assisted after all, or
  density-variable by region. Wall estimate improved to ~16:15–16:45 — then a THIRD sweep appeared at 16:35 (see the 16:45 addendum): composite re-projected ~9.3–9.9 min/M with the added emission phase; per-phase accounting is the honest comparison — the arm's stream-1 win diluted by the sweep the arm can't
  touch. Six executors queue in acquireBuildPermit behind the giant; the next 16M's pass
  parks in joinAll — the starvation pattern repeats on schedule.

- E13 (19:43, THE CROWN MEASUREMENT): **the campaign's first complete 63.6M-class wall:
  pass 09:00:40 → TERMS_DATA 19:43:20 = 642.7 min = 10.11 min/M** (63,576,748 ords;
  vectorMergeMillis 688.6 min incl. pre-pass; TERMS_DATA body 320.5 GB). Full anatomy:
  setup 24.3 min (starved) · **stream-1 search 247 min = 3.89 min/M (the arm's win — Run E
  ran 4.6–5.0 on this segment)** · stream-2 183 min = 2.88 · stream-3 emission 115 min =
  1.81 · stream-4 47 min = 0.74 · upper/adopt tail 19.3 min · write-back 7.3 min (320 GB ≈
  730 MB/s). Reading: the search phase — the only phase prior runs ever measured — is now
  38% of the total; the new build's added sweeps (emission et al.) buy the SPLAT ramp at
  ~2.5 min/M of one-time cost per merge; and every phase after stream-1 is arm-blind, so
  further wall reduction belongs to the SPLAT staged rewrite, exactly as its design doc
  argues. Two 63.6M giants remain in the 200M cascade to confirm repeatability.

**E14 — Post-giant recovery curve (the giant's externality, priced).** After E13's landing (19:43), the six queued 4M merges land at 3.29 / 3.95 / 2.65 / 3.56 / 2.23 / 1.17 min/M — monotone decay back into the solo band within 40 minutes, and the ingest servo opens to a run-record 8.2M rows/h. So a giant's total cost = its own 642.7-min wall + ~40 min of 2–4× degraded 4M walls behind it — the starved-class horror walls (6.3–13.4, record 79.3) occur only when victims are co-resident DURING the monopoly, not after it. Scheduler-fix sizing: permit aging must protect co-residents mid-giant; post-giant drainage needs nothing.

**E15 — SPLAT staged machinery: ACTIVE all run, log-loud; E5/E11 falsified (21:55).** The staged-permutation pipeline (vector_merge_splat_design.md) has been running on every merge since 00:37 — 3,974 `Bands for source / Stage DISTRIBUTE / Stage FINALIZE / Cross-link` lines in rotated logs, all classes including the giant. Consequences: (1) E13's "streams 1–4" anatomy is the staged per-source pipeline (4 sources ≈ quarters of the ordinal space; bands=16 per source); (2) batch-clock totals re-denominate per stage (base 15,864,116 → DISTRIBUTE 3,966,042 for a 16M), which is the mechanical cause of every "batch reset"/"99.999% wedge" artifact this campaign has chased; (3) Run E→F deltas price the arm ON an active SPLAT substrate, not the arm alone — any pure-arm claim needs a SPLAT-off control; (4) the fleet of misreadings (E5 "inert", E11 "only token-stream, stack-visible only", 21:17 "nodetool rows honest") corrected in place. Detection failure mode for the record: watched for imagined shapes (lowercase 'band/stage/spill'), never grepped the actual INFO-line vocabulary.

  - *E15 addendum (22:18):* instrument model closed — batch counter is a per-source cycle (30,985/source, 4 per 16M); `Stage BASE_LAYER` is the merge-global ordinal clock; `Stage X completed ... in N ms` lines make per-stage anatomy retrospectively extractable for every landed merge including E13. All historical batch-clock paradoxes (resets, 66%@16%, end-at-50%) reduce to this structure.

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

### 12:17 UTC — t+11h40m — giant at 71.7%, average locked at 3.87

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant: 88,890/123,941 (71.7%)** — interval 11:47→12:17: 14,640 b in 30.1 min = **486 b/min** (523→486, the familiar depth-cooling); cumulative 88,890 b / 192.8 min = 461... recompute: 88,890/192.8 = 461 b/min?? — correction: cumulative = 88,890 b since 09:25:00 = 172.8 min ⇒ **514 b/min = 3.91 min/M average**. Base ETA ~13:25–13:30.
- No landings, no passes — third solo interval. Rows: part 10 at 5.31M (~105.3M). Table 705.2 GB / 25 SSTables. Device 244k r/s @ 6.9 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: gentle cooling inside a locked ~3.9 average — the giant remains on pace to beat every reference; boundary and token-stream tail next hour.

### 12:47 UTC — t+12h10m — giant at 83.2%; boundary ~13:30

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant: 103,140/123,941 (83.2%)** — interval 12:17→12:47: 14,250 b in 30.0 min = **475 b/min**; cumulative 103,140 b / 202.8 min = **509 b/min = 3.95 min/M average**. Remaining ~20.8k b ⇒ **boundary ~13:30**, then the silent token-stream tail (E11: expect tens of minutes, no logging), then adopt + TERMS_DATA = the wall.
- No landings, no passes — fourth solo interval. Rows: part 10 at 6.83M (~106.8M). Table 711.8 GB / 26 SSTables. Device 244k r/s @ 6.9 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: the cooling curve (546→523→486→475) mirrors Run E's giant exactly, but from a floor ~20% lower — the average will finish ~3.95–4.0 vs Run E's 4.6–5.0 stream-1; the wall then adds the emission tail that Run E never measured.

### 13:17 UTC — t+12h40m — giant at 94.5%; boundary in ~15 min

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant: 117,080/123,941 (94.5%)** — interval 465 b/min; cumulative 117,080 b / 232.8 min = **503 b/min = 3.96 min/M**. Remaining ~6.9k b ⇒ **boundary ~13:32**; then the token-stream tail (silent, expected: #1–#4 took 10–18 min uncontended, #5 took 3h starved — the giant emits ~4× #5's stream, solo, so estimate 30–60 min), then adopt → TERMS_DATA.
- No landings, no passes — fifth solo interval. Rows: part 10 at 8.16M (~108.2M). Table 718.4 GB / 27 SSTables. Device 241k r/s @ 6.9 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: stream-1 will close at ~3.96 min/M vs Run E's 4.6–5.0 — a ~15–20% arm win on the like-for-like segment, banked regardless of what the emission tail adds; wall projection ~14:15–15:00.

### 13:47 UTC — t+13h10m — boundary crossed: stream-2 live and PRINTING (E12); wall estimate widens

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-1 closed at the boundary ~13:32 (final average 3.96 min/M — the banked arm win). Stream-2 running: 9,020/123,941 at ~570 b/min** — recurrence confirmed, printing at this scale, clip ≈ Run E's stream-2 ⇒ arm-independent (ledger E12). Wall now projects **~17:10–17:45 if the sweep runs full-length** (composite ~7.7–8.3 min/M); earlier if the denominator misleads (it has before).
- Permit queue: 6 executors waiting; next 16M's pass parked in joinAll — the familiar debt accumulating for the post-landing burst.
- Rows: part 10 ~9.5M (~109.5M). Gate 0; integrity 0; max=0; wire live; lint 0.
- Trend: the run has cleanly split the merge into an arm-sensitive half (stream-1, 20% faster) and an arm-blind half (stream-2) — which is precisely the case for the SPLAT staged rewrite that targets the second half.

### 14:17 UTC — t+13h40m — stream-2 accelerates to 795 b/min (E12 revised); wall ~16:15–16:45

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-2: 32,880/123,941 (26.5%)** — interval clip **795 b/min** (570→795; exceeds Run E's stream-2 band — E12's "arm-independent" softened to "partially/regionally"). At this clip TERMS_DATA lands **~16:15–16:45**, composite wall ~**6.9–7.3 min/M**.
- Rows: part 10 at 10.67M (~110.7M). Table 738.2 GB / 30 SSTables. Device 241k r/s @ 6.7 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: stream-2 is outrunning its Run E counterpart too — the composite wall estimate improves each interval; the first complete 63.6M wall remains on track for late afternoon with the push armed.

### 14:47 UTC — t+14h10m — stream-2 at 44.4%, clip 736 b/min; wall holding ~16:20–16:45

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-2: 54,970/123,941 (44.4%)** — interval 14:17→14:47: 22,090 b in 30.0 min = **736 b/min** (795→736; still well above Run E's 560–605 band). Remaining ~69.0k b ⇒ **TERMS_DATA ~16:20–16:45**, composite wall tracking **~7.0–7.3 min/M**.
- Rows: part 10 at 11.82M (~111.8M). Table 744.8 GB / 31 SSTables. Device 235k r/s @ 6.7 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: both sweeps of this giant outrun their Run E counterparts — the composite estimate has stabilized inside a half-minute-per-M band across two checks; landing late afternoon as projected.

### 15:17 UTC — t+14h40m — stream-2 at 61.0%, clip steady 690 b/min; wall ~16:30–16:50

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-2: 75,660/123,941 (61.0%)** — interval 14:47→15:17: 20,690 b in 30.0 min = **690 b/min** (795→736→690, its own gentle cooling curve, still above Run E's band). Remaining ~48.3k b ⇒ **TERMS_DATA ~16:30–16:50**, composite ~**7.1–7.3 min/M**.
- Rows: part 10 at 12.89M (~112.9M). Table 751.4 GB / 32 SSTables. Device 235k r/s @ 6.8 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: both sweeps cooling on the same gentle slope, projections stable within minutes across three checks — the landing is a formality of time now.

### 15:47 UTC — t+15h10m — stream-2 at 76.7%; wall ~16:35–16:55

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-2: 95,120/123,941 (76.7%)** — interval 15:17→15:47: 19,460 b in 30.0 min = **649 b/min** (690→649, same slope). Remaining ~28.8k b ⇒ stream-2 ends ~16:32; upper layers + adopt + ~78 GB write-back ⇒ **TERMS_DATA ~16:45–17:05**, composite ~**7.2–7.4 min/M**.
- Rows: part 10 at 13.95M (~113.9M). Table 758.0 GB / 33 SSTables. Device 234k r/s @ 6.9 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: final descent — the projections have converged to a 20-minute window; landing push imminent.

### 16:17 UTC — t+15h40m — stream-2 at 91.6%; landing inside the hour

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-2: 113,520/123,941 (91.6%)** — interval 613 b/min (the cooling slope continues); remaining ~10.4k b ⇒ stream-2 ends ~16:35, then upper layers + adopt + write-back ⇒ **TERMS_DATA ~16:50–17:10**. Composite tracking **~7.25–7.45 min/M**.
- Rows: part 10 ~15.0M (~115M). Table ~764 GB. Device 23x k r/s. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: last quiet entry before the wall — the waiter owns the landing.

### 16:45 UTC addendum — a THIRD full-scale sweep began ~16:35 (counter reset to 6,090/123,943)

- The giant's structure under db987fd0 is (at least) THREE sweeps: search (stream-1, 3.96 min/M — the arm's win), the Run-E-style second sweep (13:32→~16:35 ≈ 183 min ≈ 2.88 min/M-equivalent), and now a third — most plausibly the token-stream emission (E11), silent at 16M scale but printing here. Opening ~780 b/min.
- **Wall re-projection: if stream-3 runs full-length at ~700–800 b/min, TERMS_DATA ~18:45–19:30 and the composite lands ~9.3–9.9 min/M** — the arm's stream-1 win swamped by the build's ADDED emission phase, a cost Run E never carried. The honest comparison is per-phase, not composite: the ledger already splits them. Ledger E12 annotated accordingly (three-sweep structure at giant scale).
- Waiter unchanged — the wall prints when it prints.

### 16:47 UTC — t+16h10m — stream-3 at 9.9%, ~980 b/min; wall ~19:00–19:20

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-3 (emission): 12,260/123,943 (9.9%)** at ~980 b/min — the fastest sweep of the three (sequential-leaning writes as expected for emission). If full-length: ends ~18:45 ⇒ **TERMS_DATA ~19:00–19:20**, composite ~**9.5–9.7 min/M** (per-phase: s1 3.96 / s2 2.88 / s3 ~1.8–1.9 equivalent + boundaries).
- Rows: part 10 at 15.77M (~115.8M). Device 230k r/s @ 6.5 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: the three-sweep anatomy is pricing itself cleanly phase by phase — emission is cheap per-M but long in absolute terms at giant scale; the wall lands this evening.

### 17:17 UTC — t+16h40m — stream-3 at 39.3%, accelerating (1,215 b/min); wall pulls in to ~18:30–18:50

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-3: 48,700/123,943 (39.3%)** — interval 16:47→17:17: 36,440 b in 30.1 min = **1,215 b/min** (980→1,215, still accelerating). Remaining ~75.2k b ⇒ stream-3 ends ~18:20 ⇒ **TERMS_DATA ~18:30–18:50**, composite ~**9.0–9.3 min/M**.
- Rows: part 10 at 16.33M (~116.3M — servo throttled to ~1.1M/h under the emission storm). Device 233k r/s @ 6.7 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: emission keeps quickening as it goes — the per-phase splits (3.96 / 2.88 / ~1.5 equiv) will make the composite legible when it lands this evening.

### 17:47 UTC — t+17h10m — stream-3 at 65.8% (1,095 b/min); wall ~18:35–18:55

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-3: 81,570/123,943 (65.8%)** — interval 1,095 b/min (1,215→1,095). Remaining ~42.4k b ⇒ stream-3 ends ~18:25 ⇒ **TERMS_DATA ~18:35–18:55**, composite ~**9.1–9.4 min/M** (per-phase: 3.96 / 2.88 / ~1.6 + boundaries + write).
- Rows: part 10 at 16.90M (~116.9M). Device 228k r/s @ 6.8 KB. max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: on final approach — the waiter takes it from here.

### 18:17 UTC — t+17h40m — stream-3 at 89.6%; landing imminent

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-3: 111,090/123,943 (89.6%)** — interval 984 b/min; remaining ~12.9k b ⇒ stream-3 ends ~18:30 ⇒ **TERMS_DATA ~18:40–19:00**. Composite finishing ~**9.2–9.5 min/M**; per-phase ledger ready.
- max=0; gate 0; integrity 0; wire live; part 10 at ~17.5M. Waiter armed; push on the wall.
- Trend: minutes out.

### 18:47 UTC — t+18h10m — starved 4M lands at 79.3 min/M (all-time record, E4 extreme); giant in a FOURTH stream

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Giant adopt line: still absent ⇒ waiter correctly armed.
- **The 18:40:02 landing was NOT the giant** — a 4M merge at **79.3 min/M** (5.24 h wall, parked through all three giant sweeps; E4 updated: the multi-sweep giant triples the starvation window; victims need scheduler help, not width).
- **Giant stream-4: 43,830/124,870** (total drift +927; numerator reset ~18:30) at ~2,580 b/min — write-phase-like speed. If full-length: ends ~19:19 ⇒ **TERMS_DATA ~19:25–19:45**, composite ~**9.8–10.1 min/M** (four-sweep anatomy: 3.96 / 2.88 / ~1.6 / ~0.5 equiv).
- max=0; gate 0; integrity 0; wire live.
- Trend: the anatomy keeps growing sweeps but each is faster than the last — the wall lands tonight and prices all of it at once.

### 19:17 UTC — t+18h40m — stream-4 COMPLETED to its full counter (19:16:43); upper-layer tail running

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- **Giant stream-4 finished 124,870/124,870 at 19:16:43** — the first stream of the campaign to print an exact-total completion. Now in the post-sweep tail (upper layers + cross-link + adopt): >10 min so far without the adopt line, consistent with giant-scale upper layers being non-trivial (~1.6M nodes cross-source). **TERMS_DATA expected ~19:30–20:00**; composite ~**9.8–10.3 min/M**.
- max=0; gate 0; integrity 0; wire live. Two waiters armed (bdt18922r, b1ctwtzye) — push on the wall.
- Trend: four sweeps done, tail phases pricing themselves now; the complete anatomy lands within the hour.

### 19:45 UTC — THE GIANT LANDED: 642.7 min = 10.11 min/M — the class's first complete wall (ledger E13)

- ADOPT 19:36:02 (REORDERED), TERMS_DATA 19:43:20, 320.5 GB body, zero integrity/assertion lines across the entire 10.7-hour merge. Landing pushed.
- The per-phase decomposition is in E13; the headline splits: search 3.89 min/M (arm win), everything-else 6.22 min/M (arm-blind, SPLAT's target).
- Post-landing: expect the queued-build burst (the permit queue has been deep for hours) and the servo to reopen ingest; giants #2 and #3 re-price from E13.

### 19:47 UTC — t+19h10m — post-landing burst underway; ingest reopened

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- The burst began on schedule: 1 landing since 19:43:30, a fresh 4M (pass 19:44:39, 0.6 µs/node — clean passes again with the pool free) already at 97% four minutes in. SSTables consolidated 34→25; table 804.9 GB post-giant (+320.5 GB TERMS_DATA landed).
- **Ingest reopened**: part 10 jumped to 20.30M (+2.8M in the hour post-storm — servo unthrottling as the backlog drains).
- Device: 165k r/s @ 9.0 KB (burst mix). max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: the system exhales after the giant — burst landings will re-tier toward giant #2 (needs four more 16Ms; ETA roughly 6–8 h at reopened pace), which E13 now prices in advance.

### 20:17 UTC — t+19h40m — burst in full swing: three 4M walls landed (2.65–3.95), two more in flight

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED.
- Burst walls: **3.29, 3.95, 2.65 min/M** (lightly co-scheduled with each other — between the solo band 1.0–1.5 and the starved band; the backlog clears in overlapping pairs); two more 4Ms in flight (passes 20:08, 20:15). SSTable re-tiering toward the next 16M generation underway.
- Ingest: part 10 at 22.48M (+2.2M/30min = 4.4M/h — the servo's strongest part-10 pace of the run). Device 273k r/s @ 5.0 KB (burst mix). max=0; gate 0; integrity 0; wire live; lint 0.
- Trend: recovery dynamics textbook — clear the 4M backlog, rebuild the 16M tier, giant #2 forming roughly 02:00–04:00 with E13 as its prior.

### 20:50 UTC — t+20h13m — backlog cleared in 40 min; gen-2's first 16M already 66% through batches; ingest at run-record 8.2M/h

- Provenance: pid 290993 / db987fd0 / fp16+simord — UNCHANGED. Client nmbrs 301327 alive.
- 4M backlog CLEARED — all six post-giant walls: 3.29, 3.95, 2.65, 3.56, **2.23, 1.17** min/M. Monotone decay as co-scheduling thins; the last lands in the solo band (1.17 vs 1.01–1.16). → E14.
- **16M #6 IN FLIGHT** (first of the gen-2 tier): pass 20:29:52, 15.86M ords; 20,390/30,985 batches at 20:50 (66% in 20 min — solo-fast). References: 4.48/4.34/4.50/5.42/12.04**.
- Ingest: part 10 at 27.03M (+4.5M/33min ≈ **8.2M/h, run record** — servo wide open with compaction debt gone). Table 859 GB, 0 sstables.
- Pass µs/node: 0.6 22.2 0.7 0.7 14.2 0.9 0.8 (two burst-co-scheduled blips, sub-flag). Device 302k r/s @ 6.3 KB, r_await 0.19 ms member-avg 0.21. Cgroup anon 106.5G/file 36.4G, max=0. Gate 0, integrity 0 (4 grep hits since 19:43 were pre-run artifacts at 00:03–00:32 — ERROR lines carry date in field 3, filter misaligned; verified clean), wire live (27.03M).
- Trend: the exhale is complete — recovery from the 10.7 h monopoly took <45 min of degraded 4M walls; the machine is already building giant #2's tier at solo pace.

### 21:17 UTC — t+20h40m — gen-2 builds two-at-a-time: 16M #6 + #7 co-resident; batch clock now ambiguous

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Client nmbrs 301327 alive.
- No landed walls this interval (the 0.11 sub-min match on the 20:24 4M is the known TERMS_DATA double-match artifact of the moving cut; its true wall 1.17 stands as recorded at 20:50).
- **16M #7 STARTED 21:00:53** (CompactionExecutor:65; #6 is :64 — thread-name discrimination, not counters): one of the two parked byte-complete 23.2 GiB compactions took a build slot. First time two 16M builds run co-resident in Run F. #6 ordinal clock (nodetool 21:15): 6.07M/15.86M = 38% at t+45min — co-scheduled pace, off the solo 4.48 reference; expect walls toward the 5.42 cohort band. Their landings will price 16M×16M co-scheduling (candidate E15).
- Instrument note: both merges total 30,985 batches (same size) → interleaved `Compaction I/O progress` lines are indistinguishable (apparent 20390→19940 "regression" at 21:17 was the two streams, not a reset). Batch clock UNRESOLVABLE while same-size merges co-run; use nodetool per-task ordinal rows.
- Queue (nodetool 21:15): 6 pending = #6 indexing + #7 now started + one 4M build at 0% + one more byte-complete 16M parked + two fresh 4M compactions. Tier: 1×92.9G (giant) + 3×23.2G + 10×5.8G + 10×1.4G; giant #2 needs the fourth 16M — already byte-complete, awaiting slot.
- Ingest: part 10 at 29.97M (+2.94M/27min ≈ 6.5M/h — servo still wide open). Table 898 GB, 26 sstables. Pass µs/node 0.9 0.8 1.4. Device 276k r/s @ 6.2 KB, r_await 0.20 ms. Cgroup anon 106.5G/file 36.2G, max=0. Gate 0, integrity 0, wire live (29.97M).
- Trend: the tier factory has shifted to parallel production — two of giant #2's four 16Ms mid-build simultaneously, third parked ready; forming window holds at ~02:00–04:00, possibly earlier, at the cost of co-scheduled (5+ min/M) rather than solo 16M walls.

### 21:55 UTC — t+21h18m — SPLAT WAS NEVER INERT: staged machinery log-loud all run (E15); #6 healthy mid-base; no new walls

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Client nmbrs 301327 alive.
- **E15 (major correction): SPLAT staged machinery ACTIVE and log-loud since 00:37.** Shapes never grepped before: `Cross-link: L0 source order`, `Bands for source N released: 16 bands mapped`, `Stage DISTRIBUTE started/: N units`, `Stage FINALIZE`. 3,974 lines in rotated zips + every hour of the current log, all classes incl. the giant's window → E13's "streams 1–4" = the staged per-source pipeline itself. E5/E11's "inert / only token-stream, log-silent" is FALSIFIED.
- Instrument corrections stacked: (a) batch totals are PER-STAGE units — #6's base denominator 15,864,116 gave way to DISTRIBUTE 3,966,042 units at 21:28 (per-source, 15.86M/4), so batch "end" at ords=50.0% (21:27:55) is a stage boundary, not completion, and fingerprint-by-total breaks across stages; (b) 21:17's "nodetool per-task rows are honest" WITHDRAWN — rows re-denominate per phase (the apparent ID swap at 21:47); (c) honest instruments: per-executor stage lines + thread samples.
- **#6 NOT wedged** (24-min quiet channel was my instrument gap): jcmd sample shows exec 64 awaiting `GraphSearcher.search` inside `computeBaseBatch → processBaseNode → gatherCandidates → gatherFromOtherSource`, ForkJoin workers in `gatherFromSameSource/willNeedL0Record` — active base compute via BoundedParallelExecutor, 288.6s cpu on the coordinator. Anatomy refined: per-source gather → bands released → DISTRIBUTE → next source, interleaved.
- Walls: none landed this interval. #6 (exec 64) mid-base ords ≈50% at t+86min; #7 (exec 65) base at 2.41M/15.86M = 15% at t+48min (21:49 lines) — both co-scheduled-slow vs 4.48 solo; 5.42/12.04 band still the expectation. Pending 7 (three 4M compactions now queued behind the two 16M builds + parked pair).
- Ingest: part 10 at 32.66M (+2.69M/30min ≈ 5.4M/h). Table 934 GB, 30 sstables. No new passes (µs series empty). Device 265k r/s @ 6.5 KB, r_await 0.21 ms. Cgroup anon 106.5G/file 36.1G, max=0. Gate 0, integrity 0, errs 0, wire live (32.66M).
- Trend: the run's biggest interpretive correction — the arm has been riding ON TOP of an active SPLAT pipeline all along, so cross-run deltas (Run E→F) price arm+SPLAT jointly, not the arm alone; next: map stage lines onto #6/#7 walls when they land, and re-cut E13's anatomy in stage terms.

### 22:18 UTC — t+21h41m — batch-clock model CLOSED (per-source cycles); #6 at 67% ords, #7 entering DISTRIBUTE; no landings yet

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Client nmbrs 301327 alive.
- **E15 addendum — the instrument model closes.** `Stage BASE_LAYER progress: N/15864116` = merge-global ordinal clock; the batch counter = PER-SOURCE CYCLE (30,985 batches/source, 4 cycles per 16M). Verified against all prior paradoxes: 20:50 "66% batches @ 16% ords" = cycle 1 (0–25% quarter) at 66% ✓; 21:17 "20390→19940 regression" = #6's cycle-2 reset (not interleaving — that reading corrected) ✓; 21:27 "end at 50% ords" = cycle-2 end ✓. Also: `Stage DISTRIBUTE completed: 3966042 units in 2186716 ms` — stage-completed lines carry elapsed ms → **full stage anatomy of every landed merge (incl. E13's giant) is retrospectively reconstructible from logs**. Queued for the next lull.
- Stage kinetics oddity (watch, don't conclude): #6's source-1 DISTRIBUTE showed 36.4 min elapsed but its 40→100% progress burst took ~12 s — the stage appears gated on upstream base compute, then executes fast. Overlap structure, not stall.
- In flight: **#6** BASE_LAYER 9.52M/15.86M = 60% at 22:13 (67% by batch clock 22:18), rate ≈182k ords/min segment → base done ~22:48, landing projected ~23:00± → wall ≈9.5–10 min/M territory (>6 flag arms if #7 follows). **#7** source-1 bands released 22:08:45 (t+68min for the first quarter — co-scheduled slow), DISTRIBUTE started 3,965,993 units.
- No new passes. Ingest: part 10 at 35.06M (+2.4M/30min ≈ 4.8M/h). Table 973 GB, 34 sstables. Device 253k r/s @ 6.3 KB, r_await 0.21 ms. Cgroup anon 106.5G/file 36.0G, max=0. Gate 0, integrity 0, wire live (35.06M).
- Trend: both 16Ms are pricing 16M×16M co-scheduling well above the 5.42 band — the first stage-resolved walls land next interval and will seed the E13 stage re-cut.

### 22:47 UTC — t+22h10m — >6 FLAG LOCKED BY FLOORS (pushed): both 16Ms still in flight, walls guaranteed >6.7/8.7 min/M

- Provenance: pid 290993 / db987fd0 / fp16 — UNCHANGED. Client nmbrs 301327 alive.
- No landings, no new passes. Elapsed floors already exceed the flag: **#6 t+138min = ≥8.7 min/M; #7 t+107min = ≥6.7 min/M** — "16M >6 twice consecutively" mathematically locked pre-landing → PushNotification sent 22:47 (framed as co-scheduling penalty, not fault).
- Stage progress: #7 source-1 DISTRIBUTE completed 22:23:37 (891,364 ms = 14.9 min — vs #6's source-1 2,186,716 ms = 36.4 min: #7's cheaper, consistent with gating-on-upstream, not fixed stage cost). #6 `Bands for source 0 released` + third DISTRIBUTE started 22:28:52 — **source order is permuted per merge** (#6 ran source 1 before source 0; matches `Cross-link: L0 source order [3,…]`). #6 mid-third-quarter at 22:47, base-done projection slipped past 22:48; landing ~23:10–23:30 → wall ≈10–11 min/M. Giant #2's forming window slips accordingly (tier needs #6+#7+parked third+a fourth).
- Ingest: part 10 at 36.94M (+1.88M/30min ≈ 3.8M/h — servo tapering as debt accumulates behind the 16M builds). Table 999 GB (1 TB threshold next interval), 35 sstables. Device 251k r/s @ 6.6 KB, r_await 0.21 ms. Cgroup anon 106.5G/file 35.9G, max=0. Gate 0, integrity 0, wire live (36.94M).
- Trend: two-wide 16M production costs ~2–2.5× per-merge vs solo — throughput math (2 merges × ~10 min/M each ≈ 5 min/M effective) may still beat serial 4.48+4.48, but the walls will decide; exact numbers land next interval (candidate E16).
