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
- E5 (probe 03:17): NO distinctive SPLAT log phrases (band-staged / token stream / spill /
  key-window) in the run's logs — the de79d5bf machinery is INERT this run, as presumed; the
  ledger's effects attribute to WIDTH=16 + the pass rewrite + upstream fixes only.
- E4 (REVISED 03:47): starved-4M inflation is BIMODAL, not halved — observed 6.28, 7.40, and
  now **11.79** (the 02:49 co-runner, starved by 16M #2 its whole life) vs the old world's
  10.6–18.6. The improvement is real but variance dominates; withhold the "halved" claim.
- E6 (forming, 03:47): **16M #2 is running ~2× slower than #1** — 32.5% in 58 min incl. a
  gated start; recent clip 281 b/min projects a wall ≈8–9 min/M vs #1's 4.48 at similar
  depth. One data point; if it lands >6 the two-consecutive flag arms on #3. Possible
  drivers: deeper table (40M vs 30M rows), a second 4M co-runner (pass 03:45:47), request
  size down to 4.8 KB (more small co-traffic).

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
