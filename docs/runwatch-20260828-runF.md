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
- E4: **starved-4M inflation halved** (6.3–7.4 vs 10.6–18.6) — consistent with E1 + WIDTH=16
  helping victims more than owners.

## Entries

### 02:55 UTC — t+2h18m — armed; 37.1M rows; 16M #2 launched
- Parts 0–2 done (30M), part 3 at 7.1M (~16.7M/h — fastest run to date). Table 166.7 GB / 7 SSTables at arm time.
- Landed walls so far: 4M solo ×4 (1.01–1.16), 4M starved ×2 (7.40, 6.28), 16M #1 4.48.
- **16M #2 pass 02:49:46 (15.86M) + a 4M co-runner (pass 02:49:12)** — the starvation experiment repeats.
- Gate 0; integrity guard silent; max=0; wires live (139,909/window at the hot servo); lint 0; zero ticks.
