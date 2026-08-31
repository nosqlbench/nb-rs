# Run G — the pre-SPLAT control (jvector 30cdfdaf, arm ON)

**Purpose.** Isolate SPLAT. Run F (jvector `de79d5bf`) carried the staged-permutation algorithm *and* the
frontierPrefetch=16 arm, so its delta against Run E priced the two jointly (ledger E15). Run G holds every
flag identical and removes only the SPLAT algorithm, so its walls attribute the difference to SPLAT alone.

## What is running

| | Run G (control) | Run F (SPLAT) |
|---|---|---|
| jvector | `30cdfdaf` — branch `experiment/pre-splat-control-30cdfdaf`, jar md5 **6dcb0e4c** (645,094 B) | `de79d5bf`, db987fd0 (694,695 B) |
| Cassandra | `ea38d33954` — source unchanged, **recompiled** against the control jvector → **0517567f**, class version 69 | `ea38d33954` → efadc76c |
| daemon | pid **1544653**, started 2026-08-31 04:45:07 | pid 290993 |
| client | `nmbrs` pid **1546323**, session `stcs_adaptive_20260831_044727`, started 04:47:27 | pid 301327 |
| flags | **all 24 identical to Run F** (frontierPrefetch=16, similarityOrdinals=true, clusterSearch=false, pretouch −1/1048576) | same |

Staging: node stopped → jvector branched at 30cdfdaf → `mvn -DskipTests install` → jar into `~/.m2` and
`lib/` → `bin/build-cassandra.sh` (**Cassandra compiled cleanly against the pre-SPLAT jvector** — the
compatibility question settled by the compiler, not by assumption) → `new_cass` wipe → start.
Anchors: control `build-anchors/runG-20260831-presplat-30cdfdaf/`, rollback
`build-anchors/runF-20260828-splat-de79d5bf/` (includes Run F's 523 archived log files).

**Instrument note:** the batch counter's per-source-cycle semantics from Run F (E15 addendum) do NOT apply —
banded DISTRIBUTE does not exist pre-SPLAT. Walls and `Stage X completed … in N ms` lines are unaffected.

## Reference walls (Run F, what the control is measured against)

- 4M solo **1.01–1.16**; 4M starved under a giant 6.28–7.40, record **92.23**.
- 16M **4.48 / 4.34 / 4.50**, late-run near-solo **5.67**; two-wide pair 11.99 + 10.06 (6.00 effective, E16).
- Giants **10.11 / 10.21** min/M at 63.5M (10.97 at 68.4M, E17). 16M BASE_LAYER stage 156.9 min.

## Control ledger

**C1 — The control is faster at every class measured so far (first 3 h, provisional).** 4M solo walls
0.69 / 0.90 / 0.87 / 0.87 / 1.03 / 0.89 / 0.89 vs Run F's 1.01–1.16 — the control's *slowest* opening 4M
beats Run F's *fastest*. First 16M landed **3.65 min/M** (58.4 min, 15.99M ords) vs Run F's 4.48 / 4.34 /
4.50. Direction: removing SPLAT looks like a straight win at cache-resident and mid classes. Provisional
because early-run co-scheduling is lighter than Run F's steady state and the table is still small; the giant
at ~t+19h is the measurement that decides it.

## Entries

### 07:37 UTC — t+2h50m — control is running clean and ahead of Run F at 4M and 16M

- Provenance: pid 1544653 / jvector 6dcb0e4c / dse-db 0517567f / 24 flags identical — as staged.
- **Gates all pass.** G1 SPLAT-absent = **0** DISTRIBUTE/TOKEN_STREAM/Bands lines (the control is genuinely
  pre-SPLAT). G2 wall instrument live (11 passes / 9 adopts / 9 TERMS_DATA). G3 stage lines present for
  SOURCE_PRETOUCH, SIMILARITY_ORDINALS, PQ_RETRAIN, CODE_PRE_ENCODE, BASE_LAYER, UPPER_LAYERS, FINALIZE —
  and *only* those, exactly the pre-SPLAT enum. G5 cost 0, integrity 0, max=0. G6 satisfied (see C1).
- Walls: 4M **0.69, 0.90, 0.87, 0.87** then **1.03, 0.89, 0.89**; 16M #1 **3.65** (58.4 min). In flight: a
  4M from 05:40 (long-running — watch it), 16M #2 from 06:58, 4M from 07:23.
- Table 332 GB, device 304k r/s @ 5.5 KB, r_await **0.19 ms** — above the 190k sync ceiling, so G4's arm is
  demonstrably live on the control build too.
- Trend: the control opens faster than Run F at both measured classes with every gate green; the giant
  (~t+19h, so ~23:45 UTC) is the number that matters.
