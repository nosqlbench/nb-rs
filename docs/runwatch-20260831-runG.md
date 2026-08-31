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

**Capture protocol — do this for every important session.** A run is only comparable later if all four
pieces survive: (1) the **jars** — jvector + dse-db, since a rebuild need not reproduce the md5;
(2) the **flags** from the live `/proc/<pid>/cmdline`, not from the conf file, which can drift;
(3) the **server logs** — logback keeps only 7 days / 5 GB, and they are the sole source of walls and stage
elapsed; (4) the **full client session directory** → `nb-rs/keepsessions/<session>/`, which carries the
structured `metrics.db`, `checkpoint.jsonl`, session/transcript logs and end-of-run summaries. Run F is
complete on all four (session at `keepsessions/stcs_adaptive_20260828_003711`, 139 MB). Run G owes (3) and
(4) at run end; its jars and flags are already banked.

**Instrument note:** the batch counter's per-source-cycle semantics from Run F (E15 addendum) do NOT apply —
banded DISTRIBUTE does not exist pre-SPLAT. Walls and `Stage X completed … in N ms` lines are unaffected.

## Reference walls (Run F, what the control is measured against)

- 4M solo **1.01–1.16**; 4M starved under a giant 6.28–7.40, record **92.23**.
- 16M **4.48 / 4.34 / 4.50**, late-run near-solo **5.67**; two-wide pair 11.99 + 10.06 (6.00 effective, E16).
- Giants **10.11 / 10.21** min/M at 63.5M (10.97 at 68.4M, E17). 16M BASE_LAYER stage 156.9 min.

## The endpoint that decides it: the tail

SPLAT buys amortization of work and reduced read/write amplification. That machinery has nothing to pay for
itself with when resources are not saturated — so at 4M and 16M, where the working set is cache-resident or
nearly so, SPLAT is expected to be **pure overhead** and the control should look equal or slightly better.
Those classes are not the experiment. The planning and sequencing only pay off, potentially by multiples,
once merges are large enough to be genuinely IO-saturated.

**The key tell is the tail**: after ingest completes there is a remainder of compaction work, and its shape
plus the total time to completion — measured against the last full run — is what decides whether SPLAT earns
its keep.

### Run F's tail (the baseline, reconstructed from the archived logs)

| boundary | timestamp | span |
|---|---|---|
| run start | 2026-08-28 00:37:11 | — |
| ingest ends (`load_increment_adaptive` returns) | 2026-08-29 15:35:25 | ingest **38 h 58 m** |
| `settle_compactions` starts | 2026-08-29 15:47:42 | |
| `settle_compactions` returns | 2026-08-30 12:26:57 | **tail 20 h 51 m** |
| all phases complete | 2026-08-30 12:32:10 | **total 59 h 49 m** |

**The tail was 34.9% of the whole run**, and its shape was:

| pass | class | wall | min/M | note |
|---|---|---|---|---|
| 15:39:53 | 19.83M | 362.5 min | **18.28** | co-scheduled pair — 4× its solo band |
| 16:54:42 | 15.86M | 288.8 min | **18.20** | " |
| 21:43:41 | 3.97M | 6.5 min | 1.65 | |
| 21:50:41 | 16.86M | 108.5 min | 6.44 | |
| 23:54:18 | **68.41M** | 750.8 min | 10.97 | **giant #3 — landed 12:25:06** |

`settle_compactions` returned **1 m 51 s after the last giant landed**, so Run F's tail is
**giant-terminated**: its length ≈ (time before the final giant can form) + (that giant's wall). Two
sub-metrics follow, and SPLAT should move both if it moves anything —

1. **the final giant's wall** (10.97 min/M at 68.4M here), and
2. **how degraded the mid-class cleanup is while the giant hogs the pool** (18.20/18.28 — 4× solo).

A third, cheaper tell: the run left **5 sstables** (100.0 / 92.9 / 92.8 / 5.8 / 0.9 GB) — i.e. it did not
fully consolidate, it simply ran out of work to schedule. Compare Run G's terminal sstable shape too.

## Control ledger

**C1 — Small-class deltas are the expected null region, not a verdict (revised).** Run G's opening 4M walls
(0.69–1.03 vs Run F's 1.01–1.16) and first 16M (3.65 vs 4.48/4.34/4.50) run ahead of Run F. Under SPLAT's
own theory this is what should happen: with the working set cache-resident there is no saturation to
amortize, so the staged planning is overhead with nothing to recover it. The magnitude is small and
sign-consistent with the theory; it is **not** evidence against SPLAT, and the earlier reading of it as "a
straight win" was wrong. The classes that can falsify or vindicate SPLAT are the giants and, above all, the
tail defined above.

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
