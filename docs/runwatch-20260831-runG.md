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

**C2 — Under contention the control is markedly WORSE, and that is SPLAT's predicted payoff region
(first real signal).** 4M merges starved beneath a co-resident 16M measured **15.69, 17.97 and 10.57 min/M**
in Run G (plus **18.23** and 7.19 measured 08:09–08:59), against Run F's armed starved band of **6.28–7.40** — the control is 1.4–2.9× worse at exactly the
job SPLAT claims to improve. Meanwhile the uncontended cases stay in the null region (solo 4M 0.69–0.90 vs
Run F 1.01–1.16; 16M 3.65 / 3.84 vs 4.48 / 4.34 / 4.50), so this is not a general slowdown — it is
contention-specific. Reading: SPLAT's amortization and reduced read amplification chiefly buy *protection for
co-resident work*, which is precisely the mechanism that should also compress the tail. Corroborating signal:
ordinal-pass µs/node runs 10–41 here (one 399 outlier) versus Run F's mixed 0.6–22 — the pre-SPLAT ordinal
pass is slower and more contention-sensitive, consistent with `8eba8b9e` (SPLAT's ordinal plan from the token
stream) doing real work.

**Instrument correction (08:42).** The ±500-ordinal pass→adopt matcher inherited from Run F **mis-assigned
walls**: counter drift reached −1,253 here, so drifted passes were paired with the wrong adopts and the
starved cases were silently reported as fast ones. The 07:37 entry's "0.89 / 1.03 / 1.16 / 0.90 / 1.06"
figures are withdrawn. Matching now uses a proportional tolerance (0.1% of the pass count, floor 500) with
earliest-unused-adopt ordering; drifts observed since: 0, ±56–74, −1,253. Every wall in this doc from 08:42
onward uses the corrected matcher.

**C3 — The giant test reduces to one number: BASE_LAYER min/M.** Recovering all three Run F giants'
per-stage anatomy from the archived logs (the stage instrument is byte-identical across builds, so this is
apples-to-apples):

| Run F giant | PRETOUCH | ORDINALS | PRE_ENCODE | **BASE_LAYER** | TOKEN_STREAM |
|---|---|---|---|---|---|
| #1 63.58M | 4.84 min | 23.21 | 22.33 | **593.71 min = 9.34 min/M** | 15.66 |
| #2 63.46M | 5.38 | 11.13 | 12.51 | **609.87 = 9.61 min/M** | 15.32 |
| #3 68.41M | 1.85 | 2.26 | 9.95 | **716.50 = 10.47 min/M** | 19.09 |

**BASE_LAYER is 90%+ of a giant's wall, and TOKEN_STREAM — the SPLAT-only stage — costs just 15–19 min,
2–3%.** So SPLAT's overhead at giant scale is nearly free, and its entire case rests on whether the staged
plan makes BASE_LAYER cheaper. Run G's giant BASE_LAYER against **9.34 / 9.61 min/M** is the single
measurement that decides this experiment at merge scale; the tail decides it at run scale.

**C4 — SPLAT's ordinal plan is ~3× cheaper at giant scale, but it is only ~8% of the wall.** Run G's giant
took **73.0 min** on SIMILARITY_ORDINALS (4,380,348 ms for 63,579,887 units = **68.9 µs/node**); Run F's
giants took 23.21 / 11.13 / 2.26 min (giant #1 = 21.9 µs/node, same mid-ingest context). That is a genuine
algorithmic difference — `8eba8b9e` derives the similarity-ordinal plan from the token stream, and this is
the cost of not having it — and it also explains the elevated µs/node readings flagged earlier: they are the
pre-SPLAT ordinal pass, not an anomaly. Scale matters though: 73 min against a ~600 min BASE_LAYER is ~8% of
a giant's wall, so C4 is a real SPLAT win that cannot by itself decide the experiment. CODE_PRE_ENCODE was
12.88 min vs Run F's 22.33 / 12.51 / 9.95 — no signal.

**C5 — WITHDRAWN AS STATED; the giant comparison cannot be projected from early rate (revised 15:42).**
The 14:42 reading (Run G's giant at 9.5% after 90 min = 66.7k nodes/min, projecting 15.0 min/M against Run
F's 9.34) compared **Run G's early-stage rate against Run F's whole-stage average**, which is invalid.
Checking Run F's giant #1 progress counters from the archive:

| | Run F giant #1 | Run G giant #1 |
|---|---|---|
| ordinal rate, first ~7 h of BASE_LAYER | **73.4k nodes/min** (0 → 31.73M, 09:25→16:37) | **63.8k nodes/min** (0 → 9.51M, 149 min) |
| BASE_LAYER average from the stage line | **107.1k/min** (593.71 min, authoritative) | unknown until it completes |

Run F's giant therefore ran at 58–74k/min for most of its stage and still averaged 107.1k/min, implying a
**~2.7× acceleration in its final stretch** (≈31.85M ordinals in the last ~159 min). Whatever causes that —
plausibly SPLAT's token stream making later sources cheap, which is the amortization claim itself — it means
**linear extrapolation from early rate badly under-predicts completion**, and the same extrapolation applied
to Run G is untrustworthy.

What survives: on the **like-for-like early window the control is ~15% slower** (63.8k vs 73.4k), not 60%.
The verdict at giant scale must wait for Run G's `Stage BASE_LAYER completed … in N ms` line, which is the
only authoritative measure, and the key question becomes **whether the control shows the same late
acceleration** — if it does not, SPLAT's win is real and concentrated exactly where amortization predicts.

**Counter caveat for both runs:** Run F's *batch* counter cycles (reset observed 13:10 → 13:55) while its
*ordinal* counter is monotonic; Run G's batch counter is merge-global. Both giants share the same batch total
(123,941 vs 123,940). Progress counters are usable for coarse liveness only — never for cross-build rate
claims.

**Instrument note (14:42).** Pre-SPLAT the batch counter is **merge-global**, not per-source: the giant logs
`46,840/123,940 batches (5,995,520/63,579,887 ordinals)` against the full ordinal count. Run F's per-source
cycle model (E15 addendum) genuinely does not apply here, as the runbook warned. Also, `Stage BASE_LAYER
progress` only fires per 10% decile, so at this rate the first decile line arrives ~95 min in — the absence
of progress lines at 90 min was reporting granularity, not a stall (thread sample confirmed 40 ForkJoin
workers in `processBaseNode`/`gatherCandidates`, coordinator parked on the task, 1,164 s CPU).

**C6 — The giant's real structure: 4 L0 sources, and Run F's win is entirely in sources 3–4.** One batch
cycle = **123,940 batches × 128 ordinals = 15.86M = exactly one L0 source**, so a 63.58M giant is four
cycles. Re-reading both runs through that lens:

| | source 1+2 | sources 3+4 | stage total |
|---|---|---|---|
| **Run F giant #1** | 31.73M by 16:37 = **432 min** (~216 min/source) | remaining 31.85M in **~160 min** (~80 min/source) | 593.71 min |
| **Run G giant #1** | source 1 at 85% after 209 min → **~246 min/source** | unknown | — |

Run F's giants did **not** run uniformly fast: their first two sources cost ~216 min each — barely better
than Run G's ~246 — and then sources 3–4 came in at ~80 min each, **2.7× cheaper**. That is the amortization
signature itself: the token stream built while processing early sources makes later cross-source search
cheap, which is precisely what `29f24feb`/`8aa6d329` were written to do.

  - *C6 running result (18:42):* source 1 = **252 min**, source 2 = **40% in 77 min → ~192 min projected**.
    So the control **does** amortize, but shallowly: 252 → 192 is **1.31×**, where Run F went ~216 → ~80 =
    **2.7×**. Combined, the control's sources 1+2 project **444 min against Run F's 432 — only 2.8% slower**.
    The entire Run F advantage therefore lives in **sources 3–4** (160 min for the pair). If the control holds
    ~192/source there, the stage lands ≈828 min (13.0 min/M, 1.39× Run F); if it keeps declining toward ~120,
    ≈684 min (1.15×). The giant-scale verdict is a question about the second half only.

  - *C6 resolved (21:42):* per-source costs are now **252 / 211 / ~117 / ?** min. The control amortizes
    substantially after all — source 3 is running **1.8× faster than source 2** — where Run F went ~216 →
    ~80 (2.7×). Both builds amortize; SPLAT simply amortizes harder. If source 4 matches source 3, the stage
    lands at **697 min = 10.96 min/M vs Run F's 593.71 = 9.34 — a 1.17× SPLAT win**, and the giant's wall
    ≈750 min ≈11.8 min/M vs 10.11/10.21 (1.16×). That is a real but **modest** giant-scale advantage, far
    from the 1.6–1.7× the early extrapolations suggested, and it puts the weight of the experiment back onto
    the tail, where a ~1.2× on the final giant compounds with the much larger starvation gap (C2).

**This makes the experiment a clean binary.** Run G's source 1 is ~14% slower than Run F's — consistent with
the null-region overhead everywhere else. The question is whether **Run G's sources 2–4 get cheaper at all**:
- if they stay near 246 min each → stage ≈ 984 min vs 594, SPLAT wins ~1.65× at giant scale, and the win is
  specifically *cross-source amortization*, not raw per-node speed;
- if they drop toward 80 min each → there is no giant-scale SPLAT win and the case rests on C4 plus the tail.
Source 1 completes ~17:20; source 2's rate answers it within a couple of hours.

**C7 — THE GIANT IS MEASURED: near-parity on the wall, ~9% total, and SPLAT's win is in the ordinal plan,
not the base layer.** Run G's giant landed **2026-08-31 23:47:14** (TERMS_DATA), with every stage now
authoritative rather than projected:

| stage | Run F giant #1 | Run G giant #1 | delta |
|---|---|---|---|
| SOURCE_PRETOUCH | 4.84 min | 3.82 min | −1.0 |
| PQ_RETRAIN | 25.17 | 35.76 | +10.6 |
| SIMILARITY_ORDINALS | 23.21 | **73.00** | **+49.8** |
| CODE_PRE_ENCODE | 22.33 | 12.88 | −9.5 |
| **BASE_LAYER** | **593.71** | **626.99** | **+33.3** |
| TOKEN_STREAM (SPLAT-only) | 15.66 | — | −15.7 |
| UPPER_LAYERS + FINALIZE + write-back | ~7 | ~4 | −3 |
| **wall (pass → TERMS_DATA)** | **642.7 = 10.11 min/M** | **647.2 = 10.18 min/M** | **+0.7%** |
| **total (pretouch → TERMS_DATA)** | **695.8 min** | **759.8 min** | **+9.2%** |

Read carefully, three things follow. (1) **The wall is a dead heat** — 10.18 vs Run F's 10.11 and 10.21, i.e.
Run G's giant sits *between* Run F's two same-class giants. (2) **BASE_LAYER, the stage that carries 90% of
the work, is only 5.6% slower** — the control amortizes across its four L0 sources (252/211/116/47 min) very
nearly as well as SPLAT does. (3) **SPLAT's real giant-scale advantage is the ordinal plan** (+49.8 min for
the control, C4), which sits *outside* the pass→TERMS_DATA wall and is therefore invisible to the wall metric
— it shows up only in the end-to-end total, where SPLAT is ~9% ahead. The staged machinery largely pays for
itself: TOKEN_STREAM costs 15.7 min and buys 33.3 min of BASE_LAYER, a net ~18 min, partly given back by a
9.5-min more expensive CODE_PRE_ENCODE.

**Consequence for the experiment:** at merge scale SPLAT is worth ~9% end-to-end, not the 1.6× the early
extrapolation suggested and not nothing either. The large remaining differences are contention (C2 — the
control's starved merges run 2–3× worse, with an in-flight 4M past 700 min against Run F's 92.23 record) and
the tail, which is still the deciding metric.

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

### 08:42 UTC — t+3h55m — corrected matcher reveals severe starvation; control loses badly under contention (C2)

- Provenance: pid 1544653 / jvector 6dcb0e4c / dse-db 0517567f / client 1546323 / **24 flags identical** — unchanged.
- Gates: **G1 = 0** SPLAT lines (control intact). G3 shows the pre-SPLAT enum only (SOURCE_PRETOUCH,
  SIMILARITY_ORDINALS, PQ_RETRAIN, CODE_PRE_ENCODE, BASE_LAYER, UPPER_LAYERS, FINALIZE). G5 cost 0,
  integrity 0, max 0. G4 arm live: 312k r/s @ **0.19 ms** r_await, above the 190k sync ceiling. G6 satisfied.
- Phase: **still ingesting** — 6 `load_increment_adaptive` rounds started, latest returned 08:11:56; no
  `settle_compactions` yet, so the tail has not begun. 56.9M rows in at t+3h55m (~14.6M/h).
- Walls (corrected matcher): solo 4M **0.69 / 0.90 / 0.87 / 0.87**; light co-scheduling **1.79 / 2.25 / 1.80
  / 2.34**; **starved 4M 15.69 / 17.97 / 10.57** (Run F armed band 6.28–7.40); 16M **3.65 / 3.84** (Run F
  4.48 / 4.34 / 4.50). In flight: 4M from 08:09 (36m), 16M #3 from 08:21 (24m).
- Table 396 GB, 18 sstables — largest 23.3 / 23.2 / 23.2 GB (three 16M-class outputs; a giant needs four).
  Pass µs/node 10.3–40.9 with one 399.0 (single occurrence, below the two-strike flag). Cgroup anon 105.9G /
  file 38.4G.
- Trend: the null region holds where predicted and the first saturation-sensitive measurement lands
  decisively against the control — if that carries into the tail, SPLAT is earning its keep exactly where the
  theory says it should.

### 09:42 UTC — t+4h55m — giant precondition met; starvation signal repeats; µs/node flag tripped (pushed)

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0** SPLAT lines; G3 pre-SPLAT enum only; G5 cost 0 / integrity 0 / max 0. **Flag tripped:**
  ordinal passes at **399.0 and 544.7 µs/node** (two >100 occurrences → PushNotification sent). Both are
  contention-driven and land beside starved walls, so they corroborate C2 rather than indicate a fault; Run E
  precedent for a starved pass is 4,555 µs/node.
- Phase: **still ingesting** — 7 `load_increment_adaptive` rounds, latest returned 09:03:01; no
  `settle_compactions` yet. 68.5M rows at t+4h55m (~13.9M/h).
- Walls: starved **18.23** (72.3 min) and **7.19**; lightly co-scheduled 1.80 / 2.34 / 1.61 / 2.33; **16M #3
  = 3.82** (Run F 4.48 / 4.34 / 4.50 — the control's three 16Ms are now 3.65 / 3.84 / 3.82, tightly clustered
  and ~15% under Run F).
- **Giant precondition met**: four 16M-class sstables now on disk (23.3G 05:32, 23.2G 06:54, 23.2G 08:17,
  23.2G 09:39). Nothing giant-class has been selected yet — largest stage in flight is a 15.86M
  SOURCE_PRETOUCH/SIMILARITY_ORDINALS pair started 09:39–09:40, plus a 3.97M. Expect the giant within the hour.
- Table 445 GB, sstables 18 → **13** (consolidating). Device caught in an inter-merge lull: 6.4k r/s @ 81 KB,
  r_await 0.44 ms, 22% util — sequential/low-IOPS, not a storm, so G4's arm check defers to the next storm.
  Cgroup anon 105.9G / file 38.3G.
- Trend: the two-regime split is holding cleanly — 16M walls tightly ~15% better than Run F while starved 4Ms
  run 2–3× worse — and the first giant-class measurement, the one that actually tests amortization, is imminent.

### 10:42 UTC — t+5h55m — ingest pace is at parity with Run F (correction); storm confirms the arm; still no giant

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0. **G4 now positively confirmed** — the deferred arm check
  caught a real storm this hour: **252k r/s @ 4.6 KB, r_await 0.19 ms, 100% util**, above the 190k @ 0.22 ms
  sync ceiling. A third pass over 100 µs/node (404.5, after 399.0 and 544.7); already flagged, still tracking
  starved merges rather than a fault.
- Phase: **still ingesting** — 8 rounds, latest returned 09:48:32; no `settle_compactions`. 77.5M rows.
- **Ingest-pace claim withdrawn before it was made.** Comparing whole-run averages (Run G ~13.1M/h vs Run F's
  5.1M/h) would have been wrong: Run F's average is dragged down by late-run servo throttling. Like-for-like,
  **both runs completed 7 load rounds at the same point** — Run G at t+5h01m, Run F at ~t+5h13m. Ingest pace
  is at parity so far; no conclusion is available yet, and the meaningful test is whether Run G's servo
  throttles later as debt accumulates, or defers that debt into the tail.
- Walls: nothing new landed this hour — three merges in flight, and the shape is the story: a 4M running
  **70 min** (≥17.6 min/M floor already, a fourth severe starvation for C2), a 16M at 59 min, a 4M at 34 min.
- **Still no giant.** Four 16M-class sstables have been available since 09:39 (t+4h52m) but nothing
  giant-class has been selected an hour later; the largest stage since then is 15.86M. Run F's first giant
  passed at t+8h23m, so this is not yet late — but selection latency after availability is now worth tracking
  in its own right, since the tail's length depends on when the *final* giant can form.
- Table 587 GB, sstables 13 → **25** (fresh flushes outpacing consolidation). Cgroup anon 105.9G / file 37.8G.
- Trend: no new landed walls, but the in-flight shape keeps reproducing C2's contention penalty, and the
  giant — the first genuine test of amortization — remains pending.

### 11:42 UTC — t+6h55m — GIANT #1 STARTED (pushed); Run F's giant anatomy recovered, defining the target

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 289k r/s @ 6.6 KB, r_await **0.20 ms**.
- **GIANT #1 STARTED 11:07:29 on CompactionExecutor:37 — 63,579,887 ordinals, the same class as Run F's
  #1 (63,576,748) and #2 (63.46M).** SOURCE_PRETOUCH completed 11:11:18 in **3.82 min** (Run F giants:
  4.84 / 5.38 / 1.85 — in family, no signal); now in PQ_RETRAIN. It started at **t+6h20m vs Run F's first
  giant at t+8h23m**, i.e. ~2 h earlier in the run, so the selection-latency worry from 10:42 is resolved:
  the delay was the strategy waiting for its moment, not a pre-SPLAT deficiency. Expect BASE_LAYER to run
  ~10 h; landing due late tonight.
- Phase: **still ingesting** — 8 rounds, latest returned 10:56:08; no `settle_compactions`. 87.8M rows.
- Walls: starved 4M **18.36** and **10.86** (starved-4M count now **7**, none below Run F's 6.28–7.40 band);
  clean 4M 1.90 / 2.69; **16M #4 = 3.93** — the control's four 16Ms are 3.65 / 3.84 / 3.82 / 3.93, remarkably
  tight and ~13% under Run F's 4.48 / 4.34 / 4.50. In flight: 4M from 10:56, 16M from 11:07, plus the giant.
- Table 678 GB, sstables 25 → **20**; largest now **92.9 GB** — the giant's data-side output is already
  written and parked, with the vector index build (the slow half) the thing actually running. Cgroup anon
  106.0G / file 37.4G. µs/node 9.5–44.2 this hour, no new >100.
- Trend: the experiment's decisive measurement is now in flight, and thanks to the archive we know exactly
  what it has to beat — 9.34 / 9.61 min/M of BASE_LAYER.

### 12:42 UTC — t+7h55m — giant in setup: PQ_RETRAIN 35.76 min, now on SIMILARITY_ORDINALS; monopoly starvation begins

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 272k r/s @ 6.3 KB, r_await **0.21 ms**.
- **Giant #1 setup, stage by stage:** SOURCE_PRETOUCH 3.82 min → **PQ_RETRAIN 35.76 min** (2,145,571 ms,
  256,000 units) → SIMILARITY_ORDINALS started ~12:23. Run F's giants took 25.17 / 14.20 / **0.19** min on
  the identical 256,000-unit PQ_RETRAIN, and that 130× internal spread makes the stage a **contention proxy,
  not a SPLAT signal** — giant #3's 0.19 min came when it ran alone in the tail. Read narrowly, Run G's 35.76
  vs Run F #1's 25.17 (both mid-ingest) is +42% and weakly corroborates C2; it is n=1 vs n=1 and should not
  be leaned on. BASE_LAYER — the number that matters (C3: **9.34 / 9.61 min/M** to beat) — should begin
  ~13:00 and run ~10 h, so the landing is due ~23:00 UTC.
- Phase: **still ingesting** — 9 rounds, latest returned 12:02:09; no `settle_compactions`. 93.2M rows.
- No walls landed this hour. Both in-flight merges are being crushed by the giant's monopoly: a 4M running
  **105 min** (≥26 min/M floor — worse than any completed starvation so far) and a 16M at 94 min. Run F's
  armed band under a 16M was 6.28–7.40, with 92.23 the deep-monopoly record; Run G is heading into that
  regime with its first giant.
- Table 747 GB, sstables 20 → 26, largest 92.9 GB (giant's parked data output). µs/node unchanged this hour
  (9.5–44.2, no new >100). Cgroup anon 106.0G / file 37.2G.
- Trend: setup stages are contention-noisy and prove nothing either way; everything now waits on BASE_LAYER,
  which starts within the hour.

### 13:42 UTC — t+8h55m — BASE_LAYER UNDERWAY (started 13:12:58); ordinal plan costs 3× without SPLAT (C4)

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 271k r/s @ 6.8 KB, r_await **0.21 ms**.
- **Giant #1 setup complete, BASE_LAYER started 13:12:58.** Full setup: PRETOUCH 3.82 → PQ_RETRAIN 35.76 →
  **SIMILARITY_ORDINALS 73.0 min** (C4: 3.1× Run F #1's 23.21) → CODE_PRE_ENCODE 12.88 (in family). If
  BASE_LAYER matches Run F's 593.7–609.9 min it completes ~23:06–23:23, and with UPPER_LAYERS/FINALIZE the
  wall lands ~23:45–00:00 for **≈10.1–10.4 min/M — parity with Run F's 10.11 / 10.21**. So the whole question
  is now whether BASE_LAYER's own rate beats or misses **9.34 / 9.61 min/M**; first progress readings next check.
- Phase: **still ingesting** — 9 rounds, latest returned 12:02:09. 97.9M rows (~49% of target).
- Walls: **16M under the giant = 7.95** (126.2 min, pass 11:07). Useful context — Run F's mid-class victims
  measured 18.20 / 18.28 but that was in its *tail*; its two-wide mid-ingest pair was 11.99 / 10.06, so the
  control's 7.95 under a monopoly is better than Run F's two-wide penalty and not directly comparable to the
  tail figures. In flight: the 4M from 10:56 now at **165 min (≥41.6 min/M floor)** — deep monopoly
  starvation, heading toward Run F's 92.23 record territory; and the giant itself at 42 min past its pass.
- Table 813 GB, sstables 26 → 32. µs/node 9.6–68.9 (the 68.9 is the giant's own ordinal pass, per C4).
  Cgroup anon 106.0G / file 37.2G.
- Trend: setup is behind us with one real SPLAT win banked (C4, worth ~8% of a giant), and the stage that
  carries the other 90% is now running against a known target.

### 14:42 UTC — t+9h55m — FIRST DECISIVE READING: control's BASE_LAYER is 1.6× slower than Run F (C5, pushed)

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 249k r/s @ 6.9 KB, r_await **0.22 ms**.
- **Giant BASE_LAYER: 9.48% in 90.4 min → 15.00–15.75 min/M projected, vs Run F's 9.34 / 9.61** (C5). Not a
  wedge and not warm-up: threads confirm 40 workers computing, and the trailing-30-min rate is slightly
  *slower* than the overall. Projected stage end ~05:07 on 09-01; projected wall ≈15.6 min/M against Run F's
  10.11 / 10.21.
- Phase: **still ingesting** — 10 rounds, latest returned 14:09:57. **101.7M rows — past halfway.**
- Walls: none landed this hour. The 4M from 10:56 is now at **225 min (≥56.7 min/M floor)**, deep in
  monopoly starvation and closing on Run F's 92.23 all-time record; the giant itself is 102 min past its pass.
- Table 857 GB, sstables 32 → 37 (flushes accumulating behind the giant). µs/node unchanged. Cgroup anon
  106.0G / file 37.0G.
- Trend: the saturated-regime measurement has arrived and it is decisive so far — SPLAT is buying ~1.6× on
  the stage that is 90% of a giant, which is precisely the amortization claim under test.

### 15:42 UTC — t+10h55m — C5 WITHDRAWN as stated (pushed): early-rate projection was apples-to-oranges

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 247k r/s @ 6.9 KB, r_await **0.22 ms**.
- **Self-correction.** The 14:42 "SPLAT is 1.6× faster" reading compared Run G's early rate to Run F's
  whole-stage average. Run F's giant ran 58–74k nodes/min through most of BASE_LAYER yet averaged 107.1k —
  it accelerated ~2.7× late. Like-for-like, **Run G 63.8k vs Run F 73.4k = ~15% slower**, not 60%. A
  correcting PushNotification was sent. Only the completed stage line will settle this.
- Giant: 14.95% after 149 min; first decile line fired 14:48:39. Overall 63.8k/min, trailing-45-min
  58.4k/min — mildly decelerating so far, which is exactly what Run F also did before its late acceleration.
- Phase: **still ingesting** — 10 rounds, latest returned 14:09:57. 105.1M rows.
- Walls: none landed. The 4M from 10:56 is now **285 min (≥71.8 min/M)**, closing on Run F's 92.23 record;
  the giant is 162 min past its pass. Table 892 GB, sstables 37 → **41** (debt accumulating behind the giant).
  Cgroup anon 106.0G / file 36.9G.
- Trend: the honest position is that the giant comparison is still open, and the interesting question has
  sharpened — not "is the control slower" but "does the control show Run F's late acceleration at all".

### 16:42 UTC — t+11h55m — giant structure decoded (C6): the test is now cross-source amortization

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 235k r/s @ 6.8 KB, r_await **0.23 ms**.
- **Giant: batches 105,320/123,940 (85.0%) but ordinals 13.48M/63.58M (21.2%)** — the apparent contradiction
  decodes the structure: one batch cycle is exactly one 15.86M L0 source, so the giant is four cycles and it
  is still inside **source 1**, 85% done after 209 min. Rate steady at 64.5k/min overall, 66.2k last hour;
  zero batch resets so far, so source 2 begins ~17:20. C6 reframes the whole test around whether sources
  2–4 amortize — Run F's did (216 → 80 min/source, 2.7×).
- Phase: **still ingesting** — 10 rounds, latest returned 14:09:57. 108.0M rows.
- Walls: none landed. The 4M from 10:56 is at **345 min (≥86.9 min/M)** — now within 6% of Run F's all-time
  starvation record of 92.23, and it will pass it if it does not land in the next ~20 min.
- Table 929 GB, sstables 41 → **45**; debt keeps stacking behind the monopoly. Cgroup anon 106.0G / file 36.8G.
- Trend: the giant's per-source structure converts an ambiguous rate comparison into a decisive one, and the
  answer arrives with source 2 rather than at stage end.

### 17:42 UTC — t+12h55m — C6's first data point: source 1 = 252 min; source 2 opens faster; new starvation record

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 239k r/s @ 6.8 KB, r_await **0.22 ms**.
- **C6 first result: giant source 1 completed in 252 min** (13:12 → 17:24), against Run F's ~216 min average
  across its sources 1+2 — the control is ~17% slower, matching the early-window figure and the null-region
  overhead seen at every other class. **Source 2 started 17:24** and is 9.4% through its cycle after 17 min,
  which extrapolates to ~181 min — i.e. *faster* than source 1, hinting the control amortizes somewhat too.
  17 minutes is far too little to call: early-cycle rates are the least representative part of a source, and
  this is exactly the kind of extrapolation that produced the withdrawn C5. Treat as unresolved.
- The three scenarios now bracket cleanly: uniform 252 min/source → stage ≈1,008 min (15.9 min/M, SPLAT wins
  1.7×); Run F-like decline (252/181/80/80) → ≈593 min, parity, no giant-scale win; anything between is a
  partial-amortization result. Source 2's completion (~20:30) is the real answer.
- Phase: **still ingesting** — 10 rounds, latest returned 14:09:57. 110.5M rows.
- **New all-time starvation record.** The 4M from 10:56 has now been in flight **405 min = ≥102 min/M**,
  passing Run F's 92.23 record (E18) while still unfinished. Its final wall will set the mark. This is C2 at
  monopoly scale and the clearest cost of the control's weaker co-resident protection.
- Table 971 GB, sstables 45 → **48**; a fresh 4M started 17:20. Trend: the giant's amortization question now
  has a concrete deadline — source 2 lands around 20:30 and settles whether SPLAT's giant-scale win is real.

### 18:42 UTC — t+13h55m — control amortizes too, but shallowly; at the giant's midpoint the two builds are within 3%

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 235k r/s @ 6.8 KB, r_await **0.23 ms**.
- **C6 sharpens again.** Source 2 is 40% done in 77 min → ~192 min, confirming last hour's hint was real
  rather than an early-cycle artifact: the control amortizes **1.31×** (252 → 192). Run F amortized **2.7×**
  (~216 → ~80). Crucially, **sources 1+2 project 444 min vs Run F's 432 — a 2.8% gap.** Through the first
  half of the giant the two builds are effectively tied, and *all* of Run F's advantage is in sources 3–4
  (160 min for the pair). Stage projections: ~828 min if the control flattens at 192/source (13.0 min/M,
  1.39× Run F); ~684 min if it keeps declining (1.15×). Overall node rate has risen 64.5k → 67.7k/min as
  source 2 proceeds, exactly as an amortizing build should look.
- Phase: **still ingesting** — 10 rounds, latest returned 14:09:57. 112.7M rows. **Table crossed 1 TB**
  (1,007 GB), sstables 48 → **51**.
- Walls: none landed. The starved 4M is now at **465 min (≥117 min/M)**, extending its new record well past
  Run F's 92.23; a second 4M has been waiting 82 min behind it. The giant is 342 min past its pass.
- Trend: the giant is no longer a story about raw speed — both builds cost the same for the first half, and
  the experiment now turns entirely on how steeply each one amortizes across the back half.

### 19:42 UTC — t+14h55m — source 2 tracking to 197 min; ingest throttling under a giant is identical in both runs

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 235k r/s @ 6.9 KB, r_await **0.23 ms**.
- **C6 holds.** Source 2 is 70% done after 138 min → **197 min projected** (was 192). Sources 1+2 =
  252 + 197 = **449 min vs Run F's 432, +4.0%** — still effectively tied through the giant's first half, with
  the verdict resting entirely on sources 3–4 (Run F: 160 min for the pair). Source 2 completes ~20:41, so
  source 3's opening rate is the next real signal. Overall node rate continues climbing — 64.5 → 67.7 →
  **69.3k/min** — the expected shape of a build that amortizes, if shallowly.
- **Instrument fix.** Source position must be derived from the **ordinal count** (`n / 15,864,320`), not from
  batch-counter resets: the log rotated this hour, the pre-17:24 reset scrolled out of the live file, and the
  boundary detector silently relabelled source 2 as "source 1 at 389 min". Ordinal-derived position is
  rotation-proof and agrees exactly (26.97M → source 2, 70%).
- **Ingest throttling is not a control-specific effect.** Run G has completed **1** load round since its
  giant began (13:12 → now, 6.5 h); Run F completed **1** during its own giant #1 monopoly (09:00 → 19:43,
  10.7 h). The current ~2.1M rows/h crawl is what a giant does to ingest in both builds — no signal, and a
  claim I would otherwise have been tempted to make.
- Phase: **still ingesting**, 114.9M rows. Table 1,042 GB, sstables 51 → **53**.
- Walls: none landed. Starved 4M now **525 min (≥132 min/M)**, record still extending; a second 4M waits at
  142 min. Giant 402 min past its pass.
- Trend: nothing has moved the giant verdict this hour — first half remains a tie, and the answer arrives
  with source 3 in about an hour.

### 20:42 UTC — t+15h55m — source 2 finishing at 201 min; source 3 (the answer) starts within minutes

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 230k r/s @ 6.9 KB, r_await **0.23 ms**.
- **C6: source 2 is 98% done at 197 min → 201 min.** Sources 1+2 = 252 + 201 = **453 min vs Run F's 432,
  +4.9%.** The first half of the giant is confirmed a near-tie, and **source 3 begins within minutes** — its
  rate is the whole answer. Overall node rate 70.0k/min, still inching up (64.5 → 67.7 → 69.3 → 70.0).
- Phase: **still ingesting**, 116.7M rows; last completed load round remains 14:09:57 (the giant's monopoly,
  matched by Run F's behaviour — see 19:42).
- Walls: none landed. Starved 4M at **586 min (≥147.6 min/M)** — 1.6× past Run F's 92.23 record, and it is
  still running; the second 4M is at 202 min (≥50.9 min/M), itself already beyond Run F's starved band.
- Table 1,075 GB, sstables 53 → **56**. Cgroup steady.
- Trend: an hour with no new information by design — source 2 merely confirmed its projection, and the
  decisive source-3 rate lands in the next check.

### 21:42 UTC — t+16h55m — source 3 is 1.8× faster: the giant-scale win shrinks to ~1.17×

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 231k r/s @ 6.6 KB, r_await **0.23 ms**.
- **The C6 answer arrived.** Source 2 finished at **211 min**; **source 3 is 39% done in 46 min → ~117 min**,
  i.e. 1.8× faster than source 2. Per-source: **252 / 211 / ~117 / ?**. The control amortizes properly — just
  less steeply than Run F (~216 → ~80, 2.7×). Projected stage **697 min = 10.96 min/M vs Run F's 9.34**, a
  **1.17× SPLAT win**; giant wall ≈750 min ≈**11.8 min/M** vs 10.11 / 10.21. Overall node rate jumped 70.0 →
  **74.6k/min**, confirming the acceleration is real rather than a sampling artifact.
- Caveat: 46 min at 39% is a better sample than source 2's opening but still partial, and source 4 is
  unmeasured. The stage-completed line (~00:50 on 09-01) remains the authority.
- Phase: **still ingesting**, 117.9M rows. Table 1,117 GB, sstables **57**.
- Walls: none landed. Starved 4M at **646 min (≥162.7 min/M)**; second 4M at 262 min (≥66 min/M) — both far
  past Run F's 92.23 record and 6.28–7.40 band respectively.
- Trend: the giant-scale verdict is converging on *modest* (~1.17×) rather than decisive, which raises the
  stakes on the tail — the metric that was always meant to decide this.

### 22:42 UTC — t+17h55m — source 3 lands at ~115 min; stage projection stable at 1.17×

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0**; G5 cost 0 / integrity 0 / max 0; G4 storm 233k r/s @ 6.6 KB, r_await **0.23 ms**.
- **Giant, 72.9% complete (46.33M/63.58M at 22:42:11).** Source 3 is 92% done at 106 min → **115 min**,
  confirming last hour's 117 estimate rather than drifting. Per-source **252 / 211 / ~115 / pending**; stage
  projects **694 min = 10.91 min/M vs Run F's 593.71 = 9.34, a steady 1.17×**. Source 3 completes ~22:51 and
  **source 4 — the last unknown — begins immediately after**; if it matches source 3 the projection holds, and
  the authoritative `Stage BASE_LAYER completed` line arrives ~00:47 on 09-01.
- Phase: **still ingesting**, 119.0M rows (59.5%); last completed load round still 14:09:57 — the monopoly
  continues to hold the servo down, matching Run F's behaviour under its own giant.
- Walls: none landed this interval. Starved 4M at **705 min (≥177.6 min/M)**, second 4M at 322 min
  (≥81 min/M) — the latter alone is now approaching Run F's all-time record of 92.23.
- Table 1,162 GB, sstables 57 → **58**. Cgroup steady.
- Trend: the 1.17× giant-scale figure has now held across three consecutive checks, so the merge-scale
  verdict is effectively settled pending source 4 — and the tail remains the open question.

### 23:47 UTC — t+19h00m — GIANT LANDED (pushed): wall 10.18 min/M, a dead heat with Run F's 10.11/10.21

- Provenance: pid 1544653 / 6dcb0e4c / 0517567f / client 1546323 / 24 flags identical — unchanged.
- Gates: **G1 = 0** SPLAT lines across the entire run; G5 cost 0 / integrity 0 / max 0. Device eased to 156k
  r/s @ 4.7 KB, r_await **0.17 ms** as the monopoly released.
- **The decisive merge-scale measurement is in (C7).** `Stage BASE_LAYER completed … in 37,619,398 ms` =
  **627.0 min = 9.86 min/M** (Run F 593.71 = 9.34, so **+5.6%**), then UPPER_LAYERS 46 ms, FINALIZE 1.0 s,
  TERMS_DATA written **23:47:14**. Wall from the 13:00:04 pass = **647.2 min = 10.18 min/M**, sitting between
  Run F's two 63.5M giants (10.11 and 10.21). End-to-end from pretouch: **759.8 vs 695.8 min = +9.2%**.
  My 23:25 projection of 626 min was accurate to 1 min — but the earlier 15.0–15.7 min/M projections were
  wrong by 50%, which is the standing lesson about extrapolating from partial stages.
- Per-source, final: **252 / 211 / 116 / 47 min** — a 5.4× decline that closely tracks Run F's own
  amortization curve. The control amortizes; it is simply a few percent behind at each step.
- Phase: **still ingesting**, 120.2M rows (60.1%); a fresh 4M started 23:45 as the pool reopened. Table
  1,254 GB, sstables **61**.
- Trend: the giant is a near-tie, so the experiment now rests entirely on the tail and on the contention gap
  — exactly where the run started, but with the merge-scale question answered rather than assumed.
