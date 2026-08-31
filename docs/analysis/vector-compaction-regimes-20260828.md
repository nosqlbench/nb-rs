# Vector-compaction completion rates across runtime regimes

**Date:** 2026-08-28 · **Scope:** SAI vector merges, `baselines.ibm_datapile_1b_default`, dse-db-4.0.11.0-SNAPSHOT + jvector db987fd0 (branch `experiment/cluster-rescore-prefetch-20260824` @ de79d5bf) · **Rig:** MemoryHigh=146G ≈ 38 GiB file cache, md0 NVMe raid (10 members)
**Updated 2026-08-31** with Run F's full completed record (giants #2/#3, E16–E18) recovered after the monitoring blackout.

**Evidence base:** Runs C–F walls (`docs/runwatch-20260826-runC.md`, `-runD.md`, `runwatch-20260827-runE.md`, `runwatch-20260828-runF.md` incl. CHANGE-EFFECTS LEDGER E1–E14) and the read-path deep dive (`vector-compaction-readpath-20260826.md`).
**Method note:** every wall here is pass→adopt→TERMS_DATA, ordinal-count matched ("Similarity ordinals assigned" → "adopted effective ordinal mapping" → "TERMS_DATA written in place"). Batch/ordinal counters lie at boundaries (resets, +1..+927 drift, 5× ordinal-density variance); they are used only for in-flight pacing, never for walls.

## Headline

Measured completion rates for identical work span **~70×** — 1.01 min/M (best solo 4M) to 79.3 min/M (pathological starved 4M, ledger E4). The variation factorizes into three multiplicative axes — **size class (= cache residency) × contention × read-path arm** — plus a phase mix inside giant merges that makes any single-number rate for that class misleading. A historic fifth regime (bistable cache collapse) was the campaign's original pathology and is now eliminated; what remains is a graded, predictable ladder.

## Axis 1 — Size class, which is really cache residency

Per-ordinal cost rises super-linearly with merge size because the source working set crosses the ~38 GiB file-cache budget and per-node graph reads (T×(k−1) fanout of random ~4,880 B records — see the readpath analysis) shift from cache hits to device IO:

| Class | Solo wall (min/M) | vs 4M | Regime | Source |
|---|---|---|---|---|
| 4M | 1.01–1.16 | 1× | cache-resident | Run F early cohort |
| 16M | 4.29–4.50 | ~4× | partially resident | Run D 4.29 (no arm) · Run C 4.5–4.8 · Run F 4.48/4.34/4.50 |
| 63.6M | **10.11 / 10.21** (642.7 / 647.9 min) | ~9× | IO-bound streaming, 310 GB source | Run F E13 + giant #2 (E17) — two same-class walls agree within 1% |
| 68.4M | **10.97** (750.8 min) | ~10× | same, larger source | Run F giant #3 (E17) — +7.7% ords → +7.4% per-ord |

A 4× size step costs ~4× per-ordinal at 4M→16M, then only ~2.3× more at 16M→63.6M — damped because the giant's later streams are cheap (Axis 4), not because IO pressure relents: stream-1 alone runs 3.89 min/M.

## Axis 2 — Contention: a multiplier on top of class

Same 4M work under different co-residency (Run F unless noted):

| Context | min/M | Multiplier |
|---|---|---|
| solo | 1.01–1.16 | ×1 |
| light co-scheduling (post-giant drainage burst) | 2.23–3.95, decaying monotonically to 1.17 in 40 min | ×2–3.5 |
| starved under a 16M — pre-arm (Runs C–E) | 10.6–18.6 | ×10–16 |
| starved under a 16M — armed | 6.28–7.40 | ×5.5–6.5 (inflation halved, E-ledger) |
| starved under the giant's monopoly | up to 13.40 | ×12 |
| fourth concurrent stream under the giant | 79.3 (5.24 h for 4M) | ×70 (E4) |
| deep monopoly, giant #2 (Run F, recovered) | **92.23** (6.10 h for 4M — all-time record) | ×80 (E18) |
| 16M–20M victims under a giant | 18.20 / 18.28 | ×3–4 of their own solo band (E18) |

**E14 (the giant's externality, priced):** the contention penalty exists only *during* a monopoly window. After E13 landed, the queued 4M backlog drained 3.95→1.17 min/M in under 45 minutes and the ingest servo hit a run-record 8.2M rows/h. A giant's total cost = its own wall + ~40 min of 2–4× degraded 4M walls; the horror walls happen only to victims co-resident mid-monopoly. Scheduler-fix sizing (server session's lane): permit priority/aging must protect co-residents *during* a giant; post-giant drainage needs nothing.

## Axis 3 — Read-path arm (sync vs frontierPrefetch=16)

Visible only where the workload is IO-latency-bound; same build (db987fd0), arm off (Run E) vs on (Run F):

- Giant stream-1 search: 4.6–5.0 → **3.89–3.96 min/M (~20% win)**.
- Device during the storm: 190k r/s @ 0.22 ms sync ceiling → 259–328k r/s @ 0.19–0.21 ms (~70–120k of it hint IOPS).
- Blocked-in-`readFully` thread share: 79% → 51% (E10 capture, `iosat-20260828-0928-giant`).
- **Invisible at 16M**: 4.48/4.34/4.50 armed vs 4.29 unarmed — cache-resident enough that hints buy nothing.
- **Blind to streams 2–4 by construction** — exactly the territory SPLAT (`vector_merge_splat_design.md`) targets. Token-stream emission (8aa6d329) is active and log-silent this run (E11); the staged-permutation machinery is the unexercised remainder.

## Axis 4 — Phase regime inside a giant: rate is not constant within one merge

E13 anatomy (63,576,748 ords, TERMS_DATA body 320.5 GB):

| Phase | Wall | Rate |
|---|---|---|
| setup (pretouch 310 GB + PQ) | 24.3 min | — |
| stream-1 search (arm-sensitive) | 247 min | 3.89 min/M |
| stream-2 | 183 min | 2.88 min/M |
| stream-3 token-stream emission | 115 min | 1.81 min/M |
| stream-4 (write-like) | 47 min | 0.74 min/M |
| upper/adopt tail | 19.3 min | — |
| TERMS_DATA write-back | 7.3 min | — |
| **total** | **642.7 min** | **10.11 min/M** |

A 5× rate spread inside a single merge; search is only 38% of the wall. Consequence: batch-clock extrapolation across a phase boundary is structurally wrong — it produced every "99.999% wedge" / "base complete" misread in Runs C and E (both were the stream-1 boundary). Cross-run comparisons must be per-phase or full-wall, never mid-flight.

## The eliminated regime — bistable cache collapse (historic)

The campaign's original pathology (server1, early August; memory `vector-merge-cache-bistability`) was a cliff, not a ladder: cache-hot merges at 5,387–27,530 batches/min vs an IO-bound absorbing state at 39–46 (~100–700× on the batch clock; 5–50× per-merge walls; `FusedPQ$PackedNeighbors.readInto` signature; self-reinforcing via servo throttle to w≈0.09). Capacity shaping plus the arm converted the cliff into the graded ladder above. Today's worst *systematic* regime is the giant's 10.11 composite; the worst *transient* one is monopoly starvation — a scheduler problem, not an IO one.

## Coupling to ingest

Compaction completion rate sets the servo's admission: run-record 8.2M rows/h with zero debt vs deep throttle during monopolies. The class ladder and the starvation window are therefore also the throughput ceiling of the whole pipeline.

## Implications

1. **Scheduler (server session):** permit aging scoped to mid-monopoly protection only (E14), covering every co-resident class — 4M through 20M victims all measured 3–80× penalties (E18). Also serialize same-class merges: two concurrent 16Ms cost 2.2–2.7× per-merge latency and buy nothing in throughput (190.1 min for the pair vs 143–180 min serial, E16).
2. **SPLAT:** streams 2–4 = 345 min of E13's wall (54%) are arm-blind — that is SPLAT's addressable budget.
3. **Arm applicability:** ship frontierPrefetch for giant-class search; expect nothing from it below ~16M residency.
4. **Measurement discipline:** walls only via pass→adopt→TERMS_DATA; per-phase accounting for any giant-class comparison.
