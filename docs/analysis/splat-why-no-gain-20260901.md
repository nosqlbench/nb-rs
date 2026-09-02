# Why SPLAT did not deliver the expected gains

**Date:** 2026-09-01 · **Question:** SPLAT was built to amortize work and cut read/write amplification in
large SAI vector merges. Run G (pre-SPLAT control, jvector `30cdfdaf`) versus Run F (SPLAT, `de79d5bf`),
with all 24 flags identical and `frontierPrefetch=16` on in both, shows near-parity at giant scale. Why?

**Evidence base:** 124 per-merge `Compaction stage times` summaries (65 Run F, 59 Run G) — the compactor
emits one per merge with every stage's elapsed time — plus the authoritative `Stage X completed … in N ms`
lines. This is a stage-resolved comparison on identical instrumentation, not an inference from walls.

## The finding in one line

**The stage SPLAT was built to improve — BASE_LAYER — costs the same in both builds.** Across every
giant-class merge in both runs:

| build | giants | BASE_LAYER total | ordinals | **per-ordinal** |
|---|---|---|---|---|
| Run F (SPLAT) | 3 (63.58M, 63.46M, 68.41M) | 115,205 s | 195.45M | **589.4 s/M** |
| Run G (control) | 2 (63.58M, 64.45M) | 76,696 s | 128.03M | **599.1 s/M** |

**A 1.6% difference**, well inside the 12% spread Run F's own three giants show from co-scheduling alone.
BASE_LAYER is ~87% of a giant's wall, so if it does not move, nothing moves.

## Where SPLAT's stages actually land

Averages per merge, seconds, by class (n in brackets):

| class | build | ORDINALS | BASE_LAYER | DISTRIBUTE | TOKEN_STREAM | wall |
|---|---|---|---|---|---|---|
| 4M | F [50] | **44.4** | 1353.6 | 990.6 | 140.6 | 1699.8 |
| 4M | G [48] | 576.8 | **1352.2** | — | — | 2092.8 |
| 16M | F [12] | **22.9** | 7870.9 | 1675.6 | 963.9 | 9488.9 |
| 16M | G [9] | 430.2 | **4206.4** | — | — | 5247.6 |
| giant | F [3] | **732.0** | 38401.7 | 211.5 | 1001.2 | 42241.8 |
| giant | G [2] | 2867.9 | **38348.4** | — | — | 44021.0 |

Three things follow.

1. **SPLAT's one real, reproducible win is the ordinal plan** (`8eba8b9e`, deriving the similarity-ordinal
   plan from the token stream): 13× cheaper at 4M, 19× at 16M, 3.9× at giant scale. It is genuine and shows
   at every class.
2. **That win is small in absolute terms and partly refunded.** At giant scale it saves ~2,136 s but SPLAT
   pays ~1,212 s back in `DISTRIBUTE` + `TOKEN_STREAM`, stages that exist only to support it. Net ≈ 1,650 s
   on a ~42,000 s merge — under 4%.
3. **BASE_LAYER is untouched at 4M and at giant scale** (1353.6 vs 1352.2 s; 589.4 vs 599.1 s/M). The 16M row
   favours the control, but those averages mix co-scheduling regimes and should not be read as a build
   effect — Run F's 16M sample includes its heavily-starved 11.99 and 18.2 min/M cases.

## Why BASE_LAYER didn't move — four candidate causes

**1. The prefetch arm already collects the win SPLAT was aiming at.** Both runs ran `frontierPrefetch=16`,
which hides the latency of random neighbour reads: during every storm in both runs the device sustained
230–320k r/s at **0.19–0.23 ms** r_await, against a 190k @ 0.22 ms synchronous ceiling. SPLAT reduces the
*number* and *locality* of those reads; the arm makes each one nearly free. Two mechanisms attacking the same
bottleneck are substitutes, and the cheaper one was already deployed. **This is the leading explanation and
it is directly testable** — see below.

**2. The base layer may not be IO-bound at all at this rate.** Thread samples during both giants showed ~40
ForkJoin workers inside `processBaseNode` / `gatherCandidates` / `gatherFromOtherSource` with the coordinator
parked on the task, and node rates pinned at 62–75k/min in both builds regardless of build. Device util is
100% but r_await stays flat — a saturated-but-fast device, not a queuing collapse. If the limiter is
similarity computation rather than fetch, reorganising IO cannot help.

**3. Cross-source amortization happens anyway, without the token stream.** The control's per-source costs
fall **272 / 200 / 134 / 44 min** — a 6.2× decline across four L0 sources, comparable to Run F's own decay.
Because sources are processed in similarity-ordinal order, later sources land on neighbourhoods the page
cache already holds. SPLAT makes this reuse explicit; the baseline was getting most of it implicitly.

**4. Write amplification was never the binding constraint.** Measured WA sat at 2.33–3.35 across both runs
with no systematic gap, so the write-side half of SPLAT's thesis had little to bite on in this workload.

## What SPLAT *does* buy, that this experiment nearly missed

The largest reproducible difference between the builds is not throughput but **behaviour under monopoly**:

- **SPLAT degrades co-residents; the control stalls them.** Under a giant, Run F's victim 4M merges kept
  completing at 6.28–7.40 min/M (worst 92.23). Run G's victims did not complete at all — two waited out
  the entire giant and landed at ≈98.7 and ≈193 min/M, the latter 2.1× Run F's all-time record.
- **The mirror image is recovery.** Run G resumes at full speed instantly (post-giant 4Ms at 1.10–1.25) with
  no backlog to clear, while Run F spent ~40 min ramping (3.29 → 1.17).

Same total work, very different latency distribution. If the goal is bounded tail latency for concurrent
compactions, SPLAT wins clearly. If the goal is merge throughput, it is a wash.

## The experiment that would settle cause #1

Run the same 200M load **with `frontierPrefetch` disabled**, both builds:

| | arm ON | arm OFF |
|---|---|---|
| SPLAT | Run F — measured | **needed** |
| control | Run G — measured | **needed** |

If SPLAT's advantage reappears with the arm off, causes #1 is confirmed and the correct conclusion is
"SPLAT and frontier prefetch are redundant; ship whichever is cheaper to maintain." If it stays flat, cause
#2 holds and the base layer is compute-bound, which redirects optimisation effort entirely — toward the
similarity/scoring path rather than IO scheduling. A cheaper first probe: profile the base-layer inner loop
for CPU-vs-stall breakdown on a single 16M merge under each build; that discriminates #1 from #2 in about
an hour rather than 2.5 days.

## Caveats

- n = 2 and 3 giants. Run F's own giants span 9.34–10.47 min/M on BASE_LAYER, so effects smaller than ~12%
  are not resolvable from this sample.
- The 4M and 16M class averages pool merges from different co-scheduling regimes; only the giant comparison
  is like-for-like, and even there the class sizes differ slightly (65.15M vs 64.02M mean).
- Three ledger claims in this campaign were withdrawn or revised after further sampling (C4, C5, and the
  1.6× early projection). Every number here comes from completed stage lines, not from extrapolating a
  partial stage.

## Addendum (2026-09-02): per-source decomposition sharpens the mechanism

All six giants across both runs now decompose per L0 source (minutes), which localises SPLAT's advantage
precisely:

| giant | src 1 | src 2 | src 3 | src 4 | total |
|---|---|---|---|---|---|
| Run F #1 63.58M | 254 | **181** | 116 | 43 | 594 |
| Run F #2 63.46M | 259 | **188** | 118 | 46 | 611 |
| Run F #3 68.41M | 328 | 212 | 126 | ~50 | 716 |
| Run G #1 63.58M | 252 | **212** | 116 | 47 | 627 |
| Run G #2 64.45M | 272 | **200** | 134 | 45 | 651 |
| Run G #3 63.46M | ~291 (97% at 04:48) | — | — | — | running |

Averaging the four same-class 63.5M giants:

| source | SPLAT | control | control slower by |
|---|---|---|---|
| 1 | 256.5 | 262.0 | **+2.1%** |
| **2** | **184.5** | **206.0** | **+11.7%** |
| 3 | 117.0 | 125.0 | +6.8% |
| 4 | 44.5 | 46.0 | +3.4% |
| **total** | **9.49 min/M** | **9.98 min/M** | **+5.2%** |

**SPLAT's entire giant-scale advantage is source 2.** Source 1 — where the token stream is being *built*
rather than exploited — is a dead heat (2.1%), and by source 4 both builds converge again (3.4%). The
mechanism is now specific: **the token stream front-loads amortization by roughly one source.** The control
reaches the same steady state on its own, one source later, via page-cache warmth from similarity-ordinal
ordering. SPLAT buys the head start, not a lower floor.

That also corrects the headline figure. The earlier per-ordinal comparison (589.4 vs 599.1 s/M = 1.6%) pooled
Run F's larger 68.41M giant, which is slower per ordinal and diluted the gap. **Restricted to same-class
giants the difference is 5.2%** — still small, still consistent with "no material gain," but the honest
number is three times the first estimate.
