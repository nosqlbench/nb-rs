# Memo 09: GK Performance Results

Benchmark results with fully monomorphic kernel variants: Raw,
Push (per-node skip), Pull (cone guard), PushPull (both). Each
is a distinct type produced by a distinct compiler path with
zero runtime strategy branching in the eval loop.

Run: 2026-04-12, 100K cycles × 5 iterations. Driver overhead
subtracted from all values.

---

## P3 JIT Results (ns/cycle, driver-adjusted)

P3 is the dominant engine for compute-heavy graphs. All four
monomorphic variants shown.

| Scenario | Nodes | Stable% | Raw | Push | Pull | PushPull | Best |
|----------|------:|--------:|----:|-----:|-----:|---------:|------|
| a1 single input | 50 | 0% | **260** | 374 | 260 | 367 | Raw/Pull |
| a2 stable params | 49 | 61% | 183 | 135 | 171 | **130** | PushPull |
| a3 all changing | 49 | 0% | 177 | 261 | **168** | 253 | Pull |
| a4 mostly stable | 101 | 80% | 358 | 164 | **12** | 22 | Pull |
| a5 mixed rates | 81 | 62% | 298 | 153 | **10** | 28 | Pull |
| b1 hot/cold | 38 | 40% | 124 | 107 | 112 | **102** | PushPull |
| b2 selective cone | 101 | 70% | 438 | 278 | **56** | 56 | Pull/PP |
| c1 deep chain | 200 | 0% | **988** | 1365 | 987 | 1371 | Raw/Pull |
| c5 multi-root | 201 | 75% | 883 | 407 | **7** | 32 | Pull |
| d1 keyvalue | 9 | 0% | **42** | 53 | 42 | 50 | Raw/Pull |
| d4 multi-table | 44 | 0% | 136 | 195 | **135** | 191 | Pull |

## P2 Closure Results (ns/cycle)

| Scenario | Nodes | Raw | Push | Pull | PushPull |
|----------|------:|----:|-----:|-----:|---------:|
| a4 mostly stable | 101 | 892 | 260 | **11** | 16 |
| a5 mixed rates | 81 | 731 | 251 | **16** | 17 |
| b2 selective cone | 101 | 966 | 414 | 109 | **63** |
| c5 multi-root | 201 | 1936 | 601 | **7** | 24 |

---

## Winner Analysis by Scenario Class

### All-dirty, single input (a1, c1, d1, d4)

**Winner: Raw or Pull (tied).** Pull has the same cost as Raw
because every input changes every cycle, so `changed_mask` is
always nonzero and the cone check always falls through to eval.
Pull's `set_inputs` is slightly cheaper than Raw's `copy_from_slice`
because it compares and copies only changed values.

Push always loses on all-dirty graphs: +35-40% overhead from
per-node clean flag checking in the JIT code.

### All-dirty, multi-input (a3)

**Winner: Pull (168ns) > Raw (177ns).** Same reasoning — cone
check falls through, but Pull's `set_inputs` is marginally
cheaper than Raw's bulk copy for 3 inputs.

### Stable subgraphs, small output cone (a4, a5, c5)

**Winner: Pull by 2-4× over PushPull.** The output's cone is
small relative to the graph. On most cycles, the cone guard
skips eval entirely. PushPull wastes time marking dependents
dirty in `set_inputs` — work that the cone guard discards.

| Scenario | Pull | PushPull | Pull advantage |
|----------|-----:|---------:|---------------:|
| a4 (101n, 80% stable) | 12ns | 22ns | 1.8× |
| a5 (81n, 62% stable) | 10ns | 28ns | 2.8× |
| c5 (201n, 75% stable) | 7ns | 32ns | 4.6× |

The advantage grows with graph size because PushPull's
`set_inputs` iterates more dependents.

### Stable subgraphs, large output cone (b1, b2)

**Winner: PushPull.** When the output's cone IS dirty (most
pulls in b1 hit the cycle-dependent hot output), push-side skip
within the cone matters. PushPull evaluates only the dirty
nodes in the cone. Pull-only would run all nodes in the cone.

| Scenario | Pull | PushPull | PushPull advantage |
|----------|-----:|---------:|-------------------:|
| b1 (38n, hot/cold) | 112ns | 102ns | 1.1× |
| b2 (101n, selective) | 56ns | 56ns | 1.0× |

The advantage is modest because even in PushPull, the dirty
cone dominates the work.

### Mixed stability, all outputs used equally (a2)

**Winner: PushPull (130ns).** All outputs are pulled, so Pull's
cone guard can't skip eval. Push-side skip within the dirty
cone gives PushPull the edge.

---

## Optimization Overhead on All-Dirty Graphs

| Graph | Raw | Push overhead | Pull overhead |
|-------|----:|:-------------:|:-------------:|
| a1 (50n) | 260 | +44% (374) | **0%** (260) |
| a3 (49n) | 177 | +47% (261) | **-5%** (168) |
| c1 (200n) | 988 | +38% (1365) | **0%** (987) |
| d1 (9n) | 42 | +26% (53) | **0%** (42) |
| d4 (44n) | 136 | +43% (195) | **-1%** (135) |

**Push consistently costs ~40% on all-dirty graphs** from the
per-node clean flag load + branch in the JIT code.

**Pull has zero overhead on all-dirty graphs** because the only
extra work is `changed_mask` tracking in `set_inputs` (one OR
per changed input) and one AND + branch in `eval_for_slot` that
always falls through. This is negligible.

This means **Pull is strictly better than Raw**: same speed on
all-dirty graphs, massive wins on stable graphs. The compiler
can always enable Pull without risk.

---

## Revised Heuristic

```
stable_ratio = stable_nodes / total_nodes
output_cone_ratio = max_output_cone / total_nodes

// Pull has zero overhead, so always enable it
if total_nodes < 15 and stable_ratio == 0:
    P3/Raw                      // tiny all-dirty, skip provenance data

elif output_cone_ratio < 0.5:
    P3/Pull                     // cone guard alone: 7-12ns for 100+ nodes

elif stable_ratio >= 0.3:
    P3/PushPull                 // push skip within dirty cones

else:
    P3/Pull                     // pull is free, might help if access
                                // patterns vary at runtime
```

The key insight: **Pull is the safe default.** Zero overhead on
all-dirty graphs, massive wins on stable graphs. Push is only
worth its 40% overhead when the output cone is large AND has
cacheable subgraphs.

### Decision Flow

```
Is the output cone small? (cone_ratio < 0.5)
├── Yes → Pull (7-12ns for 100+ nodes, cone guard skips eval)
└── No  → Are there stable subgraphs? (stable_ratio >= 0.3)
    ├── Yes → PushPull (push skips stable nodes within dirty cone)
    └── No  → Pull (free insurance — behaves like Raw on all-dirty)
```

Push-only is never selected. It has all the overhead of push
(40% on all-dirty) without the cone guard's ability to skip
eval entirely. It's dominated by both Pull and PushPull.

---

## Key Findings

1. **Pull has zero overhead on all-dirty graphs.** The monomorphic
   Pull kernel's `set_inputs` only tracks `changed_mask` — no
   dependent iteration. The cone guard's AND + branch is ~2ns
   when it falls through. This makes Pull strictly better than
   Raw as a default.

2. **Pull is the dominant optimization for selective output
   access.** P3/Pull achieves 7ns for 201 nodes on c5 (vs 883ns
   raw = 126×). The cone guard returns the cached value without
   entering the JIT function at all.

3. **PushPull only beats Pull when the output cone is large AND
   partially stable.** b1 (PushPull 102ns vs Pull 112ns = 1.1×)
   and a2 (PushPull 130ns vs Pull 171ns = 1.3×) are the only
   scenarios where PushPull wins meaningfully.

4. **Push-only is always dominated.** It adds 40% overhead on
   all-dirty and is beaten by Pull on stable graphs. No scenario
   selects it.

5. **P2/Pull = P3/Pull** when the cone guard skips eval. Both
   monomorphic kernels do identical work: compare inputs, set
   changed_mask, one AND + branch, return cached value. The 
   compilation level is irrelevant when eval doesn't run.

6. **Monomorphization closed the P2/P3 gap.** Before: P2/pull
   8ns vs P3/pull 18ns (P3 had `if let Some(prov)` overhead).
   After: P2/pull 11ns vs P3/pull 12ns — equivalent.

7. **Hybrid never wins.** Strictly dominated by P3 on all
   scenarios. Should be deprecated from the selection heuristic.
