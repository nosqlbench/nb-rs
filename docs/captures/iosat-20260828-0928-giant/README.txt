THE class-matched capture: Run F giant (63,576,748 ords) L0 storm under frontierPrefetch=16,
2026-08-28 09:28 (L0 began 09:25:00; opening clip ~555 b/min; #5 tail co-resident).
120 RUNNABLE FJP samples over 3 dumps:
  61 (51%) blocked readFully(:141) <- FusedPQ.readInto(:235/238) <- getPackedNeighbors(:704)
           [Run E giant, WIDTH=3: 79%]  <- THE ARM'S CLASS-SCALE VERDICT
  37 (31%) issuing willNeed hints (32 frontier hint(:183), 5 same-source(:2724))
           [Run E giant: 0 visible; Run C: 24%]
  14 (12%) rescore readFloats(:186) via gatherFromOtherSource(:2844)  [identical to C/E]
   6 ( 5%) search/scoring CPU + diversity
Device: 259k r/s @ 6.8 KB, per-member r_await 0.21 ms, util 100%, iowait 42.
Queue model: sync-only ceiling ~40/0.21ms = 190k; measured 259k => ~70k IOPS of landed
hint depth at giant scale (vs ~+120k at 4M scale — hint efficiency decays with working-set
depth but remains strongly positive).
