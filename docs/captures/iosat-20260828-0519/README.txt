Run F captures under frontierPrefetch=16 + jvector db987fd0 (de79d5bf), 2026-08-28.
STORM (05:19, 4M-class base layer, 130-313k r/s):  120 RUNNABLE FJP samples over 3 dumps:
  60 (50%) blocked readFully(RebufferingInputStream:141) <- FusedPQ.readInto(:235/238)
           <- getPackedNeighbors(OnDiskGraphIndex:704)          [Run E giant-storm: 79%]
  12 (10%) issuing willNeed hints (FrontierPrefetchingView.hint:183, FileHandleReaderSupplier:197)
   8 ( 7%) scoring lambda (searchOneLayer$0 via FrontierPrefetchingView:95) — CPU, not blocked
   9 ( 8%) mid-gatherFromOtherSource CPU; 6 diversity getVectorInto; 5 rescore(:2931)
  => blocked-read share 50% vs Run E's 79%; ~40% of pool computing vs ~15% in Run E.
  CAVEAT: 4M-class storm at 10.5KB requests (fresher sources, better readahead) — the
  class-matched giant capture is still pending.
LULL (05:18, between merges): 67/88 RUNNABLE in ProductQuantization.getNearestCluster —
  the ea38d33954 "cold-start PQ on the build pool" change visible live (PQ training now
  pool-wide instead of flush-thread-confined).
Line shifts (db987fd0): readInto 224->235, getPackedNeighbors 667->704, retainDiverse
  3067->3817, processBaseNode 1729->2226, compactLevels L0 loop 1462->1706.
