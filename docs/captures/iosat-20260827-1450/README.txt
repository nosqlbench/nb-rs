Run E IO-pressure capture, 2026-08-27 ~14:50 UTC. 63.6M merge at ~74% (source group 3/4,
rank-2 source, 1 cross-source search/node). 146 compactor-pool samples over 3 dumps:
  95 RUNNABLE  readFully(RebufferingInputStream:141) <- readByteSequence(MemorySegmentVectorProvider:94)
               <- FusedPQ$PackedNeighbors.readInto(FusedPQ:224/227) <- getPackedNeighbors(OnDiskGraphIndex:667)
               <- enableSimilarityToNeighbors(FusedPQDecoder:88) <- processNeighbors(:692)
               <- FrontierPrefetchingView(:138) <- searchOneLayer(GraphSearcher:478)   [79% of RUNNABLE]
  14 RUNNABLE  readFloats(RandomAccessReader:186) <- getVectorInto(OnDiskGraphIndex:621)
               <- rescore(OnDiskGraphIndexCompactor:2414) <- gatherFromOtherSource(:2324)  [12%]
   5 RUNNABLE  adjacency ints/seek within getPackedNeighbors (:676/:666) — same family
  18 WAITING   acquireBuildPermit(SegmentBuilder:934) — ~6 builds queued behind the giant
   3 WAITING   joinAll(:2911) in buildSimilarityOrdinalMappers(:2864) — the NEXT merge's ordinal
               pass live-starved by the 63.6M base layer
   3 WAITING   runBatches drain (:2449) — the 63.6M orchestrator, normal
Device: md0 = 4x nvme, each ~46.8k r/s @ 6.03 KB, r_await 0.22 ms, util 100%
  => aggregate 187k r/s / 1.13 GB/s. Closure: ~40 sync readers / 220 us = 182k IOPS ~= measured.
pidstat: ~37 FJP workers x ~30.7 MB/s = the whole java stream; nothing else material.
