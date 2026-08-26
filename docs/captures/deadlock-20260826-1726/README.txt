FJP-wide deadlock, 2026-08-26 ~17:18 UTC (last merge progress 17:17:40).
Cycle: CompactionExecutor:51 SSTableIndexWriter.addRow -> CompactionGraph.maybeAddVector:358
  -> ProductQuantization.refine -> BoundedParallelExecutor.forEachInt -> Drain.settle (waits on pool)
  while holding/queued-for trainingLock WRITE;
ALL 40 ForkJoinPool-1 workers parked at CompactionGraph.addGraphNode:434 trainingLock.readLock()
  (lock 0x0000772260f43880, NonfairSync - queued writer blocks new readers);
training tasks queued behind 40 parked workers -> circular wait, total pool deadlock.
Victims: 63.6M merge (CompactionExecutor:38) starved in CompactionSort.sort forEachInt at
  compactLevels:1462, wedged at 123,940/123,941 batches (99.9992%); 5 threads in acquireBuildPermit;
  flush segment builds queued; device idle (1.5k r/s vs 190k during merge).
New-code surface: BoundedParallelExecutor.java (untracked WIP) + CompactionGraph trainingLock + InsertFanout.
