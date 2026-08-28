2026-08-28 09:05 capture — intended as the giant's storm; actually caught 16M #5's FINAL
GROUP (largest source: 0 forward searches, reverse-candidate folding only). First clean
profile of that group: 120 RUNNABLE FJP samples,
  92 (77%) readFloats(RandomAccessReader:186) <- getVectorInto(OnDiskGraphIndex:658)
           <- retainDiverse(OnDiskGraphIndexCompactor:3817)   [diversity candidate reads]
  22 (18%) willNeed hints in gatherFromSameSource(:2724); 5 same-source getVectorInto
  ZERO cross-source search / packed-neighbor frames — cross-link elision in action;
  the last group's cost is DIVERSITY VECTOR READS, not search.
Device during window: 289k r/s @ 6.1 KB, per-member r_await 0.20 ms, iowait 37.
Giant (63,576,748 ords, pass 09:00:40 @ 21.9 us/node — starved-class pass) was in its
setup phases behind #5 at capture time; its L0-storm capture re-armed separately.
