# Run E watch log — full-fix stack, 200M attempt (2026-08-27 01:57 UTC)

Successor to `runwatch-20260826-runD.md` (Run D: integrity guard fail-stopped the node
at t+2h17m on an EMPTY vector index — root-caused to the `isEmpty()` async race, fixed).
Run E is the first run with every known defect fixed on both ends.

## Stack
- **Client**: `nmbrs` pid **4007198** (process name `nmbrs`; pgrep 'nmbrs run', mind the
  self-match trap; verify via fuser on the session metrics.db), session
  `sessions/stcs_adaptive_20260827_015712`, started 01:57:12. Binary = the 01:37 build:
  breaker_watch server-independent sensor (b8506e1/121fc23), all six dead `{part}`
  selectors fixed (27b27b5 + 673988e: `part_str` + `metricsql_scalar_dyn`), metricsql
  guards (b5c623f). KNOWN COSMETIC: `breaker-watch tick` INF lines at 1/s — the
  stdout-channel excision fix (910709e) landed after this client started; silenced on
  its next restart.
- **Server**: Cassandra pid **4005715** up 01:53:45 (new_cass wipe), jar built 00:49
  **with the `VectorMergeSegmentBuilder.isEmpty()` fix** (rowsWithVector counter, not the
  racing async postingsMap) + the VectorIndexIntegrity fail-stop guard
  (RestartPreventExitStatus=100 drop-in: an abort HOLDS THE NODE DOWN for inspection —
  do not blindly restart). jvector jar unchanged: `33f1202c` (7de94e83). Flags verified
  live: similarityOrdinals=true, clusterSearch=false, sourcePretouchMaxNodes=-1,
  WindowNodes=1048576, frontierPrefetch unset (=3). MemoryHigh=146G (~38 GiB file cache).
- CUT ALL LOG ANALYSIS AT 2026-08-27 01:57:00. compaction.log HAS ROTATED during this
  run already — include .zip/rotated archives when computing per-merge walls.

## Discipline (unchanged — rationale in Run C/D headers)
WALL min/M (`Similarity ordinals assigned` → `TERMS_DATA written in place`, matched per
merge) is the metric of record; batch/ordinal instantaneous rates are segments only.
Log fields $3=date $4=time; the run will cross midnight — filter
`($3=="2026-08-27" && $4>="01:57:00") || $3>"2026-08-27"`.

## References
- 16M class walls: Run D 4.29 min/M; Run C steady 4.5–4.8; Run E's first four:
  ..., fourth = **6.79 min/M** (09:12→10:59:49, co-ran against sibling 16Ms — within the
  historical 1.5–8.5 spread). Cluster-ON 9-src ref 2.31. Collapse era 8.5+.
- 63.6M class: Run C base layer 3.9±0.3 min/M through 99.99% (never landed — deadlock).
  Run E's 63.6M: pass 10:58:36 (32.5 µs/node), base clip ~4.7 min/M at 28%,
  projected land ~16:30±30m — would be the campaign's FIRST completed 63.6M wall.
- Ordinal-pass norm 11.5–12.5 µs/node; flag >100 twice per interval. Starved-pass
  outliers seen: 40.5 (Run D), 44.6 (Run E 09:01).
- 100M rows reached at ~t+10h08m; partition 10 (final 100M) began ~12:05.

## Entries

### 12:30 UTC — t+10h33m — armed; 100M in; 63.6M at 28%
- Parts 0–9 complete (100M rows), part 10 at 1.5M. Table 663.6 GB / 21 SSTables.
- 63.6M: 34,960/123,949 (28.2%), clip 414 b/min ⇒ 4.71 min/M. Land ~16:30±30m.
- Gate 0; integrity guard silent through 10.5h (isEmpty fix's first extended run);
  max=0; device 190k r/s @ 6.1 KB 100% util (expected storm).
- Client: breaker wires live (57,325 attempts/window), lint 0, no-data blips 36
  (cold-start cosmetic), ticks 38k (known, silenced next restart).
