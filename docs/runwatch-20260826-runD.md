# Run D watch log — fresh 200M attempt on the fixed stack (2026-08-26 21:21 UTC)

Successor to `runwatch-20260826-runC.md` (Run C: killed at t+14h by the trainingLock
pool deadlock at 18:10, then table wiped). Run D is the first run where BOTH ends of
the stack carry the day's fixes.

## Stack
- **Client**: `nmbrs 0.1.2` pid **3842588** (note: process name is `nmbrs`, not `nbrs` —
  pgrep accordingly), rebuilt 20:17 from main @84cbc0f with the breaker fixes:
  phase-only windowed selectors (27b27b5 — `{part}` was inert in bindings) and the
  never-matching-selector guards (b5c623f: parse-time `"{ident}"` lint + 10-consecutive
  no-data warning). Session `sessions/stcs_adaptive_20260826_212108`, started 21:21:08.
  Same invocation as Run C (stcs_adaptive, 200M target, `^200m$` final recall).
- **Server**: Cassandra pid **3341212** up 18:17:26 on the 18:10 `dse-db-4.0.11.0-SNAPSHOT.jar`
  — includes the trainingLock deadlock fix (applied by the owning session; refine no
  longer wedges the shared pool). jvector jar unchanged from Run C: `jvector-4.0.1-SNAPSHOT`
  md5 `33f1202c` (= branch tip 7de94e83).
- **Flags (verified live in /proc/3341212)**: similarityOrdinals=true, clusterSearch=false,
  sourcePretouchMaxNodes=-1, sourcePretouchWindowNodes=1048576. frontierPrefetch unset (=3).
- **Rig**: MemoryHigh=146G → ~38 GiB file cache; table wiped 18:1x, so above-RAM arrives
  around the 30M-row mark (~t+1h) rather than Run C's t+1h10m.

## Discipline (unchanged from Run C — full rationale in that doc's header)
- CUT ALL LOG ANALYSIS AT 2026-08-26 21:21:00. Metric of record: WALL min/M
  (`Similarity ordinals assigned` → `TERMS_DATA written in place`). Ordinal-delta and
  batch-window rates are both invalid. Cassandra log fields: $3=date $4=time — filter on $4.
- pgrep self-match trap: filter your own shells; the client is `nmbrs`, pid 3842588.
- Breaker-wire check (new): the four `recent_*` gauges in the session db must be
  non-NULL while a loader phase is active (sqlite `sample_value.mean`); NULL-while-loading
  = the fix regressed. Validated live at 22:47: 51/51 samples, total=139,890, failures=0.
- Wedge watch (new, trainingLock-class): a mid-base-layer merge whose progress stream
  goes silent >7 min with the device quiet = suspected pool wedge → jcmd Thread.print
  immediately (capture first, alert second).

## Reference points
- Run C 16M class steady state (storm regime): 4.5–4.8 min/M. Early-run envelope: 6.35→1.37.
- Run C 63.6M base layer: 3.9±0.3 min/M through 99.99% (never landed — deadlock).
- Cluster-ON post-fix 9-src reference: 2.31 min/M. Collapse era: 8.5+ min/M @ 4–6 KB device storm.
- Run D first segments: 4M-class merges healthy; first 16M merge ran ~1.61 min/M
  mid-base at t+68m (early, partially cache-warm — not yet comparable to Run C steady).
- Ordinal-pass norm 11.5–12.5 µs/node post-fix; flag >100. One 40.5 outlier at 22:24 noted.

## Entries
