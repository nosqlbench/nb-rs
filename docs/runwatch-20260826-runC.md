# Run C diagnostic log — symmetric path, `clusterSearch=false`

Companion to `runwatch-20260826.md` (staging state + corroboration) and
`runwatch-20260825.md` (Runs A/B/B2). 30-minute entries appended below by the watch.

## Context

**Run C** = the overnight-prepared post-fix stack + one flag. Started
**2026-08-26 03:48** (session `stcs_adaptive_20260826_034801`), table wiped, cache-
constrained rig (`MemoryHigh=146G` → ~38 GiB page cache; above-RAM from ~05:00).

| | |
|---|---|
| jvector | `experiment/cluster-rescore-prefetch-20260824` @ `7de94e83`, jar md5 `33f1202c…` |
| cassandra | `4c29e7c6ee` + 29 uncommitted files (jar 01:06) — known provenance debt |
| flags | `similarityOrdinals=true`, **`clusterSearch=false`**, pretouch `-1`/`1048576` |
| decision | symmetric merges are the practical reality (STCS(4) equal sources defeat retain-largest's 0.5 base-share gate); optimize the symmetric path first |

**Config verification (done 05:58):** flag live on pid 3081002's cmdline; 5+ merges
completed with zero `Cluster path cost` lines (that line prints whenever the path
runs — silence + flag = path off); `REORDERED by the compactor` on every merge; zero
read-back assertions; ordinal passes at post-fix 11.5–11.9 µs/node.

**References.** Post-fix cluster-ON 9-src merge: 2.31 min/M. Run A/B2 16M-class on
this rig: 1.5–8.5 min/M, 5.5× spread, collapse ≈ 8.5+ min/M with the 5.6 KB / 300k r/s
/ high-util device signature. Merges are compared in **min per million ordinals**
(the progress line carries ordinals; batch counts are not comparable across merges).
Timestamp fields in Cassandra logs: `$3`=date `$4`=time — filter on **$4** (a $3
filter passed everything and produced one false alarm already this run).

**Early Run C series: RETRACTED — see the 06:55 correction entry.** The
ordinal-delta and batch-window rates both proved unreliable (source-group counter
resets; interleaved concurrent builds fragment the progress stream). Metric of
record is per-merge WALL: `Similarity ordinals assigned` start → `TERMS_DATA
written in place`, in min/M — the same method as the efficiency analysis.

**Watching for:** the larger classes (≥20M ordinals — overnight produced a 35.7M;
the historical decisive class is 126.9M) landing and their min/M; any `Cluster path
cost` line (= flag regression); ordinal passes >100 µs/node (the 05:18 outlier hit
445 µs during peak overlap — recurrence makes it the next symmetric-path target);
`memory.events max>0` (rig misbehaving); the saturation breaker (first run with it
live — `recent_attempt_*` scoped windows).

---

## Entries

| 05:58 (manual baseline) | t+2h10m, ~200 GB / 10 SSTables. Series and device as above. cgroup high-events 562k, max 0. |
| 06:26 | t+2h38m, 231 GB / 5 SSTables. Gate ✓ (0 cost lines, 9 REORDERED, 0 assertions). Completed since baseline: only the 06:00 close (1.37 min/M, already in series). **In flight: 15.9M at 5.69 min/M (17%)** — and the device has flipped to the read-storm signature: **5.35 KB @ 264–269k r/s, 100% util, iowait 29.4%** (was 90 KB @ 30% util). Ordinal passes: two more slow ones under merge overlap (98.9, 29.0 µs/node) between clean 10.4–11.9s; both 16M passes at 11.9. cgroup file 38.2 GiB, high-events 671k, max 0. Trend: the small-request read storm is back **with the cluster path off** — the plain symmetric searches alone produce it above RAM; rate swing 1.37→~5.7 min/M is inside the known 5.5× spread, so watching, not alarmed. |
| 06:55 | **CORRECTION + the real picture.** Three defects in my merge-rate derivation, found via a raw line reading `30940/30985 batches (3960320/15864790 ordinals)` — 99.9% batches, 25% ordinals: (1) the ordinal numerator resets per source group → ordinal-delta rates ~4× pessimistic; (2) batch windows measure only the L0 write phase; (3) with `concurrent_builds=2` two merges interleave one progress stream, so my per-merge segmentation manufactured "four accelerating 16M merges" out of fragments. **Wall-based truth (pass→TERMS pairing), all 9 completions since 03:48:** seven ~4M merges — 0.82/1.11/1.06/0.97 early, then 13.20 (the 52-min one starved under overlap), 2.68/0.58/1.96 — and **one 16M at 4.64 min/M (74-min wall)**. References on the same metric: pre-fix 16M 8.54, post-fix cluster-ON 9-src 2.31 (below-RAM, no overlap). So Run C's 16M sits between them, above RAM with a concurrent build — **not** the sub-reference miracle I reported at 05:58/06:26; those entries' rates are retracted (gate/config verification in them stands). Slow ordinal passes now 2×: 445 and **455 µs/node** (30 min for 4M), both fully overlapped by the 16M's read storm — starvation of the pass's full-precision encode reads, the clearest O4 motivation yet. Gate still ✓ (0 cost lines, 0 assertions). |
| 06:56 | t+3h08m, 261 GB / 10 SSTables. Gate ✓ (0/9/0). **No completions this interval** — two merges in flight: the 15.86M (pass 06:20 @ 12 µs/node; **36+ min elapsed and counting**) and the starved 4M (pass 06:47 @ 455 µs — the monitor's second slow-pass event, folded here). Device escalated to full collapse-era intensity: **5.13 KB @ 238–244k r/s, 100% util, iowait 40.4%** (29.4% at 06:26). cgroup file 37.9 GiB, high 909k, max 0. Trend: storm deepening while both in-flight builds crawl; the 15.86M's completion wall is the discriminator — ≤74 min repeats the 4.64 min/M shape, ≥95 min crosses the 6 min/M flag line. |
| 07:26 | t+3h38m, 279 GB / 13 SSTables. Gate ✓ (0 cost lines, 0 assertions). **Still no completions** — 15.86M now 70 min elapsed (**4.39 min/M and climbing**; crosses the 6 min/M flag line at ~07:51), starved 4M at 69 min (**17.35 min/M so far**). But **the storm broke** in the last minutes: md0 29k→2k r/s, util 60→22%, **iowait 0.0** (was 40.4% at 06:56) — a base layer finished its search phase. cgroup high 1.12M, max 0. Trend: the 15.86M is tracking the first 16M's ~74-min wall shape; its landing decides repeat (~4.6–5.0) vs flag (≥6). |
