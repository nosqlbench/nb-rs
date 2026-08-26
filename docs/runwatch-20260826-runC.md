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

**Early Run C series (16M class):** 6.35 → 4.68 → 2.97 → **1.37 min/M**
(04:54–06:00), accelerating above RAM; device 89–91 KB requests at 10–11k r/s,
29–31% util, iowait 0.0 — opposite of the collapse signature. Four merges = shape,
not verdict.

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
