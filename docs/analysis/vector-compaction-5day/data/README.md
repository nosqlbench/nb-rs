# Data for the 5-day analysis

**The measurement data is identical to the month-scale analysis** — every retained
vector merge (2026-08-21 .. 08-24, n=480) falls inside the 5-day window, so no
separate extraction exists or is needed. See
`../../vector-compaction-campaign/data/` for:

| file | contents |
|---|---|
| `history-merges.csv` | 480 merges with size class and regime |
| `history-pretouch.csv` | 82 pretouch calls with µs/ordinal |
| `cycle{3,4}-segments.csv` | per-segment cells/s |
| `provenance.csv` | run -> commit -> flags, recovered from JVM args |
| `extract*.py` | regenerate all of the above from the Cassandra logs |

`commit-window.csv` (here) is the one dataset specific to this analysis: every
commit on the deployed lineage, tagged with whether it falls inside the 5-day
window and what role it plays in the diagnosis. It is the evidence for §4.
