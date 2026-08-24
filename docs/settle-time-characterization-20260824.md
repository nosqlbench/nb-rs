# Time-to-settle after ingest — cycle 3, characterized 2026-08-24 06:45 UTC

Written before resetting for the `frontierPrefetch` sweep, so the sweep has a
baseline to beat and so the next run's monitoring uses the better metric found
here.

## Summary

**If ingest stopped right now, the node needs ~7 hours to settle — and that
figure is only valid if STCS does not schedule another large merge.** If it
does, add ~7 hours per merge, which is why cycle 1's `settle_compactions` never
drained in the 10.5 hours it was watched.

| component | remaining | estimate |
|---|---|---|
| in-flight 30,985-batch merge / 126.9M-cell index build | 80.1% | **~7 h** |
| 4 byte compactions parked at 0.00% | 43.6 GB | ~25 min |
| index build parked at exactly 50.00% | 1.98M cells | ~2–3 min healthy |
| index builds triggered by those 4 compactions | ~4 x 4M cells | ~20 min healthy |
| **total, no new large merge** | | **~8 h** |
| **each additional large merge STCS schedules** | | **+7 h** |

## A better metric than the batch counter

`SSTableIndexWriter.java:374` logs every completed segment:

    Flushed segment with 3966046 cells for a total of 18.114GiB in 299040 ms

This is the actual unit of settle work, it has an unambiguous denominator, and
40 of them completed in cycle 3. Healthy population (n=32 standard segments):

| | |
|---|---|
| size | 3,966,1xx cells / 18.1 GiB |
| duration | **median 277 s**, range 216–369 s |
| rate | **14,318 cells/s, 67 MiB/s** |

Two off-size points bracket the scaling:

| cells | GiB | seconds | cells/s | MiB/s |
|---|---|---|---|---|
| 3,966,100 (median of 32) | 18.1 | 277 | 14,318 | 67 |
| 16,013,399 | 73.1 | 1,943 | 8,241 | 38.5 |

**4x the cells costs 1.7x the per-cell rate.** Extrapolating the 126.9M-cell
segment now in flight gives 2.5 h (linear from the median) to 4.3 h (using the
16M point's degraded rate). The merge's own progress counter says ~7 h. The
three agree to within a factor of ~2.5 and all say hours, not days.

### The `nodetool` "token range parts" counter is unreliable — do not use it

`compactionstats` reports the same build as `793,344 / 126,916,949 token range
parts`, advancing at ~142 parts/s, which implies **10.3 days**. That is 30–100x
slower than every other measure of the same work and it should be disregarded.
The counter has the right total but does not advance in step with the merge:
at 05:06 it read 80,000 while the merge had done 570 batches x 4,096 = 2.33M
ordinals. Earlier entries in this watch quoted the 10.3-day figure; treat that
as an artifact of the counter, not a finding.

### The merge and the index build are the same operation

    126,916,949 ordinals / 30,985 batches = 4,096.08 ordinals per batch
    "Starting a compaction index build"   04:54:03
    merge first progress line             04:59:08
    pretouch, 126,916,949 ord / 32 srcs   04:38:28

One operation, three counters. Worth stating because they appear as separate
rows in `compactionstats`.

## Caveat on the batch-rate comparisons used throughout this watch

Batch counts are **not comparable across merges** unless ordinals-per-batch is
constant, and it is not. This merge carries 4,096 ordinals/batch; the four
earlier ~31k-batch merges (20:44–21:05) completed in 2–8 minutes, which at
4,096 ordinals/batch would require ~271k cells/s against a healthy ceiling of
14,318 — impossible. So those merges had far smaller batches.

This does not undermine the collapse finding: cycle 1 and cycle 3 collapsed on
merges of the *same* 30,985-batch size with the same device signature, and
that comparison is like-for-like. It does mean the "5,387–27,530 b/min healthy"
reference band should not be applied to arbitrary merges, and the **next run
should track `Flushed segment ... cells ... in N ms` instead** — same units
across every merge, and directly interpretable as settle work.

## What actually decides settle time

The 30,985-batch merge is ~580 GiB of index build (32 standard segments' worth).
At the healthy 67 MiB/s ceiling that is 2.5 h of unavoidable work; the collapse
stretches it to ~7 h. So the collapse costs roughly **2.8x on this operation**,
not the 100x the batch-rate band suggests — the batch-rate band is comparing
across merge geometries.

**The real risk to settling is not the slowdown, it is recurrence.** The table
is 1,051 GB across 19 SSTables and still growing. Every future STCS major merge
at this size will hit the same wall, and each one is a ~7-hour serial block
during which four-plus compactions park at 0.00% and write throughput goes to
~45/s. That is the mechanism behind cycle 1's non-draining settle.

## Baseline for the frontierPrefetch sweep

The sweep should be judged on these, all measurable within one merge:

| metric | cycle 3 value to beat |
|---|---|
| `Flushed segment` rate, standard 3.966M-cell segment | 14,318 cells/s, 67 MiB/s (median 277 s) |
| same, during a large merge | 4,792–6,229 cells/s (762–828 s) at the 03:30–04:00 trough |
| 30,985-batch merge rate | 60–74 b/min, ~7 h implied |
| iowait during that merge | 40–45% sustained |
| md0 mean request size | 6.3–6.5 KB |
| ingest during the collapse | 12 GB/30 min, down 4x from 48–49 |
| time to first collapse | 9h 31m from empty, at ~1,000 GB |

A sweep arm succeeds if the 30,985-class merge stays out of the 60–75 b/min
regime and the device keeps mean request size well above ~6.5 KB.
