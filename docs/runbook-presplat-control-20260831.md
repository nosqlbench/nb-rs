# Runbook: the pre-SPLAT control run (jvector `30cdfdaf`)

**Goal.** Measure the same workload on the last commit before the SPLAT algorithm, with the arm and every
other flag held identical to Run F, so the Run E→F delta stops conflating arm+SPLAT (the question E15 left
open). Nothing here has been executed; the server trees are untouched.

## Why 30cdfdaf is a true control (verified, not assumed)

- **The SPLAT delta is exactly two stages.** `CompactionStage` at `30cdfdaf`: SOURCE_PRETOUCH,
  SIMILARITY_ORDINALS, PQ_RETRAIN, CODE_PRE_ENCODE, BASE_LAYER, UPPER_LAYERS, FINALIZE, REFINE, SIDECAR.
  At `de79d5bf` the same list **plus DISTRIBUTE and TOKEN_STREAM**. Nothing else differs in the enum.
- **The stage instrument is byte-identical** at both commits (`StageInstrumentation`):
  `Stage X started` / `Stage X: N units` / `Stage X progress: c/t (p%)` / `Stage X completed: … ms`.
  So per-stage anatomy is directly comparable to Run F's — including BASE_LAYER, the phase that dominates.
- **The wall instrument is unaffected by the swap.** `adopted effective ordinal mapping` and
  `TERMS_DATA written in place` are emitted by the **Cassandra** tree, which does not change;
  `Similarity ordinals assigned` comes from jvector and exists at `30cdfdaf`. The
  pass→adopt→TERMS_DATA rule works unchanged.
- **All Run F flags exist at `30cdfdaf`**: frontierPrefetch, similarityOrdinals, clusterSearch,
  sourcePretouchMaxNodes, sourcePretouchWindowNodes, jvector.mode, enable_native_vectorization.
  (An unrecognized `-D` would silently no-op and quietly change the experiment.)
- **API compatibility**: Cassandra calls four entry points — `new OnDiskGraphIndexCompactor`,
  `setProgressLimiter`, `effectiveRemappers`, `compact` — with identical signatures at both commits.

## Deploy topology (established, not guessed)

- `/opt/cassandra` and `/mnt/nvme/opt/cassandra` are **the same tree** (same inode). A build deploys in
  place; there is no copy step, which is why `build-cassandra.sh` refuses to run while the unit is active.
- jvector reaches the node as: `mvn install` → `~/.m2/repository/io/github/jbellis/jvector/4.0.1-SNAPSHOT/`
  → `lib/jvector-4.0.1-SNAPSHOT.jar` on the classpath. All three copies are md5 `db987fd0` today.
- **Gap:** `build-cassandra.sh` tells you to install jvector via `$JVECTOR_REPO/bin/build-jvector.sh
  --verify --deploy`, but `/mnt/nvme/opt/jvector/bin` **does not exist**. Use `mvn install` (below).

## The wipe is the protocol, not a loss

The measurement *is* the 200M load: the tier ladder (4M → 16M → 63.6M) forms as the dataset is built, so the
control run must start from empty exactly as Run F did. `new_cass` hard-deletes `/mnt/nvme/cassandra` except
`heapdumps` — that is the intended starting condition, and Run F's sstables are a byproduct with no role in
the comparison. Nothing to preserve there.

**What did need preserving is the log evidence, and it is now archived.** logback keeps `maxHistory=7` days /
`totalSizeCap=5GB`, so Run F's 08-28/08-29 compaction logs — the source for every wall in E13 and E16–E18,
all three giants included — expire around 09-04 and would additionally compete with the control run's own
output. All 523 files (394 MB) are copied verbatim to
`/mnt/nvme/opt/rc-tools/build-anchors/runF-20260828-splat-de79d5bf/logs/`. **Leave live logrotate settings
alone**; the control run needs the same retention that made Run F reconstructable.

## When each comparison point arrives (from Run F's own clock, t = run start)

| t+ | event on Run F | what the control gives you |
|---|---|---|
| ~0–2 h | 4M solo walls, first 16M lands t+2h01m at 4.48 | early SPLAT-vs-control at cache-resident scale; also gate G6 |
| ~8 h 23 m | giant #1's ordinal pass begins | first giant-class stream timings |
| **~19 h 06 m** | **giant #1 lands — 642.7 min, 10.11 min/M** | **the decisive number** |
| ~38 h | giant #2 lands (10.21) | repeatability of the control, as E17 did for SPLAT |
| ~60 h | 200M complete, clean shutdown | full-run parity: 2 d 12 h end to end |

So the headline answer arrives roughly **19 hours in**, not at the end — but the run should go the full
distance for the repeat giant and the ingest-rate comparison.

## Phase 1 — swap jvector (node down)

    sudo systemctl stop cassandra                       # build guard requires this
    cd /mnt/nvme/opt/jvector
    git switch -c experiment/pre-splat-control-30cdfdaf 30cdfdaf   # name it; don't sit on detached HEAD
    mvn -q -DskipTests install                          # refreshes target/ AND ~/.m2
    md5sum jvector-multirelease/target/jvector-4.0.1-SNAPSHOT.jar \
           ~/.m2/repository/io/github/jbellis/jvector/4.0.1-SNAPSHOT/jvector-4.0.1-SNAPSHOT.jar
    # the two must match each other and must NOT be db987fd0
    cp ~/.m2/repository/io/github/jbellis/jvector/4.0.1-SNAPSHOT/jvector-4.0.1-SNAPSHOT.jar \
       /mnt/nvme/opt/cassandra/lib/jvector-4.0.1-SNAPSHOT.jar

**No Cassandra rebuild is required** — its source is unchanged and the four call signatures are identical,
so the existing `dse-db` jar (md5 `efadc76c`) links against the older jvector. Refreshing `~/.m2` matters
anyway: it stops a later `ant jar` from silently resurrecting `db987fd0`. If you do choose to rebuild, use
`bin/build-cassandra.sh` (enforces JDK 25 / class version 69 and the artifact-consistency checks).

## Phase 2 — reset and start

    /mnt/nvme/opt/cassandra/bin/new_cass                # wipe + start + wait for 9042

Then capture the new anchor immediately (pid changes; flags must be byte-identical to Run F):

    P=$(pgrep -f 'java.*CassandraDaemon' | head -1)
    tr '\0' '\n' < /proc/$P/cmdline | grep -E '^-Djvector'
    md5sum /opt/cassandra/lib/jvector-4.0.1-SNAPSHOT.jar

Expected flags, unchanged from Run F: `frontierPrefetch=16`, `similarityOrdinals=true`,
`clusterSearch=false`, `sourcePretouchMaxNodes=-1`, `sourcePretouchWindowNodes=1048576`, `mode=production`,
`enable_native_vectorization=true`. Archive under
`/mnt/nvme/opt/rc-tools/build-anchors/runG-<date>-presplat-30cdfdaf/`.

## Phase 3 — validation gates (pass all before trusting a number)

| # | gate | check | pass condition |
|---|---|---|---|
| G1 | SPLAT is really gone | `grep -cE 'Stage DISTRIBUTE\|Stage TOKEN_STREAM\|Bands for source' compaction.log` | **0**, permanently |
| G2 | wall instrument intact | first merge emits `Similarity ordinals assigned`, `adopted effective ordinal mapping`, `TERMS_DATA written in place` | all three present |
| G3 | stage instrument intact | `Stage X completed: N/N units in M ms` for SOURCE_PRETOUCH / SIMILARITY_ORDINALS / CODE_PRE_ENCODE / BASE_LAYER / UPPER_LAYERS / FINALIZE | present, same format as Run F |
| G4 | arm is live | flags show `frontierPrefetch=16`; during a storm, md0 > 190k r/s at ~0.2 ms r_await | above the 190k @ 0.22 ms sync ceiling |
| G5 | hygiene parity | `Cluster path cost` = 0; VectorIndexIntegrity/assertions = 0; cgroup `memory.events` max = 0; breaker wires non-NULL; 0 lint warns | all clean, as Run F |
| G6 | build sanity | first **solo** 4M wall | 1.0–1.6 min/M (Run F 1.01–1.16). A solo 4M ≫3 min/M means the build is wrong, not that SPLAT mattered |

**Do not reuse Run F's batch-clock model.** E15's per-source-cycle denominator came from banded DISTRIBUTE,
which does not exist here. Re-derive the batch semantics from the first 4M before using the batch counter
for any in-flight estimate; walls and stage-elapsed lines are unaffected either way.

## Phase 4 — what makes the comparison publishable

Run the identical workload (same scenario shape, 200M target, same `nmbrs` all-engines client, same breaker
config) and compare against Run F's completed record:

| class | Run F reference | what the control answers |
|---|---|---|
| 4M solo | 1.01–1.16 min/M | does SPLAT cost anything at cache-resident scale? |
| 16M solo | 4.48 / 4.34 / 4.50, and 5.67 late-run | SPLAT's effect where the arm is invisible |
| **63.6M giant** | **10.11 / 10.21** (68.4M: 10.97) | **the headline** — SPLAT's whole contribution, same arm, same class |
| BASE_LAYER (16M) | 156.9 min (#6) | stage-resolved, the cleanest apples-to-apples |
| starvation | 6.28–7.40 armed; 92.23 record | does SPLAT change the monopoly's externality (E14/E18)? |

Watch cadence: same 30-minute cron and monitor as Run F. The Run F blackout showed the record is
reconstructable from rotated logs (337 zips retained), so **leave logrotate retention alone** — it is what
saved the giants.

## Rollback to Run F state

No rebuild needed; the artifacts are archived at
`/mnt/nvme/opt/rc-tools/build-anchors/runF-20260828-splat-de79d5bf/`:

    sudo systemctl stop cassandra
    cp <anchor>/jvector-4.0.1-SNAPSHOT.jar /mnt/nvme/opt/cassandra/lib/
    cp <anchor>/jvector-4.0.1-SNAPSHOT.jar ~/.m2/repository/io/github/jbellis/jvector/4.0.1-SNAPSHOT/
    cp <anchor>/dse-db-4.0.11.0-SNAPSHOT.jar /mnt/nvme/opt/cassandra/build/
    git -C /mnt/nvme/opt/jvector switch experiment/cluster-rescore-prefetch-20260824   # de79d5bf
    sudo systemctl start cassandra    # verify md5 db987fd0 / efadc76c on the classpath

## Open risks

1. `bin/build-jvector.sh` referenced by `build-cassandra.sh` does not exist — the documented install path
   is broken; `mvn install` is the working equivalent.
2. `bin/new_cass`, `bin/cass-restarts.sh`, `bin/run_foreground`, `conf/jvm-server.options.pre-*` are
   **untracked** in the Cassandra repo. A wipe or a bad checkout loses them. They belong to the server
   session; worth committing there.
3. `30cdfdaf` has never been run. It is the clean control, not a proven build — G6 exists to catch that.
