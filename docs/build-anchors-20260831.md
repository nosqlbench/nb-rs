# Build anchors: returning to Run F, and the pre-SPLAT control

**Written 2026-08-31.** Two questions: how to get back to exactly what Run F ran, and whether a SPLAT-free
"last known good" state already exists in git. Both answered from the live server (pid 290993, never
restarted during Run F) and the two server repos. Diagnosis only — no commits were made in those trees.

## 1. The Run F anchor (the state that produced the 200M run)

| | repo | branch | commit | deployed jar |
|---|---|---|---|---|
| jvector | `/mnt/nvme/opt/jvector` | `experiment/cluster-rescore-prefetch-20260824` | **`de79d5bf`** (2026-08-27 23:53) | md5 **db987fd0**, 694,695 B |
| Cassandra | `/mnt/nvme/opt/cassandra` | `experiment/effective-remappers-readback-20260825` | **`ea38d33954`** (2026-08-28 00:33) | dse-db md5 **efadc76c**, 11,330,829 B |

Flags in force: `frontierPrefetch=16`, `similarityOrdinals=true`, `clusterSearch=false`, pretouch `-1/1048576`.
Both repo tips are clean (jvector 0 dirty; Cassandra dirty only in untracked helper scripts/conf backups).

**Artifacts archived** to `/mnt/nvme/opt/rc-tools/build-anchors/runF-20260828-splat-de79d5bf/`: both jars,
`jvm-server.options`, the live process's `-D` flags, and `ANCHOR.md`. This matters because a Maven rebuild at
another commit overwrites `jvector/**/target/*.jar`, and deploying it overwrites `/opt/cassandra/lib` — the
only two places db987fd0 existed. Rebuilding `de79d5bf` will not necessarily reproduce the md5 (jar
timestamps), so **the jar is the artifact of record; the commit is the source of record.**

**Fragility to fix on the server side (not this session's tree):** `conf/jvm-server.options.pre-cluster-rescore`,
`conf/jvm-server.options.pre-frontier32`, `bin/new_cass`, `bin/cass-restarts.sh`, `bin/run_foreground` are
**untracked** in the Cassandra repo — a wipe loses them. They are copied into the anchor dir only insofar as
`jvm-server.options` itself was; the rest should be committed or archived by their owning session.

## 2. The pre-SPLAT control — it already exists, on the same branch

SPLAT entered in one burst on the evening of 2026-08-27. The first commit carrying the algorithm is
**`29f24feb`** (20:56, "node token stream"); everything through `de79d5bf` builds on it. The arm is *not*
part of it — `frontierPrefetch` was introduced 2026-08-19 (`985bfe1e`) and made reachable 2026-08-23
(`11cb4acf`), so every candidate below already has it.

| candidate | date | what it is | arm | stage logging | SPLAT |
|---|---|---|---|---|---|
| **`7de94e83`** | 08-26 01:04 | **exactly what Run E ran** (deployed as md5 `33f1202c`) | yes | no | no |
| `9b7a0fff` | 08-27 19:58 | + step-0 IO knobs (own-record hints, sync barrier, prefetch lead) | yes | no | no |
| **`30cdfdaf`** | 08-27 19:58 | + "instrument every stage for logging and progress" — last commit before SPLAT | yes | **yes** | no |
| `29f24feb` … `de79d5bf` | 08-27 20:56→23:53 | the SPLAT burst: 8 commits, 27 files, +4,171/−67 | yes | yes | yes |

**Recommendation depends on what "known good" has to mean:**
- **`7de94e83` — proven.** It is literally the build Run E ran for 16.5 h, so it carries measured references
  (giant stream-1 4.6–5.0 min/M at WIDTH=3, the starved-pass data). Choose this if the test must stand on a
  state we have already exercised. Cost: no `Stage X completed … in N ms` lines, so per-stage anatomy — the
  instrument every Run F conclusion (E13, E15–E18) leans on — is unavailable, and comparisons fall back to
  whole walls only.
- **`30cdfdaf` — the clean control.** Isolates precisely the SPLAT delta while keeping the stage instrument,
  so a wall measured on it is directly comparable to Run F's per-phase breakdown. Cost: never been run.

Either way the arm stays on, so the result answers the question E15 left open: Run E→F prices arm+SPLAT
jointly, and a `30cdfdaf` (or `7de94e83`) run at `frontierPrefetch=16` separates them.

## 3. Compatibility with the current Cassandra tree

The Cassandra side touches jvector through four entry points only: `new OnDiskGraphIndexCompactor`,
`setProgressLimiter`, `effectiveRemappers`, `compact`. All four are present at `7de94e83`, `9b7a0fff`, and
`30cdfdaf` (`effectiveRemappers` and `setProgressLimiter` each in 2 files at every candidate). So
`ea38d33954` should build and link against a pre-SPLAT jvector with no source change — the isEmpty fix and
the cold-start PQ codebook change live on the Cassandra side and travel with it. **Verify by compiling**
before trusting it: a narrow surface makes signature drift unlikely, not impossible.
