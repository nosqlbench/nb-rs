# SRD-45 — Sessions

**Status:** normative
**Owner:** runtime / runner
**Implementation:** `nbrs-activity/src/session.rs`
**Cross-refs:** SRD-04 (umbrella options), SRD-40 (metrics),
  SRD-44 (checkpointing)

---

## What a session is

A *session* is the root context for one `nbrs run`. It owns:

- A **session name** — short identifier appearing on every
  metric label as `session=<name>`.
- A **session directory** — the on-disk home for every
  artifact the run produces (`metrics.db`, `session.log`,
  `checkpoint.json`, `summary.md`, `flamegraph*.svg`,
  `tui.dump`, …).
- A **component tree root** for metrics labeling and dynamic
  controls (SRD-19, SRD-23).

Every read-side command (plot, report, summary, TUI replay)
targets a session by referencing its directory or following
the `logs/latest` symlink that points at it.

---

## CLI surface

Sessions follow the umbrella-options pattern (SRD-04). One
short flag for everyday use, full long-form flags for
unambiguous expression. Each flag has an `NBRS_`-prefixed env
equivalent (SRD-04 Rule 7); setting both CLI and env for the
same flag is a hard error.

### Umbrella

```
--session <kv-list>
NBRS_SESSION=<kv-list>
```

`<kv-list>` is comma-separated. Each item is a `key:value`
pair or a bare-token shortcut.

| Item                | Meaning                                    |
| ------------------- | ------------------------------------------ |
| `name:<id>`         | Session name (id).                         |
| `path:<dir>` / `dir:<dir>` | Full session directory path.        |
| `reuse:<mode>`      | Reuse policy. `error` / `restart` / `resume`.|
| `keep:<N>`          | Retention cap by count.                    |
| `shelflife:<dur>`   | Retention cap by age (`s|m|h|d|w` suffix). |
| `restart`           | Bare. Equivalent to `reuse:restart`.       |
| `resume`            | Bare. Equivalent to `reuse:resume`.        |
| `error`             | Bare. Equivalent to `reuse:error`.         |

Last-wins on repeated keys. Unknown keys log a `Warn` and are
skipped (the rest of the spec still applies).

```
--session 'restart,name:baseline'
--session 'keep:42,name:experiment,path:/data/runs/SESSION,reuse:resume'
NBRS_SESSION='restart,path:/tmp/scratch'
```

### Long-form per-key flags

| Flag                          | Env equivalent              |
| ----------------------------- | --------------------------- |
| `--session-name <name>`       | `NBRS_SESSION_NAME`         |
| `--session-path <path>`       | `NBRS_SESSION_PATH`         |
| `--session-reuse <mode>`      | `NBRS_SESSION_REUSE`        |
| `--session-keep <N>`          | `NBRS_SESSION_KEEP`         |
| `--session-shelflife <dur>`   | `NBRS_SESSION_SHELFLIFE`    |

Long-form flag wins over umbrella value when both are set.

### Legacy env

`SESSION_DIRECTORY` is the pre-SRD-04 name for
`NBRS_SESSION_PATH`. Honored as a deprecated fallback with a
one-line `Warn` on read; remove from your shell config and
use `NBRS_SESSION_PATH` instead.

---

## Path / name resolution

Computed at `Session::new_with_args` time:

1. `path` is set → use it. The `SESSION` token inside the
   path is replaced with the resolved session name (see
   step 2). Basename of the resolved path becomes the
   session name when no explicit `name` was given.
2. `path` is unset, `name` is set → path = `logs/<name>/`.
3. Neither is set → name = `<scenario>_<timestamp>`,
   path = `logs/<name>/`.

The `SESSION` token is the umbrella's only template variable.
It's the most-specific name-related value the umbrella
knows about (per SRD-04 Rule 4).

---

## `logs/latest` symlink

Always maintained. Points at the active session.

- For paths under `logs/`, the symlink target is relative
  (`logs/latest -> <name>`) so it survives directory moves.
- For paths outside `logs/`, the target is absolute so it
  resolves regardless of cwd.

---

## Process-startup wiring

`apply_session_directory_at_startup(args)` runs first in
`nbrs::main()`. It resolves the spec, applies lifecycle
cleanup, and (when the path is fully concrete at startup)
updates `logs/latest` so subsequent subcommands —
`plot`, `report`, `summary`, `tui` — target the configured
session automatically.

If the path needs the auto-id (e.g. `--session-path`
contains a `SESSION` token and no `--session-name` is given),
the symlink is updated later when `Session::new` resolves the
token. The startup hook is a no-op for that case.

```
$ export NBRS_SESSION_PATH=/data/runs/today
$ nbrs run workload=foo.yaml
$ nbrs plot     # reads /data/runs/today/metrics.db
$ nbrs report   # ditto
$ nbrs run workload=foo.yaml --session=resume   # resumes today
```

---

## Session-reuse policy

When a fresh `nbrs run` resolves to a directory that already
contains prior session artifacts (`metrics.db`,
`session.log`, or `checkpoint.json`), the `reuse` policy
applies:

| Policy | Behavior |
| --- | --- |
| `error` (default) | Refuse to start. Exit code 2 with a message naming the existing path and the available reuse modes. |
| `restart` | Delete prior artifacts, start fresh. Directory itself stays. |
| `resume` | Refuse to start unless `--resume` (or `--resume-latest`) is also specified. The policy reminds the operator that resume continues the prior session rather than overwriting. |

The default `error` ensures accidental reuse can never
silently destroy a prior session. Any other policy is
opt-in.

`--resume <name>` is independent: when specified, the resume
path takes effect via `Session::resume` and the reuse policy
doesn't apply.

---

## Session-lifecycle cleanup

At binary startup, `purge_stale_sessions` runs against the
parent directory (the resolved path's parent, or `logs/` by
default). Two policies, both checked on every startup:

| Key | Default | Purges |
| --- | --- | --- |
| `keep` (`--session-keep`) | `10` | Anything past the N most recent (by mtime). `0` disables. |
| `shelflife` (`--session-shelflife`) | `4w` | Anything older than `now - duration`. `0` disables. |

Duration syntax: `<n>s|m|h|d|w` (seconds / minutes / hours /
days / weeks). Bare integer = seconds.

Skipped from purge:

- The `logs/latest` symlink and its target (the active
  session).
- Symlinks in general — only real directories are removed.
- Loose files at the parent level — only directory entries
  are candidates.

Failures (read-dir, remove-dir) log Warn and continue;
housekeeping doesn't abort startup.

---

## Keep-cap forecast at exit

At the end of every `nbrs run`, before the binary exits, the
runtime checks whether the **next** new session would auto-
purge sessions due to the `--session-keep` cap. If yes, it
logs at INFO level:

```
the next new nbrs session will auto-purge N prior session
director{y|ies} under <parent> due to --session-keep=<cap>.
To disable: --session-keep=0 (or NBRS_SESSION_KEEP=0).
To raise the cap: --session-keep=<bigger>.
```

The forecast lets operators see imminent cleanup before it
happens, with concrete instructions for opting out. Logged
once per run, at the very end (Drop guard alongside the
resume hint). Implementation: `forecast_keep_purge`.

`keep:0` (or any path with no parent under `keep` enforcement)
suppresses the message — the cap is disabled, so there's
nothing to forecast.

---

## Resume hint on workload exit

When a workload run finishes (success OR error path) and the
checkpoint state shows phases declared `checkpoint:
idempotent` that aren't `Completed`, the runtime prints a
multi-line hint to stderr naming the session and showing the
resume command. Implementation:
`CheckpointWriter::resume_hint()` + Drop guard in the
runner.

The hint never fires when every skip-eligible phase reached
`Completed` or when the workload declared no idempotent
phases at all.

---

## Invariants

- A session name is a stable identifier for the lifetime of
  the session. `Session::resume` preserves the prior name;
  the new invocation does **not** allocate a fresh one.
- `logs/latest` always points at exactly one session at a
  time (the most recently started, or the env-overridden one
  at startup).
- Session directories are atomic units: deleting one cleans
  up that session entirely. State that escapes the session
  dir is the testkit fixture's job (SRD-44, design memo
  `resumable_test_fixture.md`).
- Reuse policy must be explicit. There is no
  guess-what-the-operator-wanted path — `error` default
  makes accidental destruction impossible.
- CLI flag and `NBRS_<FLAG>` env both set = hard error
  (SRD-04 Rule 7). No silent disambiguation.

---

## See also

- SRD-04 — Umbrella options (the pattern this surface
  pioneers).
- SRD-40 — metrics labeling (session label is the root)
- SRD-44 — workload checkpointing (resume / skip / verify)
- `docs/guide/sessions.md` — operator-facing tutorial
