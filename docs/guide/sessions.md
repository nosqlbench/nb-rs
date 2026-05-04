# Sessions — operator's guide

Every `nbrs run` is a *session*. The session owns a directory
on disk where every artifact from the run lands —
`metrics.db`, `session.log`, `checkpoint.json`, `summary.md`,
flame graphs, TUI dumps. Other commands (`nbrs plot`, `nbrs
report`, `nbrs summary`, `nbrs tui`) read from a session.

This guide walks through the everyday patterns. The
authoritative spec is **SRD-45**.

---

## The simplest case

Out of the box, `nbrs run workload=foo.yaml` writes to
`logs/<scenario>_<timestamp>/`. The shell-friendly
`logs/latest` symlink always points at the most recent
session, so `nbrs plot` / `nbrs report` work without
arguments:

```
$ nbrs run workload=foo.yaml
$ nbrs plot      # reads logs/latest/metrics.db
$ nbrs report    # ditto
```

That's it. Most users never need anything else.

---

## Pinning a session name

Sometimes you want a memorable name instead of the
auto-generated timestamp:

```
$ nbrs run workload=foo.yaml --session=baseline
# writes to logs/baseline/

$ nbrs run workload=foo.yaml --session=tuning1
# writes to logs/tuning1/
```

Then `nbrs plot --db logs/baseline/metrics.db` (or
`SESSION_DIRECTORY=logs/baseline nbrs plot`) targets that
specific session even after you've started newer ones.

---

## Pinning a session directory

For longer runs / shared filesystems, point at an explicit
directory:

```
$ nbrs run workload=foo.yaml --session-dir=/data/runs/2026-q1-baseline
```

The basename (`2026-q1-baseline`) becomes the session id.
`logs/latest` is updated to point absolute-path at the new
location, so `nbrs plot` still finds it.

### Templating with the `SESSION` token

If you want one shell-level pin that produces distinct
per-run directories, put the literal token `SESSION` in the
path:

```
$ export SESSION_DIRECTORY=/data/runs/SESSION_dir
$ nbrs run workload=foo.yaml
# writes to /data/runs/default_20260101_120000_dir/

$ nbrs run workload=foo.yaml
# writes to /data/runs/default_20260101_120100_dir/
```

The token is replaced with the auto-generated session name at
write time. `--session-dir` and `SESSION_DIRECTORY` are
equivalent; the env var is just shorthand so a shell can
share it with sibling subcommands.

---

## SESSION_DIRECTORY in a subshell

Set the env var once, every subcommand in that subshell sees
the same session directory:

```
$ export SESSION_DIRECTORY=/data/runs/today
$ nbrs run workload=foo.yaml
$ nbrs plot                          # reads /data/runs/today/metrics.db
$ nbrs report                        # ditto
$ nbrs run workload=foo.yaml --resume   # resumes /data/runs/today
```

This is the recommended pattern when iterating on a workload —
no need to copy paths around between commands.

---

## Reusing an existing session directory

If a session directory **already contains artifacts** (a prior
run's `metrics.db`, `session.log`, or `checkpoint.json`),
`nbrs run` refuses to start by default — it won't silently
destroy the prior session. You have three options, picked via
`--session-reuse`:

| Mode | What happens |
| --- | --- |
| `error` (default) | Run aborts with exit code 2 and a message naming the existing path. |
| `restart` | Prior artifacts deleted, fresh run starts in the same dir. |
| `resume` | Refuses to start unless you also pass `--resume`. Reminds you that resume continues the prior session rather than overwriting. |

Examples:

```
# Wipe and restart in the same dir
$ nbrs run workload=foo.yaml --session=baseline --session-reuse=restart

# Continue the prior session (idempotent phases skip; failed ones rerun)
$ nbrs run workload=foo.yaml --session=baseline --resume
```

If you really do want destructive overwrite without thinking,
make it explicit. The default protects against typos.

---

## Resume after failure

When a workload's idempotent phases fail or stop short, the
runtime tells you on exit:

```
This session has resumable phases that didn't complete.
  To continue from where it stopped:
    nbrs run <workload> --session-dir /data/runs/today --resume
  To pin the session name for repeatable resumes:
    nbrs run <workload> --session today (then add --resume next time)
```

The hint fires automatically — no action needed beyond reading
the message. See SRD-44 for the resume model itself.

---

## Automatic cleanup

By default, the runtime keeps the **10 most recent** sessions
under each session-parent directory. Older sessions are
purged on the next `nbrs` startup. Tunable:

```
$ nbrs run workload=foo.yaml --sessions-max=5     # keep just 5
$ nbrs run workload=foo.yaml --sessions-max=0     # never purge by count
```

Sessions older than **4 weeks** are also purged regardless of
the count cap. Tunable:

```
$ nbrs run workload=foo.yaml --sessions-shelflife=2w  # keep 2 weeks
$ nbrs run workload=foo.yaml --sessions-shelflife=0   # never purge by age
```

Duration syntax: `<n>s|m|h|d|w` (seconds / minutes / hours /
days / weeks). Bare integers are seconds.

Env-var equivalents: `SESSIONS_MAX`, `SESSIONS_SHELFLIFE`.

The active session (`logs/latest` and its target) is never
purged regardless of policy. Symlinks at the parent level
are skipped.

---

## CLI quick reference

| Flag | Env | Default | Use it when… |
| --- | --- | --- | --- |
| `--session <name>` | — | auto | You want a memorable session id. |
| `--logs-dir <path>` | — | `logs` | All sessions should go under a custom parent. |
| `--session-dir <path>` | `SESSION_DIRECTORY` | unset | You want full control over the path; use `SESSION` token for per-run templating. |
| `--session-reuse <mode>` | — | `error` | A session dir already exists and you've decided what to do with it (`error` / `restart` / `resume`). |
| `--sessions-max <N>` | `SESSIONS_MAX` | `10` | Custom retention count (`0` disables). |
| `--sessions-shelflife <dur>` | `SESSIONS_SHELFLIFE` | `4w` | Custom retention age (`0` disables). |
| `--resume[ <id>]` | — | unset | Continue a prior session from where it stopped. |
| `--resume-latest` | — | unset | Continue from `logs/latest`. |

The env vars are shorthand for the equivalent flag — useful in
shell sessions that want consistent wiring across multiple
`nbrs` invocations.

---

## See also

- SRD-45 — Sessions (authoritative)
- SRD-44 — Workload checkpointing (resume / skip / verify)
- `docs/guide/gk_purity.md` — when to mark wires `volatile`
  to keep their values out of resume identity
