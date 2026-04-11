# SRD 43: Logging System and Conventions

## Overview

nb-rs uses structured stderr logging for operational diagnostics.
This document defines the logging conventions, severity levels,
output format, and the rules for what gets logged where.

## Current State

nb-rs currently uses `eprintln!` for all diagnostic output. There
is no structured logging framework (no `log` crate, no `tracing`).
This is intentional for now — the output is simple and predictable.

## Logging Channels

| Channel | Target | Content |
|---------|--------|---------|
| **stderr** | Human operator | Diagnostic messages, warnings, errors, progress |
| **stdout** | Machine/pipeline | Op output (stdout adapter), data, results |
| **metrics** | Reporters | Structured metrics via MetricsFrame (CSV, SQLite, OpenMetrics, TUI) |

stdout and stderr must never be mixed. Op output goes to stdout
(or a file). Diagnostics go to stderr. Metrics go to reporters.

## Severity Levels

| Level | Prefix | When to use |
|-------|--------|-------------|
| **error** | `error:` | Unrecoverable: activity cannot proceed. Followed by `process::exit(1)`. |
| **warning** | `warning:` | Recoverable but unexpected: fallback used, config silently adjusted, I/O failure on non-critical path. |
| **info** | (none) | Normal operational progress: activity start, completion, op count, cycle count. |
| **debug** | (not yet) | Verbose diagnostic detail. Currently not emitted. Reserved for future `--verbose` flag. |

## Message Format

```
{prefix}: {message}
```

- `error: CQL connection failed: Connection refused`
- `warning: invalid error spec 'bogus': parse error; using default`
- `warning: unrecognized parameter 'cyclse' — check for typos`
- `nbrs: 3 ops selected, 1000 cycles, 4 threads, adapter=cql`
- `nbrs: done.`

Info-level messages use the binary name as prefix (`nbrs:`,
`cassnbrs:`, `opennbrs:`). Warnings and errors use `warning:` and
`error:` respectively — no binary-name prefix on these, so they
stand out visually.

## Rules

1. **All errors to stderr.** Never to stdout. Never swallowed.
2. **No silent failures.** Every fallible operation that silently
   succeeds must log a warning if it fails. See SRD 41.
3. **No spam in steady state.** Per-cycle logging is forbidden.
   Diagnostics are init-time or error-time only. The hot path
   (cycle execution) produces zero log output on success.
4. **Warnings are actionable.** Every warning message tells the user
   what happened, what the fallback is, and (if applicable) how to
   fix it.
5. **Progress messages are concise.** One line for startup, one line
   for completion. No per-stanza or per-percent progress spam.
6. **Metrics reporters handle their own output.** Console reporter,
   CSV reporter, TUI — these have dedicated channels and cadences.
   They don't use `eprintln!`.
7. **Adapter errors flow through the ErrorRouter.** The error router
   decides whether to warn, count, or stop. Adapters do not
   `eprintln!` errors directly — they return `ExecutionError`.

## CLI Option Registry (TODO)

The current CLI has a design deficiency: flags, help text, and shell
completions are maintained as independent string literals in separate
functions. Adding a flag requires updating the parser, the error
message, the help text, and three shell completion generators. This
violates the explicit-error-handling principle — omitting a completion
is a silent failure in discoverability.

The fix: a declarative CLI option registry. Each option is declared
once with its name, aliases, type, description, and applicable
subcommands. The parser, help text, completions, and unrecognized-
parameter validation are all generated from this single source.

This is deferred to a future implementation but is a known gap.

## Future: Structured Logging

When nb-rs needs structured logging (for machine-parseable output,
log aggregation, or debug-level verbosity), the plan is:

- Adopt the `tracing` crate with `tracing-subscriber`
- Levels: ERROR, WARN, INFO, DEBUG, TRACE
- Default filter: INFO and above
- `--verbose` flag: DEBUG
- `--trace` flag: TRACE
- Structured fields: `cycle`, `op`, `adapter`, `error_name`
- JSON output option for log aggregation

This is deferred until the complexity warrants it. For now,
`eprintln!` with consistent prefixes is sufficient.
