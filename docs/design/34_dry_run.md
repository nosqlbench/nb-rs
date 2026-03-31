# SRD 34 — Dry Run Mode

## Purpose

`--dry-run` assembles operations (resolves all bind points, evaluates
the GK kernel) but does NOT execute them through the adapter. This
validates the complete workload pipeline — parsing, binding, GK
compilation, op assembly — without touching any external system.

## Modes

| Flag | Behavior |
|------|----------|
| `--dry-run` | Assemble ops silently. Report count and any errors. |
| `--dry-run=emit` | Assemble ops and print each to stdout (like stdout adapter). |
| `--dry-run=json` | Assemble ops and print as JSON (one per line). |

## What Gets Exercised

- YAML parsing and normalization
- Binding compilation (GK kernel assembly)
- GK evaluation for each cycle (all node functions execute)
- Bind point resolution (including qualified bind points)
- Op assembly (template field substitution)
- Op sequencing (stanza pattern, ratios)

## What Gets Skipped

- Adapter execution (no HTTP requests, no CQL queries)
- Rate limiting (no delays)
- Error handling (no retries)
- Metrics recording (no timers)
- Capture extraction (no results to capture)

## Implementation

Dry-run is the outermost decorator in the op pipeline (SRD 33).
It short-circuits in `before_execute()` — the adapter's `execute()`
method is never called.

```
--dry-run:      before_execute returns Ok(OpResult { success: true, body: None })
--dry-run=emit: before_execute prints the op, returns Ok(...)
--dry-run=json: before_execute prints the op as JSON, returns Ok(...)
```

## Usage

```
nbrs run workload=example.yaml cycles=10 --dry-run
nbrs run workload=example.yaml cycles=5 --dry-run=emit
nbrs run workload=example.yaml cycles=5 --dry-run=json
```

Dry-run with `emit` is particularly useful for verifying that
bind points resolve correctly before running against a real system.
