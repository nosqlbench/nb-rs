# Report-items early persistence

**Status:** planned, not implemented
**Owner:** nbrs-activity (`runner.rs` end-of-run block),
  nbrs/src/report_cmd.rs (db-fallback consumer)
**Cross-refs:** SRD-46 (reports — persisted-spec contract,
  `report.<name>` keys in `session_metadata`), SRD-44 (session
  layout, sqlite_reporter lifecycle)

---

## The bug

Two issues in one block at `nbrs-activity/src/runner.rs:1958-1992`:

```rust
if !active_summaries.is_empty() {
    if let Ok(mut guard) = sqlite_reporter.lock() {
        if let Some(ref mut reporter) = *guard {
            // Persist every report item (SRD-46) under
            // `report.<name>` keys.
            for item in workload_report.items() {
                ...
                reporter.set_metadata(&format!("report.{}", item.name), &value);
            }
            // ... summary writing follows in the same block
        }
    }
}
```

1. **Wrong gate.** The persistence of report items is conditional on
   `active_summaries` being non-empty. Report items and summaries are
   independent concerns — a workload with `report:` items but no
   `summary:` block (and no `table` items synthesised into
   `workload_summaries` at runner.rs:572-577) silently skips
   report-item persistence. Only the *coincidence* that
   `full_cql_vector.yaml`'s report block has `table` items keeps it
   working today.

2. **End-of-run timing.** The block runs at session finalize. For
   long-running workloads, `nbrs report list` against the in-progress
   session shows "(no report items defined)" — even though the report
   block is fully known at workload load and could have been
   persisted before the first phase ran.

The user-facing symptom: `nbrs report list` on a multi-hour run
returns nothing for the duration of the run, even though the
workload's report block is complete and stable from the moment the
run starts.

---

## The fix

**Move the report-items persistence from the end-of-run block to a
new init-time hook**, executed right after the sqlite reporter is
opened and before the first phase starts. Drop the
`active_summaries` gate — it has no relevance to the
report-item-persistence concern.

The summary-writing logic at the same block stays at end-of-run
(it's correctly placed: summaries report cumulative metric state,
which only exists after phases have run).

### Concrete shape

In `nbrs-activity/src/runner.rs`, near where `sqlite_reporter` is
constructed and the run-wide Details metadata is written
(`reporter.set_metadata("end_time", ...)` at line 1944-1956), add a
sibling block:

```rust
// SRD-46: persist every report item under `report.<name>`
// keys at run start. The report block is workload-load-time
// known; items don't depend on per-phase outcomes. Persisting
// early makes `nbrs report list` work against an in-progress
// session immediately, with no end-of-run dependency.
if let Ok(mut guard) = sqlite_reporter.lock()
    && let Some(ref mut reporter) = *guard
{
    for item in workload_report.items() {
        let mut value = String::new();
        value.push_str(item.kind.as_str());
        value.push(' ');
        value.push_str(&item.name);
        value.push('\n');
        if let Some(label) = item.label.as_deref() {
            value.push_str("label ");
            value.push('"');
            value.push_str(label);
            value.push_str("\"\n");
        }
        if let Some(tf) = item.target_file.as_deref() {
            value.push_str("target ");
            value.push_str(tf);
            value.push('\n');
        }
        value.push_str(&item.body);
        reporter.set_metadata(&format!("report.{}", item.name), &value);
    }
}
```

Remove the corresponding `for item in workload_report.items() { ... }`
loop from the end-of-run block. Leave the summary-writing intact.

### Where the new block lives

The natural attach point is **alongside the existing run-start
metadata writes** for `workload`, `scenario`, `start_time`, and the
per-param `param.<name>` rows (search for
`reporter.set_metadata("workload"` to find the cluster). Putting the
report-items block there keeps all session_metadata writes that are
fixed-at-start in one place; the end-of-run block only writes the
keys that depend on cumulative state (`end_time`, `phase_count`,
`scenario_count`, `adapter`).

### What `nbrs report list` sees

After this fix:

- An in-progress session has `report.*` rows from the moment the
  run starts.
- `nbrs report list` (no `workload=` arg) reads the in-progress
  session db and lists every item correctly.
- A run that crashes / is killed mid-flight still has the items
  persisted (the report block is, structurally, run metadata, not
  run output).

### What changes for `db_merge`

The existing `db_merge` SQL at `nbrs/src/db_merge.rs:288-291`
already handles `session_metadata` correctly — it merges by
`INSERT OR REPLACE` per key, last-input-wins. Report items
persisted at run start round-trip through a multi-db merge the
same way they do post-run today.

---

## Out of scope for this fix

- **Stale items on resume.** When `--session-reuse=resume` reuses
  a session dir whose original run wrote report items under one
  workload version, and the resumed invocation uses a different
  workload, the existing items would be overwritten by the new
  workload's items (key-for-key match → `INSERT OR REPLACE`
  semantics). This is the existing behaviour at end-of-run too;
  this fix doesn't change it.
- **Item drift mid-run.** Workloads aren't reloaded mid-run, so
  `report:` block edits during a run don't matter. If we ever
  add hot-reload, the persistence would need to re-run on each
  workload reload — but that's a hot-reload feature, not this
  fix.
- **The `summary` synthesis from `table` items**
  (runner.rs:572-577). Untouched. The `report:` block's `table
  <name>` items continue to populate `workload_summaries` for
  the end-of-run summary writer; only the report-item
  persistence moves.

---

## Testing

Two tests cover the change:

1. **`report_items_persisted_before_first_phase`** — set up a
   minimal workload with a `report:` block and one phase that
   includes a sentinel sleep / barrier. Trigger the persistence
   path, verify `session_metadata` has the `report.<name>` rows
   *before* the phase completes.
2. **`report_items_persist_without_summary_block`** — workload
   with `report:` items (no `table` form, only `plot` / `text`),
   no `summary:` block, no `summary=` CLI override. After the
   run, `session_metadata` has `report.<name>` rows. Today this
   test would fail because `active_summaries` is empty.

Both live in `nbrs-activity/tests/` or as integration tests
under `nbrs/tests/` — wherever existing report-persistence
coverage lives.

---

## Definition of done

- The `for item in workload_report.items() { ... }` loop is in a
  run-start block, not the `if !active_summaries.is_empty()`
  end-of-run block.
- The end-of-run block retains only the summary-writing path,
  with the report-item persistence removed.
- Both test cases above pass.
- `nbrs report list` against an in-progress session lists items
  immediately (verified manually against any long-running
  workload like `full_cql_vector.yaml fulltest`).
