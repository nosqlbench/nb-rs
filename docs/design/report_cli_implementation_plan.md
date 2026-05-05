# Implementation Plan: Report CLI (SRD-64)

Companion to:
- [SRD 64 — Report CLI](../sysref/64_report_cli.md) — normative
- [SRD 46 — Reports](../sysref/46_reports.md) — data model parent

The SRD specifies *what* the CLI surface is. This plan
specifies *how and when* the unfinished pieces land. Each
phase is independently mergeable, leaves the engine in a
working state, and ends with a green CI.

> **Status**: punch list in flight. Keep the table at the
> top current as work lands.

---

## Progress checklist

| Phase | Title | Status |
|-------|-------|--------|
| A | Vocab registry + `ReportItem::to_yaml_directive_string()` round-trip emitter | ✅ shipped — `nbrs-workload::report::vocab` (19 directives × kind-applicability + closed-set vocabularies + repeatability metadata, 12 tests); `ReportItem::to_yaml_directive_string()` + `Style::scalar_directive_lines()` round-trip emitter, 7 round-trip property tests covering minimal-plot / full-style / table / series-overrides / escaped-quotes / canonical directive ordering; all 38 nbrs-workload report tests green |
| B | `nbrs-workload::edit` workload-mutation primitive — 3-layer hybrid: `tree-sitter-yaml` locator (CST byte ranges, comments preserved by construction), `marked-yaml` emitter for missing-anchor inserts, byte-range splicer for the write; `fs2` flock + 2-deep `<workload>.bak`/`.bak.prev` rotation | ✅ shipped — `edit::{lock,backup,locate,splice}` + `add_item`/`replace_item`/`rename_item` public API; `with_workload` transactional driver that locks, reads, runs the mutation closure, validates by re-parsing, rotates `.bak`/`.bak.prev`, and atomically commits via temp+rename, with rollback on any failure. 36 tests including comment-preservation, quote-style-preservation, and concurrent-acquire blocking. `marked-yaml` import deferred — the splicer + literal-block-scalar formatter sufficed for the Phase B surface; will pull in when nested-anchor materialisation lands in Phase D. |
| C | `nbrs report` subcommand router + flag-form parser + scratch render path | ✅ shipped — `report_build` flag-form parser (consumes vocab directives + dispatch flags into a `ReportItem` + `Dispatch`, validates closed sets / hex / JSON / numbers, mutually-exclusive `--at` vs `--contextual`, 14 tests); `report_scratch` lifecycle (`scratch_paths` per kind, `timestamp_id` for `--name auto`, `nbrs report scratch list/clean/promote` dispatcher, 7 tests); `report_cmd` extended with kind-flag-form arm that routes `nbrs report plot|table <name> --<flag> ...` through the builder to the scratch dir; `--add` reserved with clean Phase-D-pending message; `nbrs_activity::session::resolve_active` consolidates active-session lookup. **Plus**: regression-fixed the orphaned-`scenario=` bug class — `normalize_args` and `nbrs::main` bare-file path now both peek for value-taking flags so `--session-path X` doesn't auto-promote `X` to `scenario=X` (5 new regression tests). |
| D | `--add` anchor selection (`root` / `--at` / `--contextual {auto,root,scenario,phase,op}`) | ✅ shipped — `nbrs-activity::report_anchor` resolver: `AnchorFlag::parse_at` / `parse_contextual` for the CLI surface; `resolve(db, item, flag)` walks `session_metadata.scenario` + `label_key='phase'` to pick the deepest unique scope under `auto`, errors with remediation hints on data span ambiguity in forced modes; op-template anchoring deferred with a clear schema-extension message. Wired into `report_cmd::run_add` end-to-end through the Phase B `edit::add_item` primitive — `--workload` precedence (explicit → `checkpoint.json::workload_path` → error), `--replace` collision policy, `--dry-run` prints chosen anchor + emit body without touching disk. 13 anchor tests + live exercise covering insert / dry-run / collision / replace / `--contextual auto` / `--contextual op` schema-gap / comment preservation across the full Phase B+D round trip. |
| E | `nbrs report rename <old> <new>` | ✅ shipped — `edit::rename_item(path, old, new, replace)` now supports the destructive `--replace` overwrite path (drops existing `new`, renames `old` over it). New helper `remove_existing_item` for the destructive splice. CLI dispatcher `run_rename` in `report_cmd` parses positionals + `--replace` / `--dry-run` / `--workload`, routes to the edit primitive. Self-rename (`old == new`) errors with "nothing to do"; nonexistent `old` errors clearly; collision without `--replace` points at the remediation flag. 4 new edit tests + live exercise verifying simple rename / collision / `--replace` destructive / nonexistent / self-rename / comment preservation. |
| F | Dynamic completion node rebuild against vocab registry | ✅ shipped — `report_node()` rebuilt as a per-kind subcommand tree (`StrictNode::group` with one child per `Kind` plus `list`/`all`/`show`/`figure`/`rename`/`scratch`). Each kind subcommand sources its flag list from `vocab::cli_flags_for(kind)` so plot offers axis flags + marker, table excludes them, text excludes figure-data flags. Per-flag value providers wire closed sets (palette / line / marker / agg / xscale / yscale) via dedicated `fn` pointers, db-derived sets (`--metric` / `--over` / `--by` / `--where`) via the existing `metric_provider` / `series_provider` / `filter_provider`, and orthogonal flags `--at` / `--contextual` via dedicated closed-set providers. 13 completion tests verify subcommand listing, per-kind flag filtering, closed-set value providers, partial-prefix narrowing. |
| G | §12 acceptance test (9-step end-to-end + concurrent-edit lock) | ✅ shipped — `nbrs/tests/report_cli_acceptance.rs::srd64_acceptance_full_flow` drives all 9 steps in one integration test: tiny workload setup, scratch render (no mutation), `--add` with root anchor + backup, name collision, `--add --replace` with backup-pair rotation, `nbrs report rename`, `--contextual auto` resolves to `phase:setup`, post-edit YAML parse, concurrent two-process `--add` lock semantics with byte-integrity check on the workload. Sandbox under `target/test-tmp/` (TMPDIR-redirected); no project-root pollution. |

---

## Phase A — vocab registry + round-trip emitter

**Closes:** the SRD-64 "single source of truth for grammar"
goal (§1) and unblocks Phase F (completion).

**Files touched:**

- `nbrs-workload/src/report/vocab.rs` — new module
- `nbrs-workload/src/report.rs` — add
  `ReportItem::to_yaml_directive_string()` and
  `Style::to_directive_lines()`
- Tests inline in both files

**Surgery scope:**

- New module is purely declarative — `Directive`, `ValueProvider`,
  per-kind directive tables. ~300 lines of data + accessors.
- The emitter pairs with the existing tokenizer; a round-trip
  property test asserts `parse → emit → parse` is identity for
  every shape the parser accepts today.

**Risk:** low. Pure data + serialization. No new deps.

**Acceptance:**

- `Vocab::directives_for(Kind::Plot)` returns every flag
  named in SRD-64 §3 plus their value providers.
- Round-trip property test passes on the corpus of YAML
  examples already in `nbrs-workload/tests/`.
- `cargo test -p nbrs-workload` green.

---

## Phase B — workload edit primitive

**Closes:** the §6.4-§6.5 "AST-preserving edit + backup +
locking" requirement. Single primitive consumed by Phases D
and E.

**Files touched:**

- `nbrs-workload/src/edit.rs` — new module
- `nbrs-workload/src/edit/locate.rs` — tree-sitter walker
- `nbrs-workload/src/edit/splice.rs` — byte-range splicer
- `nbrs-workload/src/edit/emit.rs` — generated-block formatter
- `nbrs-workload/Cargo.toml` — add `tree-sitter`,
  `tree-sitter-yaml`, plus `fs2` for cross-platform `flock`,
  plus a YAML emitter (`marked-yaml` or `yaml-rust2`) for
  the generated content
- Tests — concurrent-write contention, backup pair rotation,
  roundtrip rollback, comment / quote-style preservation

**Architecture (3-layer hybrid):**

The "AST-preserving edit" requirement decomposes into three
distinct subproblems that no single library solves cleanly:

1. **Locate** the target byte range — find where the
   `report:` block lives at the chosen anchor (root /
   scenario / phase / op) inside the original YAML source.
2. **Emit** new content — render a `ReportItem` (or a
   whole sub-block) as well-formed YAML matching the
   surrounding indentation conventions.
3. **Splice** the emitted bytes back into the source —
   replace the located range with the emitted block,
   leaving the rest of the file (comments, blank lines,
   non-target keys, quote styles) byte-identical.

Each layer picks a different library:

- **Locate: `tree-sitter-yaml`.** Produces a concrete
  syntax tree (CST) — every node has a byte range that
  maps back to the original source. Comments and
  whitespace are first-class CST nodes, not stripped.
  We walk the CST to find the target by YAML path
  (e.g. `scenarios.<name>.report.cli_added`) and read off
  the byte range of the value (or the gap to insert into).
- **Emit: `marked-yaml` + the existing `vocab::ALL_DIRECTIVES`
  emitter.** For new groups / new items, render a single
  multi-line block scalar string that the existing parser
  in [`super::report`] accepts. The Phase A
  `ReportItem::to_yaml_directive_string` does most of the
  work; `marked-yaml` only matters when we're inserting a
  new top-level `report:` mapping that didn't exist before.
- **Splice: byte-range string splicing.** Once located
  and emitted, the actual write is `original[..start] +
  emitted + original[end..]`. No library needed.

This avoids the round-trip-emission problem entirely:
nothing other than the spliced range gets re-serialized,
so quote styles, comment placement, and key ordering in
unrelated parts of the file are preserved by construction.

**Locking + backup (orthogonal to the YAML problem):**

- `fs2::FileExt::lock_exclusive` with a 5-second poll loop
  and pid surfacing on contention.
- Backup rotation: `<workload>.bak.prev ← <workload>.bak`,
  then `<workload> → <workload>.bak`, then write new
  content. All via `fs::rename` for atomicity. Two-deep
  policy floor.

**Risk:** medium. The 3-layer split contains the risk in
the locator layer (which is read-only — wrong byte range
is a clear test failure, not a corruption). The splicer
and emitter are simple enough to land confidently.

The riskiest sub-problem is **insertion at a missing
anchor** — e.g., `--add --at scenario:foo` when `foo` has
no `report:` block yet. The locator returns "no target;
here's the byte position where the new key should be
inserted, with this indentation level"; the emitter
generates the new `report:` mapping including its
group-and-item structure; the splicer inserts at the gap.
This needs careful indent computation but is bounded.

**Acceptance:**

- `edit::with_workload(path, |yaml_handle| { ... })?` is
  the only public entry point; mutations flow through it.
- Concurrent test: two threads attempt `--add` against the
  same file; one succeeds, the other waits or errors with
  pid (does not corrupt the file).
- Backup pair test: three sequential edits leave
  `<workload>.bak` (just-prior) and `<workload>.bak.prev`
  (one-before-prior) populated; `cmp` agrees.
- Roundtrip-fail rollback: a deliberately-broken mutation
  leaves the workload byte-identical to the pre-edit state.
- **Comment-preservation test.** A workload with mixed
  comments (block, inline, leading), after one
  `--add` mutation, has every original comment present at
  the same position with byte-identical text — verified
  by `diff` against a pre-computed expected output.
- **Quote-style preservation test.** A workload with
  `key: "double"`, `key2: 'single'`, `key3: bare` keeps
  each style exactly as written for keys outside the
  edited range.

---

## Phase C — subcommand router + flag-form parser

**Closes:** §2 (subcommand surface), §3 (flag-form parity),
§5 (default scratch render destination), §7 (scratch
lifecycle, render-only paths). Does not include `--add`.

**Files touched:**

- `nbrs/src/report_cmd.rs` — heavy edits, ~500 lines of
  new dispatch logic
- `nbrs-activity/src/session.rs` — new
  `resolve_active(args, env) -> SessionHandle` helper
  consolidating the patterns from `replay.rs` /
  `summary.rs`
- `nbrs/src/main.rs` — route `nbrs report scratch ...`,
  `nbrs report rename ...`, `nbrs report list/all/figure/show`

**Surgery scope:**

- Subcommand dispatch is shallow: each `<kind>` subcommand
  parses flags via the Phase A vocab registry, builds a
  `ReportItem` AST node, hands it to the existing renderer,
  writes to `<session>/scratch/<name>.<ext>`.
- Active-session resolution helper consolidates four
  near-duplicates from `replay.rs` / `summary.rs` /
  `report_cmd.rs` / `metricsql_cmd.rs`.
- `--add` flag is reserved at the parser level but errors
  with "not yet wired — pending Phase D" so the surface is
  visible.

**Risk:** medium. Wide surface; each subcommand is a leaf
but the per-kind flag rules need vocab integration.

**Acceptance:**

- `nbrs report plot foo --over cycle --metric throughput`
  (against an active session) renders `plot_foo.png` and
  `scratch/foo.md`. No workload mutation.
- `nbrs report list` enumerates items from the workload
  pointed at by the active session.
- `nbrs report show <name>` prints the resolved spec
  including cascaded styles.
- `nbrs report scratch list/clean` operate on the
  `<session>/scratch/` directory.

---

## Phase D — `--add` anchor selection

**Closes:** §6.1 (anchor model), §6.2 (group default), §6.3
(name collision policy). Promotes single items to the
workload via the Phase B primitive.

**Files touched:**

- `nbrs-activity/src/report_anchor.rs` — new module,
  ~250 lines for the dispatch-tree walk
- `nbrs/src/report_cmd.rs` — wire `--add`, `--at`,
  `--contextual`, `--replace`, `--rename`, `--group`
- Phase B edit primitive — accept the resolved anchor as
  input

**Surgery scope:**

- `report_anchor::resolve(item, session, mode)` is the
  centerpiece. It runs metricsql against the session db to
  find every (scenario, phase, op-template) tuple that
  emitted at least one row matching the item's filter.
- `--contextual auto` walks coarsest-fits-uniquely; forced
  `--contextual <level>` errors if data is broader.
- Group selection defaults to `cli_added`; `--group`
  override.
- Name collision check walks the workload's already-parsed
  `Report` looking for the target name; emits an
  informative error if found and `--replace` not passed.

**Risk:** medium. The metricsql queries need to be schema-
correct for the existing metrics db; that's where most of
the bug surface lives.

**Acceptance:**

- §12 steps 3-7 pass against a live workload.
- Errors point at remediation flags
  (`--replace` / `--rename` / coarser `--contextual` mode).

---

## Phase E — rename

**Closes:** §6.6 (`nbrs report rename`).

**Files touched:**

- `nbrs-workload/src/edit.rs` — add a `rename_item` op
- `nbrs/src/report_cmd.rs` — route the subcommand

**Surgery scope:**

- Pure metadata edit through the Phase B primitive. No
  rendering. Anchor stays at the existing site.
- Collision policy mirrors `--add --replace`.

**Risk:** low.

**Acceptance:**

- §12 step 6 passes.
- Backup pair shows the rename in the diff.

---

## Phase F — completion node rebuild

**Closes:** §1 goal 1 (discoverability), §4 (completion
cascade), §4.3 (per-flag value providers).

**Files touched:**

- `nbrs/src/completion.rs` — replace the current 20-line
  flat `report_node()` with a kind-aware tree (~200 lines)
- New value providers: `metric_provider_kind_aware`,
  `label_key_provider`, `label_value_provider`
  (parameterized by current `--where` key)

**Surgery scope:**

- Rebuild `report_node()` using `Node::group` with one
  child per kind subcommand.
- Each kind node carries its directive flag list from the
  Phase A vocab registry.
- Closed-set value providers wrap the vocab `ValueProvider`
  variants; db-derived ones share the existing
  `metric_provider` / `series_provider` plumbing.

**Risk:** medium. Completion failures are silent; needs the
per-flag tests from §12.

**Acceptance:**

- `nbrs report <TAB>` offers
  `plot table text file details list all show figure rename scratch`.
- `nbrs report plot <existing-name> --<TAB>` offers exactly
  the directive flags from SRD-64 §3 for `Plot`.
- `nbrs report plot foo --palette <TAB>` offers the closed
  palette set.
- `nbrs report plot foo --over <TAB>` lists distinct label
  keys observed in the active session db.

---

## Phase G — acceptance test

**Closes:** §12 (acceptance test sketch).

**Files touched:**

- `nbrs/tests/report_cli_acceptance.rs` — new integration
  test, ~250 lines
- Test fixture: a tiny workload + pre-built session db

**Surgery scope:**

- Drive the 9-step §12 sequence as a single integration
  test (or 9 separate `#[test]` fns sharing a fixture).
- Concurrent-edit lock test spawns two threads, asserts
  one waits or errors with the holding pid.
- Per-directive completion assertions piggyback on the
  existing completion-test plumbing.

**Risk:** low.

**Acceptance:**

- Test green in CI.
- Each step's intermediate state visible in test output for
  diagnostic walk-through.

---

## Open implementation choices

These deliberately punted to "decide at start of phase" so
the plan doesn't lock in choices before evidence arrives.

- **Phase B library mix.** Architecture decided:
  `tree-sitter-yaml` for the read/locate layer (CST with
  byte ranges, comments preserved by construction);
  `marked-yaml` for emitting new structural blocks (only
  needed for missing-anchor inserts); byte-range splicing
  for the write. Spike validates this choice against a
  fixture workload before the rest of the layer lands.
- **Phase D contextual-walk implementation.** Choice between
  hand-rolled metricsql query vs reusing the metrics-cache
  walker. Decide after reading the existing
  `metrics_cmd.rs::list` to see how its tuple-extraction
  shape generalizes.
- **Phase F db-derived completion staleness.** Caching for
  speed vs. always-fresh queries against the active session
  db. Lean: always-fresh until the latency is visibly bad,
  then add a 1s-TTL cache.
