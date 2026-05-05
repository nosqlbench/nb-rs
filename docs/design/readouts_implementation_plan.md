# Implementation Plan: Readouts (SRD-63)

Companion to `docs/sysref/63_status_readouts.md`. The SRD
specifies *what* readouts are; this plan specifies *how
and when* each piece lands. Each push is independently
mergeable, leaves the binary in a working state, and ends
with a green CI.

> **Status: in progress.** This document is the working
> punch list. Update push-by-push as work lands; keep the
> SRD stable. The two top tables are the canonical
> "where are we" view — they are kept current as code
> lands.

## Progress checklist

| Push | Title | Status |
|------|-------|--------|
| 1 | Trait, registry, `phase_done` at `on_phase_end` | ✅ done |
| 2 | `on_update` event + `phase_status`, retire inline thread | ✅ done |
| 3 | Workload `readouts:` block (Forms A / B / C) + `ReadoutBinder` skeleton | ✅ done |
| 4 | Color / style sub-language + wildcard bindings | ✅ done |
| 5a | Stateful `TuiReadoutBinder` + `Lod::Expanded` for `phase_status` | ✅ done |
| 5b | TUI app-loop integration: key forwarding, focus highlight, panel rewrite | ✅ done — TuiReadoutSink (ANSI→ratatui), readout_panel adapter, key wiring (Tab/+/-/`\`), focus tint, frame_tick spinner, integrated into format_phase_detail_with_live |
| 6 | Snapshots and session retention | ✅ done |
| 7 | Explanation overlays for built-in readouts | ✅ done |
| 7b | TUI surface wires `OverlayHeld` keystroke (depends on 5b) | ✅ done — bound to `\` as press-toggle (crossterm's standard polling doesn't surface key-release reliably; full held-key would need Kitty keyboard protocol — deferred enhancement, pragmatic press-toggle covers the user-facing intent) |
| 8 | `--readout` CLI override + `phase_summary` readout | ✅ done |
| 8b | Post-run observer phase rows through `phase_summary` | ✅ done |
| 8c | `scope_header` readout + post-run / live scope rows through it | ✅ done |
| 8d | Inspector socket text + web report HTML through the engine | 🟡 partial — `readout` command added to inspector socket (G20 closed); HTML report (G21) is N/A (reports are markdown-driven, no phase-row HTML surface exists) |
| 9 | Audit-driven gap remediation (see status table below) | ✅ done (6/6 active sub-pushes; remaining items deferred to TUI bundle) |

## Audit-driven gap status

Tracks the 11 active gaps + 4 already-deferred items the
post-Push-8c audit surfaced. Update as each gap is closed
(✅), narrowed (🟡 partial), or actively held (⏳ deferred).

### Spec-promised behaviour the code doesn't yet deliver

| # | Gap | Severity | Status | Lands in |
|---|-----|----------|--------|----------|
| G1 | 7 of 9 lifecycle events never fire (`on_phase_start`, `on_each_*`, `on_scope_*`, `on_session_*`) | 🔴 major | ✅ closed | Push 9a |
| G2 | `ReadoutOptions` is empty struct — `precision=` / `unit=` / `pattern=` parse but discard | 🔴 major | ✅ closed | Push 9b |
| G3 | `metric:pattern` colon-arg parses but is discarded; readout still falls back to whole `status_metric_chips()` | 🔴 major | ✅ closed | Push 9b |
| G4 | Synthesised final `on_update` tick at `_end` boundary (SRD §6.2) not implemented | 🔴 major | ✅ closed | Push 9c |

### Spec vocabulary missing builtins

| # | Builtin | Default slot | Status | Lands in |
|---|---------|--------------|--------|----------|
| G5 | `each_close` | `on_each_end` | ✅ closed | Push 9d |
| G6 | `phase_starting` (opt-in) | `on_phase_start` | ✅ closed | Push 9d |
| G7 | `scope_open` | `on_scope_start` | ✅ closed | Push 9d |
| G8 | `scope_close` | `on_scope_end` | ✅ closed | Push 9d |
| G9 | `session_banner` | `on_session_start` | ✅ closed | Push 9d |
| G10 | `session_summary` | `on_session_end` | ✅ closed | Push 9d |

### Code surfaces still bypassing the engine

| # | Surface | Status | Lands in |
|---|---------|--------|----------|
| G11 | Failed-phase inset row (`observer.rs:596`) — direct eprintln, ignores `phase_summary` | ✅ closed | Push 9e |
| G12 | Session-rollup line `phases:  X completed, Y failed, Z not run` (`observer.rs:431`) | ✅ closed | Push 9d (via `session_summary`) |
| G13 | Pre-map "not reached" warning (`observer.rs:660`) | ✅ closed | Push 9h |

### Design-intent partials

| # | Partial | Status | Lands in |
|---|---------|--------|----------|
| G14 | `phase_status` computes ETA inline rather than via `ctx.eta_secs()` | ✅ closed | Push 9f |
| G15 | `lod:colon` shorthand for options doesn't work (only primary arg) | ✅ closed | Push 9f |
| G16 | `LayoutHint::Focused` decoration emitted but no sink applies it | ✅ closed — `readout_panel::apply_focus_tint` wraps the focused-slot's lines with a background tint when `binder.focus_for(event).is_some()` | Push 5b |
| G17 | `phase_done` Compact / Expanded forms zero-byte (Labeled-only) | ✅ closed | Push 9g |

### Already-deferred (TUI-integration bundle)

| # | Item | Status |
|---|------|--------|
| G18 | TUI app-loop key forwarding (`Tab` / `+` / `-`) + focus highlight | ✅ closed — Push 5b |
| G19 | `OverlayHeld` keystroke wiring (held-key explanation overlay) | ✅ closed — Push 7b (press-toggle on `\`) |
| G20 | Inspector socket text mode through engine | ✅ closed — `readout [name]` command in inspector_server |
| G21 | Web report HTML through engine | N/A — reports are markdown + SVG; no HTML phase-row surface exists |

### Push 1 — completed tasks

- [x] Create `nbrs-activity::readouts` module skeleton
  (originally planned as a separate `nbrs-readouts` crate;
  see Deviations below for the reason)
- [x] Implement core types: `Lod`, `ContentMode`,
  `ReadoutOptions`, `ReadoutBuf`, `Readout` trait
- [x] Implement `ReadoutContext` trait (minimal surface
  for `phase_done`)
- [x] Implement static `Registry` + `phase_done` builtin
- [x] Implement `ActivityReadoutContext` in
  `nbrs-activity::readout_context`
- [x] Wire `phase_done` into `activity.rs`'s end-of-activity
  ✓ DONE block — output byte-equivalent to pre-engine
  format string
- [x] 4 unit tests for `phase_done`; activity (255) +
  nbrs (29) + scheduler (3) green; visual integration
  check on a multi-coord workload

### Push 2 — completed tasks

- [x] Add `Event` enum (lifecycle / refresh delamination)
  + extend `ReadoutContext` with `event()`,
  `refresh_tick()`, `ops_started()`, `ops_finished()`,
  `eta_secs()`, `activity_name()`,
  `adapter_counters_text()`, `batch_info_text()` (each
  default-impl'd so contexts don't churn each push)
- [x] Implement `phase_status` builtin —
  `Lod::{Compact, Labeled}` × `ContentMode::Value`;
  spinner + bar + counters + adapter + batch + chips +
  ETA at Labeled; trimmed compact form
- [x] Implement minimal `trace` diagnostic readout —
  surfaces every relevant `ReadoutContext` field for
  every event; doubles as the smallest reference impl
  for custom-renderer authors
- [x] Move presentation helpers (`spinner_frame`,
  `braille_bar`, `format_eta`, `format_rate`) from
  `activity.rs` into `readouts/format.rs` next to the
  consumers
- [x] Add `InlineRefreshContext` +
  `build_inline_refresh_context` constructor in
  `readout_context.rs`
- [x] Convert the inline-progress thread in `activity.rs`
  to drive `phase_status` per tick — same cadence
  (500 ms), same `\r\x1b[K` clear, same gates
  (`is_stderr_tty`, `total_extent > 1000`,
  `!suppress_progress`, `!suppress_status_line`)
- [x] 13 readouts unit tests; activity (264) + nbrs (29)
  + scheduler (3) + tui (14) green

### Push 3 — completed tasks

- [x] `Readout` / `Readout​Binder` / `ReadoutSink` /
  `LayoutHint` / `LayoutMode` / `BinderKey` / `BakedBody`
  / `RenderStep` traits & types in
  `nbrs-activity/src/readouts/binder.rs`
- [x] Body-grammar parser + bake step list in
  `readouts/parse.rs` (whitespace-separated items, quoted
  literals, `name:arg` shorthand, `key=value` options
  including `lod=` / `layout=`)
- [x] `metric` parameterised readout (Push 3 thin shim
  over the existing `status_metric_chips()` surface)
- [x] `Workload.readouts: ReadoutsBindings` field +
  per-slot accessor; parser accepts SRD-63 §5.0 Forms
  A / B / C
- [x] `DefaultBinder` (stateless) + `StringSink`
  (terminal-mode line buffer)
- [x] `build_event_binder` helper applies the workload's
  bound bodies on top of the supplied default
- [x] `nbrs-activity::activity` inline-status thread and
  ✓ DONE block both fire through the binder (replacing
  the direct `phase_status.render()` /
  `phase_done.render()` calls); byte-equivalent output
- [x] Shadow-terminal integration test
  (`nbrs/tests/readout_pipeline.rs`): a workload binding
  `on_phase_end: trace` produces the trace readout's
  output through a real PTY
- [x] activity (283) + workload (109) + nbrs (29) +
  scheduler (3) + readout_pipeline (1) + tui (14) green

### Push 4 — completed tasks

- [x] `readouts/color.rs`: `ColorSpec` (Direct / Bright /
  Rgb / Style / Dim) + `StyleName` (Error / Warn / Info /
  Ok / Header / Subhead / Emphasis / Muted) + Wong palette
  resolver + ANSI emission
- [x] Body grammar recognises `@COLOR` and `[#hex]` inline
  directives — single-shot, wrap the next non-directive
  step in ANSI on/off bytes
- [x] Per-readout `color=` / `style=` options resolve
  through the same `ColorSpec` path; option wins over
  inline directive (more explicit)
- [x] Wildcard event-slot bindings: `each_*` / `phase_*` /
  `scope_*` / `session_*` / `*` expand at workload-load
  into the matching slot set
- [x] Composition / override resolver (SRD-63 §5.4.1):
  Rule 2 (REPLACE by default), Rule 3 (`+`-prefix
  appends to default; mixed list treats whole list as
  replacement)
- [x] activity (305) + workload (116) + nbrs (29) +
  scheduler (3) + readout_pipeline (1) + tui (14) green

### Push 5a — completed tasks

- [x] `TuiReadoutBinder` — stateful binder with focus
  index per slot, per-(slot,body) LOD overrides,
  overlay-held flag
- [x] `BinderKey::{CycleFocusNext,CycleFocusPrev,
  CycleLodUp,CycleLodDown,OverlayHeld(bool)}` interpreted
  via `on_key`
- [x] `LayoutHint::Focused(...)` wraps the focused body's
  hint so sinks can apply visual emphasis without the
  readout knowing
- [x] `Lod::Expanded` content for `phase_status` —
  multi-line block (progress / throughput / counters /
  adapter / batch / metrics)
- [x] activity (310) + workload (116) + nbrs (29) +
  scheduler (3) + readout_pipeline (1) + tui (14) green

### Push 6 — completed tasks

- [x] `readout_snapshots` table in
  `nbrs-metrics/src/reporters/sqlite.rs` per SRD-63 §6.4
  schema; `upsert_readout_snapshot` (insert-or-replace)
  + `read_readout_snapshots` reader. PK
  `(slot, subject_kind, subject_id, readout_name, lod)`
  collapses repeats to the latest render.
- [x] `nbrs-activity::readouts::snapshot` module:
  `strip_ansi`, `build_subject_id`, `lod_str`,
  `capture(writer, slot, subject_kind, subject_id,
  readout_name, lod, rendered)` — best-effort
  upsert helper.
- [x] `ActivityConfig.snapshot_writer` plumbed through
  executor.rs / runner.rs from the session-level
  `Arc<Mutex<Option<SqliteReporter>>>`.
- [x] Snapshot capture wired at both fire sites in
  `activity.rs` (the inline-status thread's per-tick
  `Event::Update` and the end-of-activity
  `Event::PhaseEnd` block).
- [x] `nbrs replay` CLI command (`nbrs/src/replay.rs`) —
  walks the snapshot table and prints rendered bodies
  to stdout. `--session=<dir>` / `--db=<path>` /
  `--plain` / `-h` options.
- [x] activity (315) + metrics (227, +3 round-trip
  tests) + workload (116) + nbrs (29) + scheduler (3)
  + readout_pipeline (1) + tui (14) green; end-to-end
  replay verified against a real `nbrs run` session db.

### Push 7 — completed tasks

- [x] `phase_done` Explanation: descriptive overlay (✓ →
  "done", `[name]` → "[phase-name]", `(coords)` →
  "(scope-coords)", percent / counters / `(elapsed)` →
  semantic descriptors). Width-parity with Value
  preserved per SRD-63 §3.2.
- [x] `phase_status` Explanation at all three LODs
  (Compact: `spin progress% rate/s`; Labeled: full
  bar + counters + ETA descriptors; Expanded:
  multi-line block with one descriptor per row).
- [x] `metric` Explanation emits a "live aggregates"
  descriptor when chips would be present; zero-byte
  when chips empty (matches Value visibility).
- [x] `trace` Explanation produces a static schema
  dump — same field list, each row reads
  `field: <semantic type>` instead of `field=<value>`.
- [x] activity (317, +2 explanation-mode tests) +
  metrics (227) + workload (116) + nbrs (29) +
  scheduler (3) + readout_pipeline (1) + tui (14)
  green.

### Push 8 — completed tasks

- [x] `--readout=<body>` CLI flag (registered in
  `parse_params`'s consumed-flags list; resolved via
  `crate::session::resolve_flag` so it picks up the
  matching `NBRS_READOUT` env var).
- [x] `ExecCtx.cli_readout_override` +
  `ActivityConfig.cli_readout_override` plumbing
  through executor / runner construction.
- [x] `binder::build_event_binder_with_cli` — applies
  the override to the `Update` slot only (other slots
  fall through unchanged), supports `+` prefix for
  append, warns when a non-default workload binding
  is being silently replaced (SRD-63 §5.4.1 Rule 2
  safety net).
- [x] `phase_summary` readout (`[ok] [N/total] name
  duration` form). Available for workload binding
  via the `readouts:` block — the live observer's
  post-run tree-walk integration is deferred to Push
  8b (needs `phase_state()` on `ReadoutContext`).
- [x] activity (324, +7 from CLI override + phase_summary
  tests) + metrics (227) + workload (116) + nbrs (29) +
  scheduler (3) + readout_pipeline (1) + tui (14) green.

### Push 8b — completed tasks

- [x] `PhaseState` enum + `phase_state()` accessor on
  `ReadoutContext` (default `Running`; lifecycle
  contexts override).
- [x] `ActivityReadoutContext::phase_state` → `Completed`
  (matches the end-of-activity fire site).
- [x] `phase_summary` Value branch on `phase_state` —
  `[ok]` / `[!!]` / `[..]` / `[  ]` markers + status
  suffix; 4 new tests cover all branches.
- [x] `nbrs-tui::observer::emit_run_summary`'s phase
  rows now route through `phase_summary` via a
  small `SummaryRowContext` adapter; output
  byte-equivalent to the prior eprintln.
- [x] activity (327, +3 state-branch tests) + metrics
  (227) + workload (116) + nbrs (29) + scheduler (3)
  + readout_pipeline (1) + tui (14) green.

### Push 8c — completed tasks

- [x] `scope_header` readout (`· {scope_name}` form) +
  Compact / Labeled / Expanded LODs + Explanation
  overlay; cyan-bullet + italic-name byte sequence
  honors `use_color`.
- [x] `nbrs-tui::observer`'s post-run summary
  scope-row eprintln + the focused-error-inset's
  scope-ancestor walk both route through
  `scope_header` via a `ScopeRowContext` adapter.
- [x] `nbrs-tui::log_only_observer`'s live mid-run
  scope-ancestor walker (the
  `· for_each profile=…` lines that fire on each
  fresh iteration during a run) routes through
  `scope_header` via a `ScopeAncestorContext`
  adapter.
- [x] activity (332, +5 scope_header tests) +
  metrics (227) + workload (116) + nbrs (29) +
  scheduler (3) + readout_pipeline (1) + tui (14)
  green; visual byte-equivalence verified against
  a multi-coord workload.

### Push 8d — pending tasks

- [ ] Inspector socket text mode through the engine —
  the inspector's structured access continues to
  bypass readouts per SRD-63 §2 / §11 decision 3,
  but the text-mode `inspector phases` /
  `inspector active` etc. surfaces benefit from
  consistent formatting via `phase_summary` /
  `phase_status`.
- [ ] Web report HTML — `nbrs report ...` already
  goes through the snapshot store (Push 6), but
  embedding the live ✓ DONE / inline-status forms
  in HTML reports needs a sink that emits HTML
  spans for the ANSI styling. Standalone work,
  scoped only when there's demand.

### Push 5b — pending tasks

- [ ] Wire `TuiReadoutBinder` into `nbrs-tui::app`'s run
  loop: forward keyboard events as `BinderKey`, swap the
  default binder for a stateful one when the TUI is the
  active surface
- [ ] Focus highlight rendering in the active-phase panel
  — `LayoutHint::Focused` decoration applied by the
  ratatui sink (offset / background tint per the focused
  body)
- [ ] Active-phase panel + scenario tree row through the
  binder (replaces the existing direct readout calls in
  `nbrs-tui::app::draw_active_phase_panel` /
  `draw_tree`)
- [ ] Shadow-terminal key-handling integration test
  (`nbrs/tests/readout_keys.rs`) — drive `Tab` /
  `+` / `-` / `?`-held through `SteppableTerminal` and
  assert the visible output reflects each interaction

### Push 9a — completed tasks  *(closed G1)*

- [x] `LifecycleContext` adapter in
  `nbrs-activity::readout_context` — minimal
  `ReadoutContext` impl carrying just the fields
  structural readouts need (subject name + labels +
  depth indent + colour + event).
- [x] `fire_lifecycle()` helper — builds binder via
  `build_event_binder`, runs through `StringSink`,
  routes through `crate::diag!`, captures snapshot.
  Empty-body bindings stay quiet (no spurious blank
  lines for slots with no workload binding).
- [x] `Event::SessionStart` / `SessionEnd` fired in
  `nbrs-activity::runner`'s `run_impl` — start fires
  right after the SQLite reporter is constructed, end
  fires after `cadence_reporter.shutdown()` so both
  branches (phased + single-activity) converge through
  the same fire site.
- [x] `Event::PhaseStart` fired in
  `nbrs-activity::executor::run_phase` right after
  `set_phase_running` so the scene-tree state is
  consistent with what `phase_done` will see at
  PhaseEnd.
- [x] `Event::EachStart` / `EachEnd` fired in
  `run_one_iteration` — every iteration of a
  `for_each` / `for_combinations` produces a paired
  start/end. Subject ID is `"{iter_label}@{coords}"`
  so nested loops collapse correctly in the snapshot
  store.
- [x] `Event::ScopeStart` / `ScopeEnd` fired around
  `do_while` / `do_until` via `fire_scope_lifecycle`
  helper. Subject ID carries the scope's spec text.
- [x] 5 shadow-terminal-style integration tests in
  `nbrs/tests/readout_lifecycle_events.rs` — one per
  event family + a universal-wildcard test that
  binds `*: trace` and verifies every reachable slot
  fires.
- [x] activity (332) + metrics (227) + workload (116)
  + nbrs (29) + scheduler (3) + readout_pipeline (1) +
  readout_lifecycle_events (5) + tui (14) green.

### Push 9b — completed tasks  *(closed G2, G3)*

- [x] `ReadoutOptions` no longer empty — backed by an
  inline `Vec<(String, OptionValue)>` with typed
  accessors `get_str` / `get_int` / `get_float` /
  `get_bool` and `set` for last-one-wins inserts.
- [x] Body parser's `apply_option` falls through any
  unrecognised key into the option store rather than
  silently dropping it. Structural keys (`lod`,
  `layout`, `color`, `style`) keep their dedicated
  paths.
- [x] `name:arg` colon-shorthand routes its argument
  into the options under the conventional key
  `pattern` — matches the SRD-63 §5.1
  `metric:recall* ≡ metric pattern="recall*"`
  example.
- [x] `metric` builtin filters chips by `pattern=`
  (glob-matched) when set; bare `metric` emits all
  chips (Push 3 backwards-compat). Glob is the same
  minimal `*`/`?` matcher the rest of the engine
  uses.
- [x] 5 new metric tests covering `recall*` filter,
  `latency*` filter, exact-name match, no-match
  returns zero, bare `metric` emits all.
- [x] activity (337) + workload (116) + nbrs (29) +
  scheduler (3) + readout_pipeline (1) +
  readout_lifecycle_events (5) green.

### Push 9c — completed tasks  *(closed G4)*

- [x] Synthesised final `on_update` fire added at the
  end of the activity DONE block (right before the
  `phase_done` fire). Reuses the existing
  `build_inline_refresh_context` + binder path; uses
  the activity's last-known counters / status_metric
  chips / elapsed time.
- [x] Render goes only to the snapshot store — the
  visible DONE line still comes from the
  `on_phase_end` fire that follows.
- [x] Works for short phases too — even when the
  inline-status thread never spawned (gated on
  `is_stderr_tty && total_extent > 1000`), the
  synthesised final tick guarantees the snapshot
  store has an `on_update` row per phase.
- [x] Verified end-to-end via `nbrs replay` against a
  short-phase session: both `on_update` and
  `on_phase_end` rows present, with body content
  reflecting end-state counters.
- [x] activity (337) + nbrs (29) + scheduler +
  readout_pipeline + readout_lifecycle_events green.

### Push 9 — audit-driven gap remediation

Each sub-push closes a tightly-scoped subset of the audit
gaps from the top-of-document status table. Order
deliberately picks the highest-leverage items first so
each merge is independently shippable.

#### Push 9a — Fire all 7 missing lifecycle events  *(closes G1)*

- [ ] Add `Event::PhaseStart` fire in
  `executor::run_phase` right after `set_phase_running`
  (before the activity spawn). Build a one-shot binder,
  fire, capture to snapshot store.
- [ ] Add `Event::EachStart` / `EachEnd` fires in the
  comprehension dispatcher (`executor::dispatch_comprehension`
  or the for_each / for_combinations path) at the
  iteration enter / leave boundaries.
- [ ] Add `Event::ScopeStart` / `ScopeEnd` fires for
  non-iteration scope groups (the do_while / do_until
  paths and any other scope kinds).
- [ ] Add `Event::SessionStart` / `SessionEnd` fires in
  `runner::run_impl` — start fires once after session
  construction, end fires before the post-run summary.
- [ ] Each fire site builds a `DefaultBinder` via
  `build_event_binder`, walks the workload's bindings
  for that slot, and runs through `StringSink` →
  `crate::diag!`. Same pattern as the existing
  `on_phase_end` site.
- [ ] Snapshot capture wired at every fire site.
- [ ] Tests: a workload binding `each_*: trace` produces
  `event=on_each_start` and `event=on_each_end` lines;
  same for `phase_*`, `scope_*`, `session_*`. Single
  shadow-terminal test per family.

#### Push 9b — `ReadoutOptions` storage + `metric:pattern`  *(closes G2, G3)*

- [ ] Replace the empty `ReadoutOptions` struct with
  `Vec<(SmolStr, OptionValue)>` (or a small inline
  hashmap). Add typed accessors: `get_str`, `get_int`,
  `get_float`, `get_bool`, with `or_default` helpers.
- [ ] Update the body parser's `apply_option` to push
  unrecognised keys into the option store rather than
  silently dropping. Known structural keys (`lod`,
  `layout`, `color`, `style`) keep their current
  dedicated paths.
- [ ] Wire `primary_arg` from the parser into the
  options store under a conventional key like
  `pattern` or the readout's declared primary-option
  name. (Decide: hardcode `pattern` for now, or
  introduce a `Readout::primary_option_name()`
  method?)
- [ ] Update `metric` builtin: read `pattern=…` from
  options; if present, glob-filter the chip string to
  only matching names. Falls back to the whole chip
  set when no pattern given (preserves Push 3
  behaviour).
- [ ] Tests: `metric:recall*` filters out latency
  chips; `metric pattern="latency_p99"` renders only
  the p99 chip; bare `metric` renders everything.

#### Push 9c — Final on_update tick at phase_end  *(closes G4)*

- [ ] In `activity.rs` after the inline thread loop
  exits (when `progress_flag` drops), fire one
  synthesised `on_update` render with the phase's
  last-known context. Capture to snapshot store so
  the latest snapshot reflects the actual end-state
  rather than the most recent 500 ms tick.
- [ ] Update the inline thread's `\r\x1b[K` clear to
  not race the final fire — clear before the
  synthesised tick, then let the ✓ DONE line replace
  it as today.
- [ ] Tests: shadow-terminal test that runs a workload
  with rate-limited cycles such that the natural
  500 ms cadence misses the last 100 ms; verify the
  snapshot store's `on_update` body matches the
  final state, not the penultimate tick.

#### Push 9d — Implement missing structural builtins  *(closes G5–G10, G12)*  ✅

- [x] `session_summary` — totals across phases:
  `phases: X completed, Y failed, Z not run`. Compact
  emits the slash-form `5/2/1/8`; Labeled matches the
  pre-engine observer rollup byte-for-byte; Expanded
  splits onto multi-line breakdown.
- [x] `session_banner` — `session: <scenario> (<workload>)`
  opening row, with workload tail dropped on inline-CLI
  runs that lack a yaml on disk.
- [x] `each_close` — companion to `scope_header`'s `·`
  opener: thin `└─ end` close-marker; Expanded re-prints
  the iteration tuple labels for scrollback context.
- [x] `scope_open` / `scope_close` — `┌─ name` and
  `└─ end name` glyphs for non-iteration scope groups
  (`do_while` / `do_until`); Expanded surfaces elapsed
  on close.
- [x] `phase_starting` — `▶ {name} ({idx}/{total}) starting`
  pre-phase header. Compact drops the seq + word, Expanded
  appends iter-tuple labels.
- [x] All five new builtins + `session_summary` ship
  Compact / Labeled / Expanded Value forms +
  Explanation overlay (per §3.3); 22 new unit tests.
- [x] Registered in `Registry::lookup()` /
  `Registry::all_names()`.
- [x] Wired `session_banner` + `session_summary` into the
  post-run observer (`print_post_run_summary`) via a
  shared `SessionSummaryContext`. Replaces the
  `observer.rs:411` and `observer.rs:431` direct
  `eprintln!`s. Closes G12.
- [x] `ReadoutContext` extended with
  `session_scenario_name()` / `session_workload_file()`
  (default empty) so session-scope readouts can pull
  identity without bleeding it into the phase-row
  contexts.

#### Push 9e — Failed-phase inset through `phase_summary`  *(closes G11)*  ✅

- [x] Added `show_labels` option to `phase_summary`:
  when `true`, the iter-tuple coord-path renders
  inline as ` ({labels})` between name and
  duration/suffix. Default off — primary post-run
  rows still hide labels (carried by scope-header
  rows above).
- [x] Replaced the `eprintln!("  {indent}[!!] ...")` in
  `nbrs-tui::observer::print_post_run_summary`'s
  failed-phase inset with a `PhaseSummary.render()`
  call against a `SummaryRowContext` carrying
  `LifecycleState::Failed(err)` and `show_labels=true`.
  Output matches the prior direct emission byte-for-byte.
- [x] Two new `phase_summary` tests:
  `show_labels_inserts_iter_tuple_between_name_and_dur`
  and `default_omits_labels_even_when_present`.

#### Push 9f — Cleanup partials  *(closes G14, G15)*  ✅

- [x] `phase_status` Labeled and Expanded renders now
  prefer `ctx.eta_secs()`; fall back to the inline
  derivation only when the context returns `None`
  (so existing test contexts that haven't been
  updated keep working).
- [x] `InlineRefreshContext` overrides `eta_secs()`
  with `(cycles_total - ops_finished) / rate`,
  returning `None` when extent is unknown or no
  progress has been made.
- [x] Parser accepts `key:value` colon-shorthand for
  the four structural keys (`lod` / `layout` /
  `color` / `style`). Other keys keep `=` so the
  per-readout primary-arg shorthand
  (`metric:recall*`) stays unambiguous. New
  helper `is_structural_key()` gates the lexer's
  acceptance.
- [x] 5 new parse tests:
  `parses_lod_colon_shorthand`,
  `parses_layout_colon_shorthand`,
  `parses_color_colon_shorthand`,
  `parses_mixed_colon_and_equals_options`,
  `non_structural_colon_is_primary_arg_not_option`.

#### Push 9 not scheduled

- G13 (pre-map "not reached" warning) — different
  format from `phase_summary`; small surface; defer
  unless it grows.
- G17 (`phase_done` Compact / Expanded) — currently
  zero-byte; no demand. Add when a workload wants
  `phase_done` at non-Labeled LOD.

### How this status section stays current

When each Push 9 sub-push lands:
1. Flip its row in the **Audit-driven gap status** table
   from `⏳ planned` → `✅ closed` (or `🟡 partial` if
   only some gaps in the row resolved).
2. Update the **Push 9 — Title** entry in **Progress
   checklist** to reflect overall progress (e.g.
   `🚧 in progress (3/6 sub-pushes done)`).
3. Move that sub-push's task list under **completed
   tasks** following the Push 1–8 convention.

### Deviations from the original plan

- **Push 1**: dropped the separate `nbrs-readouts` crate;
  the engine lives as `nbrs-activity::readouts`. Rationale:
  the trait surface needs `ActivityMetrics` data sources
  later, so a separate crate would either depend on
  `nbrs-activity` (defeating the split) or pull data
  types up. The conceptual boundary is preserved without
  the build-graph cost.
- **Push 2**: added the `trace` diagnostic readout —
  bound to all events in tests, dumps every relevant
  `ReadoutContext` field. Doubles as the smallest
  reference implementation for custom-renderer authors.
- **Push 3 / 4 (planned)**: Push 3 will implement the
  composition vs. override semantics from SRD-63 §5.4.1
  (list-canonical, replace-by-default, `+`-prefix
  append, silent-override warning); previously this was
  underspecified.
- **`ReadoutBinder` (SRD-63 §7) introduced post-Push-1.**
  The runtime adapter trait that owns interactive state
  (focus, LOD overrides, overlay-held flag) and applies
  the layout-composition rules. Lands across multiple
  pushes:
  - **Push 3** introduces the `ReadoutBinder` /
    `ReadoutSink` traits and a stateless default impl
    that drives event firing and resolves bindings.
    Required for the workload `readouts:` block to
    actually run.
  - **Push 5** (TUI surfaces) introduces a stateful
    `TuiReadoutBinder` with focus / LOD overrides /
    keyboard event handling, plus the layout
    composition rules (compact composes, multi-line
    blocks).
  - **Push 7** (explanation overlays) hooks the
    held-key `OverlayHeld` flag through the binder.

### Deferred / known issues

- **TUI mode toggle leaves a history gap.** When
  toggling `tui=on → tui=terminal` after running in TUI
  mode for a while, the output skip-jumps with a span
  of rendered history missing. The terminal sink
  doesn't replay events that fired while the TUI was
  active. Sample of the cliff:

  ```
  ✓ [397/546] [ann_query] (… (50.50s)
    recall_at_100: mean=99.78% …
  · k=100, limit=400
    ⠙ ⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶ [524/546] pvs_query (k=10, limit=50) 97% …
  ```

  Note the leap from `[397/546]` to `[524/546]` —
  every readout fire in between went only to the TUI's
  own surface. The fix likely belongs alongside the
  Push 6 snapshot store: when the surface swaps, the
  new surface replays the relevant snapshot history
  (or at least a "you missed N events" marker) so the
  visible scrollback stays continuous. Park until then.

---

## Pre-flight

### Crate placement

A new crate **`nbrs-readouts`** at the workspace root:

```
nbrs-readouts/
├── Cargo.toml
└── src/
    ├── lib.rs                  # public surface
    ├── readout.rs              # Readout trait + Lod + ContentMode
    ├── context.rs              # ReadoutContext trait + LiveMetricSource
    ├── buf.rs                  # ReadoutBuf abstraction
    ├── registry.rs             # name → Readout lookup
    ├── parse.rs                # body grammar (unbraced, §5.1)
    ├── bake.rs                 # AST → BakedReadout (Vec<RenderStep>)
    ├── render.rs               # render-step walker
    ├── glob.rs                 # patterns for `metric:`, wildcard slots
    ├── color.rs                # palette resolution, style→color mapping
    └── builtins/               # built-in readout impls
        ├── phase_done.rs
        ├── phase_status.rs
        ├── scope_header.rs
        └── …
```

Rationale: keep it out of `nbrs-activity` so the trait
doesn't pull in adapter / executor types, and out of
`nbrs-tui` so terminal-mode and TUI both use the same
crate. Depends on `nbrs-metrics` (for `LiveMetricSource`
adapters) and `nbrs-activity` (for the `ScopeCoord` /
`PhaseState` types it surfaces). `nbrs-tui` and
`nbrs-activity::activity` depend on `nbrs-readouts`, not
the other way around.

### Cross-cutting invariants

These hold in every push from Push 1 onward:

- **No allocation per render** beyond what the output
  buffer requires. Enforced via `cargo bench` baseline
  in Push 1; subsequent pushes preserve it.
- **No silent omissions.** Unknown readout names,
  unknown options, unknown event slots are
  load-time errors (per `feedback_never_ignore_silently`).
- **LOD monotonicity** (§3.3) is a code-review contract
  on each readout impl, not a runtime check.
- **Byte equivalence with current output** until Push 4,
  when explanation overlays + new built-ins justify
  visible diffs.

### Tests common to every push

Every new readout / event / option ships with:

- Unit test in the readout's own module.
- Property test where applicable (golden output for
  every LOD × ContentMode × representative context).
- Integration test in the surface that drives it
  (terminal mode → `nbrs/tests/`, TUI → existing
  `nbrs-tui` test harnesses).

---

## Push 1 — trait, registry, `phase_done` at `on_phase_end`

**Goal:** stand up `nbrs-readouts` end-to-end with one
built-in, replacing today's hard-coded ✓ DONE line.
Inline-progress thread untouched — still its own
formatter for now.

### In scope

- New crate `nbrs-readouts` per Pre-flight §"Crate
  placement".
- `Readout` trait, `Lod`, `ContentMode`, `ReadoutOptions`,
  `ReadoutBuf`, `ReadoutContext` (the §2 surface).
- Registry: `ReadoutRegistry::lookup(name) -> Option<&dyn
  Readout>`. Static set, populated at crate init.
- One built-in: **`phase_done`** at `Lod::Labeled,
  ContentMode::Value`. Other LODs / overlay stub-rendered
  for now (`todo!()`-equivalent that returns empty).
- `BakedReadout` (post-bake step list) and the renderer
  walker. Parse + bake supported but only one body shape
  exercised: a literal one-readout body.
- `ActivityReadoutContext` — the only `ReadoutContext`
  impl in this push, built per-phase from `ActivityConfig`
  + `ActivityMetrics` + `scene_tree::current()`.
- Wire `nbrs-activity::activity`'s end-of-activity ✓ DONE
  emit through `phase_done`. Existing format string
  removed.

### Out of scope

- Inline-progress thread (Push 2).
- Workload `readouts:` block parser (Push 3).
- TUI surfaces (Push 5).
- Snapshots / retention (Push 6).
- Explanation overlays (Push 7).
- Color / style sub-language beyond what the existing ✓
  line uses (just bold / dim / blue / yellow / green).

### Files touched

- **New:** `nbrs-readouts/{Cargo.toml,src/*}` (per
  Pre-flight).
- **New:** `nbrs-activity/src/readout_context.rs` —
  `ActivityReadoutContext` impl.
- **Modified:** `nbrs-activity/src/activity.rs` — the
  ✓ DONE block at end-of-activity. Replace `crate::diag!(…
  format string…)` with a readout invocation against the
  active surface.
- **Modified:** `nbrs-activity/src/lib.rs`,
  `nbrs-activity/Cargo.toml` — depend on
  `nbrs-readouts`.
- **Modified:** `Cargo.toml` (workspace) — register the
  new crate.

### New surface (sketch)

```rust
// nbrs-readouts/src/readout.rs
pub trait Readout: Send + Sync {
    fn name(&self) -> &'static str;
    fn render(
        &self,
        ctx: &dyn ReadoutContext,
        lod: Lod,
        mode: ContentMode,
        opts: &ReadoutOptions,
        out: &mut ReadoutBuf,
    ) -> usize;
}

#[derive(Copy, Clone, Debug)]
pub enum Lod { Compact, Labeled, Expanded }

#[derive(Copy, Clone, Debug)]
pub enum ContentMode { Value, Explanation }

#[derive(Default, Clone)]
pub struct ReadoutOptions {
    pub kv: smallvec::SmallVec<[(SmolStr, OptionValue); 4]>,
}

pub enum OptionValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(SmolStr),
    LodLevel(Lod),
}
```

### Tests

- `nbrs-readouts` lib: golden output for `phase_done @
  Labeled @ Value` against several context shapes
  (zero errors, with errors, with retries, with relevancy
  metrics, no metrics).
- `nbrs/tests/workload_examples.rs`: assert the new ✓
  line still matches the format that already passes.
- `cargo bench --bench readouts_render` (new): record a
  baseline of `phase_done` render time. Subsequent pushes
  keep within +/-10% of this number.

### Risks / fallbacks

- **Output drift.** Byte-equivalence with today's ✓ line
  is the gating criterion for merge. The output is
  short and well-tested; mismatch shows up in
  `concurrent_scheduler` + `workload_examples`.
- **`ReadoutContext` field count.** The §2 trait is
  wide. Push 1 implements *only* the methods
  `phase_done` actually calls; the rest are stubs that
  panic with a clear message. Stubs get filled as later
  built-ins need them.

### Definition of done

- New crate compiles and has > 0 tests passing.
- The ✓ DONE line is rendered through the engine in
  every nbrs invocation. No code path still calls the
  legacy `crate::diag!("…✓…")`.
- Workload examples + scheduler tests green, byte-for-byte
  match.
- Bench baseline recorded.

---

## Push 2 — `on_update` event + `phase_status`, retire inline thread

**Goal:** replace the inline-progress rewriter
(`\r\x1b[K…`) in `nbrs-activity::activity` with a
refresh-event-driven `phase_status` readout.

### In scope

- `Event` enum on `ReadoutContext`. Push 1 implementations
  hard-code `Event::PhaseEnd`; Push 2 starts setting it
  per call.
- New built-in: **`phase_status`** at `Lod::{Compact,
  Labeled}, ContentMode::Value`. Replicates today's
  inline line (spinner + bar + counters + adapter
  status + ETA) at `Lod::Labeled` so byte-equivalence
  holds.
- New built-in: **`trace`** — a minimal diagnostic
  readout that surfaces every `ReadoutContext` field as
  text for whatever event triggered it. Two purposes:
  - Test affordance: bind it to events in unit tests to
    assert that events fire with the expected context.
  - Reference implementation: the smallest possible
    custom renderer, demonstrating the minimum surface
    a third-party readout author has to consume. Lands
    in Push 2 even though full wildcard-binding from
    workloads (`'*': trace`) doesn't arrive until Push 4.
- New refresh-event firing path: a small actor in
  `nbrs-activity::activity` that ticks at the surface's
  cadence (0.5 s for terminal mode), builds a fresh
  `ActivityReadoutContext`, and asks `phase_status` to
  render. Replaces the existing rewriter thread.
- The synthesized "final on_update tick at `_end`"
  guarantee (§6.2) — the actor fires one last render at
  phase_end before shutting down.
- `\r\x1b[K` clear-line behavior preserved at the surface
  layer (sink concern), not the readout's concern.

### Out of scope

- Spinner + bar rendering (deferred to Push 5 with
  `Lod::Expanded`).
- TUI surface — terminal-mode only.

### Files touched

- **New:** `nbrs-readouts/src/builtins/phase_status.rs`.
- **Modified:** `nbrs-activity/src/activity.rs` — replace
  the `std::thread::spawn` rewriter loop with a new
  `RefreshActor` (or fold into the existing
  progress-flag plumbing). Keep the `is_stderr_tty`
  gate as a surface-level concern.
- **Modified:**
  `nbrs-activity/src/readout_context.rs` — `event()`
  method, called distinct values per fire.

### Tests

- Golden output for `phase_status @ Compact` and
  `phase_status @ Labeled` against representative
  contexts.
- Integration: a test that runs a workload and checks
  the inline rewriter still appears (it's the surface
  that emits the `\r`, not the readout) and that the
  final tick is rendered after `_end`.
- `nbrs/tests/concurrent_scheduler.rs` continues to
  pass — the `concurrent dispatch` line is upstream of
  readouts, unaffected.

### Risks / fallbacks

- **Cadence drift.** The refresh actor must fire at the
  same cadence the existing thread used or live status
  feels different. Use the same `Duration::from_millis(500)`
  constant; bench under load.
- **Final-tick correctness.** §6.2 requires a final
  on_update render at `_end`. Easy to miss if the
  refresh actor is shut down before the activity's
  finalize block runs. Sequence: `phase_end` triggers
  (1) refresh actor fires final tick, (2) activity calls
  `phase_done` at `on_phase_end`, (3) actor exits.

### Definition of done

- Inline progress emits via the engine; no
  `format!("{spinner}{bar}…ETA…")` literal remains.
- Visible output unchanged (byte-equivalent): the same
  fields, same units, same rate at which the line
  refreshes.
- Final-tick guarantee covered by a test that asserts a
  `phase_status` render fires after `phase_end` for any
  phase with a non-trivial duration.

---

## Push 3 — workload `readouts:` block (Forms A / B / C)

**Goal:** wire the workload-level configuration surface so
operators can pick built-ins by name and define custom
inline readouts.

### In scope

- `nbrs-workload/src/parse.rs`: parse `readouts:` per
  §5.0 — three legal forms (scalar, mapping, mapping with
  named custom readouts).
- `nbrs-workload/src/model.rs`: new `ReadoutsConfig` on
  `Workload` and `WorkloadPhase` (workload-only for now;
  per-phase deferred per §11 decision 2).
- Plumb the resolved `Vec<(EventSlot, BakedReadout)>` from
  `Workload` through `ActivityConfig` into
  `nbrs-activity::activity` and `nbrs-activity::executor`.
- The full **body grammar** from §5.1 (unbraced, items as
  whitespace-separated calls / quoted literals / color
  directives / shorthand `name:arg`).
- The `metric:pattern` resolution path (parameterized
  readout backed by `LiveMetricSource`).
- The `status_metrics:` continuity layer (§5.4) — when
  a workload declares `status_metrics:` and no
  `readouts:` body uses `metric:status_metrics`, inject
  the equivalent into the relevant built-in's options.

### Out of scope

- Color / style sub-language depth (Push 4).
- LOD `lod:` modifier inside readout bodies (works, but
  doesn't change behavior until Push 5 because all
  current built-ins render same-ish across LODs).
- Wildcard event slot binding (`each_*`, `*`) — Push 4.

### Files touched

- **New:** `nbrs-readouts/src/{parse,bake,glob}.rs` (the
  full body grammar parser + baker; Push 1 had stubs for
  literal one-readout bodies).
- **Modified:** `nbrs-workload/src/{parse,model}.rs`.
- **Modified:** `nbrs-activity/src/{activity,executor,
  runner}.rs` — accept the resolved bindings on
  `ActivityConfig`.
- **Workload yamls:** `adapters/cql/workloads/full_cql_vector.yaml`
  gets a `readouts:` block exercising at least Form B
  + a custom-named readout (Form C). Used as a
  documentation example.

### Tests

- `nbrs-workload`: parser unit tests for each of the
  three forms, plus error-path tests (unknown event
  slot, unknown readout name, unknown option,
  malformed body).
- `nbrs-readouts/src/parse.rs`: round-trip tests
  (parse → bake → render → known output).
- Integration: a workload that uses Form B to override
  `on_phase_end` produces the override; one that uses
  Form A produces the default ✓ line plus a custom
  `phase_status` body.

### Risks / fallbacks

- **Grammar bikeshed.** §5.1's grammar drops braces; the
  whitespace-separated item parser is small but has
  edge cases (quoted literals next to readout calls,
  `name:arg` collisions with literal colons, etc.).
  Solution: a dedicated lexer + parser module with a
  fuzz test (cargo-fuzz harness); 100k random bodies,
  no panics, no infinite loops.
- **Color directives in this push.** Keep the lexer
  forward-compatible with `@COLOR` and `[#hex]` even
  though Push 4 implements the resolution. Parser
  emits a placeholder `ColorDirective(span)` step;
  Push 4 hooks rendering.

### Definition of done

- Workloads can declare `readouts:` and the engine picks
  them up. No more hard-coded built-in selection.
- `status_metrics:` continues to work (regression: the
  `full_cql_vector.yaml` recall + latency display
  unchanged).
- Fuzz test passes 100k iterations without panic.

---

## Push 4 — color / style sub-language + wildcard bindings

**Goal:** uniform color and style surface (§5.2) and
wildcard event-slot binding (§4.1.1).

### In scope

- Full §5.2 color/style implementation:
  - `@RED` / `@INFO` / `@WONG.error` directives in bodies.
  - `[#aabbcc]` and `@#aabbcc` hex forms.
  - `color=` and `style=` per-readout options.
  - Style-name → palette resolution (`ERROR`, `INFO`,
    etc.) with the active palette, defaulting to
    `WONG`. Reuses `nbrs-metrics::reporters::summary`'s
    palette plumbing where it overlaps.
- Wildcard event slot bindings: `each_*`, `phase_*`,
  `*`. Resolution at workload-load: glob expands to
  the matching set of slots, each gets the readout
  added in declaration order (per §5.5).
- `Event` field on `ReadoutContext` is wired everywhere
  (Push 1's stub becomes the per-event values that
  wildcard-bound readouts can branch on).

### Out of scope

- TUI surface integration of styled output (handled in
  Push 5 — the TUI's `Vec<Span>` writer needs the
  resolved styles).

### Files touched

- **New:** `nbrs-readouts/src/color.rs`.
- **Modified:** `nbrs-readouts/src/{parse,bake,render}.rs`
  to handle directives + per-readout options.
- **Modified:** `nbrs-workload/src/parse.rs` — wildcard
  expansion at workload-load.

### Tests

- Unit: `glob_match` for slot patterns, palette
  resolution, hex parsing.
- Integration: a workload declaring `each_*: scope_bracket`
  produces the readout at both `_start` and `_end` events
  with the `Event` correctly set in context.
- ANSI output snapshot tests for each style name + palette.

### Risks / fallbacks

- **Palette divergence.** SRD-46 already uses palette
  names (`WONG` etc.) for plot rendering. Same registry
  must back style-name resolution here, or readouts
  will look subtly different from plot legends.
  Approach: extract the palette table from
  `nbrs-metrics`'s plot module into a shared module
  used by both.

### Definition of done

- A workload using `style=ERROR` / `@WONG.warn` /
  `[#7AC166]` renders correctly in terminal mode.
- Wildcard-bound readouts fire at every matching slot;
  the binding cross-matrix in §5.5 is reproducible.

---

## Push 5 — TUI surfaces + `Lod::Expanded` content

**Goal:** wire the readout engine through every TUI
emit site, with `expanded` LOD for the active-phase
detail block (sparkline, latency bar, full counter
table).

### In scope

- `nbrs-tui::widgets`: a `ReadoutBuf` impl backed by
  `Vec<ratatui::Span>`. Styled spans produced by the
  color/style layer (Push 4) translate to `ratatui::Style`.
- `nbrs-tui::app::draw_active_phase_panel`: header line +
  detail block both go through readouts. The detail
  block uses `phase_status @ Lod::Expanded`.
- `nbrs-tui::app::draw_tree`: per-row format goes through
  readouts at `Lod::Compact` (Default LOD) /
  `Lod::Labeled` (expanded rows).
- New built-ins: **`progress_bar`**, **`latency_block`**,
  **`adapter_counters`**, **`throughput_sparkline`**.
  All ship with `Compact`, `Labeled`, `Expanded`
  content for `Value` mode; `Explanation` mode is
  stubbed.
- `nbrs-tui::log_only_observer::phase_starting` —
  the scope-ancestor walker becomes a `scope_header`
  readout bound to `on_each_start`.

### Out of scope

- Explanation overlay rendering (Push 7).
- Snapshot retention (Push 6).
- Inspector / web sinks (Push 8).

### Files touched

- **New:** `nbrs-readouts/src/builtins/{progress_bar,
  latency_block,adapter_counters,throughput_sparkline}.rs`.
- **Modified:** `nbrs-tui/src/app.rs` (panel + tree),
  `nbrs-tui/src/log_only_observer.rs` (scope walker),
  `nbrs-tui/src/widgets.rs` (the `Vec<Span>` buf impl).

### Tests

- Per-built-in golden output (rendered to a plain
  `String` for snapshot matching).
- TUI golden frame tests for the active-phase panel +
  tree at each LOD setting.

### Risks / fallbacks

- **Sparkline / latency-bar data plumbing.** These read
  from `ThroughputWindow` / `LatencyWindow` on
  `ReadoutContext`; existing TUI code reads them
  directly from `ActivePhase`. Ensure the trait surface
  exposes the same data without re-buffering.
- **Per-frame cost.** Sparkline + latency bar are the
  heaviest readouts. Bench against today's TUI to
  confirm ≤ +10% redraw time.

### Definition of done

- Every per-phase line in the TUI is rendered through a
  readout. No remaining `format!("…")` calls in the
  active-phase panel or tree-row formatter.
- TUI redraw time within budget (bench).

---

## Push 6 — snapshots and session retention

**Goal:** every render is persisted (§6) so the user can
scroll back through completed phases and see each
phase's last live status verbatim.

### In scope

- `readout_snapshots` table per §6.4 schema.
  Implementation in `nbrs-metrics-sqlite` (already owns
  the session db).
- A small `SnapshotSink` that wraps every readout call
  in the engine: render once normally, then render again
  to a snapshot buffer (plain + ANSI), upsert into the
  table by primary key.
- TUI scrollback wiring: the active-phase panel and tree
  row, when displaying a *completed* phase, read the
  snapshot's `body_ansi` instead of re-rendering. Live
  phases re-render every frame.
- `nbrs replay` command (minimal): walk the snapshot
  store and write the latest render of each
  `(slot, subject, readout)` to stdout. No interactive
  UI yet.

### Out of scope

- Context-data freeze (§6.3 callout — explicitly
  deferred to a separate SRD).
- LOD-switching from a snapshot (the snapshot stores
  one LOD per `(slot, subject, readout)`; rendering at
  a different LOD requires re-running from
  `metrics.db`).

### Files touched

- **New:** `nbrs-metrics-sqlite/src/readout_snapshots.rs`.
- **Modified:** `nbrs-readouts/src/render.rs` — accept
  an optional snapshot sink.
- **Modified:** `nbrs-tui/src/app.rs` — read snapshot
  for completed phases.
- **New:** `nbrs/src/replay.rs` (the minimal CLI).

### Tests

- Round-trip: render → snapshot → re-read → byte
  equality (both ANSI and plain forms).
- Integration: run a workload, then run `nbrs replay`
  → output matches the live ✓ DONE lines from the
  original run.
- Storage bound: a workload with N phases produces
  bounded snapshot rows (≤ M per phase per slot per
  LOD); regression-tested at 1k phases.

### Risks / fallbacks

- **Double-render cost.** Naively rendering twice
  (once for display, once for snapshot) wastes work.
  Mitigation: the snapshot sink shares the same
  output buffer, then post-processes (ANSI strip for
  the plain-text column). Single-render with two
  outputs.
- **Storage size.** Long-running sessions with many
  phases. Mitigation: §6.4's primary-key upsert keeps
  only the latest render per `(slot, subject,
  readout, lod)`. Add a bench at 1k phases.

### Definition of done

- Every render produces exactly one snapshot row at
  upsert time.
- `nbrs replay` reproduces the run's status output.
- TUI scrollback shows authentic per-phase final lines.

---

## Push 7 — explanation overlays

**Goal:** every built-in ships an `Explanation` mode;
the user toggles overlays at runtime.

### In scope

- For every built-in delivered in Pushes 1–5, fill in
  the `ContentMode::Explanation` branch. Same shape and
  width as `Value`; text describes what each glyph
  means.
- TUI keybinding to toggle overlay (a single
  `Ctrl+?` or similar — keybinding chosen in this push).
- Terminal-mode equivalent: a CLI flag
  `--explain-readouts` produces the overlay forms in
  every render of the run.

### Out of scope

- Per-readout overlay customization from workloads
  (the overlay is the readout author's; workloads can't
  override it). If demand emerges, future revision.

### Files touched

- **Modified:** every `nbrs-readouts/src/builtins/*.rs`.
- **Modified:** `nbrs-tui/src/app.rs` — overlay-state
  flag, keybinding, redraw on toggle.
- **Modified:** `nbrs/src/main.rs` — CLI flag
  parsing.

### Tests

- For each built-in, a golden snapshot of `Value` and
  `Explanation` modes at the same LOD: assert the
  rendered widths match (LOD invariant: overlay shares
  width with value).
- TUI test: toggle overlay, verify the panel's text
  changes but no layout shift.

### Risks / fallbacks

- **Width drift.** Easy to author an overlay that's
  wider than the value at the same LOD. Test enforces
  width parity.
- **Aesthetic load.** Overlays are read material; if
  they're cryptic the feature dies. Each overlay
  goes through review with a "would a new operator
  understand this?" pass.

### Definition of done

- Toggle works in TUI and `--explain-readouts` works in
  terminal mode.
- Width-parity test green for every built-in.

---

## Push 8 — `--readout` CLI override + post-run / inspector / web sinks

**Goal:** complete the surface map — every readout
emit site (§3 of the SRD) goes through the engine.

### In scope

- `--readout=…` CLI flag (§7 of SRD). Accepts a
  built-in name or a literal body. Overrides the
  workload's `on_update` binding for the run.
- Post-run summary list (`nbrs::run::print_summary`):
  the `[ok] [N/total] name duration` line goes through
  a `phase_summary` readout.
- Inspector socket: structured access continues to
  bypass readouts (per §11 decision 3), but the
  inspector's *text* mode can render through the
  engine as a convenience.
- Web report HTML (`nbrs::report_cmd`): use the
  snapshot store (Push 6) to embed final ✓ lines into
  generated reports verbatim.

### Out of scope

- Per-event CLI flags (`--readout-on-each=…`); single
  `--readout` is the documented escape hatch.

### Files touched

- **Modified:** `nbrs/src/{main,run,report_cmd}.rs`.
- **Modified:** `nbrs-tui/src/inspector_*` (text mode
  only).
- **New:** `nbrs-readouts/src/builtins/phase_summary.rs`.

### Tests

- `nbrs-readouts/tests/cli_override.rs`: override via
  `--readout` lands in the resolved bindings; precedence
  over workload yaml.
- Web report: generated HTML contains the snapshot
  ANSI-stripped text for every completed phase.

### Risks / fallbacks

- **Inspector text mode regression.** Existing scripted
  consumers of the inspector text output may rely on
  exact format. Surface-level `--inspector-format=raw`
  flag preserves the legacy path.

### Definition of done

- Every readout emit site listed in SRD §3 is covered.
- The legacy hard-coded format strings are gone (grep
  audit attached to the merge).

---

## Cross-push checks

Run on every merge:

1. **Byte-equivalence regression** until Push 4. After
   Push 4, allow visible diffs only behind a documented
   workload yaml change.
2. **Bench.** Render-time of `phase_done`, `phase_status`,
   sparkline + latency_block within +/-10% of Push 1's
   baseline.
3. **Workload-examples.** All 29+ tests pass.
4. **Concurrent scheduler.** All 3 tests pass (the
   `concurrent dispatch` line marker isn't a readout
   surface, but easy to break by accident).
5. **No `format!("…✓…")` audit.** Each push's "Definition
   of done" includes a grep over the touched paths
   confirming no legacy strings remain.

---

## Cuts and deferrals

These stay out of scope for the entire 8-push plan; each
gets a follow-on SRD if/when the need surfaces:

- **Per-phase readout binding** (§11 decision 2). Plumbing
  is straightforward; SRD-63 doesn't presume it.
- **Context-data freeze for diagnostics** (§6.3 callout).
  Sibling to the snapshot store, separate decision doc.
- **Replay UI** beyond `nbrs replay` printing snapshots
  to stdout. An interactive scroll/playback UI is a
  separate TUI feature.
- **Operator-side template language extensions** (loops,
  conditionals inside readout bodies). The SRD's
  grammar is intentionally flat; readouts compose at
  the binding level, not via inner control flow.
- **Multiple-palette per-run** (light vs dark per
  surface). Single active palette throughout a run is
  enough for now.
