# 59: Terminal UI — Contract & Axioms (nbrs-tui)

Front door for **nbrs-tui** (layer L5): the ratatui live dashboard and the `TuiObserver`
that drives it from the run's event stream. Pillars 1+2 of the
[Subsystem Treatment Standard](00b_subsystem_standard.md).

## Contract

- **Public surface:** `observer` (`TuiObserver`), `state` (`RunState`), `run_state_actor`,
  the sink seam (`display_sink`, `log_only_sink`, `log_only_observer`,
  `formatted_line_sink`, `sink_supervisor`), and the inspector REPL
  (`inspector_server`, `inspector_repl`, `repl_state`, `key_watcher`).
- **Internal** (`pub(crate)`, compiler-enforced — SRD-05 D5): `widgets`, `frame_broker`,
  `prompt_state`, `readout_panel`, `readout_sink`, `tui_sink`. (`app`, `reporter` stay `pub`
  only for the crate's own integration tests.)
- **Inbound contract:** `nbrs_activity::observer` (the lifecycle callbacks it implements),
  `nbrs_metrics` (the snapshot it folds).
- **Allowed edges:** `nbrs-activity`, `nbrs-metrics`, `polydat`. See
  [SRD 05 §Contract Registry](05_dependency_rules.md).

## Axioms

- **T1 — No screen-buffer for unseen state.** History lives in the log stream /
  `session.log`; a managed screen region holds only the live **active** display and
  re-derives it from source on mode activation, regardless of geometry. [SRD 81].
- **T2 — Display = fold of a typed event stream.** Every surface (scrollback, tree, log,
  panels) projects the readout/lifecycle events; `RunState` is the fold. No surface
  consumes a string rendered for another surface. [SRD 81](81_event_sourced_display.md).
- **T3 — Actor + `ArcSwap`, never shared locks.** Typed commands upstream, `ArcSwap`
  snapshots downstream. No `std::RwLock` for render state (Linux's writer-preferring
  `RwLock` deadlocks on nested reads).
- **T4 — The console belongs to the adapter.** A console-owning adapter on an interactive
  TTY yields the whole console projection; otherwise signals flow through the sink at their
  natural level. [SRD 41 §Output Routing](41_logging.md).

## Mechanism (Pillar 3)
[SRD 62](62_tui_layout.md) (layout), [SRD 63](63_status_readouts.md) (status readouts),
[SRD 81](81_event_sourced_display.md) (event-sourced display projections).

## See also
`nbrs-tui/src/lib.rs`; [SRD 62](62_tui_layout.md); [SRD 63](63_status_readouts.md); [SRD 81](81_event_sourced_display.md).
