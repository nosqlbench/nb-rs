# SRD-92 — Readout Layout Contract (header / detail / gutter)

## Problem

The live footer and the completion scrollback grew their layouts renderer-by-
renderer: the margin triad, the completion bar, latency trends, `gutter:`
cells, memo banners, key-metric lines and op leaves each picked their own row
and margin treatment. The result was observably incoherent — a progress bar
beside one phase's *header* while a latency trend sat beside another phase's
*detail* row, a memo banner above the header it belonged to, and a key-metric
summary line carrying a timing triad as if it were a header. Row roles were
*inferred by sniffing rendered strings* (`starts_with("[[")` — defeated by SGR
prefixes), not specified anywhere.

This SRD is the specification. Every display surface (live footer, completion
scrollback, TUI tree) renders visible nodes against ONE block model.

## The block model

Every node enabled for visibility — phase, op, stanza, scenario — renders as a
**block**:

```
<margin: timing triad>  │ <header body: status icon · name · coords · meter>
<margin: gutter cell 1> │     <detail line 1  (standard counters)>
<margin: gutter cell 2> │     <detail line 2  (key metrics)>
<margin: blank>         │     <detail line …  (memo, leaves, …)>
```

### R1 — the header line

Each visible node has exactly ONE header line, always first, with a stable
format. Its left margin is the **timing triad** (`session-time · [n/N] ·
node-time`, per `widgets::margin_body`); its body is the node's identity
(status icon, name, coords, meter slot). Nothing else in the block repeats the
triad (single-placement rule). This holds for every header in a multi-block
region — not just the first footer row: each concurrently-visible phase's
header carries its OWN triad ([n/N] from the node's plan seq, node-time from
its session delta).

Op leaves are visible nodes too. Their gutter cell carries the node's own
EXECUTION DURATION — cumulative while running, final once terminal — the
default cell for any visible node with no declared gutter, forming a
scannable per-step time ledger (live and retained into scrollback). The
body keeps identity plus the terminal session stamp (`✓ name [i/N] @
session`); single placement holds — duration only in the cell, stamp only
in the body.

### R2 — detail lines and the gutter stack

A block has zero or more detail lines under its header. Each detail line owns
one **gutter cell** in the left margin (possibly blank). Cells therefore stack
vertically under the header's triad — the gutter column is per-row, never
shared or floated.

### R3 — the standard detail line

The standard counters line (`rate ok% att% e: r: c: cycles/rows [eta]`) is
always rendered for a running node. Its gutter cell defaults to the node's
**completion indicator** — the braille bar from the single fraction source
(override → rows → cycles) — whenever a viewable cursor/extent exists, or the
latency trend for open-ended nodes. A workload `gutter:` wrapper spec
overrides this cell (`text:` / `bar:` / `spark:`, with `final:` per SRD's
gutter/final rules — final falls back to the last published live value).

### R4 — key metrics

Key metrics (`status_metrics:` chips — recall etc., plus adapter/batch chips)
get their own detail line when present. That line's gutter cell defaults to
the **metric macro**: a compact, bright (key-metric accent `1;95m`) live
summary of the metric — useful both live and as the last-rendered value at
completion. Metric macros are typed (text now; bar/spark/latency-typed macros
join the same slot), and any customization follows the live/final override
rule: `final:` defaults to the live config when unspecified.

### R5 — completion is not a collapse

When a visible node ends, its block is preserved into scrollback in contract
shape: header (triad-stamped at completion), then detail lines with gutter
cells (the `final:` cell — or last live cell — beside the counters line), then
op-leaf lines. How much detail is retained is configurable via the readout
selection param `completed_phases=`:

- `full` (default) — header + detail lines + op leaves;
- `headers` — header line only (details and leaves dropped from scrollback;
  session.log keeps everything unconditionally).

## Row-role classification

The rendered block is still a string (the readout bodies compose it), so role
assignment lives in ONE shared classifier (`status_fold::classify_block`),
which is **SGR-aware** (strips ANSI before matching):

- a row whose stripped text starts with `[[` is a **memo** detail row;
- the first non-memo row is the **header**;
- the first non-memo row after the header is the **standard detail** row;
- remaining non-memo rows are **key-metric** detail rows.

Renderers MUST compose blocks header-first (memo banners render as detail rows
*below* the header, never above it) so positional classification and the
visual contract agree.

## Margin assignment by surface

- **Live footer** (`draw_footer_at_cursor`): per-row `RowGutter` from the fold
  — `Header(body)` renders the colorized triad; `Bar`/`Latency`/`LatencyHist`/
  `Text`/`Spark` render detail cells; `Metric` renders the bright metric
  macro; `Blank` renders the divider-only margin.
- **Scrollback drain**: row 0 of every multi-row entry gets the actor-stamped
  triad; rows ≥ 1 get the blank divider margin, except cells explicitly
  attached (the ✓ block's `final:` cell on the counters row). A multi-row
  entry never repeats the triad.
