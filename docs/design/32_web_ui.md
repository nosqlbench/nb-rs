# SRD 32 — Web UI (nb-web)

## Purpose

The web UI provides a browser-based dashboard for monitoring,
controlling, and inspecting nb-rs workloads. It serves as the
graphical counterpart to the terminal TUI (`nb-tui`) and the
CLI (`nbrs describe`).

Built with Axum + Askama + htmx. No JavaScript build step, no SPA
framework, no node_modules. The server renders HTML fragments; htmx
handles partial page updates. WebSockets provide live metric
streaming.

## Core Capabilities

### 1. Live Workload Dashboard

Real-time view of running activities:

- **Summary stats**: total cycles, ops/sec, error count, P50/P99/P999
  latency, rate limiter utilization
- **Per-activity breakdown**: each activity shows its own stats,
  progress bar (cycles completed / target), and status (running,
  paused, completed, errored)
- **Live metric stream**: htmx WebSocket extension receives metric
  frames and updates stats in-place. Same frame-based capture system
  that feeds the TUI (SRD 16/24).
- **Op sequence view**: shows the current stanza pattern with ratios

Update cadence: 1-second metric frames via WebSocket. Dashboard cards
update via htmx `hx-swap-oob` (out-of-band swap) — the server pushes
updated HTML fragments over the WebSocket, htmx swaps them into the
DOM by element ID. No polling.

### 2. GK Function Browser

Interactive version of `nbrs describe gk functions`:

- **Category-grouped table**: all native node functions with
  signature, arity, P123 level, description
- **Category filtering**: click a category to filter the table
- **Search**: type-ahead filter by function name or description
- **Detail panel**: click a function to see its full documentation
  (from the struct doc comments), parameter types, example usage
- **Compile level probing**: live P123 badges probed from node
  instances, same as CLI describe

All filtering and search via htmx — server renders filtered HTML
fragments, no client-side JavaScript.

### 3. Stdlib Module Browser

Interactive version of `nbrs describe gk stdlib`:

- **Module list**: all embedded stdlib modules with typed signatures
- **Source view**: click a module to see its `.gk` source with syntax
  highlighting
- **Category grouping**: from `@category:` annotations

### 4. DAG Viewer

Interactive version of `nbrs describe gk dag`:

- **Source editor**: textarea where you paste or type GK source
- **Render button**: submits source via htmx, server renders the DAG
  and returns an SVG fragment
- **Format selector**: DOT, Mermaid, or SVG output
- **Live preview**: as you type (debounced), the DAG re-renders
- **Example loader**: dropdown of example `.gk` files

Uses `nb_variates::viz::gk_to_svg()` for pure-Rust SVG rendering.
No graphviz installation needed.

### 5. Workload Inspector

Dry-run and inspection tools:

- **Bind point analysis**: paste a workload YAML, see all bind points,
  their sources (GK binding, capture, coordinate), and which ops
  reference them
- **Sample output**: render N sample cycles showing the assembled ops
  with all bind points resolved
- **GK kernel info**: coordinate names, output names, node count,
  compilation level per node
- **Module resolution trace**: show which `.gk` modules were loaded
  and from where (workload dir, stdlib)

### 6. Activity Control (Future)

Start, pause, resume, and stop activities from the browser:

- **Run form**: upload or paste a workload YAML, set parameters
  (cycles, threads, driver, rate), and launch
- **Parameter tuning**: adjust rate limits and thread count on a
  running activity
- **Error inspector**: view recent errors grouped by error class,
  with the error handler chain and disposition

This requires the execution layer to support external control
signals — deferred until the activity lifecycle API is designed.

## Architecture

```
Browser (htmx)
    ↕ HTTP + WebSocket
Axum server (nb-web)
    ├── Static files (htmx.js, CSS)
    ├── Askama templates (HTML fragments)
    ├── API routes (JSON for programmatic access)
    ├── WebSocket handler (metric frame stream)
    └── Shared state:
        ├── Arc<GkRuntime>     — unified function registry
        ├── Arc<ActivityState> — live activity metrics (optional)
        └── Stdlib sources     — for module browser
```

### Embedding vs Standalone

Two modes:

**Embedded**: The web server starts inside `nbrs run` when
`--web` or `--web-port=8080` is passed. The server shares the
same tokio runtime as the activity executor. It has direct access
to the `Arc<ActivityMetrics>` for live streaming.

```
nbrs run workload=w.yaml cycles=1M --web --web-port=8080
```

**Standalone**: `nbrs web` starts the server without running a
workload. Provides the function browser, stdlib browser, DAG
viewer, and workload inspector. No live metrics.

```
nbrs web --port=8080
```

### Route Structure

```
GET  /                    → Dashboard (full page)
GET  /functions           → Function browser (full page)
GET  /stdlib              → Stdlib browser (full page)
GET  /dag                 → DAG viewer (full page)

GET  /api/activities      → Activities table fragment (htmx)
GET  /api/recent-ops      → Recent ops fragment (htmx)
GET  /api/functions?q=..  → Filtered function table fragment (htmx)
GET  /api/stdlib          → Stdlib module list fragment (htmx)
POST /api/dag/render      → Render GK source, return SVG fragment (htmx)
POST /api/inspect         → Inspect workload YAML, return analysis (htmx)

WS   /ws/metrics          → Live metric frame stream (WebSocket)
```

Full-page routes return the complete HTML (base template + content).
API routes return HTML fragments for htmx swap. The same templates
are used — htmx requests get fragments, direct browser requests
get full pages. Detected via the `HX-Request` header.

### Template Strategy

Askama templates with compile-time checking:

```
templates/
  base.html              — Shell: head, nav, main container
  dashboard.html         — Dashboard page (extends base)
  functions.html         — Function browser page
  stdlib.html            — Stdlib browser page
  dag.html               — DAG viewer page
  fragments/
    activities_table.html — Activity rows (htmx fragment)
    function_row.html     — Single function row
    function_detail.html  — Function detail panel
    module_card.html      — Stdlib module card
    dag_result.html       — Rendered DAG SVG container
    stats_cards.html      — Dashboard stat cards (WebSocket OOB)
```

### WebSocket Metric Streaming

The metric frame stream uses htmx's WebSocket extension:

```html
<div hx-ext="ws" ws-connect="/ws/metrics">
    <div id="total-cycles" ws-swap="innerHTML">0</div>
    <div id="ops-per-sec" ws-swap="innerHTML">0</div>
</div>
```

The server sends HTML fragments with `id` attributes matching the
dashboard elements. htmx swaps them in-place via out-of-band swap.
No JSON parsing, no DOM manipulation — just HTML over WebSocket.

The frame source is the same `MetricsFrame` system from SRD 16
that feeds the TUI. The web server subscribes to the same
`mpsc::channel` and converts frames to HTML fragments.

## Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Web framework | Axum | Tokio-native, Tower middleware, htmx crate |
| Templates | Askama | Compile-time checked, zero-alloc rendering |
| Interactivity | htmx | No JS build step, server-rendered fragments |
| Live updates | htmx WS extension | Native htmx, no custom JavaScript |
| Styling | Inline CSS in base.html | No build step, one file, dark theme |
| DAG rendering | layout-rs | Pure Rust SVG, no graphviz needed |

## Non-Goals

- No SPA framework (React, Vue, Svelte)
- No JavaScript build pipeline (webpack, vite, esbuild)
- No client-side state management
- No database for the web UI itself
- No authentication (local tool, not a cloud service)
- No REST API for external consumers (internal htmx only)

## Relationship to Other Components

- **nb-tui (SRD TUI)**: Same metric frame source, different renderer.
  TUI renders to terminal; web renders to HTML.
- **nb-metrics (SRD 16)**: `MetricsFrame` is the shared data model.
  Both TUI and web consume frames from the capture thread.
- **nb-variates (SRD 24/27/30)**: Registry, stdlib, and viz module
  power the function browser, stdlib browser, and DAG viewer.
- **nb-activity (SRD 21)**: Live activity state for the dashboard.
  Shared via `Arc`.
