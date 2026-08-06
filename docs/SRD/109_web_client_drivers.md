# SRD-109 — Web-Only Client Drivers on the HTTP Module

Status: DRAFT (design only — nothing on this SRD is implemented).
Builds directly on SRD-108: a web-only client driver is an
SRD-108 implementation module whose op bodies are http op
templates, plus a thin registration layer that gives it
classic-driver ergonomics.

## Problem

Many targets — REST-native vector databases foremost — ship
"client drivers" that are, on the wire, nothing but HTTP
requests. Reaching them today means either writing a native Rust
adapter per vendor (the per-vendor cost the VDBBench gap analysis
identified as the blocking adapter-reach gap) or hand-writing raw
http workloads per benchmark. Neither scales, and a native
adapter for a plain-HTTP protocol also *hides* the literal
request from the operator.

The op-authenticity principle decides the shape: for a web-only
client, the authentic native usage IS the HTTP request. A driver
for such a client should therefore surface literal requests —
never invent harness-side structured protocol fields (the C8
ruling) — and cost zero Rust per vendor.

## Part 1 — the driver library (an SRD-108 implementation module)

A vendor driver is a bundled implementation workload:

```yaml
# drivers/vendorx/vector_impl.yaml
implements: vector_suite_logical

params:
  base_url: "http://localhost:6333"
  api_key: ""

phases:
  load_train:
    ops:
      insert:
        method: PUT
        uri: "{base_url}/collections/{table}/points"
        headers:
          api-key: "{api_key}"
        body: |
          {"points":[{"id":"{id}","vector":{train_vector}}]}
  serial_probe:
    ops:
      select_ann:
        method: POST
        uri: "{base_url}/collections/{table}/points/search"
        body: |
          {"vector":{query_vector},"limit":{suite_limit},"with_payload":true}
        captures: ...   # response keys via JSON-pointer capture paths
```

Everything SRD-108 established applies unchanged: the logical
suite owns scaffolding and relevancy; binding is load-time;
interface types prove at pre-map synthesis; SRD-107 provenance
covers the request shapes, so switching vendors re-runs
idempotent prereqs correctly.

## Part 2 — the driver manifest (registration + defaults)

A small manifest gives library drivers the ergonomics of built-in
adapters without any new op vocabulary:

```yaml
# drivers/vendorx/driver.yaml
driver: vendorx
adapter: http
library: drivers/vendorx/vector_impl.yaml
defaults:
  params:
    base_url: "http://localhost:6333"
  errors: "429:retry,backoff; 5..:count,retry; .*:count"
description: VendorX REST vector client (native HTTP op forms)
```

- `driver=vendorx` on the CLI (or `driver: vendorx` in a
  workload) resolves through the manifest: http adapter + the
  implementation library as the default `impl=` + the declared
  defaults (overridable by ordinary params).
- `nbrs describe drivers` lists manifests (bundled + local),
  mirroring `describe workloads`.
- The manifest maps NAMES to templates and defaults — it never
  defines op fields. The op surface remains exactly the http
  module's (`method`/`uri`/`body`/`headers`), so every request
  stays literal and operator-visible.

## Resolved-by-design questions

- **Auth flows** (login → token → refresh): expressed as ordinary
  ops — a prereq phase captures the token into a shared wire;
  subsequent ops reference it in headers. A named test case, not
  new machinery. Token EXPIRY mid-run is v2 (a poll/refresh
  daemon op is expressible today; codify the pattern when a real
  vendor needs it).
- **Result-shape contracts**: SRD-108 left probe yields empty
  because relevancy traverses result columns. For HTTP drivers
  the result body is vendor JSON, so the implementation MUST
  normalize: capture the neighbor keys via JSON-pointer capture
  paths into the wires/columns the logical evaluations read. A
  typed result-shape interface (declaring the traversal path and
  row shape, checked at load) is the open design item this SRD
  must settle before implementation — Option A: extend
  `abstract:` with a `results:` section (name → JSON-pointer +
  type) that compiles to capture declarations; Option B: keep it
  conventional and document per-slot. Option A is favored: it
  makes the last implicit leg of the logical/implementation
  contract explicit and load-checkable.

## Non-goals

- gRPC / custom-binary vendors: real adapters, out of scope here.
- Multi-vendor comparison orchestration (running N drivers in one
  invocation): composes at the session/report layer, not here.
- Any harness-side structured protocol vocabulary (C8 stands).

## Deliverables (when implementation starts)

1. Driver manifest model + resolution (`driver=` param, manifest
   discovery local-first then bundled, defaults overlay) +
   `describe drivers`.
2. The `results:` interface extension (Option A above) compiling
   to capture paths, with load + synthesis checks per SRD-108's
   table.
3. First vendor library implementing `vector_suite_logical`,
   with an auth-flow prereq phase as the named test case.
4. e2e: the logical suite bound to the vendor library against a
   mock http endpoint (testkit-style), plus walker examples.
