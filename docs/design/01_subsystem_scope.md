# Subsystem Scope & Elaboration Plan

This document tracks the three priority subsystems identified for nb-rs and
the plan for elaborating each into full design specifications.

---

## Subsystem 1: Variate Generation

**Status:** Active — design discussion in progress

**Java reference:** `links/nosqlbench/nb-virtdata/` (11 modules)

**Scope:** The deterministic data generation layer that maps cycle numbers to
typed values via composable function pipelines.

**Approach:** New design concepts will drive this subsystem rather than a
direct port of the Java virtdata implementation. The core invariant
(deterministic cycle → value mapping) is preserved, but the architecture,
composition model, and function library will be designed fresh for Rust.

**Next steps:**
- Capture new design concepts for the variate generation model
- Define the composition and dispatch model (traits, generics, type erasure)
- Identify the essential function set for initial release
- Design the binding expression syntax and parsing
- Prototype and iterate

---

## Subsystem 2: Metrics Collection & Processing

**Status:** Pending — awaiting design discussion

**Java reference:** `links/nosqlbench/nb-apis/nb-api/.../engine/metrics/`

**Scope:** Instrument the execution pipeline with labeled timing data, collect
HDR histograms per interval, and fan out to reporters.

**Key decisions to make:**
- Which histogram library to use in Rust (hdrhistogram crate, custom, etc.)
- Label/dimensional model for metric identity
- Snapshot scheduling and cadence aggregation strategy
- Initial reporter set (console, file, remote push)
- Integration points with the engine execution loop

**Next steps:**
- Survey available Rust metrics and histogram crates
- Draft the metric instrument model (timers, counters, gauges)
- Define the reporter trait and initial implementations
- Design the snapshot lifecycle

---

## Subsystem 3: Workload Description Language

**Status:** Pending — awaiting design discussion

**Java reference:** `links/nosqlbench/nb-apis/adapters-api/.../activityconfig/`

**Scope:** The format and processing pipeline for workload definitions —
document structure, op templates, bind-point substitution, parameter
inheritance, tag filtering, and scenario composition.

**Key decisions to make:**
- Whether to retain YAML or adopt a different format
- Inheritance model (document → block → op) — keep, simplify, or extend
- Template expression syntax and bind-point resolution
- Scenario composition model (phases, sequencing, parameterization)
- How the workload spec connects to variate generation and the adapter layer

**Next steps:**
- Review the Java workload format strengths and pain points
- Draft candidate workload format(s) for nb-rs
- Define the parsing and normalization pipeline
- Design the bind-point resolution bridge to variate generation

---

## Elaboration Process

For each subsystem, the design process follows this sequence:

1. **Concept capture** — record new design ideas and constraints
2. **Trade-off discussion** — evaluate alternatives, identify risks
3. **Specification draft** — write the design into the SRD
4. **Review and refine** — iterate on the spec through discussion
5. **Implementation** — build only after the spec is agreed upon

Each subsystem will get its own design document in `docs/design/` as it
moves through elaboration.
