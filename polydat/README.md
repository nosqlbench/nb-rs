## What is Polydat?

Polydat is a variates construction engine.

More specifically, Polydat is all of:

* a function graph grammar
* a procedural generation kernel
* a function library and loader
* a runtime type-safety reifier
* a parameter space projection system
* a context layering API
* an expression language

## Why? It's too many things!

Polydat is, ultimately, the distilled and sharpened variate generation system needed for systems like nb-rs. In fact,
Polydat wasn't designed as a system against a set of new requirements. It evolved as a reduction of nosqlbench's core
features during a rust port. Polydat is what you get when you take the essence of a complex but capable system like
nosqlbench and compress it down with heat and pressure until it is forced to take a minimal, elemental shape.

## Motivation

A lesson learned while evolving nosqlbench steered Polydat to where it is now:

__Managing many testing and simulation parameters can be overwhelming!__

In nosqlbench, users have to manage a variety of parameters in different places with different formats. Polydat's design
categorically avoids this problem, since all the values used in a workload simulation are owned by the same subsystem.

* Workload parameters on the command line -> Polydat
* Template parameters in the workload -> Polydat
* Procedural generation recipes -> Polydat
* Op template fields and bindings -> Polydat
* Dimensional configuration parameters and labels -> Polydat

What this means in practice is that a single set of rules applies everywhere, and since everything is owned by the same
subsystem, you can always ask it exactly where a value comes from or how it is computed.

But this also means something unavoidable about how the many nosqlbench features are realized in nb-rs. In order to make
configuration, parameterization, templating, and all layers of variates work together with no friction, they all have to
be owned and managed by one system.

## Documentation

- [Illustrations](docs/illustrations.md) — runnable examples through the GK DSL and the programmatic Assembler API.
- [Compilation](docs/compilation.md) — Phase 1 / 2 / 3 levels, Hybrid mode, throughput numbers, and the `jit` / `vectordata` Cargo features.
- [Nodes](docs/nodes.md) — the 250+ built-in function nodes.
- [License](docs/license.md) — Apache-2.0; canonical text at the repo-root [LICENSE](../LICENSE).

Part of the [nb-rs](https://github.com/nosqlbench/nb-rs) workspace.
