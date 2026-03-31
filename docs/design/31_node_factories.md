# SRD 31 — Node Factories and External Node Providers

## Overview

External crates can provide GK node functions by implementing the
`NodeFactory` trait and registering their factory at startup. Once
registered, factory-provided nodes are indistinguishable from
built-in nodes: same registry, same describe output, same category
grouping, same type checking, same compilation levels.

There is no "fallback" or "secondary" lookup path. All factories
contribute their signatures to the unified registry before any
compilation occurs. The compiler, the describe commands, and the
module resolver all see one flat set of available functions.

## NodeFactory Trait

```rust
/// External crate implements this to provide GK node functions.
pub trait NodeFactory: Send + Sync {
    /// Return signatures for all functions this factory provides.
    ///
    /// Called once at registration time. The returned signatures are
    /// merged into the global registry and treated identically to
    /// built-in function signatures.
    fn signatures(&self) -> Vec<FuncSig>;

    /// Build a node by name with the given constant arguments.
    ///
    /// Called by the compiler when assembling a kernel that references
    /// one of this factory's functions.
    fn build(
        &self,
        name: &str,
        wires: &[WireRef],
        consts: &[ConstArg],
    ) -> Result<Box<dyn GkNode>, String>;
}
```

The factory returns full `FuncSig` entries including category,
description, variadic flags — everything. The signatures appear in
`describe gk functions` under their declared category, with compile
level probed from live instances just like built-in nodes.

## Registration

Factories are registered on a `GkRuntime` context before compilation:

```rust
let mut runtime = GkRuntime::new();

// Built-in nodes are always registered
runtime.register_builtins();

// External crate registers its factory
runtime.register_factory(Box::new(vectordata::VectordataFactory::new()));

// Compile — sees all registered nodes as one unified set
let kernel = runtime.compile_gk(source)?;
```

The `GkRuntime` holds:
- The unified `Vec<FuncSig>` registry (built-in + factory-provided)
- The factory instances (for `build()` dispatch)
- Stdlib sources (embedded)
- Module search paths

All compilation goes through the runtime. There is no global mutable
state — each runtime is self-contained. Multiple runtimes can coexist
with different factory sets (useful for testing).

## Build Dispatch

When the compiler needs to construct a node, it checks:

1. Built-in match (the existing `build_node` function)
2. If no built-in match, iterate factories: `factory.build(name, ...)`
3. If no factory match, try module resolution (.gk files, stdlib)
4. If nothing matches, error

Step 1 and 2 are not a priority ordering — they're checked in
sequence because built-in nodes are hardcoded in a match arm (fast)
while factory dispatch requires iterating. But from the user's
perspective, all are first-class. The registry doesn't distinguish
source.

In the future, if built-in nodes are also registered via a factory
(the "builtins factory"), the dispatch becomes fully uniform: just
iterate factories in registration order.

## Example: vectordata Crate

```rust
pub struct VectordataFactory {
    // Initialized at factory construction — loads dataset metadata,
    // opens connections, etc. This happens once at startup.
}

impl VectordataFactory {
    pub fn new() -> Self {
        Self { /* init */ }
    }
}

impl NodeFactory for VectordataFactory {
    fn signatures(&self) -> Vec<FuncSig> {
        vec![
            FuncSig {
                name: "vector_lookup",
                category: FuncCategory::RealData,
                wire_inputs: 2,  // (index, dimension)
                const_params: &[("dataset", true)],
                outputs: 1,
                description: "look up a vector component from a hosted dataset",
                variadic: false,
                identity: None,
                variadic_ctor: None,
            },
            FuncSig {
                name: "vector_dim",
                category: FuncCategory::RealData,
                wire_inputs: 0,
                const_params: &[("dataset", true)],
                outputs: 1,
                description: "dimensionality of a hosted vector dataset",
                variadic: false,
                identity: None,
                variadic_ctor: None,
            },
        ]
    }

    fn build(
        &self,
        name: &str,
        _wires: &[WireRef],
        consts: &[ConstArg],
    ) -> Result<Box<dyn GkNode>, String> {
        let dataset_name = consts.first()
            .map(|c| c.as_str())
            .ok_or("vector_lookup requires a dataset name")?;

        match name {
            "vector_lookup" => {
                let ds = self.load_dataset(dataset_name)?;
                Ok(Box::new(VectorLookup::new(ds)))
            }
            "vector_dim" => {
                let ds = self.load_dataset(dataset_name)?;
                Ok(Box::new(VectorDim::new(ds)))
            }
            _ => Err(format!("unknown function: {name}")),
        }
    }
}
```

The `VectorLookup` node captures `Arc<Dataset>` at construction.
It's `Send + Sync`. It goes into the `GkProgram` node list and is
shared across all fibers. No per-fiber allocation, no synchronization.

## Usage in GK Grammar

```gk
// vectordata functions are first-class — no special syntax
(vector_id, dim) := mixed_radix(cycle, 768, 0)
component := vector_lookup(vector_id, dim, dataset: "glove-100")
```

The user doesn't know or care whether `vector_lookup` is built-in,
from a factory, or from a .gk module. It appears in `describe gk
functions` under "Real Data" with its full signature.

## Fiber Safety

Factory-provided nodes follow the same rules as built-in nodes:

- The node struct is `Send + Sync` (required by `GkNode` trait)
- Init-time data (dataset handles, connections, caches) is captured
  in the struct and shared via the `GkProgram`
- `eval()` is called with `&self` — read-only access to captured data
- Mutable per-fiber state (if needed) goes through interior mutability
  patterns (e.g., `thread_local!` for connection pools) that the
  factory manages internally
- The GK runtime provides no cross-fiber synchronization — if a
  factory's nodes need internal coordination, the factory handles it

## Describe Integration

`describe gk functions` shows all registered functions:

```
── Real Data ──

  first_names                                    1→1   P✓✗✗  Census first name (weighted)
  full_names                                     1→1   P✓✗✗  full name (first + last)
  vector_lookup        (dataset)                 2→1   P✓✗✗  look up a vector component from a hosted dataset
  vector_dim           (dataset)                 0→1   P✓✗✗  dimensionality of a hosted vector dataset
```

Built-in `first_names` and factory-provided `vector_lookup` appear
side by side under the same category. The user sees one unified
function library.

## Future: Built-in Nodes as a Factory

The current built-in nodes are hardcoded in `build_node()` match
arms. A future refactoring could wrap them in a `BuiltinsFactory`
that implements `NodeFactory`, making the dispatch fully uniform.
This is not required for correctness — it's a code organization
improvement.

## Relationship to GK Modules

Node factories provide **native Rust nodes** — compiled code with
full P1/P2/P3 capabilities. GK modules provide **composed subgraphs**
from `.gk` files. They're complementary:

- Factory nodes are fast, can do I/O at init time, can implement
  loops and complex algorithms
- GK modules compose existing nodes (including factory nodes) into
  higher-level patterns

A factory node like `vector_lookup` can be used inside a GK module:

```gk
// vector_embedding.gk
// @category: Real Data
vector_embedding(index: u64, dataset: String) -> (components: f64) := {
    dim := vector_dim(dataset: dataset)
    (vec_id, d) := mixed_radix(index, dim, 0)
    components := vector_lookup(vec_id, d, dataset: dataset)
}
```
