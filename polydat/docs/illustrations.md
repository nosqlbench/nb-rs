# Polydat Illustrations

It may actually be easier to show what polydat can do than to explain
what it is or how it actually works.

Polydat transforms named `u64` coordinate tuples into typed output
variates via a compiled DAG of composable function nodes. The same
coordinate always produces the same outputs — deterministic,
reproducible, and parallelizable with zero shared mutable state.

The two entry points below are the two surfaces polydat presents to a
caller: the GK DSL (a small expression grammar you write as a string)
and the Assembler API (programmatic node-by-node construction). They
produce the same kind of compiled kernel; pick the one that fits your
call-site shape.

Every code block in this file is the body of a runnable example under
[`polydat/examples/`](../examples/). They all compile under
`cargo build --examples` and produce the printed outputs you see
quoted below. If a snippet ever drifts from what the example actually
does, the example is the source of truth.

## From the GK DSL

```rust
use polydat::dsl::compile_gk;

let mut kernel = compile_gk(r#"
    input cycle: u64
    hashed := hash(cycle)
    user_id := mod(hashed, 1000000)
"#).unwrap();

kernel.set_inputs(&[42]);
let user_id = kernel.pull("user_id").as_u64();
assert!(user_id < 1_000_000);
```

See [`examples/basic.rs`](../examples/basic.rs) for the full runnable form.

## From the Assembler API

```rust
use polydat::assembly::{GkAssembler, WireRef};
use polydat::nodes::hash::Hash64;
use polydat::nodes::arithmetic::ModU64;

let mut asm = GkAssembler::new(vec!["cycle".into()]);
asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
asm.add_node("user_id", Box::new(ModU64::new(1_000_000)), vec![WireRef::node("hashed")]);
asm.add_output("user_id", WireRef::node("user_id"));

let mut kernel = asm.compile().unwrap();
kernel.set_inputs(&[42]);
assert!(kernel.pull("user_id").as_u64() < 1_000_000);
```

See [`examples/assembler.rs`](../examples/assembler.rs).

---

The README claims polydat is seven things at once. Each section below
shows one of those facets in a small, runnable form so the claim isn't
just an assertion.

## A function graph grammar

GK source is a literal grammar for declaring function graphs. Each
`name := expr` line is a graph node; the expressions reference other
nodes by name, forming directed edges. The compiler topologically
sorts the result and refuses cycles.

```rust
let mut kernel = polydat::dsl::compile_gk(r#"
    input cycle: u64

    // Decompose one coordinate into two dimensions (device, reading).
    (device, reading) := mixed_radix(cycle, 100, 0)

    // Derive an identity hash and split it into independent streams.
    device_h := hash(device)
    h_temp := hash(device_h)
    h_humid := hash(h_temp)

    // Convert each independent stream into a unit-interval quantile.
    q_temp := unit_interval(h_temp)
    q_humid := unit_interval(h_humid)
"#).expect("compile failed");

kernel.set_inputs(&[12_345]);
// device=45, reading=123, q_temp=0.071753, q_humid=0.452280
```

The grammar fits on one page (see [`src/dsl/`](../src/dsl/)) but the
graphs you can build are arbitrarily wide and deep. The compiler
tracks each wire's port type (u64, f64, str, bool, bytes, json,
vectors…) and rejects mismatches at compile time, before the kernel
ever runs.

Note: GK numeric literals don't accept Rust-style underscores —
`1000000` is fine, `1_000_000` is a parse error.

See [`examples/function_graph_grammar.rs`](../examples/function_graph_grammar.rs).

## A procedural generation kernel

A compiled kernel is a pure function of its coordinate inputs. Same
coordinate in, same outputs out, every time, no shared state. That
property is what lets a multi-thread benchmark generate billions of
distinct, reproducible variates in parallel — each thread gets its
own state, the program is shared via `Arc`.

```rust
let kernel = polydat::dsl::compile_gk(r#"
    input cycle: u64
    user_id := mod(hash(cycle), 1000000)
"#).unwrap();

let program = kernel.into_program();

// Two threads, no locks, identical results for the same coord.
let results: Vec<u64> = std::thread::scope(|s| {
    let handles: Vec<_> = (0..2).map(|_| {
        let program = program.clone();
        s.spawn(move || {
            let mut state = program.create_state();
            state.set_inputs(&[42]);
            state.pull(&program, "user_id").as_u64()
        })
    }).collect();
    handles.into_iter().map(|h| h.join().unwrap()).collect()
});
// All threads produce the same value (915720 for coord=42).
```

See [`examples/generation_kernel.rs`](../examples/generation_kernel.rs)
and [`examples/multi_thread.rs`](../examples/multi_thread.rs) (the
larger benchmark form).

## A function library and loader

The 230 built-in nodes (see [nodes.md](nodes.md)) are one library.
Workload-author functions written in `.gk` files are another —
`compile_gk_with_libs` loads them from disk and they're callable from
your DSL by name as if they were built in.

```rust
let stdlib = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("stdlib").join("identity.gk");

let mut kernel = polydat::dsl::compile_gk_with_libs(
    r#"
        input cycle: u64
        // `hashed_id` is loaded from identity.gk, not a built-in.
        uid := hashed_id(cycle, 1000000)
    "#,
    None,                 // source_dir — no implicit ./*.gk lookup
    vec![stdlib],         // explicit library paths
    &[],                  // required_outputs — compile everything
    false,                // strict
    "library example",
).expect("compile failed");
```

The shipped [`stdlib/`](../stdlib/) directory has more examples:
`identity.gk`, `distributions.gk`, `hashing.gk`, `modeling.gk`, etc.
Each is loadable the same way.

See [`examples/library_and_loader.rs`](../examples/library_and_loader.rs).

## A runtime type-safety reifier

Polydat's `Value` enum reifies the port-type lattice at the runtime
boundary. The compiler enforces port-type contracts when wiring nodes
(e.g., `mod` accepts two u64 inputs, returns u64); the runtime carries
the typed payload through to the consumer, who reads it through a
typed accessor.

```rust
use polydat::node::Value;

let mut kernel = polydat::dsl::compile_gk(r#"
    input cycle: u64
    n := mod(hash(cycle), 1000)             // u64
    p := unit_interval(hash(cycle))         // f64 in [0.0, 1.0)
    label := number_to_words(n)             // Str
    is_big := u64_gt(n, 500)                // u64 (0 or 1)
"#).unwrap();

kernel.set_inputs(&[42]);
match kernel.pull("n")      { Value::U64(_) => {}, _ => unreachable!() }
match kernel.pull("p")      { Value::F64(_) => {}, _ => unreachable!() }
match kernel.pull("label")  { Value::Str(_) => {}, _ => unreachable!() }
match kernel.pull("is_big") { Value::U64(_) => {}, _ => unreachable!() }
```

Connecting incompatible port types — e.g., feeding the u64 result of
`mod(...)` directly into `sin(...)` which expects f64 — is a compile
error, not a silent coercion. The type discipline survives through
Phase 3 JIT codegen: each Cranelift signature mirrors the port-type
shape, so a typed mistake can't survive into the generated native
code.

Note: polydat's comparison family (`u64_gt`, `u64_lt`, `f64_eq`, …)
returns `u64` (0 or 1) rather than `Bool`. The `Bool` variant of
`Value` exists and is used by `regex_match`, `control_bool`, and a
few others, but most comparison-driven workloads stay in u64 for
fast-path math.

See [`examples/type_safety.rs`](../examples/type_safety.rs).

## A parameter space projection system

A single integer coordinate (`cycle`) is a 1-D parameter space.
`mixed_radix` projects it onto an N-D space — useful when you want
"100 devices × N readings" but only have one cycle counter to
drive everything.

```rust
let mut kernel = polydat::dsl::compile_gk(r#"
    input cycle: u64
    (device, reading) := mixed_radix(cycle, 100, 0)
"#).unwrap();

for cycle in [0u64, 1, 99, 100, 49_999, 50_000] {
    kernel.set_inputs(&[cycle]);
    let d = kernel.pull("device").as_u64();
    let r = kernel.pull("reading").as_u64();
    println!("cycle={cycle} → device={d} reading={r}");
}
```

Actual output (from the runnable example):

```text
cycle=0      → device=0  reading=0
cycle=1      → device=1  reading=0    (devices fill before readings advance)
cycle=99     → device=99 reading=0
cycle=100    → device=0  reading=1
cycle=49999  → device=99 reading=499
cycle=50000  → device=0  reading=500  (second dim unbounded with radix `0`)
```

The trailing `0` in `mixed_radix(cycle, 100, 0)` declares the second
dimension as unbounded — it grows indefinitely rather than wrapping.
Replace it with a finite cardinality to cap the total space.

`partition` nodes (see [nodes.md §Determinism](nodes.md#determinism-and-sampling))
give type-checked access to the resulting cells.

See [`examples/parameter_space.rs`](../examples/parameter_space.rs).

## A context layering API

A library function can be composed into a parent kernel as a sub-DAG.
Each invocation site provides its own inputs and gets its own
outputs — same compiled function, layered over a different context
at each call.

```rust
let stdlib = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("stdlib").join("identity.gk");

let mut kernel = polydat::dsl::compile_gk_with_libs(
    r#"
        input cycle: u64
        (tenant, device) := mixed_radix(cycle, 100, 0)
        // Same compiled function, two layered call sites.
        tenant_id := hashed_id(tenant, 10000)
        device_id := hashed_id(device, 10000)
    "#,
    None, vec![stdlib], &[], false, "context layering",
).expect("compile failed");
```

The polydat-direct form is single-file kernel composition like the
above. The heavier `polydat::subcontext` API (`SubcontextBuilder`,
`ScopeKernel`, `GkMatter`) is what nbrs uses for layering entire
scope-trees over parent kernels — with typed import/export contracts
at every boundary. The principle is the same; the multi-scope case
just needs more bookkeeping.

See [`examples/context_layering.rs`](../examples/context_layering.rs).

## An expression language

The `:=` lines in GK are full expressions, not just direct node calls.
You can nest, chain, and combine without naming every intermediate
wire — useful for one-line derivations where naming the intermediate
would just be noise.

```rust
let mut kernel = polydat::dsl::compile_gk(r#"
    input cycle: u64

    // Nested expressions — no named intermediates needed.
    user_id := mod(hash(cycle), 1000000)

    // Multi-arg, mixed-type composition.
    word := str_upper(number_to_words(mod(cycle, 10)))

    // Inline conditional via select_u64(cond, then, else).
    bucket := select_u64(u64_lt(user_id, 500000), 0, 1)
"#).unwrap();
```

Sample output (from the runnable example):

```text
cycle  user_id  word     bucket
-----  -------  -------  ------
    0   527897  ZERO          1
    1   460078  ONE           0
    2   564547  TWO           1
    3   960189  THREE         1
    4   862456  FOUR          1
```

See [`examples/expression_language.rs`](../examples/expression_language.rs).
