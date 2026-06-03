# IR Architecture — Stack-Machine with Stream Operands

Reference for developers working on or against polydat's
comprehension IR (`polydat::comprehension::ir`).
Companion to the algebra spec
(`polydat/docs/design/comprehension_forms.md`) — focuses on
the **how** of the runtime model rather than the **what** of
the algebra.

## The execution model: stack machine + stream operands

The IR is interpreted by a **stack machine**. Two terms easy
to conflate:

- The **machine model** is a stack machine. There is a stack
  of operands, processed left-to-right by a linear sequence
  of opcodes; each opcode pops `pop` operands off the top
  and pushes `push` operands onto the top.
- The **operands on the stack** are **streams**, not raw
  values. Each operand is a lazy producer of tuples — a
  `Box<dyn TupleStream>` whose `advance()` method yields one
  tuple at a time or `None` when exhausted.

So the canonical full phrase per spec §9.1 is
**"stack-machine interpreter that maintains a stream stack."**
Don't shorten to "stream machine" — that loses the stack-
machine semantics. Don't shorten to plain "stack machine"
either — the stack-of-values vs stack-of-streams distinction
matters for understanding the model's lazy-evaluation
behavior.

## How interpretation works

The [`interpret`](../../src/comprehension/algebra/ir/interpreter.rs)
function walks the IR opcode sequence **once**, manipulating
the stream stack:

```
for op in program.ops() {
    match op {
        PushClause { name, source }      => stack.push(ClauseStream::new(...))
        Cartesian { n }                  => stack.push(CartesianStream::new(pop_n(n)))
        Zip { n, mode }                  => stack.push(ZipStream::new(pop_n(n), mode))
        Union { n }                      => stack.push(UnionStream::new(pop_n(n)))
        Filter { predicate }             => stack.push(FilterStream::new(pop(), pred))
        OrderStreaming { kind, trunc }   => stack.push(OrderStreamingStream::new(pop(), kind, trunc))
        OrderMaterialize { strategy, .. }=> stack.push(OrderMaterializeStream::new(pop(), ...))
        Dispense                         => /* no-op; top of stack is result */
    }
}
return stack.pop();  // the final stream
```

The walk happens **once at interpret-time**. After that, the
returned stream is a tree of `TupleStream` trait objects.
**Tuple production happens lazily** when the consumer pulls
from the top: each `advance()` on the returned stream
propagates downward through the tree, pulling from leaf
clause streams only as needed.

### Concrete example

For the AST `cartesian(clause(k, [1, 2]), clause(b, [10, 20]))`:

1. Compiler emits IR:
   ```
   PushClause(k, [1, 2])
   PushClause(b, [10, 20])
   Cartesian(2)
   Dispense
   ```
2. Interpreter walk:
   ```
   step 1: stack = [ClauseStream(k)]
   step 2: stack = [ClauseStream(k), ClauseStream(b)]
   step 3: stack = [CartesianStream([ClauseStream(k), ClauseStream(b)])]
   step 4: dispense — return stack.pop()
   ```
3. Consumer calls `advance()` on the returned `CartesianStream`,
   which pulls from its child streams lazily and emits
   `[(k, 1), (b, 10)]`, then `[(k, 1), (b, 20)]`, then
   `[(k, 2), (b, 10)]`, then `[(k, 2), (b, 20)]`, then `None`.

## Why this model

Two simultaneous wins:

1. **The IR sequence is small and analyzable.** Linear,
   typed, immutable. Easy to inspect (e.g., bounds checker
   walks the opcodes once). Easy to serialize. Easy to swap
   for an alternative interpretation strategy (a stream-
   fusion compiler that rewrites the IR into a single nested
   generator function would produce identical dispense
   sequences per §9.2's correctness contract — option (b)
   of the spec).
2. **Per-tuple cost stays bounded.** No opcode dispatch per
   tuple — the per-opcode work happens at interpret-time
   (one walk). Per-tuple cost lives inside the stream
   types' `advance()` methods, which are direct virtual
   calls.

The combination means: declarative IR with clear semantics,
without sacrificing per-tuple throughput.

## Materialization barriers vs streaming

Per spec §6.2 + §6.3, exactly two opcodes are materialization
barriers:

- `OrderMaterialize` — non-Lex `order` strategy. The
  `OrderMaterializeStream` pulls its input fully on first
  `advance()`, applies the strategy, then emits permuted
  tuples one at a time.
- `Zip { mode: Cycle }` — the shorter children buffer their
  values for replay. `ZipStream`'s Cycle branch pulls all
  children to exhaustion on first `advance()`, then iterates
  with modular cursors.

Every other stream type is **streaming**: O(operator-local
state) per `advance()` above its arity. `CartesianStream` is
the most subtle case — it caches axes 1..N (which the
operator needs to re-iterate over) but streams axis 0 lazily.

## The R1 + R2 boundary: AST vs IR

The optimizer (PR 5 / spec §10) catalog includes R1 ("order
Lex → counter wrapper") and R2 ("order non-Lex → indexed
push-down"). Both are **IR compilation decisions**, not AST
rewrites. The compiler (`compile.rs`) reads
`metadata.index_addressable` to decide:

- `order(Lex, _)` → `Op::OrderStreaming` (R1's realization).
- `order(non-Lex, Some(n))` over index-addressable input →
  `Op::OrderMaterialize { indexed: true }` (R2 fires; the
  interpreter's `OrderMaterializeStream` uses the strategy's
  closed-form indexed lookup over the input's `IndexFn`).
- `order(non-Lex, Some(n))` over non-addressable input →
  `Op::OrderMaterialize { indexed: false }` (naïve: pull
  full input, apply strategy, emit).

The AST shape doesn't change for R1/R2; only the chosen IR
opcode does. The reducibility catalog (spec §10.10.3) records
R1 and R2 as IR-compilation eligibilities so the optimizer's
introspection surface reports them, but `optimize()` doesn't
rewrite for them.

## Trait object choice

`Box<dyn TupleStream>` was the chosen representation for the
stack. Alternatives considered:

- **Generic enums** — one enum variant per stream type.
  Would avoid trait-object indirection but inflate the enum
  to cover every stream-type's distinct state. Harder to
  extend (adding a new strategy means a new enum arm
  everywhere).
- **Stream-fusion via closures** — a "compile to a single
  nested generator" pass (spec §9.1 option (b)). More work
  to implement; potential perf win on hot loops but unclear
  benefit when most consumers pull fewer than 100k tuples.
  Tracked as a possible future enhancement; current trait-
  object impl is the spec's option (a).

Trait-object overhead is one vtable dispatch per `advance()`.
For workloads pulling ≤10⁶ tuples this is negligible. Hot-
path workloads can revisit if profiling shows it's the
bottleneck.

## File layout

| File | Role |
|---|---|
| `mod.rs` | Module re-exports |
| `op.rs` | `Op` enum (8 opcodes) + `OrderStreamingKind` + `Op::stack_effect()` + `Op::is_barrier()` |
| `program.rs` | `#[non_exhaustive] Program` wrapper; `Program::stack_depth()` |
| `compile.rs` | `compile(ast)` — bottom-up AST walker; R1 + R2 dispatch via metadata |
| `interpreter.rs` | `interpret(program)`; `TupleStream` trait; per-opcode stream type impls; predicate evaluator |
| `bounds.rs` | `check_bounds(program) -> ResourceBound` — spec §9.3 |

## Stack effect rules (spec §9.1)

The `Op::stack_effect()` method returns `(pop, push)` per
opcode:

| Opcode | Pop | Push |
|---|---|---|
| `PushClause` | 0 | 1 |
| `Cartesian { n }` | n | 1 |
| `Zip { n, .. }` | n | 1 |
| `Union { n }` | n | 1 |
| `Filter` | 1 | 1 |
| `OrderStreaming` | 1 | 1 |
| `OrderMaterialize` | 1 | 1 |
| `Dispense` | 1 | 0 |

A well-formed program ends with exactly one stream on the
stack just before `Dispense` (which consumes it). The
`Program::stack_depth()` method computes the maximum depth
ever reached during interpretation, which bounds spec §9.3's
`O(depth(C))` operator-stack term.

## What lives in this layer, what doesn't

**In scope for the IR layer:**

- Opcode definitions and their stack semantics.
- AST → IR compilation.
- Interpretation: streams, lazy evaluation, predicate
  evaluation (subset covering §10.9.5 catalog).
- Resource-bound checking.

**Out of scope (deferred to higher layers):**

- Predicate evaluation beyond the §10.9.5 catalog. A
  production wiring would consult polydat's Polydat expression
  evaluator. The current evaluator is sufficient for
  algebra-layer tests; "unknown" predicates evaluate to
  `true` (conservative pass-through).
- Source evaluation for `Generator` / `WorkloadParamList` /
  continuous sources. The interpreter exhausts these to
  `None` (silent no-tuple) — they require runtime evaluator
  wiring that Phase 9 (nb-rs migration) provides.
- The stream-fusion compiler interpretation strategy
  (spec §9.1 option (b)). The current stack-machine
  interpreter is option (a); both must produce identical
  dispense sequences per §9.2's correctness contract.

## Adding a new opcode

If a new IR opcode is genuinely needed (vs. just a new
strategy that's a parameterization of `OrderMaterialize`):

1. Add the variant to `Op` in `op.rs`. Implement
   `stack_effect` and `is_barrier`.
2. Add a stream type in `interpreter.rs` implementing
   `TupleStream`.
3. Add the dispatch arm in `interpret()`.
4. Update `compile.rs` if any AST node should emit the new
   opcode.
5. Update `bounds.rs` if the new opcode is a barrier.
6. Add the opcode to the resource-bound formula and the
   `Op` enum's documentation in the comprehension spec
   §9.1.

Per spec §10.8's "It does NOT add new operators to the IR.
The eight §9.1 opcodes are sufficient" — adding a new opcode
is a coordinated change across the algebra spec + IR.
Strategies should normally be added as new `StrategyName`
variants (no IR change) rather than new opcodes.
