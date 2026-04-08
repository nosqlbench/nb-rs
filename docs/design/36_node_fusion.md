# Node Fusion — Graph Optimization Pass

An assembly-time optimization pass that recognizes subgraph patterns
in the GK DAG and replaces them with single fused nodes that are
semantically equivalent but computationally cheaper.

---

## Motivation

Phase 3 JIT eliminates inter-node overhead (no gather/scatter, no
closure calls), but it cannot change the *algorithm*. When two or
more nodes compose into a pattern that has a fundamentally cheaper
fused implementation, only a graph-level rewrite can capture that.

Examples where fusion provides algorithmic improvement:

| Decomposed | Fused | Why it's cheaper |
|-----------|-------|-----------------|
| `mod(hash(x), K)` | `hash_range(x, K)` | Debiased modular reduction (Lemire); avoids full 64-bit modulus |
| `lerp(unit_interval(hash(x)), lo, hi)` | `hash_interval(x, lo, hi)` | Single hash + scaled float; eliminates intermediate u64→f64 |
| `add(x, mul(y, K))` | `add_mul(x, y, K)` | FMA-style single pass; one fewer buffer write |
| `clamp(mod(x, K), 0, K-1)` | `mod(x, K)` | Algebraic identity: mod already bounds; clamp is dead |
| `mixed_radix(identity(cycle))` on monotonic input | `incrementing_radix(radixes)` | O(1) amortized digit increment vs O(radix_count) division chain |

Fusion also reduces overhead budget (fewer nodes = fewer buffer
slots, fewer JIT instructions, less L1 pressure), which matters for
measurement quality and aggregate scaling even when the generator is
not the wall-clock bottleneck (see SRD 24).

---

## Commutativity

Commutativity is a first-class concept on node metadata, not a
per-rule annotation. This ensures the fusion pass (and any future
optimization passes) can reason about operand ordering universally.

### The `Commutativity` Enum

```rust
/// Declares which inputs of a node are interchangeable.
///
/// Used by the fusion pattern matcher to recognize equivalent
/// subgraphs regardless of operand order, and by future passes
/// (e.g., canonical ordering, common subexpression elimination).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Commutativity {
    /// Input order matters. No permutations attempted during
    /// pattern matching.
    ///
    /// Examples: `mod(dividend, divisor)`, `div(x, K)`,
    /// `concat(left, right)`, `sub(a, b)`.
    Positional,

    /// All inputs are interchangeable. For variadic nodes, this
    /// covers any arity — the matcher uses set-matching rather
    /// than permutation.
    ///
    /// Examples: `sum(a, b, ..., n)`, `product(a, b, ..., n)`,
    /// `min(a, b, ..., n)`, `max(a, b, ..., n)`,
    /// `add(a, b)` (binary form).
    AllCommutative,

    /// Specific groups of input port indices are interchangeable
    /// within each group. Inputs not listed in any group are
    /// positional.
    ///
    /// Example: `fma(x, y, z) = x + y * z`
    /// The multiplicands `y` (index 1) and `z` (index 2) commute,
    /// but the addend `x` (index 0) does not.
    /// → `Groups(vec![vec![1, 2]])`
    Groups(Vec<Vec<usize>>),
}
```

### Placement on `NodeMeta`

```rust
pub struct NodeMeta {
    pub name: String,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
    pub commutativity: Commutativity,
}
```

Every node declares its commutativity at construction time. The
default for existing unary nodes is `Positional` (vacuously correct
for single-input nodes). Nodes with symmetric semantics declare
`AllCommutative` or `Groups` as appropriate.

### Annotations on Existing Nodes

| Node | Inputs | Commutativity | Rationale |
|------|--------|---------------|-----------|
| `AddU64` | 1 (+ const) | `Positional` | Unary; vacuous |
| `MulU64` | 1 (+ const) | `Positional` | Unary; vacuous |
| `DivU64` | 1 (+ const) | `Positional` | Not commutative |
| `ModU64` | 1 (+ const) | `Positional` | Not commutative |
| `SumN` | N | `AllCommutative` | Addition is commutative and associative |
| `ProductN` | N | `AllCommutative` | Multiplication is commutative and associative |
| `MinN` | N | `AllCommutative` | min is commutative and associative |
| `MaxN` | N | `AllCommutative` | max is commutative and associative |
| `Interleave` | 2 | `Positional` | Bit interleaving is order-dependent |
| `Hash64` | 1 | `Positional` | Unary |
| `HashRange` | 1 (+ const) | `Positional` | Unary |
| `LerpConst` | 1 (+ const) | `Positional` | Unary |
| `MixedRadix` | 1 (+ const) | `Positional` | Unary |
| `ClampU64` | 1 (+ const) | `Positional` | Unary |

Future binary/ternary nodes introduced by fusion (e.g., `AddBinary`,
`FusedAddMul`) will declare their commutativity explicitly.

### Matching Semantics

The pattern matcher respects commutativity as follows:

- **`Positional`**: Inputs match in declared order only.
- **`AllCommutative`**: The matcher treats the node's inputs as an
  unordered set. For small arity (2-3), it tries all permutations.
  For larger arity, it uses set-matching: each pattern input must
  match exactly one actual input, but the assignment is unordered.
- **`Groups`**: For each group, the matcher tries permutations of
  the indices within that group. Inputs outside any group match
  positionally. Groups are independent — permutations within one
  group do not affect others.

Commutativity is declared on the *node being matched*, not on the
pattern. The pattern always specifies inputs in a canonical order;
the matcher handles reordering.

---

## Fusion Patterns

### The `FusionPattern` Enum

A fusion pattern is a structural template describing a subgraph
shape to match.

```rust
/// A structural pattern that matches a subgraph of the GK DAG.
///
/// Patterns are trees (not DAGs) — each sub-pattern matches exactly
/// one node. Diamond shapes (two pattern leaves matching the same
/// upstream node) are handled by bind-name equality checks after
/// matching.
pub enum FusionPattern {
    /// Match a node by its `meta().name` string.
    ///
    /// The node's constants are captured into the `MatchResult`
    /// under `bind`. Sub-patterns match the node's inputs
    /// (respecting the node's declared commutativity).
    Node {
        /// The node's `meta().name` (e.g., "hash", "mod", "add").
        op: &'static str,
        /// Sub-patterns for the node's inputs.
        inputs: &'static [FusionPattern],
        /// Binding name for this node's constants in the match result.
        bind: &'static str,
    },

    /// Match any wire source (coordinate, upstream node output,
    /// volatile port, etc.). This is the "hole" — it matches
    /// anything and captures the wire reference.
    ///
    /// The matched wire becomes an input to the fused replacement node.
    Any {
        /// Binding name for this wire in the match result.
        bind: &'static str,
    },

    /// Match a constant-parameterized node (zero dynamic inputs,
    /// all state captured at construction). This is syntactic sugar
    /// for `Node { op, inputs: &[], bind }` but makes the pattern
    /// more readable for leaf constants.
    Const {
        /// Expected `meta().name`.
        op: &'static str,
        /// Binding name.
        bind: &'static str,
    },
}
```

### Match Results

```rust
/// The result of a successful pattern match against a subgraph.
pub struct MatchResult {
    /// Bound wire sources: `bind_name → WireRef` for each `Any` leaf.
    pub wires: HashMap<&'static str, WireRef>,

    /// Bound node constants: `bind_name → jit_constants()` for each
    /// matched `Node` or `Const`.
    pub constants: HashMap<&'static str, Vec<u64>>,

    /// The set of node indices consumed by this match. These nodes
    /// will be removed from the DAG and replaced by the fused node.
    pub consumed_nodes: Vec<usize>,
}
```

### Example: `mod(hash(x), K) → hash_range(x, K)`

```rust
FusionRule {
    name: "hash_mod_to_hash_range",
    pattern: Node {
        op: "mod",
        inputs: &[
            Node {
                op: "hash",
                inputs: &[Any { bind: "x" }],
                bind: "hash_node",
            },
        ],
        bind: "mod_node",
    },
    replacement: |m| {
        let max = m.constants["mod_node"][0];
        Box::new(HashRange::new(max))
    },
    // Wiring: the fused node's single input is bound to "x"
    input_bindings: &["x"],
}
```

---

## Fusion Rules

### The `FusionRule` Struct

```rust
/// A graph rewrite rule: a subgraph pattern and its replacement.
///
/// Each rule is a declarative specification. The pattern describes
/// what to match; the replacement factory produces the fused node;
/// the equivalence contract (on the fused node) ensures correctness.
pub struct FusionRule {
    /// Human-readable name for diagnostics, logging, and test output.
    pub name: &'static str,

    /// The subgraph pattern to match.
    pub pattern: FusionPattern,

    /// Factory: given the match result (captured wires and constants),
    /// produce the replacement fused node.
    pub replacement: fn(&MatchResult) -> Box<dyn GkNode>,

    /// Binding names for the fused node's inputs, in order.
    /// Each name must correspond to an `Any` leaf in the pattern.
    /// The fusion pass wires the fused node's input ports to the
    /// captured wire sources in this order.
    pub input_bindings: &'static [&'static str],
}
```

### The Rules Table

```rust
/// All graph fusion rules, applied during assembly.
///
/// Rules are tried in order. Earlier rules have priority when
/// multiple rules match the same subgraph root. Each rule's
/// correctness is verified by `tests::all_fusions_are_equivalent`,
/// which exercises the fused node's `decomposed()` contract against
/// random inputs.
pub static FUSION_RULES: &[FusionRule] = &[
    // --- Algebraic identities (dead code elimination) ---

    FusionRule {
        name: "clamp_after_mod_is_redundant",
        // clamp(mod(x, K), 0, K-1) → mod(x, K)
        // mod already produces [0, K). Clamp to [0, K-1] is identity.
        pattern: ...,
        replacement: |m| Box::new(ModU64::new(m.constants["mod_node"][0])),
        input_bindings: &["x"],
    },

    // --- Hash fusions ---

    FusionRule {
        name: "hash_mod_to_hash_range",
        // mod(hash(x), K) → hash_range(x, K)
        // Combines hashing and bounded reduction in one node.
        pattern: ...,
        replacement: |m| Box::new(HashRange::new(m.constants["mod_node"][0])),
        input_bindings: &["x"],
    },

    FusionRule {
        name: "hash_unit_lerp_to_hash_interval",
        // lerp(unit_interval(hash(x)), lo, hi) → hash_interval(x, lo, hi)
        // Single hash + scaled float conversion.
        pattern: ...,
        replacement: |m| {
            let lo = f64::from_bits(m.constants["lerp_node"][0]);
            let hi = f64::from_bits(m.constants["lerp_node"][1]);
            Box::new(HashInterval::new(lo, hi))
        },
        input_bindings: &["x"],
    },

    // --- Arithmetic fusions ---

    FusionRule {
        name: "add_mul_to_affine",
        // add(mul(x, A), B) → affine(x, A, B)
        // Single pass: x * A + B.
        pattern: ...,
        replacement: |m| {
            let a = m.constants["mul_node"][0];
            let b = m.constants["add_node"][0];
            Box::new(Affine::new(a, b))
        },
        input_bindings: &["x"],
    },
];
```

---

## Equivalence Contracts

Every fused node must declare what decomposed subgraph it replaces.
This declaration is the machine-checkable proof that the fusion is
semantically correct.

### The `FusedNode` Trait

```rust
/// A fused node carries an equivalence contract: it declares the
/// decomposed subgraph it replaces, enabling automated verification.
///
/// Any node produced by a fusion rule's `replacement` factory must
/// implement this trait. The test harness uses `decomposed()` to
/// build the unfused equivalent and verify output agreement.
pub trait FusedNode: GkNode {
    /// Build the decomposed (unfused) subgraph that this node
    /// is semantically equivalent to.
    ///
    /// The returned graph has the same number of inputs and outputs
    /// as this node. The test harness evaluates both on random inputs
    /// and asserts identical outputs for all test vectors.
    fn decomposed(&self) -> DecomposedGraph;
}

/// A mini-DAG used for equivalence testing.
///
/// Built by `FusedNode::decomposed()` to represent the unfused form.
/// Not used at runtime — only in tests.
pub struct DecomposedGraph {
    pub input_count: usize,
    pub nodes: Vec<(Box<dyn GkNode>, Vec<DecomposedWire>)>,
    pub output_wires: Vec<DecomposedWire>,
}

pub enum DecomposedWire {
    /// One of the graph's external inputs, by index.
    Input(usize),
    /// Output of a node within this graph: (node_index, port).
    Node(usize, usize),
}
```

### Example: `HashRange` Decomposition

```rust
impl FusedNode for HashRange {
    fn decomposed(&self) -> DecomposedGraph {
        // hash_range(x, K) ≡ mod(hash(x), K)
        let mut g = DecomposedGraph::new(1); // 1 input: x
        let h = g.add_node(
            Box::new(Hash64::new()),
            vec![DecomposedWire::Input(0)],
        );
        let m = g.add_node(
            Box::new(ModU64::new(self.max)),
            vec![DecomposedWire::Node(h, 0)],
        );
        g.set_output(vec![DecomposedWire::Node(m, 0)]);
        g
    }
}
```

---

## Fusion Pass

### Placement in the Assembly Pipeline

The fusion pass runs inside `GkAssembler::resolve()`:

```
1. Validate arity
2. Auto-insert edge adapters
3. ► apply_fusions()          ← NEW
4. Dead code elimination (backward reachability)
5. Topological sort
6. Index remapping
```

It operates on `PendingNode` + `WireRef` (name-based wiring), before
the graph is converted to index-based representation. This makes
pattern matching more readable and allows the fusion pass to insert
and remove named nodes without disturbing index arithmetic.

### Algorithm

```
fn apply_fusions(nodes, wiring, outputs) -> (nodes, wiring, outputs):
    loop:
        matched = false
        for rule in FUSION_RULES:
            for each node N where N.meta().name == rule.pattern.root_op():
                // Check: does N have external consumers beyond the pattern?
                result = try_match(rule.pattern, N, nodes, wiring)
                if result is None: continue

                // Guard: intermediate nodes must have no external consumers
                if any consumed node (except root) has consumers outside
                   the matched subgraph: continue

                // Apply: replace subgraph with fused node
                remove consumed_nodes from graph
                insert fused_node with wiring from result.wires
                rewire any consumers of the root node to the fused node
                matched = true
                break  // restart scan (graph has changed)

        if not matched: break  // fixed point reached
```

### The External Consumer Guard

A fusion is only valid when the intermediate nodes it absorbs have
no consumers outside the matched pattern. If `hash(x)` feeds both
`mod(hash(x), K)` and `unit_interval(hash(x))`, the fusion
`hash_mod → hash_range` would delete the hash node that
`unit_interval` still needs.

In this case the fusion is skipped. The unfused graph is always
correct — fusion is purely an optimization.

**Exception**: If the intermediate node's external consumers are
*also* part of a different fusion match, both fusions could fire
independently (each gets its own copy of the shared upstream). This
is handled by the fixed-point loop: after one fusion fires, the
graph changes, and the next iteration may find new opportunities.

---

## Test Strategy

### Equivalence Property Tests

The core correctness guarantee: every fused node agrees with its
decomposed form on all inputs.

```rust
#[test]
fn all_fusions_are_equivalent() {
    for rule in FUSION_RULES {
        // Build a fused node with representative constants
        let fused = (rule.replacement)(&sample_match_for(rule));
        let decomposed = fused.decomposed();

        let input_count = fused.meta().inputs.len();
        for seed in 0..10_000 {
            let inputs = deterministic_inputs(seed, input_count);
            let fused_out = eval_node(&*fused, &inputs);
            let decomposed_out = eval_graph(&decomposed, &inputs);
            assert_eq!(
                fused_out, decomposed_out,
                "fusion '{}' diverged at seed {seed}: fused={fused_out:?}, decomposed={decomposed_out:?}",
                rule.name,
            );
        }
    }
}
```

This test runs both forms on 10,000 deterministic input vectors and
asserts bit-exact agreement. The test covers:
- Normal values (small, large, boundary)
- Edge cases (0, 1, u64::MAX, powers of two)
- The full u64 range via hashed seeds

### Pattern Matching Tests

Verify that patterns match the expected subgraph shapes:

```rust
#[test]
fn hash_mod_pattern_matches() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
    asm.add_node("m", Box::new(ModU64::new(100)), vec![WireRef::node("h")]);
    asm.add_output("out", WireRef::node("m"));
    let kernel = asm.compile_with_fusion();
    // The kernel should contain a HashRange(100), not Hash64 + ModU64
    assert_eq!(kernel.node_count(), 1);
    assert_eq!(kernel.node(0).meta().name, "hash_range");
}
```

### Commutativity Tests

Verify that commutative nodes match regardless of input order:

```rust
#[test]
fn commutative_match_either_order() {
    // sum(hash(x), mul(y, 3)) should match the same pattern as
    // sum(mul(y, 3), hash(x)) when sum is AllCommutative
    // ... build both orderings, assert both match the same rule
}
```

### External Consumer Guard Tests

Verify that fusion is correctly suppressed when intermediate nodes
have external consumers:

```rust
#[test]
fn fusion_skipped_when_intermediate_has_consumers() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
    asm.add_node("m", Box::new(ModU64::new(100)), vec![WireRef::node("h")]);
    asm.add_node("u", Box::new(UnitInterval::new()), vec![WireRef::node("h")]);
    asm.add_output("bounded", WireRef::node("m"));
    asm.add_output("unit", WireRef::node("u"));
    let kernel = asm.compile_with_fusion();
    // hash_mod fusion should NOT fire because "h" has two consumers
    assert!(kernel.node_names().contains(&"h"));
}
```

---

## Future Extensions

### Monotonic Coordinate Fusion

When the cycle source is known to be strictly monotonic at assembly
time, `mixed_radix(identity(cycle))` can be replaced with an
incrementing counter that advances digits in O(1) amortized time.
This requires runtime context (monotonicity guarantee) beyond pure
graph structure, so it will use an extended pattern type:

```rust
FusionPattern::NodeWithGuard {
    op: "mixed_radix",
    inputs: ...,
    bind: "mr",
    guard: |ctx| ctx.cycle_source_is_monotonic(),
}
```

### Cross-Boundary Fusion

Currently fusion only operates within a single GK kernel. A future
extension could fuse across the kernel boundary — for example,
recognizing that the coordinate transform + the first kernel node
form a fusible pattern.

### Algebraic Simplification

Beyond pattern-matched fusion, an algebraic simplifier could apply
identity laws (add 0, mul 1, mod by power-of-2 → bitwise AND) as
a separate pass using the same infrastructure.

---

## Relationship to Other SRDs

- **SRD 10 (AOT Compilation)**: Fusion runs before compilation.
  The fused nodes participate in Phase 2/3/Hybrid like any node.
- **SRD 24 (Compilation Levels)**: Fused nodes provide
  `compiled_u64()` and `jit_constants()` for all compilation levels.
- **SRD 27 (GK Modules)**: Module-defined subgraphs are inlined
  before fusion, so fusion can see through module boundaries.
