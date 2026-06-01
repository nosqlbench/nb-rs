# Cross-Fiber Cell Invalidation

The validity-tracking mechanism for `SharedCell`s
([composition_substrate.md] §S5). A producer's write
on any fiber is observable by every consumer on its
next read, with no host-side ceremony — satisfying
S5's reader contract by construction.

Implementation: `polydat/src/kernel/engines.rs`
(`SharedCellInner`, `EngineCore::check_cell_clean`,
`EngineCore::build_cell_cone`,
`EngineCore::allocate_cell_bit`).

[composition_substrate.md]: composition_substrate.md

---

## 1. Mechanism

Each cell carries a monotonic 64-bit revision counter.
Each scope holds a `Vec<Arc<AtomicU64>>` of intent-
dirty words — one word per 64 cells, appended on
demand by the allocator. Each cell holds a clone of
its scope's specific word `Arc` plus a bit position
within that word.

On write, the producer mutates the cell value under
the cell's `Mutex`, bumps the cell's revision (one
`fetch_add(Release)`), and sets the cell's bit on the
scope's word (one `fetch_or(Release)`). Three Release
stores, O(1) total, no fan-out.

On read, each consumer fiber's cone walker iterates
the precomputed cell-cone groups for the node it is
about to evaluate. Each group corresponds to one
scope-word the cone reads cells from. For each group:
load the word (one `Acquire` load), AND with the
group's interest mask; if zero, every cell in this
group is clean for this consumer (one bit per cell,
short-circuit). Otherwise, drill down by comparing
each cell's revision against the consumer fiber's
`last_seen[cell_handle]`; cells whose revision has
advanced mark the cone dirty and update `last_seen`
in preparation for re-evaluation.

Bulk-clean cost: one Acquire load + one AND per
group, typically ≤4 ops per node-eval regardless of
how many cells exist substrate-wide. Per-cell
compares are reached only when the bulk mask
indicates change.

---

## 2. Data shapes

The canonical Rust types in `polydat/src/kernel/engines.rs`:

```rust
pub struct SharedCellInner {
    pub value:               Mutex<Value>,
    pub revision:            AtomicU64,
    pub scope_intent_dirty:  Arc<AtomicU64>,  // shared with sibling cells
                                              // in the same scope-word
    pub bit:                 u8,              // 0..64 position in
                                              // scope_intent_dirty
}

pub type SharedCell = Arc<SharedCellInner>;

pub struct EngineCore {
    // ... unrelated fields ...

    pub(crate) shared_cells:      Vec<Option<SharedCell>>,
    pub(crate) output_cells:      Vec<Option<SharedCell>>,
    pub(crate) scope_intent_words: Vec<Arc<AtomicU64>>,
    pub(crate) next_cell_bit:     u32,
    pub(crate) last_seen:         HashMap<*const SharedCellInner, u64>,
    pub(crate) cell_cones:        Vec<Option<CellCone>>,
}

pub(crate) struct CellCone {
    pub(crate) groups: Vec<CellConeGroup>,
}

pub(crate) struct CellConeGroup {
    pub(crate) intent_dirty:   Arc<AtomicU64>,   // one scope-word
    pub(crate) interest_mask:  u64,              // OR of cells'
                                                  // 1<<bit in this group
    pub(crate) cells:          Vec<CellConeEntry>,
}

pub(crate) struct CellConeEntry {
    pub(crate) bit:         u8,
    pub(crate) input_slot:  usize,
}
```

`shared_cells[i]` is the cell attached to input slot
`i` on this fiber's kernel. `output_cells[j]` is the
broadcast cell for output `j` (SRD-13f Push B.2) —
descendants that bind to this output receive the same
`Arc<SharedCellInner>` via `materialize_wiring_from_outer`.

`scope_intent_words` and `next_cell_bit` together are
the scope's bit allocator. `last_seen` keys are raw
pointers (`Arc::as_ptr`) — they are compared by
identity only, never dereferenced, and stay valid for
the cell's `Arc` lifetime. `cell_cones` is the per-
node, per-fiber metadata cache; entries are built
lazily and invalidated on `attach_shared_cell`.

---

## 3. Lifecycle

### 3.1 Cell creation

`EngineCore::allocate_cell_bit` yields the next bit
position from `scope_intent_words`, growing the Vec
by one fresh `Arc<AtomicU64>` whenever the cursor
crosses a 64-bit boundary. `EngineCore::make_shared_cell`
wraps this allocator: it draws the next bit, builds
a `SharedCellInner` with the corresponding word Arc,
and returns the cell as `Arc<SharedCellInner>`.

Two call sites in `polydat/src/kernel/state.rs` /
`engines.rs` create cells:

- `seed_shared_cells` — creates one cell per `shared`
  output that has a backing input slot. Called at
  kernel construction from `GkKernel::from_program`.
- `seed_output_cells` — creates one cell per output
  for SRD-13f Push B.2 broadcast. Called from the
  same constructor.

Both paths route through `make_shared_cell`, so every
cell allocated by an `EngineCore` inherits that
core's `scope_intent_words`.

### 3.2 Cone metadata

Built lazily by `EngineCore::build_cell_cone` on the
first `check_cell_clean` invocation per node:

1. Read `program.input_provenance[node_idx]` — a u64
   bitmask of every input slot that flows into the
   node transitively.
2. Iterate set bits. For each input slot, look up
   `shared_cells[slot]`; if `Some(cell)`, the cell
   joins the cone.
3. Group cells by `Arc::ptr_eq` of their
   `scope_intent_dirty` word. Within each group, OR
   `1 << cell.bit` into `interest_mask` and append
   `CellConeEntry { bit, input_slot }` to `cells`.

Built cones are cached in `EngineCore::cell_cones[node_idx]`.
`EngineCore::invalidate_cell_cones` (called by
`GkState::attach_shared_cell`) clears every cached
entry to `None`, forcing rebuild on next access.

### 3.3 Per-fiber state

`EngineCore::last_seen` starts empty when the
`EngineCore` is constructed. Entries are inserted
lazily on first observation in §5's consumer
protocol. The map is per-`EngineCore`, hence
per-fiber-state — no contention.

### 3.4 Scope teardown

Dropping an `EngineCore` releases its
`scope_intent_words` Arcs and `cell_cones` cache.
Cells defined in this scope are dropped when their
`Arc` refcount hits zero (descendants that hold
clones keep them alive). Consumer `last_seen`
entries for dropped cells stay in the map but the
pointer is never observed again — harmless.

---

## 4. Producer protocol

```rust
impl SharedCellInner {
    pub fn publish(&self, value: Value) {
        {
            let mut guard = self.value.lock().unwrap();
            *guard = value;
        }
        self.revision.fetch_add(1, Ordering::Release);
        self.scope_intent_dirty
            .fetch_or(1u64 << self.bit, Ordering::Release);
    }
}
```

Every cell-write path calls `publish`:

- `GkState::set_input` on a cell-bound slot
  (`engines.rs`).
- `EngineCore::pull` on an output whose `output_cell`
  is attached (the SRD-13f Push B.2 broadcast).

Cost: one mutex acquire/release + one `fetch_add` +
one `fetch_or`. O(1). No upward propagation, no
fan-out, no consumer enumeration.

---

## 5. Consumer protocol

```rust
fn check_cell_clean(
    &mut self,
    program: &GkProgram,
    node_idx: usize,
) -> bool {
    // Lazy-build the cone metadata.
    if self.cell_cones.len() <= node_idx {
        self.cell_cones.resize_with(node_idx + 1, || None);
    }
    if self.cell_cones[node_idx].is_none() {
        let cone = self.build_cell_cone(program, node_idx);
        self.cell_cones[node_idx] = Some(cone);
    }

    // Pass 1: walk the cone, collect mismatches.
    let mut dirty: Vec<(*const SharedCellInner, u64)> = Vec::new();
    {
        let cone = self.cell_cones[node_idx].as_ref().unwrap();
        for group in &cone.groups {
            let intent = group.intent_dirty.load(Ordering::Acquire);
            let masked = intent & group.interest_mask;
            if masked == 0 { continue; }
            for entry in &group.cells {
                if masked & (1u64 << entry.bit) == 0 { continue; }
                let Some(Some(cell)) = self.shared_cells.get(entry.input_slot)
                    else { continue };
                let r = cell.revision.load(Ordering::Acquire);
                let ptr = Arc::as_ptr(cell);
                let prev = self.last_seen.get(&ptr).copied().unwrap_or(0);
                if r != prev {
                    dirty.push((ptr, r));
                }
            }
        }
    }
    // Pass 2: update last_seen for advanced cells.
    let clean = dirty.is_empty();
    for (ptr, r) in dirty {
        self.last_seen.insert(ptr, r);
    }
    clean
}
```

Invoked by `EngineCore::eval_node` ahead of the
memoization early-return: when `node_clean[node_idx]`
is `true`, the early-return fires only if
`check_cell_clean` also returns `true`. A `false`
return forces `node_clean[node_idx] := false` and
falls through to re-evaluation; the re-evaluation
re-reads the cells through `read_input → cell.value.lock()`.

Cost when clean: `|groups|` Acquire loads + ANDs
(typically ≤4). No allocation, no host-visible side
effect.

Cost when dirty: above + one Acquire load and one
HashMap lookup per cell whose intent bit is set,
plus one HashMap insert per cell whose revision
mismatched.

---

## 6. Memory ordering

| Operation | Ordering | Pairs with | Guarantee |
| --- | --- | --- | --- |
| Producer cell value write | mutex acquire/release | consumer mutex acquire | atomic value visibility |
| Producer `revision.fetch_add` | Release | consumer `revision.load(Acquire)` | revision visible |
| Producer `intent_dirty.fetch_or` | Release | consumer `intent_dirty.load(Acquire)` | dirty bit visible |
| Consumer `last_seen` access | non-atomic | n/a (per-fiber) | no contention |

The producer issues two atomic Release stores; the
consumer issues two atomic Acquire loads. Per the
C++20 / Rust memory model, any `load(Acquire)` that
observes the value of a `store(Release)`
synchronizes-with that store, establishing
happens-before from every write the producer made
before the Release to every read the consumer makes
after the Acquire. The cell's `Mutex` provides the
analogous synchronizes-with edge for the value
itself.

---

## 7. Correctness

**Claim.** For any producer write `W` on cell `C`
that completes its `intent_dirty.fetch_or(Release)`
before fiber `F`'s `check_cell_clean` issues its
`intent_dirty.load(Acquire)` on `C`'s scope-word:
either `F` re-evaluates the affected cone, or
`F.last_seen[C_ptr]` already reflects a revision ≥
`W`'s post-bump revision.

**Proof sketch.** `W`'s `intent_dirty.fetch_or`
happens-after `W`'s `revision.fetch_add` in program
order on the producer fiber. If `F`'s
`intent_dirty.load(Acquire)` observes the bit set by
`W`, the load synchronizes-with `W`'s `fetch_or`
Release, so `F` is guaranteed to also observe `W`'s
revision bump on its subsequent `revision.load(Acquire)`
— the compare `r != prev` returns true and the cone
re-evaluates. If `F`'s load does not observe the bit
set, either `F.last_seen` already reflects a
revision ≥ `W`'s (a prior check captured it; the
bit is sticky), or `F`'s next `check_cell_clean`
will observe it (memory propagation is bounded).

**Multi-consumer independence.** No consumer's
update to its own `last_seen` affects any other
consumer. Each `EngineCore` owns its own map; only
the producer-side atomic state is shared.

**Sticky-bit semantics.** Intent bits are never
cleared. A scope-word that has ever published a
write reports its bit as set for the remainder of
the scope's lifetime. The bulk-mask check therefore
forces the consumer into the per-cell drill-down,
where the revision compare correctly returns clean
for cells whose `last_seen` already matches. The
cost is bounded by the number of cells in the cone,
not the number of writes.

---

## 8. Parent-child composition

A child scope's intent words are independent of its
parent's — no propagation. The "logical composition"
of parent and child masks the substrate exposes to a
consumer is realized by §5's loop over
`cone.groups`. The lazy `build_cell_cone` walker
enumerates every scope (every distinct
`Arc<AtomicU64>` word) whose cells the cone reads —
parent, child, sibling-of-ancestor, any depth — and
emits one group per scope-word.

Equivalent to one combined bitmask check:

```text
(parent.intent_dirty & parent_interest)
    | (child.intent_dirty & child_interest)
    | (other.intent_dirty & other_interest)
    | ...   != 0
```

decomposed into a per-scope loop that early-outs
per scope and avoids contention on any single mask.

---

## 9. Bit allocation

### 9.1 Per-scope capacity

The scope holds `Vec<Arc<AtomicU64>>` — one
`AtomicU64` word per 64 cells, appended on demand by
the allocator. Each cell carries a clone of the
specific `Arc<AtomicU64>` for its word plus the
bit-within-word, so the cell needs only a single
`Arc` indirection (not a `Vec` lookup) on every
publish.

Growing the scope's word list is non-disruptive:
every cell already allocated keeps its
`Arc<AtomicU64>` reference, and a new cell that
needs a new word simply appends. There is no upper
bound on cells per scope.

Cone grouping (§5) groups cells by `Arc::ptr_eq` of
the word: cells in the same scope's same word share
one group; cells in different words (even in the
same scope) form separate groups, each with its own
bulk-mask early-out.

### 9.2 Allocation policy

First-fit, monotonic. Bits are not reused within a
scope's lifetime. Bit positions are stable for the
cell's lifetime in this scope.

### 9.3 Per-cell, not per-consumer

Bits are allocated per cell (producer-side), so the
vector bound is the scope's cell count — known at
scope construction. Per-consumer allocation would
require registration at sub-context construction, an
unbounded vector that grows with the consumer
population, and a registration-time fan-out. The
per-cell scheme avoids all three.

---

## 10. Worked example

A scenario covering cells at multiple scope levels,
two consumer fibers, and a concurrent producer.

### 10.1 Scene

```
                    Scope tree
                   ────────────
                                              ╔════════════════════════════════╗
                       Root R                 ║ Cells & their bit assignments  ║
                      ╱      ╲                ╠════════════════════════════════╣
                     ╱        ╲               ║ pop_size                       ║
                    ▼          ▼              ║   defined in R                 ║
              Child C1      Child C2          ║   word = R.intent_words[0]     ║
                  │            │              ║   bit = 0                      ║
                  ▼            ▼              ║                                ║
              Fiber FA      Fiber FB          ║ c1_counter                     ║
            (consumer)    (consumer)          ║   defined in C1                ║
                                              ║   word = C1.intent_words[0]    ║
                                              ║   bit = 0                      ║
       Cone deps per fiber                    ║                                ║
      ─────────────────────                   ║ c2_counter                     ║
                                              ║   defined in C2                ║
      FA's output node reads:                 ║   word = C2.intent_words[0]    ║
        pop_size    (slot 5 on FA)            ║   bit = 0                      ║
        c1_counter  (slot 7 on FA)            ║                                ║
                                              ║ Producer FW:                   ║
      FB's output node reads:                 ║   writes pop_size from R       ║
        pop_size    (slot 5 on FB)            ║   writes c1_counter from C1    ║
        c2_counter  (slot 7 on FB)            ║   writes c2_counter from C2    ║
                                              ╚════════════════════════════════╝
```

### 10.2 Cone metadata

`build_cell_cone` groups each fiber's cell-bound
deps by `Arc::ptr_eq` of `scope_intent_dirty`:

```
FA's CellCone for its output node:
┌─────────────────────────────────────────────────────┐
│ Group #0                                            │
│   intent_dirty: ─► R.intent_words[0]                │
│   interest_mask: 0b0000_0001  (bit 0 → pop_size)    │
│   cells: [ (bit=0, input_slot=5) ]                  │
├─────────────────────────────────────────────────────┤
│ Group #1                                            │
│   intent_dirty: ─► C1.intent_words[0]               │
│   interest_mask: 0b0000_0001  (bit 0 → c1_counter)  │
│   cells: [ (bit=0, input_slot=7) ]                  │
└─────────────────────────────────────────────────────┘

FB's CellCone for its output node:
┌─────────────────────────────────────────────────────┐
│ Group #0                                            │
│   intent_dirty: ─► R.intent_words[0]   ← SAME Arc!  │
│   interest_mask: 0b0000_0001  (bit 0 → pop_size)    │
│   cells: [ (bit=0, input_slot=5) ]                  │
├─────────────────────────────────────────────────────┤
│ Group #1                                            │
│   intent_dirty: ─► C2.intent_words[0]               │
│   interest_mask: 0b0000_0001  (bit 0 → c2_counter)  │
│   cells: [ (bit=0, input_slot=7) ]                  │
└─────────────────────────────────────────────────────┘
```

FA's Group #0 and FB's Group #0 point to the same
`Arc<AtomicU64>` — both cones share read access to
R's intent word. FA's Group #1 and FB's Group #1
point to different Arcs (C1's and C2's). This is
what makes scope isolation cheap: when C1 publishes
dirty intent on `C1.intent_words[0]`, FB's cone walk
never loads that atomic — its scope chain doesn't
include C1.

### 10.3 Timeline

Notation: cells in form `name [value | rev]`; intent
words as `word: bitmask`; `last_seen` per fiber as a
sparse map.

```
═══════════════════════════════════════════════════════════════════════════════
T0 — Initial state
═══════════════════════════════════════════════════════════════════════════════
  pop_size      [ 10 | rev=0 ]      R.intent_words[0]:  0
  c1_counter    [  0 | rev=0 ]      C1.intent_words[0]: 0
  c2_counter    [  0 | rev=0 ]      C2.intent_words[0]: 0
  FA.last_seen = {}                  node_clean[FA.output] = false
  FB.last_seen = {}                  node_clean[FB.output] = false

═══════════════════════════════════════════════════════════════════════════════
T1 — FA evaluates output (first pull)
═══════════════════════════════════════════════════════════════════════════════
  node_clean is false → no early return → normal eval
  read_input(5): pop_size.value.lock() → 10
  read_input(7): c1_counter.value.lock() → 0
  Node evaluates; buffer = f(10, 0) = (say) 10.
  node_clean[FA.output] := true

═══════════════════════════════════════════════════════════════════════════════
T2 — FW (producer fiber) writes pop_size = 20  via R
═══════════════════════════════════════════════════════════════════════════════
  cell.value.lock() → *value = 20 → mutex released
  cell.revision.fetch_add(1, Release)            → rev = 1   ◄┐ three
  cell.scope_intent_dirty.fetch_or(0b1, Release) │            ├ Release
                                                 │            │ stores,
  ─── R.intent_words[0] now = 0b1               ◄┘            │ all happens-
                                                              │ after the
                                                              │ value swap

═══════════════════════════════════════════════════════════════════════════════
T3 — FA evaluates output again (second pull)
═══════════════════════════════════════════════════════════════════════════════
  node_clean[FA.output] is true → check_cell_clean(FA.output):
  
    Group #0 (R.intent_words[0]):
      intent.load(Acquire) = 0b1                              ◄── synchronizes
      0b1 AND 0b1 = 0b1   → DRILL DOWN                            with T2's
        bit 0 → pop_size at slot 5:                               Release
          pop_size.revision.load(Acquire) = 1                 ◄── synchronizes
          FA.last_seen[&pop_size] = (none → treat as 0)           with T2's
          1 != 0 → DIRTY                                          revision
          FA.last_seen[&pop_size] := 1                            Release
    
    Group #1 (C1.intent_words[0]):
      intent.load(Acquire) = 0                                ◄── clean
      0 AND 0b1 = 0 → skip
    
  → returns false (dirty)
  node_clean[FA.output] := false → fall through to re-eval
  read_input(5): pop_size.value.lock() → 20                   ◄── synchronizes
  read_input(7): c1_counter.value.lock() → 0                      with T2's
  Node evaluates; buffer = f(20, 0) = 20.                          mutex release
  node_clean[FA.output] := true

═══════════════════════════════════════════════════════════════════════════════
T4 — FB evaluates output (parallel with T3, independent)
═══════════════════════════════════════════════════════════════════════════════
  check_cell_clean(FB.output):
    Group #0 (R.intent_words[0]):
      intent.load(Acquire) = 0b1 (set at T2)                  ◄── same atomic
      0b1 AND 0b1 = 0b1 → DRILL DOWN                              FA read at T3
        pop_size.revision = 1
        FB.last_seen[&pop_size] = (none → 0)                  ◄── independent
        1 != 0 → DIRTY                                            of FA's
        FB.last_seen[&pop_size] := 1                              last_seen
    Group #1 (C2.intent_words[0]):
      intent.load(Acquire) = 0 → clean
  → returns false; re-eval; FB sees pop_size=20.

═══════════════════════════════════════════════════════════════════════════════
T5 — C1 writes c1_counter = 5
═══════════════════════════════════════════════════════════════════════════════
  cell.value.lock() → *value = 5 → release
  cell.revision.fetch_add(1, Release)            → rev = 1
  cell.scope_intent_dirty.fetch_or(0b1, Release)
  ─── C1.intent_words[0] now = 0b1
  ─── R.intent_words[0] still = 0b1 (untouched; c1_counter doesn't live in R)

═══════════════════════════════════════════════════════════════════════════════
T6 — FA evaluates output again
═══════════════════════════════════════════════════════════════════════════════
  check_cell_clean(FA.output):
    Group #0 (R.intent_words[0]):
      intent.load = 0b1 (sticky from T2)
      0b1 AND 0b1 = 0b1 → DRILL DOWN
        pop_size.revision = 1
        FA.last_seen[&pop_size] = 1 → MATCH (sticky-bit false-positive
                                              caught by per-cell compare)
    Group #1 (C1.intent_words[0]):
      intent.load = 0b1
      0b1 AND 0b1 = 0b1 → DRILL DOWN
        c1_counter.revision = 1
        FA.last_seen[&c1_counter] = (none → 0)
        1 != 0 → DIRTY
        FA.last_seen[&c1_counter] := 1
  → returns false; re-eval; FA reads c1_counter=5.

═══════════════════════════════════════════════════════════════════════════════
T7 — FB evaluates output again
═══════════════════════════════════════════════════════════════════════════════
  check_cell_clean(FB.output):
    Group #0 (R.intent_words[0]):
      intent.load = 0b1 (sticky)  → drill: pop_size.rev=1, last_seen=1 → match
    Group #1 (C2.intent_words[0]):
      intent.load = 0  → 0 AND 0b1 = 0 → CLEAN (early-out, no per-cell work)
  → returns true → CLEAN, return cached value (no re-eval).
```

T2/T3/T4 show multi-fiber independence (one cell,
two consumers, each tracks its own last_seen). T5/T6
show sticky-bit semantics (R's bit remains set after
T2; the per-cell compare correctly catches it).
T5/T7 show scope isolation (C1's write at T5 has no
effect on FB's eval — FB's cone never loads
`C1.intent_words[0]` because C1 isn't in FB's scope
chain).

### 10.4 Happens-before for cell publication

The producer's three-store publish + the consumer's
two-load check form a synchronization pattern that
delivers the appearance of a single atomic
`(value, revision, intent_bit)` triple, even though
it is six separate atomic operations:

```
                Producer fiber FW                Consumer fiber FA
              ──────────────────────           ──────────────────────

  P1: mutex.lock() ─────╮
  P2: *value = V_new    │ value
  P3: mutex.unlock() ───╯ publication

  P4: revision.fetch_add(1, Release) ════╗
  P5: intent.fetch_or(bit, Release) ════╗║
                                        ║║
                                        ║║          C1: intent.load(Acquire)
                                        ║╠════════► ──synchronizes-with P5──
                                        ║              ↓ happens-before edge:
                                        ║              every store FW made
                                        ║              before P5 is visible
                                        ║              to FA after C1
                                        ║
                                        ║              (bulk-mask dirty;
                                        ║               drill down)
                                        ║
                                        ║           C2: revision.load(Acquire)
                                        ╚═════════► ──synchronizes-with P4──
                                                       ↓ happens-before edge:
                                                       every store FW made
                                                       before P4 is visible
                                                       to FA after C2

                                                    (revision mismatched;
                                                     mark node dirty;
                                                     fall through to re-eval)

                                                    C3: read_input(slot)
                                                        → mutex.lock()
                                                        ──synchronizes-with P3──
                                                          mutex acquire-release
                                                          carries the new value
                                                          across the barrier
                                                        ← V_new
```

If the consumer's bulk-mask check observes the
intent bit set (C1 sees P5's store), the consumer's
subsequent `revision.load(Acquire)` (C2) is
guaranteed to observe at least the revision P4 set,
and the subsequent `value.lock()` (C3) is guaranteed
to observe at least the value P2 wrote. If the
consumer's bulk-mask check observes the intent bit
clear (C1 sees P5's store as not-yet-published), the
consumer's cached value and `last_seen` reflect the
prior revision consistently; the next
`check_cell_clean` after P5 propagates will detect
the change.

There is no observable interleaving where the
consumer reads a torn `(value, revision, intent_bit)`
triple — i.e., sees a new intent_bit but an old
revision, or a new revision but the old value.

### 10.5 Per-event cost

| Event | Producer cost | Consumer cost (clean) | Consumer cost (dirty) |
| --- | --- | --- | --- |
| `pop_size` write (T2) | 1 mutex + 1 fetch_add + 1 fetch_or | n/a | n/a |
| FA pull, all clean | n/a | 2 Acquire loads + 2 ANDs | n/a |
| FA pull, after T2 write | n/a | 2 Acquire loads + 2 ANDs | + 1 revision load + 1 HashMap lookup + 1 insert |
| `c1_counter` write (T5) | 1 mutex + 1 fetch_add + 1 fetch_or | n/a | n/a |
| FB pull, after T5 (scope iso) | n/a | 2 Acquire loads + 2 ANDs | none (C2 bulk check is zero → early-out) |

The fast path — "every cell in every scope this
fiber cares about is at the last revision I
observed" — costs one Acquire load + one AND per
scope group, typically ≤4 ops per pull regardless of
how many cells exist substrate-wide. The dirty path
costs extra only for the cells that actually
changed, not for the rest of the cone.

Measured numbers from `polydat/benches/cell_throughput.rs`
(steady state, release build): `cell.publish`
uncontended ≈ 25 ns; `cell.snapshot` ≈ 24 ns;
clean-path pull ≈ 45 ns regardless of cells-per-
scope (1, 8, 32, 64) or cells-per-multi-word-scope
(64, 128, 256); each additional scope-group in the
cone adds ≈ 1.5 ns.

---

## 11. Out of scope

- **Volatile-node handling.** Nodes declared
  `Purity::Nondeterministic` re-evaluate every cycle
  unconditionally; that path is independent of cell
  validity tracking. Shared and volatile are
  orthogonal.
- **Cell value-read atomicity primitive.** The
  `Mutex<Value>` (or `ArcSwap<Value>`) provides
  single-value atomicity. The choice of value-
  publication primitive is orthogonal to validity
  tracking.
- **Intent-bit clearing.** Bits are sticky; the
  per-cell revision compare handles the
  consequence (§7 "Sticky-bit semantics").
- **Cross-process / distributed cells.** polydat
  is single-process; the substrate makes no claim
  beyond that boundary.

---

## 12. Bounds and invariants

- `revision: u64` — wraparound is treated as a
  non-event (would require 2⁶⁴ writes per cell).
- `scope_intent_words` grows monotonically with
  cell allocations; cells per scope are unbounded.
- `last_seen` per fiber is unbounded by type; in
  practice bounded by the distinct cell handles the
  fiber has read.
- Cell handles (`*const SharedCellInner` =
  `Arc::as_ptr`) are stable for the cell's Arc
  lifetime; a freed cell's pointer is never
  reissued (Arc allocation is unique).
- Per-write cost is O(1); per-clean-read cost is
  O(scopes_in_cone); per-dirty-read cost is
  O(dirty_cells_in_cone).
- `attach_shared_cell` to a coordinate input slot
  is a precondition violation — coordinate slots
  have an independent write path (`set_inputs`)
  that bypasses cells, producing dual writers.
  The invariant is maintained by convention (see
  composition_substrate.md §12.1 "A latent
  invariant worth naming").
