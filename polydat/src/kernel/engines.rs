// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polydat evaluation engines: EngineCore (shared eval loop) and the three
//! P1 engine types — PolydatState (dependent-list), RawState (no provenance),
//! and ProvScanState (provenance-scan).

use std::sync::{Arc, Mutex, OnceLock};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::ast::Value;
use super::WireSource;
use super::program::PolydatProgram;

/// Cached lookup of the `NBRS_DIRTY_DEBUG` env var. Called from
/// the per-cycle hot path (`PolydatState::set_input`); reading the
/// real `std::env::var` on every cycle costs ~30% of CPU on
/// single-fiber dryrun benches (it walks the libc env table and
/// formats a fresh CString each call). The OnceLock evaluates
/// once on first touch and every subsequent call is one atomic
/// load.
fn nbrs_dirty_debug_enabled() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| std::env::var("NBRS_DIRTY_DEBUG").is_ok())
}

/// A cross-kernel mutable cell for a `shared`-modifier wire.
///
/// When a `shared` output in an outer scope is bound into an
/// inner kernel via `materialize_wiring_from_outer`, both kernels' input
/// slots reference the same `SharedCell`. Writes from inner via
/// `set_input` flow through to the cell; reads on either side
/// pick up the latest value.
///
/// Concurrent writers serialize at the Mutex; the current
/// semantic is **last-write-wins** (lock-acquisition order).
/// Future templated patterns (atomic-fetch-add, sum-reduction,
/// merge, etc.) — see SRD-16 §"Open: concurrent shared
/// mutation" — will introduce alternative cell types selected
/// per binding declaration.
///
/// ## Cross-fiber validity tracking
///
/// Each cell carries its own validity-tracking handles per
/// `polydat/docs/design/cross_fiber_invalidation.md`:
///
/// - `revision: AtomicU64` — monotonic counter, bumped on every
///   write. Consumer fibers cache the last revision they
///   observed in their per-fiber `last_seen` map; a mismatch
///   tells the cone walker to re-evaluate.
/// - `scope_intent_dirty: Arc<AtomicU64>` — bit-vector shared
///   with every other cell defined in this cell's scope. The
///   cell's `bit` position is set on every write, allowing
///   consumers to do an O(1) bulk check ("any cell in this
///   scope dirty?") before drilling down to the per-cell
///   revision compare.
/// - `bit: u8` — this cell's position in the scope's intent-
///   dirty vector. Allocated at cell creation by the defining
///   scope's `EngineCore::allocate_cell_bit`. Bounded at 64
///   for the first cut; spill-to-`Vec<AtomicU64>` is deferred.
///
/// The reader contract (S5 §1.1) is preserved: a producer's
/// `publish` writes value + revision + intent bit in three
/// Release stores; a consumer's `check_clean` walk on its next
/// read observes the change without any host-side ceremony.
pub struct SharedCellInner {
    /// Cell value. The mutex serialises concurrent writers and
    /// gives readers single-value atomicity.
    pub value: Mutex<Value>,
    /// Monotonic revision counter. Bumped on every write
    /// (Release); compared by consumers (Acquire) against
    /// per-fiber `last_seen`.
    pub revision: AtomicU64,
    /// Defining scope's intent-dirty bit-vector. Shared by Arc
    /// across every cell allocated by the same scope. On every
    /// write the producer ORs `1 << self.bit` into this
    /// (Release) so consumers' bulk-mask check sees the scope
    /// as dirty.
    pub scope_intent_dirty: Arc<AtomicU64>,
    /// This cell's bit position in `scope_intent_dirty`. Stable
    /// for the cell's lifetime.
    pub bit: u8,
}

impl std::fmt::Debug for SharedCellInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedCellInner")
            .field("revision", &self.revision.load(Ordering::Relaxed))
            .field("bit", &self.bit)
            .finish_non_exhaustive()
    }
}

impl SharedCellInner {
    /// Construct a new cell with the given initial value, bound
    /// to the defining scope's intent-dirty word at the given
    /// bit position within that word. Callers must allocate
    /// `(word, bit)` via [`EngineCore::allocate_cell_bit`] —
    /// the bit is not reusable for the cell's lifetime.
    pub fn new(
        initial: Value,
        scope_intent_dirty: Arc<AtomicU64>,
        bit: u8,
    ) -> Self {
        debug_assert!(
            bit < 64,
            "bit-within-word {bit} must be < 64; the allocator splits >64-bit \
             scope vectors across multiple words"
        );
        Self {
            value: Mutex::new(initial),
            revision: AtomicU64::new(0),
            scope_intent_dirty,
            bit,
        }
    }

    /// Producer-side write: replace the cell's value, bump the
    /// revision, set the intent bit. Three Release stores
    /// publish the write across any consumer fiber per
    /// `cross_fiber_invalidation.md` §6. The mutex critical
    /// section is held only for the value swap; the atomics
    /// run outside it.
    pub fn publish(&self, value: Value) {
        {
            let mut guard = self.value.lock().unwrap();
            *guard = value;
        }
        self.revision.fetch_add(1, Ordering::Release);
        self.scope_intent_dirty
            .fetch_or(1u64 << self.bit, Ordering::Release);
    }

    /// Consumer-side read: snapshot the cell's value and the
    /// revision it was published at. Returns a pair so the
    /// caller can update its `last_seen[cell] = revision`
    /// alongside taking the value, without a second cell access.
    pub fn snapshot(&self) -> (Value, u64) {
        // Acquire-load the revision first so the value read
        // synchronises-with the producer's value publication.
        // The mutex itself provides the memory barrier for the
        // value, but the revision is read with explicit Acquire
        // for the cross-fiber happens-before relation.
        let value = self.value.lock().unwrap().clone();
        let revision = self.revision.load(Ordering::Acquire);
        (value, revision)
    }
}

/// Externally-held handle to a shared cell. `Arc<SharedCellInner>`
/// so a single cell can be referenced from many kernels at
/// once. The handle is cheap to clone (Arc bump).
pub type SharedCell = Arc<SharedCellInner>;

/// Per-node cone metadata for cell-bound input dependencies.
///
/// Built lazily on first `check_cell_clean` per node and
/// cached in [`EngineCore::cell_cones`]; invalidated by
/// clearing the cache whenever cells are attached or detached.
///
/// The structure groups a node's cell-bound input dependencies
/// by the defining scope's `intent_dirty` Arc (compared by
/// `Arc::ptr_eq`). Each group carries the bulk-check
/// `interest_mask` for the scope plus per-cell drill-down
/// entries — implementing the bulk-mask + per-cell-revision
/// protocol from `cross_fiber_invalidation.md` §5.
#[derive(Debug, Default, Clone)]
pub(crate) struct CellCone {
    /// Cells grouped by defining-scope's `intent_dirty`. Empty
    /// = no cell-bound deps; check returns trivially clean.
    pub(crate) groups: Vec<CellConeGroup>,
}

#[derive(Debug, Clone)]
pub(crate) struct CellConeGroup {
    /// Defining scope's `intent_dirty` vector (Arc cloned from
    /// the cells). Bulk-mask check: AND this against
    /// `interest_mask`; if zero, every cell in this group is
    /// clean for this consumer (modulo last_seen) — skip the
    /// drill-down.
    pub(crate) intent_dirty: Arc<AtomicU64>,
    /// OR of `1 << cell.bit` for every cell in this group.
    pub(crate) interest_mask: u64,
    /// Per-cell drill-down entries. Each gives the bit position
    /// in `intent_dirty` plus the input slot index where the
    /// cell is attached, for revision compare against
    /// `last_seen`.
    pub(crate) cells: Vec<CellConeEntry>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CellConeEntry {
    pub(crate) bit: u8,
    pub(crate) input_slot: usize,
}

/// One named shared cell propagated through the parent → child
/// scope chain. Carried on `PolydatKernel` (and surfaced through
/// `ScopeKernel::shared_cells_in_scope`) so a descendant whose
/// program declares a matching input slot can attach the cell —
/// even when intermediate scopes' bodies never name it and so
/// have no input slot for it themselves.
///
/// Without this carrier, an ancestral `shared X := …` cell
/// becomes invisible past the first intermediate scope under
/// the closure-binding economy. With it, every spawn step
/// computes "every cell visible at this scope" and threads the
/// full set forward — the cascade is transitive by
/// construction.
#[derive(Clone, Debug)]
pub struct SharedCellEntry {
    pub name: String,
    pub port_type: crate::ast::PortType,
    pub cell: SharedCell,
}

/// SRD-82 §"Panic reporting: one full render" — set by a host
/// runtime that catches worker panics and renders the full
/// enriched diagnostic itself (the `errors:` block). When set,
/// the re-raise hook below prints a single first-line notice
/// instead of the full body; bare polydat consumers never set it
/// and keep the full print.
static PANIC_REPORTING_DOWNSTREAM: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Declare that a downstream reporter will render eval-panic
/// diagnostics in full (see [`PANIC_REPORTING_DOWNSTREAM`]).
pub fn set_panic_reporting_downstream(on: bool) {
    PANIC_REPORTING_DOWNSTREAM.store(on, std::sync::atomic::Ordering::Relaxed);
}

thread_local! {
    /// True while a node eval runs inside the enrichment
    /// catch_unwind in `eval_node`. The suppression hook checks
    /// this to swallow the raw std panic-hook print (bare payload
    /// + backtrace pointer at the original panic site) — that
    /// same panic is about to be caught, enriched with
    /// node/output/input context, and re-raised via `panic_any`,
    /// which fires the hook again with this flag clear. Net
    /// effect: exactly ONE hook print, and it's the enriched one.
    static EVAL_PANIC_CAPTURE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    /// Original panic location captured by the suppression hook
    /// while the flag above is set. Folded into the enriched
    /// message so the true `file:line` survives the re-raise
    /// (the re-raised panic's own location points at the
    /// re-raise site, which is useless).
    static EVAL_PANIC_LOCATION: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
    /// One-shot marker armed just before the enriched re-raise
    /// when a downstream reporter exists: the hook prints a short
    /// first-line notice for that panic instead of the full body.
    static RERAISE_SHORT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Install (once, process-wide) a panic hook that chains to the
/// previously installed hook unless the current thread is inside
/// the wrapped node eval, in which case it records the panic
/// location and stays quiet.
fn install_eval_panic_hook() {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if EVAL_PANIC_CAPTURE.with(|c| c.get()) {
                let loc = info.location().map(|l| l.to_string());
                EVAL_PANIC_LOCATION.with(|slot| *slot.borrow_mut() = loc);
            } else if RERAISE_SHORT.with(|c| c.replace(false)) {
                // The runtime will render the full enriched
                // diagnostic in the phase error list; one short
                // line keeps the terminal signal without the
                // four-fold repeat (SRD-82 §one full render).
                let first = info
                    .payload()
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .and_then(|m| m.lines().next())
                    .unwrap_or("<non-string panic payload>");
                eprintln!("op eval panic (detail in phase errors): {first}");
            } else {
                prev(info);
            }
        }));
    });
}

/// RAII guard arming the suppression hook for one wrapped eval.
/// Saves and restores the previous flag value: nodes that drive
/// sub-kernels (comprehensions, gk-call) nest evals, and each
/// level's catch_unwind must see its own panics suppressed.
struct EvalPanicCaptureGuard {
    prev: bool,
}

impl EvalPanicCaptureGuard {
    fn arm() -> Self {
        install_eval_panic_hook();
        let prev = EVAL_PANIC_CAPTURE.with(|c| c.replace(true));
        EVAL_PANIC_LOCATION.with(|slot| slot.borrow_mut().take());
        Self { prev }
    }
}

impl Drop for EvalPanicCaptureGuard {
    fn drop(&mut self) {
        EVAL_PANIC_CAPTURE.with(|c| c.set(self.prev));
    }
}

/// Build the rich diagnostic message for a node-level eval panic.
/// Includes the node's function name, every output it feeds, the
/// input values it was called with, the original panic location
/// (captured by the suppression hook), and the program's
/// diagnostic context (typically the source path / scope label).
/// This is what the user sees instead of the bare panic payload.
fn enrich_eval_panic(
    payload: Box<dyn std::any::Any + Send>,
    program: &PolydatProgram,
    node_idx: usize,
    inputs: &[Value],
) -> String {
    let original = payload
        .downcast_ref::<&'static str>().map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".into());
    // A payload that already carries node context came from a
    // nested wrapped eval's re-raise; its captured "location" is
    // the re-raise site, not the original panic — skip it.
    let location_line = if original.contains("↳ in node") {
        String::new()
    } else {
        EVAL_PANIC_LOCATION
            .with(|slot| slot.borrow_mut().take())
            .map(|loc| format!("\n  ↳ panicked at {loc}"))
            .unwrap_or_default()
    };
    let node_name = program.nodes.get(node_idx)
        .map(|n| n.meta().name.to_string())
        .unwrap_or_else(|| format!("<unknown node #{node_idx}>"));
    let mut output_names: Vec<&str> = program.output_map_iter()
        .filter_map(|(name, (n_idx, _))| {
            if *n_idx == node_idx { Some(name.as_str()) } else { None }
        })
        .collect();
    output_names.sort();
    let outputs_label = if output_names.is_empty() {
        "no declared output".to_string()
    } else {
        format!("output{} {}",
            if output_names.len() == 1 { "" } else { "s" },
            output_names.join(", "))
    };
    let mut input_label = String::new();
    for (i, v) in inputs.iter().enumerate() {
        if i > 0 { input_label.push_str(", "); }
        input_label.push_str(&format!("[{i}]={}", format_value_for_diag(v)));
    }
    format!(
        "{original}{location_line}\n  ↳ in node `{node_name}` ({outputs_label}) \
         while evaluating {context}\n  \
         ↳ inputs: [{input_label}]",
        context = program.context(),
    )
}

/// Format a `Value` into a short diagnostic string. Strings are
/// quoted + truncated; vectors print their length not contents.
fn format_value_for_diag(v: &Value) -> String {
    match v {
        Value::U64(n) => format!("U64({n})"),
        Value::F64(n) => format!("F64({n})"),
        Value::Bool(b) => format!("Bool({b})"),
        Value::Str(s) => {
            let trimmed: String = s.chars().take(40).collect();
            if s.chars().count() > 40 {
                format!("Str({trimmed:?}…)")
            } else {
                format!("Str({trimmed:?})")
            }
        }
        Value::None => "None".to_string(),
        other => format!("{:?}", other.port_type()),
    }
}

/// Shared evaluation state for all Polydat engines. Contains the node
/// output buffers, input values, and the eval loop.
/// Engine types wrap this and provide their own invalidation strategy.
pub struct EngineCore {
    /// Per-node output value buffers, reused across evaluations.
    pub(crate) buffers: Vec<Vec<Value>>,
    /// Per-node: true = cached output is valid, false = needs eval.
    pub(crate) node_clean: Vec<bool>,
    /// Current input values (coordinates + captures, all unified).
    /// For `shared`-bound slots, this holds a local snapshot of
    /// the cell value — `refresh_shared` re-syncs it from the
    /// cell, and `set_input` writes through to both the cell
    /// and the snapshot.
    pub(crate) inputs: Vec<Value>,
    /// Default values for each input (used by reset_inputs).
    pub(crate) input_defaults: Vec<Value>,
    /// Optional cross-kernel shared cell per input slot. `None`
    /// = local-only input (the common case). `Some(cell)` =
    /// the slot is bound to a shared cell; writes propagate
    /// through the cell to whatever other kernels share it.
    pub(crate) shared_cells: Vec<Option<SharedCell>>,
    /// SRD-13f Push B.2 — per-output broadcast cell. Indexed
    /// by output position in `program.output_list`. `Some(cell)`
    /// = the output broadcasts its value to descendants via
    /// the cell whenever the owner pulls the output; `None` =
    /// no broadcast subscribers were set up (no descendant
    /// scope binds against this output's name).
    ///
    /// `materialize_wiring_from_outer` plumbs the same `Arc<SharedCell>`
    /// onto the matching input slot on the inner kernel — at
    /// that point both ends share the storage. Inner reads
    /// transparently through the cell on every `read_input`;
    /// outer's `pull` writes the freshly computed value into
    /// the cell so subsequent inner reads return the current
    /// value with no traversal.
    pub(crate) output_cells: Vec<Option<SharedCell>>,
    /// Pre-allocated scratch buffer for node input gathering.
    pub(crate) input_scratch: Vec<Value>,
    /// This scope's intent-dirty bit-vector. One `AtomicU64`
    /// word per 64 cells allocated by this scope; new words
    /// are appended on demand by [`Self::allocate_cell_bit`].
    /// Each cell carries a clone of the specific `Arc<AtomicU64>`
    /// for its word (and its bit-within-word). Consumer fibers'
    /// bulk-mask check (per `cross_fiber_invalidation.md` §5)
    /// groups cells by `Arc::ptr_eq` of their word and ANDs
    /// the loaded word against the cone's interest mask for
    /// that word.
    ///
    /// The `Vec<Arc<...>>` shape — rather than a single
    /// `Arc<Vec<AtomicU64>>` — lets cells take a stable
    /// per-word handle that the scope can grow without
    /// invalidating any existing cell's reference.
    pub(crate) scope_intent_words: Vec<Arc<AtomicU64>>,
    /// Next bit position to allocate from
    /// [`Self::scope_intent_words`]. Word index is
    /// `next_cell_bit / 64`; bit within word is
    /// `next_cell_bit % 64`. Monotonic; bits are never reused
    /// within a scope's lifetime.
    pub(crate) next_cell_bit: u32,
    /// Per-fiber cache of the last revision this engine observed
    /// for each cell it has read. Keyed by `Arc::as_ptr` of the
    /// `SharedCellInner`. Sparse; entries are inserted lazily
    /// on first observation via `check_cell_clean`.
    ///
    /// Per-fiber state — no contention. Pointer keys are stable
    /// for the cell's lifetime; orphaned entries for dropped
    /// cells are harmless (the handle is never observed again).
    pub(crate) last_seen: std::collections::HashMap<*const SharedCellInner, u64>,
    /// Per-node cone metadata for cell-bound input deps. Lazy:
    /// `None` until first `check_cell_clean` for that node;
    /// then built once and reused. Cleared in bulk on any
    /// attach/detach of shared cells.
    pub(crate) cell_cones: Vec<Option<CellCone>>,
}

// `last_seen` keys are `*const SharedCellInner` raw pointers,
// which Rust treats as non-Send/non-Sync. The pointers are
// only compared by identity (never dereferenced) and each
// `EngineCore` is owned by exactly one fiber, so the
// non-thread-safe pointer keys are sound. The Send/Sync
// markers here cover that gap explicitly.
unsafe impl Send for EngineCore {}
unsafe impl Sync for EngineCore {}

impl EngineCore {
    /// Allocate the next bit position from this scope's
    /// intent-dirty vector for a newly-created cell. Returns
    /// the specific word's `Arc<AtomicU64>` plus the bit
    /// position within that word. Grows
    /// [`Self::scope_intent_words`] on demand — each new word
    /// is a freshly-allocated `Arc<AtomicU64>` so existing
    /// cells' references stay stable.
    pub(crate) fn allocate_cell_bit(&mut self) -> (Arc<AtomicU64>, u8) {
        let bit = self.next_cell_bit;
        let word_idx = (bit / 64) as usize;
        let bit_in_word = (bit % 64) as u8;
        while self.scope_intent_words.len() <= word_idx {
            self.scope_intent_words.push(Arc::new(AtomicU64::new(0)));
        }
        let word = self.scope_intent_words[word_idx].clone();
        self.next_cell_bit += 1;
        (word, bit_in_word)
    }

    /// Construct a new `SharedCell` bound to this scope's
    /// intent-dirty vector. Convenience wrapper that allocates
    /// a fresh bit and builds the cell — every cell creation
    /// site goes through here so the scope's bit allocator
    /// stays the single source of truth.
    pub(crate) fn make_shared_cell(&mut self, initial: Value) -> SharedCell {
        let (word, bit) = self.allocate_cell_bit();
        Arc::new(SharedCellInner::new(initial, word, bit))
    }
}

impl EngineCore {
    /// Read an input slot's current value, transparent to whether
    /// it's a plain slot or backed by a `SharedCell`. The
    /// canonical read path used by both `eval_node` and
    /// `PolydatState::get_input` — there's no separate "refresh" step
    /// the caller must remember; the cell is queried on every
    /// read.
    ///
    /// Cost: one Mutex lock per read on shared slots; a clone of
    /// `inputs[idx]` on plain slots (Value's clone is cheap —
    /// Arc-based for vectors, primitive copy otherwise).
    #[inline]
    pub(crate) fn read_input(&self, idx: usize) -> Value {
        if let Some(cell) = self.shared_cells.get(idx).and_then(|c| c.as_ref()) {
            return cell.value.lock().unwrap().clone();
        }
        self.inputs[idx].clone()
    }

    /// Build the cone metadata for `node_idx` — the per-scope
    /// groups of cell-bound input dependencies, derived from
    /// `program.input_provenance[node_idx]` and the cells
    /// currently attached on this engine.
    ///
    /// Returns an empty `CellCone { groups: [] }` for nodes
    /// with no cell-bound deps (the common case).
    fn build_cell_cone(&self, program: &PolydatProgram, node_idx: usize) -> CellCone {
        let empty = crate::kernel::ProvMask::empty();
        let prov = program.input_provenance
            .get(node_idx)
            .unwrap_or(&empty);
        let mut groups: Vec<CellConeGroup> = Vec::new();
        // Iterate set bits of `prov` directly: each bit is an
        // input slot that flows into this node transitively.
        for input_idx in prov.iter_ones() {
            let Some(Some(cell)) = self.shared_cells.get(input_idx) else { continue; };
            // Group by Arc-pointer identity of scope_intent_dirty.
            let group_idx = groups.iter()
                .position(|g| Arc::ptr_eq(&g.intent_dirty, &cell.scope_intent_dirty));
            let i = match group_idx {
                Some(i) => i,
                None => {
                    groups.push(CellConeGroup {
                        intent_dirty: cell.scope_intent_dirty.clone(),
                        interest_mask: 0,
                        cells: Vec::new(),
                    });
                    groups.len() - 1
                }
            };
            groups[i].interest_mask |= 1u64 << cell.bit;
            groups[i].cells.push(CellConeEntry {
                bit: cell.bit,
                input_slot: input_idx,
            });
        }
        CellCone { groups }
    }

    /// Cross-fiber check: return `true` if this fiber's
    /// `last_seen` is up-to-date for every cell in `node_idx`'s
    /// cone (no cross-fiber writes since last observation).
    /// Returns `false` if any cell's revision has advanced,
    /// updating `last_seen` to reflect the new revisions in
    /// preparation for the caller's re-evaluation.
    ///
    /// Per cross_fiber_invalidation.md §5: bulk-mask check
    /// (one Acquire load + AND per scope group) early-outs
    /// when nothing in the scope is dirty; per-cell drill-down
    /// runs only on set bits.
    fn check_cell_clean(
        &mut self,
        program: &PolydatProgram,
        node_idx: usize,
    ) -> bool {
        // Lazy build the cone metadata.
        if self.cell_cones.len() <= node_idx {
            self.cell_cones.resize_with(node_idx + 1, || None);
        }
        if self.cell_cones[node_idx].is_none() {
            let cone = self.build_cell_cone(program, node_idx);
            self.cell_cones[node_idx] = Some(cone);
        }

        // First pass: walk the cone, collect mismatches. The
        // immutable borrow of `self.cell_cones`,
        // `self.shared_cells`, and `self.last_seen` coexist
        // because they're disjoint fields of `self`.
        let mut dirty: Vec<(*const SharedCellInner, u64, usize)> = Vec::new();
        {
            let cone = self.cell_cones[node_idx].as_ref().unwrap();
            for group in &cone.groups {
                let intent = group.intent_dirty.load(Ordering::Acquire);
                let masked = intent & group.interest_mask;
                if masked == 0 { continue; }
                for entry in &group.cells {
                    if masked & (1u64 << entry.bit) == 0 { continue; }
                    let Some(Some(cell)) = self.shared_cells.get(entry.input_slot) else {
                        continue;
                    };
                    let r = cell.revision.load(Ordering::Acquire);
                    let ptr = Arc::as_ptr(cell);
                    let prev = self.last_seen.get(&ptr).copied().unwrap_or(0);
                    if r != prev {
                        dirty.push((ptr, r, entry.input_slot));
                    }
                }
            }
        }
        let clean = dirty.is_empty();
        // Second pass: update last_seen for every cell whose
        // revision we observed has advanced. Done in a
        // separate pass to release the cone borrow above.
        //
        // Updating `last_seen` CONSUMES the dirty signal for this
        // fiber, so the re-evaluation it triggers must reach every
        // memoized node between the dirty slot and any consumer —
        // not just the node that happened to check first. The
        // caller only re-evaluates the CHECKED node; its recursive
        // upstream walk re-checks each parent's own cone, which now
        // reads the just-updated `last_seen` and comes back clean,
        // leaving the intermediate buffers stale — the checked node
        // then recomputes from stale parents (observed as a
        // phase-poll predicate memoized at its pre-write value
        // forever). Mirror `set_input`'s write-side rule on the
        // read side: a detected cross-fiber write invalidates every
        // node whose transitive input provenance covers the dirty
        // slot.
        if !clean {
            // Exact multi-word mask: slots >= 64 invalidate too
            // (the one-word form silently SKIPPED them — a latent
            // under-invalidation on >64-input scopes).
            let mut dirty_mask = crate::kernel::ProvMask::empty();
            for (ptr, r, slot) in dirty {
                self.last_seen.insert(ptr, r);
                dirty_mask.set(slot);
            }
            for node_idx in 0..program.nodes.len() {
                if program.input_provenance
                    .get(node_idx)
                    .is_some_and(|prov| prov.intersects(&dirty_mask))
                {
                    self.node_clean[node_idx] = false;
                }
            }
        }
        clean
    }

    /// Mark `cell_cones` as stale. Called after any change to
    /// `shared_cells` that could affect the per-node cone
    /// metadata (attach, detach). Next `check_cell_clean` on
    /// any node will rebuild on demand.
    pub(crate) fn invalidate_cell_cones(&mut self) {
        for cone in self.cell_cones.iter_mut() {
            *cone = None;
        }
    }

    /// Evaluate a node by index. Shared by all engines.
    /// Checks the clean flag, recursively evaluates upstream, gathers
    /// inputs, calls node.eval(), marks clean.
    pub fn eval_node(&mut self, program: &PolydatProgram, node_idx: usize) {
        if self.node_clean[node_idx] {
            // Memoization hit candidate — confirm cell-bound
            // inputs in this node's cone are still at the
            // revisions this fiber last observed. If any
            // producer fiber has bumped a cell's revision since
            // then, force a re-eval (the cache is stale even
            // though `node_clean` is true) per
            // cross_fiber_invalidation.md §5.
            if self.check_cell_clean(program, node_idx) {
                return;
            }
            self.node_clean[node_idx] = false;
        }

        let wiring = &program.wiring[node_idx];
        for source in wiring.iter() {
            if let WireSource::NodeOutput(upstream_idx, _) = source {
                self.eval_node(program, *upstream_idx);
            }
        }

        for (i, source) in wiring.iter().enumerate() {
            self.input_scratch[i] = match source {
                // `read_input` transparently reads the cell for
                // `shared`-bound slots, so per-cycle eval picks
                // up cross-kernel writes without any explicit
                // refresh.
                WireSource::Input(idx) => self.read_input(*idx),
                WireSource::NodeOutput(upstream_idx, port_idx) => {
                    self.buffers[*upstream_idx][*port_idx].clone()
                }
            };
        }

        let input_count = wiring.len();

        // SRD-74 Rule 1 — None propagation lifted to the kernel
        // level. Any node whose inputs include `Value::None`
        // emits `Value::None` on every output without invoking
        // the node's `eval`. This holds the SQL-NULL / Rust
        // `Option::?` propagation rule uniformly for ALL GK
        // nodes, avoiding the dozens of duplicate per-node
        // `if matches!(input, Value::None)` checks. Individual
        // nodes (e.g. `Printf`) keep their checks redundant but
        // harmless — the kernel guard fires first.
        //
        // Opt-out: nodes whose semantics explicitly consume
        // `Value::None` (coalesce-style `default_or`, explicit
        // optionality handlers per SRD-74 Rule 2) override
        // `PolydatNode::accepts_none_inputs` to skip this guard. Such
        // nodes handle `None` in their own `eval`.
        let node_ref = &*program.nodes[node_idx];
        if !node_ref.accepts_none_inputs()
            && self.input_scratch[..input_count]
                .iter()
                .any(|v| matches!(v, Value::None))
        {
            for slot in &mut self.buffers[node_idx] {
                *slot = Value::None;
            }
            self.node_clean[node_idx] = true;
            return;
        }

        // Wrap the node's eval in catch_unwind so a node-level
        // panic (e.g. `Value::as_u64` on a Str) can be re-raised
        // with the diagnostic context the user actually needs:
        // which node panicked, which output(s) it feeds, what
        // the input values were, and where in the source the
        // node came from. Without this, the fiber-level catcher
        // sees only the bare message — "expected U64, got Str"
        // — and the user has no way to find the offending
        // binding short of bisecting the workload.
        //
        // Cost: one catch_unwind frame per slow-path node eval.
        // The JIT path doesn't go through here. On the success
        // path the frame is a few stack words; on the panic
        // path it's strictly an improvement over what the
        // user sees today.
        //
        // The capture guard suppresses the std panic hook for
        // the duration: without it, the hook prints the BARE
        // payload ("expected U64, got F64" + backtrace) at the
        // original panic site, before enrichment exists, and
        // that raw print is the loudest thing the user sees.
        // Re-raising with `panic_any` (not `resume_unwind`)
        // fires the hook again — now unsuppressed — so the one
        // message that prints is the enriched one.
        let guard = EvalPanicCaptureGuard::arm();
        let payload = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| {
                program.nodes[node_idx].eval(
                    &self.input_scratch[..input_count],
                    &mut self.buffers[node_idx],
                );
            })
        );
        drop(guard);
        if let Err(e) = payload {
            let enriched = enrich_eval_panic(
                e, program, node_idx,
                &self.input_scratch[..input_count],
            );
            if PANIC_REPORTING_DOWNSTREAM.load(std::sync::atomic::Ordering::Relaxed) {
                RERAISE_SHORT.with(|c| c.set(true));
            }
            std::panic::panic_any(enriched);
        }
        self.node_clean[node_idx] = true;
    }

    /// Pull a named output.
    pub fn pull(&mut self, program: &PolydatProgram, output_name: &str) -> &Value {
        let (node_idx, port_idx) = *program.output_map
            .get(output_name)
            .unwrap_or_else(|| panic!("unknown output variate: {output_name}"));
        self.eval_node(program, node_idx);
        // SRD-13f Push B.2: broadcast the freshly computed
        // value through this output's cell (if attached) so
        // descendant kernels that bound their matching input
        // slot to the same cell see the current value on
        // their next read.
        if let Some(output_idx) = program.output_index(output_name)
            && let Some(Some(cell)) = self.output_cells.get(output_idx)
        {
            let v = self.buffers[node_idx][port_idx].clone();
            // `publish` does the mutex write + revision bump +
            // intent-bit set in three Release stores so the
            // descendant's cone walker observes the change on
            // its next read (cross_fiber_invalidation.md §5).
            cell.publish(v);
        }
        &self.buffers[node_idx][port_idx]
    }

    /// SRD-13f Push B.2 — allocate broadcast cells for every
    /// output in `program`. Idempotent: if cells are already
    /// allocated (size matches the program's output count),
    /// the call is a no-op. Initial cell value is taken from
    /// the current buffer (typically `Value::None` at
    /// construction, before any pull has fired).
    ///
    /// Called from kernel constructors and from
    /// `materialize_wiring_from_outer`-style operations that materialize
    /// new descendants — the inner side needs the cell to
    /// exist before it can attach to its input slot.
    pub(crate) fn seed_output_cells(&mut self, program: &PolydatProgram) {
        let n = program.output_names().len();
        if self.output_cells.len() == n { return; }
        // Two-pass to avoid borrowing `self` immutably (for
        // buffer lookups) while also borrowing it mutably (for
        // `make_shared_cell`). First collect initial values,
        // then construct the cells.
        let initials: Vec<Value> = (0..n).map(|i| {
            let name = &program.output_list()[i].0;
            let (node_idx, port_idx) = program.output_map[name];
            // Defensive bounds-check: some construction paths
            // (raw state, partial programs) may not populate
            // buffers for every node referenced in the output
            // map. Seed with `Value::None` rather than panic.
            self.buffers.get(node_idx)
                .and_then(|b| b.get(port_idx))
                .cloned()
                .unwrap_or(Value::None)
        }).collect();
        self.output_cells = initials.into_iter()
            .map(|init| Some(self.make_shared_cell(init)))
            .collect();
    }

    /// Output broadcast cell for the named output, if seeded.
    pub(crate) fn output_cell(&self, program: &PolydatProgram, name: &str) -> Option<SharedCell> {
        let idx = program.output_index(name)?;
        self.output_cells.get(idx).and_then(|c| c.clone())
    }
}

// =================================================================
// PolydatState: dependent-list engine (default, O(affected) invalidation)
// =================================================================

/// Polydat evaluation engine using precomputed per-input dependent lists.
///
/// On `set_input()`, only nodes that depend on the changed input
/// are dirtied. O(affected_nodes) per input change.
/// This is the default engine for production use.
pub struct PolydatState {
    /// Shared evaluation core (buffers, clean flags, inputs).
    pub core: EngineCore,
    /// Per-input dependent node lists for O(affected) invalidation.
    input_dependents: Vec<Vec<usize>>,
    /// Indices of non-deterministic nodes (zero-provenance, no declared inputs).
    ///
    /// These nodes produce a different value on every evaluation (e.g.,
    /// `counter()`, `current_epoch_millis()`). They are unconditionally
    /// marked dirty on every `set_input()` call so they are never cached.
    nondeterministic_nodes: Vec<usize>,
}

impl PolydatState {
    /// Construct a PolydatState from its component parts.
    pub(crate) fn from_parts(
        core: EngineCore,
        input_dependents: Vec<Vec<usize>>,
        nondeterministic_nodes: Vec<usize>,
    ) -> Self {
        Self { core, input_dependents, nondeterministic_nodes }
    }

    /// Set all coordinate inputs at once (convenience for the common
    /// single-cycle case). Wraps each u64 as `Value::U64` and sets
    /// them at indices 0..N with per-input change detection.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        for (i, &c) in coords.iter().enumerate().take(self.core.inputs.len()) {
            self.core.inputs[i] = Value::U64(c);
            // Unconditional invalidation: the write itself is the
            // signal — see `set_input` for the rationale.
            if i < self.input_dependents.len() {
                for &node_idx in &self.input_dependents[i] {
                    self.core.node_clean[node_idx] = false;
                }
            }
        }
        // Non-deterministic nodes must always re-evaluate.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Set a single input by index, dirtying only dependent nodes.
    ///
    /// Single-register semantics: a cell-bound slot's only
    /// register IS the cell — `set_input` writes through the
    /// cell. A non-cell slot's register is the local
    /// `inputs[idx]` array. There's no second snapshot kept in
    /// lockstep with the cell; reads always go to whichever is
    /// the slot's register.
    ///
    /// Dependents-marking is the dependent-list invalidation
    /// strategy carried by `PolydatState`; it's the write-side
    /// half of the engine's dirty-tracking. Other engines
    /// (`RawState`, `ProvScanState`) implement different
    /// strategies — see their own `set_inputs` impls.
    pub fn set_input(&mut self, idx: usize, value: Value) {
        if let Some(cell) = self.core.shared_cells.get(idx).and_then(|c| c.as_ref()) {
            // Cell-bound slot: the cell is the register. We do
            // NOT mirror the value into `inputs[idx]`; that
            // array slot is unused for cell-bound inputs.
            //
            // `publish` does the mutex write + revision bump +
            // intent-bit set in three Release stores so the
            // any other fiber's cone walker observes the
            // change on its next read
            // (cross_fiber_invalidation.md §5).
            cell.publish(value);
        } else {
            self.core.inputs[idx] = value;
        }
        // Mark every transitive dependent dirty unconditionally.
        // The act of writing an input IS the invalidation
        // signal — we don't gate on value equality because (a)
        // structural equality on rich Value variants
        // (Json/Bytes/VecF32) is expensive enough to defeat
        // the purpose of the optimisation, and (b) a same-
        // value rewrite is still a legitimate "the upstream
        // owner asked for a re-evaluation" signal that
        // downstream side-effecting nodes (`log_*`, audit
        // emitters, time-stamped observers) MUST honour.
        let dirty_debug = nbrs_dirty_debug_enabled();
        if idx < self.input_dependents.len() {
            if dirty_debug {
                eprintln!(
                    "DIRTY: set_input idx={idx} input_count={} dependents_for_idx={} \
                     total_input_dependents_len={}",
                    self.core.inputs.len(),
                    self.input_dependents[idx].len(),
                    self.input_dependents.len()
                );
            }
            for &node_idx in &self.input_dependents[idx] {
                self.core.node_clean[node_idx] = false;
            }
        } else if dirty_debug {
            eprintln!(
                "DIRTY: set_input idx={idx} OUT_OF_RANGE input_dependents_len={}",
                self.input_dependents.len()
            );
        }
        // Non-deterministic nodes must always re-evaluate.
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Read the value of an input by index.
    ///
    /// Single-register read: cell-bound slots return the cell's
    /// current value; non-cell slots return the local register.
    /// One canonical value per slot, no stale snapshot.
    pub fn get_input(&self, idx: usize) -> Value {
        self.core.read_input(idx)
    }

    /// Alias for [`Self::get_input`]; kept for legacy callers
    /// that picked the more explicit name. Both read the cell
    /// when one is attached.
    pub fn read_input_value(&self, idx: usize) -> Value {
        self.core.read_input(idx)
    }

    /// Attach a `SharedCell` to an input slot.
    ///
    /// After this call the cell becomes the slot's sole
    /// register: reads via `read_input` go through the cell,
    /// `set_input` writes through the cell. The local
    /// `inputs[idx]` array entry for this slot is unused for
    /// cell-bound slots — there is no second register kept in
    /// lockstep.
    ///
    /// Dependents are dirtied because the slot's effective
    /// value just changed from the local default to whatever
    /// the cell currently holds.
    pub fn attach_shared_cell(&mut self, idx: usize, cell: SharedCell) {
        if idx >= self.core.shared_cells.len() {
            self.core.shared_cells.resize(idx + 1, None);
        }
        self.core.shared_cells[idx] = Some(cell);
        if idx < self.input_dependents.len() {
            for &node_idx in &self.input_dependents[idx] {
                self.core.node_clean[node_idx] = false;
            }
        }
        // Cone metadata depends on which slots have cells; the
        // new attachment invalidates any cached cone groups.
        // Next `check_cell_clean` per node rebuilds on demand
        // per cross_fiber_invalidation.md §3.1.
        self.core.invalidate_cell_cones();
    }

    /// Returns the `SharedCell` attached to an input slot, if any.
    /// Used by `materialize_wiring_from_outer` to share an existing cell with
    /// inner kernels.
    pub fn shared_cell(&self, idx: usize) -> Option<SharedCell> {
        self.core.shared_cells.get(idx).and_then(|c| c.clone())
    }

    /// Reset a range of inputs to their defaults. Used at stanza
    /// boundaries to prevent capture leakage across stanzas.
    /// `from_idx` is typically `coord_count` (skip coordinates,
    /// reset only capture inputs).
    ///
    /// Cell-bound slots are skipped: the cell is cross-kernel
    /// shared state with its own lifecycle (managed by the
    /// owning ancestor scope), and a stanza-local reset must
    /// not clobber other kernels' views.
    pub fn reset_inputs_from(&mut self, from_idx: usize) {
        for i in from_idx..self.core.inputs.len() {
            // Cell-bound slots: the cell is the register, owned
            // by the ancestor that declared `shared X := init`.
            // Don't touch.
            if self.core.shared_cells.get(i).is_some_and(|c| c.is_some()) {
                continue;
            }
            if self.core.inputs[i] != self.core.input_defaults[i] {
                self.core.inputs[i] = self.core.input_defaults[i].clone();
                if i < self.input_dependents.len() {
                    for &node_idx in &self.input_dependents[i] {
                        self.core.node_clean[node_idx] = false;
                    }
                }
            }
        }
    }

    /// Invalidate all state: reset all inputs to defaults and mark
    /// every node dirty. Provides "clean slate" semantics.
    pub fn invalidate_all(&mut self) {
        self.core.inputs.clone_from_slice(&self.core.input_defaults);
        for clean in &mut self.core.node_clean {
            *clean = false;
        }
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &PolydatProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }

    /// Pre-populate a node's output buffer slot and mark it clean,
    /// suppressing on-demand evaluation. Used by the scope-init
    /// pass (SRD 11 §"Init Binding Contract" Plan B) to seed
    /// per-fiber states with init binding values that the
    /// activation kernel already evaluated, so each fiber doesn't
    /// re-fire the eval at first pull.
    pub fn seed_node_buffer(&mut self, node_idx: usize, port_idx: usize, value: Value) {
        if node_idx >= self.core.buffers.len() { return; }
        if port_idx >= self.core.buffers[node_idx].len() { return; }
        self.core.buffers[node_idx][port_idx] = value;
        self.core.node_clean[node_idx] = true;
    }

    /// Read a node's output buffer slot. Used by the scope-init
    /// pass to extract a pre-pulled init binding value from one
    /// state and seed it into another.
    pub fn node_buffer(&self, node_idx: usize, port_idx: usize) -> Option<&Value> {
        self.core.buffers.get(node_idx)
            .and_then(|ports| ports.get(port_idx))
    }

    /// Pull an output by index (declaration order). Only evaluates
    /// the computation cone for this specific output.
    pub fn pull_by_index(&mut self, program: &PolydatProgram, output_idx: usize) -> &Value {
        let (node_idx, port_idx) = program.resolve_output_by_index(output_idx);
        self.core.eval_node(program, node_idx);
        &self.core.buffers[node_idx][port_idx]
    }

    /// Pull all outputs in declaration order.
    pub fn pull_all<'a>(&'a mut self, program: &PolydatProgram) -> Vec<&'a Value> {
        for i in 0..program.output_count() {
            let (node_idx, _) = program.resolve_output_by_index(i);
            self.core.eval_node(program, node_idx);
        }
        (0..program.output_count())
            .map(|i| {
                let (ni, pi) = program.resolve_output_by_index(i);
                &self.core.buffers[ni][pi]
            })
            .collect()
    }

    /// Create a memoized accessor for a named subset of outputs.
    /// Resolves names to indices once; subsequent access uses indices only.
    pub fn accessor(program: &PolydatProgram, names: &[&str]) -> OutputAccessor {
        let indices: Vec<usize> = names.iter()
            .filter_map(|n| program.output_index(n))
            .collect();
        OutputAccessor { indices }
    }

    /// Evaluate a node by index (exposed for constant folding in PolydatProgram).
    pub(crate) fn eval_node_public(&mut self, program: &PolydatProgram, node_idx: usize) {
        self.core.eval_node(program, node_idx);
    }
}

/// Memoized output accessor for a named subset of outputs.
///
/// Created once from output names via `PolydatState::accessor()`.
/// Subsequent pulls use pre-resolved indices — no name lookups.
pub struct OutputAccessor {
    indices: Vec<usize>,
}

impl OutputAccessor {
    /// Pull all outputs in this accessor from the given state.
    pub fn pull_all<'a>(&self, state: &'a mut PolydatState, program: &PolydatProgram) -> Vec<&'a Value> {
        for &idx in &self.indices {
            let (node_idx, _) = program.resolve_output_by_index(idx);
            state.core.eval_node(program, node_idx);
        }
        self.indices.iter()
            .map(|&idx| {
                let (ni, pi) = program.resolve_output_by_index(idx);
                &state.core.buffers[ni][pi]
            })
            .collect()
    }

    /// Number of outputs in this accessor.
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Whether this accessor has no outputs.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

// =================================================================
// RawState: no provenance engine (all nodes dirty every eval)
// =================================================================

/// Polydat evaluation engine with no provenance. Every `set_inputs()`
/// marks all nodes dirty. Baseline for benchmarking provenance overhead.
pub struct RawState {
    /// Shared evaluation core.
    pub core: EngineCore,
}

impl RawState {
    /// Set new input values and mark all nodes dirty (no provenance check).
    pub fn set_inputs(&mut self, coords: &[u64]) {
        for (i, &c) in coords.iter().enumerate().take(self.core.inputs.len()) {
            self.core.inputs[i] = Value::U64(c);
        }
        for clean in &mut self.core.node_clean {
            *clean = false;
        }
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &PolydatProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }
}

// =================================================================
// ProvScanState: provenance-scan engine (O(all) invalidation)
// =================================================================

/// Polydat evaluation engine using provenance bitmask scanning.
///
/// On `set_inputs()`, scans ALL nodes and checks each node's
/// provenance bitmask against the changed-inputs mask.
/// O(all_nodes) per input change regardless of how many changed.
pub struct ProvScanState {
    /// Shared evaluation core.
    pub core: EngineCore,
    input_provenance: Vec<crate::kernel::ProvMask>,
    /// Indices of non-deterministic nodes.
    nondeterministic_nodes: Vec<usize>,
}

impl ProvScanState {
    /// Construct a ProvScanState from its component parts.
    pub(crate) fn from_parts(
        core: EngineCore,
        input_provenance: Vec<crate::kernel::ProvMask>,
        nondeterministic_nodes: Vec<usize>,
    ) -> Self {
        Self { core, input_provenance, nondeterministic_nodes }
    }

    /// Set new input values and invalidate affected nodes.
    pub fn set_inputs(&mut self, coords: &[u64]) {
        let mut mask = crate::kernel::ProvMask::empty();
        for (i, &c) in coords.iter().enumerate().take(self.core.inputs.len()) {
            self.core.inputs[i] = Value::U64(c);
            // Unconditional: writing the input IS the
            // invalidation signal regardless of value equality.
            mask.set(i);
        }
        if !mask.is_zero() {
            for (i, clean) in self.core.node_clean.iter_mut().enumerate() {
                if *clean && self.input_provenance[i].intersects(&mask) {
                    *clean = false;
                }
            }
        }
        for &idx in &self.nondeterministic_nodes {
            self.core.node_clean[idx] = false;
        }
    }

    /// Pull a named output variate from the program.
    pub fn pull(&mut self, program: &PolydatProgram, output_name: &str) -> &Value {
        self.core.pull(program, output_name)
    }
}

#[cfg(test)]
mod panic_enrichment_tests {
    //! Verify the eval-panic enricher produces a usable diagnostic
    //! when a node panics on a type mismatch (the "expected U64,
    //! got Str" surface that operators see in the wild).

    use crate::dsl::compile::compile_polydat_with_libs;

    #[test]
    fn type_mismatch_panic_carries_node_and_output_context() {
        // Declare the input as u64, then write a Str into the
        // slot — `mul`'s u64 path will panic on `as_u64()`.
        // The enricher must wrap the message with the node
        // name + output name + program context.
        let mut k = compile_polydat_with_libs(
            "extern x: u64\n\
             doubled := mul(x, 2)\n",
            None, vec![], &[], false, "test_workload",
        ).expect("compile");
        let idx = k.program().find_input("x").unwrap();
        k.state().set_input(idx, crate::ast::Value::Str("oops".into()));
        let result = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| { k.pull("doubled"); })
        );
        let err = result.expect_err("pull should panic on type mismatch");
        let msg = err.downcast_ref::<String>().cloned()
            .or_else(|| err.downcast_ref::<&'static str>().map(|s| (*s).to_string()))
            .expect("panic payload should be a String");
        assert!(msg.contains("expected U64"),
            "missing original panic body in: {msg}");
        assert!(msg.contains("`mul`"),
            "missing node name in enriched message: {msg}");
        assert!(msg.contains("doubled"),
            "missing output binding in enriched message: {msg}");
        assert!(msg.contains("test_workload"),
            "missing program context in enriched message: {msg}");
        assert!(msg.contains("\"oops\""),
            "missing input snapshot in enriched message: {msg}");
        // The suppression hook must have captured the ORIGINAL
        // panic site (Value::as_u64 in ast.rs) — not the
        // re-raise site in engines.rs.
        assert!(msg.contains("panicked at") && msg.contains("ast.rs"),
            "missing original panic location in enriched message: {msg}");
        // Surface the full enriched message in `cargo test --
        // --nocapture` runs so the format is easy to eyeball.
        eprintln!("== enriched message ==\n{msg}\n======================");
    }
}
