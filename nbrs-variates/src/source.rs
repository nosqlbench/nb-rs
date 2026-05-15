// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Data sources: typed sequences that drive workload iteration.
//!
//! A **source** is a data provider with identity. It knows what it yields
//! (schema), how much it has (extent), where the consumer is (cursor),
//! and how to partition across concurrent fibers.
//!
//! Sources replace the `cycles` counter as the workload iteration driver.
//! The GK graph declares sources via the `source` keyword. The runtime
//! pulls from sources to drive op dispatch. When a source is exhausted,
//! the phase is done.
//!
//! ## Source Types
//!
//! - **Range**: `range(0, N)` — finite sequence of ordinals. Replaces `cycles: N`.
//! - **Dataset**: `dataset_source("example:label_00", "base")` — vectors, queries, etc.
//! - **Derived**: any GK binding promoted to a source via the `source` keyword.
//!
//! ## Crate Sovereignty
//!
//! All source API surface lives here in nbrs-variates. The runtime crates
//! (nbrs-activity, adapters) consume these types but don't define them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::{PortType, Value};

/// A single item yielded by a source.
#[derive(Clone, Debug)]
pub struct SourceItem {
    /// Position in the source sequence.
    pub ordinal: u64,
    /// Named field values. Empty for range sources (ordinal IS the data).
    /// For dataset sources: `[("vector", Value::Json(...)), ("metadata", Value::U64(...))]`.
    pub fields: Vec<(String, Value)>,
}

impl SourceItem {
    /// Create a range item (ordinal only, no fields).
    pub fn ordinal(ordinal: u64) -> Self {
        Self { ordinal, fields: Vec::new() }
    }

    /// Create an item with ordinal and named fields.
    pub fn with_fields(ordinal: u64, fields: Vec<(String, Value)>) -> Self {
        Self { ordinal, fields }
    }

    /// Get a field value by name.
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }
}

/// Schema describing what a source yields.
#[derive(Clone, Debug)]
pub struct SourceSchema {
    /// Source name as declared in the GK graph.
    pub name: String,
    /// Field names and types available for projection (e.g., `ordinal: U64`, `vector: Json`).
    pub projections: Vec<(String, PortType)>,
    /// Known extent, if finite. `None` for infinite sources.
    ///
    /// `None` may also mean the extent is computable but only at runtime
    /// (e.g., the cursor's `range(...)` bounds depend on iteration-variable
    /// externs). In that case `extent_outputs` carries the kernel output
    /// names whose values yield `[start, end)` once externs are bound.
    pub extent: Option<u64>,
    /// Optional aux output names `(start, end)` that the runtime can pull
    /// from the kernel after externs are populated to compute extent. Set
    /// when `range(...)` bounds are non-literal (e.g., wire-bound dataset
    /// function calls). The compiler also writes back to `extent` if both
    /// values fold to constants at compile time.
    pub extent_outputs: Option<(String, String)>,
    /// Optional cursor limit clamp (from `--limit` / `limit=` param).
    /// Applied after runtime extent evaluation.
    pub extent_limit: Option<u64>,
    /// What kind of cursor this is. Default `Range` (the legacy
    /// bounded-finite cursor); `ExtendingTimed { ... }` declares
    /// the cursor as runtime-extending under a wall-clock
    /// minimum-duration policy. The executor branches on this
    /// to pick between `RangeSourceFactory` and
    /// `ExtendingRangeSourceFactory` at phase setup.
    pub cursor_kind: CursorKind,
}

/// Cursor-construction discriminator. Set by the GK compiler
/// when it recognises a cursor's constructor expression
/// (`range(...)`, `until_elapsed(...)`, etc.); read by the
/// executor at phase setup to instantiate the matching
/// data-source factory + policy.
///
/// Each `Extending*` variant carries the GK output names the
/// runtime should pull at phase setup to resolve the policy's
/// parameters (same pattern as `extent_outputs` for `range`).
/// `delta_output` is optional — when `None`, the cursor's base
/// is used as the extension delta.
#[derive(Clone, Debug, Default)]
pub enum CursorKind {
    /// Bounded `range(start, end)` cursor. Default — keeps
    /// every existing call site working.
    #[default]
    Range,
    /// `until_elapsed(base, min_ms[, delta])`.
    /// Extends while elapsed_ms < min_ms.
    ExtendingTimed {
        min_ms_output: String,
        delta_output: Option<String>,
    },
    /// `until_passes(base, min_passes[, delta])`.
    /// Extends while completed passes < min_passes.
    ExtendingPasses {
        min_passes_output: String,
        delta_output: Option<String>,
    },
    /// `until_count(base, min_count[, delta])`.
    /// Extends while raw consumed count < min_count.
    ExtendingCount {
        min_count_output: String,
        delta_output: Option<String>,
    },
    /// `until_elapsed_and_passes(base, min_ms, min_passes[, delta])`.
    /// AND: stops when EITHER target is reached. Extends while
    /// BOTH conditions are still below target.
    ExtendingElapsedAndPasses {
        min_ms_output: String,
        min_passes_output: String,
        delta_output: Option<String>,
    },
    /// `until_elapsed_or_passes(base, min_ms, min_passes[, delta])`.
    /// OR: stops only when BOTH targets are reached. Extends
    /// while EITHER condition is still below target.
    ExtendingElapsedOrPasses {
        min_ms_output: String,
        min_passes_output: String,
        delta_output: Option<String>,
    },
}

/// Consumption API for data sources. One instance per fiber.
///
/// The interaction model has two phases:
///
/// 1. **Reserve** — `reserve(stride)` atomically claims a range of
///    ordinals via CAS on the shared cursor. This touches shared
///    state but is instantaneous (one atomic op). Returns `None`
///    when the source is globally exhausted.
///
/// 2. **Render** — the fiber uses the reserved range with its own
///    GK instance to produce field values. No shared state, no
///    contention between fibers. For range sources, rendering is
///    trivial (ordinal IS the data). For dataset sources, rendering
///    reads vectors/metadata from mmap'd storage.
///
/// The `next_chunk` convenience method combines both phases. Use
/// `reserve` directly when the rendering is handled by the
/// executor's GK fiber.
pub trait DataSource: Send {
    /// Atomically reserve up to `stride` ordinals from the source.
    ///
    /// Returns the half-open range `[start..end)` of reserved
    /// ordinals, or `None` if the source is exhausted. The range
    /// may be shorter than `stride` at the tail of the source.
    ///
    /// This is the only method that touches shared state (the
    /// global cursor). It must be lock-free — a single CAS or
    /// fetch_add.
    fn reserve(&mut self, stride: usize) -> Option<std::ops::Range<u64>>;

    /// Pull the next item. `None` = source exhausted.
    fn next(&mut self) -> Option<SourceItem> {
        let range = self.reserve(1)?;
        Some(self.render_item(range.start))
    }

    /// Pull up to `limit` items. Combines reserve + render.
    /// Returns fewer than `limit` only when the source is globally
    /// exhausted. Empty vec = exhausted.
    fn next_chunk(&mut self, limit: usize) -> Vec<SourceItem> {
        let range = match self.reserve(limit) {
            Some(r) => r,
            None => return Vec::new(),
        };
        (range.start..range.end)
            .map(|ordinal| self.render_item(ordinal))
            .collect()
    }

    /// Produce a source item for a previously reserved ordinal.
    ///
    /// This is the fiber-local rendering step — no shared state.
    /// For range sources: returns `SourceItem::ordinal(ordinal)`.
    /// For dataset sources: reads vector/metadata from storage.
    fn render_item(&self, ordinal: u64) -> SourceItem;

    /// Known extent, if finite.
    fn extent(&self) -> Option<u64>;

    /// Items consumed so far (for progress reporting).
    fn consumed(&self) -> u64;

    /// The schema of items this source yields.
    fn schema(&self) -> &SourceSchema;
}

/// Factory that creates per-fiber `DataSource` readers.
///
/// Holds shared state (atomic cursor, partition pool) that's
/// distributed across readers. Each fiber gets its own reader.
///
/// ## Dispatch model
///
/// The **stride** is the stanza length — the number of source items
/// a fiber acquires as an atomic unit. One stanza of ops processes
/// one stride of source items. Strides are inseparable: a fiber
/// that acquires a stride processes all items before acquiring the
/// next.
///
/// The default implementation (`RangeSourceFactory`) uses a shared
/// atomic cursor — all fibers pull strides from the same counter,
/// producing natural monotonic striping. This is correct for range
/// sources where items are independent ordinals.
///
/// For dataset sources with locality benefits (mmap prefetch),
/// factories can implement partitioned allocation: each fiber gets
/// a pre-assigned range of strides, and when exhausted, steals
/// strides from a shared pool. The stride is the minimum unit of
/// work stealing — a fiber never steals partial stanzas.
pub trait DataSourceFactory: Send + Sync {
    /// Create a new reader for a fiber.
    fn create_reader(&self) -> Box<dyn DataSource>;

    /// Schema for all readers from this factory.
    fn schema(&self) -> &SourceSchema;

    /// Global items consumed across all readers (for progress reporting).
    fn global_consumed(&self) -> u64;

    /// Known extent, if finite. Same as schema().extent but avoids clone.
    fn global_extent(&self) -> Option<u64> {
        self.schema().extent
    }
}

// =========================================================================
// RangeSource: finite sequence of ordinals
// =========================================================================

/// Factory for range sources. Shared atomic cursor distributes
/// ordinals across fibers. Replaces `CycleSource`.
pub struct RangeSourceFactory {
    cursor: Arc<AtomicU64>,
    end: u64,
    schema: SourceSchema,
}

impl RangeSourceFactory {
    /// Create a range source from `[start, end)`.
    pub fn new(start: u64, end: u64) -> Self {
        Self {
            cursor: Arc::new(AtomicU64::new(start)),
            end,
            schema: SourceSchema {
                name: "_range".into(),
                projections: vec![("ordinal".into(), PortType::U64)],
                extent: Some(end.saturating_sub(start)),
                extent_outputs: None,
                extent_limit: None,
                cursor_kind: CursorKind::Range,
            },
        }
    }

    /// Create a range source with a named schema.
    pub fn named(name: &str, start: u64, end: u64) -> Self {
        let mut factory = Self::new(start, end);
        factory.schema.name = name.to_string();
        factory
    }
}

impl DataSourceFactory for RangeSourceFactory {
    fn create_reader(&self) -> Box<dyn DataSource> {
        Box::new(RangeSource {
            cursor: self.cursor.clone(),
            end: self.end,
            consumed: 0,
            schema: self.schema.clone(),
        })
    }

    fn schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn global_consumed(&self) -> u64 {
        let pos = self.cursor.load(Ordering::Relaxed);
        let start = self.end.saturating_sub(self.schema.extent.unwrap_or(0));
        pos.saturating_sub(start).min(self.schema.extent.unwrap_or(u64::MAX))
    }
}

/// Per-fiber range reader. Pulls ordinals from a shared atomic cursor.
struct RangeSource {
    cursor: Arc<AtomicU64>,
    end: u64,
    consumed: u64,
    schema: SourceSchema,
}

impl DataSource for RangeSource {
    fn reserve(&mut self, stride: usize) -> Option<std::ops::Range<u64>> {
        let base = self.cursor.fetch_add(stride as u64, Ordering::Relaxed);
        if base >= self.end {
            return None;
        }
        let actual_end = (base + stride as u64).min(self.end);
        let count = actual_end - base;
        self.consumed += count;
        Some(base..actual_end)
    }

    fn render_item(&self, ordinal: u64) -> SourceItem {
        SourceItem::ordinal(ordinal)
    }

    fn extent(&self) -> Option<u64> {
        self.schema.extent
    }

    fn consumed(&self) -> u64 {
        self.consumed
    }

    fn schema(&self) -> &SourceSchema {
        &self.schema
    }
}

// =========================================================================
// ExtendingRangeSource: runtime-growable extent
// =========================================================================

/// Snapshot of cursor state passed to an [`ExtensionPolicy`]
/// at each end-reach decision point. Policies are pure
/// predicates over this context — they don't carry their own
/// clocks or counters.
#[derive(Clone, Copy, Debug)]
pub struct ExtensionContext {
    /// Wall-clock milliseconds since the source factory was
    /// constructed (typically phase start).
    pub elapsed_ms: u64,
    /// Global ordinals consumed so far — `cursor.load() - start`.
    /// Pass count is `consumed / base`.
    pub consumed: u64,
    /// The `base` chunk size declared by the cursor. Used by
    /// policies that reason in passes rather than raw counts.
    pub base: u64,
}

impl ExtensionContext {
    /// Convenience: integer pass count (consumed / base).
    /// Returns 0 when `base == 0` (degenerate cursor).
    pub fn passes(&self) -> u64 {
        if self.base == 0 { 0 } else { self.consumed / self.base }
    }
}

/// Policy that decides whether to extend an
/// [`ExtendingRangeSource`] when its current end is reached.
///
/// Implementations are pure predicates over the
/// [`ExtensionContext`] — no internal state, no side effects.
/// The source provides elapsed time and consumed counts; the
/// policy returns `Some(delta)` to grow the extent or `None`
/// to terminate. Cheap-and-possibly-duplicated call contract
/// (concurrent fibers may invoke under racing end-reach; only
/// one CAS-wins extension takes effect per round).
pub trait ExtensionPolicy: Send + Sync {
    /// Decide how to extend (if at all) given the current
    /// cursor context.
    fn next_extension(&self, ctx: &ExtensionContext) -> Option<u64>;
}

/// Factory for ExtendingRangeSource. Differs from
/// [`RangeSourceFactory`] in that `end` is an atomic that the
/// extension policy may grow over the lifetime of the phase.
/// `global_extent()` returns the CURRENT end so phase-status
/// displays reflect any growth honestly.
pub struct ExtendingRangeSourceFactory {
    cursor: Arc<AtomicU64>,
    end: Arc<AtomicU64>,
    start: u64,
    /// Per-pass chunk size — also the default extension delta
    /// when the policy reports "continue". Exposed to the
    /// policy via `ExtensionContext::base` so pass-count
    /// predicates work.
    base: u64,
    /// Wall-clock baseline. Captured at factory construction
    /// so per-phase factories yield per-phase elapsed numbers
    /// without external clock plumbing.
    started: std::time::Instant,
    policy: Arc<dyn ExtensionPolicy>,
    schema: SourceSchema,
}

impl ExtendingRangeSourceFactory {
    /// Build with an initial extent `[start, start + initial_extent)`.
    /// The extension policy is consulted only when the cursor
    /// reaches the end — the first stride consumed produces
    /// indices in the initial range.
    pub fn new(
        name: &str,
        start: u64,
        initial_extent: u64,
        policy: Arc<dyn ExtensionPolicy>,
    ) -> Self {
        let end = start.saturating_add(initial_extent);
        Self {
            cursor: Arc::new(AtomicU64::new(start)),
            end: Arc::new(AtomicU64::new(end)),
            start,
            base: initial_extent,
            started: std::time::Instant::now(),
            policy,
            schema: SourceSchema {
                name: name.to_string(),
                projections: vec![("ordinal".into(), PortType::U64)],
                extent: Some(initial_extent),
                extent_outputs: None,
                cursor_kind: CursorKind::Range,
                extent_limit: None,
            },
        }
    }
}

impl DataSourceFactory for ExtendingRangeSourceFactory {
    fn create_reader(&self) -> Box<dyn DataSource> {
        Box::new(ExtendingRangeSource {
            cursor: self.cursor.clone(),
            end: self.end.clone(),
            policy: self.policy.clone(),
            start: self.start,
            base: self.base,
            started: self.started,
            consumed: 0,
            schema: self.schema.clone(),
        })
    }

    fn schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn global_consumed(&self) -> u64 {
        let pos = self.cursor.load(Ordering::Relaxed);
        pos.saturating_sub(self.start)
    }

    fn global_extent(&self) -> Option<u64> {
        // Live extent — readers / status displays see growth.
        Some(self.end.load(Ordering::Acquire)
            .saturating_sub(self.start))
    }
}

/// Per-fiber reader for an extending range source. Reservation
/// uses CAS so concurrent fibers race cleanly without lock
/// contention; on end-reach the policy is consulted (possibly
/// duplicated under concurrent end-reach), and the end atomic
/// is grown via a single CAS (only one extension takes effect
/// per round; losers retry and see the new end).
struct ExtendingRangeSource {
    cursor: Arc<AtomicU64>,
    end: Arc<AtomicU64>,
    policy: Arc<dyn ExtensionPolicy>,
    start: u64,
    base: u64,
    started: std::time::Instant,
    consumed: u64,
    schema: SourceSchema,
}

impl DataSource for ExtendingRangeSource {
    fn reserve(&mut self, stride: usize) -> Option<std::ops::Range<u64>> {
        loop {
            let cur = self.cursor.load(Ordering::Acquire);
            let end = self.end.load(Ordering::Acquire);
            if cur < end {
                // Try to claim [cur, min(cur+stride, end)).
                let target = (cur.saturating_add(stride as u64)).min(end);
                match self.cursor.compare_exchange(
                    cur, target,
                    Ordering::AcqRel, Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let count = target - cur;
                        self.consumed += count;
                        return Some(cur..target);
                    }
                    Err(_) => continue, // raced; retry
                }
            }
            // Cursor has caught up to end. Consult policy with
            // a snapshot of the current state. Each fiber that
            // races to this point gets its own context read;
            // duplicate consultations are idempotent for pure
            // predicates.
            let ctx = ExtensionContext {
                elapsed_ms: self.started.elapsed().as_millis() as u64,
                consumed: end.saturating_sub(self.start),
                base: self.base,
            };
            match self.policy.next_extension(&ctx) {
                Some(delta) if delta > 0 => {
                    // CAS the end forward. If someone else extended
                    // ahead of us, that's fine — our duplicate
                    // policy consultation is harmless and the new
                    // end is at least as far as ours would have
                    // been.
                    let _ = self.end.compare_exchange(
                        end, end.saturating_add(delta),
                        Ordering::AcqRel, Ordering::Acquire,
                    );
                    continue;
                }
                _ => return None,
            }
        }
    }

    fn render_item(&self, ordinal: u64) -> SourceItem {
        SourceItem::ordinal(ordinal)
    }

    fn extent(&self) -> Option<u64> {
        // Live extent — same convention as the factory's
        // `global_extent`: subscribers see the current ceiling
        // even after it grows.
        Some(self.end.load(Ordering::Acquire)
            .saturating_sub(self.start))
    }

    fn consumed(&self) -> u64 {
        self.consumed
    }

    fn schema(&self) -> &SourceSchema {
        &self.schema
    }
}

// =========================================================================
// Cursors: provenance-driven cursor targeting
// =========================================================================

/// A cursor target: a DataSource reader paired with its GK input index.
struct CursorTarget {
    /// The DataSource reader that provides values for this cursor.
    reader: Box<dyn DataSource>,
    /// The GK input index where the cursor's ordinal is injected.
    input_index: usize,
    /// Source name (for diagnostics).
    #[allow(dead_code)]
    source_name: String,
}

/// Provenance-driven advancer that targets only the cursor nodes
/// relevant to a specific set of output fields.
///
/// Built at phase setup by tracing GK provenance from the op template's
/// referenced fields back to root cursor nodes. Only those cursors
/// advance — unused cursors are left untouched.
pub struct Cursors {
    targets: Vec<CursorTarget>,
    /// Last items read from each target (for injecting into GK state).
    last_items: Vec<Option<SourceItem>>,
    /// Total advances performed.
    advances: u64,
}

impl Cursors {
    /// Build an advancer from a GK program, a set of output field names,
    /// and a map of source name → DataSourceFactory.
    ///
    /// Traces provenance: for each field, finds the output node, gets its
    /// input provenance bitmask, and identifies which inputs are cursor
    /// ordinals (matching `{source}__ordinal` pattern). Creates a reader
    /// for each targeted source.
    pub fn for_fields(
        program: &crate::kernel::GkProgram,
        field_names: &[&str],
        source_factories: &std::collections::HashMap<String, Arc<dyn DataSourceFactory>>,
    ) -> Self {
        // Collect the union of input provenance for all referenced fields
        let mut combined_provenance: u64 = 0;
        for name in field_names {
            if let Some((node_idx, _)) = program.resolve_output(name) {
                combined_provenance |= program.input_provenance_for(node_idx);
            }
        }

        // Find cursor inputs in the provenance
        let input_names = program.input_names();
        let mut targets = Vec::new();
        let mut seen_sources = std::collections::HashSet::new();

        for (idx, input_name) in input_names.iter().enumerate() {
            if combined_provenance & (1u64 << idx) == 0 { continue; }

            // Check if this input is a source projection ({source}__ordinal)
            if let Some(source_name) = input_name.strip_suffix("__ordinal") {
                if seen_sources.contains(source_name) { continue; }
                seen_sources.insert(source_name.to_string());

                if let Some(factory) = source_factories.get(source_name) {
                    targets.push(CursorTarget {
                        reader: factory.create_reader(),
                        input_index: idx,
                        source_name: source_name.to_string(),
                    });
                }
            }
        }

        let target_count = targets.len();
        Cursors {
            targets,
            last_items: vec![None; target_count],
            advances: 0,
        }
    }

    /// Advance all targeted cursors. Returns `false` if any targeted
    /// cursor is exhausted (no more data).
    ///
    /// After advancing, the new ordinals and field projections are
    /// available via `inject_into_state()`.
    pub fn advance(&mut self) -> bool {
        for (i, target) in self.targets.iter_mut().enumerate() {
            match target.reader.next() {
                Some(item) => {
                    self.last_items[i] = Some(item);
                }
                None => return false, // this cursor exhausted
            }
        }
        self.advances += 1;
        true
    }

    /// Inject the current cursor values into a GK state.
    ///
    /// Sets each cursor's ordinal at its input index, plus any
    /// field projections from the source item.
    pub fn inject_into_state(&self, state: &mut crate::kernel::GkState) {
        for (i, target) in self.targets.iter().enumerate() {
            if let Some(ref item) = self.last_items[i] {
                state.set_input(target.input_index, crate::node::Value::U64(item.ordinal));
                // Inject field projections (e.g., base__vector)
                // These are handled by set_source_item on the FiberBuilder
            }
        }
    }

    /// Get the last items read from all targets (for FiberBuilder injection).
    pub fn last_items(&self) -> &[Option<SourceItem>] {
        &self.last_items
    }

    /// Known extent of the driving cursor (smallest among targeted
    /// cursors with known extent). Used for progress reporting.
    pub fn extent(&self) -> Option<u64> {
        self.targets.iter()
            .filter_map(|t| t.reader.extent())
            .min()
    }

    /// Total advances performed so far.
    pub fn consumed(&self) -> u64 {
        self.advances
    }

    /// Number of targeted cursors.
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    /// Whether this advancer has any targets.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

// =========================================================================
// Built-in ExtensionPolicy implementations
// =========================================================================
//
// Each policy is a pure predicate over `ExtensionContext` and
// returns `Some(delta)` when the cursor should continue or
// `None` when it should stop. The delta is independent of the
// stop condition — workloads can extend in chunks unrelated to
// the cursor's base size (e.g. base=10000 but delta=1000 to
// check the condition more often).

/// Extend by `delta` while `ctx.elapsed_ms < min_ms`.
pub struct UntilElapsedPolicy { pub min_ms: u64, pub delta: u64 }

impl ExtensionPolicy for UntilElapsedPolicy {
    fn next_extension(&self, ctx: &ExtensionContext) -> Option<u64> {
        if ctx.elapsed_ms < self.min_ms { Some(self.delta) } else { None }
    }
}

/// Extend by `delta` while `ctx.passes() < min_passes`. Passes
/// are whole multiples of `ctx.base`.
pub struct UntilPassesPolicy { pub min_passes: u64, pub delta: u64 }

impl ExtensionPolicy for UntilPassesPolicy {
    fn next_extension(&self, ctx: &ExtensionContext) -> Option<u64> {
        if ctx.passes() < self.min_passes { Some(self.delta) } else { None }
    }
}

/// Extend by `delta` while `ctx.consumed < min_count`.
pub struct UntilCountPolicy { pub min_count: u64, pub delta: u64 }

impl ExtensionPolicy for UntilCountPolicy {
    fn next_extension(&self, ctx: &ExtensionContext) -> Option<u64> {
        if ctx.consumed < self.min_count { Some(self.delta) } else { None }
    }
}

/// Continue (extend) while ALL child policies say continue.
/// Stops the moment any child policy returns `None`. The
/// delta is the minimum of the children's deltas — conservative
/// step size keeps any single condition from over-shooting its
/// stop point.
pub struct AndPolicy { pub policies: Vec<Arc<dyn ExtensionPolicy>> }

impl ExtensionPolicy for AndPolicy {
    fn next_extension(&self, ctx: &ExtensionContext) -> Option<u64> {
        let mut min_delta = u64::MAX;
        for p in &self.policies {
            match p.next_extension(ctx) {
                Some(d) => min_delta = min_delta.min(d),
                None => return None,
            }
        }
        if min_delta == u64::MAX || min_delta == 0 { None } else { Some(min_delta) }
    }
}

/// Continue (extend) while ANY child policy says continue.
/// Stops only when every child policy returns `None`. The
/// delta is the maximum of the policies that said continue —
/// matches the most aggressive child still pushing forward.
pub struct OrPolicy { pub policies: Vec<Arc<dyn ExtensionPolicy>> }

impl ExtensionPolicy for OrPolicy {
    fn next_extension(&self, ctx: &ExtensionContext) -> Option<u64> {
        let mut max_delta: Option<u64> = None;
        for p in &self.policies {
            if let Some(d) = p.next_extension(ctx) {
                max_delta = Some(max_delta.map(|m| m.max(d)).unwrap_or(d));
            }
        }
        max_delta
    }
}

/// Back-compat alias for the original time-only policy.
/// Constructs an [`UntilElapsedPolicy`] with delta = base.
pub struct TimeElapsedPolicy { inner: UntilElapsedPolicy }

impl TimeElapsedPolicy {
    pub fn new(base: u64, min_ms: u64) -> Self {
        Self { inner: UntilElapsedPolicy { min_ms, delta: base } }
    }
}

impl ExtensionPolicy for TimeElapsedPolicy {
    fn next_extension(&self, ctx: &ExtensionContext) -> Option<u64> {
        self.inner.next_extension(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_source_yields_ordinals() {
        let factory = RangeSourceFactory::new(0, 5);
        let mut reader = factory.create_reader();
        assert_eq!(reader.extent(), Some(5));

        for i in 0..5 {
            let item = reader.next().unwrap();
            assert_eq!(item.ordinal, i);
            assert!(item.fields.is_empty());
        }
        assert!(reader.next().is_none());
        assert_eq!(reader.consumed(), 5);
    }

    #[test]
    fn range_source_chunk() {
        let factory = RangeSourceFactory::new(0, 10);
        let mut reader = factory.create_reader();

        let chunk = reader.next_chunk(3);
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk[0].ordinal, 0);
        assert_eq!(chunk[2].ordinal, 2);

        let chunk = reader.next_chunk(100);
        assert_eq!(chunk.len(), 7); // only 7 remaining
        assert_eq!(chunk[0].ordinal, 3);
        assert_eq!(chunk[6].ordinal, 9);

        let chunk = reader.next_chunk(1);
        assert!(chunk.is_empty()); // exhausted
    }

    #[test]
    fn range_source_concurrent_readers() {
        let factory = RangeSourceFactory::new(0, 100);
        let mut r1 = factory.create_reader();
        let mut r2 = factory.create_reader();

        // Each reader gets unique ordinals from the shared cursor
        let a = r1.next().unwrap().ordinal;
        let b = r2.next().unwrap().ordinal;
        assert_ne!(a, b);

        // Drain both readers
        let mut total = 2;
        while r1.next().is_some() { total += 1; }
        while r2.next().is_some() { total += 1; }
        assert_eq!(total, 100);
    }

    #[test]
    fn source_item_field_access() {
        let item = SourceItem::with_fields(42, vec![
            ("name".into(), Value::Str("test".into())),
            ("score".into(), Value::F64(0.95)),
        ]);
        assert_eq!(item.ordinal, 42);
        assert_eq!(item.field("name"), Some(&Value::Str("test".into())));
        assert_eq!(item.field("score"), Some(&Value::F64(0.95)));
        assert_eq!(item.field("missing"), None);
    }

    #[test]
    fn range_source_named() {
        let factory = RangeSourceFactory::named("users", 0, 1000);
        assert_eq!(factory.schema().name, "users");
        assert_eq!(factory.schema().extent, Some(1000));
    }

    // ── ExtendingRangeSource ─────────────────────────────────

    /// Trivial extension policy for tests: lets the caller
    /// decide how many extensions remain. Each call to
    /// `next_extension` decrements the count.
    struct FixedExtensions {
        delta: u64,
        remaining: std::sync::atomic::AtomicU64,
    }

    impl FixedExtensions {
        fn new(delta: u64, times: u64) -> Self {
            Self {
                delta,
                remaining: std::sync::atomic::AtomicU64::new(times),
            }
        }
    }

    impl ExtensionPolicy for FixedExtensions {
        fn next_extension(&self, _ctx: &ExtensionContext) -> Option<u64> {
            let prev = self.remaining.fetch_sub(1, Ordering::Relaxed);
            if prev == 0 || prev > i64::MAX as u64 {
                self.remaining.store(0, Ordering::Relaxed);
                None
            } else {
                Some(self.delta)
            }
        }
    }

    #[test]
    fn extending_source_consumes_initial_extent_then_extends() {
        // Initial extent = 5; policy extends once by 5 more.
        // Total expected: 10 ordinals consumed.
        let policy = Arc::new(FixedExtensions::new(5, 1));
        let factory = ExtendingRangeSourceFactory::new("ext", 0, 5, policy);
        let mut reader = factory.create_reader();
        let mut got: Vec<u64> = Vec::new();
        while let Some(item) = reader.next() {
            got.push(item.ordinal);
            if got.len() > 50 { panic!("runaway extension"); }
        }
        assert_eq!(got, (0..10).collect::<Vec<u64>>());
        assert_eq!(reader.consumed(), 10);
    }

    #[test]
    fn extending_source_zero_extensions_behaves_like_fixed_range() {
        let policy = Arc::new(FixedExtensions::new(0, 0));
        let factory = ExtendingRangeSourceFactory::new("ext", 0, 3, policy);
        let mut reader = factory.create_reader();
        let mut got: Vec<u64> = Vec::new();
        while let Some(item) = reader.next() {
            got.push(item.ordinal);
        }
        assert_eq!(got, vec![0, 1, 2]);
    }

    #[test]
    fn extending_source_global_extent_grows_after_extension() {
        // global_extent must reflect the CURRENT end so phase
        // status displays show growth honestly.
        let policy = Arc::new(FixedExtensions::new(10, 2));
        let factory = ExtendingRangeSourceFactory::new("ext", 0, 5, policy);
        assert_eq!(factory.global_extent(), Some(5));
        let mut reader = factory.create_reader();
        // Drain the first 5 to force an extension.
        for _ in 0..5 { reader.next().unwrap(); }
        // Trigger the extension by attempting one more pull.
        let _ = reader.next().unwrap();
        assert_eq!(factory.global_extent(), Some(15),
            "extent should grow by the extension delta");
    }

    #[test]
    fn extending_source_chunked_reservation_caps_at_current_end() {
        // A stride request that crosses the current `end` must
        // return a SHORT range up to `end`, not an over-claim.
        // Otherwise the next reservation would jump past the
        // extended range and lose ordinals.
        let policy = Arc::new(FixedExtensions::new(5, 1));
        let factory = ExtendingRangeSourceFactory::new("ext", 0, 3, policy);
        let mut reader = factory.create_reader();
        let range = reader.reserve(10).expect("first reserve");
        // Initial end was 3; the reservation must cap there.
        assert_eq!(range, 0..3);
        // Next reserve triggers the extension and consumes the
        // remainder.
        let range = reader.reserve(10).expect("second reserve");
        assert_eq!(range, 3..8);
    }

    fn ctx_at(elapsed_ms: u64, consumed: u64, base: u64) -> ExtensionContext {
        ExtensionContext { elapsed_ms, consumed, base }
    }

    #[test]
    fn until_elapsed_policy_extends_while_below_min_ms() {
        let policy = UntilElapsedPolicy { min_ms: 50, delta: 7 };
        assert_eq!(policy.next_extension(&ctx_at(0, 0, 0)), Some(7));
        assert_eq!(policy.next_extension(&ctx_at(49, 0, 0)), Some(7));
        assert_eq!(policy.next_extension(&ctx_at(50, 0, 0)), None);
        assert_eq!(policy.next_extension(&ctx_at(1000, 0, 0)), None);
    }

    #[test]
    fn until_passes_policy_counts_in_base_multiples() {
        let policy = UntilPassesPolicy { min_passes: 3, delta: 100 };
        // 0 passes done — extend.
        assert_eq!(policy.next_extension(&ctx_at(0, 0, 100)), Some(100));
        // 2 passes done (200 consumed @ base=100) — extend.
        assert_eq!(policy.next_extension(&ctx_at(0, 200, 100)), Some(100));
        // 3 passes done — stop.
        assert_eq!(policy.next_extension(&ctx_at(0, 300, 100)), None);
        // 4 passes done — stop.
        assert_eq!(policy.next_extension(&ctx_at(0, 400, 100)), None);
    }

    #[test]
    fn until_count_policy_uses_raw_consumed() {
        let policy = UntilCountPolicy { min_count: 250, delta: 50 };
        assert_eq!(policy.next_extension(&ctx_at(0, 0, 100)), Some(50));
        assert_eq!(policy.next_extension(&ctx_at(0, 249, 100)), Some(50));
        assert_eq!(policy.next_extension(&ctx_at(0, 250, 100)), None);
    }

    #[test]
    fn and_policy_stops_when_any_child_stops() {
        // time<5000 AND passes<3.
        let time = Arc::new(UntilElapsedPolicy { min_ms: 5000, delta: 10 });
        let passes = Arc::new(UntilPassesPolicy { min_passes: 3, delta: 20 });
        let and = AndPolicy { policies: vec![time, passes] };
        // Both still want to continue: delta = min(10, 20) = 10.
        assert_eq!(and.next_extension(&ctx_at(0, 0, 100)), Some(10));
        // Time done — stop.
        assert_eq!(and.next_extension(&ctx_at(5000, 0, 100)), None);
        // Passes done — stop.
        assert_eq!(and.next_extension(&ctx_at(0, 300, 100)), None);
    }

    #[test]
    fn or_policy_continues_if_any_child_continues() {
        let time = Arc::new(UntilElapsedPolicy { min_ms: 5000, delta: 10 });
        let passes = Arc::new(UntilPassesPolicy { min_passes: 3, delta: 20 });
        let or = OrPolicy { policies: vec![time, passes] };
        // Both want to continue → delta = max(10, 20) = 20.
        assert_eq!(or.next_extension(&ctx_at(0, 0, 100)), Some(20));
        // Time done, passes still wants → delta = 20.
        assert_eq!(or.next_extension(&ctx_at(5000, 0, 100)), Some(20));
        // Passes done, time still wants → delta = 10.
        assert_eq!(or.next_extension(&ctx_at(0, 300, 100)), Some(10));
        // Both done → stop.
        assert_eq!(or.next_extension(&ctx_at(5000, 300, 100)), None);
    }

    #[test]
    fn time_elapsed_policy_compat_alias_still_works() {
        let p = TimeElapsedPolicy::new(7, 1);
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(p.next_extension(&ctx_at(20, 0, 0)), None);
    }
}
