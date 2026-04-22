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
//! All source API surface lives here in nb-variates. The runtime crates
//! (nb-activity, adapters) consume these types but don't define them.

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
    pub extent: Option<u64>,
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
}
