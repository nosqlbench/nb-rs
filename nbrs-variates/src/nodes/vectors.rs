// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Vector dataset access nodes via the `vectordata` crate.
//!
//! Each node takes a dataset source string (URL, local path, or
//! catalog name) as a const parameter and loads the dataset handle at
//! construction time.
//!
//! Source specifier formats:
//! - `"dataset"` — catalog lookup, uses default profile
//! - `"dataset:profile"` — catalog lookup with explicit profile
//! - `"https://..."` — direct URL
//! - `"/path/to/dir"` — local filesystem path
//!
//! ## Prebuffering
//!
//! Use `dataset_prebuffer("source")` to eagerly download all facets
//! for a dataset before workload execution. After prebuffering, data
//! access uses local mmap readers (zero HTTP overhead).
//!
//! ## Cache-aware loading
//!
//! For catalog-resolved datasets, the loader checks the local cache
//! at `~/.cache/vectordata/<dataset>/` before issuing HTTP requests.
//! Prebuffering populates this cache; subsequent loads are local.
//!
//! Feature-gated behind `vectordata`.
//!
//! ## Upstream API reference
//!
//! See the vectordata consumer API docs for the full dataset access,
//! catalog, caching, and prebuffer model:
//! <https://github.com/nosqlbench/vectordata-rs/blob/main/docs/sysref/02-api.md>

use std::sync::{Arc, LazyLock};

use crate::cache::OnceCache;

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use vectordata::TestDataGroup;
use vectordata::TestDataView;
use vectordata::io::{VectorReader, VvecReader};
use vectordata::catalog::sources::CatalogSources;
use vectordata::catalog::resolver::Catalog;

/// Global cache for loaded dataset groups keyed by source string.
/// Ensures each dataset is loaded exactly once regardless of how many
/// node functions reference it. The race-free init pattern lives in
/// [`crate::cache::OnceCache`].
static DATASET_CACHE: LazyLock<OnceCache<String, Arc<TestDataGroup>>> =
    LazyLock::new(OnceCache::new);

/// Type-erased facet cache: (source, profile, facet) → Arc<dyn Any + Send + Sync>.
/// Ensures each reader (of any element type) is opened exactly once
/// and shared across all node instances that reference the same data.
/// The concrete type inside is `Arc<UniformDataset<T>>` or `Arc<Ivvec32Dataset>`
/// or `Arc<GenericFacetDataset>`. The race-free init pattern lives
/// in [`OnceCache`].
static FACET_CACHE: LazyLock<OnceCache<(String, String, String), Arc<dyn std::any::Any + Send + Sync>>> =
    LazyLock::new(OnceCache::new);

/// Whole-dataset prebuffer cache keyed by source string. Wraps
/// [`do_dataset_prebuffer_inner`] so N fibers all calling
/// `dataset_prebuffer("ds:profile")` at init time serialize on
/// one OnceLock and share the resulting handle. Without this,
/// each fiber's own kernel-init pass invokes the real prebuffer
/// body and they all race into the manifest walk + per-facet
/// download — exactly the thundering herd we hit. The inner
/// caches (DATASET_CACHE, FACET_CACHE) only protect the
/// group-resolve and per-facet-reader steps, not the outer
/// "walk every facet and pull all chunks" work.
static PREBUFFER_CACHE: LazyLock<OnceCache<String, Arc<DatasetHandle>>> =
    LazyLock::new(OnceCache::new);

// =================================================================
// Dataset resolution — catalog-aware, cache-aware
// =================================================================

/// Parse a source specifier into (dataset_name, profile_name).
///
/// Supports `"dataset:profile"` syntax. If no colon is present,
/// the profile defaults to `"default"`.
fn parse_source_specifier(source: &str) -> (&str, &str) {
    // Don't split on colon in URLs
    if source.starts_with("http://") || source.starts_with("https://") {
        return (source, "default");
    }
    if let Some(pos) = source.find(':') {
        (&source[..pos], &source[pos + 1..])
    } else {
        (source, "default")
    }
}

/// Run a synchronous body that may internally drive
/// `reqwest::blocking` (and thus spin up a private tokio
/// runtime per HTTP request). If we're sitting on an outer
/// async runtime — we are, in every per-cycle and per-phase
/// path — the inner runtime panics on drop with "Cannot drop a
/// runtime in a context where blocking is not allowed".
/// `block_in_place` parks the outer multi-thread worker for
/// the duration of the call so the inner runtime sees a
/// non-async drop context. Falls back to a direct call when no
/// runtime is current (e.g. unit tests). Single helper used by
/// all three vectordata-facing entry points
/// ([`load_dataset_group`], [`load_uniform_facet`],
/// [`GenericFacetDataset::load`]).
fn run_blocking_io<R>(body: impl FnOnce() -> R) -> R {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(body)
    } else {
        body()
    }
}

/// Load a dataset group by name.
///
/// Uses the vectordata catalog API: `catalog.open(name)` handles
/// catalog discovery, cache resolution, and download transparently.
fn load_dataset_group(source: &str) -> Result<Arc<TestDataGroup>, String> {
    let (dataset_name, _profile) = parse_source_specifier(source);
    // Both keys (dataset name and full source spec) point at the
    // same `Arc<TestDataGroup>`; the `OnceCache` slot for one is
    // primed by the other on first hit.
    DATASET_CACHE.get_or_init(dataset_name.to_string(), || {
        run_blocking_io(|| {
            let catalog = Catalog::of(&CatalogSources::new().configure_default());
            catalog.open(dataset_name)
                .map(Arc::new)
                .map_err(|e| format!("failed to load dataset '{dataset_name}': {e}"))
        })
    })
}


// =================================================================
// Dataset handles — loaded once at node construction, shared via Arc
// =================================================================

/// Generic handle to a loaded uniform vector facet. Thread-safe, random-access.
/// Supports any element type provided by the vectordata API (f32, f64,
/// i32, i16, u8, i8, u16, u32, u64, i64, f16).
pub(crate) struct UniformDataset<T: Send + Sync + 'static> {
    reader: Arc<dyn VectorReader<T>>,
    count: usize,
    dim: usize,
}


/// Cache-aware loader for uniform vector facets.
/// Returns a shared `Arc<UniformDataset<T>>`, creating and caching it
/// on first access. Subsequent loads for the same (source, profile, facet)
/// return the cached instance.
fn load_uniform_facet<T: Send + Sync + 'static>(
    source: &str,
    profile: &str,
    facet: &str,
    open_fn: impl FnOnce(&dyn TestDataView) -> std::result::Result<Arc<dyn VectorReader<T>>, vectordata::Error>,
) -> Result<Arc<UniformDataset<T>>, String> {
    let key = (source.to_string(), profile.to_string(), facet.to_string());
    let any = FACET_CACHE.get_or_init(key, || {
        let group = load_dataset_group(source)?;
        let view = group.profile(profile)
            .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
        // Audit: log the open *before* `open_fn` runs so the
        // line appears even if the open errors. Inside
        // `get_or_init`'s closure, this fires exactly once per
        // (source, profile, facet) — the prior shape logged
        // once per concurrent miss (storms of N for N fibers).
        crate::audit::record_opened(source, profile, facet, "uniform");
        let reader = run_blocking_io(|| open_fn(view.as_ref()))
            .map_err(|e| format!("failed to access {facet} from '{source}': {e}"))?;
        let count = reader.count();
        let dim = reader.dim();
        let arc: Arc<UniformDataset<T>> = Arc::new(UniformDataset { reader, count, dim });
        Ok(arc as Arc<dyn std::any::Any + Send + Sync>)
    })?;
    any.downcast::<UniformDataset<T>>()
        .map_err(|_| format!(
            "facet cache type mismatch for '{source}:{profile}/{facet}' — \
             this should be impossible; please file a bug."))
}

// Type aliases for backward compatibility
type F32Dataset = UniformDataset<f32>;
type I32Dataset = UniformDataset<i32>;

impl F32Dataset {
    fn load(source: &str, profile: &str, facet: &str) -> Result<Arc<Self>, String> {
        let facet_name = facet.to_string();
        load_uniform_facet(source, profile, facet, move |view| {
            match facet_name.as_str() {
                "base" => view.base_vectors(),
                "query" => view.query_vectors(),
                "neighbor_distances" => view.neighbor_distances(),
                "filtered_neighbor_distances" => view.filtered_neighbor_distances(),
                other => Err(vectordata::Error::MissingFacet(format!("unknown f32 facet: '{other}'"))),
            }
        })
    }

}

impl I32Dataset {
    fn load(source: &str, profile: &str, facet: &str) -> Result<Arc<Self>, String> {
        let facet_name = facet.to_string();
        load_uniform_facet(source, profile, facet, move |view| {
            match facet_name.as_str() {
                "neighbor_indices" => view.neighbor_indices(),
                "filtered_neighbor_indices" => view.filtered_neighbor_indices(),
                other => Err(vectordata::Error::MissingFacet(format!("unknown i32 facet: '{other}'"))),
            }
        })
    }

}

// =================================================================
// Dataset handle — typed enum wrapping all per-facet dataset shapes
// =================================================================
//
// Per SRD 53 §"Dataset Handles": `dataset_open(source, facet)` is
// the resolver node; per-cycle accessors take a handle wire and
// downcast to the concrete variant they expect. A single Value
// variant `Value::Handle(Arc<dyn Any>)` carries any of the
// concrete shapes — the enum below is what's actually inside the
// Arc, and accessors `match` on the variant.
//
// One handle may be opened against multiple facets at the
// `dataset_open` boundary (the user calls `dataset_open(spec,
// "base")` vs. `dataset_open(spec, "query")` and gets two
// distinct handles); the downstream accessor matches on whatever
// variant came back.

/// Typed wrapper around a resolved dataset facet or group. Held
/// inside `Value::Handle` as `Arc<DatasetHandle>` and downcast by
/// accessor nodes via [`Value::as_handle::<DatasetHandle>()`].
/// Cloning a `Value::Handle` is one `Arc::clone` — one atomic
/// increment, no allocation — which is the design contract this
/// enum exists to satisfy.
#[derive(Clone)]
pub(crate) enum DatasetHandle {
    /// Uniform `f32` vector facet (base, query, neighbor_distances,
    /// filtered_neighbor_distances).
    F32(Arc<F32Dataset>),
    /// Uniform `i32` vector facet (neighbor_indices, filtered_neighbor_indices).
    I32(Arc<I32Dataset>),
    /// Variable-length `i32` facet (metadata_indices).
    Ivvec32(Arc<Ivvec32Dataset>),
    /// Type-erased generic-typed scalar facet (metadata_content,
    /// metadata_predicates, ...).
    Generic(Arc<GenericFacetDataset>),
    /// Dataset-group handle — used by group-level metadata
    /// accessors (`dataset_profile_count`, `dataset_facets`,
    /// `dataset_distance_function`, ...) that operate on the
    /// `TestDataGroup` before any profile/facet is chosen.
    Group(Arc<TestDataGroup>),
    /// Prebuffered-and-resident dataset, returned by
    /// `dataset_prebuffer(source)`. Carries both the group AND
    /// the source spec so per-facet accessors can re-resolve
    /// (`query_vector_at(prebuffered, q)` → resolves the
    /// `query` facet from `<source>:<profile>`). Distinct from
    /// `Group` so existing Group-only consumers stay typed.
    ///
    /// The `_group` field keeps the prebuffered `TestDataGroup`
    /// alive for the duration of the handle — vectordata's
    /// internal storage cache is keyed off the group instance,
    /// so dropping the group prematurely would force per-facet
    /// readers to re-open against transport. The field isn't
    /// read directly by accessors (they use `source` to re-open
    /// via `DATASET_CACHE`, which has the same group cached);
    /// the field's purpose is the lifetime extension.
    Prebuffered { _group: Arc<TestDataGroup>, source: String },
}

impl DatasetHandle {
    fn open(source: &str, facet: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        match facet {
            "base" | "query" | "neighbor_distances" | "filtered_neighbor_distances" => {
                F32Dataset::load(source, profile, facet).map(DatasetHandle::F32)
            }
            "neighbor_indices" | "filtered_neighbor_indices" => {
                I32Dataset::load(source, profile, facet).map(DatasetHandle::I32)
            }
            "metadata_indices" => {
                Ivvec32Dataset::load(source, profile).map(DatasetHandle::Ivvec32)
            }
            // Anything else routes through GenericFacetDataset (typed
            // scalar reader), which covers metadata_content,
            // metadata_predicates, and any future scalar facet.
            _ => GenericFacetDataset::load(source, profile, facet).map(DatasetHandle::Generic),
        }
    }

    fn open_group(source: &str) -> Result<Self, String> {
        load_dataset_group(source).map(DatasetHandle::Group)
    }
}

/// Downcast a handle Value to the typed `DatasetHandle` enum.
fn handle_of(v: &Value) -> &DatasetHandle {
    v.as_handle::<DatasetHandle>()
}

/// `dataset_open(source: str, facet: str) -> Handle`
///
/// The single resolver node. Provenance follows its two wire
/// inputs; when both are scope-extern constants (the iter-var
/// case), this evaluates exactly once at iteration entry and
/// stays cached for every cycle in that iteration. When source
/// or facet is cycle-time, it re-evaluates accordingly.
///
/// All per-cycle accessors take the resulting handle on a wire
/// — the catalog/HTTP/mmap path is never on the cycle hot path.
pub struct DatasetOpen {
    meta: NodeMeta,
}

impl DatasetOpen {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "dataset_open".into(),
                outs: vec![Port::handle("output")],
                ins: vec![
                    Slot::Wire(Port::str("source")),
                    Slot::Wire(Port::str("facet")),
                ],
            },
        }
    }
}

impl Default for DatasetOpen {
    fn default() -> Self { Self::new() }
}

impl GkNode for DatasetOpen {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let source = inputs[0].as_str();
        let facet = inputs[1].as_str();
        // Surfacing a *runtime error* (bad source/profile string)
        // as a panic crashes the whole runtime — see
        // adapters/cql/workloads/full_cql_vector.yaml for the
        // canonical reproducer where an outer iter var was not
        // cascaded and the source string came in as
        // "<dataset>:0". Returning `Value::None` lets a wrapper
        // surface this as an op-level error instead.
        outputs[0] = match DatasetHandle::open(source, facet) {
            Ok(h) => Value::handle(Arc::new(h)),
            Err(e) => {
                crate::audit::error(&format!(
                    "dataset_open: failed to resolve '{source}' facet='{facet}': {e}"
                ));
                Value::None
            }
        };
    }
}

/// `dataset_group_open(source: str) -> Handle`
///
/// Group-level resolver. Returns a handle wrapping `Arc<TestDataGroup>`
/// — used by group-level metadata accessors (`dataset_profile_count`,
/// `dataset_facets`, ...) that operate on the dataset as a whole
/// before any profile/facet is selected.
pub struct DatasetGroupOpen {
    meta: NodeMeta,
}

impl DatasetGroupOpen {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "dataset_group_open".into(),
                outs: vec![Port::handle("output")],
                ins: vec![Slot::Wire(Port::str("source"))],
            },
        }
    }
}

impl Default for DatasetGroupOpen {
    fn default() -> Self { Self::new() }
}

impl GkNode for DatasetGroupOpen {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let source = inputs[0].as_str();
        // Same rationale as `DatasetOpen::eval` — surface as
        // Value::None plus stderr, not a panic.
        outputs[0] = match DatasetHandle::open_group(source) {
            Ok(h) => Value::handle(Arc::new(h)),
            Err(e) => {
                crate::audit::error(&format!(
                    "dataset_group_open: failed to resolve '{source}': {e}"
                ));
                Value::None
            }
        };
    }
}

// =================================================================
// Base vector nodes
// =================================================================

// All indexed-accessor nodes share the same shape: an `index`
// wire (u64) and a `source` wire (Str), with the dataset
// resolved lazily on first eval per spec via `DATASET_CACHE`
// (inside `F32Dataset::load` / `I32Dataset::load`). The
// per-cycle hot path is one HashMap lookup on the cached spec
// plus the existing facet read; the spec doesn't change within
// an iteration scope so subsequent cycles hit the cache.

// =================================================================
// Per-cycle indexed accessors
// =================================================================
//
// All take `(handle: Handle, index: u64)`. The handle is resolved
// once per iteration by `dataset_open` (or by cursor sugar that
// emits an implicit one); per-cycle eval downcasts the handle's
// `Arc<DatasetHandle>` and reads at `index`. No HashMap, no Mutex,
// no String allocation on the source side. The `Bytes` outputs
// allocate a fresh per-cycle buffer — that's the remaining
// per-cycle alloc until SRD 46 (native vector binding).
//
// `expected_variant` documents which `DatasetHandle` variant is
// expected; a panic on wrong variant indicates the workload
// opened a handle for a different facet than the accessor expects.

macro_rules! handle_indexed_node {
    (
        $(#[$meta:meta])*
        $name:ident, $func_name:literal, $out_port:ident,
        facet = $facet:literal,
        eval = $eval_fn:expr
    ) => {
        $(#[$meta])*
        pub struct $name {
            meta: NodeMeta,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        outs: vec![Port::new("output", PortType::$out_port)],
                        ins: vec![
                            Slot::Wire(Port::handle("handle")),
                            Slot::Wire(Port::u64("index")),
                        ],
                    },
                }
            }
        }

        impl Default for $name {
            fn default() -> Self { Self::new() }
        }

        impl GkNode for $name {
            fn meta(&self) -> &NodeMeta { &self.meta }
            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                let handle = handle_of(&inputs[0]);
                // If the handle is `Prebuffered` (returned by
                // `dataset_prebuffer`), resolve to the specific
                // facet variant the accessor expects. For all
                // other handles this is a no-op borrow.
                let resolved = handle.resolve_facet($facet);
                let index = inputs[1].as_u64() as usize;
                outputs[0] = ($eval_fn)(resolved.as_ref(), index);
            }
        }
    };
}

/// Resolve a Prebuffered handle to a specific-facet handle by
/// opening the named facet via the existing FACET_CACHE path
/// (which is `OnceCache`-backed — concurrent callers serialize
/// per (source, profile, facet)). For non-Prebuffered handles
/// this is a borrow-through; the macro callers use the result as
/// `&DatasetHandle` regardless of which arm fires.
///
/// Lives on `DatasetHandle` so the resolution logic — and the
/// "what does Prebuffered mean to a per-facet accessor" question
/// — sits next to the variant declaration.
impl DatasetHandle {
    fn resolve_facet<'a>(&'a self, facet: &str) -> std::borrow::Cow<'a, DatasetHandle> {
        match self {
            DatasetHandle::Prebuffered { source, .. } => {
                match DatasetHandle::open(source, facet) {
                    Ok(opened) => std::borrow::Cow::Owned(opened),
                    Err(e) => panic!(
                        "DatasetHandle::resolve_facet: failed to open \
                         '{facet}' from prebuffered '{source}': {e}"
                    ),
                }
            }
            _ => std::borrow::Cow::Borrowed(self),
        }
    }
}

fn f32_vec_at(h: &DatasetHandle, index: usize) -> Value {
    match h {
        DatasetHandle::F32(d) => Value::VecF32(slice_arc_from_uniform(d, index)),
        other => panic!(
            "expected F32 dataset handle, got {}",
            dataset_handle_kind(other)
        ),
    }
}

fn i32_vec_at(h: &DatasetHandle, index: usize) -> Value {
    match h {
        DatasetHandle::I32(d) => Value::VecI32(slice_arc_from_uniform(d, index)),
        other => panic!(
            "expected I32 dataset handle, got {}",
            dataset_handle_kind(other)
        ),
    }
}

fn ivvec32_vec_at(h: &DatasetHandle, index: usize) -> Value {
    match h {
        // Variable-length records — vectordata's trait doesn't expose
        // a zero-copy slice path for these (per-record dim is read
        // from the file), so we always allocate. One Vec<i32> per
        // cycle; same bound as the upstream trait API.
        DatasetHandle::Ivvec32(d) => {
            if d.count == 0 {
                return Value::VecI32(crate::node::SliceArc::from_vec(Vec::<i32>::new()));
            }
            let v = d.reader.get(index % d.count).unwrap_or_default();
            Value::VecI32(crate::node::SliceArc::from_vec(v))
        }
        other => panic!(
            "expected Ivvec32 dataset handle, got {}",
            dataset_handle_kind(other)
        ),
    }
}

/// Build a [`SliceArc<T>`] from any uniform-stride dataset at
/// `index`. Single generic helper consolidating what used to be
/// per-element-type slice-arc constructors — element type is
/// erased by the upstream `VectorReader<T>` trait, and `SliceArc`
/// is element-type-generic, so one body suffices for `f32`,
/// `i32`, and any other element type the trait supports.
///
/// Tries the zero-copy `VectorReader::get_slice` path first —
/// that returns a borrow into the mmap'd file pages, kept alive
/// by the `Arc<UniformDataset<T>>` that we move into the
/// `SliceArc` as owner. No allocation, no element decoding, no
/// copy.
///
/// Falls back to `VectorReader::get(idx) -> Vec<T>` for readers
/// that don't support zero-copy (HTTP-backed, or merkle-cached
/// storage that hasn't been promoted to mmap yet); that path
/// allocates one `Vec<T>` per cycle.
fn slice_arc_from_uniform<T>(
    d: &Arc<UniformDataset<T>>,
    index: usize,
) -> crate::node::SliceArc<T>
where
    T: Send + Sync + Copy + 'static,
{
    if d.count == 0 {
        return crate::node::SliceArc::from_vec(Vec::<T>::new());
    }
    let idx = index % d.count;
    if let Some(slice) = d.reader.get_slice(idx) {
        // SAFETY: the slice points into the mmap pages owned by
        // `d`'s reader; cloning `d` into the `SliceArc`'s owner
        // keeps the mmap alive for as long as the slice is held.
        let ptr_len = (slice.as_ptr(), slice.len());
        let owner = d.clone();
        let owner_dyn: Arc<dyn std::any::Any + Send + Sync> = owner;
        return unsafe {
            crate::node::SliceArc::from_borrowed(
                owner_dyn,
                std::slice::from_raw_parts(ptr_len.0, ptr_len.1),
            )
        };
    }
    crate::node::SliceArc::from_vec(d.reader.get(idx).unwrap_or_default())
}

fn dataset_handle_kind(h: &DatasetHandle) -> &'static str {
    match h {
        DatasetHandle::F32(_) => "F32",
        DatasetHandle::I32(_) => "I32",
        DatasetHandle::Ivvec32(_) => "Ivvec32",
        DatasetHandle::Generic(_) => "Generic",
        DatasetHandle::Group(_) => "Group",
        DatasetHandle::Prebuffered { .. } => "Prebuffered",
    }
}

/// Helper for group-level accessors: extract the `TestDataGroup`
/// from a `DatasetHandle::Group` variant. Panics on mismatch.
fn group_of(handle: &DatasetHandle) -> &TestDataGroup {
    match handle {
        DatasetHandle::Group(g) => g.as_ref(),
        other => panic!(
            "expected Group handle, got {}",
            dataset_handle_kind(other)
        ),
    }
}

handle_indexed_node!(
    /// Access an `f32` vector by index, returning a typed `VecF32`.
    /// Works on any F32 handle (base or query facet).
    ///
    /// Signature: `vector_at(handle, index: u64) -> VecF32`
    VectorAt, "vector_at", VecF32, facet = "base", eval = f32_vec_at
);

handle_indexed_node!(
    /// Access a query vector by index. Alias for [`VectorAt`] kept
    /// for clarity in workloads that distinguish base and query
    /// handles by name.
    ///
    /// Signature: `query_vector_at(handle, index: u64) -> VecF32`
    QueryVectorAt, "query_vector_at", VecF32, facet = "query", eval = f32_vec_at
);

handle_indexed_node!(
    /// Access ground-truth neighbor indices for a query. Expects an
    /// I32 handle.
    ///
    /// Signature: `neighbor_indices_at(handle, index: u64) -> VecI32`
    NeighborIndicesAt, "neighbor_indices_at", VecI32, facet = "neighbor_indices", eval = i32_vec_at
);

handle_indexed_node!(
    /// Access ground-truth neighbor distances for a query. Expects an
    /// F32 handle.
    ///
    /// Signature: `neighbor_distances_at(handle, index: u64) -> VecF32`
    NeighborDistancesAt, "neighbor_distances_at", VecF32, facet = "neighbor_distances", eval = f32_vec_at
);

handle_indexed_node!(
    /// Access filtered ground-truth neighbor indices. Expects an I32 handle.
    FilteredNeighborIndicesAt, "filtered_neighbor_indices_at", VecI32, facet = "filtered_neighbor_indices", eval = i32_vec_at
);

handle_indexed_node!(
    /// Access filtered ground-truth neighbor distances. Expects an F32 handle.
    FilteredNeighborDistancesAt, "filtered_neighbor_distances_at", VecF32, facet = "filtered_neighbor_distances", eval = f32_vec_at
);

// =================================================================
// Metadata nodes (constant per dataset)
// =================================================================

// =================================================================
// Per-handle metadata nodes
// =================================================================
//
// Take `(handle: Handle)` and read shape/metadata directly from the
// resolved dataset. Provenance bounded by the handle, which itself
// is bounded by the resolver's externs — so these collapse to a
// single per-iteration eval chain via the standard provenance
// caching. No per-cycle work.

macro_rules! handle_metadata_node {
    (
        $(#[$meta:meta])*
        $name:ident, $func_name:literal, $out_port:ident,
        eval = $eval_fn:expr
    ) => {
        $(#[$meta])*
        pub struct $name {
            meta: NodeMeta,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        outs: vec![Port::new("output", PortType::$out_port)],
                        ins: vec![Slot::Wire(Port::handle("handle"))],
                    },
                }
            }
        }

        impl Default for $name {
            fn default() -> Self { Self::new() }
        }

        impl GkNode for $name {
            fn meta(&self) -> &NodeMeta { &self.meta }
            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                let handle = handle_of(&inputs[0]);
                outputs[0] = ($eval_fn)(handle);
            }
        }
    };
}

handle_metadata_node!(
    /// Return the dimensionality (`f32` count per record) of a
    /// vector facet handle.
    ///
    /// Signature: `vector_dim(handle) -> (u64)`
    VectorDim, "vector_dim", U64,
    eval = |h: &DatasetHandle| match h {
        DatasetHandle::F32(d) => Value::U64(d.dim as u64),
        DatasetHandle::I32(d) => Value::U64(d.dim as u64),
        _ => Value::U64(0),
    }
);

/// Return the dataset's distance function (e.g., "cosine", "euclidean").
///
/// Signature: `dataset_distance_function(source) -> (String)`
// Source-only nodes (just take a `source` String wire) all
// share the new shape: declare one Wire slot, read inputs[0]
// at eval, look up via the global cache, return the property.

macro_rules! source_only_node {
    (
        $(#[$meta:meta])*
        $name:ident, $func_name:literal,
        out_port = $out:ident,
        eval = |$src:ident| $body:expr
    ) => {
        $(#[$meta])*
        pub struct $name {
            meta: NodeMeta,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    meta: NodeMeta {
                        name: $func_name.into(),
                        outs: vec![Port::new("output", PortType::$out)],
                        ins: vec![Slot::Wire(Port::str("source"))],
                    },
                }
            }
        }

        impl Default for $name {
            fn default() -> Self { Self::new() }
        }

        impl GkNode for $name {
            fn meta(&self) -> &NodeMeta { &self.meta }
            fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
                let $src = inputs[0].as_str();
                outputs[0] = $body;
            }
        }
    };
}

// Helper: read a dataset-group attribute via a handle's underlying
// `TestDataGroup`. Used by `dataset_distance_function`. We cache
// the source string on the dataset handle itself to keep
// dataset-group attributes accessible without an extra lookup —
// but for now the simpler path is to require a separate
// dataset-group-level node that takes `source: str` (kept below).

handle_metadata_node!(
    /// Return the count of records in the facet a handle was opened
    /// against. This is the canonical "how many vectors / queries /
    /// neighbor-rows" accessor — `vector_count(base_handle)` for
    /// base vectors, `vector_count(query_handle)` for query
    /// vectors, etc.
    ///
    /// Signature: `vector_count(handle) -> (u64)`
    VectorCount, "vector_count", U64,
    eval = |h: &DatasetHandle| match h {
        DatasetHandle::F32(d) => Value::U64(d.count as u64),
        DatasetHandle::I32(d) => Value::U64(d.count as u64),
        DatasetHandle::Ivvec32(d) => Value::U64(d.count as u64),
        DatasetHandle::Generic(d) => Value::U64(d.count as u64),
        DatasetHandle::Group(_) => panic!("vector_count: expected facet handle, got Group"),
        DatasetHandle::Prebuffered { source, .. } => {
            // Resolve the `base` facet (canonical interpretation
            // of `vector_count(prebuffered)` — base vectors).
            match DatasetHandle::open(source, "base") {
                Ok(DatasetHandle::F32(d)) => Value::U64(d.count as u64),
                Ok(other) => panic!(
                    "vector_count: expected F32 base facet, got {}",
                    dataset_handle_kind(&other)),
                Err(e) => panic!(
                    "vector_count: failed to open 'base' from prebuffered '{source}': {e}"),
            }
        }
    }
);

handle_metadata_node!(
    /// Alias for [`VectorCount`] kept for clarity in workloads that
    /// distinguish base/query handles by name.
    ///
    /// Signature: `query_count(handle) -> (u64)`
    QueryCount, "query_count", U64,
    eval = |h: &DatasetHandle| match h {
        DatasetHandle::F32(d) => Value::U64(d.count as u64),
        DatasetHandle::I32(d) => Value::U64(d.count as u64),
        DatasetHandle::Ivvec32(d) => Value::U64(d.count as u64),
        DatasetHandle::Generic(d) => Value::U64(d.count as u64),
        DatasetHandle::Group(_) => panic!("query_count: expected facet handle, got Group"),
        DatasetHandle::Prebuffered { source, .. } => {
            // `query_count(prebuffered)` is a legitimate idiom in the
            // pvs_query workload (`cursor q = range(0, query_count(...))`),
            // so resolve to the query-facet handle and read its count.
            // Same OnceCache-backed open as the per-cycle accessors.
            match DatasetHandle::open(source, "query") {
                Ok(DatasetHandle::F32(d)) => Value::U64(d.count as u64),
                Ok(other) => panic!(
                    "query_count: expected F32 query facet, got {}",
                    dataset_handle_kind(&other)),
                Err(e) => panic!(
                    "query_count: failed to open 'query' from prebuffered '{source}': {e}"),
            }
        }
    }
);

handle_metadata_node!(
    /// Return the per-record neighbor count (k) for an I32
    /// neighbor-indices handle.
    ///
    /// Signature: `neighbor_count(handle) -> (u64)`
    NeighborCount, "neighbor_count", U64,
    eval = |h: &DatasetHandle| match h {
        DatasetHandle::I32(d) => Value::U64(d.dim as u64),
        _ => Value::U64(0),
    }
);

handle_metadata_node!(
    /// Return the dataset's distance function (e.g., "COSINE",
    /// "EUCLIDEAN"). Operates on the dataset group; takes a Group
    /// handle.
    ///
    /// Signature: `dataset_distance_function(group) -> (String)`
    DatasetDistanceFunction, "dataset_distance_function", Str,
    eval = |h: &DatasetHandle| {
        let group = group_of(h);
        let raw = group
            .attribute("distance_function")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let df = match raw.to_uppercase().as_str() {
            "L2" | "EUCLIDEAN" => "EUCLIDEAN",
            "L1" | "MANHATTAN" => "MANHATTAN",
            "COSINE" => "COSINE",
            "DOT_PRODUCT" | "DOTPRODUCT" | "DOT" | "INNER_PRODUCT" | "IP" => "DOT_PRODUCT",
            _ => raw,
        };
        Value::Str(df.to_string())
    }
);

// =================================================================
// Metadata facet nodes
// =================================================================

/// Handle to a loaded variable-length i32 facet (metadata_indices).
pub(crate) struct Ivvec32Dataset {
    reader: Arc<dyn VvecReader<i32>>,
    count: usize,
}

impl Ivvec32Dataset {
    fn load(source: &str, profile: &str) -> Result<Arc<Self>, String> {
        let key = (source.to_string(), profile.to_string(), "metadata_indices".to_string());
        let any = FACET_CACHE.get_or_init(key, || {
            let group = load_dataset_group(source)?;
            let view = group.profile(profile)
                .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
            crate::audit::record_opened(source, profile, "metadata_indices", "ivvec32");
            let reader = run_blocking_io(|| view.metadata_indices())
                .map_err(|e| format!("failed to access metadata_indices from '{source}': {e}"))?;
            let count = reader.count();
            let arc: Arc<Self> = Arc::new(Self { reader, count });
            Ok(arc as Arc<dyn std::any::Any + Send + Sync>)
        })?;
        any.downcast::<Self>()
            .map_err(|_| format!(
                "facet cache type mismatch for '{source}:{profile}/metadata_indices'"))
    }

}

handle_indexed_node!(
    /// Access metadata indices (variable-length matching base ordinals
    /// per query) at an index. Expects an Ivvec32 handle.
    ///
    /// Signature: `metadata_indices_at(handle, index: u64) -> VecI32`
    MetadataIndicesAt, "metadata_indices_at", VecI32, facet = "metadata_indices", eval = ivvec32_vec_at
);

handle_indexed_node!(
    /// Return the length of one metadata_indices record without
    /// loading the data (reads only the 4-byte header). Expects an
    /// Ivvec32 handle.
    ///
    /// Signature: `metadata_indices_len_at(handle, index: u64) -> (u64)`
    MetadataIndicesLenAt, "metadata_indices_len_at", U64, facet = "metadata_indices",
    eval = |h: &DatasetHandle, idx: usize| match h {
        DatasetHandle::Ivvec32(d) if d.count > 0 => {
            let len = d.reader.dim_at(idx % d.count).unwrap_or(0);
            Value::U64(len as u64)
        }
        _ => Value::U64(0),
    }
);

handle_metadata_node!(
    /// Return the metadata indices count (number of predicate result
    /// sets). Expects an Ivvec32 handle.
    MetadataIndicesCount, "metadata_indices_count", U64,
    eval = |h: &DatasetHandle| match h {
        DatasetHandle::Ivvec32(d) => Value::U64(d.count as u64),
        _ => Value::U64(0),
    }
);

handle_metadata_node!(
    /// Report which facets are available for the default profile of
    /// a dataset group as a comma-separated list. Expects a Group
    /// handle.
    ///
    /// Signature: `dataset_facets(group) -> (String)`
    DatasetFacets, "dataset_facets", Str,
    eval = |h: &DatasetHandle| {
        let group = group_of(h);
        // Use the default profile — group-level callers want a
        // dataset-wide manifest; specific profile facets come via
        // `profile_facets(group, idx)`.
        let names = group.profile_names();
        if let Some(first) = names.first() {
            if let Some(view) = group.profile(first) {
                let manifest = view.facet_manifest();
                let mut names: Vec<&String> = manifest.keys().collect();
                names.sort();
                return Value::Str(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                );
            }
        }
        Value::Str(String::new())
    }
);

// =================================================================
// Profile enumeration — discover and iterate over dataset profiles
// =================================================================
//
// Group-level: take a Group handle, read the dataset's sorted profile
// list. Sort order is canonical (by base_count via `profile_sort_by_size`)
// and computed once per group via the underlying TestDataGroup.

handle_metadata_node!(
    /// Total number of profiles in a dataset group.
    ///
    /// Signature: `dataset_profile_count(group) -> (u64)`
    DatasetProfileCount, "dataset_profile_count", U64,
    eval = |h: &DatasetHandle| Value::U64(group_of(h).profile_names().len() as u64)
);

handle_metadata_node!(
    /// Comma-separated list of all profile names in canonical sort
    /// order (by base_count).
    ///
    /// Signature: `dataset_profile_names(group) -> (String)`
    DatasetProfileNames, "dataset_profile_names", Str,
    eval = |h: &DatasetHandle| Value::Str(group_of(h).profile_names().join(", "))
);

/// Return profile names matching a prefix, comma-separated.
///
/// Signature: `matching_profiles(group, prefix: str) -> (String)`
///
/// If prefix is empty, returns all profiles. Used by `for_each:`
/// phase templates to discover profiles dynamically.
pub struct MatchingProfiles {
    meta: NodeMeta,
}

impl MatchingProfiles {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "matching_profiles".into(),
                outs: vec![Port::str("output")],
                ins: vec![
                    Slot::Wire(Port::handle("group")),
                    Slot::Wire(Port::str("prefix")),
                ],
            },
        }
    }
}

impl Default for MatchingProfiles {
    fn default() -> Self { Self::new() }
}

impl GkNode for MatchingProfiles {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let group = group_of(handle_of(&inputs[0]));
        let prefix = inputs[1].as_str();
        let all = group.profile_names();
        let mut matched: Vec<&str> = if prefix.is_empty() {
            all.iter().map(|s| s.as_str()).collect()
        } else {
            all.iter()
                .filter(|s| s.starts_with(prefix))
                .map(|s| s.as_str())
                .collect()
        };
        // Natural-order sort: alphabetic with numeric runs
        // compared as numbers so `label_03` sorts before
        // `label_10`. The upstream `group.profile_names()`
        // orders by `base_count` (vectordata's choice — useful
        // for index-based lookups), but `for_each` iteration
        // wants stable, human-natural order so users see
        // label_01, label_02, label_03 instead of whatever the
        // size-sort happens to produce.
        matched.sort_by(|a, b| natural_cmp(a, b));
        outputs[0] = Value::Str(matched.join(","));
    }
}

/// Natural ordering: split each string into alternating text
/// and numeric runs and compare run-by-run, comparing numeric
/// runs as integers. Beats lexicographic on `label_03` vs
/// `label_10` (lex: "10" < "3"; natural: 3 < 10).
fn natural_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let mut ai = a.chars().peekable();
    let mut bi = b.chars().peekable();
    loop {
        match (ai.peek().copied(), bi.peek().copied()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, _)    => return std::cmp::Ordering::Less,
            (_, None)    => return std::cmp::Ordering::Greater,
            (Some(ac), Some(bc)) => {
                if ac.is_ascii_digit() && bc.is_ascii_digit() {
                    let mut na: u64 = 0;
                    while let Some(c) = ai.peek().copied()
                        && c.is_ascii_digit()
                    {
                        na = na.saturating_mul(10).saturating_add((c as u8 - b'0') as u64);
                        ai.next();
                    }
                    let mut nb: u64 = 0;
                    while let Some(c) = bi.peek().copied()
                        && c.is_ascii_digit()
                    {
                        nb = nb.saturating_mul(10).saturating_add((c as u8 - b'0') as u64);
                        bi.next();
                    }
                    match na.cmp(&nb) {
                        std::cmp::Ordering::Equal => continue,
                        non_eq => return non_eq,
                    }
                } else {
                    match ac.cmp(&bc) {
                        std::cmp::Ordering::Equal => { ai.next(); bi.next(); }
                        non_eq => return non_eq,
                    }
                }
            }
        }
    }
}

/// Look up a profile name by index from the canonical sorted list.
///
/// Signature: `dataset_profile_name_at(group, index: u64) -> (String)`
///
/// Index wraps modulo the number of profiles.
pub struct DatasetProfileNameAt {
    meta: NodeMeta,
}

impl DatasetProfileNameAt {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "dataset_profile_name_at".into(),
                outs: vec![Port::str("output")],
                ins: vec![
                    Slot::Wire(Port::handle("group")),
                    Slot::Wire(Port::u64("index")),
                ],
            },
        }
    }
}

impl Default for DatasetProfileNameAt {
    fn default() -> Self { Self::new() }
}

impl GkNode for DatasetProfileNameAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let group = group_of(handle_of(&inputs[0]));
        let idx = inputs[1].as_u64() as usize;
        let names = group.profile_names();
        outputs[0] = if names.is_empty() {
            Value::Str(String::new())
        } else {
            Value::Str(names[idx % names.len()].clone())
        };
    }
}

/// Return the base vector count for the profile at a given index.
///
/// Signature: `profile_base_count(group, index: u64) -> (u64)`
pub struct ProfileBaseCount {
    meta: NodeMeta,
}

impl ProfileBaseCount {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "profile_base_count".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::handle("group")),
                    Slot::Wire(Port::u64("index")),
                ],
            },
        }
    }
}

impl Default for ProfileBaseCount {
    fn default() -> Self { Self::new() }
}

impl GkNode for ProfileBaseCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let group = group_of(handle_of(&inputs[0]));
        let idx = inputs[1].as_u64() as usize;
        let names = group.profile_names();
        let result = if names.is_empty() {
            0
        } else {
            let name = &names[idx % names.len()];
            group
                .profile(name)
                .and_then(|view| view.base_count())
                .unwrap_or(0)
        };
        outputs[0] = Value::U64(result);
    }
}

/// Return the comma-separated facet list for the profile at a given index.
///
/// Signature: `profile_facets(group, index: u64) -> (String)`
pub struct ProfileFacets {
    meta: NodeMeta,
}

impl ProfileFacets {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "profile_facets".into(),
                outs: vec![Port::str("output")],
                ins: vec![
                    Slot::Wire(Port::handle("group")),
                    Slot::Wire(Port::u64("index")),
                ],
            },
        }
    }
}

impl Default for ProfileFacets {
    fn default() -> Self { Self::new() }
}

impl GkNode for ProfileFacets {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let group = group_of(handle_of(&inputs[0]));
        let idx = inputs[1].as_u64() as usize;
        let names = group.profile_names();
        let facets = if names.is_empty() {
            String::new()
        } else {
            let name = &names[idx % names.len()];
            match group.profile(name) {
                Some(view) => {
                    let manifest = view.facet_manifest();
                    let mut fnames: Vec<&String> = manifest.keys().collect();
                    fnames.sort();
                    fnames
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
                None => String::new(),
            }
        };
        outputs[0] = Value::Str(facets);
    }
}

// =================================================================
// Prebuffering — eagerly download dataset facets before workload run
// =================================================================

source_only_node!(
    /// Eagerly download all facets for a dataset profile into the
    /// local cache, returning a [`DatasetHandle::Group`] handle
    /// that downstream facet accessors take as their first
    /// argument. After this returns, every subsequent facet read
    /// served by [`vectordata::TestDataView`] hits the merkle-
    /// verified mmap fast path with no further network traffic.
    ///
    /// Signature: `dataset_prebuffer(source) -> Handle`
    ///
    /// **Why a handle, not a count.** Returning a value the
    /// downstream accessors *consume* makes prebuffer part of
    /// the dataflow graph: DCE keeps the chain alive because
    /// `vector_at(prebuffered, q)` needs `prebuffered` to be
    /// resolvable, which forces evaluation. Bindings such as
    /// `init prebuffered = dataset_prebuffer(...)` whose result
    /// nothing reads would still be pruned (TODO: rationalise
    /// dangling-init dataflow as a separate followup; the
    /// established pattern is to thread the handle through).
    ///
    /// `source` is the canonical `dataset:profile` string. The
    /// returned handle is a `Group` handle — accessors that take
    /// it route through the same `DatasetHandle::open(...)`
    /// resolver path used by other group-aware nodes
    /// (`dataset_facets`, `dataset_distance_function`, ...).
    ///
    /// Errors during prebuffer surface via stderr; the node
    /// still returns the group handle so downstream binds don't
    /// fail with a "missing value" cascade — the operator's
    /// intent is "best effort warm-up", and a workload that
    /// wants strict guarantees can wrap this in a `required(...)`
    /// predicate or check facet readiness explicitly.
    DatasetPrebuffer, "dataset_prebuffer",
    out_port = Handle,
    eval = |source| do_dataset_prebuffer(source)
);

/// Implementation behind the `dataset_prebuffer` node — kept
/// out of the macro body so we can use `?`/early return
/// without fighting the closure-vs-eval-fn return type
/// mismatch the `source_only_node!` macro embeds.
fn do_dataset_prebuffer(source: &str) -> Value {
    // The init-binding contract (SRD 11) means this function is
    // expected to fire exactly once per scope activation: Plan B
    // pulls the binding on the activation kernel; OpBuilder's
    // init_overrides propagate the result to every fiber. The
    // PREBUFFER_CACHE below is belt-and-suspenders: it serializes
    // anyone who ends up calling here through a non-init path,
    // and makes per-source thundering-herd structurally
    // impossible regardless of upstream caller behavior.
    match PREBUFFER_CACHE.get_or_init(source.to_string(), || {
        // Only the first concurrent caller for this source runs
        // the inner body — the audit "entered" event reflects
        // that single download, not per-caller noise.
        crate::audit::record_prebuffer_entered(source);
        do_dataset_prebuffer_inner(source)
    }) {
        Ok(handle) => Value::handle(handle),
        Err(_) => Value::None,
    }
}

/// Inner body of [`do_dataset_prebuffer`]. Runs **at most once
/// per (source, process)** under the [`PREBUFFER_CACHE`] OnceLock.
fn do_dataset_prebuffer_inner(source: &str) -> Result<Arc<DatasetHandle>, String> {
    // Return a Group handle in every exit (success or error) so
    // the downstream `*_at(prebuffered, q)` accessors can resolve.
    // Failed prebuffer still hands back the group handle — the
    // accessors will then HTTP-fall-through, and the operator
    // sees the prebuffer error in the audit log.
    let group = match load_dataset_group(source) {
        Ok(g) => g,
        Err(e) => {
            let msg = format!("dataset_prebuffer: cannot resolve '{source}': {e}");
            crate::audit::error(&msg);
            // No group → no handle to hand back. Sticky error in
            // the cache; downstream accessors will produce a
            // diagnostic when they fail to downcast.
            return Err(msg);
        }
    };
    let group_for_handle = group.clone();
    let (_, profile) = parse_source_specifier(source);
    let view = match group.profile(profile) {
        Some(v) => v,
        None => {
            crate::audit::error(&format!(
                "dataset_prebuffer: profile '{profile}' not found in '{source}'"));
            return Ok(Arc::new(DatasetHandle::Prebuffered {
                _group: group_for_handle,
                source: source.to_string(),
            }));
        }
    };
    // vectordata's default `prebuffer_all_with_progress`
    // walks the manifest and calls `FacetStorage::prebuffer`
    // per facet — works for both record-shaped (xvec) and
    // scalar (typed) facets after the storage-transport
    // refactor in vectordata 1.0.0.
    //
    // Audit instrumentation: log every facet the prebuffer
    // *covers* with its key (`source:profile/facet`). Compared
    // against the `vectordata: opened …` lines emitted by
    // [`load_uniform_facet`] / [`GenericFacetDataset::load`],
    // this surfaces any facet the workload reads at cycle time
    // that prebuffer did NOT pull — the typical cause of a
    // "still hitting HTTP after prebuffer" symptom.
    // Manual facet walk so we can hook the per-chunk
    // `DownloadProgress` callback (vectordata's
    // `view.prebuffer_all_with_progress` only fires its outer cb
    // once per facet *completion*; the per-chunk cb is exposed
    // via `FacetStorage::prebuffer_with_progress`). Every facet's
    // chunk-level progress is throttled to ~1 Hz log emissions
    // and then a final per-facet `covered` line lands when its
    // download finishes.
    let mut facet_count: u64 = 0;
    let mut last_log_at = std::time::Instant::now();
    let log_interval = std::time::Duration::from_secs(1);
    for (name, _descriptor) in view.facet_manifest() {
        // Skip facets with unrecognised element types (vectordata's
        // own default impl skips these — they're not data facets the
        // typed reader would touch).
        if view.facet_element_type(&name).is_err() { continue; }

        let storage = match run_blocking_io(|| view.open_facet_storage(&name)) {
            Ok(s) => s,
            Err(e) => {
                crate::audit::warn(&format!(
                    "dataset_prebuffer: open '{name}' for prebuffer failed: {e}"));
                continue;
            }
        };

        // Inline the closure at the call site so Rust's type
        // inference can read `&DownloadProgress` straight from
        // the trait bound on `prebuffer_with_progress`.
        // The `DownloadProgress` type is `pub(crate)` upstream
        // (vectordata 1.0.2) so we can't name it ourselves.
        let prebuf_source  = source.to_string();
        let prebuf_profile = profile.to_string();
        let prebuf_facet   = name.clone();
        let mut last_done: u64 = 0;
        // `prebuffer_with_progress` drives `reqwest::blocking`,
        // which spins up a private tokio runtime per request.
        // Without parking the outer worker via `block_in_place`,
        // dropping that inner runtime inside an async context
        // panics with "Cannot drop a runtime in a context where
        // blocking is not allowed". Same pattern as
        // `load_dataset_group` and `load_uniform_facet`.
        let prebuf_result = run_blocking_io(|| storage.prebuffer_with_progress(|p| {
            // Rate-limit to one line per second per facet so a
            // gigabyte-sized facet doesn't spew thousands of
            // progress lines into session.log.
            let now = std::time::Instant::now();
            if now.duration_since(last_log_at) < log_interval {
                return;
            }
            last_log_at = now;
            let total_b   = p.total_bytes();
            let done_b    = p.downloaded_bytes();
            let total_c   = p.total_chunks();
            let done_c    = p.completed_chunks();
            let pct       = p.fraction() * 100.0;
            let delta_mb  = (done_b.saturating_sub(last_done)) as f64 / (1024.0 * 1024.0);
            last_done = done_b;
            crate::audit::info(&format!(
                "prebuffer: progress {prebuf_source}:{prebuf_profile}/{prebuf_facet} \
                 {pct:5.1}% ({done_c}/{total_c} chunks, \
                 {done_mb:.1}/{total_mb:.1} MB, +{delta_mb:.1} MB last sec)",
                done_mb  = done_b  as f64 / (1024.0 * 1024.0),
                total_mb = total_b as f64 / (1024.0 * 1024.0),
            ));
        }));
        if let Err(e) = prebuf_result {
            crate::audit::warn(&format!(
                "dataset_prebuffer: download error for '{source}' facet '{name}': {e}"));
            continue;
        }
        facet_count = facet_count.saturating_add(1);
        crate::audit::record_prebuffered(source, profile, &name);
    }
    crate::audit::log_prebuffer_summary(source, profile, facet_count);
    Ok(Arc::new(DatasetHandle::Prebuffered {
        _group: group_for_handle,
        source: source.to_string(),
    }))
}

// =================================================================
// Generic facet access — type-aware scalar/vector readers
// =================================================================

/// A type-erased facet reader that stores values as i64.
/// Uses `generic_view().open_facet_typed::<i64>()` which handles all
/// element types (u8, i32, etc.), caching, and local/remote access.
pub(crate) struct GenericFacetDataset {
    reader: vectordata::typed_access::TypedReader<i64>,
    count: usize,
}

impl GenericFacetDataset {
    fn load(source: &str, profile: &str, facet: &str) -> Result<Arc<Self>, String> {
        let key = (source.to_string(), profile.to_string(), facet.to_string());
        let any = FACET_CACHE.get_or_init(key, || {
            let group = load_dataset_group(source)?;
            let gv = group.generic_view(profile)
                .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
            crate::audit::record_opened(source, profile, facet, "generic-typed");
            let reader = run_blocking_io(|| gv.open_facet_typed::<i64>(facet))
                .map_err(|e| format!("failed to open {facet} from '{source}:{profile}': {e}"))?;
            let count = reader.count();
            let arc: Arc<Self> = Arc::new(Self { reader, count });
            Ok(arc as Arc<dyn std::any::Any + Send + Sync>)
        })?;
        any.downcast::<Self>()
            .map_err(|_| format!(
                "facet cache type mismatch for '{source}:{profile}/{facet}'"))
    }

    fn get_scalar(&self, index: usize) -> i64 {
        if self.count == 0 { return 0; }
        self.reader.get_value(index % self.count).unwrap_or(0)
    }

    fn format_scalar(&self, index: usize) -> String {
        self.get_scalar(index).to_string()
    }
}

// Generic-facet readers (typed scalar). Take a Generic handle and
// read the i64-cast value at the requested index.

fn generic_str_at(h: &DatasetHandle, idx: usize) -> Value {
    match h {
        DatasetHandle::Generic(d) => Value::Str(d.format_scalar(idx)),
        _ => Value::Str(String::new()),
    }
}

handle_indexed_node!(
    /// Access a metadata value per base ordinal. Expects a Generic
    /// handle opened against the `metadata_content` facet.
    ///
    /// Signature: `metadata_value_at(handle, index: u64) -> (String)`
    MetadataValueAt, "metadata_value_at", Str, facet = "metadata_content", eval = generic_str_at
);

handle_indexed_node!(
    /// Access a predicate value per query ordinal. Expects a Generic
    /// handle opened against the `metadata_predicates` facet.
    ///
    /// Signature: `predicate_value_at(handle, index: u64) -> (String)`
    PredicateValueAt, "predicate_value_at", Str, facet = "metadata_predicates", eval = generic_str_at
);

handle_metadata_node!(
    /// Count of metadata content records. Expects a Generic handle.
    ///
    /// Signature: `metadata_content_count(handle) -> (u64)`
    MetadataContentCount, "metadata_content_count", U64,
    eval = |h: &DatasetHandle| match h {
        DatasetHandle::Generic(d) => Value::U64(d.count as u64),
        _ => Value::U64(0),
    }
);

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, DefaultResolver, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

// Macros to keep the bulk of the registry compact and consistent.
// Per SRD 53 §"Source-string call-site sugar": each handle-taking
// accessor declares a `default_resolver` so the binding compiler
// can promote a string source into the right resolver call.

macro_rules! sig_handle_indexed {
    ($name:literal, $resolver:expr, $desc:literal, $help:literal) => {
        FuncSig {
            name: $name, category: FuncCategory::RealData, outputs: 1,
            description: $desc, help: $help,
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "handle", slot_type: SlotType::Wire, required: true, example: "base", constraint: None },
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: Some($resolver),
        }
    };
}

macro_rules! sig_handle_metadata {
    ($name:literal, $resolver:expr, $desc:literal, $help:literal) => {
        FuncSig {
            name: $name, category: FuncCategory::RealData, outputs: 1,
            description: $desc, help: $help,
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "handle", slot_type: SlotType::Wire, required: true, example: "base", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: Some($resolver),
        }
    };
}

/// Signatures for vector dataset access nodes (feature-gated).
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        // ===== Resolvers =====
        FuncSig {
            name: "dataset_open", category: C::RealData, outputs: 1,
            description: "open a dataset facet, returning a handle",
            help: "Resolve a (source, facet) pair to a typed handle. The handle\nflows on a wire to per-cycle accessors, which downcast and read\nat the requested index — no per-cycle string lookup.\nProvenance follows source/facet inputs; with scope-extern\ninputs this evaluates once per iteration via the engine's\nstandard provenance caching.\nExample: init base = dataset_open(\"glove-25\", \"base\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "source", slot_type: SlotType::Wire, required: true, example: "\"glove-25\"", constraint: None },
                ParamSpec { name: "facet", slot_type: SlotType::Wire, required: true, example: "\"base\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "dataset_group_open", category: C::RealData, outputs: 1,
            description: "open a dataset group, returning a handle",
            help: "Resolve a dataset to a group-level handle. Used by group\nmetadata accessors (dataset_profile_count, dataset_facets, ...)\nthat operate on TestDataGroup before any profile/facet is selected.\nExample: init group = dataset_group_open(\"glove-25\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "source", slot_type: SlotType::Wire, required: true, example: "\"glove-25\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },

        // ===== Per-cycle facet accessors (typed-vector outputs) =====
        sig_handle_indexed!("vector_at", DefaultResolver::Facet("base"),
            "access f32 vector by index",
            "Read an f32 vector from a facet handle as a typed VecF32.\nAuto-promotes a string source via dataset_open(_,\"base\").\nExample: vector_at(base, cycle)"),
        sig_handle_indexed!("query_vector_at", DefaultResolver::Facet("query"),
            "access query vector by index",
            "Alias for vector_at over a query-facet handle.\nAuto-promotes a string source via dataset_open(_,\"query\")."),
        sig_handle_indexed!("neighbor_indices_at", DefaultResolver::Facet("neighbor_indices"),
            "ground-truth neighbor indices for a query",
            "Read ground-truth k-nearest neighbor indices for a query as a\ntyped VecI32. Auto-promotes a string source via\ndataset_open(_,\"neighbor_indices\")."),
        sig_handle_indexed!("neighbor_distances_at", DefaultResolver::Facet("neighbor_distances"),
            "ground-truth neighbor distances for a query",
            "Read ground-truth distances for a query's k-nearest neighbors\nas a typed VecF32."),
        sig_handle_indexed!("filtered_neighbor_indices_at", DefaultResolver::Facet("filtered_neighbor_indices"),
            "filtered ground-truth neighbor indices",
            "Read filtered ground-truth indices for a query as a typed VecI32.\nUsed for filtered-ANN recall verification."),
        sig_handle_indexed!("filtered_neighbor_distances_at", DefaultResolver::Facet("filtered_neighbor_distances"),
            "filtered ground-truth neighbor distances",
            "Read filtered ground-truth distances for a query as a typed VecF32."),
        sig_handle_indexed!("metadata_indices_len_at", DefaultResolver::Facet("metadata_indices"),
            "length of metadata indices for a query",
            "Return the per-record matching-base count for a query without\nloading the full index list (reads only the 4-byte header)."),
        sig_handle_indexed!("metadata_indices_at", DefaultResolver::Facet("metadata_indices"),
            "matching base ordinals for a query predicate",
            "Variable-length list of base vector ordinals matching a query's\npredicate."),
        sig_handle_indexed!("metadata_value_at", DefaultResolver::Facet("metadata_content"),
            "scalar metadata value per base vector",
            "Read a metadata value for a base vector by ordinal.\nReads from the metadata_content facet."),
        sig_handle_indexed!("predicate_value_at", DefaultResolver::Facet("metadata_predicates"),
            "scalar predicate value per query",
            "Read a predicate value for a query by ordinal.\nReads from the metadata_predicates facet."),

        // ===== Per-handle metadata =====
        sig_handle_metadata!("vector_dim", DefaultResolver::Facet("base"),
            "vector dimensionality of a facet handle",
            "Return the per-record element count (dimension) of a vector facet."),
        sig_handle_metadata!("vector_count", DefaultResolver::Facet("base"),
            "record count of a facet handle",
            "Return the number of records in a facet handle (base vectors,\nquery vectors, ...). Auto-promotes a string source via\ndataset_open(_,\"base\")."),
        sig_handle_metadata!("query_count", DefaultResolver::Facet("query"),
            "record count of a query-facet handle",
            "Same as vector_count but defaults to dataset_open(_,\"query\")\nfor string sources."),
        sig_handle_metadata!("neighbor_count", DefaultResolver::Facet("neighbor_indices"),
            "ground-truth neighbors per query (maxk)",
            "Return the per-record neighbor count (k) of a neighbor-indices handle."),
        sig_handle_metadata!("metadata_indices_count", DefaultResolver::Facet("metadata_indices"),
            "number of predicate result sets",
            "Return the record count of a metadata-indices handle."),
        sig_handle_metadata!("metadata_content_count", DefaultResolver::Facet("metadata_content"),
            "number of metadata content records",
            "Return the record count of a metadata-content handle."),

        // ===== Group-level (Group handle) =====
        sig_handle_metadata!("dataset_distance_function", DefaultResolver::Group,
            "dataset distance/similarity function name",
            "Return the distance function declared in the dataset metadata\n('COSINE','EUCLIDEAN','DOT_PRODUCT','MANHATTAN'). Group-level."),
        sig_handle_metadata!("dataset_facets", DefaultResolver::Group,
            "list available facets in default profile",
            "Comma-separated facet names available in the group's default profile."),
        sig_handle_metadata!("dataset_profile_count", DefaultResolver::Group,
            "total number of profiles in a dataset",
            "Number of profiles defined in the dataset group."),
        sig_handle_metadata!("dataset_profile_names", DefaultResolver::Group,
            "comma-separated list of profile names",
            "All profile names in canonical sort order (by base_count)."),
        FuncSig {
            name: "matching_profiles", category: C::RealData, outputs: 1,
            description: "profile names matching a prefix",
            help: "Profile names from a group handle that start with the given prefix.\nIf prefix is empty, returns all profile names. Used by for_each:\nphase templates to discover profiles dynamically.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "group", slot_type: SlotType::Wire, required: true, example: "group", constraint: None },
                ParamSpec { name: "prefix", slot_type: SlotType::Wire, required: false, example: "\"label_\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: Some(DefaultResolver::Group),
        },
        FuncSig {
            name: "dataset_profile_name_at", category: C::RealData, outputs: 1,
            description: "profile name by index from sorted list",
            help: "Profile name at a given index from the canonical sorted list.\nIndex wraps modulo profile count.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "group", slot_type: SlotType::Wire, required: true, example: "group", constraint: None },
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: Some(DefaultResolver::Group),
        },
        FuncSig {
            name: "profile_base_count", category: C::RealData, outputs: 1,
            description: "base vector count for profile at index",
            help: "Base vector count of the profile at a given index in the dataset's\ncanonical sorted profile list.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "group", slot_type: SlotType::Wire, required: true, example: "group", constraint: None },
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: Some(DefaultResolver::Group),
        },
        FuncSig {
            name: "profile_facets", category: C::RealData, outputs: 1,
            description: "available facets for profile at index",
            help: "Comma-separated facet names for the profile at a given index in\nthe dataset's canonical sorted profile list.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "group", slot_type: SlotType::Wire, required: true, example: "group", constraint: None },
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: Some(DefaultResolver::Group),
        },

        // ===== Side-effect resolver =====
        FuncSig {
            name: "dataset_prebuffer", category: C::RealData, outputs: 1,
            description: "eagerly download dataset facets to local cache",
            help: "Downloads all facets for a dataset to the local cache. Returns 0\n(side-effect resolver). Subsequent loads use fast local mmap access.\nKept on a string source — typically called once per workload at\ninit time.\nExample: init _pb = dataset_prebuffer(\"example\")",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::Wire, required: true, example: "\"test\"", constraint: None }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
    ]
}

/// Try to build a vector dataset node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
/// All functions in this module are feature-gated on `vectordata`.
#[cfg(feature = "vectordata")]
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    // Every dataset function in this module now takes its
    // `source` (and any other previously-const string params)
    // as a Wire input, so `consts` is unused — the spec arrives
    // through `inputs[i]` at eval time. Literal-string args
    // are auto-lifted to anonymous `ConstStr` wire nodes by the
    // binding compiler; non-literal args (e.g. `printf` from a
    // string-interpolated source spec) wire directly.
    match name {
        "dataset_open" => Some(Ok(Box::new(DatasetOpen::new()) as Box<dyn crate::node::GkNode>)),
        "dataset_group_open" => Some(Ok(Box::new(DatasetGroupOpen::new()) as Box<dyn crate::node::GkNode>)),
        "vector_at" => Some(Ok(Box::new(VectorAt::new()) as Box<dyn crate::node::GkNode>)),
        "query_vector_at" => Some(Ok(Box::new(QueryVectorAt::new()) as Box<dyn crate::node::GkNode>)),
        "neighbor_indices_at" => Some(Ok(Box::new(NeighborIndicesAt::new()) as Box<dyn crate::node::GkNode>)),
        "neighbor_distances_at" => Some(Ok(Box::new(NeighborDistancesAt::new()) as Box<dyn crate::node::GkNode>)),
        "filtered_neighbor_indices_at" => Some(Ok(Box::new(FilteredNeighborIndicesAt::new()) as Box<dyn crate::node::GkNode>)),
        "filtered_neighbor_distances_at" => Some(Ok(Box::new(FilteredNeighborDistancesAt::new()) as Box<dyn crate::node::GkNode>)),
        "dataset_distance_function" => Some(Ok(Box::new(DatasetDistanceFunction::new()) as Box<dyn crate::node::GkNode>)),
        "vector_dim" => Some(Ok(Box::new(VectorDim::new()) as Box<dyn crate::node::GkNode>)),
        "vector_count" => Some(Ok(Box::new(VectorCount::new()) as Box<dyn crate::node::GkNode>)),
        "query_count" => Some(Ok(Box::new(QueryCount::new()) as Box<dyn crate::node::GkNode>)),
        "neighbor_count" => Some(Ok(Box::new(NeighborCount::new()) as Box<dyn crate::node::GkNode>)),
        "metadata_indices_len_at" => Some(Ok(Box::new(MetadataIndicesLenAt::new()) as Box<dyn crate::node::GkNode>)),
        "metadata_indices_at" => Some(Ok(Box::new(MetadataIndicesAt::new()) as Box<dyn crate::node::GkNode>)),
        "metadata_indices_count" => Some(Ok(Box::new(MetadataIndicesCount::new()) as Box<dyn crate::node::GkNode>)),
        "dataset_facets" => Some(Ok(Box::new(DatasetFacets::new()) as Box<dyn crate::node::GkNode>)),
        "dataset_profile_count" => Some(Ok(Box::new(DatasetProfileCount::new()) as Box<dyn crate::node::GkNode>)),
        "dataset_profile_names" => Some(Ok(Box::new(DatasetProfileNames::new()) as Box<dyn crate::node::GkNode>)),
        "matching_profiles" => Some(Ok(Box::new(MatchingProfiles::new()) as Box<dyn crate::node::GkNode>)),
        "dataset_profile_name_at" => Some(Ok(Box::new(DatasetProfileNameAt::new()) as Box<dyn crate::node::GkNode>)),
        "profile_base_count" => Some(Ok(Box::new(ProfileBaseCount::new()) as Box<dyn crate::node::GkNode>)),
        "profile_facets" => Some(Ok(Box::new(ProfileFacets::new()) as Box<dyn crate::node::GkNode>)),
        "dataset_prebuffer" => Some(Ok(Box::new(DatasetPrebuffer::new()) as Box<dyn crate::node::GkNode>)),
        "metadata_value_at" => Some(Ok(Box::new(MetadataValueAt::new()) as Box<dyn crate::node::GkNode>)),
        "predicate_value_at" => Some(Ok(Box::new(PredicateValueAt::new()) as Box<dyn crate::node::GkNode>)),
        "metadata_content_count" => Some(Ok(Box::new(MetadataContentCount::new()) as Box<dyn crate::node::GkNode>)),
        _ => None,
    }
}

#[cfg(feature = "vectordata")]
crate::register_nodes!(signatures, build_node);

// =========================================================================
// Cursor-sugar handlers (SRD 18 §"Source-driven workloads")
// =========================================================================
//
// Three sugar forms recognized by this module — none of them
// known to the core compiler, which dispatches generically
// through `dsl::cursor_sugar`. Each desugars to a synthetic
// `range(0, vector_count|query_count(...))` constructor plus
// auxiliary bindings:
//
//   - `__<cursor>_prebuffer := dataset_prebuffer("ds:profile")`
//     loaded once at init time so cycle-time accessor calls hit
//     prefetched memory.
//   - `<cursor>__vector := vector_at("ds:profile", <cursor>__ordinal)`
//     (or `query_vector_at` for the query
//     facet) — published as the cursor's `vector` projection so
//     workloads can reference `<cursor>.vector`.
//
// Forms:
//   - `vectordata_source(dataset, profile, facet)` — explicit facet
//   - `vectordata_base(dataset, profile)`           — facet = "base"
//   - `vectordata_query(dataset, profile)`          — facet = "query"
//
// Facet-specific projections like `metadata` / `ground_truth` /
// `predicate` stay explicit (the user writes
// `meta := metadata_value_at(<cursor>.ordinal, "ds:profile")` by
// hand) because their existence is dataset-conditional — not
// every dataset declares a metadata column or a predicate facet.

#[cfg(feature = "vectordata")]
fn vectordata_sugar(
    source_name: &str,
    constructor: &crate::dsl::ast::Expr,
) -> Result<Option<crate::dsl::cursor_sugar::CursorSugar>, String> {
    use crate::dsl::ast::{Arg, CallExpr, Expr};
    use crate::dsl::compile::positional_str_lit;
    use crate::dsl::cursor_sugar::{AuxBinding, CursorSugar};

    let Expr::Call(call) = constructor else { return Ok(None); };

    let (dataset, profile, facet) = match call.func.as_str() {
        "vectordata_source" => {
            let d = positional_str_lit(call.args.first()).ok_or_else(|| format!(
                "cursor '{source_name}': vectordata_source(dataset, profile, facet) — first arg must be a string literal"
            ))?;
            let p = positional_str_lit(call.args.get(1)).ok_or_else(|| format!(
                "cursor '{source_name}': vectordata_source(dataset, profile, facet) — second arg must be a string literal"
            ))?;
            let f = positional_str_lit(call.args.get(2)).ok_or_else(|| format!(
                "cursor '{source_name}': vectordata_source(dataset, profile, facet) — third arg must be a string literal (\"base\" or \"query\")"
            ))?;
            (d, p, f)
        }
        "vectordata_base" | "vectordata_query" => {
            let f = call.func.strip_prefix("vectordata_").unwrap().to_string();
            let d = positional_str_lit(call.args.first()).ok_or_else(|| format!(
                "cursor '{source_name}': {}(dataset, profile) — first arg must be a string literal",
                call.func,
            ))?;
            let p = positional_str_lit(call.args.get(1)).ok_or_else(|| format!(
                "cursor '{source_name}': {}(dataset, profile) — second arg must be a string literal",
                call.func,
            ))?;
            (d, p, f)
        }
        _ => return Ok(None),
    };

    if facet != "base" && facet != "query" {
        return Err(format!(
            "cursor '{source_name}': vectordata facet must be \"base\" or \"query\", got \"{facet}\""
        ));
    }
    let (count_func, vector_func) = match facet.as_str() {
        "base" => ("vector_count", "vector_at"),
        "query" => ("query_count", "query_vector_at"),
        _ => unreachable!(),
    };

    let combined = format!("{dataset}:{profile}");
    let span = call.span;
    let lit = |s: String| Expr::StringLit(s, span);
    let positional = |e: Expr| Arg::Positional(e);

    let effective_constructor = Expr::Call(CallExpr {
        func: "range".into(),
        args: vec![
            positional(Expr::IntLit(0, span)),
            positional(Expr::Call(CallExpr {
                func: count_func.into(),
                args: vec![positional(lit(combined.clone()))],
                span,
            })),
        ],
        span,
    });

    let prebuffer_binding = AuxBinding {
        name: format!("__{source_name}_prebuffer"),
        value: Expr::Call(CallExpr {
            func: "dataset_prebuffer".into(),
            args: vec![positional(lit(combined.clone()))],
            span,
        }),
        projection: None,
    };

    // Cursor sugar emits the call with the new (handle, index) order.
    // The combined source string auto-promotes to the right facet
    // handle via the binding compiler's call-site sugar (SRD 53).
    let vector_binding = AuxBinding {
        name: format!("{source_name}__vector"),
        value: Expr::Call(CallExpr {
            func: vector_func.into(),
            args: vec![
                positional(lit(combined)),
                positional(Expr::Ident(format!("{source_name}__ordinal"), span)),
            ],
            span,
        }),
        projection: Some(("vector".into(), crate::node::PortType::VecF32)),
    };

    Ok(Some(CursorSugar {
        effective_constructor,
        aux_bindings: vec![prebuffer_binding, vector_binding],
    }))
}

#[cfg(feature = "vectordata")]
inventory::submit! {
    crate::dsl::cursor_sugar::CursorSugarRegistration {
        handler: vectordata_sugar,
        name: "vectordata",
    }
}
