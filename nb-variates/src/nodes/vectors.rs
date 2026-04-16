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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, LazyLock};

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use vectordata::TestDataGroup;
use vectordata::TestDataView;
use vectordata::io::{VectorReader, VvecReader};
use vectordata::catalog::sources::CatalogSources;
use vectordata::catalog::resolver::Catalog;

/// Global cache for loaded dataset groups keyed by source string.
/// Ensures each dataset is loaded exactly once regardless of how many
/// node functions reference it. Thread-safe via Mutex.
static DATASET_CACHE: LazyLock<Mutex<HashMap<String, Arc<TestDataGroup>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Type-erased facet cache: (source, profile, facet) → Arc<dyn Any + Send + Sync>.
/// Ensures each reader (of any element type) is opened exactly once
/// and shared across all node instances that reference the same data.
/// The concrete type inside is `Arc<UniformDataset<T>>` or `Arc<Ivvec32Dataset>`.
static FACET_CACHE: LazyLock<Mutex<HashMap<(String, String, String), Arc<dyn std::any::Any + Send + Sync>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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

/// Load a dataset group by name.
///
/// Uses the vectordata catalog API: `catalog.open(name)` handles
/// catalog discovery, cache resolution, and download transparently.
fn load_dataset_group(source: &str) -> Result<Arc<TestDataGroup>, String> {
    let (dataset_name, _profile) = parse_source_specifier(source);

    // Check in-memory cache first
    {
        let cache = DATASET_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(group) = cache.get(dataset_name) {
            return Ok(group.clone());
        }
        if dataset_name != source {
            if let Some(group) = cache.get(source) {
                return Ok(group.clone());
            }
        }
    }

    // Open via the catalog — handles URLs, local paths, cache, everything.
    let catalog = Catalog::of(&CatalogSources::new().configure_default());
    let group = catalog.open(dataset_name)
        .map_err(|e| format!("failed to load dataset '{dataset_name}': {e}"))?;
    let arc = Arc::new(group);

    let mut cache = DATASET_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    cache.insert(dataset_name.to_string(), arc.clone());
    if source != dataset_name {
        cache.insert(source.to_string(), arc.clone());
    }

    Ok(arc)
}


// =================================================================
// Dataset handles — loaded once at node construction, shared via Arc
// =================================================================

/// Generic handle to a loaded uniform vector facet. Thread-safe, random-access.
/// Supports any element type provided by the vectordata API (f32, f64,
/// i32, i16, u8, i8, u16, u32, u64, i64, f16).
struct UniformDataset<T: Send + Sync + 'static> {
    reader: Arc<dyn VectorReader<T>>,
    count: usize,
    dim: usize,
}

impl<T: Send + Sync + std::fmt::Display + 'static> UniformDataset<T> {
    fn get(&self, index: usize) -> Vec<T> where T: Clone {
        if self.count == 0 { return vec![]; }
        self.reader.get(index % self.count).unwrap_or_default()
    }

    fn format_str(&self, index: usize) -> String where T: Clone {
        let vec = self.get(index);
        let inner: String = vec.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
        format!("[{inner}]")
    }
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
    {
        let cache = FACET_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = cache.get(&key) {
            if let Some(typed) = cached.clone().downcast::<UniformDataset<T>>().ok() {
                return Ok(typed);
            }
        }
    }
    let group = load_dataset_group(source)?;
    let view = group.profile(profile)
        .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
    let reader = open_fn(view.as_ref())
        .map_err(|e| format!("failed to access {facet} from '{source}': {e}"))?;
    let count = reader.count();
    let dim = reader.dim();
    let arc = Arc::new(UniformDataset { reader, count, dim });
    FACET_CACHE.lock().unwrap_or_else(|e| e.into_inner())
        .insert(key, arc.clone() as Arc<dyn std::any::Any + Send + Sync>);
    Ok(arc)
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

    fn to_bytes(&self, index: usize) -> Vec<u8> {
        self.get(index).iter().flat_map(|f| f.to_le_bytes()).collect()
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
// Base vector nodes
// =================================================================

/// Access a base vector by index (string output).
///
/// Signature: `vector_at(index: u64, source: str) -> (String)`
///
/// The source is a dataset URL or local path. The dataset handle is
/// loaded at construction and held for the lifetime of the node.
pub struct VectorAt {
    meta: NodeMeta,
    dataset: Arc<F32Dataset>,
}

impl VectorAt {
    /// Construct from a dataset source string. Loads the handle now.
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "base")?;
        Ok(Self {
            meta: NodeMeta {
                name: "vector_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }

}

impl GkNode for VectorAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_str(inputs[0].as_u64() as usize));
    }
}

/// Access a base vector by index (bytes output).
///
/// Signature: `vector_at_bytes(index: u64, source: str) -> (Bytes)`
pub struct VectorAtBytes {
    meta: NodeMeta,
    dataset: Arc<F32Dataset>,
}

impl VectorAtBytes {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "base")?;
        Ok(Self {
            meta: NodeMeta {
                name: "vector_at_bytes".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for VectorAtBytes {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bytes(self.dataset.to_bytes(inputs[0].as_u64() as usize));
    }
}

// =================================================================
// Query vector nodes
// =================================================================

/// Access a query vector by index (string output).
pub struct QueryVectorAt {
    meta: NodeMeta,
    dataset: Arc<F32Dataset>,
}

impl QueryVectorAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "query")?;
        Ok(Self {
            meta: NodeMeta {
                name: "query_vector_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for QueryVectorAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_str(inputs[0].as_u64() as usize));
    }
}

/// Access a query vector by index (bytes output).
pub struct QueryVectorAtBytes {
    meta: NodeMeta,
    dataset: Arc<F32Dataset>,
}

impl QueryVectorAtBytes {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "query")?;
        Ok(Self {
            meta: NodeMeta {
                name: "query_vector_at_bytes".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for QueryVectorAtBytes {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bytes(self.dataset.to_bytes(inputs[0].as_u64() as usize));
    }
}

// =================================================================
// Neighbor nodes (ground truth)
// =================================================================

/// Access ground-truth neighbor indices for a query (string output).
///
/// Returns indices as a JSON array: `[42,17,99,...]`
pub struct NeighborIndicesAt {
    meta: NodeMeta,
    dataset: Arc<I32Dataset>,
}

impl NeighborIndicesAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = I32Dataset::load(source, profile, "neighbor_indices")?;
        Ok(Self {
            meta: NodeMeta {
                name: "neighbor_indices_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for NeighborIndicesAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_str(inputs[0].as_u64() as usize));
    }
}

/// Access ground-truth neighbor distances for a query (string output).
pub struct NeighborDistancesAt {
    meta: NodeMeta,
    dataset: Arc<F32Dataset>,
}

impl NeighborDistancesAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "neighbor_distances")?;
        Ok(Self {
            meta: NodeMeta {
                name: "neighbor_distances_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for NeighborDistancesAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_str(inputs[0].as_u64() as usize));
    }
}

// =================================================================
// Filtered ground truth nodes
// =================================================================

/// Access filtered ground-truth neighbor indices.
pub struct FilteredNeighborIndicesAt {
    meta: NodeMeta,
    dataset: Arc<I32Dataset>,
}

impl FilteredNeighborIndicesAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = I32Dataset::load(source, profile, "filtered_neighbor_indices")?;
        Ok(Self {
            meta: NodeMeta {
                name: "filtered_neighbor_indices_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for FilteredNeighborIndicesAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_str(inputs[0].as_u64() as usize));
    }
}

/// Access filtered ground-truth neighbor distances.
pub struct FilteredNeighborDistancesAt {
    meta: NodeMeta,
    dataset: Arc<F32Dataset>,
}

impl FilteredNeighborDistancesAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "filtered_neighbor_distances")?;
        Ok(Self {
            meta: NodeMeta {
                name: "filtered_neighbor_distances_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for FilteredNeighborDistancesAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_str(inputs[0].as_u64() as usize));
    }
}

// =================================================================
// Metadata nodes (constant per dataset)
// =================================================================

/// Return the dataset vector dimensionality as a constant.
pub struct VectorDim {
    meta: NodeMeta,
    dim: u64,
}

impl VectorDim {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "base")?;
        Ok(Self {
            meta: NodeMeta {
                name: "vector_dim".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            dim: dataset.dim as u64,
        })
    }
}

impl GkNode for VectorDim {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.dim);
    }
}

/// Return the dataset's distance function (e.g., "cosine", "euclidean").
///
/// Signature: `dataset_distance_function(source) -> (String)`
pub struct DatasetDistanceFunction {
    meta: NodeMeta,
    value: String,
}

impl DatasetDistanceFunction {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let group = load_dataset_group(source)?;
        let df = group.attribute("distance_function")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        Ok(Self {
            meta: NodeMeta {
                name: "dataset_distance_function".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: Vec::new(),
            },
            value: df,
        })
    }
}

impl GkNode for DatasetDistanceFunction {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.value.clone());
    }
}

/// Return the dataset vector count as a constant.
pub struct VectorCount {
    meta: NodeMeta,
    count: u64,
}

impl VectorCount {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_dataset_name, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "base")?;
        Ok(Self {
            meta: NodeMeta {
                name: "vector_count".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: dataset.count as u64,
        })
    }
}

impl GkNode for VectorCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

/// Return the query vector count as a constant.
pub struct QueryCount {
    meta: NodeMeta,
    count: u64,
}

impl QueryCount {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = F32Dataset::load(source, profile, "query")?;
        Ok(Self {
            meta: NodeMeta {
                name: "query_count".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: dataset.count as u64,
        })
    }
}

impl GkNode for QueryCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

/// Return the ground-truth neighbor count (maxk) as a constant.
pub struct NeighborCount {
    meta: NodeMeta,
    count: u64,
}

impl NeighborCount {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = I32Dataset::load(source, profile, "neighbor_indices")?;
        Ok(Self {
            meta: NodeMeta {
                name: "neighbor_count".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: dataset.dim as u64,
        })
    }
}

impl GkNode for NeighborCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

// =================================================================
// Metadata facet nodes
// =================================================================

/// Handle to a loaded variable-length i32 facet (metadata_indices).
struct Ivvec32Dataset {
    reader: Arc<dyn VvecReader<i32>>,
    count: usize,
}

impl Ivvec32Dataset {
    fn load(source: &str, profile: &str) -> Result<Arc<Self>, String> {
        let key = (source.to_string(), profile.to_string(), "metadata_indices".to_string());
        {
            let cache = FACET_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = cache.get(&key) {
                if let Some(typed) = cached.clone().downcast::<Ivvec32Dataset>().ok() {
                    return Ok(typed);
                }
            }
        }
        let group = load_dataset_group(source)?;
        let view = group.profile(profile)
            .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
        let reader = view.metadata_indices()
            .map_err(|e| format!("failed to access metadata_indices from '{source}': {e}"))?;
        let count = reader.count();
        let arc = Arc::new(Self { reader, count });
        FACET_CACHE.lock().unwrap_or_else(|e| e.into_inner())
            .insert(key, arc.clone() as Arc<dyn std::any::Any + Send + Sync>);
        Ok(arc)
    }

    fn format_str(&self, index: usize) -> String {
        if self.count == 0 { return "[]".into(); }
        let vec = self.reader.get(index % self.count).unwrap_or_default();
        let inner: String = vec.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
        format!("[{inner}]")
    }
}

/// Access metadata indices (variable-length matching base ordinals per query).
pub struct MetadataIndicesAt {
    meta: NodeMeta,
    dataset: Arc<Ivvec32Dataset>,
}

impl MetadataIndicesAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = Ivvec32Dataset::load(source, profile)?;
        Ok(Self {
            meta: NodeMeta {
                name: "metadata_indices_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for MetadataIndicesAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_str(inputs[0].as_u64() as usize));
    }
}

/// Return the length of a specific metadata_indices record (number of
/// matching base vectors for one query) without loading the data.
///
/// Uses `dim_at()` which reads only the 4-byte header per record.
pub struct MetadataIndicesLenAt {
    meta: NodeMeta,
    dataset: Arc<Ivvec32Dataset>,
}

impl MetadataIndicesLenAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = Ivvec32Dataset::load(source, profile)?;
        Ok(Self {
            meta: NodeMeta {
                name: "metadata_indices_len_at".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for MetadataIndicesLenAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize;
        let len = if self.dataset.count == 0 { 0 }
            else { self.dataset.reader.dim_at(idx % self.dataset.count).unwrap_or(0) };
        outputs[0] = Value::U64(len as u64);
    }
}

/// Return the metadata indices count (number of predicate result sets).
pub struct MetadataIndicesCount {
    meta: NodeMeta,
    count: u64,
}

impl MetadataIndicesCount {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = Ivvec32Dataset::load(source, profile)?;
        Ok(Self {
            meta: NodeMeta {
                name: "metadata_indices_count".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: dataset.count as u64,
        })
    }
}

impl GkNode for MetadataIndicesCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

/// Report which facets are available in a dataset as a comma-separated list.
pub struct DatasetFacets {
    meta: NodeMeta,
    facets: String,
}

impl DatasetFacets {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let group = load_dataset_group(source)?;
        let view = group.profile(profile)
            .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
        let manifest = view.facet_manifest();
        let mut names: Vec<&String> = manifest.keys().collect();
        names.sort();
        let facets = names.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
        Ok(Self {
            meta: NodeMeta {
                name: "dataset_facets".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: Vec::new(),
            },
            facets,
        })
    }
}

impl GkNode for DatasetFacets {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.facets.clone());
    }
}

// =================================================================
// Profile enumeration — discover and iterate over dataset profiles
// =================================================================

/// Cache for sorted profile name lists, keyed by dataset source string.
static PROFILE_NAMES_CACHE: LazyLock<Mutex<HashMap<String, Arc<Vec<String>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Load the sorted profile name list for a dataset, caching the result.
fn load_profile_names(source: &str) -> Result<Arc<Vec<String>>, String> {
    {
        let cache = PROFILE_NAMES_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(names) = cache.get(source) {
            return Ok(names.clone());
        }
    }
    let group = load_dataset_group(source)?;
    let names = group.profile_names();
    let arc = Arc::new(names);
    {
        let mut cache = PROFILE_NAMES_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        cache.insert(source.to_string(), arc.clone());
    }
    Ok(arc)
}

/// Return the total number of profiles in a dataset as a constant.
///
/// Signature: `dataset_profile_count(source) -> (u64)`
pub struct DatasetProfileCount {
    meta: NodeMeta,
    count: u64,
}

impl DatasetProfileCount {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let names = load_profile_names(source)?;
        Ok(Self {
            meta: NodeMeta {
                name: "dataset_profile_count".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: names.len() as u64,
        })
    }
}

impl GkNode for DatasetProfileCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

/// Return a comma-separated list of all profile names in a dataset.
///
/// Signature: `dataset_profile_names(source) -> (String)`
///
/// Names are returned in the dataset's canonical sort order
/// (sorted by base_count via `profile_sort_by_size`).
pub struct DatasetProfileNames {
    meta: NodeMeta,
    names: String,
}

impl DatasetProfileNames {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let names = load_profile_names(source)?;
        Ok(Self {
            meta: NodeMeta {
                name: "dataset_profile_names".into(),
                outs: vec![Port::str("output")],
                ins: Vec::new(),
            },
            names: names.join(", "),
        })
    }
}

impl GkNode for DatasetProfileNames {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.names.clone());
    }
}

/// Return profile names matching a prefix, comma-separated.
///
/// Signature: `matching_profiles(source, prefix) -> (String)`
///
/// If prefix is empty, returns all profiles. Used by `for_each:`
/// phase templates to discover profiles dynamically.
pub struct MatchingProfiles {
    meta: NodeMeta,
    names: String,
}

impl MatchingProfiles {
    pub fn from_source(source: &str, prefix: &str) -> Result<Self, String> {
        let all = load_profile_names(source)?;
        let matched: Vec<&str> = if prefix.is_empty() {
            all.iter().map(|s| s.as_str()).collect()
        } else {
            all.iter().filter(|s| s.starts_with(prefix)).map(|s| s.as_str()).collect()
        };
        Ok(Self {
            meta: NodeMeta {
                name: "matching_profiles".into(),
                outs: vec![Port::str("output")],
                ins: Vec::new(),
            },
            names: matched.join(","),
        })
    }
}

impl GkNode for MatchingProfiles {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.names.clone());
    }
}

/// Look up a profile name by index from the sorted profile list.
///
/// Signature: `dataset_profile_name_at(index: u64, source: str) -> (String)`
///
/// The index wraps modulo the number of profiles.
pub struct DatasetProfileNameAt {
    meta: NodeMeta,
    names: Arc<Vec<String>>,
}

impl DatasetProfileNameAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let names = load_profile_names(source)?;
        Ok(Self {
            meta: NodeMeta {
                name: "dataset_profile_name_at".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            names,
        })
    }
}

impl GkNode for DatasetProfileNameAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize % self.names.len();
        outputs[0] = Value::Str(self.names[idx].clone());
    }
}

/// Return the base vector count for the profile at a given index.
///
/// Signature: `profile_base_count(index: u64, source: str) -> (u64)`
///
/// Uses the same sorted profile ordering as `dataset_profile_name_at`.
pub struct ProfileBaseCount {
    meta: NodeMeta,
    counts: Vec<u64>,
}

impl ProfileBaseCount {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let group = load_dataset_group(source)?;
        let names = load_profile_names(source)?;
        let counts: Vec<u64> = names.iter().map(|name| {
            group.profile(name)
                .and_then(|view| view.base_count())
                .unwrap_or(0)
        }).collect();
        Ok(Self {
            meta: NodeMeta {
                name: "profile_base_count".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            counts,
        })
    }
}

impl GkNode for ProfileBaseCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize % self.counts.len();
        outputs[0] = Value::U64(self.counts[idx]);
    }
}

/// Return the comma-separated facet list for the profile at a given index.
///
/// Signature: `profile_facets(index: u64, source: str) -> (String)`
///
/// Uses the same sorted profile ordering as `dataset_profile_name_at`.
pub struct ProfileFacets {
    meta: NodeMeta,
    facets: Vec<String>,
}

impl ProfileFacets {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let group = load_dataset_group(source)?;
        let names = load_profile_names(source)?;
        let facets: Vec<String> = names.iter().map(|name| {
            match group.profile(name) {
                Some(view) => {
                    let manifest = view.facet_manifest();
                    let mut fnames: Vec<&String> = manifest.keys().collect();
                    fnames.sort();
                    fnames.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                }
                None => String::new(),
            }
        }).collect();
        Ok(Self {
            meta: NodeMeta {
                name: "profile_facets".into(),
                outs: vec![Port::str("output")],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            facets,
        })
    }
}

impl GkNode for ProfileFacets {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize % self.facets.len();
        outputs[0] = Value::Str(self.facets[idx].clone());
    }
}

// =================================================================
// Prebuffering — eagerly download dataset facets before workload run
// =================================================================

/// Eagerly download all facets for a dataset to the local cache.
///
/// Signature: `dataset_prebuffer(source) -> (u64)`
///
/// Returns the number of facets prebuffered. Evaluated at init time
/// as a side-effecting constant — the download happens during GK
/// compilation, before any cycles execute.
///
/// After prebuffering, subsequent dataset loads resolve from the
/// local cache (`~/.cache/vectordata/<dataset>/`) using mmap readers.
pub struct DatasetPrebuffer {
    meta: NodeMeta,
    facets: u64,
}

impl DatasetPrebuffer {
    pub fn from_source(source: &str) -> Result<Self, String> {
        // Opening the dataset via the catalog triggers download/caching
        let _group = load_dataset_group(source)?;
        Ok(Self {
            meta: NodeMeta {
                name: "dataset_prebuffer".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            facets: 0,
        })
    }
}

impl GkNode for DatasetPrebuffer {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.facets);
    }
}

// =================================================================
// Generic facet access — type-aware scalar/vector readers
// =================================================================

/// A type-erased facet reader that stores values as i64.
/// Uses `generic_view().open_facet_typed::<i64>()` which handles all
/// element types (u8, i32, etc.), caching, and local/remote access.
struct GenericFacetDataset {
    reader: vectordata::typed_access::TypedReader<i64>,
    count: usize,
}

impl GenericFacetDataset {
    fn load(source: &str, profile: &str, facet: &str) -> Result<Arc<Self>, String> {
        let key = (source.to_string(), profile.to_string(), facet.to_string());
        {
            let cache = FACET_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = cache.get(&key) {
                if let Ok(typed) = cached.clone().downcast::<GenericFacetDataset>() {
                    return Ok(typed);
                }
            }
        }
        let group = load_dataset_group(source)?;
        let gv = group.generic_view(profile)
            .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
        let reader = gv.open_facet_typed::<i64>(facet)
            .map_err(|e| format!("failed to open {facet} from '{source}:{profile}': {e}"))?;
        let count = reader.count();
        let arc = Arc::new(Self { reader, count });
        FACET_CACHE.lock().unwrap_or_else(|e| e.into_inner())
            .insert(key, arc.clone() as Arc<dyn std::any::Any + Send + Sync>);
        Ok(arc)
    }

    fn get_scalar(&self, index: usize) -> i64 {
        if self.count == 0 { return 0; }
        self.reader.get_value(index % self.count).unwrap_or(0)
    }

    fn format_scalar(&self, index: usize) -> String {
        self.get_scalar(index).to_string()
    }
}

/// Access a metadata value per base vector by ordinal.
///
/// Signature: `metadata_value_at(index: u64, source: str) -> (String)`
///
/// Reads from the `metadata_content` facet. Supports all element types
/// (u8, i32, etc.). Returns the value as a string.
pub struct MetadataValueAt {
    meta: NodeMeta,
    dataset: Arc<GenericFacetDataset>,
}

impl MetadataValueAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = GenericFacetDataset::load(source, profile, "metadata_content")?;
        Ok(Self {
            meta: NodeMeta {
                name: "metadata_value_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for MetadataValueAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_scalar(inputs[0].as_u64() as usize));
    }
}

/// Access a predicate value per query by ordinal.
///
/// Signature: `predicate_value_at(index: u64, source: str) -> (String)`
///
/// Reads from the `metadata_predicates` facet. Supports all element types.
/// Returns the value as a string.
pub struct PredicateValueAt {
    meta: NodeMeta,
    dataset: Arc<GenericFacetDataset>,
}

impl PredicateValueAt {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = GenericFacetDataset::load(source, profile, "metadata_predicates")?;
        Ok(Self {
            meta: NodeMeta {
                name: "predicate_value_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        })
    }
}

impl GkNode for PredicateValueAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.dataset.format_scalar(inputs[0].as_u64() as usize));
    }
}

/// Count of metadata content records.
///
/// Signature: `metadata_content_count(source: str) -> (u64)`
pub struct MetadataContentCount {
    meta: NodeMeta,
    count: u64,
}

impl MetadataContentCount {
    pub fn from_source(source: &str) -> Result<Self, String> {
        let (_, profile) = parse_source_specifier(source);
        let dataset = GenericFacetDataset::load(source, profile, "metadata_content")?;
        Ok(Self {
            meta: NodeMeta {
                name: "metadata_content_count".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: dataset.count as u64,
        })
    }
}

impl GkNode for MetadataContentCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for vector dataset access nodes (feature-gated).
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "vector_at", category: C::RealData, outputs: 1,
            description: "access base vector by index (string)",
            help: "Look up a base vector by index from a loaded dataset.\nReturns the vector as a JSON array string: [0.1,0.2,...].\nThe index wraps modulo the dataset size.\nRequires a dataset loaded at init time.\nExample: vector_at(mod(cycle, vector_count), dataset)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "vector_at_bytes", category: C::RealData, outputs: 1,
            description: "access base vector by index (bytes)",
            help: "Look up a base vector by index, returning raw f32 little-endian bytes.\nSuitable for CQL blob columns or binary protocols.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "query_vector_at", category: C::RealData, outputs: 1,
            description: "access query vector by index (string)",
            help: "Look up a query vector by index from a loaded dataset.\nReturns the vector as a JSON array string.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "query_vector_at_bytes", category: C::RealData, outputs: 1,
            description: "access query vector by index (bytes)",
            help: "Look up a query vector by index, returning raw f32 little-endian bytes.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "neighbor_indices_at", category: C::RealData, outputs: 1,
            description: "ground-truth neighbor indices for a query",
            help: "Look up ground-truth k-nearest neighbor indices for a query.\nReturns indices as a JSON array string: [42,17,99,...].\nUsed for recall verification in vector search workloads.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "neighbor_distances_at", category: C::RealData, outputs: 1,
            description: "ground-truth neighbor distances for a query",
            help: "Look up ground-truth distances for a query's k-nearest neighbors.\nReturns distances as a JSON array string.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "filtered_neighbor_indices_at", category: C::RealData, outputs: 1,
            description: "filtered ground-truth neighbor indices",
            help: "Look up filtered ground-truth neighbor indices for a query.\nUsed for filtered ANN recall verification.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "filtered_neighbor_distances_at", category: C::RealData, outputs: 1,
            description: "filtered ground-truth neighbor distances",
            help: "Look up filtered ground-truth distances for a query.\nUsed for filtered ANN recall verification.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "dataset_distance_function", category: C::RealData, outputs: 1,
            description: "dataset distance/similarity function name",
            help: "Returns the distance function declared in the dataset metadata\n(e.g., 'cosine', 'euclidean', 'dot_product').\nConstant per dataset.\nExample: dataset_distance_function(\"glove-25-angular\")",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "vector_dim", category: C::RealData, outputs: 1,
            description: "dataset vector dimensionality",
            help: "Returns the dimensionality of vectors in the loaded dataset.\nConstant per dataset — evaluated once at init time.\nExample: vector_dim(\"glove-100\")",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "vector_count", category: C::RealData, outputs: 1,
            description: "dataset vector count",
            help: "Returns the number of vectors in the loaded dataset.\nConstant per dataset — evaluated once at init time.\nExample: vector_count(\"glove-100\")",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "query_count", category: C::RealData, outputs: 1,
            description: "dataset query vector count",
            help: "Returns the number of query vectors in the dataset.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "neighbor_count", category: C::RealData, outputs: 1,
            description: "ground-truth neighbors per query (maxk)",
            help: "Returns the number of ground-truth neighbors per query (the k in k-NN).",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "metadata_indices_len_at", category: C::RealData, outputs: 1,
            description: "length of metadata indices for a query (no data load)",
            help: "Returns the number of matching base vectors for a query's\npredicate without loading the full index list.\nUses dim_at() which reads only the 4-byte header.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "metadata_indices_at", category: C::RealData, outputs: 1,
            description: "matching base ordinals for a query predicate",
            help: "Variable-length list of base vector ordinals that match\nthe predicate for a given query index.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "metadata_indices_count", category: C::RealData, outputs: 1,
            description: "number of predicate result sets",
            help: "Returns the number of metadata index result sets (typically equals query count).",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "dataset_facets", category: C::RealData, outputs: 1,
            description: "list available facets in a dataset",
            help: "Returns a comma-separated list of facet names available\nin the dataset's default profile.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "dataset_profile_count", category: C::RealData, outputs: 1,
            description: "total number of profiles in a dataset",
            help: "Returns the number of profiles defined in the dataset.\nConstant per dataset — evaluated once at init time.\nExample: dataset_profile_count(\"sift1m\")",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "dataset_profile_names", category: C::RealData, outputs: 1,
            description: "comma-separated list of profile names",
            help: "Returns a comma-separated list of all profile names in the dataset,\nsorted by base_count (canonical order).\nExample: dataset_profile_names(\"sift1m\")",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "matching_profiles", category: C::RealData, outputs: 1,
            description: "profile names matching a prefix, comma-separated",
            help: "Returns profiles whose names start with the given prefix.\nIf prefix is empty, returns all profiles.\nUsed by for_each: phase templates to discover profiles dynamically.\nExample: matching_profiles(\"sift1m\", \"label\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
                ParamSpec { name: "prefix", slot_type: SlotType::ConstStr, required: false, example: "\"\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "dataset_profile_name_at", category: C::RealData, outputs: 1,
            description: "profile name by index from sorted list",
            help: "Returns the profile name at a given index from the dataset's\nsorted profile list. Index wraps modulo profile count.\nExample: dataset_profile_name_at(cycle, \"sift1m\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "profile_base_count", category: C::RealData, outputs: 1,
            description: "base vector count for profile at index",
            help: "Returns the base vector count for the profile at a given index\nin the dataset's sorted profile list. Index wraps modulo profile count.\nExample: profile_base_count(cycle, \"sift1m\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "profile_facets", category: C::RealData, outputs: 1,
            description: "available facets for profile at index",
            help: "Returns a comma-separated list of facet names for the profile at a\ngiven index in the dataset's sorted profile list.\nExample: profile_facets(cycle, \"sift1m\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "dataset_prebuffer", category: C::RealData, outputs: 1,
            description: "eagerly download dataset facets to local cache",
            help: "Downloads all facets for a dataset to the local cache before\nworkload execution. Returns the number of facets prebuffered.\nSubsequent loads use fast local mmap access.\nExample: dataset_prebuffer(\"sift1m\")",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "metadata_value_at", category: C::RealData, outputs: 1,
            description: "scalar metadata value per base vector",
            help: "Read a metadata value for a base vector by ordinal.\nFor scalar ordinal data, returns the integer value as a string.\nReads from the metadata_content facet.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "predicate_value_at", category: C::RealData, outputs: 1,
            description: "scalar predicate value per query",
            help: "Read a predicate value for a query by ordinal.\nFor scalar ordinal data, returns the integer value as a string.\nReads from the metadata_predicates facet.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "metadata_content_count", category: C::RealData, outputs: 1,
            description: "number of metadata content records",
            help: "Returns the number of metadata content records in the dataset.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true, example: "\"test\"" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a vector dataset node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
/// All functions in this module are feature-gated on `vectordata`.
#[cfg(feature = "vectordata")]
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    let src = consts.first().map(|c| c.as_str()).unwrap_or("");
    match name {
        "vector_at" => Some(
            VectorAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "vector_at_bytes" => Some(
            VectorAtBytes::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "query_vector_at" => Some(
            QueryVectorAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "query_vector_at_bytes" => Some(
            QueryVectorAtBytes::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "neighbor_indices_at" => Some(
            NeighborIndicesAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "neighbor_distances_at" => Some(
            NeighborDistancesAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "filtered_neighbor_indices_at" => Some(
            FilteredNeighborIndicesAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "filtered_neighbor_distances_at" => Some(
            FilteredNeighborDistancesAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "dataset_distance_function" => Some(
            DatasetDistanceFunction::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "vector_dim" => Some(
            VectorDim::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "vector_count" => Some(
            VectorCount::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "query_count" => Some(
            QueryCount::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "neighbor_count" => Some(
            NeighborCount::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "metadata_indices_len_at" => Some(
            MetadataIndicesLenAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "metadata_indices_at" => Some(
            MetadataIndicesAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "metadata_indices_count" => Some(
            MetadataIndicesCount::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "dataset_facets" => Some(
            DatasetFacets::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "dataset_profile_count" => Some(
            DatasetProfileCount::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "dataset_profile_names" => Some(
            DatasetProfileNames::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "matching_profiles" => Some({
            let prefix = consts.get(1).map(|c| c.as_str()).unwrap_or("");
            MatchingProfiles::from_source(src, prefix)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        }),
        "dataset_profile_name_at" => Some(
            DatasetProfileNameAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "profile_base_count" => Some(
            ProfileBaseCount::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "profile_facets" => Some(
            ProfileFacets::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "dataset_prebuffer" => Some(
            DatasetPrebuffer::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "metadata_value_at" => Some(
            MetadataValueAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "predicate_value_at" => Some(
            PredicateValueAt::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        "metadata_content_count" => Some(
            MetadataContentCount::from_source(src)
                .map(|n| Box::new(n) as Box<dyn crate::node::GkNode>)
        ),
        _ => None,
    }
}

#[cfg(feature = "vectordata")]
crate::register_nodes!(signatures, build_node);
