// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector dataset access nodes via the `vectordata` crate.
//!
//! Each node takes a dataset source string (URL or local path) as a
//! const parameter and loads the dataset handle at construction time.
//! The `vectordata` crate manages I/O, caching, and lazy access
//! internally — the node just holds a `Send + Sync` reader handle.
//!
//! At cycle time, vectors are accessed by index with wrapping.
//!
//! Feature-gated behind `vectordata`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, LazyLock};

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use vectordata::{TestDataGroup, VectorReader};

/// Global cache for loaded dataset groups keyed by source string.
/// Ensures each dataset is loaded exactly once regardless of how many
/// node functions reference it. Thread-safe via Mutex.
static DATASET_CACHE: LazyLock<Mutex<HashMap<String, Arc<TestDataGroup>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// =================================================================
// Dataset resolution
// =================================================================

/// Load a dataset group from a source string.
///
/// Resolution order:
/// 1. Direct URL (starts with `http://` or `https://`)
/// 2. Local path (contains `/` or exists as a directory)
/// 3. Catalog lookup by name via `~/.config/vectordata/catalogs.yaml`
fn load_dataset_group(source: &str) -> Result<Arc<TestDataGroup>, String> {
    // Check cache first
    {
        let cache = DATASET_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(group) = cache.get(source) {
            return Ok(group.clone());
        }
    }

    // Load the dataset
    let group = load_dataset_group_uncached(source)?;
    let arc = Arc::new(group);

    // Cache it
    {
        let mut cache = DATASET_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        cache.insert(source.to_string(), arc.clone());
    }

    Ok(arc)
}

fn load_dataset_group_uncached(source: &str) -> Result<TestDataGroup, String> {
    // Direct URL
    if source.starts_with("http://") || source.starts_with("https://") {
        return TestDataGroup::load(source)
            .map_err(|e| format!("failed to load dataset '{source}': {e}"));
    }

    // Local path
    if source.contains('/') || std::path::Path::new(source).exists() {
        return TestDataGroup::load(source)
            .map_err(|e| format!("failed to load dataset '{source}': {e}"));
    }

    // Catalog resolution by name
    let sources = vectordata::catalog::CatalogSources::new().configure_default();
    let catalog = vectordata::catalog::Catalog::of(&sources);

    if let Some(entry) = catalog.find_exact(source) {
        eprintln!("vectordata: resolved '{source}' → {}", entry.path);
        return TestDataGroup::load(&entry.path)
            .map_err(|e| format!("failed to load dataset '{source}' from catalog: {e}"));
    }

    // Not found — show available datasets
    catalog.list_datasets(source);
    Err(format!("dataset '{source}' not found in catalog or as a local path"))
}

// =================================================================
// Dataset handles — loaded once at node construction, shared via Arc
// =================================================================

/// Handle to a loaded f32 vector facet (base vectors, query vectors,
/// neighbor distances). Thread-safe, random-access.
struct F32Dataset {
    reader: Arc<dyn VectorReader<f32>>,
    count: usize,
    dim: usize,
}

impl F32Dataset {
    fn load(source: &str, profile: &str, facet: &str) -> Result<Arc<Self>, String> {
        let group = load_dataset_group(source)?;
        let view = group.profile(profile)
            .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
        let reader: Arc<dyn VectorReader<f32>> = match facet {
            "base" => view.base_vectors(),
            "query" => view.query_vectors(),
            "neighbor_distances" => view.neighbor_distances(),
            "filtered_neighbor_distances" => view.filtered_neighbor_distances(),
            other => return Err(format!("unknown f32 facet: '{other}'")),
        }.map_err(|e| format!("failed to access {facet} from '{source}': {e}"))?;
        let count = reader.count();
        let dim = reader.dim();
        Ok(Arc::new(Self { reader, count, dim }))
    }

    fn get(&self, index: usize) -> Vec<f32> {
        if self.count == 0 { return vec![]; }
        self.reader.get(index % self.count).unwrap_or_default()
    }

    fn format_str(&self, index: usize) -> String {
        let vec = self.get(index);
        let inner: String = vec.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
        format!("[{inner}]")
    }

    fn to_bytes(&self, index: usize) -> Vec<u8> {
        self.get(index).iter().flat_map(|f| f.to_le_bytes()).collect()
    }
}

/// Handle to a loaded i32 vector facet (neighbor indices).
struct I32Dataset {
    reader: Arc<dyn VectorReader<i32>>,
    count: usize,
    #[allow(dead_code)]
    dim: usize,
}

impl I32Dataset {
    fn load(source: &str, profile: &str, facet: &str) -> Result<Arc<Self>, String> {
        let group = load_dataset_group(source)?;
        let view = group.profile(profile)
            .ok_or_else(|| format!("profile '{profile}' not found in '{source}'"))?;
        let reader: Arc<dyn VectorReader<i32>> = match facet {
            "neighbor_indices" => view.neighbor_indices(),
            "filtered_neighbor_indices" => view.filtered_neighbor_indices(),
            other => return Err(format!("unknown i32 facet: '{other}'")),
        }.map_err(|e| format!("failed to access {facet} from '{source}': {e}"))?;
        let count = reader.count();
        let dim = reader.dim();
        Ok(Arc::new(Self { reader, count, dim }))
    }

    fn format_str(&self, index: usize) -> String {
        if self.count == 0 { return "[]".into(); }
        let vec = self.reader.get(index % self.count).unwrap_or_default();
        let inner: String = vec.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
        format!("[{inner}]")
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
        let dataset = F32Dataset::load(source, "default", "base")?;
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
        let dataset = F32Dataset::load(source, "default", "base")?;
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
        let dataset = F32Dataset::load(source, "default", "query")?;
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
        let dataset = F32Dataset::load(source, "default", "query")?;
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
        let dataset = I32Dataset::load(source, "default", "neighbor_indices")?;
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
        let dataset = F32Dataset::load(source, "default", "neighbor_distances")?;
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
        let dataset = I32Dataset::load(source, "default", "filtered_neighbor_indices")?;
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
        let dataset = F32Dataset::load(source, "default", "filtered_neighbor_distances")?;
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
        let dataset = F32Dataset::load(source, "default", "base")?;
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
        let view = group.profile("default")
            .ok_or_else(|| format!("profile 'default' not found in '{source}'"))?;
        let df = view.distance_function()
            .unwrap_or_else(|| "unknown".to_string());
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
        let dataset = F32Dataset::load(source, "default", "base")?;
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
