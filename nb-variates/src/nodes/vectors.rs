// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector dataset access nodes via the `vectordata` crate.
//!
//! These nodes load vector datasets (fvec, ivec, etc.) at init time
//! via `vectordata::TestDataGroup` and provide cycle-time indexed
//! access to individual vectors.
//!
//! Feature-gated behind `vectordata`.
//!
//! # Init-time
//! ```gk
//! init dataset = load_vectors("https://example.com/datasets/glove-100/")
//! ```
//!
//! # Cycle-time
//! ```gk
//! vector := vector_at(hash(cycle), dataset)
//! ```

use std::sync::Arc;

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use vectordata::{TestDataGroup, VectorReader};

// =================================================================
// Init-time: dataset loading produces Arc<dyn VectorReader>
// =================================================================

/// A loaded vector dataset, held as an init-time artifact.
///
/// Wraps `Arc<dyn VectorReader<f32>>` — the underlying data may be
/// memory-mapped (local) or HTTP-fetched (remote).
pub struct VectorDataset {
    reader: Arc<dyn VectorReader<f32>>,
    count: usize,
    dim: usize,
}

impl VectorDataset {
    /// Load base vectors from a dataset URL or local path.
    pub fn load_base(source: &str) -> Result<Self, String> {
        Self::load_facet(source, "default", |view| view.base_vectors())
    }

    /// Load query vectors from a dataset.
    pub fn load_query(source: &str) -> Result<Self, String> {
        Self::load_facet(source, "default", |view| view.query_vectors())
    }

    /// Load base vectors from a specific profile.
    pub fn load_base_profile(source: &str, profile: &str) -> Result<Self, String> {
        Self::load_facet(source, profile, |view| view.base_vectors())
    }

    fn load_facet<F>(source: &str, profile: &str, accessor: F) -> Result<Self, String>
    where
        F: FnOnce(&dyn vectordata::TestDataView) -> vectordata::Result<Arc<dyn VectorReader<f32>>>,
    {
        let group = TestDataGroup::load(source)
            .map_err(|e| format!("failed to load dataset from {source}: {e}"))?;
        let view = group.profile(profile)
            .ok_or_else(|| format!("profile '{profile}' not found in dataset"))?;
        let reader = accessor(view.as_ref())
            .map_err(|e| format!("failed to access vectors: {e}"))?;
        let count = reader.count();
        let dim = reader.dim();
        Ok(Self { reader, count, dim })
    }

    /// Number of vectors in the dataset.
    pub fn count(&self) -> usize { self.count }

    /// Dimensionality of each vector.
    pub fn dim(&self) -> usize { self.dim }

    /// Get vector at index, wrapping if index >= count.
    pub fn get(&self, index: usize) -> Vec<f32> {
        if self.count == 0 {
            return vec![];
        }
        let idx = index % self.count;
        self.reader.get(idx).unwrap_or_default()
    }
}

// =================================================================
// Cycle-time: indexed vector access as a GK node
// =================================================================

/// Access a vector from a loaded dataset by index.
///
/// Signature: `(index: u64) -> (String)`
///
/// The output is the vector formatted as a comma-separated string of
/// floats (suitable for CQL vector columns, JSON arrays, etc.).
/// The index wraps modulo the dataset size.
///
/// The `VectorDataset` is an init-time artifact passed at construction.
pub struct VectorAt {
    meta: NodeMeta,
    dataset: Arc<VectorDataset>,
}

impl VectorAt {
    pub fn new(dataset: Arc<VectorDataset>) -> Self {
        Self {
            meta: NodeMeta {
                name: "vector_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        }
    }
}

impl GkNode for VectorAt {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize;
        let vec = self.dataset.get(idx);
        let formatted: String = vec.iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");
        outputs[0] = Value::Str(format!("[{formatted}]"));
    }
}

/// Access a vector from a loaded dataset, returning raw bytes.
///
/// Signature: `(index: u64) -> (bytes)`
///
/// Each f32 element is encoded as 4 little-endian bytes.
pub struct VectorAtBytes {
    meta: NodeMeta,
    dataset: Arc<VectorDataset>,
}

impl VectorAtBytes {
    pub fn new(dataset: Arc<VectorDataset>) -> Self {
        Self {
            meta: NodeMeta {
                name: "vector_at_bytes".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::u64("index"))],
            },
            dataset,
        }
    }
}

impl GkNode for VectorAtBytes {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize;
        let vec = self.dataset.get(idx);
        let bytes: Vec<u8> = vec.iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        outputs[0] = Value::Bytes(bytes);
    }
}

/// Return the dataset dimensionality as a constant.
///
/// Signature: `() -> (u64)`
///
/// Useful for workloads that need to know the vector size.
pub struct VectorDim {
    meta: NodeMeta,
    dim: u64,
}

impl VectorDim {
    pub fn new(dataset: &VectorDataset) -> Self {
        Self {
            meta: NodeMeta {
                name: "vector_dim".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            dim: dataset.dim() as u64,
        }
    }
}

impl GkNode for VectorDim {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.dim);
    }
}

/// Return the dataset count as a constant.
///
/// Signature: `() -> (u64)`
pub struct VectorCount {
    meta: NodeMeta,
    count: u64,
}

impl VectorCount {
    pub fn new(dataset: &VectorDataset) -> Self {
        Self {
            meta: NodeMeta {
                name: "vector_count".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: dataset.count() as u64,
        }
    }
}

impl GkNode for VectorCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

// No tests here — vectordata requires network/file access that isn't
// suitable for unit tests. Integration tests should use a local
// dataset fixture.
