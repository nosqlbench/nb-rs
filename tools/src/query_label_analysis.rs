// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Query label distribution and ground truth analysis for filtered-kNN.
//!
//! Loads the dbdir dataset from a local path and analyzes:
//! 1. Query predicate distribution across labels
//! 2. Whether label profile query vectors match the base query vectors
//! 3. Ordinal mapping from partition-local to global space
//! 4. Ground truth overlap between oracle partitions and filtered-kNN
//!
//! Usage:
//!   query-label-analysis /path/to/dbdir

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dataset_dir = args.first()
        .map(|s| s.as_str())
        .unwrap_or("/mnt/datamir/testdata/vectordata-testdata/a442022ac863b79dbcaf350bd8b71de69742da8c/dbdir");

    let base = Path::new(dataset_dir);
    eprintln!("Dataset: {}", base.display());

    // ── 1. Read predicates (query→label mapping) ─────────────────

    let predicates = read_u8_facet(&base.join("profiles/base/predicates.u8"));
    let num_queries = predicates.len();
    eprintln!("Predicates (query labels): {} entries", num_queries);

    let mut label_query_indices: HashMap<u8, Vec<usize>> = HashMap::new();
    for (qi, &label) in predicates.iter().enumerate() {
        label_query_indices.entry(label).or_default().push(qi);
    }

    let mut sorted_labels: Vec<u8> = label_query_indices.keys().copied().collect();
    sorted_labels.sort();

    println!("\n## Query Distribution by Predicate Label\n");
    println!("| Label | Queries | Fraction |");
    println!("|-------|---------|----------|");
    for &label in &sorted_labels {
        let count = label_query_indices[&label].len();
        let frac = count as f64 / num_queries as f64;
        println!("| {label:>5} | {count:>7} | {frac:>8.4} |");
    }
    println!("| Total | {num_queries:>7} | {:.4} |", 1.0);

    // ── 2. Read metadata_content (base vector→label) ─────────────

    let metadata = read_u8_facet(&base.join("profiles/base/metadata_content.u8"));
    let num_base = metadata.len();
    eprintln!("Metadata content (base labels): {} entries", num_base);

    // Build global ordinal → label, and label → sorted global ordinals
    let mut label_globals: HashMap<u8, Vec<usize>> = HashMap::new();
    for (global_ord, &label) in metadata.iter().enumerate() {
        label_globals.entry(label).or_default().push(global_ord);
    }

    println!("\n## Ordinal Mapping (metadata_content → partition)\n");
    println!("| Label | Global Vectors | Profile base_count | Match? |");
    println!("|-------|---------------|-------------------|--------|");
    for &label in &sorted_labels {
        let globals = label_globals.get(&label).map(|v| v.len()).unwrap_or(0);
        let profile_base = read_ivec_count(
            &base.join(format!("profiles/label_{:02}/base_vectors.fvec", label))
        );
        let matches = if globals == profile_base { "yes" } else { "NO" };
        println!("| {label:>5} | {globals:>13} | {profile_base:>17} | {matches:>6} |");
    }

    // ── 3. Check if label profiles share the same query vectors ──

    println!("\n## Query Vector Identity Check\n");
    let base_queries = read_fvec_raw(&base.join("profiles/base/query_vectors.fvec"));
    let base_query_count = fvec_count(&base_queries);
    let base_query_dim = fvec_dim(&base_queries);
    println!("Base query vectors: {} × dim {}", base_query_count, base_query_dim);

    for &label in &sorted_labels {
        let profile_queries = read_fvec_raw(
            &base.join(format!("profiles/label_{:02}/query_vectors.fvec", label))
        );
        let pq_count = fvec_count(&profile_queries);
        let identical = base_queries == profile_queries;
        let status = if identical { "identical" } else { "DIFFERENT" };
        println!("label_{:02}: {} queries, {} to base", label, pq_count, status);
    }

    // ── 4. Ground truth comparison ───────────────────────────────
    //
    // For each query, compare:
    //   default/filtered_neighbor_indices[qi] (global ordinals)
    //   label_XX/neighbor_indices[qi] (local ordinals, translated to global)

    let default_fni = read_ivec(&base.join("profiles/default/filtered_neighbor_indices.ivec"));
    let fni_k = if default_fni.is_empty() { 0 } else { default_fni[0].len() };
    println!("\n## Ground Truth Comparison (max k={})\n", fni_k);

    // Pre-load all label profile neighbor indices
    let mut profile_neighbors: HashMap<u8, Vec<Vec<i32>>> = HashMap::new();
    for &label in &sorted_labels {
        let ni = read_ivec(
            &base.join(format!("profiles/label_{:02}/neighbor_indices.ivec", label))
        );
        profile_neighbors.insert(label, ni);
    }

    for k in [10, 100] {
        println!("### k={k}\n");
        println!("| Label | Queries | Overlap | Exact |");
        println!("|-------|---------|---------|-------|");

        let mut total_queries = 0usize;
        let mut total_overlap = 0.0f64;
        let mut total_exact = 0usize;

        for &label in &sorted_labels {
            let globals = match label_globals.get(&label) {
                Some(g) => g,
                None => continue,
            };
            let pni = match profile_neighbors.get(&label) {
                Some(n) => n,
                None => continue,
            };
            let query_indices = &label_query_indices[&label];

            let mut label_overlap = 0.0f64;
            let mut label_exact = 0usize;
            let mut label_count = 0usize;

            // The label profile has 10K queries (same vectors as base).
            // For query qi, the oracle neighbors are pni[qi] in local ordinals.
            // The filtered ground truth is default_fni[qi] in global ordinals.
            // Only queries targeting this label have meaningful filtered GT.
            for &qi in query_indices {
                if qi >= default_fni.len() || qi >= pni.len() { continue; }

                let filtered_global = &default_fni[qi];
                let oracle_local = &pni[qi];

                // Translate oracle local → global
                let ki = k.min(filtered_global.len()).min(oracle_local.len());
                if ki == 0 { continue; }

                let oracle_global: Vec<i32> = oracle_local[..ki].iter()
                    .map(|&local| {
                        let l = local as usize;
                        if l < globals.len() { globals[l] as i32 } else { -1 }
                    })
                    .collect();

                let fg: HashSet<i32> = filtered_global[..ki].iter().copied().collect();
                let og: HashSet<i32> = oracle_global.iter().copied().collect();
                let overlap = fg.intersection(&og).count() as f64 / ki as f64;

                label_overlap += overlap;
                if filtered_global[..ki] == oracle_global[..] { label_exact += 1; }
                label_count += 1;
            }

            let avg_overlap = if label_count > 0 {
                label_overlap / label_count as f64
            } else { 0.0 };
            let exact_pct = if label_count > 0 {
                label_exact as f64 / label_count as f64 * 100.0
            } else { 0.0 };

            println!("| {label:>5} | {label_count:>7} | {avg_overlap:>7.4} | {exact_pct:>4.1}% |");
            total_queries += label_count;
            total_overlap += label_overlap;
            total_exact += label_exact;
        }

        if total_queries > 0 {
            let avg = total_overlap / total_queries as f64;
            let exact_pct = total_exact as f64 / total_queries as f64 * 100.0;
            println!("| **All** | {total_queries:>7} | {avg:>7.4} | {exact_pct:>4.1}% |");
        }
        println!();
    }

    println!("Overlap = fraction of filtered GT (global) found in oracle GT (translated).");
    println!("1.000 = identical ground truth. <1.0 = the two GTs disagree.");
}

// ── Facet readers (direct file access, no vectordata crate) ──────

/// Read a .u8 scalar facet file. Format: raw bytes, one u8 per entry.
fn read_u8_facet(path: &Path) -> Vec<u8> {
    std::fs::read(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// Read an ivec file into Vec<Vec<i32>>. Format: [dim:i32, data:i32×dim] repeated.
fn read_ivec(path: &Path) -> Vec<Vec<i32>> {
    let data = std::fs::read(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let mut result = Vec::new();
    let mut pos = 0;
    while pos + 4 <= data.len() {
        let dim = i32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + dim * 4 > data.len() { break; }
        let vec: Vec<i32> = (0..dim)
            .map(|i| i32::from_le_bytes(data[pos + i*4..pos + i*4 + 4].try_into().unwrap()))
            .collect();
        pos += dim * 4;
        result.push(vec);
    }
    result
}

/// Read an fvec file as raw bytes (for identity comparison).
fn read_fvec_raw(path: &Path) -> Vec<u8> {
    std::fs::read(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// Count vectors in an fvec file from raw bytes.
fn fvec_count(data: &[u8]) -> usize {
    if data.len() < 4 { return 0; }
    let dim = i32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let record_size = 4 + dim * 4;
    if record_size == 0 { return 0; }
    data.len() / record_size
}

/// Get dimension from fvec raw bytes.
fn fvec_dim(data: &[u8]) -> usize {
    if data.len() < 4 { return 0; }
    i32::from_le_bytes(data[0..4].try_into().unwrap()) as usize
}

/// Count vectors in an fvec/ivec file by reading just the first dim.
fn read_ivec_count(path: &Path) -> usize {
    let data = std::fs::read(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    fvec_count(&data)
}
