// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Graph editor backend: palette API and graph JSON → GK source compilation.
//!
//! The palette API serves the function registry as JSON for the
//! Litegraph.js client to register node types. The compile endpoint
//! converts a Litegraph graph JSON into GK source, compiles it, and
//! returns the source text, SVG visualization, and sample output.

use std::collections::HashMap;

use nb_variates::dsl::registry;
use nb_variates::viz;
use serde::{Deserialize, Serialize};

// ─── Palette API ────────────────────────────────────────────

/// A category of functions for the node palette.
#[derive(Serialize)]
pub struct PaletteCategory {
    pub category: String,
    pub functions: Vec<PaletteFunction>,
}

/// A single function in the palette.
#[derive(Serialize)]
pub struct PaletteFunction {
    pub name: String,
    pub description: String,
    pub wire_inputs: usize,
    pub outputs: usize,
    pub const_params: Vec<PaletteParam>,
    pub variadic: bool,
}

/// A constant parameter on a function.
#[derive(Serialize)]
pub struct PaletteParam {
    pub name: String,
    pub required: bool,
}

/// Build the palette from the GK function registry.
pub fn build_palette() -> Vec<PaletteCategory> {
    let grouped = registry::by_category();
    grouped
        .into_iter()
        .map(|(cat, funcs)| PaletteCategory {
            category: cat.display_name().to_string(),
            functions: funcs
                .iter()
                .map(|sig| PaletteFunction {
                    name: sig.name.to_string(),
                    description: sig.description.to_string(),
                    wire_inputs: sig.wire_inputs,
                    outputs: if sig.outputs == 0 { 1 } else { sig.outputs },
                    const_params: sig
                        .const_params
                        .iter()
                        .map(|(name, req)| PaletteParam {
                            name: name.to_string(),
                            required: *req,
                        })
                        .collect(),
                    variadic: sig.variadic,
                })
                .collect(),
        })
        .collect()
}

// ─── Graph Compilation ──────────────────────────────────────

/// Litegraph serialized graph (subset of fields we need).
#[derive(Deserialize)]
pub struct LiteGraph {
    #[serde(default)]
    pub nodes: Vec<LiteNode>,
    #[serde(default)]
    pub links: Vec<serde_json::Value>,
}

/// A node in the Litegraph graph.
#[derive(Deserialize)]
pub struct LiteNode {
    pub id: i64,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub properties: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub inputs: Vec<LiteSlot>,
    #[serde(default)]
    pub outputs: Vec<LiteSlot>,
    #[serde(default)]
    pub widgets_values: Vec<serde_json::Value>,
}

/// A slot (input or output) on a node.
#[derive(Deserialize)]
pub struct LiteSlot {
    pub name: String,
    #[serde(rename = "type")]
    pub slot_type: Option<String>,
    #[serde(default)]
    pub link: Option<serde_json::Value>,
}

/// Result of compiling a graph.
#[derive(Serialize)]
pub struct CompileResult {
    pub gk_source: String,
    pub svg: String,
    pub samples: Vec<String>,
    pub error: Option<String>,
}

/// Convert a Litegraph JSON graph into GK source text, compile it,
/// render SVG, and produce sample output for a few cycles.
pub fn compile_graph(graph_json: &str) -> CompileResult {
    let graph: LiteGraph = match serde_json::from_str(graph_json) {
        Ok(g) => g,
        Err(e) => {
            return CompileResult {
                gk_source: String::new(),
                svg: String::new(),
                samples: vec![],
                error: Some(format!("invalid graph JSON: {e}")),
            };
        }
    };

    let gk_source = match graph_to_gk(&graph) {
        Ok(src) => src,
        Err(e) => {
            return CompileResult {
                gk_source: String::new(),
                svg: String::new(),
                samples: vec![],
                error: Some(e),
            };
        }
    };

    if gk_source.trim().is_empty() {
        return CompileResult {
            gk_source,
            svg: String::new(),
            samples: vec![],
            error: None,
        };
    }

    let svg = viz::gk_to_svg(&gk_source).unwrap_or_default();

    // Compile and sample a few cycles.
    let samples = match nb_variates::dsl::compile_gk(&gk_source) {
        Ok(mut kernel) => {
            let output_names: Vec<String> = kernel.output_names().iter().map(|s| s.to_string()).collect();
            (0..5u64)
                .map(|cycle| {
                    kernel.set_coordinates(&[cycle]);
                    let vals: Vec<String> = output_names
                        .iter()
                        .map(|name| {
                            let v = kernel.pull(name);
                            format!("{name}={}", v.to_display_string())
                        })
                        .collect();
                    format!("cycle {cycle}: {}", vals.join(", "))
                })
                .collect()
        }
        Err(e) => {
            return CompileResult {
                gk_source,
                svg,
                samples: vec![],
                error: Some(format!("compile error: {e}")),
            };
        }
    };

    CompileResult {
        gk_source,
        svg,
        samples,
        error: None,
    }
}

/// Convert a Litegraph graph structure into GK source text.
fn graph_to_gk(graph: &LiteGraph) -> Result<String, String> {
    if graph.nodes.is_empty() {
        return Ok(String::new());
    }

    // Build link map: link_id → (source_node_id, source_slot, dest_node_id, dest_slot)
    // Litegraph link format: [link_id, origin_id, origin_slot, target_id, target_slot, type]
    let mut link_map: HashMap<i64, (i64, usize)> = HashMap::new(); // link_id → (source_node, source_slot)
    for link_val in &graph.links {
        if let Some(arr) = link_val.as_array() {
            if arr.len() >= 5 {
                let link_id = arr[0].as_i64().unwrap_or(-1);
                let origin_id = arr[1].as_i64().unwrap_or(-1);
                let origin_slot = arr[2].as_u64().unwrap_or(0) as usize;
                link_map.insert(link_id, (origin_id, origin_slot));
            }
        }
    }

    // Build node output name map: (node_id, slot_index) → variable name
    let mut output_names: HashMap<(i64, usize), String> = HashMap::new();

    // Find coordinate nodes and collect their output names.
    let mut coord_names: Vec<String> = Vec::new();
    for node in &graph.nodes {
        if node.node_type == "gk/coordinates" {
            for (i, out) in node.outputs.iter().enumerate() {
                let name = out.name.clone();
                coord_names.push(name.clone());
                output_names.insert((node.id, i), name);
            }
        }
    }

    if coord_names.is_empty() {
        coord_names.push("cycle".into());
    }

    // Assign output variable names for function nodes.
    for node in &graph.nodes {
        if node.node_type == "gk/coordinates" || node.node_type == "gk/output" {
            continue;
        }
        let func_name = node.node_type.strip_prefix("gk/").unwrap_or(&node.node_type);
        if node.outputs.len() == 1 {
            // Single output: use the node's custom name or func_id.
            let var_name = node
                .properties
                .get("output_name")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}_{}", func_name, node.id));
            output_names.insert((node.id, 0), var_name);
        } else {
            for (i, out) in node.outputs.iter().enumerate() {
                let var_name = format!("{}_{}", out.name, node.id);
                output_names.insert((node.id, i), var_name);
            }
        }
    }

    // Helper: resolve what variable name feeds a given input slot.
    let resolve_input = |node: &LiteNode, slot_idx: usize| -> Option<String> {
        let input = node.inputs.get(slot_idx)?;
        // input.link can be a single link_id or null
        let link_id = match &input.link {
            Some(serde_json::Value::Number(n)) => n.as_i64()?,
            Some(serde_json::Value::Array(arr)) if !arr.is_empty() => arr[0].as_i64()?,
            _ => return None,
        };
        let (src_node, src_slot) = link_map.get(&link_id)?;
        output_names.get(&(*src_node, *src_slot)).cloned()
    };

    // Generate GK source.
    let mut lines = Vec::new();
    lines.push(format!(
        "coordinates := ({})",
        coord_names.join(", ")
    ));

    // Topological order: process nodes in ID order (Litegraph assigns
    // incrementing IDs, which naturally respects creation order).
    let mut sorted_nodes: Vec<&LiteNode> = graph
        .nodes
        .iter()
        .filter(|n| n.node_type != "gk/coordinates" && n.node_type != "gk/output")
        .collect();
    sorted_nodes.sort_by_key(|n| n.id);

    for node in &sorted_nodes {
        let func_name = node.node_type.strip_prefix("gk/").unwrap_or(&node.node_type);

        // Collect wire inputs.
        let mut args: Vec<String> = Vec::new();
        for i in 0..node.inputs.len() {
            if let Some(src) = resolve_input(node, i) {
                args.push(src);
            } else {
                // Unconnected input — use "cycle" as fallback.
                args.push(coord_names.first().cloned().unwrap_or("cycle".into()));
            }
        }

        // Collect const params from widget values.
        for (i, widget_val) in node.widgets_values.iter().enumerate() {
            let val_str = match widget_val {
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::String(s) => {
                    // If it parses as a number, emit raw. Otherwise quote.
                    if s.parse::<f64>().is_ok() {
                        s.clone()
                    } else {
                        format!("\"{}\"", s.replace('"', "\\\""))
                    }
                }
                _ => continue,
            };
            args.push(val_str);
        }

        // Build the output targets.
        let targets: Vec<String> = (0..node.outputs.len())
            .filter_map(|i| output_names.get(&(node.id, i)).cloned())
            .collect();

        let target_str = if targets.len() == 1 {
            targets[0].clone()
        } else if targets.len() > 1 {
            format!("({})", targets.join(", "))
        } else {
            format!("{}_{}", func_name, node.id)
        };

        lines.push(format!(
            "{} := {}({})",
            target_str,
            func_name,
            args.join(", ")
        ));
    }

    Ok(lines.join("\n"))
}
