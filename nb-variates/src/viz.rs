// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! DAG visualization for GK kernels.
//!
//! Renders a GK kernel's node graph as DOT (with record nodes and
//! port-based edge routing), Mermaid, or self-contained SVG.
//!
//! The DOT output uses graphviz record syntax:
//! - Each node has named input ports (top) and output ports (bottom)
//! - Edges connect from output ports to input ports
//! - Dark theme colors via graph/node/edge attributes

use std::collections::{HashMap, HashSet};

use crate::dsl::ast::*;
use crate::dsl::{lexer, parser};

/// A node in the visualization graph.
struct VizNode {
    /// Unique ID for DOT (e.g., "n0", "n1")
    id: String,
    /// Display label (function name or binding expression)
    label: String,
    /// Input wire names (from upstream nodes/coords)
    inputs: Vec<String>,
    /// Output wire names (what this node produces)
    outputs: Vec<String>,
    /// Is this a coordinate input?
    is_coord: bool,
}

/// An edge connecting an output to an input.
struct VizEdge {
    from_node: String,
    from_port: String,
    to_node: String,
    to_port: String,
}

/// Render a GK source string as DOT with record nodes and ports.
pub fn gk_to_dot(source: &str) -> Result<String, String> {
    let (nodes, edges) = build_graph(source)?;
    let mut dot = String::new();

    dot.push_str("digraph gk {\n");
    dot.push_str("    rankdir=TB;\n");
    dot.push_str("    bgcolor=\"#1a1a2e\";\n");
    dot.push_str("    node [shape=record, style=filled, fontname=\"monospace\", fontsize=11];\n");
    dot.push_str("    edge [color=\"#4da6ff\", fontcolor=\"#8888a0\", fontname=\"monospace\", fontsize=9];\n");
    dot.push('\n');

    for node in &nodes {
        if node.is_coord {
            // Register nodes (INPUTS / OUTPUTS): record with labeled ports
            if node.outputs.is_empty() && !node.inputs.is_empty() {
                // OUTPUTS register: input ports only (bottom of graph)
                let ports: Vec<String> = node.inputs.iter()
                    .map(|name| format!("<i_{name}> {name}"))
                    .collect();
                dot.push_str(&format!(
                    "    {} [label=\"{{ {{ {} }} | {} }}\", fillcolor=\"#0f3460\", \
                     fontcolor=\"#4ecca3\", color=\"#4ecca3\", penwidth=2];\n",
                    node.id, ports.join(" | "), dot_escape(&node.label),
                ));
            } else if !node.outputs.is_empty() && node.inputs.is_empty() {
                // INPUTS register: output ports only (top of graph)
                let ports: Vec<String> = node.outputs.iter()
                    .map(|name| format!("<o_{name}> {name}"))
                    .collect();
                dot.push_str(&format!(
                    "    {} [label=\"{{ {} | {{ {} }} }}\", fillcolor=\"#0f3460\", \
                     fontcolor=\"#4da6ff\", color=\"#4da6ff\", penwidth=2];\n",
                    node.id, dot_escape(&node.label), ports.join(" | "),
                ));
            } else {
                // Fallback
                dot.push_str(&format!(
                    "    {} [label=\"{}\", shape=oval, fillcolor=\"#16213e\", \
                     fontcolor=\"#4da6ff\", color=\"#4da6ff\"];\n",
                    node.id, dot_escape(&node.label)
                ));
            }
        } else {
            // Function nodes: record with input ports | label | output ports
            let input_ports = if node.inputs.is_empty() {
                String::new()
            } else {
                let ports: Vec<String> = node.inputs.iter()
                    .map(|name| format!("<i_{name}> {name}"))
                    .collect();
                format!("{{ {} }} | ", ports.join(" | "))
            };

            let output_ports = if node.outputs.is_empty() {
                String::new()
            } else {
                let ports: Vec<String> = node.outputs.iter()
                    .map(|name| format!("<o_{name}> {name}"))
                    .collect();
                format!(" | {{ {} }}", ports.join(" | "))
            };

            dot.push_str(&format!(
                "    {} [label=\"{}{}{}\", fillcolor=\"#16213e\", \
                 fontcolor=\"#e0e0e0\", color=\"#0f3460\"];\n",
                node.id,
                input_ports,
                dot_escape(&node.label),
                output_ports,
            ));
        }
    }

    dot.push('\n');

    for edge in &edges {
        let from = if edge.from_port.is_empty() {
            edge.from_node.clone()
        } else {
            format!("{}:o_{}", edge.from_node, edge.from_port)
        };
        let to = if edge.to_port.is_empty() {
            edge.to_node.clone()
        } else {
            format!("{}:i_{}", edge.to_node, edge.to_port)
        };
        dot.push_str(&format!("    {} -> {};\n", from, to));
    }

    dot.push_str("}\n");
    Ok(dot)
}

/// Render a GK source string as a Mermaid flowchart.
pub fn gk_to_mermaid(source: &str) -> Result<String, String> {
    let (nodes, edges) = build_graph(source)?;
    let mut lines = vec!["flowchart TD".to_string()];

    for node in &nodes {
        let escaped = node.label.replace('"', "'");
        if node.is_coord {
            lines.push(format!("    {}([\"{}\"])", node.id, escaped));
        } else {
            lines.push(format!("    {}[\"{}\"]", node.id, escaped));
        }
    }

    for edge in &edges {
        let label = if edge.from_port.is_empty() && edge.to_port.is_empty() {
            String::new()
        } else {
            let port_name = if !edge.from_port.is_empty() { &edge.from_port } else { &edge.to_port };
            format!("|{}|", port_name)
        };
        lines.push(format!("    {} -->{} {}", edge.from_node, label, edge.to_node));
    }

    lines.push("    classDef coord fill:#16213e,stroke:#4da6ff,color:#4da6ff".into());
    lines.push("    classDef func fill:#16213e,stroke:#0f3460,color:#e0e0e0".into());
    for node in &nodes {
        let class = if node.is_coord { "input" } else { "func" };
        lines.push(format!("    class {} {class}", node.id));
    }

    Ok(lines.join("\n"))
}

/// Render a GK source string as self-contained SVG.
///
/// Generates DOT with record nodes and port syntax, then renders
/// through layout-rs (pure Rust, no external graphviz needed).
/// Layout-rs supports record shapes and port-based edge routing.
pub fn gk_to_svg(source: &str) -> Result<String, String> {
    let dot_source = gk_to_dot(source)?;

    // Parse DOT through layout-rs
    let mut parser = layout::gv::DotParser::new(&dot_source);
    let graph = parser.process()
        .map_err(|e| format!("DOT parse error: {e}"))?;

    // Build visual graph from parsed DOT
    let mut builder = layout::gv::GraphBuilder::new();
    builder.visit_graph(&graph);
    let mut visual = builder.get();

    // Layout and render to SVG
    let mut svg_writer = layout::backends::svg::SVGWriter::new();
    visual.do_it(false, false, false, &mut svg_writer);

    let raw = svg_writer.finalize();
    // Inject dark background
    let styled = raw.replacen("<svg ", "<svg style=\"background:#1a1a2e\" ", 1);
    Ok(styled)
}

// ─── Graph building ─────────────────────────────────────────

fn build_graph(source: &str) -> Result<(Vec<VizNode>, Vec<VizEdge>), String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;

    let mut nodes: Vec<VizNode> = Vec::new();
    let mut edges: Vec<VizEdge> = Vec::new();
    let mut name_to_node_id: HashMap<String, String> = HashMap::new();
    let mut node_counter = 0usize;

    // Collect coordinates and defined names
    let mut input_names: Vec<String> = Vec::new();
    let mut defined_names: HashSet<String> = HashSet::new();
    let mut all_output_names: Vec<String> = Vec::new();

    for stmt in &ast.statements {
        match stmt {
            Statement::Coordinates(names, _) => input_names.extend(names.clone()),
            Statement::InitBinding(b) => {
                defined_names.insert(b.name.clone());
                all_output_names.push(b.name.clone());
            }
            Statement::CycleBinding(b) => {
                for t in &b.targets {
                    defined_names.insert(t.clone());
                    all_output_names.push(t.clone());
                }
            }
            Statement::ModuleDef(_) | Statement::ExternPort(_) => {}
            Statement::Cursor(_) => {}
        }
    }

    // Infer coordinates
    if input_names.is_empty() {
        let mut refs: HashSet<String> = HashSet::new();
        for stmt in &ast.statements {
            let expr = match stmt {
                Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) => continue,
                Statement::InitBinding(b) => &b.value,
                Statement::CycleBinding(b) => &b.value,
            };
            collect_expr_idents(expr, &mut refs);
        }
        for name in refs {
            if !defined_names.contains(&name) { input_names.push(name); }
        }
        input_names.sort();
    }

    // Determine which outputs are terminal (not consumed by other nodes)
    let mut consumed: HashSet<String> = HashSet::new();
    for stmt in &ast.statements {
        let expr = match stmt {
            Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) => continue,
            Statement::InitBinding(b) => &b.value,
            Statement::CycleBinding(b) => &b.value,
        };
        collect_expr_idents(expr, &mut consumed);
    }
    let terminal_outputs: Vec<String> = all_output_names.iter()
        .filter(|name| !consumed.contains(*name))
        .cloned()
        .collect();

    // ─── INPUTS register (top) ──────────────────────────
    // Single record node with all coordinates as output ports
    let inputs_id = "inputs".to_string();
    {
        let mut input_ports: Vec<String> = Vec::new();
        for name in &input_names {
            input_ports.push(name.clone());
        }
        // TODO: add external ports here when capture wiring is implemented
        nodes.push(VizNode {
            id: inputs_id.clone(),
            label: "INPUTS".into(),
            inputs: vec![],
            outputs: input_ports,
            is_coord: true,
        });
        for name in &input_names {
            name_to_node_id.insert(name.clone(), inputs_id.clone());
        }
    }

    // ─── Function nodes (middle) ────────────────────────
    for stmt in &ast.statements {
        match stmt {
            Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) => continue,
            Statement::InitBinding(b) => {
                let id = format!("n{node_counter}");
                node_counter += 1;

                let mut input_refs: Vec<String> = Vec::new();
                collect_expr_idents_ordered(&b.value, &mut input_refs);

                let label = format_node_label(&b.value, &b.name);
                let output_names = vec![b.name.clone()];

                for ref_name in &input_refs {
                    if let Some(src_id) = name_to_node_id.get(ref_name) {
                        edges.push(VizEdge {
                            from_node: src_id.clone(),
                            from_port: ref_name.clone(),
                            to_node: id.clone(),
                            to_port: ref_name.clone(),
                        });
                    }
                }

                name_to_node_id.insert(b.name.clone(), id.clone());
                nodes.push(VizNode {
                    id, label, inputs: input_refs, outputs: output_names, is_coord: false,
                });
            }
            Statement::CycleBinding(b) => {
                let id = format!("n{node_counter}");
                node_counter += 1;

                let target_label = if b.targets.len() == 1 {
                    b.targets[0].clone()
                } else {
                    format!("({})", b.targets.join(", "))
                };

                let mut input_refs: Vec<String> = Vec::new();
                collect_expr_idents_ordered(&b.value, &mut input_refs);

                let label = format_node_label(&b.value, &target_label);

                for ref_name in &input_refs {
                    if let Some(src_id) = name_to_node_id.get(ref_name) {
                        edges.push(VizEdge {
                            from_node: src_id.clone(),
                            from_port: ref_name.clone(),
                            to_node: id.clone(),
                            to_port: ref_name.clone(),
                        });
                    }
                }

                for t in &b.targets {
                    name_to_node_id.insert(t.clone(), id.clone());
                }
                nodes.push(VizNode {
                    id, label, inputs: input_refs, outputs: b.targets.clone(), is_coord: false,
                });
            }
        }
    }

    // ─── OUTPUTS register (bottom) ──────────────────────
    // Single record node with all terminal outputs as input ports
    if !terminal_outputs.is_empty() {
        let outputs_id = "outputs".to_string();
        for name in &terminal_outputs {
            if let Some(src_id) = name_to_node_id.get(name) {
                edges.push(VizEdge {
                    from_node: src_id.clone(),
                    from_port: name.clone(),
                    to_node: outputs_id.clone(),
                    to_port: name.clone(),
                });
            }
        }
        nodes.push(VizNode {
            id: outputs_id,
            label: "OUTPUTS".into(),
            inputs: terminal_outputs,
            outputs: vec![],
            is_coord: true, // use coord styling (blue accent)
        });
    }

    Ok((nodes, edges))
}

fn format_node_label(expr: &Expr, target: &str) -> String {
    match expr {
        Expr::Call(call) => {
            let args: Vec<String> = call.args.iter().map(|a| match a {
                Arg::Positional(e) => format_expr_short(e),
                Arg::Named(n, e) => format!("{}: {}", n, format_expr_short(e)),
            }).collect();
            format!("{} := {}({})", target, call.func, args.join(", "))
        }
        Expr::Ident(id, _) => format!("{} := {}", target, id),
        Expr::IntLit(v, _) => format!("{} = {}", target, v),
        Expr::FloatLit(v, _) => format!("{} = {}", target, v),
        Expr::StringLit(s, _) => {
            let trunc = if s.len() > 20 { format!("{}...", &s[..20]) } else { s.clone() };
            format!("{} = \"{}\"", target, trunc)
        }
        _ => target.to_string(),
    }
}

fn format_expr_short(expr: &Expr) -> String {
    match expr {
        Expr::Ident(id, _) => id.clone(),
        Expr::IntLit(v, _) => v.to_string(),
        Expr::FloatLit(v, _) => format!("{v}"),
        Expr::StringLit(s, _) => format!("\"{s}\""),
        Expr::Call(call) => format!("{}(..)", call.func),
        _ => "..".into(),
    }
}

fn collect_expr_idents(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Ident(name, _) => { out.insert(name.clone()); }
        Expr::Call(call) => {
            for arg in &call.args {
                let inner = match arg { Arg::Positional(e) | Arg::Named(_, e) => e };
                collect_expr_idents(inner, out);
            }
        }
        Expr::ArrayLit(elems, _) => { for e in elems { collect_expr_idents(e, out); } }
        _ => {}
    }
}

/// Like collect_expr_idents but preserves order and avoids duplicates.
fn collect_expr_idents_ordered(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Ident(name, _) => {
            if !out.contains(name) { out.push(name.clone()); }
        }
        Expr::Call(call) => {
            for arg in &call.args {
                let inner = match arg { Arg::Positional(e) | Arg::Named(_, e) => e };
                collect_expr_idents_ordered(inner, out);
            }
        }
        Expr::ArrayLit(elems, _) => { for e in elems { collect_expr_idents_ordered(e, out); } }
        _ => {}
    }
}

fn dot_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
     .replace('"', "\\\"")
     .replace('{', "\\{")
     .replace('}', "\\}")
     .replace('<', "\\<")
     .replace('>', "\\>")
     .replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIMPLE_GK: &str = "coordinates := (cycle)\nh := hash(cycle)\nuser_id := mod(h, 1000000)";

    #[test]
    fn dot_has_ports() {
        let dot = gk_to_dot(SIMPLE_GK).unwrap();
        assert!(dot.contains("shape=record"));
        assert!(dot.contains("bgcolor"));
        assert!(dot.contains(":o_"));  // output port syntax
        assert!(dot.contains(":i_"));  // input port syntax
    }

    #[test]
    fn dot_dark_theme() {
        let dot = gk_to_dot(SIMPLE_GK).unwrap();
        assert!(dot.contains("#1a1a2e"));  // dark bg
        assert!(dot.contains("#16213e"));  // node fill
        assert!(dot.contains("#e0e0e0"));  // light text
    }

    #[test]
    fn mermaid_output() {
        let mermaid = gk_to_mermaid(SIMPLE_GK).unwrap();
        assert!(mermaid.contains("flowchart TD"));
        assert!(mermaid.contains("-->"));
    }

    #[test]
    fn svg_dark_background() {
        let svg = gk_to_svg(SIMPLE_GK).unwrap();
        assert!(svg.contains("<svg"));
        assert!(svg.contains("#1a1a2e"));
    }

    #[test]
    fn inferred_coords() {
        let src = "h := hash(cycle)\nid := mod(h, 100)";
        let dot = gk_to_dot(src).unwrap();
        assert!(dot.contains("cycle"));
    }

    #[test]
    fn multi_output() {
        let src = "coordinates := (cycle)\n(x, y) := mixed_radix(cycle, 100, 0)\nhx := hash(x)";
        let dot = gk_to_dot(src).unwrap();
        assert!(dot.contains("mixed_radix"));
        assert!(dot.contains("hash"));
    }
}
