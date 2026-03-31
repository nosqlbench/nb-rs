// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! DAG visualization for GK kernels.
//!
//! Renders a GK kernel's node graph as DOT, Mermaid, or self-contained
//! SVG. Uses petgraph for DOT export and layout-rs for pure-Rust SVG
//! rendering (no external graphviz needed).

use std::collections::{HashMap, HashSet};

use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::dot::{Dot, Config};

use crate::dsl::ast::*;
use crate::dsl::{lexer, parser};

/// Node kind for styling.
#[derive(Debug, Clone, Copy, PartialEq)]
enum VizNodeKind {
    Coordinate,
    Function,
}

/// Render a GK source string as a DOT digraph.
pub fn gk_to_dot(source: &str) -> Result<String, String> {
    let graph = build_petgraph(source)?;
    Ok(format!("{:?}", Dot::with_config(&graph, &[Config::EdgeNoLabel])))
}

/// Render a GK source string as a Mermaid flowchart.
pub fn gk_to_mermaid(source: &str) -> Result<String, String> {
    let (nodes, edges, node_kinds) = build_raw_graph(source)?;

    let mut lines = vec!["flowchart TD".to_string()];

    for (i, (label, kind)) in nodes.iter().zip(node_kinds.iter()).enumerate() {
        let escaped = label.replace('"', "'");
        let shape = match kind {
            VizNodeKind::Coordinate => format!("    n{i}([\"{escaped}\"])"),
            VizNodeKind::Function => format!("    n{i}[\"{escaped}\"]"),
        };
        lines.push(shape);
    }

    for (from, to) in &edges {
        lines.push(format!("    n{from} --> n{to}"));
    }

    lines.push("    classDef coord fill:#e1f5fe,stroke:#0288d1".into());
    lines.push("    classDef func fill:#fff3e0,stroke:#f57c00".into());
    for (i, kind) in node_kinds.iter().enumerate() {
        let class = match kind {
            VizNodeKind::Coordinate => "coord",
            VizNodeKind::Function => "func",
        };
        lines.push(format!("    class n{i} {class}"));
    }

    Ok(lines.join("\n"))
}

/// Render a GK source string as self-contained SVG.
///
/// Uses layout-rs for pure-Rust layout and rendering — no external
/// tools needed.
pub fn gk_to_svg(source: &str) -> Result<String, String> {
    use layout::backends::svg::SVGWriter;
    use layout::core::base::Orientation;
    use layout::core::geometry::Point;
    use layout::core::style::*;
    use layout::std_shapes::shapes::*;
    use layout::topo::layout::VisualGraph;

    let (nodes, edges, node_kinds) = build_raw_graph(source)?;

    let mut vg = VisualGraph::new(Orientation::TopToBottom);
    let size = Point::new(180., 40.);

    // Add nodes
    let handles: Vec<_> = nodes.iter().zip(node_kinds.iter()).map(|(label, kind)| {
        let shape = match kind {
            VizNodeKind::Coordinate => ShapeKind::new_box(label),
            VizNodeKind::Function => ShapeKind::new_box(label),
        };
        let style = StyleAttr::simple();
        let element = layout::std_shapes::shapes::Element::create(
            shape, style, Orientation::LeftToRight, size,
        );
        vg.add_node(element)
    }).collect();

    // Add edges
    for (from, to) in &edges {
        let arrow = layout::std_shapes::shapes::Arrow::simple("");
        vg.add_edge(arrow, handles[*from], handles[*to]);
    }

    // Layout and render
    let mut svg = SVGWriter::new();
    vg.do_it(false, false, false, &mut svg);
    Ok(svg.finalize())
}

/// Build a petgraph DiGraph from GK source (for DOT export).
fn build_petgraph(source: &str) -> Result<DiGraph<String, ()>, String> {
    let (nodes, edges, _) = build_raw_graph(source)?;

    let mut graph = DiGraph::new();
    let indices: Vec<NodeIndex> = nodes.iter()
        .map(|label| graph.add_node(label.clone()))
        .collect();

    for (from, to) in &edges {
        graph.add_edge(indices[*from], indices[*to], ());
    }

    Ok(graph)
}

/// Build raw node/edge lists from GK source.
fn build_raw_graph(source: &str) -> Result<(Vec<String>, Vec<(usize, usize)>, Vec<VizNodeKind>), String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;

    let mut nodes: Vec<String> = Vec::new();
    let mut node_kinds: Vec<VizNodeKind> = Vec::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut edges: Vec<(usize, usize)> = Vec::new();

    // Collect coordinates and defined names
    let mut coord_names: Vec<String> = Vec::new();
    let mut defined_names: HashSet<String> = HashSet::new();

    for stmt in &ast.statements {
        match stmt {
            Statement::Coordinates(names, _) => {
                coord_names.extend(names.clone());
            }
            Statement::InitBinding(b) => { defined_names.insert(b.name.clone()); }
            Statement::CycleBinding(b) => {
                for t in &b.targets { defined_names.insert(t.clone()); }
            }
            Statement::ModuleDef(_) => {}
        }
    }

    // Infer coordinates if not declared
    if coord_names.is_empty() {
        let mut refs: HashSet<String> = HashSet::new();
        for stmt in &ast.statements {
            let expr = match stmt {
                Statement::Coordinates(_, _) | Statement::ModuleDef(_) => continue,
                Statement::InitBinding(b) => &b.value,
                Statement::CycleBinding(b) => &b.value,
            };
            collect_expr_idents(expr, &mut refs);
        }
        for name in refs {
            if !defined_names.contains(&name) {
                coord_names.push(name);
            }
        }
        coord_names.sort();
    }

    // Add coordinate nodes
    for name in &coord_names {
        let idx = nodes.len();
        nodes.push(name.clone());
        node_kinds.push(VizNodeKind::Coordinate);
        name_to_idx.insert(name.clone(), idx);
    }

    // Process each binding
    for stmt in &ast.statements {
        match stmt {
            Statement::Coordinates(_, _) | Statement::ModuleDef(_) => continue,
            Statement::InitBinding(b) => {
                let idx = nodes.len();
                let label = format_node_label(&b.value, &b.name);
                nodes.push(label);
                node_kinds.push(VizNodeKind::Function);
                name_to_idx.insert(b.name.clone(), idx);

                let mut refs: HashSet<String> = HashSet::new();
                collect_expr_idents(&b.value, &mut refs);
                for ref_name in &refs {
                    if let Some(&from_idx) = name_to_idx.get(ref_name.as_str()) {
                        edges.push((from_idx, idx));
                    }
                }
            }
            Statement::CycleBinding(b) => {
                let target = if b.targets.len() == 1 {
                    b.targets[0].clone()
                } else {
                    format!("({})", b.targets.join(", "))
                };
                let idx = nodes.len();
                let label = format_node_label(&b.value, &target);
                nodes.push(label);
                node_kinds.push(VizNodeKind::Function);

                for t in &b.targets {
                    name_to_idx.insert(t.clone(), idx);
                }

                let mut refs: HashSet<String> = HashSet::new();
                collect_expr_idents(&b.value, &mut refs);
                for ref_name in &refs {
                    if let Some(&from_idx) = name_to_idx.get(ref_name.as_str()) {
                        if from_idx != idx {
                            edges.push((from_idx, idx));
                        }
                    }
                }
            }
        }
    }

    Ok((nodes, edges, node_kinds))
}

/// Format a node label from its expression and target name.
fn format_node_label(expr: &Expr, target: &str) -> String {
    match expr {
        Expr::Call(call) => format!("{} := {}(..)", target, call.func),
        Expr::Ident(id, _) => format!("{} := {}", target, id),
        Expr::IntLit(v, _) => format!("{} = {}", target, v),
        Expr::FloatLit(v, _) => format!("{} = {}", target, v),
        Expr::StringLit(s, _) => format!("{} = \"{}\"", target, truncate(s, 20)),
        _ => target.to_string(),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max { format!("{}...", &s[..max]) } else { s.to_string() }
}

/// Collect all identifier references from an expression.
fn collect_expr_idents(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Ident(name, _) => { out.insert(name.clone()); }
        Expr::Call(call) => {
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) | Arg::Named(_, e) => e,
                };
                collect_expr_idents(inner, out);
            }
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems { collect_expr_idents(e, out); }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIMPLE_GK: &str = r#"
        coordinates := (cycle)
        h := hash(cycle)
        user_id := mod(h, 1000000)
    "#;

    #[test]
    fn dot_output() {
        let dot = gk_to_dot(SIMPLE_GK).unwrap();
        assert!(dot.contains("digraph"));
        assert!(dot.contains("cycle"));
        assert!(dot.contains("hash"));
    }

    #[test]
    fn mermaid_output() {
        let mermaid = gk_to_mermaid(SIMPLE_GK).unwrap();
        assert!(mermaid.contains("flowchart TD"));
        assert!(mermaid.contains("-->"));
    }

    #[test]
    fn svg_output() {
        let svg = gk_to_svg(SIMPLE_GK).unwrap();
        assert!(svg.contains("<svg"));
        assert!(svg.contains("</svg>"));
    }

    #[test]
    fn inferred_coords() {
        let src = "h := hash(cycle)\nid := mod(h, 100)";
        let dot = gk_to_dot(src).unwrap();
        assert!(dot.contains("cycle"));
    }

    #[test]
    fn multi_output_destructuring() {
        let src = "coordinates := (cycle)\n(x, y) := mixed_radix(cycle, 100, 0)\nhx := hash(x)";
        let mermaid = gk_to_mermaid(src).unwrap();
        assert!(mermaid.contains("mixed_radix"));
    }

    #[test]
    fn svg_realistic_dag() {
        let src = r#"
            (device, reading) := mixed_radix(cycle, 100, 0)
            device_h := hash(device)
            device_id := mod(device_h, 100000)
            combined := interleave(device_h, reading)
            h0 := hash(combined)
            q_temp := unit_interval(h0)
            temperature := icd_normal(q_temp, 22.0, 3.0)
            timestamp := add(reading, 1710000000000)
        "#;
        let svg = gk_to_svg(src).unwrap();
        assert!(svg.contains("<svg"));
        assert!(svg.len() > 1000); // a real DAG should produce substantial SVG
    }
}
