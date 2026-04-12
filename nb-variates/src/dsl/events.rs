// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK compiler diagnostic event stream (SRD 45).
//!
//! The compiler emits typed events for each step: parsing, binding
//! resolution, module inlining, type adaptation, constant folding,
//! fusion, and compilation level selection.

/// A diagnostic event from the GK compilation pipeline.
#[derive(Debug, Clone)]
pub enum CompileEvent {
    /// DSL source parsed into AST.
    Parsed { statements: usize },
    /// A binding was resolved from DSL to a node.
    BindingResolved { name: String, node_type: String },
    /// A module was loaded and inlined.
    ModuleInlined { name: String, nodes_added: usize },
    /// A legacy binding chain was translated to GK source.
    LegacyTranslated { name: String, gk_expr: String },
    /// Type adapter inserted between mismatched ports.
    TypeAdapterInserted { from_node: String, to_node: String, adapter: String },
    /// Init-time constant folded (SRD 44).
    ConstantFolded { node: String, value: String },
    /// Fusion pattern matched and applied (SRD 36).
    FusionApplied { pattern: String, nodes_replaced: usize },
    /// Output declared.
    OutputDeclared { name: String },
    /// Compilation level selected for a node.
    CompileLevelSelected { node: String, level: String },
    /// Workload parameter injected as constant.
    ParamInjected { name: String, value: String },
    /// Config wire connected to a cycle-time source (performance warning).
    ConfigWireCycleWarning { node: String, port: String },
    /// Warning during compilation.
    Warning { message: String },
    /// Summary of the compiled program.
    Summary { nodes: usize, outputs: usize, constants_folded: usize },
}

/// Collects diagnostic events during compilation.
#[derive(Debug, Default)]
pub struct CompileEventLog {
    events: Vec<CompileEvent>,
}

impl CompileEventLog {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn push(&mut self, event: CompileEvent) {
        self.events.push(event);
    }

    pub fn events(&self) -> &[CompileEvent] {
        &self.events
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Format all events as human-readable diagnostic lines.
    pub fn format(&self) -> String {
        self.events.iter().map(|e| match e {
            CompileEvent::Parsed { statements } =>
                format!("gk: parsed {statements} statement(s)"),
            CompileEvent::BindingResolved { name, node_type } =>
                format!("gk: resolved '{name}' → {node_type}"),
            CompileEvent::ModuleInlined { name, nodes_added } =>
                format!("gk: module '{name}' inlined ({nodes_added} nodes)"),
            CompileEvent::LegacyTranslated { name, gk_expr } =>
                format!("gk: legacy '{name}' → {gk_expr}"),
            CompileEvent::TypeAdapterInserted { from_node, to_node, adapter } =>
                format!("gk: type adapter {adapter}: {from_node} → {to_node}"),
            CompileEvent::ConstantFolded { node, value } =>
                format!("gk: constant folded: {node} → {value}"),
            CompileEvent::FusionApplied { pattern, nodes_replaced } =>
                format!("gk: fusion applied: {pattern} ({nodes_replaced} nodes replaced)"),
            CompileEvent::OutputDeclared { name } =>
                format!("gk: output '{name}'"),
            CompileEvent::CompileLevelSelected { node, level } =>
                format!("gk: {node} → {level}"),
            CompileEvent::ParamInjected { name, value } =>
                format!("gk: param '{name}' = {value}"),
            CompileEvent::ConfigWireCycleWarning { node, port } =>
                format!("gk: warning: config wire '{port}' on '{node}' connected to cycle-time source"),
            CompileEvent::Warning { message } =>
                format!("gk: warning: {message}"),
            CompileEvent::Summary { nodes, outputs, constants_folded } =>
                format!("gk: {nodes} nodes, {outputs} outputs, {constants_folded} constant(s) folded"),
        }).collect::<Vec<_>>().join("\n")
    }
}
