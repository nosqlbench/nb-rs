// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK compiler diagnostic event stream.
//!
//! The compiler emits typed events for each step: parsing, binding
//! resolution, module inlining, type adaptation, constant folding,
//! fusion, and compilation level selection.
//!
//! Events are tagged with severity levels:
//! - **Info**: normal compilation steps (parsed, resolved, folded)
//! - **Advisory**: type coercions, widenings, and implicit conversions
//!   that the user should be aware of for module design quality
//! - **Warning**: potential performance or correctness issues
//! - **Error**: compilation failures (surfaced as Result::Err, not events)

/// Severity level for compiler diagnostic events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLevel {
    /// Normal compilation step — informational only.
    Info,
    /// Design advisory — implicit conversion or coercion that the user
    /// should review for module quality. Query with `--diagnose`.
    Advisory,
    /// Potential performance or correctness issue.
    Warning,
}

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
    /// Auto-widening type coercion inserted by the compiler.
    TypeWidening { from: &'static str, to: &'static str, context: String },
    /// Warning during compilation.
    Warning { message: String },
    /// Summary of the compiled program.
    Summary { nodes: usize, outputs: usize, constants_folded: usize },
}

impl CompileEvent {
    /// The severity level of this event.
    pub fn level(&self) -> EventLevel {
        match self {
            // Info: normal steps
            CompileEvent::Parsed { .. } => EventLevel::Info,
            CompileEvent::BindingResolved { .. } => EventLevel::Info,
            CompileEvent::ModuleInlined { .. } => EventLevel::Info,
            CompileEvent::OutputDeclared { .. } => EventLevel::Info,
            CompileEvent::CompileLevelSelected { .. } => EventLevel::Info,
            CompileEvent::ParamInjected { .. } => EventLevel::Info,
            CompileEvent::ConstantFolded { .. } => EventLevel::Info,
            CompileEvent::FusionApplied { .. } => EventLevel::Info,
            CompileEvent::Summary { .. } => EventLevel::Info,

            // Advisory: implicit conversions the user should review
            CompileEvent::TypeAdapterInserted { .. } => EventLevel::Advisory,
            CompileEvent::TypeWidening { .. } => EventLevel::Advisory,
            CompileEvent::LegacyTranslated { .. } => EventLevel::Advisory,

            // Warning: potential issues
            CompileEvent::ConfigWireCycleWarning { .. } => EventLevel::Warning,
            CompileEvent::Warning { .. } => EventLevel::Warning,
        }
    }
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

    /// Return only advisory-level events (type coercions, widenings).
    /// These are the "module design quality" messages users query with --diagnose.
    pub fn advisories(&self) -> Vec<&CompileEvent> {
        self.events.iter().filter(|e| e.level() == EventLevel::Advisory).collect()
    }

    /// Return only warning-level events.
    pub fn warnings(&self) -> Vec<&CompileEvent> {
        self.events.iter().filter(|e| e.level() == EventLevel::Warning).collect()
    }

    /// Format all events as human-readable diagnostic lines.
    /// Each line is prefixed with the severity tag.
    pub fn format(&self) -> String {
        self.events.iter().map(|e| {
            let tag = match e.level() {
                EventLevel::Info => "info",
                EventLevel::Advisory => "advisory",
                EventLevel::Warning => "warning",
            };
            let msg = match e {
            CompileEvent::Parsed { statements } =>
                format!("parsed {statements} statement(s)"),
            CompileEvent::BindingResolved { name, node_type } =>
                format!("resolved '{name}' → {node_type}"),
            CompileEvent::ModuleInlined { name, nodes_added } =>
                format!("module '{name}' inlined ({nodes_added} nodes)"),
            CompileEvent::LegacyTranslated { name, gk_expr } =>
                format!("legacy '{name}' → {gk_expr}"),
            CompileEvent::TypeAdapterInserted { from_node, to_node, adapter } =>
                format!("type adapter {adapter}: {from_node} → {to_node}"),
            CompileEvent::ConstantFolded { node, value } =>
                format!("constant folded: {node} → {value}"),
            CompileEvent::FusionApplied { pattern, nodes_replaced } =>
                format!("fusion: {pattern} ({nodes_replaced} nodes replaced)"),
            CompileEvent::OutputDeclared { name } =>
                format!("output '{name}'"),
            CompileEvent::CompileLevelSelected { node, level } =>
                format!("{node} → {level}"),
            CompileEvent::ParamInjected { name, value } =>
                format!("param '{name}' = {value}"),
            CompileEvent::ConfigWireCycleWarning { node, port } =>
                format!("config wire '{port}' on '{node}' connected to cycle-time source"),
            CompileEvent::TypeWidening { from, to, context } =>
                format!("widening {from} → {to} in {context}"),
            CompileEvent::Warning { message } =>
                format!("{message}"),
            CompileEvent::Summary { nodes, outputs, constants_folded } =>
                format!("{nodes} nodes, {outputs} outputs, {constants_folded} constant(s) folded"),
            };
            format!("gk[{tag}]: {msg}")
        }).collect::<Vec<_>>().join("\n")
    }
}
