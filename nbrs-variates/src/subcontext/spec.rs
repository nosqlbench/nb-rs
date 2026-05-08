// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Typed import / export contracts (per SRD-13e §1.2).
//!
//! Phase 1 ships a minimal shape sufficient to enforce
//! SRD-67's Rule 1 (import resolution) and Rule 2 (export
//! collision diagnostics). The full SRD-13e contract surface
//! (lifecycle classifications, widening rules, modifier
//! compatibility matrix) is captured here as data; the spawn
//! step in [`crate::subcontext::ScopeKernel::spawn`] applies
//! whichever rules Phase 1 can ground in the existing kernel
//! semantics, and stubs / TODOs out the rest.

use crate::dsl::ast::BindingModifier;
use crate::node::PortType;

/// Lifecycle classification for an import — taxonomically what
/// SRD-13e §1.2 specifies. Drives the spawn-time validation
/// decisions per SRD-67 Rule 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportClassification {
    /// `final X: T` — fold the parent's value into the child at
    /// compile/init time.
    CompileConst,
    /// `extern X: T` — wire to the parent's value via an input
    /// slot. Most common shape.
    Extern,
    /// `shared X: T` — share-cell-attach against a parent's
    /// `shared`-modifier export.
    Shared,
    /// Iteration extern: parent's coordinate / iteration variable
    /// (SRD-67 Rule 4 routes through the parent's coordinate
    /// buffer).
    IterationExtern,
}

/// Lifecycle classification for an export — what kind of
/// downstream contract this export carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportClassification {
    /// Locally-defined output of the body. The default shape.
    Local,
    /// `final` modifier on the body's binding.
    Final,
    /// `shared` modifier on the body's binding — shared cell
    /// available to descendants.
    Shared,
    /// Iteration variable (comprehension coordinate). Routed
    /// through the parent's coord buffer at spawn (Rule 4).
    Coordinate,
    /// `volatile` modifier — excluded from const-fold identity.
    Volatile,
}

/// Typed import declaration: a name the child's body will
/// reference, expecting the parent to export it.
#[derive(Debug, Clone)]
pub struct ImportSpec {
    /// Name as referenced inside the child body.
    pub name: String,
    /// Expected port type. Spawn validates against the parent's
    /// matching export (Rule 1).
    pub port_type: PortType,
    /// Lifecycle classification.
    pub classification: ImportClassification,
}

impl ImportSpec {
    pub fn extern_(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            classification: ImportClassification::Extern,
        }
    }

    pub fn final_(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            classification: ImportClassification::CompileConst,
        }
    }

    pub fn shared(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            classification: ImportClassification::Shared,
        }
    }

    pub fn iter_var(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            classification: ImportClassification::IterationExtern,
        }
    }
}

/// Typed export declaration: a named value the child produces,
/// available to its own descendants.
#[derive(Debug, Clone)]
pub struct ExportSpec {
    /// Name as it appears in the child's body.
    pub name: String,
    /// Port type the child binds.
    pub port_type: PortType,
    /// Modifier (`final` / `shared` / none) — the standard GK
    /// modifier set; spawn uses it to apply Rule 2.
    pub modifier: BindingModifier,
    /// Lifecycle classification.
    pub classification: ExportClassification,
}

impl ExportSpec {
    pub fn local(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            modifier: BindingModifier::NONE,
            classification: ExportClassification::Local,
        }
    }

    pub fn final_(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            modifier: BindingModifier::FINAL,
            classification: ExportClassification::Final,
        }
    }

    pub fn shared(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            modifier: BindingModifier::SHARED,
            classification: ExportClassification::Shared,
        }
    }

    pub fn iter_var(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            name: name.into(),
            port_type,
            modifier: BindingModifier::NONE,
            classification: ExportClassification::Coordinate,
        }
    }
}
