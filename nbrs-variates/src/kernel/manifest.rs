// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Output manifest — the typed contract a compiled GK program
//! exposes to consumers.
//!
//! A `ManifestEntry` records `(name, port_type, modifier)` for one
//! output of a [`crate::kernel::GkProgram`]. Synthesizers that
//! emit GK source for descendant scopes consume manifests to
//! decide which names a child can extern from a parent (auto-
//! extern + `materialize_wiring_from_outer`) and what type to declare each
//! extern as.
//!
//! Lives in the kernel module because the data it carries is
//! pure GK metadata — name, port type, binding modifier — all
//! of which already have homes in `kernel` / `node` / `dsl`.

use crate::dsl::ast::BindingModifier;
use crate::kernel::GkProgram;
use crate::node::PortType;

/// One entry in a program's output manifest: typed, modifier-
/// aware view of a single output name.
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    pub name: String,
    pub port_type: PortType,
    pub modifier: BindingModifier,
}

/// Extract the output manifest from a compiled GK program.
/// Returns one entry per output, in declaration order.
pub fn extract_manifest(program: &GkProgram) -> Vec<ManifestEntry> {
    (0..program.output_count())
        .map(|i| {
            let name = program.output_name(i).to_string();
            let (ni, pi) = program.resolve_output_by_index(i);
            let port_type = program.node_meta(ni).outs[pi].typ;
            let modifier = program.output_modifier(&name);
            ManifestEntry { name, port_type, modifier }
        })
        .collect()
}
