// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-80b §"Lock in the convention" — CI gate.
//!
//! Asserts that **hand-written `impl PolydatNode for X` blocks**
//! only appear in the explicit by-design carve-out files. Every
//! workload-callable library node must route through
//! `#[polydat_node]`. The carve-out covers compiler-synthesised
//! infrastructure (programmatic-API only, fold-pass synthesised,
//! cursor-compiler synthesised) and Rust-internal composition
//! primitives that have no DSL surface.
//!
//! Adding a hand-written `impl PolydatNode for X` to any other
//! `polydat/src/library/**` file fails this test. The escape
//! hatch is to either (a) migrate to `#[polydat_node]`, or (b)
//! add an entry to `CARVEOUT_FILES` below with a doc-style
//! comment justifying the architectural exception.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Files allowed to contain hand-written `impl PolydatNode for`
/// blocks. Each entry must have a justifying rationale recorded
/// at the top of the file's module doc (see e.g.
/// `identity.rs`'s module comment for the canonical pattern).
const CARVEOUT_FILES: &[&str] = &[
    // Compiler-synthesised infrastructure: PortPassthrough
    // (extern-port nodes), ConstHandle / ConstExt (fold-pass
    // synthesised from runtime Handle/Ext values).
    "src/library/identity.rs",
    // Compiler-synthesised: AssertType / AssertValue (assembled
    // by `assert_type_node` / `assert_value_node`, runtime
    // PortType / ConstConstraint dispatched).
    "src/library/assertions.rs",
    // Cursor-compiler synthesised: CursorLimit (built by the
    // cursor materialiser; not workload-callable).
    "src/library/context.rs",
    // Rust-internal composition primitive: LutSample backs the
    // `dist_*` family; no DSL surface (the `dist_*` functions
    // are the workload-callable wrappers).
    "src/library/sampling/lut.rs",
    // Compiler-synthesised: RegView (free-bitcast retag adapter,
    // auto-inserted by `auto_adapter` for reg→reg wires; runtime
    // PortType dispatched like AssertType — not workload-callable).
    "src/library/register.rs",
];

#[test]
fn srd80b_no_handwritten_polydat_node_impl_outside_carveouts() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let library_root = crate_root.join("src/library");
    assert!(
        library_root.exists(),
        "expected polydat/src/library to exist at {}",
        library_root.display(),
    );

    let mut offending: Vec<String> = Vec::new();
    let carveout: HashSet<PathBuf> = CARVEOUT_FILES.iter()
        .map(|p| crate_root.join(p))
        .collect();

    visit_rust_files(&library_root, &mut |path| {
        if carveout.contains(path) {
            return;
        }
        let body = match fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => return,
        };
        for (lineno, line) in body.lines().enumerate() {
            let trimmed = line.trim_start();
            // Skip macro-rules expansions (the line starts with
            // whitespace + `impl PolydatNode for $name` — the
            // `$` sigil never appears in a real impl).
            if trimmed.contains('$') {
                continue;
            }
            // Match only top-level impls (no leading indent).
            if line.starts_with("impl PolydatNode for")
                || line.starts_with("impl<") && line.contains("PolydatNode for")
            {
                let rel = path.strip_prefix(crate_root).unwrap_or(path);
                offending.push(format!(
                    "{}:{}: hand-written `impl PolydatNode for`",
                    rel.display(),
                    lineno + 1,
                ));
            }
        }
    });

    assert!(
        offending.is_empty(),
        "SRD-80b invariant violated — hand-written `impl PolydatNode for X` \
         blocks must use the `#[polydat_node]` macro, OR the file must be \
         added to CARVEOUT_FILES in `polydat/tests/srd80b_invariant.rs` with \
         a justifying rationale.\n\nOffending sites:\n  {}\n",
        offending.join("\n  ")
    );
}

fn visit_rust_files(dir: &Path, visit: &mut dyn FnMut(&Path)) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            visit_rust_files(&path, visit);
        } else if path.extension().is_some_and(|e| e == "rs") {
            visit(&path);
        }
    }
}
