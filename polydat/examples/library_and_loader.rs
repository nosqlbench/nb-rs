// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Illustration: polydat is a function library AND a function loader.
//!
//! The 230 built-in nodes are one library. Workload-author functions
//! written in `.gk` files are another — `compile_gk_with_libs` loads
//! them from disk and they're callable from your DSL by name as if
//! they were built in.
//!
//! This example loads the shipped `stdlib/identity.gk` module and
//! calls its `hashed_id` function from the workload-author DSL above.

fn main() {
    // Path to the stdlib module shipped with the crate.
    let stdlib = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("stdlib").join("identity.gk");

    let mut kernel = polydat::dsl::compile_gk_with_libs(
        r#"
            input cycle: u64
            // Calls `hashed_id` from identity.gk — not a built-in,
            // loaded from disk just above.
            uid := hashed_id(cycle, 1000000)
        "#,
        None,                 // source_dir — None: no implicit ./*.gk lookup
        vec![stdlib.clone()], // explicit library paths
        &[],                  // required_outputs — empty: compile everything
        false,                // strict
        "library_and_loader example",
    ).expect("compile failed");

    println!("library loaded from: {}", stdlib.display());
    for c in 0..5u64 {
        kernel.set_inputs(&[c]);
        let uid = kernel.pull("uid").as_u64();
        println!("  cycle={c} → uid={uid}");
        assert!(uid < 1_000_000);
    }
}
