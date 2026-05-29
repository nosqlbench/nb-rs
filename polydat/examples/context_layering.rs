// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Illustration: a library kernel can be composed into a parent
//! kernel as a sub-DAG, with each invocation getting its own
//! context (its own inputs, its own output bindings).
//!
//! The library file `library_kernel.gk` declares a single function
//! with a typed signature `(entity: u64) -> (entity_h, entity_code,
//! entity_bucket)`. The outer kernel here calls it TWICE in the same
//! scope — once for `tenant` and once for `device` — and each call
//! site is a separate context. Same definition, two layered uses.
//!
//! The same machinery extends to deeper nesting: nbrs's scope-tree
//! executor uses `polydat::kernel::subcontext` to layer entire scope-trees
//! over parent kernels, with typed import/export contracts at every
//! boundary. The illustration here is the polydat-direct form
//! (single-file kernel composition); the heavier `SubcontextBuilder`
//! / `ScopeKernel` machinery is what nbrs uses for the multi-scope
//! case.

fn main() {
    let lib_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("stdlib").join("identity.gk");

    let mut kernel = polydat::dsl::compile_gk_with_libs(
        r#"
            input cycle: u64
            (tenant, device) := mixed_radix(cycle, 100, 0)

            // `hashed_id` is a stdlib library function loaded from
            // identity.gk. The two calls below layer the SAME compiled
            // function over two different contexts: each call site
            // gets its own input (tenant vs device) and emits an
            // independent sub-DAG into the parent graph.
            tenant_id := hashed_id(tenant, 10000)
            device_id := hashed_id(device, 10000)
        "#,
        None,
        vec![lib_path.clone()],
        &[],
        false,
        "context_layering example",
    ).expect("compile failed");

    println!("library function loaded from: {}", lib_path.display());
    println!();
    println!("cycle  tenant  tenant_id    device  device_id");
    println!("-----  ------  ---------    ------  ---------");
    for cycle in [0u64, 1, 99, 100, 234] {
        kernel.set_inputs(&[cycle]);
        let t = kernel.pull("tenant").as_u64();
        let tid = kernel.pull("tenant_id").as_u64();
        let d = kernel.pull("device").as_u64();
        let did = kernel.pull("device_id").as_u64();
        println!("{cycle:>5}  {t:>6}  {tid:>9}    {d:>6}  {did:>9}");
    }
}
