// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Basic usage: compile a GK kernel from DSL source and pull values.

fn main() {
    let mut kernel = nb_variates::dsl::compile_gk(r#"
        coordinates := (cycle)
        hashed := hash(cycle)
        user_id := mod(hashed, 1000000)
        bucket := mod(hashed, 64)
    "#).expect("compile failed");

    println!("cycle  user_id  bucket");
    println!("-----  -------  ------");
    for cycle in 0..10 {
        kernel.set_inputs(&[cycle]);
        let uid = kernel.pull("user_id").as_u64();
        let bucket = kernel.pull("bucket").as_u64();
        println!("{cycle:>5}  {uid:>7}  {bucket:>6}");
    }
}
