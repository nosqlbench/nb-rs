// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Illustration: the `:=` lines in GK are full expressions, not just
//! direct node calls.
//!
//! You can nest, chain, and combine without naming every intermediate
//! wire — useful for one-line derivations where naming the
//! intermediate would just be noise.

fn main() {
    let mut kernel = polydat::dsl::compile_gk(r#"
        input cycle: u64

        // Nested expressions — no named intermediates needed.
        user_id := mod(hash(cycle), 1000000)

        // Multi-arg, mixed-type composition. `number_to_words` takes
        // a u64 and returns a Str; `str_upper` returns a Str.
        word := str_upper(number_to_words(mod(cycle, 10)))

        // Inline conditional via select_u64(cond, then, else). The
        // condition is a u64-as-bool (0/1) value produced by the
        // comparison op — polydat's comparison family returns u64,
        // not Bool, by convention.
        bucket := select_u64(u64_lt(user_id, 500000), 0, 1)
    "#).expect("compile failed");

    println!("cycle  user_id  word     bucket");
    println!("-----  -------  -------  ------");
    for cycle in 0..5u64 {
        kernel.set_inputs(&[cycle]);
        let uid = kernel.pull("user_id").as_u64();
        let word = kernel.pull("word").as_str().to_string();
        let bucket = kernel.pull("bucket").as_u64();
        println!("{cycle:>5}  {uid:>7}  {word:<7}  {bucket:>6}");
    }
}
