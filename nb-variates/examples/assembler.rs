// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Programmatic kernel construction via the assembler API.

use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::nodes::hash::Hash64;
use nb_variates::nodes::arithmetic::{ModU64, MixedRadix};

fn main() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    // Decompose cycle into (region, device, reading) coordinates
    asm.add_node("decompose", Box::new(MixedRadix::new(vec![100, 1000, 0])),
        vec![WireRef::input("cycle")]);

    // Hash region for a deterministic region code
    asm.add_node("region_h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);
    asm.add_node("region_code", Box::new(ModU64::new(10000)),
        vec![WireRef::node("region_h")]);

    // Declare outputs
    asm.add_output("region", WireRef::node_port("decompose", 0));
    asm.add_output("device", WireRef::node_port("decompose", 1));
    asm.add_output("reading", WireRef::node_port("decompose", 2));
    asm.add_output("region_code", WireRef::node("region_code"));

    let mut kernel = asm.compile().expect("assembly failed");

    println!("cycle     region  device  reading  region_code");
    println!("--------  ------  ------  -------  -----------");
    for cycle in [0, 1, 50, 100, 10000, 100000] {
        kernel.set_inputs(&[cycle]);
        println!("{cycle:>8}  {:>6}  {:>6}  {:>7}  {:>11}",
            kernel.pull("region").as_u64(),
            kernel.pull("device").as_u64(),
            kernel.pull("reading").as_u64(),
            kernel.pull("region_code").as_u64(),
        );
    }
}
