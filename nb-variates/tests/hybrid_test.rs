// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Hybrid kernel tests: mixed JIT + closure nodes in the same graph.

use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::nodes::arithmetic::*;
use nb_variates::nodes::hash::Hash64;
use nb_variates::nodes::identity::Identity;

#[test]
fn hybrid_simple_identity() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("id", Box::new(Identity::new()), vec![WireRef::coord("cycle")]);
    asm.add_output("out", WireRef::node("id"));

    let mut kernel = asm.compile_hybrid().unwrap();
    kernel.eval(&[42]);
    assert_eq!(kernel.get("out"), 42);
}

#[test]
fn hybrid_hash_mod_chain() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
    asm.add_node("m", Box::new(ModU64::new(1000)), vec![WireRef::node("h")]);
    asm.add_output("out", WireRef::node("m"));

    let mut kernel = asm.compile_hybrid().unwrap();
    kernel.eval(&[42]);
    let v = kernel.get("out");
    assert!(v < 1000, "got {v}");
}

#[test]
fn hybrid_mixed_jit_and_closure() {
    // MixedRadix can't be JIT-compiled (fallback), but Hash and Mod can.
    // The hybrid should handle this: MixedRadix as closure, rest as JIT.
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    // MixedRadix: not JIT-able → closure
    asm.add_node("decompose", Box::new(MixedRadix::new(vec![100, 0])),
        vec![WireRef::coord("cycle")]);

    // Hash: JIT-able
    asm.add_node("h", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);

    // Mod: JIT-able
    asm.add_node("code", Box::new(ModU64::new(10000)),
        vec![WireRef::node("h")]);

    asm.add_output("tenant", WireRef::node_port("decompose", 0));
    asm.add_output("code", WireRef::node("code"));

    let mut kernel = asm.compile_hybrid().unwrap();

    kernel.eval(&[4242]);
    let tenant = kernel.get("tenant");
    let code = kernel.get("code");
    assert_eq!(tenant, 42); // 4242 % 100
    assert!(code < 10000, "code={code}");
}

#[test]
fn hybrid_deterministic() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
    asm.add_node("m", Box::new(ModU64::new(1000000)), vec![WireRef::node("h")]);
    asm.add_output("out", WireRef::node("m"));

    let mut kernel = asm.compile_hybrid().unwrap();

    kernel.eval(&[42]);
    let v1 = kernel.get("out");
    kernel.eval(&[42]);
    let v2 = kernel.get("out");
    assert_eq!(v1, v2);
}

#[test]
fn hybrid_multi_output() {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);

    // MixedRadix (closure) → two outputs → each hashed (JIT) → modded (JIT)
    asm.add_node("decompose", Box::new(MixedRadix::new(vec![100, 1000, 0])),
        vec![WireRef::coord("cycle")]);
    asm.add_node("h0", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 0)]);
    asm.add_node("h1", Box::new(Hash64::new()),
        vec![WireRef::node_port("decompose", 1)]);
    asm.add_node("code0", Box::new(ModU64::new(10000)),
        vec![WireRef::node("h0")]);
    asm.add_node("code1", Box::new(ModU64::new(100000)),
        vec![WireRef::node("h1")]);

    asm.add_output("c0", WireRef::node("code0"));
    asm.add_output("c1", WireRef::node("code1"));

    let mut kernel = asm.compile_hybrid().unwrap();

    kernel.eval(&[4_201_337]);
    assert!(kernel.get("c0") < 10000);
    assert!(kernel.get("c1") < 100000);
}

#[test]
fn hybrid_interleave_plus_hash() {
    // Interleave is not JIT-able, but Hash and Mod are
    let mut asm = GkAssembler::new(vec!["a".into(), "b".into()]);
    asm.add_node("mixed", Box::new(Interleave::new()),
        vec![WireRef::coord("a"), WireRef::coord("b")]);
    asm.add_node("h", Box::new(Hash64::new()),
        vec![WireRef::node("mixed")]);
    asm.add_node("result", Box::new(ModU64::new(1000)),
        vec![WireRef::node("h")]);
    asm.add_output("out", WireRef::node("result"));

    let mut kernel = asm.compile_hybrid().unwrap();
    kernel.eval(&[5, 10]);
    let v1 = kernel.get("out");
    assert!(v1 < 1000);

    kernel.eval(&[10, 5]);
    let v2 = kernel.get("out");
    assert!(v2 < 1000);
    assert_ne!(v1, v2, "interleave should make (5,10) != (10,5)");
}

#[test]
fn hybrid_long_chain() {
    // add → add → add → hash → mod — mix of JIT-able nodes
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("a1", Box::new(AddU64::new(1)), vec![WireRef::coord("cycle")]);
    asm.add_node("a2", Box::new(AddU64::new(2)), vec![WireRef::node("a1")]);
    asm.add_node("a3", Box::new(AddU64::new(3)), vec![WireRef::node("a2")]);
    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::node("a3")]);
    asm.add_node("m", Box::new(ModU64::new(100)), vec![WireRef::node("h")]);
    asm.add_output("out", WireRef::node("m"));

    let mut kernel = asm.compile_hybrid().unwrap();
    kernel.eval(&[0]);
    // 0 + 1 + 2 + 3 = 6, hash(6) % 100
    let v = kernel.get("out");
    assert!(v < 100);
}
