// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CI gates for the slot-state axioms (S1–S10) —
//! `polydat/docs/design/jit_boundary.md` §"Slot-state axioms".
//!
//! - **S2** — pointer containment: raw u64 readers refuse
//!   Ref2-colored slots; the typed borrow-checked accessors are
//!   the sanctioned path.
//! - **S5** — skip coherence: the Raw (never-skip) engine is the
//!   oracle; the Push / Pull / PushPull (skip) engines and the
//!   hybrid kernel must agree with it for arbitrary input-change
//!   sequences, including repeats that exercise the clean-skip
//!   paths over Ref state.
//! - **S9(a)** runs implicitly throughout: every eval in these
//!   debug-built tests executes the ref validator.
//! - **S10** — the `from_raw_parts` tripwire: Ref-deref unsafe
//!   stays inside the enumerated allowlist.

use polydat::dsl::compile::compile_polydat_to_assembler;

/// Mixed scalar / register / slice flow — every slot color in one
/// kernel, with a Ref-colored named output (`vsum`) to exercise
/// the typed accessors and a scalar output (`out`) for the raw
/// readers.
const MIXED_SRC: &str = r#"
    input cycle: u64
    a := hash_vec(cycle, 19)
    b := hash_vec(hash(cycle), 19)
    vsum := vec_add(a, b)
    r := reg_splat_i16(cycle)
    r2 := reg_add_i16(r, r)
    lane := reg_lane_i16(r2, 3)
    out := vec_dot(vsum, b) * unit_interval(hash(cycle))
"#;

/// Deterministic input sequence with repeats (repeats are what
/// drive the skip paths the S5 oracle exists to check).
fn input_seq(n: usize) -> Vec<u64> {
    let mut seq = Vec::with_capacity(n);
    let mut last = 7u64;
    for i in 0..n {
        let h = xxhash_rust::xxh3::xxh3_64(&(i as u64).to_le_bytes());
        if h % 3 == 0 {
            seq.push(last); // repeat → skip machinery engages
        } else {
            last = h;
            seq.push(h);
        }
    }
    seq
}

/// S5 — every skip engine must match the Raw oracle on both the
/// scalar output (raw reader) and the Ref output (typed
/// accessor), across a repeat-laden input sequence.
#[test]
fn s5_skip_engines_match_raw_oracle() {
    let mk = || compile_polydat_to_assembler(MIXED_SRC).unwrap();

    let mut raw = mk().try_compile_raw().expect("raw P2");
    let mut push = mk().try_compile_push().expect("push P2");
    let mut pull = mk().try_compile_pull().expect("pull P2");
    let mut pushpull = mk().try_compile().expect("pushpull P2");
    let mut hybrid = mk().compile_hybrid().expect("hybrid");

    let out = raw.resolve_output("out").unwrap();
    let vsum = raw.resolve_output("vsum").unwrap();

    for (i, &cycle) in input_seq(120).iter().enumerate() {
        raw.eval(&[cycle]);
        push.eval(&[cycle]);
        pull.eval(&[cycle]);
        pushpull.eval(&[cycle]);
        hybrid.eval(&[cycle]);

        let want_out = raw.get_slot(out);
        let want_vsum: Vec<f32> = raw.read_vec_f32(vsum).to_vec();

        assert_eq!(push.get_slot(out), want_out, "push scalar @ {i}");
        assert_eq!(pull.get_slot(out), want_out, "pull scalar @ {i}");
        assert_eq!(pushpull.get_slot(out), want_out, "pushpull scalar @ {i}");
        assert_eq!(hybrid.get_slot(out), want_out, "hybrid scalar @ {i}");

        assert_eq!(push.read_vec_f32(vsum), &want_vsum[..], "push ref @ {i}");
        assert_eq!(pull.read_vec_f32(vsum), &want_vsum[..], "pull ref @ {i}");
        assert_eq!(pushpull.read_vec_f32(vsum), &want_vsum[..], "pushpull ref @ {i}");
        assert_eq!(hybrid.read_vec_f32(vsum), &want_vsum[..], "hybrid ref @ {i}");
    }
}

/// S2 — the raw u64 readers refuse Ref2-colored slots (they would
/// leak an interior address); the typed accessor is the sanctioned
/// path and returns the actual contents.
#[test]
fn s2_raw_readers_refuse_ref_slots() {
    let mut p2 = compile_polydat_to_assembler(MIXED_SRC)
        .unwrap()
        .try_compile_raw()
        .expect("P2");
    p2.eval(&[42]);
    let vsum = p2.resolve_output("vsum").unwrap();

    // Typed accessor works and sees real data.
    assert_eq!(p2.read_vec_f32(vsum).len(), 19);

    // Raw readers panic, citing S2.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = p2.get_slot(vsum);
    }));
    assert!(r.is_err(), "get_slot must refuse a Ref2-colored slot");

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = p2.get("vsum");
    }));
    assert!(r.is_err(), "get must refuse a Ref2-colored output");

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = p2.eval_for_slot(&[43], vsum);
    }));
    assert!(r.is_err(), "eval_for_slot must refuse a Ref2-colored slot");
}

/// S2 — wrong-typed accessor reads fail loudly instead of
/// reinterpreting scratch.
#[test]
fn s2_typed_accessor_rejects_wrong_lane_type() {
    let mut p2 = compile_polydat_to_assembler(MIXED_SRC)
        .unwrap()
        .try_compile_raw()
        .expect("P2");
    p2.eval(&[42]);
    let vsum = p2.resolve_output("vsum").unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = p2.read_vec_i32(vsum);
    }));
    assert!(r.is_err(), "f32 scratch must not read as i32");
}

/// S10 — every `from_raw_parts` in the workspace's polydat crates
/// sits in the enumerated allowlist. Anything else is a new
/// Ref-deref unsafe site that must be brought under the axioms
/// (SAFETY comment citing S3/S4) and added here deliberately.
// Miri runs with FS isolation; the tripwire is a source scan, not
// an aliasing concern — native test runs cover it.
#[cfg_attr(miri, ignore)]
#[test]
fn s10_from_raw_parts_tripwire() {
    // (file, why it's allowed)
    let allow: &[(&str, &str)] = &[
        // SliceArc's own borrow projection — predates the axioms,
        // governed by SliceArc's owner-lifetime contract.
        ("src/ast.rs", "SliceArc as_slice"),
        // JIT extern helpers reading node-owned const tables baked
        // at compile time (retain_nodes keeps them alive).
        ("src/compile/jit/codegen.rs", "extern const-table reads"),
        // Dataset accessor reading an mmap-backed uniform facet
        // (vectordata owner-lifetime contract).
        ("src/library/vectors.rs", "dataset facet view"),
    ];
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offending = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read_dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).expect("read");
                if text.contains("from_raw_parts") {
                    let rel = path
                        .strip_prefix(root.parent().unwrap())
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/");
                    if !allow.iter().any(|(f, _)| rel == *f) {
                        offending.push(rel);
                    }
                }
            }
        }
    }
    assert!(
        offending.is_empty(),
        "S10 tripwire: from_raw_parts outside the allowlist (bring the \
         site under the slot-state axioms and add it here deliberately):\n{}",
        offending.join("\n"),
    );
}


/// Registry `lookup` returns a real `&'static` into the link-time
/// inventory — same address across calls — proving it does not
/// allocate (and leak) per call. Regression guard for the Miri
/// leak finding (2026-06-12): the former impl `Box::leak`'d a
/// clone, leaking ~200 bytes every call.
#[test]
fn lookup_does_not_allocate_per_call() {
    use polydat::dsl::registry::lookup;
    let a = lookup("hash").expect("hash registered") as *const _;
    let b = lookup("hash").expect("hash registered") as *const _;
    assert_eq!(a, b, "lookup must return a stable &'static, not a fresh leak");
}
