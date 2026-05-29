// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! §9.5.2 independence contract tests.
//!
//! Verifies that the three consumption surfaces
//! (`CoordinateStream`, `ScopedKernelStream<K>`, `scope_once`)
//! maintain independent dispense state per streamer instance.
//! Multiple streamers from the same `CompiledComprehension`
//! share the underlying IR but advance independently.

use std::sync::Arc;
use std::thread;

use polydat::comprehension::ast::Comprehension;
use polydat::comprehension::source::{LiteralValue, Source};
use polydat::comprehension::strategies::{Tuple, TupleValue};
use polydat::comprehension::surfaces::{
    compile, scope_once, CompiledComprehension, KernelScope,
};

#[derive(Debug, Clone)]
struct MockKernel(String);

impl KernelScope for MockKernel {
    type Scoped = (String, Vec<(String, TupleValue)>);
    fn scope(&self, coords: &Tuple) -> Self::Scoped {
        (self.0.clone(), coords.bindings.clone())
    }
}

fn clause(name: &str, vs: &[i64]) -> Comprehension {
    Comprehension::clause(
        name,
        Source::Literal {
            values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
        },
    )
}

#[test]
fn two_coordinate_streams_advance_independently() {
    let compiled = compile(&clause("k", &[1, 2, 3, 4, 5]));

    let mut a = compiled.coordinate_stream();
    let mut b = compiled.coordinate_stream();

    // Pull 2 from a — b is untouched.
    a.advance();
    a.advance();

    // b still produces the full sequence from the start.
    let b_tuples: Vec<Tuple> = b.by_ref().collect();
    assert_eq!(b_tuples.len(), 5);
    assert_eq!(b_tuples[0].bindings[0].1, TupleValue::I64(1));

    // a continues from where it left off.
    let a_remaining: Vec<Tuple> = a.collect();
    assert_eq!(a_remaining.len(), 3);
    assert_eq!(a_remaining[0].bindings[0].1, TupleValue::I64(3));
}

#[test]
fn cross_surface_independence_coord_then_scoped() {
    let compiled = compile(&clause("k", &[1, 2, 3]));
    let parent = MockKernel("p".into());

    let mut coord = compiled.coordinate_stream();
    let mut scoped = compiled.scoped_kernel_stream(parent);

    // Drain coord fully — scoped untouched.
    let coord_tuples: Vec<Tuple> = coord.by_ref().collect();
    assert_eq!(coord_tuples.len(), 3);

    // Scoped still produces full sequence.
    let scoped_instances: Vec<_> = scoped.by_ref().collect();
    assert_eq!(scoped_instances.len(), 3);
}

#[test]
fn cross_surface_independence_scoped_then_coord() {
    let compiled = compile(&clause("k", &[10, 20, 30]));
    let parent = MockKernel("p".into());

    let mut scoped = compiled.scoped_kernel_stream(parent);
    let mut coord = compiled.coordinate_stream();

    // Drain scoped first.
    let scoped_first: Vec<_> = scoped.by_ref().collect();
    assert_eq!(scoped_first.len(), 3);

    // Coord still produces full sequence.
    let coord_after: Vec<Tuple> = coord.by_ref().collect();
    assert_eq!(coord_after.len(), 3);
    assert_eq!(coord_after[0].bindings[0].1, TupleValue::I64(10));
}

#[test]
fn scope_once_consistency_with_scoped_stream() {
    let compiled = compile(&clause("k", &[7, 14, 21]));
    let parent = MockKernel("p".into());

    let stream_instances: Vec<_> = compiled.scoped_kernel_stream(parent.clone()).collect();
    let coord_tuples: Vec<Tuple> = compiled.coordinate_stream().collect();

    assert_eq!(stream_instances.len(), coord_tuples.len());
    for (stream_inst, tuple) in stream_instances.iter().zip(coord_tuples.iter()) {
        let one_shot = scope_once(&parent, tuple);
        assert_eq!(stream_inst.coords, one_shot.coords);
        assert_eq!(stream_inst.scoped, one_shot.scoped);
    }
}

#[test]
fn ir_sharing_across_many_streamers() {
    let compiled = compile(&Comprehension::cartesian(vec![
        clause("a", &[1, 2, 3]),
        clause("b", &[10, 20]),
    ]));

    // Instantiate 100 coordinate streams; they all share IR
    // (no recompilation).
    let streams: Vec<_> = (0..100).map(|_| compiled.coordinate_stream()).collect();

    // Each should produce 6 tuples (3 * 2 cartesian).
    for stream in streams {
        let tuples: Vec<Tuple> = stream.collect();
        assert_eq!(tuples.len(), 6);
    }
}

#[test]
fn concurrent_pulls_no_data_races() {
    // §9.5.2 promises independent dispense state. Each
    // streamer's cursor lives in its own thread; the shared
    // `Arc<Program>` is read-only. Confirms no shared mutable
    // state.
    let compiled = compile(&Comprehension::cartesian(vec![
        clause("a", &[1, 2, 3]),
        clause("b", &[10, 20]),
    ]));
    let compiled = Arc::new(compiled);

    let mut handles = Vec::new();
    for _ in 0..8 {
        let cc: Arc<CompiledComprehension> = Arc::clone(&compiled);
        let h = thread::spawn(move || {
            let stream = cc.coordinate_stream();
            let tuples: Vec<Tuple> = stream.collect();
            assert_eq!(tuples.len(), 6);
            tuples
        });
        handles.push(h);
    }

    // All threads should produce the same sequence (same IR,
    // same per-streamer behavior).
    let mut reference: Option<Vec<Tuple>> = None;
    for h in handles {
        let result = h.join().unwrap();
        match &reference {
            None => reference = Some(result),
            Some(ref_seq) => assert_eq!(*ref_seq, result),
        }
    }
}

#[test]
fn many_streamers_all_produce_identical_sequences() {
    // §9.5.2: "two CoordinateStream instances from the same
    // comprehension produce identical sequences but advance
    // independently."
    let compiled = compile(&Comprehension::cartesian(vec![
        clause("a", &[1, 2]),
        clause("b", &[10, 20]),
        clause("c", &[100]),
    ]));

    let s1: Vec<Tuple> = compiled.coordinate_stream().collect();
    let s2: Vec<Tuple> = compiled.coordinate_stream().collect();
    let s3: Vec<Tuple> = compiled.coordinate_stream().collect();
    assert_eq!(s1, s2);
    assert_eq!(s2, s3);
}

#[test]
fn scope_once_does_not_advance_any_cursor() {
    let compiled = compile(&clause("k", &[1, 2, 3]));
    let parent = MockKernel("p".into());
    let mut stream = compiled.coordinate_stream();

    let manual_coords = Tuple::new().with("k", TupleValue::I64(999));
    let _ = scope_once(&parent, &manual_coords);
    let _ = compiled.scope_once(&parent, &manual_coords);

    // Stream cursor untouched — should still produce all 3.
    let tuples: Vec<Tuple> = stream.by_ref().collect();
    assert_eq!(tuples.len(), 3);
}
