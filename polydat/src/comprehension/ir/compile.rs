// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! AST → IR compiler — spec §9.1.
//!
//! Bottom-up tree walk: each AST node emits its children's IR
//! sequences in left-to-right order, then its own operator(s).
//! `cartesian` / `zip` / `union` use N-arity opcodes; `filter`
//! / `order` use unary wrappers. The terminal `Dispense` is
//! appended at the end.
//!
//! R1 and R2 (the metadata-driven catalog entries from spec
//! §10.2) are realized here: `order(Lex, _)` compiles to
//! `Op::OrderStreaming` (R1); `order(non-Lex, Some(n))` over
//! an index-addressable input compiles to
//! `Op::OrderMaterialize { indexed: true }` (R2); the naïve
//! path uses `indexed: false`.

use crate::comprehension::ast::Comprehension;
use crate::comprehension::strategy::StrategyName;

use super::op::{Op, OrderStreamingKind};
use super::program::Program;

/// Compile an optimized AST to a `Program`. The result is
/// ready for execution by [`super::interpreter::interpret`].
///
/// Per spec §9.4 + §10.6 the input AST should have already
/// been validated (§5) and optimized (§10). Compiling an
/// un-optimized AST is well-defined but may produce
/// catastrophic working sets (spec §10's motivating example).
pub fn compile(ast: &Comprehension) -> Program {
    let mut ops = Vec::new();
    emit(ast, &mut ops);
    ops.push(Op::Dispense);
    Program::new(ops)
}

/// Recursive emit walker.
fn emit(ast: &Comprehension, ops: &mut Vec<Op>) {
    match ast {
        Comprehension::Clause { name, source } => {
            ops.push(Op::PushClause {
                name: name.clone(),
                source: source.clone(),
            });
        }
        Comprehension::Cartesian { children } => {
            for child in children {
                emit(child, ops);
            }
            ops.push(Op::Cartesian { n: children.len() });
        }
        Comprehension::Zip { children, mode } => {
            for child in children {
                emit(child, ops);
            }
            ops.push(Op::Zip {
                n: children.len(),
                mode: *mode,
            });
        }
        Comprehension::Union { children } => {
            for child in children {
                emit(child, ops);
            }
            ops.push(Op::Union { n: children.len() });
        }
        Comprehension::Filter { child, predicate } => {
            emit(child, ops);
            ops.push(Op::Filter {
                predicate: predicate.clone(),
            });
        }
        Comprehension::Order { child, strategy, truncation } => {
            emit(child, ops);
            ops.push(order_op(child, *strategy, *truncation));
        }
    }
}

/// Choose between `OrderStreaming` (R1: Lex) and
/// `OrderMaterialize` (R2: non-Lex with indexed push-down
/// when the input's metadata is index-addressable).
fn order_op(child: &Comprehension, strategy: StrategyName, truncation: Option<u64>) -> Op {
    if matches!(strategy, StrategyName::Lex) {
        return Op::OrderStreaming {
            kind: OrderStreamingKind::Lex,
            truncation,
        };
    }
    // R2: check whether the input is index-addressable. The
    // metadata propagator (spec §10.7) is the authority.
    let metadata = child.metadata();
    let indexed = metadata.index_addressable.is_some();
    Op::OrderMaterialize {
        strategy,
        truncation,
        indexed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::source::{LiteralValue, Source};
    use crate::comprehension::strategy::ZipMode;

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn compile_single_clause_emits_3_opcodes() {
        let ast = clause("k", &[1, 2, 3]);
        let prog = compile(&ast);
        assert_eq!(prog.len(), 2);
        // [PUSH_CLAUSE, DISPENSE]
        assert!(matches!(prog.ops()[0], Op::PushClause { .. }));
        assert!(matches!(prog.ops()[1], Op::Dispense));
    }

    #[test]
    fn compile_cartesian_emits_children_then_combinator() {
        let ast = Comprehension::cartesian(vec![clause("a", &[1, 2]), clause("b", &[10, 20])]);
        let prog = compile(&ast);
        // [PUSH a, PUSH b, CARTESIAN(2), DISPENSE]
        assert_eq!(prog.len(), 4);
        match &prog.ops()[2] {
            Op::Cartesian { n: 2 } => {}
            other => panic!("expected Cartesian(2), got {other:?}"),
        }
    }

    #[test]
    fn compile_order_lex_emits_streaming() {
        let inner = clause("k", &[1, 2, 3]);
        let ast = Comprehension::order(inner, StrategyName::Lex, Some(2));
        let prog = compile(&ast);
        // [PUSH, ORDER_STREAMING(Lex, Some(2)), DISPENSE]
        assert!(matches!(
            prog.ops()[1],
            Op::OrderStreaming {
                kind: OrderStreamingKind::Lex,
                truncation: Some(2)
            }
        ));
    }

    #[test]
    fn compile_order_halton_emits_materialize_indexed() {
        // halton over a cartesian (index-addressable) → R2 fires.
        let cart = Comprehension::cartesian(vec![clause("a", &[1, 2, 3]), clause("b", &[10, 20])]);
        let ast = Comprehension::order(cart, StrategyName::Halton, Some(3));
        let prog = compile(&ast);
        let last_non_dispense = &prog.ops()[prog.len() - 2];
        match last_non_dispense {
            Op::OrderMaterialize {
                strategy: StrategyName::Halton,
                truncation: Some(3),
                indexed: true,
            } => {}
            other => panic!("expected OrderMaterialize indexed, got {other:?}"),
        }
    }

    #[test]
    fn compile_order_halton_over_filter_is_naive() {
        // Filter destroys index addressability → R2 does NOT
        // fire → indexed = false.
        let cart = Comprehension::cartesian(vec![clause("a", &[1, 2, 3]), clause("b", &[10, 20])]);
        let filtered = Comprehension::filter(cart, "{a} > 0");
        let ast = Comprehension::order(filtered, StrategyName::Halton, Some(2));
        let prog = compile(&ast);
        let order_op = prog
            .ops()
            .iter()
            .find(|op| matches!(op, Op::OrderMaterialize { .. }))
            .unwrap();
        assert!(matches!(
            order_op,
            Op::OrderMaterialize { indexed: false, .. }
        ));
    }

    #[test]
    fn compile_zip_emits_zip_with_mode() {
        let ast = Comprehension::zip(
            vec![clause("x", &[1, 2, 3]), clause("y", &[10, 20, 30])],
            ZipMode::Strict,
        );
        let prog = compile(&ast);
        assert!(matches!(
            prog.ops()[2],
            Op::Zip {
                n: 2,
                mode: ZipMode::Strict
            }
        ));
    }

    #[test]
    fn compile_terminates_with_dispense() {
        let ast = clause("k", &[1]);
        let prog = compile(&ast);
        assert!(matches!(prog.ops().last(), Some(Op::Dispense)));
    }
}
