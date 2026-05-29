# Comprehension Migration Regression Gate

The load-bearing acceptance gate for PR 9c (the cutover from
legacy `polydat::comprehension::{ast, parse, eval, synthesis,
order, iteration}` to the operator-tree algebra now living at
`polydat::comprehension::*`).

**Status:** PR 9c-1a through 9c-5 shipped (synthesis
dissolved, iteration / order deleted, algebra promoted to
the canonical comprehension namespace). The 93-baseline
regression gate stayed green at every push; this doc is kept
for archaeology and as the template for any future
similarly-shaped cutover.

**Goal:** Every comprehension-dependent behavior preserved
across the cutover. This gate is what made the cutover safe
to land in pieces.

## Baseline state — 2026-05-28

The following test suites exercise the comprehension surface
end-to-end. All passing under the legacy code path:

| Crate | Test file | Tests | Run as |
|---|---|---|---|
| `nbrs` | `tests/comprehension.rs` | 27 | `cargo test --test comprehension -p nbrs` |
| `nbrs-workload` | `tests/for_each_forms.rs` | 13 | `cargo test --test for_each_forms -p nbrs-workload` |
| `nbrs` | `tests/workload_examples.rs` | 41 | `cargo test --test workload_examples -p nbrs` |
| `nbrs` | `tests/scope.rs` | 9 | `cargo test --test scope -p nbrs` |
| `nbrs-activity` | `tests/srd71_scope_tree_probe.rs` | 3 | `cargo test --test srd71_scope_tree_probe -p nbrs-activity` |

**Baseline total: 93 tests passing.** Plus the polydat-internal
algebra suite (1262 lib + 73 integration) which is invariant
across the cutover (the algebra is what the cutover migrates
*toward*, not *away from*).

## Workload corpus

The cutover preserves behavior on the 16 of 43 workload
fixtures in `examples/workloads/` that use comprehension forms
(`for:`, `for_each`, `for_combinations`, `for_each_union`):

```
$ grep -l 'for_each\|for_combinations\|for_each_union\|for:' examples/workloads/*.yaml | wc -l
16
```

`nbrs/tests/workload_examples.rs` walks the full workload set
including these 16; any behavioral regression on a
comprehension workload surfaces as a test failure there.

## Gate definition

The cutover (PR 9c) is **accepted** when:

1. **All 93 baseline tests pass** after the legacy code is
   removed and consumers are migrated to the algebra. Same
   counts, same names, no regressions.
2. **The polydat lib suite still passes** (1262 tests; verifies
   the algebra survived the consumer-side rewrite unchanged).
3. **The polydat integration suite still passes** (73 tests).

The cutover is **rejected** when:

- Any baseline test fails or hangs.
- Any baseline test name changes (renaming masks regressions).
- The polydat suites regress (indicates the cutover touched
  algebra-internal code paths).

## Why the existing test suite is sufficient

The 93 baseline tests collectively cover:

- **Parser shapes** — `for_each_forms.rs` exercises every one
  of the 6 YAML surface forms (single-var, multi-var inline /
  array, parallel-iter, union via repeated names, etc.).
- **Workload execution** — `workload_examples.rs` runs the full
  43-workload corpus through the executor, including the 16
  comprehension workloads.
- **Scope tree integration** — `scope.rs` and
  `srd71_scope_tree_probe.rs` exercise the scope-tree's
  consumption of comprehensions.
- **End-to-end semantics** — `comprehension.rs` tests
  dispense order, tuple shape, filter behavior, ordering
  strategies as authored.

A targeted dispense-capture tool (the original PR 9a sketch
in the implementation plan) would catch a narrower class of
bugs — specifically, dispense-order changes that happen to
satisfy the assertions in the existing tests. The risk of
that class is low because:

- The algebra preserves dispense order by construction (spec
  §9.2 correctness contract; verified on ~2000 random ASTs
  in PR 8's equivalence harness).
- Any order change at the comprehension layer would cascade
  to checkpoint resume tests (`checkpoint_resume_staircase.rs`)
  which assert exact tuple sequences.

If the cutover team wants extra confidence, the optional
follow-up is: capture per-workload dispense fingerprints
(e.g., hash of the bindings sequence) and assert
post-cutover hashes match. This is a one-off tooling effort
that can land separately if pressure arises.

## Capturing the baseline

The baseline is captured implicitly by the test suites
themselves — each test bakes in expected output. No separate
"snapshot" infrastructure is needed; the assertions in the
tests are the snapshot.

To re-verify the baseline at any time:

```bash
cargo test --test comprehension -p nbrs && \
cargo test --test for_each_forms -p nbrs-workload && \
cargo test --test workload_examples -p nbrs && \
cargo test --test scope -p nbrs && \
cargo test --test srd71_scope_tree_probe -p nbrs-activity
```

Total runtime: ~10 seconds on a modern dev machine.

## Cutover sequence reminder

This gate is consumed by PR 9c. The PR 9 plan is:

- **PR 9a** (this doc) — gate definition + baseline confirmed.
- **PR 9b** — `KernelScope for GkKernel` implementation +
  algebra-side adapter layer that bridges nb-rs's existing
  use sites to the new types. Code lands but isn't activated;
  the legacy path remains the default.
- **PR 9c** — atomic cutover. Consumers switch to the algebra
  path; legacy code paths deleted. Gate must pass.

PR 9a is documentation + baseline; PR 9b is the bulk of the
adapter / glue work; PR 9c is the deletion + cutover.
