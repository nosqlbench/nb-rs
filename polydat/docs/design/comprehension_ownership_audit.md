# Comprehension Ownership Audit

**Status:** AUDIT EXECUTED 2026-05-28. All five phases (A
through E) landed in the same session. The polydat
comprehension spec at
`polydat/docs/design/comprehension_forms.md` is now the
single authoritative reference for comprehension semantics
across the SRD corpus. This audit doc remains as the
historical record of the extrication; reading order is the
polydat spec first, this audit second only if you need to
understand why a particular non-polydat SRD has the polydat
cross-references it does.

**Execution summary:**
- **Phase A** (polydat self-update): canonical declaration in
  preamble, §15 restructured into 5 sub-sections.
- **Phase B** (5 high-impact SRDs): SRD-18e retired to redirect
  stub; SRD-18b tightened (scenario-tree integration); SRD-18c
  reframed as parser-layer (with continuous-source grammar
  extension push); SRD-18d reframed as algorithmic detail
  (Custom removed, Shuffle noted as follow-up); SRD-78 tightened
  with §9.5 consumption-surfaces wiring + audit alignment note.
- **Phase C** (5 medium + internals): SRD-18 reframes
  `for_each` shapes as desugar-to-polydat; SRD-71 cross-
  references polydat for the iteration semantics (the surface
  itself is SRD-71-owned); SRD-13f/67/74 internals references
  replaced with polydat-public-API references.
- **Phase D** (passing-mention sweep): SRD-02, 11, 13e, 17,
  40b, 44, 68 each gain a one-line polydat-spec cross-
  reference on first comprehension mention.
- **Phase E** (verification): grep confirms every SRD touching
  comprehension material has at least one polydat-spec
  cross-reference; no polydat-internal symbols redefined
  outside the polydat spec; SRD-18e live cross-references
  converted to polydat-spec targets where appropriate, with
  the only remaining SRD-18e narrative refs being in SRD-78's
  cross-refs block explaining the stub nature.

**Original audit content follows below** for historical
reference; treat as a snapshot, not a live work list.

---



**Goal:** Establish polydat as the single authoritative owner of
the comprehension algebra and its semantics. Other nb-rs SRDs
retain their right to *describe how they integrate with*
comprehensions, but lose any authority to *define* comprehension
semantics. Where authoritative material currently lives in a
non-polydat SRD, this audit identifies the migration target
(usually a section in `polydat/docs/design/comprehension_forms.md`,
hereafter "the polydat spec").

**Companion to:** `polydat/docs/design/comprehension_forms.md`
— the authoritative comprehension reference. This audit is the
execution plan for making the canonical/derivative split explicit
across the SRD corpus.

---

## 1. Canonical declaration (proposed)

After this audit executes, the following statement is law:

> The **polydat comprehension spec** (`polydat/docs/design/comprehension_forms.md`)
> is the single authoritative reference for the comprehension
> algebra — its constructors, validity axioms, metadata
> propagation rules, optimizer rewrites, IR opcodes, and
> consumption surfaces. Any non-polydat SRD discussing
> comprehensions does so **strictly to describe how that
> SRD's subject integrates with polydat-owned comprehensions**;
> it does not redefine, extend, or shadow polydat-owned
> material. Apparent contradictions between a non-polydat SRD
> and the polydat spec resolve in favor of the polydat spec.

This declaration belongs in the polydat spec's preamble and in
§15 ("Relationship to the SRDs"), and as a one-line cross-
reference from each non-polydat SRD that discusses comprehensions.

---

## 2. Ownership postures

Every comprehension-touching passage in the SRD corpus falls into
one of four ownership postures:

- **Authoritative duplicator** — passage defines comprehension
  semantics or extends the algebra independently of polydat.
  *Action:* rewrite as a reference to the polydat spec section
  that now owns the material; remove the duplicated definition.
- **Integration** — passage describes how some non-polydat
  subsystem consumes, dispatches, or interacts with polydat
  comprehensions. *Action:* keep, tighten wording to make the
  polydat-ownership boundary explicit, add a cross-reference
  to the polydat spec.
- **Internals reference** — passage names a polydat-crate-
  internal path (`polydat/src/comprehension/...`) or symbol.
  *Action:* replace with a reference to the polydat public API
  (or to the polydat spec section that documents the surface);
  internals references should not appear in nb-rs SRDs.
- **Passing mention** — passage names "comprehension" as a
  background concept without making any claim about its
  semantics. *Action:* keep as-is or lightly link to the
  polydat spec.

---

## 3. Per-SRD audit

Eighteen SRDs touch comprehension material. The audit covers all
of them, grouped by posture-mix.

### 3.1 Heavy authoritative duplicators

These SRDs currently carry substantial definitional material
that needs to migrate or be reduced to references.

#### SRD-18b — Scenario Tree and Scheduler (58 hits)

**Posture:** Mixed authoritative / integration.

**Authoritative passages** (to migrate):
- `ScenarioNode::Comprehension { comprehension, children }`
  definition (§"The Comprehension model"). The variant lives
  in the scenario-tree owned by nb-rs's scenario layer, so the
  *variant* stays here; but its `comprehension: polydat::comprehension::Comprehension`
  field references the polydat type, and the inline description
  of `ComprehensionMode::Cartesian(clauses) with one clause is
  ...` etc. is duplicating polydat's §3 constructor semantics.
- The scenario-tree's claim that "comprehensions enumerate
  distinct tuples" is a polydat-owned semantic.

**Integration passages** (to keep, tighten):
- How scenario-tree nodes wrap and dispatch comprehensions.
- find-by-comprehension lookup semantics specific to the
  scenario tree.

**Action:**
1. Replace inline `ComprehensionMode::Cartesian / Union /
   for_each / for_combinations / for_each_union` semantics
   descriptions with single-line references: *"semantics per
   polydat spec §3 constructors and §8 syntactic surface."*
2. Keep the wrapper-variant definition (`ScenarioNode::Comprehension`
   is a scenario-tree concept) but make its field type
   explicit: `comprehension: polydat::comprehension::Comprehension`.
3. Add a preamble cross-reference: *"This SRD describes how
   scenario-tree consumes polydat comprehensions; comprehension
   semantics are owned by the polydat spec."*

#### SRD-18c — Comprehension Syntax

**Posture:** Currently authoritative on surface grammar.

**Status under the new ownership boundary:** The polydat spec's
§8 ("The syntactic surface") owns the desugaring rules. SRD-18c
still has a role *if* it owns the **parser layer** that converts
text into the polydat AST — i.e., it covers the layered grammar
(literal lists, ranges, generators, SI suffixes, tuple LHS,
sequencer expansions) that polydat's §3.1 `source` parameter
consumes.

**Action:**
1. Reframe SRD-18c as "the parser-side surface that produces
   polydat comprehension ASTs," not as the comprehension
   semantics owner.
2. Where SRD-18c describes what `for k in 1..10` *means* (as
   distinct from what it *parses to*), replace with a
   reference to polydat §8.1's desugaring table.
3. Add the continuous-source grammar extension push noted in
   the polydat spec's §15 — SRD-18c needs to declare how
   `0.0..2π` parses, what distribution-object source forms look
   like, etc.
4. Add preamble cross-reference: *"This SRD owns the parser-
   layer grammar; comprehension semantics (constructors,
   validity, optimization, IR) are polydat-owned."*

#### SRD-18d — Comprehension Traversal Order

**Posture:** Currently authoritative on strategy taxonomy.

**Status under the new ownership boundary:** The polydat spec's
§3.6 owns the strategy taxonomy with its per-strategy input-
`IndexFn` requirements. SRD-18d's role narrows to **per-strategy
algorithmic detail** (Halton base-2 sequence, Sobol direction
numbers, etc.) — implementation-level material the polydat spec
references but doesn't reproduce.

**Action:**
1. Drop any general "what is a traversal order" framing from
   SRD-18d; the polydat spec owns that.
2. Keep per-strategy mathematical detail (the actual Halton
   recurrence, the Sobol construction, etc.).
3. Update SRD-18d to point at polydat §3.6's per-strategy
   input-shape table as the authoritative compositional
   reference; SRD-18d's per-strategy detail is the
   "implementation-level" companion.
4. Drop any `Custom(fn)` references — polydat §3.6 has
   removed the user-callback escape hatch.
5. Add Shuffle to the per-strategy detail (polydat §3.6 added
   it as part of F1/F4 resolution).

#### SRD-18e — Comprehension Canonical Reference

**Posture:** Currently authoritative; explicitly named as "the
contract" in the index.

**Status under the new ownership boundary:** **Retired.** The
polydat spec is now the canonical reference. SRD-18e's
predecessor AST (`Comprehension { mode, filter, order }` flat
struct) is the thing the polydat spec's operator-tree replaces.

**Action:**
1. Replace SRD-18e's body with a stub: *"This SRD has been
   superseded by the polydat comprehension spec
   (`polydat/docs/design/comprehension_forms.md`). The
   index-space ordering contract (V4 + V5 in the polydat spec)
   and the Layer 7 extension path are the migration targets
   for SRD-18e's authoritative material. This stub remains as
   a redirect for existing cross-references."*
2. Update every cross-reference *to* SRD-18e elsewhere to
   point to the polydat spec directly.

#### SRD-78 — PolyStreamer (79 hits)

**Posture:** Mixed authoritative / integration.

**Authoritative passages** (to migrate):
- Any claim about comprehension semantics, validity, or
  algebra — should reference polydat.
- The "lock-free shared cursor" model is SRD-78's concern, but
  its interaction with the IR (§9.1) and the consumption
  surfaces (§9.5 — `CoordinateStream` / `ScopedKernelStream` /
  `scope_once`) is polydat-defined.

**Integration passages** (to keep, tighten):
- Streamer instantiation lifecycle.
- Thread-safety, cursor concurrency, lock-free semantics.
- SRD-78's "one streamer per Arc, each with its own dispense
  cursor" model — applies *per streamer instance* across the
  two surfaces polydat §9.5 defines.
- Unbounded-variant queue (SRD-78 §"Unbounded sources") — the
  runtime mechanism for accepting `Unbounded` cardinality
  streamers.

**Action:**
1. Rewrite SRD-78's intro to make the polydat-ownership
   boundary explicit: SRD-78 implements the runtime that hosts
   polydat-owned comprehension types.
2. Replace any inline comprehension semantics with references
   to the polydat spec.
3. Explicitly add the §9.5 consumption-surfaces split to
   SRD-78 — two concrete streamer types (`CoordinateStream`
   and `ScopedKernelStream<K>`) plus the one-shot `scope_once`
   function, all over the shared compiled IR. SRD-78 owns the
   *runtime types*; polydat owns the *contracts they implement*.

### 3.2 Medium references (mixed integration + drift)

#### SRD-18 — Control Flow (14 hits)

**Posture:** Authoritative on user-facing control-flow shapes.

**Authoritative passages:** Definitions of `ForCombinations`,
`ForEachUnion`, and the `for_each` family in the workload-
authoring sense. These are user-facing control-flow constructs
that *desugar to* polydat comprehensions.

**Action:**
1. Reframe each control-flow shape as "the workload-side
   construct that desugars to [polydat constructor]." For
   example, `ForCombinations` desugars to a polydat `cartesian`
   per §3.2 + §8.1.
2. Drop any inline description of what `cartesian` *means* —
   that's polydat-owned.
3. Add a one-line cross-reference at the top of each control-
   flow shape's section.

#### SRD-71 — Cursor Partitions (11 hits)

**Posture:** Workload-side wrapper over standard polydat
comprehensions. **No polydat-algebra extension required.**

**Resolution (2026-05-28):** Reading SRD-71's §"Comprehension
syntax for partition iteration" (lines 231–290), the pattern
`for: "p in cursor.partitions"` is a standard polydat
`clause(p, cursor.partitions)` per §3.1 and §8.1. The
`cursor.partitions` projection wire is a polydat-external value
(a list of `(idx, start_pct, end_pct, start_ord, end_ord)`
tuples produced by SRD-71's cursor-management layer); once
surfaced as a list value, polydat's existing list-source clause
handles iteration with no special protocol. The novel surface
is the cursor-declaration `over <iter-var>` clause and the
`<param>.partitions` projection wire — both are SRD-71-owned
and operate outside the polydat algebra.

**Action:**
1. Keep SRD-71's content as-is.
2. Add a one-line cross-reference at SRD-71's §"Comprehension
   syntax for partition iteration": *"The `for:` clause is
   standard polydat clause-over-list semantics per
   [polydat spec §3.1 + §8.1]; SRD-71 owns only the
   `cursor.partitions` projection and the `over <iter-var>`
   cursor-binding clause."*
3. No deferral entry needed in polydat §14.

### 3.3 Internals references (path-level coupling)

#### SRD-13f — Cross-Scope Wire Materialization (6 hits)

**Internals reference:**
`polydat/src/comprehension.rs::synthesize_for_each_scope`.

**Action:**
1. Replace the internals reference with the polydat-public-API
   form: the spec section that documents the relevant public
   surface (likely §9.5 consumption surfaces or §10 optimizer).
2. If no public-API surface covers the use, that's a polydat
   API gap to fix — surface the gap as a finding in polydat
   §16.

#### SRD-67 — GK Subcontext Construction (7 hits)

**Internals reference:** "comprehension synthesiser" mentioned
multiple times.

**Action:** Same as SRD-13f. Replace internal-symbol
references with public-API or polydat-spec references; surface
any API gaps.

#### SRD-74 — None Propagation (2 hits)

**Internals reference:** `polydat/src/comprehension/synthesis.rs`
test file path.

**Action:** Remove the internals reference; replace with a
description of the test's intent (the polydat-public-API
behavior being verified) or drop the test reference entirely
(SRD-74 isn't the polydat test corpus's catalog).

### 3.4 Passing mentions (lightest touch)

These SRDs name "comprehension" as a background concept without
making semantic claims. Action: keep as-is, optionally add a
one-line link to the polydat spec on first mention per SRD.

- **SRD-00 — Index** (5 hits): table-of-contents entries for
  18b/18c/18d/18e. Update entries to reflect post-audit reality
  (18e becomes a redirect stub; 18c/18d are scoped to
  parser/algorithm detail).
- **SRD-02 — Concurrency Model** (2 hits): mentions
  "comprehension iter-steps" alongside other iteration sources.
  Passing.
- **SRD-11 — GK Evaluation** (1 hit): mentions "enclosing
  comprehension advancing" in the scope-init pull context.
  Passing.
- **SRD-13e — Scope-as-Module** (3 hits): mentions
  comprehension scopes in the scope-model context. Passing,
  but should clarify that ComprehensionModule's content is
  polydat-defined.
- **SRD-17 — Diagnostic Modes** (2 hits): mentions
  comprehension iteration logging. Passing.
- **SRD-40b — Synthetic Metrics from GK** (1 hit): example
  syntax `comprehension_var in ...`. Passing.
- **SRD-44 — Workload Checkpointing** (1 hit): "comprehensions
  enumerate distinct tuples" — borderline authoritative claim,
  but it's true per polydat semantics, so just add the
  cross-reference.
- **SRD-68 — Dispenser-Owned GK Context** (1 hit): mentions
  `for_each` comprehensions positionally. Passing.

---

## 4. Execution plan

The audit's recommended execution order, optimized for
minimizing cross-doc churn:

### Phase A — Polydat spec self-update (no external touches)

1. Add the canonical declaration (§1 of this audit) to the
   polydat spec's preamble.
2. Expand polydat spec §15 with explicit per-SRD cross-
   references reflecting the post-audit ownership boundary.

### Phase B — High-impact SRD rewrites (sequenced)

3. **SRD-18e — retire** to redirect stub. Highest payoff per
   line changed; clears the "two canonical references" problem.
4. **SRD-18b — tighten**. Replace inline comprehension
   semantics with polydat references; keep scenario-tree
   wrapper as integration.
5. **SRD-18c — reframe** as parser-layer; add continuous-source
   grammar extension push.
6. **SRD-18d — reframe** as per-strategy algorithmic detail;
   point to polydat §3.6 for compositional behavior; drop
   Custom; add Shuffle.
7. **SRD-78 — tighten**. Make polydat-ownership boundary
   explicit; add §9.5 consumption-surfaces wiring.

### Phase C — Medium / internals (parallel to Phase B)

8. **SRD-18 — reframe** control-flow shapes as desugaring to
   polydat constructors.
9. **SRD-71 — resolve** the protocol-extension question.
10. **SRD-13f, SRD-67, SRD-74 — replace internals references**
    with polydat-API or polydat-spec references. Surface any
    polydat API gaps that result.

### Phase D — Passing mentions (sweep)

11. Add one-line polydat-spec cross-references in SRD-02,
    SRD-11, SRD-13e, SRD-17, SRD-40b, SRD-44, SRD-68.
12. Update SRD-00 index entries to reflect post-audit structure.

### Phase E — Verification

13. Re-grep the SRD corpus for comprehension-related terms;
    every passage should either be in the polydat spec, be an
    integration description, or be a single-line link.
14. Property-check: no two SRDs make different claims about
    the same comprehension behavior.

---

## 5. Open questions for execution

- **SRD-18e stub vs deletion.** A stub preserves cross-references
  for tooling and historical context; deletion is cleaner but
  breaks links. Recommend stub.
- **SRD-18d retention scope.** If SRD-18d's per-strategy
  algorithmic detail is short enough, it could be absorbed into
  polydat as a §3.6 appendix or a separate `polydat/docs/strategies.md`.
  Recommend keeping SRD-18d as long as it carries non-trivial
  mathematical content; reconsider only if the post-trim version
  is <50 lines.
- **SRD-71 protocol-extension ambiguity.** RESOLVED 2026-05-28:
  Interpretation (b) — workload-side wrapper, no polydat
  algebra extension. See §3.2's SRD-71 entry for the action.
- **Polydat API gaps surfaced by SRD-13f / SRD-67 / SRD-74.**
  If those SRDs reference internals because no public API
  exists, the API gap is a polydat-internal finding (likely
  belongs in polydat §16 or as a polydat crate issue).

---

## 6. Out of scope

- Code-side refactoring (moving symbols, renaming, etc.). This
  audit is doc-only; code follows once docs settle.
- Re-implementation of any comprehension semantics. The polydat
  spec is already the canonical reference for the algebra;
  this audit just makes that consistent across the doc corpus.
- Comprehension grammar extensions (continuous-source syntax in
  SRD-18c, partition-protocol extensions in SRD-71). These are
  noted as follow-ups but are separate work.
