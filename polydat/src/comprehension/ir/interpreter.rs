// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Stack-machine IR interpreter — spec §9.1 option (a) +
//! §9.2 correctness contract.
//!
//! Walks an IR `Program` linearly, maintaining a stack of
//! tuple-stream operands. Each opcode either pushes a new
//! stream ([`Op::PushClause`]) or combines / wraps the top-N
//! ([`Op::Cartesian`], `Zip`, `Union`, `Filter`,
//! `OrderStreaming`, `OrderMaterialize`). `Dispense` marks
//! the top stream as the output.
//!
//! Returns a [`TupleStream`] — a lazy producer the consumer
//! pulls from. The stream graph is built at
//! [`interpret`]-time; tuple production happens on-demand.

use crate::comprehension::source::{LiteralValue, Source};
use crate::comprehension::strategies::{Tuple, TupleValue};
use crate::comprehension::strategy::{StrategyName, ZipMode};

use super::op::{Op, OrderStreamingKind};
use super::program::Program;

/// A lazy tuple stream — `advance` returns the next tuple or
/// `None` when the stream is exhausted.
pub trait TupleStream {
    fn advance(&mut self) -> Option<Tuple>;
}

/// Boxed stream alias used throughout the interpreter's stack
/// manipulation.
type BoxedStream = Box<dyn TupleStream>;

/// Interpret a compiled `Program` and return the result
/// stream. Per spec §9.1 the final opcode must be
/// `Op::Dispense`; if it's missing the function panics
/// (programs not produced by the compiler are caller-error).
pub fn interpret(program: &Program) -> BoxedStream {
    let mut stack: Vec<BoxedStream> = Vec::new();
    for op in program.ops() {
        match op {
            Op::PushClause { name, source } => {
                stack.push(Box::new(ClauseStream::new(name.clone(), source.clone())));
            }
            Op::Cartesian { n } => {
                let children = pop_n(&mut stack, *n);
                stack.push(Box::new(CartesianStream::new(children)));
            }
            Op::Zip { n, mode } => {
                let children = pop_n(&mut stack, *n);
                stack.push(Box::new(ZipStream::new(children, *mode)));
            }
            Op::Union { n } => {
                let children = pop_n(&mut stack, *n);
                stack.push(Box::new(UnionStream::new(children)));
            }
            Op::Filter { predicate } => {
                let inner = stack.pop().expect("Filter on empty stack");
                stack.push(Box::new(FilterStream::new(inner, predicate.clone())));
            }
            Op::OrderStreaming { kind, truncation } => {
                let inner = stack.pop().expect("OrderStreaming on empty stack");
                stack.push(Box::new(OrderStreamingStream::new(inner, *kind, *truncation)));
            }
            Op::OrderMaterialize { strategy, truncation, indexed } => {
                let inner = stack.pop().expect("OrderMaterialize on empty stack");
                stack.push(Box::new(OrderMaterializeStream::new(
                    inner, *strategy, *truncation, *indexed,
                )));
            }
            Op::Dispense => {
                // No-op at the interpreter level; the top of
                // stack is the result.
            }
        }
    }
    stack.pop().expect("Program produced no result stream")
}

fn pop_n(stack: &mut Vec<BoxedStream>, n: usize) -> Vec<BoxedStream> {
    assert!(stack.len() >= n, "stack underflow: needed {n}, have {}", stack.len());
    let split_at = stack.len() - n;
    stack.split_off(split_at)
}

// ---- ClauseStream ----

/// Streams a Source's values as single-name tuples.
/// Continuous / Distribution sources are not interpretable
/// at this layer (they require a sampling order to be
/// meaningful per V8); attempting to advance one returns
/// `None` so the interpreter doesn't deadlock.
struct ClauseStream {
    name: String,
    state: ClauseState,
}

enum ClauseState {
    LiteralList { values: Vec<LiteralValue>, pos: usize },
    IntRange { current: i64, hi: i64, step: i64 },
    Exhausted,
}

impl ClauseStream {
    fn new(name: String, source: Source) -> Self {
        let state = match source {
            Source::Literal { values } => ClauseState::LiteralList { values, pos: 0 },
            Source::IntRange { lo, hi, step } => ClauseState::IntRange {
                current: lo,
                hi,
                step: step.max(1),
            },
            // Generator / WorkloadParamList: produce nothing at
            // this layer (would need runtime evaluator wiring).
            // ContinuousInterval / Distribution: must be sampled
            // via an enclosing order; bare clause is not pulled
            // in valid programs.
            _ => ClauseState::Exhausted,
        };
        Self { name, state }
    }
}

impl TupleStream for ClauseStream {
    fn advance(&mut self) -> Option<Tuple> {
        let value = match &mut self.state {
            ClauseState::LiteralList { values, pos } => {
                if *pos >= values.len() {
                    return None;
                }
                let v = literal_to_tuple_value(&values[*pos]);
                *pos += 1;
                v
            }
            ClauseState::IntRange { current, hi, step } => {
                if *current >= *hi {
                    return None;
                }
                let v = TupleValue::I64(*current);
                *current += *step;
                v
            }
            ClauseState::Exhausted => return None,
        };
        Some(Tuple::new().with(self.name.clone(), value))
    }
}

fn literal_to_tuple_value(lv: &LiteralValue) -> TupleValue {
    match lv {
        LiteralValue::Int(n) => TupleValue::I64(*n),
        LiteralValue::Float(f) => TupleValue::F64(*f),
        LiteralValue::String(s) => TupleValue::Str(s.clone()),
        LiteralValue::Bool(b) => TupleValue::Bool(*b),
    }
}

// ---- CartesianStream ----

/// Enumerates the cross product of N child streams in Lex
/// order (rightmost varies fastest). Builds the per-axis
/// value vectors lazily on demand: the first advance pulls
/// child 0 once and child 1..N to exhaustion, caching them;
/// subsequent advances iterate over the cached cross product.
///
/// This caches all but the first axis. A fully-streaming
/// cartesian (no caching) would require child re-iteration,
/// which the IR layer doesn't currently expose.
struct CartesianStream {
    children: Vec<BoxedStream>,
    /// Cached values for axes 1..N (axis 0 streams).
    cached: Vec<Vec<Tuple>>,
    /// Current cursor for axis 0 (lazy pull).
    current_a0: Option<Tuple>,
    /// Cursor positions for axes 1..N.
    cursors: Vec<usize>,
    /// True once we've initialized — first advance() needs to
    /// cache children 1..N and pull initial child 0.
    initialized: bool,
    done: bool,
}

impl CartesianStream {
    fn new(children: Vec<BoxedStream>) -> Self {
        let n = children.len();
        Self {
            children,
            cached: Vec::with_capacity(n.saturating_sub(1)),
            current_a0: None,
            cursors: vec![0; n.saturating_sub(1)],
            initialized: false,
            done: false,
        }
    }

    fn initialize(&mut self) {
        if self.children.is_empty() {
            self.done = true;
            return;
        }
        // Cache all children 1..N to exhaustion.
        for i in 1..self.children.len() {
            let mut v = Vec::new();
            while let Some(t) = self.children[i].advance() {
                v.push(t);
            }
            self.cached.push(v);
        }
        // Pull first axis 0 value.
        self.current_a0 = self.children[0].advance();
        if self.current_a0.is_none() || self.cached.iter().any(|v| v.is_empty()) {
            // Any empty axis → empty cartesian.
            self.done = true;
        }
    }
}

impl TupleStream for CartesianStream {
    fn advance(&mut self) -> Option<Tuple> {
        if !self.initialized {
            self.initialize();
            self.initialized = true;
        }
        if self.done {
            return None;
        }
        // Compose current cursor + current axis-0 value.
        let mut out = Tuple::new();
        if let Some(a0) = self.current_a0.as_ref() {
            for (k, v) in &a0.bindings {
                out.bindings.push((k.clone(), v.clone()));
            }
        }
        for (i, cursor) in self.cursors.iter().enumerate() {
            let tup = &self.cached[i][*cursor];
            for (k, v) in &tup.bindings {
                out.bindings.push((k.clone(), v.clone()));
            }
        }

        // Advance cursors (rightmost-fastest).
        let n_cached = self.cursors.len();
        let mut overflow = true;
        for i in (0..n_cached).rev() {
            self.cursors[i] += 1;
            if self.cursors[i] < self.cached[i].len() {
                overflow = false;
                break;
            }
            self.cursors[i] = 0;
        }
        if overflow {
            // Advance axis 0.
            self.current_a0 = self.children[0].advance();
            if self.current_a0.is_none() {
                self.done = true;
            }
        }
        Some(out)
    }
}

// ---- ZipStream ----

struct ZipStream {
    children: Vec<BoxedStream>,
    mode: ZipMode,
    /// For Cycle: cached values of shorter children.
    cycle_cache: Option<Vec<Vec<Tuple>>>,
    cycle_cursors: Vec<usize>,
    cycle_longest_idx: Option<usize>,
    initialized: bool,
    done: bool,
}

impl ZipStream {
    fn new(children: Vec<BoxedStream>, mode: ZipMode) -> Self {
        Self {
            children,
            mode,
            cycle_cache: None,
            cycle_cursors: Vec::new(),
            cycle_longest_idx: None,
            initialized: false,
            done: false,
        }
    }

    fn initialize_cycle(&mut self) {
        // Pull every child to exhaustion (Cycle's barrier).
        // Identify the longest child; cache the others.
        let mut all: Vec<Vec<Tuple>> = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            let mut v = Vec::new();
            while let Some(t) = child.advance() {
                v.push(t);
            }
            all.push(v);
        }
        let longest_idx = all
            .iter()
            .enumerate()
            .max_by_key(|(_, v)| v.len())
            .map(|(i, _)| i)
            .unwrap_or(0);
        self.cycle_longest_idx = Some(longest_idx);
        self.cycle_cursors = vec![0; all.len()];
        self.cycle_cache = Some(all);
    }
}

impl TupleStream for ZipStream {
    fn advance(&mut self) -> Option<Tuple> {
        if self.done {
            return None;
        }
        match self.mode {
            ZipMode::Strict | ZipMode::Truncate => {
                // Pull one tuple from each child; if any returns
                // None, this stream is exhausted.
                let mut out = Tuple::new();
                for child in &mut self.children {
                    match child.advance() {
                        Some(t) => {
                            for (k, v) in t.bindings {
                                out.bindings.push((k, v));
                            }
                        }
                        None => {
                            self.done = true;
                            return None;
                        }
                    }
                }
                Some(out)
            }
            ZipMode::Cycle => {
                if !self.initialized {
                    self.initialize_cycle();
                    self.initialized = true;
                }
                let cache = self.cycle_cache.as_ref().unwrap();
                let longest = self.cycle_longest_idx.unwrap();
                if cache[longest].is_empty() {
                    self.done = true;
                    return None;
                }
                if self.cycle_cursors[longest] >= cache[longest].len() {
                    self.done = true;
                    return None;
                }
                let mut out = Tuple::new();
                for (i, v) in cache.iter().enumerate() {
                    if v.is_empty() {
                        // Empty child → empty zip.
                        self.done = true;
                        return None;
                    }
                    let idx = self.cycle_cursors[i] % v.len();
                    for (k, val) in &v[idx].bindings {
                        out.bindings.push((k.clone(), val.clone()));
                    }
                }
                // Advance cursors: longest by 1, others wrap.
                for c in self.cycle_cursors.iter_mut() {
                    *c += 1;
                }
                Some(out)
            }
        }
    }
}

// ---- UnionStream ----

/// Drain children in order: child 0 fully, then child 1, etc.
struct UnionStream {
    children: Vec<BoxedStream>,
    active_idx: usize,
}

impl UnionStream {
    fn new(children: Vec<BoxedStream>) -> Self {
        Self { children, active_idx: 0 }
    }
}

impl TupleStream for UnionStream {
    fn advance(&mut self) -> Option<Tuple> {
        loop {
            if self.active_idx >= self.children.len() {
                return None;
            }
            if let Some(t) = self.children[self.active_idx].advance() {
                return Some(t);
            }
            // Advance to next child.
            self.active_idx += 1;
        }
    }
}

// ---- FilterStream ----

struct FilterStream {
    inner: BoxedStream,
    predicate: String,
}

impl FilterStream {
    fn new(inner: BoxedStream, predicate: String) -> Self {
        Self { inner, predicate }
    }
}

impl TupleStream for FilterStream {
    fn advance(&mut self) -> Option<Tuple> {
        loop {
            let candidate = self.inner.advance()?;
            if evaluate_predicate(&self.predicate, &candidate) {
                return Some(candidate);
            }
        }
    }
}

// ---- OrderStreamingStream ----

/// `order(c, Lex, truncation)` — pass-through optionally
/// capped at truncation tuples.
struct OrderStreamingStream {
    inner: BoxedStream,
    truncation: Option<u64>,
    emitted: u64,
}

impl OrderStreamingStream {
    fn new(inner: BoxedStream, _kind: OrderStreamingKind, truncation: Option<u64>) -> Self {
        Self { inner, truncation, emitted: 0 }
    }
}

impl TupleStream for OrderStreamingStream {
    fn advance(&mut self) -> Option<Tuple> {
        if let Some(cap) = self.truncation
            && self.emitted >= cap
        {
            return None;
        }
        let t = self.inner.advance()?;
        self.emitted += 1;
        Some(t)
    }
}

// ---- OrderMaterializeStream ----

/// MATERIALIZATION BARRIER. On first advance, build the
/// working set per the strategy, then emit permuted tuples
/// (optionally truncated). For Phase 7 the naïve path
/// materializes the full input. The indexed path is a
/// follow-up that requires plumbing the IndexFn through the
/// IR (deferred; the optimizer's R2 marker is already
/// emitted by the compiler).
struct OrderMaterializeStream {
    inner: BoxedStream,
    strategy: StrategyName,
    truncation: Option<u64>,
    #[allow(dead_code)] // used by indexed path (follow-up)
    indexed: bool,
    materialized: Option<Vec<Tuple>>,
    pos: usize,
}

impl OrderMaterializeStream {
    fn new(
        inner: BoxedStream,
        strategy: StrategyName,
        truncation: Option<u64>,
        indexed: bool,
    ) -> Self {
        Self {
            inner,
            strategy,
            truncation,
            indexed,
            materialized: None,
            pos: 0,
        }
    }

    fn materialize(&mut self) {
        let mut buf = Vec::new();
        while let Some(t) = self.inner.advance() {
            buf.push(t);
        }
        let dispatched = crate::comprehension::strategies::for_name(self.strategy);
        let out = dispatched.naive_apply(buf, self.truncation);
        self.materialized = Some(out);
    }
}

impl TupleStream for OrderMaterializeStream {
    fn advance(&mut self) -> Option<Tuple> {
        if self.materialized.is_none() {
            self.materialize();
        }
        let buf = self.materialized.as_ref().unwrap();
        if self.pos >= buf.len() {
            return None;
        }
        let t = buf[self.pos].clone();
        self.pos += 1;
        Some(t)
    }
}

// ---- Predicate evaluator ----

/// Simple predicate evaluator covering the §10.9.5 catalog.
/// Returns `true` for unrecognized predicates (the
/// conservative choice: keep tuples we can't decide on; the
/// caller's algebra-level predicate analyzer marks unknown
/// patterns Opaque so the optimizer doesn't push them
/// down; the IR interpreter then runs them per-tuple here).
///
/// Implementations:
/// - `{name} OP literal` and `literal OP {name}` for the 6
///   comparison operators.
/// - `p && q`, `p || q`, `!p` (recursive).
/// - `{name} in [v1, v2, ...]` discrete-set membership.
/// - Literal `true` / `false`.
///
/// Anything else evaluates to `true` (passes through). A
/// production interpreter would wire polydat's GK expression
/// evaluator; this Phase 7 evaluator is sufficient for
/// algebra-layer tests and the §11 worked examples.
fn evaluate_predicate(predicate: &str, tuple: &Tuple) -> bool {
    let trimmed = predicate.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return true;
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return false;
    }
    // Negation.
    if let Some(inner) = trimmed.strip_prefix('!') {
        return !evaluate_predicate(inner.trim(), tuple);
    }
    // Conjunction.
    if let Some(parts) = split_top_level(trimmed, "&&") {
        return parts.iter().all(|p| evaluate_predicate(p, tuple));
    }
    // Disjunction.
    if let Some(parts) = split_top_level(trimmed, "||") {
        return parts.iter().any(|p| evaluate_predicate(p, tuple));
    }
    // `{name} in [v1, v2, ...]`
    if let Some(in_pos) = trimmed.find(" in ") {
        let lhs = trimmed[..in_pos].trim();
        let rhs = trimmed[in_pos + 4..].trim();
        if let Some(name) = strip_curly(lhs)
            && let Some(inner) = rhs.strip_prefix('[').and_then(|s| s.strip_suffix(']'))
        {
            let needle = lookup(tuple, &name);
            if needle.is_none() {
                return true; // Unknown coord — pass through.
            }
            return inner.split(',').any(|item| {
                parse_literal(item.trim())
                    .map(|v| values_eq(&lit_to_tuple_value(&v), needle.unwrap()))
                    .unwrap_or(false)
            });
        }
    }
    // Comparison ops: try longest first.
    for (op, op_kind) in [
        ("==", CmpKind::Eq),
        ("!=", CmpKind::Ne),
        ("<=", CmpKind::Le),
        (">=", CmpKind::Ge),
        ("<", CmpKind::Lt),
        (">", CmpKind::Gt),
    ] {
        if let Some((lhs, rhs)) = split_top_level_op(trimmed, op) {
            let lhs = lhs.trim();
            let rhs = rhs.trim();
            // {name} OP literal
            if let (Some(name), Some(lit)) = (strip_curly(lhs), parse_literal(rhs)) {
                let val = lookup(tuple, &name);
                if val.is_none() {
                    return true;
                }
                return compare(val.unwrap(), op_kind, &lit_to_tuple_value(&lit));
            }
            // literal OP {name}
            if let (Some(name), Some(lit)) = (strip_curly(rhs), parse_literal(lhs)) {
                let val = lookup(tuple, &name);
                if val.is_none() {
                    return true;
                }
                // Invert kind: a < b iff b > a.
                let inv = invert_kind(op_kind);
                return compare(val.unwrap(), inv, &lit_to_tuple_value(&lit));
            }
            // {a} OP {b}
            if let (Some(a), Some(b)) = (strip_curly(lhs), strip_curly(rhs)) {
                let va = lookup(tuple, &a);
                let vb = lookup(tuple, &b);
                if va.is_none() || vb.is_none() {
                    return true;
                }
                return compare(va.unwrap(), op_kind, vb.unwrap());
            }
        }
    }
    true
}

#[derive(Clone, Copy)]
enum CmpKind {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

fn invert_kind(k: CmpKind) -> CmpKind {
    match k {
        CmpKind::Lt => CmpKind::Gt,
        CmpKind::Le => CmpKind::Ge,
        CmpKind::Gt => CmpKind::Lt,
        CmpKind::Ge => CmpKind::Le,
        other => other,
    }
}

fn lookup<'a>(tuple: &'a Tuple, name: &str) -> Option<&'a TupleValue> {
    tuple
        .bindings
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
}

fn compare(a: &TupleValue, kind: CmpKind, b: &TupleValue) -> bool {
    let ord = match (a, b) {
        (TupleValue::I64(a), TupleValue::I64(b)) => a.cmp(b),
        (TupleValue::U64(a), TupleValue::U64(b)) => a.cmp(b),
        (TupleValue::F64(a), TupleValue::F64(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (TupleValue::I64(a), TupleValue::F64(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (TupleValue::F64(a), TupleValue::I64(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (TupleValue::Str(a), TupleValue::Str(b)) => a.cmp(b),
        (TupleValue::Bool(a), TupleValue::Bool(b)) => a.cmp(b),
        _ => return false,
    };
    match kind {
        CmpKind::Eq => ord.is_eq(),
        CmpKind::Ne => !ord.is_eq(),
        CmpKind::Lt => ord.is_lt(),
        CmpKind::Le => ord.is_le(),
        CmpKind::Gt => ord.is_gt(),
        CmpKind::Ge => ord.is_ge(),
    }
}

fn values_eq(a: &TupleValue, b: &TupleValue) -> bool {
    compare(a, CmpKind::Eq, b)
}

fn strip_curly(s: &str) -> Option<String> {
    let s = s.trim();
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        let trimmed = inner.trim();
        if trimmed.chars().all(|c| c.is_alphanumeric() || c == '_') && !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn parse_literal(s: &str) -> Option<LiteralValue> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("true") {
        return Some(LiteralValue::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Some(LiteralValue::Bool(false));
    }
    if s.len() >= 2
        && ((s.starts_with('"') && s.ends_with('"'))
            || (s.starts_with('\'') && s.ends_with('\'')))
    {
        return Some(LiteralValue::String(s[1..s.len() - 1].to_string()));
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(LiteralValue::Int(n));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Some(LiteralValue::Float(f));
    }
    None
}

fn lit_to_tuple_value(lv: &LiteralValue) -> TupleValue {
    literal_to_tuple_value(lv)
}

fn split_top_level(s: &str, sep: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut depth = 0i64;
    let mut last = 0usize;
    let bytes = s.as_bytes();
    let sep_bytes = sep.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && i + sep_bytes.len() <= bytes.len() && &bytes[i..i + sep_bytes.len()] == sep_bytes {
            parts.push(s[last..i].trim().to_string());
            last = i + sep_bytes.len();
            i = last;
            continue;
        }
        i += 1;
    }
    if parts.is_empty() {
        return None;
    }
    parts.push(s[last..].trim().to_string());
    Some(parts)
}

fn split_top_level_op<'a>(s: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut depth = 0i64;
    let bytes = s.as_bytes();
    let op_bytes = op.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && i + op_bytes.len() <= bytes.len() && &bytes[i..i + op_bytes.len()] == op_bytes {
            if op.len() == 1 {
                let next = bytes.get(i + 1).copied();
                if next == Some(b'=') {
                    i += 1;
                    continue;
                }
            }
            return Some((&s[..i], &s[i + op_bytes.len()..]));
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::ast::Comprehension;
    use crate::comprehension::ir::compile;
    use crate::comprehension::source::{LiteralValue, Source};

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    fn collect(stream: &mut BoxedStream) -> Vec<Tuple> {
        let mut out = Vec::new();
        while let Some(t) = stream.advance() {
            out.push(t);
        }
        out
    }

    #[test]
    fn single_clause_dispense() {
        let ast = clause("k", &[1, 2, 3]);
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].bindings[0].1, TupleValue::I64(1));
        assert_eq!(tuples[2].bindings[0].1, TupleValue::I64(3));
    }

    #[test]
    fn cartesian_2d_lex_order() {
        let ast = Comprehension::cartesian(vec![clause("a", &[1, 2]), clause("b", &[10, 20])]);
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 4);
        // Lex: (a=1, b=10), (a=1, b=20), (a=2, b=10), (a=2, b=20)
        assert_eq!(tuples[0].bindings[0].1, TupleValue::I64(1));
        assert_eq!(tuples[0].bindings[1].1, TupleValue::I64(10));
        assert_eq!(tuples[1].bindings[1].1, TupleValue::I64(20));
        assert_eq!(tuples[2].bindings[0].1, TupleValue::I64(2));
    }

    #[test]
    fn zip_strict_3() {
        let ast = Comprehension::zip(
            vec![clause("x", &[1, 2, 3]), clause("y", &[10, 20, 30])],
            ZipMode::Strict,
        );
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].bindings[0].1, TupleValue::I64(1));
        assert_eq!(tuples[0].bindings[1].1, TupleValue::I64(10));
        assert_eq!(tuples[2].bindings[1].1, TupleValue::I64(30));
    }

    #[test]
    fn zip_truncate_shortest() {
        let ast = Comprehension::zip(
            vec![clause("x", &[1, 2, 3, 4]), clause("y", &[10, 20])],
            ZipMode::Truncate,
        );
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 2);
    }

    #[test]
    fn union_drains_in_order() {
        let ast = Comprehension::union(vec![clause("k", &[1, 2]), clause("k", &[10, 20])]);
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 4);
        assert_eq!(tuples[0].bindings[0].1, TupleValue::I64(1));
        assert_eq!(tuples[2].bindings[0].1, TupleValue::I64(10));
    }

    #[test]
    fn filter_keeps_only_matching() {
        let cart = Comprehension::cartesian(vec![clause("k", &[1, 2, 3, 4, 5])]);
        let ast = Comprehension::filter(cart, "{k} > 2");
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 3);
        for t in &tuples {
            match t.bindings[0].1 {
                TupleValue::I64(n) => assert!(n > 2),
                _ => panic!(),
            }
        }
    }

    #[test]
    fn order_streaming_lex_truncates() {
        let ast = Comprehension::order(clause("k", &[1, 2, 3, 4, 5]), StrategyName::Lex, Some(2));
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0].bindings[0].1, TupleValue::I64(1));
        assert_eq!(tuples[1].bindings[0].1, TupleValue::I64(2));
    }

    #[test]
    fn order_materialize_shuffle_produces_full_set() {
        let ast = Comprehension::order(clause("k", &[1, 2, 3, 4, 5]), StrategyName::Shuffle, None);
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        assert_eq!(tuples.len(), 5);
        // All original values must be present (just permuted).
        let mut sorted: Vec<i64> = tuples
            .iter()
            .map(|t| match t.bindings[0].1 {
                TupleValue::I64(n) => n,
                _ => panic!(),
            })
            .collect();
        sorted.sort();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn dispense_sequence_for_section_11_1() {
        // Spec §11.1: cartesian over (k in 1..=2) × (b in 10..=20 step 10).
        let ast = Comprehension::cartesian(vec![clause("k", &[1, 2]), clause("b", &[10, 20])]);
        let prog = compile(&ast);
        let mut stream = interpret(&prog);
        let tuples = collect(&mut stream);
        // 2 × 2 = 4 tuples in Lex.
        assert_eq!(tuples.len(), 4);
    }
}
