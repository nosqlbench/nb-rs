# Memo 14: GK Type System and Operator Expansion

Design for multi-type arithmetic, bitwise operators, and
precision-safe numeric operations in the GK DSL.

---

## Current State

The GK has two numeric types: `u64` and `f64`. All values in
the DAG buffer are stored as `u64` (f64 values bit-packed via
`to_bits()`/`from_bits()`). Arithmetic operators added in this
session (`+`, `-`, `*`, `/`, `%`, `^`) all desugar to f64 node
calls.

### Problems

1. **`^` is ambiguous**: In Rust, `^` is XOR. We used it for
   power (Python/math convention). Need to pick one.

2. **No integer arithmetic via operators**: `cycle * 2` should
   be u64 multiplication, not f64.

3. **No bitwise operations**: `&`, `|`, `^`, `<<`, `>>`, `!`
   are essential for hash manipulation, bit extraction, flag
   construction.

4. **No narrower types**: f32, i32, i16, u8 etc. needed for
   database column matching and memory efficiency.

5. **No overflow/precision checking**: `u64::MAX + 1` silently
   wraps. `f64 → i32` silently truncates.

---

## Proposed: Follow Rust Syntax

### Infix Operators

| Operator | Meaning | Rust equivalent |
|----------|---------|-----------------|
| `+` | Addition | `+` |
| `-` | Subtraction | `-` |
| `*` | Multiplication | `*` |
| `/` | Division | `/` |
| `%` | Remainder/modulo | `%` |
| `**` | Power | `.pow()` / `.powf()` |
| `&` | Bitwise AND | `&` |
| `\|` | Bitwise OR | `\|` |
| `^` | Bitwise XOR | `^` |
| `<<` | Left shift | `<<` |
| `>>` | Right shift | `>>` |
| `!` | Bitwise NOT (unary) | `!` |

### Precedence (matching Rust)

| Level | Operators |
|-------|-----------|
| 1 (lowest) | `\|` |
| 2 | `^` |
| 3 | `&` |
| 4 | `<<` `>>` |
| 5 | `+` `-` |
| 6 | `*` `/` `%` |
| 7 | `**` (right-assoc) |
| 8 (highest) | `-` `!` (unary) |

### Breaking Change

`^` changes from power to XOR. Power becomes `**`:
- Before: `x ^ 0.5` (square root)
- After: `x ** 0.5` (square root), `x ^ mask` (XOR)

---

## Type System

### Numeric Types

| Type | Width | Range | Buffer Storage |
|------|-------|-------|----------------|
| `u8` | 8-bit | 0..255 | u64 (zero-extended) |
| `u16` | 16-bit | 0..65535 | u64 (zero-extended) |
| `u32` | 32-bit | 0..2^32-1 | u64 (zero-extended) |
| `u64` | 64-bit | 0..2^64-1 | u64 (native) |
| `i8` | 8-bit | -128..127 | u64 (sign-extended) |
| `i16` | 16-bit | -32768..32767 | u64 (sign-extended) |
| `i32` | 32-bit | -2^31..2^31-1 | u64 (sign-extended) |
| `i64` | 64-bit | -2^63..2^63-1 | u64 (bit-reinterpret) |
| `f32` | 32-bit | IEEE 754 | u64 (f32 bits in low 32) |
| `f64` | 64-bit | IEEE 754 | u64 (bit-packed) |

All types fit in the u64 buffer slot. The type tag is on the
Port, not the value — the compiler tracks types statically.

### Type Inference for Operators

When both operands have the same type, the result has that type:
```
u64 + u64 → u64
f64 * f64 → f64
i32 & i32 → i32
```

When types differ, widening rules apply (safe direction only):
```
u32 + u64 → u64        (widen u32 → u64)
i32 + i64 → i64        (widen i32 → i64)
f32 * f64 → f64        (widen f32 → f64)
i32 + f64 → f64        (widen i32 → f64)
```

**No implicit narrowing.** These are compile-time errors:
```
u64 + i32 → ERROR      (u64 might not fit in i32)
f64 + f32 → ERROR      (f64 might lose precision in f32)
u64 + f32 → ERROR      (u64 might not fit in f32)
```

Use explicit cast functions for narrowing:
```
narrow := as_i32(wide_u64)   // runtime bounds check
lossy  := as_f32(precise_f64) // explicit precision loss
```

### Overflow Behavior

| Type class | Default | Configurable |
|------------|---------|-------------|
| Unsigned int | Wrapping | `checked_add(a, b)` returns error on overflow |
| Signed int | Wrapping | Same |
| Float | IEEE 754 (inf, NaN) | Unchanged |

Wrapping is the default because GK is a data generation engine,
not a business logic engine. Hash-derived values routinely
overflow. The `checked_*` variants are available for workloads
that need bounds safety.

### Bitwise Operations

Only valid on integer types (u8..u64, i8..i64). Applying bitwise
ops to floats is a compile-time error:

```gk
mask := cycle & 0xFF          // OK: u64 & u64
bits := hash(cycle) >> 32     // OK: u64 >> u64
bad  := 3.14 & 0xFF           // ERROR: bitwise op on f64
```

### Cranelift JIT Support

All proposed operations map directly to Cranelift instructions:

| Operation | Cranelift IR |
|-----------|-------------|
| `+` (int) | `iadd` |
| `+` (float) | `fadd` |
| `*` (int) | `imul` |
| `*` (float) | `fmul` |
| `&` | `band` |
| `\|` | `bor` |
| `^` | `bxor` |
| `<<` | `ishl` |
| `>>` (unsigned) | `ushr` |
| `>>` (signed) | `sshr` |
| `!` | `bnot` |
| `**` (float) | extern `pow` |

All are P3 JIT-ready — no extern calls needed except `**`.

---

## Implementation Plan

### Phase 1: Fix `^` → XOR, add `**` for power
- Change lexer: `^` → `Caret` (XOR), `**` → `StarStar` (power)
- Update parser precedence
- Update desugaring: `^` → `u64_xor(a, b)`, `**` → `pow(a, b)`
- Add `u64_xor` node

### Phase 2: Add bitwise operators
- Lexer: `&`, `|`, `<<`, `>>`, `!`
- Parser: precedence per Rust
- Nodes: `u64_and`, `u64_or`, `u64_xor`, `u64_shl`, `u64_shr`, `u64_not`
- JIT: direct Cranelift `band`/`bor`/`bxor`/`ishl`/`ushr`/`bnot`

### Phase 3: Integer infix arithmetic
- Type-aware desugaring: `u64 + u64` → `u64_add(a, b)` (new 2-wire node)
- `f64 + f64` → `f64_add(a, b)` (existing)
- Mixed: widen to common type, then operate
- Add 2-wire u64 nodes: `u64_add`, `u64_sub`, `u64_mul`, `u64_div`

### Phase 4: Narrower types
- Port types: extend PortType with u8/u16/u32/i8/i16/i32/i64/f32
- Widening adapters (auto-inserted like u64→f64 today)
- Narrowing cast functions (explicit only)
- JIT: Cranelift `ireduce`/`uextend`/`sextend` for width changes

### Phase 5: Overflow checking
- `checked_add`, `checked_mul` etc. as explicit function nodes
- Strict mode: make wrapping arithmetic a warning
- Result type: Option-like (value or overflow error)

---

## What This Enables

```gk
// Bit extraction from hash
inputs := (cycle)
h := hash(cycle)
low_byte := h & 0xFF
high_nybble := (h >> 60) & 0xF
flag := (h >> 32) & 1

// Integer arithmetic without float coercion
partition := (cycle / 10000) % 100
bucket := cycle % num_buckets

// Power and math
amplitude := sin(to_f64(cycle) * 0.01) ** 2.0
decay := exp(-to_f64(cycle) * 0.001)

// Bit manipulation for key construction
key := (region << 48) | (tenant << 32) | sequence
```
