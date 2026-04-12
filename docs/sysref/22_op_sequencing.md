# 22: Op Sequencing

The op sequence maps cycle numbers to op templates. It defines
the stanza structure and controls the ratio of different
operations in the workload.

---

### Design Note: Dynamic and Non-Uniform Stanzas

The current model (fixed ratio LUT, uniform stanzas) is
deterministic, efficient, and understandable. However it does not
support:

- **Dynamic op injection**: adding ops to the stanza at runtime
  based on results or conditions (e.g., "if SELECT returns empty,
  inject an INSERT")
- **Non-uniform stanzas**: stanzas that vary in structure across
  iterations (e.g., "first 1000 stanzas do setup, then switch to
  steady-state mix")
- **Conditional branching**: choosing between op paths based on
  a GK-evaluated condition

These capabilities are needed for realistic service simulation
workloads. The design should generalize the stanza contract to
allow non-uniform execution while preserving the current model as
the default (simple, deterministic) case. This is a separate
design discussion — see memos when available.

---

## Stanza Model

A **stanza** is one complete pass through all active op templates.
The stanza length is the sum of all op ratios.

```yaml
ops:
  read:                    # ratio 4 (default or explicit)
    tags: { phase: main }
    prepared: "SELECT ..."
  write:                   # ratio 1
    tags: { phase: main }
    prepared: "INSERT ..."

# Stanza length = 4 + 1 = 5 cycles
# Each stanza: 4 reads + 1 write (interleaved by sequencer)
```

### Stanza Isolation

- `CaptureContext` resets at stanza boundaries
- Captures flow within a stanza (op A's output feeds op B)
- Captures do NOT leak across stanza boundaries
- Each fiber processes one stanza at a time

---

## Cycle-to-Template Mapping

`OpSequence` maps cycle numbers to template indices via a
precomputed lookup table (LUT). O(1) per cycle.

```rust
pub struct OpSequence {
    templates: Vec<ParsedOp>,
    lut: Vec<usize>,  // cycle % lut.len() → template index
}

impl OpSequence {
    pub fn get_with_index(&self, cycle: u64) -> (usize, &ParsedOp) {
        let idx = self.lut[(cycle % self.lut.len() as u64) as usize];
        (idx, &self.templates[idx])
    }

    pub fn stanza_length(&self) -> usize {
        self.lut.len()
    }
}
```

---

## Sequencer Types

### Bucket (default)

Interleaved distribution. With ratios 3:1 (read:write):

```
R R R W R R R W R R R W ...
```

Distributes ops as evenly as possible across the stanza.

### Interval

Similar to bucket but with fractional positioning. Produces
slightly different interleaving patterns for non-power-of-two
ratios.

### Concat

Sequential blocks. Same 3:1 ratio:

```
R R R W W R R R W W ...
```

All reads, then all writes, per stanza. Useful when operation
order within a stanza matters (e.g., setup then verify).

---

## Default Cycles

When `cycles` is not specified on CLI or in workload params:

1. Check workload params for `cycles` key
2. Default to one stanza length (sum of all op ratios)

One stanza ensures every op template executes at least once.

---

## Tag Filtering

Before sequencing, ops are filtered by tag expressions:

```
tags=phase:rampup         → only ops with phase=rampup
tags=phase:main,type:read → ops matching BOTH conditions
```

After filtering, only the matching ops form the op sequence.
This is how workloads define separate phases (schema, rampup,
search) in a single YAML file, selected at runtime.

Filtered ops retain their original ratios. A 4:1 read:write
workload filtered to `type:read` produces a sequence of all
reads with no writes.
