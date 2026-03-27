# Inverse CDF Sampling — Design Sketch

Sampling from continuous and discrete probability distributions via the
inverse cumulative distribution function (ICD). This is the mechanism
behind Normal, Zipf, Exponential, Poisson, and all other shaped
distribution nodes in the GK.

---

## What It Does

Given a uniform value in [0, 1), the inverse CDF maps it to a value
drawn from the target distribution. For example, a uniform input near
0.5 maps to a value near the median; inputs near 0.0 or 1.0 map to
the tails.

## Java nosqlbench Implementation

The Java version combined several concerns into single classes:
- Input hashing (hash vs. map mode)
- Normalization (long → [0,1])
- ICD lookup (interpolate vs. compute mode)
- Clamping (bounding extreme values)
- Type conversion (various int/long/double combinations)

This produced 70 classes (20 distributions × ~4 type combos), each
bundling all stages together.

## nb-rs Design: Decomposed Building Blocks

Following the single-responsibility principle, we decompose the ICD
pipeline into independent node functions. No node combines hashing
with distribution sampling.

### The Pipeline (explicit in the DAG)

```
coordinate
    │
    ▼
  hash         ← explicit Hash64 node (user-placed)
    │
    ▼
  normalize    ← UnitInterval node: u64 → f64 in [0, 1)
    │
    ▼
  sample       ← IcdSample node: f64 → f64 (continuous) or f64 → u64 (discrete)
    │
    ▼
  clamp        ← Clamp node (optional, user-placed)
    │
    ▼
  output
```

In the Java version, all of this was one function call. In nb-rs,
each step is a visible, composable node. The user can:
- Omit hashing (for map-mode sequential access)
- Omit clamping (for unbounded distributions)
- Chain multiple distributions (e.g., normal → clamp → discretize)
- Share the hash across multiple distribution branches

### Node Functions

**UnitInterval** — normalize u64 to [0, 1)

| Node          | Arity | Signature   | Description                       |
|---------------|-------|-------------|-----------------------------------|
| UnitInterval  | 1→1   | u64 → f64   | Map u64 to [0.0, 1.0) uniformly  |

Implementation: `input as f64 / u64::MAX as f64`

**IcdContinuous** — sample from a continuous distribution

| Node          | Arity | Signature   | Description                       |
|---------------|-------|-------------|-----------------------------------|
| IcdContinuous | 1→1   | f64 → f64   | Inverse CDF lookup/interpolation  |

Parameterized at assembly time with:
- Distribution type and parameters (e.g., Normal(mean, stddev))
- Resolution (number of interpolation table entries, default 1000)

Two internal modes (chosen at assembly time, not a user concern):
- **Interpolate** (default): pre-compute a lookup table and linearly
  interpolate between entries. O(1) per sample, ~8KB memory.
- **Compute**: call the inverse CDF directly per sample. Zero memory,
  variable speed depending on distribution.

**IcdDiscrete** — sample from a discrete distribution

| Node          | Arity | Signature   | Description                       |
|---------------|-------|-------------|-----------------------------------|
| IcdDiscrete   | 1→1   | f64 → u64   | Inverse CDF for discrete dists    |

Same structure as IcdContinuous but outputs u64 (the discrete outcome
index).

**ClampF64** — bound a float value

| Node          | Arity | Signature   | Description                       |
|---------------|-------|-------------|-----------------------------------|
| ClampF64      | 1→1   | f64 → f64   | Clamp to [min, max]              |

Already exists conceptually in the arithmetic module. Needed for
distributions like Cauchy that produce extreme tail values.

### Supported Distributions

**Continuous** (IcdContinuous parameters):

| Distribution | Parameters          | Support        |
|-------------|---------------------|----------------|
| Normal      | mean, stddev        | (-∞, +∞)       |
| Uniform     | min, max            | [min, max]     |
| Exponential | rate                | [0, +∞)        |
| Pareto      | scale, shape        | [scale, +∞)    |
| LogNormal   | mean, stddev        | (0, +∞)        |
| Beta        | alpha, beta         | [0, 1]         |
| Gamma       | shape, scale        | (0, +∞)        |
| Weibull     | shape, scale        | [0, +∞)        |
| Cauchy      | location, scale     | (-∞, +∞)       |
| Laplace     | location, scale     | (-∞, +∞)       |

**Discrete** (IcdDiscrete parameters):

| Distribution  | Parameters              | Support     |
|--------------|-------------------------|-------------|
| Zipf         | n_elements, exponent    | [1, n]      |
| Poisson      | lambda                  | [0, +∞)     |
| Binomial     | trials, probability     | [0, trials] |
| Geometric    | probability             | [1, +∞)     |

### Interpolation Table

The core optimization for ICD sampling. Pre-computed at assembly time:

```rust
struct IcdTable {
    /// Pre-computed inverse CDF values at evenly spaced quantiles.
    lut: Vec<f64>,
    /// 1.0 / (lut.len() - 1), pre-computed for the hot path.
    inv_resolution: f64,
}
```

Sampling:
```rust
fn sample(&self, u: f64) -> f64 {
    let pos = u * (self.lut.len() - 1) as f64;
    let idx = pos as usize;
    let frac = pos - idx as f64;
    self.lut[idx] * (1.0 - frac) + self.lut[idx + 1] * frac
}
```

Linear interpolation between the two nearest pre-computed points.
Resolution of 1000 gives ~0.1% accuracy for well-behaved
distributions.

### Example DAG: Normal-distributed reading values

```
coordinates := (cycle)
(tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)
reading_seed := hash(interleave(device, reading))
reading_unit := unit_interval(reading_seed)
reading_value := icd_normal(reading_unit, 72.0, 5.0)
```

vs. Java nosqlbench:
```yaml
reading_value: Normal(72.0, 5.0)
```

The Java version implicitly hashes, normalizes, and samples. The GK
makes every step visible. The AOT compiler ensures no performance cost
for the decomposition.
