# Interesting Binding Chains from nosqlbench Workloads

Scan of `links/nosqlbench/**/*.yaml` for the most interesting
bindings graphs to test with GK.

## Top Binding Chains for GK Testing

### Longest / Most Complex Composition

**1. Flight Date (12 nodes!)**
Source: `adapter-cqld4/.../bindings/expr.yaml`
```
HashRange(0,2); Mul(3600000); Save('hour'); Shuffle(0,2); Mul(60000); Save('minute');
HashRange(0,60); Mul(1000); Save('second'); Expr('hour + minute + second');
StartingEpochMillis('2018-10-02 04:00:00'); ToDate(); ToString()
```
Exercises Save/Load (stateful context), expressions, arithmetic,
and type cascading long→timestamp→date→string.

**2. Bulky Key-Value CharBuf (4 nodes, massive buffer)**
Source: `baselines/cql_keyvalue2_bulky.yaml`
```
Hash(); Mod(1000000000); CharBufImage('A-Za-z0-9 _|/',16000000,HashRange(50000,150000)); ToString()
```

### Higher-Order / Nested Functions

**3. Normalized Vectors with Dynamic Sizing**
Source: `examples/bindings_vectors.yaml`
```
HashedDoubleVectors(long->HashRange(1,5)->int, long->HashRange(2.0d,3.0d)->double); NormalizeDoubleVector(); Stringify()
```
HOF syntax with nested lambdas and type annotations — great for
testing the node graph builder.

**4. CQL Vector with Variable Dimensions**
Source: `adapter-cqld4/.../bindings/cqlvectors.yaml`
```
CqlVector(ListSizedHashed(HashRange(3,5)->int, HashRange(0.0f,1.0f))); NormalizeCqlVector()
```

**5. Nested Collections**
Source: `examples/bindings_collections.yaml`
```
SetSizedStepped(HashRange(3,4), ListSizedStepped(HashRange(2,3), Combinations('A-Z;0-9;a-z')))
MapSized(2, NumberNameToString(), MapSized(2, NumberNameToString(), long->ToString()))
```

### Distribution / Probability Chains

**6. Normal + Clamp + Conditional**
Source: `adapter-cqld4/.../bindings/expr.yaml`
```
Normal(0.0,5.0); Clamp(1,100); Save('riskScore') -> int
Expr('riskScore > 90 ? 0 : 1') -> long; ToBoolean(); ToString()
```

**7. WeightedStrings (real-world categorical)**
Source: `baselines/graph_wheels.yaml`, `bindings/text.yaml`
```
WeightedStrings('android:6;ios:4;linux:2;osx:7;windows:3')
WeightedStrings('Mr:0.45;Mrs:0.25;Ms:0.1;Miss:0.1;Dr:0.05')
```

### Crypto / Buffer Chains

**8. Digest Pipeline**
Source: `examples/bindings_bytebuffers.yaml`
```
ByteBufferSizedHashed(1000); DigestToByteBuffer('SHA-256'); ToHexString()
NumberNameToString(); ToCharBuffer(); ToByteBuffer(); ToHexString()
```

### Access Pattern / Partitioning

**9. Scaled Hash Ranges**
Source: `baselines/incremental.yaml`
```
Div(2L); Hash(); HashRangeScaled(1.0d); Hash(); ToString()
```

**10. Multi-Modulo Partitioning**
Source: `baselines/cql_iot.yaml`
```
Div(10000); Mod(100); ToHashedUUID() -> java.util.UUID
```

## Coverage Gap Analysis

- Linear chains (Hash;Mod;ToString) — well covered, no gap
- HOF / nested lambdas (`long->HashRange(1,5)->int`) — partially covered via registry. **Gap: HOF node builder**
- Save/Load context vars — not implemented. **Gap: stateful context**
- `Expr()` evaluation — not implemented. **Gap: expression engine**
- WeightedStrings/Longs — not implemented. **Moderate priority**
- File-backed (HashedLineToString) — not implemented. **Lower priority**
- Nested collections (SetSizedStepped of ListSizedStepped) — not implemented. **Gap: complex HOF composition**
- Vector normalization — not implemented. **Needed for vector workloads**
- Template-based strings — not implemented. **Common pattern**
- BigDecimal precision/rounding — not applicable (Rust). Adapt to f64

## Notes

The **flight date chain** (#1) and the **nested HOF
vector/collection chains** (#3-5) are the most interesting for
stress-testing GK's graph composition and type inference. The
Save/Load + Expr pattern (#6) would require a new stateful-context
feature in the node graph.
