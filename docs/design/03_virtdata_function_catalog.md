# Virtdata Function Catalog — Warehouse Assessment

Inventory of all `@ThreadSafeMapper` annotated classes in the Java nosqlbench
virtdata modules. This serves as the starting reference for scoping the
standard GK node function library in nb-rs.

**Total: ~526 classes across 7 modules**

---

## Module Summary

| Module               | Count | Description                                |
|----------------------|------:|--------------------------------------------|
| virtdata-lib-basics  |   383 | Core transforms, conversions, collections  |
| virtdata-lib-curves4 |    70 | Statistical distribution samplers          |
| virtdata-lib-vectors |    28 | Vector generation, normalization, DNN      |
| virtdata-lib-realer  |    23 | Realistic data (names, places, codes)      |
| virtdata-lib-hdf5    |    14 | HDF5 file reading and array extraction     |
| virtdata-api (test)  |     5 | Test utilities only                        |
| virtdata-lib-io      |     3 | Vector file format readers (fvec, ivec)    |

---

## 1. virtdata-lib-basics (383 classes)

### 1.1 Hashing (46 classes)

Core deterministic hashing functions — the foundation of pseudo-random
but reproducible data generation.

- **Hash** — Murmur3F hash (long→long, long→int, int→int variants)
- **FullHash** — full 64-bit Murmur3F (long→long)
- **SignedHash** — signed hash variants (long→long, long→int, int→int)
- **HashRange** — hash into a bounded range (long→long, long→int, long→double, int→int)
- **HashInterval** — hash into an interval (long→long, long→int, long→double, int→int)
- **HashRangeScaled** — scaled hash range (long→long, long→int, int→int)
- **AddHashRange** — add hash-derived offset (long→long, long→int, int→int)
- **HashedLineToString** — pseudo-random line selection from file (long→String)
- **HashedLineToInt** — pseudo-random int from file (long→int)
- **HashedLoremExtractToString** — extract from lorem ipsum (long→String)
- **HashedFileExtractToString** — extract from file (long→String)
- **HashedLinesToKeyValueString** — key-value from file lines (long→String)
- **HashedByteBufferExtract** — extract from ByteBuffer (long→ByteBuffer)
- **HashedToByteBuffer** — hash to ByteBuffer (long→ByteBuffer)
- **ToHashedUUID** — stable hash to UUID (long→UUID)
- **HashMix** — hash mixing (long→double)
- **Murmur3DivToString** — hash and modulo to string (long→String)
- ~29 additional hashing variants

### 1.2 Modular Arithmetic (12 classes)

- **Mod** — modulo division (long→long, long→int, int→int)
- **ModuloToBoolean** — boolean from modulo (long→Boolean)
- **ModuloToByte/Short/Integer/Long** — typed modulo results
- **ModuloToBigInt/BigDecimal** — big number modulo
- **ModuloLineToString** — file line by modulo (long→String)
- **ModuloCSVLineToString** — CSV line by modulo (long→String)

### 1.3 Basic Arithmetic (long, int, double variants)

- **Add** — addition (long→long, int→int, double→double)
- **Mul** — multiplication (long→long, int→int, double→double, long→double)
- **Div** — division (long→long, int→int, double→double)
- **Clamp** — clamp to range (long→long, double→double)
- **Max** / **Min** — bounds (long→long, double→double)
- **Identity** — passthrough (long→long)
- **FixedValue** / **FixedValues** — constant output (long, int, double, String)

### 1.4 Range & Cycle Functions

- **CycleRange** — cycle through a range (long→long, long→int)
- **AddCycleRange** — add cycled offset (long→long, long→int)
- **Interpolate** — interpolation curve (long→long, long→double)
- **ScaledDouble** — scale to [0,1] (long→double)
- **TriangleWave** — triangle wave (double→double)

### 1.5 String Operations (26 classes)

- **Template** — string template with substitution (long→String)
- **AlphaNumericString** — alpha-numeric generation (long→String)
- **NumberNameToString** — spell out numbers (long→String)
- **CharBufImage** — character buffer sampling (long→String)
- **Combinations** — combinatorial ASCII codes (long→String)
- **Concat** variants — ConcatFixed, ConcatCycle, ConcatStepped,
  ConcatChained, ConcatArray, ConcatHashed
- **JoinTemplate** / **Join** — join function results
- **Expr** — expression evaluation (long→String)
- **Format** / **Suffix** / **Prefix** — string formatting
- **ReplaceRegex** / **URLEncode** / **URLDecode** — transformations
- **HTMLEntityEncode** / **HTMLEntityDecode** — HTML encoding
- **Base32Encode** / **Base32Decode** — base32
- **ToBase64String** / **ToBase64** — base64 encoding
- **DirectoryLines** / **DirectoryLinesStable** — file line reading
- **WeightedStrings** — weighted string selection

### 1.6 Type Conversion (52 classes)

Conversions between Java types. Organized by source/target:

- **Long to**: BigDecimal, BigInt, Boolean, Byte, Short, Int, Float,
  Double, Char, ByteBuffer, UUID, Date, Time, InetAddress, String
- **Double to**: Float, Int, Long, String
- **Int to**: Long, Double, String
- **String to**: various targets
- **Serialization**: ToJSON, ToJSONPretty, ToJSONF, ToJSONFPretty,
  ToHexString, ToBase64

### 1.7 Time & Date Operations (24 classes)

- **ToDate** / **ToDateTime** / **ToLocalTime** — type conversions
- **ToJavaInstant** / **ToJodaInstant** / **ToJodaDateTime**
- **ToEpochTimeUUID** / **ToFinestTimeUUID** — time-based UUIDs
- **CurrentEpochMillis** / **StartingEpochMillis** / **ElapsedNanoTime**
- **StringDateWrapper** — epoch millis as date string
- **ToMillisAtStartOf*** — Joda time rounding (Hour, Day, Month, Year,
  Minute, Second, NamedWeekDay, NextDay, NextNamedWeekDay)

### 1.8 UUID Operations (6 classes)

- **ToHashedUUID** — deterministic UUID from hash
- **ToEpochTimeUUID** / **ToFinestTimeUUID** — time-based UUIDs
- Plus 3 additional UUID variants

### 1.9 Collection Generators (20 classes)

Parameterized collection builders with size/step/hash variants:

- **List** / **ListSized** / **ListStepped** / **ListSizedStepped** /
  **ListHashed** / **ListSizedHashed** / **ListFunctions** / **ListTemplate**
- **Map** / **MapSized** / **MapStepped** / **MapSizedStepped** /
  **MapHashed** / **MapSizedHashed** / **MapFunctions**
- **Set** / **SetSized** / **SetHashed** / **SetSizedHashed**
- **HashedLineToStringList** / **HashedLineToStringSet** /
  **HashedLineToStringStringMap** / **HashedRangeToLongList**

### 1.10 Stateful Operations (Save/Load)

Thread-local variable storage for sharing values between bindings
within a single cycle:

- **Save** / **Load** — long, double, String, Float, Integer variants
- **Swap** / **Clear** — state management

### 1.11 Distribution & Statistical (6 classes)

- **CSVFrequencySampler** / **CSVSampler** — CSV-based sampling
- **DelimFrequencySampler** — delimited data sampling
- **EmpiricalDistribution** — empirical distribution
- **WeightedInts** — weighted integer selection
- **WeightedStringsFromCSV** — weighted string from CSV

### 1.12 Expression Evaluation

- **Expr** — evaluate expressions (long→long, long→int, long→double,
  long→String, double→double variants)

### 1.13 Byte Buffer Operations

- **ByteBufferSizedHashed** — sized buffer (long→ByteBuffer)
- **HashedByteBufferExtract** — extract from buffer
- **HashedToByteBuffer** — hash to buffer
- **ToMD5ByteBuffer** / **DigestToByteBuffer** — digest operations

### 1.14 Miscellaneous

- **LongFlow** — combine multiple operators (long→long)
- **SequenceOf** — sequence ordering (long→int)
- **ThreadNum** — extract thread number (long→int)
- **RandomStringFromRegex** — regex-based string generation
- **SumFunctions** — sum of functions (long→double)
- ~100 additional specialized mappers

---

## 2. virtdata-lib-curves4 (70 classes)

Statistical distribution samplers. Each distribution is implemented across
multiple input/output type combinations.

### 2.1 Continuous Distributions (40 classes)

Each available as both `int→double` and `long→double`:

| Distribution      | Description                                    |
|-------------------|------------------------------------------------|
| Beta              | Shape parameters alpha and beta                |
| Cauchy            | Long-tailed probability                        |
| ChiSquared        | Degrees of freedom parameter                   |
| ConstantContinuous| Always yields same value                       |
| Enumerated        | User-defined weights                           |
| Exponential       | Rate parameter                                 |
| F                 | Numerator/denominator degrees of freedom       |
| Gamma             | Shape and scale                                |
| Gumbel            | Extreme value distribution                     |
| Laplace           | Double exponential                             |
| Levy              | Heavy-tailed                                   |
| Logistic          | S-curve                                        |
| LogNormal         | Log-normal                                     |
| Nakagami          | Fading distribution                            |
| Normal            | Gaussian with mean and stddev                  |
| Pareto            | Power-law                                      |
| T                 | Student's t with degrees of freedom            |
| Triangular        | Three-point                                    |
| Uniform           | Continuous range                               |
| Weibull           | Shape and scale                                |

### 2.2 Discrete Distributions (30 classes)

Each available across `int→int`, `int→long`, `long→int`, `long→long`:

| Distribution    | Description                                      |
|-----------------|--------------------------------------------------|
| Binomial        | Trials and success probability                   |
| EnumeratedInts  | User-defined integer weights                     |
| Geometric       | Success probability                              |
| Hypergeometric  | Sampling without replacement                     |
| Pascal          | Negative binomial                                |
| Poisson         | Lambda (mean) parameter                          |
| Uniform         | Discrete range                                   |
| Zipf            | Power-law rank                                   |

### 2.3 Common Features

All distribution classes support configurable:
- **Sampling mode**: hash (deterministic) or map (sequential)
- **Interpolation**: interpolate (fast) or compute (precise)
- **Clamping**: for continuous distributions

---

## 3. virtdata-lib-vectors (28 classes)

### 3.1 DNN Vector Generation

- **DnnAngular1V** — angular-distributed vectors (long→float[])
- **DNN_angular1_neighbors** — angular neighbor indices (int→int[])
- **DNN_euclidean_v** — euclidean vectors (long→float[])
- **DNN_euclidean_v_series** — series of k vectors (long→float[][])
- **DNN_euclidean_v_wrap** — wrapping euclidean vectors (long→float[])
- **DNN_euclidean_neighbors** — euclidean neighbor indices (int→int[])
- **CircleVectors** — pluggable circle algorithm (long→List)

### 3.2 Primitive Vector Generation

- **DoubleVectors** / **FloatVectors** — radix-mapped vectors (long→double[]/float[])
- **HashedDoubleVectors** / **HashedFloatVectors** — hash-based vectors

### 3.3 Vector Manipulation

- **NormalizeDoubleVector** / **NormalizeFloatVector** — L2 normalization
- **DoubleVectorPadLeft/Right** / **FloatVectorPadLeft/Right** — padding
- **DoubleVectorPrefix/Suffix** / **FloatVectorPrefix/Suffix** — extend
- **ToFloatVector** — double[] to float[] conversion

### 3.4 Caching

- **DoubleArrayCache** — pre-computed vector cache (long→double[])
- **DoubleCache** — pre-computed scalar cache (long→double)

### 3.5 Dataset Access

- **BaseVectors** / **QueryVectors** — hosted dataset access (long→float[])
- **NeighborDistances** — neighbor distances (long→float[])
- **NeighborIndices** — neighbor indices (long→int[])

---

## 4. virtdata-lib-realer (23 classes)

All `long→String`, all extend CSVSampler. Realistic data from US census
and geographic datasets. Each category has uniform, by-density, and
by-population variants:

| Category     | Classes                                            |
|--------------|----------------------------------------------------|
| Names        | FirstNames, LastNames, FullNames                   |
| Countries    | CountryNames, CountryCodes                         |
| States       | StateCodes, StateNames (+ByDensity, +ByPopulation) |
| Cities       | Cities (+ByDensity, +ByPopulation)                 |
| Counties     | Counties (+ByDensity, +ByPopulation)               |
| Zip Codes    | ZipCodes (+ByDensity, +ByPopulation)               |
| Time Zones   | TimeZones (+ByDensity, +ByPopulation)              |

---

## 5. virtdata-lib-hdf5 (14 classes)

HDF5 file reading for large-scale dataset access:

- **HdfFileToFloatArray/IntArray/LongArray** — vector datasets (long→array)
- **HdfFileToFloatList/IntList/LongList** — vector datasets (long→List)
- **HdfFileToVarLengthIntArray/List** — variable-length arrays
- **HdfFileToInt** — scalar extraction (long→int)
- **HdfDatasetToString/Strings** — string serialization
- **HdfDatasetsToString** — paired dataset serialization
- **IntArrayToString** — array to string conversion
- **HdfBinToCql** — predicate parsing to CQL

---

## 6. virtdata-lib-io (3 classes)

Vector file format readers with random access:

- **FVecReader** — .fvec files (long→float[])
- **IVecReader** — .ivec files (long→int[])
- **BVecToFloatReader** — binary vectors to float (long→float[])

---

## Functional Cross-Cut Analysis

Looking across all modules, the functions cluster into these fundamental
categories relevant to GK node design:

### Category A: Number-Theoretic (u64 → u64)
Core building blocks. ~80 classes.
- Hashing (Murmur3F, signed/unsigned, range-bounded)
- Modular arithmetic (mod, div, cycle range)
- Basic arithmetic (add, mul, clamp, min, max)
- Identity, fixed values
- Bit manipulation, interleaving

### Category B: Statistical Distribution Sampling (u64 → u64/f64)
Shaped randomness. ~76 classes.
- 20 continuous distributions (Normal, Pareto, Zipf, etc.)
- 8 discrete distributions (Binomial, Poisson, etc.)
- Weighted/empirical sampling from data files
- Hash-then-sample pattern (deterministic + shaped)

### Category C: Type Crossing (u64 → String/bytes/UUID/etc.)
Bridge to typed output. ~80 classes.
- Numeric to string (formatting, base conversion, templates)
- Numeric to date/time types
- Numeric to UUID
- Numeric to byte buffers
- JSON serialization

### Category D: Realistic Data (u64 → String)
Domain-flavored output. ~23 classes.
- Person names, places, codes
- CSV/file-backed lookup tables
- Weighted by demographic distributions

### Category E: Collection Construction (u64 → List/Map/Set)
Composite output. ~20 classes.
- Parameterized by size, step, hash
- Nested generation (element functions)

### Category F: Vector Operations (u64 → float[]/double[])
ML/AI workload support. ~45 classes.
- Deterministic vector generation
- DNN-specific patterns (angular, euclidean)
- Normalization, padding, type conversion
- Dataset access (HDF5, fvec, ivec)

### Category G: Stateful / Side-Effect
Cross-binding communication. ~10 classes.
- Save/Load thread-local state
- Non-deterministic (current time, thread number)

---

## Observations for nb-rs GK Scoping

1. **Categories A and B are the core** — number-theoretic transforms and
   statistical distributions account for ~160 classes and form the
   computational heart of data generation. These map directly to u64-space
   GK nodes.

2. **Category C is the output boundary** — type-crossing functions are
   primarily edge adapters in the GK model. Many of the 80 Java classes
   exist only because Java lacks trait-based dispatch; in Rust, a single
   generic `ToString` or `Format` trait implementation may replace dozens.

3. **Category D is data, not logic** — realistic data functions are
   thin wrappers over CSV lookup tables. The GK needs a generic
   "weighted lookup from dataset" node, not 23 specialized classes.

4. **Category E maps to *-arity nodes** — collection construction in
   Java required dedicated classes because virtdata was unary. In the GK,
   an N→1 node that assembles a collection from N inputs is natural.

5. **Category F is specialized** — vector operations may be deferred to
   a later phase unless vector DB testing is an immediate priority.

6. **Category G needs careful design** — stateful operations (Save/Load)
   existed because Java virtdata had no way to share intermediate results.
   The GK's DAG model with named wires largely eliminates this need, but
   non-deterministic functions (current time) remain a special case.
