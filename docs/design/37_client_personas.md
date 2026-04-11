# Client Personas — Protocol-Specific nbrs Binaries

nbrs client personas are standalone binaries that extend the core
framework with protocol-specific adapters for testing particular
families of services. Each persona includes all core adapters
(stdout, http, model) plus one or more native protocol drivers.

---

## Motivation

The core `nbrs` binary includes lightweight, universal adapters
(stdout for local testing, http for REST APIs, model for simulation).
But real workload testing requires native protocol drivers that speak
the wire format of the target system — CQL for Cassandra, gRPC for
microservices, SQL for relational databases, etc.

Rather than bundling every driver into one monolithic binary (which
would pull in dozens of transitive dependencies and slow compilation),
each persona is a separate binary crate that depends on:

1. The core `nb-activity` runtime (executor, metrics, rate limiting)
2. The core adapters (stdout, http, model)
3. One or more protocol-specific adapter crates

This gives:
- **Fast compilation**: only the drivers you need
- **Small binaries**: no unused protocol libraries
- **Clean dependency trees**: CQL users don't pull gRPC, and vice versa
- **Independent release cadence**: driver updates don't require core changes

---

## Architecture

### Binary Crate Layout

```
nb-rs/                     ← core binary (stdout, http, model only)
personas/
  cassnbrs/                ← Cassandra/CQL persona
    Cargo.toml
    src/main.rs
  grpcnbrs/                ← gRPC persona (future)
  sqlnbrs/                 ← SQL/JDBC persona (future)
adapters/
  nb-adapter-stdout/       ← core (included in all personas)
  nb-adapter-http/         ← core (included in all personas)
  nb-adapter-model/        ← core (included in all personas)
  nb-adapter-cql/          ← CQL adapter crate
  nb-adapter-grpc/         ← gRPC adapter crate (future)
  nb-adapter-sql/          ← SQL adapter crate (future)
```

### Persona Binary Structure

Each persona's `main.rs` is a thin wrapper that:
1. Reuses the core CLI parsing from `nb-rs`
2. Registers additional adapters in the dispatch match
3. Provides persona-specific help text and defaults

```rust
// personas/cassnbrs/src/main.rs
fn main() {
    // Core CLI + adapter dispatch, extended with CQL
    nbrs_core::run(|adapter_name, params| {
        match adapter_name {
            "cql" | "cassandra" | "astra" => {
                Some(Arc::new(CqlAdapter::from_params(params)?))
            }
            _ => None, // fall through to core adapters
        }
    });
}
```

### Adapter Registration

The current hardcoded match in `nb-rs/src/main.rs` is refactored
into a pluggable dispatch:

```rust
// nb-activity/src/adapter.rs (or a new nb-core crate)
pub trait AdapterFactory: Send + Sync {
    fn create(&self, name: &str, params: &HashMap<String, String>)
        -> Option<Result<Arc<dyn Adapter>, String>>;
}
```

The core binary registers stdout/http/model. Persona binaries
register additional factories before the core ones, so
persona-specific adapter names take priority.

---

## cassnbrs — The Cassandra Persona

### What It Includes

- All core adapters (stdout, http, model)
- `nb-adapter-cql`: native CQL protocol adapter using the
  `scylla` Rust driver (scylla-rust-driver)

### Why cassandra-cpp (Apache Cassandra C++ driver)

| Driver | Language | Provenance | Async | Notes |
|--------|----------|-----------|-------|-------|
| **cassandra-cpp** | C++ FFI | Apache Cassandra | Callback-based | Official Apache project |
| scylla | Pure Rust | ScyllaDB | Tokio | Third-party, not Apache |
| cdrs-tokio | Pure Rust | Community | Tokio | No corporate backing |

The `cassandra-cpp` Rust crate wraps the **Apache Cassandra C++
driver** (`apache/cassandra-cpp-driver`), the official C/C++
driver maintained under the Apache Cassandra project. Originally
developed by DataStax and contributed to Apache, it is the
canonical non-Java driver used internally by the official Python
and Node.js drivers.

- **Official Apache project**: https://github.com/apache/cassandra-cpp-driver
- **Full CQL v4 support**: prepared statements, batch, paging,
  token-aware routing, speculative execution, SSL/TLS
- **Astra compatibility**: supports cloud secure connect bundles
- **Battle-tested**: years of production use across all major
  Cassandra deployments

**Rust crate chain**: `cassandra-cpp` (safe Rust API) →
`cassandra-cpp-sys` (bindgen FFI) → `libcassandra.so` (system
library). The `-sys` crate links to whatever `libcassandra` is
installed on the system — install the Apache fork and the Rust
crate works unchanged.

**Build requirement**: The Apache Cassandra C++ driver must be
installed on the build system:

```bash
# Build from source (recommended):
git clone https://github.com/apache/cassandra-cpp-driver.git
cd cassandra-cpp-driver
sudo apt-get install libuv1-dev cmake g++
mkdir build && cd build
cmake .. && make && sudo make install
sudo ldconfig
```

**Async bridge**: The C++ driver uses callback-based async
internally. The adapter wraps this in a Tokio-compatible future
using `tokio::task::spawn_blocking` to bridge the callback
into the nb-rs async executor model.

### CQL Adapter Design

```rust
pub struct CqlAdapter {
    session: cassandra_cpp::Session,
    prepared: HashMap<String, cassandra_cpp::PreparedStatement>,
}
```

#### Connection Parameters

```yaml
# In workload YAML or CLI params:
adapter: cql
hosts: "node1:9042,node2:9042,node3:9042"
keyspace: "my_keyspace"
localdc: "datacenter1"
consistency: "LOCAL_QUORUM"
username: "cassandra"
password: "cassandra"

# Astra cloud:
adapter: cql
secure_connect_bundle: "/path/to/secure-connect-mydb.zip"
username: "client_id"
password: "client_secret"
```

#### Op Template Mapping

CQL ops use the standard op template format with bind points:

```yaml
ops:
  insert_user:
    stmt: "INSERT INTO users (id, name, email) VALUES ({id}, {name}, {email})"
    id: "{mod(hash(cycle), 1000000)}"
    name: "{weighted_strings(hash(cycle), 'alice:0.3;bob:0.3;carol:0.4')}"
    email: "{format_u64(hash(cycle), 10)}@example.com"

  read_user:
    stmt: "SELECT * FROM users WHERE id = {id}"
    id: "{mod(hash(cycle), 1000000)}"
```

The adapter:
1. Parses the `stmt` field as a CQL statement
2. Extracts bind points (`{id}`, `{name}`, `{email}`)
3. On first execution, prepares the statement via the driver
4. On each cycle, binds the GK-generated values and executes

#### Type Mapping

GK values map to CQL types via the `Value` enum:

| GK Value | CQL Type |
|----------|----------|
| `Value::U64` | `bigint` |
| `Value::F64` | `double` |
| `Value::Str` | `text` / `varchar` |
| `Value::Bool` | `boolean` |
| `Value::Bytes` | `blob` |
| `Value::Json` | `text` (serialized) |
| `Value::Ext(Uuid)` | `uuid` / `timeuuid` |

The `Ext` variant with `ReflectedValue` enables adapter-contributed
types. The CQL adapter can register UUID, timestamp, and other
Cassandra-native types that flow through the GK kernel as `Ext`
values and bind natively without string conversion.

#### Metrics

The CQL adapter reports standard nb-rs metrics:
- `op_timer` — per-op latency (including network round-trip)
- `op_counter` — total ops executed
- `error_counter` — CQL errors by error code
- `rows_counter` — rows returned for SELECT queries

Plus CQL-specific metrics:
- `cql_retries` — driver-level retries
- `cql_speculative_executions` — speculative execution attempts
- `cql_coordinator` — which node coordinated each query (for
  load balancing analysis)

#### Error Handling

CQL errors map to the nb-rs error handler framework:

| CQL Error | nb-rs Action |
|-----------|-------------|
| `Unavailable` | Retry or stop (configurable) |
| `ReadTimeout` | Retry with backoff |
| `WriteTimeout` | Retry with backoff |
| `Overloaded` | Rate-limit back-pressure |
| `SyntaxError` | Stop immediately (workload bug) |
| `Unauthorized` | Stop immediately (config bug) |

### Capture Points

The CQL adapter implements `CaptureExtractor` to support inter-op
data flow (SRD 28). For SELECT queries, the adapter extracts column
values from the result set and feeds them back to the GK kernel as
volatile ports:

```yaml
ops:
  read_user:
    stmt: "SELECT name, email FROM users WHERE id = {id}"
    capture:
      user_name: "name"       # column name → capture point
      user_email: "email"

  update_user:
    stmt: "UPDATE users SET email = {new_email} WHERE id = {id} AND name = {capture:user_name}"
    new_email: "{capture:user_email}@updated.com"
```

---

## Future Personas

| Persona | Binary | Adapter | Driver |
|---------|--------|---------|--------|
| **cassnbrs** | `cassnbrs` | `nb-adapter-cql` | `cassandra-cpp` (Apache Cassandra C++ driver) |
| **grpcnbrs** | `grpcnbrs` | `nb-adapter-grpc` | `tonic` |
| **sqlnbrs** | `sqlnbrs` | `nb-adapter-sql` | `sqlx` |
| **redisnbrs** | `redisnbrs` | `nb-adapter-redis` | `redis` |
| **kafkanbrs** | `kafkanbrs` | `nb-adapter-kafka` | `rdkafka` |
| **mongonbrs** | `mongonbrs` | `nb-adapter-mongo` | `mongodb` |

Each follows the same pattern: thin binary crate in `personas/`,
adapter crate in `adapters/`, protocol-specific op template mapping,
and native type binding through the `Value::Ext` mechanism.

---

## Implementation Path for cassnbrs

1. Install the Apache Cassandra C++ driver from
   https://github.com/apache/cassandra-cpp-driver
2. Create `adapters/nb-adapter-cql/` crate with `cassandra-cpp` dependency
2. Implement `Adapter` trait: connection, prepare, bind, execute
3. Implement `CaptureExtractor` for SELECT result extraction
4. Create `personas/cassnbrs/` binary crate
5. Refactor `nb-rs/src/main.rs` adapter dispatch into a shared
   `AdapterFactory` trait so personas can extend it
6. Add to workspace `Cargo.toml`
7. Add CQL-specific workload examples
8. Add integration tests (requires a running Cassandra instance;
   use `#[ignore]` for CI, run manually against docker)

---

## Relationship to Other SRDs

- **SRD 21 (Execution Layer)**: Personas use the same activity
  engine, cycle source, rate limiters, and op sequencing.
- **SRD 28 (Capture Points)**: CQL adapter implements capture
  extraction for SELECT result columns.
- **SRD 29 (Model Adapter)**: All personas include the model
  adapter for offline prototyping without a live cluster.
- **SRD 31 (Node Factories)**: CQL adapter can register
  Cassandra-specific GK nodes (uuid, timeuuid generators) via
  the `NodeFactory` trait.
- **SRD 33 (Op Pipeline)**: CQL ops flow through the same
  decorator stack (dry-run, capture, assert, metrics).
