# 61: Persona Model

Personas are protocol-specific binaries that extend the core
nb-rs engine with native database drivers.

---

## Architecture

```
nbrs (core)                    cassnbrs (persona)
├── nb-adapter-stdout          ├── nb-adapter-stdout
├── nb-adapter-http            ├── nb-adapter-http
├── nb-adapter-model           ├── nb-adapter-model
└── (no native drivers)        └── cassnbrs-adapter-cql
                                    └── cassandra-cpp (static)
```

The core `nbrs` binary includes only lightweight adapters with
no system dependencies. Persona binaries add native protocol
drivers that may require C/C++ libraries, system packages, or
special build procedures.

---

## Persona Structure

Each persona is a thin composition root:

```rust
// personas/cassnbrs/src/main.rs
fn main() {
    let args = std::env::args().skip(1).collect();
    let prepared = nb_activity::runner::prepare(&args)?;

    match prepared.driver.as_str() {
        "cql" | "cassandra" => {
            let adapter = CqlAdapter::connect(&config).await?;
            prepared.run_with_driver(Arc::new(adapter)).await;
        }
        "stdout" => { /* core adapter */ }
        "http" => { /* core adapter */ }
        other => { eprintln!("unknown adapter '{other}'"); }
    }
}
```

All personas reuse:
- `nb-activity::runner::prepare()` for workload parsing, GK
  compilation, activity setup
- `nb-activity::Activity` for execution engine
- `nb-metrics` for instrumentation
- All core adapters (stdout, http, model)

---

## Current Personas

### cassnbrs — Cassandra/CQL

- **Driver**: Apache Cassandra C++ driver via `cassandra-cpp`
- **Build**: Docker-based sysroot for static linking
  (`bash build.sh` or `bash build.sh docker`)
- **Excluded from workspace**: requires C++ driver installation
- **Adapters**: `CqlAdapter` with `CqlRawDispenser` and
  `CqlPreparedDispenser`

### opennbrs — OpenSearch

- **Driver**: HTTP-based (uses core HTTP adapter)
- **In workspace**: no special dependencies

---

## Build

### Workspace Build (core + opennbrs)

```bash
cargo build --release
```

### cassnbrs Build

```bash
cd personas/cassnbrs

# Full build: C++ driver in Docker + cargo
bash build.sh

# Driver only (first time)
bash build.sh driver

# Cargo only (driver already built)
bash build.sh cargo

# Everything in Docker (no host Rust needed)
bash build.sh docker
```

The `build.sh` script:
1. Builds the C++ driver from source in Docker
2. Extracts static libraries and headers to `sysroot/`
3. Sets `CASSANDRA_SYS_LIB_PATH` and builds with cargo
4. Verifies static linking (no runtime `libcassandra.so` needed)

---

## Future Personas

| Persona | Binary | Driver Crate | Status |
|---------|--------|-------------|--------|
| cassnbrs | `cassnbrs` | `cassandra-cpp` | Implemented |
| opennbrs | `opennbrs` | HTTP | Implemented |
| grpcnbrs | `grpcnbrs` | `tonic` | Planned |
| sqlnbrs | `sqlnbrs` | `sqlx` | Planned |
| redisnbrs | `redisnbrs` | `redis` | Planned |

Each new persona follows the same pattern: adapter crate
implementing `DriverAdapter`/`OpDispenser`, persona binary
crate with `main.rs` dispatch, build script for any native
dependencies.

---

## Design Principle

Personas prevent dependency bloat. A user testing Cassandra
doesn't need gRPC dependencies. A user testing HTTP doesn't
need the Cassandra C++ driver. Each persona is a self-contained
binary with exactly the drivers it needs, statically linked
for single-binary deployment.
