# cassnbrs

Cassandra persona for nbrs. Adds a native CQL adapter using
the Apache Cassandra C++ driver alongside all standard adapters
(stdout, http, testkit, plotter).

## Build

Requires the Cassandra C++ driver (`libcassandra`). Install it or
build from the provided Dockerfile:

```bash
# Install the driver (Ubuntu/Debian)
sudo apt-get install libcassandra-dev

# Or build the driver from Dockerfile
docker build -f cassandra-cpp-driver.Dockerfile -t cassandra-cpp-driver .

# Build cassnbrs
cargo build --release
```

The binary is at `target/release/cassnbrs`.

## Usage

```bash
# CQL workload against localhost
cassnbrs cql_vector.yaml dataset=example prefix=label

# Dry-run to stdout
cassnbrs cql_vector.yaml adapter=stdout

# Run a specific scenario phase
cassnbrs cql_vector.yaml schema

# Inline CQL
cassnbrs run adapter=cql hosts=localhost op='SELECT * FROM system.local LIMIT 1' cycles=1
```

Workload files with `#!/usr/bin/env cassnbrs` shebangs run directly:

```bash
chmod +x workloads/cql_vector.yaml
./workloads/cql_vector.yaml dataset=example prefix=label
```

## Adapters

| Name | Description |
|------|-------------|
| `cassandra` / `cql` | Native CQL via C++ driver. Supports prepared statements, raw DDL, consistency levels. |
| `stdout` | Text output for debugging (stmt, json, csv, readout, tsv, raw formats). |
| `http` | REST API testing. |
| `testkit` | Simulated execution with configurable latency/errors. |
| `plotter` | Live terminal plots (line, parametric, polar modes). |

## CQL Adapter Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `hosts` | `127.0.0.1` | Cassandra contact points (comma-separated) |
| `port` | `9042` | CQL native transport port |
| `keyspace` | (none) | Keyspace to connect to. Auto-falls back to no-keyspace if not found (for DDL). |
| `consistency` | `LOCAL_ONE` | Default consistency level |
| `username` | (none) | Authentication username |
| `password` | (none) | Authentication password |
| `request_timeout_ms` | `12000` | Per-request timeout in milliseconds |

## Statement Modes

The op field name selects the CQL execution mode:

| Field | Mode | Use Case |
|-------|------|----------|
| `raw:` | String interpolation, `session.execute(text)` | DDL (CREATE, DROP, ALTER) |
| `prepared:` | Prepared statement with typed bind | DML (INSERT, SELECT, UPDATE) |
| `stmt:` | Alias for `prepared:` | Default for most ops |

## Workloads

- `cql_vector.yaml` — Multi-profile vector search with filtered ground truth
- `cql_keyvalue.yaml` — Simple key-value read/write workload

## GK Extensions

cassnbrs registers additional GK node functions via inventory:

- `cql_timeuuid(seed)` — Deterministic CQL timeuuid from a u64 seed
