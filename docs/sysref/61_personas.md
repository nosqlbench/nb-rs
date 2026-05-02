# 61: Single Binary, Feature-Gated Drivers

`nbrs` is the single user-facing CLI. Protocol-specific drivers
that need heavy or non-portable build requirements (C++
toolchain, system libraries) are gated behind Cargo features,
so users compile in only what they need.

This is a deliberate flip from the earlier *persona* model
(separate binaries per protocol family). Personas were retired
in favor of features once the cost of multi-binary maintenance
(duplicate `main.rs`, duplicate TUI wiring, fork drift) outgrew
the benefit (avoiding optional deps).

---

## Architecture

```
nbrs (single binary)
├── nbrs-adapter-stdout              (always)
├── nbrs-adapter-http                (always)
├── nbrs-adapter-testkit             (always)
├── nbrs-adapter-plotter             (always)
├── nbrs-adapter-cql                 (always — common surface)
│   ├── engine-scylla feature        (default — pure-Rust)
│   └── engine-cassandra-cpp feature (opt-in — needs libcassandra)
└── nbrs-adapter-openapi             (openapi feature)
```

The default build picks up every adapter that compiles cleanly
on stock Rust. Opt-in features add drivers that require system
toolchains.

---

## Cargo features on `nbrs`

| Feature | Default | Adds |
|---------|---------|------|
| `engine-scylla` | yes | Pure-Rust ScyllaDB driver. `cqldriver=scylla`. |
| `engine-cassandra-cpp` | no | DataStax Cassandra C++ driver. `cqldriver=cassandra-cpp`. Requires `libcassandra` + libuv + openssl on the host or via `adapters/cql/build.sh`. |
| `all-engines` | no | Both CQL engines linked; runtime selection via `cqldriver=`. |
| `openapi` | no | Adds `describe-openapi` and `run-openapi` subcommands that synthesize ops from an OpenAPI 3.x spec. |
| `flamegraph` | no | Forwards to `nbrs-activity/flamegraph` for built-in CPU profiling. |

---

## Building

### Default (everything that needs no system toolchain)

```bash
cargo build --release -p nbrs
```

### Opt-in: cassandra-cpp engine

The DataStax C++ driver isn't on crates.io; build infrastructure
lives under `adapters/cql/`:

```bash
cd adapters/cql

# Full build: builds C driver in Docker, extracts to sysroot/, then cargo
bash build.sh

# Driver only — useful for first-time setup
bash build.sh driver

# Cargo only (after sysroot/ exists)
bash build.sh cargo

# Everything inside Docker (no host Rust needed)
bash build.sh docker
```

`build.sh cargo` invokes:

```bash
cargo build --release -p nbrs --no-default-features \
    --features engine-cassandra-cpp
```

Both engines together:

```bash
cargo build --release -p nbrs --features all-engines
```

### Opt-in: OpenAPI workload generation

```bash
cargo build --release -p nbrs --features openapi
```

Adds `describe-openapi` and `run-openapi` subcommands.

---

## Adapter selection at runtime

User-facing names (registered in inventory):

- `adapter=stdout`
- `adapter=http`
- `adapter=testkit`
- `adapter=plotter`
- `adapter=cql`

`adapter=cql` is a meta-adapter that resolves to a concrete
engine via [`AliasResolverEntry`](../../nbrs-activity/src/adapter.rs).
The user picks the engine with `cqldriver=scylla` /
`cqldriver=cassandra-cpp`. Direct dispatch by engine name is
intentionally not exposed — engines stay an internal concept.

```bash
# Default — picks the lowest-rank engine (cassandra-cpp if both
# are linked; scylla otherwise).
nbrs run adapter=cql workload=...

# Force a specific engine.
nbrs run adapter=cql cqldriver=scylla
nbrs run adapter=cql cqldriver=cassandra-cpp
```

---

## Why features instead of personas

- **Single composition root.** One `main.rs`, one TUI wiring,
  one set of subcommand dispatch. No fork drift.
- **Familiar Cargo idiom.** Users who already know how to
  `cargo build --features ...` for any other Rust crate can
  drive nbrs the same way.
- **Reusable glue.** TUI observer, post-run summary, completion
  shell — all live in `nbrs-tui` / `nbrs-activity` / `nbrs-workload`
  and are reachable from any future binary that wants them.
- **Honest minimal default.** `cargo build -p nbrs` builds
  cleanly on a stock Rust toolchain with no system packages.
- **Independent driver evolution.** Driver crates
  (`nbrs-adapter-cql`, `nbrs-adapter-openapi`) version
  independently. A driver-only release doesn't churn the
  user-facing CLI.

---

## Future drivers

| Driver | Crate | Feature flag | Status |
|--------|-------|--------------|--------|
| Scylla / Cassandra | `nbrs-adapter-cql` | `engine-scylla` | Default |
| Cassandra (C++) | `nbrs-adapter-cql` | `engine-cassandra-cpp` | Implemented |
| OpenAPI 3.x | `nbrs-adapter-openapi` | `openapi` | Implemented |
| gRPC | `nbrs-adapter-grpc` (planned) | `grpc` | Planned |
| SQL (sqlx) | `nbrs-adapter-sql` (planned) | `sql` | Planned |
| Redis | `nbrs-adapter-redis` (planned) | `redis` | Planned |

Each new driver follows the same pattern: a library crate that
implements [`DriverAdapter`](../../nbrs-activity/src/adapter.rs)
or registers an `AliasResolverEntry`, plus a feature flag on
`nbrs` that pulls it in.
