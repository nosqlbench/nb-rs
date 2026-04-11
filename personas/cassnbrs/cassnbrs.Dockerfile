# Dockerfile for building cassnbrs — the Cassandra/CQL persona binary.
#
# Two-stage build:
#   1. Build the Apache Cassandra C++ driver from source
#   2. Build cassnbrs with Rust, linking against the driver
#
# Usage:
#   docker build -f docker/cassnbrs.Dockerfile -t cassnbrs .
#   docker run --rm cassnbrs --help
#
# To connect to a Cassandra cluster:
#   docker run --rm --network host cassnbrs run adapter=cql hosts=localhost keyspace=myks ...

# --- Stage 1: Build the Apache Cassandra C++ driver ---
FROM ubuntu:24.04 AS cpp-driver

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git libuv1-dev libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 \
    https://github.com/apache/cassandra-cpp-driver.git /tmp/cass \
    && cd /tmp/cass && mkdir build && cd build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release -DCASS_BUILD_STATIC=ON \
    && make -j$(nproc) && make install && ldconfig

# --- Stage 2: Build cassnbrs with Rust ---
FROM ubuntu:24.04 AS rust-builder

# Install Rust
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential pkg-config libuv1-dev libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Copy the built C driver libraries and headers
COPY --from=cpp-driver /usr/local/lib/libcassandra* /usr/local/lib/
COPY --from=cpp-driver /usr/local/include/cassandra.h /usr/local/include/
COPY --from=cpp-driver /usr/local/lib/pkgconfig/cassandra* /usr/local/lib/pkgconfig/
RUN ldconfig

# Copy the full nb-rs workspace
WORKDIR /src
COPY . .

# Move CQL crates from exclude to members for the build
RUN sed -i 's/^exclude = \[/# exclude = [/' Cargo.toml \
    && sed -i 's/"adapters\/nb-adapter-cql",$/# "adapters\/nb-adapter-cql",/' Cargo.toml \
    && sed -i 's/"personas\/cassnbrs",$/# "personas\/cassnbrs",/' Cargo.toml \
    && sed -i 's/"personas\/openapi-nbrs",/"personas\/openapi-nbrs",\n    "adapters\/nb-adapter-cql",\n    "personas\/cassnbrs",/' Cargo.toml

RUN cargo build --release -p cassnbrs

# --- Stage 3: Runtime ---
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libuv1 libssl3t64 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=cpp-driver /usr/local/lib/libcassandra* /usr/local/lib/
RUN ldconfig

COPY --from=rust-builder /src/target/release/cassnbrs /usr/local/bin/cassnbrs

ENTRYPOINT ["cassnbrs"]
