# Dockerfile for building nbrs with the cassandra-cpp engine.
#
# Two-stage build:
#   1. Build the Apache Cassandra C++ driver from source
#   2. Build nbrs with Rust, --features engine-cassandra-cpp,
#      linking against the driver
#
# Build context is the workspace root, not adapters/cql:
#   docker build -f adapters/cql/nbrs-cassandra-cpp.Dockerfile -t nbrs-cassandra-cpp .
#   docker run --rm nbrs-cassandra-cpp --help
#
# To connect to a Cassandra cluster:
#   docker run --rm --network host nbrs-cassandra-cpp run \
#     adapter=cql cqldriver=cassandra-cpp \
#     hosts=localhost keyspace=myks ...

# --- Stage 1: Build the Apache Cassandra C++ driver ---
FROM ubuntu:24.04 AS cpp-driver

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git libuv1-dev libssl-dev zlib1g-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 \
    https://github.com/apache/cassandra-cpp-driver.git /tmp/cass \
    && cd /tmp/cass && mkdir build && cd build \
    && cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCASS_BUILD_STATIC=ON \
        -DCMAKE_INSTALL_LIBDIR=lib \
    && make -j$(nproc) && make install && ldconfig

# --- Stage 2: Build nbrs with Rust ---
FROM ubuntu:24.04 AS rust-builder

# Install Rust + clang + mold + zlib1g-dev.
# Workspace .cargo/config.toml sets `linker = "clang"` and
# rustflags = ["-C", "link-arg=-fuse-ld=mold", …, "-C", "link-arg=-lz"]
# for every Linux target. zlib1g-dev supplies the `libz.so` symlink
# the linker resolves `-lz` against; the runtime-only `zlib1g`
# only ships `libz.so.1`.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential pkg-config libuv1-dev libssl-dev zlib1g-dev \
    clang mold ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Copy the built C driver libraries and headers. The cpp-driver
# stage forces `CMAKE_INSTALL_LIBDIR=lib` so libcassandra lands
# in /usr/local/lib/ directly on every arch (not the
# `lib/<triple>/` multiarch path cmake picks by default under
# /usr/local) — upstream cassandra-cpp-sys build.rs hardcodes
# x86_64-linux-gnu in its link-search list and won't find an
# arm64 multiarch dir.
COPY --from=cpp-driver /usr/local/lib/ /usr/local/lib/
COPY --from=cpp-driver /usr/local/include/cassandra.h /usr/local/include/
RUN ldconfig

# Copy the full nb-rs workspace
WORKDIR /src
COPY . .

RUN cargo build --release -p nbrs \
    --no-default-features --features engine-cassandra-cpp

# --- Stage 3: Runtime ---
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libuv1 libssl3t64 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=cpp-driver /usr/local/lib/ /usr/local/lib/
RUN ldconfig

COPY --from=rust-builder /src/target/release/nbrs /usr/local/bin/nbrs

ENTRYPOINT ["nbrs"]
