#!/bin/bash
# Build script for the cassandra-cpp engine of nbrs-adapter-cql.
#
# This script:
#   1. Builds the Apache Cassandra C++ driver from source in
#      Docker (matching the host OS for ABI compatibility).
#   2. Extracts the static library and headers to a local
#      sysroot under adapters/cql/sysroot/.
#   3. Builds nbrs with --features engine-cassandra-cpp,
#      linking statically against the driver.
#
# Usage:
#   cd adapters/cql
#   bash build.sh           # full build (driver + nbrs)
#   bash build.sh driver    # build only the C++ driver
#   bash build.sh cargo     # build only nbrs (driver must exist)
#   bash build.sh install   # cargo install --path nbrs with the cpp engine
#   bash build.sh docker    # build nbrs entirely inside Docker
#   bash build.sh clean     # remove sysroot and build artifacts

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SYSROOT="$SCRIPT_DIR/sysroot"
DOCKER_IMAGE="nbrs-cql-cpp-driver-builder"

# ─── Build the C++ driver in Docker (matching host OS) ───

build_driver() {
    # Detect host OS for matching Docker base image
    local base_image="ubuntu:22.04"
    if [ -f /etc/os-release ]; then
        local version_id
        version_id=$(grep '^VERSION_ID=' /etc/os-release | cut -d'"' -f2)
        if [ -n "$version_id" ]; then
            base_image="ubuntu:${version_id}"
        fi
    fi
    echo "==> Host OS: $(grep PRETTY_NAME /etc/os-release 2>/dev/null | cut -d'"' -f2)"
    echo "==> Docker base: $base_image"
    echo "==> Building Apache Cassandra C++ driver..."
    docker build \
        --build-arg BASE_IMAGE="$base_image" \
        -f "$SCRIPT_DIR/cassandra-cpp-driver.Dockerfile" \
        -t "$DOCKER_IMAGE" \
        "$SCRIPT_DIR"

    echo "==> Extracting libraries and headers to $SYSROOT..."
    rm -rf "$SYSROOT"
    mkdir -p "$SYSROOT/lib" "$SYSROOT/include"

    # Create a temporary container to copy files from
    local cid
    cid=$(docker create "$DOCKER_IMAGE")
    docker cp "$cid:/usr/local/lib/." "$SYSROOT/lib/"
    docker cp "$cid:/usr/local/include/cassandra.h" "$SYSROOT/include/"
    docker rm "$cid" > /dev/null

    # Flatten multiarch: if libs are in lib/x86_64-linux-gnu/, copy to lib/
    if [ -d "$SYSROOT/lib/x86_64-linux-gnu" ]; then
        cp -a "$SYSROOT/lib/x86_64-linux-gnu/"* "$SYSROOT/lib/"
    fi

    # Remove shared libs (we only want static linking)
    find "$SYSROOT/lib" -name "*.so*" -delete

    # Create libcassandra.a symlink if only _static.a exists
    # (the -sys crate links -lcassandra, not -lcassandra_static)
    if [ -f "$SYSROOT/lib/libcassandra_static.a" ] && [ ! -f "$SYSROOT/lib/libcassandra.a" ]; then
        ln -sf libcassandra_static.a "$SYSROOT/lib/libcassandra.a"
    fi

    echo "==> Driver libraries:"
    ls -la "$SYSROOT/lib"/libcassandra* 2>/dev/null || echo "  (not found)"
    echo "==> Headers:"
    ls -la "$SYSROOT/include"/cassandra.h
    echo "==> Driver build complete."
}

# ─── Build nbrs with cargo, using the local sysroot ───

build_cargo() {
    if [ ! -f "$SYSROOT/lib/libcassandra_static.a" ] && [ ! -f "$SYSROOT/lib/libcassandra.so" ]; then
        echo "ERROR: Driver not found in $SYSROOT"
        echo "  Run 'bash build.sh driver' first, or 'bash build.sh' for a full build."
        exit 1
    fi

    echo "==> Building nbrs --features engine-cassandra-cpp (static linking)..."

    # Point cassandra-cpp-sys at our sysroot
    export CASSANDRA_SYS_LIB_PATH="$SYSROOT/lib"
    export LIBRARY_PATH="$SYSROOT/lib:${LIBRARY_PATH:-}"
    export C_INCLUDE_PATH="$SYSROOT/include:${C_INCLUDE_PATH:-}"

    cd "$PROJECT_ROOT"

    cargo build --release -p nbrs --no-default-features --features engine-cassandra-cpp

    local bin="$PROJECT_ROOT/target/release/nbrs"

    if [ -f "$bin" ]; then
        echo "==> Built: $bin"
        # Verify it's statically linked against libcassandra
        if ldd "$bin" 2>/dev/null | grep -q "libcassandra"; then
            echo "  WARNING: dynamically linked to libcassandra (static link may have failed)"
        else
            echo "  libcassandra: statically linked"
        fi
        echo "==> To run: $bin --help"
    else
        echo "==> Build completed but binary not found."
    fi
}

# ─── cargo install --path nbrs with the cassandra-cpp engine ───

build_install() {
    if [ ! -f "$SYSROOT/lib/libcassandra_static.a" ] && [ ! -f "$SYSROOT/lib/libcassandra.so" ]; then
        echo "ERROR: Driver not found in $SYSROOT"
        echo "  Run 'bash build.sh driver' first, or 'bash build.sh' for a full build."
        exit 1
    fi

    echo "==> cargo install --path nbrs --features all-engines..."

    # Same sysroot env as build_cargo so the linker picks up the
    # static libcassandra and the cassandra-cpp-sys crate's headers.
    export CASSANDRA_SYS_LIB_PATH="$SYSROOT/lib"
    export LIBRARY_PATH="$SYSROOT/lib:${LIBRARY_PATH:-}"
    export C_INCLUDE_PATH="$SYSROOT/include:${C_INCLUDE_PATH:-}"

    cd "$PROJECT_ROOT"

    # `--path nbrs` from the workspace root. `--locked` keeps the
    # install hermetic against drift in Cargo.lock. `--force` lets
    # you re-run after iterating without removing the prior install.
    # `all-engines` links both scylla and cassandra-cpp so the
    # built binary can pick at runtime via `cqldriver=…`.
    cargo install --locked --force \
        --path nbrs \
        --no-default-features --features all-engines

    local cargo_bin="${CARGO_HOME:-$HOME/.cargo}/bin/nbrs"
    if [ -x "$cargo_bin" ]; then
        echo "==> Installed: $cargo_bin"
    else
        echo "==> Install completed but $cargo_bin not found (see cargo output above)"
    fi
}

# ─── Build everything inside Docker (no host Rust needed) ───

build_docker() {
    echo "==> Staging docker build context..."
    # Workspace Cargo.toml [patch.crates-io] points at
    # links/vectordata-rs/veks-completion, which is a symlink
    # to a sibling project outside the workspace. Docker won't
    # follow symlinks across the build-context boundary, so we
    # stage the workspace into a tmpdir and materialize that one
    # patched path. target/ is huge and excluded.
    local staging
    staging=$(mktemp -d -t nbrs-docker-ctx.XXXXXX)
    trap 'rm -rf "$staging"' RETURN

    rsync -a \
        --exclude=target \
        --exclude=.git \
        --exclude=links \
        "$PROJECT_ROOT/" "$staging/"

    # Materialize only the symlinked path-deps cargo patches against.
    # rsync -L dereferences the symlink chain; --exclude=target keeps
    # the upstream project's build artifacts out.
    mkdir -p "$staging/links/vectordata-rs"
    if [ -e "$PROJECT_ROOT/links/vectordata-rs/veks-completion" ]; then
        rsync -aL --exclude=target --exclude=.git \
            "$PROJECT_ROOT/links/vectordata-rs/veks-completion/" \
            "$staging/links/vectordata-rs/veks-completion/"
    else
        echo "ERROR: $PROJECT_ROOT/links/vectordata-rs/veks-completion not found"
        echo "  workspace Cargo.toml patches veks-completion against this path"
        exit 1
    fi

    echo "==> Context size: $(du -sh "$staging" | cut -f1)"
    echo "==> Building nbrs (cassandra-cpp) entirely in Docker..."
    docker build \
        -f "$SCRIPT_DIR/nbrs-cassandra-cpp.Dockerfile" \
        -t nbrs-cassandra-cpp \
        "$staging"

    echo "==> Docker image: nbrs-cassandra-cpp"
    echo "==> Run: docker run --rm --network host nbrs-cassandra-cpp --help"

    # Optionally extract the binary
    echo "==> Extracting binary..."
    local cid
    cid=$(docker create nbrs-cassandra-cpp)
    docker cp "$cid:/usr/local/bin/nbrs" "$SCRIPT_DIR/nbrs"
    docker rm "$cid" > /dev/null
    echo "==> Extracted: $SCRIPT_DIR/nbrs"
}

# ─── Clean ───

clean() {
    echo "==> Cleaning..."
    rm -rf "$SYSROOT"
    rm -f "$SCRIPT_DIR/nbrs"
    echo "==> Clean complete."
}

# ─── Main ───


case "${1:-default}" in
    default|"")
        build_driver
        build_cargo
        ;;
    driver)
        build_driver
        ;;
    cargo)
        build_cargo
        ;;
    install)
        build_install
        ;;
    docker)
        build_docker
        ;;
    clean)
        clean
        ;;
    *)
        echo "Usage: bash build.sh [driver|cargo|install|docker|clean]"
        echo ""
        echo "  (default)    Build C++ driver in Docker, extract libs, cargo build on host"
        echo "  driver       Build only the C++ driver in Docker, extract to sysroot/"
        echo "  cargo        Build only nbrs --features engine-cassandra-cpp (driver must exist in sysroot/)"
        echo "  install      cargo install --path nbrs --features engine-cassandra-cpp (driver must exist in sysroot/)"
        echo "  docker       Build everything inside Docker (no host Rust needed)"
        echo "  clean        Remove sysroot and artifacts"
        exit 1
        ;;
esac
