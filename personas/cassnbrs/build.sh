#!/bin/bash
# Build script for cassnbrs — the Cassandra/CQL persona.
#
# This script:
#   1. Builds the Apache Cassandra C++ driver from source in Docker
#   2. Extracts the static library and headers to a local sysroot
#   3. Builds cassnbrs with cargo, linking statically against the driver
#
# Usage:
#   cd personas/cassnbrs
#   bash build.sh           # full build (driver + cassnbrs)
#   bash build.sh driver    # build only the C++ driver
#   bash build.sh cargo     # build only cassnbrs (driver must exist)
#   bash build.sh docker    # build cassnbrs entirely inside Docker
#   bash build.sh clean     # remove sysroot and build artifacts

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SYSROOT="$SCRIPT_DIR/sysroot"
DOCKER_IMAGE="cassnbrs-cpp-driver-builder"

# ─── Build the C++ driver in Docker (matching host OS) ───

build_driver() {
    # Detect host OS for matching Docker base image
    local base_image="ubuntu:22.04"
    if [ -f /etc/os-release ]; then
        local version_id
        version_id=$(grep VERSION_ID /etc/os-release | cut -d'"' -f2)
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

# ─── Build cassnbrs with cargo, using the local sysroot ───

build_cargo() {
    if [ ! -f "$SYSROOT/lib/libcassandra_static.a" ] && [ ! -f "$SYSROOT/lib/libcassandra.so" ]; then
        echo "ERROR: Driver not found in $SYSROOT"
        echo "  Run 'bash build.sh driver' first, or 'bash build.sh' for a full build."
        exit 1
    fi

    echo "==> Building cassnbrs with cargo (static linking)..."

    # Point cassandra-cpp-sys at our sysroot
    export CASSANDRA_SYS_LIB_PATH="$SYSROOT/lib"
    export LIBRARY_PATH="$SYSROOT/lib:${LIBRARY_PATH:-}"
    export C_INCLUDE_PATH="$SYSROOT/include:${C_INCLUDE_PATH:-}"

    cd "$SCRIPT_DIR"

    cargo build --release

    local bin="$SCRIPT_DIR/target/release/cassnbrs"
    if [ ! -f "$bin" ]; then
        bin="$PROJECT_ROOT/target/release/cassnbrs"
    fi

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

# ─── Build everything inside Docker (no host Rust needed) ───

build_docker() {
    echo "==> Building cassnbrs entirely in Docker..."
    docker build \
        -f "$SCRIPT_DIR/cassnbrs.Dockerfile" \
        -t cassnbrs \
        "$PROJECT_ROOT"

    echo "==> Docker image: cassnbrs"
    echo "==> Run: docker run --rm --network host cassnbrs --help"

    # Optionally extract the binary
    echo "==> Extracting binary..."
    local cid
    cid=$(docker create cassnbrs)
    docker cp "$cid:/usr/local/bin/cassnbrs" "$SCRIPT_DIR/cassnbrs"
    docker rm "$cid" > /dev/null
    echo "==> Extracted: $SCRIPT_DIR/cassnbrs"
}

# ─── Clean ───

clean() {
    echo "==> Cleaning..."
    rm -rf "$SYSROOT"
    rm -f "$SCRIPT_DIR/cassnbrs"
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
    docker)
        build_docker
        ;;
    clean)
        clean
        ;;
    *)
        echo "Usage: bash build.sh [driver|cargo|docker|clean]"
        echo ""
        echo "  (default)    Build C++ driver in Docker, extract libs, cargo build on host"
        echo "  driver       Build only the C++ driver in Docker, extract to sysroot/"
        echo "  cargo        Build only cassnbrs (driver must exist in sysroot/)"
        echo "  docker       Build everything inside Docker (no host Rust needed)"
        echo "  clean        Remove sysroot and artifacts"
        exit 1
        ;;
esac
