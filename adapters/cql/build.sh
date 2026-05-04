#!/bin/bash
# Build script for the cassandra-cpp engine of nbrs-adapter-cql.
#
# This script:
#   1. Builds the Apache Cassandra C++ driver from source in
#      Docker (matching the host OS for ABI compatibility).
#   2. Extracts the static library and headers into the cql
#      adapter's per-crate target directory:
#      adapters/cql/target/sysroot/.
#   3. Builds nbrs with --features engine-cassandra-cpp,
#      linking statically against the driver.
#
# Lifecycle:
#   Every artifact this script produces lives under
#   `adapters/cql/target/`, which cargo manages via the
#   `[build] target-dir = "target"` setting in
#   `adapters/cql/.cargo/config.toml`. The script itself
#   never runs `rm`: fresh state comes from docker-cp
#   overwriting files of the same name, and cleanup is
#   `cargo clean` (run from adapters/cql/).
#
# Usage:
#   cd adapters/cql
#   bash build.sh           # full build (driver + nbrs)
#   bash build.sh driver    # build only the C++ driver
#   bash build.sh cargo     # build only nbrs (driver must exist)
#   bash build.sh install   # cargo install --path nbrs with the cpp engine
#   bash build.sh docker    # build nbrs entirely inside Docker
#   bash build.sh clean     # `cargo clean` (this crate) + docker rmi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Pre-cargo linker artifacts (C++ driver static lib + header)
# and the docker-extracted nbrs binary all live under the
# adapter-local cargo target/ — see adapters/cql/.cargo/config.toml
# (`[build] target-dir = "target"`). That makes them part of
# cargo's lifecycle: `cd adapters/cql && cargo clean` wipes
# everything; `cargo clean` at workspace root leaves them
# alone (different target dir entirely).
#
# Build script never runs `rm` — fresh-state mechanics defer
# either to docker (overwriting copies) or to cargo clean.
ADAPTER_TARGET="$SCRIPT_DIR/target"
SYSROOT="$ADAPTER_TARGET/sysroot"
DOCKER_NBRS="$ADAPTER_TARGET/nbrs"
DOCKER_CONTEXT="$ADAPTER_TARGET/docker-context"
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
    # Fresh-state mechanics:
    #   - mkdir -p is non-destructive over an existing dir.
    #   - The Dockerfile builds with `CASS_BUILD_SHARED=OFF`
    #     so only `libcassandra_static.a` lands in
    #     `/usr/local/lib/` — no `.so*` for us to clean up.
    #   - `docker cp` overwrites existing files of the same
    #     name; old ones are replaced rather than appended.
    # The user runs `cd adapters/cql && cargo clean` when
    # they want a totally fresh start.
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
    # The driver build (build_driver) deletes all `.so*` files
    # under sysroot — only the static archive remains. So check
    # for either the canonical name or the upstream-installed
    # `_static` variant.
    if [ ! -f "$SYSROOT/lib/libcassandra_static.a" ] \
       && [ ! -f "$SYSROOT/lib/libcassandra.a" ]; then
        echo "ERROR: Driver not found in $SYSROOT" >&2
        echo "  Run 'bash build.sh driver' first, or 'bash build.sh' for a full build." >&2
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
    if [ ! -f "$SYSROOT/lib/libcassandra_static.a" ] \
       && [ ! -f "$SYSROOT/lib/libcassandra.a" ]; then
        echo "ERROR: Driver not found in $SYSROOT" >&2
        echo "  Run 'bash build.sh driver' first, or 'bash build.sh' for a full build." >&2
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
    echo "==> Staging docker build context at $DOCKER_CONTEXT..."
    # Workspace Cargo.toml [patch.crates-io] points at
    # links/vectordata-rs/veks-completion, which is a symlink
    # to a sibling project outside the workspace. Docker won't
    # follow symlinks across the build-context boundary, so we
    # stage the workspace into a known dir and materialize
    # that one patched path. target/ is huge and excluded.
    #
    # The staging dir lives under our cargo-managed target/
    # so its lifecycle defers to `cargo clean`. `rsync --delete`
    # ensures fresh content each build without any rm step in
    # this script.
    mkdir -p "$DOCKER_CONTEXT"

    rsync -a --delete \
        --exclude=target \
        --exclude=.git \
        --exclude=links \
        "$PROJECT_ROOT/" "$DOCKER_CONTEXT/"

    # Materialize only the symlinked path-deps cargo patches against.
    # rsync -L dereferences the symlink chain; --exclude=target keeps
    # the upstream project's build artifacts out.
    mkdir -p "$DOCKER_CONTEXT/links/vectordata-rs"
    if [ -e "$PROJECT_ROOT/links/vectordata-rs/veks-completion" ]; then
        rsync -aL --delete --exclude=target --exclude=.git \
            "$PROJECT_ROOT/links/vectordata-rs/veks-completion/" \
            "$DOCKER_CONTEXT/links/vectordata-rs/veks-completion/"
    else
        echo "ERROR: $PROJECT_ROOT/links/vectordata-rs/veks-completion not found" >&2
        echo "  workspace Cargo.toml patches veks-completion against this path" >&2
        exit 1
    fi

    echo "==> Context size: $(du -sh "$DOCKER_CONTEXT" | cut -f1)"
    echo "==> Building nbrs (cassandra-cpp) entirely in Docker..."
    docker build \
        -f "$SCRIPT_DIR/nbrs-cassandra-cpp.Dockerfile" \
        -t nbrs-cassandra-cpp \
        "$DOCKER_CONTEXT"

    echo "==> Docker image: nbrs-cassandra-cpp"
    echo "==> Run: docker run --rm --network host nbrs-cassandra-cpp --help"

    # Optionally extract the binary
    echo "==> Extracting binary..."
    local cid
    cid=$(docker create nbrs-cassandra-cpp)
    # Extract under the cargo-managed target/ alongside the
    # sysroot — `cargo clean` from this crate cleans it up.
    docker cp "$cid:/usr/local/bin/nbrs" "$DOCKER_NBRS"
    docker rm "$cid" > /dev/null
    echo "==> Extracted: $DOCKER_NBRS"
}

# ─── Clean ───

clean() {
    echo "==> Cleaning..."

    # All file-system artifacts (sysroot, docker-extracted
    # nbrs, docker context) live under `adapters/cql/target/`,
    # which is the per-crate target directory configured in
    # `adapters/cql/.cargo/config.toml`. `cargo clean` from
    # this crate dir owns its lifecycle — no rm in this
    # script.
    if ! command -v cargo >/dev/null 2>&1; then
        echo "ERROR: cargo not on PATH — cannot clean" >&2
        exit 1
    fi
    echo "==> cd $SCRIPT_DIR && cargo clean"
    (cd "$SCRIPT_DIR" && cargo clean)

    # Docker images — defer to docker's own rm.
    if command -v docker >/dev/null 2>&1; then
        for img in "$DOCKER_IMAGE" nbrs-cassandra-cpp; do
            if docker image inspect "$img" >/dev/null 2>&1; then
                echo "==> docker rmi $img"
                docker rmi "$img" || true
            fi
        done
    else
        echo "WARN: docker not on PATH — driver/app images not cleaned" >&2
    fi

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
        echo "  driver       Build only the C++ driver in Docker, extract to target/sysroot/"
        echo "  cargo        Build only nbrs --features engine-cassandra-cpp (driver must exist)"
        echo "  install      cargo install --path nbrs --features engine-cassandra-cpp (driver must exist)"
        echo "  docker       Build everything inside Docker (no host Rust needed)"
        echo "  clean        cargo clean (this crate's target/) + docker rmi"
        exit 1
        ;;
esac
