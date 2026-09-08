# syntax=docker/dockerfile:1
FROM rust:1.97-slim-bookworm AS builder

WORKDIR /usr/src/kachedb

# Install Linux build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    binutils \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace sources
COPY . .

# Build release binaries with native Linux optimizations (io_uring enabled)
RUN cargo build --workspace --release

# Strip debug symbols to minimize binary footprint
RUN strip /usr/src/kachedb/target/release/kachedb-server \
          /usr/src/kachedb/target/release/kachedb-cli \
          /usr/src/kachedb/target/release/kachedb-bench

# Runtime image — distroless cc-debian12 provides minimal glibc runtime (37.5 MB uncompressed, ~12 MB download)
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /data

# Copy compiled, stripped binaries from builder
COPY --from=builder /usr/src/kachedb/target/release/kachedb-server /usr/local/bin/kachedb-server
COPY --from=builder /usr/src/kachedb/target/release/kachedb-cli    /usr/local/bin/kachedb-cli
COPY --from=builder /usr/src/kachedb/target/release/kachedb-bench  /usr/local/bin/kachedb-bench

# Copy default canonical configuration
COPY --from=builder /usr/src/kachedb/kachedb.conf /etc/kachedb/kachedb.conf

EXPOSE 6379

ENTRYPOINT ["kachedb-server"]
CMD ["-c", "/etc/kachedb/kachedb.conf"]
