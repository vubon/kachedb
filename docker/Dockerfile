# syntax=docker/dockerfile:1
FROM rust:1.80-bullseye as builder

WORKDIR /usr/src/kachedb

# Install Linux build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace sources
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY bindings ./bindings

# Build release binaries with native optimizations
RUN cargo build --workspace --release

# Runtime image
FROM debian:bullseye-slim

WORKDIR /app

# Copy compiled binaries from builder
COPY --from=builder /usr/src/kachedb/target/release/kachedb-server /usr/local/bin/kachedb-server
COPY --from=builder /usr/src/kachedb/target/release/kachedb-cli /usr/local/bin/kachedb-cli

EXPOSE 6379

ENTRYPOINT ["kachedb-server"]
CMD ["--port", "6379", "--bind", "0.0.0.0:6379"]
