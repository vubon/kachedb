# syntax=docker/dockerfile:1
FROM rust:1.97-slim-bookworm AS builder

WORKDIR /usr/src/kachedb

# Install Linux build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace sources
COPY . .

# Build release binaries with native Linux optimizations (io_uring enabled)
RUN cargo build --workspace --release

# Runtime image
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled binaries from builder
COPY --from=builder /usr/src/kachedb/target/release/kachedb-server /usr/local/bin/kachedb-server
COPY --from=builder /usr/src/kachedb/target/release/kachedb-cli    /usr/local/bin/kachedb-cli
COPY --from=builder /usr/src/kachedb/target/release/kachedb-bench  /usr/local/bin/kachedb-bench

EXPOSE 6379

ENTRYPOINT ["kachedb-server"]
CMD ["--port", "6379", "--bind", "0.0.0.0:6379"]
