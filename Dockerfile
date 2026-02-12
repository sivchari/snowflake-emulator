# Build stage
FROM rust:1.93-slim AS builder

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY Cargo.toml Cargo.lock ./
COPY cli ./cli
COPY engine ./engine
COPY server ./server

# Build release binary
RUN cargo build --release --package server

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/server /app/snowflake-emulator

EXPOSE 8080

ENTRYPOINT ["/app/snowflake-emulator"]
