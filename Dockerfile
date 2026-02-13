# Stage 1: Build frontend
FROM node:18-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Build Rust backend (cmake-build compiles librdkafka from source)
FROM rust:1.88-bookworm AS rust-builder
RUN apt-get update && \
    apt-get install -y cmake libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Cache dependency build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release --no-default-features --features static-kafka && \
    rm -rf src

# Build the actual application
COPY src/ src/
RUN touch src/main.rs && cargo build --release --no-default-features --features static-kafka

# Stage 3: Minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=rust-builder /app/target/release/kafka-dashboard ./
COPY --from=frontend-builder /app/frontend/dist ./static/

ENV STATIC_DIR=./static
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=3001
ENV RUST_LOG=info

EXPOSE 3001
ENTRYPOINT ["./kafka-dashboard"]
