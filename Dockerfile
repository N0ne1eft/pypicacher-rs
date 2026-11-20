FROM rust:1-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/pypicacher-rs /usr/local/bin/pypicacher-rs

VOLUME ["/data/cache"]
EXPOSE 8080

ENV PYPICACHER_LISTEN=0.0.0.0:8080 \
    PYPICACHER_CACHE_DIR=/data/cache \
    PYPICACHER_MAX_CACHE_SIZE=10GiB

ENTRYPOINT ["/usr/local/bin/pypicacher-rs"]
