# syntax=docker/dockerfile:1

# --- builder ---------------------------------------------------------------
FROM rust:1-slim-bookworm AS builder
WORKDIR /build

# A C toolchain covers any dependency with a build script that needs cc.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release --bin soma-server

# --- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --user-group soma \
 && mkdir -p /var/lib/soma \
 && chown soma:soma /var/lib/soma

COPY --from=builder /build/target/release/soma-server /usr/local/bin/soma-server

USER soma
VOLUME ["/var/lib/soma"]
# S3 endpoint + admin (health/metrics)
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/soma-server"]
