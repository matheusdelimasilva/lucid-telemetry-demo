# battery-health job image. Build from the repo root:
#   docker build -f deploy/battery-health.Dockerfile -t lucid-battery-health:dev .
# (what ci/rust_parity.sh runs). Multi-arch base images; no arch switch needed.
FROM rust:1.90.0-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential libssl-dev zlib1g-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY proto/ proto/
COPY stream-rs/ stream-rs/
RUN cd stream-rs && cargo build --release --locked -p battery-health

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates zlib1g libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/stream-rs/target/release/battery-health /usr/local/bin/battery-health
USER 65532:65532
# Kafka mode by default; replay modes work too (`--out DIR --fixture ...`).
ENTRYPOINT ["battery-health"]
CMD ["--kafka"]
