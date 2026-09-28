# lucid-telemetry-demo

An illustrative environment for porting Scala Spark Structured Streaming jobs
that ingest vehicle telemetry (battery health, charging sessions) to
purpose-built Rust stream processors on Kafka (Redpanda, Protobuf wire format).
The port is verified against a frozen Scala baseline with a deterministic
replay harness, parity checks, and privacy regression checks. All data is
synthetic — this is not Lucid's system and contains no real Lucid data.

## Stage status

- [x] Stage 1 — environment check scaffold: folder layout, Redpanda via
      docker-compose, one-message smoke tests for the Python, Rust, and
      Spark/Scala protobuf paths, CI skeleton. No job logic yet.

## Quickstart

```sh
make up           # start Redpanda (docker compose up -d --wait)
make smoke-rust   # Rust: produce + consume one Smoke protobuf message
make smoke-py     # Python: venv + protoc codegen + produce one message
make spark-image  # build the pinned JDK17/Scala/Spark/sbt image (slow, once)
make spark-hello  # Spark: decode one Smoke protobuf message via from_protobuf
make down         # stop and remove the broker
```

## Pinned versions

| Piece | Version |
| --- | --- |
| Redpanda | `redpandadata/redpanda:v25.3.17` |
| JDK | `eclipse-temurin:17.0.15_6-jdk-jammy` |
| Scala | 2.12.18 |
| Spark (`spark-sql`, `spark-protobuf` `_2.12`) | 3.5.9 |
| sbt | 1.10.11 |
| ScalaPB / sbt-protoc / protoc | 0.11.17 / 1.0.7 / 25.5 (pinned release binary) |
| Rust toolchain | stable 1.90.0 (`rust:1.90.0-bookworm` in CI) |
| rdkafka / prost / prost-build / protoc-bin-vendored / tokio | 0.37.x / 0.13.x / 0.13.x / 3.x / 1.x |
| Python (CI) | python:3.12-slim |
| protobuf / grpcio-tools / confluent-kafka | 6.31.1 / 1.76.0 / 2.9.0 |
| Docker / dind (CI) | docker:27 / docker:27-dind |
