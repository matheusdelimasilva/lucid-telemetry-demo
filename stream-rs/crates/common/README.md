# `common`

Shared plumbing for the two Rust telemetry jobs:

- `config`: environment-based Kafka and batch settings.
- `proto`: generated input and output Protobuf types.
- `event` and `validate`: strict fixture decoding and input contract checks.
- `processor` and `runner`: small event-time processing interface, per-VIN
  buffering/deduplication, watermarks, counters, and drain inspection.
- `kafka`: single-partition batch consumption, output delivery, and offset
  commits.
- `replay`: fixture schedule execution and `trace.jsonl`, `drain.json`, and
  `outputs.json` artifacts.

The replay trace follows the Spark harness fields but intentionally omits the
Spark-only `microbatches` field.

The Kafka runner keeps processor state in memory and commits offsets only after
processing and output delivery. A crash loses open sessions/windows; there are
no Kafka transactions or durable state recovery. Undecodable Kafka payloads
count as rejected and are skipped.
