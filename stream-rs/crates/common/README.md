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
- `record`: the `JsonRecord` trait a job's output implements (its JSON in the
  replay files; each job owns its encoding).
- `artifacts`: fixture-mode `<job>.{records.jsonl,counters.json,trace.jsonl,drain.json,result.json,run.json}`
  and suite-mode `outputs.json`/`result.json`, in the Spark harness shapes,
  plus the informational `expected.json` comparison.
- `cli`: the job binary's argument parsing and the fixture/suite/Kafka drivers
  (`stream-rs/jobs/README.md`); a job's `main` is one call.
- `hash`: `sha256_hex` for `output_id` and `fixture_sha256`.

The replay trace follows the Spark harness fields but intentionally omits the
Spark-only `microbatches` field. `drain.json` uses the Spark shape too; since
the runner visits every VIN's state in every batch, each `last_markers[].batch`
is the final flush batch.

The Kafka runner keeps processor state in memory and commits offsets only after
processing and output delivery. A crash loses open sessions/windows; there are
no Kafka transactions or durable state recovery. Undecodable Kafka payloads
count as rejected and are skipped.
