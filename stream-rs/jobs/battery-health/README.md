# stream-rs/jobs/battery-health/

The `battery-health` reference implementation in Rust (stage 5); what
`charging-sessions` is measured against. The pattern is described in
`docs/PORTING.md`.

- `src/lib.rs`: `BatteryHealth`, a `common::processor::Processor` over
  per-VIN epoch-aligned 5-minute windows (`contracts/battery-health.md`), the
  `BatteryWindowRecord` output (`JsonRecord` for the replay files, `encode`
  for Kafka: protobuf keyed by VIN).
- `src/main.rs`: `common::cli::main` with `common::job::BATTERY`.
- `tests/`: `determinism.rs`, `golden_trace.rs`, `kafka_it.rs` (`#[ignore]`d,
  needs `KAFKA_BROKER`).

```
cargo run --release -p battery-health -- --out build/rust-parity --fixture battery-health=parity/replay/battery-health.jsonl
cargo run --release -p battery-health -- --out build/rust-parity/examples/battery-health parity/examples parity/probes
cargo run --release -p battery-health -- --kafka      # INPUT_TOPIC/OUTPUT_TOPIC/GROUP_ID default to the contract's
```
