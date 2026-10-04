# stream-rs/jobs/charging-sessions/

The `charging-sessions` port (stage 6), built on the `battery-health` pattern
described in `docs/PORTING.md`.

- `src/lib.rs`: `ChargingSessions`, a `common::processor::Processor` with one
  optional open `Session` per VIN (`contracts/charging-sessions.md`): the
  30-minute gap check, `PLUG_IN` replacing an open session, orphans, energy
  sums, `UNPLUG`, and the inactivity timeout on watermarks. It also tallies the
  job's own counters (`orphan`, `sessions_by_close_reason`) through
  `Processor::counters`; the runner's four come from `common`. Coordinates are
  rounded with `rust_decimal` on the double's shortest decimal form. The
  `ChargingSessionRecord` output implements `JsonRecord` (replay files, enum as
  its name) and `encode` (protobuf keyed by VIN for Kafka).
- `src/main.rs`: `common::cli::main` with `common::job::CHARGING`.
- `tests/`: `determinism.rs`, `golden_trace.rs`, `kafka_it.rs` (`#[ignore]`d,
  needs `KAFKA_BROKER`).

```
cargo run --release -p charging-sessions -- --out build/rust-parity --fixture charging-sessions=parity/replay/charging-sessions.jsonl
cargo run --release -p charging-sessions -- --out build/rust-parity/examples/charging-sessions parity/examples parity/probes
cargo run --release -p charging-sessions -- --kafka      # INPUT_TOPIC/OUTPUT_TOPIC/GROUP_ID default to the contract's
```
