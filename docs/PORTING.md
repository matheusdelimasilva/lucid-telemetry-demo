# Porting a job to Rust

The checklist from SPEC.md ("Rust skeleton and reference job"), with the
mechanics the CI relies on. Seeded in stage 4b; the "pattern" section below is
what the `battery-health` reference port (stage 5) teaches.

## The pattern: `battery-health`

A job is one crate under `stream-rs/jobs/<job>/` that writes a `Processor`
and nothing else; `crates/common` does the rest.

- **Where the Processor lives:** `stream-rs/jobs/battery-health/src/lib.rs`
  (~100 lines). `BatteryHealth` implements `common::processor::Processor`
  with `State = BTreeMap<window_start, Window>` per VIN: `on_event` folds a
  reading into its epoch-aligned 5-minute window, `on_watermark` closes every
  window whose end is `<=` the watermark (in `window_start` order), `is_open`
  is "any window still open". The output type is a newtype over the prost
  `BatteryWindow` that implements `common::record::JsonRecord` (the JSON the
  records/trace files use; each job owns its encoding, e.g. enum fields as
  names) and `encode` (protobuf bytes keyed by VIN for Kafka).
  `src/main.rs` is one line: `common::cli::main::<BatteryHealth>(BATTERY, encode)`.
- **What `common` provides** (job-agnostic, nothing to copy):
  `runner` (validation, lateness, per-VIN dedup, watermark, `(ts, arrival_seq)`
  release order, the four common counters, drain inspection; a job with more
  counters, like `charging-sessions`' `orphan` and `sessions_by_close_reason`,
  tallies them itself and returns them from `Processor::counters`), `replay`
  (fixture parsing, the
  standard flush, trace lines, `drain.json` in the Spark shape), `artifacts`
  (fixture-mode `<job>.*` files and suite-mode `outputs.json`/`result.json`,
  records sorted by `output_id`, `run.json` with `engine: "rust"`), `cli`
  (`--out`/`--fixture`/suite dirs/`--kafka` parsing and the two replay
  drivers), `kafka` (single-partition consume, produce, commit), `hash`
  (`sha256_hex` for `output_id` and `fixture_sha256`), and `job::JobSpec`
  (name, topics, watermark delay, validator, flush control event).
- **Tests the job crate carries** (`stream-rs/jobs/battery-health/tests/`):
  `determinism.rs` (two fixture runs through the CLI are byte-identical),
  `golden_trace.rs` (batch-by-batch against `parity/golden/<job>.trace.jsonl`:
  watermarks, late count, records emitted; extra evidence, `tools/` decides),
  `kafka_it.rs` (`#[ignore]`d Redpanda round trip: protobuf out, keyed by VIN).
- **The three checks locally** (what `ci/rust_parity.sh` runs; `make up`
  first for the ignored Kafka test, Docker for the image build):

  ```
  make rust-parity                     # everything below for every job
  # or step by step, from the repo root:
  cargo run --manifest-path stream-rs/Cargo.toml --release -p battery-health -- \
      --out build/rust-parity --fixture battery-health=parity/replay/battery-health.jsonl
  cargo run --manifest-path stream-rs/Cargo.toml --release -p battery-health -- \
      --out build/rust-parity/examples/battery-health parity/examples parity/probes
  python3 tools/suite_check.py --job battery-health --run build/rust-parity/examples/battery-health
  python3 tools/parity.py --job battery-health --run build/rust-parity
  python3 tools/privacy_check.py --job battery-health --run build/rust-parity
  ```

  Reports: `build/rust-parity/examples/battery-health/battery-health.suite.md`,
  `build/rust-parity/battery-health.parity.md`, `build/rust-parity/battery-health.privacy.md`.
- **Deploy:** `deploy/battery-health.Dockerfile` (`make battery-image`) and
  `deploy/k8s/battery-health.yaml` (ConfigMap + one-replica Deployment).
  `charging-sessions` (stage 6) follows the same layout under
  `stream-rs/jobs/charging-sessions/`, `deploy/charging-sessions.Dockerfile`
  (`make charging-image`) and `deploy/k8s/charging-sessions.yaml`.

## Checklist

1. Read the Scala job (`legacy-spark/<job>/`) and list every rule. Compare the
   list with `contracts/<job>.md`; ask about any difference before coding.
2. Implement `Processor` in `stream-rs/jobs/<job>/`, following `battery-health`.
   The crate is a workspace member named after its directory and builds a binary
   that follows the CLI contract in `stream-rs/jobs/README.md` (fixture mode
   writing `<job>.{records.jsonl,counters.json,trace.jsonl,drain.json,result.json,run.json}`
   in the Spark harness shapes, and suite mode for the examples and probes).
3. Keep the output message, the `output_id` encoding and the counters exactly as
   the contract says.
4. Pass `make rust-parity` locally (it is what the `rust-parity` CI job runs:
   fixture replay, the 16 examples and the probes, then `tools/suite_check.py`
   on `build/rust-parity/examples/<job>/`, and `tools/parity.py` and
   `tools/privacy_check.py` on `build/rust-parity/`). Attach the three reports
   (`build/rust-parity/examples/<job>/<job>.suite.md`,
   `build/rust-parity/<job>.parity.md`, `<job>.privacy.md`) to the MR.
   The grading is done by `tools/` (protected), not by the job: the binary's
   own per-case PASS/FAIL lines are informational, `tools/suite_check.py`
   compares `outputs.json` with `expected.json` and `tools/parity.py` compares
   the fixture run with `parity/golden/`.
   Known limit: both engines report their own drain result (`drain.json`), since
   only the job can see its own state; the records and counters compared
   against the golden files and `expected.json` are still checked
   independently, so this is acceptable.
5. Don't touch anything the oracle guard protects: `parity/golden/`,
   `parity/replay/`, `parity/examples/`, `parity/probes/`, `legacy-spark/`,
   `contracts/`, `proto/`, `baseline/manifest.json`, `privacy/`, `tools/`, `ci/`,
   `.github/workflows/` or `.gitlab-ci.yml`. CI blocks it: the `oracle-guard`
   job runs the target branch's `ci/oracle_guard.sh` against the MR diff. Run
   `make oracle-guard` locally to check before pushing.
6. Open an MR with the reports and a "What remains before production" section.
