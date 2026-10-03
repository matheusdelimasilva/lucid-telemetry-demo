# stream-rs/jobs/

One directory per Rust job, each a workspace member crate named after its
directory (`battery-health`, `charging-sessions`). The `rust-parity` CI job
(`ci/rust_parity.sh`) auto-discovers `stream-rs/jobs/*/Cargo.toml`, so adding a
job never touches CI. Stage 5 adds `battery-health`, stage 6 `charging-sessions`.

## CLI contract (what `ci/rust_parity.sh` runs)

Each job crate builds a binary with the same two modes as the Spark replay
harness (`legacy-spark/replay-harness`), all paths relative to the repo root:

```
cargo run --release -p <job> -- --out build/rust-parity --fixture <job>=parity/replay/<job>.jsonl
cargo run --release -p <job> -- --out build/rust-parity/examples/<job> parity/examples parity/probes
```

**Fixture mode** (`--fixture <job>=<path>`) replays the frozen fixture and
writes, under `--out`, the files `tools/parity.py` and `tools/privacy_check.py`
read. Same names and shapes as the Spark harness's whole-job mode (see
`parity/golden/` for records, counters and trace; `Harness.runFixture` for the
rest):

| File | Shape |
| --- | --- |
| `<job>.records.jsonl` | one output record per line, sorted by `output_id` |
| `<job>.counters.json` | the contract's counters (`parity/golden/<job>.counters.json`) |
| `<job>.trace.jsonl` | one line per batch: `batch`, `kind`, `arrival_seqs`, `watermark_in_effect`, `watermark_after`, `num_rows_dropped_by_watermark`, `harness_late`, `records_emitted`; the Spark-only `microbatches` field is omitted |
| `<job>.drain.json` | `passed`, `final_watermark`, `target_watermark`, `state_rows_total_last_progress`, `vins_reached_state_plus_reserved`, `last_markers` (`{vin, batch, busy}`), `failures` |
| `<job>.result.json` | `job`, `status` (`PASS`/`FAIL`/`REFUSED`), `failures` |
| `<job>.run.json` | `job`, `engine: "rust"`, `fixture_sha256` (of the fixture bytes), `output_topics` (one logical topic), `watermark_delay_ms`, `replay` (free-form: toolchain, crate versions) |

`tools/parity.py` refuses to compare unless `engine` is `spark` or `rust`,
`job` and `fixture_sha256` match `baseline/manifest.json`, and
`watermark_delay_ms` equals the manifest's delay for the job. The legacy Spark
harness writes `implementation: spark-legacy` instead of `engine`; that counts
as `spark` and is additionally checked against the pinned Spark environment.

**Suite mode** (`--out <dir> <suite-dir>...`) runs every `<nn>-<slug>/` case
under the given suites whose `expected.json` names this job, writes
`<dir>/<suite>/<case>/{outputs.json,trace.jsonl,drain.json,result.json}` as the
Spark harness does, prints one `PASS`/`FAIL` line per case, and exits 1 on any
mismatch. Cases for the other job are skipped, not failed.

Exit code: 0 only if every check passed. The script runs fixture mode, then
suite mode, then `tools/parity.py --job <job> --run build/rust-parity` and
`tools/privacy_check.py --job <job> --run build/rust-parity`.
