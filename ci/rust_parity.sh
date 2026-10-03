#!/bin/sh
# rust-parity CI job (stage 4b). Auto-discovers every Rust job under
# stream-rs/jobs/*/Cargo.toml and, for each one:
#   1. replays its frozen fixture        -> build/rust-parity/<job>.*
#   2. replays the 16 examples + probes  -> build/rust-parity/examples/<job>/
#   3. tools/suite_check.py grades the examples/probes output independently
#      (the binary's own PASS/FAIL lines are informational)
#   4. tools/parity.py and tools/privacy_check.py on the fixture run
#   5. the crate's own tests: `cargo test -p <job>` and then the #[ignore]d
#      ones (`-- --ignored`, Kafka integration tests; KAFKA_BROKER must point
#      at a running Redpanda, see `make up`)
#   6. `docker build -f deploy/<job>.Dockerfile .` when that file exists
# Any failure fails the job. The CLI contract a job binary must follow is in
# stream-rs/jobs/README.md. Stages 5-6 add a job directory; this script and the
# CI config stay untouched.
set -eu
cd "$(dirname "$0")/.."

OUT=build/rust-parity
PYTHON=${PYTHON:-python3}
CARGO_FLAGS=${CARGO_FLAGS:---release}

jobs=""
for toml in stream-rs/jobs/*/Cargo.toml; do
  [ -f "$toml" ] || continue
  jobs="$jobs $(basename "$(dirname "$toml")")"
done
set -- $jobs

if [ "$#" -eq 0 ]; then
  echo "0 Rust jobs found under stream-rs/jobs/*; nothing to run"
  exit 0
fi
echo "$# Rust job(s) found under stream-rs/jobs/*:$jobs"

rm -rf "$OUT"
mkdir -p "$OUT"
status=0
for job in "$@"; do
  echo "== $job: fixture replay (parity/replay/$job.jsonl -> $OUT)"
  if ! cargo run --manifest-path stream-rs/Cargo.toml $CARGO_FLAGS -p "$job" -- \
      --out "$OUT" --fixture "$job=parity/replay/$job.jsonl"; then
    echo "FAIL $job: fixture replay failed; parity and privacy-check not run"
    status=1
    continue
  fi

  echo "== $job: examples and probes (parity/examples parity/probes -> $OUT/examples/$job)"
  if ! cargo run --manifest-path stream-rs/Cargo.toml $CARGO_FLAGS -p "$job" -- \
      --out "$OUT/examples/$job" parity/examples parity/probes; then
    echo "note: $job's own suite verdict is FAIL (informational; tools/suite_check.py decides)"
  fi

  echo "== $job: suite-check (tools/suite_check.py grades $OUT/examples/$job against expected.json)"
  "$PYTHON" tools/suite_check.py --job "$job" --run "$OUT/examples/$job" \
      --suite parity/examples --suite parity/probes || status=1

  echo "== $job: parity"
  "$PYTHON" tools/parity.py --job "$job" --run "$OUT" || status=1

  echo "== $job: privacy-check"
  "$PYTHON" tools/privacy_check.py --job "$job" --run "$OUT" || status=1

  echo "== $job: cargo test -p $job"
  cargo test --manifest-path stream-rs/Cargo.toml $CARGO_FLAGS -p "$job" || status=1

  echo "== $job: cargo test -p $job -- --ignored (KAFKA_BROKER=${KAFKA_BROKER:-unset})"
  cargo test --manifest-path stream-rs/Cargo.toml $CARGO_FLAGS -p "$job" -- --ignored --nocapture || status=1

  dockerfile="deploy/$job.Dockerfile"
  if [ -f "$dockerfile" ]; then
    echo "== $job: docker build -f $dockerfile ."
    docker build -f "$dockerfile" -t "lucid-$job:dev" . || status=1
  else
    echo "== $job: no $dockerfile; docker build skipped"
  fi
done

exit $status
